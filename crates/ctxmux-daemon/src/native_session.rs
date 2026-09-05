//! POSIX session ownership for one native Run.

use std::{
    thread,
    time::{Duration, Instant},
};

use portable_pty::{Child, ChildKiller, ExitStatus};
#[cfg(not(target_os = "macos"))]
use rustix::process::getpgid;
use rustix::{
    io::Errno,
    process::{
        Pid, Signal, WaitId, WaitIdOptions, WaitIdStatus, getsid, kill_process, kill_process_group,
        waitid,
    },
};

/// Longest gap between quiescence checks. Reached by doubling from
/// [`QUIESCENCE_FIRST_POLL`], so a long wait stays cheap in wakeups.
const QUIESCENCE_POLL: Duration = Duration::from_millis(10);

/// Gap before the FIRST re-check after signalling.
///
/// A signalled child without a handler dies in microseconds, but the check
/// immediately after `signal_members` races the kernel and essentially always
/// loses -- so the first sleep is what the caller actually waits out. At a flat
/// 10 ms that made a whole `stop` cost 12.15 ms on the farm, against 0.27 ms
/// for a Run whose child had already exited: the Stop machinery is nearly free
/// and the sleep was the operation. Starting 50x finer and doubling keeps the
/// common case at a fraction of a millisecond without turning a 500 ms
/// graceful timeout into thousands of wakeups.
const QUIESCENCE_FIRST_POLL: Duration = Duration::from_micros(200);

/// One native Run's kernel-owned session identity.
///
/// `portable-pty` establishes the spawned child as a session leader before
/// exec, so the direct child PID is also the Run SID and initial PGID.
pub(crate) struct NativeSession {
    id: Pid,
    leader_reaped: bool,
    #[cfg(test)]
    leader_probe: Option<std::sync::Arc<dyn Fn() -> Result<bool, String> + Send + Sync>>,
}

impl std::fmt::Debug for NativeSession {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NativeSession")
            .field("id", &self.id)
            .field("leader_reaped", &self.leader_reaped)
            .finish_non_exhaustive()
    }
}

impl NativeSession {
    pub(crate) fn from_child_pid(pid: u32) -> Result<Self, String> {
        let raw = i32::try_from(pid)
            .map_err(|_| format!("native child PID {pid} does not fit a POSIX process ID"))?;
        let id = Pid::from_raw(raw)
            .ok_or_else(|| format!("native child PID {pid} is not a positive process ID"))?;
        Ok(Self {
            id,
            leader_reaped: false,
            #[cfg(test)]
            leader_probe: None,
        })
    }

    /// Deliver SIGINT to the current foreground process group after proving
    /// that group still belongs to this Run's session.
    #[cfg(not(target_os = "macos"))]
    pub(crate) fn interrupt(&self, foreground_group: u32) -> Result<(), String> {
        if self.leader_is_terminal()? {
            return Err(format!(
                "native session {} leader has already exited",
                self.id.as_raw_pid()
            ));
        }
        let raw = i32::try_from(foreground_group).map_err(|_| {
            format!("foreground process group {foreground_group} does not fit a POSIX process ID")
        })?;
        let group = Pid::from_raw(raw).ok_or_else(|| {
            format!("foreground process group {foreground_group} is not a positive process ID")
        })?;
        let member = self
            .members(true)?
            .into_iter()
            .find(|pid| getpgid(Some(*pid)).is_ok_and(|pgid| pgid == group))
            .ok_or_else(|| {
                format!(
                    "foreground process group {foreground_group} no longer belongs to native session {}",
                    self.id.as_raw_pid()
                )
            })?;
        self.verify_member(member)?;
        if getpgid(Some(member)).map_err(|error| {
            format!("failed to revalidate foreground process group {foreground_group}: {error}")
        })? != group
        {
            return Err(format!(
                "foreground process group {foreground_group} changed before interrupt"
            ));
        }
        if self.leader_is_terminal()? {
            return Err(format!(
                "native session {} leader exited before interrupt",
                self.id.as_raw_pid()
            ));
        }
        kill_process_group(group, Signal::INT).map_err(|error| {
            format!("failed to interrupt foreground process group {foreground_group}: {error}")
        })
    }

    /// Gracefully terminate, then force, every process still in the owned
    /// session. Success includes direct-child reap and an empty session.
    pub(crate) fn stop(
        &mut self,
        child: &mut (dyn Child + Send + Sync),
        graceful: Duration,
        forced: Duration,
    ) -> Result<(ctxmux_protocol::StopDisposition, ExitStatus), String> {
        self.signal_members(Signal::TERM)?;
        if let Some(status) = self.wait_quiescent(child, Signal::TERM, Instant::now() + graceful)? {
            return Ok((ctxmux_protocol::StopDisposition::Graceful, status));
        }

        self.signal_members(Signal::KILL)?;
        self.wait_quiescent(child, Signal::KILL, Instant::now() + forced)?
            .map(|status| (ctxmux_protocol::StopDisposition::Forced, status))
            .ok_or_else(|| {
                format!(
                    "native session {} remained live after graceful and forced Stop phases",
                    self.id.as_raw_pid()
                )
            })
    }

    /// A naturally exited direct child cannot leave Run-owned descendants
    /// behind. Force any remainder and require an empty session.
    pub(crate) fn finish_after_direct_exit(
        &mut self,
        child: &mut (dyn Child + Send + Sync),
        deadline: Instant,
    ) -> Result<(ExitStatus, ctxmux_protocol::StopDisposition), String> {
        if !self.leader_is_terminal()? {
            return Err(format!(
                "native session {} leader was not terminal at natural-exit cleanup",
                self.id.as_raw_pid()
            ));
        }
        if self.members(false)?.is_empty() {
            return self
                .reap_leader(child)
                .map(|status| (status, ctxmux_protocol::StopDisposition::Graceful));
        }
        self.signal_members(Signal::KILL)?;
        let mut backoff = QUIESCENCE_FIRST_POLL;
        while Instant::now() < deadline {
            let members = self.members(false)?;
            if members.is_empty() {
                return self
                    .reap_leader(child)
                    .map(|status| (status, ctxmux_protocol::StopDisposition::Forced));
            }
            // Survivors of a group-wide KILL left the group; sweep them off the
            // census this loop already took.
            let _ = self.signal_stragglers(&members, Signal::KILL);
            thread::sleep(backoff.min(deadline.saturating_duration_since(Instant::now())));
            backoff = (backoff * 2).min(QUIESCENCE_POLL);
        }
        Err(format!(
            "native session {} retained descendants after direct-child exit",
            self.id.as_raw_pid()
        ))
    }

    /// Wait for the owned session to drain, sweeping any process the group-wide
    /// signal could not reach.
    ///
    /// The census here is the proof of session-emptiness and is not optional:
    /// it is what makes a returned Stop mean "nothing of this Run is left". Its
    /// member list doubles as the straggler list, so the sweep is free.
    fn wait_quiescent(
        &mut self,
        child: &mut (dyn Child + Send + Sync),
        signal: Signal,
        deadline: Instant,
    ) -> Result<Option<ExitStatus>, String> {
        let mut backoff = QUIESCENCE_FIRST_POLL;
        loop {
            // The `&&` short-circuit is load-bearing: while the leader is still
            // alive there is nothing to prove and no census is taken. Making
            // this unconditional costs a full host walk per poll.
            if self.leader_is_terminal()? {
                let members = self.members(false)?;
                if members.is_empty() {
                    return self.reap_leader(child).map(Some);
                }
                // These outlived a signal to the whole group, so they left the
                // group while staying in the session: signal them directly, off
                // the census this loop already took. Failures are not fatal --
                // the census is the authority on emptiness, and a straggler we
                // cannot signal just keeps the loop going until the caller
                // escalates or gives up.
                let _ = self.signal_stragglers(&members, signal);
            }
            if Instant::now() >= deadline {
                return Ok(None);
            }
            thread::sleep(backoff.min(deadline.saturating_duration_since(Instant::now())));
            backoff = (backoff * 2).min(QUIESCENCE_POLL);
        }
    }

    /// Signal every owned process, without walking the host to find them.
    ///
    /// `portable-pty` makes the child a session leader before exec, so the
    /// leader's PID is also the initial process-group ID: `kill(-leader)`
    /// reaches the whole group in one syscall. The walk this replaces cost
    /// 1.009 ms of a 3.046 ms Stop on a 868-process farm host -- it existed
    /// only to build the list of PIDs to signal, and in the common case that
    /// list is just the leader.
    ///
    /// Reuse is safe without a fresh check: the leader is observed with
    /// `NOWAIT` and reaped only at the end, so it stays a zombie in its own
    /// group for this whole window, and a process-group ID cannot be recycled
    /// while any member -- zombie included -- remains.
    ///
    /// The group is not the session. A descendant that called `setpgid` but not
    /// `setsid` leaves the group while staying owned, so it survives this and is
    /// swept by `signal_stragglers` off the census that `wait_quiescent` takes
    /// anyway. (One that called `setsid` left the session and was never owned by
    /// either path.)
    /// Nobody left to signal is not a failure. `ESRCH` is the portable way the
    /// kernel says the group is gone, but on Darwin an all-zombie group answers
    /// `EPERM` instead: a zombie keeps the group ID alive while owning no
    /// credentials to check a signal against. Measured directly — `killpg` on a
    /// group whose only member is a zombie leader returns `EPERM` while `kill`
    /// on that same PID returns success. So a `stop` that raced the child's own
    /// exit reported `Unknown` on a session it had every right to signal, which
    /// is how `concurrent_interrupt_and_stop_have_only_owner_declared_outcomes`
    /// failed ~60% of local runs: the Interrupt reaped the shell first.
    ///
    /// Neither errno tells us the session is *empty* — `members()` is the sole
    /// authority on that, and every caller consults it after this returns.
    fn signal_members(&self, signal: Signal) -> Result<(), String> {
        self.require_waitable_anchor()?;
        match kill_process_group(self.id, signal) {
            Ok(()) | Err(Errno::SRCH | Errno::PERM) => Ok(()),
            Err(error) => Err(format!(
                "failed to signal native session {} process group: {error}",
                self.id.as_raw_pid()
            )),
        }
    }

    /// Signal owned processes the group-wide signal could not reach.
    ///
    /// Takes the members the caller already enumerated, so this adds no walk of
    /// its own.
    fn signal_stragglers(&self, stragglers: &[Pid], signal: Signal) -> Result<(), String> {
        let mut failures = Vec::new();
        for pid in stragglers {
            if let Err(error) = self.signal_member(*pid, signal) {
                failures.push(error);
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(failures.join("; "))
        }
    }

    /// Revalidate one numeric PID and signal it immediately.
    ///
    /// POSIX exposes no portable incarnation handle for an arbitrary session
    /// descendant. Keep this boundary to the two adjacent syscalls: no wait,
    /// lock acquisition, allocation, logging, or unrelated I/O belongs between
    /// the successful `getsid` check and `kill`.
    fn signal_member(&self, pid: Pid, signal: Signal) -> Result<(), String> {
        match getsid(Some(pid)) {
            Ok(session) if session == self.id => match kill_process(pid, signal) {
                Ok(()) | Err(Errno::SRCH) => Ok(()),
                Err(error) => Err(format!(
                    "failed to signal native session member {}: {error}",
                    pid.as_raw_pid()
                )),
            },
            Ok(session) => Err(format!(
                "process {} moved from native session {} to {} before signal",
                pid.as_raw_pid(),
                self.id.as_raw_pid(),
                session.as_raw_pid()
            )),
            Err(Errno::SRCH) => Ok(()),
            Err(error) => Err(format!(
                "failed to revalidate native session member {}: {error}",
                pid.as_raw_pid()
            )),
        }
    }

    #[cfg(not(target_os = "macos"))]
    fn verify_member(&self, pid: Pid) -> Result<(), String> {
        match getsid(Some(pid)) {
            Ok(session) if session == self.id => Ok(()),
            Ok(session) => Err(format!(
                "process {} moved from native session {} to {} before signal",
                pid.as_raw_pid(),
                self.id.as_raw_pid(),
                session.as_raw_pid()
            )),
            Err(Errno::SRCH) => Err(format!("process {} disappeared", pid.as_raw_pid())),
            Err(error) => Err(format!(
                "failed to revalidate native session member {}: {error}",
                pid.as_raw_pid()
            )),
        }
    }

    /// Every process still in this session, optionally including the leader.
    ///
    /// The census reaps before it counts, because otherwise its answer is
    /// wrong: see [`reap_inherited_orphans`].
    fn members(&self, include_leader: bool) -> Result<Vec<Pid>, String> {
        self.require_waitable_anchor()?;
        let candidates = session_candidates(self.id)?;
        #[cfg(not(target_os = "macos"))]
        reap_inherited_orphans(self.id, &candidates);
        self.classify_members(candidates, include_leader, |pid| getsid(Some(pid)))
    }

    fn classify_members(
        &self,
        process_ids: Vec<u32>,
        include_leader: bool,
        mut session_for: impl FnMut(Pid) -> Result<Pid, Errno>,
    ) -> Result<Vec<Pid>, String> {
        let mut members = Vec::new();
        for process_pid in process_ids {
            let raw = i32::try_from(process_pid).map_err(|_| {
                format!("observed process ID {process_pid} does not fit POSIX pid_t")
            })?;
            let Some(pid) = Pid::from_raw(raw) else {
                continue;
            };
            match session_for(pid) {
                Ok(session) if session == self.id => {
                    if include_leader || pid != self.id {
                        members.push(pid);
                    }
                }
                Ok(_) | Err(Errno::SRCH) => {}
                Err(error) => {
                    return Err(format!(
                        "failed to classify process {} during native session {} census: {error}",
                        pid.as_raw_pid(),
                        self.id.as_raw_pid()
                    ));
                }
            }
        }
        Ok(members)
    }

    /// Whether any child of this process has exited and not yet been reaped.
    ///
    /// Answers in one syscall what [`Self::leader_is_terminal`] answers per Run.
    /// Every un-reaped session leader is a direct child of the daemon (that is
    /// what [`Self::require_waitable_anchor`] keeps true), so `false` here means
    /// no watched leader can be terminal, and a caller may skip peeking them
    /// individually.
    ///
    /// `WNOWAIT` keeps this a peek: the status stays queued for the sequenced
    /// `reap_leader`, which remains the sole reaper. Note the kernel reports the
    /// *same* child until it is reaped, so this can only ever answer "is there
    /// at least one", never enumerate them -- which is why it is a gate rather
    /// than a replacement for the sweep.
    ///
    /// An error is reported as `true`, so a caller falls back to the per-Run
    /// peeks that own the error handling. The gate never decides anything a
    /// sweep would not; it only decides whether the sweep is worth running.
    pub(crate) fn any_child_exited() -> bool {
        let options = WaitIdOptions::EXITED | WaitIdOptions::NOHANG | WaitIdOptions::NOWAIT;
        match waitid(WaitId::All, options) {
            Ok(status) => status.is_some(),
            // ECHILD means there are no children at all, which is a definite
            // "nothing to find". Anything else is unexpected, and an unexpected
            // gate must not be the thing that hides an exit.
            Err(Errno::CHILD) => false,
            Err(_) => true,
        }
    }

    /// [`Self::leader_is_terminal`], skipped when the daemon has no exited child.
    ///
    /// `any_child_exited` comes from one [`Self::any_child_exited`] call shared
    /// by a whole sweep: `false` proves no leader is terminal, so N per-Run
    /// peeks collapse into that single syscall.
    ///
    /// The test probe is consulted *before* the gate on purpose. A fixture that
    /// simulates a terminal leader has no real exited child behind it, so
    /// gating first would make the sweep skip exactly the Runs those fixtures
    /// are about -- a performance knob silently deciding what the tests can
    /// observe.
    pub(crate) fn leader_is_terminal_gated(&self, any_child_exited: bool) -> Result<bool, String> {
        self.require_waitable_anchor()?;
        #[cfg(test)]
        if let Some(probe) = &self.leader_probe {
            return probe();
        }
        if !any_child_exited {
            return Ok(false);
        }
        self.leader_is_terminal()
    }

    pub(crate) fn leader_is_terminal(&self) -> Result<bool, String> {
        self.require_waitable_anchor()?;
        #[cfg(test)]
        if let Some(probe) = &self.leader_probe {
            return probe();
        }
        let options = WaitIdOptions::EXITED | WaitIdOptions::NOHANG | WaitIdOptions::NOWAIT;
        waitid(WaitId::Pid(self.id), options)
            .map(|status| status.is_some())
            .map_err(|error| {
                format!(
                    "failed to observe native session {} leader without reaping: {error}",
                    self.id.as_raw_pid()
                )
            })
    }

    fn reap_leader(&mut self, child: &mut (dyn Child + Send + Sync)) -> Result<ExitStatus, String> {
        self.require_waitable_anchor()?;
        if !self.leader_is_terminal()? {
            return Err(format!(
                "native session {} leader cannot be reaped before terminal observation",
                self.id.as_raw_pid()
            ));
        }
        let status = child
            .wait()
            .map_err(|error| format!("failed to reap native session leader: {error}"))?;
        self.leader_reaped = true;
        Ok(status)
    }

    fn require_waitable_anchor(&self) -> Result<(), String> {
        if self.leader_reaped {
            Err(format!(
                "native session {} lost its waitable leader incarnation anchor",
                self.id.as_raw_pid()
            ))
        } else {
            Ok(())
        }
    }

    #[cfg(test)]
    pub(crate) fn mark_leader_reaped_for_test(&mut self) {
        self.leader_reaped = true;
    }

    #[cfg(test)]
    pub(crate) fn with_leader_probe_for_test(
        mut self,
        probe: std::sync::Arc<dyn Fn() -> Result<bool, String> + Send + Sync>,
    ) -> Self {
        self.leader_probe = Some(probe);
        self
    }
}

/// A live child inherited across an exec-in-place upgrade, addressable only by
/// its bare PID.
///
/// `portable_pty::Child` has no "construct from a PID" path, so after the
/// daemon re-execs itself a surviving direct child is just a number. This
/// `Child` implementation reaps that number through `waitid`, letting an
/// adopted child route through the same `NativeSession` reap machinery as a
/// freshly spawned one.
#[derive(Debug)]
pub(crate) struct AdoptedChild {
    pid: Pid,
    reaped: Option<ExitStatus>,
}

impl AdoptedChild {
    pub(crate) fn from_pid(pid: u32) -> Result<Self, String> {
        let raw = i32::try_from(pid)
            .map_err(|_| format!("adopted child PID {pid} does not fit a POSIX process ID"))?;
        let pid = Pid::from_raw(raw)
            .ok_or_else(|| format!("adopted child PID {pid} is not a positive process ID"))?;
        Ok(Self { pid, reaped: None })
    }
}

impl Child for AdoptedChild {
    fn try_wait(&mut self) -> std::io::Result<Option<ExitStatus>> {
        if let Some(status) = &self.reaped {
            return Ok(Some(status.clone()));
        }
        // REAPING, non-blocking: no NOWAIT here, so a terminal child is
        // actually collected rather than left waitable.
        let options = WaitIdOptions::EXITED | WaitIdOptions::NOHANG;
        match waitid(WaitId::Pid(self.pid), options).map_err(errno_to_io)? {
            Some(status) => {
                let status = exit_status_from_waitid(&status);
                self.reaped = Some(status.clone());
                Ok(Some(status))
            }
            None => Ok(None),
        }
    }

    fn wait(&mut self) -> std::io::Result<ExitStatus> {
        if let Some(status) = &self.reaped {
            return Ok(status.clone());
        }
        // REAPING, blocking: a terminal child is collected before returning.
        let status = waitid(WaitId::Pid(self.pid), WaitIdOptions::EXITED)
            .map_err(errno_to_io)?
            .ok_or_else(|| {
                std::io::Error::other(format!(
                    "blocking wait on adopted child {} returned no status",
                    self.pid.as_raw_pid()
                ))
            })?;
        let status = exit_status_from_waitid(&status);
        self.reaped = Some(status.clone());
        Ok(status)
    }

    fn process_id(&self) -> Option<u32> {
        u32::try_from(self.pid.as_raw_pid()).ok()
    }

    #[cfg(windows)]
    fn as_raw_handle(&self) -> Option<std::os::windows::io::RawHandle> {
        None
    }
}

impl ChildKiller for AdoptedChild {
    fn kill(&mut self) -> std::io::Result<()> {
        kill_process(self.pid, Signal::KILL).map_err(errno_to_io)
    }

    fn clone_killer(&self) -> Box<dyn ChildKiller + Send + Sync> {
        Box::new(AdoptedChildKiller(self.pid))
    }
}

/// The signal-only half of an [`AdoptedChild`], safe to hold while another
/// thread blocks in [`AdoptedChild::wait`].
#[derive(Debug)]
struct AdoptedChildKiller(Pid);

impl ChildKiller for AdoptedChildKiller {
    fn kill(&mut self) -> std::io::Result<()> {
        kill_process(self.0, Signal::KILL).map_err(errno_to_io)
    }

    fn clone_killer(&self) -> Box<dyn ChildKiller + Send + Sync> {
        Box::new(AdoptedChildKiller(self.0))
    }
}

fn errno_to_io(error: Errno) -> std::io::Error {
    std::io::Error::from_raw_os_error(error.raw_os_error())
}

/// Encode a `waitid` result as a `portable_pty::ExitStatus`, preserving whether
/// the child exited normally or was terminated by a signal.
///
/// The final `1` is a defensive default: reaped with `WEXITED`, a terminal
/// child is always `CLD_EXITED` or `CLD_KILLED`/`CLD_DUMPED`, so one of the two
/// branches above fires — this arm should never surface in practice.
fn exit_status_from_waitid(status: &WaitIdStatus) -> ExitStatus {
    if let Some(code) = status.exit_status() {
        ExitStatus::with_exit_code(code.unsigned_abs())
    } else if let Some(signal) = status.terminating_signal() {
        use std::os::unix::process::ExitStatusExt as _;

        // A terminating wait status encodes the signal number in the low bits.
        // `portable_pty`'s standard conversion then retains the platform signal
        // name, matching freshly spawned children instead of flattening it to a
        // synthetic `128 + signal` normal exit code.
        std::process::ExitStatus::from_raw(signal).into()
    } else {
        ExitStatus::with_exit_code(1)
    }
}

#[cfg(target_os = "macos")]
fn session_candidates(_leader: Pid) -> Result<Vec<u32>, String> {
    ctxmux_process_stats::process_ids()
        .map_err(|error| format!("failed to enumerate native session members: {error}"))
}

/// Read the direct children of one thread into `out`.
///
/// `/proc/<pid>/task/<tid>/children` is a space-separated list of PIDs. It is
/// per-thread, so a threaded process needs every `tid` visited -- our leaders
/// are single-threaded shells, but nothing stops a Run from spawning something
/// threaded, and missing a thread's children would under-report the session.
///
/// A vanished pid yields `ENOENT`, which is not an error here: the process
/// exiting is exactly the outcome the caller is waiting for.
#[cfg(not(target_os = "macos"))]
fn push_children(pid: u32, out: &mut Vec<u32>) {
    let Ok(threads) = std::fs::read_dir(format!("/proc/{pid}/task")) else {
        return;
    };
    for thread in threads.flatten() {
        let Some(tid) = thread
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u32>().ok())
        else {
            continue;
        };
        let Ok(children) = std::fs::read_to_string(format!("/proc/{pid}/task/{tid}/children"))
        else {
            continue;
        };
        out.extend(
            children
                .split_ascii_whitespace()
                .filter_map(|entry| entry.parse::<u32>().ok()),
        );
    }
}

/// Enumerate the processes that could still belong to `leader`'s session, by
/// descending `leader`'s own process tree instead of walking the whole host.
///
/// A child inherits its parent's session across `fork`, and the only way out is
/// `setsid`, which puts the caller in a *new* session it leads. So every member
/// of `leader`'s session is a descendant of `leader` -- the host walk was
/// asking a question that only our own subtree can answer positively.
///
/// **The prune is what makes this scale.** Recursion stops at any child outside
/// the session: that child left via `setsid`, so its entire subtree left with
/// it. Without the prune the walk covers the daemon's whole tree, which grows
/// with the fleet -- measured on a 64-core farm host at the 128-Run cap, the
/// unpruned walk costs 0.786 ms against the host census's 1.139 ms, i.e. it
/// gives back almost everything. Pruned, the same case is 0.061 ms, and the
/// cost tracks *this Run's* descendants rather than the fleet or the host:
///
/// | fleet | host census | unpruned | pruned |
/// |---|---|---|---|
/// | 0 | 1.071 ms | 0.029 | **0.028** |
/// | 16 | 1.089 ms | 0.191 | **0.052** |
/// | 128 | 1.139 ms | 0.786 | **0.061** |
///
/// The returned list is a superset of the session, not the session itself: the
/// caller still applies the `getsid` filter, which is what keeps the answer
/// identical to the host walk's (see `classify_members`). Two things make the
/// superset correct rather than merely cheap. The leader itself is included, so
/// a session whose only remaining member is the leader is still observable. And
/// the daemon is a child subreaper, so a descendant orphaned mid-teardown
/// reparents to the *daemon* rather than to init -- it would otherwise leave
/// this subtree while staying in the session, and the sweep would go blind
/// exactly where the census still saw it. That is why the orphan's new parent
/// is swept too.
#[cfg(not(target_os = "macos"))]
fn session_candidates(leader: Pid) -> Result<Vec<u32>, String> {
    // The subtree answer is only equivalent to the host walk while orphaned
    // descendants reparent to US. If this process is not a subreaper they
    // reparent to init instead -- still in the session, no longer in our tree --
    // and the sweep would silently return a narrower answer. Correctness must
    // not depend on a process-wide flag some caller may not have set, so this
    // falls back to the walk rather than quietly under-reporting. The daemon
    // arms the bit at startup (`become_child_subreaper`), so the fast path is
    // what production takes.
    if !is_child_subreaper() {
        return process_ids();
    }
    let leader = leader.as_raw_pid().unsigned_abs();
    // Prune against the leader's ACTUAL session, not against its pid. The two
    // coincide for a real Run -- portable-pty makes the child a session leader,
    // so pid == sid -- but `from_child_pid` accepts any pid, and comparing to
    // the pid would reject every member whenever they differ. `classify_members`
    // applies the same session filter afterwards, so this only decides which
    // subtrees are worth descending.
    let Some(session) = Pid::from_raw(leader.cast_signed())
        .and_then(|pid| getsid(Some(pid)).ok())
        .map(|sid| sid.as_raw_pid().unsigned_abs())
    else {
        // The leader is gone, so nothing can still be attributed to it through
        // a live session id. Fall back rather than return a narrower answer.
        return process_ids();
    };
    let mut candidates = vec![leader];
    // Orphans reparent to this process, not to the leader, so our own direct
    // children are candidates too -- and once the leader dies, its surviving
    // descendants are found ONLY here. Their subtrees are descended into only
    // when they pass the session filter, so an unrelated Run's leader costs one
    // `getsid`.
    let mut frontier = Vec::with_capacity(16);
    push_children(std::process::id(), &mut frontier);
    let mut cursor = 0;
    while cursor < candidates.len() {
        let pid = candidates[cursor];
        cursor += 1;
        push_children(pid, &mut frontier);
        // A child inherits the session; one that does not match has left via
        // `setsid` and takes its subtree with it, so it is never expanded.
        for child in frontier.drain(..) {
            if child != leader && in_session(child, session) && !candidates.contains(&child) {
                candidates.push(child);
            }
        }
    }
    Ok(candidates)
}

/// Reap descendants this process inherited as a subreaper, so the census that
/// follows sees a session that has actually drained.
///
/// This is the other half of `PR_SET_CHILD_SUBREAPER`, not a separate feature.
/// Arming the bit is what keeps an orphaned descendant inside our subtree, and
/// it simultaneously transfers the reaping duty: before it was set, an orphan
/// reparented to init and was reaped immediately; now nothing else in this
/// process will ever `wait` for it. A zombie still answers `getsid`, so
/// [`NativeSession::members`] would count it as a live member and `stop()`
/// would poll to its deadline before reporting a session that is, in fact,
/// empty. Measured on the farm host: two killed orphans report `state=Z`
/// and their session id indefinitely, and the same census returns empty the
/// instant they are reaped.
///
/// **What keeps this from stealing a status someone else owns.** The candidate
/// list is the authority. `session_candidates` already proved every entry is
/// both a descendant of this daemon and a member of *this* session, so the set
/// is exactly this Run's own processes -- a `tmux` short command's orphan sits
/// in the daemon's session under its own group and never appears, and neither
/// does another Run's member. The leader is excluded by pid on top of that,
/// leaving its status to the sequenced `reap_leader`, which stays the only
/// place a Run's exit status is consumed. A candidate that is not our direct
/// child answers `ECHILD`, which is ignored.
///
/// `WNOHANG` throughout: a still-running member is left alone rather than
/// blocking the poll loop.
#[cfg(not(target_os = "macos"))]
fn reap_inherited_orphans(leader: Pid, candidates: &[u32]) {
    use rustix::process::{WaitOptions, waitpid};

    let raw_leader = leader.as_raw_pid().unsigned_abs();
    for &candidate in candidates {
        // The leader's status belongs to the sequenced `reap_leader`.
        if candidate == raw_leader {
            continue;
        }
        let Some(pid) = Pid::from_raw(candidate.cast_signed()) else {
            continue;
        };
        let _ = waitpid(Some(pid), WaitOptions::NOHANG);
    }
}

/// Whether this process inherits orphaned descendants.
///
/// `PR_GET_CHILD_SUBREAPER` reports the attribute set by
/// `PR_SET_CHILD_SUBREAPER`. A failure here is read as "not a subreaper", which
/// selects the conservative host walk.
#[cfg(not(target_os = "macos"))]
fn is_child_subreaper() -> bool {
    rustix::process::child_subreaper().is_ok_and(|pid| pid.is_some())
}

/// Whether `pid` belongs to session `session`.
///
/// Only used to decide whether to *descend* into a subtree. A pid that exits
/// between the `children` read and this call answers `false`, which is correct:
/// it has no descendants left to find.
#[cfg(not(target_os = "macos"))]
fn in_session(pid: u32, session: u32) -> bool {
    let Ok(raw) = i32::try_from(pid) else {
        return false;
    };
    Pid::from_raw(raw)
        .and_then(|pid| getsid(Some(pid)).ok())
        .is_some_and(|sid| sid.as_raw_pid() == session.cast_signed())
}

/// Enumerate every process on the host.
///
/// Linux-only, and reached only as the fallback [`session_candidates`] takes
/// when this process is not a child subreaper, so an orphan that reparented to
/// init is still found. macOS has no fallback to be: without
/// `/proc/<pid>/task/<tid>/children` there is no subtree to descend, so its
/// `session_candidates` is the host walk and calls `ctxmux_process_stats`
/// directly.
///
/// `members()` needs exactly one thing from each process: its session ID, which
/// it obtains with `getsid`. It never reads a name, a command line, or memory
/// figures. Asking `sysinfo` for `ProcessesToUpdate::All` used to answer this
/// question, and it harvested per-process `stat`, `statm`, `status` and
/// `cmdline` for every process on the HOST -- then all of it was discarded
/// except the key set.
///
/// That harvest is why `Stop` cost `63.6 ms + 0.0775 ms * host_process_count`
/// (R^2 = 0.999, measured on a 64-core farm worker): `wait_quiescent` calls
/// `members()` once per 10 ms poll and `signal_members` calls it again, so the
/// scan ran several times per Stop. It also made the cost depend on the whole
/// machine rather than on this daemon -- one Run alongside 3000 unrelated
/// processes measured 352.7 ms, against 121 ms on an idle host.
///
/// Measured split of the two halves on the same host (694 processes):
/// `readdir` 0.496 ms, 694 `getsid` calls 0.162 ms, total 0.657 ms. The
/// enumeration was never the expensive part; the discarded harvest was. Nor
/// was it reducible: replacing `read_dir` with a raw `getdents64` over a
/// 256 KiB buffer saved 0.090 ms of 1.056 (8.7%) at 886 processes, because the
/// cost is the kernel materialising one dentry per process. Only asking a
/// smaller question removes it.
#[cfg(not(target_os = "macos"))]
fn process_ids() -> Result<Vec<u32>, String> {
    let entries = std::fs::read_dir("/proc")
        .map_err(|error| format!("failed to enumerate native session members: {error}"))?;
    let mut pids = Vec::new();
    for entry in entries {
        let entry = entry
            .map_err(|error| format!("failed to read a /proc entry during census: {error}"))?;
        // Only the numeric entries are processes; `/proc` also holds `self`,
        // `net`, `sys` and friends. A PID that exits between readdir and the
        // subsequent `getsid` is handled there as `Errno::SRCH`, so a stale
        // entry here is harmless.
        if let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u32>().ok())
        {
            pids.push(pid);
        }
    }
    if pids.is_empty() {
        // The caller is itself a process, so an empty census means /proc did
        // not answer rather than that the host is empty. Fail closed: an empty
        // member list would otherwise read as "the session is quiescent" and
        // let `stop()` report success over a Run that is still alive.
        return Err("enumerating /proc yielded no processes".to_owned());
    }
    Ok(pids)
}

#[cfg(test)]
mod tests {
    #[cfg(not(target_os = "macos"))]
    use std::time::{Duration, Instant};
    use std::{os::unix::process::CommandExt, process::Command, sync::Arc};

    use rustix::{io::Errno, process::Pid};

    use super::{AdoptedChild, NativeSession};

    #[test]
    fn adopted_child_probes_then_reaps_by_pid_with_latch() {
        // A child that stays alive briefly, then exits 7. We forget std's
        // handle so `AdoptedChild` (via waitid) is the sole reaper.
        let child = Command::new("/bin/sh")
            .args(["-c", "sleep 0.2; exit 7"])
            .spawn()
            .expect("spawn adoptable child");
        let pid = child.id();
        std::mem::forget(child);

        let mut session = NativeSession::from_child_pid(pid).unwrap();
        let mut adopted = AdoptedChild::from_pid(pid).unwrap();

        // (d) The freshly built session is not pre-authorized as reaped: the
        // non-reaping probe succeeds instead of erroring on the lost anchor.
        assert!(
            !session
                .leader_is_terminal()
                .expect("fresh session retains its waitable anchor"),
            "child is still in its sleep, so it is not terminal yet"
        );

        // Poll (bounded) until the probe observes the exit without reaping it.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while !session
            .leader_is_terminal()
            .expect("non-reaping probe keeps working while the child lives")
        {
            assert!(
                std::time::Instant::now() < deadline,
                "adopted child never became terminal within the deadline"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        // (b) After exit, the probe reports terminal and the reap yields 7.
        assert!(session.leader_is_terminal().unwrap());
        let status = session
            .reap_leader(&mut adopted)
            .expect("terminal adopted child reaps cleanly");
        assert_eq!(status.exit_code(), 7);

        // (c) The latch flips: a second reap is refused with the anchor error.
        let error = session
            .reap_leader(&mut adopted)
            .expect_err("a reaped session cannot be reaped again");
        assert!(
            error.contains("lost its waitable leader incarnation anchor"),
            "unexpected second-reap error: {error}"
        );
    }

    #[test]
    fn the_gate_agrees_with_the_per_run_peek_on_both_sides_of_an_exit() {
        // The gate is only sound if `any_child_exited() == false` really does
        // imply no leader is terminal. Prove it against a real child on both
        // sides of its exit, and prove the gated peek matches the ungated one
        // -- a gate that disagreed would be skipping work that mattered.
        let child = Command::new("/bin/sh")
            .args(["-c", "sleep 0.2; exit 5"])
            .spawn()
            .expect("spawn adoptable child");
        let pid = child.id();
        std::mem::forget(child);

        let mut session = NativeSession::from_child_pid(pid).unwrap();
        let mut adopted = AdoptedChild::from_pid(pid).unwrap();

        // While it lives, the gate must not claim an exit this child has not
        // had. Another test's stray child could make the gate true, so assert
        // the implication that actually matters rather than the raw value:
        // a false gate must agree with the real peek.
        if !NativeSession::any_child_exited() {
            assert!(
                !session
                    .leader_is_terminal()
                    .expect("fresh session retains its waitable anchor"),
                "the gate reported no exited child while the leader was terminal"
            );
            assert!(
                !session
                    .leader_is_terminal_gated(false)
                    .expect("gated peek keeps the anchor"),
                "a closed gate must report not-terminal"
            );
        }

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while !session
            .leader_is_terminal()
            .expect("non-reaping probe keeps working")
        {
            assert!(
                std::time::Instant::now() < deadline,
                "child never became terminal within the deadline"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        // Once this child is terminal the gate MUST be open: it is a child of
        // this process and it has exited. This is the direction that would hide
        // an exit forever if it were wrong.
        assert!(
            NativeSession::any_child_exited(),
            "a child of this process has exited, so the gate must report it"
        );
        assert!(
            session
                .leader_is_terminal_gated(true)
                .expect("gated peek keeps the anchor"),
            "an open gate must defer to the per-Run peek, which sees the exit"
        );

        // And the gate is a peek, not a reap: the status is still there to take.
        assert_eq!(
            session
                .reap_leader(&mut adopted)
                .expect("the gate must not have consumed the exit status")
                .exit_code(),
            5
        );
    }

    #[test]
    fn adopted_child_reap_is_idempotent_through_the_cache() {
        use portable_pty::Child;

        // Child exits 7 quickly; forget std's handle so `AdoptedChild` (via
        // waitid) is the sole reaper.
        let child = Command::new("/bin/sh")
            .args(["-c", "sleep 0.2; exit 7"])
            .spawn()
            .expect("spawn adoptable child");
        let pid = child.id();
        std::mem::forget(child);

        let mut adopted = AdoptedChild::from_pid(pid).unwrap();

        // The reaping non-blocking poll collects the zombie exactly once. Every
        // later wait/try_wait must answer from the cache and never fire a second
        // `waitid` — the zombie is gone, so an uncached call would `ECHILD`.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        let first = loop {
            if let Some(status) = adopted.try_wait().expect("non-blocking reap keeps working") {
                break status;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "adopted child never exited within the deadline"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        };
        assert_eq!(first.exit_code(), 7);

        // Cached answers: a blocking wait and a second try_wait both return 7.
        assert_eq!(adopted.wait().expect("cached blocking wait").exit_code(), 7);
        assert_eq!(
            adopted
                .try_wait()
                .expect("cached non-blocking wait")
                .expect("cache retains the reaped status")
                .exit_code(),
            7
        );
    }

    #[test]
    fn adopted_child_preserves_signal_exit_identity() {
        use portable_pty::Child;

        let child = Command::new("/bin/sh")
            .args(["-c", "exec sleep 30"])
            .spawn()
            .expect("spawn signal-exit adopted child");
        let pid = child.id();
        std::mem::forget(child);
        let mut adopted = AdoptedChild::from_pid(pid).unwrap();

        assert!(
            Command::new("kill")
                .args(["-TERM", &pid.to_string()])
                .status()
                .expect("terminate adopted child")
                .success()
        );
        let status = adopted.wait().expect("reap signal-exit adopted child");
        assert!(
            status.signal().is_some(),
            "signal exit must not be flattened into a normal numeric code: {status:?}"
        );
    }

    #[test]
    fn reaped_numeric_session_identity_cannot_regain_census_authority() {
        let mut unrelated = Command::new("/bin/sleep")
            .arg("30")
            .spawn()
            .expect("spawn same-numeric unrelated sentinel");
        let pid = unrelated.id();
        let mut session = NativeSession::from_child_pid(pid).unwrap();
        session.mark_leader_reaped_for_test();

        let error = session
            .members(true)
            .expect_err("reaped numeric identity cannot regain census authority");
        assert!(error.contains("lost its waitable leader incarnation anchor"));
        assert!(
            Command::new("/bin/sh")
                .args(["-c", "kill -0 \"$1\" 2>/dev/null", "ctxmux-fixture"])
                .arg(pid.to_string())
                .status()
                .expect("probe unrelated sentinel")
                .success(),
            "same-numeric unrelated process was signalled"
        );
        let _ = unrelated.kill();
        let _ = unrelated.wait();
    }

    /// The census must see a freshly spawned session member, must not see an
    /// unrelated process, and must not be paying a host-wide walk to decide.
    ///
    /// The budget is the point of the test, not decoration. `members()` runs
    /// once per 10 ms quiescence poll, so a census that costs tens of
    /// milliseconds turns every `Stop` into a host-wide scan -- which is
    /// exactly the regression this replaced (`Stop` measured
    /// `63.6 ms + 0.0775 ms * host_process_count`).
    ///
    /// The *exclusion* is what pins the current contract. `session_candidates`
    /// descends the leader's own subtree rather than enumerating every process
    /// on the host, so an unrelated process must be absent from the candidate
    /// list. Reintroducing a host walk would make this assertion fail even
    /// though the cheap assertion above still passed.
    #[cfg(not(target_os = "macos"))]
    #[test]
    fn process_census_sees_session_members_without_walking_the_host() {
        // The subtree path is only taken by a subreaper; otherwise the census
        // falls back to the host walk and the exclusion below cannot hold.
        arm_subreaper_like_the_daemon();

        let mut sentinel = Command::new("/bin/sleep")
            .arg("30")
            .spawn()
            .expect("spawn census sentinel");
        let sentinel_pid = sentinel.id();

        // An unrelated session: spawned by us, but its own session leader, so
        // it is outside the session under test and must be pruned away.
        let mut unrelated = Command::new("setsid")
            .args(["/bin/sleep", "30"])
            .spawn()
            .expect("spawn an unrelated session leader");
        let unrelated_pid = unrelated.id();

        let own_pid = std::process::id();

        // `setsid` has not called setsid(2) at the moment `spawn` returns, so
        // until it does the child is still legitimately in OUR session and the
        // exclusion below would fail for the right reason at the wrong time.
        // Wait for the kernel to make the split real before measuring.
        await_session_leader(unrelated_pid);

        let started = Instant::now();
        let pids = super::session_candidates(Pid::from_raw(own_pid.cast_signed()).unwrap())
            .expect("enumerate session candidates");
        let elapsed = started.elapsed();

        assert!(
            pids.contains(&sentinel_pid),
            "census missed a live session member it must be able to signal"
        );
        assert!(pids.contains(&own_pid), "census missed its own process");
        assert!(
            !pids.contains(&unrelated_pid),
            "census returned process {unrelated_pid}, which leads its own \
             session; the subtree prune is gone and the walk is host-wide again"
        );
        assert!(
            elapsed < Duration::from_millis(50),
            "census took {elapsed:?}; a per-process attribute harvest is back on the Stop path"
        );

        let _ = sentinel.kill();
        let _ = sentinel.wait();
        let _ = unrelated.kill();
        let _ = unrelated.wait();
    }

    #[test]
    fn signalling_an_all_zombie_group_is_not_a_failure() {
        // Darwin answers `killpg` on a group whose members are all zombies with
        // EPERM, not ESRCH: the zombie keeps the group ID alive but owns no
        // credentials to check the signal against. Establish that kernel
        // behaviour first, so this test fails loudly if the premise ever
        // changes rather than silently proving nothing.
        let child = Command::new("/bin/sh")
            .args(["-c", "exit 0"])
            .process_group(0)
            .spawn()
            .expect("spawn a child that leads its own group and exits at once");
        let pid = child.id();
        std::mem::forget(child);
        let group = Pid::from_raw(i32::try_from(pid).unwrap()).unwrap();
        let mut session = NativeSession::from_child_pid(pid)
            .unwrap()
            .with_leader_probe_for_test(Arc::new(|| Ok(false)));

        let mut observed = None;
        for _ in 0..200 {
            std::thread::sleep(std::time::Duration::from_millis(5));
            if let Err(error) =
                rustix::process::kill_process_group(group, rustix::process::Signal::TERM)
            {
                observed = Some(error);
                break;
            }
        }
        let observed = observed.expect("an exited leader's group stops accepting signals");
        assert!(
            matches!(observed, Errno::PERM | Errno::SRCH),
            "unexpected errno {observed:?} for an all-zombie group; \
             signal_members only forgives PERM and SRCH"
        );

        // The real assertion: whichever of the two this platform reports, a Stop
        // that raced the child's own exit must not surface it as a failure.
        session
            .signal_members(rustix::process::Signal::KILL)
            .expect("signalling a session nobody is left to receive it is not a Stop failure");

        let mut adopted = AdoptedChild::from_pid(pid).unwrap();
        let _ = session.reap_leader(&mut adopted);
    }

    #[test]
    fn session_census_preserves_non_absence_lookup_errors() {
        let own_pid = std::process::id();
        let session = NativeSession::from_child_pid(own_pid)
            .unwrap()
            .with_leader_probe_for_test(Arc::new(|| Ok(false)));
        let error = session
            .classify_members(vec![own_pid], true, |_| Err(Errno::PERM))
            .expect_err("permission uncertainty cannot prove an empty session");
        assert!(error.contains("failed to classify process"));

        let absent = session
            .classify_members(vec![own_pid], true, |_| Err(Errno::SRCH))
            .expect("ESRCH is the only typed absence");
        assert!(absent.is_empty());

        let session_id = Pid::from_raw(i32::try_from(own_pid).unwrap()).unwrap();
        let present = session
            .classify_members(vec![own_pid], true, |_| Ok(session_id))
            .expect("matching SID is retained");
        assert_eq!(present, [session_id]);
    }

    #[test]
    fn stop_member_signal_rejects_a_pid_outside_the_anchored_session() {
        let mut unrelated = Command::new("/bin/sleep")
            .arg("30")
            .spawn()
            .expect("spawn unrelated sentinel");
        let unrelated_pid = Pid::from_raw(i32::try_from(unrelated.id()).unwrap()).unwrap();
        let session = NativeSession::from_child_pid(std::process::id())
            .unwrap()
            .with_leader_probe_for_test(Arc::new(|| Ok(false)));

        let error = session
            .signal_member(unrelated_pid, rustix::process::Signal::TERM)
            .expect_err("foreign session membership must fail before signal");
        assert!(error.contains("moved from native session"));
        assert!(
            Command::new("/bin/sh")
                .args(["-c", "kill -0 \"$1\" 2>/dev/null", "ctxmux-fixture"])
                .arg(unrelated.id().to_string())
                .status()
                .expect("probe unrelated sentinel")
                .success(),
            "foreign session sentinel was signalled"
        );

        let _ = unrelated.kill();
        let _ = unrelated.wait();
    }

    /// Stopping a Run whose child dies instantly must not cost a poll interval.
    ///
    /// The check right after `signal_members` races the kernel and essentially
    /// always loses, so the FIRST sleep is what the caller waits out. At a flat
    /// 10 ms that sleep *was* the operation: a whole `stop` measured 12.15 ms
    /// on a 64-core host against 0.27 ms for a Run whose child had already
    /// exited, proving the Stop machinery itself is nearly free.
    ///
    /// The budget is the point of this test. 5 ms sits well above the ~0.2 ms
    /// first backoff and well under the 10 ms flat poll it replaced, so a
    /// revert to a flat `QUIESCENCE_POLL` first sleep fails here while a loaded
    /// CI box does not.
    #[cfg(not(target_os = "macos"))]
    #[test]
    fn stopping_an_instantly_dying_child_does_not_wait_out_a_poll_interval() {
        // `setsid` makes the child a real session leader, which is the shape
        // `NativeSession` owns -- portable-pty establishes the same thing
        // before exec. A bare spawn shares our session, so `members()` cannot
        // see it and the Stop fails its emptiness requirement instead of
        // measuring anything. `setsid --wait` keeps the pid we adopt as the
        // leader's, and `sleep` has no SIGTERM handler so it dies at once.
        let child = Command::new("setsid")
            .args(["--wait", "/bin/sleep", "600"])
            .spawn()
            .expect("spawn quiescence timing child");
        let pid = child.id();
        std::mem::forget(child);

        // Wait for the kernel to make the child its own session leader; until
        // then the census cannot attribute it to this session.
        await_session_leader(pid);

        let mut session = NativeSession::from_child_pid(pid).unwrap();
        let mut adopted = AdoptedChild::from_pid(pid).unwrap();

        let started = Instant::now();
        let (disposition, _status) = session
            .stop(
                &mut adopted,
                Duration::from_millis(500),
                Duration::from_millis(500),
            )
            .expect("stop the child");
        let elapsed = started.elapsed();

        assert_eq!(
            disposition,
            ctxmux_protocol::StopDisposition::Graceful,
            "an unhandled SIGTERM ends the child in the graceful phase"
        );
        assert!(
            elapsed < Duration::from_millis(5),
            "stop took {elapsed:?} for a child that dies on SIGTERM; the first \
             quiescence sleep is back to a flat poll interval"
        );
    }

    /// A descendant that left the process group must still be stopped.
    ///
    /// `signal_members` signals the process GROUP in one syscall rather than
    /// walking `/proc` to enumerate the session, which is what makes a Stop
    /// cost one census instead of two. The group is not the session: a
    /// descendant that called `setpgid` leaves the group while remaining
    /// session-owned, so `kill(-leader)` cannot reach it and only the straggler
    /// sweep in `wait_quiescent` does.
    ///
    /// This is the falsifier for that sweep. Deleting it leaves the escapee
    /// alive, `members()` never empties, and the Stop fails its emptiness
    /// requirement -- so the test goes red rather than silently weakening the
    /// whole-session guarantee to tmux's leader-only one.
    #[cfg(not(target_os = "macos"))]
    #[test]
    fn stopping_reaches_a_descendant_that_left_the_process_group() {
        // The escapee has to call `setpgid` itself: a shell only moves a
        // background job into its own group when job control is on, and a
        // session with no controlling terminal turns it off ("can't access
        // tty"), so `set -m` yields no escapee here. `setsid --wait` makes the
        // outer shell the session leader we adopt, as portable-pty does before
        // exec.
        let child = Command::new("setsid")
            .args([
                "--wait",
                "/bin/sh",
                "-c",
                "python3 -c 'import os,time; os.setpgid(0,0); time.sleep(600)' & \
                 /bin/sleep 600",
            ])
            .spawn()
            .expect("spawn a leader that puts a child in its own group");
        let pid = child.id();
        std::mem::forget(child);

        await_session_leader(pid);

        let mut session = NativeSession::from_child_pid(pid).unwrap();

        // Find the escapee: session-owned, but in a different process group.
        let mut escapee = None;
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            let members = session.members(false).unwrap_or_default();
            escapee = members.into_iter().find(|member| {
                rustix::process::getpgid(Some(*member))
                    .is_ok_and(|group| group.as_raw_pid().unsigned_abs() != pid)
            });
            if escapee.is_some() {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let escapee = escapee.expect(
            "the fixture must produce a session member outside the leader's \
             process group, or it is not exercising the gap this test covers",
        );

        let mut adopted = AdoptedChild::from_pid(pid).unwrap();
        session
            .stop(
                &mut adopted,
                Duration::from_millis(500),
                Duration::from_secs(2),
            )
            .expect("stop must drain the whole session, group escapees included");

        // A successful Stop asserts the session is empty; prove the escapee is
        // actually gone rather than trusting the disposition.
        assert!(
            super::getsid(Some(escapee)).is_err(),
            "process {} left the leader's process group and survived the Stop; \
             the straggler sweep in wait_quiescent is gone",
            escapee.as_raw_pid()
        );
    }

    /// A descendant orphaned mid-session must still be counted as a member.
    ///
    /// `session_candidates` answers "is this session empty?" from the leader's
    /// own subtree instead of walking every process on the host. The two are
    /// only equivalent while every session member is reachable from this
    /// process. An orphan is the case that can break it: when its parent exits
    /// first, it reparents away -- to init on a normal process, leaving the
    /// subtree while *staying in the session*. The host walk still saw it; a
    /// subtree sweep would not.
    ///
    /// What closes the gap is `PR_SET_CHILD_SUBREAPER` (armed by the daemon in
    /// `become_child_subreaper`), which makes the orphan reparent to *us*, plus
    /// the daemon-children level `session_candidates` sweeps for exactly this
    /// reason. This test is the falsifier for both: drop either, and the
    /// orphan stops being enumerable while still holding the session open.
    ///
    /// The test process stands in for the daemon, so it arms the subreaper bit
    /// itself -- `cargo test` is not the daemon and never calls `serve`.
    #[cfg(not(target_os = "macos"))]
    #[test]
    fn a_member_orphaned_by_its_parent_is_still_enumerated() {
        arm_subreaper_like_the_daemon();

        // The inner shell forks a sleep and exits immediately, orphaning it.
        // `setsid --wait` gives us a session leader to adopt, as portable-pty
        // does before exec; the leader stays alive so the session persists.
        let child = Command::new("setsid")
            .args([
                "--wait",
                "/bin/sh",
                "-c",
                "/bin/sh -c '/bin/sleep 600 & exit 0'; /bin/sleep 600",
            ])
            .spawn()
            .expect("spawn a leader whose grandchild gets orphaned");
        let pid = child.id();
        std::mem::forget(child);

        await_session_leader(pid);

        let mut session = NativeSession::from_child_pid(pid).unwrap();

        // The orphan: session-owned, but its parent is no longer the leader.
        // Identified by ppid rather than by pgid, which is what separates this
        // from the group-escapee case above.
        let mut orphan = None;
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            let members = session.members(false).unwrap_or_default();
            orphan = members.into_iter().find(|member| {
                parent_of(member.as_raw_pid()).is_some_and(|parent| parent != pid.cast_signed())
            });
            if orphan.is_some() {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let orphan = orphan.expect(
            "the fixture must produce a session member whose parent is not the \
             leader, or it is not exercising the gap this test covers",
        );

        let mut adopted = AdoptedChild::from_pid(pid).unwrap();
        session
            .stop(
                &mut adopted,
                Duration::from_millis(500),
                Duration::from_secs(2),
            )
            .expect("stop must drain the whole session, orphans included");

        assert!(
            super::getsid(Some(orphan)).is_err(),
            "process {} was orphaned out of the leader's subtree and survived \
             the Stop; the session sweep no longer sees orphaned members",
            orphan.as_raw_pid()
        );
    }

    /// The parent pid of `pid`, read from `/proc/<pid>/stat`.
    ///
    /// The comm field can contain spaces and parentheses, so the fields after
    /// it are located from the LAST `)` rather than by splitting the line.
    #[cfg(not(target_os = "macos"))]
    fn parent_of(pid: i32) -> Option<i32> {
        let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
        let tail = &stat[stat.rfind(')')? + 1..];
        tail.split_ascii_whitespace().nth(1)?.parse().ok()
    }

    /// Block until `pid` leads its own session, or five seconds elapse.
    ///
    /// `setsid` has not called `setsid(2)` at the moment `spawn` returns, so a
    /// fixture that measures immediately sees the child still in the test's own
    /// session -- failing for the right reason at the wrong time. Returning on
    /// timeout rather than panicking leaves the assertion that follows to say
    /// what actually went wrong.
    #[cfg(not(target_os = "macos"))]
    fn await_session_leader(pid: u32) {
        let ready = Instant::now();
        while ready.elapsed() < Duration::from_secs(5) {
            if super::getsid(Pid::from_raw(pid.cast_signed()))
                .is_ok_and(|sid| sid.as_raw_pid().unsigned_abs() == pid)
            {
                return;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    /// Put the test process in the same shape as a running daemon.
    ///
    /// `serve` arms this at startup; `cargo test` never calls `serve`, so a
    /// test that wants the subtree census has to arm it itself. Naming our own
    /// pid is what SETS the attribute -- `None` maps to 0, which clears it.
    #[cfg(not(target_os = "macos"))]
    fn arm_subreaper_like_the_daemon() {
        rustix::process::set_child_subreaper(Some(rustix::process::getpid()))
            .expect("arm the subreaper bit the subtree census depends on");
    }
}
