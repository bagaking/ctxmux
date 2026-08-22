//! POSIX session ownership for one native Run.

use std::{
    thread,
    time::{Duration, Instant},
};

use portable_pty::{Child, ChildKiller, ExitStatus};
#[cfg(not(target_os = "macos"))]
use rustix::process::{getpgid, kill_process_group};
use rustix::{
    io::Errno,
    process::{Pid, Signal, WaitId, WaitIdOptions, WaitIdStatus, getsid, kill_process, waitid},
};
#[cfg(not(target_os = "macos"))]
use sysinfo::{ProcessesToUpdate, System};

const QUIESCENCE_POLL: Duration = Duration::from_millis(10);

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
        if let Some(status) = self.wait_quiescent(child, Instant::now() + graceful)? {
            return Ok((ctxmux_protocol::StopDisposition::Graceful, status));
        }

        self.signal_members(Signal::KILL)?;
        self.wait_quiescent(child, Instant::now() + forced)?
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
        while Instant::now() < deadline {
            if self.members(false)?.is_empty() {
                return self
                    .reap_leader(child)
                    .map(|status| (status, ctxmux_protocol::StopDisposition::Forced));
            }
            thread::sleep(QUIESCENCE_POLL.min(deadline.saturating_duration_since(Instant::now())));
        }
        Err(format!(
            "native session {} retained descendants after direct-child exit",
            self.id.as_raw_pid()
        ))
    }

    fn wait_quiescent(
        &mut self,
        child: &mut (dyn Child + Send + Sync),
        deadline: Instant,
    ) -> Result<Option<ExitStatus>, String> {
        loop {
            if self.leader_is_terminal()? && self.members(false)?.is_empty() {
                return self.reap_leader(child).map(Some);
            }
            if Instant::now() >= deadline {
                return Ok(None);
            }
            thread::sleep(QUIESCENCE_POLL.min(deadline.saturating_duration_since(Instant::now())));
        }
    }

    fn signal_members(&self, signal: Signal) -> Result<(), String> {
        let mut failures = Vec::new();
        let leader_terminal = self.leader_is_terminal()?;
        for pid in self.members(!leader_terminal)? {
            if let Err(error) = self.signal_member(pid, signal) {
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

    fn members(&self, include_leader: bool) -> Result<Vec<Pid>, String> {
        self.require_waitable_anchor()?;
        self.classify_members(process_ids()?, include_leader, |pid| getsid(Some(pid)))
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
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug)]
pub(crate) struct AdoptedChild {
    pid: Pid,
    reaped: Option<ExitStatus>,
}

impl AdoptedChild {
    #[cfg_attr(not(test), allow(dead_code))]
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

/// Encode a `waitid` result as a `portable_pty::ExitStatus`, preferring the
/// normal exit code and falling back to the conventional `128 + signal`.
///
/// The final `1` is a defensive default: reaped with `WEXITED`, a terminal
/// child is always `CLD_EXITED` or `CLD_KILLED`/`CLD_DUMPED`, so one of the two
/// branches above fires — this arm should never surface in practice.
fn exit_status_from_waitid(status: &WaitIdStatus) -> ExitStatus {
    if let Some(code) = status.exit_status() {
        ExitStatus::with_exit_code(code.unsigned_abs())
    } else if let Some(signal) = status.terminating_signal() {
        ExitStatus::with_exit_code(128 + signal.unsigned_abs())
    } else {
        ExitStatus::with_exit_code(1)
    }
}

#[cfg(target_os = "macos")]
fn process_ids() -> Result<Vec<u32>, String> {
    ctxmux_process_stats::process_ids()
        .map_err(|error| format!("failed to enumerate native session members: {error}"))
}

#[cfg(not(target_os = "macos"))]
fn process_ids() -> Result<Vec<u32>, String> {
    let mut system = System::new();
    system.refresh_processes(ProcessesToUpdate::All, true);
    Ok(system
        .processes()
        .keys()
        .map(sysinfo::Pid::as_u32)
        .collect())
}

#[cfg(test)]
mod tests {
    use std::{process::Command, sync::Arc};

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
}
