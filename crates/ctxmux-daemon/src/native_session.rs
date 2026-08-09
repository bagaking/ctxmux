//! POSIX session ownership for one native Run.

use std::{
    thread,
    time::{Duration, Instant},
};

use portable_pty::{Child, ExitStatus};
use rustix::{
    io::Errno,
    process::{Pid, Signal, getpgid, getsid, kill_process, kill_process_group},
};
use sysinfo::{ProcessesToUpdate, System};

const QUIESCENCE_POLL: Duration = Duration::from_millis(10);

/// One native Run's kernel-owned session identity.
///
/// `portable-pty` establishes the spawned child as a session leader before
/// exec, so the direct child PID is also the Run SID and initial PGID.
#[derive(Clone, Copy, Debug)]
pub(crate) struct NativeSession {
    id: Pid,
}

impl NativeSession {
    pub(crate) fn from_child_pid(pid: u32) -> Result<Self, String> {
        let raw = i32::try_from(pid)
            .map_err(|_| format!("native child PID {pid} does not fit a POSIX process ID"))?;
        let id = Pid::from_raw(raw)
            .ok_or_else(|| format!("native child PID {pid} is not a positive process ID"))?;
        Ok(Self { id })
    }

    /// Deliver SIGINT to the current foreground process group after proving
    /// that group still belongs to this Run's session.
    pub(crate) fn interrupt(self, foreground_group: u32) -> Result<(), String> {
        let raw = i32::try_from(foreground_group).map_err(|_| {
            format!("foreground process group {foreground_group} does not fit a POSIX process ID")
        })?;
        let group = Pid::from_raw(raw).ok_or_else(|| {
            format!("foreground process group {foreground_group} is not a positive process ID")
        })?;
        let member = self
            .members()?
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
        kill_process_group(group, Signal::INT).map_err(|error| {
            format!("failed to interrupt foreground process group {foreground_group}: {error}")
        })
    }

    /// Gracefully terminate, then force, every process still in the owned
    /// session. Success includes direct-child reap and an empty session.
    pub(crate) fn stop(
        self,
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
        self,
        child: &mut (dyn Child + Send + Sync),
        status: ExitStatus,
        deadline: Instant,
    ) -> Result<ExitStatus, String> {
        if self.members()?.is_empty() {
            return Ok(status);
        }
        self.signal_members(Signal::KILL)?;
        while Instant::now() < deadline {
            if self.members()?.is_empty() {
                return Ok(status);
            }
            thread::sleep(QUIESCENCE_POLL.min(deadline.saturating_duration_since(Instant::now())));
            // Retain the child-handle owner contract even though the cached
            // terminal status has already been observed.
            let _ = child.try_wait();
        }
        Err(format!(
            "native session {} retained descendants after direct-child exit",
            self.id.as_raw_pid()
        ))
    }

    fn wait_quiescent(
        self,
        child: &mut (dyn Child + Send + Sync),
        deadline: Instant,
    ) -> Result<Option<ExitStatus>, String> {
        let mut child_status = None;
        loop {
            if child_status.is_none() {
                child_status = child.try_wait().map_err(|error| {
                    format!("failed to wait for native session leader: {error}")
                })?;
            }
            if child_status.is_some() && self.members()?.is_empty() {
                return Ok(child_status);
            }
            if Instant::now() >= deadline {
                return Ok(None);
            }
            thread::sleep(QUIESCENCE_POLL.min(deadline.saturating_duration_since(Instant::now())));
        }
    }

    fn signal_members(self, signal: Signal) -> Result<(), String> {
        let mut failures = Vec::new();
        for pid in self.members()? {
            match getsid(Some(pid)) {
                Ok(session) if session == self.id => {
                    if let Err(error) = kill_process(pid, signal)
                        && error != Errno::SRCH
                    {
                        failures.push(format!(
                            "failed to signal native session member {}: {error}",
                            pid.as_raw_pid()
                        ));
                    }
                }
                Ok(session) => failures.push(format!(
                    "process {} moved from native session {} to {} before signal",
                    pid.as_raw_pid(),
                    self.id.as_raw_pid(),
                    session.as_raw_pid()
                )),
                Err(Errno::SRCH) => {}
                Err(error) => failures.push(format!(
                    "failed to revalidate native session member {}: {error}",
                    pid.as_raw_pid()
                )),
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(failures.join("; "))
        }
    }

    fn verify_member(self, pid: Pid) -> Result<(), String> {
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

    fn members(self) -> Result<Vec<Pid>, String> {
        let mut system = System::new();
        system.refresh_processes(ProcessesToUpdate::All, true);
        let mut members = Vec::new();
        for (process_pid, process) in system.processes() {
            if process.session_id().map(sysinfo::Pid::as_u32)
                != Some(self.id.as_raw_pid().cast_unsigned())
            {
                continue;
            }
            let raw = i32::try_from(process_pid.as_u32()).map_err(|_| {
                format!("observed process ID {process_pid} does not fit POSIX pid_t")
            })?;
            let Some(pid) = Pid::from_raw(raw) else {
                continue;
            };
            members.push(pid);
        }
        Ok(members)
    }
}
