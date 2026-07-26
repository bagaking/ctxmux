use std::{
    fmt,
    io::{self, Read},
    os::unix::process::CommandExt,
    process::{Child, Command, ExitStatus, Stdio},
    thread,
    time::{Duration, Instant},
};

use rustix::{
    event::{PollFd, PollFlags, Timespec, poll},
    fs::{OFlags, fcntl_getfl, fcntl_setfl},
    io::Errno,
    process::{Pid, Signal, kill_process_group},
};
use thiserror::Error;

const READ_BUFFER_BYTES: usize = 8 * 1024;
const CHILD_STATUS_POLL: Duration = Duration::from_millis(10);

#[derive(Clone, Copy, Debug)]
pub(super) struct CaptureLimits {
    pub(super) stdout_bytes: usize,
    pub(super) stderr_bytes: usize,
}

#[derive(Debug)]
pub(super) struct BoundedOutput {
    pub(super) status: ExitStatus,
    pub(super) stdout: Vec<u8>,
    pub(super) stderr: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Stream {
    Stdout,
    Stderr,
}

impl fmt::Display for Stream {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Stdout => "stdout",
            Self::Stderr => "stderr",
        })
    }
}

#[derive(Debug, Error)]
pub(super) enum ShortCommandError {
    #[error("failed to spawn helper: {0}")]
    Spawn(#[source] io::Error),
    #[error("helper did not expose its {0} pipe")]
    MissingPipe(Stream),
    #[error("failed to configure helper {stream}: {source}")]
    PipeSetup {
        stream: Stream,
        #[source]
        source: io::Error,
    },
    #[error("failed to poll helper output: {0}")]
    Poll(#[source] io::Error),
    #[error("helper output poll reported an invalid file descriptor")]
    InvalidPollFd,
    #[error("failed to read helper {stream}: {source}")]
    Read {
        stream: Stream,
        #[source]
        source: io::Error,
    },
    #[error("helper exceeded its execution deadline")]
    Timeout,
    #[error("helper {stream} exceeded its {limit}-byte capture limit")]
    Overflow { stream: Stream, limit: usize },
    #[error("failed to query or wait for helper: {0}")]
    Wait(#[source] io::Error),
    #[error("{primary}; cleanup failed: {detail}")]
    Cleanup { primary: Box<Self>, detail: String },
}

pub(super) fn run(
    command: &mut Command,
    deadline: Instant,
    limits: CaptureLimits,
) -> Result<BoundedOutput, ShortCommandError> {
    if Instant::now() >= deadline {
        return Err(ShortCommandError::Timeout);
    }
    command
        .process_group(0)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().map_err(ShortCommandError::Spawn)?;
    let process_group = Pid::from_child(&child);
    let result = capture_to_exit(&mut child, deadline, limits);
    match result {
        Ok(output) => Ok(output),
        Err(primary) => match terminate_and_reap(&mut child, process_group) {
            Ok(()) => Err(primary),
            Err(detail) => Err(ShortCommandError::Cleanup {
                primary: Box::new(primary),
                detail,
            }),
        },
    }
}

fn capture_to_exit(
    child: &mut Child,
    deadline: Instant,
    limits: CaptureLimits,
) -> Result<BoundedOutput, ShortCommandError> {
    let mut stdout = Some(
        child
            .stdout
            .take()
            .ok_or(ShortCommandError::MissingPipe(Stream::Stdout))?,
    );
    let mut stderr = Some(
        child
            .stderr
            .take()
            .ok_or(ShortCommandError::MissingPipe(Stream::Stderr))?,
    );
    set_nonblocking(stdout.as_ref().expect("stdout is present"), Stream::Stdout)?;
    set_nonblocking(stderr.as_ref().expect("stderr is present"), Stream::Stderr)?;

    let mut stdout_bytes = Vec::new();
    let mut stderr_bytes = Vec::new();
    let mut status = None;

    loop {
        drain_stream(
            &mut stdout,
            &mut stdout_bytes,
            limits.stdout_bytes,
            Stream::Stdout,
        )?;
        drain_stream(
            &mut stderr,
            &mut stderr_bytes,
            limits.stderr_bytes,
            Stream::Stderr,
        )?;

        if Instant::now() >= deadline {
            return Err(ShortCommandError::Timeout);
        }
        if status.is_none() && stdout.is_none() && stderr.is_none() {
            status = child.try_wait().map_err(ShortCommandError::Wait)?;
        }
        if let Some(status) = status
            && stdout.is_none()
            && stderr.is_none()
        {
            return Ok(BoundedOutput {
                status,
                stdout: stdout_bytes,
                stderr: stderr_bytes,
            });
        }

        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            return Err(ShortCommandError::Timeout);
        };
        if remaining.is_zero() {
            return Err(ShortCommandError::Timeout);
        }

        let mut poll_fds = Vec::with_capacity(2);
        if let Some(reader) = &stdout {
            poll_fds.push(PollFd::new(reader, PollFlags::IN));
        }
        if let Some(reader) = &stderr {
            poll_fds.push(PollFd::new(reader, PollFlags::IN));
        }
        if poll_fds.is_empty() {
            thread::sleep(CHILD_STATUS_POLL.min(remaining));
            continue;
        }

        let timeout = Timespec::try_from(remaining).expect("short command deadline fits Timespec");
        match poll(&mut poll_fds, Some(&timeout)) {
            Ok(_) => {
                if poll_fds
                    .iter()
                    .any(|fd| fd.revents().contains(PollFlags::NVAL))
                {
                    return Err(ShortCommandError::InvalidPollFd);
                }
            }
            Err(Errno::INTR) => {}
            Err(error) => return Err(ShortCommandError::Poll(error.into())),
        }
    }
}

fn set_nonblocking(
    reader: &(impl std::os::fd::AsFd + ?Sized),
    stream: Stream,
) -> Result<(), ShortCommandError> {
    let flags = fcntl_getfl(reader).map_err(|source| ShortCommandError::PipeSetup {
        stream,
        source: source.into(),
    })?;
    fcntl_setfl(reader, flags | OFlags::NONBLOCK).map_err(|source| ShortCommandError::PipeSetup {
        stream,
        source: source.into(),
    })
}

fn drain_stream<R: Read>(
    reader: &mut Option<R>,
    captured: &mut Vec<u8>,
    limit: usize,
    stream: Stream,
) -> Result<(), ShortCommandError> {
    let Some(open_reader) = reader else {
        return Ok(());
    };
    let mut buffer = [0_u8; READ_BUFFER_BYTES];
    loop {
        match open_reader.read(&mut buffer) {
            Ok(0) => {
                *reader = None;
                return Ok(());
            }
            Ok(count) if count <= limit.saturating_sub(captured.len()) => {
                captured.extend_from_slice(&buffer[..count]);
            }
            Ok(_) => return Err(ShortCommandError::Overflow { stream, limit }),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(()),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(source) => return Err(ShortCommandError::Read { stream, source }),
        }
    }
}

fn terminate_and_reap(child: &mut Child, process_group: Pid) -> Result<(), String> {
    let mut failures = Vec::new();
    let initial_group_error = match kill_process_group(process_group, Signal::KILL) {
        Ok(()) | Err(Errno::SRCH) => None,
        Err(error) => Some(error),
    };
    let direct_kill_error = child.kill().err();
    let unresolved_group_error = if let Some(initial_error) = initial_group_error {
        match kill_process_group(process_group, Signal::KILL) {
            Ok(()) | Err(Errno::SRCH) => None,
            Err(retry_error) => Some((initial_error, retry_error)),
        }
    } else {
        None
    };
    match child.wait() {
        Ok(_) => {
            if let Some((initial_error, retry_error)) = unresolved_group_error
                && !(initial_error == Errno::PERM && retry_error == Errno::PERM)
            {
                failures.push(format!(
                    "failed to kill helper process group: {initial_error}; retry before direct-child reap failed: {retry_error}"
                ));
            }
            // macOS reports EPERM when the group contains only an exited,
            // unreaped leader. Both group attempts happened while that leader
            // still anchored the PGID, and the direct child is now reaped.
        }
        Err(wait_error) => {
            if let Some((initial_error, retry_error)) = unresolved_group_error {
                failures.push(format!(
                    "failed to kill helper process group: {initial_error}; retry before direct-child reap failed: {retry_error}"
                ));
            }
            if let Some(kill_error) = direct_kill_error {
                failures.push(format!("failed to kill direct helper child: {kill_error}"));
            }
            failures.push(format!("failed to reap direct helper child: {wait_error}"));
        }
    }

    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("; "))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        process::Command,
        thread,
        time::{Duration, Instant},
    };

    use rustix::process::{Pid, test_kill_process};

    use super::{CaptureLimits, ShortCommandError, Stream, capture_to_exit, run};

    const TEST_LIMITS: CaptureLimits = CaptureLimits {
        stdout_bytes: 4096,
        stderr_bytes: 4096,
    };

    #[test]
    fn captures_exact_stdout_stderr_and_status() {
        let mut command = Command::new("/bin/sh");
        command.args(["-c", "printf stdout; printf stderr >&2; exit 7"]);
        let output = run(
            &mut command,
            Instant::now() + Duration::from_secs(2),
            TEST_LIMITS,
        )
        .expect("capture bounded helper output");
        assert_eq!(output.status.code(), Some(7));
        assert_eq!(output.stdout, b"stdout");
        assert_eq!(output.stderr, b"stderr");
    }

    #[test]
    fn helper_starts_in_its_own_process_group() {
        let mut command = Command::new("/bin/sh");
        command.args(["-c", "printf '%s ' $$; ps -o pgid= -p $$"]);
        let output = run(
            &mut command,
            Instant::now() + Duration::from_secs(2),
            TEST_LIMITS,
        )
        .expect("inspect helper process group");
        let ids = String::from_utf8(output.stdout)
            .expect("process IDs are UTF-8")
            .split_whitespace()
            .map(|value| value.parse::<u32>().expect("parse process ID"))
            .collect::<Vec<_>>();
        assert_eq!(ids.len(), 2);
        assert_eq!(ids[0], ids[1], "helper must own its process group");
    }

    #[test]
    fn exact_capture_limits_succeed_without_truncation() {
        let mut command = Command::new("/bin/sh");
        command.args(["-c", "printf 1234; printf abcd >&2"]);
        let output = run(
            &mut command,
            Instant::now() + Duration::from_secs(2),
            CaptureLimits {
                stdout_bytes: 4,
                stderr_bytes: 4,
            },
        )
        .expect("accept exact stream capture limits");
        assert_eq!(output.stdout, b"1234");
        assert_eq!(output.stderr, b"abcd");
    }

    #[test]
    fn stdout_limit_plus_one_fails_without_truncated_success() {
        assert_overflow("printf 12345", Stream::Stdout);
    }

    #[test]
    fn stderr_limit_plus_one_fails_without_truncated_success() {
        assert_overflow("printf 12345 >&2", Stream::Stderr);
    }

    #[test]
    fn zero_capture_limit_rejects_the_first_byte() {
        let mut command = Command::new("/bin/sh");
        command.args(["-c", "printf x"]);
        let error = run(
            &mut command,
            Instant::now() + Duration::from_secs(2),
            CaptureLimits {
                stdout_bytes: 0,
                stderr_bytes: 0,
            },
        )
        .expect_err("reject the first byte above a zero limit");
        assert!(
            matches!(
                error,
                ShortCommandError::Overflow {
                    stream: Stream::Stdout,
                    limit: 0
                }
            ),
            "unexpected failure: {error}"
        );
    }

    #[test]
    fn missing_executable_is_a_spawn_failure() {
        let mut command = Command::new("/ctxmux/definitely-missing-short-command");
        let error = run(
            &mut command,
            Instant::now() + Duration::from_secs(2),
            TEST_LIMITS,
        )
        .expect_err("missing helper must fail at spawn");
        assert!(
            matches!(error, ShortCommandError::Spawn(_)),
            "unexpected failure: {error}"
        );
    }

    #[test]
    fn expired_deadline_returns_before_reaping_the_completed_leader() {
        let mut command = Command::new("/bin/sh");
        command
            .args(["-c", "printf completed"])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        let mut child = command.spawn().expect("spawn completed helper fixture");
        let deadline = Instant::now();
        thread::sleep(Duration::from_millis(50));
        let error = capture_to_exit(&mut child, deadline, TEST_LIMITS)
            .expect_err("completion after the owner deadline must fail");
        assert!(matches!(error, ShortCommandError::Timeout), "{error}");
        assert!(
            test_kill_process(Pid::from_child(&child)).is_ok(),
            "deadline failure must retain the leader PID anchor for outer cleanup"
        );
        child.wait().expect("reap completed helper fixture");
    }

    #[test]
    fn finite_dual_pipe_pressure_drains_without_deadlock() {
        const STREAM_BYTES: usize = 128 * 1024;
        let mut command = Command::new("/bin/sh");
        command.args([
            "-c",
            "i=0; while [ \"$i\" -lt 8192 ]; do printf 0123456789abcdef; printf fedcba9876543210 >&2; i=$((i + 1)); done",
        ]);
        let output = run(
            &mut command,
            Instant::now() + Duration::from_secs(5),
            CaptureLimits {
                stdout_bytes: STREAM_BYTES,
                stderr_bytes: STREAM_BYTES,
            },
        )
        .expect("drain finite pressure from both helper pipes");
        assert!(output.status.success());
        assert_eq!(output.stdout.len(), STREAM_BYTES);
        assert_eq!(output.stderr.len(), STREAM_BYTES);
    }

    #[test]
    fn simultaneous_pipe_pressure_fails_on_a_capture_limit() {
        let directory = tempfile::tempdir().expect("create overflow fixture directory");
        let direct_pid_path = directory.path().join("direct.pid");
        let script = format!(
            "printf '%s' $$ > '{}'; while :; do printf 0123456789abcdef; printf fedcba9876543210 >&2; done",
            direct_pid_path.display()
        );
        let mut command = Command::new("/bin/sh");
        command.args(["-c", &script]);
        let error = run(
            &mut command,
            Instant::now() + Duration::from_secs(2),
            TEST_LIMITS,
        )
        .expect_err("reject unbounded helper output");
        assert!(
            matches!(
                error,
                ShortCommandError::Overflow {
                    stream: Stream::Stdout | Stream::Stderr,
                    limit: 4096
                }
            ),
            "unexpected failure: {error}"
        );
        assert_process_gone(read_pid(&direct_pid_path));
    }

    #[test]
    fn timeout_kills_same_group_descendant_holding_pipes() {
        let directory = tempfile::tempdir().expect("create short-command fixture directory");
        let direct_pid_path = directory.path().join("direct.pid");
        let descendant_pid_path = directory.path().join("descendant.pid");
        let script = format!(
            "printf '%s' $$ > '{}'; sleep 30 & printf '%s' $! > '{}'; exit 0",
            direct_pid_path.display(),
            descendant_pid_path.display()
        );
        let mut command = Command::new("/bin/sh");
        command.args(["-c", &script]);
        let error = run(
            &mut command,
            Instant::now() + Duration::from_secs(2),
            TEST_LIMITS,
        )
        .expect_err("inherited pipes must not outlive the deadline");
        assert!(matches!(error, ShortCommandError::Timeout), "{error}");

        let direct_pid = read_pid(&direct_pid_path);
        let descendant_pid = read_pid(&descendant_pid_path);
        assert_process_gone(direct_pid);
        assert_process_gone(descendant_pid);
    }

    #[test]
    fn timeout_kills_and_reaps_live_direct_child() {
        let directory = tempfile::tempdir().expect("create direct-child fixture directory");
        let direct_pid_path = directory.path().join("direct.pid");
        let script = format!(
            "printf '%s' $$ > '{}'; exec sleep 30",
            direct_pid_path.display()
        );
        let mut command = Command::new("/bin/sh");
        command.args(["-c", &script]);
        let error = run(
            &mut command,
            Instant::now() + Duration::from_secs(2),
            TEST_LIMITS,
        )
        .expect_err("live direct child must not outlive the deadline");
        assert!(matches!(error, ShortCommandError::Timeout), "{error}");
        assert_process_gone(read_pid(&direct_pid_path));
    }

    fn assert_overflow(script: &str, expected_stream: Stream) {
        let mut command = Command::new("/bin/sh");
        command.args(["-c", script]);
        let error = run(
            &mut command,
            Instant::now() + Duration::from_secs(2),
            CaptureLimits {
                stdout_bytes: 4,
                stderr_bytes: 4,
            },
        )
        .expect_err("reject capture limit plus one");
        assert!(
            matches!(
                error,
                ShortCommandError::Overflow { stream, limit: 4 }
                    if stream == expected_stream
            ),
            "unexpected failure: {error}"
        );
    }

    fn read_pid(path: &std::path::Path) -> Pid {
        let raw = fs::read_to_string(path)
            .expect("read fixture PID")
            .parse()
            .expect("parse fixture PID");
        Pid::from_raw(raw).expect("fixture PID is nonzero")
    }

    fn assert_process_gone(pid: Pid) {
        for _ in 0..100 {
            if test_kill_process(pid).is_err() {
                return;
            }
            thread::sleep(Duration::from_millis(10));
        }
        panic!("fixture process {pid} remained alive");
    }
}
