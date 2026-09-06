//! Real open-full/closed diagnostic receivers through the public protocol.
//! Every daemon and child here is private and attributed; no production Runs.
use std::{
    collections::BTreeMap,
    fmt::Debug,
    fs::File,
    future::Future,
    os::fd::OwnedFd,
    process::{Child, Command, Stdio},
    time::Duration,
};

use ctxmux_client::Client;
use ctxmux_protocol::{
    DiagnosticsSinkState, DiagnosticsSnapshot, NativeOutputStatus, NativeOwnerStatus, RunId,
    RunInfo, RunSpec, TerminalSize,
};
use ctxmux_test_support::daemon_spawn_permit;
use rustix::fs::{OFlags, fcntl_getfl, fcntl_setfl};
use rustix::io::Errno;
use rustix::process::{Pid, Signal, kill_process, kill_process_group, test_kill_process};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::{sleep, timeout};

// The same two-second arrival budget used by the controlled public proof.
// This is a test failure deadline, never a product capacity or retry policy.
const ARRIVAL_BUDGET: Duration = Duration::from_secs(2);
const POLL_INTERVAL: Duration = Duration::from_millis(10);

struct PrivateDaemon {
    child: Child,
    directory: TempDir,
    child_sessions: Vec<u32>,
    receiver: Option<OwnedFd>,
    inherited_flags: OFlags,
    writer_inspector: OwnedFd,
    filled_bytes: usize,
}

impl PrivateDaemon {
    async fn start(full: bool, nonblocking: bool) -> (Self, Client) {
        let permit = daemon_spawn_permit().await;
        let directory = tempfile::tempdir().expect("private daemon directory");
        let socket = directory.path().join("ctxmux.sock");
        let (reader, writer) = rustix::pipe::pipe().unwrap();
        rustix::io::fcntl_setfd(&reader, rustix::io::FdFlags::CLOEXEC).unwrap();
        rustix::io::fcntl_setfd(&writer, rustix::io::FdFlags::CLOEXEC).unwrap();
        let inherited_flags = fcntl_getfl(&writer).unwrap();
        fcntl_setfl(&reader, fcntl_getfl(&reader).unwrap() | OFlags::NONBLOCK).unwrap();
        let mut filled_bytes = 0;
        if full {
            // Only this newly created private pipe is changed before inheritance.
            // Measure actual capacity; no guessed kernel pipe size.
            fcntl_setfl(&writer, inherited_flags | OFlags::NONBLOCK).unwrap();
            loop {
                match rustix::io::write(&writer, b"F") {
                    Ok(1) => filled_bytes += 1,
                    Err(Errno::AGAIN) => break,
                    other => panic!("private pipe fill: {other:?}"),
                }
            }
            fcntl_setfl(&writer, inherited_flags).unwrap();
            assert!(filled_bytes > 0);
        }
        if nonblocking {
            // Preserve a legitimate inherited status policy; the logger may
            // wait on readiness only in its existing dedicated writer.
            fcntl_setfl(&writer, fcntl_getfl(&writer).unwrap() | OFlags::NONBLOCK).unwrap();
        }
        // Baseline belongs immediately before inheritance, after all fixture
        // writes and flag restoration; host-visible flags can reflect filling.
        let inherited_flags = fcntl_getfl(&writer).unwrap();
        let writer_inspector = rustix::io::fcntl_dupfd_cloexec(&writer, 0).unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_ctxmuxd"))
            .arg("--socket")
            .arg(&socket)
            // Hostile logger policy is independent of admitted Run workload.
            .arg("--resource-limits")
            .arg(r#"{"diagnostic_queue_bytes":1024,"diagnostic_record_bytes":512}"#)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::from(File::from(writer)))
            .spawn()
            .expect("spawn build-owned memory-only daemon");
        let mut daemon = Self {
            child,
            directory,
            child_sessions: Vec::new(),
            receiver: Some(reader),
            inherited_flags,
            writer_inspector,
            filled_bytes,
        };
        let probe = Client::new(&socket);
        let mut last_ping_error = None;
        let readiness = timeout(ARRIVAL_BUDGET, async {
            loop {
                assert!(
                    daemon.child.try_wait().unwrap().is_none(),
                    "daemon alive during startup"
                );
                match probe.ping().await {
                    Ok(()) => break,
                    Err(error) => last_ping_error = Some(error.to_string()),
                }
                sleep(POLL_INTERVAL).await;
            }
        })
        .await;
        if readiness.is_err() {
            daemon.fail_readiness(&socket, last_ping_error.as_deref());
        }
        drop(permit);
        (daemon, probe)
    }

    fn fail_readiness(&mut self, socket: &std::path::Path, last_ping_error: Option<&str>) -> ! {
        let deadline_socket_present = socket.exists();
        let deadline_child_exit = self.child.try_wait().unwrap();
        let deadline_flags = fcntl_getfl(&self.writer_inspector).unwrap();
        #[cfg(target_os = "macos")]
        if let Some(destination) = std::env::var_os("CTXMUX_DIAGNOSTIC_READINESS_SAMPLE_DIR") {
            // Optional private failure evidence for this exact fixture-owned
            // child, for one second at one-millisecond intervals. The deadline
            // facts above remain fixed; sampling does not extend or retry it.
            let path = std::path::PathBuf::from(destination)
                .join(format!("private-readiness-{}.sample", self.child.id()));
            let sampled = Command::new("/usr/bin/sample")
                .arg(self.child.id().to_string())
                .arg("1")
                .arg("1")
                .arg("-file")
                .arg(path)
                .output()
                .expect("sample private daemon");
            assert!(sampled.status.success(), "private daemon sampling failed");
        }
        panic!(
            "private daemon readiness at original deadline: socket_present={deadline_socket_present} child_exit={deadline_child_exit:?} last_completed_ping_error={last_ping_error:?} inherited_flags={deadline_flags:?}"
        );
    }

    fn close_receiver(&mut self) {
        drop(self.receiver.take().unwrap());
    }

    fn drain(&self, collected: &mut Vec<u8>) {
        let mut bytes = [0u8; 4096];
        loop {
            match rustix::io::read(self.receiver.as_ref().unwrap(), &mut bytes) {
                Ok(0) | Err(Errno::AGAIN) => break,
                Ok(count) => collected.extend_from_slice(&bytes[..count]),
                other => panic!("private pipe drain: {other:?}"),
            }
        }
    }

    async fn finish(&mut self) {
        kill_process(native_pid(self.child.id()), Signal::INT).expect("stop private daemon");
        timeout(ARRIVAL_BUDGET, async {
            loop {
                if let Some(status) = self.child.try_wait().unwrap() {
                    assert!(
                        status.success(),
                        "diagnostic failure must not cause daemon error exit"
                    );
                    break;
                }
                sleep(POLL_INTERVAL).await;
            }
        })
        .await
        .expect("private daemon reaped after graceful shutdown");
    }
}

impl Drop for PrivateDaemon {
    fn drop(&mut self) {
        // Only sessions created by this fixture may be cleaned up on failure.
        // Successful public Stop removes its reaped PID before this fallback.
        for raw in &self.child_sessions {
            if let Ok(raw) = i32::try_from(*raw) {
                let pid = nix::unistd::Pid::from_raw(raw);
                if nix::unistd::getsid(Some(pid)) == Ok(pid)
                    && let Some(pid) = Pid::from_raw(raw)
                {
                    let _ = kill_process_group(pid, Signal::KILL);
                }
            }
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

async fn result<T, E: Debug>(operation: impl Future<Output = Result<T, E>>, context: &str) -> T {
    timeout(ARRIVAL_BUDGET, operation)
        .await
        .unwrap_or_else(|error| panic!("{context} timed out: {error}"))
        .unwrap_or_else(|error| panic!("{context} failed: {error:?}"))
}

fn native_pid(raw: u32) -> Pid {
    Pid::from_raw(i32::try_from(raw).expect("native child PID fits signed PID"))
        .expect("native child PID is nonzero")
}

fn process_exists(raw: u32) -> bool {
    match test_kill_process(native_pid(raw)) {
        Ok(()) => true,
        Err(rustix::io::Errno::SRCH) => false,
        Err(error) => panic!("probe fixture child liveness: {error}"),
    }
}

fn echo_spec() -> RunSpec {
    RunSpec {
        program: "/usr/bin/python3".to_owned(),
        args: vec![
            "-c".to_owned(),
            r#"import os, signal, termios
attributes = termios.tcgetattr(0)
attributes[0] = 0
attributes[1] = 0
attributes[3] = termios.ISIG
attributes[6][termios.VMIN] = 1
attributes[6][termios.VTIME] = 0
termios.tcsetattr(0, termios.TCSANOW, attributes)
signal.signal(signal.SIGINT, lambda *_: os.write(1, b"CTRL_C\n"))
os.write(1, b"READY\n")
while True:
    value = os.read(0, 1)
    if not value: break
    os.write(1, value)
"#
            .to_owned(),
        ],
        cwd: None,
        env: BTreeMap::new(),
        initial_size: TerminalSize::default(),
        declared_inputs: Vec::new(),
    }
}

async fn exact_output(client: &Client, original: &RunInfo, expected: &[u8]) {
    timeout(ARRIVAL_BUDGET, async {
        loop {
            let (attachment, snapshot) = client.attach(original.id, 0).await.expect("attach Run");
            assert_eq!(snapshot.run.id, original.id);
            assert_eq!(snapshot.run.pid, original.pid);
            assert_eq!(snapshot.replay.first_available_byte, 0);
            assert!(!snapshot.replay.truncated);
            let mut observed = Vec::new();
            for chunk in snapshot.replay.chunks {
                assert_eq!(chunk.start_byte, u64::try_from(observed.len()).unwrap());
                observed.extend(chunk.data);
                assert_eq!(chunk.end_byte, u64::try_from(observed.len()).unwrap());
            }
            assert_eq!(
                snapshot.replay.latest_output_bytes,
                u64::try_from(observed.len()).unwrap()
            );
            attachment
                .detach()
                .await
                .expect("detach without stopping Run");
            assert!(
                expected.starts_with(&observed),
                "no extra, reordered or corrupted bytes"
            );
            if observed == expected {
                break;
            }
            sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .expect("exact ordered output arrives within original budget");
}

async fn write_exact(client: &Client, id: RunId, bytes: &[u8]) {
    let accepted = result(client.input(id, bytes.to_vec()), "public input").await;
    assert_eq!(accepted.run.id, id);
    assert_eq!(
        usize::try_from(accepted.receipt.written_bytes).unwrap(),
        bytes.len()
    );
}

async fn await_diagnostics(
    client: &Client,
    expected: impl Fn(&DiagnosticsSnapshot) -> bool,
) -> DiagnosticsSnapshot {
    timeout(ARRIVAL_BUDGET, async {
        loop {
            let facts = client.diagnostics().await.expect("public diagnostic facts");
            if expected(&facts) {
                return facts;
            }
            sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .expect("real diagnostic owner transition")
}

async fn malformed_connection(daemon: &PrivateDaemon) {
    let mut stream = tokio::net::UnixStream::connect(daemon.directory.path().join("ctxmux.sock"))
        .await
        .unwrap();
    stream.write_all(b"invalid JSON\n").await.unwrap();
    let mut response = Vec::new();
    result(
        stream.read_to_end(&mut response),
        "malformed connection closes",
    )
    .await;
}

async fn exercise_run(
    daemon: &mut PrivateDaemon,
    clients: &[Client; 2],
    index: usize,
    identity: &ctxmux_protocol::RuntimeIdentity,
) -> (RunInfo, Vec<u8>) {
    let client = &clients[index];
    assert_eq!(
        result(client.runtime_info(), "client identity").await,
        *identity
    );
    let run = result(
        client.start(echo_spec()),
        "start original child while diagnostics unavailable",
    )
    .await;
    let raw_pid = run.pid.unwrap();
    let pid = nix::unistd::Pid::from_raw(i32::try_from(raw_pid).unwrap());
    assert_eq!(nix::unistd::getsid(Some(pid)).unwrap(), pid);
    daemon.child_sessions.push(raw_pid);
    exact_output(client, &run, b"READY\n").await;
    let input = [0xff, 0x00, b"AB"[index], b"\r"[0], b"\n"[0]];
    write_exact(client, run.id, &input).await;
    let mut bytes = b"READY\n".to_vec();
    bytes.extend_from_slice(&input);
    exact_output(client, &run, &bytes).await;
    // Real PTY Ctrl+C traverses Input, then kernel ISIG; the handler keeps
    // the original child alive so subsequent progress cannot be replacement.
    write_exact(client, run.id, b"\x03").await;
    bytes.extend_from_slice(b"CTRL_C\n");
    exact_output(&clients[1 - index], &run, &bytes).await;
    write_exact(client, run.id, b"after").await;
    bytes.extend_from_slice(b"after");
    exact_output(client, &run, &bytes).await;
    let status = result(client.status(run.id), "original status").await;
    assert_eq!(status.pid, run.pid);
    assert_eq!(status.id, run.id);
    assert_eq!(
        status.applied_input_bytes,
        Some((input.len() + 1 + b"after".len()) as u64)
    );
    let service = status.native_service.unwrap();
    assert_eq!(service.owner, NativeOwnerStatus::Serving {});
    assert_eq!(service.output, NativeOutputStatus::Serving {});
    assert!(process_exists(raw_pid));
    (run, bytes)
}

async fn recover_diagnostic_sink(daemon: &PrivateDaemon, probe: &Client) {
    let mut observed = Vec::new();
    timeout(ARRIVAL_BUDGET, async {
        loop {
            daemon.drain(&mut observed);
            if probe.diagnostics().await.unwrap().funded_bytes == 0 {
                break;
            }
            sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .expect("real receiver resumes blocked writer");
    // A subsequent record ensures any loss racing the final queued record
    // gets an explicit recovered notice; no automatic diagnostic retry.
    malformed_connection(daemon).await;
    timeout(ARRIVAL_BUDGET, async {
        loop {
            daemon.drain(&mut observed);
            let facts = probe.diagnostics().await.unwrap();
            if facts.funded_bytes == 0 && facts.notice_written_bytes > 0 {
                break;
            }
            sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .expect("diagnostic-incomplete notice on recovered sink");
    daemon.drain(&mut observed);
    assert!(
        observed[..daemon.filled_bytes]
            .iter()
            .all(|byte| *byte == b"F"[0])
    );
    let diagnostics = String::from_utf8(observed[daemon.filled_bytes..].to_vec()).unwrap();
    assert!(diagnostics.contains("diagnostic-incomplete"));
    assert!(diagnostics.contains("unformatted_or_oversized_source_lengths=unknown"));
}

async fn verify_lane(full: bool, recover: bool, nonblocking: bool) {
    let (mut daemon, probe) = PrivateDaemon::start(full, nonblocking).await;
    if !full {
        daemon.close_receiver();
    }
    // Public parser errors exercise diagnostic admission; observing returned
    // counters, rather than counting coalescible signals, establishes causality.
    malformed_connection(&daemon).await;
    if full {
        await_diagnostics(&probe, |facts| {
            facts.sink == DiagnosticsSinkState::Writing && facts.active_record_bytes > 0
        })
        .await;
        timeout(ARRIVAL_BUDGET, async {
            loop {
                malformed_connection(&daemon).await;
                let facts = probe.diagnostics().await.unwrap();
                if facts.dropped_before_encoding_records > 0 {
                    break;
                }
            }
        })
        .await
        .expect("full sink causes observable admission pressure");
    } else {
        let facts = await_diagnostics(&probe, |facts| {
            facts.sink == DiagnosticsSinkState::Failed && !facts.writer_alive
        })
        .await;
        assert_eq!(facts.initialization_failures, 0);
        assert!(facts.sink_write_failures > 0);
        assert_eq!(facts.last_sink_errno, Some(Errno::PIPE.raw_os_error()));
    }
    assert_eq!(
        fcntl_getfl(&daemon.writer_inspector).unwrap(),
        daemon.inherited_flags,
        "daemon must not modify shared inherited OFD flags"
    );
    let identity = result(probe.runtime_info(), "runtime identity").await;
    let socket = daemon.directory.path().join("ctxmux.sock");
    let clients = [
        Client::new(&socket).with_expected_runtime_identity(identity.clone()),
        Client::new(&socket).with_expected_runtime_identity(identity.clone()),
    ];
    let mut runs = Vec::new();
    let mut expected = Vec::new();
    for index in 0..clients.len() {
        let (run, bytes) = exercise_run(&mut daemon, &clients, index, &identity).await;
        runs.push(run);
        expected.push(bytes);
    }
    assert_ne!(runs[0].id, runs[1].id);
    assert_ne!(runs[0].pid, runs[1].pid);
    for (index, run) in runs.iter().enumerate() {
        // Reattach from both clients checks disconnect/reconnect byte order.
        exact_output(&clients[1 - index], run, &expected[index]).await;
    }
    let facts = probe.diagnostics().await.unwrap();
    assert!(facts.funded_bytes <= facts.queue_budget_bytes);
    assert_eq!(facts.queue_budget_bytes, 1024);
    assert_eq!(facts.record_limit_bytes, 512);
    if full {
        assert_eq!(facts.written_bytes, 0);
        assert!(facts.writer_alive);
    }
    if recover {
        recover_diagnostic_sink(&daemon, &probe).await;
        for (index, run) in runs.iter().enumerate() {
            exact_output(&clients[index], run, &expected[index]).await;
        }
    }
    for (client, run) in clients.iter().zip(&runs) {
        let stop = result(client.prepare_stop(run.id), "prepare Stop").await;
        let receipt = result(
            client.stop(stop),
            "public Stop with unavailable diagnostics",
        )
        .await;
        assert_eq!(receipt.run.id, run.id);
        assert!(!process_exists(run.pid.unwrap()));
        daemon.child_sessions.retain(|pid| Some(*pid) != run.pid);
    }
    assert_eq!(
        result(probe.runtime_info(), "same daemon after logger fault").await,
        identity
    );
    assert_eq!(
        fcntl_getfl(&daemon.writer_inspector).unwrap(),
        daemon.inherited_flags
    );
    // For the full/unrecovered lane, receiver remains open/unread through real
    // SIGINT: a blocked logger cannot make daemon shutdown wait on thread join.
    daemon.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_stderr_keeps_two_runs_and_clients_serving_through_shutdown() {
    verify_lane(true, false, false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovered_stderr_exposes_loss_without_corrupting_two_runs() {
    verify_lane(true, true, false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closed_stderr_exposes_failure_and_preserves_two_runs() {
    verify_lane(false, false, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nonblocking_full_stderr_preserves_runs_and_shutdown_without_join() {
    verify_lane(true, false, true).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nonblocking_recovered_stderr_preserves_original_flags_and_exact_runs() {
    verify_lane(true, true, true).await;
}
