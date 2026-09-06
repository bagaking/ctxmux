//! Closing a diagnostic pipe must not terminate a daemon or its real Runs.
//! This uses the build-owned daemon and only the public Rust client boundary.
//! Memory-only SIGHUP has a documented no-op diagnostic; it must preserve both
//! original children and exact input/output. An open, full stderr pipe is a
//! separate blocking hazard and is not qualified by this regression.
//! The no-op has no public completion receipt: this checks subsequent service,
//! while a same-source failing control establishes the diagnostic's causality.

use std::{
    collections::BTreeMap,
    fmt::Debug,
    future::Future,
    process::{Child, Command, Stdio},
    time::Duration,
};

use ctxmux_client::Client;
use ctxmux_protocol::{RunId, RunInfo, RunSpec, TerminalSize};
use ctxmux_test_support::daemon_spawn_permit;
use rustix::process::{Pid, Signal, kill_process, kill_process_group, test_kill_process};
use tempfile::TempDir;
use tokio::time::{sleep, timeout};

// The same two-second arrival budget used by the controlled public proof.
// This is a test failure deadline, never a product capacity or retry policy.
const ARRIVAL_BUDGET: Duration = Duration::from_secs(2);
const POLL_INTERVAL: Duration = Duration::from_millis(10);

struct PrivateDaemon {
    child: Child,
    directory: TempDir,
    child_sessions: Vec<u32>,
}

impl PrivateDaemon {
    async fn start() -> (Self, Client) {
        let permit = daemon_spawn_permit().await;
        let directory = tempfile::tempdir().expect("private daemon directory");
        let socket = directory.path().join("ctxmux.sock");
        let child = Command::new(env!("CARGO_BIN_EXE_ctxmuxd"))
            .arg("--socket")
            .arg(&socket)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn build-owned memory-only daemon");
        let mut daemon = Self {
            child,
            directory,
            child_sessions: Vec::new(),
        };
        let probe = Client::new(&socket);
        timeout(ARRIVAL_BUDGET, async {
            loop {
                assert!(
                    daemon.child.try_wait().unwrap().is_none(),
                    "daemon alive during startup"
                );
                if probe.ping().await.is_ok() {
                    break;
                }
                sleep(POLL_INTERVAL).await;
            }
        })
        .await
        .expect("private daemon readiness");
        drop(permit);
        (daemon, probe)
    }

    fn close_diagnostic_receiver_and_signal(&mut self) {
        drop(
            self.child
                .stderr
                .take()
                .expect("close private stderr receiver"),
        );
        kill_process(native_pid(self.child.id()), Signal::HUP).expect("deliver real SIGHUP");
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

fn cat_spec() -> RunSpec {
    RunSpec {
        program: "/bin/sh".to_owned(),
        args: vec![
            "-c".to_owned(),
            "stty raw -echo; printf READY; exec /bin/cat".to_owned(),
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closed_stderr_preserves_two_real_runs_and_clients() {
    let (mut daemon, probe) = PrivateDaemon::start().await;
    let identity = result(probe.runtime_info(), "runtime identity").await;
    let socket = daemon.directory.path().join("ctxmux.sock");
    let clients = [
        Client::new(&socket).with_expected_runtime_identity(identity.clone()),
        Client::new(&socket).with_expected_runtime_identity(identity.clone()),
    ];
    let mut runs = Vec::new();
    for (index, client) in clients.iter().enumerate() {
        assert_eq!(
            result(client.runtime_info(), "second client identity").await,
            identity
        );
        let run = result(client.start(cat_spec()), "start real Run").await;
        let raw_pid = run.pid.expect("native child PID");
        let pid = nix::unistd::Pid::from_raw(i32::try_from(raw_pid).unwrap());
        assert_eq!(nix::unistd::getsid(Some(pid)).unwrap(), pid);
        daemon.child_sessions.push(raw_pid);
        exact_output(client, &run, b"READY").await;
        let before = format!("run-{index}:before\n");
        write_exact(client, run.id, before.as_bytes()).await;
        exact_output(client, &run, format!("READY{before}").as_bytes()).await;
        runs.push(run);
    }
    assert_ne!(runs[0].id, runs[1].id);
    assert_ne!(runs[0].pid, runs[1].pid);
    daemon.close_diagnostic_receiver_and_signal();

    // These operations establish actual service, not a cached Running label.
    for (index, (client, run)) in clients.iter().zip(&runs).enumerate() {
        let before = format!("run-{index}:before\n");
        let after = format!("run-{index}:after\n");
        write_exact(client, run.id, after.as_bytes()).await;
        exact_output(client, run, format!("READY{before}{after}").as_bytes()).await;
        let status = result(client.status(run.id), "status original Run").await;
        assert_eq!(status.id, run.id);
        assert_eq!(status.pid, run.pid);
        assert_eq!(
            status.applied_input_bytes,
            Some(u64::try_from(before.len() + after.len()).unwrap())
        );
        assert!(process_exists(run.pid.unwrap()));
        assert_eq!(
            result(client.runtime_info(), "unchanged runtime identity").await,
            identity
        );
    }
    assert!(daemon.child.try_wait().unwrap().is_none());
    for (client, run) in clients.iter().zip(&runs) {
        let operation = result(client.prepare_stop(run.id), "prepare public Stop").await;
        let stopped = result(client.stop(operation), "complete public Stop").await;
        assert_eq!(stopped.run.id, run.id);
        assert!(
            !process_exists(run.pid.unwrap()),
            "public Stop reaps original child"
        );
        daemon.child_sessions.retain(|pid| Some(*pid) != run.pid);
    }
    daemon.finish().await;
}
