//! Two real PTYs through two public Clients; fault injection lives only in a
//! private source copy selected by the proof runner, never in shipping runtime.
use ctxmux_client::{Client, replay_bytes};
use ctxmux_protocol::{
    RunId, RunInfo, RunSpec, RunState, TerminalCheckpointUnavailableReason, TerminalContinuation,
    TerminalSize,
};
use std::{
    collections::BTreeMap,
    fs,
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::Mutex,
    time::{Duration, Instant},
};
use tempfile::TempDir;
use tokio::time::{sleep, timeout};

struct Daemon {
    child: Child,
    directory: TempDir,
    socket: PathBuf,
    children: Mutex<Vec<(u32, String)>>,
}
impl Daemon {
    async fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("socket");
        let binary = std::env::var_os("CTXMUX_OUTPUT_PROOF_DAEMON").map_or_else(
            || PathBuf::from(env!("CARGO_BIN_EXE_ctxmuxd")),
            PathBuf::from,
        );
        let stderr = fs::File::create(directory.path().join("stderr.log")).unwrap();
        let child = Command::new(binary)
            .arg("--socket")
            .arg(&socket)
            .arg("--state-dir")
            .arg(directory.path().join("state"))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(stderr)
            .spawn()
            .unwrap();
        let result = Self {
            child,
            directory,
            socket,
            children: Mutex::new(Vec::new()),
        };
        let client = Client::new(result.socket.clone());
        timeout(Duration::from_secs(5), async {
            while client.list().await.is_err() {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        result
    }
    fn remember(&self, run: &RunInfo) {
        let pid = run.pid.unwrap();
        let output = Command::new("/bin/ps")
            .args(["-p", &pid.to_string(), "-o", "lstart=,command="])
            .output()
            .unwrap();
        assert!(output.status.success());
        let identity = String::from_utf8(output.stdout).unwrap();
        assert!(identity.contains(self.directory.path().to_str().unwrap()));
        self.children.lock().unwrap().push((pid, identity.clone()));
        println!(
            "PRIVATE_OUTPUT_OWNER_CHILD {}",
            serde_json::json!({"pid":pid,"identity":identity})
        );
    }
}
impl Drop for Daemon {
    fn drop(&mut self) {
        // Exact child owned by this private fixture; never a production lookup.
        if self.child.try_wait().unwrap().is_none() {
            assert!(
                Command::new("kill")
                    .arg("-INT")
                    .arg(self.child.id().to_string())
                    .status()
                    .unwrap()
                    .success()
            );
            let deadline = Instant::now() + Duration::from_secs(3);
            while Instant::now() < deadline {
                if self.child.try_wait().unwrap().is_some() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            assert!(
                self.child.try_wait().unwrap().is_some(),
                "private daemon did not exit after SIGINT"
            );
        }
        let mut actions = Vec::new();
        let mut remaining = Vec::new();
        for (pid, identity) in self.children.lock().unwrap().iter() {
            let output = Command::new("/bin/ps")
                .args(["-p", &pid.to_string(), "-o", "lstart=,command="])
                .output()
                .unwrap();
            if !output.status.success() {
                continue;
            }
            assert_eq!(
                String::from_utf8(output.stdout).unwrap(),
                *identity,
                "private PID identity changed; do not signal"
            );
            assert!(
                Command::new("kill")
                    .args(["-TERM", &pid.to_string()])
                    .status()
                    .unwrap()
                    .success()
            );
            actions.push(*pid);
            let deadline = Instant::now() + Duration::from_secs(3);
            loop {
                if !Command::new("/bin/ps")
                    .args(["-p", &pid.to_string(), "-o", "pid="])
                    .output()
                    .unwrap()
                    .status
                    .success()
                {
                    break;
                }
                if Instant::now() >= deadline {
                    remaining.push(*pid);
                    break;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        }
        println!(
            "PRIVATE_OUTPUT_OWNER_CLEANUP {}",
            serde_json::json!({"exactPrivateChildTerm":actions,"remaining":remaining,"productionControls":[]})
        );
        assert!(remaining.is_empty(), "private child cleanup incomplete");
    }
}
fn spec(name: &str, daemon: &Daemon) -> RunSpec {
    let source = format!(
        r"
import os,signal,termios
attrs=termios.tcgetattr(0);attrs[3]&=~(termios.ICANON|termios.ECHO);attrs[3]|=termios.ISIG
attrs[1]&=~termios.ONLCR;attrs[6][termios.VMIN]=1;attrs[6][termios.VTIME]=0
termios.tcsetattr(0,termios.TCSANOW,attrs)
name={name:?}.encode()
def interrupted(signum,frame):
 os.write(1,name+b':CTRL_C\n');raise SystemExit(0)
signal.signal(signal.SIGINT,interrupted)
os.write(1,name+b':READY\n')
while True:
 b=os.read(0,1)
 if not b: break
 os.write(1,name+b':'+b+b'\n')
 if b==b'q': break
"
    );
    RunSpec {
        program: "/usr/bin/python3".into(),
        args: vec![
            "-u".into(),
            "-c".into(),
            source,
            daemon.directory.path().display().to_string(),
        ],
        cwd: Some(daemon.directory.path().display().to_string()),
        env: BTreeMap::new(),
        initial_size: TerminalSize { rows: 4, cols: 12 },
        declared_inputs: Vec::new(),
    }
}
async fn raw(client: &Client, id: RunId) -> Vec<u8> {
    let (view, snapshot) = client.attach(id, 0).await.unwrap();
    assert_eq!(snapshot.terminal, TerminalContinuation::NotRequested);
    let mut cursor = 0;
    for chunk in &snapshot.replay.chunks {
        assert_eq!(chunk.start_byte, cursor);
        cursor = chunk.end_byte;
    }
    assert_eq!(cursor, snapshot.replay.latest_output_bytes);
    let bytes = replay_bytes(&snapshot.replay.chunks);
    drop(view);
    bytes
}
async fn wait_exact(client: &Client, id: RunId, expected: &[u8]) -> RunInfo {
    timeout(Duration::from_secs(5), async {
        loop {
            let status = client.status(id).await.unwrap();
            if status.latest_output_bytes == expected.len() as u64
                && status.durable_output_bytes == Some(expected.len() as u64)
            {
                assert_eq!(raw(client, id).await, expected);
                return status;
            }
            assert!(
                status.latest_output_bytes <= expected.len() as u64,
                "unexpected bytes {:?}",
                raw(client, id).await
            );
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
#[ignore = "proof runner supplies a private source copy with a completed owner thread"]
async fn stopped_owner_public_start_is_rejected_before_child_launch() {
    let daemon = Daemon::new().await;
    let client = Client::new(daemon.socket.clone());
    // The private source copy completes its real owner thread at construction.
    sleep(Duration::from_millis(100)).await;
    let marker = daemon.directory.path().join("child-launched");
    let mut request = spec("A", &daemon);
    request.args = vec![
        "-c".into(),
        "import sys;open(sys.argv[1],'w').write('launched')".into(),
        marker.display().to_string(),
    ];
    let error = client.start(request).await.unwrap_err();
    assert!(
        matches!(
            error,
            ctxmux_client::ClientError::Protocol {
                code: ctxmux_protocol::ErrorCode::BackendUnavailable,
                ..
            }
        ),
        "{error:?}"
    );
    assert!(
        error
            .to_string()
            .contains("daemon-wide native owner stopped")
    );
    sleep(Duration::from_millis(100)).await;
    assert!(!marker.exists(), "rejected owner must not run a child");
    assert_eq!(
        client.list().await.unwrap(),
        Vec::<ctxmux_protocol::RunSummary>::new()
    );
    println!(
        "PRIVATE_OUTPUT_OWNER_START_RECEIPT {}",
        serde_json::json!({
        "actualPublicStart":true,"code":"backend_unavailable","ownerStopped":true,"childMarkerAbsent":true,
        "daemonPid":daemon.child.id(),"productionControls":[]})
    );
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one ordered two-PTY fixture keeps byte fences, control receipts and lifecycle boundaries visible together"
)]
async fn two_runs_two_clients_keep_ordered_output_input_and_ctrl_c_after_one_derivation_failure() {
    let fault = std::env::var("CTXMUX_OUTPUT_PROOF_FAULT").unwrap_or_else(|_| "none".into());
    assert!(["none", "process", "resize", "export"].contains(&fault.as_str()));
    let daemon = Daemon::new().await;
    let first = Client::new(daemon.socket.clone());
    let second = Client::new(daemon.socket.clone());
    let a = first.start(spec("A", &daemon)).await.unwrap();
    daemon.remember(&a);
    let b = second.start(spec("B", &daemon)).await.unwrap();
    daemon.remember(&b);
    assert_ne!(a.id, b.id);
    assert!(a.pid.is_some());
    assert!(b.pid.is_some());
    assert_ne!(a.pid, b.pid);
    wait_exact(&first, a.id, b"A:READY\n").await;
    wait_exact(&second, b.id, b"B:READY\n").await;
    let (a_view, initial) = first.attach_terminal(a.id, 0).await.unwrap();
    assert!(matches!(
        initial.terminal,
        TerminalContinuation::BasicVt { .. }
    ));
    let (b_view, _) = second.attach(b.id, 0).await.unwrap();
    a_view
        .resize(TerminalSize { rows: 5, cols: 14 })
        .await
        .unwrap();
    b_view
        .resize(TerminalSize { rows: 6, cols: 15 })
        .await
        .unwrap();
    assert_eq!(
        first.status(a.id).await.unwrap().current_size,
        Some(TerminalSize { rows: 5, cols: 14 })
    );
    assert_eq!(
        second.status(b.id).await.unwrap().current_size,
        Some(TerminalSize { rows: 6, cols: 15 })
    );
    assert_eq!(
        a_view
            .input(b"a".to_vec())
            .await
            .unwrap()
            .receipt
            .written_bytes,
        1
    );
    assert_eq!(
        b_view
            .input(b"b".to_vec())
            .await
            .unwrap()
            .receipt
            .written_bytes,
        1
    );
    wait_exact(&first, a.id, b"A:READY\nA:a\n").await;
    wait_exact(&second, b.id, b"B:READY\nB:b\n").await;
    a_view
        .resize(TerminalSize { rows: 7, cols: 13 })
        .await
        .unwrap();
    assert_eq!(
        a_view
            .input(b"!".to_vec())
            .await
            .unwrap()
            .receipt
            .written_bytes,
        1
    );
    wait_exact(&first, a.id, b"A:READY\nA:a\nA:!\n").await;
    let (fresh, snapshot) = second.attach_terminal(a.id, 0).await.unwrap();
    if fault == "none" {
        assert!(matches!(
            snapshot.terminal,
            TerminalContinuation::BasicVt { .. }
        ));
    } else {
        assert_eq!(
            snapshot.terminal,
            TerminalContinuation::Unavailable {
                reason: TerminalCheckpointUnavailableReason::InvalidCheckpoint
            }
        );
        assert!(snapshot.terminal_restore.is_empty());
        assert_eq!(
            replay_bytes(&snapshot.replay.chunks),
            b"A:READY\nA:a\nA:!\n"
        );
    }
    fresh.detach().await.unwrap();
    assert_eq!(
        b_view
            .input(b"c".to_vec())
            .await
            .unwrap()
            .receipt
            .written_bytes,
        1
    );
    wait_exact(&second, b.id, b"B:READY\nB:b\nB:c\n").await;
    // Ordinary client/view release does not transfer or stop either native Run.
    a_view.detach().await.unwrap();
    drop(first);
    assert_eq!(second.status(a.id).await.unwrap().pid, a.pid);
    assert_eq!(second.status(b.id).await.unwrap().pid, b.pid);
    assert_eq!(
        second
            .input(a.id, b"z".to_vec())
            .await
            .unwrap()
            .receipt
            .written_bytes,
        1
    );
    let live_a = wait_exact(&second, a.id, b"A:READY\nA:a\nA:!\nA:z\n").await;
    assert_eq!(live_a.state, RunState::Running);
    assert_eq!(live_a.applied_input_bytes, Some(3));
    assert_eq!(
        live_a.current_size,
        Some(TerminalSize { rows: 7, cols: 13 })
    );
    // A real PTY Ctrl+C, rather than a signal from the test process.
    assert_eq!(
        second
            .input(a.id, vec![3])
            .await
            .unwrap()
            .receipt
            .written_bytes,
        1
    );
    wait_exact(&second, a.id, b"A:READY\nA:a\nA:!\nA:z\nA:CTRL_C\n").await;
    assert_eq!(
        b_view
            .input(b"d".to_vec())
            .await
            .unwrap()
            .receipt
            .written_bytes,
        1
    );
    let live_b = wait_exact(&second, b.id, b"B:READY\nB:b\nB:c\nB:d\n").await;
    assert_eq!(live_b.state, RunState::Running);
    assert_eq!(live_b.pid, b.pid);
    assert_eq!(
        b_view
            .input(b"q".to_vec())
            .await
            .unwrap()
            .receipt
            .written_bytes,
        1
    );
    wait_exact(&second, b.id, b"B:READY\nB:b\nB:c\nB:d\nB:q\n").await;
    timeout(Duration::from_secs(5), async {
        loop {
            if matches!(
                second.status(a.id).await.unwrap().state,
                RunState::Exited { code: 0, .. }
            ) && matches!(
                second.status(b.id).await.unwrap().state,
                RunState::Exited { code: 0, .. }
            ) {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    drop(b_view);
    println!(
        "PRIVATE_OUTPUT_OWNER_RECEIPT {}",
        serde_json::json!({
        "fault":fault,"daemonPid":daemon.child.id(),"runA":a.id,"pidA":a.pid,"runB":b.id,"pidB":b.pid,
        "twoPublicClients":true,"originalByteOrdering":true,"durableBytes":true,"originalPidAfterClientRelease":true,
        "actualPtyCtrlC":true,"naturalExits":[0,0],"forcedCleanup":[],"productionControls":[]})
    );
}
