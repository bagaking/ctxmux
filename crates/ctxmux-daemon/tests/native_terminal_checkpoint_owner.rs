//! Actual public attachment through a private native PTY, not an emulator-only gate.
use ctxmux_client::Client;
use ctxmux_protocol::{AttachedSnapshot, RunId, RunSpec, TerminalContinuation, TerminalSize};
use std::{
    collections::BTreeMap,
    io::{BufRead, BufReader},
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    time::Duration,
};
use tempfile::TempDir;
use tokio::time::{sleep, timeout};

struct Daemon {
    child: Child,
    directory: TempDir,
    client: Client,
    logs: Arc<Mutex<Vec<String>>>,
}
impl Daemon {
    async fn new(persistent: bool) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("ctxmux.sock");
        let mut command = Command::new(env!("CARGO_BIN_EXE_ctxmuxd"));
        command.arg("--socket").arg(&socket);
        if persistent {
            command
                .arg("--state-dir")
                .arg(directory.path().join("state"));
        }
        let mut child = command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let stderr = child.stderr.take().unwrap();
        let logs = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&logs);
        std::thread::spawn(move || {
            for line in BufReader::new(stderr).lines() {
                captured.lock().unwrap().push(line.unwrap());
            }
        });
        let mut result = Self {
            child,
            directory,
            client: Client::new(socket),
            logs,
        };
        let mut last_error = None;
        let startup = timeout(Duration::from_secs(5), async {
            loop {
                match result.client.list().await {
                    Ok(_) => break,
                    Err(error) => last_error = Some(error),
                }
                if let Some(status) = result.child.try_wait().expect("poll owned daemon") {
                    panic!(
                        "owned daemon exited before readiness: {status}; last request: {last_error:?}; stderr: {:?}",
                        result.logs.lock().unwrap()
                    );
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(
            startup.is_ok(),
            "original five-second readiness deadline expired; owned pid {}; exit {:?}; socket exists {}; last request {last_error:?}; stderr {:?}",
            result.child.id(),
            result
                .child
                .try_wait()
                .expect("poll owned daemon after deadline"),
            result.client.socket_path().exists(),
            result.logs.lock().unwrap()
        );
        result
    }
    async fn wait_log(&self, needle: &str) {
        timeout(Duration::from_secs(10), async {
            loop {
                if self
                    .logs
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|line| line.contains(needle))
                {
                    return;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
    fn sighup(&self) {
        assert!(
            Command::new("kill")
                .arg("-HUP")
                .arg(self.child.id().to_string())
                .status()
                .unwrap()
                .success()
        );
    }
    async fn stop_run(&self, id: RunId) {
        let operation = self.client.prepare_stop(id).await.unwrap();
        self.client.stop(operation).await.unwrap();
    }
}
impl Drop for Daemon {
    fn drop(&mut self) {
        // Only the subprocess created by this fixture; no global PID lookup.
        if self.child.try_wait().unwrap().is_none() {
            let delivered = Command::new("kill")
                .arg("-INT")
                .arg(self.child.id().to_string())
                .status()
                .unwrap();
            assert!(delivered.success());
            for _ in 0..100 {
                if self.child.try_wait().unwrap().is_some() {
                    return;
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            self.child.kill().unwrap();
            self.child.wait().unwrap();
            panic!("private daemon failed bounded graceful cleanup");
        }
    }
}
fn spec(alternate: bool) -> RunSpec {
    let source = format!(
        r"
import os,termios
attrs=termios.tcgetattr(0);attrs[3]&=~(termios.ICANON|termios.ECHO)
attrs[6][termios.VMIN]=1;attrs[6][termios.VTIME]=0
termios.tcsetattr(0,termios.TCSANOW,attrs)
os.write(1,b''.join(('ROW%03d\r\n'%i).encode() for i in range(30)))
if {alternate}: os.write(1,b'\x1b[?1049h\x1b[?1003h\x1b[?1006h')
os.write(1,b'\x1b[HFRAME'+b'\x1b[1;1Hframe'*450000+b'\x1b[2;1HREADY')
while True:
 b=os.read(0,1)
 if not b: break
 if b==b'x': os.write(1,b'\x1b[?1049lTAIL')
 if b==b'p': os.write(1,b'\x1b[3;')
 if b==b'z': os.write(1,b'4HZ')
 if b==b'q': break
",
        alternate = if alternate { "True" } else { "False" }
    );
    RunSpec {
        program: "/usr/bin/python3".into(),
        args: vec!["-u".into(), "-c".into(), source],
        cwd: None,
        env: BTreeMap::new(),
        initial_size: TerminalSize { rows: 4, cols: 12 },
        declared_inputs: Vec::new(),
    }
}
async fn wait_ready(client: &Client, id: RunId) {
    timeout(Duration::from_secs(10), async {
        loop {
            let status = client.status(id).await.unwrap();
            if status.latest_output_bytes > 4 * 1024 * 1024 {
                let (view, snapshot) = client.attach_terminal(id, 0).await.unwrap();
                let parser = restore(&snapshot);
                view.detach().await.unwrap();
                if parser.screen().contents().contains("READY") {
                    return;
                }
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}
fn restore(snapshot: &AttachedSnapshot) -> vt100::Parser {
    assert!(
        matches!(&snapshot.terminal, TerminalContinuation::BasicVt { .. }),
        "new byte0 native Run must have a known seed: {:?}",
        snapshot.terminal
    );
    let TerminalContinuation::BasicVt {
        checkpoint,
        resizes,
    } = &snapshot.terminal
    else {
        unreachable!()
    };
    assert_eq!(checkpoint.run_id, snapshot.run.id);
    assert_eq!(
        usize::try_from(checkpoint.restore_bytes).expect("restore bytes fit the fixture host"),
        snapshot.terminal_restore.len()
    );
    // This consumer retention is the original fixture policy, not a Run
    // capacity requirement. Honor temporary restore geometry before live data.
    let history_rows = 10_000;
    let restore_history = checkpoint
        .restore_scrollback_rows
        .map_or(history_rows, |rows| {
            usize::try_from(rows).expect("restore history fits the fixture host")
        });
    let split = usize::try_from(checkpoint.resize_after_restore_bytes)
        .expect("restore boundary fits the fixture host");
    assert!(split <= snapshot.terminal_restore.len());
    let mut parser = vt100::Parser::new(
        checkpoint.restore_size.rows,
        checkpoint.restore_size.cols,
        restore_history,
    );
    parser.process(&snapshot.terminal_restore[..split]);
    assert!(
        parser.is_ground(),
        "prefix ends at a complete restore boundary"
    );
    parser.set_scrollback_limit(history_rows);
    parser.set_size(checkpoint.size.rows, checkpoint.size.cols);
    parser.process(&snapshot.terminal_restore[split..]);
    assert!(
        parser.is_ground(),
        "final suffix ends at a complete restore boundary"
    );
    let mut geometry = resizes.iter().peekable();
    let mut cursor = checkpoint.through_byte;
    for chunk in &snapshot.replay.chunks {
        assert_eq!(chunk.start_byte, cursor);
        while cursor < chunk.end_byte {
            while let Some(resize) = geometry.peek().filter(|r| r.through_byte == cursor) {
                parser.set_size(resize.size.rows, resize.size.cols);
                geometry.next();
            }
            let end = geometry
                .peek()
                .map_or(chunk.end_byte, |r| r.through_byte.min(chunk.end_byte));
            parser.process(
                &chunk.data[usize::try_from(cursor - chunk.start_byte)
                    .expect("replay start fits this chunk")
                    ..usize::try_from(end - chunk.start_byte).expect("replay end fits this chunk")],
            );
            cursor = end;
        }
    }
    for resize in geometry {
        assert_eq!(resize.through_byte, cursor);
        parser.set_size(resize.size.rows, resize.size.cols);
    }
    assert_eq!(cursor, snapshot.replay.latest_output_bytes);
    parser
}
#[tokio::test]
async fn native_normal_history_survives_raw_eviction_and_new_attachment() {
    let daemon = Daemon::new(false).await;
    let run = daemon.client.start(spec(false)).await.unwrap();
    wait_ready(&daemon.client, run.id).await;
    let (view, snapshot) = daemon.client.attach_terminal(run.id, 0).await.unwrap();
    assert!(snapshot.run.first_available_byte > 0);
    assert!(
        snapshot.replay.chunks.is_empty(),
        "complete-state attach must not send unrelated old raw bytes"
    );
    let mut parser = restore(&snapshot);
    assert!(!parser.screen().alternate_screen());
    parser.set_scrollback(10_000);
    assert!(
        parser.screen().contents().contains("ROW000"),
        "true retained normal rows missing"
    );
    view.detach().await.unwrap();
    let (raw, raw_snapshot) = daemon.client.attach(run.id, 0).await.unwrap();
    assert_eq!(raw_snapshot.terminal, TerminalContinuation::NotRequested);
    assert!(raw_snapshot.terminal_restore.is_empty());
    assert!(!raw_snapshot.replay.chunks.is_empty());
    assert!(raw_snapshot.replay.truncated);
    raw.detach().await.unwrap();
    assert_eq!(daemon.client.status(run.id).await.unwrap().pid, run.pid);
    daemon.stop_run(run.id).await;
}
#[tokio::test]
async fn native_alternate_sgr_and_normal_history_survive_new_attachment() {
    let daemon = Daemon::new(false).await;
    let run = daemon.client.start(spec(true)).await.unwrap();
    wait_ready(&daemon.client, run.id).await;
    let (view, snapshot) = daemon.client.attach_terminal(run.id, 0).await.unwrap();
    let mut parser = restore(&snapshot);
    assert!(parser.screen().alternate_screen());
    assert_eq!(
        parser.screen().mouse_protocol_mode(),
        vt100::MouseProtocolMode::AnyMotion
    );
    assert_eq!(
        parser.screen().mouse_protocol_encoding(),
        vt100::MouseProtocolEncoding::Sgr
    );
    assert!(parser.screen().contents().contains("READY"));
    let before = daemon.client.status(run.id).await.unwrap();
    let receipt = view.input(b"x".to_vec()).await.unwrap();
    assert_eq!(receipt.receipt.written_bytes, 1);
    timeout(Duration::from_secs(5), async {
        while daemon
            .client
            .status(run.id)
            .await
            .unwrap()
            .latest_output_bytes
            == before.latest_output_bytes
        {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    view.detach().await.unwrap();
    let (next, next_snapshot) = daemon.client.attach_terminal(run.id, 0).await.unwrap();
    parser = restore(&next_snapshot);
    assert!(!parser.screen().alternate_screen());
    parser.set_scrollback(10_000);
    assert!(parser.screen().contents().contains("ROW000"));
    assert_eq!(next_snapshot.run.pid, run.pid);
    next.detach().await.unwrap();
    daemon.stop_run(run.id).await;
}

#[tokio::test]
async fn native_same_byte_resizes_and_partial_control_tail_keep_one_ordered_fence() {
    let daemon = Daemon::new(false).await;
    let run = daemon.client.start(spec(false)).await.unwrap();
    wait_ready(&daemon.client, run.id).await;
    let (view, initial) = daemon.client.attach_terminal(run.id, 0).await.unwrap();
    let fence = initial.replay.latest_output_bytes;
    view.input(b"p".to_vec()).await.unwrap();
    timeout(Duration::from_secs(5), async {
        while daemon
            .client
            .status(run.id)
            .await
            .unwrap()
            .latest_output_bytes
            != fence + 4
        {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    view.resize(TerminalSize { rows: 5, cols: 14 })
        .await
        .unwrap();
    view.resize(TerminalSize { rows: 6, cols: 15 })
        .await
        .unwrap();
    view.detach().await.unwrap();
    let (next, snapshot) = daemon.client.attach_terminal(run.id, 0).await.unwrap();
    let TerminalContinuation::BasicVt {
        checkpoint,
        resizes,
    } = &snapshot.terminal
    else {
        panic!("no complete preceding seed")
    };
    assert_eq!(checkpoint.through_byte, fence);
    assert_eq!(
        snapshot
            .replay
            .chunks
            .iter()
            .flat_map(|c| c.data.iter())
            .copied()
            .collect::<Vec<_>>(),
        b"\x1b[3;"
    );
    assert_eq!(resizes.len(), 2);
    assert_eq!(resizes[0].through_byte, fence + 4);
    assert_eq!(resizes[1].through_byte, fence + 4);
    assert_eq!(resizes[1].resize_revision, resizes[0].resize_revision + 1);
    let mut parser = restore(&snapshot);
    assert_eq!(parser.screen().size(), (6, 15));
    assert!(
        !parser.is_ground(),
        "partial original CSI must remain carry, not a fake complete seed"
    );
    parser.process(b"4HZ");
    assert!(parser.is_ground());
    assert_eq!(parser.screen().cell(2, 3).unwrap().contents(), "Z");
    next.detach().await.unwrap();
    daemon.stop_run(run.id).await;
}
#[tokio::test]
async fn native_planned_exec_keeps_known_state_same_daemon_pid_child_pty_and_input() {
    let mut daemon = Daemon::new(true).await;
    let run = daemon.client.start(spec(true)).await.unwrap();
    wait_ready(&daemon.client, run.id).await;
    let daemon_pid = daemon.child.id();
    let (old, before) = daemon.client.attach_terminal(run.id, 0).await.unwrap();
    daemon.sighup();
    daemon
        .wait_log("adopted inherited listener for handoff")
        .await;
    timeout(Duration::from_secs(5), async {
        loop {
            match old.next_event().await {
                Ok(None) | Err(_) => break,
                Ok(Some(_)) => {}
            }
        }
    })
    .await
    .unwrap();
    assert!(daemon.child.try_wait().unwrap().is_none());
    assert_eq!(daemon.child.id(), daemon_pid);
    let (view, after) = daemon.client.attach_terminal(run.id, 0).await.unwrap();
    assert_eq!(after.run.pid, before.run.pid);
    assert_eq!(after.run.id, before.run.id);
    let parser = restore(&after);
    assert!(parser.screen().alternate_screen());
    assert_eq!(
        parser.screen().mouse_protocol_encoding(),
        vt100::MouseProtocolEncoding::Sgr
    );
    assert_eq!(
        parser.screen().mouse_protocol_mode(),
        vt100::MouseProtocolMode::AnyMotion
    );
    assert!(parser.screen().contents().contains("READY"));
    assert_eq!(
        view.input(b"x".to_vec())
            .await
            .unwrap()
            .receipt
            .written_bytes,
        1
    );
    timeout(Duration::from_secs(5), async {
        while daemon
            .client
            .status(run.id)
            .await
            .unwrap()
            .latest_output_bytes
            == after.replay.latest_output_bytes
        {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    view.detach().await.unwrap();
    let (last, final_snapshot) = daemon.client.attach_terminal(run.id, 0).await.unwrap();
    let mut parser = restore(&final_snapshot);
    assert!(!parser.screen().alternate_screen());
    parser.set_scrollback(10_000);
    assert!(parser.screen().contents().contains("ROW000"));
    last.detach().await.unwrap();
    daemon.stop_run(run.id).await;
}
#[tokio::test]
async fn native_checkpoint_file_failure_refuses_exec_before_extract_and_keeps_old_service() {
    let daemon = Daemon::new(true).await;
    let run = daemon.client.start(spec(true)).await.unwrap();
    wait_ready(&daemon.client, run.id).await;
    let (view, before) = daemon.client.attach_terminal(run.id, 0).await.unwrap();
    let base = daemon.directory.path().join("state/terminal-checkpoints");
    std::fs::create_dir_all(&base).unwrap();
    let path = base.join(format!("{}.json", run.id));
    // Obstruct only this derived file, after its raw output has settled. A
    // directory cannot be atomically overwritten by the checkpoint writer.
    for _ in 0..20 {
        if path.is_file() {
            std::fs::remove_file(&path).unwrap();
        }
        match std::fs::create_dir(&path) {
            Ok(()) => break,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                sleep(Duration::from_millis(10)).await;
            }
            Err(error) => panic!("private obstruction failed: {error}"),
        }
    }
    assert!(
        path.is_dir(),
        "private checkpoint obstruction must exist before upgrade"
    );
    daemon.sighup();
    daemon.wait_log("aborted before").await;
    assert_eq!(daemon.client.status(run.id).await.unwrap().pid, run.pid);
    assert_eq!(
        view.input(b"x".to_vec())
            .await
            .unwrap()
            .receipt
            .written_bytes,
        1
    );
    timeout(Duration::from_secs(5), async {
        while daemon
            .client
            .status(run.id)
            .await
            .unwrap()
            .latest_output_bytes
            == before.replay.latest_output_bytes
        {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    view.detach().await.unwrap();
    daemon.stop_run(run.id).await;
}

fn seed_pressure_spec() -> RunSpec {
    let mut launch = spec(false);
    launch.args = vec![
        "-u".into(),
        "-c".into(),
        r"
import os,termios
attrs=termios.tcgetattr(0);attrs[3]&=~(termios.ICANON|termios.ECHO)
attrs[6][termios.VMIN]=1;attrs[6][termios.VTIME]=0
termios.tcsetattr(0,termios.TCSANOW,attrs)
os.write(1,b'READY')
while True:
 b=os.read(0,1)
 if not b: break
 os.write(1,b)
"
        .into(),
    ];
    launch
}

#[tokio::test]
async fn native_local_seed_pressure_preserves_two_original_runs_and_raw_controls() {
    use ctxmux_client::{ClientError, TerminalSeedLimits, TerminalSeedResourceError};
    use ctxmux_protocol::RunEvent;

    let daemon = Daemon::new(false).await;
    let runtime = daemon.client.runtime_info().await.unwrap();
    let launch = seed_pressure_spec();
    let first = daemon.client.start(launch.clone()).await.unwrap();
    let second = daemon.client.start(launch).await.unwrap();
    timeout(Duration::from_secs(5), async {
        for run in [&first, &second] {
            while daemon
                .client
                .status(run.id)
                .await
                .unwrap()
                .latest_output_bytes
                != 5
            {
                sleep(Duration::from_millis(10)).await;
            }
        }
    })
    .await
    .unwrap();
    let limited = daemon
        .client
        .clone()
        .with_terminal_seed_limits(TerminalSeedLimits { restore_bytes: 0 });
    let (raw, before) = limited.attach(first.id, 0).await.unwrap();
    let (other, other_before) = daemon.client.attach(second.id, 0).await.unwrap();
    for snapshot in [&before, &other_before] {
        assert_eq!(
            snapshot
                .replay
                .chunks
                .iter()
                .flat_map(|chunk| chunk.data.iter())
                .copied()
                .collect::<Vec<_>>(),
            b"READY"
        );
    }
    assert!(matches!(limited.attach_terminal(first.id, 0).await,
        Err(ClientError::TerminalSeedResource(TerminalSeedResourceError::RestoreLimit { requested_bytes, limit_bytes: 0 })) if requested_bytes > 0));
    for (view, original) in [(&raw, &first), (&other, &second)] {
        assert_eq!(
            view.input(b"Z".to_vec())
                .await
                .unwrap()
                .receipt
                .written_bytes,
            1
        );
        timeout(Duration::from_secs(5), async {
            loop {
                if let Some(RunEvent::Output { chunk }) = view.next_event().await.unwrap() {
                    assert_eq!(chunk.start_byte, 5);
                    assert_eq!(chunk.end_byte, 6);
                    assert_eq!(chunk.data, b"Z");
                    break;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(
            daemon.client.status(original.id).await.unwrap().pid,
            original.pid
        );
    }
    let (restored, snapshot) = daemon.client.attach_terminal(first.id, 0).await.unwrap();
    assert_eq!(snapshot.run.id, first.id);
    assert_eq!(snapshot.run.pid, first.pid);
    assert!(restore(&snapshot).screen().contents().contains("READYZ"));
    restored.detach().await.unwrap();
    other.input(vec![3]).await.unwrap();
    timeout(Duration::from_secs(5), async {
        loop {
            if matches!(
                other.next_event().await.unwrap(),
                Some(RunEvent::Exited { .. })
            ) {
                break;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(limited.runtime_info().await.unwrap(), runtime);
    assert_eq!(daemon.client.runtime_info().await.unwrap(), runtime);
    raw.detach().await.unwrap();
    daemon.stop_run(first.id).await;
}
