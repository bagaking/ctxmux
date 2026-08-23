use std::{
    collections::{BTreeMap, BTreeSet},
    io::{self, BufRead, BufReader},
    path::Path,
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    time::Duration,
};

use ctxmux_client::{Attachment, Client, ClientError, replay_bytes};
use ctxmux_protocol::{
    AttachmentCommandId, ClientFrame, ClientHello, CommandDisposition, ControlOutcome,
    CreateOperationKey, ErrorCode, ForkFidelity, ForkPlan, InputOperationKey, MAX_FRAME_BYTES,
    PROTOCOL_VERSION, RecoverableInput, Request, RunEvent, RunId, RunInputKind, RunInputReference,
    RunLineage, RunSignal, RunSpec, RunState, ServerFrame, StopDisposition, TerminalSize,
    decode_frame, encode_frame,
};
use futures_util::{SinkExt, StreamExt, future::join_all};
use serde_json::Value;
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
    time::{sleep, timeout},
};
use tokio_util::codec::{Framed, LinesCodec};

struct TestDaemon {
    child: Child,
    directory: TempDir,
    client: Client,
    /// Lines captured from a stderr-piped daemon, shared with a drain thread.
    /// `None` for the inherit-stderr constructors, which do not scan stderr.
    stderr_lines: Option<Arc<Mutex<Vec<String>>>>,
}

impl TestDaemon {
    async fn start() -> Self {
        let directory = tempfile::tempdir().expect("create daemon temp directory");
        let socket = directory.path().join("ctxmux.sock");
        let child = Command::new(env!("CARGO_BIN_EXE_ctxmuxd"))
            .arg("--socket")
            .arg(&socket)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn ctxmuxd");
        Self::from_spawned(child, directory, socket).await
    }

    async fn start_with_inherited_fd(sentinel: &Path) -> Self {
        let directory = tempfile::tempdir().expect("create daemon temp directory");
        let socket = directory.path().join("ctxmux.sock");
        let child = Command::new("/bin/sh")
            .arg("-c")
            .arg("exec 9<\"$1\"; shift; exec \"$@\"")
            .arg("ctxmux-inherited-fd-fixture")
            .arg(sentinel)
            .arg(env!("CARGO_BIN_EXE_ctxmuxd"))
            .arg("--socket")
            .arg(&socket)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn ctxmuxd with inherited descriptor");
        Self::from_spawned(child, directory, socket).await
    }

    async fn start_with_qualification_stats_fd(sentinel: &Path) -> Self {
        let directory = tempfile::tempdir().expect("create daemon temp directory");
        let socket = directory.path().join("ctxmux.sock");
        let child = Command::new("/bin/sh")
            .arg("-c")
            .arg("exec 3>\"$1\"; shift; exec \"$@\" --qualification-stats-fd 3")
            .arg("ctxmux-qualification-fd-fixture")
            .arg(sentinel)
            .arg(env!("CARGO_BIN_EXE_ctxmuxd"))
            .arg("--socket")
            .arg(&socket)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn ctxmuxd with qualification descriptor");
        Self::from_spawned(child, directory, socket).await
    }

    async fn start_with_readiness_fd(receipt: &Path) -> Self {
        let directory = tempfile::tempdir().expect("create daemon temp directory");
        let socket = directory.path().join("ctxmux.sock");
        let child = Command::new("/bin/sh")
            .arg("-c")
            .arg("exec 3>\"$1\"; shift; exec \"$@\" --readiness-fd 3")
            .arg("ctxmux-readiness-fd-fixture")
            .arg(receipt)
            .arg(env!("CARGO_BIN_EXE_ctxmuxd"))
            .arg("--socket")
            .arg(&socket)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn ctxmuxd with readiness descriptor");
        Self::from_spawned(child, directory, socket).await
    }

    async fn from_spawned(
        child: Child,
        directory: TempDir,
        socket: impl Into<std::path::PathBuf>,
    ) -> Self {
        Self::from_spawned_with_stderr(child, directory, socket, None).await
    }

    /// Start a persistent daemon (`--state-dir`) with stderr piped so the
    /// incoming handoff image's resume log line can be scanned. A drain thread
    /// reads the pipe line-by-line into a shared buffer so the tokio runtime is
    /// never blocked on the synchronous pipe. The thread ends when the daemon
    /// dies and closes the pipe (see `Drop`).
    async fn start_persistent() -> Self {
        let directory = tempfile::tempdir().expect("create daemon temp directory");
        let socket = directory.path().join("ctxmux.sock");
        let state_dir = directory.path().join("state");
        let mut child = Command::new(env!("CARGO_BIN_EXE_ctxmuxd"))
            .arg("--socket")
            .arg(&socket)
            .arg("--state-dir")
            .arg(&state_dir)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn persistent ctxmuxd");

        let stderr = child.stderr.take().expect("persistent daemon exposes stderr");
        let stderr_lines = Arc::new(Mutex::new(Vec::new()));
        let drain = Arc::clone(&stderr_lines);
        std::thread::spawn(move || {
            let reader = BufReader::new(stderr);
            for line in reader.lines() {
                match line {
                    Ok(line) => {
                        // Mirror the daemon's stderr so `--nocapture` runs stay
                        // observable while iterating.
                        eprintln!("[ctxmuxd stderr] {line}");
                        drain.lock().expect("stderr buffer lock").push(line);
                    }
                    Err(_) => break,
                }
            }
        });

        Self::from_spawned_with_stderr(child, directory, socket, Some(stderr_lines)).await
    }

    async fn from_spawned_with_stderr(
        child: Child,
        directory: TempDir,
        socket: impl Into<std::path::PathBuf>,
        stderr_lines: Option<Arc<Mutex<Vec<String>>>>,
    ) -> Self {
        let socket = socket.into();
        let client = Client::new(socket);
        let mut daemon = Self {
            child,
            directory,
            client,
            stderr_lines,
        };

        timeout(Duration::from_secs(5), async {
            loop {
                if let Some(status) = daemon.child.try_wait().expect("poll ctxmuxd") {
                    panic!("ctxmuxd exited before accepting connections: {status}");
                }
                if daemon.client.ping().await.is_ok() {
                    break;
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("ctxmuxd should accept connections");
        daemon
    }

    /// Deliver a real `SIGHUP` to the daemon process and assert delivery.
    fn sighup(&self) {
        let delivered = Command::new("kill")
            .arg("-HUP")
            .arg(self.child.id().to_string())
            .status()
            .expect("send SIGHUP to ctxmuxd")
            .success();
        assert!(delivered, "SIGHUP should be delivered to ctxmuxd");
    }

    /// Poll the piped stderr buffer until the incoming handoff image logs its
    /// resume line, then return that line so the run count can be asserted.
    async fn wait_resume_signal(&self, timeout_secs: u64) -> String {
        let lines = self
            .stderr_lines
            .as_ref()
            .expect("wait_resume_signal requires a stderr-piped daemon");
        timeout(Duration::from_secs(timeout_secs), async {
            loop {
                {
                    let captured = lines.lock().expect("stderr buffer lock");
                    if let Some(line) = captured
                        .iter()
                        .find(|line| line.contains("adopted inherited listener for handoff"))
                    {
                        return line.clone();
                    }
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("incoming handoff image should log its resume signal")
    }
}

impl Drop for TestDaemon {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let interrupted = Command::new("kill")
                .arg("-INT")
                .arg(self.child.id().to_string())
                .status()
                .is_ok_and(|status| status.success());
            if interrupted {
                for _ in 0..100 {
                    match self.child.try_wait() {
                        Ok(Some(_)) => return,
                        Ok(None) => std::thread::sleep(Duration::from_millis(20)),
                        Err(_) => break,
                    }
                }
            }
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
    }
}

fn interactive_shell() -> RunSpec {
    RunSpec {
        program: "/bin/sh".to_owned(),
        args: vec![
            "-c".to_owned(),
            concat!(
                "printf 'READY\\n'; ",
                "while IFS= read -r line; do ",
                "case \"$line\" in ",
                "size) printf 'SIZE:'; stty size ;; ",
                "burst=*) n=${line#burst=}; i=0; ",
                "while [ \"$i\" -lt \"$n\" ]; do printf 'OUT:burst-%06d\\n' \"$i\"; i=$((i+1)); done ;; ",
                "quit) printf 'OUT:quit\\n'; exit 7 ;; ",
                "*) printf 'OUT:%s\\n' \"$line\" ;; ",
                "esac; done"
            )
            .to_owned(),
        ],
        cwd: None,
        env: BTreeMap::default(),
        size: TerminalSize::default(),
        declared_inputs: Vec::new(),
    }
}

fn raw_capture_shell(expected_bytes: usize) -> RunSpec {
    RunSpec {
        program: "/bin/sh".to_owned(),
        args: vec![
            "-c".to_owned(),
            format!(
                concat!(
                    "stty raw -echo; ",
                    "printf 'READY\\n'; ",
                    "dd bs=1 count={} 2>/dev/null | od -An -v -tx1; ",
                    "trap 'printf \\\"FINAL\\n\\\"; exit 0' HUP TERM; ",
                    "printf 'CAPTURED\\n'; ",
                    "while IFS= read -r ignored; do :; done"
                ),
                expected_bytes
            ),
        ],
        cwd: None,
        env: BTreeMap::default(),
        size: TerminalSize::default(),
        declared_inputs: Vec::new(),
    }
}

fn non_reading_shell() -> RunSpec {
    RunSpec {
        program: "/bin/sh".to_owned(),
        args: vec![
            "-c".to_owned(),
            "stty raw -echo; printf 'READY\\n'; exec /bin/sleep 30".to_owned(),
        ],
        cwd: None,
        env: BTreeMap::default(),
        size: TerminalSize::default(),
        declared_inputs: Vec::new(),
    }
}

fn fragmented_terminal_chunks() -> Vec<Vec<u8>> {
    let fragments = [
        b"\x1b[".as_slice(),
        b"200~".as_slice(),
        b"pasted".as_slice(),
        b"-bytes".as_slice(),
        b"\x1b".as_slice(),
        b"[201~".as_slice(),
        b"\x1b[<".as_slice(),
        b"0;40;12".as_slice(),
        b"M".as_slice(),
        b"raw".as_slice(),
    ];
    (0..1_000)
        .map(|index| fragments[index % fragments.len()].to_vec())
        .collect()
}

fn marker_shell(marker: &Path) -> RunSpec {
    RunSpec {
        program: "/bin/sh".to_owned(),
        args: vec![
            "-c".to_owned(),
            "printf '%s\\n' \"$$\" >> \"$CTXMUX_CREATION_MARKER\"; exec /bin/cat".to_owned(),
        ],
        cwd: None,
        env: BTreeMap::from([(
            "CTXMUX_CREATION_MARKER".to_owned(),
            marker.to_string_lossy().into_owned(),
        )]),
        size: TerminalSize::default(),
        declared_inputs: Vec::new(),
    }
}

async fn wait_for_marker_pids(marker: &Path, expected: usize) -> Vec<u32> {
    timeout(Duration::from_secs(5), async {
        loop {
            let pids = std::fs::read_to_string(marker)
                .unwrap_or_default()
                .lines()
                .map(|line| line.parse::<u32>().expect("marker records a child PID"))
                .collect::<Vec<_>>();
            if pids.len() == expected {
                return pids;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("marker reaches the expected execution count")
}

struct UnrelatedProcess(Child);

impl UnrelatedProcess {
    fn spawn() -> Self {
        Self(
            Command::new("/bin/sleep")
                .arg("30")
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("spawn unrelated process sentinel"),
        )
    }

    fn pid(&self) -> u32 {
        self.0.id()
    }
}

impl Drop for UnrelatedProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn send_request_without_reading_response(client: &Client, request: Request) {
    let stream = UnixStream::connect(client.socket_path())
        .await
        .expect("connect response-loss client");
    let mut wire = Framed::new(stream, LinesCodec::new_with_max_length(MAX_FRAME_BYTES));
    wire.send(
        encode_frame(&ClientFrame::Hello {
            hello: ClientHello {
                protocol: PROTOCOL_VERSION,
            },
        })
        .expect("encode response-loss hello"),
    )
    .await
    .expect("send response-loss hello");
    let hello = wire
        .next()
        .await
        .expect("daemon sends hello")
        .expect("read daemon hello");
    assert!(matches!(
        decode_frame::<ServerFrame>(&hello).expect("decode daemon hello"),
        ServerFrame::Hello {
            protocol: PROTOCOL_VERSION,
            ..
        }
    ));
    wire.send(encode_frame(&ClientFrame::Request { request }).expect("encode abandoned request"))
        .await
        .expect("send abandoned request completely");
    drop(wire);
}

async fn receive_server_frame(
    wire: &mut Framed<UnixStream, LinesCodec>,
    context: &str,
) -> ServerFrame {
    let line = timeout(Duration::from_secs(5), wire.next())
        .await
        .unwrap_or_else(|_| panic!("timed out while {context}"))
        .unwrap_or_else(|| panic!("daemon closed while {context}"))
        .unwrap_or_else(|error| panic!("transport failed while {context}: {error}"));
    decode_frame(&line)
        .unwrap_or_else(|error| panic!("invalid server frame while {context}: {error}"))
}

async fn wait_for_run_count(client: &Client, expected: usize) -> Vec<ctxmux_protocol::RunInfo> {
    timeout(Duration::from_secs(5), async {
        loop {
            let runs = client.list().await.expect("list response-loss Runs");
            if runs.len() == expected {
                return runs;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("abandoned request reaches Run publication")
}

fn fork_inputs() -> Vec<RunInputReference> {
    [
        (RunInputKind::Workspace, "workspace://ctxmux-fixture"),
        (RunInputKind::Artifact, "artifact://plan.json"),
        (RunInputKind::Context, "context://parent-turn"),
    ]
    .into_iter()
    .map(|(kind, reference)| RunInputReference {
        kind,
        reference: reference.to_owned(),
    })
    .collect()
}

async fn wait_for_output(
    attachment: &mut Attachment,
    observed: &mut Vec<u8>,
    last_byte: &mut u64,
    expected: &[u8],
) {
    timeout(Duration::from_secs(5), async {
        while !observed
            .windows(expected.len())
            .any(|window| window == expected)
        {
            match attachment
                .next_event()
                .await
                .expect("receive attachment event")
                .expect("attachment remains live")
            {
                RunEvent::Output { chunk } => {
                    assert_eq!(chunk.start_byte, *last_byte);
                    *last_byte = chunk.end_byte;
                    observed.extend_from_slice(&chunk.data);
                }
                RunEvent::Gap {
                    latest_output_bytes,
                } => panic!("unexpected output gap at {latest_output_bytes}"),
                RunEvent::Exited { state } => {
                    panic!("Run exited before expected output: {state:?}")
                }
                RunEvent::Interrupted { reason } => {
                    panic!("Run was interrupted before expected output: {reason:?}")
                }
                RunEvent::Tmux { event } => panic!("unexpected tmux event: {event:?}"),
            }
        }
    })
    .await
    .expect("expected PTY output should arrive");
}

async fn wait_for_exit(attachment: &mut Attachment) -> RunState {
    timeout(Duration::from_secs(5), async {
        loop {
            match attachment
                .next_event()
                .await
                .expect("receive attachment event")
                .expect("exit event arrives before attachment closes")
            {
                RunEvent::Exited { state } => return state,
                RunEvent::Output { .. } => {}
                RunEvent::Gap {
                    latest_output_bytes,
                } => panic!("unexpected output gap at {latest_output_bytes}"),
                RunEvent::Interrupted { reason } => {
                    panic!("live Run was unexpectedly interrupted: {reason:?}")
                }
                RunEvent::Tmux { event } => panic!("unexpected tmux event: {event:?}"),
            }
        }
    })
    .await
    .expect("Run should exit")
}

async fn wait_until_exited(client: &Client, id: RunId) -> RunState {
    timeout(Duration::from_secs(5), async {
        loop {
            let state = client.status(id).await.expect("read Run state").state;
            if !state.is_running() {
                return state;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("Run should reach terminal state")
}

async fn assert_raw_connection_closes(client: &Client, frame: &[u8], newline: bool) {
    let mut stream = UnixStream::connect(client.socket_path())
        .await
        .expect("connect malformed-wire fixture");
    stream
        .write_all(frame)
        .await
        .expect("write malformed-wire fixture");
    if newline {
        match stream.write_all(b"\n").await {
            Ok(()) => {}
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::ConnectionReset | io::ErrorKind::BrokenPipe
                ) =>
            {
                return;
            }
            Err(error) => panic!("terminate malformed-wire fixture: {error}"),
        }
    }

    assert_connection_closes(stream).await;
}

async fn assert_malformed_request_closes(client: &Client, frame: &[u8]) {
    let stream = UnixStream::connect(client.socket_path())
        .await
        .expect("connect malformed request fixture");
    let mut wire = Framed::new(stream, LinesCodec::new_with_max_length(MAX_FRAME_BYTES));
    wire.send(
        encode_frame(&ClientFrame::Hello {
            hello: ClientHello {
                protocol: PROTOCOL_VERSION,
            },
        })
        .expect("encode fixture handshake"),
    )
    .await
    .expect("send fixture handshake");
    let response = timeout(Duration::from_secs(5), wire.next())
        .await
        .expect("fixture handshake should settle")
        .expect("daemon sends fixture handshake response")
        .expect("read fixture handshake response");
    assert!(matches!(
        decode_frame::<ServerFrame>(&response).expect("decode fixture handshake response"),
        ServerFrame::Hello {
            protocol: PROTOCOL_VERSION,
            ..
        }
    ));

    let mut stream = wire.into_inner();
    stream
        .write_all(frame)
        .await
        .expect("write malformed request fixture");
    stream
        .write_all(b"\n")
        .await
        .expect("terminate malformed request fixture");
    assert_connection_closes(stream).await;
}

async fn assert_connection_closes(mut stream: UnixStream) {
    let mut byte = [0];
    let result = timeout(Duration::from_secs(5), stream.read(&mut byte))
        .await
        .expect("daemon should settle malformed connection");
    match result {
        Ok(0) => {}
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::ConnectionReset | io::ErrorKind::BrokenPipe
            ) => {}
        other => panic!("expected malformed connection to close, got {other:?}"),
    }
}

fn malformed_protocol_frames() -> Vec<(String, Vec<u8>)> {
    let corpus: serde_json::Value = serde_json::from_str(include_str!(
        "../../../fixtures/malformed-protocol-frames.json"
    ))
    .expect("parse shared malformed-frame corpus");
    corpus["frames"]
        .as_array()
        .expect("corpus frames are an array")
        .iter()
        .map(|frame| {
            let id = frame["id"]
                .as_str()
                .expect("frame id is a string")
                .to_owned();
            let bytes = frame["bytes"]
                .as_array()
                .expect("frame bytes are an array")
                .iter()
                .map(|byte| {
                    u8::try_from(byte.as_u64().expect("frame byte is an unsigned integer"))
                        .expect("frame byte fits in u8")
                })
                .collect();
            (id, bytes)
        })
        .collect()
}

fn assert_protocol_error(error: ClientError, expected: ErrorCode) {
    match error {
        ClientError::Protocol { code, .. } => assert_eq!(code, expected),
        ClientError::ControlRejected { failure } => assert_eq!(failure.error.code, expected),
        other => panic!("expected protocol error {expected:?}, got {other:?}"),
    }
}

fn process_exists(pid: u32) -> bool {
    Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(
    clippy::too_many_lines,
    reason = "one continuous lifecycle test makes the disconnect and PID identity proof auditable"
)]
async fn run_survives_attachment_disconnect_and_reconnects_to_the_same_child() {
    let daemon = TestDaemon::start().await;
    let run = daemon
        .client
        .start(interactive_shell())
        .await
        .expect("start native Run");
    let pid = run.pid.expect("shell exposes a process id");

    let (mut first_attachment, first_snapshot) = daemon
        .client
        .attach(run.id, 0)
        .await
        .expect("attach to native Run");
    let mut observed = replay_bytes(&first_snapshot.replay.chunks);
    let mut last_seq = first_snapshot.replay.latest_output_bytes;
    if !observed.windows(5).any(|window| window == b"READY") {
        wait_for_output(
            &mut first_attachment,
            &mut observed,
            &mut last_seq,
            b"READY",
        )
        .await;
    }
    first_attachment
        .input(b"hello\n".to_vec())
        .await
        .expect("write through attachment");
    wait_for_output(
        &mut first_attachment,
        &mut observed,
        &mut last_seq,
        b"OUT:hello",
    )
    .await;

    drop(first_attachment);
    let status_after_disconnect = timeout(Duration::from_secs(5), async {
        loop {
            let status = daemon.client.status(run.id).await.expect("read Run status");
            if status.attachments == 0 {
                return status;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("daemon should release disconnected attachment");
    assert_eq!(status_after_disconnect.pid, Some(pid));
    assert_eq!(status_after_disconnect.state, RunState::Running);

    daemon
        .client
        .resize(
            run.id,
            TerminalSize {
                cols: 120,
                rows: 40,
            },
        )
        .await
        .expect("resize live PTY");
    assert_protocol_error(
        daemon
            .client
            .resize(run.id, TerminalSize { cols: 0, rows: 40 })
            .await
            .expect_err("zero terminal width is rejected"),
        ErrorCode::InvalidRequest,
    );

    let (mut second_attachment, second_snapshot) = daemon
        .client
        .attach(run.id, last_seq)
        .await
        .expect("reattach through a fresh connection");
    assert_eq!(second_snapshot.run.pid, Some(pid));
    assert!(!second_snapshot.replay.truncated);
    observed = replay_bytes(&second_snapshot.replay.chunks);
    last_seq = second_snapshot.replay.latest_output_bytes;
    second_attachment
        .input(b"size\n".to_vec())
        .await
        .expect("request terminal size");
    wait_for_output(
        &mut second_attachment,
        &mut observed,
        &mut last_seq,
        b"SIZE:40 120",
    )
    .await;
    second_attachment
        .input(b"quit\n".to_vec())
        .await
        .expect("request shell exit");
    wait_for_output(
        &mut second_attachment,
        &mut observed,
        &mut last_seq,
        b"OUT:quit",
    )
    .await;
    assert_eq!(
        wait_for_exit(&mut second_attachment).await,
        RunState::Exited {
            code: 7,
            signal: None,
        }
    );

    let final_status = daemon.client.status(run.id).await.expect("read exit state");
    assert_eq!(final_status.pid, Some(pid));
    assert_eq!(
        final_status.state,
        RunState::Exited {
            code: 7,
            signal: None,
        }
    );
    assert_protocol_error(
        daemon
            .client
            .input(run.id, b"after exit".to_vec())
            .await
            .expect_err("exited Run rejects input"),
        ErrorCode::InvalidRunState,
    );
    assert_protocol_error(
        daemon
            .client
            .status(RunId::new())
            .await
            .expect_err("unknown Run is rejected"),
        ErrorCode::RunNotFound,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(
    clippy::too_many_lines,
    reason = "one real-PTY proof keeps command correlation, raw-byte fidelity, resize readback, and stop ordering auditable together"
)]
async fn attachment_pipeline_preserves_raw_bytes_applied_size_and_stop_ordering() {
    let daemon = TestDaemon::start().await;
    let chunks = fragmented_terminal_chunks();
    let expected = chunks.concat();
    let run = daemon
        .client
        .start(raw_capture_shell(expected.len()))
        .await
        .expect("start raw capture Run");
    let (mut attachment, snapshot) = daemon
        .client
        .attach(run.id, 0)
        .await
        .expect("attach raw pipeline client");
    let mut observed = replay_bytes(&snapshot.replay.chunks);
    let mut last_seq = snapshot.replay.latest_output_bytes;
    if !observed.windows(5).any(|window| window == b"READY") {
        wait_for_output(&mut attachment, &mut observed, &mut last_seq, b"READY").await;
    }

    let requested_size = TerminalSize {
        rows: 41,
        cols: 123,
    };
    let mut command_ids = Vec::with_capacity(1_001);
    let mut resize_receipt = None;
    for (window_index, window) in chunks.chunks(31).enumerate() {
        let input_results = join_all(window.iter().cloned().map(|data| {
            let attachment = &attachment;
            async move {
                let expected_bytes = u32::try_from(data.len()).expect("fixture chunk fits u32");
                let accepted = attachment
                    .input(data)
                    .await
                    .expect("pipeline input accepted");
                assert_eq!(accepted.receipt.written_bytes, expected_bytes);
                accepted.command_id.get()
            }
        }));
        if window_index == 7 {
            let (input_ids, resize) =
                tokio::join!(input_results, attachment.resize(requested_size));
            command_ids.extend(input_ids);
            let resize = resize.expect("concurrent resize accepted");
            command_ids.push(resize.command_id.get());
            resize_receipt = Some(resize.receipt);
        } else {
            command_ids.extend(input_results.await);
        }
    }
    command_ids.sort_unstable();
    assert_eq!(
        command_ids,
        (1..=1_001).collect::<Vec<_>>(),
        "every pipelined command has one unique correlated result"
    );
    assert_eq!(
        resize_receipt
            .expect("fixture issues one resize")
            .applied_size,
        requested_size,
        "resize receipt comes from PTY readback"
    );

    wait_for_output(&mut attachment, &mut observed, &mut last_seq, b"CAPTURED").await;
    let ready = observed
        .windows(b"READY\n".len())
        .position(|window| window == b"READY\n")
        .expect("raw child published readiness")
        + b"READY\n".len();
    let captured = observed[ready..]
        .windows(b"CAPTURED".len())
        .position(|window| window == b"CAPTURED")
        .expect("raw child published capture marker")
        + ready;
    let oracle = std::str::from_utf8(&observed[ready..captured])
        .expect("od byte oracle is ASCII")
        .split_ascii_whitespace()
        .map(|byte| u8::from_str_radix(byte, 16).expect("od emits hexadecimal bytes"))
        .collect::<Vec<_>>();
    assert_eq!(
        oracle, expected,
        "real PTY preserved every opaque input byte"
    );

    let mut stop = Box::pin(attachment.stop());
    let mut stop_command_id = None;
    let mut after_stop_output = Vec::new();
    let terminal = timeout(Duration::from_secs(5), async {
        loop {
            tokio::select! {
                biased;
                accepted = &mut stop, if stop_command_id.is_none() => {
                    let accepted = accepted.expect("stop reaches child owner");
                    stop_command_id = Some(accepted.command_id.get());
                }
                event = attachment.next_event() => {
                    match event
                        .expect("read post-stop event")
                        .expect("terminal event precedes attachment EOF")
                    {
                        RunEvent::Output { chunk } => after_stop_output.extend_from_slice(&chunk.data),
                        RunEvent::Exited { state } => {
                            assert!(stop_command_id.is_some(), "stop receipt precedes Exited");
                            assert!(
                                after_stop_output.windows(b"FINAL".len()).any(|bytes| bytes == b"FINAL"),
                                "final child output precedes Exited"
                            );
                            return state;
                        }
                        RunEvent::Gap { latest_output_bytes } => panic!("unexpected post-stop gap at {latest_output_bytes}"),
                        RunEvent::Interrupted { reason } => panic!("native Run interrupted: {reason:?}"),
                        RunEvent::Tmux { event } => panic!("unexpected tmux event: {event:?}"),
                    }
                }
            }
        }
    })
    .await
    .expect("stop receipt, final output, and exit arrive");
    assert_eq!(stop_command_id, Some(1_002));
    assert_eq!(
        terminal,
        RunState::Exited {
            code: 0,
            signal: None,
        }
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(
    clippy::too_many_lines,
    reason = "one non-reading real PTY proves daemon backpressure and the independent resize/stop lanes under saturation"
)]
async fn saturated_real_pty_backpressures_input_without_starving_resize_or_stop() {
    const SEED_ATTACHMENTS: usize = 17;
    const INPUTS_PER_ATTACHMENT: usize = 32;
    const INPUT_BYTES: usize = 8 * 1024;

    let daemon = TestDaemon::start().await;
    let run = daemon
        .client
        .start(non_reading_shell())
        .await
        .expect("start non-reading PTY Run");
    let (mut control, snapshot) = daemon
        .client
        .attach(run.id, 0)
        .await
        .expect("attach independent control lane");
    let mut observed = replay_bytes(&snapshot.replay.chunks);
    let mut last_seq = snapshot.replay.latest_output_bytes;
    if !observed.windows(5).any(|window| window == b"READY") {
        wait_for_output(&mut control, &mut observed, &mut last_seq, b"READY").await;
    }

    let mut seed_tasks = Vec::with_capacity(SEED_ATTACHMENTS);
    for _ in 0..SEED_ATTACHMENTS {
        let (attachment, _) = daemon
            .client
            .attach(run.id, last_seq)
            .await
            .expect("attach input saturation client");
        seed_tasks.push(tokio::spawn(async move {
            let payload = vec![b'x'; INPUT_BYTES];
            join_all((0..INPUTS_PER_ATTACHMENT).map(|_| attachment.input(payload.clone()))).await
        }));
    }

    timeout(Duration::from_secs(8), async {
        loop {
            let (probe, _) = daemon
                .client
                .attach(run.id, last_seq)
                .await
                .expect("attach input backpressure probe");
            match timeout(
                Duration::from_millis(250),
                probe.input(vec![b'p'; INPUT_BYTES]),
            )
            .await
            {
                Ok(Err(ClientError::ControlRejected { failure }))
                    if failure.error.code == ErrorCode::ControlBackpressure =>
                {
                    assert_eq!(failure.disposition, CommandDisposition::NotApplied);
                    return;
                }
                Ok(Ok(_)) | Err(_) => drop(probe),
                Ok(Err(error)) => panic!("unexpected saturation probe result: {error:?}"),
            }
        }
    })
    .await
    .expect("real PTY reaches the daemon input queue bound");

    let requested_size = TerminalSize {
        rows: 43,
        cols: 127,
    };
    let resize = timeout(Duration::from_secs(2), control.resize(requested_size))
        .await
        .expect("resize is not starved by saturated input")
        .expect("resize reaches the PTY owner");
    assert_eq!(resize.command_id.get(), 1);
    assert_eq!(resize.receipt.applied_size, requested_size);
    let stop = timeout(Duration::from_secs(3), control.stop())
        .await
        .expect("stop is not starved by saturated input")
        .expect("stop reaches the child owner");
    assert_eq!(stop.command_id.get(), 2);

    let terminal = timeout(Duration::from_secs(5), async {
        loop {
            match control
                .next_event()
                .await
                .expect("read saturated Run event")
                .expect("Exited precedes attachment EOF")
            {
                RunEvent::Exited { state } => return state,
                RunEvent::Output { .. } => {}
                RunEvent::Gap {
                    latest_output_bytes,
                } => panic!("unexpected saturation gap at {latest_output_bytes}"),
                RunEvent::Interrupted { reason } => panic!("native Run interrupted: {reason:?}"),
                RunEvent::Tmux { event } => panic!("unexpected tmux event: {event:?}"),
            }
        }
    })
    .await
    .expect("saturated Run publishes Exited after stop acceptance");
    assert!(!terminal.is_running());

    let mut not_applied = 0;
    for task in seed_tasks {
        let results = timeout(Duration::from_secs(5), task)
            .await
            .expect("saturated input task resolves after stop")
            .expect("saturated input task does not panic");
        for result in results {
            match result {
                Ok(_) | Err(ClientError::AttachmentCommandUnknown { .. }) => {}
                Err(ClientError::ControlRejected { failure }) => {
                    if failure.disposition == CommandDisposition::NotApplied {
                        not_applied += 1;
                    }
                }
                Err(error) => panic!("unexpected saturated input result: {error:?}"),
            }
        }
    }
    assert!(
        not_applied > 0,
        "stop rejects queued commands that never reached PTY I/O"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(
    clippy::too_many_lines,
    reason = "one raw connection proves the command-id fence precedes PTY mutation and closes transport"
)]
async fn backward_attachment_command_id_is_fatal_before_input_mutation() {
    let daemon = TestDaemon::start().await;
    let run = daemon
        .client
        .start(interactive_shell())
        .await
        .expect("start command-id fence Run");
    let stream = UnixStream::connect(daemon.client.socket_path())
        .await
        .expect("connect raw attachment");
    let mut wire = Framed::new(stream, LinesCodec::new_with_max_length(MAX_FRAME_BYTES));
    wire.send(
        encode_frame(&ClientFrame::Hello {
            hello: ClientHello {
                protocol: PROTOCOL_VERSION,
            },
        })
        .expect("encode current hello"),
    )
    .await
    .expect("send current hello");
    assert!(matches!(
        receive_server_frame(&mut wire, "reading attachment hello").await,
        ServerFrame::Hello {
            protocol: PROTOCOL_VERSION,
            ..
        }
    ));
    wire.send(
        encode_frame(&ClientFrame::Request {
            request: Request::Attach {
                id: run.id,
                after_byte: 0,
            },
        })
        .expect("encode raw attach"),
    )
    .await
    .expect("send raw attach");
    assert!(matches!(
        receive_server_frame(&mut wire, "reading attached header").await,
        ServerFrame::Attached { .. }
    ));

    for (command_id, data) in [(1, b"A".to_vec()), (3, Vec::new())] {
        let command_id = AttachmentCommandId::new(command_id).unwrap();
        wire.send(
            encode_frame(&ClientFrame::Input { command_id, data }).expect("encode ordered input"),
        )
        .await
        .expect("send ordered input");
        loop {
            if let ServerFrame::CommandResult {
                command_id: returned,
                outcome: ControlOutcome::Accepted { .. },
            } = receive_server_frame(&mut wire, "awaiting ordered command result").await
            {
                assert_eq!(returned, command_id);
                break;
            }
        }
    }

    wire.send(
        encode_frame(&ClientFrame::Input {
            command_id: AttachmentCommandId::new(2).unwrap(),
            data: b"B".to_vec(),
        })
        .expect("encode backward input"),
    )
    .await
    .expect("send backward input");
    loop {
        if let ServerFrame::Error { error } =
            receive_server_frame(&mut wire, "awaiting fatal command-id error").await
        {
            assert_eq!(error.code, ErrorCode::InvalidRequest);
            break;
        }
    }
    assert!(
        timeout(Duration::from_secs(5), wire.next())
            .await
            .expect("fatal attachment close is bounded")
            .is_none(),
        "command-id violation closes the attachment"
    );

    let (mut attachment, snapshot) = daemon
        .client
        .attach(run.id, 0)
        .await
        .expect("reattach after fatal command-id violation");
    let mut observed = replay_bytes(&snapshot.replay.chunks);
    let mut last_seq = snapshot.replay.latest_output_bytes;
    let reconnected = attachment
        .input(b"\n".to_vec())
        .await
        .expect("complete the line after reattach");
    assert_eq!(
        reconnected.command_id.get(),
        1,
        "a fresh attachment restarts its connection-local command IDs"
    );
    wait_for_output(&mut attachment, &mut observed, &mut last_seq, b"OUT:A").await;
    assert!(
        !observed
            .windows(b"OUT:AB".len())
            .any(|bytes| bytes == b"OUT:AB"),
        "backward command mutated the PTY before the fatal fence"
    );
    attachment.stop().await.expect("stop command-id fence Run");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_start_and_fork_recover_after_the_response_is_abandoned() {
    let daemon = TestDaemon::start().await;
    let marker = daemon.directory.path().join("creation-processes.log");
    let spec = marker_shell(&marker);
    let unrelated = UnrelatedProcess::spawn();
    let start_key = CreateOperationKey::new("public-abandoned-start").unwrap();
    send_request_without_reading_response(
        &daemon.client,
        Request::Start {
            operation_key: start_key.clone(),
            spec: spec.clone(),
        },
    )
    .await;
    let published = wait_for_run_count(&daemon.client, 1).await;
    let parent = published[0].clone();
    let parent_pid = parent.pid.expect("abandoned Start has one child PID");
    assert_eq!(wait_for_marker_pids(&marker, 1).await, vec![parent_pid]);
    let retried_parent = daemon
        .client
        .start_with_operation_key(spec, start_key)
        .await
        .expect("Start retry returns the abandoned response Run");
    assert_eq!(retried_parent.id, parent.id);
    assert_eq!(retried_parent.pid, parent.pid);
    assert_eq!(wait_for_marker_pids(&marker, 1).await, vec![parent_pid]);
    assert!(process_exists(unrelated.pid()));

    let fork_key = CreateOperationKey::new("public-abandoned-fork").unwrap();
    send_request_without_reading_response(
        &daemon.client,
        Request::Fork {
            operation_key: fork_key.clone(),
            parent: parent.id,
            plan: ForkPlan::LevelA,
        },
    )
    .await;
    let published = wait_for_run_count(&daemon.client, 2).await;
    let child = published
        .iter()
        .find(|run| run.id != parent.id)
        .expect("abandoned Fork published one child")
        .clone();
    let child_pid = child.pid.expect("abandoned Fork has one child PID");
    assert_eq!(
        wait_for_marker_pids(&marker, 2)
            .await
            .into_iter()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([parent_pid, child_pid])
    );
    let retried_child = daemon
        .client
        .fork_with_operation_key(parent.id, ForkPlan::LevelA, fork_key)
        .await
        .expect("Fork retry returns the abandoned response Run");
    assert_eq!(retried_child.id, child.id);
    assert_eq!(retried_child.pid, child.pid);
    let final_runs = wait_for_run_count(&daemon.client, 2).await;
    assert_eq!(
        final_runs
            .into_iter()
            .map(|run| run.id.to_string())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([parent.id.to_string(), child.id.to_string()])
    );
    assert_eq!(
        wait_for_marker_pids(&marker, 2)
            .await
            .into_iter()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([parent_pid, child_pid])
    );
    assert!(process_exists(unrelated.pid()));

    daemon.client.stop(child.id).await.expect("stop child");
    daemon.client.stop(parent.id).await.expect("stop parent");
    assert!(process_exists(unrelated.pid()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recoverable_input_response_loss_reconnects_without_duplicate_write() {
    let daemon = TestDaemon::start().await;
    let run = daemon
        .client
        .start(raw_capture_shell(2))
        .await
        .expect("start recoverable Input capture Run");
    assert_eq!(run.applied_input_bytes, Some(0));
    let daemon_instance = daemon
        .client
        .daemon_instance()
        .await
        .expect("read daemon incarnation");
    let (mut attachment, snapshot) = daemon
        .client
        .attach(run.id, 0)
        .await
        .expect("attach response-loss oracle");
    let mut observed = replay_bytes(&snapshot.replay.chunks);
    let mut last_seq = snapshot.replay.latest_output_bytes;
    if !observed.windows(5).any(|window| window == b"READY") {
        wait_for_output(&mut attachment, &mut observed, &mut last_seq, b"READY").await;
    }

    let first_key = InputOperationKey::new("lost-response-input").unwrap();
    send_request_without_reading_response(
        &daemon.client,
        Request::RecoverableInput {
            operation: RecoverableInput {
                daemon_instance,
                operation_key: first_key.clone(),
                id: run.id,
                expected_byte: 0,
                data: b"A".to_vec(),
            },
        },
    )
    .await;
    let recovered = daemon
        .client
        .recoverable_input(RecoverableInput {
            daemon_instance,
            operation_key: first_key,
            id: run.id,
            expected_byte: 0,
            data: b"A".to_vec(),
        })
        .await
        .expect("fresh client recovers abandoned Input result");
    assert_eq!(recovered.receipt.start_byte, 0);
    assert_eq!(recovered.receipt.end_byte, 1);
    assert_eq!(recovered.run.applied_input_bytes, Some(1));

    let second = daemon
        .client
        .recoverable_input(RecoverableInput {
            daemon_instance,
            operation_key: InputOperationKey::new("following-input").unwrap(),
            id: run.id,
            expected_byte: 1,
            data: b"B".to_vec(),
        })
        .await
        .expect("write following operation");
    assert_eq!(second.receipt.start_byte, 1);
    assert_eq!(second.receipt.end_byte, 2);

    let retried_after_progress = daemon
        .client
        .recoverable_input(RecoverableInput {
            daemon_instance,
            operation_key: InputOperationKey::new("lost-response-input").unwrap(),
            id: run.id,
            expected_byte: 0,
            data: b"A".to_vec(),
        })
        .await
        .expect("retained old result remains valid after later Input");
    assert_eq!(retried_after_progress.receipt.start_byte, 0);
    assert_eq!(retried_after_progress.receipt.end_byte, 1);
    assert_eq!(retried_after_progress.run.applied_input_bytes, Some(2));

    wait_for_output(&mut attachment, &mut observed, &mut last_seq, b"CAPTURED").await;
    let ready = observed
        .windows(b"READY\n".len())
        .position(|window| window == b"READY\n")
        .expect("raw child published readiness")
        + b"READY\n".len();
    let captured = observed[ready..]
        .windows(b"CAPTURED".len())
        .position(|window| window == b"CAPTURED")
        .expect("raw child published capture marker")
        + ready;
    let bytes = std::str::from_utf8(&observed[ready..captured])
        .expect("od byte oracle is ASCII")
        .split_ascii_whitespace()
        .map(|byte| u8::from_str_radix(byte, 16).expect("od emits hexadecimal bytes"))
        .collect::<Vec<_>>();
    assert_eq!(bytes, b"AB", "abandoned retry wrote the first payload once");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recoverable_input_rejects_another_daemon_instance_before_pty_mutation() {
    let first = TestDaemon::start().await;
    let stale_instance = first
        .client
        .daemon_instance()
        .await
        .expect("read first daemon incarnation");
    let second = TestDaemon::start().await;
    let run = second
        .client
        .start(raw_capture_shell(1))
        .await
        .expect("start live target on replacement daemon");
    let (mut attachment, snapshot) = second
        .client
        .attach(run.id, 0)
        .await
        .expect("attach replacement capture oracle");
    let mut observed = replay_bytes(&snapshot.replay.chunks);
    let mut last_seq = snapshot.replay.latest_output_bytes;
    if !observed.windows(5).any(|window| window == b"READY") {
        wait_for_output(&mut attachment, &mut observed, &mut last_seq, b"READY").await;
    }
    assert_ne!(
        stale_instance,
        second
            .client
            .daemon_instance()
            .await
            .expect("read replacement daemon incarnation")
    );

    assert_protocol_error(
        second
            .client
            .recoverable_input(RecoverableInput {
                daemon_instance: stale_instance,
                operation_key: InputOperationKey::new("stale-instance").unwrap(),
                id: run.id,
                expected_byte: 0,
                data: b"X".to_vec(),
            })
            .await
            .expect_err("old incarnation cannot mutate a live replacement Run"),
        ErrorCode::DaemonInstanceMismatch,
    );
    assert_eq!(
        second
            .client
            .status(run.id)
            .await
            .expect("read unchanged target")
            .applied_input_bytes,
        Some(0)
    );

    let replacement_instance = second.client.daemon_instance().await.unwrap();
    second
        .client
        .recoverable_input(RecoverableInput {
            daemon_instance: replacement_instance,
            operation_key: InputOperationKey::new("current-instance").unwrap(),
            id: run.id,
            expected_byte: 0,
            data: b"Y".to_vec(),
        })
        .await
        .expect("current incarnation writes target once");
    wait_for_output(&mut attachment, &mut observed, &mut last_seq, b"CAPTURED").await;
    assert!(observed.windows(2).any(|bytes| bytes == b"59"));
    assert!(!observed.windows(2).any(|bytes| bytes == b"58"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn level_a_fork_clones_declared_inputs_and_runs_independently() {
    let daemon = TestDaemon::start().await;
    let mut spec = interactive_shell();
    spec.declared_inputs = fork_inputs();
    let parent = daemon
        .client
        .start(spec.clone())
        .await
        .expect("start parent Run");
    assert_eq!(parent.spec, Some(spec));
    assert_eq!(parent.lineage, None);

    let child = daemon
        .client
        .fork(parent.id, ForkPlan::LevelA)
        .await
        .expect("fork portable Run inputs");
    assert_ne!(child.id, parent.id);
    assert_ne!(child.pid, parent.pid);
    assert_eq!(child.spec, parent.spec);
    assert_eq!(
        child.lineage,
        Some(RunLineage {
            parent: parent.id,
            fidelity: ForkFidelity::LevelA,
        })
    );
    let mut rejected_spec = parent.spec.clone().expect("native parent has a spec");
    rejected_spec.declared_inputs.push(RunInputReference {
        kind: RunInputKind::Context,
        reference: String::new(),
    });
    assert_protocol_error(
        daemon
            .client
            .fork(
                parent.id,
                ForkPlan::LevelB {
                    spec: rejected_spec,
                },
            )
            .await
            .expect_err("invalid fork plan is rejected"),
        ErrorCode::InvalidRequest,
    );
    assert_eq!(
        daemon
            .client
            .list()
            .await
            .expect("list retained Runs")
            .len(),
        2,
        "rejected fork published a child",
    );

    let (mut parent_attachment, parent_snapshot) = daemon
        .client
        .attach(parent.id, 0)
        .await
        .expect("attach parent Run");
    let (mut child_attachment, child_snapshot) = daemon
        .client
        .attach(child.id, 0)
        .await
        .expect("attach child Run");
    let mut parent_output = replay_bytes(&parent_snapshot.replay.chunks);
    let mut parent_seq = parent_snapshot.replay.latest_output_bytes;
    let mut child_output = replay_bytes(&child_snapshot.replay.chunks);
    let mut child_seq = child_snapshot.replay.latest_output_bytes;
    parent_attachment
        .input(b"parent\n".to_vec())
        .await
        .expect("write parent input");
    child_attachment
        .input(b"child\n".to_vec())
        .await
        .expect("write child input");
    wait_for_output(
        &mut parent_attachment,
        &mut parent_output,
        &mut parent_seq,
        b"OUT:parent",
    )
    .await;
    wait_for_output(
        &mut child_attachment,
        &mut child_output,
        &mut child_seq,
        b"OUT:child",
    )
    .await;
    assert!(!parent_output.windows(9).any(|bytes| bytes == b"OUT:child"));
    assert!(!child_output.windows(10).any(|bytes| bytes == b"OUT:parent"));

    daemon
        .client
        .stop(parent.id)
        .await
        .expect("stop parent Run");
    daemon.client.stop(child.id).await.expect("stop child Run");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn same_epoch_exited_run_has_no_fresh_level_b_authority() {
    let daemon = TestDaemon::start().await;
    let parent = daemon
        .client
        .start(RunSpec {
            program: "/bin/sh".to_owned(),
            args: vec!["-c".to_owned(), "exit 0".to_owned()],
            cwd: None,
            env: BTreeMap::default(),
            size: TerminalSize::default(),
            declared_inputs: Vec::new(),
        })
        .await
        .expect("start short-lived Level B parent");
    assert!(matches!(
        wait_until_exited(&daemon.client, parent.id).await,
        RunState::Exited { .. }
    ));

    assert_protocol_error(
        daemon
            .client
            .fork(
                parent.id,
                ForkPlan::LevelB {
                    spec: interactive_shell(),
                },
            )
            .await
            .expect_err("exited parent has no live Level B continuation authority"),
        ErrorCode::InvalidRunState,
    );
    assert_eq!(
        daemon
            .client
            .list()
            .await
            .expect("list retained Runs")
            .len(),
        1,
        "rejected Level B fork must not publish a child",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn daemon_rejects_generation_8_before_request_dispatch() {
    assert_eq!(
        PROTOCOL_VERSION, 9,
        "fixture must name the current generation"
    );
    let daemon = TestDaemon::start().await;
    let mut stream = UnixStream::connect(daemon.client.socket_path())
        .await
        .expect("connect raw protocol client");
    let generation_8_hello = encode_frame(&ClientFrame::Hello {
        hello: ClientHello { protocol: 8 },
    })
    .expect("encode previous-generation hello");
    let start = encode_frame(&ClientFrame::Request {
        request: ctxmux_protocol::Request::Start {
            operation_key: CreateOperationKey::new("old-generation-must-not-run").unwrap(),
            spec: RunSpec {
                program: "/bin/sh".to_owned(),
                args: vec!["-c".to_owned(), "printf must-not-run".to_owned()],
                cwd: None,
                env: BTreeMap::new(),
                size: TerminalSize::default(),
                declared_inputs: Vec::new(),
            },
        },
    })
    .expect("encode queued start request");
    stream
        .write_all(format!("{generation_8_hello}\n{start}\n").as_bytes())
        .await
        .expect("send coalesced old hello and start request");
    let mut wire = Framed::new(stream, LinesCodec::new_with_max_length(MAX_FRAME_BYTES));
    let line = timeout(Duration::from_secs(5), wire.next())
        .await
        .expect("version mismatch response must be bounded")
        .expect("daemon responds")
        .expect("read daemon response");
    match decode_frame::<ServerFrame>(&line).expect("decode daemon response") {
        ServerFrame::Error { error } => assert_eq!(error.code, ErrorCode::VersionMismatch),
        other => panic!("expected version mismatch, got {other:?}"),
    }
    assert!(
        timeout(Duration::from_secs(5), wire.next())
            .await
            .expect("old-generation connection close must be bounded")
            .is_none(),
        "daemon must close the old-generation connection"
    );
    assert!(
        daemon
            .client
            .list()
            .await
            .expect("list current Runs")
            .is_empty(),
        "a request queued behind an old hello reached dispatch"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn protocol_frame_ceiling_and_duplicate_names_fail_before_run_mutation() {
    // LP-02 / LP-03: byte-exact framing and duplicate-name rejection are
    // exercised against the real daemon boundary, not only serde helpers.
    let daemon = TestDaemon::start().await;
    let empty_padding =
        format!(r#"{{"type":"hello","hello":{{"protocol":{PROTOCOL_VERSION}}},"padding":""}}"#);
    let padding = "x".repeat(MAX_FRAME_BYTES - empty_padding.len());
    let exact_frame = format!(
        r#"{{"type":"hello","hello":{{"protocol":{PROTOCOL_VERSION}}},"padding":"{padding}"}}"#
    );
    assert_eq!(exact_frame.len(), MAX_FRAME_BYTES);

    let stream = UnixStream::connect(daemon.client.socket_path())
        .await
        .expect("connect exact-limit fixture");
    let mut wire = Framed::new(stream, LinesCodec::new_with_max_length(MAX_FRAME_BYTES));
    wire.send(exact_frame)
        .await
        .expect("send exact-limit handshake");
    let line = timeout(Duration::from_secs(5), wire.next())
        .await
        .expect("exact-limit handshake should settle")
        .expect("daemon sends handshake response")
        .expect("read exact-limit handshake response");
    assert!(matches!(
        decode_frame::<ServerFrame>(&line).expect("decode exact-limit response"),
        ServerFrame::Hello {
            protocol: PROTOCOL_VERSION,
            ..
        }
    ));
    drop(wire);

    assert_raw_connection_closes(&daemon.client, &vec![b'x'; MAX_FRAME_BYTES + 1], true).await;
    assert_raw_connection_closes(&daemon.client, &vec![b'x'; MAX_FRAME_BYTES + 1], false).await;
    for (id, frame) in malformed_protocol_frames() {
        assert_malformed_request_closes(&daemon.client, &frame).await;
        assert!(
            daemon
                .client
                .list()
                .await
                .expect("list after malformed frame")
                .is_empty(),
            "shared malformed frame {id} dispatched a start request"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retained_replay_larger_than_one_frame_streams_exactly_to_the_client() {
    // A 4 MiB raw replay expands far beyond the 1 MiB JSON frame cap when
    // bytes are integer arrays. The public client must receive ordered replay
    // chunks across frames instead of losing the retained contract at attach.
    let daemon = TestDaemon::start().await;
    let run = daemon
        .client
        .start(RunSpec {
            program: "/bin/sh".to_owned(),
            args: vec![
                "-c".to_owned(),
                "head -c 5242880 /dev/zero; printf FRAME-SPLIT-FINAL".to_owned(),
            ],
            cwd: None,
            env: BTreeMap::default(),
            size: TerminalSize::default(),
            declared_inputs: Vec::new(),
        })
        .await
        .expect("start frame-split replay Run");
    wait_until_exited(&daemon.client, run.id).await;

    let (_, snapshot) = daemon
        .client
        .attach(run.id, 0)
        .await
        .expect("stream retained replay across protocol frames");
    let replay = replay_bytes(&snapshot.replay.chunks);
    assert!(snapshot.replay.truncated);
    assert!(replay.len() >= 4 * 1024 * 1024 - 8192);
    assert!(replay.len() <= 4 * 1024 * 1024);
    let marker = b"FRAME-SPLIT-FINAL";
    let zero_prefix = replay
        .len()
        .checked_sub(marker.len())
        .expect("retained replay contains the final marker");
    assert!(replay[..zero_prefix].iter().all(|byte| *byte == 0));
    assert_eq!(&replay[zero_prefix..], marker);
    assert_eq!(
        snapshot.replay.chunks.first().map(|chunk| chunk.start_byte),
        Some(snapshot.replay.first_available_byte)
    );
    assert_eq!(
        snapshot.replay.chunks.last().map(|chunk| chunk.end_byte),
        Some(snapshot.replay.latest_output_bytes)
    );
    assert!(
        snapshot
            .replay
            .chunks
            .windows(2)
            .all(|pair| pair[1].start_byte == pair[0].end_byte)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn already_exited_run_replays_exact_binary_bytes_before_one_exit_event() {
    // LC-002 / OR-001: final state must not make retained raw bytes
    // unreachable, including NUL, invalid UTF-8, and split control bytes.
    let daemon = TestDaemon::start().await;
    let run = daemon
        .client
        .start(RunSpec {
            program: "/bin/sh".to_owned(),
            args: vec![
                "-c".to_owned(),
                concat!(
                    "printf '\\000\\377\\033[31m\\342'; ",
                    "sleep 0.05; ",
                    "printf '\\202\\254\\033[0mFINAL'"
                )
                .to_owned(),
            ],
            cwd: None,
            env: BTreeMap::default(),
            size: TerminalSize::default(),
            declared_inputs: Vec::new(),
        })
        .await
        .expect("start binary-output Run");
    let exited = wait_until_exited(&daemon.client, run.id).await;

    let (attachment, snapshot) = daemon
        .client
        .attach(run.id, 0)
        .await
        .expect("attach to already-exited Run");
    assert_eq!(snapshot.run.state, exited);
    assert!(!snapshot.replay.truncated);
    assert_eq!(
        replay_bytes(&snapshot.replay.chunks),
        vec![
            0x00, 0xff, 0x1b, b'[', b'3', b'1', b'm', 0xe2, 0x82, 0xac, 0x1b, b'[', b'0', b'm',
            b'F', b'I', b'N', b'A', b'L',
        ]
    );
    assert!(
        snapshot
            .replay
            .chunks
            .windows(2)
            .any(|pair| pair[0].end_byte == 8 && pair[1].start_byte == 8),
        "the fixture must split the three-byte UTF-8 scalar after its first byte"
    );
    let interior = snapshot
        .replay
        .chunks
        .iter()
        .find(|chunk| chunk.data.len() > 1)
        .expect("fixture retains a multi-byte output chunk");
    let interior_cursor = interior.start_byte + 1;
    let (_, suffix) = daemon
        .client
        .attach(run.id, interior_cursor)
        .await
        .expect("reattach from inside one retained byte range");
    let expected = replay_bytes(&snapshot.replay.chunks);
    assert_eq!(
        replay_bytes(&suffix.replay.chunks),
        expected[usize::try_from(interior_cursor).expect("fixture cursor fits usize")..]
    );
    assert_eq!(
        suffix.replay.chunks.first().map(|chunk| chunk.start_byte),
        Some(interior_cursor)
    );
    assert_eq!(
        attachment.next_event().await.expect("read terminal event"),
        Some(RunEvent::Exited { state: exited })
    );
    assert_eq!(
        attachment.next_event().await.expect("read attachment EOF"),
        None
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_pty_child_does_not_inherit_an_ambient_daemon_descriptor() {
    // PTY-002: protect portable-pty's descriptor-hygiene boundary with one
    // controlled non-CLOEXEC descriptor rather than asserting a global count.
    let sentinel_directory = tempfile::tempdir().expect("create descriptor fixture directory");
    let sentinel = sentinel_directory.path().join("sentinel");
    std::fs::write(&sentinel, b"ambient authority").expect("write descriptor sentinel");
    let daemon = TestDaemon::start_with_inherited_fd(&sentinel).await;
    let run = daemon
        .client
        .start(RunSpec {
            program: "/bin/sh".to_owned(),
            args: vec![
                "-c".to_owned(),
                "( : <&9 ) 2>/dev/null && printf LEAKED || printf CLOSED".to_owned(),
            ],
            cwd: None,
            env: BTreeMap::default(),
            size: TerminalSize::default(),
            declared_inputs: Vec::new(),
        })
        .await
        .expect("start descriptor-probe Run");
    wait_until_exited(&daemon.client, run.id).await;
    let (_, snapshot) = daemon
        .client
        .attach(run.id, 0)
        .await
        .expect("replay descriptor probe");
    assert_eq!(replay_bytes(&snapshot.replay.chunks), b"CLOSED");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_pty_child_does_not_inherit_the_private_qualification_descriptor() {
    let sentinel_directory = tempfile::tempdir().expect("create descriptor fixture directory");
    let sentinel = sentinel_directory.path().join("qualification-stats.ndjson");
    let daemon = TestDaemon::start_with_qualification_stats_fd(&sentinel).await;
    let run = daemon
        .client
        .start(RunSpec {
            program: "/bin/sh".to_owned(),
            args: vec![
                "-c".to_owned(),
                "( : >&3 ) 2>/dev/null && printf LEAKED || printf CLOSED".to_owned(),
            ],
            cwd: None,
            env: BTreeMap::default(),
            size: TerminalSize::default(),
            declared_inputs: Vec::new(),
        })
        .await
        .expect("start qualification-descriptor probe Run");
    wait_until_exited(&daemon.client, run.id).await;
    let (_, snapshot) = daemon
        .client
        .attach(run.id, 0)
        .await
        .expect("replay qualification-descriptor probe");
    assert_eq!(replay_bytes(&snapshot.replay.chunks), b"CLOSED");
    assert!(
        std::fs::metadata(sentinel)
            .expect("qualification stats file")
            .len()
            > 0,
        "daemon should have consumed the inherited stats descriptor",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inherited_readiness_receipt_matches_the_public_daemon_instance() {
    let receipt_directory = tempfile::tempdir().expect("create readiness receipt directory");
    let receipt = receipt_directory.path().join("ready.ndjson");
    let daemon = TestDaemon::start_with_readiness_fd(&receipt).await;
    let content = std::fs::read_to_string(&receipt).expect("read readiness receipt");
    assert_eq!(content.lines().count(), 1);
    let record: Value = serde_json::from_str(&content).expect("parse readiness receipt");
    assert_eq!(record["schema"], "ctxmux.daemon-ready.v1");
    assert_eq!(
        record["daemon_instance"],
        daemon
            .client
            .daemon_instance()
            .await
            .expect("read public daemon instance")
            .to_string()
    );

    let run = daemon
        .client
        .start(RunSpec {
            program: "/bin/sh".to_owned(),
            args: vec![
                "-c".to_owned(),
                "( : >&3 ) 2>/dev/null && printf LEAKED || printf CLOSED".to_owned(),
            ],
            cwd: None,
            env: BTreeMap::default(),
            size: TerminalSize::default(),
            declared_inputs: Vec::new(),
        })
        .await
        .expect("start readiness-descriptor probe Run");
    wait_until_exited(&daemon.client, run.id).await;
    let (_, snapshot) = daemon
        .client
        .attach(run.id, 0)
        .await
        .expect("replay readiness-descriptor probe");
    assert_eq!(replay_bytes(&snapshot.replay.chunks), b"CLOSED");
}

#[test]
fn closed_qualification_descriptor_fails_before_the_async_runtime_starts() {
    let directory = tempfile::tempdir().expect("create invalid descriptor fixture directory");
    let output = std::process::Command::new("/bin/sh")
        .arg("-c")
        .arg("exec 3>&-; exec \"$1\" --socket \"$2\" --qualification-stats-fd 3")
        .arg("ctxmux-closed-qualification-fd-fixture")
        .arg(env!("CARGO_BIN_EXE_ctxmuxd"))
        .arg(directory.path().join("ctxmux.sock"))
        .output()
        .expect("run ctxmuxd with a closed qualification descriptor");
    assert_eq!(output.status.code(), Some(2));
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("invalid qualification stats fd"),
        "closed descriptor should be rejected directly: {}",
        String::from_utf8_lossy(&output.stderr),
    );
    assert!(
        !String::from_utf8_lossy(&output.stderr).contains("panicked")
            && !String::from_utf8_lossy(&output.stderr).contains("tokio-runtime-worker"),
        "Tokio must not start with or later lose the rejected descriptor",
    );
}

#[test]
fn closed_or_conflicting_readiness_descriptor_fails_before_startup() {
    let directory = tempfile::tempdir().expect("create invalid readiness fixture directory");
    let socket = directory.path().join("ctxmux.sock");
    let closed = std::process::Command::new("/bin/sh")
        .arg("-c")
        .arg("exec 3>&-; exec \"$1\" --socket \"$2\" --readiness-fd 3")
        .arg("ctxmux-closed-readiness-fd-fixture")
        .arg(env!("CARGO_BIN_EXE_ctxmuxd"))
        .arg(&socket)
        .output()
        .expect("run ctxmuxd with a closed readiness descriptor");
    assert_eq!(closed.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&closed.stderr).contains("invalid readiness fd"));

    let receipt = directory.path().join("conflicting.ndjson");
    let conflicting = std::process::Command::new("/bin/sh")
        .arg("-c")
        .arg("exec 3>\"$1\"; shift; exec \"$@\" --qualification-stats-fd 3 --readiness-fd 3")
        .arg("ctxmux-conflicting-readiness-fd-fixture")
        .arg(&receipt)
        .arg(env!("CARGO_BIN_EXE_ctxmuxd"))
        .arg("--socket")
        .arg(&socket)
        .output()
        .expect("run ctxmuxd with conflicting inherited descriptors");
    assert_eq!(conflicting.status.code(), Some(2));
    assert!(
        String::from_utf8_lossy(&conflicting.stderr)
            .contains("inherited descriptors must be distinct")
    );
}

#[test]
fn readiness_write_failure_removes_the_unpublished_socket() {
    let directory = tempfile::tempdir().expect("create readiness write fixture directory");
    let socket = directory.path().join("ctxmux.sock");
    let sentinel = directory.path().join("read-only");
    std::fs::write(&sentinel, b"read-only").expect("write readiness sentinel");
    let output = std::process::Command::new("/bin/sh")
        .arg("-c")
        .arg("exec 3<\"$1\"; shift; exec \"$@\" --readiness-fd 3")
        .arg("ctxmux-readiness-write-failure-fixture")
        .arg(&sentinel)
        .arg(env!("CARGO_BIN_EXE_ctxmuxd"))
        .arg("--socket")
        .arg(&socket)
        .output()
        .expect("run ctxmuxd with a read-only readiness descriptor");
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("<readiness-fd>"));
    assert!(
        !socket.exists(),
        "failed readiness publication must remove the socket"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stop_escalates_past_ignored_hup_and_rejects_repeated_stop() {
    let daemon = TestDaemon::start().await;
    let run = daemon
        .client
        .start(RunSpec {
            program: "/bin/sh".to_owned(),
            args: vec![
                "-c".to_owned(),
                "trap '' HUP; printf 'READY\\n'; while :; do sleep 1; done".to_owned(),
            ],
            cwd: None,
            env: BTreeMap::default(),
            size: TerminalSize::default(),
            declared_inputs: Vec::new(),
        })
        .await
        .expect("start HUP-ignoring Run");
    let pid = run.pid.expect("direct child exposes a PID");

    let (mut attachment, snapshot) = daemon
        .client
        .attach(run.id, 0)
        .await
        .expect("attach before clean detach");
    let mut observed = replay_bytes(&snapshot.replay.chunks);
    let mut last_seq = snapshot.replay.latest_output_bytes;
    if !observed.windows(5).any(|window| window == b"READY") {
        wait_for_output(&mut attachment, &mut observed, &mut last_seq, b"READY").await;
    }
    attachment.detach().await.expect("detach cleanly");
    assert_eq!(
        daemon
            .client
            .status(run.id)
            .await
            .expect("status after detach")
            .attachments,
        0
    );
    daemon
        .client
        .stop(run.id)
        .await
        .expect("stop HUP-ignoring Run");
    assert!(!wait_until_exited(&daemon.client, run.id).await.is_running());
    assert!(
        !process_exists(pid),
        "stopped direct child {pid} remained live"
    );
    assert_protocol_error(
        daemon
            .client
            .stop(run.id)
            .await
            .expect_err("repeated stop is rejected"),
        ErrorCode::InvalidRunState,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interrupt_reaches_the_foreground_group_without_stopping_the_run() {
    let daemon = TestDaemon::start().await;
    let run = daemon
        .client
        .start(RunSpec {
            program: "/bin/sh".to_owned(),
            args: vec![
                "-c".to_owned(),
                concat!(
                    "trap 'printf \"INTERRUPTED\\n\"' INT; ",
                    "printf 'READY\\n'; ",
                    "while :; do sleep 1; wait $!; done"
                )
                .to_owned(),
            ],
            cwd: None,
            env: BTreeMap::default(),
            size: TerminalSize::default(),
            declared_inputs: Vec::new(),
        })
        .await
        .expect("start interrupt fixture");
    assert!(run.capabilities.signal);
    let (mut attachment, snapshot) = daemon.client.attach(run.id, 0).await.expect("attach");
    let mut observed = replay_bytes(&snapshot.replay.chunks);
    let mut last_byte = snapshot.replay.latest_output_bytes;
    wait_for_output(&mut attachment, &mut observed, &mut last_byte, b"READY").await;

    let accepted = attachment
        .interrupt()
        .await
        .expect("interrupt foreground group");
    assert_eq!(accepted.receipt.signal, RunSignal::Interrupt);
    wait_for_output(
        &mut attachment,
        &mut observed,
        &mut last_byte,
        b"INTERRUPTED",
    )
    .await;
    assert!(
        daemon
            .client
            .status(run.id)
            .await
            .expect("status")
            .state
            .is_running()
    );
    daemon.client.stop(run.id).await.expect("stop fixture");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cleanup_saturation_rejects_the_ninth_stop_before_mutation() {
    const CLEANUP_OWNERS: usize = 8;

    let daemon = TestDaemon::start().await;
    let stubborn = RunSpec {
        program: "/bin/sh".to_owned(),
        args: vec![
            "-c".to_owned(),
            "trap '' HUP TERM; printf 'READY\n'; while :; do IFS= read -r line || :; done"
                .to_owned(),
        ],
        cwd: None,
        env: BTreeMap::new(),
        size: TerminalSize::default(),
        declared_inputs: Vec::new(),
    };
    let mut runs = Vec::new();
    for _ in 0..=CLEANUP_OWNERS {
        let run = daemon
            .client
            .start(stubborn.clone())
            .await
            .expect("start stubborn cleanup fixture");
        let (mut attachment, snapshot) = daemon
            .client
            .attach(run.id, 0)
            .await
            .expect("attach stubborn cleanup fixture");
        let mut output = replay_bytes(&snapshot.replay.chunks);
        let mut cursor = snapshot.replay.latest_output_bytes;
        if !output.windows(5).any(|window| window == b"READY") {
            wait_for_output(&mut attachment, &mut output, &mut cursor, b"READY").await;
        }
        drop(attachment);
        runs.push(run);
    }

    let mut accepted = Vec::new();
    for run in runs.iter().take(CLEANUP_OWNERS) {
        let client = daemon.client.clone();
        let id = run.id;
        accepted.push(tokio::spawn(async move {
            timeout(Duration::from_secs(3), client.stop(id))
                .await
                .expect("accepted stubborn Stop stays inside its receipt fence")
        }));
    }
    sleep(Duration::from_millis(100)).await;

    let ninth = runs[CLEANUP_OWNERS].id;
    let error = daemon
        .client
        .stop(ninth)
        .await
        .expect_err("ninth Stop cannot enter Stopping without cleanup capacity");
    assert!(matches!(
        error,
        ClientError::ControlRejected { failure }
            if failure.error.code == ErrorCode::ControlBackpressure
                && failure.disposition == CommandDisposition::NotApplied
    ));
    daemon
        .client
        .input(ninth, b"still-open\n".to_vec())
        .await
        .expect("capacity rejection leaves the ninth Run open");

    for result in join_all(accepted).await {
        result
            .expect("join accepted stubborn Stop")
            .expect("accepted stubborn Stop has an exact receipt");
    }
    daemon
        .client
        .stop(ninth)
        .await
        .expect("ninth Stop succeeds after cleanup capacity returns");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stop_forces_stubborn_descendants_and_preserves_unrelated_processes() {
    let daemon = TestDaemon::start().await;
    let marker = daemon.directory.path().join("descendants");
    let mut unrelated = Command::new("/bin/sleep")
        .arg("30")
        .spawn()
        .expect("spawn unrelated sentinel");
    let unrelated_pid = unrelated.id();
    let run = daemon
        .client
        .start(RunSpec {
            program: "/bin/sh".to_owned(),
            args: vec![
                "-c".to_owned(),
                concat!(
                    "trap '' TERM; ",
                    "sh -c 'trap \"\" TERM; while :; do sleep 1; done' & ",
                    "child=$!; ",
                    "sh -c 'trap \"\" TERM; while :; do sleep 1; done' & ",
                    "grandchild=$!; ",
                    "printf '%s\\n%s\\n' \"$child\" \"$grandchild\" > \"$CTXMUX_DESCENDANTS\"; ",
                    "printf 'READY\\n'; while :; do sleep 1; done"
                )
                .to_owned(),
            ],
            cwd: None,
            env: BTreeMap::from([(
                "CTXMUX_DESCENDANTS".to_owned(),
                marker.to_string_lossy().into_owned(),
            )]),
            size: TerminalSize::default(),
            declared_inputs: Vec::new(),
        })
        .await
        .expect("start hostile descendant fixture");
    let descendants = wait_for_marker_pids(&marker, 2).await;
    let accepted = daemon
        .client
        .stop(run.id)
        .await
        .expect("stop complete session");
    assert_eq!(accepted.receipt.disposition, StopDisposition::Forced);
    assert!(!wait_until_exited(&daemon.client, run.id).await.is_running());
    for pid in descendants {
        assert!(
            !process_exists(pid),
            "Run-owned descendant {pid} survived Stop"
        );
    }
    assert!(
        process_exists(unrelated_pid),
        "unrelated process was signalled"
    );
    let _ = unrelated.kill();
    let _ = unrelated.wait();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_interrupt_and_stop_have_only_owner_declared_outcomes() {
    let daemon = TestDaemon::start().await;
    let run = daemon
        .client
        .start(RunSpec {
            program: "/bin/sh".to_owned(),
            args: vec!["-c".to_owned(), "while :; do sleep 1; done".to_owned()],
            cwd: None,
            env: BTreeMap::default(),
            size: TerminalSize::default(),
            declared_inputs: Vec::new(),
        })
        .await
        .expect("start concurrency fixture");
    let interrupt_client = daemon.client.clone();
    let stop_client = daemon.client.clone();
    let (interrupt, stop) =
        tokio::join!(interrupt_client.interrupt(run.id), stop_client.stop(run.id),);
    assert!(
        stop.is_ok(),
        "Stop must retain the unique terminal owner: {stop:?}"
    );
    if let Err(error) = interrupt {
        assert!(matches!(
            error,
            ClientError::ControlRejected { ref failure }
                if failure.error.code == ErrorCode::InvalidRunState
        ));
    }
    assert!(!wait_until_exited(&daemon.client, run.id).await.is_running());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_interrupt_stop_and_natural_exit_leave_no_signal_or_process_survivor() {
    let daemon = TestDaemon::start().await;
    let marker = daemon.directory.path().join("terminal-race-child");
    let run = daemon
        .client
        .start(RunSpec {
            program: "/bin/sh".to_owned(),
            args: vec![
                "-c".to_owned(),
                concat!(
                    "trap 'printf X >> \"$CTXMUX_RACE_MARKER\"' INT; ",
                    "sh -c 'trap \"\" TERM; while :; do sleep 1; done' & ",
                    "descendant=$!; ",
                    "printf '%s\\n%s\\n' \"$$\" \"$descendant\" > \"$CTXMUX_RACE_PID\"; ",
                    "(sleep 0.05; kill -TERM $$) & ",
                    "while :; do sleep 1; wait $!; done"
                )
                .to_owned(),
            ],
            cwd: None,
            env: BTreeMap::from([
                (
                    "CTXMUX_RACE_PID".to_owned(),
                    marker.to_string_lossy().into_owned(),
                ),
                (
                    "CTXMUX_RACE_MARKER".to_owned(),
                    marker
                        .with_extension("interrupts")
                        .to_string_lossy()
                        .into_owned(),
                ),
            ]),
            size: TerminalSize::default(),
            declared_inputs: Vec::new(),
        })
        .await
        .expect("start terminal race fixture");
    let pids = wait_for_marker_pids(&marker, 2).await;
    let interrupt_client = daemon.client.clone();
    let stop_client = daemon.client.clone();
    let (interrupt, stop) =
        tokio::join!(interrupt_client.interrupt(run.id), stop_client.stop(run.id),);
    if let Err(error) = interrupt {
        assert!(matches!(
            error,
            ClientError::ControlRejected { ref failure }
                if failure.error.code == ErrorCode::InvalidRunState
        ));
    }
    if let Err(error) = stop {
        assert!(matches!(
            error,
            ClientError::ControlRejected { ref failure }
                if failure.error.code == ErrorCode::InvalidRunState
        ));
    }
    assert!(!wait_until_exited(&daemon.client, run.id).await.is_running());
    for pid in pids {
        assert!(
            !process_exists(pid),
            "terminal race left Run process {pid} live"
        );
    }

    let before = std::fs::read(marker.with_extension("interrupts")).unwrap_or_default();
    assert_protocol_error(
        daemon
            .client
            .interrupt(run.id)
            .await
            .expect_err("terminal Run rejects post-Stop Interrupt"),
        ErrorCode::InvalidRunState,
    );
    sleep(Duration::from_millis(50)).await;
    let after = std::fs::read(marker.with_extension("interrupts")).unwrap_or_default();
    assert_eq!(after, before, "post-Stop Interrupt produced a side effect");
}

#[tokio::test]
async fn sighup_memory_only_noop() {
    // A memory-only daemon (no --state-dir) cannot do exec-in-place continuity,
    // so SIGHUP must be a no-op: the daemon keeps serving and live runs are
    // unaffected (same child pid), rather than taking the default-terminate
    // disposition that would kill the daemon.
    let daemon = TestDaemon::start().await; // memory-only
    let run = daemon
        .client
        .start(interactive_shell())
        .await
        .expect("start native Run");
    let pid = run.pid.expect("shell exposes a process id");
    assert!(process_exists(pid), "child should be running before SIGHUP");

    let delivered = Command::new("kill")
        .arg("-HUP")
        .arg(daemon.child.id().to_string())
        .status()
        .expect("send SIGHUP to ctxmuxd")
        .success();
    assert!(delivered, "SIGHUP should be delivered to ctxmuxd");

    // Give the daemon a moment to handle the signal (a default-terminate
    // disposition would have exited the process by now).
    sleep(Duration::from_millis(200)).await;

    // The daemon survived SIGHUP and still answers requests.
    daemon
        .client
        .ping()
        .await
        .expect("daemon still serves after a no-op SIGHUP");

    // The live run is untouched: same PID, still running.
    let status = daemon
        .client
        .status(run.id)
        .await
        .expect("read Run status after SIGHUP");
    assert_eq!(status.pid, Some(pid), "SIGHUP must not replace the child");
    assert_eq!(
        status.state,
        RunState::Running,
        "SIGHUP must not interrupt the live run"
    );
    assert!(process_exists(pid), "child should still be running after SIGHUP");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(
    clippy::too_many_lines,
    reason = "one continuous test proves same-pid child survival, master/writer re-adoption, and gapless replay across a real execve upgrade"
)]
async fn upgrade_preserves_live_run() {
    // HEADLINE acceptance test for the exec-in-place upgrade: a persistent
    // daemon that receives SIGHUP drains, re-execs its own binary at the SAME
    // pid, and re-adopts the live native run. This drives a real
    // SIGHUP -> execve of CARGO_BIN_EXE_ctxmuxd (no shim, no fake) and proves
    // the child, its pty master + input writer, and the durable output cursor
    // all cross the exec with no replay gap.

    // 1. Start a persistent daemon and a live interactive Run. Record P0.
    let mut daemon = TestDaemon::start_persistent().await;
    let run = daemon
        .client
        .start(interactive_shell())
        .await
        .expect("start native Run");
    let p0 = run.pid.expect("shell exposes a process id");
    assert!(process_exists(p0), "child should be running before the upgrade");

    // 2. Attach, reach READY, and drive I/O to a known cursor C0.
    let (mut first_attachment, first_snapshot) = daemon
        .client
        .attach(run.id, 0)
        .await
        .expect("attach to native Run");
    let mut observed = replay_bytes(&first_snapshot.replay.chunks);
    let mut last_seq = first_snapshot.replay.latest_output_bytes;
    if !observed.windows(5).any(|window| window == b"READY") {
        wait_for_output(&mut first_attachment, &mut observed, &mut last_seq, b"READY").await;
    }
    first_attachment
        .input(b"before\n".to_vec())
        .await
        .expect("write through attachment before the upgrade");
    wait_for_output(
        &mut first_attachment,
        &mut observed,
        &mut last_seq,
        b"OUT:before",
    )
    .await;
    let c0 = last_seq;
    // The attachment is per-connection: the old image's accept loop that serves
    // it is replaced by execve, so this connection cannot survive the upgrade.
    // The RUN survives; drop the attachment now and re-attach afterwards.
    drop(first_attachment);

    // 3. Trigger the real re-exec and await the incoming image's resume signal.
    daemon.sighup();
    let resume = daemon.wait_resume_signal(10).await;
    assert!(
        resume.contains("1 run(s)"),
        "incoming image should adopt exactly one live run, got: {resume}"
    );
    // A real execve replaces the image but keeps the SAME os process, so the
    // child never exits across the upgrade: try_wait stays None. This is a
    // strong same-pid proof at the daemon level.
    assert!(
        daemon
            .child
            .try_wait()
            .expect("poll daemon across the upgrade")
            .is_none(),
        "execve must reuse the same os process (no daemon exit)"
    );

    // 4. Same-child survival: the run reports the same pid and the child is
    //    still alive. The client reconnects to the unchanged socket inode.
    let status = timeout(Duration::from_secs(10), async {
        loop {
            match daemon.client.status(run.id).await {
                Ok(status) => return status,
                // The new accept loop is serving as of the resume log line, but
                // a status/attach may briefly race the top of serve_with_manager.
                Err(_) => sleep(Duration::from_millis(20)).await,
            }
        }
    })
    .await
    .expect("upgraded daemon should answer status for the surviving run");
    assert_eq!(
        status.pid,
        Some(p0),
        "the surviving run must keep its original child pid"
    );
    assert!(
        process_exists(p0),
        "the original child must still be alive after the upgrade"
    );
    assert_eq!(status.state, RunState::Running, "the run must stay running");

    // 5. Master + writer re-adopted: attach fresh at C0 and observe an echo.
    //    Observing OUT:resumed proves both the pty master (read path) and the
    //    input writer were re-bound by Run::readopt on the incoming image.
    let (second_attachment, second_snapshot) = timeout(Duration::from_secs(10), async {
        loop {
            match daemon.client.attach(run.id, c0).await {
                Ok(pair) => return pair,
                Err(_) => sleep(Duration::from_millis(20)).await,
            }
        }
    })
    .await
    .expect("re-attach to the surviving run after the upgrade");

    // 6. No replay gap (f04, the core continuity claim): the fresh attach at
    //    after_byte == C0 must yield contiguous output. The durable cursor
    //    continued, so replay is not truncated below C0, and the first chunk
    //    after C0 is contiguous with it. wait_for_output PANICS on a
    //    RunEvent::Gap, so driving output through it below also proves no gap.
    assert!(
        second_snapshot.replay.latest_output_bytes >= c0,
        "durable output cursor must not regress across the upgrade"
    );
    if let Some(first_chunk) = second_snapshot.replay.chunks.first() {
        assert_eq!(
            first_chunk.start_byte, c0,
            "replay after the upgrade must be contiguous from the requested cursor"
        );
    }

    let mut observed = replay_bytes(&second_snapshot.replay.chunks);
    let mut last_seq = second_snapshot.replay.latest_output_bytes;
    let mut second_attachment = second_attachment;
    second_attachment
        .input(b"resumed\n".to_vec())
        .await
        .expect("write through the re-adopted input writer");
    wait_for_output(
        &mut second_attachment,
        &mut observed,
        &mut last_seq,
        b"OUT:resumed",
    )
    .await;

    // Let the child exit cleanly through the re-adopted session.
    second_attachment
        .input(b"quit\n".to_vec())
        .await
        .expect("write quit through the re-adopted input writer");
    wait_for_output(
        &mut second_attachment,
        &mut observed,
        &mut last_seq,
        b"OUT:quit",
    )
    .await;
    assert_eq!(
        wait_until_exited(&daemon.client, run.id).await,
        RunState::Exited {
            code: 7,
            signal: None,
        },
        "the re-adopted run should exit through its own quit path"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(
    clippy::too_many_lines,
    reason = "one continuous test drives a burst through the exec-upgrade reader window and proves content-complete, gapless continuity from the settled snapshot"
)]
async fn upgrade_preserves_output_across_the_reader_window() {
    // REGRESSION GUARD for the f04 replay-gap defect: at extract the owner MUST
    // stop reading each pty master (native_runtime.rs sets `entry.output = None`
    // so the retain predicate drops the just-extracted entry). If the reader is
    // NOT stopped, it keeps draining the master in the window between the
    // durable barrier returning and execve; any byte it consumes there is pulled
    // out of the pty kernel buffer (unre-readable by the incoming image) and its
    // Append races — and loses to — execve killing the persistence actor. The
    // durable head then lags the bytes actually consumed, so after readopt the
    // adopted master is misaligned: k bytes are silently lost mid-stream.
    //
    // This test drives a distinctive multi-line burst through that exact window
    // (a single small input triggers the child to spew BURST_LINES ordered
    // lines, so output keeps flowing while the owner is torn down at SIGHUP),
    // lets it fully settle on the incoming image, then re-attaches at C0 and
    // proves from the SETTLED snapshot that EVERY burst line survived. Offsets
    // alone do not catch the defect (the incoming image re-numbers from the
    // persisted cursor, so start_byte stays contiguous even when a run of bytes
    // is dropped); the per-line CONTENT assertions in step 8 are what fail if
    // any byte is lost in the window. Confirmed to FAIL against the pre-fix code
    // (a run of ~300-860 burst lines vanishes mid-stream) and PASS with the
    // `entry.output = None` fix.

    // A single small input triggers the child to emit this many ordered lines
    // (`OUT:burst-000000` ..), so output keeps flowing across the whole
    // extract -> barrier -> exec window while the owner is being torn down.
    const BURST_LINES: usize = 4000;

    // 1. Start a persistent daemon and a live interactive Run.
    let mut daemon = TestDaemon::start_persistent().await;
    let run = daemon
        .client
        .start(interactive_shell())
        .await
        .expect("start native Run");
    let p0 = run.pid.expect("shell exposes a process id");
    assert!(process_exists(p0), "child should be running before the upgrade");

    // 2. Attach, reach READY, drive `before` to a known cursor C0.
    let (mut first_attachment, first_snapshot) = daemon
        .client
        .attach(run.id, 0)
        .await
        .expect("attach to native Run");
    let mut observed = replay_bytes(&first_snapshot.replay.chunks);
    let mut last_seq = first_snapshot.replay.latest_output_bytes;
    if !observed.windows(5).any(|window| window == b"READY") {
        wait_for_output(&mut first_attachment, &mut observed, &mut last_seq, b"READY").await;
    }
    first_attachment
        .input(b"before\n".to_vec())
        .await
        .expect("write through attachment before the upgrade");
    wait_for_output(
        &mut first_attachment,
        &mut observed,
        &mut last_seq,
        b"OUT:before",
    )
    .await;
    let c0 = last_seq;

    // 3. Trigger a long child-driven burst and SIGHUP immediately. A single
    //    small input line makes the child emit BURST_LINES ordered lines
    //    (`OUT:burst-000000` ..) with NO further input needed, so output keeps
    //    flowing across the whole extract -> barrier -> exec window while the
    //    owner is being torn down — the exact window the defect corrupts. We do
    //    NOT wait for the echo: awaiting the input receipt only proves the
    //    `burst=N` line reached the pty; the child then spews on its own. We
    //    SIGHUP right after so the reader is stopped mid-burst.
    first_attachment
        .input(format!("burst={BURST_LINES}\n").into_bytes())
        .await
        .expect("flush the burst trigger to the pty writer before the upgrade");
    // The per-connection attachment cannot survive execve; drop it and re-attach
    // afterwards. Dropping it does NOT stop the owner's reader (output is
    // persisted independently of attachments) — only extract must.
    drop(first_attachment);

    // 4. Trigger the real re-exec and await the incoming image's resume signal.
    daemon.sighup();
    let resume = daemon.wait_resume_signal(10).await;
    assert!(
        resume.contains("1 run(s)"),
        "incoming image should adopt exactly one live run, got: {resume}"
    );
    assert!(
        daemon
            .child
            .try_wait()
            .expect("poll daemon across the upgrade")
            .is_none(),
        "execve must reuse the same os process (no daemon exit)"
    );

    // 5. Wait for the burst to FULLY settle on the incoming image. Output is
    //    logged + persisted by the owner independently of any attachment, so we
    //    poll status until `latest_output_bytes` stops growing. Settling first
    //    is deliberate: it lets us read the whole burst from the attach SNAPSHOT
    //    (replay, contiguous by construction) instead of the live broadcast
    //    channel, whose bounded capacity would raise a spurious lag `Gap` on a
    //    68KB in-flight burst and mask the real defect either way.
    let settled = timeout(Duration::from_secs(15), async {
        let mut stable_at = None;
        let mut stable_polls = 0;
        loop {
            if let Ok(info) = daemon.client.status(run.id).await {
                if Some(info.latest_output_bytes) == stable_at {
                    stable_polls += 1;
                    // ~600ms of no growth: the child's burst loop has ended and
                    // every byte the incoming image will ever read is logged.
                    if stable_polls >= 6 {
                        return info.latest_output_bytes;
                    }
                } else {
                    stable_at = Some(info.latest_output_bytes);
                    stable_polls = 0;
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("burst output should settle on the incoming image");

    // 6. Attach fresh at C0 on the incoming image and take the settled snapshot.
    let (second_attachment, second_snapshot) = timeout(Duration::from_secs(10), async {
        loop {
            match daemon.client.attach(run.id, c0).await {
                Ok(pair) => return pair,
                Err(_) => sleep(Duration::from_millis(20)).await,
            }
        }
    })
    .await
    .expect("re-attach to the surviving run after the upgrade");

    // 7. Offsets: replay must be contiguous from the requested cursor C0 (first
    //    chunk starts exactly at C0, no truncation below it) and the durable
    //    cursor never regressed. NOTE: contiguous offsets ALONE do not catch the
    //    defect — the incoming image re-numbers from the persisted cursor, so a
    //    lost byte leaves the stream offset-contiguous but content-short. The
    //    content check in step 8 is the real detector.
    assert_eq!(
        second_snapshot.replay.latest_output_bytes, settled,
        "the settled snapshot must expose the full logged output length"
    );
    assert!(
        second_snapshot.replay.latest_output_bytes >= c0,
        "durable output cursor must not regress across the upgrade"
    );
    let first_chunk = second_snapshot
        .replay
        .chunks
        .first()
        .expect("a settled snapshot from C0 must replay at least one chunk");
    assert_eq!(
        first_chunk.start_byte, c0,
        "replay after the upgrade must be contiguous from the requested cursor C0"
    );
    // Chunks within the snapshot must themselves be gap-free.
    let mut cursor = first_chunk.start_byte;
    for chunk in &second_snapshot.replay.chunks {
        assert_eq!(
            chunk.start_byte, cursor,
            "settled replay chunks must be byte-contiguous (no gap) from C0"
        );
        cursor = chunk.end_byte;
    }

    // 8. CONTENT completeness (the real f04 defect detector): every burst line
    //    must be present in the settled replay. If the pre-fix reader kept
    //    draining the master in the barrier→exec window, the bytes it consumed
    //    there are lost (their Append raced and lost to execve, and they are
    //    gone from the kernel buffer), so a run of lines vanishes mid-stream
    //    even though the offsets above stayed perfectly contiguous.
    let observed = replay_bytes(&second_snapshot.replay.chunks);
    for index in 0..BURST_LINES {
        let marker = format!("OUT:burst-{index:06}");
        assert!(
            observed
                .windows(marker.len())
                .any(|window| window == marker.as_bytes()),
            "burst line {marker} must survive the exec-upgrade reader window \
             (a missing line means bytes were lost between the barrier and execve)"
        );
    }

    // 9. Drive MORE output after resume to confirm the re-adopted writer + master
    //    keep echoing past the boundary. This is a small live burst, so it does
    //    not overflow the live channel; `wait_for_output` panics on any Gap and
    //    asserts chunk.start_byte == last_byte, proving live continuity too.
    let mut observed = observed;
    let mut last_seq = second_snapshot.replay.latest_output_bytes;
    let mut second_attachment = second_attachment;
    second_attachment
        .input(b"resumed\n".to_vec())
        .await
        .expect("write through the re-adopted input writer");
    wait_for_output(
        &mut second_attachment,
        &mut observed,
        &mut last_seq,
        b"OUT:resumed",
    )
    .await;

    // 10. Clean exit through the re-adopted session.
    second_attachment
        .input(b"quit\n".to_vec())
        .await
        .expect("write quit through the re-adopted input writer");
    wait_for_output(
        &mut second_attachment,
        &mut observed,
        &mut last_seq,
        b"OUT:quit",
    )
    .await;
    assert_eq!(
        wait_until_exited(&daemon.client, run.id).await,
        RunState::Exited {
            code: 7,
            signal: None,
        },
        "the re-adopted run should exit through its own quit path"
    );
}
