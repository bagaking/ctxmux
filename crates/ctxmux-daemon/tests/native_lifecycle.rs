use std::{
    collections::{BTreeMap, BTreeSet},
    io::{self, BufRead, BufReader},
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    time::Duration,
};

use ctxmux_client::{Attachment, Client, ClientError, RuntimeCapabilityRequirements, replay_bytes};
use ctxmux_protocol::{
    AttachmentCommandId, ClientFrame, ClientHello, CommandDisposition, ControlOutcome,
    CreateOperationKey, ErrorCode, ForkFidelity, ForkPlan, InputOperationKey, MAX_FRAME_BYTES,
    PROTOCOL_VERSION, RUNTIME_CAPABILITY_NATIVE_EXECUTE_MATERIALIZED_LEVEL_B,
    RUNTIME_CAPABILITY_NATIVE_FORK_LEVEL_A, RUNTIME_CAPABILITY_NATIVE_RECOVERABLE_INPUT,
    RUNTIME_CAPABILITY_NATIVE_RECOVERABLE_STOP, RUNTIME_CAPABILITY_NATIVE_START,
    RUNTIME_CAPABILITY_PERSISTENT_STATE, RUNTIME_CAPABILITY_PLANNED_EXEC_UPGRADE_CONTINUITY,
    RUNTIME_CAPABILITY_TMUX_DISCOVER, RUNTIME_CAPABILITY_TMUX_IMPORT, RecoverableInput,
    RecoverableStop, Request, RunEvent, RunId, RunInputKind, RunInputReference, RunLineage,
    RunSignal, RunSpec, RunState, RuntimeIdPersistence, RuntimeIdentity, ServerFrame,
    StopDisposition, StopOperationKey, TerminalSize, decode_frame, encode_frame,
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
    directory: Arc<TempDir>,
    client: Client,
    /// Lines captured from a stderr-piped daemon, shared with a drain thread.
    /// `None` for the inherit-stderr constructors, which do not scan stderr.
    stderr_lines: Option<Arc<Mutex<Vec<String>>>>,
}

async fn fresh_stop(client: &Client, id: RunId) -> RecoverableStop {
    client
        .prepare_stop(id)
        .await
        .expect("prepare recoverable Stop operation")
}

async fn stop_run(client: &Client, id: RunId) {
    let operation = fresh_stop(client, id).await;
    client.stop(operation).await.expect("stop Run");
}

impl TestDaemon {
    async fn start() -> Self {
        let directory = Arc::new(tempfile::tempdir().expect("create daemon temp directory"));
        let socket = directory.path().join("ctxmux.sock");
        Self::start_memory_only_at(directory, socket).await
    }

    async fn start_memory_only_at(directory: Arc<TempDir>, socket: PathBuf) -> Self {
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
        let directory = Arc::new(tempfile::tempdir().expect("create daemon temp directory"));
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
        let directory = Arc::new(tempfile::tempdir().expect("create daemon temp directory"));
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
        let directory = Arc::new(tempfile::tempdir().expect("create daemon temp directory"));
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
        directory: Arc<TempDir>,
        socket: impl Into<PathBuf>,
    ) -> Self {
        Self::from_spawned_with_stderr(child, directory, socket, None).await
    }

    /// Start a persistent daemon (`--state-dir`) with stderr piped so the
    /// incoming handoff image's resume log line can be scanned. A drain thread
    /// reads the pipe line-by-line into a shared buffer so the tokio runtime is
    /// never blocked on the synchronous pipe. The thread ends when the daemon
    /// dies and closes the pipe (see `Drop`).
    async fn start_persistent() -> Self {
        let directory = Arc::new(tempfile::tempdir().expect("create daemon temp directory"));
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

        let stderr = child
            .stderr
            .take()
            .expect("persistent daemon exposes stderr");
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
        directory: Arc<TempDir>,
        socket: impl Into<PathBuf>,
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

    fn stop_and_wait(mut self) {
        self.wait_for_interrupt_shutdown()
            .unwrap_or_else(|error| panic!("stop ctxmuxd before replacement: {error}"));
    }

    fn wait_for_interrupt_shutdown(&mut self) -> Result<(), String> {
        if self
            .child
            .try_wait()
            .map_err(|error| format!("poll ctxmuxd before shutdown: {error}"))?
            .is_some()
        {
            return Ok(());
        }
        let interrupted = Command::new("kill")
            .arg("-INT")
            .arg(self.child.id().to_string())
            .status()
            .map_err(|error| format!("send SIGINT to ctxmuxd: {error}"))?;
        if !interrupted.success() {
            return Err(format!("SIGINT command failed with {interrupted}"));
        }
        for _ in 0..100 {
            match self.child.try_wait() {
                Ok(Some(_)) => return Ok(()),
                Ok(None) => std::thread::sleep(Duration::from_millis(20)),
                Err(error) => return Err(format!("wait for ctxmuxd shutdown: {error}")),
            }
        }
        Err("ctxmuxd did not exit within the interrupt shutdown deadline".to_owned())
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
        self.wait_stderr_line(
            "adopted inherited listener for handoff",
            timeout_secs,
            "incoming handoff image should log its resume signal",
        )
        .await
    }

    async fn wait_stderr_line(
        &self,
        needle: &str,
        timeout_secs: u64,
        timeout_message: &str,
    ) -> String {
        let lines = self
            .stderr_lines
            .as_ref()
            .expect("stderr matching requires a stderr-piped daemon");
        timeout(Duration::from_secs(timeout_secs), async {
            loop {
                {
                    let captured = lines.lock().expect("stderr buffer lock");
                    if let Some(line) = captured.iter().find(|line| line.contains(needle)) {
                        return line.clone();
                    }
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{timeout_message}"))
    }

    async fn wait_stderr_occurrences(&self, needle: &str, expected: usize) {
        let lines = self
            .stderr_lines
            .as_ref()
            .expect("stderr counting requires a stderr-piped daemon");
        timeout(Duration::from_secs(10), async {
            loop {
                let count = lines
                    .lock()
                    .expect("stderr buffer lock")
                    .iter()
                    .filter(|line| line.contains(needle))
                    .count();
                if count >= expected {
                    break;
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("expected daemon stderr occurrences arrive");
    }
}

impl Drop for TestDaemon {
    fn drop(&mut self) {
        if self.wait_for_interrupt_shutdown().is_err() {
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

fn externally_released_reader_shell(
    ready: &Path,
    release: &Path,
    expected_bytes: usize,
) -> RunSpec {
    RunSpec {
        program: "/bin/sh".to_owned(),
        args: vec![
            "-c".to_owned(),
            format!(
                concat!(
                    "stty raw -echo; ",
                    "printf ready > \"$CTXMUX_READER_READY\"; ",
                    "while [ ! -e \"$CTXMUX_READER_RELEASE\" ]; do sleep 0.01; done; ",
                    "dd bs=1 count={} 2>/dev/null | wc -c; ",
                    "printf 'CAPTURED\\n'; exec /bin/sleep 30"
                ),
                expected_bytes
            ),
        ],
        cwd: None,
        env: BTreeMap::from([
            (
                "CTXMUX_READER_READY".to_owned(),
                ready.to_string_lossy().into_owned(),
            ),
            (
                "CTXMUX_READER_RELEASE".to_owned(),
                release.to_string_lossy().into_owned(),
            ),
        ]),
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

fn recoverable_stop_marker_shell(marker: &Path, exit_on_term: bool) -> RunSpec {
    let action = if exit_on_term {
        "printf \"TERM\\n\" >> \"$CTXMUX_STOP_MARKER\"; exit 0"
    } else {
        "printf \"TERM\\n\" >> \"$CTXMUX_STOP_MARKER\""
    };
    RunSpec {
        program: "/bin/sh".to_owned(),
        args: vec![
            "-c".to_owned(),
            format!(
                "trap '{action}' TERM; printf 'READY\\n'; while :; do sleep 0.01; wait $!; done"
            ),
        ],
        cwd: None,
        env: BTreeMap::from([(
            "CTXMUX_STOP_MARKER".to_owned(),
            marker.to_string_lossy().into_owned(),
        )]),
        size: TerminalSize::default(),
        declared_inputs: Vec::new(),
    }
}

async fn wait_for_stop_marker_lines(marker: &Path, expected: usize) -> Vec<String> {
    timeout(Duration::from_secs(5), async {
        loop {
            let lines = std::fs::read_to_string(marker)
                .unwrap_or_default()
                .lines()
                .map(str::to_owned)
                .collect::<Vec<_>>();
            if lines.len() == expected {
                return lines;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("Stop marker reaches the expected physical signal count")
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
        ServerFrame::Hello { runtime }
            if runtime.protocol_generation == PROTOCOL_VERSION
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
        ServerFrame::Hello { runtime }
            if runtime.protocol_generation == PROTOCOL_VERSION
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

fn assert_control_failure(
    error: ClientError,
    expected_code: ErrorCode,
    expected_disposition: CommandDisposition,
) {
    match error {
        ClientError::ControlRejected { failure } => {
            assert_eq!(failure.error.code, expected_code);
            assert_eq!(failure.disposition, expected_disposition);
        }
        other => panic!(
            "expected {expected_code:?}/{expected_disposition:?} control failure, got {other:?}"
        ),
    }
}

fn assert_persistent_runtime_identity(runtime: &RuntimeIdentity) {
    assert_eq!(runtime.protocol_generation, PROTOCOL_VERSION);
    assert_eq!(
        runtime.runtime_id_persistence,
        RuntimeIdPersistence::StateDir
    );
    assert_eq!(runtime.platform, std::env::consts::OS);
    assert_eq!(runtime.arch, std::env::consts::ARCH);
    assert_eq!(
        runtime.capabilities,
        BTreeMap::from([
            (RUNTIME_CAPABILITY_NATIVE_START.to_owned(), 1),
            (RUNTIME_CAPABILITY_NATIVE_RECOVERABLE_INPUT.to_owned(), 1),
            (RUNTIME_CAPABILITY_NATIVE_RECOVERABLE_STOP.to_owned(), 1),
            (RUNTIME_CAPABILITY_NATIVE_FORK_LEVEL_A.to_owned(), 1),
            (
                RUNTIME_CAPABILITY_NATIVE_EXECUTE_MATERIALIZED_LEVEL_B.to_owned(),
                1,
            ),
            (RUNTIME_CAPABILITY_TMUX_DISCOVER.to_owned(), 1),
            (RUNTIME_CAPABILITY_PERSISTENT_STATE.to_owned(), 1),
            (
                RUNTIME_CAPABILITY_PLANNED_EXEC_UPGRADE_CONTINUITY.to_owned(),
                1,
            ),
        ])
    );
}

fn assert_memory_only_runtime_identity(runtime: &RuntimeIdentity) {
    assert_eq!(runtime.protocol_generation, PROTOCOL_VERSION);
    assert_eq!(runtime.runtime_id_persistence, RuntimeIdPersistence::Daemon);
    assert_eq!(runtime.platform, std::env::consts::OS);
    assert_eq!(runtime.arch, std::env::consts::ARCH);
    assert_eq!(
        runtime.capabilities,
        BTreeMap::from([
            (RUNTIME_CAPABILITY_NATIVE_START.to_owned(), 1),
            (RUNTIME_CAPABILITY_NATIVE_RECOVERABLE_INPUT.to_owned(), 1),
            (RUNTIME_CAPABILITY_NATIVE_RECOVERABLE_STOP.to_owned(), 1),
            (RUNTIME_CAPABILITY_NATIVE_FORK_LEVEL_A.to_owned(), 1),
            (
                RUNTIME_CAPABILITY_NATIVE_EXECUTE_MATERIALIZED_LEVEL_B.to_owned(),
                1,
            ),
            (RUNTIME_CAPABILITY_TMUX_DISCOVER.to_owned(), 1),
            (RUNTIME_CAPABILITY_TMUX_IMPORT.to_owned(), 1),
        ])
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_only_cold_replacement_changes_both_runtime_identities() {
    let directory = Arc::new(tempfile::tempdir().expect("create replacement temp directory"));
    let socket = directory.path().join("ctxmux.sock");
    let first = TestDaemon::start_memory_only_at(Arc::clone(&directory), socket.clone()).await;
    assert_eq!(first.client.socket_path(), socket);
    let first_runtime = first
        .client
        .runtime_info()
        .await
        .expect("read first memory-only Runtime identity");
    assert_memory_only_runtime_identity(&first_runtime);
    first.stop_and_wait();

    let replacement =
        TestDaemon::start_memory_only_at(Arc::clone(&directory), socket.clone()).await;
    assert_eq!(replacement.client.socket_path(), socket);
    let replacement_runtime = replacement
        .client
        .runtime_info()
        .await
        .expect("read replacement memory-only Runtime identity");
    assert_memory_only_runtime_identity(&replacement_runtime);
    assert_ne!(
        replacement_runtime.runtime_id, first_runtime.runtime_id,
        "memory-only cold replacement allocates a new logical Runtime"
    );
    assert_ne!(
        replacement_runtime.daemon_instance_id, first_runtime.daemon_instance_id,
        "memory-only cold replacement allocates a new daemon incarnation"
    );
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

#[cfg(target_os = "linux")]
fn process_file_descriptor_count(pid: u32) -> usize {
    std::fs::read_dir(format!("/proc/{pid}/fd"))
        .expect("read daemon file descriptors from procfs")
        .count()
}

#[cfg(target_os = "macos")]
fn process_file_descriptor_count(pid: u32) -> usize {
    let output = Command::new("lsof")
        .args(["-a", "-p", &pid.to_string(), "-Fn"])
        .output()
        .expect("inspect daemon file descriptors with lsof");
    assert!(output.status.success(), "lsof descriptor census failed");
    String::from_utf8(output.stdout)
        .expect("lsof descriptor census is UTF-8")
        .lines()
        .filter(|line| line.starts_with('f'))
        .count()
}

#[cfg(target_os = "linux")]
fn process_thread_count(pid: u32) -> usize {
    std::fs::read_dir(format!("/proc/{pid}/task"))
        .expect("read daemon threads from procfs")
        .count()
}

#[cfg(target_os = "macos")]
fn process_thread_count(pid: u32) -> usize {
    let output = Command::new("ps")
        .args(["-M", "-p", &pid.to_string()])
        .output()
        .expect("inspect daemon threads with ps");
    assert!(output.status.success(), "ps thread census failed");
    String::from_utf8(output.stdout)
        .expect("ps thread census is UTF-8")
        .lines()
        .skip(1)
        .count()
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
async fn stable_process_resources(pid: u32) -> (usize, usize) {
    timeout(Duration::from_secs(5), async {
        let mut previous = None;
        let mut stable_samples = 0;
        loop {
            let current = (
                process_file_descriptor_count(pid),
                process_thread_count(pid),
            );
            if previous == Some(current) {
                stable_samples += 1;
            } else {
                previous = Some(current);
                stable_samples = 1;
            }
            if stable_samples == 5 {
                return current;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("daemon resource census settles")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn capability_requirements_reject_before_a_real_memory_only_run_is_created() {
    let daemon = TestDaemon::start().await;
    assert!(
        daemon
            .client
            .list()
            .await
            .expect("list initial Runs")
            .is_empty()
    );

    for (capability, required_version, expected_advertised) in [
        (RUNTIME_CAPABILITY_PERSISTENT_STATE, 1, None),
        (RUNTIME_CAPABILITY_NATIVE_START, 2, Some(1)),
    ] {
        let guarded = Client::new(daemon.client.socket_path())
            .with_required_capabilities(RuntimeCapabilityRequirements::from([(
                capability.to_owned(),
                required_version,
            )]))
            .expect("construct guarded client");

        guarded.ping().await.expect("guarded ping stays raw");
        let runtime = guarded
            .runtime_info()
            .await
            .expect("guarded runtime_info stays raw");
        assert_eq!(runtime.runtime_id_persistence, RuntimeIdPersistence::Daemon);

        let error = guarded
            .start(RunSpec {
                program: "/bin/true".to_owned(),
                args: Vec::new(),
                cwd: None,
                env: BTreeMap::new(),
                size: TerminalSize::default(),
                declared_inputs: Vec::new(),
            })
            .await
            .expect_err("unsupported requirement rejects start locally");
        assert_eq!(
            error.control_disposition(),
            Some(CommandDisposition::NotApplied)
        );
        assert!(matches!(
            error,
            ClientError::UnsupportedCapability {
                capability: actual_capability,
                required_version: actual_required,
                advertised_version,
            } if actual_capability == capability
                && actual_required == required_version
                && advertised_version == expected_advertised
        ));
        assert!(
            daemon
                .client
                .list()
                .await
                .expect("list Runs after local rejection")
                .is_empty(),
            "a rejected capability requirement must not create a Run"
        );
    }
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

    let stop_operation = fresh_stop(&daemon.client, run.id).await;
    let mut stop = Box::pin(attachment.stop(stop_operation));
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
    let stop_operation = fresh_stop(&daemon.client, run.id).await;
    let stop = timeout(Duration::from_secs(3), control.stop(stop_operation))
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
        ServerFrame::Hello { runtime }
            if runtime.protocol_generation == PROTOCOL_VERSION
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
    let stop_operation = fresh_stop(&daemon.client, run.id).await;
    attachment
        .stop(stop_operation)
        .await
        .expect("stop command-id fence Run");
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

    let child_stop = fresh_stop(&daemon.client, child.id).await;
    daemon.client.stop(child_stop).await.expect("stop child");
    let parent_stop = fresh_stop(&daemon.client, parent.id).await;
    daemon.client.stop(parent_stop).await.expect("stop parent");
    assert!(process_exists(unrelated.pid()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recoverable_stop_concurrent_duplicate_joins_and_conflicts_before_mutation() {
    let daemon = TestDaemon::start().await;
    let marker = daemon.directory.path().join("concurrent-stop-signals.log");
    let first = daemon
        .client
        .start(recoverable_stop_marker_shell(&marker, true))
        .await
        .expect("start duplicate Stop fixture");
    let second = daemon
        .client
        .start(interactive_shell())
        .await
        .expect("start conflict sentinel Run");
    let (mut attachment, snapshot) = daemon
        .client
        .attach(first.id, 0)
        .await
        .expect("attach duplicate Stop fixture");
    let mut observed = replay_bytes(&snapshot.replay.chunks);
    let mut last_seq = snapshot.replay.latest_output_bytes;
    if !observed.windows(5).any(|window| window == b"READY") {
        wait_for_output(&mut attachment, &mut observed, &mut last_seq, b"READY").await;
    }
    attachment
        .detach()
        .await
        .expect("detach duplicate Stop readiness observer");

    let operation = fresh_stop(&daemon.client, first.id).await;
    let first_client = daemon.client.clone();
    let second_client = daemon.client.clone();
    let (left, right) = tokio::join!(
        first_client.stop(operation.clone()),
        second_client.stop(operation.clone()),
    );
    let left = left.expect("first duplicate receives the owner result");
    let right = right.expect("concurrent duplicate joins the owner result");
    assert_eq!(left.run.id, first.id);
    assert_eq!(right.run.id, first.id);
    assert_eq!(left.receipt, right.receipt);
    assert_eq!(
        wait_for_stop_marker_lines(&marker, 1).await,
        vec!["TERM"],
        "concurrent duplicate entered the physical Stop owner more than once"
    );

    let different_key = fresh_stop(&daemon.client, first.id).await;
    assert_control_failure(
        daemon
            .client
            .stop(different_key)
            .await
            .expect_err("another key cannot replace a retained Stop result"),
        ErrorCode::StopOperationConflict,
        CommandDisposition::NotApplied,
    );

    let mut cross_run = operation;
    cross_run.id = second.id;
    assert_control_failure(
        daemon
            .client
            .stop(cross_run)
            .await
            .expect_err("one Stop key cannot name another Run"),
        ErrorCode::StopOperationConflict,
        CommandDisposition::NotApplied,
    );
    assert!(
        daemon
            .client
            .status(second.id)
            .await
            .expect("conflict sentinel remains queryable")
            .state
            .is_running(),
        "conflicting key reuse entered the second Run Stop owner"
    );

    let second_stop = fresh_stop(&daemon.client, second.id).await;
    daemon
        .client
        .stop(second_stop)
        .await
        .expect("stop conflict sentinel with its own key");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recoverable_stop_response_loss_recovers_from_a_fresh_client() {
    let daemon = TestDaemon::start().await;
    let marker = daemon
        .directory
        .path()
        .join("response-loss-stop-signals.log");
    let run = daemon
        .client
        .start(recoverable_stop_marker_shell(&marker, false))
        .await
        .expect("start response-loss Stop fixture");
    let (mut attachment, snapshot) = daemon
        .client
        .attach(run.id, 0)
        .await
        .expect("attach response-loss Stop readiness observer");
    let mut observed = replay_bytes(&snapshot.replay.chunks);
    let mut last_seq = snapshot.replay.latest_output_bytes;
    if !observed.windows(5).any(|window| window == b"READY") {
        wait_for_output(&mut attachment, &mut observed, &mut last_seq, b"READY").await;
    }
    attachment
        .detach()
        .await
        .expect("detach response-loss Stop readiness observer");
    let operation = fresh_stop(&daemon.client, run.id).await;

    send_request_without_reading_response(
        &daemon.client,
        Request::Stop {
            operation: operation.clone(),
        },
    )
    .await;

    let fresh_client = Client::new(daemon.client.socket_path());
    let recovered = timeout(Duration::from_secs(5), fresh_client.stop(operation.clone()))
        .await
        .expect("daemon-owned Stop settlement remains live after response loss")
        .expect("fresh client recovers the exact Stop result");
    let replayed = fresh_client
        .stop(operation)
        .await
        .expect("settled Stop result remains replayable");
    assert_eq!(recovered.run.id, run.id);
    assert_eq!(replayed.run.id, run.id);
    assert_eq!(replayed.receipt, recovered.receipt);
    assert_eq!(
        wait_for_stop_marker_lines(&marker, 1).await,
        vec!["TERM"],
        "response-loss retry entered the physical Stop owner more than once"
    );
    assert!(!process_exists(run.pid.expect("native Run exposes a PID")));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recoverable_stop_attachment_short_to_attachment_recovers_one_result() {
    let daemon = TestDaemon::start().await;
    let marker = daemon.directory.path().join("short-to-attachment-stop.log");
    let run = daemon
        .client
        .start(recoverable_stop_marker_shell(&marker, false))
        .await
        .expect("start short-to-attachment Stop fixture");
    let (mut attachment, snapshot) = daemon
        .client
        .attach(run.id, 0)
        .await
        .expect("attach cross-path recovery client");
    let mut observed = replay_bytes(&snapshot.replay.chunks);
    let mut last_seq = snapshot.replay.latest_output_bytes;
    if !observed.windows(5).any(|window| window == b"READY") {
        wait_for_output(&mut attachment, &mut observed, &mut last_seq, b"READY").await;
    }
    let operation = fresh_stop(&daemon.client, run.id).await;
    send_request_without_reading_response(
        &daemon.client,
        Request::Stop {
            operation: operation.clone(),
        },
    )
    .await;

    let recovered = attachment
        .stop(operation)
        .await
        .expect("attachment joins the abandoned short Stop");
    assert_eq!(recovered.receipt.disposition, StopDisposition::Forced);
    assert_eq!(wait_for_stop_marker_lines(&marker, 1).await, ["TERM"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recoverable_stop_attachment_to_short_survives_attachment_disconnect() {
    let daemon = TestDaemon::start().await;
    let marker = daemon.directory.path().join("attachment-to-short-stop.log");
    let run = daemon
        .client
        .start(recoverable_stop_marker_shell(&marker, false))
        .await
        .expect("start attachment-to-short Stop fixture");
    let (mut attachment, snapshot) = daemon
        .client
        .attach(run.id, 0)
        .await
        .expect("attach abandoned Stop client");
    let mut observed = replay_bytes(&snapshot.replay.chunks);
    let mut last_seq = snapshot.replay.latest_output_bytes;
    if !observed.windows(5).any(|window| window == b"READY") {
        wait_for_output(&mut attachment, &mut observed, &mut last_seq, b"READY").await;
    }
    let attachment = Arc::new(attachment);
    let operation = fresh_stop(&daemon.client, run.id).await;
    let abandoned = {
        let attachment = Arc::clone(&attachment);
        let operation = operation.clone();
        tokio::spawn(async move { attachment.stop(operation).await })
    };
    assert_eq!(wait_for_stop_marker_lines(&marker, 1).await, ["TERM"]);
    abandoned.abort();
    let _ = abandoned.await;
    drop(attachment);

    let recovered = Client::new(daemon.client.socket_path())
        .stop(operation)
        .await
        .expect("short request recovers the disconnected attachment Stop");
    assert_eq!(recovered.receipt.disposition, StopDisposition::Forced);
    assert_eq!(
        std::fs::read_to_string(&marker)
            .expect("read attachment disconnect marker")
            .lines()
            .collect::<Vec<_>>(),
        ["TERM"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recoverable_stop_fresh_terminal_attachment_replays_settled_receipt() {
    let daemon = TestDaemon::start().await;
    let marker = daemon.directory.path().join("terminal-attachment-stop.log");
    let run = daemon
        .client
        .start(recoverable_stop_marker_shell(&marker, false))
        .await
        .expect("start terminal attachment Stop fixture");
    let (mut readiness, snapshot) = daemon
        .client
        .attach(run.id, 0)
        .await
        .expect("attach terminal Stop readiness observer");
    let mut observed = replay_bytes(&snapshot.replay.chunks);
    let mut last_seq = snapshot.replay.latest_output_bytes;
    if !observed.windows(5).any(|window| window == b"READY") {
        wait_for_output(&mut readiness, &mut observed, &mut last_seq, b"READY").await;
    }
    readiness
        .detach()
        .await
        .expect("detach terminal Stop readiness observer");
    let operation = fresh_stop(&daemon.client, run.id).await;
    let settled = daemon
        .client
        .stop(operation.clone())
        .await
        .expect("settle Stop before the fresh attachment");
    assert_eq!(settled.receipt.disposition, StopDisposition::Forced);
    assert_eq!(wait_for_stop_marker_lines(&marker, 1).await, ["TERM"]);
    let terminal = wait_until_exited(&daemon.client, run.id).await;

    let (attachment, snapshot) = daemon
        .client
        .attach(run.id, 0)
        .await
        .expect("attach after recoverable Stop settlement");
    assert_eq!(snapshot.run.state, terminal);
    let replayed = timeout(Duration::from_secs(2), attachment.stop(operation))
        .await
        .expect("terminal attachment keeps the recovery lane available")
        .expect("terminal attachment replays the retained Stop receipt");
    assert_eq!(replayed.receipt, settled.receipt);
    assert_eq!(
        attachment.next_event().await.expect("read terminal event"),
        Some(RunEvent::Exited { state: terminal })
    );
    assert_eq!(
        attachment
            .next_event()
            .await
            .expect("read terminal attachment EOF after replay"),
        None
    );
    assert_eq!(wait_for_stop_marker_lines(&marker, 1).await, ["TERM"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recoverable_stop_same_attachment_duplicates_join_and_conflicts_fail_closed() {
    let daemon = TestDaemon::start().await;
    let marker = daemon
        .directory
        .path()
        .join("attachment-duplicate-stop.log");
    let run = daemon
        .client
        .start(recoverable_stop_marker_shell(&marker, false))
        .await
        .expect("start attachment duplicate Stop fixture");
    let (mut attachment, snapshot) = daemon
        .client
        .attach(run.id, 0)
        .await
        .expect("attach duplicate Stop client");
    let mut observed = replay_bytes(&snapshot.replay.chunks);
    let mut last_seq = snapshot.replay.latest_output_bytes;
    if !observed.windows(5).any(|window| window == b"READY") {
        wait_for_output(&mut attachment, &mut observed, &mut last_seq, b"READY").await;
    }

    let attachment = Arc::new(attachment);
    let operation = fresh_stop(&daemon.client, run.id).await;
    let first = {
        let attachment = Arc::clone(&attachment);
        let operation = operation.clone();
        tokio::spawn(async move { attachment.stop(operation).await })
    };
    assert_eq!(wait_for_stop_marker_lines(&marker, 1).await, ["TERM"]);
    assert!(!first.is_finished(), "forced Stop remains in flight");

    let duplicate = {
        let attachment = Arc::clone(&attachment);
        let operation = operation.clone();
        tokio::spawn(async move { attachment.stop(operation).await })
    };
    tokio::task::yield_now().await;
    let mut conflicting = operation.clone();
    conflicting.operation_key = StopOperationKey::new("attachment-conflicting-stop").unwrap();
    assert_control_failure(
        attachment
            .stop(conflicting)
            .await
            .expect_err("another Stop operation conflicts before mutation"),
        ErrorCode::StopOperationConflict,
        CommandDisposition::NotApplied,
    );

    let first = timeout(Duration::from_secs(5), first)
        .await
        .expect("first attachment Stop settles")
        .expect("join first attachment Stop task")
        .expect("first attachment Stop is accepted");
    let duplicate = timeout(Duration::from_secs(5), duplicate)
        .await
        .expect("duplicate attachment Stop settles")
        .expect("join duplicate attachment Stop task")
        .expect("duplicate attachment Stop joins the retained operation");
    assert_eq!(first.receipt.disposition, StopDisposition::Forced);
    assert_eq!(duplicate.receipt, first.receipt);
    assert_ne!(duplicate.command_id, first.command_id);
    assert_eq!(wait_for_stop_marker_lines(&marker, 1).await, ["TERM"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recoverable_stop_attachment_disconnect_keeps_upgrade_drain_on_settlement_owner() {
    let daemon = TestDaemon::start_persistent().await;
    let marker = daemon.directory.path().join("attachment-upgrade-stop.log");
    let run = daemon
        .client
        .start(recoverable_stop_marker_shell(&marker, false))
        .await
        .expect("start attachment upgrade Stop fixture");
    let before = daemon
        .client
        .runtime_info()
        .await
        .expect("read Runtime identity before attachment Stop");
    let (mut attachment, snapshot) = daemon
        .client
        .attach(run.id, 0)
        .await
        .expect("attach upgrade Stop client");
    let mut observed = replay_bytes(&snapshot.replay.chunks);
    let mut last_seq = snapshot.replay.latest_output_bytes;
    if !observed.windows(5).any(|window| window == b"READY") {
        wait_for_output(&mut attachment, &mut observed, &mut last_seq, b"READY").await;
    }

    let attachment = Arc::new(attachment);
    let operation = fresh_stop(&daemon.client, run.id).await;
    let abandoned = {
        let attachment = Arc::clone(&attachment);
        let operation = operation.clone();
        tokio::spawn(async move { attachment.stop(operation).await })
    };
    assert_eq!(wait_for_stop_marker_lines(&marker, 1).await, ["TERM"]);
    assert!(!abandoned.is_finished(), "forced Stop remains in flight");

    daemon.sighup();
    timeout(Duration::from_secs(2), async {
        loop {
            match attachment.resize(TerminalSize { rows: 25, cols: 81 }).await {
                Err(ClientError::ControlRejected { failure })
                    if failure.error.code == ErrorCode::BackendUnavailable
                        && failure.disposition == CommandDisposition::NotApplied =>
                {
                    break;
                }
                Ok(_) | Err(_) => sleep(Duration::from_millis(5)).await,
            }
        }
    })
    .await
    .expect("attachment observes the planned-exec request drain");
    assert!(
        !abandoned.is_finished(),
        "fixture disconnects while daemon-owned Stop settlement is pending"
    );
    abandoned.abort();
    let _ = abandoned.await;
    drop(attachment);

    let resume = daemon.wait_resume_signal(10).await;
    assert!(resume.contains(" 0 run(s)"), "unexpected resume: {resume}");
    let after = daemon
        .client
        .runtime_info()
        .await
        .expect("read Runtime identity after attachment Stop handoff");
    assert_eq!(after.runtime_id, before.runtime_id);
    assert_eq!(after.daemon_instance_id, before.daemon_instance_id);
    let replayed = daemon
        .client
        .stop(operation)
        .await
        .expect("incoming image replays attachment-owned Stop settlement");
    assert_eq!(replayed.receipt.disposition, StopDisposition::Forced);
    assert_eq!(wait_for_stop_marker_lines(&marker, 1).await, ["TERM"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recoverable_stop_planned_exec_replays_the_settled_same_incarnation_result() {
    let daemon = TestDaemon::start_persistent().await;
    let marker = daemon
        .directory
        .path()
        .join("planned-exec-stop-signals.log");
    let run = daemon
        .client
        .start(recoverable_stop_marker_shell(&marker, false))
        .await
        .expect("start planned-exec Stop fixture");
    let before = daemon
        .client
        .runtime_info()
        .await
        .expect("read Runtime identity before Stop handoff");
    let (mut attachment, snapshot) = daemon
        .client
        .attach(run.id, 0)
        .await
        .expect("attach planned-exec Stop readiness observer");
    let mut observed = replay_bytes(&snapshot.replay.chunks);
    let mut last_seq = snapshot.replay.latest_output_bytes;
    if !observed.windows(5).any(|window| window == b"READY") {
        wait_for_output(&mut attachment, &mut observed, &mut last_seq, b"READY").await;
    }
    attachment
        .detach()
        .await
        .expect("detach planned-exec Stop readiness observer");
    let operation = fresh_stop(&daemon.client, run.id).await;

    send_request_without_reading_response(
        &daemon.client,
        Request::Stop {
            operation: operation.clone(),
        },
    )
    .await;
    assert_eq!(wait_for_stop_marker_lines(&marker, 1).await, ["TERM"]);
    assert!(!wait_until_exited(&daemon.client, run.id).await.is_running());

    daemon.sighup();
    let resume = daemon.wait_resume_signal(10).await;
    assert!(resume.contains(" 0 run(s)"), "unexpected resume: {resume}");
    let after = daemon
        .client
        .runtime_info()
        .await
        .expect("read Runtime identity after Stop handoff");
    assert_eq!(after.runtime_id, before.runtime_id);
    assert_eq!(after.daemon_instance_id, before.daemon_instance_id);

    let recovered = Client::new(daemon.client.socket_path())
        .stop(operation)
        .await
        .expect("incoming image replays handed-off Stop result");
    assert_eq!(recovered.run.id, run.id);
    assert_eq!(recovered.receipt.disposition, StopDisposition::Forced);
    assert_eq!(
        std::fs::read_to_string(&marker)
            .expect("read planned-exec Stop marker")
            .lines()
            .collect::<Vec<_>>(),
        ["TERM"],
        "planned-exec retry must not execute Stop again"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recoverable_stop_not_applied_does_not_retain_an_operation() {
    let daemon = TestDaemon::start().await;
    let run = daemon
        .client
        .start(RunSpec {
            program: "/bin/sh".to_owned(),
            args: vec!["-c".to_owned(), "exit 0".to_owned()],
            cwd: None,
            env: BTreeMap::new(),
            size: TerminalSize::default(),
            declared_inputs: Vec::new(),
        })
        .await
        .expect("start naturally exiting Stop fixture");
    assert!(!wait_until_exited(&daemon.client, run.id).await.is_running());

    let first = fresh_stop(&daemon.client, run.id).await;
    assert_control_failure(
        daemon
            .client
            .stop(first.clone())
            .await
            .expect_err("terminal Run rejects Stop before admission"),
        ErrorCode::InvalidRunState,
        CommandDisposition::NotApplied,
    );
    assert_control_failure(
        daemon
            .client
            .stop(first)
            .await
            .expect_err("same key is not retained after not-applied admission"),
        ErrorCode::InvalidRunState,
        CommandDisposition::NotApplied,
    );

    let different_key = fresh_stop(&daemon.client, run.id).await;
    assert_control_failure(
        daemon
            .client
            .stop(different_key)
            .await
            .expect_err("another key is not fenced by a not-applied Stop"),
        ErrorCode::InvalidRunState,
        CommandDisposition::NotApplied,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recoverable_stop_rejects_another_daemon_before_run_lookup_or_mutation() {
    let first = TestDaemon::start().await;
    let stale_instance = first
        .client
        .daemon_instance()
        .await
        .expect("read stale daemon incarnation");
    let second = TestDaemon::start().await;
    let run = second
        .client
        .start(interactive_shell())
        .await
        .expect("start replacement daemon Stop target");
    let operation_key = StopOperationKey::new("replacement-fence-stop").unwrap();
    assert_control_failure(
        second
            .client
            .stop(RecoverableStop {
                daemon_instance: stale_instance,
                operation_key: operation_key.clone(),
                id: run.id,
            })
            .await
            .expect_err("old daemon operation cannot Stop a replacement target"),
        ErrorCode::DaemonInstanceMismatch,
        CommandDisposition::NotApplied,
    );
    assert!(
        second
            .client
            .status(run.id)
            .await
            .expect("replacement target remains queryable")
            .state
            .is_running()
    );

    let current_instance = second.client.daemon_instance().await.unwrap();
    second
        .client
        .stop(RecoverableStop {
            daemon_instance: current_instance,
            operation_key,
            id: run.id,
        })
        .await
        .expect("same key remains available to the current daemon incarnation");
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
#[allow(
    clippy::too_many_lines,
    reason = "one end-to-end fixture keeps response loss, re-exec, retry, and child byte oracle in one causal proof"
)]
async fn upgrade_preserves_response_loss_input_ledger_and_cursor() {
    let daemon = TestDaemon::start_persistent().await;
    let run = daemon
        .client
        .start(raw_capture_shell(2))
        .await
        .expect("start persistent response-loss capture Run");
    let before_runtime = daemon
        .client
        .runtime_info()
        .await
        .expect("read pre-upgrade Runtime identity");
    assert_persistent_runtime_identity(&before_runtime);
    let daemon_instance = before_runtime.daemon_instance_id;
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
    let replay_cursor = last_seq;

    let key = InputOperationKey::new("upgrade-lost-response-input").unwrap();
    send_request_without_reading_response(
        &daemon.client,
        Request::RecoverableInput {
            operation: RecoverableInput {
                daemon_instance,
                operation_key: key.clone(),
                id: run.id,
                expected_byte: 0,
                data: b"A".to_vec(),
            },
        },
    )
    .await;
    timeout(Duration::from_secs(5), async {
        loop {
            if daemon
                .client
                .status(run.id)
                .await
                .is_ok_and(|status| status.applied_input_bytes == Some(1))
            {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("abandoned Input settles before upgrade");
    drop(attachment);

    daemon.sighup();
    let resume = daemon.wait_resume_signal(10).await;
    assert!(resume.contains(" 1 run(s)"), "unexpected resume: {resume}");
    let after_runtime = daemon
        .client
        .runtime_info()
        .await
        .expect("read post-upgrade Runtime identity");
    assert_persistent_runtime_identity(&after_runtime);
    assert_eq!(
        after_runtime.runtime_id, before_runtime.runtime_id,
        "planned upgrade preserves the logical Runtime"
    );
    assert_eq!(
        after_runtime.daemon_instance_id, daemon_instance,
        "planned upgrade preserves the retry fence"
    );

    let recovered = daemon
        .client
        .recoverable_input(RecoverableInput {
            daemon_instance,
            operation_key: key,
            id: run.id,
            expected_byte: 0,
            data: b"A".to_vec(),
        })
        .await
        .expect("post-upgrade retry returns the pre-upgrade applied range");
    assert_eq!(recovered.receipt.start_byte, 0);
    assert_eq!(recovered.receipt.end_byte, 1);
    assert_eq!(recovered.run.applied_input_bytes, Some(1));

    let (mut attachment, snapshot) = daemon
        .client
        .attach(run.id, replay_cursor)
        .await
        .expect("reconnect output oracle after upgrade");
    let mut observed = replay_bytes(&snapshot.replay.chunks);
    let mut last_seq = snapshot.replay.latest_output_bytes;
    daemon
        .client
        .recoverable_input(RecoverableInput {
            daemon_instance,
            operation_key: InputOperationKey::new("upgrade-following-input").unwrap(),
            id: run.id,
            expected_byte: 1,
            data: b"B".to_vec(),
        })
        .await
        .expect("following operation continues from restored cursor");
    wait_for_output(&mut attachment, &mut observed, &mut last_seq, b"CAPTURED").await;
    let captured = observed
        .windows(b"CAPTURED".len())
        .position(|window| window == b"CAPTURED")
        .expect("raw child publishes capture marker");
    let bytes = std::str::from_utf8(&observed[..captured])
        .expect("od byte oracle is ASCII")
        .split_ascii_whitespace()
        .filter_map(|byte| u8::from_str_radix(byte, 16).ok())
        .collect::<Vec<_>>();
    assert_eq!(
        bytes, b"AB",
        "retry after re-exec must not physically write A twice"
    );
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

    stop_run(&daemon.client, parent.id).await;
    stop_run(&daemon.client, child.id).await;
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
async fn daemon_rejects_generation_10_before_request_dispatch() {
    assert_eq!(
        PROTOCOL_VERSION, 11,
        "fixture must name the current generation"
    );
    let daemon = TestDaemon::start().await;
    let mut stream = UnixStream::connect(daemon.client.socket_path())
        .await
        .expect("connect raw protocol client");
    let generation_10_hello = encode_frame(&ClientFrame::Hello {
        hello: ClientHello { protocol: 10 },
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
        .write_all(format!("{generation_10_hello}\n{start}\n").as_bytes())
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
        ServerFrame::Hello { runtime }
            if runtime.protocol_generation == PROTOCOL_VERSION
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
async fn recoverable_stop_replays_settled_forced_result() {
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
    let stop_operation = fresh_stop(&daemon.client, run.id).await;
    let first_stop = daemon
        .client
        .stop(stop_operation.clone())
        .await
        .expect("stop HUP-ignoring Run");
    assert!(!wait_until_exited(&daemon.client, run.id).await.is_running());
    assert!(
        !process_exists(pid),
        "stopped direct child {pid} remained live"
    );
    let replayed_stop = daemon
        .client
        .stop(stop_operation)
        .await
        .expect("exact repeated Stop replays its result");
    assert_eq!(replayed_stop.receipt, first_stop.receipt);
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
    let stop_operation = fresh_stop(&daemon.client, run.id).await;
    daemon
        .client
        .stop(stop_operation)
        .await
        .expect("stop fixture");
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
            let operation = fresh_stop(&client, id).await;
            timeout(Duration::from_secs(3), client.stop(operation))
                .await
                .expect("accepted stubborn Stop stays inside its receipt fence")
        }));
    }
    sleep(Duration::from_millis(100)).await;

    let ninth = runs[CLEANUP_OWNERS].id;
    let rejected_operation = fresh_stop(&daemon.client, ninth).await;
    let error = daemon
        .client
        .stop(rejected_operation)
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
    let admitted_operation = fresh_stop(&daemon.client, ninth).await;
    daemon
        .client
        .stop(admitted_operation)
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
    let stop_operation = fresh_stop(&daemon.client, run.id).await;
    let accepted = daemon
        .client
        .stop(stop_operation)
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
    let stop_operation = fresh_stop(&stop_client, run.id).await;
    let (interrupt, stop) = tokio::join!(
        interrupt_client.interrupt(run.id),
        stop_client.stop(stop_operation),
    );
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
    let stop_operation = fresh_stop(&stop_client, run.id).await;
    let (interrupt, stop) = tokio::join!(
        interrupt_client.interrupt(run.id),
        stop_client.stop(stop_operation),
    );
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
    assert!(
        process_exists(pid),
        "child should still be running after SIGHUP"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_upgrade_before_extract_restores_complete_service() {
    let daemon = TestDaemon::start_persistent().await;
    let run = daemon
        .client
        .start(interactive_shell())
        .await
        .expect("start reversible-upgrade Run");
    let pid = run.pid.expect("live Run exposes pid");
    let (mut attachment, snapshot) = daemon
        .client
        .attach(run.id, 0)
        .await
        .expect("attach before reversible upgrade failure");
    let mut observed = replay_bytes(&snapshot.replay.chunks);
    let mut last_seq = snapshot.replay.latest_output_bytes;
    if !observed.windows(5).any(|window| window == b"READY") {
        wait_for_output(&mut attachment, &mut observed, &mut last_seq, b"READY").await;
    }

    let state_dir = daemon.directory.path().join("state");
    std::fs::set_permissions(&state_dir, std::fs::Permissions::from_mode(0o500))
        .expect("make handoff-file creation fail before extract");
    daemon.sighup();
    let aborted = daemon
        .wait_stderr_line(
            "upgrade aborted before extract, continuing to serve",
            5,
            "outgoing image should report a reversible pre-extract abort",
        )
        .await;
    assert!(
        aborted.contains("ctxmux-handoff"),
        "unexpected abort: {aborted}"
    );
    std::fs::set_permissions(&state_dir, std::fs::Permissions::from_mode(0o700))
        .expect("restore persistent state directory permissions");

    daemon
        .client
        .ping()
        .await
        .expect("same image resumes ordinary requests");
    let status = daemon
        .client
        .status(run.id)
        .await
        .expect("same Run remains published");
    assert_eq!(status.pid, Some(pid));
    assert_eq!(status.state, RunState::Running);
    assert!(process_exists(pid));
    attachment
        .input(b"after-abort\n".to_vec())
        .await
        .expect("existing attachment remains controllable after abort");
    wait_for_output(
        &mut attachment,
        &mut observed,
        &mut last_seq,
        b"OUT:after-abort",
    )
    .await;

    let second = daemon
        .client
        .start(interactive_shell())
        .await
        .expect("creation admission is fully restored after abort");
    let second_stop = fresh_stop(&daemon.client, second.id).await;
    daemon
        .client
        .stop(second_stop)
        .await
        .expect("stop second Run");
    let original_stop = fresh_stop(&daemon.client, run.id).await;
    daemon
        .client
        .stop(original_stop)
        .await
        .expect("stop original Run");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn adopted_child_preserves_public_signal_exit_identity() {
    let daemon = TestDaemon::start_persistent().await;
    let run = daemon
        .client
        .start(RunSpec {
            program: "/bin/sleep".to_owned(),
            args: vec!["30".to_owned()],
            cwd: None,
            env: BTreeMap::new(),
            size: TerminalSize::default(),
            declared_inputs: Vec::new(),
        })
        .await
        .expect("start signal-exit Run");
    let pid = run.pid.expect("signal-exit Run exposes pid");
    daemon.sighup();
    daemon.wait_resume_signal(10).await;
    assert!(
        Command::new("kill")
            .args(["-TERM", &pid.to_string()])
            .status()
            .expect("terminate re-adopted child")
            .success()
    );
    let state = wait_until_exited(&daemon.client, run.id).await;
    let RunState::Exited { code, signal } = state else {
        panic!("signal-terminated re-adopted child must exit: {state:?}");
    };
    assert!(
        signal.is_some(),
        "signal termination must not flatten into a normal numeric exit: code={code}"
    );
    assert_ne!(code, 143, "SIGTERM must not masquerade as exit code 143");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(
    clippy::too_many_lines,
    reason = "one real PTY fixture keeps drain admission, crossing ACK, exec resume, and exact-once cursor ordering auditable"
)]
async fn upgrade_drains_crossing_input_through_its_ack_response() {
    const INPUT_BYTES: usize = 256 * 1024;

    let daemon = TestDaemon::start_persistent().await;
    let ready = daemon.directory.path().join("crossing-reader-ready");
    let release = daemon.directory.path().join("crossing-reader-release");
    let run = daemon
        .client
        .start(externally_released_reader_shell(
            &ready,
            &release,
            INPUT_BYTES,
        ))
        .await
        .expect("start crossing-control reader");
    timeout(Duration::from_secs(5), async {
        while !ready.exists() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("child reaches its external read barrier");
    let (attachment, _) = daemon
        .client
        .attach(run.id, 0)
        .await
        .expect("attach crossing-control writer");
    let attachment = Arc::new(attachment);
    let input = {
        let attachment = Arc::clone(&attachment);
        tokio::spawn(async move { attachment.input(vec![b'A'; INPUT_BYTES]).await })
    };
    sleep(Duration::from_millis(100)).await;
    assert!(
        !input.is_finished(),
        "the real PTY write must be blocked before SIGHUP"
    );

    daemon.sighup();
    timeout(Duration::from_secs(5), async {
        loop {
            match attachment.resize(TerminalSize { rows: 25, cols: 81 }).await {
                Err(ClientError::ControlRejected { failure })
                    if failure.error.code == ErrorCode::BackendUnavailable
                        && failure.disposition == CommandDisposition::NotApplied =>
                {
                    break;
                }
                Ok(_) | Err(_) => sleep(Duration::from_millis(10)).await,
            }
        }
    })
    .await
    .expect("existing connection retry response proves the upgrade gate is draining");
    assert!(
        !input.is_finished(),
        "crossing Input remains owner-pending while the gate drains"
    );

    std::fs::write(&release, b"release").expect("release the child reader externally");
    let accepted = timeout(Duration::from_secs(10), input)
        .await
        .expect("crossing Input ACK precedes upgrade resume")
        .expect("join crossing Input task")
        .expect("crossing Input receives its unique accepted result");
    assert_eq!(
        accepted.receipt.written_bytes,
        u32::try_from(INPUT_BYTES).expect("fixture input length fits u32")
    );

    let resume = daemon.wait_resume_signal(10).await;
    assert!(resume.contains(" 1 run(s)"), "unexpected resume: {resume}");
    timeout(Duration::from_secs(5), async {
        loop {
            match attachment.next_event().await {
                Ok(Some(_)) => {}
                Ok(None) | Err(ClientError::Closed) => break,
                Err(error) => panic!("old attachment termination was unexpected: {error}"),
            }
        }
    })
    .await
    .expect("old attachment closes after its crossing ACK");
    let rejected = attachment
        .input(b"late".to_vec())
        .await
        .expect_err("old attachment cannot mutate the incoming image");
    assert_eq!(
        rejected.control_disposition(),
        Some(CommandDisposition::NotApplied)
    );

    let status = daemon
        .client
        .status(run.id)
        .await
        .expect("incoming image exposes the re-adopted Run");
    assert_eq!(
        status.applied_input_bytes,
        Some(INPUT_BYTES as u64),
        "crossing Input crosses the PTY boundary exactly once"
    );
    let (mut fresh, snapshot) = daemon
        .client
        .attach(run.id, 0)
        .await
        .expect("reconnect after crossing ACK");
    let mut observed = replay_bytes(&snapshot.replay.chunks);
    let mut last_seq = snapshot.replay.latest_output_bytes;
    if !observed
        .windows(b"CAPTURED".len())
        .any(|window| window == b"CAPTURED")
    {
        wait_for_output(&mut fresh, &mut observed, &mut last_seq, b"CAPTURED").await;
    }
    assert!(
        observed
            .windows(INPUT_BYTES.to_string().len())
            .any(|window| window == INPUT_BYTES.to_string().as_bytes()),
        "child confirms the complete crossing payload"
    );
    let stop_operation = fresh_stop(&daemon.client, run.id).await;
    daemon
        .client
        .stop(stop_operation)
        .await
        .expect("stop crossing fixture");
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repeated_upgrades_have_zero_settled_fd_and_thread_delta() {
    let daemon = TestDaemon::start_persistent().await;
    let daemon_pid = daemon.child.id();
    let socket_inode = std::fs::metadata(daemon.client.socket_path())
        .expect("stat published daemon socket")
        .ino();
    let run = daemon
        .client
        .start(non_reading_shell())
        .await
        .expect("start resource-census Run");
    let child_pid = run.pid.expect("resource-census Run exposes pid");
    let baseline = stable_process_resources(daemon_pid).await;

    for upgrade in 1..=2 {
        daemon.sighup();
        daemon
            .wait_stderr_occurrences("adopted inherited listener for handoff", upgrade)
            .await;
        timeout(Duration::from_secs(5), async {
            loop {
                if daemon.client.ping().await.is_ok() {
                    break;
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("incoming image resumes public service");

        assert_eq!(daemon.child.id(), daemon_pid, "exec keeps the daemon PID");
        let status = daemon
            .client
            .status(run.id)
            .await
            .expect("read re-adopted Run during census");
        assert_eq!(status.pid, Some(child_pid));
        assert!(process_exists(child_pid));
        assert_eq!(
            std::fs::metadata(daemon.client.socket_path())
                .expect("stat inherited listener socket")
                .ino(),
            socket_inode,
            "upgrade must not rebind the listener inode"
        );
        assert_eq!(
            stable_process_resources(daemon_pid).await,
            baseline,
            "upgrade {upgrade} must not add a permanent descriptor or owner thread"
        );
        let handoff_names = std::fs::read_dir(daemon.directory.path().join("state"))
            .expect("inspect state directory after upgrade")
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".ctxmux-handoff-")
            })
            .count();
        assert_eq!(
            handoff_names, 0,
            "unlinked handoff files must leave no pathname"
        );
    }

    let stop_operation = fresh_stop(&daemon.client, run.id).await;
    daemon
        .client
        .stop(stop_operation)
        .await
        .expect("stop resource-census Run");
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
    assert!(
        process_exists(p0),
        "child should be running before the upgrade"
    );

    // 2. Attach, reach READY, and drive I/O to a known cursor C0.
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
    // 3. Trigger the real re-exec and await the incoming image's resume signal.
    daemon.sighup();
    let resume = daemon.wait_resume_signal(10).await;
    assert!(
        resume.contains(" 1 run(s)"),
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

    // Connections are deliberately not migrated. Observe the old attachment's
    // transport terminate, then prove any later command is rejected locally as
    // not-applied rather than crossing into the incoming image ambiguously.
    timeout(Duration::from_secs(5), async {
        loop {
            match first_attachment.next_event().await {
                Ok(Some(RunEvent::Output { chunk })) => {
                    assert_eq!(chunk.start_byte, last_seq);
                    last_seq = chunk.end_byte;
                }
                Ok(Some(RunEvent::Gap {
                    latest_output_bytes,
                })) => panic!("old attachment observed an output gap at {latest_output_bytes}"),
                Ok(Some(event)) => {
                    panic!("old attachment ended with an unexpected event: {event:?}")
                }
                Ok(None) | Err(ClientError::Closed) => break,
                Err(error) => {
                    panic!("old attachment ended with an unexpected client error: {error}")
                }
            }
        }
    })
    .await
    .expect("old attachment transport terminates across exec");
    let old_command = first_attachment
        .input(b"must-not-cross\n".to_vec())
        .await
        .expect_err("closed pre-upgrade attachment rejects later commands");
    assert_eq!(
        old_command.control_disposition(),
        Some(CommandDisposition::NotApplied)
    );

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
    assert!(
        process_exists(p0),
        "child should be running before the upgrade"
    );

    // 2. Attach, reach READY, drive `before` to a known cursor C0.
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
        resume.contains(" 1 run(s)"),
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
