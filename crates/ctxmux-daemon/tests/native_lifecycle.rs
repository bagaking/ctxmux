use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    path::Path,
    process::{Child, Command, Stdio},
    time::Duration,
};

use ctxmux_client::{Attachment, Client, ClientError, replay_bytes};
use ctxmux_protocol::{
    AttachmentCommandId, ClientFrame, ClientHello, CommandDisposition, ControlOutcome,
    CreateOperationKey, ErrorCode, ForkFidelity, ForkPlan, MAX_FRAME_BYTES, PROTOCOL_VERSION,
    Request, RunEvent, RunId, RunInputKind, RunInputReference, RunLineage, RunSpec, RunState,
    ServerFrame, TerminalSize, decode_frame, encode_frame,
};
use futures_util::{SinkExt, StreamExt, future::join_all};
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

    async fn from_spawned(
        child: Child,
        directory: TempDir,
        socket: impl Into<std::path::PathBuf>,
    ) -> Self {
        let socket = socket.into();
        let client = Client::new(socket);
        let mut daemon = Self {
            child,
            directory,
            client,
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
            protocol: PROTOCOL_VERSION
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
    last_seq: &mut u64,
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
                    *last_seq = chunk.seq;
                    observed.extend_from_slice(&chunk.data);
                }
                RunEvent::Gap { head_seq } => panic!("unexpected output gap at {head_seq}"),
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
                RunEvent::Gap { head_seq } => panic!("unexpected output gap at {head_seq}"),
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
            protocol: PROTOCOL_VERSION
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
    let mut last_seq = first_snapshot.replay.head_seq;
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
    last_seq = second_snapshot.replay.head_seq;
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
    let mut last_seq = snapshot.replay.head_seq;
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
                        RunEvent::Gap { head_seq } => panic!("unexpected post-stop gap at {head_seq}"),
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
    let mut last_seq = snapshot.replay.head_seq;
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
                RunEvent::Gap { head_seq } => panic!("unexpected saturation gap at {head_seq}"),
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
            protocol: PROTOCOL_VERSION
        }
    ));
    wire.send(
        encode_frame(&ClientFrame::Request {
            request: Request::Attach {
                id: run.id,
                after_seq: 0,
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
    let mut last_seq = snapshot.replay.head_seq;
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
    let mut parent_seq = parent_snapshot.replay.head_seq;
    let mut child_output = replay_bytes(&child_snapshot.replay.chunks);
    let mut child_seq = child_snapshot.replay.head_seq;
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
async fn daemon_rejects_generation_4_before_request_dispatch() {
    assert_eq!(
        PROTOCOL_VERSION, 5,
        "fixture must name the current generation"
    );
    let daemon = TestDaemon::start().await;
    let mut stream = UnixStream::connect(daemon.client.socket_path())
        .await
        .expect("connect raw protocol client");
    let generation_4_hello = encode_frame(&ClientFrame::Hello {
        hello: ClientHello { protocol: 4 },
    })
    .expect("encode generation-4 hello");
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
        .write_all(format!("{generation_4_hello}\n{start}\n").as_bytes())
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
            protocol: PROTOCOL_VERSION
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
        snapshot.replay.chunks.first().map(|chunk| chunk.seq),
        Some(snapshot.replay.oldest_seq)
    );
    assert_eq!(
        snapshot.replay.chunks.last().map(|chunk| chunk.seq),
        Some(snapshot.replay.head_seq)
    );
    assert!(
        snapshot
            .replay
            .chunks
            .windows(2)
            .all(|pair| pair[1].seq == pair[0].seq + 1)
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
    let mut last_seq = snapshot.replay.head_seq;
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
