use std::{
    collections::BTreeMap,
    io,
    path::Path,
    process::{Child, Command, Stdio},
    time::Duration,
};

use ctxmux_client::{Attachment, Client, ClientError, replay_bytes};
use ctxmux_protocol::{
    ClientFrame, ClientHello, ErrorCode, MAX_FRAME_BYTES, PROTOCOL_VERSION, RunEvent, RunId,
    RunSpec, RunState, ServerFrame, TerminalSize, decode_frame, encode_frame,
};
use futures_util::{SinkExt, StreamExt};
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
    time::{sleep, timeout},
};
use tokio_util::codec::{Framed, LinesCodec};

struct TestDaemon {
    child: Child,
    _directory: TempDir,
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
            _directory: directory,
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
        let _ = self.child.kill();
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
    }
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
                RunEvent::Accepted { .. } => {}
                RunEvent::Gap { head_seq } => panic!("unexpected output gap at {head_seq}"),
                RunEvent::Exited { state } => {
                    panic!("Run exited before expected output: {state:?}")
                }
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
                RunEvent::Output { .. } | RunEvent::Accepted { .. } => {}
                RunEvent::Gap { head_seq } => panic!("unexpected output gap at {head_seq}"),
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
        stream
            .write_all(b"\n")
            .await
            .expect("terminate malformed-wire fixture");
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
        other => panic!("expected protocol error {expected:?}, got {other:?}"),
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn daemon_rejects_an_incompatible_protocol_generation() {
    let daemon = TestDaemon::start().await;
    let stream = UnixStream::connect(daemon.client.socket_path())
        .await
        .expect("connect raw protocol client");
    let mut wire = Framed::new(stream, LinesCodec::new_with_max_length(MAX_FRAME_BYTES));
    wire.send(
        encode_frame(&ClientFrame::Hello {
            hello: ClientHello { protocol: 999 },
        })
        .expect("encode incompatible hello"),
    )
    .await
    .expect("send incompatible hello");
    let line = wire
        .next()
        .await
        .expect("daemon responds")
        .expect("read daemon response");
    match decode_frame::<ServerFrame>(&line).expect("decode daemon response") {
        ServerFrame::Error { error } => assert_eq!(error.code, ErrorCode::VersionMismatch),
        other => panic!("expected version mismatch, got {other:?}"),
    }
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
        })
        .await
        .expect("start binary-output Run");
    let exited = wait_until_exited(&daemon.client, run.id).await;

    let (mut attachment, snapshot) = daemon
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
async fn stop_terminates_a_live_run_and_rejects_repeated_stop() {
    let daemon = TestDaemon::start().await;
    let run = daemon
        .client
        .start(RunSpec {
            program: "/bin/sh".to_owned(),
            args: vec!["-c".to_owned(), "while :; do sleep 1; done".to_owned()],
            cwd: None,
            env: BTreeMap::default(),
            size: TerminalSize::default(),
        })
        .await
        .expect("start long-lived Run");

    let (attachment, _) = daemon
        .client
        .attach(run.id, 0)
        .await
        .expect("attach before clean detach");
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
    daemon.client.stop(run.id).await.expect("stop live Run");
    timeout(Duration::from_secs(5), async {
        loop {
            let status = daemon.client.status(run.id).await.expect("read stop state");
            if !status.state.is_running() {
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("stopped Run should exit");
    assert_protocol_error(
        daemon
            .client
            .stop(run.id)
            .await
            .expect_err("repeated stop is rejected"),
        ErrorCode::InvalidRunState,
    );
}
