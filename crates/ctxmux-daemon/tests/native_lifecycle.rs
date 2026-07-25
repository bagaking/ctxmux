use std::{
    collections::BTreeMap,
    io,
    path::Path,
    process::{Child, Command, Stdio},
    time::Duration,
};

use ctxmux_client::{Attachment, Client, ClientError, replay_bytes};
use ctxmux_protocol::{
    ClientFrame, ClientHello, ErrorCode, ForkFidelity, ForkPlan, MAX_FRAME_BYTES, PROTOCOL_VERSION,
    RunEvent, RunId, RunInputKind, RunInputReference, RunLineage, RunSpec, RunState, ServerFrame,
    TerminalSize, decode_frame, encode_frame,
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
                RunEvent::Accepted { .. } => {}
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
                RunEvent::Output { .. } | RunEvent::Accepted { .. } => {}
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
async fn daemon_rejects_generation_2_before_request_dispatch() {
    assert_eq!(
        PROTOCOL_VERSION, 3,
        "fixture must name the current generation"
    );
    let daemon = TestDaemon::start().await;
    let mut stream = UnixStream::connect(daemon.client.socket_path())
        .await
        .expect("connect raw protocol client");
    let generation_2_hello = encode_frame(&ClientFrame::Hello {
        hello: ClientHello { protocol: 2 },
    })
    .expect("encode generation-2 hello");
    let start = encode_frame(&ClientFrame::Request {
        request: ctxmux_protocol::Request::Start {
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
        .write_all(format!("{generation_2_hello}\n{start}\n").as_bytes())
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
