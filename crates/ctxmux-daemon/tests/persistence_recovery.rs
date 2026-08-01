#![cfg(unix)]

use std::{
    collections::BTreeMap,
    os::unix::fs::{PermissionsExt, symlink},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

use ctxmux_client::{Attachment, Client, ClientError, replay_bytes};
use ctxmux_protocol::{
    CreateOperationKey, ErrorCode, ForkFidelity, ForkPlan, InterruptionReason, RunEvent, RunId,
    RunInfo, RunSpec, RunState, TerminalSize,
};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::time::{sleep, timeout};

struct Daemon {
    child: Child,
    socket: PathBuf,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovered_creation_keys_resolve_before_current_fork_state_checks() {
    let temp = TempDir::new().expect("create creation recovery fixture");
    let state_dir = temp.path().join("state");
    let socket = temp.path().join("ctxmux.sock");
    let parent_key = CreateOperationKey::new("persistent-parent-start").unwrap();
    let child_key = CreateOperationKey::new("persistent-level-b-child").unwrap();
    let parent_spec = shell_spec("exec /bin/sleep 60");
    let child_spec = shell_spec("printf 'persistent-child'");

    let mut first = Daemon::start(socket.clone(), &state_dir).await;
    let first_client = first.client();
    let parent = first_client
        .start_with_operation_key(parent_spec.clone(), parent_key.clone())
        .await
        .expect("start persistent parent");
    let child = first_client
        .fork_with_operation_key(
            parent.id,
            ForkPlan::LevelB {
                spec: child_spec.clone(),
            },
            child_key.clone(),
        )
        .await
        .expect("create persistent Level B child");
    wait_terminal(&first_client, child.id).await;
    first.kill_and_wait();

    let second = Daemon::start(socket, &state_dir).await;
    let client = second.client();
    let retried_parent = client
        .start_with_operation_key(parent_spec, parent_key)
        .await
        .expect("recovered Start key returns original Run");
    assert_eq!(retried_parent.id, parent.id);
    assert!(matches!(
        retried_parent.state,
        RunState::Interrupted {
            reason: InterruptionReason::DaemonRestart
        }
    ));

    let retried_child = client
        .fork_with_operation_key(parent.id, ForkPlan::LevelB { spec: child_spec }, child_key)
        .await
        .expect("existing child resolves before historical parent rejection");
    assert_eq!(retried_child.id, child.id);
    assert_invalid_state(
        &client
            .fork_with_operation_key(
                parent.id,
                ForkPlan::LevelB {
                    spec: shell_spec("exit 0"),
                },
                CreateOperationKey::new("fresh-level-b-after-restart").unwrap(),
            )
            .await,
    );
    assert_eq!(client.list().await.expect("list recovered Runs").len(), 2);
}

impl Daemon {
    async fn start(socket: PathBuf, state_dir: &Path) -> Self {
        let child = Command::new(env!("CARGO_BIN_EXE_ctxmuxd"))
            .arg("--socket")
            .arg(&socket)
            .arg("--state-dir")
            .arg(state_dir)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn persistent ctxmuxd");
        let mut daemon = Self { child, socket };
        daemon.wait_ready().await;
        daemon
    }

    fn client(&self) -> Client {
        Client::new(self.socket.clone())
    }

    async fn wait_ready(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if self.client().ping().await.is_ok() {
                return;
            }
            if let Some(status) = self.child.try_wait().expect("poll persistent ctxmuxd") {
                panic!("persistent ctxmuxd exited before readiness: {status}");
            }
            assert!(
                Instant::now() < deadline,
                "persistent ctxmuxd did not become ready"
            );
            sleep(Duration::from_millis(20)).await;
        }
    }

    fn kill_and_wait(&mut self) {
        if self
            .child
            .try_wait()
            .expect("poll ctxmuxd before kill")
            .is_none()
        {
            self.child.kill().expect("kill ctxmuxd");
        }
        self.child.wait().expect("reap ctxmuxd");
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        self.kill_and_wait();
    }
}

fn shell_spec(script: &str) -> RunSpec {
    RunSpec {
        program: "/bin/sh".to_owned(),
        args: vec!["-c".to_owned(), script.to_owned()],
        cwd: None,
        env: BTreeMap::new(),
        size: TerminalSize::default(),
        declared_inputs: Vec::new(),
    }
}

async fn wait_terminal(client: &Client, id: RunId) -> RunInfo {
    timeout(Duration::from_secs(10), async {
        loop {
            let info = client.status(id).await.expect("read Run status");
            if !info.state.is_running() {
                return info;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("Run reaches terminal state")
}

async fn terminal_event(attachment: &mut Attachment) -> RunEvent {
    timeout(Duration::from_secs(5), async {
        loop {
            match attachment
                .next_event()
                .await
                .expect("read recovered attachment event")
                .expect("terminal event precedes attachment close")
            {
                event @ (RunEvent::Exited { .. } | RunEvent::Interrupted { .. }) => return event,
                RunEvent::Output { .. } | RunEvent::Accepted { .. } => {}
                RunEvent::Gap { head_seq } => panic!("unexpected recovered gap at {head_seq}"),
                RunEvent::Tmux { event } => panic!("unexpected recovered tmux event: {event:?}"),
            }
        }
    })
    .await
    .expect("recovered terminal event arrives")
}

fn assert_invalid_state<T: std::fmt::Debug>(result: &Result<T, ClientError>) {
    assert!(
        matches!(
            result,
            Err(ClientError::Protocol {
                code: ErrorCode::InvalidRunState,
                ..
            })
        ),
        "expected invalid_run_state, got {result:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exited_run_recovers_metadata_replay_terminal_controls_and_level_a_fork() {
    let temp = TempDir::new().expect("create persistence fixture directory");
    let state_dir = temp.path().join("state");
    let socket = temp.path().join("ctxmux.sock");
    let mut first = Daemon::start(socket.clone(), &state_dir).await;
    let first_client = first.client();
    let parent = first_client
        .start(shell_spec("printf 'persisted-output'"))
        .await
        .expect("start durable parent");
    assert_eq!(parent.durable_head_seq, Some(0));
    let exited = wait_terminal(&first_client, parent.id).await;
    assert!(matches!(exited.state, RunState::Exited { code: 0, .. }));
    assert_eq!(exited.durable_head_seq, Some(exited.head_seq));
    assert!(exited.head_seq > 0);
    first.kill_and_wait();

    let second = Daemon::start(socket, &state_dir).await;
    let client = second.client();
    let recovered = client
        .status(parent.id)
        .await
        .expect("status recovered parent");
    assert_eq!(recovered.id, parent.id);
    assert_eq!(recovered.spec, parent.spec);
    assert_eq!(recovered.lineage, None);
    assert_eq!(recovered.state, exited.state);
    assert_eq!(recovered.head_seq, exited.durable_head_seq.unwrap());
    assert_eq!(recovered.durable_head_seq, Some(recovered.head_seq));

    let (mut attachment, snapshot) = client
        .attach(parent.id, 0)
        .await
        .expect("attach recovered parent");
    assert_eq!(replay_bytes(&snapshot.replay.chunks), b"persisted-output");
    assert!(matches!(
        terminal_event(&mut attachment).await,
        RunEvent::Exited {
            state: RunState::Exited { code: 0, .. }
        }
    ));
    assert_invalid_state(&client.input(parent.id, b"never".to_vec()).await);
    assert_invalid_state(
        &client
            .resize(parent.id, TerminalSize { cols: 90, rows: 30 })
            .await,
    );
    assert_invalid_state(&client.stop(parent.id).await);

    let child = client
        .fork(parent.id, ForkPlan::LevelA)
        .await
        .expect("Level A fork recovered parent");
    assert_ne!(child.id, parent.id);
    assert_eq!(child.spec, parent.spec);
    assert_eq!(
        child.lineage,
        Some(ctxmux_protocol::RunLineage {
            parent: parent.id,
            fidelity: ForkFidelity::LevelA,
        })
    );
    let _ = wait_terminal(&client, child.id).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn running_record_becomes_interrupted_without_adopting_or_signalling_its_pid() {
    let temp = TempDir::new().expect("create stale PID fixture directory");
    let state_dir = temp.path().join("state");
    let socket = temp.path().join("ctxmux.sock");
    let mut first = Daemon::start(socket.clone(), &state_dir).await;
    let first_client = first.client();
    let live = first_client
        .start(shell_spec(
            "trap '' HUP TERM; printf 'stale-pid-sentinel'; exec /bin/sleep 60",
        ))
        .await
        .expect("start HUP-ignoring sentinel");
    let sentinel_pid = live.pid.expect("sentinel has a PID");
    timeout(Duration::from_secs(5), async {
        loop {
            let info = first_client
                .status(live.id)
                .await
                .expect("read live status");
            if info.durable_head_seq.is_some_and(|head| head > 0) {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("sentinel output reaches durable cursor");
    first.kill_and_wait();
    assert!(process_is_alive(sentinel_pid));

    let mut unrelated = Command::new("/bin/sleep")
        .arg("60")
        .spawn()
        .expect("start unrelated PID sentinel");
    let unrelated_pid = unrelated.id();
    let database = state_dir.join("state.sqlite3");
    let connection = rusqlite::Connection::open(&database).expect("open stale PID state");
    connection
        .execute(
            "UPDATE runs SET pid = ?2 WHERE id = ?1 AND state_kind = 'running'",
            rusqlite::params![live.id.to_string(), unrelated_pid],
        )
        .expect("replace stored PID with unrelated live process");
    drop(connection);
    assert!(process_is_alive(unrelated_pid));

    let second = Daemon::start(socket, &state_dir).await;
    let client = second.client();
    let recovered = client
        .status(live.id)
        .await
        .expect("status interrupted Run");
    assert_eq!(
        recovered.state,
        RunState::Interrupted {
            reason: InterruptionReason::DaemonRestart
        }
    );
    assert_eq!(recovered.pid, None);
    assert_eq!(recovered.durable_head_seq, Some(recovered.head_seq));
    assert_invalid_state(&client.input(live.id, b"never".to_vec()).await);
    assert_invalid_state(
        &client
            .resize(live.id, TerminalSize { cols: 81, rows: 25 })
            .await,
    );
    assert_invalid_state(&client.stop(live.id).await);
    assert_invalid_state(
        &client
            .fork(
                live.id,
                ForkPlan::LevelB {
                    spec: shell_spec("exit 0"),
                },
            )
            .await,
    );
    assert!(process_is_alive(sentinel_pid));
    assert!(process_is_alive(unrelated_pid));

    let (mut attachment, snapshot) = client
        .attach(live.id, 0)
        .await
        .expect("attach interrupted Run");
    assert_eq!(replay_bytes(&snapshot.replay.chunks), b"stale-pid-sentinel");
    assert_eq!(
        terminal_event(&mut attachment).await,
        RunEvent::Interrupted {
            reason: InterruptionReason::DaemonRestart
        }
    );
    assert!(process_is_alive(sentinel_pid));
    assert!(process_is_alive(unrelated_pid));
    kill_process(sentinel_pid);
    unrelated.kill().expect("kill unrelated PID sentinel");
    unrelated.wait().expect("reap unrelated PID sentinel");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persisted_replay_prunes_to_the_exact_per_run_budget_and_recovers_the_tail() {
    const RETENTION_BYTES: usize = 4 * 1024 * 1024;
    let temp = TempDir::new().expect("create durable retention fixture directory");
    let state_dir = temp.path().join("state");
    let socket = temp.path().join("ctxmux.sock");
    let mut first = Daemon::start(socket.clone(), &state_dir).await;
    let client = first.client();
    let run = client
        .start(shell_spec(
            "dd if=/dev/zero bs=8192 count=528 2>/dev/null | tr '\\000' Z",
        ))
        .await
        .expect("start output retention Run");
    let exited = wait_terminal(&client, run.id).await;
    assert_eq!(exited.durable_head_seq, Some(exited.head_seq));
    assert!(exited.oldest_seq > 1);
    let (_, live_snapshot) = client
        .attach(run.id, 0)
        .await
        .expect("attach retained live-daemon tail");
    let live_bytes = replay_bytes(&live_snapshot.replay.chunks);
    assert!(live_snapshot.replay.truncated);
    assert!(live_bytes.len() <= RETENTION_BYTES);
    assert!(live_bytes.iter().all(|byte| *byte == b'Z'));
    first.kill_and_wait();

    let second = Daemon::start(socket, &state_dir).await;
    let (_, recovered_snapshot) = second
        .client()
        .attach(run.id, 0)
        .await
        .expect("attach recovered retained tail");
    let recovered_bytes = replay_bytes(&recovered_snapshot.replay.chunks);
    assert_eq!(recovered_bytes, live_bytes);
    assert_eq!(recovered_snapshot.replay.oldest_seq, exited.oldest_seq);
    assert_eq!(recovered_snapshot.replay.head_seq, exited.head_seq);
    assert!(recovered_snapshot.replay.truncated);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn state_lock_and_unknown_schema_fail_before_socket_publication() {
    let temp = TempDir::new().expect("create state ownership fixture directory");
    let state_dir = temp.path().join("state");
    let socket = temp.path().join("owner.sock");
    let owner = Daemon::start(socket, &state_dir).await;
    assert_eq!(
        std::fs::metadata(&state_dir)
            .expect("read state directory metadata")
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    for name in [
        "state.lock",
        "state.sqlite3",
        "state.sqlite3-wal",
        "state.sqlite3-shm",
    ] {
        let path = state_dir.join(name);
        assert!(path.is_file(), "{name} is a regular state file");
        assert_eq!(
            std::fs::metadata(path)
                .expect("read state file metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600,
            "{name} is owner-only"
        );
    }
    let contender_socket = temp.path().join("contender.sock");
    let contender = Command::new(env!("CARGO_BIN_EXE_ctxmuxd"))
        .arg("--socket")
        .arg(&contender_socket)
        .arg("--state-dir")
        .arg(&state_dir)
        .output()
        .expect("run second state owner");
    assert!(!contender.status.success());
    assert!(
        String::from_utf8_lossy(&contender.stderr).contains("already in use"),
        "unexpected contender stderr: {}",
        String::from_utf8_lossy(&contender.stderr)
    );
    assert!(!contender_socket.exists());
    drop(owner);

    let database = state_dir.join("state.sqlite3");
    let connection = rusqlite::Connection::open(&database).expect("open stopped state database");
    connection
        .pragma_update(None, "user_version", 1_i64)
        .expect("write prior unsupported schema version");
    drop(connection);
    let version_socket = temp.path().join("version.sock");
    let version = Command::new(env!("CARGO_BIN_EXE_ctxmuxd"))
        .arg("--socket")
        .arg(&version_socket)
        .arg("--state-dir")
        .arg(&state_dir)
        .output()
        .expect("run daemon against prior schema");
    assert!(!version.status.success());
    assert!(String::from_utf8_lossy(&version.stderr).contains("unsupported ctxmux state schema"));
    assert!(!version_socket.exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_persisted_creation_key_fails_before_socket_publication() {
    let temp = TempDir::new().expect("create creation-key corruption fixture");
    let state_dir = temp.path().join("state");
    let socket = temp.path().join("ctxmux.sock");
    let mut first = Daemon::start(socket, &state_dir).await;
    let run = first
        .client()
        .start(shell_spec("exit 0"))
        .await
        .expect("create corruption source Run");
    wait_terminal(&first.client(), run.id).await;
    first.kill_and_wait();

    let database = state_dir.join("state.sqlite3");
    rusqlite::Connection::open(database)
        .expect("open creation-key state")
        .execute(
            "UPDATE runs SET creation_key = '' WHERE id = ?1",
            [run.id.to_string()],
        )
        .expect("write empty creation key");
    let failed_socket = temp.path().join("failed.sock");
    let output = rejected_persistent_daemon(&failed_socket, &state_dir);
    assert!(String::from_utf8_lossy(&output.stderr).contains("corrupt"));
    assert!(!failed_socket.exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrupt_replay_generation_fails_closed_without_partial_run_exposure() {
    let temp = TempDir::new().expect("create corrupt generation fixture directory");
    let state_dir = temp.path().join("state");
    let socket = temp.path().join("ctxmux.sock");
    let mut first = Daemon::start(socket, &state_dir).await;
    let client = first.client();
    let run = client
        .start(shell_spec("printf 'atomic-generation'"))
        .await
        .expect("start corruption source Run");
    let exited = wait_terminal(&client, run.id).await;
    assert!(exited.head_seq > 0);
    first.kill_and_wait();

    let database = state_dir.join("state.sqlite3");
    let connection = rusqlite::Connection::open(&database).expect("open state for corruption");
    connection
        .execute(
            "UPDATE runs SET durable_head_seq = durable_head_seq + 1 WHERE id = ?1",
            [run.id.to_string()],
        )
        .expect("create parseable mixed replay generation");
    drop(connection);
    let failed_socket = temp.path().join("failed.sock");
    let failed = Command::new(env!("CARGO_BIN_EXE_ctxmuxd"))
        .arg("--socket")
        .arg(&failed_socket)
        .arg("--state-dir")
        .arg(&state_dir)
        .output()
        .expect("run daemon against mixed generation");
    assert!(!failed.status.success());
    assert!(String::from_utf8_lossy(&failed.stderr).contains("corrupt"));
    assert!(!failed_socket.exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn semantically_invalid_native_specs_fail_before_socket_or_sibling_publication() {
    let temp = TempDir::new().expect("create invalid spec fixture directory");
    let state_dir = temp.path().join("state");
    let socket = temp.path().join("ctxmux.sock");
    let mut first = Daemon::start(socket, &state_dir).await;
    let client = first.client();
    let corrupted = client
        .start(shell_spec("printf 'corrupted-source'"))
        .await
        .expect("start corruption source Run");
    let sibling = client
        .start(shell_spec("printf 'valid-sibling'"))
        .await
        .expect("start valid sibling Run");
    wait_terminal(&client, corrupted.id).await;
    wait_terminal(&client, sibling.id).await;
    first.kill_and_wait();

    let database = state_dir.join("state.sqlite3");
    let original_json: String = rusqlite::Connection::open(&database)
        .expect("open state for spec fixture")
        .query_row(
            "SELECT spec_json FROM runs WHERE id = ?1",
            [corrupted.id.to_string()],
            |row| row.get(0),
        )
        .expect("read original native spec");
    let original: Value = serde_json::from_str(&original_json).expect("parse original native spec");

    let mut empty_program = original.clone();
    empty_program["program"] = json!("");
    let mut zero_columns = original.clone();
    zero_columns["size"]["cols"] = json!(0);
    let mut zero_rows = original.clone();
    zero_rows["size"]["rows"] = json!(0);
    let mut empty_reference = original.clone();
    empty_reference["declared_inputs"] = json!([{ "kind": "workspace", "reference": "" }]);

    let cases = [
        ("null", "null".to_owned(), "invalid spec for"),
        (
            "empty-program",
            serde_json::to_string(&empty_program).expect("encode empty program"),
            "Run program must not be empty",
        ),
        (
            "zero-columns",
            serde_json::to_string(&zero_columns).expect("encode zero columns"),
            "terminal rows and columns must be greater than zero",
        ),
        (
            "zero-rows",
            serde_json::to_string(&zero_rows).expect("encode zero rows"),
            "terminal rows and columns must be greater than zero",
        ),
        (
            "empty-reference",
            serde_json::to_string(&empty_reference).expect("encode empty reference"),
            "Run input references must not be empty",
        ),
    ];

    for (label, spec_json, expected) in cases {
        rewrite_persisted_spec(&database, corrupted.id, &spec_json);
        let failed_socket = temp.path().join(format!("failed-{label}.sock"));
        let output = rejected_persistent_daemon(&failed_socket, &state_dir);
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("corrupt"),
            "unexpected {label} startup error: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            String::from_utf8_lossy(&output.stderr).contains(expected),
            "{label} failed through the wrong invariant: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            !failed_socket.exists(),
            "{label} published a socket exposing the valid sibling"
        );
        let rejected_client = Client::new(&failed_socket);
        assert!(matches!(
            rejected_client.list().await,
            Err(ClientError::Connect { .. })
        ));
        assert!(matches!(
            rejected_client.attach(sibling.id, 0).await,
            Err(ClientError::Connect { .. })
        ));
    }

    rewrite_persisted_spec(&database, corrupted.id, &original_json);
    let recovered_socket = temp.path().join("recovered.sock");
    let recovered = Daemon::start(recovered_socket, &state_dir).await;
    let recovered_runs = recovered
        .client()
        .list()
        .await
        .expect("list repaired fixture state");
    assert!(recovered_runs.iter().any(|run| run.id == corrupted.id));
    assert!(recovered_runs.iter().any(|run| run.id == sibling.id));
}

#[test]
fn unsafe_state_directory_and_sidecar_paths_fail_before_socket_publication() {
    let temp = TempDir::new().expect("create unsafe state path fixture directory");

    let insecure = temp.path().join("insecure");
    std::fs::create_dir(&insecure).expect("create insecure state directory");
    std::fs::set_permissions(&insecure, std::fs::Permissions::from_mode(0o755))
        .expect("set insecure state directory mode");
    assert_startup_rejected(
        &temp.path().join("insecure.sock"),
        &insecure,
        "permissions must be exactly 0700",
    );

    let real = temp.path().join("real");
    std::fs::create_dir(&real).expect("create real state directory");
    std::fs::set_permissions(&real, std::fs::Permissions::from_mode(0o700))
        .expect("protect real state directory");
    let linked = temp.path().join("linked");
    symlink(&real, &linked).expect("create state directory symlink");
    assert_startup_rejected(&temp.path().join("linked.sock"), &linked, "not a symlink");

    let sidecar_state = temp.path().join("sidecar-state");
    std::fs::create_dir(&sidecar_state).expect("create sidecar state directory");
    std::fs::set_permissions(&sidecar_state, std::fs::Permissions::from_mode(0o700))
        .expect("protect sidecar state directory");
    let unrelated = temp.path().join("unrelated-database");
    std::fs::write(&unrelated, b"unrelated").expect("write unrelated sidecar target");
    symlink(&unrelated, sidecar_state.join("state.sqlite3"))
        .expect("create database sidecar symlink");
    assert_startup_rejected(
        &temp.path().join("sidecar.sock"),
        &sidecar_state,
        "regular file",
    );
    assert_eq!(
        std::fs::read(unrelated).expect("read untouched sidecar target"),
        b"unrelated"
    );
}

fn process_is_alive(pid: u32) -> bool {
    Command::new("/bin/kill")
        .arg("-0")
        .arg(pid.to_string())
        .status()
        .is_ok_and(|status| status.success())
}

fn rewrite_persisted_spec(database: &Path, id: RunId, spec_json: &str) {
    let connection = rusqlite::Connection::open(database).expect("open state to rewrite spec");
    let updated = connection
        .execute(
            "UPDATE runs
             SET metadata_bytes = metadata_bytes - length(CAST(spec_json AS BLOB)) + ?3,
                 spec_json = ?2
             WHERE id = ?1",
            rusqlite::params![
                id.to_string(),
                spec_json,
                i64::try_from(spec_json.len()).expect("fixture spec length fits SQLite")
            ],
        )
        .expect("rewrite persisted native spec and metadata accounting");
    assert_eq!(updated, 1);
}

fn rejected_persistent_daemon(socket: &Path, state_dir: &Path) -> std::process::Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_ctxmuxd"))
        .arg("--socket")
        .arg(socket)
        .arg("--state-dir")
        .arg(state_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn rejected persistent daemon");
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if child
            .try_wait()
            .expect("poll rejected persistent daemon")
            .is_some()
        {
            let output = child
                .wait_with_output()
                .expect("collect rejected persistent daemon output");
            assert!(!output.status.success());
            return output;
        }
        if socket.exists() || Instant::now() >= deadline {
            let _ = child.kill();
            let output = child
                .wait_with_output()
                .expect("reap unexpectedly live persistent daemon");
            panic!(
                "invalid state did not fail before socket publication: status={} stderr={}",
                output.status,
                String::from_utf8_lossy(&output.stderr)
            );
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn kill_process(pid: u32) {
    let status = Command::new("/bin/kill")
        .arg("-KILL")
        .arg(pid.to_string())
        .status()
        .expect("kill test-owned stale PID sentinel");
    assert!(status.success());
}

fn assert_startup_rejected(socket: &Path, state_dir: &Path, expected: &str) {
    let output = Command::new(env!("CARGO_BIN_EXE_ctxmuxd"))
        .arg("--socket")
        .arg(socket)
        .arg("--state-dir")
        .arg(state_dir)
        .output()
        .expect("run daemon against unsafe state path");
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains(expected),
        "unexpected startup failure: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!socket.exists());
}
