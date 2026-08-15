use std::{
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};

use ctxmux_client::Client;
use ctxmux_protocol::DaemonInstanceId;

const DEADLINE: Duration = Duration::from_secs(15);

struct SpawnedSocket {
    socket: PathBuf,
}

impl Drop for SpawnedSocket {
    fn drop(&mut self) {
        terminate_daemon_for_socket(&self.socket);
    }
}

fn ctxmux_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_ctxmux"))
}

fn ctxmuxd_bin() -> PathBuf {
    let path = ctxmux_bin().with_file_name("ctxmuxd");
    assert!(
        path.is_file(),
        "ctxmuxd must be built next to ctxmux at {}",
        path.display()
    );
    path
}

fn terminate_daemon_for_socket(socket: &Path) {
    let pattern = format!("ctxmuxd --socket {}", socket.display());
    let Ok(output) = Command::new("pgrep").arg("-f").arg(&pattern).output() else {
        return;
    };
    for pid in String::from_utf8_lossy(&output.stdout).split_whitespace() {
        let _ = Command::new("kill").arg("-TERM").arg(pid).status();
    }
}

fn ping(socket: &Path) -> std::process::Output {
    let _ = ctxmuxd_bin();
    Command::new(ctxmux_bin())
        .arg("--socket")
        .arg(socket)
        .arg("ping")
        .env_remove("CTXMUX_SOCKET")
        .output()
        .expect("run ctxmux ping")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ping_starts_a_sibling_daemon_when_nothing_is_listening() {
    let directory = tempfile::tempdir().expect("create auto-start fixture directory");
    let socket = directory.path().join("ctxmux.sock");
    let _guard = SpawnedSocket {
        socket: socket.clone(),
    };
    assert!(
        !socket.exists(),
        "auto-start fixture must not pre-create the socket"
    );

    let output = ping(&socket);
    assert!(
        output.status.success(),
        "ctxmux ping should start ctxmuxd: status={:?} stdout={} stderr={}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "ok");

    Client::new(&socket)
        .ping()
        .await
        .expect("spawned ctxmuxd should still answer after the CLI exits");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ping_reuses_an_already_listening_daemon() {
    let directory = tempfile::tempdir().expect("create reuse fixture directory");
    let socket = directory.path().join("ctxmux.sock");
    let server = tokio::spawn(ctxmux_daemon::serve(socket.clone()));
    let client = Client::new(&socket);
    let instance = tokio::time::timeout(DEADLINE, async {
        loop {
            if let Ok(instance) = client.daemon_instance().await {
                return instance;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("in-process daemon accepts connections");

    let output = ping(&socket);
    assert!(
        output.status.success(),
        "ctxmux ping against a live daemon failed: stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );

    let after: DaemonInstanceId = client
        .daemon_instance()
        .await
        .expect("live daemon remains reachable");
    assert_eq!(
        instance, after,
        "CLI ping must reuse the listening daemon instead of replacing it"
    );

    server.abort();
    let _ = server.await;
}

#[test]
fn unknown_command_does_not_start_a_daemon() {
    let directory = tempfile::tempdir().expect("create unknown-command fixture directory");
    let socket = directory.path().join("ctxmux.sock");
    let output = Command::new(ctxmux_bin())
        .arg("--socket")
        .arg(&socket)
        .arg("nope")
        .env_remove("CTXMUX_SOCKET")
        .output()
        .expect("run ctxmux with an unknown command");
    assert!(
        !output.status.success(),
        "unknown command should fail: stdout={}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("unknown command"),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!socket.exists(), "unknown commands must not spawn ctxmuxd");
}

#[test]
fn version_does_not_start_a_daemon() {
    let directory = tempfile::tempdir().expect("create version fixture directory");
    let socket = directory.path().join("ctxmux.sock");
    let output = Command::new(ctxmux_bin())
        .env("CTXMUX_SOCKET", &socket)
        .arg("--version")
        .output()
        .expect("run ctxmux --version");
    assert!(
        output.status.success(),
        "ctxmux --version failed: stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !socket.exists(),
        "--version must not spawn ctxmuxd or create the socket"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ping_uses_the_runtime_dir_default_when_no_socket_is_supplied() {
    let runtime = tempfile::tempdir().expect("create runtime-dir fixture");
    let socket = runtime.path().join("ctxmux").join("ctxmux.sock");
    let _guard = SpawnedSocket {
        socket: socket.clone(),
    };

    let output = Command::new(ctxmux_bin())
        .env("XDG_RUNTIME_DIR", runtime.path())
        .env_remove("CTXMUX_SOCKET")
        .arg("ping")
        .output()
        .expect("run ctxmux ping with default socket");
    assert!(
        output.status.success(),
        "default-socket ping failed: status={:?} stdout={} stderr={}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "ok");
    assert!(
        socket.exists(),
        "CLI should publish the default runtime-dir socket"
    );

    Client::new(&socket)
        .ping()
        .await
        .expect("default-socket daemon should remain reachable");
}
