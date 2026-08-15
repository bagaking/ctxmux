use std::{
    collections::BTreeMap,
    process::{Child, Command, Stdio},
    time::Duration,
};

use ctxmux_client::{Client, replay_bytes};
use ctxmux_protocol::{RunId, RunSpec, TerminalSize};
use tempfile::TempDir;
use tokio::time::{sleep, timeout};

struct TestDaemon {
    child: Child,
    _directory: TempDir,
    client: Client,
}

impl TestDaemon {
    async fn start_with_host_term(term: &str) -> Self {
        let directory = tempfile::tempdir().expect("create daemon temp directory");
        let socket = directory.path().join("ctxmux.sock");
        let child = Command::new(env!("CARGO_BIN_EXE_ctxmuxd"))
            .arg("--socket")
            .arg(&socket)
            .env("TERM", term)
            .env_remove("COLORTERM")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn ctxmuxd");
        let client = Client::new(&socket);
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
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
    }
}

fn env_probe_spec(env: BTreeMap<String, String>) -> RunSpec {
    RunSpec {
        program: "/bin/sh".to_owned(),
        args: vec![
            "-c".to_owned(),
            "printf 'TERM=%s\\nCOLORTERM=%s\\n' \"$TERM\" \"$COLORTERM\"".to_owned(),
        ],
        cwd: None,
        env,
        size: TerminalSize::default(),
        declared_inputs: Vec::new(),
    }
}

async fn wait_until_exited(client: &Client, id: RunId) {
    timeout(Duration::from_secs(5), async {
        loop {
            if !client
                .status(id)
                .await
                .expect("read Run state")
                .state
                .is_running()
            {
                return;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("Run should reach terminal state");
}

async fn attached_output(client: &Client, spec: RunSpec) -> String {
    let run = client.start(spec).await.expect("start env probe Run");
    wait_until_exited(client, run.id).await;
    let (_attachment, snapshot) = client
        .attach(run.id, 0)
        .await
        .expect("attach env probe Run");
    String::from_utf8_lossy(&replay_bytes(&snapshot.replay.chunks)).into_owned()
}

#[tokio::test]
async fn native_child_uses_stable_xterm_identity_instead_of_host_term() {
    let daemon = TestDaemon::start_with_host_term("unknown-test-term").await;
    let output = attached_output(&daemon.client, env_probe_spec(BTreeMap::new())).await;
    assert!(
        output.contains("TERM=xterm-256color") && output.contains("COLORTERM=truecolor"),
        "native child kept the daemon host TERM; output={output:?}"
    );
}

#[tokio::test]
async fn native_child_keeps_explicit_spec_term_identity() {
    let daemon = TestDaemon::start_with_host_term("unknown-test-term").await;
    let output = attached_output(
        &daemon.client,
        env_probe_spec(BTreeMap::from([
            ("TERM".to_owned(), "vt100".to_owned()),
            ("COLORTERM".to_owned(), "24bit".to_owned()),
        ])),
    )
    .await;
    assert!(
        output.contains("TERM=vt100") && output.contains("COLORTERM=24bit"),
        "explicit RunSpec.env did not win; output={output:?}"
    );
}
