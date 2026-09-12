//! A departed startup observer must not decide the initialized Runtime's life.
//! Each case uses a private persistent daemon and the public Rust client.

use std::{
    collections::BTreeMap,
    os::unix::net::UnixStream,
    process::{Child, Command, Stdio},
    time::Duration,
};

use ctxmux_client::{Client, replay_bytes};
use ctxmux_protocol::{NativeOwnerStatus, RunSpec, TerminalSize};
use ctxmux_test_support::daemon_spawn_permit;
use rustix::process::{Pid, Signal, kill_process, kill_process_group, test_kill_process};
use tempfile::TempDir;
use tokio::time::{sleep, timeout};

// A fixture arrival deadline, not a product startup or capacity limit.
const ARRIVAL_BUDGET: Duration = Duration::from_secs(5);

struct PrivateDaemon {
    child: Child,
    directory: TempDir,
    run_pid: Option<u32>,
}

impl Drop for PrivateDaemon {
    fn drop(&mut self) {
        if let Some(raw) = self.run_pid {
            let pid = nix::unistd::Pid::from_raw(i32::try_from(raw).unwrap());
            if nix::unistd::getsid(Some(pid)) == Ok(pid) {
                let _ = kill_process_group(native_pid(raw), Signal::KILL);
            }
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn native_pid(raw: u32) -> Pid {
    Pid::from_raw(i32::try_from(raw).unwrap()).unwrap()
}

#[allow(
    clippy::too_many_lines,
    reason = "one fixture keeps private process ownership and cleanup visible"
)]
async fn prove_serving_without_observer(observer: Stdio, shape: &str) {
    let _permit = daemon_spawn_permit().await;
    let directory = tempfile::tempdir().expect("private Runtime directory");
    let socket = directory.path().join("ctxmux.sock");
    let child = Command::new("/bin/sh")
        .arg("-c")
        .arg("exec 3>&1; exec 1>/dev/null; exec \"$@\" --readiness-fd 3")
        .arg("ctxmux-readiness-observer-fixture")
        .arg(env!("CARGO_BIN_EXE_ctxmuxd"))
        .arg("--socket")
        .arg(&socket)
        .arg("--state-dir")
        .arg(directory.path().join("state"))
        .stdin(Stdio::null())
        .stdout(observer)
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn build-owned private daemon");
    let mut daemon = PrivateDaemon {
        child,
        directory,
        run_pid: None,
    };
    let client = Client::new(&socket);
    timeout(ARRIVAL_BUDGET, async {
        loop {
            assert!(
                daemon.child.try_wait().unwrap().is_none(),
                "departed readiness observer terminated the initialized Runtime"
            );
            if client.runtime_info().await.is_ok() {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("public Runtime service after departed observer");
    let identity = client
        .runtime_info()
        .await
        .expect("actual Runtime identity");
    let run = client
        .start(RunSpec {
            program: "/bin/sh".to_owned(),
            args: vec![
                "-c".to_owned(),
                "stty raw -echo; printf READY; exec /bin/cat".to_owned(),
            ],
            cwd: None,
            env: BTreeMap::new(),
            initial_size: TerminalSize::default(),
            declared_inputs: Vec::new(),
        })
        .await
        .expect("new real Native PTY after observer departure");
    let pid = run.pid.expect("native child PID");
    daemon.run_pid = Some(pid);
    assert!(matches!(
        run.native_service.as_ref().unwrap().owner,
        NativeOwnerStatus::Serving {}
    ));
    let bytes = format!("observer-gone:{shape}\n").into_bytes();
    // Wait for raw mode before input so the exact oracle cannot count PTY echo.
    let mut expected = b"READY".to_vec();
    exact_replay(&client, run.id, &expected).await;
    let result = client
        .input(run.id, bytes.clone())
        .await
        .expect("public Input");
    assert_eq!(
        result.receipt.written_bytes,
        u32::try_from(bytes.len()).unwrap()
    );
    expected.extend(&bytes);
    exact_replay(&client, run.id, &expected).await;
    let status = client.status(run.id).await.expect("same Run status");
    assert_eq!(status.id, run.id);
    assert_eq!(status.pid, Some(pid));
    assert!(matches!(
        status.native_service.unwrap().owner,
        NativeOwnerStatus::Serving {}
    ));
    assert_eq!(client.runtime_info().await.unwrap(), identity);
    let stop = client
        .prepare_stop(run.id)
        .await
        .expect("prepare public Stop");
    client.stop(stop).await.expect("public Stop reaps test Run");
    assert_eq!(
        test_kill_process(native_pid(pid)),
        Err(rustix::io::Errno::SRCH)
    );
    daemon.run_pid = None;
    kill_process(native_pid(daemon.child.id()), Signal::INT).expect("private daemon shutdown");
    timeout(ARRIVAL_BUDGET, async {
        loop {
            if let Some(status) = daemon.child.try_wait().unwrap() {
                assert!(status.success());
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("reap private daemon");
    let mut stderr = String::new();
    std::io::Read::read_to_string(&mut daemon.child.stderr.take().unwrap(), &mut stderr).unwrap();
    assert!(
        stderr.contains("readiness observer unavailable"),
        "{stderr}"
    );
    assert!(
        !socket.exists(),
        "normal shutdown removes the private socket"
    );
    assert!(daemon.directory.path().join("state").is_dir());
    println!(
        "readiness public proof: {shape}, Serving, exact input/output, Run Stop and daemon reap"
    );
}

async fn exact_replay(client: &Client, run_id: ctxmux_protocol::RunId, expected: &[u8]) {
    timeout(ARRIVAL_BUDGET, async {
        loop {
            let (attachment, snapshot) = client.attach(run_id, 0).await.expect("public Attach");
            let mut cursor = 0;
            for chunk in &snapshot.replay.chunks {
                assert_eq!(chunk.start_byte, cursor);
                cursor = chunk.end_byte;
            }
            assert_eq!(cursor, snapshot.replay.latest_output_bytes);
            assert_eq!(snapshot.replay.first_available_byte, 0);
            assert!(!snapshot.replay.truncated);
            let actual = replay_bytes(&snapshot.replay.chunks);
            attachment
                .detach()
                .await
                .expect("release observer without Stop");
            assert!(expected.starts_with(&actual), "extra or reordered output");
            if actual == expected {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("exact original bytes arrive");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closed_pipe_reader_preserves_runtime_and_new_pty() {
    let (reader, writer) = rustix::pipe::pipe().expect("private readiness pipe");
    drop(reader);
    timeout(
        ARRIVAL_BUDGET * 2,
        prove_serving_without_observer(Stdio::from(writer), "closed-pipe"),
    )
    .await
    .expect("bounded private pipe fixture");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closed_socket_peer_preserves_runtime_and_new_pty() {
    let (observer, writer) = UnixStream::pair().expect("private readiness socket pair");
    drop(observer);
    timeout(
        ARRIVAL_BUDGET * 2,
        prove_serving_without_observer(
            Stdio::from(std::os::fd::OwnedFd::from(writer)),
            "closed-socket",
        ),
    )
    .await
    .expect("bounded private socket fixture");
}
