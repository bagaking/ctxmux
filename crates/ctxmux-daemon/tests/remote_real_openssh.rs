//! The owner-host vertical carried by the maintained system OpenSSH client.
//!
//! This file is separate from `remote_owner_host_endpoint.rs` on purpose. That
//! file is required PR evidence and must contain no ignored test, or CI would
//! claim coverage for something it never ran. This lane cannot run without an
//! SSH boundary to an owner host, so it is `#[ignore]` and is driven by
//! `scripts/check-remote-runtime.sh --stage partition`, which fails rather than
//! skips when that boundary is absent.
//!
//! It proves the shipped transport rather than a stand-in: no `with_ssh_program`
//! override, so the real system `ssh` carries the protocol.

use std::{
    collections::BTreeMap,
    io::Write,
    process::{Command, Stdio},
    time::Duration,
};

use ctxmux_client::Client;
use ctxmux_protocol::{
    CreateOperationKey, ErrorCode, RunSpec, RunState, StopDisposition, TerminalSize,
};
use ctxmux_remote::{RemoteEndpoint, connect};
use ctxmux_test_support::scaled;
use tokio::time::{Instant, sleep};

/// Budget for a tunnel to become usable over a real network hop.
fn ready_budget() -> Duration {
    scaled(Duration::from_secs(20))
}

fn sleeper() -> RunSpec {
    RunSpec {
        program: "/bin/sh".to_owned(),
        args: vec!["-c".to_owned(), "printf ready; sleep 300".to_owned()],
        cwd: None,
        env: BTreeMap::new(),
        size: TerminalSize::default(),
        declared_inputs: Vec::new(),
    }
}

fn delayed_stop() -> RunSpec {
    RunSpec {
        program: "/bin/sh".to_owned(),
        args: vec![
            "-c".to_owned(),
            // The trap publishes a marker and holds the owner long enough for
            // the tunnel to be killed before the Stop receipt can be returned.
            "trap 'printf stopping; sleep 2; exit 0' TERM; while :; do sleep 300; done".to_owned(),
        ],
        cwd: None,
        env: BTreeMap::new(),
        size: TerminalSize::default(),
        declared_inputs: Vec::new(),
    }
}

fn outage_output() -> RunSpec {
    RunSpec {
        program: "/bin/sh".to_owned(),
        args: vec![
            "-c".to_owned(),
            // Keep the first cursor observable, then emit more than the
            // daemon's retained history while the caller has no tunnel.
            "stty -echo; printf first; sleep 1; head -c 5242880 /dev/zero; printf second; sleep 300"
                .to_owned(),
        ],
        cwd: None,
        env: BTreeMap::new(),
        size: TerminalSize::default(),
        declared_inputs: Vec::new(),
    }
}

fn operation_key(prefix: &str) -> CreateOperationKey {
    CreateOperationKey::new(format!("{prefix}-{}", std::process::id())).expect("operation key")
}

async fn stop_run(socket: &std::path::Path, run: ctxmux_protocol::RunId) {
    let client = Client::new(socket.to_path_buf());
    let operation = client
        .prepare_stop(run)
        .await
        .expect("prepare cleanup Stop operation");
    let receipt = client
        .stop(operation)
        .await
        .expect("cleanup Stop operation");
    assert_eq!(receipt.run.id, run, "cleanup Stop must name its own Run");
    let terminal = wait_for_terminal(socket, run).await;
    assert!(
        matches!(terminal.state, RunState::Exited { .. }),
        "cleanup Stop must leave the Run terminal"
    );
}

/// The same vertical, carried by the maintained system OpenSSH client.
///
/// This is the lane that proves the shipped transport rather than a stand-in. It
/// is `#[ignore]` because it needs an SSH boundary to an owner host, and it is
/// driven by `scripts/check-remote-runtime.sh --stage partition`, which fails
/// rather than skips when that boundary is absent.
#[tokio::test]
#[ignore = "requires an SSH boundary; run via scripts/check-remote-runtime.sh --stage partition"]
async fn real_openssh_carries_the_owner_host_vertical() {
    let destination = std::env::var("CTXMUX_REMOTE_SSH_DESTINATION").expect(
        "CTXMUX_REMOTE_SSH_DESTINATION must name an SSH destination whose owner host runs ctxmuxd",
    );
    let remote_socket = std::env::var("CTXMUX_REMOTE_SOCKET")
        .expect("CTXMUX_REMOTE_SOCKET must name the owner-host ctxmuxd socket");
    // No `with_ssh_program`: this lane runs the real system client.
    let endpoint = RemoteEndpoint::new(destination, &remote_socket)
        .expect("valid endpoint")
        .with_ready_timeout(ready_budget());
    let private = tempfile::tempdir().expect("private dir");

    let tunnel = connect(&endpoint, private.path().join("tunnel"))
        .await
        .expect("establish a tunnel with the system ssh client");
    let identity = Client::new(tunnel.socket_path().to_path_buf())
        .runtime_info()
        .await
        .expect("owner-host identity through real ssh");
    let run = Client::new(tunnel.socket_path().to_path_buf())
        .start_with_operation_key(sleeper(), operation_key("remote-real-ssh-run"))
        .await
        .expect("start a Run through real ssh");
    let pid = run.pid.expect("owner-host reports the child pid");

    // Lose the transport, then prove the owner kept the Run and its identity.
    tunnel.shutdown().await.expect("shutdown tunnel");

    let reconnected = connect(&endpoint, private.path().join("tunnel"))
        .await
        .expect("recreate the tunnel with the system ssh client");
    let client = Client::new(reconnected.socket_path().to_path_buf());

    assert_eq!(
        client
            .runtime_info()
            .await
            .expect("identity after reconnect"),
        identity,
        "reconnect must reach the same Runtime"
    );
    let recovered = client
        .status(run.id)
        .await
        .expect("the owner still knows the Run");
    assert!(
        matches!(recovered.state, RunState::Running),
        "transport loss must not publish a lifecycle transition, got {:?}",
        recovered.state
    );
    assert_eq!(
        recovered.pid,
        Some(pid),
        "the remote child must survive the transport"
    );

    // Observe replay through the public attachment API, from byte zero, so this
    // lane proves the ordered-byte contract over real ssh rather than only
    // proving that the child survived.
    let (_attachment, snapshot) = client
        .attach(run.id, 0)
        .await
        .expect("attach through the recreated real-ssh tunnel");
    let replayed: Vec<u8> = snapshot
        .replay
        .chunks
        .iter()
        .flat_map(|chunk| chunk.data.clone())
        .collect();
    assert!(
        String::from_utf8_lossy(&replayed).contains("ready") || snapshot.replay.truncated,
        "replay must return retained output or report truncation, got {:?}",
        String::from_utf8_lossy(&replayed)
    );

    stop_run(reconnected.socket_path(), run.id).await;
    reconnected.shutdown().await.expect("shutdown tunnel");
}

/// A Stop receipt is owner truth, not a local inference. Killing the real
/// OpenSSH tunnel while the owner is still settling leaves the caller with an
/// unknown response; retrying the exact operation later replays the owner's
/// disposition and the Run reaches the owner's terminal state.
#[tokio::test]
#[ignore = "requires an SSH boundary; run via scripts/check-remote-runtime.sh --stage partition"]
async fn real_openssh_stop_receipt_survives_tunnel_loss() {
    let destination = std::env::var("CTXMUX_REMOTE_SSH_DESTINATION")
        .expect("CTXMUX_REMOTE_SSH_DESTINATION must name an SSH destination");
    let remote_socket = std::env::var("CTXMUX_REMOTE_SOCKET")
        .expect("CTXMUX_REMOTE_SOCKET must name the owner-host ctxmuxd socket");
    let endpoint = RemoteEndpoint::new(destination.clone(), &remote_socket)
        .expect("valid endpoint")
        .with_ready_timeout(ready_budget());
    let private = tempfile::tempdir().expect("private dir");
    let tunnel = connect(&endpoint, private.path().join("stop-tunnel"))
        .await
        .expect("establish a real OpenSSH tunnel");
    let socket = tunnel.socket_path().to_path_buf();
    let client = Client::new(socket.clone());
    let run = client
        .start_with_operation_key(delayed_stop(), operation_key("remote-real-stop-run"))
        .await
        .expect("start the Stop fixture");
    let operation = client
        .prepare_stop(run.id)
        .await
        .expect("prepare the owner-bound Stop operation");

    let stop_client = Client::new(socket);
    let retry_operation = operation.clone();
    let stop_request = tokio::spawn(async move { stop_client.stop(retry_operation).await });
    wait_for_output(tunnel.socket_path(), run.id, b"stopping").await;
    // Exiting the local client makes the first response deliberately unknown;
    // killing the tunnel process group closes the transport as well. Neither
    // side gets to manufacture a receipt — only the retained owner operation
    // can settle it.
    stop_request.abort();
    let _ = stop_request.await;
    tunnel.shutdown().await.expect("kill the OpenSSH tunnel");

    let reconnected = connect(&endpoint, private.path().join("stop-tunnel"))
        .await
        .expect("reconnect to the same owner");
    let receipt = Client::new(reconnected.socket_path().to_path_buf())
        .stop(operation)
        .await
        .expect("exact retry must replay the owner receipt");
    assert_eq!(receipt.receipt.disposition, StopDisposition::Forced);
    let terminal = wait_for_terminal(reconnected.socket_path(), run.id).await;
    assert!(matches!(terminal.state, RunState::Exited { .. }));
    reconnected.shutdown().await.expect("shutdown tunnel");
}

/// Output history evicted while the caller is disconnected is reported as an
/// explicit truncation from the caller's cursor. The replay still contains the
/// retained tail and never invents bytes for the evicted prefix.
#[tokio::test]
#[ignore = "requires an SSH boundary; run via scripts/check-remote-runtime.sh --stage partition"]
async fn real_openssh_reports_truncation_after_outage_eviction() {
    let destination = std::env::var("CTXMUX_REMOTE_SSH_DESTINATION")
        .expect("CTXMUX_REMOTE_SSH_DESTINATION must name an SSH destination");
    let remote_socket = std::env::var("CTXMUX_REMOTE_SOCKET")
        .expect("CTXMUX_REMOTE_SOCKET must name the owner-host ctxmuxd socket");
    let endpoint = RemoteEndpoint::new(destination, &remote_socket)
        .expect("valid endpoint")
        .with_ready_timeout(ready_budget());
    let private = tempfile::tempdir().expect("private dir");
    let tunnel = connect(&endpoint, private.path().join("replay-tunnel"))
        .await
        .expect("establish a real OpenSSH tunnel");
    let socket = tunnel.socket_path().to_path_buf();
    let run = Client::new(socket.clone())
        .start_with_operation_key(outage_output(), operation_key("remote-real-truncation-run"))
        .await
        .expect("start the truncation fixture");
    let cursor = wait_for_output(&socket, run.id, b"first").await;
    tunnel.shutdown().await.expect("kill the OpenSSH tunnel");

    // The command's one-second barrier makes the bytes after `cursor` causal:
    // they are produced after the tunnel has been removed and while no caller
    // can observe the Run.
    sleep(Duration::from_secs(2)).await;
    let reconnected = connect(&endpoint, private.path().join("replay-tunnel"))
        .await
        .expect("reconnect to the same owner");
    let (_attachment, snapshot) = Client::new(reconnected.socket_path().to_path_buf())
        .attach(run.id, cursor)
        .await
        .expect("attach after the outage");
    assert!(
        snapshot.replay.truncated,
        "evicted history must be explicit"
    );
    assert!(
        snapshot.replay.first_available_byte > cursor,
        "the caller cursor must precede the retained tail"
    );
    let replayed: Vec<u8> = snapshot
        .replay
        .chunks
        .iter()
        .flat_map(|chunk| chunk.data.clone())
        .collect();
    assert!(
        String::from_utf8_lossy(&replayed).contains("second"),
        "retained tail must remain readable after truncation"
    );
    stop_run(reconnected.socket_path(), run.id).await;
    reconnected.shutdown().await.expect("shutdown tunnel");
}

/// The operation fence is tied to the daemon incarnation observed before the
/// tunnel break. A reconnect carrying a replaced incarnation is refused before
/// Run lookup or Stop admission.
#[tokio::test]
#[ignore = "requires an SSH boundary; run via scripts/check-remote-runtime.sh --stage partition"]
async fn real_openssh_rejects_a_replaced_daemon_instance() {
    let destination = std::env::var("CTXMUX_REMOTE_SSH_DESTINATION")
        .expect("CTXMUX_REMOTE_SSH_DESTINATION must name an SSH destination");
    let remote_socket = std::env::var("CTXMUX_REMOTE_SOCKET")
        .expect("CTXMUX_REMOTE_SOCKET must name the owner-host ctxmuxd socket");
    let daemon_binary = std::env::var("CTXMUX_REMOTE_DAEMON_BINARY").expect(
        "CTXMUX_REMOTE_DAEMON_BINARY must point to the compiled daemon binary provisioned on the owner host",
    );
    let endpoint = RemoteEndpoint::new(destination.clone(), &remote_socket)
        .expect("valid endpoint")
        .with_ready_timeout(ready_budget());
    let private = tempfile::tempdir().expect("private dir");
    let tunnel = connect(&endpoint, private.path().join("fence-tunnel"))
        .await
        .expect("establish a real OpenSSH tunnel");
    let client = Client::new(tunnel.socket_path().to_path_buf());
    let run = client
        .start_with_operation_key(sleeper(), operation_key("remote-real-fence-run"))
        .await
        .expect("start the fence fixture");
    let operation = client
        .prepare_stop(run.id)
        .await
        .expect("prepare the owner-bound Stop operation");
    tunnel.shutdown().await.expect("kill the OpenSSH tunnel");

    // Replace the actual owner process, keeping only the compiled binary on
    // the owner host. The new process gets a fresh daemon incarnation, so the
    // operation's original fence must fail before Run lookup or Stop admission.
    restart_remote_daemon(&destination, &daemon_binary, &remote_socket);
    let reconnected = connect(&endpoint, private.path().join("fence-tunnel"))
        .await
        .expect("reconnect to the owner socket");
    let error = Client::new(reconnected.socket_path().to_path_buf())
        .stop(operation)
        .await
        .expect_err("a replaced daemon incarnation must fail at its fence");
    assert!(
        matches!(
            &error,
            ctxmux_client::ClientError::ControlRejected { failure }
                if failure.error.code == ErrorCode::DaemonInstanceMismatch
        ),
        "unexpected replaced-incarnation error: {error:?}"
    );

    // The original Run belonged to the replaced memory-only daemon and is no
    // longer retained. Prove cleanup against the fresh incarnation with a new
    // owner-admitted Run and a normal public Stop operation.
    let fresh = Client::new(reconnected.socket_path().to_path_buf())
        .start_with_operation_key(sleeper(), operation_key("remote-real-fence-cleanup"))
        .await
        .expect("start a fresh Run on the replacement daemon");
    stop_run(reconnected.socket_path(), fresh.id).await;
    reconnected.shutdown().await.expect("shutdown tunnel");
}

/// Version skew is exercised with two compiled clients and two compiled
/// owner-host daemons. The current client is newer than the protocol-12 owner,
/// while the protocol-12 client is older than the current protocol-13 owner;
/// neither direction reaches a business frame. A real capability absence on
/// the current memory-only owner is rejected independently, before dispatch.
#[tokio::test]
#[ignore = "requires an SSH boundary and two distinct builds; run via scripts/check-remote-runtime.sh --stage partition"]
async fn real_openssh_rejects_bidirectional_build_skew() {
    let destination = std::env::var("CTXMUX_REMOTE_SSH_DESTINATION")
        .expect("CTXMUX_REMOTE_SSH_DESTINATION must name an SSH destination");
    let remote_socket = std::env::var("CTXMUX_REMOTE_SOCKET").expect(
        "CTXMUX_REMOTE_SOCKET must name a memory-only current owner-host socket (no --state-dir)",
    );
    let old_socket = std::env::var("CTXMUX_REMOTE_OLD_SOCKET")
        .expect("CTXMUX_REMOTE_OLD_SOCKET must name the protocol-12 owner socket");
    let old_client = std::env::var_os("CTXMUX_REMOTE_OLD_CLIENT")
        .expect("CTXMUX_REMOTE_OLD_CLIENT must point to the compiled protocol-12 client");
    let endpoint = |socket: String| {
        RemoteEndpoint::new(destination.clone(), socket)
            .expect("valid endpoint")
            .with_ready_timeout(ready_budget())
    };
    let private = tempfile::tempdir().expect("private dir");

    // New local (protocol 13) against old owner (protocol 12).
    let old_tunnel = connect(&endpoint(old_socket), private.path().join("old-tunnel"))
        .await
        .expect("establish the old-owner tunnel");
    let newer_error = Client::new(old_tunnel.socket_path().to_path_buf())
        .ping()
        .await
        .expect_err("newer client must reject an older protocol");
    assert!(matches!(
        newer_error,
        ctxmux_client::ClientError::Protocol {
            code: ErrorCode::VersionMismatch,
            ..
        }
    ));
    old_tunnel
        .shutdown()
        .await
        .expect("shutdown old-owner tunnel");

    // Old local (protocol 12) against current owner (protocol 13).
    let current_tunnel = connect(
        &endpoint(remote_socket),
        private.path().join("current-tunnel"),
    )
    .await
    .expect("establish the current-owner tunnel");
    let old_result = Command::new(old_client)
        .arg("--socket")
        .arg(current_tunnel.socket_path())
        .arg("runtime")
        .output()
        .expect("run the compiled old client");
    assert!(
        !old_result.status.success(),
        "old client must reject protocol 13"
    );
    let old_diagnostics = format!(
        "{}{}",
        String::from_utf8_lossy(&old_result.stdout),
        String::from_utf8_lossy(&old_result.stderr)
    );
    assert!(
        old_diagnostics.to_ascii_lowercase().contains("version")
            && old_diagnostics.to_ascii_lowercase().contains("mismatch"),
        "old client must report typed protocol skew, got {old_diagnostics:?}"
    );

    // Capability skew is a real owner advertisement, not a fabricated Hello.
    // This lane deliberately requires the current daemon to be memory-only:
    // only that mode omits the persistent-state capability.
    let capability_client = Client::new(current_tunnel.socket_path().to_path_buf())
        .with_required_capabilities(BTreeMap::from([(
            "services.persistent_state".to_owned(),
            1,
        )]))
        .expect("valid capability requirement");
    let capability_error = capability_client
        .list()
        .await
        .expect_err("absent owner capability must fail before dispatch");
    assert!(matches!(
        capability_error,
        ctxmux_client::ClientError::UnsupportedCapability {
            capability,
            required_version: 1,
            advertised_version: None,
        } if capability == "services.persistent_state"
    ));
    current_tunnel
        .shutdown()
        .await
        .expect("shutdown current-owner tunnel");
}

async fn wait_for_output(
    socket: &std::path::Path,
    run: ctxmux_protocol::RunId,
    needle: &[u8],
) -> u64 {
    let deadline = Instant::now() + scaled(Duration::from_secs(20));
    loop {
        let (_attachment, snapshot) = Client::new(socket.to_path_buf())
            .attach(run, 0)
            .await
            .expect("attach while waiting for output");
        let bytes: Vec<u8> = snapshot
            .replay
            .chunks
            .iter()
            .flat_map(|chunk| chunk.data.clone())
            .collect();
        if bytes.windows(needle.len()).any(|window| window == needle) {
            return snapshot.replay.latest_output_bytes;
        }
        assert!(Instant::now() < deadline, "expected output never arrived");
        sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_terminal(
    socket: &std::path::Path,
    run: ctxmux_protocol::RunId,
) -> ctxmux_protocol::RunInfo {
    let deadline = Instant::now() + scaled(Duration::from_secs(10));
    loop {
        let info = Client::new(socket.to_path_buf())
            .status(run)
            .await
            .expect("owner status");
        if !matches!(info.state, RunState::Running) {
            return info;
        }
        assert!(
            Instant::now() < deadline,
            "Stop never published terminal state"
        );
        sleep(Duration::from_millis(50)).await;
    }
}

/// Restart exactly the daemon command named by the fixture. This deliberately
/// avoids broad pattern kills: the fence test must replace the owner process,
/// not any unrelated daemon or SSH helper on the host.
fn restart_remote_daemon(destination: &str, binary: &str, socket: &str) {
    const SCRIPT: &str = r#"
set -eu
binary=$1
socket=$2
expected="$binary --socket $socket"
pids=$(ps -eo pid=,args= | awk -v expected="$expected" '
  {
    pid = $1
    sub(/^[^[:space:]]+[[:space:]]+/, "", $0)
    if ($0 == expected) print pid
  }
')
pid_count=$(printf '%s\n' "$pids" | awk 'NF { count++ } END { print count + 0 }')
if [ "$pid_count" -ne 1 ]; then
  echo "expected exactly one owner daemon ($expected), found $pid_count" >&2
  exit 1
fi
pid=$pids
kill -INT "$pid"
for _ in $(seq 1 100); do
  if ! kill -0 "$pid" 2>/dev/null; then
    break
  fi
  sleep 0.1
done
if kill -0 "$pid" 2>/dev/null; then
  echo "owner daemon $pid did not exit after SIGINT" >&2
  exit 1
fi
rm -f -- "$socket"
nohup "$binary" --socket "$socket" >/dev/null 2>&1 </dev/null &
new_pid=$!
for _ in $(seq 1 100); do
  if [ -S "$socket" ] && kill -0 "$new_pid" 2>/dev/null; then
    exit 0
  fi
  sleep 0.1
done
echo "replacement daemon did not become ready at $socket" >&2
kill "$new_pid" 2>/dev/null || true
exit 1
"#;

    let mut child = Command::new("ssh")
        .args([
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=10",
            destination,
            "sh",
            "-s",
            "--",
            binary,
            socket,
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn SSH owner-daemon restart helper");
    child
        .stdin
        .take()
        .expect("SSH restart helper stdin")
        .write_all(SCRIPT.as_bytes())
        .expect("write SSH restart helper");
    let output = child
        .wait_with_output()
        .expect("wait for SSH owner-daemon restart helper");
    assert!(
        output.status.success(),
        "owner-daemon replacement failed: stdout={:?}, stderr={:?}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
