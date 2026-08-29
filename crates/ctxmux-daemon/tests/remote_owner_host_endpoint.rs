//! Owner-host endpoint behavior against a real daemon through a real forwarder.
//!
//! These tests use an actual `ctxmuxd` process and an actual forwarding child
//! process. The forwarder is the test stand-in described in `bin/fake-ssh.rs`,
//! which speaks the same `-L` contract the production argument builder emits, so
//! the supervision contract is proven on machines without an SSH loopback. The
//! separate real-OpenSSH fixture owns the claim that the system client works.
//!
//! Every test here starts a real `ctxmuxd` plus a real forwarder, and cargo runs
//! test binaries in parallel. The shared spawn permit is process-local, so it
//! bounds this file but not the peak across binaries. These tests therefore run
//! one at a time — the whole file still finishes in about a second, and the lower
//! footprint keeps unrelated suites inside their readiness budgets.
//!
//! What is proven here is deliberately the part that is easy to get wrong:
//! readiness is observed rather than assumed, a Run reached through the tunnel
//! is the same Run the owner owns, losing the tunnel is not lifecycle truth, and
//! teardown leaves no socket, directory, or process behind.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::Duration,
};

use ctxmux_client::{Client, ClientError};
use ctxmux_protocol::{CreateOperationKey, RunSpec, RunState, TerminalSize};
use ctxmux_remote::{RemoteEndpoint, connect};
use ctxmux_test_support::{daemon_spawn_permit, scaled};
use tokio::time::{Instant, sleep};

/// Serializes this binary's tests.
///
/// Held for the whole test body, so at most one owner-host daemon and forwarder
/// exist at a time regardless of the harness thread count.
static ONE_AT_A_TIME: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Budget for an owner-host daemon or a tunnel to become usable.
fn ready_budget() -> Duration {
    scaled(Duration::from_secs(20))
}

/// Whether any process in `pid`'s group is still alive.
///
/// The tunnel's child is its own group leader, so signalling the group with
/// signal 0 asks the kernel about the forwarder and every helper it started
/// without delivering anything. `ESRCH` is the only answer that means the whole
/// group is gone; `EPERM` means it exists but is not ours to signal, which is
/// still alive for this purpose.
fn group_is_alive(pid: u32) -> bool {
    use rustix::process::{Pid, test_kill_process_group};

    let raw = i32::try_from(pid).expect("a pid fits in i32");
    let Some(pid) = Pid::from_raw(raw) else {
        return false;
    };
    match test_kill_process_group(pid) {
        Ok(()) => true,
        Err(errno) => errno != rustix::io::Errno::SRCH,
    }
}

/// Wait, bounded, for a tunnel's process group to disappear.
///
/// Teardown unlinks the forwarded socket, so an absent or refused path is
/// evidence the file is gone and says nothing about the process that held the
/// authenticated channel open. This observes the process itself. It polls
/// because reaping is asynchronous: a signalled group may outlive the call that
/// signalled it by a scheduling quantum.
async fn assert_group_dies(pid: u32, what: &str) {
    let deadline = Instant::now() + scaled(Duration::from_secs(10));
    while Instant::now() < deadline {
        if !group_is_alive(pid) {
            return;
        }
        sleep(Duration::from_millis(10)).await;
    }
    panic!("{what}: process group {pid} was still alive at the deadline");
}

/// One real owner-host daemon, standing in for a daemon on another machine.
struct OwnerHost {
    child: Child,
    socket: PathBuf,
    _dir: tempfile::TempDir,
}

impl OwnerHost {
    async fn start() -> Self {
        // Held across spawn and the readiness wait, so many owner hosts in this
        // binary do not all race their startup at once.
        let _permit = daemon_spawn_permit().await;
        let dir = tempfile::tempdir().expect("owner-host dir");
        let socket = dir.path().join("owner-host.sock");
        let child = Command::new(env!("CARGO_BIN_EXE_ctxmuxd"))
            .arg("--socket")
            .arg(&socket)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn owner-host ctxmuxd");
        let mut owner = Self {
            child,
            socket,
            _dir: dir,
        };
        owner.wait_ready().await;
        owner
    }

    async fn wait_ready(&mut self) {
        let deadline = Instant::now() + ready_budget();
        loop {
            if Client::new(self.socket.clone()).ping().await.is_ok() {
                return;
            }
            if let Some(status) = self.child.try_wait().expect("poll owner-host") {
                panic!("owner-host ctxmuxd exited before readiness: {status}");
            }
            assert!(
                Instant::now() < deadline,
                "owner-host ctxmuxd did not become ready"
            );
            sleep(Duration::from_millis(20)).await;
        }
    }
}

impl Drop for OwnerHost {
    fn drop(&mut self) {
        if self.child.try_wait().expect("poll owner-host").is_none() {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
    }
}

fn endpoint_for(owner: &OwnerHost) -> RemoteEndpoint {
    RemoteEndpoint::new("owner-host.test", &owner.socket)
        .expect("valid endpoint")
        .with_ssh_program(env!("CARGO_BIN_EXE_fake-ssh"))
        .with_ready_timeout(ready_budget())
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

/// The forwarded socket reaches the owner-host daemon, and the Run observed
/// through the tunnel is the owner's Run rather than a local one.
#[tokio::test]
async fn forwarded_socket_reaches_the_owner_host_runtime() {
    let _serial = ONE_AT_A_TIME.lock().await;
    let owner = OwnerHost::start().await;
    let private = tempfile::tempdir().expect("private dir");
    let tunnel = connect(&endpoint_for(&owner), private.path().join("tunnel"))
        .await
        .expect("establish tunnel");

    let direct = Client::new(owner.socket.clone())
        .runtime_info()
        .await
        .expect("owner-host identity");
    let through_tunnel = Client::new(tunnel.socket_path().to_path_buf())
        .runtime_info()
        .await
        .expect("tunnelled identity");

    assert_eq!(
        direct, through_tunnel,
        "the tunnel must reach the same Runtime, not a different endpoint"
    );

    let run = Client::new(tunnel.socket_path().to_path_buf())
        .start_with_operation_key(
            sleeper(),
            CreateOperationKey::new("remote-vertical-start").expect("key"),
        )
        .await
        .expect("start a Run through the tunnel");

    let from_owner = Client::new(owner.socket.clone())
        .status(run.id)
        .await
        .expect("owner-host sees the same Run");
    assert_eq!(
        from_owner.id, run.id,
        "the owner must own the Run we created"
    );
    assert!(
        from_owner.pid.is_some(),
        "the owner-host daemon owns the child process"
    );

    tunnel.shutdown().await.expect("shutdown tunnel");
}

/// Losing the tunnel is a reachability fact. The remote child keeps running and
/// no lifecycle transition is published, which is the property a consumer relies
/// on to avoid treating a network blip as an exit.
#[tokio::test]
async fn losing_the_tunnel_is_not_lifecycle_truth() {
    let _serial = ONE_AT_A_TIME.lock().await;
    let owner = OwnerHost::start().await;
    let private = tempfile::tempdir().expect("private dir");
    let endpoint = endpoint_for(&owner);

    let tunnel = connect(&endpoint, private.path().join("tunnel"))
        .await
        .expect("establish tunnel");
    let socket_path = tunnel.socket_path().to_path_buf();
    let run = Client::new(socket_path.clone())
        .start_with_operation_key(
            sleeper(),
            CreateOperationKey::new("remote-partition-run").expect("key"),
        )
        .await
        .expect("start a Run through the tunnel");
    let pid = run.pid.expect("owner-host reports the child pid");

    tunnel.shutdown().await.expect("shutdown tunnel");

    // The local transport is gone, so this client can no longer observe the
    // owner at all.
    assert!(
        Client::new(socket_path.clone()).ping().await.is_err(),
        "a removed tunnel socket must not still answer"
    );

    // The owner, however, still owns a running Run with the same pid. Nothing
    // synthesized an exit or interruption while we were disconnected.
    let after = Client::new(owner.socket.clone())
        .status(run.id)
        .await
        .expect("owner-host still knows the Run");
    assert!(
        matches!(after.state, RunState::Running),
        "transport loss must not publish a lifecycle transition, got {:?}",
        after.state
    );
    assert_eq!(
        after.pid,
        Some(pid),
        "the remote child must be the same process"
    );

    // A recreated tunnel reaches the same Runtime and the same Run.
    let reconnected = connect(&endpoint, private.path().join("tunnel"))
        .await
        .expect("recreate tunnel");
    let recovered = Client::new(reconnected.socket_path().to_path_buf())
        .status(run.id)
        .await
        .expect("reattach by exact Run identity");
    assert_eq!(recovered.id, run.id);
    assert_eq!(
        recovered.pid,
        Some(pid),
        "reconnect must find the same remote child, not a replacement"
    );
    reconnected.shutdown().await.expect("shutdown tunnel");
}

/// Output produced while disconnected is recoverable from the caller's own byte
/// cursor, because remote reuses the local replay contract rather than adding a
/// second one.
///
/// The second burst is triggered by input rather than by a timer, so "written
/// while disconnected" is caused rather than hoped for: the Run cannot emit it
/// until the owner is asked to, which happens only after this client has already
/// torn its tunnel down.
#[tokio::test]
async fn output_written_while_disconnected_replays_from_the_caller_cursor() {
    let _serial = ONE_AT_A_TIME.lock().await;
    let owner = OwnerHost::start().await;
    let private = tempfile::tempdir().expect("private dir");
    let endpoint = endpoint_for(&owner);

    let tunnel = connect(&endpoint, private.path().join("tunnel"))
        .await
        .expect("establish tunnel");
    let run = Client::new(tunnel.socket_path().to_path_buf())
        .start_with_operation_key(
            RunSpec {
                program: "/bin/sh".to_owned(),
                args: vec![
                    "-c".to_owned(),
                    // Echo is off, so the only bytes on this PTY are the ones the
                    // script prints. `read` blocks until input arrives.
                    "stty -echo; printf first; read line; printf second; sleep 300".to_owned(),
                ],
                cwd: None,
                env: BTreeMap::new(),
                size: TerminalSize::default(),
                declared_inputs: Vec::new(),
            },
            CreateOperationKey::new("remote-replay-run").expect("key"),
        )
        .await
        .expect("start a Run through the tunnel");

    // Observe the first burst and remember exactly where we stopped reading.
    let cursor = wait_for_output(tunnel.socket_path(), run.id, b"first").await;
    tunnel.shutdown().await.expect("shutdown tunnel");

    // With our transport gone, ask the owner directly to release the second
    // burst. Any bytes after `cursor` were therefore produced while this client
    // could not observe the Run at all.
    Client::new(owner.socket.clone())
        .input(run.id, b"go\n".to_vec())
        .await
        .expect("owner applies input while the tunnel is down");
    wait_for_output(&owner.socket, run.id, b"second").await;

    let reconnected = connect(&endpoint, private.path().join("tunnel"))
        .await
        .expect("recreate tunnel");
    let (_attachment, snapshot) = Client::new(reconnected.socket_path().to_path_buf())
        .attach(run.id, cursor)
        .await
        .expect("replay from our own cursor");
    let replayed: Vec<u8> = snapshot
        .replay
        .chunks
        .iter()
        .flat_map(|chunk| chunk.data.clone())
        .collect();

    assert!(
        !snapshot.replay.truncated,
        "retained history must not report truncation here"
    );
    assert!(
        String::from_utf8_lossy(&replayed).contains("second"),
        "output written while disconnected must replay, got {:?}",
        String::from_utf8_lossy(&replayed)
    );
    reconnected.shutdown().await.expect("shutdown tunnel");
}

/// A missing owner-host listener fails closed on first use, and nothing is
/// provisioned to make it succeed.
///
/// This asserts the failure point the shipped transport actually has rather than
/// a stricter one. `ExitOnForwardFailure` fires when `ssh` cannot establish the
/// forward, and for `-L` that is the *local* bind: a `StreamLocal` forward whose
/// remote socket has no listener still binds locally and stays up, so readiness
/// by connect is satisfied and the absence surfaces on the connection that would
/// carry the business frame. What a consumer relies on is that it never gets a
/// working Runtime out of a dead owner and that no daemon is started for it —
/// not that the refusal arrives at establishment.
#[tokio::test]
async fn a_missing_owner_host_listener_fails_closed() {
    let _serial = ONE_AT_A_TIME.lock().await;
    let absent = tempfile::tempdir().expect("dir");
    let missing = absent.path().join("nothing.sock");
    let endpoint = RemoteEndpoint::new("owner-host.test", &missing)
        .expect("valid endpoint")
        .with_ssh_program(env!("CARGO_BIN_EXE_fake-ssh"))
        .with_ready_timeout(ready_budget());
    let private = tempfile::tempdir().expect("private dir");

    let tunnel = connect(&endpoint, private.path().join("tunnels"))
        .await
        .expect("a local forward binds even when the owner is absent");

    // The first request is refused, and it is refused for reachability rather
    // than by a daemon that answered.
    let error = Client::new(tunnel.socket_path().to_path_buf())
        .runtime_info()
        .await
        .expect_err("a missing owner-host listener must fail");
    assert!(
        matches!(
            error,
            ClientError::Connect { .. } | ClientError::Closed | ClientError::Transport(_)
        ),
        "expected a reachability failure, got {error:?}"
    );

    // Nothing was provisioned to paper over the absence: no listener appeared at
    // the owner-host path, and a retry fails the same way rather than succeeding
    // against something this call started.
    assert!(
        !missing.exists(),
        "no owner-host listener may be created on the caller's behalf"
    );
    assert!(
        Client::new(tunnel.socket_path().to_path_buf())
            .runtime_info()
            .await
            .is_err(),
        "a retry must keep failing while the owner is absent"
    );

    tunnel.shutdown().await.expect("shutdown tunnel");
}

/// Teardown removes the forwarded socket, its own private directory, and the
/// forwarding process. A leaked forwarder would hold an authenticated channel
/// open after the caller believed it was closed.
///
/// The process half is asserted by watching the forwarder's own group die, not
/// by re-reading the socket path: teardown unlinks that path, so its absence is
/// satisfied by the unlink alone and would stay satisfied by a teardown that
/// never signalled anything.
#[tokio::test]
async fn shutdown_removes_the_socket_directory_and_process() {
    let _serial = ONE_AT_A_TIME.lock().await;
    let owner = OwnerHost::start().await;
    let private = tempfile::tempdir().expect("private dir");

    let mut tunnel = connect(&endpoint_for(&owner), private.path().join("tunnels"))
        .await
        .expect("establish tunnel");
    let socket_path = tunnel.socket_path().to_path_buf();
    // Each tunnel owns a directory beneath the caller's; that directory, not the
    // caller's, is what teardown must remove.
    let tunnel_dir = socket_path
        .parent()
        .expect("the socket lives in the tunnel directory")
        .to_path_buf();
    // Captured while the tunnel is live, because a reaped child no longer
    // reports one and the assertion below is about this exact group.
    let leader = tunnel
        .leader_pid()
        .expect("a live tunnel has a forwarding process");

    assert!(
        socket_path.exists(),
        "forwarded socket must exist while live"
    );
    assert!(
        tunnel.is_connected().expect("observe tunnel"),
        "tunnel must report connected while live"
    );
    assert!(
        group_is_alive(leader),
        "the forwarder's group must be alive while the tunnel is live"
    );

    tunnel.shutdown().await.expect("shutdown tunnel");

    assert_group_dies(leader, "shutdown").await;
    assert!(
        !socket_path.exists(),
        "forwarded socket must be removed on shutdown"
    );
    assert!(
        !tunnel_dir.exists(),
        "the tunnel's private directory must be removed on shutdown"
    );
    // The caller's directory is theirs, so teardown must leave it alone.
    assert!(
        private.path().join("tunnels").exists(),
        "teardown must not remove the caller's directory"
    );
}

/// Dropping the guard without an explicit shutdown must not leak the socket, the
/// tunnel's directory, or the forwarding process.
///
/// Process death is asserted by watching the forwarder's own group disappear.
/// Reading the socket path cannot carry that claim: `Drop` unlinks the path, so
/// a connection there is refused with `ENOENT` whether or not the process
/// holding the authenticated channel is still running.
#[tokio::test]
async fn dropping_the_guard_cleans_up() {
    let _serial = ONE_AT_A_TIME.lock().await;
    let owner = OwnerHost::start().await;
    let private = tempfile::tempdir().expect("private dir");
    let (socket_path, tunnel_dir, leader) = {
        let tunnel = connect(&endpoint_for(&owner), private.path().join("tunnels"))
            .await
            .expect("establish tunnel");
        let socket_path = tunnel.socket_path().to_path_buf();
        let tunnel_dir = socket_path
            .parent()
            .expect("the socket lives in the tunnel directory")
            .to_path_buf();
        let leader = tunnel
            .leader_pid()
            .expect("a live tunnel has a forwarding process");
        assert!(
            tokio::net::UnixStream::connect(&socket_path).await.is_ok(),
            "the forwarded socket must be usable while the guard is held"
        );
        (socket_path, tunnel_dir, leader)
    };
    assert_group_dies(leader, "drop").await;
    assert!(
        !socket_path.exists(),
        "a dropped tunnel must not leave its socket behind"
    );
    assert!(
        !tunnel_dir.exists(),
        "a dropped tunnel must not leave its private directory behind"
    );
}

async fn wait_for_output(socket: &Path, run: ctxmux_protocol::RunId, needle: &[u8]) -> u64 {
    let deadline = Instant::now() + scaled(Duration::from_secs(10));
    loop {
        let (_attachment, snapshot) = Client::new(socket.to_path_buf())
            .attach(run, 0)
            .await
            .expect("attach for output");
        let bytes: Vec<u8> = snapshot
            .replay
            .chunks
            .iter()
            .flat_map(|chunk| chunk.data.clone())
            .collect();
        if bytes.windows(needle.len()).any(|window| window == needle) {
            return snapshot.replay.latest_output_bytes;
        }
        assert!(
            Instant::now() < deadline,
            "expected output {:?} never arrived",
            String::from_utf8_lossy(needle)
        );
        sleep(Duration::from_millis(50)).await;
    }
}

/// Selecting an owner-host Runtime through a tunnel is fail-closed on identity.
///
/// The tunnel cannot attest the daemon behind it, so a caller pins the exact
/// Runtime identity it expects and the ordinary client compares it against Hello
/// on the same connection that would carry the business frame. This is the
/// property a consumer relies on to know a reconnect landed on the same remote
/// daemon rather than on whatever now answers at that destination.
#[tokio::test]
async fn a_tunnel_to_another_runtime_fails_closed_before_dispatch() {
    let _serial = ONE_AT_A_TIME.lock().await;
    let intended = OwnerHost::start().await;
    let other = OwnerHost::start().await;
    let private = tempfile::tempdir().expect("private dir");

    // Retain the identity of the Runtime we meant to reach.
    let expected = Client::new(intended.socket.clone())
        .runtime_info()
        .await
        .expect("intended owner-host identity");

    // Then forward to a different owner host, as a reused destination or a
    // replaced daemon would.
    let endpoint = RemoteEndpoint::new("owner-host.test", &other.socket)
        .expect("valid endpoint")
        .with_ssh_program(env!("CARGO_BIN_EXE_fake-ssh"))
        .with_ready_timeout(ready_budget());
    let tunnel = connect(&endpoint, private.path().join("tunnels"))
        .await
        .expect("the tunnel itself establishes; identity is the client's job");

    let error = Client::new(tunnel.socket_path().to_path_buf())
        .with_expected_runtime_identity(expected)
        .list()
        .await
        .expect_err("a different Runtime must be refused");
    assert!(
        matches!(error, ClientError::RuntimeIdentityMismatch { .. }),
        "expected a typed identity mismatch, got {error:?}"
    );

    // The intended owner is untouched: refusing to dispatch is not an action
    // against either daemon.
    assert!(
        Client::new(intended.socket.clone())
            .runtime_info()
            .await
            .is_ok(),
        "the intended owner-host must remain reachable"
    );

    tunnel.shutdown().await.expect("shutdown tunnel");
}
