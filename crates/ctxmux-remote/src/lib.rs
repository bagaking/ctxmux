//! Owner-host endpoint for the ctxmux local protocol.
//!
//! A Run owned by a remote `ctxmuxd` should stay durable when the local client
//! or its transport disappears. This crate carries the *existing* protocol to
//! that owner instead of inventing a second one: the maintained system OpenSSH
//! client maps the owner-host daemon socket to one caller-private local socket
//! through `StreamLocal` forwarding, and the ordinary local client speaks the
//! unchanged protocol over it.
//!
//! # What this crate deliberately does not own
//!
//! - **The wire.** No frame, request, event, or protocol generation is added
//!   here. This crate never parses protocol bytes; it only produces a socket
//!   path that the ordinary client consumes.
//! - **Identity.** Proving that a reconnect reached the same Runtime is the
//!   client's existing exact-identity comparison against Hello on the dispatch
//!   connection. A tunnel cannot attest a daemon, so it does not try.
//! - **Credentials and host trust.** OpenSSH owns authentication, host-key
//!   policy, `~/.ssh/config`, `ProxyJump`, and agent forwarding. Caller
//!   arguments are passed through so those keep working. This crate never
//!   reads, copies, prompts for, stores, or logs a credential.
//! - **Lifecycle truth.** Losing the tunnel means this client cannot currently
//!   observe the owner. It never means the Run exited. Reachability is a local
//!   fact; only the owner-host daemon publishes lifecycle truth.
//! - **Provisioning.** Nothing is uploaded, installed, version-matched, or
//!   spawned on the owner host. A missing owner-host listener is an explicit
//!   error, never an invitation to provision one.
//!
//! Because remote reuses the same socket contract, the ordered-byte cursor,
//! replay, truncation, and gap semantics are the local ones verbatim. There is
//! no second recovery state machine to keep in agreement.

#[cfg(not(unix))]
compile_error!("the ctxmux remote endpoint currently requires Unix sockets");

use std::{
    ffi::{OsStr, OsString},
    fs, io,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use thiserror::Error;
use tokio::{net::UnixStream, process::Child, time::Instant};

/// Default ceiling for observing the forwarded socket accept a connection.
pub const DEFAULT_READY_TIMEOUT: Duration = Duration::from_secs(30);

/// Interval between forwarded-socket readiness probes.
const READY_POLL: Duration = Duration::from_millis(50);

/// Bound on how long a terminating tunnel is given to exit before it is killed.
const SHUTDOWN_GRACE: Duration = Duration::from_millis(500);

/// Why establishing or holding an owner-host endpoint failed.
#[derive(Debug, Error)]
pub enum RemoteEndpointError {
    /// The caller-supplied destination could not be used as an SSH destination.
    ///
    /// A destination that begins with `-` would be parsed by `ssh` as an option
    /// rather than a host, so it is rejected instead of being forwarded into an
    /// argument list.
    #[error("invalid ssh destination: {reason}")]
    InvalidDestination {
        /// Why the destination was refused.
        reason: String,
    },
    /// The caller-supplied owner-host socket path could not be used.
    #[error("invalid owner-host socket path: {reason}")]
    InvalidRemoteSocket {
        /// Why the remote socket path was refused.
        reason: String,
    },
    /// The private local directory or socket path could not be prepared.
    #[error("failed to prepare local tunnel directory {path}: {source}")]
    PrepareLocalSocket {
        /// Directory that could not be prepared.
        path: PathBuf,
        /// Underlying filesystem error.
        #[source]
        source: io::Error,
    },
    /// The system OpenSSH client could not be started at all.
    #[error("failed to start the system ssh client ({program}): {source}")]
    SpawnSsh {
        /// Program that could not be executed.
        program: String,
        /// Underlying spawn error.
        #[source]
        source: io::Error,
    },
    /// `ssh` exited before the forwarded socket became usable.
    ///
    /// This covers host-key refusal, authentication failure, a missing
    /// owner-host listener, and `ExitOnForwardFailure` refusing a collision.
    /// The exact cause belongs to ssh's own diagnostics, which are left on the
    /// caller's stderr rather than being re-interpreted here.
    #[error("the ssh tunnel to {destination} exited before forwarding was usable{status}")]
    TunnelExited {
        /// Destination that was requested.
        destination: String,
        /// Rendered exit status, when one was observed.
        status: String,
    },
    /// The forwarded socket never accepted a connection within the deadline.
    #[error(
        "the forwarded socket {path} did not accept a connection within {}s",
        timeout.as_secs()
    )]
    ReadyTimeout {
        /// Local socket that never became usable.
        path: PathBuf,
        /// Deadline that elapsed.
        timeout: Duration,
    },
    /// Observing the tunnel process itself failed.
    #[error("failed to observe the ssh tunnel: {source}")]
    ObserveTunnel {
        /// Underlying error.
        #[source]
        source: io::Error,
    },
}

/// Caller-owned description of one owner-host endpoint.
///
/// The destination and extra arguments are handed to the system `ssh` client
/// unchanged, so an alias, `ProxyJump`, `IdentityFile`, or agent configuration
/// the caller already has in `~/.ssh/config` keeps working.
#[derive(Debug, Clone)]
pub struct RemoteEndpoint {
    destination: OsString,
    remote_socket: PathBuf,
    ssh_program: OsString,
    extra_args: Vec<OsString>,
    ready_timeout: Duration,
}

impl RemoteEndpoint {
    /// Describe an owner-host daemon socket reachable through one SSH
    /// destination.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteEndpointError::InvalidDestination`] when the destination
    /// is empty or would be parsed as an `ssh` option, and
    /// [`RemoteEndpointError::InvalidRemoteSocket`] when the owner-host socket
    /// path is empty, relative, or contains a colon. A colon cannot be escaped
    /// inside an `ssh -L` specification, so such a path is refused rather than
    /// silently forwarded to the wrong target.
    pub fn new(
        destination: impl Into<OsString>,
        remote_socket: impl Into<PathBuf>,
    ) -> Result<Self, RemoteEndpointError> {
        let destination = destination.into();
        let remote_socket = remote_socket.into();
        validate_destination(&destination)?;
        validate_remote_socket(&remote_socket)?;
        Ok(Self {
            destination,
            remote_socket,
            ssh_program: OsString::from("ssh"),
            extra_args: Vec::new(),
            ready_timeout: DEFAULT_READY_TIMEOUT,
        })
    }

    /// Override the `ssh` executable.
    ///
    /// This exists so a test can substitute a forwarder that speaks the same
    /// `-L <local>:<remote> -N <destination>` contract. It is not a place to
    /// bundle or prefer a private SSH implementation.
    #[must_use]
    pub fn with_ssh_program(mut self, program: impl Into<OsString>) -> Self {
        self.ssh_program = program.into();
        self
    }

    /// Append caller-owned `ssh` arguments.
    ///
    /// These are forwarded verbatim and are the caller's supported way to pass
    /// `-J`, `-i`, `-p`, or any other option their configuration needs.
    #[must_use]
    pub fn with_extra_args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<OsString>,
    {
        self.extra_args.extend(args.into_iter().map(Into::into));
        self
    }

    /// Bound how long the forwarded socket may take to accept a connection.
    #[must_use]
    pub fn with_ready_timeout(mut self, timeout: Duration) -> Self {
        self.ready_timeout = timeout;
        self
    }
}

/// A live forwarded endpoint holding one supervised `ssh` process.
///
/// The forwarded socket path is the only thing a protocol client needs: pass it
/// to the ordinary local client, which then performs its usual exact-identity
/// and capability checks before any business frame.
///
/// Dropping this guard tears the tunnel down and removes its private directory.
/// The owner-host daemon and its Runs are unaffected: closing a client
/// transport removes one attachment and never stops a Run.
#[derive(Debug)]
pub struct RemoteTunnel {
    socket_path: PathBuf,
    private_dir: PathBuf,
    child: Option<Child>,
}

impl RemoteTunnel {
    /// Local socket that now forwards to the owner-host daemon.
    #[must_use]
    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    /// Process id of the tunnel's group leader, while it is still running.
    ///
    /// `None` once the process has been reaped. This exists so a caller can
    /// observe the forwarder's death directly instead of inferring it from the
    /// socket path: teardown unlinks the socket, so an absent or refused path
    /// says the file is gone and nothing about whether the process that held
    /// the authenticated channel open is gone with it.
    ///
    /// The value is the group id as well, because the child is spawned as its
    /// own group leader.
    #[must_use]
    pub fn leader_pid(&self) -> Option<u32> {
        self.child.as_ref().and_then(Child::id)
    }

    /// Whether the supervised tunnel process is still running.
    ///
    /// A `false` result means this client can no longer observe the owner-host
    /// daemon. It is explicitly **not** evidence that a remote Run exited,
    /// was interrupted, or changed state in any way.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteEndpointError::ObserveTunnel`] when the tunnel process
    /// status cannot be observed, so an unobservable tunnel is never reported
    /// as a healthy one.
    pub fn is_connected(&mut self) -> Result<bool, RemoteEndpointError> {
        let Some(child) = self.child.as_mut() else {
            return Ok(false);
        };
        match child.try_wait() {
            Ok(None) => Ok(true),
            Ok(Some(_)) => Ok(false),
            Err(source) => Err(RemoteEndpointError::ObserveTunnel { source }),
        }
    }

    /// Terminate the tunnel and remove its private directory.
    ///
    /// Shutdown is bounded: the process is asked to exit, then killed if it
    /// outlasts a short grace period, so a wedged tunnel cannot hold the caller
    /// forever.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteEndpointError::ObserveTunnel`] when the terminating
    /// process cannot be reaped. The private directory is removed regardless.
    pub async fn shutdown(mut self) -> Result<(), RemoteEndpointError> {
        let result = self.terminate().await;
        self.cleanup_private_dir();
        result
    }

    async fn terminate(&mut self) -> Result<(), RemoteEndpointError> {
        let Some(mut child) = self.child.take() else {
            return Ok(());
        };
        // A forwarding-only ssh holds no work worth draining, so ask the whole
        // group to exit immediately and only then wait, bounded, for the reap.
        kill_tunnel_group(&child);
        let _ = child.start_kill();
        match tokio::time::timeout(SHUTDOWN_GRACE, child.wait()).await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(source)) => Err(RemoteEndpointError::ObserveTunnel { source }),
            Err(_) => match child.wait().await {
                Ok(_) => Ok(()),
                Err(source) => Err(RemoteEndpointError::ObserveTunnel { source }),
            },
        }
    }

    fn cleanup_private_dir(&self) {
        // Remove the socket first so a client cannot connect to a path whose
        // forwarder is already gone, then drop the now-empty private directory.
        let _ = fs::remove_file(&self.socket_path);
        let _ = fs::remove_dir(&self.private_dir);
    }
}

impl Drop for RemoteTunnel {
    fn drop(&mut self) {
        if let Some(child) = self.child.as_mut() {
            // A dropped guard must not leak a forwarding process or the helpers
            // it started. There is no async context here, so this is the one
            // place that cannot wait for the reap; `kill_on_drop` covers that.
            kill_tunnel_group(child);
            let _ = child.start_kill();
        }
        self.cleanup_private_dir();
    }
}

/// Signal the tunnel's whole process group.
///
/// `ssh` is spawned as its own group leader, so this reaches the helpers a
/// caller's configuration may add — a `ControlMaster` mux, a `ProxyCommand`
/// child — which a signal to the direct child alone would leave running with an
/// authenticated channel open. A missing group means the child is already
/// reaped, which is not an error.
fn kill_tunnel_group(child: &Child) {
    use rustix::process::{Pid, Signal, kill_process_group};

    let Some(raw) = child.id() else {
        return;
    };
    let Ok(raw) = i32::try_from(raw) else {
        return;
    };
    if let Some(pid) = Pid::from_raw(raw) {
        // The group id equals the leader's pid because of `process_group(0)`.
        let _ = kill_process_group(pid, Signal::KILL);
    }
}

/// Establish one forwarded endpoint inside a caller-owned private directory.
///
/// `private_dir` must be a directory this caller controls; the forwarded socket
/// is created inside it and both are removed when the returned guard is dropped
/// or shut down. The directory is created owner-only when it does not exist.
///
/// Readiness is proven by connecting to the forwarded socket, not by a delay:
/// the function returns only after the socket actually accepts a connection, or
/// fails explicitly. It never returns a path that merely looks plausible.
///
/// # Errors
///
/// Returns [`RemoteEndpointError`] when the private directory cannot be
/// prepared, the system `ssh` client cannot be started, `ssh` exits before
/// forwarding works, or the forwarded socket never accepts a connection within
/// the endpoint's ready timeout.
pub async fn connect(
    endpoint: &RemoteEndpoint,
    private_dir: impl AsRef<Path>,
) -> Result<RemoteTunnel, RemoteEndpointError> {
    let parent = private_dir.as_ref().to_path_buf();
    // The caller owns `parent`; each tunnel gets its own owner-only directory
    // beneath it. That keeps two tunnels from sharing a socket path — there is
    // no lock, so sharing one would have them silently clobber each other — and
    // means the directory is always one this call created rather than one it
    // adopted and tightened.
    let private_dir = create_tunnel_dir(&parent)?;
    let socket_path = private_dir.join("owner-host.sock");
    // A colon cannot be escaped inside an `ssh -L` specification. The remote half
    // is validated at construction; the local half is derived from a
    // caller-supplied directory, so it is checked here for the same reason.
    if socket_path.to_string_lossy().contains(':') {
        let _ = fs::remove_dir(&private_dir);
        return Err(RemoteEndpointError::PrepareLocalSocket {
            path: private_dir,
            source: io::Error::new(
                io::ErrorKind::InvalidInput,
                "local tunnel directory must not contain ':'",
            ),
        });
    }

    let child = spawn_tunnel(endpoint, &socket_path)?;
    let mut tunnel = RemoteTunnel {
        socket_path,
        private_dir,
        child: Some(child),
    };
    await_ready(&mut tunnel, endpoint).await?;
    Ok(tunnel)
}

/// Create one fresh owner-only directory for a single tunnel.
///
/// Both levels are created owner-only from the instant they exist. A plain
/// `create_dir` applies the process umask, which typically yields `0o755`, and
/// tightening afterwards both leaves a window and follows a symlink an attacker
/// could have planted in a writable parent. `tempfile::tempdir_in` has the same
/// umask behavior, so the directory is created here instead.
///
/// Uniqueness comes from `DirBuilder::create` being exclusive: a name that loses
/// a race fails with `AlreadyExists` and the next candidate is tried. Names are
/// sequential rather than random because the exclusive create, not the name, is
/// what makes this safe.
fn create_tunnel_dir(parent: &Path) -> Result<PathBuf, RemoteEndpointError> {
    use std::os::unix::fs::DirBuilderExt;

    let map_error = |source: io::Error| RemoteEndpointError::PrepareLocalSocket {
        path: parent.to_path_buf(),
        source,
    };
    let owner_only = || {
        let mut builder = fs::DirBuilder::new();
        builder.mode(0o700);
        builder
    };
    owner_only()
        .recursive(true)
        .create(parent)
        .map_err(map_error)?;

    let mut last: Option<io::Error> = None;
    for attempt in 0..1024 {
        let candidate = parent.join(format!("tunnel-{attempt}"));
        match owner_only().create(&candidate) {
            Ok(()) => return Ok(candidate),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => last = Some(error),
            Err(error) => return Err(map_error(error)),
        }
    }
    Err(map_error(last.unwrap_or_else(|| {
        io::Error::new(
            io::ErrorKind::AlreadyExists,
            "could not find an unused tunnel directory name",
        )
    })))
}

/// Build the `ssh` argument list for one `StreamLocal` forward.
///
/// The fixed options are deliberate:
///
/// - `-N` asks for no remote command, because this connection exists only to
///   carry a forward.
/// - `-T` disables pseudo-terminal allocation, which a forward never needs.
/// - `-o BatchMode=yes` refuses interactive prompting. Without it a missing
///   credential turns a programmatic call into an invisible stall; with it the
///   caller gets a fast, explicit failure and can fix their SSH setup.
/// - `-o ExitOnForwardFailure=yes` makes a refused forward fail the connection
///   instead of yielding a live session whose socket silently forwards nothing.
///
/// Caller arguments are appended after these, then the destination last, so a
/// caller can still add options while the destination cannot be mistaken for
/// one.
fn tunnel_args(endpoint: &RemoteEndpoint, local_socket: &Path) -> Vec<OsString> {
    let mut forward = OsString::from(local_socket);
    forward.push(":");
    forward.push(endpoint.remote_socket.as_os_str());

    let mut args: Vec<OsString> = vec![
        OsString::from("-N"),
        OsString::from("-T"),
        OsString::from("-o"),
        OsString::from("BatchMode=yes"),
        OsString::from("-o"),
        OsString::from("ExitOnForwardFailure=yes"),
        OsString::from("-L"),
        forward,
    ];
    args.extend(endpoint.extra_args.iter().cloned());
    args.push(endpoint.destination.clone());
    args
}

fn spawn_tunnel(
    endpoint: &RemoteEndpoint,
    local_socket: &Path,
) -> Result<Child, RemoteEndpointError> {
    let mut command = tokio::process::Command::new(&endpoint.ssh_program);
    command
        .args(tunnel_args(endpoint, local_socket))
        // stdin is closed so ssh cannot consume caller input, while stderr is
        // inherited so the user sees ssh's own diagnostics verbatim instead of
        // this crate paraphrasing them.
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .kill_on_drop(true);
    // Make ssh its own group leader so `kill_tunnel_group` can reach the helpers
    // it starts. Without this the child stays in this process's group, and
    // signalling that group would hit the caller itself rather than the tunnel.
    command.process_group(0);
    command
        .spawn()
        .map_err(|source| RemoteEndpointError::SpawnSsh {
            program: endpoint.ssh_program.to_string_lossy().into_owned(),
            source,
        })
}

async fn await_ready(
    tunnel: &mut RemoteTunnel,
    endpoint: &RemoteEndpoint,
) -> Result<(), RemoteEndpointError> {
    let deadline = Instant::now() + endpoint.ready_timeout;
    loop {
        // A connect proves the forward is actually carrying traffic. Checking
        // for the file's existence would accept a socket ssh has not bound yet.
        if UnixStream::connect(tunnel.socket_path()).await.is_ok() {
            return Ok(());
        }
        if !tunnel.is_connected()? {
            let status = match tunnel.child.as_mut() {
                Some(child) => match child.try_wait() {
                    Ok(Some(status)) => format!(": {status}"),
                    _ => String::new(),
                },
                None => String::new(),
            };
            return Err(RemoteEndpointError::TunnelExited {
                destination: endpoint.destination.to_string_lossy().into_owned(),
                status,
            });
        }
        if Instant::now() >= deadline {
            return Err(RemoteEndpointError::ReadyTimeout {
                path: tunnel.socket_path().to_path_buf(),
                timeout: endpoint.ready_timeout,
            });
        }
        tokio::time::sleep(READY_POLL).await;
    }
}

fn validate_destination(destination: &OsStr) -> Result<(), RemoteEndpointError> {
    if destination.is_empty() {
        return Err(RemoteEndpointError::InvalidDestination {
            reason: "destination must not be empty".to_owned(),
        });
    }
    let rendered = destination.to_string_lossy();
    if rendered.starts_with('-') {
        return Err(RemoteEndpointError::InvalidDestination {
            reason: format!("`{rendered}` would be parsed as an ssh option"),
        });
    }
    Ok(())
}

fn validate_remote_socket(path: &Path) -> Result<(), RemoteEndpointError> {
    if path.as_os_str().is_empty() {
        return Err(RemoteEndpointError::InvalidRemoteSocket {
            reason: "owner-host socket path must not be empty".to_owned(),
        });
    }
    if !path.is_absolute() {
        return Err(RemoteEndpointError::InvalidRemoteSocket {
            reason: "owner-host socket path must be absolute".to_owned(),
        });
    }
    let rendered = path.to_string_lossy();
    if rendered.contains(':') {
        return Err(RemoteEndpointError::InvalidRemoteSocket {
            reason: "owner-host socket path must not contain ':'".to_owned(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        RemoteEndpoint, RemoteEndpointError, create_tunnel_dir, kill_tunnel_group, spawn_tunnel,
        tunnel_args, validate_destination, validate_remote_socket,
    };
    use std::{ffi::OsString, path::Path};

    fn endpoint() -> RemoteEndpoint {
        RemoteEndpoint::new("owner-host", "/run/ctxmux/ctxmux.sock").expect("valid endpoint")
    }

    #[test]
    fn forward_specification_pairs_local_and_remote_sockets() {
        let args = tunnel_args(&endpoint(), Path::new("/tmp/private/owner-host.sock"));
        let forward_index = args
            .iter()
            .position(|arg| arg == "-L")
            .expect("forward flag present");
        assert_eq!(
            args[forward_index + 1],
            OsString::from("/tmp/private/owner-host.sock:/run/ctxmux/ctxmux.sock")
        );
    }

    #[test]
    fn fixed_options_refuse_prompting_and_silent_forward_failure() {
        // Position is the whole guarantee, not decoration. OpenSSH resolves a
        // repeated option to its FIRST occurrence, so these are unbypassable only
        // while they precede `extra_args`; a caller passing `-o BatchMode=no`
        // then loses to the fixed value instead of overriding it. Asserting mere
        // presence would still pass if `extra_args` were extended ahead of them,
        // which is exactly the regression that reopens the interactive stall and
        // the inert-forward wrong-cases, so assert the index relationship.
        let args = tunnel_args(
            &endpoint().with_extra_args(["-o", "BatchMode=no", "-o", "ExitOnForwardFailure=no"]),
            Path::new("/tmp/private/owner-host.sock"),
        );
        let first = |needle: &str| {
            args.iter()
                .position(|arg| arg == needle)
                .unwrap_or_else(|| panic!("{needle} must appear in the invocation"))
        };
        let fixed_batch_mode = first("BatchMode=yes");
        let fixed_forward_failure = first("ExitOnForwardFailure=yes");
        assert!(args.contains(&OsString::from("-N")));
        assert!(args.contains(&OsString::from("-T")));
        // Each fixed value must be reached before the caller's contrary value.
        let caller_batch_mode = first("BatchMode=no");
        let caller_forward_failure = first("ExitOnForwardFailure=no");
        assert!(
            fixed_batch_mode < caller_batch_mode,
            "fixed BatchMode=yes must precede a caller's BatchMode=no to win"
        );
        assert!(
            fixed_forward_failure < caller_forward_failure,
            "fixed ExitOnForwardFailure=yes must precede a caller's contrary value"
        );
        // And no caller argument may be reached before any fixed option at all.
        let first_caller = caller_batch_mode.min(caller_forward_failure);
        assert!(
            fixed_batch_mode.max(fixed_forward_failure) < first_caller,
            "every fixed option must precede the caller's argument list"
        );
    }

    #[test]
    fn destination_is_last_so_it_cannot_be_read_as_an_option() {
        let args = tunnel_args(
            &endpoint().with_extra_args(["-J", "jump-host"]),
            Path::new("/tmp/private/owner-host.sock"),
        );
        assert_eq!(args.last(), Some(&OsString::from("owner-host")));
        let jump = args
            .iter()
            .position(|arg| arg == "-J")
            .expect("caller argument preserved");
        assert_eq!(args[jump + 1], OsString::from("jump-host"));
    }

    #[test]
    fn option_shaped_destination_is_refused() {
        let error = validate_destination(&OsString::from("-oProxyCommand=touch /tmp/pwned"))
            .expect_err("option-shaped destination must be refused");
        assert!(matches!(
            error,
            RemoteEndpointError::InvalidDestination { .. }
        ));
    }

    #[test]
    fn empty_destination_is_refused() {
        assert!(validate_destination(&OsString::new()).is_err());
    }

    #[test]
    fn remote_socket_must_be_absolute_and_colon_free() {
        assert!(validate_remote_socket(Path::new("relative/ctxmux.sock")).is_err());
        assert!(validate_remote_socket(Path::new("/run/ctx:mux.sock")).is_err());
        assert!(validate_remote_socket(Path::new("/run/ctxmux/ctxmux.sock")).is_ok());
    }

    #[tokio::test]
    async fn each_tunnel_gets_a_fresh_owner_only_directory() {
        use std::{fs, os::unix::fs::PermissionsExt};

        let parent = tempfile::tempdir().expect("temp dir");
        let root = parent.path().join("tunnels");

        let first = create_tunnel_dir(&root).expect("first tunnel dir");
        let second = create_tunnel_dir(&root).expect("second tunnel dir");

        assert_ne!(
            first, second,
            "two tunnels must not share a directory, since nothing locks the socket path"
        );
        // Both the per-tunnel directories and the parent this call created must
        // be owner-only. A plain create_dir would have left the parent at the
        // umask default, exposing every tunnel socket beneath it.
        for dir in [&first, &second, &root] {
            let mode = fs::metadata(dir).expect("metadata").permissions().mode() & 0o777;
            assert_eq!(
                mode,
                0o700,
                "{} must be owner-only from creation",
                dir.display()
            );
        }
    }

    /// The teardown guarantee is a *group* guarantee, so prove the group exists.
    ///
    /// `kill_tunnel_group` signals `-pid`, which reaches ssh's helpers only when
    /// the spawned child is its own group leader. No argument list shows that;
    /// only the running process does. So this drives a stand-in that leaves a
    /// helper behind — the `ControlMaster` mux or `ProxyCommand` child a caller's
    /// configuration may add — and requires teardown to end it. Without
    /// `process_group(0)` the signal would go to the caller's own group instead,
    /// and the helper would survive with an authenticated channel open.
    #[tokio::test]
    async fn the_tunnel_child_leads_a_group_that_teardown_reaches() {
        use std::{fs, os::unix::fs::PermissionsExt, time::Duration};

        use rustix::process::{Pid, getpgid, test_kill_process};

        let dir = tempfile::tempdir().expect("temp dir");
        let helper_pid_path = dir.path().join("helper.pid");
        let stand_in = dir.path().join("ssh-with-helper");
        // The background child is the point: killing the direct child does not
        // signal it, so only a group signal can end it.
        fs::write(
            &stand_in,
            format!(
                "#!/bin/sh\nsleep 30 &\necho $! > '{}'\nexec sleep 30\n",
                helper_pid_path.display()
            ),
        )
        .expect("write the stand-in ssh");
        fs::set_permissions(&stand_in, fs::Permissions::from_mode(0o700))
            .expect("make the stand-in executable");

        let endpoint = endpoint().with_ssh_program(&stand_in);
        let mut child =
            spawn_tunnel(&endpoint, &dir.path().join("local.sock")).expect("spawn the stand-in");
        let leader =
            Pid::from_raw(i32::try_from(child.id().expect("child pid")).expect("in range"))
                .expect("child pid is nonzero");

        let helper = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Some(pid) = fs::read_to_string(&helper_pid_path)
                    .ok()
                    .and_then(|text| text.trim().parse::<i32>().ok())
                    .and_then(Pid::from_raw)
                {
                    return pid;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the stand-in must report the helper it started");

        assert_eq!(
            getpgid(Some(helper)).expect("helper process group"),
            leader,
            "the helper must sit in a group led by the tunnel child, or signalling \
             that group cannot reach it"
        );

        kill_tunnel_group(&child);
        let _ = child.start_kill();
        let _ = child.wait().await;

        // The helper is not this process's child, so nothing here can reap it.
        // Its disappearance is therefore evidence of the group signal itself.
        tokio::time::timeout(Duration::from_secs(10), async {
            while test_kill_process(helper).is_ok() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("teardown must end the helper the direct child left running");
    }
}
