//! Long-lived native Run owner and local protocol server.

#[cfg(not(unix))]
compile_error!("the first ctxmux native transport currently requires Unix sockets");

use std::{
    collections::{HashMap, HashSet, VecDeque},
    fs,
    io::{self, Write},
    os::fd::{AsRawFd, OwnedFd, RawFd},
    os::unix::{
        fs::{FileTypeExt, MetadataExt, PermissionsExt},
        net::UnixStream as StdUnixStream,
    },
    path::{Path, PathBuf},
    sync::{
        Arc, Condvar, Mutex, OnceLock, RwLock,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

mod adopted_pty;
mod attachment;
mod creation;
mod handoff;
mod native_control;
mod native_runtime;
mod native_session;
mod native_spawn_env;
mod persistence;
mod qualification_stats;
mod run_spec;
mod tmux;

pub use persistence::PersistenceError;

use ctxmux_protocol::{
    AppliedInputRange, AttachedSnapshot, ClientFrame, CommandDisposition, ControlFailure,
    CreateOperationKey, DaemonInstanceId, ErrorCode, ForkFidelity, ForkPlan, InterruptionReason,
    MAX_FRAME_BYTES, OutputChunk, OutputReplay, PROTOCOL_VERSION, ProtocolError, RecoverableInput,
    Request, Response, RunBackend, RunCapabilities, RunEvent, RunId, RunInfo, RunLineage, RunSpec,
    RunState, ServerFrame, TerminalSize, TmuxRunEvent, decode_frame, encode_frame,
};
use futures_util::{SinkExt, StreamExt};
use portable_pty::{Child, ChildKiller, CommandBuilder, ExitStatus, PtySize, native_pty_system};
use run_spec::{validate_run_spec, validate_terminal_size};
use thiserror::Error;
use tokio::{
    net::{UnixListener, UnixStream},
    sync::{Notify, broadcast},
};
use tokio_util::codec::{Framed, LinesCodec, LinesCodecError};

use crate::adopted_pty::AdoptedMasterPty;
use crate::creation::{
    CommitUnknownReservation, CreationFlight, CreationFlightOwner, CreationRequest,
    PendingPublication, PersistentCollectionCandidate, PublicationReservation, RunRegistry,
    TerminalOrdinal, TerminalPublicationOwner, TmuxCleanupReservation, UnpublishedCleanupOwner,
    UnpublishedCleanupReservation,
};
use crate::native_control::{
    ControlResult, DetachedNativeDescriptors, HandoffInputState, InputDrainGate,
    NativeControlOwner, PendingInput, PendingSignal, PendingStop,
};
use crate::native_runtime::{NativeRunOwner as NativeRuntimeOwner, NativeRunRegistration};
use crate::native_session::{AdoptedChild, NativeSession};
use crate::persistence::{
    CommittedStart, HandoffHint, Persistence, PersistentCandidate, PersistentRun,
    PersistentStartCompletion, PersistentStartFailure, RecoveredRun, StagedPersistentStart,
    StartDisposition,
};
use crate::qualification_stats::{Gauge as QualificationGauge, QualificationStats};
use crate::tmux::{
    BoundedLineRead, ControlItem, ControlParser, SocketIdentity as TmuxSocketIdentity,
};

const OUTPUT_RETENTION_BYTES: usize = 4 * 1024 * 1024;
const LIVE_EVENT_CAPACITY: usize = 256;
const CHILD_CONTROL_POLL: Duration = Duration::from_millis(20);
const STOP_ACK_TIMEOUT: Duration = Duration::from_secs(3);
const STOP_GRACEFUL_TIMEOUT: Duration = Duration::from_millis(500);
const STOP_FORCED_TIMEOUT: Duration = Duration::from_secs(1);
const UNPUBLISHED_REAP_INLINE_TIMEOUT: Duration = Duration::from_millis(25);
const TMUX_OUTPUT_DRAIN_TIMEOUT: Duration = Duration::from_secs(1);
const TMUX_FAILED_IMPORT_CLEANUP_TIMEOUT: Duration = Duration::from_secs(2);
const TMUX_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(4);
const TMUX_IMPORT_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(3);
const TMUX_IMPORT_PREPARE_TIMEOUT: Duration = Duration::from_secs(5);
const TMUX_IMPORT_TOTAL_TIMEOUT: Duration = Duration::from_secs(7);
const TMUX_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(8);
const UPGRADE_QUIESCE_TIMEOUT: Duration = Duration::from_secs(8);

/// Failure that prevents the daemon server from running.
#[derive(Debug, Error)]
pub enum ServerError {
    /// The requested path exists but is not a Unix socket.
    #[error("refusing to replace non-socket path: {0}")]
    InvalidSocketTarget(PathBuf),
    /// Another daemon is already accepting connections at this path.
    #[error("a ctxmux daemon is already listening at {0}")]
    AlreadyRunning(PathBuf),
    /// The checked stale socket was replaced before ctxmux could remove it.
    #[error("socket target changed during stale cleanup: {0}")]
    SocketTargetChanged(PathBuf),
    /// A platform I/O operation failed.
    #[error("ctxmux daemon I/O failed at {path}: {source}")]
    Io {
        /// Path involved in the failure.
        path: PathBuf,
        /// Platform I/O failure.
        #[source]
        source: io::Error,
    },
    /// Optional durable state could not be safely opened or reconciled.
    #[error(transparent)]
    Persistence(#[from] PersistenceError),
    /// A handed-off Run could not be re-adopted onto live native control after an
    /// exec-in-place upgrade.
    #[error("adopt handed-off run: {0}")]
    Adopt(String),
    /// One or more owned runtime operations or Backend controls failed cleanup.
    #[error("ctxmux daemon shutdown failed: {failures}")]
    Shutdown {
        /// Aggregated drain and cleanup failures for ctxmux-owned work.
        failures: String,
    },
}

impl ServerError {
    fn io(path: impl Into<PathBuf>, source: io::Error) -> Self {
        Self::Io {
            path: path.into(),
            source,
        }
    }
}

/// Serve native Runs until the daemon receives Ctrl-C.
///
/// # Errors
///
/// Returns [`ServerError`] when the socket path is unsafe, another daemon is
/// already listening, the local listener cannot be created or operated, or a
/// ctxmux-owned runtime work cannot be drained or Backend control processes
/// cannot be cleaned up during shutdown.
pub async fn serve(socket_path: impl Into<PathBuf>) -> Result<(), ServerError> {
    serve_with_qualification(socket_path, None).await
}

#[doc(hidden)]
pub async fn serve_with_qualification(
    socket_path: impl Into<PathBuf>,
    qualification_stats_fd: Option<OwnedFd>,
) -> Result<(), ServerError> {
    serve_with_inherited_descriptors(socket_path, qualification_stats_fd, None).await
}

#[doc(hidden)]
pub async fn serve_with_inherited_descriptors(
    socket_path: impl Into<PathBuf>,
    qualification_stats_fd: Option<OwnedFd>,
    readiness_fd: Option<OwnedFd>,
) -> Result<(), ServerError> {
    serve_with_persistence(
        socket_path.into(),
        None,
        qualification_stats_fd,
        readiness_fd,
    )
    .await
}

/// Serve Runs with historical metadata and replay persisted in `state_dir`.
///
/// # Errors
///
/// Returns [`ServerError`] when the state directory cannot be exclusively and
/// safely opened, its exact schema or invariants fail validation, the socket
/// cannot be published, or owned runtime cleanup fails during shutdown.
pub async fn serve_with_state_dir(
    socket_path: impl Into<PathBuf>,
    state_dir: impl Into<PathBuf>,
) -> Result<(), ServerError> {
    serve_with_state_dir_and_qualification(socket_path, state_dir, None).await
}

#[doc(hidden)]
pub async fn serve_with_state_dir_and_qualification(
    socket_path: impl Into<PathBuf>,
    state_dir: impl Into<PathBuf>,
    qualification_stats_fd: Option<OwnedFd>,
) -> Result<(), ServerError> {
    serve_with_state_dir_and_inherited_descriptors(
        socket_path,
        state_dir,
        qualification_stats_fd,
        None,
        None,
    )
    .await
}

#[doc(hidden)]
pub async fn serve_with_state_dir_and_inherited_descriptors(
    socket_path: impl Into<PathBuf>,
    state_dir: impl Into<PathBuf>,
    qualification_stats_fd: Option<OwnedFd>,
    readiness_fd: Option<OwnedFd>,
    handoff_fd: Option<OwnedFd>,
) -> Result<(), ServerError> {
    let handoff = match handoff_fd {
        Some(fd) => Some(
            crate::handoff::read_manifest(fd)
                .map_err(|source| ServerError::io("<handoff-fd>", source))?,
        ),
        None => None,
    };
    let state_dir = state_dir.into();
    let manager = if let Some(manifest) = &handoff {
        // Incoming exec-in-place image: reuse the handed-off epoch, exclude
        // the still-live Run set from reconciliation, and adopt the inherited
        // state-lock descriptor rather than re-locking. Each raw fd number in
        // the manifest is wrapped into an `OwnedFd` exactly once here — the
        // state lock into the hint and each Run's pty master into the adopt
        // map. The listener fd is intentionally left untouched: it is wrapped
        // later inside `serve_with_persistence_manager`/`adopt_listener`.
        let live_set: HashSet<RunId> = manifest.runs.iter().map(|run| run.run_id).collect();
        let mut adopt: HashMap<RunId, (OwnedFd, u32, HandoffInputState)> =
            HashMap::with_capacity(manifest.runs.len());
        for run in &manifest.runs {
            let master = ctxmux_inherited_fd::claim_inherited_process_fd(run.master_fd)
                .map_err(|source| ServerError::io("<handoff master fd>", source))?;
            adopt.insert(run.run_id, (master, run.child_pid, run.input_state.clone()));
        }
        let hint = HandoffHint {
            epoch: manifest.epoch.clone(),
            live_set,
            state_lock_fd: Some(
                ctxmux_inherited_fd::claim_inherited_process_fd(manifest.state_lock_fd)
                    .map_err(|source| ServerError::io("<handoff state-lock fd>", source))?,
            ),
        };
        let (persistence, recovered) = Persistence::open_with_handoff(state_dir.clone(), hint)?;
        let stats = QualificationStats::from_optional_inherited_fd(
            qualification_stats_fd,
            persistence.daemon_instance().to_string(),
        )
        .map_err(|source| ServerError::io("qualification stats fd", source))?;
        Arc::new(
            RunManager::persistent_with_handoff_and_stats(persistence, recovered, stats, adopt)
                .map_err(|error| ServerError::Adopt(error.message))?,
        )
    } else {
        let (persistence, recovered) = Persistence::open(state_dir.clone())?;
        let stats = QualificationStats::from_optional_inherited_fd(
            qualification_stats_fd,
            persistence.daemon_instance().to_string(),
        )
        .map_err(|source| ServerError::io("qualification stats fd", source))?;
        Arc::new(RunManager::persistent_with_stats(
            persistence,
            recovered,
            stats,
        ))
    };
    serve_with_persistence_manager(
        socket_path.into(),
        manager,
        readiness_fd,
        handoff,
        Some(state_dir),
    )
    .await
}

async fn serve_with_persistence(
    socket_path: PathBuf,
    persistence: Option<(Persistence, Vec<RecoveredRun>)>,
    qualification_stats_fd: Option<OwnedFd>,
    readiness_fd: Option<OwnedFd>,
) -> Result<(), ServerError> {
    let manager = if let Some((persistence, recovered)) = persistence {
        let stats = QualificationStats::from_optional_inherited_fd(
            qualification_stats_fd,
            persistence.daemon_instance().to_string(),
        )
        .map_err(|source| ServerError::io("qualification stats fd", source))?;
        Arc::new(RunManager::persistent_with_stats(
            persistence,
            recovered,
            stats,
        ))
    } else {
        let daemon_instance = DaemonInstanceId::new();
        let stats = QualificationStats::from_optional_inherited_fd(
            qualification_stats_fd,
            daemon_instance.to_string(),
        )
        .map_err(|source| ServerError::io("qualification stats fd", source))?;
        Arc::new(RunManager::with_instance_and_stats(daemon_instance, stats))
    };
    serve_with_persistence_manager(socket_path, manager, readiness_fd, None, None).await
}

async fn serve_with_persistence_manager(
    socket_path: PathBuf,
    manager: Arc<RunManager>,
    readiness_fd: Option<OwnedFd>,
    handoff: Option<crate::handoff::HandoffManifest>,
    state_dir: Option<PathBuf>,
) -> Result<(), ServerError> {
    // On the exec-in-place path, reconstruct the listener from the inherited
    // socket fd. Re-binding would unlink and recreate the socket inode, dropping
    // every connected client and tripping our own AlreadyRunning guard, so the
    // adopted path must skip prepare_socket_path / bind / set_permissions.
    let listener = if let Some(manifest) = &handoff {
        adopt_listener(manifest.listener_fd)?
    } else {
        prepare_socket_path(&socket_path)?;
        let listener = UnixListener::bind(&socket_path)
            .map_err(|source| ServerError::io(&socket_path, source))?;
        fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600))
            .map_err(|source| ServerError::io(&socket_path, source))?;
        listener
    };
    serve_with_manager(
        socket_path,
        listener,
        manager,
        readiness_fd,
        handoff,
        state_dir,
    )
    .await
}

/// Reconstruct the local listener from an inherited socket fd without binding.
///
/// The incoming exec-in-place image claims ownership of the descriptor the
/// outgoing image left (its CLOEXEC bit cleared just before exec) and wraps it
/// through the safe `From<OwnedFd>` impl, so the socket inode is unchanged.
fn adopt_listener(listener_fd: RawFd) -> Result<UnixListener, ServerError> {
    let owned = ctxmux_inherited_fd::claim_inherited_process_fd(listener_fd)
        .map_err(|source| ServerError::io("<handoff listener fd>", source))?;
    let std_listener = std::os::unix::net::UnixListener::from(owned); // safe From<OwnedFd>
    std_listener
        .set_nonblocking(true)
        .map_err(|source| ServerError::io("<handoff listener fd>", source))?;
    UnixListener::from_std(std_listener)
        .map_err(|source| ServerError::io("<handoff listener fd>", source))
}

async fn serve_with_manager(
    socket_path: PathBuf,
    listener: UnixListener,
    manager: Arc<RunManager>,
    readiness_fd: Option<OwnedFd>,
    handoff: Option<crate::handoff::HandoffManifest>,
    state_dir: Option<PathBuf>,
) -> Result<(), ServerError> {
    let _socket_guard = SocketGuard::new(socket_path.clone())?;
    if let Some(handoff) = &handoff {
        // A12 wires manifest.state_lock_fd into the incoming-image startup path.
        eprintln!(
            "ctxmuxd: adopted inherited listener for handoff (epoch {}, {} run(s))",
            handoff.epoch,
            handoff.runs.len()
        );
    }
    if let Some(readiness_fd) = readiness_fd {
        let mut readiness = fs::File::from(readiness_fd);
        let record = serde_json::to_vec(&serde_json::json!({
            "schema": "ctxmux.daemon-ready.v1",
            "daemon_instance": manager.daemon_instance.to_string(),
        }))
        .map_err(|source| ServerError::io("<readiness-fd>", io::Error::other(source)))?;
        readiness
            .write_all(&record)
            .and_then(|()| readiness.write_all(b"\n"))
            .and_then(|()| readiness.flush())
            .map_err(|source| ServerError::io("<readiness-fd>", source))?;
    }

    let mut sighup = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())
        .map_err(|source| ServerError::io("<sighup>", source))?;

    loop {
        tokio::select! {
            result = listener.accept() => {
                let (stream, _) = result.map_err(|source| ServerError::io(&socket_path, source))?;
                let manager = Arc::clone(&manager);
                tokio::spawn(async move {
                    if let Err(error) = handle_connection(stream, manager).await {
                        eprintln!("ctxmuxd connection error: {error}");
                    }
                });
            }
            signal = tokio::signal::ctrl_c() => {
                signal.map_err(|source| ServerError::io(&socket_path, source))?;
                manager.shutdown_owned_controls(TMUX_SHUTDOWN_TIMEOUT)?;
                manager.qualification_stats.finish();
                return Ok(());
            }
            _ = sighup.recv() => {
                if manager.persistence.is_none() {
                    eprintln!(
                        "ctxmuxd: SIGHUP ignored: upgrade continuity requires --state-dir"
                    );
                    continue;
                }
                let Some(state_dir) = state_dir.as_deref() else {
                    eprintln!(
                        "ctxmuxd: SIGHUP ignored: no state directory recorded for re-exec"
                    );
                    continue;
                };
                match perform_exec_upgrade(&socket_path, state_dir, &listener, &manager) {
                    Ok(()) => unreachable!(
                        "a successful exec-in-place replaces the process image and never returns"
                    ),
                    Err(UpgradeAbort::BeforeExtract(error)) => {
                        // Reversible failure: nothing has been extracted, all
                        // controls are still owned. Abort the upgrade and keep
                        // serving by falling through to the next loop iteration.
                        eprintln!(
                            "ctxmuxd: exec-in-place upgrade aborted before extract, continuing to serve: {error}"
                        );
                    }
                    Err(UpgradeAbort::AfterExtract(error)) => {
                        // Point of no return passed: native children/controls were
                        // forgotten and their fds marked to survive exec, but exec
                        // did not happen. There is no in-image owner to roll back
                        // to — fail-stop so process death reclaims the fds (never
                        // resume serving with forgotten controls).
                        let message = format!("exec-in-place upgrade failed after extract: {error}");
                        manager.incarnation_failure.record(message.clone());
                        let _ = manager.shutdown_owned_controls(TMUX_SHUTDOWN_TIMEOUT);
                        manager.qualification_stats.finish();
                        return Err(ServerError::Shutdown { failures: message });
                    }
                }
            }
            failure = manager.incarnation_failure.wait() => {
                let cleanup = manager.shutdown_owned_controls(TMUX_SHUTDOWN_TIMEOUT);
                let failures = match cleanup {
                    Ok(()) => failure,
                    Err(error) => format!("{failure}; shutdown: {error}"),
                };
                manager.qualification_stats.finish();
                return Err(ServerError::Shutdown { failures });
            }
        }
    }
}

/// How an aborted exec-in-place upgrade failed, relative to two irreversible
/// points. The upgrade proceeds: reversible setup → drain admitted requests →
/// extract (point of no return) → exec.
/// - `BeforeExtract`: a reversible setup step failed (handoff file / request drain)
///   before anything daemon-global was mutated — the daemon keeps serving.
/// - `AfterExtract`: a failure past the point of no return — native
///   children/controls were forgotten and their fds marked to survive exec, but
///   exec did not happen — fail-stop so process death reclaims the fds (never
///   resume serving with forgotten controls).
enum UpgradeAbort {
    BeforeExtract(ServerError),
    AfterExtract(ServerError),
}

/// Perform an exec-in-place upgrade: drain, extract the live native runs, write
/// the handoff manifest, clear CLOEXEC on exactly the descriptors that must
/// survive, and execve this binary. On success this replaces the process image
/// and never returns. `manager.persistence` MUST be `Some` (the caller checks).
fn perform_exec_upgrade(
    socket_path: &std::path::Path,
    state_dir: &std::path::Path,
    listener: &UnixListener,
    manager: &RunManager,
) -> Result<(), UpgradeAbort> {
    use std::io::{Seek as _, Write as _};
    use std::os::fd::AsRawFd as _;
    use std::os::unix::fs::OpenOptionsExt as _;
    use std::os::unix::process::CommandExt as _;

    // --- Reversible phase (before the point of no return) ---

    // A regular, immediately unlinked state-dir file avoids the pipe-capacity
    // deadlock that a complete bounded Input ledger could trigger before exec:
    // no incoming reader exists until the image has already been replaced.
    let handoff_path = state_dir.join(format!(".ctxmux-handoff-{}", uuid::Uuid::new_v4()));
    let mut handoff_file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&handoff_path)
        .map_err(|source| UpgradeAbort::BeforeExtract(ServerError::io(&handoff_path, source)))?;
    std::fs::remove_file(&handoff_path)
        .map_err(|source| UpgradeAbort::BeforeExtract(ServerError::io(&handoff_path, source)))?;
    rustix::io::fcntl_setfd(&handoff_file, rustix::io::FdFlags::CLOEXEC).map_err(|errno| {
        UpgradeAbort::BeforeExtract(ServerError::io(
            "<handoff file cloexec>",
            std::io::Error::from(errno),
        ))
    })?;
    let exe = std::env::current_exe()
        .map_err(|source| UpgradeAbort::BeforeExtract(ServerError::io("<current_exe>", source)))?;

    // Fence new request mutations and wait until every already-admitted request
    // has written its response. The fence is RAII-reversible until extraction,
    // so timeout or owner preflight failure restores complete service.
    let mut request_fence = manager
        .upgrade_requests
        .begin_drain(UPGRADE_QUIESCE_TIMEOUT)
        .map_err(|failure| {
            UpgradeAbort::BeforeExtract(ServerError::Shutdown { failures: failure })
        })?;

    // --- POINT OF NO RETURN: extract relinquishes reap/close authority for
    // every live native child; from here, any failure is fail-stop. ---
    let live = manager
        .native_runs
        .extract_for_handoff()
        .map_err(|failure| {
            UpgradeAbort::BeforeExtract(ServerError::Shutdown { failures: failure })
        })?;
    request_fence.commit();

    // Durable-commit barrier AFTER extract (corrected order): extract closes
    // each run's pty reader (via `entry.output = None`) and relinquishes its
    // child/control, so after extract the owner enqueues no further Appends.
    // Draining the FIFO barrier here fences every byte ever read before we exec,
    // guaranteeing the persisted cursor covers all of them. A barrier *before*
    // extract would race the still-running reader and could leave a replay gap.
    manager
        .persistence_barrier()
        .map_err(UpgradeAbort::AfterExtract)?;

    // Build the manifest from the extracted descriptors + the process listener
    // and state-lock fds.
    let runs: Vec<crate::handoff::HandoffRun> = live
        .into_iter()
        .map(|d| crate::handoff::HandoffRun {
            run_id: d.run_id,
            child_pid: d.child_pid,
            master_fd: d.master_fd,
            input_state: d.input_state,
        })
        .collect();
    let epoch = manager.daemon_instance.to_string();
    let listener_fd = listener.as_raw_fd();
    let state_lock_fd = manager
        .persistence
        .as_ref()
        .expect("perform_exec_upgrade requires persistent mode (verified by caller)")
        .state_lock_fd();
    let manifest = crate::handoff::HandoffManifest::new(epoch, listener_fd, state_lock_fd, runs);

    // Serialize directly into the unlinked file, append one NDJSON newline, and
    // rewind it for the incoming image. This keeps transient memory bounded by
    // serde's per-value work instead of cloning every retained Input payload.
    serde_json::to_writer(&mut handoff_file, &manifest).map_err(|source| {
        UpgradeAbort::AfterExtract(ServerError::io(
            "<handoff manifest>",
            std::io::Error::other(source),
        ))
    })?;
    handoff_file
        .write_all(b"\n")
        .and_then(|()| handoff_file.flush())
        .and_then(|()| handoff_file.seek(std::io::SeekFrom::Start(0)).map(|_| ()))
        .map_err(|source| {
            UpgradeAbort::AfterExtract(ServerError::io("<handoff manifest>", source))
        })?;
    let read_fd = handoff_file.as_raw_fd();

    // Clear CLOEXEC LAST, immediately before execve, on exactly the fds that
    // must survive: the manifest's fds ([listener, state_lock, ...masters]) plus
    // the handoff manifest file. Nothing else.
    for fd in manifest.all_fds() {
        ctxmux_inherited_fd::clear_cloexec(fd).map_err(|source| {
            UpgradeAbort::AfterExtract(ServerError::io("<handoff fd cloexec>", source))
        })?;
    }
    ctxmux_inherited_fd::clear_cloexec(read_fd).map_err(|source| {
        UpgradeAbort::AfterExtract(ServerError::io("<handoff-fd cloexec>", source))
    })?;

    // Re-exec this same binary with the inherited descriptors. exec() returns
    // ONLY on failure; on success the image is replaced here.
    let mut command = std::process::Command::new(exe);
    command
        .arg("--socket")
        .arg(socket_path)
        .arg("--state-dir")
        .arg(state_dir)
        .arg("--handoff-fd")
        .arg(read_fd.to_string());
    let exec_error = command.exec(); // only returns on failure
    // Keep the manifest file alive until here so the fd is not closed before exec.
    drop(handoff_file);
    Err(UpgradeAbort::AfterExtract(ServerError::io(
        "<exec-in-place>",
        exec_error,
    )))
}

fn prepare_socket_path(path: &Path) -> Result<(), ServerError> {
    prepare_socket_path_with_hook(path, || {})
}

fn prepare_socket_path_with_hook<F>(path: &Path, after_inactive_probe: F) -> Result<(), ServerError>
where
    F: FnOnce(),
{
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| ServerError::io(parent, source))?;
    }
    if !path.exists() {
        return Ok(());
    }

    let metadata = fs::symlink_metadata(path).map_err(|source| ServerError::io(path, source))?;
    if !metadata.file_type().is_socket() {
        return Err(ServerError::InvalidSocketTarget(path.to_path_buf()));
    }
    if StdUnixStream::connect(path).is_ok() {
        return Err(ServerError::AlreadyRunning(path.to_path_buf()));
    }
    let checked_identity = SocketIdentity::from_metadata(&metadata);
    after_inactive_probe();
    let current_metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(ServerError::io(path, error)),
    };
    if !current_metadata.file_type().is_socket()
        || SocketIdentity::from_metadata(&current_metadata) != checked_identity
    {
        return Err(ServerError::SocketTargetChanged(path.to_path_buf()));
    }
    if StdUnixStream::connect(path).is_ok() {
        return Err(ServerError::AlreadyRunning(path.to_path_buf()));
    }
    let final_metadata =
        fs::symlink_metadata(path).map_err(|source| ServerError::io(path, source))?;
    if !final_metadata.file_type().is_socket()
        || SocketIdentity::from_metadata(&final_metadata) != checked_identity
    {
        return Err(ServerError::SocketTargetChanged(path.to_path_buf()));
    }
    fs::remove_file(path).map_err(|source| ServerError::io(path, source))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SocketIdentity {
    device: u64,
    inode: u64,
}

impl SocketIdentity {
    fn from_metadata(metadata: &fs::Metadata) -> Self {
        Self {
            device: metadata.dev(),
            inode: metadata.ino(),
        }
    }
}

struct SocketGuard {
    path: PathBuf,
    identity: SocketIdentity,
}

impl SocketGuard {
    fn new(path: PathBuf) -> Result<Self, ServerError> {
        let metadata =
            fs::symlink_metadata(&path).map_err(|source| ServerError::io(&path, source))?;
        if !metadata.file_type().is_socket() {
            return Err(ServerError::SocketTargetChanged(path));
        }
        let identity = SocketIdentity::from_metadata(&metadata);
        Ok(Self { path, identity })
    }
}

impl Drop for SocketGuard {
    fn drop(&mut self) {
        let Ok(metadata) = fs::symlink_metadata(&self.path) else {
            return;
        };
        if metadata.file_type().is_socket()
            && SocketIdentity::from_metadata(&metadata) == self.identity
        {
            let _ = fs::remove_file(&self.path);
        }
    }
}

struct RunManager {
    daemon_instance: DaemonInstanceId,
    registry: RunRegistry,
    creation_flights: CreationFlightOwner,
    unpublished_cleanups: UnpublishedCleanupOwner,
    terminal_publications: TerminalPublicationOwner,
    native_input_drains: InputDrainGate,
    native_runs: NativeRuntimeOwner,
    qualification_stats: QualificationStats,
    live_event_capacity: usize,
    persistence: Option<Persistence>,
    commit_unknown_reservations: Mutex<Vec<CommitUnknownReservation>>,
    incarnation_failure: IncarnationFailure,
    upgrade_requests: UpgradeRequestGate,
    tmux_shutting_down: AtomicBool,
    tmux_operation_gate: RwLock<()>,
    #[cfg(test)]
    attachment_hook: Option<Arc<AttachmentTestHook>>,
    #[cfg(test)]
    creation_hook: Option<Arc<CreationTestHook>>,
}

#[derive(Clone, Default)]
struct UpgradeRequestGate {
    inner: Arc<UpgradeRequestGateInner>,
}

#[derive(Default)]
struct UpgradeRequestGateInner {
    state: Mutex<UpgradeRequestGateState>,
    changed: Condvar,
}

#[derive(Default)]
struct UpgradeRequestGateState {
    phase: UpgradeRequestPhase,
    active: usize,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum UpgradeRequestPhase {
    #[default]
    Open,
    Draining,
    Sealed,
}

enum UpgradeRequestAdmission {
    Execute(UpgradeRequestPermit),
    Retry(UpgradeRequestPermit),
    Sealed,
}

struct UpgradeRequestPermit {
    inner: Arc<UpgradeRequestGateInner>,
}

struct UpgradeRequestFence {
    gate: UpgradeRequestGate,
    committed: bool,
}

impl UpgradeRequestGate {
    fn admit(&self) -> UpgradeRequestAdmission {
        let mut state = mutex_lock(&self.inner.state);
        match state.phase {
            UpgradeRequestPhase::Open | UpgradeRequestPhase::Draining => {
                state.active += 1;
                let permit = UpgradeRequestPermit {
                    inner: Arc::clone(&self.inner),
                };
                if state.phase == UpgradeRequestPhase::Open {
                    UpgradeRequestAdmission::Execute(permit)
                } else {
                    UpgradeRequestAdmission::Retry(permit)
                }
            }
            UpgradeRequestPhase::Sealed => UpgradeRequestAdmission::Sealed,
        }
    }

    fn begin_drain(&self, timeout: Duration) -> Result<UpgradeRequestFence, String> {
        let deadline = Instant::now() + timeout;
        let mut state = mutex_lock(&self.inner.state);
        if state.phase != UpgradeRequestPhase::Open {
            return Err("another exec-in-place upgrade is already draining requests".to_owned());
        }
        state.phase = UpgradeRequestPhase::Draining;
        while state.active != 0 {
            let now = Instant::now();
            if now >= deadline {
                state.phase = UpgradeRequestPhase::Open;
                self.inner.changed.notify_all();
                return Err(format!(
                    "timed out waiting for {} admitted request(s) to finish",
                    state.active
                ));
            }
            let (next, _) = self
                .inner
                .changed
                .wait_timeout(state, deadline.saturating_duration_since(now))
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state = next;
        }
        state.phase = UpgradeRequestPhase::Sealed;
        Ok(UpgradeRequestFence {
            gate: self.clone(),
            committed: false,
        })
    }
}

impl Drop for UpgradeRequestPermit {
    fn drop(&mut self) {
        let mut state = mutex_lock(&self.inner.state);
        state.active = state
            .active
            .checked_sub(1)
            .expect("upgrade request permits remain balanced");
        if state.active == 0 {
            self.inner.changed.notify_all();
        }
    }
}

impl UpgradeRequestFence {
    fn commit(&mut self) {
        self.committed = true;
    }
}

impl Drop for UpgradeRequestFence {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        let mut state = mutex_lock(&self.gate.inner.state);
        debug_assert_eq!(state.phase, UpgradeRequestPhase::Sealed);
        state.phase = UpgradeRequestPhase::Open;
        self.gate.inner.changed.notify_all();
    }
}

fn upgrade_retry_error() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::BackendUnavailable,
        "ctxmux daemon is draining for an exec-in-place upgrade; reconnect and retry",
    )
}

#[derive(Clone, Default)]
struct IncarnationFailure {
    inner: Arc<IncarnationFailureInner>,
}

#[derive(Default)]
struct IncarnationFailureInner {
    message: Mutex<Option<String>>,
    changed: Notify,
}

impl IncarnationFailure {
    fn record(&self, message: String) {
        let mut current = mutex_lock(&self.inner.message);
        if current.is_none() {
            *current = Some(message);
            self.inner.changed.notify_waiters();
        }
    }

    async fn wait(&self) -> String {
        loop {
            let notified = self.inner.changed.notified();
            if let Some(message) = mutex_lock(&self.inner.message).clone() {
                return message;
            }
            notified.await;
        }
    }

    #[cfg(test)]
    fn message(&self) -> Option<String> {
        mutex_lock(&self.inner.message).clone()
    }
}

#[derive(Clone, Default)]
struct NativeWaitFailure {
    creation_flights: CreationFlightOwner,
    incarnation_failure: IncarnationFailure,
}

impl NativeWaitFailure {
    fn record(&self, run_id: RunId, error: &str) {
        self.creation_flights.fence();
        self.incarnation_failure.record(format!(
            "Run {run_id} lost native child wait authority; restart is required: {error}"
        ));
    }
}

/// Couples the exact Registry ticket to `SQLite`'s staged transaction across
/// ordinary errors and thread unwind. Only a proven `NotCommitted` disposition
/// may let Drop restore the Registry candidates.
struct PersistentPublicationOwner<'a> {
    manager: &'a RunManager,
    reservation: Option<PublicationReservation>,
    staged: Option<StagedPersistentStart>,
    phase: PersistentPublicationPhase,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PersistentPublicationPhase {
    Staged,
    Deciding,
    NotCommitted,
    CommittedUnpublished,
    Finished,
}

enum CreationPublication<'a> {
    Memory(PublicationReservation),
    Persistent(PersistentPublicationOwner<'a>),
}

impl CreationPublication<'_> {
    fn abort(self) -> Result<(), PersistentStartFailure> {
        match self {
            Self::Memory(_) => Ok(()),
            Self::Persistent(owner) => owner.abort(),
        }
    }
}

impl<'a> PersistentPublicationOwner<'a> {
    fn new(
        manager: &'a RunManager,
        reservation: PublicationReservation,
        staged: StagedPersistentStart,
    ) -> Self {
        Self {
            manager,
            reservation: Some(reservation),
            staged: Some(staged),
            phase: PersistentPublicationPhase::Staged,
        }
    }

    fn commit(&mut self) -> PersistentStartCompletion {
        self.phase = PersistentPublicationPhase::Deciding;
        let completion = self
            .staged
            .take()
            .expect("persistent publication commits one staged transaction")
            .commit();
        self.phase = match &completion {
            PersistentStartCompletion::NotCommitted(_) => PersistentPublicationPhase::NotCommitted,
            PersistentStartCompletion::Committed(_) => {
                PersistentPublicationPhase::CommittedUnpublished
            }
            PersistentStartCompletion::CommitUnknown(_) => PersistentPublicationPhase::Deciding,
        };
        completion
    }

    fn abort(mut self) -> Result<(), PersistentStartFailure> {
        self.phase = PersistentPublicationPhase::Deciding;
        let result = self
            .staged
            .take()
            .expect("persistent publication aborts one staged transaction")
            .abort();
        match &result {
            Ok(()) => self.phase = PersistentPublicationPhase::NotCommitted,
            Err(failure) if failure.disposition() == StartDisposition::NotCommitted => {
                self.phase = PersistentPublicationPhase::NotCommitted;
            }
            Err(failure) => self.retain_unknown(&failure.to_string()),
        }
        result
    }

    fn publish_committed(
        &mut self,
        operation_key: CreateOperationKey,
        pending: PendingPublication,
        committed: CommittedStart,
    ) -> (RunInfo, Option<PersistenceError>) {
        debug_assert_eq!(self.phase, PersistentPublicationPhase::CommittedUnpublished);
        let post_commit_error = committed.post_commit_error;
        #[cfg(test)]
        if let Some(hook) = &self.manager.creation_hook {
            hook.capture_run(
                CreationHookPoint::PanicAfterPersistentCommit,
                Arc::clone(pending.run()),
            );
            hook.pause_once(CreationHookPoint::PanicAfterPersistentCommit);
        }
        pending
            .run()
            .install_committed_persistence(committed.durable);
        #[cfg(test)]
        if let Some(hook) = &self.manager.creation_hook {
            hook.capture_run(
                CreationHookPoint::PanicBeforePersistentRegistryConsume,
                Arc::clone(pending.run()),
            );
            hook.pause_once(CreationHookPoint::PanicBeforePersistentRegistryConsume);
        }
        let info = self.manager.registry.publish_creation(
            operation_key,
            pending,
            self.reservation.as_mut(),
        );
        self.phase = PersistentPublicationPhase::Finished;
        (info, post_commit_error)
    }

    fn retain_unknown(&mut self, message: &str) {
        let reservation = self
            .reservation
            .take()
            .and_then(PublicationReservation::into_commit_unknown);
        self.phase = PersistentPublicationPhase::Finished;
        self.manager.fail_stop_persistence(reservation, message);
    }
}

impl Drop for PersistentPublicationOwner<'_> {
    fn drop(&mut self) {
        if let Some(staged) = self.staged.take() {
            match staged.abort() {
                Ok(()) => self.phase = PersistentPublicationPhase::NotCommitted,
                Err(failure) if failure.disposition() == StartDisposition::NotCommitted => {
                    self.phase = PersistentPublicationPhase::NotCommitted;
                    eprintln!("ctxmuxd failed to finish staged persistence rollback: {failure}");
                }
                Err(failure) => self.retain_unknown(&failure.to_string()),
            }
        }
        if matches!(
            self.phase,
            PersistentPublicationPhase::Deciding | PersistentPublicationPhase::CommittedUnpublished
        ) {
            self.retain_unknown("persistent publication unwound after durable disposition began");
        }
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AttachmentHookPoint {
    AfterSubscribe,
    AfterSnapshot,
    BeforeDetachAck,
}

#[cfg(test)]
struct AttachmentTestHook {
    point: AttachmentHookPoint,
    armed: AtomicBool,
    reached: tokio::sync::mpsc::UnboundedSender<()>,
    release: tokio::sync::Notify,
}

#[cfg(test)]
struct CreationTestHook {
    point: CreationHookPoint,
    armed: AtomicBool,
    physical_spawns: AtomicUsize,
    tmux_import_starts: AtomicUsize,
    reached: tokio::sync::mpsc::UnboundedSender<()>,
    released: Mutex<bool>,
    release: std::sync::Condvar,
    captured_runs: Mutex<Vec<Arc<Run>>>,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CreationHookPoint {
    AfterSpawn,
    AfterSpawnWithRunHold,
    PanicAfterSpawn,
    PanicAfterPersistentCommit,
    PanicBeforePersistentRegistryConsume,
    AfterPublication,
}

#[cfg(test)]
impl AttachmentTestHook {
    async fn pause_once(&self, point: AttachmentHookPoint) {
        if self.point != point || !self.armed.swap(false, Ordering::AcqRel) {
            return;
        }
        let _ = self.reached.send(());
        self.release.notified().await;
    }
}

#[cfg(test)]
impl CreationTestHook {
    fn record_physical_spawn(&self) {
        self.physical_spawns.fetch_add(1, Ordering::AcqRel);
    }

    fn physical_spawn_count(&self) -> usize {
        self.physical_spawns.load(Ordering::Acquire)
    }

    fn record_tmux_import_start(&self) {
        self.tmux_import_starts.fetch_add(1, Ordering::AcqRel);
    }

    fn tmux_import_start_count(&self) -> usize {
        self.tmux_import_starts.load(Ordering::Acquire)
    }

    fn capture_run(&self, point: CreationHookPoint, run: Arc<Run>) {
        if self.point == point && self.armed.load(Ordering::Acquire) {
            mutex_lock(&self.captured_runs).push(run);
        }
    }

    fn pause_once(&self, point: CreationHookPoint) {
        if self.point != point || !self.armed.swap(false, Ordering::AcqRel) {
            return;
        }
        let _ = self.reached.send(());
        let mut released = mutex_lock(&self.released);
        while !*released {
            released = self
                .release
                .wait(released)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        assert_ne!(
            point,
            CreationHookPoint::PanicAfterSpawn,
            "injected creation owner panic after physical spawn"
        );
        assert_ne!(
            point,
            CreationHookPoint::PanicAfterPersistentCommit,
            "injected creation owner panic after persistent COMMIT"
        );
        assert_ne!(
            point,
            CreationHookPoint::PanicBeforePersistentRegistryConsume,
            "injected creation owner panic before exact Registry replacement"
        );
    }

    fn release(&self) {
        *mutex_lock(&self.released) = true;
        self.release.notify_one();
    }

    fn arm(&self) {
        *mutex_lock(&self.released) = false;
        self.armed.store(true, Ordering::Release);
    }

    fn release_captured_runs(&self) {
        mutex_lock(&self.captured_runs).clear();
    }

    fn captured_runs_are_backend_quiescent(&self) -> bool {
        mutex_lock(&self.captured_runs).iter().all(|run| {
            run.native_control()
                .is_ok_and(|control| control.closed_quiescence_result().is_ok())
        })
    }
}

impl Default for RunManager {
    fn default() -> Self {
        Self::with_instance_and_stats(DaemonInstanceId::new(), QualificationStats::default())
    }
}

impl RunManager {
    fn with_instance_and_stats(
        daemon_instance: DaemonInstanceId,
        qualification_stats: QualificationStats,
    ) -> Self {
        Self {
            daemon_instance,
            registry: RunRegistry::with_stats(qualification_stats.clone()),
            creation_flights: CreationFlightOwner::with_stats(qualification_stats.clone()),
            unpublished_cleanups: UnpublishedCleanupOwner::with_stats(qualification_stats.clone()),
            terminal_publications: TerminalPublicationOwner::default(),
            native_input_drains: InputDrainGate::with_stats(qualification_stats.clone()),
            native_runs: NativeRuntimeOwner::default(),
            qualification_stats,
            live_event_capacity: LIVE_EVENT_CAPACITY,
            persistence: None,
            commit_unknown_reservations: Mutex::new(Vec::new()),
            incarnation_failure: IncarnationFailure::default(),
            upgrade_requests: UpgradeRequestGate::default(),
            tmux_shutting_down: AtomicBool::new(false),
            tmux_operation_gate: RwLock::new(()),
            #[cfg(test)]
            attachment_hook: None,
            #[cfg(test)]
            creation_hook: None,
        }
    }

    #[cfg(test)]
    fn persistent(persistence: Persistence, recovered: Vec<RecoveredRun>) -> Self {
        Self::persistent_with_stats(persistence, recovered, QualificationStats::default())
    }

    fn persistent_with_stats(
        persistence: Persistence,
        recovered: Vec<RecoveredRun>,
        qualification_stats: QualificationStats,
    ) -> Self {
        let terminal_publications = TerminalPublicationOwner::default();
        let runs = recovered
            .into_iter()
            .map(|recovered| {
                let operation_key = recovered.operation_key.clone();
                let metadata_bytes = recovered.metadata_bytes;
                let durable = persistence.recovered_run(
                    recovered
                        .info
                        .durable_output_bytes
                        .unwrap_or(recovered.info.latest_output_bytes),
                    metadata_bytes,
                );
                let metadata_owner = durable.metadata_bytes_owner();
                (
                    operation_key,
                    Run::recover(
                        recovered,
                        durable,
                        LIVE_EVENT_CAPACITY,
                        terminal_publications.clone(),
                        qualification_stats.clone(),
                    ),
                    metadata_owner,
                )
            })
            .collect();
        Self {
            daemon_instance: persistence.daemon_instance(),
            registry: RunRegistry::recovered_with_stats(runs, qualification_stats.clone()),
            creation_flights: CreationFlightOwner::with_stats(qualification_stats.clone()),
            unpublished_cleanups: UnpublishedCleanupOwner::with_stats(qualification_stats.clone()),
            terminal_publications,
            native_input_drains: InputDrainGate::with_stats(qualification_stats.clone()),
            native_runs: NativeRuntimeOwner::default(),
            qualification_stats,
            live_event_capacity: LIVE_EVENT_CAPACITY,
            persistence: Some(persistence),
            commit_unknown_reservations: Mutex::new(Vec::new()),
            incarnation_failure: IncarnationFailure::default(),
            upgrade_requests: UpgradeRequestGate::default(),
            tmux_shutting_down: AtomicBool::new(false),
            tmux_operation_gate: RwLock::new(()),
            #[cfg(test)]
            attachment_hook: None,
            #[cfg(test)]
            creation_hook: None,
        }
    }

    /// Startup sibling of [`persistent_with_stats`](Self::persistent_with_stats)
    /// used only on the exec-in-place adopt path. Recovered rows whose `run_id`
    /// appears in `adopt` re-bind live native control via [`Run::readopt`]
    /// (consuming the handed-off pty master and child pid); every other row takes
    /// the historical [`Run::recover`] path, byte-identical to the cold-start
    /// method. Fallible because `readopt` can fail to re-adopt a child or master.
    ///
    /// Any `adopt` entry left after the loop (the manifest listed a Run that
    /// persistence did not recover — not expected, since live rows stay in the
    /// recovered set) has its `OwnedFd` dropped and closed at function end. That
    /// is an acceptable fail-safe; no extra validation is added for it.
    #[allow(clippy::too_many_arguments)]
    fn persistent_with_handoff_and_stats(
        persistence: Persistence,
        recovered: Vec<RecoveredRun>,
        qualification_stats: QualificationStats,
        mut adopt: HashMap<RunId, (OwnedFd, u32, HandoffInputState)>,
    ) -> Result<Self, ProtocolError> {
        let terminal_publications = TerminalPublicationOwner::default();
        let native_runs = NativeRuntimeOwner::default();
        let native_input_drains = InputDrainGate::with_stats(qualification_stats.clone());
        let creation_flights = CreationFlightOwner::with_stats(qualification_stats.clone());
        let incarnation_failure = IncarnationFailure::default();
        let mut runs = Vec::with_capacity(recovered.len());
        for recovered in recovered {
            let operation_key = recovered.operation_key.clone();
            let metadata_bytes = recovered.metadata_bytes;
            let durable = persistence.recovered_run(
                recovered
                    .info
                    .durable_output_bytes
                    .unwrap_or(recovered.info.latest_output_bytes),
                metadata_bytes,
            );
            let metadata_owner = durable.metadata_bytes_owner();
            let run = match adopt.remove(&recovered.info.id) {
                Some((master_fd, child_pid, input_state)) => Run::readopt(
                    recovered,
                    durable,
                    master_fd,
                    child_pid,
                    input_state,
                    native_runs.clone(),
                    LIVE_EVENT_CAPACITY,
                    terminal_publications.clone(),
                    qualification_stats.clone(),
                    native_input_drains.clone(),
                    NativeWaitFailure {
                        creation_flights: creation_flights.clone(),
                        incarnation_failure: incarnation_failure.clone(),
                    },
                )?,
                None => Run::recover(
                    recovered,
                    durable,
                    LIVE_EVENT_CAPACITY,
                    terminal_publications.clone(),
                    qualification_stats.clone(),
                ),
            };
            runs.push((operation_key, run, metadata_owner));
        }
        Ok(Self {
            daemon_instance: persistence.daemon_instance(),
            registry: RunRegistry::recovered_with_stats(runs, qualification_stats.clone()),
            creation_flights,
            unpublished_cleanups: UnpublishedCleanupOwner::with_stats(qualification_stats.clone()),
            terminal_publications,
            native_input_drains,
            native_runs,
            qualification_stats,
            live_event_capacity: LIVE_EVENT_CAPACITY,
            persistence: Some(persistence),
            commit_unknown_reservations: Mutex::new(Vec::new()),
            incarnation_failure,
            upgrade_requests: UpgradeRequestGate::default(),
            tmux_shutting_down: AtomicBool::new(false),
            tmux_operation_gate: RwLock::new(()),
            #[cfg(test)]
            attachment_hook: None,
            #[cfg(test)]
            creation_hook: None,
        })
    }

    async fn create(
        self: &Arc<Self>,
        operation_key: CreateOperationKey,
        request: CreationRequest,
    ) -> Result<RunInfo, ProtocolError> {
        operation_key
            .validate()
            .map_err(|error| ProtocolError::new(ErrorCode::InvalidRequest, error.to_string()))?;
        let operation_guard = self.registry.lock_creation(&operation_key).await;
        if let Some(info) = self
            .registry
            .resolve_creation_info(&operation_key, &request)?
        {
            return Ok(info);
        }
        self.unpublished_cleanups
            .resolve_fence(&operation_key, &request)?;
        let materialized = self.materialize_creation(request)?;
        let flight = self.begin_creation_flight().await?;
        let cleanup_reservation = self.unpublished_cleanups.reserve(&operation_key)?;
        let new_run_id = RunId::new();
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let manager = Arc::clone(self);
        thread::Builder::new()
            .name("ctxmux-create".to_owned())
            .spawn(move || {
                let _flight = flight;
                let _operation_guard = operation_guard;
                let result = manager.create_unique(
                    operation_key,
                    materialized,
                    cleanup_reservation,
                    new_run_id,
                );
                let _ = result_tx.send(result);
            })
            .map_err(|error| {
                ProtocolError::new(
                    ErrorCode::Internal,
                    format!("failed to start Run creation owner: {error}"),
                )
            })?;
        let info = result_rx.await.map_err(|error| {
            ProtocolError::new(
                ErrorCode::Internal,
                format!("Run creation owner ended without a result: {error}"),
            )
        })??;
        Ok(info)
    }

    async fn begin_creation_flight(&self) -> Result<CreationFlight, ProtocolError> {
        let admission = self
            .creation_flights
            .acquire_admission()
            .await
            .ok_or_else(|| {
                ProtocolError::new(
                    ErrorCode::BackendUnavailable,
                    "ctxmux daemon is shutting down",
                )
            })?;
        self.creation_flights.try_begin(admission).ok_or_else(|| {
            ProtocolError::new(
                ErrorCode::BackendUnavailable,
                "ctxmux daemon is shutting down",
            )
        })
    }

    fn fail_stop_persistence(&self, reservation: Option<CommitUnknownReservation>, message: &str) {
        if let Some(reservation) = reservation {
            mutex_lock(&self.commit_unknown_reservations).push(reservation);
        }
        self.creation_flights.fence();
        self.incarnation_failure.record(format!(
            "persistent start COMMIT outcome is unknown; restart is required: {message}"
        ));
    }

    fn create_unique(
        &self,
        operation_key: CreateOperationKey,
        materialized: MaterializedCreation,
        cleanup_reservation: UnpublishedCleanupReservation,
        new_run_id: RunId,
    ) -> Result<RunInfo, ProtocolError> {
        let persistence_mode = self.persistence_mode();
        let publication =
            self.prepare_creation_publication(&operation_key, &materialized, new_run_id)?;
        let MaterializedCreation {
            request,
            spec,
            lineage,
        } = materialized;
        let pending = match Run::spawn_pending(
            NativeSpawnConfig {
                id: new_run_id,
                spec,
                lineage,
                persistence_mode,
                live_event_capacity: self.live_event_capacity,
                input_drains: self.native_input_drains.clone(),
                native_runs: self.native_runs.clone(),
                terminal_publications: self.terminal_publications.clone(),
                wait_failure: NativeWaitFailure {
                    creation_flights: self.creation_flights.clone(),
                    incarnation_failure: self.incarnation_failure.clone(),
                },
                qualification_stats: self.qualification_stats.clone(),
            },
            request,
            cleanup_reservation,
        ) {
            Ok(pending) => pending,
            Err(spawn_error) => {
                if let Err(failure) = publication.abort() {
                    return Err(ProtocolError::new(
                        ErrorCode::Persistence,
                        format!("{}; persistent rollback: {failure}", spawn_error.message),
                    ));
                }
                return Err(spawn_error);
            }
        };
        #[cfg(test)]
        if let Some(hook) = &self.creation_hook {
            hook.record_physical_spawn();
        }
        let result = match publication {
            CreationPublication::Memory(reservation) => {
                Ok(self.publish_memory_creation(operation_key, pending, reservation))
            }
            CreationPublication::Persistent(publication) => {
                self.publish_persistent_creation(operation_key, pending, publication)
            }
        };
        #[cfg(test)]
        if let Some(hook) = &self.creation_hook {
            hook.pause_once(CreationHookPoint::AfterPublication);
        }
        result
    }

    fn prepare_creation_publication<'a>(
        &'a self,
        operation_key: &CreateOperationKey,
        materialized: &MaterializedCreation,
        new_run_id: RunId,
    ) -> Result<CreationPublication<'a>, ProtocolError> {
        let Some(persistence) = &self.persistence else {
            let reservation = self
                .registry
                .reserve_memory_publication(new_run_id, Some(operation_key.clone()))?;
            return Ok(CreationPublication::Memory(reservation));
        };
        let prospective = materialized.persistence_start_info(new_run_id);
        let prepared = persistence
            .prepare_start(operation_key, &prospective)
            .map_err(|error| persistence_protocol_error(&error))?;
        let reservation = self.registry.reserve_persistent_publication(
            new_run_id,
            operation_key.clone(),
            materialized.request.clone(),
            prepared.metadata_bytes(),
        )?;
        let candidates = reservation
            .persistent_candidates()
            .into_iter()
            .map(PersistentCandidate::from)
            .collect();
        match persistence.stage_start(prepared, candidates) {
            Ok(staged) => Ok(CreationPublication::Persistent(
                PersistentPublicationOwner::new(self, reservation, staged),
            )),
            Err(failure) => {
                let disposition = failure.disposition();
                let code = if failure.is_capacity() {
                    ErrorCode::RunCapacity
                } else {
                    ErrorCode::Persistence
                };
                let message = failure.into_error().to_string();
                if disposition == StartDisposition::CommitUnknown {
                    self.fail_stop_persistence(reservation.into_commit_unknown(), &message);
                }
                Err(ProtocolError::new(code, message))
            }
        }
    }

    fn publish_memory_creation(
        &self,
        operation_key: CreateOperationKey,
        pending: PendingPublication,
        mut reservation: PublicationReservation,
    ) -> RunInfo {
        #[cfg(test)]
        if let Some(hook) = &self.creation_hook {
            hook.pause_once(CreationHookPoint::AfterSpawn);
            hook.capture_run(
                CreationHookPoint::PanicAfterSpawn,
                Arc::clone(pending.run()),
            );
            hook.pause_once(CreationHookPoint::PanicAfterSpawn);
        }
        self.registry
            .publish_creation(operation_key, pending, Some(&mut reservation))
    }

    fn publish_persistent_creation(
        &self,
        operation_key: CreateOperationKey,
        pending: PendingPublication,
        mut publication: PersistentPublicationOwner<'_>,
    ) -> Result<RunInfo, ProtocolError> {
        debug_assert!(std::ptr::eq(self, publication.manager));
        #[cfg(test)]
        if let Some(hook) = &self.creation_hook {
            hook.capture_run(
                CreationHookPoint::AfterSpawnWithRunHold,
                Arc::clone(pending.run()),
            );
            hook.pause_once(CreationHookPoint::AfterSpawn);
            hook.pause_once(CreationHookPoint::AfterSpawnWithRunHold);
        }
        let committed = match publication.commit() {
            PersistentStartCompletion::Committed(committed) => committed,
            PersistentStartCompletion::NotCommitted(failure) => {
                return cleanup_failed_persistent_creation(pending, failure);
            }
            PersistentStartCompletion::CommitUnknown(failure) => {
                let message = failure.into_error().to_string();
                publication.retain_unknown(&message);
                return cleanup_unknown_persistent_creation(pending, message);
            }
        };
        let (info, post_commit_error) =
            publication.publish_committed(operation_key, pending, committed);
        let post_commit_error = post_commit_error
            .map(|error| ProtocolError::new(ErrorCode::Persistence, error.to_string()));
        post_commit_error.map_or(Ok(info), Err)
    }

    #[cfg(test)]
    fn start(&self, spec: RunSpec) -> Result<RunInfo, ProtocolError> {
        let operation_key = CreateOperationKey::random();
        let request = CreationRequest::Start { spec };
        self.unpublished_cleanups
            .resolve_fence(&operation_key, &request)?;
        let materialized = self.materialize_creation(request)?;
        let cleanup_reservation = self.unpublished_cleanups.reserve(&operation_key)?;
        let new_run_id = RunId::new();
        self.create_unique(operation_key, materialized, cleanup_reservation, new_run_id)
    }

    #[cfg(test)]
    fn fork(&self, parent: RunId, plan: ForkPlan) -> Result<RunInfo, ProtocolError> {
        let operation_key = CreateOperationKey::random();
        let request = CreationRequest::Fork { parent, plan };
        self.unpublished_cleanups
            .resolve_fence(&operation_key, &request)?;
        let materialized = self.materialize_creation(request)?;
        let cleanup_reservation = self.unpublished_cleanups.reserve(&operation_key)?;
        let new_run_id = RunId::new();
        self.create_unique(operation_key, materialized, cleanup_reservation, new_run_id)
    }

    fn materialize_creation(
        &self,
        request: CreationRequest,
    ) -> Result<MaterializedCreation, ProtocolError> {
        let (spec, lineage) = match &request {
            CreationRequest::Start { spec } => (spec.clone(), None),
            CreationRequest::Fork { parent, plan } => {
                let parent_run = self.pin(*parent)?;
                let (spec, fidelity) = match plan {
                    ForkPlan::LevelA if parent_run.capabilities.fork_level_a => (
                        parent_run.spec.clone().ok_or_else(|| {
                            ProtocolError::new(
                                ErrorCode::UnsupportedCapability,
                                format!("Run {parent} has no portable launch specification"),
                            )
                        })?,
                        ForkFidelity::LevelA,
                    ),
                    ForkPlan::LevelB { spec } if parent_run.capabilities.fork_level_b => {
                        if !parent_run.has_continuation_authority() {
                            return Err(ProtocolError::new(
                                ErrorCode::InvalidRunState,
                                format!(
                                    "cannot Level B fork Run {parent} without live continuation authority"
                                ),
                            ));
                        }
                        (spec.clone(), ForkFidelity::LevelB)
                    }
                    ForkPlan::LevelA | ForkPlan::LevelB { .. } => {
                        return Err(ProtocolError::new(
                            ErrorCode::UnsupportedCapability,
                            format!("Run {parent} backend does not support the requested fork"),
                        ));
                    }
                };
                (
                    spec,
                    Some(RunLineage {
                        parent: *parent,
                        fidelity,
                    }),
                )
            }
        };
        validate_run_spec(&spec).map_err(invalid_run_spec)?;
        Ok(MaterializedCreation {
            request,
            spec,
            lineage,
        })
    }

    #[cfg(test)]
    fn reserve_memory_publication(
        &self,
        new_run_id: RunId,
        operation_key: Option<CreateOperationKey>,
    ) -> Result<Option<PublicationReservation>, ProtocolError> {
        if self.persistence_mode() == PersistenceMode::MemoryOnly {
            self.registry
                .reserve_memory_publication(new_run_id, operation_key)
                .map(Some)
        } else {
            Ok(None)
        }
    }

    fn with_tmux_operation<T>(
        &self,
        operation: impl FnOnce() -> Result<T, ProtocolError>,
    ) -> Result<T, ProtocolError> {
        if self.tmux_shutting_down.load(Ordering::Acquire) {
            return Err(ProtocolError::new(
                ErrorCode::BackendUnavailable,
                "ctxmux daemon is shutting down",
            ));
        }
        let _operation_guard = read_lock(&self.tmux_operation_gate);
        if self.tmux_shutting_down.load(Ordering::Acquire) {
            return Err(ProtocolError::new(
                ErrorCode::BackendUnavailable,
                "ctxmux daemon is shutting down",
            ));
        }
        operation()
    }

    fn discover_tmux(&self, socket_path: &str) -> Result<tmux::TmuxDiscovery, ProtocolError> {
        self.with_tmux_operation(|| {
            tmux::discover(socket_path, Instant::now() + TMUX_DISCOVERY_TIMEOUT)
        })
    }

    fn import_tmux(
        &self,
        socket_path: &str,
        pane_id: &str,
        _flight: CreationFlight,
    ) -> Result<RunInfo, ProtocolError> {
        self.ensure_tmux_import_supported()?;
        self.with_tmux_operation(|| {
            let cleanup_reservation = self.unpublished_cleanups.reserve_tmux()?;
            let new_run_id = RunId::new();
            let registry_reservation =
                self.registry.reserve_memory_publication(new_run_id, None)?;
            let started_at = Instant::now();
            #[cfg(test)]
            if let Some(hook) = &self.creation_hook {
                hook.record_tmux_import_start();
            }
            let pending = Run::import_tmux(
                socket_path,
                pane_id,
                TmuxImportConfig {
                    id: new_run_id,
                    live_event_capacity: self.live_event_capacity,
                    terminal_publications: self.terminal_publications.clone(),
                    discovery_deadline: started_at + TMUX_IMPORT_DISCOVERY_TIMEOUT,
                    prepare_deadline: started_at + TMUX_IMPORT_PREPARE_TIMEOUT,
                    total_deadline: started_at + TMUX_IMPORT_TOTAL_TIMEOUT,
                    qualification_stats: self.qualification_stats.clone(),
                },
                cleanup_reservation,
            )?;
            let info = pending.run().info();
            let (run, cleanup_reservation) = pending.into_published();
            self.registry.publish_unkeyed(run, registry_reservation);
            drop(cleanup_reservation);
            Ok(info)
        })
    }

    fn ensure_tmux_import_supported(&self) -> Result<(), ProtocolError> {
        if self.persistence.is_none() {
            return Ok(());
        }
        Err(ProtocolError::new(
            ErrorCode::UnsupportedCapability,
            "tmux pane import is not persisted; use a memory-only ctxmux daemon",
        ))
    }

    fn persistence_barrier(&self) -> Result<(), ServerError> {
        match &self.persistence {
            Some(persistence) => persistence.barrier().map_err(ServerError::from),
            None => Ok(()),
        }
    }

    fn shutdown_owned_controls(&self, timeout: Duration) -> Result<(), ServerError> {
        let deadline = Instant::now() + timeout;
        self.creation_flights.fence();
        self.tmux_shutting_down.store(true, Ordering::Release);

        let mut failures = Vec::new();
        let operation_guard = loop {
            match self.tmux_operation_gate.try_write() {
                Ok(guard) => break Some(guard),
                Err(std::sync::TryLockError::Poisoned(error)) => {
                    break Some(error.into_inner());
                }
                Err(std::sync::TryLockError::WouldBlock) => {
                    let now = Instant::now();
                    if now >= deadline {
                        failures.push(
                            "timed out waiting for an in-flight tmux operation to finish"
                                .to_owned(),
                        );
                        break None;
                    }
                    thread::sleep(CHILD_CONTROL_POLL.min(deadline.saturating_duration_since(now)));
                }
            }
        };

        let mut pending = self.registry.pin_tmux_for_shutdown();

        for run in &pending {
            if let Some(RunControl::Tmux(control)) = &run.incarnation_control {
                // A failed send can race a naturally completed waiter. Its
                // completion receipt, not channel state, is authoritative.
                let _ = control.commands.send(TmuxControlCommand::Shutdown);
            }
        }
        drop(operation_guard);

        while !pending.is_empty() {
            let mut index = 0;
            while index < pending.len() {
                let run = Arc::clone(&pending[index]);
                let Some(RunControl::Tmux(control)) = &run.incarnation_control else {
                    pending.swap_remove(index);
                    continue;
                };
                match control.observe_completion() {
                    TmuxCompletionObservation::Complete(Ok(())) => {
                        pending.swap_remove(index);
                    }
                    TmuxCompletionObservation::Complete(Err(error)) => {
                        failures.push(format!("Run {}: {error}", run.id));
                        pending.swap_remove(index);
                    }
                    TmuxCompletionObservation::Pending => {
                        index += 1;
                    }
                }
            }

            if pending.is_empty() {
                break;
            }
            let now = Instant::now();
            if now >= deadline {
                for run in pending.drain(..) {
                    failures.push(format!(
                        "Run {}: timed out waiting for tmux control cleanup",
                        run.id
                    ));
                }
                break;
            }
            thread::sleep(CHILD_CONTROL_POLL.min(deadline.saturating_duration_since(now)));
        }

        if !self.creation_flights.wait_until(deadline) {
            failures.push("timed out waiting for in-flight Run creation to finish".to_owned());
        }
        failures.extend(
            self.unpublished_cleanups
                .wait_until(deadline)
                .into_iter()
                .map(|failure| format!("unpublished Run cleanup {failure}")),
        );
        failures.extend(
            self.registry
                .native_wait_failures()
                .into_iter()
                .map(|(id, failure)| format!("Run {id}: {failure}")),
        );

        if failures.is_empty() {
            Ok(())
        } else {
            failures.sort();
            Err(ServerError::Shutdown {
                failures: failures.join("; "),
            })
        }
    }

    fn pin(&self, id: RunId) -> Result<Arc<Run>, ProtocolError> {
        self.registry.pin(id)?.ok_or_else(|| {
            ProtocolError::new(ErrorCode::RunNotFound, format!("Run {id} does not exist"))
        })
    }

    fn info(&self, id: RunId) -> Result<RunInfo, ProtocolError> {
        self.registry.info(id).ok_or_else(|| {
            ProtocolError::new(ErrorCode::RunNotFound, format!("Run {id} does not exist"))
        })
    }

    #[cfg(test)]
    fn get(&self, id: RunId) -> Result<Arc<Run>, ProtocolError> {
        self.pin(id)
    }

    fn list(&self) -> Vec<RunInfo> {
        let mut runs = self.registry.list_infos();
        runs.sort_by_key(|run| run.id.to_string());
        runs
    }

    const fn persistence_mode(&self) -> PersistenceMode {
        if self.persistence.is_some() {
            PersistenceMode::PersistentCapable
        } else {
            PersistenceMode::MemoryOnly
        }
    }

    #[cfg(test)]
    fn start_with_setup<F>(
        &self,
        operation_key: CreateOperationKey,
        spec: RunSpec,
        captured_run: &Arc<Mutex<Option<Arc<Run>>>>,
        setup: F,
    ) -> Result<RunInfo, ProtocolError>
    where
        F: FnMut(LaunchSetupStep, Option<u32>) -> Result<(), ProtocolError>,
    {
        let request = CreationRequest::Start { spec: spec.clone() };
        let cleanup_reservation = self.unpublished_cleanups.reserve(&operation_key)?;
        let new_run_id = RunId::new();
        let mut registry_reservation =
            self.reserve_memory_publication(new_run_id, Some(operation_key.clone()))?;
        let pending = Run::spawn_pending_with_setup(
            NativeSpawnConfig {
                id: new_run_id,
                spec,
                lineage: None,
                persistence_mode: self.persistence_mode(),
                live_event_capacity: LIVE_EVENT_CAPACITY,
                input_drains: InputDrainGate::default(),
                native_runs: self.native_runs.clone(),
                terminal_publications: self.terminal_publications.clone(),
                wait_failure: NativeWaitFailure::default(),
                qualification_stats: self.qualification_stats.clone(),
            },
            request,
            cleanup_reservation,
            captured_run,
            setup,
        )?;
        Ok(self
            .registry
            .publish_creation(operation_key, pending, registry_reservation.as_mut()))
    }

    #[cfg(test)]
    fn start_with_wait_hook<G>(
        &self,
        spec: RunSpec,
        after_wait: G,
    ) -> Result<RunInfo, ProtocolError>
    where
        G: FnOnce() + Send + 'static,
    {
        let new_run_id = RunId::new();
        let reservation = self.registry.reserve_memory_publication(new_run_id, None)?;
        let run = Run::spawn_with_wait_hook_owner(
            new_run_id,
            spec,
            self.persistence_mode(),
            self.terminal_publications.clone(),
            after_wait,
        )?;
        let info = run.info();
        self.registry.publish_unkeyed(run, reservation);
        Ok(info)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LaunchSetupStep {
    CloneReader,
    TakeWriter,
    RegisterOutputOwner,
    RegisterWaitOwner,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PersistenceMode {
    MemoryOnly,
    PersistentCapable,
}

struct NativeSpawnConfig {
    id: RunId,
    spec: RunSpec,
    lineage: Option<RunLineage>,
    persistence_mode: PersistenceMode,
    live_event_capacity: usize,
    input_drains: InputDrainGate,
    native_runs: NativeRuntimeOwner,
    terminal_publications: TerminalPublicationOwner,
    wait_failure: NativeWaitFailure,
    qualification_stats: QualificationStats,
}

struct MaterializedCreation {
    request: CreationRequest,
    spec: RunSpec,
    lineage: Option<RunLineage>,
}

impl MaterializedCreation {
    fn persistence_start_info(&self, id: RunId) -> RunInfo {
        RunInfo {
            id,
            spec: Some(self.spec.clone()),
            lineage: self.lineage.clone(),
            backend: RunBackend::Native,
            capabilities: RunCapabilities::NATIVE,
            pid: None,
            state: RunState::Running,
            latest_output_bytes: 0,
            durable_output_bytes: Some(0),
            first_available_byte: 0,
            attachments: 0,
            applied_input_bytes: Some(0),
        }
    }
}

impl From<PersistentCollectionCandidate> for PersistentCandidate {
    fn from(candidate: PersistentCollectionCandidate) -> Self {
        Self::new(
            candidate.id,
            candidate.operation_key,
            candidate.metadata_bytes,
        )
    }
}

struct TmuxImportConfig {
    id: RunId,
    live_event_capacity: usize,
    terminal_publications: TerminalPublicationOwner,
    discovery_deadline: Instant,
    prepare_deadline: Instant,
    total_deadline: Instant,
    qualification_stats: QualificationStats,
}

#[must_use = "a started tmux Control owner must be published or transferred for cleanup"]
struct PendingTmuxPublication {
    run: Option<Arc<Run>>,
    cleanup_reservation: Option<TmuxCleanupReservation>,
}

impl PendingTmuxPublication {
    fn new(run: Arc<Run>, cleanup_reservation: TmuxCleanupReservation) -> Self {
        Self {
            run: Some(run),
            cleanup_reservation: Some(cleanup_reservation),
        }
    }

    fn run(&self) -> &Arc<Run> {
        self.run
            .as_ref()
            .expect("pending tmux publication retains its Run")
    }

    fn into_published(mut self) -> (Arc<Run>, TmuxCleanupReservation) {
        let run = self
            .run
            .take()
            .expect("tmux publication consumes one pending Run");
        let cleanup_reservation = self
            .cleanup_reservation
            .take()
            .expect("tmux publication consumes one cleanup reservation");
        (run, cleanup_reservation)
    }

    fn transfer(&mut self, transfer_reason: String) {
        let run = self
            .run
            .take()
            .expect("tmux publication transfers its Run at most once");
        self.cleanup_reservation
            .take()
            .expect("tmux publication transfers its cleanup reservation at most once")
            .transfer(run, transfer_reason);
    }
}

impl Drop for PendingTmuxPublication {
    fn drop(&mut self) {
        let Some(run) = self.run.as_ref() else {
            return;
        };
        run.request_tmux_import_cleanup();
        self.transfer("tmux import owner unwound before publication".to_owned());
    }
}

impl NativeSpawnConfig {
    fn command(&self) -> CommandBuilder {
        let mut command = CommandBuilder::new(&self.spec.program);
        command.args(&self.spec.args);
        if let Some(cwd) = &self.spec.cwd {
            command.cwd(cwd);
        }
        for (name, value) in native_spawn_env::with_native_terminal_identity(&self.spec.env) {
            command.env(name, value);
        }
        command
    }
}

struct PendingChild {
    child: Option<Box<dyn Child + Send + Sync>>,
    reap_control: Option<NativeControlOwner>,
}

struct ObservedChild {
    child: Box<dyn Child + Send + Sync>,
    _qualification_guard: crate::qualification_stats::GaugeGuard,
}

impl fmt::Debug for ObservedChild {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ObservedChild")
            .finish_non_exhaustive()
    }
}

impl ChildKiller for ObservedChild {
    fn kill(&mut self) -> io::Result<()> {
        self.child.kill()
    }

    fn clone_killer(&self) -> Box<dyn ChildKiller + Send + Sync> {
        self.child.clone_killer()
    }
}

impl Child for ObservedChild {
    fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        self.child.try_wait()
    }

    fn wait(&mut self) -> io::Result<ExitStatus> {
        self.child.wait()
    }

    fn process_id(&self) -> Option<u32> {
        self.child.process_id()
    }
}

impl PendingChild {
    const fn new(child: Box<dyn Child + Send + Sync>) -> Self {
        Self {
            child: Some(child),
            reap_control: None,
        }
    }

    fn child(&self) -> &(dyn Child + Send + Sync) {
        self.child.as_deref().expect("pending child is present")
    }

    fn bind_reap_control(&mut self, control: NativeControlOwner) {
        debug_assert!(self.reap_control.is_none());
        self.reap_control = Some(control);
    }

    fn into_child(mut self) -> Box<dyn Child + Send + Sync> {
        self.reap_control.take();
        self.child.take().expect("pending child is present")
    }
}

impl Drop for PendingChild {
    fn drop(&mut self) {
        let Some(mut child) = self.child.take() else {
            return;
        };
        if let Err(error) = child.kill() {
            eprintln!("ctxmuxd failed to terminate rejected child: {error}");
            if let Some(control) = &self.reap_control {
                control.record_cleanup_error(format!(
                    "failed to terminate rejected unpublished child: {error}"
                ));
            }
        }
        match child.wait() {
            Ok(_) => {
                if let Some(control) = &self.reap_control {
                    control.mark_reaped();
                }
            }
            Err(error) => {
                eprintln!("ctxmuxd failed to reap rejected child: {error}");
                if let Some(control) = &self.reap_control {
                    control.record_wait_error(format!(
                        "failed to reap rejected unpublished child: {error}"
                    ));
                }
            }
        }
    }
}

struct Run {
    id: RunId,
    spec: Option<RunSpec>,
    lineage: Option<RunLineage>,
    backend: RunBackend,
    capabilities: RunCapabilities,
    pid: Option<u32>,
    state: Mutex<RunState>,
    output: Mutex<OutputLog>,
    incarnation_control: Option<RunControl>,
    native_runs: Option<NativeRuntimeOwner>,
    persistence_mode: PersistenceMode,
    persistence_transition: Mutex<()>,
    persistence: Mutex<PersistenceBinding>,
    attachments: AtomicUsize,
    qualification_stats: QualificationStats,
    terminal_publications: TerminalPublicationOwner,
    terminal_ordinal: OnceLock<TerminalOrdinal>,
    events: LiveEventOwner,
}

struct LiveEventOwner {
    capacity: usize,
    sender: Mutex<Option<broadcast::Sender<RunEvent>>>,
}

impl LiveEventOwner {
    const fn new(capacity: usize) -> Self {
        Self {
            capacity,
            sender: Mutex::new(None),
        }
    }
}

/// Native persistence publication stays private until both durable COMMIT and
/// Registry publication have completed. A fast waiter deposits its terminal
/// result here instead of making an unpublished Run externally terminal.
enum PersistenceBinding {
    Disabled,
    Pending {
        terminal: Option<RunState>,
    },
    CommittedPendingActivation {
        durable: PersistentRun,
        terminal: Option<RunState>,
    },
    Active(PersistentRun),
}

impl PersistenceBinding {
    fn durable(&self) -> Option<&PersistentRun> {
        match self {
            Self::CommittedPendingActivation { durable, .. } | Self::Active(durable) => {
                Some(durable)
            }
            Self::Disabled | Self::Pending { .. } => None,
        }
    }

    fn active(&self) -> Option<&PersistentRun> {
        match self {
            Self::Active(durable) => Some(durable),
            Self::Disabled | Self::Pending { .. } | Self::CommittedPendingActivation { .. } => None,
        }
    }
}

/// Private owner shape used while native setup can still fail or unwind.
///
/// Production creation supplies `PendingPublication`; the plain `Arc` owner is
/// retained only by test seams that never carry a public operation key.
trait NativeRunOwner {
    fn run(&self) -> &Arc<Run>;
}

impl NativeRunOwner for Arc<Run> {
    fn run(&self) -> &Arc<Run> {
        self
    }
}

impl NativeRunOwner for PendingPublication {
    fn run(&self) -> &Arc<Run> {
        self.run()
    }
}

enum RunControl {
    Native(NativeControlOwner),
    Tmux(TmuxRunControl),
}

struct TmuxRunControl {
    writer: Mutex<Option<TmuxCommandWriter>>,
    commands: mpsc::Sender<TmuxControlCommand>,
    completion: Mutex<TmuxCompletion>,
}

enum TmuxCompletion {
    Pending(mpsc::Receiver<Result<(), String>>),
    Complete(Result<(), String>),
}

enum TmuxCompletionObservation {
    Pending,
    Complete(Result<(), String>),
}

struct TmuxCommandWriter {
    stdin: std::process::ChildStdin,
    tracker: TmuxCommandTracker,
}

#[derive(Default)]
struct TmuxCommandTracker {
    session_established: bool,
    bootstrap_result_seen: bool,
    last_result_number: Option<u64>,
    pending: VecDeque<TmuxCommandKind>,
}

impl TmuxRunControl {
    fn with_writer<T>(
        &self,
        operation: impl FnOnce(&mut TmuxCommandWriter) -> io::Result<T>,
    ) -> io::Result<T> {
        let mut writer = mutex_lock(&self.writer);
        let writer = writer.as_mut().ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotConnected, "tmux control client is closed")
        })?;
        operation(writer)
    }

    fn correlate_result(&self, number: u64) -> Result<TmuxCommandResultKind, &'static str> {
        let mut writer = mutex_lock(&self.writer);
        let writer = writer.as_mut().ok_or("tmux control client is closed")?;
        writer.tracker.correlate_result(number)
    }

    fn close_writer(&self) -> bool {
        mutex_lock(&self.writer).take().is_some()
    }

    fn observe_completion(&self) -> TmuxCompletionObservation {
        mutex_lock(&self.completion).observe()
    }

    fn wait_for_completion(&self, timeout: Duration) -> Result<(), String> {
        mutex_lock(&self.completion).wait(timeout)
    }

    fn closed_quiescence_result(&self) -> Result<(), String> {
        if mutex_lock(&self.writer).is_some() {
            return Err("tmux control writer is still open".to_owned());
        }
        match self.observe_completion() {
            TmuxCompletionObservation::Complete(Ok(())) => Ok(()),
            TmuxCompletionObservation::Complete(Err(error)) => Err(error),
            TmuxCompletionObservation::Pending => {
                Err("tmux control cleanup is still pending".to_owned())
            }
        }
    }
}

impl TmuxCompletion {
    fn observe(&mut self) -> TmuxCompletionObservation {
        let observed = match self {
            Self::Pending(receiver) => match receiver.try_recv() {
                Ok(result) => Some(result),
                Err(mpsc::TryRecvError::Empty) => None,
                Err(mpsc::TryRecvError::Disconnected) => Some(Err(
                    "tmux control waiter ended without a completion receipt".to_owned(),
                )),
            },
            Self::Complete(result) => return TmuxCompletionObservation::Complete(result.clone()),
        };
        let Some(result) = observed else {
            return TmuxCompletionObservation::Pending;
        };
        *self = Self::Complete(result.clone());
        TmuxCompletionObservation::Complete(result)
    }

    fn wait(&mut self, timeout: Duration) -> Result<(), String> {
        let received = match self {
            Self::Pending(receiver) => match receiver.recv_timeout(timeout) {
                Ok(result) => result,
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    return Err("timed out waiting for tmux control cleanup".to_owned());
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    Err("tmux control waiter ended without a completion receipt".to_owned())
                }
            },
            Self::Complete(result) => return result.clone(),
        };
        *self = Self::Complete(received.clone());
        received
    }
}

impl TmuxCommandWriter {
    fn new(stdin: std::process::ChildStdin) -> Self {
        Self {
            stdin,
            tracker: TmuxCommandTracker::default(),
        }
    }

    fn establish_session_and_write(
        &mut self,
        kind: TmuxCommandKind,
        command: &[u8],
    ) -> io::Result<()> {
        if !self.tracker.observe_session() {
            return Ok(());
        }
        self.write_command(kind, command)
    }

    fn write_command(&mut self, kind: TmuxCommandKind, command: &[u8]) -> io::Result<()> {
        if !self
            .tracker
            .prepare_enqueue(kind)
            .map_err(|message| io::Error::new(io::ErrorKind::InvalidData, message))?
        {
            return Ok(());
        }
        self.stdin.write_all(command)?;
        self.stdin.flush()?;
        self.tracker.commit_enqueue(kind);
        Ok(())
    }

    fn write_periodic_probe(&mut self, command: &[u8]) -> io::Result<()> {
        if !self.tracker.session_established {
            return Ok(());
        }
        self.write_command(TmuxCommandKind::TargetProbe, command)
    }
}

impl TmuxCommandTracker {
    const MAX_PENDING: usize = 2;

    fn observe_session(&mut self) -> bool {
        if self.session_established {
            false
        } else {
            self.session_established = true;
            true
        }
    }

    fn prepare_enqueue(&self, kind: TmuxCommandKind) -> Result<bool, &'static str> {
        if !self.session_established {
            return Err("tmux adapter command arrived before session establishment");
        }
        if self.pending.contains(&kind) {
            return Ok(false);
        }
        if self.pending.len() >= Self::MAX_PENDING {
            return Err("tmux adapter command queue exceeded its bound");
        }
        Ok(true)
    }

    fn commit_enqueue(&mut self, kind: TmuxCommandKind) {
        debug_assert!(self.session_established);
        debug_assert!(!self.pending.contains(&kind));
        debug_assert!(self.pending.len() < Self::MAX_PENDING);
        self.pending.push_back(kind);
    }

    fn correlate_result(&mut self, number: u64) -> Result<TmuxCommandResultKind, &'static str> {
        if self.last_result_number.is_some_and(|last| number <= last) {
            return Err("tmux command result number did not advance");
        }
        self.last_result_number = Some(number);

        if let Some(kind) = self.pending.pop_front() {
            return Ok(TmuxCommandResultKind::Pending(kind));
        }
        if !self.session_established && !self.bootstrap_result_seen {
            self.bootstrap_result_seen = true;
            return Ok(TmuxCommandResultKind::Bootstrap);
        }
        Err("tmux returned a command result without a pending adapter command")
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TmuxCommandKind {
    TargetProbe,
    Continue,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TmuxCommandResultKind {
    Bootstrap,
    Pending(TmuxCommandKind),
}

enum TmuxControlCommand {
    Interrupt(InterruptionReason),
    ReaderTerminated,
    Shutdown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TmuxTermination {
    error: ProtocolError,
    reason: InterruptionReason,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TmuxReaderTermination {
    failure: TmuxTermination,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TmuxWaitCause {
    ReaderTerminated,
    Interrupted(InterruptionReason),
    Shutdown,
    CommandChannelClosed,
    SocketTargetChanged,
    ProbeWriteFailed(String),
    ChildExited,
    ChildStatusFailed(String),
}

struct TmuxWaitOutcome {
    cause: TmuxWaitCause,
    cleanup: Result<(), String>,
}

impl Run {
    #[cfg(test)]
    fn new_native_for_owner_test(
        id: RunId,
        control: NativeControlOwner,
        native_runs: NativeRuntimeOwner,
        wait_failure: NativeWaitFailure,
    ) -> Arc<Self> {
        Self::new_native(
            NativeSpawnConfig {
                id,
                spec: RunSpec {
                    program: "/bin/cat".to_owned(),
                    args: Vec::new(),
                    cwd: None,
                    env: std::collections::BTreeMap::new(),
                    size: TerminalSize::default(),
                    declared_inputs: Vec::new(),
                },
                lineage: None,
                persistence_mode: PersistenceMode::MemoryOnly,
                live_event_capacity: LIVE_EVENT_CAPACITY,
                input_drains: InputDrainGate::default(),
                native_runs,
                terminal_publications: TerminalPublicationOwner::default(),
                wait_failure,
                qualification_stats: QualificationStats::default(),
            },
            id,
            Some(42),
            control,
        )
    }

    #[cfg(test)]
    fn spawn(
        spec: RunSpec,
        lineage: Option<RunLineage>,
        persistence_mode: PersistenceMode,
        live_event_capacity: usize,
        input_drains: InputDrainGate,
    ) -> Result<Arc<Self>, ProtocolError> {
        Self::spawn_with_hooks(
            NativeSpawnConfig {
                id: RunId::new(),
                spec,
                lineage,
                persistence_mode,
                live_event_capacity,
                input_drains,
                native_runs: NativeRuntimeOwner::default(),
                terminal_publications: TerminalPublicationOwner::default(),
                wait_failure: NativeWaitFailure::default(),
                qualification_stats: QualificationStats::default(),
            },
            |run| run,
            |_, _| Ok(()),
            || {},
        )
    }

    #[cfg(test)]
    fn spawn_pending_with_setup<F>(
        config: NativeSpawnConfig,
        request: CreationRequest,
        cleanup_reservation: UnpublishedCleanupReservation,
        captured_run: &Arc<Mutex<Option<Arc<Run>>>>,
        setup: F,
    ) -> Result<PendingPublication, ProtocolError>
    where
        F: FnMut(LaunchSetupStep, Option<u32>) -> Result<(), ProtocolError>,
    {
        Self::spawn_with_hooks(
            config,
            |run| {
                let previous = mutex_lock(captured_run).replace(Arc::clone(&run));
                assert!(previous.is_none(), "setup fixture captures one Run owner");
                PendingPublication::new(request, run, cleanup_reservation)
            },
            setup,
            || {},
        )
    }

    #[cfg(test)]
    fn spawn_with_wait_hook<G>(
        spec: RunSpec,
        persistence_mode: PersistenceMode,
        after_wait: G,
    ) -> Result<Arc<Self>, ProtocolError>
    where
        G: FnOnce() + Send + 'static,
    {
        Self::spawn_with_wait_hook_owner(
            RunId::new(),
            spec,
            persistence_mode,
            TerminalPublicationOwner::default(),
            after_wait,
        )
    }

    #[cfg(test)]
    fn spawn_with_wait_hook_owner<G>(
        id: RunId,
        spec: RunSpec,
        persistence_mode: PersistenceMode,
        terminal_publications: TerminalPublicationOwner,
        after_wait: G,
    ) -> Result<Arc<Self>, ProtocolError>
    where
        G: FnOnce() + Send + 'static,
    {
        Self::spawn_with_hooks(
            NativeSpawnConfig {
                id,
                spec,
                lineage: None,
                persistence_mode,
                live_event_capacity: LIVE_EVENT_CAPACITY,
                input_drains: InputDrainGate::default(),
                native_runs: NativeRuntimeOwner::default(),
                terminal_publications,
                wait_failure: NativeWaitFailure::default(),
                qualification_stats: QualificationStats::default(),
            },
            |run| run,
            |_, _| Ok(()),
            after_wait,
        )
    }

    fn spawn_pending(
        config: NativeSpawnConfig,
        request: CreationRequest,
        cleanup_reservation: UnpublishedCleanupReservation,
    ) -> Result<PendingPublication, ProtocolError> {
        Self::spawn_with_hooks(
            config,
            |run| PendingPublication::new(request, run, cleanup_reservation),
            |_, _| Ok(()),
            || {},
        )
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one linear launch transaction keeps fallible setup and child-owner handoff auditable"
    )]
    fn spawn_with_hooks<O, H, F, G>(
        config: NativeSpawnConfig,
        make_owner: H,
        mut setup: F,
        after_wait: G,
    ) -> Result<O, ProtocolError>
    where
        O: NativeRunOwner,
        H: FnOnce(Arc<Self>) -> O,
        F: FnMut(LaunchSetupStep, Option<u32>) -> Result<(), ProtocolError>,
        G: FnOnce() + Send + 'static,
    {
        validate_run_spec(&config.spec).map_err(invalid_run_spec)?;
        let qualification_stats = config.qualification_stats.clone();
        let pair = native_pty_system()
            .openpty(to_pty_size(config.spec.size))
            .map_err(|error| spawn_error("open PTY", error))?;
        // Prepare every fallible PTY view before physical launch. Once a child
        // exists, native control and PendingPublication can be built without a
        // setup error window that lacks exact-key cleanup ownership.
        setup(LaunchSetupStep::CloneReader, None)?;
        let reader_fd = pair.master.as_raw_fd().ok_or_else(|| {
            spawn_error(
                "identify PTY reader",
                "native PTY master does not expose a raw descriptor",
            )
        })?;
        let reader = fs::File::from(
            ctxmux_inherited_fd::duplicate_cloexec(reader_fd)
                .map_err(|error| spawn_error("clone PTY reader", error))?,
        );
        setup(LaunchSetupStep::TakeWriter, None)?;
        let writer = pair
            .master
            .take_writer()
            .map_err(|error| spawn_error("take PTY writer", error))?;
        let child = pair
            .slave
            .spawn_command(config.command())
            .map_err(|error| spawn_error("spawn child", error))?;
        qualification_stats.record_physical_start();
        let child: Box<dyn Child + Send + Sync> = Box::new(ObservedChild {
            child,
            _qualification_guard: qualification_stats.guard(QualificationGauge::DirectChildren),
        });
        drop(pair.slave);
        let mut pending_child = PendingChild::new(child);
        let pid = pending_child.child().process_id();
        let session =
            NativeSession::from_child_pid(pid.ok_or_else(|| {
                spawn_error("identify native session", "child PID is unavailable")
            })?)
            .map_err(|error| spawn_error("identify native session", error))?;
        let id = config.id;
        let owner_wake = config.native_runs.owner_wake();
        let native_control = NativeControlOwner::new(
            id,
            pair.master,
            writer,
            config.input_drains.clone(),
            owner_wake,
        );
        pending_child.bind_reap_control(native_control.clone());
        let wait_failure = config.wait_failure.clone();
        let owner = make_owner(Self::new_native(config, id, pid, native_control));
        let run = Arc::clone(owner.run());
        let registration_control = run
            .native_control()
            .expect("spawned Run retains native control")
            .clone();

        // Both fallible owner-registration seams run before the single atomic
        // handoff. Any injected failure therefore leaves `PendingChild` as the
        // synchronous kill/reap owner and publishes no partial reactor entry.
        for step in [
            LaunchSetupStep::RegisterWaitOwner,
            LaunchSetupStep::RegisterOutputOwner,
        ] {
            if let Err(error) = setup(step, pid) {
                registration_control.mark_closed();
                drop(pending_child);
                return Err(error);
            }
        }
        let reader_guard = qualification_stats.guard(QualificationGauge::Readers);
        let waiter_guard = qualification_stats.guard(QualificationGauge::Waiters);
        let native_runs = run
            .native_runs
            .as_ref()
            .expect("native Run retains its daemon-wide owner")
            .clone();
        let registration = NativeRunRegistration::new(
            &run,
            reader,
            pending_child,
            session,
            registration_control,
            wait_failure,
            after_wait,
            reader_guard,
            waiter_guard,
        );
        native_runs.register(registration).map_err(|error| {
            let (message, registration) = error.into_parts();
            drop(registration);
            spawn_error("register native Run owner", message)
        })?;

        Ok(owner)
    }

    fn new_native(
        config: NativeSpawnConfig,
        id: RunId,
        pid: Option<u32>,
        native_control: NativeControlOwner,
    ) -> Arc<Self> {
        Arc::new(Self {
            id,
            spec: Some(config.spec),
            lineage: config.lineage,
            backend: RunBackend::Native,
            capabilities: RunCapabilities::NATIVE,
            pid,
            state: Mutex::new(RunState::Running),
            output: Mutex::new(OutputLog::default()),
            incarnation_control: Some(RunControl::Native(native_control)),
            native_runs: Some(config.native_runs),
            persistence_mode: config.persistence_mode,
            persistence_transition: Mutex::new(()),
            persistence: Mutex::new(match config.persistence_mode {
                PersistenceMode::MemoryOnly => PersistenceBinding::Disabled,
                PersistenceMode::PersistentCapable => {
                    PersistenceBinding::Pending { terminal: None }
                }
            }),
            attachments: AtomicUsize::new(0),
            qualification_stats: config.qualification_stats,
            terminal_publications: config.terminal_publications,
            terminal_ordinal: OnceLock::new(),
            events: LiveEventOwner::new(config.live_event_capacity),
        })
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one tmux Control Mode owner handoff remains linear and rollback-auditable"
    )]
    fn import_tmux(
        socket_path: &str,
        pane_id: &str,
        config: TmuxImportConfig,
        cleanup_reservation: TmuxCleanupReservation,
    ) -> Result<PendingTmuxPublication, ProtocolError> {
        let mut pending = tmux::spawn_control(
            socket_path,
            pane_id,
            config.discovery_deadline,
            &config.qualification_stats,
        )?;
        let target = pending.target.clone();
        let socket_identity = pending.socket_identity;
        let control_pid = pending.child_id();
        let stdin = pending.take_stdin();
        let stdout = pending.take_stdout();
        let (commands_tx, commands_rx) = mpsc::channel();
        let (completion_tx, completion_rx) = mpsc::sync_channel(1);
        let run = Arc::new(Self {
            id: config.id,
            spec: None,
            lineage: None,
            backend: RunBackend::Tmux {
                socket_path: target.socket_path.clone(),
                server_pid: target.server_pid,
                server_started_at: target.server_started_at,
                session_id: target.session_id.clone(),
                window_id: target.window_id.clone(),
                pane_id: target.pane_id.clone(),
                tmux_version: target.tmux_version.clone(),
            },
            capabilities: RunCapabilities::TMUX_READ_ONLY,
            pid: Some(target.pane_pid),
            state: Mutex::new(RunState::Running),
            output: Mutex::new(OutputLog::with_initial_truncation()),
            incarnation_control: Some(RunControl::Tmux(TmuxRunControl {
                writer: Mutex::new(Some(TmuxCommandWriter::new(stdin))),
                commands: commands_tx,
                completion: Mutex::new(TmuxCompletion::Pending(completion_rx)),
            })),
            native_runs: None,
            persistence_mode: PersistenceMode::MemoryOnly,
            persistence_transition: Mutex::new(()),
            persistence: Mutex::new(PersistenceBinding::Disabled),
            attachments: AtomicUsize::new(0),
            qualification_stats: config.qualification_stats.clone(),
            terminal_publications: config.terminal_publications,
            terminal_ordinal: OnceLock::new(),
            events: LiveEventOwner::new(config.live_event_capacity),
        });
        let pending_publication = PendingTmuxPublication::new(run, cleanup_reservation);
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let (output_done_tx, output_done_rx) = mpsc::channel();
        let output_run = Arc::clone(pending_publication.run());
        let output_target = target.clone();
        let output_ready = ready_tx.clone();
        let reader_guard = config
            .qualification_stats
            .guard(QualificationGauge::Readers);
        thread::Builder::new()
            .name(format!("ctxmux-tmux-output-{}", config.id))
            .spawn(move || {
                let _reader_guard = reader_guard;
                let termination =
                    read_tmux_output(&output_run, stdout, &output_target, &output_ready);
                if output_done_tx.send(termination).is_ok() {
                    output_run.notify_tmux_reader_terminated();
                }
            })
            .map_err(|error| backend_protocol_error("start tmux output reader", error))?;

        let wait_run = Arc::clone(pending_publication.run());
        let wait_target = target;
        let wait_ready = ready_tx;
        let (child_tx, child_rx) = mpsc::sync_channel(0);
        let waiter_guard = config
            .qualification_stats
            .guard(QualificationGauge::Waiters);
        thread::Builder::new()
            .name(format!("ctxmux-tmux-wait-{}", config.id))
            .spawn(move || {
                let _waiter_guard = waiter_guard;
                let Ok(mut child) = child_rx.recv() else {
                    return;
                };
                let outcome = wait_for_tmux_control(
                    &mut child,
                    &wait_run,
                    &commands_rx,
                    &wait_target,
                    socket_identity,
                );
                complete_tmux_control(
                    &wait_run,
                    outcome,
                    &output_done_rx,
                    &wait_ready,
                    &completion_tx,
                    control_pid,
                );
            })
            .map_err(|error| backend_protocol_error("start tmux control waiter", error))?;
        let child = pending.take_child();
        if let Err(error) = child_tx.send(child) {
            let mut child = error.0;
            let _ = child.kill();
            let _ = child.wait();
            return Err(backend_protocol_error(
                "handoff tmux control child",
                "waiter stopped before taking ownership",
            ));
        }

        Self::finish_tmux_import(
            pending_publication,
            &ready_rx,
            config.prepare_deadline,
            config.total_deadline,
        )
    }

    fn finish_tmux_import(
        mut pending: PendingTmuxPublication,
        ready: &mpsc::Receiver<Result<(), ProtocolError>>,
        prepare_deadline: Instant,
        total_deadline: Instant,
    ) -> Result<PendingTmuxPublication, ProtocolError> {
        let readiness =
            match ready.recv_timeout(prepare_deadline.saturating_duration_since(Instant::now())) {
                Ok(Ok(())) if Instant::now() < prepare_deadline => return Ok(pending),
                Ok(Ok(())) => ProtocolError::new(
                    ErrorCode::BackendUnavailable,
                    "tmux Control Mode readiness exceeded the import preparation deadline",
                ),
                Ok(Err(error)) => error,
                Err(error) => backend_protocol_error("wait for tmux Control Mode readiness", error),
            };
        pending.run().request_tmux_import_cleanup();
        let cleanup_timeout = TMUX_FAILED_IMPORT_CLEANUP_TIMEOUT
            .min(total_deadline.saturating_duration_since(Instant::now()));
        match pending.run().wait_for_tmux_completion(cleanup_timeout) {
            Ok(()) => {
                pending.transfer(
                    "tmux import cleanup completed; waiting for worker-owned Run references to settle"
                        .to_owned(),
                );
                Err(readiness)
            }
            Err(cleanup_error) => {
                pending.transfer(format!("tmux import cleanup failed: {cleanup_error}"));
                Err(ProtocolError::new(
                    readiness.code,
                    format!("{}; cleanup failed: {cleanup_error}", readiness.message),
                ))
            }
        }
    }

    fn recover(
        recovered: RecoveredRun,
        persistence: PersistentRun,
        live_event_capacity: usize,
        terminal_publications: TerminalPublicationOwner,
        qualification_stats: QualificationStats,
    ) -> Arc<Self> {
        let terminal_ordinal = OnceLock::new();
        terminal_publications.recover(&terminal_ordinal);
        Arc::new(Self {
            id: recovered.info.id,
            spec: recovered.info.spec,
            lineage: recovered.info.lineage,
            backend: recovered.info.backend,
            capabilities: recovered.info.capabilities,
            pid: recovered.info.pid,
            state: Mutex::new(recovered.info.state),
            output: Mutex::new(OutputLog::from_replay(recovered.replay)),
            incarnation_control: None,
            native_runs: None,
            persistence_mode: PersistenceMode::PersistentCapable,
            persistence_transition: Mutex::new(()),
            persistence: Mutex::new(PersistenceBinding::Active(persistence)),
            attachments: AtomicUsize::new(0),
            qualification_stats,
            terminal_publications,
            terminal_ordinal,
            events: LiveEventOwner::new(live_event_capacity),
        })
    }

    /// Re-bind live native control onto a freshly recovered Run whose child and
    /// PTY master crossed an exec-in-place daemon upgrade.
    ///
    /// This is the live counterpart of [`recover`](Self::recover): it reuses the
    /// same recovered persistence binding and replay — so the durable output
    /// cursor continues from the committed head rather than resetting to zero
    /// (a reset would trip persistence gap-rejection on the next append) — but
    /// populates the two control fields `recover` leaves `None`. The child is
    /// adopted by pid and the master by descriptor; nothing is respawned.
    ///
    /// Returns `Err` when the inherited descriptor cannot be duplicated, the pid
    /// cannot be adopted, or the daemon-wide owner rejects registration.
    #[cfg_attr(not(test), allow(dead_code))]
    #[allow(clippy::too_many_arguments)]
    fn readopt(
        recovered: RecoveredRun,
        persistence: PersistentRun,
        master_fd: OwnedFd,
        child_pid: u32,
        input_state: HandoffInputState,
        native_runs: NativeRuntimeOwner,
        live_event_capacity: usize,
        terminal_publications: TerminalPublicationOwner,
        qualification_stats: QualificationStats,
        input_drains: InputDrainGate,
        wait_failure: NativeWaitFailure,
    ) -> Result<Arc<Self>, ProtocolError> {
        let id = recovered.info.id;

        // Derive the reader and writer as independent CLOEXEC dups of the
        // inherited master BEFORE it is moved into the control adapter. Reading
        // from / writing to a PTY master is plain read(2)/write(2), so each end
        // is a distinct `fs::File` over its own owned descriptor — never the
        // same fd aliased. `duplicate_cloexec` borrows the raw number without
        // consuming `master_fd`, keeping ownership clean: `master_fd` moves into
        // the adapter, and each dup is a fresh `OwnedFd` closed exactly once.
        let master_raw = master_fd.as_raw_fd();
        let reader = fs::File::from(
            ctxmux_inherited_fd::duplicate_cloexec(master_raw)
                .map_err(|error| spawn_error("clone re-adopted PTY reader", error))?,
        );
        let writer: Box<dyn Write + Send> = Box::new(fs::File::from(
            ctxmux_inherited_fd::duplicate_cloexec(master_raw)
                .map_err(|error| spawn_error("clone re-adopted PTY writer", error))?,
        ));
        let adopted = AdoptedMasterPty::from_owned_fd(master_fd);

        // The child already exists and crossed the exec, so it is adopted by
        // pid rather than spawned: `AdoptedChild` reaps it through `waitid`, and
        // `NativeSession` routes its reap/signal authority through the same pid.
        let child: Box<dyn Child + Send + Sync> = Box::new(
            AdoptedChild::from_pid(child_pid)
                .map_err(|error| spawn_error("adopt re-adopted child", error))?,
        );
        let session = NativeSession::from_child_pid(child_pid)
            .map_err(|error| spawn_error("identify re-adopted native session", error))?;

        let owner_wake = native_runs.owner_wake();
        let native_control = NativeControlOwner::new_adopted(
            id,
            adopted,
            writer,
            input_drains,
            owner_wake,
            input_state,
        );
        // Mirror the spawn seam: bind the reap control so that if registration
        // fails, `PendingChild::drop` records the kill/reap outcome against the
        // control. On success, `NativeRunRegistration::into_entry` clears the
        // bound control before taking the child, so the owner is the sole reaper
        // and the child is never double-owned.
        let mut pending_child = PendingChild::new(child);
        pending_child.bind_reap_control(native_control.clone());
        let reader_guard = qualification_stats.guard(QualificationGauge::Readers);
        let waiter_guard = qualification_stats.guard(QualificationGauge::Waiters);

        // A live re-adopted run defers its terminal ordinal to `publish()` (run
        // when the child later exits), mirroring the live `new_native` spawn
        // path — deliberately NOT `terminal_publications.recover(...)`. Unlike
        // `Run::recover` (which restores historical dead runs and so DOES call
        // `recover` here), calling `recover` on this cell would `set()` it now,
        // and the child's exit-time `publish()` would double-`set()` the same
        // `OnceLock` and panic the finalize worker. This single-set contract is
        // unit-tested by
        // `recover_then_publish_on_the_same_cell_panics_the_single_set_contract`
        // in creation.rs.
        let terminal_ordinal = OnceLock::new();
        let run = Arc::new(Self {
            id,
            spec: recovered.info.spec,
            lineage: recovered.info.lineage,
            backend: recovered.info.backend,
            capabilities: recovered.info.capabilities,
            pid: Some(child_pid),
            state: Mutex::new(recovered.info.state),
            output: Mutex::new(OutputLog::from_replay(recovered.replay)),
            incarnation_control: Some(RunControl::Native(native_control)),
            native_runs: Some(native_runs),
            persistence_mode: PersistenceMode::PersistentCapable,
            persistence_transition: Mutex::new(()),
            persistence: Mutex::new(PersistenceBinding::Active(persistence)),
            attachments: AtomicUsize::new(0),
            qualification_stats,
            terminal_publications,
            terminal_ordinal,
            events: LiveEventOwner::new(live_event_capacity),
        });

        let registration_control = run
            .native_control()
            .expect("re-adopted Run retains native control")
            .clone();
        let native_runs = run
            .native_runs
            .as_ref()
            .expect("re-adopted Run retains its daemon-wide owner")
            .clone();
        let registration = NativeRunRegistration::new(
            &run,
            reader,
            pending_child,
            session,
            registration_control,
            wait_failure,
            || {},
            reader_guard,
            waiter_guard,
        );
        native_runs.register(registration).map_err(|error| {
            let (message, registration) = error.into_parts();
            drop(registration);
            spawn_error("register re-adopted native Run owner", message)
        })?;

        Ok(run)
    }

    fn info(&self) -> RunInfo {
        let output = mutex_lock(&self.output);
        let applied_input_bytes = match &self.incarnation_control {
            Some(RunControl::Native(control)) => Some(control.applied_input_bytes()),
            Some(RunControl::Tmux(_)) | None => None,
        };
        RunInfo {
            id: self.id,
            spec: self.spec.clone(),
            lineage: self.lineage.clone(),
            backend: self.backend.clone(),
            capabilities: self.capabilities,
            pid: self.pid,
            state: mutex_lock(&self.state).clone(),
            latest_output_bytes: output.latest_output_bytes(),
            durable_output_bytes: mutex_lock(&self.persistence)
                .durable()
                .map(PersistentRun::durable_head),
            first_available_byte: output.first_available_byte(),
            attachments: self.attachments.load(Ordering::Acquire),
            applied_input_bytes,
        }
    }

    #[cfg(test)]
    fn persistence_start_info(&self) -> RunInfo {
        let mut info = self.info();
        info.state = RunState::Running;
        info
    }

    #[cfg(test)]
    fn persistence_terminal_is_pending(&self) -> bool {
        matches!(
            &*mutex_lock(&self.persistence),
            PersistenceBinding::Pending { terminal: Some(_) }
                | PersistenceBinding::CommittedPendingActivation {
                    terminal: Some(_),
                    ..
                }
        )
    }

    async fn input(&self, data: Vec<u8>) -> ControlResult {
        self.begin_input(data)?.resolve().await
    }

    async fn recoverable_input(
        &self,
        operation: RecoverableInput,
    ) -> Result<AppliedInputRange, ControlFailure> {
        self.native_control()
            .map_err(control_not_applied)?
            .begin_recoverable_input(
                operation.operation_key,
                operation.expected_byte,
                operation.data,
            )?
            .resolve()
            .await
    }

    async fn signal(&self, signal: ctxmux_protocol::RunSignal) -> ControlResult {
        self.begin_signal(signal)?.resolve().await
    }

    fn begin_signal(
        &self,
        signal: ctxmux_protocol::RunSignal,
    ) -> Result<PendingSignal, ControlFailure> {
        self.native_control()
            .map_err(control_not_applied)?
            .begin_signal(signal)
    }

    fn begin_input(&self, data: Vec<u8>) -> Result<PendingInput, ControlFailure> {
        self.native_control()
            .map_err(control_not_applied)?
            .begin_input(data)
    }

    fn resize(&self, size: TerminalSize) -> ControlResult {
        if let Err(error) = validate_terminal_size(size) {
            return Err(control_not_applied(invalid_run_spec(error)));
        }
        match self.native_control() {
            Ok(control) => control.resize(size),
            Err(error) => Err(control_not_applied(error)),
        }
    }

    async fn stop(&self) -> ControlResult {
        self.begin_stop()?.resolve(STOP_ACK_TIMEOUT).await
    }

    fn begin_stop(&self) -> Result<PendingStop, ControlFailure> {
        self.native_control()
            .map_err(control_not_applied)?
            .begin_stop()
    }

    fn record_output(&self, data: Vec<u8>) {
        if data.is_empty() {
            return;
        }
        let chunk = match self.persistence_mode {
            PersistenceMode::MemoryOnly => mutex_lock(&self.output).push(data),
            PersistenceMode::PersistentCapable => {
                let _transition = mutex_lock(&self.persistence_transition);
                let (chunk, replay, running, persistence) = {
                    let mut output = mutex_lock(&self.output);
                    let chunk = output.push(data);
                    let replay = output.replay(chunk.start_byte);
                    let running = mutex_lock(&self.state).is_running();
                    let persistence = mutex_lock(&self.persistence).active().cloned();
                    (chunk, replay, running, persistence)
                };
                if running && let Some(persistence) = persistence {
                    persistence.append(self.id, replay);
                }
                chunk
            }
        };
        self.publish_event(RunEvent::Output { chunk });
    }

    fn mark_output_source_gap(&self) -> u64 {
        mutex_lock(&self.output).mark_source_gap()
    }

    fn subscribe(self: &Arc<Self>) -> (AttachmentGuard, broadcast::Receiver<RunEvent>) {
        let mut sender = mutex_lock(&self.events.sender);
        self.attachments.fetch_add(1, Ordering::AcqRel);
        let receiver = sender
            .get_or_insert_with(|| {
                let (sender, _) = broadcast::channel(self.events.capacity);
                sender
            })
            .subscribe();
        let qualification_guard = self
            .qualification_stats
            .guard(QualificationGauge::Attachments);
        let guard = AttachmentGuard {
            run: Arc::clone(self),
            _qualification_guard: qualification_guard,
        };
        (guard, receiver)
    }

    fn attachment_snapshot(&self, after_byte: u64) -> AttachedSnapshot {
        let replay = mutex_lock(&self.output).replay(after_byte);
        AttachedSnapshot {
            run: self.info(),
            replay,
        }
    }

    fn publish_event(&self, event: RunEvent) {
        if self.attachments.load(Ordering::Acquire) == 0 {
            return;
        }
        if let Some(sender) = mutex_lock(&self.events.sender).as_ref() {
            let _ = sender.send(event);
        }
    }

    fn native_control(&self) -> Result<&NativeControlOwner, ProtocolError> {
        match &self.incarnation_control {
            Some(RunControl::Native(control)) => Ok(control),
            Some(RunControl::Tmux(_)) => Err(ProtocolError::new(
                ErrorCode::UnsupportedCapability,
                format!("Run {} backend does not support native control", self.id),
            )),
            None => Err(ProtocolError::new(
                ErrorCode::InvalidRunState,
                format!("cannot control historical Run {}", self.id),
            )),
        }
    }

    fn has_continuation_authority(&self) -> bool {
        matches!(
            &self.incarnation_control,
            Some(RunControl::Native(control)) if control.has_continuation_authority()
        )
    }

    fn write_tmux_command(&self, kind: TmuxCommandKind, command: &[u8]) -> io::Result<()> {
        let Some(RunControl::Tmux(control)) = &self.incarnation_control else {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "Run has no tmux control client",
            ));
        };
        control.with_writer(|writer| writer.write_command(kind, command))
    }

    fn write_tmux_periodic_probe(&self, command: &[u8]) -> io::Result<()> {
        let Some(RunControl::Tmux(control)) = &self.incarnation_control else {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "Run has no tmux control client",
            ));
        };
        control.with_writer(|writer| writer.write_periodic_probe(command))
    }

    fn establish_tmux_session(&self, command: &[u8]) -> io::Result<()> {
        let Some(RunControl::Tmux(control)) = &self.incarnation_control else {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "Run has no tmux control client",
            ));
        };
        control.with_writer(|writer| {
            writer.establish_session_and_write(TmuxCommandKind::TargetProbe, command)
        })
    }

    fn correlate_tmux_command_result(
        &self,
        number: u64,
    ) -> Result<TmuxCommandResultKind, &'static str> {
        let Some(RunControl::Tmux(control)) = &self.incarnation_control else {
            return Err("Run has no tmux control client");
        };
        control.correlate_result(number)
    }

    fn request_tmux_import_cleanup(&self) {
        if let Some(RunControl::Tmux(control)) = &self.incarnation_control {
            let _ = control.commands.send(TmuxControlCommand::Interrupt(
                InterruptionReason::TmuxServerUnavailable,
            ));
            control.close_writer();
        }
    }

    fn tmux_unpublished_cleanup_result(&self) -> Result<(), String> {
        match &self.incarnation_control {
            Some(RunControl::Tmux(control)) => control.closed_quiescence_result(),
            Some(RunControl::Native(_)) => {
                Err("unpublished tmux cleanup references a native Run".to_owned())
            }
            None => Err("unpublished tmux cleanup has no incarnation owner".to_owned()),
        }
    }

    fn notify_tmux_reader_terminated(&self) {
        if let Some(RunControl::Tmux(control)) = &self.incarnation_control {
            let _ = control.commands.send(TmuxControlCommand::ReaderTerminated);
        }
    }

    fn wait_for_tmux_completion(&self, timeout: Duration) -> Result<(), String> {
        let Some(RunControl::Tmux(control)) = &self.incarnation_control else {
            return Ok(());
        };
        control.wait_for_completion(timeout)
    }

    fn collection_ordinal(&self) -> Option<TerminalOrdinal> {
        if mutex_lock(&self.state).is_running() || self.attachments.load(Ordering::Acquire) != 0 {
            return None;
        }
        let ordinal = *self.terminal_ordinal.get()?;
        let backend_is_quiescent = match &self.incarnation_control {
            Some(RunControl::Native(control)) => control.closed_quiescence_result().is_ok(),
            Some(RunControl::Tmux(control)) => control.closed_quiescence_result().is_ok(),
            None => self.persistence_mode == PersistenceMode::PersistentCapable,
        };
        backend_is_quiescent.then_some(ordinal)
    }

    fn detach_collection_descriptors(&self) -> Result<Option<DetachedNativeDescriptors>, String> {
        match &self.incarnation_control {
            Some(RunControl::Native(control)) => control
                .detach_closed_descriptors_after_owner_fence()
                .map(Some),
            Some(RunControl::Tmux(control)) => {
                control.closed_quiescence_result()?;
                Ok(None)
            }
            None if self.persistence_mode == PersistenceMode::PersistentCapable => Ok(None),
            None => Err(format!(
                "Run {} has no incarnation owner to collect",
                self.id
            )),
        }
    }

    fn persistent_metadata_owner(&self) -> Option<Arc<AtomicU64>> {
        mutex_lock(&self.persistence)
            .durable()
            .map(PersistentRun::metadata_bytes_owner)
    }

    /// Install a durable COMMIT result without issuing persistence I/O.
    ///
    /// Registry exact replacement must run after this method and before
    /// `activate_persistence_after_publication`.
    fn install_committed_persistence(&self, persistence: PersistentRun) {
        assert_eq!(
            self.persistence_mode,
            PersistenceMode::PersistentCapable,
            "only persistence-capable Runs can bind durable state"
        );
        let _transition = mutex_lock(&self.persistence_transition);
        let mut binding = mutex_lock(&self.persistence);
        let terminal = match std::mem::replace(&mut *binding, PersistenceBinding::Disabled) {
            PersistenceBinding::Pending { terminal } => terminal,
            PersistenceBinding::Disabled
            | PersistenceBinding::CommittedPendingActivation { .. }
            | PersistenceBinding::Active(_) => {
                panic!("persistent Run installs one committed binding")
            }
        };
        *binding = PersistenceBinding::CommittedPendingActivation {
            durable: persistence,
            terminal,
        };
    }

    /// Activate output durability only after the Run and exact key are public.
    fn activate_persistence_after_publication(&self) {
        let _transition = mutex_lock(&self.persistence_transition);
        let replay = mutex_lock(&self.output).replay(0);
        let (persistence, terminal) = {
            let mut binding = mutex_lock(&self.persistence);
            let (durable, terminal) =
                match std::mem::replace(&mut *binding, PersistenceBinding::Disabled) {
                    PersistenceBinding::CommittedPendingActivation { durable, terminal } => {
                        (durable, terminal)
                    }
                    PersistenceBinding::Disabled
                    | PersistenceBinding::Pending { .. }
                    | PersistenceBinding::Active(_) => {
                        panic!("committed persistence activates exactly once after publication")
                    }
                };
            *binding = PersistenceBinding::Active(durable.clone());
            (durable, terminal)
        };
        if let Some(terminal) = terminal {
            persistence.finalize(
                self.id,
                self.pid.expect("native Run has a child PID"),
                replay,
                terminal.clone(),
            );
            self.publish_terminal_state(terminal.clone());
            self.publish_event(RunEvent::Exited { state: terminal });
        } else {
            persistence.append(self.id, replay);
        }
    }

    fn publish_terminal(&self, terminal: RunState) {
        if self.persistence_mode == PersistenceMode::MemoryOnly {
            self.publish_terminal_state(terminal.clone());
            self.publish_event(RunEvent::Exited { state: terminal });
            return;
        }
        let _transition = mutex_lock(&self.persistence_transition);
        let persistence = {
            let mut binding = mutex_lock(&self.persistence);
            match &mut *binding {
                PersistenceBinding::Pending { terminal: pending }
                | PersistenceBinding::CommittedPendingActivation {
                    terminal: pending, ..
                } => {
                    debug_assert!(pending.is_none());
                    *pending = Some(terminal);
                    return;
                }
                PersistenceBinding::Active(persistence) => persistence.clone(),
                PersistenceBinding::Disabled => {
                    panic!("persistence-capable Run retains a persistence binding")
                }
            }
        };
        let replay = mutex_lock(&self.output).replay(0);
        persistence.finalize(
            self.id,
            self.pid.expect("native Run has a child PID"),
            replay,
            terminal.clone(),
        );
        let _output = mutex_lock(&self.output);
        self.publish_terminal_state(terminal.clone());
        self.publish_event(RunEvent::Exited { state: terminal });
    }

    fn publish_interrupted(&self, reason: InterruptionReason) {
        self.publish_terminal_state(RunState::Interrupted { reason });
        self.publish_event(RunEvent::Interrupted { reason });
    }

    fn publish_terminal_state(&self, terminal: RunState) {
        self.terminal_publications
            .publish(&self.terminal_ordinal, || {
                *mutex_lock(&self.state) = terminal;
            });
    }

    fn terminate_unpublished(self: &Arc<Self>) -> Result<(), String> {
        let request_error = self.request_unpublished_cleanup().err();
        let control = self
            .native_control()
            .map_err(|error| error.message.clone())?;
        let deadline = Instant::now() + UNPUBLISHED_REAP_INLINE_TIMEOUT;
        if let Err(wait_error) = control.wait_until_reaped(deadline) {
            return Err(match request_error {
                Some(request_error) => format!("{request_error}; {wait_error}"),
                None => wait_error,
            });
        }
        loop {
            match self.unpublished_cleanup_result() {
                Ok(()) => {
                    let descriptors = control.detach_closed_descriptors_after_owner_fence()?;
                    drop(descriptors);
                    return Ok(());
                }
                Err(error) if Instant::now() >= deadline => {
                    return Err(match request_error {
                        Some(request_error) => format!("{request_error}; {error}"),
                        None => error,
                    });
                }
                Err(_) => thread::sleep(
                    CHILD_CONTROL_POLL.min(deadline.saturating_duration_since(Instant::now())),
                ),
            }
        }
    }

    fn request_unpublished_cleanup(&self) -> Result<(), String> {
        self.native_control()
            .map_err(|error| error.message.clone())?
            .cleanup_unpublished()
    }

    fn unpublished_cleanup_result(self: &Arc<Self>) -> Result<(), String> {
        let control = self
            .native_control()
            .map_err(|error| error.message.clone())?;
        control.unpublished_cleanup_result()?;
        let owners = Arc::strong_count(self);
        if owners != 1 {
            return Err(format!(
                "unpublished Run {} retains {owners} reader, waiter, or external owners",
                self.id
            ));
        }
        Ok(())
    }
}

fn exit_state(status: &portable_pty::ExitStatus) -> RunState {
    RunState::Exited {
        code: status.exit_code(),
        signal: status.signal().map(str::to_owned),
    }
}

fn wait_for_tmux_control(
    child: &mut tmux::ObservedControl,
    run: &Run,
    commands: &mpsc::Receiver<TmuxControlCommand>,
    target: &ctxmux_protocol::TmuxPaneInfo,
    socket_identity: TmuxSocketIdentity,
) -> TmuxWaitOutcome {
    const TARGET_POLL: Duration = Duration::from_millis(500);
    let mut next_target_poll = Instant::now() + TARGET_POLL;
    loop {
        match commands.recv_timeout(CHILD_CONTROL_POLL) {
            Ok(TmuxControlCommand::Interrupt(reason)) => {
                return TmuxWaitOutcome {
                    cause: TmuxWaitCause::Interrupted(reason),
                    cleanup: terminate_tmux_control_child(child),
                };
            }
            Ok(TmuxControlCommand::ReaderTerminated) => {
                return TmuxWaitOutcome {
                    cause: TmuxWaitCause::ReaderTerminated,
                    cleanup: terminate_tmux_control_child(child),
                };
            }
            Ok(TmuxControlCommand::Shutdown) => {
                return TmuxWaitOutcome {
                    cause: TmuxWaitCause::Shutdown,
                    cleanup: terminate_tmux_control_child(child),
                };
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return TmuxWaitOutcome {
                    cause: TmuxWaitCause::CommandChannelClosed,
                    cleanup: terminate_tmux_control_child(child),
                };
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        }
        if Instant::now() >= next_target_poll {
            if !tmux::socket_identity_matches(&target.socket_path, socket_identity) {
                return TmuxWaitOutcome {
                    cause: TmuxWaitCause::SocketTargetChanged,
                    cleanup: terminate_tmux_control_child(child),
                };
            }
            let command = tmux::target_probe_command(&target.pane_id);
            if let Err(error) = run.write_tmux_periodic_probe(command.as_bytes()) {
                return TmuxWaitOutcome {
                    cause: TmuxWaitCause::ProbeWriteFailed(error.to_string()),
                    cleanup: terminate_tmux_control_child(child),
                };
            }
            next_target_poll = Instant::now() + TARGET_POLL;
        }
        match child.try_wait() {
            Ok(Some(_)) => {
                return TmuxWaitOutcome {
                    cause: TmuxWaitCause::ChildExited,
                    cleanup: Ok(()),
                };
            }
            Ok(None) => {}
            Err(error) => {
                return TmuxWaitOutcome {
                    cause: TmuxWaitCause::ChildStatusFailed(error.to_string()),
                    cleanup: combine_cleanup_failure(
                        terminate_tmux_control_child(child),
                        &format!("failed to query tmux Control Mode client status: {error}"),
                    ),
                };
            }
        }
    }
}

fn complete_tmux_control(
    run: &Run,
    outcome: TmuxWaitOutcome,
    output_done: &mpsc::Receiver<TmuxReaderTermination>,
    ready: &mpsc::SyncSender<Result<(), ProtocolError>>,
    completion: &mpsc::SyncSender<Result<(), String>>,
    control_pid: u32,
) {
    let (reader_termination, cleanup) = match output_done.recv_timeout(TMUX_OUTPUT_DRAIN_TIMEOUT) {
        Ok(termination) => (Some(termination), outcome.cleanup),
        Err(mpsc::RecvTimeoutError::Timeout) => (
            None,
            combine_cleanup_failure(
                outcome.cleanup,
                "tmux output reader did not finish during shutdown",
            ),
        ),
        Err(mpsc::RecvTimeoutError::Disconnected) => (
            None,
            combine_cleanup_failure(
                outcome.cleanup,
                "tmux output reader ended without a completion receipt",
            ),
        ),
    };
    let termination = resolve_tmux_termination(outcome.cause, reader_termination, control_pid);
    if let Some(RunControl::Tmux(control)) = &run.incarnation_control {
        control.close_writer();
    }
    let _ = ready.try_send(Err(termination.error));
    run.publish_interrupted(termination.reason);
    let _ = completion.send(cleanup);
}

fn resolve_tmux_termination(
    cause: TmuxWaitCause,
    reader: Option<TmuxReaderTermination>,
    control_pid: u32,
) -> TmuxTermination {
    match cause {
        cause @ (TmuxWaitCause::ReaderTerminated | TmuxWaitCause::ChildExited) => reader
            .map_or_else(
                || fallback_tmux_termination(cause, control_pid),
                |termination| termination.failure,
            ),
        cause => fallback_tmux_termination(cause, control_pid),
    }
}

fn fallback_tmux_termination(cause: TmuxWaitCause, control_pid: u32) -> TmuxTermination {
    let (code, message, reason) = match cause {
        TmuxWaitCause::ReaderTerminated => (
            ErrorCode::BackendUnavailable,
            "tmux output reader ended without a termination receipt".to_owned(),
            InterruptionReason::TmuxServerUnavailable,
        ),
        TmuxWaitCause::Interrupted(reason) => (
            interruption_error_code(reason),
            "tmux Control Mode client was interrupted".to_owned(),
            reason,
        ),
        TmuxWaitCause::Shutdown => (
            ErrorCode::BackendUnavailable,
            "tmux Control Mode client stopped during daemon shutdown".to_owned(),
            InterruptionReason::TmuxServerUnavailable,
        ),
        TmuxWaitCause::CommandChannelClosed => (
            ErrorCode::BackendUnavailable,
            "tmux Control Mode command channel closed".to_owned(),
            InterruptionReason::TmuxServerUnavailable,
        ),
        TmuxWaitCause::SocketTargetChanged => (
            ErrorCode::TargetChanged,
            "tmux server socket identity changed".to_owned(),
            InterruptionReason::TmuxTargetChanged,
        ),
        TmuxWaitCause::ProbeWriteFailed(error) => (
            ErrorCode::BackendUnavailable,
            format!("failed to write tmux target probe: {error}"),
            InterruptionReason::TmuxServerUnavailable,
        ),
        TmuxWaitCause::ChildExited => (
            ErrorCode::BackendUnavailable,
            format!("tmux Control Mode client {control_pid} exited before import"),
            InterruptionReason::TmuxServerUnavailable,
        ),
        TmuxWaitCause::ChildStatusFailed(error) => (
            ErrorCode::BackendUnavailable,
            format!("failed to query tmux Control Mode client {control_pid}: {error}"),
            InterruptionReason::TmuxServerUnavailable,
        ),
    };
    TmuxTermination {
        error: ProtocolError::new(code, message),
        reason,
    }
}

const fn interruption_error_code(reason: InterruptionReason) -> ErrorCode {
    match reason {
        InterruptionReason::TmuxTargetChanged => ErrorCode::TargetChanged,
        InterruptionReason::DaemonRestart
        | InterruptionReason::TmuxServerUnavailable
        | InterruptionReason::TmuxProtocolError => ErrorCode::BackendUnavailable,
    }
}

fn terminate_tmux_control_child(child: &mut tmux::ObservedControl) -> Result<(), String> {
    let status_error = match child.try_wait() {
        Ok(Some(_)) => return Ok(()),
        Ok(None) => None,
        Err(error) => Some(error),
    };

    let kill_error = child.kill().err();
    let wait_error = child.wait().err();
    match (kill_error, wait_error) {
        (None | Some(_), None) => Ok(()),
        (kill_error, Some(wait_error)) => {
            let mut failures = Vec::new();
            if let Some(status_error) = status_error {
                failures.push(format!(
                    "failed to query tmux Control Mode client before termination: {status_error}"
                ));
            }
            if let Some(kill_error) = kill_error {
                failures.push(format!(
                    "failed to terminate tmux Control Mode client: {kill_error}"
                ));
            }
            failures.push(format!(
                "failed to reap tmux Control Mode client: {wait_error}"
            ));
            Err(failures.join("; "))
        }
    }
}

fn combine_cleanup_failure(existing: Result<(), String>, failure: &str) -> Result<(), String> {
    match existing {
        Ok(()) => Err(failure.to_owned()),
        Err(existing) => Err(format!("{existing}; {failure}")),
    }
}

fn read_tmux_output(
    run: &Run,
    stdout: std::process::ChildStdout,
    target: &ctxmux_protocol::TmuxPaneInfo,
    ready: &mpsc::SyncSender<Result<(), ProtocolError>>,
) -> TmuxReaderTermination {
    let mut reader = io::BufReader::new(stdout);
    let mut parser = ControlParser::default();
    let mut line = Vec::new();
    let mut readiness = TmuxReadiness::default();
    let failure = loop {
        match tmux::read_bounded_line(&mut reader, &mut line) {
            Ok(BoundedLineRead::Eof) => {
                if let Err(error) = parser.finish() {
                    break tmux_control_failure(
                        backend_protocol_error("finish tmux Control Mode stream", error),
                        if readiness.ready {
                            InterruptionReason::TmuxProtocolError
                        } else {
                            InterruptionReason::TmuxServerUnavailable
                        },
                    );
                }
                break tmux_control_failure(
                    ProtocolError::new(
                        ErrorCode::BackendUnavailable,
                        "tmux Control Mode stream closed",
                    ),
                    InterruptionReason::TmuxServerUnavailable,
                );
            }
            Ok(BoundedLineRead::Line) => {}
            Err(error) => {
                let reason = if readiness.ready {
                    InterruptionReason::TmuxProtocolError
                } else {
                    InterruptionReason::TmuxServerUnavailable
                };
                break tmux_control_failure(
                    backend_protocol_error("read tmux Control Mode stream", &error),
                    reason,
                );
            }
        }
        let item = match parser.parse_line(&line) {
            Ok(item) => item,
            Err(error) => {
                let reason = if readiness.ready {
                    InterruptionReason::TmuxProtocolError
                } else {
                    InterruptionReason::TmuxServerUnavailable
                };
                break tmux_control_failure(
                    backend_protocol_error("parse tmux Control Mode stream", &error),
                    reason,
                );
            }
        };
        let Some(item) = item else {
            continue;
        };
        if let Err(failure) = handle_tmux_control_item(run, target, ready, &mut readiness, item) {
            break failure;
        }
    };
    TmuxReaderTermination { failure }
}

#[derive(Default)]
struct TmuxReadiness {
    ready: bool,
}

fn handle_tmux_control_item(
    run: &Run,
    target: &ctxmux_protocol::TmuxPaneInfo,
    ready: &mpsc::SyncSender<Result<(), ProtocolError>>,
    readiness: &mut TmuxReadiness,
    item: ControlItem,
) -> Result<(), TmuxTermination> {
    match item {
        ControlItem::Output { pane_id, data, .. } if pane_id == target.pane_id => {
            run.record_output(data);
        }
        ControlItem::SessionChanged { session_id } if session_id == target.session_id => {
            let command = tmux::target_probe_command(&target.pane_id);
            if let Err(error) = run.establish_tmux_session(command.as_bytes()) {
                return Err(tmux_control_failure(
                    backend_protocol_error("write initial tmux target probe", error),
                    InterruptionReason::TmuxServerUnavailable,
                ));
            }
        }
        ControlItem::SessionChanged { .. } => {
            return Err(tmux_control_failure(
                ProtocolError::new(
                    ErrorCode::TargetChanged,
                    "tmux Control Mode client attached to a different session",
                ),
                InterruptionReason::TmuxTargetChanged,
            ));
        }
        ControlItem::CommandResult {
            number,
            success,
            output,
        } => {
            return handle_tmux_command_result(
                run, target, ready, readiness, number, success, &output,
            );
        }
        ControlItem::SessionRenamed { session_id, name } if session_id == target.session_id => {
            run.publish_event(RunEvent::Tmux {
                event: TmuxRunEvent::SessionRenamed { name },
            });
        }
        ControlItem::WindowClosed { window_id } if window_id == target.window_id => {
            return Err(tmux_control_failure(
                ProtocolError::new(
                    ErrorCode::TargetChanged,
                    format!("tmux target window {window_id} closed"),
                ),
                InterruptionReason::TmuxTargetChanged,
            ));
        }
        ControlItem::Paused { pane_id } if pane_id == target.pane_id => {
            let latest_output_bytes = run.mark_output_source_gap();
            run.publish_event(RunEvent::Tmux {
                event: TmuxRunEvent::Paused,
            });
            run.publish_event(RunEvent::Gap {
                latest_output_bytes,
            });
            let command = format!("refresh-client -A {pane_id}:continue\n");
            if let Err(error) =
                run.write_tmux_command(TmuxCommandKind::Continue, command.as_bytes())
            {
                return Err(tmux_control_failure(
                    backend_protocol_error("write tmux continue command", error),
                    InterruptionReason::TmuxServerUnavailable,
                ));
            }
        }
        ControlItem::Continued { pane_id } if pane_id == target.pane_id => {
            run.publish_event(RunEvent::Tmux {
                event: TmuxRunEvent::Continued,
            });
        }
        ControlItem::Exit => {
            return Err(tmux_control_failure(
                ProtocolError::new(
                    ErrorCode::BackendUnavailable,
                    "tmux Control Mode client reported exit",
                ),
                InterruptionReason::TmuxServerUnavailable,
            ));
        }
        ControlItem::Output { .. }
        | ControlItem::Notification
        | ControlItem::SessionRenamed { .. }
        | ControlItem::WindowClosed { .. }
        | ControlItem::Paused { .. }
        | ControlItem::Continued { .. } => {}
    }
    Ok(())
}

fn handle_tmux_command_result(
    run: &Run,
    target: &ctxmux_protocol::TmuxPaneInfo,
    ready: &mpsc::SyncSender<Result<(), ProtocolError>>,
    readiness: &mut TmuxReadiness,
    number: u64,
    success: bool,
    output: &[Vec<u8>],
) -> Result<(), TmuxTermination> {
    let result_kind = match run.correlate_tmux_command_result(number) {
        Ok(kind) => kind,
        Err(error) => {
            return Err(tmux_control_failure(
                backend_protocol_error("correlate tmux command result", error),
                if readiness.ready {
                    InterruptionReason::TmuxProtocolError
                } else {
                    InterruptionReason::TmuxServerUnavailable
                },
            ));
        }
    };
    if result_kind == TmuxCommandResultKind::Bootstrap {
        if success && output.is_empty() {
            return Ok(());
        }
        return Err(tmux_control_failure(
            ProtocolError::new(
                ErrorCode::BackendUnavailable,
                "tmux Control Mode bootstrap returned an unexpected result",
            ),
            InterruptionReason::TmuxServerUnavailable,
        ));
    }
    if !success {
        return Err(tmux_control_failure(
            ProtocolError::new(
                ErrorCode::TargetChanged,
                format!(
                    "tmux pane {} no longer accepts adapter commands",
                    target.pane_id
                ),
            ),
            InterruptionReason::TmuxTargetChanged,
        ));
    }
    match result_kind {
        TmuxCommandResultKind::Pending(TmuxCommandKind::TargetProbe) => {
            if output.len() != 1 {
                return Err(tmux_control_failure(
                    ProtocolError::new(
                        ErrorCode::BackendUnavailable,
                        "tmux target probe returned an unexpected output shape",
                    ),
                    InterruptionReason::TmuxProtocolError,
                ));
            }
            match tmux::target_identity_matches(target, &output[0]) {
                Ok(true) => {
                    if !readiness.ready {
                        readiness.ready = true;
                        let _ = ready.try_send(Ok(()));
                    }
                }
                Ok(false) => {
                    return Err(tmux_control_failure(
                        ProtocolError::new(
                            ErrorCode::TargetChanged,
                            "tmux target identity changed after import",
                        ),
                        InterruptionReason::TmuxTargetChanged,
                    ));
                }
                Err(error) => {
                    return Err(tmux_control_failure(
                        backend_protocol_error("parse tmux target probe", error),
                        InterruptionReason::TmuxProtocolError,
                    ));
                }
            }
        }
        TmuxCommandResultKind::Pending(TmuxCommandKind::Continue) => {
            if !output.is_empty() {
                return Err(tmux_control_failure(
                    ProtocolError::new(
                        ErrorCode::BackendUnavailable,
                        "tmux continue command returned unexpected output",
                    ),
                    InterruptionReason::TmuxProtocolError,
                ));
            }
        }
        TmuxCommandResultKind::Bootstrap => unreachable!("bootstrap handled above"),
    }
    Ok(())
}

fn tmux_control_failure(error: ProtocolError, reason: InterruptionReason) -> TmuxTermination {
    TmuxTermination { error, reason }
}

fn backend_protocol_error(action: &str, error: impl fmt::Display) -> ProtocolError {
    ProtocolError::new(
        ErrorCode::BackendUnavailable,
        format!("failed to {action}: {error}"),
    )
}

struct AttachmentGuard {
    run: Arc<Run>,
    _qualification_guard: crate::qualification_stats::GaugeGuard,
}

impl Drop for AttachmentGuard {
    fn drop(&mut self) {
        let previous = self.run.attachments.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "attachment count cannot underflow");
        if previous != 1 {
            return;
        }
        let mut sender = mutex_lock(&self.run.events.sender);
        if self.run.attachments.load(Ordering::Acquire) == 0 {
            sender.take();
        }
    }
}

#[derive(Default)]
struct OutputLog {
    chunks: VecDeque<OutputChunk>,
    retained_bytes: usize,
    latest_output_bytes: u64,
    source_gap_after_byte: Option<u64>,
}

impl OutputLog {
    fn with_initial_truncation() -> Self {
        Self {
            source_gap_after_byte: Some(0),
            ..Self::default()
        }
    }

    fn from_replay(replay: OutputReplay) -> Self {
        Self {
            retained_bytes: replay.chunks.iter().map(|chunk| chunk.data.len()).sum(),
            chunks: replay.chunks.into(),
            latest_output_bytes: replay.latest_output_bytes,
            source_gap_after_byte: None,
        }
    }

    fn mark_source_gap(&mut self) -> u64 {
        self.source_gap_after_byte = Some(self.latest_output_bytes);
        self.latest_output_bytes
    }

    fn push(&mut self, data: Vec<u8>) -> OutputChunk {
        assert!(
            !data.is_empty(),
            "output chunks must contain at least one byte"
        );
        let start_byte = self.latest_output_bytes;
        let end_byte = start_byte
            .checked_add(u64::try_from(data.len()).expect("output chunk length fits u64"))
            .expect("one Run cannot allocate more than u64::MAX output bytes");
        let chunk = OutputChunk {
            start_byte,
            end_byte,
            data,
        };
        self.latest_output_bytes = end_byte;
        self.retained_bytes = self.retained_bytes.saturating_add(chunk.data.len());
        self.chunks.push_back(chunk.clone());
        while self.retained_bytes > OUTPUT_RETENTION_BYTES && self.chunks.len() > 1 {
            if let Some(evicted) = self.chunks.pop_front() {
                self.retained_bytes = self.retained_bytes.saturating_sub(evicted.data.len());
            }
        }
        chunk
    }

    const fn latest_output_bytes(&self) -> u64 {
        self.latest_output_bytes
    }

    fn first_available_byte(&self) -> u64 {
        self.chunks.front().map_or(0, |chunk| chunk.start_byte)
    }

    fn replay(&self, after_byte: u64) -> OutputReplay {
        let first_available_byte = self.first_available_byte();
        OutputReplay {
            chunks: self
                .chunks
                .iter()
                .filter_map(|chunk| retained_after(chunk, after_byte))
                .collect(),
            first_available_byte,
            latest_output_bytes: self.latest_output_bytes(),
            truncated: self
                .source_gap_after_byte
                .is_some_and(|gap_byte| after_byte <= gap_byte)
                || after_byte < first_available_byte,
        }
    }
}

fn retained_after(chunk: &OutputChunk, after_byte: u64) -> Option<OutputChunk> {
    if chunk.end_byte <= after_byte {
        return None;
    }
    if chunk.start_byte >= after_byte {
        return Some(chunk.clone());
    }
    let offset = usize::try_from(after_byte - chunk.start_byte).ok()?;
    Some(OutputChunk {
        start_byte: after_byte,
        end_byte: chunk.end_byte,
        data: chunk.data.get(offset..)?.to_vec(),
    })
}

fn invalid_run_spec(error: run_spec::RunSpecValidationError) -> ProtocolError {
    ProtocolError::new(ErrorCode::InvalidRequest, error.to_string())
}

fn persistence_protocol_error(error: &PersistenceError) -> ProtocolError {
    ProtocolError::new(ErrorCode::Persistence, error.to_string())
}

fn cleanup_failed_persistent_creation(
    pending: PendingPublication,
    failure: PersistentStartFailure,
) -> Result<RunInfo, ProtocolError> {
    let code = if failure.is_capacity() {
        ErrorCode::RunCapacity
    } else {
        ErrorCode::Persistence
    };
    let error = ProtocolError::new(code, failure.into_error().to_string());
    if let Err(cleanup_error) = pending.cleanup_unpublished() {
        return Err(ProtocolError::new(
            error.code,
            format!(
                "{}; rollback pending: exact creation key remains fenced until all unpublished native owners are quiescent: {cleanup_error}",
                error.message
            ),
        ));
    }
    Err(error)
}

fn cleanup_unknown_persistent_creation(
    pending: PendingPublication,
    message: String,
) -> Result<RunInfo, ProtocolError> {
    let error = ProtocolError::new(ErrorCode::Persistence, message);
    if let Err(cleanup_error) = pending.cleanup_unpublished() {
        return Err(ProtocolError::new(
            error.code,
            format!(
                "{}; cleanup pending after unknown COMMIT: {cleanup_error}",
                error.message
            ),
        ));
    }
    Err(error)
}

const fn to_pty_size(size: TerminalSize) -> PtySize {
    PtySize {
        rows: size.rows,
        cols: size.cols,
        pixel_width: 0,
        pixel_height: 0,
    }
}

fn spawn_error(action: &str, error: impl fmt::Display) -> ProtocolError {
    ProtocolError::new(
        ErrorCode::SpawnFailed,
        format!("failed to {action}: {error}"),
    )
}

fn control_not_applied(error: ProtocolError) -> ControlFailure {
    ControlFailure {
        error,
        disposition: CommandDisposition::NotApplied,
    }
}

async fn handle_connection(
    stream: UnixStream,
    manager: Arc<RunManager>,
) -> Result<(), ConnectionError> {
    let mut wire = Framed::new(stream, codec());
    match receive(&mut wire).await? {
        Some(ClientFrame::Hello { hello }) if hello.protocol == PROTOCOL_VERSION => {
            send(
                &mut wire,
                &ServerFrame::Hello {
                    protocol: PROTOCOL_VERSION,
                    daemon_instance: manager.daemon_instance,
                },
            )
            .await?;
        }
        Some(ClientFrame::Hello { hello }) => {
            send(
                &mut wire,
                &ServerFrame::Error {
                    error: ProtocolError::new(
                        ErrorCode::VersionMismatch,
                        format!(
                            "client protocol {} does not match daemon protocol {}",
                            hello.protocol, PROTOCOL_VERSION
                        ),
                    ),
                },
            )
            .await?;
            return Ok(());
        }
        _ => {
            send(&mut wire, &invalid_request("first frame must be hello")).await?;
            return Ok(());
        }
    }

    let Some(frame) = receive(&mut wire).await? else {
        return Ok(());
    };
    let ClientFrame::Request { request } = frame else {
        send(&mut wire, &invalid_request("expected request after hello")).await?;
        return Ok(());
    };

    let request_permit = match manager.upgrade_requests.admit() {
        UpgradeRequestAdmission::Execute(permit) => permit,
        UpgradeRequestAdmission::Retry(permit) => {
            send(
                &mut wire,
                &ServerFrame::Error {
                    error: upgrade_retry_error(),
                },
            )
            .await?;
            drop(permit);
            return Ok(());
        }
        UpgradeRequestAdmission::Sealed => return Ok(()),
    };
    if let Request::Attach { id, after_byte } = request {
        return attachment::handle(wire, manager, id, after_byte, request_permit).await;
    }
    let response = execute_request(&manager, request).await;
    match response {
        Ok(response) => send(&mut wire, &ServerFrame::Response { response }).await?,
        Err(error) => send(&mut wire, &ServerFrame::Error { error }).await?,
    }
    drop(request_permit);
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "one exhaustive protocol dispatch keeps every public request variant visibly total"
)]
async fn execute_request(
    manager: &Arc<RunManager>,
    request: Request,
) -> Result<Response, ProtocolError> {
    match request {
        Request::Start {
            operation_key,
            spec,
        } => Ok(Response::Started {
            run: manager
                .create(operation_key, CreationRequest::Start { spec })
                .await?,
        }),
        Request::DiscoverTmux { socket_path } => {
            let operation_manager = Arc::clone(manager);
            let discovery =
                run_blocking_tmux_operation(move || operation_manager.discover_tmux(&socket_path))
                    .await?;
            Ok(Response::TmuxPanes {
                tmux_version: discovery.version,
                panes: discovery.panes,
            })
        }
        Request::ImportTmux {
            socket_path,
            pane_id,
        } => {
            manager.ensure_tmux_import_supported()?;
            let flight = manager.begin_creation_flight().await?;
            let operation_manager = Arc::clone(manager);
            let run = run_blocking_tmux_operation(move || {
                operation_manager.import_tmux(&socket_path, &pane_id, flight)
            })
            .await?;
            Ok(Response::Imported { run })
        }
        Request::Fork {
            operation_key,
            parent,
            plan,
        } => Ok(Response::Forked {
            run: manager
                .create(operation_key, CreationRequest::Fork { parent, plan })
                .await?,
        }),
        Request::List => Ok(Response::Runs {
            runs: manager.list(),
        }),
        Request::Status { id } => Ok(Response::Status {
            run: manager.info(id)?,
        }),
        Request::Input { id, data } => {
            let run = match manager.pin(id) {
                Ok(run) => run,
                Err(error) => {
                    return Ok(Response::ControlRejected {
                        failure: control_not_applied(error),
                    });
                }
            };
            Ok(short_control_response(&run, run.input(data).await))
        }
        Request::RecoverableInput { operation } => {
            recoverable_input_response(manager, operation).await
        }
        Request::Resize { id, size } => {
            let run = match manager.pin(id) {
                Ok(run) => run,
                Err(error) => {
                    return Ok(Response::ControlRejected {
                        failure: control_not_applied(error),
                    });
                }
            };
            Ok(short_control_response(&run, run.resize(size)))
        }
        Request::Signal { id, signal } => {
            let run = match manager.pin(id) {
                Ok(run) => run,
                Err(error) => {
                    return Ok(Response::ControlRejected {
                        failure: control_not_applied(error),
                    });
                }
            };
            Ok(short_control_response(&run, run.signal(signal).await))
        }
        Request::Stop { id } => {
            let run = match manager.pin(id) {
                Ok(run) => run,
                Err(error) => {
                    return Ok(Response::ControlRejected {
                        failure: control_not_applied(error),
                    });
                }
            };
            Ok(short_control_response(&run, run.stop().await))
        }
        Request::Attach { .. } => Err(ProtocolError::new(
            ErrorCode::Internal,
            "attach request reached short-lived request handler",
        )),
    }
}

async fn recoverable_input_response(
    manager: &Arc<RunManager>,
    operation: RecoverableInput,
) -> Result<Response, ProtocolError> {
    if operation.daemon_instance != manager.daemon_instance {
        return Ok(Response::ControlRejected {
            failure: control_not_applied(ProtocolError::new(
                ErrorCode::DaemonInstanceMismatch,
                "recoverable native Input belongs to another daemon incarnation",
            )),
        });
    }
    let run = match manager.pin(operation.id) {
        Ok(run) => run,
        Err(error) => {
            return Ok(Response::ControlRejected {
                failure: control_not_applied(error),
            });
        }
    };
    match run.recoverable_input(operation).await {
        Ok(range) => Ok(Response::InputApplied {
            run: run.info(),
            range,
        }),
        Err(failure) => Ok(Response::ControlRejected { failure }),
    }
}

fn short_control_response(run: &Run, result: ControlResult) -> Response {
    match result {
        Ok(receipt) => Response::ControlAccepted {
            run: run.info(),
            receipt,
        },
        Err(failure) => Response::ControlRejected { failure },
    }
}

async fn run_blocking_tmux_operation<T>(
    operation: impl FnOnce() -> Result<T, ProtocolError> + Send + 'static,
) -> Result<T, ProtocolError>
where
    T: Send + 'static,
{
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|error| {
            ProtocolError::new(
                ErrorCode::BackendUnavailable,
                format!("tmux operation worker failed: {error}"),
            )
        })?
}

fn invalid_request(message: impl Into<String>) -> ServerFrame {
    ServerFrame::Error {
        error: ProtocolError::new(ErrorCode::InvalidRequest, message),
    }
}

#[derive(Debug, Error)]
enum ConnectionError {
    #[error("transport failed: {0}")]
    Transport(#[from] LinesCodecError),
    #[error(transparent)]
    Frame(#[from] ctxmux_protocol::FrameError),
}

fn codec() -> LinesCodec {
    LinesCodec::new_with_max_length(MAX_FRAME_BYTES)
}

async fn send(
    wire: &mut Framed<UnixStream, LinesCodec>,
    frame: &ServerFrame,
) -> Result<(), ConnectionError> {
    wire.send(encode_frame(frame)?).await?;
    Ok(())
}

async fn receive(
    wire: &mut Framed<UnixStream, LinesCodec>,
) -> Result<Option<ClientFrame>, ConnectionError> {
    match wire.next().await {
        Some(Ok(line)) => Ok(Some(decode_frame(&line)?)),
        Some(Err(error)) => Err(error.into()),
        None => Ok(None),
    }
}

fn mutex_lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn read_lock<T>(lock: &RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    lock.read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn write_lock<T>(lock: &RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    lock.write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

use std::fmt;

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        fs, io,
        os::unix::{
            fs::{PermissionsExt, symlink},
            net::{UnixListener, UnixStream},
        },
        process::{Command, Stdio},
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::{Duration, Instant},
    };

    use ctxmux_client::{Client, ClientError, replay_bytes};
    use ctxmux_protocol::{
        CommandDisposition, ControlReceipt, CreateOperationKey, ErrorCode, ForkPlan,
        InterruptionReason, ProtocolError, RunBackend, RunCapabilities, RunEvent, RunId, RunInfo,
        RunSpec, RunState, StopDisposition, TerminalSize,
    };
    use portable_pty::{Child, ChildKiller, ExitStatus};
    use tokio::sync::{Barrier, Notify, broadcast, mpsc};

    use super::{
        AttachmentHookPoint, AttachmentTestHook, CreationHookPoint, CreationRequest,
        CreationTestHook, HandoffInputState, LIVE_EVENT_CAPACITY, LaunchSetupStep,
        NativeRuntimeOwner, NativeWaitFailure, OUTPUT_RETENTION_BYTES, OutputLog, OutputReplay,
        PendingTmuxPublication, Persistence, PersistenceBinding, PersistenceMode, RecoveredRun,
        Run, RunManager, ServerError, TMUX_DISCOVERY_TIMEOUT, TMUX_FAILED_IMPORT_CLEANUP_TIMEOUT,
        TMUX_IMPORT_DISCOVERY_TIMEOUT, TMUX_IMPORT_PREPARE_TIMEOUT, TMUX_IMPORT_TOTAL_TIMEOUT,
        TMUX_SHUTDOWN_TIMEOUT, TmuxCommandKind, TmuxCommandResultKind, TmuxCommandTracker,
        TmuxCommandWriter, TmuxCompletion, TmuxCompletionObservation, TmuxReaderTermination,
        TmuxRunControl, TmuxTermination, TmuxWaitCause, UpgradeRequestAdmission,
        UpgradeRequestGate, mutex_lock, prepare_socket_path, prepare_socket_path_with_hook,
        resolve_tmux_termination, serve_with_manager, serve_with_persistence_manager, spawn_error,
    };
    use crate::creation::{TerminalPublicationOwner, UnpublishedCleanupOwner};
    use crate::native_control::NativeControlOwner;

    mod creation;

    #[test]
    fn tmux_import_stages_share_one_shutdown_bounded_budget() {
        assert!(TMUX_IMPORT_DISCOVERY_TIMEOUT < TMUX_IMPORT_PREPARE_TIMEOUT);
        assert_eq!(
            TMUX_IMPORT_PREPARE_TIMEOUT + TMUX_FAILED_IMPORT_CLEANUP_TIMEOUT,
            TMUX_IMPORT_TOTAL_TIMEOUT
        );
        assert!(TMUX_IMPORT_TOTAL_TIMEOUT < TMUX_SHUTDOWN_TIMEOUT);
        assert!(TMUX_DISCOVERY_TIMEOUT < TMUX_SHUTDOWN_TIMEOUT);
    }

    #[test]
    fn upgrade_request_gate_drains_the_complete_response_window_and_reopens_on_abort() {
        let gate = UpgradeRequestGate::default();
        let UpgradeRequestAdmission::Execute(in_flight) = gate.admit() else {
            panic!("open upgrade gate admits the existing request");
        };

        let draining_gate = gate.clone();
        let drain = std::thread::spawn(move || draining_gate.begin_drain(Duration::from_secs(2)));
        let retry = loop {
            match gate.admit() {
                UpgradeRequestAdmission::Execute(permit) => {
                    drop(permit);
                    std::thread::yield_now();
                }
                UpgradeRequestAdmission::Retry(permit) => break permit,
                UpgradeRequestAdmission::Sealed => {
                    panic!("drain cannot seal while the original request is active")
                }
            }
        };

        // The first permit represents owner completion through response write;
        // the retry permit represents the explicit retry response itself. Both
        // are part of the crossing-control window and must drain before seal.
        drop(in_flight);
        assert!(
            !drain.is_finished(),
            "retry response permit still keeps upgrade extraction fenced"
        );
        drop(retry);
        let fence = drain
            .join()
            .expect("join upgrade drain")
            .expect("all admitted response windows drain");
        assert!(matches!(gate.admit(), UpgradeRequestAdmission::Sealed));

        // A pre-extract abort drops the uncommitted fence and restores full
        // admission; the current image remains a complete owner.
        drop(fence);
        let UpgradeRequestAdmission::Execute(reopened) = gate.admit() else {
            panic!("uncommitted upgrade fence must reopen admission");
        };
        drop(reopened);
    }

    #[test]
    fn upgrade_request_gate_timeout_restores_full_admission() {
        let gate = UpgradeRequestGate::default();
        let UpgradeRequestAdmission::Execute(in_flight) = gate.admit() else {
            panic!("open upgrade gate admits the existing request");
        };
        let Err(failure) = gate.begin_drain(Duration::from_millis(10)) else {
            panic!("an unfinished response window must time out the drain");
        };
        assert!(failure.contains("1 admitted request"));
        let UpgradeRequestAdmission::Execute(after_timeout) = gate.admit() else {
            panic!("timed-out pre-extract drain must restore full admission");
        };
        drop(after_timeout);
        drop(in_flight);
    }

    #[derive(Debug, Default)]
    struct WaitFailureCounts {
        try_wait: AtomicUsize,
        kill: AtomicUsize,
        wait: AtomicUsize,
        clone_killer: AtomicUsize,
        dropped: AtomicUsize,
    }

    #[derive(Debug)]
    struct WaitFailingChild(Arc<WaitFailureCounts>);

    impl Drop for WaitFailingChild {
        fn drop(&mut self) {
            self.0.dropped.fetch_add(1, Ordering::AcqRel);
        }
    }

    impl Child for WaitFailingChild {
        fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
            let attempt = self.0.try_wait.fetch_add(1, Ordering::AcqRel);
            if attempt == 0 {
                Err(io::Error::other("fixture wait authority lost"))
            } else {
                Ok(Some(ExitStatus::with_exit_code(91)))
            }
        }

        fn wait(&mut self) -> io::Result<ExitStatus> {
            self.0.wait.fetch_add(1, Ordering::AcqRel);
            Ok(ExitStatus::with_exit_code(91))
        }

        fn process_id(&self) -> Option<u32> {
            Some(42)
        }

        #[cfg(windows)]
        fn as_raw_handle(&self) -> Option<std::os::windows::io::RawHandle> {
            None
        }
    }

    impl ChildKiller for WaitFailingChild {
        fn kill(&mut self) -> io::Result<()> {
            self.0.kill.fetch_add(1, Ordering::AcqRel);
            Ok(())
        }

        fn clone_killer(&self) -> Box<dyn ChildKiller + Send + Sync> {
            self.0.clone_killer.fetch_add(1, Ordering::AcqRel);
            Box::new(WaitFailingKiller(Arc::clone(&self.0)))
        }
    }

    #[derive(Debug)]
    struct WaitFailingKiller(Arc<WaitFailureCounts>);

    impl ChildKiller for WaitFailingKiller {
        fn kill(&mut self) -> io::Result<()> {
            self.0.kill.fetch_add(1, Ordering::AcqRel);
            Ok(())
        }

        fn clone_killer(&self) -> Box<dyn ChildKiller + Send + Sync> {
            self.0.clone_killer.fetch_add(1, Ordering::AcqRel);
            Box::new(Self(Arc::clone(&self.0)))
        }
    }

    fn wait_failing_session(counts: &Arc<WaitFailureCounts>) -> super::NativeSession {
        let probe_counts = Arc::clone(counts);
        super::NativeSession::from_child_pid(42)
            .unwrap()
            .with_leader_probe_for_test(Arc::new(move || {
                probe_counts.try_wait.fetch_add(1, Ordering::AcqRel);
                Err("fixture wait authority lost".to_owned())
            }))
    }

    #[test]
    fn native_wait_error_fail_stops_once_without_dropping_or_signalling_child() {
        let run_id = RunId::new();
        let counts = Arc::new(WaitFailureCounts::default());
        let native_runs = NativeRuntimeOwner::default();
        let control = NativeControlOwner::new_for_wait_test(run_id, native_runs.owner_wake());
        let failure = NativeWaitFailure::default();
        let run = Run::new_native_for_owner_test(
            run_id,
            control.clone(),
            native_runs.clone(),
            failure.clone(),
        );
        native_runs
            .register_for_test(
                &run,
                Box::new(WaitFailingChild(Arc::clone(&counts))),
                wait_failing_session(&counts),
                control.clone(),
                failure.clone(),
                || {},
            )
            .map_err(|error| error.into_parts().0)
            .expect("register production native owner fixture");
        let deadline = Instant::now() + Duration::from_secs(2);
        while !control.retains_failed_child() {
            assert!(
                Instant::now() < deadline,
                "production owner did not fail-stop"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(counts.try_wait.load(Ordering::Acquire), 1);
        assert_eq!(counts.kill.load(Ordering::Acquire), 0);
        assert_eq!(counts.wait.load(Ordering::Acquire), 0);
        assert_eq!(counts.clone_killer.load(Ordering::Acquire), 0);
        assert_eq!(counts.dropped.load(Ordering::Acquire), 0);
        assert!(control.retains_failed_child());
        assert!(
            control
                .reap_result()
                .unwrap_err()
                .contains("fixture wait authority lost")
        );

        let stop = control.begin_stop().expect_err("failed waiter fences stop");
        assert_eq!(stop.disposition, CommandDisposition::NotApplied);
        assert_eq!(stop.error.code, ErrorCode::BackendUnavailable);
        let input = control
            .begin_input(vec![1])
            .expect_err("failed waiter fences input");
        assert_eq!(input.error.code, ErrorCode::BackendUnavailable);
        let resize = control
            .resize(TerminalSize { rows: 24, cols: 80 })
            .expect_err("failed waiter fences resize");
        assert_eq!(resize.error.code, ErrorCode::BackendUnavailable);
        let started = Instant::now();
        let reap_error = control
            .wait_until_reaped(Instant::now() + Duration::from_secs(30))
            .expect_err("authority loss can never prove reap");
        assert!(started.elapsed() < Duration::from_millis(20));
        assert!(reap_error.contains("fixture wait authority lost"));
        assert!(control.closed_quiescence_result().is_err());
        assert_eq!(counts.try_wait.load(Ordering::Acquire), 1);
        assert_eq!(counts.kill.load(Ordering::Acquire), 0);
        assert_eq!(counts.wait.load(Ordering::Acquire), 0);
        assert_eq!(counts.clone_killer.load(Ordering::Acquire), 0);
        assert_eq!(counts.dropped.load(Ordering::Acquire), 0);
        assert!(failure.creation_flights.is_fenced());
        let message = failure
            .incarnation_failure
            .message()
            .expect("daemon incarnation is failed");
        assert!(message.contains(&run_id.to_string()));
        assert!(message.contains("fixture wait authority lost"));

        let manager = RunManager::default();
        manager.registry.publish_unkeyed_for_test(run);
        let shutdown = manager
            .shutdown_owned_controls(Duration::ZERO)
            .expect_err("shutdown reports retained wait-authority failure");
        let ServerError::Shutdown { failures } = shutdown else {
            panic!("wait-authority failure has shutdown disposition");
        };
        assert!(failures.contains(&run_id.to_string()));
        assert!(failures.contains("fixture wait authority lost"));
        assert_eq!(counts.kill.load(Ordering::Acquire), 0);
        assert_eq!(counts.wait.load(Ordering::Acquire), 0);
        assert_eq!(counts.clone_killer.load(Ordering::Acquire), 0);
        assert_eq!(counts.dropped.load(Ordering::Acquire), 0);

        drop(manager);
        drop(native_runs);
        drop(control);
        assert_eq!(counts.dropped.load(Ordering::Acquire), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(
        clippy::too_many_lines,
        reason = "one continuous re-adoption proof carrying both threaded-value probes is easier to audit whole"
    )]
    async fn readopt_rebinds_live_control_and_continues_the_durable_cursor() {
        // A non-zero durable head is the continuity pivot: a from-scratch
        // reconstruction would show 0 and the next append would trip
        // persistence gap-rejection.
        const DURABLE_HEAD: u64 = 4096;

        // A live pty pair with a real child on the slave stands in for the
        // descriptors that crossed an exec-in-place upgrade: `cat` blocks
        // reading its stdin, so the pid stays live to be re-adopted, and the
        // slave stays owned by `pair` so the kernel pair is not torn down.
        let pair = portable_pty::native_pty_system()
            .openpty(portable_pty::PtySize {
                rows: 24,
                cols: 80,
                pixel_width: 0,
                pixel_height: 0,
            })
            .expect("open readopt pty pair");
        let child = pair
            .slave
            .spawn_command(portable_pty::CommandBuilder::new("/bin/cat"))
            .expect("spawn re-adopted child fixture");
        let child_pid = child.process_id().expect("re-adopted child exposes a pid");

        // Duplicate the master into an owned handle without consuming
        // `pair.master`, the same move the SIGHUP handoff makes over the
        // inherited descriptor.
        let master_fd = ctxmux_inherited_fd::duplicate_cloexec(
            pair.master
                .as_raw_fd()
                .expect("pty master exposes a raw fd"),
        )
        .expect("dup inherited master fd");

        // A persistence recovered at the non-zero durable head above.
        let directory = tempfile::tempdir().expect("create readopt persistence directory");
        let (persistence, _recovered) =
            Persistence::open(directory.path().join("state")).expect("open readopt persistence");
        let persistence_run = persistence.recovered_run(DURABLE_HEAD, 0);

        let run_id = RunId::new();
        let recovered = RecoveredRun {
            operation_key: CreateOperationKey::new("readopt-fixture")
                .expect("valid readopt operation key"),
            info: RunInfo {
                id: run_id,
                spec: None,
                lineage: None,
                backend: RunBackend::Native,
                capabilities: RunCapabilities::NATIVE,
                // A recovered `running` row's DB pid column is NULL (the pid is
                // only written at `finalize`), so `readopt` must derive the live
                // pid from the `child_pid` manifest parameter, not from the row.
                pid: None,
                state: RunState::Running,
                latest_output_bytes: DURABLE_HEAD,
                durable_output_bytes: Some(DURABLE_HEAD),
                first_available_byte: DURABLE_HEAD,
                attachments: 0,
                applied_input_bytes: Some(0),
            },
            // Committed durable bytes with none retained in memory: the honest
            // replay of a Run whose output crossed the exec on disk only.
            replay: OutputReplay {
                chunks: Vec::new(),
                first_available_byte: DURABLE_HEAD,
                latest_output_bytes: DURABLE_HEAD,
                truncated: true,
            },
            metadata_bytes: 0,
        };

        let native_runs = NativeRuntimeOwner::default();

        // The two manager-shared values now flow in from the caller — matching
        // the production spawn seam — instead of being fabricated inside
        // `readopt`. Each is wired to a probe so the threading is load-bearing:
        //
        // * `input_drains` carries a probe `QualificationStats` sink. The spawn
        //   path threads the DAEMON-WIDE gate so every run shares one input
        //   concurrency budget; a re-adopted run must join THAT gate, not a
        //   fresh one. Passing a DISTINCT no-sink `qualification_stats` param
        //   below is the crux: the only way this probe sink can ever observe an
        //   `InputDrains` gauge pulse is if `readopt` actually schedules input
        //   through the gate we pass here. The pre-fix code discarded this gate
        //   and rebuilt one from `qualification_stats`, leaving the probe silent.
        let (input_drain_frames, input_drain_sink) =
            UnixStream::pair().expect("open input-drain probe stats stream");
        let input_drain_stats = crate::qualification_stats::QualificationStats::from_sink(
            input_drain_sink.into(),
            "readopt-input-drain-probe",
        )
        .expect("open input-drain probe stats");
        let input_drains =
            crate::native_control::InputDrainGate::with_stats(input_drain_stats.clone());

        // `wait_failure` carries a probe `IncarnationFailure` — the value the
        // serve loop's fail-stop arm watches — wired exactly as the manager
        // wires it (mirrors `native_wait_failure_exits_daemon_without_a_terminal_
        // event`). A pre-fix `NativeWaitFailure::default()` would record wait-
        // authority loss into a DETACHED incarnation the daemon never watches.
        let incarnation = super::IncarnationFailure::default();
        let wait_failure = NativeWaitFailure {
            creation_flights: crate::creation::CreationFlightOwner::default(),
            incarnation_failure: incarnation.clone(),
        };

        let run = Run::readopt(
            recovered,
            persistence_run,
            master_fd,
            child_pid,
            HandoffInputState::empty(),
            native_runs.clone(),
            LIVE_EVENT_CAPACITY,
            TerminalPublicationOwner::default(),
            crate::qualification_stats::QualificationStats::default(),
            input_drains,
            wait_failure,
        )
        .expect("readopt rebinds live control onto the recovered Run");

        // Snapshot before any I/O so the durable assertion cannot be perturbed
        // by echoed output committing asynchronously.
        let info = run.info();
        assert_eq!(info.state, RunState::Running);
        assert_eq!(info.pid, Some(child_pid));
        // Live native control is bound (`recover` leaves this `None`).
        assert!(run.native_control().is_ok());
        // Continuity proof: the durable cursor reuses the recovered head — it
        // does NOT reset to zero.
        assert_eq!(info.durable_output_bytes, Some(DURABLE_HEAD));

        // The master fd is live: a resize round-trips through the adopted
        // adapter (a non-tty fd would return ENOTTY here).
        run.resize(TerminalSize {
            rows: 40,
            cols: 132,
        })
        .expect("resize the re-adopted live master");

        // The input path is live end to end: bytes reach the real pty master
        // and the applied cursor advances by exactly the bytes written.
        run.input(b"readopt\n".to_vec())
            .await
            .expect("drive input through the re-adopted control");
        assert_eq!(run.info().applied_input_bytes, Some(8));

        // Defect 2 (resource governance) — the load-bearing proof of the fix.
        // The re-adopted run scheduled its input through the manager-shared
        // `InputDrainGate` we passed in, shown by the gauge pulse landing on
        // THAT gate's probe stats sink. `begin_input` sets the `InputDrains`
        // gauge synchronously before the `PendingInput` is returned, so the
        // pulse is already recorded by the time the await above resolves. The
        // pre-fix code discarded the passed gate and rebuilt a fresh one from
        // the SEPARATE no-sink `qualification_stats` param, so this probe sink
        // could never pulse — making the assertion below genuinely load-bearing
        // against the old fabricated gate (it goes red without the fix).
        input_drain_stats.finish();
        let observed_input_drain_gauge = {
            use std::io::BufRead;
            std::io::BufReader::new(input_drain_frames)
                .lines()
                .map(|line| {
                    serde_json::from_str::<serde_json::Value>(&line.expect("probe frame line"))
                        .expect("probe frame parses")
                })
                .filter_map(|frame| {
                    frame["high_water"][crate::qualification_stats::Gauge::InputDrains as usize]
                        .as_u64()
                })
                .max()
                .expect("probe stats emitted at least one frame")
        };
        assert!(
            observed_input_drain_gauge >= 1,
            "re-adopted input must schedule through the manager-shared gate we passed in, \
             not a fresh gate rebuilt from the separate qualification_stats"
        );

        // Defect 1 (reliability) guard. The strongest discriminator would be
        // observing a wait-authority-loss record land in this probe
        // `incarnation_failure` (proving `readopt` used the passed-in one, not a
        // detached `NativeWaitFailure::default()`). But `readopt` builds its own
        // real `AdoptedChild` from the live pid, so there is no wait-failing
        // child seam here: forcing a `record()` would need a reap-race or a
        // worker-spawn-failure injection that leaves the child unreaped — either
        // one regresses the clean-reap / no-zombie assertions below. Per the
        // "avoid over-validation" directive we do not contort the test for it;
        // the shared-gate observation above is the load-bearing proof that the
        // caller-threaded values reach the seam. Here we only assert the clean
        // re-adoption never spuriously fenced the daemon.
        assert!(
            incarnation.message().is_none(),
            "a successful re-adoption must not record an incarnation failure"
        );

        // Scope note: this test covers Bug 1 (pid derivation / caller-threaded
        // values) only. The terminal-ordinal single-set contract (Bug 2 — a
        // live re-adopted run must defer to `publish()` and never `recover()`)
        // is covered by the focused
        // `recover_then_publish_on_the_same_cell_panics_the_single_set_contract`
        // in creation.rs.
        // Teardown: Stop drives TERM + reap through the owner (the sole reaper,
        // via waitid), so no zombie survives. Our own `child` handle never
        // waits — `std::process::Child::drop` does not reap — so there is no
        // double-reap race.
        run.stop().await.expect("stop reaps the re-adopted child");
        drop(child);
        drop(run);
        drop(native_runs);
        // The pty pair stayed alive through every round-trip above.
        drop(pair);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn admitted_stop_receives_natural_exit_reap_after_the_receiver_poll_gap() {
        let run_id = RunId::new();
        let counts = Arc::new(WaitFailureCounts::default());
        let native_runs = NativeRuntimeOwner::default();
        let control = NativeControlOwner::new_for_wait_test(run_id, native_runs.owner_wake());
        let probe_reached = Arc::new(std::sync::Barrier::new(2));
        let release_probe = Arc::new(std::sync::Barrier::new(2));
        let probe_calls = Arc::new(AtomicUsize::new(0));
        let session = super::NativeSession::from_child_pid(2_000_000_000)
            .unwrap()
            .with_leader_probe_for_test(Arc::new({
                let probe_reached = Arc::clone(&probe_reached);
                let release_probe = Arc::clone(&release_probe);
                let probe_calls = Arc::clone(&probe_calls);
                move || {
                    if probe_calls.fetch_add(1, Ordering::AcqRel) == 0 {
                        probe_reached.wait();
                        release_probe.wait();
                    }
                    Ok(true)
                }
            }));
        let failure = NativeWaitFailure::default();
        let run = Run::new_native_for_owner_test(
            run_id,
            control.clone(),
            native_runs.clone(),
            failure.clone(),
        );
        native_runs
            .register_for_test(
                &run,
                Box::new(WaitFailingChild(Arc::clone(&counts))),
                session,
                control.clone(),
                failure,
                || {},
            )
            .map_err(|error| error.into_parts().0)
            .expect("register production natural-exit fixture");

        // The waiter has already observed an empty receive poll and is paused
        // immediately before publishing natural terminal ownership.
        probe_reached.wait();
        let pending = control
            .begin_stop()
            .expect("Stop is admitted before the natural-exit owner fence");
        release_probe.wait();

        assert_eq!(
            pending
                .resolve(Duration::from_secs(1))
                .await
                .expect("admitted Stop reuses final reap evidence"),
            ControlReceipt::Stop {
                disposition: StopDisposition::Graceful
            }
        );
        tokio::time::timeout(Duration::from_secs(2), async {
            while !matches!(
                run.info().state,
                RunState::Exited {
                    code: 91,
                    signal: None
                }
            ) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("production owner publishes the natural terminal state");
        assert_eq!(counts.kill.load(Ordering::Acquire), 0);
        assert_eq!(counts.wait.load(Ordering::Acquire), 1);
        control
            .reap_result()
            .expect("natural-exit Stop proves reap");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn native_wait_failure_exits_daemon_without_a_terminal_event() {
        let directory = tempfile::tempdir().expect("create daemon failure fixture directory");
        let socket = directory.path().join("ctxmux.sock");
        let manager = Arc::new(RunManager::default());
        let run_id = RunId::new();
        let counts = Arc::new(WaitFailureCounts::default());
        let control =
            NativeControlOwner::new_for_wait_test(run_id, manager.native_runs.owner_wake());
        let wait_failure = NativeWaitFailure {
            creation_flights: manager.creation_flights.clone(),
            incarnation_failure: manager.incarnation_failure.clone(),
        };
        let run = Run::new_native_for_owner_test(
            run_id,
            control.clone(),
            manager.native_runs.clone(),
            wait_failure.clone(),
        );
        manager.registry.publish_unkeyed_for_test(Arc::clone(&run));

        let server_manager = Arc::clone(&manager);
        let server_socket = socket.clone();
        let (server_result_tx, server_result_rx) = std::sync::mpsc::sync_channel(1);
        let server = std::thread::Builder::new()
            .name("ctxmux-wait-failure-server".to_owned())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()
                    .expect("build dedicated daemon runtime");
                let result = runtime.block_on(serve_with_persistence_manager(
                    server_socket,
                    server_manager,
                    None,
                    None,
                    None,
                ));
                drop(runtime);
                let _ = server_result_tx.send(result);
            })
            .expect("start dedicated daemon runtime");

        let client = Client::new(&socket);
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if client.ping().await.is_ok() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("daemon publishes the fixture socket");
        let (attachment, snapshot) = client
            .attach(run_id, 0)
            .await
            .expect("attach through the public client before wait authority fails");
        assert_eq!(snapshot.run.state, RunState::Running);

        manager
            .native_runs
            .register_for_test(
                &run,
                Box::new(WaitFailingChild(Arc::clone(&counts))),
                wait_failing_session(&counts),
                control,
                wait_failure,
                || {},
            )
            .map_err(|error| error.into_parts().0)
            .expect("register production wait-authority fixture");
        let event = tokio::time::timeout(Duration::from_secs(2), attachment.next_event())
            .await
            .expect("daemon failure closes the public attachment");
        assert!(
            matches!(event, Err(ClientError::Closed)),
            "pre-terminal daemon exit must not look like a clean terminal EOF: {event:?}"
        );
        assert_eq!(counts.try_wait.load(Ordering::Acquire), 1);

        let result = server_result_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("dedicated daemon runtime reports wait-authority failure");
        let Err(ServerError::Shutdown { failures }) = result else {
            panic!("daemon must fail its serving incarnation: {result:?}");
        };
        assert!(failures.contains(&run_id.to_string()));
        assert!(failures.contains("fixture wait authority lost"));
        assert!(
            client.ping().await.is_err(),
            "failed daemon incarnation must not leave a connectable socket"
        );
        server.join().expect("join dedicated daemon runtime");
    }

    #[test]
    fn tmux_completion_receipt_is_reusable_and_timeout_preserves_pending() {
        let (observed_tx, observed_rx) = std::sync::mpsc::sync_channel(1);
        let mut observed = TmuxCompletion::Pending(observed_rx);
        assert!(matches!(
            observed.observe(),
            TmuxCompletionObservation::Pending
        ));
        observed_tx.send(Ok(())).expect("publish tmux completion");
        assert!(matches!(
            observed.observe(),
            TmuxCompletionObservation::Complete(Ok(()))
        ));
        assert!(matches!(
            observed.observe(),
            TmuxCompletionObservation::Complete(Ok(()))
        ));
        assert_eq!(observed.wait(Duration::ZERO), Ok(()));

        let (waited_tx, waited_rx) = std::sync::mpsc::sync_channel(1);
        let mut waited = TmuxCompletion::Pending(waited_rx);
        waited_tx.send(Ok(())).expect("publish tmux completion");
        assert_eq!(waited.wait(Duration::ZERO), Ok(()));
        assert_eq!(waited.wait(Duration::ZERO), Ok(()));
        assert!(matches!(
            waited.observe(),
            TmuxCompletionObservation::Complete(Ok(()))
        ));

        let (pending_tx, pending_rx) = std::sync::mpsc::sync_channel(1);
        let mut pending = TmuxCompletion::Pending(pending_rx);
        assert_eq!(
            pending.wait(Duration::ZERO),
            Err("timed out waiting for tmux control cleanup".to_owned())
        );
        assert!(matches!(
            pending.observe(),
            TmuxCompletionObservation::Pending
        ));

        let explicit_failure = "tmux control failed".to_owned();
        pending_tx
            .send(Err(explicit_failure.clone()))
            .expect("publish tmux failure");
        let TmuxCompletionObservation::Complete(Err(first)) = pending.observe() else {
            panic!("explicit completion failure is retained");
        };
        let TmuxCompletionObservation::Complete(Err(second)) = pending.observe() else {
            panic!("cached completion failure remains observable");
        };
        assert_eq!(first, explicit_failure);
        assert_eq!(second, explicit_failure);
        assert_eq!(pending.wait(Duration::ZERO), Err(explicit_failure));

        let (disconnected_tx, disconnected_rx) = std::sync::mpsc::sync_channel(1);
        let mut disconnected = TmuxCompletion::Pending(disconnected_rx);
        drop(disconnected_tx);
        let expected_disconnect =
            "tmux control waiter ended without a completion receipt".to_owned();
        assert_eq!(
            disconnected.wait(Duration::ZERO),
            Err(expected_disconnect.clone())
        );
        assert_eq!(
            disconnected.wait(Duration::ZERO),
            Err(expected_disconnect.clone())
        );
        let TmuxCompletionObservation::Complete(Err(observed_disconnect)) = disconnected.observe()
        else {
            panic!("disconnected completion fails closed");
        };
        assert_eq!(observed_disconnect, expected_disconnect);
    }

    #[test]
    fn pending_tmux_publication_transfers_overlap_until_cleanup_is_proven() {
        let cleanup_owner = UnpublishedCleanupOwner::default();
        let cleanup_reservation = cleanup_owner
            .reserve_tmux()
            .expect("reserve one tmux physical-overlap owner");
        let (completion_tx, completion_rx) = std::sync::mpsc::sync_channel(1);
        let (run, command_rx) = tmux_cleanup_test_run(completion_rx);

        drop(PendingTmuxPublication::new(
            Arc::clone(&run),
            cleanup_reservation,
        ));
        assert!(matches!(
            command_rx.try_recv(),
            Ok(super::TmuxControlCommand::Interrupt(
                InterruptionReason::TmuxServerUnavailable
            ))
        ));
        assert_eq!(cleanup_owner.owned_count(), 1);
        assert_eq!(cleanup_owner.unresolved_count(), 1);

        completion_tx
            .send(Ok(()))
            .expect("publish exact tmux cleanup completion");
        assert_eq!(cleanup_owner.unresolved_count(), 1);
        assert_eq!(cleanup_owner.owned_count(), 1);
        drop(run);
        assert_eq!(cleanup_owner.unresolved_count(), 0);
        assert_eq!(cleanup_owner.owned_count(), 0);

        let fail_stop_reservation = cleanup_owner
            .reserve_tmux()
            .expect("reserve fail-stop tmux overlap owner");
        let (disconnected_tx, disconnected_rx) = std::sync::mpsc::sync_channel(1);
        drop(disconnected_tx);
        let (fail_stop_run, _) = tmux_cleanup_test_run(disconnected_rx);
        drop(PendingTmuxPublication::new(
            fail_stop_run,
            fail_stop_reservation,
        ));
        assert_eq!(cleanup_owner.unresolved_count(), 1);
        let failures = cleanup_owner.wait_until(Instant::now());
        assert_eq!(failures.len(), 1);
        assert!(failures[0].contains("without a completion receipt"));

        let mut remaining = Vec::new();
        for _ in 1..8 {
            remaining.push(
                cleanup_owner
                    .reserve_tmux()
                    .expect("a fail-stop owner leaves only the remaining bounded slots"),
            );
        }
        assert_eq!(
            cleanup_owner
                .reserve_tmux()
                .err()
                .expect("ninth shared overlap owner is rejected")
                .code,
            ErrorCode::BackendUnavailable
        );
        drop(remaining);
        assert_eq!(cleanup_owner.owned_count(), 1);
    }

    #[test]
    fn failed_tmux_readiness_keeps_overlap_until_worker_run_owner_settles() {
        let cleanup_owner = UnpublishedCleanupOwner::default();
        let cleanup_reservation = cleanup_owner
            .reserve_tmux()
            .expect("reserve one tmux physical-overlap owner");
        let (completion_tx, completion_rx) = std::sync::mpsc::sync_channel(1);
        let (run, command_rx) = tmux_cleanup_test_run(completion_rx);
        let worker_run = Arc::clone(&run);
        let pending = PendingTmuxPublication::new(run, cleanup_reservation);
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
        ready_tx
            .send(Err(ProtocolError::new(
                ErrorCode::BackendUnavailable,
                "injected tmux readiness failure",
            )))
            .expect("publish tmux readiness failure");
        completion_tx
            .send(Ok(()))
            .expect("publish exact tmux cleanup completion");

        let Err(error) = Run::finish_tmux_import(
            pending,
            &ready_rx,
            Instant::now() + Duration::from_secs(1),
            Instant::now() + Duration::from_secs(2),
        ) else {
            panic!("readiness failure must reject tmux publication");
        };
        assert_eq!(error.code, ErrorCode::BackendUnavailable);
        assert_eq!(error.message, "injected tmux readiness failure");
        assert!(matches!(
            command_rx.try_recv(),
            Ok(super::TmuxControlCommand::Interrupt(
                InterruptionReason::TmuxServerUnavailable
            ))
        ));
        assert_eq!(cleanup_owner.unresolved_count(), 1);
        assert_eq!(cleanup_owner.owned_count(), 1);

        drop(worker_run);
        assert_eq!(cleanup_owner.unresolved_count(), 0);
        assert_eq!(cleanup_owner.owned_count(), 0);
    }

    fn tmux_cleanup_test_run(
        completion: std::sync::mpsc::Receiver<Result<(), String>>,
    ) -> (
        Arc<Run>,
        std::sync::mpsc::Receiver<super::TmuxControlCommand>,
    ) {
        let (commands, command_rx) = std::sync::mpsc::channel();
        let run = Arc::new(Run {
            id: RunId::new(),
            spec: None,
            lineage: None,
            backend: RunBackend::Tmux {
                socket_path: "/tmp/ctxmux-test-tmux.sock".to_owned(),
                server_pid: 1,
                server_started_at: 1,
                session_id: "$1".to_owned(),
                window_id: "@1".to_owned(),
                pane_id: "%1".to_owned(),
                tmux_version: "3.4".to_owned(),
            },
            capabilities: RunCapabilities::TMUX_READ_ONLY,
            pid: Some(1),
            state: Mutex::new(RunState::Running),
            output: Mutex::new(OutputLog::with_initial_truncation()),
            incarnation_control: Some(super::RunControl::Tmux(TmuxRunControl {
                writer: Mutex::new(None),
                commands,
                completion: Mutex::new(TmuxCompletion::Pending(completion)),
            })),
            native_runs: None,
            persistence_mode: PersistenceMode::MemoryOnly,
            persistence_transition: Mutex::new(()),
            persistence: Mutex::new(PersistenceBinding::Disabled),
            attachments: std::sync::atomic::AtomicUsize::new(0),
            qualification_stats: crate::qualification_stats::QualificationStats::default(),
            terminal_publications: TerminalPublicationOwner::default(),
            terminal_ordinal: std::sync::OnceLock::new(),
            events: super::LiveEventOwner::new(1),
        });
        (run, command_rx)
    }

    #[test]
    fn tmux_control_writer_close_is_owner_bound_idempotent_and_fail_closed() {
        let mut child = Command::new("/bin/sh")
            .args(["-c", "cat >/dev/null"])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn tmux writer close sentinel");
        let stdin = child.stdin.take().expect("take sentinel stdin");
        let (commands, _command_rx) = std::sync::mpsc::channel();
        let (_completion_tx, completion) = std::sync::mpsc::channel::<Result<(), String>>();
        let control = TmuxRunControl {
            writer: std::sync::Mutex::new(Some(TmuxCommandWriter::new(stdin))),
            commands,
            completion: std::sync::Mutex::new(TmuxCompletion::Pending(completion)),
        };

        control
            .with_writer(|writer| {
                writer
                    .establish_session_and_write(TmuxCommandKind::TargetProbe, b"display-message\n")
            })
            .expect("write while the Control owner is live");
        assert!(control.close_writer());
        assert!(!control.close_writer());

        let error = control
            .with_writer(|writer| writer.write_periodic_probe(b"display-message\n"))
            .expect_err("closed Control writer must reject writes");
        assert_eq!(error.kind(), std::io::ErrorKind::NotConnected);
        assert_eq!(error.to_string(), "tmux control client is closed");
        assert_eq!(
            control.correlate_result(0).unwrap_err(),
            "tmux control client is closed"
        );

        for _ in 0..100 {
            if child
                .try_wait()
                .expect("poll tmux writer close sentinel")
                .is_some()
            {
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let _ = child.kill();
        let _ = child.wait();
        panic!("dropping the Control writer did not close its child pipe");
    }

    #[test]
    fn tmux_command_tracker_bounds_and_deduplicates_serial_commands() {
        let mut tracker = TmuxCommandTracker::default();
        assert_eq!(
            tracker
                .prepare_enqueue(TmuxCommandKind::Continue)
                .unwrap_err(),
            "tmux adapter command arrived before session establishment"
        );
        assert!(tracker.observe_session());
        assert!(!tracker.observe_session());

        assert!(
            tracker
                .prepare_enqueue(TmuxCommandKind::TargetProbe)
                .unwrap()
        );
        tracker.commit_enqueue(TmuxCommandKind::TargetProbe);
        assert!(
            !tracker
                .prepare_enqueue(TmuxCommandKind::TargetProbe)
                .unwrap()
        );

        for _ in 0..64 {
            if tracker.prepare_enqueue(TmuxCommandKind::Continue).unwrap() {
                tracker.commit_enqueue(TmuxCommandKind::Continue);
            }
        }
        assert_eq!(tracker.pending.len(), TmuxCommandTracker::MAX_PENDING);
        assert_eq!(
            tracker.correlate_result(10).unwrap(),
            TmuxCommandResultKind::Pending(TmuxCommandKind::TargetProbe)
        );
        assert_eq!(
            tracker.correlate_result(42).unwrap(),
            TmuxCommandResultKind::Pending(TmuxCommandKind::Continue)
        );
        assert!(tracker.pending.is_empty());
    }

    #[test]
    fn tmux_command_tracker_allows_one_pre_session_bootstrap_and_monotonic_gaps() {
        let mut tracker = TmuxCommandTracker::default();
        assert_eq!(
            tracker.correlate_result(0).unwrap(),
            TmuxCommandResultKind::Bootstrap
        );
        assert_eq!(
            tracker.correlate_result(7).unwrap_err(),
            "tmux returned a command result without a pending adapter command"
        );

        let mut nonzero = TmuxCommandTracker::default();
        assert_eq!(
            nonzero.correlate_result(41).unwrap(),
            TmuxCommandResultKind::Bootstrap
        );
        assert!(nonzero.observe_session());
        assert!(
            nonzero
                .prepare_enqueue(TmuxCommandKind::TargetProbe)
                .unwrap()
        );
        nonzero.commit_enqueue(TmuxCommandKind::TargetProbe);
        assert_eq!(
            nonzero.correlate_result(47).unwrap(),
            TmuxCommandResultKind::Pending(TmuxCommandKind::TargetProbe)
        );
    }

    #[test]
    fn tmux_command_tracker_rejects_duplicate_and_backward_numbers_before_pop() {
        for invalid in [7, 6] {
            let mut tracker = TmuxCommandTracker::default();
            assert_eq!(
                tracker.correlate_result(7).unwrap(),
                TmuxCommandResultKind::Bootstrap
            );
            assert!(tracker.observe_session());
            assert!(
                tracker
                    .prepare_enqueue(TmuxCommandKind::TargetProbe)
                    .unwrap()
            );
            tracker.commit_enqueue(TmuxCommandKind::TargetProbe);
            assert_eq!(
                tracker.correlate_result(invalid).unwrap_err(),
                "tmux command result number did not advance"
            );
            assert_eq!(tracker.pending.front(), Some(&TmuxCommandKind::TargetProbe));
        }

        let mut ready = TmuxCommandTracker::default();
        assert!(ready.observe_session());
        assert_eq!(
            ready.correlate_result(0).unwrap_err(),
            "tmux returned a command result without a pending adapter command"
        );
    }

    #[test]
    fn tmux_child_exit_resolution_preserves_the_reader_protocol_receipt() {
        let observed = TmuxTermination {
            error: ProtocolError::new(
                ErrorCode::BackendUnavailable,
                "Control Mode stream ended inside a command block",
            ),
            reason: InterruptionReason::TmuxProtocolError,
        };

        for cause in [TmuxWaitCause::ReaderTerminated, TmuxWaitCause::ChildExited] {
            assert_eq!(
                resolve_tmux_termination(
                    cause,
                    Some(TmuxReaderTermination {
                        failure: observed.clone(),
                    }),
                    42,
                ),
                observed,
            );
        }
    }

    #[test]
    fn tmux_owner_causes_are_not_overwritten_by_cleanup_eof() {
        let cleanup_eof = TmuxReaderTermination {
            failure: TmuxTermination {
                error: ProtocolError::new(
                    ErrorCode::BackendUnavailable,
                    "tmux Control Mode stream closed",
                ),
                reason: InterruptionReason::TmuxServerUnavailable,
            },
        };
        let cases = [
            (
                TmuxWaitCause::Interrupted(InterruptionReason::TmuxTargetChanged),
                InterruptionReason::TmuxTargetChanged,
                ErrorCode::TargetChanged,
                "interrupted",
            ),
            (
                TmuxWaitCause::Shutdown,
                InterruptionReason::TmuxServerUnavailable,
                ErrorCode::BackendUnavailable,
                "shutdown",
            ),
            (
                TmuxWaitCause::CommandChannelClosed,
                InterruptionReason::TmuxServerUnavailable,
                ErrorCode::BackendUnavailable,
                "command channel closed",
            ),
            (
                TmuxWaitCause::SocketTargetChanged,
                InterruptionReason::TmuxTargetChanged,
                ErrorCode::TargetChanged,
                "socket identity changed",
            ),
            (
                TmuxWaitCause::ProbeWriteFailed("broken pipe".to_owned()),
                InterruptionReason::TmuxServerUnavailable,
                ErrorCode::BackendUnavailable,
                "broken pipe",
            ),
            (
                TmuxWaitCause::ChildStatusFailed("fixture status failure".to_owned()),
                InterruptionReason::TmuxServerUnavailable,
                ErrorCode::BackendUnavailable,
                "fixture status failure",
            ),
        ];

        for (cause, expected_reason, expected_code, expected_detail) in cases {
            let resolved = resolve_tmux_termination(cause, Some(cleanup_eof.clone()), 42);
            assert_eq!(resolved.reason, expected_reason);
            assert_eq!(resolved.error.code, expected_code);
            assert!(
                resolved.error.message.contains(expected_detail),
                "owner detail was lost: {}",
                resolved.error.message,
            );
        }
    }

    #[test]
    fn post_spawn_setup_failures_terminate_reap_and_publish_nothing() {
        // DR-001: every rejected post-spawn transition rolls child ownership back.
        for failed_step in [
            LaunchSetupStep::CloneReader,
            LaunchSetupStep::TakeWriter,
            LaunchSetupStep::RegisterOutputOwner,
            LaunchSetupStep::RegisterWaitOwner,
        ] {
            let manager = RunManager::default();
            let operation_key =
                CreateOperationKey::new(format!("post-spawn-setup-{}", failed_step as u8))
                    .expect("fixture operation key");
            let spec = long_running_spec();
            let request = CreationRequest::Start { spec: spec.clone() };
            let captured_run = Arc::new(Mutex::new(None));
            let mut failed_pid = None;
            let error = manager
                .start_with_setup(
                    operation_key.clone(),
                    spec.clone(),
                    &captured_run,
                    |step, pid| {
                        if step == failed_step {
                            if matches!(
                                step,
                                LaunchSetupStep::CloneReader | LaunchSetupStep::TakeWriter
                            ) {
                                assert!(pid.is_none(), "PTY views are prepared before spawn");
                            } else {
                                let pid = pid.expect("post-spawn setup exposes its child pid");
                                assert!(process_exists(pid), "fixture child must start live");
                                failed_pid = Some(pid);
                            }
                            return Err(spawn_error("complete injected setup step", "fixture"));
                        }
                        Ok(())
                    },
                )
                .expect_err("injected setup failure rejects start");

            assert_eq!(error.code, ErrorCode::SpawnFailed);
            assert!(manager.list().is_empty(), "failed start published a Run");
            if matches!(
                failed_step,
                LaunchSetupStep::RegisterOutputOwner | LaunchSetupStep::RegisterWaitOwner
            ) {
                let pid = failed_pid.expect("post-spawn fixture records the rejected child pid");
                assert!(
                    !process_exists(pid),
                    "{failed_step:?} left child {pid} live or unreaped"
                );
                assert!(
                    mutex_lock(&captured_run).is_some(),
                    "post-construction failure retains the injected Run owner"
                );
                assert_eq!(manager.unpublished_cleanups.owned_count(), 1);
                assert_eq!(manager.unpublished_cleanups.unresolved_count(), 1);
                let matching = manager
                    .unpublished_cleanups
                    .resolve_fence(&operation_key, &request)
                    .expect_err("matching setup retry remains fenced");
                assert_eq!(matching.code, ErrorCode::BackendUnavailable);
                let mut conflicting_spec = spec.clone();
                conflicting_spec.args.push("different".to_owned());
                let conflicting = manager
                    .unpublished_cleanups
                    .resolve_fence(
                        &operation_key,
                        &CreationRequest::Start {
                            spec: conflicting_spec,
                        },
                    )
                    .expect_err("conflicting setup retry sees the same fence");
                assert_eq!(conflicting.code, ErrorCode::CreationConflict);
                mutex_lock(&captured_run).take();
                let pending = manager
                    .unpublished_cleanups
                    .wait_until(Instant::now() + Duration::from_secs(2));
                assert!(
                    pending.is_empty(),
                    "{failed_step:?} released setup owner did not reach full native quiescence: {pending:?}"
                );
            } else {
                assert!(failed_pid.is_none(), "pre-spawn setup launches no child");
                assert!(mutex_lock(&captured_run).is_none());
                assert_eq!(manager.unpublished_cleanups.owned_count(), 0);
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_spec_semantics_map_to_invalid_request_for_start_fork_and_resize() {
        let manager = RunManager::default();

        let mut invalid_start = long_running_spec();
        invalid_start.program.clear();
        let start_error = manager
            .start(invalid_start)
            .expect_err("empty start program must fail");
        assert_eq!(start_error.code, ErrorCode::InvalidRequest);
        assert_eq!(start_error.message, "Run program must not be empty");

        let parent = manager
            .start(long_running_spec())
            .expect("start valid fork parent");
        let mut invalid_fork = long_running_spec();
        invalid_fork.program.clear();
        let fork_error = manager
            .fork(parent.id, ForkPlan::LevelB { spec: invalid_fork })
            .expect_err("invalid materialized fork must fail");
        assert_eq!(fork_error.code, ErrorCode::InvalidRequest);
        assert_eq!(fork_error.message, "Run program must not be empty");

        let run = manager.get(parent.id).expect("resolve resize fixture Run");
        let resize_error = run
            .resize(TerminalSize { cols: 0, rows: 24 })
            .expect_err("zero-width resize must fail");
        assert_eq!(resize_error.error.code, ErrorCode::InvalidRequest);
        assert_eq!(
            resize_error.error.message,
            "terminal rows and columns must be greater than zero"
        );
        run.stop().await.expect("stop validation fixture Run");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stop_after_wait_disables_signalling_before_state_publication() {
        struct ChildGuard(std::process::Child);

        impl Drop for ChildGuard {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }

        struct ReleaseGuard(Option<std::sync::mpsc::SyncSender<()>>);

        impl Drop for ReleaseGuard {
            fn drop(&mut self) {
                if let Some(sender) = self.0.take() {
                    let _ = sender.send(());
                }
            }
        }

        let (wait_reached_tx, wait_reached_rx) = std::sync::mpsc::sync_channel(0);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
        let mut release = ReleaseGuard(Some(release_tx));
        let run = Run::spawn_with_wait_hook(
            RunSpec {
                program: "/bin/sh".to_owned(),
                args: vec!["-c".to_owned(), "exit 0".to_owned()],
                cwd: None,
                env: BTreeMap::new(),
                size: TerminalSize::default(),
                declared_inputs: Vec::new(),
            },
            PersistenceMode::MemoryOnly,
            move || {
                wait_reached_tx.send(()).expect("publish wait barrier");
                release_rx.recv().expect("release wait barrier");
            },
        )
        .expect("spawn short-lived barrier Run");
        wait_reached_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("child wait reaches publication barrier");

        let unrelated = Command::new("/bin/sh")
            .args(["-c", "sleep 30"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn unrelated identity sentinel");
        let unrelated_pid = unrelated.id();
        let _unrelated = ChildGuard(unrelated);
        assert!(process_exists(unrelated_pid));

        let error = run
            .stop()
            .await
            .expect_err("reaped child rejects stop before state publication");
        assert_eq!(error.error.code, ErrorCode::InvalidRunState);
        assert!(
            process_exists(unrelated_pid),
            "stop after wait signalled unrelated identity {unrelated_pid}"
        );

        release
            .0
            .take()
            .expect("barrier release is present")
            .send(())
            .expect("release state publication");
        let deadline = Instant::now() + Duration::from_secs(5);
        while run.info().state.is_running() {
            assert!(Instant::now() < deadline, "Run state was not published");
            std::thread::yield_now();
        }
    }

    struct InProcessServer {
        directory: tempfile::TempDir,
        client: Client,
        manager: Arc<RunManager>,
        task: tokio::task::JoinHandle<Result<(), ServerError>>,
    }

    impl InProcessServer {
        fn start(manager: Arc<RunManager>) -> Self {
            let directory = tempfile::tempdir().expect("create in-process server directory");
            let socket = directory.path().join("ctxmux.sock");
            let listener =
                tokio::net::UnixListener::bind(&socket).expect("bind in-process server socket");
            let task = tokio::spawn(serve_with_manager(
                socket.clone(),
                listener,
                Arc::clone(&manager),
                None,
                None,
                None,
            ));
            Self {
                directory,
                client: Client::new(socket),
                manager,
                task,
            }
        }
    }

    impl Drop for InProcessServer {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    fn hooked_server(
        point: AttachmentHookPoint,
    ) -> (
        InProcessServer,
        Arc<AttachmentTestHook>,
        mpsc::UnboundedReceiver<()>,
    ) {
        let (reached_tx, reached_rx) = mpsc::unbounded_channel();
        let hook = Arc::new(AttachmentTestHook {
            point,
            armed: AtomicBool::new(true),
            reached: reached_tx,
            release: Notify::new(),
        });
        let manager = Arc::new(RunManager {
            attachment_hook: Some(Arc::clone(&hook)),
            ..RunManager::default()
        });
        (InProcessServer::start(manager), hook, reached_rx)
    }

    async fn wait_for_exit(client: &Client, id: ctxmux_protocol::RunId) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while client
                .status(id)
                .await
                .expect("read Run state while waiting for exit")
                .state
                .is_running()
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Run exits before the test deadline");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn subscribe_snapshot_join_delivers_interleaved_output_exactly_once() {
        let (server, hook, mut reached) = hooked_server(AttachmentHookPoint::AfterSubscribe);
        let run = server
            .client
            .start(long_running_spec())
            .await
            .expect("start subscribe/snapshot Run");
        let client = server.client.clone();
        let id = run.id;
        let attaching = tokio::spawn(async move { client.attach(id, 0).await });

        tokio::time::timeout(Duration::from_secs(5), reached.recv())
            .await
            .expect("attachment reaches subscribe/snapshot barrier")
            .expect("subscribe/snapshot barrier remains connected");
        let recorded = server.manager.get(run.id).expect("Run remains owned");
        recorded.record_output(b"between".to_vec());
        hook.release.notify_one();

        let (attachment, snapshot) = attaching
            .await
            .expect("attachment task completes")
            .expect("attach after subscribe/snapshot barrier");
        assert_eq!(replay_bytes(&snapshot.replay.chunks), b"between");
        assert_eq!(snapshot.replay.latest_output_bytes, b"between".len() as u64);

        recorded.record_output(b"after".to_vec());
        let event = tokio::time::timeout(Duration::from_secs(5), attachment.next_event())
            .await
            .expect("post-snapshot output arrives")
            .expect("read post-snapshot output")
            .expect("attachment stays live");
        let RunEvent::Output { chunk } = event else {
            panic!("expected post-snapshot output, got {event:?}");
        };
        assert_eq!(
            (chunk.start_byte, chunk.end_byte),
            (b"between".len() as u64, b"betweenafter".len() as u64)
        );
        assert_eq!(chunk.data, b"after");

        attachment.detach().await.expect("detach joined attachment");
        server.client.stop(run.id).await.expect("stop joined Run");
        wait_for_exit(&server.client, run.id).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn detach_output_race_keeps_new_bytes_replayable_and_releases_the_guard() {
        let (server, hook, mut reached) = hooked_server(AttachmentHookPoint::BeforeDetachAck);
        let run = server
            .client
            .start(long_running_spec())
            .await
            .expect("start detach/output Run");
        let (attachment, snapshot) = server
            .client
            .attach(run.id, 0)
            .await
            .expect("attach before detach/output race");
        let caller_cursor = snapshot.replay.latest_output_bytes;
        let detaching = tokio::spawn(async move { attachment.detach().await });

        tokio::time::timeout(Duration::from_secs(5), reached.recv())
            .await
            .expect("detach reaches acknowledgement barrier")
            .expect("detach barrier remains connected");
        server
            .manager
            .get(run.id)
            .expect("Run remains owned during detach")
            .record_output(b"detach-race".to_vec());
        hook.release.notify_one();
        detaching
            .await
            .expect("detach task completes")
            .expect("detach is acknowledged");
        assert_eq!(
            server
                .client
                .status(run.id)
                .await
                .expect("status after detach")
                .attachments,
            0
        );

        let (recovered, replay) = server
            .client
            .attach(run.id, caller_cursor)
            .await
            .expect("reattach after detach/output race");
        assert!(!replay.replay.truncated);
        assert_eq!(replay_bytes(&replay.replay.chunks), b"detach-race");
        recovered
            .detach()
            .await
            .expect("detach recovered attachment");
        server.client.stop(run.id).await.expect("stop detached Run");
        wait_for_exit(&server.client, run.id).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn final_output_recorded_after_wait_precedes_exit_and_remains_replayable() {
        let manager = Arc::new(RunManager::default());
        let server = InProcessServer::start(Arc::clone(&manager));
        let (wait_reached_tx, wait_reached_rx) = std::sync::mpsc::sync_channel(0);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
        let run = manager
            .start_with_wait_hook(
                RunSpec {
                    program: "/bin/sh".to_owned(),
                    args: vec!["-c".to_owned(), "exit 0".to_owned()],
                    cwd: None,
                    env: BTreeMap::new(),
                    size: TerminalSize::default(),
                    declared_inputs: Vec::new(),
                },
                move || {
                    wait_reached_tx.send(()).expect("publish wait barrier");
                    let _ = release_rx.recv();
                },
            )
            .expect("start final-output barrier Run");
        wait_reached_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("child wait reaches final-output barrier");

        let (attachment, snapshot) = server
            .client
            .attach(run.id, 0)
            .await
            .expect("attach while exit publication is paused");
        assert!(snapshot.run.state.is_running());
        assert!(snapshot.replay.chunks.is_empty());
        manager
            .get(run.id)
            .expect("final-output Run remains owned")
            .record_output(b"FINAL-AFTER-WAIT".to_vec());
        release_tx.send(()).expect("release exit publication");

        let output = tokio::time::timeout(Duration::from_secs(5), attachment.next_event())
            .await
            .expect("final output event arrives")
            .expect("read final output event")
            .expect("attachment remains live for final output");
        let RunEvent::Output { chunk } = output else {
            panic!("expected final output before exit, got {output:?}");
        };
        assert_eq!(chunk.data, b"FINAL-AFTER-WAIT");
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(5), attachment.next_event())
                .await
                .expect("exit event arrives")
                .expect("read exit event"),
            Some(RunEvent::Exited { .. })
        ));

        let (_, late) = server
            .client
            .attach(run.id, 0)
            .await
            .expect("reattach after final output and exit");
        assert_eq!(replay_bytes(&late.replay.chunks), b"FINAL-AFTER-WAIT");
        assert!(!late.run.state.is_running());
    }

    #[derive(Clone, Copy, Debug)]
    enum MutationOperation {
        Input(u8),
        Resize(u16),
        Stop,
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn seeded_multi_client_mutation_model_accepts_only_declared_outcomes() {
        let server = InProcessServer::start(Arc::new(RunManager::default()));
        let mut seed = environment_u64("CTXMUX_MODEL_SEED", 0x4354_584d_5558);
        let cases = usize::try_from(environment_u64("CTXMUX_MODEL_CASES", 8))
            .expect("model case count fits usize");
        assert!(cases > 0, "model case count must be positive");

        for case_index in 0..cases {
            let run = server
                .client
                .start(long_running_spec())
                .await
                .unwrap_or_else(|error| panic!("seed {seed} case {case_index}: start: {error}"));
            let mut operations = vec![
                MutationOperation::Input((next_random(&mut seed) & 0xff) as u8),
                MutationOperation::Resize(
                    u16::try_from(40 + next_random(&mut seed) % 161).expect("bounded width"),
                ),
                MutationOperation::Stop,
                MutationOperation::Stop,
            ];
            for index in (1..operations.len()).rev() {
                let selected = usize::try_from(next_random(&mut seed))
                    .expect("random value fits usize")
                    % (index + 1);
                operations.swap(index, selected);
            }

            let barrier = Arc::new(Barrier::new(operations.len() + 1));
            let mut tasks = Vec::new();
            for operation in operations {
                let client = server.client.clone();
                let start = Arc::clone(&barrier);
                let id = run.id;
                tasks.push(tokio::spawn(async move {
                    start.wait().await;
                    let result = match operation {
                        MutationOperation::Input(byte) => {
                            client.input(id, vec![byte]).await.map(|_| ())
                        }
                        MutationOperation::Resize(cols) => client
                            .resize(id, TerminalSize { cols, rows: 24 })
                            .await
                            .map(|_| ()),
                        MutationOperation::Stop => client.stop(id).await.map(|_| ()),
                    };
                    (operation, result)
                }));
            }
            barrier.wait().await;

            let mut accepted_stops = 0;
            let mut rejected_stops = 0;
            for task in tasks {
                let (operation, result) = task
                    .await
                    .unwrap_or_else(|error| panic!("seed {seed} case {case_index}: {error}"));
                match operation {
                    MutationOperation::Stop => match result {
                        Ok(()) => accepted_stops += 1,
                        Err(ClientError::ControlRejected { failure })
                            if matches!(
                                failure.error.code,
                                ErrorCode::InvalidRunState | ErrorCode::ControlBackpressure
                            ) =>
                        {
                            rejected_stops += 1;
                        }
                        result => panic!(
                            "seed {seed} case {case_index}: undeclared Stop result {result:?}"
                        ),
                    },
                    operation @ (MutationOperation::Input(_) | MutationOperation::Resize(_)) => {
                        match result {
                            Ok(()) => {}
                            Err(ClientError::ControlRejected { failure })
                                if matches!(
                                    failure.error.code,
                                    ErrorCode::InvalidRunState | ErrorCode::Io
                                ) => {}
                            result => panic!(
                                "seed {seed} case {case_index}: undeclared {operation:?} result {result:?}"
                            ),
                        }
                    }
                }
            }
            assert_eq!(
                (accepted_stops, rejected_stops),
                (1, 1),
                "seed {seed} case {case_index}: concurrent stop model drifted"
            );
            wait_for_exit(&server.client, run.id).await;
        }
    }

    fn environment_u64(name: &str, default: u64) -> u64 {
        std::env::var(name).map_or(default, |value| {
            value
                .parse::<u64>()
                .unwrap_or_else(|error| panic!("{name} must be an unsigned integer: {error}"))
        })
    }

    fn next_random(state: &mut u64) -> u64 {
        if *state == 0 {
            *state = 0x9e37_79b9_7f4a_7c15;
        }
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        *state
    }

    fn long_running_spec() -> RunSpec {
        RunSpec {
            program: "/bin/cat".to_owned(),
            args: Vec::new(),
            cwd: None,
            env: BTreeMap::new(),
            size: TerminalSize::default(),
            declared_inputs: Vec::new(),
        }
    }

    fn process_exists(pid: u32) -> bool {
        Command::new("/bin/sh")
            .args(["-c", "kill -0 \"$1\" 2>/dev/null", "ctxmux-fixture"])
            .arg(pid.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|status| status.success())
    }

    #[test]
    fn replay_marks_a_cursor_older_than_retained_output_as_truncated() {
        let mut output = OutputLog::default();
        for _ in 0..600 {
            output.push(vec![0; 8192]);
        }
        let replay = output.replay(0);
        assert!(replay.truncated);
        assert!(replay.first_available_byte > 0);
        assert_eq!(replay.latest_output_bytes, 600 * 8192);
    }

    #[test]
    fn replay_cursor_and_retention_boundaries_are_exact() {
        // OR-002: retained ranges and truncation are caller-cursor relative.
        let mut output = OutputLog::default();
        let first_size = OUTPUT_RETENTION_BYTES / 2;
        let second_size = OUTPUT_RETENTION_BYTES - first_size;
        output.push(vec![b'a'; first_size]);
        output.push(vec![b'b'; second_size]);

        let exact_limit = output.replay(0);
        assert!(!exact_limit.truncated);
        assert_eq!(exact_limit.first_available_byte, 0);
        assert_eq!(
            exact_limit.latest_output_bytes,
            OUTPUT_RETENTION_BYTES as u64
        );
        assert_eq!(
            exact_limit
                .chunks
                .iter()
                .map(|chunk| (chunk.start_byte, chunk.end_byte))
                .collect::<Vec<_>>(),
            vec![
                (0, first_size as u64),
                (first_size as u64, OUTPUT_RETENTION_BYTES as u64),
            ]
        );

        output.push(vec![b'c']);
        let evicted = output.replay(0);
        assert!(evicted.truncated);
        assert_eq!(evicted.first_available_byte, first_size as u64);
        assert_eq!(
            evicted.latest_output_bytes,
            OUTPUT_RETENTION_BYTES as u64 + 1
        );
        assert_eq!(
            evicted
                .chunks
                .iter()
                .map(|chunk| (chunk.start_byte, chunk.end_byte))
                .collect::<Vec<_>>(),
            vec![
                (first_size as u64, OUTPUT_RETENTION_BYTES as u64),
                (
                    OUTPUT_RETENTION_BYTES as u64,
                    OUTPUT_RETENTION_BYTES as u64 + 1
                ),
            ]
        );

        let immediately_before_oldest = output.replay(first_size as u64);
        assert!(!immediately_before_oldest.truncated);
        assert_eq!(immediately_before_oldest.chunks, evicted.chunks);
        assert_eq!(
            output
                .replay(OUTPUT_RETENTION_BYTES as u64)
                .chunks
                .iter()
                .map(|chunk| (chunk.start_byte, chunk.end_byte))
                .collect::<Vec<_>>(),
            vec![(
                OUTPUT_RETENTION_BYTES as u64,
                OUTPUT_RETENTION_BYTES as u64 + 1
            )]
        );
        assert!(
            output
                .replay(OUTPUT_RETENTION_BYTES as u64 + 1)
                .chunks
                .is_empty()
        );
        assert!(output.replay(u64::MAX).chunks.is_empty());
        assert!(!output.replay(u64::MAX).truncated);
    }

    #[test]
    fn replay_keeps_a_tmux_source_gap_visible_to_late_attachments() {
        let mut output = OutputLog::with_initial_truncation();
        assert!(output.replay(0).truncated);
        assert_eq!(output.mark_source_gap(), 0);

        output.push(b"before-gap".to_vec());
        assert_eq!(output.mark_source_gap(), b"before-gap".len() as u64);
        let at_gap = output.replay(b"before-gap".len() as u64);
        assert!(at_gap.truncated);
        assert!(at_gap.chunks.is_empty());

        output.push(b"after-gap".to_vec());
        let recovery = output.replay(b"before-gap".len() as u64);
        assert!(recovery.truncated);
        assert_eq!(recovery.first_available_byte, 0);
        assert_eq!(
            recovery.latest_output_bytes,
            b"before-gapafter-gap".len() as u64
        );
        assert_eq!(
            (recovery.chunks[0].start_byte, recovery.chunks[0].end_byte),
            (
                b"before-gap".len() as u64,
                b"before-gapafter-gap".len() as u64
            )
        );
        assert!(!output.replay(b"before-gapafter-gap".len() as u64).truncated);
    }

    #[test]
    fn one_oversized_output_chunk_is_retained_as_an_honest_replay_unit() {
        let mut output = OutputLog::default();
        let oversized = vec![0xa5; OUTPUT_RETENTION_BYTES + 1];
        output.push(oversized.clone());

        let replay = output.replay(0);
        assert!(!replay.truncated);
        assert_eq!(replay.first_available_byte, 0);
        assert_eq!(replay.latest_output_bytes, oversized.len() as u64);
        assert_eq!(replay.chunks[0].data, oversized);

        output.push(vec![0x5a]);
        let after_eviction = output.replay(0);
        assert!(after_eviction.truncated);
        assert_eq!(after_eviction.first_available_byte, oversized.len() as u64);
        assert_eq!(
            after_eviction.latest_output_bytes,
            oversized.len() as u64 + 1
        );
        assert_eq!(after_eviction.chunks[0].data, vec![0x5a]);
    }

    #[tokio::test]
    async fn lag_recovery_replays_from_the_callers_cursor_without_loss_or_duplicates() {
        // LC-001 / OR-002: a live-ring lag does not replace the caller's
        // durable replay cursor with the daemon head.
        let (events, mut receiver) = broadcast::channel(2);
        let mut output = OutputLog::default();
        for byte in b"abcd" {
            let chunk = output.push(vec![*byte]);
            events.send(chunk).expect("keep receiver live");
        }

        assert!(matches!(
            receiver.recv().await,
            Err(broadcast::error::RecvError::Lagged(2))
        ));
        let replay = output.replay(0);
        assert!(!replay.truncated);
        assert_eq!(
            replay
                .chunks
                .iter()
                .map(|chunk| (chunk.start_byte, chunk.end_byte))
                .collect::<Vec<_>>(),
            vec![(0, 1), (1, 2), (2, 3), (3, 4)]
        );
        assert_eq!(
            replay
                .chunks
                .iter()
                .flat_map(|chunk| chunk.data.iter().copied())
                .collect::<Vec<_>>(),
            b"abcd"
        );
    }

    #[test]
    fn live_event_ring_exists_only_while_an_attachment_owns_it() {
        let id = RunId::new();
        let native_runs = NativeRuntimeOwner::default();
        let control = NativeControlOwner::new_for_wait_test(id, native_runs.owner_wake());
        let run =
            Run::new_native_for_owner_test(id, control, native_runs, NativeWaitFailure::default());
        assert!(mutex_lock(&run.events.sender).is_none());

        let (first, mut first_events) = run.subscribe();
        let (second, _second_events) = run.subscribe();
        assert_eq!(run.attachments.load(Ordering::Acquire), 2);
        assert!(mutex_lock(&run.events.sender).is_some());

        run.publish_event(RunEvent::Gap {
            latest_output_bytes: 7,
        });
        assert!(matches!(
            first_events.try_recv(),
            Ok(RunEvent::Gap {
                latest_output_bytes: 7
            })
        ));

        drop(first);
        assert!(mutex_lock(&run.events.sender).is_some());
        drop(second);
        assert_eq!(run.attachments.load(Ordering::Acquire), 0);
        assert!(mutex_lock(&run.events.sender).is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(
        clippy::too_many_lines,
        reason = "one continuous public Gap and caller-cursor recovery proof is easier to audit"
    )]
    async fn public_gap_reattaches_from_the_callers_cursor_without_loss_or_duplicates() {
        // LC-001 / OR-002: force the real attachment receiver to lag, observe
        // Gap through the public socket client, then recover from the cursor
        // the caller actually persisted rather than the daemon's newer head.
        let directory = tempfile::tempdir().expect("create Gap fixture directory");
        let socket = directory.path().join("ctxmux.sock");
        let listener = tokio::net::UnixListener::bind(&socket).expect("bind Gap fixture socket");
        let (reached_tx, mut reached_rx) = mpsc::unbounded_channel();
        let hook = Arc::new(AttachmentTestHook {
            point: AttachmentHookPoint::AfterSnapshot,
            armed: AtomicBool::new(true),
            reached: reached_tx,
            release: Notify::new(),
        });
        let manager = Arc::new(RunManager {
            live_event_capacity: 2,
            attachment_hook: Some(Arc::clone(&hook)),
            ..RunManager::default()
        });
        let server = tokio::spawn(serve_with_manager(
            socket.clone(),
            listener,
            Arc::clone(&manager),
            None,
            None,
            None,
        ));
        let client = Client::new(socket);

        let ready = directory.path().join("child-ready");
        let mut env = BTreeMap::new();
        env.insert(
            "CTXMUX_GAP_READY".to_owned(),
            ready.to_string_lossy().into_owned(),
        );
        let run = client
            .start(RunSpec {
                program: "/bin/sh".to_owned(),
                args: vec![
                    "-c".to_owned(),
                    concat!(
                        "stty -echo -icanon min 1 time 0; ",
                        ": > \"$CTXMUX_GAP_READY\"; ",
                        "dd bs=1 count=1 of=/dev/null 2>/dev/null; ",
                        "dd if=/dev/zero bs=8192 count=4 2>/dev/null; ",
                        "sleep 30"
                    )
                    .to_owned(),
                ],
                cwd: None,
                env,
                size: TerminalSize::default(),
                declared_inputs: Vec::new(),
            })
            .await
            .expect("start controlled Gap Run");
        tokio::time::timeout(Duration::from_secs(5), async {
            while !ready.exists() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("child reaches raw-input barrier");

        let (lagged_attachment, initial) = client
            .attach(run.id, 0)
            .await
            .expect("open public attachment before output");
        assert!(initial.replay.chunks.is_empty());
        let caller_cursor = initial.replay.latest_output_bytes;
        tokio::time::timeout(Duration::from_secs(5), reached_rx.recv())
            .await
            .expect("attachment reaches post-snapshot barrier")
            .expect("attachment barrier remains connected");

        client
            .input(run.id, b"x".to_vec())
            .await
            .expect("release controlled child output");
        let recorded_run = manager.get(run.id).expect("Gap Run remains manager-owned");
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let recorded_bytes = mutex_lock(&recorded_run.output)
                    .replay(caller_cursor)
                    .chunks
                    .iter()
                    .map(|chunk| chunk.data.len())
                    .sum::<usize>();
                if recorded_bytes == 4 * 8192 {
                    break;
                }
                assert!(
                    recorded_bytes < 4 * 8192,
                    "controlled child emitted unexpected extra bytes"
                );
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("daemon records all controlled output");
        hook.release.notify_one();

        let gap_head =
            match tokio::time::timeout(Duration::from_secs(5), lagged_attachment.next_event())
                .await
                .expect("lagged attachment reports Gap")
                .expect("read lagged attachment event")
                .expect("lagged attachment remains connected")
            {
                RunEvent::Gap {
                    latest_output_bytes,
                } => latest_output_bytes,
                event => panic!("expected public Gap event, got {event:?}"),
            };
        drop(lagged_attachment);

        let (recovered_attachment, recovered) = client
            .attach(run.id, caller_cursor)
            .await
            .expect("reattach from caller-owned cursor");
        assert!(!recovered.replay.truncated);
        assert_eq!(recovered.replay.latest_output_bytes, gap_head);
        let mut expected_byte = caller_cursor;
        for chunk in &recovered.replay.chunks {
            assert_eq!(chunk.start_byte, expected_byte);
            assert_eq!(chunk.end_byte - chunk.start_byte, chunk.data.len() as u64);
            expected_byte = chunk.end_byte;
        }
        assert_eq!(expected_byte, gap_head);
        let recovered_bytes = replay_bytes(&recovered.replay.chunks);
        assert_eq!(recovered_bytes.len(), 4 * 8192);
        assert!(recovered_bytes.iter().all(|byte| *byte == 0));

        recovered_attachment
            .detach()
            .await
            .expect("detach recovered attachment");
        client.stop(run.id).await.expect("stop controlled Gap Run");
        tokio::time::timeout(Duration::from_secs(5), async {
            while client
                .status(run.id)
                .await
                .expect("read controlled Gap Run state")
                .state
                .is_running()
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("controlled Gap Run exits");
        server.abort();
        let _ = server.await;
    }

    #[test]
    fn socket_path_preparation_refuses_protected_and_live_targets() {
        // LP-01: setup never replaces a protected path or steals a live one.
        let directory = tempfile::tempdir().expect("create socket fixture directory");

        let ordinary = directory.path().join("ordinary");
        fs::write(&ordinary, b"keep me").expect("write protected fixture file");
        assert!(matches!(
            prepare_socket_path(&ordinary),
            Err(ServerError::InvalidSocketTarget(path)) if path == ordinary
        ));
        assert_eq!(
            fs::read(&ordinary).expect("protected file remains readable"),
            b"keep me"
        );

        let link = directory.path().join("link");
        symlink(&ordinary, &link).expect("create protected fixture symlink");
        assert!(matches!(
            prepare_socket_path(&link),
            Err(ServerError::InvalidSocketTarget(path)) if path == link
        ));
        assert!(fs::symlink_metadata(&link).is_ok());

        let live = directory.path().join("live.sock");
        let listener = UnixListener::bind(&live).expect("bind live socket fixture");
        assert!(matches!(
            prepare_socket_path(&live),
            Err(ServerError::AlreadyRunning(path)) if path == live
        ));
        assert!(fs::symlink_metadata(&live).is_ok());
        drop(listener);
    }

    #[test]
    fn socket_path_preparation_removes_only_an_inactive_socket() {
        // LP-01: stale recovery is limited to an actual inactive socket.
        let directory = tempfile::tempdir().expect("create socket fixture directory");
        let stale = directory.path().join("stale.sock");
        let mut listener = Some(UnixListener::bind(&stale).expect("bind stale socket fixture"));

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let still_connectable = UnixStream::connect(&stale).is_ok();
            drop(listener.take());
            if !still_connectable {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "closed listener remained connectable"
            );
            std::thread::yield_now();
        }

        prepare_socket_path(&stale).expect("remove inactive socket");
        assert!(!stale.exists());
    }

    #[test]
    fn stale_socket_replacement_race_preserves_the_unrelated_live_target() {
        // LP-01: stop after the inactive probe, replace the checked inode with
        // an unrelated listener, and require identity revalidation to fail
        // before unlink or bind can affect that listener.
        let directory = tempfile::tempdir().expect("create socket race fixture directory");
        let target = directory.path().join("ctxmux.sock");
        let displaced = directory.path().join("checked-stale.sock");
        let mut stale = Some(UnixListener::bind(&target).expect("bind checked stale socket"));
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let still_connectable = UnixStream::connect(&target).is_ok();
            drop(stale.take());
            if !still_connectable {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "closed stale socket remained connectable"
            );
            std::thread::yield_now();
        }

        let mut replacement = None;
        let error = prepare_socket_path_with_hook(&target, || {
            fs::rename(&target, &displaced).expect("move checked stale socket aside");
            replacement =
                Some(UnixListener::bind(&target).expect("bind unrelated replacement listener"));
        })
        .expect_err("changed stale target fails closed");
        assert!(matches!(
            error,
            ServerError::SocketTargetChanged(path) if path == target
        ));
        assert!(
            UnixStream::connect(&target).is_ok(),
            "stale cleanup removed the unrelated live listener"
        );
        drop(replacement);
        assert!(fs::symlink_metadata(&target).is_ok());
        assert!(fs::symlink_metadata(&displaced).is_ok());
    }

    #[tokio::test]
    async fn shutdown_preserves_a_replacement_listener_at_the_published_path() {
        // LP-01: shutdown may remove only the identity this server bound, even
        // when another live listener replaces its pathname before task drop.
        let directory = tempfile::tempdir().expect("create shutdown socket fixture directory");
        let target = directory.path().join("ctxmux.sock");
        let displaced = directory.path().join("old-daemon.sock");
        let listener =
            tokio::net::UnixListener::bind(&target).expect("bind old daemon socket fixture");
        let client = Client::new(target.clone());
        let server = tokio::spawn(serve_with_manager(
            target.clone(),
            listener,
            Arc::new(RunManager::default()),
            None,
            None,
            None,
        ));

        client
            .list()
            .await
            .expect("old daemon accepts a public request before replacement");
        fs::rename(&target, &displaced).expect("move old daemon socket pathname aside");
        let replacement = UnixListener::bind(&target).expect("bind unrelated replacement listener");
        assert!(
            UnixStream::connect(&target).is_ok(),
            "replacement listener is reachable before old daemon shutdown"
        );

        server.abort();
        let _ = server.await;

        assert!(
            fs::symlink_metadata(&target).is_ok(),
            "old daemon shutdown removed the replacement pathname"
        );
        assert!(
            UnixStream::connect(&target).is_ok(),
            "replacement listener is reachable after old daemon shutdown"
        );
        assert!(
            fs::symlink_metadata(&displaced).is_ok(),
            "old daemon socket identity remains at its displaced pathname"
        );
        drop(replacement);
    }

    #[tokio::test]
    async fn published_socket_has_owner_only_permissions() {
        // LP-01: the supported Unix baseline publishes owner-only mode.
        let directory = tempfile::tempdir().expect("create socket fixture directory");
        let socket = directory.path().join("ctxmux.sock");
        let server = tokio::spawn(super::serve(socket.clone()));

        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if let Ok(metadata) = fs::symlink_metadata(&socket) {
                    assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("daemon publishes socket");

        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn adopt_listener_reuses_the_socket_inode_without_rebinding() {
        use std::os::fd::{AsRawFd, IntoRawFd};
        use std::os::unix::fs::MetadataExt;

        // AL-01: adopting an inherited listener fd must reuse the live socket
        // inode, not rebind/replace it — a rebind would drop connected clients
        // and trip the daemon's own AlreadyRunning guard.
        let directory = tempfile::tempdir().expect("create adopt-listener fixture directory");
        let socket = directory.path().join("ctxmux.sock");
        let bound = UnixListener::bind(&socket).expect("bind the pre-exec listener");
        let before = fs::symlink_metadata(&socket).expect("stat the bound socket");

        // Dup first so the inherited-process claim inside `adopt_listener` owns exactly
        // one owner; keep `bound` alive so the inode is never unlinked.
        let dup =
            ctxmux_inherited_fd::duplicate_cloexec(bound.as_raw_fd()).expect("dup the listener fd");
        let raw = dup.into_raw_fd();
        let adopted = super::adopt_listener(raw).expect("adopt the inherited listener");

        let after = fs::symlink_metadata(&socket).expect("stat the socket after adoption");
        assert_eq!(before.ino(), after.ino(), "adoption must not replace inode");
        assert_eq!(
            before.dev(),
            after.dev(),
            "adoption must not replace device"
        );

        // Prove it is the live socket, not a dead fd: a client connect is
        // accepted on the adopted listener.
        let accept = tokio::spawn(async move { adopted.accept().await });
        let _client = tokio::net::UnixStream::connect(&socket)
            .await
            .expect("connect to the adopted listener");
        let accepted = tokio::time::timeout(std::time::Duration::from_secs(5), accept)
            .await
            .expect("adopted listener accepts before timeout")
            .expect("accept task joins");
        accepted.expect("adopted listener yields the connection");
    }
}
