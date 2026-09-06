//! Long-lived native Run owner and local protocol server.

#[cfg(not(unix))]
compile_error!("the first ctxmux native transport currently requires Unix sockets");

use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
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
mod diagnostics;
mod fd_budget;
mod foreground_observation;
mod handoff;
mod native_control;
mod native_output;
mod native_runtime;
mod native_service;
mod native_session;
mod native_spawn_env;
mod persistence;
mod qualification_stats;
pub mod resources;
mod retention;
pub use resources::ResourceLimits;
mod run_spec;
mod terminal_checkpoint;
mod tmux;

pub use persistence::PersistenceError;

/// How this binary declares its handoff schema in `--version` output.
///
/// Re-exported for `ctxmuxd --version`, which is the one place the string has to
/// leave the crate: an upgrade target that cannot state what it accepts cannot
/// be verified before the exec that would kill every live Run.
#[must_use]
pub fn handoff_version_token() -> String {
    handoff::version_token()
}

use ctxmux_protocol::{
    AppliedInputRange, AttachedSnapshot, AttachmentView, ClientFrame, CommandDisposition,
    ControlFailure, CreateOperationKey, DaemonInstanceId, ErrorCode, ForkFidelity, ForkPlan,
    InterruptionReason, LIST_MAX_PAGE_RUNS, MAX_FRAME_BYTES, OutputChunk, OutputReplay,
    PROTOCOL_VERSION, ProtocolError, RUNTIME_CAPABILITY_NATIVE_EXECUTE_MATERIALIZED_LEVEL_B,
    RUNTIME_CAPABILITY_NATIVE_FORK_LEVEL_A, RUNTIME_CAPABILITY_NATIVE_RECOVERABLE_INPUT,
    RUNTIME_CAPABILITY_NATIVE_RECOVERABLE_STOP, RUNTIME_CAPABILITY_NATIVE_START,
    RUNTIME_CAPABILITY_PERSISTENT_STATE, RUNTIME_CAPABILITY_PLANNED_EXEC_UPGRADE_CONTINUITY,
    RUNTIME_CAPABILITY_TMUX_DISCOVER, RUNTIME_CAPABILITY_TMUX_IMPORT, RecoverableInput,
    RecoverableStop, Request, Response, RunBackend, RunBackendKind, RunCapabilities, RunEvent,
    RunId, RunInfo, RunLineage, RunSpec, RunState, RunSummary, RuntimeBuildId, RuntimeId,
    RuntimeIdPersistence, RuntimeIdentity, ServerFrame, TerminalCheckpointUnavailableReason,
    TerminalContinuation, TerminalResize, TerminalSize, TmuxRunEvent, decode_frame, encode_frame,
};
use futures_util::{SinkExt, StreamExt};
use portable_pty::{Child, ChildKiller, CommandBuilder, ExitStatus, native_pty_system};
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
    HandoffStopOperation, PendingPublication, PersistentCollectionCandidate,
    PublicationReservation, RecoverableStopAdmission, RecoverableStopFlight,
    RecoverableStopSettlement, RunRegistry, TerminalOrdinal, TerminalPublicationOwner,
    TmuxCleanupReservation, UnpublishedCleanupOwner, UnpublishedCleanupReservation,
};
use crate::native_control::{
    ControlResult, DetachedNativeDescriptors, HandoffInputState, InputDrainGate,
    NativeControlOwner, PendingInput, PendingSignal, PendingStop, to_pty_size,
};
use crate::native_runtime::{
    LiveDescriptors, NativeRunOwner as NativeRuntimeOwner, NativeRunRegistration,
};
use crate::native_session::{AdoptedChild, NativeSession};
use crate::persistence::{
    CommittedStart, HandoffHint, Persistence, PersistentCandidate, PersistentRun,
    PersistentStartCompletion, PersistentStartFailure, RecoveredRun, RemovalDisposition,
    StagedPersistentStart, StartDisposition,
};
use crate::qualification_stats::{Gauge as QualificationGauge, QualificationStats};
use crate::retention::{RetentionBudget, RetentionVictim};
use crate::terminal_checkpoint::{StoredCheckpoint, TerminalModel, derive_terminal};
use crate::tmux::{
    BoundedLineRead, ControlItem, ControlParser, SocketIdentity as TmuxSocketIdentity,
};

#[cfg(test)]
const OUTPUT_RETENTION_BYTES: usize = 4 * 1024 * 1024;
const LIVE_EVENT_CAPACITY: usize = 256;
const CHILD_CONTROL_POLL: Duration = Duration::from_millis(20);
const STOP_ACK_TIMEOUT: Duration = Duration::from_secs(3);
const STOP_GRACEFUL_TIMEOUT: Duration = Duration::from_millis(500);
const STOP_FORCED_TIMEOUT: Duration = Duration::from_secs(1);
/// How long a public Stop or `remove` waits for a reaped Run's terminal state to
/// become visible before treating publication as unconfirmed.
///
/// Publication happens on a worker that is already running by the time a Stop
/// receipt exists: measured at 1-6 ms on a persistent daemon. But publication
/// can also sit behind a durable finalize, and under a loud fleet that finalize
/// is queued behind the appends it must be ordered after: measured 0.3-3.6 s.
///
/// Expiring here is therefore **not** harmless. `remove` reads the very `state`
/// publication writes (`creation.rs`, `validate_removable_entry`), so a wait
/// that gives up refuses the caller with `InvalidRunState`. The timer converts
/// *slow* into *wrong*.
///
/// So this is a backstop against a hung publication, not a latency budget: it is
/// sized past the measured worst case rather than under it. On the farm host,
/// 100 ms gave 0/120 successful removes at the plateau and this bound gives
/// 120/120, while the quiet shapes — where there is nothing to wait for — are
/// unchanged within the noise floor. Waiting is bounded in turn by
/// `PERSISTENCE_QUEUE_CAPACITY`, which is what stops the wait escalating across
/// consecutive stops; see `docs/architecture/r22-the-stop-that-stops-lying.md`.
///
/// The wait is per Run and does not consume cleanup admission. This keeps a
/// slow durable finalizer from delaying unrelated controls while making a
/// successful public Stop safe to follow with List or Remove.
pub(crate) const TERMINAL_VISIBILITY_GRACE: Duration = Duration::from_secs(10);
const UNPUBLISHED_REAP_INLINE_TIMEOUT: Duration = Duration::from_millis(25);
const TMUX_OUTPUT_DRAIN_TIMEOUT: Duration = Duration::from_secs(1);
const TMUX_FAILED_IMPORT_CLEANUP_TIMEOUT: Duration = Duration::from_secs(2);
const TMUX_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(4);
const TMUX_IMPORT_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(3);
const TMUX_IMPORT_PREPARE_TIMEOUT: Duration = Duration::from_secs(5);
const TMUX_IMPORT_TOTAL_TIMEOUT: Duration = Duration::from_secs(7);
const TMUX_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(8);
const UPGRADE_QUIESCE_TIMEOUT: Duration = Duration::from_secs(8);
/// Pause after a transient `accept(2)` failure. Under `EMFILE`/`ENFILE` the
/// listening socket stays readable, so an immediate retry would spin the accept
/// loop at 100% CPU until a descriptor frees. A short fixed sleep drains that
/// window without any backpressure state to reason about.
const ACCEPT_BACKOFF: Duration = Duration::from_millis(50);

fn runtime_build_id() -> RuntimeBuildId {
    RuntimeBuildId::new(concat!("ctxmuxd/", env!("CARGO_PKG_VERSION")))
        .expect("ctxmuxd package version forms a valid Runtime build identity")
}

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
    serve_with_inherited_descriptors(socket_path, None, None).await
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
        ResourceLimits::default(),
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
    serve_with_state_dir_and_inherited_descriptors(socket_path, state_dir, None, None, None).await
}

#[doc(hidden)]
pub async fn serve_with_state_dir_and_inherited_descriptors(
    socket_path: impl Into<PathBuf>,
    state_dir: impl Into<PathBuf>,
    qualification_stats_fd: Option<OwnedFd>,
    readiness_fd: Option<OwnedFd>,
    handoff_fd: Option<OwnedFd>,
) -> Result<(), ServerError> {
    serve_configured(
        socket_path.into(),
        Some(state_dir.into()),
        qualification_stats_fd,
        readiness_fd,
        handoff_fd,
        ResourceLimits::default(),
    )
    .await
}

/// Serve with an explicit resource policy. Existing listener reuse is handled
/// by activation clients; this policy applies when this daemon starts.
///
/// # Errors
/// Returns an error for invalid policy, unsafe state, or runtime owner failure.
pub async fn serve_configured(
    socket_path: PathBuf,
    state_dir: Option<PathBuf>,
    qualification_stats_fd: Option<OwnedFd>,
    readiness_fd: Option<OwnedFd>,
    handoff_fd: Option<OwnedFd>,
    resources: ResourceLimits,
) -> Result<(), ServerError> {
    resources
        .validate()
        .map_err(|error| ServerError::Adopt(format!("invalid resource policy: {error}")))?;
    if state_dir.is_none() {
        initialize_diagnostics(resources)?;
    }
    let Some(state_dir) = state_dir else {
        return serve_with_persistence(
            socket_path,
            None,
            qualification_stats_fd,
            readiness_fd,
            resources,
        )
        .await;
    };
    let handoff = match handoff_fd {
        Some(fd) => Some(
            crate::handoff::read_manifest_with_limit(fd, resources.handoff_bytes)
                .map_err(|source| ServerError::io("<handoff-fd>", source))?,
        ),
        None => None,
    };
    initialize_diagnostics(
        handoff
            .as_ref()
            .map_or(resources, |manifest| manifest.resources),
    )?;
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
        let (persistence, recovered) =
            Persistence::open_with_resources(state_dir.clone(), manifest.resources, Some(hint))?;
        let stats = QualificationStats::from_optional_inherited_fd(
            qualification_stats_fd,
            persistence.daemon_instance().to_string(),
        )
        .map_err(|source| ServerError::io("qualification stats fd", source))?;
        Arc::new(
            RunManager::persistent_with_handoff_and_stats(
                persistence,
                recovered,
                stats,
                adopt,
                manifest.stop_operations.clone(),
                manifest.closed_inputs.clone(),
            )
            .map_err(|error| ServerError::Adopt(error.message))?,
        )
    } else {
        let (persistence, recovered) =
            Persistence::open_with_resources(state_dir.clone(), resources, None)?;
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
    serve_with_persistence_manager(socket_path, manager, readiness_fd, handoff, Some(state_dir))
        .await
}

/// Diagnostic sink failure is observable independently of Run availability.
/// Its one-shot owner does not change an inherited descriptor's status flags.
fn initialize_diagnostics(resources: ResourceLimits) -> Result<(), ServerError> {
    let result = diagnostics::initialize(diagnostics::DiagnosticLimits {
        queue_bytes: usize::try_from(resources.diagnostic_queue_bytes)
            .expect("validated diagnostic policy fits the host"),
        record_bytes: resources.diagnostic_record_bytes,
    });
    if let Err(error) = result {
        if error.kind() == io::ErrorKind::AlreadyExists {
            return Err(ServerError::Adopt(error.to_string()));
        }
        let _ = diagnostics::record(format_args!(
            "ctxmuxd diagnostic initialization failed: {error}"
        ));
    }
    Ok(())
}

async fn serve_with_persistence(
    socket_path: PathBuf,
    persistence: Option<(Persistence, Vec<RecoveredRun>)>,
    qualification_stats_fd: Option<OwnedFd>,
    readiness_fd: Option<OwnedFd>,
    resources: ResourceLimits,
) -> Result<(), ServerError> {
    initialize_diagnostics(resources)?;
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
        Arc::new(RunManager::with_instance_stats_and_resources(
            daemon_instance,
            stats,
            resources,
        ))
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

/// Whether an `accept(2)` failure means the listening socket itself is gone and
/// the daemon must exit, as opposed to a transient resource shortage it should
/// ride out.
///
/// Descriptor exhaustion (`EMFILE`/`ENFILE`), a kernel buffer shortage
/// (`ENOBUFS`/`ENOMEM`, which BSD/macOS raise under pressure), an aborted
/// client handshake (`ECONNABORTED`), and a signal interruption (`EINTR`) are
/// all expected and recoverable: the listener is intact and later accepts will
/// succeed once the pressure clears. Anything else — a dead or invalid listener
/// (`EBADF`, `EINVAL`, `ENOTSOCK`), or an errno the OS did not attach — leaves
/// no connection to serve, so the daemon fails stop.
fn accept_error_is_fatal(source: &io::Error) -> bool {
    use rustix::io::Errno;
    !matches!(
        Errno::from_io_error(source),
        Some(
            Errno::MFILE
                | Errno::NFILE
                | Errno::NOBUFS
                | Errno::NOMEM
                | Errno::CONNABORTED
                | Errno::INTR,
        )
    )
}

/// Read and raise `RLIMIT_NOFILE` toward the computed fd budget, then clamp the
/// effective live-Run ceiling to what descriptors actually allow.
///
/// Runs once per incarnation on the single startup funnel, before the socket is
/// published — so no reservation or Collecting fence is in flight when the
/// record capacity is lowered. On the exec-in-place re-exec path the incoming
/// image inherits the prior raise, so the raise is idempotent (a no-op) and the
/// clamp recomputes the same ceiling. When the OS will not fund the full
/// budget, the clamp is logged with both the funded ceiling and the daemon's
/// concurrency target so the operator sees why fewer Runs are admitted than the
/// target provisions for, instead of the daemon reaching EMFILE by surprise
/// mid-spawn.
fn apply_startup_fd_budget(manager: &RunManager) {
    let outcome = fd_budget::apply_fd_budget(manager.resources);
    let describe =
        |limit: Option<u64>| limit.map_or_else(|| "unlimited".to_owned(), |n| n.to_string());
    if outcome.clamped {
        let _ = crate::diagnostics::record(format_args!(
            "ctxmuxd: RLIMIT_NOFILE soft {} hard {} funds only {} live Run(s); \
             effective Run ceiling clamped below the configured {} \
             (fd budget {} unavailable); excess Runs are refused with run_capacity",
            describe(outcome.effective_soft),
            describe(outcome.hard),
            outcome.run_ceiling,
            outcome.provisioned_runs,
            outcome.provisioned_fds,
        ));
    } else if outcome.raised {
        let _ = crate::diagnostics::record(format_args!(
            "ctxmuxd: raised RLIMIT_NOFILE soft {} -> {} (hard {}); funds {} live Run(s)",
            describe(outcome.original_soft),
            describe(outcome.effective_soft),
            describe(outcome.hard),
            outcome.run_ceiling,
        ));
    }
    manager.registry.clamp_live_capacity(outcome.run_ceiling);
}

/// Register the process-wide SIGCHLD relay that drives native Run exit
/// detection, mark the owner signal-driven, and fire the exec-window catch-up.
///
/// SIGCHLD readiness replaces the owner's old 50 Hz `waitid` sweep as the exit
/// trigger: one child exit wakes the native owner thread, which peeks its whole
/// watched set once (a coalesced burst is one wake, which is correct — see
/// `native_runtime::owner_main`). This costs ZERO extra descriptors per Run:
/// tokio's signal driver funnels every registration through one process-wide
/// self-pipe singleton, and the daemon already created that pipe with the SIGHUP
/// registration in `serve_with_manager`. tokio's handler only records an event
/// id and writes one self-pipe byte — it neither reaps nor blocks the signal —
/// so it coexists with the owner's non-reaping peek-then-reap discipline and with
/// the tmux backend's own SIGCHLD-based pane reaping. (tokio's own child reaper is
/// not compiled in: the workspace enables tokio's `signal` feature, not
/// `process`.)
///
/// ORDERING INVARIANT — do not reorder without re-reading this: SIGCHLD must be
/// registered alongside the existing SIGHUP registration at startup, before any
/// Run is published, and must NOT become the first signal the process ever
/// registers. The reliability harness samples its descriptor baseline after
/// startup; if the driver's self-pipe were created inside that window (SIGCHLD
/// moved earlier, or SIGHUP moved later), the harness would attribute two extra
/// descriptors it did not expect to the fleet. Keeping both registrations here
/// keeps the pipe out of the measured window.
///
/// The catch-up wake after `mark_signal_driven` forces the owner to peek its
/// whole watched set once before it is allowed to block, closing the
/// exec-in-place adopt window: a child re-adopted by this image via
/// `Run::readopt` (same pid across execve, still our child) can exit DURING the
/// exec — after the old image stopped watching, before this image registered the
/// handler — and SIGCHLD's default-ignore disposition drops that transition, so
/// it is queued for no one. Without the catch-up that adopted Run would sit
/// undetected until some unrelated wake. Cold-recovered Runs (`Run::recover`) are
/// children of the dead previous process, reparented to init, so they are not
/// ours to reap and need no coverage; the peek is non-reaping, so a spurious
/// catch-up is harmless.
fn arm_native_exit_relay(manager: &RunManager) -> Result<tokio::signal::unix::Signal, ServerError> {
    let sigchld = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::child())
        .map_err(|source| ServerError::io("<sigchld>", source))?;
    become_child_subreaper();
    manager.native_runs.mark_signal_driven();
    manager.native_runs.owner_wake().wake();
    Ok(sigchld)
}

/// Claim orphaned descendants so a Run's session stays inside this daemon's
/// process tree.
///
/// `NativeSession::members` proves a stopped Run's session empty by descending
/// the leader's own subtree rather than walking every process on the host (see
/// `native_session::session_candidates`). That is only equivalent to the host
/// walk while every session member is reachable from this daemon. A descendant
/// whose parent exits first would otherwise reparent to init -- leaving the
/// subtree while *staying in the session*, which is precisely the case the walk
/// still saw and the sweep would not. As a subreaper we inherit it instead, and
/// the sweep's daemon-children level finds it.
///
/// Arming the bit also makes us responsible for reaping what we inherit --
/// nothing else in this process will `wait` for an orphan, and an unreaped one
/// still answers `getsid`. That duty is discharged by
/// `native_session::reap_inherited_orphans`, called from the census that
/// depends on it rather than from here.
///
/// Best-effort on purpose. `PR_SET_CHILD_SUBREAPER` is Linux 3.4+ and takes no
/// permission, so a failure here means a kernel that also lacks the
/// `/proc/<pid>/task/<tid>/children` file the sweep reads; on any other
/// platform the sweep is not compiled in and this is a no-op. Failing startup
/// over it would trade a rare, narrower emptiness proof for no daemon at all.
fn become_child_subreaper() {
    #[cfg(not(target_os = "macos"))]
    {
        // `set_child_subreaper` takes the pid to install, and `None` means 0 --
        // which CLEARS the attribute. Naming ourselves is what sets it.
        let me = rustix::process::getpid();
        if let Err(error) = rustix::process::set_child_subreaper(Some(me)) {
            let _ = crate::diagnostics::record(format_args!(
                "ctxmuxd: failed to become a child subreaper ({error}); an orphaned \
                 Run descendant may escape the session emptiness proof"
            ));
        }
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "one select loop makes cancellation, upgrade and fail-stop ordering visible"
)]
async fn serve_with_manager(
    socket_path: PathBuf,
    listener: UnixListener,
    manager: Arc<RunManager>,
    readiness_fd: Option<OwnedFd>,
    handoff: Option<crate::handoff::HandoffManifest>,
    state_dir: Option<PathBuf>,
) -> Result<(), ServerError> {
    let listener = Arc::new(listener);
    let _socket_guard = SocketGuard::new(socket_path.clone())?;
    apply_startup_fd_budget(&manager);
    if let Some(handoff) = &handoff {
        // A12 wires manifest.state_lock_fd into the incoming-image startup path.
        let _ = crate::diagnostics::record(format_args!(
            "ctxmuxd: adopted inherited listener for handoff (epoch {}, {} run(s))",
            handoff.epoch,
            handoff.runs.len()
        ));
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

    let mut sigchld = arm_native_exit_relay(&manager)?;

    loop {
        tokio::select! {
            result = listener.accept() => {
                let stream = match result {
                    Ok((stream, _)) => stream,
                    Err(source) if accept_error_is_fatal(&source) => {
                        return Err(ServerError::io(&socket_path, source));
                    }
                    Err(source) => {
                        // The listener is intact; a descriptor or buffer shortage
                        // rejected this one connection. Log and keep serving every
                        // Run we still own. Back off inside this arm — a bare retry
                        // spins at 100% CPU while the socket stays readable, and
                        // staying in this arm keeps SIGHUP/ctrl_c responsive.
                        let _ = crate::diagnostics::record(format_args!("ctxmuxd: transient accept error, continuing to serve: {source}"));
                        tokio::time::sleep(ACCEPT_BACKOFF).await;
                        continue;
                    }
                };
                let manager = Arc::clone(&manager);
                tokio::spawn(async move {
                    if let Err(error) = handle_connection(stream, manager).await {
                        let _ = crate::diagnostics::record(format_args!("ctxmuxd connection error: {error}"));
                    }
                });
            }
            signal = tokio::signal::ctrl_c() => {
                signal.map_err(|source| ServerError::io(&socket_path, source))?;
                manager.shutdown_owned_controls(TMUX_SHUTDOWN_TIMEOUT)?;
                manager.qualification_stats.finish();
                let _ = diagnostics::shutdown();
                return Ok(());
            }
            _ = sigchld.recv() => {
                // A child (any child) reached a waitable terminal state. Poke the
                // native owner thread; it re-peeks its whole watched set once and
                // routes any terminal leader through the same non-reaping
                // peek-then-`reap_leader` path the timed sweep used. `recv`
                // coalesces pending SIGCHLDs into a single readiness, so a burst of
                // exits collapses to one wake — the owner's single sweep still sees
                // every one, which is why coalescing is correct rather than lossy.
                // We consult no siginfo/pid here: the wake is a pure hint, and the
                // owner's peek remains the authoritative observation.
                manager.native_runs.owner_wake().wake();
            }
            _ = sighup.recv() => {
                if manager.persistence.is_none() {
                    let _ = crate::diagnostics::record(format_args!(
                        "ctxmuxd: SIGHUP ignored: upgrade continuity requires --state-dir"
                    ));
                    continue;
                }
                let Some(state_dir) = state_dir.as_deref() else {
                    let _ = crate::diagnostics::record(format_args!(
                        "ctxmuxd: SIGHUP ignored: no state directory recorded for re-exec"
                    ));
                    continue;
                };
                let Some(result) = drive_exec_upgrade(
                    socket_path.clone(), state_dir.to_path_buf(), Arc::clone(&listener), Arc::clone(&manager)
                ).await? else { return Ok(()); };
                match result {
                    Ok(()) => unreachable!(
                        "a successful exec-in-place replaces the process image and never returns"
                    ),
                    Err(UpgradeAbort::BeforeExtract(error)) => {
                        // Reversible failure: nothing has been extracted, all
                        // controls are still owned. Abort the upgrade and keep
                        // serving by falling through to the next loop iteration.
                        let _ = crate::diagnostics::record(format_args!(
                            "ctxmuxd: exec-in-place upgrade aborted before extract, continuing to serve: {error}"
                        ));
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

fn snapshot_stop_operations_for_upgrade(
    manager: &RunManager,
) -> Result<Vec<HandoffStopOperation>, UpgradeAbort> {
    manager
        .registry
        .handoff_stop_operations()
        .map_err(|failures| UpgradeAbort::BeforeExtract(ServerError::Shutdown { failures }))
}

/// Perform an exec-in-place upgrade: drain, extract the live native runs, write
/// the handoff manifest, clear CLOEXEC on exactly the descriptors that must
/// survive, and execve this binary. On success this replaces the process image
/// and never returns. `manager.persistence` MUST be `Some` (the caller checks).
/// The reversible half of an exec-in-place upgrade: everything that can still be
/// abandoned with every live Run intact.
///
/// Split out because the boundary matters more than the line count. Each step
/// here fails as [`UpgradeAbort::BeforeExtract`], which costs a log line and
/// leaves the daemon serving. Once the caller passes the point of no return,
/// none of that is true any more — so a step that *can* live in here belongs in
/// here, and this signature is where to add one.
///
/// Returns the unlinked manifest file and the verified exec target.
fn prepare_exec_upgrade(
    state_dir: &std::path::Path,
) -> Result<(std::fs::File, PathBuf), UpgradeAbort> {
    use std::os::unix::fs::OpenOptionsExt as _;

    persistence::validate_state_dir(state_dir)
        .map_err(|error| UpgradeAbort::BeforeExtract(ServerError::Persistence(error)))?;

    // A regular, immediately unlinked state-dir file avoids the pipe-capacity
    // deadlock that a complete bounded Input ledger could trigger before exec:
    // no incoming reader exists until the image has already been replaced.
    let handoff_path = state_dir.join(format!(".ctxmux-handoff-{}", uuid::Uuid::new_v4()));
    let handoff_file = std::fs::OpenOptions::new()
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
    // A libtest process cannot advertise the daemon CLI's schema. The signal
    // cancellation fixture supplies a probe target; verification still runs and
    // that fixture must cancel before exec is reachable.
    #[cfg(test)]
    let exe = std::env::var_os("CTXMUX_TEST_UPGRADE_TARGET").map_or(exe, PathBuf::from);

    // Ask the target what it accepts, while refusing is still free. This is the
    // last moment it is: the schema is checked again on the far side of the exec
    // (`handoff::read_manifest`), but by then the old image no longer exists to
    // refuse anything, and the incoming image's exit closes the inherited pty
    // masters — killing every live Run at once. Here the same mismatch is a log
    // line and a daemon that keeps serving.
    crate::handoff::verify_exec_target(&exe).map_err(|reason| {
        UpgradeAbort::BeforeExtract(ServerError::Shutdown {
            failures: format!("exec target rejected before any live Run was risked: {reason}"),
        })
    })?;
    Ok((handoff_file, exe))
}

/// Settle every extracted Run's unoffered output debt, so the durable barrier
/// that follows fences every byte READ rather than merely every byte OFFERED.
///
/// Fail-stop on a Run that is no longer registered rather than skipping it. A
/// Run whose descriptors were just extracted is by construction still
/// registered and `Retained` — `extract_live_descriptors` validates every entry
/// as `Watching` and refuses the whole attempt when one is crossing terminal
/// cleanup. So `None` here is a broken invariant, and skipping it would exec
/// over exactly the bytes this pass exists to save: the same silent-skip shape
/// as the defect it fixes.
fn offer_outstanding_output_before_barrier(
    manager: &RunManager,
    live: &[LiveDescriptors],
) -> Result<(), UpgradeAbort> {
    for run_id in live.iter().map(|descriptors| descriptors.run_id) {
        let run = manager
            .registry
            .pin(run_id)
            .map_err(|error| error.message.clone())
            .and_then(|run| {
                run.ok_or_else(|| {
                    "it is no longer registered, though extract accepted it as live".to_owned()
                })
            })
            .map_err(|reason| {
                UpgradeAbort::AfterExtract(ServerError::Shutdown {
                    failures: format!(
                        "Run {run_id} cannot offer its outstanding output before the handoff \
                         barrier: {reason}"
                    ),
                })
            })?;
        run.offer_outstanding_output_for_handoff()
            .map_err(|failures| UpgradeAbort::AfterExtract(ServerError::Shutdown { failures }))?;
    }
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "one reversible preflight to irreversible extraction boundary keeps upgrade ownership auditable"
)]
async fn drive_exec_upgrade(
    socket_path: PathBuf,
    state_dir: PathBuf,
    listener: Arc<UnixListener>,
    manager: Arc<RunManager>,
) -> Result<Option<Result<(), UpgradeAbort>>, ServerError> {
    let upgrade_manager = Arc::clone(&manager);
    let upgrade_socket = socket_path.clone();
    let cancellation = Arc::new(UpgradeCancellation::default());
    let worker_cancellation = Arc::clone(&cancellation);
    let mut upgrade = tokio::task::spawn_blocking(move || {
        perform_exec_upgrade(
            &upgrade_socket,
            &state_dir,
            &listener,
            &upgrade_manager,
            &worker_cancellation,
        )
    });
    // Storage retries stay user-cancellable while requests are quiesced.
    // A timeout must not trade away committed output to make upgrade faster.
    tokio::select! {
        result = &mut upgrade => result.map(Some).map_err(|error|
            ServerError::Shutdown { failures: error.to_string() }),
        signal = tokio::signal::ctrl_c() => {
            signal.map_err(|source| ServerError::io(&socket_path, source))?;
            cancellation.cancel();
            if let Some(durable) = &manager.persistence { durable.cancel_storage_waits(); }
            let _ = upgrade.await;
            manager.shutdown_owned_controls(TMUX_SHUTDOWN_TIMEOUT)?;
            manager.qualification_stats.finish();
            Ok(None)
        }
    }
}

#[derive(Default)]
struct UpgradeCancellation {
    cancelled: Mutex<bool>,
    #[cfg(test)]
    changed: std::sync::Condvar,
}

impl UpgradeCancellation {
    fn cancel(&self) {
        *mutex_lock(&self.cancelled) = true;
        #[cfg(test)]
        self.changed.notify_all();
    }

    fn check(&self) -> Result<(), String> {
        if *mutex_lock(&self.cancelled) {
            Err("exec-in-place upgrade cancelled".to_owned())
        } else {
            Ok(())
        }
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "reversible preflight and irreversible descriptor extraction form one ordered transaction"
)]
fn perform_exec_upgrade(
    socket_path: &std::path::Path,
    state_dir: &std::path::Path,
    listener: &UnixListener,
    manager: &RunManager,
    cancellation: &Arc<UpgradeCancellation>,
) -> Result<(), UpgradeAbort> {
    use std::os::fd::AsRawFd as _;
    use std::os::unix::process::CommandExt as _;

    // --- Reversible phase (before the point of no return) ---

    cancellation
        .check()
        .map_err(|failures| UpgradeAbort::BeforeExtract(ServerError::Shutdown { failures }))?;
    if manager
        .persistence
        .as_ref()
        .is_some_and(Persistence::is_failed)
    {
        return Err(UpgradeAbort::BeforeExtract(ServerError::Shutdown {
            failures: "durable state requires recovery; live ownership was preserved".to_owned(),
        }));
    }
    let (handoff_file, exe) = prepare_exec_upgrade(state_dir)?;

    // Fence new request mutations and wait until every already-admitted request
    // has written its response. The fence is RAII-reversible until extraction,
    // so timeout or owner preflight failure restores complete service.
    let mut request_fence = manager
        .upgrade_requests
        .begin_drain(UPGRADE_QUIESCE_TIMEOUT)
        .map_err(|failure| {
            UpgradeAbort::BeforeExtract(ServerError::Shutdown { failures: failure })
        })?;

    let durable = manager
        .persistence
        .as_ref()
        .expect("persistent upgrade")
        .clone();
    if durable.is_failed() {
        return Err(UpgradeAbort::BeforeExtract(ServerError::Shutdown {
            failures: "durable state requires recovery; live ownership was preserved".to_owned(),
        }));
    }
    let deadline = Instant::now() + UPGRADE_QUIESCE_TIMEOUT;
    if !manager.creation_flights.wait_until(deadline) {
        return Err(UpgradeAbort::BeforeExtract(ServerError::Shutdown {
            failures: "in-flight creation still owns unpublished native resources".to_owned(),
        }));
    }
    let cleanup_failures = manager.unpublished_cleanups.wait_until(deadline);
    if !cleanup_failures.is_empty() {
        return Err(UpgradeAbort::BeforeExtract(ServerError::Shutdown {
            failures: cleanup_failures.join("; "),
        }));
    }
    let stop_operations = snapshot_stop_operations_for_upgrade(manager)?;
    let retained = manager.registry.snapshot();
    let epoch = manager.daemon_instance.to_string();
    let listener_fd = listener.as_raw_fd();
    let state_lock_fd = manager
        .persistence
        .as_ref()
        .expect("persistent upgrade")
        .state_lock_fd();
    let resources = manager.resources;
    let prepared = Arc::new(Mutex::new(None));
    let prepared_by_owner = Arc::clone(&prepared);
    let owner_cancellation = Arc::clone(cancellation);
    // Snapshot, validate, and serialize under the same native owner turn that
    // extracts descriptors. Any preflight error returns before forgetting a
    // child or closing a reader, so the request fence restores service.
    // Persist the derived continuation while ownership transfer is reversible.
    // The owner preflight below still owns the point of no return.
    for run in manager.registry.pin_native_for_checkpoint() {
        run.prepare_terminal_checkpoint_for_upgrade()
            .map_err(|failures| UpgradeAbort::BeforeExtract(ServerError::Shutdown { failures }))?;
    }
    #[cfg(test)]
    if let Some(directory) = std::env::var_os("CTXMUX_TEST_UPGRADE_LATE_TAIL") {
        let directory = PathBuf::from(directory);
        let run = manager
            .registry
            .pin_native_for_checkpoint()
            .pop()
            .expect("one private tail Run");
        let before = run.info().latest_output_bytes;
        manager
            .persistence
            .as_ref()
            .unwrap()
            .force_append_storage_full();
        fs::write(directory.join("release-tail"), b"release").unwrap();
        while run.info().latest_output_bytes < before + b"post-checkpoint-tail\r\n".len() as u64 {
            cancellation.check().map_err(|failures| {
                UpgradeAbort::BeforeExtract(ServerError::Shutdown { failures })
            })?;
            thread::sleep(Duration::from_millis(1));
        }
        assert!(run.info().durable_output_bytes.unwrap() < run.info().latest_output_bytes);
    }
    let live = manager
        .native_runs
        .extract_for_handoff_after_preflight(Box::new(move |live| {
            owner_cancellation.check()?;
            if durable.is_failed() {
                return Err(
                    "durable state requires recovery; live ownership was preserved".to_owned(),
                );
            }
            let retained_ids: HashSet<_> = retained.iter().map(|run| run.id).collect();
            if live.iter().any(|run| !retained_ids.contains(&run.run_id)) {
                return Err("handoff native owner contains an unpublished Run".to_owned());
            }
            let live_ids: HashSet<_> = live.iter().map(|run| run.run_id).collect();
            let mut closed_inputs = Vec::new();
            for run in &retained {
                if live_ids.contains(&run.id) {
                    continue;
                }
                if run.is_running() {
                    return Err(format!("Run {} has no live handoff owner", run.id));
                }
                if let Some(RunControl::Native(control)) = &run.incarnation_control {
                    control.closed_quiescence_result()?;
                    closed_inputs.push(crate::handoff::HandoffClosedInput {
                        run_id: run.id,
                        input_state: control.handoff_input_state()?,
                    });
                }
            }
            let manifest = crate::handoff::HandoffManifest::configured(
                epoch,
                listener_fd,
                state_lock_fd,
                live.iter()
                    .map(|run| crate::handoff::HandoffRun {
                        run_id: run.run_id,
                        child_pid: run.child_pid,
                        master_fd: run.master_fd,
                        input_state: run.input_state.clone(),
                    })
                    .collect(),
                stop_operations,
                closed_inputs,
                resources,
            );
            let mut file = handoff_file;
            manifest
                .write_preflight(&mut file)
                .map_err(|error| error.to_string())?;
            *mutex_lock(&prepared_by_owner) = Some((file, manifest));
            Ok(())
        }))
        .map_err(|failures| UpgradeAbort::BeforeExtract(ServerError::Shutdown { failures }))?;
    request_fence.commit();
    #[cfg(test)]
    if let Some(marker) = std::env::var_os("CTXMUX_TEST_UPGRADE_EXTRACTED") {
        fs::write(marker, b"extracted").expect("publish subprocess upgrade barrier");
    }

    // Durable-commit barrier AFTER extract (corrected order): extract closes
    // each run's pty reader (via `entry.output = None`) and relinquishes its
    // child/control, so after extract the owner enqueues no further Appends.
    // Draining the FIFO barrier here fences every byte ever read before we exec,
    // guaranteeing the persisted cursor covers all of them. A barrier *before*
    // extract would race the still-running reader and could leave a replay gap.
    //
    // The barrier fences what was OFFERED, which is only the same thing as what
    // was READ once every outstanding byte has been offered. Ordinary admission
    // may drop or skip an append under queue pressure and let the next push
    // re-offer those bytes — and extract just removed every next push. So each
    // Run settles its debt first, blocking; then the barrier's guarantee is the
    // one this comment claims.
    offer_outstanding_output_before_barrier(manager, &live)?;
    manager
        .persistence_barrier()
        .map_err(UpgradeAbort::AfterExtract)?;

    #[cfg(test)]
    if let Some(marker) = std::env::var_os("CTXMUX_TEST_UPGRADE_BEFORE_EXEC") {
        fs::write(marker, b"barrier committed").expect("publish pre-exec cancellation barrier");
        let mut cancelled = mutex_lock(&cancellation.cancelled);
        while !*cancelled {
            cancelled = cancellation.changed.wait(cancelled).unwrap();
        }
    }

    let (handoff_file, manifest) = mutex_lock(&prepared)
        .take()
        .expect("successful extraction completed manifest preflight");
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
        .arg(read_fd.to_string())
        .arg("--resource-limits")
        .arg(serde_json::to_string(&resources).expect("resource policy serializes"));
    // Linearize the last cancel/exec decision. A completed storage barrier
    // must not override cancellation already accepted by the signal handler.
    let cancelled = mutex_lock(&cancellation.cancelled);
    if *cancelled {
        return Err(UpgradeAbort::AfterExtract(ServerError::Shutdown {
            failures: "exec-in-place upgrade cancelled".to_owned(),
        }));
    }
    let exec_error = command.exec(); // only returns on failure
    drop(cancelled);
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
    runtime_id: RuntimeId,
    daemon_instance: DaemonInstanceId,
    build_id: RuntimeBuildId,
    registry: RunRegistry,
    creation_flights: CreationFlightOwner,
    unpublished_cleanups: UnpublishedCleanupOwner,
    terminal_publications: TerminalPublicationOwner,
    native_input_drains: InputDrainGate,
    native_runs: NativeRuntimeOwner,
    foreground_observations: foreground_observation::ForegroundObservationOwner,
    qualification_stats: QualificationStats,
    retention_budget: RetentionBudget,
    resources: ResourceLimits,
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

/// Keeps the first recoverable Stop settlement independent of any request or
/// attachment connection. Dropping the daemon-owned task records an unknown
/// terminal result instead of leaving every retry blocked on an empty cell.
struct RecoverableStopSettlementOwner {
    manager: Arc<RunManager>,
    run_id: RunId,
    settlement: Option<RecoverableStopSettlement>,
    upgrade_permit: Option<UpgradeRequestPermit>,
}

impl RecoverableStopSettlementOwner {
    async fn run(mut self) {
        let result = self
            .settlement
            .as_mut()
            .expect("daemon Stop settlement remains owned")
            .wait()
            .await;
        self.finish(result);
        if self.upgrade_permit.is_some() {
            self.wait_for_handoff_ready().await;
        }
    }

    fn finish(&mut self, result: ControlResult) {
        let settlement = self
            .settlement
            .take()
            .expect("daemon Stop settlement finishes once");
        self.manager
            .registry
            .settle_recoverable_stop(settlement, result);
    }

    async fn wait_for_handoff_ready(&self) {
        let deadline = Instant::now() + STOP_ACK_TIMEOUT;
        loop {
            match self.manager.native_runs.handoff_ready(self.run_id) {
                Ok(true) => return,
                Ok(false) => {}
                Err(error) => {
                    let _ = crate::diagnostics::record(format_args!(
                        "ctxmuxd recoverable Stop handoff readiness probe failed for Run {}: {error}",
                        self.run_id
                    ));
                    return;
                }
            }
            let now = Instant::now();
            if now >= deadline {
                let _ = crate::diagnostics::record(format_args!(
                    "ctxmuxd timed out waiting for Run {} Stop cleanup to reach a handoff boundary",
                    self.run_id
                ));
                return;
            }
            tokio::time::sleep(CHILD_CONTROL_POLL.min(deadline.saturating_duration_since(now)))
                .await;
        }
    }
}

impl Drop for RecoverableStopSettlementOwner {
    fn drop(&mut self) {
        let Some(settlement) = self.settlement.take() else {
            return;
        };
        self.manager.registry.settle_recoverable_stop(
            settlement,
            Err(control_unknown(ProtocolError::new(
                ErrorCode::Internal,
                "recoverable native Stop settlement owner ended without a result",
            ))),
        );
    }
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
                    let _ = crate::diagnostics::record(format_args!(
                        "ctxmuxd failed to finish staged persistence rollback: {failure}"
                    ));
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
    AfterReplayPage,
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
        Self::with_instance_stats_and_resources(
            daemon_instance,
            qualification_stats,
            ResourceLimits::default(),
        )
    }

    fn with_instance_stats_and_resources(
        daemon_instance: DaemonInstanceId,
        qualification_stats: QualificationStats,
        resources: ResourceLimits,
    ) -> Self {
        let registry =
            RunRegistry::with_stats_and_resources(qualification_stats.clone(), resources);
        let native_input_drains = InputDrainGate::with_stats_resources_and_budget(
            qualification_stats.clone(),
            resources,
            registry.control_budget(),
        );
        Self {
            runtime_id: RuntimeId::new(),
            daemon_instance,
            build_id: runtime_build_id(),
            registry,
            creation_flights: CreationFlightOwner::with_slots(
                qualification_stats.clone(),
                resources.creation_workers,
            ),
            unpublished_cleanups: UnpublishedCleanupOwner::with_slots(
                qualification_stats.clone(),
                resources.creation_workers,
            ),
            terminal_publications: TerminalPublicationOwner::default(),
            native_input_drains,
            native_runs: NativeRuntimeOwner::with_resources(resources),
            foreground_observations: foreground_observation::ForegroundObservationOwner::new(
                resources,
            ),
            qualification_stats,
            retention_budget: RetentionBudget::with_resources(resources),
            resources,
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

    fn runtime_identity(&self) -> RuntimeIdentity {
        let persistent = self.persistence.is_some();
        let mut capabilities = BTreeMap::from([
            (RUNTIME_CAPABILITY_NATIVE_START.to_owned(), 1),
            (RUNTIME_CAPABILITY_NATIVE_RECOVERABLE_INPUT.to_owned(), 1),
            (RUNTIME_CAPABILITY_NATIVE_RECOVERABLE_STOP.to_owned(), 1),
            (RUNTIME_CAPABILITY_NATIVE_FORK_LEVEL_A.to_owned(), 1),
            (
                RUNTIME_CAPABILITY_NATIVE_EXECUTE_MATERIALIZED_LEVEL_B.to_owned(),
                1,
            ),
            (RUNTIME_CAPABILITY_TMUX_DISCOVER.to_owned(), 1),
        ]);
        #[cfg(target_os = "macos")]
        capabilities.insert(
            ctxmux_protocol::RUNTIME_CAPABILITY_FOREGROUND_OBSERVATION.to_owned(),
            1,
        );
        if persistent {
            capabilities.insert(RUNTIME_CAPABILITY_PERSISTENT_STATE.to_owned(), 1);
            capabilities.insert(
                RUNTIME_CAPABILITY_PLANNED_EXEC_UPGRADE_CONTINUITY.to_owned(),
                1,
            );
        } else {
            capabilities.insert(RUNTIME_CAPABILITY_TMUX_IMPORT.to_owned(), 1);
        }
        RuntimeIdentity {
            daemon_instance_id: self.daemon_instance,
            runtime_id: self.runtime_id,
            runtime_id_persistence: if persistent {
                RuntimeIdPersistence::StateDir
            } else {
                RuntimeIdPersistence::Daemon
            },
            build_id: self.build_id.clone(),
            protocol_generation: PROTOCOL_VERSION,
            platform: std::env::consts::OS.to_owned(),
            arch: std::env::consts::ARCH.to_owned(),
            capabilities,
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
        let resources = persistence.resources();
        let retention_budget = RetentionBudget::with_resources(resources);
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
                        retention_budget.clone(),
                    ),
                    metadata_owner,
                )
            })
            .collect();
        let registry =
            RunRegistry::recovered_with_resources(runs, qualification_stats.clone(), resources);
        let native_input_drains = InputDrainGate::with_stats_resources_and_budget(
            qualification_stats.clone(),
            resources,
            registry.control_budget(),
        );
        Self {
            runtime_id: persistence.runtime_id(),
            daemon_instance: persistence.daemon_instance(),
            build_id: runtime_build_id(),
            registry,
            creation_flights: CreationFlightOwner::with_slots(
                qualification_stats.clone(),
                resources.creation_workers,
            ),
            unpublished_cleanups: UnpublishedCleanupOwner::with_slots(
                qualification_stats.clone(),
                resources.creation_workers,
            ),
            terminal_publications,
            native_input_drains,
            native_runs: NativeRuntimeOwner::with_resources(resources),
            foreground_observations: foreground_observation::ForegroundObservationOwner::new(
                resources,
            ),
            qualification_stats,
            retention_budget,
            resources,
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
    #[allow(
        clippy::too_many_arguments,
        clippy::too_many_lines,
        reason = "one recovery funnel restores live and descriptor-free settled owners together"
    )]
    fn persistent_with_handoff_and_stats(
        persistence: Persistence,
        recovered: Vec<RecoveredRun>,
        qualification_stats: QualificationStats,
        mut adopt: HashMap<RunId, (OwnedFd, u32, HandoffInputState)>,
        stop_operations: Vec<HandoffStopOperation>,
        closed_inputs: Vec<crate::handoff::HandoffClosedInput>,
    ) -> Result<Self, ProtocolError> {
        let resources = persistence.resources();
        let terminal_publications = TerminalPublicationOwner::default();
        let native_runs = NativeRuntimeOwner::with_resources(resources);
        let native_input_drains =
            InputDrainGate::with_stats_and_resources(qualification_stats.clone(), resources);
        let creation_flights = CreationFlightOwner::with_slots(
            qualification_stats.clone(),
            resources.creation_workers,
        );
        let incarnation_failure = IncarnationFailure::default();
        let retention_budget = RetentionBudget::with_resources(resources);
        let known: HashSet<_> = recovered.iter().map(|run| run.info.id).collect();
        if adopt.keys().any(|id| !known.contains(id))
            || closed_inputs.iter().any(|run| !known.contains(&run.run_id))
        {
            return Err(ProtocolError::new(
                ErrorCode::Internal,
                "handoff contains a Run absent from durable Registry",
            ));
        }
        let mut closed_inputs: HashMap<_, _> = closed_inputs
            .into_iter()
            .map(|run| (run.run_id, run.input_state))
            .collect();
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
            let run = if let Some((master_fd, child_pid, input_state)) =
                adopt.remove(&recovered.info.id)
            {
                Run::readopt(
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
                    retention_budget.clone(),
                )?
            } else {
                let control = closed_inputs.remove(&recovered.info.id).map(|input_state| {
                    RunControl::Native(NativeControlOwner::closed_with_input_state(
                        recovered.info.id,
                        input_state,
                        native_input_drains.clone(),
                        native_runs.owner_wake(),
                    ))
                });
                Run::recover_with_control(
                    recovered,
                    durable,
                    LIVE_EVENT_CAPACITY,
                    terminal_publications.clone(),
                    qualification_stats.clone(),
                    retention_budget.clone(),
                    control,
                )
            };
            runs.push((operation_key, run, metadata_owner));
        }
        Ok(Self {
            runtime_id: persistence.runtime_id(),
            daemon_instance: persistence.daemon_instance(),
            build_id: runtime_build_id(),
            registry: RunRegistry::recovered_with_handoff_and_stats(
                runs,
                stop_operations,
                qualification_stats.clone(),
                resources,
                native_input_drains.control_budget(),
            )?,
            creation_flights,
            unpublished_cleanups: UnpublishedCleanupOwner::with_slots(
                qualification_stats.clone(),
                resources.creation_workers,
            ),
            terminal_publications,
            native_input_drains,
            native_runs,
            qualification_stats,
            retention_budget,
            resources,
            live_event_capacity: LIVE_EVENT_CAPACITY,
            persistence: Some(persistence),
            commit_unknown_reservations: Mutex::new(Vec::new()),
            incarnation_failure,
            upgrade_requests: UpgradeRequestGate::default(),
            foreground_observations: foreground_observation::ForegroundObservationOwner::new(
                resources,
            ),
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
        let authority = if let CreationRequest::Fork {
            parent,
            plan: ForkPlan::LevelB { .. },
        } = &request
        {
            Some(
                self.pin(*parent)?
                    .has_continuation_authority_async()
                    .await?,
            )
        } else {
            None
        };
        let materialized = self.materialize_creation_with_authority(request, authority)?;
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
                retention_budget: self.retention_budget.clone(),
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
            let reservation = self.registry.reserve_memory_publication(
                new_run_id,
                Some(operation_key.clone()),
                registry_metadata_bytes(
                    &materialized.persistence_start_info(new_run_id),
                    Some(operation_key),
                ),
            )?;
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
            prepared
                .metadata_bytes()
                .saturating_add(resident_run_metadata_bytes(&prospective)),
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

    #[cfg(test)]
    fn materialize_creation(
        &self,
        request: CreationRequest,
    ) -> Result<MaterializedCreation, ProtocolError> {
        self.materialize_creation_with_authority(request, None)
    }

    fn materialize_creation_with_authority(
        &self,
        request: CreationRequest,
        authority: Option<bool>,
    ) -> Result<MaterializedCreation, ProtocolError> {
        if let CreationRequest::Fork {
            plan: ForkPlan::LevelB { spec },
            ..
        } = &request
        {
            validate_run_spec(spec).map_err(invalid_run_spec)?;
        }
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
                        if !authority.map_or_else(|| parent_run.has_continuation_authority(), Ok)? {
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
                .reserve_memory_publication(
                    new_run_id,
                    operation_key,
                    std::mem::size_of::<Run>() as u64,
                )
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
            tmux::discover(
                socket_path,
                Instant::now() + TMUX_DISCOVERY_TIMEOUT,
                self.resources.tmux_discovery_bytes,
            )
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
            let registry_reservation = self.registry.reserve_memory_publication(
                new_run_id,
                None,
                (std::mem::size_of::<Run>() + socket_path.len() + pane_id.len()) as u64,
            )?;
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
                    discovery_bytes: self.resources.tmux_discovery_bytes,
                    prepare_deadline: started_at + TMUX_IMPORT_PREPARE_TIMEOUT,
                    total_deadline: started_at + TMUX_IMPORT_TOTAL_TIMEOUT,
                    qualification_stats: self.qualification_stats.clone(),
                    retention_budget: self.retention_budget.clone(),
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

    /// Reclaim one already-terminal, unpinned Run so its retained slot returns.
    ///
    /// Memory-only removal is one Registry critical section. Persistent removal
    /// fences the entry, deletes the exact durable row on the persistence actor
    /// off the async runtime, then commits the in-memory removal; a durable
    /// failure restores the fence and the Run stays intact. An unclassifiable
    /// durable outcome fail-stops the incarnation, exactly like a start's
    /// `CommitUnknown`, so durable and in-memory truth never diverge.
    async fn remove(self: &Arc<Self>, id: RunId) -> Result<(), ProtocolError> {
        let Some(_persistence) = &self.persistence else {
            return self.registry.remove_memory(id);
        };
        // Stop already waits for its own publication. Keep this wait for
        // natural exits and older in-flight removal callers; it also releases
        // the pin `validate_removable_entry` requires to be unique.
        if let Ok(run) = self.pin(id) {
            run.await_reaped_publication(Instant::now() + TERMINAL_VISIBILITY_GRACE)
                .await;
            drop(run);
        }
        let manager = Arc::clone(self);
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        thread::Builder::new()
            .name("ctxmux-remove".to_owned())
            .spawn(move || {
                let _ = result_tx.send(manager.remove_persistent(id));
            })
            .map_err(|error| {
                ProtocolError::new(
                    ErrorCode::Internal,
                    format!("failed to start Run removal owner: {error}"),
                )
            })?;
        result_rx.await.map_err(|error| {
            ProtocolError::new(
                ErrorCode::Internal,
                format!("Run removal owner ended without a result: {error}"),
            )
        })?
    }

    fn remove_persistent(&self, id: RunId) -> Result<(), ProtocolError> {
        let persistence = self
            .persistence
            .as_ref()
            .expect("persistent removal has a persistence owner");
        let (candidate, removal) = self.registry.begin_persistent_removal(id)?;
        match persistence.remove_terminal(PersistentCandidate::from(candidate)) {
            RemovalDisposition::Removed => {
                removal.commit();
                Ok(())
            }
            RemovalDisposition::NotRemoved(error) => {
                // The fence restores on drop; the Run stays intact and resolvable.
                drop(removal);
                Err(ProtocolError::new(
                    ErrorCode::Persistence,
                    format!("durable Run removal was rejected: {error}"),
                ))
            }
            RemovalDisposition::Unknown(error) => {
                let message = error.to_string();
                // The durable outcome is unknown: never guess in memory. Keep
                // the fence (leak the owner) and fail-stop the incarnation so
                // restart's SQLite recovery is the only authority.
                std::mem::forget(removal);
                self.incarnation_failure.record(format!(
                    "durable Run removal outcome is unknown; restart is required: {message}"
                ));
                self.creation_flights.fence();
                Err(ProtocolError::new(
                    ErrorCode::Persistence,
                    format!(
                        "durable Run removal outcome is unknown; restart is required: {message}"
                    ),
                ))
            }
        }
    }

    fn validate_recoverable_stop(
        &self,
        operation: &RecoverableStop,
        attached_run: Option<RunId>,
    ) -> Result<(), ControlFailure> {
        if operation.daemon_instance != self.daemon_instance {
            return Err(control_not_applied(ProtocolError::new(
                ErrorCode::DaemonInstanceMismatch,
                "recoverable native Stop belongs to another daemon incarnation",
            )));
        }
        if attached_run.is_some_and(|attached_run| attached_run != operation.id) {
            return Err(control_not_applied(ProtocolError::new(
                ErrorCode::StopOperationConflict,
                "attachment Stop operation names another Run",
            )));
        }
        Ok(())
    }

    fn begin_recoverable_stop(
        self: &Arc<Self>,
        operation: RecoverableStop,
    ) -> Result<RecoverableStopFlight, ControlFailure> {
        let mut upgrade_permit = None;
        self.begin_recoverable_stop_with_owner_permit(operation, &mut upgrade_permit)
    }

    fn begin_recoverable_stop_with_owner_permit(
        self: &Arc<Self>,
        operation: RecoverableStop,
        upgrade_permit: &mut Option<UpgradeRequestPermit>,
    ) -> Result<RecoverableStopFlight, ControlFailure> {
        self.validate_recoverable_stop(&operation, None)?;
        let run_id = operation.id;
        match self
            .registry
            .begin_recoverable_stop(operation.id, operation.operation_key)?
        {
            RecoverableStopAdmission::Retry(flight) => Ok(flight),
            RecoverableStopAdmission::Owner { flight, settlement } => {
                let owner = RecoverableStopSettlementOwner {
                    manager: Arc::clone(self),
                    run_id,
                    settlement: Some(settlement),
                    upgrade_permit: upgrade_permit.take(),
                };
                tokio::spawn(owner.run());
                Ok(flight)
            }
        }
    }

    #[cfg(test)]
    fn get(&self, id: RunId) -> Result<Arc<Run>, ProtocolError> {
        self.pin(id)
    }

    /// Return one ascending page of thin Run summaries and the cursor to
    /// continue from, if any.
    ///
    /// `after` is the exclusive `RunId` cursor from the request and `limit` is
    /// the caller's requested page size before clamping. The clamp lives here so
    /// every entry point (the request handler, tests) shares one bound:
    /// `None`/`0` and any value above [`LIST_MAX_PAGE_RUNS`] all resolve to the
    /// ceiling, which — together with the thin per-row shape — is what keeps the
    /// `Runs` frame inside [`MAX_FRAME_BYTES`]. The returned `next_cursor` is the
    /// last row's id when more Runs remain, and `None` at the end of the fleet.
    fn list(&self, after: Option<RunId>, limit: Option<u32>) -> (Vec<RunSummary>, Option<RunId>) {
        let clamped = clamp_list_limit(limit);
        let (runs, has_more) = self.registry.list_summaries_after(after, clamped);
        let next_cursor = has_more.then(|| runs.last().map(|run| run.id)).flatten();
        (runs, next_cursor)
    }

    /// Whole-fleet ascending listing used by daemon-internal tests.
    ///
    /// Tests assert over the complete retained set rather than one page, so this
    /// walks the same paged primitive to exhaustion. It is not a public path:
    /// clients page through [`RunManager::list`] (over the wire) precisely so no
    /// caller depends on the whole fleet fitting one response.
    #[cfg(test)]
    fn list_all(&self) -> Vec<RunSummary> {
        let mut runs = Vec::new();
        let mut cursor = None;
        loop {
            let (page, next) = self.list(cursor, None);
            runs.extend(page);
            match next {
                Some(next) => cursor = Some(next),
                None => return runs,
            }
        }
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
                retention_budget: self.retention_budget.clone(),
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
        let reservation = self.registry.reserve_memory_publication(
            new_run_id,
            None,
            serde_json::to_vec(&spec).unwrap().len() as u64 + std::mem::size_of::<Run>() as u64,
        )?;
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
    retention_budget: RetentionBudget,
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
            // This record is built for the persistence actor before any PTY
            // owner exists, and what persistence restores is a Run with no
            // live terminal at all. Neither moment has a size to confirm.
            current_size: None,
            native_service: None,
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
    discovery_bytes: usize,
    id: RunId,
    live_event_capacity: usize,
    terminal_publications: TerminalPublicationOwner,
    discovery_deadline: Instant,
    prepare_deadline: Instant,
    total_deadline: Instant,
    qualification_stats: QualificationStats,
    retention_budget: RetentionBudget,
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
            let _ = crate::diagnostics::record(format_args!(
                "ctxmuxd failed to terminate rejected child: {error}"
            ));
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
                let _ = crate::diagnostics::record(format_args!(
                    "ctxmuxd failed to reap rejected child: {error}"
                ));
                if let Some(control) = &self.reap_control {
                    control.record_wait_error(format!(
                        "failed to reap rejected unpublished child: {error}"
                    ));
                }
            }
        }
    }
}

enum ResizeWait {
    Output,
    Persistence,
    Control,
}

struct Run {
    id: RunId,
    spec: Option<RunSpec>,
    lineage: Option<RunLineage>,
    backend: RunBackend,
    capabilities: RunCapabilities,
    pid: Option<u32>,
    state: Mutex<RunState>,
    output: crate::native_output::OutputOwner,
    incarnation_control: Option<RunControl>,
    native_runs: Option<NativeRuntimeOwner>,
    native_service: Option<native_service::NativeService>,
    persistence_mode: PersistenceMode,
    owner_deferred: AtomicBool,
    output_unlocked: Notify,
    persistence_unlocked: Notify,
    persistence_transition: Mutex<()>,
    persistence: Mutex<PersistenceBinding>,
    /// Shares the actor's actual COMMIT cursor after its one durable binding.
    /// Observation must not wait for a persistence publication transition.
    durable_output_head: OnceLock<Arc<AtomicU64>>,
    attachments: AtomicUsize,
    qualification_stats: QualificationStats,
    terminal_publications: TerminalPublicationOwner,
    terminal_ordinal: OnceLock<TerminalOrdinal>,
    live_permit: Mutex<Option<creation::LiveResourcePermit>>,
    /// Woken on terminal publication and actual Native entry retirement, so a
    /// Stop cannot report success while its original runtime holders remain.
    ///
    /// A `Notify` rather than a poll because the waiter is the request path:
    /// the common case must cost one wakeup, not a sleep.
    terminal_visible: Notify,
    events: LiveEventOwner,
    /// Daemon-wide retained-output budget this Run participates in. Held so the
    /// Run can register itself as an eviction victim and so `record_output` can
    /// drive cross-Run reclamation after admitting new bytes.
    retention_budget: RetentionBudget,
}

struct LiveEventOwner {
    capacity: usize,
    budget: crate::resources::ByteBudget,
    state: Mutex<LiveEventState>,
}

struct LiveEventState {
    sender: Option<broadcast::Sender<LiveRunEvent>>,
    ring_memory: Option<crate::resources::BytePermit>,
    cursor: LiveEventCursor,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct LiveEventCursor {
    output_bytes: u64,
    output_discontinuity_revision: u64,
    latest_output_discontinuity_byte: u64,
    observation_revision: u64,
    terminal_revision: u64,
    /// Number of confirmed resizes published for this Run.
    ///
    /// Deliberately separate from `observation_revision`: a missed resize is
    /// recoverable, because the current size is authoritative in `RunInfo`,
    /// whereas a missed observation has no snapshot and closes a live
    /// attachment. Counting them together would tear down an attachment that
    /// merely fell behind on window sizes.
    resize_revision: u64,
    /// Service state has an authoritative snapshot, unlike tmux observations.
    service_revision: u64,
}

#[derive(Clone, Debug)]
struct LiveRunEvent {
    published: PublishedRunEvent,
    before: LiveEventCursor,
    after: LiveEventCursor,
}

#[derive(Debug)]
struct FundedRunEvent {
    event: RunEvent,
    _memory: crate::resources::BytePermit,
}

// Pressure markers contain no heap data. They remain deliverable when every
// leased allocation is held by slow receivers outside the broadcast ring.
#[derive(Clone, Debug)]
enum PublishedRunEvent {
    Funded(Arc<FundedRunEvent>),
    OutputGap(u64),
    ObservationDiscontinuity,
}

const EVENT_ALLOCATION_BYTES: usize =
    std::mem::size_of::<FundedRunEvent>() + 2 * std::mem::size_of::<usize>();

impl LiveRunEvent {
    fn event(&self) -> std::borrow::Cow<'_, RunEvent> {
        match &self.published {
            PublishedRunEvent::Funded(funded) => std::borrow::Cow::Borrowed(&funded.event),
            PublishedRunEvent::OutputGap(latest_output_bytes) => {
                std::borrow::Cow::Owned(RunEvent::Gap {
                    latest_output_bytes: *latest_output_bytes,
                })
            }
            PublishedRunEvent::ObservationDiscontinuity => {
                std::borrow::Cow::Owned(RunEvent::ObservationDiscontinuity)
            }
        }
    }
}

struct LiveEventSubscription {
    receiver: broadcast::Receiver<LiveRunEvent>,
    cursor: LiveEventCursor,
}

impl LiveEventOwner {
    #[cfg(test)]
    fn new(capacity: usize) -> Self {
        Self::with_budget(
            capacity,
            crate::resources::ByteBudget::new(ResourceLimits::DEFAULT.live_event_bytes),
        )
    }

    fn with_budget(capacity: usize, budget: crate::resources::ByteBudget) -> Self {
        Self {
            capacity,
            budget,
            state: Mutex::new(LiveEventState {
                sender: None,
                ring_memory: None,
                cursor: LiveEventCursor {
                    output_bytes: 0,
                    output_discontinuity_revision: 0,
                    latest_output_discontinuity_byte: 0,
                    observation_revision: 0,
                    terminal_revision: 0,
                    resize_revision: 0,
                    service_revision: 0,
                },
            }),
        }
    }

    fn publish(&self, mut event: RunEvent) {
        let mut state = mutex_lock(&self.state);
        // The lease follows each heap envelope past ring eviction and across
        // async sends, including events with no variable payload.
        let memory = if state
            .sender
            .as_ref()
            .is_some_and(|sender| sender.receiver_count() > 0)
        {
            let bytes = match &event {
                RunEvent::Output { chunk } => chunk.data.capacity(),
                RunEvent::Tmux {
                    event: TmuxRunEvent::SessionRenamed { name },
                } => name.capacity(),
                RunEvent::Exited {
                    state:
                        RunState::Exited {
                            signal: Some(signal),
                            ..
                        },
                    ..
                } => signal.capacity(),
                _ => 0,
            };
            if let Some(permit) = self
                .budget
                .reserve(bytes.saturating_add(EVENT_ALLOCATION_BYTES))
            {
                Some(permit)
            } else {
                event = match &event {
                    RunEvent::Output { chunk } => RunEvent::Gap {
                        latest_output_bytes: chunk.end_byte,
                    },
                    _ => RunEvent::ObservationDiscontinuity,
                };
                None
            }
        } else {
            None
        };

        let before = state.cursor;
        match &event {
            RunEvent::Output { chunk } => {
                state.cursor.output_bytes = state.cursor.output_bytes.max(chunk.end_byte);
            }
            RunEvent::Gap {
                latest_output_bytes,
            } => {
                state.cursor.output_discontinuity_revision = state
                    .cursor
                    .output_discontinuity_revision
                    .checked_add(1)
                    .expect("live output-discontinuity revision remains representable");
                state.cursor.latest_output_discontinuity_byte = *latest_output_bytes;
            }
            RunEvent::Exited { .. } | RunEvent::Interrupted { .. } => {
                state.cursor.terminal_revision = state
                    .cursor
                    .terminal_revision
                    .checked_add(1)
                    .expect("live terminal revision remains representable");
            }
            RunEvent::Tmux { .. } | RunEvent::ObservationDiscontinuity => {
                state.cursor.observation_revision = state
                    .cursor
                    .observation_revision
                    .checked_add(1)
                    .expect("live observation revision remains representable");
            }
            RunEvent::ServiceChanged { .. } | RunEvent::Resized { .. } => {
                let revision = if matches!(&event, RunEvent::Resized { .. }) {
                    &mut state.cursor.resize_revision
                } else {
                    &mut state.cursor.service_revision
                };
                *revision = revision
                    .checked_add(1)
                    .expect("live recoverable snapshot revision remains representable");
            }
        }
        if let Some(sender) = state.sender.as_ref() {
            let published = match memory {
                Some(memory) => PublishedRunEvent::Funded(Arc::new(FundedRunEvent {
                    event,
                    _memory: memory,
                })),
                None => match event {
                    RunEvent::Gap {
                        latest_output_bytes,
                    } => PublishedRunEvent::OutputGap(latest_output_bytes),
                    _ => PublishedRunEvent::ObservationDiscontinuity,
                },
            };
            let envelope = LiveRunEvent {
                published,
                before,
                after: state.cursor,
            };
            let _ = sender.send(envelope);
        }
    }

    fn cursor(&self) -> LiveEventCursor {
        mutex_lock(&self.state).cursor
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

impl Drop for Run {
    fn drop(&mut self) {
        self.retention_budget.unregister(self.id);
    }
}

impl RetentionVictim for Run {
    fn run_id(&self) -> RunId {
        self.id
    }

    fn retained_output_bytes(&self) -> Option<usize> {
        self.try_owner(&self.output)
            .map(|output| output.retained_bytes())
    }

    fn reclaimable_output_bytes(&self) -> usize {
        let Some(protected) = self.try_output_protected_from() else {
            return 0;
        };
        let Some(output) = self.try_owner(&self.output) else {
            return 0;
        };
        output.retained_bytes().min(
            usize::try_from(protected.saturating_sub(output.first_available_byte()))
                .unwrap_or(usize::MAX),
        )
    }

    fn is_attached(&self) -> bool {
        self.attachments.load(Ordering::Acquire) != 0
    }

    fn reclaim_output(&self, drop_at_least: usize) -> usize {
        // Takes only this Run's own `output` lock, matching the trait's
        // one-lock-at-a-time contract so the daemon-wide reclaimer never holds
        // two `output` locks or the participants lock while trimming.
        let Some(protected) = self.try_output_protected_from() else {
            return 0;
        };
        let Some(mut output) = self.try_owner(&self.output) else {
            return 0;
        };
        let reclaimable = protected.saturating_sub(output.first_available_byte());
        let freed = output.trim_front_bounded(
            drop_at_least,
            usize::try_from(reclaimable).unwrap_or(usize::MAX),
        );
        output.terminal_pressure(self.id);
        freed
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
    ready: bool,
}

struct TmuxTerminationContext<'a> {
    target: &'a ctxmux_protocol::TmuxPaneInfo,
    socket_identity: TmuxSocketIdentity,
    discovery_bytes: usize,
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
    /// A minimal terminal, collection-eligible memory-only Run for benchmarks —
    /// no PTY, no descriptors, no threads, no child process, so a benchmark can
    /// build thousands of them cheaply to exercise the creation-path candidate
    /// scan at scale. Terminal (`Exited`), unattached, no live control, and its
    /// terminal ordinal is set through `TerminalPublicationOwner::recover` so
    /// `collection_ordinal` returns `Some` (it treats a control-less
    /// `PersistentCapable` Run as quiescent). Held by the Registry alone, so
    /// `strong_count == 1` makes it an eligible replacement candidate.
    #[cfg(test)]
    fn terminal_eligible_for_bench(
        terminal_publications: &TerminalPublicationOwner,
        retention_budget: RetentionBudget,
    ) -> Arc<Self> {
        let terminal_ordinal = OnceLock::new();
        terminal_publications.recover(&terminal_ordinal);
        Arc::new(Self {
            id: RunId::new(),
            spec: None,
            lineage: None,
            backend: RunBackend::Native,
            capabilities: RunCapabilities::NATIVE,
            pid: Some(1),
            state: Mutex::new(RunState::Exited {
                code: 0,
                signal: None,
            }),
            output: crate::native_output::OutputOwner::new(OutputLog::new(
                retention_budget.clone(),
            )),
            incarnation_control: None,
            native_runs: None,
            native_service: Some(native_service::NativeService::new(true)),
            persistence_mode: PersistenceMode::PersistentCapable,
            owner_deferred: AtomicBool::new(false),
            output_unlocked: Notify::new(),
            persistence_unlocked: Notify::new(),
            persistence_transition: Mutex::new(()),
            durable_output_head: std::sync::OnceLock::new(),
            persistence: Mutex::new(PersistenceBinding::Disabled),
            attachments: AtomicUsize::new(0),
            qualification_stats: QualificationStats::default(),
            terminal_publications: terminal_publications.clone(),
            terminal_ordinal,
            live_permit: Mutex::new(None),
            terminal_visible: Notify::new(),
            events: LiveEventOwner::with_budget(
                LIVE_EVENT_CAPACITY,
                retention_budget.event_budget(),
            ),
            retention_budget,
        })
    }

    #[cfg(test)]
    fn new_native_for_owner_test(
        id: RunId,
        control: NativeControlOwner,
        native_runs: NativeRuntimeOwner,
        wait_failure: NativeWaitFailure,
    ) -> Arc<Self> {
        Self::new_native_for_owner_test_with_budget(
            id,
            control,
            native_runs,
            wait_failure,
            RetentionBudget::production(),
        )
    }

    #[cfg(test)]
    fn new_native_for_owner_test_with_budget(
        id: RunId,
        control: NativeControlOwner,
        native_runs: NativeRuntimeOwner,
        wait_failure: NativeWaitFailure,
        retention_budget: RetentionBudget,
    ) -> Arc<Self> {
        Self::new_native(
            NativeSpawnConfig {
                id,
                spec: RunSpec {
                    program: "/bin/cat".to_owned(),
                    args: Vec::new(),
                    cwd: None,
                    env: std::collections::BTreeMap::new(),
                    initial_size: TerminalSize::default(),
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
                retention_budget,
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
                retention_budget: RetentionBudget::production(),
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
                retention_budget: RetentionBudget::production(),
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
        config
            .native_runs
            .ensure_running()
            .map_err(|message| ProtocolError::new(ErrorCode::BackendUnavailable, message))?;
        let qualification_stats = config.qualification_stats.clone();
        let pair = native_pty_system()
            .openpty(to_pty_size(config.spec.initial_size))
            .map_err(|error| pty_open_error(&error))?;
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
        // dup shares the master open-file description, so O_NONBLOCK applies
        // to every master view (including the reader). Never take_writer:
        // portable-pty's writer Drop writes EOF bytes into the child's input.
        let writer = fs::File::from(
            ctxmux_inherited_fd::duplicate_nonblocking_cloexec(reader_fd)
                .map_err(|error| spawn_error("clone nonblocking PTY writer", error))?,
        );
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
        let output = OutputLog::new_native(
            id,
            native_control.confirmed_size(),
            config.retention_budget.clone(),
        );
        let run = Arc::new(Self {
            id,
            spec: Some(config.spec),
            lineage: config.lineage,
            backend: RunBackend::Native,
            capabilities: RunCapabilities::NATIVE,
            pid,
            state: Mutex::new(RunState::Running),
            output: crate::native_output::OutputOwner::new(output),
            incarnation_control: Some(RunControl::Native(native_control)),
            native_runs: Some(config.native_runs),
            native_service: Some(native_service::NativeService::new(false)),
            persistence_mode: config.persistence_mode,
            owner_deferred: AtomicBool::new(false),
            output_unlocked: Notify::new(),
            persistence_unlocked: Notify::new(),
            persistence_transition: Mutex::new(()),
            durable_output_head: std::sync::OnceLock::new(),
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
            live_permit: Mutex::new(None),
            terminal_visible: Notify::new(),
            events: LiveEventOwner::with_budget(
                config.live_event_capacity,
                config.retention_budget.event_budget(),
            ),
            retention_budget: config.retention_budget,
        });
        Self::register_retention(run)
    }

    /// Register a freshly built Run with its daemon-wide retention budget and
    /// return it. Every constructor funnels through this so no Run can retain
    /// output without being an eviction participant. Registration stores only a
    /// `Weak` handle (see `retention`), so it never immortalizes the Run or
    /// disturbs collection's `strong_count == 1` eligibility.
    fn register_retention(run: Arc<Self>) -> Arc<Self> {
        if let Some(durable) = run.lock_owner(&run.persistence).durable() {
            run.bind_durable_output_head(durable);
        }
        if let Some(service) = &run.native_service {
            service.bind(&run);
            run.lock_owner(&run.output).bind_service(service.clone());
            if let Some(RunControl::Native(control)) = &run.incarnation_control {
                control.bind_service(service.clone());
            }
        }
        let victim: Arc<dyn RetentionVictim + Send + Sync> = run.clone();
        run.retention_budget.register(&victim);
        if let (Some(durable), Some(native)) =
            (run.lock_owner(&run.persistence).active(), &run.native_runs)
        {
            durable.register_output_wake(native.owner_wake());
        }
        run
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
            config.discovery_bytes,
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
            output: crate::native_output::OutputOwner::new(OutputLog::with_initial_truncation(
                config.retention_budget.clone(),
            )),
            incarnation_control: Some(RunControl::Tmux(TmuxRunControl {
                writer: Mutex::new(Some(TmuxCommandWriter::new(stdin))),
                commands: commands_tx,
                completion: Mutex::new(TmuxCompletion::Pending(completion_rx)),
            })),
            native_runs: None,
            native_service: None,
            persistence_mode: PersistenceMode::MemoryOnly,
            owner_deferred: AtomicBool::new(false),
            output_unlocked: Notify::new(),
            persistence_unlocked: Notify::new(),
            persistence_transition: Mutex::new(()),
            durable_output_head: std::sync::OnceLock::new(),
            persistence: Mutex::new(PersistenceBinding::Disabled),
            attachments: AtomicUsize::new(0),
            qualification_stats: config.qualification_stats.clone(),
            terminal_publications: config.terminal_publications,
            terminal_ordinal: OnceLock::new(),
            live_permit: Mutex::new(None),
            terminal_visible: Notify::new(),
            events: LiveEventOwner::with_budget(
                config.live_event_capacity,
                config.retention_budget.event_budget(),
            ),
            retention_budget: config.retention_budget,
        });
        let run = Self::register_retention(run);
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
        let discovery_bytes = config.discovery_bytes;
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
                    &TmuxTerminationContext {
                        target: &wait_target,
                        socket_identity,
                        discovery_bytes,
                    },
                );
            })
            .map_err(|error| backend_protocol_error("start tmux control waiter", error))?;
        let child = pending.take_child();
        if let Err(error) = child_tx.send(child) {
            let mut child = error.0;
            let _ = child.terminate_and_reap();
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
        retention_budget: RetentionBudget,
    ) -> Arc<Self> {
        Self::recover_with_control(
            recovered,
            persistence,
            live_event_capacity,
            terminal_publications,
            qualification_stats,
            retention_budget,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn recover_with_control(
        recovered: RecoveredRun,
        persistence: PersistentRun,
        live_event_capacity: usize,
        terminal_publications: TerminalPublicationOwner,
        qualification_stats: QualificationStats,
        retention_budget: RetentionBudget,
        control: Option<RunControl>,
    ) -> Arc<Self> {
        let terminal_ordinal = OnceLock::new();
        terminal_publications.recover(&terminal_ordinal);
        let native_service = matches!(&recovered.info.backend, RunBackend::Native)
            .then(|| native_service::NativeService::new(true));
        let run = Arc::new(Self {
            id: recovered.info.id,
            spec: recovered.info.spec,
            lineage: recovered.info.lineage,
            backend: recovered.info.backend,
            capabilities: recovered.info.capabilities,
            pid: recovered.info.pid,
            state: Mutex::new(recovered.info.state),
            output: crate::native_output::OutputOwner::new(
                OutputLog::from_replay(
                    recovered.replay,
                    retention_budget.clone(),
                    recovered.source_gap_after_byte,
                )
                .recover_terminal(recovered.info.id, &persistence, None),
            ),
            incarnation_control: control,
            native_runs: None,
            native_service,
            persistence_mode: PersistenceMode::PersistentCapable,
            owner_deferred: AtomicBool::new(false),
            output_unlocked: Notify::new(),
            persistence_unlocked: Notify::new(),
            persistence_transition: Mutex::new(()),
            durable_output_head: std::sync::OnceLock::new(),
            persistence: Mutex::new(PersistenceBinding::Active(persistence)),
            attachments: AtomicUsize::new(0),
            qualification_stats,
            terminal_publications,
            terminal_ordinal,
            live_permit: Mutex::new(None),
            terminal_visible: Notify::new(),
            events: LiveEventOwner::with_budget(
                live_event_capacity,
                retention_budget.event_budget(),
            ),
            retention_budget,
        });
        Self::register_retention(run)
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
        retention_budget: RetentionBudget,
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
        // O_NONBLOCK belongs to the shared OFD, not just this fresh fd number.
        // The recovered reader therefore also handles WouldBlock readiness races.
        let writer = fs::File::from(
            ctxmux_inherited_fd::duplicate_nonblocking_cloexec(master_raw)
                .map_err(|error| spawn_error("clone nonblocking re-adopted PTY writer", error))?,
        );
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
            output: crate::native_output::OutputOwner::new(
                OutputLog::from_replay(
                    recovered.replay,
                    retention_budget.clone(),
                    recovered.source_gap_after_byte,
                )
                .recover_terminal(
                    recovered.info.id,
                    &persistence,
                    native_control.confirmed_size(),
                ),
            ),
            incarnation_control: Some(RunControl::Native(native_control)),
            native_runs: Some(native_runs),
            native_service: Some(native_service::NativeService::new(false)),
            persistence_mode: PersistenceMode::PersistentCapable,
            owner_deferred: AtomicBool::new(false),
            output_unlocked: Notify::new(),
            persistence_unlocked: Notify::new(),
            persistence_transition: Mutex::new(()),
            durable_output_head: std::sync::OnceLock::new(),
            persistence: Mutex::new(PersistenceBinding::Active(persistence)),
            attachments: AtomicUsize::new(0),
            qualification_stats,
            terminal_publications,
            terminal_ordinal,
            live_permit: Mutex::new(None),
            terminal_visible: Notify::new(),
            events: LiveEventOwner::with_budget(
                live_event_capacity,
                retention_budget.event_budget(),
            ),
            retention_budget,
        });
        let run = Self::register_retention(run);

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

    fn resident_metadata_bytes(&self) -> u64 {
        resident_metadata_parts(self.spec.as_ref(), &self.backend)
    }

    fn registry_metadata_bytes(&self, key: Option<&CreateOperationKey>) -> u64 {
        registry_metadata_bytes(&self.info(), key)
    }

    fn release_closed_resources(&self) {
        if self.is_running() {
            return;
        }
        if let Some(RunControl::Native(control)) = &self.incarnation_control {
            if let Ok(descriptors) = control.detach_closed_descriptors_after_owner_fence() {
                drop(descriptors);
                mutex_lock(&self.live_permit).take();
            }
        } else if self
            .incarnation_control
            .as_ref()
            .is_none_or(|control| match control {
                RunControl::Tmux(control) => control.closed_quiescence_result().is_ok(),
                RunControl::Native(_) => false,
            })
        {
            mutex_lock(&self.live_permit).take();
        }
    }

    /// Called after the sole Native runtime removes and drops this Run's entry.
    /// Terminal publication alone is earlier than physical owner retirement.
    fn native_entry_retired(&self) {
        if let Some(RunControl::Native(control)) = &self.incarnation_control {
            control.mark_entry_retired();
        }
        self.release_closed_resources();
        self.terminal_visible.notify_waiters();
    }

    fn native_owner_ready(&self) {
        if let Some(service) = &self.native_service {
            service.ready();
        }
    }

    fn native_owner_draining(&self) {
        if let Some(service) = &self.native_service {
            service.draining();
        }
    }

    fn native_owner_stopped(&self, reason: ctxmux_protocol::NativeServiceFailure) {
        // Fan out availability independently of any held per-Run control lock.
        // The runtime fences actual commands separately, with truthful settlement.
        if let Some(service) = &self.native_service {
            service.stopped(reason);
        }
    }

    fn native_output_closed(&self, error: Option<&str>) {
        if let Some(service) = &self.native_service {
            service.update_output(if error.is_some() {
                ctxmux_protocol::NativeOutputStatus::Unavailable {
                    reason: ctxmux_protocol::NativeServiceFailure::ReadFailed,
                }
            } else {
                ctxmux_protocol::NativeOutputStatus::Closed {}
            });
        }
    }

    fn native_output_pressure(&self, pressured: bool) {
        if let Some(service) = &self.native_service {
            service.update_output(if pressured {
                ctxmux_protocol::NativeOutputStatus::Backpressured {}
            } else {
                ctxmux_protocol::NativeOutputStatus::Serving {}
            });
        }
    }

    fn info(&self) -> RunInfo {
        let output = self.output.snapshot();
        let service = self
            .native_service
            .as_ref()
            .map(native_service::NativeService::snapshot);
        RunInfo {
            id: self.id,
            spec: self.spec.clone(),
            lineage: self.lineage.clone(),
            backend: self.backend.clone(),
            capabilities: self.capabilities,
            pid: self.pid,
            state: mutex_lock(&self.state).clone(),
            latest_output_bytes: output.latest_output_bytes,
            durable_output_bytes: self
                .durable_output_head
                .get()
                .map(|head| head.load(Ordering::Acquire)),
            first_available_byte: output.first_available_byte,
            attachments: self.attachments.load(Ordering::Acquire),
            applied_input_bytes: service
                .as_ref()
                .and_then(|service| service.input.completed_input_bytes),
            current_size: service
                .as_ref()
                .and_then(|service| service.input.current_size),
            native_service: service,
        }
    }

    /// Fleet enumeration uses only current short owner observations. A slow
    /// terminal export does not consume every async request worker via List.
    fn summary(&self) -> RunSummary {
        let output = self.output.snapshot();
        RunSummary {
            id: self.id,
            backend: RunBackendKind::from(&self.backend),
            pid: self.pid,
            state: mutex_lock(&self.state).clone(),
            latest_output_bytes: output.latest_output_bytes,
            retained_output_bytes: output.retained_bytes as u64,
            attachments: self.attachments.load(Ordering::Acquire),
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
            &*self.lock_owner(&self.persistence),
            PersistenceBinding::Pending { terminal: Some(_) }
                | PersistenceBinding::CommittedPendingActivation {
                    terminal: Some(_),
                    ..
                }
        )
    }

    async fn input(&self, data: Vec<u8>) -> ControlResult {
        self.begin_input(data).await?.resolve().await
    }

    async fn recoverable_input(
        &self,
        operation: RecoverableInput,
    ) -> Result<AppliedInputRange, ControlFailure> {
        self.native_control()
            .map_err(control_not_applied)?
            .begin_recoverable_input_async(
                operation.operation_key,
                operation.expected_byte,
                operation.data,
            )
            .await?
            .resolve()
            .await
    }

    async fn signal(&self, signal: ctxmux_protocol::RunSignal) -> ControlResult {
        self.begin_signal(signal).await?.resolve().await
    }

    async fn begin_signal(
        &self,
        signal: ctxmux_protocol::RunSignal,
    ) -> Result<PendingSignal, ControlFailure> {
        self.native_control()
            .map_err(control_not_applied)?
            .begin_signal_async(signal)
            .await
    }

    async fn begin_input(&self, data: Vec<u8>) -> Result<PendingInput, ControlFailure> {
        self.native_control()
            .map_err(control_not_applied)?
            .begin_input_async(data)
            .await
    }

    async fn resize_async(&self, size: TerminalSize) -> ControlResult {
        if let Err(error) = validate_terminal_size(size) {
            return Err(control_not_applied(invalid_run_spec(error)));
        }
        let control = self.native_control().map_err(control_not_applied)?;
        loop {
            // Register both actual unlock notifications before attempting either
            // owner. No OS mutex guard crosses an await, and a stopped owner
            // wakes control waiters even while a client holds the VT model.
            let output_changed = self.output_unlocked.notified();
            let persistence_changed = self.persistence_unlocked.notified();
            let control_changed = control.admission_changed().notified();
            tokio::pin!(output_changed, persistence_changed, control_changed);
            output_changed.as_mut().enable();
            persistence_changed.as_mut().enable();
            control_changed.as_mut().enable();
            if self.native_service.as_ref().is_some_and(|service| {
                matches!(
                    service.snapshot().owner,
                    ctxmux_protocol::NativeOwnerStatus::Stopped { .. }
                )
            }) {
                return Err(control_not_applied(ProtocolError::new(
                    ErrorCode::BackendUnavailable,
                    "Native owner stopped before resize admission",
                )));
            }
            match self.try_resize(size, control) {
                Ok(result) => return result,
                Err(ResizeWait::Control) => control_changed.await,
                Err(ResizeWait::Output) => tokio::select! {
                    () = &mut output_changed => {},
                    () = &mut control_changed => {},
                },
                Err(ResizeWait::Persistence) => tokio::select! {
                    () = &mut persistence_changed => {},
                    () = &mut control_changed => {},
                },
            }
        }
    }

    /// Acquire every derived-state owner before the physical ioctl. A busy
    /// view postpones only this request; partial geometry is never published.
    fn try_resize(
        &self,
        size: TerminalSize,
        control: &NativeControlOwner,
    ) -> Result<ControlResult, ResizeWait> {
        let binding = self
            .try_owner(&self.persistence)
            .ok_or(ResizeWait::Persistence)?;
        let mut output = self.try_owner(&self.output).ok_or(ResizeWait::Output)?;
        let persistence = binding.active().cloned();
        let result = control
            .try_resize(size, |applied| {
                let resize = output.resize_terminal(applied);
                self.publish_event(RunEvent::Resized {
                    size: applied,
                    through_byte: resize.through_byte,
                    resize_revision: resize.resize_revision,
                });
                if let Some(persistence) = persistence
                    && let Some(saved) = output.take_stored_checkpoint()
                {
                    persistence.offer_terminal_checkpoint(saved);
                }
            })
            .ok_or(ResizeWait::Control)?;
        if result
            .as_ref()
            .is_err_and(|failure| failure.disposition == CommandDisposition::Unknown)
        {
            output.terminal = None;
            output.terminal_absence = TerminalCheckpointUnavailableReason::InvalidCheckpoint;
            output.note_terminal_fault(ctxmux_protocol::NativeTerminalFaultStage::Resize);
        }
        Ok(result)
    }

    #[cfg(test)]
    fn resize(&self, size: TerminalSize) -> ControlResult {
        if let Err(error) = validate_terminal_size(size) {
            return Err(control_not_applied(invalid_run_spec(error)));
        }
        let control = self.native_control().map_err(control_not_applied)?;
        self.try_resize(size, control).unwrap_or_else(|_| {
            Err(control_not_applied(ProtocolError::new(
                ErrorCode::ControlBackpressure,
                "Native resize owner is busy before admission",
            )))
        })
    }

    #[cfg(test)]
    async fn stop(&self) -> ControlResult {
        self.begin_stop_async()
            .await?
            .resolve(STOP_ACK_TIMEOUT)
            .await
    }

    async fn begin_stop_async(&self) -> Result<PendingStop, ControlFailure> {
        self.native_control()
            .map_err(control_not_applied)?
            .begin_stop_async()
            .await
    }

    fn record_output(&self, data: Vec<u8>) {
        if data.is_empty() {
            return;
        }
        let transition = (self.persistence_mode == PersistenceMode::PersistentCapable)
            .then(|| self.lock_owner(&self.persistence_transition));
        let mut output = self.lock_owner(&self.output);
        let persistence = if self.persistence_mode == PersistenceMode::PersistentCapable {
            self.lock_owner(&self.persistence).active().cloned()
        } else {
            None
        };
        self.record_output_locked(data, &mut output, persistence.as_ref());
        drop(output);
        drop(transition);
        self.retention_budget.reclaim_excess(self.id);
    }

    /// The Native caller already owns raw admission before its PTY read. Tmux
    /// and tests use the blocking wrapper on their independently owned lane.
    fn record_output_locked(
        &self,
        data: Vec<u8>,
        output: &mut OutputLog,
        persistence: Option<&PersistentRun>,
    ) {
        if data.is_empty() {
            return;
        }
        let protected = if self.persistence_mode == PersistenceMode::MemoryOnly {
            u64::MAX
        } else {
            persistence.map_or(0, |durable| {
                if durable.is_failed() {
                    u64::MAX
                } else {
                    durable.next_replay_start()
                }
            })
        };
        if self.persistence_mode == PersistenceMode::PersistentCapable
            && protected == u64::MAX
            && output.source_gap_after_byte.is_none()
        {
            output.mark_source_gap();
        }
        let chunk = output.push_protected(data, protected);
        output.terminal_pressure(self.id);
        // Raw and geometry publications share this admission lock. Both memory
        // and persistent Runs publish the actual chunk, independently of a
        // refused append or failed optional terminal derivation.
        self.publish_event(RunEvent::Output {
            chunk: chunk.clone(),
        });
        if let Some(durable) = persistence
            && !durable.is_failed()
            && durable.queue_has_room()
            && mutex_lock(&self.state).is_running()
        {
            // The offered watermark advances only on acceptance. A refusal
            // leaves all outstanding original bytes for the next funded offer.
            let replay = output.offer_replay(durable.next_replay_start());
            if durable.append(self.id, replay)
                && let Some(saved) = output.take_stored_checkpoint()
            {
                durable.offer_terminal_checkpoint(saved);
            }
        }
    }

    fn mark_output_source_gap(&self) -> u64 {
        self.lock_owner(&self.output).mark_source_gap()
    }

    /// Offer every byte this Run has read but never handed to persistence, so a
    /// following durable barrier actually fences the whole log.
    ///
    /// Called once per Run on the exec-in-place path, between extract (which
    /// stops the pty readers) and the barrier. Ordinary output admission is
    /// allowed to drop or skip an append because the offered watermark stays put
    /// and the NEXT push re-offers the same bytes; extract removes that next
    /// push, so without this the barrier fences an offered watermark that lags
    /// the log and the upgrade exec's over the difference. Those bytes are then
    /// unrecoverable: they are out of the pty kernel buffer already, and the
    /// incoming image resumes from the persisted cursor.
    ///
    /// Safe to call when nothing is outstanding, which is the common case: the
    /// render is empty, `append_blocking` is never reached, and the barrier
    /// behaves exactly as before.
    fn prepare_terminal_checkpoint_for_upgrade(&self) -> Result<(), String> {
        let Some(persistence) = self.lock_owner(&self.persistence).active().cloned() else {
            return Ok(());
        };
        let _transition = self.lock_owner(&self.persistence_transition);
        let (replay, saved) = {
            let mut output = self.lock_owner(&self.output);
            let (scope, _) = output.terminal_snapshot(self.id);
            let saved = if matches!(scope, TerminalContinuation::BasicVt { .. }) {
                output.terminal.as_ref().and_then(TerminalModel::stored)
            } else {
                None
            };
            (output.replay(persistence.next_replay_start()), saved)
        };
        if !replay.chunks.is_empty() && !persistence.append_blocking(self.id, replay) {
            return Err(format!(
                "Run {} could not commit original output before its terminal checkpoint",
                self.id
            ));
        }
        persistence.save_terminal_checkpoint_for_handoff(self.id, saved)
    }

    fn offer_outstanding_output_for_handoff(&self) -> Result<(), String> {
        let Some(persistence) = self.lock_owner(&self.persistence).active().cloned() else {
            return Ok(());
        };
        loop {
            if persistence.is_failed() {
                return Err(format!("Run {} persistence requires recovery", self.id));
            }
            let replay = {
                let output = self.lock_owner(&self.output);
                let outstanding = persistence.next_replay_start();
                if output.latest_output_bytes() <= outstanding {
                    return Ok(());
                }
                output.offer_replay(outstanding)
            };
            if !persistence.append_blocking(self.id, replay) {
                return Err(format!(
                    "Run {} could not offer output before handoff",
                    self.id
                ));
            }
        }
    }

    #[cfg(test)]
    fn subscribe(self: &Arc<Self>) -> (AttachmentGuard, LiveEventSubscription) {
        self.try_subscribe().expect("test attachment is funded")
    }

    fn try_subscribe(
        self: &Arc<Self>,
    ) -> Result<(AttachmentGuard, LiveEventSubscription), ProtocolError> {
        // Publish checks the attachment count before taking this same lock.
        // Taking the lock first closes the count-to-subscription race: once
        // publishers can observe the new attachment, its receiver exists.
        let mut event_state = mutex_lock(&self.events.state);
        if event_state.sender.is_none() {
            // Tokio broadcast uses a power-of-two boxed ring of Mutex<Slot<T>>;
            // Slot owns remaining receivers, position and Option<T>. Shared
            // control/allocator headers have an additional 1 KiB reservation.
            let slots = self.events.capacity.next_power_of_two();
            let bytes = slots
                .saturating_mul(std::mem::size_of::<
                    Mutex<(AtomicUsize, u64, Option<LiveRunEvent>)>,
                >())
                .saturating_add(1024);
            event_state.ring_memory = Some(self.events.budget.reserve(bytes).ok_or_else(|| ProtocolError::new(
                ErrorCode::BackendUnavailable, "live event buffer policy exhausted; increase live_event_bytes or release an attachment",
            ))?);
        }
        self.attachments.fetch_add(1, Ordering::AcqRel);
        let receiver = event_state
            .sender
            .get_or_insert_with(|| {
                let (sender, _) = broadcast::channel(self.events.capacity);
                sender
            })
            .subscribe();
        let subscription = LiveEventSubscription {
            receiver,
            cursor: event_state.cursor,
        };
        drop(event_state);
        let qualification_guard = self
            .qualification_stats
            .guard(QualificationGauge::Attachments);
        let guard = AttachmentGuard {
            run: Arc::clone(self),
            _qualification_guard: qualification_guard,
        };
        Ok((guard, subscription))
    }

    async fn attachment_snapshot(
        &self,
        after_byte: u64,
        view: AttachmentView,
    ) -> Result<AttachedSnapshot, persistence::PersistenceError> {
        // Metadata can acquire control locks; do not nest them under output.
        let mut run = self.info();
        let (mut terminal, mut terminal_restore, resize_revision, head) = {
            let mut output = self.lock_owner(&self.output);
            let (terminal, restore) = match view {
                AttachmentView::Terminal => output.terminal_snapshot(self.id),
                AttachmentView::Raw => (TerminalContinuation::NotRequested, Vec::new()),
            };
            (
                terminal,
                restore,
                output.resize_revision,
                output.latest_output_bytes(),
            )
        };
        let replay_start = match &terminal {
            TerminalContinuation::BasicVt { checkpoint, .. } => checkpoint.through_byte,
            _ => after_byte,
        };
        let replay = self.attachment_replay_page(replay_start, head).await?;
        if matches!(terminal, TerminalContinuation::BasicVt { .. })
            && replay.first_available_byte > replay_start
        {
            terminal = TerminalContinuation::Unavailable {
                reason: TerminalCheckpointUnavailableReason::TailEvicted,
            };
            terminal_restore.clear();
        }
        run.latest_output_bytes = head;
        run.first_available_byte = replay.first_available_byte;
        Ok(AttachedSnapshot {
            run,
            replay,
            terminal,
            terminal_restore,
            resize_revision,
        })
    }

    async fn attachment_replay_page(
        &self,
        after: u64,
        through: u64,
    ) -> Result<OutputReplay, persistence::PersistenceError> {
        let (floor, head) = {
            let output = self.lock_owner(&self.output);
            (
                output.first_available_byte(),
                output.latest_output_bytes().min(through),
            )
        };
        let durable = self.lock_owner(&self.persistence).active().cloned();
        if after < floor
            && let Some(durable) = durable
        {
            let mut page = durable
                .read_replay_page(self.id, after, floor.min(head))
                .await?;
            if !page.chunks.is_empty() {
                page.latest_output_bytes = head;
                return Ok(page);
            }
        }
        Ok(self.lock_owner(&self.output).replay_page(after, head))
    }

    fn publish_event(&self, event: RunEvent) {
        if self.attachments.load(Ordering::Acquire) == 0 {
            return;
        }
        self.events.publish(event);
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

    async fn has_continuation_authority_async(&self) -> Result<bool, ProtocolError> {
        match &self.incarnation_control {
            Some(RunControl::Native(control)) => control.has_continuation_authority_async().await,
            Some(RunControl::Tmux(_)) | None => Ok(false),
        }
    }

    fn has_continuation_authority(&self) -> Result<bool, ProtocolError> {
        match &self.incarnation_control {
            Some(RunControl::Native(control)) => control.has_continuation_authority(),
            Some(RunControl::Tmux(_)) | None => Ok(false),
        }
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

    fn is_running(&self) -> bool {
        mutex_lock(&self.state).is_running()
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
        self.lock_owner(&self.persistence)
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
        let _transition = self.lock_owner(&self.persistence_transition);
        let mut binding = self.lock_owner(&self.persistence);
        let terminal = match std::mem::replace(&mut *binding, PersistenceBinding::Disabled) {
            PersistenceBinding::Pending { terminal } => terminal,
            PersistenceBinding::Disabled
            | PersistenceBinding::CommittedPendingActivation { .. }
            | PersistenceBinding::Active(_) => {
                panic!("persistent Run installs one committed binding")
            }
        };
        if let Some(native) = &self.native_runs {
            persistence.register_output_wake(native.owner_wake());
        }
        self.bind_durable_output_head(&persistence);
        *binding = PersistenceBinding::CommittedPendingActivation {
            durable: persistence,
            terminal,
        };
    }

    fn bind_durable_output_head(&self, persistence: &PersistentRun) {
        assert!(
            self.durable_output_head
                .set(persistence.durable_head_owner())
                .is_ok(),
            "one committed output owner per Run"
        );
    }

    /// Activate output durability only after the Run and exact key are public.
    fn activate_persistence_after_publication(&self) {
        let _transition = self.lock_owner(&self.persistence_transition);
        let replay = self.lock_owner(&self.output).durable_replay(0);
        let (persistence, terminal) = {
            let mut binding = self.lock_owner(&self.persistence);
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
            persistence.mark_source_gap(self.lock_owner(&self.output).source_gap_after_byte);
            persistence.finalize(
                self.id,
                self.pid.expect("native Run has a child PID"),
                replay,
                terminal.clone(),
            );
            self.publish_terminal_state(terminal.clone());
            self.publish_event(RunEvent::Exited { state: terminal });
        } else {
            // This is the whole log from byte 0, the one append that is
            // deliberately not a delta. If the actor refuses it, the offered
            // watermark stays at 0, so the next push re-sends from there rather
            // than a delta that would strand these bytes.
            let _accepted = persistence.append(self.id, replay);
        }
    }

    fn publish_terminal(&self, terminal: RunState) {
        if self.persistence_mode == PersistenceMode::MemoryOnly {
            self.publish_terminal_state(terminal.clone());
            self.publish_event(RunEvent::Exited { state: terminal });
            return;
        }
        let _transition = self.lock_owner(&self.persistence_transition);
        let persistence = {
            let mut binding = self.lock_owner(&self.persistence);
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
        let replay = {
            let output = self.lock_owner(&self.output);
            persistence.mark_source_gap(output.source_gap_after_byte);
            output.durable_replay(0)
        };
        persistence.finalize(
            self.id,
            self.pid.expect("native Run has a child PID"),
            replay,
            terminal.clone(),
        );
        let _output = self.lock_owner(&self.output);
        self.publish_terminal_state(terminal.clone());
        self.publish_event(RunEvent::Exited { state: terminal });
    }

    fn publish_interrupted(&self, reason: InterruptionReason) {
        self.publish_terminal_state(RunState::Interrupted { reason });
        self.publish_event(RunEvent::Interrupted { reason });
    }

    fn publish_terminal_state(&self, terminal: RunState) {
        self.terminal_publications
            .publish(&self.terminal_ordinal, &self.state, terminal);
        // Ordered after publication so a woken waiter always observes the
        // terminal state, never the write that is about to happen. The waiter's
        // own `is_running` recheck also covers this, so no test goes red on the
        // swap -- it is defence in depth, not a tested guarantee.
        self.terminal_visible.notify_waiters();
    }

    /// A terminal Native Run has completed its original entry retirement.
    /// This does not bypass Registry pins, attachments, control uniqueness, or
    /// the descriptor/ledger quiescence checks required for collection.
    ///
    pub(crate) fn terminal_visibility_ready(&self) -> bool {
        !self.is_running()
            && self
                .incarnation_control
                .as_ref()
                .is_none_or(|control| match control {
                    RunControl::Native(control) => control.entry_retired(),
                    RunControl::Tmux(_) => true,
                })
    }

    /// Wait for a reaped Run's terminal publication and Native entry retirement
    /// within the original visibility deadline.
    ///
    /// Publication runs on a `ctxmux-native-blocking` worker, and for a
    /// persistent Run it sits behind a durable finalize ordered after the
    /// appends already queued: measured 1-6 ms quiet, 0.3-3.6 s under a loud
    /// fleet. `remove` reads the very `state` publication writes
    /// (`validate_removable_entry`), so without this wait the `remove` on the
    /// line after a Stop is refused `InvalidRunState` — the deadline would turn
    /// *slow* into *wrong*, which is why it is sized past the measured worst
    /// case rather than under it.
    ///
    /// A Run whose child is not yet reaped is genuinely live: no publication is
    /// coming, so it gets no wait and `remove` refuses it promptly.
    pub(crate) async fn await_reaped_publication(&self, deadline: Instant) {
        loop {
            let notified = self.terminal_visible.notified();
            tokio::pin!(notified);
            // Register before checking both publication and actual entry Drop.
            // Either edge can occur between the check and suspension.
            notified.as_mut().enable();
            if self.terminal_visibility_ready() || !self.child_reaped() {
                return;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() || tokio::time::timeout(remaining, notified).await.is_err() {
                return;
            }
        }
    }

    /// Whether this Run's child is reaped, so a terminal publication is owed.
    ///
    /// Ordered before the Stop receipt (`mark_reaped` precedes the owner reply),
    /// unlike closed quiescence, which is marked after it.
    fn child_reaped(&self) -> bool {
        match &self.incarnation_control {
            Some(RunControl::Native(control)) => control.reap_result().is_ok(),
            Some(RunControl::Tmux(control)) => matches!(
                control.observe_completion(),
                TmuxCompletionObservation::Complete(_)
            ),
            None => false,
        }
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
            if tmux::socket_identity_changed(&target.socket_path, socket_identity) {
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
        match child.exited_without_reaping() {
            Ok(true) => {
                return TmuxWaitOutcome {
                    cause: TmuxWaitCause::ChildExited,
                    cleanup: terminate_tmux_control_child(child),
                };
            }
            Ok(false) => {}
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
    context: &TmuxTerminationContext<'_>,
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
    let confirm_identity = reader_termination
        .as_ref()
        .is_some_and(|reader| reader.ready)
        && matches!(
            &outcome.cause,
            TmuxWaitCause::ReaderTerminated
                | TmuxWaitCause::ChildExited
                | TmuxWaitCause::ProbeWriteFailed(_)
        );
    let mut termination = resolve_tmux_termination(outcome.cause, reader_termination, control_pid);
    if confirm_identity && termination.reason == InterruptionReason::TmuxServerUnavailable {
        match tmux::target_changed_after_control_loss(
            context.target,
            context.socket_identity,
            Instant::now() + TMUX_DISCOVERY_TIMEOUT,
            context.discovery_bytes,
        ) {
            Ok(true) => {
                termination = tmux_control_failure(
                    ProtocolError::new(
                        ErrorCode::TargetChanged,
                        "tmux target identity no longer exists after control loss",
                    ),
                    InterruptionReason::TmuxTargetChanged,
                );
            }
            Err(error) if error.code == ErrorCode::TargetChanged => {
                termination = tmux_control_failure(error, InterruptionReason::TmuxTargetChanged);
            }
            Ok(false) => {}
            Err(error) => {
                termination
                    .error
                    .message
                    .push_str("; target identity unconfirmed: ");
                termination.error.message.push_str(&error.message);
            }
        }
    }
    if let Some(RunControl::Tmux(control)) = &run.incarnation_control {
        control.close_writer();
    }
    let _ = ready.try_send(Err(termination.error));
    run.publish_interrupted(termination.reason);
    let _ = completion.send(cleanup);
    run.release_closed_resources();
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
        cause @ TmuxWaitCause::ProbeWriteFailed(_) => reader
            .filter(|reader| reader.failure.reason != InterruptionReason::TmuxServerUnavailable)
            .map_or_else(
                || fallback_tmux_termination(cause, control_pid),
                |reader| reader.failure,
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
    child.terminate_and_reap()
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
    TmuxReaderTermination {
        failure,
        ready: readiness.ready,
    }
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
        let mut state = mutex_lock(&self.run.events.state);
        if self.run.attachments.load(Ordering::Acquire) == 0 {
            state.sender.take();
            state.ring_memory.take();
        }
    }
}

/// Per-Run retained-output ring, with its bytes welded to the daemon-wide
/// [`RetentionBudget`].
///
/// The log holds a budget handle and reports every retained-byte change through
/// it: `push` adds, each trim (per-Run *and* global) subtracts, and `Drop`
/// subtracts the remainder. Because the log drops exactly when its `Run` drops,
/// every reclamation path — ordinary drop, Registry collection/replacement, and
/// the explicit `remove` verb — decrements the total for free, with no separate
/// accounting hook to keep in sync.
// Bounds cache extent overhead and fits worst-case JSON byte inflation in one frame.
const REPLAY_CACHE_BLOCK_BYTES: usize = 64 * 1024;

struct OutputLog {
    chunks: VecDeque<OutputChunk>,
    // Logical head inside the first block. Advancing it avoids repeatedly
    // copying a 64 KiB block under small writes while retaining the exact suffix.
    front_skip: usize,
    retained_bytes: usize,
    latest_output_bytes: u64,
    source_gap_after_byte: Option<u64>,
    /// Daemon-wide budget this log's bytes count against. A cheap `Arc`-backed
    /// handle; every log shares the one `RunManager` budget.
    budget: RetentionBudget,
    terminal: Option<TerminalModel>,
    terminal_absence: TerminalCheckpointUnavailableReason,
    resize_revision: u64,
    checkpoint_dirty: bool,
    terminal_fault: Option<ctxmux_protocol::NativeTerminalFault>,
    service: Option<native_service::NativeService>,
    facts: Option<Arc<Mutex<native_output::OutputFacts>>>,
}

impl OutputLog {
    fn current_facts(&self) -> native_output::OutputFacts {
        native_output::OutputFacts {
            latest_output_bytes: self.latest_output_bytes,
            first_available_byte: self.first_available_byte(),
            retained_bytes: self.retained_bytes,
        }
    }

    fn publish_facts(&self) {
        if let Some(facts) = &self.facts {
            *mutex_lock(facts) = self.current_facts();
        }
    }

    fn new(budget: RetentionBudget) -> Self {
        Self {
            chunks: VecDeque::new(),
            front_skip: 0,
            retained_bytes: 0,
            latest_output_bytes: 0,
            source_gap_after_byte: None,
            budget,
            terminal: None,
            terminal_absence: TerminalCheckpointUnavailableReason::OriginUnknown,
            resize_revision: 0,
            checkpoint_dirty: false,
            terminal_fault: None,
            service: None,
            facts: None,
        }
    }

    fn with_initial_truncation(budget: RetentionBudget) -> Self {
        Self {
            chunks: VecDeque::new(),
            front_skip: 0,
            retained_bytes: 0,
            latest_output_bytes: 0,
            source_gap_after_byte: Some(0),
            budget,
            terminal: None,
            terminal_absence: TerminalCheckpointUnavailableReason::OriginUnknown,
            resize_revision: 0,
            checkpoint_dirty: false,
            terminal_fault: None,
            service: None,
            facts: None,
        }
    }

    fn from_replay(replay: OutputReplay, budget: RetentionBudget, source_gap: Option<u64>) -> Self {
        let retained_bytes = replay.chunks.iter().map(|chunk| chunk.data.len()).sum();
        // Recovered bytes are live retained payload the moment they load, so
        // they count against the budget exactly as freshly pushed bytes do.
        budget.add(retained_bytes);
        Self {
            retained_bytes,
            chunks: replay.chunks.into(),
            front_skip: 0,
            latest_output_bytes: replay.latest_output_bytes,
            source_gap_after_byte: source_gap.or_else(|| {
                (replay.truncated && replay.first_available_byte == 0)
                    .then_some(replay.latest_output_bytes)
            }),
            budget,
            terminal: None,
            terminal_absence: TerminalCheckpointUnavailableReason::OriginUnknown,
            resize_revision: 0,
            checkpoint_dirty: false,
            terminal_fault: None,
            service: None,
            facts: None,
        }
    }

    fn new_native(id: RunId, size: Option<TerminalSize>, budget: RetentionBudget) -> Self {
        let mut output = Self::new(budget);
        if let Some(size) = size {
            output.terminal = derive_terminal(|| TerminalModel::new(id, size));
            if output.terminal.is_none() {
                output.terminal_absence = TerminalCheckpointUnavailableReason::InvalidCheckpoint;
                output.note_terminal_fault(ctxmux_protocol::NativeTerminalFaultStage::Process);
            }
        }
        output
    }

    fn recover_terminal(
        mut self,
        id: RunId,
        persistence: &PersistentRun,
        confirmed_size: Option<TerminalSize>,
    ) -> Self {
        if let Some(saved) = persistence.load_terminal_checkpoint(id) {
            self.terminal =
                derive_terminal(|| TerminalModel::recover(id, saved, &self.replay(0))).flatten();
            if self
                .terminal
                .as_ref()
                .is_some_and(|t| confirmed_size.is_some_and(|size| size != t.size()))
            {
                self.terminal = None;
            }
            if let Some(terminal) = &self.terminal {
                self.resize_revision = terminal.resize_revision();
            } else {
                self.terminal_absence = TerminalCheckpointUnavailableReason::InvalidCheckpoint;
                self.note_terminal_fault(ctxmux_protocol::NativeTerminalFaultStage::Recovery);
            }
        }
        self
    }

    fn resize_terminal(&mut self, size: TerminalSize) -> TerminalResize {
        self.resize_revision += 1;
        let through_byte = self.latest_output_bytes;
        let resize = self
            .derive_terminal(
                ctxmux_protocol::NativeTerminalFaultStage::Resize,
                |terminal| terminal.resize(size, through_byte),
            )
            .unwrap_or(TerminalResize {
                through_byte,
                resize_revision: self.resize_revision,
                size,
            });
        self.checkpoint_dirty = self.terminal.is_some();
        resize
    }

    fn terminal_snapshot(&mut self, id: RunId) -> (TerminalContinuation, Vec<u8>) {
        let first = self.first_available_byte();
        let latest = self.latest_output_bytes;
        self.derive_terminal(
            ctxmux_protocol::NativeTerminalFaultStage::Export,
            |terminal| terminal.continuation(id, first, latest),
        )
        .unwrap_or_else(|| {
            let terminal = if self.terminal_absence
                == TerminalCheckpointUnavailableReason::InvalidCheckpoint
            {
                TerminalContinuation::Unavailable {
                    reason: self.terminal_absence.clone(),
                }
            } else {
                TerminalContinuation::Unknown {
                    reason: self.terminal_absence.clone(),
                }
            };
            (terminal, Vec::new())
        })
    }

    fn terminal_pressure(&mut self, id: RunId) {
        let first = self.first_available_byte();
        let latest = self.latest_output_bytes;
        if self
            .derive_terminal(
                ctxmux_protocol::NativeTerminalFaultStage::Export,
                |terminal| terminal.retention_cut(id, first, latest),
            )
            .unwrap_or(false)
        {
            self.checkpoint_dirty = true;
        }
    }

    fn take_stored_checkpoint(&mut self) -> Option<StoredCheckpoint> {
        if !self.checkpoint_dirty {
            return None;
        }
        self.checkpoint_dirty = false;
        self.derive_terminal(
            ctxmux_protocol::NativeTerminalFaultStage::Export,
            |terminal| terminal.stored(),
        )
        .flatten()
    }

    fn mark_source_gap(&mut self) -> u64 {
        self.terminal = None;
        self.terminal_absence = TerminalCheckpointUnavailableReason::SourceGap;
        self.source_gap_after_byte = Some(self.latest_output_bytes);
        self.latest_output_bytes
    }

    /// Discard a partially mutated VT model, never its authoritative raw log.
    fn derive_terminal<T>(
        &mut self,
        stage: ctxmux_protocol::NativeTerminalFaultStage,
        derive: impl FnOnce(&mut TerminalModel) -> T,
    ) -> Option<T> {
        let terminal = self.terminal.as_mut()?;
        if let Some(value) = derive_terminal(|| derive(terminal)) {
            Some(value)
        } else {
            self.terminal = None;
            self.terminal_absence = TerminalCheckpointUnavailableReason::InvalidCheckpoint;
            self.checkpoint_dirty = false;
            self.note_terminal_fault(stage);
            None
        }
    }

    fn bind_service(&mut self, service: native_service::NativeService) {
        if let Some(fault) = &self.terminal_fault {
            service.terminal_fault(fault.clone());
        }
        self.service = Some(service);
    }

    fn note_terminal_fault(&mut self, stage: ctxmux_protocol::NativeTerminalFaultStage) {
        if self.terminal_fault.is_none() {
            let fault = ctxmux_protocol::NativeTerminalFault {
                stage,
                through_byte: self.latest_output_bytes,
            };
            if let Some(service) = &self.service {
                service.terminal_fault(fault.clone());
            }
            self.terminal_fault = Some(fault);
        }
    }

    #[cfg(test)]
    fn push(&mut self, data: Vec<u8>) -> OutputChunk {
        self.push_protected(data, u64::MAX)
    }

    fn push_protected(&mut self, data: Vec<u8>, protected: u64) -> OutputChunk {
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
        self.budget.add(chunk.data.len());
        for (index, data) in chunk.data.chunks(REPLAY_CACHE_BLOCK_BYTES).enumerate() {
            let start_byte = chunk.start_byte + (index * REPLAY_CACHE_BLOCK_BYTES) as u64;
            let end_byte = start_byte + data.len() as u64;
            if let Some(tail) = self.chunks.back_mut().filter(|tail| {
                tail.end_byte == start_byte
                    && tail.data.len() + data.len() <= REPLAY_CACHE_BLOCK_BYTES
            }) {
                tail.data.extend_from_slice(data);
                tail.end_byte = end_byte;
            } else {
                self.chunks.push_back(OutputChunk {
                    start_byte,
                    end_byte,
                    data: data.to_vec(),
                });
            }
        }
        self.publish_facts();
        // The complete source chunk is admitted before optional derived work.
        self.derive_terminal(
            ctxmux_protocol::NativeTerminalFaultStage::Process,
            |terminal| terminal.process(&chunk.data),
        );
        let excess = self
            .retained_bytes
            .saturating_sub(self.budget.per_run_limit());
        let reclaimable = usize::try_from(protected.saturating_sub(self.first_available_byte()))
            .unwrap_or(usize::MAX);
        self.trim_front_bounded(excess, reclaimable);
        chunk
    }

    /// Trim exactly the requested reclaimable prefix, even inside a block.
    /// Only offered persistent bytes can be reclaimed. Emptying a window never
    /// rewinds its independently stored lifetime head. A partially consumed
    /// block keeps at most one 64 KiB allocation until fully consumed; no copy
    /// per tiny trim and no over-eviction to satisfy an allocation boundary.
    fn trim_front_bounded(&mut self, drop_at_least: usize, maximum: usize) -> usize {
        let mut freed = 0;
        while freed < drop_at_least && freed < maximum {
            let Some(evicted) = self.chunks.pop_front() else {
                break;
            };
            let skipped = std::mem::take(&mut self.front_skip);
            let available = evicted.data.len() - skipped;
            let bytes = available.min(drop_at_least - freed).min(maximum - freed);
            if bytes < available {
                self.front_skip = skipped + bytes;
                self.chunks.push_front(evicted);
            }
            self.retained_bytes -= bytes;
            self.budget.sub(bytes);
            freed += bytes;
        }
        self.publish_facts();
        freed
    }

    const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    const fn latest_output_bytes(&self) -> u64 {
        self.latest_output_bytes
    }

    fn first_available_byte(&self) -> u64 {
        self.chunks
            .front()
            .map_or(self.latest_output_bytes, |chunk| {
                chunk.start_byte + self.front_skip as u64
            })
    }

    fn replay_page(&self, after_byte: u64, through: u64) -> OutputReplay {
        // A work unit fits worst-case JSON byte inflation inside MAX_FRAME_BYTES.
        let first = after_byte.max(self.first_available_byte());
        let last = through.min(first.saturating_add(64 * 1024));
        let mut replay = OutputReplay {
            chunks: Vec::new(),
            first_available_byte: self.first_available_byte(),
            latest_output_bytes: through.min(self.latest_output_bytes()),
            truncated: after_byte < self.first_available_byte()
                || self
                    .source_gap_after_byte
                    .is_some_and(|gap| after_byte <= gap),
        };
        for chunk in &self.chunks {
            let start = chunk.start_byte.max(first);
            let end = chunk.end_byte.min(last);
            if start < end {
                replay.chunks.push(OutputChunk {
                    start_byte: start,
                    end_byte: end,
                    data: chunk.data[usize::try_from(start - chunk.start_byte)
                        .expect("slice offset fits its allocated chunk")
                        ..usize::try_from(end - chunk.start_byte)
                            .expect("slice end fits its allocated chunk")]
                        .to_vec(),
                });
            }
        }
        replay
    }

    // Cache eviction is not durable retention. Persistence already owns every
    // offered prefix; only the actor's disk policy may change its history floor.
    fn durable_replay(&self, after_byte: u64) -> OutputReplay {
        let mut replay = self.replay(after_byte);
        replay.first_available_byte = 0;
        replay.truncated = self.source_gap_after_byte.is_some();
        replay
    }

    // Bound one asynchronous append's copy to the same 64 KiB replay work unit.
    // The offered watermark advances only through bytes actually in this page.
    fn offer_replay(&self, after_byte: u64) -> OutputReplay {
        let mut replay = self.replay_page(after_byte, self.latest_output_bytes());
        replay.first_available_byte = 0;
        replay.truncated = self.source_gap_after_byte.is_some();
        replay.latest_output_bytes = replay
            .chunks
            .last()
            .map_or(after_byte, |chunk| chunk.end_byte);
        replay
    }

    fn replay(&self, after_byte: u64) -> OutputReplay {
        let first_available_byte = self.first_available_byte();
        OutputReplay {
            chunks: self
                .chunks
                .iter()
                .filter_map(|chunk| retained_after(chunk, after_byte.max(first_available_byte)))
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

impl Drop for OutputLog {
    /// Release this Run's still-retained bytes from the daemon-wide total. This
    /// is the single accounting hook that makes every reclamation path correct
    /// for free: an ordinary drop, a Registry collection or exact replacement,
    /// and the explicit `remove` verb all drop the `Run` (and thus this log),
    /// so none of them needs its own budget bookkeeping. Trims already
    /// decremented what they shed; only the live remainder is left to release.
    fn drop(&mut self) {
        self.budget.sub(self.retained_bytes);
    }
}

pub(crate) const fn resident_run_owner_bytes() -> u64 {
    (std::mem::size_of::<Run>()
        + native_service::resident_service_owner_bytes()
        + native_output::resident_output_facts_bytes()
        + native_control::resident_control_owner_bytes()
        + native_runtime::resident_runtime_owner_bytes()
        + creation::registry_owner_bytes()) as u64
}

/// Resident variable-size owners, separate from serialized durable accounting.
/// Vec capacity includes spare slots. The `BTreeMap` charge reserves nodes
/// at minimum occupancy (headers/edges included), covering sparse nodes
/// without turning an argument count into a product capability limit.
pub(crate) fn resident_spec_bytes(spec: &RunSpec) -> u64 {
    let strings = spec.program.capacity()
        + spec.cwd.as_ref().map_or(0, String::capacity)
        + spec.args.iter().map(String::capacity).sum::<usize>()
        + spec
            .env
            .iter()
            .map(|(key, value)| key.capacity() + value.capacity())
            .sum::<usize>()
        + spec
            .declared_inputs
            .iter()
            .map(|input| input.reference.capacity())
            .sum::<usize>();
    let vectors = spec.args.capacity() * std::mem::size_of::<String>()
        + spec.declared_inputs.capacity()
            * std::mem::size_of::<ctxmux_protocol::RunInputReference>();
    // std BTreeMap nodes hold eleven key/value slots and twelve edges;
    // non-root nodes have at least five entries. Round up for the root.
    let map_nodes = spec.env.len().div_ceil(5)
        * (11 * std::mem::size_of::<(String, String)>() + 14 * std::mem::size_of::<usize>());
    (strings + vectors + map_nodes) as u64
}

pub(crate) fn resident_run_metadata_bytes(info: &RunInfo) -> u64 {
    resident_metadata_parts(info.spec.as_ref(), &info.backend)
}

fn resident_metadata_parts(spec: Option<&RunSpec>, backend: &RunBackend) -> u64 {
    let spec = spec.map_or(0, resident_spec_bytes);
    let backend = match backend {
        RunBackend::Native => 0,
        RunBackend::Tmux {
            socket_path,
            session_id,
            window_id,
            pane_id,
            tmux_version,
            ..
        } => {
            (socket_path.capacity()
                + session_id.capacity()
                + window_id.capacity()
                + pane_id.capacity()
                + tmux_version.capacity()) as u64
        }
    };
    spec + backend
}

fn registry_metadata_bytes(info: &RunInfo, key: Option<&CreateOperationKey>) -> u64 {
    // Serialized variable-size Run metadata plus the retained Rust owner and
    // lifecycle state reservation; output and control queues have byte owners.
    resident_run_metadata_bytes(info)
        + (serde_json::to_vec(info).expect("RunInfo serializes").len()
            + key.map_or(0, |key| key.as_str().len())
            + usize::try_from(resident_run_owner_bytes()).expect("owner type sizes fit host")
            + 128) as u64
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

fn spawn_error(action: &str, error: impl fmt::Display) -> ProtocolError {
    ProtocolError::new(
        ErrorCode::SpawnFailed,
        format!("failed to {action}: {error}"),
    )
}

/// Errnos that mean the host has no pty device left, as opposed to something
/// being wrong with this particular request.
///
/// Both were measured rather than read off a man page, because the two
/// platforms disagree and the Linux one is a false friend:
///
/// * Linux reports `ENOSPC` — reproduced in a `devpts` mount with
///   `newinstance,max=4`, where the fifth `openpty` fails. The name says "no
///   space left on device" and is easy to misread as a full disk; here the
///   exhausted "device" is the pty allocation table.
/// * macOS reports `ENXIO` — reproduced against the default
///   `kern.tty.ptmx_max=511`, where `openpty` fails once the table is full.
///
/// macOS is the reason this classification is load-bearing and not cosmetic:
/// its ceiling sits far below [`fd_budget::FD_BUDGET_LIVE_RUNS`], so pty
/// exhaustion *is* the effective admission ceiling on that platform. Reported
/// as `SpawnFailed` it would be indistinguishable from a bad command, and a
/// macOS fleet could never refuse cleanly at all.
const PTY_EXHAUSTION_ERRNOS: [rustix::io::Errno; 2] =
    [rustix::io::Errno::NOSPC, rustix::io::Errno::NXIO];

/// Classify a failure to allocate a pty.
///
/// A full host is a capacity condition: the request is fine and will succeed
/// once Runs drain, so the caller should back off rather than give up. Every
/// other errno keeps [`ErrorCode::SpawnFailed`], which tells the caller the
/// opposite — retrying this request unchanged is pointless.
///
/// The errno is recovered by downcast rather than by matching the rendered
/// message. That is only possible because `portable-pty` is vendored: upstream
/// formatted the errno into a string with `bail!`, leaving no typed value to
/// inspect. See `third_party/portable-pty/src/unix.rs`.
fn pty_open_error(error: &anyhow::Error) -> ProtocolError {
    let errno = error
        .chain()
        .find_map(|cause| cause.downcast_ref::<io::Error>())
        .and_then(io::Error::raw_os_error)
        .map(rustix::io::Errno::from_raw_os_error);
    if errno.is_some_and(|errno| PTY_EXHAUSTION_ERRNOS.contains(&errno)) {
        return ProtocolError::new(
            ErrorCode::RunCapacity,
            format!("the host has no pty device available: {error:#}"),
        );
    }
    spawn_error("open PTY", format!("{error:#}"))
}

fn control_not_applied(error: ProtocolError) -> ControlFailure {
    ControlFailure {
        confirmed_input_bytes: None,
        error,
        disposition: CommandDisposition::NotApplied,
    }
}

fn control_unknown(error: ProtocolError) -> ControlFailure {
    ControlFailure {
        confirmed_input_bytes: None,
        error,
        disposition: CommandDisposition::Unknown,
    }
}

async fn handle_recoverable_stop_attachment(
    mut wire: Framed<UnixStream, LinesCodec>,
    manager: Arc<RunManager>,
    operation: RecoverableStop,
    after_byte: u64,
    request_permit: UpgradeRequestPermit,
) -> Result<(), ConnectionError> {
    let flight = match manager.begin_recoverable_stop(operation) {
        Ok(flight) => flight,
        Err(failure) => {
            send(
                &mut wire,
                &ServerFrame::Response {
                    response: Response::ControlRejected { failure },
                },
            )
            .await?;
            drop(request_permit);
            return Ok(());
        }
    };
    let (run, result) = flight.resolve().await;
    let response = short_control_response(&run, result);
    if matches!(&response, Response::ControlAccepted { .. }) {
        return attachment::handle_pinned(
            wire,
            manager,
            run,
            after_byte,
            AttachmentView::Raw,
            request_permit,
            Some(response),
        )
        .await;
    }
    send(&mut wire, &ServerFrame::Response { response }).await?;
    drop(request_permit);
    Ok(())
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
                    runtime: manager.runtime_identity(),
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
    match request {
        Request::Attach {
            id,
            after_byte,
            view,
        } => {
            return attachment::handle(wire, manager, id, after_byte, view, request_permit).await;
        }
        Request::AttachRecoverableStop {
            operation,
            after_byte,
        } => {
            return handle_recoverable_stop_attachment(
                wire,
                manager,
                operation,
                after_byte,
                request_permit,
            )
            .await;
        }
        request => {
            let Some(response) = execute_connected_request(&manager, &mut wire, request).await?
            else {
                return Ok(());
            };
            match response {
                // A successful response is the only frame here whose size grows
                // with fleet or user-controlled data, so it goes through the
                // capped sender: if it somehow exceeds the frame maximum, the
                // client receives a typed ResponseTooLarge error instead of the
                // silent socket drop that an unsendable frame caused before. A
                // ProtocolError frame is tiny and bounded, so it uses the plain
                // sender.
                Ok(response) => {
                    // Ignore whether the real frame or the fallback error was
                    // sent: this is a one-shot request connection, so either way
                    // the client has a framed answer and the connection ends
                    // cleanly below.
                    let frame = ServerFrame::Response {
                        response: response.response,
                    };
                    send_capped(&mut wire, &frame).await?;
                    drop(frame);
                    drop(response.foreground_bytes);
                }
                Err(error) => send(&mut wire, &ServerFrame::Error { error }).await?,
            }
        }
    }
    drop(request_permit);
    Ok(())
}

// This is the original connected request's response, not another registry.
// Only variable-sized foreground facts carry their original observation funds.
struct ConnectedResponse {
    response: Response,
    foreground_bytes: Option<crate::resources::BytePermit>,
}

/// Only unadmitted controls are cancelled by socket EOF. Creation owners and
/// recoverable Stop settlements retain their established daemon-owned boundary.
async fn execute_connected_request(
    manager: &Arc<RunManager>,
    wire: &mut Framed<UnixStream, LinesCodec>,
    request: Request,
) -> Result<Option<Result<ConnectedResponse, ProtocolError>>, ConnectionError> {
    let cancel_unadmitted = matches!(
        &request,
        Request::Input { .. }
            | Request::RecoverableInput { .. }
            | Request::Resize { .. }
            | Request::Signal { .. }
    );
    let mut foreground_bytes = None;
    let execution = execute_request(manager, request, &mut foreground_bytes);
    let response = if cancel_unadmitted {
        tokio::select! {
            response = execution => response,
            next = receive(wire) => {
                if next?.is_some() {
                    send(wire, &invalid_request("one-shot request connection accepts exactly one request")).await?;
                }
                return Ok(None);
            }
        }
    } else {
        execution.await
    };
    Ok(Some(response.map(|response| ConnectedResponse {
        response,
        foreground_bytes,
    })))
}

#[allow(
    clippy::too_many_lines,
    reason = "one exhaustive protocol dispatch keeps every public request variant visibly total"
)]
async fn execute_request(
    manager: &Arc<RunManager>,
    request: Request,
    foreground_bytes: &mut Option<crate::resources::BytePermit>,
) -> Result<Response, ProtocolError> {
    match request {
        Request::ObserveForeground { run_id } => {
            let run = manager.pin(run_id)?;
            let (observation, permit) = manager
                .foreground_observations
                .observe(run)
                .await
                .into_parts();
            *foreground_bytes = permit;
            Ok(Response::ForegroundObservation { observation })
        }
        Request::Diagnostics {} => Ok(Response::Diagnostics {
            diagnostics: diagnostics::snapshot(),
        }),
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
        Request::List { after, limit } => {
            let (runs, next_cursor) = manager.list(after, limit);
            Ok(Response::Runs { runs, next_cursor })
        }
        Request::Status { id } => Ok(Response::Status {
            run: manager.info(id)?,
        }),
        Request::Remove { id } => {
            manager.remove(id).await?;
            Ok(Response::Removed { id })
        }
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
            Ok(short_control_response(&run, run.resize_async(size).await))
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
        Request::Stop { operation } => recoverable_stop_response(manager, operation).await,
        Request::Attach { .. } | Request::AttachRecoverableStop { .. } => Err(ProtocolError::new(
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

async fn recoverable_stop_response(
    manager: &Arc<RunManager>,
    operation: RecoverableStop,
) -> Result<Response, ProtocolError> {
    let flight = match manager.begin_recoverable_stop(operation) {
        Ok(flight) => flight,
        Err(failure) => return Ok(Response::ControlRejected { failure }),
    };
    let (run, result) = flight.resolve().await;
    Ok(match result {
        Ok(receipt) => Response::ControlAccepted {
            run: run.info(),
            receipt,
        },
        Err(failure) => Response::ControlRejected { failure },
    })
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

/// Resolve a caller's requested `List` page size to a concrete bound.
///
/// `None` (field absent) and `0` both mean "as many as allowed"; any request is
/// capped at [`LIST_MAX_PAGE_RUNS`]. Centralized so the clamp cannot drift
/// between the request handler and tests, and so a client can never enlarge a
/// page past the size the thin-summary frame budget was proven against.
fn clamp_list_limit(limit: Option<u32>) -> usize {
    match limit {
        None | Some(0) => LIST_MAX_PAGE_RUNS,
        Some(requested) => usize::try_from(requested)
            .unwrap_or(LIST_MAX_PAGE_RUNS)
            .min(LIST_MAX_PAGE_RUNS),
    }
}

async fn send(
    wire: &mut Framed<UnixStream, LinesCodec>,
    frame: &impl serde::Serialize,
) -> Result<(), ConnectionError> {
    wire.send(encode_frame(frame)?).await?;
    Ok(())
}

/// Send a server frame, and if it cannot be framed because it exceeds
/// [`MAX_FRAME_BYTES`], send a typed `ResponseTooLarge` error frame instead of
/// letting the oversize frame drop the connection.
///
/// Returns `Ok(true)` when the requested frame was sent and `Ok(false)` when the
/// fallback error was sent instead, so a caller with follow-on frames (an
/// attachment about to stream replay) can stop after the error rather than
/// speak past it.
///
/// This closes the generic half of the original defect. `List` is fixed
/// structurally by paging thin summaries, but the accept loop's response path is
/// generic: *any* frame that grew past the cap — historically an enormous
/// `RunInfo` in a `List` or an `Attached` header, or any future fat response —
/// would encode to `FrameError::TooLarge`, propagate as `ConnectionError`, and
/// reach the spawn task's diagnostic-and-return, closing the socket with no
/// frame. The client then saw a bare EOF it could not tell apart from a daemon
/// crash. Here the daemon instead emits a small, always-framable error the caller
/// can branch on (`ErrorCode::ResponseTooLarge`) and, for `List`, retry with a
/// smaller page.
///
/// A transport failure while sending the fallback error is still returned as a
/// `ConnectionError` — the connection is genuinely gone at that point, and there
/// is nothing smaller to send. Only the *encode-too-large* case is converted;
/// an encode failure for any other reason (which would be an internal bug, not a
/// size problem) is propagated unchanged.
async fn send_capped(
    wire: &mut Framed<UnixStream, LinesCodec>,
    frame: &ServerFrame,
) -> Result<bool, ConnectionError> {
    match encode_frame(frame) {
        Ok(encoded) => {
            wire.send(encoded).await?;
            Ok(true)
        }
        Err(ctxmux_protocol::FrameError::TooLarge { actual, maximum }) => {
            send(
                wire,
                &ServerFrame::Error {
                    error: ProtocolError::new(
                        ErrorCode::ResponseTooLarge,
                        format!(
                            "response is {actual} bytes; the protocol frame maximum is {maximum}"
                        ),
                    ),
                },
            )
            .await?;
            Ok(false)
        }
        Err(error) => Err(error.into()),
    }
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

    use ctxmux_client::{Attachment, Client, ClientError, replay_bytes};
    use ctxmux_protocol::{
        CommandDisposition, ControlReceipt, CreateOperationKey, ErrorCode, ForkPlan,
        InterruptionReason, ProtocolError, RecoverableStop, Response, RunBackend, RunCapabilities,
        RunEvent, RunId, RunInfo, RunInputKind, RunInputReference, RunSpec, RunState,
        StopDisposition, TerminalSize, TmuxRunEvent, decode_frame,
    };
    use portable_pty::{Child, ChildKiller, ExitStatus};
    use tokio::sync::{Barrier, Notify, broadcast, mpsc};

    use super::{
        AttachmentHookPoint, AttachmentTestHook, CreationHookPoint, CreationRequest,
        CreationTestHook, HandoffInputState, LIVE_EVENT_CAPACITY, LaunchSetupStep,
        NativeRuntimeOwner, NativeWaitFailure, OUTPUT_RETENTION_BYTES, OutputLog, OutputReplay,
        PendingTmuxPublication, Persistence, PersistenceBinding, PersistenceMode, RecoveredRun,
        Run, RunManager, ServerError, ServerFrame, TMUX_DISCOVERY_TIMEOUT,
        TMUX_FAILED_IMPORT_CLEANUP_TIMEOUT, TMUX_IMPORT_DISCOVERY_TIMEOUT,
        TMUX_IMPORT_PREPARE_TIMEOUT, TMUX_IMPORT_TOTAL_TIMEOUT, TMUX_SHUTDOWN_TIMEOUT,
        TmuxCommandKind, TmuxCommandResultKind, TmuxCommandTracker, TmuxCommandWriter,
        TmuxCompletion, TmuxCompletionObservation, TmuxReaderTermination, TmuxRunControl,
        TmuxTermination, TmuxWaitCause, UpgradeRequestAdmission, UpgradeRequestGate, codec,
        mutex_lock, prepare_socket_path, prepare_socket_path_with_hook, resolve_tmux_termination,
        send_capped, serve_with_manager, serve_with_persistence_manager, spawn_error,
    };
    use crate::creation::{TerminalPublicationOwner, UnpublishedCleanupOwner};

    pub(super) async fn fresh_stop(client: &Client, id: RunId) -> RecoverableStop {
        client
            .prepare_stop(id)
            .await
            .expect("prepare recoverable Stop operation")
    }

    async fn next_event_before_timeout(attachment: &Attachment) -> Option<RunEvent> {
        tokio::time::timeout(Duration::from_secs(5), attachment.next_event())
            .await
            .expect("receive attachment event before timeout")
            .expect("read attachment event")
    }

    async fn next_non_service_event(
        attachment: &Attachment,
    ) -> Result<Option<RunEvent>, ClientError> {
        let mut revision = 0;
        loop {
            match attachment.next_event().await? {
                Some(RunEvent::ServiceChanged { service }) => {
                    assert!(
                        service.revision > revision,
                        "service snapshots stay ordered"
                    );
                    revision = service.revision;
                }
                event => return Ok(event),
            }
        }
    }
    use crate::native_control::NativeControlOwner;

    mod creation;

    /// Build a `RunInfo` whose echoed spec carries a declared input of exactly
    /// `reference_bytes`, so the caller can dial the encoded frame size across the
    /// `MAX_FRAME_BYTES` boundary. `declared_inputs` is metadata that never
    /// reaches exec, which is why it is the honest lever for an oversize
    /// `RunInfo`: the same field the public `List`/`Status` responses echo back.
    fn run_info_with_declared_input_bytes(reference_bytes: usize) -> RunInfo {
        RunInfo {
            id: RunId::new(),
            spec: Some(RunSpec {
                program: "/bin/true".to_owned(),
                args: Vec::new(),
                cwd: None,
                env: std::collections::BTreeMap::new(),
                initial_size: TerminalSize::default(),
                declared_inputs: vec![RunInputReference {
                    kind: RunInputKind::Context,
                    reference: "r".repeat(reference_bytes),
                }],
            }),
            lineage: None,
            backend: RunBackend::Native,
            capabilities: RunCapabilities::NATIVE,
            pid: Some(4242),
            state: RunState::Running,
            latest_output_bytes: 0,
            durable_output_bytes: None,
            first_available_byte: 0,
            attachments: 0,
            applied_input_bytes: Some(0),
            current_size: Some(TerminalSize { cols: 80, rows: 24 }),
            native_service: None,
        }
    }

    #[tokio::test]
    async fn send_capped_converts_an_oversize_frame_into_a_typed_error() {
        use ctxmux_protocol::MAX_FRAME_BYTES;
        use futures_util::StreamExt;
        use tokio_util::codec::Framed;

        // This is the generic backstop half of the generation-15 fix, pinned
        // deterministically. The public client cannot smuggle an oversize single
        // `RunInfo` in — the daemon refuses an oversize inbound REQUEST frame at
        // the codec before dispatch — so the response-side overflow is proven
        // here at the exact seam that used to drop the socket in silence.
        //
        // Before the fix, an unsendable frame surfaced as `FrameError::TooLarge`,
        // propagated to the accept loop as a `ConnectionError`, and closed the
        // socket with nothing written; the client saw a bare EOF. `send_capped`
        // instead writes a small, always-framable `ResponseTooLarge` error the
        // client can branch on.

        // A response whose RunInfo clears the frame budget passes through
        // untouched and returns `Ok(true)`; the peer decodes exactly that frame.
        let (server, client) = tokio::net::UnixStream::pair().expect("open a socket pair");
        let mut peer = Framed::new(client, codec());
        let small = ServerFrame::Response {
            response: Response::Status {
                run: run_info_with_declared_input_bytes(64),
            },
        };
        let mut server_wire = Framed::new(server, codec());
        let sent = send_capped(&mut server_wire, &small)
            .await
            .expect("a framable response sends cleanly");
        assert!(
            sent,
            "a within-budget frame reports that the real frame was sent"
        );
        let line = peer
            .next()
            .await
            .expect("peer receives the framable response")
            .expect("read the framable frame");
        assert_eq!(
            decode_frame::<ServerFrame>(&line).expect("decode the framable frame"),
            small,
            "a within-budget response arrives byte-for-byte"
        );

        // A response whose RunInfo overflows the frame budget is NOT dropped: the
        // caller gets `Ok(false)` and the peer decodes a typed ResponseTooLarge
        // error instead of an EOF.
        let (server, client) = tokio::net::UnixStream::pair().expect("open a socket pair");
        let mut peer = Framed::new(client, codec());
        let oversize = ServerFrame::Response {
            response: Response::Status {
                run: run_info_with_declared_input_bytes(MAX_FRAME_BYTES + 64 * 1024),
            },
        };
        let mut server_wire = Framed::new(server, codec());
        let sent = send_capped(&mut server_wire, &oversize)
            .await
            .expect("an oversize response still yields a framed fallback, not a transport error");
        assert!(
            !sent,
            "an oversize frame reports that the fallback error was sent instead"
        );
        let line = peer
            .next()
            .await
            .expect("peer receives the fallback frame rather than an EOF")
            .expect("read the fallback frame");
        match decode_frame::<ServerFrame>(&line).expect("decode the fallback frame") {
            ServerFrame::Error { error } => assert_eq!(
                error.code,
                ErrorCode::ResponseTooLarge,
                "the fallback is a typed ResponseTooLarge protocol error"
            ),
            other => panic!("oversize response must fall back to an error frame, got {other:?}"),
        }
    }

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
            .resize(TerminalSize { rows: 24, cols: 80 }, |_| {})
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
            source_gap_after_byte: None,
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
                // A recovered row carries no confirmed size; re-adoption must
                // take it from the PTY it inherits, not from this record.
                current_size: None,
                native_service: None,
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

        // The manager-shared gate now owns funded input state, not blocking
        // worker slots. With the funding budget occupied, an eight-byte command
        // must refuse before PTY mutation. The original command is accepted
        // below after the reservation pressure is removed at its causal owner.
        let shared_control_budget = crate::resources::ByteBudget::new(4096);
        let budget_hold = shared_control_budget.reserve(4096).unwrap();
        let input_drains = crate::native_control::InputDrainGate::with_stats_resources_and_budget(
            crate::qualification_stats::QualificationStats::default(),
            crate::ResourceLimits::DEFAULT,
            shared_control_budget,
        );

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
            crate::retention::RetentionBudget::production(),
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

        let refused = run.input(b"readopt\n".to_vec()).await.unwrap_err();
        assert_eq!(refused.error.code, ErrorCode::ControlBackpressure);
        assert_eq!(refused.disposition, CommandDisposition::NotApplied);
        assert_eq!(run.info().applied_input_bytes, Some(0));
        drop(budget_hold);
        let receipt = run
            .input(b"readopt\n".to_vec())
            .await
            .expect("original request reaches the real adopted master after funding returns");
        assert_eq!(receipt, ControlReceipt::Input { written_bytes: 8 });
        let info = run.info();
        assert_eq!(info.applied_input_bytes, Some(8));
        assert_eq!(info.pid, Some(child_pid));
        let service = info.native_service.unwrap();
        assert_eq!(service.input.completed_input_bytes, Some(8));
        assert_eq!(service.input.unsettled_commands, 0);
        assert_eq!(service.input.unsettled_request_bytes, 0);
        assert_eq!(service.input.active_confirmed_bytes, 0);

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
        let event =
            tokio::time::timeout(Duration::from_secs(2), next_non_service_event(&attachment))
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
            output: crate::native_output::OutputOwner::new(OutputLog::with_initial_truncation(
                crate::retention::RetentionBudget::production(),
            )),
            incarnation_control: Some(super::RunControl::Tmux(TmuxRunControl {
                writer: Mutex::new(None),
                commands,
                completion: Mutex::new(TmuxCompletion::Pending(completion)),
            })),
            native_runs: None,
            native_service: None,
            persistence_mode: PersistenceMode::MemoryOnly,
            owner_deferred: AtomicBool::new(false),
            output_unlocked: Notify::new(),
            persistence_unlocked: Notify::new(),
            persistence_transition: Mutex::new(()),
            durable_output_head: std::sync::OnceLock::new(),
            persistence: Mutex::new(PersistenceBinding::Disabled),
            attachments: std::sync::atomic::AtomicUsize::new(0),
            qualification_stats: crate::qualification_stats::QualificationStats::default(),
            terminal_publications: TerminalPublicationOwner::default(),
            terminal_ordinal: std::sync::OnceLock::new(),
            live_permit: Mutex::new(None),
            terminal_visible: Notify::new(),
            events: super::LiveEventOwner::new(1),
            retention_budget: crate::retention::RetentionBudget::production(),
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

        for cause in [
            TmuxWaitCause::ReaderTerminated,
            TmuxWaitCause::ChildExited,
            TmuxWaitCause::ProbeWriteFailed("broken pipe".to_owned()),
        ] {
            assert_eq!(
                resolve_tmux_termination(
                    cause,
                    Some(TmuxReaderTermination {
                        failure: observed.clone(),
                        ready: true,
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
            ready: true,
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
    fn stopped_native_owner_rejects_start_before_pty_setup() {
        let manager = RunManager::default();
        manager
            .native_runs
            .shutdown(Instant::now() + Duration::from_secs(2))
            .expect("complete the actual native owner thread");
        let captured_run = Arc::new(Mutex::new(None));
        let mut setup_steps = 0;
        let error = manager
            .start_with_setup(
                CreateOperationKey::new("stopped-owner-before-pty").unwrap(),
                long_running_spec(),
                &captured_run,
                |_, _| {
                    setup_steps += 1;
                    Ok(())
                },
            )
            .expect_err("a stopped owner cannot admit physical launch");
        assert_eq!(error.code, ErrorCode::BackendUnavailable);
        assert!(error.message.contains("owner stopped"));
        assert_eq!(setup_steps, 0);
        assert!(mutex_lock(&captured_run).is_none());
        assert!(manager.list_all().is_empty());
        assert_eq!(manager.unpublished_cleanups.owned_count(), 0);
    }

    #[tokio::test]
    async fn stopped_native_owner_public_start_reports_backend_unavailable() {
        let manager = Arc::new(RunManager::default());
        manager
            .native_runs
            .shutdown(Instant::now() + Duration::from_secs(2))
            .expect("complete the actual native owner thread");
        let server = InProcessServer::start(Arc::clone(&manager));
        let error = tokio::time::timeout(
            Duration::from_secs(2),
            server.client.start(long_running_spec()),
        )
        .await
        .expect("public Start returns instead of waiting for a dead owner")
        .expect_err("the public protocol reports owner unavailability");
        assert!(matches!(
            error,
            ClientError::Protocol {
                code: ErrorCode::BackendUnavailable,
                ..
            }
        ));
        assert!(error.to_string().contains("owner stopped"));
        assert!(server.client.list().await.unwrap().is_empty());
        assert_eq!(manager.unpublished_cleanups.owned_count(), 0);
        server.abort_and_wait().await;
    }

    #[test]
    fn native_owner_exit_after_spawn_rolls_registration_back() {
        let manager = RunManager::default();
        let captured_run = Arc::new(Mutex::new(None));
        let mut child_pid = None;
        let error = manager
            .start_with_setup(
                CreateOperationKey::new("owner-exit-during-launch").unwrap(),
                long_running_spec(),
                &captured_run,
                |step, pid| {
                    if step == LaunchSetupStep::RegisterWaitOwner {
                        let pid = pid.expect("physical child exists before registration");
                        assert!(process_exists(pid));
                        child_pid = Some(pid);
                        manager
                            .native_runs
                            .shutdown(Instant::now() + Duration::from_secs(2))
                            .expect("stop only the private empty fixture owner");
                    }
                    Ok(())
                },
            )
            .expect_err("owner exit races physical launch but cannot publish it");
        assert_eq!(error.code, ErrorCode::SpawnFailed);
        assert!(error.message.contains("owner stopped"));
        assert!(!process_exists(
            child_pid.expect("record the exact fixture child")
        ));
        assert!(manager.list_all().is_empty());
        mutex_lock(&captured_run).take();
        assert!(
            manager
                .unpublished_cleanups
                .wait_until(Instant::now() + Duration::from_secs(2))
                .is_empty(),
            "rejected registration must release exact private child ownership"
        );
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
            assert!(
                manager.list_all().is_empty(),
                "failed start published a Run"
            );
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
                initial_size: TerminalSize::default(),
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

    pub(super) struct InProcessServer {
        pub(super) directory: tempfile::TempDir,
        pub(super) client: Client,
        pub(super) manager: Arc<RunManager>,
        task: tokio::task::JoinHandle<Result<(), ServerError>>,
    }

    impl InProcessServer {
        pub(super) fn start(manager: Arc<RunManager>) -> Self {
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

        async fn abort_and_wait(mut self) {
            self.task.abort();
            assert!(
                (&mut self.task)
                    .await
                    .is_err_and(|error| error.is_cancelled()),
                "fixture listener must release its owner before cold reopening"
            );
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
    async fn recoverable_stop_owner_drop_settles_unknown_and_retains_the_fence() {
        let server = InProcessServer::start(Arc::new(RunManager::default()));
        let run = server
            .client
            .start(long_running_spec())
            .await
            .expect("start owner-drop Stop fixture");
        let operation = fresh_stop(&server.client, run.id).await;
        let admission = server
            .manager
            .registry
            .begin_recoverable_stop(run.id, operation.operation_key.clone())
            .expect("first Stop enters the owner");
        let super::RecoverableStopAdmission::Owner {
            flight,
            mut settlement,
        } = admission
        else {
            panic!("first Stop admission must create one settlement owner");
        };

        // Registry admission only binds the key. Exercise the real Native
        // effect before dropping the task that owes registry settlement.
        // Losing that publication still means Unknown on the same key.
        settlement
            .wait()
            .await
            .expect("real Stop reaches its child owner");

        drop(super::RecoverableStopSettlementOwner {
            manager: Arc::clone(&server.manager),
            run_id: run.id,
            settlement: Some(settlement),
            upgrade_permit: None,
        });

        let (_, result) = flight.resolve().await;
        let failure = result.expect_err("dropped daemon owner settles an unknown result");
        assert_eq!(failure.error.code, ErrorCode::Internal);
        assert_eq!(failure.disposition, CommandDisposition::Unknown);

        let replay = server
            .manager
            .begin_recoverable_stop(operation.clone())
            .expect("same operation replays the retained unknown result");
        let (_, replayed) = replay.resolve().await;
        let replayed = replayed.expect_err("unknown result remains unknown on retry");
        assert_eq!(replayed.error.code, ErrorCode::Internal);
        assert_eq!(replayed.disposition, CommandDisposition::Unknown);

        let different_key = fresh_stop(&server.client, run.id).await;
        let Err(conflict) = server.manager.begin_recoverable_stop(different_key) else {
            panic!("unknown Stop must retain its exact Run fence");
        };
        assert_eq!(conflict.error.code, ErrorCode::StopOperationConflict);
        assert_eq!(conflict.disposition, CommandDisposition::NotApplied);
        wait_for_exit(&server.client, run.id).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn public_attach_completes_an_empty_retained_window_without_extra_frames() {
        let resources = super::ResourceLimits {
            hot_output_bytes: 1,
            run_output_bytes: 1,
            ..super::ResourceLimits::DEFAULT
        };
        let manager = Arc::new(RunManager::with_instance_stats_and_resources(
            ctxmux_protocol::DaemonInstanceId::new(),
            super::QualificationStats::default(),
            resources,
        ));
        let server = InProcessServer::start(manager);
        let first = server.client.start(long_running_spec()).await.unwrap();
        let second = server.client.start(long_running_spec()).await.unwrap();
        server
            .manager
            .get(first.id)
            .unwrap()
            .record_output(b"abcdef".to_vec());
        server
            .manager
            .get(second.id)
            .unwrap()
            .record_output(b"z".to_vec());
        let (attachment, snapshot) =
            tokio::time::timeout(Duration::from_secs(5), server.client.attach(first.id, 0))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(snapshot.replay.first_available_byte, 6);
        assert_eq!(snapshot.replay.latest_output_bytes, 6);
        assert!(snapshot.replay.chunks.is_empty());
        assert!(snapshot.replay.truncated);
        attachment.detach().await.unwrap();
        for id in [first.id, second.id] {
            server.manager.get(id).unwrap().stop().await.unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn latched_persistence_refuses_upgrade_before_extract_and_keeps_public_controls() {
        let temp = tempfile::tempdir().unwrap();
        let state_dir = temp.path().join("state");
        let (persistence, recovered) = super::Persistence::open(&state_dir).unwrap();
        let server = InProcessServer::start(Arc::new(RunManager::persistent(
            persistence.clone(),
            recovered,
        )));
        let first = server.client.start(long_running_spec()).await.unwrap();
        persistence.fail_next_insert_after_commit();
        assert!(server.client.start(long_running_spec()).await.is_err());
        assert!(persistence.is_failed());
        let listener = tokio::net::UnixListener::bind(temp.path().join("upgrade.sock")).unwrap();
        assert!(matches!(
            super::perform_exec_upgrade(
                &server.directory.path().join("ctxmux.sock"),
                &state_dir,
                &listener,
                &server.manager,
                &Arc::new(super::UpgradeCancellation::default())
            ),
            Err(super::UpgradeAbort::BeforeExtract(super::ServerError::Shutdown { failures }))
                if failures == "durable state requires recovery; live ownership was preserved"
        ));
        assert!(
            server
                .client
                .status(first.id)
                .await
                .unwrap()
                .state
                .is_running()
        );
        assert!(process_exists(first.pid.unwrap()));
        let (attachment, _) = server.client.attach(first.id, 0).await.unwrap();
        server
            .client
            .input(first.id, b"still-owned\n".to_vec())
            .await
            .unwrap();
        let event =
            tokio::time::timeout(Duration::from_secs(5), next_non_service_event(&attachment))
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        assert!(matches!(event, RunEvent::Output { .. }));
        attachment.detach().await.unwrap();
        server.manager.get(first.id).unwrap().stop().await.unwrap();
    }

    #[test]
    fn upgrade_storage_pressure_subprocess() {
        let Some(directory) = std::env::var_os("CTXMUX_TEST_UPGRADE_PRESSURE_DIR") else {
            return;
        };
        let directory = std::path::PathBuf::from(directory);
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let (persistence, recovered) =
                    super::Persistence::open(directory.join("state")).unwrap();
                let manager = Arc::new(RunManager::persistent(persistence, recovered));
                let socket = directory.join("sock");
                let listener = tokio::net::UnixListener::bind(&socket).unwrap();
                super::serve_with_manager(
                    socket,
                    listener,
                    manager,
                    None,
                    None,
                    Some(directory.join("state")),
                )
                .await
                .unwrap();
            });
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn real_sighup_storage_wait_remains_ctrl_c_cancellable_after_extract() {
        signal_upgrade_cancellation(false).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn real_ctrl_c_after_durable_upgrade_barrier_prevents_exec() {
        signal_upgrade_cancellation(true).await;
    }

    fn upgrade_cancellation_spec(after_barrier: bool, directory: &std::path::Path) -> RunSpec {
        if after_barrier {
            long_running_spec()
        } else {
            RunSpec {
                program: "/bin/sh".to_owned(),
                args: vec![
                    "-c".to_owned(),
                    concat!(
                        "IFS= read -r line; printf '%s\\n' \"$line\"; ",
                        "while [ ! -e \"$1/release-tail\" ]; do sleep 0.01; done; ",
                        "printf 'post-checkpoint-tail\\n'; exec /bin/cat"
                    )
                    .to_owned(),
                    "ctxmux-upgrade-tail".to_owned(),
                    directory.to_string_lossy().into_owned(),
                ],
                ..long_running_spec()
            }
        }
    }

    struct UpgradeTestChild(std::process::Child);

    impl Drop for UpgradeTestChild {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    async fn signal_upgrade_cancellation(after_barrier: bool) {
        let temp = tempfile::tempdir().unwrap();
        let marker = temp.path().join("extracted");
        let before_exec = temp.path().join("before-exec");
        let target = ctxmux_test_support::fixture_executable();
        let log = fs::File::create(temp.path().join("daemon.log")).unwrap();
        let mut command = Command::new(std::env::current_exe().unwrap());
        if after_barrier {
            command.env("CTXMUX_TEST_UPGRADE_BEFORE_EXEC", &before_exec);
        } else {
            command
                .env("CTXMUX_TEST_UPGRADE_LATE_TAIL", temp.path())
                .env(
                    "CTXMUX_TEST_UPGRADE_RETRY_MARKER",
                    temp.path().join("actual-storage-retry"),
                );
        }
        let mut child = UpgradeTestChild(
            command
                .args([
                    "--exact",
                    "tests::upgrade_storage_pressure_subprocess",
                    "--nocapture",
                ])
                .env("CTXMUX_TEST_UPGRADE_PRESSURE_DIR", temp.path())
                .env("CTXMUX_TEST_UPGRADE_EXTRACTED", &marker)
                .env("CTXMUX_TEST_UPGRADE_TARGET", target)
                .env(
                    "CTXMUX_FIXTURE_HANDOFF_SCHEMA",
                    super::handoff::HANDOFF_SCHEMA,
                )
                .stdout(Stdio::null())
                .stderr(log)
                .spawn()
                .unwrap(),
        );
        let client = Client::new(temp.path().join("sock"));
        tokio::time::timeout(Duration::from_secs(5), async {
            while client.ping().await.is_err() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let spec = upgrade_cancellation_spec(after_barrier, temp.path());
        let run = client.start(spec).await.unwrap();
        client.input(run.id, b"pressure\n".to_vec()).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while client.status(run.id).await.unwrap().latest_output_bytes == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let pid = rustix::process::Pid::from_raw(i32::try_from(child.0.id()).unwrap()).unwrap();
        rustix::process::kill_process(pid, rustix::process::Signal::HUP).unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while !(if after_barrier { &before_exec } else { &marker }).exists() {
                assert!(child.0.try_wait().unwrap().is_none());
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|error| {
            panic!(
                "upgrade did not reach extraction: {error}; {}",
                fs::read_to_string(temp.path().join("daemon.log")).unwrap()
            )
        });
        assert!(
            child.0.try_wait().unwrap().is_none(),
            "durable barrier really waits under pressure"
        );
        if !after_barrier {
            assert!(
                temp.path().join("actual-storage-retry").exists(),
                "actual accepted output reached the failing storage owner"
            );
        }
        rustix::process::kill_process(pid, rustix::process::Signal::INT).unwrap();
        let status = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(status) = child.0.try_wait().unwrap() {
                    break status;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("Ctrl-C cancels a real post-extract storage wait");
        assert!(
            status.success(),
            "{}",
            fs::read_to_string(temp.path().join("daemon.log")).unwrap()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn public_attach_accepts_retention_advancing_during_initial_replay() {
        let (server, hook, mut reached) = hooked_server(AttachmentHookPoint::AfterReplayPage);
        let info = server.client.start(long_running_spec()).await.unwrap();
        let run = server.manager.get(info.id).unwrap();
        run.record_output(vec![b'a'; 192 * 1024]);
        let client = server.client.clone();
        let attaching = tokio::spawn(async move { client.attach(info.id, 0).await });
        tokio::time::timeout(Duration::from_secs(5), reached.recv())
            .await
            .unwrap()
            .unwrap();
        // Actual native child remains daemon-owned while this client is paused.
        // Advance beyond the captured head using normal-sized output pieces.
        for _ in 0..64 {
            run.record_output(vec![b'b'; 64 * 1024]);
        }
        hook.release.notify_one();
        let (attachment, snapshot) = attaching
            .await
            .unwrap()
            .expect("forward window movement is an explicit replay fact");
        assert_eq!(snapshot.replay.first_available_byte, 192 * 1024);
        assert_eq!(snapshot.replay.latest_output_bytes, 192 * 1024);
        assert!(snapshot.replay.truncated);
        assert!(
            snapshot.replay.chunks.is_empty(),
            "a partial old prefix cannot masquerade as the available suffix"
        );
        assert!(run.info().state.is_running());
        assert!(process_exists(info.pid.unwrap()));
        attachment.detach().await.unwrap();
        run.stop().await.unwrap();
        wait_for_exit(&server.client, info.id).await;
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
        let stop_operation = fresh_stop(&server.client, run.id).await;
        server
            .client
            .stop(stop_operation)
            .await
            .expect("stop joined Run");
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
        let stop_operation = fresh_stop(&server.client, run.id).await;
        server
            .client
            .stop(stop_operation)
            .await
            .expect("stop detached Run");
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
                    initial_size: TerminalSize::default(),
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
                        MutationOperation::Stop => {
                            let operation = fresh_stop(&client, id).await;
                            client.stop(operation).await.map(|_| ())
                        }
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
                                ErrorCode::InvalidRunState
                                    | ErrorCode::ControlBackpressure
                                    | ErrorCode::StopOperationConflict
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
            initial_size: TerminalSize::default(),
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
        let mut output = OutputLog::new(crate::retention::RetentionBudget::production());
        for _ in 0..600 {
            output.push(vec![0; 8192]);
        }
        let replay = output.replay(0);
        assert!(replay.truncated);
        assert!(replay.first_available_byte > 0);
        assert_eq!(replay.latest_output_bytes, 600 * 8192);
    }

    #[test]
    fn output_log_reports_its_bytes_to_the_shared_budget() {
        // Each OutputLog's live bytes count against the daemon-wide total, and
        // the per-Run cap keeps one log's contribution at or below 4 MiB.
        let budget = crate::retention::RetentionBudget::with_limit(u64::MAX);
        let mut output = OutputLog::new(budget.clone());
        for _ in 0..8 {
            output.push(vec![0_u8; 1024 * 1024]); // 1 MiB each, 8 MiB pushed
        }
        // Per-Run eviction held the log itself at or below the 4 MiB cap...
        assert!(output.retained_bytes() <= OUTPUT_RETENTION_BYTES);
        // ...and the shared total exactly mirrors that surviving payload.
        assert_eq!(budget.retained_total(), output.retained_bytes() as u64);
    }

    #[test]
    fn failed_terminal_derivation_discards_only_the_partial_model() {
        let id = RunId::new();
        let mut output = OutputLog::new_native(
            id,
            Some(TerminalSize { rows: 4, cols: 12 }),
            crate::retention::RetentionBudget::production(),
        );
        output.push(b"before".to_vec());
        let failure: Option<()> = output.derive_terminal(
            ctxmux_protocol::NativeTerminalFaultStage::Process,
            |terminal| {
                terminal.process(b"partial mutation");
                panic!("owning terminal derivation failure");
            },
        );
        assert_eq!(failure, None);
        assert!(output.terminal.is_none());
        output.push(b"after".to_vec());
        assert_eq!(replay_bytes(&output.replay(0).chunks), b"beforeafter");
        let (continuation, restore) = output.terminal_snapshot(id);
        assert_eq!(
            continuation,
            ctxmux_protocol::TerminalContinuation::Unavailable {
                reason: ctxmux_protocol::TerminalCheckpointUnavailableReason::InvalidCheckpoint
            }
        );
        assert!(restore.is_empty());
        assert!(output.take_stored_checkpoint().is_none());
        let resize = output.resize_terminal(TerminalSize { rows: 5, cols: 14 });
        assert_eq!(resize.through_byte, 11);
        assert_eq!(resize.resize_revision, 1);
        assert_eq!(resize.size, TerminalSize { rows: 5, cols: 14 });
        output.terminal_pressure(id);
        assert_eq!(replay_bytes(&output.replay(0).chunks), b"beforeafter");
    }

    async fn expect_original_raw(client: &Client, original: &RunInfo, expected: &[u8]) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let (view, snapshot) = client.attach(original.id, 0).await.unwrap();
                assert_eq!(snapshot.run.id, original.id);
                assert_eq!(snapshot.run.pid, original.pid);
                assert_eq!(snapshot.replay.first_available_byte, 0);
                let mut next = 0;
                for chunk in &snapshot.replay.chunks {
                    assert_eq!(chunk.start_byte, next);
                    next += chunk.data.len() as u64;
                    assert_eq!(chunk.end_byte, next);
                }
                let bytes = replay_bytes(&snapshot.replay.chunks);
                assert!(expected.starts_with(&bytes), "original bytes changed");
                view.detach().await.unwrap();
                if bytes == expected {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("real PTY bytes arrive within original owning budget");
    }

    struct HeldPublicOwner {
        release: Option<std::sync::mpsc::Sender<()>>,
        worker: Option<std::thread::JoinHandle<()>>,
    }
    impl Drop for HeldPublicOwner {
        fn drop(&mut self) {
            if let Some(release) = self.release.take() {
                let _ = release.send(());
            }
            if let Some(worker) = self.worker.take() {
                let _ = worker.join();
            }
        }
    }

    async fn expect_unadmitted_socket_eof_refund(server: &InProcessServer, id: RunId) {
        use futures_util::{SinkExt as _, StreamExt as _};
        let budget = server.manager.native_input_drains.control_budget();
        let before = budget.used();
        let socket = tokio::net::UnixStream::connect(server.directory.path().join("ctxmux.sock"))
            .await
            .unwrap();
        let mut wire = tokio_util::codec::Framed::new(socket, super::codec());
        wire.send(
            ctxmux_protocol::encode_frame(&ctxmux_protocol::ClientFrame::Hello {
                hello: ctxmux_protocol::ClientHello {
                    protocol: ctxmux_protocol::PROTOCOL_VERSION,
                },
            })
            .unwrap(),
        )
        .await
        .unwrap();
        let hello = wire.next().await.unwrap().unwrap();
        assert!(matches!(
            ctxmux_protocol::decode_frame::<ctxmux_protocol::ServerFrame>(&hello).unwrap(),
            ctxmux_protocol::ServerFrame::Hello { .. }
        ));
        wire.send(
            ctxmux_protocol::encode_frame(&ctxmux_protocol::ClientFrame::Request {
                request: ctxmux_protocol::Request::Input {
                    id,
                    data: b"cancel-before-admission".to_vec(),
                },
            })
            .unwrap(),
        )
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while budget.used() == before {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("actual one-shot input reached funded pending admission");
        drop(wire);
        tokio::time::timeout(Duration::from_secs(5), async {
            while budget.used() != before {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("actual socket EOF cancels and refunds unadmitted payload");
    }

    async fn expect_public_busy_owners(
        server: &InProcessServer,
        clients: &[Client; 2],
        runs: &[RunInfo],
    ) {
        let run = server.manager.pin(runs[0].id).unwrap();
        let (ready, reached) = std::sync::mpsc::channel();
        let (release, blocked) = std::sync::mpsc::channel();
        let held = HeldPublicOwner {
            release: Some(release),
            worker: Some(std::thread::spawn(move || {
                run.native_control().unwrap().with_metadata(|_, _| {
                    let _output = run.lock_owner(&run.output);
                    let _persistence = run.lock_owner(&run.persistence);
                    ready.send(()).unwrap();
                    blocked.recv().unwrap();
                });
            })),
        };
        reached.recv().unwrap();
        expect_unadmitted_socket_eof_refund(server, runs[0].id).await;
        let operation = ctxmux_protocol::RecoverableInput {
            daemon_instance: clients[0].daemon_instance().await.unwrap(),
            operation_key: ctxmux_protocol::InputOperationKey::new("held-public-owner").unwrap(),
            id: runs[0].id,
            expected_byte: 0,
            data: b"not-replayed".to_vec(),
        };
        let input_client = clients[0].clone();
        let resize_client = clients[1].clone();
        let id = runs[0].id;
        let input =
            tokio::spawn(async move { input_client.input(id, b"not-applied".to_vec()).await });
        let resize = tokio::spawn(async move {
            resize_client
                .resize(id, TerminalSize { rows: 5, cols: 14 })
                .await
        });
        let (status, listing, healthy) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(
                clients[0].status(runs[0].id),
                clients[1].list(),
                clients[0].input(runs[1].id, b"h".to_vec()),
            )
        })
        .await
        .expect("busy Run owners cannot occupy both public request workers");
        assert!(
            !input.is_finished(),
            "ordinary input awaits actual owner unlock"
        );
        assert!(!resize.is_finished(), "resize awaits actual owner unlock");
        let status = status.unwrap();
        assert_eq!(status.pid, runs[0].pid);
        assert_eq!(status.applied_input_bytes, Some(0));
        assert_eq!(
            status.current_size,
            Some(TerminalSize { rows: 4, cols: 12 })
        );
        assert_eq!(status.latest_output_bytes, b"A:READY\n".len() as u64);
        let list = listing.unwrap();
        assert_eq!(list.len(), 2);
        assert!(
            list.iter()
                .any(|row| row.id == runs[0].id && row.pid == runs[0].pid)
        );
        assert_eq!(healthy.unwrap().receipt.written_bytes, 1);
        expect_original_raw(&clients[1], &runs[1], b"B:READY\nB:h\n").await;
        drop(held);
        let (input, resize) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(input, resize)
        })
        .await
        .expect("same original requests resume after the actual unlock");
        assert_eq!(input.unwrap().unwrap().receipt.written_bytes, 11);
        resize.unwrap().unwrap();
        let mut expected = b"A:READY\n".to_vec();
        for byte in b"not-applied" {
            expected.extend_from_slice(&[b'A', b':', *byte, b'\n']);
        }
        expect_original_raw(&clients[0], &runs[0], &expected).await;
        let mut operation = operation;
        operation.expected_byte = 11;
        let first = clients[1]
            .recoverable_input(operation.clone())
            .await
            .unwrap();
        let repeated = clients[0].recoverable_input(operation).await.unwrap();
        assert_eq!(first.receipt, repeated.receipt);
        for byte in b"not-replayed" {
            expected.extend_from_slice(&[b'A', b':', *byte, b'\n']);
        }
        expect_original_raw(&clients[0], &runs[0], &expected).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn public_busy_owners_wait_without_stalling_unrelated_run() {
        let server = InProcessServer::start(Arc::new(RunManager::default()));
        let clients = [
            server.client.clone(),
            Client::new(server.directory.path().join("ctxmux.sock")),
        ];
        let mut runs = Vec::new();
        for (name, client) in ["A", "B"].into_iter().zip(&clients) {
            let script = format!(
                "import os,signal,termios\na=termios.tcgetattr(0);a[3]&=~(termios.ICANON|termios.ECHO);a[3]|=termios.ISIG\na[1]&=~termios.ONLCR;a[6][termios.VMIN]=1;a[6][termios.VTIME]=0\ntermios.tcsetattr(0,termios.TCSANOW,a)\nname={name:?}.encode()\nsignal.signal(signal.SIGINT,lambda s,f:exit(0))\nos.write(1,name+b':READY\\n')\nwhile True:\n b=os.read(0,1)\n if not b:break\n os.write(1,name+b':'+b+b'\\n')\n"
            );
            let run = client
                .start(RunSpec {
                    program: "/usr/bin/python3".into(),
                    args: vec!["-u".into(), "-c".into(), script],
                    cwd: None,
                    env: BTreeMap::new(),
                    initial_size: TerminalSize { rows: 4, cols: 12 },
                    declared_inputs: Vec::new(),
                })
                .await
                .unwrap();
            expect_original_raw(client, &run, format!("{name}:READY\n").as_bytes()).await;
            runs.push(run);
        }
        expect_public_busy_owners(&server, &clients, &runs).await;
        for (client, run) in clients.iter().zip(&runs) {
            assert_eq!(client.status(run.id).await.unwrap().pid, run.pid);
            client.input(run.id, vec![3]).await.unwrap();
            wait_for_exit(client, run.id).await;
            assert!(!process_exists(run.pid.unwrap()));
        }
    }

    async fn expect_local_derivation_fault(
        live: &ctxmux_client::Attachment,
        before_service: &ctxmux_protocol::NativeServiceSnapshot,
    ) -> ctxmux_protocol::NativeServiceSnapshot {
        let reported = tokio::time::timeout(Duration::from_secs(5), async {
            let mut revision = before_service.revision;
            loop {
                if let Some(RunEvent::ServiceChanged { service }) = live.next_event().await.unwrap()
                {
                    assert!(
                        service.revision > revision,
                        "service events never overwrite a newer snapshot"
                    );
                    revision = service.revision;
                    if service.terminal_fault.is_some() {
                        break service;
                    }
                }
            }
        })
        .await
        .expect("actual local derivation failure reaches an existing public observer");
        assert!(matches!(
            reported.owner,
            ctxmux_protocol::NativeOwnerStatus::Serving {}
        ));
        assert!(matches!(
            reported.output,
            ctxmux_protocol::NativeOutputStatus::Serving {}
        ));
        assert!(matches!(
            reported.input.phase,
            ctxmux_protocol::NativeInputPhase::Open {}
        ));
        assert_eq!(
            reported.terminal_fault,
            Some(ctxmux_protocol::NativeTerminalFault {
                stage: ctxmux_protocol::NativeTerminalFaultStage::Process,
                through_byte: b"A:READY\nA:!\n".len() as u64,
            })
        );
        reported
    }

    async fn start_derivation_fault_runs(clients: &[Client; 2]) -> Vec<RunInfo> {
        let mut runs = Vec::new();
        for (name, client) in ["A", "B"].into_iter().zip(clients) {
            let script = format!(
                "import os,signal,termios\n\
                 a=termios.tcgetattr(0);a[3]&=~(termios.ICANON|termios.ECHO);a[3]|=termios.ISIG\n\
                 a[1]&=~termios.ONLCR;a[6][termios.VMIN]=1;a[6][termios.VTIME]=0\n\
                 termios.tcsetattr(0,termios.TCSANOW,a)\n\
                 name={name:?}.encode()\n\
                 def interrupt(s,f):\n os.write(1,name+b':CTRL_C\\n');raise SystemExit(0)\n\
                 signal.signal(signal.SIGINT,interrupt)\n\
                 os.write(1,name+b':READY\\n')\n\
                 while True:\n b=os.read(0,1)\n if not b:break\n os.write(1,name+b':'+b+b'\\n')\n"
            );
            let run = client
                .start(RunSpec {
                    program: "/usr/bin/python3".to_owned(),
                    args: vec!["-u".to_owned(), "-c".to_owned(), script],
                    cwd: None,
                    env: BTreeMap::new(),
                    initial_size: TerminalSize { rows: 4, cols: 12 },
                    declared_inputs: Vec::new(),
                })
                .await
                .unwrap();
            expect_original_raw(client, &run, format!("{name}:READY\n").as_bytes()).await;
            runs.push(run);
        }
        runs
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_derivation_failure_keeps_two_real_runs_and_clients_serving() {
        let server = InProcessServer::start(Arc::new(RunManager::default()));
        let identity = server.client.runtime_info().await.unwrap();
        let clients = [
            server
                .client
                .clone()
                .with_expected_runtime_identity(identity.clone()),
            Client::new(server.directory.path().join("ctxmux.sock"))
                .with_expected_runtime_identity(identity),
        ];
        let runs = start_derivation_fault_runs(&clients).await;
        assert_ne!(runs[0].id, runs[1].id);
        assert_ne!(runs[0].pid, runs[1].pid);
        let (live, before_fault) = clients[1].attach(runs[0].id, 0).await.unwrap();
        let before_service = before_fault.run.native_service.unwrap();
        assert!(matches!(
            before_service.owner,
            ctxmux_protocol::NativeOwnerStatus::Serving {}
        ));
        assert!(before_service.terminal_fault.is_none());
        let run = server.manager.pin(runs[0].id).unwrap();
        run.lock_owner(&run.output)
            .terminal
            .as_mut()
            .unwrap()
            .fail_next_process_for_test();
        let accepted = clients[0].input(runs[0].id, b"!".to_vec()).await.unwrap();
        assert_eq!(accepted.receipt.written_bytes, 1);
        expect_original_raw(&clients[0], &runs[0], b"A:READY\nA:!\n").await;
        let reported = expect_local_derivation_fault(&live, &before_service).await;
        live.detach().await.unwrap();
        let (failed_view, snapshot) = clients[1].attach_terminal(runs[0].id, 0).await.unwrap();
        assert_eq!(
            snapshot.terminal,
            ctxmux_protocol::TerminalContinuation::Unavailable {
                reason: ctxmux_protocol::TerminalCheckpointUnavailableReason::InvalidCheckpoint,
            }
        );
        assert!(snapshot.terminal_restore.is_empty());
        assert_eq!(
            snapshot.run.native_service.unwrap().terminal_fault,
            reported.terminal_fault
        );
        assert_eq!(replay_bytes(&snapshot.replay.chunks), b"A:READY\nA:!\n");
        failed_view.detach().await.unwrap();
        clients[1].input(runs[1].id, b"b".to_vec()).await.unwrap();
        expect_original_raw(&clients[1], &runs[1], b"B:READY\nB:b\n").await;
        let (healthy_view, _) = clients[0].attach_terminal(runs[1].id, 0).await.unwrap();
        healthy_view
            .resize(TerminalSize { rows: 3, cols: 9 })
            .await
            .unwrap();
        healthy_view.detach().await.unwrap();
        clients[1].input(runs[0].id, b"z".to_vec()).await.unwrap();
        expect_original_raw(&clients[1], &runs[0], b"A:READY\nA:!\nA:z\n").await;
        assert_eq!(
            clients[0]
                .status(runs[0].id)
                .await
                .unwrap()
                .applied_input_bytes,
            Some(2)
        );
        for (index, name) in ["A", "B"].into_iter().enumerate() {
            let status = clients[index].status(runs[index].id).await.unwrap();
            assert_eq!(status.pid, runs[index].pid);
            assert!(status.state.is_running());
            clients[index].input(runs[index].id, vec![3]).await.unwrap();
            let expected: &[u8] = if name == "A" {
                b"A:READY\nA:!\nA:z\nA:CTRL_C\n"
            } else {
                b"B:READY\nB:b\nB:CTRL_C\n"
            };
            expect_original_raw(&clients[index], &runs[index], expected).await;
            wait_for_exit(&clients[index], runs[index].id).await;
            assert!(
                !process_exists(runs[index].pid.unwrap()),
                "natural exit is reaped"
            );
        }
    }
    #[test]
    fn the_two_wire_byte_counters_diverge_once_trimming_starts() {
        // The reason `retained_output_bytes` exists as a separate wire field.
        // Both counters agree while nothing has been evicted, and an external
        // harness that mixed them up would look correct on a fresh fleet — the
        // bug only appears once the daemon starts trimming, which is exactly
        // when a retention cap is worth checking.
        let budget = crate::retention::RetentionBudget::with_limit(u64::MAX);
        let mut output = OutputLog::new(budget);
        output.push(vec![0_u8; 1024]);
        assert_eq!(output.latest_output_bytes(), 1024);
        assert_eq!(output.retained_bytes() as u64, output.latest_output_bytes());

        // Push well past the 4 MiB per-Run cap so eviction has to run.
        for _ in 0..8 {
            output.push(vec![0_u8; 1024 * 1024]);
        }

        // The lifetime counter keeps every byte that ever passed through...
        assert_eq!(output.latest_output_bytes(), 1024 + 8 * 1024 * 1024);
        // ...while the retained count fell back under the per-Run cap.
        assert!(output.retained_bytes() <= OUTPUT_RETENTION_BYTES);
        // The gap is the whole point: reading the lifetime total as "memory
        // held" would report this Run at 8 MiB against a 4 MiB cap and fail a
        // healthy daemon.
        assert!(
            (output.retained_bytes() as u64) < output.latest_output_bytes(),
            "a trimmed log must retain strictly less than it has ever emitted"
        );
    }

    #[test]
    fn the_listing_row_reports_bytes_held_not_bytes_ever_emitted() {
        // The wire contract, asserted on a real Run through the same `summary()`
        // the List walk calls. The OutputLog-level test above proves the two
        // counters diverge; this proves the LISTING carries the right one.
        // Without it, wiring `latest_output_bytes()` into the retained field
        // passes every other test in the suite — an external harness would then
        // sum lifetime totals, read a healthy fleet as far over its retention
        // cap, and the defect would only surface as a false gate failure.
        let budget = crate::retention::RetentionBudget::with_limit(u64::MAX);
        let id = RunId::new();
        let native_runs = NativeRuntimeOwner::default();
        let run = Run::new_native_for_owner_test_with_budget(
            id,
            NativeControlOwner::new_for_wait_test(id, native_runs.owner_wake()),
            native_runs,
            NativeWaitFailure::default(),
            budget,
        );

        // Push well past the 4 MiB per-Run cap so the log has to evict.
        for _ in 0..128 {
            run.record_output(vec![0_u8; 64 * 1024]); // 8 MiB total
        }

        let summary = run.summary();
        let held = run.lock_owner(&run.output).retained_bytes() as u64;

        assert_eq!(
            summary.retained_output_bytes, held,
            "the listing row must carry the log's live retained count"
        );
        assert_eq!(summary.latest_output_bytes, 128 * 64 * 1024);
        assert!(
            summary.retained_output_bytes <= OUTPUT_RETENTION_BYTES as u64,
            "a listed Run must never report holding more than the per-Run cap"
        );
        assert!(
            summary.retained_output_bytes < summary.latest_output_bytes,
            "after eviction the two wire counters must not be equal; if they \
             are, the retained field is wired to the lifetime total"
        );
    }

    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "one dropped-offer fixture verifies both durable count and exact recovered bytes"
    )]
    fn record_output_survives_a_dropped_append_without_losing_bytes() {
        // `Run::record_output` is allowed to DROP an append when the persistence
        // actor is behind — that is what stops one slow fsync from stalling the
        // single daemon-wide output reader and, through it, every Run's pty.
        //
        // Dropping is only sound if what it sends is a catch-up from the
        // durable watermark rather than a delta of the current push. This test
        // makes a real append get dropped (the actor is held at a barrier while
        // the queue overflows) and then asserts the DURABLE bytes are still
        // whole. Send a delta instead and the dropped bytes never arrive:
        // `append_replay` sees a forward gap, rejects it, and `remember_failure`
        // latches durability off daemon-wide — exactly when the disk is slow.
        let directory = tempfile::tempdir().expect("create dropped-append run directory");
        let (persistence, _recovered) = Persistence::open(directory.path().join("state"))
            .expect("open dropped-append persistence");

        let run_id = RunId::new();
        let info = RunInfo {
            id: run_id,
            // A persistent native Run must carry its launch specification.
            spec: Some(RunSpec {
                program: "/bin/true".to_owned(),
                args: Vec::new(),
                cwd: None,
                env: BTreeMap::new(),
                initial_size: TerminalSize::default(),
                declared_inputs: Vec::new(),
            }),
            lineage: None,
            backend: RunBackend::Native,
            capabilities: RunCapabilities::NATIVE,
            pid: Some(42),
            state: RunState::Running,
            latest_output_bytes: 0,
            durable_output_bytes: Some(0),
            first_available_byte: 0,
            attachments: 0,
            applied_input_bytes: Some(0),
            current_size: Some(TerminalSize { cols: 80, rows: 24 }),
            native_service: None,
        };
        let operation_key =
            CreateOperationKey::new("dropped-append").expect("valid dropped-append key");
        let durable = persistence
            .insert_start(&operation_key, &info)
            .expect("seed the dropped-append row")
            .durable;

        let recovered = RecoveredRun {
            source_gap_after_byte: None,
            operation_key,
            info,
            replay: OutputReplay {
                chunks: Vec::new(),
                first_available_byte: 0,
                latest_output_bytes: 0,
                truncated: false,
            },
            metadata_bytes: 0,
        };
        let run = Run::recover(
            recovered,
            durable,
            16,
            TerminalPublicationOwner::default(),
            crate::qualification_stats::QualificationStats::default(),
            crate::retention::RetentionBudget::with_limit(u64::MAX),
        );

        // Hold the actor inside its first append so everything behind it is
        // dropped rather than queued.
        let (attachment_guard, mut live) = run.subscribe();
        let (reached, release) = persistence.pause_next_append();
        run.record_output(b"alpha".to_vec());
        assert_eq!(
            run.events.cursor().output_bytes,
            5,
            "persistent raw admission must also advance the public event owner"
        );
        let envelope = live
            .receiver
            .try_recv()
            .expect("subscribed raw event is published");
        assert!(
            matches!(envelope.event().as_ref(), RunEvent::Output { chunk }
            if chunk.start_byte == 0 && chunk.end_byte == 5 && chunk.data == b"alpha")
        );
        reached
            .recv()
            .expect("the actor reaches the append barrier");

        // These pushes cannot be queued: their appends are dropped on the
        // floor. Before this fix they would have blocked the output reader.
        for _ in 0..(2 * 1024) {
            run.record_output(b"x".to_vec());
        }
        run.record_output(b"omega".to_vec());

        release.send(()).expect("release the append barrier");

        // One more push after the actor drains. Its catch-up still starts at
        // the durable head, so it carries every byte whose own append was
        // dropped.
        run.record_output(b"tail".to_vec());
        // Flush through the durable handle rather than `publish_terminal`:
        // `Run::recover` already claimed this Run's terminal ordinal, and the
        // point under test is the durable byte stream, not terminal publication.
        let (expected, catch_up) = {
            let output = run.lock_owner(&run.output);
            (output.latest_output_bytes(), output.replay(0))
        };
        run.lock_owner(&run.persistence)
            .active()
            .expect("a recovered Run has active persistence")
            .finalize(
                run_id,
                42,
                catch_up,
                RunState::Exited {
                    code: 0,
                    signal: None,
                },
            );

        assert!(
            !persistence.is_failed(),
            "a dropped append must never latch persistence: a forward gap here \
             would kill durability for every Run in the fleet"
        );

        drop(live);
        drop(attachment_guard);
        drop(run);
        persistence.assert_exclusive_owner();
        drop(persistence);

        let (reopened, recovered) =
            Persistence::open(directory.path().join("state")).expect("reopen durable state");
        let row = recovered
            .iter()
            .find(|run| run.info.id == run_id)
            .expect("the Run is durable");
        assert_eq!(
            row.replay.latest_output_bytes, expected,
            "every byte must reach the disk despite thousands of dropped \
             appends; a short count means the offer was a delta, not a catch-up"
        );
        let mut expected_bytes = b"alpha".to_vec();
        expected_bytes.extend(std::iter::repeat_n(b'x', 2 * 1024));
        expected_bytes.extend_from_slice(b"omegatail");
        assert_eq!(
            row.replay
                .chunks
                .iter()
                .flat_map(|chunk| chunk.data.iter().copied())
                .collect::<Vec<_>>(),
            expected_bytes
        );
        drop(reopened);
    }

    #[test]
    fn a_steady_run_offers_only_new_bytes_instead_of_recopying_the_whole_log() {
        // The companion to the test above, guarding the OPPOSITE failure.
        //
        // Rendering the durable-watermark catch-up on EVERY push is safe but
        // ruinous: `OutputLog::replay` copies every retained chunk above the
        // start byte, so as soon as the actor lags at all, each push copies up
        // to `OUTPUT_RETENTION_BYTES` — inline, on the single daemon-wide thread
        // that reads every Run's pty. The lag then feeds itself. Measured on
        // a 512-Run fleet at 40 chunks/s that wedged admission at 211 Runs with
        // the reactor thread at 97.4% USER time, against 512/512 admitted for
        // the same load with persistence off.
        //
        // So this asserts the steady-state offer is the DELTA. The check is on
        // an observable effect — the bytes the persistence layer is actually
        // handed — not on a re-derivation of the expression under test, which
        // is what let this class of bug survive two earlier attempts.
        let directory = tempfile::tempdir().expect("create steady-append run directory");
        let (persistence, _recovered) =
            Persistence::open(directory.path().join("state")).expect("open steady-append state");

        let run_id = RunId::new();
        let info = RunInfo {
            id: run_id,
            spec: Some(RunSpec {
                program: "/bin/true".to_owned(),
                args: Vec::new(),
                cwd: None,
                env: BTreeMap::new(),
                initial_size: TerminalSize::default(),
                declared_inputs: Vec::new(),
            }),
            lineage: None,
            backend: RunBackend::Native,
            capabilities: RunCapabilities::NATIVE,
            pid: Some(42),
            state: RunState::Running,
            latest_output_bytes: 0,
            durable_output_bytes: Some(0),
            first_available_byte: 0,
            attachments: 0,
            applied_input_bytes: Some(0),
            current_size: Some(TerminalSize { cols: 80, rows: 24 }),
            native_service: None,
        };
        let operation_key =
            CreateOperationKey::new("steady-append").expect("valid steady-append key");
        let durable = persistence
            .insert_start(&operation_key, &info)
            .expect("seed the steady-append row")
            .durable;
        let run = Run::recover(
            RecoveredRun {
                source_gap_after_byte: None,
                operation_key,
                info,
                replay: OutputReplay {
                    chunks: Vec::new(),
                    first_available_byte: 0,
                    latest_output_bytes: 0,
                    truncated: false,
                },
                metadata_bytes: 0,
            },
            durable,
            16,
            TerminalPublicationOwner::default(),
            crate::qualification_stats::QualificationStats::default(),
            crate::retention::RetentionBudget::with_limit(u64::MAX),
        );

        // Hold the actor BEHIND for the whole test.
        //
        // This is the load-bearing setup, and getting it wrong is what made an
        // earlier version of this test useless: if the actor is allowed to
        // drain, `durable_head` equals the head and the catch-up render and the
        // delta render produce byte-identical offers. The bug then survives the
        // test by construction. The wedge only exists while the actor LAGS —
        // that is the whole shape of the positive feedback — so the test must
        // reproduce a lagging actor, not a drained one.
        //
        // The barrier stops the actor inside its first append, so `durable_head`
        // stays at 0 while the log grows. Every push after that renders against
        // a watermark far below the head, which is precisely when a catch-up
        // costs the entire retained log.
        let (reached, _release) = persistence.pause_next_append();

        // A recovered Run starts owing one catch-up (its log may hold bytes
        // above the recovered watermark), so spend that debt first. From here
        // on every append is accepted and nothing more is owed.
        run.record_output(b"prime".to_vec());
        reached
            .recv()
            .expect("the actor reaches the append barrier");

        // Build a log far larger than any single push. If the offer were the
        // catch-up, it would carry all of these bytes again.
        //
        // The log has to get big while the QUEUE stays short, and those pull in
        // opposite directions: a push is only rendered when `queue_has_room()`
        // (see `record_output`), so once the wedged actor's queue fills, every
        // later push — including the one this test observes — is correctly
        // skipped and offers nothing. Sizing this loop in units of
        // `PERSISTENCE_QUEUE_CAPACITY` rather than in a bare count is what keeps
        // that from happening silently: an earlier version pushed a hardcoded 64
        // chunks, which fit when the capacity was 1024 and stopped fitting when
        // R22 took it to 64. The test went red three rounds before anyone looked
        // at it, and read as a defect in the code rather than in its own
        // arithmetic.
        //
        // So: use half the queue, and make each chunk big enough that half a
        // queue still clears the quarter-megabyte the assertion below needs.
        let pushes = crate::persistence::PERSISTENCE_QUEUE_CAPACITY / 2;
        assert!(
            pushes >= 4,
            "the queue must hold at least a few appends for this fixture to \
             build a log without tripping the admission skip"
        );
        let filler = vec![b'f'; 512 * 1024 / pushes];
        for _ in 0..pushes {
            run.record_output(filler.clone());
        }

        // The actor is parked, so this is genuinely un-committed: a catch-up
        // from the durable head would copy all of it on every single push.
        let before_final_push = run.lock_owner(&run.output).latest_output_bytes();
        assert_eq!(
            run.lock_owner(&run.persistence)
                .active()
                .expect("a recovered Run has active persistence")
                .durable_head(),
            0,
            "the barrier must hold the actor behind; a drained actor makes the \
             delta and the catch-up identical and the assertion below vacuous"
        );
        assert!(
            before_final_push > 256 * 1024,
            "the log must be big enough that a whole-log recopy is unmistakable"
        );

        // Observe what the NEXT push actually hands to persistence.
        let observed = persistence.capture_next_append_payload();
        run.record_output(b"final".to_vec());
        let offered = observed.take().expect("the push offers exactly one append");

        assert_eq!(
            offered.first_byte,
            Some(before_final_push),
            "a steady Run must offer only the bytes it just produced; an offer \
             starting at the durable head means every push recopies the log"
        );
        assert_eq!(
            offered.payload_bytes,
            b"final".len(),
            "the steady-state offer must cost one chunk, not the retained log"
        );
    }

    #[test]
    fn an_overloaded_run_renders_no_replay_at_all_instead_of_one_it_will_discard() {
        // The half of #79 that bounding the render did NOT fix.
        //
        // Rendering is the expensive part of an append and the `try_send` that
        // rejects it comes AFTER. So under sustained overload — every send
        // refused — the reactor thread renders a replay for every push and
        // throws every one away. Bounding the render's size does not help here,
        // because a refusal leaves the offered watermark behind, so the very
        // next push renders everything outstanding again. Measured at 512 Runs
        // x 40 chunks/s, bounding alone moved admission from 199/512 to 325/512
        // and still stalled with the owner thread at 96.8% USER time.
        //
        // The invariant: when the queue is visibly full, do NO per-chunk work
        // proportional to the log. This asserts the observable effect — that
        // nothing is offered to persistence at all — rather than re-deriving
        // the predicate under test.
        let directory = tempfile::tempdir().expect("create overloaded run directory");
        let (persistence, _recovered) =
            Persistence::open(directory.path().join("state")).expect("open overloaded state");

        let run_id = RunId::new();
        let info = RunInfo {
            id: run_id,
            spec: Some(RunSpec {
                program: "/bin/true".to_owned(),
                args: Vec::new(),
                cwd: None,
                env: BTreeMap::new(),
                initial_size: TerminalSize::default(),
                declared_inputs: Vec::new(),
            }),
            lineage: None,
            backend: RunBackend::Native,
            capabilities: RunCapabilities::NATIVE,
            pid: Some(42),
            state: RunState::Running,
            latest_output_bytes: 0,
            durable_output_bytes: Some(0),
            first_available_byte: 0,
            attachments: 0,
            applied_input_bytes: Some(0),
            current_size: Some(TerminalSize { cols: 80, rows: 24 }),
            native_service: None,
        };
        let operation_key = CreateOperationKey::new("overloaded").expect("valid overloaded key");
        let durable = persistence
            .insert_start(&operation_key, &info)
            .expect("seed the overloaded row")
            .durable;
        let run = Run::recover(
            RecoveredRun {
                source_gap_after_byte: None,
                operation_key,
                info,
                replay: OutputReplay {
                    chunks: Vec::new(),
                    first_available_byte: 0,
                    latest_output_bytes: 0,
                    truncated: false,
                },
                metadata_bytes: 0,
            },
            durable,
            16,
            TerminalPublicationOwner::default(),
            crate::qualification_stats::QualificationStats::default(),
            crate::retention::RetentionBudget::with_limit(u64::MAX),
        );

        // Park the actor inside its first append, then fill the queue behind it.
        let (reached, _release) = persistence.pause_next_append();
        run.record_output(b"prime".to_vec());
        reached
            .recv()
            .expect("the actor reaches the append barrier");
        for _ in 0..(crate::persistence::PERSISTENCE_QUEUE_CAPACITY * 2) {
            run.record_output(b"flood".to_vec());
        }

        // With the queue saturated, the next push must not build anything.
        let observed = persistence.capture_next_append_payload();
        run.record_output(b"after saturation".to_vec());
        assert!(
            observed.take().is_none(),
            "an overloaded Run must skip the render entirely; offering anything \
             here means the reactor thread paid to build a message the full \
             queue was always going to reject"
        );

        // The bytes are not lost — the skip owes a catch-up exactly as a refusal
        // does, so what the Run holds still covers everything written.
        assert!(
            run.lock_owner(&run.output).latest_output_bytes() > 0,
            "the skip must not drop the Run's own retained bytes"
        );
    }

    #[test]
    fn a_run_that_leaves_running_mid_push_keeps_owing_its_catch_up() {
        // REGRESSION GUARD for a daemon-wide persistence latch that actually
        // happened: `durable replay gap: got 18451664, expected 17087786`,
        // reproduced end-to-end on a farm host with a chatty 32-Run fleet.
        //
        // `record_output` renders the replay under the `output` lock and only
        // then checks `running`. The original defect was that rendering had a
        // SIDE EFFECT: it consumed a `catch_up_owed` flag as a promise about the
        // replay the caller was about to offer. A refusal re-armed the flag and
        // a skip re-armed the flag, but render-then-discard re-armed nothing, so
        // the next push offered a delta starting at its OWN offset while the
        // actor's watermark still sat where the discarded replay began.
        // `append_replay` saw a forward gap, rejected it, and `remember_failure`
        // latched persistence off for EVERY Run in the daemon.
        //
        // The whole class is now structurally impossible: the watermark advances
        // only inside `append`, on acceptance, so a render that never reaches
        // the actor cannot move it and there is no debt to consume, drop, or
        // double-count. This still asserts the observable consequence -- that a
        // discarded render leaves the next offer covering the same bytes --
        // because the guarantee is what matters, not the mechanism that provides
        // it.
        let directory = tempfile::tempdir().expect("create mid-push run directory");
        let (persistence, _recovered) =
            Persistence::open(directory.path().join("state")).expect("open mid-push state");

        let run_id = RunId::new();
        let info = RunInfo {
            id: run_id,
            spec: Some(RunSpec {
                program: "/bin/true".to_owned(),
                args: Vec::new(),
                cwd: None,
                env: BTreeMap::new(),
                initial_size: TerminalSize::default(),
                declared_inputs: Vec::new(),
            }),
            lineage: None,
            backend: RunBackend::Native,
            capabilities: RunCapabilities::NATIVE,
            pid: Some(42),
            state: RunState::Running,
            latest_output_bytes: 0,
            durable_output_bytes: Some(0),
            first_available_byte: 0,
            attachments: 0,
            applied_input_bytes: Some(0),
            current_size: Some(TerminalSize { cols: 80, rows: 24 }),
            native_service: None,
        };
        let operation_key = CreateOperationKey::new("mid-push").expect("valid mid-push key");
        let durable = persistence
            .insert_start(&operation_key, &info)
            .expect("seed the mid-push row")
            .durable;
        let run = Run::recover(
            RecoveredRun {
                source_gap_after_byte: None,
                operation_key,
                info,
                replay: OutputReplay {
                    chunks: Vec::new(),
                    first_available_byte: 0,
                    latest_output_bytes: 0,
                    truncated: false,
                },
                metadata_bytes: 0,
            },
            durable,
            16,
            TerminalPublicationOwner::default(),
            crate::qualification_stats::QualificationStats::default(),
            crate::retention::RetentionBudget::with_limit(u64::MAX),
        );

        // One accepted push, so the Run is in steady state and owes nothing.
        run.record_output(b"first".to_vec());
        persistence.barrier().expect("the first append commits");

        // The Run exits. Its next push renders (the queue has room) and is then
        // discarded by the `running` check -- this is the moment the debt was
        // being lost.
        *mutex_lock(&run.state) = RunState::Exited {
            code: 0,
            signal: None,
        };
        run.record_output(b"written while leaving".to_vec());

        // Back to running, as a rebind would leave it, and push again. That
        // offer must carry the discarded bytes, which means starting at the
        // durable head rather than at this chunk's own offset.
        *mutex_lock(&run.state) = RunState::Running;
        let durable_head = run
            .lock_owner(&run.persistence)
            .active()
            .expect("the Run is still persistent")
            .durable_head();
        let observed = persistence.capture_next_append_payload();
        run.record_output(b"after".to_vec());
        let offered = observed.take().expect("the next push offers a replay");

        assert_eq!(
            offered.first_byte,
            Some(durable_head),
            "a push that was rendered and then discarded must leave the catch-up \
             owed; offering only the newest delta declares the discarded bytes \
             durable and `append_replay` rejects the gap, latching persistence \
             off for the whole daemon"
        );
    }

    #[test]
    fn the_listing_walk_never_holds_run_state_while_taking_run_output() {
        // REGRESSION GUARD for a daemon-wide wedge that actually happened.
        //
        // `summary()` once read its fields straight into the struct literal:
        //
        //     RunSummary {
        //         state: mutex_lock(&self.state).clone(),
        //         latest_output_bytes: self.lock_owner(&self.output).latest_output_bytes(),
        //     }
        //
        // Struct-literal fields evaluate in source order and their temporaries
        // live to the end of the statement, so the `state` guard was still held
        // when `output` was locked — a `state -> output` edge on the List path.
        // Every other path takes them the other way (`record_output`,
        // `publish_terminal`), so a List walking one Run while that Run's
        // terminal publication ran deadlocked both threads, and with them the
        // whole daemon: 5 threads in futex, none in epoll_wait, every new
        // client hanging on connect.
        //
        // Asserting "no deadlock" would be useless here — the ABBA needs a
        // precise interleaving and the buggy code passes such a test nearly
        // always. So assert the ORDERING PROPERTY that makes the cycle
        // impossible instead: while another thread holds `output`, a
        // `summary()` in flight must not be holding `state`. If someone
        // reintroduces the fused form, `summary()` grabs `state` first and then
        // blocks on `output` — and this test deadlocks instead of passing,
        // which is the loudest possible failure.
        let id = RunId::new();
        let native_runs = NativeRuntimeOwner::default();
        let run = Run::new_native_for_owner_test_with_budget(
            id,
            NativeControlOwner::new_for_wait_test(id, native_runs.owner_wake()),
            native_runs,
            NativeWaitFailure::default(),
            crate::retention::RetentionBudget::with_limit(u64::MAX),
        );
        run.record_output(vec![0_u8; 4096]);

        let output_guard = run.lock_owner(&run.output);

        let summarising = Arc::clone(&run);
        let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(0);
        let walker = std::thread::spawn(move || {
            entered_tx.send(()).expect("report the walk has started");
            summarising.summary()
        });
        entered_rx.recv().expect("the walk starts");

        // The walker is now inside `summary()` and must be blocked on `output`,
        // which this thread holds. The load-bearing assertion: `state` is free.
        // Under the fused form the walker would be holding it and this would
        // fail (or, once we then block, the whole test would hang).
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut observed_state_free = false;
        while Instant::now() < deadline {
            if run.state.try_lock().is_ok() {
                observed_state_free = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            observed_state_free,
            "a List walk blocked on `output` must not be holding `state`; \
             that pairing is the state -> output edge that wedged the daemon"
        );

        drop(output_guard);
        let summary = walker.join().expect("the walk completes once output frees");
        assert_eq!(summary.latest_output_bytes, 4096);
    }

    #[test]
    fn dropping_an_output_log_releases_its_bytes_from_the_total() {
        let budget = crate::retention::RetentionBudget::with_limit(u64::MAX);
        let mut output = OutputLog::new(budget.clone());
        output.push(vec![0_u8; 4096]);
        assert_eq!(budget.retained_total(), 4096);
        drop(output);
        assert_eq!(
            budget.retained_total(),
            0,
            "an OutputLog must return its bytes to the total when it drops"
        );
    }

    #[test]
    fn a_quiet_output_log_is_trimmed_by_aggregate_pressure() {
        // The bug a per-Run-only design misses. `quiet` fills to its 4 MiB cap
        // and then never pushes again; `busy` pushes until the shared total
        // exceeds the limit, and `busy`'s own record_output — the production
        // trigger, no manual call — reclaims from the idle `quiet` Run.
        //
        // Limit 5 MiB: one Run's 4 MiB cap fits, but the two together (8 MiB)
        // bust it, so pressure must cross Run boundaries to recover.
        let budget = crate::retention::RetentionBudget::with_limit(5 * 1024 * 1024);
        assert_eq!(budget.limit(), 5 * 1024 * 1024);

        let quiet_id = RunId::new();
        let quiet_native_runs = NativeRuntimeOwner::default();
        let quiet_run = Run::new_native_for_owner_test_with_budget(
            quiet_id,
            NativeControlOwner::new_for_wait_test(quiet_id, quiet_native_runs.owner_wake()),
            quiet_native_runs,
            NativeWaitFailure::default(),
            budget.clone(),
        );
        // Fill the quiet Run to its 4 MiB cap in fine chunks. Alone it stays
        // under the 5 MiB budget, so it does not trim itself.
        for _ in 0..64 {
            quiet_run.record_output(vec![0_u8; 64 * 1024]);
        }
        let filled = quiet_run.lock_owner(&quiet_run.output).retained_bytes();
        assert!(
            filled >= 3 * 1024 * 1024,
            "quiet Run should be near its cap"
        );

        // A second Run sharing the budget goes busy. Its record_output pushes
        // drive cross-Run reclamation with no manual trigger.
        let busy_id = RunId::new();
        let busy_native_runs = NativeRuntimeOwner::default();
        let busy_run = Run::new_native_for_owner_test_with_budget(
            busy_id,
            NativeControlOwner::new_for_wait_test(busy_id, busy_native_runs.owner_wake()),
            busy_native_runs,
            NativeWaitFailure::default(),
            budget.clone(),
        );
        for _ in 0..64 {
            busy_run.record_output(vec![0_u8; 64 * 1024]);
        }

        let after = quiet_run.lock_owner(&quiet_run.output).retained_bytes();
        assert!(
            after < filled,
            "the quiet Run pinned {after} bytes; aggregate pressure must trim it (was {filled})"
        );
        assert!(
            budget.retained_total() <= budget.limit(),
            "aggregate reclamation left {} bytes above the {} limit",
            budget.retained_total(),
            budget.limit()
        );
    }

    #[test]
    fn replay_cursor_and_retention_boundaries_are_exact() {
        // OR-002: exact retention and byte-cursor semantics, independent of
        // private packing boundaries. Keep the same full 4 MiB + 1 workload.
        let mut output = OutputLog::new(crate::retention::RetentionBudget::production());
        let first_size = OUTPUT_RETENTION_BYTES / 2;
        let mut expected = vec![b'a'; first_size];
        expected.extend(vec![b'b'; OUTPUT_RETENTION_BYTES - first_size]);
        output.push(expected[..first_size].to_vec());
        output.push(expected[first_size..].to_vec());
        let exact = output.replay(0);
        assert!(!exact.truncated);
        assert_eq!(exact.first_available_byte, 0);
        assert_eq!(exact.latest_output_bytes, OUTPUT_RETENTION_BYTES as u64);
        assert_eq!(
            exact
                .chunks
                .iter()
                .flat_map(|chunk| &chunk.data)
                .copied()
                .collect::<Vec<_>>(),
            expected
        );
        output.push(vec![b'c']);
        expected.push(b'c');
        let evicted = output.replay(0);
        assert!(evicted.truncated);
        assert_eq!(evicted.first_available_byte, 1);
        assert_eq!(
            evicted.latest_output_bytes,
            OUTPUT_RETENTION_BYTES as u64 + 1
        );
        assert_eq!(output.retained_bytes(), OUTPUT_RETENTION_BYTES);
        assert_eq!(
            evicted
                .chunks
                .iter()
                .flat_map(|chunk| &chunk.data)
                .copied()
                .collect::<Vec<_>>(),
            expected[1..]
        );
        assert!(
            evicted
                .chunks
                .windows(2)
                .all(|pair| pair[0].end_byte == pair[1].start_byte)
        );
        assert!(!output.replay(1).truncated);
        assert_eq!(output.replay(1).chunks, evicted.chunks);
        let tail = output.replay(OUTPUT_RETENTION_BYTES as u64);
        assert_eq!(tail.chunks[0].data, b"c");
        assert_eq!(tail.chunks[0].start_byte, OUTPUT_RETENTION_BYTES as u64);
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
        let mut output =
            OutputLog::with_initial_truncation(crate::retention::RetentionBudget::production());
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
    fn oversized_output_preserves_exact_tail_and_monotone_cursors() {
        let mut output = OutputLog::new(crate::retention::RetentionBudget::production());
        let oversized = vec![0xa5; OUTPUT_RETENTION_BYTES + 1];
        let live = output.push(oversized.clone());
        assert_eq!(live.data, oversized);
        let replay = output.replay(0);
        assert!(replay.truncated);
        assert_eq!(replay.first_available_byte, 1);
        assert_eq!(replay.latest_output_bytes, oversized.len() as u64);
        assert!(
            replay
                .chunks
                .iter()
                .flat_map(|chunk| &chunk.data)
                .copied()
                .eq(oversized[1..].iter().copied()),
            "packing boundaries cannot change the exact retained suffix"
        );
        assert_eq!(output.retained_bytes(), OUTPUT_RETENTION_BYTES);
        output.push(vec![0x5a]);
        let tail = output.replay(oversized.len() as u64);
        assert!(!tail.truncated);
        assert_eq!(tail.latest_output_bytes, oversized.len() as u64 + 1);
        assert_eq!(tail.chunks[0].data, vec![0x5a]);
    }

    #[test]
    fn tiny_prefix_trims_keep_exact_bytes_without_recopying_the_cache_block() {
        let size = super::REPLAY_CACHE_BLOCK_BYTES;
        let budget = crate::retention::RetentionBudget::with_limits((size * 2) as u64, size);
        let mut output = OutputLog::new(budget.clone());
        output.push(vec![b'a'; size]);
        let allocation = output.chunks.front().unwrap().data.as_ptr();
        for _ in 0..32 {
            output.push(vec![b'b']);
            assert_eq!(output.chunks.front().unwrap().data.as_ptr(), allocation);
        }
        let replay = output.replay(0);
        assert_eq!(replay.first_available_byte, 32);
        assert_eq!(output.retained_bytes(), size);
        assert_eq!(budget.retained_total(), size as u64);
        let bytes: Vec<_> = replay
            .chunks
            .iter()
            .flat_map(|chunk| &chunk.data)
            .copied()
            .collect();
        assert!(bytes[..size - 32].iter().all(|byte| *byte == b'a'));
        assert_eq!(&bytes[size - 32..], vec![b'b'; 32]);
    }

    #[tokio::test]
    async fn lag_recovery_replays_from_the_callers_cursor_without_loss_or_duplicates() {
        // LC-001 / OR-002: a live-ring lag does not replace the caller's
        // durable replay cursor with the daemon head.
        let (events, mut receiver) = broadcast::channel(2);
        let mut output = OutputLog::new(crate::retention::RetentionBudget::production());
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
            vec![(0, 4)]
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

    #[tokio::test]
    async fn broadcast_receivers_share_one_funded_payload_until_the_last_owner_drops() {
        let bytes = super::EVENT_ALLOCATION_BYTES + 8;
        let budget = crate::resources::ByteBudget::new(bytes as u64);
        let owner = super::LiveEventOwner::with_budget(2, budget.clone());
        let (sender, mut first) = tokio::sync::broadcast::channel(2);
        let mut second = sender.subscribe();
        mutex_lock(&owner.state).sender = Some(sender);
        owner.publish(RunEvent::Output {
            chunk: ctxmux_protocol::OutputChunk {
                start_byte: 0,
                end_byte: 8,
                data: b"12345678".to_vec(),
            },
        });
        let first = first.recv().await.unwrap();
        let second = second.recv().await.unwrap();
        assert!(
            matches!((&first.published, &second.published),
                (super::PublishedRunEvent::Funded(first), super::PublishedRunEvent::Funded(second))
                if Arc::ptr_eq(first, second)),
            "broadcast fanout must share payload and its lease"
        );
        assert!(budget.reserve(1).is_none());
        drop(first);
        assert!(budget.reserve(1).is_none());
        drop(second);
        assert!(
            budget.reserve(bytes).is_some(),
            "the last payload owner releases its bytes and envelope"
        );
    }

    #[tokio::test]
    async fn empty_event_envelopes_remain_funded_after_ring_eviction() {
        let bytes = super::EVENT_ALLOCATION_BYTES;
        let budget = crate::resources::ByteBudget::new(bytes as u64);
        let owner = super::LiveEventOwner::with_budget(1, budget.clone());
        let (sender, mut receiver) = tokio::sync::broadcast::channel(1);
        mutex_lock(&owner.state).sender = Some(sender);
        owner.publish(RunEvent::Resized {
            size: TerminalSize::default(),
            through_byte: 0,
            resize_revision: 1,
        });
        let held = receiver.recv().await.unwrap();
        assert!(matches!(
            held.published,
            super::PublishedRunEvent::Funded(_)
        ));
        for _ in 0..3 {
            owner.publish(RunEvent::Resized {
                size: TerminalSize::default(),
                through_byte: 0,
                resize_revision: 1,
            });
            let marker = receiver.recv().await.unwrap();
            assert!(matches!(
                marker.published,
                super::PublishedRunEvent::ObservationDiscontinuity
            ));
            assert!(matches!(
                marker.event().as_ref(),
                RunEvent::ObservationDiscontinuity
            ));
            assert!(
                budget.reserve(1).is_none(),
                "held envelope keeps its lease after eviction"
            );
        }
        drop(held);
        assert!(budget.reserve(bytes).is_some());
    }

    #[test]
    fn live_event_ring_exists_only_while_an_attachment_owns_it() {
        let id = RunId::new();
        let native_runs = NativeRuntimeOwner::default();
        let control = NativeControlOwner::new_for_wait_test(id, native_runs.owner_wake());
        let run =
            Run::new_native_for_owner_test(id, control, native_runs, NativeWaitFailure::default());
        assert!(mutex_lock(&run.events.state).sender.is_none());

        let (first, mut first_events) = run.subscribe();
        let (second, _second_events) = run.subscribe();
        assert_eq!(run.attachments.load(Ordering::Acquire), 2);
        assert!(mutex_lock(&run.events.state).sender.is_some());

        run.publish_event(RunEvent::Gap {
            latest_output_bytes: 7,
        });
        assert!(matches!(
            first_events
                .receiver
                .try_recv()
                .map(|envelope| envelope.event().into_owned()),
            Ok(RunEvent::Gap {
                latest_output_bytes: 7
            })
        ));

        drop(first);
        assert!(mutex_lock(&run.events.state).sender.is_some());
        drop(second);
        assert_eq!(run.attachments.load(Ordering::Acquire), 0);
        assert!(mutex_lock(&run.events.state).sender.is_none());
    }

    fn tmux_delivery_test_run(pane_pid: u32, live_event_capacity: usize) -> Arc<Run> {
        let (commands, _command_rx) = std::sync::mpsc::channel();
        let (_completion_tx, completion) = std::sync::mpsc::channel::<Result<(), String>>();
        Arc::new(Run {
            id: RunId::new(),
            spec: None,
            lineage: None,
            backend: RunBackend::Tmux {
                socket_path: "/tmp/ctxmux-observation-lag.sock".to_owned(),
                server_pid: pane_pid,
                server_started_at: 1,
                session_id: "$1".to_owned(),
                window_id: "@1".to_owned(),
                pane_id: "%1".to_owned(),
                tmux_version: "3.4".to_owned(),
            },
            capabilities: RunCapabilities::TMUX_READ_ONLY,
            pid: Some(pane_pid),
            state: Mutex::new(RunState::Running),
            output: crate::native_output::OutputOwner::new(OutputLog::with_initial_truncation(
                crate::retention::RetentionBudget::production(),
            )),
            incarnation_control: Some(super::RunControl::Tmux(TmuxRunControl {
                writer: Mutex::new(None),
                commands,
                completion: Mutex::new(TmuxCompletion::Pending(completion)),
            })),
            native_runs: None,
            native_service: None,
            persistence_mode: PersistenceMode::MemoryOnly,
            owner_deferred: AtomicBool::new(false),
            output_unlocked: Notify::new(),
            persistence_unlocked: Notify::new(),
            persistence_transition: Mutex::new(()),
            durable_output_head: std::sync::OnceLock::new(),
            persistence: Mutex::new(PersistenceBinding::Disabled),
            attachments: AtomicUsize::new(0),
            qualification_stats: crate::qualification_stats::QualificationStats::default(),
            terminal_publications: TerminalPublicationOwner::default(),
            terminal_ordinal: std::sync::OnceLock::new(),
            live_permit: Mutex::new(None),
            terminal_visible: Notify::new(),
            events: super::LiveEventOwner::new(live_event_capacity),
            retention_budget: crate::retention::RetentionBudget::production(),
        })
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mixed_tmux_lag_separates_output_gap_from_observation_discontinuity() {
        let (server, hook, mut reached) = hooked_server(AttachmentHookPoint::AfterSnapshot);
        let mut pane = Command::new("/bin/sh")
            .args(["-c", "sleep 30"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn tmux-owned pane sentinel");
        let pane_pid = pane.id();
        let run = tmux_delivery_test_run(pane_pid, 2);
        server
            .manager
            .registry
            .publish_unkeyed_for_test(Arc::clone(&run));

        let (attachment, initial) = server
            .client
            .attach(run.id, 0)
            .await
            .expect("attach before mixed lag");
        assert!(initial.replay.chunks.is_empty());
        assert!(initial.replay.truncated);
        tokio::time::timeout(Duration::from_secs(5), reached.recv())
            .await
            .expect("attachment reaches snapshot barrier")
            .expect("attachment barrier remains connected");

        run.record_output(b"a".to_vec());
        run.publish_event(RunEvent::Tmux {
            event: TmuxRunEvent::SessionRenamed {
                name: b"renamed".to_vec(),
            },
        });
        run.record_output(b"b".to_vec());
        run.publish_event(RunEvent::Tmux {
            event: TmuxRunEvent::Paused,
        });
        let gap_head = run.mark_output_source_gap();
        run.publish_event(RunEvent::Gap {
            latest_output_bytes: gap_head,
        });
        run.publish_event(RunEvent::Tmux {
            event: TmuxRunEvent::Continued,
        });
        run.publish_interrupted(InterruptionReason::TmuxServerUnavailable);
        run.record_output(b"c".to_vec());
        run.record_output(b"d".to_vec());
        hook.release.notify_one();

        assert_eq!(
            next_event_before_timeout(&attachment).await,
            Some(RunEvent::ObservationDiscontinuity)
        );
        assert_eq!(
            next_event_before_timeout(&attachment).await,
            Some(RunEvent::Interrupted {
                reason: InterruptionReason::TmuxServerUnavailable,
            })
        );
        assert_eq!(next_event_before_timeout(&attachment).await, None);

        let (late, replay) = server
            .client
            .attach(run.id, 0)
            .await
            .expect("reattach after source discontinuity");
        assert!(replay.replay.truncated);
        assert_eq!(replay_bytes(&replay.replay.chunks), b"abcd");
        assert_eq!(
            next_event_before_timeout(&late).await,
            Some(RunEvent::Interrupted {
                reason: InterruptionReason::TmuxServerUnavailable,
            })
        );
        assert_eq!(next_event_before_timeout(&late).await, None);
        assert!(
            process_exists(pane_pid),
            "ctxmux observation failure must not terminate the tmux-owned pane"
        );

        pane.kill().expect("terminate pane sentinel after proof");
        pane.wait().expect("reap pane sentinel after proof");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn output_only_lag_preserves_retained_tmux_observation_before_terminal() {
        let (server, hook, mut reached) = hooked_server(AttachmentHookPoint::AfterSnapshot);
        let mut pane = Command::new("/bin/sh")
            .args(["-c", "sleep 30"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn retained-observation pane sentinel");
        let pane_pid = pane.id();
        let run = tmux_delivery_test_run(pane_pid, 3);
        server
            .manager
            .registry
            .publish_unkeyed_for_test(Arc::clone(&run));
        let (attachment, _) = server
            .client
            .attach(run.id, 0)
            .await
            .expect("attach before output-only lag");
        tokio::time::timeout(Duration::from_secs(5), reached.recv())
            .await
            .expect("attachment reaches retained-observation barrier")
            .expect("retained-observation barrier remains connected");

        for byte in b"abcd" {
            run.record_output(vec![*byte]);
        }
        run.publish_event(RunEvent::Tmux {
            event: TmuxRunEvent::SessionRenamed {
                name: b"retained".to_vec(),
            },
        });
        run.publish_interrupted(InterruptionReason::TmuxServerUnavailable);
        hook.release.notify_one();

        assert_eq!(
            next_event_before_timeout(&attachment).await,
            Some(RunEvent::Gap {
                latest_output_bytes: 4,
            })
        );
        assert_eq!(
            next_event_before_timeout(&attachment).await,
            Some(RunEvent::Tmux {
                event: TmuxRunEvent::SessionRenamed {
                    name: b"retained".to_vec(),
                },
            })
        );
        assert_eq!(
            next_event_before_timeout(&attachment).await,
            Some(RunEvent::Interrupted {
                reason: InterruptionReason::TmuxServerUnavailable,
            })
        );
        assert_eq!(next_event_before_timeout(&attachment).await, None);
        assert!(process_exists(pane_pid));
        pane.kill()
            .expect("terminate retained-observation sentinel");
        pane.wait().expect("reap retained-observation sentinel");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dropped_terminal_is_resnapshotted_once_after_late_output() {
        let (server, hook, mut reached) = hooked_server(AttachmentHookPoint::AfterSnapshot);
        let mut pane = Command::new("/bin/sh")
            .args(["-c", "sleep 30"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn terminal-resnapshot pane sentinel");
        let pane_pid = pane.id();
        let run = tmux_delivery_test_run(pane_pid, 1);
        server
            .manager
            .registry
            .publish_unkeyed_for_test(Arc::clone(&run));
        let (attachment, _) = server
            .client
            .attach(run.id, 0)
            .await
            .expect("attach before terminal overwrite");
        tokio::time::timeout(Duration::from_secs(5), reached.recv())
            .await
            .expect("attachment reaches terminal-overwrite barrier")
            .expect("terminal-overwrite barrier remains connected");

        run.publish_interrupted(InterruptionReason::TmuxServerUnavailable);
        run.record_output(b"a".to_vec());
        run.record_output(b"b".to_vec());
        hook.release.notify_one();

        assert_eq!(
            next_event_before_timeout(&attachment).await,
            Some(RunEvent::Gap {
                latest_output_bytes: 2,
            })
        );
        assert_eq!(
            next_event_before_timeout(&attachment).await,
            Some(RunEvent::Interrupted {
                reason: InterruptionReason::TmuxServerUnavailable,
            })
        );
        assert_eq!(next_event_before_timeout(&attachment).await, None);

        let (late, replay) = server
            .client
            .attach(run.id, 0)
            .await
            .expect("reattach after terminal resnapshot");
        assert_eq!(replay_bytes(&replay.replay.chunks), b"ab");
        assert_eq!(
            next_event_before_timeout(&late).await,
            Some(RunEvent::Interrupted {
                reason: InterruptionReason::TmuxServerUnavailable,
            })
        );
        assert_eq!(next_event_before_timeout(&late).await, None);
        assert!(process_exists(pane_pid));
        pane.kill().expect("terminate terminal-resnapshot sentinel");
        pane.wait().expect("reap terminal-resnapshot sentinel");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn terminal_snapshot_marks_tmux_observation_from_the_join_window() {
        let (server, hook, mut reached) = hooked_server(AttachmentHookPoint::AfterSubscribe);
        let mut pane = Command::new("/bin/sh")
            .args(["-c", "sleep 30"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn join-window pane sentinel");
        let pane_pid = pane.id();
        let run = tmux_delivery_test_run(pane_pid, 2);
        server
            .manager
            .registry
            .publish_unkeyed_for_test(Arc::clone(&run));
        let client = server.client.clone();
        let run_id = run.id;
        let attaching = tokio::spawn(async move { client.attach(run_id, 0).await });
        tokio::time::timeout(Duration::from_secs(5), reached.recv())
            .await
            .expect("attachment reaches subscribe/snapshot join barrier")
            .expect("join barrier remains connected");

        run.publish_event(RunEvent::Tmux {
            event: TmuxRunEvent::SessionRenamed {
                name: b"between".to_vec(),
            },
        });
        run.record_output(b"x".to_vec());
        let source_gap_head = run.mark_output_source_gap();
        run.publish_event(RunEvent::Gap {
            latest_output_bytes: source_gap_head,
        });
        run.publish_interrupted(InterruptionReason::TmuxServerUnavailable);
        hook.release.notify_one();

        let (attachment, snapshot) = attaching
            .await
            .expect("join-window attachment task completes")
            .expect("join-window attachment succeeds");
        assert!(!snapshot.run.state.is_running());
        assert!(snapshot.replay.truncated);
        assert_eq!(replay_bytes(&snapshot.replay.chunks), b"x");
        assert_eq!(
            next_event_before_timeout(&attachment).await,
            Some(RunEvent::ObservationDiscontinuity)
        );
        assert_eq!(
            next_event_before_timeout(&attachment).await,
            Some(RunEvent::Interrupted {
                reason: InterruptionReason::TmuxServerUnavailable,
            })
        );
        assert_eq!(next_event_before_timeout(&attachment).await, None);
        assert!(process_exists(pane_pid));
        pane.kill().expect("terminate join-window sentinel");
        pane.wait().expect("reap join-window sentinel");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn terminal_snapshot_marks_output_and_source_gap_after_replay() {
        let (server, hook, mut reached) = hooked_server(AttachmentHookPoint::AfterSnapshot);
        let mut pane = Command::new("/bin/sh")
            .args(["-c", "sleep 30"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn terminal-join output sentinel");
        let pane_pid = pane.id();
        let run = tmux_delivery_test_run(pane_pid, 2);
        run.publish_interrupted(InterruptionReason::TmuxServerUnavailable);
        server
            .manager
            .registry
            .publish_unkeyed_for_test(Arc::clone(&run));

        let (attachment, snapshot) = server
            .client
            .attach(run.id, 0)
            .await
            .expect("attach to terminal snapshot before late output");
        assert!(snapshot.replay.chunks.is_empty());
        tokio::time::timeout(Duration::from_secs(5), reached.recv())
            .await
            .expect("attachment reaches post-replay terminal barrier")
            .expect("post-replay terminal barrier remains connected");
        run.record_output(b"x".to_vec());
        let source_gap_head = run.mark_output_source_gap();
        run.publish_event(RunEvent::Gap {
            latest_output_bytes: source_gap_head,
        });
        hook.release.notify_one();

        assert_eq!(
            next_event_before_timeout(&attachment).await,
            Some(RunEvent::Gap {
                latest_output_bytes: 1,
            })
        );
        assert_eq!(
            next_event_before_timeout(&attachment).await,
            Some(RunEvent::Interrupted {
                reason: InterruptionReason::TmuxServerUnavailable,
            })
        );
        assert_eq!(next_event_before_timeout(&attachment).await, None);

        let (late, replay) = server
            .client
            .attach(run.id, 0)
            .await
            .expect("reattach after post-snapshot output Gap");
        assert!(replay.replay.truncated);
        assert_eq!(replay_bytes(&replay.replay.chunks), b"x");
        assert_eq!(
            next_event_before_timeout(&late).await,
            Some(RunEvent::Interrupted {
                reason: InterruptionReason::TmuxServerUnavailable,
            })
        );
        assert_eq!(next_event_before_timeout(&late).await, None);
        assert!(process_exists(pane_pid));
        pane.kill()
            .expect("terminate terminal-join output sentinel");
        pane.wait().expect("reap terminal-join output sentinel");
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
                initial_size: TerminalSize::default(),
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
                let recorded_bytes = recorded_run
                    .lock_owner(&recorded_run.output)
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
        let stop_operation = fresh_stop(&client, run.id).await;
        client
            .stop(stop_operation)
            .await
            .expect("stop controlled Gap Run");
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

    #[test]
    fn accept_errors_are_classified_transient_or_fatal() {
        use rustix::io::Errno;

        // Resource pressure and interruptions leave the listener intact: the
        // daemon must ride these out, mirroring the openpty→SpawnFailed path
        // that already survives descriptor exhaustion on spawn.
        for transient in [
            Errno::MFILE,
            Errno::NFILE,
            Errno::NOBUFS,
            Errno::NOMEM,
            Errno::CONNABORTED,
            Errno::INTR,
        ] {
            let error = io::Error::from_raw_os_error(transient.raw_os_error());
            assert!(
                !super::accept_error_is_fatal(&error),
                "{transient:?} must be treated as transient"
            );
        }

        // A dead or invalid listener has no connection to serve: fail-stop.
        for fatal in [Errno::BADF, Errno::INVAL, Errno::NOTSOCK] {
            let error = io::Error::from_raw_os_error(fatal.raw_os_error());
            assert!(
                super::accept_error_is_fatal(&error),
                "{fatal:?} must be treated as fatal"
            );
        }

        // An error the OS did not attach an errno to cannot be proven
        // recoverable, so it fails closed.
        assert!(
            super::accept_error_is_fatal(&io::Error::other("no errno")),
            "an errno-less accept error must be treated as fatal"
        );
    }
}
