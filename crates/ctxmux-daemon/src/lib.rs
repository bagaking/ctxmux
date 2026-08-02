//! Long-lived native Run owner and local protocol server.

#[cfg(not(unix))]
compile_error!("the first ctxmux native transport currently requires Unix sockets");

use std::{
    collections::VecDeque,
    fs,
    io::{self, Read, Write},
    os::unix::{
        fs::{FileTypeExt, MetadataExt, PermissionsExt},
        net::UnixStream as StdUnixStream,
    },
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, OnceLock, RwLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

mod attachment;
mod creation;
mod native_control;
mod persistence;
mod run_spec;
mod tmux;

pub use persistence::PersistenceError;

use ctxmux_protocol::{
    AttachedSnapshot, ClientFrame, CommandDisposition, ControlFailure, CreateOperationKey,
    ErrorCode, ForkFidelity, ForkPlan, InterruptionReason, MAX_FRAME_BYTES, OutputChunk,
    OutputReplay, PROTOCOL_VERSION, ProtocolError, Request, Response, RunBackend, RunCapabilities,
    RunEvent, RunId, RunInfo, RunLineage, RunSpec, RunState, ServerFrame, TerminalSize,
    TmuxRunEvent, decode_frame, encode_frame,
};
use futures_util::{SinkExt, StreamExt};
use portable_pty::{Child, CommandBuilder, PtySize, native_pty_system};
use run_spec::{validate_run_spec, validate_terminal_size};
use thiserror::Error;
use tokio::{
    net::{UnixListener, UnixStream},
    sync::broadcast,
};
use tokio_util::codec::{Framed, LinesCodec, LinesCodecError};

use crate::creation::{
    CreationFlightOwner, CreationRequest, PendingPublication, RunRegistry, TerminalOrdinal,
    TerminalPublicationOwner, UnpublishedCleanupOwner, UnpublishedCleanupReservation,
};
use crate::native_control::{
    ChildCommand, ControlResult, InputDrainGate, NativeControlOwner, PendingInput, PendingStop,
};
use crate::persistence::{Persistence, PersistentRun, RecoveredRun};
use crate::tmux::{
    BoundedLineRead, ControlItem, ControlParser, SocketIdentity as TmuxSocketIdentity,
};

const OUTPUT_RETENTION_BYTES: usize = 4 * 1024 * 1024;
const OUTPUT_READ_BUFFER_BYTES: usize = 8192;
const LIVE_EVENT_CAPACITY: usize = 256;
const CHILD_CONTROL_POLL: Duration = Duration::from_millis(20);
const STOP_ACK_TIMEOUT: Duration = Duration::from_secs(1);
const UNPUBLISHED_REAP_INLINE_TIMEOUT: Duration = Duration::from_millis(25);
const TMUX_OUTPUT_DRAIN_TIMEOUT: Duration = Duration::from_secs(1);
const TMUX_FAILED_IMPORT_CLEANUP_TIMEOUT: Duration = Duration::from_secs(2);
const TMUX_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(4);
const TMUX_IMPORT_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(3);
const TMUX_IMPORT_PREPARE_TIMEOUT: Duration = Duration::from_secs(5);
const TMUX_IMPORT_TOTAL_TIMEOUT: Duration = Duration::from_secs(7);
const TMUX_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(8);

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
    serve_with_persistence(socket_path.into(), None).await
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
    let (persistence, recovered) = Persistence::open(state_dir)?;
    let manager = Arc::new(RunManager::persistent(persistence, recovered));
    serve_with_persistence_manager(socket_path.into(), manager).await
}

async fn serve_with_persistence(
    socket_path: PathBuf,
    persistence: Option<(Persistence, Vec<RecoveredRun>)>,
) -> Result<(), ServerError> {
    let manager = match persistence {
        Some((persistence, recovered)) => Arc::new(RunManager::persistent(persistence, recovered)),
        None => Arc::new(RunManager::default()),
    };
    serve_with_persistence_manager(socket_path, manager).await
}

async fn serve_with_persistence_manager(
    socket_path: PathBuf,
    manager: Arc<RunManager>,
) -> Result<(), ServerError> {
    prepare_socket_path(&socket_path)?;
    let listener =
        UnixListener::bind(&socket_path).map_err(|source| ServerError::io(&socket_path, source))?;
    fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600))
        .map_err(|source| ServerError::io(&socket_path, source))?;
    serve_with_manager(socket_path, listener, manager).await
}

async fn serve_with_manager(
    socket_path: PathBuf,
    listener: UnixListener,
    manager: Arc<RunManager>,
) -> Result<(), ServerError> {
    let _socket_guard = SocketGuard::new(socket_path.clone())?;

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
                return Ok(());
            }
        }
    }
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
    registry: RunRegistry,
    creation_flights: CreationFlightOwner,
    unpublished_cleanups: UnpublishedCleanupOwner,
    terminal_publications: TerminalPublicationOwner,
    native_input_drains: InputDrainGate,
    live_event_capacity: usize,
    persistence: Option<Persistence>,
    tmux_shutting_down: AtomicBool,
    tmux_operation_gate: RwLock<()>,
    #[cfg(test)]
    attachment_hook: Option<Arc<AttachmentTestHook>>,
    #[cfg(test)]
    creation_hook: Option<Arc<CreationTestHook>>,
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
    reached: tokio::sync::mpsc::UnboundedSender<()>,
    released: Mutex<bool>,
    release: std::sync::Condvar,
    captured_run: Mutex<Option<Arc<Run>>>,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CreationHookPoint {
    AfterSpawn,
    PanicAfterSpawn,
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
    fn capture_run(&self, point: CreationHookPoint, run: Arc<Run>) {
        if self.point == point && self.armed.load(Ordering::Acquire) {
            let previous = mutex_lock(&self.captured_run).replace(run);
            assert!(previous.is_none(), "test hook captures one Run owner");
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
    }

    fn release(&self) {
        *mutex_lock(&self.released) = true;
        self.release.notify_one();
    }

    fn arm(&self) {
        *mutex_lock(&self.released) = false;
        self.armed.store(true, Ordering::Release);
    }

    fn release_captured_run(&self) {
        mutex_lock(&self.captured_run).take();
    }
}

impl Default for RunManager {
    fn default() -> Self {
        Self {
            registry: RunRegistry::default(),
            creation_flights: CreationFlightOwner::default(),
            unpublished_cleanups: UnpublishedCleanupOwner::default(),
            terminal_publications: TerminalPublicationOwner::default(),
            native_input_drains: InputDrainGate::default(),
            live_event_capacity: LIVE_EVENT_CAPACITY,
            persistence: None,
            tmux_shutting_down: AtomicBool::new(false),
            tmux_operation_gate: RwLock::new(()),
            #[cfg(test)]
            attachment_hook: None,
            #[cfg(test)]
            creation_hook: None,
        }
    }
}

impl RunManager {
    fn persistent(persistence: Persistence, recovered: Vec<RecoveredRun>) -> Self {
        let terminal_publications = TerminalPublicationOwner::default();
        let runs = recovered
            .into_iter()
            .map(|recovered| {
                let operation_key = recovered.operation_key.clone();
                let durable = persistence.recovered_run(
                    recovered
                        .info
                        .durable_head_seq
                        .unwrap_or(recovered.info.head_seq),
                );
                (
                    operation_key,
                    Run::recover(
                        recovered,
                        durable,
                        LIVE_EVENT_CAPACITY,
                        terminal_publications.clone(),
                    ),
                )
            })
            .collect();
        Self {
            registry: RunRegistry::recovered(runs),
            creation_flights: CreationFlightOwner::default(),
            unpublished_cleanups: UnpublishedCleanupOwner::default(),
            terminal_publications,
            native_input_drains: InputDrainGate::default(),
            live_event_capacity: LIVE_EVENT_CAPACITY,
            persistence: Some(persistence),
            tmux_shutting_down: AtomicBool::new(false),
            tmux_operation_gate: RwLock::new(()),
            #[cfg(test)]
            attachment_hook: None,
            #[cfg(test)]
            creation_hook: None,
        }
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
        let flight = self.creation_flights.try_begin(admission).ok_or_else(|| {
            ProtocolError::new(
                ErrorCode::BackendUnavailable,
                "ctxmux daemon is shutting down",
            )
        })?;
        let cleanup_reservation = self.unpublished_cleanups.reserve(&operation_key)?;
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let manager = Arc::clone(self);
        thread::Builder::new()
            .name("ctxmux-create".to_owned())
            .spawn(move || {
                let _flight = flight;
                let _operation_guard = operation_guard;
                let result = manager.create_unique(operation_key, request, cleanup_reservation);
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

    fn create_unique(
        &self,
        operation_key: CreateOperationKey,
        request: CreationRequest,
        cleanup_reservation: UnpublishedCleanupReservation,
    ) -> Result<RunInfo, ProtocolError> {
        let persistence_mode = self.persistence_mode();
        let (spec, lineage) = match request.clone() {
            CreationRequest::Start { spec } => (spec, None),
            CreationRequest::Fork { parent, plan } => {
                let parent_run = self.pin(parent)?;
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
                        (spec, ForkFidelity::LevelB)
                    }
                    ForkPlan::LevelA | ForkPlan::LevelB { .. } => {
                        return Err(ProtocolError::new(
                            ErrorCode::UnsupportedCapability,
                            format!("Run {parent} backend does not support the requested fork"),
                        ));
                    }
                };
                (spec, Some(RunLineage { parent, fidelity }))
            }
        };
        let pending = Run::spawn_pending(
            NativeSpawnConfig {
                spec,
                lineage,
                persistence_mode,
                live_event_capacity: self.live_event_capacity,
                input_drains: self.native_input_drains.clone(),
                terminal_publications: self.terminal_publications.clone(),
            },
            request,
            cleanup_reservation,
        )?;
        let (info, post_commit_error) = if persistence_mode == PersistenceMode::MemoryOnly {
            #[cfg(test)]
            if let Some(hook) = &self.creation_hook {
                hook.pause_once(CreationHookPoint::AfterSpawn);
                hook.capture_run(
                    CreationHookPoint::PanicAfterSpawn,
                    Arc::clone(pending.run()),
                );
                hook.pause_once(CreationHookPoint::PanicAfterSpawn);
            }
            (self.registry.publish_creation(operation_key, pending), None)
        } else {
            #[cfg(test)]
            if let Some(hook) = &self.creation_hook {
                hook.pause_once(CreationHookPoint::AfterSpawn);
            }
            let post_commit_error = match self.prepare_publication(&operation_key, pending.run()) {
                Ok(post_commit_error) => post_commit_error,
                Err(error) => {
                    if let Err(cleanup_error) = pending.cleanup_unpublished() {
                        return Err(ProtocolError::new(
                            error.code,
                            format!(
                                "{}; rollback pending: exact creation key remains fenced until all unpublished native owners are quiescent: {cleanup_error}",
                                error.message
                            ),
                        ));
                    }
                    return Err(error);
                }
            };
            (
                self.registry.publish_creation(operation_key, pending),
                post_commit_error,
            )
        };
        #[cfg(test)]
        if let Some(hook) = &self.creation_hook {
            hook.pause_once(CreationHookPoint::AfterPublication);
        }
        match post_commit_error {
            Some(error) => Err(error),
            None => Ok(info),
        }
    }

    #[cfg(test)]
    fn start(&self, spec: RunSpec) -> Result<RunInfo, ProtocolError> {
        let operation_key = CreateOperationKey::random();
        let request = CreationRequest::Start { spec };
        self.unpublished_cleanups
            .resolve_fence(&operation_key, &request)?;
        let cleanup_reservation = self.unpublished_cleanups.reserve(&operation_key)?;
        self.create_unique(operation_key, request, cleanup_reservation)
    }

    #[cfg(test)]
    fn fork(&self, parent: RunId, plan: ForkPlan) -> Result<RunInfo, ProtocolError> {
        let operation_key = CreateOperationKey::random();
        let request = CreationRequest::Fork { parent, plan };
        self.unpublished_cleanups
            .resolve_fence(&operation_key, &request)?;
        let cleanup_reservation = self.unpublished_cleanups.reserve(&operation_key)?;
        self.create_unique(operation_key, request, cleanup_reservation)
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

    fn import_tmux(&self, socket_path: &str, pane_id: &str) -> Result<RunInfo, ProtocolError> {
        if self.persistence.is_some() {
            return Err(ProtocolError::new(
                ErrorCode::UnsupportedCapability,
                "tmux pane import is not persisted; use a memory-only ctxmux daemon",
            ));
        }
        self.with_tmux_operation(|| {
            let started_at = Instant::now();
            let run = Run::import_tmux(
                socket_path,
                pane_id,
                self.live_event_capacity,
                self.terminal_publications.clone(),
                started_at + TMUX_IMPORT_DISCOVERY_TIMEOUT,
                started_at + TMUX_IMPORT_PREPARE_TIMEOUT,
                started_at + TMUX_IMPORT_TOTAL_TIMEOUT,
            )?;
            let info = run.info();
            self.registry.publish_unkeyed(run);
            Ok(info)
        })
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
                let completion = mutex_lock(&control.completion).try_recv();
                match completion {
                    Ok(Ok(())) => {
                        pending.swap_remove(index);
                    }
                    Ok(Err(error)) => {
                        failures.push(format!("Run {}: {error}", run.id));
                        pending.swap_remove(index);
                    }
                    Err(mpsc::TryRecvError::Disconnected) => {
                        failures.push(format!(
                            "Run {}: tmux control waiter ended without a completion receipt",
                            run.id
                        ));
                        pending.swap_remove(index);
                    }
                    Err(mpsc::TryRecvError::Empty) => {
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
                .map(|failure| format!("unpublished child cleanup {failure}")),
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
        self.registry.pin(id).ok_or_else(|| {
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

    fn prepare_publication(
        &self,
        operation_key: &CreateOperationKey,
        run: &Arc<Run>,
    ) -> Result<Option<ProtocolError>, ProtocolError> {
        let Some(persistence) = &self.persistence else {
            return Ok(None);
        };
        match persistence.insert_start(operation_key, &run.persistence_start_info()) {
            Ok(committed) => {
                run.enable_persistence(&committed.durable);
                Ok(committed
                    .post_commit_error
                    .map(|error| ProtocolError::new(ErrorCode::Persistence, error.to_string())))
            }
            Err(error) => Err(ProtocolError::new(
                ErrorCode::Persistence,
                error.to_string(),
            )),
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
        let pending = Run::spawn_pending_with_setup(
            NativeSpawnConfig {
                spec,
                lineage: None,
                persistence_mode: self.persistence_mode(),
                live_event_capacity: LIVE_EVENT_CAPACITY,
                input_drains: InputDrainGate::default(),
                terminal_publications: self.terminal_publications.clone(),
            },
            request,
            cleanup_reservation,
            captured_run,
            setup,
        )?;
        Ok(self.registry.publish_creation(operation_key, pending))
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
        let run = Run::spawn_with_wait_hook_owner(
            spec,
            self.persistence_mode(),
            self.terminal_publications.clone(),
            after_wait,
        )?;
        let info = run.info();
        self.registry.publish_unkeyed(run);
        Ok(info)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LaunchSetupStep {
    CloneReader,
    TakeWriter,
    StartOutputThread,
    StartWaiterThread,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PersistenceMode {
    MemoryOnly,
    PersistentCapable,
}

struct NativeSpawnConfig {
    spec: RunSpec,
    lineage: Option<RunLineage>,
    persistence_mode: PersistenceMode,
    live_event_capacity: usize,
    input_drains: InputDrainGate,
    terminal_publications: TerminalPublicationOwner,
}

impl NativeSpawnConfig {
    fn command(&self) -> CommandBuilder {
        let mut command = CommandBuilder::new(&self.spec.program);
        command.args(&self.spec.args);
        if let Some(cwd) = &self.spec.cwd {
            command.cwd(cwd);
        }
        for (name, value) in &self.spec.env {
            command.env(name, value);
        }
        command
    }
}

struct PendingChild {
    child: Option<Box<dyn Child + Send + Sync>>,
    reap_control: Option<NativeControlOwner>,
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
    persistence_mode: PersistenceMode,
    persistence_transition: Mutex<()>,
    persistence: Mutex<Option<PersistentRun>>,
    attachments: AtomicUsize,
    terminal_publications: TerminalPublicationOwner,
    terminal_ordinal: OnceLock<TerminalOrdinal>,
    events: broadcast::Sender<RunEvent>,
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
    completion: Mutex<mpsc::Receiver<Result<(), String>>>,
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
    fn spawn(
        spec: RunSpec,
        lineage: Option<RunLineage>,
        persistence_mode: PersistenceMode,
        live_event_capacity: usize,
        input_drains: InputDrainGate,
    ) -> Result<Arc<Self>, ProtocolError> {
        Self::spawn_with_hooks(
            NativeSpawnConfig {
                spec,
                lineage,
                persistence_mode,
                live_event_capacity,
                input_drains,
                terminal_publications: TerminalPublicationOwner::default(),
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
            spec,
            persistence_mode,
            TerminalPublicationOwner::default(),
            after_wait,
        )
    }

    #[cfg(test)]
    fn spawn_with_wait_hook_owner<G>(
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
                spec,
                lineage: None,
                persistence_mode,
                live_event_capacity: LIVE_EVENT_CAPACITY,
                input_drains: InputDrainGate::default(),
                terminal_publications,
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
        let pair = native_pty_system()
            .openpty(to_pty_size(config.spec.size))
            .map_err(|error| spawn_error("open PTY", error))?;
        // Prepare every fallible PTY view before physical launch. Once a child
        // exists, native control and PendingPublication can be built without a
        // setup error window that lacks exact-key cleanup ownership.
        setup(LaunchSetupStep::CloneReader, None)?;
        let reader = pair
            .master
            .try_clone_reader()
            .map_err(|error| spawn_error("clone PTY reader", error))?;
        setup(LaunchSetupStep::TakeWriter, None)?;
        let writer = pair
            .master
            .take_writer()
            .map_err(|error| spawn_error("take PTY writer", error))?;
        let child = pair
            .slave
            .spawn_command(config.command())
            .map_err(|error| spawn_error("spawn child", error))?;
        drop(pair.slave);
        let mut pending_child = PendingChild::new(child);
        let pid = pending_child.child().process_id();
        let (events, _) = broadcast::channel(config.live_event_capacity);
        let id = RunId::new();
        let (native_control, child_command_rx) =
            NativeControlOwner::new(id, pair.master, writer, config.input_drains.clone());
        pending_child.bind_reap_control(native_control.clone());
        let owner = make_owner(Self::new_native(config, id, pid, native_control, events));
        let run = Arc::clone(owner.run());

        // Start the waiter first, but do not hand it the child until the output
        // reader also exists. A reader setup failure therefore drops the child
        // sender, lets the empty waiter exit, and leaves `PendingChild` to
        // synchronously kill/reap without an orphaned output owner.
        let (output_done_tx, output_done_rx) = mpsc::channel();
        let wait_run = Arc::clone(&run);
        let wait_control = run
            .native_control()
            .expect("spawned Run retains native control")
            .clone();
        if let Err(error) = setup(LaunchSetupStep::StartWaiterThread, pid) {
            run.native_control()
                .expect("spawned Run retains native control")
                .mark_closed();
            return Err(error);
        }
        let (child_tx, child_rx) = mpsc::channel::<PendingChild>();
        thread::Builder::new()
            .name(format!("ctxmux-wait-{}", run.id))
            .spawn(move || {
                let Ok(pending_child) = child_rx.recv() else {
                    wait_control.mark_closed();
                    return;
                };
                let mut child = pending_child.into_child();
                let state = wait_for_child(child.as_mut(), &child_command_rx, &wait_control);
                drop(child_command_rx);
                wait_control.mark_closed();
                after_wait();
                let _ = output_done_rx.recv_timeout(Duration::from_secs(1));
                wait_run.publish_terminal(state);
            })
            .map_err(|error| {
                run.native_control()
                    .expect("spawned Run retains native control")
                    .mark_closed();
                spawn_error("start child waiter", error)
            })?;

        let output_run = Arc::clone(&run);
        setup(LaunchSetupStep::StartOutputThread, pid)?;
        thread::Builder::new()
            .name(format!("ctxmux-output-{}", run.id))
            .spawn(move || {
                read_output(&output_run, reader);
                let _ = output_done_tx.send(());
            })
            .map_err(|error| spawn_error("start PTY reader", error))?;

        child_tx.send(pending_child).map_err(|_| {
            spawn_error(
                "handoff child to waiter",
                "waiter stopped before taking ownership",
            )
        })?;

        Ok(owner)
    }

    fn new_native(
        config: NativeSpawnConfig,
        id: RunId,
        pid: Option<u32>,
        native_control: NativeControlOwner,
        events: broadcast::Sender<RunEvent>,
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
            persistence_mode: config.persistence_mode,
            persistence_transition: Mutex::new(()),
            persistence: Mutex::new(None),
            attachments: AtomicUsize::new(0),
            terminal_publications: config.terminal_publications,
            terminal_ordinal: OnceLock::new(),
            events,
        })
    }

    fn import_tmux(
        socket_path: &str,
        pane_id: &str,
        live_event_capacity: usize,
        terminal_publications: TerminalPublicationOwner,
        discovery_deadline: Instant,
        prepare_deadline: Instant,
        total_deadline: Instant,
    ) -> Result<Arc<Self>, ProtocolError> {
        let mut pending = tmux::spawn_control(socket_path, pane_id, discovery_deadline)?;
        let target = pending.target.clone();
        let socket_identity = pending.socket_identity;
        let control_pid = pending.child_id();
        let stdin = pending.take_stdin();
        let stdout = pending.take_stdout();
        let (commands_tx, commands_rx) = mpsc::channel();
        let (completion_tx, completion_rx) = mpsc::sync_channel(1);
        let (events, _) = broadcast::channel(live_event_capacity);
        let run = Arc::new(Self {
            id: RunId::new(),
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
                completion: Mutex::new(completion_rx),
            })),
            persistence_mode: PersistenceMode::MemoryOnly,
            persistence_transition: Mutex::new(()),
            persistence: Mutex::new(None),
            attachments: AtomicUsize::new(0),
            terminal_publications,
            terminal_ordinal: OnceLock::new(),
            events,
        });
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let (output_done_tx, output_done_rx) = mpsc::channel();
        let output_run = Arc::clone(&run);
        let output_target = target.clone();
        let output_ready = ready_tx.clone();
        thread::Builder::new()
            .name(format!("ctxmux-tmux-output-{}", run.id))
            .spawn(move || {
                let termination =
                    read_tmux_output(&output_run, stdout, &output_target, &output_ready);
                if output_done_tx.send(termination).is_ok() {
                    output_run.notify_tmux_reader_terminated();
                }
            })
            .map_err(|error| backend_protocol_error("start tmux output reader", error))?;

        let wait_run = Arc::clone(&run);
        let wait_target = target;
        let wait_ready = ready_tx;
        let (child_tx, child_rx) = mpsc::sync_channel(0);
        thread::Builder::new()
            .name(format!("ctxmux-tmux-wait-{}", run.id))
            .spawn(move || {
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

        Self::finish_tmux_import(run, &ready_rx, prepare_deadline, total_deadline)
    }

    fn finish_tmux_import(
        run: Arc<Self>,
        ready: &mpsc::Receiver<Result<(), ProtocolError>>,
        prepare_deadline: Instant,
        total_deadline: Instant,
    ) -> Result<Arc<Self>, ProtocolError> {
        let readiness =
            match ready.recv_timeout(prepare_deadline.saturating_duration_since(Instant::now())) {
                Ok(Ok(())) if Instant::now() < prepare_deadline => return Ok(run),
                Ok(Ok(())) => ProtocolError::new(
                    ErrorCode::BackendUnavailable,
                    "tmux Control Mode readiness exceeded the import preparation deadline",
                ),
                Ok(Err(error)) => error,
                Err(error) => backend_protocol_error("wait for tmux Control Mode readiness", error),
            };
        run.interrupt_tmux(InterruptionReason::TmuxServerUnavailable);
        let cleanup_timeout = TMUX_FAILED_IMPORT_CLEANUP_TIMEOUT
            .min(total_deadline.saturating_duration_since(Instant::now()));
        match run.wait_for_tmux_completion(cleanup_timeout) {
            Ok(()) => Err(readiness),
            Err(cleanup_error) => Err(ProtocolError::new(
                readiness.code,
                format!("{}; cleanup failed: {cleanup_error}", readiness.message),
            )),
        }
    }

    fn recover(
        recovered: RecoveredRun,
        persistence: PersistentRun,
        live_event_capacity: usize,
        terminal_publications: TerminalPublicationOwner,
    ) -> Arc<Self> {
        let (events, _) = broadcast::channel(live_event_capacity);
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
            persistence_mode: PersistenceMode::PersistentCapable,
            persistence_transition: Mutex::new(()),
            persistence: Mutex::new(Some(persistence)),
            attachments: AtomicUsize::new(0),
            terminal_publications,
            terminal_ordinal: OnceLock::new(),
            events,
        })
    }

    fn info(&self) -> RunInfo {
        let output = mutex_lock(&self.output);
        RunInfo {
            id: self.id,
            spec: self.spec.clone(),
            lineage: self.lineage.clone(),
            backend: self.backend.clone(),
            capabilities: self.capabilities,
            pid: self.pid,
            state: mutex_lock(&self.state).clone(),
            head_seq: output.head_seq(),
            durable_head_seq: mutex_lock(&self.persistence)
                .as_ref()
                .map(PersistentRun::durable_head),
            oldest_seq: output.oldest_seq(),
            attachments: self.attachments.load(Ordering::Acquire),
        }
    }

    fn persistence_start_info(&self) -> RunInfo {
        let mut info = self.info();
        info.state = RunState::Running;
        info
    }

    async fn input(&self, data: Vec<u8>) -> ControlResult {
        self.begin_input(data)?.resolve().await
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
        let chunk = match self.persistence_mode {
            PersistenceMode::MemoryOnly => mutex_lock(&self.output).push(data),
            PersistenceMode::PersistentCapable => {
                let _transition = mutex_lock(&self.persistence_transition);
                let (chunk, replay, running, persistence) = {
                    let mut output = mutex_lock(&self.output);
                    let chunk = output.push(data);
                    let replay = output.replay(chunk.seq.saturating_sub(1));
                    let running = mutex_lock(&self.state).is_running();
                    let persistence = mutex_lock(&self.persistence).as_ref().cloned();
                    (chunk, replay, running, persistence)
                };
                if running && let Some(persistence) = persistence {
                    persistence.append(self.id, replay);
                }
                chunk
            }
        };
        let _ = self.events.send(RunEvent::Output { chunk });
    }

    fn mark_output_source_gap(&self) -> u64 {
        mutex_lock(&self.output).mark_source_gap()
    }

    fn attach(self: &Arc<Self>, after_seq: u64) -> (AttachmentGuard, AttachedSnapshot) {
        self.attachments.fetch_add(1, Ordering::AcqRel);
        let guard = AttachmentGuard(Arc::clone(self));
        let replay = mutex_lock(&self.output).replay(after_seq);
        let snapshot = AttachedSnapshot {
            run: self.info(),
            replay,
        };
        (guard, snapshot)
    }

    fn subscribe(&self) -> broadcast::Receiver<RunEvent> {
        self.events.subscribe()
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

    fn interrupt_tmux(&self, reason: InterruptionReason) {
        if let Some(RunControl::Tmux(control)) = &self.incarnation_control {
            let _ = control.commands.send(TmuxControlCommand::Interrupt(reason));
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
        match mutex_lock(&control.completion).recv_timeout(timeout) {
            Ok(result) => result,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                Err("timed out waiting for tmux control cleanup".to_owned())
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                Err("tmux control waiter ended without a completion receipt".to_owned())
            }
        }
    }

    fn enable_persistence(&self, persistence: &PersistentRun) {
        assert_eq!(
            self.persistence_mode,
            PersistenceMode::PersistentCapable,
            "only persistence-capable Runs can bind durable state"
        );
        let _transition = mutex_lock(&self.persistence_transition);
        let (replay, state) = {
            let output = mutex_lock(&self.output);
            let state = mutex_lock(&self.state).clone();
            let persistence_guard = mutex_lock(&self.persistence);
            debug_assert!(persistence_guard.is_none());
            (output.replay(0), state)
        };
        if state.is_running() {
            persistence.append(self.id, replay);
        } else {
            persistence.finalize(self.id, replay, state);
        }
        let _output = mutex_lock(&self.output);
        let _state = mutex_lock(&self.state);
        *mutex_lock(&self.persistence) = Some(persistence.clone());
    }

    fn publish_terminal(&self, terminal: RunState) {
        if self.persistence_mode == PersistenceMode::MemoryOnly {
            self.publish_terminal_state(terminal.clone());
            let _ = self.events.send(RunEvent::Exited { state: terminal });
            return;
        }
        let _transition = mutex_lock(&self.persistence_transition);
        let (replay, persistence) = {
            let output = mutex_lock(&self.output);
            let persistence = mutex_lock(&self.persistence).as_ref().cloned();
            (output.replay(0), persistence)
        };
        if let Some(persistence) = persistence {
            persistence.finalize(self.id, replay, terminal.clone());
            let _output = mutex_lock(&self.output);
            self.publish_terminal_state(terminal.clone());
        } else {
            self.publish_terminal_state(terminal.clone());
        }
        let _ = self.events.send(RunEvent::Exited { state: terminal });
    }

    fn publish_interrupted(&self, reason: InterruptionReason) {
        self.publish_terminal_state(RunState::Interrupted { reason });
        let _ = self.events.send(RunEvent::Interrupted { reason });
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
                Ok(()) => return Ok(()),
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
        self.native_control()
            .map_err(|error| error.message.clone())?
            .unpublished_cleanup_result()?;
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

fn wait_for_child(
    child: &mut dyn Child,
    commands: &mpsc::Receiver<ChildCommand>,
    control: &NativeControlOwner,
) -> RunState {
    loop {
        match commands.recv_timeout(CHILD_CONTROL_POLL) {
            Ok(ChildCommand::Stop(reply)) => {
                let result = child.kill().map_err(|error| error.to_string());
                let _ = reply.send(result);
            }
            Ok(ChildCommand::CleanupUnpublished) => {
                if let Err(error) = child.kill() {
                    control.record_cleanup_error(format!(
                        "failed to kill unpublished Run child: {error}"
                    ));
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout | mpsc::RecvTimeoutError::Disconnected) => {}
        }
        match child.try_wait() {
            Ok(Some(status)) => {
                control.mark_reaped();
                return RunState::Exited {
                    code: status.exit_code(),
                    signal: status.signal().map(str::to_owned),
                };
            }
            Ok(None) => {}
            Err(error) => {
                control.record_wait_error(format!("failed to wait for child: {error}"));
            }
        }
    }
}

fn wait_for_tmux_control(
    child: &mut std::process::Child,
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

fn terminate_tmux_control_child(child: &mut std::process::Child) -> Result<(), String> {
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
            let _ = run.events.send(RunEvent::Tmux {
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
            let head_seq = run.mark_output_source_gap();
            let _ = run.events.send(RunEvent::Tmux {
                event: TmuxRunEvent::Paused,
            });
            let _ = run.events.send(RunEvent::Gap { head_seq });
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
            let _ = run.events.send(RunEvent::Tmux {
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

struct AttachmentGuard(Arc<Run>);

impl Drop for AttachmentGuard {
    fn drop(&mut self) {
        self.0.attachments.fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Default)]
struct OutputLog {
    chunks: VecDeque<OutputChunk>,
    retained_bytes: usize,
    next_seq: u64,
    source_gap_after_seq: Option<u64>,
}

impl OutputLog {
    fn with_initial_truncation() -> Self {
        Self {
            source_gap_after_seq: Some(0),
            ..Self::default()
        }
    }

    fn from_replay(replay: OutputReplay) -> Self {
        Self {
            retained_bytes: replay.chunks.iter().map(|chunk| chunk.data.len()).sum(),
            chunks: replay.chunks.into(),
            next_seq: replay.head_seq,
            source_gap_after_seq: None,
        }
    }

    fn mark_source_gap(&mut self) -> u64 {
        self.source_gap_after_seq = Some(self.next_seq);
        self.next_seq
    }

    fn push(&mut self, data: Vec<u8>) -> OutputChunk {
        self.next_seq = self.next_seq.saturating_add(1);
        let chunk = OutputChunk {
            seq: self.next_seq,
            data,
        };
        self.retained_bytes = self.retained_bytes.saturating_add(chunk.data.len());
        self.chunks.push_back(chunk.clone());
        while self.retained_bytes > OUTPUT_RETENTION_BYTES && self.chunks.len() > 1 {
            if let Some(evicted) = self.chunks.pop_front() {
                self.retained_bytes = self.retained_bytes.saturating_sub(evicted.data.len());
            }
        }
        chunk
    }

    const fn head_seq(&self) -> u64 {
        self.next_seq
    }

    fn oldest_seq(&self) -> u64 {
        self.chunks.front().map_or(0, |chunk| chunk.seq)
    }

    fn replay(&self, after_seq: u64) -> OutputReplay {
        let oldest_seq = self.oldest_seq();
        OutputReplay {
            chunks: self
                .chunks
                .iter()
                .filter(|chunk| chunk.seq > after_seq)
                .cloned()
                .collect(),
            oldest_seq,
            head_seq: self.head_seq(),
            truncated: self
                .source_gap_after_seq
                .is_some_and(|gap_seq| after_seq <= gap_seq)
                || (oldest_seq > 0 && after_seq.saturating_add(1) < oldest_seq),
        }
    }
}

fn read_output(run: &Run, mut reader: Box<dyn Read + Send>) {
    let mut buffer = vec![0; OUTPUT_READ_BUFFER_BYTES];
    loop {
        match reader.read(&mut buffer) {
            Ok(0) => return,
            Ok(read) => run.record_output(buffer[..read].to_vec()),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => {
                eprintln!("ctxmuxd PTY read failed for {}: {error}", run.id);
                return;
            }
        }
    }
}

fn invalid_run_spec(error: run_spec::RunSpecValidationError) -> ProtocolError {
    ProtocolError::new(ErrorCode::InvalidRequest, error.to_string())
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

    if let Request::Attach { id, after_seq } = request {
        return attachment::handle(wire, manager, id, after_seq).await;
    }
    let response = execute_request(&manager, request).await;
    match response {
        Ok(response) => send(&mut wire, &ServerFrame::Response { response }).await?,
        Err(error) => send(&mut wire, &ServerFrame::Error { error }).await?,
    }
    Ok(())
}

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
            let operation_manager = Arc::clone(manager);
            let run = run_blocking_tmux_operation(move || {
                operation_manager.import_tmux(&socket_path, &pane_id)
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
        fs,
        os::unix::{
            fs::{PermissionsExt, symlink},
            net::{UnixListener, UnixStream},
        },
        process::{Command, Stdio},
        sync::{Arc, Mutex, atomic::AtomicBool},
        time::{Duration, Instant},
    };

    use ctxmux_client::{Client, ClientError, replay_bytes};
    use ctxmux_protocol::{
        CreateOperationKey, ErrorCode, ForkPlan, InterruptionReason, ProtocolError, RunEvent,
        RunSpec, RunState, TerminalSize,
    };
    use tokio::sync::{Barrier, Notify, broadcast, mpsc};

    use super::{
        AttachmentHookPoint, AttachmentTestHook, CreationHookPoint, CreationRequest,
        CreationTestHook, LIVE_EVENT_CAPACITY, LaunchSetupStep, OUTPUT_RETENTION_BYTES, OutputLog,
        OutputReplay, Persistence, PersistenceMode, RecoveredRun, Run, RunManager, ServerError,
        TMUX_DISCOVERY_TIMEOUT, TMUX_FAILED_IMPORT_CLEANUP_TIMEOUT, TMUX_IMPORT_DISCOVERY_TIMEOUT,
        TMUX_IMPORT_PREPARE_TIMEOUT, TMUX_IMPORT_TOTAL_TIMEOUT, TMUX_SHUTDOWN_TIMEOUT,
        TmuxCommandKind, TmuxCommandResultKind, TmuxCommandTracker, TmuxCommandWriter,
        TmuxReaderTermination, TmuxRunControl, TmuxTermination, TmuxWaitCause, mutex_lock,
        prepare_socket_path, prepare_socket_path_with_hook, resolve_tmux_termination,
        serve_with_manager, spawn_error,
    };

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
            completion: std::sync::Mutex::new(completion),
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
            LaunchSetupStep::StartOutputThread,
            LaunchSetupStep::StartWaiterThread,
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
                LaunchSetupStep::StartOutputThread | LaunchSetupStep::StartWaiterThread
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
        assert_eq!(snapshot.replay.head_seq, 1);

        recorded.record_output(b"after".to_vec());
        let event = tokio::time::timeout(Duration::from_secs(5), attachment.next_event())
            .await
            .expect("post-snapshot output arrives")
            .expect("read post-snapshot output")
            .expect("attachment stays live");
        let RunEvent::Output { chunk } = event else {
            panic!("expected post-snapshot output, got {event:?}");
        };
        assert_eq!(chunk.seq, 2);
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
        let caller_cursor = snapshot.replay.head_seq;
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
                            if failure.error.code == ErrorCode::InvalidRunState =>
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
        assert!(replay.oldest_seq > 1);
        assert_eq!(replay.head_seq, 600);
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
        assert_eq!(exact_limit.oldest_seq, 1);
        assert_eq!(exact_limit.head_seq, 2);
        assert_eq!(
            exact_limit
                .chunks
                .iter()
                .map(|chunk| chunk.seq)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );

        output.push(vec![b'c']);
        let evicted = output.replay(0);
        assert!(evicted.truncated);
        assert_eq!(evicted.oldest_seq, 2);
        assert_eq!(evicted.head_seq, 3);
        assert_eq!(
            evicted
                .chunks
                .iter()
                .map(|chunk| chunk.seq)
                .collect::<Vec<_>>(),
            vec![2, 3]
        );

        let immediately_before_oldest = output.replay(1);
        assert!(!immediately_before_oldest.truncated);
        assert_eq!(immediately_before_oldest.chunks, evicted.chunks);
        assert_eq!(
            output
                .replay(2)
                .chunks
                .iter()
                .map(|chunk| chunk.seq)
                .collect::<Vec<_>>(),
            vec![3]
        );
        assert!(output.replay(3).chunks.is_empty());
        assert!(output.replay(99).chunks.is_empty());
        assert!(!output.replay(99).truncated);
    }

    #[test]
    fn replay_keeps_a_tmux_source_gap_visible_to_late_attachments() {
        let mut output = OutputLog::with_initial_truncation();
        assert!(output.replay(0).truncated);
        assert_eq!(output.mark_source_gap(), 0);

        output.push(b"before-gap".to_vec());
        assert_eq!(output.mark_source_gap(), 1);
        let at_gap = output.replay(1);
        assert!(at_gap.truncated);
        assert!(at_gap.chunks.is_empty());

        output.push(b"after-gap".to_vec());
        let recovery = output.replay(1);
        assert!(recovery.truncated);
        assert_eq!(recovery.oldest_seq, 1);
        assert_eq!(recovery.head_seq, 2);
        assert_eq!(recovery.chunks[0].seq, 2);
        assert!(!output.replay(2).truncated);
    }

    #[test]
    fn one_oversized_output_chunk_is_retained_as_an_honest_replay_unit() {
        let mut output = OutputLog::default();
        let oversized = vec![0xa5; OUTPUT_RETENTION_BYTES + 1];
        output.push(oversized.clone());

        let replay = output.replay(0);
        assert!(!replay.truncated);
        assert_eq!(replay.oldest_seq, 1);
        assert_eq!(replay.head_seq, 1);
        assert_eq!(replay.chunks[0].data, oversized);

        output.push(vec![0x5a]);
        let after_eviction = output.replay(0);
        assert!(after_eviction.truncated);
        assert_eq!(after_eviction.oldest_seq, 2);
        assert_eq!(after_eviction.head_seq, 2);
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
                .map(|chunk| chunk.seq)
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
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
        let caller_cursor = initial.replay.head_seq;
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
                RunEvent::Gap { head_seq } => head_seq,
                event => panic!("expected public Gap event, got {event:?}"),
            };
        drop(lagged_attachment);

        let (recovered_attachment, recovered) = client
            .attach(run.id, caller_cursor)
            .await
            .expect("reattach from caller-owned cursor");
        assert!(!recovered.replay.truncated);
        assert_eq!(recovered.replay.head_seq, gap_head);
        assert_eq!(
            recovered
                .replay
                .chunks
                .iter()
                .map(|chunk| chunk.seq)
                .collect::<Vec<_>>(),
            ((caller_cursor + 1)..=gap_head).collect::<Vec<_>>()
        );
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
}
