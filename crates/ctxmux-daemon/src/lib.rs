//! Long-lived native Run owner and local protocol server.

#[cfg(not(unix))]
compile_error!("the first ctxmux native transport currently requires Unix sockets");

use std::{
    collections::{HashMap, VecDeque},
    fs,
    io::{self, Read, Write},
    os::unix::{
        fs::{FileTypeExt, MetadataExt, PermissionsExt},
        net::UnixStream as StdUnixStream,
    },
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

mod persistence;
mod run_spec;
mod tmux;

pub use persistence::PersistenceError;

use ctxmux_protocol::{
    AttachedHeader, AttachedSnapshot, ClientFrame, ErrorCode, ForkFidelity, ForkPlan,
    InterruptionReason, MAX_FRAME_BYTES, OutputChunk, OutputReplay, OutputReplayHeader,
    PROTOCOL_VERSION, ProtocolError, Request, Response, RunBackend, RunCapabilities, RunEvent,
    RunId, RunInfo, RunLineage, RunSpec, RunState, ServerFrame, TerminalSize, TmuxRunEvent,
    decode_frame, encode_frame,
};
use futures_util::{SinkExt, StreamExt};
use portable_pty::{Child, CommandBuilder, MasterPty, PtySize, native_pty_system};
use run_spec::{validate_run_spec, validate_terminal_size};
use thiserror::Error;
use tokio::{
    net::{UnixListener, UnixStream},
    sync::broadcast,
};
use tokio_util::codec::{Framed, LinesCodec, LinesCodecError};

use crate::persistence::{Persistence, PersistentRun, RecoveredRun};
use crate::tmux::{ControlItem, ControlParser, SocketIdentity as TmuxSocketIdentity};

const OUTPUT_RETENTION_BYTES: usize = 4 * 1024 * 1024;
const OUTPUT_READ_BUFFER_BYTES: usize = 8192;
const LIVE_EVENT_CAPACITY: usize = 256;
const CHILD_CONTROL_POLL: Duration = Duration::from_millis(20);
const STOP_ACK_TIMEOUT: Duration = Duration::from_secs(1);
const TMUX_OUTPUT_DRAIN_TIMEOUT: Duration = Duration::from_secs(1);
const TMUX_FAILED_IMPORT_CLEANUP_TIMEOUT: Duration = Duration::from_secs(2);
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
    /// One or more ctxmux-owned Backend control processes failed cleanup.
    #[error("ctxmux daemon shutdown failed: {failures}")]
    Shutdown {
        /// Aggregated cleanup failures for ctxmux-owned control processes.
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
/// ctxmux-owned Backend control process cannot be cleaned up during shutdown.
pub async fn serve(socket_path: impl Into<PathBuf>) -> Result<(), ServerError> {
    serve_with_persistence(socket_path.into(), None).await
}

/// Serve Runs with historical metadata and replay persisted in `state_dir`.
///
/// # Errors
///
/// Returns [`ServerError`] when the state directory cannot be exclusively and
/// safely opened, its exact schema or invariants fail validation, the socket
/// cannot be published, or Backend control cleanup fails during shutdown.
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
                manager.shutdown_tmux_controls(TMUX_SHUTDOWN_TIMEOUT)?;
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
    runs: RwLock<HashMap<RunId, Arc<Run>>>,
    live_event_capacity: usize,
    persistence: Option<Persistence>,
    tmux_shutting_down: AtomicBool,
    tmux_operation_gate: RwLock<()>,
    #[cfg(test)]
    attachment_hook: Option<Arc<AttachmentTestHook>>,
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
impl AttachmentTestHook {
    async fn pause_once(&self, point: AttachmentHookPoint) {
        if self.point != point || !self.armed.swap(false, Ordering::AcqRel) {
            return;
        }
        let _ = self.reached.send(());
        self.release.notified().await;
    }
}

impl Default for RunManager {
    fn default() -> Self {
        Self {
            runs: RwLock::default(),
            live_event_capacity: LIVE_EVENT_CAPACITY,
            persistence: None,
            tmux_shutting_down: AtomicBool::new(false),
            tmux_operation_gate: RwLock::new(()),
            #[cfg(test)]
            attachment_hook: None,
        }
    }
}

impl RunManager {
    fn persistent(persistence: Persistence, recovered: Vec<RecoveredRun>) -> Self {
        let runs = recovered
            .into_iter()
            .map(|recovered| {
                let id = recovered.info.id;
                let durable = persistence.recovered_run(
                    recovered
                        .info
                        .durable_head_seq
                        .unwrap_or(recovered.info.head_seq),
                );
                (id, Run::recover(recovered, durable, LIVE_EVENT_CAPACITY))
            })
            .collect();
        Self {
            runs: RwLock::new(runs),
            live_event_capacity: LIVE_EVENT_CAPACITY,
            persistence: Some(persistence),
            tmux_shutting_down: AtomicBool::new(false),
            tmux_operation_gate: RwLock::new(()),
            #[cfg(test)]
            attachment_hook: None,
        }
    }

    fn start(&self, spec: RunSpec) -> Result<RunInfo, ProtocolError> {
        let run = Run::spawn(spec, None, self.live_event_capacity)?;
        self.prepare_publication(&run)?;
        let info = run.info();
        write_lock(&self.runs).insert(info.id, run);
        Ok(info)
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
        self.with_tmux_operation(|| tmux::discover(socket_path))
    }

    fn import_tmux(&self, socket_path: &str, pane_id: &str) -> Result<RunInfo, ProtocolError> {
        if self.persistence.is_some() {
            return Err(ProtocolError::new(
                ErrorCode::UnsupportedCapability,
                "tmux pane import is not persisted; use a memory-only ctxmux daemon",
            ));
        }
        self.with_tmux_operation(|| {
            let run = Run::import_tmux(socket_path, pane_id, self.live_event_capacity)?;
            let info = run.info();
            write_lock(&self.runs).insert(info.id, run);
            Ok(info)
        })
    }

    fn shutdown_tmux_controls(&self, timeout: Duration) -> Result<(), ServerError> {
        let deadline = Instant::now() + timeout;
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

        let mut pending = read_lock(&self.runs)
            .values()
            .filter(|run| matches!(run.live, Some(RunControl::Tmux(_))))
            .cloned()
            .collect::<Vec<_>>();

        for run in &pending {
            if let Some(RunControl::Tmux(control)) = &run.live {
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
                let Some(RunControl::Tmux(control)) = &run.live else {
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

        if failures.is_empty() {
            Ok(())
        } else {
            failures.sort();
            Err(ServerError::Shutdown {
                failures: failures.join("; "),
            })
        }
    }

    fn fork(&self, parent: RunId, plan: ForkPlan) -> Result<RunInfo, ProtocolError> {
        let parent_run = self.get(parent)?;
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
                if parent_run.live.is_none() {
                    return Err(ProtocolError::new(
                        ErrorCode::InvalidRunState,
                        format!("cannot Level B fork historical Run {parent}"),
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
        let run = Run::spawn(
            spec,
            Some(RunLineage { parent, fidelity }),
            self.live_event_capacity,
        )?;
        self.prepare_publication(&run)?;
        let info = run.info();
        write_lock(&self.runs).insert(info.id, run);
        Ok(info)
    }

    fn get(&self, id: RunId) -> Result<Arc<Run>, ProtocolError> {
        read_lock(&self.runs).get(&id).cloned().ok_or_else(|| {
            ProtocolError::new(ErrorCode::RunNotFound, format!("Run {id} does not exist"))
        })
    }

    fn list(&self) -> Vec<RunInfo> {
        let mut runs = read_lock(&self.runs)
            .values()
            .map(|run| run.info())
            .collect::<Vec<_>>();
        runs.sort_by_key(|run| run.id.to_string());
        runs
    }

    fn prepare_publication(&self, run: &Arc<Run>) -> Result<(), ProtocolError> {
        let Some(persistence) = &self.persistence else {
            return Ok(());
        };
        match persistence.insert_start(&run.info()) {
            Ok(durable) => {
                run.enable_persistence(&durable);
                Ok(())
            }
            Err(error) => {
                run.terminate_unpublished();
                Err(ProtocolError::new(
                    ErrorCode::Persistence,
                    error.to_string(),
                ))
            }
        }
    }

    #[cfg(test)]
    fn start_with_setup<F>(&self, spec: RunSpec, setup: F) -> Result<RunInfo, ProtocolError>
    where
        F: FnMut(LaunchSetupStep, Option<u32>) -> Result<(), ProtocolError>,
    {
        let run = Run::spawn_with_setup(spec, None, setup)?;
        let info = run.info();
        write_lock(&self.runs).insert(info.id, run);
        Ok(info)
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
        let run = Run::spawn_with_wait_hook(spec, after_wait)?;
        let info = run.info();
        write_lock(&self.runs).insert(info.id, run);
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

struct PendingChild {
    child: Option<Box<dyn Child + Send + Sync>>,
}

enum ChildCommand {
    Stop(mpsc::SyncSender<Result<(), String>>),
}

struct ChildController {
    sender: Option<mpsc::Sender<ChildCommand>>,
    stop_requested: bool,
}

impl PendingChild {
    const fn new(child: Box<dyn Child + Send + Sync>) -> Self {
        Self { child: Some(child) }
    }

    fn child(&self) -> &(dyn Child + Send + Sync) {
        self.child.as_deref().expect("pending child is present")
    }

    fn into_child(mut self) -> Box<dyn Child + Send + Sync> {
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
        }
        if let Err(error) = child.wait() {
            eprintln!("ctxmuxd failed to reap rejected child: {error}");
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
    live: Option<RunControl>,
    persistence: Mutex<Option<PersistentRun>>,
    attachments: AtomicUsize,
    events: broadcast::Sender<RunEvent>,
}

enum RunControl {
    Native(NativeRunControl),
    Tmux(TmuxRunControl),
}

struct NativeRunControl {
    master: Mutex<Box<dyn MasterPty + Send>>,
    writer: Mutex<Box<dyn Write + Send>>,
    child_controller: Mutex<ChildController>,
}

struct TmuxRunControl {
    writer: Mutex<TmuxCommandWriter>,
    commands: mpsc::Sender<TmuxControlCommand>,
    completion: Mutex<mpsc::Receiver<Result<(), String>>>,
}

struct TmuxCommandWriter {
    stdin: std::process::ChildStdin,
    pending: VecDeque<TmuxCommandKind>,
}

impl TmuxCommandWriter {
    fn new(stdin: std::process::ChildStdin) -> Self {
        Self {
            stdin,
            pending: VecDeque::new(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TmuxCommandKind {
    TargetProbe,
    Continue,
}

enum TmuxControlCommand {
    Interrupt(InterruptionReason),
    Shutdown,
}

struct TmuxWaitOutcome {
    reason: InterruptionReason,
    cleanup: Result<(), String>,
}

impl Run {
    fn spawn(
        spec: RunSpec,
        lineage: Option<RunLineage>,
        live_event_capacity: usize,
    ) -> Result<Arc<Self>, ProtocolError> {
        Self::spawn_with_hooks(spec, lineage, live_event_capacity, |_, _| Ok(()), || {})
    }

    #[cfg(test)]
    fn spawn_with_setup<F>(
        spec: RunSpec,
        lineage: Option<RunLineage>,
        setup: F,
    ) -> Result<Arc<Self>, ProtocolError>
    where
        F: FnMut(LaunchSetupStep, Option<u32>) -> Result<(), ProtocolError>,
    {
        Self::spawn_with_hooks(spec, lineage, LIVE_EVENT_CAPACITY, setup, || {})
    }

    #[cfg(test)]
    fn spawn_with_wait_hook<G>(spec: RunSpec, after_wait: G) -> Result<Arc<Self>, ProtocolError>
    where
        G: FnOnce() + Send + 'static,
    {
        Self::spawn_with_hooks(spec, None, LIVE_EVENT_CAPACITY, |_, _| Ok(()), after_wait)
    }

    fn spawn_with_hooks<F, G>(
        spec: RunSpec,
        lineage: Option<RunLineage>,
        live_event_capacity: usize,
        mut setup: F,
        after_wait: G,
    ) -> Result<Arc<Self>, ProtocolError>
    where
        F: FnMut(LaunchSetupStep, Option<u32>) -> Result<(), ProtocolError>,
        G: FnOnce() + Send + 'static,
    {
        validate_run_spec(&spec).map_err(invalid_run_spec)?;
        let pair = native_pty_system()
            .openpty(to_pty_size(spec.size))
            .map_err(|error| spawn_error("open PTY", error))?;
        let mut command = CommandBuilder::new(&spec.program);
        command.args(&spec.args);
        if let Some(cwd) = &spec.cwd {
            command.cwd(cwd);
        }
        for (name, value) in &spec.env {
            command.env(name, value);
        }

        let child = pair
            .slave
            .spawn_command(command)
            .map_err(|error| spawn_error("spawn child", error))?;
        drop(pair.slave);
        let pending_child = PendingChild::new(child);
        let pid = pending_child.child().process_id();
        setup(LaunchSetupStep::CloneReader, pid)?;
        let reader = pair
            .master
            .try_clone_reader()
            .map_err(|error| spawn_error("clone PTY reader", error))?;
        setup(LaunchSetupStep::TakeWriter, pid)?;
        let writer = pair
            .master
            .take_writer()
            .map_err(|error| spawn_error("take PTY writer", error))?;
        let (child_command_tx, child_command_rx) = mpsc::channel();
        let (events, _) = broadcast::channel(live_event_capacity);
        let run = Arc::new(Self {
            id: RunId::new(),
            spec: Some(spec),
            lineage,
            backend: RunBackend::Native,
            capabilities: RunCapabilities::NATIVE,
            pid,
            state: Mutex::new(RunState::Running),
            output: Mutex::new(OutputLog::default()),
            live: Some(RunControl::Native(NativeRunControl {
                master: Mutex::new(pair.master),
                writer: Mutex::new(writer),
                child_controller: Mutex::new(ChildController {
                    sender: Some(child_command_tx),
                    stop_requested: false,
                }),
            })),
            persistence: Mutex::new(None),
            attachments: AtomicUsize::new(0),
            events,
        });

        let (output_done_tx, output_done_rx) = mpsc::channel();
        let output_run = Arc::clone(&run);
        setup(LaunchSetupStep::StartOutputThread, pid)?;
        thread::Builder::new()
            .name(format!("ctxmux-output-{}", run.id))
            .spawn(move || {
                read_output(&output_run, reader);
                let _ = output_done_tx.send(());
            })
            .map_err(|error| spawn_error("start PTY reader", error))?;

        let wait_run = Arc::clone(&run);
        setup(LaunchSetupStep::StartWaiterThread, pid)?;
        let (child_tx, child_rx) = mpsc::channel::<PendingChild>();
        thread::Builder::new()
            .name(format!("ctxmux-wait-{}", run.id))
            .spawn(move || {
                let Ok(pending_child) = child_rx.recv() else {
                    return;
                };
                let mut child = pending_child.into_child();
                let state = wait_for_child(child.as_mut(), &child_command_rx);
                mutex_lock(
                    &wait_run
                        .native_control()
                        .expect("spawned Run retains native control")
                        .child_controller,
                )
                .sender = None;
                after_wait();
                let _ = output_done_rx.recv_timeout(Duration::from_secs(1));
                wait_run.persist_terminal(&state);
                *mutex_lock(&wait_run.state) = state.clone();
                let _ = wait_run.events.send(RunEvent::Exited { state });
            })
            .map_err(|error| spawn_error("start child waiter", error))?;
        child_tx.send(pending_child).map_err(|_| {
            spawn_error(
                "handoff child to waiter",
                "waiter stopped before taking ownership",
            )
        })?;

        Ok(run)
    }

    fn import_tmux(
        socket_path: &str,
        pane_id: &str,
        live_event_capacity: usize,
    ) -> Result<Arc<Self>, ProtocolError> {
        let mut pending = tmux::spawn_control(socket_path, pane_id)?;
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
            live: Some(RunControl::Tmux(TmuxRunControl {
                writer: Mutex::new(TmuxCommandWriter::new(stdin)),
                commands: commands_tx,
                completion: Mutex::new(completion_rx),
            })),
            persistence: Mutex::new(None),
            attachments: AtomicUsize::new(0),
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
                read_tmux_output(&output_run, stdout, &output_target, &output_ready);
                let _ = output_done_tx.send(());
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
                let _ = wait_ready.try_send(Err(ProtocolError::new(
                    ErrorCode::BackendUnavailable,
                    format!("tmux Control Mode client {control_pid} exited before import"),
                )));
                let cleanup = match output_done_rx.recv_timeout(TMUX_OUTPUT_DRAIN_TIMEOUT) {
                    Ok(()) => outcome.cleanup,
                    Err(mpsc::RecvTimeoutError::Timeout) => combine_cleanup_failure(
                        outcome.cleanup,
                        "tmux output reader did not finish during shutdown",
                    ),
                    Err(mpsc::RecvTimeoutError::Disconnected) => combine_cleanup_failure(
                        outcome.cleanup,
                        "tmux output reader ended without a completion receipt",
                    ),
                };
                let state = RunState::Interrupted {
                    reason: outcome.reason,
                };
                *mutex_lock(&wait_run.state) = state;
                let _ = wait_run.events.send(RunEvent::Interrupted {
                    reason: outcome.reason,
                });
                let _ = completion_tx.send(cleanup);
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

        Self::finish_tmux_import(run, &ready_rx)
    }

    fn finish_tmux_import(
        run: Arc<Self>,
        ready: &mpsc::Receiver<Result<(), ProtocolError>>,
    ) -> Result<Arc<Self>, ProtocolError> {
        let readiness = match ready.recv_timeout(Duration::from_secs(5)) {
            Ok(Ok(())) => return Ok(run),
            Ok(Err(error)) => error,
            Err(error) => backend_protocol_error("wait for tmux Control Mode readiness", error),
        };
        run.interrupt_tmux(InterruptionReason::TmuxServerUnavailable);
        match run.wait_for_tmux_completion(TMUX_FAILED_IMPORT_CLEANUP_TIMEOUT) {
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
            live: None,
            persistence: Mutex::new(Some(persistence)),
            attachments: AtomicUsize::new(0),
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

    fn ensure_running(&self, operation: &str) -> Result<(), ProtocolError> {
        if mutex_lock(&self.state).is_running() {
            Ok(())
        } else {
            Err(ProtocolError::new(
                ErrorCode::InvalidRunState,
                format!("cannot {operation} terminal Run {}", self.id),
            ))
        }
    }

    fn input(&self, data: &[u8]) -> Result<RunInfo, ProtocolError> {
        self.ensure_running("write to")?;
        let live = self.native_control()?;
        let mut writer = mutex_lock(&live.writer);
        writer.write_all(data).map_err(io_protocol_error)?;
        writer.flush().map_err(io_protocol_error)?;
        Ok(self.info())
    }

    fn resize(&self, size: TerminalSize) -> Result<RunInfo, ProtocolError> {
        validate_terminal_size(size).map_err(invalid_run_spec)?;
        self.ensure_running("resize")?;
        let live = self.native_control()?;
        mutex_lock(&live.master)
            .resize(to_pty_size(size))
            .map_err(io_protocol_error)?;
        Ok(self.info())
    }

    fn stop(&self) -> Result<RunInfo, ProtocolError> {
        self.ensure_running("stop")?;
        let live = self.native_control()?;
        let sender = {
            let mut controller = mutex_lock(&live.child_controller);
            if controller.stop_requested {
                return Err(ProtocolError::new(
                    ErrorCode::InvalidRunState,
                    format!("stop already requested for Run {}", self.id),
                ));
            }
            let Some(sender) = controller.sender.clone() else {
                return Err(ProtocolError::new(
                    ErrorCode::InvalidRunState,
                    format!("cannot stop exited Run {}", self.id),
                ));
            };
            controller.stop_requested = true;
            sender
        };
        let (reply_tx, reply_rx) = mpsc::sync_channel(0);
        if sender.send(ChildCommand::Stop(reply_tx)).is_err() {
            mutex_lock(&live.child_controller).sender = None;
            return Err(ProtocolError::new(
                ErrorCode::InvalidRunState,
                format!("cannot stop exited Run {}", self.id),
            ));
        }
        match reply_rx.recv_timeout(STOP_ACK_TIMEOUT) {
            Ok(Ok(())) => Ok(self.info()),
            Ok(Err(error)) => {
                let mut controller = mutex_lock(&live.child_controller);
                if controller.sender.is_some() {
                    controller.stop_requested = false;
                }
                Err(io_protocol_error(error))
            }
            Err(mpsc::RecvTimeoutError::Timeout) => Err(ProtocolError::new(
                ErrorCode::Internal,
                format!("timed out while stopping Run {}", self.id),
            )),
            Err(mpsc::RecvTimeoutError::Disconnected) => Err(ProtocolError::new(
                ErrorCode::InvalidRunState,
                format!("cannot stop exited Run {}", self.id),
            )),
        }
    }

    fn record_output(&self, data: Vec<u8>) {
        let (chunk, replay) = {
            let mut output = mutex_lock(&self.output);
            let chunk = output.push(data);
            let replay = output.replay(chunk.seq.saturating_sub(1));
            (chunk, replay)
        };
        if let Some(persistence) = mutex_lock(&self.persistence).as_ref() {
            persistence.append(self.id, replay);
        }
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

    fn native_control(&self) -> Result<&NativeRunControl, ProtocolError> {
        match &self.live {
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

    fn write_tmux_command(&self, kind: TmuxCommandKind, command: &[u8]) -> io::Result<()> {
        let Some(RunControl::Tmux(control)) = &self.live else {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "Run has no tmux control client",
            ));
        };
        let mut writer = mutex_lock(&control.writer);
        if kind == TmuxCommandKind::TargetProbe
            && writer
                .pending
                .iter()
                .any(|pending| *pending == TmuxCommandKind::TargetProbe)
        {
            return Ok(());
        }
        writer.stdin.write_all(command)?;
        writer.stdin.flush()?;
        writer.pending.push_back(kind);
        Ok(())
    }

    fn take_tmux_command(&self) -> Option<TmuxCommandKind> {
        let Some(RunControl::Tmux(control)) = &self.live else {
            return None;
        };
        mutex_lock(&control.writer).pending.pop_front()
    }

    fn interrupt_tmux(&self, reason: InterruptionReason) {
        if let Some(RunControl::Tmux(control)) = &self.live {
            let _ = control.commands.send(TmuxControlCommand::Interrupt(reason));
        }
    }

    fn wait_for_tmux_completion(&self, timeout: Duration) -> Result<(), String> {
        let Some(RunControl::Tmux(control)) = &self.live else {
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
        let output = mutex_lock(&self.output);
        let state = mutex_lock(&self.state).clone();
        *mutex_lock(&self.persistence) = Some(persistence.clone());
        let replay = output.replay(0);
        drop(output);
        if state.is_running() {
            persistence.append(self.id, replay);
        } else {
            persistence.finalize(self.id, replay, state);
        }
    }

    fn persist_terminal(&self, state: &RunState) {
        let replay = mutex_lock(&self.output).replay(0);
        if let Some(persistence) = mutex_lock(&self.persistence).as_ref().cloned() {
            persistence.finalize(self.id, replay, state.clone());
        }
    }

    fn terminate_unpublished(&self) {
        if mutex_lock(&self.state).is_running() {
            let _ = self.stop();
        }
        for _ in 0..1_000 {
            if !mutex_lock(&self.state).is_running() {
                return;
            }
            thread::sleep(Duration::from_millis(5));
        }
        eprintln!(
            "ctxmuxd child for rejected durable Run {} did not exit",
            self.id
        );
    }
}

fn wait_for_child(child: &mut dyn Child, commands: &mpsc::Receiver<ChildCommand>) -> RunState {
    loop {
        match commands.recv_timeout(CHILD_CONTROL_POLL) {
            Ok(ChildCommand::Stop(reply)) => {
                let result = child.kill().map_err(|error| error.to_string());
                let _ = reply.send(result);
            }
            Err(mpsc::RecvTimeoutError::Timeout | mpsc::RecvTimeoutError::Disconnected) => {}
        }
        match child.try_wait() {
            Ok(Some(status)) => {
                return RunState::Exited {
                    code: status.exit_code(),
                    signal: status.signal().map(str::to_owned),
                };
            }
            Ok(None) => {}
            Err(error) => {
                return RunState::Exited {
                    code: 1,
                    signal: Some(format!("wait failed: {error}")),
                };
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
                    reason,
                    cleanup: terminate_tmux_control_child(child),
                };
            }
            Ok(TmuxControlCommand::Shutdown) | Err(mpsc::RecvTimeoutError::Disconnected) => {
                return TmuxWaitOutcome {
                    reason: InterruptionReason::TmuxServerUnavailable,
                    cleanup: terminate_tmux_control_child(child),
                };
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        }
        if Instant::now() >= next_target_poll {
            if !tmux::socket_identity_matches(&target.socket_path, socket_identity) {
                return TmuxWaitOutcome {
                    reason: InterruptionReason::TmuxTargetChanged,
                    cleanup: terminate_tmux_control_child(child),
                };
            }
            let command = tmux::target_probe_command(&target.pane_id);
            if run
                .write_tmux_command(TmuxCommandKind::TargetProbe, command.as_bytes())
                .is_err()
            {
                return TmuxWaitOutcome {
                    reason: InterruptionReason::TmuxServerUnavailable,
                    cleanup: terminate_tmux_control_child(child),
                };
            }
            next_target_poll = Instant::now() + TARGET_POLL;
        }
        match child.try_wait() {
            Ok(Some(_)) => {
                return TmuxWaitOutcome {
                    reason: InterruptionReason::TmuxServerUnavailable,
                    cleanup: Ok(()),
                };
            }
            Ok(None) => {}
            Err(error) => {
                return TmuxWaitOutcome {
                    reason: InterruptionReason::TmuxServerUnavailable,
                    cleanup: combine_cleanup_failure(
                        terminate_tmux_control_child(child),
                        &format!("failed to query tmux Control Mode client status: {error}"),
                    ),
                };
            }
        }
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
) {
    let mut reader = io::BufReader::new(stdout);
    let mut parser = ControlParser::default();
    let mut line = Vec::new();
    let mut readiness = TmuxReadiness::default();
    loop {
        match tmux::read_bounded_line(&mut reader, &mut line) {
            Ok(0) => {
                fail_tmux_control(
                    run,
                    ready,
                    ProtocolError::new(
                        ErrorCode::BackendUnavailable,
                        "tmux Control Mode stream closed",
                    ),
                    InterruptionReason::TmuxServerUnavailable,
                );
                return;
            }
            Ok(_) => {}
            Err(error) => {
                let reason = if readiness.ready {
                    InterruptionReason::TmuxProtocolError
                } else {
                    InterruptionReason::TmuxServerUnavailable
                };
                fail_tmux_control(
                    run,
                    ready,
                    backend_protocol_error("read tmux Control Mode stream", &error),
                    reason,
                );
                return;
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
                fail_tmux_control(
                    run,
                    ready,
                    backend_protocol_error("parse tmux Control Mode stream", &error),
                    reason,
                );
                return;
            }
        };
        let Some(item) = item else {
            continue;
        };
        if !handle_tmux_control_item(run, target, ready, &mut readiness, item) {
            return;
        }
    }
}

#[derive(Default)]
struct TmuxReadiness {
    session_seen: bool,
    ready: bool,
}

fn handle_tmux_control_item(
    run: &Run,
    target: &ctxmux_protocol::TmuxPaneInfo,
    ready: &mpsc::SyncSender<Result<(), ProtocolError>>,
    readiness: &mut TmuxReadiness,
    item: ControlItem,
) -> bool {
    match item {
        ControlItem::Output { pane_id, data, .. } if pane_id == target.pane_id => {
            run.record_output(data);
        }
        ControlItem::SessionChanged { session_id } if session_id == target.session_id => {
            if !readiness.session_seen {
                readiness.session_seen = true;
                let command = tmux::target_probe_command(&target.pane_id);
                if let Err(error) =
                    run.write_tmux_command(TmuxCommandKind::TargetProbe, command.as_bytes())
                {
                    return fail_tmux_control(
                        run,
                        ready,
                        backend_protocol_error("write initial tmux target probe", error),
                        InterruptionReason::TmuxServerUnavailable,
                    );
                }
            }
        }
        ControlItem::SessionChanged { .. } => {
            return fail_tmux_control(
                run,
                ready,
                ProtocolError::new(
                    ErrorCode::TargetChanged,
                    "tmux Control Mode client attached to a different session",
                ),
                InterruptionReason::TmuxTargetChanged,
            );
        }
        ControlItem::CommandResult {
            success, output, ..
        } => return handle_tmux_command_result(run, target, ready, readiness, success, &output),
        ControlItem::SessionRenamed { session_id, name } if session_id == target.session_id => {
            let _ = run.events.send(RunEvent::Tmux {
                event: TmuxRunEvent::SessionRenamed { name },
            });
        }
        ControlItem::WindowClosed { window_id } if window_id == target.window_id => {
            run.interrupt_tmux(InterruptionReason::TmuxTargetChanged);
            return false;
        }
        ControlItem::Paused { pane_id } if pane_id == target.pane_id => {
            let head_seq = run.mark_output_source_gap();
            let _ = run.events.send(RunEvent::Tmux {
                event: TmuxRunEvent::Paused,
            });
            let _ = run.events.send(RunEvent::Gap { head_seq });
            let command = format!("refresh-client -A {pane_id}:continue\n");
            if run
                .write_tmux_command(TmuxCommandKind::Continue, command.as_bytes())
                .is_err()
            {
                run.interrupt_tmux(InterruptionReason::TmuxServerUnavailable);
                return false;
            }
        }
        ControlItem::Continued { pane_id } if pane_id == target.pane_id => {
            let _ = run.events.send(RunEvent::Tmux {
                event: TmuxRunEvent::Continued,
            });
        }
        ControlItem::Exit => {
            run.interrupt_tmux(InterruptionReason::TmuxServerUnavailable);
            return false;
        }
        ControlItem::Output { .. }
        | ControlItem::Notification
        | ControlItem::SessionRenamed { .. }
        | ControlItem::WindowClosed { .. }
        | ControlItem::Paused { .. }
        | ControlItem::Continued { .. } => {}
    }
    true
}

fn handle_tmux_command_result(
    run: &Run,
    target: &ctxmux_protocol::TmuxPaneInfo,
    ready: &mpsc::SyncSender<Result<(), ProtocolError>>,
    readiness: &mut TmuxReadiness,
    success: bool,
    output: &[Vec<u8>],
) -> bool {
    let pending = run.take_tmux_command();
    if !success {
        return fail_tmux_control(
            run,
            ready,
            ProtocolError::new(
                ErrorCode::TargetChanged,
                format!(
                    "tmux pane {} no longer accepts adapter commands",
                    target.pane_id
                ),
            ),
            InterruptionReason::TmuxTargetChanged,
        );
    }
    match pending {
        Some(TmuxCommandKind::TargetProbe) => {
            if output.len() != 1 {
                return fail_tmux_control(
                    run,
                    ready,
                    ProtocolError::new(
                        ErrorCode::BackendUnavailable,
                        "tmux target probe returned an unexpected output shape",
                    ),
                    InterruptionReason::TmuxProtocolError,
                );
            }
            match tmux::target_identity_matches(target, &output[0]) {
                Ok(true) => {
                    if !readiness.ready {
                        readiness.ready = true;
                        let _ = ready.try_send(Ok(()));
                    }
                }
                Ok(false) => {
                    return fail_tmux_control(
                        run,
                        ready,
                        ProtocolError::new(
                            ErrorCode::TargetChanged,
                            "tmux target identity changed after import",
                        ),
                        InterruptionReason::TmuxTargetChanged,
                    );
                }
                Err(error) => {
                    return fail_tmux_control(
                        run,
                        ready,
                        backend_protocol_error("parse tmux target probe", error),
                        InterruptionReason::TmuxProtocolError,
                    );
                }
            }
        }
        Some(TmuxCommandKind::Continue) => {
            if !output.is_empty() {
                return fail_tmux_control(
                    run,
                    ready,
                    ProtocolError::new(
                        ErrorCode::BackendUnavailable,
                        "tmux continue command returned unexpected output",
                    ),
                    InterruptionReason::TmuxProtocolError,
                );
            }
        }
        None => {
            if readiness.ready {
                return fail_tmux_control(
                    run,
                    ready,
                    ProtocolError::new(
                        ErrorCode::BackendUnavailable,
                        "tmux returned a command result without a pending adapter command",
                    ),
                    InterruptionReason::TmuxProtocolError,
                );
            }
        }
    }
    true
}

fn fail_tmux_control(
    run: &Run,
    ready: &mpsc::SyncSender<Result<(), ProtocolError>>,
    error: ProtocolError,
    reason: InterruptionReason,
) -> bool {
    let _ = ready.try_send(Err(error));
    run.interrupt_tmux(reason);
    false
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

fn io_protocol_error(error: impl fmt::Display) -> ProtocolError {
    ProtocolError::new(ErrorCode::Io, error.to_string())
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
        return handle_attachment(wire, manager, id, after_seq).await;
    }
    let response = execute_request(&manager, request);
    match response {
        Ok(response) => send(&mut wire, &ServerFrame::Response { response }).await?,
        Err(error) => send(&mut wire, &ServerFrame::Error { error }).await?,
    }
    Ok(())
}

fn execute_request(manager: &RunManager, request: Request) -> Result<Response, ProtocolError> {
    match request {
        Request::Start { spec } => Ok(Response::Started {
            run: manager.start(spec)?,
        }),
        Request::DiscoverTmux { socket_path } => {
            let discovery = manager.discover_tmux(&socket_path)?;
            Ok(Response::TmuxPanes {
                tmux_version: discovery.version,
                panes: discovery.panes,
            })
        }
        Request::ImportTmux {
            socket_path,
            pane_id,
        } => Ok(Response::Imported {
            run: manager.import_tmux(&socket_path, &pane_id)?,
        }),
        Request::Fork { parent, plan } => Ok(Response::Forked {
            run: manager.fork(parent, plan)?,
        }),
        Request::List => Ok(Response::Runs {
            runs: manager.list(),
        }),
        Request::Status { id } => Ok(Response::Status {
            run: manager.get(id)?.info(),
        }),
        Request::Input { id, data } => Ok(Response::Accepted {
            run: manager.get(id)?.input(&data)?,
        }),
        Request::Resize { id, size } => Ok(Response::Accepted {
            run: manager.get(id)?.resize(size)?,
        }),
        Request::Stop { id } => Ok(Response::Accepted {
            run: manager.get(id)?.stop()?,
        }),
        Request::Attach { .. } => Err(ProtocolError::new(
            ErrorCode::Internal,
            "attach request reached short-lived request handler",
        )),
    }
}

async fn handle_attachment(
    mut wire: Framed<UnixStream, LinesCodec>,
    manager: Arc<RunManager>,
    id: RunId,
    after_seq: u64,
) -> Result<(), ConnectionError> {
    let run = match manager.get(id) {
        Ok(run) => run,
        Err(error) => {
            send(&mut wire, &ServerFrame::Error { error }).await?;
            return Ok(());
        }
    };
    let mut events = run.subscribe();
    #[cfg(test)]
    if let Some(hook) = &manager.attachment_hook {
        hook.pause_once(AttachmentHookPoint::AfterSubscribe).await;
    }
    let (_guard, snapshot) = run.attach(after_seq);
    let (header, replay_chunks, terminal_state) = split_attachment_snapshot(snapshot);
    let mut last_sent_seq = header.replay.head_seq;
    send(&mut wire, &ServerFrame::Attached { snapshot: header }).await?;
    for chunk in replay_chunks {
        send(
            &mut wire,
            &ServerFrame::Event {
                event: RunEvent::Output { chunk },
            },
        )
        .await?;
    }
    #[cfg(test)]
    if let Some(hook) = &manager.attachment_hook {
        hook.pause_once(AttachmentHookPoint::AfterSnapshot).await;
    }
    if !terminal_state.is_running() {
        send(
            &mut wire,
            &ServerFrame::Event {
                event: terminal_event(terminal_state),
            },
        )
        .await?;
        return Ok(());
    }

    loop {
        tokio::select! {
            incoming = receive(&mut wire) => {
                let Some(frame) = incoming? else {
                    return Ok(());
                };
                match frame {
                    ClientFrame::Input { data } => send_attachment_result(&mut wire, run.input(&data)).await?,
                    ClientFrame::Resize { size } => send_attachment_result(&mut wire, run.resize(size)).await?,
                    ClientFrame::Stop => send_attachment_result(&mut wire, run.stop()).await?,
                    ClientFrame::Detach => {
                        #[cfg(test)]
                        if let Some(hook) = &manager.attachment_hook {
                            hook.pause_once(AttachmentHookPoint::BeforeDetachAck).await;
                        }
                        send(&mut wire, &ServerFrame::Detached).await?;
                        return Ok(());
                    }
                    ClientFrame::Hello { .. } | ClientFrame::Request { .. } => {
                        send(&mut wire, &invalid_request("frame is not valid during attachment")).await?;
                    }
                }
            }
            event = events.recv() => {
                match event {
                    Ok(RunEvent::Output { chunk }) if chunk.seq <= last_sent_seq => {}
                    Ok(RunEvent::Output { chunk }) => {
                        last_sent_seq = chunk.seq;
                        send(&mut wire, &ServerFrame::Event {
                            event: RunEvent::Output { chunk },
                        }).await?;
                    }
                    Ok(event @ (RunEvent::Exited { .. } | RunEvent::Interrupted { .. })) => {
                        send(&mut wire, &ServerFrame::Event { event }).await?;
                        return Ok(());
                    }
                    Ok(event) => send(&mut wire, &ServerFrame::Event { event }).await?,
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        let head_seq = run.info().head_seq;
                        last_sent_seq = head_seq;
                        send(&mut wire, &ServerFrame::Event {
                            event: RunEvent::Gap { head_seq },
                        }).await?;
                    }
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                }
            }
        }
    }
}

fn terminal_event(state: RunState) -> RunEvent {
    match state {
        RunState::Interrupted { reason } => RunEvent::Interrupted { reason },
        state @ RunState::Exited { .. } => RunEvent::Exited { state },
        RunState::Running => unreachable!("running state is not terminal"),
    }
}

fn split_attachment_snapshot(
    snapshot: AttachedSnapshot,
) -> (AttachedHeader, Vec<OutputChunk>, RunState) {
    let AttachedSnapshot {
        run: run_info,
        replay,
    } = snapshot;
    let OutputReplay {
        chunks,
        oldest_seq,
        head_seq,
        truncated,
    } = replay;
    let terminal_state = run_info.state.clone();
    let header = AttachedHeader {
        run: run_info,
        replay: OutputReplayHeader {
            oldest_seq,
            head_seq,
            truncated,
        },
    };
    (header, chunks, terminal_state)
}

async fn send_attachment_result(
    wire: &mut Framed<UnixStream, LinesCodec>,
    result: Result<RunInfo, ProtocolError>,
) -> Result<(), ConnectionError> {
    match result {
        Ok(run) => {
            send(
                wire,
                &ServerFrame::Event {
                    event: RunEvent::Accepted { run: Box::new(run) },
                },
            )
            .await
        }
        Err(error) => send(wire, &ServerFrame::Error { error }).await,
    }
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
        collections::BTreeMap,
        fs,
        os::unix::{
            fs::{PermissionsExt, symlink},
            net::{UnixListener, UnixStream},
        },
        process::{Command, Stdio},
        sync::{Arc, atomic::AtomicBool},
        time::{Duration, Instant},
    };

    use ctxmux_client::{Client, ClientError, replay_bytes};
    use ctxmux_protocol::{ErrorCode, ForkPlan, RunEvent, RunSpec, TerminalSize};
    use tokio::sync::{Barrier, Notify, broadcast, mpsc};

    use super::{
        AttachmentHookPoint, AttachmentTestHook, LaunchSetupStep, OUTPUT_RETENTION_BYTES,
        OutputLog, Run, RunManager, ServerError, mutex_lock, prepare_socket_path,
        prepare_socket_path_with_hook, serve_with_manager, spawn_error,
    };

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
            let mut failed_pid = None;
            let error = manager
                .start_with_setup(long_running_spec(), |step, pid| {
                    if step == failed_step {
                        let pid = pid.expect("native child exposes its pid");
                        assert!(process_exists(pid), "fixture child must start live");
                        failed_pid = Some(pid);
                        return Err(spawn_error("complete injected setup step", "fixture"));
                    }
                    Ok(())
                })
                .expect_err("injected setup failure rejects start");

            assert_eq!(error.code, ErrorCode::SpawnFailed);
            assert!(manager.list().is_empty(), "failed start published a Run");
            let pid = failed_pid.expect("fixture records the rejected child pid");
            assert!(
                !process_exists(pid),
                "{failed_step:?} left child {pid} live or unreaped"
            );
        }
    }

    #[test]
    fn run_spec_semantics_map_to_invalid_request_for_start_fork_and_resize() {
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
        assert_eq!(resize_error.code, ErrorCode::InvalidRequest);
        assert_eq!(
            resize_error.message,
            "terminal rows and columns must be greater than zero"
        );
        run.stop().expect("stop validation fixture Run");
    }

    #[test]
    fn stop_after_wait_disables_signalling_before_state_publication() {
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
            .expect_err("reaped child rejects stop before state publication");
        assert_eq!(error.code, ErrorCode::InvalidRunState);
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
        _directory: tempfile::TempDir,
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
                _directory: directory,
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

        let (mut attachment, snapshot) = attaching
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

        let (mut attachment, snapshot) = server
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
                        MutationOperation::Input(byte) => client.input(id, vec![byte]).await,
                        MutationOperation::Resize(cols) => {
                            client.resize(id, TerminalSize { cols, rows: 24 }).await
                        }
                        MutationOperation::Stop => client.stop(id).await,
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
                match (operation, result) {
                    (MutationOperation::Stop, Ok(_)) => accepted_stops += 1,
                    (
                        MutationOperation::Stop,
                        Err(ClientError::Protocol {
                            code: ErrorCode::InvalidRunState,
                            ..
                        }),
                    ) => rejected_stops += 1,
                    (
                        MutationOperation::Input(_) | MutationOperation::Resize(_),
                        Ok(_)
                        | Err(ClientError::Protocol {
                            code: ErrorCode::InvalidRunState | ErrorCode::Io,
                            ..
                        }),
                    ) => {}
                    (operation, result) => panic!(
                        "seed {seed} case {case_index}: undeclared {operation:?} result {result:?}"
                    ),
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

        let (mut lagged_attachment, initial) = client
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
