//! Long-lived native Run owner and local protocol server.

#[cfg(not(unix))]
compile_error!("the first ctxmux native transport currently requires Unix sockets");

use std::{
    collections::{HashMap, VecDeque},
    fs,
    io::{self, Read, Write},
    os::unix::{
        fs::{FileTypeExt, PermissionsExt},
        net::UnixStream as StdUnixStream,
    },
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::Duration,
};

use ctxmux_protocol::{
    AttachedSnapshot, ClientFrame, ErrorCode, ForkFidelity, ForkPlan, MAX_FRAME_BYTES, OutputChunk,
    OutputReplay, PROTOCOL_VERSION, ProtocolError, Request, Response, RunEvent, RunId, RunInfo,
    RunLineage, RunSpec, RunState, ServerFrame, TerminalSize, decode_frame, encode_frame,
};
use futures_util::{SinkExt, StreamExt};
use portable_pty::{Child, ChildKiller, CommandBuilder, MasterPty, PtySize, native_pty_system};
use thiserror::Error;
use tokio::{
    net::{UnixListener, UnixStream},
    sync::broadcast,
};
use tokio_util::codec::{Framed, LinesCodec, LinesCodecError};

const OUTPUT_RETENTION_BYTES: usize = 4 * 1024 * 1024;
const OUTPUT_READ_BUFFER_BYTES: usize = 8192;
const LIVE_EVENT_CAPACITY: usize = 256;

/// Failure that prevents the daemon server from running.
#[derive(Debug, Error)]
pub enum ServerError {
    /// The requested path exists but is not a Unix socket.
    #[error("refusing to replace non-socket path: {0}")]
    InvalidSocketTarget(PathBuf),
    /// Another daemon is already accepting connections at this path.
    #[error("a ctxmux daemon is already listening at {0}")]
    AlreadyRunning(PathBuf),
    /// A platform I/O operation failed.
    #[error("ctxmux daemon I/O failed at {path}: {source}")]
    Io {
        /// Path involved in the failure.
        path: PathBuf,
        /// Platform I/O failure.
        #[source]
        source: io::Error,
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
/// already listening, or the local listener cannot be created or operated.
pub async fn serve(socket_path: impl Into<PathBuf>) -> Result<(), ServerError> {
    let socket_path = socket_path.into();
    prepare_socket_path(&socket_path)?;
    let listener =
        UnixListener::bind(&socket_path).map_err(|source| ServerError::io(&socket_path, source))?;
    fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600))
        .map_err(|source| ServerError::io(&socket_path, source))?;
    let _socket_guard = SocketGuard(socket_path.clone());
    let manager = Arc::new(RunManager::default());

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
                return Ok(());
            }
        }
    }
}

fn prepare_socket_path(path: &Path) -> Result<(), ServerError> {
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
    fs::remove_file(path).map_err(|source| ServerError::io(path, source))
}

struct SocketGuard(PathBuf);

impl Drop for SocketGuard {
    fn drop(&mut self) {
        let Ok(metadata) = fs::symlink_metadata(&self.0) else {
            return;
        };
        if metadata.file_type().is_socket() {
            let _ = fs::remove_file(&self.0);
        }
    }
}

#[derive(Default)]
struct RunManager {
    runs: RwLock<HashMap<RunId, Arc<Run>>>,
}

impl RunManager {
    fn start(&self, spec: RunSpec) -> Result<RunInfo, ProtocolError> {
        let run = Run::spawn(spec, None)?;
        let info = run.info();
        write_lock(&self.runs).insert(info.id, run);
        Ok(info)
    }

    fn fork(&self, parent: RunId, plan: ForkPlan) -> Result<RunInfo, ProtocolError> {
        let parent_run = self.get(parent)?;
        let (spec, fidelity) = match plan {
            ForkPlan::LevelA => (parent_run.spec.clone(), ForkFidelity::LevelA),
            ForkPlan::LevelB { spec } => (spec, ForkFidelity::LevelB),
        };
        let run = Run::spawn(spec, Some(RunLineage { parent, fidelity }))?;
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
    spec: RunSpec,
    lineage: Option<RunLineage>,
    pid: Option<u32>,
    state: Mutex<RunState>,
    output: Mutex<OutputLog>,
    master: Mutex<Box<dyn MasterPty + Send>>,
    writer: Mutex<Box<dyn Write + Send>>,
    killer: Mutex<Box<dyn ChildKiller + Send + Sync>>,
    attachments: AtomicUsize,
    events: broadcast::Sender<RunEvent>,
}

impl Run {
    fn spawn(spec: RunSpec, lineage: Option<RunLineage>) -> Result<Arc<Self>, ProtocolError> {
        Self::spawn_with_setup(spec, lineage, |_, _| Ok(()))
    }

    fn spawn_with_setup<F>(
        spec: RunSpec,
        lineage: Option<RunLineage>,
        mut setup: F,
    ) -> Result<Arc<Self>, ProtocolError>
    where
        F: FnMut(LaunchSetupStep, Option<u32>) -> Result<(), ProtocolError>,
    {
        validate_spec(&spec)?;
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
        let killer = pending_child.child().clone_killer();
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
        let (events, _) = broadcast::channel(LIVE_EVENT_CAPACITY);
        let run = Arc::new(Self {
            id: RunId::new(),
            spec,
            lineage,
            pid,
            state: Mutex::new(RunState::Running),
            output: Mutex::new(OutputLog::default()),
            master: Mutex::new(pair.master),
            writer: Mutex::new(writer),
            killer: Mutex::new(killer),
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
                let state = match child.wait() {
                    Ok(status) => RunState::Exited {
                        code: status.exit_code(),
                        signal: status.signal().map(str::to_owned),
                    },
                    Err(error) => RunState::Exited {
                        code: 1,
                        signal: Some(format!("wait failed: {error}")),
                    },
                };
                let _ = output_done_rx.recv_timeout(Duration::from_secs(1));
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

    fn info(&self) -> RunInfo {
        let output = mutex_lock(&self.output);
        RunInfo {
            id: self.id,
            spec: self.spec.clone(),
            lineage: self.lineage.clone(),
            pid: self.pid,
            state: mutex_lock(&self.state).clone(),
            head_seq: output.head_seq(),
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
                format!("cannot {operation} exited Run {}", self.id),
            ))
        }
    }

    fn input(&self, data: &[u8]) -> Result<RunInfo, ProtocolError> {
        self.ensure_running("write to")?;
        let mut writer = mutex_lock(&self.writer);
        writer.write_all(data).map_err(io_protocol_error)?;
        writer.flush().map_err(io_protocol_error)?;
        Ok(self.info())
    }

    fn resize(&self, size: TerminalSize) -> Result<RunInfo, ProtocolError> {
        validate_size(size)?;
        self.ensure_running("resize")?;
        mutex_lock(&self.master)
            .resize(to_pty_size(size))
            .map_err(io_protocol_error)?;
        Ok(self.info())
    }

    fn stop(&self) -> Result<RunInfo, ProtocolError> {
        self.ensure_running("stop")?;
        mutex_lock(&self.killer).kill().map_err(io_protocol_error)?;
        Ok(self.info())
    }

    fn record_output(&self, data: Vec<u8>) {
        let chunk = mutex_lock(&self.output).push(data);
        let _ = self.events.send(RunEvent::Output { chunk });
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
}

impl OutputLog {
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
            truncated: oldest_seq > 0 && after_seq.saturating_add(1) < oldest_seq,
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

fn validate_spec(spec: &RunSpec) -> Result<(), ProtocolError> {
    if spec.program.is_empty() {
        return Err(ProtocolError::new(
            ErrorCode::InvalidRequest,
            "Run program must not be empty",
        ));
    }
    if spec
        .declared_inputs
        .iter()
        .any(|input| input.reference.is_empty())
    {
        return Err(ProtocolError::new(
            ErrorCode::InvalidRequest,
            "Run input references must not be empty",
        ));
    }
    validate_size(spec.size)
}

fn validate_size(size: TerminalSize) -> Result<(), ProtocolError> {
    if size.cols == 0 || size.rows == 0 {
        return Err(ProtocolError::new(
            ErrorCode::InvalidRequest,
            "terminal rows and columns must be greater than zero",
        ));
    }
    Ok(())
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
    let (_guard, snapshot) = run.attach(after_seq);
    let mut last_sent_seq = snapshot.replay.head_seq;
    let terminal_state = snapshot.run.state.clone();
    send(&mut wire, &ServerFrame::Attached { snapshot }).await?;
    if !terminal_state.is_running() {
        send(
            &mut wire,
            &ServerFrame::Event {
                event: RunEvent::Exited {
                    state: terminal_state,
                },
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
                    Ok(event @ RunEvent::Exited { .. }) => {
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

async fn send_attachment_result(
    wire: &mut Framed<UnixStream, LinesCodec>,
    result: Result<RunInfo, ProtocolError>,
) -> Result<(), ConnectionError> {
    match result {
        Ok(run) => {
            send(
                wire,
                &ServerFrame::Event {
                    event: RunEvent::Accepted { run },
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
            net::UnixListener,
        },
        process::{Command, Stdio},
    };

    use ctxmux_protocol::{ErrorCode, RunSpec, TerminalSize};
    use tokio::sync::broadcast;

    use super::{
        LaunchSetupStep, OUTPUT_RETENTION_BYTES, OutputLog, RunManager, ServerError,
        prepare_socket_path, spawn_error,
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
        drop(UnixListener::bind(&stale).expect("bind stale socket fixture"));

        prepare_socket_path(&stale).expect("remove inactive socket");
        assert!(!stale.exists());
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
