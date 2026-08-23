//! Rust client for the versioned ctxmux local protocol.

#[cfg(not(unix))]
compile_error!("the first ctxmux native transport currently requires Unix sockets");

mod attachment;

pub use attachment::Attachment;

use std::{
    io,
    path::{Path, PathBuf},
};

use ctxmux_protocol::{
    AppliedInputRange, AttachedSnapshot, AttachmentCommandId, ClientFrame, ClientHello,
    CommandDisposition, ControlFailure, ControlReceipt, CreateOperationKey, DaemonInstanceId,
    ForkPlan, FrameError, MAX_FRAME_BYTES, OutputChunk, OutputReplay, PROTOCOL_VERSION,
    ProtocolError, RUNTIME_CAPABILITY_MANIFEST_VERSION, RecoverableInput, Request, Response,
    RunEvent, RunId, RunInfo, RunSignal, RunSpec, RuntimeDescription, ServerFrame, StopDisposition,
    TerminalSize, TmuxPaneInfo, decode_frame, encode_frame,
};
use futures_util::{SinkExt, StreamExt};
use thiserror::Error;
use tokio::net::UnixStream;
use tokio_util::codec::{Framed, LinesCodec, LinesCodecError};

type Wire = Framed<UnixStream, LinesCodec>;

/// Failure observed at the public Rust client boundary.
#[derive(Debug, Error)]
pub enum ClientError {
    /// The daemon socket could not be reached.
    #[error("failed to connect to ctxmux daemon at {path}: {source}")]
    Connect {
        /// Requested socket path.
        path: PathBuf,
        /// Platform I/O failure.
        #[source]
        source: io::Error,
    },
    /// The socket closed before the expected frame arrived.
    #[error("ctxmux daemon closed the connection")]
    Closed,
    /// The JSON-lines transport failed.
    #[error("ctxmux transport failed: {0}")]
    Transport(#[from] LinesCodecError),
    /// A frame could not be encoded or decoded.
    #[error(transparent)]
    Frame(#[from] FrameError),
    /// The daemon explicitly rejected the operation.
    #[error("ctxmux request failed ({code:?}): {message}")]
    Protocol {
        /// Stable machine-readable category.
        code: ctxmux_protocol::ErrorCode,
        /// Human-readable detail.
        message: String,
    },
    /// A correlated control request was rejected with a known application
    /// boundary.
    #[error("ctxmux control request was rejected: {failure:?}")]
    ControlRejected {
        /// Typed daemon failure, including whether the command may have been
        /// applied.
        failure: ControlFailure,
    },
    /// A short-lived control request was sent but its unique correlated result
    /// was not proven.
    #[error("ctxmux short-lived control request has unknown disposition: {source}")]
    ControlRequestUnknown {
        /// Transport or contract failure that prevented a unique result.
        #[source]
        source: Box<ClientError>,
    },
    /// A local control preflight failed before the command reached transport.
    #[error("ctxmux control command was not applied: {source}")]
    ControlNotApplied {
        /// Local connection or encoding failure observed before send.
        #[source]
        source: Box<ClientError>,
    },
    /// The client could not admit another attachment command without
    /// violating its local hard bounds.
    #[error("ctxmux attachment command backpressure: {limit}")]
    AttachmentBackpressure {
        /// Bound that rejected the command before an ID was allocated.
        limit: &'static str,
    },
    /// The attachment no longer accepts new commands.
    #[error("ctxmux attachment does not accept new commands: {reason}")]
    AttachmentUnavailable {
        /// Stable local reason for fencing new commands.
        reason: AttachmentUnavailableReason,
    },
    /// An attachment command lost its unique result and may have crossed its
    /// owner boundary.
    #[error("ctxmux attachment command {command_id:?} has unknown disposition: {reason}")]
    AttachmentCommandUnknown {
        /// Connection-local command correlation identity.
        command_id: AttachmentCommandId,
        /// Why the client cannot prove the command result.
        reason: AttachmentUnknownReason,
    },
    /// More than one caller tried to await the same attachment event stream.
    #[error("only one Attachment::next_event call may be active at a time")]
    ConcurrentEventRead,
    /// The daemon violated a control receipt or attachment delivery invariant.
    #[error("ctxmux protocol contract violated: {0}")]
    ProtocolContractViolation(&'static str),
    /// The daemon returned a valid frame in the wrong protocol state.
    #[error("unexpected ctxmux frame: {0}")]
    UnexpectedFrame(&'static str),
}

impl ClientError {
    /// Return the known application disposition for a failed control command.
    ///
    /// `None` means the error is not itself a correlated control outcome.
    #[must_use]
    pub const fn control_disposition(&self) -> Option<CommandDisposition> {
        match self {
            Self::ControlRejected { failure } => Some(failure.disposition),
            Self::ControlRequestUnknown { .. } | Self::AttachmentCommandUnknown { .. } => {
                Some(CommandDisposition::Unknown)
            }
            Self::ControlNotApplied { .. }
            | Self::AttachmentBackpressure { .. }
            | Self::AttachmentUnavailable { .. } => Some(CommandDisposition::NotApplied),
            Self::Connect { .. }
            | Self::Closed
            | Self::Transport(_)
            | Self::Frame(_)
            | Self::Protocol { .. }
            | Self::ConcurrentEventRead
            | Self::ProtocolContractViolation(_)
            | Self::UnexpectedFrame(_) => None,
        }
    }
}

/// Why an Attachment no longer admits commands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum AttachmentUnavailableReason {
    /// A clean detach has started.
    #[error("clean detach is in progress")]
    Detaching,
    /// The connection has terminated or was closed locally.
    #[error("the attachment is closed")]
    Closed,
    /// The connection-local u32 command-id space was consumed.
    #[error("the attachment command-id space is exhausted")]
    CommandIdsExhausted,
}

/// Why a sent or ambiguously sent attachment command has no unique result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum AttachmentUnknownReason {
    /// The transport ended before the unique result arrived.
    #[error("the attachment transport terminated")]
    TransportTerminated,
    /// A daemon frame violated correlation or receipt invariants.
    #[error("the daemon violated the attachment protocol")]
    ProtocolViolation,
    /// The caller abruptly closed the attachment.
    #[error("the attachment was closed locally")]
    ClosedLocally,
}

/// Typed receipt for one complete PTY input write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InputReceipt {
    /// Bytes that reached the daemon-owned PTY write boundary.
    pub written_bytes: u32,
}

/// Typed receipt for one applied PTY resize.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResizeReceipt {
    /// Size read back from the owning PTY.
    pub applied_size: TerminalSize,
}

/// Typed receipt that one portable signal reached the native lifecycle owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SignalReceipt {
    /// Exact signal delivered by the daemon.
    pub signal: RunSignal,
}

/// Typed receipt that the complete native Run session reached quiescence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StopReceipt {
    /// Whether the graceful phase was sufficient or force was required.
    pub disposition: StopDisposition,
}

/// One accepted short-lived control request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlAccepted<R> {
    /// Current Run metadata returned with the owner receipt.
    pub run: RunInfo,
    /// Operation-specific owner-boundary receipt.
    pub receipt: R,
}

/// One accepted command on a persistent attachment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttachmentControlAccepted<R> {
    /// Connection-local command correlation identity.
    pub command_id: AttachmentCommandId,
    /// Operation-specific owner-boundary receipt.
    pub receipt: R,
}

impl From<ProtocolError> for ClientError {
    fn from(error: ProtocolError) -> Self {
        Self::Protocol {
            code: error.code,
            message: error.message,
        }
    }
}

/// Stateless connector to one local ctxmux daemon.
#[derive(Debug, Clone)]
pub struct Client {
    socket_path: PathBuf,
}

impl Client {
    /// Target one explicit daemon socket.
    #[must_use]
    pub fn new(socket_path: impl Into<PathBuf>) -> Self {
        Self {
            socket_path: socket_path.into(),
        }
    }

    /// Socket path used by this connector.
    #[must_use]
    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    /// Verify that the daemon is reachable and protocol-compatible.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] when the socket cannot be reached or the
    /// handshake is incompatible.
    pub async fn ping(&self) -> Result<(), ClientError> {
        self.connect().await.map(|_| ())
    }

    /// Return the Provider-neutral description of the reachable Runtime.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] when the handshake cannot complete or reports
    /// an incompatible protocol generation.
    pub async fn runtime_info(&self) -> Result<RuntimeDescription, ClientError> {
        self.connect().await.map(|(_, runtime)| runtime)
    }

    /// Return the identity of the currently reachable daemon incarnation.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] when the handshake cannot complete.
    pub async fn daemon_instance(&self) -> Result<DaemonInstanceId, ClientError> {
        self.runtime_info()
            .await
            .map(|runtime| runtime.daemon_instance_id)
    }

    /// Start one daemon-owned native Run.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] when transport, framing, or process creation
    /// fails.
    pub async fn start(&self, spec: RunSpec) -> Result<RunInfo, ClientError> {
        self.start_with_operation_key(spec, CreateOperationKey::random())
            .await
    }

    /// Start one daemon-owned native Run with a caller-retained retry key.
    ///
    /// Reusing the key with the same specification converges on the original
    /// Run while that Run is retained. Reusing it with another request returns
    /// a typed creation conflict.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] when transport, framing, key validation, or
    /// process creation fails.
    pub async fn start_with_operation_key(
        &self,
        spec: RunSpec,
        operation_key: CreateOperationKey,
    ) -> Result<RunInfo, ClientError> {
        match self
            .request(Request::Start {
                operation_key,
                spec,
            })
            .await?
        {
            Response::Started { run } => Ok(run),
            _ => Err(ClientError::UnexpectedFrame("expected started response")),
        }
    }

    /// Discover existing panes from one explicit tmux server socket.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] when the daemon or selected tmux server is
    /// unavailable, incompatible, or returns an invalid response.
    pub async fn discover_tmux(
        &self,
        socket_path: impl Into<String>,
    ) -> Result<(String, Vec<TmuxPaneInfo>), ClientError> {
        match self
            .request(Request::DiscoverTmux {
                socket_path: socket_path.into(),
            })
            .await?
        {
            Response::TmuxPanes {
                tmux_version,
                panes,
            } => Ok((tmux_version, panes)),
            _ => Err(ClientError::UnexpectedFrame("expected tmux panes response")),
        }
    }

    /// Import one existing tmux pane as a read-only observable Run.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] when the daemon rejects the pane, the tmux
    /// target changes, or Control Mode cannot establish observation.
    pub async fn import_tmux(
        &self,
        socket_path: impl Into<String>,
        pane_id: impl Into<String>,
    ) -> Result<RunInfo, ClientError> {
        match self
            .request(Request::ImportTmux {
                socket_path: socket_path.into(),
                pane_id: pane_id.into(),
            })
            .await?
        {
            Response::Imported { run } => Ok(run),
            _ => Err(ClientError::UnexpectedFrame("expected imported response")),
        }
    }

    /// Create one child Run from an explicit fidelity plan.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] when the parent does not exist or the selected
    /// plan cannot create a child.
    pub async fn fork(&self, parent: RunId, plan: ForkPlan) -> Result<RunInfo, ClientError> {
        self.fork_with_operation_key(parent, plan, CreateOperationKey::random())
            .await
    }

    /// Create one child Run with a caller-retained retry key.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] when the parent, plan, key, transport, or
    /// process creation boundary rejects the operation.
    pub async fn fork_with_operation_key(
        &self,
        parent: RunId,
        plan: ForkPlan,
        operation_key: CreateOperationKey,
    ) -> Result<RunInfo, ClientError> {
        match self
            .request(Request::Fork {
                operation_key,
                parent,
                plan,
            })
            .await?
        {
            Response::Forked { run } => Ok(run),
            _ => Err(ClientError::UnexpectedFrame("expected forked response")),
        }
    }

    /// List Runs retained by the daemon.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] when the daemon cannot be reached or returns an
    /// invalid response.
    pub async fn list(&self) -> Result<Vec<RunInfo>, ClientError> {
        match self.request(Request::List).await? {
            Response::Runs { runs } => Ok(runs),
            _ => Err(ClientError::UnexpectedFrame("expected runs response")),
        }
    }

    /// Read current metadata for one Run.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] when the Run does not exist or the request
    /// cannot complete.
    pub async fn status(&self, id: RunId) -> Result<RunInfo, ClientError> {
        match self.request(Request::Status { id }).await? {
            Response::Status { run } => Ok(run),
            _ => Err(ClientError::UnexpectedFrame("expected status response")),
        }
    }

    /// Write bytes to one live Run.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] when the Run is not live or PTY input fails.
    pub async fn input(
        &self,
        id: RunId,
        data: Vec<u8>,
    ) -> Result<ControlAccepted<InputReceipt>, ClientError> {
        let expected_bytes = data.len();
        decode_short_control(
            self.control_request(Request::Input { id, data }).await?,
            |receipt| decode_input_receipt(receipt, expected_bytes),
        )
    }

    /// Execute or recover one caller-retained native Input operation.
    ///
    /// The operation retains its original daemon instance across retries. A
    /// replacement daemon rejects it before Run lookup or PTY mutation.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] when the request is rejected or no unique
    /// result can be received.
    pub async fn recoverable_input(
        &self,
        operation: RecoverableInput,
    ) -> Result<ControlAccepted<AppliedInputRange>, ClientError> {
        let expected_byte = operation.expected_byte;
        let expected_run = operation.id;
        let expected_end = expected_byte
            .checked_add(u64::try_from(operation.data.len()).map_err(|_| {
                control_not_applied(ClientError::ProtocolContractViolation(
                    "recoverable Input payload length does not fit u64",
                ))
            })?)
            .ok_or_else(|| {
                control_not_applied(ClientError::ProtocolContractViolation(
                    "recoverable Input expected cursor overflows",
                ))
            })?;
        let request = Request::RecoverableInput { operation };
        match self.control_request(request).await? {
            Response::InputApplied { run, range }
                if run.id == expected_run
                    && range.start_byte == expected_byte
                    && range.end_byte == expected_end
                    && run
                        .applied_input_bytes
                        .is_some_and(|cursor| cursor >= range.end_byte) =>
            {
                Ok(ControlAccepted {
                    run,
                    receipt: range,
                })
            }
            Response::InputApplied { .. } => Err(control_request_unknown(
                ClientError::ProtocolContractViolation(
                    "recoverable Input Run, range, or cursor does not prove its request",
                ),
            )),
            Response::ControlRejected { failure } => {
                validate_control_failure(&failure)
                    .map_err(ClientError::ProtocolContractViolation)
                    .map_err(control_request_unknown)?;
                Err(ClientError::ControlRejected { failure })
            }
            _ => Err(control_request_unknown(ClientError::UnexpectedFrame(
                "expected recoverable Input result",
            ))),
        }
    }

    /// Resize one live Run.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] when the size is invalid, the Run is not live,
    /// or the PTY resize fails.
    pub async fn resize(
        &self,
        id: RunId,
        size: TerminalSize,
    ) -> Result<ControlAccepted<ResizeReceipt>, ClientError> {
        decode_short_control(
            self.control_request(Request::Resize { id, size }).await?,
            decode_resize_receipt,
        )
    }

    /// Interrupt the current foreground process group without stopping Run ownership.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] when the Run cannot receive a native signal or
    /// the owner cannot prove delivery.
    pub async fn interrupt(
        &self,
        id: RunId,
    ) -> Result<ControlAccepted<SignalReceipt>, ClientError> {
        decode_short_control(
            self.control_request(Request::Signal {
                id,
                signal: RunSignal::Interrupt,
            })
            .await?,
            |receipt| decode_signal_receipt(receipt, RunSignal::Interrupt),
        )
    }

    /// Terminate one live Run.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] when the Run is not live or termination fails.
    pub async fn stop(&self, id: RunId) -> Result<ControlAccepted<StopReceipt>, ClientError> {
        decode_short_control(
            self.control_request(Request::Stop { id }).await?,
            decode_stop_receipt,
        )
    }

    /// Attach after the cumulative output byte cursor already observed by the caller.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] when the Run does not exist, the handshake
    /// fails, or the attachment snapshot cannot be read.
    pub async fn attach(
        &self,
        id: RunId,
        after_byte: u64,
    ) -> Result<(Attachment, AttachedSnapshot), ClientError> {
        let (mut wire, _) = self.connect().await?;
        send(
            &mut wire,
            &ClientFrame::Request {
                request: Request::Attach { id, after_byte },
            },
        )
        .await?;

        match receive(&mut wire).await? {
            ServerFrame::Attached { snapshot: header } => {
                let mut snapshot = AttachedSnapshot {
                    run: header.run,
                    replay: OutputReplay {
                        chunks: Vec::new(),
                        first_available_byte: header.replay.first_available_byte,
                        latest_output_bytes: header.replay.latest_output_bytes,
                        truncated: header.replay.truncated,
                    },
                };
                receive_replay(&mut wire, after_byte, &mut snapshot).await?;
                Ok((Attachment::from_wire(wire), snapshot))
            }
            ServerFrame::Error { error } => Err(error.into()),
            _ => Err(ClientError::UnexpectedFrame("expected attached snapshot")),
        }
    }

    async fn request(&self, request: Request) -> Result<Response, ClientError> {
        let (mut wire, _) = self.connect().await?;
        send(&mut wire, &ClientFrame::Request { request }).await?;
        match receive(&mut wire).await? {
            ServerFrame::Response { response } => Ok(response),
            ServerFrame::Error { error } => Err(error.into()),
            _ => Err(ClientError::UnexpectedFrame("expected request response")),
        }
    }

    async fn control_request(&self, request: Request) -> Result<Response, ClientError> {
        let (mut wire, _) = self.connect().await.map_err(control_not_applied)?;
        let encoded = encode_frame(&ClientFrame::Request { request })
            .map_err(ClientError::Frame)
            .map_err(control_not_applied)?;
        send_encoded_sink(&mut wire, encoded)
            .await
            .map_err(control_request_unknown)?;
        let frame = receive(&mut wire).await.map_err(control_request_unknown)?;
        match frame {
            ServerFrame::Response { response } => Ok(response),
            ServerFrame::Error { error } => Err(control_request_unknown(error.into())),
            _ => Err(control_request_unknown(ClientError::UnexpectedFrame(
                "expected correlated control response",
            ))),
        }
    }

    async fn connect(&self) -> Result<(Wire, RuntimeDescription), ClientError> {
        let stream = UnixStream::connect(&self.socket_path)
            .await
            .map_err(|source| ClientError::Connect {
                path: self.socket_path.clone(),
                source,
            })?;
        let mut wire = Framed::new(stream, codec());
        send(
            &mut wire,
            &ClientFrame::Hello {
                hello: ClientHello {
                    protocol: PROTOCOL_VERSION,
                },
            },
        )
        .await?;
        match receive(&mut wire).await? {
            ServerFrame::Hello { runtime } => {
                validate_runtime_description(&runtime)?;
                Ok((wire, runtime))
            }
            ServerFrame::Error { error } => Err(error.into()),
            _ => Err(ClientError::UnexpectedFrame("expected compatible hello")),
        }
    }
}

fn validate_runtime_description(runtime: &RuntimeDescription) -> Result<(), ClientError> {
    if runtime.protocol_generation != PROTOCOL_VERSION
        || runtime.capabilities.version != RUNTIME_CAPABILITY_MANIFEST_VERSION
    {
        return Err(ClientError::UnexpectedFrame("expected compatible hello"));
    }
    Ok(())
}

fn decode_short_control<R>(
    response: Response,
    decode_receipt: impl FnOnce(&ControlReceipt) -> Result<R, ClientError>,
) -> Result<ControlAccepted<R>, ClientError> {
    match response {
        Response::ControlAccepted { run, receipt } => decode_receipt(&receipt)
            .map(|receipt| ControlAccepted { run, receipt })
            .map_err(control_request_unknown),
        Response::ControlRejected { failure } => {
            validate_control_failure(&failure)
                .map_err(ClientError::ProtocolContractViolation)
                .map_err(control_request_unknown)?;
            Err(ClientError::ControlRejected { failure })
        }
        _ => Err(control_request_unknown(ClientError::UnexpectedFrame(
            "expected correlated control response",
        ))),
    }
}

fn control_request_unknown(source: ClientError) -> ClientError {
    ClientError::ControlRequestUnknown {
        source: Box::new(source),
    }
}

fn control_not_applied(source: ClientError) -> ClientError {
    ClientError::ControlNotApplied {
        source: Box::new(source),
    }
}

fn validate_control_failure(failure: &ControlFailure) -> Result<(), &'static str> {
    if failure.error.code == ctxmux_protocol::ErrorCode::ControlBackpressure
        && failure.disposition != CommandDisposition::NotApplied
    {
        return Err("control_backpressure must have not_applied disposition");
    }
    Ok(())
}

fn decode_input_receipt(
    receipt: &ControlReceipt,
    expected_bytes: usize,
) -> Result<InputReceipt, ClientError> {
    match receipt {
        ControlReceipt::Input { written_bytes }
            if usize::try_from(*written_bytes).ok() == Some(expected_bytes) =>
        {
            Ok(InputReceipt {
                written_bytes: *written_bytes,
            })
        }
        ControlReceipt::Input { .. } => Err(ClientError::ProtocolContractViolation(
            "input receipt byte count differs from the command payload",
        )),
        ControlReceipt::Resize { .. }
        | ControlReceipt::Signal { .. }
        | ControlReceipt::Stop { .. } => Err(ClientError::ProtocolContractViolation(
            "input returned another receipt kind",
        )),
    }
}

fn decode_resize_receipt(receipt: &ControlReceipt) -> Result<ResizeReceipt, ClientError> {
    match receipt {
        ControlReceipt::Resize { applied_size }
            if applied_size.cols != 0 && applied_size.rows != 0 =>
        {
            Ok(ResizeReceipt {
                applied_size: *applied_size,
            })
        }
        ControlReceipt::Resize { .. } => Err(ClientError::ProtocolContractViolation(
            "resize receipt reported a zero applied dimension",
        )),
        ControlReceipt::Input { .. }
        | ControlReceipt::Signal { .. }
        | ControlReceipt::Stop { .. } => Err(ClientError::ProtocolContractViolation(
            "resize returned another receipt kind",
        )),
    }
}

fn decode_stop_receipt(receipt: &ControlReceipt) -> Result<StopReceipt, ClientError> {
    match receipt {
        ControlReceipt::Stop { disposition } => Ok(StopReceipt {
            disposition: *disposition,
        }),
        ControlReceipt::Input { .. }
        | ControlReceipt::Resize { .. }
        | ControlReceipt::Signal { .. } => Err(ClientError::ProtocolContractViolation(
            "stop returned another receipt kind",
        )),
    }
}

fn decode_signal_receipt(
    receipt: &ControlReceipt,
    expected: RunSignal,
) -> Result<SignalReceipt, ClientError> {
    match receipt {
        ControlReceipt::Signal { signal } if *signal == expected => {
            Ok(SignalReceipt { signal: *signal })
        }
        ControlReceipt::Signal { .. } => Err(ClientError::ProtocolContractViolation(
            "signal receipt differs from the requested signal",
        )),
        ControlReceipt::Input { .. }
        | ControlReceipt::Resize { .. }
        | ControlReceipt::Stop { .. } => Err(ClientError::ProtocolContractViolation(
            "signal returned another receipt kind",
        )),
    }
}

async fn receive_replay<S>(
    wire: &mut S,
    after_byte: u64,
    snapshot: &mut AttachedSnapshot,
) -> Result<(), ClientError>
where
    S: futures_util::Stream<Item = Result<String, LinesCodecError>> + Unpin,
{
    if snapshot
        .replay
        .chunks
        .last()
        .is_some_and(|chunk| chunk.end_byte == snapshot.replay.latest_output_bytes)
        || (snapshot.replay.chunks.is_empty() && after_byte >= snapshot.replay.latest_output_bytes)
    {
        return Ok(());
    }
    let mut expected_byte = snapshot.replay.chunks.last().map_or_else(
        || after_byte.max(snapshot.replay.first_available_byte),
        |chunk| chunk.end_byte,
    );
    loop {
        match receive_optional(wire).await?.ok_or(ClientError::Closed)? {
            ServerFrame::Event {
                event: RunEvent::Output { chunk },
            } if chunk.start_byte == expected_byte
                && chunk.end_byte > chunk.start_byte
                && chunk.end_byte <= snapshot.replay.latest_output_bytes
                && chunk.end_byte - chunk.start_byte
                    == u64::try_from(chunk.data.len()).unwrap_or(u64::MAX) =>
            {
                let complete = chunk.end_byte == snapshot.replay.latest_output_bytes;
                expected_byte = chunk.end_byte;
                snapshot.replay.chunks.push(chunk);
                if complete {
                    return Ok(());
                }
            }
            ServerFrame::Error { error } => return Err(error.into()),
            _ => {
                return Err(ClientError::UnexpectedFrame(
                    "expected ordered replay output",
                ));
            }
        }
    }
}

fn codec() -> LinesCodec {
    LinesCodec::new_with_max_length(MAX_FRAME_BYTES)
}

async fn send(wire: &mut Wire, frame: &ClientFrame) -> Result<(), ClientError> {
    send_sink(wire, frame).await
}

async fn send_sink<S>(sink: &mut S, frame: &ClientFrame) -> Result<(), ClientError>
where
    S: futures_util::Sink<String, Error = LinesCodecError> + Unpin,
{
    send_encoded_sink(sink, encode_frame(frame)?).await
}

async fn send_encoded_sink<S>(sink: &mut S, encoded: String) -> Result<(), ClientError>
where
    S: futures_util::Sink<String, Error = LinesCodecError> + Unpin,
{
    sink.send(encoded).await?;
    Ok(())
}

async fn receive(wire: &mut Wire) -> Result<ServerFrame, ClientError> {
    receive_optional(wire).await?.ok_or(ClientError::Closed)
}

async fn receive_optional<S>(stream: &mut S) -> Result<Option<ServerFrame>, ClientError>
where
    S: futures_util::Stream<Item = Result<String, LinesCodecError>> + Unpin,
{
    match stream.next().await {
        Some(Ok(line)) => Ok(Some(decode_frame(&line)?)),
        Some(Err(error)) => Err(error.into()),
        None => Ok(None),
    }
}

/// Concatenate output data from one replay for terminal presentation.
#[must_use]
pub fn replay_bytes(chunks: &[OutputChunk]) -> Vec<u8> {
    let capacity = chunks.iter().map(|chunk| chunk.data.len()).sum();
    let mut output = Vec::with_capacity(capacity);
    for chunk in chunks {
        output.extend_from_slice(&chunk.data);
    }
    output
}

#[cfg(test)]
mod runtime_tests {
    use ctxmux_protocol::{
        DaemonInstanceId, NativeRuntimeCapabilities, RuntimeBuildId, RuntimeCapabilityManifest,
        RuntimeDescription, RuntimeId, RuntimeServiceCapabilities, TmuxRuntimeCapabilities,
    };

    use super::{ClientError, PROTOCOL_VERSION, RUNTIME_CAPABILITY_MANIFEST_VERSION};

    #[test]
    fn client_rejects_unknown_runtime_manifest_versions() {
        let mut runtime = runtime_description();
        super::validate_runtime_description(&runtime).expect("accept current Runtime manifest");

        runtime.capabilities.version = RUNTIME_CAPABILITY_MANIFEST_VERSION + 1;
        assert!(matches!(
            super::validate_runtime_description(&runtime),
            Err(ClientError::UnexpectedFrame("expected compatible hello"))
        ));
    }

    fn runtime_description() -> RuntimeDescription {
        RuntimeDescription {
            runtime_id: RuntimeId::new(),
            daemon_instance_id: DaemonInstanceId::new(),
            build_id: RuntimeBuildId::new("ctxmuxd/test").unwrap(),
            protocol_generation: PROTOCOL_VERSION,
            capabilities: RuntimeCapabilityManifest {
                version: RUNTIME_CAPABILITY_MANIFEST_VERSION,
                native: NativeRuntimeCapabilities {
                    start: true,
                    recoverable_input: true,
                    fork_level_a: true,
                    execute_materialized_level_b: true,
                },
                tmux: TmuxRuntimeCapabilities {
                    discover: true,
                    import: true,
                },
                services: RuntimeServiceCapabilities {
                    persistent_state_active: false,
                    planned_exec_upgrade_continuity: false,
                },
            },
        }
    }
}

#[cfg(test)]
mod replay_tests {
    use ctxmux_protocol::{
        AttachedSnapshot, OutputChunk, OutputReplay, RunBackend, RunCapabilities, RunEvent,
        RunInfo, RunState, ServerFrame, encode_frame,
    };
    use futures_util::stream;
    use tokio_util::codec::LinesCodecError;

    use super::{ClientError, receive_replay};

    #[tokio::test]
    async fn replay_accepts_the_exact_advertised_byte_boundary() {
        let mut snapshot = snapshot(2);
        let mut frames = stream::iter([Ok(frame(OutputChunk {
            start_byte: 0,
            end_byte: 2,
            data: vec![0, 255],
        }))]);

        receive_replay(&mut frames, 0, &mut snapshot)
            .await
            .expect("accept exact replay boundary");
        assert_eq!(snapshot.replay.chunks[0].end_byte, 2);
    }

    #[tokio::test]
    async fn replay_rejects_overshoot_non_progress_and_eof() {
        for (label, chunk) in [
            (
                "overshoot",
                OutputChunk {
                    start_byte: 0,
                    end_byte: 2,
                    data: vec![1, 2],
                },
            ),
            (
                "non-progress",
                OutputChunk {
                    start_byte: 0,
                    end_byte: 0,
                    data: Vec::new(),
                },
            ),
        ] {
            let mut snapshot = snapshot(1);
            let mut frames = stream::iter([Ok(frame(chunk))]);
            assert!(
                matches!(
                    receive_replay(&mut frames, 0, &mut snapshot).await,
                    Err(ClientError::UnexpectedFrame(
                        "expected ordered replay output"
                    ))
                ),
                "{label} must fail closed"
            );
            assert!(snapshot.replay.chunks.is_empty(), "{label} was appended");
        }

        let mut snapshot = snapshot(1);
        let mut eof = stream::empty::<Result<String, LinesCodecError>>();
        assert!(matches!(
            receive_replay(&mut eof, 0, &mut snapshot).await,
            Err(ClientError::Closed)
        ));
    }

    fn frame(chunk: OutputChunk) -> String {
        encode_frame(&ServerFrame::Event {
            event: RunEvent::Output { chunk },
        })
        .expect("encode replay fixture")
    }

    fn snapshot(latest_output_bytes: u64) -> AttachedSnapshot {
        AttachedSnapshot {
            run: RunInfo {
                id: ctxmux_protocol::RunId::new(),
                spec: None,
                lineage: None,
                backend: RunBackend::Native,
                capabilities: RunCapabilities::NATIVE,
                pid: None,
                state: RunState::Running,
                latest_output_bytes,
                durable_output_bytes: None,
                first_available_byte: 0,
                attachments: 1,
                applied_input_bytes: Some(0),
            },
            replay: OutputReplay {
                chunks: Vec::new(),
                first_available_byte: 0,
                latest_output_bytes,
                truncated: false,
            },
        }
    }
}
