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
    AttachedSnapshot, AttachmentCommandId, ClientFrame, ClientHello, CommandDisposition,
    ControlFailure, ControlReceipt, CreateOperationKey, ForkPlan, FrameError, MAX_FRAME_BYTES,
    OutputChunk, OutputReplay, PROTOCOL_VERSION, ProtocolError, Request, Response, RunEvent, RunId,
    RunInfo, RunSpec, ServerFrame, TerminalSize, TmuxPaneInfo, decode_frame, encode_frame,
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

/// Typed receipt that the direct-child owner accepted a stop request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StopReceipt;

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

    /// Attach after the last output sequence already observed by the caller.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] when the Run does not exist, the handshake
    /// fails, or the attachment snapshot cannot be read.
    pub async fn attach(
        &self,
        id: RunId,
        after_seq: u64,
    ) -> Result<(Attachment, AttachedSnapshot), ClientError> {
        let mut wire = self.connect().await?;
        send(
            &mut wire,
            &ClientFrame::Request {
                request: Request::Attach { id, after_seq },
            },
        )
        .await?;

        match receive(&mut wire).await? {
            ServerFrame::Attached { snapshot: header } => {
                let mut snapshot = AttachedSnapshot {
                    run: header.run,
                    replay: OutputReplay {
                        chunks: Vec::new(),
                        oldest_seq: header.replay.oldest_seq,
                        head_seq: header.replay.head_seq,
                        truncated: header.replay.truncated,
                    },
                };
                receive_replay(&mut wire, after_seq, &mut snapshot).await?;
                Ok((Attachment::from_wire(wire), snapshot))
            }
            ServerFrame::Error { error } => Err(error.into()),
            _ => Err(ClientError::UnexpectedFrame("expected attached snapshot")),
        }
    }

    async fn request(&self, request: Request) -> Result<Response, ClientError> {
        let mut wire = self.connect().await?;
        send(&mut wire, &ClientFrame::Request { request }).await?;
        match receive(&mut wire).await? {
            ServerFrame::Response { response } => Ok(response),
            ServerFrame::Error { error } => Err(error.into()),
            _ => Err(ClientError::UnexpectedFrame("expected request response")),
        }
    }

    async fn control_request(&self, request: Request) -> Result<Response, ClientError> {
        let mut wire = self.connect().await.map_err(control_not_applied)?;
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

    async fn connect(&self) -> Result<Wire, ClientError> {
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
            ServerFrame::Hello { protocol } if protocol == PROTOCOL_VERSION => Ok(wire),
            ServerFrame::Error { error } => Err(error.into()),
            _ => Err(ClientError::UnexpectedFrame("expected compatible hello")),
        }
    }
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
        ControlReceipt::Resize { .. } | ControlReceipt::Stop => Err(
            ClientError::ProtocolContractViolation("input returned another receipt kind"),
        ),
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
        ControlReceipt::Input { .. } | ControlReceipt::Stop => Err(
            ClientError::ProtocolContractViolation("resize returned another receipt kind"),
        ),
    }
}

fn decode_stop_receipt(receipt: &ControlReceipt) -> Result<StopReceipt, ClientError> {
    match receipt {
        ControlReceipt::Stop => Ok(StopReceipt),
        ControlReceipt::Input { .. } | ControlReceipt::Resize { .. } => Err(
            ClientError::ProtocolContractViolation("stop returned another receipt kind"),
        ),
    }
}

async fn receive_replay(
    wire: &mut Wire,
    after_seq: u64,
    snapshot: &mut AttachedSnapshot,
) -> Result<(), ClientError> {
    if snapshot
        .replay
        .chunks
        .last()
        .is_some_and(|chunk| chunk.seq == snapshot.replay.head_seq)
        || (snapshot.replay.chunks.is_empty() && after_seq >= snapshot.replay.head_seq)
    {
        return Ok(());
    }
    let mut next_seq = snapshot.replay.chunks.last().map_or_else(
        || after_seq.saturating_add(1).max(snapshot.replay.oldest_seq),
        |chunk| chunk.seq + 1,
    );
    loop {
        match receive(wire).await? {
            ServerFrame::Event {
                event: RunEvent::Output { chunk },
            } if chunk.seq == next_seq => {
                let complete = chunk.seq == snapshot.replay.head_seq;
                snapshot.replay.chunks.push(chunk);
                if complete {
                    return Ok(());
                }
                next_seq += 1;
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
