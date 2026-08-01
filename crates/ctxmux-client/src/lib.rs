//! Rust client for the versioned ctxmux local protocol.

#[cfg(not(unix))]
compile_error!("the first ctxmux native transport currently requires Unix sockets");

use std::{
    io,
    path::{Path, PathBuf},
};

use ctxmux_protocol::{
    AttachedSnapshot, ClientFrame, ClientHello, CreateOperationKey, ForkPlan, FrameError,
    MAX_FRAME_BYTES, OutputChunk, OutputReplay, PROTOCOL_VERSION, ProtocolError, Request, Response,
    RunEvent, RunId, RunInfo, RunSpec, ServerFrame, TerminalSize, TmuxPaneInfo, decode_frame,
    encode_frame,
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
    /// The daemon returned a valid frame in the wrong protocol state.
    #[error("unexpected ctxmux frame: {0}")]
    UnexpectedFrame(&'static str),
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
    pub async fn input(&self, id: RunId, data: Vec<u8>) -> Result<RunInfo, ClientError> {
        match self.request(Request::Input { id, data }).await? {
            Response::Accepted { run } => Ok(run),
            _ => Err(ClientError::UnexpectedFrame("expected accepted response")),
        }
    }

    /// Resize one live Run.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] when the size is invalid, the Run is not live,
    /// or the PTY resize fails.
    pub async fn resize(&self, id: RunId, size: TerminalSize) -> Result<RunInfo, ClientError> {
        match self.request(Request::Resize { id, size }).await? {
            Response::Accepted { run } => Ok(run),
            _ => Err(ClientError::UnexpectedFrame("expected accepted response")),
        }
    }

    /// Terminate one live Run.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] when the Run is not live or termination fails.
    pub async fn stop(&self, id: RunId) -> Result<RunInfo, ClientError> {
        match self.request(Request::Stop { id }).await? {
            Response::Accepted { run } => Ok(run),
            _ => Err(ClientError::UnexpectedFrame("expected accepted response")),
        }
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
                Ok((Attachment { wire }, snapshot))
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

/// Live attachment to one daemon-owned Run.
pub struct Attachment {
    wire: Wire,
}

impl Attachment {
    /// Write bytes through this attachment.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] when the attachment transport is closed or
    /// cannot encode the frame.
    pub async fn input(&mut self, data: Vec<u8>) -> Result<(), ClientError> {
        send(&mut self.wire, &ClientFrame::Input { data }).await
    }

    /// Resize through this attachment.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] when the attachment transport is closed or
    /// cannot encode the frame.
    pub async fn resize(&mut self, size: TerminalSize) -> Result<(), ClientError> {
        send(&mut self.wire, &ClientFrame::Resize { size }).await
    }

    /// Stop the attached Run.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] when the attachment transport is closed or
    /// cannot encode the frame.
    pub async fn stop(&mut self) -> Result<(), ClientError> {
        send(&mut self.wire, &ClientFrame::Stop).await
    }

    /// Wait for the next live Run event. A clean detach or closed daemon returns `None`.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] when transport, frame decoding, or a Run
    /// operation reports an error.
    pub async fn next_event(&mut self) -> Result<Option<RunEvent>, ClientError> {
        match receive_optional(&mut self.wire).await? {
            Some(ServerFrame::Event { event }) => Ok(Some(event)),
            Some(ServerFrame::Detached) | None => Ok(None),
            Some(ServerFrame::Error { error }) => Err(error.into()),
            Some(_) => Err(ClientError::UnexpectedFrame("expected attachment event")),
        }
    }

    /// Detach cleanly without affecting the Run.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] when the detach frame cannot be sent or the
    /// acknowledgement cannot be read.
    pub async fn detach(mut self) -> Result<(), ClientError> {
        send(&mut self.wire, &ClientFrame::Detach).await?;
        loop {
            match receive_optional(&mut self.wire).await? {
                Some(ServerFrame::Detached) | None => return Ok(()),
                Some(ServerFrame::Event { .. }) => {}
                Some(ServerFrame::Error { error }) => return Err(error.into()),
                Some(_) => return Err(ClientError::UnexpectedFrame("expected detached frame")),
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
    sink.send(encode_frame(frame)?).await?;
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
