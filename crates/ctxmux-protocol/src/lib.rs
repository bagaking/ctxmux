//! Versioned wire contract shared by ctxmux clients and the local daemon.

use std::{collections::BTreeMap, fmt};

use serde::{
    Deserialize, Serialize,
    de::{DeserializeOwned, DeserializeSeed, Error as _, MapAccess, SeqAccess, Visitor},
};
use thiserror::Error;
use ts_rs::TS;
use uuid::Uuid;

/// Current protocol generation developed in this repository.
pub const PROTOCOL_VERSION: u16 = 2;

/// Maximum size of one JSON-lines frame.
pub const MAX_FRAME_BYTES: usize = 1024 * 1024;

/// Stable identity of a Run for the lifetime of its owning daemon.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, TS)]
#[serde(transparent)]
#[ts(type = "string")]
pub struct RunId(Uuid);

impl RunId {
    /// Allocate a new random Run identity.
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for RunId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for RunId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl std::str::FromStr for RunId {
    type Err = uuid::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value.parse().map(Self)
    }
}

/// PTY dimensions visible to a Run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct TerminalSize {
    /// Terminal columns.
    pub cols: u16,
    /// Terminal rows.
    pub rows: u16,
}

impl Default for TerminalSize {
    fn default() -> Self {
        Self { cols: 80, rows: 24 }
    }
}

/// Kind of one opaque input reference explicitly declared by a caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum RunInputKind {
    /// A workspace location or identity.
    Workspace,
    /// An artifact location or identity.
    Artifact,
    /// An opaque context or native-session identity.
    Context,
}

/// One opaque reference required to interpret or continue a Run.
///
/// The daemon records this value but never dereferences, copies, normalizes,
/// or infers ownership from it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct RunInputReference {
    /// Reference category visible to generic clients.
    pub kind: RunInputKind,
    /// Non-empty opaque reference value.
    pub reference: String,
}

/// Portable inputs required to start one native Run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct RunSpec {
    /// Executable name or path.
    pub program: String,
    /// Arguments passed directly to the executable.
    #[serde(default)]
    pub args: Vec<String>,
    /// Working directory. The daemon's current directory is used when absent.
    pub cwd: Option<String>,
    /// Environment entries added to the inherited daemon environment.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// Initial PTY dimensions.
    #[serde(default)]
    pub size: TerminalSize,
    /// Explicit workspace, artifact, and context references used by this Run.
    pub declared_inputs: Vec<RunInputReference>,
}

/// Context fidelity actually used to create a child Run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum ForkFidelity {
    /// Exact copy of the parent's portable [`RunSpec`].
    LevelA,
    /// Integration-materialized native continuation or fork plan.
    LevelB,
}

/// Immediate derivation of one forked Run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct RunLineage {
    /// Retained parent Run used as the fork source.
    pub parent: RunId,
    /// Fidelity path actually executed by the daemon.
    pub fidelity: ForkFidelity,
}

/// Explicit fork plan. The daemon never substitutes one variant for another.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ForkPlan {
    /// Clone the parent's complete immutable [`RunSpec`].
    LevelA,
    /// Execute an Integration-materialized [`RunSpec`] without merging it
    /// with the parent.
    LevelB { spec: RunSpec },
}

/// Observable lifecycle state of a Run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RunState {
    /// The child process is still live.
    Running,
    /// The child process has exited and its retained output remains readable.
    Exited {
        /// Portable exit code reported by the PTY implementation.
        code: u32,
        /// Signal description when termination was signal-driven.
        signal: Option<String>,
    },
}

impl RunState {
    /// Whether the Run still accepts control operations.
    #[must_use]
    pub const fn is_running(&self) -> bool {
        matches!(self, Self::Running)
    }
}

/// Current public metadata for one Run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct RunInfo {
    /// Stable Run identity.
    pub id: RunId,
    /// Portable inputs used to launch the Run.
    pub spec: RunSpec,
    /// Immediate parent and actual fidelity for a fork, or `None` for start.
    pub lineage: Option<RunLineage>,
    /// Child process identifier when supplied by the platform.
    pub pid: Option<u32>,
    /// Current lifecycle state.
    pub state: RunState,
    /// Highest output sequence allocated so far, or zero before output.
    pub head_seq: u64,
    /// Oldest output sequence still retained, or zero before output.
    pub oldest_seq: u64,
    /// Number of live attachment connections.
    pub attachments: usize,
}

/// One ordered PTY output chunk.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct OutputChunk {
    /// Monotonically increasing sequence within one Run.
    pub seq: u64,
    /// Raw PTY bytes. JSON represents these as an integer array in v2.
    pub data: Vec<u8>,
}

/// Bounded output retained for a newly attached client.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct OutputReplay {
    /// Retained chunks newer than the requested sequence.
    pub chunks: Vec<OutputChunk>,
    /// Oldest retained sequence, or zero when there has been no output.
    pub oldest_seq: u64,
    /// Highest allocated output sequence, or zero when there has been no output.
    pub head_seq: u64,
    /// Whether output newer than the requested cursor had already been evicted.
    pub truncated: bool,
}

/// First message sent by every client connection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct ClientHello {
    /// Protocol generation understood by the client.
    pub protocol: u16,
}

/// Command sent on a short-lived request connection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Request {
    /// Start a new daemon-owned Run.
    Start { spec: RunSpec },
    /// Create one child Run from an explicit fidelity plan.
    Fork { parent: RunId, plan: ForkPlan },
    /// List all Runs retained by this daemon.
    List,
    /// Read current metadata for one Run.
    Status { id: RunId },
    /// Write bytes to a live Run's PTY.
    Input { id: RunId, data: Vec<u8> },
    /// Resize a live Run's PTY.
    Resize { id: RunId, size: TerminalSize },
    /// Terminate a live Run.
    Stop { id: RunId },
    /// Attach to retained output and future lifecycle events.
    Attach {
        id: RunId,
        /// Last sequence already observed by the client.
        #[serde(default)]
        after_seq: u64,
    },
}

/// Frames sent by a client after the connection handshake.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientFrame {
    /// Initial version handshake.
    Hello { hello: ClientHello },
    /// One short-lived request.
    Request { request: Request },
    /// Write bytes through a live attachment.
    Input { data: Vec<u8> },
    /// Resize through a live attachment.
    Resize { size: TerminalSize },
    /// Stop the attached Run.
    Stop,
    /// Close this attachment without affecting the Run.
    Detach,
}

/// Successful response to a short-lived request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Response {
    /// A Run was created.
    Started { run: RunInfo },
    /// A forked child Run was created.
    Forked { run: RunInfo },
    /// Current Runs retained by the daemon.
    Runs { runs: Vec<RunInfo> },
    /// Current metadata for one Run.
    Status { run: RunInfo },
    /// A state-changing request was accepted.
    Accepted { run: RunInfo },
}

/// Snapshot delivered when attachment begins.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct AttachedSnapshot {
    /// Current Run metadata.
    pub run: RunInfo,
    /// Retained output after the requested cursor.
    pub replay: OutputReplay,
}

/// Event delivered after an attachment snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RunEvent {
    /// New ordered PTY output.
    Output { chunk: OutputChunk },
    /// Terminal lifecycle state.
    Exited { state: RunState },
    /// The attachment lagged behind live delivery and should request replay.
    Gap { head_seq: u64 },
    /// A state-changing attachment command was accepted.
    Accepted { run: RunInfo },
}

/// Stable error categories exposed by the protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    /// The client and daemon use different protocol generations.
    VersionMismatch,
    /// A frame or request is not valid in the current protocol state.
    InvalidRequest,
    /// A request names an unknown Run.
    RunNotFound,
    /// The Run exists but cannot perform the operation in its current state.
    InvalidRunState,
    /// The daemon could not spawn the requested process.
    SpawnFailed,
    /// A local I/O operation failed.
    Io,
    /// An unexpected daemon failure occurred.
    Internal,
}

/// Error returned through the public client boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct ProtocolError {
    /// Machine-readable error category.
    pub code: ErrorCode,
    /// Human-readable detail that must not be parsed for control flow.
    pub message: String,
}

impl ProtocolError {
    /// Construct one protocol error.
    #[must_use]
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

/// Frames sent by the daemon.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerFrame {
    /// Successful protocol handshake.
    Hello { protocol: u16 },
    /// Successful short-lived request response.
    Response { response: Response },
    /// Initial attachment state and retained output.
    Attached { snapshot: AttachedSnapshot },
    /// Live attachment event.
    Event { event: RunEvent },
    /// The daemon acknowledged a clean detach.
    Detached,
    /// Explicit request or lifecycle error.
    Error { error: ProtocolError },
}

/// JSON encoding or decoding failure for one wire frame.
#[derive(Debug, Error)]
pub enum FrameError {
    /// JSON serialization failed.
    #[error("failed to encode protocol frame: {0}")]
    Encode(#[source] serde_json::Error),
    /// JSON deserialization failed.
    #[error("failed to decode protocol frame: {0}")]
    Decode(#[source] serde_json::Error),
    /// The encoded frame exceeds the protocol maximum.
    #[error("protocol frame is {actual} bytes; maximum is {maximum}")]
    TooLarge { actual: usize, maximum: usize },
}

/// Encode a protocol value as one JSON-lines payload without the newline.
///
/// # Errors
///
/// Returns [`FrameError`] when JSON serialization fails or the encoded frame
/// exceeds [`MAX_FRAME_BYTES`].
pub fn encode_frame<T: Serialize>(value: &T) -> Result<String, FrameError> {
    let encoded = serde_json::to_string(value).map_err(FrameError::Encode)?;
    if encoded.len() > MAX_FRAME_BYTES {
        return Err(FrameError::TooLarge {
            actual: encoded.len(),
            maximum: MAX_FRAME_BYTES,
        });
    }
    Ok(encoded)
}

/// Decode one JSON-lines payload.
///
/// # Errors
///
/// Returns [`FrameError`] when the payload exceeds [`MAX_FRAME_BYTES`] or is
/// not valid JSON for the requested protocol type.
pub fn decode_frame<T: DeserializeOwned>(value: impl AsRef<[u8]>) -> Result<T, FrameError> {
    let value = value.as_ref();
    if value.len() > MAX_FRAME_BYTES {
        return Err(FrameError::TooLarge {
            actual: value.len(),
            maximum: MAX_FRAME_BYTES,
        });
    }

    let mut deserializer = serde_json::Deserializer::from_slice(value);
    RejectDuplicateObjectMembers
        .deserialize(&mut deserializer)
        .map_err(FrameError::Decode)?;
    deserializer.end().map_err(FrameError::Decode)?;

    serde_json::from_slice(value).map_err(FrameError::Decode)
}

struct RejectDuplicateObjectMembers;

impl<'de> DeserializeSeed<'de> for RejectDuplicateObjectMembers {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for RejectDuplicateObjectMembers {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON value without duplicate object members")
    }

    fn visit_bool<E>(self, _value: bool) -> Result<Self::Value, E> {
        Ok(())
    }

    fn visit_i64<E>(self, _value: i64) -> Result<Self::Value, E> {
        Ok(())
    }

    fn visit_u64<E>(self, _value: u64) -> Result<Self::Value, E> {
        Ok(())
    }

    fn visit_f64<E>(self, _value: f64) -> Result<Self::Value, E> {
        Ok(())
    }

    fn visit_str<E>(self, _value: &str) -> Result<Self::Value, E> {
        Ok(())
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(())
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(())
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        self.deserialize(deserializer)
    }

    fn visit_newtype_struct<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        self.deserialize(deserializer)
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while sequence.next_element_seed(Self)?.is_some() {}
        Ok(())
    }

    fn visit_map<A>(self, mut object: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut names = std::collections::BTreeSet::new();
        while let Some(name) = object.next_key::<String>()? {
            if !names.insert(name.clone()) {
                return Err(A::Error::custom(format!(
                    "duplicate object member {name:?}"
                )));
            }
            object.next_value_seed(Self)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ClientFrame, ClientHello, FrameError, MAX_FRAME_BYTES, PROTOCOL_VERSION, RunId,
        decode_frame, encode_frame,
    };

    fn malformed_protocol_frames() -> Vec<(String, Vec<u8>)> {
        let corpus: serde_json::Value = serde_json::from_str(include_str!(
            "../../../fixtures/malformed-protocol-frames.json"
        ))
        .expect("parse shared malformed-frame corpus");
        corpus["frames"]
            .as_array()
            .expect("corpus frames are an array")
            .iter()
            .map(|frame| {
                let id = frame["id"]
                    .as_str()
                    .expect("frame id is a string")
                    .to_owned();
                let bytes = frame["bytes"]
                    .as_array()
                    .expect("frame bytes are an array")
                    .iter()
                    .map(|byte| {
                        u8::try_from(byte.as_u64().expect("frame byte is an unsigned integer"))
                            .expect("frame byte fits in u8")
                    })
                    .collect();
                (id, bytes)
            })
            .collect()
    }

    #[test]
    fn frame_round_trip_preserves_the_versioned_handshake() {
        let frame = ClientFrame::Hello {
            hello: ClientHello {
                protocol: PROTOCOL_VERSION,
            },
        };
        let encoded = encode_frame(&frame).expect("encode handshake");
        assert_eq!(decode_frame::<ClientFrame>(&encoded).unwrap(), frame);
    }

    #[test]
    fn run_ids_round_trip_through_the_wire_format() {
        let id = RunId::new();
        let encoded = encode_frame(&id).expect("encode Run id");
        assert_eq!(decode_frame::<RunId>(&encoded).unwrap(), id);
    }

    #[test]
    fn malformed_json_is_an_explicit_decode_error() {
        assert!(matches!(
            decode_frame::<ClientFrame>("{"),
            Err(FrameError::Decode(_))
        ));
    }

    #[test]
    fn frame_byte_limit_accepts_exactly_the_ceiling_and_rejects_one_byte_more() {
        // LP-02: both helpers use the same byte-exact inclusive ceiling.
        let exact_value = "x".repeat(MAX_FRAME_BYTES - 2);
        let exact_frame = encode_frame(&exact_value).expect("encode exact-limit JSON string");
        assert_eq!(exact_frame.len(), MAX_FRAME_BYTES);
        assert_eq!(
            decode_frame::<String>(&exact_frame).expect("decode exact-limit JSON string"),
            exact_value
        );

        let oversized_value = "x".repeat(MAX_FRAME_BYTES - 1);
        assert!(matches!(
            encode_frame(&oversized_value),
            Err(FrameError::TooLarge {
                actual,
                maximum: MAX_FRAME_BYTES,
            }) if actual == MAX_FRAME_BYTES + 1
        ));
        let oversized_frame = format!("\"{oversized_value}\"");
        assert!(matches!(
            decode_frame::<String>(&oversized_frame),
            Err(FrameError::TooLarge {
                actual,
                maximum: MAX_FRAME_BYTES,
            }) if actual == MAX_FRAME_BYTES + 1
        ));
    }

    #[test]
    fn duplicate_json_names_are_rejected_instead_of_using_last_value() {
        // LP-03: the protocol owner rejects one shared raw-byte corpus before
        // typed decoding, including map and unknown nested object members.
        for (id, bytes) in malformed_protocol_frames() {
            assert!(
                matches!(
                    decode_frame::<ClientFrame>(&bytes),
                    Err(FrameError::Decode(_))
                ),
                "shared malformed frame {id} was accepted"
            );
        }
    }
}
