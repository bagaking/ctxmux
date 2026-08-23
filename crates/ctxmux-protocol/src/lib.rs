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
pub const PROTOCOL_VERSION: u16 = 10;

/// Start a daemon-owned native Run.
pub const RUNTIME_CAPABILITY_NATIVE_START: &str = "native.start";

/// Apply or recover one caller-keyed native Input operation.
pub const RUNTIME_CAPABILITY_NATIVE_RECOVERABLE_INPUT: &str = "native.recoverable_input";

/// Execute a portable Level A fork from a compatible retained Run.
pub const RUNTIME_CAPABILITY_NATIVE_FORK_LEVEL_A: &str = "native.fork_level_a";

/// Execute a complete caller-materialized Level B Run specification.
pub const RUNTIME_CAPABILITY_NATIVE_EXECUTE_MATERIALIZED_LEVEL_B: &str =
    "native.execute_materialized_level_b";

/// Discover panes through an explicitly selected public tmux endpoint.
pub const RUNTIME_CAPABILITY_TMUX_DISCOVER: &str = "tmux.discover";

/// Import one pane as a read-only memory-only Run.
pub const RUNTIME_CAPABILITY_TMUX_IMPORT: &str = "tmux.import";

/// Retain Runtime identity and historical Run state in one state directory.
pub const RUNTIME_CAPABILITY_PERSISTENT_STATE: &str = "services.persistent_state";

/// Preserve live ownership across a validated planned exec-in-place upgrade.
pub const RUNTIME_CAPABILITY_PLANNED_EXEC_UPGRADE_CONTINUITY: &str =
    "services.planned_exec_upgrade_continuity";

/// Maximum size of one JSON-lines frame.
pub const MAX_FRAME_BYTES: usize = 1024 * 1024;

/// Maximum UTF-8 byte length of one caller-owned Run creation operation key.
pub const MAX_CREATE_OPERATION_KEY_BYTES: usize = 128;

/// Maximum UTF-8 byte length of one caller-owned native Input operation key.
pub const MAX_INPUT_OPERATION_KEY_BYTES: usize = 128;

/// Maximum UTF-8 byte length of one daemon-authored build identity.
pub const MAX_RUNTIME_BUILD_ID_BYTES: usize = 128;

/// Attachment-local correlation identity for one control command.
///
/// The first ID is one and later IDs are strictly greater; gaps are allowed, so
/// the daemon need retain only the latest structurally valid ID it observed.
/// IDs are not idempotency keys, replay credentials, or identities that survive
/// reconnect.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, TS)]
#[serde(transparent)]
#[ts(type = "number")]
pub struct AttachmentCommandId(u32);

impl AttachmentCommandId {
    /// Validate one attachment-local command identity.
    ///
    /// # Errors
    ///
    /// Returns [`AttachmentCommandIdError`] when `value` is zero.
    pub const fn new(value: u32) -> Result<Self, AttachmentCommandIdError> {
        if value == 0 {
            return Err(AttachmentCommandIdError::Zero);
        }
        Ok(Self(value))
    }

    /// Return the numeric correlation identity.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl<'de> Deserialize<'de> for AttachmentCommandId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Self::new(u32::deserialize(deserializer)?).map_err(D::Error::custom)
    }
}

impl TryFrom<u32> for AttachmentCommandId {
    type Error = AttachmentCommandIdError;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<AttachmentCommandId> for u32 {
    fn from(value: AttachmentCommandId) -> Self {
        value.get()
    }
}

/// Invalid attachment-local control correlation identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum AttachmentCommandIdError {
    /// Zero is reserved so every accepted command has a positive identity.
    #[error("attachment command id must be greater than zero")]
    Zero,
}

/// Caller-owned idempotency key for one bounded Run creation operation.
///
/// Equality is exact over the UTF-8 bytes. The key identifies no Session and
/// carries no mutable metadata; it is retained only with the Run it created.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, TS)]
#[serde(transparent)]
#[ts(type = "string")]
pub struct CreateOperationKey(String);

impl CreateOperationKey {
    /// Validate and retain one opaque operation key.
    ///
    /// # Errors
    ///
    /// Returns [`CreateOperationKeyError`] when the key is empty or exceeds
    /// [`MAX_CREATE_OPERATION_KEY_BYTES`] UTF-8 bytes.
    pub fn new(value: impl Into<String>) -> Result<Self, CreateOperationKeyError> {
        let key = Self(value.into());
        key.validate()?;
        Ok(key)
    }

    /// Generate a fresh opaque operation key for one creation attempt.
    #[must_use]
    pub fn random() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    /// Return the exact opaque value.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Validate a key received through an untrusted protocol decoder.
    ///
    /// # Errors
    ///
    /// Returns [`CreateOperationKeyError`] for an empty or oversized value.
    pub fn validate(&self) -> Result<(), CreateOperationKeyError> {
        let bytes = self.0.len();
        if bytes == 0 {
            return Err(CreateOperationKeyError::Empty);
        }
        if bytes > MAX_CREATE_OPERATION_KEY_BYTES {
            return Err(CreateOperationKeyError::TooLong {
                actual: bytes,
                maximum: MAX_CREATE_OPERATION_KEY_BYTES,
            });
        }
        Ok(())
    }
}

impl fmt::Display for CreateOperationKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl std::str::FromStr for CreateOperationKey {
    type Err = CreateOperationKeyError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

/// Invalid caller-owned Run creation operation key.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CreateOperationKeyError {
    /// The key has no bytes.
    #[error("Run creation operation key must not be empty")]
    Empty,
    /// The UTF-8 representation exceeds the public bound.
    #[error("Run creation operation key is {actual} bytes; maximum is {maximum}")]
    TooLong { actual: usize, maximum: usize },
}

/// Identity of one running daemon incarnation.
///
/// A persistent daemon uses the same freshly generated value as its serving
/// epoch. The identity is never recovered as authority after restart.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, TS)]
#[serde(transparent)]
#[ts(type = "string")]
pub struct DaemonInstanceId(Uuid);

impl DaemonInstanceId {
    /// Allocate a new random daemon-incarnation identity.
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for DaemonInstanceId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for DaemonInstanceId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl std::str::FromStr for DaemonInstanceId {
    type Err = uuid::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value.parse().map(Self)
    }
}

/// Identity of one logical Runtime endpoint.
///
/// Persistent mode stores this identity with the state directory so a cold
/// replacement retains the same Runtime while receiving a new daemon
/// incarnation. Memory-only mode allocates a fresh Runtime identity at daemon
/// startup. This identity is not a Run, build, host, Provider, or credential.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, TS)]
#[serde(transparent)]
#[ts(type = "string")]
pub struct RuntimeId(Uuid);

impl RuntimeId {
    /// Allocate a fresh logical Runtime identity.
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for RuntimeId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for RuntimeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl std::str::FromStr for RuntimeId {
    type Err = uuid::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value.parse().map(Self)
    }
}

/// Opaque identity of the daemon build serving one Runtime connection.
///
/// The value is suitable for equality checks across reconnect or planned
/// exec. It is not a binary signature, source attestation, host identity, or
/// authorization credential.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, TS)]
#[serde(transparent)]
#[ts(type = "string")]
pub struct RuntimeBuildId(String);

impl RuntimeBuildId {
    /// Validate one non-empty bounded build identity.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeBuildIdError`] when the value is empty or exceeds the
    /// public UTF-8 byte bound.
    pub fn new(value: impl Into<String>) -> Result<Self, RuntimeBuildIdError> {
        let identity = Self(value.into());
        identity.validate()?;
        Ok(identity)
    }

    /// Return the exact opaque value.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Validate an identity received through an untrusted protocol decoder.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeBuildIdError`] for an empty or oversized value.
    pub fn validate(&self) -> Result<(), RuntimeBuildIdError> {
        let bytes = self.0.len();
        if bytes == 0 {
            return Err(RuntimeBuildIdError::Empty);
        }
        if bytes > MAX_RUNTIME_BUILD_ID_BYTES {
            return Err(RuntimeBuildIdError::TooLong {
                actual: bytes,
                maximum: MAX_RUNTIME_BUILD_ID_BYTES,
            });
        }
        Ok(())
    }
}

impl<'de> Deserialize<'de> for RuntimeBuildId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(D::Error::custom)
    }
}

impl fmt::Display for RuntimeBuildId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl std::str::FromStr for RuntimeBuildId {
    type Err = RuntimeBuildIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

/// Invalid daemon-authored Runtime build identity.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RuntimeBuildIdError {
    /// The identity has no bytes.
    #[error("Runtime build identity must not be empty")]
    Empty,
    /// The identity exceeds the public byte bound.
    #[error("Runtime build identity is {actual} bytes; maximum is {maximum}")]
    TooLong { actual: usize, maximum: usize },
}

/// Caller-owned identity for one recoverable native Input operation.
///
/// The key namespace is one `(daemon incarnation, Run)` pair. Equality is
/// byte-exact and conflict detection lasts only while the bounded Run-local
/// operation result remains pending or retained.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, TS)]
#[serde(transparent)]
#[ts(type = "string")]
pub struct InputOperationKey(String);

impl InputOperationKey {
    /// Validate and retain one opaque Input operation key.
    ///
    /// # Errors
    ///
    /// Returns [`InputOperationKeyError`] when the key is empty or exceeds
    /// [`MAX_INPUT_OPERATION_KEY_BYTES`] UTF-8 bytes.
    pub fn new(value: impl Into<String>) -> Result<Self, InputOperationKeyError> {
        let key = Self(value.into());
        key.validate()?;
        Ok(key)
    }

    /// Generate a fresh opaque key for one logical Input operation.
    #[must_use]
    pub fn random() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    /// Return the exact opaque value.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Validate a key received through an untrusted protocol decoder.
    ///
    /// # Errors
    ///
    /// Returns [`InputOperationKeyError`] for an empty or oversized value.
    pub fn validate(&self) -> Result<(), InputOperationKeyError> {
        let bytes = self.0.len();
        if bytes == 0 {
            return Err(InputOperationKeyError::Empty);
        }
        if bytes > MAX_INPUT_OPERATION_KEY_BYTES {
            return Err(InputOperationKeyError::TooLong {
                actual: bytes,
                maximum: MAX_INPUT_OPERATION_KEY_BYTES,
            });
        }
        Ok(())
    }
}

impl fmt::Display for InputOperationKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl std::str::FromStr for InputOperationKey {
    type Err = InputOperationKeyError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

/// Invalid caller-owned native Input operation key.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum InputOperationKeyError {
    /// The key has no bytes.
    #[error("native Input operation key must not be empty")]
    Empty,
    /// The UTF-8 representation exceeds the public bound.
    #[error("native Input operation key is {actual} bytes; maximum is {maximum}")]
    TooLong { actual: usize, maximum: usize },
}

/// Exact native Input byte range applied by one recoverable operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct AppliedInputRange {
    /// Inclusive applied-input cursor before this operation.
    pub start_byte: u64,
    /// Exclusive applied-input cursor after this operation.
    pub end_byte: u64,
}

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

/// Backend owner of one ctxmux Run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RunBackend {
    /// The ctxmux daemon owns the native PTY and direct child handle.
    Native,
    /// tmux owns the server, session, pane PTY, and pane process. ctxmux owns
    /// only one public Control Mode observation client.
    Tmux {
        /// Explicit tmux server socket selected by the caller.
        socket_path: String,
        /// tmux server PID at import time.
        server_pid: u32,
        /// tmux server start time at import time.
        server_started_at: u64,
        /// Stable session ID within that tmux server lifetime.
        session_id: String,
        /// Stable window ID within that tmux server lifetime.
        window_id: String,
        /// Stable pane ID represented by this Run.
        pane_id: String,
        /// Released tmux version accepted by the adapter.
        tmux_version: String,
    },
}

/// Replay fidelity available for one Run backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum ReplayCapability {
    /// Raw bytes are retained from native Run start.
    RawFromStart,
    /// Raw bytes are retained only after ctxmux imports an existing pane.
    RawSinceImport,
}

/// Generic operations honestly supported by one Run backend.
#[allow(
    clippy::struct_excessive_bools,
    reason = "independent public capability bits must remain explicit on the wire"
)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct RunCapabilities {
    pub input: bool,
    pub resize: bool,
    pub signal: bool,
    pub stop: bool,
    pub fork_level_a: bool,
    pub fork_level_b: bool,
    pub replay: ReplayCapability,
}

impl RunCapabilities {
    /// Capabilities of the current native PTY backend.
    pub const NATIVE: Self = Self {
        input: true,
        resize: true,
        signal: true,
        stop: true,
        fork_level_a: true,
        fork_level_b: true,
        replay: ReplayCapability::RawFromStart,
    };

    /// Deliberately read-only capabilities of an imported tmux pane.
    pub const TMUX_READ_ONLY: Self = Self {
        input: false,
        resize: false,
        signal: false,
        stop: false,
        fork_level_a: false,
        fork_level_b: false,
        replay: ReplayCapability::RawSinceImport,
    };
}

/// One existing pane returned by public tmux discovery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct TmuxPaneInfo {
    pub socket_path: String,
    pub tmux_version: String,
    pub server_pid: u32,
    pub server_started_at: u64,
    pub session_id: String,
    pub window_id: String,
    pub pane_id: String,
    pub pane_pid: u32,
    pub size: TerminalSize,
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
    /// The prior daemon lost live ownership and no replacement process was
    /// adopted or signalled.
    Interrupted {
        /// Explicit reason that live ownership ended without an exit status.
        reason: InterruptionReason,
    },
}

impl RunState {
    /// Whether terminal lifecycle publication has not occurred yet.
    ///
    /// Live-control authority is owned separately by the current daemon
    /// incarnation and may already be fenced while this still returns true.
    #[must_use]
    pub const fn is_running(&self) -> bool {
        matches!(self, Self::Running)
    }
}

/// Why a Run became historical without a portable child exit status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum InterruptionReason {
    /// A new daemon epoch reconciled a previously running durable record.
    DaemonRestart,
    /// The public tmux Control Mode client lost its tmux server.
    TmuxServerUnavailable,
    /// The tmux Control Mode stream violated its bounded public framing.
    TmuxProtocolError,
    /// The imported tmux pane identity disappeared or changed.
    TmuxTargetChanged,
}

/// Current public metadata for one Run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct RunInfo {
    /// Stable Run identity.
    pub id: RunId,
    /// Portable inputs used to launch the Run.
    pub spec: Option<RunSpec>,
    /// Immediate parent and actual fidelity for a fork, or `None` for start.
    pub lineage: Option<RunLineage>,
    /// Runtime owner and backend-specific stable identity.
    pub backend: RunBackend,
    /// Operations and replay fidelity honestly supported by this backend.
    pub capabilities: RunCapabilities,
    /// Child process identifier when supplied by the platform.
    pub pid: Option<u32>,
    /// Current lifecycle state.
    pub state: RunState,
    /// Total output bytes allocated so far.
    pub latest_output_bytes: u64,
    /// Total output bytes committed by the persistence actor, or `None`
    /// when this daemon is running without a state directory.
    pub durable_output_bytes: Option<u64>,
    /// First output byte still retained, or zero before output.
    pub first_available_byte: u64,
    /// Number of live attachment connections.
    pub attachments: usize,
    /// Bytes successfully applied by the current native Input owner, or
    /// `None` when this Run has no current-incarnation native cursor authority.
    pub applied_input_bytes: Option<u64>,
}

/// Caller-retained request for one recoverable native Input operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct RecoverableInput {
    /// Daemon incarnation originally observed by the caller.
    pub daemon_instance: DaemonInstanceId,
    /// Per-Run operation key retained across response loss.
    pub operation_key: InputOperationKey,
    /// Native Run receiving the bytes.
    pub id: RunId,
    /// Applied-input cursor expected immediately before this write.
    pub expected_byte: u64,
    /// Exact non-empty PTY bytes for this logical operation.
    pub data: Vec<u8>,
}

/// Portable signal semantics exposed by a native Run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum RunSignal {
    /// Interrupt the foreground process group as if the terminal received
    /// Ctrl-C, without ending Run ownership.
    Interrupt,
}

/// How a complete native Run session reached quiescence during Stop.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum StopDisposition {
    /// The session emptied without a forced phase, including an owner-ordered
    /// natural exit after Stop admission.
    Graceful,
    /// At least one session member required the forced phase.
    Forced,
}

/// One ordered half-open PTY output byte range.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct OutputChunk {
    /// Inclusive cumulative byte offset of the first byte in `data`.
    pub start_byte: u64,
    /// Exclusive cumulative byte offset immediately after `data`.
    pub end_byte: u64,
    /// Raw PTY bytes. JSON represents these as an integer array in generation 10.
    pub data: Vec<u8>,
}

/// Bounded output retained for a newly attached client.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct OutputReplay {
    /// Retained ranges beginning at or after the requested byte cursor.
    pub chunks: Vec<OutputChunk>,
    /// First retained byte, or zero when there has been no output.
    pub first_available_byte: u64,
    /// Total output bytes allocated so far.
    pub latest_output_bytes: u64,
    /// Whether output at or after the requested cursor is unavailable.
    pub truncated: bool,
}

/// Replay metadata sent in the initial attachment frame.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct OutputReplayHeader {
    /// First retained byte, or zero when there has been no output.
    pub first_available_byte: u64,
    /// Total output bytes allocated so far.
    pub latest_output_bytes: u64,
    /// Whether output newer than the requested cursor had already been evicted.
    pub truncated: bool,
}

/// Persistence class of one logical Runtime identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeIdPersistence {
    /// The Runtime ID lasts for one memory-only daemon lifetime.
    Daemon,
    /// The selected state-directory lineage preserves the Runtime ID.
    StateDir,
}

/// Provider-neutral identity of the Runtime serving one connection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[ts(rename_all = "camelCase")]
pub struct RuntimeIdentity {
    /// Current live retry and authority fence.
    pub daemon_instance_id: DaemonInstanceId,
    /// Logical Runtime or persistent-store lineage.
    pub runtime_id: RuntimeId,
    /// Whether the Runtime ID belongs to this daemon or a state-directory lineage.
    pub runtime_id_persistence: RuntimeIdPersistence,
    /// Opaque serving build identity.
    pub build_id: RuntimeBuildId,
    /// Exact public protocol generation used by this connection.
    pub protocol_generation: u16,
    /// Canonical Rust build-target operating-system value.
    #[serde(deserialize_with = "deserialize_non_empty_runtime_string")]
    pub platform: String,
    /// Canonical Rust build-target architecture value.
    #[serde(deserialize_with = "deserialize_non_empty_runtime_string")]
    pub arch: String,
    /// Highest implemented public contract version for each exact flat key.
    #[serde(deserialize_with = "deserialize_runtime_capabilities")]
    #[ts(type = "Record<string, number>")]
    pub capabilities: BTreeMap<String, u16>,
}

fn deserialize_non_empty_runtime_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    if value.is_empty() {
        return Err(D::Error::custom(
            "Runtime build-target value must not be empty",
        ));
    }
    Ok(value)
}

fn deserialize_runtime_capabilities<'de, D>(
    deserializer: D,
) -> Result<BTreeMap<String, u16>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let capabilities = BTreeMap::<String, u16>::deserialize(deserializer)?;
    if let Some((key, _)) = capabilities.iter().find(|(_, version)| **version == 0) {
        return Err(D::Error::custom(format!(
            "Runtime capability {key:?} must have a positive integer version"
        )));
    }
    Ok(capabilities)
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
    Start {
        operation_key: CreateOperationKey,
        spec: RunSpec,
    },
    /// Discover panes from one explicitly selected tmux server socket.
    DiscoverTmux { socket_path: String },
    /// Import one existing tmux pane as a read-only observable Run.
    ImportTmux {
        socket_path: String,
        pane_id: String,
    },
    /// Create one child Run from an explicit fidelity plan.
    Fork {
        operation_key: CreateOperationKey,
        parent: RunId,
        plan: ForkPlan,
    },
    /// List all Runs retained by this daemon.
    List,
    /// Read current metadata for one Run.
    Status { id: RunId },
    /// Write bytes to a live Run's PTY.
    Input { id: RunId, data: Vec<u8> },
    /// Write one caller-keyed native Input whose result survives reconnect.
    RecoverableInput { operation: RecoverableInput },
    /// Resize a live Run's PTY.
    Resize { id: RunId, size: TerminalSize },
    /// Deliver one portable signal to a live Run.
    Signal { id: RunId, signal: RunSignal },
    /// Terminate the complete owned native Run session.
    Stop { id: RunId },
    /// Attach to retained output and future lifecycle events.
    Attach {
        id: RunId,
        /// Cumulative number of output bytes already observed by the client.
        #[serde(default)]
        after_byte: u64,
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
    Input {
        command_id: AttachmentCommandId,
        data: Vec<u8>,
    },
    /// Resize through a live attachment.
    Resize {
        command_id: AttachmentCommandId,
        size: TerminalSize,
    },
    /// Deliver one portable signal through a live attachment.
    Signal {
        command_id: AttachmentCommandId,
        signal: RunSignal,
    },
    /// Stop the attached Run.
    Stop { command_id: AttachmentCommandId },
    /// Close this attachment without affecting the Run.
    Detach,
}

/// Owner-boundary receipt for one accepted Run control operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ControlReceipt {
    /// The complete input reached the daemon-owned PTY write boundary.
    Input { written_bytes: u32 },
    /// The owning PTY reported this size after the resize attempt.
    Resize { applied_size: TerminalSize },
    /// The native lifecycle owner delivered the requested signal.
    Signal { signal: RunSignal },
    /// The complete owned native Run session reached quiescence.
    Stop { disposition: StopDisposition },
}

/// Whether a rejected control command may already have crossed its owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum CommandDisposition {
    /// The command did not cross the operation's mutation boundary.
    NotApplied,
    /// The command may have crossed the mutation boundary; callers must not
    /// infer that retry is safe.
    Unknown,
}

/// One correlated control failure and its retry-safety boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct ControlFailure {
    /// Machine-readable public failure.
    pub error: ProtocolError,
    /// Whether the failed command is known not to have been applied.
    pub disposition: CommandDisposition,
}

/// Correlated result of one attachment control command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ControlOutcome {
    /// The command reached its documented owner boundary.
    Accepted { receipt: ControlReceipt },
    /// The command failed with an explicit application disposition.
    Rejected { failure: ControlFailure },
}

/// Response to a short-lived request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Response {
    /// A Run was created.
    Started { run: RunInfo },
    /// Existing panes discovered through the tmux executable.
    TmuxPanes {
        tmux_version: String,
        panes: Vec<TmuxPaneInfo>,
    },
    /// One existing tmux pane was imported as a ctxmux Run.
    Imported { run: RunInfo },
    /// A forked child Run was created.
    Forked { run: RunInfo },
    /// Current Runs retained by the daemon.
    Runs { runs: Vec<RunInfo> },
    /// Current metadata for one Run.
    Status { run: RunInfo },
    /// A short-lived control request reached its documented owner boundary.
    ControlAccepted {
        run: RunInfo,
        receipt: ControlReceipt,
    },
    /// A short-lived control request failed with an explicit disposition.
    ControlRejected { failure: ControlFailure },
    /// One recoverable native Input reached the PTY write boundary.
    InputApplied {
        run: RunInfo,
        range: AppliedInputRange,
    },
}

/// Snapshot delivered when attachment begins.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct AttachedSnapshot {
    /// Current Run metadata.
    pub run: RunInfo,
    /// Retained output after the requested cursor.
    pub replay: OutputReplay,
}

/// Metadata-only first frame for an attachment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct AttachedHeader {
    /// Current Run metadata.
    pub run: RunInfo,
    /// Replay bounds whose chunks follow as ordered output-event frames.
    pub replay: OutputReplayHeader,
}

/// Event delivered after an attachment snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RunEvent {
    /// New ordered PTY output.
    Output { chunk: OutputChunk },
    /// Terminal lifecycle state.
    Exited { state: RunState },
    /// Historical terminal state produced by restart reconciliation.
    Interrupted { reason: InterruptionReason },
    /// Backend-specific observable event that does not change generic Run
    /// ownership semantics.
    Tmux { event: TmuxRunEvent },
    /// The attachment lagged behind live delivery and should request replay.
    Gap { latest_output_bytes: u64 },
}

/// Observable public-Control-Mode event for one imported tmux pane.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TmuxRunEvent {
    /// The imported session's mutable name changed. Identity remains its ID.
    SessionRenamed { name: Vec<u8> },
    /// tmux paused delivery for this control client.
    Paused,
    /// tmux resumed delivery for this control client.
    Continued,
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
    /// Durable state could not accept or commit a mutation.
    Persistence,
    /// The selected Backend executable, server, or transport is unavailable.
    BackendUnavailable,
    /// The installed Backend version is outside the declared supported range.
    UnsupportedBackendVersion,
    /// The Run backend explicitly does not support the requested operation.
    UnsupportedCapability,
    /// The selected external Backend target changed or disappeared.
    TargetChanged,
    /// A retained Run creation key names a different canonical request.
    CreationConflict,
    /// A retained per-Run Input key names a different canonical request.
    InputOperationConflict,
    /// Recoverable Input expected another applied-input cursor.
    InputCursorMismatch,
    /// Recoverable Input belongs to another daemon incarnation.
    DaemonInstanceMismatch,
    /// The daemon cannot reserve a retained Run record before mutation.
    RunCapacity,
    /// The bounded live-control path has no capacity for this command.
    ControlBackpressure,
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
    Hello { runtime: RuntimeIdentity },
    /// Result of one short-lived request.
    Response { response: Response },
    /// Initial attachment metadata. Retained output follows as event frames.
    Attached { snapshot: AttachedHeader },
    /// Live attachment event.
    Event { event: RunEvent },
    /// Result of one attachment-local control command.
    CommandResult {
        command_id: AttachmentCommandId,
        outcome: ControlOutcome,
    },
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

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(())
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
        AppliedInputRange, AttachmentCommandId, ClientFrame, ClientHello, CommandDisposition,
        ControlFailure, ControlOutcome, ControlReceipt, CreateOperationKey, DaemonInstanceId,
        ErrorCode, FrameError, InputOperationKey, MAX_CREATE_OPERATION_KEY_BYTES, MAX_FRAME_BYTES,
        MAX_INPUT_OPERATION_KEY_BYTES, PROTOCOL_VERSION, ProtocolError,
        RUNTIME_CAPABILITY_NATIVE_EXECUTE_MATERIALIZED_LEVEL_B,
        RUNTIME_CAPABILITY_NATIVE_FORK_LEVEL_A, RUNTIME_CAPABILITY_NATIVE_RECOVERABLE_INPUT,
        RUNTIME_CAPABILITY_NATIVE_START, RUNTIME_CAPABILITY_TMUX_DISCOVER,
        RUNTIME_CAPABILITY_TMUX_IMPORT, RecoverableInput, Request, Response, RunBackend,
        RunCapabilities, RunId, RunInfo, RunSignal, RunSpec, RunState, RuntimeBuildId, RuntimeId,
        RuntimeIdPersistence, RuntimeIdentity, ServerFrame, StopDisposition, TerminalSize,
        decode_frame, encode_frame,
    };

    fn sample_run_info() -> RunInfo {
        RunInfo {
            id: RunId::new(),
            spec: Some(RunSpec {
                program: "fixture".to_owned(),
                args: Vec::new(),
                cwd: None,
                env: std::collections::BTreeMap::new(),
                size: TerminalSize::default(),
                declared_inputs: Vec::new(),
            }),
            lineage: None,
            backend: RunBackend::Native,
            capabilities: RunCapabilities::NATIVE,
            pid: Some(42),
            state: RunState::Running,
            latest_output_bytes: 0,
            durable_output_bytes: None,
            first_available_byte: 0,
            attachments: 1,
            applied_input_bytes: Some(0),
        }
    }

    fn sample_runtime_identity(daemon_instance_id: DaemonInstanceId) -> RuntimeIdentity {
        RuntimeIdentity {
            daemon_instance_id,
            runtime_id: "018f47f2-9df7-7f5f-8f2d-d3353f114aea"
                .parse::<RuntimeId>()
                .unwrap(),
            runtime_id_persistence: RuntimeIdPersistence::Daemon,
            build_id: RuntimeBuildId::new("ctxmuxd/0.1.0").unwrap(),
            protocol_generation: PROTOCOL_VERSION,
            platform: "linux".to_owned(),
            arch: "x86_64".to_owned(),
            capabilities: std::collections::BTreeMap::from([
                (RUNTIME_CAPABILITY_NATIVE_START.to_owned(), 1),
                (RUNTIME_CAPABILITY_NATIVE_RECOVERABLE_INPUT.to_owned(), 1),
                (RUNTIME_CAPABILITY_NATIVE_FORK_LEVEL_A.to_owned(), 1),
                (
                    RUNTIME_CAPABILITY_NATIVE_EXECUTE_MATERIALIZED_LEVEL_B.to_owned(),
                    1,
                ),
                (RUNTIME_CAPABILITY_TMUX_DISCOVER.to_owned(), 1),
                (RUNTIME_CAPABILITY_TMUX_IMPORT.to_owned(), 1),
            ]),
        }
    }

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
    fn runtime_identity_and_recoverable_input_have_exact_generation_10_wire_shapes() {
        let daemon_instance: DaemonInstanceId =
            "018f47f2-9df7-7f5f-8f2d-d3353f114ae9".parse().unwrap();
        let run_id = RunId::new();
        let operation_key = InputOperationKey::new("input-8").unwrap();

        assert_eq!(
            serde_json::to_value(ServerFrame::Hello {
                runtime: sample_runtime_identity(daemon_instance),
            })
            .unwrap(),
            serde_json::json!({
                "type": "hello",
                "runtime": {
                    "daemonInstanceId": daemon_instance.to_string(),
                    "runtimeId": "018f47f2-9df7-7f5f-8f2d-d3353f114aea",
                    "runtimeIdPersistence": "daemon",
                    "buildId": "ctxmuxd/0.1.0",
                    "protocolGeneration": 10,
                    "platform": "linux",
                    "arch": "x86_64",
                    "capabilities": {
                        "native.start": 1,
                        "native.recoverable_input": 1,
                        "native.fork_level_a": 1,
                        "native.execute_materialized_level_b": 1,
                        "tmux.discover": 1,
                        "tmux.import": 1,
                    },
                },
            })
        );
        assert_eq!(
            serde_json::to_value(Request::RecoverableInput {
                operation: RecoverableInput {
                    daemon_instance,
                    operation_key,
                    id: run_id,
                    expected_byte: 4,
                    data: vec![0, 255],
                },
            })
            .unwrap(),
            serde_json::json!({
                "type": "recoverable_input",
                "operation": {
                    "daemon_instance": daemon_instance.to_string(),
                    "operation_key": "input-8",
                    "id": run_id.to_string(),
                    "expected_byte": 4,
                    "data": [0, 255],
                },
            })
        );

        let mut run = sample_run_info();
        run.id = run_id;
        run.applied_input_bytes = Some(6);
        let run_value = serde_json::to_value(&run).unwrap();
        assert_eq!(
            serde_json::to_value(Response::InputApplied {
                run,
                range: AppliedInputRange {
                    start_byte: 4,
                    end_byte: 6,
                },
            })
            .unwrap(),
            serde_json::json!({
                "type": "input_applied",
                "run": run_value,
                "range": {"start_byte": 4, "end_byte": 6},
            })
        );

        assert_eq!(
            serde_json::to_value([
                ErrorCode::InputOperationConflict,
                ErrorCode::InputCursorMismatch,
                ErrorCode::DaemonInstanceMismatch,
            ])
            .unwrap(),
            serde_json::json!([
                "input_operation_conflict",
                "input_cursor_mismatch",
                "daemon_instance_mismatch",
            ])
        );
    }

    #[test]
    fn run_capacity_error_has_the_exact_generation_6_wire_name() {
        let frame = ServerFrame::Error {
            error: ProtocolError::new(
                ErrorCode::RunCapacity,
                "retained Run capacity is unavailable",
            ),
        };
        let encoded = encode_frame(&frame).expect("encode run-capacity error");
        assert_eq!(
            decode_frame::<ServerFrame>(&encoded).expect("decode run-capacity error"),
            frame
        );
        assert_eq!(
            serde_json::to_value(&frame).unwrap(),
            serde_json::json!({
                "type": "error",
                "error": {
                    "code": "run_capacity",
                    "message": "retained Run capacity is unavailable"
                }
            })
        );
    }

    #[test]
    fn run_ids_round_trip_through_the_wire_format() {
        let id = RunId::new();
        let encoded = encode_frame(&id).expect("encode Run id");
        assert_eq!(decode_frame::<RunId>(&encoded).unwrap(), id);
    }

    #[test]
    fn creation_operation_keys_are_bounded_by_exact_utf8_bytes() {
        let exact = "x".repeat(MAX_CREATE_OPERATION_KEY_BYTES);
        let key = CreateOperationKey::new(exact.clone()).expect("accept exact key ceiling");
        assert_eq!(key.as_str(), exact);
        assert_eq!(
            decode_frame::<CreateOperationKey>(&encode_frame(&key).unwrap()).unwrap(),
            key
        );

        assert!(CreateOperationKey::new("").is_err());
        assert!(CreateOperationKey::new("x".repeat(MAX_CREATE_OPERATION_KEY_BYTES + 1)).is_err());
        assert!(CreateOperationKey::new("界".repeat(MAX_CREATE_OPERATION_KEY_BYTES / 3)).is_ok());
        assert!(
            CreateOperationKey::new("界".repeat(MAX_CREATE_OPERATION_KEY_BYTES / 3 + 1)).is_err()
        );
    }

    #[test]
    fn input_operation_keys_are_bounded_by_exact_utf8_bytes() {
        let exact = "x".repeat(MAX_INPUT_OPERATION_KEY_BYTES);
        let key = InputOperationKey::new(exact.clone()).expect("accept exact key ceiling");
        assert_eq!(key.as_str(), exact);
        assert_eq!(
            decode_frame::<InputOperationKey>(&encode_frame(&key).unwrap()).unwrap(),
            key
        );

        assert!(InputOperationKey::new("").is_err());
        assert!(InputOperationKey::new("x".repeat(MAX_INPUT_OPERATION_KEY_BYTES + 1)).is_err());
        assert!(InputOperationKey::new("界".repeat(MAX_INPUT_OPERATION_KEY_BYTES / 3)).is_ok());
        assert!(
            InputOperationKey::new("界".repeat(MAX_INPUT_OPERATION_KEY_BYTES / 3 + 1)).is_err()
        );
    }

    #[test]
    fn attachment_command_ids_enforce_the_u32_nonzero_boundary() {
        let last = AttachmentCommandId::new(u32::MAX).expect("accept maximum command id");
        assert!(AttachmentCommandId::new(1).is_ok());
        assert_eq!(u32::from(last), u32::MAX);
        assert!(AttachmentCommandId::new(0).is_err());
        assert!(decode_frame::<AttachmentCommandId>("0").is_err());
        assert!(decode_frame::<AttachmentCommandId>("4294967296").is_err());
        assert_eq!(
            decode_frame::<AttachmentCommandId>(&encode_frame(&last).unwrap()).unwrap(),
            last
        );
        assert!(decode_frame::<ClientFrame>(r#"{"type":"stop","command_id":0}"#).is_err());
    }

    #[test]
    fn attachment_control_commands_have_exact_correlated_wire_shapes() {
        let first = AttachmentCommandId::new(1).unwrap();
        let later = AttachmentCommandId::new(8).unwrap();
        assert_eq!(
            serde_json::to_value(ClientFrame::Input {
                command_id: first,
                data: vec![0, 255],
            })
            .unwrap(),
            serde_json::json!({"type": "input", "command_id": 1, "data": [0, 255]})
        );
        assert_eq!(
            serde_json::to_value(ClientFrame::Resize {
                command_id: later,
                size: TerminalSize { cols: 90, rows: 30 },
            })
            .unwrap(),
            serde_json::json!({
                "type": "resize",
                "command_id": 8,
                "size": {"cols": 90, "rows": 30}
            })
        );
        assert_eq!(
            serde_json::to_value(ClientFrame::Signal {
                command_id: later,
                signal: RunSignal::Interrupt,
            })
            .unwrap(),
            serde_json::json!({
                "type": "signal",
                "command_id": 8,
                "signal": "interrupt"
            })
        );
        assert_eq!(
            serde_json::to_value(ControlReceipt::Signal {
                signal: RunSignal::Interrupt,
            })
            .unwrap(),
            serde_json::json!({"type": "signal", "signal": "interrupt"})
        );
        assert_eq!(
            serde_json::to_value(ClientFrame::Stop { command_id: later }).unwrap(),
            serde_json::json!({"type": "stop", "command_id": 8})
        );
        assert_eq!(
            serde_json::to_value(ControlReceipt::Stop {
                disposition: StopDisposition::Forced,
            })
            .unwrap(),
            serde_json::json!({"type": "stop", "disposition": "forced"})
        );
    }

    #[test]
    fn attachment_control_results_round_trip_separately_from_run_events() {
        let command_id = AttachmentCommandId::new(7).unwrap();
        let accepted = ServerFrame::CommandResult {
            command_id,
            outcome: ControlOutcome::Accepted {
                receipt: ControlReceipt::Resize {
                    applied_size: TerminalSize {
                        cols: 132,
                        rows: 43,
                    },
                },
            },
        };
        assert_eq!(
            decode_frame::<ServerFrame>(&encode_frame(&accepted).unwrap()).unwrap(),
            accepted
        );
        assert_eq!(
            serde_json::to_value(&accepted).unwrap(),
            serde_json::json!({
                "type": "command_result",
                "command_id": 7,
                "outcome": {
                    "type": "accepted",
                    "receipt": {
                        "type": "resize",
                        "applied_size": {"cols": 132, "rows": 43}
                    }
                }
            })
        );

        let rejected = ServerFrame::CommandResult {
            command_id,
            outcome: ControlOutcome::Rejected {
                failure: ControlFailure {
                    error: ProtocolError::new(
                        ErrorCode::ControlBackpressure,
                        "input capacity is full",
                    ),
                    disposition: CommandDisposition::NotApplied,
                },
            },
        };
        assert_eq!(
            decode_frame::<ServerFrame>(&encode_frame(&rejected).unwrap()).unwrap(),
            rejected
        );
        assert_eq!(
            serde_json::to_value(&rejected).unwrap(),
            serde_json::json!({
                "type": "command_result",
                "command_id": 7,
                "outcome": {
                    "type": "rejected",
                    "failure": {
                        "error": {
                            "code": "control_backpressure",
                            "message": "input capacity is full"
                        },
                        "disposition": "not_applied"
                    }
                }
            })
        );
    }

    #[test]
    fn short_lived_control_responses_share_the_receipt_and_failure_contract() {
        let accepted = Response::ControlAccepted {
            run: sample_run_info(),
            receipt: ControlReceipt::Input { written_bytes: 3 },
        };
        assert_eq!(
            decode_frame::<Response>(&encode_frame(&accepted).unwrap()).unwrap(),
            accepted
        );
        let accepted_value = serde_json::to_value(&accepted).unwrap();
        assert_eq!(accepted_value["type"], "control_accepted");
        assert_eq!(
            accepted_value["receipt"],
            serde_json::json!({"type": "input", "written_bytes": 3})
        );

        let rejected = Response::ControlRejected {
            failure: ControlFailure {
                error: ProtocolError::new(ErrorCode::Io, "PTY write outcome is unknown"),
                disposition: CommandDisposition::Unknown,
            },
        };
        assert_eq!(
            decode_frame::<Response>(&encode_frame(&rejected).unwrap()).unwrap(),
            rejected
        );
        assert_eq!(
            serde_json::to_value(&rejected).unwrap(),
            serde_json::json!({
                "type": "control_rejected",
                "failure": {
                    "error": {
                        "code": "io",
                        "message": "PTY write outcome is unknown"
                    },
                    "disposition": "unknown"
                }
            })
        );
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
            let error = decode_frame::<ClientFrame>(&bytes)
                .expect_err("shared malformed frame must fail before typed decoding");
            assert!(
                matches!(error, FrameError::Decode(_)),
                "shared malformed frame {id} was accepted"
            );
        }
    }

    #[test]
    fn duplicate_guard_accepts_every_valid_json_value_shape() {
        // The duplicate-name guard walks the untyped value before serde performs
        // typed decoding, so every JSON scalar and container shape is part of
        // the protocol codec contract rather than an unreachable visitor arm.
        let value = br#"{
            "bool": true,
            "negative": -1,
            "positive": 1,
            "float": 1.5,
            "string": "value",
            "null": null,
            "array": [false, -2, 2, 2.5, "nested", null],
            "object": {"member": "value"}
        }"#;

        let decoded = decode_frame::<serde_json::Value>(value).expect("decode every JSON shape");
        assert_eq!(decoded["negative"], -1);
        assert_eq!(decoded["array"][4], "nested");
        let run_id = RunId::default();
        assert_eq!(run_id.to_string().parse::<RunId>(), Ok(run_id));
        assert!("not-a-run-id".parse::<RunId>().is_err());
    }
}
