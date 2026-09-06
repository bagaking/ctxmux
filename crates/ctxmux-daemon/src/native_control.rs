//! Current-incarnation ownership of one native Run's PTY controls.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt,
    fs::File,
    io::{self, Write},
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Condvar, Mutex, MutexGuard, OnceLock, TryLockError,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

#[cfg(test)]
use std::{sync::Weak, thread};

use ctxmux_protocol::{
    AppliedInputRange, CommandDisposition, ControlFailure, ControlReceipt, ErrorCode,
    InputOperationKey, NativeInputPhase, NativeInputStatus, NativeServiceFailure, ProtocolError,
    RunId, RunSignal, StopDisposition, TerminalSize,
};
use portable_pty::{Child, MasterPty, PtySize};
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, oneshot, watch};

use crate::adopted_pty::AdoptedMasterPty;
use crate::native_runtime::OwnerWake;
use crate::native_service::NativeService;
#[cfg(test)]
use crate::qualification_stats::Gauge as QualificationGauge;
use crate::qualification_stats::QualificationStats;

#[cfg(test)]
const INPUT_DRAIN_MAX_ACTIVE: usize = 8;
#[cfg(test)]
const INPUT_RESULT_MAX_ENTRIES: usize = 256;
#[cfg(test)]
const INPUT_RESULT_MAX_REQUEST_BYTES: usize = 1024 * 1024;
// Historical per-result diagnostic reservation, not an aggregate handoff cap.
// Known allocation and opaque heap-cost qualification remains a separate task.
pub(crate) const INPUT_RESULT_DIAGNOSTIC_RESERVE_BYTES: usize = 4 * 1024;

pub(crate) type ControlResult = Result<ControlReceipt, ControlFailure>;

#[derive(Debug)]
pub(crate) struct PendingInput {
    run_id: RunId,
    reply: oneshot::Receiver<ControlResult>,
}

type RecoverableInputResult = Result<AppliedInputRange, ControlFailure>;

#[derive(Debug)]
pub(crate) enum PendingRecoverableInput {
    Ready(RecoverableInputResult),
    Pending {
        run_id: RunId,
        result: watch::Receiver<Option<RecoverableInputResult>>,
    },
}

#[derive(Debug)]
pub(crate) struct PendingStop {
    run_id: RunId,
    reply: oneshot::Receiver<StopOwnerResult>,
}

#[derive(Debug)]
pub(crate) enum StopOwnerResult {
    Accepted(StopDisposition),
    Rejected(ControlFailure),
    Unknown(String),
}

#[derive(Debug)]
pub(crate) struct PendingSignal {
    run_id: RunId,
    signal: RunSignal,
    reply: oneshot::Receiver<Result<(), String>>,
}

/// One direct-child command handled by the daemon-wide native lifecycle owner.
pub(crate) enum ChildCommand {
    #[cfg(not(target_os = "macos"))]
    Signal {
        signal: RunSignal,
        foreground_group: u32,
        reply: oneshot::Sender<Result<(), String>>,
    },
    Stop {
        reply: oneshot::Sender<StopOwnerResult>,
        deadline: Instant,
    },
    CleanupUnpublished,
}

/// Funded Native input queue and retained-operation admission.
/// Production writes belong only to the readiness-driven Native Run owner.
/// The opaque-writer burst scheduler below is compiled only for historical
/// unit probes; production never takes a blocking input-worker lease.
#[derive(Clone)]
pub(crate) struct InputDrainGate {
    inner: Arc<InputDrainGateInner>,
}

struct InputDrainGateInner {
    control_budget: crate::resources::ByteBudget,
    resources: crate::ResourceLimits,
    #[cfg(test)]
    state: Mutex<InputDrainGateState>,
    #[cfg(test)]
    max_active: usize,
    #[cfg(test)]
    burst_max_commands: usize,
    #[cfg(test)]
    burst_max_bytes: usize,
    #[cfg(test)]
    qualification_stats: QualificationStats,
}

#[cfg(test)]
#[derive(Default)]
struct InputDrainGateState {
    active: usize,
    waiting: VecDeque<Weak<NativeControlInner>>,
}

impl Default for InputDrainGate {
    fn default() -> Self {
        Self::with_stats_and_resources(
            QualificationStats::default(),
            crate::ResourceLimits::DEFAULT,
        )
    }
}

// Arc's known reference counters and payload include ABI alignment padding.
// The allocator's private bookkeeping is measured separately as host RSS.
#[repr(C)]
struct ArcFileCharge {
    counters: [std::sync::atomic::AtomicUsize; 2],
    file: File,
}

pub(crate) const fn resident_control_owner_bytes() -> usize {
    // Production owns one Arc<File> heap payload and Arc reference counters,
    // in addition to the control cell. Opaque PTY/library allocations remain
    // separately qualified as host RSS.
    std::mem::size_of::<NativeControlInner>() + std::mem::size_of::<ArcFileCharge>()
}

impl InputDrainGate {
    pub(crate) fn with_stats_and_resources(
        stats: QualificationStats,
        resources: crate::ResourceLimits,
    ) -> Self {
        Self::with_stats_resources_and_budget(
            stats,
            resources,
            crate::resources::ByteBudget::new(resources.control_state_bytes),
        )
    }

    pub(crate) fn with_stats_resources_and_budget(
        stats: QualificationStats,
        resources: crate::ResourceLimits,
        budget: crate::resources::ByteBudget,
    ) -> Self {
        #[cfg(not(test))]
        drop(stats);
        Self {
            inner: Arc::new(InputDrainGateInner {
                control_budget: budget,
                resources,
                #[cfg(test)]
                state: Mutex::new(InputDrainGateState::default()),
                #[cfg(test)]
                max_active: INPUT_DRAIN_MAX_ACTIVE,
                #[cfg(test)]
                burst_max_commands: resources.input_turn_commands,
                #[cfg(test)]
                burst_max_bytes: resources.input_turn_bytes,
                #[cfg(test)]
                qualification_stats: stats,
            }),
        }
    }

    pub(crate) fn control_budget(&self) -> crate::resources::ByteBudget {
        self.inner.control_budget.clone()
    }

    #[cfg(test)]
    fn with_limits(max_active: usize, burst_max_commands: usize, burst_max_bytes: usize) -> Self {
        Self::with_limits_and_stats(
            max_active,
            burst_max_commands,
            burst_max_bytes,
            QualificationStats::default(),
        )
    }

    #[cfg(test)]
    fn with_limits_and_stats(
        max_active: usize,
        burst_max_commands: usize,
        burst_max_bytes: usize,
        qualification_stats: QualificationStats,
    ) -> Self {
        debug_assert!(max_active > 0);
        debug_assert!(burst_max_commands > 0);
        debug_assert!(burst_max_bytes > 0);
        Self {
            inner: Arc::new(InputDrainGateInner {
                control_budget: crate::resources::ByteBudget::new(
                    crate::ResourceLimits::DEFAULT.control_state_bytes,
                ),
                resources: crate::ResourceLimits::DEFAULT,
                state: Mutex::new(InputDrainGateState::default()),
                max_active,
                burst_max_commands,
                burst_max_bytes,
                qualification_stats,
            }),
        }
    }

    #[cfg(test)]
    fn schedule(&self, owner: Arc<NativeControlInner>) {
        let start = {
            let mut state = mutex_lock(&self.inner.state);
            if state.active < self.inner.max_active {
                state.active += 1;
                self.inner
                    .qualification_stats
                    .set(QualificationGauge::InputDrains, state.active);
                Some(owner)
            } else {
                state.waiting.push_back(Arc::downgrade(&owner));
                None
            }
        };
        if let Some(owner) = start {
            self.spawn(owner);
        }
    }

    #[cfg(test)]
    fn spawn(&self, mut owner: Arc<NativeControlInner>) {
        loop {
            let gate = self.clone();
            let thread_owner = Arc::clone(&owner);
            match thread::Builder::new()
                .name("ctxmux-input-drain".to_owned())
                .spawn(move || gate.run_worker(thread_owner))
            {
                Ok(_) => return,
                Err(error) => {
                    owner.fail_scheduled(format!("failed to start PTY input owner: {error}"));
                    let Some(next) = self.handoff_after_burst(false, &owner) else {
                        return;
                    };
                    owner = next;
                }
            }
        }
    }

    #[cfg(test)]
    fn run_worker(&self, mut owner: Arc<NativeControlInner>) {
        loop {
            let has_more =
                owner.drain_burst(self.inner.burst_max_commands, self.inner.burst_max_bytes);
            let Some(next) = self.handoff_after_burst(has_more, &owner) else {
                return;
            };
            owner = next;
        }
    }

    #[cfg(test)]
    fn handoff_after_burst(
        &self,
        requeue: bool,
        owner: &Arc<NativeControlInner>,
    ) -> Option<Arc<NativeControlInner>> {
        let mut state = mutex_lock(&self.inner.state);
        if requeue {
            state.waiting.push_back(Arc::downgrade(owner));
        }
        while let Some(candidate) = state.waiting.pop_front() {
            if let Some(candidate) = candidate
                .upgrade()
                .filter(|candidate| candidate.has_scheduled_input())
            {
                return Some(candidate);
            }
        }
        debug_assert!(state.active > 0);
        state.active -= 1;
        self.inner
            .qualification_stats
            .set(QualificationGauge::InputDrains, state.active);
        None
    }
}

/// The only live-control authority for one daemon-owned native Run.
#[derive(Clone)]
pub(crate) struct NativeControlOwner {
    inner: Arc<NativeControlInner>,
}

struct NativeControlInner {
    run_id: RunId,
    #[cfg(target_os = "macos")]
    foreground_root: OnceLock<Option<(u32, String)>>,
    pty: Mutex<Option<Box<dyn PtyControl>>>,
    writer: Mutex<Option<NativeInputWriter>>,
    service: OnceLock<NativeService>,
    state: Mutex<NativeControlState>,
    owner_deferred: AtomicBool,
    admission_changed: Notify,
    owner_failure: OnceLock<NativeServiceFailure>,
    // Set only after the sole Native entry and its physical holders are dropped.
    // Historical closed controls never register an entry.
    entry_retired: AtomicBool,
    reap: Mutex<ChildReapState>,
    reap_changed: Condvar,
    input_drains: InputDrainGate,
    owner_wake: OwnerWake,
    // Actual restored diagnostics beyond the original keyed error reserves.
    _adopted_diagnostic_memory: Option<crate::resources::BytePermit>,
}

/// Descriptor handles detached from a closed native incarnation after the
/// caller has fenced every Run lookup owner. Dropping this value closes the
/// descriptors outside the native-control and Registry locks.
#[must_use = "detached native descriptors must be dropped outside owner locks"]
pub(crate) struct DetachedNativeDescriptors {
    pty: Option<Box<dyn PtyControl>>,
    writer: Option<NativeInputWriter>,
}

impl fmt::Debug for DetachedNativeDescriptors {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DetachedNativeDescriptors")
            .field("pty", &self.pty.is_some())
            .field("writer", &self.writer.is_some())
            .finish()
    }
}

enum NativeInputWriter {
    /// An unbuffered nonblocking PTY descriptor. Drop only closes this FD;
    /// portable-pty's writer injects EOF bytes on Drop and is never used here.
    File(Arc<File>),
    #[cfg(test)]
    Opaque(Box<dyn Write + Send>),
}

enum ChildReapState {
    Pending {
        cleanup_error: Option<String>,
        wait_error: Option<String>,
    },
    OwnerStopped {
        cleanup_error: Option<String>,
        _child: Box<dyn Child + Send + Sync>,
    },
    WaitAuthorityLost {
        cleanup_error: Option<String>,
        wait_error: String,
        _child: Box<dyn Child + Send + Sync>,
    },
    Reaped,
}

struct ControlStateGuard<'a> {
    state: Option<MutexGuard<'a, NativeControlState>>,
    inner: &'a NativeControlInner,
}

impl std::ops::Deref for ControlStateGuard<'_> {
    type Target = NativeControlState;
    fn deref(&self) -> &Self::Target {
        self.state.as_ref().unwrap()
    }
}
impl std::ops::DerefMut for ControlStateGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.state.as_mut().unwrap()
    }
}
impl Drop for ControlStateGuard<'_> {
    fn drop(&mut self) {
        let (rejected, commands) = if let Some(state) = self.state.as_mut() {
            self.inner.settle_owner_loss(state)
        } else {
            (Vec::new(), VecDeque::new())
        };
        drop(self.state.take());
        self.inner.admission_changed.notify_waiters();
        send_rejections(rejected);
        reject_child_commands(
            commands,
            "native owner stopped before control admission",
            ErrorCode::BackendUnavailable,
        );
        if self.inner.owner_deferred.swap(false, Ordering::AcqRel) {
            self.inner.owner_wake.wake();
        }
    }
}

pub(crate) struct NativeControlTurn<'a> {
    state: ControlStateGuard<'a>,
}
impl NativeControlTurn<'_> {
    pub(crate) fn drain_child_commands(&mut self) -> VecDeque<ChildCommand> {
        std::mem::take(&mut self.state.child_commands)
    }
    pub(crate) fn reject_pending_stop(&mut self) {
        self.state.stop_pending = false;
    }
    pub(crate) fn commit_pending_stop(&mut self) -> Result<(), ControlFailure> {
        let run_id = self.state.inner.run_id;
        if self.state.phase != ControlPhase::Open
            || !self.state.stop_pending
            || !self.state.child_open
        {
            self.state.stop_pending = false;
            return Err(not_applied(invalid_phase_error(
                run_id,
                self.state.phase,
                "stop",
            )));
        }
        self.state.stop_pending = false;
        self.state.phase = ControlPhase::Stopping;
        let rejected = reject_queued_inputs(
            &mut self.state,
            &ProtocolError::new(
                ErrorCode::InvalidRunState,
                format!("cannot write to stopping Run {run_id}"),
            ),
        );
        self.state.inner.publish_input(&self.state);
        send_rejections(rejected);
        Ok(())
    }
    pub(crate) fn fence_child_commands(&mut self) -> VecDeque<ChildCommand> {
        if self.state.phase != ControlPhase::Failed {
            self.state.phase = ControlPhase::Closed;
        }
        self.state.child_open = false;
        self.state.stop_pending = false;
        let error = invalid_phase_error(self.state.inner.run_id, self.state.phase, "write to");
        let rejected = reject_queued_inputs(&mut self.state, &error);
        self.state.inner.publish_input(&self.state);
        send_rejections(rejected);
        std::mem::take(&mut self.state.child_commands)
    }
}

#[allow(
    clippy::struct_excessive_bools,
    reason = "child liveness, Stop admission and PTY readiness are independent observed facts"
)]
struct NativeControlState {
    phase: ControlPhase,
    /// Live PTY dimensions last read back from the owning terminal.
    ///
    /// Seeded at construction from the freshly opened master and replaced by
    /// the read-back of each applied resize. It lives under this lock, rather
    /// than beside the `pty` handle, because that makes confirming a new size
    /// and publishing it one atomic step: see `resize`.
    ///
    /// `None` only when the owner could not read the master at construction.
    confirmed_size: Option<TerminalSize>,
    input_failure: Option<ProtocolError>,
    input_queue: VecDeque<InputCommand>,
    input_commands: usize,
    input_bytes: usize,
    input_scheduled: bool,
    input_blocked: bool,
    owner_failure: Option<NativeServiceFailure>,
    applied_input_bytes: u64,
    input_operations: HashMap<InputOperationKey, InputOperationEntry>,
    completed_input_operations: VecDeque<InputOperationKey>,
    retained_input_request_bytes: usize,
    input_result_max_entries: usize,
    input_result_max_request_bytes: usize,
    child_open: bool,
    stop_pending: bool,
    child_commands: VecDeque<ChildCommand>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ControlPhase {
    Open,
    Stopping,
    Closed,
    Failed,
}

struct InputCommand {
    _memory: Option<crate::resources::BytePermit>,
    data: Arc<[u8]>,
    confirmed: usize,
    reply: InputReply,
}

enum InputReply {
    Legacy(oneshot::Sender<ControlResult>),
    Recoverable {
        key: InputOperationKey,
        completion: watch::Sender<Option<RecoverableInputResult>>,
    },
}

#[derive(Clone)]
struct InputOperationRequest {
    memory: Option<Arc<crate::resources::BytePermit>>,
    expected_byte: u64,
    data: Arc<[u8]>,
}

impl PartialEq for InputOperationRequest {
    fn eq(&self, other: &Self) -> bool {
        self.expected_byte == other.expected_byte && self.data == other.data
    }
}
impl Eq for InputOperationRequest {}

fn input_receipt_charge(key: &InputOperationKey, data: &[u8]) -> usize {
    data.len()
        + key.as_str().len() * 2
        + std::mem::size_of::<InputOperationEntry>()
        + INPUT_RESULT_DIAGNOSTIC_RESERVE_BYTES
}

enum InputOperationEntry {
    Pending {
        request: InputOperationRequest,
        completion: watch::Sender<Option<RecoverableInputResult>>,
    },
    Completed {
        request: InputOperationRequest,
        range: AppliedInputRange,
    },
    Unknown {
        request: InputOperationRequest,
        failure: ControlFailure,
    },
}

/// Recoverable native Input truth that must cross an exec-in-place upgrade
/// because that upgrade deliberately preserves the daemon incarnation fence.
/// Only settled entries can be handed off: the upgrade request gate waits for
/// every admitted mutation and its response write before extraction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct HandoffInputState {
    pub(crate) applied_input_bytes: u64,
    pub(crate) input_failure: Option<ProtocolError>,
    pub(crate) operations: Vec<HandoffInputOperation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub(crate) enum HandoffInputOperation {
    Completed {
        key: InputOperationKey,
        expected_byte: u64,
        #[serde(with = "handoff_bytes")]
        data: Vec<u8>,
        range: AppliedInputRange,
    },
    Unknown {
        key: InputOperationKey,
        expected_byte: u64,
        #[serde(with = "handoff_bytes")]
        data: Vec<u8>,
        failure: ControlFailure,
    },
}

fn validate_handoff_unknown_prefix(
    data_bytes: usize,
    failure: &ControlFailure,
) -> Result<(), String> {
    if failure
        .confirmed_input_bytes
        .is_some_and(|prefix| prefix > data_bytes)
    {
        return Err("handoff unknown native Input prefix exceeds its request bytes".to_owned());
    }
    if failure.disposition != CommandDisposition::Unknown {
        return Err("handoff unknown native Input has a non-unknown disposition".to_owned());
    }
    Ok(())
}

impl HandoffInputState {
    pub(crate) fn empty() -> Self {
        Self {
            applied_input_bytes: 0,
            input_failure: None,
            operations: Vec::new(),
        }
    }

    pub(crate) fn validate_with_resources(
        &self,
        resources: crate::ResourceLimits,
    ) -> Result<(), String> {
        let max_entries = resources.input_result_entries;
        let max_bytes = resources.input_result_bytes;
        if self.operations.len() > max_entries {
            return Err(format!(
                "handoff native Input ledger has {} entries; maximum is {max_entries}",
                self.operations.len()
            ));
        }
        let mut retained_bytes = 0_usize;
        let mut keys = HashSet::<InputOperationKey>::new();
        let mut completed_end = 0_u64;
        let mut unknown_count = 0_usize;
        let mut diagnostic_bytes = self
            .input_failure
            .as_ref()
            .map_or(0, |failure| failure.message.len());
        for operation in &self.operations {
            let (key, expected_byte, data) = match operation {
                HandoffInputOperation::Completed {
                    key,
                    expected_byte,
                    data,
                    range,
                } => {
                    let expected_end = expected_byte
                        .checked_add(
                            u64::try_from(data.len())
                                .map_err(|_| "handoff native Input length does not fit u64")?,
                        )
                        .ok_or("handoff native Input range overflows")?;
                    if range.start_byte != *expected_byte
                        || range.end_byte != expected_end
                        || range.end_byte > self.applied_input_bytes
                        || range.start_byte < completed_end
                    {
                        return Err(
                            "handoff completed native Input range is inconsistent".to_owned()
                        );
                    }
                    completed_end = range.end_byte;
                    (key, expected_byte, data)
                }
                HandoffInputOperation::Unknown {
                    key,
                    expected_byte,
                    data,
                    failure,
                } => {
                    unknown_count += 1;
                    diagnostic_bytes = diagnostic_bytes
                        .checked_add(failure.error.message.len())
                        .ok_or("handoff native Input diagnostic size overflows")?;
                    validate_handoff_unknown_prefix(data.len(), failure)?;
                    if *expected_byte != self.applied_input_bytes {
                        return Err(
                            "handoff unknown native Input does not fence the current cursor"
                                .to_owned(),
                        );
                    }
                    if self.input_failure.as_ref().map(|error| error.code)
                        != Some(failure.error.code)
                    {
                        return Err(
                            "handoff unknown native Input has a different poisoned-lane failure code"
                                .to_owned(),
                        );
                    }
                    (key, expected_byte, data)
                }
            };
            key.validate()
                .map_err(|error| format!("invalid handoff native Input key: {error}"))?;
            if data.is_empty() {
                return Err("handoff recoverable native Input payload is empty".to_owned());
            }
            if !keys.insert(key.clone()) {
                return Err("handoff native Input ledger contains a duplicate key".to_owned());
            }
            let _ = expected_byte;
            retained_bytes = retained_bytes
                .checked_add(data.len())
                .ok_or("handoff native Input retained-byte count overflows")?;
        }
        if retained_bytes > max_bytes {
            return Err(format!(
                "handoff native Input ledger retains {retained_bytes} request bytes; maximum is {max_bytes}"
            ));
        }
        if unknown_count > 1 {
            return Err(
                "handoff native Input ledger contains multiple unknown operations".to_owned(),
            );
        }
        validate_handoff_diagnostic_bytes(diagnostic_bytes, resources.handoff_diagnostic_bytes)
    }

    /// Original recoverable-Input payload bytes this Run carries across handoff.
    /// The manifest funds the actual daemon-wide sum; Run-local retention uses
    /// its configured policy, rather than a test population or fixed multiplier.
    pub(crate) fn retained_request_bytes(&self) -> usize {
        self.operations
            .iter()
            .fold(0, |sum, op| sum.saturating_add(op.request_len()))
    }

    pub(crate) fn retained_diagnostic_bytes(&self) -> usize {
        self.operations.iter().fold(
            self.input_failure
                .as_ref()
                .map_or(0, |error| error.message.len()),
            |total, operation| {
                total.saturating_add(match operation {
                    HandoffInputOperation::Unknown { failure, .. } => failure.error.message.len(),
                    HandoffInputOperation::Completed { .. } => 0,
                })
            },
        )
    }

    fn additional_diagnostic_memory_bytes(&self) -> usize {
        // Completed keys' spare error reserves must not subsidize the poisoned
        // lane: completed keys can be independently evicted from the ledger.
        let keyed_reserve = self
            .operations
            .iter()
            .filter(|operation| matches!(operation, HandoffInputOperation::Unknown { .. }))
            .count()
            .saturating_mul(INPUT_RESULT_DIAGNOSTIC_RESERVE_BYTES);
        self.retained_diagnostic_bytes()
            .saturating_sub(keyed_reserve)
    }

    pub(crate) fn control_memory_bytes(&self) -> u64 {
        self.operations.iter().fold(
            self.additional_diagnostic_memory_bytes() as u64,
            |total, operation| {
                let (key, data) = match operation {
                    HandoffInputOperation::Completed { key, data, .. }
                    | HandoffInputOperation::Unknown { key, data, .. } => (key, data),
                };
                total.saturating_add(input_receipt_charge(key, data) as u64)
            },
        )
    }
}

impl HandoffInputOperation {
    /// Recoverable request payload length this operation carries in the manifest.
    fn request_len(&self) -> usize {
        match self {
            HandoffInputOperation::Completed { data, .. }
            | HandoffInputOperation::Unknown { data, .. } => data.len(),
        }
    }
}

fn validate_handoff_diagnostic_bytes(
    diagnostic_bytes: usize,
    maximum_bytes: usize,
) -> Result<(), String> {
    if diagnostic_bytes > maximum_bytes {
        return Err(format!(
            "handoff native Input diagnostics retain {diagnostic_bytes} bytes; maximum is {maximum_bytes}"
        ));
    }
    Ok(())
}

mod handoff_bytes {
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    use serde::{Deserialize as _, Deserializer, Serializer, de::Error as _};

    pub(super) fn serialize<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&STANDARD.encode(bytes))
    }

    pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let encoded = String::deserialize(deserializer)?;
        STANDARD.decode(encoded).map_err(D::Error::custom)
    }
}

impl InputOperationEntry {
    fn request(&self) -> &InputOperationRequest {
        match self {
            Self::Pending { request, .. }
            | Self::Completed { request, .. }
            | Self::Unknown { request, .. } => request,
        }
    }
}

trait PtyControl: Send {
    fn resize(&self, size: PtySize) -> io::Result<()>;
    fn get_size(&self) -> io::Result<PtySize>;
    // Exercised through the owner accessor's test today; the exec-in-place
    // handoff that carries this fd across the re-exec lands in a later task.
    #[cfg_attr(not(test), allow(dead_code))]
    fn master_raw_fd(&self) -> Option<std::os::fd::RawFd>;
    #[cfg(target_os = "macos")]
    fn interrupt_foreground(&self) -> io::Result<()>;
    fn foreground_process_group(&self) -> Option<u32> {
        None
    }
}

struct PortablePtyControl(Box<dyn MasterPty + Send>);

impl PtyControl for PortablePtyControl {
    fn resize(&self, size: PtySize) -> io::Result<()> {
        self.0.resize(size).map_err(io::Error::other)
    }

    fn get_size(&self) -> io::Result<PtySize> {
        self.0.get_size().map_err(io::Error::other)
    }

    fn master_raw_fd(&self) -> Option<std::os::fd::RawFd> {
        self.0.as_raw_fd()
    }

    #[cfg(target_os = "macos")]
    fn interrupt_foreground(&self) -> io::Result<()> {
        let raw_fd = self
            .0
            .as_raw_fd()
            .ok_or_else(|| io::Error::other("PTY master does not expose a raw descriptor"))?;
        ctxmux_pty_signal::interrupt_foreground(raw_fd)
    }

    fn foreground_process_group(&self) -> Option<u32> {
        self.0
            .process_group_leader()
            .and_then(|pid| u32::try_from(pid).ok())
    }
}

/// Bridge the inherited-fd adapter onto this module's private `PtyControl`
/// surface. Kept here — rather than in `adopted_pty` — so the trait stays
/// private to `native_control` while the adapter carries only pure fd
/// operations. The exec-in-place recovery path (a later task) builds a
/// `Box<dyn PtyControl>` from a recovered master through this impl.
#[cfg_attr(not(test), allow(dead_code))]
impl PtyControl for AdoptedMasterPty {
    fn resize(&self, size: PtySize) -> io::Result<()> {
        AdoptedMasterPty::resize(self, size)
    }

    fn get_size(&self) -> io::Result<PtySize> {
        AdoptedMasterPty::get_size(self)
    }

    fn master_raw_fd(&self) -> Option<std::os::fd::RawFd> {
        Some(AdoptedMasterPty::master_raw_fd(self))
    }

    #[cfg(target_os = "macos")]
    fn interrupt_foreground(&self) -> io::Result<()> {
        AdoptedMasterPty::interrupt_foreground(self)
    }

    fn foreground_process_group(&self) -> Option<u32> {
        AdoptedMasterPty::foreground_process_group(self)
    }
}

impl NativeControlOwner {
    /// Bound by the sole Native registration from its original `NativeSession`,
    /// never guessed from a client `RunInfo`. Failure only disables observation.
    #[cfg(target_os = "macos")]
    pub(crate) fn bind_foreground_root(&self, pid: Option<u32>) {
        let root = pid.and_then(|pid| {
            ctxmux_process_stats::process_incarnation(pid)
                .ok()
                .map(|identity| (pid, identity))
        });
        let _ = self.inner.foreground_root.set(root);
    }

    /// Read the current original-master scope without exporting a borrowed fd.
    /// Called by the bounded read-only worker, never the shared Native turn.
    #[cfg(target_os = "macos")]
    pub(crate) fn foreground_scope(&self) -> Option<(u32, String, u32)> {
        if self.inner.entry_retired.load(Ordering::Acquire) {
            return None;
        }
        let root = self.inner.foreground_root.get()?.as_ref()?;
        if ctxmux_process_stats::process_incarnation(root.0)
            .ok()?
            .as_str()
            != root.1
        {
            return None;
        }
        let state = self.inner.try_state()?;
        if state.phase != ControlPhase::Open {
            return None;
        }
        let pty = self.inner.pty.try_lock().ok()?;
        let group = pty.as_ref()?.foreground_process_group()?;
        if self.inner.entry_retired.load(Ordering::Acquire) {
            return None;
        }
        Some((root.0, root.1.clone(), group))
    }

    pub(crate) fn try_turn(&self) -> Option<NativeControlTurn<'_>> {
        self.inner
            .try_state()
            .map(|state| NativeControlTurn { state })
    }

    pub(crate) fn bind_service(&self, service: NativeService) {
        assert!(
            self.inner.service.set(service).is_ok(),
            "one Native control service binding"
        );
        self.inner.publish_input(&self.inner.lock_state());
    }

    #[cfg(test)]
    pub(crate) fn input_service(&self) -> NativeInputStatus {
        input_status(&self.inner.lock_state())
    }

    fn schedule_input(&self) {
        #[cfg(test)]
        if matches!(
            &*mutex_lock(&self.inner.writer),
            Some(NativeInputWriter::Opaque(_))
        ) {
            self.inner.input_drains.schedule(Arc::clone(&self.inner));
            return;
        }
        self.inner.owner_wake.wake();
    }

    /// Borrow one already-owned unbuffered FD through an Arc, never dup a new
    /// descriptor and never retain the writer mutex across blocking poll.
    pub(crate) fn input_poll_file(&self) -> Option<Arc<File>> {
        let state = self.inner.try_state()?;
        if state.phase != ControlPhase::Open
            || state.input_failure.is_some()
            || self.inner.owner_failure.get().is_some()
            || state.input_queue.is_empty()
        {
            return None;
        }
        match &*mutex_lock(&self.inner.writer) {
            Some(NativeInputWriter::File(file)) => Some(Arc::clone(file)),
            #[cfg(test)]
            Some(NativeInputWriter::Opaque(_)) => None,
            None => None,
        }
    }

    pub(crate) fn progress_empty_input(&self, max_commands: usize) {
        for _ in 0..max_commands {
            let empty = {
                let Some(state) = self.inner.try_state() else {
                    return;
                };
                state
                    .input_queue
                    .front()
                    .is_some_and(|command| command.data.is_empty())
                    && matches!(
                        &*mutex_lock(&self.inner.writer),
                        Some(NativeInputWriter::File(_))
                    )
            };
            if !empty {
                break;
            }
            self.progress_input(1, 1);
        }
    }

    #[cfg(test)]
    pub(crate) fn new_opaque_for_owner_test(
        run_id: RunId,
        master: Box<dyn MasterPty + Send>,
        writer: Box<dyn Write + Send>,
        input_drains: InputDrainGate,
        owner_wake: OwnerWake,
    ) -> Self {
        let resources = input_drains.inner.resources;
        Self::new_with_components(
            run_id,
            Some(Box::new(PortablePtyControl(master))),
            Some(NativeInputWriter::Opaque(writer)),
            input_drains,
            owner_wake,
            resources.input_result_entries,
            resources.input_result_bytes,
            HandoffInputState::empty(),
            false,
        )
    }

    /// The readiness-driven owner spends at most one configured turn per Run.
    /// The state lock fences Stop/close with short nonblocking write syscalls;
    /// no command can interleave its bytes with the next accepted command.
    #[allow(
        clippy::too_many_lines,
        reason = "one physical input turn keeps confirmed prefix, whole-command cursor and funded ledger transitions auditable"
    )]
    pub(crate) fn progress_input(&self, max_commands: usize, max_bytes: usize) {
        let mut commands = 0;
        let mut bytes = 0;
        while commands < max_commands && bytes < max_bytes {
            let Some(mut state) = self.inner.try_state() else {
                return;
            };
            if state.phase != ControlPhase::Open
                || state.input_failure.is_some()
                || self.inner.owner_failure.get().is_some()
            {
                return;
            }
            let Some(front) = state.input_queue.front() else {
                state.input_scheduled = false;
                state.input_blocked = false;
                self.inner.publish_input(&state);
                return;
            };
            let data = Arc::clone(&front.data);
            let offset = front.confirmed;
            let expected = match &front.reply {
                InputReply::Legacy(_) => state.applied_input_bytes,
                InputReply::Recoverable { key, .. } => {
                    state
                        .input_operations
                        .get(key)
                        .expect("queued Input retains its funded ledger entry")
                        .request()
                        .expected_byte
                }
            };
            let end = state.applied_input_bytes.checked_add(data.len() as u64);
            if expected != state.applied_input_bytes || end.is_none() {
                debug_assert_eq!(offset, 0, "cursor checks precede every physical attempt");
                let command = state.input_queue.pop_front().unwrap();
                release_input_capacity(&mut state, data.len());
                if let InputReply::Recoverable { key, .. } = &command.reply {
                    remove_input_operation(&mut state, key);
                }
                let failure = not_applied(ProtocolError::new(
                    ErrorCode::InputCursorMismatch,
                    format!(
                        "Run {} applied-input cursor is {}, not expected {expected}, or exhausted",
                        self.inner.run_id, state.applied_input_bytes
                    ),
                ));
                self.inner.publish_input(&state);
                drop(state);
                resolve_input_reply(command.reply, Err(failure));
                commands += 1;
                continue;
            }
            let length = (data.len() - offset).min(max_bytes - bytes);
            let result = if length == 0 {
                Ok(Ok(0))
            } else {
                catch_unwind(AssertUnwindSafe(|| {
                    let writer = mutex_lock(&self.inner.writer);
                    match &*writer {
                        Some(NativeInputWriter::File(file)) => {
                            (&**file).write(&data[offset..offset + length])
                        }
                        #[cfg(test)]
                        Some(NativeInputWriter::Opaque(_)) => {
                            unreachable!("opaque fixture writer has its own test-only owner")
                        }
                        None => Err(io::Error::new(
                            io::ErrorKind::BrokenPipe,
                            "PTY input descriptor is closed",
                        )),
                    }
                }))
            };
            match result {
                Ok(Ok(written)) if written > 0 || data.is_empty() => {
                    debug_assert!(written <= length);
                    state.input_blocked = false;
                    bytes += written;
                    let front = state.input_queue.front_mut().unwrap();
                    front.confirmed += written;
                    if front.confirmed != data.len() {
                        self.inner.publish_input(&state);
                        continue;
                    }
                    let command = state.input_queue.pop_front().unwrap();
                    let range = AppliedInputRange {
                        start_byte: state.applied_input_bytes,
                        end_byte: end.unwrap(),
                    };
                    state.applied_input_bytes = range.end_byte;
                    release_input_capacity(&mut state, data.len());
                    if let InputReply::Recoverable { key, .. } = &command.reply {
                        let entry = state.input_operations.get_mut(key).unwrap();
                        let request = entry.request().clone();
                        *entry = InputOperationEntry::Completed { request, range };
                        state.completed_input_operations.push_back(key.clone());
                    }
                    state.input_scheduled = !state.input_queue.is_empty();
                    self.inner.publish_input(&state);
                    drop(state);
                    resolve_input_reply(command.reply, Ok(range));
                    commands += 1;
                }
                Ok(Err(error)) if error.kind() == io::ErrorKind::WouldBlock => {
                    state.input_blocked = true;
                    self.inner.publish_input(&state);
                    return;
                }
                Ok(Err(error)) if error.kind() == io::ErrorKind::Interrupted => {
                    self.inner.publish_input(&state);
                    return;
                }
                result => {
                    let (code, detail) = match result {
                        Ok(Ok(_)) => (ErrorCode::Io, "PTY input write returned zero".to_owned()),
                        Ok(Err(error)) => {
                            (ErrorCode::Io, format!("PTY input I/O failure: {error}"))
                        }
                        Err(_) => (
                            ErrorCode::Internal,
                            "PTY input writer unwound before confirming its attempt".to_owned(),
                        ),
                    };
                    let command = state.input_queue.pop_front().unwrap();
                    release_input_capacity(&mut state, command.data.len());
                    let error = input_failure(&mut state, self.inner.run_id, code, &detail);
                    let mut failure = unknown(error.clone());
                    failure.confirmed_input_bytes = Some(command.confirmed);
                    retain_unknown_input_operation(&mut state, &command.reply, &failure);
                    let rejected = reject_queued_inputs(&mut state, &error);
                    self.inner.publish_input(&state);
                    drop(state);
                    resolve_input_reply(command.reply, Err(failure));
                    send_rejections(rejected);
                    return;
                }
            }
        }
    }

    /// Completion of the unique owner fences future attempts before settling
    /// queued work. Child/session identity and lifecycle are not rewritten.
    pub(crate) fn fence_owner_loss(&self, reason: NativeServiceFailure) {
        let _ = self.inner.owner_failure.set(reason);
        self.inner.admission_changed.notify_waiters();
        // Busy control owners retain their actual unresolved commands. Their
        // guard settles on real release; no stopped poll thread or timer is
        // required, and completion continues fencing every other Run now.
        if let Some(state) = self.inner.try_state() {
            drop(state);
        }
    }

    /// Project control metadata under the same fence as input and resize.
    /// The callback may take output/service locks, never reenter controls.
    #[cfg(test)]
    pub(crate) fn with_metadata<R>(&self, read: impl FnOnce(u64, Option<TerminalSize>) -> R) -> R {
        let state = self.inner.lock_state();
        read(state.applied_input_bytes, state.confirmed_size)
    }

    pub(crate) fn run_id(&self) -> RunId {
        self.inner.run_id
    }

    /// Raw fd number of the live PTY master, or `None` once the pty has been
    /// detached. Borrowed number only — no dup, no ownership transfer.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn master_raw_fd(&self) -> Option<std::os::fd::RawFd> {
        mutex_lock(&self.inner.pty)
            .as_ref()
            .and_then(|pty| pty.master_raw_fd())
    }

    pub(crate) fn new(
        run_id: RunId,
        master: Box<dyn MasterPty + Send>,
        writer: File,
        input_drains: InputDrainGate,
        owner_wake: OwnerWake,
    ) -> Self {
        let limits = input_drains.inner.resources;
        Self::new_with_components(
            run_id,
            Some(Box::new(PortablePtyControl(master))),
            Some(NativeInputWriter::File(Arc::new(writer))),
            input_drains,
            owner_wake,
            limits.input_result_entries,
            limits.input_result_bytes,
            HandoffInputState::empty(),
            false,
        )
    }

    /// Rebind current-incarnation control onto a PTY master inherited across an
    /// exec-in-place upgrade.
    ///
    /// The spawn seam wraps a freshly opened `portable_pty` master
    /// ([`new`](Self::new)); the exec-in-place recovery path instead adopts a
    /// bare master fd via [`AdoptedMasterPty`], which already satisfies the same
    /// private `PtyControl` surface — so this is only a boxing shim, carrying no
    /// new state.
    pub(crate) fn new_adopted(
        run_id: RunId,
        adopted: AdoptedMasterPty,
        writer: File,
        input_drains: InputDrainGate,
        owner_wake: OwnerWake,
        input_state: HandoffInputState,
    ) -> Self {
        let limits = input_drains.inner.resources;
        Self::new_with_components(
            run_id,
            Some(Box::new(adopted)),
            Some(NativeInputWriter::File(Arc::new(writer))),
            input_drains,
            owner_wake,
            limits.input_result_entries,
            limits.input_result_bytes,
            input_state,
            false,
        )
    }

    #[cfg(test)]
    pub(crate) fn new_for_wait_test(run_id: RunId, owner_wake: OwnerWake) -> Self {
        struct TestPty;

        impl PtyControl for TestPty {
            fn resize(&self, _size: PtySize) -> io::Result<()> {
                Ok(())
            }

            fn get_size(&self) -> io::Result<PtySize> {
                Ok(PtySize::default())
            }

            fn master_raw_fd(&self) -> Option<std::os::fd::RawFd> {
                None
            }

            #[cfg(target_os = "macos")]
            fn interrupt_foreground(&self) -> io::Result<()> {
                Err(io::Error::other("test PTY has no foreground owner"))
            }

            #[cfg(not(target_os = "macos"))]
            fn foreground_process_group(&self) -> Option<u32> {
                None
            }
        }

        Self::new_with_pty(
            run_id,
            Box::new(TestPty),
            Box::new(io::sink()),
            InputDrainGate::default(),
            owner_wake,
        )
    }

    #[cfg(test)]
    pub(crate) fn retained_pty_reader_for_test(&self) -> io::Result<File> {
        let writer = mutex_lock(&self.inner.writer);
        match writer.as_ref() {
            Some(NativeInputWriter::File(file)) => file.try_clone(),
            _ => Err(io::Error::other("test requires an actual retained PTY")),
        }
    }

    #[cfg(test)]
    pub(crate) fn cleanup_retained_owner_child_for_test(
        &self,
        session: &mut crate::native_session::NativeSession,
    ) -> Result<(), String> {
        let mut reap = mutex_lock(&self.inner.reap);
        let ChildReapState::OwnerStopped { _child: child, .. } = &mut *reap else {
            return Err("test requires actual retained child".to_owned());
        };
        session.stop(
            child.as_mut(),
            crate::STOP_GRACEFUL_TIMEOUT,
            crate::STOP_FORCED_TIMEOUT,
        )?;
        *reap = ChildReapState::Reaped;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn retains_failed_child(&self) -> bool {
        matches!(
            &*mutex_lock(&self.inner.reap),
            ChildReapState::WaitAuthorityLost { .. } | ChildReapState::OwnerStopped { .. }
        )
    }

    pub(crate) fn wait_authority_failure(&self) -> Option<String> {
        match &*mutex_lock(&self.inner.reap) {
            ChildReapState::WaitAuthorityLost { wait_error, .. } => Some(wait_error.clone()),
            ChildReapState::Pending { .. }
            | ChildReapState::OwnerStopped { .. }
            | ChildReapState::Reaped => None,
        }
    }

    #[cfg(test)]
    fn new_with_pty(
        run_id: RunId,
        pty: Box<dyn PtyControl>,
        writer: Box<dyn Write + Send>,
        input_drains: InputDrainGate,
        owner_wake: OwnerWake,
    ) -> Self {
        let limits = input_drains.inner.resources;
        Self::new_with_pty_and_input_results(
            run_id,
            pty,
            writer,
            input_drains,
            owner_wake,
            limits.input_result_entries,
            limits.input_result_bytes,
        )
    }

    #[cfg(test)]
    fn new_with_pty_and_input_results(
        run_id: RunId,
        pty: Box<dyn PtyControl>,
        writer: Box<dyn Write + Send>,
        input_drains: InputDrainGate,
        owner_wake: OwnerWake,
        input_result_max_entries: usize,
        input_result_max_request_bytes: usize,
    ) -> Self {
        Self::new_with_pty_and_input_state(
            run_id,
            pty,
            writer,
            input_drains,
            owner_wake,
            input_result_max_entries,
            input_result_max_request_bytes,
            HandoffInputState::empty(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    #[cfg(test)]
    fn new_with_pty_and_input_state(
        run_id: RunId,
        pty: Box<dyn PtyControl>,
        writer: Box<dyn Write + Send>,
        input_drains: InputDrainGate,
        owner_wake: OwnerWake,
        input_result_max_entries: usize,
        input_result_max_request_bytes: usize,
        input_state: HandoffInputState,
    ) -> Self {
        Self::new_with_components(
            run_id,
            Some(pty),
            Some(NativeInputWriter::Opaque(writer)),
            input_drains,
            owner_wake,
            input_result_max_entries,
            input_result_max_request_bytes,
            input_state,
            false,
        )
    }

    pub(crate) fn closed_with_input_state(
        run_id: RunId,
        input_state: HandoffInputState,
        input_drains: InputDrainGate,
        owner_wake: OwnerWake,
    ) -> Self {
        let limits = input_drains.inner.resources;
        Self::new_with_components(
            run_id,
            None,
            None,
            input_drains,
            owner_wake,
            limits.input_result_entries,
            limits.input_result_bytes,
            input_state,
            true,
        )
    }

    #[allow(
        clippy::too_many_arguments,
        clippy::too_many_lines,
        reason = "one constructor validates and funds the complete adopted input ledger before exposing its owner"
    )]
    fn new_with_components(
        run_id: RunId,
        pty: Option<Box<dyn PtyControl>>,
        writer: Option<NativeInputWriter>,
        input_drains: InputDrainGate,
        owner_wake: OwnerWake,
        input_result_max_entries: usize,
        input_result_max_request_bytes: usize,
        input_state: HandoffInputState,
        closed: bool,
    ) -> Self {
        debug_assert!(input_result_max_entries > 0);
        debug_assert!(input_result_max_request_bytes > 0);
        // Read the master before it moves into the mutex. A failure here is not
        // fatal: the Run is fully usable, it just has no confirmed size to
        // report until its first resize supplies one.
        let pty_size = pty.as_ref().and_then(|pty| pty.get_size().ok());
        input_state
            .validate_with_resources(input_drains.inner.resources)
            .expect("re-adopted native Input state is validated before construction");
        let diagnostic_memory_bytes = input_state.additional_diagnostic_memory_bytes();
        let adopted_diagnostic_memory = (diagnostic_memory_bytes > 0).then(|| {
            input_drains.inner.control_budget.reserve(diagnostic_memory_bytes)
                .expect("handed-off aggregate diagnostics were admitted by the preserved control policy")
        });
        let retained_input_request_bytes = input_state
            .operations
            .iter()
            .map(|operation| match operation {
                HandoffInputOperation::Completed { data, .. }
                | HandoffInputOperation::Unknown { data, .. } => data.len(),
            })
            .sum();
        let mut input_operations = HashMap::with_capacity(input_state.operations.len());
        let mut completed_input_operations = VecDeque::new();
        for operation in input_state.operations {
            match operation {
                HandoffInputOperation::Completed {
                    key,
                    expected_byte,
                    data,
                    range,
                } => {
                    input_operations.insert(
                        key.clone(),
                        InputOperationEntry::Completed {
                            request: InputOperationRequest {
                                memory: Some(Arc::new(input_drains.inner.control_budget.reserve(input_receipt_charge(&key, &data)).expect("handed-off control state was admitted by the same policy"))),
                                expected_byte,
                                data: Arc::from(data),
                            },
                            range,
                        },
                    );
                    completed_input_operations.push_back(key);
                }
                HandoffInputOperation::Unknown {
                    key,
                    expected_byte,
                    data,
                    failure,
                } => {
                    input_operations.insert(
                        key.clone(),
                        InputOperationEntry::Unknown {
                            request: InputOperationRequest {
                                memory: Some(Arc::new(input_drains.inner.control_budget.reserve(input_receipt_charge(&key, &data)).expect("handed-off control state was admitted by the same policy"))),
                                expected_byte,
                                data: Arc::from(data),
                            },
                            failure,
                        },
                    );
                }
            }
        }
        Self {
            inner: Arc::new(NativeControlInner {
                run_id,
                pty: Mutex::new(pty),
                writer: Mutex::new(writer),
                service: OnceLock::new(),
                state: Mutex::new(NativeControlState {
                    phase: if closed {
                        ControlPhase::Closed
                    } else {
                        ControlPhase::Open
                    },
                    // Ask the master what it actually opened rather than
                    // echoing the requested size back. The kernel is free to
                    // clamp, and the requested value already lives in the
                    // Run's `spec` -- a `current_size` that merely repeated it
                    // would report an unconfirmed number as confirmed truth.
                    confirmed_size: confirmed_size(pty_size),
                    input_failure: input_state.input_failure,
                    input_queue: VecDeque::new(),
                    input_commands: 0,
                    input_bytes: 0,
                    input_scheduled: false,
                    input_blocked: false,
                    owner_failure: None,
                    applied_input_bytes: input_state.applied_input_bytes,
                    input_operations,
                    completed_input_operations,
                    retained_input_request_bytes,
                    input_result_max_entries,
                    input_result_max_request_bytes,
                    child_open: !closed,
                    stop_pending: false,
                    child_commands: VecDeque::new(),
                }),
                owner_deferred: AtomicBool::new(false),
                admission_changed: Notify::new(),
                owner_failure: OnceLock::new(),
                entry_retired: AtomicBool::new(closed),
                #[cfg(target_os = "macos")]
                foreground_root: OnceLock::new(),
                reap: Mutex::new(if closed {
                    ChildReapState::Reaped
                } else {
                    ChildReapState::Pending {
                        cleanup_error: None,
                        wait_error: None,
                    }
                }),
                reap_changed: Condvar::new(),
                input_drains,
                owner_wake,
                _adopted_diagnostic_memory: adopted_diagnostic_memory,
            }),
        }
    }

    pub(crate) fn admission_changed(&self) -> &Notify {
        &self.inner.admission_changed
    }

    #[cfg(test)]
    pub(crate) fn begin_input(&self, data: Vec<u8>) -> Result<PendingInput, ControlFailure> {
        self.begin_input_locked(Arc::from(data), self.inner.try_admission_state()?, None)
    }

    pub(crate) async fn begin_input_async(
        &self,
        data: Vec<u8>,
    ) -> Result<PendingInput, ControlFailure> {
        if self.inner.owner_failure.get().is_some() {
            return Err(not_applied(ProtocolError::new(
                ErrorCode::BackendUnavailable,
                "Native owner stopped before command admission",
            )));
        }
        let data: Arc<[u8]> = Arc::from(data);
        if let Some(state) = self.inner.try_state() {
            return self.begin_input_locked(data, state, None);
        }
        let memory = self
            .inner
            .input_drains
            .inner
            .control_budget
            .reserve(data.len() + std::mem::size_of::<InputCommand>())
            .ok_or_else(|| {
                not_applied(ProtocolError::new(
                    ErrorCode::ControlBackpressure,
                    "daemon input waiting-payload byte budget is full",
                ))
            })?;
        let state = self.inner.wait_admission_state(false).await?;
        self.begin_input_locked(data, state, Some(memory))
    }

    pub(crate) fn reserve_control_memory(
        &self,
        bytes: usize,
    ) -> Option<crate::resources::BytePermit> {
        self.inner.input_drains.inner.control_budget.reserve(bytes)
    }

    pub(crate) fn reserve_input_payload(
        &self,
        bytes: usize,
    ) -> Option<crate::resources::BytePermit> {
        self.reserve_control_memory(bytes.checked_add(std::mem::size_of::<InputCommand>())?)
    }

    pub(crate) async fn begin_input_async_funded(
        &self,
        data: Vec<u8>,
        memory: crate::resources::BytePermit,
    ) -> Result<PendingInput, ControlFailure> {
        let data = Arc::from(data);
        let state = self.inner.wait_admission_state(false).await?;
        self.begin_input_locked(data, state, Some(memory))
    }

    fn begin_input_locked(
        &self,
        data: Arc<[u8]>,
        state: ControlStateGuard<'_>,
        memory: Option<crate::resources::BytePermit>,
    ) -> Result<PendingInput, ControlFailure> {
        let (reply, schedule) = {
            let mut state = state;
            if state.phase != ControlPhase::Open {
                return Err(not_applied(invalid_phase_error(
                    self.inner.run_id,
                    state.phase,
                    "write to",
                )));
            }
            if self.inner.owner_failure.get().is_some() {
                return Err(not_applied(ProtocolError::new(
                    ErrorCode::BackendUnavailable,
                    "daemon-wide Native input owner stopped",
                )));
            }
            if let Some(error) = &state.input_failure {
                return Err(not_applied(error.clone()));
            }
            if state.input_commands >= self.inner.input_drains.inner.resources.input_queue_commands
                || data.len()
                    > self
                        .inner
                        .input_drains
                        .inner
                        .resources
                        .input_queue_bytes
                        .saturating_sub(state.input_bytes)
            {
                return Err(not_applied(ProtocolError::new(
                    ErrorCode::ControlBackpressure,
                    format!(
                        "Run {} PTY input queue exceeds its {}-command or {}-byte budget",
                        self.inner.run_id,
                        self.inner.input_drains.inner.resources.input_queue_commands,
                        self.inner.input_drains.inner.resources.input_queue_bytes
                    ),
                )));
            }

            let memory = memory
                .or_else(|| {
                    self.inner
                        .input_drains
                        .inner
                        .control_budget
                        .reserve(data.len() + std::mem::size_of::<InputCommand>())
                })
                .ok_or_else(|| {
                    not_applied(ProtocolError::new(
                        ErrorCode::ControlBackpressure,
                        "daemon input queue byte budget is full",
                    ))
                })?;
            let (reply_tx, reply_rx) = oneshot::channel();
            state.input_commands += 1;
            state.input_bytes += data.len();
            state.input_queue.push_back(InputCommand {
                _memory: Some(memory),
                data,
                confirmed: 0,
                reply: InputReply::Legacy(reply_tx),
            });
            let schedule = !state.input_scheduled;
            if schedule {
                state.input_scheduled = true;
            }
            self.inner.publish_input(&state);
            (reply_rx, schedule)
        };
        if schedule {
            self.schedule_input();
        }

        Ok(PendingInput {
            run_id: self.inner.run_id,
            reply,
        })
    }

    fn input_request(
        key: &InputOperationKey,
        expected_byte: u64,
        data: Vec<u8>,
    ) -> Result<InputOperationRequest, ControlFailure> {
        key.validate().map_err(|error| {
            not_applied(ProtocolError::new(
                ErrorCode::InvalidRequest,
                error.to_string(),
            ))
        })?;
        if data.is_empty() {
            return Err(not_applied(ProtocolError::new(
                ErrorCode::InvalidRequest,
                "recoverable native Input must not be empty",
            )));
        }
        Ok(InputOperationRequest {
            memory: None,
            expected_byte,
            data: Arc::from(data),
        })
    }

    #[cfg(test)]
    pub(crate) fn begin_recoverable_input(
        &self,
        key: InputOperationKey,
        expected_byte: u64,
        data: Vec<u8>,
    ) -> Result<PendingRecoverableInput, ControlFailure> {
        let request = Self::input_request(&key, expected_byte, data)?;
        let state = self.inner.try_state().ok_or_else(|| {
            unknown(ProtocolError::new(
                if self.inner.owner_failure.get().is_some() {
                    ErrorCode::BackendUnavailable
                } else {
                    ErrorCode::ControlBackpressure
                },
                "Native input result owner is busy; application status is unknown",
            ))
        })?;
        self.begin_recoverable_input_locked(key, request, state)
    }

    pub(crate) async fn begin_recoverable_input_async(
        &self,
        key: InputOperationKey,
        expected_byte: u64,
        data: Vec<u8>,
    ) -> Result<PendingRecoverableInput, ControlFailure> {
        let mut request = Self::input_request(&key, expected_byte, data)?;
        // Inspect the retained key before requesting any additional budget.
        if let Some(state) = self.inner.try_state() {
            return self.begin_recoverable_input_locked(key, request, state);
        }
        request.memory = Some(Arc::new(self.inner.input_drains.inner.control_budget
            .reserve(input_receipt_charge(&key, &request.data))
            .ok_or_else(|| unknown(ProtocolError::new(
                ErrorCode::ControlBackpressure,
                "Native result ledger is busy and waiting-payload budget is full; application status is unknown",
            )))?));
        let state = self.inner.wait_admission_state(true).await?;
        self.begin_recoverable_input_locked(key, request, state)
    }

    #[allow(
        clippy::too_many_lines,
        reason = "duplicate resolution and funded admission precede all PTY effects"
    )]
    fn begin_recoverable_input_locked(
        &self,
        key: InputOperationKey,
        mut request: InputOperationRequest,
        state: ControlStateGuard<'_>,
    ) -> Result<PendingRecoverableInput, ControlFailure> {
        let (pending, schedule) = {
            let mut state = state;
            if let Some(retained) =
                retained_input_result(&state, &key, &request, self.inner.run_id)?
            {
                return Ok(retained);
            }
            if state.phase != ControlPhase::Open {
                return Err(not_applied(invalid_phase_error(
                    self.inner.run_id,
                    state.phase,
                    "write to",
                )));
            }
            if self.inner.owner_failure.get().is_some() {
                return Err(not_applied(ProtocolError::new(
                    ErrorCode::BackendUnavailable,
                    "daemon-wide Native input owner stopped",
                )));
            }
            if let Some(error) = &state.input_failure {
                return Err(not_applied(error.clone()));
            }
            evict_completed_input_results(&mut state, request.data.len());
            if state.input_operations.len() >= state.input_result_max_entries
                || request.data.len()
                    > state
                        .input_result_max_request_bytes
                        .saturating_sub(state.retained_input_request_bytes)
                || state.input_commands
                    >= self.inner.input_drains.inner.resources.input_queue_commands
                || request.data.len()
                    > self
                        .inner
                        .input_drains
                        .inner
                        .resources
                        .input_queue_bytes
                        .saturating_sub(state.input_bytes)
            {
                return Err(not_applied(ProtocolError::new(
                    ErrorCode::ControlBackpressure,
                    format!(
                        "Run {} recoverable Input result or queue capacity is full",
                        self.inner.run_id
                    ),
                )));
            }

            if request.memory.is_none() {
                request.memory = Some(Arc::new(
                    self.inner
                        .input_drains
                        .inner
                        .control_budget
                        .reserve(input_receipt_charge(&key, &request.data))
                        .ok_or_else(|| {
                            not_applied(ProtocolError::new(
                                ErrorCode::ControlBackpressure,
                                "daemon control-state byte budget is full",
                            ))
                        })?,
                ));
            }
            let (completion, result) = watch::channel(None);
            state.input_commands += 1;
            state.input_bytes += request.data.len();
            state.retained_input_request_bytes += request.data.len();
            state.input_operations.insert(
                key.clone(),
                InputOperationEntry::Pending {
                    request: request.clone(),
                    completion: completion.clone(),
                },
            );
            state.input_queue.push_back(InputCommand {
                _memory: None,
                data: Arc::clone(&request.data),
                confirmed: 0,
                reply: InputReply::Recoverable { key, completion },
            });
            let schedule = !state.input_scheduled;
            if schedule {
                state.input_scheduled = true;
            }
            self.inner.publish_input(&state);
            (
                PendingRecoverableInput::Pending {
                    run_id: self.inner.run_id,
                    result,
                },
                schedule,
            )
        };
        if schedule {
            self.schedule_input();
        }
        Ok(pending)
    }

    #[cfg(test)]
    pub(crate) fn applied_input_bytes(&self) -> u64 {
        self.inner.lock_state().applied_input_bytes
    }

    /// Snapshot the complete bounded recoverable-Input contract after the
    /// daemon request gate has drained. This is a precondition check as well as
    /// serialization: a pending input/child command makes extraction fail
    /// before any ownership is relinquished.
    pub(crate) fn handoff_input_state(&self) -> Result<HandoffInputState, String> {
        let state = self
            .inner
            .try_state()
            .ok_or("Native control is busy; handoff not admitted")?;
        if !matches!(state.phase, ControlPhase::Open | ControlPhase::Closed)
            || state.stop_pending
            || !state.child_commands.is_empty()
            || state.input_scheduled
            || state.input_commands != 0
            || state.input_bytes != 0
            || !state.input_queue.is_empty()
        {
            return Err(format!(
                "Run {} still has a crossing native control at handoff",
                self.inner.run_id
            ));
        }

        let mut operations = Vec::with_capacity(state.input_operations.len());
        for key in &state.completed_input_operations {
            let Some(InputOperationEntry::Completed { request, range }) =
                state.input_operations.get(key)
            else {
                return Err(format!(
                    "Run {} completed native Input order is inconsistent",
                    self.inner.run_id
                ));
            };
            operations.push(HandoffInputOperation::Completed {
                key: key.clone(),
                expected_byte: request.expected_byte,
                data: request.data.to_vec(),
                range: *range,
            });
        }
        let mut unknown = state
            .input_operations
            .iter()
            .filter_map(|(key, entry)| match entry {
                InputOperationEntry::Unknown { request, failure } => Some((
                    key.clone(),
                    request.expected_byte,
                    request.data.to_vec(),
                    failure.clone(),
                )),
                InputOperationEntry::Pending { .. } | InputOperationEntry::Completed { .. } => None,
            })
            .collect::<Vec<_>>();
        if state
            .input_operations
            .values()
            .any(|entry| matches!(entry, InputOperationEntry::Pending { .. }))
        {
            return Err(format!(
                "Run {} has a pending recoverable Input at handoff",
                self.inner.run_id
            ));
        }
        unknown.sort_by(|left, right| left.0.as_str().cmp(right.0.as_str()));
        operations.extend(
            unknown
                .into_iter()
                .map(
                    |(key, expected_byte, data, failure)| HandoffInputOperation::Unknown {
                        key,
                        expected_byte,
                        data,
                        failure,
                    },
                ),
        );
        if operations.len() != state.input_operations.len() {
            return Err(format!(
                "Run {} native Input handoff ledger is incomplete",
                self.inner.run_id
            ));
        }
        let snapshot = HandoffInputState {
            applied_input_bytes: state.applied_input_bytes,
            input_failure: state.input_failure.clone(),
            operations,
        };
        snapshot.validate_with_resources(self.inner.input_drains.inner.resources)?;
        Ok(snapshot)
    }

    #[cfg(test)]
    pub(crate) fn begin_stop(&self) -> Result<PendingStop, ControlFailure> {
        if self.inner.owner_failure.get().is_some() {
            return Err(not_applied(ProtocolError::new(
                ErrorCode::BackendUnavailable,
                "daemon-wide Native owner stopped before control admission",
            )));
        }
        Ok(PendingStop {
            run_id: self.inner.run_id,
            reply: self.begin_stop_inner()?,
        })
    }

    #[cfg(test)]
    pub(crate) fn begin_signal(&self, signal: RunSignal) -> Result<PendingSignal, ControlFailure> {
        self.begin_signal_locked(signal, self.inner.try_admission_state()?)
    }

    pub(crate) async fn begin_signal_async(
        &self,
        signal: RunSignal,
    ) -> Result<PendingSignal, ControlFailure> {
        self.begin_signal_locked(signal, self.inner.wait_admission_state(false).await?)
    }

    fn begin_signal_locked(
        &self,
        signal: RunSignal,
        state: ControlStateGuard<'_>,
    ) -> Result<PendingSignal, ControlFailure> {
        let (reply_tx, reply_rx) = oneshot::channel();
        {
            // The non-macOS arm below pushes onto `state.child_commands`, so the
            // guard is a mutable borrow. macOS signals via the pty owner and only
            // reads `state`, so `mut` is unused there — which is why a darwin-only
            // build never catches a missing `mut`. Bind `mut` unconditionally and
            // silence the macOS-only `unused_mut` rather than duplicate the line.
            #[cfg_attr(target_os = "macos", allow(unused_mut))]
            let mut state = state;
            if self.inner.owner_failure.get().is_some() {
                return Err(not_applied(ProtocolError::new(
                    ErrorCode::BackendUnavailable,
                    "daemon-wide Native owner stopped before control admission",
                )));
            }
            if state.phase != ControlPhase::Open {
                return Err(not_applied(invalid_phase_error(
                    self.inner.run_id,
                    state.phase,
                    "signal",
                )));
            }
            if !state.child_open {
                return Err(not_applied(ProtocolError::new(
                    ErrorCode::InvalidRunState,
                    format!("cannot signal exited Run {}", self.inner.run_id),
                )));
            }
            #[cfg(target_os = "macos")]
            {
                let result = mutex_lock(&self.inner.pty)
                    .as_ref()
                    .ok_or_else(|| "Run PTY owner is no longer available".to_owned())
                    .and_then(|pty| {
                        pty.interrupt_foreground()
                            .map_err(|error| format!("failed to interrupt Run PTY: {error}"))
                    });
                let _ = reply_tx.send(result);
            }
            #[cfg(not(target_os = "macos"))]
            {
                let foreground_group = mutex_lock(&self.inner.pty)
                    .as_ref()
                    .and_then(|pty| pty.foreground_process_group())
                    .ok_or_else(|| {
                        not_applied(ProtocolError::new(
                            ErrorCode::InvalidRunState,
                            format!(
                                "Run {} has no current foreground process group",
                                self.inner.run_id
                            ),
                        ))
                    })?;
                state.child_commands.push_back(ChildCommand::Signal {
                    signal,
                    foreground_group,
                    reply: reply_tx,
                });
            }
        }
        self.inner.owner_wake.wake();
        Ok(PendingSignal {
            run_id: self.inner.run_id,
            signal,
            reply: reply_rx,
        })
    }

    /// Ask the daemon-wide child owner to clean up a Run rejected before durable
    /// publication. Completion remains a separate cleanup-owned reap receipt.
    pub(crate) fn cleanup_unpublished(&self) -> Result<(), String> {
        let rejected = {
            let mut state = self.inner.lock_state();
            if self.inner.owner_failure.get().is_some() {
                return Err("Native owner unavailable before unpublished cleanup".to_owned());
            }
            match state.phase {
                ControlPhase::Open => state.phase = ControlPhase::Stopping,
                ControlPhase::Stopping => return Ok(()),
                ControlPhase::Closed | ControlPhase::Failed => return self.reap_result(),
            }
            if !state.child_open {
                let error = format!(
                    "Run {} child owner channel closed before unpublished cleanup",
                    self.inner.run_id
                );
                self.record_cleanup_error(error.clone());
                return Err(error);
            }
            let rejected = reject_queued_inputs(
                &mut state,
                &ProtocolError::new(
                    ErrorCode::InvalidRunState,
                    format!("cannot write to stopping Run {}", self.inner.run_id),
                ),
            );
            state
                .child_commands
                .push_back(ChildCommand::CleanupUnpublished);
            self.inner.publish_input(&state);
            rejected
        };
        send_rejections(rejected);
        self.inner.owner_wake.wake();
        Ok(())
    }

    pub(crate) async fn begin_stop_async(&self) -> Result<PendingStop, ControlFailure> {
        let state = self.inner.wait_admission_state(false).await?;
        Ok(PendingStop {
            run_id: self.inner.run_id,
            reply: self.begin_stop_locked(state)?,
        })
    }

    #[cfg(test)]
    fn begin_stop_inner(&self) -> Result<oneshot::Receiver<StopOwnerResult>, ControlFailure> {
        self.begin_stop_locked(self.inner.try_admission_state()?)
    }

    fn begin_stop_locked(
        &self,
        state: ControlStateGuard<'_>,
    ) -> Result<oneshot::Receiver<StopOwnerResult>, ControlFailure> {
        let (reply_tx, reply_rx) = oneshot::channel();
        {
            let mut state = state;
            if self.inner.owner_failure.get().is_some() {
                return Err(not_applied(ProtocolError::new(
                    ErrorCode::BackendUnavailable,
                    "daemon-wide Native owner stopped before control admission",
                )));
            }
            if state.phase != ControlPhase::Open {
                return Err(not_applied(invalid_phase_error(
                    self.inner.run_id,
                    state.phase,
                    "stop",
                )));
            }
            if !state.child_open {
                state.phase = ControlPhase::Closed;
                return Err(not_applied(ProtocolError::new(
                    ErrorCode::InvalidRunState,
                    format!("cannot stop exited Run {}", self.inner.run_id),
                )));
            }
            if state.stop_pending {
                return Err(not_applied(ProtocolError::new(
                    ErrorCode::ControlBackpressure,
                    format!(
                        "Run {} already has one pending Stop admission",
                        self.inner.run_id
                    ),
                )));
            }
            state.stop_pending = true;
            state.child_commands.push_back(ChildCommand::Stop {
                reply: reply_tx,
                deadline: Instant::now()
                    + Duration::from_millis(
                        self.inner
                            .input_drains
                            .inner
                            .resources
                            .stop_admission_timeout_ms,
                    ),
            });
        }
        self.inner.owner_wake.wake();
        Ok(reply_rx)
    }

    #[cfg(test)]
    pub(crate) fn commit_pending_stop(&self) -> Result<(), ControlFailure> {
        NativeControlTurn {
            state: self.inner.lock_state(),
        }
        .commit_pending_stop()
    }

    /// Fence all future live control as soon as the waiter loses child
    /// authority, before terminal `RunState` publication can lag behind it.
    pub(crate) fn mark_closed(&self) {
        let (rejected, commands) = {
            let mut state = self.inner.lock_state();
            if state.phase != ControlPhase::Failed {
                state.phase = ControlPhase::Closed;
            }
            state.child_open = false;
            state.stop_pending = false;
            let phase = state.phase;
            let rejected = reject_queued_inputs(
                &mut state,
                &invalid_phase_error(self.inner.run_id, phase, "write to"),
            );
            self.inner.publish_input(&state);
            (rejected, std::mem::take(&mut state.child_commands))
        };
        send_rejections(rejected);
        reject_child_commands(
            commands,
            "native child owner is closed",
            ErrorCode::InvalidRunState,
        );
    }

    #[cfg(test)]
    pub(crate) fn drain_child_commands(&self) -> VecDeque<ChildCommand> {
        NativeControlTurn {
            state: self.inner.lock_state(),
        }
        .drain_child_commands()
    }

    /// Retain an actual child handle when its owner stops, without fabricating
    /// a waitid error. Only the stopped owner transfers this unreaped holder.
    pub(crate) fn retain_owner_stopped_child(&self, child: Box<dyn Child + Send + Sync>) {
        let mut reap = mutex_lock(&self.inner.reap);
        if let ChildReapState::Pending { cleanup_error, .. } = &mut *reap {
            *reap = ChildReapState::OwnerStopped {
                cleanup_error: cleanup_error.take(),
                _child: child,
            };
            self.inner.reap_changed.notify_all();
        } else {
            // No Drop-based cleanup may masquerade as reap if an interrupted
            // owner transition left a more specific authority fact behind.
            std::mem::forget(child);
        }
    }

    /// Irreversibly fence live control after the child waiter can no longer
    /// observe process status. This does not claim exit or reap.
    pub(crate) fn mark_wait_authority_lost(
        &self,
        error: String,
        child: Box<dyn Child + Send + Sync>,
    ) {
        {
            let mut reap = mutex_lock(&self.inner.reap);
            if let ChildReapState::Pending { cleanup_error, .. } = &mut *reap {
                *reap = ChildReapState::WaitAuthorityLost {
                    cleanup_error: cleanup_error.take(),
                    wait_error: error.clone(),
                    _child: child,
                };
                self.inner.reap_changed.notify_all();
            } else {
                unreachable!("child wait authority is lost at most once");
            }
        }
        let (rejected, commands) = {
            let mut state = self.inner.lock_state();
            state.phase = ControlPhase::Failed;
            state.child_open = false;
            state.stop_pending = false;
            let rejected = reject_queued_inputs(
                &mut state,
                &ProtocolError::new(ErrorCode::BackendUnavailable, error),
            );
            self.inner.publish_input(&state);
            (rejected, std::mem::take(&mut state.child_commands))
        };
        send_rejections(rejected);
        reject_child_commands(
            commands,
            "native child wait authority was lost",
            ErrorCode::BackendUnavailable,
        );
    }

    pub(crate) fn has_continuation_authority(&self) -> Result<bool, ProtocolError> {
        let state = self.inner.try_state().ok_or_else(|| {
            ProtocolError::new(
                ErrorCode::ControlBackpressure,
                "Native continuation authority owner is busy; authority is not yet confirmed",
            )
        })?;
        Ok(state.phase == ControlPhase::Open && self.inner.owner_failure.get().is_none())
    }

    pub(crate) async fn has_continuation_authority_async(&self) -> Result<bool, ProtocolError> {
        let state = self
            .inner
            .wait_admission_state(false)
            .await
            .map_err(|failure| failure.error)?;
        Ok(state.phase == ControlPhase::Open && self.inner.owner_failure.get().is_none())
    }

    /// Record the only successful terminal-and-reaped proof: the waiter kept
    /// the leader waitable through session cleanup, then completed its final
    /// `child.wait()` before any authority-loss transfer. Once the handle moves
    /// into `WaitAuthorityLost`, this cannot replace it.
    pub(crate) fn mark_reaped(&self) {
        let mut reap = mutex_lock(&self.inner.reap);
        if matches!(&*reap, ChildReapState::Pending { .. }) {
            *reap = ChildReapState::Reaped;
            self.inner.reap_changed.notify_all();
        }
    }

    pub(crate) fn record_cleanup_error(&self, error: String) {
        let mut reap = mutex_lock(&self.inner.reap);
        match &mut *reap {
            ChildReapState::Pending { cleanup_error, .. }
            | ChildReapState::WaitAuthorityLost { cleanup_error, .. }
            | ChildReapState::OwnerStopped { cleanup_error, .. } => {
                cleanup_error.get_or_insert(error);
                self.inner.reap_changed.notify_all();
            }
            ChildReapState::Reaped => {}
        }
    }

    pub(crate) fn record_wait_error(&self, error: String) {
        let mut reap = mutex_lock(&self.inner.reap);
        if let ChildReapState::Pending { wait_error, .. } = &mut *reap {
            wait_error.get_or_insert(error);
            self.inner.reap_changed.notify_all();
        }
    }

    pub(crate) fn wait_until_reaped(&self, deadline: Instant) -> Result<(), String> {
        let mut reap = mutex_lock(&self.inner.reap);
        loop {
            match &*reap {
                ChildReapState::Reaped => return Ok(()),
                ChildReapState::WaitAuthorityLost { .. } | ChildReapState::OwnerStopped { .. } => {
                    drop(reap);
                    return self.reap_result();
                }
                ChildReapState::Pending { .. } if Instant::now() >= deadline => {
                    drop(reap);
                    return self.reap_result();
                }
                ChildReapState::Pending { .. } => {}
            }
            let now = Instant::now();
            let (next, _) = self
                .inner
                .reap_changed
                .wait_timeout(reap, deadline.saturating_duration_since(now))
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            reap = next;
        }
    }

    pub(crate) fn reap_result(&self) -> Result<(), String> {
        match &*mutex_lock(&self.inner.reap) {
            ChildReapState::Reaped => Ok(()),
            ChildReapState::Pending {
                cleanup_error,
                wait_error,
            } => {
                let mut errors = [cleanup_error.as_deref(), wait_error.as_deref()]
                    .into_iter()
                    .flatten();
                let Some(first) = errors.next() else {
                    return Err(format!(
                        "Run {} child waiter has not yet proven reap",
                        self.inner.run_id
                    ));
                };
                Err(errors.fold(first.to_owned(), |mut combined, error| {
                    combined.push_str("; ");
                    combined.push_str(error);
                    combined
                }))
            }
            ChildReapState::OwnerStopped { .. } => Err(format!(
                "Run {} retains its unreaped child after Native owner stopped",
                self.inner.run_id
            )),
            ChildReapState::WaitAuthorityLost {
                cleanup_error,
                wait_error,
                _child: _,
            } => {
                let mut errors = [cleanup_error.as_deref(), Some(wait_error.as_str())]
                    .into_iter()
                    .flatten();
                let Some(first) = errors.next() else {
                    return Err(format!(
                        "Run {} child waiter has not yet proven reap",
                        self.inner.run_id
                    ));
                };
                Err(errors.fold(first.to_owned(), |mut combined, error| {
                    combined.push_str("; ");
                    combined.push_str(error);
                    combined
                }))
            }
        }
    }

    /// Whether the original sole Native runtime entry has actually retired.
    pub(crate) fn entry_retired(&self) -> bool {
        self.inner.entry_retired.load(Ordering::Acquire)
    }

    pub(crate) fn mark_entry_retired(&self) {
        self.inner.entry_retired.store(true, Ordering::Release);
    }

    /// Prove that a closed native owner retains no child, control, or input
    /// worker. This is only the Backend-local part of collection eligibility;
    /// the Registry must separately fence Run lookup pins and terminal state.
    pub(crate) fn closed_quiescence_result(&self) -> Result<(), String> {
        self.reap_result()?;
        let state = self.inner.try_state().ok_or_else(|| {
            format!(
                "Run {} native control cleanup owner is busy",
                self.inner.run_id
            )
        })?;
        if state.phase != ControlPhase::Closed
            || state.child_open
            || state.stop_pending
            || !state.child_commands.is_empty()
            || state.input_scheduled
            || state.input_commands != 0
            || state.input_bytes != 0
            || !state.input_queue.is_empty()
        {
            return Err(format!(
                "Run {} native control cleanup is not quiescent",
                self.inner.run_id
            ));
        }
        drop(state);
        let owners = Arc::strong_count(&self.inner);
        if owners != 1 {
            return Err(format!(
                "Run {} native control cleanup retains {owners} owners",
                self.inner.run_id
            ));
        }
        Ok(())
    }

    /// Detach descriptors whose public semantics ended with the closed native
    /// incarnation. The caller must already own the Registry or unpublished
    /// Run fence; this method revalidates Backend-local quiescence before the
    /// irreversible take. A later reservation abort restores Run history, not
    /// these already-closed descriptors.
    pub(crate) fn detach_closed_descriptors_after_owner_fence(
        &self,
    ) -> Result<DetachedNativeDescriptors, String> {
        self.closed_quiescence_result()?;
        let pty = mutex_lock(&self.inner.pty).take();
        let writer = mutex_lock(&self.inner.writer).take();
        Ok(DetachedNativeDescriptors { pty, writer })
    }

    /// Keep the T-026 cleanup contract named at its original owner boundary.
    pub(crate) fn unpublished_cleanup_result(&self) -> Result<(), String> {
        self.closed_quiescence_result()
    }
}

impl PendingInput {
    pub(crate) async fn resolve(self) -> ControlResult {
        self.reply.await.unwrap_or_else(|_| {
            Err(unknown(ProtocolError::new(
                ErrorCode::Internal,
                format!(
                    "Run {} PTY input owner ended without a receipt",
                    self.run_id
                ),
            )))
        })
    }
}

impl PendingRecoverableInput {
    pub(crate) async fn resolve(mut self) -> RecoverableInputResult {
        match &mut self {
            Self::Ready(result) => result.clone(),
            Self::Pending { run_id, result } => loop {
                if let Some(result) = result.borrow().clone() {
                    return result;
                }
                if result.changed().await.is_err() {
                    return Err(unknown(ProtocolError::new(
                        ErrorCode::Internal,
                        format!("Run {run_id} recoverable Input owner ended without a result"),
                    )));
                }
            },
        }
    }
}

impl PendingStop {
    pub(crate) async fn resolve(self, timeout: Duration) -> ControlResult {
        match tokio::time::timeout(timeout, self.reply).await {
            Ok(Ok(StopOwnerResult::Accepted(disposition))) => {
                Ok(ControlReceipt::Stop { disposition })
            }
            Ok(Ok(StopOwnerResult::Rejected(failure))) => Err(failure),
            Ok(Ok(StopOwnerResult::Unknown(error))) => {
                Err(unknown(ProtocolError::new(ErrorCode::Io, error)))
            }
            Ok(Err(_)) => Err(unknown(ProtocolError::new(
                ErrorCode::InvalidRunState,
                format!(
                    "Run {} child owner ended before acknowledging stop",
                    self.run_id
                ),
            ))),
            Err(_) => Err(unknown(ProtocolError::new(
                ErrorCode::Internal,
                format!("timed out while stopping Run {}", self.run_id),
            ))),
        }
    }
}

impl PendingSignal {
    pub(crate) async fn resolve(self) -> ControlResult {
        match self.reply.await {
            Ok(Ok(())) => Ok(ControlReceipt::Signal {
                signal: self.signal,
            }),
            Ok(Err(error)) => Err(unknown(ProtocolError::new(ErrorCode::Io, error))),
            Err(_) => Err(unknown(ProtocolError::new(
                ErrorCode::InvalidRunState,
                format!(
                    "Run {} native owner ended before acknowledging signal",
                    self.run_id
                ),
            ))),
        }
    }
}

impl NativeControlOwner {
    /// Last size the owning PTY confirmed, or `None` when none is confirmed.
    pub(crate) fn confirmed_size(&self) -> Option<TerminalSize> {
        self.inner.lock_state().confirmed_size
    }

    /// Apply one resize, then confirm and publish the size the PTY reports.
    ///
    /// `publish` is invoked with the read-back size while this owner's state
    /// lock is held, and only for a resize that actually applied. Holding the
    /// lock across it is the whole ordering guarantee: two concurrent resizes
    /// serialize here, so the stored `confirmed_size` and the order observers
    /// see the events in cannot disagree. Publishing after releasing the lock
    /// would let 80x24 and 200x87 be confirmed in one order and published in
    /// the other, and an observer would watch the size go backwards.
    ///
    /// The callback must not resize or reacquire this control's state. Its
    /// production caller orders derived terminal geometry and publication under
    /// pre-acquired Run output lock. Public admission tries control only while
    /// holding that output guard, releases it on busy, then awaits actual unlock;
    /// it never waits for one owner while holding the other. Metadata is a short
    /// factual projection and does not take this control or the VT lock.
    #[cfg(test)]
    pub(crate) fn resize(
        &self,
        size: TerminalSize,
        publish: impl FnOnce(TerminalSize),
    ) -> ControlResult {
        self.try_resize(size, publish).unwrap_or_else(|| {
            Err(not_applied(ProtocolError::new(
                ErrorCode::ControlBackpressure,
                "Native control owner is busy before resize admission",
            )))
        })
    }

    /// None means no ioctl, readback, geometry or publication was attempted.
    pub(crate) fn try_resize(
        &self,
        size: TerminalSize,
        publish: impl FnOnce(TerminalSize),
    ) -> Option<ControlResult> {
        if self.inner.owner_failure.get().is_some() {
            return Some(Err(not_applied(ProtocolError::new(
                ErrorCode::BackendUnavailable,
                "Native owner stopped before resize admission",
            ))));
        }
        let state = self.inner.try_state()?;
        Some(self.resize_locked(size, publish, state))
    }

    fn resize_locked(
        &self,
        size: TerminalSize,
        publish: impl FnOnce(TerminalSize),
        state: ControlStateGuard<'_>,
    ) -> ControlResult {
        if self.inner.owner_failure.get().is_some() {
            return Err(not_applied(ProtocolError::new(
                ErrorCode::BackendUnavailable,
                "daemon-wide Native owner stopped before control admission",
            )));
        }
        // The phase lock makes stop/exit a fence for new resize operations.
        // portable-pty resize/get_size are short ioctl calls; no lock crosses
        // an await or the broader Run metadata path.
        let mut state = state;
        if self.inner.owner_failure.get().is_some() {
            return Err(not_applied(ProtocolError::new(
                ErrorCode::BackendUnavailable,
                "daemon-wide Native owner stopped before resize admission",
            )));
        }
        if state.phase != ControlPhase::Open {
            return Err(not_applied(invalid_phase_error(
                self.inner.run_id,
                state.phase,
                "resize",
            )));
        }
        let pty = mutex_lock(&self.inner.pty);
        let pty = pty.as_ref().ok_or_else(|| {
            unknown(ProtocolError::new(
                ErrorCode::Internal,
                format!("Run {} PTY control descriptor is closed", self.inner.run_id),
            ))
        })?;
        pty.resize(to_pty_size(size)).map_err(|error| {
            unknown(ProtocolError::new(
                ErrorCode::Io,
                format!("failed to resize Run {} PTY: {error}", self.inner.run_id),
            ))
        })?;
        let applied = pty.get_size().map_err(|error| {
            unknown(ProtocolError::new(
                ErrorCode::Io,
                format!(
                    "failed to read back Run {} PTY size after resize: {error}",
                    self.inner.run_id
                ),
            ))
        })?;
        // A read-back the owner cannot vouch for publishes nothing and leaves
        // the previously confirmed size standing. The mutation did cross the
        // boundary, so the caller is told `unknown` -- but no observer is told
        // a size that no terminal acknowledged.
        let Some(applied) = confirmed_size(Some(applied)) else {
            return Err(unknown(ProtocolError::new(
                ErrorCode::Io,
                format!(
                    "Run {} PTY returned an invalid zero applied size",
                    self.inner.run_id
                ),
            )));
        };
        state.confirmed_size = Some(applied);
        self.inner.publish_input(&state);
        publish(applied);
        drop(state);
        Ok(ControlReceipt::Resize {
            applied_size: applied,
        })
    }
}

fn input_status(state: &NativeControlState) -> NativeInputStatus {
    NativeInputStatus {
        phase: if let Some(reason) = &state.owner_failure {
            NativeInputPhase::Unavailable { reason: *reason }
        } else if state.phase != ControlPhase::Open {
            NativeInputPhase::Closed {}
        } else if state.input_failure.is_some() {
            NativeInputPhase::Unavailable {
                reason: NativeServiceFailure::WriteFailed,
            }
        } else {
            NativeInputPhase::Open {}
        },
        unsettled_commands: state.input_commands,
        unsettled_request_bytes: state.input_bytes,
        write_blocked: state.input_blocked,
        completed_input_bytes: Some(state.applied_input_bytes),
        current_size: state.confirmed_size,
        active_confirmed_bytes: state
            .input_queue
            .front()
            .map_or(0, |command| command.confirmed),
    }
}

impl NativeControlInner {
    fn settle_owner_loss(
        &self,
        state: &mut NativeControlState,
    ) -> (Vec<(InputReply, ControlFailure)>, VecDeque<ChildCommand>) {
        let Some(reason) = self.owner_failure.get().copied() else {
            return (Vec::new(), VecDeque::new());
        };
        if state.owner_failure.is_some() {
            return (Vec::new(), VecDeque::new());
        }
        state.owner_failure = Some(reason);
        state.stop_pending = false;
        let rejected = reject_queued_inputs(
            state,
            &ProtocolError::new(
                ErrorCode::BackendUnavailable,
                "daemon-wide Native input owner stopped",
            ),
        );
        self.publish_input(state);
        (rejected, std::mem::take(&mut state.child_commands))
    }

    async fn wait_admission_state(
        &self,
        recoverable: bool,
    ) -> Result<ControlStateGuard<'_>, ControlFailure> {
        if !recoverable && self.owner_failure.get().is_some() {
            return Err(not_applied(ProtocolError::new(
                ErrorCode::BackendUnavailable,
                "Native owner stopped before command admission",
            )));
        }
        loop {
            let changed = self.admission_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(state) = self.try_state() {
                return Ok(state);
            }
            if self.owner_failure.get().is_some() {
                let error = ProtocolError::new(
                    ErrorCode::BackendUnavailable,
                    "Native owner stopped while control admission was waiting",
                );
                return Err(if recoverable {
                    unknown(error)
                } else {
                    not_applied(error)
                });
            }
            changed.await;
        }
    }

    #[cfg(test)]
    fn try_admission_state(&self) -> Result<ControlStateGuard<'_>, ControlFailure> {
        if self.owner_failure.get().is_some() {
            return Err(not_applied(ProtocolError::new(
                ErrorCode::BackendUnavailable,
                "Native owner stopped before command admission",
            )));
        }
        self.try_state().ok_or_else(|| {
            not_applied(ProtocolError::new(
                ErrorCode::ControlBackpressure,
                "Native control owner is busy before command admission",
            ))
        })
    }

    fn lock_state(&self) -> ControlStateGuard<'_> {
        ControlStateGuard {
            state: Some(mutex_lock(&self.state)),
            inner: self,
        }
    }
    fn try_state(&self) -> Option<ControlStateGuard<'_>> {
        match self.state.try_lock() {
            Ok(state) => Some(ControlStateGuard {
                state: Some(state),
                inner: self,
            }),
            Err(TryLockError::Poisoned(error)) => Some(ControlStateGuard {
                state: Some(error.into_inner()),
                inner: self,
            }),
            Err(TryLockError::WouldBlock) => {
                self.owner_deferred.store(true, Ordering::Release);
                // The first holder may have unlocked before the flag store.
                // A second try either obtains the lock or leaves a live holder
                // responsible for the wake after its actual release.
                match self.state.try_lock() {
                    Ok(state) => Some(ControlStateGuard {
                        state: Some(state),
                        inner: self,
                    }),
                    Err(TryLockError::Poisoned(error)) => Some(ControlStateGuard {
                        state: Some(error.into_inner()),
                        inner: self,
                    }),
                    Err(TryLockError::WouldBlock) => None,
                }
            }
        }
    }

    fn publish_input(&self, state: &NativeControlState) {
        if let Some(service) = self.service.get() {
            service.update_input(input_status(state));
        }
    }

    #[cfg(test)]
    fn has_scheduled_input(&self) -> bool {
        let state = self.lock_state();
        state.input_scheduled && !state.input_queue.is_empty()
    }

    #[cfg(test)]
    fn drain_burst(&self, max_commands: usize, max_bytes: usize) -> bool {
        let mut commands = 0;
        let mut bytes = 0;
        loop {
            if commands > 0 && (commands >= max_commands || bytes >= max_bytes) {
                return self.has_more_or_unschedule();
            }
            let command = {
                let mut state = self.lock_state();
                if let Some(command) = state.input_queue.pop_front() {
                    command
                } else {
                    state.input_scheduled = false;
                    return false;
                }
            };
            commands += 1;
            bytes += command.data.len();
            if self.execute_input(command) {
                return false;
            }
        }
    }

    /// Returns true when the lane failed and this worker must stop.
    #[cfg(test)]
    fn execute_input(&self, command: InputCommand) -> bool {
        let written_bytes = command.data.len();
        let expected_cursor = match &command.reply {
            InputReply::Legacy(_) => None,
            InputReply::Recoverable { key, .. } => {
                let mut state = self.lock_state();
                let expected = state
                    .input_operations
                    .get(key)
                    .and_then(|entry| match entry {
                        InputOperationEntry::Pending { request, .. } => Some(request.expected_byte),
                        InputOperationEntry::Completed { .. }
                        | InputOperationEntry::Unknown { .. } => None,
                    })
                    .expect("queued recoverable Input retains one pending entry");
                if expected != state.applied_input_bytes {
                    release_input_capacity(&mut state, written_bytes);
                    let failure = not_applied(ProtocolError::new(
                        ErrorCode::InputCursorMismatch,
                        format!(
                            "Run {} applied-input cursor is {}, not expected {expected}",
                            self.run_id, state.applied_input_bytes
                        ),
                    ));
                    remove_input_operation(&mut state, key);
                    drop(state);
                    resolve_input_reply(command.reply, Err(failure));
                    return false;
                }
                Some(expected)
            }
        };
        let Some(end_byte) = self
            .lock_state()
            .applied_input_bytes
            .checked_add(u64::try_from(written_bytes).expect("bounded frame length fits u64"))
        else {
            let mut state = self.lock_state();
            release_input_capacity(&mut state, written_bytes);
            let failure = not_applied(ProtocolError::new(
                ErrorCode::InputCursorMismatch,
                format!("Run {} applied-input cursor is exhausted", self.run_id),
            ));
            if let InputReply::Recoverable { key, .. } = &command.reply {
                remove_input_operation(&mut state, key);
            }
            drop(state);
            resolve_input_reply(command.reply, Err(failure));
            return false;
        };
        let result = catch_unwind(AssertUnwindSafe(|| {
            let mut writer = mutex_lock(&self.writer);
            let writer = writer.as_mut().ok_or_else(|| {
                io::Error::new(io::ErrorKind::BrokenPipe, "PTY input writer is closed")
            })?;
            match writer {
                NativeInputWriter::Opaque(writer) => writer
                    .write_all(&command.data)
                    .and_then(|()| writer.flush()),
                NativeInputWriter::File(_) => {
                    unreachable!("real FD input belongs to the Native poll owner")
                }
            }
        }));

        let (receipt, rejected, failed) = self.finish_input(
            &command.reply,
            written_bytes,
            expected_cursor,
            end_byte,
            result,
        );
        resolve_input_reply(command.reply, receipt);
        send_rejections(rejected);
        failed
    }

    #[cfg(test)]
    fn finish_input(
        &self,
        reply: &InputReply,
        written_bytes: usize,
        expected_cursor: Option<u64>,
        end_byte: u64,
        result: std::thread::Result<io::Result<()>>,
    ) -> (
        RecoverableInputResult,
        Vec<(InputReply, ControlFailure)>,
        bool,
    ) {
        let mut state = self.lock_state();
        release_input_capacity(&mut state, written_bytes);
        match result {
            Ok(Ok(())) => {
                let start_byte = state.applied_input_bytes;
                debug_assert_eq!(expected_cursor.unwrap_or(start_byte), start_byte);
                state.applied_input_bytes = end_byte;
                let range = AppliedInputRange {
                    start_byte,
                    end_byte,
                };
                if let InputReply::Recoverable { key, .. } = reply {
                    let entry = state
                        .input_operations
                        .get_mut(key)
                        .expect("recoverable Input entry remains pending through write");
                    let request = entry.request().clone();
                    *entry = InputOperationEntry::Completed { request, range };
                    state.completed_input_operations.push_back(key.clone());
                }
                (Ok(range), Vec::new(), false)
            }
            Ok(Err(error)) => finish_failed_input(
                &mut state,
                self.run_id,
                reply,
                ErrorCode::Io,
                &format!("PTY input I/O failure: {error}"),
            ),
            Err(_) => finish_failed_input(
                &mut state,
                self.run_id,
                reply,
                ErrorCode::Internal,
                "PTY input writer panicked",
            ),
        }
    }

    #[cfg(test)]
    fn has_more_or_unschedule(&self) -> bool {
        let mut state = self.lock_state();
        if state.input_queue.is_empty() {
            state.input_scheduled = false;
            false
        } else {
            true
        }
    }

    #[cfg(test)]
    fn fail_scheduled(&self, message: String) {
        let rejected = {
            let mut state = self.lock_state();
            state.input_scheduled = false;
            reject_queued_inputs(
                &mut state,
                &ProtocolError::new(ErrorCode::Internal, message),
            )
        };
        send_rejections(rejected);
    }
}

fn input_failure(
    state: &mut NativeControlState,
    run_id: RunId,
    code: ErrorCode,
    detail: &str,
) -> ProtocolError {
    let current = ProtocolError::new(
        code,
        format!("failed to write Run {run_id} PTY input: {detail}"),
    );
    state.input_failure = Some(ProtocolError::new(
        code,
        format!("Run {run_id} PTY input lane is unavailable after {detail}"),
    ));
    current
}

#[cfg(test)]
fn finish_failed_input(
    state: &mut NativeControlState,
    run_id: RunId,
    reply: &InputReply,
    code: ErrorCode,
    detail: &str,
) -> (
    RecoverableInputResult,
    Vec<(InputReply, ControlFailure)>,
    bool,
) {
    let protocol_error = input_failure(state, run_id, code, detail);
    let queued_error = state
        .input_failure
        .clone()
        .expect("input failure was just recorded");
    let rejected = reject_queued_inputs(state, &queued_error);
    let failure = unknown(protocol_error);
    retain_unknown_input_operation(state, reply, &failure);
    (Err(failure), rejected, true)
}

fn reject_queued_inputs(
    state: &mut NativeControlState,
    error: &ProtocolError,
) -> Vec<(InputReply, ControlFailure)> {
    state.input_scheduled = false;
    state.input_blocked = false;
    let mut rejected = Vec::with_capacity(state.input_queue.len());
    while let Some(command) = state.input_queue.pop_front() {
        release_input_capacity(state, command.data.len());
        let failure = if command.confirmed == 0 {
            if let InputReply::Recoverable { key, .. } = &command.reply {
                remove_input_operation(state, key);
            }
            not_applied(error.clone())
        } else {
            // A confirmed prefix is not a whole-command receipt, and is not
            // permission to replay a suffix after Stop or owner loss.
            state.input_failure = Some(error.clone());
            let mut failure = unknown(error.clone());
            failure.confirmed_input_bytes = Some(command.confirmed);
            retain_unknown_input_operation(state, &command.reply, &failure);
            failure
        };
        rejected.push((command.reply, failure));
    }
    rejected
}

fn release_input_capacity(state: &mut NativeControlState, bytes: usize) {
    state.input_commands = state
        .input_commands
        .checked_sub(1)
        .expect("input command accounting remains balanced");
    state.input_bytes = state
        .input_bytes
        .checked_sub(bytes)
        .expect("input byte accounting remains balanced");
}

fn send_rejections(rejected: Vec<(InputReply, ControlFailure)>) {
    for (reply, failure) in rejected {
        resolve_input_reply(reply, Err(failure));
    }
}

fn reject_child_commands(commands: VecDeque<ChildCommand>, reason: &str, code: ErrorCode) {
    for command in commands {
        match command {
            #[cfg(not(target_os = "macos"))]
            ChildCommand::Signal { reply, .. } => {
                let _ = reply.send(Err(reason.to_owned()));
            }
            ChildCommand::Stop { reply, deadline: _ } => {
                let _ = reply.send(StopOwnerResult::Rejected(not_applied(ProtocolError::new(
                    code, reason,
                ))));
            }
            ChildCommand::CleanupUnpublished => {}
        }
    }
}

fn resolve_input_reply(reply: InputReply, result: RecoverableInputResult) {
    match reply {
        InputReply::Legacy(reply) => {
            let receipt = result.map(|range| ControlReceipt::Input {
                written_bytes: u32::try_from(range.end_byte - range.start_byte)
                    .expect("bounded input frame length fits u32"),
            });
            let _ = reply.send(receipt);
        }
        InputReply::Recoverable { completion, .. } => {
            completion.send_replace(Some(result));
        }
    }
}

fn retain_unknown_input_operation(
    state: &mut NativeControlState,
    reply: &InputReply,
    failure: &ControlFailure,
) {
    let InputReply::Recoverable { key, .. } = reply else {
        return;
    };
    let entry = state
        .input_operations
        .get_mut(key)
        .expect("recoverable Input failure retains its pending operation");
    let request = entry.request().clone();
    *entry = InputOperationEntry::Unknown {
        request,
        failure: failure.clone(),
    };
}

fn retained_input_result(
    state: &NativeControlState,
    key: &InputOperationKey,
    request: &InputOperationRequest,
    run_id: RunId,
) -> Result<Option<PendingRecoverableInput>, ControlFailure> {
    let Some(existing) = state.input_operations.get(key) else {
        return Ok(None);
    };
    if existing.request() != request {
        return Err(not_applied(ProtocolError::new(
            ErrorCode::InputOperationConflict,
            format!("native Input operation key is retained for another request on Run {run_id}"),
        )));
    }
    let result = match existing {
        InputOperationEntry::Pending { completion, .. } => PendingRecoverableInput::Pending {
            run_id,
            result: completion.subscribe(),
        },
        InputOperationEntry::Completed { range, .. } => PendingRecoverableInput::Ready(Ok(*range)),
        InputOperationEntry::Unknown { failure, .. } => {
            PendingRecoverableInput::Ready(Err(failure.clone()))
        }
    };
    Ok(Some(result))
}

fn remove_input_operation(state: &mut NativeControlState, key: &InputOperationKey) {
    if let Some(entry) = state.input_operations.remove(key) {
        state.retained_input_request_bytes = state
            .retained_input_request_bytes
            .checked_sub(entry.request().data.len())
            .expect("retained recoverable Input bytes remain balanced");
    }
}

fn evict_completed_input_results(state: &mut NativeControlState, new_bytes: usize) {
    while state.input_operations.len() >= state.input_result_max_entries
        || new_bytes
            > state
                .input_result_max_request_bytes
                .saturating_sub(state.retained_input_request_bytes)
    {
        let Some(key) = state.completed_input_operations.pop_front() else {
            return;
        };
        if matches!(
            state.input_operations.get(&key),
            Some(InputOperationEntry::Completed { .. })
        ) {
            remove_input_operation(state, &key);
        }
    }
}

fn invalid_phase_error(run_id: RunId, phase: ControlPhase, operation: &str) -> ProtocolError {
    match phase {
        ControlPhase::Open => unreachable!("open phase is valid"),
        ControlPhase::Stopping => ProtocolError::new(
            ErrorCode::InvalidRunState,
            format!("cannot {operation} stopping Run {run_id}"),
        ),
        ControlPhase::Closed => ProtocolError::new(
            ErrorCode::InvalidRunState,
            format!("cannot {operation} exited Run {run_id}"),
        ),
        ControlPhase::Failed => ProtocolError::new(
            ErrorCode::BackendUnavailable,
            format!("cannot {operation} Run {run_id} after child wait authority was lost"),
        ),
    }
}

fn not_applied(error: ProtocolError) -> ControlFailure {
    ControlFailure {
        error,
        disposition: CommandDisposition::NotApplied,
        confirmed_input_bytes: None,
    }
}

fn unknown(error: ProtocolError) -> ControlFailure {
    ControlFailure {
        error,
        disposition: CommandDisposition::Unknown,
        confirmed_input_bytes: None,
    }
}

pub(crate) const fn to_pty_size(size: TerminalSize) -> PtySize {
    PtySize {
        rows: size.rows,
        cols: size.cols,
        pixel_width: 0,
        pixel_height: 0,
    }
}

/// One PTY read-back accepted as a confirmed size, or `None`.
///
/// A zero row or column count is what the master reports when it has no
/// window size to give. It is rejected rather than stored: `current_size` is
/// meant to name a size some terminal acknowledged, and 0x0 names nothing.
const fn confirmed_size(size: Option<PtySize>) -> Option<TerminalSize> {
    match size {
        Some(PtySize { rows, cols, .. }) if rows != 0 && cols != 0 => {
            Some(TerminalSize { rows, cols })
        }
        _ => None,
    }
}

fn mutex_lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        io,
        sync::{
            Arc, Condvar, Mutex, Weak,
            atomic::{AtomicUsize, Ordering},
            mpsc,
        },
        time::{Duration, Instant},
    };

    #[cfg(target_os = "macos")]
    use ctxmux_protocol::RunSignal;
    use ctxmux_protocol::{
        AppliedInputRange, CommandDisposition, ControlReceipt, ErrorCode, InputOperationKey, RunId,
        StopDisposition, TerminalSize,
    };
    use portable_pty::PtySize;

    use super::{
        ChildCommand, HandoffInputOperation, HandoffInputState, InputDrainGate, NativeControlOwner,
        PortablePtyControl, PtyControl, StopOwnerResult, mutex_lock,
    };
    use crate::native_runtime::NativeRunOwner;

    #[test]
    fn handoff_input_diagnostics_follow_the_configured_aggregate_budget() {
        // Preserve the former 4 KiB characterization as a boundary example,
        // not the configurable aggregate handoff resource owner's capacity.
        let message = "e".repeat(super::INPUT_RESULT_DIAGNOSTIC_RESERVE_BYTES + 1);
        let error = ctxmux_protocol::ProtocolError::new(ErrorCode::Io, message.clone());
        let original = HandoffInputState {
            applied_input_bytes: 0,
            input_failure: Some(error.clone()),
            operations: vec![HandoffInputOperation::Unknown {
                key: InputOperationKey::new("handoff-diagnostics-original").unwrap(),
                expected_byte: 0,
                data: vec![1, 2, 3],
                failure: ctxmux_protocol::ControlFailure {
                    error,
                    disposition: CommandDisposition::Unknown,
                    confirmed_input_bytes: Some(1),
                },
            }],
        };
        let resources = crate::ResourceLimits::DEFAULT;
        original
            .validate_with_resources(resources)
            .expect("configured aggregate budget admits the complete original diagnostics");
        let encoded = serde_json::to_vec(&original).unwrap();
        let restored: HandoffInputState = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(
            restored, original,
            "recovery preserves full error facts, original bytes and exact unknown prefix"
        );
        let aggregate_bytes = 2 * message.len();
        assert_eq!(original.retained_diagnostic_bytes(), aggregate_bytes);
        let gate = InputDrainGate::with_stats_and_resources(
            crate::qualification_stats::QualificationStats::default(),
            resources,
        );
        let budget = gate.control_budget();
        let runtime = NativeRunOwner::default();
        let owner = NativeControlOwner::closed_with_input_state(
            RunId::new(),
            restored,
            gate,
            runtime.owner_wake(),
        );
        assert_eq!(
            budget.used(),
            original.control_memory_bytes(),
            "rehydration funds the full restored diagnostics beyond the keyed reserve"
        );
        assert_eq!(owner.handoff_input_state().unwrap(), original);
        drop(owner);
        assert_eq!(
            budget.used(),
            0,
            "the real diagnostic lease follows its actual control owner"
        );
        let mut exact = resources;
        exact.handoff_diagnostic_bytes = aggregate_bytes;
        original.validate_with_resources(exact).unwrap();
        exact.handoff_diagnostic_bytes -= 1;
        assert!(
            original.validate_with_resources(exact).is_err(),
            "a real configured aggregate byte refusal does not truncate or weaken the ledger"
        );
    }

    async fn wait_for_handoff_input_state(owner: &NativeControlOwner) -> HandoffInputState {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            match owner.handoff_input_state() {
                Ok(state) => return state,
                Err(_) if Instant::now() < deadline => tokio::task::yield_now().await,
                Err(error) => panic!("native Input owner did not become handoff-ready: {error}"),
            }
        }
    }

    struct FakePty {
        size: Mutex<PtySize>,
        readback_row_delta: u16,
        /// Report a zero-column read-back, the way a driver can when the far
        /// side of the pty is gone: the resize "succeeded" but the size it
        /// hands back is not one any terminal is actually using.
        zero_readback: bool,
    }

    impl FakePty {
        fn new(readback_row_delta: u16) -> Self {
            Self {
                size: Mutex::new(PtySize::default()),
                readback_row_delta,
                zero_readback: false,
            }
        }

        fn with_zero_readback() -> Self {
            Self {
                zero_readback: true,
                ..Self::new(0)
            }
        }
    }

    impl PtyControl for FakePty {
        fn resize(&self, mut size: PtySize) -> io::Result<()> {
            size.rows = size.rows.saturating_add(self.readback_row_delta);
            *mutex_lock(&self.size) = size;
            Ok(())
        }

        fn get_size(&self) -> io::Result<PtySize> {
            if self.zero_readback {
                return Ok(PtySize {
                    cols: 0,
                    ..*mutex_lock(&self.size)
                });
            }
            Ok(*mutex_lock(&self.size))
        }

        fn master_raw_fd(&self) -> Option<std::os::fd::RawFd> {
            None
        }

        #[cfg(target_os = "macos")]
        fn interrupt_foreground(&self) -> io::Result<()> {
            Ok(())
        }

        #[cfg(not(target_os = "macos"))]
        fn foreground_process_group(&self) -> Option<u32> {
            None
        }
    }

    struct RecordingWriter(Arc<Mutex<Vec<u8>>>);

    impl io::Write for RecordingWriter {
        fn write(&mut self, data: &[u8]) -> io::Result<usize> {
            mutex_lock(&self.0).extend_from_slice(data);
            Ok(data.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    struct DropCountingPty(Arc<AtomicUsize>);

    impl Drop for DropCountingPty {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::AcqRel);
        }
    }

    impl PtyControl for DropCountingPty {
        fn resize(&self, _size: PtySize) -> io::Result<()> {
            Ok(())
        }

        fn get_size(&self) -> io::Result<PtySize> {
            Ok(PtySize::default())
        }

        fn master_raw_fd(&self) -> Option<std::os::fd::RawFd> {
            None
        }

        #[cfg(target_os = "macos")]
        fn interrupt_foreground(&self) -> io::Result<()> {
            Ok(())
        }

        #[cfg(not(target_os = "macos"))]
        fn foreground_process_group(&self) -> Option<u32> {
            None
        }
    }

    struct DropCountingWriter(Arc<AtomicUsize>);

    #[cfg(target_os = "macos")]
    struct InterruptCountingPty(Arc<AtomicUsize>);

    #[cfg(target_os = "macos")]
    impl PtyControl for InterruptCountingPty {
        fn resize(&self, _size: PtySize) -> io::Result<()> {
            Ok(())
        }

        fn get_size(&self) -> io::Result<PtySize> {
            Ok(PtySize::default())
        }

        fn master_raw_fd(&self) -> Option<std::os::fd::RawFd> {
            None
        }

        fn interrupt_foreground(&self) -> io::Result<()> {
            self.0.fetch_add(1, Ordering::AcqRel);
            Ok(())
        }
    }

    impl Drop for DropCountingWriter {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::AcqRel);
        }
    }

    impl io::Write for DropCountingWriter {
        fn write(&mut self, data: &[u8]) -> io::Result<usize> {
            Ok(data.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    struct BlockingWriter {
        started: Option<mpsc::SyncSender<()>>,
        release: Arc<(Mutex<bool>, Condvar)>,
        written: Arc<Mutex<usize>>,
    }

    impl io::Write for BlockingWriter {
        fn write(&mut self, data: &[u8]) -> io::Result<usize> {
            if let Some(started) = self.started.take() {
                let _ = started.send(());
                let (released, wake) = &*self.release;
                let mut released = mutex_lock(released);
                while !*released {
                    released = wake
                        .wait(released)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                }
            }
            *mutex_lock(&self.written) += data.len();
            Ok(data.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    struct FailingWriter;

    impl io::Write for FailingWriter {
        fn write(&mut self, _data: &[u8]) -> io::Result<usize> {
            Err(io::Error::new(io::ErrorKind::BrokenPipe, "fixture failure"))
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    struct PrefixThenFailWriter {
        wrote_prefix: bool,
        written: Arc<Mutex<Vec<u8>>>,
    }

    impl io::Write for PrefixThenFailWriter {
        fn write(&mut self, data: &[u8]) -> io::Result<usize> {
            if !self.wrote_prefix {
                self.wrote_prefix = true;
                mutex_lock(&self.written).push(data[0]);
                return Ok(1);
            }
            Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "fixture partial write",
            ))
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    struct PanickingWriter {
        started: Option<mpsc::SyncSender<()>>,
        release: Arc<(Mutex<bool>, Condvar)>,
    }

    impl io::Write for PanickingWriter {
        fn write(&mut self, _data: &[u8]) -> io::Result<usize> {
            if let Some(started) = self.started.take() {
                let _ = started.send(());
                let (released, wake) = &*self.release;
                let mut released = mutex_lock(released);
                while !*released {
                    released = wake
                        .wait(released)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                }
            }
            panic!("fixture writer panic");
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    struct OrderedWriter {
        label: u8,
        order: Arc<Mutex<Vec<u8>>>,
    }

    impl io::Write for OrderedWriter {
        fn write(&mut self, data: &[u8]) -> io::Result<usize> {
            mutex_lock(&self.order).push(self.label);
            Ok(data.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    struct OrderedBlockingWriter {
        label: u8,
        order: Arc<Mutex<Vec<u8>>>,
        started: Option<mpsc::SyncSender<()>>,
        release: Arc<(Mutex<bool>, Condvar)>,
    }

    impl io::Write for OrderedBlockingWriter {
        fn write(&mut self, data: &[u8]) -> io::Result<usize> {
            if let Some(started) = self.started.take() {
                let _ = started.send(());
                let (released, wake) = &*self.release;
                let mut released = mutex_lock(released);
                while !*released {
                    released = wake
                        .wait(released)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                }
            }
            mutex_lock(&self.order).push(self.label);
            Ok(data.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    struct TestChildCommands {
        owner: Weak<super::NativeControlInner>,
        _runtime: NativeRunOwner,
        pending: Mutex<VecDeque<ChildCommand>>,
    }

    impl TestChildCommands {
        fn try_recv(&self) -> Result<ChildCommand, mpsc::TryRecvError> {
            let mut pending = mutex_lock(&self.pending);
            let owner = self
                .owner
                .upgrade()
                .map(|inner| NativeControlOwner { inner })
                .ok_or(mpsc::TryRecvError::Disconnected)?;
            pending.extend(owner.drain_child_commands());
            pending.pop_front().ok_or(mpsc::TryRecvError::Empty)
        }

        fn recv_timeout(&self, timeout: Duration) -> Result<ChildCommand, mpsc::RecvTimeoutError> {
            let deadline = Instant::now() + timeout;
            loop {
                match self.try_recv() {
                    Ok(command) => return Ok(command),
                    Err(mpsc::TryRecvError::Disconnected) => {
                        return Err(mpsc::RecvTimeoutError::Disconnected);
                    }
                    Err(mpsc::TryRecvError::Empty) if Instant::now() < deadline => {
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    Err(mpsc::TryRecvError::Empty) => {
                        return Err(mpsc::RecvTimeoutError::Timeout);
                    }
                }
            }
        }
    }

    fn owner(
        writer: Box<dyn io::Write + Send>,
        pty: Box<dyn PtyControl>,
        gate: InputDrainGate,
    ) -> (NativeControlOwner, TestChildCommands) {
        let runtime = NativeRunOwner::default();
        let owner_wake = runtime.owner_wake();
        let owner = NativeControlOwner::new_with_pty(RunId::new(), pty, writer, gate, owner_wake);
        let commands = TestChildCommands {
            owner: Arc::downgrade(&owner.inner),
            _runtime: runtime,
            pending: Mutex::new(VecDeque::new()),
        };
        (owner, commands)
    }

    fn release_writer(release: &Arc<(Mutex<bool>, Condvar)>) {
        let (released, wake) = &**release;
        *mutex_lock(released) = true;
        wake.notify_all();
    }

    #[test]
    fn master_raw_fd_exposes_the_live_master_without_closing() {
        let pair = portable_pty::native_pty_system()
            .openpty(PtySize {
                rows: 24,
                cols: 80,
                pixel_width: 0,
                pixel_height: 0,
            })
            .unwrap();
        // Keep the slave end alive so the pty pair is not torn down mid-test.
        let _slave = pair.slave;

        let expected = pair.master.as_raw_fd();
        assert!(
            matches!(expected, Some(fd) if fd >= 0),
            "real pty master should expose a non-negative raw fd"
        );

        let pty: Box<dyn PtyControl> = Box::new(PortablePtyControl(pair.master));
        let (owner, _child) = owner(Box::new(io::sink()), pty, InputDrainGate::default());

        assert_eq!(owner.master_raw_fd(), expected);
        // Reading the fd must be idempotent and non-consuming: the control still
        // holds the live master and hands back the very same descriptor number.
        assert_eq!(owner.master_raw_fd(), expected);

        // The exposed fd is the *live* master, not a stale integer snapshot: a
        // resize round-trips through the same descriptor (ioctl on the real
        // master) and the fd number is unchanged afterward. A cached number
        // could not carry a resize; only an open master fd can.
        let applied = owner
            .resize(
                TerminalSize {
                    rows: 40,
                    cols: 132,
                },
                |_| {},
            )
            .expect("resize the live master exposed for handoff");
        assert_eq!(
            applied,
            ControlReceipt::Resize {
                applied_size: TerminalSize {
                    rows: 40,
                    cols: 132,
                },
            },
        );
        assert_eq!(
            owner.master_raw_fd(),
            expected,
            "the master fd number is stable across a resize on the live descriptor"
        );
    }

    #[test]
    fn input_service_size_tracks_confirmed_readback() {
        let (owner, _commands) = owner(
            Box::new(io::sink()),
            Box::new(FakePty::new(1)),
            InputDrainGate::default(),
        );
        let service = crate::native_service::NativeService::new(false);
        owner.bind_service(service.clone());
        let receipt = owner
            .resize(
                TerminalSize {
                    rows: 40,
                    cols: 132,
                },
                |_| {},
            )
            .unwrap();
        let ControlReceipt::Resize { applied_size } = receipt else {
            panic!("resize receipt")
        };
        assert_eq!(
            applied_size,
            TerminalSize {
                rows: 41,
                cols: 132
            }
        );
        assert_eq!(service.snapshot().input.current_size, Some(applied_size));
    }

    #[test]
    fn handoff_rejects_a_confirmed_prefix_beyond_original_request() {
        let error = ctxmux_protocol::ProtocolError::new(ErrorCode::Io, "original write failure");
        let mut state = HandoffInputState {
            applied_input_bytes: 0,
            input_failure: Some(error.clone()),
            operations: vec![HandoffInputOperation::Unknown {
                key: InputOperationKey::new("unknown-prefix-range").unwrap(),
                expected_byte: 0,
                data: vec![1, 2, 3],
                failure: ctxmux_protocol::ControlFailure {
                    error,
                    disposition: CommandDisposition::Unknown,
                    confirmed_input_bytes: Some(4),
                },
            }],
        };
        assert!(
            state
                .validate_with_resources(crate::ResourceLimits::DEFAULT)
                .is_err()
        );
        let HandoffInputOperation::Unknown { failure, .. } = &mut state.operations[0] else {
            unreachable!()
        };
        failure.confirmed_input_bytes = Some(3);
        state
            .validate_with_resources(crate::ResourceLimits::DEFAULT)
            .unwrap();
    }

    #[test]
    fn an_unusable_zero_read_back_publishes_nothing_and_keeps_the_last_confirmed_size() {
        // The master reports zero columns after a resize whose ioctl succeeded.
        // The mutation did cross the boundary, so the caller must be told
        // `unknown` -- but zero columns is not a geometry any terminal is
        // using, so it must never be stored or published as confirmed truth.
        let (zeroed, _child) = owner(
            Box::new(io::sink()),
            Box::new(FakePty::with_zero_readback()),
            InputDrainGate::default(),
        );
        let seeded = zeroed.confirmed_size();

        let mut published = Vec::new();
        let failure = zeroed
            .resize(TerminalSize { rows: 24, cols: 80 }, |size| {
                published.push(size);
            })
            .expect_err("a zero read-back cannot be confirmed");

        assert_eq!(failure.error.code, ErrorCode::Io);
        assert_eq!(
            failure.disposition,
            CommandDisposition::Unknown,
            "the ioctl already crossed the boundary, so the outcome is unknown"
        );
        assert!(
            published.is_empty(),
            "no observer may be told a size no terminal acknowledged"
        );
        assert_eq!(
            zeroed.confirmed_size(),
            seeded,
            "the previously confirmed size stands rather than being replaced"
        );
    }

    #[test]
    fn pending_reap_receipt_preserves_cleanup_and_wait_failures() {
        let (owner, _child) = owner(
            Box::new(RecordingWriter(Arc::new(Mutex::new(Vec::new())))),
            Box::new(FakePty::new(0)),
            InputDrainGate::default(),
        );
        owner.record_cleanup_error("fixture kill failure".to_owned());
        owner.record_wait_error("fixture wait failure".to_owned());
        owner.record_wait_error("later wait failure".to_owned());

        let error = owner.reap_result().expect_err("reap remains unproven");
        assert!(error.contains("fixture kill failure"));
        assert!(error.contains("fixture wait failure"));
        assert!(!error.contains("later wait failure"));
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn interrupt_uses_the_retained_pty_owner_without_a_numeric_child_command() {
        let interrupts = Arc::new(AtomicUsize::new(0));
        let (owner, child) = owner(
            Box::new(io::sink()),
            Box::new(InterruptCountingPty(Arc::clone(&interrupts))),
            InputDrainGate::default(),
        );

        let receipt = owner
            .begin_signal(RunSignal::Interrupt)
            .expect("admit PTY-owned interrupt")
            .resolve()
            .await
            .expect("PTY owner acknowledges interrupt");

        assert_eq!(
            receipt,
            ControlReceipt::Signal {
                signal: RunSignal::Interrupt
            }
        );
        assert_eq!(interrupts.load(Ordering::Acquire), 1);
        assert!(matches!(child.try_recv(), Err(mpsc::TryRecvError::Empty)));
    }

    #[test]
    fn closed_descriptors_require_full_quiescence_and_drop_once() {
        let pty_drops = Arc::new(AtomicUsize::new(0));
        let writer_drops = Arc::new(AtomicUsize::new(0));
        let (owner, child) = owner(
            Box::new(DropCountingWriter(Arc::clone(&writer_drops))),
            Box::new(DropCountingPty(Arc::clone(&pty_drops))),
            InputDrainGate::default(),
        );

        owner
            .detach_closed_descriptors_after_owner_fence()
            .expect_err("pending child cannot lose descriptors");
        owner.mark_reaped();
        owner
            .detach_closed_descriptors_after_owner_fence()
            .expect_err("open control cannot lose descriptors");

        let stop = owner.begin_stop().expect("enter stopping phase");
        owner
            .detach_closed_descriptors_after_owner_fence()
            .expect_err("stopping control cannot lose descriptors");
        let super::ChildCommand::Stop { reply, deadline: _ } = child
            .recv_timeout(Duration::from_secs(1))
            .expect("fixture receives stop")
        else {
            panic!("public stop sends the stop command variant");
        };
        owner
            .commit_pending_stop()
            .expect("production owner commits admitted Stop");
        reply
            .send(StopOwnerResult::Accepted(StopDisposition::Graceful))
            .expect("acknowledge fixture stop");
        drop(stop);

        owner.mark_closed();
        let extra_owner = owner.clone();
        owner
            .detach_closed_descriptors_after_owner_fence()
            .expect_err("an independent control owner blocks compaction");
        assert_eq!(pty_drops.load(Ordering::Acquire), 0);
        assert_eq!(writer_drops.load(Ordering::Acquire), 0);

        drop(extra_owner);
        let descriptors = owner
            .detach_closed_descriptors_after_owner_fence()
            .expect("closed quiescent descriptors detach");
        assert_eq!(pty_drops.load(Ordering::Acquire), 0);
        assert_eq!(writer_drops.load(Ordering::Acquire), 0);
        drop(descriptors);
        assert_eq!(pty_drops.load(Ordering::Acquire), 1);
        assert_eq!(writer_drops.load(Ordering::Acquire), 1);

        let already_compacted = owner
            .detach_closed_descriptors_after_owner_fence()
            .expect("descriptor compaction is idempotent");
        drop(already_compacted);
        assert_eq!(pty_drops.load(Ordering::Acquire), 1);
        assert_eq!(writer_drops.load(Ordering::Acquire), 1);
        assert_eq!(
            owner
                .begin_input(vec![1])
                .expect_err("compacted closed control rejects input")
                .error
                .code,
            ErrorCode::InvalidRunState
        );
        assert_eq!(
            owner
                .resize(TerminalSize { rows: 24, cols: 80 }, |_| {})
                .expect_err("compacted closed control rejects resize")
                .error
                .code,
            ErrorCode::InvalidRunState
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blocked_input_owner_prevents_closed_descriptor_compaction() {
        let pty_drops = Arc::new(AtomicUsize::new(0));
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let (owner, _child) = owner(
            Box::new(BlockingWriter {
                started: Some(started_tx),
                release: Arc::clone(&release),
                written: Arc::new(Mutex::new(0)),
            }),
            Box::new(DropCountingPty(Arc::clone(&pty_drops))),
            InputDrainGate::default(),
        );

        let pending = owner.begin_input(vec![1]).expect("admit blocking input");
        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("input worker blocks inside the writer");
        owner.mark_reaped();
        owner.mark_closed();
        owner
            .detach_closed_descriptors_after_owner_fence()
            .expect_err("active input owner blocks descriptor compaction");
        assert_eq!(pty_drops.load(Ordering::Acquire), 0);

        release_writer(&release);
        pending
            .resolve()
            .await
            .expect("already-started input retains its outcome");
        let deadline = Instant::now() + Duration::from_secs(2);
        let descriptors = loop {
            match owner.detach_closed_descriptors_after_owner_fence() {
                Ok(descriptors) => break descriptors,
                Err(_) if Instant::now() < deadline => tokio::task::yield_now().await,
                Err(error) => panic!("input owner did not quiesce: {error}"),
            }
        };
        assert_eq!(pty_drops.load(Ordering::Acquire), 0);
        drop(descriptors);
        assert_eq!(pty_drops.load(Ordering::Acquire), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn input_fifo_preserves_one_thousand_opaque_chunks_and_exact_receipts() {
        let written = Arc::new(Mutex::new(Vec::new()));
        let (owner, _child) = owner(
            Box::new(RecordingWriter(Arc::clone(&written))),
            Box::new(FakePty::new(0)),
            InputDrainGate::default(),
        );
        let mouse = b"\x1b[<0;40;12M";
        let paste_start = b"\x1b[200~";
        let paste_end = b"\x1b[201~";
        let mut expected = Vec::new();
        let mut pending = Vec::new();
        for index in 0..1_000_u16 {
            let data = match index % 4 {
                0 => mouse.to_vec(),
                1 => paste_start.to_vec(),
                2 => index.to_be_bytes().to_vec(),
                _ => paste_end.to_vec(),
            };
            expected.extend_from_slice(&data);
            pending.push((
                data.len(),
                owner.begin_input_async(data).await.expect("admit input"),
            ));
        }

        for (expected_bytes, pending) in pending {
            assert_eq!(
                pending.resolve().await.expect("input reaches writer"),
                ControlReceipt::Input {
                    written_bytes: u32::try_from(expected_bytes).unwrap(),
                }
            );
        }
        assert_eq!(*mutex_lock(&written), expected);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn recoverable_input_owner_deduplicates_ranges_and_fences_evicted_retries() {
        let written = Arc::new(Mutex::new(Vec::new()));
        let runtime = NativeRunOwner::default();
        let owner_wake = runtime.owner_wake();
        let owner = NativeControlOwner::new_with_pty_and_input_results(
            RunId::new(),
            Box::new(FakePty::new(0)),
            Box::new(RecordingWriter(Arc::clone(&written))),
            InputDrainGate::default(),
            owner_wake,
            1,
            16,
        );
        let first_key = InputOperationKey::new("first").expect("valid key");
        let first = owner
            .begin_recoverable_input(first_key.clone(), 0, b"A".to_vec())
            .expect("admit first operation")
            .resolve()
            .await
            .expect("apply first operation");
        assert_eq!(
            first,
            AppliedInputRange {
                start_byte: 0,
                end_byte: 1,
            }
        );

        assert_eq!(
            owner
                .begin_recoverable_input(first_key.clone(), 0, b"A".to_vec())
                .expect("recover retained operation")
                .resolve()
                .await
                .expect("return retained result"),
            first
        );
        let conflict = owner
            .begin_recoverable_input(first_key.clone(), 0, b"different".to_vec())
            .expect_err("retained key rejects another request");
        assert_eq!(conflict.error.code, ErrorCode::InputOperationConflict);
        assert_eq!(conflict.disposition, CommandDisposition::NotApplied);
        assert_eq!(*mutex_lock(&written), b"A");

        owner
            .begin_input(b"B".to_vec())
            .expect("legacy input shares the cursor")
            .resolve()
            .await
            .expect("legacy input applies");
        assert_eq!(owner.applied_input_bytes(), 2);

        assert_eq!(
            owner
                .begin_recoverable_input(
                    InputOperationKey::new("second").expect("valid key"),
                    2,
                    b"C".to_vec(),
                )
                .expect("new operation evicts completed first result")
                .resolve()
                .await
                .expect("apply second operation"),
            AppliedInputRange {
                start_byte: 2,
                end_byte: 3,
            }
        );
        let stale = owner
            .begin_recoverable_input(first_key, 0, b"A".to_vec())
            .expect("stale operation reaches FIFO cursor check")
            .resolve()
            .await
            .expect_err("evicted exact retry fails closed");
        assert_eq!(stale.error.code, ErrorCode::InputCursorMismatch);
        assert_eq!(stale.disposition, CommandDisposition::NotApplied);
        assert_eq!(*mutex_lock(&written), b"ABC");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn completed_recoverable_input_ledger_round_trips_without_a_second_write() {
        let original_written = Arc::new(Mutex::new(Vec::new()));
        let (original, _child) = owner(
            Box::new(RecordingWriter(Arc::clone(&original_written))),
            Box::new(FakePty::new(0)),
            InputDrainGate::default(),
        );
        let key = InputOperationKey::new("handoff-completed").unwrap();
        let range = original
            .begin_recoverable_input(key.clone(), 0, b"A".to_vec())
            .expect("admit original operation")
            .resolve()
            .await
            .expect("apply original operation");
        let snapshot = wait_for_handoff_input_state(&original).await;
        assert_eq!(snapshot.applied_input_bytes, 1);
        assert!(matches!(
            snapshot.operations.as_slice(),
            [HandoffInputOperation::Completed { range: retained, .. }] if *retained == range
        ));

        let adopted_written = Arc::new(Mutex::new(Vec::new()));
        let runtime = NativeRunOwner::default();
        let adopted = NativeControlOwner::new_with_pty_and_input_state(
            original.run_id(),
            Box::new(FakePty::new(0)),
            Box::new(RecordingWriter(Arc::clone(&adopted_written))),
            InputDrainGate::default(),
            runtime.owner_wake(),
            super::INPUT_RESULT_MAX_ENTRIES,
            super::INPUT_RESULT_MAX_REQUEST_BYTES,
            snapshot,
        );
        assert_eq!(
            adopted
                .begin_recoverable_input(key, 0, b"A".to_vec())
                .expect("adopted owner finds retained operation")
                .resolve()
                .await
                .expect("retained operation stays successful"),
            range
        );
        assert!(
            mutex_lock(&adopted_written).is_empty(),
            "retained retry must not cross the PTY write boundary again"
        );
        adopted
            .begin_recoverable_input(
                InputOperationKey::new("handoff-following").unwrap(),
                1,
                b"B".to_vec(),
            )
            .expect("cursor continues after handoff")
            .resolve()
            .await
            .expect("following operation applies");
        assert_eq!(*mutex_lock(&adopted_written), b"B");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unknown_recoverable_input_and_poisoned_lane_round_trip() {
        let original_written = Arc::new(Mutex::new(Vec::new()));
        let (original, _child) = owner(
            Box::new(PrefixThenFailWriter {
                wrote_prefix: false,
                written: Arc::clone(&original_written),
            }),
            Box::new(FakePty::new(0)),
            InputDrainGate::default(),
        );
        let key = InputOperationKey::new("handoff-unknown").unwrap();
        let failure = original
            .begin_recoverable_input(key.clone(), 0, b"AB".to_vec())
            .expect("admit ambiguous operation")
            .resolve()
            .await
            .expect_err("partial write is unknown");
        let snapshot = wait_for_handoff_input_state(&original).await;
        assert_eq!(snapshot.applied_input_bytes, 0);
        assert!(snapshot.input_failure.is_some());

        let adopted_written = Arc::new(Mutex::new(Vec::new()));
        let runtime = NativeRunOwner::default();
        let adopted = NativeControlOwner::new_with_pty_and_input_state(
            original.run_id(),
            Box::new(FakePty::new(0)),
            Box::new(RecordingWriter(Arc::clone(&adopted_written))),
            InputDrainGate::default(),
            runtime.owner_wake(),
            super::INPUT_RESULT_MAX_ENTRIES,
            super::INPUT_RESULT_MAX_REQUEST_BYTES,
            snapshot,
        );
        assert_eq!(
            adopted
                .begin_recoverable_input(key, 0, b"AB".to_vec())
                .expect("retained unknown remains addressable")
                .resolve()
                .await
                .expect_err("retained unknown stays unknown"),
            failure
        );
        let rejected = adopted
            .begin_input(b"C".to_vec())
            .expect_err("poisoned lane rejects new input after handoff");
        assert_eq!(rejected.disposition, CommandDisposition::NotApplied);
        assert!(mutex_lock(&adopted_written).is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pending_input_rejects_handoff_until_the_owner_settles() {
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let (owner, _child) = owner(
            Box::new(BlockingWriter {
                started: Some(started_tx),
                release: Arc::clone(&release),
                written: Arc::new(Mutex::new(0)),
            }),
            Box::new(FakePty::new(0)),
            InputDrainGate::default(),
        );
        let pending = owner
            .begin_recoverable_input(
                InputOperationKey::new("handoff-pending").unwrap(),
                0,
                b"A".to_vec(),
            )
            .expect("admit crossing operation");
        started_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("operation reaches blocking writer");
        assert!(
            owner.handoff_input_state().is_err(),
            "crossing operation must reject extraction"
        );
        release_writer(&release);
        pending.resolve().await.expect("crossing operation settles");
        wait_for_handoff_input_state(&owner).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn recoverable_input_pending_retry_joins_one_physical_write() {
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let written = Arc::new(Mutex::new(0));
        let (owner, _child) = owner(
            Box::new(BlockingWriter {
                started: Some(started_tx),
                release: Arc::clone(&release),
                written: Arc::clone(&written),
            }),
            Box::new(FakePty::new(0)),
            InputDrainGate::default(),
        );
        let key = InputOperationKey::new("pending-join").unwrap();
        let first = owner
            .begin_recoverable_input(key.clone(), 0, b"AB".to_vec())
            .expect("admit first caller");
        started_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("first caller reaches writer");
        let joined = owner
            .begin_recoverable_input(key, 0, b"AB".to_vec())
            .expect("matching pending caller joins");

        release_writer(&release);
        let expected = AppliedInputRange {
            start_byte: 0,
            end_byte: 2,
        };
        assert_eq!(first.resolve().await.unwrap(), expected);
        assert_eq!(joined.resolve().await.unwrap(), expected);
        assert_eq!(*mutex_lock(&written), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn input_bound_counts_the_active_write_and_rejects_without_mutation() {
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let written = Arc::new(Mutex::new(0));
        let (owner, _child) = owner(
            Box::new(BlockingWriter {
                started: Some(started_tx),
                release: Arc::clone(&release),
                written: Arc::clone(&written),
            }),
            Box::new(FakePty::new(0)),
            InputDrainGate::default(),
        );

        let mut pending = vec![
            owner
                .begin_input(vec![7; 4_096])
                .expect("admit active input"),
        ];
        started_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("active input reaches writer");
        for _ in 1..1_024 {
            pending.push(
                owner
                    .begin_input(vec![7; 4_096])
                    .expect("admit within exact input bound"),
            );
        }
        let error = owner
            .begin_input(Vec::new())
            .expect_err("1025th command exceeds the command bound");
        assert_eq!(error.error.code, ErrorCode::ControlBackpressure);
        assert_eq!(error.disposition, CommandDisposition::NotApplied);

        release_writer(&release);
        for receipt in pending {
            receipt.resolve().await.expect("drain bounded input");
        }
        assert_eq!(*mutex_lock(&written), 4 * 1024 * 1024);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn global_gate_keeps_a_second_run_out_of_a_busy_blocking_slot() {
        let gate = InputDrainGate::with_limits(1, 64, 256 * 1024);
        let (first_started_tx, first_started_rx) = mpsc::sync_channel(1);
        let first_release = Arc::new((Mutex::new(false), Condvar::new()));
        let (first, _first_child) = owner(
            Box::new(BlockingWriter {
                started: Some(first_started_tx),
                release: Arc::clone(&first_release),
                written: Arc::new(Mutex::new(0)),
            }),
            Box::new(FakePty::new(0)),
            gate.clone(),
        );
        let (second_started_tx, second_started_rx) = mpsc::sync_channel(1);
        let second_release = Arc::new((Mutex::new(false), Condvar::new()));
        let (second, _second_child) = owner(
            Box::new(BlockingWriter {
                started: Some(second_started_tx),
                release: Arc::clone(&second_release),
                written: Arc::new(Mutex::new(0)),
            }),
            Box::new(FakePty::new(0)),
            gate,
        );

        let first = first.begin_input(vec![1]).expect("admit first Run");
        first_started_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("first Run occupies the global slot");
        let second = second.begin_input(vec![2]).expect("queue second Run");
        assert!(
            second_started_rx
                .recv_timeout(Duration::from_millis(50))
                .is_err(),
            "second Run entered the one-slot writer gate"
        );

        release_writer(&first_release);
        first.resolve().await.expect("first Run drains");
        second_started_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("second Run receives the released slot");
        release_writer(&second_release);
        second.resolve().await.expect("second Run drains");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn global_gate_skips_a_stopped_waiter_without_starting_its_writer() {
        let gate = InputDrainGate::with_limits(1, 1, 1);
        let (first_started_tx, first_started_rx) = mpsc::sync_channel(1);
        let first_release = Arc::new((Mutex::new(false), Condvar::new()));
        let (first, _first_child) = owner(
            Box::new(BlockingWriter {
                started: Some(first_started_tx),
                release: Arc::clone(&first_release),
                written: Arc::new(Mutex::new(0)),
            }),
            Box::new(FakePty::new(0)),
            gate.clone(),
        );
        let (stale_started_tx, stale_started_rx) = mpsc::sync_channel(1);
        let (stale, _stale_child) = owner(
            Box::new(BlockingWriter {
                started: Some(stale_started_tx),
                release: Arc::new((Mutex::new(true), Condvar::new())),
                written: Arc::new(Mutex::new(0)),
            }),
            Box::new(FakePty::new(0)),
            gate.clone(),
        );
        let (live_started_tx, live_started_rx) = mpsc::sync_channel(1);
        let live_release = Arc::new((Mutex::new(false), Condvar::new()));
        let (live, _live_child) = owner(
            Box::new(BlockingWriter {
                started: Some(live_started_tx),
                release: Arc::clone(&live_release),
                written: Arc::new(Mutex::new(0)),
            }),
            Box::new(FakePty::new(0)),
            gate,
        );

        let first = first.begin_input(vec![1]).expect("occupy global slot");
        first_started_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("first writer starts");
        let stale_result = stale.begin_input(vec![2]).expect("queue stale Run");
        let live_result = live.begin_input(vec![3]).expect("queue live Run");
        stale.mark_closed();
        assert_eq!(
            stale_result
                .resolve()
                .await
                .expect_err("closed waiter is rejected")
                .disposition,
            CommandDisposition::NotApplied
        );

        release_writer(&first_release);
        first.resolve().await.expect("first Run drains");
        live_started_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("live Run skips the stale ticket");
        assert!(
            stale_started_rx.try_recv().is_err(),
            "stopped waiting Run never starts its writer"
        );
        release_writer(&live_release);
        live_result.resolve().await.expect("live Run drains");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn one_command_bursts_handoff_round_robin_between_waiting_runs() {
        let gate = InputDrainGate::with_limits(1, 1, 1);
        let order = Arc::new(Mutex::new(Vec::new()));
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let (first, _first_child) = owner(
            Box::new(OrderedBlockingWriter {
                label: b'A',
                order: Arc::clone(&order),
                started: Some(started_tx),
                release: Arc::clone(&release),
            }),
            Box::new(FakePty::new(0)),
            gate.clone(),
        );
        let (second, _second_child) = owner(
            Box::new(OrderedWriter {
                label: b'B',
                order: Arc::clone(&order),
            }),
            Box::new(FakePty::new(0)),
            gate,
        );

        let first_one = first.begin_input(vec![1]).expect("start first Run");
        started_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("first Run blocks inside its first burst");
        let first_two = first.begin_input(vec![2]).expect("queue first Run again");
        let second_one = second.begin_input(vec![3]).expect("queue second Run");
        release_writer(&release);
        first_one.resolve().await.expect("first burst resolves");
        second_one
            .resolve()
            .await
            .expect("second Run receives handoff");
        first_two
            .resolve()
            .await
            .expect("first Run resumes afterward");
        assert_eq!(*mutex_lock(&order), b"ABA");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stop_fences_queued_input_but_does_not_wait_for_the_active_writer() {
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let (owner, child) = owner(
            Box::new(BlockingWriter {
                started: Some(started_tx),
                release: Arc::clone(&release),
                written: Arc::new(Mutex::new(0)),
            }),
            Box::new(FakePty::new(0)),
            InputDrainGate::default(),
        );
        let active = owner.begin_input(vec![1]).expect("admit active input");
        started_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("input blocks inside writer");
        let queued = owner.begin_input(vec![2]).expect("queue second input");

        let stop = owner.begin_stop().expect("stop uses independent lane");
        let super::ChildCommand::Stop { reply, deadline: _ } = child
            .recv_timeout(Duration::from_secs(2))
            .expect("waiter receives stop without writer release")
        else {
            panic!("public stop sends the stop command variant");
        };
        owner
            .commit_pending_stop()
            .expect("production owner commits independent Stop lane");
        reply
            .send(StopOwnerResult::Accepted(StopDisposition::Graceful))
            .expect("acknowledge stop");
        assert_eq!(
            stop.resolve(Duration::from_secs(1))
                .await
                .expect("stop is accepted"),
            ControlReceipt::Stop {
                disposition: StopDisposition::Graceful,
            }
        );
        let rejected = queued
            .resolve()
            .await
            .expect_err("unstarted input is fenced");
        assert_eq!(rejected.disposition, CommandDisposition::NotApplied);
        assert_eq!(rejected.error.code, ErrorCode::InvalidRunState);
        assert_eq!(
            owner
                .begin_input(vec![3])
                .expect_err("new input is fenced")
                .disposition,
            CommandDisposition::NotApplied
        );

        release_writer(&release);
        active
            .resolve()
            .await
            .expect("already-started input retains its own outcome");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn input_failure_is_unknown_but_resize_and_stop_remain_live() {
        let (owner, child) = owner(
            Box::new(FailingWriter),
            Box::new(FakePty::new(1)),
            InputDrainGate::default(),
        );
        let failure = owner
            .begin_input(vec![1])
            .expect("admit failing input")
            .resolve()
            .await
            .expect_err("write failure is reported");
        assert_eq!(failure.error.code, ErrorCode::Io);
        assert_eq!(failure.disposition, CommandDisposition::Unknown);
        assert_eq!(
            owner
                .begin_input(vec![2])
                .expect_err("failed input lane rejects new bytes")
                .disposition,
            CommandDisposition::NotApplied
        );

        // This fake master clamps rows 30 -> 31, which is the only way to tell
        // a read-back apart from an echo of the request: every field the owner
        // reports -- receipt, published event, and retained `confirmed_size` --
        // must carry 31, the size the terminal acknowledged, not the 30 asked
        // for.
        let mut published = Vec::new();
        assert_eq!(
            owner
                .resize(TerminalSize { rows: 30, cols: 90 }, |size| published
                    .push(size))
                .expect("resize remains available"),
            ControlReceipt::Resize {
                applied_size: TerminalSize { rows: 31, cols: 90 },
            }
        );
        assert_eq!(
            published,
            vec![TerminalSize { rows: 31, cols: 90 }],
            "exactly one event is published, carrying the clamped read-back"
        );
        assert_eq!(
            owner.confirmed_size(),
            Some(TerminalSize { rows: 31, cols: 90 }),
            "the retained size is the clamped read-back, not the request"
        );
        let stop = owner.begin_stop().expect("stop remains available");
        let super::ChildCommand::Stop { reply, deadline: _ } = child
            .recv_timeout(Duration::from_secs(2))
            .expect("stop reaches child waiter")
        else {
            panic!("public stop sends the stop command variant");
        };
        owner
            .commit_pending_stop()
            .expect("production owner commits Stop after input failure");
        reply
            .send(StopOwnerResult::Accepted(StopDisposition::Graceful))
            .expect("acknowledge stop");
        assert_eq!(
            stop.resolve(Duration::from_secs(1))
                .await
                .expect("stop accepted after input failure"),
            ControlReceipt::Stop {
                disposition: StopDisposition::Graceful,
            }
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn recoverable_partial_write_retains_unknown_without_an_applied_range() {
        let written = Arc::new(Mutex::new(Vec::new()));
        let (owner, _child) = owner(
            Box::new(PrefixThenFailWriter {
                wrote_prefix: false,
                written: Arc::clone(&written),
            }),
            Box::new(FakePty::new(0)),
            InputDrainGate::default(),
        );
        let key = InputOperationKey::new("partial").unwrap();
        let first = owner
            .begin_recoverable_input(key.clone(), 0, b"AB".to_vec())
            .expect("admit partial write")
            .resolve()
            .await
            .expect_err("partial write is ambiguous");
        assert_eq!(first.disposition, CommandDisposition::Unknown);
        assert_eq!(owner.applied_input_bytes(), 0);
        assert_eq!(*mutex_lock(&written), b"A");

        let retry = owner
            .begin_recoverable_input(key, 0, b"AB".to_vec())
            .expect("unknown operation remains retained")
            .resolve()
            .await
            .expect_err("retry returns the same unknown result");
        assert_eq!(retry, first);
        assert_eq!(*mutex_lock(&written), b"A");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn writer_panic_fails_its_lane_and_hands_the_global_slot_to_another_run() {
        let gate = InputDrainGate::with_limits(1, 64, 256 * 1024);
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let (panicking, _panicking_child) = owner(
            Box::new(PanickingWriter {
                started: Some(started_tx),
                release: Arc::clone(&release),
            }),
            Box::new(FakePty::new(0)),
            gate.clone(),
        );
        let written = Arc::new(Mutex::new(Vec::new()));
        let (healthy, _healthy_child) = owner(
            Box::new(RecordingWriter(Arc::clone(&written))),
            Box::new(FakePty::new(0)),
            gate,
        );

        let failed = panicking
            .begin_input(vec![1])
            .expect("admit panicking input");
        started_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("writer reaches panic barrier");
        let rejected = panicking
            .begin_input(vec![2])
            .expect("queue input behind panicking writer");
        let healthy = healthy
            .begin_input(vec![3])
            .expect("queue another Run behind panicking writer");
        release_writer(&release);

        let failure = failed
            .resolve()
            .await
            .expect_err("started panic has unknown disposition");
        assert_eq!(failure.error.code, ErrorCode::Internal);
        assert_eq!(failure.disposition, CommandDisposition::Unknown);
        let rejection = rejected
            .resolve()
            .await
            .expect_err("unstarted input is not applied");
        assert_eq!(rejection.error.code, ErrorCode::Internal);
        assert_eq!(rejection.disposition, CommandDisposition::NotApplied);
        healthy
            .resolve()
            .await
            .expect("global slot is handed to healthy Run");
        assert_eq!(*mutex_lock(&written), vec![3]);
    }

    /// Concurrent resizes publish in the same order they are confirmed.
    ///
    /// The store and the publish must be one atomic step. If the owner released
    /// its lock before publishing, two resizes could be confirmed as 24 then 87
    /// but published as 87 then 24 -- an observer would watch the terminal
    /// shrink back, and the last event it received would disagree with the
    /// Run's `current_size`.
    ///
    /// The gate has to sit in the publish callback, not in the PTY: a PTY that
    /// stalls stalls *inside* the lock either way, so both orderings look
    /// identical from there. Blocking the first resize's callback before it
    /// records anything is what separates them -- while it waits, a second
    /// resize either cannot proceed (lock held across publish, correct) or runs
    /// to completion and publishes first (lock released early, the bug).
    #[test]
    fn concurrent_resizes_publish_in_confirmation_order() {
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let (entered_tx, entered_rx) = mpsc::sync_channel::<()>(1);
        let (owner, _commands) = owner(
            Box::new(RecordingWriter(Arc::new(Mutex::new(Vec::new())))),
            Box::new(FakePty::new(0)),
            InputDrainGate::with_limits(1, 64, 256 * 1024),
        );

        // The first callback parks while holding this log's lock, so a second
        // publish that does slip through blocks here rather than recording --
        // which is exactly what must be observed. The assertion below therefore
        // checks the log is EMPTY at a point where a correct implementation has
        // not even entered the second publish, rather than trying to catch the
        // push itself.
        let published = Arc::new(Mutex::new(Vec::new()));
        let first = {
            let owner = owner.clone();
            let published = Arc::clone(&published);
            std::thread::spawn(move || {
                owner
                    .resize(TerminalSize { rows: 24, cols: 80 }, |size| {
                        // Announce and wait BEFORE recording. Recording first
                        // would make both orderings produce the same log.
                        entered_tx.send(()).expect("report publish entry");
                        release_rx.recv().expect("await release inside publish");
                        mutex_lock(&published).push(size);
                    })
                    .expect("first resize applies");
            })
        };
        entered_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("first resize reaches its publish callback");

        let second_size = TerminalSize {
            rows: 87,
            cols: 200,
        };
        let second = {
            let owner = owner.clone();
            let published = Arc::clone(&published);
            std::thread::spawn(move || {
                owner.resize(second_size, |size| mutex_lock(&published).push(size))
            })
        };

        // The original held-callback workload and observation budget remain.
        // Public callers receive truthful before-effect pressure instead of
        // blocking a Tokio worker while this owner is held.
        std::thread::sleep(Duration::from_millis(200));
        assert!(
            mutex_lock(&published).is_empty(),
            "a second resize must not publish while the first holds the owner"
        );
        let failure = second
            .join()
            .expect("busy second resize returns")
            .unwrap_err();
        assert_eq!(failure.error.code, ErrorCode::ControlBackpressure);
        assert_eq!(failure.disposition, CommandDisposition::NotApplied);
        assert_eq!(failure.confirmed_input_bytes, None);

        release_tx.send(()).expect("release the gated publish");
        first.join().expect("first resize thread finishes");
        // Explicitly reissue the identical unaccepted request after the actual
        // owner release. Both accepted resizes still apply and publish exactly.
        owner
            .resize(second_size, |size| mutex_lock(&published).push(size))
            .expect("second resize applies after the real owner release");

        let published = mutex_lock(&published).clone();
        assert_eq!(
            published,
            vec![
                TerminalSize { rows: 24, cols: 80 },
                TerminalSize {
                    rows: 87,
                    cols: 200
                },
            ],
            "resizes publish in the order they were confirmed"
        );
        assert_eq!(
            owner.confirmed_size(),
            published.last().copied(),
            "the retained size equals the last size published"
        );
    }

    /// The vendored portable-pty fork resolves a master's tty name lazily, from
    /// the master, instead of eagerly from the slave at `openpty` time -- the
    /// eager call cost more than the `openpty` it followed and grew with the
    /// number of open ptys. Every spawn closes the slave, so the property that
    /// makes the deferral safe is that the answer outlives that close.
    ///
    /// Lives here, not in the fork, because the fork is patched in via
    /// `[patch.crates-io]` and its own tests never run.
    #[test]
    fn a_master_still_names_its_slave_after_the_slave_is_closed() {
        let pair = portable_pty::native_pty_system()
            .openpty(PtySize {
                rows: 24,
                cols: 80,
                pixel_width: 0,
                pixel_height: 0,
            })
            .unwrap();

        let while_open = pair
            .master
            .tty_name()
            .expect("name the slave while it is open");
        drop(pair.slave);
        let after_close = pair
            .master
            .tty_name()
            .expect("name the slave after it is closed");

        assert_eq!(
            while_open, after_close,
            "one master named two different slaves across the slave's close"
        );
        assert!(
            after_close.exists(),
            "named a device that does not exist: {}",
            after_close.display()
        );
    }
}
