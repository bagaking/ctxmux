//! Current-incarnation ownership of one native Run's PTY controls.

use std::{
    collections::{HashMap, VecDeque},
    fmt,
    io::{self, Write},
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Condvar, Mutex, Weak, mpsc},
    thread,
    time::{Duration, Instant},
};

use ctxmux_protocol::{
    AppliedInputRange, CommandDisposition, ControlFailure, ControlReceipt, ErrorCode,
    InputOperationKey, ProtocolError, RunId, TerminalSize,
};
use portable_pty::{Child, MasterPty, PtySize};
use tokio::sync::{oneshot, watch};

const INPUT_QUEUE_MAX_COMMANDS: usize = 1_024;
const INPUT_QUEUE_MAX_BYTES: usize = 4 * 1024 * 1024;
const INPUT_DRAIN_MAX_ACTIVE: usize = 8;
const INPUT_BURST_MAX_COMMANDS: usize = 64;
const INPUT_BURST_MAX_BYTES: usize = 256 * 1024;
const INPUT_RESULT_MAX_ENTRIES: usize = 256;
const INPUT_RESULT_MAX_REQUEST_BYTES: usize = 1024 * 1024;

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
    reply: oneshot::Receiver<Result<(), String>>,
}

/// One direct-child command handled by the existing waiter thread.
pub(crate) enum ChildCommand {
    Stop(oneshot::Sender<Result<(), String>>),
    CleanupUnpublished,
}

/// Daemon-wide admission for lazy blocking PTY input drains.
///
/// A Run owns no permanent input thread. At most eight burst workers exist for
/// one daemon, and each worker yields after a bounded completed burst. One
/// blocking PTY write has no independent deadline, so stalled writers may hold
/// those bounded slots until the PTY owner returns or closes.
#[derive(Clone)]
pub(crate) struct InputDrainGate {
    inner: Arc<InputDrainGateInner>,
}

struct InputDrainGateInner {
    state: Mutex<InputDrainGateState>,
    max_active: usize,
    burst_max_commands: usize,
    burst_max_bytes: usize,
}

#[derive(Default)]
struct InputDrainGateState {
    active: usize,
    waiting: VecDeque<Weak<NativeControlInner>>,
}

impl Default for InputDrainGate {
    fn default() -> Self {
        Self::with_limits(
            INPUT_DRAIN_MAX_ACTIVE,
            INPUT_BURST_MAX_COMMANDS,
            INPUT_BURST_MAX_BYTES,
        )
    }
}

impl InputDrainGate {
    fn with_limits(max_active: usize, burst_max_commands: usize, burst_max_bytes: usize) -> Self {
        debug_assert!(max_active > 0);
        debug_assert!(burst_max_commands > 0);
        debug_assert!(burst_max_bytes > 0);
        Self {
            inner: Arc::new(InputDrainGateInner {
                state: Mutex::new(InputDrainGateState::default()),
                max_active,
                burst_max_commands,
                burst_max_bytes,
            }),
        }
    }

    fn schedule(&self, owner: Arc<NativeControlInner>) {
        let start = {
            let mut state = mutex_lock(&self.inner.state);
            if state.active < self.inner.max_active {
                state.active += 1;
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
    pty: Mutex<Option<Box<dyn PtyControl>>>,
    writer: Mutex<Option<Box<dyn Write + Send>>>,
    state: Mutex<NativeControlState>,
    reap: Mutex<ChildReapState>,
    reap_changed: Condvar,
    input_drains: InputDrainGate,
}

/// Descriptor handles detached from a closed native incarnation after the
/// caller has fenced every Run lookup owner. Dropping this value closes the
/// descriptors outside the native-control and Registry locks.
#[must_use = "detached native descriptors must be dropped outside owner locks"]
pub(crate) struct DetachedNativeDescriptors {
    pty: Option<Box<dyn PtyControl>>,
    writer: Option<Box<dyn Write + Send>>,
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

enum ChildReapState {
    Pending {
        cleanup_error: Option<String>,
        wait_error: Option<String>,
    },
    WaitAuthorityLost {
        cleanup_error: Option<String>,
        wait_error: String,
        _child: Box<dyn Child + Send + Sync>,
    },
    Reaped,
}

struct NativeControlState {
    phase: ControlPhase,
    input_failure: Option<ProtocolError>,
    input_queue: VecDeque<InputCommand>,
    input_commands: usize,
    input_bytes: usize,
    input_scheduled: bool,
    applied_input_bytes: u64,
    input_operations: HashMap<InputOperationKey, InputOperationEntry>,
    completed_input_operations: VecDeque<InputOperationKey>,
    retained_input_request_bytes: usize,
    input_result_max_entries: usize,
    input_result_max_request_bytes: usize,
    child_sender: Option<mpsc::Sender<ChildCommand>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ControlPhase {
    Open,
    Stopping,
    Closed,
    Failed,
}

struct InputCommand {
    data: Arc<[u8]>,
    reply: InputReply,
}

enum InputReply {
    Legacy(oneshot::Sender<ControlResult>),
    Recoverable {
        key: InputOperationKey,
        completion: watch::Sender<Option<RecoverableInputResult>>,
    },
}

#[derive(Clone, PartialEq, Eq)]
struct InputOperationRequest {
    expected_byte: u64,
    data: Arc<[u8]>,
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
}

struct PortablePtyControl(Box<dyn MasterPty + Send>);

impl PtyControl for PortablePtyControl {
    fn resize(&self, size: PtySize) -> io::Result<()> {
        self.0.resize(size).map_err(io::Error::other)
    }

    fn get_size(&self) -> io::Result<PtySize> {
        self.0.get_size().map_err(io::Error::other)
    }
}

impl NativeControlOwner {
    pub(crate) fn run_id(&self) -> RunId {
        self.inner.run_id
    }

    pub(crate) fn new(
        run_id: RunId,
        master: Box<dyn MasterPty + Send>,
        writer: Box<dyn Write + Send>,
        input_drains: InputDrainGate,
    ) -> (Self, mpsc::Receiver<ChildCommand>) {
        Self::new_with_pty(
            run_id,
            Box::new(PortablePtyControl(master)),
            writer,
            input_drains,
        )
    }

    #[cfg(test)]
    pub(crate) fn new_for_wait_test(run_id: RunId) -> (Self, mpsc::Receiver<ChildCommand>) {
        struct TestPty;

        impl PtyControl for TestPty {
            fn resize(&self, _size: PtySize) -> io::Result<()> {
                Ok(())
            }

            fn get_size(&self) -> io::Result<PtySize> {
                Ok(PtySize::default())
            }
        }

        Self::new_with_pty(
            run_id,
            Box::new(TestPty),
            Box::new(io::sink()),
            InputDrainGate::default(),
        )
    }

    #[cfg(test)]
    pub(crate) fn retains_failed_child(&self) -> bool {
        matches!(
            &*mutex_lock(&self.inner.reap),
            ChildReapState::WaitAuthorityLost { .. }
        )
    }

    pub(crate) fn wait_authority_failure(&self) -> Option<String> {
        match &*mutex_lock(&self.inner.reap) {
            ChildReapState::WaitAuthorityLost { wait_error, .. } => Some(wait_error.clone()),
            ChildReapState::Pending { .. } | ChildReapState::Reaped => None,
        }
    }

    fn new_with_pty(
        run_id: RunId,
        pty: Box<dyn PtyControl>,
        writer: Box<dyn Write + Send>,
        input_drains: InputDrainGate,
    ) -> (Self, mpsc::Receiver<ChildCommand>) {
        Self::new_with_pty_and_input_results(
            run_id,
            pty,
            writer,
            input_drains,
            INPUT_RESULT_MAX_ENTRIES,
            INPUT_RESULT_MAX_REQUEST_BYTES,
        )
    }

    fn new_with_pty_and_input_results(
        run_id: RunId,
        pty: Box<dyn PtyControl>,
        writer: Box<dyn Write + Send>,
        input_drains: InputDrainGate,
        input_result_max_entries: usize,
        input_result_max_request_bytes: usize,
    ) -> (Self, mpsc::Receiver<ChildCommand>) {
        debug_assert!(input_result_max_entries > 0);
        debug_assert!(input_result_max_request_bytes > 0);
        let (child_sender, child_receiver) = mpsc::channel();
        (
            Self {
                inner: Arc::new(NativeControlInner {
                    run_id,
                    pty: Mutex::new(Some(pty)),
                    writer: Mutex::new(Some(writer)),
                    state: Mutex::new(NativeControlState {
                        phase: ControlPhase::Open,
                        input_failure: None,
                        input_queue: VecDeque::new(),
                        input_commands: 0,
                        input_bytes: 0,
                        input_scheduled: false,
                        applied_input_bytes: 0,
                        input_operations: HashMap::new(),
                        completed_input_operations: VecDeque::new(),
                        retained_input_request_bytes: 0,
                        input_result_max_entries,
                        input_result_max_request_bytes,
                        child_sender: Some(child_sender),
                    }),
                    reap: Mutex::new(ChildReapState::Pending {
                        cleanup_error: None,
                        wait_error: None,
                    }),
                    reap_changed: Condvar::new(),
                    input_drains,
                }),
            },
            child_receiver,
        )
    }

    pub(crate) fn begin_input(&self, data: Vec<u8>) -> Result<PendingInput, ControlFailure> {
        let (reply, schedule) = {
            let mut state = mutex_lock(&self.inner.state);
            if state.phase != ControlPhase::Open {
                return Err(not_applied(invalid_phase_error(
                    self.inner.run_id,
                    state.phase,
                    "write to",
                )));
            }
            if let Some(error) = &state.input_failure {
                return Err(not_applied(error.clone()));
            }
            if state.input_commands >= INPUT_QUEUE_MAX_COMMANDS
                || data.len() > INPUT_QUEUE_MAX_BYTES.saturating_sub(state.input_bytes)
            {
                return Err(not_applied(ProtocolError::new(
                    ErrorCode::ControlBackpressure,
                    format!(
                        "Run {} PTY input queue exceeds its 1024-command or 4-MiB bound",
                        self.inner.run_id
                    ),
                )));
            }

            let (reply_tx, reply_rx) = oneshot::channel();
            state.input_commands += 1;
            state.input_bytes += data.len();
            state.input_queue.push_back(InputCommand {
                data: Arc::from(data),
                reply: InputReply::Legacy(reply_tx),
            });
            let schedule = !state.input_scheduled;
            if schedule {
                state.input_scheduled = true;
            }
            (reply_rx, schedule)
        };
        if schedule {
            self.inner.input_drains.schedule(Arc::clone(&self.inner));
        }

        Ok(PendingInput {
            run_id: self.inner.run_id,
            reply,
        })
    }

    pub(crate) fn begin_recoverable_input(
        &self,
        key: InputOperationKey,
        expected_byte: u64,
        data: Vec<u8>,
    ) -> Result<PendingRecoverableInput, ControlFailure> {
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
        let request = InputOperationRequest {
            expected_byte,
            data: Arc::from(data),
        };
        let (pending, schedule) = {
            let mut state = mutex_lock(&self.inner.state);
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
            if let Some(error) = &state.input_failure {
                return Err(not_applied(error.clone()));
            }
            evict_completed_input_results(&mut state, request.data.len());
            if state.input_operations.len() >= state.input_result_max_entries
                || request.data.len()
                    > state
                        .input_result_max_request_bytes
                        .saturating_sub(state.retained_input_request_bytes)
                || state.input_commands >= INPUT_QUEUE_MAX_COMMANDS
                || request.data.len() > INPUT_QUEUE_MAX_BYTES.saturating_sub(state.input_bytes)
            {
                return Err(not_applied(ProtocolError::new(
                    ErrorCode::ControlBackpressure,
                    format!(
                        "Run {} recoverable Input result or queue capacity is full",
                        self.inner.run_id
                    ),
                )));
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
                data: Arc::clone(&request.data),
                reply: InputReply::Recoverable { key, completion },
            });
            let schedule = !state.input_scheduled;
            if schedule {
                state.input_scheduled = true;
            }
            (
                PendingRecoverableInput::Pending {
                    run_id: self.inner.run_id,
                    result,
                },
                schedule,
            )
        };
        if schedule {
            self.inner.input_drains.schedule(Arc::clone(&self.inner));
        }
        Ok(pending)
    }

    pub(crate) fn applied_input_bytes(&self) -> u64 {
        mutex_lock(&self.inner.state).applied_input_bytes
    }

    pub(crate) fn begin_stop(&self) -> Result<PendingStop, ControlFailure> {
        Ok(PendingStop {
            run_id: self.inner.run_id,
            reply: self.begin_stop_inner()?,
        })
    }

    /// Ask the child-handle waiter to clean up a Run rejected before durable
    /// publication. Completion remains a separate waiter-owned reap receipt.
    pub(crate) fn cleanup_unpublished(&self) -> Result<(), String> {
        let (sender, rejected) = {
            let mut state = mutex_lock(&self.inner.state);
            match state.phase {
                ControlPhase::Open => state.phase = ControlPhase::Stopping,
                ControlPhase::Stopping => return Ok(()),
                ControlPhase::Closed | ControlPhase::Failed => return self.reap_result(),
            }
            let Some(sender) = state.child_sender.clone() else {
                let error = format!(
                    "Run {} child owner channel closed before unpublished cleanup",
                    self.inner.run_id
                );
                self.record_cleanup_error(error.clone());
                return Err(error);
            };
            let rejected = reject_queued_inputs(
                &mut state,
                &ProtocolError::new(
                    ErrorCode::InvalidRunState,
                    format!("cannot write to stopping Run {}", self.inner.run_id),
                ),
            );
            (sender, rejected)
        };
        send_rejections(rejected);
        sender.send(ChildCommand::CleanupUnpublished).map_err(|_| {
            let error = format!(
                "Run {} child owner channel closed before unpublished cleanup",
                self.inner.run_id
            );
            self.record_cleanup_error(error.clone());
            error
        })
    }

    fn begin_stop_inner(&self) -> Result<oneshot::Receiver<Result<(), String>>, ControlFailure> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let (sender, rejected) = {
            let mut state = mutex_lock(&self.inner.state);
            if state.phase != ControlPhase::Open {
                return Err(not_applied(invalid_phase_error(
                    self.inner.run_id,
                    state.phase,
                    "stop",
                )));
            }
            let Some(sender) = state.child_sender.clone() else {
                state.phase = ControlPhase::Closed;
                return Err(not_applied(ProtocolError::new(
                    ErrorCode::InvalidRunState,
                    format!("cannot stop exited Run {}", self.inner.run_id),
                )));
            };
            state.phase = ControlPhase::Stopping;
            let rejected = reject_queued_inputs(
                &mut state,
                &ProtocolError::new(
                    ErrorCode::InvalidRunState,
                    format!("cannot write to stopping Run {}", self.inner.run_id),
                ),
            );
            (sender, rejected)
        };
        send_rejections(rejected);
        if sender.send(ChildCommand::Stop(reply_tx)).is_err() {
            self.mark_closed();
            let phase = mutex_lock(&self.inner.state).phase;
            return Err(not_applied(invalid_phase_error(
                self.inner.run_id,
                phase,
                "stop",
            )));
        }
        Ok(reply_rx)
    }

    /// Fence all future live control as soon as the waiter loses child
    /// authority, before terminal `RunState` publication can lag behind it.
    pub(crate) fn mark_closed(&self) {
        let rejected = {
            let mut state = mutex_lock(&self.inner.state);
            if state.phase != ControlPhase::Failed {
                state.phase = ControlPhase::Closed;
            }
            state.child_sender = None;
            let phase = state.phase;
            reject_queued_inputs(
                &mut state,
                &invalid_phase_error(self.inner.run_id, phase, "write to"),
            )
        };
        send_rejections(rejected);
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
        let rejected = {
            let mut state = mutex_lock(&self.inner.state);
            state.phase = ControlPhase::Failed;
            state.child_sender = None;
            reject_queued_inputs(
                &mut state,
                &ProtocolError::new(ErrorCode::BackendUnavailable, error),
            )
        };
        send_rejections(rejected);
    }

    pub(crate) fn has_continuation_authority(&self) -> bool {
        mutex_lock(&self.inner.state).phase == ControlPhase::Open
    }

    /// Record the only successful terminal-and-reaped proof: the waiter
    /// observed `try_wait(Some(_))` before any authority-loss transfer. Once
    /// the handle moves into `WaitAuthorityLost`, this cannot replace it.
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
            | ChildReapState::WaitAuthorityLost { cleanup_error, .. } => {
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
                ChildReapState::WaitAuthorityLost { .. } => {
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

    /// Prove that a closed native owner retains no child, control, or input
    /// worker. This is only the Backend-local part of collection eligibility;
    /// the Registry must separately fence Run lookup pins and terminal state.
    pub(crate) fn closed_quiescence_result(&self) -> Result<(), String> {
        self.reap_result()?;
        let state = mutex_lock(&self.inner.state);
        if state.phase != ControlPhase::Closed
            || state.child_sender.is_some()
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
            Ok(Ok(Ok(()))) => Ok(ControlReceipt::Stop),
            Ok(Ok(Err(error))) => Err(unknown(ProtocolError::new(ErrorCode::Io, error))),
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

impl NativeControlOwner {
    pub(crate) fn resize(&self, size: TerminalSize) -> ControlResult {
        // The phase lock makes stop/exit a fence for new resize operations.
        // portable-pty resize/get_size are short ioctl calls; no lock crosses
        // an await or the broader Run metadata path.
        let state = mutex_lock(&self.inner.state);
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
        if applied.rows == 0 || applied.cols == 0 {
            return Err(unknown(ProtocolError::new(
                ErrorCode::Io,
                format!(
                    "Run {} PTY returned an invalid zero applied size",
                    self.inner.run_id
                ),
            )));
        }
        Ok(ControlReceipt::Resize {
            applied_size: TerminalSize {
                rows: applied.rows,
                cols: applied.cols,
            },
        })
    }
}

impl NativeControlInner {
    fn has_scheduled_input(&self) -> bool {
        let state = mutex_lock(&self.state);
        state.input_scheduled && !state.input_queue.is_empty()
    }

    fn drain_burst(&self, max_commands: usize, max_bytes: usize) -> bool {
        let mut commands = 0;
        let mut bytes = 0;
        loop {
            if commands > 0 && (commands >= max_commands || bytes >= max_bytes) {
                return self.has_more_or_unschedule();
            }
            let command = {
                let mut state = mutex_lock(&self.state);
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
    fn execute_input(&self, command: InputCommand) -> bool {
        let written_bytes = command.data.len();
        let expected_cursor = match &command.reply {
            InputReply::Legacy(_) => None,
            InputReply::Recoverable { key, .. } => {
                let mut state = mutex_lock(&self.state);
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
        let Some(end_byte) = mutex_lock(&self.state)
            .applied_input_bytes
            .checked_add(u64::try_from(written_bytes).expect("bounded frame length fits u64"))
        else {
            let mut state = mutex_lock(&self.state);
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
            writer
                .write_all(&command.data)
                .and_then(|()| writer.flush())
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
        let mut state = mutex_lock(&self.state);
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

    fn has_more_or_unschedule(&self) -> bool {
        let mut state = mutex_lock(&self.state);
        if state.input_queue.is_empty() {
            state.input_scheduled = false;
            false
        } else {
            true
        }
    }

    fn fail_scheduled(&self, message: String) {
        let rejected = {
            let mut state = mutex_lock(&self.state);
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
    let mut rejected = Vec::with_capacity(state.input_queue.len());
    while let Some(command) = state.input_queue.pop_front() {
        release_input_capacity(state, command.data.len());
        if let InputReply::Recoverable { key, .. } = &command.reply {
            remove_input_operation(state, key);
        }
        rejected.push((command.reply, not_applied(error.clone())));
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
    }
}

fn unknown(error: ProtocolError) -> ControlFailure {
    ControlFailure {
        error,
        disposition: CommandDisposition::Unknown,
    }
}

const fn to_pty_size(size: TerminalSize) -> PtySize {
    PtySize {
        rows: size.rows,
        cols: size.cols,
        pixel_width: 0,
        pixel_height: 0,
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
        io,
        sync::{
            Arc, Condvar, Mutex,
            atomic::{AtomicUsize, Ordering},
            mpsc,
        },
        time::{Duration, Instant},
    };

    use ctxmux_protocol::{
        AppliedInputRange, CommandDisposition, ControlReceipt, ErrorCode, InputOperationKey, RunId,
        TerminalSize,
    };
    use portable_pty::PtySize;

    use super::{InputDrainGate, NativeControlOwner, PtyControl, mutex_lock};

    struct FakePty {
        size: Mutex<PtySize>,
        readback_row_delta: u16,
    }

    impl FakePty {
        fn new(readback_row_delta: u16) -> Self {
            Self {
                size: Mutex::new(PtySize::default()),
                readback_row_delta,
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
            Ok(*mutex_lock(&self.size))
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
    }

    struct DropCountingWriter(Arc<AtomicUsize>);

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

    fn owner(
        writer: Box<dyn io::Write + Send>,
        pty: Box<dyn PtyControl>,
        gate: InputDrainGate,
    ) -> (NativeControlOwner, mpsc::Receiver<super::ChildCommand>) {
        NativeControlOwner::new_with_pty(RunId::new(), pty, writer, gate)
    }

    fn release_writer(release: &Arc<(Mutex<bool>, Condvar)>) {
        let (released, wake) = &**release;
        *mutex_lock(released) = true;
        wake.notify_all();
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
        let super::ChildCommand::Stop(reply) = child
            .recv_timeout(Duration::from_secs(1))
            .expect("fixture receives stop")
        else {
            panic!("public stop sends the stop command variant");
        };
        reply.send(Ok(())).expect("acknowledge fixture stop");
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
                .resize(TerminalSize { rows: 24, cols: 80 })
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
            pending.push((data.len(), owner.begin_input(data).expect("admit input")));
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
        let (owner, _child) = NativeControlOwner::new_with_pty_and_input_results(
            RunId::new(),
            Box::new(FakePty::new(0)),
            Box::new(RecordingWriter(Arc::clone(&written))),
            InputDrainGate::default(),
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
        let super::ChildCommand::Stop(reply) = child
            .recv_timeout(Duration::from_secs(2))
            .expect("waiter receives stop without writer release")
        else {
            panic!("public stop sends the stop command variant");
        };
        reply.send(Ok(())).expect("acknowledge stop");
        assert_eq!(
            stop.resolve(Duration::from_secs(1))
                .await
                .expect("stop is accepted"),
            ControlReceipt::Stop
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

        assert_eq!(
            owner
                .resize(TerminalSize { rows: 30, cols: 90 })
                .expect("resize remains available"),
            ControlReceipt::Resize {
                applied_size: TerminalSize { rows: 31, cols: 90 },
            }
        );
        let stop = owner.begin_stop().expect("stop remains available");
        let super::ChildCommand::Stop(reply) = child
            .recv_timeout(Duration::from_secs(2))
            .expect("stop reaches child waiter")
        else {
            panic!("public stop sends the stop command variant");
        };
        reply.send(Ok(())).expect("acknowledge stop");
        assert_eq!(
            stop.resolve(Duration::from_secs(1))
                .await
                .expect("stop accepted after input failure"),
            ControlReceipt::Stop
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
}
