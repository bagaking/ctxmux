use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
};

use ctxmux_protocol::{
    AttachmentCommandId, ClientFrame, ControlFailure, ControlOutcome, ControlReceipt,
    OutputGapCauses, RecoverableStop, RunEvent, RunSignal, ServerFrame, TerminalSize, decode_frame,
    encode_frame,
};
use futures_util::{
    StreamExt,
    stream::{SplitSink, SplitStream},
};
use tokio::{
    sync::{Mutex as AsyncMutex, Notify, mpsc, oneshot},
    task::AbortHandle,
};

use super::{
    AttachmentControlAccepted, AttachmentUnavailableReason, AttachmentUnknownReason, ClientError,
    InputReceipt, ResizeReceipt, SignalReceipt, StopReceipt, Wire, control_not_applied,
    decode_input_receipt, decode_resize_receipt, decode_signal_receipt, decode_stop_receipt,
    send_encoded_sink, send_sink, validate_control_failure,
};

type WireSink = SplitSink<Wire, String>;
type WireStream = SplitStream<Wire>;

// Per-attachment pipeline windows bound queued envelopes and payload while a
// peer is slow. Count limits cover empty/small commands; byte limits cover large
// commands. Exhaustion is explicit backpressure before enqueue, with recoverable
// command identity preserved. These windows do not cap fleet/lifetime work;
// independent short-client requests use the same public protocol.
const MAX_PENDING_COMMANDS: usize = 64;
const MAX_PENDING_INPUT_COMMANDS: usize = 32;
const MAX_PENDING_INPUT_BYTES: usize = 1024 * 1024;
const MAX_QUEUED_EVENT_BYTES: usize = ctxmux_protocol::MAX_FRAME_BYTES;

/// Per-view operating policy. Payload and envelope allocations have separate
/// owners, so accounting envelopes does not shrink the accepted payload window.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AttachmentEventLimits {
    /// Retained Output and tmux-name payload, preserving the existing window.
    pub payload_bytes: usize,
    /// Inbox state, actual event-slot capacity, allocation slack and signal text.
    pub envelope_bytes: usize,
}

impl Default for AttachmentEventLimits {
    fn default() -> Self {
        Self {
            payload_bytes: MAX_QUEUED_EVENT_BYTES,
            // One peer-frame-sized envelope operating point, allocated lazily;
            // this is configurable policy, not an event-population ceiling.
            envelope_bytes: ctxmux_protocol::MAX_FRAME_BYTES,
        }
    }
}

impl AttachmentEventLimits {
    pub(super) fn validate(self) -> Result<(), ClientError> {
        if self.payload_bytes == 0
            || self.envelope_bytes < std::mem::size_of::<EventInbox>()
            || self
                .payload_bytes
                .checked_add(self.envelope_bytes)
                .is_none_or(|n| n > isize::MAX as usize)
        {
            return Err(ClientError::InvalidAttachmentEventLimits);
        }
        Ok(())
    }
}

/// Live attachment to one daemon-owned Run.
pub struct Attachment {
    shared: Arc<AttachmentShared>,
    writer_tx: mpsc::Sender<WriterCommand>,
}

impl Attachment {
    #[cfg(test)]
    pub(super) fn from_wire(wire: Wire) -> Self {
        Self::from_wire_with_event_limits(wire, AttachmentEventLimits::default())
    }

    pub(super) fn from_wire_with_event_limits(wire: Wire, limits: AttachmentEventLimits) -> Self {
        let (sink, stream) = wire.split();
        let shared = Arc::new(AttachmentShared::with_event_limits(limits));
        let (writer_tx, writer_rx) = mpsc::channel(MAX_PENDING_COMMANDS);

        let reader_shared = Arc::clone(&shared);
        let reader = tokio::spawn(async move {
            reader_loop(stream, reader_shared).await;
        });
        shared
            .reader_abort
            .set(reader.abort_handle())
            .expect("attachment reader abort handle is initialized once");

        let writer_shared = Arc::clone(&shared);
        let writer = tokio::spawn(async move {
            writer_loop(sink, writer_rx, writer_shared).await;
        });
        shared
            .writer_abort
            .set(writer.abort_handle())
            .expect("attachment writer abort handle is initialized once");

        Self { shared, writer_tx }
    }

    /// Write bytes through this attachment and await its PTY owner receipt.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] when local admission, the live owner, or the
    /// attachment transport cannot produce a unique correlated result.
    pub async fn input(
        &self,
        data: Vec<u8>,
    ) -> Result<AttachmentControlAccepted<InputReceipt>, ClientError> {
        let expected_bytes = data.len();
        let (command_id, receipt) = self
            .issue_command(PendingKind::Input { expected_bytes }, |command_id| {
                ClientFrame::Input { command_id, data }
            })
            .await?;
        let ValidatedReceipt::Input(receipt) = receipt else {
            return Err(ClientError::ProtocolContractViolation(
                "validated input command resolved with another receipt kind",
            ));
        };
        Ok(AttachmentControlAccepted {
            command_id,
            receipt,
        })
    }

    /// Resize through this attachment and await the applied PTY size.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] when local admission, the live owner, or the
    /// attachment transport cannot produce a unique correlated result.
    pub async fn resize(
        &self,
        size: TerminalSize,
    ) -> Result<AttachmentControlAccepted<ResizeReceipt>, ClientError> {
        let (command_id, receipt) = self
            .issue_command(PendingKind::Resize, |command_id| ClientFrame::Resize {
                command_id,
                size,
            })
            .await?;
        let ValidatedReceipt::Resize(receipt) = receipt else {
            return Err(ClientError::ProtocolContractViolation(
                "validated resize command resolved with another receipt kind",
            ));
        };
        Ok(AttachmentControlAccepted {
            command_id,
            receipt,
        })
    }

    /// Interrupt the attached Run without ending Run ownership.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] when the live owner cannot prove signal delivery.
    pub async fn interrupt(&self) -> Result<AttachmentControlAccepted<SignalReceipt>, ClientError> {
        let (command_id, receipt) = self
            .issue_command(PendingKind::Signal(RunSignal::Interrupt), |command_id| {
                ClientFrame::Signal {
                    command_id,
                    signal: RunSignal::Interrupt,
                }
            })
            .await?;
        let ValidatedReceipt::Signal(receipt) = receipt else {
            return Err(ClientError::ProtocolContractViolation(
                "validated signal command resolved with another receipt kind",
            ));
        };
        Ok(AttachmentControlAccepted {
            command_id,
            receipt,
        })
    }

    /// Apply or recover one caller-retained Stop operation through this
    /// attachment and await complete owned-session quiescence.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] when local admission, the live owner, or the
    /// attachment transport cannot produce a unique correlated result.
    pub async fn stop(
        &self,
        operation: RecoverableStop,
    ) -> Result<AttachmentControlAccepted<StopReceipt>, ClientError> {
        let (command_id, receipt) = self
            .issue_command(PendingKind::Stop, |command_id| ClientFrame::Stop {
                command_id,
                operation,
            })
            .await?;
        let ValidatedReceipt::Stop(receipt) = receipt else {
            return Err(ClientError::ProtocolContractViolation(
                "validated stop command resolved with another receipt kind",
            ));
        };
        Ok(AttachmentControlAccepted {
            command_id,
            receipt,
        })
    }

    /// Wait for the next live Run event. A clean detach or terminal EOF returns `None`.
    /// Only one `next_event` call may be active for an Attachment at a time.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] when transport, frame decoding, or an
    /// attachment delivery invariant fails.
    pub async fn next_event(&self) -> Result<Option<RunEvent>, ClientError> {
        self.shared.events.next().await
    }

    /// Detach cleanly without affecting the Run.
    ///
    /// New commands are fenced before pending results drain. The detach frame
    /// is sent only after every admitted command has a unique result.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] when pending results or the detach
    /// acknowledgement are lost.
    pub async fn detach(self) -> Result<(), ClientError> {
        self.shared.begin_detach()?;
        self.shared.wait_until_pending_empty().await?;
        self.writer_tx
            .try_send(WriterCommand::Detach)
            .map_err(|_| ClientError::AttachmentUnavailable {
                reason: AttachmentUnavailableReason::Closed,
            })?;
        self.shared.wait_until_detached().await
    }

    /// Abruptly close this client attachment without affecting the Run.
    ///
    /// Commands without their unique daemon result become locally unknown.
    pub fn close(self) {}

    async fn issue_command(
        &self,
        kind: PendingKind,
        frame: impl FnOnce(AttachmentCommandId) -> ClientFrame,
    ) -> Result<(AttachmentCommandId, ValidatedReceipt), ClientError> {
        let issue_guard = self.shared.issue_lock.lock().await;
        let (result_tx, result_rx) = oneshot::channel();
        let command_id = self.shared.register_command(kind, result_tx)?;
        let encoded = match encode_frame(&frame(command_id)) {
            Ok(encoded) => encoded,
            Err(error) => {
                self.shared.cancel_unsent_command(command_id);
                return Err(control_not_applied(ClientError::Frame(error)));
            }
        };
        match self.writer_tx.try_send(WriterCommand::Control(encoded)) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.shared.cancel_unsent_command(command_id);
                return Err(ClientError::AttachmentBackpressure {
                    limit: "64 queued writer commands",
                });
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.shared.cancel_unsent_command(command_id);
                self.shared.terminate(
                    AttachmentUnknownReason::TransportTerminated,
                    Some(ClientError::Closed),
                );
                return Err(control_not_applied(ClientError::Closed));
            }
        }
        drop(issue_guard);

        match result_rx.await {
            Ok(PendingResolution::Accepted(receipt)) => Ok((command_id, receipt)),
            Ok(PendingResolution::Rejected(failure)) => {
                Err(ClientError::ControlRejected { failure })
            }
            Ok(PendingResolution::Unknown(reason)) => {
                Err(ClientError::AttachmentCommandUnknown { command_id, reason })
            }
            Err(_) => Err(ClientError::AttachmentCommandUnknown {
                command_id,
                reason: AttachmentUnknownReason::TransportTerminated,
            }),
        }
    }
}

impl Drop for Attachment {
    fn drop(&mut self) {
        self.shared
            .terminate(AttachmentUnknownReason::ClosedLocally, None);
    }
}

#[derive(Debug, Clone, Copy)]
enum PendingKind {
    Input { expected_bytes: usize },
    Resize,
    Signal(RunSignal),
    Stop,
}

impl PendingKind {
    fn input_bytes(self) -> usize {
        match self {
            Self::Input { expected_bytes } => expected_bytes,
            Self::Resize | Self::Signal(_) | Self::Stop => 0,
        }
    }

    fn validate(self, receipt: &ControlReceipt) -> Result<ValidatedReceipt, &'static str> {
        match self {
            Self::Input { expected_bytes } => decode_input_receipt(receipt, expected_bytes)
                .map(ValidatedReceipt::Input)
                .map_err(|_| "attachment input receipt does not match its command"),
            Self::Resize => decode_resize_receipt(receipt)
                .map(ValidatedReceipt::Resize)
                .map_err(|_| "attachment resize receipt does not match its command"),
            Self::Signal(signal) => decode_signal_receipt(receipt, signal)
                .map(ValidatedReceipt::Signal)
                .map_err(|_| "attachment signal receipt does not match its command"),
            Self::Stop => decode_stop_receipt(receipt)
                .map(ValidatedReceipt::Stop)
                .map_err(|_| "attachment stop receipt does not match its command"),
        }
    }
}

#[derive(Debug)]
enum ValidatedReceipt {
    Input(InputReceipt),
    Resize(ResizeReceipt),
    Signal(SignalReceipt),
    Stop(StopReceipt),
}

enum PendingResolution {
    Accepted(ValidatedReceipt),
    Rejected(ControlFailure),
    Unknown(AttachmentUnknownReason),
}

struct PendingCommand {
    kind: PendingKind,
    result_tx: oneshot::Sender<PendingResolution>,
}

enum WriterCommand {
    Control(String),
    Detach,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AttachmentEnd {
    Detached,
    Terminated,
}

struct AttachmentState {
    next_command_id: u32,
    fence: Option<AttachmentUnavailableReason>,
    detaching: bool,
    end: Option<AttachmentEnd>,
    pending: HashMap<AttachmentCommandId, PendingCommand>,
    pending_inputs: usize,
    pending_input_bytes: usize,
}

impl Default for AttachmentState {
    fn default() -> Self {
        Self {
            next_command_id: 1,
            fence: None,
            detaching: false,
            end: None,
            pending: HashMap::new(),
            pending_inputs: 0,
            pending_input_bytes: 0,
        }
    }
}

struct AttachmentShared {
    state: Mutex<AttachmentState>,
    issue_lock: AsyncMutex<()>,
    state_changed: Notify,
    events: EventInbox,
    reader_abort: OnceLock<AbortHandle>,
    writer_abort: OnceLock<AbortHandle>,
}

impl AttachmentShared {
    #[cfg(test)]
    fn new() -> Self {
        Self::with_event_limits(AttachmentEventLimits::default())
    }

    fn with_event_limits(limits: AttachmentEventLimits) -> Self {
        Self {
            state: Mutex::new(AttachmentState::default()),
            issue_lock: AsyncMutex::new(()),
            state_changed: Notify::new(),
            events: EventInbox::with_limits(limits),
            reader_abort: OnceLock::new(),
            writer_abort: OnceLock::new(),
        }
    }

    fn register_command(
        &self,
        kind: PendingKind,
        result_tx: oneshot::Sender<PendingResolution>,
    ) -> Result<AttachmentCommandId, ClientError> {
        let input_bytes = kind.input_bytes();
        let mut state = lock(&self.state);
        if state.end.is_some() {
            return Err(ClientError::AttachmentUnavailable {
                reason: AttachmentUnavailableReason::Closed,
            });
        }
        if let Some(reason) = state.fence {
            return Err(ClientError::AttachmentUnavailable { reason });
        }
        if self.events.observation_unavailable.load(Ordering::Acquire) {
            return Err(ClientError::AttachmentUnavailable {
                reason: AttachmentUnavailableReason::ObservationUnavailable,
            });
        }
        if state.pending.len() == MAX_PENDING_COMMANDS {
            return Err(ClientError::AttachmentBackpressure {
                limit: "64 unresolved commands",
            });
        }
        if matches!(kind, PendingKind::Input { .. })
            && state.pending_inputs == MAX_PENDING_INPUT_COMMANDS
        {
            return Err(ClientError::AttachmentBackpressure {
                limit: "32 unresolved input commands",
            });
        }
        if state
            .pending_input_bytes
            .checked_add(input_bytes)
            .is_none_or(|bytes| bytes > MAX_PENDING_INPUT_BYTES)
        {
            return Err(ClientError::AttachmentBackpressure {
                limit: "1 MiB unresolved input data",
            });
        }

        let command_id = AttachmentCommandId::new(state.next_command_id)
            .expect("attachment state never allocates command id zero");
        if state.next_command_id == u32::MAX {
            state.fence = Some(AttachmentUnavailableReason::CommandIdsExhausted);
        } else {
            state.next_command_id += 1;
        }
        state.pending_inputs += usize::from(matches!(kind, PendingKind::Input { .. }));
        state.pending_input_bytes += input_bytes;
        let replaced = state
            .pending
            .insert(command_id, PendingCommand { kind, result_tx });
        debug_assert!(replaced.is_none(), "command IDs are never reused");
        Ok(command_id)
    }

    fn resolve_command(
        &self,
        command_id: AttachmentCommandId,
        outcome: ControlOutcome,
    ) -> Result<(), &'static str> {
        let (pending, resolution) = {
            let mut state = lock(&self.state);
            let Some(pending) = state.pending.get(&command_id) else {
                return Err("command result names an unknown or completed ID");
            };
            let resolution = match outcome {
                ControlOutcome::Accepted { receipt } => {
                    PendingResolution::Accepted(pending.kind.validate(&receipt)?)
                }
                ControlOutcome::Rejected { failure } => {
                    validate_control_failure(&failure)?;
                    if let Some(confirmed) = failure.confirmed_input_bytes {
                        match pending.kind {
                            PendingKind::Input { expected_bytes }
                                if confirmed <= expected_bytes => {}
                            _ => {
                                return Err(
                                    "confirmed input prefix exceeds its original input request",
                                );
                            }
                        }
                    }
                    PendingResolution::Rejected(failure)
                }
            };
            let pending = state
                .pending
                .remove(&command_id)
                .expect("pending command was observed under the same lock");
            release_pending_capacity(&mut state, pending.kind);
            (pending, resolution)
        };
        let _ = pending.result_tx.send(resolution);
        self.state_changed.notify_one();
        Ok(())
    }

    fn cancel_unsent_command(&self, command_id: AttachmentCommandId) {
        let mut state = lock(&self.state);
        let Some(pending) = state.pending.remove(&command_id) else {
            return;
        };
        release_pending_capacity(&mut state, pending.kind);
        if command_id.get() == u32::MAX {
            if state.fence == Some(AttachmentUnavailableReason::CommandIdsExhausted) {
                state.fence = None;
            }
        } else {
            debug_assert_eq!(state.next_command_id, command_id.get() + 1);
            state.next_command_id = command_id.get();
        }
        drop(pending);
    }

    fn begin_detach(&self) -> Result<(), ClientError> {
        let mut state = lock(&self.state);
        if state.end.is_some() {
            return Err(ClientError::AttachmentUnavailable {
                reason: AttachmentUnavailableReason::Closed,
            });
        }
        state.fence = Some(AttachmentUnavailableReason::Detaching);
        state.detaching = true;
        Ok(())
    }

    async fn wait_until_pending_empty(&self) -> Result<(), ClientError> {
        loop {
            let changed = self.state_changed.notified();
            {
                let state = lock(&self.state);
                if state.pending.is_empty() {
                    return Ok(());
                }
                if state.end.is_some() {
                    return Err(ClientError::Closed);
                }
            }
            changed.await;
        }
    }

    async fn wait_until_detached(&self) -> Result<(), ClientError> {
        loop {
            let changed = self.state_changed.notified();
            match lock(&self.state).end {
                Some(AttachmentEnd::Detached) => return Ok(()),
                Some(AttachmentEnd::Terminated) => return Err(ClientError::Closed),
                None => {}
            }
            changed.await;
        }
    }

    fn finish_detach(&self) -> Result<(), &'static str> {
        {
            let mut state = lock(&self.state);
            if !state.detaching {
                return Err("daemon acknowledged a detach the client did not request");
            }
            if !state.pending.is_empty() {
                return Err("daemon acknowledged detach while commands were unresolved");
            }
            state.end = Some(AttachmentEnd::Detached);
            state.fence = Some(AttachmentUnavailableReason::Closed);
        }
        self.events.close(None);
        self.state_changed.notify_one();
        if let Some(writer) = self.writer_abort.get() {
            writer.abort();
        }
        Ok(())
    }

    fn terminate(&self, reason: AttachmentUnknownReason, event_error: Option<ClientError>) {
        let pending = {
            let mut state = lock(&self.state);
            if state.end.is_some() {
                return;
            }
            state.end = Some(AttachmentEnd::Terminated);
            state.fence = Some(AttachmentUnavailableReason::Closed);
            state.pending_inputs = 0;
            state.pending_input_bytes = 0;
            std::mem::take(&mut state.pending)
        };
        for (_, pending) in pending {
            let _ = pending.result_tx.send(PendingResolution::Unknown(reason));
        }
        self.events.close(event_error);
        self.state_changed.notify_one();
        if let Some(reader) = self.reader_abort.get() {
            reader.abort();
        }
        if let Some(writer) = self.writer_abort.get() {
            writer.abort();
        }
    }
}

fn release_pending_capacity(state: &mut AttachmentState, kind: PendingKind) {
    if matches!(kind, PendingKind::Input { .. }) {
        state.pending_inputs -= 1;
        state.pending_input_bytes -= kind.input_bytes();
    }
}

struct EventInbox {
    state: Mutex<EventInboxState>,
    ready: Notify,
    consumer_active: AtomicBool,
    limits: AttachmentEventLimits,
    observation_unavailable: AtomicBool,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum LocalObservationState {
    Available,
    LostMarkerPending,
    LostMarkerDelivered,
}

struct EventInboxState {
    queue: VecDeque<RunEvent>,
    queued_bytes: usize,
    envelope_payload_bytes: usize,
    pending_gap: Option<(u64, OutputGapCauses)>,
    local_observation: LocalObservationState,
    terminal: Option<RunEvent>,
    saw_terminal: bool,
    saw_observation_discontinuity: bool,
    closed: bool,
    error: Option<ClientError>,
}

impl EventInbox {
    #[cfg(test)]
    fn new() -> Self {
        Self::with_limits(AttachmentEventLimits::default())
    }

    fn with_limits(limits: AttachmentEventLimits) -> Self {
        Self {
            state: Mutex::new(EventInboxState {
                queue: VecDeque::new(),
                queued_bytes: 0,
                envelope_payload_bytes: 0,
                pending_gap: None,
                local_observation: LocalObservationState::Available,
                terminal: None,
                saw_terminal: false,
                saw_observation_discontinuity: false,
                closed: false,
                error: None,
            }),
            ready: Notify::new(),
            consumer_active: AtomicBool::new(false),
            limits,
            observation_unavailable: AtomicBool::new(false),
        }
    }

    fn mark_observation_loss(&self, state: &mut EventInboxState) {
        state.local_observation = LocalObservationState::LostMarkerPending;
        state.closed = true;
        state.error = Some(ClientError::AttachmentObservationUnavailable {
            payload_bytes: self.limits.payload_bytes,
            envelope_bytes: self.limits.envelope_bytes,
        });
        self.observation_unavailable.store(true, Ordering::Release);
    }

    fn reserve_slots(
        &self,
        state: &mut EventInboxState,
        additional: usize,
        payload: usize,
    ) -> bool {
        let Some(required) = state.queue.len().checked_add(additional) else {
            return false;
        };
        let Some(extra) = state.envelope_payload_bytes.checked_add(payload) else {
            return false;
        };
        let Some(available) = self
            .limits
            .envelope_bytes
            .checked_sub(std::mem::size_of::<Self>())
            .and_then(|n| n.checked_sub(extra))
        else {
            return false;
        };
        let maximum = available / std::mem::size_of::<RunEvent>();
        if required > maximum || state.queue.capacity() > maximum {
            return false;
        }
        if required > state.queue.capacity() {
            // Fund amortized requested backing before allocation; when doubling
            // cannot fit, fund the exact necessary capacity rather than refuse
            // an otherwise legitimate event. Allocator overhead is measured RSS.
            let target = state
                .queue
                .capacity()
                .saturating_mul(2)
                .max(required)
                .min(maximum);
            if state
                .queue
                .try_reserve_exact(target - state.queue.len())
                .is_err()
            {
                return false;
            }
            if state.queue.capacity() > maximum {
                return false;
            }
        }
        true
    }

    fn replace_input_progress(state: &mut EventInboxState, event: &RunEvent) -> bool {
        if state.pending_gap.is_some() {
            return false;
        }
        let RunEvent::ServiceChanged { service: next } = event else {
            return false;
        };
        let Some(RunEvent::ServiceChanged { service: previous }) = state.queue.back_mut() else {
            return false;
        };
        // These are current-state snapshots, not a log of each write quantum.
        // Replace only an unconsumed confirmed-prefix advance for the same
        // pending command. Keep every availability, blocking, geometry, fault,
        // settlement and intervening event boundary in its original order.
        if next.revision <= previous.revision
            || next.owner != previous.owner
            || next.output != previous.output
            || next.terminal_fault != previous.terminal_fault
            || next.input.phase != previous.input.phase
            || next.input.unsettled_commands == 0
            || next.input.unsettled_commands != previous.input.unsettled_commands
            || next.input.unsettled_request_bytes != previous.input.unsettled_request_bytes
            || next.input.write_blocked != previous.input.write_blocked
            || next.input.completed_input_bytes != previous.input.completed_input_bytes
            || next.input.current_size != previous.input.current_size
            || next.input.active_confirmed_bytes <= previous.input.active_confirmed_bytes
        {
            return false;
        }
        // NativeServiceSnapshot has no variable-size allocations; replacement
        // uses the already funded slot and does not change either byte window.
        *previous = next.clone();
        true
    }

    fn push(&self, event: RunEvent) -> Result<(), &'static str> {
        let mut state = lock(&self.state);
        if state.closed && state.local_observation == LocalObservationState::Available {
            return Err("daemon sent an event after attachment termination");
        }
        if state.saw_terminal {
            return Err("daemon sent an event after terminal lifecycle");
        }
        if state.saw_observation_discontinuity
            && !matches!(
                &event,
                RunEvent::Exited { .. } | RunEvent::Interrupted { .. }
            )
        {
            return Err("daemon sent a non-terminal event after observation discontinuity");
        }

        if state.local_observation != LocalObservationState::Available {
            // Keep validating lifecycle even when this view cannot retain its
            // observations. Local pressure never makes malformed wire valid.
            match event {
                RunEvent::Exited { .. } | RunEvent::Interrupted { .. } => state.saw_terminal = true,
                RunEvent::ObservationDiscontinuity => state.saw_observation_discontinuity = true,
                _ => {}
            }
            return Ok(());
        }

        if Self::replace_input_progress(&mut state, &event) {
            drop(state);
            self.ready.notify_one();
            return Ok(());
        }

        match event {
            RunEvent::Output { chunk } => {
                let latest_output_bytes = chunk.end_byte;
                let event = RunEvent::Output { chunk };
                let bytes = event_bytes(&event);
                let required_slots = 1 + usize::from(state.pending_gap.is_some());
                if state
                    .queued_bytes
                    .checked_add(bytes)
                    .is_none_or(|total| total > self.limits.payload_bytes)
                    || !self.reserve_slots(
                        &mut state,
                        required_slots,
                        event_envelope_payload_bytes(&event),
                    )
                {
                    let (head, causes) = state.pending_gap.unwrap_or((0, OutputGapCauses::NONE));
                    state.pending_gap = Some((
                        head.max(latest_output_bytes),
                        causes.union(OutputGapCauses::CLIENT_VIEW_PRESSURE),
                    ));
                } else {
                    Self::retain_pending_gap(&mut state);
                    state.queued_bytes += bytes;
                    state.envelope_payload_bytes += event_envelope_payload_bytes(&event);
                    state.queue.push_back(event);
                }
            }
            RunEvent::Gap {
                latest_output_bytes,
                causes,
            } => {
                let (head, previous) = state.pending_gap.unwrap_or((0, OutputGapCauses::NONE));
                state.pending_gap = Some((head.max(latest_output_bytes), previous.union(causes)));
            }
            terminal @ (RunEvent::Exited { .. } | RunEvent::Interrupted { .. }) => {
                self.retain_terminal(&mut state, terminal);
            }
            event @ (RunEvent::ServiceChanged { .. }
            | RunEvent::Tmux { .. }
            | RunEvent::ObservationDiscontinuity
            | RunEvent::Resized { .. }) => {
                if matches!(&event, RunEvent::ObservationDiscontinuity) {
                    state.saw_observation_discontinuity = true;
                }
                let bytes = event_bytes(&event);
                let required_slots = 1 + usize::from(state.pending_gap.is_some());
                if state
                    .queued_bytes
                    .checked_add(bytes)
                    .is_none_or(|total| total > self.limits.payload_bytes)
                    || !self.reserve_slots(
                        &mut state,
                        required_slots,
                        event_envelope_payload_bytes(&event),
                    )
                {
                    self.mark_observation_loss(&mut state);
                    drop(state);
                    self.ready.notify_one();
                    return Ok(());
                }
                Self::retain_pending_gap(&mut state);
                state.queued_bytes += bytes;
                state.envelope_payload_bytes += event_envelope_payload_bytes(&event);
                state.queue.push_back(event);
            }
        }
        drop(state);
        self.ready.notify_one();
        Ok(())
    }

    fn retain_pending_gap(state: &mut EventInboxState) {
        if let Some((latest_output_bytes, causes)) = state.pending_gap.take() {
            state.queue.push_back(RunEvent::Gap {
                latest_output_bytes,
                causes,
            });
        }
    }

    fn retain_terminal(&self, state: &mut EventInboxState, terminal: RunEvent) {
        state.saw_terminal = true;
        let envelope = event_envelope_payload_bytes(&terminal);
        if state.pending_gap.is_none()
            && state.terminal.is_none()
            && self.reserve_slots(state, 1, envelope)
        {
            state.envelope_payload_bytes += envelope;
            state.queue.push_back(terminal);
        } else {
            let backing = state.queue.capacity() * std::mem::size_of::<RunEvent>();
            let funded = std::mem::size_of::<Self>()
                .checked_add(backing)
                .and_then(|n| n.checked_add(state.envelope_payload_bytes))
                .and_then(|n| n.checked_add(envelope))
                .is_some_and(|n| n <= self.limits.envelope_bytes);
            if funded {
                state.envelope_payload_bytes += envelope;
                state.terminal = Some(terminal);
            } else {
                self.mark_observation_loss(state);
            }
        }
    }

    fn accepts_clean_eof(&self) -> bool {
        let state = lock(&self.state);
        state.saw_terminal || state.saw_observation_discontinuity
    }

    fn close(&self, error: Option<ClientError>) {
        let mut state = lock(&self.state);
        if state.closed {
            return;
        }
        state.closed = true;
        state.error = error;
        drop(state);
        self.ready.notify_one();
    }

    async fn next(&self) -> Result<Option<RunEvent>, ClientError> {
        let _consumer = EventConsumerGuard::acquire(&self.consumer_active)?;
        loop {
            // `notify_one` retains one permit. Creating the future before the
            // state check also protects the check/await boundary.
            let ready = self.ready.notified();
            {
                let mut state = lock(&self.state);
                if let Some(event) = state.queue.pop_front() {
                    state.queued_bytes -= event_bytes(&event);
                    state.envelope_payload_bytes -= event_envelope_payload_bytes(&event);
                    return Ok(Some(event));
                }
                if let Some((latest_output_bytes, causes)) = state.pending_gap.take() {
                    return Ok(Some(RunEvent::Gap {
                        latest_output_bytes,
                        causes,
                    }));
                }
                if state.local_observation == LocalObservationState::LostMarkerPending {
                    state.local_observation = LocalObservationState::LostMarkerDelivered;
                    return Ok(Some(RunEvent::ObservationDiscontinuity));
                }
                if let Some(event) = state.terminal.take() {
                    state.envelope_payload_bytes -= event_envelope_payload_bytes(&event);
                    return Ok(Some(event));
                }
                if let Some(error) = state.error.take() {
                    return Err(error);
                }
                if state.closed {
                    return Ok(None);
                }
            }
            ready.await;
        }
    }
}

struct EventConsumerGuard<'a>(&'a AtomicBool);

impl<'a> EventConsumerGuard<'a> {
    fn acquire(active: &'a AtomicBool) -> Result<Self, ClientError> {
        active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| Self(active))
            .map_err(|_| ClientError::ConcurrentEventRead)
    }
}

impl Drop for EventConsumerGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

fn event_envelope_payload_bytes(event: &RunEvent) -> usize {
    match event {
        RunEvent::Output { chunk } => chunk.data.capacity() - chunk.data.len(),
        RunEvent::Tmux {
            event: ctxmux_protocol::TmuxRunEvent::SessionRenamed { name },
        } => name.capacity() - name.len(),
        RunEvent::Exited {
            state:
                ctxmux_protocol::RunState::Exited {
                    signal: Some(signal),
                    ..
                },
        } => signal.capacity(),
        _ => 0,
    }
}

fn event_bytes(event: &RunEvent) -> usize {
    match event {
        RunEvent::Output { chunk } => chunk.data.len(),
        RunEvent::Tmux {
            event: ctxmux_protocol::TmuxRunEvent::SessionRenamed { name },
        } => name.len(),
        RunEvent::Exited { .. }
        | RunEvent::Interrupted { .. }
        | RunEvent::Tmux { .. }
        | RunEvent::ServiceChanged { .. }
        | RunEvent::ObservationDiscontinuity
        | RunEvent::Resized { .. }
        | RunEvent::Gap { .. } => 0,
    }
}

async fn writer_loop(
    mut sink: WireSink,
    mut commands: mpsc::Receiver<WriterCommand>,
    shared: Arc<AttachmentShared>,
) {
    while let Some(command) = commands.recv().await {
        let result = match command {
            WriterCommand::Control(encoded) => send_encoded_sink(&mut sink, encoded).await,
            WriterCommand::Detach => send_sink(&mut sink, &ClientFrame::Detach).await,
        };
        if let Err(error) = result {
            shared.terminate(AttachmentUnknownReason::TransportTerminated, Some(error));
            return;
        }
    }
}

async fn reader_loop(mut stream: WireStream, shared: Arc<AttachmentShared>) {
    loop {
        let frame = match stream.next().await {
            Some(Ok(line)) => match decode_frame(&line) {
                Ok(frame) => frame,
                Err(error) => {
                    shared.terminate(
                        AttachmentUnknownReason::ProtocolViolation,
                        Some(ClientError::Frame(error)),
                    );
                    return;
                }
            },
            Some(Err(error)) => {
                shared.terminate(
                    AttachmentUnknownReason::TransportTerminated,
                    Some(ClientError::Transport(error)),
                );
                return;
            }
            None => {
                let event_error =
                    (!shared.events.accepts_clean_eof()).then_some(ClientError::Closed);
                shared.terminate(AttachmentUnknownReason::TransportTerminated, event_error);
                return;
            }
        };

        let detached = matches!(frame, ServerFrame::Detached);
        let result = match frame {
            ServerFrame::Event { event } => shared.events.push(event),
            ServerFrame::CommandResult {
                command_id,
                outcome,
            } => shared.resolve_command(command_id, outcome),
            ServerFrame::Detached => shared.finish_detach(),
            ServerFrame::Error { error } => {
                shared.terminate(
                    AttachmentUnknownReason::ProtocolViolation,
                    Some(ClientError::from(error)),
                );
                return;
            }
            ServerFrame::ReplayWindow { .. }
            | ServerFrame::Hello { .. }
            | ServerFrame::Response { .. }
            | ServerFrame::Attached { .. }
            | ServerFrame::TerminalCheckpointChunk { .. } => {
                Err("daemon sent a non-attachment frame after attach")
            }
        };

        if let Err(message) = result {
            shared.terminate(
                AttachmentUnknownReason::ProtocolViolation,
                Some(ClientError::ProtocolContractViolation(message)),
            );
            return;
        }
        if detached {
            return;
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use ctxmux_protocol::{CommandDisposition, OutputChunk, TmuxRunEvent};
    use futures_util::SinkExt;
    use tokio::{net::UnixStream, time::timeout};
    use tokio_util::codec::{Framed, LinesCodec};

    #[test]
    fn public_control_errors_expose_retry_safety_disposition() {
        let short_unknown = super::super::control_request_unknown(ClientError::Closed);
        assert_eq!(
            short_unknown.control_disposition(),
            Some(CommandDisposition::Unknown)
        );
        assert_eq!(
            ClientError::AttachmentCommandUnknown {
                command_id: AttachmentCommandId::new(1).unwrap(),
                reason: AttachmentUnknownReason::TransportTerminated,
            }
            .control_disposition(),
            Some(CommandDisposition::Unknown)
        );
        assert_eq!(
            ClientError::AttachmentUnavailable {
                reason: AttachmentUnavailableReason::Detaching,
            }
            .control_disposition(),
            Some(CommandDisposition::NotApplied)
        );
    }

    #[test]
    fn attachment_admission_reserves_control_capacity_and_input_bytes() {
        let shared = AttachmentShared::new();
        let mut input_receivers = Vec::new();
        for expected_id in 1..=MAX_PENDING_INPUT_COMMANDS {
            let (result_tx, result_rx) = oneshot::channel();
            let command_id = shared
                .register_command(PendingKind::Input { expected_bytes: 1 }, result_tx)
                .expect("admit bounded input");
            assert_eq!(command_id.get() as usize, expected_id);
            input_receivers.push(result_rx);
        }
        let (result_tx, _) = oneshot::channel();
        assert!(matches!(
            shared.register_command(PendingKind::Input { expected_bytes: 1 }, result_tx),
            Err(ClientError::AttachmentBackpressure {
                limit: "32 unresolved input commands"
            })
        ));

        let mut control_receivers = Vec::new();
        for _ in MAX_PENDING_INPUT_COMMANDS..MAX_PENDING_COMMANDS {
            let (result_tx, result_rx) = oneshot::channel();
            shared
                .register_command(PendingKind::Resize, result_tx)
                .expect("reserved control capacity remains available");
            control_receivers.push(result_rx);
        }
        let (result_tx, _) = oneshot::channel();
        assert!(matches!(
            shared.register_command(PendingKind::Stop, result_tx),
            Err(ClientError::AttachmentBackpressure {
                limit: "64 unresolved commands"
            })
        ));

        let first = AttachmentCommandId::new(1).unwrap();
        shared
            .resolve_command(
                first,
                ControlOutcome::Accepted {
                    receipt: ControlReceipt::Input { written_bytes: 1 },
                },
            )
            .expect("resolve first input");
        let (result_tx, _) = oneshot::channel();
        assert_eq!(
            shared
                .register_command(PendingKind::Stop, result_tx)
                .expect("one completed command restores total capacity")
                .get(),
            65
        );
        drop(input_receivers);
        drop(control_receivers);

        let byte_bounded = AttachmentShared::new();
        let (result_tx, _) = oneshot::channel();
        byte_bounded
            .register_command(
                PendingKind::Input {
                    expected_bytes: MAX_PENDING_INPUT_BYTES,
                },
                result_tx,
            )
            .expect("admit exactly 1 MiB of unresolved input");
        let (result_tx, _) = oneshot::channel();
        assert!(matches!(
            byte_bounded.register_command(PendingKind::Input { expected_bytes: 1 }, result_tx),
            Err(ClientError::AttachmentBackpressure {
                limit: "1 MiB unresolved input data"
            })
        ));
    }

    #[test]
    fn invalid_receipt_keeps_implicated_command_pending_until_unknown() {
        let shared = AttachmentShared::new();
        let (result_tx, result_rx) = oneshot::channel();
        let command_id = shared
            .register_command(PendingKind::Input { expected_bytes: 3 }, result_tx)
            .expect("register command");
        assert_eq!(
            shared.resolve_command(
                command_id,
                ControlOutcome::Accepted {
                    receipt: ControlReceipt::Input { written_bytes: 2 },
                }
            ),
            Err("attachment input receipt does not match its command")
        );

        shared.terminate(AttachmentUnknownReason::ProtocolViolation, None);
        assert!(matches!(
            result_rx.blocking_recv(),
            Ok(PendingResolution::Unknown(
                AttachmentUnknownReason::ProtocolViolation
            ))
        ));
    }

    #[test]
    fn invalid_backpressure_disposition_is_attachment_fatal() {
        let shared = AttachmentShared::new();
        let (result_tx, result_rx) = oneshot::channel();
        let command_id = shared
            .register_command(PendingKind::Stop, result_tx)
            .expect("register stop");
        assert_eq!(
            shared.resolve_command(
                command_id,
                ControlOutcome::Rejected {
                    failure: ControlFailure {
                        confirmed_input_bytes: None,
                        error: ctxmux_protocol::ProtocolError::new(
                            ctxmux_protocol::ErrorCode::ControlBackpressure,
                            "invalid fixture",
                        ),
                        disposition: CommandDisposition::Unknown,
                    },
                }
            ),
            Err("control_backpressure must have not_applied disposition")
        );
        shared.terminate(AttachmentUnknownReason::ProtocolViolation, None);
        assert!(matches!(
            result_rx.blocking_recv(),
            Ok(PendingResolution::Unknown(
                AttachmentUnknownReason::ProtocolViolation
            ))
        ));
    }

    const HISTORICAL_EVENT_COUNT: usize = 256;

    fn input_progress(prefix: usize) -> ctxmux_protocol::NativeServiceSnapshot {
        use ctxmux_protocol::{
            NativeInputPhase, NativeInputStatus, NativeOutputStatus, NativeOwnerStatus,
        };
        ctxmux_protocol::NativeServiceSnapshot {
            revision: u64::try_from(prefix).unwrap() + 1,
            owner: NativeOwnerStatus::Serving {},
            output: NativeOutputStatus::Serving {},
            input: NativeInputStatus {
                phase: NativeInputPhase::Open {},
                unsettled_commands: 1,
                unsettled_request_bytes: MAX_PENDING_INPUT_BYTES,
                write_blocked: false,
                completed_input_bytes: Some(0),
                current_size: Some(TerminalSize::default()),
                active_confirmed_bytes: prefix,
            },
            terminal_fault: None,
        }
    }

    #[tokio::test]
    async fn paused_view_retains_latest_input_prefix_beyond_its_slot_window() {
        let inbox = EventInbox::new();
        // Qualification exceeds the actual default slot window; it is not a
        // new production event count or a smaller accepted input workload.
        let updates = AttachmentEventLimits::default().envelope_bytes
            / std::mem::size_of::<RunEvent>()
            + HISTORICAL_EVENT_COUNT;
        for prefix in 0..=updates {
            inbox
                .push(RunEvent::ServiceChanged {
                    service: input_progress(prefix),
                })
                .unwrap();
        }
        inbox.close(None);
        assert_eq!(
            inbox.next().await.unwrap(),
            Some(RunEvent::ServiceChanged {
                service: input_progress(updates)
            })
        );
        assert_eq!(inbox.next().await.unwrap(), None);
    }

    #[tokio::test]
    async fn input_progress_keeps_intervening_output_gap_geometry_and_tmux_order() {
        let barriers = [
            RunEvent::Output {
                chunk: OutputChunk {
                    start_byte: 0,
                    end_byte: 1,
                    data: vec![42],
                },
            },
            RunEvent::Gap {
                latest_output_bytes: 1,
                causes: OutputGapCauses::SOURCE_DISCONTINUITY,
            },
            RunEvent::Resized {
                size: TerminalSize { cols: 81, rows: 25 },
                through_byte: 0,
                resize_revision: 1,
            },
            RunEvent::Tmux {
                event: TmuxRunEvent::Paused,
            },
        ];
        for barrier in barriers {
            let inbox = EventInbox::new();
            let first = RunEvent::ServiceChanged {
                service: input_progress(1),
            };
            let next = RunEvent::ServiceChanged {
                service: input_progress(2),
            };
            for event in [first.clone(), barrier.clone(), next.clone()] {
                inbox.push(event).unwrap();
            }
            inbox.close(None);
            for event in [first, barrier, next] {
                assert_eq!(inbox.next().await.unwrap(), Some(event));
            }
            assert_eq!(inbox.next().await.unwrap(), None);
        }
    }

    #[tokio::test]
    async fn input_progress_never_erases_service_transitions_or_backward_facts() {
        use ctxmux_protocol::{
            NativeInputPhase, NativeOutputStatus, NativeOwnerStatus, NativeTerminalFault,
            NativeTerminalFaultStage,
        };
        let changes: &[fn(&mut ctxmux_protocol::NativeServiceSnapshot)] = &[
            |s| s.owner = NativeOwnerStatus::Draining {},
            |s| s.output = NativeOutputStatus::Backpressured {},
            |s| s.input.phase = NativeInputPhase::Closed {},
            |s| s.input.write_blocked = true,
            |s| s.input.unsettled_commands = 0,
            |s| s.input.unsettled_request_bytes -= 1,
            |s| s.input.completed_input_bytes = Some(1),
            |s| s.input.current_size = Some(TerminalSize { cols: 81, rows: 25 }),
            |s| {
                s.terminal_fault = Some(NativeTerminalFault {
                    stage: NativeTerminalFaultStage::Process,
                    through_byte: 0,
                });
            },
            |s| s.input.active_confirmed_bytes = 0,
            |s| s.revision = 1,
        ];
        for change in changes {
            let inbox = EventInbox::new();
            let first = RunEvent::ServiceChanged {
                service: input_progress(1),
            };
            let mut service = input_progress(2);
            change(&mut service);
            let next = RunEvent::ServiceChanged { service };
            inbox.push(first.clone()).unwrap();
            inbox.push(next.clone()).unwrap();
            inbox.close(None);
            assert_eq!(inbox.next().await.unwrap(), Some(first));
            assert_eq!(inbox.next().await.unwrap(), Some(next));
            assert_eq!(inbox.next().await.unwrap(), None);
        }
    }

    #[tokio::test]
    async fn gap_causes_merge_without_discarding_an_admissible_later_output() {
        for gap_first in [false, true] {
            let inbox = EventInbox::with_limits(AttachmentEventLimits {
                payload_bytes: 1,
                ..AttachmentEventLimits::default()
            });
            let source = RunEvent::Gap {
                latest_output_bytes: 1,
                causes: OutputGapCauses::SOURCE_DISCONTINUITY,
            };
            if gap_first {
                inbox.push(source.clone()).unwrap();
            }
            inbox
                .push(RunEvent::Output {
                    chunk: OutputChunk {
                        start_byte: 0,
                        end_byte: 2,
                        data: vec![0, 255],
                    },
                })
                .unwrap();
            if !gap_first {
                inbox.push(source).unwrap();
            }
            assert_eq!(
                inbox.next().await.unwrap(),
                Some(RunEvent::Gap {
                    latest_output_bytes: 2,
                    causes: OutputGapCauses::SOURCE_DISCONTINUITY
                        .union(OutputGapCauses::CLIENT_VIEW_PRESSURE),
                })
            );
        }
        let inbox = EventInbox::new();
        inbox
            .push(RunEvent::Gap {
                latest_output_bytes: 2,
                causes: OutputGapCauses::SOURCE_DISCONTINUITY,
            })
            .unwrap();
        let later = RunEvent::Output {
            chunk: OutputChunk {
                start_byte: 2,
                end_byte: 3,
                data: vec![255],
            },
        };
        inbox.push(later.clone()).unwrap();
        assert_eq!(
            inbox.next().await.unwrap(),
            Some(RunEvent::Gap {
                latest_output_bytes: 2,
                causes: OutputGapCauses::SOURCE_DISCONTINUITY,
            })
        );
        assert_eq!(inbox.next().await.unwrap(), Some(later));
    }

    #[tokio::test]
    async fn output_overflow_gap_precedes_later_tmux_event() {
        let inbox = EventInbox::new();
        inbox
            .push(RunEvent::Output {
                chunk: OutputChunk {
                    start_byte: 0,
                    end_byte: MAX_QUEUED_EVENT_BYTES as u64,
                    data: vec![0; MAX_QUEUED_EVENT_BYTES],
                },
            })
            .expect("fill bounded byte inbox");
        inbox
            .push(RunEvent::Output {
                chunk: OutputChunk {
                    start_byte: MAX_QUEUED_EVENT_BYTES as u64,
                    end_byte: MAX_QUEUED_EVENT_BYTES as u64 + 1,
                    data: vec![1],
                },
            })
            .expect("coalesce overflowing output into a Gap");
        assert!(matches!(
            inbox.next().await.unwrap(),
            Some(RunEvent::Output {
                chunk: OutputChunk { start_byte: 0, .. }
            })
        ));

        inbox
            .push(RunEvent::Tmux {
                event: TmuxRunEvent::Paused,
            })
            .expect("materialize Gap before a later non-output event");
        assert_eq!(
            inbox.next().await.unwrap(),
            Some(RunEvent::Gap {
                latest_output_bytes: MAX_QUEUED_EVENT_BYTES as u64 + 1,
                causes: OutputGapCauses::CLIENT_VIEW_PRESSURE,
            })
        );
        assert_eq!(
            inbox.next().await.unwrap(),
            Some(RunEvent::Tmux {
                event: TmuxRunEvent::Paused
            })
        );
    }

    #[tokio::test]
    async fn daemon_output_gaps_coalesce_but_observation_loss_fails_closed() {
        let gaps = EventInbox::new();
        gaps.push(RunEvent::Gap {
            latest_output_bytes: 1,
            causes: ctxmux_protocol::OutputGapCauses::UNKNOWN,
        })
        .expect("accept first output Gap");
        gaps.push(RunEvent::Gap {
            latest_output_bytes: 7,
            causes: ctxmux_protocol::OutputGapCauses::UNKNOWN,
        })
        .expect("coalesce later output Gap");
        assert_eq!(
            gaps.next().await.unwrap(),
            Some(RunEvent::Gap {
                latest_output_bytes: 7,
                causes: ctxmux_protocol::OutputGapCauses::UNKNOWN,
            })
        );

        let observations = EventInbox::new();
        for _ in 0..=HISTORICAL_EVENT_COUNT {
            observations
                .push(RunEvent::Tmux {
                    event: TmuxRunEvent::Paused,
                })
                .expect("the historical population ceiling is not a capacity contract");
        }
        for _ in 0..=HISTORICAL_EVENT_COUNT {
            assert_eq!(
                observations.next().await.unwrap(),
                Some(RunEvent::Tmux {
                    event: TmuxRunEvent::Paused
                })
            );
        }

        let pressured = EventInbox::with_limits(AttachmentEventLimits {
            envelope_bytes: std::mem::size_of::<EventInbox>()
                + HISTORICAL_EVENT_COUNT * std::mem::size_of::<RunEvent>(),
            ..AttachmentEventLimits::default()
        });
        for _ in 0..=HISTORICAL_EVENT_COUNT {
            pressured
                .push(RunEvent::Tmux {
                    event: TmuxRunEvent::Paused,
                })
                .unwrap();
        }
        assert!(pressured.observation_unavailable.load(Ordering::Acquire));
        for _ in 0..HISTORICAL_EVENT_COUNT {
            assert_eq!(
                pressured.next().await.unwrap(),
                Some(RunEvent::Tmux {
                    event: TmuxRunEvent::Paused
                })
            );
        }
        assert_eq!(
            pressured.next().await.unwrap(),
            Some(RunEvent::ObservationDiscontinuity)
        );
        assert!(matches!(
            pressured.next().await,
            Err(ClientError::AttachmentObservationUnavailable { .. })
        ));
        pressured
            .push(RunEvent::Exited {
                state: ctxmux_protocol::RunState::Exited {
                    code: 0,
                    signal: None,
                },
            })
            .unwrap();
        assert!(pressured.accepts_clean_eof());
        assert_eq!(
            pressured.push(RunEvent::Tmux {
                event: TmuxRunEvent::Paused
            }),
            Err("daemon sent an event after terminal lifecycle")
        );
    }

    #[tokio::test]
    async fn terminal_event_survives_full_output_inbox() {
        let inbox = EventInbox::with_limits(AttachmentEventLimits {
            envelope_bytes: std::mem::size_of::<EventInbox>()
                + HISTORICAL_EVENT_COUNT * std::mem::size_of::<RunEvent>(),
            ..AttachmentEventLimits::default()
        });
        for start_byte in 0..HISTORICAL_EVENT_COUNT as u64 {
            inbox
                .push(RunEvent::Output {
                    chunk: OutputChunk {
                        start_byte,
                        end_byte: start_byte + 1,
                        data: vec![b'x'],
                    },
                })
                .expect("fill explicitly configured backing budget");
        }
        inbox
            .push(RunEvent::Output {
                chunk: OutputChunk {
                    start_byte: HISTORICAL_EVENT_COUNT as u64,
                    end_byte: HISTORICAL_EVENT_COUNT as u64 + 1,
                    data: vec![b'y'],
                },
            })
            .expect("coalesce overflowing output");
        let exited = RunEvent::Exited {
            state: ctxmux_protocol::RunState::Exited {
                code: 0,
                signal: None,
            },
        };
        inbox
            .push(exited.clone())
            .expect("retain terminal event outside the full queue");

        for _ in 0..HISTORICAL_EVENT_COUNT {
            assert!(matches!(
                inbox.next().await.unwrap(),
                Some(RunEvent::Output { .. })
            ));
        }
        assert_eq!(
            inbox.next().await.unwrap(),
            Some(RunEvent::Gap {
                latest_output_bytes: HISTORICAL_EVENT_COUNT as u64 + 1,
                causes: OutputGapCauses::CLIENT_VIEW_PRESSURE,
            })
        );
        assert_eq!(inbox.next().await.unwrap(), Some(exited));
    }

    #[tokio::test]
    async fn event_inbox_rejects_a_second_pending_consumer_and_closes_the_first() {
        let inbox = Arc::new(EventInbox::new());
        let first_inbox = Arc::clone(&inbox);
        let first = tokio::spawn(async move { first_inbox.next().await });
        while !inbox.consumer_active.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }

        assert!(matches!(
            inbox.next().await,
            Err(ClientError::ConcurrentEventRead)
        ));
        inbox.close(None);
        assert!(matches!(first.await, Ok(Ok(None))));
    }

    async fn serve_pressure_input(mut server: Wire, invalid_id: bool) {
        let line = server.next().await.unwrap().unwrap();
        let ClientFrame::Input { command_id, data } = decode_frame::<ClientFrame>(&line).unwrap()
        else {
            panic!("original command must be Input");
        };
        assert_eq!(data, b"abc");
        for _ in 0..2 {
            server
                .send(
                    encode_frame(&ServerFrame::Event {
                        event: RunEvent::Tmux {
                            event: TmuxRunEvent::Paused,
                        },
                    })
                    .unwrap(),
                )
                .await
                .unwrap();
        }
        server
            .send(
                encode_frame(&ServerFrame::CommandResult {
                    command_id: if invalid_id {
                        AttachmentCommandId::new(command_id.get() + 1).unwrap()
                    } else {
                        command_id
                    },
                    outcome: ControlOutcome::Accepted {
                        receipt: ControlReceipt::Input { written_bytes: 3 },
                    },
                })
                .unwrap(),
            )
            .await
            .unwrap();
        if !invalid_id {
            let line = server.next().await.unwrap().unwrap();
            assert!(matches!(
                decode_frame::<ClientFrame>(&line).unwrap(),
                ClientFrame::Detach
            ));
            server
                .send(encode_frame(&ServerFrame::Detached).unwrap())
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn view_pressure_keeps_sent_input_result_and_detach_owner() {
        for invalid_id in [false, true] {
            let (client_stream, server_stream) = UnixStream::pair().unwrap();
            let limits = AttachmentEventLimits {
                envelope_bytes: std::mem::size_of::<EventInbox>() + std::mem::size_of::<RunEvent>(),
                ..AttachmentEventLimits::default()
            };
            let attachment = Attachment::from_wire_with_event_limits(
                Framed::new(
                    client_stream,
                    LinesCodec::new_with_max_length(ctxmux_protocol::MAX_FRAME_BYTES),
                ),
                limits,
            );
            let server = Framed::new(
                server_stream,
                LinesCodec::new_with_max_length(ctxmux_protocol::MAX_FRAME_BYTES),
            );
            let server_task = tokio::spawn(serve_pressure_input(server, invalid_id));
            let result = timeout(Duration::from_secs(1), attachment.input(b"abc".to_vec()))
                .await
                .unwrap();
            if invalid_id {
                assert!(matches!(
                    result,
                    Err(ClientError::AttachmentCommandUnknown {
                        reason: AttachmentUnknownReason::ProtocolViolation,
                        ..
                    })
                ));
            } else {
                let accepted =
                    result.expect("view pressure must not destroy a validated sent-input ACK");
                assert_eq!(accepted.command_id.get(), 1);
                assert_eq!(accepted.receipt.written_bytes, 3);
                assert_eq!(
                    attachment.next_event().await.unwrap(),
                    Some(RunEvent::Tmux {
                        event: TmuxRunEvent::Paused
                    })
                );
                assert_eq!(
                    attachment.next_event().await.unwrap(),
                    Some(RunEvent::ObservationDiscontinuity)
                );
                assert!(matches!(
                    attachment.next_event().await,
                    Err(ClientError::AttachmentObservationUnavailable { .. })
                ));
                let rejected = attachment
                    .resize(TerminalSize { rows: 30, cols: 90 })
                    .await
                    .unwrap_err();
                assert!(matches!(
                    rejected,
                    ClientError::AttachmentUnavailable {
                        reason: AttachmentUnavailableReason::ObservationUnavailable
                    }
                ));
                assert_eq!(
                    rejected.control_disposition(),
                    Some(CommandDisposition::NotApplied)
                );
                timeout(Duration::from_secs(1), attachment.detach())
                    .await
                    .unwrap()
                    .unwrap();
            }
            server_task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn unfunded_terminal_signal_exposes_view_loss_without_retaining_heap() {
        let inbox = EventInbox::with_limits(AttachmentEventLimits {
            envelope_bytes: std::mem::size_of::<EventInbox>(),
            ..AttachmentEventLimits::default()
        });
        inbox
            .push(RunEvent::Exited {
                state: ctxmux_protocol::RunState::Exited {
                    code: 1,
                    signal: Some(String::from("SIGTERM")),
                },
            })
            .unwrap();
        assert!(inbox.accepts_clean_eof());
        assert_eq!(lock(&inbox.state).envelope_payload_bytes, 0);
        assert!(lock(&inbox.state).terminal.is_none());
        assert_eq!(
            inbox.next().await.unwrap(),
            Some(RunEvent::ObservationDiscontinuity)
        );
        assert!(matches!(
            inbox.next().await,
            Err(ClientError::AttachmentObservationUnavailable { .. })
        ));
    }

    #[tokio::test]
    async fn oversized_encoded_input_is_not_applied_and_does_not_consume_its_id() {
        let (client_stream, server_stream) = UnixStream::pair().expect("create attachment pair");
        let attachment = Attachment::from_wire(Framed::new(
            client_stream,
            LinesCodec::new_with_max_length(ctxmux_protocol::MAX_FRAME_BYTES),
        ));
        let mut server = Framed::new(
            server_stream,
            LinesCodec::new_with_max_length(ctxmux_protocol::MAX_FRAME_BYTES),
        );

        let error = attachment
            .input(vec![u8::MAX; 300 * 1024])
            .await
            .expect_err("expanded JSON exceeds the exact frame ceiling");
        assert_eq!(
            error.control_disposition(),
            Some(CommandDisposition::NotApplied)
        );

        let server_task = tokio::spawn(async move {
            let line = timeout(Duration::from_secs(1), server.next())
                .await
                .expect("client sends the next command")
                .expect("writer remains connected")
                .expect("decode command line");
            let command_id = match decode_frame::<ClientFrame>(&line).expect("valid client frame") {
                ClientFrame::Resize { command_id, .. } => command_id,
                frame => panic!("expected resize after rejected input, got {frame:?}"),
            };
            assert_eq!(command_id.get(), 1, "unsent input does not consume its ID");
            server
                .send(
                    encode_frame(&ServerFrame::CommandResult {
                        command_id,
                        outcome: ControlOutcome::Accepted {
                            receipt: ControlReceipt::Resize {
                                applied_size: TerminalSize { rows: 30, cols: 90 },
                            },
                        },
                    })
                    .expect("encode resize result"),
                )
                .await
                .expect("send resize result");
        });
        let accepted = attachment
            .resize(TerminalSize { rows: 30, cols: 90 })
            .await
            .expect("attachment remains usable after local preflight rejection");
        assert_eq!(accepted.command_id.get(), 1);
        server_task.await.expect("server task completes");
    }
}
