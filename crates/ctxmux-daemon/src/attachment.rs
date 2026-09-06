//! Replay join and correlated control delivery for one attachment connection.

use std::{collections::VecDeque, future::Future, sync::Arc};

use ctxmux_protocol::{
    AttachedHeader, AttachedSnapshot, AttachmentCommandId, AttachmentView, ClientFrame,
    ControlOutcome, ErrorCode, MAX_FRAME_BYTES, OutputChunk, OutputGapCauses, OutputReplay,
    OutputReplayHeader, ProtocolError, RecoverableStop, Response, RunEvent, RunId, RunSignal,
    RunState, ServerFrame, TerminalSize,
};
use futures_util::{FutureExt, StreamExt, future::BoxFuture, stream::FuturesUnordered};
use tokio::{net::UnixStream, sync::broadcast};
use tokio_util::codec::{Framed, LinesCodec};

#[cfg(test)]
use super::AttachmentHookPoint;
use super::{
    ConnectionError, ControlResult, LiveEventCursor, Run, RunManager, UpgradeRequestAdmission,
    UpgradeRequestPermit, control_not_applied, invalid_request, receive, send, send_capped,
    upgrade_retry_error,
};

pub(super) async fn handle(
    mut wire: Framed<UnixStream, LinesCodec>,
    manager: Arc<RunManager>,
    id: RunId,
    after_byte: u64,
    view: AttachmentView,
    request_permit: UpgradeRequestPermit,
) -> Result<(), ConnectionError> {
    let run = match manager.pin(id) {
        Ok(run) => run,
        Err(error) => {
            send(&mut wire, &ServerFrame::Error { error }).await?;
            return Ok(());
        }
    };
    handle_pinned(wire, manager, run, after_byte, view, request_permit, None).await
}

#[allow(
    clippy::too_many_lines,
    reason = "one select loop keeps command-result, terminal, and live-delivery ordering auditable"
)]
pub(super) async fn handle_pinned(
    mut wire: Framed<UnixStream, LinesCodec>,
    manager: Arc<RunManager>,
    run: Arc<Run>,
    after_byte: u64,
    view: AttachmentView,
    request_permit: UpgradeRequestPermit,
    initial_response: Option<Response>,
) -> Result<(), ConnectionError> {
    let (_guard, subscription) = match run.try_subscribe() {
        Ok(subscription) => subscription,
        Err(error) => {
            send(&mut wire, &ServerFrame::Error { error }).await?;
            return Ok(());
        }
    };
    let mut events = subscription.receiver;
    let mut live_cursor = subscription.cursor;
    #[cfg(test)]
    if let Some(hook) = &manager.attachment_hook {
        hook.pause_once(AttachmentHookPoint::AfterSubscribe).await;
    }
    let snapshot = match run.attachment_snapshot(after_byte, view).await {
        Ok(snapshot) => snapshot,
        Err(error) => {
            send(
                &mut wire,
                &ServerFrame::Error {
                    error: ProtocolError::new(ErrorCode::Io, error.to_string()),
                },
            )
            .await?;
            return Ok(());
        }
    };
    let (header, replay_chunks, terminal_state, restore) = split_snapshot(snapshot);
    let mut sent_resize_revision = header.resize_revision;
    let mut sent_service_revision = header
        .run
        .native_service
        .as_ref()
        .map_or(0, |service| service.revision);
    let replay_start = match &header.terminal {
        ctxmux_protocol::TerminalContinuation::BasicVt { checkpoint, .. } => {
            checkpoint.through_byte
        }
        _ => after_byte,
    };
    let mut sent_through_byte = header.replay.latest_output_bytes;
    let initial_floor = header.replay.first_available_byte;
    let has_terminal_seed = matches!(
        header.terminal,
        ctxmux_protocol::TerminalContinuation::BasicVt { .. }
    );
    // The attachment header embeds a full RunInfo, whose RunSpec is
    // caller-controlled and unbounded. If it cannot be framed, the client gets a
    // typed ResponseTooLarge error rather than a silently dropped socket, and we
    // stop before streaming replay onto a connection whose header never landed.
    if !send_capped(&mut wire, &ServerFrame::Attached { snapshot: header }).await? {
        drop(request_permit);
        return Ok(());
    }
    let mut replay_cursor = replay_chunks
        .last()
        .map_or(replay_start.max(initial_floor), |chunk| chunk.end_byte);
    let restore_chunk_bytes = terminal_checkpoint_chunk_bytes()?;
    for (index, data) in restore.chunks(restore_chunk_bytes).enumerate() {
        send(
            &mut wire,
            &ServerFrame::TerminalCheckpointChunk {
                offset: (index * restore_chunk_bytes) as u64,
                data: data.to_vec(),
            },
        )
        .await?;
    }
    send_replay(&mut wire, replay_chunks).await?;
    #[cfg(test)]
    if let Some(hook) = &manager.attachment_hook {
        hook.pause_once(AttachmentHookPoint::AfterReplayPage).await;
    }
    while replay_cursor < sent_through_byte {
        let page = match run
            .attachment_replay_page(replay_cursor, sent_through_byte)
            .await
        {
            Ok(page) => page,
            Err(error) => {
                send(
                    &mut wire,
                    &ServerFrame::Error {
                        error: ProtocolError::new(ErrorCode::Io, error.to_string()),
                    },
                )
                .await?;
                return Ok(());
            }
        };
        if page.first_available_byte > replay_cursor {
            if has_terminal_seed {
                send(
                    &mut wire,
                    &ServerFrame::Error {
                        error: ProtocolError::new(
                            ErrorCode::Io,
                            "terminal checkpoint original tail was evicted while replay was streaming; raw attachment remains available",
                        ),
                    },
                )
                .await?;
                return Ok(());
            }
            send(
                &mut wire,
                &ServerFrame::ReplayWindow {
                    first_available_byte: page.first_available_byte.min(sent_through_byte),
                    latest_output_bytes: sent_through_byte,
                },
            )
            .await?;
        }
        let next = page
            .chunks
            .last()
            .map_or(sent_through_byte, |chunk| chunk.end_byte);
        if next <= replay_cursor {
            break;
        }
        replay_cursor = next;
        send_replay(&mut wire, page.chunks).await?;
    }
    if let Some(response) = initial_response {
        send(&mut wire, &ServerFrame::Response { response }).await?;
    }
    // Attach setup is one admitted request, but a long-lived attachment is only
    // a view. Release its request permit after replay plus any composite Stop
    // result; each later mutation acquires its own permit through `handle_frame`.
    drop(request_permit);
    #[cfg(test)]
    if let Some(hook) = &manager.attachment_hook {
        hook.pause_once(AttachmentHookPoint::AfterSnapshot).await;
    }
    if !terminal_state.is_running() {
        return finish_terminal_snapshot(
            &mut wire,
            &run,
            terminal_state,
            live_cursor,
            sent_through_byte,
        )
        .await;
    }

    let mut command_results = PendingResults::new();
    let mut command_admissions = PendingAdmissions::new();
    let mut controls = ControlState::default();
    let mut receiver_lagged = false;
    let mut observation_closed = false;
    loop {
        // A lost observation stream ends this view, not already admitted
        // commands. Their request permits cover actual result flush, including
        // an upgrade crossing a partial PTY write.
        if observation_closed && command_admissions.is_empty() && command_results.is_empty() {
            return Ok(());
        }
        // Pending command nodes share the daemon's funded control-memory owner.
        // The explicit Stop barrier, not select readiness, orders terminal exit.
        // Keep Tokio's fair branch polling: a continuously ready command lane
        // must not starve observations until their retained window is lost.
        tokio::select! {
            Some(completed) = command_results.next(), if !command_results.is_empty() => {
                send_command_result(&mut wire, completed.command_id, completed.outcome).await?;
                drop(completed.permit);
                if completed.is_stop {
                    controls.pending_stops -= 1;
                }
                if controls.pending_stops == 0
                    && command_admissions.is_empty()
                    && command_results.is_empty()
                    && let Some(event) = controls.held_terminal.take()
                {
                    send(&mut wire, &ServerFrame::Event { event }).await?;
                    return Ok(());
                }
            }
            admitted = async { command_admissions.front_mut().expect("nonempty admission queue").future.as_mut().await }, if !command_admissions.is_empty() => {
                command_admissions.pop_front();
                command_results.push(admitted.result);
            }
            incoming = receive(&mut wire) => {
                let Some(frame) = incoming? else {
                    return Ok(());
                };
                if observation_closed {
                    if reject_after_observation_close(&mut wire, &mut controls, frame).await? {
                        return Ok(());
                    }
                    continue;
                }
                if handle_frame(
                    &mut wire,
                    &manager,
                    &run,
                    &mut controls,
                    &mut command_admissions,
                    frame,
                ).await? {
                    return Ok(());
                }
            }
            received = events.recv(), if controls.held_terminal.is_none() && !observation_closed => {
                match received {
                    Ok(envelope) => {
                        if receiver_lagged {
                            receiver_lagged = false;
                            match recover_lagged_delivery(
                                &mut wire,
                                &run,
                                live_cursor,
                                envelope.before,
                                &mut sent_through_byte,
                                &mut sent_service_revision,
                                sent_resize_revision,
                            )
                            .await?
                            {
                                LagRecovery::Continue => {}
                                LagRecovery::Close => {
                                    observation_closed = true;
                                    cancel_unadmitted_after_observation_close(
                                        &mut wire, &mut controls, &mut command_admissions,
                                    ).await?;
                                    continue;
                                },
                                LagRecovery::Terminal(terminal) => {
                                    if controls.pending_stops == 0
                                        && command_admissions.is_empty()
                                        && command_results.is_empty()
                                    {
                                        send(&mut wire, &ServerFrame::Event { event: terminal })
                                            .await?;
                                        return Ok(());
                                    }
                                    controls.held_terminal = Some(terminal);
                                    continue;
                                }
                            }
                        }
                        live_cursor = envelope.after;
                        let event = envelope.event();
                        match event.as_ref() {
                            RunEvent::Output { chunk }
                                if chunk.end_byte <= sent_through_byte => {}
                            RunEvent::Output { chunk } => {
                                sent_through_byte = chunk.end_byte;
                                send_event(&mut wire, event.as_ref()).await?;
                            }
                            RunEvent::Resized { resize_revision, .. } if *resize_revision <= sent_resize_revision => {}
                            RunEvent::ServiceChanged { service } if service.revision <= sent_service_revision => {}
                            event @ RunEvent::ServiceChanged { service } => {
                                sent_service_revision = service.revision;
                                send_event(&mut wire, event).await?;
                            }
                            event @ RunEvent::Resized { resize_revision, .. } => {
                                sent_resize_revision = *resize_revision;
                                send_event(&mut wire, event).await?;
                            }
                            event @ (RunEvent::Exited { .. } | RunEvent::Interrupted { .. }) => {
                                if controls.pending_stops == 0
                                    && command_admissions.is_empty()
                                    && command_results.is_empty()
                                {
                                    send_event(&mut wire, event).await?;
                                    return Ok(());
                                }
                                controls.held_terminal = Some(event.clone());
                            }
                            event => send_event(&mut wire, event).await?,
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        receiver_lagged = true;
                    }
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                }
            }
        }
    }
}

// Borrow the shared, funded payload while encoding. Broadcast fanout must not
// deep-clone Vec bytes into unleased envelopes held across an async send.
async fn send_event(
    wire: &mut Framed<UnixStream, LinesCodec>,
    event: &RunEvent,
) -> Result<(), ConnectionError> {
    #[derive(serde::Serialize)]
    struct EventFrame<'a> {
        r#type: &'static str,
        event: &'a RunEvent,
    }
    send(
        wire,
        &EventFrame {
            r#type: "event",
            event,
        },
    )
    .await
}

async fn finish_terminal_snapshot(
    wire: &mut Framed<UnixStream, LinesCodec>,
    run: &Run,
    terminal_state: RunState,
    live_cursor: LiveEventCursor,
    sent_through_byte: u64,
) -> Result<(), ConnectionError> {
    let cursor_after_snapshot = run.events.cursor();
    let observation_discontinuous =
        cursor_after_snapshot.observation_revision > live_cursor.observation_revision;
    if observation_discontinuous {
        send(
            wire,
            &ServerFrame::Event {
                event: RunEvent::ObservationDiscontinuity,
            },
        )
        .await?;
    }
    if !observation_discontinuous
        && (cursor_after_snapshot.output_bytes > sent_through_byte
            || cursor_after_snapshot.output_discontinuity_revision
                > live_cursor.output_discontinuity_revision)
    {
        let latest_output_bytes = cursor_after_snapshot
            .output_bytes
            .max(cursor_after_snapshot.latest_output_discontinuity_byte);
        send(
            wire,
            &ServerFrame::Event {
                event: RunEvent::Gap {
                    latest_output_bytes,
                    causes: OutputGapCauses::TERMINAL_CATCHUP.union(
                        cursor_after_snapshot
                            .gap_causes
                            .since(live_cursor.gap_causes),
                    ),
                },
            },
        )
        .await?;
    }
    send(
        wire,
        &ServerFrame::Event {
            event: terminal_event(terminal_state),
        },
    )
    .await?;
    Ok(())
}

enum LagRecovery {
    Continue,
    Close,
    Terminal(RunEvent),
}

async fn recover_lagged_delivery(
    wire: &mut Framed<UnixStream, LinesCodec>,
    run: &Run,
    delivered: LiveEventCursor,
    retained_before: LiveEventCursor,
    sent_through_byte: &mut u64,
    sent_service_revision: &mut u64,
    sent_resize_revision: u64,
) -> Result<LagRecovery, ConnectionError> {
    let authoritative = run.info();
    let lost_observation = retained_before.observation_revision > delivered.observation_revision;
    let lost_terminal = retained_before.terminal_revision > delivered.terminal_revision;
    let lost_output_marker =
        retained_before.output_discontinuity_revision > delivered.output_discontinuity_revision;
    let lost_output_bytes = retained_before.output_bytes > *sent_through_byte;
    // The initial snapshot can already cover revisions published after
    // subscription. Geometry has its own sent fence; advancing shared cause
    // cursors here could conceal an unreplayable source discontinuity.
    let lost_resize = retained_before.resize_revision > sent_resize_revision;
    if lost_observation {
        send(
            wire,
            &ServerFrame::Event {
                event: RunEvent::ObservationDiscontinuity,
            },
        )
        .await?;
    }
    if !lost_observation && (lost_output_marker || lost_output_bytes || lost_resize) {
        let marker_head = if lost_output_marker {
            retained_before.latest_output_discontinuity_byte
        } else {
            0
        };
        let latest_output_bytes = retained_before
            .output_bytes
            .max(marker_head)
            .max(authoritative.latest_output_bytes);
        *sent_through_byte = (*sent_through_byte).max(latest_output_bytes);
        send(
            wire,
            &ServerFrame::Event {
                event: RunEvent::Gap {
                    latest_output_bytes,
                    causes: OutputGapCauses::SUBSCRIBER_LAG
                        .union(if lost_resize {
                            OutputGapCauses::GEOMETRY_LAG
                        } else {
                            OutputGapCauses::NONE
                        })
                        .union(retained_before.gap_causes.since(delivered.gap_causes)),
                },
            },
        )
        .await?;
    }
    if lost_observation && authoritative.state.is_running() {
        return Ok(LagRecovery::Close);
    }
    if !lost_observation
        && retained_before.service_revision > delivered.service_revision
        && let Some(service) = authoritative.native_service
        && service.revision > *sent_service_revision
    {
        *sent_service_revision = service.revision;
        send(
            wire,
            &ServerFrame::Event {
                event: RunEvent::ServiceChanged { service },
            },
        )
        .await?;
    }
    if (lost_observation || lost_terminal) && !authoritative.state.is_running() {
        return Ok(LagRecovery::Terminal(terminal_event(authoritative.state)));
    }
    Ok(LagRecovery::Continue)
}

fn terminal_checkpoint_chunk_bytes() -> Result<usize, ctxmux_protocol::FrameError> {
    // The public codec uses padded base64: each three raw bytes occupies four
    // ASCII bytes. Use the actual largest-offset envelope, then whole encoding
    // groups, preserving the complete restore stream at the frame boundary.
    let envelope = ctxmux_protocol::encode_frame(&ServerFrame::TerminalCheckpointChunk {
        offset: u64::MAX,
        data: Vec::new(),
    })?;
    let available = MAX_FRAME_BYTES.saturating_sub(envelope.len());
    let bytes = (available / 4) * 3;
    if bytes == 0 {
        return Err(ctxmux_protocol::FrameError::TooLarge {
            actual: envelope.len() + 4,
            maximum: MAX_FRAME_BYTES,
        });
    }
    Ok(bytes)
}

async fn send_replay(
    wire: &mut Framed<UnixStream, LinesCodec>,
    chunks: Vec<OutputChunk>,
) -> Result<(), ConnectionError> {
    for chunk in chunks {
        send(
            wire,
            &ServerFrame::Event {
                event: RunEvent::Output { chunk },
            },
        )
        .await?;
    }
    Ok(())
}

#[derive(Default)]
struct ControlState {
    last_command_id: Option<AttachmentCommandId>,
    // One scalar barrier: command identity stays in the funded result node.
    pending_stops: usize,
    held_terminal: Option<RunEvent>,
}

struct CompletedCommand {
    command_id: AttachmentCommandId,
    outcome: ControlOutcome,
    permit: Option<UpgradeRequestPermit>,
    is_stop: bool,
}

type PendingResult = BoxFuture<'static, CompletedCommand>;
type PendingResults = FuturesUnordered<PendingResult>;
// Only the front admission is polled. Receiving later frames stays responsive,
// but no later command can overtake an unadmitted earlier frame. Receipt waits
// become independent futures as soon as the daemon queue owns the command.
struct AdmissionFinished {
    result: PendingResult,
}
struct PendingAdmission {
    command_id: AttachmentCommandId,
    is_stop: bool,
    future: BoxFuture<'static, AdmissionFinished>,
}
type PendingAdmissions = VecDeque<PendingAdmission>;

fn pending_result<F>(
    command_id: AttachmentCommandId,
    permit: Option<UpgradeRequestPermit>,
    result: F,
    is_stop: bool,
) -> AdmissionFinished
where
    F: Future<Output = ControlResult> + Send + 'static,
{
    AdmissionFinished {
        result: async move {
            CompletedCommand {
                command_id,
                outcome: outcome(result.await),
                permit,
                is_stop,
            }
        }
        .boxed(),
    }
}

fn push_admission<F>(
    run: &Run,
    admissions: &mut PendingAdmissions,
    command_id: AttachmentCommandId,
    is_stop: bool,
    future: F,
) -> Result<(), ctxmux_protocol::ControlFailure>
where
    F: Future<Output = AdmissionFinished> + Send + 'static,
{
    // Charge the concrete future state and both erased future handles. PTY/
    // allocator and FuturesUnordered bookkeeping remain separately measured RSS.
    let bytes = std::mem::size_of_val(&future)
        + std::mem::size_of::<crate::resources::BytePermit>()
        + std::mem::size_of::<PendingAdmission>()
        + std::mem::size_of::<PendingResult>();
    let memory = run
        .native_control()
        .map_err(control_not_applied)?
        .reserve_control_memory(bytes)
        .ok_or_else(|| {
            control_not_applied(ProtocolError::new(
                ErrorCode::ControlBackpressure,
                "daemon attachment admission-memory budget is full",
            ))
        })?;
    admissions.push_back(PendingAdmission {
        command_id,
        is_stop,
        future: async move {
            let admission = future.await;
            AdmissionFinished {
                result: async move {
                    let result = admission.result.await;
                    drop(memory);
                    result
                }
                .boxed(),
            }
        }
        .boxed(),
    });
    Ok(())
}

enum ControlCommand {
    Input(Vec<u8>),
    Resize(TerminalSize),
    Signal(RunSignal),
    Stop(RecoverableStop),
}

impl ControlState {
    async fn handle_stop_command(
        &mut self,
        wire: &mut Framed<UnixStream, LinesCodec>,
        manager: &Arc<RunManager>,
        admissions: &mut PendingAdmissions,
        run: &Arc<Run>,
        command: (AttachmentCommandId, RecoverableStop),
        permit: UpgradeRequestPermit,
    ) -> Result<(), ConnectionError> {
        let (command_id, operation) = command;
        if let Err(failure) = manager.validate_recoverable_stop(&operation, Some(run.id)) {
            send_command_result(wire, command_id, ControlOutcome::Rejected { failure }).await?;
            drop(permit);
            return Ok(());
        }
        let operation_manager = Arc::clone(manager);
        let future = async move {
            let mut permit = Some(permit);
            match operation_manager.begin_recoverable_stop_with_owner_permit(operation, &mut permit)
            {
                Ok(pending) => pending_result(
                    command_id,
                    permit,
                    async move { pending.resolve().await.1 },
                    true,
                ),
                Err(failure) => pending_result(
                    command_id,
                    permit,
                    futures_util::future::ready(Err(failure)),
                    true,
                ),
            }
        };
        match push_admission(run, admissions, command_id, true, future) {
            Ok(()) => {
                self.pending_stops += 1;
            }
            Err(mut failure) => {
                // The retained Stop key has not been inspected. A budget refusal
                // cannot prove this operation was never admitted elsewhere.
                failure.disposition = ctxmux_protocol::CommandDisposition::Unknown;
                send_command_result(wire, command_id, ControlOutcome::Rejected { failure }).await?;
            }
        }
        Ok(())
    }
}

fn queue_input_admission(
    run: &Arc<Run>,
    admissions: &mut PendingAdmissions,
    command_id: AttachmentCommandId,
    data: Vec<u8>,
    permit: UpgradeRequestPermit,
) -> Result<(), ctxmux_protocol::ControlFailure> {
    let control = run.native_control().map_err(control_not_applied)?.clone();
    let memory = control.reserve_input_payload(data.len()).ok_or_else(|| {
        control_not_applied(ProtocolError::new(
            ErrorCode::ControlBackpressure,
            "daemon attachment waiting-payload budget is full",
        ))
    })?;
    push_admission(run, admissions, command_id, false, async move {
        match control.begin_input_async_funded(data, memory).await {
            Ok(pending) => pending_result(command_id, Some(permit), pending.resolve(), false),
            Err(failure) => pending_result(
                command_id,
                Some(permit),
                futures_util::future::ready(Err(failure)),
                false,
            ),
        }
    })
}

fn observation_closed_failure(recoverable: bool) -> ctxmux_protocol::ControlFailure {
    let mut failure = control_not_applied(ProtocolError::new(
        ErrorCode::BackendUnavailable,
        "attachment observation stream ended; reattach to resume observation",
    ));
    if recoverable {
        // The retained Stop key has not been inspected on this failed view.
        failure.disposition = ctxmux_protocol::CommandDisposition::Unknown;
    }
    failure
}

async fn cancel_unadmitted_after_observation_close(
    wire: &mut Framed<UnixStream, LinesCodec>,
    controls: &mut ControlState,
    admissions: &mut PendingAdmissions,
) -> Result<(), ConnectionError> {
    while let Some(admission) = admissions.pop_front() {
        // Do not poll the effect-bearing admission future. Its funded buffers
        // and upgrade permit remain owned through this truthful response flush;
        // cancellation then refunds them without entering the daemon queue.
        send_command_result(
            wire,
            admission.command_id,
            ControlOutcome::Rejected {
                failure: observation_closed_failure(admission.is_stop),
            },
        )
        .await?;
        if admission.is_stop {
            controls.pending_stops -= 1;
        }
        drop(admission);
    }
    Ok(())
}

// The observation discontinuity has already been sent. Keep the socket alive
// only for original command results and disconnect detection; later mutations
// cannot turn a failed view into a silently usable control channel.
async fn reject_after_observation_close(
    wire: &mut Framed<UnixStream, LinesCodec>,
    controls: &mut ControlState,
    frame: ClientFrame,
) -> Result<bool, ConnectionError> {
    let (command_id, recoverable) = match frame {
        ClientFrame::Input { command_id, .. }
        | ClientFrame::Resize { command_id, .. }
        | ClientFrame::Signal { command_id, .. } => (command_id, false),
        ClientFrame::Stop { command_id, .. } => (command_id, true),
        ClientFrame::Detach => {
            send(wire, &ServerFrame::Detached).await?;
            return Ok(true);
        }
        ClientFrame::Hello { .. } | ClientFrame::Request { .. } => {
            send(
                wire,
                &invalid_request("frame is not valid during attachment"),
            )
            .await?;
            return Ok(false);
        }
    };
    if let Err(error) = observe_command_id(&mut controls.last_command_id, command_id) {
        send(wire, &ServerFrame::Error { error }).await?;
        return Ok(true);
    }
    send_command_result(
        wire,
        command_id,
        ControlOutcome::Rejected {
            failure: observation_closed_failure(recoverable),
        },
    )
    .await?;
    Ok(false)
}

async fn handle_frame(
    wire: &mut Framed<UnixStream, LinesCodec>,
    manager: &Arc<RunManager>,
    run: &Arc<Run>,
    controls: &mut ControlState,
    admissions: &mut PendingAdmissions,
    frame: ClientFrame,
) -> Result<bool, ConnectionError> {
    #[cfg(not(test))]
    let _ = manager;
    let (command_id, command) = match frame {
        ClientFrame::Input { command_id, data } => (command_id, ControlCommand::Input(data)),
        ClientFrame::Resize { command_id, size } => (command_id, ControlCommand::Resize(size)),
        ClientFrame::Signal { command_id, signal } => (command_id, ControlCommand::Signal(signal)),
        ClientFrame::Stop {
            command_id,
            operation,
        } => (command_id, ControlCommand::Stop(operation)),
        ClientFrame::Detach => {
            #[cfg(test)]
            if let Some(hook) = &manager.attachment_hook {
                hook.pause_once(AttachmentHookPoint::BeforeDetachAck).await;
            }
            send(wire, &ServerFrame::Detached).await?;
            return Ok(true);
        }
        ClientFrame::Hello { .. } | ClientFrame::Request { .. } => {
            send(
                wire,
                &invalid_request("frame is not valid during attachment"),
            )
            .await?;
            return Ok(false);
        }
    };
    if let Err(error) = observe_command_id(&mut controls.last_command_id, command_id) {
        send(wire, &ServerFrame::Error { error }).await?;
        return Ok(true);
    }
    let permit = match manager.upgrade_requests.admit() {
        UpgradeRequestAdmission::Execute(permit) => permit,
        UpgradeRequestAdmission::Retry(permit) => {
            send_command_result(
                wire,
                command_id,
                ControlOutcome::Rejected {
                    failure: control_not_applied(upgrade_retry_error()),
                },
            )
            .await?;
            drop(permit);
            return Ok(false);
        }
        UpgradeRequestAdmission::Sealed => return Ok(true),
    };

    let queued = match command {
        ControlCommand::Input(data) => {
            queue_input_admission(run, admissions, command_id, data, permit)
        }
        ControlCommand::Resize(size) => {
            let operation_run = Arc::clone(run);
            push_admission(run, admissions, command_id, false, async move {
                let result = operation_run.resize_async(size).await;
                pending_result(
                    command_id,
                    Some(permit),
                    futures_util::future::ready(result),
                    false,
                )
            })
        }
        ControlCommand::Signal(signal) => {
            let operation_run = Arc::clone(run);
            push_admission(run, admissions, command_id, false, async move {
                match operation_run.begin_signal(signal).await {
                    Ok(pending) => {
                        pending_result(command_id, Some(permit), pending.resolve(), false)
                    }
                    Err(failure) => pending_result(
                        command_id,
                        Some(permit),
                        futures_util::future::ready(Err(failure)),
                        false,
                    ),
                }
            })
        }
        ControlCommand::Stop(operation) => {
            controls
                .handle_stop_command(
                    wire,
                    manager,
                    admissions,
                    run,
                    (command_id, operation),
                    permit,
                )
                .await?;
            return Ok(false);
        }
    };
    if let Err(failure) = queued {
        send_command_result(wire, command_id, ControlOutcome::Rejected { failure }).await?;
    }
    Ok(false)
}

fn terminal_event(state: RunState) -> RunEvent {
    match state {
        RunState::Interrupted { reason } => RunEvent::Interrupted { reason },
        state @ RunState::Exited { .. } => RunEvent::Exited { state },
        RunState::Running => unreachable!("running state is not terminal"),
    }
}

fn split_snapshot(
    snapshot: AttachedSnapshot,
) -> (AttachedHeader, Vec<OutputChunk>, RunState, Vec<u8>) {
    let AttachedSnapshot {
        run: run_info,
        replay,
        terminal,
        terminal_restore,
        resize_revision,
    } = snapshot;
    let OutputReplay {
        chunks,
        first_available_byte,
        latest_output_bytes,
        truncated,
    } = replay;
    let terminal_state = run_info.state.clone();
    let header = AttachedHeader {
        run: run_info,
        terminal,
        resize_revision,
        replay: OutputReplayHeader {
            first_available_byte,
            latest_output_bytes,
            truncated,
        },
    };
    (header, chunks, terminal_state, terminal_restore)
}

fn observe_command_id(
    last: &mut Option<AttachmentCommandId>,
    command_id: AttachmentCommandId,
) -> Result<(), ProtocolError> {
    let valid = last.map_or(command_id.get() == 1, |last| command_id.get() > last.get());
    if !valid {
        return Err(ProtocolError::new(
            ErrorCode::InvalidRequest,
            "attachment command ids must start at 1 and then increase strictly",
        ));
    }
    *last = Some(command_id);
    Ok(())
}

fn outcome(result: ControlResult) -> ControlOutcome {
    match result {
        Ok(receipt) => ControlOutcome::Accepted { receipt },
        Err(failure) => ControlOutcome::Rejected { failure },
    }
}

async fn send_command_result(
    wire: &mut Framed<UnixStream, LinesCodec>,
    command_id: AttachmentCommandId,
    outcome: ControlOutcome,
) -> Result<(), ConnectionError> {
    send(
        wire,
        &ServerFrame::CommandResult {
            command_id,
            outcome,
        },
    )
    .await
}

#[cfg(test)]
mod tests {
    use ctxmux_protocol::{AttachmentCommandId, ErrorCode};

    use super::observe_command_id;

    async fn wire_frame(
        wire: &mut tokio_util::codec::Framed<tokio::net::UnixStream, tokio_util::codec::LinesCodec>,
        deadline: tokio::time::Instant,
    ) -> ctxmux_protocol::ServerFrame {
        use futures_util::StreamExt as _;
        let line = tokio::time::timeout_at(deadline, wire.next())
            .await
            .unwrap()
            .expect("actual attachment remains connected")
            .unwrap();
        ctxmux_protocol::decode_frame(line).unwrap()
    }

    async fn raw_view(
        socket: &std::path::Path,
        id: ctxmux_protocol::RunId,
        deadline: tokio::time::Instant,
    ) -> tokio_util::codec::Framed<tokio::net::UnixStream, tokio_util::codec::LinesCodec> {
        use futures_util::SinkExt as _;
        let socket = tokio::net::UnixStream::connect(socket).await.unwrap();
        let mut wire = tokio_util::codec::Framed::new(socket, super::super::codec());
        wire.send(
            ctxmux_protocol::encode_frame(&ctxmux_protocol::ClientFrame::Hello {
                hello: ctxmux_protocol::ClientHello {
                    protocol: ctxmux_protocol::PROTOCOL_VERSION,
                },
            })
            .unwrap(),
        )
        .await
        .unwrap();
        assert!(matches!(
            wire_frame(&mut wire, deadline).await,
            ctxmux_protocol::ServerFrame::Hello { .. }
        ));
        wire.send(
            ctxmux_protocol::encode_frame(&ctxmux_protocol::ClientFrame::Request {
                request: ctxmux_protocol::Request::Attach {
                    id,
                    after_byte: 5,
                    view: ctxmux_protocol::AttachmentView::Raw,
                },
            })
            .unwrap(),
        )
        .await
        .unwrap();
        let ctxmux_protocol::ServerFrame::Attached { snapshot } =
            wire_frame(&mut wire, deadline).await
        else {
            panic!("actual attachment header");
        };
        assert_eq!(snapshot.run.id, id);
        assert_eq!(snapshot.replay.latest_output_bytes, 5);
        wire
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(
        clippy::too_many_lines,
        reason = "one real two-Run fixture preserves the original frame sequence across funded waiting, Detach cancellation, reconnect, and exact FIFO receipts"
    )]
    async fn actual_attachment_waits_in_frame_fifo_and_detach_cancels_only_unadmitted_input() {
        use ctxmux_protocol::{
            ClientFrame, ControlOutcome, ControlReceipt, RunEvent, RunSpec, ServerFrame,
            TerminalSize,
        };
        use futures_util::SinkExt as _;
        use std::{
            collections::BTreeMap,
            sync::{Arc, mpsc},
            time::Duration,
        };
        let server = super::super::tests::InProcessServer::start(Arc::new(
            super::super::RunManager::default(),
        ));
        let socket = server.directory.path().join("ctxmux.sock");
        let second_client = ctxmux_client::Client::new(&socket);
        let payloads = [
            vec![b'a'; 16 * 1024 + 19],
            vec![b'b'; 16 * 1024 + 23],
            vec![3],
        ];
        let original = payloads.iter().flatten().copied().collect::<Vec<_>>();
        let spec = |script: String| RunSpec {
            program: "/bin/sh".to_owned(),
            args: vec!["-c".to_owned(), script],
            cwd: None,
            env: BTreeMap::new(),
            initial_size: TerminalSize::default(),
            declared_inputs: Vec::new(),
        };
        // This oracle is input FIFO, not dd's one-byte output chunking. Read
        // every original byte before emitting the same complete sequence.
        let mut first_spec = spec(String::new());
        first_spec.program = "/usr/bin/python3".to_owned();
        first_spec.args = vec![
            "-c".to_owned(),
            format!(
                "import os,tty\ntty.setraw(0)\nos.write(1,b'READY')\ndata=bytearray()\nwhile len(data)<{length}:\n data.extend(os.read(0,{length}-len(data)))\nwritten=0\nwhile written<len(data):\n written+=os.write(1,data[written:])\n",
                length = original.len()
            ),
        ];
        let first = server.client.start(first_spec).await.unwrap();
        let second = second_client
            .start(spec("stty -echo; printf READY; exec /bin/cat".to_owned()))
            .await
            .unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        for run in [&first, &second] {
            loop {
                let current = second_client.status(run.id).await.unwrap();
                assert_eq!(current.pid, run.pid);
                if current.latest_output_bytes == 5 {
                    break;
                }
                assert!(tokio::time::Instant::now() < deadline);
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
        let actual = server.manager.get(first.id).unwrap();
        let control = actual.native_control().unwrap().clone();
        let (held_tx, held_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let held = control.clone();
        let holder = std::thread::spawn(move || {
            held.with_metadata(|_, _| {
                held_tx.send(()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            });
        });
        held_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let budget = server.manager.native_input_drains.control_budget();
        let baseline = budget.used();
        let mut wire = raw_view(&socket, first.id, deadline).await;
        // First send every original frame, then detach before any can acquire
        // the actual held owner. Budget deltas prove each frame was consumed.
        for epoch in 0..2 {
            for (index, payload) in payloads.iter().enumerate() {
                let before = budget.used();
                wire.send(
                    ctxmux_protocol::encode_frame(&ClientFrame::Input {
                        command_id: AttachmentCommandId::new(u32::try_from(index + 1).unwrap())
                            .unwrap(),
                        data: payload.clone(),
                    })
                    .unwrap(),
                )
                .await
                .unwrap();
                while budget.used() <= before {
                    assert!(
                        tokio::time::Instant::now() < deadline,
                        "each original frame funds waiting admission"
                    );
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
            if epoch == 0 {
                wire.send(ctxmux_protocol::encode_frame(&ClientFrame::Detach).unwrap())
                    .await
                    .unwrap();
                loop {
                    if matches!(wire_frame(&mut wire, deadline).await, ServerFrame::Detached) {
                        break;
                    }
                }
                drop(wire);
                while budget.used() != baseline {
                    assert!(
                        tokio::time::Instant::now() < deadline,
                        "detach refunds all unadmitted payloads and future nodes"
                    );
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                assert_eq!(
                    second_client
                        .status(first.id)
                        .await
                        .unwrap()
                        .applied_input_bytes,
                    Some(0)
                );
                wire = raw_view(&socket, first.id, deadline).await;
            }
        }
        // While A's original frames wait, a second real client and Run retain
        // their original identities and serve real input, output and Signal.
        second_client
            .input(second.id, b"healthy\n".to_vec())
            .await
            .unwrap();
        while second_client
            .status(second.id)
            .await
            .unwrap()
            .latest_output_bytes
            < 14
        {
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let (view, snapshot) = second_client.attach(second.id, 0).await.unwrap();
        assert_eq!(
            ctxmux_client::replay_bytes(&snapshot.replay.chunks),
            b"READYhealthy\r\n"
        );
        view.detach().await.unwrap();
        second_client.interrupt(second.id).await.unwrap();
        release_tx.send(()).unwrap();
        holder.join().unwrap();
        let mut bytes = Vec::new();
        let mut receipts = BTreeMap::new();
        let mut exited = false;
        while bytes.len() != original.len() || receipts.len() != payloads.len() || !exited {
            match wire_frame(&mut wire, deadline).await {
                ServerFrame::CommandResult {
                    command_id,
                    outcome:
                        ControlOutcome::Accepted {
                            receipt: ControlReceipt::Input { written_bytes },
                        },
                } => {
                    assert!(receipts.insert(command_id.get(), written_bytes).is_none());
                }
                ServerFrame::Event {
                    event: RunEvent::Output { chunk },
                } => {
                    assert_eq!(chunk.start_byte, 5 + bytes.len() as u64);
                    bytes.extend_from_slice(&chunk.data);
                }
                ServerFrame::Event {
                    event: RunEvent::Exited { .. },
                } => exited = true,
                ServerFrame::Event {
                    event: RunEvent::ServiceChanged { .. },
                } => {}
                frame => panic!("unexpected FIFO proof frame {frame:?}"),
            }
        }
        assert_eq!(
            bytes, original,
            "both original payloads and Ctrl-C byte retain exact order"
        );
        for (index, payload) in payloads.iter().enumerate() {
            assert_eq!(
                receipts[&(u32::try_from(index).unwrap() + 1)] as usize,
                payload.len()
            );
        }
        let status = second_client.status(first.id).await.unwrap();
        assert_eq!((status.id, status.pid), (first.id, first.pid));
        assert_eq!(status.applied_input_bytes, Some(original.len() as u64));
        while second_client
            .status(second.id)
            .await
            .unwrap()
            .state
            .is_running()
        {
            assert!(
                tokio::time::Instant::now() < deadline,
                "Signal interrupts actual original child"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(
            budget.used(),
            baseline,
            "all original legacy commands and response nodes refund funding"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(
        clippy::too_many_lines,
        reason = "one real Stop fixture proves all original aliases, one effect, the terminal receipt barrier, retained-key replay, and Remove refund"
    )]
    async fn funded_stop_aliases_cross_the_old_count_ceiling_and_join_one_actual_result() {
        use ctxmux_protocol::{
            ClientFrame, ControlOutcome, ControlReceipt, RunEvent, RunSpec, ServerFrame,
            TerminalSize,
        };
        use futures_util::SinkExt as _;
        use std::{
            collections::BTreeMap,
            sync::{Arc, mpsc},
            time::Duration,
        };
        let server = super::super::tests::InProcessServer::start(Arc::new(
            super::super::RunManager::default(),
        ));
        let socket = server.directory.path().join("ctxmux.sock");
        let second_client = ctxmux_client::Client::new(&socket);
        let run = server
            .client
            .start(RunSpec {
                program: "/bin/sh".into(),
                args: vec![
                    "-c".into(),
                    "stty -echo; printf READY; exec /bin/cat".into(),
                ],
                cwd: None,
                env: BTreeMap::new(),
                initial_size: TerminalSize::default(),
                declared_inputs: Vec::new(),
            })
            .await
            .unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while second_client
            .status(run.id)
            .await
            .unwrap()
            .latest_output_bytes
            != 5
        {
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let operation = super::super::tests::fresh_stop(&second_client, run.id).await;
        let mut wire = raw_view(&socket, run.id, deadline).await;
        let control = server
            .manager
            .get(run.id)
            .unwrap()
            .native_control()
            .unwrap()
            .clone();
        let (held_tx, held_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let holder = std::thread::spawn(move || {
            control.with_metadata(|_, _| {
                held_tx.send(()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            });
        });
        held_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let budget = server.manager.native_input_drains.control_budget();
        let baseline = budget.used();
        // Regression population crosses the removed 64-alias characterization;
        // it is not a product capacity requirement. All original nodes fund
        // against the unchanged shared memory policy before admission.
        let aliases = 65;
        for index in 1..=aliases {
            let before = budget.used();
            wire.send(
                ctxmux_protocol::encode_frame(&ClientFrame::Stop {
                    command_id: AttachmentCommandId::new(index).unwrap(),
                    operation: operation.clone(),
                })
                .unwrap(),
            )
            .await
            .unwrap();
            while budget.used() <= before {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "every original alias is funded while control remains held"
                );
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
        let status = second_client.status(run.id).await.unwrap();
        assert_eq!((status.id, status.pid), (run.id, run.pid));
        assert!(
            status.state.is_running(),
            "a held before-effect owner has not stopped its real child"
        );
        release_tx.send(()).unwrap();
        holder.join().unwrap();
        let mut receipts = BTreeMap::new();
        let mut disposition = None;
        loop {
            match wire_frame(&mut wire, deadline).await {
                ServerFrame::CommandResult {
                    command_id,
                    outcome:
                        ControlOutcome::Accepted {
                            receipt:
                                ControlReceipt::Stop {
                                    disposition: actual,
                                },
                        },
                } => {
                    if let Some(expected) = disposition {
                        assert_eq!(actual, expected);
                    } else {
                        disposition = Some(actual);
                    }
                    assert!(receipts.insert(command_id.get(), actual).is_none());
                }
                ServerFrame::Event {
                    event: RunEvent::ServiceChanged { .. },
                } => {}
                ServerFrame::Event {
                    event: RunEvent::Exited { .. },
                } => {
                    assert_eq!(
                        receipts.len(),
                        aliases as usize,
                        "terminal barrier preserves every funded alias receipt"
                    );
                    break;
                }
                frame => panic!("unexpected Stop alias frame {frame:?}"),
            }
        }
        let retained_funding = budget.used();
        let retry = second_client.stop(operation).await.unwrap();
        assert_eq!(
            Some(retry.receipt.disposition),
            disposition,
            "same retained key replays the actual one Stop outcome"
        );
        let status = second_client.status(run.id).await.unwrap();
        assert_eq!((status.id, status.pid), (run.id, run.pid));
        assert!(!status.state.is_running());
        assert_eq!(status.applied_input_bytes, Some(0));
        assert_eq!(
            budget.used(),
            retained_funding,
            "same retained key does not allocate another Stop outcome"
        );
        assert!(
            retained_funding > baseline,
            "one authoritative Stop result remains funded while the Run is retained"
        );
        drop(wire);
        second_client.remove(run.id).await.unwrap();
        assert_eq!(
            budget.used(),
            baseline,
            "public collection releases the retained outcome and every original alias node"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(
        clippy::too_many_lines,
        reason = "one real byte-pressure fixture keeps the original stream, explicit discontinuity, and exact retained reattachment in one oracle"
    )]
    async fn one_byte_child_output_pressure_is_explicit_and_original_raw_bytes_reattach() {
        use ctxmux_protocol::{RunEvent, RunSpec, ServerFrame, TerminalSize};
        use futures_util::StreamExt as _;
        use std::{collections::BTreeMap, sync::Arc, time::Duration};
        let server = super::super::tests::InProcessServer::start(Arc::new(
            super::super::RunManager::default(),
        ));
        let socket = server.directory.path().join("ctxmux.sock");
        let second_client = ctxmux_client::Client::new(&socket);
        // Preserve the exact original failing workload, including dd's one-byte
        // output writes, independently of the input FIFO qualification.
        let original = [
            vec![b'a'; 16 * 1024 + 19],
            vec![b'b'; 16 * 1024 + 23],
            vec![3],
        ]
        .concat();
        let run = server
            .client
            .start(RunSpec {
                program: "/bin/sh".into(),
                args: vec![
                    "-c".into(),
                    format!(
                        "stty raw -echo; printf READY; exec /bin/dd bs=1 count={} 2>/dev/null",
                        original.len()
                    ),
                ],
                cwd: None,
                env: BTreeMap::new(),
                initial_size: TerminalSize::default(),
                declared_inputs: Vec::new(),
            })
            .await
            .unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while second_client
            .status(run.id)
            .await
            .unwrap()
            .latest_output_bytes
            != 5
        {
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let mut wire = raw_view(&socket, run.id, deadline).await;
        let receipt = second_client.input(run.id, original.clone()).await.unwrap();
        assert_eq!(receipt.receipt.written_bytes as usize, original.len());
        while second_client
            .status(run.id)
            .await
            .unwrap()
            .state
            .is_running()
        {
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let mut delivered = Vec::new();
        let mut pressure = false;
        loop {
            let line = tokio::time::timeout_at(deadline, wire.next())
                .await
                .unwrap();
            let Some(line) = line else {
                break;
            };
            let frame: ServerFrame = ctxmux_protocol::decode_frame(line.unwrap()).unwrap();
            match frame {
                ServerFrame::Event {
                    event: RunEvent::Output { chunk },
                } => {
                    assert!(
                        !pressure,
                        "output resumes through a new attachment after explicit discontinuity"
                    );
                    assert_eq!(chunk.start_byte, 5 + delivered.len() as u64);
                    delivered.extend_from_slice(&chunk.data);
                }
                ServerFrame::Event {
                    event: RunEvent::Gap { .. } | RunEvent::ObservationDiscontinuity,
                } => pressure = true,
                ServerFrame::Event {
                    event: RunEvent::ServiceChanged { .. },
                } => {}
                ServerFrame::Event {
                    event: RunEvent::Exited { .. },
                } => break,
                frame => panic!("unexpected one-byte pressure frame {frame:?}"),
            }
        }
        // A fast host may deliver all events without pressure. In either case
        // reconnect uses the public original-byte cursor and identity, never
        // treating a discontinuity as lost source bytes or cached availability.
        if !pressure {
            assert_eq!(delivered, original);
        }
        let (view, snapshot) = second_client.attach(run.id, 5).await.unwrap();
        assert_eq!((snapshot.run.id, snapshot.run.pid), (run.id, run.pid));
        assert!(!snapshot.run.state.is_running());
        assert_eq!(
            snapshot.run.applied_input_bytes,
            Some(original.len() as u64)
        );
        assert_eq!(
            snapshot.replay.latest_output_bytes,
            5 + original.len() as u64
        );
        assert_eq!(
            ctxmux_client::replay_bytes(&snapshot.replay.chunks),
            original,
            "explicit live-view pressure preserves all original raw source bytes"
        );
        drop(view);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(
        clippy::too_many_lines,
        reason = "one open/closed real socket fixture verifies original funded cancellation, before-effect results, uninspected Stop identity, and actual permit refund"
    )]
    async fn observation_fence_cancels_before_effect_and_releases_funding_on_send_failure() {
        use ctxmux_protocol::{CommandDisposition, ControlOutcome, RunSpec, ServerFrame};
        use std::sync::Arc;
        use std::time::Duration;

        let server = super::super::tests::InProcessServer::start(Arc::new(
            super::super::RunManager::default(),
        ));
        let info = server
            .client
            .start(RunSpec {
                program: "/bin/cat".into(),
                args: Vec::new(),
                cwd: None,
                env: std::collections::BTreeMap::new(),
                initial_size: ctxmux_protocol::TerminalSize::default(),
                declared_inputs: Vec::new(),
            })
            .await
            .unwrap();
        let run = server.manager.get(info.id).unwrap();
        let budget = server.manager.native_input_drains.control_budget();
        let baseline = budget.used();
        for close_peer in [false, true] {
            let super::super::UpgradeRequestAdmission::Execute(permit) =
                server.manager.upgrade_requests.admit()
            else {
                panic!("test gate is open");
            };
            let mut admissions = super::PendingAdmissions::new();
            super::queue_input_admission(
                &run,
                &mut admissions,
                AttachmentCommandId::new(1).unwrap(),
                b"original".to_vec(),
                permit,
            )
            .unwrap();
            assert!(budget.used() > baseline);
            assert_eq!(
                super::super::mutex_lock(&server.manager.upgrade_requests.inner.state).active,
                1
            );
            let (writer, reader) = tokio::net::UnixStream::pair().unwrap();
            let mut wire = tokio_util::codec::Framed::new(writer, super::super::codec());
            let mut peer = Some(tokio_util::codec::Framed::new(
                reader,
                super::super::codec(),
            ));
            if close_peer {
                drop(peer.take());
            }
            let mut controls = super::ControlState::default();
            let result = super::cancel_unadmitted_after_observation_close(
                &mut wire,
                &mut controls,
                &mut admissions,
            )
            .await;
            if close_peer {
                assert!(result.is_err(), "closed transport is a real failed send");
            } else {
                result.unwrap();
                let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
                assert!(matches!(
                    wire_frame(peer.as_mut().unwrap(), deadline).await,
                    ServerFrame::CommandResult {
                        command_id,
                        outcome: ControlOutcome::Rejected { failure },
                    } if command_id == AttachmentCommandId::new(1).unwrap()
                        && failure.disposition == CommandDisposition::NotApplied
                        && failure.error.code == ErrorCode::BackendUnavailable
                ));
                controls.last_command_id = Some(AttachmentCommandId::new(1).unwrap());
                assert!(
                    !super::reject_after_observation_close(
                        &mut wire,
                        &mut controls,
                        ctxmux_protocol::ClientFrame::Input {
                            command_id: AttachmentCommandId::new(2).unwrap(),
                            data: b"later".to_vec(),
                        },
                    )
                    .await
                    .unwrap()
                );
                assert!(matches!(
                    wire_frame(peer.as_mut().unwrap(), deadline).await,
                    ServerFrame::CommandResult {
                        command_id,
                        outcome: ControlOutcome::Rejected { failure },
                    } if command_id == AttachmentCommandId::new(2).unwrap()
                        && failure.disposition == CommandDisposition::NotApplied
                        && failure.error.code == ErrorCode::BackendUnavailable
                ));
                let stop = super::super::tests::fresh_stop(&server.client, info.id).await;
                assert!(
                    !super::reject_after_observation_close(
                        &mut wire,
                        &mut controls,
                        ctxmux_protocol::ClientFrame::Stop {
                            command_id: AttachmentCommandId::new(3).unwrap(),
                            operation: stop,
                        },
                    )
                    .await
                    .unwrap()
                );
                assert!(matches!(
                    wire_frame(peer.as_mut().unwrap(), deadline).await,
                    ServerFrame::CommandResult {
                        command_id,
                        outcome: ControlOutcome::Rejected { failure },
                    } if command_id == AttachmentCommandId::new(3).unwrap()
                        && failure.disposition == CommandDisposition::Unknown
                        && failure.error.code == ErrorCode::BackendUnavailable
                ));
            }
            assert!(admissions.is_empty());
            assert_eq!(
                budget.used(),
                baseline,
                "cancelled original payload is refunded"
            );
            assert_eq!(
                super::super::mutex_lock(&server.manager.upgrade_requests.inner.state).active,
                0
            );
            assert_eq!(
                server
                    .client
                    .status(info.id)
                    .await
                    .unwrap()
                    .applied_input_bytes,
                Some(0)
            );
        }
        assert_eq!(
            super::observation_closed_failure(true).disposition,
            CommandDisposition::Unknown
        );
        server
            .client
            .stop(super::super::tests::fresh_stop(&server.client, info.id).await)
            .await
            .unwrap();
    }

    #[test]
    fn restore_base64_chunks_fit_actual_codec_without_losing_original_bytes() {
        let original = vec![u8::MAX; ctxmux_protocol::MAX_FRAME_BYTES + 37];
        let bytes = super::terminal_checkpoint_chunk_bytes().unwrap();
        // The previous half-frame rule is legal for the actual base64 codec,
        // but leaves usable capacity idle. The derived grouping reaches its
        // real envelope boundary; adding one raw byte needs another group.
        assert!(bytes > ctxmux_protocol::MAX_FRAME_BYTES / 2);
        ctxmux_protocol::encode_frame(&ctxmux_protocol::ServerFrame::TerminalCheckpointChunk {
            offset: u64::MAX,
            data: original[..ctxmux_protocol::MAX_FRAME_BYTES / 2].to_vec(),
        })
        .unwrap();
        assert!(
            ctxmux_protocol::encode_frame(&ctxmux_protocol::ServerFrame::TerminalCheckpointChunk {
                offset: u64::MAX,
                data: original[..=bytes].to_vec(),
            })
            .is_err()
        );
        let mut restored = Vec::new();
        for (index, data) in original.chunks(bytes).enumerate() {
            let offset = (index * bytes) as u64;
            let encoded = ctxmux_protocol::encode_frame(
                &ctxmux_protocol::ServerFrame::TerminalCheckpointChunk {
                    offset,
                    data: data.to_vec(),
                },
            )
            .unwrap();
            assert!(encoded.len() <= ctxmux_protocol::MAX_FRAME_BYTES);
            let decoded: ctxmux_protocol::ServerFrame =
                ctxmux_protocol::decode_frame(encoded).unwrap();
            let ctxmux_protocol::ServerFrame::TerminalCheckpointChunk { offset, data } = decoded
            else {
                panic!("checkpoint transport retains frame identity");
            };
            assert_eq!(offset, restored.len() as u64);
            restored.extend_from_slice(&data);
        }
        assert_eq!(restored, original);
    }

    #[test]
    fn command_ids_start_at_one_advance_and_fail_closed() {
        let id = |value| AttachmentCommandId::new(value).expect("positive command id");
        let mut last = None;

        let first_error = observe_command_id(&mut last, id(2))
            .expect_err("first attachment command must use id one");
        assert_eq!(first_error.code, ErrorCode::InvalidRequest);
        assert!(last.is_none(), "rejected id does not advance the fence");

        observe_command_id(&mut last, id(1)).expect("accept first id");
        observe_command_id(&mut last, id(7)).expect("gaps are valid");
        for invalid in [7, 6] {
            let error = observe_command_id(&mut last, id(invalid))
                .expect_err("duplicate and backward ids fail closed");
            assert_eq!(error.code, ErrorCode::InvalidRequest);
            assert_eq!(last, Some(id(7)));
        }
        observe_command_id(&mut last, id(u32::MAX)).expect("accept maximum id");
        assert_eq!(last, Some(id(u32::MAX)));
    }
}
