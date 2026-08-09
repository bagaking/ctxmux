//! Replay join and correlated control delivery for one attachment connection.

use std::sync::Arc;

use ctxmux_protocol::{
    AttachedHeader, AttachedSnapshot, AttachmentCommandId, ClientFrame, ControlOutcome, ErrorCode,
    OutputChunk, OutputReplay, OutputReplayHeader, ProtocolError, RunEvent, RunId, RunState,
    ServerFrame, TerminalSize,
};
use futures_util::{FutureExt, StreamExt, future::BoxFuture, stream::FuturesUnordered};
use tokio::{net::UnixStream, sync::broadcast};
use tokio_util::codec::{Framed, LinesCodec};

#[cfg(test)]
use super::AttachmentHookPoint;
use super::{
    ConnectionError, ControlResult, Run, RunManager, STOP_ACK_TIMEOUT, invalid_request, receive,
    send,
};

pub(super) async fn handle(
    mut wire: Framed<UnixStream, LinesCodec>,
    manager: Arc<RunManager>,
    id: RunId,
    after_byte: u64,
) -> Result<(), ConnectionError> {
    let run = match manager.pin(id) {
        Ok(run) => run,
        Err(error) => {
            send(&mut wire, &ServerFrame::Error { error }).await?;
            return Ok(());
        }
    };
    let mut events = run.subscribe();
    #[cfg(test)]
    if let Some(hook) = &manager.attachment_hook {
        hook.pause_once(AttachmentHookPoint::AfterSubscribe).await;
    }
    let (_guard, snapshot) = run.attach(after_byte);
    let (header, replay_chunks, terminal_state) = split_snapshot(snapshot);
    let mut sent_through_byte = header.replay.latest_output_bytes;
    send(&mut wire, &ServerFrame::Attached { snapshot: header }).await?;
    send_replay(&mut wire, replay_chunks).await?;
    #[cfg(test)]
    if let Some(hook) = &manager.attachment_hook {
        hook.pause_once(AttachmentHookPoint::AfterSnapshot).await;
    }
    if !terminal_state.is_running() {
        send(
            &mut wire,
            &ServerFrame::Event {
                event: terminal_event(terminal_state),
            },
        )
        .await?;
        return Ok(());
    }

    let mut command_results = PendingResults::new();
    let mut controls = ControlState::default();
    loop {
        // Result-first bias is bounded by 1,024 input receipts plus one stop;
        // the explicit stop barrier, not select readiness, orders terminal exit.
        tokio::select! {
            biased;
            Some((command_id, outcome)) = command_results.next(), if !command_results.is_empty() => {
                send_command_result(&mut wire, command_id, outcome).await?;
                if controls.pending_stop == Some(command_id) {
                    controls.pending_stop = None;
                    if let Some(event) = controls.held_terminal.take() {
                        send(&mut wire, &ServerFrame::Event { event }).await?;
                        return Ok(());
                    }
                }
            }
            incoming = receive(&mut wire) => {
                let Some(frame) = incoming? else {
                    return Ok(());
                };
                if handle_frame(
                    &mut wire,
                    &manager,
                    &run,
                    &mut controls,
                    &mut command_results,
                    frame,
                ).await? {
                    return Ok(());
                }
            }
            event = events.recv() => {
                match event {
                    Ok(RunEvent::Output { chunk }) if chunk.end_byte <= sent_through_byte => {}
                    Ok(RunEvent::Output { chunk }) => {
                        sent_through_byte = chunk.end_byte;
                        send(&mut wire, &ServerFrame::Event {
                            event: RunEvent::Output { chunk },
                        }).await?;
                    }
                    Ok(event @ (RunEvent::Exited { .. } | RunEvent::Interrupted { .. })) => {
                        if controls.pending_stop.is_some() {
                            controls.held_terminal = Some(event);
                        } else {
                            send(&mut wire, &ServerFrame::Event { event }).await?;
                            return Ok(());
                        }
                    }
                    Ok(event) => send(&mut wire, &ServerFrame::Event { event }).await?,
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        let latest_output_bytes = run.info().latest_output_bytes;
                        sent_through_byte = latest_output_bytes;
                        send(&mut wire, &ServerFrame::Event {
                            event: RunEvent::Gap { latest_output_bytes },
                        }).await?;
                    }
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                }
            }
        }
    }
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
    pending_stop: Option<AttachmentCommandId>,
    held_terminal: Option<RunEvent>,
}

type PendingResults = FuturesUnordered<BoxFuture<'static, (AttachmentCommandId, ControlOutcome)>>;

enum ControlCommand {
    Input(Vec<u8>),
    Resize(TerminalSize),
    Stop,
}

async fn handle_frame(
    wire: &mut Framed<UnixStream, LinesCodec>,
    manager: &RunManager,
    run: &Arc<Run>,
    controls: &mut ControlState,
    results: &mut PendingResults,
    frame: ClientFrame,
) -> Result<bool, ConnectionError> {
    #[cfg(not(test))]
    let _ = manager;
    let (command_id, command) = match frame {
        ClientFrame::Input { command_id, data } => (command_id, ControlCommand::Input(data)),
        ClientFrame::Resize { command_id, size } => (command_id, ControlCommand::Resize(size)),
        ClientFrame::Stop { command_id } => (command_id, ControlCommand::Stop),
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

    match command {
        ControlCommand::Input(data) => match run.begin_input(data) {
            Ok(pending) => {
                results.push(async move { (command_id, outcome(pending.resolve().await)) }.boxed());
            }
            Err(failure) => {
                send_command_result(wire, command_id, ControlOutcome::Rejected { failure }).await?;
            }
        },
        ControlCommand::Resize(size) => {
            send_command_result(wire, command_id, outcome(run.resize(size))).await?;
        }
        ControlCommand::Stop => match run.begin_stop() {
            Ok(pending) => {
                controls.pending_stop = Some(command_id);
                results.push(
                    async move { (command_id, outcome(pending.resolve(STOP_ACK_TIMEOUT).await)) }
                        .boxed(),
                );
            }
            Err(failure) => {
                send_command_result(wire, command_id, ControlOutcome::Rejected { failure }).await?;
            }
        },
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

fn split_snapshot(snapshot: AttachedSnapshot) -> (AttachedHeader, Vec<OutputChunk>, RunState) {
    let AttachedSnapshot {
        run: run_info,
        replay,
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
        replay: OutputReplayHeader {
            first_available_byte,
            latest_output_bytes,
            truncated,
        },
    };
    (header, chunks, terminal_state)
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
