//! Actual public Run/Client facts after a shared-owner I/O failure.
//! Descriptor faults and the explicitly synthetic poll errno exist only here.
use super::*;
use ctxmux_client::{Client, ClientError, replay_bytes};
use ctxmux_protocol::{
    InputOperationKey, NativeInputPhase, NativeOutputStatus, NativeOwnerStatus, RecoverableInput,
    RunEvent, RunSpec, TerminalSize,
};
use futures_util::FutureExt;
use std::{os::fd::OwnedFd, path::Path};

#[derive(Clone, Copy)]
enum Fault {
    WakeEof,
    WakeRead,
    Poll,
}

fn inject(owner: &NativeRunOwner, directory: &Path, fault: Fault) -> NativeServiceFailure {
    let (reason, reader) = match fault {
        Fault::WakeEof => {
            let (reader, peer) = UnixStream::pair().unwrap();
            reader.set_nonblocking(true).unwrap();
            peer.shutdown(std::net::Shutdown::Write).unwrap();
            (
                NativeServiceFailure::OwnerIoFailed {
                    stage: NativeOwnerIoStage::WakeDrain,
                    os_error: None,
                },
                Some(reader),
            )
        }
        Fault::WakeRead => {
            // A normally owned, write-only regular FD cannot serve socket reads.
            // No raw close, aliasing or FD reuse is involved. The read's real OS
            // error is measured here and independently observed by the reactor.
            let file = File::create(directory.join("write-only-wake-reader")).unwrap();
            let mut reader = UnixStream::from(OwnedFd::from(file));
            reader.set_nonblocking(true).unwrap();
            let os_error = reader.read(&mut [0]).unwrap_err().raw_os_error();
            assert!(os_error.is_some_and(|errno| errno > 0));
            (
                NativeServiceFailure::OwnerIoFailed {
                    stage: NativeOwnerIoStage::WakeDrain,
                    os_error,
                },
                Some(reader),
            )
        }
        Fault::Poll => {
            // Same owner-loop error branch, explicitly a synthetic errno.
            *mutex_lock(&owner.inner.diagnostics.poll_error) = Some(Errno::BADF);
            (
                NativeServiceFailure::OwnerIoFailed {
                    stage: NativeOwnerIoStage::Poll,
                    os_error: Some(Errno::BADF.raw_os_error()),
                },
                None,
            )
        }
    };
    if let Some(reader) = reader {
        let commands = {
            let state = mutex_lock(&owner.inner.state);
            let OwnerState::Running { commands, .. } = &*state else {
                panic!("live owner before actual descriptor fault");
            };
            commands.clone()
        };
        owner.owner_wake().wake();
        commands
            .send(OwnerCommand::ReplaceWakeReaderForTest { reader })
            .unwrap();
        drop(commands);
    }
    owner.owner_wake().wake();
    reason
}

fn rejected(error: ClientError) -> ControlFailure {
    let ClientError::ControlRejected { failure } = error else {
        panic!("public typed control failure required: {error:?}");
    };
    failure
}

async fn stopped_event(
    view: &ctxmux_client::Attachment,
    mut revision: u64,
    reason: NativeServiceFailure,
) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let Some(RunEvent::ServiceChanged { service }) = view.next_event().await.unwrap()
            else {
                panic!("service fact must be publicly delivered");
            };
            assert!(service.revision > revision);
            revision = service.revision;
            if matches!(service.owner, NativeOwnerStatus::Stopped { .. }) {
                assert_eq!(service.owner, NativeOwnerStatus::Stopped { reason });
                break;
            }
        }
    })
    .await
    .unwrap();
}

#[allow(
    clippy::too_many_lines,
    reason = "one actual two-Run/two-Client fault proof keeps identity, service, byte and input-accounting oracles with guaranteed private cleanup"
)]
async fn public_failure(fault: Fault) {
    let manager = Arc::new(crate::RunManager::default());
    let server = crate::tests::InProcessServer::start(Arc::clone(&manager));
    let first = server.client.clone();
    let second = Client::new(server.directory.path().join("ctxmux.sock"));
    let mut runs = Vec::new();
    for name in ["a", "b"] {
        let info = first
            .start(RunSpec {
                program: "/bin/sh".to_owned(),
                args: vec![
                    "-c".to_owned(),
                    format!("stty raw -echo; printf 'READY-{name}\\000\\377'; exec /bin/sleep 30"),
                ],
                cwd: None,
                env: std::collections::BTreeMap::new(),
                initial_size: TerminalSize { rows: 4, cols: 12 },
                declared_inputs: Vec::new(),
            })
            .await
            .unwrap();
        let run = manager.get(info.id).unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while second.status(info.id).await.unwrap().latest_output_bytes < 9 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        runs.push((info, run));
    }
    let (a, a_snapshot) = first.attach(runs[0].0.id, 0).await.unwrap();
    let (b, b_snapshot) = second.attach(runs[1].0.id, 0).await.unwrap();
    assert_eq!(replay_bytes(&a_snapshot.replay.chunks), b"READY-a\0\xff");
    assert_eq!(replay_bytes(&b_snapshot.replay.chunks), b"READY-b\0\xff");
    assert_ne!(runs[0].0.id, runs[1].0.id);
    assert_ne!(runs[0].0.pid, runs[1].0.pid);
    first.input(runs[0].0.id, vec![0x41]).await.unwrap();
    let runtime = second.runtime_info().await.unwrap();
    assert_eq!(first.runtime_info().await.unwrap(), runtime);
    let operation = RecoverableInput {
        daemon_instance: runtime.daemon_instance_id,
        operation_key: InputOperationKey::new("owner-io-partial").unwrap(),
        id: runs[1].0.id,
        expected_byte: 0,
        // Existing actual-PTY pressure workload, not a production capacity cap.
        data: vec![0x61; 128 * 1024 + 3],
    };
    let request = operation.clone();
    let sender = second.clone();
    let pending = tokio::spawn(async move { sender.recoverable_input(request).await });
    let Some(crate::RunControl::Native(control)) = &runs[1].1.incarnation_control else {
        panic!("actual Native control");
    };
    let earlier_confirmed = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            // Observe the public snapshot without repeatedly borrowing the
            // physical input-owner lock that the reactor needs to progress.
            let input = second
                .status(runs[1].0.id)
                .await
                .unwrap()
                .native_service
                .unwrap()
                .input;
            if input.write_blocked && input.active_confirmed_bytes > 0 {
                break input.active_confirmed_bytes;
            }
        }
    })
    .await
    .unwrap();
    assert!(earlier_confirmed < operation.data.len());
    let (prefix_tx, prefix_rx) = mpsc::channel();
    let input = control.clone();
    // Preserve the exact final physical prefix, not a possibly earlier EAGAIN
    // sample whose value can increase before the injected I/O fault is handled.
    *mutex_lock(&manager.native_runs.inner.diagnostics.before_completion) =
        Some(Box::new(move || {
            prefix_tx
                .send(input.input_service().active_confirmed_bytes)
                .unwrap();
        }));
    let reason = inject(&manager.native_runs, server.directory.path(), fault);
    tokio::time::timeout(Duration::from_secs(2), async {
        while !manager
            .native_runs
            .inner
            .completion_finished
            .load(Ordering::Acquire)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let confirmed = prefix_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert!(confirmed >= earlier_confirmed && confirmed < operation.data.len());
    // Preserve the original failed oracle while always retiring this fixture's
    // actual retained children. A source inverse must not leak private Runs.
    let outcome = AssertUnwindSafe(async {
        stopped_event(&a, a_snapshot.run.native_service.unwrap().revision, reason).await;
        stopped_event(&b, b_snapshot.run.native_service.unwrap().revision, reason).await;
        let partial = rejected(
            tokio::time::timeout(Duration::from_secs(2), pending)
                .await
                .unwrap()
                .unwrap()
                .unwrap_err(),
        );
        assert_eq!(partial.disposition, CommandDisposition::Unknown);
        assert_eq!(partial.confirmed_input_bytes, Some(confirmed));
        assert_eq!(
            rejected(second.recoverable_input(operation).await.unwrap_err()),
            partial,
            "same-key recovery must preserve uncertainty without another write"
        );
        let fresh = rejected(first.input(runs[0].0.id, vec![0x42]).await.unwrap_err());
        assert_eq!(fresh.disposition, CommandDisposition::NotApplied);
        assert_eq!(fresh.confirmed_input_bytes, None);
        for (index, (original, run)) in runs.iter().enumerate() {
            for client in [&first, &second] {
                let info = client.status(original.id).await.unwrap();
                assert_eq!((info.id, info.pid), (original.id, original.pid));
                assert_eq!(info.state, RunState::Running);
                assert_eq!(info.latest_output_bytes, 9);
                assert_eq!(info.applied_input_bytes, Some(u64::from(index == 0)));
                let service = info.native_service.unwrap();
                assert_eq!(service.owner, NativeOwnerStatus::Stopped { reason });
                assert_eq!(service.output, NativeOutputStatus::Unavailable { reason });
                assert_eq!(
                    service.input.phase,
                    NativeInputPhase::Unavailable { reason }
                );
                assert_eq!(service.input.unsettled_request_bytes, 0);
                assert_eq!(service.input.unsettled_commands, 0);
                let (view, replay) = client.attach(original.id, 0).await.unwrap();
                assert_eq!(
                    replay_bytes(&replay.replay.chunks),
                    if index == 0 {
                        b"READY-a\0\xff"
                    } else {
                        b"READY-b\0\xff"
                    }
                );
                view.detach().await.unwrap();
            }
            let session =
                crate::native_session::NativeSession::from_child_pid(original.pid.unwrap())
                    .unwrap();
            assert!(
                !session.leader_is_terminal().unwrap(),
                "original child survives"
            );
            let Some(crate::RunControl::Native(control)) = &run.incarnation_control else {
                panic!("actual Native control");
            };
            assert!(control.wait_authority_failure().is_none());
        }
        a.detach().await.unwrap();
        b.detach().await.unwrap();
    })
    .catch_unwind()
    .await;
    for (original, run) in &runs {
        let Some(crate::RunControl::Native(control)) = &run.incarnation_control else {
            panic!("actual retained Native control");
        };
        let mut session =
            crate::native_session::NativeSession::from_child_pid(original.pid.unwrap()).unwrap();
        control
            .cleanup_retained_owner_child_for_test(&mut session)
            .unwrap();
    }
    if let Err(error) = outcome {
        std::panic::resume_unwind(error);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_wake_eof_is_public_without_inventing_an_os_error() {
    public_failure(Fault::WakeEof).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_wake_read_error_is_public_with_its_actual_os_errno() {
    public_failure(Fault::WakeRead).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn synthetic_poll_errno_uses_the_same_public_owner_completion() {
    public_failure(Fault::Poll).await;
}
