use super::creation::creation_hook;
use super::*;
use ctxmux_protocol::{CreationObservation, StorageTransactionOutcome};

fn shell(command: &str) -> RunSpec {
    RunSpec {
        program: "/bin/sh".to_owned(),
        args: vec!["-c".to_owned(), command.to_owned()],
        ..long_running_spec()
    }
}

async fn observe_original(client: &Client, key: &CreateOperationKey) -> RunInfo {
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            match client.observe_creation(key.clone()).await {
                Ok((_, CreationObservation::Retained { run, .. })) => return *run,
                Err(ClientError::Protocol {
                    code: ErrorCode::BackendUnavailable,
                    ..
                }) => {}
                other => panic!("original creation identity was lost: {other:?}"),
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("original creation settles without another Start")
}

async fn wait_for_probes(persistence: &Persistence) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while persistence.start_commit_probe_attempts_for_test() < 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("same owner retries its exact witness while unavailable");
}

fn original_pending(manager: &RunManager) -> Arc<Run> {
    let pending = mutex_lock(&manager.pending_persistence);
    assert_eq!(pending.len(), 1);
    let crate::PendingPersistence::Creation(original) = &pending[0] else {
        panic!("spawned original continuation is retained");
    };
    Arc::clone(original.pending.run())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_start_old_and_new_settle_original_live_and_terminal_runs() {
    for committed in [false, true] {
        for ended in [false, true] {
            original_start_case(committed, ended).await;
        }
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "one real fault-to-publication lifecycle proves original effects, controls and cold state"
)]
async fn original_start_case(committed: bool, ended: bool) {
    let directory = tempfile::tempdir().unwrap();
    let state = directory.path().join("state");
    let (persistence, recovered) = Persistence::open(&state).unwrap();
    let (hook, _) = creation_hook(CreationHookPoint::AfterSpawn, false);
    let manager = Arc::new(RunManager {
        creation_hook: Some(Arc::clone(&hook)),
        ..RunManager::persistent(persistence.clone(), recovered)
    });
    let server = InProcessServer::start(Arc::clone(&manager));
    let parent = server
        .client
        .start(shell("stty -echo -onlcr; printf 'ready\\n'; exec cat"))
        .await
        .unwrap();
    expect_original_raw(&server.client, &parent, b"ready\n").await;
    persistence.hold_start_commit_probe_for_test(committed);
    let key = CreateOperationKey::new("original-unconfirmed").unwrap();
    let spec = shell(if ended {
        "stty -echo -onlcr; printf 'original\\n'; exit 7"
    } else {
        "stty -echo -onlcr; printf 'original\\n'; exec cat"
    });
    let error = server
        .client
        .start_with_operation_key(spec, key.clone())
        .await
        .unwrap_err();
    let ClientError::Protocol {
        creation_failure: Some(creation),
        storage_failure: Some(storage),
        ..
    } = error
    else {
        panic!("original effect and storage facts must reach the public client");
    };
    assert_eq!(creation.operation_key, key);
    assert_eq!(creation.disposition, CommandDisposition::Unknown);
    assert_eq!(
        storage.transaction_outcome,
        StorageTransactionOutcome::Unknown
    );
    wait_for_probes(&persistence).await;
    let original = original_pending(&manager);
    let identity = original.info();
    let pid = identity.pid.unwrap();
    assert_eq!(hook.physical_spawn_count(), 2);
    assert_eq!(manager.creation_flights.active_count(), 1);
    assert!(!manager.creation_flights.is_fenced());
    assert!(manager.incarnation_failure.message().is_none());
    assert!(matches!(
        server.client.observe_creation(key.clone()).await,
        Err(ClientError::Protocol {
            code: ErrorCode::BackendUnavailable,
            ..
        })
    ));
    server
        .client
        .input(parent.id, b"healthy\n".to_vec())
        .await
        .unwrap();
    expect_original_raw(&server.client, &parent, b"ready\nhealthy\n").await;
    if ended {
        tokio::time::timeout(Duration::from_secs(5), async {
            while process_exists(pid) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    } else {
        assert!(
            process_exists(pid),
            "unknown commit never kills the original healthy child"
        );
    }
    persistence.release_start_commit_probe_for_test();
    let observed = observe_original(&server.client, &key).await;
    assert_eq!((observed.id, observed.pid), (identity.id, identity.pid));
    expect_original_raw(&server.client, &observed, b"original\n").await;
    if ended {
        assert!(matches!(
            observed.native_service.as_ref().unwrap().input.phase,
            ctxmux_protocol::NativeInputPhase::Closed {}
        ));
        // RunState is the durable terminal-publication contract. The already
        // closed Native input lane does not wait for that storage receipt.
        let terminal = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let current = server.client.status(observed.id).await.unwrap();
                if !current.state.is_running() {
                    break current;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            terminal.state,
            RunState::Exited {
                code: 7,
                signal: None
            }
        );
    } else {
        assert!(observed.state.is_running());
        server
            .client
            .input(observed.id, b"continued\n".to_vec())
            .await
            .unwrap();
        expect_original_raw(&server.client, &observed, b"original\ncontinued\n").await;
        server
            .client
            .stop(fresh_stop(&server.client, observed.id).await)
            .await
            .unwrap();
    }
    assert_eq!(
        hook.physical_spawn_count(),
        2,
        "reconciliation does not spawn"
    );
    server
        .client
        .stop(fresh_stop(&server.client, parent.id).await)
        .await
        .unwrap();
    manager
        .shutdown_owned_controls(Duration::from_secs(5))
        .unwrap();
    assert_eq!(manager.creation_flights.active_count(), 0);
    assert!(mutex_lock(&manager.pending_persistence).is_empty());
    let runtime = manager.runtime_id;
    drop(original);
    server.abort_and_wait().await;
    drop(manager);
    persistence.barrier().unwrap();
    drop(persistence);
    let (cold, recovered) = Persistence::open(&state).unwrap();
    assert_eq!(cold.runtime_id(), runtime);
    let retained = recovered
        .iter()
        .find(|item| item.operation_key == key)
        .unwrap();
    assert_eq!(
        (retained.info.id, retained.info.pid),
        (identity.id, identity.pid)
    );
    assert!(!retained.info.state.is_running());
    assert_eq!(
        replay_bytes(&retained.replay.chunks),
        if ended {
            b"original\n".as_slice()
        } else {
            b"original\ncontinued\n".as_slice()
        }
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_keeps_original_pending_child_and_never_reopens_admission() {
    let directory = tempfile::tempdir().unwrap();
    let (persistence, recovered) = Persistence::open(directory.path().join("state")).unwrap();
    let manager = Arc::new(RunManager::persistent(persistence.clone(), recovered));
    let server = InProcessServer::start(Arc::clone(&manager));
    persistence.hold_start_commit_probe_for_test(true);
    let key = CreateOperationKey::new("shutdown-original").unwrap();
    server
        .client
        .start_with_operation_key(
            shell("stty -echo -onlcr; printf 'held\\n'; exec cat"),
            key.clone(),
        )
        .await
        .unwrap_err();
    let original = original_pending(&manager);
    let id = original.id;
    let shutdown_manager = Arc::clone(&manager);
    let shutdown = tokio::task::spawn_blocking(move || {
        shutdown_manager.shutdown_owned_controls(Duration::from_secs(8))
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while !manager.creation_flights.is_fenced() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    persistence.release_start_commit_probe_for_test();
    shutdown.await.unwrap().unwrap();
    let observed = observe_original(&server.client, &key).await;
    assert_eq!(observed.id, id);
    assert!(
        !observed.state.is_running(),
        "cleanup outcome is published, never revived"
    );
    assert!(manager.creation_flights.is_fenced());
    assert!(matches!(
        server.client.start(long_running_spec()).await,
        Err(ClientError::Protocol {
            code: ErrorCode::BackendUnavailable,
            ..
        })
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_spawn_abort_recovers_its_original_admission_without_stopping_neighbor() {
    let directory = tempfile::tempdir().unwrap();
    let (persistence, recovered) = Persistence::open(directory.path().join("state")).unwrap();
    let manager = Arc::new(RunManager::persistent(persistence.clone(), recovered));
    let server = InProcessServer::start(Arc::clone(&manager));
    let parent = server
        .client
        .start(shell("stty -echo -onlcr; printf 'ready\\n'; exec cat"))
        .await
        .unwrap();
    expect_original_raw(&server.client, &parent, b"ready\n").await;
    persistence.hold_original_rollback_for_test();
    let key = CreateOperationKey::new("failed-before-exec").unwrap();
    let error = server
        .client
        .start_with_operation_key(
            RunSpec {
                program: directory
                    .path()
                    .join("no-such-program")
                    .to_string_lossy()
                    .into_owned(),
                ..long_running_spec()
            },
            key.clone(),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, ClientError::Protocol { storage_failure: Some(ref storage), .. }
        if storage.transaction_outcome == StorageTransactionOutcome::Unknown)
    );
    assert!(matches!(
        &mutex_lock(&manager.pending_persistence)[0],
        crate::PendingPersistence::Admission { .. }
    ));
    assert!(manager.incarnation_failure.message().is_none());
    assert!(!manager.creation_flights.is_fenced());
    assert!(matches!(
        server.client.observe_creation(key.clone()).await,
        Err(ClientError::Protocol {
            code: ErrorCode::BackendUnavailable,
            ..
        })
    ));
    server
        .client
        .input(parent.id, b"healthy\n".to_vec())
        .await
        .unwrap();
    expect_original_raw(&server.client, &parent, b"ready\nhealthy\n").await;
    persistence.release_original_rollback_for_test();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok((_, CreationObservation::NotRetained { .. })) =
                server.client.observe_creation(key.clone()).await
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("proven old abort releases only its original reservation");
    assert_eq!(manager.creation_flights.active_count(), 0);
    assert_eq!(
        (server.client.status(parent.id).await.unwrap()).pid,
        parent.pid
    );
    let later = server.client.start(shell("exit 0")).await.unwrap();
    assert_ne!(later.id, parent.id);
    server
        .client
        .stop(fresh_stop(&server.client, parent.id).await)
        .await
        .unwrap();
}

/// The actual serving loop is exercised in its own process by the SDK/CLI test.
#[test]
fn public_recovery_subprocess() {
    let Some(directory) = std::env::var_os("CTXMUX_CREATION_RECOVERY_DIR") else {
        return;
    };
    let directory = std::path::PathBuf::from(directory);
    let committed = std::env::var_os("CTXMUX_CREATION_RECOVERY_OLD").is_none();
    tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap().block_on(async {
        let (persistence, recovered) = Persistence::open(directory.join("state")).unwrap();
        let (hook, _) = creation_hook(CreationHookPoint::AfterSpawn, false);
        let manager = Arc::new(RunManager { creation_hook: Some(Arc::clone(&hook)), ..RunManager::persistent(persistence.clone(), recovered) });
        let server = InProcessServer::start(Arc::clone(&manager));
        let parent = server.client.start(shell("stty -echo -onlcr; printf 'ready\\n'; exec cat")).await.unwrap();
        expect_original_raw(&server.client, &parent, b"ready\n").await;
        persistence.hold_start_commit_probe_for_test(committed);
        std::fs::write(directory.join("ready.json"), serde_json::to_vec(&serde_json::json!({"socketPath":server.directory.path().join("ctxmux.sock"),"parent":parent})).unwrap()).unwrap();
        tokio::time::timeout(Duration::from_secs(45), async {
            while !directory.join("release").exists() { tokio::time::sleep(Duration::from_millis(10)).await; }
        }).await.unwrap();
        let attempts = persistence.start_commit_probe_attempts_for_test();
        persistence.release_start_commit_probe_for_test();
        std::fs::write(directory.join("released.json"), serde_json::to_vec(&serde_json::json!({"probeAttempts":attempts})).unwrap()).unwrap();
        tokio::time::timeout(Duration::from_secs(45), async {
            while !directory.join("finish").exists() { tokio::time::sleep(Duration::from_millis(10)).await; }
        }).await.unwrap();
        for run in manager.list_all() {
            if run.state.is_running() { server.client.stop(fresh_stop(&server.client, run.id).await).await.unwrap(); }
        }
        manager.shutdown_owned_controls(Duration::from_secs(5)).unwrap();
        assert_eq!(hook.physical_spawn_count(), 2, "one healthy neighbor and one original child only");
        persistence.barrier().unwrap();
    });
}
