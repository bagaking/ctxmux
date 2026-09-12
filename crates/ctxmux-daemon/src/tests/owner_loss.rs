use super::creation::creation_hook;
use super::*;
use crate::persistence::ActorLossPoint;

fn cat_spec() -> RunSpec {
    RunSpec {
        program: "/bin/sh".to_owned(),
        args: vec![
            "-c".to_owned(),
            "stty -echo -onlcr; printf 'ready\\n'; exec cat".to_owned(),
        ],
        ..long_running_spec()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_actor_keeps_original_effects_and_two_native_runs_available() {
    for point in [
        ActorLossPoint::BeforeStartReady,
        ActorLossPoint::BeforeStartCommit,
        ActorLossPoint::AfterStartCommit,
    ] {
        owner_loss_case(Some(point), CreationHookPoint::AfterSpawn).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn publication_unwind_keeps_original_child_and_two_native_runs_available() {
    for point in [
        CreationHookPoint::PanicAfterPersistentCommit,
        CreationHookPoint::PanicBeforePersistentRegistryConsume,
    ] {
        owner_loss_case(None, point).await;
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "one real fault lifecycle proves original effects, two independent controls, and cold durable bytes"
)]
async fn owner_loss_case(loss: Option<ActorLossPoint>, point: CreationHookPoint) {
    let directory = tempfile::tempdir().unwrap();
    let state = directory.path().join("state");
    let (persistence, recovered) = Persistence::open(&state).unwrap();
    let (hook, mut reached) = creation_hook(point, false);
    let manager = Arc::new(RunManager {
        creation_hook: Some(Arc::clone(&hook)),
        ..RunManager::persistent(persistence.clone(), recovered)
    });
    let server = InProcessServer::start(Arc::clone(&manager));
    let first = server.client.start(cat_spec()).await.unwrap();
    let second = server.client.start(cat_spec()).await.unwrap();
    expect_original_raw(&server.client, &first, b"ready\n").await;
    expect_original_raw(&server.client, &second, b"ready\n").await;
    persistence.barrier().unwrap();
    let old_head = server
        .client
        .observe_storage(first.id)
        .await
        .unwrap()
        .1
        .persistence
        .unwrap()
        .committed_output_bytes;
    assert_eq!(old_head, 6);
    if let Some(loss) = loss {
        persistence.lose_actor_at_for_test(loss);
    } else {
        hook.arm();
    }
    let key = CreateOperationKey::new("owner-loss-original").unwrap();
    let client = server.client.clone();
    let request_key = key.clone();
    let request = tokio::spawn(async move {
        client
            .start_with_operation_key(cat_spec(), request_key)
            .await
    });
    if loss.is_none() {
        tokio::time::timeout(Duration::from_secs(5), reached.recv())
            .await
            .unwrap()
            .unwrap();
        hook.release();
    }
    let failure = tokio::time::timeout(Duration::from_secs(5), request)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    let ClientError::Protocol {
        creation_failure: Some(creation),
        ..
    } = failure
    else {
        panic!("original unknown operation facts survive the public boundary");
    };
    assert_eq!(creation.operation_key, key);
    assert_eq!(creation.disposition, CommandDisposition::Unknown);
    let original = {
        let pending = mutex_lock(&manager.pending_persistence);
        assert_eq!(pending.len(), 1);
        let crate::PendingPersistence::Fenced { pending, .. } = &pending[0] else {
            panic!("lost publication remains owned without a recovery worker")
        };
        pending.as_ref().map(|pending| Arc::clone(pending.run()))
    };
    assert_eq!(
        original.is_some(),
        !matches!(loss, Some(ActorLossPoint::BeforeStartReady)),
        "spawned original Run never loses its publication owner"
    );
    assert_eq!(
        hook.physical_spawn_count(),
        if original.is_some() { 3 } else { 2 }
    );
    assert_eq!(manager.creation_flights.active_count(), 1);
    assert!(
        !manager.creation_flights.is_fenced(),
        "durability failure does not revoke Native creation ownership"
    );
    assert!(
        manager.incarnation_failure.message().is_none(),
        "durability failure never becomes Native authority loss"
    );
    if let Some(original) = &original {
        assert!(
            process_exists(original.info().pid.unwrap()),
            "lost publication preserves its original child"
        );
    }
    for _ in 0..3 {
        assert!(matches!(
            server.client.observe_creation(key.clone()).await,
            Err(ClientError::Protocol {
                code: ErrorCode::BackendUnavailable,
                ..
            })
        ));
        manager.advance_pending_persistence();
    }
    for (run, bytes) in [
        (&first, b"first\n".as_slice()),
        (&second, b"second\n".as_slice()),
    ] {
        assert_eq!(server.client.status(run.id).await.unwrap().pid, run.pid);
        server.client.input(run.id, bytes.to_vec()).await.unwrap();
        let mut expected = b"ready\n".to_vec();
        expected.extend_from_slice(bytes);
        expect_original_raw(&server.client, run, &expected).await;
        let observation = server
            .client
            .observe_storage(run.id)
            .await
            .unwrap()
            .1
            .persistence
            .unwrap();
        assert_eq!(observation.actor_stopped, loss.is_some());
        if loss.is_some() {
            assert_eq!(observation.committed_output_bytes, old_head);
        }
    }
    assert!(
        Persistence::open(&state).is_err(),
        "Native lifetime keeps the original state namespace exclusive after actor loss"
    );
    assert_eq!(
        hook.physical_spawn_count(),
        if original.is_some() { 3 } else { 2 }
    );
    if loss.is_none() {
        let independent = server.client.start(cat_spec()).await.unwrap();
        assert_eq!(
            hook.physical_spawn_count(),
            4,
            "one authorized independent Start, no original replay"
        );
        server
            .client
            .stop(fresh_stop(&server.client, independent.id).await)
            .await
            .unwrap();
    }
    for run in [&first, &second] {
        server
            .client
            .stop(fresh_stop(&server.client, run.id).await)
            .await
            .unwrap();
        assert!(
            !process_exists(run.pid.unwrap()),
            "Native Stop remains usable without a durable actor"
        );
    }
    if let Some(original) = &original {
        assert!(
            process_exists(original.info().pid.unwrap()),
            "original child survives while independent Native controls complete"
        );
    }
    let original_identity = original.as_ref().map(|run| run.info());
    let before = Instant::now();
    let shutdown = manager
        .shutdown_owned_controls(Duration::from_millis(150))
        .unwrap_err();
    assert!(
        shutdown
            .to_string()
            .contains("original Run creation or durable mutation")
    );
    assert!(
        before.elapsed() < Duration::from_secs(2),
        "unresolved shutdown honors its existing finite deadline"
    );
    let original_pid = original.as_ref().and_then(|run| run.info().pid);
    drop(original);
    hook.release_captured_runs();
    server.abort_and_wait().await;
    drop(manager);
    if let Some(pid) = original_pid {
        tokio::time::timeout(Duration::from_secs(5), async {
            while process_exists(pid) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
    persistence.assert_exclusive_owner();
    drop(persistence);
    let (_cold, recovered) = Persistence::open(&state).unwrap();
    let saved_original = recovered.iter().find(|saved| saved.operation_key == key);
    if matches!(loss, None | Some(ActorLossPoint::AfterStartCommit)) {
        assert_eq!(
            saved_original.unwrap().info.id,
            original_identity.unwrap().id
        );
    } else {
        assert!(
            saved_original.is_none(),
            "an uncommitted original has no invented durable Run"
        );
    }
    for run in [&first, &second] {
        let saved = recovered
            .iter()
            .find(|saved| saved.info.id == run.id)
            .unwrap();
        assert!(
            replay_bytes(&saved.replay.chunks).starts_with(b"ready\n"),
            "previous durable bytes survive owner loss"
        );
    }
}
