use super::*;
use crate::creation::RunRegistry;
use crate::native_control::InputDrainGate;
use ctxmux_protocol::{ForkFidelity, RunId, RunInfo, RunLineage};
use std::collections::{HashSet, VecDeque};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn daemon_shutdown_fences_and_drains_a_cancelled_creation_owner() {
    let (server, hook, mut reached) = creation_hooked_server();
    let manager = Arc::clone(&server.manager);
    let marker = server.directory.path().join("shutdown-creation-starts.log");
    let (operation_key, same_stripe_key) = colliding_operation_keys(&manager);
    let spec = marker_spec(&marker, true);
    let request_manager = Arc::clone(&manager);
    let request_key = operation_key.clone();
    let request_spec = spec.clone();
    let requester = tokio::spawn(async move {
        request_manager
            .create(request_key, CreationRequest::Start { spec: request_spec })
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), reached.recv())
        .await
        .expect("creation reaches the post-publication owner barrier")
        .expect("creation barrier remains connected");
    let original = manager
        .list()
        .into_iter()
        .next()
        .expect("the active creation published one Run");
    let same_stripe_marker = server.directory.path().join("shutdown-stripe-waiter.log");
    let mut same_stripe_waiter = Box::pin(manager.create(
        same_stripe_key,
        CreationRequest::Start {
            spec: marker_spec(&same_stripe_marker, false),
        },
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(20), same_stripe_waiter.as_mut())
            .await
            .is_err(),
        "same-stripe unbound request waits behind the active owner"
    );

    requester.abort();
    assert!(
        requester
            .await
            .expect_err("the requester loses its response")
            .is_cancelled()
    );

    let shutdown_manager = Arc::clone(&manager);
    let shutdown = tokio::task::spawn_blocking(move || {
        shutdown_manager.shutdown_owned_controls(Duration::from_secs(5))
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while !manager.creation_flights.is_fenced() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("shutdown fences creation before waiting for its active owner");

    assert!(
        !shutdown.is_finished(),
        "shutdown returned while the cancelled request's creation owner was active"
    );

    hook.release();

    let same_stripe_error = same_stripe_waiter
        .await
        .expect_err("shutdown rejects the pre-fence unbound stripe waiter");
    assert_eq!(same_stripe_error.code, ErrorCode::BackendUnavailable);
    assert_eq!(manager.list().len(), 1, "stripe waiter spawned no Run");
    shutdown
        .await
        .expect("shutdown owner task remains live")
        .expect("creation and tmux owners drain within the shared deadline");
    let retried = manager
        .create(operation_key, CreationRequest::Start { spec })
        .await
        .expect("the retained mapping remains resolvable after the fence");
    assert_eq!((retried.id, retried.pid), (original.id, original.pid));
    assert_eq!(
        wait_for_marker_pids(&marker, 1).await,
        vec![original.pid.expect("native Run has one PID")]
    );
    manager
        .get(original.id)
        .expect("resolve shutdown fixture Run")
        .stop()
        .await
        .expect("stop shutdown fixture Run");
    wait_for_run_terminal_async(&manager.get(original.id).unwrap()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_only_output_does_not_take_durable_transition_locks() {
    let run = Run::spawn(
        long_running_spec(),
        None,
        PersistenceMode::MemoryOnly,
        LIVE_EVENT_CAPACITY,
        InputDrainGate::default(),
    )
    .expect("spawn memory-only output fixture");
    let transition = mutex_lock(&run.persistence_transition);
    let state = mutex_lock(&run.state);
    let persistence = mutex_lock(&run.persistence);
    let output_run = Arc::clone(&run);
    let (completed_tx, completed_rx) = std::sync::mpsc::sync_channel(0);
    let output = std::thread::spawn(move || {
        output_run.record_output(b"memory-only-fast-path".to_vec());
        completed_tx
            .send(())
            .expect("report memory-only output completion");
    });

    completed_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("memory-only output bypasses durable transition locks");
    drop(persistence);
    drop(state);
    drop(transition);
    output.join().expect("memory-only output worker joins");
    assert_eq!(
        replay_bytes(&mutex_lock(&run.output).replay(0).chunks),
        b"memory-only-fast-path"
    );

    run.stop().await.expect("stop memory-only output fixture");
    wait_for_run_terminal(&run);
    wait_for_direct_run_workers(&run);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memory_capacity_rejects_before_spawn_then_replaces_one_quiescent_terminal_run() {
    let manager = Arc::new(RunManager {
        registry: RunRegistry::with_record_capacity(1),
        ..RunManager::default()
    });
    let temp = tempfile::tempdir().expect("create memory capacity fixture");
    let first_marker = temp.path().join("first.log");
    let rejected_marker = temp.path().join("rejected.log");
    let replacement_marker = temp.path().join("replacement.log");
    let first_key = CreateOperationKey::new("memory-capacity-first").unwrap();
    let first_spec = marker_spec(&first_marker, true);
    let first = manager
        .create(
            first_key.clone(),
            CreationRequest::Start {
                spec: first_spec.clone(),
            },
        )
        .await
        .expect("fill the one-record Registry");
    assert_eq!(wait_for_marker_pids(&first_marker, 1).await.len(), 1);

    let mut invalid_spec = marker_spec(&rejected_marker, false);
    invalid_spec.program.clear();
    let invalid_start = manager
        .create(
            CreateOperationKey::new("memory-capacity-invalid-start").unwrap(),
            CreationRequest::Start {
                spec: invalid_spec.clone(),
            },
        )
        .await
        .expect_err("invalid Start is rejected before Registry capacity admission");
    assert_eq!(invalid_start.code, ErrorCode::InvalidRequest);
    let invalid_level_b = manager
        .create(
            CreateOperationKey::new("memory-capacity-invalid-level-b").unwrap(),
            CreationRequest::Fork {
                parent: first.id,
                plan: ForkPlan::LevelB { spec: invalid_spec },
            },
        )
        .await
        .expect_err("invalid Level B materialization precedes Registry capacity admission");
    assert_eq!(invalid_level_b.code, ErrorCode::InvalidRequest);
    assert!(read_marker_pids(&rejected_marker).is_empty());

    let rejected = manager
        .create(
            CreateOperationKey::new("memory-capacity-rejected").unwrap(),
            CreationRequest::Start {
                spec: marker_spec(&rejected_marker, false),
            },
        )
        .await
        .expect_err("a live retained Run cannot fund replacement");
    assert_eq!(rejected.code, ErrorCode::RunCapacity);
    assert!(read_marker_pids(&rejected_marker).is_empty());
    assert_eq!(manager.list().len(), 1);

    stop_run_and_wait(&manager, first.id).await;
    wait_for_run_workers(&manager).await;

    let terminal_pin = manager
        .get(first.id)
        .expect("pin the terminal candidate across admission");
    let pinned_rejection = manager
        .create(
            CreateOperationKey::new("memory-capacity-pinned").unwrap(),
            CreationRequest::Start {
                spec: marker_spec(&rejected_marker, false),
            },
        )
        .await
        .expect_err("a pinned terminal Run cannot be collected");
    assert_eq!(pinned_rejection.code, ErrorCode::RunCapacity);
    assert!(read_marker_pids(&rejected_marker).is_empty());
    drop(terminal_pin);

    let replacement = manager
        .create(
            CreateOperationKey::new("memory-capacity-replacement").unwrap(),
            CreationRequest::Start {
                spec: marker_spec(&replacement_marker, false),
            },
        )
        .await
        .expect("replace the exact quiescent terminal Run");
    assert_ne!(replacement.id, first.id);
    assert_eq!(manager.list().len(), 1);
    assert_eq!(manager.list()[0].id, replacement.id);
    assert_eq!(
        manager.info(first.id).unwrap_err().code,
        ErrorCode::RunNotFound
    );
    assert_eq!(wait_for_marker_pids(&replacement_marker, 1).await.len(), 1);
    wait_for_run_terminal_async(&manager.get(replacement.id).unwrap()).await;
    wait_for_run_workers(&manager).await;

    let recreated = manager
        .create(first_key, CreationRequest::Start { spec: first_spec })
        .await
        .expect("the removed exact key may elect one new Run");
    assert_ne!(recreated.id, first.id);
    assert_eq!(manager.list().len(), 1);
    assert_eq!(wait_for_marker_pids(&first_marker, 2).await.len(), 2);
    stop_run_and_wait(&manager, recreated.id).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn projected_memory_record_blocks_a_second_physical_start_before_publication() {
    let (hook, mut reached) = creation_hook(CreationHookPoint::AfterSpawn, true);
    let manager = Arc::new(RunManager {
        registry: RunRegistry::with_record_capacity(1),
        creation_hook: Some(Arc::clone(&hook)),
        ..RunManager::default()
    });
    let temp = tempfile::tempdir().expect("create projected capacity fixture");
    let first_marker = temp.path().join("first.log");
    let rejected_marker = temp.path().join("rejected.log");
    let first_manager = Arc::clone(&manager);
    let first = tokio::spawn(async move {
        first_manager
            .create(
                CreateOperationKey::new("projected-first").unwrap(),
                CreationRequest::Start {
                    spec: marker_spec(&first_marker, true),
                },
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), reached.recv())
        .await
        .expect("first creation reaches post-spawn barrier")
        .expect("first creation barrier remains connected");
    assert!(manager.list().is_empty());

    let rejected = manager
        .create(
            CreateOperationKey::new("projected-second").unwrap(),
            CreationRequest::Start {
                spec: marker_spec(&rejected_marker, false),
            },
        )
        .await
        .expect_err("the first projected record consumes the only capacity");
    assert_eq!(rejected.code, ErrorCode::RunCapacity);
    assert!(read_marker_pids(&rejected_marker).is_empty());

    hook.release();
    let first = first
        .await
        .expect("first creation owner task remains live")
        .expect("first projected Run publishes");
    assert_eq!(manager.list().len(), 1);
    manager
        .get(first.id)
        .unwrap()
        .stop()
        .await
        .expect("stop projected capacity fixture");
    wait_for_run_terminal_async(&manager.get(first.id).unwrap()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_client_observes_collecting_and_removed_matrix_and_reuses_exact_key() {
    let (hook, mut reached) = creation_hook(CreationHookPoint::AfterSpawn, false);
    let manager = Arc::new(RunManager {
        registry: RunRegistry::with_record_capacity(1),
        creation_hook: Some(Arc::clone(&hook)),
        ..RunManager::default()
    });
    let server = InProcessServer::start(Arc::clone(&manager));
    let temp = tempfile::tempdir().expect("create collection fence fixture");
    let first_marker = temp.path().join("first.log");
    let replacement_marker = temp.path().join("replacement.log");
    let rejected_marker = temp.path().join("rejected.log");
    let (first_key, replacement_key) = distinct_operation_keys(&manager);
    let fork_key = operation_key_on_other_stripe(&manager, &replacement_key, "collecting-fork");
    let rejected_key =
        operation_key_on_other_stripe(&manager, &replacement_key, "collecting-start");
    let first_spec = marker_spec(&first_marker, false);
    let first = server
        .client
        .start_with_operation_key(first_spec.clone(), first_key.clone())
        .await
        .expect("publish collection candidate");
    wait_for_exit(&server.client, first.id).await;
    wait_for_run_workers(&manager).await;

    hook.arm();
    let replacement_client = server.client.clone();
    let replacement_spec = marker_spec(&replacement_marker, false);
    let replacement_request = replacement_spec.clone();
    let replacement = tokio::spawn(async move {
        replacement_client
            .start_with_operation_key(replacement_request, replacement_key)
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), reached.recv())
        .await
        .expect("replacement reaches post-spawn barrier")
        .expect("replacement barrier remains connected");

    assert_collecting_public_matrix(
        &server,
        first.id,
        &first_spec,
        &first_key,
        fork_key,
        &rejected_marker,
        rejected_key,
    )
    .await;

    hook.release();
    let replacement = replacement
        .await
        .expect("replacement owner task remains live")
        .expect("replacement publishes");
    let retained = server.client.list().await.unwrap();
    assert_eq!(retained.len(), 1);
    assert_eq!(retained[0].id, replacement.id);
    assert_removed_public_matrix(&server, first.id).await;
    assert_eq!(wait_for_marker_pids(&replacement_marker, 1).await.len(), 1);
    wait_for_exit(&server.client, replacement.id).await;
    wait_for_run_workers(&manager).await;

    let recreated = server
        .client
        .start_with_operation_key(first_spec, first_key)
        .await
        .expect("removed exact key may elect one new physical Run");
    assert_ne!(recreated.id, first.id);
    let retained = server.client.list().await.unwrap();
    assert_eq!(retained.len(), 1);
    assert_eq!(retained[0].id, recreated.id);
    assert_eq!(wait_for_marker_pids(&first_marker, 2).await.len(), 2);
    wait_for_exit(&server.client, recreated.id).await;
    wait_for_run_workers(&manager).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multiple_terminal_candidates_replace_earliest_run_and_its_exact_key() {
    let manager = Arc::new(RunManager {
        registry: RunRegistry::with_record_capacity(2),
        ..RunManager::default()
    });
    let temp = tempfile::tempdir().expect("create ordered collection fixture");
    let first_spec = marker_spec(&temp.path().join("first.log"), false);
    let second_spec = marker_spec(&temp.path().join("second.log"), false);
    let replacement_spec = marker_spec(&temp.path().join("replacement.log"), false);
    let first_key = CreateOperationKey::new("ordered-candidate-first").unwrap();
    let second_key = CreateOperationKey::new("ordered-candidate-second").unwrap();

    let first = manager
        .create(
            first_key.clone(),
            CreationRequest::Start {
                spec: first_spec.clone(),
            },
        )
        .await
        .expect("publish first terminal candidate");
    wait_for_run_terminal_async(&manager.get(first.id).unwrap()).await;
    wait_for_run_workers(&manager).await;
    let second = manager
        .create(
            second_key.clone(),
            CreationRequest::Start {
                spec: second_spec.clone(),
            },
        )
        .await
        .expect("publish second terminal candidate");
    wait_for_run_terminal_async(&manager.get(second.id).unwrap()).await;
    wait_for_run_workers(&manager).await;

    let replacement = manager
        .create(
            CreateOperationKey::new("ordered-candidate-replacement").unwrap(),
            CreationRequest::Start {
                spec: replacement_spec,
            },
        )
        .await
        .expect("replace the earliest terminal candidate");

    assert_eq!(
        manager.info(first.id).unwrap_err().code,
        ErrorCode::RunNotFound
    );
    assert_eq!(manager.info(second.id).unwrap().id, second.id);
    assert!(
        manager
            .registry
            .resolve_creation_info(&first_key, &CreationRequest::Start { spec: first_spec },)
            .expect("resolve removed exact key")
            .is_none()
    );
    assert_eq!(
        manager
            .registry
            .resolve_creation_info(&second_key, &CreationRequest::Start { spec: second_spec },)
            .expect("resolve later candidate exact key")
            .expect("later candidate key remains bound")
            .id,
        second.id
    );
    assert_eq!(
        manager
            .list()
            .into_iter()
            .map(|run| run.id)
            .collect::<HashSet<_>>(),
        HashSet::from([second.id, replacement.id])
    );
    wait_for_run_terminal_async(&manager.get(replacement.id).unwrap()).await;
    wait_for_run_workers(&manager).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_reservations_stay_bounded_under_reverse_publication() {
    let (hook, mut reached) = creation_hook(CreationHookPoint::AfterSpawn, false);
    let manager = Arc::new(RunManager {
        registry: RunRegistry::with_record_capacity(2),
        creation_hook: Some(Arc::clone(&hook)),
        ..RunManager::default()
    });
    let temp = tempfile::tempdir().expect("create projected-capacity fixture");
    let initial_marker = temp.path().join("initial.log");
    let delayed_marker = temp.path().join("delayed.log");
    let replacement_marker = temp.path().join("replacement.log");
    let rejected_marker = temp.path().join("rejected.log");
    let initial = manager
        .create(
            CreateOperationKey::new("projected-capacity-initial").unwrap(),
            CreationRequest::Start {
                spec: marker_spec(&initial_marker, false),
            },
        )
        .await
        .expect("publish the initial terminal candidate");
    wait_for_run_terminal_async(&manager.get(initial.id).unwrap()).await;
    wait_for_run_workers(&manager).await;

    let delayed_key = CreateOperationKey::new("projected-capacity-delayed").unwrap();
    let replacement_key =
        operation_key_on_other_stripe(&manager, &delayed_key, "projected-capacity-replacement");
    let rejected_key =
        operation_key_on_other_stripe(&manager, &delayed_key, "projected-capacity-rejected");
    hook.arm();
    let delayed_manager = Arc::clone(&manager);
    let delayed = tokio::spawn(async move {
        delayed_manager
            .create(
                delayed_key,
                CreationRequest::Start {
                    spec: marker_spec(&delayed_marker, true),
                },
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), reached.recv())
        .await
        .expect("delayed publication reaches its post-spawn barrier")
        .expect("delayed publication barrier remains connected");

    let replacement = manager
        .create(
            replacement_key,
            CreationRequest::Start {
                spec: marker_spec(&replacement_marker, true),
            },
        )
        .await
        .expect("a self-funded reservation may publish before an older ticket");
    assert_eq!(manager.list().len(), 1);
    assert_eq!(manager.list()[0].id, replacement.id);
    assert_eq!(
        manager.info(initial.id).unwrap_err().code,
        ErrorCode::RunNotFound
    );

    let rejected = manager
        .create(
            rejected_key,
            CreationRequest::Start {
                spec: marker_spec(&rejected_marker, false),
            },
        )
        .await
        .expect_err("another ticket cannot borrow the delayed ticket's projection");
    assert_eq!(rejected.code, ErrorCode::RunCapacity);
    assert!(read_marker_pids(&rejected_marker).is_empty());

    hook.release();
    let delayed = delayed
        .await
        .expect("delayed creation task remains live")
        .expect("the older reservation publishes after its self-funded successor");
    let retained: HashSet<_> = manager.list().into_iter().map(|run| run.id).collect();
    assert_eq!(retained, HashSet::from([delayed.id, replacement.id]));

    for id in [delayed.id, replacement.id] {
        manager
            .get(id)
            .expect("pin retained live Run")
            .stop()
            .await
            .expect("stop retained projected-capacity fixture");
        wait_for_run_terminal_async(&manager.get(id).unwrap()).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_replacement_restores_candidate_and_tmux_admission_checks_capacity_first() {
    let manager = Arc::new(RunManager {
        registry: RunRegistry::with_record_capacity(1),
        ..RunManager::default()
    });
    let first_key = CreateOperationKey::new("restored-candidate").unwrap();
    let first_spec = short_lived_spec();
    let first = manager
        .create(
            first_key.clone(),
            CreationRequest::Start {
                spec: first_spec.clone(),
            },
        )
        .await
        .expect("publish candidate restored after abort");
    wait_for_run_terminal_async(&manager.get(first.id).unwrap()).await;
    wait_for_run_workers(&manager).await;

    let failed = manager
        .create(
            CreateOperationKey::new("failed-replacement").unwrap(),
            CreationRequest::Start {
                spec: RunSpec {
                    program: "/ctxmux/definitely/missing".to_owned(),
                    ..short_lived_spec()
                },
            },
        )
        .await
        .expect_err("spawn failure aborts the publication reservation");
    assert_eq!(failed.code, ErrorCode::SpawnFailed);
    assert_eq!(manager.info(first.id).unwrap().id, first.id);
    assert_eq!(
        manager
            .create(first_key, CreationRequest::Start { spec: first_spec })
            .await
            .expect("candidate key remains bound after abort")
            .id,
        first.id
    );

    let live = manager
        .create(
            CreateOperationKey::new("live-after-restored-candidate").unwrap(),
            CreationRequest::Start {
                spec: long_running_spec(),
            },
        )
        .await
        .expect("replace the restored terminal candidate with a live Run");
    let tmux_flight = manager
        .begin_creation_flight()
        .await
        .expect("tmux fixture acquires shared physical admission");
    let tmux_error = manager
        .import_tmux("/ctxmux/definitely/missing.sock", "%0", tmux_flight)
        .expect_err("tmux import reserves capacity before Control startup");
    assert_eq!(tmux_error.code, ErrorCode::RunCapacity);
    manager
        .get(live.id)
        .unwrap()
        .stop()
        .await
        .expect("stop live capacity fixture");
    wait_for_run_terminal_async(&manager.get(live.id).unwrap()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fresh_level_a_fork_materializes_then_releases_its_parent_before_reservation() {
    let manager = Arc::new(RunManager {
        registry: RunRegistry::with_record_capacity(1),
        ..RunManager::default()
    });
    let parent = manager
        .create(
            CreateOperationKey::new("collectible-fork-parent").unwrap(),
            CreationRequest::Start {
                spec: short_lived_spec(),
            },
        )
        .await
        .expect("publish fork parent");
    wait_for_run_terminal_async(&manager.get(parent.id).unwrap()).await;
    wait_for_run_workers(&manager).await;

    let child_key = CreateOperationKey::new("child-replacing-parent").unwrap();
    let child_request = CreationRequest::Fork {
        parent: parent.id,
        plan: ForkPlan::LevelA,
    };
    let child = manager
        .create(child_key.clone(), child_request.clone())
        .await
        .expect("fork materialization does not pin its parent through admission");
    assert_eq!(
        child.lineage,
        Some(RunLineage {
            parent: parent.id,
            fidelity: ForkFidelity::LevelA,
        })
    );
    assert_eq!(
        manager.info(parent.id).unwrap_err().code,
        ErrorCode::RunNotFound
    );
    assert_eq!(manager.list().len(), 1);
    assert_eq!(manager.list()[0].id, child.id);
    assert_eq!(
        manager
            .create(child_key, child_request)
            .await
            .expect("matching child retry resolves before collected-parent lookup")
            .id,
        child.id
    );
    assert_eq!(
        manager
            .create(
                CreateOperationKey::new("fresh-child-after-parent-removal").unwrap(),
                CreationRequest::Fork {
                    parent: parent.id,
                    plan: ForkPlan::LevelA,
                },
            )
            .await
            .expect_err("a fresh Fork cannot resolve a collected parent")
            .code,
        ErrorCode::RunNotFound
    );
    wait_for_run_terminal_async(&manager.get(child.id).unwrap()).await;
}

fn colliding_operation_keys(manager: &RunManager) -> (CreateOperationKey, CreateOperationKey) {
    // 65 distinct keys guarantee a pair across the production owner's 64 stripes.
    let keys = (0..65)
        .map(|index| CreateOperationKey::new(format!("shutdown-stripe-{index}")).unwrap())
        .collect::<Vec<_>>();
    for (index, left) in keys.iter().enumerate() {
        if let Some(right) = keys[index + 1..]
            .iter()
            .find(|right| manager.registry.shares_creation_stripe(left, right))
        {
            return (left.clone(), right.clone());
        }
    }
    unreachable!("pigeonhole principle guarantees a collision pair")
}

fn distinct_operation_keys(manager: &RunManager) -> (CreateOperationKey, CreateOperationKey) {
    let first = CreateOperationKey::new("collection-fence-first").unwrap();
    for index in 0..64 {
        let candidate =
            CreateOperationKey::new(format!("collection-fence-replacement-{index}")).unwrap();
        if !manager.registry.shares_creation_stripe(&first, &candidate) {
            return (first, candidate);
        }
    }
    unreachable!("64 random-hash stripes include a distinct candidate")
}

fn operation_key_on_other_stripe(
    manager: &RunManager,
    reference: &CreateOperationKey,
    prefix: &str,
) -> CreateOperationKey {
    for index in 0..64 {
        let candidate = CreateOperationKey::new(format!("{prefix}-{index}")).unwrap();
        if !manager
            .registry
            .shares_creation_stripe(reference, &candidate)
        {
            return candidate;
        }
    }
    unreachable!("64 random-hash stripes include a key on another stripe")
}

#[test]
fn durable_terminal_handoff_finalizes_when_binding_precedes_publication() {
    let temp = tempfile::tempdir().expect("create binding-first state directory");
    let state_dir = temp.path().join("state");
    let (persistence, recovered) =
        Persistence::open(&state_dir).expect("open binding-first persistence");
    assert!(recovered.is_empty());
    let (reached_tx, reached_rx) = std::sync::mpsc::sync_channel(0);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
    let run = Run::spawn_with_wait_hook(
        short_lived_spec(),
        PersistenceMode::PersistentCapable,
        move || {
            reached_tx
                .send(())
                .expect("report terminal publication barrier");
            release_rx.recv().expect("release terminal publication");
        },
    )
    .expect("spawn binding-first Run");
    reached_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("waiter reaches binding-first barrier");

    let committed = persistence
        .insert_start(
            &CreateOperationKey::new("binding-before-terminal").unwrap(),
            &run.persistence_start_info(),
        )
        .expect("commit binding-first start");
    run.install_committed_persistence(committed.durable.clone());
    run.activate_persistence_after_publication();
    assert!(run.info().state.is_running());
    release_tx.send(()).expect("release binding-first waiter");
    wait_for_run_terminal(&run);
    assert_eq!(run.info().state, clean_exit());
    let sentinel = insert_persistence_sentinel(&persistence, &run, "binding-first-sentinel");
    let run_id = run.id;
    wait_for_direct_run_workers(&run);

    drop(committed);
    drop(run);
    persistence.assert_exclusive_owner();
    drop(persistence);
    let (reopened, recovered) = Persistence::open(state_dir).expect("reopen binding-first state");
    assert_eq!(recovered.len(), 2);
    assert_recovered_exit(&recovered, run_id);
    assert_recovered_exit(&recovered, sentinel);
    drop(reopened);
}

#[test]
fn durable_terminal_handoff_finalizes_when_publication_precedes_binding() {
    let temp = tempfile::tempdir().expect("create terminal-first state directory");
    let state_dir = temp.path().join("state");
    let (persistence, recovered) =
        Persistence::open(&state_dir).expect("open terminal-first persistence");
    assert!(recovered.is_empty());
    let (reached_tx, reached_rx) = std::sync::mpsc::sync_channel(0);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
    let run = Run::spawn_with_wait_hook(
        short_lived_spec(),
        PersistenceMode::PersistentCapable,
        move || {
            reached_tx.send(()).expect("report terminal-first barrier");
            release_rx
                .recv()
                .expect("release terminal-first publication");
        },
    )
    .expect("spawn terminal-first Run");
    reached_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("waiter reaches terminal-first barrier");

    release_tx.send(()).expect("release terminal-first waiter");
    wait_for_pending_persistence_terminal(&run);
    assert!(run.info().state.is_running());
    let committed = persistence
        .insert_start(
            &CreateOperationKey::new("terminal-before-binding").unwrap(),
            &run.persistence_start_info(),
        )
        .expect("commit a creation-time Running row after fast exit");
    run.install_committed_persistence(committed.durable.clone());
    run.activate_persistence_after_publication();
    wait_for_run_terminal(&run);
    assert_eq!(run.info().state, clean_exit());
    let sentinel = insert_persistence_sentinel(&persistence, &run, "terminal-first-sentinel");
    let run_id = run.id;
    wait_for_direct_run_workers(&run);

    drop(committed);
    drop(run);
    persistence.assert_exclusive_owner();
    drop(persistence);
    let (reopened, recovered) = Persistence::open(state_dir).expect("reopen terminal-first state");
    assert_eq!(recovered.len(), 2);
    assert_recovered_exit(&recovered, run_id);
    assert_recovered_exit(&recovered, sentinel);
    drop(reopened);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_finalize_keeps_reads_responsive_and_late_output_memory_only() {
    let temp = tempfile::tempdir().expect("create finalize response fixture");
    let state_dir = temp.path().join("state");
    let (persistence, recovered) =
        Persistence::open(&state_dir).expect("open finalize response persistence");
    assert!(recovered.is_empty());
    let manager = Arc::new(RunManager::persistent(persistence.clone(), recovered));
    let server = InProcessServer::start(Arc::clone(&manager));
    let run = server
        .client
        .start(long_running_spec())
        .await
        .expect("start finalize response Run");
    let recorded = manager.get(run.id).expect("resolve finalize response Run");
    let mut events = recorded.subscribe();
    let initial_head = recorded.info().head_seq;
    let (finalize_reached, finalize_release) = persistence.pause_next_finalize();

    server
        .client
        .stop(run.id)
        .await
        .expect("stop finalize response Run");
    finalize_reached
        .recv_timeout(Duration::from_secs(5))
        .expect("persistence actor reaches finalize barrier");

    assert_finalize_reads_responsive(&server, run.id, initial_head).await;
    record_late_output_after_finalize(&recorded, &finalize_release).await;

    let terminal = tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .expect("terminal event arrives after finalize")
        .expect("read terminal event");
    assert!(matches!(terminal, RunEvent::Exited { .. }));
    let late = tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .expect("late output remains broadcast")
        .expect("read late output event");
    assert!(matches!(
        late,
        RunEvent::Output { chunk } if chunk.data == b"late-after-terminal"
    ));
    let status = server
        .client
        .status(run.id)
        .await
        .expect("read terminal status");
    assert!(!status.state.is_running());
    assert_eq!(status.head_seq, initial_head + 1);
    assert_eq!(status.durable_head_seq, Some(initial_head));
    let (_, late_snapshot) = server
        .client
        .attach(run.id, initial_head)
        .await
        .expect("late memory output remains replayable");
    assert_eq!(
        replay_bytes(&late_snapshot.replay.chunks),
        b"late-after-terminal"
    );
    assert!(!persistence.is_failed());
    let sentinel = insert_persistence_sentinel(&persistence, &recorded, "late-output-sentinel");
    assert!(!persistence.is_failed());
    assert_ne!(sentinel, run.id);
    drop(recorded);
    wait_for_run_workers(&manager).await;
    drop(server);
    drop(manager);
    drop(persistence);

    let (reopened, recovered) = Persistence::open(state_dir).expect("reopen late-output state");
    let durable = recovered
        .iter()
        .find(|candidate| candidate.info.id == run.id)
        .expect("original Run remains durable");
    assert!(!durable.info.state.is_running());
    assert_eq!(durable.info.head_seq, initial_head);
    assert_eq!(durable.info.durable_head_seq, Some(initial_head));
    assert!(durable.replay.chunks.is_empty());
    assert_recovered_exit(&recovered, sentinel);
    drop(reopened);
}

async fn assert_finalize_reads_responsive(
    server: &InProcessServer,
    run_id: ctxmux_protocol::RunId,
    initial_head: u64,
) {
    let status = tokio::time::timeout(Duration::from_secs(1), server.client.status(run_id))
        .await
        .expect("status remains responsive during finalize")
        .expect("read status during finalize");
    assert!(status.state.is_running());
    let listed = tokio::time::timeout(Duration::from_secs(1), server.client.list())
        .await
        .expect("list remains responsive during finalize")
        .expect("list during finalize");
    assert!(
        listed
            .iter()
            .find(|candidate| candidate.id == run_id)
            .expect("finalizing Run remains listed")
            .state
            .is_running()
    );
    let (attachment, snapshot) = tokio::time::timeout(
        Duration::from_secs(1),
        server.client.attach(run_id, initial_head),
    )
    .await
    .expect("attach remains responsive during finalize")
    .expect("attach during finalize");
    assert!(snapshot.run.state.is_running());
    drop(attachment);
}

async fn record_late_output_after_finalize(
    run: &Arc<Run>,
    finalize_release: &std::sync::mpsc::SyncSender<()>,
) {
    let late_run = Arc::clone(run);
    let (late_started_tx, late_started_rx) = std::sync::mpsc::channel();
    let (late_done_tx, late_done_rx) = std::sync::mpsc::channel();
    let late_output = tokio::task::spawn_blocking(move || {
        late_started_tx
            .send(())
            .expect("report late output attempt");
        late_run.record_output(b"late-after-terminal".to_vec());
        late_done_tx
            .send(())
            .expect("report late output completion");
    });
    assert!(
        late_started_rx.recv_timeout(Duration::from_secs(5)).is_ok(),
        "late output owner starts"
    );
    assert!(
        matches!(
            late_done_rx.recv_timeout(Duration::from_millis(50)),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        ),
        "late output bypassed the in-flight terminal transition gate"
    );
    finalize_release
        .send(())
        .expect("release persistence finalize barrier");
    late_output.await.expect("late output owner completes");
    late_done_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("late output completes after terminal transition");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn committed_creation_survives_a_post_commit_failure_and_restart() {
    let temp = tempfile::tempdir().expect("create post-commit failure fixture");
    let state_dir = temp.path().join("state");
    let marker = temp.path().join("starts.log");
    let (persistence, recovered) =
        Persistence::open(&state_dir).expect("open post-commit persistence");
    assert!(recovered.is_empty());
    persistence.fail_next_insert_after_commit();
    let manager = Arc::new(RunManager::persistent(persistence.clone(), recovered));
    let operation_key = CreateOperationKey::new("committed-postcheck-failure").unwrap();
    let spec = marker_spec(&marker, true);

    let error = manager
        .create(
            operation_key.clone(),
            CreationRequest::Start { spec: spec.clone() },
        )
        .await
        .expect_err("post-commit check remains visible to the first caller");
    assert_eq!(error.code, ErrorCode::Persistence);
    assert!(persistence.is_failed());
    let published = manager.list();
    assert_eq!(published.len(), 1);
    let original = published[0].clone();
    let original_pid = original.pid.expect("committed Run has one child PID");
    assert_eq!(wait_for_marker_pids(&marker, 1).await, vec![original_pid]);

    let retried = manager
        .create(
            operation_key.clone(),
            CreationRequest::Start { spec: spec.clone() },
        )
        .await
        .expect("retry resolves the committed registry entry");
    assert_eq!((retried.id, retried.pid), (original.id, original.pid));
    let mut conflict = spec.clone();
    conflict.args.push("different".to_owned());
    let conflict = manager
        .create(
            operation_key.clone(),
            CreationRequest::Start { spec: conflict },
        )
        .await
        .expect_err("committed key rejects a different request");
    assert_eq!(conflict.code, ErrorCode::CreationConflict);
    assert_eq!(wait_for_marker_pids(&marker, 1).await, vec![original_pid]);
    manager
        .get(original.id)
        .expect("resolve committed Run")
        .stop()
        .await
        .expect("stop committed Run");
    wait_for_run_terminal_async(&manager.get(original.id).unwrap()).await;
    assert!(!manager.get(original.id).unwrap().info().state.is_running());
    wait_for_run_workers(&manager).await;
    drop(manager);
    drop(persistence);

    let connection =
        rusqlite::Connection::open(state_dir.join("state.sqlite3")).expect("inspect committed row");
    let rows: i64 = connection
        .query_row(
            "SELECT count(*) FROM runs WHERE id = ?1 AND creation_key = ?2",
            rusqlite::params![original.id.to_string(), operation_key.as_str()],
            |row| row.get(0),
        )
        .expect("count committed Run/key row");
    assert_eq!(rows, 1);
    drop(connection);

    let (persistence, recovered) =
        Persistence::open(&state_dir).expect("reopen committed postcheck state");
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].operation_key, operation_key);
    assert_eq!(recovered[0].info.id, original.id);
    assert_eq!(
        recovered[0].info.state,
        RunState::Interrupted {
            reason: InterruptionReason::DaemonRestart
        }
    );
    let restarted = Arc::new(RunManager::persistent(persistence, recovered));
    let retry_after_restart = restarted
        .create(operation_key, CreationRequest::Start { spec })
        .await
        .expect("restart retry resolves the durable Run/key");
    assert_eq!(retry_after_restart.id, original.id);
    assert_eq!(wait_for_marker_pids(&marker, 1).await, vec![original_pid]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persistent_replacement_removes_exact_run_and_key_in_the_same_epoch() {
    let temp = tempfile::tempdir().expect("create collection fixture");
    let state_dir = temp.path().join("state");
    let marker = temp.path().join("starts.log");
    let (persistence, recovered) =
        Persistence::open_with_test_limits(state_dir.clone(), 1, 64 * 1024 * 1024)
            .expect("open tiny persistence store");
    assert!(recovered.is_empty());
    let manager = Arc::new(RunManager {
        registry: RunRegistry::with_record_capacity(1),
        ..RunManager::persistent(persistence, recovered)
    });
    let operation_key = CreateOperationKey::new("collected-key").unwrap();
    let spec = marker_spec(&marker, false);
    let first = manager
        .create(
            operation_key.clone(),
            CreationRequest::Start { spec: spec.clone() },
        )
        .await
        .expect("create collection candidate");
    wait_for_run_terminal_async(&manager.get(first.id).unwrap()).await;
    assert_eq!(
        wait_for_marker_pids(&marker, 1).await,
        vec![first.pid.unwrap()]
    );
    wait_for_run_workers(&manager).await;

    let replacement_key = CreateOperationKey::new("retained-replacement").unwrap();
    let replacement = manager
        .create(
            replacement_key.clone(),
            CreationRequest::Start {
                spec: short_lived_spec(),
            },
        )
        .await
        .expect("replace exact terminal durable history");
    wait_for_run_terminal_async(&manager.get(replacement.id).unwrap()).await;
    wait_for_run_workers(&manager).await;
    assert_eq!(
        manager.info(first.id).unwrap_err().code,
        ErrorCode::RunNotFound
    );
    assert_eq!(
        manager.list().iter().map(|run| run.id).collect::<Vec<_>>(),
        vec![replacement.id]
    );
    assert_persistent_run_key_absent(&state_dir, first.id, &operation_key);
    assert_persistent_run_key_present(&state_dir, replacement.id, &replacement_key);

    let recreated = manager
        .create(
            operation_key.clone(),
            CreationRequest::Start { spec: spec.clone() },
        )
        .await
        .expect("the removed exact key elects a new same-epoch Run");
    assert_ne!(recreated.id, first.id);
    wait_for_run_terminal_async(&manager.get(recreated.id).unwrap()).await;
    wait_for_run_workers(&manager).await;
    let pids = wait_for_marker_pids(&marker, 2).await;
    assert_eq!(pids, vec![first.pid.unwrap(), recreated.pid.unwrap()]);
    let retried = manager
        .create(
            operation_key.clone(),
            CreationRequest::Start { spec: spec.clone() },
        )
        .await
        .expect("same-epoch retry converges without another process");
    assert_eq!(retried.id, recreated.id);
    assert_eq!(wait_for_marker_pids(&marker, 2).await, pids);
    assert_eq!(
        manager.info(replacement.id).unwrap_err().code,
        ErrorCode::RunNotFound
    );
    assert_eq!(
        manager.list().iter().map(|run| run.id).collect::<Vec<_>>(),
        vec![recreated.id]
    );
    assert_persistent_run_key_absent(&state_dir, replacement.id, &replacement_key);
    assert_persistent_run_key_present(&state_dir, recreated.id, &operation_key);
    drop(manager);
    assert_persistent_replacement_restart(
        &state_dir,
        [first.id, replacement.id],
        recreated.id,
        &operation_key,
        &spec,
        &marker,
        &pids,
    )
    .await;
}

async fn assert_persistent_replacement_restart(
    state_dir: &std::path::Path,
    removed: [RunId; 2],
    retained: RunId,
    operation_key: &CreateOperationKey,
    spec: &RunSpec,
    marker: &std::path::Path,
    pids: &[u32],
) {
    let (persistence, recovered) =
        Persistence::open_with_test_limits(state_dir.to_path_buf(), 1, 64 * 1024 * 1024)
            .expect("reopen tiny persistence store");
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].info.id, retained);
    assert_eq!(&recovered[0].operation_key, operation_key);
    let restarted = Arc::new(RunManager::persistent(persistence, recovered));
    for id in removed {
        assert!(restarted.get(id).is_err());
    }
    let recovered_retry = restarted
        .create(
            operation_key.clone(),
            CreationRequest::Start { spec: spec.clone() },
        )
        .await
        .expect("restart retry resolves the exact recovered Run/key");
    assert_eq!(recovered_retry.id, retained);
    assert_eq!(wait_for_marker_pids(marker, 2).await, pids);
    assert_eq!(
        restarted
            .list()
            .iter()
            .map(|run| run.id)
            .collect::<Vec<_>>(),
        vec![retained]
    );
}

const TURNOVER_RECORDS: usize = 4;
const TURNOVER_WINDOWS: usize = 3;

struct TurnoverRun {
    id: RunId,
    operation_key: CreateOperationKey,
    spec: RunSpec,
    payload: Vec<u8>,
}

#[derive(Debug, Eq, PartialEq)]
struct TurnoverTuple {
    id: RunId,
    operation_key: String,
    state: RunState,
    lineage: Option<RunLineage>,
    oldest_seq: u64,
    head_seq: u64,
    durable_head_seq: Option<u64>,
    truncated: bool,
    replay: Vec<u8>,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retained_state_converges_across_three_turnover_windows_and_restart() {
    for persistent in [false, true] {
        let temp = tempfile::tempdir().expect("create retained turnover fixture");
        let state_dir = temp.path().join("state");
        let marker = temp.path().join("physical-starts.log");
        let (creation_hook, _reached) = creation_hook(CreationHookPoint::AfterSpawn, false);
        let mut manager = if persistent {
            let (persistence, recovered) = Persistence::open_with_test_limits(
                state_dir.clone(),
                TURNOVER_RECORDS as u64,
                64 * 1024 * 1024,
            )
            .expect("open reduced persistent turnover store");
            assert!(recovered.is_empty());
            Arc::new(RunManager {
                registry: RunRegistry::with_record_capacity(TURNOVER_RECORDS),
                creation_hook: Some(Arc::clone(&creation_hook)),
                ..RunManager::persistent(persistence, recovered)
            })
        } else {
            Arc::new(RunManager {
                registry: RunRegistry::with_record_capacity(TURNOVER_RECORDS),
                creation_hook: Some(Arc::clone(&creation_hook)),
                ..RunManager::default()
            })
        };
        let mode = if persistent { "persistent" } else { "memory" };
        let mut retained = VecDeque::new();

        for index in 0..TURNOVER_RECORDS {
            retained.push_back(
                create_turnover_run(&manager, &creation_hook, &marker, mode, index).await,
            );
        }
        assert_turnover_boundary(
            &manager,
            &retained,
            &creation_hook,
            &marker,
            persistent,
            false,
        )
        .await;

        for window in 0..TURNOVER_WINDOWS {
            for offset in 0..TURNOVER_RECORDS {
                let index = TURNOVER_RECORDS * (window + 1) + offset;
                retained.pop_front().expect("one oldest Run is replaced");
                retained.push_back(
                    create_turnover_run(&manager, &creation_hook, &marker, mode, index).await,
                );
            }
            assert_turnover_boundary(
                &manager,
                &retained,
                &creation_hook,
                &marker,
                persistent,
                false,
            )
            .await;

            if persistent && window == 1 {
                let before_restart = turnover_tuples(&manager, &retained);
                drop(manager);
                let (persistence, recovered) = Persistence::open_with_test_limits(
                    state_dir.clone(),
                    TURNOVER_RECORDS as u64,
                    64 * 1024 * 1024,
                )
                .expect("restart reduced persistent turnover store");
                assert_eq!(recovered.len(), TURNOVER_RECORDS);
                let restarted = Arc::new(RunManager {
                    creation_hook: Some(Arc::clone(&creation_hook)),
                    ..RunManager::persistent(persistence, recovered)
                });
                restarted
                    .registry
                    .set_record_capacity_for_test(TURNOVER_RECORDS);
                manager = restarted;
                assert_eq!(turnover_tuples(&manager, &retained), before_restart);
                assert_turnover_boundary(&manager, &retained, &creation_hook, &marker, true, true)
                    .await;
            }
        }
    }
}

async fn create_turnover_run(
    manager: &Arc<RunManager>,
    creation_hook: &CreationTestHook,
    marker: &std::path::Path,
    mode: &str,
    index: usize,
) -> TurnoverRun {
    let payload = format!("ctxmux-{mode}-turnover-{index:03}").into_bytes();
    let spec = turnover_spec(marker, &payload);
    let operation_key = CreateOperationKey::new(format!("turnover:{mode}:{index:03}")).unwrap();
    let physical_spawns_before = creation_hook.physical_spawn_count();
    let info = manager
        .create(
            operation_key.clone(),
            CreationRequest::Start { spec: spec.clone() },
        )
        .await
        .expect("publish turnover Run");
    assert_eq!(
        creation_hook.physical_spawn_count(),
        physical_spawns_before + 1,
        "one fresh creation owns exactly one physical spawn"
    );
    wait_for_run_terminal_async(&manager.get(info.id).unwrap()).await;
    wait_for_run_workers(manager).await;
    assert_eq!(
        wait_for_marker_pids(marker, physical_spawns_before + 1)
            .await
            .len(),
        physical_spawns_before + 1
    );
    TurnoverRun {
        id: info.id,
        operation_key,
        spec,
        payload,
    }
}

async fn assert_turnover_boundary(
    manager: &Arc<RunManager>,
    retained: &VecDeque<TurnoverRun>,
    creation_hook: &CreationTestHook,
    marker: &std::path::Path,
    persistent: bool,
    recovered: bool,
) {
    assert_eq!(retained.len(), TURNOVER_RECORDS);
    assert_eq!(manager.list().len(), TURNOVER_RECORDS);
    assert_eq!(
        manager
            .list()
            .into_iter()
            .map(|info| info.id)
            .collect::<HashSet<_>>(),
        retained.iter().map(|run| run.id).collect::<HashSet<_>>()
    );
    let physical_spawns_before_retries = creation_hook.physical_spawn_count();
    for expected in retained {
        let info = manager.info(expected.id).expect("retained Run is visible");
        assert!(!info.state.is_running());
        if persistent {
            assert_eq!(info.durable_head_seq, Some(info.head_seq));
        }
        let run = manager.get(expected.id).expect("pin retained turnover Run");
        if recovered {
            assert!(
                run.incarnation_control.is_none(),
                "recovered terminal Run has no current-incarnation control authority"
            );
        }
        assert_eq!(
            replay_bytes(&mutex_lock(&run.output).replay(0).chunks),
            expected.payload
        );
        drop(run);
        let retried = manager
            .create(
                expected.operation_key.clone(),
                CreationRequest::Start {
                    spec: expected.spec.clone(),
                },
            )
            .await
            .expect("same-key turnover retry resolves retained Run");
        assert_eq!(retried.id, expected.id);
    }
    assert_eq!(
        creation_hook.physical_spawn_count(),
        physical_spawns_before_retries,
        "same-key retry wave starts no physical child"
    );
    assert_eq!(
        read_marker_pids(marker).len(),
        creation_hook.physical_spawn_count()
    );
}

fn turnover_tuples(manager: &RunManager, retained: &VecDeque<TurnoverRun>) -> Vec<TurnoverTuple> {
    let mut tuples = retained
        .iter()
        .map(|expected| {
            let info = manager
                .info(expected.id)
                .expect("retained tuple is visible");
            let run = manager.get(expected.id).expect("pin retained tuple Run");
            let replay = mutex_lock(&run.output).replay(0);
            TurnoverTuple {
                id: info.id,
                operation_key: expected.operation_key.as_str().to_owned(),
                state: info.state,
                lineage: info.lineage,
                oldest_seq: replay.oldest_seq,
                head_seq: replay.head_seq,
                durable_head_seq: info.durable_head_seq,
                truncated: replay.truncated,
                replay: replay_bytes(&replay.chunks),
            }
        })
        .collect::<Vec<_>>();
    tuples.sort_by_key(|tuple| tuple.id.to_string());
    tuples
}

fn turnover_spec(marker: &std::path::Path, payload: &[u8]) -> RunSpec {
    RunSpec {
        program: "/bin/sh".to_owned(),
        args: vec![
            "-c".to_owned(),
            "printf '%s\\n' \"$$\" >> \"$CTXMUX_CREATION_MARKER\"; \
             printf '%s' \"$CTXMUX_TURNOVER_PAYLOAD\""
                .to_owned(),
        ],
        cwd: None,
        env: BTreeMap::from([
            (
                "CTXMUX_CREATION_MARKER".to_owned(),
                marker.to_string_lossy().into_owned(),
            ),
            (
                "CTXMUX_TURNOVER_PAYLOAD".to_owned(),
                String::from_utf8(payload.to_vec()).expect("turnover payload is ASCII"),
            ),
        ]),
        size: TerminalSize::default(),
        declared_inputs: Vec::new(),
    }
}

fn short_lived_spec() -> RunSpec {
    RunSpec {
        program: "/bin/sh".to_owned(),
        args: vec!["-c".to_owned(), "exit 0".to_owned()],
        cwd: None,
        env: BTreeMap::new(),
        size: TerminalSize::default(),
        declared_inputs: Vec::new(),
    }
}

fn wait_for_run_terminal(run: &Arc<Run>) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while run.info().state.is_running() {
        assert!(
            Instant::now() < deadline,
            "Run did not publish terminal state"
        );
        std::thread::yield_now();
    }
}

fn wait_for_pending_persistence_terminal(run: &Arc<Run>) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !run.persistence_terminal_is_pending() {
        assert!(
            Instant::now() < deadline,
            "Run did not retain its unpublished terminal result"
        );
        std::thread::yield_now();
    }
}

fn insert_persistence_sentinel(
    persistence: &Persistence,
    source: &Run,
    label: &str,
) -> ctxmux_protocol::RunId {
    let mut info = source.persistence_start_info();
    info.id = ctxmux_protocol::RunId::new();
    let operation_key = CreateOperationKey::new(label).expect("valid sentinel key");
    let committed = persistence
        .insert_start(&operation_key, &info)
        .expect("terminal handoff leaves persistence actor healthy");
    assert!(committed.post_commit_error.is_none());
    committed.durable.finalize(
        info.id,
        info.pid.expect("native sentinel retains a historical PID"),
        OutputReplay {
            chunks: Vec::new(),
            oldest_seq: 0,
            head_seq: 0,
            truncated: false,
        },
        clean_exit(),
    );
    info.id
}

fn assert_recovered_exit(recovered: &[RecoveredRun], id: ctxmux_protocol::RunId) {
    assert_eq!(
        recovered
            .iter()
            .find(|run| run.info.id == id)
            .expect("sentinel or primary Run recovers")
            .info
            .state,
        clean_exit()
    );
}

fn wait_for_direct_run_workers(run: &Arc<Run>) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Arc::strong_count(run) != 1 {
        assert!(
            Instant::now() < deadline,
            "native output and waiter workers retained the direct Run"
        );
        std::thread::yield_now();
    }
}

async fn wait_for_run_terminal_async(run: &Arc<Run>) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while run.info().state.is_running() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("Run publishes terminal state");
}

async fn stop_run_and_wait(manager: &RunManager, id: RunId) {
    manager
        .get(id)
        .expect("pin live Run for stop")
        .stop()
        .await
        .expect("stop live Run");
    wait_for_run_terminal_async(&manager.get(id).unwrap()).await;
}

async fn wait_for_run_workers(manager: &RunManager) {
    let runs = manager.registry.snapshot();
    tokio::time::timeout(Duration::from_secs(5), async {
        while runs.iter().any(|run| Arc::strong_count(run) != 2) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("native output and waiter workers release their Run owners");
}

const fn clean_exit() -> RunState {
    RunState::Exited {
        code: 0,
        signal: None,
    }
}

fn assert_protocol_code(error: ClientError, expected: ErrorCode) {
    match error {
        ClientError::Protocol { code, .. } => assert_eq!(code, expected),
        other => panic!("expected {expected:?} protocol error, got {other:?}"),
    }
}

fn assert_control_not_applied(error: ClientError, expected: ErrorCode) {
    match error {
        ClientError::ControlRejected { failure } => {
            assert_eq!(failure.error.code, expected);
            assert_eq!(failure.disposition, CommandDisposition::NotApplied);
        }
        other => panic!("expected {expected:?} not-applied control rejection, got {other:?}"),
    }
}

async fn assert_collecting_public_matrix(
    server: &InProcessServer,
    id: RunId,
    spec: &RunSpec,
    operation_key: &CreateOperationKey,
    fork_key: CreateOperationKey,
    rejected_marker: &std::path::Path,
    rejected_key: CreateOperationKey,
) {
    assert_eq!(server.client.status(id).await.unwrap().id, id);
    let retained = server.client.list().await.unwrap();
    assert_eq!(retained.len(), 1);
    assert_eq!(retained[0].id, id);
    let Err(attach_error) = server.client.attach(id, 0).await else {
        panic!("Collecting Run must reject Attach");
    };
    assert_protocol_code(attach_error, ErrorCode::BackendUnavailable);
    assert_control_not_applied(
        server
            .client
            .input(id, b"x".to_vec())
            .await
            .expect_err("Collecting Run rejects short control"),
        ErrorCode::BackendUnavailable,
    );
    assert_control_not_applied(
        server
            .client
            .resize(id, TerminalSize { cols: 81, rows: 25 })
            .await
            .expect_err("Collecting Run rejects Resize"),
        ErrorCode::BackendUnavailable,
    );
    assert_control_not_applied(
        server
            .client
            .stop(id)
            .await
            .expect_err("Collecting Run rejects Stop"),
        ErrorCode::BackendUnavailable,
    );
    assert_protocol_code(
        server
            .client
            .fork_with_operation_key(id, ForkPlan::LevelA, fork_key)
            .await
            .expect_err("Collecting Run rejects a fresh Fork"),
        ErrorCode::BackendUnavailable,
    );
    assert_protocol_code(
        server
            .client
            .start_with_operation_key(spec.clone(), operation_key.clone())
            .await
            .expect_err("Collecting key rejects its matching Start"),
        ErrorCode::BackendUnavailable,
    );
    assert_protocol_code(
        server
            .client
            .start_with_operation_key(long_running_spec(), operation_key.clone())
            .await
            .expect_err("Collecting key rejects a conflicting Start"),
        ErrorCode::CreationConflict,
    );
    assert_protocol_code(
        server
            .client
            .start_with_operation_key(marker_spec(rejected_marker, false), rejected_key)
            .await
            .expect_err("projected replacement leaves no second capacity slot"),
        ErrorCode::RunCapacity,
    );
    assert!(
        read_marker_pids(rejected_marker).is_empty(),
        "capacity rejection happens before physical spawn"
    );
}

async fn assert_removed_public_matrix(server: &InProcessServer, id: RunId) {
    assert_protocol_code(
        server
            .client
            .status(id)
            .await
            .expect_err("removed Run rejects Status"),
        ErrorCode::RunNotFound,
    );
    let Err(attach_error) = server.client.attach(id, 0).await else {
        panic!("removed Run must reject Attach");
    };
    assert_protocol_code(attach_error, ErrorCode::RunNotFound);
    assert_control_not_applied(
        server
            .client
            .input(id, b"x".to_vec())
            .await
            .expect_err("removed Run rejects short control"),
        ErrorCode::RunNotFound,
    );
    assert_control_not_applied(
        server
            .client
            .resize(id, TerminalSize { cols: 81, rows: 25 })
            .await
            .expect_err("removed Run rejects Resize"),
        ErrorCode::RunNotFound,
    );
    assert_control_not_applied(
        server
            .client
            .stop(id)
            .await
            .expect_err("removed Run rejects Stop"),
        ErrorCode::RunNotFound,
    );
    assert_protocol_code(
        server
            .client
            .fork(id, ForkPlan::LevelA)
            .await
            .expect_err("removed Run rejects a fresh Fork"),
        ErrorCode::RunNotFound,
    );
}

fn creation_hooked_server() -> (
    InProcessServer,
    Arc<CreationTestHook>,
    mpsc::UnboundedReceiver<()>,
) {
    let (hook, reached_rx) = creation_hook(CreationHookPoint::AfterPublication, true);
    let manager = Arc::new(RunManager {
        creation_hook: Some(Arc::clone(&hook)),
        ..RunManager::default()
    });
    (InProcessServer::start(manager), hook, reached_rx)
}

fn creation_hook(
    point: CreationHookPoint,
    armed: bool,
) -> (Arc<CreationTestHook>, mpsc::UnboundedReceiver<()>) {
    let (reached_tx, reached_rx) = mpsc::unbounded_channel();
    let hook = Arc::new(CreationTestHook {
        point,
        armed: AtomicBool::new(armed),
        physical_spawns: AtomicUsize::new(0),
        reached: reached_tx,
        released: Mutex::new(false),
        release: std::sync::Condvar::new(),
        captured_runs: Mutex::new(Vec::new()),
    });
    (hook, reached_rx)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_creation_retries_publish_exactly_one_run() {
    let server = InProcessServer::start(Arc::new(RunManager::default()));
    let marker = server.directory.path().join("concurrent-starts.log");
    let spec = marker_spec(&marker, true);
    let unrelated = UnrelatedProcess::spawn();
    let operation_key = CreateOperationKey::new("concurrent-public-start").unwrap();
    let barrier = Arc::new(Barrier::new(33));
    let mut attempts = Vec::new();
    for _ in 0..32 {
        let client = server.client.clone();
        let operation_key = operation_key.clone();
        let barrier = Arc::clone(&barrier);
        let spec = spec.clone();
        attempts.push(tokio::spawn(async move {
            barrier.wait().await;
            client.start_with_operation_key(spec, operation_key).await
        }));
    }
    barrier.wait().await;

    let mut runs = Vec::new();
    for attempt in attempts {
        runs.push(
            attempt
                .await
                .expect("creation task remains live")
                .expect("same canonical request converges"),
        );
    }
    let first = runs.first().expect("at least one creation result");
    assert!(runs.iter().all(|run| run.id == first.id));
    assert!(runs.iter().all(|run| run.pid == first.pid));
    assert_eq!(
        server
            .client
            .list()
            .await
            .unwrap()
            .into_iter()
            .map(|run| run.id.to_string())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([first.id.to_string()])
    );
    let pid = first.pid.expect("native Run exposes its PID");
    assert_eq!(wait_for_marker_pids(&marker, 1).await, vec![pid]);
    assert!(process_exists(pid), "the one published child remains live");
    assert!(process_exists(unrelated.pid()));
    server.client.stop(first.id).await.expect("stop one Run");
    assert!(process_exists(unrelated.pid()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn response_loss_retry_returns_the_published_run_without_respawn() {
    let (server, hook, mut reached) = creation_hooked_server();
    let marker = server.directory.path().join("response-loss-starts.log");
    let spec = marker_spec(&marker, true);
    let unrelated = UnrelatedProcess::spawn();
    let operation_key = CreateOperationKey::new("lost-start-response").unwrap();
    let client = server.client.clone();
    let first_key = operation_key.clone();
    let first_spec = spec.clone();
    let first =
        tokio::spawn(async move { client.start_with_operation_key(first_spec, first_key).await });
    tokio::time::timeout(Duration::from_secs(5), reached.recv())
        .await
        .expect("creation reaches post-publication response barrier")
        .expect("creation barrier remains connected");
    let published = server
        .client
        .list()
        .await
        .expect("published Run is visible before response");
    assert_eq!(published.len(), 1);
    let original = published[0].clone();

    first.abort();
    assert!(
        first
            .await
            .expect_err("lost response task is cancelled")
            .is_cancelled()
    );
    let retry_client = server.client.clone();
    let retry_key = operation_key.clone();
    let retry_spec = spec.clone();
    let retry = tokio::spawn(async move {
        retry_client
            .start_with_operation_key(retry_spec, retry_key)
            .await
    });
    tokio::task::yield_now().await;
    assert!(
        !retry.is_finished(),
        "same-key retry bypassed the held creation stripe"
    );
    hook.release();

    let retried = retry
        .await
        .expect("retry task remains live")
        .expect("retry resolves the published Run");
    assert_eq!(retried.id, original.id);
    assert_eq!(retried.pid, original.pid);
    assert_eq!(
        server
            .client
            .list()
            .await
            .unwrap()
            .into_iter()
            .map(|run| run.id.to_string())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([retried.id.to_string()])
    );
    assert_eq!(
        wait_for_marker_pids(&marker, 1).await,
        vec![retried.pid.expect("native Run PID")]
    );
    assert!(process_exists(retried.pid.expect("native Run PID")));
    assert!(process_exists(unrelated.pid()));
    server
        .client
        .stop(retried.id)
        .await
        .expect("stop retried Run");
    assert!(process_exists(unrelated.pid()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conflicting_creation_key_reuse_is_typed_and_does_not_spawn() {
    let server = InProcessServer::start(Arc::new(RunManager::default()));
    let marker = server.directory.path().join("conflict-starts.log");
    let spec = marker_spec(&marker, true);
    let unrelated = UnrelatedProcess::spawn();
    let operation_key = CreateOperationKey::new("typed-creation-conflict").unwrap();
    let original = server
        .client
        .start_with_operation_key(spec.clone(), operation_key.clone())
        .await
        .expect("start conflict owner");
    let mut conflict_spec = spec;
    conflict_spec.args.push("different".to_owned());
    let error = server
        .client
        .start_with_operation_key(conflict_spec, operation_key)
        .await
        .expect_err("different request conflicts");
    assert!(matches!(
        error,
        ClientError::Protocol {
            code: ErrorCode::CreationConflict,
            ..
        }
    ));
    assert_eq!(
        server
            .client
            .list()
            .await
            .unwrap()
            .into_iter()
            .map(|run| run.id.to_string())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([original.id.to_string()])
    );
    assert_eq!(
        wait_for_marker_pids(&marker, 1).await,
        vec![original.pid.expect("original PID")]
    );
    assert!(process_exists(original.pid.expect("original PID")));
    assert!(process_exists(unrelated.pid()));
    server
        .client
        .stop(original.id)
        .await
        .expect("stop original Run");
    assert!(process_exists(unrelated.pid()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_unpublished_creation_does_not_consume_the_key() {
    let server = InProcessServer::start(Arc::new(RunManager::default()));
    let operation_key = CreateOperationKey::new("retry-after-failed-spawn").unwrap();
    let mut missing = long_running_spec();
    missing.program = "/ctxmux/fixture/does-not-exist".to_owned();
    let error = server
        .client
        .start_with_operation_key(missing, operation_key.clone())
        .await
        .expect_err("missing executable rejects creation");
    assert!(matches!(
        error,
        ClientError::Protocol {
            code: ErrorCode::SpawnFailed,
            ..
        }
    ));
    assert!(server.client.list().await.unwrap().is_empty());

    let retried = server
        .client
        .start_with_operation_key(long_running_spec(), operation_key)
        .await
        .expect("uncommitted failure releases its key");
    assert_eq!(server.client.list().await.unwrap().len(), 1);
    server
        .client
        .stop(retried.id)
        .await
        .expect("stop retry Run");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn creation_owner_panic_after_spawn_transfers_cleanup_and_preserves_key_safety() {
    let temp = tempfile::tempdir().expect("create panic rollback fixture");
    let marker = temp.path().join("panic-after-spawn.log");
    let (hook, mut reached) = creation_hook(CreationHookPoint::PanicAfterSpawn, true);
    let manager = Arc::new(RunManager {
        creation_hook: Some(Arc::clone(&hook)),
        ..RunManager::default()
    });
    let unrelated = UnrelatedProcess::spawn();
    let operation_key = CreateOperationKey::new("panic-after-spawn-owner").unwrap();
    let spec = marker_spec(&marker, true);
    let request_manager = Arc::clone(&manager);
    let request_key = operation_key.clone();
    let request_spec = spec.clone();
    let request = tokio::spawn(async move {
        request_manager
            .create(request_key, CreationRequest::Start { spec: request_spec })
            .await
    });

    wait_for_spawned_marker(&mut reached, &marker, 1).await;
    let rejected_pid = read_marker_pids(&marker)[0];
    assert!(process_exists(rejected_pid));
    hook.release();

    let error = request
        .await
        .expect("request task observes the creation owner result")
        .expect_err("panicked creation owner cannot publish a Run");
    assert_eq!(error.code, ErrorCode::Internal);
    assert!(manager.list().is_empty());
    assert_eq!(manager.unpublished_cleanups.unresolved_count(), 1);
    assert_eq!(manager.unpublished_cleanups.owned_count(), 1);
    let retry = manager
        .create(
            operation_key.clone(),
            CreationRequest::Start { spec: spec.clone() },
        )
        .await
        .expect_err("matching retry remains fenced by the captured cleanup owner");
    assert_eq!(retry.code, ErrorCode::BackendUnavailable);
    let mut conflicting = spec.clone();
    conflicting.args.push("different".to_owned());
    let conflict = manager
        .create(
            operation_key.clone(),
            CreationRequest::Start { spec: conflicting },
        )
        .await
        .expect_err("conflicting retry observes the same exact-key fence");
    assert_eq!(conflict.code, ErrorCode::CreationConflict);
    assert_eq!(read_marker_pids(&marker), vec![rejected_pid]);
    hook.release_captured_runs();
    wait_for_rejected_children_reaped(&manager, &marker).await;
    assert_eq!(manager.unpublished_cleanups.owned_count(), 0);
    assert!(process_exists(unrelated.pid()));

    let retried = manager
        .create(
            operation_key.clone(),
            CreationRequest::Start { spec: spec.clone() },
        )
        .await
        .expect("same key is reusable only after the panicked child is reaped");
    let pids = wait_for_marker_pids(&marker, 2).await;
    assert_eq!(pids[0], rejected_pid);
    assert_eq!(pids[1], retried.pid.expect("retried Run has a child PID"));
    assert!(!process_exists(rejected_pid));
    assert!(process_exists(pids[1]));
    let converged = manager
        .create(operation_key, CreationRequest::Start { spec })
        .await
        .expect("matching retry converges on the one replacement process");
    assert_eq!(converged.id, retried.id);
    assert_eq!(wait_for_marker_pids(&marker, 2).await, pids);

    manager
        .get(retried.id)
        .expect("resolve replacement Run")
        .stop()
        .await
        .expect("stop replacement Run");
    wait_for_run_terminal_async(&manager.get(retried.id).unwrap()).await;
    assert!(process_exists(unrelated.pid()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn persistent_commit_unwind_never_restores_the_registry_reservation() {
    for (index, point) in [
        CreationHookPoint::PanicAfterPersistentCommit,
        CreationHookPoint::PanicBeforePersistentRegistryConsume,
    ]
    .into_iter()
    .enumerate()
    {
        let fixture = trigger_persistent_commit_unwind(index, point).await;
        assert_persistent_commit_unwind_converges(index, fixture).await;
    }
}

struct PersistentCommitUnwindFixture {
    _temp: tempfile::TempDir,
    state_dir: std::path::PathBuf,
    marker: std::path::PathBuf,
    persistence: Persistence,
    hook: Arc<CreationTestHook>,
    manager: Arc<RunManager>,
    old: RunInfo,
    old_key: CreateOperationKey,
    old_spec: RunSpec,
    operation_key: CreateOperationKey,
    spec: RunSpec,
    committed_pid: u32,
}

async fn trigger_persistent_commit_unwind(
    index: usize,
    point: CreationHookPoint,
) -> PersistentCommitUnwindFixture {
    let temp = tempfile::tempdir().expect("create committed unwind fixture");
    let state_dir = temp.path().join(format!("state-{index}"));
    let marker = temp.path().join(format!("committed-unwind-{index}.log"));
    let (persistence, recovered) =
        Persistence::open_with_test_limits(state_dir.clone(), 1, 64 * 1024 * 1024)
            .expect("open committed unwind persistence");
    assert!(recovered.is_empty());
    let (hook, mut reached) = creation_hook(point, false);
    let manager = Arc::new(RunManager {
        registry: RunRegistry::with_record_capacity(1),
        creation_hook: Some(Arc::clone(&hook)),
        ..RunManager::persistent(persistence.clone(), recovered)
    });
    let old_key = CreateOperationKey::new(format!("old-before-unwind-{index}")).unwrap();
    let old_spec = short_lived_spec();
    let old = manager
        .create(
            old_key.clone(),
            CreationRequest::Start {
                spec: old_spec.clone(),
            },
        )
        .await
        .expect("create one exact terminal replacement candidate");
    wait_for_run_terminal_async(&manager.get(old.id).unwrap()).await;
    wait_for_run_workers(&manager).await;
    hook.arm();
    let operation_key = CreateOperationKey::new(format!("committed-unwind-{index}")).unwrap();
    let spec = marker_spec(&marker, true);
    let request_manager = Arc::clone(&manager);
    let request_key = operation_key.clone();
    let request_spec = spec.clone();
    let request = tokio::spawn(async move {
        request_manager
            .create(request_key, CreationRequest::Start { spec: request_spec })
            .await
    });
    wait_for_spawned_marker(&mut reached, &marker, 1).await;
    let committed_pid = read_marker_pids(&marker)[0];
    hook.release();
    let error = request
        .await
        .expect("request task observes committed owner unwind")
        .expect_err("committed owner unwind cannot report publication success");
    assert_eq!(error.code, ErrorCode::Internal);
    PersistentCommitUnwindFixture {
        _temp: temp,
        state_dir,
        marker,
        persistence,
        hook,
        manager,
        old,
        old_key,
        old_spec,
        operation_key,
        spec,
        committed_pid,
    }
}

async fn assert_persistent_commit_unwind_converges(
    index: usize,
    fixture: PersistentCommitUnwindFixture,
) {
    let PersistentCommitUnwindFixture {
        _temp,
        state_dir,
        marker,
        persistence,
        hook,
        manager,
        old,
        old_key,
        old_spec,
        operation_key,
        spec,
        committed_pid,
    } = fixture;
    assert_eq!(
        manager
            .list()
            .iter()
            .map(|info| info.id)
            .collect::<Vec<_>>(),
        vec![old.id]
    );
    assert_eq!(mutex_lock(&manager.commit_unknown_reservations).len(), 1);
    assert!(mutex_lock(&manager.incarnation_failure.message).is_some());
    let old_retry = manager
        .create(old_key.clone(), CreationRequest::Start { spec: old_spec })
        .await
        .expect_err("durably removed old key remains fenced in this incarnation");
    assert_eq!(old_retry.code, ErrorCode::BackendUnavailable);
    let retry = manager
        .create(
            operation_key.clone(),
            CreationRequest::Start { spec: spec.clone() },
        )
        .await
        .expect_err("known COMMIT unwind retains the exact new-key fence");
    assert_eq!(retry.code, ErrorCode::BackendUnavailable);
    let unrelated = manager
        .create(
            CreateOperationKey::new(format!("unrelated-after-commit-{index}")).unwrap(),
            CreationRequest::Start {
                spec: short_lived_spec(),
            },
        )
        .await
        .expect_err("known COMMIT unwind fences new creation admission");
    assert_eq!(unrelated.code, ErrorCode::BackendUnavailable);
    assert_eq!(read_marker_pids(&marker), vec![committed_pid]);
    assert_persistent_run_key_absent(&state_dir, old.id, &old_key);
    assert_eq!(persistent_key_count(&state_dir, &operation_key), 1);
    hook.release_captured_runs();
    wait_for_rejected_children_reaped(&manager, &marker).await;
    assert!(!process_exists(committed_pid));
    drop(manager);
    persistence.assert_exclusive_owner();
    drop(persistence);

    let (persistence, recovered) =
        Persistence::open(&state_dir).expect("restart resolves committed unwind from SQLite");
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].operation_key, operation_key);
    assert_ne!(recovered[0].info.id, old.id);
    assert_eq!(
        recovered[0].info.state,
        RunState::Interrupted {
            reason: InterruptionReason::DaemonRestart
        }
    );
    let recovered_id = recovered[0].info.id;
    let restarted = Arc::new(RunManager::persistent(persistence, recovered));
    let converged = restarted
        .create(operation_key, CreationRequest::Start { spec })
        .await
        .expect("restart retry resolves the one durably committed Run");
    assert_eq!(converged.id, recovered_id);
    assert_eq!(read_marker_pids(&marker), vec![committed_pid]);
}

fn persistent_key_count(state_dir: &std::path::Path, key: &CreateOperationKey) -> i64 {
    let connection = rusqlite::Connection::open(state_dir.join("state.sqlite3"))
        .expect("inspect committed replacement row");
    connection
        .query_row(
            "SELECT count(*) FROM runs WHERE creation_key = ?1 COLLATE BINARY",
            [key.as_str()],
            |row| row.get(0),
        )
        .expect("count the committed replacement key")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn persistent_capacity_rejects_before_spawn_without_consuming_key() {
    let temp = tempfile::tempdir().expect("create capacity fixture");
    let marker = temp.path().join("capacity-starts.log");
    let (persistence, recovered) =
        Persistence::open_with_test_limits(temp.path().join("state"), 16, 4 * 1024)
            .expect("open metadata-bounded persistence");
    assert!(recovered.is_empty());
    let manager = Arc::new(RunManager::persistent(persistence.clone(), recovered));
    let sentinel = UnrelatedProcess::spawn();
    let operation_key = CreateOperationKey::new("capacity-rejected-owner").unwrap();
    let error = manager
        .create(
            operation_key.clone(),
            CreationRequest::Start {
                spec: oversized_hup_ignoring_marker_spec(&marker),
            },
        )
        .await
        .expect_err("oversized metadata rejects before physical spawn");
    assert_eq!(error.code, ErrorCode::RunCapacity);
    assert!(error.message.contains("metadata"));
    assert!(read_marker_pids(&marker).is_empty());
    assert!(manager.list().is_empty());
    assert_eq!(manager.unpublished_cleanups.unresolved_count(), 0);
    assert_eq!(manager.unpublished_cleanups.owned_count(), 0);
    assert!(!persistence.is_failed());
    assert!(process_exists(sentinel.pid()));

    let smaller_spec = marker_spec(&marker, false);
    let admitted = manager
        .create(
            operation_key.clone(),
            CreationRequest::Start {
                spec: smaller_spec.clone(),
            },
        )
        .await
        .expect("a pre-spawn rejection leaves its exact key reusable");
    wait_for_run_terminal_async(&manager.get(admitted.id).unwrap()).await;
    wait_for_run_workers(&manager).await;
    let pids = wait_for_marker_pids(&marker, 1).await;
    assert_eq!(pids, vec![admitted.pid.expect("admitted Run has a PID")]);
    let retried = manager
        .create(operation_key, CreationRequest::Start { spec: smaller_spec })
        .await
        .expect("matching retry converges on the one admitted physical Run");
    assert_eq!(retried.id, admitted.id);
    assert_eq!(wait_for_marker_pids(&marker, 1).await, pids);
    assert!(!persistence.is_failed());
    assert!(process_exists(sentinel.pid()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejected_persistent_fork_cleans_only_the_unpublished_child() {
    let temp = tempfile::tempdir().expect("create rejected fork fixture");
    let marker = temp.path().join("rejected-forks.log");
    let (persistence, recovered) =
        Persistence::open_with_test_limits(temp.path().join("state"), 16, 64 * 1024 * 1024)
            .expect("open rollback fixture persistence");
    let (hook, mut reached) = creation_hook(CreationHookPoint::AfterSpawnWithRunHold, false);
    let manager = Arc::new(RunManager {
        creation_hook: Some(Arc::clone(&hook)),
        ..RunManager::persistent(persistence.clone(), recovered)
    });
    let parent = manager
        .create(
            CreateOperationKey::new("fork-parent").unwrap(),
            CreationRequest::Start {
                spec: long_running_spec(),
            },
        )
        .await
        .expect("publish a small live parent");
    let sentinel = UnrelatedProcess::spawn();
    let operation_key = CreateOperationKey::new("rejected-fork-owner").unwrap();
    let spec = hup_ignoring_marker_spec(&marker);
    persistence.fail_next_start_before_commit();
    hook.arm();
    let fork_manager = Arc::clone(&manager);
    let fork_key = operation_key.clone();
    let fork_spec = spec.clone();
    let fork = tokio::spawn(async move {
        fork_manager
            .create(
                fork_key,
                CreationRequest::Fork {
                    parent: parent.id,
                    plan: ForkPlan::LevelB { spec: fork_spec },
                },
            )
            .await
    });
    wait_for_spawned_marker(&mut reached, &marker, 1).await;
    hook.release();
    let error = fork
        .await
        .expect("fork owner task remains live")
        .expect_err("injected pre-COMMIT failure rejects the unpublished fork");
    assert_pending_rollback(&error);
    assert_eq!(
        manager.list().iter().map(|run| run.id).collect::<Vec<_>>(),
        vec![parent.id]
    );
    assert_eq!(manager.unpublished_cleanups.unresolved_count(), 1);
    assert!(manager.get(parent.id).unwrap().has_continuation_authority());
    assert!(process_exists(parent.pid.unwrap()));
    assert!(process_exists(sentinel.pid()));
    assert!(!persistence.is_failed());
    hook.release_captured_runs();
    wait_for_rejected_children_reaped(&manager, &marker).await;
    manager
        .get(parent.id)
        .unwrap()
        .stop()
        .await
        .expect("stop retained parent");
    assert!(process_exists(sentinel.pid()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_reports_an_exact_unresolved_cleanup_fence() {
    let temp = tempfile::tempdir().expect("create cleanup shutdown fixture");
    let marker = temp.path().join("shutdown-cleanup.log");
    let (persistence, recovered) =
        Persistence::open_with_test_limits(temp.path().join("state"), 16, 64 * 1024 * 1024)
            .expect("open rollback fixture persistence");
    persistence.fail_next_start_before_commit();
    let (hook, mut reached) = creation_hook(CreationHookPoint::AfterSpawnWithRunHold, true);
    let manager = Arc::new(RunManager {
        creation_hook: Some(Arc::clone(&hook)),
        ..RunManager::persistent(persistence, recovered)
    });
    let operation_key = CreateOperationKey::new("shutdown-unresolved-owner").unwrap();
    let request_manager = Arc::clone(&manager);
    let request_key = operation_key.clone();
    let request_marker = marker.clone();
    let request = tokio::spawn(async move {
        request_manager
            .create(
                request_key,
                CreationRequest::Start {
                    spec: hup_ignoring_marker_spec(&request_marker),
                },
            )
            .await
    });
    wait_for_spawned_marker(&mut reached, &marker, 1).await;
    hook.release();
    let error = request
        .await
        .expect("creation task remains live")
        .expect_err("injected pre-COMMIT failure rejects the unpublished start");
    assert_pending_rollback(&error);
    assert_eq!(manager.unpublished_cleanups.unresolved_count(), 1);
    let error = manager
        .shutdown_owned_controls(Duration::ZERO)
        .expect_err("zero-budget shutdown reports the unresolved cleanup");
    let ServerError::Shutdown { failures } = error else {
        panic!("cleanup failure uses the shutdown aggregate");
    };
    assert!(!failures.contains(operation_key.as_str()));
    assert!(failures.contains("unpublished Run"));
    assert!(failures.contains("exact-key fence"));
    assert!(
        failures.contains("child waiter has not yet proven reap")
            || failures.contains("reader, waiter, or external owners"),
        "shutdown reports the exact unresolved cleanup owner: {failures}"
    );
    hook.release_captured_runs();
    wait_for_rejected_children_reaped(&manager, &marker).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn new_key_admission_prunes_reaped_fences_across_all_old_keys() {
    let temp = tempfile::tempdir().expect("create cross-key cleanup fixture");
    let marker = temp.path().join("cross-key-cleanups.log");
    let (persistence, recovered) =
        Persistence::open_with_test_limits(temp.path().join("state"), 16, 64 * 1024 * 1024)
            .expect("open rollback fixture persistence");
    let (hook, mut reached) = creation_hook(CreationHookPoint::AfterSpawnWithRunHold, true);
    let manager = Arc::new(RunManager {
        creation_hook: Some(Arc::clone(&hook)),
        ..RunManager::persistent(persistence.clone(), recovered)
    });

    for index in 0..8 {
        if index != 0 {
            hook.arm();
        }
        persistence.fail_next_start_before_commit();
        let request_manager = Arc::clone(&manager);
        let request_marker = marker.clone();
        let request = tokio::spawn(async move {
            request_manager
                .create(
                    CreateOperationKey::new(format!("stale-cleanup-{index}")).unwrap(),
                    CreationRequest::Start {
                        spec: hup_ignoring_marker_spec(&request_marker),
                    },
                )
                .await
        });
        wait_for_spawned_marker(&mut reached, &marker, index + 1).await;
        hook.release();
        let error = request
            .await
            .expect("creation task remains live")
            .expect_err("injected pre-COMMIT failure rejects the unpublished start");
        assert_pending_rollback(&error);
    }
    assert_eq!(read_marker_pids(&marker).len(), 8);
    assert_eq!(manager.unpublished_cleanups.unresolved_count(), 8);
    assert_eq!(manager.unpublished_cleanups.owned_count(), 8);
    wait_for_captured_runs_backend_quiescent(&hook).await;
    assert_marker_pids_gone(&marker);
    hook.release_captured_runs();

    let ninth = manager
        .create(
            CreateOperationKey::new("new-key-after-eight-reaps").unwrap(),
            CreationRequest::Start {
                spec: short_lived_spec(),
            },
        )
        .await
        .expect("new-key admission prunes completed old-key fences");
    wait_for_run_terminal_async(&manager.get(ninth.id).unwrap()).await;
    assert!(!persistence.is_failed());
    wait_for_rejected_children_reaped(&manager, &marker).await;
    assert_eq!(manager.unpublished_cleanups.owned_count(), 0);
}

async fn wait_for_captured_runs_backend_quiescent(hook: &CreationTestHook) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !hook.captured_runs_are_backend_quiescent() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("captured unpublished Runs reach Backend-local quiescence");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn eight_pending_cleanup_owners_reject_a_ninth_key_before_spawn() {
    let temp = tempfile::tempdir().expect("create pending cleanup bound fixture");
    let marker = temp.path().join("ninth-cleanup-launch.log");
    let manager = Arc::new(RunManager::default());
    let request = CreationRequest::Start {
        spec: long_running_spec(),
    };
    let mut pending = Vec::new();
    for index in 0..8 {
        let key = CreateOperationKey::new(format!("pending-cleanup-{index}")).unwrap();
        manager
            .unpublished_cleanups
            .resolve_fence(&key, &request)
            .unwrap();
        let reservation = manager.unpublished_cleanups.reserve(&key).unwrap();
        let run = Run::spawn(
            long_running_spec(),
            None,
            PersistenceMode::MemoryOnly,
            LIVE_EVENT_CAPACITY,
            manager.native_input_drains.clone(),
        )
        .expect("spawn real pending cleanup child");
        reservation.transfer(
            request.clone(),
            Arc::clone(&run),
            "fixture pending cleanup".to_owned(),
        );
        pending.push(run);
    }
    assert_eq!(manager.unpublished_cleanups.unresolved_count(), 8);
    assert_eq!(manager.unpublished_cleanups.owned_count(), 8);

    let error = manager
        .create(
            CreateOperationKey::new("ninth-pending-cleanup").unwrap(),
            CreationRequest::Start {
                spec: marker_spec(&marker, true),
            },
        )
        .await
        .expect_err("ninth key is rejected before physical launch");
    assert_eq!(error.code, ErrorCode::BackendUnavailable);
    assert!(read_marker_pids(&marker).is_empty());
    assert!(manager.list().is_empty());

    for run in &pending {
        run.stop()
            .await
            .expect("stop pending cleanup fixture child");
        wait_for_run_terminal_async(run).await;
    }
    drop(pending);
    wait_for_unpublished_cleanups(&manager, 0).await;
    assert_eq!(manager.unpublished_cleanups.owned_count(), 0);
}

fn marker_spec(marker: &std::path::Path, keep_running: bool) -> RunSpec {
    let script = if keep_running {
        "printf '%s\\n' \"$$\" >> \"$CTXMUX_CREATION_MARKER\"; exec /bin/cat"
    } else {
        "printf '%s\\n' \"$$\" >> \"$CTXMUX_CREATION_MARKER\""
    };
    RunSpec {
        program: "/bin/sh".to_owned(),
        args: vec!["-c".to_owned(), script.to_owned()],
        cwd: None,
        env: BTreeMap::from([(
            "CTXMUX_CREATION_MARKER".to_owned(),
            marker.to_string_lossy().into_owned(),
        )]),
        size: TerminalSize::default(),
        declared_inputs: Vec::new(),
    }
}

fn hup_ignoring_marker_spec(marker: &std::path::Path) -> RunSpec {
    RunSpec {
        program: "/bin/sh".to_owned(),
        args: vec![
            "-c".to_owned(),
            "trap '' HUP; printf '%s\\n' \"$$\" >> \"$CTXMUX_CREATION_MARKER\"; exec /bin/sleep 30"
                .to_owned(),
        ],
        cwd: None,
        env: BTreeMap::from([(
            "CTXMUX_CREATION_MARKER".to_owned(),
            marker.to_string_lossy().into_owned(),
        )]),
        size: TerminalSize::default(),
        declared_inputs: Vec::new(),
    }
}

fn oversized_hup_ignoring_marker_spec(marker: &std::path::Path) -> RunSpec {
    let mut spec = hup_ignoring_marker_spec(marker);
    spec.env
        .insert("CTXMUX_OVERSIZED_METADATA".to_owned(), "x".repeat(8 * 1024));
    spec
}

fn assert_persistent_run_key_absent(
    state_dir: &std::path::Path,
    id: RunId,
    operation_key: &CreateOperationKey,
) {
    let connection = rusqlite::Connection::open(state_dir.join("state.sqlite3"))
        .expect("open persistent exact-replacement fixture");
    let id_rows: i64 = connection
        .query_row(
            "SELECT count(*) FROM runs WHERE id = ?1",
            [id.to_string()],
            |row| row.get(0),
        )
        .expect("count persistent Run identity");
    let key_rows: i64 = connection
        .query_row(
            "SELECT count(*) FROM runs WHERE creation_key = ?1 COLLATE BINARY",
            [operation_key.as_str()],
            |row| row.get(0),
        )
        .expect("count persistent creation key");
    assert_eq!((id_rows, key_rows), (0, 0));
}

fn assert_persistent_run_key_present(
    state_dir: &std::path::Path,
    id: RunId,
    operation_key: &CreateOperationKey,
) {
    let connection = rusqlite::Connection::open(state_dir.join("state.sqlite3"))
        .expect("open persistent exact-replacement fixture");
    let exact_rows: i64 = connection
        .query_row(
            "SELECT count(*) FROM runs WHERE id = ?1 AND creation_key = ?2 COLLATE BINARY",
            rusqlite::params![id.to_string(), operation_key.as_str()],
            |row| row.get(0),
        )
        .expect("count exact persistent Run/key unit");
    assert_eq!(exact_rows, 1);
}

fn assert_pending_rollback(error: &ProtocolError) {
    assert_eq!(error.code, ErrorCode::Persistence);
    assert!(error.message.contains("rollback pending"));
    assert!(error.message.contains("exact creation key remains fenced"));
}

async fn wait_for_spawned_marker(
    reached: &mut mpsc::UnboundedReceiver<()>,
    marker: &std::path::Path,
    expected: usize,
) {
    tokio::time::timeout(Duration::from_secs(5), reached.recv())
        .await
        .expect("creation reaches the post-spawn barrier")
        .expect("creation barrier remains connected");
    let _ = wait_for_marker_pids(marker, expected).await;
}

fn read_marker_pids(marker: &std::path::Path) -> Vec<u32> {
    fs::read_to_string(marker)
        .unwrap_or_default()
        .lines()
        .map(|line| line.parse::<u32>().expect("marker records a child PID"))
        .collect()
}

fn assert_marker_pids_gone(marker: &std::path::Path) {
    for pid in read_marker_pids(marker) {
        assert!(
            !process_exists(pid),
            "rejected unpublished child PID {pid} remains live after waiter reap"
        );
    }
}

async fn wait_for_unpublished_cleanups(manager: &RunManager, expected: usize) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if manager.unpublished_cleanups.unresolved_count() == expected {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("unpublished cleanup accounting reaches the expected count");
}

async fn wait_for_rejected_children_reaped(manager: &RunManager, marker: &std::path::Path) {
    wait_for_unpublished_cleanups(manager, 0).await;
    assert_marker_pids_gone(marker);
}

async fn wait_for_marker_pids(marker: &std::path::Path, expected: usize) -> Vec<u32> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let content = fs::read_to_string(marker).unwrap_or_default();
            let pids = content
                .lines()
                .map(|line| line.parse::<u32>().expect("marker records a child PID"))
                .collect::<Vec<_>>();
            if pids.len() == expected {
                return pids;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("marker reaches the expected execution count")
}

struct UnrelatedProcess(std::process::Child);

impl UnrelatedProcess {
    fn spawn() -> Self {
        Self(
            Command::new("/bin/sleep")
                .arg("30")
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("spawn unrelated process sentinel"),
        )
    }

    fn pid(&self) -> u32 {
        self.0.id()
    }
}

impl Drop for UnrelatedProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
