use super::*;

fn echo_spec(name: &str) -> RunSpec {
    RunSpec {
        program: "/usr/bin/python3".to_owned(),
        args: vec![
            "-u".to_owned(),
            "-c".to_owned(),
            format!(
                "import os,termios\na=termios.tcgetattr(0);a[3]&=~(termios.ICANON|termios.ECHO);a[1]&=~termios.ONLCR;a[6][termios.VMIN]=1;a[6][termios.VTIME]=0\ntermios.tcsetattr(0,termios.TCSANOW,a)\nos.write(1,b'{name}:READY\\n')\nwhile True:\n b=os.read(0,1)\n if not b:break\n os.write(1,b'{name}:'+b+b'\\n')\n"
            ),
        ],
        cwd: None,
        env: BTreeMap::new(),
        initial_size: TerminalSize::default(),
        declared_inputs: Vec::new(),
    }
}

async fn wait_durable(client: &Client, id: RunId, through: u64) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let (_, storage) = client.observe_storage(id).await.unwrap();
            let owner = storage.persistence.unwrap();
            if owner.first_failure.is_none() && owner.committed_output_bytes == through {
                assert_eq!(owner.storage_failure, None);
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("original public Run must settle retained bytes without resubmission");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_unknown_append_preserves_two_live_runs_and_cold_bytes() {
    let state = tempfile::tempdir().unwrap();
    let state_dir = state.path().join("state");
    let (persistence, recovered) = Persistence::open(&state_dir).unwrap();
    let server = InProcessServer::start(Arc::new(RunManager::persistent(
        persistence.clone(),
        recovered,
    )));
    let runtime = server.client.runtime_info().await.unwrap();
    let first = server.client.start(echo_spec("A")).await.unwrap();
    let second = server.client.start(echo_spec("B")).await.unwrap();
    let mut first_bytes = b"A:READY\n".to_vec();
    let mut second_bytes = b"B:READY\n".to_vec();
    expect_original_raw(&server.client, &first, &first_bytes).await;
    expect_original_raw(&server.client, &second, &second_bytes).await;
    wait_durable(&server.client, first.id, first_bytes.len() as u64).await;
    wait_durable(&server.client, second.id, second_bytes.len() as u64).await;
    for (committed, byte) in [(false, b'x'), (true, b'y')] {
        persistence.hold_maintenance_commit_probe_for_test(committed);
        assert_eq!(
            server
                .client
                .input(first.id, vec![byte])
                .await
                .unwrap()
                .receipt
                .written_bytes,
            1
        );
        first_bytes.extend_from_slice(&[b'A', b':', byte, b'\n']);
        tokio::time::timeout(Duration::from_secs(5), async {
            while !persistence.is_failed() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let (_, fault) = server.client.observe_storage(first.id).await.unwrap();
        assert!(fault.persistence.unwrap().storage_failure.is_some());
        assert_eq!(server.client.status(first.id).await.unwrap().pid, first.pid);
        assert_eq!(
            server.client.status(second.id).await.unwrap().pid,
            second.pid
        );
        assert!(process_exists(first.pid.unwrap()) && process_exists(second.pid.unwrap()));
        assert_eq!(
            server
                .client
                .input(second.id, vec![byte])
                .await
                .unwrap()
                .receipt
                .written_bytes,
            1
        );
        second_bytes.extend_from_slice(&[b'B', b':', byte, b'\n']);
        expect_original_raw(&server.client, &first, &first_bytes).await;
        expect_original_raw(&server.client, &second, &second_bytes).await;
        persistence.release_maintenance_commit_probe_for_test();
        wait_durable(&server.client, first.id, first_bytes.len() as u64).await;
        wait_durable(&server.client, second.id, second_bytes.len() as u64).await;
        assert_eq!(server.client.runtime_info().await.unwrap(), runtime);
    }
    for run in [&first, &second] {
        server
            .client
            .stop(fresh_stop(&server.client, run.id).await)
            .await
            .unwrap();
        wait_for_exit(&server.client, run.id).await;
        assert!(!process_exists(run.pid.unwrap()));
    }
    server.abort_and_wait().await;
    drop(persistence);
    let expected = state.path().join("expected.json");
    std::fs::write(
        &expected,
        serde_json::to_vec(&serde_json::json!({
            "runtime": runtime.runtime_id,
            "runs": [[first.id.to_string(), first_bytes], [second.id.to_string(), second_bytes]],
        }))
        .unwrap(),
    )
    .unwrap();
    let result = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "tests::persistence_reconciliation::unknown_output_cold_reader",
            "--exact",
            "--nocapture",
        ])
        .env("CTXMUX_RECONCILIATION_STATE", &state_dir)
        .env("CTXMUX_RECONCILIATION_EXPECTED", &expected)
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "actual cold process failed: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(String::from_utf8_lossy(&result.stdout).contains("1 passed"));
}

#[test]
fn unknown_output_cold_reader() {
    let Some(state) = std::env::var_os("CTXMUX_RECONCILIATION_STATE") else {
        return;
    };
    let expected: serde_json::Value = serde_json::from_slice(
        &std::fs::read(std::env::var_os("CTXMUX_RECONCILIATION_EXPECTED").unwrap()).unwrap(),
    )
    .unwrap();
    let (owner, recovered) = Persistence::open(std::path::Path::new(&state)).unwrap();
    assert_eq!(
        serde_json::to_value(owner.runtime_id()).unwrap(),
        expected["runtime"]
    );
    for pair in expected["runs"].as_array().unwrap() {
        let id: RunId = pair[0].as_str().unwrap().parse().unwrap();
        let bytes: Vec<u8> = serde_json::from_value(pair[1].clone()).unwrap();
        let original = recovered.iter().find(|run| run.info.id == id).unwrap();
        assert_eq!(
            original
                .replay
                .chunks
                .iter()
                .flat_map(|chunk| chunk.data.iter().copied())
                .collect::<Vec<_>>(),
            bytes
        );
        assert!(!original.info.state.is_running());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_unknown_finalize_preserves_physical_stop_and_healthy_control() {
    for committed in [false, true] {
        let state = tempfile::tempdir().unwrap();
        let (persistence, recovered) = Persistence::open(state.path().join("state")).unwrap();
        let server = InProcessServer::start(Arc::new(RunManager::persistent(
            persistence.clone(),
            recovered,
        )));
        let first = server.client.start(echo_spec("A")).await.unwrap();
        let second = server.client.start(echo_spec("B")).await.unwrap();
        expect_original_raw(&server.client, &first, b"A:READY\n").await;
        expect_original_raw(&server.client, &second, b"B:READY\n").await;
        wait_durable(&server.client, first.id, 8).await;
        wait_durable(&server.client, second.id, 8).await;
        let operation = fresh_stop(&server.client, first.id).await;
        persistence.hold_maintenance_commit_probe_for_test(committed);
        let client = server.client.clone();
        let stopping = tokio::spawn(async move { client.stop(operation).await });
        tokio::time::timeout(Duration::from_secs(5), async {
            while !persistence.is_failed() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("actual terminal transaction reaches the unknown boundary");
        assert!(
            !process_exists(first.pid.unwrap()),
            "physical Stop already occurred; no retry may signal a replacement"
        );
        assert_eq!(
            server
                .client
                .input(second.id, b"h".to_vec())
                .await
                .unwrap()
                .receipt
                .written_bytes,
            1
        );
        expect_original_raw(&server.client, &second, b"B:READY\nB:h\n").await;
        let (_, observation) = server.client.observe_storage(first.id).await.unwrap();
        assert!(observation.persistence.unwrap().first_failure.is_some());
        persistence.release_maintenance_commit_probe_for_test();
        tokio::time::timeout(Duration::from_secs(5), stopping)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        wait_for_exit(&server.client, first.id).await;
        wait_durable(&server.client, first.id, 8).await;
        wait_durable(&server.client, second.id, 12).await;
        assert_eq!(server.client.status(first.id).await.unwrap().pid, first.pid);
        assert_eq!(
            server.client.status(second.id).await.unwrap().pid,
            second.pid
        );
        server
            .client
            .stop(fresh_stop(&server.client, second.id).await)
            .await
            .unwrap();
        wait_for_exit(&server.client, second.id).await;
    }
}
