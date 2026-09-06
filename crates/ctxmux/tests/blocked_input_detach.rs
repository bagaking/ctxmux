//! A disposable CLI view must leave a blocked Run without fabricating its input result.
use ctxmux_client::{Client, ClientError};
use ctxmux_protocol::{CommandDisposition, RunId, RunSpec, RunState, TerminalSize};
use portable_pty::{Child, CommandBuilder, PtyPair, PtySize, native_pty_system};
use std::{
    collections::BTreeMap,
    io::{Read, Write},
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

type Observed = Arc<Mutex<Vec<u8>>>;

fn cli_pty() -> (PtyPair, Observed, thread::JoinHandle<()>) {
    let pair = native_pty_system()
        .openpty(PtySize {
            rows: 24,
            cols: 80,
            pixel_width: 0,
            pixel_height: 0,
        })
        .unwrap();
    let mut reader = pair.master.try_clone_reader().unwrap();
    let observed = Arc::new(Mutex::new(Vec::new()));
    let bytes = Arc::clone(&observed);
    let task = thread::spawn(move || {
        let mut buffer = [0; 4096];
        loop {
            match reader.read(&mut buffer) {
                Ok(0) | Err(_) => return,
                Ok(count) => bytes.lock().unwrap().extend_from_slice(&buffer[..count]),
            }
        }
    });
    (pair, observed, task)
}

fn paused_raw_spec() -> RunSpec {
    RunSpec {
        program: "/usr/bin/python3".to_owned(),
        args: vec![
            "-u".to_owned(),
            "-c".to_owned(),
            concat!(
                "import os,termios,signal\n",
                "a=termios.tcgetattr(0);a[3]&=~(termios.ICANON|termios.ECHO)\n",
                "termios.tcsetattr(0,termios.TCSANOW,a)\n",
                "os.write(1,b'WAIT')\n",
                "while True: signal.pause()\n",
            )
            .to_owned(),
        ],
        cwd: None,
        env: BTreeMap::new(),
        initial_size: TerminalSize { rows: 24, cols: 80 },
        declared_inputs: Vec::new(),
    }
}

async fn wait_input(client: &Client, id: RunId, commands: usize) {
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let service = client.status(id).await.unwrap().native_service.unwrap();
            if service.input.write_blocked && service.input.unsettled_commands >= commands {
                assert!(service.input.active_confirmed_bytes > 0);
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(
        result.is_ok(),
        "expected {commands} pending inputs; actual status: {:?}",
        client.status(id).await
    );
}

async fn wait_ready(client: &Client, id: RunId) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let (view, snapshot) = client.attach(id, 0).await.unwrap();
            let bytes: Vec<u8> = snapshot
                .replay
                .chunks
                .iter()
                .flat_map(|chunk| chunk.data.iter().copied())
                .collect();
            view.detach().await.unwrap();
            if bytes == b"WAIT" {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("real child has disabled canonical input and echo before the workload");
}

fn wait_cli(child: &mut dyn Child) -> Option<portable_pty::ExitStatus> {
    let until = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return Some(status);
        }
        if Instant::now() >= until {
            return None;
        }
        thread::sleep(Duration::from_millis(10));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controlling_pty_detach_exposes_unconfirmed_input_without_waiting_for_blocked_run() {
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("ctxmux.sock");
    let server = tokio::spawn(ctxmux_daemon::serve(socket.clone()));
    let client = Client::new(&socket);
    tokio::time::timeout(Duration::from_secs(5), async {
        while client.ping().await.is_err() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let runtime = client.runtime_info().await.unwrap();
    let run = client.start(paused_raw_spec()).await.unwrap();
    wait_ready(&client, run.id).await;
    let blocked_client = client.clone();
    let blocked =
        tokio::spawn(async move { blocked_client.input(run.id, vec![b'a'; 256 * 1024]).await });
    wait_input(&client, run.id, 1).await;
    let (pair, observed, reader) = cli_pty();
    let baseline = pair.master.get_termios().unwrap();
    let mut writer = pair.master.take_writer().unwrap();
    let mut command = CommandBuilder::new(env!("CARGO_BIN_EXE_ctxmux"));
    command.arg("--socket");
    command.arg(&socket);
    command.arg("attach");
    command.arg(run.id.to_string());
    let mut cli = pair.slave.spawn_command(command).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while pair.master.get_termios().unwrap() == baseline {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    writer.write_all(b"x").unwrap();
    writer.flush().unwrap();
    wait_input(&client, run.id, 2).await;
    writer.write_all(&[2, b'd']).unwrap();
    writer.flush().unwrap();
    let detached = wait_cli(&mut *cli);
    let alive = client.status(run.id).await.unwrap();
    assert_eq!(alive.pid, run.pid);
    assert_eq!(alive.state, RunState::Running);
    assert_eq!(client.runtime_info().await.unwrap(), runtime);
    let stop = client.prepare_stop(run.id).await.unwrap();
    client.stop(stop).await.unwrap();
    let original = tokio::time::timeout(Duration::from_secs(5), blocked)
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(original, Err(ClientError::ControlRejected { failure })
        if failure.disposition == CommandDisposition::Unknown && failure.confirmed_input_bytes.is_some_and(|prefix| prefix > 0))
    );
    if detached.is_none() {
        let _ = wait_cli(&mut *cli);
    }
    assert!(
        detached
            .expect("CLI must detach before Stop releases the blocked input")
            .success()
    );
    assert_eq!(pair.master.get_termios().unwrap(), baseline);
    drop(writer);
    drop(pair.slave);
    drop(pair.master);
    reader.join().unwrap();
    {
        let output = observed.lock().unwrap();
        assert!(
            output
                .windows(b"unconfirmed input result".len())
                .any(|part| part == b"unconfirmed input result")
        );
    }
    server.abort();
    let _ = server.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn atomic_data_and_detach_never_silently_discard_locally_queued_input() {
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("ctxmux.sock");
    let server = tokio::spawn(ctxmux_daemon::serve(socket.clone()));
    let client = Client::new(&socket);
    tokio::time::timeout(Duration::from_secs(5), async {
        while client.ping().await.is_err() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let runtime = client.runtime_info().await.unwrap();
    let run = client.start(paused_raw_spec()).await.unwrap();
    wait_ready(&client, run.id).await;
    let mut failures = Vec::new();
    for trial in 0..16 {
        let before = client
            .status(run.id)
            .await
            .unwrap()
            .native_service
            .unwrap()
            .input
            .completed_input_bytes
            .unwrap();
        let (pair, observed, reader) = cli_pty();
        let baseline = pair.master.get_termios().unwrap();
        let mut writer = pair.master.take_writer().unwrap();
        let mut command = CommandBuilder::new(env!("CARGO_BIN_EXE_ctxmux"));
        command.arg("--socket");
        command.arg(&socket);
        command.arg("attach");
        command.arg(run.id.to_string());
        let mut cli = pair.slave.spawn_command(command).unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while pair.master.get_termios().unwrap() == baseline {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        writer.write_all(b"x\x02d").unwrap();
        writer.flush().unwrap();
        let detached = wait_cli(&mut *cli);
        assert!(
            detached
                .expect("atomic detach exits within original deadline")
                .success()
        );
        let current = client.status(run.id).await.unwrap();
        let after = current
            .native_service
            .unwrap()
            .input
            .completed_input_bytes
            .unwrap();
        assert_eq!(current.pid, run.pid);
        assert_eq!(client.runtime_info().await.unwrap(), runtime);
        assert_eq!(pair.master.get_termios().unwrap(), baseline);
        drop(writer);
        drop(pair.slave);
        drop(pair.master);
        reader.join().unwrap();
        let text = String::from_utf8_lossy(&observed.lock().unwrap()).into_owned();
        if after != before + 1
            && !text.contains("unconfirmed input result")
            && !text.contains("1 locally buffered input bytes were not sent (not_applied)")
        {
            failures.push(format!(
                "trial {trial}: no confirmed byte or truthful input result: {text:?}"
            ));
        }
        if text.contains("1 locally buffered input bytes were not sent (not_applied)")
            && after != before
        {
            failures.push(format!(
                "trial {trial}: unsubmitted local bytes were confirmed"
            ));
        }
    }
    let stop = client.prepare_stop(run.id).await.unwrap();
    client.stop(stop).await.unwrap();
    server.abort();
    let _ = server.await;
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
