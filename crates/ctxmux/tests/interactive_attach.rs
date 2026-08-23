use std::{
    collections::BTreeMap,
    ffi::OsString,
    io::{Read, Write},
    path::{Path, PathBuf},
    process::Command,
    sync::{Arc, Condvar, Mutex},
    thread,
    time::{Duration, Instant},
};

use ctxmux_client::Client;
use ctxmux_protocol::{RunSpec, RunState, TerminalSize};
use portable_pty::{CommandBuilder, PtySize, native_pty_system};

const DEADLINE: Duration = Duration::from_secs(5);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(
    clippy::too_many_lines,
    reason = "one continuous controlling-PTY lifecycle makes restoration and Run survival auditable"
)]
async fn controlling_pty_attach_restores_terminal_and_leaves_the_run_alive() {
    // PTY-003: exercise the public CLI in a real controlling terminal. The
    // terminal is a disposable view; Ctrl-b d must restore it and leave the
    // daemon-owned child identity running.
    let directory = tempfile::tempdir().expect("create CLI PTY fixture directory");
    let socket = directory.path().join("ctxmux.sock");
    let server = tokio::spawn(ctxmux_daemon::serve(socket.clone()));
    let client = Client::new(&socket);
    tokio::time::timeout(DEADLINE, async {
        loop {
            if client.ping().await.is_ok() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("daemon accepts CLI PTY fixture connections");

    let run = client
        .start(RunSpec {
            program: "/bin/sh".to_owned(),
            args: vec![
                "-c".to_owned(),
                concat!(
                    "trap '' WINCH; stty -echo; ",
                    "printf 'READY\\n'; ",
                    "while IFS= read -r line; do ",
                    "case \"$line\" in ",
                    "raw) printf 'INPUT:%s\\n' \"$line\" ;; ",
                    "size) size=$(stty size); printf 'SIZE:%s\\n' \"$size\" ;; ",
                    "esac; done"
                )
                .to_owned(),
            ],
            cwd: None,
            env: BTreeMap::new(),
            size: TerminalSize { cols: 80, rows: 24 },
            declared_inputs: Vec::new(),
        })
        .await
        .expect("start CLI PTY fixture Run");
    let run_pid = run.pid.expect("native Run exposes direct child PID");

    let pair = native_pty_system()
        .openpty(PtySize {
            rows: 24,
            cols: 80,
            pixel_width: 0,
            pixel_height: 0,
        })
        .expect("open controlling PTY for CLI");
    let baseline_termios = pair
        .master
        .get_termios()
        .expect("native PTY exposes baseline termios");
    let mut reader = pair
        .master
        .try_clone_reader()
        .expect("clone CLI PTY reader");
    let mut writer = pair.master.take_writer().expect("take CLI PTY writer");
    let observed = Arc::new((Mutex::new(Vec::new()), Condvar::new()));
    let reader_observed = Arc::clone(&observed);
    let reader_thread = thread::Builder::new()
        .name("ctxmux-cli-pty-reader".to_owned())
        .spawn(move || {
            let mut buffer = [0; 4096];
            loop {
                match reader.read(&mut buffer) {
                    Ok(0) => return,
                    Ok(read) => {
                        let (bytes, changed) = &*reader_observed;
                        mutex_lock(bytes).extend_from_slice(&buffer[..read]);
                        changed.notify_all();
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                    Err(_) => return,
                }
            }
        })
        .expect("start CLI PTY reader");

    let mut command = CommandBuilder::new(env!("CARGO_BIN_EXE_ctxmux"));
    command.arg("--socket");
    command.arg(&socket);
    command.arg("attach");
    command.arg(run.id.to_string());
    let mut cli = pair
        .slave
        .spawn_command(command)
        .expect("spawn ctxmux attach in controlling PTY");

    wait_for_bytes(&observed, b"READY", DEADLINE);
    let raw_deadline = Instant::now() + DEADLINE;
    let raw_termios = loop {
        let current = pair
            .master
            .get_termios()
            .expect("read CLI PTY termios while attached");
        if current != baseline_termios {
            break current;
        }
        assert!(
            Instant::now() < raw_deadline,
            "CLI did not enter raw mode; output={}",
            String::from_utf8_lossy(&mutex_lock(&observed.0))
        );
        thread::yield_now();
    };
    assert_ne!(raw_termios, baseline_termios);

    writer.write_all(b"raw\n").expect("send raw terminal input");
    writer.flush().expect("flush raw terminal input");
    wait_for_bytes(&observed, b"INPUT:raw", DEADLINE);

    pair.master
        .resize(PtySize {
            rows: 40,
            cols: 120,
            pixel_width: 0,
            pixel_height: 0,
        })
        .expect("resize controlling PTY and deliver SIGWINCH");
    let resize_deadline = Instant::now() + DEADLINE;
    loop {
        if contains_bytes(&observed, b"SIZE:40 120") {
            break;
        }
        assert!(
            Instant::now() < resize_deadline,
            "CLI did not propagate controlling-PTY resize; output={}",
            String::from_utf8_lossy(&mutex_lock(&observed.0))
        );
        assert!(
            cli.try_wait()
                .expect("poll ctxmux attach during resize")
                .is_none(),
            "ctxmux attach exited during resize; output={}",
            String::from_utf8_lossy(&mutex_lock(&observed.0))
        );
        client
            .input(run.id, b"size\n".to_vec())
            .await
            .expect("query resized child PTY through the public Run boundary");
        let (bytes, changed) = &*observed;
        let guard = mutex_lock(bytes);
        let _ = changed
            .wait_timeout(guard, Duration::from_millis(20))
            .unwrap_or_else(std::sync::PoisonError::into_inner);
    }

    writer
        .write_all(&[0x02, b'd'])
        .expect("send Ctrl-b d detach sequence");
    writer.flush().expect("flush detach sequence");
    let cli_status = wait_for_child(&mut *cli, DEADLINE);
    assert!(cli_status.success(), "ctxmux attach failed: {cli_status:?}");
    assert_eq!(
        pair.master
            .get_termios()
            .expect("read restored CLI PTY termios"),
        baseline_termios,
        "ctxmux attach did not restore the controlling terminal"
    );

    let status = client
        .status(run.id)
        .await
        .expect("read Run after CLI detach");
    assert_eq!(status.pid, Some(run_pid));
    assert_eq!(status.state, RunState::Running);
    assert_eq!(status.attachments, 0);

    let stop_operation = client
        .prepare_stop(run.id)
        .await
        .expect("prepare CLI PTY Stop");
    client
        .stop(stop_operation)
        .await
        .expect("stop surviving CLI PTY Run");
    tokio::time::timeout(DEADLINE, async {
        while client
            .status(run.id)
            .await
            .expect("read stopped CLI PTY Run")
            .state
            .is_running()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("surviving CLI PTY Run exits on explicit stop");

    drop(writer);
    drop(pair.slave);
    drop(pair.master);
    reader_thread.join().expect("join CLI PTY reader");
    server.abort();
    let _ = server.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(
    clippy::too_many_lines,
    reason = "one controlling-PTY attach must keep reconstruction, detach, and cleanup auditable"
)]
async fn controlling_pty_attach_paints_current_screen_not_csi_history() {
    let directory = tempfile::tempdir().expect("create CLI screen fixture directory");
    let socket = directory.path().join("ctxmux.sock");
    let server = tokio::spawn(ctxmux_daemon::serve(socket.clone()));
    let client = Client::new(&socket);
    tokio::time::timeout(DEADLINE, async {
        loop {
            if client.ping().await.is_ok() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("daemon accepts CLI screen fixture connections");

    let run = client
        .start(RunSpec {
            program: "/bin/sh".to_owned(),
            args: vec![
                "-c".to_owned(),
                concat!(
                    "trap '' WINCH; stty -echo; ",
                    "printf 'STALE\\n'; ",
                    "printf '\\033[2J\\033[H'; ",
                    "printf 'READY\\n'; ",
                    "while IFS= read -r _; do :; done"
                )
                .to_owned(),
            ],
            cwd: None,
            env: BTreeMap::new(),
            size: TerminalSize { cols: 80, rows: 24 },
            declared_inputs: Vec::new(),
        })
        .await
        .expect("start CLI screen fixture Run");

    tokio::time::timeout(DEADLINE, async {
        loop {
            let (readiness, snapshot) = client
                .attach(run.id, 0)
                .await
                .expect("observe CLI screen fixture replay readiness");
            let replay = snapshot
                .replay
                .chunks
                .iter()
                .flat_map(|chunk| chunk.data.iter().copied())
                .collect::<Vec<_>>();
            readiness
                .detach()
                .await
                .expect("detach CLI screen fixture readiness observer");
            if replay
                .windows(b"READY".len())
                .any(|window| window == b"READY")
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("CLI screen fixture reaches its stable current screen");

    let pair = native_pty_system()
        .openpty(PtySize {
            rows: 24,
            cols: 80,
            pixel_width: 0,
            pixel_height: 0,
        })
        .expect("open controlling PTY for CLI screen fixture");
    let mut reader = pair
        .master
        .try_clone_reader()
        .expect("clone CLI screen PTY reader");
    let mut writer = pair
        .master
        .take_writer()
        .expect("take CLI screen PTY writer");
    let observed = Arc::new((Mutex::new(Vec::new()), Condvar::new()));
    let reader_observed = Arc::clone(&observed);
    let reader_thread = thread::Builder::new()
        .name("ctxmux-cli-screen-reader".to_owned())
        .spawn(move || {
            let mut buffer = [0; 4096];
            loop {
                match reader.read(&mut buffer) {
                    Ok(0) => return,
                    Ok(read) => {
                        let (bytes, changed) = &*reader_observed;
                        mutex_lock(bytes).extend_from_slice(&buffer[..read]);
                        changed.notify_all();
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                    Err(_) => return,
                }
            }
        })
        .expect("start CLI screen PTY reader");

    let mut command = CommandBuilder::new(env!("CARGO_BIN_EXE_ctxmux"));
    command.arg("--socket");
    command.arg(&socket);
    command.arg("attach");
    command.arg(run.id.to_string());
    let mut cli = pair
        .slave
        .spawn_command(command)
        .expect("spawn ctxmux attach in controlling PTY");

    wait_for_bytes(&observed, b"READY", DEADLINE);
    assert!(
        !contains_bytes(&observed, b"STALE"),
        "interactive attach replayed erased CSI history; output={}",
        String::from_utf8_lossy(&mutex_lock(&observed.0))
    );

    writer
        .write_all(&[0x02, b'd'])
        .expect("send Ctrl-b d detach sequence");
    writer.flush().expect("flush detach sequence");
    let cli_status = wait_for_child(&mut *cli, DEADLINE);
    assert!(cli_status.success(), "ctxmux attach failed: {cli_status:?}");

    let stop_operation = client
        .prepare_stop(run.id)
        .await
        .expect("prepare CLI screen Stop");
    client
        .stop(stop_operation)
        .await
        .expect("stop CLI screen fixture Run");
    drop(writer);
    drop(pair.slave);
    drop(pair.master);
    reader_thread.join().expect("join CLI screen PTY reader");
    server.abort();
    let _ = server.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(
    clippy::too_many_lines,
    reason = "one continuous read-only controlling-PTY lifecycle keeps input suppression, detach, restoration, and pane survival auditable"
)]
async fn controlling_pty_detaches_from_read_only_tmux_run_without_forwarding_input() {
    let directory = tempfile::tempdir().expect("create tmux CLI PTY fixture directory");
    let Some(tmux) = TmuxFixture::start(directory.path()) else {
        return;
    };
    let socket = directory.path().join("ctxmux.sock");
    let server = tokio::spawn(ctxmux_daemon::serve(socket.clone()));
    let client = Client::new(&socket);
    tokio::time::timeout(DEADLINE, async {
        loop {
            if client.ping().await.is_ok() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("daemon accepts tmux CLI PTY fixture connections");

    let panes = client
        .discover_tmux(tmux.socket_string())
        .await
        .expect("discover real tmux pane through public client")
        .1;
    let pane = panes
        .into_iter()
        .find(|pane| pane.session_id == tmux.session_id)
        .expect("discover selected tmux fixture session");
    let run = client
        .import_tmux(tmux.socket_string(), &pane.pane_id)
        .await
        .expect("import real tmux pane through public client");

    let pair = native_pty_system()
        .openpty(PtySize {
            rows: 24,
            cols: 80,
            pixel_width: 0,
            pixel_height: 0,
        })
        .expect("open controlling PTY for tmux CLI");
    let baseline_termios = pair
        .master
        .get_termios()
        .expect("native PTY exposes baseline tmux CLI termios");
    let mut reader = pair
        .master
        .try_clone_reader()
        .expect("clone tmux CLI PTY reader");
    let mut writer = pair.master.take_writer().expect("take tmux CLI PTY writer");
    let observed = Arc::new((Mutex::new(Vec::new()), Condvar::new()));
    let reader_observed = Arc::clone(&observed);
    let reader_thread = thread::Builder::new()
        .name("ctxmux-tmux-cli-pty-reader".to_owned())
        .spawn(move || {
            let mut buffer = [0; 4096];
            loop {
                match reader.read(&mut buffer) {
                    Ok(0) => return,
                    Ok(read) => {
                        let (bytes, changed) = &*reader_observed;
                        mutex_lock(bytes).extend_from_slice(&buffer[..read]);
                        changed.notify_all();
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                    Err(_) => return,
                }
            }
        })
        .expect("start tmux CLI PTY reader");

    let mut command = CommandBuilder::new(env!("CARGO_BIN_EXE_ctxmux"));
    command.arg("--socket");
    command.arg(&socket);
    command.arg("attach");
    command.arg(run.id.to_string());
    let mut cli = pair
        .slave
        .spawn_command(command)
        .expect("spawn ctxmux tmux attach in controlling PTY");

    tokio::time::timeout(DEADLINE, async {
        loop {
            let status = client
                .status(run.id)
                .await
                .expect("read imported Run attachment count");
            if status.attachments == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("CLI attaches to imported tmux Run");
    let raw_deadline = Instant::now() + DEADLINE;
    loop {
        if pair
            .master
            .get_termios()
            .expect("read raw tmux CLI PTY termios")
            != baseline_termios
        {
            break;
        }
        assert!(
            cli.try_wait()
                .expect("poll ctxmux tmux attach before raw mode")
                .is_none(),
            "ctxmux tmux attach exited before raw mode"
        );
        assert!(
            Instant::now() < raw_deadline,
            "tmux attach did not enter raw mode"
        );
        thread::yield_now();
    }

    writer
        .write_all(b"must-not-reach-tmux\n")
        .expect("send ignored read-only input");
    writer.flush().expect("flush ignored read-only input");
    tmux.checked(&[
        "send-keys",
        "-t",
        &pane.pane_id,
        "public-tmux-output",
        "Enter",
    ]);
    wait_for_bytes(&observed, b"TMUX:1:public-tmux-output", DEADLINE);
    assert!(
        !contains_bytes(&observed, b"must-not-reach-tmux"),
        "read-only CLI input reached the tmux pane"
    );

    writer
        .write_all(&[0x02, b'd'])
        .expect("send tmux Run Ctrl-b d detach sequence");
    writer.flush().expect("flush tmux Run detach sequence");
    let cli_status = wait_for_child(&mut *cli, DEADLINE);
    assert!(
        cli_status.success(),
        "ctxmux tmux attach failed: {cli_status:?}"
    );
    assert_eq!(
        pair.master
            .get_termios()
            .expect("read restored tmux CLI PTY termios"),
        baseline_termios,
        "tmux Run attach did not restore the controlling terminal"
    );

    let status = client
        .status(run.id)
        .await
        .expect("read imported Run after CLI detach");
    assert_eq!(status.state, RunState::Running);
    assert_eq!(status.attachments, 0);
    assert_eq!(tmux.pane_pid(&pane.pane_id), pane.pane_pid);

    drop(writer);
    drop(pair.slave);
    drop(pair.master);
    reader_thread.join().expect("join tmux CLI PTY reader");
    server.abort();
    let _ = server.await;
}

struct TmuxFixture {
    executable: OsString,
    socket: PathBuf,
    session_id: String,
}

impl TmuxFixture {
    fn start(directory: &Path) -> Option<Self> {
        let executable =
            std::env::var_os("CTXMUX_TMUX_BIN").unwrap_or_else(|| OsString::from("tmux"));
        match Command::new(&executable).arg("-V").output() {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                assert_ne!(
                    std::env::var_os("CTXMUX_REQUIRE_TMUX").as_deref(),
                    Some(std::ffi::OsStr::new("1")),
                    "required tmux executable is unavailable"
                );
                eprintln!("skipping real tmux CLI test: tmux executable is unavailable");
                return None;
            }
            Err(error) => panic!("probe tmux executable: {error}"),
            Ok(output) => assert!(
                output.status.success(),
                "tmux version probe failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ),
        }

        let socket = directory.join("tmux-cli.sock");
        let mut fixture = Self {
            executable,
            socket,
            session_id: String::new(),
        };
        fixture.checked(&[
            "new-session",
            "-d",
            "-s",
            "ctxmux-cli-target",
            "/bin/sh",
            "-c",
            concat!(
                "stty -echo; printf 'BEFORE-IMPORT\\n'; i=0; ",
                "while IFS= read -r line; do i=$((i + 1)); ",
                "printf 'TMUX:%s:%s\\n' \"$i\" \"$line\"; done"
            ),
        ]);
        fixture
            .checked(&[
                "display-message",
                "-p",
                "-t",
                "ctxmux-cli-target",
                "#{session_id}",
            ])
            .trim()
            .clone_into(&mut fixture.session_id);
        Some(fixture)
    }

    fn socket_string(&self) -> String {
        self.socket.to_string_lossy().into_owned()
    }

    fn pane_pid(&self, pane_id: &str) -> u32 {
        self.checked(&["display-message", "-p", "-t", pane_id, "#{pane_pid}"])
            .trim()
            .parse()
            .expect("parse tmux pane PID")
    }

    fn checked(&self, args: &[&str]) -> String {
        let output = Command::new(&self.executable)
            .env_remove("TMUX")
            .env_remove("TMUX_PANE")
            .arg("-S")
            .arg(&self.socket)
            .args(args)
            .output()
            .expect("run tmux CLI fixture command");
        assert!(
            output.status.success(),
            "tmux fixture command {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).expect("tmux fixture output is UTF-8")
    }
}

impl Drop for TmuxFixture {
    fn drop(&mut self) {
        let _ = Command::new(&self.executable)
            .env_remove("TMUX")
            .env_remove("TMUX_PANE")
            .arg("-S")
            .arg(&self.socket)
            .arg("kill-server")
            .status();
    }
}

fn wait_for_bytes(observed: &Arc<(Mutex<Vec<u8>>, Condvar)>, expected: &[u8], timeout: Duration) {
    let deadline = Instant::now() + timeout;
    let (bytes, changed) = &**observed;
    let mut guard = mutex_lock(bytes);
    loop {
        if guard
            .windows(expected.len())
            .any(|window| window == expected)
        {
            return;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "PTY output did not contain {:?}; output={}",
            String::from_utf8_lossy(expected),
            String::from_utf8_lossy(&guard)
        );
        let (next_guard, result) = changed
            .wait_timeout(guard, remaining)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard = next_guard;
        assert!(
            !result.timed_out(),
            "PTY output did not contain {:?}; output={}",
            String::from_utf8_lossy(expected),
            String::from_utf8_lossy(&guard)
        );
    }
}

fn contains_bytes(observed: &Arc<(Mutex<Vec<u8>>, Condvar)>, expected: &[u8]) -> bool {
    mutex_lock(&observed.0)
        .windows(expected.len())
        .any(|window| window == expected)
}

fn wait_for_child(
    child: &mut dyn portable_pty::Child,
    timeout: Duration,
) -> portable_pty::ExitStatus {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait().expect("poll ctxmux attach") {
            return status;
        }
        assert!(Instant::now() < deadline, "ctxmux attach did not exit");
        thread::yield_now();
    }
}

fn mutex_lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
