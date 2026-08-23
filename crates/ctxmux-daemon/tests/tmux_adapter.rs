#![cfg(unix)]

use std::{
    collections::BTreeMap,
    ffi::OsString,
    io,
    os::unix::{
        fs::PermissionsExt,
        net::{UnixListener, UnixStream},
    },
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Output, Stdio},
    sync::{Mutex, MutexGuard},
    time::{Duration, Instant},
};

use ctxmux_client::{Attachment, Client, ClientError, replay_bytes};
use ctxmux_protocol::{
    AttachedSnapshot, ErrorCode, ForkPlan, InterruptionReason, RecoverableStop, ReplayCapability,
    RunBackend, RunCapabilities, RunEvent, RunId, RunSpec, RunState, TerminalSize, TmuxRunEvent,
};
use tempfile::TempDir;
use tokio::time::{sleep, timeout};

const TARGET_SESSION: &str = "ctxmux-target";
const PUBLIC_ATTACHMENT_TIMEOUT: Duration = Duration::from_secs(5);
static TMUX_FIXTURE_OWNER: Mutex<()> = Mutex::new(());
const FIXTURE_SHELL: &str = concat!(
    "stty -echo; printf 'BEFORE-IMPORT\\n'; : > \"$1\"; ",
    "while IFS= read -r line; do case \"$line\" in ",
    "quit) exit 0 ;; ",
    "bytes) printf 'BYTES:\\045\\012\\033\\177\\377' ;; ",
    "burst) printf 'BURST-BEGIN\\n'; i=0; while [ \"$i\" -lt 4096 ]; do ",
    "printf 'BURST:%04d:abcdefgh\\n' \"$i\"; i=$((i + 1)); done; ",
    "printf 'BURST-END\\n' ;; ",
    "*) printf 'OUT:%s\\n' \"$line\" ;; ",
    "esac; done"
);

async fn fresh_stop(client: &Client, id: RunId) -> RecoverableStop {
    client
        .prepare_stop(id)
        .await
        .expect("prepare recoverable Stop operation")
}

struct TestDaemon {
    child: Child,
    _directory: TempDir,
    client: Client,
}

impl TestDaemon {
    async fn start() -> Self {
        Self::start_with_options(None, None, None).await
    }

    async fn start_persistent() -> Self {
        let directory = tempfile::tempdir().expect("create persistent daemon directory");
        let state_dir = directory.path().join("state");
        Self::spawn(directory, Some(&state_dir), None, None).await
    }

    async fn start_with_tmux_bin(executable: &Path) -> Self {
        Self::start_with_options(None, Some(executable), None).await
    }

    async fn start_with_single_worker_and_tmux_bin(executable: &Path) -> Self {
        Self::start_with_options(None, Some(executable), Some("1")).await
    }

    async fn start_with_options(
        state_dir: Option<&Path>,
        tmux_bin: Option<&Path>,
        worker_threads: Option<&str>,
    ) -> Self {
        let directory = tempfile::tempdir().expect("create daemon temp directory");
        Self::spawn(directory, state_dir, tmux_bin, worker_threads).await
    }

    async fn spawn(
        directory: TempDir,
        state_dir: Option<&Path>,
        tmux_bin: Option<&Path>,
        worker_threads: Option<&str>,
    ) -> Self {
        let socket = directory.path().join("ctxmux.sock");
        let mut command = Command::new(env!("CARGO_BIN_EXE_ctxmuxd"));
        command.arg("--socket").arg(&socket);
        if let Some(state_dir) = state_dir {
            command.arg("--state-dir").arg(state_dir);
        }
        if let Some(tmux_bin) = tmux_bin {
            command.env("CTXMUX_TMUX_BIN", tmux_bin);
        }
        if let Some(worker_threads) = worker_threads {
            command.env("TOKIO_WORKER_THREADS", worker_threads);
        }
        let child = command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn ctxmuxd");
        let client = Client::new(socket);
        let mut daemon = Self {
            child,
            _directory: directory,
            client,
        };

        timeout(Duration::from_secs(5), async {
            loop {
                if let Some(status) = daemon.child.try_wait().expect("poll ctxmuxd") {
                    panic!("ctxmuxd exited before accepting connections: {status}");
                }
                if daemon.client.ping().await.is_ok() {
                    break;
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("ctxmuxd should accept connections");
        daemon
    }

    fn client(&self) -> Client {
        self.client.clone()
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn file_descriptor_count(&self) -> usize {
        process_file_descriptor_count(self.child.id())
    }

    fn shutdown_clean(&mut self) {
        let status = self.shutdown_status(Duration::from_secs(2));
        assert!(status.success(), "ctxmuxd clean shutdown failed: {status}");
    }

    fn shutdown_status(&mut self, deadline: Duration) -> ExitStatus {
        match self.child.try_wait() {
            Ok(None) => {}
            Ok(Some(status)) => panic!("ctxmuxd exited before clean shutdown: {status}"),
            Err(error) => panic!("poll ctxmuxd before clean shutdown: {error}"),
        }
        let status = Command::new("kill")
            .arg("-INT")
            .arg(self.child.id().to_string())
            .status()
            .expect("send SIGINT to ctxmuxd");
        assert!(status.success(), "send SIGINT to ctxmuxd: {status}");

        let deadline = Instant::now() + deadline;
        loop {
            match self.child.try_wait() {
                Ok(Some(status)) => return status,
                Ok(None) => std::thread::sleep(Duration::from_millis(20)),
                Err(error) => panic!("wait for ctxmuxd clean shutdown: {error}"),
            }
            if Instant::now() >= deadline {
                break;
            }
        }

        let _ = self.child.kill();
        let _ = self.child.wait();
        panic!("ctxmuxd did not exit within the clean shutdown deadline");
    }

    fn stop_best_effort(&mut self) {
        if self.child.try_wait().ok().flatten().is_some() {
            return;
        }
        let interrupted = Command::new("kill")
            .arg("-INT")
            .arg(self.child.id().to_string())
            .status()
            .is_ok_and(|status| status.success());
        if interrupted {
            for _ in 0..100 {
                match self.child.try_wait() {
                    Ok(Some(_)) => return,
                    Ok(None) => std::thread::sleep(Duration::from_millis(20)),
                    Err(_) => break,
                }
            }
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for TestDaemon {
    fn drop(&mut self) {
        self.stop_best_effort();
    }
}

struct TmuxServer {
    _fixture_owner: TmuxFixtureReservation,
    executable: OsString,
    _directory: TempDir,
    socket: PathBuf,
    server_pid: Option<u32>,
    alive: bool,
}

impl TmuxServer {
    fn start() -> Option<Self> {
        let fixture_owner = lock_tmux_fixture_owner();
        let executable =
            std::env::var_os("CTXMUX_TMUX_BIN").unwrap_or_else(|| OsString::from("tmux"));
        match Command::new(&executable).arg("-V").output() {
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                assert_ne!(
                    std::env::var_os("CTXMUX_REQUIRE_TMUX").as_deref(),
                    Some(std::ffi::OsStr::new("1")),
                    "required real tmux executable is unavailable: {}",
                    Path::new(&executable).display()
                );
                eprintln!("skipping real tmux test: tmux executable is unavailable");
                return None;
            }
            Err(error) => panic!("probe tmux executable: {error}"),
            Ok(output) if !output.status.success() => {
                panic!("tmux version probe failed: {}", stderr(&output))
            }
            Ok(_) => {}
        }

        let directory = tempfile::tempdir().expect("create tmux fixture directory");
        let socket = directory.path().join("tmux.sock");
        let ready = directory.path().join("pane-ready");
        let mut server = Self {
            _fixture_owner: fixture_owner,
            executable,
            _directory: directory,
            socket,
            server_pid: None,
            alive: true,
        };
        let output = server
            .command()
            .args(["new-session", "-d", "-s", TARGET_SESSION])
            .arg("/bin/sh")
            .arg("-c")
            .arg(FIXTURE_SHELL)
            .arg("ctxmux-tmux-fixture")
            .arg(&ready)
            .output()
            .expect("start isolated tmux server");
        assert!(
            output.status.success(),
            "start isolated tmux server: {}",
            stderr(&output)
        );
        let server_pid = server.checked(&["display-message", "-p", "#{pid}"]);
        server.server_pid = Some(
            String::from_utf8(server_pid.stdout)
                .expect("tmux server pid is UTF-8")
                .trim()
                .parse()
                .expect("parse tmux server pid"),
        );
        for _ in 0..100 {
            if ready.is_file() {
                return Some(server);
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        panic!("tmux fixture pane did not become ready");
    }

    fn base_command(executable: &OsString, socket: &Path) -> Command {
        let mut command = Command::new(executable);
        command
            .env_remove("TMUX")
            .env_remove("TMUX_PANE")
            .arg("-S")
            .arg(socket);
        command
    }

    fn command(&self) -> Command {
        Self::base_command(&self.executable, &self.socket)
    }

    fn checked(&self, args: &[&str]) -> Output {
        let output = self
            .command()
            .args(args)
            .output()
            .expect("run tmux fixture command");
        assert!(
            output.status.success(),
            "tmux fixture command {args:?} failed: {}",
            stderr(&output)
        );
        output
    }

    fn socket_string(&self) -> String {
        self.socket.to_string_lossy().into_owned()
    }

    fn send_line(&self, pane_id: &str, line: &str) {
        self.checked(&["send-keys", "-t", pane_id, "-l", line]);
        self.checked(&["send-keys", "-t", pane_id, "Enter"]);
    }

    fn rename_target(&self, name: &str) {
        self.checked(&["rename-session", "-t", TARGET_SESSION, name]);
    }

    fn rename_session(&self, target: &str, name: &str) {
        self.checked(&["rename-session", "-t", target, name]);
    }

    fn add_keeper_session(&self) {
        self.checked(&[
            "new-session",
            "-d",
            "-s",
            "ctxmux-keeper",
            "/bin/sh",
            "-c",
            "while :; do sleep 60; done",
        ]);
    }

    fn is_reachable(&self) -> bool {
        self.command()
            .args(["has-session", "-t", TARGET_SESSION])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|status| status.success())
    }

    fn server_probe(&self) -> Output {
        self.command()
            .arg("list-sessions")
            .output()
            .expect("probe tmux fixture server")
    }

    fn only_client_pid(&self) -> u32 {
        for _ in 0..100 {
            let output = self.checked(&["list-clients", "-F", "#{client_pid}"]);
            let pids = String::from_utf8(output.stdout)
                .expect("tmux client PIDs are UTF-8")
                .lines()
                .filter(|line| !line.is_empty())
                .map(|line| line.parse().expect("parse tmux client PID"))
                .collect::<Vec<u32>>();
            match pids.as_slice() {
                [pid] => return *pid,
                [] => std::thread::sleep(Duration::from_millis(20)),
                _ => panic!("isolated tmux fixture has unexpected clients: {pids:?}"),
            }
        }
        panic!("ctxmux Control Mode client was not visible through tmux list-clients");
    }

    fn assert_no_clients(&self) {
        for _ in 0..100 {
            let output = self.checked(&["list-clients", "-F", "#{client_pid}"]);
            if output.stdout.is_empty() {
                return;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        let output = self.checked(&["list-clients", "-F", "#{client_pid}"]);
        panic!(
            "ctxmux Control Mode client remained attached after daemon shutdown: {:?}",
            String::from_utf8_lossy(&output.stdout)
        );
    }

    fn assert_stopped(&self) {
        let server_pid = self
            .server_pid
            .expect("tmux fixture recorded its server pid");
        for _ in 0..250 {
            if !process_exists(server_pid) {
                let probe = self.server_probe();
                let socket_connectable = UnixStream::connect(&self.socket).is_ok();
                assert!(
                    !probe.status.success() && !socket_connectable,
                    "tmux server PID exited but its public surfaces remained reachable: path={}, server_pid={}, socket_connectable={}, probe_status={}, probe_stdout={:?}, probe_stderr={:?}",
                    self.socket.display(),
                    server_pid,
                    socket_connectable,
                    probe.status,
                    String::from_utf8_lossy(&probe.stdout),
                    String::from_utf8_lossy(&probe.stderr),
                );
                return;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        let probe = self.server_probe();
        let socket_connectable = UnixStream::connect(&self.socket).is_ok();
        panic!(
            "tmux server did not exit after clean shutdown: path={}, server_pid={}, pid_live={}, socket_connectable={}, probe_status={}, probe_stdout={:?}, probe_stderr={:?}, process_tree={:?}",
            self.socket.display(),
            server_pid,
            process_exists(server_pid),
            socket_connectable,
            probe.status,
            String::from_utf8_lossy(&probe.stdout),
            String::from_utf8_lossy(&probe.stderr),
            self.process_tree_evidence(),
        );
    }

    fn process_tree_evidence(&self) -> String {
        let output = Command::new("ps")
            .args(["-axo", "pid=,ppid=,stat=,command="])
            .output()
            .expect("capture tmux process-tree evidence");
        let socket = self.socket.to_string_lossy();
        String::from_utf8_lossy(&output.stdout)
            .lines()
            .filter(|line| line.contains("tmux") || line.contains(&*socket))
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn kill_server_clean(&mut self) {
        if !self.alive {
            self.assert_stopped();
            return;
        }
        let output = self
            .command()
            .arg("kill-server")
            .output()
            .expect("cleanly stop tmux fixture server");
        assert!(
            output.status.success(),
            "cleanly stop tmux fixture server: {}",
            stderr(&output)
        );
        self.assert_stopped();
        self.alive = false;
    }

    fn shutdown_clean(mut self) {
        self.kill_server_clean();
    }

    fn kill_server_best_effort(&mut self) {
        if self.alive {
            let _ = self.command().arg("kill-server").status();
            self.alive = false;
        }
    }
}

impl Drop for TmuxServer {
    fn drop(&mut self) {
        self.kill_server_best_effort();
    }
}

struct FakeTmuxControl {
    _fixture_owner: TmuxFixtureReservation,
    _directory: TempDir,
    _socket_listener: UnixListener,
    pane_process: Child,
    socket: PathBuf,
    executable: PathBuf,
    pause_trigger: PathBuf,
    output_trigger: PathBuf,
    protocol_corruption_trigger: PathBuf,
    control_startup_mode: PathBuf,
    control_trigger: PathBuf,
    probe_signature: PathBuf,
    server_version: PathBuf,
    include_dead_pane: PathBuf,
    include_linked_duplicate: PathBuf,
    refresh_log: PathBuf,
    control_pids_file: PathBuf,
    descendant_pids_file: PathBuf,
    hold_stdout_open: PathBuf,
    short_command_mode: PathBuf,
    short_command_pids_file: PathBuf,
}

impl FakeTmuxControl {
    fn create() -> Self {
        let fixture_owner = lock_tmux_fixture_owner();
        let directory = tempfile::tempdir().expect("create fake tmux fixture directory");
        let socket = directory.path().join("tmux.sock");
        let socket_listener = UnixListener::bind(&socket).expect("bind fake tmux socket identity");
        let pane_process = Command::new("sleep")
            .arg("60")
            .spawn()
            .expect("spawn fake tmux-owned pane process");
        let pane_pid_file = directory.path().join("pane.pid");
        std::fs::write(&pane_pid_file, pane_process.id().to_string())
            .expect("record fake tmux-owned pane process PID");
        let executable = directory.path().join("tmux");
        let pause_trigger = directory.path().join("pause-trigger");
        let output_trigger = directory.path().join("output-trigger");
        let protocol_corruption_trigger = directory.path().join("protocol-corruption-trigger");
        let control_startup_mode = directory.path().join("control-startup-mode");
        let control_trigger = directory.path().join("control-trigger");
        let probe_signature = directory.path().join("probe-signature");
        let server_version = directory.path().join("server-version");
        let include_dead_pane = directory.path().join("include-dead-pane");
        let include_linked_duplicate = directory.path().join("include-linked-duplicate");
        let refresh_log = directory.path().join("refresh.log");
        let control_pids_file = directory.path().join("control.pids");
        let descendant_pids_file = directory.path().join("descendant.pids");
        let hold_stdout_open = directory.path().join("hold-stdout-open");
        let short_command_mode = directory.path().join("short-command-mode");
        let short_command_pids_file = directory.path().join("short-command.pids");
        std::fs::write(
            &executable,
            r#"#!/bin/sh
fixture_dir=${0%/*}
pause_trigger=$fixture_dir/pause-trigger
output_trigger=$fixture_dir/output-trigger
protocol_corruption_trigger=$fixture_dir/protocol-corruption-trigger
control_startup_mode_file=$fixture_dir/control-startup-mode
control_trigger_file=$fixture_dir/control-trigger
probe_signature=$fixture_dir/probe-signature
server_version=$fixture_dir/server-version
include_dead_pane=$fixture_dir/include-dead-pane
include_linked_duplicate=$fixture_dir/include-linked-duplicate
refresh_log=$fixture_dir/refresh.log
pane_pid=$(cat "$fixture_dir/pane.pid")
control_pids_file=$fixture_dir/control.pids
descendant_pids_file=$fixture_dir/descendant.pids
hold_stdout_open=$fixture_dir/hold-stdout-open
short_command_mode=$fixture_dir/short-command-mode
short_command_pids_file=$fixture_dir/short-command.pids

mode=
if [ -f "$short_command_mode" ]; then
    mode=$(cat "$short_command_mode")
fi

if [ "$#" -eq 1 ] && [ "$1" = "-V" ]; then
    if [ "$mode" = "hang-version" ]; then
        printf '%s\n' "$$" >> "$short_command_pids_file"
        exec sleep 30
    fi
    printf 'tmux 3.6\n'
    exit 0
fi

if [ "$#" -lt 3 ] || [ "$1" != "-S" ]; then
    exit 20
fi
shift 2

if [ "$1" = "list-panes" ]; then
    if [ "$#" -ne 4 ] || [ "$2" != "-a" ] || [ "$3" != "-F" ]; then
        exit 21
    fi
    if [ "$mode" = "hang-discovery" ]; then
        printf '%s\n' "$$" >> "$short_command_pids_file"
        exec sleep 30
    fi
    if [ "$mode" = "overflow-discovery" ]; then
        printf '%s\n' "$$" >> "$short_command_pids_file"
        dd if=/dev/zero bs=131073 count=1 2>/dev/null | tr '\000' x
        exit 0
    fi
    if [ -f "$server_version" ]; then
        version=$(cat "$server_version")
    else
        version=3.6
    fi
    printf '%s\t4100\t1700000000\t$0\t@0\t%%0\t%s\t80\t24\t0\n' "$version" "$pane_pid"
    if [ -f "$include_dead_pane" ]; then
        printf '%s\t4100\t1700000000\t$0\t@1\t%%1\t4199\t80\t24\t1\n' "$version"
    fi
    if [ -f "$include_linked_duplicate" ]; then
        printf '%s\t4100\t1700000000\t$1\t@2\t%%0\t%s\t80\t24\t0\n' "$version" "$pane_pid"
    fi
    exit 0
fi

if [ "$#" -ne 6 ] || [ "$1" != "-C" ] || [ "$2" != "attach-session" ] || \
   [ "$3" != "-t" ] || [ "$4" != '$0' ] || [ "$5" != "-f" ] || \
   [ "$6" != "read-only,ignore-size,no-detach-on-destroy,pause-after=1" ]; then
    exit 22
fi

printf '%s\n' "$$" >> "$control_pids_file"
if [ "$mode" = "hang-readiness" ]; then
    exec sleep 30
fi
if [ -f "$hold_stdout_open" ]; then
    sleep 30 &
    printf '%s\n' "$!" >> "$descendant_pids_file"
fi
control_startup_mode=default
if [ -f "$control_startup_mode_file" ]; then
    control_startup_mode=$(cat "$control_startup_mode_file")
fi
bootstrap_number=0
sequence=1
case "$control_startup_mode" in
    default)
        ;;
    blank-lines)
        printf '\n'
        ;;
    nonzero-gap)
        bootstrap_number=41
        sequence=47
        ;;
    bootstrap-error|bootstrap-nonempty|double-bootstrap|open-block-eof)
        ;;
    *)
        exit 26
        ;;
esac
if [ "$control_startup_mode" = "open-block-eof" ]; then
    printf '%%begin 1 %s 0\n' "$bootstrap_number"
    exit 0
fi
printf '%%begin 1 %s 0\n' "$bootstrap_number"
if [ "$control_startup_mode" = "bootstrap-nonempty" ]; then
    printf 'unexpected bootstrap output\n'
fi
if [ "$control_startup_mode" = "bootstrap-error" ]; then
    printf '%%error 1 %s 0\n' "$bootstrap_number"
else
    printf '%%end 1 %s 0\n' "$bootstrap_number"
fi
if [ "$control_startup_mode" = "double-bootstrap" ]; then
    printf '%%begin 1 1 0\n'
    printf '%%end 1 1 0\n'
    sequence=2
fi
printf '%%session-changed $0 ctxmux-fake\n'
paused=0
blank_after_ready=0
storm_wait_for_probe=0
if [ "$control_startup_mode" = "blank-lines" ]; then
    blank_after_ready=1
fi
while IFS= read -r line; do
    control_trigger=
    storm_started=0
    if [ -f "$control_trigger_file" ]; then
        control_trigger=$(cat "$control_trigger_file")
        rm -f "$control_trigger_file"
        case "$control_trigger" in
            eof)
                exit 0
                ;;
            open-block-eof)
                printf '%%begin 1 %s 0\n' "$sequence"
                exit 0
                ;;
            pause-storm)
                storm_index=0
                while [ "$storm_index" -lt 64 ]; do
                    printf '%%pause %%0\n'
                    storm_index=$((storm_index + 1))
                done
                storm_wait_for_probe=1
                storm_started=1
                ;;
            duplicate|backwards|no-pending)
                ;;
            *)
                exit 27
                ;;
        esac
    fi
    if [ -f "$output_trigger" ]; then
        output=$(cat "$output_trigger")
        rm -f "$output_trigger"
        case "$output" in
            before-pause)
                printf '%s\n' '%output %0 BEFORE-PAUSE\015\012'
                ;;
            *)
                exit 25
                ;;
        esac
    fi
    if [ "$paused" -eq 0 ] && [ -f "$pause_trigger" ]; then
        printf '%%pause %%0\n'
        paused=1
    fi
    if [ -f "$protocol_corruption_trigger" ]; then
        corruption=$(cat "$protocol_corruption_trigger")
        rm -f "$protocol_corruption_trigger"
        case "$corruption" in
            malformed)
                printf 'not a control notification\n'
                ;;
            oversized)
                dd if=/dev/zero bs=1048577 count=1 2>/dev/null | tr '\000' x
                printf '\n'
                ;;
            *)
                exit 24
                ;;
        esac
        continue
    fi
    case "$line" in
        display-message*)
            storm_finishing=0
            if [ "$storm_wait_for_probe" -eq 1 ] && [ "$storm_started" -eq 0 ]; then
                storm_finishing=1
            fi
            result_number=$sequence
            if [ "$control_trigger" = "duplicate" ]; then
                result_number=$((sequence - 1))
            elif [ "$control_trigger" = "backwards" ]; then
                result_number=0
            fi
            printf '%%begin 1 %s 0\n' "$result_number"
            if [ -f "$probe_signature" ]; then
                cat "$probe_signature"
                printf '\n'
            else
                printf '4100\t1700000000\t$0\t@0\t%%0\t%s\n' "$pane_pid"
            fi
            printf '%%end 1 %s 0\n' "$result_number"
            sequence=$((sequence + 1))
            if [ "$control_trigger" = "no-pending" ]; then
                printf '%%begin 1 %s 0\n' "$sequence"
                printf '%%end 1 %s 0\n' "$sequence"
                sequence=$((sequence + 1))
            fi
            if [ "$blank_after_ready" -eq 1 ]; then
                printf '\r\n'
                printf '%s\n' '%output %0 BLANK-EXACT\015\012'
                blank_after_ready=0
            fi
            if [ "$storm_finishing" -eq 1 ]; then
                printf '%s\n' '%output %0 STORM-DRAINED\015\012'
                storm_wait_for_probe=0
            fi
            ;;
        'refresh-client -A %0:continue')
            printf '%s\n' "$line" >> "$refresh_log"
            printf '%%begin 1 %s 0\n' "$sequence"
            printf '%%end 1 %s 0\n' "$sequence"
            sequence=$((sequence + 1))
            printf '%%continue %%0\n'
            printf '%s\n' '%output %0 AFTER-CONTINUE\015\012'
            ;;
        *)
            exit 23
            ;;
    esac
done
exit 0
"#,
        )
        .expect("write fake tmux executable");
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700))
            .expect("make fake tmux executable executable");
        Self {
            _fixture_owner: fixture_owner,
            _directory: directory,
            _socket_listener: socket_listener,
            pane_process,
            socket,
            executable,
            pause_trigger,
            output_trigger,
            protocol_corruption_trigger,
            control_startup_mode,
            control_trigger,
            probe_signature,
            server_version,
            include_dead_pane,
            include_linked_duplicate,
            refresh_log,
            control_pids_file,
            descendant_pids_file,
            hold_stdout_open,
            short_command_mode,
            short_command_pids_file,
        }
    }

    fn trigger_pause(&self) {
        std::fs::write(&self.pause_trigger, b"pause\n").expect("trigger fake tmux pause");
    }

    fn trigger_output_before_pause(&self) {
        std::fs::write(&self.output_trigger, b"before-pause\n")
            .expect("trigger deterministic pre-pause output");
    }

    fn socket_string(&self) -> String {
        self.socket.to_string_lossy().into_owned()
    }

    fn pane_pid(&self) -> u32 {
        self.pane_process.id()
    }

    fn trigger_protocol_corruption(&self, corruption: &str) {
        std::fs::write(&self.protocol_corruption_trigger, corruption)
            .expect("trigger fake tmux protocol corruption");
    }

    fn set_control_startup_mode(&self, mode: &str) {
        std::fs::write(&self.control_startup_mode, mode).expect("set fake Control startup mode");
    }

    fn trigger_control(&self, trigger: &str) {
        std::fs::write(&self.control_trigger, trigger).expect("trigger fake Control behavior");
    }

    fn set_probe_signature(&self, signature: &str) {
        std::fs::write(&self.probe_signature, signature)
            .expect("replace fake tmux target identity");
    }

    fn set_server_version(&self, version: &str) {
        std::fs::write(&self.server_version, version).expect("set fake tmux server version");
    }

    fn include_dead_pane(&self) {
        std::fs::write(&self.include_dead_pane, b"dead\n")
            .expect("include dead pane in fake discovery");
    }

    fn include_linked_duplicate(&self) {
        std::fs::write(&self.include_linked_duplicate, b"linked\n")
            .expect("include linked pane duplicate in fake discovery");
    }

    fn replace_socket_path(&self) -> UnixListener {
        std::fs::remove_file(&self.socket).expect("unlink original fake tmux socket path");
        UnixListener::bind(&self.socket).expect("bind replacement fake tmux socket path")
    }

    fn control_pid(&self) -> u32 {
        let pids = wait_for_pid_file(&self.control_pids_file, 1);
        assert_eq!(pids.len(), 1, "expected one fake Control Mode client");
        pids[0]
    }

    fn hold_stdout_open_after_control_exit(&self) {
        std::fs::write(&self.hold_stdout_open, b"hold\n")
            .expect("enable held fake Control Mode stdout");
    }

    fn set_short_command_mode(&self, mode: &str) {
        std::fs::write(&self.short_command_mode, mode).expect("set fake short-command mode");
    }

    fn short_command_pids(&self, expected: usize) -> Vec<u32> {
        wait_for_pid_file(&self.short_command_pids_file, expected)
    }

    fn control_pids(&self, expected: usize) -> Vec<u32> {
        wait_for_pid_file(&self.control_pids_file, expected)
    }

    fn descendant_pids(&self, expected: usize) -> Vec<u32> {
        wait_for_pid_file(&self.descendant_pids_file, expected)
    }

    fn terminate_descendants(&self) {
        for pid in read_pid_file(&self.descendant_pids_file) {
            terminate_process(pid);
        }
    }

    fn assert_refresh_command(&self) {
        let command = std::fs::read(&self.refresh_log).expect("read fake tmux refresh command");
        assert_eq!(command, b"refresh-client -A %0:continue\n");
    }

    fn refresh_command_count(&self) -> usize {
        std::fs::read(&self.refresh_log).map_or(0, |commands| {
            commands
                .split(|byte| *byte == b'\n')
                .filter(|line| !line.is_empty())
                .count()
        })
    }
}

struct TmuxFixtureReservation {
    _guard: MutexGuard<'static, ()>,
}

fn lock_tmux_fixture_owner() -> TmuxFixtureReservation {
    // Each real/fake fixture starts independent daemon/server process trees.
    // Serialize those owners so cross-test process pressure cannot consume a
    // fixed production wall deadline; concurrency inside one daemon remains
    // explicit in the dedicated adversarial tests.
    TmuxFixtureReservation {
        _guard: TMUX_FIXTURE_OWNER
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
    }
}

impl Drop for FakeTmuxControl {
    fn drop(&mut self) {
        for pid in read_pid_file(&self.descendant_pids_file)
            .into_iter()
            .chain(read_pid_file(&self.short_command_pids_file))
            .chain(read_pid_file(&self.control_pids_file))
        {
            if !process_exists(pid) {
                continue;
            }
            let _ = Command::new("kill")
                .arg("-TERM")
                .arg(pid.to_string())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
        let _ = self.pane_process.kill();
        let _ = self.pane_process.wait();
    }
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).trim().to_owned()
}

fn portable_spec() -> RunSpec {
    RunSpec {
        program: "/bin/sh".to_owned(),
        args: vec!["-c".to_owned(), "exit 0".to_owned()],
        cwd: None,
        env: BTreeMap::default(),
        size: TerminalSize::default(),
        declared_inputs: Vec::new(),
    }
}

fn assert_protocol_error(error: ClientError, expected: ErrorCode) {
    match error {
        ClientError::Protocol { code, .. } => assert_eq!(code, expected),
        ClientError::ControlRejected { failure } => assert_eq!(failure.error.code, expected),
        other => panic!("expected protocol error {expected:?}, got {other:?}"),
    }
}

fn process_exists(pid: u32) -> bool {
    Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

#[cfg(target_os = "linux")]
fn process_file_descriptor_count(pid: u32) -> usize {
    std::fs::read_dir(format!("/proc/{pid}/fd"))
        .expect("read daemon file descriptors from procfs")
        .count()
}

#[cfg(target_os = "macos")]
fn process_file_descriptor_count(pid: u32) -> usize {
    let pid = pid.to_string();
    let output = Command::new("lsof")
        .args(["-a", "-p", pid.as_str(), "-Fn"])
        .output()
        .expect("inspect daemon file descriptors with lsof");
    assert!(
        output.status.success(),
        "lsof daemon file descriptor census failed: {}",
        stderr(&output)
    );
    String::from_utf8(output.stdout)
        .expect("lsof file descriptor census is UTF-8")
        .lines()
        .filter(|line| line.starts_with('f'))
        .count()
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
async fn stable_daemon_file_descriptor_count(daemon: &TestDaemon) -> usize {
    timeout(Duration::from_secs(3), async {
        let mut previous = None;
        let mut stable_samples = 0;
        loop {
            let current = daemon.file_descriptor_count();
            if previous == Some(current) {
                stable_samples += 1;
            } else {
                previous = Some(current);
                stable_samples = 1;
            }
            if stable_samples == 5 {
                return current;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("daemon file descriptor census should settle")
}

fn read_pid_file(path: &Path) -> Vec<u32> {
    let Ok(value) = std::fs::read_to_string(path) else {
        return Vec::new();
    };
    let mut pids = value
        .lines()
        .map(|line| line.parse().expect("parse fixture process PID"))
        .collect::<Vec<u32>>();
    pids.sort_unstable();
    pids.dedup();
    pids
}

fn wait_for_pid_file(path: &Path, expected: usize) -> Vec<u32> {
    for _ in 0..250 {
        let pids = read_pid_file(path);
        if pids.len() == expected {
            return pids;
        }
        assert!(
            pids.len() < expected,
            "fixture recorded more PIDs than expected: {pids:?}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!(
        "fixture did not record {expected} process PIDs at {}",
        path.display()
    );
}

fn terminate_process(pid: u32) {
    if !process_exists(pid) {
        return;
    }
    let status = Command::new("kill")
        .arg("-TERM")
        .arg(pid.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("terminate fixture process");
    assert!(
        status.success() || !process_exists(pid),
        "terminate fixture process {pid}: {status}"
    );
    assert_process_exits(pid);
}

async fn attach_with_timeout(
    client: &Client,
    id: RunId,
    after_byte: u64,
) -> (Attachment, AttachedSnapshot) {
    timeout(PUBLIC_ATTACHMENT_TIMEOUT, client.attach(id, after_byte))
        .await
        .expect("public attachment handshake exceeded its deadline")
        .expect("attach to Run")
}

async fn next_event_with_timeout(
    attachment: &mut Attachment,
) -> Result<Option<RunEvent>, ClientError> {
    timeout(PUBLIC_ATTACHMENT_TIMEOUT, attachment.next_event())
        .await
        .expect("public attachment event exceeded its deadline")
}

async fn detach_with_timeout(attachment: Attachment) {
    timeout(PUBLIC_ATTACHMENT_TIMEOUT, attachment.detach())
        .await
        .expect("public detach exceeded its deadline")
        .expect("detach from Run");
}

fn assert_process_exits(pid: u32) {
    for _ in 0..100 {
        if !process_exists(pid) {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("process {pid} remained alive past its exit deadline");
}

async fn wait_for_output(
    attachment: &mut Attachment,
    mut observed: Vec<u8>,
    expected: &[u8],
) -> Vec<u8> {
    timeout(Duration::from_secs(10), async {
        while !observed
            .windows(expected.len())
            .any(|window| window == expected)
        {
            match next_event_with_timeout(attachment)
                .await
                .expect("receive tmux attachment event")
                .expect("tmux attachment remains live")
            {
                RunEvent::Output { chunk } => observed.extend_from_slice(&chunk.data),
                RunEvent::Tmux { .. } => {}
                RunEvent::ObservationDiscontinuity => {
                    panic!("unexpected tmux observation discontinuity")
                }
                RunEvent::Gap {
                    latest_output_bytes,
                } => panic!("unexpected output gap at {latest_output_bytes}"),
                RunEvent::Exited { state } => panic!("tmux Run exited unexpectedly: {state:?}"),
                RunEvent::Interrupted { reason } => {
                    panic!("tmux Run was interrupted unexpectedly: {reason:?}")
                }
            }
        }
        observed
    })
    .await
    .expect("expected tmux output should arrive")
}

async fn wait_for_tmux_event(attachment: &mut Attachment, expected: TmuxRunEvent) {
    timeout(Duration::from_secs(10), async {
        loop {
            match next_event_with_timeout(attachment)
                .await
                .expect("receive tmux attachment event")
                .expect("tmux attachment remains live")
            {
                RunEvent::Tmux { event } if event == expected => return,
                RunEvent::Tmux { event } => {
                    panic!("unexpected tmux event while waiting for {expected:?}: {event:?}")
                }
                RunEvent::ObservationDiscontinuity => {
                    panic!("observation continuity was lost while waiting for {expected:?}")
                }
                RunEvent::Output { .. } => {}
                RunEvent::Gap {
                    latest_output_bytes,
                } => panic!("unexpected output gap at {latest_output_bytes}"),
                RunEvent::Exited { state } => panic!("tmux Run exited unexpectedly: {state:?}"),
                RunEvent::Interrupted { reason } => {
                    panic!("tmux Run was interrupted unexpectedly: {reason:?}")
                }
            }
        }
    })
    .await
    .expect("expected tmux event should arrive");
}

async fn wait_for_interruption(client: &Client, id: RunId, expected: InterruptionReason) {
    timeout(Duration::from_secs(10), async {
        loop {
            match client
                .status(id)
                .await
                .expect("read imported Run status")
                .state
            {
                RunState::Running => sleep(Duration::from_millis(20)).await,
                RunState::Interrupted { reason } => {
                    assert_eq!(reason, expected);
                    return;
                }
                state @ RunState::Exited { .. } => {
                    panic!("unexpected imported Run terminal state: {state:?}")
                }
            }
        }
    })
    .await
    .expect("imported Run should become interrupted");
}

async fn import_only_pane(client: &Client, server: &TmuxServer) -> (ctxmux_protocol::RunInfo, u32) {
    let (version, panes) = client
        .discover_tmux(server.socket_string())
        .await
        .expect("discover real tmux pane");
    assert!(!version.is_empty());
    assert_eq!(panes.len(), 1);
    let pane = &panes[0];
    assert_eq!(pane.tmux_version, version);
    let pane_pid = pane.pane_pid;
    let run = client
        .import_tmux(server.socket_string(), &pane.pane_id)
        .await
        .expect("import real tmux pane");
    (run, pane_pid)
}

async fn import_fake_pane(client: &Client, fake: &FakeTmuxControl) -> ctxmux_protocol::RunInfo {
    let (version, panes) = client
        .discover_tmux(fake.socket_string())
        .await
        .expect("discover fake tmux pane through the public adapter");
    assert_eq!(version, "3.6");
    assert_eq!(panes.len(), 1);
    assert_eq!(panes[0].tmux_version, version);
    assert_eq!(panes[0].pane_id, "%0");
    let run = client
        .import_tmux(fake.socket_string(), &panes[0].pane_id)
        .await
        .expect("import fake tmux pane through the public adapter");
    assert_eq!(run.pid, Some(fake.pane_pid()));
    run
}

async fn assert_fake_target_change(signature: impl FnOnce(u32) -> String) {
    let fake = FakeTmuxControl::create();
    let mut daemon = TestDaemon::start_with_tmux_bin(&fake.executable).await;
    let client = daemon.client();
    let run = import_fake_pane(&client, &fake).await;
    let control_pid = fake.control_pid();

    fake.set_probe_signature(&signature(fake.pane_pid()));
    wait_for_interruption(&client, run.id, InterruptionReason::TmuxTargetChanged).await;
    assert_process_exits(control_pid);
    daemon.shutdown_clean();
}

async fn assert_fake_protocol_corruption(corruption: &str) {
    let fake = FakeTmuxControl::create();
    let mut daemon = TestDaemon::start_with_tmux_bin(&fake.executable).await;
    let client = daemon.client();
    let run = import_fake_pane(&client, &fake).await;
    let control_pid = fake.control_pid();

    fake.trigger_protocol_corruption(corruption);
    wait_for_interruption(&client, run.id, InterruptionReason::TmuxProtocolError).await;
    assert_process_exits(control_pid);
    daemon.shutdown_clean();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hanging_discoveries_yield_single_worker_to_unrelated_native_requests() {
    const HANGING_REQUESTS: usize = 2;

    let fake = FakeTmuxControl::create();
    fake.set_short_command_mode("hang-discovery");
    let mut daemon = TestDaemon::start_with_single_worker_and_tmux_bin(&fake.executable).await;
    let client = daemon.client();
    let mut hanging = Vec::new();
    for _ in 0..HANGING_REQUESTS {
        let request_client = client.clone();
        let socket_path = fake.socket_string();
        hanging.push(tokio::spawn(async move {
            request_client.discover_tmux(socket_path).await
        }));
    }
    let helper_pids = fake.short_command_pids(HANGING_REQUESTS);

    let native = timeout(Duration::from_secs(1), client.start(portable_spec()))
        .await
        .expect("native start must not wait for hanging tmux discovery")
        .expect("start unrelated native Run");
    let listed = timeout(Duration::from_secs(1), client.list())
        .await
        .expect("list must not wait for hanging tmux discovery")
        .expect("list Runs during hanging tmux discovery");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, native.id);

    for request in hanging {
        let error = timeout(Duration::from_secs(6), request)
            .await
            .expect("tmux discovery must honor its public deadline")
            .expect("join hanging discovery task")
            .expect_err("hanging tmux discovery must fail");
        assert_protocol_error(error, ErrorCode::BackendUnavailable);
    }
    for pid in helper_pids {
        assert_process_exits(pid);
    }
    assert_eq!(client.list().await.expect("list after failures").len(), 1);
    daemon.shutdown_clean();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hanging_version_probe_is_bounded_reaped_and_request_local() {
    let fake = FakeTmuxControl::create();
    fake.set_short_command_mode("hang-version");
    let mut daemon = TestDaemon::start_with_tmux_bin(&fake.executable).await;
    let client = daemon.client();
    let request_client = client.clone();
    let socket_path = fake.socket_string();
    let request = tokio::spawn(async move { request_client.discover_tmux(socket_path).await });
    let helper_pid = fake.short_command_pids(1)[0];

    let error = timeout(Duration::from_secs(6), request)
        .await
        .expect("tmux version probe must honor its command deadline")
        .expect("join hanging version probe")
        .expect_err("hanging tmux version probe must fail");
    assert_protocol_error(error, ErrorCode::BackendUnavailable);
    assert_process_exits(helper_pid);
    assert!(
        client
            .list()
            .await
            .expect("list after version timeout")
            .is_empty()
    );
    assert!(process_exists(fake.pane_pid()));
    daemon.shutdown_clean();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn import_readiness_timeout_yields_and_rolls_back_before_publication() {
    let fake = FakeTmuxControl::create();
    fake.set_short_command_mode("hang-readiness");
    let mut daemon = TestDaemon::start_with_single_worker_and_tmux_bin(&fake.executable).await;
    let client = daemon.client();
    let import_client = client.clone();
    let socket_path = fake.socket_string();
    let import = tokio::spawn(async move { import_client.import_tmux(socket_path, "%0").await });
    let control_pid = fake.control_pid();

    let native = timeout(Duration::from_secs(1), client.start(portable_spec()))
        .await
        .expect("native start must not wait for tmux Control Mode readiness")
        .expect("start unrelated native Run");
    let error = timeout(Duration::from_secs(9), import)
        .await
        .expect("tmux import must honor its total deadline")
        .expect("join tmux import task")
        .expect_err("missing Control Mode readiness must reject import");
    assert_protocol_error(error, ErrorCode::BackendUnavailable);
    assert_process_exits(control_pid);
    assert!(process_exists(fake.pane_pid()));
    let listed = client.list().await.expect("list after rejected import");
    assert_eq!(listed.len(), 1, "rejected tmux import published a Run");
    assert_eq!(listed[0].id, native.id);
    daemon.shutdown_clean();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oversized_discovery_output_fails_explicitly_without_publication() {
    let fake = FakeTmuxControl::create();
    fake.set_short_command_mode("overflow-discovery");
    let mut daemon = TestDaemon::start_with_tmux_bin(&fake.executable).await;
    let client = daemon.client();

    let discovery_error = client
        .discover_tmux(fake.socket_string())
        .await
        .expect_err("oversized discovery stdout must fail");
    assert_bounded_capture_error(discovery_error);
    let first_pid = fake.short_command_pids(1)[0];
    assert_process_exits(first_pid);

    let import_error = client
        .import_tmux(fake.socket_string(), "%0")
        .await
        .expect_err("import must not parse truncated discovery stdout");
    assert_bounded_capture_error(import_error);
    for pid in fake.short_command_pids(2) {
        assert_process_exits(pid);
    }
    assert!(
        client
            .list()
            .await
            .expect("list after overflows")
            .is_empty()
    );
    assert!(process_exists(fake.pane_pid()));
    daemon.shutdown_clean();
}

fn assert_bounded_capture_error(error: ClientError) {
    match error {
        ClientError::Protocol { code, message } => {
            assert_eq!(code, ErrorCode::BackendUnavailable);
            assert!(
                message.contains("131072-byte capture limit"),
                "overflow error must name its bounded owner: {message}"
            );
            assert!(
                message.len() < 1024,
                "overflow error frame grew unexpectedly"
            );
        }
        other => panic!("expected bounded backend error, got {other:?}"),
    }
}

fn expected_burst() -> Vec<u8> {
    let mut expected = b"BURST-BEGIN\r\n".to_vec();
    for index in 0..4096 {
        expected.extend_from_slice(format!("BURST:{index:04}:abcdefgh\r\n").as_bytes());
    }
    expected.extend_from_slice(b"BURST-END\r\n");
    expected
}

async fn collect_exact_output_with_gap_replay(
    client: &Client,
    id: RunId,
    after_byte: u64,
    expected_end: &[u8],
) -> Vec<u8> {
    timeout(Duration::from_secs(10), async {
        let mut cursor = after_byte;
        let mut observed = Vec::new();
        loop {
            let (mut attachment, snapshot) = attach_with_timeout(client, id, cursor).await;
            assert!(
                !snapshot.replay.truncated || cursor == 0,
                "burst remains below retention and must be replayable after the import boundary"
            );
            for chunk in snapshot.replay.chunks {
                assert_eq!(chunk.start_byte, cursor);
                cursor = chunk.end_byte;
                observed.extend_from_slice(&chunk.data);
            }
            if observed.ends_with(expected_end) {
                detach_with_timeout(attachment).await;
                return observed;
            }

            loop {
                match next_event_with_timeout(&mut attachment)
                    .await
                    .expect("receive queued-output event")
                    .expect("queued-output attachment remains live")
                {
                    RunEvent::Output { chunk } => {
                        assert_eq!(chunk.start_byte, cursor);
                        cursor = chunk.end_byte;
                        observed.extend_from_slice(&chunk.data);
                        if observed.ends_with(expected_end) {
                            detach_with_timeout(attachment).await;
                            return observed;
                        }
                    }
                    RunEvent::Gap { .. } | RunEvent::ObservationDiscontinuity => {
                        drop(attachment);
                        break;
                    }
                    RunEvent::Tmux { .. } => {}
                    RunEvent::Exited { state } => {
                        panic!("tmux Run exited during queued output: {state:?}")
                    }
                    RunEvent::Interrupted { reason } => {
                        panic!("tmux Run was interrupted during queued output: {reason:?}")
                    }
                }
            }
        }
    })
    .await
    .expect("queued output should be recoverable through public replay")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_discovery_import_exposes_raw_since_import_without_prehistory() {
    let Some(server) = TmuxServer::start() else {
        return;
    };
    let mut daemon = TestDaemon::start().await;
    let client = daemon.client();
    let (run, pane_process_id) = import_only_pane(&client, &server).await;

    assert_eq!(run.spec, None);
    assert_eq!(run.lineage, None);
    assert_eq!(run.pid, Some(pane_process_id));
    assert_eq!(run.capabilities, RunCapabilities::TMUX_READ_ONLY);
    assert_eq!(run.capabilities.replay, ReplayCapability::RawSinceImport);
    match &run.backend {
        RunBackend::Tmux {
            socket_path,
            pane_id,
            ..
        } => {
            assert_eq!(socket_path, &server.socket_string());
            assert!(pane_id.starts_with('%'));
        }
        RunBackend::Native => panic!("imported pane must expose the tmux backend"),
    }

    let target_pane_id = match &run.backend {
        RunBackend::Tmux { pane_id, .. } => pane_id.clone(),
        RunBackend::Native => unreachable!(),
    };
    let (mut attachment, snapshot) = attach_with_timeout(&client, run.id, 0).await;
    let mut observed = replay_bytes(&snapshot.replay.chunks);
    assert!(snapshot.replay.truncated);
    assert_eq!(snapshot.replay.first_available_byte, 0);
    assert_eq!(snapshot.replay.latest_output_bytes, 0);
    assert!(observed.is_empty());

    server.send_line(&target_pane_id, "after-import");
    observed = wait_for_output(&mut attachment, observed, b"OUT:after-import\r\n").await;
    assert!(
        !observed
            .windows(b"BEFORE-IMPORT".len())
            .any(|window| window == b"BEFORE-IMPORT")
    );

    server.send_line(&target_pane_id, "bytes");
    let observed = wait_for_output(&mut attachment, observed, b"BYTES:%\r\n\x1b\x7f\xff").await;
    assert!(observed.ends_with(b"BYTES:%\r\n\x1b\x7f\xff"));
    detach_with_timeout(attachment).await;
    daemon.shutdown_clean();
    server.shutdown_clean();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_clients_disconnect_and_daemon_exit_preserve_tmux_ownership() {
    let Some(server) = TmuxServer::start() else {
        return;
    };
    let mut daemon = TestDaemon::start().await;
    let first_client = daemon.client();
    let second_client = daemon.client();
    let (run, pane_process_id) = import_only_pane(&first_client, &server).await;
    let control_process_id = server.only_client_pid();
    assert_ne!(control_process_id, pane_process_id);
    assert_ne!(Some(control_process_id), server.server_pid);
    let target_pane_id = match &run.backend {
        RunBackend::Tmux { pane_id, .. } => pane_id.clone(),
        RunBackend::Native => unreachable!(),
    };
    let (mut first, first_snapshot) = attach_with_timeout(&first_client, run.id, 0).await;
    let (mut second, second_snapshot) = attach_with_timeout(&second_client, run.id, 0).await;
    assert_eq!(first_client.status(run.id).await.unwrap().attachments, 2);

    server.send_line(&target_pane_id, "two-clients");
    let first_observed = wait_for_output(
        &mut first,
        replay_bytes(&first_snapshot.replay.chunks),
        b"OUT:two-clients\r\n",
    )
    .await;
    let second_observed = wait_for_output(
        &mut second,
        replay_bytes(&second_snapshot.replay.chunks),
        b"OUT:two-clients\r\n",
    )
    .await;
    assert_eq!(first_observed, b"OUT:two-clients\r\n");
    assert_eq!(second_observed, first_observed);
    detach_with_timeout(first).await;
    drop(second);

    daemon.shutdown_clean();
    assert_process_exits(control_process_id);
    server.assert_no_clients();
    assert!(server.is_reachable());
    assert!(process_exists(pane_process_id));
    server.send_line(&target_pane_id, "after-daemon-exit");
    assert!(server.is_reachable());
    assert!(process_exists(pane_process_id));
    server.shutdown_clean();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rename_is_observable_and_pane_exit_is_a_target_change() {
    let Some(server) = TmuxServer::start() else {
        return;
    };
    server.add_keeper_session();
    let mut daemon = TestDaemon::start().await;
    let client = daemon.client();
    let (version, panes) = client
        .discover_tmux(server.socket_string())
        .await
        .expect("discover tmux panes");
    assert!(!version.is_empty());
    let target = panes
        .iter()
        .find(|pane| pane.session_id == "$0")
        .expect("find target pane");
    let run = client
        .import_tmux(server.socket_string(), &target.pane_id)
        .await
        .expect("import target pane");
    let control_process_id = server.only_client_pid();
    let (mut attachment, _) = attach_with_timeout(&client, run.id, 0).await;

    server.rename_session("ctxmux-keeper", "ctxmux-keeper-renamed");
    assert!(
        timeout(Duration::from_millis(300), attachment.next_event())
            .await
            .is_err(),
        "renaming an unrelated session must not emit an event for the imported pane"
    );
    server.rename_target("ctxmux-renamed");
    wait_for_tmux_event(
        &mut attachment,
        TmuxRunEvent::SessionRenamed {
            name: b"ctxmux-renamed".to_vec(),
        },
    )
    .await;
    assert!(client.status(run.id).await.unwrap().state.is_running());

    server.send_line(&target.pane_id, "quit");
    wait_for_interruption(&client, run.id, InterruptionReason::TmuxTargetChanged).await;
    assert_process_exits(control_process_id);
    assert!(
        server
            .command()
            .args(["has-session", "-t", "ctxmux-keeper-renamed"])
            .status()
            .is_ok_and(|status| status.success())
    );
    drop(attachment);
    daemon.shutdown_clean();
    server.shutdown_clean();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tmux_server_loss_is_an_explicit_interruption() {
    let Some(mut server) = TmuxServer::start() else {
        return;
    };
    let mut daemon = TestDaemon::start().await;
    let client = daemon.client();
    let (run, _) = import_only_pane(&client, &server).await;
    let control_process_id = server.only_client_pid();

    server.kill_server_clean();
    assert_process_exits(control_process_id);
    wait_for_interruption(&client, run.id, InterruptionReason::TmuxServerUnavailable).await;
    daemon.shutdown_clean();
    server.shutdown_clean();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unsupported_tmux_version_fails_before_server_access() {
    let _fixture_owner = lock_tmux_fixture_owner();
    let fixture = tempfile::tempdir().expect("create version fixture directory");
    let executable = fixture.path().join("tmux-unsupported");
    std::fs::write(
        &executable,
        "#!/bin/sh\nif [ \"$1\" = \"-V\" ]; then printf 'tmux 3.3\\n'; exit 0; fi\nexit 99\n",
    )
    .expect("write unsupported tmux fixture");
    std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700))
        .expect("make unsupported tmux fixture executable");
    let mut daemon = TestDaemon::start_with_tmux_bin(&executable).await;

    let error = daemon
        .client()
        .discover_tmux("/socket/must/not/be/consulted")
        .await
        .unwrap_err();
    assert_protocol_error(error, ErrorCode::UnsupportedBackendVersion);
    daemon.shutdown_clean();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn discovery_reports_and_validates_the_selected_server_version() {
    let fake = FakeTmuxControl::create();
    fake.set_server_version("3.5a");
    let mut supported = TestDaemon::start_with_tmux_bin(&fake.executable).await;

    let (version, panes) = supported
        .client()
        .discover_tmux(fake.socket_string())
        .await
        .expect("supported server version should be discoverable");
    assert_eq!(version, "3.5a");
    assert_eq!(panes.len(), 1);
    assert_eq!(panes[0].tmux_version, "3.5a");
    supported.shutdown_clean();

    fake.set_server_version("3.3");
    let mut unsupported = TestDaemon::start_with_tmux_bin(&fake.executable).await;
    let error = unsupported
        .client()
        .discover_tmux(fake.socket_string())
        .await
        .unwrap_err();
    assert_protocol_error(error, ErrorCode::UnsupportedBackendVersion);
    unsupported.shutdown_clean();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn discovery_skips_dead_panes_and_dead_target_import_fails_closed() {
    let fake = FakeTmuxControl::create();
    fake.include_dead_pane();
    let mut daemon = TestDaemon::start_with_tmux_bin(&fake.executable).await;
    let client = daemon.client();

    let (version, panes) = client
        .discover_tmux(fake.socket_string())
        .await
        .expect("a dead pane must not block discovery of live panes");
    assert_eq!(version, "3.6");
    assert_eq!(panes.len(), 1);
    assert_eq!(panes[0].pane_id, "%0");
    assert_protocol_error(
        client
            .import_tmux(fake.socket_string(), "%1")
            .await
            .unwrap_err(),
        ErrorCode::TargetChanged,
    );

    let run = client
        .import_tmux(fake.socket_string(), "%0")
        .await
        .expect("the live pane remains importable");
    assert!(run.state.is_running());
    let control_pid = fake.control_pid();
    daemon.shutdown_clean();
    assert_process_exits(control_pid);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn linked_pane_duplicate_is_discoverable_but_ambiguous_to_import() {
    let fake = FakeTmuxControl::create();
    fake.include_linked_duplicate();
    let mut daemon = TestDaemon::start_with_tmux_bin(&fake.executable).await;
    let client = daemon.client();

    let (version, panes) = client
        .discover_tmux(fake.socket_string())
        .await
        .expect("linked pane memberships should remain visible in discovery");
    assert_eq!(version, "3.6");
    assert_eq!(panes.len(), 2);
    assert!(panes.iter().all(|pane| pane.pane_id == "%0"));
    assert_ne!(panes[0].session_id, panes[1].session_id);
    assert_ne!(panes[0].window_id, panes[1].window_id);

    assert_protocol_error(
        client
            .import_tmux(fake.socket_string(), "%0")
            .await
            .unwrap_err(),
        ErrorCode::TargetChanged,
    );
    daemon.shutdown_clean();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pane_move_to_another_window_invalidates_the_import_identity() {
    assert_fake_target_change(|pane_pid| format!("4100\t1700000000\t$0\t@9\t%0\t{pane_pid}")).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pane_break_to_another_session_invalidates_the_import_identity() {
    assert_fake_target_change(|pane_pid| format!("4100\t1700000000\t$9\t@9\t%0\t{pane_pid}")).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pane_respawn_invalidates_the_import_identity() {
    assert_fake_target_change(|_| "4100\t1700000000\t$0\t@0\t%0\t4999".to_owned()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pane_id_reuse_in_a_new_server_epoch_invalidates_the_import_identity() {
    assert_fake_target_change(|pane_pid| format!("5100\t1800000000\t$0\t@0\t%0\t{pane_pid}")).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replacement_tmux_socket_path_invalidates_the_import_identity() {
    let fake = FakeTmuxControl::create();
    fake.hold_stdout_open_after_control_exit();
    let mut daemon = TestDaemon::start_with_tmux_bin(&fake.executable).await;
    let client = daemon.client();
    let run = import_fake_pane(&client, &fake).await;
    let (mut attachment, _) = attach_with_timeout(&client, run.id, 0).await;
    let control_pid = fake.control_pid();
    let descendant_pids = fake.descendant_pids(1);

    let _replacement_listener = fake.replace_socket_path();
    assert_process_exits(control_pid);
    assert!(
        descendant_pids.iter().all(|pid| process_exists(*pid)),
        "stdout holder barrier opened before the Control child was reaped: {descendant_pids:?}",
    );
    fake.terminate_descendants();
    assert!(descendant_pids.into_iter().all(|pid| !process_exists(pid)));
    assert_eq!(
        next_event_with_timeout(&mut attachment)
            .await
            .expect("read target-change interruption event"),
        Some(RunEvent::Interrupted {
            reason: InterruptionReason::TmuxTargetChanged,
        })
    );
    assert_eq!(
        next_event_with_timeout(&mut attachment)
            .await
            .expect("target change closes after one interruption event"),
        None
    );
    wait_for_interruption(&client, run.id, InterruptionReason::TmuxTargetChanged).await;
    daemon.shutdown_clean();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blank_control_lines_preserve_import_and_exact_following_output() {
    let fake = FakeTmuxControl::create();
    fake.set_control_startup_mode("blank-lines");
    let mut daemon = TestDaemon::start_with_tmux_bin(&fake.executable).await;
    let client = daemon.client();
    let run = import_fake_pane(&client, &fake).await;
    let (mut attachment, snapshot) = attach_with_timeout(&client, run.id, 0).await;
    let observed = wait_for_output(
        &mut attachment,
        replay_bytes(&snapshot.replay.chunks),
        b"BLANK-EXACT\r\n",
    )
    .await;
    assert_eq!(observed, b"BLANK-EXACT\r\n");
    assert!(client.status(run.id).await.unwrap().state.is_running());
    detach_with_timeout(attachment).await;
    daemon.shutdown_clean();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bootstrap_accepts_a_nonzero_start_and_command_number_gap() {
    let fake = FakeTmuxControl::create();
    fake.set_control_startup_mode("nonzero-gap");
    let mut daemon = TestDaemon::start_with_tmux_bin(&fake.executable).await;
    let client = daemon.client();
    let run = import_fake_pane(&client, &fake).await;
    assert!(run.state.is_running());
    assert!(client.status(run.id).await.unwrap().state.is_running());
    daemon.shutdown_clean();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bootstrap_must_be_unique_successful_and_empty() {
    for mode in ["double-bootstrap", "bootstrap-error", "bootstrap-nonempty"] {
        let fake = FakeTmuxControl::create();
        fake.set_control_startup_mode(mode);
        let mut daemon = TestDaemon::start_with_tmux_bin(&fake.executable).await;
        let client = daemon.client();
        let (_, panes) = client
            .discover_tmux(fake.socket_string())
            .await
            .expect("discover fake pane before invalid bootstrap");
        let error = client
            .import_tmux(fake.socket_string(), &panes[0].pane_id)
            .await
            .expect_err("invalid bootstrap must reject import");
        assert_protocol_error(error, ErrorCode::BackendUnavailable);
        assert!(client.list().await.unwrap().is_empty());
        daemon.shutdown_clean();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn open_command_block_before_readiness_rejects_import_with_transcript_detail() {
    let fake = FakeTmuxControl::create();
    fake.set_control_startup_mode("open-block-eof");
    fake.hold_stdout_open_after_control_exit();
    let mut daemon = TestDaemon::start_with_tmux_bin(&fake.executable).await;
    let client = daemon.client();
    let (_, panes) = client
        .discover_tmux(fake.socket_string())
        .await
        .expect("discover fake pane before truncated bootstrap");
    let import_client = client.clone();
    let socket_path = fake.socket_string();
    let pane_id = panes[0].pane_id.clone();
    let import =
        tokio::spawn(async move { import_client.import_tmux(socket_path, &pane_id).await });
    let control_pid = fake.control_pid();
    let descendant_pids = fake.descendant_pids(1);
    assert_process_exits(control_pid);
    assert!(
        descendant_pids.iter().all(|pid| process_exists(*pid)),
        "stdout holder barrier opened before the Control child was reaped: {descendant_pids:?}",
    );
    fake.terminate_descendants();
    assert!(descendant_pids.into_iter().all(|pid| !process_exists(pid)));

    match import
        .await
        .expect("join truncated-bootstrap import")
        .expect_err("truncated bootstrap must reject import")
    {
        ClientError::Protocol { code, message } => {
            assert_eq!(code, ErrorCode::BackendUnavailable);
            assert!(
                message.contains("ended inside a command block"),
                "truncated transcript detail was lost: {message}"
            );
        }
        error => panic!("expected protocol error for truncated bootstrap, got {error:?}"),
    }
    assert!(client.list().await.unwrap().is_empty());
    daemon.shutdown_clean();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn command_result_number_or_pending_mismatch_fails_closed() {
    for trigger in ["duplicate", "backwards", "no-pending"] {
        let fake = FakeTmuxControl::create();
        let mut daemon = TestDaemon::start_with_tmux_bin(&fake.executable).await;
        let client = daemon.client();
        let run = import_fake_pane(&client, &fake).await;
        fake.trigger_control(trigger);
        wait_for_interruption(&client, run.id, InterruptionReason::TmuxProtocolError).await;
        assert!(process_exists(fake.pane_pid()));
        daemon.shutdown_clean();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn control_eof_distinguishes_server_loss_from_an_open_command_block() {
    for (trigger, expected) in [
        ("eof", InterruptionReason::TmuxServerUnavailable),
        ("open-block-eof", InterruptionReason::TmuxProtocolError),
    ] {
        let fake = FakeTmuxControl::create();
        fake.hold_stdout_open_after_control_exit();
        let mut daemon = TestDaemon::start_with_tmux_bin(&fake.executable).await;
        let client = daemon.client();
        let run = import_fake_pane(&client, &fake).await;
        let (mut attachment, _) = attach_with_timeout(&client, run.id, 0).await;
        let control_pid = fake.control_pid();
        let descendant_pids = fake.descendant_pids(1);
        fake.trigger_control(trigger);
        assert_process_exits(control_pid);
        assert!(
            descendant_pids.iter().all(|pid| process_exists(*pid)),
            "stdout holder barrier opened before the Control child was reaped: {descendant_pids:?}",
        );
        fake.terminate_descendants();
        assert!(descendant_pids.into_iter().all(|pid| !process_exists(pid)));
        assert_eq!(
            next_event_with_timeout(&mut attachment)
                .await
                .expect("read one EOF interruption event"),
            Some(RunEvent::Interrupted { reason: expected })
        );
        assert_eq!(
            next_event_with_timeout(&mut attachment)
                .await
                .expect("EOF closes after one interruption event"),
            None
        );
        wait_for_interruption(&client, run.id, expected).await;
        assert!(process_exists(fake.pane_pid()));
        daemon.shutdown_clean();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(any(target_os = "linux", target_os = "macos"))]
async fn terminal_tmux_runs_release_control_writers_without_a_daemon_fd_slope() {
    const ROUNDS: usize = 4;

    let fake = FakeTmuxControl::create();
    let mut daemon = TestDaemon::start_with_tmux_bin(&fake.executable).await;
    let client = daemon.client();
    let baseline = stable_daemon_file_descriptor_count(&daemon).await;

    for round in 1..=ROUNDS {
        let run = import_fake_pane(&client, &fake).await;
        let control_pid = *fake
            .control_pids(round)
            .last()
            .expect("current fake Control Mode PID");

        fake.trigger_control("eof");
        wait_for_interruption(&client, run.id, InterruptionReason::TmuxServerUnavailable).await;
        assert_process_exits(control_pid);

        let historical = client
            .status(run.id)
            .await
            .expect("status retains interrupted tmux Run");
        assert_eq!(historical.id, run.id);
        assert_eq!(&historical.backend, &run.backend);
        assert_eq!(
            historical.state,
            RunState::Interrupted {
                reason: InterruptionReason::TmuxServerUnavailable,
            }
        );
        let (attachment, snapshot) = attach_with_timeout(&client, run.id, 0).await;
        assert_eq!(snapshot.run.id, historical.id);
        assert_eq!(&snapshot.run.backend, &historical.backend);
        assert_eq!(snapshot.run.state, historical.state);
        let mut attachment = attachment;
        assert_eq!(
            next_event_with_timeout(&mut attachment)
                .await
                .expect("read historical tmux terminal event"),
            Some(RunEvent::Interrupted {
                reason: InterruptionReason::TmuxServerUnavailable,
            })
        );
        assert_eq!(
            next_event_with_timeout(&mut attachment)
                .await
                .expect("historical tmux attachment closes after its terminal event"),
            None
        );
        drop(attachment);
        assert_eq!(client.list().await.unwrap().len(), round);
        assert_eq!(client.status(run.id).await.unwrap().attachments, 0);
        assert!(process_exists(fake.pane_pid()));

        let settled = stable_daemon_file_descriptor_count(&daemon).await;
        assert_eq!(
            settled, baseline,
            "terminal tmux Run {round}/{ROUNDS} retained a daemon file descriptor"
        );
    }

    daemon.shutdown_clean();
    assert!(process_exists(fake.pane_pid()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pause_storm_writes_one_bounded_continue_command() {
    let fake = FakeTmuxControl::create();
    let mut daemon = TestDaemon::start_with_tmux_bin(&fake.executable).await;
    let client = daemon.client();
    let run = import_fake_pane(&client, &fake).await;
    fake.trigger_control("pause-storm");
    let expected = b"AFTER-CONTINUE\r\nSTORM-DRAINED\r\n";

    timeout(Duration::from_secs(5), async {
        while client.status(run.id).await.unwrap().latest_output_bytes
            < u64::try_from(expected.len()).expect("fixture output length fits u64")
        {
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("post-storm output should follow the draining target probe result");
    let (attachment, snapshot) = attach_with_timeout(&client, run.id, 0).await;
    assert_eq!(replay_bytes(&snapshot.replay.chunks), expected);
    assert_eq!(fake.refresh_command_count(), 1);
    assert!(client.status(run.id).await.unwrap().state.is_running());
    assert!(process_exists(fake.pane_pid()));
    detach_with_timeout(attachment).await;
    daemon.shutdown_clean();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_control_mode_after_readiness_is_a_protocol_interruption() {
    assert_fake_protocol_corruption("malformed").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oversized_control_mode_after_readiness_is_a_protocol_interruption() {
    assert_fake_protocol_corruption("oversized").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_pause_emits_exact_gap_and_requests_control_mode_continue() {
    let fake = FakeTmuxControl::create();
    let mut daemon = TestDaemon::start_with_tmux_bin(&fake.executable).await;
    let client = daemon.client();
    let run = import_fake_pane(&client, &fake).await;
    let (mut attachment, snapshot) = attach_with_timeout(&client, run.id, 0).await;
    assert!(snapshot.replay.truncated);
    assert_eq!(snapshot.replay.latest_output_bytes, 0);
    assert!(snapshot.replay.chunks.is_empty());

    fake.trigger_output_before_pause();
    let before_pause = match next_event_with_timeout(&mut attachment)
        .await
        .expect("receive deterministic pre-pause output")
    {
        Some(RunEvent::Output { chunk }) => chunk,
        event => panic!("expected pre-pause output, got {event:?}"),
    };
    assert_eq!((before_pause.start_byte, before_pause.end_byte), (0, 14));
    assert_eq!(before_pause.data, b"BEFORE-PAUSE\r\n");
    let caller_cursor = before_pause.end_byte;

    fake.trigger_pause();

    assert_eq!(
        next_event_with_timeout(&mut attachment)
            .await
            .expect("receive public paused event"),
        Some(RunEvent::Tmux {
            event: TmuxRunEvent::Paused,
        })
    );
    assert_eq!(
        next_event_with_timeout(&mut attachment)
            .await
            .expect("receive public Gap event"),
        Some(RunEvent::Gap {
            latest_output_bytes: caller_cursor,
        })
    );
    assert_eq!(
        next_event_with_timeout(&mut attachment)
            .await
            .expect("receive public continued event"),
        Some(RunEvent::Tmux {
            event: TmuxRunEvent::Continued,
        })
    );
    let after_continue = match next_event_with_timeout(&mut attachment)
        .await
        .expect("receive deterministic post-continue output")
    {
        Some(RunEvent::Output { chunk }) => chunk,
        event => panic!("expected post-continue output, got {event:?}"),
    };
    assert_eq!(after_continue.start_byte, caller_cursor);
    assert_eq!(after_continue.data, b"AFTER-CONTINUE\r\n");
    fake.assert_refresh_command();
    let control_pid = fake.control_pid();
    assert!(process_exists(control_pid));
    assert!(process_exists(fake.pane_pid()));

    detach_with_timeout(attachment).await;

    let (reattached, replay) = attach_with_timeout(&client, run.id, caller_cursor).await;
    assert!(replay.replay.truncated);
    assert_eq!(replay.replay.latest_output_bytes, after_continue.end_byte);
    assert_eq!(replay.replay.chunks, vec![after_continue.clone()]);
    assert_eq!(replay_bytes(&replay.replay.chunks), b"AFTER-CONTINUE\r\n");
    detach_with_timeout(reattached).await;

    let (late, late_replay) = attach_with_timeout(&client, run.id, caller_cursor).await;
    assert!(late_replay.replay.truncated);
    assert_eq!(
        late_replay.replay.latest_output_bytes,
        after_continue.end_byte
    );
    assert_eq!(late_replay.replay.chunks, vec![after_continue]);
    assert_eq!(
        replay_bytes(&late_replay.replay.chunks),
        b"AFTER-CONTINUE\r\n"
    );
    detach_with_timeout(late).await;

    assert!(client.status(run.id).await.unwrap().state.is_running());
    assert!(process_exists(control_pid));
    assert!(process_exists(fake.pane_pid()));
    daemon.shutdown_clean();
    assert_process_exits(control_pid);
    assert!(process_exists(fake.pane_pid()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_failure_is_nonzero_and_bounded_across_multiple_control_clients() {
    const CONTROL_CLIENTS: usize = 3;

    let fake = FakeTmuxControl::create();
    fake.hold_stdout_open_after_control_exit();
    let mut daemon = TestDaemon::start_with_tmux_bin(&fake.executable).await;
    let client = daemon.client();
    let (_, panes) = client
        .discover_tmux(fake.socket_string())
        .await
        .expect("discover fake tmux pane through the public adapter");
    assert_eq!(panes.len(), 1);

    for _ in 0..CONTROL_CLIENTS {
        client
            .import_tmux(fake.socket_string(), &panes[0].pane_id)
            .await
            .expect("import an independent fake Control Mode client");
    }
    let control_pids = fake.control_pids(CONTROL_CLIENTS);
    let descendant_pids = fake.descendant_pids(CONTROL_CLIENTS);
    assert!(control_pids.iter().all(|pid| process_exists(*pid)));
    assert!(descendant_pids.iter().all(|pid| process_exists(*pid)));

    let started = Instant::now();
    let status = daemon.shutdown_status(Duration::from_secs(8));
    let elapsed = started.elapsed();

    assert!(
        !status.success(),
        "held Control Mode stdout must make strict daemon cleanup fail"
    );
    assert!(
        elapsed < Duration::from_secs(3),
        "three cleanup drains must share one bounded window, elapsed={elapsed:?}"
    );
    for pid in control_pids {
        assert_process_exits(pid);
    }

    fake.terminate_descendants();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unsupported_controls_and_persistent_import_fail_closed() {
    let Some(server) = TmuxServer::start() else {
        return;
    };
    let mut daemon = TestDaemon::start().await;
    let client = daemon.client();
    let (run, pane_process_id) = import_only_pane(&client, &server).await;

    let attachment_stop = fresh_stop(&client, run.id).await;
    let (attachment, _) = attach_with_timeout(&client, run.id, 0).await;
    assert_protocol_error(
        attachment
            .input(b"forbidden attachment input".to_vec())
            .await
            .unwrap_err(),
        ErrorCode::UnsupportedCapability,
    );
    assert_protocol_error(
        attachment
            .resize(TerminalSize {
                cols: 101,
                rows: 41,
            })
            .await
            .unwrap_err(),
        ErrorCode::UnsupportedCapability,
    );
    assert_protocol_error(
        attachment.stop(attachment_stop).await.unwrap_err(),
        ErrorCode::UnsupportedCapability,
    );
    assert_protocol_error(
        attachment.interrupt().await.unwrap_err(),
        ErrorCode::UnsupportedCapability,
    );
    detach_with_timeout(attachment).await;

    assert_protocol_error(
        client
            .input(run.id, b"forbidden".to_vec())
            .await
            .unwrap_err(),
        ErrorCode::UnsupportedCapability,
    );
    assert_protocol_error(
        client
            .resize(
                run.id,
                TerminalSize {
                    cols: 100,
                    rows: 40,
                },
            )
            .await
            .unwrap_err(),
        ErrorCode::UnsupportedCapability,
    );
    let short_stop = fresh_stop(&client, run.id).await;
    assert_protocol_error(
        client.stop(short_stop).await.unwrap_err(),
        ErrorCode::UnsupportedCapability,
    );
    assert_protocol_error(
        client.interrupt(run.id).await.unwrap_err(),
        ErrorCode::UnsupportedCapability,
    );
    assert_protocol_error(
        client.fork(run.id, ForkPlan::LevelA).await.unwrap_err(),
        ErrorCode::UnsupportedCapability,
    );
    assert_protocol_error(
        client
            .fork(
                run.id,
                ForkPlan::LevelB {
                    spec: portable_spec(),
                },
            )
            .await
            .unwrap_err(),
        ErrorCode::UnsupportedCapability,
    );
    assert!(process_exists(pane_process_id));
    daemon.shutdown_clean();

    let mut persistent = TestDaemon::start_persistent().await;
    let persistent_client = persistent.client();
    let (_, panes) = persistent_client
        .discover_tmux(server.socket_string())
        .await
        .expect("persistent daemon can discover without adopting a pane");
    let error = persistent_client
        .import_tmux(server.socket_string(), &panes[0].pane_id)
        .await
        .unwrap_err();
    assert_protocol_error(error, ErrorCode::UnsupportedCapability);
    assert!(persistent_client.list().await.unwrap().is_empty());
    assert!(server.is_reachable());
    assert!(process_exists(pane_process_id));
    persistent.shutdown_clean();
    server.shutdown_clean();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn detach_during_queued_output_preserves_replay_and_tmux_session() {
    let Some(server) = TmuxServer::start() else {
        return;
    };
    let mut daemon = TestDaemon::start().await;
    let client = daemon.client();
    let (run, pane_process_id) = import_only_pane(&client, &server).await;
    let target_pane_id = match &run.backend {
        RunBackend::Tmux { pane_id, .. } => pane_id.clone(),
        RunBackend::Native => unreachable!(),
    };
    let (attachment, _) = attach_with_timeout(&client, run.id, 0).await;

    server.send_line(&target_pane_id, "burst");
    timeout(Duration::from_secs(5), attachment.detach())
        .await
        .expect("detach should not hang behind queued output")
        .expect("detach during queued output");

    let observed = collect_exact_output_with_gap_replay(&client, run.id, 0, b"BURST-END\r\n").await;
    assert_eq!(observed, expected_burst());
    assert!(server.is_reachable());
    assert!(process_exists(pane_process_id));

    let after_byte = client.status(run.id).await.unwrap().latest_output_bytes;
    let (mut control_lifetime, snapshot) = attach_with_timeout(&client, run.id, after_byte).await;
    server.send_line(&target_pane_id, "burst");
    wait_for_output(
        &mut control_lifetime,
        replay_bytes(&snapshot.replay.chunks),
        b"BURST-BEGIN\r\n",
    )
    .await;
    daemon.shutdown_clean();
    assert!(server.is_reachable());
    assert!(process_exists(pane_process_id));
    drop(control_lifetime);
    server.shutdown_clean();
}
