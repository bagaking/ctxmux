use std::{
    collections::BTreeMap,
    env,
    ffi::OsString,
    io::{self, IsTerminal, Read, Write},
    path::PathBuf,
    process::ExitCode,
    str::FromStr,
    thread,
};

use crossterm::terminal::{disable_raw_mode, enable_raw_mode, size as terminal_size};
use ctxmux_client::{Client, replay_bytes};
use ctxmux_protocol::{
    CreateOperationKey, ForkFidelity, ForkPlan, PROTOCOL_VERSION, RunEvent, RunId, RunInfo,
    RunSpec, RunState, TerminalSize,
};
use tokio::{
    signal::unix::{SignalKind, signal},
    sync::mpsc,
};

fn usage() -> &'static str {
    "ctxmux — context-aware local Run multiplexer

usage:
  ctxmux --version
  ctxmux --socket <path> ping
  ctxmux --socket <path> start [--operation-key <key>] [--cwd <path>] [--cols <n>] [--rows <n>] -- <program> [args...]
  ctxmux --socket <path> tmux-list <tmux-socket>
  ctxmux --socket <path> tmux-import <tmux-socket> <pane-id>
  ctxmux --socket <path> fork [--operation-key <key>] <run-id>
  ctxmux --socket <path> list
  ctxmux --socket <path> status <run-id>
  ctxmux --socket <path> input <run-id> <text>
  ctxmux --socket <path> input <run-id> --stdin
  ctxmux --socket <path> resize <run-id> <cols> <rows>
  ctxmux --socket <path> attach <run-id> [after-seq]
  ctxmux --socket <path> stop <run-id>

CTXMUX_SOCKET may be used instead of --socket."
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("ctxmux: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), String> {
    let mut args = env::args_os().skip(1).collect::<Vec<_>>();
    if args.first().is_some_and(|arg| arg == "--version") {
        println!(
            "ctxmux {} (protocol {})",
            env!("CARGO_PKG_VERSION"),
            PROTOCOL_VERSION
        );
        return Ok(());
    }

    let socket = take_socket(&mut args)?;
    let command = take_string(&mut args, "command")?;
    let client = Client::new(socket);
    match command.as_str() {
        "ping" => {
            ensure_empty(&args)?;
            client.ping().await.map_err(|error| error.to_string())?;
            println!("ok");
        }
        "start" => start(&client, args).await?,
        "tmux-list" => tmux_list(&client, args).await?,
        "tmux-import" => tmux_import(&client, args).await?,
        "fork" => fork(&client, args).await?,
        "list" => {
            ensure_empty(&args)?;
            for run in client.list().await.map_err(|error| error.to_string())? {
                print_run(&run);
            }
        }
        "status" => {
            let id = take_run_id(&mut args)?;
            ensure_empty(&args)?;
            let run = client.status(id).await.map_err(|error| error.to_string())?;
            print_run(&run);
        }
        "input" => input(&client, args).await?,
        "resize" => resize(&client, args).await?,
        "attach" => attach(&client, args).await?,
        "stop" => {
            let id = take_run_id(&mut args)?;
            ensure_empty(&args)?;
            let run = client.stop(id).await.map_err(|error| error.to_string())?;
            print_run(&run);
        }
        _ => return Err(format!("unknown command {command:?}\n\n{}", usage())),
    }
    Ok(())
}

fn take_socket(args: &mut Vec<OsString>) -> Result<PathBuf, String> {
    if args.first().is_some_and(|arg| arg == "--socket") {
        args.remove(0);
        let value = args
            .first()
            .cloned()
            .ok_or_else(|| format!("--socket requires a path\n\n{}", usage()))?;
        args.remove(0);
        return Ok(PathBuf::from(value));
    }
    env::var_os("CTXMUX_SOCKET")
        .map(PathBuf::from)
        .ok_or_else(|| format!("--socket or CTXMUX_SOCKET is required\n\n{}", usage()))
}

async fn start(client: &Client, mut args: Vec<OsString>) -> Result<(), String> {
    let mut cwd = env::current_dir()
        .map_err(|error| format!("failed to read current directory: {error}"))?
        .to_string_lossy()
        .into_owned();
    let mut size = TerminalSize::default();
    let mut operation_key = None;
    while let Some(flag) = args.first() {
        if flag == "--" {
            args.remove(0);
            break;
        }
        match flag.to_str() {
            Some("--operation-key") => {
                args.remove(0);
                set_operation_key(&mut operation_key, &mut args)?;
            }
            Some("--cwd") => {
                args.remove(0);
                cwd = take_string(&mut args, "working directory")?;
            }
            Some("--cols") => {
                args.remove(0);
                size.cols = take_number(&mut args, "columns")?;
            }
            Some("--rows") => {
                args.remove(0);
                size.rows = take_number(&mut args, "rows")?;
            }
            _ => break,
        }
    }
    let program = take_string(&mut args, "program")?;
    let command_args = args
        .into_iter()
        .map(|value| os_string(value, "program argument"))
        .collect::<Result<Vec<_>, _>>()?;
    let run = client
        .start_with_operation_key(
            RunSpec {
                program,
                args: command_args,
                cwd: Some(cwd),
                env: BTreeMap::default(),
                size,
                declared_inputs: Vec::new(),
            },
            operation_key.unwrap_or_else(CreateOperationKey::random),
        )
        .await
        .map_err(|error| error.to_string())?;
    println!("{}", run.id);
    Ok(())
}

async fn fork(client: &Client, mut args: Vec<OsString>) -> Result<(), String> {
    let mut operation_key = None;
    if args
        .first()
        .is_some_and(|argument| argument == "--operation-key")
    {
        args.remove(0);
        set_operation_key(&mut operation_key, &mut args)?;
    }
    let parent = take_run_id(&mut args)?;
    ensure_empty(&args)?;
    let run = client
        .fork_with_operation_key(
            parent,
            ForkPlan::LevelA,
            operation_key.unwrap_or_else(CreateOperationKey::random),
        )
        .await
        .map_err(|error| error.to_string())?;
    print_run(&run);
    Ok(())
}

fn set_operation_key(
    operation_key: &mut Option<CreateOperationKey>,
    args: &mut Vec<OsString>,
) -> Result<(), String> {
    if operation_key.is_some() {
        return Err("--operation-key may be supplied only once".to_owned());
    }
    let value = take_string(args, "Run creation operation key")?;
    *operation_key = Some(CreateOperationKey::new(value).map_err(|error| error.to_string())?);
    Ok(())
}

async fn tmux_list(client: &Client, mut args: Vec<OsString>) -> Result<(), String> {
    let socket = take_string(&mut args, "tmux socket")?;
    ensure_empty(&args)?;
    let (version, panes) = client
        .discover_tmux(socket)
        .await
        .map_err(|error| error.to_string())?;
    for pane in panes {
        println!(
            "{}\tsession={}\twindow={}\tpid={}\tsize={}x{}\ttmux={}",
            pane.pane_id,
            pane.session_id,
            pane.window_id,
            pane.pane_pid,
            pane.size.cols,
            pane.size.rows,
            version
        );
    }
    Ok(())
}

async fn tmux_import(client: &Client, mut args: Vec<OsString>) -> Result<(), String> {
    let socket = take_string(&mut args, "tmux socket")?;
    let pane_id = take_string(&mut args, "tmux pane id")?;
    ensure_empty(&args)?;
    let run = client
        .import_tmux(socket, pane_id)
        .await
        .map_err(|error| error.to_string())?;
    print_run(&run);
    Ok(())
}

async fn input(client: &Client, mut args: Vec<OsString>) -> Result<(), String> {
    let id = take_run_id(&mut args)?;
    let data = if args.first().is_some_and(|arg| arg == "--stdin") {
        args.remove(0);
        ensure_empty(&args)?;
        let mut data = Vec::new();
        io::stdin()
            .read_to_end(&mut data)
            .map_err(|error| format!("failed to read stdin: {error}"))?;
        data
    } else {
        let data = take_string(&mut args, "input text")?.into_bytes();
        ensure_empty(&args)?;
        data
    };
    client
        .input(id, data)
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

async fn resize(client: &Client, mut args: Vec<OsString>) -> Result<(), String> {
    let id = take_run_id(&mut args)?;
    let cols = take_number(&mut args, "columns")?;
    let rows = take_number(&mut args, "rows")?;
    ensure_empty(&args)?;
    client
        .resize(id, TerminalSize { cols, rows })
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

async fn attach(client: &Client, mut args: Vec<OsString>) -> Result<(), String> {
    let id = take_run_id(&mut args)?;
    let after_seq = if args.is_empty() {
        0
    } else {
        take_number(&mut args, "output sequence")?
    };
    ensure_empty(&args)?;
    let (mut attachment, snapshot) = client
        .attach(id, after_seq)
        .await
        .map_err(|error| error.to_string())?;
    if snapshot.replay.truncated {
        eprintln!(
            "ctxmux: output before sequence {} is no longer retained",
            snapshot.replay.oldest_seq
        );
    }
    let mut stdout = io::stdout().lock();
    stdout
        .write_all(&replay_bytes(&snapshot.replay.chunks))
        .and_then(|()| stdout.flush())
        .map_err(|error| format!("failed to write output: {error}"))?;
    if !snapshot.run.state.is_running() {
        return Ok(());
    }

    let interactive = io::stdin().is_terminal() && io::stdout().is_terminal();
    if !interactive {
        return follow_output(&mut attachment, &mut stdout).await;
    }

    let input_enabled = snapshot.run.capabilities.input;
    let initial_size = if snapshot.run.capabilities.resize {
        let size = current_terminal_size(
            snapshot
                .run
                .spec
                .as_ref()
                .map_or(TerminalSize::default(), |spec| spec.size),
        )?;
        attachment
            .resize(size)
            .await
            .map_err(|error| error.to_string())?;
        Some(size)
    } else {
        None
    };
    let _raw_mode = RawModeGuard::enable()?;
    let (input_tx, mut input_rx) = mpsc::channel(16);
    thread::Builder::new()
        .name("ctxmux-terminal-input".to_owned())
        .spawn(move || read_terminal_input(&input_tx))
        .map_err(|error| format!("failed to start terminal input: {error}"))?;
    let mut resize_signal = initial_size
        .map(|_| signal(SignalKind::window_change()))
        .transpose()
        .map_err(|error| format!("failed to watch terminal resize: {error}"))?;

    loop {
        tokio::select! {
            event = attachment.next_event() => {
                let Some(event) = event.map_err(|error| error.to_string())? else {
                    return Ok(());
                };
                if !write_event(event, &mut stdout)? {
                    return Ok(());
                }
            }
            input = input_rx.recv() => {
                match input {
                    Some(TerminalInput::Data(data)) if input_enabled => attachment
                        .input(data)
                        .await
                        .map_err(|error| error.to_string())?,
                    Some(TerminalInput::Data(_)) => {}
                    Some(TerminalInput::Detach | TerminalInput::Closed) | None => {
                        return attachment.detach().await.map_err(|error| error.to_string());
                    }
                    Some(TerminalInput::Error(error)) => return Err(error),
                }
            }
            resized = async {
                match &mut resize_signal {
                    Some(signal) => signal.recv().await,
                    None => std::future::pending().await,
                }
            } => {
                if resized.is_none() {
                    return Err("terminal resize signal stream closed".to_owned());
                }
                let size = current_terminal_size(
                    initial_size.expect("resize signal exists only with an initial size"),
                )?;
                attachment
                    .resize(size)
                    .await
                    .map_err(|error| error.to_string())?;
            }
        }
    }
}

async fn follow_output(
    attachment: &mut ctxmux_client::Attachment,
    stdout: &mut io::StdoutLock<'_>,
) -> Result<(), String> {
    while let Some(event) = attachment
        .next_event()
        .await
        .map_err(|error| error.to_string())?
    {
        if !write_event(event, stdout)? {
            return Ok(());
        }
    }
    Ok(())
}

fn write_event(event: RunEvent, stdout: &mut impl Write) -> Result<bool, String> {
    match event {
        RunEvent::Output { chunk } => {
            stdout
                .write_all(&chunk.data)
                .and_then(|()| stdout.flush())
                .map_err(|error| format!("failed to write output: {error}"))?;
            Ok(true)
        }
        RunEvent::Exited { .. } | RunEvent::Interrupted { .. } => Ok(false),
        RunEvent::Tmux { .. } | RunEvent::Accepted { .. } => Ok(true),
        RunEvent::Gap { head_seq } => Err(format!(
            "attachment fell behind at output sequence {head_seq}; reattach from the last observed sequence"
        )),
    }
}

fn current_terminal_size(fallback: TerminalSize) -> Result<TerminalSize, String> {
    let (cols, rows) =
        terminal_size().map_err(|error| format!("failed to read terminal size: {error}"))?;
    Ok(normalize_terminal_size(cols, rows, fallback))
}

const fn normalize_terminal_size(cols: u16, rows: u16, fallback: TerminalSize) -> TerminalSize {
    if cols == 0 || rows == 0 {
        fallback
    } else {
        TerminalSize { cols, rows }
    }
}

enum TerminalInput {
    Data(Vec<u8>),
    Detach,
    Closed,
    Error(String),
}

fn read_terminal_input(sender: &mpsc::Sender<TerminalInput>) {
    let mut stdin = io::stdin().lock();
    let mut buffer = [0; 1024];
    let mut router = PrefixRouter::default();
    loop {
        match stdin.read(&mut buffer) {
            Ok(0) => {
                if let Some(data) = router.finish()
                    && sender.blocking_send(TerminalInput::Data(data)).is_err()
                {
                    return;
                }
                let _ = sender.blocking_send(TerminalInput::Closed);
                return;
            }
            Ok(read) => {
                let (data, detach) = router.route(&buffer[..read]);
                if !data.is_empty() && sender.blocking_send(TerminalInput::Data(data)).is_err() {
                    return;
                }
                if detach {
                    let _ = sender.blocking_send(TerminalInput::Detach);
                    return;
                }
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => {
                let _ = sender.blocking_send(TerminalInput::Error(format!(
                    "failed to read terminal input: {error}"
                )));
                return;
            }
        }
    }
}

#[derive(Default)]
struct PrefixRouter {
    prefix: bool,
}

impl PrefixRouter {
    fn route(&mut self, input: &[u8]) -> (Vec<u8>, bool) {
        let mut output = Vec::with_capacity(input.len());
        for &byte in input {
            if self.prefix {
                self.prefix = false;
                if byte == b'd' {
                    return (output, true);
                }
                output.extend_from_slice(&[0x02, byte]);
            } else if byte == 0x02 {
                self.prefix = true;
            } else {
                output.push(byte);
            }
        }
        (output, false)
    }

    fn finish(&mut self) -> Option<Vec<u8>> {
        self.prefix.then(|| {
            self.prefix = false;
            vec![0x02]
        })
    }
}

struct RawModeGuard;

impl RawModeGuard {
    fn enable() -> Result<Self, String> {
        enable_raw_mode()
            .map_err(|error| format!("failed to enable terminal raw mode: {error}"))?;
        Ok(Self)
    }
}

impl Drop for RawModeGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
    }
}

fn take_run_id(args: &mut Vec<OsString>) -> Result<RunId, String> {
    let value = take_string(args, "Run id")?;
    RunId::from_str(&value).map_err(|error| format!("invalid Run id {value:?}: {error}"))
}

fn take_number<T>(args: &mut Vec<OsString>, label: &str) -> Result<T, String>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    let value = take_string(args, label)?;
    value
        .parse()
        .map_err(|error| format!("invalid {label} {value:?}: {error}"))
}

fn take_string(args: &mut Vec<OsString>, label: &str) -> Result<String, String> {
    let value = args
        .first()
        .cloned()
        .ok_or_else(|| format!("missing {label}\n\n{}", usage()))?;
    args.remove(0);
    os_string(value, label)
}

fn os_string(value: OsString, label: &str) -> Result<String, String> {
    value
        .into_string()
        .map_err(|_| format!("{label} must be valid UTF-8"))
}

fn ensure_empty(args: &[OsString]) -> Result<(), String> {
    if args.is_empty() {
        Ok(())
    } else {
        Err(format!("unexpected arguments: {args:?}\n\n{}", usage()))
    }
}

fn print_run(run: &RunInfo) {
    let state = match &run.state {
        RunState::Running => "running".to_owned(),
        RunState::Exited { code, signal } => match signal {
            Some(signal) => format!("exited({code}, {signal})"),
            None => format!("exited({code})"),
        },
        RunState::Interrupted { reason } => format!("interrupted({reason:?})"),
    };
    let lineage = run.lineage.as_ref().map_or_else(
        || "root".to_owned(),
        |lineage| {
            let fidelity = match lineage.fidelity {
                ForkFidelity::LevelA => "level_a",
                ForkFidelity::LevelB => "level_b",
            };
            format!("{}:{fidelity}", lineage.parent)
        },
    );
    let backend = match &run.backend {
        ctxmux_protocol::RunBackend::Native => "native".to_owned(),
        ctxmux_protocol::RunBackend::Tmux { pane_id, .. } => format!("tmux:{pane_id}"),
    };
    println!(
        "{}\t{}\tpid={}\tbackend={}\tlineage={}\tattachments={}\thead={}\tdurable_head={}",
        run.id,
        state,
        run.pid
            .map_or_else(|| "unknown".to_owned(), |pid| pid.to_string()),
        backend,
        lineage,
        run.attachments,
        run.head_seq,
        run.durable_head_seq
            .map_or_else(|| "memory-only".to_owned(), |seq| seq.to_string())
    );
}

#[cfg(test)]
mod tests {
    use ctxmux_protocol::TerminalSize;

    use super::{PrefixRouter, normalize_terminal_size};

    fn route_with_partitions(input: &[u8], boundary_mask: usize) -> (Vec<u8>, bool) {
        let mut router = PrefixRouter::default();
        let mut output = Vec::new();
        let mut start = 0;
        let mut detached = false;

        for boundary in 1..input.len() {
            if boundary_mask & (1 << (boundary - 1)) == 0 {
                continue;
            }
            let (data, detach) = router.route(&input[start..boundary]);
            output.extend(data);
            if detach {
                detached = true;
                return (output, detached);
            }
            start = boundary;
        }

        let (data, detach) = router.route(&input[start..]);
        output.extend(data);
        detached |= detach;
        if !detached && let Some(data) = router.finish() {
            output.extend(data);
        }
        (output, detached)
    }

    fn assert_all_partitions(input: &[u8], expected: (&[u8], bool)) {
        let partition_count = 1usize << input.len().saturating_sub(1);
        for boundary_mask in 0..partition_count {
            let actual = route_with_partitions(input, boundary_mask);
            assert_eq!(
                actual,
                (expected.0.to_vec(), expected.1),
                "partition mask {boundary_mask:#b} changed routing for {input:?}"
            );
        }
    }

    #[test]
    fn terminal_prefix_detaches_without_forwarding_the_control_sequence() {
        let mut router = PrefixRouter::default();
        assert_eq!(router.route(&[b'a', 0x02]), (vec![b'a'], false));
        assert_eq!(router.route(b"d"), (Vec::new(), true));
    }

    #[test]
    fn terminal_prefix_forwards_non_detach_sequences_losslessly() {
        let mut router = PrefixRouter::default();
        assert_eq!(router.route(&[0x02, b'x']), (vec![0x02, b'x'], false));
        assert_eq!(router.finish(), None);
    }

    #[test]
    fn terminal_prefix_routing_is_identical_across_every_read_partition_and_eof() {
        // CLI-03: the complete partition set is small enough to enumerate;
        // no randomized property framework or OS read timing is needed.
        assert_all_partitions(b"plain", (b"plain", false));
        assert_all_partitions(
            &[b'a', 0x02, b'x', b'z'],
            (&[b'a', 0x02, b'x', b'z'], false),
        );
        assert_all_partitions(&[b'a', 0x02], (&[b'a', 0x02], false));
        assert_all_partitions(&[b'a', 0x02, b'd'], (b"a", true));
        assert_all_partitions(&[b'a', 0x02, b'd', b'z'], (b"a", true));
        assert_all_partitions(&[0x02, 0x02], (&[0x02, 0x02], false));

        let mut router = PrefixRouter::default();
        assert_eq!(router.route(&[0x02]), (Vec::new(), false));
        assert_eq!(router.finish(), Some(vec![0x02]));
        assert_eq!(router.finish(), None);
    }

    #[test]
    fn zero_sized_client_terminal_keeps_the_run_size() {
        let fallback = TerminalSize { cols: 80, rows: 24 };
        assert_eq!(normalize_terminal_size(0, 0, fallback), fallback);
        assert_eq!(
            normalize_terminal_size(120, 40, fallback),
            TerminalSize {
                cols: 120,
                rows: 40,
            }
        );
    }
}
