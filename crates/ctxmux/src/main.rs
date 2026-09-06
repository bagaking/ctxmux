use std::{
    collections::BTreeMap,
    env,
    ffi::OsString,
    future::Future,
    io::{self, IsTerminal, Read, Write},
    path::PathBuf,
    pin::Pin,
    process::ExitCode,
    str::FromStr,
    thread,
};

use crossterm::terminal::{disable_raw_mode, enable_raw_mode, size as terminal_size};
use ctxmux_client::{Client, replay_bytes};
use ctxmux_protocol::{
    CreateOperationKey, DaemonInstanceId, ForkFidelity, ForkPlan, PROTOCOL_VERSION,
    RecoverableStop, RunBackendKind, RunEvent, RunId, RunInfo, RunSpec, RunState, RunSummary,
    StopDisposition, StopOperationKey, TerminalContinuation, TerminalSize,
};
use tokio::{
    signal::unix::{SignalKind, signal},
    sync::{mpsc, oneshot},
};

mod daemon;

fn usage() -> &'static str {
    "ctxmux — context-aware local Run multiplexer

usage:
  ctxmux --version
  ctxmux [--socket <path>] ping
  ctxmux [--socket <path>] runtime
  ctxmux [--socket <path>] diagnostics
  ctxmux [--socket <path>] start [--operation-key <key>] [--cwd <path>] [--cols <n>] [--rows <n>] -- <program> [args...]
  ctxmux [--socket <path>] tmux-list <tmux-socket>
  ctxmux [--socket <path>] tmux-import <tmux-socket> <pane-id>
  ctxmux [--socket <path>] fork [--operation-key <key>] <run-id>
  ctxmux [--socket <path>] list
  ctxmux [--socket <path>] status <run-id>
  ctxmux [--socket <path>] remove <run-id>
  ctxmux [--socket <path>] input <run-id> <text>
  ctxmux [--socket <path>] input <run-id> --stdin
  ctxmux [--socket <path>] resize <run-id> <cols> <rows>
  ctxmux [--socket <path>] interrupt <run-id>
  ctxmux [--socket <path>] attach <run-id> [after-byte]
  ctxmux [--socket <path>] stop [--daemon-instance <uuid> --operation-key <key>] <run-id>

CTXMUX_SOCKET may be used instead of --socket. When neither is set, ctxmux uses
$XDG_RUNTIME_DIR/ctxmux/ctxmux.sock or a process-temp path, and starts ctxmuxd
if nothing is listening."
}

// One socket round trip per invocation, and `attach` multiplexes with
// `select!` rather than `tokio::spawn`. A worker pool buys nothing and is
// sized by host CPUs: on a 64-core host the default flavor costs 4.65 ms of
// thread spawning against 0.90 ms here, paid four times over a churn cycle.
#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            let _ = writeln!(io::stderr().lock(), "ctxmux: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), String> {
    let mut args = env::args_os().skip(1).collect::<Vec<_>>();
    if args.first().is_some_and(|arg| arg == "--version") {
        print_stdout(format_args!(
            "ctxmux {} (protocol {})",
            env!("CARGO_PKG_VERSION"),
            PROTOCOL_VERSION
        ))?;
        return Ok(());
    }

    let socket = take_socket(&mut args)?;
    let command = take_string(&mut args, "command")?;
    if !matches!(
        command.as_str(),
        "ping"
            | "runtime"
            | "diagnostics"
            | "start"
            | "tmux-list"
            | "tmux-import"
            | "fork"
            | "list"
            | "status"
            | "remove"
            | "input"
            | "resize"
            | "interrupt"
            | "attach"
            | "stop"
    ) {
        return Err(format!("unknown command {command:?}\n\n{}", usage()));
    }
    daemon::ensure_listening(&socket).await?;
    let client = Client::new(socket);
    dispatch_command(&client, &command, args).await
}

async fn dispatch_command(
    client: &Client,
    command: &str,
    mut args: Vec<OsString>,
) -> Result<(), String> {
    match command {
        "ping" => {
            ensure_empty(&args)?;
            client.ping().await.map_err(|error| error.to_string())?;
            print_stdout(format_args!("ok"))?;
        }
        "runtime" => {
            ensure_empty(&args)?;
            let runtime = client
                .runtime_info()
                .await
                .map_err(|error| error.to_string())?;
            print_stdout(format_args!(
                "{}",
                serde_json::to_string(&runtime)
                    .map_err(|error| format!("failed to encode Runtime identity: {error}"))?
            ))?;
        }
        "diagnostics" => {
            ensure_empty(&args)?;
            let diagnostics = client
                .diagnostics()
                .await
                .map_err(|error| error.to_string())?;
            print_stdout(format_args!(
                "{}",
                serde_json::to_string(&diagnostics)
                    .map_err(|error| format!("failed to encode diagnostics: {error}"))?
            ))?;
        }
        "start" => start(client, args).await?,
        "tmux-list" => tmux_list(client, args).await?,
        "tmux-import" => tmux_import(client, args).await?,
        "fork" => fork(client, args).await?,
        "list" => {
            ensure_empty(&args)?;
            // The client pages internally, so the CLI keeps its whole-fleet
            // listing without knowing about cursors.
            for run in client.list().await.map_err(|error| error.to_string())? {
                if !print_summary(&run)? {
                    break;
                }
            }
        }
        "status" => {
            let id = take_run_id(&mut args)?;
            ensure_empty(&args)?;
            let run = client.status(id).await.map_err(|error| error.to_string())?;
            print_run(&run)?;
        }
        "remove" => {
            let id = take_run_id(&mut args)?;
            ensure_empty(&args)?;
            client.remove(id).await.map_err(|error| error.to_string())?;
            print_stdout(format_args!("{id}\tremoved"))?;
        }
        "input" => input(client, args).await?,
        "resize" => resize(client, args).await?,
        "interrupt" => {
            let id = take_run_id(&mut args)?;
            ensure_empty(&args)?;
            let accepted = client
                .interrupt(id)
                .await
                .map_err(|error| error.to_string())?;
            print_run(&accepted.run)?;
        }
        "attach" => attach(client, args).await?,
        "stop" => stop(client, args).await?,
        _ => unreachable!("known commands are enumerated before connect-or-spawn"),
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
    Ok(env::var_os("CTXMUX_SOCKET").map_or_else(daemon::default_socket_path, PathBuf::from))
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
                initial_size: size,
                declared_inputs: Vec::new(),
            },
            operation_key.unwrap_or_else(CreateOperationKey::random),
        )
        .await
        .map_err(|error| error.to_string())?;
    print_stdout(format_args!("{}", run.id))?;
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
    print_run(&run)?;
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
        if !print_stdout(format_args!(
            "{}\tsession={}\twindow={}\tpid={}\tsize={}x{}\ttmux={}",
            pane.pane_id,
            pane.session_id,
            pane.window_id,
            pane.pane_pid,
            pane.size.cols,
            pane.size.rows,
            version
        ))? {
            break;
        }
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
    print_run(&run)?;
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

async fn stop(client: &Client, mut args: Vec<OsString>) -> Result<(), String> {
    let mut daemon_instance = None;
    let mut operation_key = None;
    while let Some(flag) = args.first().and_then(|value| value.to_str()) {
        match flag {
            "--daemon-instance" => {
                args.remove(0);
                if daemon_instance.is_some() {
                    return Err("--daemon-instance may be supplied only once".to_owned());
                }
                daemon_instance = Some(
                    take_string(&mut args, "daemon instance")?
                        .parse::<DaemonInstanceId>()
                        .map_err(|error| format!("invalid daemon instance: {error}"))?,
                );
            }
            "--operation-key" => {
                args.remove(0);
                if operation_key.is_some() {
                    return Err("--operation-key may be supplied only once".to_owned());
                }
                operation_key = Some(
                    take_string(&mut args, "Stop operation key")?
                        .parse::<StopOperationKey>()
                        .map_err(|error| error.to_string())?,
                );
            }
            _ => break,
        }
    }
    let id = take_run_id(&mut args)?;
    ensure_empty(&args)?;
    let accepted = match (daemon_instance, operation_key) {
        // No retained key to honour: the incarnation can be read off the Stop's
        // own connection instead of spending a round trip to fetch it first.
        (None, None) => client.stop_once(id).await,
        (Some(daemon_instance), Some(operation_key)) => {
            client
                .stop(RecoverableStop {
                    daemon_instance,
                    operation_key,
                    id,
                })
                .await
        }
        _ => {
            return Err(
                "--daemon-instance and --operation-key must be supplied together".to_owned(),
            );
        }
    }
    .map_err(|error| error.to_string())?;
    let disposition = match accepted.receipt.disposition {
        StopDisposition::Graceful => "graceful",
        StopDisposition::Forced => "forced",
    };
    if !print_stdout(format_args!("stop={disposition}"))? {
        return Ok(());
    }
    print_run(&accepted.run)?;
    Ok(())
}

async fn attach(client: &Client, mut args: Vec<OsString>) -> Result<(), String> {
    let id = take_run_id(&mut args)?;
    let after_byte = if args.is_empty() {
        0
    } else {
        take_number(&mut args, "output byte cursor")?
    };
    ensure_empty(&args)?;
    let interactive = io::stdin().is_terminal() && io::stdout().is_terminal();
    let (attachment, snapshot) = attach_view(client, id, after_byte, interactive).await?;
    let mut stdout = io::stdout().lock();
    let replay = replay_bytes(&snapshot.replay.chunks);
    if !interactive || !snapshot.run.state.is_running() {
        if interactive {
            stdout
                .write_all(&snapshot.terminal_restore)
                .map_err(|error| format!("failed to write terminal state: {error}"))?;
        }
        stdout
            .write_all(&replay)
            .and_then(|()| stdout.flush())
            .map_err(|error| format!("failed to write output: {error}"))?;
        if !snapshot.run.state.is_running() {
            return Ok(());
        }
        return follow_output(&attachment, &mut stdout).await;
    }

    let _raw_mode = RawModeGuard::enable()?;
    stdout
        .write_all(&snapshot.terminal_restore)
        .and_then(|()| stdout.write_all(&replay))
        .and_then(|()| stdout.flush())
        .map_err(|error| format!("failed to write output: {error}"))?;
    match follow_terminal(&attachment, &snapshot.run, &mut stdout).await? {
        TerminalExit::Detach => attachment.detach().await.map_err(|error| error.to_string()),
        TerminalExit::Ended => Ok(()),
        TerminalExit::InputUnsettled {
            may_have_been_sent,
            not_sent_bytes,
        } => {
            attachment.close();
            let mut stderr = io::stderr().lock();
            if may_have_been_sent {
                writeln!(stderr,
                    "ctxmux: closed view with an unconfirmed input result; do not replay it automatically")
                    .map_err(|error| format!("failed to report unconfirmed input: {error}"))?;
            }
            if not_sent_bytes > 0 {
                writeln!(stderr,
                    "ctxmux: {not_sent_bytes} locally buffered input bytes were not sent (not_applied)")
                    .map_err(|error| format!("failed to report unsent input: {error}"))?;
            }
            Ok(())
        }
    }
}

async fn attach_view(
    client: &Client,
    id: RunId,
    after_byte: u64,
    interactive: bool,
) -> Result<(ctxmux_client::Attachment, ctxmux_protocol::AttachedSnapshot), String> {
    let (mut attachment, mut snapshot) = if interactive {
        client.attach_terminal(id, after_byte).await
    } else {
        client.attach(id, after_byte).await
    }
    .map_err(|error| error.to_string())?;
    let mut terminal_view_requested = interactive;
    if interactive
        && let TerminalContinuation::BasicVt {
            checkpoint,
            resizes,
        } = &snapshot.terminal
    {
        let host_size = current_terminal_size(checkpoint.size)?;
        if host_size != checkpoint.size
            || checkpoint.restore_size != checkpoint.size
            || checkpoint.restore_scrollback_rows.is_some()
            || !resizes.is_empty()
        {
            eprintln!(
                "ctxmux: this physical terminal cannot restore the declared historical geometry; opening a raw view without verified prior terminal state"
            );
            attachment
                .detach()
                .await
                .map_err(|error| error.to_string())?;
            (attachment, snapshot) = client
                .attach(id, after_byte)
                .await
                .map_err(|error| error.to_string())?;
            terminal_view_requested = false;
        }
    }
    if snapshot.replay.truncated {
        let _ = writeln!(
            io::stderr().lock(),
            "ctxmux: output before byte {} is no longer retained",
            snapshot.replay.first_available_byte
        );
    }
    if interactive {
        match &snapshot.terminal {
            TerminalContinuation::BasicVt {
                checkpoint,
                resizes,
            } => {
                debug_assert_eq!(checkpoint.restore_size, checkpoint.size);
                debug_assert!(resizes.is_empty());
            }
            TerminalContinuation::Unknown { reason }
            | TerminalContinuation::Unavailable { reason } => {
                eprintln!(
                    "ctxmux: terminal continuation is {reason:?}; displaying retained raw output without verified prior terminal state"
                );
            }
            TerminalContinuation::NotRequested => {
                if terminal_view_requested {
                    return Err("terminal attachment returned raw-only state".to_owned());
                }
            }
        }
    }
    Ok((attachment, snapshot))
}

enum TerminalExit {
    Detach,
    Ended,
    InputUnsettled {
        may_have_been_sent: bool,
        not_sent_bytes: usize,
    },
}

type PendingInput<'a> = Pin<Box<dyn Future<Output = Result<(), String>> + 'a>>;

struct PendingTerminalInput<'a> {
    future: PendingInput<'a>,
    bytes: usize,
    attempted: bool,
}

fn finish_terminal_input(
    pending: Option<&PendingTerminalInput<'_>>,
    receiver: &mut mpsc::Receiver<TerminalInput>,
    empty: TerminalExit,
) -> TerminalExit {
    receiver.close();
    let may_have_been_sent = pending.is_some_and(|input| input.attempted);
    let mut not_sent_bytes = pending
        .filter(|input| !input.attempted)
        .map_or(0, |input| input.bytes);
    while let Ok(input) = receiver.try_recv() {
        if let TerminalInput::Data(bytes) = input {
            not_sent_bytes += bytes.len();
        }
    }
    if may_have_been_sent || not_sent_bytes > 0 {
        TerminalExit::InputUnsettled {
            may_have_been_sent,
            not_sent_bytes,
        }
    } else {
        empty
    }
}

async fn follow_terminal(
    attachment: &ctxmux_client::Attachment,
    run: &RunInfo,
    stdout: &mut io::StdoutLock<'_>,
) -> Result<TerminalExit, String> {
    let input_enabled = run.capabilities.input;
    let mut applied_size = apply_initial_terminal_size(attachment, run).await?;
    // Sixteen local read chunks plus one in-flight request backpressure stdin.
    // Each read is at most 1024 bytes plus a possible held prefix byte; this
    // queue does not change the daemon's accepted Run workload.
    let (input_tx, mut input_rx) = mpsc::channel(16);
    let (terminal_tx, mut terminal_rx) = oneshot::channel();
    let mut pending_input: Option<PendingTerminalInput<'_>> = None;
    thread::Builder::new()
        .name("ctxmux-terminal-input".to_owned())
        .spawn(move || read_terminal_input(&input_tx, terminal_tx))
        .map_err(|error| format!("failed to start terminal input: {error}"))?;
    let mut resize_signal = applied_size
        .map(|_| signal(SignalKind::window_change()))
        .transpose()
        .map_err(|error| format!("failed to watch terminal resize: {error}"))?;

    loop {
        tokio::select! {
            event = attachment.next_event() => {
                let Some(event) = event.map_err(|error| error.to_string())? else {
                    return Ok(finish_terminal_input(
                        pending_input.as_ref(), &mut input_rx, TerminalExit::Ended,
                    ));
                };
                if !write_event(event, stdout)? {
                    return Ok(finish_terminal_input(
                        pending_input.as_ref(), &mut input_rx, TerminalExit::Ended,
                    ));
                }
            }
            settled = async {
                match &mut pending_input {
                    Some(input) => {
                        input.attempted = true;
                        input.future.as_mut().await
                    }
                    None => std::future::pending().await,
                }
            } => {
                pending_input = None;
                settled?;
            }
            terminal = &mut terminal_rx => {
                if let Ok(TerminalInput::Error(error)) = terminal {
                    return Err(error);
                }
                return Ok(finish_terminal_input(
                    pending_input.as_ref(), &mut input_rx, TerminalExit::Detach,
                ));
            }
            input = input_rx.recv(), if pending_input.is_none() => {
                match input {
                    Some(TerminalInput::Data(data)) if input_enabled => {
                        pending_input = Some(PendingTerminalInput {
                            bytes: data.len(),
                            attempted: false,
                            future: Box::pin(async move {
                                attachment.input(data).await.map(|_| ()).map_err(|error| error.to_string())
                            }),
                        });
                    }
                    Some(TerminalInput::Data(_)) => {}
                    Some(TerminalInput::Detach | TerminalInput::Closed) | None => {
                        return Ok(TerminalExit::Detach);
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
                    applied_size.expect("resize signal exists only with an applied size"),
                )?;
                let accepted = attachment
                    .resize(size)
                    .await
                    .map_err(|error| error.to_string())?;
                applied_size = Some(accepted.receipt.applied_size);
            }
        }
    }
}

async fn apply_initial_terminal_size(
    attachment: &ctxmux_client::Attachment,
    run: &RunInfo,
) -> Result<Option<TerminalSize>, String> {
    if !run.capabilities.resize {
        return Ok(None);
    }
    let requested = current_terminal_size(
        run.spec
            .as_ref()
            .map_or(TerminalSize::default(), |spec| spec.initial_size),
    )?;
    let accepted = attachment
        .resize(requested)
        .await
        .map_err(|error| error.to_string())?;
    Ok(Some(accepted.receipt.applied_size))
}

async fn follow_output(
    attachment: &ctxmux_client::Attachment,
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
        RunEvent::ServiceChanged { service } => {
            // Diagnostics stay off the byte-exact PTY output stream. Lifecycle
            // remains independent: unavailable I/O does not manufacture Exited.
            if matches!(service.owner, ctxmux_protocol::NativeOwnerStatus::Stopped { .. })
                || matches!(service.output, ctxmux_protocol::NativeOutputStatus::Unavailable { .. })
                || matches!(service.input.phase, ctxmux_protocol::NativeInputPhase::Unavailable { .. })
                || service.terminal_fault.is_some()
            {
                let _ = writeln!(io::stderr().lock(), "ctxmux: native service {}",
                    serde_json::to_string(&service).map_err(|error| error.to_string())?);
            }
            Ok(true)
        }
        RunEvent::Resized { size, .. } => {
            // Another Client may have resized this Run. A physical terminal is
            // not a daemon-owned emulator and cannot recreate that geometry.
            if io::stdout().is_terminal() && current_terminal_size(size)? != size {
                let _ = writeln!(io::stderr().lock(), "ctxmux: Run geometry is {}x{}; local terminal geometry differs", size.cols, size.rows);
            }
            Ok(true)
        }
        RunEvent::Tmux { .. } => Ok(true),
        RunEvent::ObservationDiscontinuity => Err(
            "attachment lost one or more non-output observations; output replay cannot reconstruct their semantics"
                .to_owned(),
        ),
        RunEvent::Gap {
            latest_output_bytes,
        } => Err(format!(
            "attachment fell behind at output byte {latest_output_bytes}; reattach from the last observed byte cursor"
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

fn read_terminal_input(
    sender: &mpsc::Sender<TerminalInput>,
    terminal: oneshot::Sender<TerminalInput>,
) {
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
                let _ = terminal.send(TerminalInput::Closed);
                return;
            }
            Ok(read) => {
                let (data, detach) = router.route(&buffer[..read]);
                if !data.is_empty() && sender.blocking_send(TerminalInput::Data(data)).is_err() {
                    return;
                }
                if detach {
                    let _ = terminal.send(TerminalInput::Detach);
                    return;
                }
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => {
                let _ = terminal.send(TerminalInput::Error(format!(
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

/// Render a Run's lifecycle state as one operator-facing token.
fn format_run_state(state: &RunState) -> String {
    match state {
        RunState::Running => "running".to_owned(),
        RunState::Exited { code, signal } => match signal {
            Some(signal) => format!("exited({code}, {signal})"),
            None => format!("exited({code})"),
        },
        RunState::Interrupted { reason } => format!("interrupted({reason:?})"),
    }
}

// A closed downstream pipe is a normal consumer boundary. Propagate every
// other write failure, and unwind normally so terminal/attachment guards run.
fn print_stdout(arguments: std::fmt::Arguments<'_>) -> Result<bool, String> {
    print_line(&mut io::stdout().lock(), arguments)
}

fn print_line(writer: &mut impl Write, arguments: std::fmt::Arguments<'_>) -> Result<bool, String> {
    match writeln!(writer, "{arguments}") {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::BrokenPipe => Ok(false),
        Err(error) => Err(format!("failed to write stdout: {error}")),
    }
}

/// Print one thin `list` row.
///
/// A `list` page carries [`RunSummary`] values, not full `RunInfo`, so this
/// deliberately omits lineage and the durable head that `status` still shows —
/// those live on the fat per-Run record. It keeps the run id first so scripts
/// and the CLI smoke test can still find a Run by grepping the listing.
fn print_summary(run: &RunSummary) -> Result<bool, String> {
    let backend = match run.backend {
        RunBackendKind::Native => "native",
        RunBackendKind::Tmux => "tmux",
    };
    print_stdout(format_args!(
        "{}\t{}\tpid={}\tbackend={}\tattachments={}\thead={}\tretained={}",
        run.id,
        format_run_state(&run.state),
        run.pid
            .map_or_else(|| "unknown".to_owned(), |pid| pid.to_string()),
        backend,
        run.attachments,
        run.latest_output_bytes,
        // Distinct from `head=` on purpose. `head=` is a lifetime total that
        // only grows; `retained=` is what this Run holds right now, and summing
        // it across a listing is how an external harness checks the daemon's
        // fleet-wide retention cap without needing the daemon's own counter.
        run.retained_output_bytes,
    ))
}

fn print_run(run: &RunInfo) -> Result<bool, String> {
    let state = format_run_state(&run.state);
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
    print_stdout(format_args!(
        "{}\t{}\tpid={}\tbackend={}\tlineage={}\tattachments={}\thead={}\tdurable_head={}\tsize={}\tnative_service={}",
        run.id,
        state,
        run.pid
            .map_or_else(|| "unknown".to_owned(), |pid| pid.to_string()),
        backend,
        lineage,
        run.attachments,
        run.latest_output_bytes,
        run.durable_output_bytes
            .map_or_else(|| "memory-only".to_owned(), |seq| seq.to_string()),
        // The owner-confirmed size, not `spec.initial_size`: a Run started at 80x24 and
        // resized to 200x87 reports 200x87 here. "unknown" is a real answer --
        // no owner can confirm a tmux pane or a recovered Run -- so it is not
        // filled in from the spec.
        format_current_size(run.current_size),
        serde_json::to_string(&run.native_service).map_err(|error| error.to_string())?
    ))
}

fn format_current_size(size: Option<TerminalSize>) -> String {
    size.map_or_else(
        || "unknown".to_owned(),
        |size| format!("{}x{}", size.cols, size.rows),
    )
}

#[cfg(test)]
mod tests {
    use ctxmux_protocol::{RunEvent, TerminalSize};

    use super::{
        PrefixRouter, format_current_size, normalize_terminal_size, print_line, write_event,
    };

    #[test]
    fn stdout_pipe_closure_is_distinct_from_other_write_failures() {
        struct FailedWriter(std::io::ErrorKind);
        impl std::io::Write for FailedWriter {
            fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
                Err(std::io::Error::from(self.0))
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        assert!(
            !print_line(
                &mut FailedWriter(std::io::ErrorKind::BrokenPipe),
                format_args!("one row")
            )
            .unwrap()
        );
        assert!(
            print_line(
                &mut FailedWriter(std::io::ErrorKind::PermissionDenied),
                format_args!("one row")
            )
            .unwrap_err()
            .contains("failed to write stdout")
        );
        let mut bytes = Vec::new();
        assert!(print_line(&mut bytes, format_args!("run\thead=1")).unwrap());
        assert_eq!(bytes, b"run\thead=1\n");
    }

    #[test]
    fn status_reports_the_confirmed_size_and_admits_when_there_is_none() {
        assert_eq!(
            format_current_size(Some(TerminalSize {
                cols: 200,
                rows: 87
            })),
            "200x87"
        );
        // A Run nobody can ask -- an imported tmux pane, or one recovered
        // without a live PTY -- says so rather than echoing its requested size.
        assert_eq!(format_current_size(None), "unknown");
    }

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

    #[test]
    fn cli_fails_closed_on_non_output_observation_discontinuity() {
        let error = write_event(RunEvent::ObservationDiscontinuity, &mut Vec::new())
            .expect_err("CLI cannot repair non-output semantics with byte replay");
        assert!(error.contains("non-output observations"));
        assert!(error.contains("cannot reconstruct"));
    }
}
