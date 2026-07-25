use std::{env, path::PathBuf, process::ExitCode};

use ctxmux_protocol::PROTOCOL_VERSION;

fn usage() -> &'static str {
    "usage: ctxmuxd --socket <path> [--state-dir <path>]\n       ctxmuxd --version"
}

#[tokio::main]
async fn main() -> ExitCode {
    let mut args = env::args_os().skip(1).peekable();
    if args.peek().is_some_and(|value| value == "--version") {
        args.next();
        if args.next().is_none() {
            println!(
                "ctxmuxd {} (protocol {})",
                env!("CARGO_PKG_VERSION"),
                PROTOCOL_VERSION
            );
            return ExitCode::SUCCESS;
        }
        eprintln!("{}", usage());
        return ExitCode::from(2);
    }

    let mut socket = None;
    let mut state_dir = None;
    while let Some(flag) = args.next() {
        let target = if flag == "--socket" {
            &mut socket
        } else if flag == "--state-dir" {
            &mut state_dir
        } else {
            eprintln!("{}", usage());
            return ExitCode::from(2);
        };
        if target.is_some() {
            eprintln!("{}", usage());
            return ExitCode::from(2);
        }
        let Some(path) = args.next() else {
            eprintln!("{}", usage());
            return ExitCode::from(2);
        };
        *target = Some(PathBuf::from(path));
    }
    let Some(socket) = socket else {
        eprintln!("{}", usage());
        return ExitCode::from(2);
    };
    let result = match state_dir {
        Some(state_dir) => ctxmux_daemon::serve_with_state_dir(socket, state_dir).await,
        None => ctxmux_daemon::serve(socket).await,
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("ctxmuxd: {error}");
            ExitCode::FAILURE
        }
    }
}
