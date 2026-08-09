use std::{env, os::fd::RawFd, path::PathBuf, process::ExitCode};

use ctxmux_protocol::PROTOCOL_VERSION;

fn usage() -> &'static str {
    "usage: ctxmuxd --socket <path> [--state-dir <path>]\n       ctxmuxd --version"
}

fn main() -> ExitCode {
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
    let mut qualification_stats_fd = None;
    while let Some(flag) = args.next() {
        if flag == "--qualification-stats-fd" {
            if qualification_stats_fd.is_some() {
                eprintln!("{}", usage());
                return ExitCode::from(2);
            }
            let Some(value) = args.next() else {
                eprintln!("{}", usage());
                return ExitCode::from(2);
            };
            let Ok(value) = value.to_string_lossy().parse::<RawFd>() else {
                eprintln!("{}", usage());
                return ExitCode::from(2);
            };
            qualification_stats_fd = Some(value);
            continue;
        }
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
    let qualification_stats_fd = match qualification_stats_fd {
        Some(raw_fd) => match ctxmux_inherited_fd::duplicate_nonblocking_cloexec(raw_fd) {
            Ok(owned) => Some(owned),
            Err(error) => {
                eprintln!("ctxmuxd: invalid qualification stats fd: {error}");
                return ExitCode::from(2);
            }
        },
        None => None,
    };
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("ctxmuxd: failed to create runtime: {error}");
            return ExitCode::FAILURE;
        }
    };
    let result = runtime.block_on(async move {
        match state_dir {
            Some(state_dir) => {
                ctxmux_daemon::serve_with_state_dir_and_qualification(
                    socket,
                    state_dir,
                    qualification_stats_fd,
                )
                .await
            }
            None => ctxmux_daemon::serve_with_qualification(socket, qualification_stats_fd).await,
        }
    });
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("ctxmuxd: {error}");
            ExitCode::FAILURE
        }
    }
}
