use std::{
    env,
    os::fd::{OwnedFd, RawFd},
    path::PathBuf,
    process::ExitCode,
};

use ctxmux_protocol::PROTOCOL_VERSION;

fn usage() -> &'static str {
    "usage: ctxmuxd --socket <path> [--state-dir <path>] [--readiness-fd <fd>]\n       ctxmuxd --version"
}

fn inherited_fd(raw_fd: Option<RawFd>, label: &str) -> Result<Option<OwnedFd>, ExitCode> {
    raw_fd
        .map(|raw_fd| {
            ctxmux_inherited_fd::duplicate_nonblocking_cloexec(raw_fd).map_err(|error| {
                eprintln!("ctxmuxd: invalid {label} fd: {error}");
                ExitCode::from(2)
            })
        })
        .transpose()
}

async fn serve(
    socket: PathBuf,
    state_dir: Option<PathBuf>,
    qualification_stats_fd: Option<OwnedFd>,
    readiness_fd: Option<OwnedFd>,
) -> Result<(), ctxmux_daemon::ServerError> {
    match state_dir {
        Some(state_dir) => {
            ctxmux_daemon::serve_with_state_dir_and_inherited_descriptors(
                socket,
                state_dir,
                qualification_stats_fd,
                readiness_fd,
            )
            .await
        }
        None => {
            ctxmux_daemon::serve_with_inherited_descriptors(
                socket,
                qualification_stats_fd,
                readiness_fd,
            )
            .await
        }
    }
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
    let mut readiness_fd = None;
    while let Some(flag) = args.next() {
        if flag == "--qualification-stats-fd" || flag == "--readiness-fd" {
            let target = if flag == "--qualification-stats-fd" {
                &mut qualification_stats_fd
            } else {
                &mut readiness_fd
            };
            if target.is_some() {
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
            *target = Some(value);
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
    if readiness_fd.is_some() && readiness_fd == qualification_stats_fd {
        eprintln!("ctxmuxd: readiness and qualification descriptors must be distinct");
        return ExitCode::from(2);
    }
    let qualification_stats_fd = match inherited_fd(qualification_stats_fd, "qualification stats") {
        Ok(fd) => fd,
        Err(exit) => return exit,
    };
    let readiness_fd = match inherited_fd(readiness_fd, "readiness") {
        Ok(fd) => fd,
        Err(exit) => return exit,
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
    let result = runtime.block_on(serve(
        socket,
        state_dir,
        qualification_stats_fd,
        readiness_fd,
    ));
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("ctxmuxd: {error}");
            ExitCode::FAILURE
        }
    }
}
