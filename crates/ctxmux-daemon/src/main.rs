use std::{
    env,
    os::fd::{OwnedFd, RawFd},
    path::PathBuf,
    process::ExitCode,
};

use ctxmux_protocol::PROTOCOL_VERSION;

fn usage() -> &'static str {
    "usage: ctxmuxd --socket <path> [--state-dir <path>] [--readiness-fd <fd>] [--handoff-fd <fd>]\n       ctxmuxd --version"
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

struct InheritedDescriptors {
    qualification_stats: Option<OwnedFd>,
    readiness: Option<OwnedFd>,
    handoff: Option<OwnedFd>,
}

/// Validate the inherited descriptor numbers and claim owning handles.
///
/// The three descriptors must be pairwise distinct so none is double-owned or
/// double-closed, and `--handoff-fd` requires a state directory to reconcile
/// the carried runs against. On any violation the error is printed and the
/// process exit code returned.
fn resolve_inherited_descriptors(
    qualification_stats_fd: Option<RawFd>,
    readiness_fd: Option<RawFd>,
    handoff_fd: Option<RawFd>,
    state_dir: Option<&PathBuf>,
) -> Result<InheritedDescriptors, ExitCode> {
    let raw = [qualification_stats_fd, readiness_fd, handoff_fd];
    for (index, left) in raw.iter().enumerate() {
        let Some(left) = left else { continue };
        if raw[index + 1..].contains(&Some(*left)) {
            eprintln!("ctxmuxd: inherited descriptors must be distinct");
            return Err(ExitCode::from(2));
        }
    }
    if handoff_fd.is_some() && state_dir.is_none() {
        eprintln!("ctxmuxd: --handoff-fd requires --state-dir");
        return Err(ExitCode::from(2));
    }
    Ok(InheritedDescriptors {
        qualification_stats: inherited_fd(qualification_stats_fd, "qualification stats")?,
        readiness: inherited_fd(readiness_fd, "readiness")?,
        handoff: handoff_fd
            .map(|raw| {
                ctxmux_inherited_fd::claim_inherited_process_fd(raw).map_err(|error| {
                    eprintln!("ctxmuxd: invalid handoff fd: {error}");
                    ExitCode::from(2)
                })
            })
            .transpose()?,
    })
}

async fn serve(
    socket: PathBuf,
    state_dir: Option<PathBuf>,
    qualification_stats_fd: Option<OwnedFd>,
    readiness_fd: Option<OwnedFd>,
    handoff_fd: Option<OwnedFd>,
) -> Result<(), ctxmux_daemon::ServerError> {
    if let Some(state_dir) = state_dir {
        ctxmux_daemon::serve_with_state_dir_and_inherited_descriptors(
            socket,
            state_dir,
            qualification_stats_fd,
            readiness_fd,
            handoff_fd,
        )
        .await
    } else {
        debug_assert!(handoff_fd.is_none());
        ctxmux_daemon::serve_with_inherited_descriptors(
            socket,
            qualification_stats_fd,
            readiness_fd,
        )
        .await
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
    let mut handoff_fd = None;
    while let Some(flag) = args.next() {
        if flag == "--qualification-stats-fd" || flag == "--readiness-fd" || flag == "--handoff-fd"
        {
            let target = if flag == "--qualification-stats-fd" {
                &mut qualification_stats_fd
            } else if flag == "--readiness-fd" {
                &mut readiness_fd
            } else {
                &mut handoff_fd
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
    let descriptors = match resolve_inherited_descriptors(
        qualification_stats_fd,
        readiness_fd,
        handoff_fd,
        state_dir.as_ref(),
    ) {
        Ok(descriptors) => descriptors,
        Err(exit) => return exit,
    };
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
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
        descriptors.qualification_stats,
        descriptors.readiness,
        descriptors.handoff,
    ));
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("ctxmuxd: {error}");
            ExitCode::FAILURE
        }
    }
}
