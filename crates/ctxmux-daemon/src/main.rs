use std::{env, path::PathBuf, process::ExitCode};

use ctxmux_protocol::PROTOCOL_VERSION;

fn usage() -> &'static str {
    "usage: ctxmuxd --socket <path>\n       ctxmuxd --version"
}

#[tokio::main]
async fn main() -> ExitCode {
    let mut args = env::args_os().skip(1);
    match args.next().as_deref() {
        Some(value) if value == "--version" => {
            println!(
                "ctxmuxd {} (protocol {})",
                env!("CARGO_PKG_VERSION"),
                PROTOCOL_VERSION
            );
            ExitCode::SUCCESS
        }
        Some(value) if value == "--socket" => {
            let Some(path) = args.next() else {
                eprintln!("{}", usage());
                return ExitCode::from(2);
            };
            if args.next().is_some() {
                eprintln!("{}", usage());
                return ExitCode::from(2);
            }
            match ctxmux_daemon::serve(PathBuf::from(path)).await {
                Ok(()) => ExitCode::SUCCESS,
                Err(error) => {
                    eprintln!("ctxmuxd: {error}");
                    ExitCode::FAILURE
                }
            }
        }
        _ => {
            eprintln!("{}", usage());
            ExitCode::from(2)
        }
    }
}
