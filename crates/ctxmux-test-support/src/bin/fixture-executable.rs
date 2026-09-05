//! Test-only process target. No daemon path imports this executable.
//!
//! Fresh shebang inodes can stall at interpreter loading on the test host.
//! Keep fake scripts as data while retaining the real process/probe boundary.

use std::{env, process};

fn main() {
    let args: Vec<_> = env::args_os().skip(1).collect();
    if let Some(script) = env::var_os("CTXMUX_FIXTURE_TMUX_SCRIPT") {
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            let error = process::Command::new("/bin/sh")
                .arg(script)
                .args(args)
                .exec();
            eprintln!("cannot exec fixture interpreter: {error}");
        }
        process::exit(99);
    }
    if args.len() == 1
        && args[0] == "--version"
        && let Ok(schema) = env::var("CTXMUX_FIXTURE_HANDOFF_SCHEMA")
        && !schema.is_empty()
        && !schema.contains(['\n', '\r'])
    {
        println!("ctxmuxd fixture handoff {schema}");
        return;
    }
    // An accidental upgrade exec must fail the cancellation oracle.
    process::exit(99);
}
