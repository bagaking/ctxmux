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
    if args.len() == 1 && args[0] == "--pty-exit-after-release" {
        use std::io::{BufRead, Write};

        // A real exec/read/write boundary for the waitable-zombie fixture.
        // Publish readiness only after terminal setup; the parent then releases
        // this owned process without a shell/interpreter loading dependency.
        let mut output = std::io::stdout().lock();
        output.write_all(b"R").expect("publish PTY readiness");
        output.flush().expect("flush PTY readiness");
        let mut release = String::new();
        std::io::stdin()
            .lock()
            .read_line(&mut release)
            .expect("read PTY release");
        if release != "ready\n" {
            process::exit(99);
        }
        return;
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
