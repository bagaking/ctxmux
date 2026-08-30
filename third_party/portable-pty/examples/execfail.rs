//! Does a failing `execve` reach the caller, or is it reported as a successful
//! spawn?
//!
//! Upstream's `close_random_fds` closes std's exec-error pipe, so the child
//! cannot report why it died and `spawn_command` returns `Ok` for a process
//! that never ran (wezterm#7893). The Linux `close_range(CLOSE_RANGE_CLOEXEC)`
//! path in this fork leaves that pipe intact.
//!
//! Run this on Linux to see the fix; on macOS the fork falls through to the
//! original walk, so the upstream behaviour is expected and correct there.
//!
//! Every case is bounded by a timeout and the control case must succeed —
//! without one, a hang looks exactly like a pass.
use portable_pty::{native_pty_system, CommandBuilder, PtySize};
use std::{sync::mpsc, thread, time::Duration};

fn probe(label: &str, path: &str) {
    println!("--- {label} ---");
    let (tx, rx) = mpsc::channel();
    let owned = path.to_string();
    thread::spawn(move || {
        let pty = native_pty_system();
        let pair = pty
            .openpty(PtySize {
                rows: 24,
                cols: 80,
                pixel_width: 0,
                pixel_height: 0,
            })
            .expect("openpty");
        match pair.slave.spawn_command(CommandBuilder::new(&owned)) {
            Ok(mut child) => {
                let _ = tx.send("spawn_command -> Ok(child)".to_string());
                // Both ends must go before wait(): with the master open, wait
                // blocks whether or not the child died, and a hang would be
                // misread as the defect.
                drop(pair.slave);
                drop(pair.master);
                let _ = tx.send(match child.wait() {
                    Ok(status) => format!("wait -> {status:?}"),
                    Err(error) => format!("wait -> Err {error}"),
                });
            }
            Err(error) => {
                let _ = tx.send(format!(
                    "spawn_command -> Err: {}",
                    error.to_string().lines().next().unwrap_or_default()
                ));
            }
        }
    });
    for _ in 0..2 {
        match rx.recv_timeout(Duration::from_secs(10)) {
            Ok(message) => {
                let done = message.starts_with("spawn_command -> Err");
                println!("  {message}");
                if done {
                    return;
                }
            }
            Err(_) => {
                println!("  *** TIMEOUT: no result in 10s ***");
                return;
            }
        }
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let enoexec = args.next().expect("usage: execfail <enoexec-bin> <bad-shebang>");
    let shebang = args.next().expect("usage: execfail <enoexec-bin> <bad-shebang>");
    probe("control: /bin/echo (must spawn)", "/bin/echo");
    probe("ENOEXEC: exec bit set, not a valid binary", &enoexec);
    probe("bad shebang interpreter", &shebang);
}
