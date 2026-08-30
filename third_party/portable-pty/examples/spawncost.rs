//! How does spawn cost scale with the size of the parent's descriptor table?
//!
//! Upstream walks /dev/fd in the child of every spawn, so the cost grows with
//! however many descriptors the parent happens to hold — which for a process
//! that keeps descriptors per unit of work makes filling O(N^2). The Linux
//! close_range path in this fork makes it flat.
//!
//! Holds N descriptors open, then times real spawns. Compare the two builds.
use portable_pty::{native_pty_system, CommandBuilder, PtySize};
use std::{fs::File, time::Instant};

fn spawn_once() {
    let pty = native_pty_system();
    let pair = pty
        .openpty(PtySize { rows: 24, cols: 80, pixel_width: 0, pixel_height: 0 })
        .expect("openpty");
    let mut child = pair
        .slave
        .spawn_command(CommandBuilder::new("/bin/true"))
        .expect("spawn /bin/true");
    drop(pair.slave);
    drop(pair.master);
    let _ = child.wait();
}

fn main() {
    let spawns: usize = std::env::args().nth(1).and_then(|a| a.parse().ok()).unwrap_or(20);
    let mut held: Vec<File> = Vec::new();
    println!("{:>8} {:>12} {:>12}", "held_fds", "ms_total", "ms_per_spawn");
    for target in [1000_usize, 4000, 16000, 48000, 96000] {
        while held.len() < target {
            match File::open("/dev/null") {
                Ok(f) => held.push(f),
                Err(_) => break,
            }
        }
        // Warm once so the first spawn's page faults are not counted.
        spawn_once();
        let start = Instant::now();
        for _ in 0..spawns {
            spawn_once();
        }
        let ms = start.elapsed().as_secs_f64() * 1000.0;
        println!("{:>8} {:>12.1} {:>12.2}", held.len(), ms, ms / spawns as f64);
    }
}
