//! The load-bearing proof that adding SIGCHLD to the daemon costs zero
//! descriptors.
//!
//! The daemon's zero-fd exit-detection design rests on one property of tokio's
//! signal machinery: every `signal(SignalKind::…)` registration in a process
//! funnels through a single process-wide self-pipe held in a `OnceLock`
//! `Globals` singleton (via `signal_hook_registry`). Once that pipe exists,
//! every later registration for any *other* signal reuses it and opens no new
//! descriptor. The daemon adds SIGCHLD alongside a SIGHUP it already registers,
//! so SIGCHLD lands on the reuse path — it is free.
//!
//! This test measures that reuse directly rather than trusting the claim, and
//! it does so without depending on *when* the singleton descriptor is created,
//! because that moment is platform-specific:
//!
//! * On macOS the signal driver is backed by a fresh kqueue self-pipe that the
//!   *first* `signal()` call allocates, so the first registration is an
//!   observable descriptor bump.
//! * On Linux the signal driver instead rides the multi-threaded runtime's
//!   already-open epoll I/O driver, so even the first registration opens no new
//!   descriptor — the count does not move at all. (Verified on a real Linux
//!   5.15 `x86_64` host: the first registration measured a zero delta.)
//!
//! The old form of this test asserted `first_cost > 0` as a precondition, which
//! held on macOS but made the test fail on Linux — the very platform CI runs
//! and the one the frozen per-Run fd budget was never baselined against. So the
//! assertion here is the property the daemon actually depends on and that holds
//! on both platforms: after the first registration, registering a whole *batch*
//! of additional distinct signals — SIGCHLD among them — moves the descriptor
//! count by exactly zero. A design that allocated a descriptor per signal would
//! show a delta equal to the batch size; zero across many distinct kinds is the
//! guarantee, and it is independent of the batch size and of the platform's
//! allocation strategy. The first registration's cost is recorded for the log
//! only, never asserted.
//!
//! It lives in its own integration binary so that the first registration below
//! is provably the first `signal()` call in the process — a unit test sharing
//! the daemon's test binary could not promise that, and the reuse claim would
//! be meaningless if some earlier test had already created the singleton.

// Exact fd census needs the process descriptor table, which this reads via
// procfs. macOS is covered by the reliability qualification harness (see the
// per-Run descriptor test in native_lifecycle.rs for the same rationale).
#![cfg(target_os = "linux")]

use tokio::signal::unix::{Signal, SignalKind, signal};

fn open_fd_count() -> usize {
    std::fs::read_dir("/proc/self/fd")
        .expect("read this process's descriptor table from procfs")
        .count()
}

/// Reading `/proc/self/fd` itself opens a directory descriptor for the duration
/// of the read, so two consecutive counts of an unchanged table are equal only
/// because that transient fd is opened and closed within each call. Sample a few
/// times and take the steady value to shed any lingering lazily-created fd (the
/// tokio runtime's own reactor, for instance) so the delta we attribute to a
/// signal registration is really the registration's.
fn settled_fd_count() -> usize {
    let mut last = open_fd_count();
    for _ in 0..8 {
        std::thread::sleep(std::time::Duration::from_millis(10));
        let next = open_fd_count();
        if next == last {
            return next;
        }
        last = next;
    }
    last
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn additional_signal_kinds_cost_zero_descriptors() {
    // Settle the tokio runtime's own descriptors first, so the deltas below are
    // attributable to the signal registrations and not to lazy reactor setup.
    let before_first = settled_fd_count();

    // First registration in this process. On macOS this is where the driver's
    // self-pipe is allocated (an observable bump); on Linux the driver reuses
    // the runtime's epoll and this moves nothing. We do not assert on it —
    // its cost is platform-specific — but we record it so a reader of the test
    // output can see which regime the running platform is in. Keep the guard
    // alive so its descriptors (if any) are not dropped before we measure.
    let _first = signal(SignalKind::hangup()).expect("register first signal (SIGHUP)");
    let after_first = settled_fd_count();
    let first_cost = after_first
        .checked_sub(before_first)
        .expect("registering a signal never reduces the descriptor count");
    eprintln!(
        "signal_driver_fd_cost: first registration (SIGHUP) cost {first_cost} descriptor(s) \
         ({before_first} -> {after_first}); this is platform-specific and is not asserted"
    );

    // The property the daemon relies on: every registration after the first —
    // SIGCHLD is the one the daemon adds — reuses the process-wide singleton and
    // opens no descriptor. Register a batch of distinct kinds, holding every
    // guard alive, and prove the whole batch moved the count by zero. Using
    // several kinds rather than one makes "zero" a statement about scaling: a
    // per-signal-descriptor design would show a delta equal to the batch size.
    let batch = [
        SignalKind::child(), // the daemon's actual SIGCHLD case
        SignalKind::user_defined1(),
        SignalKind::user_defined2(),
        SignalKind::window_change(),
        SignalKind::io(),
    ];
    let batch_len = batch.len();
    let _guards: Vec<Signal> = batch
        .into_iter()
        .map(|kind| signal(kind).expect("register an additional distinct signal"))
        .collect();

    let after_batch = settled_fd_count();
    let batch_cost = after_batch
        .checked_sub(after_first)
        .expect("registering a signal never reduces the descriptor count");
    assert_eq!(
        batch_cost, 0,
        "registering {batch_len} more distinct SignalKinds (SIGCHLD, SIGUSR1, SIGUSR2, SIGWINCH, \
         SIGIO) after the first must reuse the process-wide signal self-pipe and cost zero \
         descriptors; measured {batch_cost} ({after_first} -> {after_batch}). A nonzero cost here \
         means signals scale the descriptor table, which would break the daemon's free SIGCHLD \
         exit-detection and the frozen per-Run fd budget."
    );
}
