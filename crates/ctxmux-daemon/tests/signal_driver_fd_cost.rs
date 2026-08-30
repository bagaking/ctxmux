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
//! it deliberately does not depend on *when* the singleton descriptor is
//! created. That moment is an implementation detail of the platform's driver,
//! and measurement has already contradicted the intuitive story once:
//!
//! * Linux rides the multi-threaded runtime's already-open epoll I/O driver, so
//!   even the first registration opens nothing. (Measured on a real Linux 5.15
//!   `x86_64` host: 10 -> 10.)
//! * macOS was expected to allocate a fresh kqueue self-pipe on the first
//!   `signal()` call and show a visible bump. It does not — measured 10 -> 10
//!   there too. The expectation was wrong, not the measurement.
//!
//! An earlier form of this test asserted `first_cost > 0` as a precondition. It
//! was written against that wrong expectation, and it failed the moment it ran
//! on Linux — the platform CI actually gates on, and the one the frozen per-Run
//! fd budget was never baselined against. Asserting a first-registration cost in
//! either direction pins a detail the daemon does not depend on, so this test
//! asserts neither: the cost is printed for the log and never checked.
//!
//! What is asserted is the property the daemon does depend on, which holds on
//! both platforms regardless of that detail: after the first registration,
//! registering a whole *batch* of additional distinct signals — SIGCHLD among
//! them — moves the descriptor count by exactly zero. A design that allocated a
//! descriptor per signal would show a delta equal to the batch size; zero across
//! many distinct kinds is the guarantee, and it is independent of the batch size
//! and of the platform's allocation strategy.
//!
//! It lives in its own integration binary so that the first registration below
//! is provably the first `signal()` call in the process — a unit test sharing
//! the daemon's test binary could not promise that, and the reuse claim would
//! be meaningless if some earlier test had already created the singleton.
// The census reads `/dev/fd`, which both platforms provide as a view of this
// process's own descriptor table (on Linux it is a symlink to `/proc/self/fd`),
// so the suite runs everywhere. A whole-file `#![cfg(target_os = "linux")]`
// would compile to an empty binary on macOS: the suite would silently assert
// nothing there while the evidence map still claimed it as macOS coverage.
// macOS is the platform where the first registration *is* an observable bump,
// so it is the more interesting half of the reuse claim, not the one to drop.

use tokio::signal::unix::{Signal, SignalKind, signal};

fn open_fd_count() -> usize {
    std::fs::read_dir("/dev/fd")
        .expect("read this process's descriptor table from /dev/fd")
        .count()
}

/// The census itself opens nothing, but the tokio runtime creates some of its
/// own descriptors lazily. Sample until two consecutive counts agree so the
/// delta we attribute to a signal registration is really the registration's and
/// not a reactor descriptor that happened to appear alongside it.
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

    // First registration in this process. Measured at zero on both Linux and
    // macOS, but that is not asserted — it is a driver implementation detail
    // the daemon does not rely on, and it is exactly the detail an earlier
    // version of this test got wrong. Recorded so a reader of the test output
    // can see which regime the running platform is in. Keep the guard alive so
    // its descriptors (if any) are not dropped before we measure.
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
