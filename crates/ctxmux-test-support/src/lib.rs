//! Load model shared by ctxmux integration-test binaries.
//!
//! Integration tests here start real `ctxmuxd` processes and wait for them on
//! wall-clock budgets. Under CPU starvation a correct daemon can miss a budget
//! that is generous when the machine is idle, so the harness fails for a reason
//! unrelated to the code under test. This crate owns the two knobs that keep
//! those budgets honest, so the assumption lives in one place instead of
//! drifting across every test binary.
//!
//! Two separate mechanisms, because they fail differently:
//!
//! - [`daemon_spawn_permit`] bounds how many daemon startups race at once. It
//!   extends no deadline, so a single daemon that is genuinely slow to accept
//!   connections still misses its budget and fails. This is the primary lever
//!   and it cannot hide a startup regression.
//! - [`scaled`] multiplies a budget by `CTXMUX_TEST_TIME_SCALE`. It exists for
//!   heavily contended CI runners. It *can* delay how fast a genuine hang
//!   surfaces, so the default is `1` and only CI raises it.
//!
//! Deliberately **not** covered: budgets that assert something does *not*
//! happen within a window, or that an operation finishes faster than a bound.
//! Scaling those either slows the suite for nothing or weakens the assertion.
//! Call sites keep those as literal durations.

use std::{
    sync::{
        Arc, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Read one unsigned test knob, falling back to `default` when unset.
///
/// Matches the existing repository convention: a malformed value is a harness
/// configuration error and panics rather than silently reverting to a default.
///
/// # Panics
///
/// Panics when `name` is set to a value that is not an unsigned integer.
fn environment_u64(name: &str, default: u64) -> u64 {
    std::env::var(name).map_or(default, |value| {
        value
            .parse::<u64>()
            .unwrap_or_else(|error| panic!("{name} must be an unsigned integer: {error}"))
    })
}

/// Multiplier applied to readiness and expected-arrival budgets.
///
/// Defaults to `1` so a developer machine observes true timing.
#[must_use]
pub fn time_scale() -> u64 {
    static SCALE: OnceLock<u64> = OnceLock::new();
    *SCALE.get_or_init(|| environment_u64("CTXMUX_TEST_TIME_SCALE", 1).max(1))
}

/// Scale one readiness or expected-arrival budget by [`time_scale`].
///
/// Only for budgets that bound how long a *wanted* outcome may take. A budget
/// whose elapsing is itself the assertion must stay literal.
#[must_use]
pub fn scaled(base: Duration) -> Duration {
    let factor = u32::try_from(time_scale()).unwrap_or(u32::MAX);
    base.saturating_mul(factor)
}

/// Scale one iteration count that backs a polling deadline.
///
/// Some waits express their budget as `for _ in 0..N { sleep(interval) }`
/// rather than a single timeout. Scaling the count keeps those equivalent to
/// the [`scaled`] budgets around them.
#[must_use]
pub fn scaled_polls(base: usize) -> usize {
    let factor = usize::try_from(time_scale()).unwrap_or(usize::MAX);
    base.saturating_mul(factor)
}

/// How many daemon startups may race inside one test binary.
///
/// Startup is the contended phase: each one forks a process, builds a Tokio
/// runtime, binds a socket, and must answer a ping before its budget expires.
/// With 43 such tests in one binary, an unbounded herd starves its own
/// readiness waits and a correct daemon misses a budget it meets easily when
/// started alone.
///
/// Two is deliberate rather than derived from the core count. Core count says
/// how wide the machine is, not how much of it is already busy, and the
/// observed failures happen precisely when something else owns most of the
/// cores. A fixed small bound is therefore both simpler and more robust than
/// probing parallelism.
///
/// Evidence, and its limits. On a 14-core host at load average 138 the suite
/// failed 5 of 46 tests on daemon readiness with a limit of four, and passed
/// 46 of 46 with a limit of two; raising `CTXMUX_TEST_TIME_SCALE` to four
/// instead only reduced failures from five to three, which is why the gate and
/// not the multiplier is the primary lever. That comparison was opportunistic
/// rather than controlled: at load average 40 or below, every limit from two to
/// effectively unbounded passes, so the ordinary machine cannot reproduce the
/// failure and cannot confirm the fix either. Wall-clock cost of the bound is
/// within run-to-run noise at that load. Treat the limit as a bound justified by
/// the failure mode, not as a value proven optimal.
///
/// Raise it with `CTXMUX_TEST_DAEMON_SPAWN_LIMIT` on a dedicated machine.
const DEFAULT_DAEMON_SPAWN_LIMIT: u64 = 2;

/// Peak number of permits ever held at once, for the gate's own assertion.
///
/// The gate is a process-wide `OnceLock`, so a test cannot rebuild it. This
/// counter lets one deterministic test prove the bound actually holds without
/// depending on host load, which is the only way the property is checkable on
/// an idle machine.
static PEAK_CONCURRENT_STARTUPS: AtomicUsize = AtomicUsize::new(0);
static LIVE_STARTUPS: AtomicUsize = AtomicUsize::new(0);

/// Highest number of daemon startups this binary ever ran concurrently.
#[must_use]
pub fn peak_concurrent_startups() -> usize {
    PEAK_CONCURRENT_STARTUPS.load(Ordering::Acquire)
}

/// Permits this binary allows to race, after the environment override.
#[must_use]
pub fn daemon_spawn_limit() -> usize {
    let permits = environment_u64("CTXMUX_TEST_DAEMON_SPAWN_LIMIT", DEFAULT_DAEMON_SPAWN_LIMIT);
    usize::try_from(permits).unwrap_or(usize::MAX).max(1)
}

/// Tracks one live startup and keeps the peak, releasing on drop.
struct StartupCensus;

impl StartupCensus {
    fn enter() -> Self {
        let live = LIVE_STARTUPS.fetch_add(1, Ordering::AcqRel) + 1;
        PEAK_CONCURRENT_STARTUPS.fetch_max(live, Ordering::AcqRel);
        Self
    }
}

impl Drop for StartupCensus {
    fn drop(&mut self) {
        LIVE_STARTUPS.fetch_sub(1, Ordering::AcqRel);
    }
}

/// One daemon-startup slot, held for the caller's spawn and readiness wait.
pub struct DaemonSpawnPermit {
    _permit: OwnedSemaphorePermit,
    _census: StartupCensus,
}

/// Reserve one daemon-startup slot for this test binary.
///
/// Hold the returned permit across spawn and the readiness wait, then drop it.
/// Running daemons keep full parallelism; only the startup window is bounded.
///
/// # Panics
///
/// Panics if the process-wide gate has been closed, which this crate never
/// does.
pub async fn daemon_spawn_permit() -> DaemonSpawnPermit {
    static GATE: OnceLock<Arc<Semaphore>> = OnceLock::new();
    let gate = GATE.get_or_init(|| Arc::new(Semaphore::new(daemon_spawn_limit())));
    let permit = Arc::clone(gate)
        .acquire_owned()
        .await
        .expect("daemon spawn gate stays open for the test binary");
    DaemonSpawnPermit {
        _permit: permit,
        _census: StartupCensus::enter(),
    }
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_DAEMON_SPAWN_LIMIT, scaled, scaled_polls, time_scale};
    use std::time::Duration;

    #[test]
    fn unset_scale_preserves_the_original_budget() {
        // The suite must observe true timing by default; only CI opts into slack.
        assert_eq!(time_scale(), 1);
        assert_eq!(scaled(Duration::from_secs(5)), Duration::from_secs(5));
        assert_eq!(scaled_polls(100), 100);
    }

    #[test]
    fn spawn_limit_bounds_the_startup_herd_without_serializing_it() {
        // Above one so startups still overlap; small enough that a contended
        // host cannot starve a readiness budget.
        assert!(
            (2..=4).contains(&DEFAULT_DAEMON_SPAWN_LIMIT),
            "spawn limit {DEFAULT_DAEMON_SPAWN_LIMIT} must stay a small bound above one"
        );
    }

    /// The gate's bound is checkable without a busy machine.
    ///
    /// The failure this gate exists to prevent only appears when the host is
    /// already loaded, so an idle machine cannot reproduce it. Counting the peak
    /// concurrent startups instead makes the property exact and load-independent:
    /// far more tasks than permits are released at once, and the observed peak
    /// must still never exceed the limit.
    #[test]
    fn concurrent_startups_never_exceed_the_permit_limit() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .expect("build census runtime");
        let limit = super::daemon_spawn_limit();
        runtime.block_on(async {
            let mut tasks = tokio::task::JoinSet::new();
            for _ in 0..64 {
                tasks.spawn(async {
                    let permit = super::daemon_spawn_permit().await;
                    // Hold the slot long enough that a broken gate would let
                    // other tasks in and lift the observed peak.
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    drop(permit);
                });
            }
            while let Some(joined) = tasks.join_next().await {
                joined.expect("census task completes");
            }
        });
        let peak = super::peak_concurrent_startups();
        assert!(peak > 0, "census must observe at least one startup");
        assert!(
            peak <= limit,
            "gate admitted {peak} concurrent startups above its limit of {limit}"
        );
    }
}
