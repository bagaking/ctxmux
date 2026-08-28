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
    sync::{Arc, OnceLock},
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
/// measured failures happen precisely when something else owns most of the
/// cores. On a 14-core host under load average 138, a limit of two completes
/// all 46 tests while four leaves five of them failing on readiness; at idle
/// the same limit costs a few seconds of wall time. A fixed small bound is
/// therefore both simpler and more robust than probing parallelism.
///
/// Raise it with `CTXMUX_TEST_DAEMON_SPAWN_LIMIT` on a dedicated machine.
const DEFAULT_DAEMON_SPAWN_LIMIT: u64 = 2;

/// Reserve one daemon-startup slot for this test binary.
///
/// Hold the returned permit across spawn and the readiness wait, then drop it.
/// Running daemons keep full parallelism; only the startup window is bounded.
///
/// # Panics
///
/// Panics if the process-wide gate has been closed, which this crate never
/// does.
pub async fn daemon_spawn_permit() -> OwnedSemaphorePermit {
    static GATE: OnceLock<Arc<Semaphore>> = OnceLock::new();
    let gate = GATE.get_or_init(|| {
        let permits = environment_u64("CTXMUX_TEST_DAEMON_SPAWN_LIMIT", DEFAULT_DAEMON_SPAWN_LIMIT);
        let permits = usize::try_from(permits).unwrap_or(usize::MAX).max(1);
        Arc::new(Semaphore::new(permits))
    });
    Arc::clone(gate)
        .acquire_owned()
        .await
        .expect("daemon spawn gate stays open for the test binary")
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
}
