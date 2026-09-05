//! Host descriptor admission, independent of historical qualification tiers.
//! Every native live owner keeps three descriptors. Startup reserves fixed
//! process/attachment/creation headroom, raises toward explicit live policy
//! or the metadata budget's minimum-owner population, and admits what the OS
//! actually funds. A generous existing soft limit is never clamped to a tier.

use rustix::process::{Resource, Rlimit, getrlimit, setrlimit};

#[cfg(test)]
use crate::creation::MAX_CREATION_OWNER_SLOTS;

/// Descriptors one live native Run keeps open: PTY master + reader dup + writer
/// dup, all referring to the same master. Held at 3 by ADR 013 and pinned as
/// `fds_per_run` in the reliability contracts.
pub(crate) const FDS_PER_RUN: usize = 3;

/// Historical 4000-Run qualification example, used only to test descriptor
/// arithmetic. Production admission has no dependency on this fixture size.
#[cfg(test)]
pub(crate) const FD_BUDGET_LIVE_RUNS: usize = 4000;

/// Fixed non-Run descriptors the daemon holds regardless of Run count: stdio,
/// the accepted listener socket, tokio's kqueue/epoll, signal registrations, up
/// to three inherited handoff/readiness/qualification-stats fds, and the
/// `SQLite` db/WAL/SHM/state-lock quartet, with a little slack. Measured at ~12
/// (ADR 013); rounded to 16 to fund fixed-owner variation. This reservation
/// reduces the measured host funding, rather than setting a product cap.
const FD_BASELINE: usize = 16;

/// Concurrent client attachment/control sockets to keep reachable on top of the
/// Run descriptors. Attachment fan-out is not itself admission-bounded (ADR
/// 013), so this is deliberate headroom rather than a promise; it keeps
/// ordinary attach/control traffic from competing with Run admission for fds.
const FD_ATTACHMENT_HEADROOM: usize = 64;

/// Fixed descriptors reserved before any live-Run descriptor: the baseline, the
/// attachment headroom, and the PTYs the eight-slot physical-overlap owner can
/// hold un-published alongside the retained records during turnover (ADR 013).
#[cfg(test)]
const fn reserved_fds() -> usize {
    FD_BASELINE + FD_ATTACHMENT_HEADROOM + MAX_CREATION_OWNER_SLOTS * FDS_PER_RUN
}

/// Soft `RLIMIT_NOFILE` the daemon raises toward so [`FD_BUDGET_LIVE_RUNS`]
/// concurrent live Runs are reachable without EMFILE:
/// `FD_BUDGET_LIVE_RUNS * FDS_PER_RUN + reserved_fds()`.
///
/// Deliberately and load-bearingly `const fn`: it is what keeps a future per-Run
/// descriptor honest. `FDS_PER_RUN` is 3 on every platform because native exit
/// detection is event-driven through the *process-wide* SIGCHLD self-pipe
/// (`native_runtime`/`serve`) — reusing descriptors the daemon already holds, so
/// a watched Run opens no per-Run watch fd. Any future scheme that instead spent
/// a descriptor per Run gated on a runtime capability (a `pidfd_open` probe, a
/// kernel-version check) could not be consulted from a `const fn`, forcing
/// whoever adds it to drop `const` here — a visible, reviewable diff — rather
/// than letting the real per-Run cost drift above the budgeted 3 the way an
/// earlier per-Run pidfd once did, unnoticed until EMFILE at a few thousand Runs.
#[cfg(test)]
pub(crate) const fn fd_budget() -> usize {
    FD_BUDGET_LIVE_RUNS * FDS_PER_RUN + reserved_fds()
}

/// Funded live owners after baseline, attachment and creation headroom.
/// An unlimited host limit has no population ceiling. PTY exhaustion is
/// discovered by actual allocation and returns `RunCapacity` without pretending
/// a stale startup PTY census predicts current host availability.
#[cfg(test)]
pub(crate) fn run_ceiling_for_soft_limit(effective_soft: Option<u64>) -> usize {
    run_ceiling_with_reserved(effective_soft, reserved_fds())
}

fn run_ceiling_with_reserved(effective_soft: Option<u64>, reserved: usize) -> usize {
    let Some(soft) = effective_soft else {
        return usize::MAX;
    };
    let reserved = reserved as u64;
    let per_run = FDS_PER_RUN as u64;
    let for_runs = soft.saturating_sub(reserved);
    let admissible = for_runs / per_run;
    usize::try_from(admissible).unwrap_or(usize::MAX)
}

/// Outcome of applying the startup budget, for logging and clamping.
pub(crate) struct FdBudgetOutcome {
    pub(crate) provisioned_runs: usize,
    pub(crate) provisioned_fds: u64,
    /// Soft limit in effect after any raise; `None` means unlimited.
    pub(crate) effective_soft: Option<u64>,
    /// Live-Run ceiling the effective soft limit funds.
    pub(crate) run_ceiling: usize,
    /// The soft limit was successfully raised toward the budget.
    pub(crate) raised: bool,
    /// The effective ceiling is below the daemon's concurrency target
    /// [`FD_BUDGET_LIVE_RUNS`].
    pub(crate) clamped: bool,
    /// Soft limit observed before any raise; `None` means unlimited.
    pub(crate) original_soft: Option<u64>,
    /// Hard limit observed; `None` means unlimited.
    pub(crate) hard: Option<u64>,
}

/// Read `RLIMIT_NOFILE`, raise the soft limit toward [`fd_budget`] without ever
/// touching the hard limit, and compute the funded live-Run ceiling.
///
/// Idempotent across the exec-in-place re-exec: the incoming image inherits the
/// already-raised soft limit, so this only ever raises (never lowers) and a
/// re-run is a no-op rather than a fight with the prior raise. If the OS refuses
/// the raise, or the hard limit is below the budget, the returned ceiling is
/// clamped and `clamped` is set — the caller logs both numbers and lowers
/// admission accordingly.
///
/// The clamp is computed from the limit the OS *actually granted*, re-read via
/// `getrlimit` after the `setrlimit`, not from the value requested. On macOS a
/// `setrlimit` can report success yet leave a soft limit that later fails to
/// fund opens against `kern.maxfilesperproc`; re-reading keeps the advertised
/// ceiling honest rather than optimistic. (Empirically, macOS reflects the
/// requested soft limit faithfully through `getrlimit` even above
/// `kern.maxfilesperproc`, enforcing that wall at `open()` time instead — so the
/// re-read is a portable honesty guard, cheap where it is a no-op.)
pub(crate) fn apply_fd_budget(resources: crate::ResourceLimits) -> FdBudgetOutcome {
    let limits = getrlimit(Resource::Nofile);
    let original_soft = limits.current;
    let hard = limits.maximum;
    let provisioned_runs = resources.live_runs.unwrap_or_else(|| {
        usize::try_from(resources.metadata_bytes / crate::resident_run_owner_bytes())
            .unwrap_or(usize::MAX)
            .max(1)
    });
    let reserved = FD_BASELINE
        .saturating_add(FD_ATTACHMENT_HEADROOM)
        .saturating_add(resources.creation_workers.saturating_mul(FDS_PER_RUN));
    let budget = (provisioned_runs as u64)
        .saturating_mul(FDS_PER_RUN as u64)
        .saturating_add(reserved as u64);

    // Desired soft: the budget, but never above the hard ceiling. Requesting a
    // soft limit above the hard limit only fails; `None` hard is unlimited, so
    // the budget stands. The hard limit is left untouched — the daemon never
    // raises the maximum, only the current.
    let desired = hard.map_or(budget, |hard| budget.min(hard));

    let mut raised = false;
    if let Some(soft) = original_soft
        && soft < desired
        && setrlimit(
            Resource::Nofile,
            Rlimit {
                current: Some(desired),
                maximum: hard,
            },
        )
        .is_ok()
    {
        raised = true;
    }

    // Re-read: the ceiling and clamp must reflect the limit the OS actually
    // granted, not the value requested. A raise the kernel silently declined to
    // honor would otherwise make the advertised ceiling lie.
    let effective_soft = getrlimit(Resource::Nofile).current;

    let run_ceiling = run_ceiling_with_reserved(effective_soft, reserved)
        .min(resources.live_runs.unwrap_or(usize::MAX));
    FdBudgetOutcome {
        effective_soft,
        run_ceiling,
        raised,
        clamped: run_ceiling < provisioned_runs,
        provisioned_runs,
        provisioned_fds: budget,
        original_soft,
        hard,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        FD_BUDGET_LIVE_RUNS, FDS_PER_RUN, fd_budget, reserved_fds, run_ceiling_for_soft_limit,
    };

    /// The budget is derived from the concurrency target, and its magnitude is
    /// pinned: 4000 live Runs at 3 fds each plus the 104-fd fixed reservation is
    /// 12104. This is the figure a thousands-concurrent-Run agent runtime needs;
    /// a regression that silently re-sized it back toward the old 128 record cap
    /// would fail here.
    #[test]
    fn budget_funds_the_configured_concurrency_target() {
        assert_eq!(FD_BUDGET_LIVE_RUNS, 4000);
        assert_eq!(
            fd_budget(),
            FD_BUDGET_LIVE_RUNS * FDS_PER_RUN + reserved_fds()
        );
        assert_eq!(fd_budget(), 12104);
        // Raising the soft limit to exactly the budget funds exactly the
        // concurrency target: admission is then bounded by that target, never by
        // descriptors falling short of it.
        assert_eq!(
            run_ceiling_for_soft_limit(Some(fd_budget() as u64)),
            FD_BUDGET_LIVE_RUNS
        );
    }

    /// The effective ceiling tracks descriptors at the concurrency-target
    /// boundary and is not simply pinned at it: a limit funding exactly the
    /// target admits it, one descriptor-set short admits one fewer.
    #[test]
    fn ceiling_tracks_descriptors_at_the_target_boundary() {
        let per_run = u64::try_from(FDS_PER_RUN).unwrap();
        let reserved = u64::try_from(reserved_fds()).unwrap();
        let at_target = reserved + u64::try_from(FD_BUDGET_LIVE_RUNS).unwrap() * per_run;
        assert_eq!(
            run_ceiling_for_soft_limit(Some(at_target)),
            FD_BUDGET_LIVE_RUNS
        );
        assert_eq!(
            run_ceiling_for_soft_limit(Some(at_target - per_run)),
            FD_BUDGET_LIVE_RUNS - 1
        );
    }

    /// A constrained limit clamps below the concurrency target by the exact
    /// descriptor arithmetic, so the operator hits an honest ceiling.
    #[test]
    fn constrained_limit_clamps_to_the_funded_run_count() {
        let reserved = u64::try_from(reserved_fds()).unwrap();
        let per_run = u64::try_from(FDS_PER_RUN).unwrap();
        for runs in [0_u64, 2, 17, 50] {
            let soft = reserved + runs * per_run;
            let expected = usize::try_from(runs).unwrap().min(FD_BUDGET_LIVE_RUNS);
            assert_eq!(run_ceiling_for_soft_limit(Some(soft)), expected);
            // A partial descriptor past a whole Run cannot fund another Run.
            assert_eq!(run_ceiling_for_soft_limit(Some(soft + 1)), expected);
        }
    }

    /// An unlimited soft limit funds the full concurrency target without
    /// clamping.
    #[test]
    fn unlimited_soft_funds_the_full_target() {
        assert_eq!(run_ceiling_for_soft_limit(None), usize::MAX);
    }

    /// A soft limit funding more Runs than the daemon provisions for does not
    /// raise admission above the concurrency target it budgeted and measured
    /// against: the ceiling stays pinned at the target.
    #[test]
    fn generous_limit_funds_runs_beyond_the_historical_fixture() {
        let per_run = u64::try_from(FDS_PER_RUN).unwrap();
        let reserved = u64::try_from(reserved_fds()).unwrap();
        let beyond_target = reserved + u64::try_from(FD_BUDGET_LIVE_RUNS + 1000).unwrap() * per_run;
        assert_eq!(
            run_ceiling_for_soft_limit(Some(beyond_target)),
            FD_BUDGET_LIVE_RUNS + 1000
        );
    }

    /// A limit below the fixed reservation saturates to zero rather than
    /// underflowing into a huge admissible count.
    #[test]
    fn tiny_limit_saturates_to_zero() {
        assert_eq!(run_ceiling_for_soft_limit(Some(1)), 0);
        assert_eq!(run_ceiling_for_soft_limit(Some(0)), 0);
    }
}
