//! Daemon-wide retained-output byte budget and its cross-Run reclamation policy.
//!
//! # The hazard this closes
//!
//! `OUTPUT_RETENTION_BYTES` (4 MiB) is a *per-Run* cap enforced inside
//! `OutputLog::push`: a Run trims its own oldest chunks the moment its own log
//! crosses 4 MiB. That leaves two holes, both of which this module fills.
//!
//! 1. **No aggregate bound.** N Runs authorize N × 4 MiB with nothing watching
//!    the sum. The record cap (`MAX_RETAINED_RUNS = 128`) only *incidentally*
//!    bounded this at 128 × 4 MiB = 512 MiB, and a sibling change is removing
//!    that cap because this is an agent runtime that runs thousands of
//!    concurrent Runs. At 4000 Runs the un-bounded design authorizes 16 GiB.
//! 2. **Quiet Runs pin memory forever.** Per-Run eviction fires *only on that
//!    Run's own push*. A Run that fills 4 MiB and then goes silent never pushes
//!    again, so nothing ever reclaims its 4 MiB — even under global pressure.
//!    A farm measurement drove RSS from 390 MiB to 810 MiB with 2048 verbose
//!    Runs in 20 s, still climbing, precisely because of this.
//!
//! tmux solves the same shape by bounding *memory* (every pane has a
//! `history-limit`), never *pane count*. We were bounding count and not memory
//! — backwards. This module bounds memory.
//!
//! # The three decisions, and the alternatives rejected
//!
//! ## What the budget is: 1 GiB (see [`RETENTION_BUDGET_BYTES`])
//!
//! Chosen against two hard reference points, not to match `128 × 4 MiB`:
//!
//! - **Floor (must not go below):** the frozen `reliability-gc-contract.json`
//!   replay-pressure phase retains 128 terminal Runs at 4 MiB *plus* an 8-wide
//!   replacement overlap = `retained_plus_overlap_payload_bytes = 570_425_344`
//!   (544 MiB), and asserts every one of those replays is `truncated == false`.
//!   If the budget sat at or below that peak, reclamation would fire *during the
//!   gate* and truncate a replay the gate demands intact. The budget must clear
//!   the peak with headroom, exactly as `fd_budget` funds its concurrency target
//!   without clamping at it. [`GATE_RETAINED_PLUS_OVERLAP_FLOOR_BYTES`] pins that
//!   floor at compile time.
//! - **Ceiling (must be defensible for thousands of Runs):** the pathological
//!   4000-Run × 4 MiB = 16 GiB must be capped hard. 1 GiB caps it 16×. Output
//!   payload dominates daemon RSS at that scale (the fd-budget measurements put
//!   4000 active Runs near ~476 MiB of *non-payload* steady RSS), so a ~1 GiB
//!   scrollback ceiling is a credible line for an agent host: it can afford a
//!   gigabyte of retained terminal output, it cannot afford sixteen.
//!
//! Relationship to the persistence-side budgets (`persistence.rs`): those pin a
//! 64 MiB durable *metadata* total and a 256 MiB durable *replay* total —
//! disk-backed, restart-surviving, and a different resource from this hot
//! in-memory payload. ADR 013 already lets the live 512 MiB in-memory payload
//! coexist with the 256 MiB durable cap; a live-memory budget larger than the
//! durable one is expected, not a contradiction. We deliberately do **not**
//! reuse 64 MiB or 256 MiB: they govern bytes on disk, not resident RSS.
//!
//! ## Who gets evicted: oldest bytes of the fattest, unattached Runs first
//!
//! Reclamation trims oldest chunks (front of the deque) from the Runs holding
//! the most retained bytes, preferring Runs with **no live attachment** over
//! Runs a client is actively reading. This directly targets the quiet-Run
//! hole: a Run that filled 4 MiB and went silent is the *largest* retained log
//! and (usually) unattached, so it is the first victim — reclaimed by *another*
//! Run's push, which is the event we already have.
//!
//! - *Rejected: refuse-admission projection* (the shape `persistence.rs` uses
//!   at its admission boundary — check the post-state and reject rather than
//!   evict after the fact). You cannot refuse a running child's stdout: the
//!   bytes already exist in the kernel PTY buffer and the read already happened.
//!   Persistence can refuse because a *new Run* is what crosses its boundary;
//!   here the boundary is bytes from a live process, which has no "no" to hear.
//! - *Rejected: per-Run-only eviction* (today's design). It cannot reclaim a
//!   quiet Run — that is the bug.
//! - *Rejected: strict least-recently-read.* It needs a per-Run read timestamp
//!   updated on every replay, i.e. new write traffic on the hot attach path for
//!   a second-order quality gain. The attachment-aware size policy captures the
//!   first-order win (evict quiet fat Runs) without that cost. Attachment state
//!   is read best-effort during a scan that only ever runs *over* budget.
//!
//! ## When it runs: on push, single-flighted, never on a timer
//!
//! Reclamation is driven by the same event that admits new bytes — a
//! `record_output` push. There is **no periodic sweep**: idle CPU is the
//! subject of a sibling task, and a quiet daemon that is already at rest under
//! budget has nothing to reclaim (the push that last put it over budget already
//! reclaimed on its way through). The fast path is a single relaxed atomic load
//! (`total <= limit` → return); the expensive scan is gated behind a
//! `try_lock` so that a burst of concurrent over-budget pushes elects exactly
//! one reclaimer and the rest return immediately.
//!
//! ## What a client observes: the existing truncation signal, unchanged
//!
//! Trimming here uses the *same* front-pop mechanic as per-Run eviction, so a
//! client observes the *same* honest signal it already handles: `replay`
//! reports `truncated = true` and an advanced `first_available_byte`, and a
//! live attachment sees the discontinuity exactly as it does when a Run
//! overruns its own 4 MiB. No new observable state, no silent data loss, and a
//! log is never emptied below its last chunk — `latest_output_bytes` and the
//! replay cursor stay monotonic so persistence finalize and live cursors are
//! never rewound.
//!
//! # Lock discipline (this crate is `#![forbid(unsafe_code)]`)
//!
//! The budget holds only [`Weak`] participant handles, never `Arc<Run>`:
//! collection and `remove` require `Arc::strong_count == 1`, so a strong ref
//! here would immortalize every Run. A victim is `upgrade`d transiently, only
//! inside a reclamation scan that only runs over budget (never during the
//! frozen gate, which stays under the limit), so the brief strong-count bump
//! cannot make collection's eligibility check flake in any measured workload.
//!
//! Accounting is tied to `OutputLog`'s own lifetime: the log increments the
//! shared total on push, decrements it on every trim (per-Run *and* global),
//! and decrements the remainder in `Drop`. Because the log drops exactly when
//! its `Run` drops, every reclamation path — ordinary drop, Registry
//! collection/replacement, and the explicit `remove` verb — decrements the
//! total for free, with no separate accounting hook to keep in sync. The
//! `remove` path and a budget trim therefore leave the same observable state: a
//! smaller (or absent) retained log with an honest truncation cursor.
//!
//! The only ordering rule reclamation must obey: never hold the participants
//! lock while locking a victim's `output`, and never hold two `output` locks at
//! once. `record_output` locks its own `output`, releases it, *then* calls
//! reclamation, which takes the participants lock, releases it with a snapshot,
//! then locks victims one at a time. No cycle is constructible.

use std::sync::{
    Arc, Mutex, Weak,
    atomic::{AtomicU64, Ordering},
};

use ctxmux_protocol::RunId;

/// Daemon-wide ceiling on the sum of retained `OutputLog` payload bytes across
/// every live and terminal Run. See the module docs for the full derivation.
///
/// 1 GiB: above the 544 MiB frozen-gate peak with headroom so reclamation never
/// fires during qualification, and 16× below the 16 GiB a 4000-Run host would
/// otherwise authorize.
pub(crate) const RETENTION_BUDGET_BYTES: u64 = 1024 * 1024 * 1024;

/// The frozen replay-pressure peak the budget must clear: 128 retained Runs
/// plus an 8-wide replacement overlap, each at the 4 MiB per-Run cap. Pinned in
/// `reliability-gc-contract.json` as `retained_plus_overlap_payload_bytes` and
/// in ADR 013 as the 544 MiB retained-plus-overlap bound. Expressed as a literal
/// (not `MAX_RETAINED_RUNS * OUTPUT_RETENTION_BYTES`) so it stays a valid floor
/// after the sibling change removes the record cap.
const GATE_RETAINED_PLUS_OVERLAP_FLOOR_BYTES: u64 = 570_425_344;

/// The budget must clear the frozen gate peak, or reclamation would truncate a
/// replay the gate asserts intact. Enforced at compile time so the two numbers
/// cannot drift into that contradiction. This is a *lower* bound only; the
/// magnitude above it is the defensible-for-thousands-of-Runs ceiling argued in
/// the module docs.
const _: () = assert!(
    RETENTION_BUDGET_BYTES > GATE_RETAINED_PLUS_OVERLAP_FLOOR_BYTES,
    "the retained-byte budget must sit above the frozen replay-pressure peak"
);

/// One Run, viewed by the reclamation policy through a `Weak` handle.
///
/// Object-safe and `Send + Sync` so the budget can hold
/// `Weak<dyn RetentionVictim + Send + Sync>` and `upgrade` it transiently during
/// a scan. Every method is cheap and self-contained: it takes the Run's own
/// `output` lock for the duration of one call and no other lock, so the policy
/// never holds two `output` locks at once.
pub(crate) trait RetentionVictim: Send + Sync {
    /// Stable identity, used to skip the Run that triggered reclamation and for
    /// deterministic tie-breaking.
    fn run_id(&self) -> RunId;

    /// Bytes this Run currently retains in memory. Best-effort: it may change
    /// between this read and a subsequent [`reclaim_output`](Self::reclaim_output).
    fn retained_output_bytes(&self) -> usize;

    /// Whether a client is currently attached and replaying this Run. An
    /// attached Run is a worse eviction victim than a quiet one, but the replay
    /// contract already tolerates truncation, so this only *orders* victims.
    fn is_attached(&self) -> bool;

    /// Trim oldest chunks until at least `drop_at_least` bytes are shed or only
    /// one chunk remains, decrementing the shared total as it goes. Returns the
    /// bytes actually reclaimed. Never empties the log below its final chunk, so
    /// `latest_output_bytes` and the replay cursor stay monotonic.
    fn reclaim_output(&self, drop_at_least: usize) -> usize;
}

/// Cloneable handle to the daemon-wide retained-byte budget. Every clone shares
/// one [`Inner`]; the daemon holds one and threads clones into each `Run` and
/// its `OutputLog`, exactly as `QualificationStats` is threaded.
#[derive(Clone)]
pub(crate) struct RetentionBudget {
    inner: Arc<Inner>,
}

struct Inner {
    /// Hard ceiling on `total`. Reclamation targets bringing `total` at or
    /// below this.
    limit: u64,
    /// Sum of `retained_bytes` across every participating `OutputLog`. Mutated
    /// only by `OutputLog` accounting (add on push, sub on trim/drop).
    total: AtomicU64,
    /// Weak handles to every participating Run. Pruned opportunistically. Never
    /// strong: a strong ref would break collection's `strong_count == 1`
    /// eligibility.
    participants: Mutex<Vec<Weak<dyn RetentionVictim + Send + Sync>>>,
    /// Single-flight gate: a burst of over-budget pushes elects one reclaimer
    /// via `try_lock`; the rest return without contending on the scan.
    reclaiming: Mutex<()>,
}

impl RetentionBudget {
    /// The production budget: [`RETENTION_BUDGET_BYTES`].
    pub(crate) fn production() -> Self {
        Self::with_limit(RETENTION_BUDGET_BYTES)
    }

    /// A budget with an explicit limit, for tests that must reach the ceiling
    /// without allocating a gigabyte.
    pub(crate) fn with_limit(limit: u64) -> Self {
        Self {
            inner: Arc::new(Inner {
                limit,
                total: AtomicU64::new(0),
                participants: Mutex::new(Vec::new()),
                reclaiming: Mutex::new(()),
            }),
        }
    }

    /// Configured ceiling. Test-only observability: the production reclaim path
    /// reads `inner.limit`/`inner.total` directly.
    #[cfg(test)]
    pub(crate) fn limit(&self) -> u64 {
        self.inner.limit
    }

    /// Current retained total across all participants. Test-only observability
    /// for the aggregate-bound assertions.
    #[cfg(test)]
    pub(crate) fn retained_total(&self) -> u64 {
        self.inner.total.load(Ordering::Acquire)
    }

    /// Account `bytes` newly retained. Called by `OutputLog::push`.
    pub(crate) fn add(&self, bytes: usize) {
        self.inner.total.fetch_add(bytes as u64, Ordering::AcqRel);
    }

    /// Account `bytes` no longer retained. Called by every `OutputLog` trim and
    /// by its `Drop`. Saturating so a double-decrement can never wrap the total
    /// into a huge value that would wedge reclamation on forever.
    pub(crate) fn sub(&self, bytes: usize) {
        let bytes = bytes as u64;
        let _ = self
            .inner
            .total
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_sub(bytes))
            });
    }

    /// Register one Run as an eviction participant. Stores a `Weak` handle and
    /// opportunistically prunes handles whose Runs have dropped, so the
    /// participant list stays proportional to live Runs without a sweeper.
    pub(crate) fn register(&self, victim: &Arc<dyn RetentionVictim + Send + Sync>) {
        let mut participants = lock(&self.inner.participants);
        participants.retain(|weak| weak.strong_count() > 0);
        participants.push(Arc::downgrade(victim));
    }

    /// Bring `total` back to at or below `limit` by trimming oldest bytes from
    /// the fattest, least-attached participants — skipping `except` (the Run
    /// that just pushed) so a newcomer's own bytes are not immediately clawed
    /// back while other Runs hold reclaimable history.
    ///
    /// Fast path is a single atomic load. The scan runs only over budget and is
    /// single-flighted; see the module docs for the lock discipline it obeys.
    pub(crate) fn reclaim_excess(&self, except: RunId) {
        // Cheap fast path: the common case is under budget, one relaxed load.
        if self.inner.total.load(Ordering::Acquire) <= self.inner.limit {
            return;
        }
        // Elect a single reclaimer. A loser returns immediately: the winner is
        // already shedding, and this push's own bytes are counted, so the
        // winner's target already accounts for them.
        let Ok(_flight) = self.inner.reclaiming.try_lock() else {
            return;
        };
        // Re-check under the flight lock: the winner of a prior race may have
        // already brought us back under budget.
        let over = self
            .inner
            .total
            .load(Ordering::Acquire)
            .saturating_sub(self.inner.limit);
        if over == 0 {
            return;
        }

        // Snapshot upgradeable victims and prune dead handles, holding the
        // participants lock only long enough to clone the Vec of strong refs —
        // never while locking a victim's output.
        let mut victims: Vec<Arc<dyn RetentionVictim + Send + Sync>> = {
            let mut participants = lock(&self.inner.participants);
            participants.retain(|weak| weak.strong_count() > 0);
            participants.iter().filter_map(Weak::upgrade).collect()
        };

        // Order victims: unattached before attached, then most-retained first,
        // then by RunId for determinism. The Run that just pushed sorts last so
        // it is the last resort, not the first casualty.
        victims.sort_by(|a, b| {
            let a_except = a.run_id() == except;
            let b_except = b.run_id() == except;
            a_except
                .cmp(&b_except)
                .then_with(|| a.is_attached().cmp(&b.is_attached()))
                .then_with(|| b.retained_output_bytes().cmp(&a.retained_output_bytes()))
                .then_with(|| a.run_id().to_string().cmp(&b.run_id().to_string()))
        });

        let mut remaining = over;
        for victim in victims {
            if remaining == 0 {
                break;
            }
            let drop_at_least = usize::try_from(remaining).unwrap_or(usize::MAX);
            let freed = victim.reclaim_output(drop_at_least) as u64;
            remaining = remaining.saturating_sub(freed);
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::{
        GATE_RETAINED_PLUS_OVERLAP_FLOOR_BYTES, RETENTION_BUDGET_BYTES, RetentionBudget,
        RetentionVictim,
    };
    use ctxmux_protocol::RunId;

    /// A minimal victim: a deque of chunk sizes standing in for `OutputLog`
    /// chunks, wired to a real `RetentionBudget` so the accounting math is
    /// exercised end to end without pulling `OutputLog` into this module.
    struct FakeVictim {
        id: RunId,
        attached: bool,
        chunks: Mutex<std::collections::VecDeque<usize>>,
        budget: RetentionBudget,
    }

    impl FakeVictim {
        fn new(budget: &RetentionBudget, attached: bool, chunks: &[usize]) -> std::sync::Arc<Self> {
            let deque: std::collections::VecDeque<usize> = chunks.iter().copied().collect();
            let total: usize = chunks.iter().sum();
            budget.add(total);
            std::sync::Arc::new(Self {
                id: RunId::new(),
                attached,
                chunks: Mutex::new(deque),
                budget: budget.clone(),
            })
        }

        fn retained(&self) -> usize {
            self.chunks.lock().unwrap().iter().copied().sum()
        }
    }

    impl RetentionVictim for FakeVictim {
        fn run_id(&self) -> RunId {
            self.id
        }

        fn retained_output_bytes(&self) -> usize {
            self.retained()
        }

        fn is_attached(&self) -> bool {
            self.attached
        }

        fn reclaim_output(&self, drop_at_least: usize) -> usize {
            let mut chunks = self.chunks.lock().unwrap();
            let mut freed = 0usize;
            // Mirror OutputLog: keep at least one chunk so the log is never
            // emptied below its final unit.
            while freed < drop_at_least && chunks.len() > 1 {
                if let Some(size) = chunks.pop_front() {
                    freed += size;
                }
            }
            self.budget.sub(freed);
            freed
        }
    }

    fn as_victim(
        victim: &std::sync::Arc<FakeVictim>,
    ) -> std::sync::Arc<dyn RetentionVictim + Send + Sync> {
        victim.clone()
    }

    #[test]
    fn budget_clears_the_frozen_gate_peak() {
        // The `const _` guard above enforces the ordering at compile time; this
        // pins the concrete numbers so a reader sees the relationship the guard
        // protects.
        assert_eq!(RETENTION_BUDGET_BYTES, 1024 * 1024 * 1024);
        assert_eq!(
            GATE_RETAINED_PLUS_OVERLAP_FLOOR_BYTES,
            136 * 4 * 1024 * 1024
        );
    }

    #[test]
    fn floor_tracks_the_frozen_gate_contract_not_a_local_literal() {
        // The floor is a *read* figure, not a measured one: it must equal the
        // value the reliability gate freezes, or the compile-time ordering guard
        // is protecting a number that has quietly drifted from what the gate
        // actually replays. Bind the literal to its single source of truth so
        // the two cannot diverge without this test failing.
        let contract: serde_json::Value =
            serde_json::from_str(include_str!("../../../reliability-gc-contract.json"))
                .expect("reliability-gc-contract.json parses");
        let frozen = contract["replay_pressure"]["retained_plus_overlap_payload_bytes"]
            .as_u64()
            .expect("retained_plus_overlap_payload_bytes is a u64");
        assert_eq!(
            GATE_RETAINED_PLUS_OVERLAP_FLOOR_BYTES, frozen,
            "the floor const drifted from the frozen gate contract"
        );
        // And the budget must clear that frozen peak (the guard, restated on the
        // contract-sourced value rather than the local copy).
        assert!(RETENTION_BUDGET_BYTES > frozen);
    }

    #[test]
    fn under_budget_reclamation_is_a_noop() {
        let budget = RetentionBudget::with_limit(1000);
        let victim = FakeVictim::new(&budget, false, &[100, 100, 100]);
        budget.register(&as_victim(&victim));
        assert_eq!(budget.retained_total(), 300);
        budget.reclaim_excess(RunId::new());
        assert_eq!(budget.retained_total(), 300);
        assert_eq!(victim.retained(), 300);
    }

    #[test]
    fn many_participants_together_cannot_exceed_the_total() {
        // The aggregate invariant: no matter how many Runs retain bytes, one
        // reclamation pass brings the sum back to at or below the limit.
        let budget = RetentionBudget::with_limit(1000);
        let mut victims = Vec::new();
        for _ in 0..10 {
            let victim = FakeVictim::new(&budget, false, &[100, 100, 100, 100]); // 400 each
            budget.register(&as_victim(&victim));
            victims.push(victim);
        }
        assert_eq!(budget.retained_total(), 4000);
        budget.reclaim_excess(RunId::new());
        assert!(
            budget.retained_total() <= 1000,
            "reclamation left {} bytes, above the 1000 limit",
            budget.retained_total()
        );
    }

    #[test]
    fn a_quiet_run_is_reclaimed_by_another_runs_pressure() {
        // The bug a naive per-Run design misses: a Run that filled up and went
        // silent must not pin memory forever. Here `quiet` never calls anything
        // itself; pressure from the others reclaims it. Chunks are fine-grained
        // so reclamation can shed below the per-Run last-chunk floor sum.
        let budget = RetentionBudget::with_limit(500);
        let quiet = FakeVictim::new(&budget, false, &[10; 60]); // 600, silent
        budget.register(&as_victim(&quiet));
        assert_eq!(quiet.retained(), 600);

        // Other Runs pile on until we are over budget, then one push reclaims.
        let mut others = Vec::new();
        for _ in 0..5 {
            let other = FakeVictim::new(&budget, false, &[10; 40]); // 400 each
            budget.register(&as_victim(&other));
            others.push(other);
        }
        assert_eq!(budget.retained_total(), 2600);
        // Reclamation triggered by an *active* Run (not `quiet`).
        budget.reclaim_excess(others[0].run_id());
        assert!(
            budget.retained_total() <= 500,
            "reclamation left {} bytes above the limit",
            budget.retained_total()
        );
        assert!(
            quiet.retained() < 600,
            "the quiet Run pinned {} bytes; global reclamation must trim it",
            quiet.retained()
        );
    }

    #[test]
    fn unattached_runs_are_evicted_before_attached_ones() {
        let budget = RetentionBudget::with_limit(300);
        let attached = FakeVictim::new(&budget, true, &[100, 100, 100]); // 300, being read
        let idle = FakeVictim::new(&budget, false, &[100, 100, 100]); // 300, quiet
        budget.register(&as_victim(&attached));
        budget.register(&as_victim(&idle));
        assert_eq!(budget.retained_total(), 600);

        budget.reclaim_excess(RunId::new());
        assert!(budget.retained_total() <= 300);
        // The quiet Run should have given up more than the actively-read one.
        assert!(
            idle.retained() <= attached.retained(),
            "attached Run kept {} but idle kept {}",
            attached.retained(),
            idle.retained()
        );
    }

    #[test]
    fn dropping_a_participant_releases_its_bytes() {
        let budget = RetentionBudget::with_limit(1_000_000);
        let victim = FakeVictim::new(&budget, false, &[100, 200, 300]);
        budget.register(&as_victim(&victim));
        assert_eq!(budget.retained_total(), 600);
        // A real OutputLog decrements in Drop; the fake mirrors that explicitly
        // to prove the accounting contract the log relies on.
        budget.sub(victim.retained());
        drop(victim);
        assert_eq!(budget.retained_total(), 0);
    }

    #[test]
    fn a_single_flight_loser_returns_without_blocking() {
        let budget = RetentionBudget::with_limit(100);
        let victim = FakeVictim::new(&budget, false, &[100, 100, 100]);
        budget.register(&as_victim(&victim));
        // Hold the flight lock to simulate another reclaimer in progress.
        let held = budget.inner.reclaiming.lock().unwrap();
        // Over budget, but the loser must not block waiting for the flight lock.
        budget.reclaim_excess(RunId::new());
        assert_eq!(
            budget.retained_total(),
            300,
            "a single-flight loser must not reclaim"
        );
        drop(held);
    }
}
