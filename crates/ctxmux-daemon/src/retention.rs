//! Bounded hot replay cache, configured independently of fleet qualification.
//! Oldest bytes of the largest unattached Runs are reclaimed first. Accounted
//! bytes follow each `OutputLog` through trim and drop; cursors report truncation.
//! No production budget is derived from, or forced above, a fixture workload.

use std::{
    cmp::Reverse,
    collections::HashMap,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicU64, Ordering},
    },
};

use ctxmux_protocol::RunId;

/// Default host replay cache allowance; operator policy can change it.
#[cfg(test)]
pub(crate) const RETENTION_BUDGET_BYTES: u64 = crate::ResourceLimits::DEFAULT.hot_output_bytes;

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
    /// None means its local owner is busy; it is not a zero-residency claim.
    fn retained_output_bytes(&self) -> Option<usize>;

    /// Already offered history that can fund a subsequent actual read.
    fn reclaimable_output_bytes(&self) -> usize;

    /// Whether a client is currently attached and replaying this Run. An
    /// attached Run is a worse eviction victim than a quiet one, but the replay
    /// contract already tolerates truncation, so this only *orders* victims.
    fn is_attached(&self) -> bool;

    /// Trim the requested reclaimable prefix, decrementing the shared total.
    /// Returns bytes actually reclaimed. A window may become empty while its
    /// independently stored lifetime head remains monotone.
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
    per_run_limit: usize,
    event_budget: crate::resources::ByteBudget,
    /// Sum of `retained_bytes` across every participating `OutputLog`. Mutated
    /// only by `OutputLog` accounting (add on push, sub on trim/drop).
    total: AtomicU64,
    /// Weak handles to every participating Run. Pruned opportunistically. Never
    /// strong: a strong ref would break collection's `strong_count == 1`
    /// eligibility.
    participants: Mutex<HashMap<RunId, Weak<dyn RetentionVictim + Send + Sync>>>,
    /// Single-flight gate: a burst of over-budget pushes elects one reclaimer
    /// via `try_lock`; the rest return without contending on the scan.
    reclaiming: Mutex<()>,
}

impl RetentionBudget {
    /// The production budget: [`RETENTION_BUDGET_BYTES`].
    #[cfg(test)]
    pub(crate) fn production() -> Self {
        Self::with_limits(
            crate::ResourceLimits::DEFAULT.hot_output_bytes,
            crate::ResourceLimits::DEFAULT.run_output_bytes,
        )
    }

    /// A budget with an explicit limit, for tests that must reach the ceiling
    /// without allocating a gigabyte.
    #[cfg(test)]
    pub(crate) fn with_limit(limit: u64) -> Self {
        Self::with_limits(limit, crate::ResourceLimits::DEFAULT.run_output_bytes)
    }

    pub(crate) fn per_run_limit(&self) -> usize {
        self.inner.per_run_limit
    }

    #[cfg(test)]
    pub(crate) fn with_limits(limit: u64, per_run_limit: usize) -> Self {
        Self::with_event_limits(
            limit,
            per_run_limit,
            crate::ResourceLimits::DEFAULT.live_event_bytes,
        )
    }

    pub(crate) fn with_resources(resources: crate::ResourceLimits) -> Self {
        Self::with_event_limits(
            resources.hot_output_bytes,
            resources.run_output_bytes,
            resources.live_event_bytes,
        )
    }

    pub(crate) fn event_budget(&self) -> crate::resources::ByteBudget {
        self.inner.event_budget.clone()
    }

    fn with_event_limits(limit: u64, per_run_limit: usize, event_bytes: u64) -> Self {
        Self {
            inner: Arc::new(Inner {
                limit,
                per_run_limit,
                event_budget: crate::resources::ByteBudget::new(event_bytes),
                total: AtomicU64::new(0),
                participants: Mutex::new(HashMap::new()),
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
        participants.insert(victim.run_id(), Arc::downgrade(victim));
    }

    /// Bring `total` back to at or below `limit` by trimming oldest bytes from
    /// the fattest, least-attached participants — skipping `except` (the Run
    /// that just pushed) so a newcomer's own bytes are not immediately clawed
    /// back while other Runs hold reclaimable history.
    ///
    /// Fast path is a single atomic load. The scan runs only over budget and is
    /// single-flighted; see the module docs for the lock discipline it obeys.
    pub(crate) fn unregister(&self, id: RunId) {
        lock(&self.inner.participants).remove(&id);
    }

    pub(crate) fn available_for_read(
        &self,
        except: RunId,
        wanted: usize,
        own_reclaimable: usize,
    ) -> usize {
        // Admission checks funding without evicting anything. EOF, EIO or an
        // interrupted read adds no bytes and must not discard existing history.
        // The one native reader records an actual read, then reclaims its exact
        // cost. Its fixed read buffer bounds the transient overlap.
        let wanted = (wanted as u64).min(self.inner.limit);
        let mut available = self
            .inner
            .limit
            .saturating_sub(self.retained_total())
            .saturating_add(own_reclaimable as u64);
        if available < wanted {
            let victims: Vec<_> = lock(&self.inner.participants)
                .values()
                .filter_map(Weak::upgrade)
                .collect();
            for victim in victims {
                if victim.run_id() != except {
                    available = available.saturating_add(victim.reclaimable_output_bytes() as u64);
                }
                if available >= wanted {
                    break;
                }
            }
        }
        usize::try_from(available.min(wanted)).unwrap_or(usize::MAX)
    }

    pub(crate) fn reclaim_excess(&self, except: RunId) {
        self.reclaim_to(except, self.inner.limit);
    }

    fn reclaim_to(&self, except: RunId, target: u64) {
        // Cheap fast path: the common case is under budget, one relaxed load.
        if self.inner.total.load(Ordering::Acquire) <= target {
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
            .saturating_sub(target);
        if over == 0 {
            return;
        }

        // Snapshot upgradeable victims and prune dead handles, holding the
        // participants lock only long enough to clone the Vec of strong refs —
        // never while locking a victim's output.
        let mut victims: Vec<Arc<dyn RetentionVictim + Send + Sync>> = {
            let mut participants = lock(&self.inner.participants);
            participants.retain(|_, weak| weak.strong_count() > 0);
            participants.values().filter_map(Weak::upgrade).collect()
        };

        // Attachment and try-lock observations can change between comparisons.
        // Cache each key once so sorting has a total order, without locking any
        // Run across the scan. Actual reclamation revalidates its own owner and
        // protected byte fence below. None remains after known byte counts.
        // Unattached/largest first, deterministic RunId ties, triggering Run last.
        victims.sort_by_cached_key(|victim| {
            let id = victim.run_id();
            (
                id == except,
                victim.is_attached(),
                Reverse(victim.retained_output_bytes()),
                id,
            )
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
    use std::{
        cmp::Reverse,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use super::{RETENTION_BUDGET_BYTES, RetentionBudget, RetentionVictim};
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

        fn retained_output_bytes(&self) -> Option<usize> {
            Some(self.retained())
        }

        fn reclaimable_output_bytes(&self) -> usize {
            self.retained()
        }

        fn is_attached(&self) -> bool {
            self.attached
        }

        fn reclaim_output(&self, drop_at_least: usize) -> usize {
            let mut chunks = self.chunks.lock().unwrap();
            let mut freed = 0usize;
            while freed < drop_at_least {
                if let Some(size) = chunks.pop_front() {
                    let shed = size.min(drop_at_least - freed);
                    if shed < size {
                        chunks.push_front(size - shed);
                    }
                    freed += shed;
                } else {
                    break;
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
    fn configured_budget_can_be_below_a_frozen_fixture_peak() {
        let budget = RetentionBudget::with_limits(4096, 512);
        assert_eq!(budget.limit(), 4096);
        assert_eq!(budget.per_run_limit(), 512);
        assert_eq!(
            RETENTION_BUDGET_BYTES,
            crate::ResourceLimits::DEFAULT.hot_output_bytes
        );
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

    #[test]
    fn changing_observations_cannot_change_one_reclamation_order() {
        // These observations have the same best-effort contract as a live
        // attachment counter and a busy/free output try-lock. Later reads may
        // disagree with the first; actual reclamation still owns its byte fence.
        struct ChangingVictim {
            victim: Arc<FakeVictim>,
            first_attached: bool,
            first_bytes: Option<usize>,
            attachment_reads: AtomicUsize,
            byte_reads: AtomicUsize,
            reclaimed: Arc<Mutex<Vec<RunId>>>,
        }
        impl RetentionVictim for ChangingVictim {
            fn run_id(&self) -> RunId {
                self.victim.run_id()
            }
            fn is_attached(&self) -> bool {
                if self.attachment_reads.fetch_add(1, Ordering::AcqRel) == 0 {
                    self.first_attached
                } else {
                    !self.first_attached
                }
            }
            fn retained_output_bytes(&self) -> Option<usize> {
                if self.byte_reads.fetch_add(1, Ordering::AcqRel) == 0 {
                    self.first_bytes
                } else {
                    self.first_bytes.is_none().then_some(usize::MAX)
                }
            }
            fn reclaimable_output_bytes(&self) -> usize {
                self.victim.reclaimable_output_bytes()
            }
            fn reclaim_output(&self, drop_at_least: usize) -> usize {
                self.reclaimed.lock().unwrap().push(self.run_id());
                self.victim.reclaim_output(drop_at_least)
            }
        }

        let budget = RetentionBudget::with_limit(1);
        let reclaimed = Arc::new(Mutex::new(Vec::new()));
        let victims: Vec<_> = (0..64)
            .map(|index| {
                let victim = Arc::new(ChangingVictim {
                    victim: FakeVictim::new(&budget, false, &[index + 2]),
                    first_attached: index % 3 == 0,
                    first_bytes: (index % 5 != 0).then_some(index % 7),
                    attachment_reads: AtomicUsize::new(0),
                    byte_reads: AtomicUsize::new(0),
                    reclaimed: Arc::clone(&reclaimed),
                });
                let participant: Arc<dyn RetentionVictim + Send + Sync> = victim.clone();
                budget.register(&participant);
                victim
            })
            .collect();
        let except = victims[7].run_id();
        let mut expected: Vec<_> = victims
            .iter()
            .map(|victim| {
                (
                    victim.run_id() == except,
                    victim.first_attached,
                    Reverse(victim.first_bytes),
                    victim.run_id(),
                )
            })
            .collect();
        expected.sort();

        budget.reclaim_excess(except);

        assert_eq!(
            budget.retained_total(),
            1,
            "actual byte accounting survives stale keys"
        );
        assert_eq!(
            *reclaimed.lock().unwrap(),
            expected.iter().map(|key| key.3).collect::<Vec<_>>(),
            "one decision uses the original observations, including busy None and except-last"
        );
        for victim in victims {
            assert_eq!(victim.attachment_reads.load(Ordering::Acquire), 1);
            assert_eq!(victim.byte_reads.load(Ordering::Acquire), 1);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[allow(clippy::too_many_lines)] // Keep the two-Run lifetime and cleanup in one proof.
    async fn two_real_runs_keep_serving_across_retention_pressure_and_reconnect() {
        use ctxmux_client::{Client, replay_bytes};
        use ctxmux_protocol::{
            DaemonInstanceId, InputOperationKey, NativeInputPhase, NativeOutputStatus,
            NativeOwnerStatus, RecoverableInput, RunInfo, RunSpec, TerminalSize,
        };
        use futures_util::FutureExt;
        use std::{panic::AssertUnwindSafe, time::Duration};

        async fn output_through(client: &Client, id: RunId, through: u64) -> RunInfo {
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let info = client.status(id).await.expect("public Run status");
                    if info.latest_output_bytes >= through {
                        return info;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .expect("real PTY output makes progress")
        }

        let manager = Arc::new(crate::RunManager::with_instance_stats_and_resources(
            DaemonInstanceId::new(),
            crate::QualificationStats::default(),
            crate::ResourceLimits {
                // The flood exceeds the shared bound but not the per-Run bound,
                // so a truncated raw tail proves shared reclamation ran.
                hot_output_bytes: 8192,
                run_output_bytes: 65536,
                ..crate::ResourceLimits::default()
            },
        ));
        let server = crate::tests::InProcessServer::start(Arc::clone(&manager));
        let mut children = Vec::new();
        let proof = AssertUnwindSafe(async {
            let client = &server.client;
            let runtime = client.runtime_info().await.expect("private Runtime");
            let spec = RunSpec {
                program: "python3".to_owned(),
                args: vec![
                    "-u".to_owned(),
                    "-c".to_owned(),
                    "import os,sys,tty\ntty.setraw(0)\nos.write(1,b'READY\\n')\nwhile True:\n b=os.read(0,1)\n if not b: break\n sys.stdout.buffer.write(b'x'*32768+b'\\nFLOOD\\n' if b==b'f' else b)\n sys.stdout.buffer.flush()\n".to_owned(),
                ],
                cwd: Some(server.directory.path().to_string_lossy().into_owned()),
                env: std::collections::BTreeMap::new(),
                initial_size: TerminalSize { rows: 4, cols: 80 },
                declared_inputs: Vec::new(),
            };
            let active = client.start(spec.clone()).await.expect("start flood Run");
            children.push(active.clone());
            let quiet = client.start(spec).await.expect("start unrelated Run");
            children.push(quiet.clone());
            assert_ne!(active.id, quiet.id);
            assert_ne!(active.pid, quiet.pid);
            assert!(active.pid.is_some() && quiet.pid.is_some());
            output_through(client, active.id, 6).await;
            output_through(client, quiet.id, 6).await;
            let (active_view, _) = client.attach(active.id, 0).await.expect("attach active");
            let (quiet_view, _) = client.attach(quiet.id, 0).await.expect("attach quiet");
            let observing = async {
                for _ in 0..20 {
                    let (view, _) = client.attach(quiet.id, 0).await.expect("concurrent attach");
                    view.detach().await.expect("concurrent detach");
                }
            };
            let pressure = async {
                let accepted = client
                    .recoverable_input(RecoverableInput {
                        daemon_instance: runtime.daemon_instance_id,
                        operation_key: InputOperationKey::new("retention-flood").unwrap(),
                        id: active.id,
                        expected_byte: 0,
                        data: b"f".to_vec(),
                    })
                    .await
                    .expect("flood input is applied");
                assert_eq!(accepted.receipt.start_byte, 0);
                assert_eq!(accepted.receipt.end_byte, 1);
                output_through(client, active.id, 6 + 32768 + 7).await;
            };
            tokio::join!(observing, pressure);
            active_view.detach().await.expect("detach active");
            quiet_view.detach().await.expect("detach quiet");

            let (tail_view, tail) = client.attach(active.id, 0).await.expect("raw retained tail");
            assert!(tail.replay.first_available_byte > 0, "shared budget reclaimed bytes");
            assert!(replay_bytes(&tail.replay.chunks).ends_with(b"\nFLOOD\n"));
            tail_view.detach().await.expect("detach tail");

            // A fresh public client observes the same Runs and exact input
            // cursor. Repeating one operation must not write it a second time.
            let reconnected = Client::new(server.directory.path().join("ctxmux.sock"));
            for (index, byte) in b"ab".iter().copied().enumerate() {
                let operation = RecoverableInput {
                    daemon_instance: runtime.daemon_instance_id,
                    operation_key: InputOperationKey::new(format!("retention-reconnect-{index}"))
                        .unwrap(),
                    id: quiet.id,
                    expected_byte: index as u64,
                    data: vec![byte],
                };
                let accepted = reconnected.recoverable_input(operation.clone()).await.unwrap();
                assert_eq!(accepted.receipt.start_byte, index as u64);
                assert_eq!(accepted.receipt.end_byte, index as u64 + 1);
                let repeated = reconnected.recoverable_input(operation).await.unwrap();
                assert_eq!(repeated.receipt, accepted.receipt);
                output_through(&reconnected, quiet.id, 7 + index as u64).await;
            }
            let (view, snapshot) = reconnected.attach(quiet.id, 6).await.unwrap();
            assert_eq!(replay_bytes(&snapshot.replay.chunks), b"ab");
            view.detach().await.unwrap();
            for original in [active, quiet] {
                let info = reconnected.status(original.id).await.unwrap();
                assert_eq!(info.pid, original.pid, "no replacement child");
                assert!(info.state.is_running());
                let service = info.native_service.expect("live native service");
                assert_eq!(service.owner, NativeOwnerStatus::Serving {});
                assert_eq!(service.output, NativeOutputStatus::Serving {});
                assert_eq!(service.input.phase, NativeInputPhase::Open {});
                eprintln!("retention public proof: id={} pid={:?} head={} owner={:?} input_bytes={:?}",
                    info.id, info.pid, info.latest_output_bytes, service.owner,
                    service.input.completed_input_bytes);
            }
        })
        .catch_unwind()
        .await;
        // This fixture owns only its private manager and children. Always reap
        // them, including after a failed assertion; never touch a user daemon.
        let mut cleanup_failures = Vec::new();
        for child in children {
            let id = child.id;
            match tokio::time::timeout(Duration::from_secs(5), server.client.stop_once(id)).await {
                Ok(Ok(stopped)) if !stopped.run.state.is_running() => {
                    eprintln!(
                        "retention cleanup proof: id={id} state={:?}",
                        stopped.run.state
                    );
                }
                result => cleanup_failures.push(format!("private Run {id}: {result:?}")),
            }
            // Even an unexpected shared-owner failure must not orphan this
            // fixture. Reuse retained-child test cleanup, without admitting
            // the failed public Stop as a passing result.
            let run = manager.get(id).unwrap();
            if let Some(crate::RunControl::Native(control)) = &run.incarnation_control
                && control.retains_failed_child()
            {
                let mut session = crate::native_session::NativeSession::from_child_pid(
                    child.pid.expect("actual native child"),
                )
                .unwrap();
                if let Err(error) = control.cleanup_retained_owner_child_for_test(&mut session) {
                    cleanup_failures.push(error);
                }
            }
            let pid = rustix::process::Pid::from_raw(
                i32::try_from(child.pid.expect("actual native child")).unwrap(),
            )
            .unwrap();
            if rustix::process::test_kill_process(pid) != Err(rustix::io::Errno::SRCH) {
                cleanup_failures.push(format!("private child {pid:?} still exists"));
            }
        }
        manager
            .shutdown_owned_controls(Duration::from_secs(5))
            .expect("clean up private PTY processes");
        assert!(cleanup_failures.is_empty(), "{cleanup_failures:?}");
        if let Err(panic) = proof {
            std::panic::resume_unwind(panic);
        }
    }
}
