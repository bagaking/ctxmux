use std::{
    collections::{HashMap, HashSet, hash_map::Entry, hash_map::RandomState},
    hash::{BuildHasher, Hasher},
    sync::{
        Arc, Condvar, Mutex, OnceLock, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use ctxmux_protocol::{
    CommandDisposition, ControlFailure, ControlReceipt, CreateOperationKey, ErrorCode,
    ForkFidelity, ForkPlan, ProtocolError, RunId, RunInfo, RunSpec, StopDisposition,
    StopOperationKey,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard, OwnedSemaphorePermit, Semaphore, watch};

use super::{Run, RunControl, STOP_ACK_TIMEOUT, control_not_applied, read_lock, write_lock};
use crate::native_control::{ControlResult, PendingStop};
use crate::qualification_stats::{Gauge as QualificationGauge, QualificationStats};

const CREATION_STRIPES: usize = 64;
// Matches the pre-registered resource start concurrency while bounding only
// transient physical launch owners; this is not a public Run quota.
const MAX_CREATION_OWNER_SLOTS: usize = 8;
pub(crate) const MAX_RETAINED_RUNS: usize = 128;
const CLEANUP_POLL: Duration = Duration::from_millis(20);

/// Total order of terminal state publication within one daemon incarnation.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct TerminalOrdinal(u64);

/// One short critical section that binds collection order to visible state.
///
/// A bare atomic counter is insufficient because a claimant can be descheduled
/// before it writes terminal `RunState`, allowing a later ordinal to publish
/// first. This owner never enters the Registry and therefore adds no
/// Run/control -> Registry lock edge.
#[derive(Clone, Default)]
pub(crate) struct TerminalPublicationOwner {
    next: Arc<Mutex<u64>>,
}

impl TerminalPublicationOwner {
    /// Restore one canonical historical terminal order before live publication.
    pub(crate) fn recover(&self, ordinal: &OnceLock<TerminalOrdinal>) {
        let mut next = self
            .next
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *next = next
            .checked_add(1)
            .expect("terminal ordinal does not overflow");
        ordinal
            .set(TerminalOrdinal(*next))
            .expect("one recovered Run receives one terminal ordinal");
    }

    pub(crate) fn publish(
        &self,
        ordinal: &OnceLock<TerminalOrdinal>,
        publish_state: impl FnOnce(),
    ) {
        let mut next = self
            .next
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *next = next
            .checked_add(1)
            .expect("terminal ordinal does not overflow");
        ordinal
            .set(TerminalOrdinal(*next))
            .expect("one Run publishes terminal state once");
        publish_state();
    }
}

/// Bounded shutdown ownership for short-lived physical Run creation threads.
///
/// This is deliberately not an executor or queue. A flight starts only after
/// its creation key is known to be unbound, and its guard follows that one OS
/// thread until launch and publication finish even if the requester cancels.
#[derive(Clone)]
pub(crate) struct CreationFlightOwner {
    inner: Arc<CreationFlightInner>,
    qualification_stats: QualificationStats,
}

struct CreationFlightInner {
    state: Mutex<CreationFlightState>,
    drained: Condvar,
    admission: Arc<Semaphore>,
}

struct CreationFlightState {
    accepting: bool,
    active: usize,
}

#[must_use = "dropping the flight releases shutdown ownership"]
pub(crate) struct CreationFlight {
    inner: Arc<CreationFlightInner>,
    admission: Option<OwnedSemaphorePermit>,
    qualification_stats: QualificationStats,
}

impl Default for CreationFlightOwner {
    fn default() -> Self {
        Self::with_stats(QualificationStats::default())
    }
}

impl CreationFlightOwner {
    pub(crate) fn with_stats(qualification_stats: QualificationStats) -> Self {
        Self {
            inner: Arc::new(CreationFlightInner {
                state: Mutex::new(CreationFlightState {
                    accepting: true,
                    active: 0,
                }),
                drained: Condvar::new(),
                admission: Arc::new(Semaphore::new(MAX_CREATION_OWNER_SLOTS)),
            }),
            qualification_stats,
        }
    }
    /// Wait asynchronously for one creation-thread admission slot.
    ///
    /// Cancellation while waiting releases no flight because none exists yet.
    /// Closing admission during shutdown wakes queued waiters with `None`. The
    /// separate cleanup reservation is the physical-overlap SSOT across active
    /// native or tmux publication and transferred cleanup.
    pub(crate) async fn acquire_admission(&self) -> Option<OwnedSemaphorePermit> {
        Arc::clone(&self.inner.admission).acquire_owned().await.ok()
    }

    /// Linearize one admitted launch against the shutdown fence.
    pub(crate) fn try_begin(&self, admission: OwnedSemaphorePermit) -> Option<CreationFlight> {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !state.accepting {
            return None;
        }
        state.active = state
            .active
            .checked_add(1)
            .expect("active creation flight count does not overflow");
        self.qualification_stats
            .set(QualificationGauge::CreationFlights, state.active);
        Some(CreationFlight {
            inner: Arc::clone(&self.inner),
            admission: Some(admission),
            qualification_stats: self.qualification_stats.clone(),
        })
    }

    /// Reject future unique launches while allowing retained-key lookups.
    pub(crate) fn fence(&self) {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.accepting = false;
        self.inner.admission.close();
    }

    #[cfg(test)]
    pub(crate) fn is_fenced(&self) -> bool {
        !self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .accepting
    }

    #[cfg(test)]
    pub(crate) fn active_count(&self) -> usize {
        self.inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .active
    }

    #[cfg(test)]
    pub(crate) fn available_admission(&self) -> usize {
        self.inner.admission.available_permits()
    }

    /// Wait for already-owned creation threads within the caller's deadline.
    pub(crate) fn wait_until(&self, deadline: Instant) -> bool {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while state.active != 0 {
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            let (next, timeout) = self
                .inner
                .drained
                .wait_timeout(state, deadline.saturating_duration_since(now))
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state = next;
            if timeout.timed_out() && state.active != 0 {
                return false;
            }
        }
        true
    }
}

impl Drop for CreationFlight {
    fn drop(&mut self) {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        debug_assert!(state.active > 0);
        state.active = state
            .active
            .checked_sub(1)
            .expect("a creation flight releases exactly one active owner");
        self.qualification_stats
            .set(QualificationGauge::CreationFlights, state.active);
        // Return admission while the active-count lock is held, so a woken
        // waiter cannot increment before this flight has decremented.
        drop(self.admission.take());
        if state.active == 0 {
            self.inner.drained.notify_all();
        }
    }
}

/// Bounded daemon-private ownership for native and tmux physical overlap.
///
/// A reservation is taken before physical launch and counts active publication
/// work as well as transferred cleanup under one `owned` ceiling. On ordinary
/// completion it disappears with the publication owner; on unresolved native
/// rollback it becomes an exact-key fence, while unresolved tmux import keeps
/// an unkeyed cleanup entry. Both retain the Run until their Backend-local
/// child, reader, waiter, writer, and control cleanup are proven. This is
/// neither a public pending Run nor durable transaction state.
pub(crate) struct UnpublishedCleanupOwner {
    inner: Arc<UnpublishedCleanupInner>,
    qualification_stats: QualificationStats,
}

#[derive(Default)]
struct UnpublishedCleanupInner {
    state: Mutex<UnpublishedCleanupState>,
}

#[derive(Default)]
struct UnpublishedCleanupState {
    owned: usize,
    entries: HashMap<CreateOperationKey, UnpublishedCleanupFence>,
    tmux_entries: Vec<TmuxCleanupEntry>,
}

struct UnpublishedCleanupFence {
    request: CreationRequest,
    owners: Vec<UnpublishedCleanupEntry>,
}

struct UnpublishedCleanupEntry {
    run: Arc<Run>,
    transfer_reason: String,
}

struct TmuxCleanupEntry {
    run: Arc<Run>,
    transfer_reason: String,
}

#[must_use = "dropping the reservation releases unpublished-cleanup capacity"]
pub(crate) struct UnpublishedCleanupReservation {
    inner: Arc<UnpublishedCleanupInner>,
    operation_key: Option<CreateOperationKey>,
    qualification_stats: QualificationStats,
}

#[must_use = "dropping the reservation releases tmux cleanup capacity"]
pub(crate) struct TmuxCleanupReservation {
    inner: Arc<UnpublishedCleanupInner>,
    active: bool,
    qualification_stats: QualificationStats,
}

/// Armed ownership for one native Run that has started but is not published.
///
/// Creation arms this owner immediately after spawn and native-control
/// construction. Persistent pre-COMMIT and commit-unknown separation belongs
/// to the later exact-replacement owner. Panic and owner-unwind paths request
/// cleanup and transfer the exact Run, request, key, and physical-overlap
/// reservation until full cleanup is proven. `Drop` never waits for cleanup or
/// reap completion.
#[must_use = "a started Run must be published or transferred for cleanup"]
pub(crate) struct PendingPublication {
    request: Option<CreationRequest>,
    run: Option<Arc<Run>>,
    cleanup_reservation: Option<UnpublishedCleanupReservation>,
}

impl UnpublishedCleanupOwner {
    pub(crate) fn with_stats(qualification_stats: QualificationStats) -> Self {
        Self {
            inner: Arc::default(),
            qualification_stats,
        }
    }

    /// Resolve an existing exact-key fence before launch admission can wait.
    pub(crate) fn resolve_fence(
        &self,
        operation_key: &CreateOperationKey,
        request: &CreationRequest,
    ) -> Result<(), ProtocolError> {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        prune_reaped(&mut state);
        sync_cleanup_stats(&self.qualification_stats, &state);
        if let Some(fence) = state.entries.get(operation_key) {
            return Err(if fence.request == *request {
                ProtocolError::new(
                    ErrorCode::BackendUnavailable,
                    "Run creation is fenced while unpublished native owners remain active",
                )
            } else {
                ProtocolError::new(
                    ErrorCode::CreationConflict,
                    "Run creation operation key is fenced for a different request",
                )
            });
        }
        Ok(())
    }

    /// Reserve physical-overlap and rollback ownership after creation-flight
    /// admission but before a child or creation thread starts.
    pub(crate) fn reserve(
        &self,
        operation_key: &CreateOperationKey,
    ) -> Result<UnpublishedCleanupReservation, ProtocolError> {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        prune_reaped(&mut state);
        sync_cleanup_stats(&self.qualification_stats, &state);
        if state.owned >= MAX_CREATION_OWNER_SLOTS {
            return Err(ProtocolError::new(
                ErrorCode::BackendUnavailable,
                "Run publication cleanup capacity is exhausted",
            ));
        }
        state.owned += 1;
        sync_cleanup_stats(&self.qualification_stats, &state);
        Ok(UnpublishedCleanupReservation {
            inner: Arc::clone(&self.inner),
            operation_key: Some(operation_key.clone()),
            qualification_stats: self.qualification_stats.clone(),
        })
    }

    /// Reserve the same physical-overlap budget for an unkeyed tmux import.
    pub(crate) fn reserve_tmux(&self) -> Result<TmuxCleanupReservation, ProtocolError> {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        prune_reaped(&mut state);
        sync_cleanup_stats(&self.qualification_stats, &state);
        if state.owned >= MAX_CREATION_OWNER_SLOTS {
            return Err(ProtocolError::new(
                ErrorCode::BackendUnavailable,
                "Run publication cleanup capacity is exhausted",
            ));
        }
        state.owned += 1;
        sync_cleanup_stats(&self.qualification_stats, &state);
        Ok(TmuxCleanupReservation {
            inner: Arc::clone(&self.inner),
            active: true,
            qualification_stats: self.qualification_stats.clone(),
        })
    }

    /// Wait for transferred waiters only; active creation threads remain owned
    /// by `CreationFlightOwner` and must drain before this is called.
    pub(crate) fn wait_until(&self, deadline: Instant) -> Vec<String> {
        loop {
            let pending = self.prune_and_report();
            if pending.is_empty() || Instant::now() >= deadline {
                return pending;
            }
            thread::sleep(CLEANUP_POLL.min(deadline.saturating_duration_since(Instant::now())));
        }
    }

    fn prune_and_report(&self) -> Vec<String> {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut reaped = 0;
        let mut pending = Vec::new();
        for fence in state.entries.values_mut() {
            fence
                .owners
                .retain(|entry| match entry.run.unpublished_cleanup_result() {
                    Ok(()) => {
                        reaped += 1;
                        false
                    }
                    Err(current) => {
                        pending.push(format!(
                            "unpublished Run {} exact-key fence: {}; {}",
                            entry.run.id, entry.transfer_reason, current
                        ));
                        true
                    }
                });
        }
        state.entries.retain(|_, fence| !fence.owners.is_empty());
        state
            .tmux_entries
            .retain(|entry| match tmux_cleanup_result(entry) {
                Ok(()) => {
                    reaped += 1;
                    false
                }
                Err(current) => {
                    pending.push(format!(
                        "unpublished tmux Run {} cleanup: {}; {}",
                        entry.run.id, entry.transfer_reason, current
                    ));
                    true
                }
            });
        state.owned = state
            .owned
            .checked_sub(reaped)
            .expect("proven cleanups release only owned slots");
        sync_cleanup_stats(&self.qualification_stats, &state);
        pending.sort();
        pending
    }

    #[cfg(test)]
    pub(crate) fn unresolved_count(&self) -> usize {
        self.prune_and_report().len()
    }

    #[cfg(test)]
    pub(crate) fn owned_count(&self) -> usize {
        self.inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .owned
    }
}

fn prune_reaped(state: &mut UnpublishedCleanupState) {
    let mut reaped = 0;
    for fence in state.entries.values_mut() {
        let before = fence.owners.len();
        fence
            .owners
            .retain(|entry| entry.run.unpublished_cleanup_result().is_err());
        reaped += before - fence.owners.len();
    }
    state.entries.retain(|_, fence| !fence.owners.is_empty());
    let before = state.tmux_entries.len();
    state
        .tmux_entries
        .retain(|entry| tmux_cleanup_result(entry).is_err());
    reaped += before - state.tmux_entries.len();
    state.owned = state
        .owned
        .checked_sub(reaped)
        .expect("proven cleanups release only owned slots");
}

fn tmux_cleanup_result(entry: &TmuxCleanupEntry) -> Result<(), String> {
    entry.run.tmux_unpublished_cleanup_result()?;
    let owners = Arc::strong_count(&entry.run);
    if owners == 1 {
        Ok(())
    } else {
        Err(format!(
            "tmux cleanup completion is recorded but {owners} Run owners remain"
        ))
    }
}

impl UnpublishedCleanupReservation {
    /// Install the exact-key fence before the creation stripe and launch permit
    /// can be released by their outer guards.
    pub(crate) fn transfer(
        mut self,
        request: CreationRequest,
        run: Arc<Run>,
        transfer_reason: String,
    ) {
        let operation_key = self
            .operation_key
            .take()
            .expect("cleanup reservation transfers at most once");
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let owner = UnpublishedCleanupEntry {
            run,
            transfer_reason,
        };
        match state.entries.entry(operation_key) {
            Entry::Vacant(entry) => {
                entry.insert(UnpublishedCleanupFence {
                    request,
                    owners: vec![owner],
                });
            }
            Entry::Occupied(mut entry) => {
                // This cannot occur while the exact-key stripe invariant holds.
                // Preserve both physical owners anyway: overwriting either one
                // would make a later reap of the other reopen the key unsafely.
                entry.get_mut().owners.push(owner);
            }
        }
        sync_cleanup_stats(&self.qualification_stats, &state);
    }
}

impl Drop for UnpublishedCleanupReservation {
    fn drop(&mut self) {
        if self.operation_key.take().is_none() {
            return;
        }
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.owned = state
            .owned
            .checked_sub(1)
            .expect("cleanup reservation releases exactly one owner slot");
        sync_cleanup_stats(&self.qualification_stats, &state);
    }
}

impl TmuxCleanupReservation {
    pub(crate) fn transfer(mut self, run: Arc<Run>, transfer_reason: String) {
        debug_assert!(
            self.active,
            "tmux cleanup reservation transfers at most once"
        );
        self.active = false;
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.tmux_entries.push(TmuxCleanupEntry {
            run,
            transfer_reason,
        });
        sync_cleanup_stats(&self.qualification_stats, &state);
    }
}

impl Drop for TmuxCleanupReservation {
    fn drop(&mut self) {
        if !std::mem::take(&mut self.active) {
            return;
        }
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.owned = state
            .owned
            .checked_sub(1)
            .expect("tmux cleanup reservation releases exactly one owner slot");
        sync_cleanup_stats(&self.qualification_stats, &state);
    }
}

impl Default for UnpublishedCleanupOwner {
    fn default() -> Self {
        Self::with_stats(QualificationStats::default())
    }
}

fn sync_cleanup_stats(telemetry: &QualificationStats, owner_state: &UnpublishedCleanupState) {
    telemetry.set(QualificationGauge::OverlapOwners, owner_state.owned);
    telemetry.set(
        QualificationGauge::CleanupOwners,
        owner_state
            .entries
            .values()
            .map(|fence| fence.owners.len())
            .sum::<usize>()
            + owner_state.tmux_entries.len(),
    );
}

impl PendingPublication {
    pub(crate) fn new(
        request: CreationRequest,
        run: Arc<Run>,
        cleanup_reservation: UnpublishedCleanupReservation,
    ) -> Self {
        Self {
            request: Some(request),
            run: Some(run),
            cleanup_reservation: Some(cleanup_reservation),
        }
    }

    pub(crate) fn run(&self) -> &Arc<Run> {
        self.run
            .as_ref()
            .expect("pending publication retains its Run until disposition")
    }

    /// Complete explicit rollback or transfer the still-owned publication.
    ///
    /// The caller receives the original cleanup failure while this owner keeps
    /// the exact request, key, Run, and overlap reservation fenced until every
    /// native owner is quiescent.
    pub(crate) fn cleanup_unpublished(mut self) -> Result<(), String> {
        match self.run().terminate_unpublished() {
            Ok(()) => {
                self.request.take();
                self.run.take();
                self.cleanup_reservation.take();
                Ok(())
            }
            Err(error) => {
                self.transfer(error.clone());
                Err(error)
            }
        }
    }

    /// Mark Registry publication while its exact write lock still owns truth.
    ///
    /// The returned reservation must be dropped after the Registry lock, so
    /// cleanup-owner accounting never nests under Registry ownership.
    fn into_published_reservation(mut self) -> UnpublishedCleanupReservation {
        self.request.take();
        self.run.take();
        self.cleanup_reservation
            .take()
            .expect("published Run releases one overlap reservation")
    }

    fn transfer(&mut self, transfer_reason: String) {
        let request = self
            .request
            .take()
            .expect("pending publication transfers its request at most once");
        let run = self
            .run
            .take()
            .expect("pending publication transfers its Run at most once");
        self.cleanup_reservation
            .take()
            .expect("pending publication transfers its overlap reservation at most once")
            .transfer(request, run, transfer_reason);
    }
}

impl Drop for PendingPublication {
    fn drop(&mut self) {
        if self.run.is_none() {
            return;
        }
        let cleanup_request = self.run().request_unpublished_cleanup().err().map_or_else(
            || "creation owner unwound after requesting unpublished child cleanup".to_owned(),
            |error| format!("creation owner unwound; cleanup request failed: {error}"),
        );
        self.transfer(cleanup_request);
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{Arc, Mutex, OnceLock, TryLockError, mpsc},
        thread,
        time::{Duration, Instant},
    };

    use ctxmux_protocol::{CreateOperationKey, ErrorCode, RunId, RunSpec, TerminalSize};

    use super::{
        CreationFlightOwner, CreationRequest, MAX_CREATION_OWNER_SLOTS, TerminalOrdinal,
        TerminalPublicationOwner, UnpublishedCleanupOwner, compare_memory_collection_candidates,
    };

    #[test]
    fn memory_collection_order_prefers_ordinal_then_run_id() {
        let lower: RunId =
            serde_json::from_str(r#""00000000-0000-0000-0000-000000000001""#).unwrap();
        let higher: RunId =
            serde_json::from_str(r#""00000000-0000-0000-0000-000000000002""#).unwrap();
        let ordinal = TerminalOrdinal(7);
        let lower_candidate = (ordinal, lower.to_string(), lower);
        let higher_candidate = (ordinal, higher.to_string(), higher);

        assert_eq!(
            compare_memory_collection_candidates(&lower_candidate, &higher_candidate),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            compare_memory_collection_candidates(
                &(TerminalOrdinal(6), higher.to_string(), higher),
                &lower_candidate,
            ),
            std::cmp::Ordering::Less
        );
    }

    #[test]
    fn terminal_ordinal_matches_visible_publication_order() {
        let owner = TerminalPublicationOwner::default();
        let first_ordinal = Arc::new(OnceLock::new());
        let second_ordinal = Arc::new(OnceLock::new());
        let visible = Arc::new(Mutex::new(Vec::new()));
        let (first_entered_tx, first_entered_rx) = mpsc::sync_channel(0);
        let (release_first_tx, release_first_rx) = mpsc::sync_channel(0);

        let first_owner = owner.clone();
        let first_cell = Arc::clone(&first_ordinal);
        let first_visible = Arc::clone(&visible);
        let first = thread::spawn(move || {
            first_owner.publish(&first_cell, || {
                first_entered_tx.send(()).expect("report first claimant");
                release_first_rx.recv().expect("release first claimant");
                first_visible.lock().unwrap().push("first");
            });
        });
        first_entered_rx.recv().expect("first claimant owns order");

        let publication_is_locked = matches!(owner.next.try_lock(), Err(TryLockError::WouldBlock));
        assert!(
            publication_is_locked,
            "the terminal owner remains locked through visible state publication"
        );

        release_first_tx
            .send(())
            .expect("release first publication");
        first.join().expect("first publisher remains live");
        owner.publish(&second_ordinal, || {
            visible.lock().unwrap().push("second");
        });
        assert_eq!(*visible.lock().unwrap(), ["first", "second"]);
        assert!(first_ordinal.get() < second_ordinal.get());
    }

    /// The terminal-ordinal single-set contract, asserted directly.
    ///
    /// A live re-adopted run (`Run::readopt`) defers its ordinal to `publish()`,
    /// run when the child later exits — it must NOT call `recover()`. A past bug
    /// had `readopt` calling `recover()` first; then the child's exit-time
    /// `publish()` would `set()` the same `OnceLock` a second time and panic the
    /// finalize worker — a panic otherwise swallowed by `let _ = worker.join()`.
    /// This contract is the last line of defense, so assert the double-`set()`
    /// panics explicitly here rather than relying on that swallowed worker.
    #[test]
    fn recover_then_publish_on_the_same_cell_panics_the_single_set_contract() {
        let owner = TerminalPublicationOwner::default();
        let cell = OnceLock::new();

        // The historical-restore path sets the ordinal now.
        owner.recover(&cell);
        assert!(
            cell.get().is_some(),
            "recover publishes the historical terminal ordinal"
        );

        // A subsequent publish() on the SAME cell is exactly what a live
        // re-adopted run would do at child-exit time; the second set() must
        // panic the single-set contract rather than silently overwrite. Suppress
        // the default hook so the deliberate panic does not spam test stderr.
        let previous_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            owner.publish(&cell, || {});
        }));
        std::panic::set_hook(previous_hook);

        let payload = result.expect_err("a second set() on one cell must panic");
        let message = payload
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| payload.downcast_ref::<&str>().copied())
            .unwrap_or_default();
        assert!(
            message.contains("publishes terminal state once"),
            "the panic must be the single-set contract failure, got: {message:?}"
        );
    }

    #[test]
    fn cleanup_reservations_enforce_and_reclaim_the_shared_owner_bound() {
        let owner = UnpublishedCleanupOwner::default();
        let request = CreationRequest::Start {
            spec: RunSpec {
                program: "/bin/true".to_owned(),
                args: Vec::new(),
                cwd: None,
                env: BTreeMap::new(),
                size: TerminalSize::default(),
                declared_inputs: Vec::new(),
            },
        };
        let tmux_reservation = owner
            .reserve_tmux()
            .expect("tmux import shares one physical-overlap slot");
        let mut reservations = Vec::new();
        for index in 0..(MAX_CREATION_OWNER_SLOTS - 1) {
            let key = CreateOperationKey::new(format!("cleanup-slot-{index}")).unwrap();
            owner.resolve_fence(&key, &request).unwrap();
            reservations.push(owner.reserve(&key).expect("reserve bounded cleanup slot"));
        }
        let ninth = CreateOperationKey::new("cleanup-slot-ninth").unwrap();
        owner.resolve_fence(&ninth, &request).unwrap();
        let error = owner
            .reserve(&ninth)
            .err()
            .expect("ninth cleanup reservation exceeds the hard bound");
        assert_eq!(error.code, ErrorCode::BackendUnavailable);
        assert_eq!(owner.owned_count(), MAX_CREATION_OWNER_SLOTS);

        drop(tmux_reservation);
        reservations.push(owner.reserve(&ninth).expect("released slot is reusable"));
        assert_eq!(owner.owned_count(), MAX_CREATION_OWNER_SLOTS);
    }

    #[tokio::test]
    async fn admission_caps_physical_launches_and_reclaims_released_permits() {
        let owner = Arc::new(CreationFlightOwner::default());
        let mut active = Vec::new();
        for _ in 0..MAX_CREATION_OWNER_SLOTS {
            let admission = owner
                .acquire_admission()
                .await
                .expect("claim one bounded launch permit");
            active.push(
                owner
                    .try_begin(admission)
                    .expect("admitted launch becomes an active flight"),
            );
        }
        assert_eq!(owner.active_count(), MAX_CREATION_OWNER_SLOTS);
        assert_eq!(owner.available_admission(), 0);

        let waiting_owner = Arc::clone(&owner);
        let mut ninth = tokio::spawn(async move {
            let admission = waiting_owner.acquire_admission().await?;
            waiting_owner.try_begin(admission)
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut ninth)
                .await
                .is_err(),
            "the ninth launch waits without becoming a flight"
        );
        assert_eq!(owner.active_count(), MAX_CREATION_OWNER_SLOTS);

        drop(active.pop());
        let ninth = tokio::time::timeout(Duration::from_secs(1), ninth)
            .await
            .expect("a released permit wakes the ninth launch")
            .expect("admission waiter task remains live")
            .expect("open admission produces a flight");
        assert_eq!(owner.active_count(), MAX_CREATION_OWNER_SLOTS);

        drop(ninth);
        drop(active);

        assert_eq!(owner.active_count(), 0);
        assert_eq!(owner.available_admission(), MAX_CREATION_OWNER_SLOTS);
    }

    #[tokio::test]
    async fn shutdown_fence_wakes_admission_waiters_and_drains_active_owners() {
        let owner = Arc::new(CreationFlightOwner::default());
        let mut active = Vec::new();
        for _ in 0..MAX_CREATION_OWNER_SLOTS {
            let admission = owner
                .acquire_admission()
                .await
                .expect("claim one bounded launch permit");
            active.push(
                owner
                    .try_begin(admission)
                    .expect("admitted launch becomes an active flight"),
            );
        }
        let waiting_owner = Arc::clone(&owner);
        let mut waiting = tokio::spawn(async move {
            let admission = waiting_owner.acquire_admission().await?;
            waiting_owner.try_begin(admission)
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut waiting)
                .await
                .is_err(),
            "the ninth launch is queued before shutdown"
        );

        owner.fence();

        assert!(
            tokio::time::timeout(Duration::from_secs(1), waiting)
                .await
                .expect("fence wakes the queued admission waiter")
                .expect("admission waiter task remains live")
                .is_none(),
            "shutdown rejects the queued unbound launch"
        );
        assert!(owner.acquire_admission().await.is_none());
        assert!(
            !owner.wait_until(Instant::now()),
            "shutdown cannot report drained while admitted owners remain active"
        );

        drop(active);

        assert!(
            owner.wait_until(Instant::now() + Duration::from_secs(1)),
            "dropping the active owner wakes the bounded shutdown drain"
        );
    }
}

/// Canonical typed meaning of one Start or Fork request after wire decoding.
///
/// This is deliberately private: it coordinates physical Run creation and is
/// neither a Session identity nor a generic transaction envelope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CreationRequest {
    Start { spec: RunSpec },
    Fork { parent: RunId, plan: ForkPlan },
}

struct StopOperationCell {
    result: watch::Sender<Option<ControlResult>>,
}

impl StopOperationCell {
    fn new() -> Self {
        let (result, _) = watch::channel(None);
        Self { result }
    }

    async fn wait(&self) -> ControlResult {
        let mut result = self.result.subscribe();
        loop {
            if let Some(result) = result.borrow().clone() {
                return result;
            }
            result
                .changed()
                .await
                .expect("retained Stop operation keeps its result owner");
        }
    }

    fn settle(&self, result: ControlResult) {
        let previous = self.result.send_replace(Some(result));
        debug_assert!(previous.is_none(), "one Stop operation settles once");
    }

    fn from_settled(result: ControlResult) -> Self {
        let (result, _) = watch::channel(Some(result));
        Self { result }
    }

    fn settled(&self) -> Option<ControlResult> {
        self.result.borrow().clone()
    }
}

struct StopOperationRecord {
    key: StopOperationKey,
    cell: Arc<StopOperationCell>,
}

/// Settled recoverable Stop truth carried only across a same-incarnation
/// exec-in-place handoff. Cold restart never loads this record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct HandoffStopOperation {
    pub(crate) run_id: RunId,
    pub(crate) operation_key: StopOperationKey,
    pub(crate) outcome: HandoffStopOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub(crate) enum HandoffStopOutcome {
    Accepted { disposition: StopDisposition },
    Unknown { failure: ControlFailure },
}

impl HandoffStopOperation {
    pub(crate) fn validate(&self) -> Result<(), String> {
        self.operation_key
            .validate()
            .map_err(|error| format!("invalid handoff native Stop key: {error}"))?;
        if let HandoffStopOutcome::Unknown { failure } = &self.outcome {
            if failure.disposition != CommandDisposition::Unknown {
                return Err("handoff native Stop failure has a non-unknown disposition".to_owned());
            }
            if failure.error.message.len()
                > crate::native_control::HANDOFF_INPUT_DIAGNOSTIC_MAX_BYTES
            {
                return Err("handoff native Stop diagnostic exceeds its bounded size".to_owned());
            }
        }
        Ok(())
    }
}

impl HandoffStopOutcome {
    fn into_result(self) -> ControlResult {
        match self {
            HandoffStopOutcome::Accepted { disposition } => {
                Ok(ControlReceipt::Stop { disposition })
            }
            HandoffStopOutcome::Unknown { failure } => Err(failure),
        }
    }
}

#[derive(Clone)]
pub(crate) struct RecoverableStopFlight {
    run: Arc<Run>,
    cell: Arc<StopOperationCell>,
}

impl RecoverableStopFlight {
    pub(crate) async fn resolve(self) -> (Arc<Run>, ControlResult) {
        let result = self.cell.wait().await;
        (self.run, result)
    }
}

/// Daemon-owned settlement work created only for the first Stop admission.
///
/// Connections receive only [`RecoverableStopFlight`]. Keeping the native
/// owner receiver here prevents attachment EOF or a dropped short response
/// from cancelling settlement.
pub(crate) struct RecoverableStopSettlement {
    id: RunId,
    key: StopOperationKey,
    cell: Arc<StopOperationCell>,
    _run: Arc<Run>,
    pending: Option<PendingStop>,
}

impl RecoverableStopSettlement {
    pub(crate) async fn wait(&mut self) -> ControlResult {
        self.pending
            .take()
            .expect("one recoverable Stop settlement waits once")
            .resolve(STOP_ACK_TIMEOUT)
            .await
    }
}

pub(crate) enum RecoverableStopAdmission {
    Owner {
        flight: RecoverableStopFlight,
        settlement: RecoverableStopSettlement,
    },
    Retry(RecoverableStopFlight),
}

impl CreationRequest {
    fn matches_run(&self, run: &Run) -> bool {
        match (self, run.spec.as_ref(), run.lineage.as_ref()) {
            (Self::Start { spec }, Some(run_spec), None) => spec == run_spec,
            (
                Self::Fork {
                    parent,
                    plan: ForkPlan::LevelA,
                },
                Some(_),
                Some(lineage),
            ) => lineage.parent == *parent && lineage.fidelity == ForkFidelity::LevelA,
            (
                Self::Fork {
                    parent,
                    plan: ForkPlan::LevelB { spec },
                },
                Some(run_spec),
                Some(lineage),
            ) => {
                lineage.parent == *parent
                    && lineage.fidelity == ForkFidelity::LevelB
                    && spec == run_spec
            }
            _ => false,
        }
    }
}

struct RegistryState {
    runs: HashMap<RunId, RegistryEntry>,
    creation_runs: HashMap<CreateOperationKey, RunId>,
    stop_runs: HashMap<StopOperationKey, RunId>,
    reservations: HashMap<PublicationTicket, RegistryReservation>,
    next_ticket: u64,
    record_capacity: usize,
}

impl Default for RegistryState {
    fn default() -> Self {
        Self {
            runs: HashMap::new(),
            creation_runs: HashMap::new(),
            stop_runs: HashMap::new(),
            reservations: HashMap::new(),
            next_ticket: 0,
            record_capacity: MAX_RETAINED_RUNS,
        }
    }
}

/// One Registry-owned Run identity and its optional exact creation mapping.
struct RegistryEntry {
    run: Arc<Run>,
    operation_key: Option<CreateOperationKey>,
    stop_operation: Option<StopOperationRecord>,
    metadata_bytes: Option<Arc<AtomicU64>>,
    residency: RegistryResidency,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RegistryResidency {
    Retained,
    Collecting(PublicationTicket),
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct PublicationTicket(u64);

struct RegistryReservation {
    new_run_id: RunId,
    operation_key: Option<CreateOperationKey>,
    request: Option<CreationRequest>,
    new_metadata_bytes: Option<u64>,
    candidates: Vec<RunId>,
}

fn compare_memory_collection_candidates(
    left: &(TerminalOrdinal, String, RunId),
    right: &(TerminalOrdinal, String, RunId),
) -> std::cmp::Ordering {
    left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1))
}

struct CandidateSelection {
    evaluated: usize,
    result: Result<Vec<RunId>, ProtocolError>,
}

fn select_publication_candidates(
    state: &RegistryState,
    new_metadata_bytes: Option<u64>,
) -> CandidateSelection {
    let projected_record_burden = state
        .reservations
        .values()
        .map(|reservation| usize::from(reservation.candidates.is_empty()))
        .sum::<usize>();
    let projected_records = state
        .runs
        .len()
        .checked_add(projected_record_burden)
        .expect("projected Registry count does not overflow");
    let projected_metadata = new_metadata_bytes.map(|_| projected_metadata_bytes(state));
    let metadata_to_fund =
        new_metadata_bytes
            .zip(projected_metadata)
            .map_or(0, |(new_bytes, projected)| {
                projected
                    .saturating_add(new_bytes)
                    .saturating_sub(super::persistence::METADATA_BYTES)
            });
    let needs_record = projected_records >= state.record_capacity;
    let mut evaluated = 0;
    let mut ordered = state
        .runs
        .iter()
        .filter_map(|(id, entry)| {
            evaluated += 1;
            if entry.residency != RegistryResidency::Retained
                || Arc::strong_count(&entry.run) != 1
                || (new_metadata_bytes.is_some() && entry.metadata_bytes.is_none())
            {
                return None;
            }
            entry
                .run
                .collection_ordinal()
                .map(|ordinal| (ordinal, id.to_string(), *id))
        })
        .collect::<Vec<_>>();
    ordered.sort_by(compare_memory_collection_candidates);

    let mut candidates = Vec::new();
    let mut candidate_metadata = 0_u64;
    for (_, _, id) in ordered {
        if (!needs_record || !candidates.is_empty()) && candidate_metadata >= metadata_to_fund {
            break;
        }
        candidate_metadata = candidate_metadata.saturating_add(
            state
                .runs
                .get(&id)
                .and_then(|entry| entry.metadata_bytes.as_ref())
                .map_or(0, |bytes| bytes.load(Ordering::Acquire)),
        );
        candidates.push(id);
    }
    if (needs_record && candidates.is_empty()) || candidate_metadata < metadata_to_fund {
        return CandidateSelection {
            evaluated,
            result: Err(ProtocolError::new(
                ErrorCode::RunCapacity,
                format!(
                    "retained Run capacity {} has no eligible exact replacement",
                    state.record_capacity
                ),
            )),
        };
    }
    if candidates.len() > 1
        && state
            .reservations
            .values()
            .any(|reservation| reservation.candidates.len() > 1)
    {
        return CandidateSelection {
            evaluated,
            result: Err(ProtocolError::new(
                ErrorCode::BackendUnavailable,
                "persistent metadata replacement is already reserved",
            )),
        };
    }
    CandidateSelection {
        evaluated,
        result: Ok(candidates),
    }
}

fn projected_metadata_bytes(state: &RegistryState) -> u64 {
    let retained = state
        .runs
        .values()
        .filter_map(|entry| entry.metadata_bytes.as_ref())
        .map(|bytes| bytes.load(Ordering::Acquire))
        .sum::<u64>();
    let reserved = state
        .reservations
        .values()
        .filter_map(|reservation| {
            reservation.new_metadata_bytes.map(|new_bytes| {
                let candidates = reservation
                    .candidates
                    .iter()
                    .filter_map(|id| state.runs.get(id))
                    .filter_map(|entry| entry.metadata_bytes.as_ref())
                    .map(|bytes| bytes.load(Ordering::Acquire))
                    .sum::<u64>();
                new_bytes.saturating_sub(candidates)
            })
        })
        .sum::<u64>();
    retained.saturating_add(reserved)
}

fn stop_operation_conflict(message: &'static str) -> ControlFailure {
    control_not_applied(ProtocolError::new(
        ErrorCode::StopOperationConflict,
        message,
    ))
}

fn reserve_registry_insertion_capacity(state: &mut RegistryState) -> Result<(), ProtocolError> {
    let pending_capacity = state.reservations.len().saturating_add(1);
    state.runs.try_reserve(pending_capacity).map_err(|error| {
        ProtocolError::new(
            ErrorCode::Internal,
            format!("failed to reserve Registry publication memory: {error}"),
        )
    })?;
    state
        .creation_runs
        .try_reserve(pending_capacity)
        .map_err(|error| {
            ProtocolError::new(
                ErrorCode::Internal,
                format!("failed to reserve creation-key publication memory: {error}"),
            )
        })
}

/// RAII ownership for one projected memory-only Registry publication.
///
/// Dropping an unconsumed reservation restores its exact collection fence.
/// Publication consumes the ticket in the same Registry write that removes
/// the candidate and inserts the new Run.
#[must_use = "a Registry publication reservation must be published or restored"]
pub(crate) struct PublicationReservation {
    state: Arc<RwLock<RegistryState>>,
    qualification_stats: QualificationStats,
    ticket: Option<PublicationTicket>,
    removed: Vec<RegistryEntry>,
}

/// Incarnation-local owner for a durable outcome `SQLite` could not classify.
/// Dropping this owner never restores candidates; daemon restart discards the
/// entire Registry and lets `SQLite` recovery decide the exact old-or-new unit.
pub(crate) struct CommitUnknownReservation {
    _state: Arc<RwLock<RegistryState>>,
    _ticket: PublicationTicket,
}

/// Byte-exact durable identity passed from the Registry fence to `SQLite`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PersistentCollectionCandidate {
    pub(crate) id: RunId,
    pub(crate) operation_key: CreateOperationKey,
    pub(crate) metadata_bytes: u64,
}

/// Single owner of retained Runs and their bounded creation-key mappings.
pub(crate) struct RunRegistry {
    state: Arc<RwLock<RegistryState>>,
    creation_stripes: [Arc<AsyncMutex<()>>; CREATION_STRIPES],
    creation_hash: RandomState,
    qualification_stats: QualificationStats,
}

impl Default for RunRegistry {
    fn default() -> Self {
        Self::with_stats(QualificationStats::default())
    }
}

impl RunRegistry {
    pub(crate) fn with_stats(qualification_stats: QualificationStats) -> Self {
        Self {
            state: Arc::new(RwLock::default()),
            creation_stripes: std::array::from_fn(|_| Arc::new(AsyncMutex::new(()))),
            creation_hash: RandomState::new(),
            qualification_stats,
        }
    }
    pub(crate) fn recovered_with_stats(
        runs: Vec<(CreateOperationKey, Arc<Run>, Arc<AtomicU64>)>,
        qualification_stats: QualificationStats,
    ) -> Self {
        let registry = Self::with_stats(qualification_stats);
        {
            let mut state = write_lock(&registry.state);
            for (operation_key, run, metadata_bytes) in runs {
                let id = run.id;
                let previous_run = state.runs.insert(
                    id,
                    RegistryEntry {
                        run,
                        operation_key: Some(operation_key.clone()),
                        stop_operation: None,
                        metadata_bytes: Some(metadata_bytes),
                        residency: RegistryResidency::Retained,
                    },
                );
                let previous_key = state.creation_runs.insert(operation_key, id);
                debug_assert!(previous_run.is_none());
                debug_assert!(previous_key.is_none());
            }
        }
        registry.sync_stats();
        registry
    }

    pub(crate) fn recovered_with_handoff_and_stats(
        runs: Vec<(CreateOperationKey, Arc<Run>, Arc<AtomicU64>)>,
        stop_operations: Vec<HandoffStopOperation>,
        qualification_stats: QualificationStats,
    ) -> Result<Self, ProtocolError> {
        let registry = Self::recovered_with_stats(runs, qualification_stats);
        registry.restore_handoff_stop_operations(stop_operations)?;
        Ok(registry)
    }

    fn restore_handoff_stop_operations(
        &self,
        stop_operations: Vec<HandoffStopOperation>,
    ) -> Result<(), ProtocolError> {
        let invalid = |message: String| ProtocolError::new(ErrorCode::InvalidRequest, message);
        let mut run_ids = HashSet::with_capacity(stop_operations.len());
        let mut keys = HashSet::with_capacity(stop_operations.len());
        let mut state = write_lock(&self.state);
        for operation in &stop_operations {
            operation.validate().map_err(invalid)?;
            if !run_ids.insert(operation.run_id) {
                return Err(invalid(
                    "handoff native Stop ledger contains a duplicate Run".to_owned(),
                ));
            }
            if !keys.insert(operation.operation_key.clone()) {
                return Err(invalid(
                    "handoff native Stop ledger contains a duplicate key".to_owned(),
                ));
            }
            let entry = state.runs.get(&operation.run_id).ok_or_else(|| {
                invalid("handoff native Stop ledger names an unknown retained Run".to_owned())
            })?;
            if entry.stop_operation.is_some() {
                return Err(invalid(
                    "handoff native Stop ledger duplicates a restored operation".to_owned(),
                ));
            }
        }
        state
            .stop_runs
            .try_reserve(stop_operations.len())
            .map_err(|error| {
                ProtocolError::new(
                    ErrorCode::Internal,
                    format!("failed to reserve handed-off native Stop keys: {error}"),
                )
            })?;
        for operation in stop_operations {
            let HandoffStopOperation {
                run_id: id,
                operation_key: key,
                outcome,
            } = operation;
            let cell = Arc::new(StopOperationCell::from_settled(outcome.into_result()));
            state
                .runs
                .get_mut(&id)
                .expect("validated handed-off Stop Run remains retained")
                .stop_operation = Some(StopOperationRecord {
                key: key.clone(),
                cell,
            });
            let previous = state.stop_runs.insert(key, id);
            debug_assert!(previous.is_none());
        }
        Ok(())
    }

    fn sync_stats(&self) {
        let state = read_lock(&self.state);
        sync_registry_stats(&self.qualification_stats, &state);
    }

    /// Acquire the bounded per-key owner before dispatching physical launch work.
    pub(crate) async fn lock_creation(
        &self,
        operation_key: &CreateOperationKey,
    ) -> OwnedMutexGuard<()> {
        let stripe = self.creation_stripe(operation_key);
        Arc::clone(&self.creation_stripes[stripe])
            .lock_owned()
            .await
    }

    fn creation_stripe(&self, operation_key: &CreateOperationKey) -> usize {
        let mut hasher = self.creation_hash.build_hasher();
        hasher.write(operation_key.as_str().as_bytes());
        usize::try_from(hasher.finish() % CREATION_STRIPES as u64)
            .expect("creation stripe index fits usize")
    }

    #[cfg(test)]
    pub(crate) fn shares_creation_stripe(
        &self,
        left: &CreateOperationKey,
        right: &CreateOperationKey,
    ) -> bool {
        self.creation_stripe(left) == self.creation_stripe(right)
    }

    /// Resolve a completed creation while its key stripe is exclusively held.
    pub(crate) fn resolve_creation_info(
        &self,
        operation_key: &CreateOperationKey,
        request: &CreationRequest,
    ) -> Result<Option<RunInfo>, ProtocolError> {
        let state = read_lock(&self.state);
        if let Some(reservation) = state
            .reservations
            .values()
            .find(|reservation| reservation.operation_key.as_ref() == Some(operation_key))
        {
            return Err(
                if reservation
                    .request
                    .as_ref()
                    .is_none_or(|owned| owned == request)
                {
                    ProtocolError::new(
                        ErrorCode::BackendUnavailable,
                        "Run creation is temporarily fenced by an unpublished Registry reservation",
                    )
                } else {
                    ProtocolError::new(
                        ErrorCode::CreationConflict,
                        "Run creation operation key is reserved for a different request",
                    )
                },
            );
        }
        let Some(id) = state.creation_runs.get(operation_key) else {
            return Ok(None);
        };
        let entry = state.runs.get(id).ok_or_else(|| {
            ProtocolError::new(
                ErrorCode::Internal,
                "Run creation registry lost its retained Run",
            )
        })?;
        if entry.operation_key.as_ref() != Some(operation_key) {
            return Err(ProtocolError::new(
                ErrorCode::Internal,
                "Run creation registry lost its exact key owner",
            ));
        }
        if !request.matches_run(&entry.run) {
            return Err(ProtocolError::new(
                ErrorCode::CreationConflict,
                "Run creation operation key is already bound to a different request",
            ));
        }
        match entry.residency {
            RegistryResidency::Retained => Ok(Some(entry.run.info())),
            RegistryResidency::Collecting(_) => Err(ProtocolError::new(
                ErrorCode::BackendUnavailable,
                "Run creation is temporarily unavailable while its retained owner is being replaced",
            )),
        }
    }

    /// Reserve one projected memory-only record before Backend mutation.
    ///
    /// The current Registry count and every uncommitted ticket are evaluated
    /// under the same write lock. A ticket may fund only its own publication
    /// by fencing one exact terminal candidate; its possible net release is
    /// never exposed as slack to another ticket.
    pub(crate) fn reserve_memory_publication(
        &self,
        new_run_id: RunId,
        operation_key: Option<CreateOperationKey>,
    ) -> Result<PublicationReservation, ProtocolError> {
        self.reserve_publication(new_run_id, operation_key, None, None)
    }

    /// Reserve persistent record and metadata capacity before `SQLite` or spawn.
    pub(crate) fn reserve_persistent_publication(
        &self,
        new_run_id: RunId,
        operation_key: CreateOperationKey,
        request: CreationRequest,
        new_metadata_bytes: u64,
    ) -> Result<PublicationReservation, ProtocolError> {
        self.reserve_publication(
            new_run_id,
            Some(operation_key),
            Some(request),
            Some(new_metadata_bytes),
        )
    }

    fn reserve_publication(
        &self,
        new_run_id: RunId,
        operation_key: Option<CreateOperationKey>,
        request: Option<CreationRequest>,
        new_metadata_bytes: Option<u64>,
    ) -> Result<PublicationReservation, ProtocolError> {
        let mut state = write_lock(&self.state);
        debug_assert!(!state.runs.contains_key(&new_run_id));
        debug_assert!(
            operation_key
                .as_ref()
                .is_none_or(|key| !state.creation_runs.contains_key(key))
        );

        let selection = select_publication_candidates(&state, new_metadata_bytes);
        self.qualification_stats
            .record_candidate_selection(selection.evaluated);
        let candidates = selection.result?;
        reserve_registry_insertion_capacity(&mut state)?;
        let mut removed = Vec::new();
        removed
            .try_reserve_exact(candidates.len())
            .map_err(|error| {
                ProtocolError::new(
                    ErrorCode::Internal,
                    format!("failed to reserve Registry replacement memory: {error}"),
                )
            })?;

        state.next_ticket = state
            .next_ticket
            .checked_add(1)
            .expect("Registry publication ticket does not overflow");
        let ticket = PublicationTicket(state.next_ticket);
        for candidate in &candidates {
            let entry = state
                .runs
                .get_mut(candidate)
                .expect("selected collection candidate remains retained");
            debug_assert_eq!(entry.residency, RegistryResidency::Retained);
            entry.residency = RegistryResidency::Collecting(ticket);
        }
        let previous = state.reservations.insert(
            ticket,
            RegistryReservation {
                new_run_id,
                operation_key,
                request,
                new_metadata_bytes,
                candidates: candidates.clone(),
            },
        );
        debug_assert!(previous.is_none());
        self.qualification_stats
            .record_candidate_fences(candidates.len());
        sync_registry_stats(&self.qualification_stats, &state);

        let detached = candidates
            .iter()
            .filter_map(|candidate| state.runs.get(candidate))
            .map(|entry| entry.run.detach_collection_descriptors())
            .collect::<Result<Vec<_>, _>>();
        let detached = match detached {
            Ok(detached) => detached,
            Err(error) => {
                restore_reservation(&mut state, ticket);
                sync_registry_stats(&self.qualification_stats, &state);
                return Err(ProtocolError::new(
                    ErrorCode::Internal,
                    format!("failed to fence retained Run replacement: {error}"),
                ));
            }
        };
        drop(state);
        drop(detached);

        Ok(PublicationReservation {
            state: Arc::clone(&self.state),
            qualification_stats: self.qualification_stats.clone(),
            ticket: Some(ticket),
            removed,
        })
    }

    /// Atomically publish one Run, its key, and the rollback-owner disposition.
    pub(crate) fn publish_creation(
        &self,
        operation_key: CreateOperationKey,
        pending: PendingPublication,
        reservation: Option<&mut PublicationReservation>,
    ) -> RunInfo {
        let run = Arc::clone(pending.run());
        let id = run.id;
        let activate_persistence =
            run.persistence_mode == super::PersistenceMode::PersistentCapable;
        let publication_run = Arc::clone(&run);
        let mut state = write_lock(&self.state);
        debug_assert!(!state.creation_runs.contains_key(&operation_key));
        debug_assert!(!state.runs.contains_key(&id));
        let (removed, entry_operation_key) = if let Some(reservation) = reservation {
            debug_assert!(Arc::ptr_eq(&self.state, &reservation.state));
            consume_reservation(&mut state, reservation, id, Some(&operation_key))
        } else {
            (Vec::new(), Some(operation_key.clone()))
        };
        state.runs.insert(
            id,
            RegistryEntry {
                run,
                operation_key: entry_operation_key,
                stop_operation: None,
                metadata_bytes: publication_run.persistent_metadata_owner(),
                residency: RegistryResidency::Retained,
            },
        );
        state.creation_runs.insert(operation_key, id);
        let cleanup_reservation = pending.into_published_reservation();
        self.qualification_stats
            .record_exact_replacements(removed.len());
        sync_registry_stats(&self.qualification_stats, &state);
        drop(state);
        drop(removed);
        drop(cleanup_reservation);
        if activate_persistence {
            publication_run.activate_persistence_after_publication();
        }
        publication_run.info()
    }

    /// Publish one non-creation-backed Run, currently only a tmux import or test seam.
    pub(crate) fn publish_unkeyed(&self, run: Arc<Run>, mut reservation: PublicationReservation) {
        let id = run.id;
        let mut state = write_lock(&self.state);
        debug_assert!(Arc::ptr_eq(&self.state, &reservation.state));
        let (removed, entry_operation_key) =
            consume_reservation(&mut state, &mut reservation, id, None);
        debug_assert!(entry_operation_key.is_none());
        let previous = state.runs.insert(
            id,
            RegistryEntry {
                run,
                operation_key: None,
                stop_operation: None,
                metadata_bytes: None,
                residency: RegistryResidency::Retained,
            },
        );
        debug_assert!(previous.is_none());
        self.qualification_stats
            .record_exact_replacements(removed.len());
        sync_registry_stats(&self.qualification_stats, &state);
        drop(state);
        drop(removed);
    }

    /// Atomically bind or recover one native Stop operation while the exact
    /// retained Run remains pinned. The Registry lock is the sole order
    /// between the Runtime-global key index, the per-Run record, and native
    /// Stop admission.
    pub(crate) fn begin_recoverable_stop(
        &self,
        id: RunId,
        key: StopOperationKey,
    ) -> Result<RecoverableStopAdmission, ControlFailure> {
        key.validate().map_err(|error| {
            control_not_applied(ProtocolError::new(
                ErrorCode::InvalidRequest,
                error.to_string(),
            ))
        })?;

        let mut state = write_lock(&self.state);
        if let Some(bound_id) = state.stop_runs.get(&key).copied() {
            if bound_id != id {
                return Err(stop_operation_conflict(
                    "native Stop operation key is already bound to another Run",
                ));
            }
            let entry = state.runs.get(&id).ok_or_else(|| {
                control_not_applied(ProtocolError::new(
                    ErrorCode::Internal,
                    "native Stop key index lost its retained Run",
                ))
            })?;
            if entry.residency != RegistryResidency::Retained {
                return Err(control_not_applied(ProtocolError::new(
                    ErrorCode::BackendUnavailable,
                    format!("Run {id} is being collected"),
                )));
            }
            let record = entry.stop_operation.as_ref().ok_or_else(|| {
                control_not_applied(ProtocolError::new(
                    ErrorCode::Internal,
                    "native Stop key index lost its per-Run operation",
                ))
            })?;
            if record.key != key {
                return Err(control_not_applied(ProtocolError::new(
                    ErrorCode::Internal,
                    "native Stop key index disagrees with its per-Run operation",
                )));
            }
            return Ok(RecoverableStopAdmission::Retry(RecoverableStopFlight {
                run: Arc::clone(&entry.run),
                cell: Arc::clone(&record.cell),
            }));
        }

        let entry = state.runs.get(&id).ok_or_else(|| {
            control_not_applied(ProtocolError::new(
                ErrorCode::RunNotFound,
                format!("Run {id} does not exist"),
            ))
        })?;
        if entry.residency != RegistryResidency::Retained {
            return Err(control_not_applied(ProtocolError::new(
                ErrorCode::BackendUnavailable,
                format!("Run {id} is being collected"),
            )));
        }
        if entry.stop_operation.is_some() {
            return Err(stop_operation_conflict(
                "Run already belongs to another native Stop operation",
            ));
        }
        state.stop_runs.try_reserve(1).map_err(|error| {
            control_not_applied(ProtocolError::new(
                ErrorCode::Internal,
                format!("failed to reserve native Stop key ownership: {error}"),
            ))
        })?;

        let entry = state
            .runs
            .get_mut(&id)
            .expect("validated retained Run remains Registry-owned");
        let pending = entry.run.begin_stop()?;
        let cell = Arc::new(StopOperationCell::new());
        entry.stop_operation = Some(StopOperationRecord {
            key: key.clone(),
            cell: Arc::clone(&cell),
        });
        let run = Arc::clone(&entry.run);
        let previous = state.stop_runs.insert(key.clone(), id);
        debug_assert!(previous.is_none());
        Ok(RecoverableStopAdmission::Owner {
            flight: RecoverableStopFlight {
                run: Arc::clone(&run),
                cell: Arc::clone(&cell),
            },
            settlement: RecoverableStopSettlement {
                id,
                key,
                cell,
                _run: run,
                pending: Some(pending),
            },
        })
    }

    pub(crate) fn settle_recoverable_stop(
        &self,
        settlement: RecoverableStopSettlement,
        result: ControlResult,
    ) {
        let RecoverableStopSettlement {
            id,
            key,
            cell,
            _run,
            pending: _,
        } = settlement;
        let remove = matches!(
            &result,
            Err(failure) if failure.disposition == CommandDisposition::NotApplied
        );
        let mut state = write_lock(&self.state);
        let entry = state
            .runs
            .get_mut(&id)
            .expect("in-flight Stop operation pins its retained Run");
        let record = entry
            .stop_operation
            .as_ref()
            .expect("in-flight Stop operation keeps its per-Run record");
        assert_eq!(record.key, key, "Stop settlement retains its exact key");
        assert!(
            Arc::ptr_eq(&record.cell, &cell),
            "Stop settlement retains its exact result cell"
        );
        if remove {
            entry.stop_operation = None;
            let mapped = state.stop_runs.remove(&key);
            debug_assert_eq!(mapped, Some(id));
        }
        cell.settle(result);
    }

    /// Snapshot only complete Stop operations for a same-incarnation planned
    /// exec. The request drain must have settled every admitted owner first;
    /// a pending cell aborts the reversible handoff phase.
    pub(crate) fn handoff_stop_operations(&self) -> Result<Vec<HandoffStopOperation>, String> {
        let state = read_lock(&self.state);
        let mut operations = Vec::with_capacity(state.stop_runs.len());
        for (id, entry) in &state.runs {
            let Some(record) = &entry.stop_operation else {
                continue;
            };
            if entry.residency != RegistryResidency::Retained {
                return Err(format!(
                    "Run {id} native Stop operation is crossing collection at handoff"
                ));
            }
            let result = record
                .cell
                .settled()
                .ok_or_else(|| format!("Run {id} has a pending recoverable Stop at handoff"))?;
            let outcome = match result {
                Ok(ControlReceipt::Stop { disposition }) => {
                    HandoffStopOutcome::Accepted { disposition }
                }
                Ok(_) => {
                    return Err(format!(
                        "Run {id} native Stop ledger retained another receipt kind"
                    ));
                }
                Err(failure) if failure.disposition == CommandDisposition::Unknown => {
                    HandoffStopOutcome::Unknown { failure }
                }
                Err(_) => {
                    return Err(format!(
                        "Run {id} native Stop ledger retained a not-applied result"
                    ));
                }
            };
            let operation = HandoffStopOperation {
                run_id: *id,
                operation_key: record.key.clone(),
                outcome,
            };
            operation.validate()?;
            operations.push(operation);
        }
        operations.sort_by_key(|operation| operation.run_id.to_string());
        Ok(operations)
    }

    /// Atomically clone a long-lived owner while the Registry still owns it.
    pub(crate) fn pin(&self, id: RunId) -> Result<Option<Arc<Run>>, ProtocolError> {
        let state = read_lock(&self.state);
        let Some(entry) = state.runs.get(&id) else {
            return Ok(None);
        };
        match entry.residency {
            RegistryResidency::Retained => Ok(Some(Arc::clone(&entry.run))),
            RegistryResidency::Collecting(_) => Err(ProtocolError::new(
                ErrorCode::BackendUnavailable,
                format!(
                    "Run {id} is temporarily unavailable while its retained owner is being replaced"
                ),
            )),
        }
    }

    /// Copy current public state without creating a long-lived Run owner.
    pub(crate) fn info(&self, id: RunId) -> Option<RunInfo> {
        read_lock(&self.state)
            .runs
            .get(&id)
            .map(|entry| entry.run.info())
    }

    /// Copy the public list without pinning every retained Run.
    pub(crate) fn list_infos(&self) -> Vec<RunInfo> {
        read_lock(&self.state)
            .runs
            .values()
            .map(|entry| entry.run.info())
            .collect()
    }

    /// Pin only tmux Runs that still belong to ordinary Registry shutdown.
    ///
    /// The later Collecting state will be filtered under this same lock rather
    /// than cloning the whole Registry before testing Backend ownership.
    pub(crate) fn pin_tmux_for_shutdown(&self) -> Vec<Arc<Run>> {
        read_lock(&self.state)
            .runs
            .values()
            .filter(|entry| {
                entry.residency == RegistryResidency::Retained
                    && matches!(&entry.run.incarnation_control, Some(RunControl::Tmux(_)))
            })
            .map(|entry| Arc::clone(&entry.run))
            .collect()
    }

    /// Copy operator-visible native wait-authority failures without taking a
    /// child owner or attempting cleanup.
    pub(crate) fn native_wait_failures(&self) -> Vec<(RunId, String)> {
        read_lock(&self.state)
            .runs
            .iter()
            .filter_map(|(id, entry)| match &entry.run.incarnation_control {
                Some(RunControl::Native(control)) => {
                    control.wait_authority_failure().map(|error| (*id, error))
                }
                Some(RunControl::Tmux(_)) | None => None,
            })
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn snapshot(&self) -> Vec<Arc<Run>> {
        read_lock(&self.state)
            .runs
            .values()
            .map(|entry| Arc::clone(&entry.run))
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn publish_unkeyed_for_test(&self, run: Arc<Run>) {
        let id = run.id;
        let mut state = write_lock(&self.state);
        state.runs.insert(
            id,
            RegistryEntry {
                run,
                operation_key: None,
                stop_operation: None,
                metadata_bytes: None,
                residency: RegistryResidency::Retained,
            },
        );
    }

    #[cfg(test)]
    pub(crate) fn with_record_capacity(record_capacity: usize) -> Self {
        let registry = Self::default();
        write_lock(&registry.state).record_capacity = record_capacity;
        registry
    }

    #[cfg(test)]
    pub(crate) fn set_record_capacity_for_test(&self, record_capacity: usize) {
        write_lock(&self.state).record_capacity = record_capacity;
    }
}

impl PublicationReservation {
    pub(crate) fn persistent_candidates(&self) -> Vec<PersistentCollectionCandidate> {
        let ticket = self
            .ticket
            .expect("active publication reservation owns one ticket");
        let state = read_lock(&self.state);
        let reservation = state
            .reservations
            .get(&ticket)
            .expect("active publication ticket remains registered");
        debug_assert!(reservation.new_metadata_bytes.is_some());
        reservation
            .candidates
            .iter()
            .map(|id| {
                let entry = state
                    .runs
                    .get(id)
                    .expect("persistent candidate remains Registry-owned");
                PersistentCollectionCandidate {
                    id: *id,
                    operation_key: entry
                        .operation_key
                        .clone()
                        .expect("persistent native Run has an exact creation key"),
                    metadata_bytes: entry
                        .metadata_bytes
                        .as_ref()
                        .expect("persistent candidate has metadata accounting")
                        .load(Ordering::Acquire),
                }
            })
            .collect()
    }

    pub(crate) fn into_commit_unknown(mut self) -> Option<CommitUnknownReservation> {
        self.ticket.take().map(|ticket| CommitUnknownReservation {
            _state: Arc::clone(&self.state),
            _ticket: ticket,
        })
    }
}

impl Drop for PublicationReservation {
    fn drop(&mut self) {
        let Some(ticket) = self.ticket.take() else {
            return;
        };
        let mut state = write_lock(&self.state);
        restore_reservation(&mut state, ticket);
        sync_registry_stats(&self.qualification_stats, &state);
    }
}

fn restore_reservation(state: &mut RegistryState, ticket: PublicationTicket) {
    let Some(reservation) = state.reservations.remove(&ticket) else {
        debug_assert!(false, "active publication ticket remains registered");
        return;
    };
    for candidate in reservation.candidates {
        let entry = state
            .runs
            .get_mut(&candidate)
            .expect("uncommitted candidate remains in the Registry");
        debug_assert_eq!(entry.residency, RegistryResidency::Collecting(ticket));
        entry.residency = RegistryResidency::Retained;
    }
}

fn sync_registry_stats(telemetry: &QualificationStats, registry_state: &RegistryState) {
    let collecting = registry_state
        .runs
        .values()
        .filter_map(|entry| match entry.residency {
            RegistryResidency::Retained => None,
            RegistryResidency::Collecting(ticket) => Some(ticket),
        })
        .collect::<std::collections::HashSet<_>>()
        .len();
    telemetry.set_many(&[
        (QualificationGauge::RetainedRuns, registry_state.runs.len()),
        (
            QualificationGauge::CreationKeys,
            registry_state.creation_runs.len(),
        ),
        (
            QualificationGauge::PublicationReservations,
            registry_state.reservations.len(),
        ),
        (QualificationGauge::CollectingTickets, collecting),
    ]);
}

fn consume_reservation(
    state: &mut RegistryState,
    reservation_owner: &mut PublicationReservation,
    new_run_id: RunId,
    operation_key: Option<&CreateOperationKey>,
) -> (Vec<RegistryEntry>, Option<CreateOperationKey>) {
    let ticket = reservation_owner
        .ticket
        .take()
        .expect("publication consumes one active Registry ticket");
    let reservation = state
        .reservations
        .remove(&ticket)
        .expect("publication ticket remains registered");
    debug_assert_eq!(reservation.new_run_id, new_run_id);
    debug_assert_eq!(reservation.operation_key.as_ref(), operation_key);
    debug_assert!(reservation_owner.removed.is_empty());
    debug_assert!(reservation_owner.removed.capacity() >= reservation.candidates.len());
    for candidate in reservation.candidates {
        let removed = state
            .runs
            .remove(&candidate)
            .expect("publication removes its exact fenced candidate");
        debug_assert_eq!(removed.residency, RegistryResidency::Collecting(ticket));
        if let Some(candidate_key) = &removed.operation_key {
            let mapped = state.creation_runs.remove(candidate_key);
            debug_assert_eq!(mapped, Some(candidate));
        }
        if let Some(stop_operation) = &removed.stop_operation {
            let mapped = state.stop_runs.remove(&stop_operation.key);
            debug_assert_eq!(mapped, Some(candidate));
        }
        reservation_owner.removed.push(removed);
    }
    (
        std::mem::take(&mut reservation_owner.removed),
        reservation.operation_key,
    )
}
