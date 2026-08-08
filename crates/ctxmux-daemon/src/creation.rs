use std::{
    collections::{HashMap, hash_map::Entry, hash_map::RandomState},
    hash::{BuildHasher, Hasher},
    sync::{Arc, Condvar, Mutex, OnceLock, RwLock},
    thread,
    time::{Duration, Instant},
};

use ctxmux_protocol::{
    CreateOperationKey, ErrorCode, ForkFidelity, ForkPlan, ProtocolError, RunId, RunInfo, RunSpec,
};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard, OwnedSemaphorePermit, Semaphore};

use super::{Run, RunControl, read_lock, write_lock};

const CREATION_STRIPES: usize = 64;
// Matches the pre-registered resource start concurrency while bounding only
// transient physical launch owners; this is not a public Run quota.
const MAX_CREATION_OWNER_SLOTS: usize = 8;
const MAX_RETAINED_RUNS: usize = 128;
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
pub(crate) struct CreationFlightOwner {
    inner: Arc<CreationFlightInner>,
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
}

impl Default for CreationFlightOwner {
    fn default() -> Self {
        Self {
            inner: Arc::new(CreationFlightInner {
                state: Mutex::new(CreationFlightState {
                    accepting: true,
                    active: 0,
                }),
                drained: Condvar::new(),
                admission: Arc::new(Semaphore::new(MAX_CREATION_OWNER_SLOTS)),
            }),
        }
    }
}

impl CreationFlightOwner {
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
        Some(CreationFlight {
            inner: Arc::clone(&self.inner),
            admission: Some(admission),
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
#[derive(Default)]
pub(crate) struct UnpublishedCleanupOwner {
    inner: Arc<UnpublishedCleanupInner>,
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
}

#[must_use = "dropping the reservation releases tmux cleanup capacity"]
pub(crate) struct TmuxCleanupReservation {
    inner: Arc<UnpublishedCleanupInner>,
    active: bool,
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
        if state.owned >= MAX_CREATION_OWNER_SLOTS {
            return Err(ProtocolError::new(
                ErrorCode::BackendUnavailable,
                "Run publication cleanup capacity is exhausted",
            ));
        }
        state.owned += 1;
        Ok(UnpublishedCleanupReservation {
            inner: Arc::clone(&self.inner),
            operation_key: Some(operation_key.clone()),
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
        if state.owned >= MAX_CREATION_OWNER_SLOTS {
            return Err(ProtocolError::new(
                ErrorCode::BackendUnavailable,
                "Run publication cleanup capacity is exhausted",
            ));
        }
        state.owned += 1;
        Ok(TmuxCleanupReservation {
            inner: Arc::clone(&self.inner),
            active: true,
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
    }
}

impl TmuxCleanupReservation {
    pub(crate) fn transfer(mut self, run: Arc<Run>, transfer_reason: String) {
        debug_assert!(
            self.active,
            "tmux cleanup reservation transfers at most once"
        );
        self.active = false;
        self.inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .tmux_entries
            .push(TmuxCleanupEntry {
                run,
                transfer_reason,
            });
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
    }
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

    use ctxmux_protocol::{CreateOperationKey, ErrorCode, RunSpec, TerminalSize};

    use super::{
        CreationFlightOwner, CreationRequest, MAX_CREATION_OWNER_SLOTS, TerminalPublicationOwner,
        UnpublishedCleanupOwner,
    };

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
    reservations: HashMap<PublicationTicket, RegistryReservation>,
    next_ticket: u64,
    record_capacity: usize,
}

impl Default for RegistryState {
    fn default() -> Self {
        Self {
            runs: HashMap::new(),
            creation_runs: HashMap::new(),
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
    candidate: Option<RunId>,
}

/// RAII ownership for one projected memory-only Registry publication.
///
/// Dropping an unconsumed reservation restores its exact collection fence.
/// Publication consumes the ticket in the same Registry write that removes
/// the candidate and inserts the new Run.
#[must_use = "a Registry publication reservation must be published or restored"]
pub(crate) struct MemoryPublicationReservation {
    state: Arc<RwLock<RegistryState>>,
    ticket: Option<PublicationTicket>,
}

/// Single owner of retained Runs and their bounded creation-key mappings.
pub(crate) struct RunRegistry {
    state: Arc<RwLock<RegistryState>>,
    creation_stripes: [Arc<AsyncMutex<()>>; CREATION_STRIPES],
    creation_hash: RandomState,
}

impl Default for RunRegistry {
    fn default() -> Self {
        Self {
            state: Arc::new(RwLock::default()),
            creation_stripes: std::array::from_fn(|_| Arc::new(AsyncMutex::new(()))),
            creation_hash: RandomState::new(),
        }
    }
}

impl RunRegistry {
    pub(crate) fn recovered(runs: Vec<(CreateOperationKey, Arc<Run>)>) -> Self {
        let registry = Self::default();
        {
            let mut state = write_lock(&registry.state);
            for (operation_key, run) in runs {
                let id = run.id;
                let previous_run = state.runs.insert(
                    id,
                    RegistryEntry {
                        run,
                        operation_key: Some(operation_key.clone()),
                        residency: RegistryResidency::Retained,
                    },
                );
                let previous_key = state.creation_runs.insert(operation_key, id);
                debug_assert!(previous_run.is_none());
                debug_assert!(previous_key.is_none());
            }
        }
        registry
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
    ) -> Result<MemoryPublicationReservation, ProtocolError> {
        let mut state = write_lock(&self.state);
        debug_assert!(!state.runs.contains_key(&new_run_id));
        debug_assert!(
            operation_key
                .as_ref()
                .is_none_or(|key| !state.creation_runs.contains_key(key))
        );

        let projected_burden = state
            .reservations
            .values()
            .filter(|reservation| reservation.candidate.is_none())
            .count();
        let projected_records = state
            .runs
            .len()
            .checked_add(projected_burden)
            .expect("projected Registry count does not overflow");
        let candidate = if projected_records < state.record_capacity {
            None
        } else {
            Some(
                state
                    .runs
                    .iter()
                    .filter_map(|(id, entry)| {
                        if entry.residency != RegistryResidency::Retained
                            || Arc::strong_count(&entry.run) != 1
                        {
                            return None;
                        }
                        entry
                            .run
                            .memory_collection_ordinal()
                            .map(|ordinal| (ordinal, id.to_string(), *id))
                    })
                    .min_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)))
                    .map(|(_, _, id)| id)
                    .ok_or_else(|| {
                        ProtocolError::new(
                            ErrorCode::RunCapacity,
                            format!(
                                "retained Run capacity {} has no eligible terminal replacement",
                                state.record_capacity
                            ),
                        )
                    })?,
            )
        };

        state.next_ticket = state
            .next_ticket
            .checked_add(1)
            .expect("Registry publication ticket does not overflow");
        let ticket = PublicationTicket(state.next_ticket);
        if let Some(candidate) = candidate {
            let entry = state
                .runs
                .get_mut(&candidate)
                .expect("selected collection candidate remains retained");
            debug_assert_eq!(entry.residency, RegistryResidency::Retained);
            entry.residency = RegistryResidency::Collecting(ticket);
        }
        let previous = state.reservations.insert(
            ticket,
            RegistryReservation {
                new_run_id,
                operation_key,
                candidate,
            },
        );
        debug_assert!(previous.is_none());

        let detached = candidate
            .and_then(|candidate| state.runs.get(&candidate))
            .map(|entry| entry.run.detach_memory_collection_descriptors())
            .transpose()
            .map_err(|error| {
                restore_reservation(&mut state, ticket);
                ProtocolError::new(
                    ErrorCode::Internal,
                    format!("failed to fence retained Run replacement: {error}"),
                )
            })?
            .flatten();
        drop(state);
        drop(detached);

        Ok(MemoryPublicationReservation {
            state: Arc::clone(&self.state),
            ticket: Some(ticket),
        })
    }

    /// Atomically publish one Run, its key, and the rollback-owner disposition.
    pub(crate) fn publish_creation(
        &self,
        operation_key: CreateOperationKey,
        pending: PendingPublication,
        reservation: Option<MemoryPublicationReservation>,
    ) -> RunInfo {
        let run = Arc::clone(pending.run());
        let info = run.info();
        let id = run.id;
        let mut state = write_lock(&self.state);
        debug_assert!(!state.creation_runs.contains_key(&operation_key));
        debug_assert!(!state.runs.contains_key(&id));
        let removed = if let Some(mut reservation) = reservation {
            debug_assert!(Arc::ptr_eq(&self.state, &reservation.state));
            consume_reservation(&mut state, &mut reservation, id, Some(&operation_key))
        } else {
            None
        };
        state.runs.insert(
            id,
            RegistryEntry {
                run,
                operation_key: Some(operation_key.clone()),
                residency: RegistryResidency::Retained,
            },
        );
        state.creation_runs.insert(operation_key, id);
        let cleanup_reservation = pending.into_published_reservation();
        drop(state);
        drop(removed);
        drop(cleanup_reservation);
        info
    }

    /// Publish one non-creation-backed Run, currently only a tmux import or test seam.
    pub(crate) fn publish_unkeyed(
        &self,
        run: Arc<Run>,
        mut reservation: MemoryPublicationReservation,
    ) {
        let id = run.id;
        let mut state = write_lock(&self.state);
        debug_assert!(Arc::ptr_eq(&self.state, &reservation.state));
        let removed = consume_reservation(&mut state, &mut reservation, id, None);
        let previous = state.runs.insert(
            id,
            RegistryEntry {
                run,
                operation_key: None,
                residency: RegistryResidency::Retained,
            },
        );
        debug_assert!(previous.is_none());
        drop(state);
        drop(removed);
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

    #[cfg(test)]
    pub(crate) fn snapshot(&self) -> Vec<Arc<Run>> {
        read_lock(&self.state)
            .runs
            .values()
            .map(|entry| Arc::clone(&entry.run))
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn with_record_capacity(record_capacity: usize) -> Self {
        let registry = Self::default();
        write_lock(&registry.state).record_capacity = record_capacity;
        registry
    }
}

impl Drop for MemoryPublicationReservation {
    fn drop(&mut self) {
        let Some(ticket) = self.ticket.take() else {
            return;
        };
        restore_reservation(&mut write_lock(&self.state), ticket);
    }
}

fn restore_reservation(state: &mut RegistryState, ticket: PublicationTicket) {
    let Some(reservation) = state.reservations.remove(&ticket) else {
        debug_assert!(false, "active publication ticket remains registered");
        return;
    };
    if let Some(candidate) = reservation.candidate {
        let entry = state
            .runs
            .get_mut(&candidate)
            .expect("uncommitted candidate remains in the Registry");
        debug_assert_eq!(entry.residency, RegistryResidency::Collecting(ticket));
        entry.residency = RegistryResidency::Retained;
    }
}

fn consume_reservation(
    state: &mut RegistryState,
    reservation_owner: &mut MemoryPublicationReservation,
    new_run_id: RunId,
    operation_key: Option<&CreateOperationKey>,
) -> Option<RegistryEntry> {
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
    reservation.candidate.map(|candidate| {
        let removed = state
            .runs
            .remove(&candidate)
            .expect("publication removes its exact fenced candidate");
        debug_assert_eq!(removed.residency, RegistryResidency::Collecting(ticket));
        if let Some(candidate_key) = &removed.operation_key {
            let mapped = state.creation_runs.remove(candidate_key);
            debug_assert_eq!(mapped, Some(candidate));
        }
        removed
    })
}
