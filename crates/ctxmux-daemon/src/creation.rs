use std::{
    collections::{HashMap, hash_map::Entry, hash_map::RandomState},
    hash::{BuildHasher, Hasher},
    sync::{Arc, Condvar, Mutex, RwLock},
    thread,
    time::{Duration, Instant},
};

use ctxmux_protocol::{
    CreateOperationKey, ErrorCode, ForkFidelity, ForkPlan, ProtocolError, RunId, RunInfo, RunSpec,
};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard, OwnedSemaphorePermit, Semaphore};

use super::{Run, read_lock, write_lock};

const CREATION_STRIPES: usize = 64;
// Matches the pre-registered resource start concurrency while bounding only
// transient physical launch owners; this is not a public Run quota.
const MAX_CREATION_OWNER_SLOTS: usize = 8;
const CLEANUP_POLL: Duration = Duration::from_millis(20);

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
    /// native launch and transferred cleanup.
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

/// Bounded daemon-private ownership for native physical overlap.
///
/// A reservation is taken before physical launch and counts active publication
/// work as well as transferred cleanup under one `owned` ceiling. On ordinary
/// completion it disappears with the creation thread; on unresolved rollback
/// it becomes an exact-key fence that retains the Run until child, reader,
/// waiter, and control cleanup are all proven. This is neither a public pending
/// Run nor durable transaction state.
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
}

struct UnpublishedCleanupFence {
    request: CreationRequest,
    owners: Vec<UnpublishedCleanupEntry>,
}

struct UnpublishedCleanupEntry {
    run: Arc<Run>,
    transfer_reason: String,
}

#[must_use = "dropping the reservation releases unpublished-cleanup capacity"]
pub(crate) struct UnpublishedCleanupReservation {
    inner: Arc<UnpublishedCleanupInner>,
    operation_key: Option<CreateOperationKey>,
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
                "unpublished child cleanup capacity is exhausted",
            ));
        }
        state.owned += 1;
        Ok(UnpublishedCleanupReservation {
            inner: Arc::clone(&self.inner),
            operation_key: Some(operation_key.clone()),
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
    state.owned = state
        .owned
        .checked_sub(reaped)
        .expect("proven cleanups release only owned slots");
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
        sync::Arc,
        time::{Duration, Instant},
    };

    use ctxmux_protocol::{CreateOperationKey, ErrorCode, RunSpec, TerminalSize};

    use super::{
        CreationFlightOwner, CreationRequest, MAX_CREATION_OWNER_SLOTS, UnpublishedCleanupOwner,
    };

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
        let mut reservations = Vec::new();
        for index in 0..MAX_CREATION_OWNER_SLOTS {
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

        drop(reservations.pop());
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

#[derive(Default)]
struct RegistryState {
    runs: HashMap<RunId, Arc<Run>>,
    creation_runs: HashMap<CreateOperationKey, RunId>,
}

/// Single owner of retained Runs and their bounded creation-key mappings.
pub(crate) struct RunRegistry {
    state: RwLock<RegistryState>,
    creation_stripes: [Arc<AsyncMutex<()>>; CREATION_STRIPES],
    creation_hash: RandomState,
}

impl Default for RunRegistry {
    fn default() -> Self {
        Self {
            state: RwLock::default(),
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
                let previous_run = state.runs.insert(id, Arc::clone(&run));
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
    pub(crate) fn resolve_creation(
        &self,
        operation_key: &CreateOperationKey,
        request: &CreationRequest,
    ) -> Result<Option<Arc<Run>>, ProtocolError> {
        let state = read_lock(&self.state);
        let Some(id) = state.creation_runs.get(operation_key) else {
            return Ok(None);
        };
        let run = state.runs.get(id).cloned().ok_or_else(|| {
            ProtocolError::new(
                ErrorCode::Internal,
                "Run creation registry lost its retained Run",
            )
        })?;
        if request.matches_run(&run) {
            Ok(Some(run))
        } else {
            Err(ProtocolError::new(
                ErrorCode::CreationConflict,
                "Run creation operation key is already bound to a different request",
            ))
        }
    }

    /// Atomically publish one Run, its key, and the rollback-owner disposition.
    pub(crate) fn publish_creation(
        &self,
        operation_key: CreateOperationKey,
        pending: PendingPublication,
    ) -> RunInfo {
        let run = Arc::clone(pending.run());
        let info = run.info();
        let id = run.id;
        let mut state = write_lock(&self.state);
        debug_assert!(!state.creation_runs.contains_key(&operation_key));
        debug_assert!(!state.runs.contains_key(&id));
        state.runs.insert(id, run);
        state.creation_runs.insert(operation_key, id);
        let cleanup_reservation = pending.into_published_reservation();
        drop(state);
        drop(cleanup_reservation);
        info
    }

    /// Publish one non-creation-backed Run, currently only a tmux import or test seam.
    pub(crate) fn publish_unkeyed(&self, run: Arc<Run>) {
        let id = run.id;
        let previous = write_lock(&self.state).runs.insert(id, run);
        debug_assert!(previous.is_none());
    }

    pub(crate) fn get(&self, id: RunId) -> Option<Arc<Run>> {
        read_lock(&self.state).runs.get(&id).cloned()
    }

    pub(crate) fn list(&self) -> Vec<RunInfo> {
        read_lock(&self.state)
            .runs
            .values()
            .map(|run| run.info())
            .collect()
    }

    pub(crate) fn snapshot(&self) -> Vec<Arc<Run>> {
        read_lock(&self.state).runs.values().cloned().collect()
    }
}
