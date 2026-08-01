use std::{
    collections::{HashMap, hash_map::RandomState},
    hash::{BuildHasher, Hasher},
    sync::{Arc, Condvar, Mutex, RwLock},
    time::Instant,
};

use ctxmux_protocol::{
    CreateOperationKey, ErrorCode, ForkFidelity, ForkPlan, ProtocolError, RunId, RunInfo, RunSpec,
};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard, OwnedSemaphorePermit, Semaphore};

use super::{Run, read_lock, write_lock};

const CREATION_STRIPES: usize = 64;
// Matches the pre-registered resource start concurrency while bounding only
// transient physical launch owners; this is not a public Run quota.
const MAX_CONCURRENT_CREATION_LAUNCHES: usize = 8;

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
                admission: Arc::new(Semaphore::new(MAX_CONCURRENT_CREATION_LAUNCHES)),
            }),
        }
    }
}

impl CreationFlightOwner {
    /// Wait asynchronously for one physical launch slot.
    ///
    /// Cancellation while waiting releases no flight because none exists yet.
    /// Closing admission during shutdown wakes queued waiters with `None`.
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

#[cfg(test)]
mod tests {
    use std::{
        sync::Arc,
        time::{Duration, Instant},
    };

    use super::{CreationFlightOwner, MAX_CONCURRENT_CREATION_LAUNCHES};

    #[tokio::test]
    async fn admission_caps_physical_launches_and_reclaims_released_permits() {
        let owner = Arc::new(CreationFlightOwner::default());
        let mut active = Vec::new();
        for _ in 0..MAX_CONCURRENT_CREATION_LAUNCHES {
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
        assert_eq!(owner.active_count(), MAX_CONCURRENT_CREATION_LAUNCHES);
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
        assert_eq!(owner.active_count(), MAX_CONCURRENT_CREATION_LAUNCHES);

        drop(active.pop());
        let ninth = tokio::time::timeout(Duration::from_secs(1), ninth)
            .await
            .expect("a released permit wakes the ninth launch")
            .expect("admission waiter task remains live")
            .expect("open admission produces a flight");
        assert_eq!(owner.active_count(), MAX_CONCURRENT_CREATION_LAUNCHES);

        drop(ninth);
        drop(active);

        assert_eq!(owner.active_count(), 0);
        assert_eq!(
            owner.available_admission(),
            MAX_CONCURRENT_CREATION_LAUNCHES
        );
    }

    #[tokio::test]
    async fn shutdown_fence_wakes_admission_waiters_and_drains_active_owners() {
        let owner = Arc::new(CreationFlightOwner::default());
        let mut active = Vec::new();
        for _ in 0..MAX_CONCURRENT_CREATION_LAUNCHES {
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

    /// Atomically publish one Run and its successful creation mapping.
    pub(crate) fn publish_creation(&self, operation_key: CreateOperationKey, run: Arc<Run>) {
        let id = run.id;
        let mut state = write_lock(&self.state);
        debug_assert!(!state.creation_runs.contains_key(&operation_key));
        debug_assert!(!state.runs.contains_key(&id));
        state.runs.insert(id, run);
        state.creation_runs.insert(operation_key, id);
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
