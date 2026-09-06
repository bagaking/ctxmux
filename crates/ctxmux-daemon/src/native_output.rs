//! Cooperative Run output ownership for the sole Native reactor.
//!
//! Every output/transition/binding unlock wakes a reactor that actually deferred
//! on that Run. A turn acquires raw admission before reading a PTY: a busy client
//! view cannot make this actor read bytes and then wait with an unowned tail.
use crate::{OutputLog, PersistenceMode, PersistentRun, Run};
use std::sync::{Mutex, MutexGuard, TryLockError, atomic::Ordering};

/// Raw observations change only at real admission/retention, independently of
/// optional terminal parsing or export. Status/List never acquire a VT lock.
#[derive(Clone, Copy)]
pub(crate) struct OutputFacts {
    pub(crate) latest_output_bytes: u64,
    pub(crate) first_available_byte: u64,
    pub(crate) retained_bytes: usize,
}
pub(crate) struct OutputOwner {
    log: Mutex<OutputLog>,
    facts: std::sync::Arc<Mutex<OutputFacts>>,
}
impl OutputOwner {
    pub(crate) fn new(mut log: OutputLog) -> Self {
        let facts = std::sync::Arc::new(Mutex::new(log.current_facts()));
        log.facts = Some(std::sync::Arc::clone(&facts));
        Self {
            log: Mutex::new(log),
            facts,
        }
    }
    pub(crate) fn snapshot(&self) -> OutputFacts {
        *crate::mutex_lock(&self.facts)
    }
}
impl std::ops::Deref for OutputOwner {
    type Target = Mutex<OutputLog>;
    fn deref(&self) -> &Self::Target {
        &self.log
    }
}
pub(crate) const fn resident_output_facts_bytes() -> usize {
    std::mem::size_of::<Mutex<OutputFacts>>() + 2 * std::mem::size_of::<usize>()
}

pub(crate) struct RunOwnerGuard<'a, T> {
    guard: Option<MutexGuard<'a, T>>,
    run: &'a Run,
    changed: Option<&'a tokio::sync::Notify>,
}
impl<T> std::ops::Deref for RunOwnerGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.guard.as_ref().unwrap()
    }
}
impl<T> std::ops::DerefMut for RunOwnerGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        self.guard.as_mut().unwrap()
    }
}
impl<T> Drop for RunOwnerGuard<'_, T> {
    fn drop(&mut self) {
        drop(self.guard.take());
        if let Some(changed) = self.changed {
            changed.notify_waiters();
        }
        if self.run.owner_deferred.swap(false, Ordering::AcqRel)
            && let Some(owner) = &self.run.native_runs
        {
            owner.owner_wake().wake();
        }
    }
}
impl Run {
    fn owner_changed<T>(&self, mutex: &Mutex<T>) -> Option<&tokio::sync::Notify> {
        let address = std::ptr::from_ref(mutex).cast::<()>();
        if address == std::ptr::from_ref(&*self.output).cast::<()>() {
            Some(&self.output_unlocked)
        } else if address == std::ptr::from_ref(&self.persistence).cast::<()>() {
            Some(&self.persistence_unlocked)
        } else {
            None
        }
    }

    pub(crate) fn lock_owner<'a, T>(&'a self, mutex: &'a Mutex<T>) -> RunOwnerGuard<'a, T> {
        RunOwnerGuard {
            guard: Some(crate::mutex_lock(mutex)),
            run: self,
            changed: self.owner_changed(mutex),
        }
    }
    pub(crate) fn try_owner<'a, T>(&'a self, mutex: &'a Mutex<T>) -> Option<RunOwnerGuard<'a, T>> {
        let acquire = || match mutex.try_lock() {
            Ok(guard) => Some(guard),
            Err(TryLockError::Poisoned(error)) => Some(error.into_inner()),
            Err(TryLockError::WouldBlock) => None,
        };
        if let Some(guard) = acquire() {
            return Some(RunOwnerGuard {
                guard: Some(guard),
                run: self,
                changed: self.owner_changed(mutex),
            });
        }
        self.owner_deferred.store(true, Ordering::Release);
        // A real unlock between the failed try and flag publication must not
        // strand this Run. A second try closes that missed-unlock window.
        acquire().map(|guard| RunOwnerGuard {
            guard: Some(guard),
            run: self,
            changed: self.owner_changed(mutex),
        })
    }
    pub(crate) fn try_output_protected_from(&self) -> Option<u64> {
        if self.persistence_mode == PersistenceMode::MemoryOnly {
            return Some(u64::MAX);
        }
        Some(
            self.try_owner(&self.persistence)?
                .active()
                .map_or(0, |durable| {
                    if durable.is_failed() {
                        u64::MAX
                    } else {
                        durable.next_replay_start()
                    }
                }),
        )
    }
    pub(crate) fn try_output_turn(&self) -> Option<NativeOutputTurn<'_>> {
        let transition = if self.persistence_mode == PersistenceMode::PersistentCapable {
            Some(self.try_owner(&self.persistence_transition)?)
        } else {
            None
        };
        let output = self.try_owner(&self.output)?;
        let persistence = if self.persistence_mode == PersistenceMode::PersistentCapable {
            self.try_owner(&self.persistence)?.active().cloned()
        } else {
            None
        };
        Some(NativeOutputTurn {
            run: self,
            output,
            _transition: transition,
            persistence,
        })
    }
}

pub(crate) struct NativeOutputTurn<'a> {
    run: &'a Run,
    output: RunOwnerGuard<'a, OutputLog>,
    _transition: Option<RunOwnerGuard<'a, ()>>,
    persistence: Option<PersistentRun>,
}
impl NativeOutputTurn<'_> {
    fn protected_from(&self) -> u64 {
        if self.run.persistence_mode == PersistenceMode::MemoryOnly {
            return u64::MAX;
        }
        self.persistence.as_ref().map_or(0, |durable| {
            if durable.is_failed() {
                u64::MAX
            } else {
                durable.next_replay_start()
            }
        })
    }
    pub(crate) fn capacity(&mut self) -> usize {
        if let Some(durable) = &self.persistence
            && !durable.is_failed()
            && durable.queue_has_room()
            && self.output.latest_output_bytes() > durable.next_replay_start()
        {
            let replay = self.output.offer_replay(durable.next_replay_start());
            durable.request_output_wake();
            let _accepted = durable.append(self.run.id, replay);
        }
        let protected = self.protected_from();
        let unoffered = self.output.latest_output_bytes().saturating_sub(protected);
        let per_run_available = self
            .run
            .retention_budget
            .per_run_limit()
            .saturating_sub(usize::try_from(unoffered).unwrap_or(usize::MAX));
        let reclaimable =
            usize::try_from(protected.saturating_sub(self.output.first_available_byte()))
                .unwrap_or(usize::MAX)
                .min(self.output.retained_bytes());
        self.run
            .retention_budget
            .available_for_read(
                self.run.id,
                crate::native_runtime::OUTPUT_READ_BUFFER_BYTES
                    .min(self.run.retention_budget.per_run_limit()),
                reclaimable,
            )
            .min(per_run_available)
    }
    pub(crate) fn has_unoffered(&self) -> bool {
        self.run.persistence_mode == PersistenceMode::PersistentCapable
            && self.output.latest_output_bytes() > self.protected_from()
    }
    pub(crate) fn record(mut self, data: Vec<u8>) -> bool {
        let run = self.run;
        run.record_output_locked(data, &mut self.output, self.persistence.as_ref());
        let unoffered = self.has_unoffered();
        drop(self);
        // No output lock survives into cross-Run reclamation.
        run.retention_budget.reclaim_excess(run.id);
        unoffered
    }
    pub(crate) fn mark_source_gap(&mut self) -> u64 {
        self.output.mark_source_gap()
    }
}
