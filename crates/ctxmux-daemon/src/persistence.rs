use std::{
    collections::{BTreeSet, HashMap, HashSet, VecDeque},
    fs::{self, File, OpenOptions},
    io,
    os::fd::{AsRawFd, OwnedFd, RawFd},
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
        mpsc,
    },
    thread,
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(test)]
use std::sync::atomic::{AtomicBool, AtomicU8};

use ctxmux_protocol::{
    CreateOperationKey, DaemonInstanceId, InterruptionReason, OutputChunk, OutputReplay,
    RunBackend, RunCapabilities, RunId, RunInfo, RunLineage, RunSpec, RunState, RuntimeId,
};
use rusqlite::{Connection, OpenFlags, OptionalExtension, Transaction, params, params_from_iter};
use thiserror::Error;
use uuid::Uuid;

use crate::{creation::MAX_RETAINED_RUNS, run_spec::validate_run_spec};

const SCHEMA_VERSION: i64 = 4;
const DATABASE_FILE: &str = "state.sqlite3";
const LOCK_FILE: &str = "state.lock";
const PAGE_SIZE_BYTES: u64 = 4 * 1024;
const DATABASE_MAX_BYTES: u64 = 384 * 1024 * 1024;
const DATABASE_MAX_PAGES: u64 = DATABASE_MAX_BYTES / PAGE_SIZE_BYTES;
const WAL_CHECKPOINT_BYTES: u64 = 8 * 1024 * 1024;
const WAL_MAX_BYTES: u64 = 16 * 1024 * 1024;
const SHM_MAX_BYTES: u64 = 4 * 1024 * 1024;
const STATE_FILES_MAX_BYTES: u64 = 404 * 1024 * 1024;
const PER_RUN_REPLAY_BYTES: u64 = 4 * 1024 * 1024;
const GLOBAL_REPLAY_BYTES: u64 = 256 * 1024 * 1024;
pub(crate) const METADATA_BYTES: u64 = 64 * 1024 * 1024;
const RUN_RECORDS: u64 = 4_096;
const MAX_TRANSACTION_PAYLOAD_BYTES: usize = 1024 * 1024;
const PERSISTENCE_QUEUE_CAPACITY: usize = 1_024;
const LIFECYCLE_METADATA_RESERVE_BYTES: usize = 128;
const WAL_HEADER_BYTES: u64 = 32;
const WAL_FRAME_BYTES: u64 = 24 + PAGE_SIZE_BYTES;
const STARTUP_BATCH_MAX_ROWS: usize = 128;

#[derive(Clone, Copy)]
struct AdmissionLimits {
    run_records: u64,
    metadata_bytes: u64,
}

impl AdmissionLimits {
    #[cfg(test)]
    const FORMAT: Self = Self {
        run_records: RUN_RECORDS,
        metadata_bytes: METADATA_BYTES,
    };

    const OPERATIONAL: Self = Self {
        run_records: MAX_RETAINED_RUNS as u64,
        metadata_bytes: METADATA_BYTES,
    };
}

#[derive(Debug, Error)]
pub enum PersistenceError {
    #[error("invalid ctxmux state directory {path}: {message}")]
    InvalidDirectory { path: PathBuf, message: String },
    #[error("ctxmux state directory is already in use: {0}")]
    StateInUse(PathBuf),
    #[error("unsupported ctxmux state schema {found}; expected {expected}")]
    UnsupportedSchema { found: i64, expected: i64 },
    #[error("ctxmux durable state is corrupt: {0}")]
    Corrupt(String),
    #[error("ctxmux durable state I/O failed at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("ctxmux durable state database failed: {0}")]
    Database(String),
    #[error("ctxmux persistence actor stopped")]
    ActorStopped,
    #[error("failed to start ctxmux persistence actor: {0}")]
    ActorStart(String),
    #[error("ctxmux durable state rejected a mutation: {0}")]
    Mutation(String),
}

impl PersistenceError {
    fn io(path: impl Into<PathBuf>, source: io::Error) -> Self {
        Self::Io {
            path: path.into(),
            source,
        }
    }

    fn database(error: impl std::fmt::Display) -> Self {
        Self::Database(error.to_string())
    }

    fn serialization(error: impl std::fmt::Display) -> Self {
        Self::Mutation(format!("serialization failed: {error}"))
    }
}

pub(crate) struct RecoveredRun {
    pub(crate) operation_key: CreateOperationKey,
    pub(crate) info: RunInfo,
    pub(crate) replay: OutputReplay,
    pub(crate) metadata_bytes: u64,
}

/// Continuity hint passed to persistence startup by an exec-in-place upgrade:
/// the Runs whose live control crossed the exec (excluded from reconciliation)
/// and the daemon epoch to reuse instead of minting a fresh one.
///
/// When absent (the crash-recovery path), startup is byte-identical to a cold
/// restart: every `running` row is reconciled to `interrupted{daemon_restart}`
/// and a fresh epoch is minted.
pub(crate) struct HandoffHint {
    pub(crate) epoch: String,
    pub(crate) live_set: HashSet<RunId>,
    /// The advisory state lock the outgoing image still holds, inherited on this
    /// descriptor across exec. `None` means acquire the lock normally (a fresh
    /// open + `try_lock`); `Some` means adopt it and skip the self-deadlocking
    /// re-lock. Owned so the actor thread closes it correctly.
    pub(crate) state_lock_fd: Option<OwnedFd>,
}

#[derive(Clone)]
pub(crate) struct Persistence {
    inner: Arc<PersistenceInner>,
}

struct PersistenceInner {
    sender: mpsc::SyncSender<Command>,
    failure: Arc<Mutex<Option<String>>>,
    join: Mutex<Option<thread::JoinHandle<()>>>,
    runtime_id: RuntimeId,
    epoch: String,
    /// Raw fd of the advisory state lock held by the persistence actor thread.
    /// Surfaced so an exec-in-place upgrade can record it in the handoff manifest
    /// and have the incoming image adopt the still-held lock instead of re-locking.
    state_lock_fd: RawFd,
    #[cfg(test)]
    test_hooks: Arc<PersistenceTestHooks>,
}

#[cfg(test)]
#[derive(Default)]
struct PersistenceTestHooks {
    append_transaction_commits: AtomicU64,
    fail_next_insert_after_commit: AtomicBool,
    fail_next_start_before_commit: AtomicBool,
    finalize_barrier: Mutex<Option<FinalizeTestBarrier>>,
    startup_batch_wal_bytes: Mutex<Vec<u64>>,
    startup_fail_after_commits: AtomicU64,
    startup_over_budget_attempts: AtomicU64,
    force_startup_over_budget_once: AtomicBool,
    start_commit_crash_phase: AtomicU8,
    fail_next_start_commit_as: Mutex<Option<CommitProbe>>,
}

#[cfg(test)]
static NEXT_OPEN_TEST_HOOKS: Mutex<Option<Arc<PersistenceTestHooks>>> = Mutex::new(None);

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum StartCommitCrashPhase {
    Before = 1,
    After = 2,
}

/// Immutable persistence-owned encoding of one native Run before launch.
///
/// Only this module can construct the value, so Registry admission can use its
/// metadata measurement without duplicating `SQLite` serialization rules.
pub(crate) struct PreparedPersistentStart {
    operation_key: CreateOperationKey,
    id: RunId,
    spec_json: String,
    lineage_json: Option<String>,
    state_json: String,
    epoch: String,
    metadata_bytes: u64,
}

impl PreparedPersistentStart {
    pub(crate) const fn metadata_bytes(&self) -> u64 {
        self.metadata_bytes
    }
}

/// Registry-owned identity snapshot for one exact terminal replacement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PersistentCandidate {
    id: RunId,
    operation_key: CreateOperationKey,
    metadata_bytes: u64,
}

impl PersistentCandidate {
    pub(crate) const fn new(
        id: RunId,
        operation_key: CreateOperationKey,
        metadata_bytes: u64,
    ) -> Self {
        Self {
            id,
            operation_key,
            metadata_bytes,
        }
    }
}

/// Monotonic durable disposition of one staged Run start.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StartDisposition {
    Pending,
    NotCommitted,
    Committed,
    CommitUnknown,
}

#[derive(Clone, Debug)]
pub(crate) struct StartReceipt {
    disposition: Arc<Mutex<StartDisposition>>,
}

impl StartReceipt {
    fn pending() -> Self {
        Self {
            disposition: Arc::new(Mutex::new(StartDisposition::Pending)),
        }
    }

    pub(crate) fn disposition(&self) -> StartDisposition {
        *mutex_lock(&self.disposition)
    }

    fn decide(&self, disposition: StartDisposition) -> bool {
        debug_assert_ne!(disposition, StartDisposition::Pending);
        let mut current = mutex_lock(&self.disposition);
        if *current != StartDisposition::Pending {
            return false;
        }
        *current = disposition;
        true
    }

    fn unknown_if_pending(&self) -> StartDisposition {
        let _ = self.decide(StartDisposition::CommitUnknown);
        self.disposition()
    }
}

#[derive(Debug, Error)]
#[error("persistent Run start is {disposition:?}: {error}")]
pub(crate) struct PersistentStartFailure {
    disposition: StartDisposition,
    capacity: bool,
    #[source]
    error: PersistenceError,
}

impl PersistentStartFailure {
    fn new(disposition: StartDisposition, error: PersistenceError) -> Self {
        Self {
            disposition,
            capacity: false,
            error,
        }
    }

    fn from_stage(disposition: StartDisposition, stage_failure: StageFailure) -> Self {
        Self {
            disposition,
            capacity: stage_failure.capacity,
            error: stage_failure.error,
        }
    }

    pub(crate) const fn disposition(&self) -> StartDisposition {
        self.disposition
    }

    pub(crate) const fn is_capacity(&self) -> bool {
        self.capacity
    }

    pub(crate) fn into_error(self) -> PersistenceError {
        self.error
    }
}

pub(crate) enum PersistentStartCompletion {
    NotCommitted(PersistentStartFailure),
    Committed(CommittedStart),
    CommitUnknown(PersistentStartFailure),
}

/// Affine decision owner for one `SQLite` transaction already staged in memory.
#[must_use = "a staged persistent start must be committed or aborted"]
pub(crate) struct StagedPersistentStart {
    durable: Option<PersistentRun>,
    decision: Option<mpsc::SyncSender<StageDecision>>,
    completion: mpsc::Receiver<StageCompletion>,
    receipt: StartReceipt,
}

#[cfg(test)]
struct FinalizeTestBarrier {
    reached: mpsc::SyncSender<()>,
    release: mpsc::Receiver<()>,
}

impl Drop for PersistenceInner {
    fn drop(&mut self) {
        let _ = self.sender.send(Command::Shutdown);
        if let Some(join) = mutex_lock(&self.join).take() {
            let _ = join.join();
        }
    }
}

#[derive(Clone)]
pub(crate) struct PersistentRun {
    persistence: Persistence,
    durable_head: Arc<AtomicU64>,
    metadata_bytes: Arc<AtomicU64>,
}

pub(crate) struct CommittedStart {
    pub(crate) durable: PersistentRun,
    pub(crate) post_commit_error: Option<PersistenceError>,
}

impl std::ops::Deref for CommittedStart {
    type Target = PersistentRun;

    fn deref(&self) -> &Self::Target {
        &self.durable
    }
}

impl PersistentRun {
    pub(crate) fn durable_head(&self) -> u64 {
        self.durable_head.load(Ordering::Acquire)
    }

    pub(crate) fn metadata_bytes_owner(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.metadata_bytes)
    }

    pub(crate) fn append(&self, id: RunId, replay: OutputReplay) {
        if mutex_lock(&self.persistence.inner.failure).is_some() {
            return;
        }
        let _ = self.persistence.inner.sender.send(Command::Append {
            id,
            replay,
            durable_head: Arc::clone(&self.durable_head),
        });
    }

    pub(crate) fn finalize(
        &self,
        id: RunId,
        actual_pid: u32,
        replay: OutputReplay,
        state: RunState,
    ) {
        if mutex_lock(&self.persistence.inner.failure).is_some() {
            return;
        }
        let (reply_tx, reply_rx) = mpsc::sync_channel(0);
        if self
            .persistence
            .inner
            .sender
            .send(Command::Finalize {
                id,
                actual_pid,
                replay,
                state,
                durable_head: Arc::clone(&self.durable_head),
                metadata_bytes: Arc::clone(&self.metadata_bytes),
                reply: reply_tx,
            })
            .is_err()
        {
            return;
        }
        let _ = reply_rx.recv();
    }
}

impl Persistence {
    pub(crate) fn runtime_id(&self) -> RuntimeId {
        self.inner.runtime_id
    }

    pub(crate) fn daemon_instance(&self) -> DaemonInstanceId {
        self.inner
            .epoch
            .parse()
            .expect("persistence serving epoch is a validated UUID")
    }

    /// Raw fd of the advisory state lock this persistence instance holds. An
    /// exec-in-place upgrade records it in the handoff manifest so the incoming
    /// image adopts the still-held lock across exec instead of re-locking
    /// (which would self-deadlock on the same open file description).
    pub(crate) fn state_lock_fd(&self) -> RawFd {
        self.inner.state_lock_fd
    }

    /// Drive a synchronous durable-commit barrier: block until every Append
    /// enqueued before this call has been committed. FIFO ordering guarantees
    /// that when the reply returns, the persisted cursor covers all prior
    /// appends. Surfaces a persistence failure (an append that failed to commit)
    /// so the caller can fail-stop instead of exec-ing into a replay gap.
    pub(crate) fn barrier(&self) -> Result<(), PersistenceError> {
        let (reply_tx, reply_rx) = mpsc::sync_channel(0);
        if self
            .inner
            .sender
            .send(Command::Barrier { reply: reply_tx })
            .is_err()
        {
            return Err(PersistenceError::ActorStopped);
        }
        reply_rx
            .recv()
            .map_err(|_| PersistenceError::ActorStopped)?;
        if let Some(message) = mutex_lock(&self.inner.failure).clone() {
            return Err(PersistenceError::Mutation(message));
        }
        Ok(())
    }

    pub(crate) fn open(
        state_dir: impl Into<PathBuf>,
    ) -> Result<(Self, Vec<RecoveredRun>), PersistenceError> {
        Self::open_with_admission_limits(state_dir.into(), AdmissionLimits::OPERATIONAL, None)
    }

    /// Incoming-image startup seam for exec-in-place: reuse the handed-off epoch,
    /// exclude the live Run set from reconciliation, and adopt the inherited
    /// state-lock descriptor instead of re-locking. A12 calls this.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn open_with_handoff(
        state_dir: impl Into<PathBuf>,
        hint: HandoffHint,
    ) -> Result<(Self, Vec<RecoveredRun>), PersistenceError> {
        Self::open_with_admission_limits(state_dir.into(), AdmissionLimits::OPERATIONAL, Some(hint))
    }

    fn open_with_admission_limits(
        state_dir: PathBuf,
        admission_limits: AdmissionLimits,
        handoff: Option<HandoffHint>,
    ) -> Result<(Self, Vec<RecoveredRun>), PersistenceError> {
        #[cfg(test)]
        let test_hooks = mutex_lock(&NEXT_OPEN_TEST_HOOKS)
            .take()
            .unwrap_or_else(|| Arc::new(PersistenceTestHooks::default()));
        Self::open_with_admission_limits_and_hooks(
            state_dir,
            admission_limits,
            handoff,
            #[cfg(test)]
            test_hooks,
        )
    }

    fn open_with_admission_limits_and_hooks(
        state_dir: PathBuf,
        admission_limits: AdmissionLimits,
        handoff: Option<HandoffHint>,
        #[cfg(test)] test_hooks: Arc<PersistenceTestHooks>,
    ) -> Result<(Self, Vec<RecoveredRun>), PersistenceError> {
        let (command_tx, command_rx) = mpsc::sync_channel(PERSISTENCE_QUEUE_CAPACITY);
        let (init_tx, init_rx) = mpsc::sync_channel(0);
        let failure = Arc::new(Mutex::new(None));
        let actor_failure = Arc::clone(&failure);
        #[cfg(test)]
        let actor_test_hooks = Arc::clone(&test_hooks);
        let join = thread::Builder::new()
            .name("ctxmux-persistence".to_owned())
            .spawn(move || {
                actor_main(
                    &state_dir,
                    admission_limits,
                    handoff,
                    &command_rx,
                    &init_tx,
                    &actor_failure,
                    #[cfg(test)]
                    &actor_test_hooks,
                );
            })
            .map_err(|error| PersistenceError::ActorStart(error.to_string()))?;
        let (runtime_id, epoch, state_lock_fd, recovered) = match init_rx.recv() {
            Ok(Ok(initialized)) => initialized,
            Ok(Err(error)) => {
                let _ = join.join();
                return Err(error);
            }
            Err(_) => {
                let _ = join.join();
                return Err(PersistenceError::ActorStopped);
            }
        };
        let persistence = Self {
            inner: Arc::new(PersistenceInner {
                sender: command_tx,
                failure,
                join: Mutex::new(Some(join)),
                runtime_id,
                epoch,
                state_lock_fd,
                #[cfg(test)]
                test_hooks,
            }),
        };
        Ok((persistence, recovered))
    }

    #[cfg(test)]
    pub(crate) fn fail_next_open_after_startup_commit() {
        let hooks = Arc::new(PersistenceTestHooks::default());
        hooks.startup_fail_after_commits.store(1, Ordering::Release);
        let mut next = mutex_lock(&NEXT_OPEN_TEST_HOOKS);
        assert!(
            next.is_none(),
            "only one persistence open fixture may be armed"
        );
        *next = Some(hooks);
    }

    pub(crate) fn prepare_start(
        &self,
        operation_key: &CreateOperationKey,
        info: &RunInfo,
    ) -> Result<PreparedPersistentStart, PersistenceError> {
        if let Some(message) = mutex_lock(&self.inner.failure).clone() {
            return Err(PersistenceError::Mutation(message));
        }
        operation_key.validate().map_err(|error| {
            PersistenceError::Mutation(format!("invalid Run creation operation key: {error}"))
        })?;
        let spec = validate_persistent_start(info)?;
        let spec_json = serde_json::to_string(spec).map_err(PersistenceError::serialization)?;
        let lineage_json = info
            .lineage
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(PersistenceError::serialization)?;
        let state_json =
            serde_json::to_string(&RunState::Running).map_err(PersistenceError::serialization)?;
        let metadata_bytes = metadata_size(
            &info.id.to_string(),
            operation_key.as_str(),
            &spec_json,
            lineage_json.as_deref(),
            &state_json,
            &self.inner.epoch,
        )?;
        Ok(PreparedPersistentStart {
            operation_key: operation_key.clone(),
            id: info.id,
            spec_json,
            lineage_json,
            state_json,
            epoch: self.inner.epoch.clone(),
            metadata_bytes,
        })
    }

    pub(crate) fn stage_start(
        &self,
        prepared: PreparedPersistentStart,
        candidates: Vec<PersistentCandidate>,
    ) -> Result<StagedPersistentStart, PersistentStartFailure> {
        if let Some(message) = mutex_lock(&self.inner.failure).clone() {
            return Err(PersistentStartFailure::new(
                StartDisposition::NotCommitted,
                PersistenceError::Mutation(message),
            ));
        }
        let metadata_bytes = prepared.metadata_bytes;
        let receipt = StartReceipt::pending();
        let (ready_tx, ready_rx) = mpsc::sync_channel(0);
        let (decision_tx, decision_rx) = mpsc::sync_channel(0);
        let (completion_tx, completion_rx) = mpsc::sync_channel(0);
        self.inner
            .sender
            .send(Command::StageStart(Box::new(StageRequest {
                prepared: Box::new(prepared),
                candidates,
                receipt: receipt.clone(),
                ready: ready_tx,
                decision: decision_rx,
                completion: completion_tx,
            })))
            .map_err(|_| {
                let _ = receipt.decide(StartDisposition::NotCommitted);
                PersistentStartFailure::new(
                    StartDisposition::NotCommitted,
                    PersistenceError::ActorStopped,
                )
            })?;
        match ready_rx.recv() {
            Ok(Ok(())) => Ok(StagedPersistentStart {
                durable: Some(PersistentRun {
                    persistence: self.clone(),
                    durable_head: Arc::new(AtomicU64::new(0)),
                    metadata_bytes: Arc::new(AtomicU64::new(metadata_bytes)),
                }),
                decision: Some(decision_tx),
                completion: completion_rx,
                receipt,
            }),
            Ok(Err(error)) => {
                let disposition = receipt.disposition();
                Err(PersistentStartFailure::from_stage(disposition, error))
            }
            Err(_) => {
                let disposition = receipt.unknown_if_pending();
                Err(PersistentStartFailure::new(
                    disposition,
                    PersistenceError::ActorStopped,
                ))
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn insert_start(
        &self,
        operation_key: &CreateOperationKey,
        info: &RunInfo,
    ) -> Result<CommittedStart, PersistenceError> {
        let prepared = self.prepare_start(operation_key, info)?;
        let staged = self
            .stage_start(prepared, Vec::new())
            .map_err(PersistentStartFailure::into_error)?;
        match staged.commit() {
            PersistentStartCompletion::Committed(start) => Ok(start),
            PersistentStartCompletion::NotCommitted(failure)
            | PersistentStartCompletion::CommitUnknown(failure) => Err(failure.into_error()),
        }
    }

    #[cfg(test)]
    pub(crate) fn fail_next_insert_after_commit(&self) {
        self.inner
            .test_hooks
            .fail_next_insert_after_commit
            .store(true, Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) fn fail_next_start_before_commit(&self) {
        self.inner
            .test_hooks
            .fail_next_start_before_commit
            .store(true, Ordering::Release);
    }

    #[cfg(test)]
    fn crash_next_start_commit_at(&self, phase: StartCommitCrashPhase) {
        self.inner
            .test_hooks
            .start_commit_crash_phase
            .store(phase as u8, Ordering::Release);
    }

    #[cfg(test)]
    fn fail_next_start_commit_as(&self, durable_unit: CommitProbe) {
        let previous =
            mutex_lock(&self.inner.test_hooks.fail_next_start_commit_as).replace(durable_unit);
        assert!(
            previous.is_none(),
            "only one failed COMMIT fixture may be armed"
        );
    }

    #[cfg(test)]
    pub(crate) fn pause_next_finalize(&self) -> (mpsc::Receiver<()>, mpsc::SyncSender<()>) {
        let (reached_tx, reached_rx) = mpsc::sync_channel(0);
        let (release_tx, release_rx) = mpsc::sync_channel(0);
        let previous =
            mutex_lock(&self.inner.test_hooks.finalize_barrier).replace(FinalizeTestBarrier {
                reached: reached_tx,
                release: release_rx,
            });
        assert!(previous.is_none(), "only one finalize barrier may be armed");
        (reached_rx, release_tx)
    }

    #[cfg(test)]
    pub(crate) fn is_failed(&self) -> bool {
        mutex_lock(&self.inner.failure).is_some()
    }

    #[cfg(test)]
    pub(crate) fn startup_batch_wal_bytes(&self) -> Vec<u64> {
        mutex_lock(&self.inner.test_hooks.startup_batch_wal_bytes).clone()
    }

    #[cfg(test)]
    pub(crate) fn assert_exclusive_owner(&self) {
        assert_eq!(
            Arc::strong_count(&self.inner),
            1,
            "test must release every durable Run before reopening its state directory"
        );
    }

    #[cfg(test)]
    pub(crate) fn open_with_test_limits(
        state_dir: PathBuf,
        run_records: u64,
        metadata_bytes: u64,
    ) -> Result<(Self, Vec<RecoveredRun>), PersistenceError> {
        Self::open_with_admission_limits(
            state_dir,
            AdmissionLimits {
                run_records,
                metadata_bytes,
            },
            None,
        )
    }

    pub(crate) fn recovered_run(&self, durable_head: u64, metadata_bytes: u64) -> PersistentRun {
        PersistentRun {
            persistence: self.clone(),
            durable_head: Arc::new(AtomicU64::new(durable_head)),
            metadata_bytes: Arc::new(AtomicU64::new(metadata_bytes)),
        }
    }
}

impl StagedPersistentStart {
    pub(crate) fn commit(mut self) -> PersistentStartCompletion {
        let result = match self.send_decision(StageDecision::Commit) {
            Ok(()) => self.recv_completion(),
            Err(failure) => self.completion_from_failure(failure),
        };
        self.decision = None;
        result
    }

    pub(crate) fn abort(mut self) -> Result<(), PersistentStartFailure> {
        self.send_decision(StageDecision::Abort)?;
        let result = match self.completion.recv() {
            Ok(StageCompletion::NotCommitted(stage_failure)) if stage_failure.fatal => Err(
                PersistentStartFailure::new(StartDisposition::NotCommitted, stage_failure.error),
            ),
            Ok(StageCompletion::NotCommitted(_)) => Ok(()),
            Ok(StageCompletion::Committed(post_commit_error)) => {
                let error = post_commit_error.unwrap_or_else(|| {
                    PersistenceError::Mutation(
                        "persistent start committed after an abort decision".to_owned(),
                    )
                });
                Err(PersistentStartFailure::new(
                    StartDisposition::Committed,
                    error,
                ))
            }
            Ok(StageCompletion::CommitUnknown(error)) => Err(PersistentStartFailure::new(
                StartDisposition::CommitUnknown,
                error,
            )),
            Err(_) => {
                let disposition = self.receipt.unknown_if_pending();
                Err(PersistentStartFailure::new(
                    disposition,
                    PersistenceError::ActorStopped,
                ))
            }
        };
        self.decision = None;
        result
    }

    fn send_decision(&mut self, decision: StageDecision) -> Result<(), PersistentStartFailure> {
        let Some(sender) = self.decision.take() else {
            return Err(PersistentStartFailure::new(
                self.receipt.unknown_if_pending(),
                PersistenceError::ActorStopped,
            ));
        };
        sender.send(decision).map_err(|_| {
            PersistentStartFailure::new(
                self.receipt.unknown_if_pending(),
                PersistenceError::ActorStopped,
            )
        })
    }

    fn recv_completion(&mut self) -> PersistentStartCompletion {
        match self.completion.recv() {
            Ok(StageCompletion::NotCommitted(stage_failure)) => {
                PersistentStartCompletion::NotCommitted(PersistentStartFailure::from_stage(
                    StartDisposition::NotCommitted,
                    stage_failure,
                ))
            }
            Ok(StageCompletion::Committed(post_commit_error)) => {
                PersistentStartCompletion::Committed(CommittedStart {
                    durable: self.take_durable(),
                    post_commit_error,
                })
            }
            Ok(StageCompletion::CommitUnknown(error)) => PersistentStartCompletion::CommitUnknown(
                PersistentStartFailure::new(StartDisposition::CommitUnknown, error),
            ),
            Err(_) => {
                let disposition = self.receipt.unknown_if_pending();
                let failure =
                    PersistentStartFailure::new(disposition, PersistenceError::ActorStopped);
                self.completion_from_failure(failure)
            }
        }
    }

    fn completion_from_failure(
        &mut self,
        failure: PersistentStartFailure,
    ) -> PersistentStartCompletion {
        match failure.disposition() {
            StartDisposition::Committed => PersistentStartCompletion::Committed(CommittedStart {
                durable: self.take_durable(),
                post_commit_error: Some(failure.into_error()),
            }),
            StartDisposition::NotCommitted => PersistentStartCompletion::NotCommitted(failure),
            StartDisposition::Pending | StartDisposition::CommitUnknown => {
                PersistentStartCompletion::CommitUnknown(failure)
            }
        }
    }

    fn take_durable(&mut self) -> PersistentRun {
        self.durable
            .take()
            .expect("committed staged start retains one preallocated durable owner")
    }
}

impl Drop for StagedPersistentStart {
    fn drop(&mut self) {
        drop(self.decision.take());
    }
}

enum Command {
    StageStart(Box<StageRequest>),
    Append {
        id: RunId,
        replay: OutputReplay,
        durable_head: Arc<AtomicU64>,
    },
    Finalize {
        id: RunId,
        actual_pid: u32,
        replay: OutputReplay,
        state: RunState,
        durable_head: Arc<AtomicU64>,
        metadata_bytes: Arc<AtomicU64>,
        reply: mpsc::SyncSender<Result<(), PersistenceError>>,
    },
    Barrier {
        reply: mpsc::SyncSender<()>,
    },
    Shutdown,
}

struct StageRequest {
    prepared: Box<PreparedPersistentStart>,
    candidates: Vec<PersistentCandidate>,
    receipt: StartReceipt,
    ready: mpsc::SyncSender<Result<(), StageFailure>>,
    decision: mpsc::Receiver<StageDecision>,
    completion: mpsc::SyncSender<StageCompletion>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StageDecision {
    Commit,
    Abort,
}

enum StageCompletion {
    NotCommitted(StageFailure),
    Committed(Option<PersistenceError>),
    CommitUnknown(PersistenceError),
}

/// The persistence actor's startup handshake payload: the serving epoch, the
/// raw fd its state lock is held on (surfaced for exec-in-place handoff), and
/// the reconciled recovered Runs.
type ActorInit = Result<(RuntimeId, String, RawFd, Vec<RecoveredRun>), PersistenceError>;

#[allow(
    clippy::too_many_lines,
    reason = "one FIFO actor loop keeps batching, failure latching, barriers, and shutdown ordering explicit"
)]
fn actor_main(
    state_dir: &Path,
    admission_limits: AdmissionLimits,
    handoff: Option<HandoffHint>,
    receiver: &mpsc::Receiver<Command>,
    init: &mpsc::SyncSender<ActorInit>,
    failure: &Mutex<Option<String>>,
    #[cfg(test)] test_hooks: &Arc<PersistenceTestHooks>,
) {
    #[cfg(test)]
    let store_test_hooks = Arc::clone(test_hooks);
    let (mut store, recovered) = match StateStore::open(
        state_dir,
        admission_limits,
        handoff,
        #[cfg(test)]
        store_test_hooks,
    ) {
        Ok(value) => value,
        Err(error) => {
            let _ = init.send(Err(error));
            return;
        }
    };
    let init_payload = Ok((
        store.runtime_id,
        store.epoch.clone(),
        store.state_lock_raw_fd(),
        recovered,
    ));
    if init.send(init_payload).is_err() {
        return;
    }

    let mut pending = VecDeque::new();
    loop {
        let command = match pending.pop_front() {
            Some(command) => command,
            None => match receiver.recv() {
                Ok(command) => command,
                Err(_) => return,
            },
        };
        match command {
            Command::StageStart(request) => {
                handle_staged_start(&mut store, &request, failure);
            }
            Command::Append {
                id,
                replay,
                durable_head,
            } => {
                let mut batch = vec![(id, replay, durable_head)];
                let mut payload = replay_payload(&batch[0].1);
                while payload < MAX_TRANSACTION_PAYLOAD_BYTES {
                    match receiver.try_recv() {
                        Ok(Command::Append {
                            id,
                            replay,
                            durable_head,
                        }) if payload.saturating_add(replay_payload(&replay))
                            <= MAX_TRANSACTION_PAYLOAD_BYTES =>
                        {
                            payload = payload.saturating_add(replay_payload(&replay));
                            batch.push((id, replay, durable_head));
                        }
                        Ok(command) => {
                            pending.push_back(command);
                            break;
                        }
                        Err(mpsc::TryRecvError::Empty | mpsc::TryRecvError::Disconnected) => break,
                    }
                }
                if mutex_lock(failure).is_none()
                    && let Err(error) = store.append_batch(&batch)
                {
                    remember_failure(failure, &error);
                }
            }
            Command::Finalize {
                id,
                actual_pid,
                replay,
                state,
                durable_head,
                metadata_bytes,
                reply,
            } => {
                #[cfg(test)]
                pause_before_finalize(test_hooks);
                let result = if let Some(message) = mutex_lock(failure).clone() {
                    Err(PersistenceError::Mutation(message))
                } else {
                    store.finalize(
                        id,
                        actual_pid,
                        &replay,
                        &state,
                        &durable_head,
                        &metadata_bytes,
                    )
                };
                if let Err(error) = &result {
                    remember_failure(failure, error);
                }
                let _ = reply.send(result);
            }
            Command::Barrier { reply } => {
                // No store work: FIFO ordering means every prior Append was
                // already committed by append_batch before this command was
                // dequeued. The reply just unblocks the caller, which then
                // inspects the shared failure slot for any commit error.
                let _ = reply.send(());
            }
            Command::Shutdown => return,
        }
    }
}

#[cfg(test)]
fn pause_before_finalize(test_hooks: &PersistenceTestHooks) {
    let barrier = mutex_lock(&test_hooks.finalize_barrier).take();
    if let Some(barrier) = barrier {
        let _ = barrier.reached.send(());
        let _ = barrier.release.recv();
    }
}

fn handle_staged_start(
    store: &mut StateStore,
    request: &StageRequest,
    failure: &Mutex<Option<String>>,
) {
    if let Some(message) = mutex_lock(failure).clone() {
        let _ = request.receipt.decide(StartDisposition::NotCommitted);
        let _ = request.ready.send(Err(StageFailure {
            error: PersistenceError::Mutation(message),
            fatal: true,
            capacity: false,
        }));
        return;
    }
    match store.drive_staged_start(
        &request.prepared,
        &request.candidates,
        &request.receipt,
        &request.ready,
        &request.decision,
    ) {
        StageDriveResult::ReadyFailed(stage_failure) => {
            if stage_failure.fatal {
                remember_failure(failure, &stage_failure.error);
            }
            let _ = request.ready.send(Err(stage_failure));
        }
        StageDriveResult::Completed(result) => {
            match &result {
                StageCompletion::Committed(Some(error)) | StageCompletion::CommitUnknown(error) => {
                    remember_failure(failure, error);
                }
                StageCompletion::NotCommitted(stage_failure) if stage_failure.fatal => {
                    remember_failure(failure, &stage_failure.error);
                }
                StageCompletion::NotCommitted(_) | StageCompletion::Committed(None) => {}
            }
            let _ = request.completion.send(result);
        }
    }
}

struct StageFailure {
    error: PersistenceError,
    fatal: bool,
    capacity: bool,
}

enum StageDriveResult {
    ReadyFailed(StageFailure),
    Completed(StageCompletion),
}

impl StageDriveResult {
    fn with_restore_failure(self, restore_error: PersistenceError) -> Self {
        match self {
            Self::ReadyFailed(stage_failure) => Self::ReadyFailed(StageFailure {
                error: combine_errors(&stage_failure.error, &restore_error),
                fatal: true,
                capacity: false,
            }),
            Self::Completed(StageCompletion::NotCommitted(stage_failure)) => {
                Self::Completed(StageCompletion::NotCommitted(StageFailure {
                    error: combine_errors(&stage_failure.error, &restore_error),
                    fatal: true,
                    capacity: false,
                }))
            }
            Self::Completed(StageCompletion::Committed(post_commit_error)) => {
                let post_commit_error = Some(match post_commit_error {
                    Some(error) => combine_errors(&error, &restore_error),
                    None => restore_error,
                });
                Self::Completed(StageCompletion::Committed(post_commit_error))
            }
            Self::Completed(StageCompletion::CommitUnknown(error)) => Self::Completed(
                StageCompletion::CommitUnknown(combine_errors(&error, &restore_error)),
            ),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CommitProbe {
    OldUnit,
    NewUnit,
    Hybrid,
}

struct StoredPreparedRow {
    operation_key: String,
    spec_json: String,
    lineage_json: Option<String>,
    state_kind: String,
    state_json: String,
    epoch: String,
    pid: Option<i64>,
    metadata_bytes: i64,
}

fn admission_failure(message: impl Into<String>) -> StageFailure {
    StageFailure {
        error: PersistenceError::Mutation(message.into()),
        fatal: false,
        capacity: true,
    }
}

fn fatal_stage_failure(message: impl Into<String>) -> StageFailure {
    StageFailure {
        error: PersistenceError::Mutation(message.into()),
        fatal: true,
        capacity: false,
    }
}

fn combine_errors(primary: &PersistenceError, secondary: &PersistenceError) -> PersistenceError {
    PersistenceError::Mutation(format!("{primary}; additionally: {secondary}"))
}

fn wal_charge_for_cache(used_bytes: u64) -> Option<u64> {
    let pages = used_bytes.checked_add(PAGE_SIZE_BYTES - 1)? / PAGE_SIZE_BYTES;
    WAL_HEADER_BYTES.checked_add(pages.checked_mul(WAL_FRAME_BYTES)?)
}

fn remember_failure(failure: &Mutex<Option<String>>, error: &PersistenceError) {
    let mut failure = mutex_lock(failure);
    if failure.is_none() {
        *failure = Some(error.to_string());
    }
}

fn replay_payload(replay: &OutputReplay) -> usize {
    replay.chunks.iter().map(|chunk| chunk.data.len()).sum()
}

struct StartupRunningRow {
    id: String,
    metadata_bytes: u64,
    terminal_at_ms: i64,
}

#[derive(Clone, Copy)]
enum StartupBatch<'a> {
    Reconcile(&'a [StartupRunningRow]),
    Evict(&'a [String]),
    PublishEpoch,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StartupBatchDisposition {
    Committed,
    OverBudget,
}

impl StartupBatch<'_> {
    fn len(self) -> usize {
        match self {
            Self::Reconcile(rows) => rows.len(),
            Self::Evict(ids) => ids.len(),
            Self::PublishEpoch => 1,
        }
    }

    fn prefix(self, len: usize) -> Self {
        match self {
            Self::Reconcile(rows) => Self::Reconcile(&rows[..len]),
            Self::Evict(ids) => Self::Evict(&ids[..len]),
            Self::PublishEpoch => Self::PublishEpoch,
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Reconcile(_) => "running reconciliation",
            Self::Evict(_) => "terminal eviction",
            Self::PublishEpoch => "epoch publication",
        }
    }
}

struct StateStore {
    state_dir: PathBuf,
    database_path: PathBuf,
    wal_path: PathBuf,
    shm_path: PathBuf,
    connection: Connection,
    runtime_id: RuntimeId,
    epoch: String,
    /// Runs handed off live across an exec-in-place upgrade: excluded from
    /// reconciliation so they stay `running`, and their count is the relaxed
    /// target for the post-normalization "running must be zero" guards. Empty
    /// on the crash-recovery path, where every `running` row is reconciled.
    live_set: HashSet<RunId>,
    admission_limits: AdmissionLimits,
    // Fields drop in declaration order: close SQLite before releasing ownership.
    _state_lock: StateLockGuard,
    #[cfg(test)]
    test_hooks: Arc<PersistenceTestHooks>,
}

struct StateLockGuard(File);

impl StateLockGuard {
    fn acquire(lock: File, state_dir: &Path, lock_path: &Path) -> Result<Self, PersistenceError> {
        match lock.try_lock() {
            Ok(()) => Ok(Self(lock)),
            Err(fs::TryLockError::WouldBlock) => {
                Err(PersistenceError::StateInUse(state_dir.to_path_buf()))
            }
            Err(fs::TryLockError::Error(source)) => Err(PersistenceError::io(lock_path, source)),
        }
    }

    /// Adopt a state lock already held on an inherited descriptor (exec-in-place).
    /// The flock is per open-file-description and survived the exec on this fd, so
    /// re-locking would self-deadlock; we take ownership and skip the lock call.
    fn adopt(lock: File) -> Self {
        Self(lock)
    }

    /// The raw fd this lock is held on. An exec-in-place upgrade records it in the
    /// handoff manifest so the inherited flock (per open-file-description, kept
    /// across exec) is adopted by the incoming image rather than re-acquired.
    fn as_raw_fd(&self) -> RawFd {
        self.0.as_raw_fd()
    }
}

impl Drop for StateLockGuard {
    fn drop(&mut self) {
        if let Err(error) = File::unlock(&self.0) {
            eprintln!("ctxmuxd failed to release its state lock: {error}");
        }
    }
}

impl StateStore {
    // `_state_lock` is underscore-prefixed to document that it is held for its
    // Drop side effect (releasing the flock); reading its raw fd for the
    // exec-in-place handoff is a deliberate, narrow exception.
    #[allow(clippy::used_underscore_binding)]
    fn state_lock_raw_fd(&self) -> RawFd {
        self._state_lock.as_raw_fd()
    }

    fn open(
        state_dir: &Path,
        admission_limits: AdmissionLimits,
        mut handoff: Option<HandoffHint>,
        #[cfg(test)] test_hooks: Arc<PersistenceTestHooks>,
    ) -> Result<(Self, Vec<RecoveredRun>), PersistenceError> {
        prepare_state_dir(state_dir)?;
        // On the exec-in-place path the process already holds the advisory lock
        // on this descriptor; adopt it (the flock is per open-file-description,
        // so a fresh open + try_lock would self-deadlock against our own lock).
        let inherited_lock_fd = handoff.as_mut().and_then(|hint| hint.state_lock_fd.take());
        let state_lock = if let Some(inherited_lock_fd) = inherited_lock_fd {
            StateLockGuard::adopt(File::from(inherited_lock_fd))
        } else {
            let lock_path = state_dir.join(LOCK_FILE);
            validate_optional_state_file(&lock_path)?;
            let lock = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .mode(0o600)
                .open(&lock_path)
                .map_err(|source| PersistenceError::io(&lock_path, source))?;
            fs::set_permissions(&lock_path, fs::Permissions::from_mode(0o600))
                .map_err(|source| PersistenceError::io(&lock_path, source))?;
            validate_state_file(&lock_path)?;
            StateLockGuard::acquire(lock, state_dir, &lock_path)?
        };

        let database_path = state_dir.join(DATABASE_FILE);
        let wal_path = state_dir.join(format!("{DATABASE_FILE}-wal"));
        let shm_path = state_dir.join(format!("{DATABASE_FILE}-shm"));
        for path in [&database_path, &wal_path, &shm_path] {
            validate_optional_state_file(path)?;
        }
        let database_existed = database_path.exists();
        // Reuse the handed-off epoch on the exec-in-place path (so reconnecting
        // clients keep passing the instance fence); mint a fresh one otherwise.
        let (epoch, live_set) = match handoff {
            Some(hint) => (hint.epoch, hint.live_set),
            None => (Uuid::new_v4().to_string(), HashSet::new()),
        };
        if database_existed
            && fs::metadata(&database_path)
                .map_err(|source| PersistenceError::io(&database_path, source))?
                .len()
                == 0
        {
            return Err(PersistenceError::Corrupt(
                "existing database file is empty".to_owned(),
            ));
        }

        let connection = Connection::open_with_flags(
            &database_path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(PersistenceError::database)?;
        fs::set_permissions(&database_path, fs::Permissions::from_mode(0o600))
            .map_err(|source| PersistenceError::io(&database_path, source))?;
        connection
            .execute_batch("PRAGMA foreign_keys=ON; PRAGMA busy_timeout=0;")
            .map_err(PersistenceError::database)?;
        let runtime_id = if database_existed {
            validate_existing_schema(&connection)?
        } else {
            create_schema(&connection, &epoch)?
        };
        connection
            .pragma_update(
                None,
                "max_page_count",
                i64::try_from(DATABASE_MAX_PAGES).expect("database page limit fits SQLite"),
            )
            .map_err(PersistenceError::database)?;
        connection
            .execute_batch(
                "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA wal_autocheckpoint=0;",
            )
            .map_err(PersistenceError::database)?;
        for path in [&database_path, &wal_path, &shm_path] {
            if path.exists() {
                fs::set_permissions(path, fs::Permissions::from_mode(0o600))
                    .map_err(|source| PersistenceError::io(path, source))?;
                validate_state_file(path)?;
            }
        }
        validate_quick_check(&connection)?;
        validate_application_state(&connection)?;
        validate_physical_limits(state_dir, &database_path, &wal_path, &shm_path)?;

        let mut store = Self {
            state_dir: state_dir.to_path_buf(),
            database_path,
            wal_path,
            shm_path,
            connection,
            runtime_id,
            epoch,
            live_set,
            admission_limits,
            _state_lock: state_lock,
            #[cfg(test)]
            test_hooks,
        };
        store.normalize_startup()?;
        validate_application_state(&store.connection)?;
        store.validate_operational_state()?;
        let recovered = load_recovered(&store.connection)?;
        store.validate_files()?;
        Ok((store, recovered))
    }

    fn normalize_startup(&mut self) -> Result<(), PersistenceError> {
        let terminal_at_ms = self.startup_terminal_anchor()?;
        loop {
            let running =
                self.load_startup_running_prefix(STARTUP_BATCH_MAX_ROWS, terminal_at_ms)?;
            if running.is_empty() {
                break;
            }
            self.commit_startup_with_reduction(StartupBatch::Reconcile(&running))?;
        }
        loop {
            let candidates = self.load_startup_eviction_prefix(STARTUP_BATCH_MAX_ROWS)?;
            if candidates.is_empty() {
                break;
            }
            self.commit_startup_with_reduction(StartupBatch::Evict(&candidates))?;
        }
        match self.commit_startup_batch(StartupBatch::PublishEpoch)? {
            StartupBatchDisposition::Committed => Ok(()),
            StartupBatchDisposition::OverBudget => Err(PersistenceError::Mutation(
                "startup epoch publication exceeds the 8 MiB WAL charge".to_owned(),
            )),
        }
    }

    fn load_startup_running_prefix(
        &self,
        limit: usize,
        terminal_at_ms: i64,
    ) -> Result<Vec<StartupRunningRow>, PersistenceError> {
        let interrupted = RunState::Interrupted {
            reason: InterruptionReason::DaemonRestart,
        };
        let state_json =
            serde_json::to_string(&interrupted).map_err(PersistenceError::serialization)?;
        let limit = i64::try_from(limit).expect("startup batch limit fits SQLite");
        // Handed-off live Runs are excluded from reconciliation so they stay
        // `running`. rusqlite has no array binding (carray is not compiled in),
        // so we splice one `?` placeholder per live RunId. An empty live-set
        // keeps the query clause-free — byte-identical to a cold restart, and
        // avoiding an invalid dangling `NOT IN ()`.
        let (sql, bindings): (String, Vec<rusqlite::types::Value>) = if self.live_set.is_empty() {
            (
                "SELECT id, creation_key, spec_json, lineage_json, source_epoch, updated_at_ms
                 FROM runs WHERE state_kind = 'running'
                 ORDER BY created_at_ms, id LIMIT ?"
                    .to_owned(),
                vec![rusqlite::types::Value::Integer(limit)],
            )
        } else {
            let placeholders = vec!["?"; self.live_set.len()].join(", ");
            let sql = format!(
                "SELECT id, creation_key, spec_json, lineage_json, source_epoch, updated_at_ms
                 FROM runs WHERE state_kind = 'running' AND id NOT IN ({placeholders})
                 ORDER BY created_at_ms, id LIMIT ?"
            );
            let mut bindings = self
                .live_set
                .iter()
                .map(|id| rusqlite::types::Value::Text(id.to_string()))
                .collect::<Vec<_>>();
            bindings.push(rusqlite::types::Value::Integer(limit));
            (sql, bindings)
        };
        let mut statement = self
            .connection
            .prepare(&sql)
            .map_err(PersistenceError::database)?;
        let rows = statement
            .query_map(params_from_iter(bindings), |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, i64>(5)?,
                ))
            })
            .map_err(PersistenceError::database)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(PersistenceError::database)?;
        rows.into_iter()
            .map(
                |(id, creation_key, spec_json, lineage_json, source_epoch, _)| {
                    Ok(StartupRunningRow {
                        metadata_bytes: metadata_size(
                            &id,
                            &creation_key,
                            &spec_json,
                            lineage_json.as_deref(),
                            &state_json,
                            &source_epoch,
                        )?,
                        id,
                        terminal_at_ms,
                    })
                },
            )
            .collect()
    }

    fn startup_terminal_anchor(&self) -> Result<i64, PersistenceError> {
        let latest: i64 = self
            .connection
            .query_row(
                "SELECT coalesce(max(updated_at_ms), 0) FROM runs",
                [],
                |row| row.get(0),
            )
            .map_err(PersistenceError::database)?;
        Ok(latest.saturating_add(1))
    }

    fn load_startup_eviction_prefix(&self, limit: usize) -> Result<Vec<String>, PersistenceError> {
        let (records, metadata, running): (i64, i64, i64) = self
            .connection
            .query_row(
                "SELECT count(*), coalesce(sum(metadata_bytes), 0),
                        coalesce(sum(state_kind = 'running'), 0) FROM runs",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map_err(PersistenceError::database)?;
        let records = nonnegative_u64(records, "record count")?;
        let metadata = nonnegative_u64(metadata, "metadata total")?;
        // Handed-off live Runs are legitimately still `running` after
        // reconciliation; the crash path passes an empty live-set so this
        // stays the historical "must be zero" check.
        if running != live_count(&self.live_set) {
            return Err(PersistenceError::Corrupt(
                "startup retention ran before every prior running row was interrupted".to_owned(),
            ));
        }
        let records_to_remove = records.saturating_sub(self.admission_limits.run_records);
        let metadata_to_remove = metadata.saturating_sub(self.admission_limits.metadata_bytes);
        if records_to_remove == 0 && metadata_to_remove == 0 {
            return Ok(Vec::new());
        }

        let mut statement = self
            .connection
            .prepare(
                // The candidate pool must agree with the `state_kind != 'running'`
                // eviction DELETE guard: live handed-off Runs are the point of
                // continuity and must never be evicted regardless of budget.
                // (Without this filter a long-quiet live child sorts early by its
                // old updated_at_ms and the DELETE aborts, hitting 0 rows.)
                "SELECT id, metadata_bytes FROM runs
                 WHERE state_kind != 'running'
                 ORDER BY coalesce(terminal_at_ms, updated_at_ms), created_at_ms, id",
            )
            .map_err(PersistenceError::database)?;
        let mut rows = statement.query([]).map_err(PersistenceError::database)?;
        let mut candidates = Vec::new();
        let mut candidate_metadata = 0_u64;
        while let Some(row) = rows.next().map_err(PersistenceError::database)? {
            candidates.push(
                row.get::<_, String>(0)
                    .map_err(PersistenceError::database)?,
            );
            candidate_metadata = candidate_metadata.saturating_add(nonnegative_u64(
                row.get(1).map_err(PersistenceError::database)?,
                "candidate metadata",
            )?);
            let candidate_records =
                u64::try_from(candidates.len()).expect("format record count fits u64");
            if (candidate_records >= records_to_remove && candidate_metadata >= metadata_to_remove)
                || candidates.len() == limit
            {
                return Ok(candidates);
            }
        }
        Err(PersistenceError::Corrupt(
            "terminal history cannot fund the operational startup limits".to_owned(),
        ))
    }

    fn commit_startup_with_reduction(
        &mut self,
        batch: StartupBatch<'_>,
    ) -> Result<(), PersistenceError> {
        let mut len = batch.len();
        loop {
            match self.commit_startup_batch(batch.prefix(len))? {
                StartupBatchDisposition::Committed => return Ok(()),
                StartupBatchDisposition::OverBudget if len > 1 => len /= 2,
                StartupBatchDisposition::OverBudget => {
                    return Err(PersistenceError::Mutation(format!(
                        "one startup {} unit exceeds the 8 MiB WAL charge",
                        batch.label()
                    )));
                }
            }
        }
    }

    fn commit_startup_batch(
        &mut self,
        batch: StartupBatch<'_>,
    ) -> Result<StartupBatchDisposition, PersistenceError> {
        self.truncate_wal_to_zero()?;
        self.connection
            .release_memory()
            .map_err(PersistenceError::database)?;
        let previous_cache_spill = self
            .disable_cache_spill()
            .map_err(|failure| failure.error)?;
        let result = self.commit_startup_batch_with_spill_disabled(batch);
        let restore = self.restore_cache_spill(previous_cache_spill);
        match (result, restore) {
            (Ok(disposition), Ok(())) => Ok(disposition),
            (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
            (Err(error), Err(restore_error)) => Err(combine_errors(&error, &restore_error)),
        }
    }

    fn commit_startup_batch_with_spill_disabled(
        &mut self,
        batch: StartupBatch<'_>,
    ) -> Result<StartupBatchDisposition, PersistenceError> {
        ctxmux_sqlite_status::reset_cache_io(&self.connection)
            .map_err(PersistenceError::database)?;
        self.connection
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(PersistenceError::database)?;
        let initial_wal = match file_len(&self.wal_path) {
            Ok(bytes) => bytes,
            Err(error) => return self.rollback_startup_error(error),
        };
        if initial_wal != 0 {
            return self.rollback_startup_error(PersistenceError::Mutation(
                "persistent WAL changed before startup normalization".to_owned(),
            ));
        }
        if let Err(error) = self.apply_startup_batch(batch) {
            return self.rollback_startup_error(error);
        }
        let snapshot = match ctxmux_sqlite_status::cache_admission_snapshot(&self.connection) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                return self.rollback_startup_error(PersistenceError::database(error));
            }
        };
        let wal_bytes = match file_len(&self.wal_path) {
            Ok(bytes) => bytes,
            Err(error) => return self.rollback_startup_error(error),
        };
        #[cfg(test)]
        let cache_used = self.startup_cache_used(snapshot.used_bytes);
        #[cfg(not(test))]
        let cache_used = snapshot.used_bytes;
        let charge = wal_charge_for_cache(cache_used);
        if wal_bytes != 0 || snapshot.writes != 0 || snapshot.spills != 0 {
            return self.rollback_startup_error(PersistenceError::Mutation(format!(
                "startup normalization violated its no-spill proof: cache writes={}, spills={}, wal={} bytes",
                snapshot.writes, snapshot.spills, wal_bytes
            )));
        }
        let Some(charge) = charge else {
            return self.rollback_startup_error(PersistenceError::Mutation(
                "startup normalization cache charge overflowed".to_owned(),
            ));
        };
        if charge > WAL_CHECKPOINT_BYTES {
            #[cfg(test)]
            self.test_hooks
                .startup_over_budget_attempts
                .fetch_add(1, Ordering::AcqRel);
            self.connection
                .execute_batch("ROLLBACK")
                .map_err(PersistenceError::database)?;
            return Ok(StartupBatchDisposition::OverBudget);
        }
        if let Err(commit_error) = self.connection.execute_batch("COMMIT") {
            let error = PersistenceError::database(commit_error);
            if self.connection.is_autocommit() {
                return Err(error);
            }
            return match self.connection.execute_batch("ROLLBACK") {
                Ok(()) => Err(error),
                Err(rollback_error) => Err(PersistenceError::Mutation(format!(
                    "{error}; startup rollback after COMMIT error failed: {rollback_error}"
                ))),
            };
        }
        let actual_wal = file_len(&self.wal_path)?;
        if actual_wal > charge || actual_wal > WAL_CHECKPOINT_BYTES {
            return Err(PersistenceError::Mutation(format!(
                "startup WAL used {actual_wal} bytes above its admitted {charge} byte charge"
            )));
        }
        self.validate_files()?;
        #[cfg(test)]
        {
            mutex_lock(&self.test_hooks.startup_batch_wal_bytes).push(actual_wal);
            let remaining = self
                .test_hooks
                .startup_fail_after_commits
                .load(Ordering::Acquire);
            if remaining > 0
                && self
                    .test_hooks
                    .startup_fail_after_commits
                    .fetch_sub(1, Ordering::AcqRel)
                    == 1
            {
                return Err(PersistenceError::Mutation(
                    "injected interruption after a committed startup batch".to_owned(),
                ));
            }
        }
        Ok(StartupBatchDisposition::Committed)
    }

    #[cfg(test)]
    fn startup_cache_used(&self, measured_bytes: u64) -> u64 {
        if self
            .test_hooks
            .force_startup_over_budget_once
            .swap(false, Ordering::AcqRel)
        {
            return WAL_CHECKPOINT_BYTES;
        }
        measured_bytes
    }

    fn apply_startup_batch(&self, batch: StartupBatch<'_>) -> Result<(), PersistenceError> {
        let interrupted = serde_json::to_string(&RunState::Interrupted {
            reason: InterruptionReason::DaemonRestart,
        })
        .map_err(PersistenceError::serialization)?;
        match batch {
            StartupBatch::Reconcile(rows) => {
                for row in rows {
                    let changed = self
                        .connection
                        .execute(
                            "UPDATE runs SET state_kind = 'interrupted', state_json = ?2,
                             pid = NULL, terminal_at_ms = ?3, metadata_bytes = ?4
                             WHERE id = ?1 AND state_kind = 'running'",
                            params![
                                row.id,
                                interrupted,
                                row.terminal_at_ms,
                                i64::try_from(row.metadata_bytes)
                                    .expect("metadata budget fits SQLite")
                            ],
                        )
                        .map_err(PersistenceError::database)?;
                    if changed != 1 {
                        return Err(PersistenceError::Mutation(format!(
                            "startup running Run {} changed before reconciliation",
                            row.id
                        )));
                    }
                }
            }
            StartupBatch::Evict(ids) => {
                for id in ids {
                    let changed = self
                        .connection
                        .execute(
                            "DELETE FROM runs WHERE id = ?1 AND state_kind != 'running'",
                            [id],
                        )
                        .map_err(PersistenceError::database)?;
                    if changed != 1 {
                        return Err(PersistenceError::Mutation(format!(
                            "startup terminal Run {id} changed before eviction"
                        )));
                    }
                }
            }
            StartupBatch::PublishEpoch => {
                let changed = self
                    .connection
                    .execute(
                        "UPDATE runtime_meta SET current_epoch = ?1 WHERE singleton = 1",
                        [&self.epoch],
                    )
                    .map_err(PersistenceError::database)?;
                if changed != 1 {
                    return Err(PersistenceError::Corrupt(
                        "runtime metadata singleton is missing".to_owned(),
                    ));
                }
            }
        }
        Ok(())
    }

    fn rollback_startup_error(
        &self,
        error: PersistenceError,
    ) -> Result<StartupBatchDisposition, PersistenceError> {
        match self.connection.execute_batch("ROLLBACK") {
            Ok(()) => Err(error),
            Err(rollback_error) => Err(PersistenceError::Mutation(format!(
                "{error}; startup normalization rollback failed: {rollback_error}"
            ))),
        }
    }

    fn validate_operational_state(&self) -> Result<(), PersistenceError> {
        let (records, metadata, running): (i64, i64, i64) = self
            .connection
            .query_row(
                "SELECT count(*), coalesce(sum(metadata_bytes), 0),
                        coalesce(sum(state_kind = 'running'), 0) FROM runs",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map_err(PersistenceError::database)?;
        let (runtime_id, current_epoch): (String, String) = self
            .connection
            .query_row(
                "SELECT runtime_id, current_epoch FROM runtime_meta WHERE singleton = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(PersistenceError::database)?;
        if nonnegative_u64(records, "record count")? > self.admission_limits.run_records
            || nonnegative_u64(metadata, "metadata total")? > self.admission_limits.metadata_bytes
            || running != live_count(&self.live_set)
            || runtime_id != self.runtime_id.to_string()
            || current_epoch != self.epoch
        {
            return Err(PersistenceError::Corrupt(
                "startup normalization did not reach the operational state".to_owned(),
            ));
        }
        Ok(())
    }

    fn drive_staged_start(
        &mut self,
        prepared: &PreparedPersistentStart,
        candidates: &[PersistentCandidate],
        receipt: &StartReceipt,
        ready: &mpsc::SyncSender<Result<(), StageFailure>>,
        decision: &mpsc::Receiver<StageDecision>,
    ) -> StageDriveResult {
        if let Err(error) = self.validate_prepared_start(prepared) {
            let _ = receipt.decide(StartDisposition::NotCommitted);
            return StageDriveResult::ReadyFailed(StageFailure {
                error,
                fatal: true,
                capacity: false,
            });
        }
        if prepared.metadata_bytes > self.admission_limits.metadata_bytes {
            let _ = receipt.decide(StartDisposition::NotCommitted);
            return StageDriveResult::ReadyFailed(admission_failure(format!(
                "one Run metadata record exceeds the {} byte budget",
                self.admission_limits.metadata_bytes
            )));
        }

        if let Err(error) = self.truncate_wal_to_zero() {
            let _ = receipt.decide(StartDisposition::NotCommitted);
            return StageDriveResult::ReadyFailed(admission_failure(format!(
                "persistent WAL admission could not reach a zero baseline: {error}"
            )));
        }
        if let Err(error) = self.connection.release_memory() {
            let _ = receipt.decide(StartDisposition::NotCommitted);
            return StageDriveResult::ReadyFailed(admission_failure(format!(
                "persistent WAL admission could not release the connection cache: {error}"
            )));
        }

        let previous_cache_spill = match self.disable_cache_spill() {
            Ok(value) => value,
            Err(stage_failure) => {
                let _ = receipt.decide(StartDisposition::NotCommitted);
                return StageDriveResult::ReadyFailed(stage_failure);
            }
        };
        let result = self
            .drive_staged_start_with_spill_disabled(prepared, candidates, receipt, ready, decision);
        match self.restore_cache_spill(previous_cache_spill) {
            Ok(()) => result,
            Err(error) => result.with_restore_failure(error),
        }
    }

    fn drive_staged_start_with_spill_disabled(
        &mut self,
        prepared: &PreparedPersistentStart,
        candidates: &[PersistentCandidate],
        receipt: &StartReceipt,
        ready: &mpsc::SyncSender<Result<(), StageFailure>>,
        decision: &mpsc::Receiver<StageDecision>,
    ) -> StageDriveResult {
        if let Err(error) = ctxmux_sqlite_status::reset_cache_io(&self.connection) {
            let _ = receipt.decide(StartDisposition::NotCommitted);
            return StageDriveResult::ReadyFailed(admission_failure(format!(
                "persistent WAL admission could not reset cache counters: {error}"
            )));
        }
        if let Err(error) = self.connection.execute_batch("BEGIN IMMEDIATE") {
            let _ = receipt.decide(StartDisposition::NotCommitted);
            return StageDriveResult::ReadyFailed(StageFailure {
                error: PersistenceError::database(error),
                fatal: false,
                capacity: false,
            });
        }
        match file_len(&self.wal_path) {
            Ok(0) => {}
            Ok(_) => {
                return self.rollback_before_ready(
                    receipt,
                    admission_failure("persistent WAL changed before exact staging"),
                );
            }
            Err(error) => {
                return self.rollback_before_ready(
                    receipt,
                    admission_failure(format!(
                        "persistent WAL baseline could not be inspected: {error}"
                    )),
                );
            }
        }
        if let Err(stage_failure) = self.stage_exact_replacement(prepared, candidates) {
            return self.rollback_before_ready(receipt, stage_failure);
        }

        let snapshot = match ctxmux_sqlite_status::cache_admission_snapshot(&self.connection) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                return self.rollback_before_ready(
                    receipt,
                    admission_failure(format!(
                        "persistent WAL admission could not observe cache status: {error}"
                    )),
                );
            }
        };
        let wal_bytes = match file_len(&self.wal_path) {
            Ok(bytes) => bytes,
            Err(error) => {
                return self.rollback_before_ready(
                    receipt,
                    admission_failure(format!(
                        "persistent WAL admission could not inspect its baseline: {error}"
                    )),
                );
            }
        };
        let charge = wal_charge_for_cache(snapshot.used_bytes);
        if wal_bytes != 0
            || snapshot.writes != 0
            || snapshot.spills != 0
            || charge.is_none_or(|charge| charge > WAL_CHECKPOINT_BYTES)
        {
            return self.rollback_before_ready(
                receipt,
                admission_failure(format!(
                    "persistent exact replacement exceeds or cannot prove its 8 MiB WAL charge: \
                     cache={} bytes, writes={}, spills={}, wal={} bytes",
                    snapshot.used_bytes, snapshot.writes, snapshot.spills, wal_bytes
                )),
            );
        }

        if ready.send(Ok(())).is_err() {
            return self.rollback_after_ready_loss(receipt);
        }
        match decision.recv().unwrap_or(StageDecision::Abort) {
            StageDecision::Abort => self.abort_staged_start(receipt),
            StageDecision::Commit => self.commit_staged_start(prepared, candidates, receipt),
        }
    }

    fn validate_prepared_start(
        &self,
        prepared: &PreparedPersistentStart,
    ) -> Result<(), PersistenceError> {
        prepared.operation_key.validate().map_err(|error| {
            PersistenceError::Mutation(format!("invalid Run creation operation key: {error}"))
        })?;
        if prepared.epoch != self.epoch {
            return Err(PersistenceError::Mutation(
                "prepared Run start belongs to another daemon epoch".to_owned(),
            ));
        }
        let _ = decode_native_spec(prepared.id, &prepared.spec_json)?;
        let state: RunState =
            serde_json::from_str(&prepared.state_json).map_err(PersistenceError::serialization)?;
        if state != RunState::Running {
            return Err(PersistenceError::Mutation(
                "prepared persistent Run start is not running".to_owned(),
            ));
        }
        if let Some(lineage_json) = &prepared.lineage_json {
            let lineage: RunLineage =
                serde_json::from_str(lineage_json).map_err(PersistenceError::serialization)?;
            if lineage.parent == prepared.id {
                return Err(PersistenceError::Mutation(
                    "prepared persistent Run has self lineage".to_owned(),
                ));
            }
        }
        let measured = metadata_size(
            &prepared.id.to_string(),
            prepared.operation_key.as_str(),
            &prepared.spec_json,
            prepared.lineage_json.as_deref(),
            &prepared.state_json,
            &prepared.epoch,
        )?;
        if measured != prepared.metadata_bytes {
            return Err(PersistenceError::Mutation(
                "prepared persistent Run metadata accounting changed".to_owned(),
            ));
        }
        Ok(())
    }

    fn disable_cache_spill(&self) -> Result<i64, StageFailure> {
        let previous = self
            .connection
            .pragma_query_value(None, "cache_spill", |row| row.get(0))
            .map_err(|error| {
                admission_failure(format!(
                    "persistent WAL admission could not read cache spill state: {error}"
                ))
            })?;
        self.connection
            .pragma_update(None, "cache_spill", false)
            .map_err(|error| {
                admission_failure(format!(
                    "persistent WAL admission could not disable cache spill: {error}"
                ))
            })?;
        let disabled: Result<i64, PersistenceError> = self
            .connection
            .pragma_query_value(None, "cache_spill", |row| row.get(0))
            .map_err(PersistenceError::database);
        let disabled = match disabled {
            Ok(value) => value,
            Err(error) => {
                return match self.restore_cache_spill(previous) {
                    Ok(()) => Err(admission_failure(format!(
                        "persistent WAL admission could not verify disabled cache spill: {error}"
                    ))),
                    Err(restore_error) => Err(StageFailure {
                        error: combine_errors(&error, &restore_error),
                        fatal: true,
                        capacity: false,
                    }),
                };
            }
        };
        if disabled != 0 {
            let error =
                PersistenceError::Mutation("SQLite cache spill remained enabled".to_owned());
            return match self.restore_cache_spill(previous) {
                Ok(()) => Err(admission_failure(error.to_string())),
                Err(restore_error) => Err(StageFailure {
                    error: combine_errors(&error, &restore_error),
                    fatal: true,
                    capacity: false,
                }),
            };
        }
        Ok(previous)
    }

    fn restore_cache_spill(&self, previous: i64) -> Result<(), PersistenceError> {
        self.connection
            .pragma_update(None, "cache_spill", previous)
            .map_err(PersistenceError::database)?;
        Ok(())
    }

    fn truncate_wal_to_zero(&self) -> Result<(), PersistenceError> {
        let (busy, _, _): (i64, i64, i64) = self
            .connection
            .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .map_err(PersistenceError::database)?;
        if busy != 0 || file_len(&self.wal_path)? != 0 {
            return Err(PersistenceError::Mutation(
                "WAL truncate checkpoint could not reach zero bytes".to_owned(),
            ));
        }
        Ok(())
    }

    fn stage_exact_replacement(
        &self,
        prepared: &PreparedPersistentStart,
        candidates: &[PersistentCandidate],
    ) -> Result<(), StageFailure> {
        self.validate_exact_candidates(prepared, candidates)?;
        self.validate_projected_capacity(prepared, candidates)?;
        self.apply_exact_replacement(prepared, candidates)
    }

    fn validate_exact_candidates(
        &self,
        prepared: &PreparedPersistentStart,
        candidates: &[PersistentCandidate],
    ) -> Result<(), StageFailure> {
        let mut seen = HashSet::new();
        for candidate in candidates {
            if !seen.insert(candidate.id) {
                return Err(fatal_stage_failure(format!(
                    "persistent replacement repeats candidate {}",
                    candidate.id
                )));
            }
            if candidate.id == prepared.id
                || candidate.operation_key.as_str().as_bytes()
                    == prepared.operation_key.as_str().as_bytes()
            {
                return Err(fatal_stage_failure(
                    "persistent replacement cannot reuse a candidate Run or creation identity",
                ));
            }
            let stored: Option<(String, i64, String, String)> = self
                .connection
                .query_row(
                    "SELECT creation_key, metadata_bytes, state_kind, state_json
                     FROM runs WHERE id = ?1",
                    [candidate.id.to_string()],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .optional()
                .map_err(|error| fatal_stage_failure(error.to_string()))?;
            let Some((stored_key, stored_metadata, state_kind, state_json)) = stored else {
                return Err(fatal_stage_failure(format!(
                    "persistent replacement candidate {} is missing",
                    candidate.id
                )));
            };
            let stored_metadata = nonnegative_u64(stored_metadata, "candidate metadata")
                .map_err(|error| fatal_stage_failure(error.to_string()))?;
            let state: RunState = serde_json::from_str(&state_json)
                .map_err(|error| fatal_stage_failure(error.to_string()))?;
            if stored_key.as_bytes() != candidate.operation_key.as_str().as_bytes()
                || stored_metadata != candidate.metadata_bytes
                || state_kind != state_kind_for(&state)
                || state.is_running()
            {
                return Err(fatal_stage_failure(format!(
                    "persistent replacement candidate {} does not match its exact terminal snapshot",
                    candidate.id
                )));
            }
        }
        Ok(())
    }

    fn validate_projected_capacity(
        &self,
        prepared: &PreparedPersistentStart,
        candidates: &[PersistentCandidate],
    ) -> Result<(), StageFailure> {
        let (records, metadata): (i64, i64) = self
            .connection
            .query_row(
                "SELECT count(*), coalesce(sum(metadata_bytes), 0) FROM runs",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(|error| fatal_stage_failure(error.to_string()))?;
        let records = nonnegative_u64(records, "record count")
            .map_err(|error| fatal_stage_failure(error.to_string()))?;
        let metadata = nonnegative_u64(metadata, "metadata total")
            .map_err(|error| fatal_stage_failure(error.to_string()))?;
        let candidate_metadata = candidates.iter().try_fold(0_u64, |total, candidate| {
            total
                .checked_add(candidate.metadata_bytes)
                .ok_or_else(|| fatal_stage_failure("candidate metadata accounting overflowed"))
        })?;
        let candidate_records = u64::try_from(candidates.len())
            .map_err(|_| fatal_stage_failure("candidate record count does not fit u64"))?;
        let projected_records = records
            .checked_sub(candidate_records)
            .and_then(|records| records.checked_add(1))
            .ok_or_else(|| fatal_stage_failure("projected record count is inconsistent"))?;
        let projected_metadata = metadata
            .checked_sub(candidate_metadata)
            .and_then(|metadata| metadata.checked_add(prepared.metadata_bytes))
            .ok_or_else(|| fatal_stage_failure("projected metadata is inconsistent"))?;
        if projected_records > self.admission_limits.run_records
            || projected_metadata > self.admission_limits.metadata_bytes
        {
            return Err(admission_failure(
                "exact persistent candidates do not fund the retained Run capacity",
            ));
        }
        Ok(())
    }

    fn apply_exact_replacement(
        &self,
        prepared: &PreparedPersistentStart,
        candidates: &[PersistentCandidate],
    ) -> Result<(), StageFailure> {
        for candidate in candidates {
            let deleted = self
                .connection
                .execute(
                    "DELETE FROM runs WHERE id = ?1 AND creation_key = ?2 COLLATE BINARY
                     AND metadata_bytes = ?3 AND state_kind != 'running'",
                    params![
                        candidate.id.to_string(),
                        candidate.operation_key.as_str(),
                        i64::try_from(candidate.metadata_bytes)
                            .expect("metadata budget fits SQLite")
                    ],
                )
                .map_err(|error| fatal_stage_failure(error.to_string()))?;
            if deleted != 1 {
                return Err(fatal_stage_failure(format!(
                    "persistent replacement candidate {} changed while staged",
                    candidate.id
                )));
            }
        }
        let now = now_millis();
        self.connection
            .execute(
                "INSERT INTO runs (
                    id, creation_key, spec_json, lineage_json, state_kind, state_json, source_epoch, pid,
                    durable_first_available_byte, durable_output_bytes, replay_bytes, replay_truncated,
                    metadata_bytes, created_at_ms, updated_at_ms, terminal_at_ms
                 ) VALUES (?1, ?2, ?3, ?4, 'running', ?5, ?6, NULL, 0, 0, 0, 0, ?7, ?8, ?8, NULL)",
                params![
                    prepared.id.to_string(),
                    prepared.operation_key.as_str(),
                    &prepared.spec_json,
                    &prepared.lineage_json,
                    &prepared.state_json,
                    &prepared.epoch,
                    i64::try_from(prepared.metadata_bytes).expect("metadata budget fits SQLite"),
                    now,
                ],
            )
            .map_err(|error| fatal_stage_failure(error.to_string()))?;
        Ok(())
    }

    fn rollback_before_ready(
        &self,
        receipt: &StartReceipt,
        stage_failure: StageFailure,
    ) -> StageDriveResult {
        match self.connection.execute_batch("ROLLBACK") {
            Ok(()) => {
                let _ = receipt.decide(StartDisposition::NotCommitted);
                StageDriveResult::ReadyFailed(stage_failure)
            }
            Err(rollback_error) => {
                let _ = receipt.decide(StartDisposition::CommitUnknown);
                StageDriveResult::ReadyFailed(StageFailure {
                    error: PersistenceError::Mutation(format!(
                        "{}; staged rollback failed: {rollback_error}",
                        stage_failure.error
                    )),
                    fatal: true,
                    capacity: false,
                })
            }
        }
    }

    fn rollback_after_ready_loss(&self, receipt: &StartReceipt) -> StageDriveResult {
        match self.connection.execute_batch("ROLLBACK") {
            Ok(()) => {
                let _ = receipt.decide(StartDisposition::NotCommitted);
                StageDriveResult::Completed(StageCompletion::NotCommitted(StageFailure {
                    error: PersistenceError::ActorStopped,
                    fatal: false,
                    capacity: false,
                }))
            }
            Err(error) => {
                let error = PersistenceError::Mutation(format!(
                    "staged reply owner disappeared and rollback failed: {error}"
                ));
                let _ = receipt.decide(StartDisposition::CommitUnknown);
                StageDriveResult::Completed(StageCompletion::CommitUnknown(error))
            }
        }
    }

    fn abort_staged_start(&self, receipt: &StartReceipt) -> StageDriveResult {
        match self.connection.execute_batch("ROLLBACK") {
            Ok(()) => {
                let _ = receipt.decide(StartDisposition::NotCommitted);
                StageDriveResult::Completed(StageCompletion::NotCommitted(StageFailure {
                    error: PersistenceError::Mutation(
                        "persistent Run start was aborted".to_owned(),
                    ),
                    fatal: false,
                    capacity: false,
                }))
            }
            Err(error) => {
                let error = PersistenceError::Mutation(format!(
                    "persistent Run start abort could not prove rollback: {error}"
                ));
                let _ = receipt.decide(StartDisposition::CommitUnknown);
                StageDriveResult::Completed(StageCompletion::CommitUnknown(error))
            }
        }
    }

    fn commit_staged_start(
        &self,
        prepared: &PreparedPersistentStart,
        candidates: &[PersistentCandidate],
        receipt: &StartReceipt,
    ) -> StageDriveResult {
        #[cfg(test)]
        if self
            .test_hooks
            .fail_next_start_before_commit
            .swap(false, Ordering::AcqRel)
        {
            return match self.connection.execute_batch("ROLLBACK") {
                Ok(()) => {
                    let _ = receipt.decide(StartDisposition::NotCommitted);
                    StageDriveResult::Completed(StageCompletion::NotCommitted(StageFailure {
                        error: PersistenceError::Mutation(
                            "injected failure before durable Run creation COMMIT".to_owned(),
                        ),
                        fatal: false,
                        capacity: false,
                    }))
                }
                Err(rollback_error) => {
                    let error = PersistenceError::Mutation(format!(
                        "injected failure before durable Run creation COMMIT and rollback failed: \
                         {rollback_error}"
                    ));
                    let _ = receipt.decide(StartDisposition::CommitUnknown);
                    StageDriveResult::Completed(StageCompletion::CommitUnknown(error))
                }
            };
        }
        #[cfg(test)]
        self.crash_start_commit_if_armed(StartCommitCrashPhase::Before);
        #[cfg(test)]
        let commit_result = match mutex_lock(&self.test_hooks.fail_next_start_commit_as).take() {
            Some(durable_unit) => self.inject_start_commit_error(prepared, durable_unit),
            None => self.connection.execute_batch("COMMIT"),
        };
        #[cfg(not(test))]
        let commit_result = self.connection.execute_batch("COMMIT");
        match commit_result {
            Ok(()) => {
                #[cfg(test)]
                self.crash_start_commit_if_armed(StartCommitCrashPhase::After);
                let _ = receipt.decide(StartDisposition::Committed);
                let mut post_commit_error = None;
                #[cfg(test)]
                if self
                    .test_hooks
                    .fail_next_insert_after_commit
                    .swap(false, Ordering::AcqRel)
                {
                    post_commit_error = Some(PersistenceError::Mutation(
                        "injected failure after durable Run creation commit".to_owned(),
                    ));
                }
                if post_commit_error.is_none() {
                    post_commit_error = self.validate_files().err();
                }
                StageDriveResult::Completed(StageCompletion::Committed(post_commit_error))
            }
            Err(commit_error) => {
                self.classify_failed_commit(prepared, candidates, receipt, commit_error)
            }
        }
    }

    #[cfg(test)]
    fn crash_start_commit_if_armed(&self, phase: StartCommitCrashPhase) {
        if self
            .test_hooks
            .start_commit_crash_phase
            .compare_exchange(phase as u8, 0, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            std::process::abort();
        }
    }

    #[cfg(test)]
    fn inject_start_commit_error(
        &self,
        prepared: &PreparedPersistentStart,
        durable_unit: CommitProbe,
    ) -> rusqlite::Result<()> {
        match durable_unit {
            CommitProbe::OldUnit => self.connection.execute_batch("ROLLBACK")?,
            CommitProbe::NewUnit => self.connection.execute_batch("COMMIT")?,
            CommitProbe::Hybrid => {
                self.connection.execute_batch("ROLLBACK; BEGIN IMMEDIATE")?;
                self.apply_exact_replacement(prepared, &[])
                    .unwrap_or_else(|failure| {
                        panic!(
                            "failed to construct old+new COMMIT fixture: {}",
                            failure.error
                        )
                    });
                self.connection.execute_batch("COMMIT")?;
            }
        }
        Err(rusqlite::Error::ExecuteReturnedResults)
    }

    fn classify_failed_commit(
        &self,
        prepared: &PreparedPersistentStart,
        candidates: &[PersistentCandidate],
        receipt: &StartReceipt,
        commit_error: rusqlite::Error,
    ) -> StageDriveResult {
        if !self.connection.is_autocommit()
            && let Err(rollback_error) = self.connection.execute_batch("ROLLBACK")
        {
            let _ = receipt.decide(StartDisposition::CommitUnknown);
            return StageDriveResult::Completed(StageCompletion::CommitUnknown(
                PersistenceError::Mutation(format!(
                    "persistent COMMIT failed ({commit_error}) and rollback failed ({rollback_error})"
                )),
            ));
        }
        match self.probe_exact_replacement(prepared, candidates) {
            Ok(CommitProbe::OldUnit) => {
                let _ = receipt.decide(StartDisposition::NotCommitted);
                StageDriveResult::Completed(StageCompletion::NotCommitted(StageFailure {
                    error: PersistenceError::database(commit_error),
                    fatal: false,
                    capacity: false,
                }))
            }
            Ok(CommitProbe::NewUnit) => {
                let _ = receipt.decide(StartDisposition::Committed);
                StageDriveResult::Completed(StageCompletion::Committed(Some(
                    PersistenceError::database(commit_error),
                )))
            }
            Ok(CommitProbe::Hybrid) => {
                let _ = receipt.decide(StartDisposition::CommitUnknown);
                StageDriveResult::Completed(StageCompletion::CommitUnknown(
                    PersistenceError::Mutation(format!(
                        "persistent COMMIT failed ({commit_error}) and durable rows are hybrid"
                    )),
                ))
            }
            Err(probe_error) => {
                let _ = receipt.decide(StartDisposition::CommitUnknown);
                StageDriveResult::Completed(StageCompletion::CommitUnknown(
                    PersistenceError::Mutation(format!(
                        "persistent COMMIT failed ({commit_error}) and exact probe failed: {probe_error}"
                    )),
                ))
            }
        }
    }

    fn probe_exact_replacement(
        &self,
        prepared: &PreparedPersistentStart,
        candidates: &[PersistentCandidate],
    ) -> Result<CommitProbe, PersistenceError> {
        let mut old_present = 0_usize;
        for candidate in candidates {
            let stored: Option<(String, i64, String, String)> = self
                .connection
                .query_row(
                    "SELECT creation_key, metadata_bytes, state_kind, state_json
                     FROM runs WHERE id = ?1",
                    [candidate.id.to_string()],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .optional()
                .map_err(PersistenceError::database)?;
            if let Some((key, metadata, state_kind, state_json)) = stored {
                let state: RunState = match serde_json::from_str(&state_json) {
                    Ok(state) => state,
                    Err(_) => return Ok(CommitProbe::Hybrid),
                };
                if key.as_bytes() != candidate.operation_key.as_str().as_bytes()
                    || nonnegative_u64(metadata, "candidate metadata")? != candidate.metadata_bytes
                    || state_kind != state_kind_for(&state)
                    || state.is_running()
                {
                    return Ok(CommitProbe::Hybrid);
                }
                old_present += 1;
            }
        }
        let new: Option<StoredPreparedRow> = self
            .connection
            .query_row(
                "SELECT creation_key, spec_json, lineage_json, state_kind, state_json,
                            source_epoch, pid, metadata_bytes FROM runs WHERE id = ?1",
                [prepared.id.to_string()],
                |row| {
                    Ok(StoredPreparedRow {
                        operation_key: row.get(0)?,
                        spec_json: row.get(1)?,
                        lineage_json: row.get(2)?,
                        state_kind: row.get(3)?,
                        state_json: row.get(4)?,
                        epoch: row.get(5)?,
                        pid: row.get(6)?,
                        metadata_bytes: row.get(7)?,
                    })
                },
            )
            .optional()
            .map_err(PersistenceError::database)?;
        let new_exact = new.as_ref().is_some_and(|row| {
            row.operation_key.as_bytes() == prepared.operation_key.as_str().as_bytes()
                && row.spec_json == prepared.spec_json
                && row.lineage_json == prepared.lineage_json
                && row.state_kind == "running"
                && row.state_json == prepared.state_json
                && row.epoch == prepared.epoch
                && row.pid.is_none()
                && u64::try_from(row.metadata_bytes).ok() == Some(prepared.metadata_bytes)
        });
        if new.is_some() && !new_exact {
            return Ok(CommitProbe::Hybrid);
        }
        match (old_present, candidates.len(), new_exact) {
            (present, expected, false) if present == expected => Ok(CommitProbe::OldUnit),
            (0, _, true) => Ok(CommitProbe::NewUnit),
            _ => Ok(CommitProbe::Hybrid),
        }
    }

    fn append_batch(
        &mut self,
        batch: &[(RunId, OutputReplay, Arc<AtomicU64>)],
    ) -> Result<(), PersistenceError> {
        let mut transaction_batch = Vec::new();
        let mut transaction_payload = 0_usize;
        let mut expected_heads = HashMap::new();
        for (id, replay, durable_head) in batch {
            let groups = split_chunks(&replay.chunks)?;
            if groups.is_empty() {
                if !transaction_batch.is_empty() {
                    self.append_transaction(&transaction_batch, None)?;
                    transaction_batch.clear();
                    transaction_payload = 0;
                    expected_heads.clear();
                }
                self.append_transaction(&[(*id, replay.clone(), Arc::clone(durable_head))], None)?;
                continue;
            }
            for (index, chunks) in groups.iter().enumerate() {
                let is_last = index + 1 == groups.len();
                let partial = OutputReplay {
                    chunks: chunks.clone(),
                    first_available_byte: replay.first_available_byte,
                    latest_output_bytes: if is_last {
                        replay.latest_output_bytes
                    } else {
                        chunks.last().map_or(0, |chunk| chunk.end_byte)
                    },
                    truncated: replay.truncated,
                };
                let partial_payload = replay_payload(&partial);
                let first_byte = partial
                    .chunks
                    .first()
                    .expect("a split replay group is non-empty")
                    .start_byte;
                let expected_head = expected_heads
                    .get(id)
                    .copied()
                    .unwrap_or_else(|| durable_head.load(Ordering::Acquire));
                let is_fresh_contiguous = first_byte == expected_head;
                if !transaction_batch.is_empty()
                    && (transaction_payload.saturating_add(partial_payload)
                        > MAX_TRANSACTION_PAYLOAD_BYTES
                        || !is_fresh_contiguous)
                {
                    self.append_transaction(&transaction_batch, None)?;
                    transaction_batch.clear();
                    transaction_payload = 0;
                    expected_heads.clear();
                }
                if !is_fresh_contiguous {
                    self.append_transaction(&[(*id, partial, Arc::clone(durable_head))], None)?;
                    continue;
                }
                transaction_payload = transaction_payload.saturating_add(partial_payload);
                expected_heads.insert(*id, partial.latest_output_bytes);
                transaction_batch.push((*id, partial, Arc::clone(durable_head)));
            }
        }
        if !transaction_batch.is_empty() {
            self.append_transaction(&transaction_batch, None)?;
        }
        Ok(())
    }

    fn finalize(
        &mut self,
        id: RunId,
        actual_pid: u32,
        replay: &OutputReplay,
        state: &RunState,
        durable_head: &Arc<AtomicU64>,
        metadata_owner: &Arc<AtomicU64>,
    ) -> Result<(), PersistenceError> {
        if state.is_running() {
            return Err(PersistenceError::Mutation(
                "cannot persist a running terminal transition".to_owned(),
            ));
        }
        let missing = self.missing_chunks(id, replay)?;
        let mut prefix = Vec::new();
        let mut final_chunks = Vec::new();
        let mut final_bytes = 0_usize;
        for chunk in missing.into_iter().rev() {
            if final_bytes.saturating_add(chunk.data.len()) <= MAX_TRANSACTION_PAYLOAD_BYTES {
                final_bytes = final_bytes.saturating_add(chunk.data.len());
                final_chunks.push(chunk);
            } else {
                prefix.push(chunk);
            }
        }
        prefix.reverse();
        final_chunks.reverse();
        for chunk_group in split_chunks(&prefix)? {
            let prefix_replay = OutputReplay {
                chunks: chunk_group.clone(),
                first_available_byte: replay.first_available_byte,
                latest_output_bytes: chunk_group.last().map_or(0, |chunk| chunk.end_byte),
                truncated: replay.truncated,
            };
            self.append_transaction(&[(id, prefix_replay, Arc::clone(durable_head))], None)?;
        }
        let terminal_replay = OutputReplay {
            chunks: final_chunks,
            first_available_byte: replay.first_available_byte,
            latest_output_bytes: replay.latest_output_bytes,
            truncated: replay.truncated,
        };
        self.append_transaction(
            &[(id, terminal_replay, Arc::clone(durable_head))],
            Some((id, actual_pid, state, metadata_owner)),
        )
    }

    fn missing_chunks(
        &self,
        id: RunId,
        replay: &OutputReplay,
    ) -> Result<Vec<OutputChunk>, PersistenceError> {
        let durable_head: i64 = self
            .connection
            .query_row(
                "SELECT durable_output_bytes FROM runs WHERE id = ?1",
                [id.to_string()],
                |row| row.get(0),
            )
            .map_err(PersistenceError::database)?;
        let durable_head = u64::try_from(durable_head)
            .map_err(|_| PersistenceError::Corrupt("negative durable head".to_owned()))?;
        Ok(replay
            .chunks
            .iter()
            .filter(|chunk| chunk.end_byte > durable_head)
            .cloned()
            .collect())
    }

    fn append_transaction(
        &mut self,
        batch: &[(RunId, OutputReplay, Arc<AtomicU64>)],
        terminal: Option<(RunId, u32, &RunState, &Arc<AtomicU64>)>,
    ) -> Result<(), PersistenceError> {
        let payload = batch
            .iter()
            .map(|(_, replay, _)| replay_payload(replay))
            .sum::<usize>();
        if payload > MAX_TRANSACTION_PAYLOAD_BYTES {
            return Err(PersistenceError::Mutation(format!(
                "output transaction payload {payload} exceeds the 1 MiB admission ceiling"
            )));
        }
        self.admit_transaction((payload as u64).saturating_mul(4) + 1024 * 1024)?;
        let transaction = self
            .connection
            .transaction()
            .map_err(PersistenceError::database)?;
        let mut cursor_updates = HashMap::new();
        for (id, replay, _) in batch {
            let _ = append_replay(&transaction, *id, replay)?;
            let head = read_run_head(&transaction, *id)?;
            cursor_updates.insert(*id, head);
        }
        let _ = prune_global_replay(&transaction)?;
        let mut terminal_metadata = None;
        if let Some((id, actual_pid, state, metadata_owner)) = terminal {
            let (kind, state_json) = encoded_state(state)?;
            let (id_text, creation_key, spec_json, lineage_json, source_epoch): (
                String,
                String,
                String,
                Option<String>,
                String,
            ) = transaction
                .query_row(
                    "SELECT id, creation_key, spec_json, lineage_json, source_epoch
                     FROM runs WHERE id = ?1",
                    [id.to_string()],
                    |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                        ))
                    },
                )
                .map_err(PersistenceError::database)?;
            let metadata_bytes = metadata_size(
                &id_text,
                &creation_key,
                &spec_json,
                lineage_json.as_deref(),
                &state_json,
                &source_epoch,
            )?;
            let now = now_millis();
            let updated = transaction
                .execute(
                    "UPDATE runs SET state_kind = ?2, state_json = ?3, updated_at_ms = ?4,
                     terminal_at_ms = ?4, metadata_bytes = ?5, pid = ?6
                     WHERE id = ?1 AND state_kind = 'running'",
                    params![
                        id.to_string(),
                        kind,
                        state_json,
                        now,
                        i64::try_from(metadata_bytes).expect("metadata budget fits SQLite"),
                        i64::from(actual_pid),
                    ],
                )
                .map_err(PersistenceError::database)?;
            if updated != 1 {
                return Err(PersistenceError::Mutation(format!(
                    "Run {id} is not durable running state"
                )));
            }
            terminal_metadata = Some((Arc::clone(metadata_owner), metadata_bytes));
        }
        transaction.commit().map_err(PersistenceError::database)?;
        #[cfg(test)]
        self.test_hooks
            .append_transaction_commits
            .fetch_add(1, Ordering::AcqRel);
        for (id, _, durable_head) in batch {
            if let Some(head) = cursor_updates.get(id) {
                durable_head.store(*head, Ordering::Release);
            }
        }
        if let Some((metadata_owner, metadata_bytes)) = terminal_metadata {
            metadata_owner.store(metadata_bytes, Ordering::Release);
        }
        self.finish_transaction()
    }

    fn admit_transaction(&mut self, worst_case_bytes: u64) -> Result<(), PersistenceError> {
        if worst_case_bytes > WAL_CHECKPOINT_BYTES {
            return Err(PersistenceError::Mutation(format!(
                "transaction WAL estimate {worst_case_bytes} exceeds 8 MiB"
            )));
        }
        let wal_bytes = file_len(&self.wal_path)?;
        if wal_bytes > WAL_CHECKPOINT_BYTES {
            let (busy, _, _): (i64, i64, i64) = self
                .connection
                .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                })
                .map_err(PersistenceError::database)?;
            if busy != 0 || file_len(&self.wal_path)? != 0 {
                return Err(PersistenceError::Mutation(
                    "WAL truncate checkpoint could not reach zero bytes".to_owned(),
                ));
            }
        }
        let current = file_len(&self.wal_path)?;
        if current.saturating_add(worst_case_bytes) > WAL_MAX_BYTES {
            return Err(PersistenceError::Mutation(
                "WAL admission would exceed 16 MiB".to_owned(),
            ));
        }
        Ok(())
    }

    fn finish_transaction(&self) -> Result<(), PersistenceError> {
        self.validate_files()
    }

    fn validate_files(&self) -> Result<(), PersistenceError> {
        for path in [&self.database_path, &self.wal_path, &self.shm_path] {
            if path.exists() {
                validate_state_file(path)?;
            }
        }
        validate_physical_limits(
            &self.state_dir,
            &self.database_path,
            &self.wal_path,
            &self.shm_path,
        )
    }
}

fn prepare_state_dir(path: &Path) -> Result<(), PersistenceError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(PersistenceError::InvalidDirectory {
                    path: path.to_path_buf(),
                    message: "path must be a real directory, not a symlink".to_owned(),
                });
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            fs::create_dir(path).map_err(|source| PersistenceError::io(path, source))?;
            fs::set_permissions(path, fs::Permissions::from_mode(0o700))
                .map_err(|source| PersistenceError::io(path, source))?;
        }
        Err(source) => return Err(PersistenceError::io(path, source)),
    }
    let metadata =
        fs::symlink_metadata(path).map_err(|source| PersistenceError::io(path, source))?;
    let expected_uid = rustix::process::geteuid().as_raw();
    if metadata.uid() != expected_uid {
        return Err(PersistenceError::InvalidDirectory {
            path: path.to_path_buf(),
            message: format!(
                "owner {} does not match effective user {expected_uid}",
                metadata.uid()
            ),
        });
    }
    if metadata.permissions().mode() & 0o777 != 0o700 {
        return Err(PersistenceError::InvalidDirectory {
            path: path.to_path_buf(),
            message: "permissions must be exactly 0700".to_owned(),
        });
    }
    Ok(())
}

fn validate_optional_state_file(path: &Path) -> Result<(), PersistenceError> {
    match fs::symlink_metadata(path) {
        Ok(_) => validate_state_file(path),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(PersistenceError::io(path, source)),
    }
}

fn validate_state_file(path: &Path) -> Result<(), PersistenceError> {
    let metadata =
        fs::symlink_metadata(path).map_err(|source| PersistenceError::io(path, source))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(PersistenceError::InvalidDirectory {
            path: path.to_path_buf(),
            message: "state path must be a regular file".to_owned(),
        });
    }
    let expected_uid = rustix::process::geteuid().as_raw();
    if metadata.uid() != expected_uid {
        return Err(PersistenceError::InvalidDirectory {
            path: path.to_path_buf(),
            message: "state file owner does not match the effective user".to_owned(),
        });
    }
    if metadata.permissions().mode() & 0o777 != 0o600 {
        return Err(PersistenceError::InvalidDirectory {
            path: path.to_path_buf(),
            message: "state file permissions must be exactly 0600".to_owned(),
        });
    }
    Ok(())
}

fn create_schema(
    connection: &Connection,
    initial_epoch: &str,
) -> Result<RuntimeId, PersistenceError> {
    let runtime_id = RuntimeId::new();
    connection
        .execute_batch(&format!(
            "PRAGMA page_size={PAGE_SIZE_BYTES};
             PRAGMA auto_vacuum=INCREMENTAL;
             PRAGMA user_version={SCHEMA_VERSION};
             CREATE TABLE runtime_meta (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                schema_version INTEGER NOT NULL,
                runtime_id TEXT NOT NULL,
                current_epoch TEXT NOT NULL
             );
             CREATE TABLE runs (
                id TEXT PRIMARY KEY NOT NULL,
                creation_key TEXT NOT NULL COLLATE BINARY,
                spec_json TEXT NOT NULL,
                lineage_json TEXT,
                state_kind TEXT NOT NULL CHECK (state_kind IN ('running', 'exited', 'interrupted')),
                state_json TEXT NOT NULL,
                source_epoch TEXT NOT NULL,
                pid INTEGER,
                durable_first_available_byte INTEGER NOT NULL CHECK (durable_first_available_byte >= 0),
                durable_output_bytes INTEGER NOT NULL CHECK (durable_output_bytes >= 0),
                replay_bytes INTEGER NOT NULL CHECK (replay_bytes >= 0),
                replay_truncated INTEGER NOT NULL CHECK (replay_truncated IN (0, 1)),
                metadata_bytes INTEGER NOT NULL CHECK (metadata_bytes >= 0),
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                terminal_at_ms INTEGER
             );
             CREATE UNIQUE INDEX runs_creation_key ON runs(creation_key);
             CREATE TABLE replay_chunks (
                ordinal INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
                start_byte INTEGER NOT NULL CHECK (start_byte >= 0),
                end_byte INTEGER NOT NULL CHECK (end_byte > start_byte),
                data BLOB NOT NULL,
                UNIQUE(run_id, start_byte)
             );
             CREATE INDEX replay_chunks_run_start_byte ON replay_chunks(run_id, start_byte);"
        ))
        .map_err(PersistenceError::database)?;
    connection
        .execute(
            "INSERT INTO runtime_meta(singleton, schema_version, runtime_id, current_epoch)
             VALUES (1, ?1, ?2, ?3)",
            params![SCHEMA_VERSION, runtime_id.to_string(), initial_epoch],
        )
        .map_err(PersistenceError::database)?;
    Ok(runtime_id)
}

fn validate_existing_schema(connection: &Connection) -> Result<RuntimeId, PersistenceError> {
    let version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(PersistenceError::database)?;
    if version != SCHEMA_VERSION {
        return Err(PersistenceError::UnsupportedSchema {
            found: version,
            expected: SCHEMA_VERSION,
        });
    }
    let (meta_rows, meta_version, runtime_id, current_epoch): (i64, i64, String, String) = connection
        .query_row(
            "SELECT count(*), min(schema_version), min(runtime_id), min(current_epoch) FROM runtime_meta",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .map_err(PersistenceError::database)?;
    if meta_rows != 1 || meta_version != SCHEMA_VERSION {
        return Err(PersistenceError::UnsupportedSchema {
            found: meta_version,
            expected: SCHEMA_VERSION,
        });
    }
    Uuid::parse_str(&current_epoch).map_err(|_| {
        PersistenceError::Corrupt("runtime metadata has an invalid daemon epoch".to_owned())
    })?;
    let runtime_id = runtime_id.parse().map_err(|_| {
        PersistenceError::Corrupt("runtime metadata has an invalid Runtime identity".to_owned())
    })?;
    let mut statement = connection
        .prepare(
            "SELECT type, name FROM sqlite_schema
             WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name",
        )
        .map_err(PersistenceError::database)?;
    let actual = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(PersistenceError::database)?
        .collect::<Result<BTreeSet<_>, _>>()
        .map_err(PersistenceError::database)?;
    let expected = BTreeSet::from([
        (
            "index".to_owned(),
            "replay_chunks_run_start_byte".to_owned(),
        ),
        ("index".to_owned(), "runs_creation_key".to_owned()),
        ("table".to_owned(), "replay_chunks".to_owned()),
        ("table".to_owned(), "runs".to_owned()),
        ("table".to_owned(), "runtime_meta".to_owned()),
    ]);
    if actual != expected {
        return Err(PersistenceError::Corrupt(format!(
            "schema objects do not match version {SCHEMA_VERSION}: {actual:?}"
        )));
    }
    validate_table_columns(
        connection,
        "runtime_meta",
        &["singleton", "schema_version", "runtime_id", "current_epoch"],
    )?;
    validate_table_columns(
        connection,
        "runs",
        &[
            "id",
            "creation_key",
            "spec_json",
            "lineage_json",
            "state_kind",
            "state_json",
            "source_epoch",
            "pid",
            "durable_first_available_byte",
            "durable_output_bytes",
            "replay_bytes",
            "replay_truncated",
            "metadata_bytes",
            "created_at_ms",
            "updated_at_ms",
            "terminal_at_ms",
        ],
    )?;
    validate_table_columns(
        connection,
        "replay_chunks",
        &["ordinal", "run_id", "start_byte", "end_byte", "data"],
    )?;
    validate_creation_key_index(connection)?;
    validate_database_format_pragmas(connection)?;
    Ok(runtime_id)
}

fn validate_database_format_pragmas(connection: &Connection) -> Result<(), PersistenceError> {
    let page_size: i64 = connection
        .pragma_query_value(None, "page_size", |row| row.get(0))
        .map_err(PersistenceError::database)?;
    if page_size != i64::try_from(PAGE_SIZE_BYTES).expect("SQLite page size fits a signed integer")
    {
        return Err(PersistenceError::Corrupt(format!(
            "database page size is {page_size}, expected {PAGE_SIZE_BYTES}"
        )));
    }
    let auto_vacuum: i64 = connection
        .pragma_query_value(None, "auto_vacuum", |row| row.get(0))
        .map_err(PersistenceError::database)?;
    if auto_vacuum != 2 {
        return Err(PersistenceError::Corrupt(
            "database must use incremental auto-vacuum".to_owned(),
        ));
    }
    Ok(())
}

fn validate_table_columns(
    connection: &Connection,
    table: &str,
    expected: &[&str],
) -> Result<(), PersistenceError> {
    let mut statement = connection
        .prepare(&format!("PRAGMA table_info({table})"))
        .map_err(PersistenceError::database)?;
    let actual = statement
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(PersistenceError::database)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(PersistenceError::database)?;
    if actual != expected {
        return Err(PersistenceError::Corrupt(format!(
            "table {table} columns do not match schema version {SCHEMA_VERSION}"
        )));
    }
    Ok(())
}

fn validate_creation_key_index(connection: &Connection) -> Result<(), PersistenceError> {
    let descriptor: Option<(i64, String, i64)> = connection
        .query_row(
            "SELECT [unique], origin, partial FROM pragma_index_list('runs')
             WHERE name = 'runs_creation_key'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(PersistenceError::database)?;
    if descriptor
        .as_ref()
        .map(|(unique, origin, partial)| (*unique, origin.as_str(), *partial))
        != Some((1, "c", 0))
    {
        return Err(PersistenceError::Corrupt(
            "runs_creation_key must be an explicit non-partial unique index".to_owned(),
        ));
    }

    let mut statement = connection
        .prepare("PRAGMA index_xinfo(runs_creation_key)")
        .map_err(PersistenceError::database)?;
    let key_columns = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, i64>(5)?,
            ))
        })
        .map_err(PersistenceError::database)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(PersistenceError::database)?
        .into_iter()
        .filter(|(_, _, _, _, key)| *key != 0)
        .collect::<Vec<_>>();
    if key_columns
        != vec![(
            1,
            Some("creation_key".to_owned()),
            0,
            "BINARY".to_owned(),
            1,
        )]
    {
        return Err(PersistenceError::Corrupt(
            "runs_creation_key must index creation_key byte-exactly in ascending order".to_owned(),
        ));
    }
    Ok(())
}

fn validate_quick_check(connection: &Connection) -> Result<(), PersistenceError> {
    let result: String = connection
        .query_row("PRAGMA quick_check", [], |row| row.get(0))
        .map_err(PersistenceError::database)?;
    if result != "ok" {
        return Err(PersistenceError::Corrupt(format!(
            "SQLite quick_check returned {result:?}"
        )));
    }
    Ok(())
}

fn validate_application_state(connection: &Connection) -> Result<(), PersistenceError> {
    let mut statement = connection
        .prepare(
            "SELECT id, creation_key, spec_json, lineage_json, state_kind, state_json, source_epoch, pid,
                    durable_first_available_byte, durable_output_bytes, replay_bytes, replay_truncated,
                    metadata_bytes FROM runs ORDER BY id",
        )
        .map_err(PersistenceError::database)?;
    let mut rows = statement.query([]).map_err(PersistenceError::database)?;
    let mut record_count = 0_u64;
    let mut metadata_total = 0_u64;
    let mut replay_total = 0_u64;
    let mut creation_keys = BTreeSet::new();
    while let Some(row) = rows.next().map_err(PersistenceError::database)? {
        record_count += 1;
        let id_text: String = row.get(0).map_err(PersistenceError::database)?;
        let id: RunId = id_text
            .parse()
            .map_err(|_| PersistenceError::Corrupt(format!("invalid Run id {id_text:?}")))?;
        let creation_key_text: String = row.get(1).map_err(PersistenceError::database)?;
        let creation_key = decode_unique_creation_key(id, creation_key_text, &mut creation_keys)?;
        let spec_json: String = row.get(2).map_err(PersistenceError::database)?;
        let _ = decode_native_spec(id, &spec_json)?;
        let lineage_json: Option<String> = row.get(3).map_err(PersistenceError::database)?;
        if let Some(lineage_json) = &lineage_json {
            let lineage: RunLineage = serde_json::from_str(lineage_json).map_err(|error| {
                PersistenceError::Corrupt(format!("invalid lineage for {id}: {error}"))
            })?;
            if lineage.parent == id {
                return Err(PersistenceError::Corrupt(format!(
                    "Run {id} has self lineage"
                )));
            }
        }
        let state_kind: String = row.get(4).map_err(PersistenceError::database)?;
        let state_json: String = row.get(5).map_err(PersistenceError::database)?;
        let state: RunState = serde_json::from_str(&state_json).map_err(|error| {
            PersistenceError::Corrupt(format!("invalid state for {id}: {error}"))
        })?;
        if state_kind != state_kind_for(&state) {
            return Err(PersistenceError::Corrupt(format!(
                "Run {id} state kind does not match its JSON"
            )));
        }
        let source_epoch: String = row.get(6).map_err(PersistenceError::database)?;
        Uuid::parse_str(&source_epoch)
            .map_err(|_| PersistenceError::Corrupt(format!("Run {id} has invalid source epoch")))?;
        let pid: Option<i64> = row.get(7).map_err(PersistenceError::database)?;
        if pid.is_some_and(|pid| u32::try_from(pid).is_err()) {
            return Err(PersistenceError::Corrupt(format!(
                "Run {id} has invalid PID"
            )));
        }
        if matches!(state, RunState::Interrupted { .. }) && pid.is_some() {
            return Err(PersistenceError::Corrupt(format!(
                "interrupted Run {id} retains a PID"
            )));
        }
        let oldest = nonnegative_u64(row.get(8).map_err(PersistenceError::database)?, "oldest")?;
        let head = nonnegative_u64(row.get(9).map_err(PersistenceError::database)?, "head")?;
        let replay_bytes = nonnegative_u64(
            row.get(10).map_err(PersistenceError::database)?,
            "replay bytes",
        )?;
        let truncated: i64 = row.get(11).map_err(PersistenceError::database)?;
        if !matches!(truncated, 0 | 1) {
            return Err(PersistenceError::Corrupt(format!(
                "Run {id} has invalid replay truncation flag"
            )));
        }
        let stored_metadata = nonnegative_u64(
            row.get(12).map_err(PersistenceError::database)?,
            "metadata bytes",
        )?;
        let actual_metadata = metadata_size(
            &id_text,
            creation_key.as_str(),
            &spec_json,
            lineage_json.as_deref(),
            &state_json,
            &source_epoch,
        )?;
        if stored_metadata != actual_metadata {
            return Err(PersistenceError::Corrupt(format!(
                "Run {id} metadata accounting does not match"
            )));
        }
        validate_replay_window(connection, id, oldest, head, replay_bytes, truncated != 0)?;
        metadata_total = metadata_total.saturating_add(stored_metadata);
        replay_total = replay_total.saturating_add(replay_bytes);
    }
    if record_count > RUN_RECORDS
        || metadata_total > METADATA_BYTES
        || replay_total > GLOBAL_REPLAY_BYTES
    {
        return Err(PersistenceError::Corrupt(
            "stored logical quota accounting exceeds the format limits".to_owned(),
        ));
    }
    Ok(())
}

fn decode_unique_creation_key(
    id: RunId,
    value: String,
    seen: &mut BTreeSet<String>,
) -> Result<CreateOperationKey, PersistenceError> {
    let creation_key = value.parse().map_err(|error| {
        PersistenceError::Corrupt(format!(
            "invalid creation operation key for Run {id}: {error}"
        ))
    })?;
    if !seen.insert(value) {
        return Err(PersistenceError::Corrupt(format!(
            "creation operation key is bound to more than one Run including {id}"
        )));
    }
    Ok(creation_key)
}

fn validate_replay_window(
    connection: &Connection,
    id: RunId,
    oldest: u64,
    head: u64,
    replay_bytes: u64,
    truncated: bool,
) -> Result<(), PersistenceError> {
    let mut statement = connection
        .prepare(
            "SELECT start_byte, end_byte, length(data)
             FROM replay_chunks WHERE run_id = ?1 ORDER BY start_byte",
        )
        .map_err(PersistenceError::database)?;
    let chunks = statement
        .query_map([id.to_string()], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })
        .map_err(PersistenceError::database)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(PersistenceError::database)?;
    if chunks.is_empty() {
        if oldest != 0 || head != 0 || replay_bytes != 0 || truncated {
            return Err(PersistenceError::Corrupt(format!(
                "Run {id} has empty replay with non-empty cursors"
            )));
        }
        return Ok(());
    }
    let mut expected = oldest;
    let mut bytes = 0_u64;
    for (start_byte, end_byte, len) in chunks {
        let start_byte = nonnegative_u64(start_byte, "chunk start byte")?;
        let end_byte = nonnegative_u64(end_byte, "chunk end byte")?;
        let len = nonnegative_u64(len, "chunk length")?;
        if start_byte != expected || end_byte <= start_byte || end_byte - start_byte != len {
            return Err(PersistenceError::Corrupt(format!(
                "Run {id} replay range [{start_byte}, {end_byte}) is invalid or not contiguous at {expected}"
            )));
        }
        expected = end_byte;
        bytes = bytes.saturating_add(len);
    }
    if expected != head || bytes != replay_bytes {
        return Err(PersistenceError::Corrupt(format!(
            "Run {id} replay cursors or bytes do not match chunks"
        )));
    }
    if oldest > 0 && !truncated {
        return Err(PersistenceError::Corrupt(format!(
            "Run {id} pruned replay is not marked truncated"
        )));
    }
    if replay_bytes > PER_RUN_REPLAY_BYTES {
        return Err(PersistenceError::Corrupt(format!(
            "Run {id} replay exceeds 4 MiB"
        )));
    }
    Ok(())
}

fn load_recovered(connection: &Connection) -> Result<Vec<RecoveredRun>, PersistenceError> {
    let mut statement = connection
        .prepare(
            "SELECT id, creation_key, spec_json, lineage_json, state_json, pid, durable_first_available_byte,
                    durable_output_bytes, replay_truncated, metadata_bytes
             FROM runs
             ORDER BY coalesce(terminal_at_ms, updated_at_ms), created_at_ms, id",
        )
        .map_err(PersistenceError::database)?;
    let mut rows = statement.query([]).map_err(PersistenceError::database)?;
    let mut recovered = Vec::new();
    while let Some(row) = rows.next().map_err(PersistenceError::database)? {
        recovered.push(decode_recovered_row(connection, row)?);
    }
    Ok(recovered)
}

fn decode_recovered_row(
    connection: &Connection,
    row: &rusqlite::Row<'_>,
) -> Result<RecoveredRun, PersistenceError> {
    let id_text: String = row.get(0).map_err(PersistenceError::database)?;
    let id: RunId = id_text
        .parse()
        .map_err(|_| PersistenceError::Corrupt("invalid recovered Run id".to_owned()))?;
    let operation_key = row
        .get::<_, String>(1)
        .map_err(PersistenceError::database)?
        .parse()
        .map_err(|error| {
            PersistenceError::Corrupt(format!(
                "invalid creation operation key for recovered Run {id}: {error}"
            ))
        })?;
    let spec_json = row
        .get::<_, String>(2)
        .map_err(PersistenceError::database)?;
    let spec = decode_native_spec(id, &spec_json)?;
    let lineage = row
        .get::<_, Option<String>>(3)
        .map_err(PersistenceError::database)?
        .map(|value| serde_json::from_str(&value))
        .transpose()
        .map_err(PersistenceError::database)?;
    let state = serde_json::from_str(
        &row.get::<_, String>(4)
            .map_err(PersistenceError::database)?,
    )
    .map_err(PersistenceError::database)?;
    let pid = row
        .get::<_, Option<i64>>(5)
        .map_err(PersistenceError::database)?
        .map(u32::try_from)
        .transpose()
        .map_err(|_| PersistenceError::Corrupt("invalid recovered PID".to_owned()))?;
    let first_available_byte = nonnegative_u64(
        row.get(6).map_err(PersistenceError::database)?,
        "recovered oldest",
    )?;
    let latest_output_bytes = nonnegative_u64(
        row.get(7).map_err(PersistenceError::database)?,
        "recovered head",
    )?;
    let truncated = row.get::<_, i64>(8).map_err(PersistenceError::database)? != 0;
    let metadata_bytes = nonnegative_u64(
        row.get(9).map_err(PersistenceError::database)?,
        "recovered metadata bytes",
    )?;
    Ok(RecoveredRun {
        operation_key,
        info: RunInfo {
            id,
            spec: Some(spec),
            lineage,
            backend: RunBackend::Native,
            capabilities: RunCapabilities::NATIVE,
            pid,
            state,
            latest_output_bytes,
            durable_output_bytes: Some(latest_output_bytes),
            first_available_byte,
            attachments: 0,
            applied_input_bytes: None,
        },
        replay: OutputReplay {
            chunks: load_recovered_chunks(connection, &id_text)?,
            first_available_byte,
            latest_output_bytes,
            truncated,
        },
        metadata_bytes,
    })
}

fn load_recovered_chunks(
    connection: &Connection,
    id: &str,
) -> Result<Vec<OutputChunk>, PersistenceError> {
    let mut statement = connection
        .prepare(
            "SELECT start_byte, end_byte, data
             FROM replay_chunks WHERE run_id = ?1 ORDER BY start_byte",
        )
        .map_err(PersistenceError::database)?;
    statement
        .query_map([id], |row| {
            Ok(OutputChunk {
                start_byte: u64::try_from(row.get::<_, i64>(0)?).map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Integer,
                        Box::new(error),
                    )
                })?,
                end_byte: u64::try_from(row.get::<_, i64>(1)?).map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        1,
                        rusqlite::types::Type::Integer,
                        Box::new(error),
                    )
                })?,
                data: row.get(2)?,
            })
        })
        .map_err(PersistenceError::database)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(PersistenceError::database)
}

fn validate_persistent_start(info: &RunInfo) -> Result<&RunSpec, PersistenceError> {
    if info.backend != RunBackend::Native {
        return Err(PersistenceError::Mutation(
            "persistent Run start must use the native backend".to_owned(),
        ));
    }
    if info.capabilities != RunCapabilities::NATIVE {
        return Err(PersistenceError::Mutation(
            "persistent native Run has invalid capabilities".to_owned(),
        ));
    }
    if !info.state.is_running() {
        return Err(PersistenceError::Mutation(
            "persistent Run start must be running".to_owned(),
        ));
    }
    let spec = info.spec.as_ref().ok_or_else(|| {
        PersistenceError::Mutation(
            "persistent native Run must have a launch specification".to_owned(),
        )
    })?;
    validate_run_spec(spec).map_err(|error| {
        PersistenceError::Mutation(format!(
            "persistent native Run has invalid specification: {error}"
        ))
    })?;
    Ok(spec)
}

fn decode_native_spec(id: RunId, spec_json: &str) -> Result<RunSpec, PersistenceError> {
    let spec: RunSpec = serde_json::from_str(spec_json)
        .map_err(|error| PersistenceError::Corrupt(format!("invalid spec for {id}: {error}")))?;
    validate_run_spec(&spec)
        .map_err(|error| PersistenceError::Corrupt(format!("invalid spec for {id}: {error}")))?;
    Ok(spec)
}

#[allow(
    clippy::too_many_lines,
    reason = "one transaction-local range validation, append, pruning, and cursor update is easier to audit as one invariant"
)]
fn append_replay(
    transaction: &Transaction<'_>,
    id: RunId,
    replay: &OutputReplay,
) -> Result<bool, PersistenceError> {
    let id_text = id.to_string();
    let (mut durable_oldest, mut durable_head, mut replay_bytes, state_kind): (
        i64,
        i64,
        i64,
        String,
    ) = transaction
        .query_row(
            "SELECT durable_first_available_byte, durable_output_bytes, replay_bytes, state_kind
             FROM runs WHERE id = ?1",
            [&id_text],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .map_err(PersistenceError::database)?;
    let durable_head_unsigned = nonnegative_u64(durable_head, "durable head")?;
    if state_kind != "running"
        && replay
            .chunks
            .iter()
            .any(|chunk| chunk.end_byte > durable_head_unsigned)
    {
        return Err(PersistenceError::Mutation(format!(
            "cannot advance replay for terminal Run {id}"
        )));
    }
    for chunk in &replay.chunks {
        let data_len = u64::try_from(chunk.data.len())
            .map_err(|_| PersistenceError::Mutation("output chunk is too large".to_owned()))?;
        if chunk.end_byte <= chunk.start_byte || chunk.end_byte - chunk.start_byte != data_len {
            return Err(PersistenceError::Mutation(format!(
                "Run {id} replay range [{}, {}) does not match its bytes",
                chunk.start_byte, chunk.end_byte
            )));
        }
        let start_byte = i64::try_from(chunk.start_byte).map_err(|_| {
            PersistenceError::Mutation("output start byte exceeds SQLite".to_owned())
        })?;
        let end_byte = i64::try_from(chunk.end_byte)
            .map_err(|_| PersistenceError::Mutation("output end byte exceeds SQLite".to_owned()))?;
        if end_byte <= durable_head {
            if start_byte < durable_oldest {
                return Err(PersistenceError::Mutation(format!(
                    "Run {id} cannot verify evicted replay range [{}, {})",
                    chunk.start_byte, chunk.end_byte
                )));
            }
            let stored: Option<(i64, Vec<u8>)> = transaction
                .query_row(
                    "SELECT end_byte, data FROM replay_chunks
                         WHERE run_id = ?1 AND start_byte = ?2",
                    params![&id_text, start_byte],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .map_err(PersistenceError::database)?;
            if stored.as_ref() != Some(&(end_byte, chunk.data.clone())) {
                return Err(PersistenceError::Mutation(format!(
                    "Run {id} replay range [{}, {}) is missing or changed bytes",
                    chunk.start_byte, chunk.end_byte
                )));
            }
            continue;
        }
        if start_byte != durable_head {
            return Err(PersistenceError::Mutation(format!(
                "Run {id} durable replay gap: got {start_byte}, expected {durable_head}"
            )));
        }
        if durable_head == 0 {
            durable_oldest = start_byte;
        }
        transaction
            .execute(
                "INSERT INTO replay_chunks(run_id, start_byte, end_byte, data)
                 VALUES (?1, ?2, ?3, ?4)",
                params![&id_text, start_byte, end_byte, &chunk.data],
            )
            .map_err(PersistenceError::database)?;
        durable_head = end_byte;
        replay_bytes = replay_bytes.saturating_add(
            i64::try_from(chunk.data.len())
                .map_err(|_| PersistenceError::Mutation("output chunk is too large".to_owned()))?,
        );
    }
    let evicted = prune_run_replay(
        transaction,
        id,
        &id_text,
        &mut durable_oldest,
        &mut replay_bytes,
    )?;
    let truncated = replay.truncated || durable_oldest > 0;
    transaction
        .execute(
            "UPDATE runs SET durable_first_available_byte = ?2, durable_output_bytes = ?3,
             replay_bytes = ?4, replay_truncated = ?5, updated_at_ms = ?6 WHERE id = ?1",
            params![
                &id_text,
                durable_oldest,
                durable_head,
                replay_bytes,
                i64::from(truncated),
                now_millis(),
            ],
        )
        .map_err(PersistenceError::database)?;
    Ok(evicted)
}

fn prune_run_replay(
    transaction: &Transaction<'_>,
    id: RunId,
    id_text: &str,
    durable_oldest: &mut i64,
    replay_bytes: &mut i64,
) -> Result<bool, PersistenceError> {
    prune_run_replay_to(
        transaction,
        id,
        id_text,
        durable_oldest,
        replay_bytes,
        PER_RUN_REPLAY_BYTES,
    )
}

fn prune_run_replay_to(
    transaction: &Transaction<'_>,
    id: RunId,
    id_text: &str,
    durable_oldest: &mut i64,
    replay_bytes: &mut i64,
    replay_limit: u64,
) -> Result<bool, PersistenceError> {
    let mut evicted_any = false;
    while u64::try_from(*replay_bytes).unwrap_or(u64::MAX) > replay_limit {
        let evicted: Option<(i64, i64)> = transaction
            .query_row(
                "SELECT start_byte, length(data) FROM replay_chunks
                 WHERE run_id = ?1 ORDER BY start_byte LIMIT 1",
                [id_text],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(PersistenceError::database)?;
        let Some((start_byte, bytes)) = evicted else {
            return Err(PersistenceError::Corrupt(format!(
                "Run {id} replay accounting has no chunks"
            )));
        };
        transaction
            .execute(
                "DELETE FROM replay_chunks WHERE run_id = ?1 AND start_byte = ?2",
                params![id_text, start_byte],
            )
            .map_err(PersistenceError::database)?;
        evicted_any = true;
        *replay_bytes = replay_bytes.saturating_sub(bytes);
        *durable_oldest = transaction
            .query_row(
                "SELECT coalesce(min(start_byte), 0) FROM replay_chunks WHERE run_id = ?1",
                [id_text],
                |row| row.get(0),
            )
            .map_err(PersistenceError::database)?;
    }
    Ok(evicted_any)
}

fn prune_global_replay(transaction: &Transaction<'_>) -> Result<bool, PersistenceError> {
    prune_global_replay_to(transaction, GLOBAL_REPLAY_BYTES)
}

fn prune_global_replay_to(
    transaction: &Transaction<'_>,
    replay_limit: u64,
) -> Result<bool, PersistenceError> {
    let mut evicted = false;
    loop {
        let total: i64 = transaction
            .query_row(
                "SELECT coalesce(sum(replay_bytes), 0) FROM runs",
                [],
                |row| row.get(0),
            )
            .map_err(PersistenceError::database)?;
        if nonnegative_u64(total, "global replay bytes")? <= replay_limit {
            return Ok(evicted);
        }
        let candidate: Option<(i64, String, i64, i64)> = transaction
            .query_row(
                "SELECT chunk.ordinal, chunk.run_id, chunk.start_byte, length(chunk.data)
                 FROM replay_chunks AS chunk
                 WHERE (SELECT count(*) FROM replay_chunks AS retained
                        WHERE retained.run_id = chunk.run_id) > 1
                 ORDER BY chunk.ordinal LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()
            .map_err(PersistenceError::database)?;
        let Some((ordinal, run_id, _start_byte, bytes)) = candidate else {
            return Err(PersistenceError::Corrupt(
                "global replay accounting has no chunks".to_owned(),
            ));
        };
        transaction
            .execute("DELETE FROM replay_chunks WHERE ordinal = ?1", [ordinal])
            .map_err(PersistenceError::database)?;
        evicted = true;
        transaction
            .execute(
                "UPDATE runs SET replay_bytes = replay_bytes - ?2, replay_truncated = 1,
                 durable_first_available_byte = coalesce(
                   (SELECT min(start_byte) FROM replay_chunks WHERE run_id = ?1), 0
                 ) WHERE id = ?1",
                params![run_id, bytes],
            )
            .map_err(PersistenceError::database)?;
    }
}

fn read_run_head(transaction: &Transaction<'_>, id: RunId) -> Result<u64, PersistenceError> {
    let value: i64 = transaction
        .query_row(
            "SELECT durable_output_bytes FROM runs WHERE id = ?1",
            [id.to_string()],
            |row| row.get(0),
        )
        .map_err(PersistenceError::database)?;
    nonnegative_u64(value, "durable head")
}

fn encoded_state(state: &RunState) -> Result<(&'static str, String), PersistenceError> {
    Ok((
        state_kind_for(state),
        serde_json::to_string(state).map_err(PersistenceError::serialization)?,
    ))
}

const fn state_kind_for(state: &RunState) -> &'static str {
    match state {
        RunState::Running => "running",
        RunState::Exited { .. } => "exited",
        RunState::Interrupted { .. } => "interrupted",
    }
}

fn metadata_size(
    id: &str,
    creation_key: &str,
    spec: &str,
    lineage: Option<&str>,
    state: &str,
    epoch: &str,
) -> Result<u64, PersistenceError> {
    u64::try_from(
        id.len()
            .saturating_add(creation_key.len())
            .saturating_add(spec.len())
            .saturating_add(lineage.map_or(0, str::len))
            .saturating_add(state.len().max(LIFECYCLE_METADATA_RESERVE_BYTES))
            .saturating_add(epoch.len()),
    )
    .map_err(|_| PersistenceError::Mutation("metadata size overflow".to_owned()))
}

fn split_chunks(chunks: &[OutputChunk]) -> Result<Vec<Vec<OutputChunk>>, PersistenceError> {
    let mut groups = Vec::new();
    let mut current = Vec::new();
    let mut bytes = 0_usize;
    for chunk in chunks {
        if chunk.data.len() > MAX_TRANSACTION_PAYLOAD_BYTES {
            return Err(PersistenceError::Mutation(format!(
                "output range [{}, {}) exceeds the transaction payload ceiling",
                chunk.start_byte, chunk.end_byte
            )));
        }
        if !current.is_empty()
            && bytes.saturating_add(chunk.data.len()) > MAX_TRANSACTION_PAYLOAD_BYTES
        {
            groups.push(std::mem::take(&mut current));
            bytes = 0;
        }
        bytes = bytes.saturating_add(chunk.data.len());
        current.push(chunk.clone());
    }
    if !current.is_empty() {
        groups.push(current);
    }
    Ok(groups)
}

fn nonnegative_u64(value: i64, label: &str) -> Result<u64, PersistenceError> {
    u64::try_from(value)
        .map_err(|_| PersistenceError::Corrupt(format!("negative {label} in durable state")))
}

/// Expected count of surviving `running` rows after startup normalization: the
/// number of Runs handed off live across an exec-in-place upgrade. Zero on the
/// crash-recovery path, where the live-set is empty and every `running` row is
/// reconciled. Typed to match the `SQLite` `count`-derived `i64` guards.
fn live_count(live_set: &HashSet<RunId>) -> i64 {
    i64::try_from(live_set.len()).expect("handoff live-set fits SQLite")
}

fn now_millis() -> i64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    i64::try_from(millis).unwrap_or(i64::MAX)
}

fn file_len(path: &Path) -> Result<u64, PersistenceError> {
    match fs::metadata(path) {
        Ok(metadata) => Ok(metadata.len()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(0),
        Err(source) => Err(PersistenceError::io(path, source)),
    }
}

fn validate_physical_limits(
    state_dir: &Path,
    database_path: &Path,
    wal_path: &Path,
    shm_path: &Path,
) -> Result<(), PersistenceError> {
    let database = file_len(database_path)?;
    let wal = file_len(wal_path)?;
    let shm = file_len(shm_path)?;
    if database > DATABASE_MAX_BYTES {
        return Err(PersistenceError::Corrupt(
            "main database exceeds 384 MiB".to_owned(),
        ));
    }
    if wal > WAL_MAX_BYTES {
        return Err(PersistenceError::Corrupt("WAL exceeds 16 MiB".to_owned()));
    }
    if shm > SHM_MAX_BYTES {
        return Err(PersistenceError::Corrupt(
            "shared-memory sidecar exceeds 4 MiB".to_owned(),
        ));
    }
    if database.saturating_add(wal).saturating_add(shm) > STATE_FILES_MAX_BYTES {
        return Err(PersistenceError::Corrupt(format!(
            "state files in {} exceed 404 MiB",
            state_dir.display()
        )));
    }
    Ok(())
}

fn mutex_lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, HashSet},
        env,
        fs::{self, OpenOptions},
        os::unix::{fs::MetadataExt, process::ExitStatusExt},
        path::{Path, PathBuf},
        process::{
            Child as ProcessChild, Command as ProcessCommand, Output as ProcessOutput, Stdio,
        },
        sync::{Arc, atomic::Ordering},
        thread,
        time::{Duration, Instant},
    };

    use ctxmux_protocol::{
        CreateOperationKey, InterruptionReason, OutputChunk, OutputReplay, RunBackend,
        RunCapabilities, RunId, RunInfo, RunSpec, RunState, TerminalSize,
    };
    use rusqlite::{Connection, params};
    use tempfile::TempDir;

    use super::{
        AdmissionLimits, CommitProbe, DATABASE_FILE, DATABASE_MAX_BYTES, GLOBAL_REPLAY_BYTES,
        MAX_TRANSACTION_PAYLOAD_BYTES, METADATA_BYTES, PAGE_SIZE_BYTES, PER_RUN_REPLAY_BYTES,
        PERSISTENCE_QUEUE_CAPACITY, Persistence, PersistenceError, PersistenceTestHooks,
        PersistentCandidate, PersistentStartCompletion, RUN_RECORDS, SHM_MAX_BYTES,
        STATE_FILES_MAX_BYTES, StartCommitCrashPhase, StartDisposition, StartReceipt,
        StateLockGuard, StateStore, WAL_CHECKPOINT_BYTES, WAL_MAX_BYTES, append_replay,
        create_schema, metadata_size, mutex_lock, prune_global_replay_to, validate_existing_schema,
        wal_charge_for_cache,
    };
    use crate::creation::MAX_RETAINED_RUNS;

    const COMMIT_CRASH_STATE_DIR: &str = "CTXMUX_COMMIT_CRASH_STATE_DIR";
    const COMMIT_CRASH_PHASE: &str = "CTXMUX_COMMIT_CRASH_PHASE";
    const COMMIT_CRASH_NEW_ID: &str = "CTXMUX_COMMIT_CRASH_NEW_ID";
    const COMMIT_CRASH_NEW_KEY: &str = "CTXMUX_COMMIT_CRASH_NEW_KEY";
    const COMMIT_CRASH_ROLE: &str = "CTXMUX_COMMIT_CRASH_ROLE";
    const STARTUP_SOCKET_STATE_DIR: &str = "CTXMUX_STARTUP_SOCKET_STATE_DIR";
    const STARTUP_SOCKET_PATH: &str = "CTXMUX_STARTUP_SOCKET_PATH";
    const STARTUP_SOCKET_ROLE: &str = "CTXMUX_STARTUP_SOCKET_ROLE";

    #[test]
    fn state_lock_release_does_not_wait_for_an_inherited_file_description() {
        let temp = TempDir::new().expect("create state-lock inheritance fixture");
        let lock_path = temp.path().join("state.lock");
        let owner_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .expect("open owner lock file");
        let owner = StateLockGuard::acquire(owner_file, temp.path(), &lock_path)
            .expect("acquire owner state lock");
        let inherited_file = owner
            .0
            .try_clone()
            .expect("model a fork-inherited file description");
        let live_contender_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&lock_path)
            .expect("open live contender lock file");
        let Err(live_error) = StateLockGuard::acquire(live_contender_file, temp.path(), &lock_path)
        else {
            panic!("a second live owner acquired the state lock");
        };
        assert!(matches!(live_error, PersistenceError::StateInUse(_)));

        drop(owner);

        let contender_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&lock_path)
            .expect("open contender lock file");
        let contender = StateLockGuard::acquire(contender_file, temp.path(), &lock_path)
            .expect("explicit owner release is not extended by an inherited descriptor");
        drop(inherited_file);
        drop(contender);
    }

    #[test]
    fn format_limits_match_the_accepted_physical_and_logical_budgets() {
        assert_eq!(PAGE_SIZE_BYTES, 4 * 1024);
        assert_eq!(PER_RUN_REPLAY_BYTES, 4 * 1024 * 1024);
        assert_eq!(GLOBAL_REPLAY_BYTES, 256 * 1024 * 1024);
        assert_eq!(METADATA_BYTES, 64 * 1024 * 1024);
        assert_eq!(RUN_RECORDS, 4_096);
        assert_eq!(PERSISTENCE_QUEUE_CAPACITY, 1_024);
        assert_eq!(DATABASE_MAX_BYTES, 384 * 1024 * 1024);
        assert_eq!(WAL_MAX_BYTES, 16 * 1024 * 1024);
        assert_eq!(SHM_MAX_BYTES, 4 * 1024 * 1024);
        assert_eq!(
            DATABASE_MAX_BYTES + WAL_MAX_BYTES + SHM_MAX_BYTES,
            STATE_FILES_MAX_BYTES
        );
        let worst_admitted_output =
            u64::try_from(MAX_TRANSACTION_PAYLOAD_BYTES).expect("payload limit fits u64") * 4
                + 1024 * 1024;
        assert!(worst_admitted_output <= WAL_CHECKPOINT_BYTES);
    }

    #[test]
    fn staged_start_receipt_resolves_once_and_never_reopens() {
        let receipt = StartReceipt::pending();
        assert_eq!(receipt.disposition(), StartDisposition::Pending);
        assert!(receipt.decide(StartDisposition::Committed));
        assert!(!receipt.decide(StartDisposition::NotCommitted));
        assert!(!receipt.decide(StartDisposition::CommitUnknown));
        assert_eq!(receipt.disposition(), StartDisposition::Committed);

        let lost = StartReceipt::pending();
        assert_eq!(lost.unknown_if_pending(), StartDisposition::CommitUnknown);
        assert!(!lost.decide(StartDisposition::NotCommitted));
    }

    #[test]
    fn ordinary_exact_replacement_recovers_old_or_new_around_real_commit_crash() {
        for (phase, expected_new) in [("before", false), ("after", true)] {
            let temp = TempDir::new().expect("create COMMIT crash fixture");
            let state_dir = temp.path().join(phase);
            let (old_id, old_key) = seed_terminal_candidate(&state_dir, phase);
            let new_id = RunId::new();
            let new_key = CreateOperationKey::new(format!("commit-crash-new-{phase}"))
                .expect("valid COMMIT crash key");
            let output = run_commit_crash_subprocess(&state_dir, phase, new_id, &new_key);
            assert_eq!(
                output.status.code(),
                None,
                "{phase}-COMMIT helper exited normally: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert_eq!(
                output.status.signal(),
                Some(rustix::process::Signal::ABORT.as_raw()),
                "{phase}-COMMIT helper did not terminate with SIGABRT: {}",
                String::from_utf8_lossy(&output.stderr)
            );

            let raw = raw_run_units(&state_dir);
            assert_eq!(raw.len(), 1, "raw crash recovery exposed a hybrid unit");
            let expected_raw = if expected_new {
                (new_id.to_string(), new_key.as_str(), "running", None)
            } else {
                (old_id.to_string(), old_key.as_str(), "exited", Some(42))
            };
            assert_eq!(
                (
                    raw[0].0.clone(),
                    raw[0].1.as_str(),
                    raw[0].2.as_str(),
                    raw[0].3
                ),
                expected_raw,
                "{phase}-COMMIT raw SQLite recovery chose the wrong durable unit"
            );
            let (persistence, recovered) =
                Persistence::open_with_test_limits(state_dir, 1, METADATA_BYTES)
                    .expect("SQLite recovery resolves the crashed exact replacement");
            assert_eq!(recovered.len(), 1, "crash recovery exposed a hybrid unit");
            let expected = if expected_new {
                (new_id, &new_key)
            } else {
                (old_id, &old_key)
            };
            assert_eq!(
                (recovered[0].info.id, &recovered[0].operation_key),
                expected,
                "{phase}-COMMIT recovery chose the wrong durable unit"
            );
            if expected_new {
                assert_eq!(
                    recovered[0].info.state,
                    RunState::Interrupted {
                        reason: InterruptionReason::DaemonRestart
                    }
                );
            } else {
                assert_eq!(recovered[0].info.state, exited_state());
            }
            persistence.assert_exclusive_owner();
        }
    }

    #[test]
    fn ordinary_commit_crash_subprocess() {
        let Ok(role) = env::var(COMMIT_CRASH_ROLE) else {
            return;
        };
        let state_dir = env::var_os(COMMIT_CRASH_STATE_DIR)
            .expect("COMMIT crash helper receives state directory");
        let phase = match env::var(COMMIT_CRASH_PHASE).as_deref() {
            Ok("before") => StartCommitCrashPhase::Before,
            Ok("after") => StartCommitCrashPhase::After,
            value => panic!("invalid COMMIT crash phase: {value:?}"),
        };
        let new_id = env::var(COMMIT_CRASH_NEW_ID)
            .expect("COMMIT crash helper receives new Run id")
            .parse()
            .expect("COMMIT crash Run id is valid");
        let expected_role = env::var(COMMIT_CRASH_NEW_ID).unwrap();
        assert_eq!(
            role, expected_role,
            "COMMIT crash helper requires its exact per-process role token"
        );
        let new_key = CreateOperationKey::new(
            env::var(COMMIT_CRASH_NEW_KEY).expect("COMMIT crash helper receives new key"),
        )
        .expect("COMMIT crash key is valid");
        let (persistence, recovered) =
            Persistence::open_with_test_limits(state_dir.into(), 1, METADATA_BYTES)
                .expect("open COMMIT crash helper persistence");
        assert_eq!(recovered.len(), 1);
        let old = &recovered[0];
        let prepared = persistence
            .prepare_start(&new_key, &running_info(new_id))
            .expect("prepare replacement for COMMIT crash");
        let staged = persistence
            .stage_start(
                prepared,
                vec![PersistentCandidate::new(
                    old.info.id,
                    old.operation_key.clone(),
                    old.metadata_bytes,
                )],
            )
            .expect("stage replacement before COMMIT crash");
        persistence.crash_next_start_commit_at(phase);
        let _ = staged.commit();
        panic!("COMMIT crash hook did not terminate the helper process");
    }

    #[test]
    fn failed_commit_actor_route_distinguishes_old_new_and_hybrid_units() {
        for expected in [
            CommitProbe::OldUnit,
            CommitProbe::NewUnit,
            CommitProbe::Hybrid,
        ] {
            let temp = TempDir::new().expect("create failed COMMIT actor fixture");
            let state_dir = temp.path().join("state");
            let (old_id, old_key) = seed_terminal_candidate(&state_dir, "classifier");
            let (persistence, recovered) =
                Persistence::open_with_test_limits(state_dir.clone(), 1, METADATA_BYTES)
                    .expect("open failed COMMIT actor persistence");
            assert_eq!(recovered.len(), 1);
            let new_id = RunId::new();
            let new_key = CreateOperationKey::new(format!("failed-commit-{expected:?}"))
                .expect("valid failed COMMIT key");
            let prepared = persistence
                .prepare_start(&new_key, &running_info(new_id))
                .expect("prepare failed COMMIT replacement");
            let staged = persistence
                .stage_start(
                    prepared,
                    vec![PersistentCandidate::new(
                        recovered[0].info.id,
                        recovered[0].operation_key.clone(),
                        recovered[0].metadata_bytes,
                    )],
                )
                .expect("stage failed COMMIT replacement");
            persistence.fail_next_start_commit_as(expected);
            let result = staged.commit();
            match (expected, result) {
                (CommitProbe::OldUnit, PersistentStartCompletion::NotCommitted(failure)) => {
                    assert_eq!(failure.disposition(), StartDisposition::NotCommitted);
                    assert!(!persistence.is_failed());
                }
                (CommitProbe::NewUnit, PersistentStartCompletion::Committed(committed)) => {
                    assert!(committed.post_commit_error.is_some());
                    assert!(persistence.is_failed());
                }
                (CommitProbe::Hybrid, PersistentStartCompletion::CommitUnknown(failure)) => {
                    assert!(failure.to_string().contains("durable rows are hybrid"));
                    assert_eq!(failure.disposition(), StartDisposition::CommitUnknown);
                    assert!(persistence.is_failed());
                }
                (_, _) => panic!("failed COMMIT actor returned the wrong disposition"),
            }
            persistence.assert_exclusive_owner();
            drop(persistence);
            let raw = raw_run_units(&state_dir);
            let expected_ids = match expected {
                CommitProbe::OldUnit => vec![old_id.to_string()],
                CommitProbe::NewUnit => vec![new_id.to_string()],
                CommitProbe::Hybrid => {
                    let mut ids = vec![old_id.to_string(), new_id.to_string()];
                    ids.sort();
                    ids
                }
            };
            assert_eq!(
                raw.iter().map(|row| row.0.clone()).collect::<Vec<_>>(),
                expected_ids
            );
            assert_eq!(
                raw.iter()
                    .map(|row| row.1.as_str())
                    .collect::<Vec<_>>()
                    .contains(&old_key.as_str()),
                !matches!(expected, CommitProbe::NewUnit)
            );
            assert_eq!(
                raw.iter()
                    .map(|row| row.1.as_str())
                    .collect::<Vec<_>>()
                    .contains(&new_key.as_str()),
                !matches!(expected, CommitProbe::OldUnit)
            );
        }
    }

    #[test]
    fn cache_charge_formula_has_an_exact_eight_mib_boundary() {
        let admitted_frames = (WAL_CHECKPOINT_BYTES - 32) / (PAGE_SIZE_BYTES + 24);
        let admitted_cache = admitted_frames * PAGE_SIZE_BYTES;
        assert!(wal_charge_for_cache(admitted_cache).unwrap() <= WAL_CHECKPOINT_BYTES);
        assert!(
            wal_charge_for_cache(admitted_cache + 1).unwrap() > WAL_CHECKPOINT_BYTES,
            "one byte into another conservative page crosses the frozen charge"
        );
    }

    #[test]
    fn schema_bootstrap_uses_a_reopenable_epoch_before_normalization() {
        let connection = Connection::open_in_memory().expect("open bootstrap fixture");
        let epoch = uuid::Uuid::new_v4().to_string();
        create_schema(&connection, &epoch).expect("create schema with a valid bootstrap epoch");
        validate_existing_schema(&connection).expect("bootstrap schema is immediately reopenable");
        let stored: String = connection
            .query_row(
                "SELECT current_epoch FROM runtime_meta WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .expect("read bootstrap epoch");
        assert_eq!(stored, epoch);
    }

    #[test]
    fn startup_normalization_is_bounded_restartable_and_canonical() {
        let temp = TempDir::new().expect("create startup normalization fixture");
        let state_dir = temp.path().join("state");
        let seeded = seed_startup_overflow(&state_dir);

        for expected_phase in ["reconcile", "evict"] {
            let hooks = Arc::new(PersistenceTestHooks::default());
            hooks.startup_fail_after_commits.store(1, Ordering::Release);
            if expected_phase == "evict" {
                hooks
                    .force_startup_over_budget_once
                    .store(true, Ordering::Release);
            }
            let Err(error) = StateStore::open(
                &state_dir,
                AdmissionLimits::OPERATIONAL,
                None,
                Arc::clone(&hooks),
            ) else {
                panic!("injected startup {expected_phase} interruption unexpectedly opened");
            };
            assert!(error.to_string().contains("injected interruption"));
            let wal = mutex_lock(&hooks.startup_batch_wal_bytes).clone();
            assert_eq!(wal.len(), 1);
            assert!(wal[0] <= WAL_CHECKPOINT_BYTES);
            if expected_phase == "evict" {
                assert!(
                    hooks.startup_over_budget_attempts.load(Ordering::Acquire) > 0,
                    "page-heavy eviction shrinks before committing one exact prefix"
                );
            }
        }

        let (persistence, recovered) =
            Persistence::open(&state_dir).expect("restart completes startup normalization");
        assert_eq!(recovered.len(), MAX_RETAINED_RUNS);
        let expected = &seeded[seeded.len() - MAX_RETAINED_RUNS..];
        assert_eq!(
            recovered
                .iter()
                .map(|run| (run.info.id, run.operation_key.clone()))
                .collect::<Vec<_>>(),
            expected.to_vec()
        );
        let interrupted = recovered.last().expect("retain newest prior running Run");
        assert_eq!(
            interrupted.info.state,
            RunState::Interrupted {
                reason: InterruptionReason::DaemonRestart
            }
        );
        assert_eq!(interrupted.info.pid, None);
        let wal = persistence.startup_batch_wal_bytes();
        assert!(!wal.is_empty());
        assert!(wal.iter().all(|bytes| *bytes <= WAL_CHECKPOINT_BYTES));
    }

    #[test]
    fn startup_normalization_failure_precedes_public_socket_publication() {
        let temp = TempDir::new().expect("create public startup failure fixture");
        let state_dir = temp.path().join("state");
        let seeded = seed_startup_overflow(&state_dir);
        let role = uuid::Uuid::new_v4().to_string();
        let socket = temp.path().join(format!("{role}.sock"));
        let sentinel = b"ctxmux startup precedence sentinel";
        fs::write(&socket, sentinel).expect("write socket precedence sentinel");
        let identity = fs::metadata(&socket)
            .map(|metadata| (metadata.dev(), metadata.ino()))
            .expect("read socket precedence identity");
        let output = run_startup_socket_subprocess(&state_dir, &socket, &role);
        assert!(
            output.status.success(),
            "startup socket helper failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            fs::read(&socket).expect("read preserved socket sentinel"),
            sentinel
        );
        assert_eq!(
            fs::metadata(&socket)
                .map(|metadata| (metadata.dev(), metadata.ino()))
                .expect("read preserved socket identity"),
            identity
        );
        let connection = Connection::open(state_dir.join(DATABASE_FILE))
            .expect("inspect committed startup side effect");
        let (records, running, interrupted): (i64, i64, i64) = connection
            .query_row(
                "SELECT count(*), coalesce(sum(state_kind = 'running'), 0),
                        coalesce(sum(state_kind = 'interrupted'), 0) FROM runs",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("read committed startup side effect");
        assert_eq!((records, running, interrupted), (131, 0, 1));
        drop(connection);

        let (persistence, recovered) =
            Persistence::open(&state_dir).expect("restart resumes startup normalization");
        assert_eq!(recovered.len(), MAX_RETAINED_RUNS);
        let expected = &seeded[seeded.len() - MAX_RETAINED_RUNS..];
        assert_eq!(
            recovered
                .iter()
                .map(|run| (run.info.id, run.operation_key.clone()))
                .collect::<Vec<_>>(),
            expected.to_vec()
        );
        assert_eq!(
            recovered
                .last()
                .expect("retain reconciled prior Run")
                .info
                .state,
            RunState::Interrupted {
                reason: InterruptionReason::DaemonRestart
            }
        );
        assert_eq!(recovered.last().unwrap().info.pid, None);
        persistence.assert_exclusive_owner();
    }

    #[test]
    fn startup_socket_subprocess() {
        let Ok(role) = env::var(STARTUP_SOCKET_ROLE) else {
            return;
        };
        let state_dir = PathBuf::from(
            env::var_os(STARTUP_SOCKET_STATE_DIR)
                .expect("startup socket helper receives state directory"),
        );
        let socket = PathBuf::from(
            env::var_os(STARTUP_SOCKET_PATH).expect("startup socket helper receives socket path"),
        );
        assert_eq!(
            socket.file_stem().and_then(std::ffi::OsStr::to_str),
            Some(role.as_str()),
            "startup socket helper requires its exact per-process role token"
        );
        let sentinel = fs::read(&socket).expect("startup socket helper reads sentinel");
        Persistence::fail_next_open_after_startup_commit();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build startup socket helper runtime");
        let error = runtime
            .block_on(crate::serve_with_state_dir(socket.clone(), state_dir))
            .expect_err("injected startup failure unexpectedly served a socket");
        assert!(error.to_string().contains("injected interruption"));
        assert_eq!(fs::read(&socket).unwrap(), sentinel);
    }

    #[test]
    fn creation_key_index_is_unique_binary_and_exactly_validated() {
        let connection = test_connection();
        validate_existing_schema(&connection).expect("accept canonical schema 4 index");

        connection
            .execute_batch(
                "DROP INDEX runs_creation_key;
                 CREATE UNIQUE INDEX runs_creation_key
                 ON runs(creation_key COLLATE NOCASE);",
            )
            .expect("replace creation index with wrong collation");
        let error = validate_existing_schema(&connection)
            .expect_err("NOCASE creation identity must fail exact schema validation");
        assert!(matches!(error, PersistenceError::Corrupt(_)));
        assert!(error.to_string().contains("byte-exactly"));
    }

    #[test]
    fn binary_creation_keys_keep_case_distinct_and_store_conflicts_are_fatal() {
        let temp = TempDir::new().expect("create byte-exact key fixture");
        let state_dir = temp.path().join("state");
        let (persistence, recovered) = Persistence::open(&state_dir).expect("open persistence");
        assert!(recovered.is_empty());

        let upper = running_info(RunId::new());
        let lower = running_info(RunId::new());
        persistence
            .insert_start(&CreateOperationKey::new("Case").unwrap(), &upper)
            .expect("insert uppercase opaque key");
        persistence
            .insert_start(&CreateOperationKey::new("case").unwrap(), &lower)
            .expect("insert lowercase opaque key");

        let conflicting = running_info(RunId::new());
        let Err(error) =
            persistence.insert_start(&CreateOperationKey::new("Case").unwrap(), &conflicting)
        else {
            panic!("store-level duplicate must be a fatal owner invariant breach");
        };
        assert!(matches!(error, PersistenceError::Mutation(_)));
        let later = running_info(RunId::new());
        let Err(latched) =
            persistence.insert_start(&CreateOperationKey::new("later").unwrap(), &later)
        else {
            panic!("fatal store conflict must latch the actor");
        };
        assert!(matches!(latched, PersistenceError::Mutation(_)));

        drop(persistence);
        let (reopened, recovered) = Persistence::open(state_dir).expect("reopen prior unit");
        assert_eq!(recovered.len(), 2);
        drop(reopened);
    }

    #[test]
    fn global_replay_pruning_evicts_oldest_chunks_and_preserves_each_tail() {
        let mut connection = test_connection();
        let first = RunId::new();
        let second = RunId::new();
        let transaction = connection.transaction().expect("start replay transaction");
        insert_test_run(&transaction, first, "running", 1);
        insert_test_run(&transaction, second, "running", 1);
        append_replay(
            &transaction,
            first,
            &replay(vec![chunk(0, b"aaa"), chunk(3, b"bbb")]),
        )
        .expect("append first replay");
        append_replay(
            &transaction,
            second,
            &replay(vec![chunk(0, b"ccc"), chunk(3, b"ddd")]),
        )
        .expect("append second replay");
        assert!(prune_global_replay_to(&transaction, 7).expect("prune global replay"));
        for id in [first, second] {
            let (oldest, head, bytes, truncated): (i64, i64, i64, i64) = transaction
                .query_row(
                    "SELECT durable_first_available_byte, durable_output_bytes, replay_bytes,
                            replay_truncated FROM runs WHERE id = ?1",
                    [id.to_string()],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .expect("read pruned replay accounting");
            assert_eq!((oldest, head, bytes, truncated), (3, 6, 3, 1));
        }
        transaction.commit().expect("commit replay pruning");
    }

    #[test]
    fn append_batch_commits_one_collected_payload_unit() {
        let temp = TempDir::new().expect("create append-batch fixture");
        let state_dir = temp.path().join("state");
        let hooks = Arc::new(PersistenceTestHooks::default());
        let (mut store, recovered) = StateStore::open(
            &state_dir,
            AdmissionLimits::OPERATIONAL,
            None,
            Arc::clone(&hooks),
        )
        .expect("open append-batch store");
        assert!(recovered.is_empty());
        let first = RunId::new();
        let second = RunId::new();
        let transaction = store
            .connection
            .transaction()
            .expect("start append-batch fixture transaction");
        insert_test_run(&transaction, first, "running", 1);
        insert_test_run(&transaction, second, "running", 1);
        transaction
            .commit()
            .expect("commit append-batch fixture Runs");

        let first_head = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let second_head = Arc::new(std::sync::atomic::AtomicU64::new(0));
        store
            .append_batch(&[
                (
                    first,
                    replay(vec![chunk(0, b"aaa")]),
                    Arc::clone(&first_head),
                ),
                (
                    first,
                    replay(vec![chunk(3, b"bbb")]),
                    Arc::clone(&first_head),
                ),
                (
                    second,
                    replay(vec![chunk(0, b"ccc")]),
                    Arc::clone(&second_head),
                ),
            ])
            .expect("commit one collected append batch");

        assert_eq!(
            hooks.append_transaction_commits.load(Ordering::Acquire),
            1,
            "one actor-collected payload unit must not expand into per-command COMMITs"
        );
        assert_eq!(first_head.load(Ordering::Acquire), 6);
        assert_eq!(second_head.load(Ordering::Acquire), 3);
        for (id, expected) in [(first, (0_i64, 6_i64, 6_i64)), (second, (0, 3, 3))] {
            let actual = store
                .connection
                .query_row(
                    "SELECT durable_first_available_byte, durable_output_bytes, replay_bytes FROM runs WHERE id = ?1",
                    [id.to_string()],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .expect("read append-batch durable tuple");
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn rejected_start_admission_does_not_poison_the_actor() {
        let temp = TempDir::new().expect("create persistence admission fixture");
        let state_dir = temp.path().join("state");
        let limits = AdmissionLimits {
            run_records: 1,
            metadata_bytes: METADATA_BYTES,
        };
        let (persistence, recovered) =
            Persistence::open_with_admission_limits(state_dir.clone(), limits, None)
                .expect("open small-capacity persistence actor");
        assert!(recovered.is_empty());

        let first = running_info(RunId::new());
        let first_key = test_operation_key(first.id);
        let first_durable = persistence
            .insert_start(&first_key, &first)
            .expect("insert first running record");
        let second = running_info(RunId::new());
        let second_key = test_operation_key(second.id);
        let prepared = persistence
            .prepare_start(&second_key, &second)
            .expect("prepare second start");
        let Err(rejection) = persistence.stage_start(prepared, Vec::new()) else {
            panic!("running-only capacity admitted a second record");
        };
        assert_eq!(rejection.disposition(), StartDisposition::NotCommitted);
        assert!(rejection.is_capacity());

        let first_replay = replay(vec![chunk(0, b"first")]);
        first_durable.append(first.id, first_replay.clone());
        first_durable.finalize(first.id, 42, first_replay, exited_state());
        assert_eq!(first_durable.durable_head(), 5);

        let prepared = persistence
            .prepare_start(&second_key, &second)
            .expect("prepare exact replacement");
        let staged = persistence
            .stage_start(
                prepared,
                vec![PersistentCandidate::new(
                    first.id,
                    first_key,
                    first_durable
                        .metadata_bytes_owner()
                        .load(std::sync::atomic::Ordering::Acquire),
                )],
            )
            .expect("exact terminal candidate funds replacement");
        let PersistentStartCompletion::Committed(second_durable) = staged.commit() else {
            panic!("exact replacement did not commit");
        };
        second_durable.finalize(second.id, 42, replay(Vec::new()), exited_state());
        let second_metadata = second_durable
            .metadata_bytes_owner()
            .load(std::sync::atomic::Ordering::Acquire);
        drop(second_durable);
        drop(first_durable);
        persistence.assert_exclusive_owner();
        drop(persistence);

        let (reopened, recovered) = Persistence::open(state_dir).expect("reopen admitted state");
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].info.id, second.id);
        assert_eq!(recovered[0].info.state, exited_state());
        assert_eq!(recovered[0].info.pid, Some(42));
        assert_eq!(recovered[0].metadata_bytes, second_metadata);
        drop(reopened);
    }

    #[test]
    fn wrong_exact_candidate_snapshot_rolls_back_without_deleting_history() {
        let temp = TempDir::new().expect("create exact-candidate fixture");
        let state_dir = temp.path().join("state");
        let limits = AdmissionLimits {
            run_records: 1,
            metadata_bytes: METADATA_BYTES,
        };
        let (persistence, recovered) =
            Persistence::open_with_admission_limits(state_dir.clone(), limits, None)
                .expect("open exact-candidate store");
        assert!(recovered.is_empty());

        let first = running_info(RunId::new());
        let first_key = test_operation_key(first.id);
        let first_durable = persistence
            .insert_start(&first_key, &first)
            .expect("insert candidate");
        let first_replay = replay(vec![chunk(0, b"retained")]);
        first_durable.append(first.id, first_replay.clone());
        first_durable.finalize(first.id, 77, first_replay, exited_state());

        let replacement = running_info(RunId::new());
        let replacement_key = test_operation_key(replacement.id);
        let prepared = persistence
            .prepare_start(&replacement_key, &replacement)
            .expect("prepare replacement");
        let wrong_metadata = first_durable
            .metadata_bytes_owner()
            .load(std::sync::atomic::Ordering::Acquire)
            .checked_add(1)
            .expect("fixture metadata does not overflow");
        let Err(failure) = persistence.stage_start(
            prepared,
            vec![PersistentCandidate::new(
                first.id,
                first_key,
                wrong_metadata,
            )],
        ) else {
            panic!("wrong candidate snapshot must fail closed");
        };
        assert_eq!(failure.disposition(), StartDisposition::NotCommitted);
        assert!(!failure.is_capacity());
        assert!(persistence.is_failed());

        drop(first_durable);
        drop(persistence);
        let (reopened, recovered) = Persistence::open(state_dir).expect("reopen rolled-back store");
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].info.id, first.id);
        assert_eq!(recovered[0].info.pid, Some(77));
        assert_eq!(recovered[0].replay.chunks, vec![chunk(0, b"retained")]);
        drop(reopened);
    }

    #[test]
    fn conflicting_replay_bytes_latch_the_actor_and_freeze_the_cursor() {
        let temp = TempDir::new().expect("create persistence fatal fixture");
        let state_dir = temp.path().join("state");
        let (persistence, recovered) = Persistence::open(&state_dir).expect("open persistence");
        assert!(recovered.is_empty());

        let first = running_info(RunId::new());
        let durable = persistence
            .insert_start(&test_operation_key(first.id), &first)
            .expect("insert fatal fixture record");
        durable.append(first.id, replay(vec![chunk(0, b"committed")]));
        durable.append(first.id, replay(vec![chunk(0, b"conflict")]));

        let later = running_info(RunId::new());
        let Err(error) = persistence.insert_start(&test_operation_key(later.id), &later) else {
            panic!("fatal replay conflict admitted a later mutation");
        };
        assert!(matches!(error, PersistenceError::Mutation(_)));
        assert!(error.to_string().contains("changed bytes"));
        assert_eq!(durable.durable_head(), b"committed".len() as u64);
        drop(durable);
        drop(persistence);

        let (reopened, recovered) =
            Persistence::open(state_dir).expect("reopen prior durable unit");
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].info.id, first.id);
        assert_eq!(recovered[0].replay.chunks, vec![chunk(0, b"committed")]);
        drop(reopened);
    }

    #[test]
    fn persistent_insert_rejects_non_native_or_invalid_start_metadata_before_writing() {
        let temp = TempDir::new().expect("create persistent insert invariant fixture");
        let base = running_info(RunId::new());

        let mut tmux_backend = base.clone();
        tmux_backend.backend = RunBackend::Tmux {
            socket_path: "/tmp/tmux.sock".to_owned(),
            server_pid: 1,
            server_started_at: 1,
            session_id: "$1".to_owned(),
            window_id: "@1".to_owned(),
            pane_id: "%1".to_owned(),
            tmux_version: "3.6b".to_owned(),
        };

        let mut tmux_capabilities = base.clone();
        tmux_capabilities.capabilities = RunCapabilities::TMUX_READ_ONLY;

        let mut missing_spec = base.clone();
        missing_spec.spec = None;

        let mut invalid_spec = base.clone();
        invalid_spec
            .spec
            .as_mut()
            .expect("fixture has a spec")
            .program
            .clear();

        let mut terminal = base;
        terminal.state = exited_state();

        for (label, info, expected) in [
            ("tmux-backend", tmux_backend, "native backend"),
            (
                "tmux-capabilities",
                tmux_capabilities,
                "invalid capabilities",
            ),
            ("missing-spec", missing_spec, "launch specification"),
            (
                "invalid-spec",
                invalid_spec,
                "Run program must not be empty",
            ),
            ("terminal-state", terminal, "must be running"),
        ] {
            let state_dir = temp.path().join(label);
            let (persistence, recovered) =
                Persistence::open(&state_dir).expect("open insert invariant actor");
            assert!(recovered.is_empty());
            let Err(error) = persistence.insert_start(&test_operation_key(info.id), &info) else {
                panic!("invalid persistent insert {label} succeeded");
            };
            assert!(error.to_string().contains(expected));
            persistence.assert_exclusive_owner();
            drop(persistence);

            let (reopened, recovered) =
                Persistence::open(state_dir).expect("reopen rejected insert store");
            assert!(recovered.is_empty(), "{label} left a partial durable row");
            drop(reopened);
        }
    }

    fn seed_terminal_candidate(state_dir: &Path, label: &str) -> (RunId, CreateOperationKey) {
        let (persistence, recovered) =
            Persistence::open_with_test_limits(state_dir.to_path_buf(), 1, METADATA_BYTES)
                .expect("open terminal candidate fixture");
        assert!(recovered.is_empty());
        let id = RunId::new();
        let key = CreateOperationKey::new(format!("commit-crash-old-{label}"))
            .expect("valid terminal candidate key");
        let durable = persistence
            .insert_start(&key, &running_info(id))
            .expect("insert terminal candidate");
        durable.finalize(id, 42, replay(Vec::new()), exited_state());
        drop(durable);
        persistence.assert_exclusive_owner();
        drop(persistence);
        (id, key)
    }

    fn run_commit_crash_subprocess(
        state_dir: &Path,
        phase: &str,
        new_id: RunId,
        new_key: &CreateOperationKey,
    ) -> ProcessOutput {
        let role = new_id.to_string();
        let child = ProcessCommand::new(env::current_exe().expect("resolve unit test binary"))
            .arg("--exact")
            .arg("persistence::tests::ordinary_commit_crash_subprocess")
            .arg("--nocapture")
            .env(COMMIT_CRASH_STATE_DIR, state_dir)
            .env(COMMIT_CRASH_PHASE, phase)
            .env(COMMIT_CRASH_NEW_ID, &role)
            .env(COMMIT_CRASH_NEW_KEY, new_key.as_str())
            .env(COMMIT_CRASH_ROLE, &role)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn isolated COMMIT crash fixture");
        wait_for_test_subprocess(child, &format!("{phase}-COMMIT"))
    }

    fn run_startup_socket_subprocess(state_dir: &Path, socket: &Path, role: &str) -> ProcessOutput {
        let child = ProcessCommand::new(env::current_exe().expect("resolve unit test binary"))
            .arg("--exact")
            .arg("persistence::tests::startup_socket_subprocess")
            .arg("--nocapture")
            .env(STARTUP_SOCKET_STATE_DIR, state_dir)
            .env(STARTUP_SOCKET_PATH, socket)
            .env(STARTUP_SOCKET_ROLE, role)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn isolated startup socket fixture");
        wait_for_test_subprocess(child, "startup-socket")
    }

    fn wait_for_test_subprocess(mut child: ProcessChild, label: &str) -> ProcessOutput {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if child
                .try_wait()
                .unwrap_or_else(|error| panic!("poll {label} fixture: {error}"))
                .is_some()
            {
                return child
                    .wait_with_output()
                    .unwrap_or_else(|error| panic!("collect {label} fixture output: {error}"));
            }
            if Instant::now() >= deadline {
                child
                    .kill()
                    .unwrap_or_else(|error| panic!("kill hung {label} fixture: {error}"));
                let output = child
                    .wait_with_output()
                    .unwrap_or_else(|error| panic!("reap hung {label} fixture: {error}"));
                panic!(
                    "{label} helper exceeded its 10 second budget: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn raw_run_units(state_dir: &Path) -> Vec<(String, String, String, Option<i64>)> {
        let connection = Connection::open(state_dir.join(DATABASE_FILE))
            .expect("open raw crash-recovered SQLite store");
        let quick_check: String = connection
            .query_row("PRAGMA quick_check", [], |row| row.get(0))
            .expect("run raw crash-recovery quick_check");
        assert_eq!(quick_check, "ok");
        let mut statement = connection
            .prepare("SELECT id, creation_key, state_kind, pid FROM runs ORDER BY id")
            .expect("prepare raw durable unit query");
        statement
            .query_map([], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })
            .expect("query raw durable units")
            .collect::<Result<Vec<_>, _>>()
            .expect("decode raw durable units")
    }

    fn seed_startup_overflow(state_dir: &Path) -> Vec<(RunId, CreateOperationKey)> {
        let (persistence, recovered) = Persistence::open_with_admission_limits(
            state_dir.to_path_buf(),
            AdmissionLimits::FORMAT,
            None,
        )
        .expect("open format-envelope persistence");
        assert!(recovered.is_empty());
        let count = MAX_RETAINED_RUNS + 3;
        let mut seeded = Vec::with_capacity(count);
        for index in 0..count {
            let id = RunId::new();
            let key = CreateOperationKey::new(format!("startup-{index:03}")).unwrap();
            let info = running_info(id);
            let durable = persistence
                .insert_start(&key, &info)
                .expect("insert startup normalization fixture");
            if index + 1 != count {
                let retained = if index < 3 {
                    replay(
                        (0..8)
                            .map(|index| OutputChunk {
                                start_byte: index * 512 * 1024,
                                end_byte: (index + 1) * 512 * 1024,
                                data: vec![b'x'; 512 * 1024],
                            })
                            .collect(),
                    )
                } else {
                    replay(Vec::new())
                };
                durable.append(id, retained.clone());
                durable.finalize(id, 42, retained, exited_state());
            }
            drop(durable);
            seeded.push((id, key));
        }
        persistence.assert_exclusive_owner();
        drop(persistence);

        let mut connection = Connection::open(state_dir.join(DATABASE_FILE))
            .expect("open startup fixture timestamps");
        let transaction = connection
            .transaction()
            .expect("start startup timestamp transaction");
        for (index, (id, _)) in seeded.iter().enumerate() {
            let timestamp = i64::try_from(index).expect("fixture index fits SQLite");
            transaction
                .execute(
                    "UPDATE runs SET created_at_ms = ?2, updated_at_ms = ?2,
                     terminal_at_ms = CASE WHEN state_kind = 'running' THEN NULL ELSE ?2 END
                     WHERE id = ?1",
                    params![id.to_string(), timestamp],
                )
                .expect("set deterministic startup fixture order");
        }
        transaction
            .commit()
            .expect("commit startup fixture timestamps");
        seeded
    }

    fn test_connection() -> Connection {
        let connection = Connection::open_in_memory().expect("open in-memory persistence store");
        connection
            .execute_batch("PRAGMA foreign_keys=ON;")
            .expect("enable test foreign keys");
        create_schema(&connection, &uuid::Uuid::new_v4().to_string())
            .expect("create test persistence schema");
        connection
            .execute(
                "UPDATE runtime_meta SET current_epoch = ?1 WHERE singleton = 1",
                [uuid::Uuid::new_v4().to_string()],
            )
            .expect("set test daemon epoch");
        connection
    }

    fn insert_test_run(
        transaction: &rusqlite::Transaction<'_>,
        id: RunId,
        state_kind: &str,
        metadata_bytes: i64,
    ) {
        let spec = RunSpec {
            program: "/bin/true".to_owned(),
            args: Vec::new(),
            cwd: None,
            env: BTreeMap::new(),
            size: TerminalSize::default(),
            declared_inputs: Vec::new(),
        };
        let spec_json = serde_json::to_string(&spec).expect("encode test spec");
        let state = if state_kind == "running" {
            RunState::Running
        } else {
            RunState::Exited {
                code: 0,
                signal: None,
            }
        };
        let state_json = serde_json::to_string(&state).expect("encode test state");
        let epoch = uuid::Uuid::new_v4().to_string();
        let operation_key = test_operation_key(id);
        let _actual_metadata = metadata_size(
            &id.to_string(),
            operation_key.as_str(),
            &spec_json,
            None,
            &state_json,
            &epoch,
        )
        .expect("measure test metadata");
        transaction
            .execute(
                "INSERT INTO runs (
                    id, creation_key, spec_json, lineage_json, state_kind, state_json, source_epoch, pid,
                    durable_first_available_byte, durable_output_bytes, replay_bytes, replay_truncated,
                    metadata_bytes, created_at_ms, updated_at_ms, terminal_at_ms
                 ) VALUES (?1, ?2, ?3, NULL, ?4, ?5, ?6, NULL, 0, 0, 0, 0, ?7, 1, 1, ?8)",
                params![
                    id.to_string(),
                    operation_key.as_str(),
                    spec_json,
                    state_kind,
                    state_json,
                    epoch,
                    metadata_bytes,
                    (state_kind != "running").then_some(1_i64),
                ],
            )
            .expect("insert test Run row");
    }

    fn running_info(id: RunId) -> RunInfo {
        RunInfo {
            id,
            spec: Some(RunSpec {
                program: "/bin/true".to_owned(),
                args: Vec::new(),
                cwd: None,
                env: BTreeMap::new(),
                size: TerminalSize::default(),
                declared_inputs: Vec::new(),
            }),
            lineage: None,
            backend: RunBackend::Native,
            capabilities: RunCapabilities::NATIVE,
            pid: Some(42),
            state: RunState::Running,
            latest_output_bytes: 0,
            durable_output_bytes: Some(0),
            first_available_byte: 0,
            attachments: 0,
            applied_input_bytes: Some(0),
        }
    }

    fn test_operation_key(id: RunId) -> CreateOperationKey {
        CreateOperationKey::new(format!("test-{id}")).expect("valid test operation key")
    }

    const fn exited_state() -> RunState {
        RunState::Exited {
            code: 0,
            signal: None,
        }
    }

    fn replay(chunks: Vec<OutputChunk>) -> OutputReplay {
        OutputReplay {
            first_available_byte: chunks.first().map_or(0, |chunk| chunk.start_byte),
            latest_output_bytes: chunks.last().map_or(0, |chunk| chunk.end_byte),
            chunks,
            truncated: false,
        }
    }

    fn chunk(start_byte: u64, data: &[u8]) -> OutputChunk {
        OutputChunk {
            start_byte,
            end_byte: start_byte + data.len() as u64,
            data: data.to_vec(),
        }
    }

    fn seed_two_running_rows(state_dir: &Path) -> (RunId, RunId, String) {
        let (persistence, recovered) =
            Persistence::open(state_dir).expect("open two-running handoff fixture");
        assert!(recovered.is_empty());
        let row_a = RunId::new();
        let row_b = RunId::new();
        for id in [row_a, row_b] {
            let info = running_info(id);
            let key = test_operation_key(id);
            persistence
                .insert_start(&key, &info)
                .expect("seed running handoff row");
        }
        let epoch = persistence.daemon_instance().to_string();
        persistence.assert_exclusive_owner();
        drop(persistence);

        // Stamp a live PID on row_a: an exec-in-place handoff keeps the running
        // Run's PID, and reconciling it would trip the interrupted-with-PID
        // corruption guard. Excluding it from reconciliation must retain both.
        let connection =
            Connection::open(state_dir.join(DATABASE_FILE)).expect("open handoff pid fixture");
        connection
            .execute(
                "UPDATE runs SET pid = 42 WHERE id = ?1 AND state_kind = 'running'",
                [row_a.to_string()],
            )
            .expect("stamp handed-off Run pid");
        drop(connection);
        (row_a, row_b, epoch)
    }

    fn row_state_kind(connection: &Connection, id: RunId) -> String {
        connection
            .query_row(
                "SELECT state_kind FROM runs WHERE id = ?1",
                [id.to_string()],
                |row| row.get(0),
            )
            .expect("read handoff row state kind")
    }

    fn published_epoch(connection: &Connection) -> String {
        connection
            .query_row(
                "SELECT current_epoch FROM runtime_meta WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .expect("read published handoff epoch")
    }

    #[test]
    fn runtime_identity_survives_cold_replacement_but_daemon_instance_changes() {
        let fixture = TempDir::new().expect("create Runtime identity fixture");
        let state_dir = fixture.path().join("state");
        let (first, recovered) = Persistence::open(&state_dir).expect("open first Runtime image");
        assert!(recovered.is_empty());
        let runtime_id = first.runtime_id();
        let first_instance = first.daemon_instance();
        first.assert_exclusive_owner();
        drop(first);

        let (replacement, recovered) =
            Persistence::open(&state_dir).expect("open cold replacement image");
        assert!(recovered.is_empty());
        assert_eq!(replacement.runtime_id(), runtime_id);
        assert_ne!(replacement.daemon_instance(), first_instance);
    }

    #[test]
    fn handoff_hint_excludes_live_runs_and_reuses_epoch() {
        // Exec-in-place path: the handed-off Run stays running and the epoch is reused.
        let handed = TempDir::new().expect("create handoff fixture");
        let handed_dir = handed.path().join("state");
        let (row_a, row_b, original_epoch) = seed_two_running_rows(&handed_dir);

        let hooks = Arc::new(PersistenceTestHooks::default());
        let (store, recovered) = StateStore::open(
            &handed_dir,
            AdmissionLimits::OPERATIONAL,
            Some(super::HandoffHint {
                epoch: original_epoch.clone(),
                live_set: HashSet::from([row_a]),
                state_lock_fd: None,
            }),
            Arc::clone(&hooks),
        )
        .expect("reopen with handoff hint");

        assert_eq!(row_state_kind(&store.connection, row_a), "running");
        assert_eq!(row_state_kind(&store.connection, row_b), "interrupted");
        assert_eq!(store.epoch, original_epoch);
        assert_eq!(published_epoch(&store.connection), original_epoch);

        let run_a = recovered
            .iter()
            .find(|run| run.info.id == row_a)
            .expect("handed-off Run recovered");
        assert_eq!(run_a.info.state, RunState::Running);
        assert_eq!(run_a.info.pid, Some(42));
        let run_b = recovered
            .iter()
            .find(|run| run.info.id == row_b)
            .expect("reconciled Run recovered");
        assert_eq!(
            run_b.info.state,
            RunState::Interrupted {
                reason: InterruptionReason::DaemonRestart
            }
        );
        assert_eq!(run_b.info.pid, None);
        drop(store);

        // Crash path (None): every running row is reconciled and a fresh epoch is minted.
        let crashed = TempDir::new().expect("create crash-path fixture");
        let crashed_dir = crashed.path().join("state");
        let (crash_a, crash_b, crash_epoch) = seed_two_running_rows(&crashed_dir);

        let crash_hooks = Arc::new(PersistenceTestHooks::default());
        let (crash_store, _) = StateStore::open(
            &crashed_dir,
            AdmissionLimits::OPERATIONAL,
            None,
            Arc::clone(&crash_hooks),
        )
        .expect("reopen crash path without a hint");

        assert_eq!(
            row_state_kind(&crash_store.connection, crash_a),
            "interrupted"
        );
        assert_eq!(
            row_state_kind(&crash_store.connection, crash_b),
            "interrupted"
        );
        assert_ne!(crash_store.epoch, crash_epoch);
        assert_eq!(published_epoch(&crash_store.connection), crash_store.epoch);
    }

    fn seed_startup_overflow_with_live_row(state_dir: &Path) -> (RunId, String) {
        // Over-budget DB whose sole live (handed-off) Run carries the OLDEST
        // updated_at_ms, so it sorts FIRST in the eviction candidate scan. This
        // is the exec-in-place upgrade shape A8 must keep openable: the earlier
        // A8 fixture used only two rows, so eviction never ran and the bug hid.
        let (persistence, recovered) = Persistence::open_with_admission_limits(
            state_dir.to_path_buf(),
            AdmissionLimits::FORMAT,
            None,
        )
        .expect("open format-envelope persistence");
        assert!(recovered.is_empty());
        let count = MAX_RETAINED_RUNS + 3;
        let mut seeded = Vec::with_capacity(count);
        let mut live_id = None;
        for index in 0..count {
            let id = RunId::new();
            let key = CreateOperationKey::new(format!("overflow-{index:03}")).unwrap();
            let durable = persistence
                .insert_start(&key, &running_info(id))
                .expect("insert overflow fixture row");
            if index == 0 {
                // Leave the earliest row un-finalized: it stays `running` and
                // becomes the live handed-off Run once we reopen with a hint.
                live_id = Some(id);
            } else {
                durable.append(id, replay(Vec::new()));
                durable.finalize(id, 42, replay(Vec::new()), exited_state());
            }
            drop(durable);
            seeded.push(id);
        }
        let epoch = persistence.daemon_instance().to_string();
        persistence.assert_exclusive_owner();
        drop(persistence);

        let mut connection = Connection::open(state_dir.join(DATABASE_FILE))
            .expect("open overflow fixture timestamps");
        let transaction = connection
            .transaction()
            .expect("start overflow timestamp transaction");
        for (index, id) in seeded.iter().enumerate() {
            let timestamp = i64::try_from(index).expect("fixture index fits SQLite");
            transaction
                .execute(
                    "UPDATE runs SET created_at_ms = ?2, updated_at_ms = ?2,
                     terminal_at_ms = CASE WHEN state_kind = 'running' THEN NULL ELSE ?2 END
                     WHERE id = ?1",
                    params![id.to_string(), timestamp],
                )
                .expect("set deterministic overflow order");
        }
        transaction
            .commit()
            .expect("commit overflow fixture timestamps");
        (live_id.expect("live row seeded"), epoch)
    }

    #[test]
    fn over_budget_handoff_evicts_terminal_history_not_the_live_run() {
        // Regression (A8): an over-budget DB whose live handed-off Run has the
        // OLDEST updated_at_ms sorts that Run FIRST in the eviction candidate
        // scan. The candidate pool must agree with the `state_kind != 'running'`
        // DELETE guard, or opening aborts trying to evict a live row it can
        // never delete ("startup terminal Run {id} changed before eviction").
        let fixture = TempDir::new().expect("create over-budget handoff fixture");
        let state_dir = fixture.path().join("state");
        let (live_id, epoch) = seed_startup_overflow_with_live_row(&state_dir);

        let hooks = Arc::new(PersistenceTestHooks::default());
        let (store, recovered) = StateStore::open(
            &state_dir,
            AdmissionLimits::OPERATIONAL,
            Some(super::HandoffHint {
                epoch: epoch.clone(),
                live_set: HashSet::from([live_id]),
                state_lock_fd: None,
            }),
            Arc::clone(&hooks),
        )
        .expect("over-budget handoff open evicts terminal history, not the live run");

        // The live row survived eviction and is still running.
        assert_eq!(row_state_kind(&store.connection, live_id), "running");
        let live_run = recovered
            .iter()
            .find(|run| run.info.id == live_id)
            .expect("live handed-off Run recovered");
        assert_eq!(live_run.info.state, RunState::Running);

        // Eviction actually ran: the DB is trimmed to the operational cap while
        // the live row is retained, proving the path was exercised, not skipped.
        let retained: i64 = store
            .connection
            .query_row("SELECT count(*) FROM runs", [], |row| row.get(0))
            .expect("count retained rows after over-budget handoff");
        assert_eq!(retained, i64::try_from(MAX_RETAINED_RUNS).unwrap());
        assert_eq!(store.epoch, epoch);
    }

    #[test]
    fn reopening_with_inherited_lock_fd_does_not_self_deadlock() {
        use std::os::fd::OwnedFd;

        // The outgoing image still holds its advisory flock across exec-in-place;
        // the incoming image inherits that same descriptor and must reuse it.
        let handed = TempDir::new().expect("create inherited-lock fixture");
        let state_dir = handed.path().join("state");
        let (row_a, _row_b, epoch) = seed_two_running_rows(&state_dir);

        let lock_path = state_dir.join(super::LOCK_FILE);
        let held = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&lock_path)
            .expect("open the inherited lock file");
        held.try_lock().expect("hold the pre-exec state lock");

        // Contrast: the naive path (no inherited fd) freshly opens the lock and
        // re-locks, which self-blocks against the held lock and fails closed.
        let blocked = Persistence::open(&state_dir);
        assert!(
            matches!(blocked, Err(PersistenceError::StateInUse(_))),
            "a fresh open + try_lock must self-block against the held lock"
        );

        // Adopt path: a dup shares the same open file description (and its lock),
        // so the incoming image reuses it and skips the self-deadlocking re-lock.
        let inherited: OwnedFd = held
            .try_clone()
            .expect("model an exec-inherited lock descriptor")
            .into();
        let (persistence, _recovered) = Persistence::open_with_handoff(
            &state_dir,
            super::HandoffHint {
                epoch: epoch.clone(),
                live_set: HashSet::from([row_a]),
                state_lock_fd: Some(inherited),
            },
        )
        .expect("adopt the inherited state lock without self-deadlocking");
        assert_eq!(persistence.daemon_instance().to_string(), epoch);
        drop(persistence);
        drop(held);
    }
}
