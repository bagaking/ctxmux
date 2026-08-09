use std::{
    collections::{BTreeSet, HashMap, HashSet, VecDeque},
    fs::{self, File, OpenOptions},
    io,
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
use std::sync::atomic::AtomicBool;

use ctxmux_protocol::{
    CreateOperationKey, InterruptionReason, OutputChunk, OutputReplay, RunBackend, RunCapabilities,
    RunId, RunInfo, RunLineage, RunSpec, RunState,
};
use rusqlite::{Connection, OpenFlags, OptionalExtension, Transaction, params};
use thiserror::Error;
use uuid::Uuid;

use crate::run_spec::validate_run_spec;

const SCHEMA_VERSION: i64 = 2;
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

#[derive(Clone, Copy)]
struct AdmissionLimits {
    run_records: u64,
    metadata_bytes: u64,
}

impl AdmissionLimits {
    const FORMAT: Self = Self {
        run_records: RUN_RECORDS,
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

#[derive(Clone)]
pub(crate) struct Persistence {
    inner: Arc<PersistenceInner>,
}

struct PersistenceInner {
    sender: mpsc::SyncSender<Command>,
    failure: Arc<Mutex<Option<String>>>,
    join: Mutex<Option<thread::JoinHandle<()>>>,
    epoch: String,
    #[cfg(test)]
    test_hooks: Arc<PersistenceTestHooks>,
}

#[cfg(test)]
#[derive(Default)]
struct PersistenceTestHooks {
    fail_next_insert_after_commit: AtomicBool,
    fail_next_start_before_commit: AtomicBool,
    finalize_barrier: Mutex<Option<FinalizeTestBarrier>>,
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
    pub(crate) fn open(
        state_dir: impl Into<PathBuf>,
    ) -> Result<(Self, Vec<RecoveredRun>), PersistenceError> {
        Self::open_with_admission_limits(state_dir.into(), AdmissionLimits::FORMAT)
    }

    fn open_with_admission_limits(
        state_dir: PathBuf,
        admission_limits: AdmissionLimits,
    ) -> Result<(Self, Vec<RecoveredRun>), PersistenceError> {
        let (command_tx, command_rx) = mpsc::sync_channel(PERSISTENCE_QUEUE_CAPACITY);
        let (init_tx, init_rx) = mpsc::sync_channel(0);
        let failure = Arc::new(Mutex::new(None));
        let actor_failure = Arc::clone(&failure);
        #[cfg(test)]
        let test_hooks = Arc::new(PersistenceTestHooks::default());
        #[cfg(test)]
        let actor_test_hooks = Arc::clone(&test_hooks);
        let join = thread::Builder::new()
            .name("ctxmux-persistence".to_owned())
            .spawn(move || {
                actor_main(
                    &state_dir,
                    admission_limits,
                    &command_rx,
                    &init_tx,
                    &actor_failure,
                    #[cfg(test)]
                    &actor_test_hooks,
                );
            })
            .map_err(|error| PersistenceError::ActorStart(error.to_string()))?;
        let (epoch, recovered) = match init_rx.recv() {
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
                epoch,
                #[cfg(test)]
                test_hooks,
            }),
        };
        Ok((persistence, recovered))
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

fn actor_main(
    state_dir: &Path,
    admission_limits: AdmissionLimits,
    receiver: &mpsc::Receiver<Command>,
    init: &mpsc::SyncSender<Result<(String, Vec<RecoveredRun>), PersistenceError>>,
    failure: &Mutex<Option<String>>,
    #[cfg(test)] test_hooks: &Arc<PersistenceTestHooks>,
) {
    #[cfg(test)]
    let store_test_hooks = Arc::clone(test_hooks);
    let (mut store, recovered) = match StateStore::open(
        state_dir,
        admission_limits,
        #[cfg(test)]
        store_test_hooks,
    ) {
        Ok(value) => value,
        Err(error) => {
            let _ = init.send(Err(error));
            return;
        }
    };
    if init.send(Ok((store.epoch.clone(), recovered))).is_err() {
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

struct StateStore {
    state_dir: PathBuf,
    database_path: PathBuf,
    wal_path: PathBuf,
    shm_path: PathBuf,
    connection: Connection,
    epoch: String,
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
}

impl Drop for StateLockGuard {
    fn drop(&mut self) {
        if let Err(error) = File::unlock(&self.0) {
            eprintln!("ctxmuxd failed to release its state lock: {error}");
        }
    }
}

impl StateStore {
    fn open(
        state_dir: &Path,
        admission_limits: AdmissionLimits,
        #[cfg(test)] test_hooks: Arc<PersistenceTestHooks>,
    ) -> Result<(Self, Vec<RecoveredRun>), PersistenceError> {
        prepare_state_dir(state_dir)?;
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
        let state_lock = StateLockGuard::acquire(lock, state_dir, &lock_path)?;

        let database_path = state_dir.join(DATABASE_FILE);
        let wal_path = state_dir.join(format!("{DATABASE_FILE}-wal"));
        let shm_path = state_dir.join(format!("{DATABASE_FILE}-shm"));
        for path in [&database_path, &wal_path, &shm_path] {
            validate_optional_state_file(path)?;
        }
        let database_existed = database_path.exists();
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
        if database_existed {
            validate_existing_schema(&connection)?;
        } else {
            create_schema(&connection)?;
        }
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

        let epoch = Uuid::new_v4().to_string();
        reconcile_epoch(&connection, &epoch)?;
        let recovered = load_recovered(&connection)?;
        let store = Self {
            state_dir: state_dir.to_path_buf(),
            database_path,
            wal_path,
            shm_path,
            connection,
            epoch,
            admission_limits,
            _state_lock: state_lock,
            #[cfg(test)]
            test_hooks,
        };
        store.validate_files()?;
        Ok((store, recovered))
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
                    durable_oldest_seq, durable_head_seq, replay_bytes, replay_truncated,
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
        match self.connection.execute_batch("COMMIT") {
            Ok(()) => {
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
        for (id, replay, durable_head) in batch {
            let groups = split_chunks(&replay.chunks)?;
            if groups.is_empty() {
                self.append_transaction(&[(*id, replay.clone(), Arc::clone(durable_head))], None)?;
                continue;
            }
            for (index, chunks) in groups.iter().enumerate() {
                let is_last = index + 1 == groups.len();
                let partial = OutputReplay {
                    chunks: chunks.clone(),
                    oldest_seq: replay.oldest_seq,
                    head_seq: if is_last {
                        replay.head_seq
                    } else {
                        chunks.last().map_or(0, |chunk| chunk.seq)
                    },
                    truncated: replay.truncated,
                };
                self.append_transaction(&[(*id, partial, Arc::clone(durable_head))], None)?;
            }
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
                oldest_seq: replay.oldest_seq,
                head_seq: chunk_group.last().map_or(0, |chunk| chunk.seq),
                truncated: replay.truncated,
            };
            self.append_transaction(&[(id, prefix_replay, Arc::clone(durable_head))], None)?;
        }
        let terminal_replay = OutputReplay {
            chunks: final_chunks,
            oldest_seq: replay.oldest_seq,
            head_seq: replay.head_seq,
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
                "SELECT durable_head_seq FROM runs WHERE id = ?1",
                [id.to_string()],
                |row| row.get(0),
            )
            .map_err(PersistenceError::database)?;
        let durable_head = u64::try_from(durable_head)
            .map_err(|_| PersistenceError::Corrupt("negative durable head".to_owned()))?;
        Ok(replay
            .chunks
            .iter()
            .filter(|chunk| chunk.seq > durable_head)
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

fn create_schema(connection: &Connection) -> Result<(), PersistenceError> {
    connection
        .execute_batch(&format!(
            "PRAGMA page_size={PAGE_SIZE_BYTES};
             PRAGMA auto_vacuum=INCREMENTAL;
             PRAGMA user_version={SCHEMA_VERSION};
             CREATE TABLE runtime_meta (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                schema_version INTEGER NOT NULL,
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
                durable_oldest_seq INTEGER NOT NULL CHECK (durable_oldest_seq >= 0),
                durable_head_seq INTEGER NOT NULL CHECK (durable_head_seq >= 0),
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
                seq INTEGER NOT NULL CHECK (seq > 0),
                data BLOB NOT NULL,
                UNIQUE(run_id, seq)
             );
             CREATE INDEX replay_chunks_run_seq ON replay_chunks(run_id, seq);
             INSERT INTO runtime_meta(singleton, schema_version, current_epoch)
             VALUES (1, {SCHEMA_VERSION}, 'initializing');"
        ))
        .map_err(PersistenceError::database)?;
    Ok(())
}

fn validate_existing_schema(connection: &Connection) -> Result<(), PersistenceError> {
    let version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(PersistenceError::database)?;
    if version != SCHEMA_VERSION {
        return Err(PersistenceError::UnsupportedSchema {
            found: version,
            expected: SCHEMA_VERSION,
        });
    }
    let (meta_rows, meta_version, current_epoch): (i64, i64, String) = connection
        .query_row(
            "SELECT count(*), min(schema_version), min(current_epoch) FROM runtime_meta",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
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
        ("index".to_owned(), "replay_chunks_run_seq".to_owned()),
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
        &["singleton", "schema_version", "current_epoch"],
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
            "durable_oldest_seq",
            "durable_head_seq",
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
        &["ordinal", "run_id", "seq", "data"],
    )?;
    validate_creation_key_index(connection)?;
    validate_database_format_pragmas(connection)
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

fn reconcile_epoch(connection: &Connection, epoch: &str) -> Result<(), PersistenceError> {
    let transaction = connection
        .unchecked_transaction()
        .map_err(PersistenceError::database)?;
    let interrupted = RunState::Interrupted {
        reason: InterruptionReason::DaemonRestart,
    };
    let state_json =
        serde_json::to_string(&interrupted).map_err(PersistenceError::serialization)?;
    let now = now_millis();
    let running = {
        let mut statement = transaction
            .prepare(
                "SELECT id, creation_key, spec_json, lineage_json, source_epoch
                 FROM runs WHERE state_kind = 'running'",
            )
            .map_err(PersistenceError::database)?;
        statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, String>(4)?,
                ))
            })
            .map_err(PersistenceError::database)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(PersistenceError::database)?
    };
    for (id, creation_key, spec_json, lineage_json, source_epoch) in running {
        let metadata_bytes = metadata_size(
            &id,
            &creation_key,
            &spec_json,
            lineage_json.as_deref(),
            &state_json,
            &source_epoch,
        )?;
        transaction
            .execute(
                "UPDATE runs SET state_kind = 'interrupted', state_json = ?2, pid = NULL,
                 updated_at_ms = ?3, terminal_at_ms = ?3, metadata_bytes = ?4 WHERE id = ?1",
                params![
                    id,
                    state_json,
                    now,
                    i64::try_from(metadata_bytes).expect("metadata budget fits SQLite")
                ],
            )
            .map_err(PersistenceError::database)?;
    }
    evict_terminal_overflow(&transaction)?;
    transaction
        .execute(
            "UPDATE runtime_meta SET current_epoch = ?1 WHERE singleton = 1",
            [epoch],
        )
        .map_err(PersistenceError::database)?;
    transaction.commit().map_err(PersistenceError::database)
}

fn evict_terminal_overflow(transaction: &Transaction<'_>) -> Result<(), PersistenceError> {
    loop {
        let (records, metadata): (i64, i64) = transaction
            .query_row(
                "SELECT count(*), coalesce(sum(metadata_bytes), 0) FROM runs",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(PersistenceError::database)?;
        if nonnegative_u64(records, "record count")? <= RUN_RECORDS
            && nonnegative_u64(metadata, "metadata total")? <= METADATA_BYTES
        {
            return Ok(());
        }
        let candidate: Option<String> = transaction
            .query_row(
                "SELECT id FROM runs WHERE state_kind != 'running'
                 ORDER BY coalesce(terminal_at_ms, updated_at_ms), created_at_ms, id LIMIT 1",
                [],
                |row| row.get(0),
            )
            .optional()
            .map_err(PersistenceError::database)?;
        let Some(candidate) = candidate else {
            return Err(PersistenceError::Corrupt(
                "running records exceed startup metadata retention".to_owned(),
            ));
        };
        transaction
            .execute("DELETE FROM runs WHERE id = ?1", [candidate])
            .map_err(PersistenceError::database)?;
    }
}

fn validate_application_state(connection: &Connection) -> Result<(), PersistenceError> {
    let mut statement = connection
        .prepare(
            "SELECT id, creation_key, spec_json, lineage_json, state_kind, state_json, source_epoch, pid,
                    durable_oldest_seq, durable_head_seq, replay_bytes, replay_truncated,
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
        .prepare("SELECT seq, length(data) FROM replay_chunks WHERE run_id = ?1 ORDER BY seq")
        .map_err(PersistenceError::database)?;
    let chunks = statement
        .query_map([id.to_string()], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
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
    for (seq, len) in chunks {
        let seq = nonnegative_u64(seq, "chunk sequence")?;
        let len = nonnegative_u64(len, "chunk length")?;
        if seq != expected {
            return Err(PersistenceError::Corrupt(format!(
                "Run {id} replay is not contiguous at {seq}, expected {expected}"
            )));
        }
        expected = expected.saturating_add(1);
        bytes = bytes.saturating_add(len);
    }
    if expected.saturating_sub(1) != head || bytes != replay_bytes {
        return Err(PersistenceError::Corrupt(format!(
            "Run {id} replay cursors or bytes do not match chunks"
        )));
    }
    if oldest > 1 && !truncated {
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
            "SELECT id, creation_key, spec_json, lineage_json, state_json, pid, durable_oldest_seq,
                    durable_head_seq, replay_truncated, metadata_bytes
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
    let oldest_seq = nonnegative_u64(
        row.get(6).map_err(PersistenceError::database)?,
        "recovered oldest",
    )?;
    let head_seq = nonnegative_u64(
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
            head_seq,
            durable_head_seq: Some(head_seq),
            oldest_seq,
            attachments: 0,
        },
        replay: OutputReplay {
            chunks: load_recovered_chunks(connection, &id_text)?,
            oldest_seq,
            head_seq,
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
        .prepare("SELECT seq, data FROM replay_chunks WHERE run_id = ?1 ORDER BY seq")
        .map_err(PersistenceError::database)?;
    statement
        .query_map([id], |row| {
            Ok(OutputChunk {
                seq: u64::try_from(row.get::<_, i64>(0)?).map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Integer,
                        Box::new(error),
                    )
                })?,
                data: row.get(1)?,
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
            "SELECT durable_oldest_seq, durable_head_seq, replay_bytes, state_kind
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
            .any(|chunk| chunk.seq > durable_head_unsigned)
    {
        return Err(PersistenceError::Mutation(format!(
            "cannot advance replay for terminal Run {id}"
        )));
    }
    for chunk in &replay.chunks {
        let seq = i64::try_from(chunk.seq)
            .map_err(|_| PersistenceError::Mutation("output sequence exceeds SQLite".to_owned()))?;
        if seq <= durable_head {
            if seq >= durable_oldest && durable_oldest != 0 {
                let stored: Vec<u8> = transaction
                    .query_row(
                        "SELECT data FROM replay_chunks WHERE run_id = ?1 AND seq = ?2",
                        params![&id_text, seq],
                        |row| row.get(0),
                    )
                    .map_err(PersistenceError::database)?;
                if stored != chunk.data {
                    return Err(PersistenceError::Mutation(format!(
                        "Run {id} replay sequence {} changed bytes",
                        chunk.seq
                    )));
                }
            }
            continue;
        }
        if durable_head == 0 && durable_oldest == 0 {
            durable_oldest = seq;
        } else if seq != durable_head + 1 {
            return Err(PersistenceError::Mutation(format!(
                "Run {id} durable replay gap: got {seq}, expected {}",
                durable_head + 1
            )));
        }
        transaction
            .execute(
                "INSERT INTO replay_chunks(run_id, seq, data) VALUES (?1, ?2, ?3)",
                params![&id_text, seq, &chunk.data],
            )
            .map_err(PersistenceError::database)?;
        durable_head = seq;
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
    let truncated = replay.truncated || durable_oldest > 1;
    transaction
        .execute(
            "UPDATE runs SET durable_oldest_seq = ?2, durable_head_seq = ?3,
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
                "SELECT seq, length(data) FROM replay_chunks WHERE run_id = ?1 ORDER BY seq LIMIT 1",
                [id_text],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(PersistenceError::database)?;
        let Some((seq, bytes)) = evicted else {
            return Err(PersistenceError::Corrupt(format!(
                "Run {id} replay accounting has no chunks"
            )));
        };
        transaction
            .execute(
                "DELETE FROM replay_chunks WHERE run_id = ?1 AND seq = ?2",
                params![id_text, seq],
            )
            .map_err(PersistenceError::database)?;
        evicted_any = true;
        *replay_bytes = replay_bytes.saturating_sub(bytes);
        *durable_oldest = transaction
            .query_row(
                "SELECT coalesce(min(seq), 0) FROM replay_chunks WHERE run_id = ?1",
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
                "SELECT chunk.ordinal, chunk.run_id, chunk.seq, length(chunk.data)
                 FROM replay_chunks AS chunk
                 WHERE (SELECT count(*) FROM replay_chunks AS retained
                        WHERE retained.run_id = chunk.run_id) > 1
                 ORDER BY chunk.ordinal LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()
            .map_err(PersistenceError::database)?;
        let Some((ordinal, run_id, _seq, bytes)) = candidate else {
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
                 durable_oldest_seq = coalesce(
                   (SELECT min(seq) FROM replay_chunks WHERE run_id = ?1), 0
                 ) WHERE id = ?1",
                params![run_id, bytes],
            )
            .map_err(PersistenceError::database)?;
    }
}

fn read_run_head(transaction: &Transaction<'_>, id: RunId) -> Result<u64, PersistenceError> {
    let value: i64 = transaction
        .query_row(
            "SELECT durable_head_seq FROM runs WHERE id = ?1",
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
                "output chunk {} exceeds the transaction payload ceiling",
                chunk.seq
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
    use std::collections::BTreeMap;
    use std::fs::OpenOptions;

    use ctxmux_protocol::{
        CreateOperationKey, OutputChunk, OutputReplay, RunBackend, RunCapabilities, RunId, RunInfo,
        RunSpec, RunState, TerminalSize,
    };
    use rusqlite::{Connection, params};
    use tempfile::TempDir;

    use super::{
        AdmissionLimits, DATABASE_MAX_BYTES, GLOBAL_REPLAY_BYTES, MAX_TRANSACTION_PAYLOAD_BYTES,
        METADATA_BYTES, PAGE_SIZE_BYTES, PER_RUN_REPLAY_BYTES, PERSISTENCE_QUEUE_CAPACITY,
        Persistence, PersistenceError, PersistentCandidate, PersistentStartCompletion, RUN_RECORDS,
        SHM_MAX_BYTES, STATE_FILES_MAX_BYTES, StartDisposition, StartReceipt, StateLockGuard,
        WAL_CHECKPOINT_BYTES, WAL_MAX_BYTES, append_replay, create_schema, metadata_size,
        prune_global_replay_to, validate_existing_schema, wal_charge_for_cache,
    };

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
    fn creation_key_index_is_unique_binary_and_exactly_validated() {
        let connection = test_connection();
        validate_existing_schema(&connection).expect("accept canonical schema 2 index");

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
            &replay(vec![chunk(1, b"aaa"), chunk(2, b"bbb")]),
        )
        .expect("append first replay");
        append_replay(
            &transaction,
            second,
            &replay(vec![chunk(1, b"ccc"), chunk(2, b"ddd")]),
        )
        .expect("append second replay");
        assert!(prune_global_replay_to(&transaction, 7).expect("prune global replay"));
        for id in [first, second] {
            let (oldest, head, bytes, truncated): (i64, i64, i64, i64) = transaction
                .query_row(
                    "SELECT durable_oldest_seq, durable_head_seq, replay_bytes,
                            replay_truncated FROM runs WHERE id = ?1",
                    [id.to_string()],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .expect("read pruned replay accounting");
            assert_eq!((oldest, head, bytes, truncated), (2, 2, 3, 1));
        }
        transaction.commit().expect("commit replay pruning");
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
            Persistence::open_with_admission_limits(state_dir.clone(), limits)
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

        let first_replay = replay(vec![chunk(1, b"first")]);
        first_durable.append(first.id, first_replay.clone());
        first_durable.finalize(first.id, 42, first_replay, exited_state());
        assert_eq!(first_durable.durable_head(), 1);

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
            Persistence::open_with_admission_limits(state_dir.clone(), limits)
                .expect("open exact-candidate store");
        assert!(recovered.is_empty());

        let first = running_info(RunId::new());
        let first_key = test_operation_key(first.id);
        let first_durable = persistence
            .insert_start(&first_key, &first)
            .expect("insert candidate");
        let first_replay = replay(vec![chunk(1, b"retained")]);
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
        assert_eq!(recovered[0].replay.chunks, vec![chunk(1, b"retained")]);
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
        durable.append(first.id, replay(vec![chunk(1, b"committed")]));
        durable.append(first.id, replay(vec![chunk(1, b"conflict")]));

        let later = running_info(RunId::new());
        let Err(error) = persistence.insert_start(&test_operation_key(later.id), &later) else {
            panic!("fatal replay conflict admitted a later mutation");
        };
        assert!(matches!(error, PersistenceError::Mutation(_)));
        assert!(error.to_string().contains("changed bytes"));
        assert_eq!(durable.durable_head(), 1);
        drop(durable);
        drop(persistence);

        let (reopened, recovered) =
            Persistence::open(state_dir).expect("reopen prior durable unit");
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].info.id, first.id);
        assert_eq!(recovered[0].replay.chunks, vec![chunk(1, b"committed")]);
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

    fn test_connection() -> Connection {
        let connection = Connection::open_in_memory().expect("open in-memory persistence store");
        connection
            .execute_batch("PRAGMA foreign_keys=ON;")
            .expect("enable test foreign keys");
        create_schema(&connection).expect("create test persistence schema");
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
                    durable_oldest_seq, durable_head_seq, replay_bytes, replay_truncated,
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
            head_seq: 0,
            durable_head_seq: Some(0),
            oldest_seq: 0,
            attachments: 0,
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
            oldest_seq: chunks.first().map_or(0, |chunk| chunk.seq),
            head_seq: chunks.last().map_or(0, |chunk| chunk.seq),
            chunks,
            truncated: false,
        }
    }

    fn chunk(seq: u64, data: &[u8]) -> OutputChunk {
        OutputChunk {
            seq,
            data: data.to_vec(),
        }
    }
}
