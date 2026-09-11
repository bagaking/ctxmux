use std::{
    collections::{BTreeSet, HashMap, HashSet, VecDeque},
    fs::{self, File, OpenOptions},
    io::{self, BufReader, Read, Seek, SeekFrom, Write},
    os::fd::{AsRawFd, OwnedFd, RawFd},
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[cfg(test)]
use std::sync::{
    OnceLock,
    atomic::{AtomicI32, AtomicU8},
};

use ctxmux_protocol::{
    CreateOperationKey, DaemonInstanceId, InterruptionReason, OutputChunk, OutputReplay,
    RunBackend, RunCapabilities, RunId, RunInfo, RunLineage, RunSpec, RunState, RuntimeId,
};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    resources::ResourceLimits,
    run_spec::validate_run_spec,
    terminal_checkpoint::{MAX_RESTORE_BYTES, StoredCheckpoint},
};

const SCHEMA_VERSION: i64 = 6;
const DATABASE_FILE: &str = "state.sqlite3";
const LOCK_FILE: &str = "state.lock";
const REPLAY_DIR: &str = "replay";
const PAGE_SIZE_BYTES: u64 = 4 * 1024;
#[cfg(test)]
const DATABASE_MAX_BYTES: u64 = 384 * 1024 * 1024;
#[cfg(test)]
const WAL_CHECKPOINT_BYTES: u64 = 8 * 1024 * 1024;
/// Smallest WAL the idle fold will volunteer to truncate.
///
/// Folding is not free to the *next* writer. `wal_checkpoint(TRUNCATE)` leaves
/// the file at zero length, so the next commit must extend it again -- which
/// degrades `fdatasync` into a full `fsync`, because a size change has to reach
/// the inode -- and must write and separately sync a fresh 32-byte WAL header.
/// On cn3 (ext4, `synchronous=FULL`) that costs the next commit +0.995 ms:
/// 1.912 ms landing on a just-truncated WAL against 0.917 ms landing on one
/// already a few hundred KiB long.
///
/// So an idle fold is a trade, not a free tidy-up: it spends its own cost now,
/// plus that penalty on whoever commits next, to avoid a larger fold later. On
/// a quiet fleet the later fold it avoids is the *cheapest* one -- a few KiB,
/// which the same measurement prices at ~1.25 ms -- so folding there pays twice
/// to dodge something smaller than either payment. Under load the trade is the
/// good one the call site describes, because the WAL is megabytes by then.
///
/// 256 KiB is the smallest measured size whose fold (1.436 ms) exceeds the
/// reset penalty with margin, and it is ~32x below `WAL_CHECKPOINT_BYTES`, so
/// every bound that folds above that ceiling is untouched. This floor changes
/// only when we *volunteer* to fold; it is not part of any proof.
const WAL_IDLE_FOLD_FLOOR_BYTES: u64 = 256 * 1024;
#[cfg(test)]
const WAL_MAX_BYTES: u64 = 16 * 1024 * 1024;
#[cfg(test)]
const SHM_MAX_BYTES: u64 = ResourceLimits::DEFAULT.shm_bytes();
// Replay payloads live outside SQLite. Keep a bounded safety ceiling for the
// state directory, while the logical replay budget remains the product-level
// retention policy. The old 404 MiB aggregate was the SQLite ceiling plus WAL
// and SHM; it would incorrectly reject a healthy store merely because its
// retained bytes no longer fit in the metadata database.
#[cfg(test)]
const STATE_FILES_MAX_BYTES: u64 = DATABASE_MAX_BYTES
    + WAL_MAX_BYTES
    + SHM_MAX_BYTES
    + 3 * GLOBAL_REPLAY_BYTES
    + MAX_TRANSACTION_PAYLOAD_BYTES as u64;
#[cfg(test)]
const PER_RUN_REPLAY_BYTES: u64 = 4 * 1024 * 1024;
#[cfg(test)]
const GLOBAL_REPLAY_BYTES: u64 = 256 * 1024 * 1024;
#[cfg(test)]
const TEST_REPLAY_COMPACTION_TRIGGER_BYTES: u64 = 1024;
pub(crate) const METADATA_BYTES: u64 = ResourceLimits::DEFAULT.metadata_bytes;
const MAX_TRANSACTION_PAYLOAD_BYTES: usize = 1024 * 1024;
/// Maximum collection time from the first Append, before any store transaction.
/// An empty queue is common for small output streams; wait briefly for the next
/// Append rather than syncing one tiny row per PTY read. The deadline never
/// slides, and a barrier, lifecycle wake or shutdown ends collection early.
/// This bounds collection only: storage retries and sync can take longer.
const APPEND_BATCH_WINDOW: Duration = Duration::from_millis(10);
/// Target size of one `replay_chunks` row.
///
/// A row costs the same fixed overhead whether it carries 200 bytes or 200 KiB:
/// a record header, a 36-byte `run_id`, three integers, and an entry in the
/// `UNIQUE(run_id, start_byte)` index. A PTY read averages 200-600 bytes on the
/// farm host, so storing one row per read spends most of the WAL on that
/// overhead — measured at 2.0-2.8 WAL bytes per byte of real output, which
/// divides the ~57 MB/s fold ceiling down to a ~24 MB/s output ceiling and is
/// exactly where the chatty cliff sits.
///
/// Bytes inside one transaction are already proven contiguous per Run (see
/// `is_fresh_contiguous`), so they can share a row without changing what is
/// stored — only how it is packed. Replicated against the real schema, packing
/// to this size cuts amplification from 2.27x to 1.07x (53%), which is 95% of
/// what an unbounded row would save.
///
/// The size is a ceiling on a row built in memory, never a row grown in place:
/// appending to a stored row would rewrite all of its pages per push, which is
/// worse than the fragmentation it would fix. It also bounds the two places a
/// row is handled whole: attachment sends bounded pages well under
/// `MAX_FRAME_BYTES`. Retention clips exact prefixes inside rows; this packing
/// size cannot discard otherwise funded output.
const COALESCE_ROW_BYTES: usize = 64 * 1024;
/// Depth of the actor's command queue.
///
/// [`PersistentRun::append`] refuses a full queue before transfer. The native
/// owner preserves unoffered bytes, pauses that reader at byte pressure and
/// retries on queue-space wake, including quiet debt. Lifecycle requests use a
/// separate shallow queue and configured finalization workers, so output queue
/// depth cannot consume every lifecycle dispatch permit.
///
/// The measurements below explain the inherited operating point; they are
/// historical evidence for burst absorption, not a license to drop output or
/// force lifecycle requests behind every queued Append.
///
/// That is why this is 16 and not the 64 it buffered at before, nor the 1024
/// before that. A loud fleet refills every slot the actor frees, so the queue
/// sits pinned full and a lifecycle verb waits for the whole depth to drain.
/// The drain rate is a hardware constant — measured 186 MB/s on the farm host,
/// identical at both depths — so the wait is simply depth ÷ rate, and quartering
/// the depth quarters the wait: c8 `start` 221 → 93 ms, `stop` 384 → 89 ms,
/// `remove` 456 → 99 ms, with commit size unchanged to the byte (2696 B/write
/// in both arms) and fleet throughput unchanged (0.995×).
///
/// 16 is where that trade is still free, and "free" is measured rather than
/// assumed. Depth is often said to buy the output path larger transactions;
/// between 64 and 16 it does not. Steady-state rows sit pinned at the
/// `COALESCE_ROW_BYTES` ceiling either way (64082 B/row at 16 vs 64059 B at 64),
/// bytes per write are identical, and write throughput is 0.995×: eight reactor
/// threads keep the actor's `try_recv` non-empty, so a batch fills to
/// `MAX_TRANSACTION_PAYLOAD_BYTES` long before the depth is what bounds it.
/// Depth still buys burst absorption for a fleet quiet enough to let the queue
/// drain — that is simply not the regime this constant is being priced for.
/// See `docs/architecture/r24-the-create-was-never-doing-the-work.md`.
pub(crate) const PERSISTENCE_QUEUE_CAPACITY: usize = 16;
/// Depth of the lifecycle command queue.
///
/// Shallow on purpose — see [`PersistenceInner::lifecycle`]. Lifecycle verbs
/// are rare and every caller blocks on its own reply, so this needs to hold
/// only the handful that can be in flight across concurrent connections, not a
/// burst.
const LIFECYCLE_QUEUE_CAPACITY: usize = 8;
const LIFECYCLE_METADATA_RESERVE_BYTES: usize = 128;
const WAL_HEADER_BYTES: u64 = 32;
const WAL_FRAME_BYTES: u64 = 24 + PAGE_SIZE_BYTES;
const STARTUP_BATCH_MAX_ROWS: usize = 128;
const STORAGE_RETRY_INTERVAL: Duration = Duration::from_millis(50);
// SQLite's WAL checkpoint pragma reports a transient reader conflict in its
// result row instead of returning an error. Keep this retry budget local to
// the checkpoint owner: a short-lived external reader must not poison the
// persistence actor, while a reader that never leaves still fails closed.
const WAL_CHECKPOINT_MAX_RETRIES: usize = 8;
const WAL_CHECKPOINT_INITIAL_BACKOFF: Duration = Duration::from_millis(10);
const WAL_CHECKPOINT_MAX_BACKOFF: Duration = Duration::from_millis(100);

#[derive(Clone, Copy)]
struct AdmissionLimits {
    run_records: u64,
    metadata_bytes: u64,
    resources: ResourceLimits,
}

impl From<ResourceLimits> for AdmissionLimits {
    fn from(resources: ResourceLimits) -> Self {
        Self {
            run_records: resources.retained_runs.map_or(u64::MAX, |n| n as u64),
            metadata_bytes: resources.metadata_bytes,
            resources,
        }
    }
}

impl AdmissionLimits {
    #[cfg(test)]
    const FORMAT: Self = Self {
        run_records: u64::MAX,
        metadata_bytes: METADATA_BYTES,
        resources: ResourceLimits::DEFAULT,
    };

    const OPERATIONAL: Self = Self {
        run_records: u64::MAX,
        metadata_bytes: METADATA_BYTES,
        resources: ResourceLimits::DEFAULT,
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
    #[error("ctxmux durable state resource pressure: {0}")]
    ResourcePressure(String),
    #[error("ctxmux durable state I/O failed at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("ctxmux durable state database failed: {message}")]
    Database {
        message: String,
        code: Option<rusqlite::ErrorCode>,
        /// `SQLite`'s extended result code, retained because the primary code is
        /// too coarse to classify an I/O failure. Every `SQLITE_IOERR_*` shares
        /// the primary `SystemIoFailure`, but only some of them describe a
        /// condition an operator can clear (a full filesystem) rather than a
        /// broken store.
        extended_code: Option<i32>,
    },
    #[error("ctxmux persistence actor stopped")]
    ActorStopped,
    #[error(
        "WAL truncate checkpoint could not reach zero bytes after {attempts} attempts ({detail})"
    )]
    WalCheckpointBusy { attempts: usize, detail: String },
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

    #[allow(
        clippy::needless_pass_by_value,
        reason = "the owned signature is used directly by Result::map_err while preserving SQLite's typed code"
    )]
    fn database(error: rusqlite::Error) -> Self {
        let extended_code = match &error {
            rusqlite::Error::SqliteFailure(failure, _) => Some(failure.extended_code),
            _ => None,
        };
        Self::Database {
            code: error.sqlite_error_code(),
            extended_code,
            message: error.to_string(),
        }
    }

    fn database_message(error: impl std::fmt::Display) -> Self {
        Self::Database {
            message: error.to_string(),
            code: None,
            extended_code: None,
        }
    }

    fn is_disk_full(&self) -> bool {
        matches!(
            self,
            Self::Database {
                code: Some(rusqlite::ErrorCode::DiskFull),
                ..
            }
        )
    }

    /// A `SQLITE_IOERR` whose extended code names a write path that a full
    /// filesystem defeats.
    ///
    /// `SQLITE_FULL` is not the only way a full disk arrives. It is what `SQLite`
    /// reports when the database file itself cannot grow, but a write to the WAL,
    /// the rollback journal, or an `fsync` that flushes either one surfaces the
    /// underlying `ENOSPC` as an `SQLITE_IOERR_*` instead. Those share the
    /// primary `SystemIoFailure` code with genuinely broken storage, so the
    /// extended code is the only thing that separates "the operator can free
    /// space and the store is intact" from "this store cannot be trusted".
    ///
    /// The list is deliberately an allowlist of write-side failures rather than
    /// "any `SystemIoFailure`". Read, lock, delete, and mmap I/O errors, and
    /// every unrecognized extended code, stay fail-closed: retrying those would
    /// convert an unreadable or corrupt store into an unbounded hang, which is
    /// strictly worse than a latched actor an operator can see.
    fn is_storage_pressure_io_failure(&self) -> bool {
        let Self::Database {
            code: Some(rusqlite::ErrorCode::SystemIoFailure),
            extended_code: Some(extended),
            ..
        } = self
        else {
            return false;
        };
        matches!(
            *extended,
            rusqlite::ffi::SQLITE_IOERR_WRITE
                | rusqlite::ffi::SQLITE_IOERR_FSYNC
                | rusqlite::ffi::SQLITE_IOERR_DIR_FSYNC
                | rusqlite::ffi::SQLITE_IOERR_TRUNCATE
        )
    }

    fn is_database_busy(&self) -> bool {
        matches!(
            self,
            Self::Database {
                code: Some(rusqlite::ErrorCode::DatabaseBusy),
                ..
            }
        )
    }

    fn is_transient_storage(&self) -> bool {
        self.is_disk_full()
            || self.is_storage_pressure_io_failure()
            || matches!(
                self,
                Self::Io { source, .. } if source.kind() == io::ErrorKind::StorageFull
            )
            || matches!(self, Self::WalCheckpointBusy { .. })
    }

    #[cfg(test)]
    fn injected_disk_full() -> Self {
        Self::Database {
            message: "injected database or disk is full".to_owned(),
            code: Some(rusqlite::ErrorCode::DiskFull),
            extended_code: Some(rusqlite::ffi::SQLITE_FULL),
        }
    }

    /// Build the exact error shape a full filesystem produces when the write
    /// lands on the WAL rather than on the database file.
    #[cfg(test)]
    fn injected_io_failure(extended_code: i32) -> Self {
        Self::Database {
            message: format!("injected disk I/O error ({extended_code})"),
            code: Some(rusqlite::ErrorCode::SystemIoFailure),
            extended_code: Some(extended_code),
        }
    }

    #[cfg(test)]
    fn injected_io_error() -> Self {
        Self::database(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_IOERR),
            Some("disk I/O error".to_owned()),
        ))
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
    pub(crate) source_gap_after_byte: Option<u64>,
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
    resources: ResourceLimits,
    lifecycle_space: Arc<tokio::sync::Notify>,
    output_wake: Arc<Mutex<Option<crate::native_runtime::OwnerWake>>>,
    output_wake_requested: Arc<AtomicBool>,
    state_dir: PathBuf,
    sender: mpsc::SyncSender<Command>,
    /// Lifecycle commands, kept off `sender` so they do not queue behind the
    /// append backlog.
    ///
    /// Measured on cn3 before this existed: at eight chatty Runs a `finalize`
    /// spent 93.0 ms of its 94.9 ms waiting to be *dequeued*, against 1.8 ms of
    /// actual work and **one microsecond** blocked at admission. The wait was
    /// pure FIFO position — the command was already in the queue, just behind
    /// up to a full depth of appends — and it scaled 5472x from a quiet fleet
    /// to a loud one while the admission time did not move at all.
    ///
    /// That measurement is what picks a second channel over the alternatives.
    /// A second `SQLite` connection cannot help: WAL permits one writer, enforced
    /// by an exclusive lock held while frames are appended, so the lifecycle
    /// transaction would block on that lock for the same interval and we would
    /// additionally own `SQLITE_BUSY` retries. A second database file would
    /// give isolation but costs cross-file atomicity, which WAL does not
    /// provide for attached databases. Neither is needed for a command that is
    /// merely standing in the wrong line.
    ///
    /// Depth is 8 and deliberately shallow. This is not a buffer: lifecycle
    /// commands are rare and each caller blocks on its own reply, so the queue
    /// exists to avoid a send-side stall, not to absorb a burst. Kafka's
    /// KIP-291 sized its controller queue at 20 on the same reasoning.
    lifecycle: mpsc::SyncSender<Command>,
    /// Appends handed to `sender` that the actor has not yet dequeued.
    ///
    /// The channel itself cannot be asked how full it is, and finding out by
    /// sending is exactly the wrong order: rendering the replay is the expensive
    /// half, so a caller that discovers fullness from a failed `try_send` has
    /// already paid for a message it then throws away. Under sustained overload
    /// that is *every* push, which is why fixing the catch-up render alone left
    /// the fleet wedged (512 x 40/s went from 199 to 325 admitted, not to 512).
    ///
    /// So the depth is tracked explicitly: incremented before a send, decremented
    /// by the actor as it dequeues. It is advisory — it can be stale in either
    /// direction, and `try_send` remains the real admission decision — but it is
    /// enough to skip the render when the queue is visibly saturated.
    queue_depth: Arc<AtomicUsize>,
    failure: Arc<Mutex<Option<String>>>,
    shutdown: Arc<AtomicBool>,
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
    append_batch_window: Mutex<Option<Duration>>,
    append_wait_started: Mutex<Option<mpsc::Sender<()>>>,
    fail_next_insert_after_commit: AtomicBool,
    maintenance_commits_before_error: AtomicU64,
    maintenance_commits_before_stat_error: AtomicU64,
    maintenance_error_committed: AtomicBool,
    fail_next_start_before_commit: AtomicBool,
    finalize_barrier: Mutex<Option<FinalizeTestBarrier>>,
    /// Stalls the actor inside an append so a test can hold it there while the
    /// queue fills, standing in for a slow fsync.
    append_barrier: Mutex<Option<FinalizeTestBarrier>>,
    /// Records the SHAPE of the next offered append — where it starts and how
    /// many bytes it carries. A test that re-derives the expected replay proves
    /// nothing (it recomputes the very expression under test); observing what
    /// the caller actually handed over is what catches a whole-log recopy.
    #[cfg(test)]
    observed_append: Mutex<Option<ObservedAppend>>,
    startup_batch_wal_bytes: Mutex<Vec<u64>>,
    startup_fail_after_commits: AtomicU64,
    startup_over_budget_attempts: AtomicU64,
    force_startup_over_budget_once: AtomicBool,
    start_commit_crash_phase: AtomicU8,
    fail_next_start_commit_as: Mutex<Option<CommitProbe>>,
    fail_next_append_as_disk_full: AtomicBool,
    force_append_storage_full: AtomicBool,
    fail_next_append_as_io_error: AtomicBool,
    fail_next_finalize_as_disk_full: AtomicBool,
    /// Extended `SQLite` result code to inject once on the next append, standing
    /// in for the `SQLITE_IOERR_*` shape a full filesystem produces when the
    /// write lands on the WAL rather than on the database file.
    fail_next_append_as_io_failure: AtomicI32,
    checkpoint_attempts: AtomicU64,
    /// Idle folds are counted apart from `checkpoint_attempts`: they are a
    /// different event (nobody is waiting on one), and folding them into the
    /// same counter would let an idle fold satisfy a test that means to observe
    /// the create path retrying a busy checkpoint.
    idle_folds: AtomicU64,
    /// Set by fixtures whose subject is the WAL state itself -- an idle fold
    /// arriving first would zero the WAL out from under them.
    suppress_idle_fold: AtomicBool,
}

#[cfg(test)]
static NEXT_OPEN_TEST_HOOKS: Mutex<Option<Arc<PersistenceTestHooks>>> = Mutex::new(None);

#[cfg(test)]
const REPLAY_COMPACTION_CRASH_PHASE: &str = "CTXMUX_REPLAY_COMPACTION_CRASH_PHASE";

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

/// Durable outcome of one exact terminal-Run removal.
///
/// Removal has no successor row, so a rolled-back or rejected removal simply
/// leaves the exact candidate present and is safe to surface as a typed error.
/// An unclassifiable rollback failure latches the persistence actor exactly as
/// a start `CommitUnknown` does, so the daemon fail-stops rather than diverging
/// durable and in-memory truth.
pub(crate) enum RemovalDisposition {
    /// The exact row and its cascading replay were deleted and committed.
    Removed,
    /// No durable mutation occurred; the exact candidate remains present.
    NotRemoved(PersistenceError),
    /// The rollback or classification could not be proven; restart is required.
    Unknown(PersistenceError),
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

/// What one offered append actually carried, captured at the moment the caller
/// handed it over.
#[cfg(test)]
#[derive(Clone, Copy, Debug)]
pub(crate) struct ObservedAppend {
    /// Start byte of the first chunk, or `None` for an empty replay.
    pub(crate) first_byte: Option<u64>,
    /// Total chunk bytes — the copy this offer cost.
    pub(crate) payload_bytes: usize,
}

/// Reads back the append offer recorded since this observer was created.
#[cfg(test)]
pub(crate) struct AppendObserver {
    persistence: Persistence,
}

#[cfg(test)]
impl AppendObserver {
    pub(crate) fn take(&self) -> Option<ObservedAppend> {
        mutex_lock(&self.persistence.inner.test_hooks.observed_append).take()
    }
}

impl PersistenceInner {
    /// Send one lifecycle command, then nudge the actor in case it is parked.
    /// Returns whether the command was accepted; a refusal means the actor is
    /// gone, which every caller turns into its own `ActorStopped` disposition.
    ///
    /// The nudge is `try_send`, never `send`. A blocking wake would put the
    /// lifecycle caller right back behind the append backlog this channel
    /// exists to escape — the bug, reintroduced through the fix. A refused
    /// nudge is also provably harmless: `try_send` only fails when the append
    /// channel is full, and a full append channel means the actor is not parked
    /// in `recv()`, so it reaches the top of the loop on its own and finds the
    /// command there.
    fn send_lifecycle(&self, command: Command) -> bool {
        if self.lifecycle.send(command).is_err() {
            return false;
        }
        let _ = self.sender.try_send(Command::LifecycleWake);
        true
    }
}

impl Drop for PersistenceInner {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
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
    /// The highest byte this Run has successfully handed to the actor — the
    /// exclusive end of the newest ACCEPTED append, which is where the next
    /// replay must start.
    ///
    /// This is deliberately NOT `durable_head`. Every accepted append either
    /// commits or latches persistence off daemon-wide (`remember_failure`);
    /// there is no third outcome in which a queued append is silently dropped.
    /// So "offered" already implies "will be durable, or nothing is", and
    /// re-sending bytes that are merely still IN FLIGHT buys no safety while
    /// costing the whole difference between the two watermarks — which under
    /// load is an entire queue depth of output.
    ///
    /// Lives here rather than on `Run` for the same reason `durable_head` does:
    /// both are cloned into every handle for one Run's binding and die with it,
    /// so a rebind cannot inherit a stale watermark.
    offered_head: Arc<AtomicU64>,
    source_gap_after_byte: Arc<AtomicU64>,
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
    pub(crate) fn mark_source_gap(&self, cursor: Option<u64>) {
        self.source_gap_after_byte
            .store(cursor.unwrap_or(u64::MAX), Ordering::Release);
    }
    pub(crate) fn is_failed(&self) -> bool {
        self.persistence.is_failed()
    }
    pub(crate) fn load_terminal_checkpoint(&self, id: RunId) -> Option<StoredCheckpoint> {
        self.persistence.load_terminal_checkpoint(id)
    }

    /// Rare derived checkpoint shares the existing ordered actor; raw output is not copied.
    pub(crate) fn offer_terminal_checkpoint(&self, saved: StoredCheckpoint) -> bool {
        self.persistence
            .inner
            .sender
            .try_send(Command::TerminalCheckpoint {
                id: saved.checkpoint.run_id,
                saved: Some(saved),
                durable_head: Arc::clone(&self.durable_head),
                reply: None,
            })
            .is_ok()
    }

    pub(crate) fn save_terminal_checkpoint_for_handoff(
        &self,
        id: RunId,
        saved: Option<StoredCheckpoint>,
    ) -> Result<(), String> {
        let (tx, rx) = mpsc::sync_channel(0);
        self.persistence
            .inner
            .sender
            .send(Command::TerminalCheckpoint {
                id,
                saved,
                durable_head: Arc::clone(&self.durable_head),
                reply: Some(tx),
            })
            .map_err(|_| "terminal persistence actor stopped".to_owned())?;
        rx.recv()
            .map_err(|_| "terminal checkpoint receipt lost".to_owned())?
    }

    #[cfg(test)]
    pub(crate) fn durable_head(&self) -> u64 {
        self.durable_head.load(Ordering::Acquire)
    }

    pub(crate) fn durable_head_owner(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.durable_head)
    }

    /// The byte offset the next replay must start at.
    ///
    /// Normally this is the caller's own end-of-log: the previous append was
    /// accepted, so only the newest bytes are outstanding and the render is one
    /// chunk. After a refusal or a skip it stays where the last ACCEPTED append
    /// ended, so the replay carries the bytes the dropped append would have —
    /// which is the only thing that keeps a drop from becoming a forward gap.
    ///
    /// Unlike the `durable_head`-based catch-up this replaced, rendering has no
    /// side effect: the watermark advances only when `append` actually hands the
    /// bytes over. A render that is then refused, or discarded because the Run
    /// left `running`, simply never moved it, so there is no debt to re-arm and
    /// no way to consume one twice.
    pub(crate) async fn read_replay_page(
        &self,
        id: RunId,
        after: u64,
        through: u64,
    ) -> Result<OutputReplay, PersistenceError> {
        let (reply, result) = tokio::sync::oneshot::channel();
        // Async bounded admission: a stalled disk does not block a Tokio worker
        // or allocate one waiting OS thread per attachment.
        let mut command = Command::ReadReplay {
            id,
            after,
            through,
            reply,
        };
        loop {
            let room = self.persistence.inner.lifecycle_space.notified();
            tokio::pin!(room);
            room.as_mut().enable();
            match self.persistence.inner.lifecycle.try_send(command) {
                Ok(()) => break,
                Err(mpsc::TrySendError::Full(returned)) => {
                    command = returned;
                    room.await;
                }
                Err(mpsc::TrySendError::Disconnected(_)) => {
                    return Err(PersistenceError::ActorStopped);
                }
            }
        }
        let _ = self
            .persistence
            .inner
            .sender
            .try_send(Command::LifecycleWake);
        result.await.map_err(|_| PersistenceError::ActorStopped)?
    }

    pub(crate) fn next_replay_start(&self) -> u64 {
        self.offered_head.load(Ordering::Acquire)
    }

    pub(crate) fn metadata_bytes_owner(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.metadata_bytes)
    }

    /// Enqueue durable output for one Run, dropping the request rather than
    /// waiting when the actor is behind. Returns whether the request was
    /// accepted, so the caller knows which replay to render NEXT time.
    ///
    /// `replay` must cover every byte from the last ACCEPTED append onward. The
    /// cheap case is the delta for this push alone, and it is what the caller
    /// sends while this method keeps returning `true`: the actor stitches a
    /// chain of queued deltas together against `expected_heads`, a pending
    /// watermark that advances per queued append rather than per commit, so
    /// contiguity holds even when the queue is deep and nothing has been
    /// committed yet.
    ///
    /// The moment this returns `false` that chain is broken, and the caller MUST
    /// render the next replay from [`Self::next_replay_start`] instead. That
    /// catch-up is what makes dropping safe: the offered watermark advances only
    /// on acceptance, so a dropped append leaves it exactly where the last
    /// accepted one ended. Nothing is lost and no gap is ever observable.
    ///
    /// Catching up from `durable_head` instead — the COMMITTED watermark — is
    /// not a safe conservative choice, it is a fleet-scale wedge, and it was one
    /// in two compounding ways.
    ///
    /// The first is the render. `OutputLog::replay` copies every retained chunk
    /// above the start byte, so a catch-up from the committed watermark copies
    /// `retained - durable` bytes, bounded only by `OUTPUT_RETENTION_BYTES`,
    /// inline on the one thread described below. Measured on 512 Runs x 40
    /// chunks/s: admission stalled at 211 Runs with the owner thread at 97.4%
    /// USER time, against 512/512 admitted for the same load with persistence
    /// off.
    ///
    /// The second is what the oversized replay then did to the ACTOR, and it is
    /// the half that made the wedge a latch. Bytes between the committed and the
    /// offered watermark are already queued, so re-sending them produced a
    /// replay that OVERLAPPED the appends still in flight ahead of it. That
    /// overlap is fatal to throughput twice over: `append_batch_with_shutdown`
    /// re-splits by contiguity, so an overlapping replay is never
    /// `is_fresh_contiguous` and gets a transaction (and an fsync) entirely to
    /// itself instead of coalescing to `MAX_TRANSACTION_PAYLOAD_BYTES`; and
    /// every chunk of it that has since committed takes the verify-against-
    /// stored branch in `append_replay`, one indexed range lookup plus a bounded
    /// file read per chunk. Slower commits deepen the queue, a deeper queue widens the
    /// gap between the two watermarks, and a wider gap makes the next catch-up
    /// bigger — a loop with no exit, which is why a chatty Run cost seconds per
    /// lifecycle verb rather than a bounded penalty.
    ///
    /// Both halves have the same cure and it is this watermark: never re-send a
    /// byte the actor already holds.
    ///
    /// A blocking send here would stall the whole fleet. Every native Run's
    /// output is read by ONE daemon-wide thread (`native_runtime::owner_main`),
    /// which calls `Run::record_output` inline, which calls this. One slow fsync
    /// would therefore stop that thread from draining ANY pty — including
    /// memory-only Runs, which never reach this code but share the reader — so
    /// every child in the fleet would block writing into a full pty buffer.
    ///
    /// Dropping is deliberately *not* the same as losing durability: the actor
    /// already coalesces queued appends into one transaction up to
    /// `MAX_TRANSACTION_PAYLOAD_BYTES`, so a full queue means the next append
    /// commits more bytes per fsync. Pressure degrades into fewer, larger
    /// writes rather than into a stalled fleet.
    ///
    /// Sending a delta after a DROP would be worse than lossy. `append_replay`
    /// rejects a forward gap outright, and that error latches persistence off
    /// daemon-wide via `remember_failure` — so a drop would poison durability
    /// for every Run at exactly the moment the disk is under pressure. That is
    /// why the return value must be honoured rather than ignored.
    #[must_use = "a refused append leaves the offered watermark behind, which the next replay must start from"]
    pub(crate) fn append(&self, id: RunId, replay: OutputReplay) -> bool {
        // Record the offer's shape BEFORE anything can consume it, and before
        // the failure short-circuit, so a test sees exactly what the caller
        // chose to render.
        #[cfg(test)]
        {
            *mutex_lock(&self.persistence.inner.test_hooks.observed_append) =
                Some(ObservedAppend {
                    first_byte: replay.chunks.first().map(|chunk| chunk.start_byte),
                    payload_bytes: replay.chunks.iter().map(|chunk| chunk.data.len()).sum(),
                });
        }
        if mutex_lock(&self.persistence.inner.failure).is_some() {
            // A failed owner accepted no bytes. Callers inspect the explicit
            // failure latch to stop retries without pretending success.
            return false;
        }
        // The offset the actor will expect the NEXT append to start at, read
        // before the replay is moved into the message. This mirrors the actor's
        // own pending watermark exactly: `append_batch_with_shutdown` records
        // `expected_heads` from the last group's `latest_output_bytes`, so any
        // other choice here would re-introduce the overlap this watermark exists
        // to prevent.
        let offered_through = replay.latest_output_bytes;
        // `try_send` rather than `send`: see above. `Full` is absorbed by the
        // next replay, which starts from the still-unmoved offered watermark.
        // `Disconnected` means the actor is gone, which `failure` already owns —
        // leaving the watermark put is merely a wasted render on a dead path,
        // never a correctness problem.
        self.persistence
            .inner
            .queue_depth
            .fetch_add(1, Ordering::AcqRel);
        let accepted = self
            .persistence
            .inner
            .sender
            .try_send(Command::Append {
                id,
                replay,
                durable_head: Arc::clone(&self.durable_head),
            })
            .is_ok();
        if accepted {
            // Only acceptance moves the watermark. `fetch_max` rather than
            // `store` keeps it monotone on its own terms: every caller today
            // renders under `Run::persistence_transition` and so arrives in
            // order, but a watermark that could move BACKWARDS would turn the
            // next delta into a forward gap and latch persistence off
            // daemon-wide, which is too sharp an edge to leave resting on a
            // lock held in another module.
            self.offered_head
                .fetch_max(offered_through, Ordering::AcqRel);
        } else {
            self.persistence
                .inner
                .queue_depth
                .fetch_sub(1, Ordering::AcqRel);
            self.request_output_wake();
        }
        accepted
    }

    /// Whether the queue has visible room, checked BEFORE rendering a replay.
    ///
    /// Rendering is the expensive half of an append, so learning that the queue
    /// is full from a failed `try_send` is learning it one full render too late.
    /// Under sustained overload every push fails that way, and the reactor
    /// thread spends all its time building messages it immediately discards:
    /// that is why bounding the render size alone moved 512 x 40/s from 199 to
    /// only 325 admitted instead of curing it.
    ///
    /// This is advisory, not an admission decision. The counter can lag the
    /// actor in either direction, so `try_send` still decides — a false "has
    /// room" merely costs the render we would have paid anyway, and a false
    /// "full" skips one append whose bytes the next render still carries,
    /// because only acceptance moves the offered watermark.
    pub(crate) fn register_output_wake(&self, wake: crate::native_runtime::OwnerWake) {
        *mutex_lock(&self.persistence.inner.output_wake) = Some(wake);
    }

    pub(crate) fn request_output_wake(&self) {
        self.persistence
            .inner
            .output_wake_requested
            .store(true, Ordering::Release);
    }

    pub(crate) fn queue_has_room(&self) -> bool {
        if self.persistence.inner.queue_depth.load(Ordering::Acquire) < PERSISTENCE_QUEUE_CAPACITY {
            return true;
        }
        self.request_output_wake();
        self.persistence.inner.queue_depth.load(Ordering::Acquire) < PERSISTENCE_QUEUE_CAPACITY
    }

    /// Enqueue one final catch-up append, BLOCKING until the queue accepts it.
    ///
    /// The mirror image of [`Self::append`], for the one caller that has no
    /// next push to fall back on. `append` may drop, and `record_output` may
    /// skip the render outright, because the offered watermark stays put and
    /// *the next push re-offers the same bytes*. Exec-in-place is where that
    /// invariant runs out: extract stops each pty reader, so the push that was
    /// going to carry the debt never happens, and the barrier that follows
    /// fences only what was OFFERED — a skipped render is not outstanding work
    /// as far as the barrier can see, so it exec's over the top of it and those
    /// bytes are gone from both the queue and the kernel buffer.
    ///
    /// Blocking is affordable here precisely where dropping is not: this runs
    /// once per upgrade on the SIGHUP path, past the point of no return, with
    /// every reader already stopped. There is no pty left to starve — the whole
    /// reason `append` must never block — and the actor is draining a queue that
    /// nothing is adding to any more, so the wait is bounded by the depth
    /// already enqueued.
    #[must_use = "a failed final offer means the barrier cannot fence these bytes"]
    pub(crate) fn append_blocking(&self, id: RunId, replay: OutputReplay) -> bool {
        if mutex_lock(&self.persistence.inner.failure).is_some() {
            return false;
        }
        let offered_through = replay.latest_output_bytes;
        self.persistence
            .inner
            .queue_depth
            .fetch_add(1, Ordering::AcqRel);
        let accepted = self
            .persistence
            .inner
            .sender
            .send(Command::Append {
                id,
                replay,
                durable_head: Arc::clone(&self.durable_head),
            })
            .is_ok();
        if accepted {
            self.offered_head
                .fetch_max(offered_through, Ordering::AcqRel);
        } else {
            self.persistence
                .inner
                .queue_depth
                .fetch_sub(1, Ordering::AcqRel);
            self.request_output_wake();
        }
        accepted
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
        if !self.persistence.inner.send_lifecycle(Command::Finalize {
            id,
            actual_pid,
            replay,
            state,
            source_gap_after_byte: match self.source_gap_after_byte.load(Ordering::Acquire) {
                u64::MAX => None,
                cursor => Some(cursor),
            },
            durable_head: Arc::clone(&self.durable_head),
            metadata_bytes: Arc::clone(&self.metadata_bytes),
            reply: reply_tx,
        }) {
            return;
        }
        let _ = reply_rx.recv();
    }
}

impl Persistence {
    #[cfg(test)]
    pub(crate) fn force_append_storage_full(&self) {
        self.inner
            .test_hooks
            .force_append_storage_full
            .store(true, Ordering::Release);
    }

    pub(crate) fn cancel_storage_waits(&self) {
        self.inner.shutdown.store(true, Ordering::Release);
        let _ = self.inner.sender.try_send(Command::LifecycleWake);
    }

    pub(crate) fn resources(&self) -> ResourceLimits {
        self.inner.resources
    }

    pub(crate) fn open_with_resources(
        state_dir: PathBuf,
        resources: ResourceLimits,
        hint: Option<HandoffHint>,
    ) -> Result<(Self, Vec<RecoveredRun>), PersistenceError> {
        resources
            .validate()
            .map_err(PersistenceError::ResourcePressure)?;
        Self::open_with_admission_limits(state_dir, &resources.into(), hint)
    }

    fn load_terminal_checkpoint(&self, id: RunId) -> Option<StoredCheckpoint> {
        use std::io::Read as _;
        let path = terminal_checkpoint_path(&self.inner.state_dir, id);
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits().cast_signed())
            .open(path)
            .ok()?;
        let metadata = file.metadata().ok()?;
        if !metadata.is_file() || metadata.len() > MAX_CHECKPOINT_FILE_BYTES as u64 {
            return None;
        }
        let mut bytes = Vec::new();
        file.take(MAX_CHECKPOINT_FILE_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .ok()?;
        if bytes.len() > MAX_CHECKPOINT_FILE_BYTES {
            return None;
        }
        let saved: PersistedTerminalCheckpoint = serde_json::from_slice(&bytes).ok()?;
        if saved.epoch != self.inner.epoch {
            return None;
        }
        saved.state
    }

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
        // Deliberately NOT on the lifecycle lane. A barrier's entire meaning is
        // its FIFO position: it returns when every append enqueued before it has
        // committed. Overtaking those appends would make it return early and
        // tell `exec`-in-place that a replay is durable when it is not — the
        // replay gap this call exists to prevent. The lifecycle lane is for
        // commands that carry their own bytes; a barrier carries none and is
        // pure ordering.
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

    #[cfg(test)]
    pub(crate) fn open(
        state_dir: impl Into<PathBuf>,
    ) -> Result<(Self, Vec<RecoveredRun>), PersistenceError> {
        Self::open_with_admission_limits(state_dir.into(), &AdmissionLimits::OPERATIONAL, None)
    }

    /// Incoming-image startup seam for exec-in-place: reuse the handed-off epoch,
    /// exclude the live Run set from reconciliation, and adopt the inherited
    /// state-lock descriptor instead of re-locking. A12 calls this.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn open_with_handoff(
        state_dir: impl Into<PathBuf>,
        hint: HandoffHint,
    ) -> Result<(Self, Vec<RecoveredRun>), PersistenceError> {
        Self::open_with_admission_limits(
            state_dir.into(),
            &AdmissionLimits::OPERATIONAL,
            Some(hint),
        )
    }

    fn open_with_admission_limits(
        state_dir: PathBuf,
        admission_limits: &AdmissionLimits,
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
        admission_limits: &AdmissionLimits,
        handoff: Option<HandoffHint>,
        #[cfg(test)] test_hooks: Arc<PersistenceTestHooks>,
    ) -> Result<(Self, Vec<RecoveredRun>), PersistenceError> {
        // The original actor still owns its complete immutable Copy policy.
        let admission_limits = *admission_limits;
        let output_wake = Arc::new(Mutex::new(None));
        let actor_output_wake = Arc::clone(&output_wake);
        let output_wake_requested = Arc::new(AtomicBool::new(false));
        let actor_output_wake_requested = Arc::clone(&output_wake_requested);
        let lifecycle_space = Arc::new(tokio::sync::Notify::new());
        let actor_lifecycle_space = Arc::clone(&lifecycle_space);
        let (command_tx, command_rx) = mpsc::sync_channel(PERSISTENCE_QUEUE_CAPACITY);
        let (lifecycle_tx, lifecycle_rx) = mpsc::sync_channel(LIFECYCLE_QUEUE_CAPACITY);
        let (init_tx, init_rx) = mpsc::sync_channel(0);
        let queue_depth = Arc::new(AtomicUsize::new(0));
        let actor_queue_depth = Arc::clone(&queue_depth);
        let failure = Arc::new(Mutex::new(None));
        let actor_failure = Arc::clone(&failure);
        let shutdown = Arc::new(AtomicBool::new(false));
        let actor_shutdown = Arc::clone(&shutdown);
        #[cfg(test)]
        let actor_test_hooks = Arc::clone(&test_hooks);
        let actor_state_dir = state_dir.clone();
        let join = thread::Builder::new()
            .name("ctxmux-persistence".to_owned())
            .spawn(move || {
                actor_main(
                    &actor_state_dir,
                    &admission_limits,
                    handoff,
                    &command_rx,
                    &lifecycle_rx,
                    &actor_queue_depth,
                    &init_tx,
                    &actor_failure,
                    &actor_shutdown,
                    &actor_lifecycle_space,
                    &actor_output_wake,
                    &actor_output_wake_requested,
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
                resources: admission_limits.resources,
                lifecycle_space,
                output_wake,
                output_wake_requested,
                state_dir,
                sender: command_tx,
                lifecycle: lifecycle_tx,
                queue_depth,
                failure,
                shutdown,
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
        if !self
            .inner
            .send_lifecycle(Command::StageStart(Box::new(StageRequest {
                prepared: Box::new(prepared),
                candidates,
                receipt: receipt.clone(),
                ready: ready_tx,
                decision: decision_rx,
                completion: completion_tx,
            })))
        {
            let _ = receipt.decide(StartDisposition::NotCommitted);
            return Err(PersistentStartFailure::new(
                StartDisposition::NotCommitted,
                PersistenceError::ActorStopped,
            ));
        }
        match ready_rx.recv() {
            Ok(Ok(())) => Ok(StagedPersistentStart {
                durable: Some(PersistentRun {
                    persistence: self.clone(),
                    durable_head: Arc::new(AtomicU64::new(0)),
                    metadata_bytes: Arc::new(AtomicU64::new(metadata_bytes)),
                    // A fresh Run starts at byte 0 with an empty log, so its
                    // first delta already begins at the watermark and nothing
                    // is outstanding.
                    offered_head: Arc::new(AtomicU64::new(0)),
                    source_gap_after_byte: Arc::new(AtomicU64::new(u64::MAX)),
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

    /// Delete one exact terminal Run row and its cascading replay in a single
    /// bounded transaction, reusing the same spill-disabled page-charge
    /// admission the exact-replacement path proves. The actor is the sole store
    /// owner, so this shares the FIFO ordering of every other durable mutation.
    pub(crate) fn remove_terminal(&self, candidate: PersistentCandidate) -> RemovalDisposition {
        if let Some(message) = mutex_lock(&self.inner.failure).clone() {
            return RemovalDisposition::NotRemoved(PersistenceError::Mutation(message));
        }
        let (reply_tx, reply_rx) = mpsc::sync_channel(0);
        if !self.inner.send_lifecycle(Command::RemoveTerminal {
            candidate,
            reply: reply_tx,
        }) {
            return RemovalDisposition::NotRemoved(PersistenceError::ActorStopped);
        }
        reply_rx.recv().unwrap_or(RemovalDisposition::NotRemoved(
            PersistenceError::ActorStopped,
        ))
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
    pub(crate) fn pause_next_append(&self) -> (mpsc::Receiver<()>, mpsc::SyncSender<()>) {
        let (reached_tx, reached_rx) = mpsc::sync_channel(0);
        let (release_tx, release_rx) = mpsc::sync_channel(0);
        let previous =
            mutex_lock(&self.inner.test_hooks.append_barrier).replace(FinalizeTestBarrier {
                reached: reached_tx,
                release: release_rx,
            });
        assert!(previous.is_none(), "only one append barrier may be armed");
        (reached_rx, release_tx)
    }

    /// Clear any recorded offer and return a handle that reads the next one.
    #[cfg(test)]
    pub(crate) fn capture_next_append_payload(&self) -> AppendObserver {
        *mutex_lock(&self.inner.test_hooks.observed_append) = None;
        AppendObserver {
            persistence: self.clone(),
        }
    }

    #[cfg(test)]
    fn fail_next_append_as_disk_full(&self) {
        assert!(
            !self
                .inner
                .test_hooks
                .fail_next_append_as_disk_full
                .swap(true, Ordering::AcqRel),
            "only one append DiskFull fixture may be armed"
        );
    }

    #[cfg(test)]
    pub(crate) fn fail_next_append_as_io_error(&self) {
        assert!(
            !self
                .inner
                .test_hooks
                .fail_next_append_as_io_error
                .swap(true, Ordering::AcqRel),
            "only one append SQLITE_IOERR fixture may be armed"
        );
    }

    #[cfg(test)]
    fn fail_next_finalize_as_disk_full(&self) {
        assert!(
            !self
                .inner
                .test_hooks
                .fail_next_finalize_as_disk_full
                .swap(true, Ordering::AcqRel),
            "only one finalize DiskFull fixture may be armed"
        );
    }

    /// Arm one append to fail with a raw `SQLITE_IOERR_*`, the shape a full
    /// filesystem produces when the failing write is to the WAL rather than to
    /// the database file.
    #[cfg(test)]
    fn fail_next_append_as_io_failure(&self, extended_code: i32) {
        assert_ne!(extended_code, 0, "an injected I/O failure needs a code");
        assert_eq!(
            self.inner
                .test_hooks
                .fail_next_append_as_io_failure
                .swap(extended_code, Ordering::AcqRel),
            0,
            "only one append I/O-failure fixture may be armed"
        );
    }

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
            &AdmissionLimits {
                run_records,
                metadata_bytes,
                resources: ResourceLimits::DEFAULT,
            },
            None,
        )
    }

    pub(crate) fn recovered_run(&self, durable_head: u64, metadata_bytes: u64) -> PersistentRun {
        PersistentRun {
            persistence: self.clone(),
            durable_head: Arc::new(AtomicU64::new(durable_head)),
            metadata_bytes: Arc::new(AtomicU64::new(metadata_bytes)),
            // A recovered Run has offered the actor nothing, so its watermark
            // starts at the recovered commit point. Its in-memory log may
            // already hold bytes above that (a rebind after the actor was
            // replaced, say), and those bytes ARE still outstanding — starting
            // here is what makes the first push carry them instead of declaring
            // them durable when they are not.
            offered_head: Arc::new(AtomicU64::new(durable_head)),
            source_gap_after_byte: Arc::new(AtomicU64::new(u64::MAX)),
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
    ReadReplay {
        id: RunId,
        after: u64,
        through: u64,
        reply: tokio::sync::oneshot::Sender<Result<OutputReplay, PersistenceError>>,
    },
    TerminalCheckpoint {
        id: RunId,
        saved: Option<StoredCheckpoint>,
        durable_head: Arc<AtomicU64>,
        reply: Option<mpsc::SyncSender<Result<(), String>>>,
    },
    StageStart(Box<StageRequest>),
    RemoveTerminal {
        candidate: PersistentCandidate,
        reply: mpsc::SyncSender<RemovalDisposition>,
    },
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
        source_gap_after_byte: Option<u64>,
        durable_head: Arc<AtomicU64>,
        metadata_bytes: Arc<AtomicU64>,
        reply: mpsc::SyncSender<Result<(), PersistenceError>>,
    },
    Barrier {
        reply: mpsc::SyncSender<()>,
    },
    /// Sent on the append channel purely to break the actor out of a blocking
    /// `recv()` when a lifecycle command arrives on the other channel. Carries
    /// nothing; the loop re-checks the lifecycle channel on its next turn.
    LifecycleWake,
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

/// Fold the WAL while the actor's queue is empty, so a later lifecycle verb
/// finds a small baseline instead of having to create one.
///
/// Returns whether a checkpoint actually ran, which is what the idle-fold tests
/// assert on: an idle daemon must fold at most once and then stay quiet, or the
/// 0.000% idle CPU this project already won would regress.
fn idle_fold_wal(store: &StateStore, shutdown: &AtomicBool) -> bool {
    if shutdown.load(Ordering::Acquire) {
        return false;
    }
    #[cfg(test)]
    if store.test_hooks.suppress_idle_fold.load(Ordering::Acquire) {
        return false;
    }
    // Folding is only worth doing once the WAL is big enough to be worth the
    // reset it causes. Below the floor, leaving the bytes alone is cheaper for
    // everyone: see `WAL_IDLE_FOLD_FLOOR_BYTES`.
    if !matches!(file_len(&store.wal_path), Ok(bytes) if bytes >= WAL_IDLE_FOLD_FLOOR_BYTES) {
        return false;
    }
    store.try_fold_wal_once()
}

#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "one FIFO actor entry keeps handoff, retry, failure, barrier, and shutdown owners explicit"
)]
fn actor_main(
    state_dir: &Path,
    admission_limits: &AdmissionLimits,
    handoff: Option<HandoffHint>,
    receiver: &mpsc::Receiver<Command>,
    lifecycle_rx: &mpsc::Receiver<Command>,
    queue_depth: &AtomicUsize,
    init: &mpsc::SyncSender<ActorInit>,
    failure: &Mutex<Option<String>>,
    shutdown: &AtomicBool,
    lifecycle_space: &tokio::sync::Notify,
    output_wake: &Mutex<Option<crate::native_runtime::OwnerWake>>,
    output_wake_requested: &AtomicBool,
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
    let mut pending_lifecycle = None;
    loop {
        // Lifecycle first, at every dequeue. This is the whole of R25: the
        // command was never short of a slot (measured: 1 us blocked at
        // admission), it was standing behind up to a full depth of appends
        // (measured: 93.0 ms of a 94.9 ms `finalize` at eight chatty Runs).
        //
        // Checking here rather than reordering inside a batch is what keeps the
        // R21 latch intact: lifecycle commands stay FIFO among themselves, so
        // the lifecycle-vs-lifecycle edge that a `StageStart` must not cross —
        // it may not overtake the `Finalize` of a candidate it evicts — is
        // preserved by the single channel's own ordering.
        let mut command = match pending_lifecycle
            .take()
            .or_else(|| lifecycle_rx.try_recv().ok())
        {
            Some(command) => command,
            None => {
                match pending.pop_front() {
                    Some(command) => command,
                    None => match receiver.try_recv() {
                        Ok(command) => command,
                        Err(mpsc::TryRecvError::Disconnected) => return,
                        Err(mpsc::TryRecvError::Empty) => {
                            // Nothing is queued, so nothing is waiting on this thread.
                            // Fold the WAL now, while the cost is nobody's latency, so
                            // a later lifecycle verb inherits a small baseline rather
                            // than an 8 MiB one. Measured on cn3 (Linux,
                            // synchronous=FULL): folding costs ~1.6 ms/MiB, linear to
                            // the 8 MiB admission ceiling (12.6 ms there), against
                            // 0.016 ms for the same call on an already-zero WAL.
                            //
                            // This is an optimization only, and a weak one under load:
                            // a chatty fleet's queue never empties, so this never fires
                            // exactly when the WAL is largest. `fold_wal_below_ceiling`
                            // is what actually bounds the baseline on the paths that
                            // must prove a charge.
                            //
                            // Errors are deliberately dropped rather than latched: an
                            // idle fold has no receipt to fail and no caller to inform,
                            // and every path that depends on a bounded WAL still folds
                            // and still proves its charge. A failure here costs only
                            // the optimization.
                            if mutex_lock(failure).is_none() && store.compaction_pending {
                                match store.compact_replay_step() {
                                    Ok(()) => continue,
                                    Err(error) if error.is_transient_storage() => {
                                        if wait_for_shutdown(shutdown, STORAGE_RETRY_INTERVAL) {
                                            return;
                                        }
                                        continue;
                                    }
                                    Err(error) => remember_failure(failure, &error),
                                }
                            }
                            if mutex_lock(failure).is_none() {
                                idle_fold_wal(&store, shutdown);
                            }
                            // Block on the append channel alone. A lifecycle
                            // sender that arrives while this thread is parked
                            // pushes a `LifecycleWake` here to break it out —
                            // and if that push is refused because the append
                            // channel is full, the thread is not parked, so the
                            // loop's own next turn finds the command. Polling
                            // both channels instead would cost idle wakeups,
                            // which this daemon spent a round driving to zero.
                            match receiver.recv() {
                                Ok(command) => command,
                                Err(_) => return,
                            }
                        }
                    },
                }
            }
        };
        // Settle this Run's accepted prefix before its terminal commit. The
        // final replay may still contain those bytes even after disk retention
        // retires their old prefix; letting Finalize overtake the accepted
        // appends would make them verify history the store itself has evicted.
        // Only this Run's queued appends are selected. Other Runs retain their
        // queue order, and Finalize supplies any tail not already accepted.
        if let Command::Finalize { id, replay, .. } = &command
            && mutex_lock(failure).is_none()
            && read_run_head(&store.connection, *id)
                .is_ok_and(|head| head < replay.latest_output_bytes)
        {
            let required_id = *id;
            pending_lifecycle = Some(command);
            command =
                take_required_append(required_id, &mut pending, receiver).unwrap_or_else(|| {
                    pending_lifecycle
                        .take()
                        .expect("pending Finalize is retained")
                });
        }
        if output_wake_requested.swap(false, Ordering::AcqRel)
            && let Some(wake) = &*mutex_lock(output_wake)
        {
            wake.wake();
        }
        lifecycle_space.notify_waiters();
        match command {
            Command::ReadReplay {
                id,
                after,
                through,
                reply,
            } => {
                let _ = reply.send(load_replay_page(
                    &store.connection,
                    &store.replay_dir,
                    id,
                    after,
                    through,
                ));
            }
            // The wake token carries nothing and needs no handling: its only
            // job was to break the actor out of a blocking `recv()`, and by the
            // time this arm runs the lifecycle check at the top of the loop has
            // already had its turn. Falling through to the next iteration is
            // the whole behaviour.
            Command::LifecycleWake => {}
            Command::StageStart(request) => {
                // Fail closed before touching the store: a latched fatal
                // failure must reject the mutation without driving any
                // database work, otherwise the rejected start could still
                // commit durably.
                if let Some(message) = mutex_lock(failure).clone() {
                    let _ = request.receipt.decide(StartDisposition::NotCommitted);
                    let _ = request.ready.send(Err(StageFailure {
                        error: PersistenceError::Mutation(message),
                        fatal: true,
                        capacity: false,
                    }));
                    continue;
                }
                let result = store.drive_staged_start_with_shutdown(
                    &request.prepared,
                    &request.candidates,
                    &request.receipt,
                    &request.ready,
                    &request.decision,
                    Some(shutdown),
                );
                let Ok(result) = result else {
                    // A shutdown while waiting for a transient checkpoint
                    // conflict must not be latched as durable corruption.
                    return;
                };
                handle_staged_start_result(&request, result, failure);
                for candidate in &request.candidates {
                    if store.connection.query_row(
                        "SELECT EXISTS(SELECT 1 FROM runs WHERE id=?1)",
                        [candidate.id.to_string()],
                        |row| row.get::<_, bool>(0),
                    ) == Ok(false)
                    {
                        let _ = fs::remove_file(terminal_checkpoint_path(state_dir, candidate.id));
                    }
                }
            }
            Command::RemoveTerminal { candidate, reply } => {
                // Fail closed before touching the store, mirroring StageStart: a
                // latched fatal failure rejects the removal without database work.
                let disposition = if let Some(message) = mutex_lock(failure).clone() {
                    RemovalDisposition::NotRemoved(PersistenceError::Mutation(message))
                } else {
                    match store.remove_terminal_with_shutdown(&candidate, Some(shutdown)) {
                        Some(disposition) => {
                            if let RemovalDisposition::Unknown(error) = &disposition {
                                remember_failure(failure, error);
                            }
                            disposition
                        }
                        None => return,
                    }
                };
                if matches!(&disposition, RemovalDisposition::Removed) {
                    let _ = fs::remove_file(terminal_checkpoint_path(state_dir, candidate.id));
                }
                let _ = reply.send(disposition);
            }
            Command::Append {
                id,
                replay,
                durable_head,
            } => {
                #[cfg(test)]
                pause_before_append(test_hooks);
                // One `Append` has left the channel. Decrement here rather than
                // at the two dequeue sites above so the counter tracks appends
                // only — a `StageStart` or `RemoveTerminal` never incremented it.
                queue_depth.fetch_sub(1, Ordering::AcqRel);
                let mut batch = vec![(id, replay, durable_head)];
                let mut payload = replay_payload(&batch[0].1);
                let window = APPEND_BATCH_WINDOW;
                #[cfg(test)]
                let window = mutex_lock(&test_hooks.append_batch_window).unwrap_or(window);
                let deadline = Instant::now() + window;
                while payload < MAX_TRANSACTION_PAYLOAD_BYTES {
                    // A lifecycle wake can be refused while the append queue
                    // is full. Check the lifecycle lane before each dequeue so
                    // draining that queue cannot turn into a timed wait with a
                    // lifecycle receipt already pending. Preserve this command
                    // ahead of newer lifecycle commands on the next actor turn.
                    if pending_lifecycle.is_none()
                        && let Ok(command) = lifecycle_rx.try_recv()
                    {
                        pending_lifecycle = Some(command);
                        break;
                    }
                    // A pending finalizer needs only its already accepted
                    // prefix. Gather that Run without a timed wait or another
                    // Run's backlog; a refused wake must not restart the batch
                    // timer with lifecycle work already waiting.
                    let next = if let Some(Command::Finalize { id, .. }) = &pending_lifecycle {
                        if *id != batch[0].0 {
                            break;
                        }
                        match take_required_append(*id, &mut pending, receiver) {
                            Some(command) => Ok(command),
                            None => break,
                        }
                    } else if pending_lifecycle.is_some() {
                        break;
                    } else {
                        let Some(remaining) = deadline.checked_duration_since(Instant::now())
                        else {
                            break;
                        };
                        // Collection owns no SQLite transaction or replay writer.
                        // Lifecycle senders also enqueue a wake here, so their
                        // receipts need not wait for the deadline to expire.
                        match pending.pop_front().map_or_else(|| receiver.try_recv(), Ok) {
                            Ok(command) => Ok(command),
                            Err(mpsc::TryRecvError::Disconnected) => {
                                Err(mpsc::RecvTimeoutError::Disconnected)
                            }
                            Err(mpsc::TryRecvError::Empty) => {
                                #[cfg(test)]
                                if let Some(notify) =
                                    mutex_lock(&test_hooks.append_wait_started).take()
                                {
                                    let _ = notify.send(());
                                }
                                receiver.recv_timeout(remaining)
                            }
                        }
                    };
                    match next {
                        Ok(Command::Append {
                            id,
                            replay,
                            durable_head,
                        }) if payload.saturating_add(replay_payload(&replay))
                            <= MAX_TRANSACTION_PAYLOAD_BYTES =>
                        {
                            queue_depth.fetch_sub(1, Ordering::AcqRel);
                            payload = payload.saturating_add(replay_payload(&replay));
                            batch.push((id, replay, durable_head));
                        }
                        Ok(command) => {
                            pending.push_front(command);
                            break;
                        }
                        Err(
                            mpsc::RecvTimeoutError::Timeout | mpsc::RecvTimeoutError::Disconnected,
                        ) => {
                            break;
                        }
                    }
                }
                if mutex_lock(failure).is_none() {
                    let result = retry_transient_storage(shutdown, || {
                        #[cfg(test)]
                        if let Some(error) = injected_append_failure(test_hooks) {
                            return Err(error);
                        }
                        store.append_batch_with_shutdown(&batch, Some(shutdown))
                    });
                    match result {
                        Some(Err(PersistenceError::ActorStopped))
                            if shutdown.load(Ordering::Acquire) =>
                        {
                            return;
                        }
                        Some(Err(error)) => remember_failure(failure, &error),
                        Some(Ok(())) => {}
                        None => return,
                    }
                }
            }
            Command::Finalize {
                id,
                actual_pid,
                replay,
                state,
                source_gap_after_byte,
                durable_head,
                metadata_bytes,
                reply,
            } => {
                #[cfg(test)]
                pause_before_finalize(test_hooks);
                let result = if let Some(message) = mutex_lock(failure).clone() {
                    Some(Err(PersistenceError::Mutation(message)))
                } else {
                    retry_transient_storage(shutdown, || {
                        #[cfg(test)]
                        if test_hooks
                            .fail_next_finalize_as_disk_full
                            .swap(false, Ordering::AcqRel)
                        {
                            return Err(PersistenceError::injected_disk_full());
                        }
                        store.finalize_with_shutdown(
                            id,
                            actual_pid,
                            &replay,
                            &state,
                            &durable_head,
                            &metadata_bytes,
                            source_gap_after_byte,
                            Some(shutdown),
                        )
                    })
                };
                let Some(result) = result else {
                    return;
                };
                if matches!(
                    &result,
                    Err(PersistenceError::ActorStopped)
                        if shutdown.load(Ordering::Acquire)
                ) {
                    return;
                }
                if let Err(error) = &result {
                    remember_failure(failure, error);
                }
                let _ = reply.send(result);
            }
            Command::TerminalCheckpoint {
                id,
                saved,
                durable_head,
                reply,
            } => {
                let exists = store
                    .connection
                    .query_row(
                        "SELECT EXISTS(SELECT 1 FROM runs WHERE id=?1)",
                        [id.to_string()],
                        |row| row.get::<_, bool>(0),
                    )
                    .unwrap_or(false);
                let latest_fence = saved.as_ref().map_or(0, |s| {
                    s.resizes
                        .last()
                        .map_or(s.checkpoint.through_byte, |r| r.through_byte)
                });
                let result = if !exists || latest_fence > durable_head.load(Ordering::Acquire) {
                    Err("checkpoint does not have a committed original-output fence".to_owned())
                } else {
                    write_terminal_checkpoint(
                        state_dir,
                        id,
                        &PersistedTerminalCheckpoint {
                            epoch: store.epoch.clone(),
                            state: saved,
                        },
                    )
                    .map_err(|error| error.to_string())
                };
                if let Err(error) = &result {
                    let _ = crate::diagnostics::record(format_args!(
                        "ctxmux terminal checkpoint for Run {id} was not saved: {error}"
                    ));
                }
                if let Some(reply) = reply {
                    let _ = reply.send(result);
                }
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
        if output_wake_requested.swap(false, Ordering::AcqRel)
            && let Some(wake) = &*mutex_lock(output_wake)
        {
            wake.wake();
        }
    }
}

fn take_required_append(
    required_id: RunId,
    pending: &mut VecDeque<Command>,
    receiver: &mpsc::Receiver<Command>,
) -> Option<Command> {
    if let Some(index) = pending
        .iter()
        .position(|command| matches!(command, Command::Append { id, .. } if *id == required_id))
    {
        return pending.remove(index);
    }
    while let Ok(command) = receiver.try_recv() {
        if matches!(&command, Command::Append { id, .. } if *id == required_id) {
            return Some(command);
        }
        pending.push_back(command);
    }
    None
}

fn retry_transient_storage(
    shutdown: &AtomicBool,
    mut mutation: impl FnMut() -> Result<(), PersistenceError>,
) -> Option<Result<(), PersistenceError>> {
    loop {
        if shutdown.load(Ordering::Acquire) {
            return None;
        }
        match mutation() {
            Err(error) if error.is_transient_storage() => {
                if wait_for_shutdown(shutdown, STORAGE_RETRY_INTERVAL) {
                    return None;
                }
            }
            result => return Some(result),
        }
    }
}

/// Retry the one `SQLite` operation whose normal result explicitly reports a
/// reader conflict: `wal_checkpoint(TRUNCATE)`. `SQLite` returns `(busy, log,
/// checkpointed)` for this case rather than a typed error, so treating a
/// non-zero `busy` value as a permanent mutation failure turns ordinary client
/// observation into a latched daemon outage. The retry is deliberately local,
/// bounded, and cancellation-aware; all other database errors remain
/// fail-closed at the caller.
fn retry_wal_checkpoint(
    shutdown: Option<&AtomicBool>,
    mut checkpoint: impl FnMut() -> Result<(i64, i64, i64), PersistenceError>,
    mut wal_len: impl FnMut() -> Result<u64, PersistenceError>,
) -> Result<(), PersistenceError> {
    let mut backoff = WAL_CHECKPOINT_INITIAL_BACKOFF;
    for attempt in 0..=WAL_CHECKPOINT_MAX_RETRIES {
        if shutdown.is_some_and(|flag| flag.load(Ordering::Acquire)) {
            return Err(PersistenceError::ActorStopped);
        }

        let (busy, log_frames, checkpointed_frames) = match checkpoint() {
            Ok(result) => result,
            Err(error) if error.is_database_busy() => {
                if attempt == WAL_CHECKPOINT_MAX_RETRIES {
                    return Err(wal_checkpoint_retry_exhausted(
                        attempt + 1,
                        Some(error.to_string()),
                    ));
                }
                if wait_for_optional_shutdown(shutdown, backoff) {
                    return Err(PersistenceError::ActorStopped);
                }
                backoff = next_wal_checkpoint_backoff(backoff);
                continue;
            }
            Err(error) => return Err(error),
        };
        let bytes = wal_len()?;
        if busy == 0 && bytes == 0 {
            return Ok(());
        }
        if attempt == WAL_CHECKPOINT_MAX_RETRIES {
            return Err(wal_checkpoint_retry_exhausted(
                attempt + 1,
                Some(format!(
                    "busy={busy}, log_frames={log_frames}, \
                     checkpointed_frames={checkpointed_frames}, wal_bytes={bytes}"
                )),
            ));
        }
        if wait_for_optional_shutdown(shutdown, backoff) {
            return Err(PersistenceError::ActorStopped);
        }
        backoff = next_wal_checkpoint_backoff(backoff);
    }
    unreachable!("the bounded WAL checkpoint retry loop always returns")
}

fn wal_checkpoint_retry_exhausted(attempts: usize, detail: Option<String>) -> PersistenceError {
    PersistenceError::WalCheckpointBusy {
        attempts,
        detail: detail.unwrap_or_else(|| "checkpoint remained busy".to_owned()),
    }
}

fn next_wal_checkpoint_backoff(current: Duration) -> Duration {
    current
        .checked_mul(2)
        .unwrap_or(WAL_CHECKPOINT_MAX_BACKOFF)
        .min(WAL_CHECKPOINT_MAX_BACKOFF)
}

fn wait_for_optional_shutdown(shutdown: Option<&AtomicBool>, duration: Duration) -> bool {
    if let Some(flag) = shutdown {
        wait_for_shutdown(flag, duration)
    } else {
        thread::sleep(duration);
        false
    }
}

fn wait_for_shutdown(shutdown: &AtomicBool, duration: Duration) -> bool {
    const POLL_INTERVAL: Duration = Duration::from_millis(10);
    let mut remaining = duration;
    while !remaining.is_zero() {
        if shutdown.load(Ordering::Acquire) {
            return true;
        }
        let interval = remaining.min(POLL_INTERVAL);
        thread::sleep(interval);
        remaining = remaining.saturating_sub(interval);
    }
    shutdown.load(Ordering::Acquire)
}

#[cfg(test)]
fn pause_before_finalize(test_hooks: &PersistenceTestHooks) {
    let barrier = mutex_lock(&test_hooks.finalize_barrier).take();
    if let Some(barrier) = barrier {
        let _ = barrier.reached.send(());
        let _ = barrier.release.recv();
    }
}

#[cfg(test)]
fn pause_before_append(test_hooks: &PersistenceTestHooks) {
    let barrier = mutex_lock(&test_hooks.append_barrier).take();
    if let Some(barrier) = barrier {
        let _ = barrier.reached.send(());
        let _ = barrier.release.recv();
    }
}

/// Consume whichever append-failure hook is armed and return the error to inject
/// once, in the same precedence the actor's hot path checked inline: disk-full,
/// then a generic I/O error, then an extended-code I/O failure. Each `swap`
/// disarms the hook, so at most one fires per offered append.
#[cfg(test)]
fn injected_append_failure(test_hooks: &PersistenceTestHooks) -> Option<PersistenceError> {
    if test_hooks.force_append_storage_full.load(Ordering::Acquire) {
        if let Some(marker) = std::env::var_os("CTXMUX_TEST_UPGRADE_RETRY_MARKER") {
            fs::write(marker, b"actual accepted append retry")
                .expect("record actual private storage retry");
        }
        return Some(PersistenceError::injected_disk_full());
    }
    if test_hooks
        .fail_next_append_as_disk_full
        .swap(false, Ordering::AcqRel)
    {
        return Some(PersistenceError::injected_disk_full());
    }
    if test_hooks
        .fail_next_append_as_io_error
        .swap(false, Ordering::AcqRel)
    {
        return Some(PersistenceError::injected_io_error());
    }
    let injected = test_hooks
        .fail_next_append_as_io_failure
        .swap(0, Ordering::AcqRel);
    (injected != 0).then(|| PersistenceError::injected_io_failure(injected))
}

fn handle_staged_start_result(
    request: &StageRequest,
    result: StageDriveResult,
    failure: &Mutex<Option<String>>,
) {
    match result {
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
            Self::PublishEpoch => 1,
        }
    }

    fn prefix(self, len: usize) -> Self {
        match self {
            Self::Reconcile(rows) => Self::Reconcile(&rows[..len]),
            Self::PublishEpoch => Self::PublishEpoch,
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Reconcile(_) => "running reconciliation",
            Self::PublishEpoch => "epoch publication",
        }
    }
}

struct StateStore {
    state_dir: PathBuf,
    replay_dir: PathBuf,
    replay_file: String,
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
    compaction_pending: bool,
    compaction_sources_pending: VecDeque<String>,
    compaction_cursor: i64,
    replay_cleanup_needed: bool,
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
            let _ = crate::diagnostics::record(format_args!(
                "ctxmuxd failed to release its state lock: {error}"
            ));
        }
    }
}

type TerminalSettlement<'a> = (RunId, u32, &'a RunState, &'a Arc<AtomicU64>, Option<u64>);

impl StateStore {
    fn replay_compaction_trigger_bytes(&self) -> u64 {
        #[cfg(test)]
        {
            (self.admission_limits.resources.durable_replay_bytes * 2)
                .min(TEST_REPLAY_COMPACTION_TRIGGER_BYTES)
        }
        #[cfg(not(test))]
        {
            self.admission_limits.resources.durable_replay_bytes * 2
        }
    }

    // `_state_lock` is underscore-prefixed to document that it is held for its
    // Drop side effect (releasing the flock); reading its raw fd for the
    // exec-in-place handoff is a deliberate, narrow exception.
    #[allow(clippy::used_underscore_binding)]
    fn state_lock_raw_fd(&self) -> RawFd {
        self._state_lock.as_raw_fd()
    }

    #[allow(clippy::too_many_lines)]
    fn open(
        state_dir: &Path,
        admission_limits: &AdmissionLimits,
        mut handoff: Option<HandoffHint>,
        #[cfg(test)] test_hooks: Arc<PersistenceTestHooks>,
    ) -> Result<(Self, Vec<RecoveredRun>), PersistenceError> {
        prepare_state_dir(state_dir)?;
        let replay_dir = state_dir.join(REPLAY_DIR);
        prepare_replay_dir(&replay_dir)?;
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
        let (runtime_id, replay_file) = if database_existed {
            // Fence the format before querying columns owned by that format.
            // A valid older store is unsupported, not a missing-column failure.
            let runtime_id = validate_existing_schema(&connection)?;
            (runtime_id, read_replay_file_name(&connection)?)
        } else {
            let replay_file = format!("replay-{}.bin", Uuid::new_v4());
            (
                create_schema(&connection, &epoch, &replay_file)?,
                replay_file,
            )
        };
        validate_replay_file_name(&replay_file)?;
        let replay_path = replay_dir.join(&replay_file);
        let replay_file_created = if replay_path.exists() {
            false
        } else {
            OpenOptions::new()
                .create_new(true)
                .write(true)
                .mode(0o600)
                .open(&replay_path)
                .map_err(|source| PersistenceError::io(&replay_path, source))?;
            true
        };
        if replay_file_created {
            sync_directory(&replay_dir)?;
        }
        validate_state_file(&replay_path)?;
        connection
            .pragma_update(
                None,
                "max_page_count",
                i64::try_from(admission_limits.resources.database_bytes / PAGE_SIZE_BYTES)
                    .expect("database page limit fits SQLite"),
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
        validate_application_state(&connection, &replay_dir, &replay_file)?;

        let compaction_pending = connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM replay_chunks WHERE data_file != ?1)",
                [&replay_file],
                |row| row.get(0),
            )
            .map_err(PersistenceError::database)?;
        let mut store = Self {
            state_dir: state_dir.to_path_buf(),
            replay_dir,
            replay_file,
            database_path,
            wal_path,
            shm_path,
            connection,
            runtime_id,
            epoch,
            live_set,
            admission_limits: *admission_limits,
            compaction_pending,
            compaction_sources_pending: VecDeque::new(),
            compaction_cursor: i64::MIN,
            replay_cleanup_needed: false,
            _state_lock: state_lock,
            #[cfg(test)]
            test_hooks,
        };
        store
            .connection
            .execute_batch("CREATE TEMP TABLE handed_live (id TEXT PRIMARY KEY) WITHOUT ROWID")
            .map_err(PersistenceError::database)?;
        {
            let mut insert = store
                .connection
                .prepare("INSERT INTO handed_live VALUES (?1)")
                .map_err(PersistenceError::database)?;
            for id in &store.live_set {
                insert
                    .execute([id.to_string()])
                    .map_err(PersistenceError::database)?;
            }
        }
        store.validate_recovery_policy()?;
        store.replay_cleanup_needed = !store.normalize_replay_files()?;
        // A valid WAL left by the old unbounded maintenance path can exceed
        // today's write window. Recover/checkpoint it after integrity checks
        // instead of treating its size alone as damaged database content.
        store.fold_wal_below_ceiling(None)?;
        store.maybe_compact_replay()?;
        validate_physical_limits(
            state_dir,
            &store.replay_dir,
            &store.database_path,
            &store.wal_path,
            &store.shm_path,
            admission_limits.resources,
        )?;
        store.normalize_startup()?;
        validate_application_state(&store.connection, &store.replay_dir, &store.replay_file)?;
        store.validate_operational_state()?;
        let recovered = load_recovered_bounded(
            &store.connection,
            &store.replay_dir,
            store.admission_limits.resources,
        )?;
        store.validate_files()?;
        Ok((store, recovered))
    }

    fn validate_recovery_policy(&self) -> Result<(), PersistenceError> {
        let (records, metadata, replay, per_run): (i64, i64, i64, i64) = self.connection.query_row(
            "SELECT count(*), coalesce(sum(metadata_bytes), 0), coalesce(sum(replay_bytes), 0), coalesce(max(replay_bytes), 0) FROM runs", [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        ).map_err(PersistenceError::database)?;
        let mut resident_specs = 0_u64;
        let mut statement = self
            .connection
            .prepare("SELECT id, spec_json FROM runs")
            .map_err(PersistenceError::database)?;
        let mut rows = statement.query([]).map_err(PersistenceError::database)?;
        while let Some(row) = rows.next().map_err(PersistenceError::database)? {
            let id: String = row.get(0).map_err(PersistenceError::database)?;
            let spec_json: String = row.get(1).map_err(PersistenceError::database)?;
            let spec = decode_native_spec(
                id.parse().map_err(PersistenceError::database_message)?,
                &spec_json,
            )?;
            resident_specs = resident_specs.saturating_add(crate::resident_spec_bytes(&spec));
        }
        for (name, actual, limit) in [
            (
                "retained_runs",
                nonnegative_u64(records, "record count")?,
                self.admission_limits.run_records,
            ),
            (
                "metadata_bytes",
                nonnegative_u64(metadata, "metadata bytes")?
                    .saturating_add(resident_specs)
                    .saturating_add(
                        nonnegative_u64(records, "record count")?
                            .saturating_mul(crate::resident_run_owner_bytes()),
                    ),
                self.admission_limits.metadata_bytes,
            ),
            (
                "durable_replay_bytes",
                nonnegative_u64(replay, "replay bytes")?,
                self.admission_limits.resources.durable_replay_bytes,
            ),
            (
                "durable_run_output_bytes",
                nonnegative_u64(per_run, "per Run replay")?,
                self.admission_limits.resources.durable_run_output_bytes,
            ),
        ] {
            if actual > limit {
                return Err(PersistenceError::ResourcePressure(format!(
                    "valid retained state requires {name} >= {actual}; current policy is {limit}; history was preserved"
                )));
            }
        }
        Ok(())
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
        match self.commit_startup_batch(StartupBatch::PublishEpoch)? {
            StartupBatchDisposition::Committed => Ok(()),
            StartupBatchDisposition::OverBudget => {
                Err(PersistenceError::ResourcePressure(format!(
                    "startup epoch publication exceeds configured wal_checkpoint_bytes {}",
                    self.admission_limits.resources.wal_checkpoint_bytes
                )))
            }
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
        let mut statement = self
            .connection
            .prepare(
                "SELECT id, creation_key, spec_json, lineage_json, source_epoch, updated_at_ms
             FROM runs WHERE state_kind = 'running'
             AND NOT EXISTS(SELECT 1 FROM handed_live WHERE handed_live.id = runs.id)
             ORDER BY created_at_ms, id LIMIT ?1",
            )
            .map_err(PersistenceError::database)?;
        let rows = statement
            .query_map([limit], |row| {
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
                    return Err(PersistenceError::ResourcePressure(format!(
                        "one startup {} unit exceeds configured WAL page budget",
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
        if charge > self.admission_limits.resources.wal_checkpoint_bytes {
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
        if actual_wal > charge || actual_wal > self.admission_limits.resources.wal_checkpoint_bytes
        {
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
            return self.admission_limits.resources.wal_checkpoint_bytes;
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

    fn drive_staged_start_with_shutdown(
        &mut self,
        prepared: &PreparedPersistentStart,
        candidates: &[PersistentCandidate],
        receipt: &StartReceipt,
        ready: &mpsc::SyncSender<Result<(), StageFailure>>,
        decision: &mpsc::Receiver<StageDecision>,
        shutdown: Option<&AtomicBool>,
    ) -> Result<StageDriveResult, PersistenceError> {
        if let Err(error) = self.validate_prepared_start(prepared) {
            let _ = receipt.decide(StartDisposition::NotCommitted);
            return Ok(StageDriveResult::ReadyFailed(StageFailure {
                error,
                fatal: true,
                capacity: false,
            }));
        }
        if prepared.metadata_bytes > self.admission_limits.metadata_bytes {
            let _ = receipt.decide(StartDisposition::NotCommitted);
            return Ok(StageDriveResult::ReadyFailed(admission_failure(format!(
                "one Run metadata record exceeds the {} byte budget",
                self.admission_limits.metadata_bytes
            ))));
        }

        let wal_baseline = match self.fold_wal_below_ceiling(shutdown) {
            Ok(baseline) => baseline,
            Err(error) => {
                if shutdown.is_some() && matches!(&error, PersistenceError::ActorStopped) {
                    return Err(error);
                }
                let _ = receipt.decide(StartDisposition::NotCommitted);
                return Ok(StageDriveResult::ReadyFailed(admission_failure(format!(
                    "persistent WAL admission could not reach its baseline: {error}"
                ))));
            }
        };
        if let Err(error) = self.connection.release_memory() {
            let _ = receipt.decide(StartDisposition::NotCommitted);
            return Ok(StageDriveResult::ReadyFailed(admission_failure(format!(
                "persistent WAL admission could not release the connection cache: {error}"
            ))));
        }

        let previous_cache_spill = match self.disable_cache_spill() {
            Ok(value) => value,
            Err(stage_failure) => {
                let _ = receipt.decide(StartDisposition::NotCommitted);
                return Ok(StageDriveResult::ReadyFailed(stage_failure));
            }
        };
        let result = self.drive_staged_start_with_spill_disabled(
            prepared,
            candidates,
            receipt,
            ready,
            decision,
            wal_baseline,
        );
        match self.restore_cache_spill(previous_cache_spill) {
            Ok(()) => Ok(result),
            Err(error) => Ok(result.with_restore_failure(error)),
        }
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the WAL baseline travels with the staging owners it constrains"
    )]
    fn drive_staged_start_with_spill_disabled(
        &mut self,
        prepared: &PreparedPersistentStart,
        candidates: &[PersistentCandidate],
        receipt: &StartReceipt,
        ready: &mpsc::SyncSender<Result<(), StageFailure>>,
        decision: &mpsc::Receiver<StageDecision>,
        wal_baseline: u64,
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
        // The WAL must not have moved between admission and `BEGIN IMMEDIATE`.
        // That is what this check has always been for; it used to spell it as
        // "== 0" only because admission left it at zero.
        match file_len(&self.wal_path) {
            Ok(bytes) if bytes == wal_baseline => {}
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
        // The staged transaction must fit the 8 MiB per-transaction ceiling, and
        // the WAL it lands on must still fit the 16 MiB total. `wal_bytes` is
        // compared against the admission baseline rather than zero: nothing may
        // have been written yet, which is the property being proven, but the
        // baseline itself is legitimately non-zero now.
        if wal_bytes != wal_baseline
            || snapshot.writes != 0
            || snapshot.spills != 0
            || charge
                .is_none_or(|charge| charge > self.admission_limits.resources.wal_checkpoint_bytes)
            || charge.is_none_or(|charge| {
                wal_baseline.saturating_add(charge) > self.admission_limits.resources.wal_bytes()
            })
        {
            return self.rollback_before_ready(
                receipt,
                admission_failure(format!(
                    "persistent exact replacement exceeds or cannot prove its 8 MiB WAL charge: \
                     cache={} bytes, writes={}, spills={}, wal={} bytes, baseline={} bytes",
                    snapshot.used_bytes, snapshot.writes, snapshot.spills, wal_bytes, wal_baseline
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
        self.truncate_wal_to_zero_with_shutdown(None)
    }

    /// Bring the WAL under `self.admission_limits.resources.wal_checkpoint_bytes` and report the baseline the
    /// caller's charge proof must be measured against.
    ///
    /// The lifecycle verbs used to checkpoint to *zero* here. That was never
    /// about needing an empty file: ADR 013 requires a proof that one staged
    /// transaction fits the 8 MiB per-transaction ceiling and that the WAL as a
    /// whole stays under 16 MiB, and starting from zero let a single comparison
    /// of the absolute WAL length cover both.
    ///
    /// It is also what made `start` and `remove` lose to tmux. Under a chatty
    /// fleet the WAL sits at 8.5 MB essentially always — sampled every 10 ms it
    /// was at zero for 3.7% of samples at chatty=2 — because the output path
    /// deliberately lets it ride up to the 8 MiB trigger. So every lifecycle op
    /// folded ~8.5 MB at ~1.6 ms/MiB, about 13 ms, which is 85-99% of why those
    /// verbs lose.
    ///
    /// The fix is to prove the same bound against a *delta* instead of an
    /// absolute. Verified against the pinned `SQLite` 3.53.2 amalgamation across
    /// 6 baselines (0 to 8.2 MB) and 4 transaction sizes: `actual - baseline`
    /// stayed under the cache-derived charge in all 24 cases, WAL growth was
    /// strictly monotone, and the delta was baseline-independent (4120 bytes
    /// for a one-row transaction whether the WAL began at 0 or at 8.2 MB). The
    /// standing version of that check is
    /// `cache_bound_covers_spill_disabled_wal_growth_from_any_baseline` in
    /// `ctxmux-sqlite-status`, which re-derives it against whatever `SQLite` the
    /// build is pinned to.
    ///
    /// The ceilings still close arithmetically: this folds whenever the
    /// baseline exceeds 8 MiB, and no transaction may charge more than 8 MiB,
    /// so the post-commit absolute can never exceed the 16 MiB total. That is
    /// the same shape `admit_transaction_with_shutdown` has always used on the
    /// output path, which is both far hotter and already shipping it.
    fn fold_wal_below_ceiling(
        &self,
        shutdown: Option<&AtomicBool>,
    ) -> Result<u64, PersistenceError> {
        if file_len(&self.wal_path)? > self.admission_limits.resources.wal_checkpoint_bytes {
            self.truncate_wal_to_zero_with_shutdown(shutdown)?;
        }
        file_len(&self.wal_path)
    }

    /// One `wal_checkpoint(TRUNCATE)` attempt, with no retry and no sleeping.
    ///
    /// The idle fold runs on the actor thread with nothing queued behind it,
    /// but a command can arrive at any moment. `retry_wal_checkpoint` would
    /// sleep up to 550 ms across its 8 attempts waiting out a reader, and that
    /// wait would be charged to whatever arrives next. A busy WAL simply means
    /// the fold does not happen this time; every path that must bound the WAL
    /// folds on its own and proves its own charge.
    fn try_fold_wal_once(&self) -> bool {
        #[cfg(test)]
        self.test_hooks.idle_folds.fetch_add(1, Ordering::AcqRel);
        let checkpointed =
            self.connection
                .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
                    row.get::<_, i64>(0)
                });
        matches!(checkpointed, Ok(busy) if busy == 0)
    }

    fn truncate_wal_to_zero_with_shutdown(
        &self,
        shutdown: Option<&AtomicBool>,
    ) -> Result<(), PersistenceError> {
        retry_wal_checkpoint(
            shutdown,
            || {
                #[cfg(test)]
                self.test_hooks
                    .checkpoint_attempts
                    .fetch_add(1, Ordering::AcqRel);
                self.connection
                    .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
                        Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                    })
                    .map_err(PersistenceError::database)
            },
            || file_len(&self.wal_path),
        )
    }

    /// Remove one exact terminal candidate, retrying only transient checkpoint
    /// pressure and reporting shutdown as `None` (the caller keeps the Registry
    /// entry). Every other outcome is a definitive [`RemovalDisposition`].
    fn remove_terminal_with_shutdown(
        &mut self,
        candidate: &PersistentCandidate,
        shutdown: Option<&AtomicBool>,
    ) -> Option<RemovalDisposition> {
        loop {
            if shutdown.is_some_and(|shutdown| shutdown.load(Ordering::Acquire)) {
                return None;
            }
            match self.remove_terminal_once(candidate, shutdown) {
                Ok(disposition) => return Some(disposition),
                Err(error) if error.is_transient_storage() => {
                    if wait_for_shutdown(shutdown?, STORAGE_RETRY_INTERVAL) {
                        return None;
                    }
                }
                Err(error) => return Some(RemovalDisposition::NotRemoved(error)),
            }
        }
    }

    fn remove_terminal_once(
        &mut self,
        candidate: &PersistentCandidate,
        shutdown: Option<&AtomicBool>,
    ) -> Result<RemovalDisposition, PersistenceError> {
        let wal_baseline = self.fold_wal_below_ceiling(shutdown)?;
        self.connection
            .release_memory()
            .map_err(PersistenceError::database)?;
        let previous_cache_spill = self
            .disable_cache_spill()
            .map_err(|failure| failure.error)?;
        let result = self.remove_terminal_spill_disabled(candidate, wal_baseline);
        match self.restore_cache_spill(previous_cache_spill) {
            Ok(()) => Ok(result),
            Err(restore_error) => match result {
                // A committed delete whose only later failure is the spill
                // restore is still durably gone; surface removal, not unknown.
                RemovalDisposition::Removed => Ok(RemovalDisposition::Removed),
                RemovalDisposition::NotRemoved(error) | RemovalDisposition::Unknown(error) => Ok(
                    RemovalDisposition::Unknown(combine_errors(&error, &restore_error)),
                ),
            },
        }
    }

    fn remove_terminal_spill_disabled(
        &self,
        candidate: &PersistentCandidate,
        wal_baseline: u64,
    ) -> RemovalDisposition {
        if let Err(error) = ctxmux_sqlite_status::reset_cache_io(&self.connection) {
            return RemovalDisposition::NotRemoved(
                admission_failure(format!(
                    "persistent removal could not reset cache counters: {error}"
                ))
                .error,
            );
        }
        if let Err(error) = self.connection.execute_batch("BEGIN IMMEDIATE") {
            return RemovalDisposition::NotRemoved(PersistenceError::database(error));
        }
        match file_len(&self.wal_path) {
            Ok(bytes) if bytes == wal_baseline => {}
            Ok(_) => {
                return self.rollback_removal(
                    PersistenceError::Mutation(
                        "persistent WAL changed before exact removal".to_owned(),
                    ),
                    false,
                );
            }
            Err(error) => return self.rollback_removal(error, false),
        }
        if let Err(error) = self.delete_exact_terminal(candidate) {
            return self.rollback_removal(error, false);
        }
        let snapshot = match ctxmux_sqlite_status::cache_admission_snapshot(&self.connection) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                return self.rollback_removal(
                    admission_failure(format!(
                        "persistent removal could not observe cache status: {error}"
                    ))
                    .error,
                    false,
                );
            }
        };
        let wal_bytes = match file_len(&self.wal_path) {
            Ok(bytes) => bytes,
            Err(error) => return self.rollback_removal(error, false),
        };
        let charge = wal_charge_for_cache(snapshot.used_bytes);
        if wal_bytes != wal_baseline
            || snapshot.writes != 0
            || snapshot.spills != 0
            || charge
                .is_none_or(|charge| charge > self.admission_limits.resources.wal_checkpoint_bytes)
            || charge.is_none_or(|charge| {
                wal_baseline.saturating_add(charge) > self.admission_limits.resources.wal_bytes()
            })
        {
            return self.rollback_removal(
                admission_failure(format!(
                    "persistent removal exceeds or cannot prove its 8 MiB WAL charge: \
                     cache={} bytes, writes={}, spills={}, wal={} bytes, baseline={} bytes",
                    snapshot.used_bytes, snapshot.writes, snapshot.spills, wal_bytes, wal_baseline
                ))
                .error,
                false,
            );
        }
        match self.connection.execute_batch("COMMIT") {
            Ok(()) => match self.validate_files() {
                Ok(()) => RemovalDisposition::Removed,
                // The row is durably gone even if a post-commit file check
                // fails; latch that as a fatal removal, never as retained.
                Err(error) => RemovalDisposition::Unknown(error),
            },
            Err(commit_error) => self.classify_failed_removal(candidate, commit_error),
        }
    }

    /// Roll back a staged removal. `after_commit` is only ever false here (no SQL
    /// runs after COMMIT), so a rollback failure is `Unknown`, matching the start
    /// path's fail-closed classification.
    fn rollback_removal(&self, error: PersistenceError, after_commit: bool) -> RemovalDisposition {
        match self.connection.execute_batch("ROLLBACK") {
            Ok(()) if after_commit => RemovalDisposition::Unknown(error),
            Ok(()) => RemovalDisposition::NotRemoved(error),
            Err(rollback_error) => RemovalDisposition::Unknown(PersistenceError::Mutation(
                format!("{error}; removal rollback failed: {rollback_error}"),
            )),
        }
    }

    fn classify_failed_removal(
        &self,
        candidate: &PersistentCandidate,
        commit_error: rusqlite::Error,
    ) -> RemovalDisposition {
        if !self.connection.is_autocommit()
            && let Err(rollback_error) = self.connection.execute_batch("ROLLBACK")
        {
            return RemovalDisposition::Unknown(PersistenceError::Mutation(format!(
                "persistent removal COMMIT failed ({commit_error}) and rollback failed \
                 ({rollback_error})"
            )));
        }
        match self.exact_terminal_present(candidate) {
            Ok(true) => RemovalDisposition::NotRemoved(PersistenceError::database(commit_error)),
            Ok(false) => RemovalDisposition::Removed,
            Err(probe_error) => RemovalDisposition::Unknown(PersistenceError::Mutation(format!(
                "persistent removal COMMIT failed ({commit_error}) and exact probe failed: \
                 {probe_error}"
            ))),
        }
    }

    fn delete_exact_terminal(
        &self,
        candidate: &PersistentCandidate,
    ) -> Result<(), PersistenceError> {
        let deleted = self
            .connection
            .execute(
                "DELETE FROM runs WHERE id = ?1 AND creation_key = ?2 COLLATE BINARY
                 AND metadata_bytes = ?3 AND state_kind != 'running'",
                params![
                    candidate.id.to_string(),
                    candidate.operation_key.as_str(),
                    i64::try_from(candidate.metadata_bytes).map_err(|_| {
                        PersistenceError::Mutation(
                            "candidate metadata does not fit SQLite".to_owned(),
                        )
                    })?
                ],
            )
            .map_err(PersistenceError::database)?;
        if deleted != 1 {
            return Err(PersistenceError::Mutation(format!(
                "persistent removal candidate {} does not match its exact terminal snapshot",
                candidate.id
            )));
        }
        Ok(())
    }

    fn exact_terminal_present(
        &self,
        candidate: &PersistentCandidate,
    ) -> Result<bool, PersistenceError> {
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
        let Some((key, metadata, state_kind, state_json)) = stored else {
            return Ok(false);
        };
        let state: RunState =
            serde_json::from_str(&state_json).map_err(PersistenceError::serialization)?;
        Ok(
            key.as_bytes() == candidate.operation_key.as_str().as_bytes()
                && nonnegative_u64(metadata, "candidate metadata")? == candidate.metadata_bytes
                && state_kind == state_kind_for(&state)
                && !state.is_running(),
        )
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
        if let Some(result) = self.abort_start_before_commit_if_armed(receipt) {
            return result;
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

    /// Consume the `fail_next_start_before_commit` hook and, if it was armed,
    /// roll back the staged transaction to stand in for a crash between staging
    /// and COMMIT. Returns the completed disposition to short-circuit with, or
    /// `None` when the hook was not armed and the real commit should proceed.
    #[cfg(test)]
    fn abort_start_before_commit_if_armed(
        &self,
        receipt: &StartReceipt,
    ) -> Option<StageDriveResult> {
        if !self
            .test_hooks
            .fail_next_start_before_commit
            .swap(false, Ordering::AcqRel)
        {
            return None;
        }
        Some(match self.connection.execute_batch("ROLLBACK") {
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
        })
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

    #[cfg(test)]
    fn append_batch(
        &mut self,
        batch: &[(RunId, OutputReplay, Arc<AtomicU64>)],
    ) -> Result<(), PersistenceError> {
        self.append_batch_with_shutdown(batch, None)
    }

    fn append_batch_with_shutdown(
        &mut self,
        batch: &[(RunId, OutputReplay, Arc<AtomicU64>)],
        shutdown: Option<&AtomicBool>,
    ) -> Result<(), PersistenceError> {
        let mut transaction_batch = Vec::new();
        let mut transaction_payload = 0_usize;
        let mut expected_heads = HashMap::new();
        for (id, replay, durable_head) in batch {
            // Finalize carries the final replay, so lifecycle removal can
            // overtake old appends. A deleted Run owns no further output;
            // discard its queued append without poisoning unrelated Runs.
            let exists = self
                .connection
                .prepare_cached("SELECT 1 FROM runs WHERE id = ?1")
                .and_then(|mut statement| statement.exists([id.to_string()]))
                .map_err(PersistenceError::database)?;
            if !exists {
                continue;
            }
            let groups = split_chunks(&replay.chunks)?;
            if groups.is_empty() {
                // A chunkless replay still has an UPDATE to make -- its
                // `truncated` flag and `first_available_byte` -- but it offers
                // no bytes, so it is contiguous with everything and belongs in
                // whatever transaction is already open.
                //
                // Giving it one of its own used to cost TWO fsyncs, not one:
                // the arm flushed the pending batch to get out of the way, so
                // an empty replay arriving mid-drain split the appends around
                // it into separate transactions. The empty COMMIT is genuinely
                // cheap (0.043 ms, measured); the flush it forced was not --
                // that one carries real output, ~1.5-2.9 ms at
                // `synchronous=FULL`. Every create ends in
                // `activate_persistence_after_publication` appending
                // `replay(0)`, so under a chatty fleet the split landed on the
                // start path once per Run.
                //
                // No watermark moves here: `expected_heads` stays put, because
                // a replay with no chunks advances nothing that the next group
                // must be contiguous against.
                transaction_batch.push((*id, replay.clone(), Arc::clone(durable_head)));
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
                    self.append_transaction_with_shutdown(&transaction_batch, None, shutdown)?;
                    transaction_batch.clear();
                    transaction_payload = 0;
                    expected_heads.clear();
                }
                if !is_fresh_contiguous {
                    self.append_transaction_with_shutdown(
                        &[(*id, partial, Arc::clone(durable_head))],
                        None,
                        shutdown,
                    )?;
                    continue;
                }
                transaction_payload = transaction_payload.saturating_add(partial_payload);
                expected_heads.insert(*id, partial.latest_output_bytes);
                transaction_batch.push((*id, partial, Arc::clone(durable_head)));
            }
        }
        if !transaction_batch.is_empty() {
            self.append_transaction_with_shutdown(&transaction_batch, None, shutdown)?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn finalize_with_shutdown(
        &mut self,
        id: RunId,
        actual_pid: u32,
        replay: &OutputReplay,
        state: &RunState,
        durable_head: &Arc<AtomicU64>,
        metadata_owner: &Arc<AtomicU64>,
        source_gap_after_byte: Option<u64>,
        shutdown: Option<&AtomicBool>,
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
            self.append_transaction_with_shutdown(
                &[(id, prefix_replay, Arc::clone(durable_head))],
                None,
                shutdown,
            )?;
        }
        let terminal_replay = OutputReplay {
            chunks: final_chunks,
            first_available_byte: replay.first_available_byte,
            latest_output_bytes: replay.latest_output_bytes,
            truncated: replay.truncated,
        };
        self.append_transaction_with_shutdown(
            &[(id, terminal_replay, Arc::clone(durable_head))],
            Some((id, actual_pid, state, metadata_owner, source_gap_after_byte)),
            shutdown,
        )
    }

    fn missing_chunks(
        &self,
        id: RunId,
        replay: &OutputReplay,
    ) -> Result<Vec<OutputChunk>, PersistenceError> {
        let (durable_head, durable_floor): (i64, i64) = self
            .connection
            .query_row(
                "SELECT durable_output_bytes, durable_first_available_byte FROM runs WHERE id = ?1",
                [id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(PersistenceError::database)?;
        let durable_head = nonnegative_u64(durable_head, "durable head")?;
        let durable_floor = nonnegative_u64(durable_floor, "durable floor")?;
        if durable_floor > durable_head {
            return Err(PersistenceError::Corrupt(
                "durable floor exceeds head".to_owned(),
            ));
        }
        replay
            .chunks
            .iter()
            .filter(|chunk| chunk.end_byte > durable_head)
            .map(|chunk| {
                let bytes = u64::try_from(chunk.data.len()).map_err(|_| {
                    PersistenceError::Mutation("output chunk is too large".to_owned())
                })?;
                if chunk.end_byte.checked_sub(chunk.start_byte) != Some(bytes) {
                    return Err(PersistenceError::Mutation(format!(
                        "Run {id} final replay range does not match its bytes"
                    )));
                }
                // This finalizer owns the original hot replay. Its confirmed prefix
                // may have been evicted by global disk retention while its suffix
                // remains uncommitted. Do not ask storage to verify bytes it has
                // already confirmed and intentionally retired. Keep the retained
                // overlap: apply_replay_chunk still verifies those original bytes.
                if chunk.start_byte >= durable_floor {
                    return Ok(chunk.clone());
                }
                let skipped = usize::try_from(durable_floor - chunk.start_byte).map_err(|_| {
                    PersistenceError::Mutation("final replay prefix exceeds memory".to_owned())
                })?;
                Ok(OutputChunk {
                    start_byte: durable_floor,
                    end_byte: chunk.end_byte,
                    data: chunk.data[skipped..].to_vec(),
                })
            })
            .collect()
    }

    #[allow(clippy::too_many_lines)]
    fn append_transaction_with_shutdown(
        &mut self,
        batch: &[(RunId, OutputReplay, Arc<AtomicU64>)],
        terminal: Option<TerminalSettlement<'_>>,
        shutdown: Option<&AtomicBool>,
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
        self.compact_replay_step()?;
        // Old and replacement generations share one scratch allowance. If
        // foreground writes consume that headroom, finish admitted maintenance
        // before accepting more payload, rather than latching ordinary growth.
        let replay_allowance = self
            .admission_limits
            .resources
            .durable_replay_bytes
            .saturating_mul(3)
            .saturating_add(MAX_TRANSACTION_PAYLOAD_BYTES as u64);
        if directory_file_len(&self.replay_dir)?.saturating_add(payload as u64) > replay_allowance {
            self.maybe_compact_replay()?;
            if directory_file_len(&self.replay_dir)?.saturating_add(payload as u64)
                > replay_allowance
            {
                return Err(PersistenceError::ResourcePressure("unreferenced replay cleanup must finish before the configured storage budget can fund this append".to_owned()));
            }
        }
        if shutdown.is_some_and(|flag| flag.load(Ordering::Acquire)) {
            return Err(PersistenceError::ActorStopped);
        }
        let replay_dir = self.replay_dir.clone();
        let replay_file = self.replay_file.clone();
        let resources = self.admission_limits.resources;
        let path = replay_dir.join(&replay_file);
        let before = file_len(&path)?;
        let mut settlement = None;
        let committed = self.commit_maintenance_batch(|transaction| {
            let mut cursor_updates = HashMap::new();
            for (id, replay, _) in coalesce_batch(batch) {
                let _ = append_replay_external_with_limit(
                    transaction,
                    id,
                    &replay,
                    &replay_dir,
                    &replay_file,
                    resources.durable_run_output_bytes,
                )?;
                let head = read_run_head(transaction, id)?;
                cursor_updates.insert(id, head);
            }
            let _ = prune_global_replay_to(transaction, resources.durable_replay_bytes)?;
            let mut terminal_metadata = None;
            if let Some((id, actual_pid, state, metadata_owner, source_gap)) = terminal {
                let (kind, mut state_json) = encoded_state(state)?;
                if let Some(cursor) = source_gap {
                    let mut facts: serde_json::Value = serde_json::from_str(&state_json)
                        .map_err(PersistenceError::database_message)?;
                    facts["ctxmux_source_gap_after_byte"] = serde_json::Value::from(cursor);
                    state_json = serde_json::to_string(&facts)
                        .map_err(PersistenceError::database_message)?;
                }
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
            settlement = Some((cursor_updates, terminal_metadata));
            Ok(())
        });
        match committed {
            Ok(true) => {}
            Ok(false) => {
                truncate_replay_tail(&path, before)?;
                return self.reduce_output_transaction(batch, terminal, shutdown);
            }
            Err(error) => {
                // Unknown COMMIT/rollback leaves every possibly indexed byte
                // intact. A proven pre-commit storage failure may discard its
                // unindexed append tail before the actor retries.
                if !matches!(error, PersistenceError::Mutation(_)) {
                    truncate_replay_tail(&path, before)?;
                }
                return Err(error);
            }
        }
        let (cursor_updates, terminal_metadata) =
            settlement.expect("committed staged output has a settlement");
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

    fn reduce_output_transaction(
        &mut self,
        batch: &[(RunId, OutputReplay, Arc<AtomicU64>)],
        terminal: Option<TerminalSettlement<'_>>,
        shutdown: Option<&AtomicBool>,
    ) -> Result<(), PersistenceError> {
        if batch.len() > 1 {
            let middle = batch.len() / 2;
            self.append_transaction_with_shutdown(&batch[..middle], None, shutdown)?;
            return self.append_transaction_with_shutdown(&batch[middle..], terminal, shutdown);
        }
        let Some((id, replay, durable)) = batch.first() else {
            return Err(PersistenceError::ResourcePressure(
                "empty output metadata does not fit the configured WAL window".to_owned(),
            ));
        };
        let chunks = &replay.chunks;
        if chunks.len() > 1 {
            let middle = chunks.len() / 2;
            let prefix = OutputReplay {
                chunks: chunks[..middle].to_vec(),
                latest_output_bytes: chunks[middle - 1].end_byte,
                ..replay.clone()
            };
            self.append_transaction_with_shutdown(
                &[(*id, prefix, Arc::clone(durable))],
                None,
                shutdown,
            )?;
            let tail = OutputReplay {
                chunks: chunks[middle..].to_vec(),
                ..replay.clone()
            };
            return self.append_transaction_with_shutdown(
                &[(*id, tail, Arc::clone(durable))],
                terminal,
                shutdown,
            );
        }
        // Splitting payload cannot shrink a large old-index deletion. Reclaim
        // only the prefix this accepted output would evict, in admitted batches,
        // then retry the original byte-complete logical operation.
        if self.reclaim_output_prefix(*id, replay)? {
            return self.append_transaction_with_shutdown(batch, terminal, shutdown);
        }
        if let Some(chunk) = chunks.first().filter(|chunk| chunk.data.len() > 1) {
            let middle = chunk.data.len() / 2;
            let split_byte = chunk.start_byte + middle as u64;
            let prefix = OutputReplay {
                chunks: vec![OutputChunk {
                    start_byte: chunk.start_byte,
                    end_byte: split_byte,
                    data: chunk.data[..middle].to_vec(),
                }],
                latest_output_bytes: split_byte,
                ..replay.clone()
            };
            self.append_transaction_with_shutdown(
                &[(*id, prefix, Arc::clone(durable))],
                None,
                shutdown,
            )?;
            let tail = OutputReplay {
                chunks: vec![OutputChunk {
                    start_byte: split_byte,
                    end_byte: chunk.end_byte,
                    data: chunk.data[middle..].to_vec(),
                }],
                ..replay.clone()
            };
            return self.append_transaction_with_shutdown(
                &[(*id, tail, Arc::clone(durable))],
                terminal,
                shutdown,
            );
        }
        Err(PersistenceError::ResourcePressure("one output metadata unit does not fit the configured WAL window; increase wal_checkpoint_bytes".to_owned()))
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one admitted prefix transaction with adaptive rollback reduction"
    )]
    fn reclaim_output_prefix(
        &mut self,
        id: RunId,
        replay: &OutputReplay,
    ) -> Result<bool, PersistenceError> {
        let (head, bytes): (i64, i64) = self
            .connection
            .query_row(
                "SELECT durable_output_bytes, replay_bytes FROM runs WHERE id = ?1",
                [id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(PersistenceError::database)?;
        let head = nonnegative_u64(head, "replay head")?;
        let bytes = nonnegative_u64(bytes, "replay bytes")?;
        let fresh = replay
            .chunks
            .iter()
            .map(|chunk| chunk.end_byte.saturating_sub(chunk.start_byte.max(head)))
            .sum::<u64>();
        let gap = replay.truncated && replay.first_available_byte > head;
        let global: i64 = self
            .connection
            .query_row(
                "SELECT coalesce(sum(replay_bytes), 0) FROM runs",
                [],
                |row| row.get(0),
            )
            .map_err(PersistenceError::database)?;
        let own_needed = if gap {
            bytes
        } else {
            (bytes)
                .saturating_add(fresh)
                .saturating_sub(self.admission_limits.resources.durable_run_output_bytes)
        };
        let global_needed = nonnegative_u64(global, "global replay bytes")?
            .saturating_add(fresh)
            .saturating_sub(self.admission_limits.resources.durable_replay_bytes);
        let needed = own_needed.max(global_needed);
        if needed == 0 {
            return Ok(false);
        }
        let target = if own_needed > 0 {
            Some(id.to_string())
        } else {
            None
        };
        let mut rows = (self.admission_limits.resources.wal_checkpoint_bytes / WAL_FRAME_BYTES)
            .max(1) as usize;
        loop {
            let candidates = {
                let mut statement = self.connection.prepare(
                    "SELECT ordinal, run_id, data_bytes FROM replay_chunks WHERE (?1 IS NULL OR run_id = ?1) ORDER BY ordinal LIMIT ?2"
                ).map_err(PersistenceError::database)?;
                let mut remaining = needed;
                statement
                    .query_map(
                        params![
                            target,
                            i64::try_from(rows).expect("WAL frame count fits SQLite")
                        ],
                        |row| {
                            Ok((
                                row.get::<_, i64>(0)?,
                                row.get::<_, String>(1)?,
                                row.get::<_, i64>(2)?,
                            ))
                        },
                    )
                    .map_err(PersistenceError::database)?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(PersistenceError::database)?
                    .into_iter()
                    .scan(&mut remaining, |remaining, (ordinal, run_id, bytes)| {
                        if **remaining == 0 {
                            return None;
                        }
                        let shed = (**remaining)
                            .min(u64::try_from(bytes).expect("validated positive extent length"));
                        **remaining -= shed;
                        Some((
                            ordinal,
                            run_id,
                            bytes,
                            i64::try_from(shed).expect("shed fits extent"),
                        ))
                    })
                    .collect::<Vec<_>>()
            };
            if candidates.is_empty() {
                return Ok(false);
            }
            if self.commit_maintenance_batch(|connection| {
                for (ordinal, run_id, bytes, shed) in &candidates {
                    if shed == bytes {
                        connection
                            .execute("DELETE FROM replay_chunks WHERE ordinal = ?1", [ordinal])
                            .map_err(PersistenceError::database)?;
                    } else {
                        connection
                            .execute(
                                "UPDATE replay_chunks SET start_byte=start_byte+?2,
                            data_offset=data_offset+?2, data_bytes=data_bytes-?2 WHERE ordinal=?1",
                                params![ordinal, shed],
                            )
                            .map_err(PersistenceError::database)?;
                    }
                    shed_run_bytes(connection, run_id, *shed)?;
                }
                Ok(())
            })? {
                return Ok(true);
            }
            if rows == 1 {
                return Err(PersistenceError::ResourcePressure(
                    "one replay reclamation unit exceeds the configured WAL window".to_owned(),
                ));
            }
            rows /= 2;
        }
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
            &self.replay_dir,
            &self.database_path,
            &self.wal_path,
            &self.shm_path,
            self.admission_limits.resources,
        )
    }

    /// Remove only unreferenced generations and abandoned append tails. A
    /// compaction interrupted between coordinate batches legitimately leaves
    /// references to both its source and its active destination.
    fn normalize_replay_files(&self) -> Result<bool, PersistenceError> {
        let mut retired = true;
        let mut referenced = self
            .connection
            .prepare("SELECT data_file, max(data_offset + data_bytes) FROM replay_chunks GROUP BY data_file")
            .map_err(PersistenceError::database)?
            .query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)))
            .map_err(PersistenceError::database)?
            .collect::<Result<HashMap<_, _>, _>>()
            .map_err(PersistenceError::database)?;
        referenced.entry(self.replay_file.clone()).or_insert(0);
        for (name, end) in &referenced {
            validate_replay_file_name(name)?;
            let path = self.replay_dir.join(name);
            validate_state_file(&path)?;
            let end = nonnegative_u64(*end, "replay file end")?;
            let actual = file_len(&path)?;
            if actual < end {
                return Err(PersistenceError::Corrupt(format!(
                    "replay file {name} is shorter than its durable references"
                )));
            }
            if actual > end {
                let file = OpenOptions::new()
                    .write(true)
                    .open(&path)
                    .map_err(|source| PersistenceError::io(&path, source))?;
                file.set_len(end)
                    .map_err(|source| PersistenceError::io(&path, source))?;
                file.sync_data()
                    .map_err(|source| PersistenceError::io(&path, source))?;
            }
        }
        for entry in fs::read_dir(&self.replay_dir)
            .map_err(|source| PersistenceError::io(&self.replay_dir, source))?
        {
            let entry = entry.map_err(|source| PersistenceError::io(&self.replay_dir, source))?;
            let path = entry.path();
            if !path
                .file_name()
                .and_then(std::ffi::OsStr::to_str)
                .is_some_and(|name| referenced.contains_key(name))
                && fs::remove_file(&path).is_err()
            {
                retired = false;
            }
        }
        if sync_directory(&self.replay_dir).is_err() {
            retired = false;
        }
        Ok(retired)
    }

    /// Apply a maintenance transaction using the same measured, spill-disabled
    /// page proof as Start and Remove. `false` means a proven rollback exceeded
    /// the transaction budget; the caller can shrink its work unit. An unknown
    /// COMMIT is always non-retryable, even when `SQLite` reports storage pressure.
    fn commit_maintenance_batch(
        &mut self,
        apply: impl FnOnce(&Connection) -> Result<(), PersistenceError>,
    ) -> Result<bool, PersistenceError> {
        let baseline = self.fold_wal_below_ceiling(None)?;
        self.connection
            .release_memory()
            .map_err(PersistenceError::database)?;
        let previous = self
            .disable_cache_spill()
            .map_err(|failure| failure.error)?;
        let result = self.commit_maintenance_batch_without_spill(baseline, apply);
        match self.restore_cache_spill(previous) {
            Ok(()) => result,
            Err(error) => Err(PersistenceError::Mutation(format!(
                "maintenance cache state could not be restored: {error}"
            ))),
        }
    }

    fn commit_maintenance_batch_without_spill(
        &self,
        baseline: u64,
        apply: impl FnOnce(&Connection) -> Result<(), PersistenceError>,
    ) -> Result<bool, PersistenceError> {
        ctxmux_sqlite_status::reset_cache_io(&self.connection)
            .map_err(PersistenceError::database)?;
        self.connection
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(PersistenceError::database)?;
        let proof = (|| {
            apply(&self.connection)?;
            let snapshot = ctxmux_sqlite_status::cache_admission_snapshot(&self.connection)
                .map_err(PersistenceError::database)?;
            if snapshot.writes != 0 || snapshot.spills != 0 || file_len(&self.wal_path)? != baseline
            {
                return Err(PersistenceError::Mutation(
                    "maintenance changed the WAL before admission".to_owned(),
                ));
            }
            Ok(wal_charge_for_cache(snapshot.used_bytes))
        })();
        let charge = match proof {
            Ok(charge) => charge,
            Err(error) => return self.rollback_maintenance_error(error),
        };
        if charge.is_none_or(|charge| {
            charge > self.admission_limits.resources.wal_checkpoint_bytes
                || baseline.saturating_add(charge) > self.admission_limits.resources.wal_bytes()
        }) {
            self.connection.execute_batch("ROLLBACK").map_err(|error| {
                PersistenceError::Mutation(format!("maintenance rollback failed: {error}"))
            })?;
            return Ok(false);
        }
        #[cfg(test)]
        let injected = self
            .test_hooks
            .maintenance_commits_before_error
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |left| {
                left.checked_sub(1)
            })
            .is_ok_and(|left| left == 1);
        #[cfg(test)]
        let committed = if injected {
            let commit = self
                .test_hooks
                .maintenance_error_committed
                .load(Ordering::Acquire);
            self.connection
                .execute_batch(if commit { "COMMIT" } else { "ROLLBACK" })
                .expect("construct the exact maintenance outcome");
            Err(PersistenceError::injected_disk_full())
        } else {
            self.connection
                .execute_batch("COMMIT")
                .map_err(PersistenceError::database)
        };
        #[cfg(not(test))]
        let committed = self
            .connection
            .execute_batch("COMMIT")
            .map_err(PersistenceError::database);
        if let Err(error) = committed {
            // No later write may guess whether the rows moved. All possibly
            // referenced payloads survive so startup can read SQLite's truth.
            if !self.connection.is_autocommit() {
                let _ = self.connection.execute_batch("ROLLBACK");
            }
            return Err(PersistenceError::Mutation(format!(
                "maintenance COMMIT outcome requires recovery: {error}"
            )));
        }
        let actual = self.committed_maintenance_wal_len().map_err(|error| {
            PersistenceError::Mutation(format!(
                "maintenance post-COMMIT WAL inspection requires recovery: {error}"
            ))
        })?;
        if actual.saturating_sub(baseline) > charge.expect("admitted charge exists") {
            return Err(PersistenceError::Mutation(format!(
                "maintenance WAL exceeded its admitted page charge: {actual} bytes"
            )));
        }
        Ok(true)
    }

    fn committed_maintenance_wal_len(&self) -> Result<u64, PersistenceError> {
        #[cfg(test)]
        if self
            .test_hooks
            .maintenance_commits_before_stat_error
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |left| {
                left.checked_sub(1)
            })
            .is_ok_and(|left| left == 1)
        {
            return Err(PersistenceError::io(
                &self.wal_path,
                io::Error::new(
                    io::ErrorKind::StorageFull,
                    "injected post-COMMIT stat failure",
                ),
            ));
        }
        file_len(&self.wal_path)
    }

    fn rollback_maintenance_error(
        &self,
        error: PersistenceError,
    ) -> Result<bool, PersistenceError> {
        match self.connection.execute_batch("ROLLBACK") {
            Ok(()) => Err(error),
            Err(rollback) => Err(PersistenceError::Mutation(format!(
                "{error}; maintenance rollback failed: {rollback}"
            ))),
        }
    }

    /// Pack retained bytes into a new active generation in page-admitted
    /// coordinate batches. The active name and each extent are SQLite-owned;
    /// a crash at any batch leaves a complete readable set of referenced files.
    /// Packed offsets still rebase, so lifetime output never becomes file size.
    fn maybe_compact_replay(&mut self) -> Result<(), PersistenceError> {
        self.compact_replay(None)
    }

    fn compact_replay_step(&mut self) -> Result<(), PersistenceError> {
        let result = self.compact_replay(Some(1));
        if result.is_err() {
            self.replay_cleanup_needed = true;
        }
        result
    }

    fn compact_replay(&mut self, batches: Option<usize>) -> Result<(), PersistenceError> {
        if self.replay_cleanup_needed {
            self.replay_cleanup_needed = !self.normalize_replay_files()?;
        }
        let active_bytes = file_len(&self.replay_dir.join(&self.replay_file))?;
        if !self.compaction_pending && active_bytes <= self.replay_compaction_trigger_bytes() {
            return Ok(());
        }
        if self.compaction_pending && self.compaction_sources_pending.is_empty() {
            self.compaction_sources_pending = self.compaction_sources()?.into();
        }
        if self.compaction_sources_pending.is_empty() {
            let old_path = self.replay_dir.join(&self.replay_file);
            if file_len(&old_path)? <= self.replay_compaction_trigger_bytes() {
                self.compaction_pending = false;
                return Ok(());
            }
            let indexed: i64 = self
                .connection
                .query_row(
                    "SELECT coalesce(sum(data_bytes), 0) FROM replay_chunks WHERE data_file = ?1",
                    [&self.replay_file],
                    |row| row.get(0),
                )
                .map_err(PersistenceError::database)?;
            // Compaction must reclaim meaningful space. A small test/operator
            // trigger can sit below the retained window; copying that same
            // live window on every append would spend I/O without reclaiming it.
            if file_len(&old_path)?
                .saturating_sub(nonnegative_u64(indexed, "indexed replay bytes")?)
                < self.replay_compaction_trigger_bytes() / 2
            {
                return Ok(());
            }
            let new_file = format!("replay-{}.bin", Uuid::new_v4());
            let new_path = self.replay_dir.join(&new_file);
            let mut generation = ReplayGenerationGuard::new(new_path.clone());
            let output = OpenOptions::new()
                .create_new(true)
                .write(true)
                .mode(0o600)
                .open(&new_path)
                .map_err(|source| PersistenceError::io(&new_path, source))?;
            output
                .sync_all()
                .map_err(|source| PersistenceError::io(&new_path, source))?;
            sync_directory(&self.replay_dir)?;
            #[cfg(test)]
            crash_replay_compaction_if_armed("before_commit");
            let result = self.commit_maintenance_batch(|connection| {
                connection
                    .execute(
                        "UPDATE runtime_meta SET replay_file = ?1 WHERE singleton = 1",
                        [&new_file],
                    )
                    .map_err(PersistenceError::database)?;
                Ok(())
            });
            // A COMMIT error can still have published the destination name.
            // Preserve it on every error; recovery removes it if unreferenced.
            if matches!(&result, Ok(true) | Err(PersistenceError::Mutation(_))) {
                generation.commit();
            }
            if !result? {
                return Err(PersistenceError::ResourcePressure(
                    "active replay publication cannot fit wal_checkpoint_bytes; increase the policy".to_owned(),
                ));
            }
            self.replay_file = new_file;
            self.compaction_pending = true;
            self.compaction_sources_pending = self.compaction_sources()?.into();
            self.compaction_cursor = i64::MIN;
            // An empty old generation has no indexed sources to migrate.
            if self.compaction_sources_pending.is_empty() {
                if fs::remove_file(&old_path).is_err() {
                    self.replay_cleanup_needed = true;
                }
                self.compaction_pending = false;
            }
        }
        if self.replay_cleanup_needed {
            self.replay_cleanup_needed = !self.normalize_replay_files()?;
        }
        while let Some(source) = self.compaction_sources_pending.front().cloned() {
            if !self.compact_source(&source, batches)? {
                return Ok(());
            }
            self.compaction_sources_pending.pop_front();
            self.compaction_cursor = i64::MIN;
            if batches.is_some() && !self.compaction_sources_pending.is_empty() {
                return Ok(());
            }
        }
        self.replay_cleanup_needed = !self.normalize_replay_files()?;
        self.compaction_pending = false;
        Ok(())
    }

    fn compaction_sources(&self) -> Result<Vec<String>, PersistenceError> {
        self.connection
            .prepare("SELECT DISTINCT data_file FROM replay_chunks WHERE data_file != ?1")
            .map_err(PersistenceError::database)?
            .query_map([&self.replay_file], |row| row.get(0))
            .map_err(PersistenceError::database)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(PersistenceError::database)
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one source migration keeps copy, payload sync, coordinate admission, rollback and source retirement in owner order"
    )]
    fn compact_source(
        &mut self,
        source: &str,
        batches: Option<usize>,
    ) -> Result<bool, PersistenceError> {
        let source_path = self.replay_dir.join(source);
        let target_path = self.replay_dir.join(&self.replay_file);
        let mut input = std::io::BufReader::new(
            File::open(&source_path)
                .map_err(|source| PersistenceError::io(&source_path, source))?,
        );
        let mut output = std::io::BufWriter::new(
            OpenOptions::new()
                .append(true)
                .open(&target_path)
                .map_err(|source| PersistenceError::io(&target_path, source))?,
        );
        // Row count bounds only the metadata working set. Admission uses actual
        // cached pages, not a guessed number of rows or payload bytes.
        let mut row_limit =
            usize::try_from(self.admission_limits.resources.wal_checkpoint_bytes / WAL_FRAME_BYTES)
                .expect("frame count fits usize")
                .max(1);
        let mut after_ordinal = self.compaction_cursor;
        let mut committed_batches = 0;
        let mut input_offset = 0_u64;
        let mut buffer = [0_u8; 8192];
        loop {
            let rows = self
                .connection
                .prepare(
                    "SELECT ordinal, data_offset, data_bytes FROM replay_chunks
                 WHERE ordinal > ?1 AND data_file = ?2 ORDER BY ordinal LIMIT ?3",
                )
                .map_err(PersistenceError::database)?
                .query_map(
                    params![
                        after_ordinal,
                        source,
                        i64::try_from(row_limit).expect("row count fits SQLite")
                    ],
                    |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, i64>(2)?,
                        ))
                    },
                )
                .map_err(PersistenceError::database)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(PersistenceError::database)?;
            if rows.is_empty() {
                break;
            }
            let start = file_len(&target_path)?;
            let mut offset = start;
            let mut moves = Vec::new();
            for (ordinal, old_offset, bytes) in rows {
                let old_offset = nonnegative_u64(old_offset, "compaction source offset")?;
                let bytes = nonnegative_u64(bytes, "compaction length")?;
                if !moves.is_empty()
                    && offset - start + bytes > MAX_TRANSACTION_PAYLOAD_BYTES as u64
                {
                    break;
                }
                if input_offset != old_offset {
                    input
                        .seek(SeekFrom::Start(old_offset))
                        .map_err(|source| PersistenceError::io(&source_path, source))?;
                }
                let mut left = bytes;
                while left != 0 {
                    let count = usize::try_from(left.min(buffer.len() as u64))
                        .expect("buffer size fits usize");
                    input
                        .read_exact(&mut buffer[..count])
                        .map_err(|source| PersistenceError::io(&source_path, source))?;
                    output
                        .write_all(&buffer[..count])
                        .map_err(|source| PersistenceError::io(&target_path, source))?;
                    left -= count as u64;
                }
                input_offset = old_offset.checked_add(bytes).ok_or_else(|| {
                    PersistenceError::Corrupt("compaction source range overflow".to_owned())
                })?;
                moves.push((
                    ordinal,
                    i64::try_from(offset).map_err(|_| {
                        PersistenceError::Mutation("compaction offset overflow".to_owned())
                    })?,
                ));
                offset = offset.checked_add(bytes).ok_or_else(|| {
                    PersistenceError::Mutation("compaction extent overflow".to_owned())
                })?;
            }
            output
                .flush()
                .map_err(|source| PersistenceError::io(&target_path, source))?;
            output
                .get_ref()
                .sync_data()
                .map_err(|source| PersistenceError::io(&target_path, source))?;
            let target_name = self.replay_file.clone();
            let committed = self.commit_maintenance_batch(|connection| {
                let mut update = connection.prepare_cached(
                    "UPDATE replay_chunks SET data_file = ?2, data_offset = ?3 WHERE ordinal = ?1 AND data_file = ?4")
                    .map_err(PersistenceError::database)?;
                for (ordinal, offset) in &moves {
                    let changed = update.execute(params![ordinal, &target_name, offset, source])
                        .map_err(PersistenceError::database)?;
                    if changed != 1 {
                        return Err(PersistenceError::Mutation("compaction coordinate owner changed".to_owned()));
                    }
                }
                Ok(())
            })?;
            if !committed {
                output
                    .get_ref()
                    .set_len(start)
                    .map_err(|source| PersistenceError::io(&target_path, source))?;
                if row_limit == 1 {
                    return Err(PersistenceError::ResourcePressure(
                        "one compaction extent cannot fit wal_checkpoint_bytes; increase the policy".to_owned(),
                    ));
                }
                row_limit = (row_limit / 2).max(1);
                continue;
            }
            after_ordinal = moves.last().expect("nonempty compaction batch").0;
            self.compaction_cursor = after_ordinal;
            #[cfg(test)]
            {
                crash_replay_compaction_if_armed("after_copy");
                crash_replay_compaction_if_armed("after_commit");
            }
            committed_batches += 1;
            if batches.is_some_and(|limit| committed_batches >= limit) {
                return Ok(false);
            }
        }
        drop(input);
        drop(output);
        if fs::remove_file(&source_path).is_err() {
            self.replay_cleanup_needed = true;
        }
        Ok(true)
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
    validate_state_dir(path)
}

/// Validate the established startup contract without creating or repairing the
/// directory. Exec preflight must reject drift even when elevated permissions
/// would allow creating a manifest which the incoming image cannot adopt.
pub(crate) fn validate_state_dir(path: &Path) -> Result<(), PersistenceError> {
    let metadata =
        fs::symlink_metadata(path).map_err(|source| PersistenceError::io(path, source))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(PersistenceError::InvalidDirectory {
            path: path.to_path_buf(),
            message: "path must be a real directory, not a symlink".to_owned(),
        });
    }
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

fn prepare_replay_dir(path: &Path) -> Result<(), PersistenceError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(PersistenceError::InvalidDirectory {
                    path: path.to_path_buf(),
                    message: "replay path must be a real directory, not a symlink".to_owned(),
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
    if metadata.uid() != expected_uid || metadata.permissions().mode() & 0o777 != 0o700 {
        return Err(PersistenceError::InvalidDirectory {
            path: path.to_path_buf(),
            message: "replay directory owner or permissions are invalid".to_owned(),
        });
    }
    Ok(())
}

fn validate_replay_file_name(name: &str) -> Result<(), PersistenceError> {
    let path = Path::new(name);
    if name.is_empty()
        || path.is_absolute()
        || path.components().count() != 1
        || path.file_name().and_then(std::ffi::OsStr::to_str) != Some(name)
        || Path::new(name)
            .extension()
            .and_then(std::ffi::OsStr::to_str)
            != Some("bin")
    {
        return Err(PersistenceError::Corrupt(format!(
            "invalid replay file name {name:?}"
        )));
    }
    Ok(())
}

fn read_replay_file_name(connection: &Connection) -> Result<String, PersistenceError> {
    connection
        .query_row(
            "SELECT replay_file FROM runtime_meta WHERE singleton = 1",
            [],
            |row| row.get(0),
        )
        .map_err(PersistenceError::database)
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
    replay_file: &str,
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
                current_epoch TEXT NOT NULL,
                replay_file TEXT NOT NULL
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
                data_file TEXT NOT NULL,
                data_offset INTEGER NOT NULL CHECK (data_offset >= 0),
                data_bytes INTEGER NOT NULL CHECK (data_bytes > 0),
                UNIQUE(run_id, start_byte)
             );"
        ))
        .map_err(PersistenceError::database)?;
    connection
        .execute(
            "INSERT INTO runtime_meta(singleton, schema_version, runtime_id, current_epoch, replay_file)
             VALUES (1, ?1, ?2, ?3, ?4)",
            params![SCHEMA_VERSION, runtime_id.to_string(), initial_epoch, replay_file],
        )
        .map_err(PersistenceError::database)?;
    Ok(runtime_id)
}

#[allow(clippy::too_many_lines)]
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
    let (meta_rows, meta_version, runtime_id, current_epoch, replay_file):
        (i64, i64, String, String, String) = connection
        .query_row(
            "SELECT count(*), min(schema_version), min(runtime_id), min(current_epoch), min(replay_file) FROM runtime_meta",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
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
    validate_replay_file_name(&replay_file)?;
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
        &[
            "singleton",
            "schema_version",
            "runtime_id",
            "current_epoch",
            "replay_file",
        ],
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
        &[
            "ordinal",
            "run_id",
            "start_byte",
            "end_byte",
            "data_file",
            "data_offset",
            "data_bytes",
        ],
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

#[allow(clippy::too_many_lines)]
fn validate_application_state(
    connection: &Connection,
    replay_dir: &Path,
    active_replay_file: &str,
) -> Result<(), PersistenceError> {
    let mut statement = connection
        .prepare(
            "SELECT id, creation_key, spec_json, lineage_json, state_kind, state_json, source_epoch, pid,
                    durable_first_available_byte, durable_output_bytes, replay_bytes, replay_truncated,
                    metadata_bytes FROM runs ORDER BY id",
        )
        .map_err(PersistenceError::database)?;
    let mut rows = statement.query([]).map_err(PersistenceError::database)?;

    let mut creation_keys = BTreeSet::new();
    while let Some(row) = rows.next().map_err(PersistenceError::database)? {
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
        let _ = source_gap_fact(&state_json, head)?;
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
        validate_replay_window_in(
            connection,
            replay_dir,
            id,
            oldest,
            head,
            replay_bytes,
            truncated != 0,
        )?;
    }
    validate_replay_extents(connection, replay_dir, active_replay_file)?;
    Ok(())
}

/// Every durable extent is produced by the single append-only writer. A
/// repeated or cross-generation range therefore means the `SQLite` index and
/// payload file no longer describe one store, even when each individual Run's
/// cursor still looks contiguous. Reject that corruption before serving any
/// recovered replay.
fn validate_replay_extents(
    connection: &Connection,
    replay_dir: &Path,
    active_replay_file: &str,
) -> Result<(), PersistenceError> {
    validate_state_file(&replay_dir.join(active_replay_file))?;
    let mut file_size = 0_u64;
    let mut current_file = String::new();
    let mut statement = connection
        .prepare(
            "SELECT data_file, data_offset, data_bytes
             FROM replay_chunks ORDER BY data_file, data_offset, ordinal",
        )
        .map_err(PersistenceError::database)?;
    let mut rows = statement.query([]).map_err(PersistenceError::database)?;
    let mut previous_end = 0_u64;
    let mut has_previous = false;
    while let Some(row) = rows.next().map_err(PersistenceError::database)? {
        let data_file: String = row.get(0).map_err(PersistenceError::database)?;
        if data_file != current_file {
            validate_replay_file_name(&data_file)?;
            let path = replay_dir.join(&data_file);
            validate_state_file(&path)?;
            file_size = file_len(&path)?;
            current_file = data_file;
            has_previous = false;
        }
        let offset = nonnegative_u64(
            row.get(1).map_err(PersistenceError::database)?,
            "replay file offset",
        )?;
        let length = nonnegative_u64(
            row.get(2).map_err(PersistenceError::database)?,
            "replay segment length",
        )?;
        let end = offset.checked_add(length).ok_or_else(|| {
            PersistenceError::Corrupt("replay extent range overflows the file offset".to_owned())
        })?;
        if end > file_size {
            return Err(PersistenceError::Corrupt(
                "replay extent exceeds its active generation file".to_owned(),
            ));
        }
        if has_previous && offset < previous_end {
            return Err(PersistenceError::Corrupt(
                "replay extents overlap in the active generation".to_owned(),
            ));
        }
        previous_end = end;
        has_previous = true;
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

#[cfg(test)]
fn validate_replay_window(
    connection: &Connection,
    id: RunId,
    oldest: u64,
    head: u64,
    replay_bytes: u64,
    truncated: bool,
) -> Result<(), PersistenceError> {
    validate_replay_window_in(
        connection,
        test_replay_dir(),
        id,
        oldest,
        head,
        replay_bytes,
        truncated,
    )
}

fn validate_replay_window_in(
    connection: &Connection,
    replay_dir: &Path,
    id: RunId,
    oldest: u64,
    head: u64,
    replay_bytes: u64,
    truncated: bool,
) -> Result<(), PersistenceError> {
    let mut statement = connection
        .prepare(
            "SELECT start_byte, end_byte, data_file, data_offset, data_bytes
             FROM replay_chunks WHERE run_id = ?1 ORDER BY start_byte",
        )
        .map_err(PersistenceError::database)?;
    let mut rows = statement
        .query([id.to_string()])
        .map_err(PersistenceError::database)?;
    let mut any = false;
    let mut file_sizes = HashMap::<String, u64>::new();
    let mut expected = oldest;
    let mut bytes = 0_u64;
    while let Some(row) = rows.next().map_err(PersistenceError::database)? {
        any = true;
        let start_byte: i64 = row.get(0).map_err(PersistenceError::database)?;
        let end_byte: i64 = row.get(1).map_err(PersistenceError::database)?;
        let data_file: String = row.get(2).map_err(PersistenceError::database)?;
        let data_offset: i64 = row.get(3).map_err(PersistenceError::database)?;
        let data_bytes: i64 = row.get(4).map_err(PersistenceError::database)?;
        let start_byte = nonnegative_u64(start_byte, "chunk start byte")?;
        let end_byte = nonnegative_u64(end_byte, "chunk end byte")?;
        let len = nonnegative_u64(data_bytes, "chunk length")?;
        let offset = nonnegative_u64(data_offset, "chunk file offset")?;
        validate_replay_file_name(&data_file)?;
        let end = offset
            .checked_add(len)
            .ok_or_else(|| PersistenceError::Corrupt("replay extent overflows".to_owned()))?;
        let file_bytes = match file_sizes.entry(data_file.clone()) {
            std::collections::hash_map::Entry::Occupied(entry) => *entry.get(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                *entry.insert(file_len(&replay_dir.join(&data_file))?)
            }
        };
        if end > file_bytes {
            return Err(PersistenceError::Corrupt(format!(
                "Run {id} replay exceeds its payload file"
            )));
        }
        if start_byte != expected || end_byte <= start_byte || end_byte - start_byte != len {
            return Err(PersistenceError::Corrupt(format!(
                "Run {id} replay range [{start_byte}, {end_byte}) is invalid or not contiguous at {expected}"
            )));
        }
        expected = end_byte;
        bytes = bytes.saturating_add(len);
    }
    if !any {
        if oldest != head || replay_bytes != 0 || (head > 0 && !truncated) {
            return Err(PersistenceError::Corrupt(format!(
                "Run {id} has empty replay with non-empty cursors"
            )));
        }
        return Ok(());
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
    Ok(())
}

fn truncate_replay_tail(path: &Path, length: u64) -> Result<(), PersistenceError> {
    OpenOptions::new()
        .write(true)
        .open(path)
        .map_err(|error| PersistenceError::io(path, error))?
        .set_len(length)
        .map_err(|error| PersistenceError::io(path, error))
}

fn read_replay_segment(
    replay_dir: &Path,
    file_name: &str,
    offset: u64,
    length: u64,
) -> Result<Vec<u8>, PersistenceError> {
    validate_replay_file_name(file_name)?;
    let path = replay_dir.join(file_name);
    validate_state_file(&path)?;
    let file_size = file_len(&path)?;
    let end = offset.checked_add(length).ok_or_else(|| {
        PersistenceError::Corrupt("replay segment range overflows the file offset".to_owned())
    })?;
    if end > file_size {
        return Err(PersistenceError::Corrupt(format!(
            "replay segment [{offset}, {end}) exceeds file length {file_size}"
        )));
    }
    let mut file = File::open(&path).map_err(|source| PersistenceError::io(&path, source))?;
    file.seek(SeekFrom::Start(offset))
        .map_err(|source| PersistenceError::io(&path, source))?;
    let length = usize::try_from(length)
        .map_err(|_| PersistenceError::Corrupt("replay segment is too large".to_owned()))?;
    let mut data = Vec::new();
    data.try_reserve_exact(length).map_err(|error| {
        PersistenceError::ResourcePressure(format!(
            "cannot allocate {length} replay bytes: {error}"
        ))
    })?;
    data.resize(length, 0);
    file.read_exact(&mut data)
        .map_err(|source| PersistenceError::io(&path, source))?;
    Ok(data)
}

#[cfg(test)]
fn load_recovered(
    connection: &Connection,
    replay_dir: &Path,
) -> Result<Vec<RecoveredRun>, PersistenceError> {
    load_recovered_bounded(
        connection,
        replay_dir,
        ResourceLimits {
            hot_output_bytes: u64::MAX,
            run_output_bytes: usize::MAX,
            ..ResourceLimits::DEFAULT
        },
    )
}

fn load_recovered_bounded(
    connection: &Connection,
    replay_dir: &Path,
    resources: ResourceLimits,
) -> Result<Vec<RecoveredRun>, PersistenceError> {
    let mut statement = connection
        .prepare(
            "SELECT id, creation_key, spec_json, lineage_json, state_json, pid, durable_first_available_byte,
                    durable_output_bytes, replay_truncated, metadata_bytes
             FROM runs
             ORDER BY coalesce(terminal_at_ms, updated_at_ms) DESC, created_at_ms DESC, id DESC",
        )
        .map_err(PersistenceError::database)?;
    let mut rows = statement.query([]).map_err(PersistenceError::database)?;
    let mut recovered = Vec::new();
    let mut available = resources.hot_output_bytes;
    while let Some(row) = rows.next().map_err(PersistenceError::database)? {
        let run = decode_recovered_row(
            connection,
            replay_dir,
            row,
            available.min(resources.run_output_bytes as u64),
        )?;
        available = available.saturating_sub(
            run.replay
                .chunks
                .iter()
                .map(|chunk| chunk.data.len() as u64)
                .sum(),
        );
        recovered.push(run);
    }
    recovered.reverse();
    Ok(recovered)
}

fn decode_recovered_row(
    connection: &Connection,
    replay_dir: &Path,
    row: &rusqlite::Row<'_>,
    cache_bytes: u64,
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
        .map_err(PersistenceError::database_message)?;
    let state = serde_json::from_str(
        &row.get::<_, String>(4)
            .map_err(PersistenceError::database)?,
    )
    .map_err(PersistenceError::database_message)?;
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
    let source_gap_after_byte = source_gap_fact(
        &row.get::<_, String>(4)
            .map_err(PersistenceError::database)?,
        latest_output_bytes,
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
            // A recovered Run is historical: the replacement daemon holds its
            // stored spec but no PTY to ask, exactly as with the input cursor
            // above. The stored `spec.size` is the size once requested, not one
            // any terminal is confirming now.
            current_size: None,
            native_service: None,
        },
        replay: OutputReplay {
            chunks: load_replay_chunks_range(
                connection,
                replay_dir,
                &id_text,
                latest_output_bytes
                    .saturating_sub(cache_bytes)
                    .max(first_available_byte),
                latest_output_bytes,
            )?,
            first_available_byte: latest_output_bytes
                .saturating_sub(cache_bytes)
                .max(first_available_byte),
            latest_output_bytes,
            truncated,
        },
        metadata_bytes,
        source_gap_after_byte,
    })
}

// Lifecycle JSON is a private durable row envelope. Optional output facts
// share its atomic terminal publication and metadata accounting; RunState stays
// agent-neutral and contains only the lifecycle enum.
fn source_gap_fact(state_json: &str, head: u64) -> Result<Option<u64>, PersistenceError> {
    let value: serde_json::Value =
        serde_json::from_str(state_json).map_err(PersistenceError::database_message)?;
    value
        .get("ctxmux_source_gap_after_byte")
        .map(|cursor| {
            cursor
                .as_u64()
                .filter(|cursor| *cursor <= head)
                .ok_or_else(|| {
                    PersistenceError::Corrupt("invalid durable source-gap cursor".to_owned())
                })
        })
        .transpose()
}

// Pages are transport work units, not a limit on retained or lifetime output.
// 64 KiB leaves room for worst-case JSON byte-array inflation under the 1 MiB frame.
const REPLAY_PAGE_BYTES: u64 = 64 * 1024;

fn load_replay_page(
    connection: &Connection,
    replay_dir: &Path,
    id: RunId,
    after: u64,
    through: u64,
) -> Result<OutputReplay, PersistenceError> {
    let (oldest, head, truncated, state_json): (i64, i64, bool, String) = connection.query_row(
        "SELECT durable_first_available_byte, durable_output_bytes, replay_truncated, state_json FROM runs WHERE id = ?1",
        [id.to_string()], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)))
        .map_err(PersistenceError::database)?;
    let oldest = nonnegative_u64(oldest, "replay floor")?;
    let head = nonnegative_u64(head, "replay head")?;
    let from = after.max(oldest);
    let to = through
        .min(head)
        .min(from.saturating_add(REPLAY_PAGE_BYTES));
    Ok(OutputReplay {
        chunks: load_replay_chunks_range(connection, replay_dir, &id.to_string(), from, to)?,
        first_available_byte: oldest,
        latest_output_bytes: head,
        truncated: after < oldest
            || source_gap_fact(&state_json, head)?.is_some_and(|cursor| after <= cursor)
            || (truncated && oldest == 0),
    })
}

fn load_replay_chunks_range(
    connection: &Connection,
    replay_dir: &Path,
    id: &str,
    from: u64,
    to: u64,
) -> Result<Vec<OutputChunk>, PersistenceError> {
    if from >= to {
        return Ok(Vec::new());
    }
    let mut statement = connection.prepare(
        "SELECT start_byte, end_byte, data_file, data_offset
         FROM replay_chunks WHERE run_id = ?1 AND end_byte > ?2 AND start_byte < ?3 ORDER BY start_byte"
    ).map_err(PersistenceError::database)?;
    let mut rows = statement
        .query(params![
            id,
            i64::try_from(from).map_err(|_| PersistenceError::ResourcePressure(
                "replay cursor exceeds SQLite range".to_owned()
            ))?,
            i64::try_from(to).map_err(|_| PersistenceError::ResourcePressure(
                "replay cursor exceeds SQLite range".to_owned()
            ))?
        ])
        .map_err(PersistenceError::database)?;
    let mut chunks = Vec::<OutputChunk>::new();
    let mut opened: Option<(String, BufReader<File>, u64)> = None;
    while let Some(row) = rows.next().map_err(PersistenceError::database)? {
        let start = nonnegative_u64(
            row.get(0).map_err(PersistenceError::database)?,
            "replay start",
        )?;
        let end = nonnegative_u64(
            row.get(1).map_err(PersistenceError::database)?,
            "replay end",
        )?;
        let name: String = row.get(2).map_err(PersistenceError::database)?;
        let offset = nonnegative_u64(
            row.get(3).map_err(PersistenceError::database)?,
            "replay offset",
        )?;
        if opened.as_ref().is_none_or(|(file, _, _)| file != &name) {
            validate_replay_file_name(&name)?;
            let path = replay_dir.join(&name);
            validate_state_file(&path)?;
            opened = Some((
                name,
                BufReader::new(
                    File::open(&path).map_err(|source| PersistenceError::io(path, source))?,
                ),
                u64::MAX,
            ));
        }
        let (_, reader, position) = opened.as_mut().expect("opened extent file");
        let first = start.max(from);
        let last = end.min(to);
        let mut cursor = first;
        while cursor < last {
            let through = last.min(cursor.saturating_add(REPLAY_PAGE_BYTES));
            let physical = offset + cursor - start;
            if *position != physical {
                reader
                    .seek(SeekFrom::Start(physical))
                    .map_err(|source| PersistenceError::io(replay_dir, source))?;
            }
            let mut data =
                vec![0; usize::try_from(through - cursor).expect("bounded replay page fits host")];
            reader
                .read_exact(&mut data)
                .map_err(|source| PersistenceError::io(replay_dir, source))?;
            *position = physical + data.len() as u64;
            if let Some(tail) = chunks.last_mut().filter(|tail| {
                tail.end_byte == cursor
                    && tail.data.len() + data.len()
                        <= usize::try_from(REPLAY_PAGE_BYTES).expect("replay page fits host")
            }) {
                tail.data.extend_from_slice(&data);
                tail.end_byte = through;
            } else {
                chunks.push(OutputChunk {
                    start_byte: cursor,
                    end_byte: through,
                    data,
                });
            }
            cursor = through;
        }
    }
    Ok(chunks)
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
    let mut spec: RunSpec = serde_json::from_str(spec_json)
        .map_err(|error| PersistenceError::Corrupt(format!("invalid spec for {id}: {error}")))?;
    validate_run_spec(&spec)
        .map_err(|error| PersistenceError::Corrupt(format!("invalid spec for {id}: {error}")))?;
    // Immutable metadata uses the same compact representation as Vec/String
    // Clone at admission. Serde growth slack cannot make an admitted policy
    // fail during cold recovery or after an irreversible exec extraction.
    spec.args.shrink_to_fit();
    spec.declared_inputs.shrink_to_fit();
    Ok(spec)
}

#[allow(
    clippy::too_many_lines,
    reason = "one transaction-local range validation, append, pruning, and cursor update is easier to audit as one invariant"
)]
/// Merge a transaction's per-Run appends into one replay each, preserving order.
///
/// One transaction usually carries many appends for the same Run — that is what
/// the actor's batching loop builds — and each one arrives as its own
/// [`OutputReplay`]. Handing them to `append_replay` separately means each call
/// starts with an empty row buffer, so its packing never spans the appends that
/// actually share the transaction, which is where the fragmentation is.
///
/// Merging is metadata-only: the chunks are concatenated, and it is
/// `append_replay` that still decides what becomes a row. `first_available_byte`
/// and `truncated` come from the LAST append for the Run because they describe
/// the producer's log at the newest render, and `latest_output_bytes` likewise.
/// Order within a Run and the relative order of distinct Runs are both kept, so
/// contiguity checks see exactly the sequence they would have seen.
fn coalesce_batch(
    batch: &[(RunId, OutputReplay, Arc<AtomicU64>)],
) -> Vec<(RunId, OutputReplay, Arc<AtomicU64>)> {
    let mut merged: Vec<(RunId, OutputReplay, Arc<AtomicU64>)> = Vec::new();
    let mut index_of: HashMap<RunId, usize> = HashMap::new();
    for (id, replay, durable_head) in batch {
        if let Some(&index) = index_of.get(id) {
            let existing: &mut OutputReplay = &mut merged[index].1;
            existing.chunks.extend(replay.chunks.iter().cloned());
            existing.first_available_byte = replay.first_available_byte;
            existing.latest_output_bytes = replay.latest_output_bytes;
            existing.truncated = replay.truncated;
        } else {
            index_of.insert(*id, merged.len());
            merged.push((*id, replay.clone(), Arc::clone(durable_head)));
        }
    }
    merged
}

/// Drop everything below `new_floor` and report the surviving byte count.
///
/// The durable log is a single contiguous
/// `[durable_first_available_byte, durable_output_bytes)` range, and
/// `validate_replay_window` re-walks it on every open with no fallback for
/// `Corrupt` — an interior hole is not a degraded read, it is a daemon that
/// will not start. So when a gap the producer proves unrecoverable moves the
/// floor, the prefix the gap made unreachable goes with it, exactly as every
/// other retention path here does.
///
/// The count is re-read from the table rather than adjusted arithmetically, so
/// it cannot drift from what was actually deleted.
fn reset_window_to(
    transaction: &Connection,
    id_text: &str,
    new_floor: i64,
) -> Result<i64, PersistenceError> {
    transaction
        .execute(
            "DELETE FROM replay_chunks WHERE run_id = ?1 AND start_byte < ?2",
            params![id_text, new_floor],
        )
        .map_err(PersistenceError::database)?;
    transaction
        .query_row(
            "SELECT coalesce(sum(data_bytes), 0) FROM replay_chunks WHERE run_id = ?1",
            [id_text],
            |row| row.get(0),
        )
        .map_err(PersistenceError::database)
}

/// Whether `[start_byte, end_byte)` is already stored with exactly these bytes.
///
/// A row now spans many appends, so a re-sent range is normally a SLICE of one
/// rather than a row keyed at its own start byte: the lookup finds the row that
/// CONTAINS the range and compares the overlapping bytes. Looking up by exact
/// `start_byte` instead finds nothing for any interior range, which reports
/// honest durable bytes as lost and latches persistence off daemon-wide.
///
fn stored_range_matches_in(
    transaction: &Connection,
    replay_dir: &Path,
    id_text: &str,
    start_byte: i64,
    end_byte: i64,
    expected: &[u8],
) -> Result<bool, PersistenceError> {
    let mut statement = transaction
        .prepare_cached(
            "SELECT start_byte, end_byte, data_file, data_offset
         FROM replay_chunks WHERE run_id = ?1 AND end_byte > ?2 AND start_byte < ?3
         ORDER BY start_byte",
        )
        .map_err(PersistenceError::database)?;
    let mut rows = statement
        .query(params![id_text, start_byte, end_byte])
        .map_err(PersistenceError::database)?;
    let mut cursor = start_byte;
    while cursor < end_byte {
        let Some(row) = rows.next().map_err(PersistenceError::database)? else {
            return Ok(false);
        };
        let row_start: i64 = row.get(0).map_err(PersistenceError::database)?;
        let row_end: i64 = row.get(1).map_err(PersistenceError::database)?;
        let file: String = row.get(2).map_err(PersistenceError::database)?;
        let offset: i64 = row.get(3).map_err(PersistenceError::database)?;
        if row_start > cursor || row_end <= cursor {
            return Ok(false);
        }
        let through = row_end.min(end_byte);
        let data = read_replay_segment(
            replay_dir,
            &file,
            nonnegative_u64(offset + cursor - row_start, "replay offset")?,
            nonnegative_u64(through - cursor, "replay length")?,
        )?;
        let from = usize::try_from(cursor - start_byte).unwrap_or(usize::MAX);
        let to = usize::try_from(through - start_byte).unwrap_or(usize::MAX);
        if expected.get(from..to) != Some(data.as_slice()) {
            return Ok(false);
        }
        cursor = through;
    }
    Ok(true)
}

/// A Run's durable replay cursors plus the row currently being packed.
///
/// The window cursors and the pending row have to move together: buffering a
/// chunk advances `durable_head` before the bytes are a row, so anything that
/// reads the table mid-loop must flush first. Keeping them in one value is what
/// makes that coupling visible rather than a rule to remember.
struct ReplayCursors {
    durable_oldest: i64,
    durable_head: i64,
    replay_bytes: i64,
    pending: Vec<u8>,
    pending_start: i64,
    writer: ReplayFileWriter,
}

fn flush_pending_row_for_cursors(
    transaction: &Connection,
    id_text: &str,
    cursors: &mut ReplayCursors,
) -> Result<(), PersistenceError> {
    if cursors.pending.is_empty() {
        return Ok(());
    }
    let len = i64::try_from(cursors.pending.len())
        .map_err(|_| PersistenceError::Mutation("coalesced row is too large".to_owned()))?;
    let end_byte = cursors.pending_start.checked_add(len).ok_or_else(|| {
        PersistenceError::Mutation("coalesced row end byte exceeds SQLite".to_owned())
    })?;
    let offset = cursors.writer.append(&cursors.pending)?;
    transaction
        .prepare_cached(
            "INSERT INTO replay_chunks(run_id, start_byte, end_byte, data_file,
             data_offset, data_bytes)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        )
        .and_then(|mut statement| {
            statement.execute(params![
                id_text,
                cursors.pending_start,
                end_byte,
                &cursors.writer.file_name,
                i64::try_from(offset).unwrap_or(i64::MAX),
                len,
            ])
        })
        .map_err(PersistenceError::database)?;
    cursors.pending.clear();
    Ok(())
}

/// Append-only payload owner for one persistence transaction.
///
/// `SQLite` stores only replay coordinates. Bytes are written and synced before
/// the transaction commits; an abandoned tail is harmless and is reclaimed by
/// the next startup/compaction sweep. The actor is the sole writer, so one
/// shared file can serve every Run without cross-process locking.
struct ReplayFileWriter {
    dir: PathBuf,
    file_name: String,
    file: File,
    base_len: u64,
    committed: bool,
}

struct ReplayGenerationGuard {
    path: PathBuf,
    committed: bool,
}

#[cfg(test)]
fn crash_replay_compaction_if_armed(phase: &str) {
    if std::env::var(REPLAY_COMPACTION_CRASH_PHASE).as_deref() == Ok(phase) {
        std::process::abort();
    }
}

impl ReplayGenerationGuard {
    fn new(path: PathBuf) -> Self {
        Self {
            path,
            committed: false,
        }
    }

    fn commit(&mut self) {
        self.committed = true;
    }
}

impl Drop for ReplayGenerationGuard {
    fn drop(&mut self) {
        if !self.committed {
            let _ = fs::remove_file(&self.path);
        }
    }
}

impl ReplayFileWriter {
    fn open(dir: &Path, file_name: &str) -> Result<Self, PersistenceError> {
        validate_replay_file_name(file_name)?;
        let path = dir.join(file_name);
        match fs::symlink_metadata(&path) {
            Ok(_) => validate_state_file(&path)?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(source) => return Err(PersistenceError::io(&path, source)),
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .mode(0o600)
            .open(&path)
            .map_err(|source| PersistenceError::io(&path, source))?;
        validate_state_file(&path)?;
        let base_len = file
            .metadata()
            .map_err(|source| PersistenceError::io(&path, source))?
            .len();
        Ok(Self {
            dir: dir.to_path_buf(),
            file_name: file_name.to_owned(),
            file,
            base_len,
            committed: false,
        })
    }

    fn append(&mut self, data: &[u8]) -> Result<u64, PersistenceError> {
        let offset = self
            .file
            .metadata()
            .map_err(|source| PersistenceError::io(self.path(), source))?
            .len();
        self.file
            .write_all(data)
            .map_err(|source| PersistenceError::io(self.path(), source))?;
        self.file
            .sync_data()
            .map_err(|source| PersistenceError::io(self.path(), source))?;
        Ok(offset)
    }

    fn path(&self) -> PathBuf {
        self.dir.join(&self.file_name)
    }

    fn commit(&mut self) {
        self.committed = true;
    }
}

impl Drop for ReplayFileWriter {
    fn drop(&mut self) {
        if !self.committed {
            let _ = self.file.set_len(self.base_len);
        }
    }
}

/// Commit one chunk against the durable window: verify it, jump the floor for
/// a proven-unrecoverable gap, or buffer it into the row being packed.
fn apply_replay_chunk(
    transaction: &Connection,
    id: RunId,
    id_text: &str,
    replay: &OutputReplay,
    chunk: &OutputChunk,
    cursors: &mut ReplayCursors,
) -> Result<(), PersistenceError> {
    let data_len = u64::try_from(chunk.data.len())
        .map_err(|_| PersistenceError::Mutation("output chunk is too large".to_owned()))?;
    if chunk.end_byte <= chunk.start_byte || chunk.end_byte - chunk.start_byte != data_len {
        return Err(PersistenceError::Mutation(format!(
            "Run {id} replay range [{}, {}) does not match its bytes",
            chunk.start_byte, chunk.end_byte
        )));
    }
    let start_byte = i64::try_from(chunk.start_byte)
        .map_err(|_| PersistenceError::Mutation("output start byte exceeds SQLite".to_owned()))?;
    let end_byte = i64::try_from(chunk.end_byte)
        .map_err(|_| PersistenceError::Mutation("output end byte exceeds SQLite".to_owned()))?;

    // Cache blocks and durable extents need not have the same boundaries.
    // Verify the committed prefix before advancing only the fresh suffix.
    if start_byte < cursors.durable_head && end_byte > cursors.durable_head {
        let prefix_bytes = usize::try_from(cursors.durable_head - start_byte)
            .map_err(|_| PersistenceError::Mutation("overlap length exceeds memory".to_owned()))?;
        apply_replay_chunk(
            transaction,
            id,
            id_text,
            replay,
            &OutputChunk {
                start_byte: chunk.start_byte,
                end_byte: nonnegative_u64(cursors.durable_head, "overlap replay head")?,
                data: chunk.data[..prefix_bytes].to_vec(),
            },
            cursors,
        )?;
        return apply_replay_chunk(
            transaction,
            id,
            id_text,
            replay,
            &OutputChunk {
                start_byte: chunk.start_byte + prefix_bytes as u64,
                end_byte: chunk.end_byte,
                data: chunk.data[prefix_bytes..].to_vec(),
            },
            cursors,
        );
    }
    if end_byte <= cursors.durable_head {
        if start_byte < cursors.durable_oldest {
            return Err(PersistenceError::Mutation(format!(
                "Run {id} cannot verify evicted replay range [{}, {})",
                chunk.start_byte, chunk.end_byte
            )));
        }
        // This range is already durable, so it is compared against storage
        // rather than written. Any buffered bytes must land first: they are
        // durable by `durable_head` but not yet a row, and the lookup would
        // otherwise miss them and report honest bytes as lost.
        flush_pending_row_for_cursors(transaction, id_text, cursors)?;
        if !stored_range_matches_in(
            transaction,
            cursors.writer.dir.as_path(),
            id_text,
            start_byte,
            end_byte,
            &chunk.data,
        )? {
            return Err(PersistenceError::Mutation(format!(
                "Run {id} replay range [{}, {}) is missing or changed bytes",
                chunk.start_byte, chunk.end_byte
            )));
        }
        return Ok(());
    }

    if start_byte != cursors.durable_head {
        // A forward jump is normally corruption. There is exactly one way it is
        // honest: the producer's bounded log no longer HOLDS the missing bytes,
        // because the daemon-wide reclaimer evicted them to stay inside the
        // frozen per-Run retention ceiling. The offer says so itself -- it
        // starts exactly at the log's surviving front and carries `truncated` --
        // and no future replay can ever produce those bytes, so refusing only
        // latches persistence off for every Run in the daemon while losing the
        // same bytes anyway.
        //
        // This is deliberately NOT "accept gaps": a chunk starting ABOVE
        // `first_available_byte` is a real contiguity bug (the producer still
        // holds the bytes and failed to send them), and it still fails here.
        // Only the case the producer can PROVE is unrecoverable is admitted, and
        // it is recorded rather than papered over -- `replay_truncated` is the
        // same flag the durable pruner sets when it evicts, so a reader cannot
        // mistake the gap for continuous output.
        let evicted_beyond_recovery =
            replay.truncated && chunk.start_byte == replay.first_available_byte;
        if !evicted_beyond_recovery {
            return Err(PersistenceError::Mutation(format!(
                "Run {id} durable replay gap: got {start_byte}, expected {}",
                cursors.durable_head
            )));
        }
        // `replay_truncated` is derived from `replay.truncated`, which this
        // branch requires, so the flag needs no extra bookkeeping here. The
        // WINDOW does, and `reset_window_to` moves it.
        //
        // Buffered bytes must become rows before that call: they are below the
        // new floor's predecessor and belong either in the prefix being dropped
        // or in the surviving sum, and the re-`SELECT` only sees rows.
        flush_pending_row_for_cursors(transaction, id_text, cursors)?;
        cursors.replay_bytes = reset_window_to(transaction, id_text, start_byte)?;
        cursors.durable_oldest = start_byte;
        cursors.durable_head = start_byte;
    }
    if cursors.durable_head == 0 {
        cursors.durable_oldest = start_byte;
    }

    // Buffer instead of inserting. Every byte that reaches here is the immediate
    // successor of the previous one (`start_byte == durable_head` is the only
    // surviving path), so the buffer is always one contiguous range starting at
    // `pending_start` -- exactly the shape of one row.
    if cursors.pending.is_empty() {
        cursors.pending_start = start_byte;
    }
    cursors.pending.extend_from_slice(&chunk.data);
    if cursors.pending.len() >= COALESCE_ROW_BYTES {
        flush_pending_row_for_cursors(transaction, id_text, cursors)?;
    }
    cursors.durable_head = end_byte;
    cursors.replay_bytes = cursors.replay_bytes.saturating_add(
        i64::try_from(chunk.data.len())
            .map_err(|_| PersistenceError::Mutation("output chunk is too large".to_owned()))?,
    );
    Ok(())
}

#[cfg(test)]
fn append_replay(
    transaction: &Connection,
    id: RunId,
    replay: &OutputReplay,
) -> Result<bool, PersistenceError> {
    let replay_file: String = transaction
        .query_row(
            "SELECT replay_file FROM runtime_meta WHERE singleton = 1",
            [],
            |row| row.get(0),
        )
        .map_err(PersistenceError::database)?;
    append_replay_external(transaction, id, replay, test_replay_dir(), &replay_file)
}

#[cfg(test)]
fn append_replay_external(
    transaction: &Connection,
    id: RunId,
    replay: &OutputReplay,
    replay_dir: &Path,
    replay_file: &str,
) -> Result<bool, PersistenceError> {
    append_replay_external_with_limit(
        transaction,
        id,
        replay,
        replay_dir,
        replay_file,
        PER_RUN_REPLAY_BYTES,
    )
}

fn append_replay_external_with_limit(
    transaction: &Connection,
    id: RunId,
    replay: &OutputReplay,
    replay_dir: &Path,
    replay_file: &str,
    replay_limit: u64,
) -> Result<bool, PersistenceError> {
    append_replay_with_storage(
        transaction,
        id,
        replay,
        replay_dir,
        replay_file,
        replay_limit,
    )
}

fn append_replay_with_storage(
    transaction: &Connection,
    id: RunId,
    replay: &OutputReplay,
    replay_dir: &Path,
    replay_file: &str,
    replay_limit: u64,
) -> Result<bool, PersistenceError> {
    let id_text = id.to_string();
    let (durable_oldest, durable_head, replay_bytes, state_kind): (
        i64,
        i64,
        i64,
        String,
    ) = transaction
        .prepare_cached(
            "SELECT durable_first_available_byte, durable_output_bytes, replay_bytes, state_kind
             FROM runs WHERE id = ?1",
        )
        .and_then(|mut statement| {
            statement.query_row([&id_text], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })
        })
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
    // `durable_head` advances as bytes are buffered, so between a buffered byte
    // and its flush the table is BEHIND the cursors. Every branch inside
    // `apply_replay_chunk` that reads the table flushes first, and the loop's
    // exit below flushes unconditionally.
    let mut cursors = ReplayCursors {
        durable_oldest,
        durable_head,
        replay_bytes,
        pending: Vec::new(),
        pending_start: 0,
        writer: ReplayFileWriter::open(replay_dir, replay_file)?,
    };
    for chunk in &replay.chunks {
        apply_replay_chunk(transaction, id, &id_text, replay, chunk, &mut cursors)?;
    }
    // Every reader below -- the pruner's index walk, and `validate_replay_window`
    // on the next open -- sees rows, not the buffer, so it must land first.
    flush_pending_row_for_cursors(transaction, &id_text, &mut cursors)?;
    let ReplayCursors {
        mut durable_oldest,
        durable_head,
        mut replay_bytes,
        pending: _,
        pending_start: _,
        mut writer,
    } = cursors;
    let evicted = prune_run_replay_to(
        transaction,
        id,
        &id_text,
        &mut durable_oldest,
        &mut replay_bytes,
        replay_limit,
    )?;
    let truncated = replay.truncated || durable_oldest > 0;
    transaction
        .prepare_cached(
            "UPDATE runs SET durable_first_available_byte = ?2, durable_output_bytes = ?3,
             replay_bytes = ?4, replay_truncated = ?5, updated_at_ms = ?6 WHERE id = ?1",
        )
        .and_then(|mut statement| {
            statement.execute(params![
                &id_text,
                durable_oldest,
                durable_head,
                replay_bytes,
                i64::from(truncated),
                now_millis(),
            ])
        })
        .map_err(PersistenceError::database)?;
    writer.commit();
    Ok(evicted)
}

/// Evict the oldest chunks until the Run is back inside `replay_limit`.
///
/// One range `DELETE` rather than a row at a time. The old shape ran three
/// statements per evicted chunk — find the oldest, delete it, then re-`SELECT
/// min(start_byte)` to recompute a floor the loop already knew — so shedding
/// the ~128 chunks a 1 MiB transaction admits cost ~384 statements against
/// ~128 inserts. Measured on the farm host, batching the eviction is worth
/// 1.79x of the SQL layer's CPU on its own.
///
/// Clip the exact contiguous prefix using indexed coordinates. Payload is
/// never loaded, and a large extent does not discard its funded suffix.
fn prune_run_replay_to(
    transaction: &Connection,
    id: RunId,
    id_text: &str,
    durable_oldest: &mut i64,
    replay_bytes: &mut i64,
    replay_limit: u64,
) -> Result<bool, PersistenceError> {
    if u64::try_from(*replay_bytes).unwrap_or(u64::MAX) <= replay_limit {
        return Ok(false);
    }
    let shed = replay_bytes.saturating_sub(i64::try_from(replay_limit).unwrap_or(i64::MAX));
    let surviving_front = durable_oldest
        .checked_add(shed)
        .ok_or_else(|| PersistenceError::Corrupt(format!("Run {id} replay floor overflows")))?;
    // Retained coordinates are contiguous. Clip a crossing extent before
    // deleting complete prefix extents; payload stays in the synced file.
    // Chunk boundaries must not discard bytes the configured window funds.
    transaction
        .execute(
            "UPDATE replay_chunks SET data_offset = data_offset + ?2 - start_byte,
         data_bytes = end_byte - ?2, start_byte = ?2
         WHERE run_id = ?1 AND start_byte < ?2 AND end_byte > ?2",
            params![id_text, surviving_front],
        )
        .map_err(PersistenceError::database)?;
    transaction
        .execute(
            "DELETE FROM replay_chunks WHERE run_id = ?1 AND end_byte <= ?2",
            params![id_text, surviving_front],
        )
        .map_err(PersistenceError::database)?;
    *replay_bytes -= shed;
    *durable_oldest = surviving_front;
    Ok(true)
}

/// Shed bytes across Runs until the daemon-wide replay total fits.
///
/// Eviction is by row in ordinal order — oldest bytes daemon-wide first — and a
/// Run's LAST row is never dropped, so no Run is emptied to serve another's
/// pressure.
///
/// Coalescing made that "never drop the last row" rule load-bearing in a way it
/// was not before. A Run used to hold hundreds of small rows, so there was
/// almost always one to drop; now a Run commonly holds a single 64 KiB row, and
/// if every Run holds one, no candidate exists at all and the ceiling cannot be
/// enforced. So the last row is not skipped, it is TRIMMED: its front is cut
/// back in place, which sheds exactly the bytes needed and keeps the Run's
/// window contiguous. A row is only ever shortened from the front, never
/// emptied, which is what preserves the invariant the skip was there to protect.
fn prune_global_replay_to(
    transaction: &Connection,
    replay_limit: u64,
) -> Result<bool, PersistenceError> {
    let mut evicted = false;
    loop {
        let total: i64 = transaction
            .prepare_cached("SELECT coalesce(sum(replay_bytes), 0) FROM runs")
            .and_then(|mut statement| statement.query_row([], |row| row.get(0)))
            .map_err(PersistenceError::database)?;
        if nonnegative_u64(total, "global replay bytes")? <= replay_limit {
            return Ok(evicted);
        }
        let candidate: Option<(i64, String, i64, i64)> = transaction
            .query_row(
                "SELECT chunk.ordinal, chunk.run_id, chunk.start_byte, chunk.data_bytes
                 FROM replay_chunks AS chunk
                 WHERE EXISTS(SELECT 1 FROM replay_chunks AS retained
                        WHERE retained.run_id = chunk.run_id AND retained.start_byte > chunk.start_byte)
                 ORDER BY chunk.ordinal LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()
            .map_err(PersistenceError::database)?;
        let Some((ordinal, run_id, _start_byte, bytes)) = candidate else {
            // Every Run is down to one row. Trim the globally-oldest row's
            // front in place instead of dropping it: the ceiling still has to
            // be met, and shortening a row sheds bytes without emptying a Run.
            return trim_oldest_row(transaction, replay_limit, evicted);
        };
        let needed = total - i64::try_from(replay_limit).unwrap_or(i64::MAX);
        let shed = bytes.min(needed);
        if shed == bytes {
            transaction
                .execute("DELETE FROM replay_chunks WHERE ordinal = ?1", [ordinal])
                .map_err(PersistenceError::database)?;
        } else {
            transaction
                .execute(
                    "UPDATE replay_chunks SET start_byte=start_byte+?2,
                data_offset=data_offset+?2, data_bytes=data_bytes-?2 WHERE ordinal=?1",
                    params![ordinal, shed],
                )
                .map_err(PersistenceError::database)?;
        }
        evicted = true;
        shed_run_bytes(transaction, &run_id, shed)?;
    }
}

/// Cut the front off the largest Run's row until the daemon-wide total fits.
///
/// The last resort for [`prune_global_replay_to`], reached when every Run holds
/// exactly one row and dropping any of them would empty a Run. Trimming in
/// place sheds the same bytes without that cost: the row keeps its tail, the
/// Run's window stays a single contiguous range, and `replay_truncated` records
/// the loss the same way every other eviction path does.
///
/// This orders by SIZE where the whole-row path orders by `ordinal`, and the
/// difference is forced rather than chosen. `ordinal` is an insertion counter,
/// so it ranks rows by age only as long as rows are whole; an in-place trim
/// leaves the ordinal untouched, so the row just trimmed still sorts oldest and
/// the next pass trims it again. Following age here therefore does not shed the
/// oldest bytes daemon-wide — it empties one Run's scrollback while every other
/// Run keeps all of its own.
///
/// Shedding the largest Run down to an equal share of the limit is the
/// max-min-fair alternative, and it terminates for the same reason it is fair:
/// the share is rounded DOWN, so whenever the total is over the limit the
/// largest Run is strictly above the share and every pass sheds at least one
/// byte. A row is never trimmed to nothing.
fn trim_oldest_row(
    transaction: &Connection,
    replay_limit: u64,
    mut evicted: bool,
) -> Result<bool, PersistenceError> {
    loop {
        let (total, runs): (i64, i64) = transaction
            .prepare_cached(
                "SELECT coalesce(sum(replay_bytes), 0), count(*) FROM runs WHERE replay_bytes > 0",
            )
            .and_then(|mut statement| statement.query_row([], |row| Ok((row.get(0)?, row.get(1)?))))
            .map_err(PersistenceError::database)?;
        if nonnegative_u64(total, "global replay bytes")? <= replay_limit || runs == 0 {
            return Ok(evicted);
        }
        let largest: Option<(i64, String, i64, i64, i64)> = transaction
            .query_row(
                "SELECT chunk.ordinal, chunk.run_id, chunk.start_byte, chunk.data_bytes,
                        chunk.data_offset
                 FROM replay_chunks AS chunk
                 JOIN runs ON runs.id = chunk.run_id
                 ORDER BY runs.replay_bytes DESC, chunk.ordinal LIMIT 1",
                [],
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
            .optional()
            .map_err(PersistenceError::database)?;
        let Some((ordinal, run_id, start_byte, bytes, data_offset)) = largest else {
            return Err(PersistenceError::Corrupt(
                "global replay accounting has no chunks".to_owned(),
            ));
        };
        // Fair shares may be empty when the aggregate policy funds fewer
        // bytes than Runs. No hidden one-byte floor can defeat the budget.
        let share =
            i64::try_from(replay_limit / nonnegative_u64(runs, "run count")?).unwrap_or(i64::MAX);
        let shed = (bytes - share)
            .min(bytes)
            .min(total - i64::try_from(replay_limit).unwrap_or(i64::MAX));
        if shed <= 0 {
            // Every Run is already at or below its share and still cannot be
            // trimmed further without emptying one. Stop rather than spin.
            return Ok(evicted);
        }
        if data_offset < 0 {
            return Err(PersistenceError::Corrupt(
                "replay row has no external offset".to_owned(),
            ));
        }
        if shed == bytes {
            transaction
                .execute("DELETE FROM replay_chunks WHERE ordinal = ?1", [ordinal])
                .map_err(PersistenceError::database)?;
        } else {
            transaction
                .execute(
                    "UPDATE replay_chunks SET start_byte = ?2, data_offset = ?3,
                 data_bytes = ?4 WHERE ordinal = ?1",
                    params![ordinal, start_byte + shed, data_offset + shed, bytes - shed],
                )
                .map_err(PersistenceError::database)?;
        }
        evicted = true;
        shed_run_bytes(transaction, &run_id, shed)?;
    }
}

/// Shed `shed` bytes from a Run's replay accounting after its stored bytes were
/// trimmed, and slide `durable_first_available_byte` up to the new oldest chunk.
///
/// This single statement is what keeps the durable window one contiguous range:
/// both eviction paths (whole-row drop in [`prune_global_replay_to`] and
/// in-place front trim in [`trim_oldest_row`]) must apply it identically, so it
/// lives here rather than being copied. `coalesce(..., 0)` handles the Run that
/// just lost its last chunk.
fn shed_run_bytes(
    transaction: &Connection,
    run_id: &str,
    shed: i64,
) -> Result<(), PersistenceError> {
    transaction
        .execute(
            "UPDATE runs SET replay_bytes = replay_bytes - ?2, replay_truncated = 1,
             durable_first_available_byte = coalesce(
               (SELECT min(start_byte) FROM replay_chunks WHERE run_id = ?1), durable_output_bytes
             ) WHERE id = ?1",
            params![run_id, shed],
        )
        .map_err(PersistenceError::database)?;
    Ok(())
}

fn read_run_head(transaction: &Connection, id: RunId) -> Result<u64, PersistenceError> {
    let value: i64 = transaction
        .prepare_cached("SELECT durable_output_bytes FROM runs WHERE id = ?1")
        .and_then(|mut statement| statement.query_row([id.to_string()], |row| row.get(0)))
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

/// Persist directory entries that are part of the replay generation protocol.
/// File `sync_all` makes payload bytes durable; the directory sync makes the
/// generation name durable before `SQLite` is allowed to publish it.
fn sync_directory(path: &Path) -> Result<(), PersistenceError> {
    File::open(path)
        .map_err(|source| PersistenceError::io(path, source))?
        .sync_all()
        .map_err(|source| PersistenceError::io(path, source))
}

fn validate_physical_limits(
    state_dir: &Path,
    replay_dir: &Path,
    database_path: &Path,
    wal_path: &Path,
    shm_path: &Path,
    limits: ResourceLimits,
) -> Result<(), PersistenceError> {
    let database = file_len(database_path)?;
    let wal = file_len(wal_path)?;
    let shm = file_len(shm_path)?;
    let replay = directory_file_len(replay_dir)?;
    if database > limits.database_bytes {
        return Err(PersistenceError::ResourcePressure(format!(
            "main database uses {database} bytes; policy funds {}",
            limits.database_bytes
        )));
    }
    if wal > limits.wal_bytes() {
        return Err(PersistenceError::ResourcePressure(format!(
            "WAL uses {wal} bytes; policy funds {}",
            limits.wal_bytes()
        )));
    }
    if shm > limits.shm_bytes() {
        return Err(PersistenceError::ResourcePressure(format!(
            "shared-memory index uses {shm} bytes; configured WAL funds {}",
            limits.shm_bytes()
        )));
    }
    if database
        .saturating_add(wal)
        .saturating_add(shm)
        .saturating_add(replay)
        > limits
            .state_file_bytes()
            .expect("validated storage arithmetic")
    {
        return Err(PersistenceError::ResourcePressure(format!(
            "state files in {} exceed the compaction-aware file budget",
            state_dir.display()
        )));
    }
    Ok(())
}

fn directory_file_len(path: &Path) -> Result<u64, PersistenceError> {
    let entries = fs::read_dir(path).map_err(|source| PersistenceError::io(path, source))?;
    let mut total = 0_u64;
    for entry in entries {
        let entry = entry.map_err(|source| PersistenceError::io(path, source))?;
        let child = entry.path();
        let metadata =
            fs::symlink_metadata(&child).map_err(|source| PersistenceError::io(&child, source))?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(PersistenceError::InvalidDirectory {
                path: child,
                message: "replay directory may contain only regular files".to_owned(),
            });
        }
        total = total.saturating_add(metadata.len());
    }
    Ok(total)
}

fn mutex_lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
fn test_replay_dir() -> &'static Path {
    static DIR: OnceLock<PathBuf> = OnceLock::new();
    DIR.get_or_init(|| {
        let path = std::env::temp_dir().join(format!("ctxmux-replay-tests-{}", std::process::id()));
        fs::create_dir_all(&path).expect("create test replay directory");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700))
            .expect("protect test replay directory");
        path
    })
    .as_path()
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, HashSet},
        env,
        fs::{self, OpenOptions},
        io,
        os::unix::{
            fs::{MetadataExt, OpenOptionsExt},
            process::ExitStatusExt,
        },
        path::{Path, PathBuf},
        process::{
            Child as ProcessChild, Command as ProcessCommand, Output as ProcessOutput, Stdio,
        },
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicU64, Ordering},
        },
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
        PersistentCandidate, PersistentStartCompletion, REPLAY_COMPACTION_CRASH_PHASE, REPLAY_DIR,
        SHM_MAX_BYTES, STATE_FILES_MAX_BYTES, StartCommitCrashPhase, StartDisposition,
        StartReceipt, StateLockGuard, StateStore, WAL_CHECKPOINT_BYTES, WAL_CHECKPOINT_MAX_RETRIES,
        WAL_IDLE_FOLD_FLOOR_BYTES, WAL_MAX_BYTES, append_replay, append_replay_external,
        create_schema, directory_file_len, file_len, idle_fold_wal, load_recovered, metadata_size,
        mutex_lock, nonnegative_u64, prune_global_replay_to, read_replay_segment,
        retry_transient_storage, retry_wal_checkpoint, validate_existing_schema,
        validate_replay_window, wal_charge_for_cache,
    };
    use crate::resources::ResourceLimits;

    /// A deliberately small explicit policy exercises canonical eviction and
    /// restartable WAL batches. This fixture is independent of the production
    /// defaults, which have no retained-record population ceiling.
    const EVICTION_TEST_CEILING: usize = 128;

    /// Assert an `append` was queued.
    ///
    /// Every fixture append below runs against an otherwise idle actor, so a
    /// refusal there is a broken fixture, not the behavior under test — and a
    /// silently dropped fixture append would leave the *next* assertion reading
    /// a log that was never written, which reads as a product bug. The one
    /// place a refusal is expected is the queue-saturation loop, which ignores
    /// the result explicitly.
    /// The fold floor as a payload length, for fixtures that must carry the WAL
    /// past it. Written out rather than cast from [`WAL_IDLE_FOLD_FLOOR_BYTES`]
    /// so no fixture needs a lossy conversion; the assertion below is what keeps
    /// the two from drifting apart.
    const FOLD_FLOOR_PAYLOAD: usize = 256 * 1024;
    const _: () = assert!(
        FOLD_FLOOR_PAYLOAD as u64 == WAL_IDLE_FOLD_FLOOR_BYTES,
        "the fixture payload floor drifted from the fold floor it is meant to clear"
    );

    #[track_caller]
    fn expect_queued(accepted: bool) {
        assert!(
            accepted,
            "the persistence queue refused a fixture append; the fixture, not \
             the code under test, is at fault"
        );
    }

    /// The eviction fixtures' explicit serving admission limits: a small row
    /// ceiling with the production metadata budget.
    const EVICTION_TEST_LIMITS: AdmissionLimits = AdmissionLimits {
        run_records: EVICTION_TEST_CEILING as u64,
        metadata_bytes: METADATA_BYTES,
        resources: ResourceLimits::DEFAULT,
    };

    const COMMIT_CRASH_STATE_DIR: &str = "CTXMUX_COMMIT_CRASH_STATE_DIR";
    const COMMIT_CRASH_PHASE: &str = "CTXMUX_COMMIT_CRASH_PHASE";
    const COMMIT_CRASH_NEW_ID: &str = "CTXMUX_COMMIT_CRASH_NEW_ID";
    const COMMIT_CRASH_NEW_KEY: &str = "CTXMUX_COMMIT_CRASH_NEW_KEY";
    const COMMIT_CRASH_ROLE: &str = "CTXMUX_COMMIT_CRASH_ROLE";
    const REPLAY_COMPACTION_CRASH_STATE_DIR: &str = "CTXMUX_REPLAY_COMPACTION_CRASH_STATE_DIR";
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
    fn default_policy_budgets_have_consistent_format_arithmetic() {
        assert_eq!(PAGE_SIZE_BYTES, 4 * 1024);
        assert_eq!(PER_RUN_REPLAY_BYTES, 4 * 1024 * 1024);
        assert_eq!(GLOBAL_REPLAY_BYTES, 256 * 1024 * 1024);
        assert_eq!(METADATA_BYTES, 64 * 1024 * 1024);
        assert_eq!(PERSISTENCE_QUEUE_CAPACITY, 16);
        assert_eq!(DATABASE_MAX_BYTES, 384 * 1024 * 1024);
        assert_eq!(WAL_MAX_BYTES, 16 * 1024 * 1024);
        assert_eq!(SHM_MAX_BYTES, 64 * 1024);
        const {
            assert!(
                DATABASE_MAX_BYTES + WAL_MAX_BYTES + SHM_MAX_BYTES + GLOBAL_REPLAY_BYTES
                    <= STATE_FILES_MAX_BYTES
            );
        }
        let worst_admitted_output =
            u64::try_from(MAX_TRANSACTION_PAYLOAD_BYTES).expect("payload limit fits u64") * 4
                + 1024 * 1024;
        assert!(worst_admitted_output <= WAL_CHECKPOINT_BYTES);
    }

    #[test]
    fn retention_population_policy_is_independent_of_live_descriptors() {
        assert_eq!(ResourceLimits::DEFAULT.retained_runs, None);
        assert_eq!(AdmissionLimits::OPERATIONAL.run_records, u64::MAX);
        let limits = ResourceLimits {
            retained_runs: Some(50_000),
            live_runs: Some(12_000),
            ..ResourceLimits::DEFAULT
        };
        assert_eq!(AdmissionLimits::from(limits).run_records, 50_000);
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
        create_schema(&connection, &epoch, "replay-test.bin")
            .expect("create schema with a valid bootstrap epoch");
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
        let temp = TempDir::new().unwrap();
        let state_dir = temp.path().join("state");
        let seeded = seed_startup_overflow(&state_dir);
        let before = raw_run_units(&state_dir);
        let Err(error) = StateStore::open(
            &state_dir,
            &EVICTION_TEST_LIMITS,
            None,
            Arc::new(PersistenceTestHooks::default()),
        ) else {
            panic!("smaller policy opened");
        };
        assert!(matches!(error, PersistenceError::ResourcePressure(_)));
        assert_eq!(
            raw_run_units(&state_dir),
            before,
            "policy pressure must not purge or reconcile history"
        );
        let hooks = Arc::new(PersistenceTestHooks::default());
        hooks.startup_fail_after_commits.store(1, Ordering::Release);
        let Err(error) = StateStore::open(
            &state_dir,
            &AdmissionLimits::OPERATIONAL,
            None,
            Arc::clone(&hooks),
        ) else {
            panic!("injected interruption opened");
        };
        assert!(error.to_string().contains("injected interruption"));
        assert!(
            mutex_lock(&hooks.startup_batch_wal_bytes)
                .iter()
                .all(|bytes| *bytes <= WAL_CHECKPOINT_BYTES)
        );
        let (persistence, recovered) = Persistence::open(&state_dir).unwrap();
        assert_eq!(recovered.len(), seeded.len());
        assert_eq!(
            recovered
                .iter()
                .map(|run| (run.info.id, run.operation_key.clone()))
                .collect::<Vec<_>>(),
            seeded
        );
        assert_eq!(
            recovered.last().unwrap().info.state,
            RunState::Interrupted {
                reason: InterruptionReason::DaemonRestart
            }
        );
        assert!(
            persistence
                .startup_batch_wal_bytes()
                .iter()
                .all(|bytes| *bytes <= WAL_CHECKPOINT_BYTES)
        );
        persistence.assert_exclusive_owner();
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
        assert_eq!(recovered.len(), seeded.len());
        let expected = &seeded;
        assert_eq!(
            recovered
                .iter()
                .map(|run| (run.info.id, run.operation_key.clone()))
                .collect::<Vec<_>>(),
            expected.clone()
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
        validate_existing_schema(&connection).expect("accept canonical schema 5 index");

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
            let expected = if id == first {
                (3, 6, 3, 1)
            } else {
                (2, 6, 4, 1)
            };
            assert_eq!((oldest, head, bytes, truncated), expected);
        }
        transaction.commit().expect("commit replay pruning");
    }

    /// The daemon-wide ceiling must still be enforceable when every Run holds
    /// exactly one row, and must not be paid by a single Run.
    ///
    /// Coalescing made this the ordinary case. The whole-row evictor skips a
    /// Run's last row so no Run is emptied for another's pressure — with one
    /// row per Run that leaves no candidate at all, and the ceiling silently
    /// stops being enforced (it returned `Corrupt`).
    ///
    /// Two assertions, and the second is the one that bites: the total must
    /// come under the limit, AND no Run may be gutted to get there. Trimming
    /// the globally-oldest row looks right and fails the second — `ordinal` is
    /// an insertion counter that an in-place trim does not change, so the same
    /// row stays "oldest" and is trimmed again and again down to one byte while
    /// its neighbour keeps everything.
    #[test]
    fn the_global_ceiling_is_met_without_gutting_one_run() {
        let mut connection = test_connection();
        let first = RunId::new();
        let second = RunId::new();
        let transaction = connection.transaction().expect("start replay transaction");
        insert_test_run(&transaction, first, "running", 1);
        insert_test_run(&transaction, second, "running", 1);
        // One row each: 600 B, well inside COALESCE_ROW_BYTES.
        let payload = [b'z'; 600];
        for id in [first, second] {
            append_replay(&transaction, id, &replay(vec![chunk(0, &payload)]))
                .expect("append single coalesced row");
        }
        let rows: i64 = transaction
            .query_row("SELECT count(*) FROM replay_chunks", [], |row| row.get(0))
            .expect("count rows");
        assert_eq!(rows, 2, "the fixture must be one row per Run");

        assert!(prune_global_replay_to(&transaction, 800).expect("prune under the ceiling"));

        let total: i64 = transaction
            .query_row(
                "SELECT coalesce(sum(replay_bytes), 0) FROM runs",
                [],
                |row| row.get(0),
            )
            .expect("read global total");
        assert!(total <= 800, "the ceiling must be met, got {total}");
        for id in [first, second] {
            let (oldest, head, bytes, truncated): (i64, i64, i64, i64) = transaction
                .query_row(
                    "SELECT durable_first_available_byte, durable_output_bytes, replay_bytes,
                            replay_truncated FROM runs WHERE id = ?1",
                    [id.to_string()],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .expect("read trimmed accounting");
            assert!(
                bytes >= 300,
                "shedding must be shared, but one Run was cut to {bytes} B"
            );
            assert_eq!(head, 600, "trimming the front must not move the head");
            assert_eq!(oldest, 600 - bytes, "the window floor must follow the trim");
            assert_eq!(truncated, 1, "a trimmed Run must be marked truncated");
        }
        transaction.commit().expect("commit the trim");
    }

    #[test]
    fn a_gap_the_producer_proves_unrecoverable_is_recorded_rather_than_refused() {
        // REGRESSION GUARD for a daemon-wide persistence latch reproduced on a
        // farm host: `durable replay gap: got 18451664, expected 17087786`,
        // hit by EVERY create once a chatty 32-Run fleet was running.
        //
        // Root cause: `OutputLog::trim_front` sheds a Run's oldest chunks to
        // hold the frozen per-Run retention ceiling, and the daemon-wide
        // reclaimer aims it at OTHER Runs under memory pressure. If it evicts
        // bytes a Run still owes as a catch-up, no replay can ever reproduce
        // them -- so refusing the offer loses exactly the same bytes AND
        // latches persistence off for every Run in the daemon.
        //
        // Holding the bytes instead (a floor on eviction) was rejected: the
        // per-Run retention ceiling is a frozen reliability contract, and a
        // lagging actor would push retention straight through it.
        let mut connection = test_connection();
        let id = RunId::new();
        let transaction = connection.transaction().expect("start replay transaction");
        insert_test_run(&transaction, id, "running", 1);
        append_replay(&transaction, id, &replay(vec![chunk(0, b"aaa")]))
            .expect("seed the durable head");

        // The reclaimer ate [3, 9) while it was still owed. What survives
        // starts at 9, and the producer says so: the offer begins exactly at
        // its log's surviving front, and it is marked truncated.
        let evicted = OutputReplay {
            chunks: vec![chunk(9, b"ddd")],
            first_available_byte: 9,
            latest_output_bytes: 12,
            truncated: true,
        };
        append_replay(&transaction, id, &evicted).expect("an unrecoverable gap must be admitted");

        let (oldest, head, truncated): (i64, i64, i64) = transaction
            .query_row(
                "SELECT durable_first_available_byte, durable_output_bytes, replay_truncated
                 FROM runs WHERE id = ?1",
                [id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("read accounting after the admitted gap");
        assert_eq!(
            head, 12,
            "the head must follow the surviving bytes; leaving it behind makes \
             every later offer look like a gap too"
        );
        assert_eq!(
            truncated, 1,
            "an admitted gap MUST be recorded -- a reader that cannot tell this \
             from continuous output is worse than the refusal this replaced"
        );
        assert_eq!(
            oldest, 9,
            "admitting the gap must move the floor to the surviving front: the \
             bytes below it are unreachable, and leaving them behind puts an \
             interior hole in a window the recovery validator requires to be \
             contiguous"
        );

        // The half that the immediate row state cannot show: a daemon restart
        // must still accept this state. `validate_replay_window` walks the
        // chunks demanding strict contiguity from `oldest`, and `open()` has no
        // fallback for `Corrupt` -- an interior hole here is not a degraded
        // read, it is a daemon that will not start.
        let (oldest, head, replay_bytes, truncated): (i64, i64, i64, i64) = transaction
            .query_row(
                "SELECT durable_first_available_byte, durable_output_bytes, replay_bytes,
                        replay_truncated FROM runs WHERE id = ?1",
                [id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .expect("re-read accounting for the recovery check");
        transaction.commit().expect("commit the admitted gap");
        validate_replay_window(
            &connection,
            id,
            nonnegative_u64(oldest, "oldest").expect("oldest fits"),
            nonnegative_u64(head, "head").expect("head fits"),
            nonnegative_u64(replay_bytes, "replay bytes").expect("bytes fit"),
            truncated != 0,
        )
        .expect("an admitted gap must survive a restart's replay validation");
    }

    #[test]
    fn a_gap_the_producer_could_still_close_is_refused() {
        // The other half of the rule above, and the one that keeps it honest.
        //
        // Admitting a gap is safe ONLY because the producer proves the bytes
        // are gone: the offer starts exactly at its log's surviving front. A
        // chunk starting ABOVE `first_available_byte` means the producer STILL
        // HOLDS the missing bytes and simply failed to send them -- a real
        // contiguity bug. That must keep failing loudly, or the fix above
        // degrades into "accept any gap" and silently masks data loss.
        let mut connection = test_connection();
        let id = RunId::new();
        let transaction = connection.transaction().expect("start replay transaction");
        insert_test_run(&transaction, id, "running", 1);
        append_replay(&transaction, id, &replay(vec![chunk(0, b"aaa")]))
            .expect("seed the durable head");

        let still_holds_them = OutputReplay {
            chunks: vec![chunk(9, b"ddd")],
            // The log still retains from byte 3 -- [3, 9) is recoverable.
            first_available_byte: 3,
            latest_output_bytes: 12,
            truncated: true,
        };
        let Err(error) = append_replay(&transaction, id, &still_holds_them) else {
            panic!("a recoverable gap must still be refused");
        };
        assert!(
            matches!(error, PersistenceError::Mutation(message)
                if message.contains("durable replay gap")),
            "the refusal must stay the contiguity error, not a new one"
        );
    }

    /// An empty replay rides along in the open transaction instead of splitting it.
    ///
    /// Every create ends in `activate_persistence_after_publication`, which
    /// appends `replay(0)` unconditionally. On a Run that has produced nothing
    /// -- the common case, since activation happens microseconds after spawn --
    /// that replay is empty.
    ///
    /// The empty COMMIT itself is cheap and was measured so: 0.043 ms against
    /// 0.303 ms for the same path carrying three bytes. That measurement is
    /// what rejected guarding the CALL SITE, and it still holds. What it did
    /// not price is the arm that used to handle the empty replay HERE: it
    /// flushed whatever was already collected before opening its own
    /// transaction, so an empty replay arriving mid-drain split the surrounding
    /// appends into separate fsyncs. Those carry real output -- 1.5-2.9 ms each
    /// at `synchronous=FULL` on the farm -- so the cost was never the empty
    /// commit, it was the split.
    ///
    /// Both halves are asserted: the empty replay alone still reaches exactly
    /// one commit (it is not skipped -- its `truncated` and
    /// `first_available_byte` still have to land), and an empty replay BETWEEN
    /// two appends no longer multiplies the commit count.
    #[test]
    fn an_empty_replay_joins_the_open_transaction() {
        let temp = TempDir::new().expect("create empty-append fixture");
        let state_dir = temp.path().join("state");
        let hooks = Arc::new(PersistenceTestHooks::default());
        let (mut store, recovered) = StateStore::open(
            &state_dir,
            &AdmissionLimits::OPERATIONAL,
            None,
            Arc::clone(&hooks),
        )
        .expect("open empty-append store");
        assert!(recovered.is_empty());
        let id = RunId::new();
        let transaction = store
            .connection
            .transaction()
            .expect("start empty-append fixture transaction");
        insert_test_run(&transaction, id, "running", 1);
        transaction
            .commit()
            .expect("commit empty-append fixture Run");

        hooks.append_transaction_commits.store(0, Ordering::Release);
        let head = Arc::new(std::sync::atomic::AtomicU64::new(0));
        store
            .append_batch(&[(id, replay(Vec::new()), Arc::clone(&head))])
            .expect("append an empty replay");

        assert_eq!(
            hooks.append_transaction_commits.load(Ordering::Acquire),
            1,
            "an empty replay reaches a real COMMIT (measured cheap: 0.043 ms \
             vs 0.303 ms carrying bytes)"
        );
        assert_eq!(head.load(Ordering::Acquire), 0, "no bytes became durable");

        // The regression this guards: the same empty replay sandwiched between
        // two contiguous appends. All three are one contiguous run of bytes for
        // one Run, so they belong in ONE transaction -- the empty one in the
        // middle must not split them into three.
        let bystander = RunId::new();
        let transaction = store
            .connection
            .transaction()
            .expect("start bystander fixture transaction");
        insert_test_run(&transaction, bystander, "running", 1);
        transaction.commit().expect("commit bystander fixture Run");

        hooks.append_transaction_commits.store(0, Ordering::Release);
        let bystander_head = Arc::new(std::sync::atomic::AtomicU64::new(0));
        store
            .append_batch(&[
                (
                    bystander,
                    replay(vec![chunk(0, b"aaa")]),
                    Arc::clone(&bystander_head),
                ),
                (id, replay(Vec::new()), Arc::clone(&head)),
                (
                    bystander,
                    replay(vec![chunk(3, b"bbb")]),
                    Arc::clone(&bystander_head),
                ),
            ])
            .expect("append around an empty replay");

        assert_eq!(
            hooks.append_transaction_commits.load(Ordering::Acquire),
            1,
            "an empty replay must not split the transaction it landed in"
        );
        assert_eq!(
            bystander_head.load(Ordering::Acquire),
            6,
            "both real appends still became durable"
        );
    }

    #[test]
    fn append_batch_commits_one_collected_payload_unit() {
        let temp = TempDir::new().expect("create append-batch fixture");
        let state_dir = temp.path().join("state");
        let hooks = Arc::new(PersistenceTestHooks::default());
        let (mut store, recovered) = StateStore::open(
            &state_dir,
            &AdmissionLimits::OPERATIONAL,
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
    fn transient_disk_full_retries_append_before_later_mutations() {
        let temp = TempDir::new().expect("create append DiskFull fixture");
        let state_dir = temp.path().join("state");
        let (persistence, recovered) = Persistence::open(&state_dir).expect("open persistence");
        assert!(recovered.is_empty());

        let first = running_info(RunId::new());
        let durable = persistence
            .insert_start(&test_operation_key(first.id), &first)
            .expect("insert append DiskFull fixture");
        let first_replay = replay(vec![chunk(0, b"survives DiskFull")]);
        persistence.fail_next_append_as_disk_full();
        expect_queued(durable.append(first.id, first_replay.clone()));
        durable.finalize(first.id, 42, first_replay.clone(), exited_state());

        assert!(!persistence.is_failed());
        assert_eq!(
            durable.durable_head(),
            first_replay.latest_output_bytes,
            "the queued finalize must run after the retried append"
        );
        let later = running_info(RunId::new());
        let later_durable = persistence
            .insert_start(&test_operation_key(later.id), &later)
            .expect("later start remains writable after recovered DiskFull");
        later_durable.finalize(later.id, 43, replay(Vec::new()), exited_state());

        drop(later_durable);
        drop(durable);
        persistence.assert_exclusive_owner();
        drop(persistence);
        let (reopened, recovered) = Persistence::open(state_dir).expect("reopen recovered state");
        let recovered_first = recovered
            .iter()
            .find(|run| run.info.id == first.id)
            .expect("first Run remains durable");
        assert_eq!(recovered_first.info.state, exited_state());
        assert_eq!(recovered_first.replay, first_replay);
        assert!(recovered.iter().any(|run| run.info.id == later.id));
        drop(reopened);
    }

    #[test]
    fn transient_disk_full_retries_finalize_without_poisoning_actor() {
        let temp = TempDir::new().expect("create finalize DiskFull fixture");
        let state_dir = temp.path().join("state");
        let (persistence, recovered) = Persistence::open(&state_dir).expect("open persistence");
        assert!(recovered.is_empty());

        let info = running_info(RunId::new());
        let durable = persistence
            .insert_start(&test_operation_key(info.id), &info)
            .expect("insert finalize DiskFull fixture");
        let output = replay(vec![chunk(0, b"terminal output")]);
        persistence.fail_next_finalize_as_disk_full();
        durable.finalize(info.id, 44, output.clone(), exited_state());

        assert!(!persistence.is_failed());
        assert_eq!(durable.durable_head(), output.latest_output_bytes);
        drop(durable);
        persistence.assert_exclusive_owner();
        drop(persistence);
        let (reopened, recovered) = Persistence::open(state_dir).expect("reopen finalized state");
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].info.state, exited_state());
        assert_eq!(recovered[0].info.pid, Some(44));
        assert_eq!(recovered[0].replay, output);
        drop(reopened);
    }

    #[test]
    fn sqlite_io_error_is_visible_and_reopen_recovers_the_isolated_state() {
        let temp = TempDir::new().expect("create SQLite I/O error fixture");
        let state_dir = temp.path().join("state");
        let (persistence, recovered) = Persistence::open(&state_dir).expect("open persistence");
        assert!(recovered.is_empty());

        let first = running_info(RunId::new());
        let durable = persistence
            .insert_start(&test_operation_key(first.id), &first)
            .expect("insert SQLite I/O error fixture Run");
        let output = replay(vec![chunk(0, b"must not be acknowledged")]);
        persistence.fail_next_append_as_io_error();
        expect_queued(durable.append(first.id, output));

        let error = persistence
            .barrier()
            .expect_err("injected SQLite I/O error must reach the barrier");
        assert!(error.to_string().contains("disk I/O error"));
        assert!(
            persistence.is_failed(),
            "non-transient I/O must latch this actor"
        );

        let later = running_info(RunId::new());
        let rejected = persistence.insert_start(&test_operation_key(later.id), &later);
        assert!(
            rejected.is_err(),
            "a latched actor must reject mutations until its daemon is reopened"
        );

        drop(durable);
        persistence.assert_exclusive_owner();
        drop(persistence);

        // Reopening the same isolated state models a daemon restart. The failed
        // append was never acknowledged, while the committed start remains
        // recoverable and the new actor accepts subsequent mutations.
        let (reopened, recovered) =
            Persistence::open(&state_dir).expect("reopen the isolated state after daemon restart");
        let recovered_first = recovered
            .iter()
            .find(|run| run.info.id == first.id)
            .expect("the committed Run remains durable after the injected I/O error");
        assert_eq!(recovered_first.replay, replay(Vec::new()));
        assert!(matches!(
            recovered_first.info.state,
            RunState::Interrupted {
                reason: InterruptionReason::DaemonRestart
            }
        ));

        let later_durable = reopened
            .insert_start(&test_operation_key(later.id), &later)
            .expect("a fresh persistence actor accepts a new mutation");
        later_durable.finalize(later.id, 43, replay(Vec::new()), exited_state());
        drop(later_durable);
        reopened.assert_exclusive_owner();
        drop(reopened);
    }

    #[test]
    fn disk_full_retry_observes_shutdown() {
        let shutdown = AtomicBool::new(false);
        let mut attempts = 0_u8;
        let result = retry_transient_storage(&shutdown, || {
            attempts = attempts.saturating_add(1);
            shutdown.store(true, Ordering::Release);
            Err(PersistenceError::injected_disk_full())
        });
        assert!(result.is_none());
        assert_eq!(attempts, 1);
    }

    #[test]
    fn wal_checkpoint_retries_a_transient_busy_result_until_zero() {
        let mut checkpoint_calls = 0_u8;
        let mut wal_lengths = [4096_u64, 4096, 0].into_iter();
        let result = retry_wal_checkpoint(
            None,
            || {
                checkpoint_calls = checkpoint_calls.saturating_add(1);
                Ok(match checkpoint_calls {
                    1 | 2 => (1, 32, 0),
                    3 => (0, 0, 0),
                    other => panic!("unexpected checkpoint attempt {other}"),
                })
            },
            || Ok(wal_lengths.next().expect("one WAL length per checkpoint")),
        );

        assert!(result.is_ok());
        assert_eq!(checkpoint_calls, 3);
    }

    #[test]
    fn wal_checkpoint_retries_a_typed_busy_error_but_not_other_errors() {
        let mut busy_calls = 0_u8;
        let busy_result = retry_wal_checkpoint(
            None,
            || {
                busy_calls = busy_calls.saturating_add(1);
                if busy_calls == 1 {
                    Err(sqlite_busy_error())
                } else {
                    Ok((0, 0, 0))
                }
            },
            || Ok(0),
        );
        assert!(busy_result.is_ok());
        assert_eq!(busy_calls, 2);

        let mut fatal_calls = 0_u8;
        let fatal_result = retry_wal_checkpoint(
            None,
            || {
                fatal_calls = fatal_calls.saturating_add(1);
                Err(PersistenceError::injected_disk_full())
            },
            || Ok(0),
        );
        assert!(matches!(
            fatal_result,
            Err(PersistenceError::Database {
                code: Some(rusqlite::ErrorCode::DiskFull),
                ..
            })
        ));
        assert_eq!(fatal_calls, 1);
    }

    #[test]
    fn wal_checkpoint_exhaustion_is_bounded_and_fail_closed() {
        let started = Instant::now();
        let mut calls = 0_usize;
        let result = retry_wal_checkpoint(
            None,
            || {
                calls += 1;
                Ok((1, 16, 0))
            },
            || Ok(WAL_CHECKPOINT_BYTES),
        );

        let Err(PersistenceError::WalCheckpointBusy { detail, .. }) = result else {
            panic!("checkpoint exhaustion must fail closed as retryable storage pressure");
        };
        assert!(detail.contains("busy=1"));
        assert_eq!(calls, WAL_CHECKPOINT_MAX_RETRIES + 1);
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "checkpoint retry exceeded its bounded budget"
        );
    }

    #[test]
    fn wal_checkpoint_retry_stops_when_shutdown_is_requested() {
        let shutdown = AtomicBool::new(false);
        let mut calls = 0_u8;
        let result = retry_wal_checkpoint(
            Some(&shutdown),
            || {
                calls = calls.saturating_add(1);
                shutdown.store(true, Ordering::Release);
                Ok((1, 1, 0))
            },
            || Ok(WAL_CHECKPOINT_BYTES),
        );

        assert!(matches!(result, Err(PersistenceError::ActorStopped)));
        assert_eq!(calls, 1);
    }

    #[test]
    fn persistence_actor_writes_replay_without_filling_the_main_wal() {
        let temp = TempDir::new().expect("create replay file fixture");
        let state_dir = temp.path().join("state");
        let (persistence, recovered) = Persistence::open(&state_dir).expect("open persistence");
        assert!(recovered.is_empty());
        let info = running_info(RunId::new());
        let durable = persistence
            .insert_start(&test_operation_key(info.id), &info)
            .expect("insert replay fixture Run");
        let payload = vec![b'x'; 4 * FOLD_FLOOR_PAYLOAD];
        expect_queued(durable.append(info.id, replay(vec![chunk(0, &payload)])));
        persistence.barrier().expect("drain replay append");
        let replay_bytes = directory_file_len(&state_dir.join(REPLAY_DIR))
            .expect("measure external replay payload");
        assert!(replay_bytes >= payload.len() as u64);
        assert!(!persistence.is_failed());
    }

    #[test]
    fn global_retention_sheds_exact_excess_across_large_and_single_extents() {
        for (payload_bytes, added_bytes, limit) in [(65_536, 1, 65_536), (100, 1, 100)] {
            let mut connection = test_connection();
            let transaction = connection.transaction().unwrap();
            let first = RunId::new();
            insert_test_run(&transaction, first, "running", 1);
            append_replay(
                &transaction,
                first,
                &replay(vec![chunk(0, &vec![b'a'; payload_bytes])]),
            )
            .unwrap();
            let second = if payload_bytes == 100 {
                let id = RunId::new();
                insert_test_run(&transaction, id, "running", 1);
                id
            } else {
                first
            };
            let next_start = if second == first {
                payload_bytes as u64
            } else {
                0
            };
            append_replay(
                &transaction,
                second,
                &replay(vec![chunk(next_start, &vec![b'b'; added_bytes])]),
            )
            .unwrap();
            assert!(prune_global_replay_to(&transaction, limit).unwrap());
            let retained: i64 = transaction
                .query_row("SELECT sum(replay_bytes) FROM runs", [], |row| row.get(0))
                .unwrap();
            assert_eq!(
                retained,
                i64::try_from(limit).unwrap(),
                "extent/fair-share boundaries cannot discard funded bytes"
            );
            let (oldest, head): (i64,i64) = transaction.query_row("SELECT durable_first_available_byte,durable_output_bytes FROM runs WHERE id=?1", [first.to_string()], |row| Ok((row.get(0)?,row.get(1)?))).unwrap();
            assert_eq!(oldest, 1);
            let chunks = super::load_replay_chunks_range(
                &transaction,
                super::test_replay_dir(),
                &first.to_string(),
                1,
                u64::try_from(head).unwrap(),
            )
            .unwrap();
            let mut expected = vec![b'a'; payload_bytes - 1];
            if second == first {
                expected.push(b'b');
            }
            assert_eq!(
                chunks
                    .iter()
                    .flat_map(|chunk| chunk.data.iter().copied())
                    .collect::<Vec<_>>(),
                expected
            );
        }
    }

    #[test]
    fn malformed_source_facts_are_rejected_before_any_recovery_publication() {
        for cursor in [
            serde_json::json!(-1),
            serde_json::json!(1),
            serde_json::json!("zero"),
        ] {
            let temp = TempDir::new().unwrap();
            let state_dir = temp.path().join("state");
            let (mut store, _) = StateStore::open(
                &state_dir,
                &AdmissionLimits::OPERATIONAL,
                None,
                Arc::new(PersistenceTestHooks::default()),
            )
            .unwrap();
            let id = RunId::new();
            let transaction = store.connection.transaction().unwrap();
            let metadata = insert_test_run(&transaction, id, "running", 0);
            let state_json =
                serde_json::json!({ "type": "running", "ctxmux_source_gap_after_byte": cursor })
                    .to_string();
            let previous: String = transaction
                .query_row(
                    "SELECT state_json FROM runs WHERE id=?1",
                    [id.to_string()],
                    |row| row.get(0),
                )
                .unwrap();
            let metadata = metadata + i64::try_from(state_json.len()).unwrap()
                - i64::try_from(previous.len()).unwrap();
            transaction
                .execute(
                    "UPDATE runs SET state_json=?2, metadata_bytes=?3 WHERE id=?1",
                    params![id.to_string(), state_json, metadata],
                )
                .unwrap();
            transaction.commit().unwrap();
            store.truncate_wal_to_zero_with_shutdown(None).unwrap();
            let before: (String, String) = store
                .connection
                .query_row(
                    "SELECT (SELECT current_epoch FROM runtime_meta), state_json FROM runs",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            let replay = fs::read(store.replay_dir.join(&store.replay_file)).unwrap();
            drop(store);
            assert!(matches!(
                StateStore::open(
                    &state_dir,
                    &AdmissionLimits::OPERATIONAL,
                    None,
                    Arc::new(PersistenceTestHooks::default())
                ),
                Err(PersistenceError::Corrupt(message)) if message == "invalid durable source-gap cursor"
            ));
            let connection = Connection::open(state_dir.join(DATABASE_FILE)).unwrap();
            let after: (String, String) = connection
                .query_row(
                    "SELECT (SELECT current_epoch FROM runtime_meta), state_json FROM runs",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            assert_eq!(
                before, after,
                "invalid facts cannot be reconciled away or publish a fresh epoch"
            );
            assert_eq!(
                fs::read(
                    fs::read_dir(state_dir.join(REPLAY_DIR))
                        .unwrap()
                        .next()
                        .unwrap()
                        .unwrap()
                        .path()
                )
                .unwrap(),
                replay
            );
        }
    }

    #[test]
    fn immutable_spec_recovery_has_the_same_resident_capacity_as_admission() {
        let mut info = running_info(RunId::new());
        let spec = info.spec.as_mut().unwrap();
        spec.args = vec![String::new(); 257];
        let compact = spec.clone();
        let recovered =
            super::decode_native_spec(info.id, &serde_json::to_string(&compact).unwrap()).unwrap();
        assert_eq!(
            crate::resident_spec_bytes(&recovered),
            crate::resident_spec_bytes(&compact),
            "deserialize growth slack cannot invalidate an admitted resource policy"
        );
    }

    #[test]
    fn valid_history_above_old_record_caps_survives_cold_recovery() {
        let temp = TempDir::new().unwrap();
        let state_dir = temp.path().join("state");
        let (mut store, _) = StateStore::open(
            &state_dir,
            &AdmissionLimits::OPERATIONAL,
            None,
            Arc::new(PersistenceTestHooks::default()),
        )
        .unwrap();
        let transaction = store.connection.transaction().unwrap();
        // Holdout population crosses both old 4000/4096 ceilings. It does not
        // set a new production target or change any qualification workload.
        let mut ids = HashSet::new();
        for _ in 0..5003 {
            let id = RunId::new();
            ids.insert(id);
            let metadata = insert_test_run(&transaction, id, "exited", 0);
            transaction
                .execute(
                    "UPDATE runs SET metadata_bytes = ?2 WHERE id = ?1",
                    params![id.to_string(), metadata],
                )
                .unwrap();
        }
        transaction.commit().unwrap();
        store.truncate_wal_to_zero_with_shutdown(None).unwrap();
        drop(store);
        let (_, recovered) = StateStore::open(
            &state_dir,
            &AdmissionLimits::OPERATIONAL,
            None,
            Arc::new(PersistenceTestHooks::default()),
        )
        .expect("valid large history is not corrupt or evicted");
        assert_eq!(
            recovered
                .iter()
                .map(|run| run.info.id)
                .collect::<HashSet<_>>(),
            ids
        );
        assert!(
            recovered
                .iter()
                .all(|run| matches!(run.info.state, RunState::Exited { code: 0, .. }))
        );
    }

    #[test]
    fn fragmented_prefix_reclamation_respects_configured_wal_and_exact_bytes() {
        let temp = TempDir::new().unwrap();
        let state_dir = temp.path().join("state");
        // Different extent count and page budget from the compaction fixture:
        // retention must fund dirty index pages, not estimate them from payload.
        let extents = 121_733_i64;
        let resources = ResourceLimits {
            wal_checkpoint_bytes: 1024 * 1024,
            durable_run_output_bytes: u64::try_from(extents).unwrap(),
            ..ResourceLimits::DEFAULT
        };
        let limits = AdmissionLimits::from(resources);
        let (mut store, _) = StateStore::open(
            &state_dir,
            &limits,
            None,
            Arc::new(PersistenceTestHooks::default()),
        )
        .unwrap();
        let id = RunId::new();
        fs::write(
            store.replay_dir.join(&store.replay_file),
            vec![b'x'; usize::try_from(extents).unwrap()],
        )
        .unwrap();
        let transaction = store.connection.transaction().unwrap();
        let metadata = insert_test_run(&transaction, id, "running", 0);
        {
            let mut insert = transaction.prepare("INSERT INTO replay_chunks (run_id, start_byte, end_byte, data_file, data_offset, data_bytes) VALUES (?1, ?2, ?2+1, ?3, ?2, 1)").unwrap();
            for index in 0..extents {
                insert
                    .execute(params![id.to_string(), index, &store.replay_file])
                    .unwrap();
            }
        }
        transaction.execute("UPDATE runs SET durable_output_bytes=?2, replay_bytes=?2, metadata_bytes=?3 WHERE id=?1",
            params![id.to_string(), extents, metadata]).unwrap();
        transaction.commit().unwrap();
        store.truncate_wal_to_zero_with_shutdown(None).unwrap();
        drop(store);
        let (mut store, _) = StateStore::open(
            &state_dir,
            &limits,
            Some(super::HandoffHint {
                epoch: uuid::Uuid::new_v4().to_string(),
                live_set: HashSet::from([id]),
                state_lock_fd: None,
            }),
            Arc::new(PersistenceTestHooks::default()),
        )
        .unwrap();
        let start = u64::try_from(extents).unwrap();
        let payload = vec![b'y'; 70_013];
        let durable_head = Arc::new(std::sync::atomic::AtomicU64::new(start));
        store
            .append_batch(&[(
                id,
                replay(vec![chunk(start, &payload)]),
                Arc::clone(&durable_head),
            )])
            .expect("fragmented pruning adapts within the WAL policy");
        assert!(file_len(&store.wal_path).unwrap() <= resources.wal_bytes());
        assert_eq!(
            durable_head.load(Ordering::Acquire),
            start + payload.len() as u64
        );
        drop(store);
        let (_, recovered) = StateStore::open(
            &state_dir,
            &limits,
            None,
            Arc::new(PersistenceTestHooks::default()),
        )
        .unwrap();
        let output = &recovered[0].replay;
        let bytes: Vec<_> = output
            .chunks
            .iter()
            .flat_map(|chunk| chunk.data.iter().copied())
            .collect();
        assert_eq!(output.first_available_byte, payload.len() as u64);
        let mut expected = vec![b'x'; usize::try_from(extents).unwrap() - payload.len()];
        expected.extend_from_slice(&payload);
        assert_eq!(bytes, expected);
    }

    #[test]
    fn source_discontinuity_survives_cold_recovery_with_a_nonzero_retention_floor() {
        let temp = TempDir::new().unwrap();
        let state_dir = temp.path().join("state");
        let resources = ResourceLimits {
            durable_run_output_bytes: 8,
            ..ResourceLimits::DEFAULT
        };
        let (persistence, _) =
            Persistence::open_with_resources(state_dir.clone(), resources, None).unwrap();
        let info = running_info(RunId::new());
        let durable = persistence
            .insert_start(&test_operation_key(info.id), &info)
            .unwrap();
        expect_queued(durable.append(info.id, replay(vec![chunk(0, b"0123456789abcdef")])));
        persistence.barrier().unwrap();
        durable.mark_source_gap(Some(16));
        durable.finalize(
            info.id,
            info.pid.unwrap(),
            OutputReplay {
                chunks: vec![],
                first_available_byte: 0,
                latest_output_bytes: 16,
                truncated: true,
            },
            RunState::Exited {
                code: 0,
                signal: None,
            },
        );
        persistence.barrier().unwrap();
        drop(durable);
        drop(persistence);
        let (store, recovered) = StateStore::open(
            &state_dir,
            &AdmissionLimits::from(resources),
            None,
            Arc::new(PersistenceTestHooks::default()),
        )
        .unwrap();
        assert_eq!(recovered[0].source_gap_after_byte, Some(16));
        let page =
            super::load_replay_page(&store.connection, &store.replay_dir, info.id, 8, 16).unwrap();
        assert_eq!(page.first_available_byte, 8);
        assert!(
            page.truncated,
            "source loss must remain visible independently of retention"
        );
        assert_eq!(
            page.chunks
                .iter()
                .flat_map(|chunk| chunk.data.iter().copied())
                .collect::<Vec<_>>(),
            b"89abcdef"
        );
        assert!(super::source_gap_fact(r#"{"ctxmux_source_gap_after_byte":17}"#, 16).is_err());
    }

    #[test]
    fn replay_compaction_of_many_small_extents_preserves_wal_and_recovery() {
        let temp = TempDir::new().expect("create many-extent compaction fixture");
        let state_dir = temp.path().join("state");
        let (mut store, _) = StateStore::open(
            &state_dir,
            &AdmissionLimits::OPERATIONAL,
            None,
            Arc::new(PersistenceTestHooks::default()),
        )
        .expect("open compaction fixture");
        let id = RunId::new();
        // One-byte writes are legitimate. A byte budget alone does not bound
        // the number of index pages a maintenance transaction can rewrite.
        let extents = 200_000_i64;
        let stale_prefix = 4096_i64;
        let file = OpenOptions::new()
            .write(true)
            .open(store.replay_dir.join(&store.replay_file))
            .expect("open payload fixture");
        let mut file = file;
        std::io::Seek::seek(
            &mut file,
            std::io::SeekFrom::Start(u64::try_from(stale_prefix).unwrap()),
        )
        .unwrap();
        std::io::Write::write_all(&mut file, &vec![b'x'; usize::try_from(extents).unwrap()])
            .unwrap();
        file.sync_all().unwrap();
        let transaction = store.connection.transaction().unwrap();
        let metadata = insert_test_run(&transaction, id, "running", 0);
        {
            let mut insert = transaction
                .prepare("INSERT INTO replay_chunks (run_id, start_byte, end_byte, data_file, data_offset, data_bytes) VALUES (?1, ?2, ?2 + 1, ?3, ?2 + ?4, 1)")
                .unwrap();
            for index in 0..extents {
                insert
                    .execute(params![
                        id.to_string(),
                        index,
                        &store.replay_file,
                        stale_prefix
                    ])
                    .unwrap();
            }
        }
        transaction.execute(
            "UPDATE runs SET durable_output_bytes = ?2, replay_bytes = ?2, metadata_bytes = ?3 WHERE id = ?1",
            params![id.to_string(), extents, metadata],
        ).unwrap();
        transaction.commit().unwrap();
        store.truncate_wal_to_zero_with_shutdown(None).unwrap();
        store
            .maybe_compact_replay()
            .expect("compact many small extents");
        let wal = file_len(&store.wal_path).unwrap();
        assert!(
            wal <= WAL_MAX_BYTES,
            "replay compaction exceeded the WAL budget: {wal} bytes"
        );
        super::validate_replay_extents(&store.connection, &store.replay_dir, &store.replay_file)
            .expect("all compacted extents remain valid");
        let recovered = load_recovered(&store.connection, &store.replay_dir).unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(
            recovered[0].replay.latest_output_bytes,
            u64::try_from(extents).unwrap()
        );
        assert!(
            recovered[0]
                .replay
                .chunks
                .iter()
                .flat_map(|chunk| chunk.data.iter())
                .all(|byte| *byte == b'x')
        );
        drop(store);
        let (_, recovered) = StateStore::open(
            &state_dir,
            &AdmissionLimits::OPERATIONAL,
            None,
            Arc::new(PersistenceTestHooks::default()),
        )
        .expect("cold recovery validates the complete compacted store");
        assert_eq!(
            recovered[0].replay.latest_output_bytes,
            u64::try_from(extents).unwrap()
        );
    }

    #[test]
    fn uncertain_compaction_commit_latches_even_when_error_looks_like_disk_full() {
        for (committed, lost_at_commit, stat_failure) in [
            (false, 1, false),
            (true, 1, false),
            (false, 2, false),
            (true, 2, false),
            (true, 1, true),
            (true, 2, true),
        ] {
            let temp = TempDir::new().expect("create uncertain maintenance fixture");
            let state_dir = temp.path().join("state");
            let (persistence, _) = Persistence::open(&state_dir).unwrap();
            persistence
                .inner
                .test_hooks
                .suppress_idle_fold
                .store(true, Ordering::Release);
            let info = running_info(RunId::new());
            let durable = persistence
                .insert_start(&test_operation_key(info.id), &info)
                .unwrap();
            let first = replay(vec![chunk(0, b"committed-before-maintenance")]);
            expect_queued(durable.append(info.id, first.clone()));
            persistence.barrier().unwrap();
            let replay_path = fs::read_dir(state_dir.join(REPLAY_DIR))
                .unwrap()
                .next()
                .unwrap()
                .unwrap()
                .path();
            OpenOptions::new()
                .write(true)
                .open(replay_path)
                .unwrap()
                .set_len(4096)
                .unwrap();
            persistence
                .inner
                .test_hooks
                .maintenance_error_committed
                .store(committed, Ordering::Release);
            // First publish the active destination, then lose the response to
            // the first coordinate COMMIT (in either physical outcome).
            let hooks = &persistence.inner.test_hooks;
            if stat_failure {
                hooks
                    .maintenance_commits_before_stat_error
                    .store(lost_at_commit, Ordering::Release);
            } else {
                hooks
                    .maintenance_commits_before_error
                    .store(lost_at_commit, Ordering::Release);
            }
            expect_queued(durable.append(
                info.id,
                replay(vec![chunk(
                    first.latest_output_bytes,
                    b"must-not-commit-after-unknown-maintenance",
                )]),
            ));
            let error = persistence
                .barrier()
                .expect_err("unknown maintenance must fence later writes");
            assert!(!error.is_transient_storage());
            if stat_failure {
                assert!(error.to_string().contains("post-COMMIT WAL inspection"));
            }
            assert!(persistence.is_failed());
            let files_before: Vec<_> = fs::read_dir(state_dir.join(REPLAY_DIR))
                .unwrap()
                .map(|entry| {
                    let path = entry.unwrap().path();
                    (path.clone(), fs::read(path).unwrap())
                })
                .collect();
            for _ in 0..3 {
                assert!(persistence.barrier().is_err());
            }
            std::thread::sleep(Duration::from_millis(50));
            for (path, bytes) in files_before {
                assert_eq!(
                    fs::read(path).unwrap(),
                    bytes,
                    "failure latch forbids idle payload mutation/removal"
                );
            }
            assert!(
                persistence
                    .prepare_start(
                        &test_operation_key(RunId::new()),
                        &running_info(RunId::new())
                    )
                    .is_err()
            );
            drop(durable);
            drop(persistence);
            let (reopened, recovered) = Persistence::open(&state_dir)
                .expect("recovery resolves either committed coordinate outcome");
            assert_eq!(recovered.len(), 1);
            assert_eq!(recovered[0].replay, first);
            assert!(!reopened.is_failed());
        }
    }

    #[test]
    fn interrupted_compaction_preserves_fragmented_extents_across_generations() {
        let temp = TempDir::new().unwrap();
        let state_dir = temp.path().join("state");
        let (mut store, _) = StateStore::open(
            &state_dir,
            &AdmissionLimits::OPERATIONAL,
            None,
            Arc::new(PersistenceTestHooks::default()),
        )
        .unwrap();
        let id = RunId::new();
        let transaction = store.connection.transaction().unwrap();
        let metadata = insert_test_run(&transaction, id, "running", 0);
        let mut file = OpenOptions::new()
            .write(true)
            .open(store.replay_dir.join(&store.replay_file))
            .unwrap();
        let count = 5000_i64;
        for index in 0..count {
            let offset = (index + 1) * 4096;
            std::io::Seek::seek(
                &mut file,
                std::io::SeekFrom::Start(u64::try_from(offset).unwrap()),
            )
            .unwrap();
            std::io::Write::write_all(&mut file, &[u8::try_from(index % 251).unwrap()]).unwrap();
            transaction.execute(
                "INSERT INTO replay_chunks (run_id, start_byte, end_byte, data_file, data_offset, data_bytes) VALUES (?1, ?2, ?2 + 1, ?3, ?4, 1)",
                params![id.to_string(), index, &store.replay_file, offset],
            ).unwrap();
        }
        file.sync_all().unwrap();
        transaction.execute(
            "UPDATE runs SET durable_output_bytes = ?2, replay_bytes = ?2, metadata_bytes = ?3 WHERE id = ?1",
            params![id.to_string(), count, metadata],
        ).unwrap();
        transaction.commit().unwrap();
        drop(store);
        let crash = run_replay_compaction_crash_subprocess(&state_dir, "after_copy");
        assert_eq!(
            crash.status.signal(),
            Some(rustix::process::Signal::ABORT.as_raw())
        );
        let connection = Connection::open(state_dir.join(DATABASE_FILE)).unwrap();
        let generations: i64 = connection
            .query_row(
                "SELECT count(DISTINCT data_file) FROM replay_chunks",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            generations, 2,
            "the crash must exercise a durable mixed-generation state"
        );
        drop(connection);
        let (reopened, recovered) = StateStore::open(
            &state_dir,
            &AdmissionLimits::OPERATIONAL,
            None,
            Arc::new(PersistenceTestHooks::default()),
        )
        .expect("resume partially committed migration");
        let bytes = recovered[0]
            .replay
            .chunks
            .iter()
            .flat_map(|chunk| chunk.data.clone())
            .collect::<Vec<_>>();
        assert_eq!(
            bytes,
            (0..count)
                .map(|index| u8::try_from(index % 251).unwrap())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            file_len(&reopened.replay_dir.join(&reopened.replay_file)).unwrap(),
            u64::try_from(count).unwrap()
        );
        assert_eq!(fs::read_dir(&reopened.replay_dir).unwrap().count(), 1);
    }

    #[test]
    fn oversized_replay_generation_past_database_ceiling_compacts_without_changing_the_window() {
        let temp = TempDir::new().expect("create replay compaction fixture");
        let state_dir = temp.path().join("state");
        let (mut store, recovered) = StateStore::open(
            &state_dir,
            &AdmissionLimits::OPERATIONAL,
            None,
            Arc::new(PersistenceTestHooks::default()),
        )
        .expect("open replay compaction store");
        assert!(recovered.is_empty());

        let id = RunId::new();
        let transaction = store
            .connection
            .transaction()
            .expect("start compaction fixture transaction");
        insert_test_run(&transaction, id, "running", 1);
        append_replay_external(
            &transaction,
            id,
            &replay(vec![chunk(0, b"retain this payload")]),
            &store.replay_dir,
            &store.replay_file,
        )
        .expect("write compaction fixture replay");
        transaction.commit().expect("commit compaction fixture");

        let old_file = store.replay_dir.join(&store.replay_file);
        let old_len = file_len(&old_file).expect("measure current generation");
        let file = OpenOptions::new()
            .write(true)
            .open(&old_file)
            .expect("open current replay generation");
        // Use a sparse file so this exercises a generation larger than the
        // frozen SQLite main database without spending hundreds of MiB in the
        // test process. The compactor must copy only referenced segments.
        file.set_len(DATABASE_MAX_BYTES + PAGE_SIZE_BYTES)
            .expect("inflate replay generation past the database ceiling");
        drop(file);
        assert!(file_len(&old_file).expect("measure inflated generation") > old_len);

        store
            .maybe_compact_replay()
            .expect("compact oversized replay generation");
        assert!(
            !old_file.exists(),
            "the obsolete generation must be removed"
        );
        let recovered =
            load_recovered(&store.connection, &store.replay_dir).expect("read compacted replay");
        assert_eq!(recovered.len(), 1);
        assert_eq!(
            recovered[0].replay,
            replay(vec![chunk(0, b"retain this payload")])
        );
        assert!(
            file_len(&store.replay_dir.join(&store.replay_file))
                .expect("measure compacted generation")
                < old_len + 128,
            "compaction must discard the sparse stale tail"
        );
    }

    #[test]
    fn replay_compaction_crash_selects_one_generation_on_reopen() {
        for phase in ["before_commit", "after_commit"] {
            let temp = TempDir::new().expect("create replay compaction crash fixture");
            let state_dir = temp.path().join(phase);
            let (mut store, recovered) = StateStore::open(
                &state_dir,
                &AdmissionLimits::OPERATIONAL,
                None,
                Arc::new(PersistenceTestHooks::default()),
            )
            .expect("open replay compaction crash store");
            assert!(recovered.is_empty());

            let id = RunId::new();
            let transaction = store
                .connection
                .transaction()
                .expect("start replay compaction crash setup");
            let actual_metadata = insert_test_run(&transaction, id, "running", 1);
            transaction
                .execute(
                    "UPDATE runs SET metadata_bytes = ?2 WHERE id = ?1",
                    params![id.to_string(), actual_metadata],
                )
                .expect("make replay compaction crash metadata valid");
            transaction
                .commit()
                .expect("commit replay compaction crash setup");

            // A committed extent after a stale prefix forces real compaction;
            // an unindexed tail alone is reclaimed by startup truncation.
            OpenOptions::new()
                .write(true)
                .open(store.replay_dir.join(&store.replay_file))
                .unwrap()
                .set_len(4096)
                .unwrap();

            let transaction = store
                .connection
                .transaction()
                .expect("start replay compaction crash payload transaction");
            let payload = vec![b'c'; 2048];
            let expected_replay = replay(vec![chunk(0, &payload)]);
            append_replay_external(
                &transaction,
                id,
                &expected_replay,
                &store.replay_dir,
                &store.replay_file,
            )
            .expect("write replay compaction crash payload");
            transaction
                .commit()
                .expect("commit replay compaction crash payload");

            drop(store);

            let output = run_replay_compaction_crash_subprocess(&state_dir, phase);
            assert_eq!(output.status.code(), None);
            assert_eq!(
                output.status.signal(),
                Some(rustix::process::Signal::ABORT.as_raw()),
                "replay compaction crash helper did not abort: {}",
                String::from_utf8_lossy(&output.stderr)
            );

            let (reopened, recovered) = StateStore::open(
                &state_dir,
                &AdmissionLimits::OPERATIONAL,
                None,
                Arc::new(PersistenceTestHooks::default()),
            )
            .expect("reopen after replay compaction crash");
            assert_eq!(recovered.len(), 1);
            assert_eq!(recovered[0].replay, expected_replay);
            let generation_files = fs::read_dir(&reopened.replay_dir)
                .expect("read replay generations after crash recovery")
                .map(|entry| entry.expect("read replay generation entry").path())
                .collect::<Vec<_>>();
            assert_eq!(
                generation_files.len(),
                1,
                "startup must remove the losing generation"
            );
            assert_eq!(
                generation_files[0]
                    .file_name()
                    .and_then(|name| name.to_str()),
                Some(reopened.replay_file.as_str())
            );
        }
    }

    #[test]
    fn replay_compaction_crash_subprocess() {
        let Some(state_dir) = env::var_os(REPLAY_COMPACTION_CRASH_STATE_DIR) else {
            return;
        };
        let phase = env::var(REPLAY_COMPACTION_CRASH_PHASE)
            .expect("replay compaction crash helper receives a phase");
        let state_dir = PathBuf::from(state_dir);
        let (_store, _recovered) = StateStore::open(
            &state_dir,
            &AdmissionLimits::OPERATIONAL,
            None,
            Arc::new(PersistenceTestHooks::default()),
        )
        .expect("open replay compaction crash helper store");
        panic!("replay compaction crash hook did not terminate at {phase}");
    }

    #[test]
    fn rolled_back_replay_transaction_tail_is_truncated_on_reopen() {
        let temp = TempDir::new().expect("create replay rollback fixture");
        let state_dir = temp.path().join("state");
        let (mut store, recovered) = StateStore::open(
            &state_dir,
            &AdmissionLimits::OPERATIONAL,
            None,
            Arc::new(PersistenceTestHooks::default()),
        )
        .expect("open replay rollback store");
        assert!(recovered.is_empty());

        let id = RunId::new();
        let transaction = store
            .connection
            .transaction()
            .expect("start replay rollback setup transaction");
        let actual_metadata = insert_test_run(&transaction, id, "running", 1);
        transaction
            .execute(
                "UPDATE runs SET metadata_bytes = ?2 WHERE id = ?1",
                params![id.to_string(), actual_metadata],
            )
            .expect("make replay rollback fixture metadata valid");
        transaction.commit().expect("commit replay rollback setup");

        let replay_path = store.replay_dir.join(&store.replay_file);
        let base_len = file_len(&replay_path).expect("measure empty replay generation");
        {
            let transaction = store
                .connection
                .transaction()
                .expect("start replay rollback transaction");
            append_replay_external(
                &transaction,
                id,
                &replay(vec![chunk(0, b"tail must not become durable")]),
                &store.replay_dir,
                &store.replay_file,
            )
            .expect("write replay before forced rollback");
            assert!(
                file_len(&replay_path).expect("measure uncommitted replay tail") > base_len,
                "the fixture must leave a physical tail behind the rolled-back transaction"
            );
            transaction
                .execute("INSERT INTO missing_table VALUES (1)", [])
                .expect_err("force the transaction to roll back");
        }
        assert!(
            file_len(&replay_path).expect("measure rolled-back replay tail") > base_len,
            "a failed SQLite transaction may leave the synced append tail behind"
        );
        drop(store);

        let (reopened, recovered) = StateStore::open(
            &state_dir,
            &AdmissionLimits::OPERATIONAL,
            None,
            Arc::new(PersistenceTestHooks::default()),
        )
        .expect("reopen after replay transaction rollback");
        assert!(recovered.iter().all(|run| run.replay.chunks.is_empty()));
        let current_path = reopened.replay_dir.join(&reopened.replay_file);
        assert_eq!(
            file_len(&current_path).expect("measure normalized replay generation"),
            base_len,
            "startup must truncate bytes that never acquired a durable index row"
        );
    }

    #[test]
    fn truncated_replay_payload_fails_closed_on_reopen() {
        let temp = TempDir::new().expect("create replay corruption fixture");
        let state_dir = temp.path().join("state");
        let (persistence, recovered) = Persistence::open(&state_dir).expect("open persistence");
        assert!(recovered.is_empty());
        let info = running_info(RunId::new());
        let durable = persistence
            .insert_start(&test_operation_key(info.id), &info)
            .expect("insert replay corruption fixture Run");
        durable.finalize(
            info.id,
            7,
            replay(vec![chunk(0, b"payload that must remain intact")]),
            exited_state(),
        );
        drop(durable);
        persistence.assert_exclusive_owner();
        drop(persistence);

        let replay_file = fs::read_dir(state_dir.join(REPLAY_DIR))
            .expect("read replay directory")
            .map(|entry| entry.expect("read replay entry").path())
            .next()
            .expect("find current replay generation");
        OpenOptions::new()
            .write(true)
            .open(&replay_file)
            .expect("open replay generation for corruption")
            .set_len(0)
            .expect("truncate replay generation");

        let Err(error) = Persistence::open(&state_dir) else {
            panic!("truncated replay must be rejected");
        };
        assert!(matches!(error, PersistenceError::Corrupt(_)));
    }

    #[test]
    fn oversized_replay_segment_length_fails_before_allocation() {
        let temp = TempDir::new().expect("create replay length fixture");
        let path = temp.path().join("replay-test.bin");
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&path)
            .expect("create replay length fixture file");
        let error =
            read_replay_segment(temp.path(), "replay-test.bin", 0, PER_RUN_REPLAY_BYTES + 1)
                .expect_err("an oversized corrupt extent must be rejected before allocation");
        assert!(
            matches!(error, PersistenceError::Corrupt(message) if message.contains("exceeds file length"))
        );
    }

    /// The change this whole round is: a lifecycle verb must NOT checkpoint a
    /// WAL that is already under the 8 MiB ceiling.
    ///
    /// Folding to zero on every `start` and `remove` cost ~1.6 ms/MiB against a
    /// WAL that a chatty fleet keeps at ~8.5 MB essentially always (sampled
    /// every 10 ms it was at zero for 3.7% of samples at chatty=2), which
    /// measured as 85-99% of why those verbs lose to tmux. Reverting
    /// `fold_wal_below_ceiling` to an unconditional truncate leaves every other
    /// test in this file green, so this one exists to fail instead.
    ///
    /// The two halves are asserted together on purpose: "did not fold" is only
    /// correct while the baseline is under the ceiling, and "did fold" is only
    /// correct once it is over.
    #[test]
    fn a_lifecycle_verb_folds_only_a_wal_that_is_over_the_ceiling() {
        let temp = TempDir::new().expect("create baseline fold fixture");
        let state_dir = temp.path().join("state");
        let hooks = Arc::new(PersistenceTestHooks::default());
        let (mut store, _recovered) = StateStore::open(
            &state_dir,
            &AdmissionLimits::OPERATIONAL,
            None,
            Arc::clone(&hooks),
        )
        .expect("open baseline fold fixture store");
        store.truncate_wal_to_zero().expect("start from zero");

        // A WAL that a chatty fleet would have left behind: real bytes, but
        // still under the ceiling the output path lets it ride up to.
        let transaction = store
            .connection
            .transaction()
            .expect("start baseline fixture transaction");
        insert_test_run(&transaction, RunId::new(), "exited", 1);
        transaction
            .commit()
            .expect("dirty the WAL below the ceiling");
        let dirty = file_len(&store.wal_path).expect("read WAL length");
        assert!(
            dirty > 0 && dirty <= WAL_CHECKPOINT_BYTES,
            "fixture WAL {dirty} must be dirty but under the 8 MiB ceiling"
        );

        hooks.checkpoint_attempts.store(0, Ordering::Release);
        let baseline = store
            .fold_wal_below_ceiling(None)
            .expect("admission reads its baseline");
        assert_eq!(
            baseline, dirty,
            "a WAL under the ceiling is the baseline, not something to erase"
        );
        assert_eq!(
            hooks.checkpoint_attempts.load(Ordering::Acquire),
            0,
            "folding a WAL that is already under the ceiling is the ~13 ms this \
             round removed; a checkpoint here means it came back"
        );

        // The other half: over the ceiling, the fold must still fire, because
        // that is what keeps `baseline + charge` inside the 16 MiB total.
        store
            .connection
            .execute_batch("CREATE TABLE ceiling_ballast(id INTEGER PRIMARY KEY, value BLOB);")
            .expect("create ballast table");
        while file_len(&store.wal_path).expect("read WAL length") <= WAL_CHECKPOINT_BYTES {
            store
                .connection
                .execute_batch("INSERT INTO ceiling_ballast(value) VALUES (zeroblob(262144));")
                .expect("grow the WAL past the ceiling");
        }
        hooks.checkpoint_attempts.store(0, Ordering::Release);
        let folded = store
            .fold_wal_below_ceiling(None)
            .expect("admission folds an over-ceiling WAL");
        assert_eq!(folded, 0, "an over-ceiling WAL must be truncated");
        assert!(
            hooks.checkpoint_attempts.load(Ordering::Acquire) > 0,
            "the 16 MiB total ceiling depends on this fold actually happening"
        );
    }

    /// The actor folds the WAL when its queue drains, so a later `StageStart`
    /// inherits a small baseline instead of paying ~1.6 ms/MiB to create one
    /// (cn3, synchronous=FULL, linear to the 8 MiB ceiling). This pins the fold
    /// actually happening off the client's path.
    ///
    /// The payload has to clear `WAL_IDLE_FOLD_FLOOR_BYTES`: below the floor the
    /// fold deliberately declines, because truncating a small WAL costs the next
    /// commit more than it saves.
    #[test]
    fn a_drained_queue_folds_the_wal_before_the_next_start_needs_it() {
        let temp = TempDir::new().expect("create idle fold fixture");
        let state_dir = temp.path().join("state");
        let (persistence, _recovered) = Persistence::open(&state_dir).expect("open persistence");
        let info = running_info(RunId::new());
        let durable = persistence
            .insert_start(&test_operation_key(info.id), &info)
            .expect("insert idle fold fixture Run");

        // Dirty the WAL the way a producing Run does, then let the queue drain.
        let payload = vec![b'x'; 4 * FOLD_FLOOR_PAYLOAD];
        expect_queued(durable.append(info.id, replay(vec![chunk(0, &payload)])));
        persistence.barrier().expect("drain the append");

        let wal = state_dir.join(format!("{DATABASE_FILE}-wal"));
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            if file_len(&wal).unwrap_or(0) == 0 {
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
        assert!(
            file_len(&wal).unwrap_or(u64::MAX) <= WAL_IDLE_FOLD_FLOOR_BYTES,
            "a drained queue must leave the WAL below the fold floor"
        );
        assert!(!persistence.is_failed());
    }

    /// The guard for the metric this project already won: idle CPU is 0.000%,
    /// and a fold that fires on every pass through the dequeue loop -- rather
    /// than only when the WAL is actually worth folding -- would quietly undo
    /// that.
    ///
    /// Drives `idle_fold_wal` directly rather than racing the actor: the
    /// property is "a WAL below the floor costs no checkpoint", which is a
    /// property of the function, and asserting it here needs no sleeping.
    #[test]
    fn an_idle_fold_skips_a_wal_below_the_floor() {
        let temp = TempDir::new().expect("create idle quiet fixture");
        let state_dir = temp.path().join("state");
        let hooks = Arc::new(PersistenceTestHooks::default());
        let (mut store, _recovered) = StateStore::open(
            &state_dir,
            &AdmissionLimits::OPERATIONAL,
            None,
            Arc::clone(&hooks),
        )
        .expect("open idle quiet fixture store");
        let shutdown = AtomicBool::new(false);

        store
            .truncate_wal_to_zero()
            .expect("reach the zero baseline the fold is supposed to notice");
        let baseline = hooks.idle_folds.load(Ordering::Acquire);

        for _ in 0..64 {
            assert!(
                !idle_fold_wal(&store, &shutdown),
                "an already-zero WAL must not be folded again"
            );
        }

        // A WAL that is dirty but still under the floor is the case the floor
        // exists for, and the one a zero-only check would get wrong: folding it
        // would charge the next commit ~1 ms to save less than that.
        let transaction = store
            .connection
            .transaction()
            .expect("start below-floor fixture transaction");
        insert_test_run(&transaction, RunId::new(), "running", 1);
        transaction.commit().expect("dirty the WAL with a real row");
        let dirty = file_len(&store.wal_path).expect("read WAL length");
        assert!(
            dirty > 0 && dirty < WAL_IDLE_FOLD_FLOOR_BYTES,
            "the fixture must leave the WAL dirty but under the {WAL_IDLE_FOLD_FLOOR_BYTES} \
             byte floor; it is {dirty} bytes"
        );
        assert!(
            !idle_fold_wal(&store, &shutdown),
            "a WAL under the floor must be left alone, not truncated"
        );
        assert_eq!(
            file_len(&store.wal_path).expect("read WAL length"),
            dirty,
            "declining to fold must leave the bytes exactly where they were"
        );

        assert_eq!(
            hooks.idle_folds.load(Ordering::Acquire),
            baseline,
            "idle passes issued checkpoints against a WAL below the floor; the skip is \
             not holding and idle CPU will regress"
        );
    }

    /// The other half: a dirty WAL must actually get folded while the actor is
    /// idle, so the bytes are gone before any verb has to carry them.
    #[test]
    fn an_idle_fold_zeroes_a_dirty_wal_exactly_once() {
        let temp = TempDir::new().expect("create idle dirty fixture");
        let state_dir = temp.path().join("state");
        let hooks = Arc::new(PersistenceTestHooks::default());
        let (mut store, _recovered) = StateStore::open(
            &state_dir,
            &AdmissionLimits::OPERATIONAL,
            None,
            Arc::clone(&hooks),
        )
        .expect("open idle dirty fixture store");
        let shutdown = AtomicBool::new(false);
        store.truncate_wal_to_zero().expect("start from zero");

        let transaction = store
            .connection
            .transaction()
            .expect("start idle dirty fixture transaction");
        // Enough rows to carry the WAL past the fold floor: one row is dirty
        // but deliberately not worth folding, which the floor fixture covers.
        let mut rows = 0;
        while file_len(&store.wal_path).expect("read WAL length") < WAL_IDLE_FOLD_FLOOR_BYTES {
            insert_test_run(&transaction, RunId::new(), "running", 1);
            rows += 1;
            assert!(
                rows < 100_000,
                "the fixture could not carry the WAL past its floor"
            );
        }
        transaction.commit().expect("dirty the WAL with real rows");
        assert!(
            file_len(&store.wal_path).expect("read WAL length") >= WAL_IDLE_FOLD_FLOOR_BYTES,
            "the fixture must leave enough WAL bytes to be worth folding"
        );

        assert!(
            idle_fold_wal(&store, &shutdown),
            "a WAL above the floor must fold"
        );
        assert_eq!(
            file_len(&store.wal_path).expect("read WAL length"),
            0,
            "an idle fold leaves nothing for a later verb to carry"
        );
        let after = hooks.idle_folds.load(Ordering::Acquire);
        assert!(
            !idle_fold_wal(&store, &shutdown),
            "the WAL is zero now; a second fold must be skipped"
        );
        assert_eq!(
            hooks.idle_folds.load(Ordering::Acquire),
            after,
            "the skip must cost no checkpoint"
        );
    }

    /// The wiring guard. The three fixtures above test `idle_fold_wal` itself,
    /// so deleting its call site leaves every one of them green -- this one
    /// goes through the real actor and fails if the fold is never reached.
    #[test]
    fn the_actor_folds_the_wal_once_its_queue_drains() {
        let temp = TempDir::new().expect("create actor idle fold fixture");
        let state_dir = temp.path().join("state");
        let (persistence, recovered) = Persistence::open(&state_dir).expect("open persistence");
        assert!(recovered.is_empty());

        let info = running_info(RunId::new());
        let durable = persistence
            .insert_start(&test_operation_key(info.id), &info)
            .expect("insert actor idle fold fixture Run");
        expect_queued(durable.append(
            info.id,
            replay(vec![chunk(0, &vec![b'x'; 4 * FOLD_FLOOR_PAYLOAD])]),
        ));
        persistence.barrier().expect("drain the queued append");

        // The barrier returns once the append has committed; the fold happens
        // on the actor's next trip through an empty queue, so poll rather than
        // sleep a fixed amount.
        let wal = state_dir.join(format!("{DATABASE_FILE}-wal"));
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut folded = false;
        while Instant::now() < deadline {
            if file_len(&wal).unwrap_or(u64::MAX) <= WAL_IDLE_FOLD_FLOOR_BYTES {
                folded = true;
                break;
            }
            thread::sleep(Duration::from_millis(5));
        }

        assert!(
            folded,
            "a drained actor queue left {} WAL bytes above the fold floor",
            file_len(&wal).unwrap_or(u64::MAX)
        );
        assert!(!persistence.is_failed());
    }

    #[test]
    fn an_idle_fold_does_nothing_once_shutdown_is_set() {
        let temp = TempDir::new().expect("create idle shutdown fixture");
        let state_dir = temp.path().join("state");
        let hooks = Arc::new(PersistenceTestHooks::default());
        let (mut store, _recovered) = StateStore::open(
            &state_dir,
            &AdmissionLimits::OPERATIONAL,
            None,
            Arc::clone(&hooks),
        )
        .expect("open idle shutdown fixture store");
        let transaction = store
            .connection
            .transaction()
            .expect("start idle shutdown fixture transaction");
        // Past the fold floor, so the only thing that can decline this fold is
        // the shutdown flag. A below-floor WAL would return false either way and
        // leave the assertion unable to fail.
        let mut rows = 0;
        while file_len(&store.wal_path).expect("read WAL length") < WAL_IDLE_FOLD_FLOOR_BYTES {
            insert_test_run(&transaction, RunId::new(), "running", 1);
            rows += 1;
            assert!(
                rows < 100_000,
                "the fixture could not carry the WAL past its floor"
            );
        }
        transaction.commit().expect("dirty the WAL");
        assert!(
            file_len(&store.wal_path).expect("read WAL length") >= WAL_IDLE_FOLD_FLOOR_BYTES,
            "the fixture must leave a WAL that would otherwise be folded"
        );

        let shutdown = AtomicBool::new(true);
        let before = hooks.idle_folds.load(Ordering::Acquire);
        assert!(!idle_fold_wal(&store, &shutdown));
        assert_eq!(
            hooks.idle_folds.load(Ordering::Acquire),
            before,
            "a shutting-down actor must not checkpoint"
        );
    }

    #[test]
    fn exhausted_checkpoint_pressure_retries_without_latching_the_actor() {
        let shutdown = AtomicBool::new(false);
        let mut attempts = 0_u8;
        let result = retry_transient_storage(&shutdown, || {
            attempts = attempts.saturating_add(1);
            if attempts < 2 {
                Err(super::wal_checkpoint_retry_exhausted(
                    WAL_CHECKPOINT_MAX_RETRIES + 1,
                    Some("busy=1, wal_bytes=4096".to_owned()),
                ))
            } else {
                Ok(())
            }
        });
        assert!(matches!(result, Some(Ok(()))));
        assert_eq!(attempts, 2);
    }

    #[test]
    fn corrupted_data_page_fails_closed_through_the_quick_check_guard() {
        use std::io::{Seek, SeekFrom, Write};

        let temp = TempDir::new().expect("create quick_check corruption fixture");
        let state_dir = temp.path().join("state");

        // Seed real durable rows with replay payloads so the main database holds
        // several b-tree data pages beyond the schema for quick_check to scan.
        {
            let (persistence, recovered) =
                Persistence::open(&state_dir).expect("open corruption fixture");
            assert!(recovered.is_empty());
            for _ in 0..8 {
                let info = running_info(RunId::new());
                let durable = persistence
                    .insert_start(&test_operation_key(info.id), &info)
                    .expect("insert corruption fixture Run");
                let payload = replay(vec![chunk(0, &[0x5a_u8; 2048])]);
                expect_queued(durable.append(info.id, payload.clone()));
                durable.finalize(info.id, 7, payload, exited_state());
            }
            persistence.assert_exclusive_owner();
            drop(persistence);
        }

        let database_path = state_dir.join(DATABASE_FILE);

        // Fold the WAL into the main database (autocheckpoint is disabled) so the
        // durable pages live in the file we damage, then prove the fixture is
        // healthy before corruption. query_row fetches only the first row, which
        // for a healthy database is the literal "ok".
        {
            let connection = Connection::open(&database_path).expect("open fixture for checkpoint");
            connection
                .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
                .expect("fold WAL into the main database");
            let quick_check: String = connection
                .query_row("PRAGMA quick_check", [], |row| row.get(0))
                .expect("healthy fixture quick_check");
            assert_eq!(
                quick_check, "ok",
                "fixture must be healthy before we damage it"
            );
        }

        // Overwrite real on-disk bytes across the data pages (page 4 onward). The
        // schema page (1), the auto-vacuum pointer map (2) and the runtime_meta
        // root (3) are left intact so validate_existing_schema still passes and
        // the malformation surfaces specifically at the quick_check integrity
        // guard rather than at schema validation. This mangles genuine SQLite
        // pages on disk; it is not a fabricated error object.
        let file_len = fs::metadata(&database_path)
            .expect("stat fixture database")
            .len();
        let first_data_page_offset = 3 * PAGE_SIZE_BYTES;
        assert!(
            file_len > first_data_page_offset + PAGE_SIZE_BYTES,
            "checkpointed fixture must hold data pages beyond the schema, got {file_len} bytes"
        );
        {
            let mut file = OpenOptions::new()
                .write(true)
                .open(&database_path)
                .expect("open fixture database for corruption");
            let mut offset = first_data_page_offset;
            while offset + 512 <= file_len {
                file.seek(SeekFrom::Start(offset + 16))
                    .expect("seek into a data page header");
                file.write_all(&[0xff_u8; 128])
                    .expect("inject garbage bytes into the cell pointer region");
                file.write_all(&[0x00_u8; 128])
                    .expect("inject NUL bytes into the cell content region");
                offset += PAGE_SIZE_BYTES;
            }
            file.flush().expect("flush the corruption to disk");
        }

        let Err(error) = Persistence::open(&state_dir) else {
            panic!("a malformed data page must fail closed, not open silently");
        };
        let PersistenceError::Corrupt(message) = &error else {
            panic!("on-disk corruption must classify as Corrupt, got {error:?}");
        };
        assert!(
            message.contains("quick_check returned"),
            "corruption must be caught by the quick_check integrity guard, got {message:?}"
        );
    }

    #[test]
    fn sqlite_disk_full_code_survives_error_translation() {
        let disk_full = rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_FULL),
            Some("database or disk is full".to_owned()),
        );
        assert!(PersistenceError::database(disk_full).is_disk_full());

        let busy = rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_BUSY),
            Some("database is busy".to_owned()),
        );
        assert!(!PersistenceError::database(busy).is_disk_full());
    }

    /// A full filesystem does not always arrive as `SQLITE_FULL`. When the write
    /// that runs out of space is to the WAL, the journal, or an `fsync` of
    /// either, `SQLite` reports the underlying `ENOSPC` as `SQLITE_IOERR_*`, whose
    /// primary code is the same `SystemIoFailure` that genuinely broken storage
    /// uses. Classification therefore has to read the extended code, and this
    /// pins both halves: the write-side codes retry, everything else stays
    /// fail-closed.
    #[test]
    fn storage_pressure_is_classified_by_extended_code_not_primary_code() {
        let write_side = [
            rusqlite::ffi::SQLITE_IOERR_WRITE,
            rusqlite::ffi::SQLITE_IOERR_FSYNC,
            rusqlite::ffi::SQLITE_IOERR_DIR_FSYNC,
            rusqlite::ffi::SQLITE_IOERR_TRUNCATE,
        ];
        for extended in write_side {
            let error = PersistenceError::database(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(extended),
                Some("disk I/O error".to_owned()),
            ));
            assert!(
                error.is_transient_storage(),
                "extended code {extended} names a write a full disk defeats, so it must retry"
            );
            assert!(
                !error.is_disk_full(),
                "extended code {extended} arrives as SystemIoFailure, not DiskFull: \
                 if is_disk_full covered it, reading the extended code would be pointless"
            );
        }

        // Everything else keeps latching. Retrying an unreadable or corrupt
        // store would replace a visible outage with an unbounded hang.
        let fail_closed = [
            rusqlite::ffi::SQLITE_IOERR_READ,
            rusqlite::ffi::SQLITE_IOERR_SHORT_READ,
            rusqlite::ffi::SQLITE_IOERR_DELETE,
            rusqlite::ffi::SQLITE_IOERR_LOCK,
            rusqlite::ffi::SQLITE_IOERR,
            rusqlite::ffi::SQLITE_CORRUPT,
            rusqlite::ffi::SQLITE_NOTADB,
        ];
        for extended in fail_closed {
            let error = PersistenceError::database(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(extended),
                Some("not a storage-pressure failure".to_owned()),
            ));
            assert!(
                !error.is_transient_storage(),
                "extended code {extended} is not storage pressure and must stay fail-closed"
            );
        }
        assert!(
            PersistenceError::io("replay.bin", io::Error::from(io::ErrorKind::StorageFull),)
                .is_transient_storage(),
            "a full replay filesystem must retry the exact ordered append"
        );
    }

    /// The reported incident, as a drill: a single `SQLITE_IOERR` during an
    /// append used to latch the actor and reject every later mutation, even
    /// though the store was intact — which a clean reopen proved.
    ///
    /// The four steps are the ones the incident report named: inject one
    /// `ENOSPC`-shaped I/O error, confirm the actor is not latched and later
    /// mutations still land, reopen the same state directory, and confirm the
    /// existing Run recovered with its exact bytes.
    #[test]
    fn a_wal_write_io_failure_retries_instead_of_latching_the_actor() {
        let temp = TempDir::new().expect("create append I/O-failure fixture");
        let state_dir = temp.path().join("state");
        let (persistence, recovered) = Persistence::open(&state_dir).expect("open persistence");
        assert!(recovered.is_empty());

        let first = running_info(RunId::new());
        let durable = persistence
            .insert_start(&test_operation_key(first.id), &first)
            .expect("insert append I/O-failure fixture");
        let first_replay = replay(vec![chunk(0, b"survives ENOSPC on the WAL")]);

        // Step 1: one write-side I/O failure, the shape ENOSPC takes when the
        // failing write lands on the WAL rather than on the database file.
        persistence.fail_next_append_as_io_failure(rusqlite::ffi::SQLITE_IOERR_WRITE);
        expect_queued(durable.append(first.id, first_replay.clone()));
        durable.finalize(first.id, 42, first_replay.clone(), exited_state());

        // Step 2: the actor is not latched, and the retried append committed.
        assert!(
            !persistence.is_failed(),
            "a full filesystem is an operator-clearable condition, not durable corruption"
        );
        assert_eq!(
            durable.durable_head(),
            first_replay.latest_output_bytes,
            "the queued finalize must run after the retried append"
        );

        // Step 3: later mutations still succeed, which is exactly what the
        // incident reported as broken.
        let later = running_info(RunId::new());
        let later_durable = persistence
            .insert_start(&test_operation_key(later.id), &later)
            .expect("later start remains writable after a recovered I/O failure");
        later_durable.finalize(later.id, 43, replay(Vec::new()), exited_state());

        drop(later_durable);
        drop(durable);
        persistence.assert_exclusive_owner();
        drop(persistence);

        // Step 4: reopening the same state directory recovers both Runs with
        // their exact bytes. The retry committed once, not twice.
        let (reopened, recovered) = Persistence::open(state_dir).expect("reopen recovered state");
        let recovered_first = recovered
            .iter()
            .find(|run| run.info.id == first.id)
            .expect("first Run remains durable");
        assert_eq!(recovered_first.info.state, exited_state());
        assert_eq!(
            recovered_first.replay, first_replay,
            "an idempotent retry must not duplicate or corrupt the committed bytes"
        );
        assert!(recovered.iter().any(|run| run.info.id == later.id));
        drop(reopened);
    }

    /// Many small appends must become few large rows, without changing a byte
    /// of what is stored.
    ///
    /// A `replay_chunks` row costs the same fixed overhead — record header,
    /// 36-byte `run_id`, three integers, an index entry — whether it carries
    /// 200 bytes or 64 KiB. PTY reads average 200-600 B on the farm host, so a
    /// row per read spent 2.0-2.8 WAL bytes per byte of real output, and the
    /// ~57 MB/s fold ceiling divided by that is the chatty cliff.
    ///
    /// This asserts the CONSEQUENCE, not the mechanism: row count collapses,
    /// and the recovered bytes are identical to what was appended. Asserting
    /// "the coalescer coalesced" by re-deriving its own arithmetic would pass
    /// even if the rows were wrong.
    ///
    /// The 400 reads arrive as ONE `OutputReplay` because that is the shape
    /// `coalesce_batch` hands the writer: a batch of appends for one Run is
    /// merged into a single replay carrying every chunk. Offering them one at a
    /// time through `append` instead would make this fixture a function of
    /// `PERSISTENCE_QUEUE_CAPACITY` — `append_replay` flushes its pending buffer
    /// unconditionally at the end (crash consistency: the buffer does not
    /// survive the transaction), so one batch is one row boundary and the batch
    /// is bounded by what `try_recv` can pull. Measured: 4 rows at capacity 64,
    /// 20 at capacity 16, which is a performance knob moving a correctness
    /// assertion. This fixture verifies packing inside one collected batch;
    /// separate actor tests cover collection across empty-queue intervals.
    #[test]
    fn many_small_appends_become_few_large_rows() {
        let temp = TempDir::new().expect("create coalescing fixture");
        let state_dir = temp.path().join("state");
        let (persistence, recovered) = Persistence::open(&state_dir).expect("open persistence");
        assert!(recovered.is_empty());

        let info = running_info(RunId::new());
        let durable = persistence
            .insert_start(&test_operation_key(info.id), &info)
            .expect("insert coalescing fixture");

        // 400 reads of 512 B: the shape the reactor actually produces, and
        // 200 KiB in total, so a correct packing needs 4 rows at 64 KiB where
        // one-row-per-read needed 400.
        let payload = [b'x'; 512];
        let mut head = 0_u64;
        let mut written = Vec::new();
        let mut chunks = Vec::new();
        for _ in 0..400 {
            chunks.push(chunk(head, &payload));
            written.extend_from_slice(&payload);
            head += payload.len() as u64;
        }
        assert!(
            durable.append(info.id, replay(chunks)),
            "an empty queue must accept the first append"
        );
        persistence.barrier().expect("the appends commit");
        assert!(!persistence.is_failed());
        assert_eq!(durable.durable_head(), head);

        let final_replay = replay(vec![chunk(head, b"tail")]);
        durable.finalize(info.id, 42, final_replay, exited_state());
        drop(durable);
        persistence.assert_exclusive_owner();
        drop(persistence);

        let (reopened, recovered) = Persistence::open(state_dir).expect("reopen coalesced state");
        assert_eq!(recovered.len(), 1);
        let rows = recovered[0].replay.chunks.len();
        assert!(
            rows <= 16,
            "400 reads of 512 B must pack into a handful of rows, got {rows}"
        );
        written.extend_from_slice(b"tail");
        let recovered_bytes: Vec<u8> = recovered[0]
            .replay
            .chunks
            .iter()
            .flat_map(|chunk| chunk.data.iter().copied())
            .collect();
        assert_eq!(
            recovered_bytes, written,
            "packing may change row boundaries, never the bytes"
        );
        assert_eq!(recovered[0].replay.latest_output_bytes, head + 4);
        drop(reopened);
    }

    // A deliberately long fixture window makes early flush tests depend on a
    // channel receipt, not on beating the production timer on a loaded host.
    fn observe_collection_wait(persistence: &Persistence) -> std::sync::mpsc::Receiver<()> {
        let (notify, reached) = std::sync::mpsc::channel();
        *mutex_lock(&persistence.inner.test_hooks.append_batch_window) =
            Some(Duration::from_secs(30));
        *mutex_lock(&persistence.inner.test_hooks.append_wait_started) = Some(notify);
        reached
    }

    #[test]
    fn spaced_appends_share_a_commit_and_barrier_flushes_without_waiting() {
        let temp = TempDir::new().unwrap();
        let state_dir = temp.path().join("state");
        let (persistence, _) = Persistence::open(&state_dir).unwrap();
        let info = running_info(RunId::new());
        let durable = persistence
            .insert_start(&test_operation_key(info.id), &info)
            .unwrap();
        let reached = observe_collection_wait(&persistence);
        expect_queued(durable.append(info.id, replay(vec![chunk(0, b"one")])));
        reached.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(durable.durable_head(), 0, "collecting is not a commit");
        expect_queued(durable.append(info.id, replay(vec![chunk(3, b"two")])));
        persistence.barrier().unwrap();
        assert_eq!(durable.durable_head(), 6);
        assert_eq!(
            persistence
                .inner
                .test_hooks
                .append_transaction_commits
                .load(Ordering::Acquire),
            1
        );
        drop(durable);
        drop(persistence);
        let (_reopened, recovered) = Persistence::open(&state_dir).unwrap();
        let bytes: Vec<u8> = recovered[0]
            .replay
            .chunks
            .iter()
            .flat_map(|chunk| chunk.data.iter().copied())
            .collect();
        assert_eq!(bytes, b"onetwo");
    }

    #[test]
    fn lifecycle_wake_flushes_collected_output_and_finalizes_the_tail() {
        let temp = TempDir::new().unwrap();
        let state_dir = temp.path().join("state");
        let (persistence, _) = Persistence::open(&state_dir).unwrap();
        let info = running_info(RunId::new());
        let durable = persistence
            .insert_start(&test_operation_key(info.id), &info)
            .unwrap();
        let reached = observe_collection_wait(&persistence);
        expect_queued(durable.append(info.id, replay(vec![chunk(0, b"one")])));
        reached.recv_timeout(Duration::from_secs(5)).unwrap();
        let finalizer = durable.clone();
        let (done, finished) = std::sync::mpsc::channel();
        let join = thread::spawn(move || {
            finalizer.finalize(info.id, 42, replay(vec![chunk(3, b"tail")]), exited_state());
            done.send(()).unwrap();
        });
        finished
            .recv_timeout(Duration::from_secs(5))
            .expect("lifecycle wake bypasses the 30-second fixture deadline");
        join.join().unwrap();
        assert!(!persistence.is_failed());
        assert_eq!(durable.durable_head(), 7);
        drop(durable);
        drop(persistence);
        let (_reopened, recovered) = Persistence::open(&state_dir).unwrap();
        assert_eq!(recovered[0].info.state, exited_state());
        let bytes: Vec<u8> = recovered[0]
            .replay
            .chunks
            .iter()
            .flat_map(|chunk| chunk.data.iter().copied())
            .collect();
        assert_eq!(bytes, b"onetail");
    }

    #[test]
    fn lifecycle_with_a_refused_wake_does_not_wait_for_the_collection_deadline() {
        let temp = TempDir::new().unwrap();
        let (persistence, _) = Persistence::open(temp.path().join("state")).unwrap();
        let info = running_info(RunId::new());
        let durable = persistence
            .insert_start(&test_operation_key(info.id), &info)
            .unwrap();
        *mutex_lock(&persistence.inner.test_hooks.append_batch_window) =
            Some(Duration::from_secs(30));
        let (paused, release) = persistence.pause_next_append();
        expect_queued(durable.append(info.id, replay(vec![chunk(0, b"x")])));
        paused.recv_timeout(Duration::from_secs(5)).unwrap();
        for offset in 1..=PERSISTENCE_QUEUE_CAPACITY {
            expect_queued(durable.append(info.id, replay(vec![chunk(offset as u64, b"x")])));
        }
        let (reply, received) = std::sync::mpsc::sync_channel(0);
        // The append lane is exactly full, so send_lifecycle cannot enqueue
        // its wake. Finalize carries the complete missing prefix, as the Run
        // transition owner does when finalization overtakes queued Appends.
        assert!(
            persistence.inner.send_lifecycle(super::Command::Finalize {
                id: info.id,
                actual_pid: 42,
                replay: replay(
                    (0..=PERSISTENCE_QUEUE_CAPACITY)
                        .map(|offset| chunk(offset as u64, b"x"))
                        .collect()
                ),
                state: exited_state(),
                source_gap_after_byte: None,
                durable_head: Arc::clone(&durable.durable_head),
                metadata_bytes: Arc::clone(&durable.metadata_bytes),
                reply,
            })
        );
        release.send(()).unwrap();
        received
            .recv_timeout(Duration::from_secs(5))
            .expect("a refused wake must not cause a 30-second collection wait")
            .expect("finalize commits the accepted prefix");
        persistence.barrier().unwrap();
        assert_eq!(
            durable.durable_head(),
            (PERSISTENCE_QUEUE_CAPACITY + 1) as u64
        );
        assert!(!persistence.is_failed());
    }

    #[test]
    fn a_silent_stream_commits_without_a_later_append_or_barrier() {
        let temp = TempDir::new().unwrap();
        let (persistence, _) = Persistence::open(temp.path().join("state")).unwrap();
        let info = running_info(RunId::new());
        let durable = persistence
            .insert_start(&test_operation_key(info.id), &info)
            .unwrap();
        expect_queued(durable.append(info.id, replay(vec![chunk(0, b"silent")])));
        let deadline = Instant::now() + Duration::from_secs(5);
        while durable.durable_head() == 0 {
            assert!(
                Instant::now() < deadline,
                "silence must not leave accepted output buffered forever"
            );
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(durable.durable_head(), 6);
        assert!(!persistence.is_failed());
    }

    #[test]
    fn arriving_appends_do_not_slide_the_first_collection_deadline() {
        let temp = TempDir::new().unwrap();
        let (persistence, _) = Persistence::open(temp.path().join("state")).unwrap();
        let info = running_info(RunId::new());
        let durable = persistence
            .insert_start(&test_operation_key(info.id), &info)
            .unwrap();
        *mutex_lock(&persistence.inner.test_hooks.append_batch_window) =
            Some(Duration::from_millis(100));
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut head = 0;
        while durable.durable_head() == 0 {
            assert!(
                Instant::now() < deadline,
                "continuous small appends must not postpone the first commit indefinitely"
            );
            if durable.append(info.id, replay(vec![chunk(head, b"x")])) {
                head += 1;
            }
            thread::sleep(Duration::from_millis(1));
        }
        assert!(head > 1);
        assert!(!persistence.is_failed());
    }

    #[test]
    fn append_collection_crash_recovers_only_committed_bytes() {
        for (phase, expected) in [("collecting", &b""[..]), ("committed", &b"onetwo"[..])] {
            let temp = TempDir::new().unwrap();
            let state_dir = temp.path().join("state");
            let output = std::process::Command::new(env::current_exe().unwrap())
                .args([
                    "--exact",
                    "persistence::tests::append_collection_crash_subprocess",
                    "--nocapture",
                ])
                .env("CTXMUX_APPEND_COLLECTION_CRASH", phase)
                .env("CTXMUX_APPEND_COLLECTION_STATE", &state_dir)
                .output()
                .unwrap();
            assert_eq!(output.status.code(), Some(86), "crash fixture: {output:?}");
            let (_persistence, recovered) = Persistence::open(&state_dir).unwrap();
            assert_eq!(recovered.len(), 1);
            assert_eq!(
                recovered[0].replay.latest_output_bytes,
                expected.len() as u64
            );
            let bytes: Vec<u8> = recovered[0]
                .replay
                .chunks
                .iter()
                .flat_map(|chunk| chunk.data.iter().copied())
                .collect();
            assert_eq!(bytes, expected, "recovery must not invent buffered output");
            assert!(matches!(
                recovered[0].info.state,
                RunState::Interrupted {
                    reason: InterruptionReason::DaemonRestart
                }
            ));
        }
    }

    #[test]
    fn append_collection_crash_subprocess() {
        let Ok(phase) = env::var("CTXMUX_APPEND_COLLECTION_CRASH") else {
            return;
        };
        let state_dir = env::var_os("CTXMUX_APPEND_COLLECTION_STATE").unwrap();
        let (persistence, _) = Persistence::open(std::path::PathBuf::from(state_dir)).unwrap();
        let info = running_info(RunId::new());
        let durable = persistence
            .insert_start(&test_operation_key(info.id), &info)
            .unwrap();
        let reached = observe_collection_wait(&persistence);
        expect_queued(durable.append(info.id, replay(vec![chunk(0, b"one")])));
        reached.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(durable.durable_head(), 0);
        expect_queued(durable.append(info.id, replay(vec![chunk(3, b"two")])));
        match phase.as_str() {
            "collecting" => {}
            "committed" => {
                persistence.barrier().unwrap();
                assert_eq!(durable.durable_head(), 6);
            }
            _ => panic!("unknown collection crash phase"),
        }
        // Terminate all threads without running Rust drops or shutdown flushes.
        std::process::exit(86);
    }

    /// A finalize that overtakes its own Run's queued appends must not latch
    /// persistence, and must not lose their bytes.
    ///
    /// This is the hazard the lifecycle lane introduces, and it is the reverse
    /// of the one that is easy to think of. The obvious worry is that the
    /// overtaken appends are LOST; they are not, because `finalize` carries the
    /// Run's full replay and `missing_chunks` commits whatever is not yet
    /// durable. The real hazard is that they are REJECTED: `append_replay`
    /// fails an append whose chunks pass `durable_head` once the Run is no
    /// longer `running`, and that error goes through `remember_failure`, which
    /// latches persistence off for every Run in the daemon.
    ///
    /// Before the lifecycle lane a finalize sat behind its Run's appends, so
    /// they always landed while the Run was still running and the guard could
    /// not fire. Now it overtakes them, so this fixture wedges the actor inside
    /// one append, queues two more appends and the finalize behind it, and
    /// releases — which is the exact interleaving production now produces.
    #[test]
    fn a_finalize_that_overtakes_its_runs_appends_does_not_latch() {
        let temp = TempDir::new().expect("create overtake fixture");
        let state_dir = temp.path().join("state");
        let (persistence, recovered) = Persistence::open(&state_dir).expect("open persistence");
        assert!(recovered.is_empty());

        let info = running_info(RunId::new());
        let durable = persistence
            .insert_start(&test_operation_key(info.id), &info)
            .expect("insert overtake fixture");

        // Wedge the actor inside the first append so everything after it queues.
        // The pause fires BEFORE the batching loop, so on release the actor
        // would otherwise swallow every queued append into that one batch and
        // no overtake would occur — which is what an earlier version of this
        // fixture measured while passing. The finalize is therefore sent while
        // the actor is still wedged, so it is sitting on the lifecycle lane
        // when the actor next picks, and the appends that arrive after it are
        // the ones it overtakes.
        let (reached, release) = persistence.pause_next_append();
        let first = [b'a'; 64];
        assert!(durable.append(info.id, replay(vec![chunk(0, &first)])));
        reached
            .recv()
            .expect("the actor reaches the append barrier");

        let second = [b'b'; 64];
        let third = [b'c'; 64];
        let mut whole = Vec::new();
        whole.extend_from_slice(&first);
        whole.extend_from_slice(&second);
        whole.extend_from_slice(&third);

        // The finalize carries the Run's whole replay, as the real publication
        // path does.
        let finalize_replay = replay(vec![
            chunk(0, &first),
            chunk(64, &second),
            chunk(128, &third),
        ]);
        let durable_for_thread = durable.clone();
        let id = info.id;
        let finalizer = std::thread::spawn(move || {
            durable_for_thread.finalize(id, 42, finalize_replay, exited_state());
        });
        // Give the finalize time to land on the lifecycle lane, then queue the
        // appends it must overtake.
        std::thread::sleep(std::time::Duration::from_millis(50));
        let late_second = durable.append(info.id, replay(vec![chunk(64, &second)]));
        let late_third = durable.append(info.id, replay(vec![chunk(128, &third)]));
        assert!(
            late_second && late_third,
            "the append channel must still accept; a refusal would mean this \
             fixture never created the overtake it exists to test"
        );

        release.send(()).expect("release the append barrier");
        finalizer.join().expect("the finalize completes");

        // The overtake must have actually happened, or this fixture is
        // asserting nothing. When `finalize` takes the lifecycle lane it is
        // dequeued BEFORE the two late appends, so by the time it returns the
        // Run is already terminal and its durable head already covers all
        // three chunks. Routed on the append lane instead, the finalize is
        // dequeued last and this is still true at the end — so the head alone
        // cannot distinguish them. What can: on the lifecycle lane the late
        // appends are dequeued against a Run that is ALREADY terminal, which
        // is precisely the state `append_replay` refuses to advance. A latch
        // here is the failure this fixture exists to catch.
        assert_eq!(
            durable.durable_head(),
            192,
            "the finalize must have committed all three chunks"
        );
        // Let the overtaken appends be dequeued against the now-terminal Run.
        persistence.barrier().expect("drain the overtaken appends");

        // The overtaken appends are dequeued after the Run is terminal. Their
        // bytes are already durable via the finalize, so they must be skipped
        // rather than rejected.
        assert!(
            !persistence.is_failed(),
            "a finalize overtaking its own Run's queued appends latched persistence"
        );

        drop(durable);
        persistence.assert_exclusive_owner();
        drop(persistence);

        let (reopened, recovered) = Persistence::open(state_dir).expect("reopen overtaken state");
        assert_eq!(recovered.len(), 1);
        let recovered_bytes: Vec<u8> = recovered[0]
            .replay
            .chunks
            .iter()
            .flat_map(|chunk| chunk.data.iter().copied())
            .collect();
        assert_eq!(
            recovered_bytes, whole,
            "overtaking may reorder the commits, never drop the bytes"
        );
        drop(reopened);
    }

    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "one minimal eviction/finalization regression joins unchanged-byte refusal, neighboring commit and exact cold recovery"
    )]
    fn finalization_preserves_a_fresh_suffix_after_global_prefix_eviction() {
        let temp = TempDir::new().unwrap();
        let state_dir = temp.path().join("state");
        let hooks = Arc::new(PersistenceTestHooks::default());
        let (mut store, _) =
            StateStore::open(&state_dir, &AdmissionLimits::OPERATIONAL, None, hooks).unwrap();
        let first = RunId::new();
        let neighbor = RunId::new();
        let transaction = store.connection.transaction().unwrap();
        for id in [first, neighbor] {
            let metadata = insert_test_run(&transaction, id, "running", 1);
            transaction
                .execute(
                    "UPDATE runs SET metadata_bytes = ?2 WHERE id = ?1",
                    params![id.to_string(), metadata],
                )
                .unwrap();
        }
        transaction.commit().unwrap();
        let first_head = Arc::new(AtomicU64::new(0));
        let neighbor_head = Arc::new(AtomicU64::new(0));
        store
            .append_batch(&[
                (
                    first,
                    replay(vec![chunk(0, b"abcdef")]),
                    Arc::clone(&first_head),
                ),
                (
                    neighbor,
                    replay(vec![chunk(0, b"NEIGHBOR")]),
                    Arc::clone(&neighbor_head),
                ),
            ])
            .unwrap();
        // Byte-sized coordinates isolate the same real eviction/overlap as
        // the fleet failure; eight bytes is a probe policy, not product capacity.
        let transaction = store.connection.transaction().unwrap();
        assert!(prune_global_replay_to(&transaction, 8).unwrap());
        transaction.commit().unwrap();
        let floor: i64 = store
            .connection
            .query_row(
                "SELECT durable_first_available_byte FROM runs WHERE id = ?1",
                [first.to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(floor, 2);
        assert_eq!(first_head.load(Ordering::Acquire), 6);
        let metadata = Arc::new(AtomicU64::new(0));
        let forged = replay(vec![chunk(0, b"abCdefgh")]);
        assert!(matches!(
            store.finalize_with_shutdown(
                first,
                42,
                &forged,
                &exited_state(),
                &first_head,
                &metadata,
                None,
                None
            ),
            Err(PersistenceError::Mutation(_))
        ));
        let original = replay(vec![chunk(0, b"abcdefgh")]);
        store
            .finalize_with_shutdown(
                first,
                42,
                &original,
                &exited_state(),
                &first_head,
                &metadata,
                None,
                None,
            )
            .expect("confirmed evicted prefix must not reject the original fresh suffix");
        assert_eq!(first_head.load(Ordering::Acquire), 8);
        store
            .append_batch(&[(
                neighbor,
                replay(vec![chunk(8, b"-live")]),
                Arc::clone(&neighbor_head),
            )])
            .expect("neighbor keeps committing after local finalization");
        store
            .finalize_with_shutdown(
                neighbor,
                43,
                &replay(Vec::new()),
                &exited_state(),
                &neighbor_head,
                &Arc::new(AtomicU64::new(0)),
                None,
                None,
            )
            .unwrap();
        drop(store);
        let (reopened, recovered) = Persistence::open(state_dir).unwrap();
        for (id, expected, head) in [
            (first, b"cdefgh".as_slice(), 8),
            (neighbor, b"HBOR-live".as_slice(), 13),
        ] {
            let saved = recovered.iter().find(|saved| saved.info.id == id).unwrap();
            let bytes: Vec<u8> = saved
                .replay
                .chunks
                .iter()
                .flat_map(|chunk| chunk.data.iter().copied())
                .collect();
            assert_eq!(bytes, expected);
            assert_eq!(saved.info.state, exited_state());
            assert_eq!(saved.replay.latest_output_bytes, head);
            assert!(saved.replay.truncated);
        }
        drop(reopened);
    }

    #[test]
    fn queued_prefix_eviction_during_finalization_keeps_the_actor_available() {
        let temp = TempDir::new().unwrap();
        let state_dir = temp.path().join("state");
        let resources = crate::ResourceLimits {
            durable_run_output_bytes: 8,
            ..crate::ResourceLimits::DEFAULT
        };
        let (persistence, _) =
            Persistence::open_with_resources(state_dir.clone(), resources, None).unwrap();
        let info = running_info(RunId::new());
        let durable = persistence
            .insert_start(&test_operation_key(info.id), &info)
            .unwrap();
        let (reached, release) = persistence.pause_next_append();
        assert!(durable.append(info.id, replay(vec![chunk(0, &[b'a'; 64])])));
        reached.recv().unwrap();
        let mut whole = vec![b'a'; 64];
        whole.extend([b'b'; 64]);
        whole.extend([b'c'; 64]);
        let (reply, completed) = std::sync::mpsc::sync_channel(1);
        assert!(persistence.inner.send_lifecycle(super::Command::Finalize {
            id: info.id,
            actual_pid: 42,
            replay: replay(vec![chunk(0, &whole)]),
            state: exited_state(),
            source_gap_after_byte: None,
            durable_head: Arc::clone(&durable.durable_head),
            metadata_bytes: Arc::clone(&durable.metadata_bytes),
            reply,
        }));
        assert!(durable.append(info.id, replay(vec![chunk(64, &[b'b'; 64])])));
        assert!(durable.append(info.id, replay(vec![chunk(128, &[b'c'; 64])])));
        release.send(()).unwrap();
        completed.recv().unwrap().unwrap();
        persistence.barrier().unwrap();
        assert_eq!(durable.durable_head(), 192);
        assert!(
            !persistence.is_failed(),
            "accepted bytes covered by terminal settlement must not poison the shared actor"
        );
        let neighbor = running_info(RunId::new());
        let neighbor_durable = persistence
            .insert_start(&test_operation_key(neighbor.id), &neighbor)
            .expect("neighbor can still start after prefix eviction");
        assert!(neighbor_durable.append(neighbor.id, replay(vec![chunk(0, b"neighbor")])));
        persistence.barrier().unwrap();
        neighbor_durable.finalize(neighbor.id, 43, replay(Vec::new()), exited_state());
        assert!(!persistence.is_failed());
        drop(neighbor_durable);
        drop(durable);
        persistence.assert_exclusive_owner();
        drop(persistence);
        let (reopened, recovered) =
            Persistence::open_with_resources(state_dir, resources, None).unwrap();
        for (id, expected, head) in [
            (info.id, b"cccccccc".as_slice(), 192),
            (neighbor.id, b"neighbor".as_slice(), 8),
        ] {
            let saved = recovered.iter().find(|run| run.info.id == id).unwrap();
            let bytes: Vec<u8> = saved
                .replay
                .chunks
                .iter()
                .flat_map(|chunk| chunk.data.iter().copied())
                .collect();
            assert_eq!(bytes, expected);
            assert_eq!(saved.info.state, exited_state());
            assert_eq!(saved.info.durable_output_bytes, Some(head));
        }
        drop(reopened);
    }

    /// A re-sent append must still verify when its range is a SLICE of a
    /// coalesced row rather than a row of its own.
    ///
    /// This is the edge coalescing introduces. Before, a re-sent range was
    /// looked up by exact `start_byte` and compared whole. Now the bytes it
    /// covers usually sit in the middle of a much larger row, so a lookup keyed
    /// on its own start byte finds nothing and reports honest, durable bytes as
    /// lost — which `remember_failure` then latches daemon-wide.
    #[test]
    fn a_resent_append_verifies_against_a_slice_of_its_row() {
        let temp = TempDir::new().expect("create slice-verify fixture");
        let state_dir = temp.path().join("state");
        let (persistence, recovered) = Persistence::open(&state_dir).expect("open persistence");
        assert!(recovered.is_empty());

        let info = running_info(RunId::new());
        let durable = persistence
            .insert_start(&test_operation_key(info.id), &info)
            .expect("insert slice-verify fixture");

        // Three appends, then re-send the MIDDLE one: its start byte is not the
        // start byte of the row that now holds it.
        let first = replay(vec![chunk(0, b"first-")]);
        let middle = replay(vec![chunk(6, b"middle-")]);
        let last = replay(vec![chunk(13, b"last")]);
        for one in [&first, &middle, &last] {
            expect_queued(durable.append(info.id, one.clone()));
        }
        persistence.barrier().expect("the appends commit");
        assert!(!persistence.is_failed());
        assert_eq!(durable.durable_head(), 17);

        expect_queued(durable.append(info.id, middle.clone()));
        persistence
            .barrier()
            .expect("a re-sent interior range is a verified no-op");
        assert!(
            !persistence.is_failed(),
            "re-sending bytes that sit inside a coalesced row must verify, not latch"
        );
        assert_eq!(durable.durable_head(), 17, "verification advances nothing");

        // Same range, different bytes. Without this the test would also pass on
        // a lookup that accepts anything it finds.
        let forged = replay(vec![chunk(6, b"MIDDLE-")]);
        expect_queued(durable.append(info.id, forged));
        let _ = persistence.barrier();
        assert!(
            persistence.is_failed(),
            "changed bytes at a durable range must fail, or verification proves nothing"
        );
        drop(durable);
        drop(persistence);
    }

    /// Retrying a mutation whose commit result is unknown must not double-commit.
    ///
    /// This is the risk the retry introduces: an `fsync` can fail with `ENOSPC`
    /// *after* the transaction already reached the disk, so a retry may re-run a
    /// write that landed. The store is what makes that safe, and this pins the
    /// mechanism rather than assuming it. `append_replay` classifies each chunk
    /// against the durable head — bytes at or below it are re-verified for exact
    /// equality and skipped, a chunk that does not abut the head is refused as a
    /// gap, and only an exactly-abutting chunk inserts. Replaying the same append
    /// is therefore a checked no-op, not a duplicate row.
    #[test]
    fn replaying_a_committed_append_verifies_instead_of_duplicating() {
        let temp = TempDir::new().expect("create replay-idempotence fixture");
        let state_dir = temp.path().join("state");
        let (persistence, recovered) = Persistence::open(&state_dir).expect("open persistence");
        assert!(recovered.is_empty());

        let info = running_info(RunId::new());
        let durable = persistence
            .insert_start(&test_operation_key(info.id), &info)
            .expect("insert replay-idempotence fixture");
        let committed = replay(vec![chunk(0, b"committed once")]);
        expect_queued(durable.append(info.id, committed.clone()));
        persistence.barrier().expect("first append commits");
        assert!(!persistence.is_failed());
        let head_after_first = durable.durable_head();
        assert_eq!(head_after_first, committed.latest_output_bytes);

        // Re-submit the identical append, exactly as a retry after an uncertain
        // commit would. It must be accepted as an already-durable no-op.
        expect_queued(durable.append(info.id, committed.clone()));
        persistence
            .barrier()
            .expect("a replayed append is a verified no-op, not a commit failure");
        assert!(
            !persistence.is_failed(),
            "re-appending already-durable bytes must verify, not fail the actor"
        );
        assert_eq!(
            durable.durable_head(),
            head_after_first,
            "a replayed append must not advance the durable head twice"
        );

        durable.finalize(info.id, 42, committed.clone(), exited_state());
        drop(durable);
        persistence.assert_exclusive_owner();
        drop(persistence);

        let (reopened, recovered) = Persistence::open(state_dir).expect("reopen recovered state");
        assert_eq!(recovered.len(), 1, "one Run, not one per append attempt");
        assert_eq!(
            recovered[0].replay, committed,
            "the recovered bytes must be the single committed copy"
        );
        drop(reopened);
    }

    #[test]
    fn rejected_start_admission_does_not_poison_the_actor() {
        let temp = TempDir::new().expect("create persistence admission fixture");
        let state_dir = temp.path().join("state");
        let limits = AdmissionLimits {
            run_records: 1,
            metadata_bytes: METADATA_BYTES,
            resources: ResourceLimits::DEFAULT,
        };
        let (persistence, recovered) =
            Persistence::open_with_admission_limits(state_dir.clone(), &limits, None)
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
        expect_queued(first_durable.append(first.id, first_replay.clone()));
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
            resources: ResourceLimits::DEFAULT,
        };
        let (persistence, recovered) =
            Persistence::open_with_admission_limits(state_dir.clone(), &limits, None)
                .expect("open exact-candidate store");
        assert!(recovered.is_empty());

        let first = running_info(RunId::new());
        let first_key = test_operation_key(first.id);
        let first_durable = persistence
            .insert_start(&first_key, &first)
            .expect("insert candidate");
        let first_replay = replay(vec![chunk(0, b"retained")]);
        expect_queued(first_durable.append(first.id, first_replay.clone()));
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
        expect_queued(durable.append(first.id, replay(vec![chunk(0, b"committed")])));
        persistence
            .barrier()
            .expect("establish the known committed prefix");
        expect_queued(durable.append(first.id, replay(vec![chunk(0, b"conflict")])));
        persistence
            .barrier()
            .expect_err("resolve the conflicting append before a later lifecycle mutation");
        assert!(!durable.append(first.id, replay(vec![chunk(9, b"refused")])));
        assert!(!durable.append_blocking(first.id, replay(vec![chunk(9, b"refused")])));

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

    fn run_replay_compaction_crash_subprocess(state_dir: &Path, phase: &str) -> ProcessOutput {
        let child = ProcessCommand::new(env::current_exe().expect("resolve unit test binary"))
            .arg("--exact")
            .arg("persistence::tests::replay_compaction_crash_subprocess")
            .arg("--nocapture")
            .env(REPLAY_COMPACTION_CRASH_STATE_DIR, state_dir)
            .env(REPLAY_COMPACTION_CRASH_PHASE, phase)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn isolated replay compaction crash fixture");
        wait_for_test_subprocess(child, &format!("{phase}-replay-compaction"))
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
            &AdmissionLimits::FORMAT,
            None,
        )
        .expect("open format-envelope persistence");
        assert!(recovered.is_empty());
        let count = EVICTION_TEST_CEILING + 3;
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
                expect_queued(durable.append(id, retained.clone()));
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
        create_schema(
            &connection,
            &uuid::Uuid::new_v4().to_string(),
            &format!("replay-test-{}.bin", uuid::Uuid::new_v4()),
        )
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
    ) -> i64 {
        let spec = RunSpec {
            program: "/bin/true".to_owned(),
            args: Vec::new(),
            cwd: None,
            env: BTreeMap::new(),
            initial_size: TerminalSize::default(),
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
        let actual_metadata = metadata_size(
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
        i64::try_from(actual_metadata).expect("test metadata fits SQLite")
    }

    fn running_info(id: RunId) -> RunInfo {
        RunInfo {
            id,
            spec: Some(RunSpec {
                program: "/bin/true".to_owned(),
                args: Vec::new(),
                cwd: None,
                env: BTreeMap::new(),
                initial_size: TerminalSize::default(),
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
            current_size: Some(TerminalSize { cols: 80, rows: 24 }),
            native_service: None,
        }
    }

    fn test_operation_key(id: RunId) -> CreateOperationKey {
        CreateOperationKey::new(format!("test-{id}")).expect("valid test operation key")
    }

    fn sqlite_busy_error() -> PersistenceError {
        PersistenceError::database(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_BUSY),
            Some("database is busy".to_owned()),
        ))
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
            &AdmissionLimits::OPERATIONAL,
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
            &AdmissionLimits::OPERATIONAL,
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
            &AdmissionLimits::FORMAT,
            None,
        )
        .expect("open format-envelope persistence");
        assert!(recovered.is_empty());
        let count = EVICTION_TEST_CEILING + 3;
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
                expect_queued(durable.append(id, replay(Vec::new())));
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
    fn over_budget_handoff_preserves_live_and_terminal_history() {
        let fixture = TempDir::new().unwrap();
        let state_dir = fixture.path().join("state");
        let (live_id, epoch) = seed_startup_overflow_with_live_row(&state_dir);
        let before = raw_run_units(&state_dir);
        let Err(error) = StateStore::open(
            &state_dir,
            &EVICTION_TEST_LIMITS,
            Some(super::HandoffHint {
                epoch: epoch.clone(),
                live_set: HashSet::from([live_id]),
                state_lock_fd: None,
            }),
            Arc::new(PersistenceTestHooks::default()),
        ) else {
            panic!("undersized handoff policy opened");
        };
        assert!(matches!(error, PersistenceError::ResourcePressure(_)));
        assert_eq!(raw_run_units(&state_dir), before);
        let connection = Connection::open(state_dir.join(DATABASE_FILE)).unwrap();
        assert_eq!(row_state_kind(&connection, live_id), "running");
        assert_eq!(published_epoch(&connection), epoch);
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

    /// A dropped append must not stall the caller, and must not lose bytes.
    ///
    /// This is the whole justification for `append`'s non-blocking send. The
    /// caller is the daemon-wide output reader thread, so blocking it would
    /// stop every Run's pty from being drained. Dropping is only sound because
    /// the replay is rendered from `durable_head`, which a dropped append
    /// leaves unmoved — so the next append re-offers the same bytes.
    #[test]
    fn a_dropped_append_is_recovered_by_the_next_one() {
        let temp = TempDir::new().expect("create dropped-append fixture");
        let state_dir = temp.path().join("state");
        let (persistence, recovered) = Persistence::open(&state_dir).expect("open persistence");
        assert!(recovered.is_empty());

        let info = running_info(RunId::new());
        let durable = persistence
            .insert_start(&test_operation_key(info.id), &info)
            .expect("insert dropped-append fixture");

        // Hold the actor inside its first append so the queue backs up behind
        // it, standing in for a slow fsync.
        let (reached, release) = persistence.pause_next_append();
        expect_queued(durable.append(info.id, replay(vec![chunk(0, b"first")])));
        reached
            .recv()
            .expect("the actor reaches the append barrier");

        // Fill the queue past its depth from another thread, so a send that
        // blocks fails this test on a deadline instead of hanging it. Every one
        // of these would have blocked the output reader before this fix; here
        // they are dropped instead. The watermark has not moved, so each
        // carries the same bytes from 0.
        let filler = durable.clone();
        let filler_id = info.id;
        let (filled_tx, filled_rx) = super::mpsc::sync_channel(0);
        let fill = thread::Builder::new()
            .name("dropped-append-filler".to_owned())
            .spawn(move || {
                let stalled = replay(vec![chunk(0, b"first")]);
                for _ in 0..(PERSISTENCE_QUEUE_CAPACITY * 2) {
                    // A refusal is the POINT here: this loop exists to saturate the queue.
                    let _ = filler.append(filler_id, stalled.clone());
                }
                let _ = filled_tx.send(());
            })
            .expect("spawn the queue filler");

        let overran = filled_rx.recv_timeout(Duration::from_secs(30)).is_err();
        assert!(
            !overran,
            "appends must never block on a stalled actor: this is the daemon-wide \
             output reader, so a blocking send stops every Run's pty from draining"
        );
        assert_eq!(
            durable.durable_head(),
            0,
            "nothing can be durable while the actor is held at the barrier"
        );

        release.send(()).expect("release the append barrier");
        fill.join().expect("the queue filler finishes");

        // The bytes that were dropped are carried by the next catch-up, which
        // still starts at the unmoved watermark and now extends past it.
        let whole = replay(vec![chunk(0, b"first"), chunk(5, b"-second")]);
        durable.finalize(info.id, 42, whole.clone(), exited_state());
        assert!(
            !persistence.is_failed(),
            "a dropped append must not latch persistence"
        );

        drop(durable);
        persistence.assert_exclusive_owner();
        drop(persistence);

        let (reopened, recovered) = Persistence::open(state_dir).expect("reopen recovered state");
        let run = recovered
            .iter()
            .find(|run| run.info.id == info.id)
            .expect("the Run survives the dropped appends");
        assert_eq!(
            run.replay
                .chunks
                .iter()
                .flat_map(|chunk| chunk.data.iter().copied())
                .collect::<Vec<_>>(),
            whole
                .chunks
                .iter()
                .flat_map(|chunk| chunk.data.iter().copied())
                .collect::<Vec<_>>(),
            "every byte must be durable despite the dropped appends"
        );
        drop(reopened);
    }

    /// A catch-up must never re-send a byte the actor is already holding.
    ///
    /// This is the performance half of the offered-watermark contract, and the
    /// half wall-clock alone cannot confirm. Catching up from the COMMITTED
    /// watermark re-sent every byte queued but not yet committed, which
    /// overlapped the appends ahead of it. `append_batch_with_shutdown`
    /// re-splits by contiguity, so an overlapping replay is never
    /// `is_fresh_contiguous`: it gets a transaction, and an fsync, entirely to
    /// itself, and each already-committed chunk in the overlap additionally
    /// takes the verify-against-stored branch (an indexed range lookup plus a
    /// bounded file read). A queue N deep cost N transactions instead of one, and every
    /// lifecycle verb waits behind all of them in the same FIFO — which is why
    /// ONE chatty Run cost seconds per create rather than a bounded penalty.
    ///
    /// The overlap is the defect, so the overlap is what this measures: the
    /// distance between what the next replay would re-send and what the actor
    /// has already been given. Asserting on transaction counts instead needs the
    /// actor to be slow-but-draining at exactly the right rate, which makes the
    /// fixture time-dependent; this states the same invariant with no race.
    #[test]
    fn a_catch_up_after_a_refusal_does_not_re_send_queued_bytes() {
        const CHUNK: &[u8] = b"bbbb";

        let temp = TempDir::new().expect("create catch-up overlap fixture");
        let state_dir = temp.path().join("state");
        let (persistence, recovered) = Persistence::open(&state_dir).expect("open persistence");
        assert!(recovered.is_empty());

        let info = running_info(RunId::new());
        let durable = persistence
            .insert_start(&test_operation_key(info.id), &info)
            .expect("insert catch-up overlap fixture");

        // Hold the actor so appends pile up ACCEPTED but UNCOMMITTED. This is
        // the state the whole contract is about: `durable_head` stays at 0 while
        // the actor's pending watermark runs far ahead of it.
        let (reached, release) = persistence.pause_next_append();
        expect_queued(durable.append(info.id, replay(vec![chunk(0, CHUNK)])));
        reached
            .recv()
            .expect("the actor reaches the append barrier");

        let mut offered_through = CHUNK.len() as u64;
        let mut refusals = 0_usize;
        for index in 1..=(PERSISTENCE_QUEUE_CAPACITY + 64) {
            let offset = (index as u64) * CHUNK.len() as u64;
            if durable.append(info.id, replay(vec![chunk(offset, CHUNK)])) {
                offered_through = offset + CHUNK.len() as u64;
            } else {
                refusals += 1;
            }
        }
        assert!(
            refusals > 0,
            "the fixture must actually overflow the queue; without a refusal \
             there is no catch-up and the test proves nothing"
        );
        assert_eq!(
            durable.durable_head(),
            0,
            "nothing can be durable while the actor is held at the barrier"
        );

        // The catch-up the next push would render. Every byte below the offered
        // watermark is already in the actor's hands.
        let committed = durable.durable_head();
        assert_eq!(
            durable.next_replay_start(),
            offered_through,
            "a catch-up must resume where the last ACCEPTED append ended. \
             Starting at the committed watermark ({committed}) instead would \
             re-send {} queued bytes, and that overlap is what fragments the \
             actor's batching into one fsync per append.",
            offered_through - committed,
        );

        release.send(()).expect("release the append barrier");
        persistence.barrier().expect("drain the queued appends");

        drop(durable);
        persistence.assert_exclusive_owner();
        drop(persistence);
    }

    /// A barrier fences only what was OFFERED, so a final blocking offer is what
    /// makes it fence what was READ.
    ///
    /// The exec-in-place invariant, stated where it can actually fail. Ordinary
    /// admission may refuse an append and rely on the next push re-offering
    /// those bytes; extract stops every pty reader, so on the upgrade path there
    /// is no next push. Without [`PersistentRun::append_blocking`] the refused
    /// bytes are simply never offered, the barrier returns satisfied anyway, and
    /// the upgrade exec's over output it has told itself is durable.
    ///
    /// This drives exactly that sequence — refuse, then offer once, blocking,
    /// then barrier — and asserts from the REOPENED database, so it measures
    /// durability rather than queue bookkeeping. With `append_blocking` replaced
    /// by `append` the refusal is unrecovered and the replay comes back short.
    #[test]
    fn a_blocking_offer_makes_the_barrier_fence_every_read_byte() {
        const CHUNK: &[u8] = b"cccc";

        let temp = TempDir::new().expect("create handoff-offer fixture");
        let state_dir = temp.path().join("state");
        let (persistence, recovered) = Persistence::open(&state_dir).expect("open persistence");
        assert!(recovered.is_empty());

        let info = running_info(RunId::new());
        let durable = persistence
            .insert_start(&test_operation_key(info.id), &info)
            .expect("insert handoff-offer fixture");

        // Hold the actor so the queue saturates behind it, exactly as a slow
        // fsync does under a chatty fleet.
        let (reached, release) = persistence.pause_next_append();
        expect_queued(durable.append(info.id, replay(vec![chunk(0, CHUNK)])));
        reached
            .recv()
            .expect("the actor reaches the append barrier");

        // Push until the queue refuses. Each refused append leaves the offered
        // watermark behind the bytes the reader has already consumed — the debt
        // that the next push would normally carry.
        let mut whole = Vec::from(CHUNK);
        let mut refusals = 0_usize;
        for index in 1..=(PERSISTENCE_QUEUE_CAPACITY + 64) {
            let offset = (index as u64) * CHUNK.len() as u64;
            if !durable.append(info.id, replay(vec![chunk(offset, CHUNK)])) {
                refusals += 1;
            }
            whole.extend_from_slice(CHUNK);
        }
        assert!(
            refusals > 0,
            "the fixture must actually overflow the queue; with no refusal there \
             is no outstanding debt and this test proves nothing"
        );
        let outstanding = whole.len() as u64 - durable.next_replay_start();
        assert!(
            outstanding > 0,
            "a refusal must leave bytes unoffered, otherwise the blocking offer \
             below has nothing to settle"
        );

        // THE HANDOFF STEP. No further push will ever come: on the real path
        // extract has already stopped this Run's reader. The queue is STILL full
        // here, which is the whole point — a non-blocking append refuses at this
        // instant and the debt is lost, so the offer must wait for a slot rather
        // than drop. A helper releases the actor shortly, standing in for the
        // fsync that eventually completes.
        assert!(
            !durable.queue_has_room(),
            "the offer must be made while the queue is full, or a non-blocking \
             append would succeed too and this test would not discriminate"
        );
        let releaser = thread::Builder::new()
            .name("handoff-offer-releaser".to_owned())
            .spawn(move || {
                thread::sleep(Duration::from_millis(200));
                let _ = release.send(());
            })
            .expect("spawn the barrier releaser");

        let outstanding_from = durable.next_replay_start();
        let offer_from = usize::try_from(outstanding_from).expect("fixture offsets fit usize");
        assert!(
            durable.append_blocking(
                info.id,
                replay(vec![chunk(outstanding_from, &whole[offer_from..],)]),
            ),
            "the final handoff offer must be accepted, not dropped: there is no \
             next push to re-offer these bytes once extract has stopped the reader"
        );
        releaser.join().expect("the barrier releaser finishes");
        persistence.barrier().expect("fence the handoff offer");

        assert_eq!(
            durable.durable_head(),
            whole.len() as u64,
            "after the blocking offer the barrier must fence every byte read, \
             not merely every byte a non-blocking append happened to place"
        );

        drop(durable);
        persistence.assert_exclusive_owner();
        drop(persistence);

        // Durability, read back from disk: this is the property the upgrade path
        // depends on, and the one a queue-depth assertion cannot establish.
        let (reopened, recovered) = Persistence::open(state_dir).expect("reopen handoff state");
        let run = recovered
            .iter()
            .find(|run| run.info.id == info.id)
            .expect("the Run survives the handoff offer");
        let recovered_bytes: Vec<u8> = run
            .replay
            .chunks
            .iter()
            .flat_map(|chunk| chunk.data.clone())
            .collect();
        assert_eq!(
            run.replay.latest_output_bytes,
            whole.len() as u64,
            "the recovered cursor must cover every byte the reader consumed"
        );
        assert_eq!(
            recovered_bytes, whole,
            "every byte the reader consumed before the handoff must be durable; \
             a short replay here is the exec-over-unoffered-output defect"
        );
        drop(reopened);
    }

    /// The catch-up re-sends bytes that are already durable. That must be a
    /// verified no-op, not a duplicate — otherwise the fix would corrupt the
    /// replay on every push after the first.
    #[test]
    fn re_sending_durable_bytes_does_not_duplicate_them() {
        let temp = TempDir::new().expect("create catch-up fixture");
        let state_dir = temp.path().join("state");
        let (persistence, recovered) = Persistence::open(&state_dir).expect("open persistence");
        assert!(recovered.is_empty());

        let info = running_info(RunId::new());
        let durable = persistence
            .insert_start(&test_operation_key(info.id), &info)
            .expect("insert catch-up fixture");

        let first = replay(vec![chunk(0, b"alpha")]);
        expect_queued(durable.append(info.id, first.clone()));
        durable.finalize(info.id, 42, first.clone(), exited_state());
        assert_eq!(durable.durable_head(), first.latest_output_bytes);

        drop(durable);
        persistence.assert_exclusive_owner();
        drop(persistence);

        let (reopened, recovered) = Persistence::open(state_dir).expect("reopen recovered state");
        let run = recovered
            .iter()
            .find(|run| run.info.id == info.id)
            .expect("the Run is durable");
        assert_eq!(
            run.replay, first,
            "re-offered durable bytes are verified, never appended twice"
        );
        drop(reopened);
    }
}

// Derived terminal state uses its own private atomic Run-keyed file, without a SQL schema change.
const MAX_CHECKPOINT_FILE_BYTES: usize = MAX_RESTORE_BYTES * 2;
// One sequential checkpoint writer's buffer, not an accepted-checkpoint ceiling.
// Source-extracted 1/8/32 MiB probes compared 8/64/256 KiB buffers. At 32 MiB,
// 256 KiB reduces file writes from 5,462 to 171 and removes the default buffer's
// measured instruction overhead while avoiding both complete encoded copies.
// Final host latency and aggregate allocation qualification remain separate.
const CHECKPOINT_WRITE_BUFFER_BYTES: usize = 256 * 1024;
fn terminal_checkpoint_path(state_dir: &Path, id: RunId) -> PathBuf {
    state_dir
        .join("terminal-checkpoints")
        .join(format!("{id}.json"))
}
#[derive(serde::Serialize, serde::Deserialize)]
struct PersistedTerminalCheckpoint {
    epoch: String,
    state: Option<StoredCheckpoint>,
}
fn write_terminal_checkpoint(
    state_dir: &Path,
    id: RunId,
    saved: &PersistedTerminalCheckpoint,
) -> io::Result<()> {
    write_terminal_checkpoint_bounded(state_dir, id, saved, MAX_CHECKPOINT_FILE_BYTES)
}

fn write_terminal_checkpoint_bounded(
    state_dir: &Path,
    id: RunId,
    saved: &PersistedTerminalCheckpoint,
    max_file_bytes: usize,
) -> io::Result<()> {
    if saved
        .state
        .as_ref()
        .is_some_and(|s| s.restore.len() > MAX_RESTORE_BYTES)
    {
        return Err(io::Error::other("checkpoint exceeds bound"));
    }
    let directory = state_dir.join("terminal-checkpoints");
    prepare_state_dir(&directory).map_err(io::Error::other)?;
    let temporary = directory.join(format!(".{}.tmp", Uuid::new_v4()));
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)?;
        {
            // Keep serializer chunks buffered instead of issuing a file write for
            // every escaped string fragment. Admission precedes buffering; no
            // complete encoded checkpoint or JSON copy is allocated here.
            let mut output = CheckpointWriter {
                writer: io::BufWriter::with_capacity(
                    CHECKPOINT_WRITE_BUFFER_BYTES.min(max_file_bytes),
                    &mut file,
                ),
                remaining: max_file_bytes,
            };
            serde_json::to_writer(&mut output, saved).map_err(io::Error::other)?;
            output.flush()?;
        }
        file.sync_all()?;
        fs::rename(&temporary, terminal_checkpoint_path(state_dir, id))?;
        File::open(directory)?.sync_all()
    })();
    if result.is_err() {
        let _ = fs::remove_file(temporary);
    }
    result
}

/// Count serialized bytes before they enter the bounded file buffer. The existing
/// file policy is separate from JSON/base64 validity and is unchanged here.
struct CheckpointWriter<W> {
    writer: W,
    remaining: usize,
}

impl<W: Write> Write for CheckpointWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.remaining {
            return Err(io::Error::other("checkpoint file exceeds bound"));
        }
        let written = self.writer.write(bytes)?;
        self.remaining -= written;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

#[cfg(test)]
mod checkpoint_stream_tests {
    use super::{
        CheckpointWriter, MAX_CHECKPOINT_FILE_BYTES, PersistedTerminalCheckpoint,
        terminal_checkpoint_path, write_terminal_checkpoint, write_terminal_checkpoint_bounded,
    };
    use ctxmux_protocol::RunId;
    use std::{
        fs,
        io::{self, Write},
        os::unix::fs::PermissionsExt,
    };

    #[test]
    fn checkpoint_stream_preserves_binary_format_and_roundtrip() {
        use base64::{Engine as _, engine::general_purpose::STANDARD};
        use ctxmux_protocol::TerminalSize;

        // Padding, encoder chunk boundaries, and multi-chunk opaque PTY bytes.
        for len in [0, 1, 2, 3, 766, 767, 768, 769, 1023, 1024, 4097] {
            let bytes: Vec<u8> = (0_u8..=255).cycle().take(len).collect();
            let model = crate::terminal_checkpoint::TerminalModel::new(
                RunId::new(),
                TerminalSize { rows: 2, cols: 4 },
            );
            let mut state = model.stored().expect("checkpoint");
            state.restore.clone_from(&bytes);
            state.checkpoint.restore_bytes = u64::try_from(len).expect("length");
            let saved = PersistedTerminalCheckpoint {
                epoch: "non-ASCII 雪 \n\t\"\\".to_owned(),
                state: Some(state),
            };
            let encoded = serde_json::to_vec(&saved).expect("serialize");
            let value: serde_json::Value = serde_json::from_slice(&encoded).expect("JSON");
            assert_eq!(value["state"]["restore"], STANDARD.encode(&bytes));
            let recovered: PersistedTerminalCheckpoint =
                serde_json::from_slice(&encoded).expect("recover");
            assert_eq!(recovered.epoch, saved.epoch);
            assert_eq!(recovered.state.expect("state").restore, bytes);
        }
    }

    #[test]
    fn checkpoint_stream_failure_preserves_old_file_and_cleans_temporary() {
        let directory = tempfile::tempdir().expect("state directory");
        let id = RunId::new();
        let old = PersistedTerminalCheckpoint {
            epoch: "old".to_owned(),
            state: None,
        };
        write_terminal_checkpoint(directory.path(), id, &old).expect("write old");
        let path = terminal_checkpoint_path(directory.path(), id);
        let original = fs::read(&path).expect("old bytes");
        let replacement = PersistedTerminalCheckpoint {
            epoch: "雪\n\"\\".repeat(4097),
            state: None,
        };
        let expected = serde_json::to_vec(&replacement).expect("reference JSON");
        assert!(expected.len() < MAX_CHECKPOINT_FILE_BYTES);
        let error = write_terminal_checkpoint_bounded(
            directory.path(),
            id,
            &replacement,
            expected.len() - 1,
        )
        .expect_err("reject one-byte-over-policy streaming file");
        assert!(error.to_string().contains("checkpoint file exceeds bound"));
        assert_eq!(fs::read(&path).expect("still old bytes"), original);
        assert_eq!(
            fs::read_dir(path.parent().expect("checkpoint directory"))
                .expect("entries")
                .count(),
            1,
            "no abandoned temporary checkpoint"
        );
        write_terminal_checkpoint_bounded(directory.path(), id, &replacement, expected.len())
            .expect("exact boundary succeeds after pressure");
        assert_eq!(fs::read(&path).expect("new bytes"), expected);
        assert_eq!(
            fs::metadata(&path).expect("metadata").permissions().mode() & 0o777,
            0o600
        );
    }

    #[test]
    fn checkpoint_stream_counts_actual_short_writes_and_preserves_io_error() {
        struct ShortWriter(Vec<u8>);
        impl Write for ShortWriter {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                let written = bytes.len().min(2);
                self.0.extend_from_slice(&bytes[..written]);
                Ok(written)
            }
            fn flush(&mut self) -> io::Result<()> {
                Err(io::Error::from(io::ErrorKind::StorageFull))
            }
        }
        let mut writer = CheckpointWriter {
            writer: ShortWriter(Vec::new()),
            remaining: 5,
        };
        writer.write_all(b"abcde").expect("short writes complete");
        assert_eq!(writer.writer.0, b"abcde");
        assert_eq!(writer.remaining, 0);
        writer
            .write_all(b"x")
            .expect_err("over-bound byte rejected");
        assert_eq!(writer.writer.0, b"abcde");
        assert_eq!(
            writer
                .flush()
                .expect_err("flush failure stays truthful")
                .kind(),
            io::ErrorKind::StorageFull
        );
    }
}
