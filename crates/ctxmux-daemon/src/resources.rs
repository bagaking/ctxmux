//! Operator resource policy, separate from durable-format integrity.
//!
//! Byte budgets bound resident replay, retained metadata and storage work.
//! Population defaults have no product ceiling: live admission follows funded
//! descriptors; retained records follow metadata. Explicit population limits
//! are optional operator policy. Qualification fleet sizes never set defaults.

// Historical aggregate byte defaults; operator policy documented in ADR 019.
const DEFAULT_HOT_REPLAY_BYTES: usize = 1024 * 1024 * 1024;

// SQLite file-format constants: 4096-byte pages in this store, 32-byte WAL
// header and 24-byte frame header. wal.c uses 32 KiB index blocks, with 4062
// frame slots in the first and 4096 thereafter (136-byte index header).
const SQLITE_PAGE_BYTES: u64 = 4096;
const WAL_HEADER_BYTES: u64 = 32;
const WAL_FRAME_BYTES: u64 = SQLITE_PAGE_BYTES + 24;
const SHM_BLOCK_BYTES: u64 = 32768;
const SHM_FRAME_SLOTS: u64 = 4096;
const SHM_FIRST_FRAME_SLOTS: u64 = 4062;

use serde::{Deserialize, Serialize};

// The field catalogue is shared by two deliberately different contracts:
// startup values may be omitted, but every established snapshot field is required.
// Keeping one declaration prevents their names/types drifting during upgrades.
macro_rules! resource_policy {
    ($( $(#[$meta:meta])* pub $field:ident: $ty:ty, )*) => {
        /// Fully resolved, immutable owner policy carried intact across exec.
        #[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
        #[serde(deny_unknown_fields)]
        pub struct ResourceLimits {
            $( $(#[$meta])*
                #[serde(deserialize_with = "required_value")]
                pub $field: $ty,
            )*
        }

        /// Sparse startup choices; omission is resolved before creating owners.
        #[derive(Clone, Copy, Debug, Default, Deserialize)]
        #[serde(default, deny_unknown_fields)]
        pub struct ResourceConfig {
            $( #[serde(deserialize_with = "configured_value")] $field: Option<$ty>, )*
        }

        impl ResourceConfig {
            fn apply(self, limits: &mut ResourceLimits) {
                $( if let Some(value) = self.$field { limits.$field = value; } )*
            }
        }
    };
}

// deserialize_with prevents Serde's implicit missing-Option -> None behavior
// in snapshots. In startup configuration Some(None) distinguishes explicit
// JSON null from an omitted quota. Duplicate fields still fail in Serde.
fn required_value<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer)
}

fn configured_value<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
}

resource_policy! {
    pub live_runs: Option<usize>,
    pub retained_runs: Option<usize>,
    pub hot_output_bytes: u64,
    pub live_event_bytes: u64,
    pub run_output_bytes: usize,
    pub metadata_bytes: u64,
    /// None follows observed filesystem funding, rather than a fixed quota.
    pub durable_replay_bytes: Option<u64>,
    pub durable_run_output_bytes: Option<u64>,
    /// None retains `SQLite`'s own page ceiling on the serving connection.
    pub database_bytes: Option<u64>,
    pub wal_checkpoint_bytes: u64,
    pub handoff_bytes: u64,
    pub control_state_bytes: u64,
    pub diagnostic_queue_bytes: u64,
    pub diagnostic_record_bytes: usize,
    pub creation_workers: usize,
    /// Concurrent read-only OS observation jobs, independent of Input/Stop.
    pub foreground_observation_workers: usize,
    /// Temporary observation memory in bytes; permits survive caller timeout.
    pub foreground_observation_bytes: u64,
    /// Maximum caller wait in milliseconds; issued OS work still drains.
    pub foreground_observation_timeout_ms: u64,
    /// Completed commands serviced for one Run before another gets its turn.
    pub input_turn_commands: usize,
    /// Maximum bytes written for one Run per poll turn, never a request cap.
    pub input_turn_bytes: usize,
    /// Milliseconds to acquire cleanup capacity before a Stop has any side effect.
    pub stop_admission_timeout_ms: u64,
    pub cleanup_workers: usize,
    pub finalize_workers: usize,
    pub input_queue_commands: usize,
    pub input_queue_bytes: usize,
    pub input_result_entries: usize,
    pub input_result_bytes: usize,
    pub tmux_discovery_bytes: usize,
}

impl ResourceConfig {
    /// Parse strict startup choices without applying any owner policy yet.
    ///
    /// # Errors
    /// Rejects invalid JSON, unknown or duplicate fields and invalid field types.
    pub fn from_json(json: &str) -> Result<Self, String> {
        serde_json::from_str(json).map_err(|error| error.to_string())
    }

    /// Resolve omissions once and validate before creating any runtime owner.
    ///
    /// # Errors
    /// Rejects unfunded or unrepresentable resource policy.
    pub fn resolve(self) -> Result<ResourceLimits, String> {
        let mut limits = ResourceLimits::default();
        self.apply(&mut limits);
        // Missing per-Run quotas follow their aggregate owner; explicit null
        // removes a durable per-Run quota and must survive snapshot round trips.
        if self.run_output_bytes.is_none() {
            limits.run_output_bytes = usize::try_from(limits.hot_output_bytes)
                .map_err(|_| "hot_output_bytes must fit the host")?;
        }
        if self.durable_run_output_bytes.is_none() {
            limits.durable_run_output_bytes = limits.durable_replay_bytes;
        }
        if self.foreground_observation_bytes.is_none() {
            limits.foreground_observation_bytes =
                observation_bytes(limits.foreground_observation_workers)?;
        }
        limits.validate()?;
        Ok(limits)
    }
}

// Each admitted read-only OS job reserves one maximum protocol frame for its
// temporary process result. Width and aggregate funding share that owner.
fn observation_bytes(workers: usize) -> Result<u64, String> {
    u64::try_from(workers)
        .ok()
        .and_then(|count| count.checked_mul(ctxmux_protocol::MAX_FRAME_BYTES as u64))
        .ok_or_else(|| "foreground observation worker funding overflows".to_owned())
}

impl Default for ResourceLimits {
    fn default() -> Self {
        let mut limits = Self::DEFAULT;
        // One read-only OS worker per four available logical CPUs. This is
        // a conservative width heuristic, not a CPU reservation guarantee.
        // Explicit startup widths derive their funding by the same relation.
        limits.foreground_observation_workers =
            std::thread::available_parallelism().map_or(1, |cpus| (cpus.get() / 4).max(1));
        limits.foreground_observation_bytes =
            observation_bytes(limits.foreground_observation_workers)
                .expect("available host CPU width fits observation funding");
        limits
    }
}

impl ResourceLimits {
    /// General host defaults, adjustable independently of the workload used
    /// to qualify them. Replay budgets trade history length for memory/disk;
    /// metadata/database budgets bound recovery/storage, and the WAL window
    /// bounds one staged write plus its prior committed window.
    pub const DEFAULT: Self = Self {
        live_runs: None,
        retained_runs: None,
        hot_output_bytes: DEFAULT_HOT_REPLAY_BYTES as u64,
        live_event_bytes: 64 * 1024 * 1024,
        run_output_bytes: DEFAULT_HOT_REPLAY_BYTES,
        metadata_bytes: 64 * 1024 * 1024,
        durable_replay_bytes: None,
        durable_run_output_bytes: None,
        database_bytes: None,
        wal_checkpoint_bytes: 8 * 1024 * 1024,
        handoff_bytes: 256 * 1024 * 1024,
        control_state_bytes: 128 * 1024 * 1024,
        diagnostic_queue_bytes: 64 * 1024 * 1024,
        diagnostic_record_bytes: ctxmux_protocol::MAX_FRAME_BYTES,
        creation_workers: 8,
        // Static one-worker baseline. Default::default derives worker width
        // from available host parallelism; each slot is funded in bytes, not
        // by a qualification member count. Wire integrity is independent.
        foreground_observation_workers: 1,
        foreground_observation_bytes: ctxmux_protocol::MAX_FRAME_BYTES as u64,
        // One-second interactive observation service objective; expiry is
        // honest unknown, never cancellation or a healthy input failure.
        foreground_observation_timeout_ms: 1000,
        input_turn_commands: 64,
        input_turn_bytes: 256 * 1024,
        stop_admission_timeout_ms: 250,
        cleanup_workers: 8,
        finalize_workers: 8,
        input_queue_commands: 1024,
        input_queue_bytes: 4 * 1024 * 1024,
        input_result_entries: 256,
        input_result_bytes: 1024 * 1024,
        tmux_discovery_bytes: 128 * 1024,
    };

    /// Validate arithmetic and representation before creating any owner.
    ///
    /// # Errors
    /// Rejects zero, unrepresentable counts, incomplete pages and overflowing sums.
    #[allow(
        clippy::too_many_lines,
        reason = "validate the complete resource snapshot before creating any owner"
    )]
    pub fn validate(&self) -> Result<(), String> {
        if self.live_runs == Some(0) || self.retained_runs == Some(0) {
            return Err("population limits must be positive or null".to_owned());
        }
        for (name, bytes) in [
            ("hot_output_bytes", self.hot_output_bytes),
            ("live_event_bytes", self.live_event_bytes),
            ("run_output_bytes", self.run_output_bytes as u64),
            ("metadata_bytes", self.metadata_bytes),
            ("wal_checkpoint_bytes", self.wal_checkpoint_bytes),
            ("handoff_bytes", self.handoff_bytes),
            ("control_state_bytes", self.control_state_bytes),
            (
                "foreground_observation_bytes",
                self.foreground_observation_bytes,
            ),
            ("diagnostic_queue_bytes", self.diagnostic_queue_bytes),
            (
                "diagnostic_record_bytes",
                self.diagnostic_record_bytes as u64,
            ),
        ] {
            if bytes == 0 || bytes > i64::MAX as u64 || usize::try_from(bytes).is_err() {
                return Err(format!("{name} must fit a positive host/SQLite byte count"));
            }
        }
        for (name, quota) in [
            ("durable_replay_bytes", self.durable_replay_bytes),
            ("durable_run_output_bytes", self.durable_run_output_bytes),
            ("database_bytes", self.database_bytes),
        ] {
            if quota.is_some_and(|bytes| {
                bytes == 0 || bytes > i64::MAX as u64 || usize::try_from(bytes).is_err()
            }) {
                return Err(format!(
                    "{name} must fit a positive host/SQLite byte count or null"
                ));
            }
        }
        if self
            .database_bytes
            .is_some_and(|bytes| !bytes.is_multiple_of(SQLITE_PAGE_BYTES))
        {
            return Err(
                "database_bytes must be a multiple of the 4096-byte SQLite page".to_owned(),
            );
        }
        if self.wal_checkpoint_bytes < WAL_HEADER_BYTES + WAL_FRAME_BYTES {
            return Err("wal_checkpoint_bytes must fund a WAL header and page frame".to_owned());
        }
        for (name, count) in [
            ("creation_workers", self.creation_workers),
            (
                "foreground_observation_workers",
                self.foreground_observation_workers,
            ),
            ("input_turn_commands", self.input_turn_commands),
            ("input_turn_bytes", self.input_turn_bytes),
            ("cleanup_workers", self.cleanup_workers),
            ("finalize_workers", self.finalize_workers),
            ("input_queue_commands", self.input_queue_commands),
            ("input_queue_bytes", self.input_queue_bytes),
            ("input_result_entries", self.input_result_entries),
            ("input_result_bytes", self.input_result_bytes),
            ("tmux_discovery_bytes", self.tmux_discovery_bytes),
        ] {
            if count == 0 || count > tokio::sync::Semaphore::MAX_PERMITS {
                return Err(format!("{name} must fit a positive owner count"));
            }
        }
        if self.stop_admission_timeout_ms == 0
            || std::time::Instant::now()
                .checked_add(std::time::Duration::from_millis(
                    self.stop_admission_timeout_ms,
                ))
                .is_none()
        {
            return Err("stop_admission_timeout_ms must fit a positive host deadline".to_owned());
        }
        if self.foreground_observation_timeout_ms == 0
            || std::time::Instant::now()
                .checked_add(std::time::Duration::from_millis(
                    self.foreground_observation_timeout_ms,
                ))
                .is_none()
        {
            return Err(
                "foreground_observation_timeout_ms must fit a positive host deadline".to_owned(),
            );
        }
        crate::diagnostics::validate_limits(crate::diagnostics::DiagnosticLimits {
            queue_bytes: usize::try_from(self.diagnostic_queue_bytes)
                .map_err(|_| "diagnostic_queue_bytes must fit the host")?,
            record_bytes: self.diagnostic_record_bytes,
        })
        .map_err(|error| error.to_string())?;
        self.state_file_bytes()?;
        Ok(())
    }

    pub(crate) const fn wal_bytes(&self) -> u64 {
        self.wal_checkpoint_bytes * 2
    }

    pub(crate) const fn shm_bytes(&self) -> u64 {
        let frames = self.wal_bytes().saturating_sub(WAL_HEADER_BYTES) / WAL_FRAME_BYTES;
        let additional = frames.saturating_sub(SHM_FIRST_FRAME_SLOTS);
        (1 + additional.div_ceil(SHM_FRAME_SLOTS)) * SHM_BLOCK_BYTES
    }

    pub(crate) fn state_file_bytes(&self) -> Result<Option<u64>, String> {
        // Source generation (up to twice retained replay), packed replacement,
        // one 1 MiB admitted append, database, WAL and SHM scratch reservation.
        let overflow = || "combined storage budgets overflow".to_owned();
        let wal = self
            .wal_checkpoint_bytes
            .checked_mul(2)
            .ok_or_else(overflow)?;
        let fixed = wal
            .checked_add(self.shm_bytes())
            .and_then(|bytes| bytes.checked_add(1024 * 1024))
            .ok_or_else(overflow)?;
        let replay = self
            .durable_replay_bytes
            .map(|bytes| bytes.checked_mul(3).ok_or_else(overflow))
            .transpose()?;
        // Still validate each bounded component when another owner is derived.
        if let Some(database) = self.database_bytes {
            database.checked_add(fixed).ok_or_else(overflow)?;
        }
        if let Some(replay) = replay {
            replay.checked_add(fixed).ok_or_else(overflow)?;
        }
        match (self.database_bytes, replay) {
            (Some(database), Some(replay)) => database
                .checked_add(replay)
                .and_then(|bytes| bytes.checked_add(fixed))
                .map(Some)
                .ok_or_else(overflow),
            _ => Ok(None),
        }
    }
}

/// Shared byte reservations follow actual queue/receipt ownership, including
/// terminal history. A new operation must fund its payload and bounded failure
/// receipt before its first side effect; a pressure refusal never changes it.
#[derive(Clone, Debug)]
pub(crate) struct ByteBudget(std::sync::Arc<ByteBudgetInner>);
#[derive(Debug)]
struct ByteBudgetInner {
    limit: u64,
    used: std::sync::atomic::AtomicU64,
}
#[derive(Debug)]
pub(crate) struct BytePermit {
    budget: ByteBudget,
    bytes: u64,
}
impl ByteBudget {
    pub(crate) fn new(limit: u64) -> Self {
        Self(std::sync::Arc::new(ByteBudgetInner {
            limit,
            used: std::sync::atomic::AtomicU64::new(0),
        }))
    }
    #[cfg(test)]
    pub(crate) fn used(&self) -> u64 {
        self.0.used.load(std::sync::atomic::Ordering::Acquire)
    }

    pub(crate) fn reserve(&self, bytes: usize) -> Option<BytePermit> {
        use std::sync::atomic::Ordering;
        let bytes = bytes as u64;
        self.0
            .used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes).filter(|next| *next <= self.0.limit)
            })
            .ok()?;
        Some(BytePermit {
            budget: self.clone(),
            bytes,
        })
    }
}
impl Drop for BytePermit {
    fn drop(&mut self) {
        self.budget
            .0
            .used
            .fetch_sub(self.bytes, std::sync::atomic::Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::{ResourceConfig, ResourceLimits};

    fn parse(json: &str) -> Result<ResourceLimits, String> {
        ResourceConfig::from_json(json)?.resolve()
    }

    #[test]
    fn population_is_not_bound_to_a_qualification_tier() {
        let limits =
            parse(r#"{"live_runs":12000,"retained_runs":50000,"hot_output_bytes":4096}"#).unwrap();
        assert_eq!(limits.live_runs, Some(12000));
        assert_eq!(limits.retained_runs, Some(50000));
        assert_eq!(limits.hot_output_bytes, 4096);
        assert_eq!(ResourceLimits::default().retained_runs, None);
    }

    #[test]
    fn disk_defaults_have_no_operator_ceiling_and_null_round_trips() {
        for json in [
            "{}",
            r#"{"database_bytes":null,"durable_replay_bytes":null,"durable_run_output_bytes":null}"#,
        ] {
            let limits = parse(json).unwrap();
            assert_eq!(limits.database_bytes, None);
            assert_eq!(limits.durable_replay_bytes, None);
            assert_eq!(limits.durable_run_output_bytes, None);
            assert_eq!(limits.state_file_bytes().unwrap(), None);
            assert_eq!(
                serde_json::from_str::<ResourceLimits>(&serde_json::to_string(&limits).unwrap())
                    .unwrap(),
                limits
            );
        }
        let explicit = parse(r#"{"database_bytes":131072,"durable_replay_bytes":8192,"durable_run_output_bytes":null}"#).unwrap();
        assert_eq!(explicit.database_bytes, Some(131_072));
        assert_eq!(explicit.durable_replay_bytes, Some(8192));
        assert_eq!(explicit.durable_run_output_bytes, None);
        assert!(explicit.state_file_bytes().unwrap().is_some());
    }

    #[test]
    fn omitted_run_history_quotas_follow_the_aggregate_owner() {
        let defaults = ResourceLimits::default();
        assert_eq!(defaults.run_output_bytes as u64, defaults.hot_output_bytes);
        assert_eq!(
            defaults.durable_run_output_bytes,
            defaults.durable_replay_bytes
        );
        let limits = parse(r#"{"hot_output_bytes":8192,"durable_replay_bytes":16384}"#).unwrap();
        assert_eq!(limits.run_output_bytes, 8192);
        assert_eq!(limits.durable_run_output_bytes, Some(16384));
    }

    #[test]
    fn explicit_history_quotas_and_resolved_exec_policy_remain_exact() {
        let limits = parse(
            r#"{"hot_output_bytes":8192,"run_output_bytes":512,"durable_replay_bytes":16384,"durable_run_output_bytes":1024}"#,
        )
        .unwrap();
        assert_eq!(limits.run_output_bytes, 512);
        assert_eq!(limits.durable_run_output_bytes, Some(1024));
        let encoded = serde_json::to_string(&limits).unwrap();
        assert_eq!(
            serde_json::from_str::<ResourceLimits>(&encoded).unwrap(),
            limits
        );
    }

    #[test]
    fn omitted_observation_funding_follows_the_selected_worker_owner() {
        for workers in [1, 3, 37, 5000] {
            let limits = parse(&format!(
                r#"{{"foreground_observation_workers":{workers}}}"#
            ))
            .unwrap();
            assert_eq!(limits.foreground_observation_workers, workers);
            assert_eq!(
                limits.foreground_observation_bytes,
                workers as u64 * ctxmux_protocol::MAX_FRAME_BYTES as u64
            );
        }
    }

    #[test]
    fn explicit_observation_funding_survives_resolution_and_exec_snapshot() {
        for bytes in [1, 5 * ctxmux_protocol::MAX_FRAME_BYTES] {
            let limits = parse(&format!(
                r#"{{"foreground_observation_workers":3,"foreground_observation_bytes":{bytes}}}"#
            ))
            .unwrap();
            assert_eq!(limits.foreground_observation_bytes, bytes as u64);
            assert_eq!(
                serde_json::from_value::<ResourceLimits>(serde_json::to_value(limits).unwrap())
                    .unwrap(),
                limits
            );
        }
        assert!(
            parse(&format!(
                r#"{{"foreground_observation_workers":{}}}"#,
                usize::MAX
            ))
            .is_err()
        );
    }

    #[test]
    fn established_snapshot_requires_every_field_including_nullable_quotas() {
        let limits = ResourceLimits::default();
        let complete = serde_json::to_value(limits).unwrap();
        for field in complete.as_object().unwrap().keys() {
            let mut partial = complete.clone();
            partial.as_object_mut().unwrap().remove(field);
            assert!(
                serde_json::from_value::<ResourceLimits>(partial).is_err(),
                "missing snapshot field {field} silently received a default"
            );
        }
        assert_eq!(parse("{}").unwrap(), limits);
        assert!(serde_json::from_str::<ResourceLimits>("{}").is_err());
    }

    #[test]
    fn invalid_policy_fails_before_owner_creation() {
        for json in [
            r#"{"hot_output_bytes":8192,"hot_output_bytes":16384}"#,
            r#"{"run_output_bytes":512,"run_output_bytes":1024}"#,
            r#"{"durable_run_output_bytes":512,"durable_run_output_bytes":1024}"#,
            r#"{"live_run":5000}"#,
            r#"{"durable_replay_bytes":null,"durable_replay_bytes":8192}"#,
            r#"{"hot_output_bytes":null}"#,
            r#"{"handoff_input_bytes":1024}"#,
            r#"{"handoff_diagnostic_bytes":1024}"#,
            r#"{"live_runs":0}"#,
            r#"{"database_bytes":4097}"#,
            r#"{"durable_replay_bytes":9223372036854775807}"#,
        ] {
            assert!(parse(json).is_err(), "accepted {json}");
        }
    }
}
