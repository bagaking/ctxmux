//! Operator resource policy, separate from durable-format integrity.
//!
//! Byte budgets bound resident replay, retained metadata and storage work.
//! Population defaults have no product ceiling: live admission follows funded
//! descriptors; retained records follow metadata. Explicit population limits
//! are optional operator policy. Qualification fleet sizes never set defaults.

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

/// Daemon resource policy, accepted as strict JSON by `ctxmuxd` and preserved
/// across planned exec. Omitted fields use the documented defaults.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ResourceLimits {
    pub live_runs: Option<usize>,
    pub retained_runs: Option<usize>,
    pub hot_output_bytes: u64,
    pub live_event_bytes: u64,
    pub run_output_bytes: usize,
    pub metadata_bytes: u64,
    pub durable_replay_bytes: u64,
    pub durable_run_output_bytes: u64,
    pub database_bytes: u64,
    pub wal_checkpoint_bytes: u64,
    pub handoff_input_bytes: usize,
    pub handoff_diagnostic_bytes: usize,
    pub handoff_bytes: u64,
    pub control_state_bytes: u64,
    pub diagnostic_queue_bytes: u64,
    pub diagnostic_record_bytes: usize,
    pub creation_workers: usize,
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

impl Default for ResourceLimits {
    fn default() -> Self {
        Self::DEFAULT
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
        hot_output_bytes: 1024 * 1024 * 1024,
        live_event_bytes: 64 * 1024 * 1024,
        run_output_bytes: 4 * 1024 * 1024,
        metadata_bytes: 64 * 1024 * 1024,
        durable_replay_bytes: 256 * 1024 * 1024,
        durable_run_output_bytes: 4 * 1024 * 1024,
        database_bytes: 384 * 1024 * 1024,
        wal_checkpoint_bytes: 8 * 1024 * 1024,
        handoff_input_bytes: 128 * 1024 * 1024,
        handoff_diagnostic_bytes: 16 * 1024 * 1024,
        handoff_bytes: 256 * 1024 * 1024,
        control_state_bytes: 128 * 1024 * 1024,
        diagnostic_queue_bytes: 64 * 1024 * 1024,
        diagnostic_record_bytes: ctxmux_protocol::MAX_FRAME_BYTES,
        creation_workers: 8,
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

    /// # Errors
    /// Rejects invalid JSON, unknown fields, and invalid resource arithmetic.
    ///
    /// Parse and validate one policy. Unknown names fail instead of silently
    /// leaving an intended budget at its default.
    pub fn from_json(json: &str) -> Result<Self, String> {
        let limits: Self = serde_json::from_str(json).map_err(|error| error.to_string())?;
        limits.validate()?;
        Ok(limits)
    }

    /// Validate arithmetic and representation before creating any owner.
    ///
    /// # Errors
    /// Rejects zero, unrepresentable counts, incomplete pages and overflowing sums.
    pub fn validate(&self) -> Result<(), String> {
        if self.live_runs == Some(0) || self.retained_runs == Some(0) {
            return Err("population limits must be positive or null".to_owned());
        }
        for (name, bytes) in [
            ("hot_output_bytes", self.hot_output_bytes),
            ("live_event_bytes", self.live_event_bytes),
            ("run_output_bytes", self.run_output_bytes as u64),
            ("metadata_bytes", self.metadata_bytes),
            ("durable_replay_bytes", self.durable_replay_bytes),
            ("durable_run_output_bytes", self.durable_run_output_bytes),
            ("database_bytes", self.database_bytes),
            ("wal_checkpoint_bytes", self.wal_checkpoint_bytes),
            ("handoff_input_bytes", self.handoff_input_bytes as u64),
            (
                "handoff_diagnostic_bytes",
                self.handoff_diagnostic_bytes as u64,
            ),
            ("handoff_bytes", self.handoff_bytes),
            ("control_state_bytes", self.control_state_bytes),
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
        if !self.database_bytes.is_multiple_of(SQLITE_PAGE_BYTES) {
            return Err(
                "database_bytes must be a multiple of the 4096-byte SQLite page".to_owned(),
            );
        }
        if self.wal_checkpoint_bytes < WAL_HEADER_BYTES + WAL_FRAME_BYTES {
            return Err("wal_checkpoint_bytes must fund a WAL header and page frame".to_owned());
        }
        for (name, count) in [
            ("creation_workers", self.creation_workers),
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
        crate::diagnostics::validate_limits(crate::diagnostics::DiagnosticLimits {
            queue_bytes: usize::try_from(self.diagnostic_queue_bytes)
                .map_err(|_| "diagnostic_queue_bytes must fit the host")?,
            record_bytes: self.diagnostic_record_bytes,
        })
        .map_err(|error| error.to_string())?;
        self.state_file_bytes()
            .ok_or("combined storage budgets overflow")?;
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

    pub(crate) const fn state_file_bytes(&self) -> Option<u64> {
        // Source generation (up to twice retained replay), packed replacement,
        // one 1 MiB admitted append, database, WAL and SHM scratch reservation.
        let Some(replay) = self.durable_replay_bytes.checked_mul(3) else {
            return None;
        };
        let Some(wal) = self.wal_checkpoint_bytes.checked_mul(2) else {
            return None;
        };
        let Some(bytes) = self.database_bytes.checked_add(wal) else {
            return None;
        };
        let Some(bytes) = bytes.checked_add(replay) else {
            return None;
        };
        let Some(bytes) = bytes.checked_add(self.shm_bytes()) else {
            return None;
        };
        bytes.checked_add(1024 * 1024)
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
    use super::ResourceLimits;

    #[test]
    fn population_is_not_bound_to_a_qualification_tier() {
        let limits = ResourceLimits::from_json(
            r#"{"live_runs":12000,"retained_runs":50000,"hot_output_bytes":4096}"#,
        )
        .unwrap();
        assert_eq!(limits.live_runs, Some(12000));
        assert_eq!(limits.retained_runs, Some(50000));
        assert_eq!(limits.hot_output_bytes, 4096);
        assert_eq!(ResourceLimits::default().retained_runs, None);
    }

    #[test]
    fn invalid_policy_fails_before_owner_creation() {
        for json in [
            r#"{"live_run":5000}"#,
            r#"{"live_runs":0}"#,
            r#"{"database_bytes":4097}"#,
            r#"{"durable_replay_bytes":9223372036854775807}"#,
        ] {
            assert!(ResourceLimits::from_json(json).is_err(), "accepted {json}");
        }
    }
}
