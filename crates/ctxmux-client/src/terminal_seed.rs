//! Client-local seed assembly policy, separate from the peer's wire contract.

use std::collections::TryReserveError;
use thiserror::Error;

/// Per-seed buffer policy for one client. This is not a protocol size ceiling,
/// total process memory bound, or aggregate quota across simultaneous attaches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TerminalSeedLimits {
    /// Maximum decoded restore bytes to assemble for a Terminal view. Zero
    /// refuses all nonempty seeds; Raw views and other clients are unaffected.
    pub restore_bytes: u64,
}

impl Default for TerminalSeedLimits {
    fn default() -> Self {
        Self {
            // Preserve the historical receive window as an operational default;
            // callers may fund a different legitimate workload explicitly.
            restore_bytes: 32 * 1024 * 1024,
        }
    }
}

/// Local inability to retain a structurally valid terminal seed.
#[derive(Debug, Error)]
pub enum TerminalSeedResourceError {
    /// This client's configured decoded-buffer policy refused the seed.
    #[error(
        "terminal seed requires {requested_bytes} decoded bytes; local limit is {limit_bytes}; raise the local policy or explicitly open a Raw view"
    )]
    RestoreLimit {
        /// Actual advertised decoded bytes.
        requested_bytes: u64,
        /// Caller-selected per-seed policy.
        limit_bytes: u64,
    },
    /// The byte length cannot be represented by this host's allocation API.
    #[error("terminal seed length {requested_bytes} does not fit this host")]
    HostByteCapacity {
        /// Advertised decoded byte length.
        requested_bytes: u64,
    },
    /// The declared restoration history cannot be represented on this host.
    #[error("terminal restore history {requested_rows} rows does not fit this host")]
    HostScrollbackCapacity {
        /// Advertised temporary restoration history rows.
        requested_rows: u64,
    },
    /// The fallible buffer reservation failed, independently of peer validity.
    #[error("local terminal seed allocation of {requested_bytes} bytes failed: {source}")]
    Allocation {
        /// Requested decoded buffer reservation.
        requested_bytes: u64,
        /// Host allocator or layout failure.
        #[source]
        source: TryReserveError,
    },
}

pub(crate) fn reserve_restore(
    restore: &mut Vec<u8>,
    requested_bytes: u64,
    limits: TerminalSeedLimits,
) -> Result<(), TerminalSeedResourceError> {
    if requested_bytes > limits.restore_bytes {
        return Err(TerminalSeedResourceError::RestoreLimit {
            requested_bytes,
            limit_bytes: limits.restore_bytes,
        });
    }
    let bytes = usize::try_from(requested_bytes)
        .map_err(|_| TerminalSeedResourceError::HostByteCapacity { requested_bytes })?;
    restore
        .try_reserve_exact(bytes)
        .map_err(|source| TerminalSeedResourceError::Allocation {
            requested_bytes,
            source,
        })
}

#[cfg(test)]
mod tests {
    use super::{TerminalSeedLimits, TerminalSeedResourceError, reserve_restore};

    #[test]
    fn local_policy_refuses_before_buffer_allocation_and_can_admit_the_same_seed() {
        let mut restore = Vec::new();
        let error = reserve_restore(&mut restore, 3, TerminalSeedLimits { restore_bytes: 2 })
            .expect_err("local policy refusal");
        assert!(matches!(
            error,
            TerminalSeedResourceError::RestoreLimit {
                requested_bytes: 3,
                limit_bytes: 2,
            }
        ));
        assert_eq!(restore.capacity(), 0);
        reserve_restore(&mut restore, 3, TerminalSeedLimits { restore_bytes: 3 })
            .expect("exact local boundary");
        restore.extend_from_slice(&[0, 255, 1]);
        assert_eq!(restore, [0, 255, 1]);
    }

    #[test]
    fn layout_failure_is_local_resource_failure_without_attempting_huge_memory() {
        let mut restore = Vec::new();
        let error = reserve_restore(
            &mut restore,
            u64::MAX,
            TerminalSeedLimits {
                restore_bytes: u64::MAX,
            },
        )
        .expect_err("host representability or layout must fail");
        assert!(matches!(
            error,
            TerminalSeedResourceError::Allocation {
                requested_bytes: u64::MAX,
                ..
            } | TerminalSeedResourceError::HostByteCapacity {
                requested_bytes: u64::MAX
            }
        ));
        assert_eq!(restore.capacity(), 0);
    }
}
