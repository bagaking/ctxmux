//! The upgrade handoff manifest carried across `execve`-in-place.
//!
//! Serialized to a single inherited descriptor by the outgoing image and read
//! back by the incoming image. Versioned so a mismatched upgrade fails closed
//! rather than misreading fd numbers.

#![allow(dead_code)]

use std::os::fd::RawFd;

use serde::{Deserialize, Serialize};

use ctxmux_protocol::RunId;

pub const HANDOFF_SCHEMA: &str = "ctxmux.daemon-handoff.v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HandoffManifest {
    pub schema: String,
    pub epoch: String,
    pub runs: Vec<HandoffRun>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HandoffRun {
    pub run_id: RunId,
    pub child_pid: i32,
    pub master_fd: RawFd,
    pub writer_fd: RawFd,
    pub reader_fd: RawFd,
}

impl HandoffManifest {
    pub fn new(epoch: String, runs: Vec<HandoffRun>) -> Self {
        Self {
            schema: HANDOFF_SCHEMA.to_string(),
            epoch,
            runs,
        }
    }

    /// Every fd number this manifest expects to survive the exec.
    pub fn all_fds(&self) -> Vec<RawFd> {
        self.runs
            .iter()
            .flat_map(|r| [r.master_fd, r.writer_fd, r.reader_fd])
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_through_json_and_lists_all_fds() {
        let manifest = HandoffManifest::new(
            "epoch-xyz".to_string(),
            vec![HandoffRun {
                run_id: RunId::new(),
                child_pid: 4321,
                master_fd: 7,
                writer_fd: 8,
                reader_fd: 9,
            }],
        );
        let bytes = serde_json::to_vec(&manifest).unwrap();
        let parsed: HandoffManifest = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed, manifest);
        assert_eq!(parsed.all_fds(), vec![7, 8, 9]);
        assert_eq!(parsed.schema, HANDOFF_SCHEMA);
    }
}
