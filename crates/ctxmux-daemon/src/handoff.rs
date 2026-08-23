//! The upgrade handoff manifest carried across `execve`-in-place.
//!
//! Serialized to a single inherited descriptor by the outgoing image and read
//! back by the incoming image. Versioned so a mismatched upgrade fails closed
//! rather than misreading fd numbers.
//!
//! Writer contract for the outgoing image: write compact JSON followed by
//! `\n`, rewind the inherited unlinked regular file, then exec. The incoming
//! image consumes only the first line, so pretty-printed multi-line JSON parses
//! just its opening brace and fails closed rather than adopting anything.
//!
//! Only the pty master fd is carried per Run. The reader and writer fds of a
//! native Run both refer to the same open file description as the master, so
//! the incoming image re-derives them from the master fd after exec (re-dup a
//! cloexec reader, write input directly to the master) exactly as the spawn
//! path does.

#![allow(dead_code)]

use std::{
    collections::HashSet,
    os::fd::{AsRawFd, OwnedFd, RawFd},
};

use serde::{Deserialize, Serialize};

use ctxmux_protocol::{DaemonInstanceId, RunId};

use crate::{creation::MAX_RETAINED_RUNS, native_control::HandoffInputState};

pub const HANDOFF_SCHEMA: &str = "ctxmux.daemon-handoff.v2";
// 128 retained Runs can each carry 1 MiB of recoverable Input request bytes.
// Those payloads use base64 in JSON, while operation keys and bounded
// diagnostics may expand under JSON escaping. Keep the read ceiling above the
// complete valid maximum without accepting an unbounded inherited file.
const MAX_HANDOFF_MANIFEST_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HandoffManifest {
    pub schema: String,
    pub epoch: String,
    pub listener_fd: RawFd,
    pub state_lock_fd: RawFd,
    pub runs: Vec<HandoffRun>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HandoffRun {
    pub run_id: RunId,
    pub child_pid: u32,
    pub master_fd: RawFd,
    pub input_state: HandoffInputState,
}

impl HandoffManifest {
    pub fn new(
        epoch: String,
        listener_fd: RawFd,
        state_lock_fd: RawFd,
        runs: Vec<HandoffRun>,
    ) -> Self {
        Self {
            schema: HANDOFF_SCHEMA.to_string(),
            epoch,
            listener_fd,
            state_lock_fd,
            runs,
        }
    }

    /// Every fd number this manifest expects to survive the exec: the process
    /// listener and state-lock descriptors first, then each Run's pty master.
    pub fn all_fds(&self) -> Vec<RawFd> {
        let mut fds = vec![self.listener_fd, self.state_lock_fd];
        fds.extend(self.runs.iter().map(|r| r.master_fd));
        fds
    }

    fn validate(&self, manifest_fd: RawFd) -> std::io::Result<()> {
        self.epoch.parse::<DaemonInstanceId>().map_err(|error| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid handoff daemon epoch: {error}"),
            )
        })?;
        if self.runs.len() > MAX_RETAINED_RUNS {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "handoff contains {} Runs; maximum is {MAX_RETAINED_RUNS}",
                    self.runs.len()
                ),
            ));
        }
        let mut fds = HashSet::new();
        for fd in self.all_fds() {
            if fd < 3 || fd == manifest_fd || !fds.insert(fd) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "handoff descriptors must be non-standard, distinct, and separate from the manifest",
                ));
            }
        }
        let mut run_ids = HashSet::new();
        for run in &self.runs {
            if run.child_pid == 0 || !run_ids.insert(run.run_id) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "handoff Runs must have unique ids and non-zero child pids",
                ));
            }
            run.input_state
                .validate()
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        }
        Ok(())
    }
}

/// Read and validate one handoff manifest from an inherited descriptor.
///
/// Takes ownership of `fd` (closing it on return) and reads the single NDJSON
/// manifest line the outgoing image wrote. Fails closed on an unreadable
/// descriptor or a manifest whose schema is not the current [`HANDOFF_SCHEMA`],
/// so a mismatched upgrade never misreads fd numbers.
///
/// # Errors
///
/// Returns an error if the descriptor cannot be read or the content is not a
/// current-schema manifest.
pub fn read_manifest(fd: OwnedFd) -> std::io::Result<HandoffManifest> {
    use std::io::Read;

    let manifest_fd = fd.as_raw_fd();
    let mut file = std::fs::File::from(fd);
    let mut buf = Vec::new();
    file.by_ref()
        .take(MAX_HANDOFF_MANIFEST_BYTES + 1)
        .read_to_end(&mut buf)?;
    if u64::try_from(buf.len()).unwrap_or(u64::MAX) > MAX_HANDOFF_MANIFEST_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "handoff manifest exceeds its bounded size",
        ));
    }
    // `split` always yields at least one slice, so an empty buffer becomes an
    // empty first line that fails the parse below — a fail-closed InvalidData.
    let line = buf.split(|&b| b == b'\n').next().unwrap_or(&[]);
    let manifest: HandoffManifest = serde_json::from_slice(line)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    if manifest.schema != HANDOFF_SCHEMA {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "unknown handoff manifest schema",
        ));
    }
    manifest.validate(manifest_fd)?;
    Ok(manifest)
}

#[cfg(test)]
mod tests {
    use ctxmux_protocol::{AppliedInputRange, ErrorCode, InputOperationKey, ProtocolError};

    use crate::native_control::HandoffInputOperation;

    use super::*;

    fn read_fixture(manifest: &HandoffManifest) -> std::io::Result<HandoffManifest> {
        use std::io::Write;

        let (reader, writer) = rustix::pipe::pipe().unwrap();
        let mut writer = std::fs::File::from(writer);
        writer
            .write_all(&serde_json::to_vec(manifest).unwrap())
            .unwrap();
        writer.write_all(b"\n").unwrap();
        drop(writer);
        read_manifest(reader)
    }

    #[test]
    fn round_trips_through_json_and_lists_all_fds() {
        let manifest = HandoffManifest::new(
            "epoch-xyz".to_string(),
            3,
            4,
            vec![
                HandoffRun {
                    run_id: RunId::new(),
                    child_pid: 4321,
                    master_fd: 7,
                    input_state: HandoffInputState::empty(),
                },
                HandoffRun {
                    run_id: RunId::new(),
                    child_pid: 8765,
                    master_fd: 9,
                    input_state: HandoffInputState::empty(),
                },
            ],
        );
        let bytes = serde_json::to_vec(&manifest).unwrap();
        let parsed: HandoffManifest = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed, manifest);
        assert_eq!(parsed.listener_fd, 3);
        assert_eq!(parsed.state_lock_fd, 4);
        // listener_fd and state_lock_fd lead, then each run's master_fd in order.
        assert_eq!(parsed.all_fds(), vec![3, 4, 7, 9]);
        assert_eq!(parsed.schema, HANDOFF_SCHEMA);
    }

    #[test]
    fn reads_manifest_from_a_pipe_fd() {
        use std::io::Write;
        let (reader, writer) = rustix::pipe::pipe().unwrap();
        let mut writer = std::fs::File::from(writer);
        let manifest = HandoffManifest::new(
            DaemonInstanceId::new().to_string(),
            100,
            101,
            vec![HandoffRun {
                run_id: RunId::new(),
                child_pid: 4321,
                master_fd: 102,
                input_state: HandoffInputState::empty(),
            }],
        );
        writer
            .write_all(&serde_json::to_vec(&manifest).unwrap())
            .unwrap();
        writer.write_all(b"\n").unwrap();
        drop(writer); // EOF so read_to_end returns
        let parsed = read_manifest(reader).unwrap(); // reader is an OwnedFd → moved in
        assert_eq!(parsed, manifest);
    }

    #[test]
    fn input_payload_uses_compact_base64_and_round_trips_exact_bytes() {
        let manifest = HandoffManifest::new(
            DaemonInstanceId::new().to_string(),
            100,
            101,
            vec![HandoffRun {
                run_id: RunId::new(),
                child_pid: 4321,
                master_fd: 102,
                input_state: HandoffInputState {
                    applied_input_bytes: 3,
                    input_failure: None,
                    operations: vec![HandoffInputOperation::Completed {
                        key: InputOperationKey::new("compact-bytes").unwrap(),
                        expected_byte: 0,
                        data: vec![0, 255, 1],
                        range: AppliedInputRange {
                            start_byte: 0,
                            end_byte: 3,
                        },
                    }],
                },
            }],
        );

        let bytes = serde_json::to_vec(&manifest).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            value.pointer("/runs/0/input_state/operations/0/data"),
            Some(&serde_json::Value::String("AP8B".to_owned()))
        );
        assert_eq!(
            serde_json::from_slice::<HandoffManifest>(&bytes).unwrap(),
            manifest
        );
    }

    #[test]
    fn rejects_unbounded_input_diagnostics_before_owner_extraction() {
        let manifest = HandoffManifest::new(
            DaemonInstanceId::new().to_string(),
            100,
            101,
            vec![HandoffRun {
                run_id: RunId::new(),
                child_pid: 4321,
                master_fd: 102,
                input_state: HandoffInputState {
                    applied_input_bytes: 0,
                    input_failure: Some(ProtocolError::new(
                        ErrorCode::Io,
                        "x".repeat(
                            super::super::native_control::HANDOFF_INPUT_DIAGNOSTIC_MAX_BYTES + 1,
                        ),
                    )),
                    operations: Vec::new(),
                },
            }],
        );

        assert_eq!(
            manifest.validate(99).unwrap_err().kind(),
            std::io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn rejects_unknown_schema() {
        let manifest = HandoffManifest::new(
            "epoch-1".to_string(),
            3,
            4,
            vec![HandoffRun {
                run_id: RunId::new(),
                child_pid: 4321,
                master_fd: 7,
                input_state: HandoffInputState::empty(),
            }],
        );
        let mut value: serde_json::Value =
            serde_json::from_slice(&serde_json::to_vec(&manifest).unwrap()).unwrap();
        value["schema"] = serde_json::Value::String("ctxmux.daemon-handoff.v1".to_string());
        let old: HandoffManifest = serde_json::from_value(value).unwrap();
        let error = read_fixture(&old).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn rejects_duplicate_descriptors_runs_and_input_keys() {
        let run_id = RunId::new();
        let epoch = DaemonInstanceId::new().to_string();

        let duplicate_fd = HandoffManifest::new(epoch.clone(), 100, 100, Vec::new());
        assert_eq!(
            duplicate_fd.validate(99).unwrap_err().kind(),
            std::io::ErrorKind::InvalidData
        );

        let duplicate_run = HandoffManifest::new(
            epoch.clone(),
            100,
            101,
            vec![
                HandoffRun {
                    run_id,
                    child_pid: 1,
                    master_fd: 102,
                    input_state: HandoffInputState::empty(),
                },
                HandoffRun {
                    run_id,
                    child_pid: 2,
                    master_fd: 103,
                    input_state: HandoffInputState::empty(),
                },
            ],
        );
        assert_eq!(
            duplicate_run.validate(99).unwrap_err().kind(),
            std::io::ErrorKind::InvalidData
        );

        let key = InputOperationKey::new("duplicate-handoff-key").unwrap();
        let duplicate_key = HandoffManifest::new(
            epoch,
            100,
            101,
            vec![HandoffRun {
                run_id: RunId::new(),
                child_pid: 1,
                master_fd: 102,
                input_state: HandoffInputState {
                    applied_input_bytes: 2,
                    input_failure: None,
                    operations: vec![
                        HandoffInputOperation::Completed {
                            key: key.clone(),
                            expected_byte: 0,
                            data: b"A".to_vec(),
                            range: AppliedInputRange {
                                start_byte: 0,
                                end_byte: 1,
                            },
                        },
                        HandoffInputOperation::Completed {
                            key,
                            expected_byte: 1,
                            data: b"B".to_vec(),
                            range: AppliedInputRange {
                                start_byte: 1,
                                end_byte: 2,
                            },
                        },
                    ],
                },
            }],
        );
        assert_eq!(
            duplicate_key.validate(99).unwrap_err().kind(),
            std::io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn rejects_inconsistent_input_cursor_truth() {
        let manifest = HandoffManifest::new(
            DaemonInstanceId::new().to_string(),
            100,
            101,
            vec![HandoffRun {
                run_id: RunId::new(),
                child_pid: 1,
                master_fd: 102,
                input_state: HandoffInputState {
                    applied_input_bytes: 0,
                    input_failure: None,
                    operations: vec![HandoffInputOperation::Completed {
                        key: InputOperationKey::new("future-range").unwrap(),
                        expected_byte: 0,
                        data: b"A".to_vec(),
                        range: AppliedInputRange {
                            start_byte: 0,
                            end_byte: 1,
                        },
                    }],
                },
            }],
        );
        assert_eq!(
            manifest.validate(99).unwrap_err().kind(),
            std::io::ErrorKind::InvalidData
        );
    }
}
