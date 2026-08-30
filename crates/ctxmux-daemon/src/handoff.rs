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

use crate::{creation::HandoffStopOperation, native_control::HandoffInputState};

pub const HANDOFF_SCHEMA: &str = "ctxmux.daemon-handoff.v4";
// The only Run-count-multiplied payload in this manifest is recoverable Input:
// each Run may retain up to INPUT_RESULT_MAX_REQUEST_BYTES (1 MiB) of request
// bytes. Multiplying that by the Run count is exactly the bound that fails at
// thousands of Runs (128 * 1 MiB was 128 MiB; 4000 would be ~4 GiB in one line
// that must be serialized, written, and re-read across the exec). So the
// aggregate carried across a handoff is a fixed daemon-wide total that every
// retained Run shares, mirroring persistence.rs's GLOBAL_REPLAY_BYTES capping
// PER_RUN_REPLAY_BYTES rather than summing it per Run. 128 MiB keeps the prior
// 128-Run ceiling as the whole-daemon budget: below it nothing sheds, and above
// it the newest handoffs shed their oldest idempotency results (a client that
// re-sends a shed key is simply re-applied, fenced by the input cursor) until
// the total fits. This does not grow with Run count.
const MAX_HANDOFF_INPUT_REQUEST_BYTES: usize = 128 * 1024 * 1024;
// The read ceiling bounds the whole inherited file. Its dominant term is the
// aggregate Input payload above, base64-inflated 4/3 in JSON; the fixed 64 MiB
// slack then covers the manifest's structural content: fd numbers, keys,
// ranges, epoch, and the bounded per-Run diagnostics. That structural content
// is still O(Runs) — every live Run contributes one irreducible descriptor set
// that must cross the exec — but at a few hundred bytes per Run the slack
// absorbs far beyond the thousands-of-Runs target. What this derivation removes
// is the 1 MiB * Run-count *payload* term that produced ~8 GiB at 4000 Runs: the
// dominant term is now a fixed aggregate, not a per-Run cap multiplied by count.
const MAX_HANDOFF_MANIFEST_BYTES: u64 =
    (MAX_HANDOFF_INPUT_REQUEST_BYTES as u64) * 4 / 3 + 64 * 1024 * 1024;
// Stop results carry no request payload; only an Unknown outcome retains a
// bounded diagnostic (HANDOFF_INPUT_DIAGNOSTIC_MAX_BYTES, 4 KiB per item). The
// old check bounded their *count* by the Run cap; the byte-equivalent daemon
// budget is a fixed 16 MiB total that every Run shares, which does not grow
// with Run count and stays well within the manifest's structural allowance.
const MAX_HANDOFF_STOP_DIAGNOSTIC_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HandoffManifest {
    pub schema: String,
    pub epoch: String,
    pub listener_fd: RawFd,
    pub state_lock_fd: RawFd,
    pub runs: Vec<HandoffRun>,
    pub stop_operations: Vec<HandoffStopOperation>,
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
        Self::new_with_stop_operations(epoch, listener_fd, state_lock_fd, runs, Vec::new())
    }

    pub fn new_with_stop_operations(
        epoch: String,
        listener_fd: RawFd,
        state_lock_fd: RawFd,
        runs: Vec<HandoffRun>,
        stop_operations: Vec<HandoffStopOperation>,
    ) -> Self {
        let mut manifest = Self {
            schema: HANDOFF_SCHEMA.to_string(),
            epoch,
            listener_fd,
            state_lock_fd,
            runs,
            stop_operations,
        };
        // Enforce the daemon-wide aggregate at construction, not merely at read
        // time: a daemon running thousands of Runs would otherwise serialize a
        // manifest it cannot re-read. Shedding here keeps the produced manifest
        // inside the bound by construction; validate() then re-checks it so a
        // corrupt inherited file still fails closed.
        manifest.shed_recoverable_input_to_budget(MAX_HANDOFF_INPUT_REQUEST_BYTES);
        manifest.shed_stop_diagnostics_to_budget(MAX_HANDOFF_STOP_DIAGNOSTIC_BYTES);
        manifest
    }

    /// Total recoverable-Input request bytes across every retained Run. This is
    /// the only payload in the manifest that scales with both Run count and a
    /// 1 MiB per-Run cap, so it is the term the aggregate budget governs.
    fn retained_input_request_bytes(&self) -> usize {
        self.runs.iter().fold(0, |sum, run| {
            sum.saturating_add(run.input_state.retained_request_bytes())
        })
    }

    /// Total Stop-diagnostic bytes across every settled Stop result. Only an
    /// Unknown outcome carries one (bounded per item), so this is normally zero.
    fn stop_diagnostic_bytes(&self) -> usize {
        self.stop_operations
            .iter()
            .fold(0, |sum, op| sum.saturating_add(op.diagnostic_bytes()))
    }

    /// Shed the oldest retained Input results, Run by Run, until the daemon-wide
    /// total fits `budget`. A shed result is only an idempotency cache entry: a
    /// client that re-sends its key after the exec is re-applied under the same
    /// incarnation-fenced Input cursor, so shedding reduces what the handoff
    /// carries without ever corrupting Input state. Parameterized by `budget` so
    /// tests exercise it without allocating [`MAX_HANDOFF_INPUT_REQUEST_BYTES`].
    fn shed_recoverable_input_to_budget(&mut self, budget: usize) {
        let mut total = self.retained_input_request_bytes();
        for run in &mut self.runs {
            if total <= budget {
                break;
            }
            while total > budget {
                let freed = run.input_state.shed_oldest_operation();
                if freed == 0 {
                    break;
                }
                total = total.saturating_sub(freed);
            }
        }
    }

    /// Shed the oldest settled Stop results until their aggregate diagnostic
    /// bytes fit `budget`. The Stop ledger is a best-effort same-incarnation
    /// idempotency cache (cold restart never loads it), and Stop is idempotent by
    /// disposition, so dropping the oldest entry only shrinks that cache rather
    /// than losing required truth.
    fn shed_stop_diagnostics_to_budget(&mut self, budget: usize) {
        let mut total = self.stop_diagnostic_bytes();
        while total > budget && !self.stop_operations.is_empty() {
            let freed = self.stop_operations.remove(0).diagnostic_bytes();
            total = total.saturating_sub(freed);
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
        // Aggregate byte bounds, not Run-count bounds: what must fit across the
        // exec is the daemon-wide recoverable-Input total and the Stop-diagnostic
        // total, each shared by every retained Run rather than multiplied by it.
        let input_bytes = self.retained_input_request_bytes();
        if input_bytes > MAX_HANDOFF_INPUT_REQUEST_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "handoff retains {input_bytes} recoverable Input request bytes; maximum is {MAX_HANDOFF_INPUT_REQUEST_BYTES}"
                ),
            ));
        }
        let stop_diagnostic_bytes = self.stop_diagnostic_bytes();
        if stop_diagnostic_bytes > MAX_HANDOFF_STOP_DIAGNOSTIC_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "handoff retains {stop_diagnostic_bytes} native Stop diagnostic bytes; maximum is {MAX_HANDOFF_STOP_DIAGNOSTIC_BYTES}"
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
        let mut stop_runs = HashSet::new();
        let mut stop_keys = HashSet::new();
        for operation in &self.stop_operations {
            operation
                .validate()
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
            if !stop_runs.insert(operation.run_id)
                || !stop_keys.insert(operation.operation_key.clone())
            {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "handoff native Stop operations must have unique Runs and keys",
                ));
            }
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
    use ctxmux_protocol::{
        AppliedInputRange, ErrorCode, InputOperationKey, ProtocolError, StopDisposition,
        StopOperationKey,
    };

    use crate::{
        creation::{HandoffStopOperation, HandoffStopOutcome},
        native_control::HandoffInputOperation,
    };

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
    fn settled_stop_ledger_round_trips_in_the_versioned_manifest() {
        let operation = HandoffStopOperation {
            run_id: RunId::new(),
            operation_key: StopOperationKey::new("handoff-stop-result").unwrap(),
            outcome: HandoffStopOutcome::Accepted {
                disposition: StopDisposition::Forced,
            },
        };
        let manifest = HandoffManifest::new_with_stop_operations(
            DaemonInstanceId::new().to_string(),
            100,
            101,
            Vec::new(),
            vec![operation.clone()],
        );

        let parsed = read_fixture(&manifest).expect("read handed-off Stop ledger");
        assert_eq!(parsed.schema, "ctxmux.daemon-handoff.v4");
        assert_eq!(parsed.stop_operations, [operation]);
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

    /// A Run whose recoverable-Input ledger holds `payloads.len()` contiguous
    /// completed operations, each `payloads[i]` bytes, keyed by `prefix`.
    fn run_with_input(prefix: &str, master_fd: RawFd, payloads: &[usize]) -> HandoffRun {
        let mut operations = Vec::with_capacity(payloads.len());
        let mut cursor = 0_u64;
        for (index, &len) in payloads.iter().enumerate() {
            let end = cursor + len as u64;
            operations.push(HandoffInputOperation::Completed {
                key: InputOperationKey::new(format!("{prefix}-{index}")).unwrap(),
                expected_byte: cursor,
                data: vec![b'x'; len],
                range: AppliedInputRange {
                    start_byte: cursor,
                    end_byte: end,
                },
            });
            cursor = end;
        }
        HandoffRun {
            run_id: RunId::new(),
            child_pid: 1,
            master_fd,
            input_state: HandoffInputState {
                applied_input_bytes: cursor,
                input_failure: None,
                operations,
            },
        }
    }

    #[test]
    fn aggregate_input_budget_does_not_scale_with_run_count() {
        // Two Runs, each already at the per-Run 1 MiB cap, would pass every
        // per-Run check yet exceed a fixed 1 MiB daemon budget between them. The
        // manifest must shed down to the aggregate regardless of how the bytes
        // are distributed across Runs.
        let mut manifest = HandoffManifest::new(
            DaemonInstanceId::new().to_string(),
            100,
            101,
            vec![
                run_with_input("run-a", 102, &[600, 600]),
                run_with_input("run-b", 103, &[600, 600]),
            ],
        );
        assert_eq!(manifest.retained_input_request_bytes(), 2400);

        // Budget below the total: the oldest results shed Run by Run until the
        // daemon-wide sum fits, and the survivors are still a valid ledger.
        manifest.shed_recoverable_input_to_budget(1000);
        assert!(manifest.retained_input_request_bytes() <= 1000);
        manifest.validate(99).expect("shed ledger stays valid");
    }

    #[test]
    fn construction_sheds_recoverable_input_to_the_daemon_budget() {
        // Enforced at construction, not merely asserted at read time. Driving the
        // real 128 MiB constant would need a 128 MiB allocation, so this checks
        // the observable invariant the constructor guarantees: whatever the input
        // distribution, the built manifest never exceeds the aggregate budget.
        // The dedicated budget-shedding coverage above uses a small budget; here
        // we confirm the production constructor path applies that shedding.
        let manifest = HandoffManifest::new(
            DaemonInstanceId::new().to_string(),
            100,
            101,
            vec![
                run_with_input("run-a", 102, &[4096, 4096]),
                run_with_input("run-b", 103, &[4096]),
            ],
        );
        assert!(manifest.retained_input_request_bytes() <= MAX_HANDOFF_INPUT_REQUEST_BYTES);
        // Well under the budget, so nothing sheds and the ledger survives intact.
        assert_eq!(manifest.retained_input_request_bytes(), 12288);
        manifest
            .validate(99)
            .expect("constructed manifest is valid");
    }

    #[test]
    fn validate_rejects_an_input_total_over_the_aggregate() {
        // A corrupt inherited file that bypasses construction-time shedding must
        // still fail closed at read time.
        let mut manifest = HandoffManifest::new(
            DaemonInstanceId::new().to_string(),
            100,
            101,
            vec![run_with_input("run-a", 102, &[8])],
        );
        manifest.runs[0].input_state = HandoffInputState {
            applied_input_bytes: (MAX_HANDOFF_INPUT_REQUEST_BYTES + 1) as u64,
            input_failure: None,
            operations: vec![HandoffInputOperation::Completed {
                key: InputOperationKey::new("oversized").unwrap(),
                expected_byte: 0,
                data: vec![b'x'; MAX_HANDOFF_INPUT_REQUEST_BYTES + 1],
                range: AppliedInputRange {
                    start_byte: 0,
                    end_byte: (MAX_HANDOFF_INPUT_REQUEST_BYTES + 1) as u64,
                },
            }],
        };
        let error = manifest.validate(99).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert!(
            error
                .to_string()
                .contains("recoverable Input request bytes")
        );
    }

    #[test]
    fn stop_diagnostic_budget_sheds_oldest_and_validate_enforces_it() {
        let unknown_stop = |key: &str, message: &str| HandoffStopOperation {
            run_id: RunId::new(),
            operation_key: StopOperationKey::new(key).unwrap(),
            outcome: HandoffStopOutcome::Unknown {
                failure: ctxmux_protocol::ControlFailure {
                    error: ProtocolError::new(ErrorCode::Io, message.to_owned()),
                    disposition: ctxmux_protocol::CommandDisposition::Unknown,
                },
            },
        };
        let mut manifest = HandoffManifest::new_with_stop_operations(
            DaemonInstanceId::new().to_string(),
            100,
            101,
            Vec::new(),
            vec![unknown_stop("stop-a", "aaaa"), unknown_stop("stop-b", "bb")],
        );
        assert_eq!(manifest.stop_diagnostic_bytes(), 6);

        // A budget below the total sheds the oldest entry first.
        manifest.shed_stop_diagnostics_to_budget(3);
        assert_eq!(manifest.stop_operations.len(), 1);
        assert_eq!(manifest.stop_operations[0].operation_key.as_str(), "stop-b");
        manifest.validate(99).expect("shed Stop ledger stays valid");

        // Read-time enforcement of a total past the aggregate ceiling.
        manifest.stop_operations = vec![unknown_stop(
            "stop-big",
            &"z".repeat(MAX_HANDOFF_STOP_DIAGNOSTIC_BYTES + 1),
        )];
        let error = manifest.validate(99).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("Stop diagnostic bytes"));
    }
}
