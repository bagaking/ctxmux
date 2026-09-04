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
    path::Path,
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};

use ctxmux_protocol::{DaemonInstanceId, RunId};

use crate::{creation::HandoffStopOperation, native_control::HandoffInputState};

/// The manifest's **structural** contract, and nothing else.
///
/// Bump this when — and only when — a serialized field is added, removed,
/// renamed, or retyped, so that a manifest written by the running daemon can no
/// longer be read by the incoming image. Do **not** bump it for a byte budget, a
/// shedding policy, or any other behaviour that leaves the JSON shape alone.
///
/// The distinction is load-bearing because [`read_manifest`] compares this for
/// exact equality on the far side of an `execve` that cannot be undone. A bump
/// is therefore not a label: it is a declaration that the next hot upgrade must
/// kill every live Run. History shows the two kinds of change were being
/// conflated — v1→v2 and v2→v3 each added a required field (real breaks), while
/// v3→v4 changed only byte budgets and moved no field at all, spending a fatal
/// bump on an upgrade that would have been safe. `the_schema_string_is_pinned_to_the_manifest_shape`
/// is the lock: it moves with the shape, so it fails on the first kind of change
/// and stays quiet through the second.
pub const HANDOFF_SCHEMA: &str = "ctxmux.daemon-handoff.v4";

/// How this binary declares its handoff schema in `--version` output.
///
/// Printed by `ctxmuxd --version` and parsed back by
/// [`schema_of_version_output`], so the outgoing image can ask an upgrade target
/// what it will accept *before* committing to the exec. Both sides go through
/// this one function precisely so the printer and the parser cannot drift apart.
pub fn version_token() -> String {
    format!("handoff {HANDOFF_SCHEMA}")
}

/// Recover the handoff schema from a `ctxmuxd --version` line, if it declares one.
///
/// `None` means the binary named no schema — an older image that predates
/// [`version_token`]. That is deliberately indistinguishable from "incompatible"
/// to the caller: a binary that cannot state what it accepts cannot be verified,
/// and the only safe reading of an unverifiable exec target is to refuse it.
pub fn schema_of_version_output(text: &str) -> Option<&str> {
    let rest = text.split_once("handoff ")?.1;
    let end = rest.find([')', '\n'])?;
    Some(rest[..end].trim())
}

/// How long the upgrade target gets to answer `--version` before we give up.
const VERSION_PROBE_TIMEOUT: Duration = Duration::from_secs(2);
const VERSION_PROBE_POLL: Duration = Duration::from_millis(10);

/// Run a command to completion under [`VERSION_PROBE_TIMEOUT`], killing it if it
/// overruns. Separated from [`verify_exec_target`] so the bounded-wait mechanics
/// stay out of the way of the compatibility decision the caller is making.
fn run_bounded(exe: &Path, mut child: Child) -> Result<Vec<u8>, String> {
    let deadline = Instant::now() + VERSION_PROBE_TIMEOUT;
    loop {
        let overrun = match child.try_wait() {
            Ok(Some(_)) => break,
            Ok(None) if Instant::now() < deadline => {
                thread::sleep(VERSION_PROBE_POLL);
                continue;
            }
            Ok(None) => format!(
                "{} --version did not answer within {VERSION_PROBE_TIMEOUT:?}",
                exe.display()
            ),
            Err(error) => format!("cannot await {} --version: {error}", exe.display()),
        };
        let _ = child.kill();
        let _ = child.wait();
        return Err(overrun);
    }
    child
        .wait_with_output()
        .map(|output| output.stdout)
        .map_err(|error| format!("cannot read {} --version: {error}", exe.display()))
}

/// Ask an upgrade target whether it can read the manifest we are about to write.
///
/// This runs *before* the point of no return, so its `Err` is a reversible
/// abort: the daemon logs it and keeps serving every live Run. That is the whole
/// value of the check. The same mismatch discovered on the far side of the exec
/// is unrecoverable — by then the old process image is gone, there is no code
/// left to roll back to, and the incoming image's exit closes the inherited pty
/// masters, which SIGHUPs every live child at once.
///
/// Note what is deliberately *not* checked: the target's protocol generation. A
/// protocol skew costs connected clients a `VersionMismatch` and a reconnect,
/// which is a designed, recoverable outcome. Only the handoff schema can turn an
/// upgrade into a fleet-wide kill, so only the handoff schema gates it.
///
/// # Errors
///
/// Returns a human-readable reason when the target cannot be run, does not
/// answer within [`VERSION_PROBE_TIMEOUT`], declares no schema, or declares one
/// this image would reject.
pub fn verify_exec_target(exe: &Path) -> Result<(), String> {
    let child = Command::new(exe)
        .arg("--version")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|error| format!("cannot run {} --version: {error}", exe.display()))?;
    let stdout = run_bounded(exe, child)?;
    let text = String::from_utf8_lossy(&stdout);
    match schema_of_version_output(&text) {
        Some(HANDOFF_SCHEMA) => Ok(()),
        Some(other) => Err(format!(
            "{} accepts handoff schema {other}, this image writes {HANDOFF_SCHEMA}",
            exe.display()
        )),
        None => Err(format!(
            "{} declares no handoff schema, so it cannot be verified to accept {HANDOFF_SCHEMA}",
            exe.display()
        )),
    }
}
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

    /// Every `path:type` pair a value serializes to, array indices collapsed to
    /// `[]` so a fixture holding two Runs reads as the same shape as one holding
    /// one. This is the manifest's wire shape reduced to something comparable.
    fn shape(value: &serde_json::Value, into: &mut Vec<String>, path: &str) {
        match value {
            serde_json::Value::Object(map) => {
                for (key, child) in map {
                    let child_path = format!("{path}.{key}");
                    into.push(format!("{child_path}:{}", type_of(child)));
                    shape(child, into, &child_path);
                }
            }
            serde_json::Value::Array(items) => {
                for item in items {
                    shape(item, into, &format!("{path}[]"));
                }
            }
            _ => {}
        }
    }

    fn type_of(value: &serde_json::Value) -> &'static str {
        match value {
            serde_json::Value::Null => "null",
            serde_json::Value::Bool(_) => "bool",
            serde_json::Value::Number(_) => "number",
            serde_json::Value::String(_) => "string",
            serde_json::Value::Array(_) => "array",
            serde_json::Value::Object(_) => "object",
        }
    }

    #[test]
    fn the_schema_string_is_pinned_to_the_manifest_shape() {
        // The lock that makes HANDOFF_SCHEMA mean "structure", not "version".
        //
        // This fixture is every field name the manifest serializes. Adding,
        // removing, renaming, or retyping one moves the shape, so the manifest
        // this image writes stops being readable by an image built before the
        // change — and reaching that discovery costs every live Run, because it
        // is only reachable past an execve that cannot be undone. The test fails
        // on exactly that kind of change and stays silent through budget or
        // policy edits, which is the distinction v3→v4 spent a fatal bump on.
        //
        // If this fails: bump HANDOFF_SCHEMA and update the fixture together.
        let manifest = HandoffManifest::new_with_stop_operations(
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
                        key: InputOperationKey::new("shape-lock").unwrap(),
                        expected_byte: 0,
                        data: vec![0, 255, 1],
                        range: AppliedInputRange {
                            start_byte: 0,
                            end_byte: 3,
                        },
                    }],
                },
            }],
            vec![HandoffStopOperation {
                run_id: RunId::new(),
                operation_key: StopOperationKey::new("shape-lock-stop").unwrap(),
                outcome: HandoffStopOutcome::Accepted {
                    disposition: StopDisposition::Forced,
                },
            }],
        );

        let mut observed = Vec::new();
        shape(&serde_json::to_value(&manifest).unwrap(), &mut observed, "");
        observed.sort();
        observed.dedup();

        let expected = [
            ".schema:string",
            ".epoch:string",
            ".listener_fd:number",
            ".state_lock_fd:number",
            ".runs:array",
            ".runs[].run_id:string",
            ".runs[].child_pid:number",
            ".runs[].master_fd:number",
            ".runs[].input_state:object",
            ".runs[].input_state.applied_input_bytes:number",
            ".runs[].input_state.input_failure:null",
            ".runs[].input_state.operations:array",
            ".runs[].input_state.operations[].outcome:string",
            ".runs[].input_state.operations[].key:string",
            ".runs[].input_state.operations[].expected_byte:number",
            ".runs[].input_state.operations[].data:string",
            ".runs[].input_state.operations[].range:object",
            ".runs[].input_state.operations[].range.start_byte:number",
            ".runs[].input_state.operations[].range.end_byte:number",
            ".stop_operations:array",
            ".stop_operations[].run_id:string",
            ".stop_operations[].operation_key:string",
            // The Stop outcome is an internally tagged enum, so its tag and
            // payload nest under `outcome` rather than flattening beside it.
            ".stop_operations[].outcome:object",
            ".stop_operations[].outcome.outcome:string",
            ".stop_operations[].outcome.disposition:string",
        ];
        // Sorted rather than compared in fixture order: the entries above read
        // top-down like the struct, and nothing about this check should depend on
        // a reader knowing that '.' sorts before ':'.
        let mut expected: Vec<String> = expected.iter().map(|s| (*s).to_owned()).collect();
        expected.sort();
        assert_eq!(
            observed,
            expected,
            "the handoff manifest's serialized shape moved. A reader built before \
             this change cannot parse what this image now writes, and it finds out \
             only after an execve it cannot undo — every live Run dies. Bump \
             HANDOFF_SCHEMA (currently {HANDOFF_SCHEMA}) and this fixture together."
        );
    }

    #[test]
    fn a_version_line_round_trips_through_its_own_parser() {
        // Printer and parser must not drift: if --version stops declaring what
        // schema_of_version_output looks for, every upgrade target reads as
        // unverifiable and no upgrade can ever proceed.
        let line = format!("ctxmuxd 0.1.0 (protocol 17, {})", version_token());
        assert_eq!(schema_of_version_output(&line), Some(HANDOFF_SCHEMA));
        assert_eq!(
            schema_of_version_output(&format!("{line}\n")),
            Some(HANDOFF_SCHEMA)
        );
        // A binary predating the token declares nothing — which must not read as
        // a match, or the probe would wave through exactly the image it exists
        // to catch.
        assert_eq!(
            schema_of_version_output("ctxmuxd 0.1.0 (protocol 16)"),
            None
        );
    }

    #[test]
    fn an_unverifiable_exec_target_is_refused() {
        // /bin/echo answers --version with something that names no schema. The
        // probe must refuse it: an image that cannot say what it accepts cannot
        // be trusted with every live Run.
        let error = verify_exec_target(Path::new("/bin/echo")).unwrap_err();
        assert!(
            error.contains("declares no handoff schema"),
            "unexpected refusal: {error}"
        );
        // A target that cannot even be run is likewise a refusal, not a panic.
        assert!(verify_exec_target(Path::new("/nonexistent/ctxmuxd")).is_err());
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
