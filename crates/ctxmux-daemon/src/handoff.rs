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

use crate::{ResourceLimits, creation::HandoffStopOperation, native_control::HandoffInputState};

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
pub const HANDOFF_SCHEMA: &str = "ctxmux.daemon-handoff.v5";

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
// Upgrade budgets belong to the runtime policy. Complete settled receipts cross
// the same-incarnation boundary; exceeding a budget must abort before extraction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HandoffManifest {
    pub schema: String,
    pub epoch: String,
    pub listener_fd: RawFd,
    pub state_lock_fd: RawFd,
    pub runs: Vec<HandoffRun>,
    pub stop_operations: Vec<HandoffStopOperation>,
    pub closed_inputs: Vec<HandoffClosedInput>,
    pub resources: ResourceLimits,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HandoffClosedInput {
    pub run_id: RunId,
    pub input_state: HandoffInputState,
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
        Self::configured(
            epoch,
            listener_fd,
            state_lock_fd,
            runs,
            stop_operations,
            Vec::new(),
            ResourceLimits::default(),
        )
    }

    pub(crate) fn configured(
        epoch: String,
        listener_fd: RawFd,
        state_lock_fd: RawFd,
        runs: Vec<HandoffRun>,
        stop_operations: Vec<HandoffStopOperation>,
        closed_inputs: Vec<HandoffClosedInput>,
        resources: ResourceLimits,
    ) -> Self {
        Self {
            schema: HANDOFF_SCHEMA.to_string(),
            epoch,
            listener_fd,
            state_lock_fd,
            runs,
            stop_operations,
            closed_inputs,
            resources,
        }
    }

    /// Total recoverable-Input request bytes across every retained Run. This is
    /// the only payload in the manifest that scales with both Run count and a
    /// 1 MiB per-Run cap, so it is the term the aggregate budget governs.
    fn retained_input_request_bytes(&self) -> usize {
        self.input_states().fold(0, |sum, state| {
            sum.saturating_add(state.retained_request_bytes())
        })
    }

    /// Total Stop-diagnostic bytes across every settled Stop result. Only an
    /// Unknown outcome carries one (bounded per item), so this is normally zero.
    fn stop_diagnostic_bytes(&self) -> usize {
        self.stop_operations
            .iter()
            .fold(0, |sum, op| sum.saturating_add(op.diagnostic_bytes()))
    }

    fn input_states(&self) -> impl Iterator<Item = &HandoffInputState> {
        self.runs
            .iter()
            .map(|run| &run.input_state)
            .chain(self.closed_inputs.iter().map(|run| &run.input_state))
    }

    /// Serialize and validate while all native owners still retain authority.
    pub(crate) fn write_preflight(&self, file: &mut std::fs::File) -> std::io::Result<()> {
        use std::io::{Seek, Write};
        self.validate(file.as_raw_fd())?;
        file.set_len(0)?;
        file.rewind()?;
        // The byte-limited writer refuses before growing a file that the next
        // image could not read. No retained result is removed to fit the budget.
        let mut writer = BoundedWriter {
            inner: file,
            remaining: self.resources.handoff_bytes,
        };
        serde_json::to_writer(&mut writer, self).map_err(std::io::Error::other)?;
        writer.write_all(b"\n")?;
        writer.inner.flush()?;
        writer.inner.rewind()?;
        Ok(())
    }

    /// Every fd number this manifest expects to survive the exec: the process
    /// listener and state-lock descriptors first, then each Run's pty master.
    pub fn all_fds(&self) -> Vec<RawFd> {
        let mut fds = vec![self.listener_fd, self.state_lock_fd];
        fds.extend(self.runs.iter().map(|r| r.master_fd));
        fds
    }

    fn validate(&self, manifest_fd: RawFd) -> std::io::Result<()> {
        self.resources
            .validate()
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
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
        if input_bytes > self.resources.handoff_input_bytes {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "handoff retains {input_bytes} recoverable Input request bytes; maximum is {}",
                    self.resources.handoff_input_bytes
                ),
            ));
        }
        let stop_diagnostic_bytes = self.stop_diagnostic_bytes();
        if stop_diagnostic_bytes > self.resources.handoff_diagnostic_bytes {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "handoff retains {stop_diagnostic_bytes} native Stop diagnostic bytes; maximum is {}",
                    self.resources.handoff_diagnostic_bytes
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
                .validate_with_resources(self.resources)
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        }
        for run in &self.closed_inputs {
            if !run_ids.insert(run.run_id) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "handoff closed Input Runs must be unique and separate from live Runs",
                ));
            }
            run.input_state
                .validate_with_resources(self.resources)
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        }
        let control_bytes = self
            .input_states()
            .fold(0_u64, |total, state| {
                total.saturating_add(state.control_memory_bytes())
            })
            .saturating_add(
                self.stop_operations
                    .iter()
                    .map(HandoffStopOperation::control_memory_bytes)
                    .sum::<u64>(),
            );
        if control_bytes > self.resources.control_state_bytes {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "handoff control state exceeds the preserved runtime policy",
            ));
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
    read_manifest_with_limit(fd, ResourceLimits::DEFAULT.handoff_bytes)
}

pub(crate) fn read_manifest_with_limit(
    fd: OwnedFd,
    limit: u64,
) -> std::io::Result<HandoffManifest> {
    use std::io::Read;
    let manifest_fd = fd.as_raw_fd();
    let mut file = std::fs::File::from(fd);
    let mut buf = Vec::new();
    file.by_ref()
        .take(limit.saturating_add(1))
        .read_to_end(&mut buf)?;
    if buf.len() as u64 > limit {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "handoff manifest exceeds the preserved byte budget",
        ));
    }
    let manifest: HandoffManifest = serde_json::from_slice(&buf)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    if manifest.schema != HANDOFF_SCHEMA {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "unknown handoff manifest schema",
        ));
    }
    manifest.validate(manifest_fd)?;
    if buf.len() as u64 > manifest.resources.handoff_bytes {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "handoff exceeds its own runtime policy",
        ));
    }
    Ok(manifest)
}

struct BoundedWriter<'a> {
    inner: &'a mut std::fs::File,
    remaining: u64,
}
impl std::io::Write for BoundedWriter<'_> {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        if data.len() as u64 > self.remaining {
            return Err(std::io::Error::other(
                "handoff exceeds handoff_bytes; upgrade remains reversible",
            ));
        }
        let written = std::io::Write::write(self.inner, data)?;
        self.remaining -= written as u64;
        Ok(written)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        std::io::Write::flush(self.inner)
    }
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
        assert_eq!(parsed.schema, "ctxmux.daemon-handoff.v5");
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
            ".closed_inputs:array",
            ".resources:object",
            ".resources.live_runs:null",
            ".resources.retained_runs:null",
            ".resources.hot_output_bytes:number",
            ".resources.live_event_bytes:number",
            ".resources.run_output_bytes:number",
            ".resources.metadata_bytes:number",
            ".resources.durable_replay_bytes:number",
            ".resources.durable_run_output_bytes:number",
            ".resources.database_bytes:number",
            ".resources.wal_checkpoint_bytes:number",
            ".resources.handoff_input_bytes:number",
            ".resources.handoff_diagnostic_bytes:number",
            ".resources.handoff_bytes:number",
            ".resources.control_state_bytes:number",
            ".resources.creation_workers:number",
            ".resources.input_workers:number",
            ".resources.cleanup_workers:number",
            ".resources.finalize_workers:number",
            ".resources.input_queue_commands:number",
            ".resources.input_queue_bytes:number",
            ".resources.input_result_entries:number",
            ".resources.input_result_bytes:number",
            ".resources.tmux_discovery_bytes:number",
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
            observed, expected,
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
    fn upgrade_refuses_pressure_without_discarding_results() {
        let mut manifest = HandoffManifest::new(
            DaemonInstanceId::new().to_string(),
            100,
            101,
            vec![
                run_with_input("run-a", 102, &[600, 600]),
                run_with_input("run-b", 103, &[600, 600]),
            ],
        );
        manifest.resources.handoff_input_bytes = 1000;
        let original = manifest.clone();
        let mut file = tempfile::tempfile().unwrap();
        assert!(manifest.write_preflight(&mut file).is_err());
        assert_eq!(manifest, original);
        assert_eq!(manifest.retained_input_request_bytes(), 2400);
        manifest.resources.handoff_input_bytes = 2400;
        manifest.resources.handoff_bytes = 32;
        assert!(manifest.write_preflight(&mut file).is_err());
        manifest.resources.handoff_bytes = 64 * 1024;
        manifest.write_preflight(&mut file).unwrap();
        let parsed = read_manifest(file.into()).unwrap();
        assert_eq!(parsed, manifest);
    }

    #[test]
    fn upgrade_never_sheds_unknown_stop_results() {
        let operation = HandoffStopOperation {
            run_id: RunId::new(),
            operation_key: StopOperationKey::new("stop-a").unwrap(),
            outcome: HandoffStopOutcome::Unknown {
                failure: ctxmux_protocol::ControlFailure {
                    error: ProtocolError::new(ErrorCode::Io, "diagnostic"),
                    disposition: ctxmux_protocol::CommandDisposition::Unknown,
                },
            },
        };
        let mut manifest = HandoffManifest::new_with_stop_operations(
            DaemonInstanceId::new().to_string(),
            100,
            101,
            Vec::new(),
            vec![operation.clone()],
        );
        manifest.resources.handoff_diagnostic_bytes = 3;
        let mut file = tempfile::tempfile().unwrap();
        assert!(manifest.write_preflight(&mut file).is_err());
        assert_eq!(manifest.stop_operations, [operation]);
        manifest.resources.handoff_diagnostic_bytes = 64;
        manifest.write_preflight(&mut file).unwrap();
    }
}
