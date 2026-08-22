//! The upgrade handoff manifest carried across `execve`-in-place.
//!
//! Serialized to a single inherited descriptor by the outgoing image and read
//! back by the incoming image. Versioned so a mismatched upgrade fails closed
//! rather than misreading fd numbers.
//!
//! Only the pty master fd is carried per Run. The reader and writer fds of a
//! native Run both refer to the same open file description as the master, so
//! the incoming image re-derives them from the master fd after exec (re-dup a
//! cloexec reader, write input directly to the master) exactly as the spawn
//! path does.

#![allow(dead_code)]

use std::os::fd::{OwnedFd, RawFd};

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
    pub child_pid: u32,
    pub master_fd: RawFd,
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
        self.runs.iter().map(|r| r.master_fd).collect()
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

    let mut file = std::fs::File::from(fd);
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)?;
    let line = buf.split(|&b| b == b'\n').next().unwrap_or(&[]);
    let manifest: HandoffManifest = serde_json::from_slice(line).map_err(std::io::Error::other)?;
    if manifest.schema != HANDOFF_SCHEMA {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "unknown handoff manifest schema",
        ));
    }
    Ok(manifest)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_through_json_and_lists_all_fds() {
        let manifest = HandoffManifest::new(
            "epoch-xyz".to_string(),
            vec![
                HandoffRun {
                    run_id: RunId::new(),
                    child_pid: 4321,
                    master_fd: 7,
                },
                HandoffRun {
                    run_id: RunId::new(),
                    child_pid: 8765,
                    master_fd: 9,
                },
            ],
        );
        let bytes = serde_json::to_vec(&manifest).unwrap();
        let parsed: HandoffManifest = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed, manifest);
        assert_eq!(parsed.all_fds(), vec![7, 9]);
        assert_eq!(parsed.schema, HANDOFF_SCHEMA);
    }

    #[test]
    fn reads_manifest_from_a_pipe_fd() {
        use std::io::Write;
        let (reader, writer) = rustix::pipe::pipe().unwrap();
        let mut writer = std::fs::File::from(writer);
        let manifest = HandoffManifest::new(
            "epoch-1".to_string(),
            vec![HandoffRun {
                run_id: RunId::new(),
                child_pid: 4321,
                master_fd: 7,
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
    fn rejects_unknown_schema() {
        use std::io::Write;
        let manifest = HandoffManifest::new(
            "epoch-1".to_string(),
            vec![HandoffRun {
                run_id: RunId::new(),
                child_pid: 4321,
                master_fd: 7,
            }],
        );
        let mut value: serde_json::Value =
            serde_json::from_slice(&serde_json::to_vec(&manifest).unwrap()).unwrap();
        value["schema"] = serde_json::Value::String("ctxmux.daemon-handoff.v99".to_string());
        let (reader, writer) = rustix::pipe::pipe().unwrap();
        let mut writer = std::fs::File::from(writer);
        writer
            .write_all(&serde_json::to_vec(&value).unwrap())
            .unwrap();
        writer.write_all(b"\n").unwrap();
        drop(writer);
        let error = read_manifest(reader).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    }
}
