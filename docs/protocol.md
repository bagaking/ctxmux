# Native Protocol Generation 1

This document describes the currently implemented local daemon boundary. It is
pre-stable: obsolete contracts are replaced directly rather than preserved with
fallbacks or migrations.

## Transport

- Unix domain socket selected explicitly by the daemon operator.
- Socket permissions are set to owner read/write only.
- Each frame is one UTF-8 JSON value followed by a newline.
- A frame may not exceed 1 MiB.
- Raw PTY bytes are represented as integer arrays in generation 1.

If a requested socket path is an ordinary file or symlink rather than a socket,
the daemon refuses to replace it. A stale socket is removed only after verifying
that it is a socket and that no process accepts a connection there.

## Connection state

Every connection begins with `ClientFrame::Hello`. The daemon either returns a
matching `ServerFrame::Hello` or an explicit `version_mismatch` error and closes
the connection.

After the handshake, a connection has one of two shapes:

1. A short-lived request receives one response or one explicit error.
2. An `attach` request receives one snapshot followed by ordered Run events
   until detach, disconnect, Run exit, or daemon exit.

Closing a client socket only removes that attachment. It does not stop the Run.

## Native Run operations

- `start`: create a PTY, spawn the declared command, and return Run metadata.
- `list`: return all Runs retained by this daemon.
- `status`: return current metadata for one Run.
- `input`: write raw bytes to a live Run's PTY.
- `resize`: change live PTY rows and columns.
- `attach`: return retained output after a sequence cursor and follow new
  output and exit events.
- `stop`: terminate a live Run.

Unknown Runs, invalid dimensions, incompatible protocol versions, failed
process spawns, and operations against an exited Run are distinct public error
categories. Unsupported or invalid behavior never silently succeeds.

## Output and reconnect

PTY output is divided into monotonically sequenced chunks. The daemon currently
retains at most 4 MiB per Run. An attachment supplies its last observed sequence
and receives:

- retained chunks newer than that sequence;
- the oldest and newest retained sequences;
- a `truncated` flag when required output was already evicted;
- future ordered output, accepted-operation, gap, and exit events.

The daemon subscribes an attachment before taking its replay snapshot and
deduplicates live events already covered by that snapshot. Before publishing an
exit event, it gives the PTY reader a bounded opportunity to drain the child's
final output.

This byte log does not reconstruct the current screen of a full-screen TUI. A
future screen model must be introduced only with an acceptance test that proves
late attachment behavior.

## Current lifetime boundary

Runs outlive CLI and SDK connections, not the daemon process itself. Metadata,
output, and live PTYs are currently memory-owned by one daemon. Daemon restart
recovery remains explicitly unimplemented.

## Authoritative schema

Rust wire types and error categories live in `crates/ctxmux-protocol`. The Rust
connector lives in `crates/ctxmux-client`.

TypeScript wire declarations under `packages/sdk/src/generated` are generated
from those Rust types with `ts-rs`; they are not maintained as a second schema.
`scripts/generate-protocol-types.sh` refreshes them, and
`scripts/check-protocol-types.sh` generates into a temporary directory and
fails on any checked-in drift. The TypeScript client implements the same hello,
request, attachment, event, and error frames as the Rust client. It also
validates the complete nested generation-1 frame at runtime, rejects duplicate
JSON members and malformed UTF-8, and rejects `u64` cursor values outside
JavaScript's safe-integer range rather than exposing rounded state.
