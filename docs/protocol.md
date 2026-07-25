# Local Protocol Generation 3

This document describes the currently implemented local daemon boundary. It is
pre-stable: obsolete contracts are replaced directly rather than preserved with
fallbacks or migrations.

## Transport

- Unix domain socket selected explicitly by the daemon operator.
- Socket permissions are set to owner read/write only.
- Each frame is one UTF-8 JSON value followed by a newline.
- A frame may not exceed 1 MiB.
- Raw PTY bytes are represented as integer arrays in generation 3.

If a requested socket path is an ordinary file or symlink rather than a socket,
the daemon refuses to replace it. A stale socket is removed only after verifying
that it is a socket and that no process accepts a connection there. Before
unlinking, the daemon rechecks the socket's device and inode and probes it a
second time; an observed replacement fails with `SocketTargetChanged` and is
left untouched.

## Connection state

Every connection begins with `ClientFrame::Hello`. The daemon either returns a
matching `ServerFrame::Hello` or an explicit `version_mismatch` error and closes
the connection.

After the handshake, a connection has one of two shapes:

1. A short-lived request receives one response or one explicit error.
2. An `attach` request receives one metadata snapshot header, zero or more
   ordered replay `output` events through the advertised replay head, then live
   Run events until detach, disconnect, Run exit, or daemon exit. Rust and
   TypeScript clients reassemble the replay events before returning their
   public attachment snapshot.

Closing a client socket only removes that attachment. It does not stop the Run.

## Run operations

- `start`: create a PTY, spawn the declared command, and return Run metadata.
- `discover_tmux`: list live panes from one explicit tmux server socket after
  separately validating the client executable and selected server version.
- `import_tmux`: bind one discovered pane identity and publish a read-only,
  memory-only Run observed through public Control Mode.
- `fork`: create a child through an explicit Level A or Level B plan and return
  metadata containing its immediate parent and actual fidelity.
- `list`: return all Runs retained by this daemon.
- `status`: return current metadata for one Run.
- `input`: write raw bytes to a live Run's PTY.
- `resize`: change live PTY rows and columns.
- `attach`: return retained output after a sequence cursor and follow new
  output and exit events.
- `stop`: request termination of the owned direct child and return after the
  native child handle accepts that request. On Unix the current
  `portable-pty` path gives `SIGHUP` a short grace period, then escalates to a
  forced kill if the direct child remains alive.

In persistent mode, recovered `exited` and `interrupted { reason:
daemon_restart }` Runs support `list`, `status`, `attach`, and Level A `fork`.
They reject `input`, `resize`, `stop`, and Level B `fork` with
`invalid_run_state`; a replacement daemon never turns a stored PID into live
process authority.

Tmux discovery remains available in persistent mode, but tmux import returns
`unsupported_capability`: ctxmux does not persist or recover Control Mode
ownership in generation 3.

Unknown Runs, invalid dimensions, incompatible protocol versions, failed
process spawns, durable mutation failures, and operations against a terminal
Run are distinct public error categories. Unsupported or invalid behavior never
silently succeeds.

Every generation-3 `RunSpec` includes `declared_inputs`, an ordered list of
opaque workspace, artifact, or context references. The daemon records these
references without dereferencing, copying, normalizing, or inferring ownership
from them. Ordinary `start` returns `lineage: null`.

Level A fork resolves the retained parent and clones its complete immutable
`RunSpec`, including `declared_inputs`. Level B executes one caller-materialized
`RunSpec` without merging it with or falling back to the parent. Both variants
publish the child only after native launch succeeds. A Level B tag is not by
itself proof that an external Integration preserved richer state; the
Integration capability gate and its behavioral evidence own that claim.

Every `RunInfo` declares a Backend and its generic capabilities. Native Runs
have a `RunSpec` and daemon-owned child authority. Imported tmux Runs have no
launch spec and expose their import identity across `backend` plus `pid`:
`backend` carries the server/session/window/pane fields, while `RunInfo.pid` is
the pane PID observed at import. That tmux PID is identity evidence, not ctxmux
process authority, and ctxmux never signals it. Imported Runs advertise only
list/status/attach plus `raw_since_import` replay. Input, resize, stop, and both
fork levels fail with `unsupported_capability`; they never fall back to a
native operation.

## Imported tmux panes

One imported Run represents one pane selected at import time. It does not
represent a tmux session, window, or mutable layout. The import fence contains:

```text
explicit socket path
+ server PID and server start time
+ session ID
+ window ID
+ pane ID
+ pane PID
```

The selected server reports `tmux_version`; the local `tmux -V` client version
is a separate compatibility check. Import publishes the Run only after the
Control connection confirms the complete tuple. Pane relocation, relinking,
respawn, death, or server replacement—including replacement that reuses tmux
IDs—fails closed as a target change. Server loss is distinct from target
change.

A linked pane can appear in more than one discovery row with the same pane ID
but different session/window associations. Discovery preserves those public
associations. Generation 3 import accepts only socket path plus pane ID, so it
fails with `target_changed` unless that pair resolves to exactly one complete
tuple; it never chooses an association by row order.

Control Mode remains a public tmux boundary. ctxmux neither speaks the private
tmux client-server protocol nor asks clients to bypass the ctxmux protocol.
Control output is parsed as bounded framing plus octal-escaped byte payloads.
A malformed escape, oversized record, invalid command block, or other
post-readiness transcript corruption interrupts the Run with
`tmux_protocol_error`; it is not relabeled as ordinary server unavailability.
Generation 3 does not claim general command correlation beyond the adapter's
bounded, serial identity and continue probes.

Tmux owns the pane process and PTY throughout. Disconnecting ctxmux clients or
shutting down ctxmux closes only ctxmux-owned Control clients. A read-only CLI
attach still supports local `Ctrl-b d`; ordinary terminal bytes are not sent to
the pane.

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

`RunInfo.durable_head_seq` is `null` in memory-only mode. In persistent mode it
is the highest contiguous output sequence committed by the store actor and may
lag the live `head_seq`. After restart, recovered `head_seq`, replay cursors,
chunks, and truncation describe exactly that committed retained window. A late
attachment receives either one `exited` event or one `interrupted` event after
replay reassembly.

Retained replay is not encoded into one potentially oversized JSON value. The
initial `attached` frame carries replay cursors and `truncated` with no retained
chunks; each retained chunk follows as one ordinary ordered `output` event.
Consequently the 1 MiB limit applies to each frame, while one attachment may
reassemble several MiB of bounded history.

The wire schema makes this distinction explicit: `AttachedHeader` contains an
`OutputReplayHeader` with no `chunks` field. `AttachedSnapshot` and
`OutputReplay` are client API types produced only after ordered reassembly; a
generation-3 peer that puts `chunks` back into the header is invalid.

`Gap { head_seq }` reports where the daemon had advanced when a live receiver
fell behind. It is not a recovery cursor: the caller must reattach using its own
last successfully observed sequence. Replay then returns an exact retained
continuation or sets `truncated` when the required history was evicted.

Imported tmux replay begins at the Control Mode import boundary. The initial
replay is therefore `truncated` even when no retained chunk has been evicted.
ctxmux does not call `capture-pane` to fill that prefix: a screen snapshot is
not raw ordered history. If tmux pauses delivery or the adapter detects another
source-side discontinuity, live attachments receive an explicit gap and later
attachments remain truncated until their cursor is beyond that source gap.

This byte log does not reconstruct the current screen of a full-screen TUI. A
future screen model must be introduced only with an acceptance test that proves
late attachment behavior.

## Lifetime and persistent mode

Without `--state-dir`, Runs outlive CLI and SDK connections but not the daemon
process. With `--state-dir <dedicated-directory>`, one owner-only SQLite store
recovers historical Run identity, exact `RunSpec`, lineage, terminal state, and
the committed bounded replay window across daemon restart.

Persistent startup requires a real same-owner `0700` directory, regular
same-owner `0600` database/WAL/SHM/lock files, and a process-lifetime exclusive
state lock. Exact schema version, SQLite integrity, typed JSON, lifecycle,
lineage, cursor, contiguous chunk, byte-accounting, and quota invariants are
validated before the socket is published. Unknown versions or corrupt state
fail startup; there is no migration, reset, salvage, or partial exposure.

A prior-epoch running record becomes `interrupted { reason: daemon_restart }`
with `pid: null`. Live PTY ownership and child control are not recovered. An old
HUP-ignoring process may remain an orphan, but the replacement daemon never
opens or signals it from persisted metadata.

## Authoritative schema

Rust wire types and error categories live in `crates/ctxmux-protocol`. The Rust
connector lives in `crates/ctxmux-client`.

TypeScript wire declarations under `packages/sdk/src/generated` are generated
from those Rust types with `ts-rs`; they are not maintained as a second schema.
`scripts/generate-protocol-types.sh` refreshes them, and
`scripts/check-protocol-types.sh` generates into a temporary directory and
fails on any checked-in drift. The TypeScript client implements the same hello,
request, attachment, event, and error frames as the Rust client. It also
validates the complete nested generation-3 frame at runtime, rejects duplicate
JSON members and malformed UTF-8, and rejects `u64` cursor values outside
JavaScript's safe-integer range rather than exposing rounded state.
