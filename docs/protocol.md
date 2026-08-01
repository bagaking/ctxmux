# Local Protocol Generation 6

This document describes the currently implemented local daemon boundary. It is
pre-stable: obsolete contracts are replaced directly rather than preserved with
fallbacks or migrations.

## Transport

- Unix domain socket selected explicitly by the daemon operator.
- Socket permissions are set to owner read/write only.
- Each frame is one UTF-8 JSON value followed by a newline.
- A frame may not exceed 1 MiB.
- Raw PTY bytes are represented as integer arrays in generation 6.

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

The generation fence covers the wire contract only. Generation 6 does not yet
negotiate runtime build identity, host identity, or a daemon-wide capability
manifest; those remain separate open work.

After the handshake, a connection has one of two shapes:

1. A short-lived request receives one response or one explicit error.
2. An `attach` request receives one metadata snapshot header, zero or more
   ordered replay `output` events through the advertised replay head, then live
   Run events until detach, disconnect, Run exit, or daemon exit. Rust and
   TypeScript clients reassemble the replay events before returning their
   public attachment snapshot.

Closing a client socket only removes that attachment. It does not stop the Run.

## Run operations

- `start`: create a PTY and spawn the declared command through one required
  creation operation key, then return Run metadata.
- `discover_tmux`: list live panes from one explicit tmux server socket after
  separately validating the client executable and selected server version.
- `import_tmux`: bind one discovered pane identity and publish a read-only,
  memory-only Run observed through public Control Mode.
- `fork`: create a child through one required creation operation key and an
  explicit Level A or Level B plan, then return metadata containing its
  immediate parent and actual fidelity.
- `list`: return all Runs retained by this daemon.
- `status`: return current metadata for one Run.
- `input`: write raw bytes to a live Run's PTY.
- `resize`: request new live PTY rows and columns and report the size read back
  from the owning PTY.
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
ownership in generation 6.

Unknown Runs, invalid dimensions, incompatible protocol versions, failed
process spawns, durable mutation failures, and operations against a terminal
Run are distinct public error categories. Unsupported or invalid behavior never
silently succeeds.

Generation 6 declares `run_capacity` for the global retained-Run admission
boundary accepted by Decision 013. It means no exact eligible replacement can
satisfy projected record or persistent-metadata capacity and must be returned
before native spawn or tmux Control startup. The Registry implementation that
emits this error remains pending under T-027; no current no-GC path may
manufacture it as a substitute for Backend or persistence failure.

Every generation-6 `RunSpec` includes `declared_inputs`, an ordered list of
opaque workspace, artifact, or context references. The daemon records these
references without dereferencing, copying, normalizing, or inferring ownership
from them. Ordinary `start` returns `lineage: null`.

Level A fork resolves the retained parent and clones its complete immutable
`RunSpec`, including `declared_inputs`. Level B requires a current-incarnation
native control owner that is still open, then executes one caller-materialized
`RunSpec` without merging it with or falling back to the parent. A same-epoch
exited or stopping parent and a recovered historical parent therefore reject a
fresh Level B request. Both variants publish the child only after native launch
succeeds. A Level B tag is not by itself proof that an external Integration
preserved richer state; the Integration capability gate and its behavioral
evidence own that claim.

### Retry-safe Run creation

Every `start` and `fork` request carries one caller-owned
`CreateOperationKey`. It is a non-empty opaque UTF-8 string of at most 128
bytes. Equality is byte-exact: ctxmux does not trim, case-fold, parse, or echo
the key in an error. The key is not a `RunId`, Session identity, mutable tag,
owner credential, or attach target.

The daemon compares canonical typed requests after generation-6 decoding and
default application, not raw JSON member order. A canonical Start is its exact
`RunSpec`. A canonical Fork is its parent `RunId` plus exact `ForkPlan`; Level A
therefore compares the parent and `level_a`, while Level B also compares its
materialized `RunSpec`. Ordered arguments and declared inputs remain ordered;
environment member order does not create a different request.

While the resulting Run remains retained, the same key and canonical request
return the current `RunInfo` for that original physical Run. Dynamic state,
output cursors, and attachment counts may have advanced since the first
response. Reusing the key for another Start or Fork returns
`creation_conflict` and creates no child. Lookup precedes current parent and
capability validation, so retrying an already-created Fork still converges
after its parent becomes historical or is no longer retained.

A spawn failure before a child exists does not consume its key. If persistence
rejects after physical launch but before durable `COMMIT`, the child-handle
waiter owns rollback and only `try_wait(Some(_))` proves terminal-and-reaped.
The key is reusable after that proof. Until then the first request reports
`persistence` with an explicit rollback-pending detail, and a bounded
daemon-private cleanup owner retains the unpublished Run plus an exact-key
fence. A matching retry reports `backend_unavailable`; different canonical
reuse reports `creation_conflict`. The fence publishes no Run, retains neither
the random key stripe nor a launch permit, and is reported by bounded shutdown.
It is not a durable tombstone and cannot survive daemon crash.

Durable `COMMIT` is the point of no return. If a post-commit vacuum or
physical-file check fails, persistence is latched and the first request reports
`persistence`, but the daemon still publishes the committed Run and key in its
registry; a same-key retry returns that Run rather than launching another
process. Likewise, failure to deliver a response does not roll back the Run or
mapping. In persistent mode the key is one required, byte-exact unique column
in the same Run row, so commit, recovery, retention eviction, and collection
bind or remove them together. Memory-only mappings last for the retained Run in
the current daemon epoch.

This is bounded retry convergence, not global exactly-once execution. After a
Run and its mapping are collected, the key may create a new Run. A daemon crash
before atomic persistent publication is still outside live process recovery;
ctxmux does not preserve a pending tombstone, adopt an unrecorded process, or
claim that such a process cannot have survived.

### Control correlation and owner receipts

Short-lived `input`, `resize`, and `stop` requests and the corresponding
persistent-attachment commands share one `ControlReceipt` contract. A
successful short-lived operation returns `control_accepted` with current Run
metadata and its receipt. A rejected short-lived control returns
`control_rejected` with a `ControlFailure`; other request classes continue to
use their ordinary response or top-level error frames.

Each attachment `input`, `resize`, or `stop` frame carries an
`AttachmentCommandId`, an unsigned 32-bit integer in `1..=4294967295`. The
first ID is one; each later ID is strictly greater, and gaps are allowed. The
daemon therefore needs to retain only the latest structurally valid ID it
observed per attachment, not an unbounded set. It returns a separate
`command_result` frame carrying the same ID and either an accepted receipt or a
rejected failure. Command results are not Run events: output, gap, Backend
observation, and terminal lifecycle remain on the event stream and cannot
consume a command promise accidentally.

An attachment command ID provides correlation only. It is not an idempotency
key, permission to retry, durable command identity, or deduplication record.
IDs reset when a new attachment connection is created. A non-increasing ID or a
first ID other than one is an attachment-fatal, pre-dispatch protocol violation:
the daemon applies no command for that frame, sends no `command_result` that
could be mistaken for an earlier occurrence of the same ID, and closes the
attachment. Zero is rejected by frame decoding. Once a structurally valid ID is
observed it is consumed before owner admission, so a command rejected for
backpressure, capability, or state cannot reuse that ID. Once the maximum ID is
consumed, the client waits for its pending results, detaches cleanly, and uses a
new attachment for future commands.

For each structurally and sequentially valid command, the daemon sends exactly
one `command_result` or the connection terminates. Reconnecting does not resolve
whether a command whose result was lost took effect. On EOF, transport error,
or fatal protocol violation, the client locally marks every command without its
unique result as disposition unknown and must not replay uncertain input unless
a separate operation-specific deduplication contract is introduced.

Receipts name the precise owner boundary reached:

- `input { written_bytes }` is accepted only when the complete input reached the
  daemon-owned PTY write boundary. The count equals that command's input length;
  a partial write is never an accepted receipt. It does not prove that the child
  read or interpreted those bytes.
- `resize { applied_size }` is the terminal size read back from the owning PTY
  after resize. It lets clients detect and repair requested-versus-applied
  drift rather than treating the requested size as fact.
- `stop` means the direct-child control owner accepted the termination request.
  Final `exited` remains a later lifecycle event and may carry the actual code
  or signal.

Every wire `ControlFailure` carries `not_applied` or `unknown`. `not_applied` means
the command did not cross its mutation boundary. `unknown` means it may have
crossed that boundary, so retry must fail closed at the client unless the
operation has independent idempotency evidence. A partial PTY write, a resize
whose readback fails after mutation, or another daemon-observed ambiguous owner
failure may therefore be rejected as unknown. Transport loss has no wire
failure frame; it produces the separate client-local unknown result described
above.
The `control_backpressure` code reports bounded live-control admission failure
with `not_applied`; input saturation must not be represented as successful
acceptance or allowed to starve resize and stop.

Clients bind each control result to its originating command; attachment results
additionally bind its ID. An unknown or already-completed ID, duplicate result,
receipt-kind mismatch, input count other than the original data length, or zero
applied rows or columns is a daemon contract violation: the client closes the
attachment and marks every unresolved pending command, including the implicated
one, unknown. Ordinary decoded control failures use correlated
`control_rejected` or `command_result { rejected }`; top-level errors are
reserved for handshake, malformed frame, attachment sequencing, and other
pre-dispatch failures.

First-party clients exact-encode a control frame before consuming its
attachment command ID or admitting it to the writer. A local connect or encode
failure is therefore `not_applied`; a transport failure after send begins is
`unknown`. One Attachment permits one pending event-consumer call. After the
single terminal event is delivered, ordinary daemon EOF ends that event stream
cleanly even though any still-unresolved command result remains unknown.

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
associations. Generation 6 import accepts only socket path plus pane ID, so it
fails with `target_changed` unless that pair resolves to exactly one complete
tuple; it never chooses an association by row order.

Control Mode remains a public tmux boundary. ctxmux neither speaks the private
tmux client-server protocol nor asks clients to bypass the ctxmux protocol.
Control output is parsed as bounded framing plus octal-escaped byte payloads.
Empty LF and CRLF records remain records; a true EOF is distinct, and EOF in an
open command block is incomplete framing. Before readiness, malformed or
incomplete framing and invalid or unowned command results reject import with an
explicit Backend error, and no Run is published. After readiness, the same
faults interrupt the imported Run with `tmux_protocol_error`. A true EOF before
readiness rejects import; after readiness it interrupts the Run with
`tmux_server_unavailable`. The adapter admits one pre-session attach bootstrap
result and keeps at most one identity probe plus one continue request pending.
Generation 6 does not claim general tmux command correlation beyond those bounded
serial operations.

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
- future ordered output, Backend observation, gap, and exit events.

The daemon subscribes an attachment before taking its replay snapshot and
deduplicates live events already covered by that snapshot. Before publishing an
exit event, it gives the PTY reader a bounded opportunity to drain the child's
final output.

Attachment command results are multiplexed beside these events but are not
part of replay and are never retained across reconnect. A client that permits
multiple in-flight commands must demultiplex the single inbound stream by
`AttachmentCommandId`; competing socket readers are outside the contract.

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
generation-6 peer that puts `chunks` back into the header is invalid.

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
state lock. Exact schema version, SQLite integrity, typed JSON, a required
native `RunSpec` satisfying the live-start semantic rules, lifecycle, lineage,
cursor, contiguous chunk, byte-accounting, and quota invariants are validated
before the socket is published. Unknown versions or corrupt state fail startup;
there is no migration, reset, salvage, or partial exposure.

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
validates the complete nested generation-6 frame at runtime, rejects duplicate
JSON members and malformed UTF-8, and rejects `u64` cursor values outside
JavaScript's safe-integer range rather than exposing rounded state.
