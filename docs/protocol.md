# Local Protocol Generation 13

This document describes the currently implemented local daemon boundary. It is
pre-stable: obsolete contracts are replaced directly rather than preserved with
fallbacks or migrations.

## Transport

- Unix domain socket selected by the daemon operator, or by the first-party CLI
  default (`$XDG_RUNTIME_DIR/ctxmux/ctxmux.sock`, else a process-temp path).
  `ctxmuxd` still requires `--socket`. The CLI starts a sibling `ctxmuxd` when
  a known command needs the daemon and nothing is listening. The SDK does not.
- Socket permissions are set to owner read/write only.
- Each frame is one UTF-8 JSON value followed by a newline.
- A frame may not exceed 1 MiB.
- Raw PTY bytes are represented as integer arrays in generation 13.

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

The generation fence covers the wire contract only. A successful generation-13
Hello carries exactly one Provider-neutral `RuntimeIdentity`:

```ts
type RuntimeIdentity = {
  daemonInstanceId: string;
  runtimeId: string;
  runtimeIdPersistence: "daemon" | "state_dir";
  buildId: string;
  protocolGeneration: number;
  platform: string;
  arch: string;
  capabilities: Record<string, number>;
};
```

These are the public camelCase wire, generated TypeScript, and CLI JSON names.
Rust uses ordinary snake_case identifiers internally. Missing or extra identity
fields, the obsolete snake_case shape, and the obsolete nested-boolean
capability shape fail closed; there is no alias or fallback.
`protocolGeneration` repeats the exact wire-generation fence; capability
versions are per-key contract versions, not another manifest schema version.

`runtimeId` names the logical Runtime or persistent-store lineage.
`runtimeIdPersistence` states its lifetime:

- `daemon`: a memory-only daemon allocates both `runtimeId` and
  `daemonInstanceId` at startup, so cold replacement changes both;
- `state_dir`: the selected state-directory lineage preserves `runtimeId`
  across cold replacement while the new daemon receives another
  `daemonInstanceId`; another state directory is another Runtime.

A validated planned exec preserves both identities. `runtimeId` is not derived
from the serving epoch and is distinct from a Run ID, daemon incarnation,
build, host, Provider, or credential identity.

`daemonInstanceId` is the live retry and authority fence. It is not a Run,
build, host, platform, Provider, credential, socket, PID, or durable process
identity. A persistent planned exec preserves it together with the complete
settled recoverable-Input ledger and cursor. Ordinary `input` retains the prior
receipt semantics: when its result is lost, callers must not retry it. The
separate `recoverable_input` operation below binds the original daemon instance
and can resolve its own lost response.

`buildId` is an opaque daemon-authored build label. The current implementation
derives it from the package version as `ctxmuxd/<CARGO_PKG_VERSION>`; clients
compare its exact bytes and must not parse that format. It may change when a
planned exec loads another image and is suitable only for equality and
diagnostics. It is not a Git commit, binary hash, signature, source
attestation, host identity, or authorization credential.

`platform` and `arch` are serving-build target facts copied exactly from
`std::env::consts::OS` and `std::env::consts::ARCH`. For example, an Apple
Silicon macOS build reports `macos` and `aarch64`, not the Node/release names
`darwin` and `arm64` and not a target triple. They do not probe or fingerprint
the host running the daemon.

`capabilities` is a flat record. Each value is the highest fully implemented
public contract version for that exact key. Both advertisements and client
requirements accept only numeric JavaScript-safe positive integer values in
`1..=9_007_199_254_740_991`; integer-valued JSON spellings such as `1`, `1.0`,
and `1e0` have the same value. Zero, negative, fractional, boolean, string,
`null`, array, nested-object, and overflow values fail closed. An absent key is
unsupported; neither `0` nor `false` represents absence.

The initial availability record is:

| Exact key                                  | `daemon` | `state_dir` |
| ------------------------------------------ | -------: | ----------: |
| `native.start`                             |        1 |           1 |
| `native.recoverable_input`                 |        1 |           1 |
| `native.recoverable_stop`                  |        1 |           1 |
| `native.fork_level_a`                      |        1 |           1 |
| `native.execute_materialized_level_b`      |        1 |           1 |
| `tmux.discover`                            |        1 |           1 |
| `tmux.import`                              |        1 |      absent |
| `services.persistent_state`                |   absent |           1 |
| `services.planned_exec_upgrade_continuity` |   absent |           1 |

An advertised endpoint capability does not promise that a particular Run,
target, external tmux server, or caller-supplied plan is currently usable.
`RunInfo.capabilities` remains the per-Run Backend truth, and Integration
capabilities remain host-process truth. No layer is inferred from another.

Rust and TypeScript clients may carry one caller-retained exact
`RuntimeIdentity` expectation plus a local exact-key capability requirement
record. Neither precondition crosses the wire or adds a negotiation frame:

```text
connect -> send ClientHello(protocol only) -> validate RuntimeIdentity
        -> compare expected identity -> compare capability requirements
        -> send Request or Attach
```

The identity comparison uses Hello on the same connection that would carry the
business frame. Any field or capability-record mismatch returns a typed local
identity-mismatch error and closes before dispatch, so a separate
`runtime_info`/`runtimeInfo()` preflight is not required and cannot create an
endpoint-replacement race.

An advertised version satisfies a requirement only when it is greater than or
equal to the required version. A missing key or lower version produces the
client-local typed `unsupported_capability` error, closes the connection before
any business frame, and never falls back. Keys are compared byte-exactly: the
clients do not whitelist, normalize, case-fold, map from operations, or infer
them from platform or executable state.

Rust `ping` and `runtime_info`, TypeScript `runtimeInfo`, and CLI
connect-or-spawn readiness remain raw identity inspection paths. “Raw” bypasses
configured identity and capability requirements; framing, the exact identity shape,
and protocol generation are still validated. Generation 13 adds no
version-range or capability negotiation, endpoint discovery, dynamic registry,
Provider catalog, plugin discovery, or host/credential identity.

Changing the confirmed RuntimeIdentity fields or persistence discriminators,
the initial flat keys, the JavaScript-safe positive-integer version semantics,
the Rust target vocabulary, or the client-local pre-dispatch boundary requires
explicit user confirmation and a later reviewed Feature Tracker plan revision
before implementation.

After the handshake, a connection has one of three shapes:

1. A short-lived request receives one response or one explicit error.
2. An `attach` request receives one metadata snapshot header, zero or more
   ordered replay `output` events through the advertised replay head, then live
   Run events until detach, disconnect, Run exit, or daemon exit. Rust and
   TypeScript clients reassemble the replay events before returning their
   public attachment snapshot.
3. An `attach_recoverable_stop` request first resolves the explicitly carried
   Stop operation. Rejection returns one ordinary control response and closes.
   Acceptance receives the metadata header and ordered replay, then the
   ordinary accepted Stop response, followed by the same live or terminal event
   stream as `attach`.

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
- `recoverable_input`: write one non-empty caller-keyed native Input at an
  expected applied-input cursor, or recover its retained exact applied range
  after reconnect within the same daemon incarnation.
- `resize`: request new live PTY rows and columns and report the size read back
  from the owning PTY.
- `signal { signal: interrupt }`: deliver `SIGINT` to the current foreground
  process group. On macOS the retained PTY master uses `TIOCSIG`, so the tty
  driver selects that group without a userspace numeric-PGID check/signal gap.
  Interrupt does not enter Stop or end Run ownership.
- `attach`: return retained output after a cumulative byte cursor and follow new
  output and exit events.
- `stop`: apply or recover one caller-keyed termination of every process in the
  daemon-owned native Run session. The
  waiter normally sends `SIGTERM`, waits for a bounded graceful phase, then
  sends `SIGKILL` to revalidated session members. If Stop is admitted after a
  receive poll but before the natural-exit fence, the waiter drains that queued
  command and reuses the same cleanup and reap proof. Success requires the
  direct child to be reaped and the session to be empty; the receipt reports
  `graceful` or `forced`. A descendant that deliberately enters another session
  leaves the Run ownership boundary and is not claimed by this POSIX session contract.
  Complete-session Stop supports local, same-user, non-elevated processes. The
  waiter keeps the session leader waitable, treats observation and permission
  uncertainty as failure, and performs each `kill` immediately after rechecking
  that numeric PID's SID. POSIX provides no portable incarnation handle for an
  arbitrary descendant, so exit and PID reuse between those two syscalls remain
  a small residual wrong-process TOCTOU; this contract does not claim zero risk.

An unclassified native child-status observation failure is daemon-fatal and
does not create a terminal Run event. The affected Run rejects further live
control with `backend_unavailable`; ctxmux retains the child handle without
signalling it and exits the daemon incarnation instead of polling, fabricating
an exit status, or permitting same-epoch collection. Persistent restart then
applies the ordinary `interrupted { daemon_restart }` reconciliation.

In persistent mode, recovered `exited` and `interrupted { reason:
daemon_restart }` Runs support `list`, `status`, `attach`, and Level A `fork`.
They reject `input`, `resize`, `signal`, `stop`, and Level B `fork` with
`invalid_run_state`; a replacement daemon never turns a stored PID into live
process authority.

Tmux discovery remains available in persistent mode, but tmux import returns
`unsupported_capability`: ctxmux does not persist or recover Control Mode
ownership in generation 13.

Unknown Runs, invalid dimensions, incompatible protocol versions, failed
process spawns, durable mutation failures, and operations against a terminal
Run are distinct public error categories. Unsupported or invalid behavior never
silently succeeds.

Generation 13 retains `run_capacity` for the global retained-Run admission
boundary owned by Decision 013. In memory-only mode it means no exact eligible
terminal replacement can satisfy projected record capacity and is returned
before native spawn or tmux Control startup. In persistent mode it also means
that no exact eligible terminal candidate set can satisfy projected record or
metadata capacity within the admitted SQLite page charge. Candidate Runs,
their replay and byte-exact keys, and the successor Run/key change in one
transaction; Backend or persistence failures remain their own error classes.

Every generation-13 `RunSpec` includes `declared_inputs`, an ordered list of
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

The daemon compares canonical typed requests after generation-13 decoding and
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

A spawn failure before a child exists does not consume its key. The daemon
prepares every fallible PTY reader and writer view before physical launch.
Immediately after launch it constructs native control and arms one private
publication owner before output/wait owner registration can fail or unwind.
Persistence rejection before durable `COMMIT` asks the daemon-wide child owner
to clean up; only the final cleanup-owned `child.wait()` proves child reap, and the key
becomes reusable only after the output, lifecycle, control, input, and Run owners
are also quiescent. Until then the first request reports `persistence` with an
explicit rollback-pending detail, while setup failure or creation-owner unwind
reports its original error, and one bounded daemon-private cleanup owner
retains the unpublished Run plus an exact-key fence. A matching retry reports
`backend_unavailable`; different canonical reuse reports `creation_conflict`.
The fence publishes no Run, retains neither the random key stripe nor a launch
permit, and is reported by bounded shutdown. It is not a durable tombstone and
cannot survive daemon crash.

Durable `COMMIT` is the point of no return. If a post-commit physical-file
check fails, persistence is latched and the first request reports
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

### Recoverable native Input

`RunInfo.applied_input_bytes` is the checked byte cursor owned by a current
native Input writer. It is `null` for tmux and recovered historical Runs. Every
successful native Input path, including legacy short-lived and attachment
commands, advances this one cursor after complete `write_all` plus flush.

A recoverable request carries the Hello's original daemon instance, one
per-Run `InputOperationKey`, the Run, exact non-empty bytes, and the cursor
expected immediately before the write. Instance comparison precedes Run lookup.
The FIFO writer checks the cursor immediately before mutation, so two unique
operations cannot both consume the same range. Success returns
`input_applied { run, range: { start_byte, end_byte } }`. The returned Run is
the requested Run and its current cursor is at least `end_byte`; later FIFO
Input may have advanced it after this operation completed.

While an operation is pending or retained, an exact retry joins or returns the
same result; another payload or expected cursor with that key returns
`input_operation_conflict`. The Run-local ledger retains at most 256 entries and
1 MiB of request bytes. It is incarnation-local, not connection-local. A
planned exec-in-place upgrade carries its complete completed/unknown entries,
poisoned-lane state, and applied cursor in the validated handoff manifest; a
cold replacement does not. It evicts only completed results. Clients use a
fresh key for every new logical operation; an exact retry after eviction still
has an old expected cursor and returns `input_cursor_mismatch` without mutation.

Partial write, flush failure, or writer panic returns an `unknown` failure,
retains that keyed unknown result, and poisons the Input lane. No applied range
or cursor advance is invented. A cold replacement daemon returns
`daemon_instance_mismatch`; ctxmux never claims cross-crash exactly-once Input.

### Recoverable native Stop

Every native Stop carries one caller-retained operation:

```ts
type RecoverableStop = {
  daemon_instance: string;
  operation_key: string;
  id: string;
};
```

The caller first retains the current Hello's `daemonInstanceId`, one fresh
`StopOperationKey`, and the exact `RunId`. The key is an opaque, byte-exact,
non-empty UTF-8 string of at most 128 bytes. Instance validation precedes Run
lookup and mutation. A request naming another daemon incarnation returns
`daemon_instance_mismatch`; it never probes or stops the named Run.

The first admitted operation atomically binds its key and Run before native
Stop mutation. Concurrent or later retries with the same key and Run join the
in-flight result or replay the settled `stop { disposition }` receipt. This
works across short connections, attachment connections, response loss, and a
fresh client. The attachment command ID remains correlation-only; recovery
comes from resending the complete retained Stop operation.

An ordinary `attach` request remains observation-only. A terminal Run sends
its retained replay, exactly one terminal event, and EOF without waiting for a
possible later command. A caller that needs both fresh-client Stop recovery and
an attachment uses `attach_recoverable_stop`, carrying the complete retained
operation in the initial request. The daemon resolves that operation through
the same ledger while pinning its exact Run, then sends the attachment snapshot
and replay, the ordinary `control_accepted` Stop receipt, and the normal live or
terminal event stream. This explicit composite avoids both a lost-wakeup window
and an indefinitely retained terminal attachment.

A different key for a Run that already owns a Stop operation, or reuse of the
same key for another retained Run, returns `stop_operation_conflict` before
mutation. The Runtime retains at most one Stop record per retained Run and one
global key binding for that record. A `not_applied` owner result releases the
record; an accepted or `unknown` result remains replayable until that exact Run
is collected. Collection cannot cross an in-flight settlement and atomically
removes both the Run record and key binding; only after that boundary may the
same key identify a Stop for another Run.

A validated planned exec carries settled accepted and unknown Stop records
with the preserved daemon incarnation. Pending Stop settlement prevents the
reversible handoff from proceeding. A cold daemon replacement receives a new
incarnation and deliberately does not recover this ledger, so an old retained
operation fails at the instance fence. This is the narrow
`native.recoverable_stop: 1` contract, not a generic mutation framework,
cross-crash process adoption, or authorization mechanism.

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
one `command_result` or the connection terminates. Reconnecting with only an
attachment command ID does not resolve whether a command whose result was lost
took effect. On EOF, transport error, or fatal protocol violation, the client
locally marks every command without its unique result as disposition unknown.
Uncertain ordinary Input, Resize, and Signal must not be replayed. Recoverable
Input and Recoverable Stop are separate, narrow operation contracts: their
complete caller-retained operations may be retried as specified above, but
their keys do not change attachment command-ID semantics or generalize recovery
to Resize or Signal.

Recoverable Input's per-Run key conflict guarantee lasts only while the
operation is pending or retained in its bounded result ledger; callers use a
fresh key for new logical operations. An evicted exact retry remains safe
because its original expected cursor is stale and fails before mutation.

Receipts name the precise owner boundary reached:

- `input { written_bytes }` is accepted only when the complete input reached the
  daemon-owned PTY write boundary. The count equals that command's input length;
  a partial write is never an accepted receipt. It does not prove that the child
  read or interpreted those bytes.
- `resize { applied_size }` is the terminal size read back from the owning PTY
  after resize. It lets clients detect and repair requested-versus-applied
  drift rather than treating the requested size as fact.
- `signal { signal }` proves the daemon's retained native PTY owner delivered
  the requested portable signal. On macOS the kernel selected the PTY's current
  foreground process group at the `TIOCSIG` mutation boundary.
- `stop { disposition }` proves the waiter reaped the direct child and observed
  the complete owned session empty. `graceful` means no forced phase was needed,
  including an owner-ordered natural exit after Stop admission; `forced` means
  at least one session member required `SIGKILL`. Public
  `exited` publication remains a later lifecycle event, so the returned
  `RunInfo` can still say `running` while no owned process remains.

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
associations. Generation 8 import accepts only socket path plus pane ID, so it
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
Generation 8 does not claim general tmux command correlation beyond those bounded
serial operations.

Tmux owns the pane process and PTY throughout. Disconnecting ctxmux clients or
shutting down ctxmux closes only ctxmux-owned Control clients. A read-only CLI
attach still supports local `Ctrl-b d`; ordinary terminal bytes are not sent to
the pane.

## Output and reconnect

PTY output is divided into contiguous half-open cumulative byte ranges. The daemon currently
retains at most 4 MiB per Run. An attachment supplies its last observed byte cursor
and receives:

- retained bytes after that cursor, slicing the first range when it falls inside a retained chunk;
- the first retained byte and total output bytes allocated;
- a `truncated` flag when required output was already evicted;
- future ordered output, Backend observation, raw-output gap, explicit
  observation discontinuity, and exit events.

The daemon subscribes an attachment before taking its replay snapshot and
deduplicates live events already covered by that snapshot. Before publishing an
exit event, it gives the PTY reader a bounded opportunity to drain the child's
final output.

Attachment command results are multiplexed beside these events but are not
part of replay and are never retained across reconnect. A client that permits
multiple in-flight commands must demultiplex the single inbound stream by
`AttachmentCommandId`; competing socket readers are outside the contract.

`RunInfo.durable_output_bytes` is `null` in memory-only mode. In persistent mode it
is the highest contiguous output byte committed by the store actor and may
lag the live `latest_output_bytes`. After restart, recovered `latest_output_bytes`, replay cursors,
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
generation-13 peer that puts `chunks` back into the header is invalid.

`Gap { latest_output_bytes }` reports raw-output delivery discontinuity only.
It is not a recovery cursor: the caller must reattach using its own last
successfully observed byte cursor. Replay then returns an exact retained
continuation or sets `truncated` when the required history was evicted or the
source itself could not provide a continuous raw stream.

`ObservationDiscontinuity` is a separate, cursor-free fail-closed marker: one
or more non-output observations did not reach this attachment, and byte replay
cannot reconstruct their meaning. The daemon's private live-event stamp
distinguishes output-only broadcast lag from lag that crossed tmux observations;
it is delivery metadata, not a durable journal or a public event sequence. The
daemon ends that attachment after the marker (or after one authoritative
terminal event when the Run is already terminal). A new attachment establishes
a new observation boundary; it does not claim to recover prior tmux events.
First-party clients retain this marker as a non-output event and close rather
than silently dropping it when their bounded local queue cannot represent it.

Imported tmux replay begins at the Control Mode import boundary. The initial
replay is therefore `truncated` even when no retained chunk has been evicted.
ctxmux does not call `capture-pane` to fill that prefix: a screen snapshot is
not raw ordered history. If tmux pauses delivery or the adapter detects another
source-side discontinuity, live attachments receive an output `Gap` and later
attachments remain output-truncated until their cursor is beyond that source
gap. `Paused` and `Continued` remain ordinary live observations when they are
delivered. Only attachment delivery that cannot preserve one of them—through
broadcast lag or a terminal subscribe/snapshot join—produces
`ObservationDiscontinuity`; no sticky Backend state or observation replay is
synthesized for a later attachment.

This byte log does not reconstruct the current screen of a full-screen TUI.
Interactive `ctxmux attach` reconstructs a client view from retained bytes and
paints one still frame; the protocol and non-interactive attach remain raw.

## Lifetime and persistent mode

An embedding parent that starts `ctxmuxd` may add `--readiness-fd <fd>`. The
descriptor must be an inherited non-standard descriptor distinct from the
private qualification descriptor. ctxmux duplicates it with close-on-exec and,
only after the Unix listener is bound, mode `0600` is applied, and the socket
guard is armed, writes exactly one NDJSON record:

```json
{ "schema": "ctxmux.daemon-ready.v1", "daemon_instance": "<uuid>" }
```

The parent accepts bootstrap only when that instance equals the
`runtime.daemonInstanceId` in the ordinary generation-13 public Hello from the selected
socket. EOF, invalid JSON, a different instance, a closed descriptor, or a
receipt write failure fails bootstrap; a requested write failure also removes
the unpublished socket. The inherited channel proves which spawned child
published readiness. A socket response, PID, delay, filesystem receipt, binary
path, or matching protocol version does not. This is activation provenance,
not Run state, discovery, persistence, cross-user authentication, or a new wire
generation. Without the flag daemon startup is unchanged.

Without `--state-dir`, Runs outlive CLI and SDK connections but not the daemon
process, and `SIGHUP` is a logged no-op. With `--state-dir
<dedicated-directory>`, one owner-only SQLite store recovers historical Run
identity, exact `RunSpec`, lineage, terminal state, and the committed bounded
replay window across cold daemon restart. On intentional `SIGHUP`, the daemon
instead performs an exec-in-place upgrade: its PID, listener inode, state lock,
live child and PTY masters, Runtime ID, daemon instance, input cursors and
settled ledgers survive; attachments reconnect from their own output cursors.
The incoming image reconstructs its build ID, `platform`, `arch`, and advertised
capability record from the new image and active persistence mode; they are not
handoff authority or attestation.

Before extraction, the daemon's upgrade request gate is reversible. Requests
already admitted retain their permit through owner completion and response
write, while later attachment commands receive an explicit retryable
`backend_unavailable` result with `not_applied`. Drain timeout, handoff-file
setup failure, or all-owner preflight failure restores normal admission. After
extraction, ownership has been relinquished to the pending exec and any error is
fail-stop. The version-2 handoff manifest and every carried descriptor are
strictly bounded, unique, and validated; generation 13 gains no upgrade wire
operation.

Persistent startup requires a real same-owner `0700` directory, regular
same-owner `0600` database/WAL/SHM/lock files, and a process-lifetime exclusive
state lock. Exact schema version, SQLite integrity, typed JSON, a required
native `RunSpec` satisfying the live-start semantic rules, lifecycle, lineage,
cursor, contiguous chunk, byte-accounting, and quota invariants are validated
against the schema-4 format envelope before the socket is published. Schema 4
stores the Runtime UUID in `runtime_meta`; bounded, restartable startup
transactions reconcile prior running rows, evict the canonical terminal prefix
to the operational 128-record ceiling, and finish serving-epoch publication before
the socket becomes visible. Unknown versions, corrupt state, or an
individually unprovable normalization unit fail startup; there is no migration,
reset, salvage, or partial exposure.

A prior-epoch running record not accompanied by the validated live handoff set
becomes `interrupted { reason: daemon_restart }` with `pid: null`. A cold
replacement never recovers live PTY ownership or child control and never opens
or signals an old HUP-ignoring process from persisted metadata. The planned
exec path is different: the same parent process carries actual master
descriptors and wait authority; it does not adopt a PID guessed from SQLite.

## Authoritative schema

Rust wire types and error categories live in `crates/ctxmux-protocol`. The Rust
connector lives in `crates/ctxmux-client`.

TypeScript wire declarations under `packages/sdk/src/generated` are generated
from those Rust types with `ts-rs`; they are not maintained as a second schema.
`scripts/generate-protocol-types.sh` refreshes them, and
`scripts/check-protocol-types.sh` generates into a temporary directory and
fails on any checked-in drift. The TypeScript client implements the same hello,
request, attachment, event, and error frames as the Rust client. It also
validates the complete nested generation-13 frame at runtime, rejects duplicate
JSON members and malformed UTF-8, and rejects `u64` cursor values outside
JavaScript's safe-integer range rather than exposing rounded state.
