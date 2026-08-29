# Architecture

ctxmux makes a Run durable by keeping its runtime ownership in one local daemon. Terminals, CLIs, SDKs, editors, and automations are replaceable views over that Run.

This page is the architecture entrypoint. It distinguishes shipped behavior from target design, follows the important end-to-end paths, and links every critical technical decision to its own record.

## Current guarantees and target boundaries

Current guarantees are deliberately narrower than the product vision.

| Area             | Current                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        | Target or open                                                                                                                   |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------- |
| Run lifetime     | A native child survives client disconnects. Optional `--state-dir` mode recovers historical Run state and committed replay after cold restart and preserves live PTY control across a planned exec-in-place `SIGHUP` upgrade. Existing attachments reconnect.                                                                                                                                                                                                                                                                                                                                                                  | Crash-time PTY adoption and host-reboot process continuity remain unsupported.                                                   |
| Transport        | Versioned NDJSON over a Unix socket. The CLI uses `$XDG_RUNTIME_DIR/ctxmux/ctxmux.sock` (else a process-temp path) and starts `ctxmuxd` when nothing is listening; other clients still select the socket explicitly.                                                                                                                                                                                                                                                                                                                                                                                                           | Windows transport and multi-daemon discovery are open.                                                                           |
| Clients          | Rust CLI and dependency-free TypeScript SDK share protocol generation 14, including strict padded base64 PTY output on the wire (decoded once to `Uint8Array` in the SDK), a daemon-authored RuntimeIdentity, daemon-incarnation fencing, recoverable native Input and Stop, foreground-group Interrupt, correlated attachment controls, typed owner receipts, explicit non-output observation discontinuity, and the shared memory-only/persistent retained-Run capacity boundary. Public Rust and TypeScript clients may additionally enforce local capability requirements; CLI readiness remains raw and requirement-free. | Other SDKs appear only for a real client requirement.                                                                            |
| Attach           | Retained raw bytes plus ordered live events; interactive CLI reconstructs the current screen, then follows live bytes with raw mode and `Ctrl-b d`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                            | Multi-writer policy remains open.                                                                                                |
| Input recovery   | A native operation adds same-incarnation retry, exact applied-input byte ranges, a bounded Run-local result ledger, and a daemon-instance fence. The cursor and complete settled ledger cross a planned exec-in-place upgrade with the preserved instance. Attachment command IDs remain connection-local; ordinary Input result loss remains unknown.                                                                                                                                                                                                                                                                         | Cold-restart exactly-once and semantic acknowledgement remain above or outside ctxmux.                                           |
| Stop recovery    | One caller-retained operation joins or replays the complete-session Stop receipt across connection loss. A Runtime-global key binding and one per-Run record fence conflicts before mutation, survive planned exec, and end at exact Run collection.                                                                                                                                                                                                                                                                                                                                                                           | Cold-restart exactly-once and recoverable Resize/Interrupt remain unsupported.                                                   |
| Backends         | Native `portable-pty`; an implemented read-only public-Control-Mode tmux pane adapter with required version-lane qualification pending. An owner-host Runtime is reached by forwarding the same socket over the caller's system OpenSSH, so it is an endpoint rather than a third Backend; its minimal vertical, its probeable client-side endpoint contract, and its partition and mixed-capability qualification against a real SSH boundary are delivered.                                                                                                                                                                  | Wider tmux control and other Backends require separate evidence.                                                                 |
| Integrations     | Explicit Provider-neutral Integration binding and a shell Integration are part of the Runtime embedding surface.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | Provider-specific sessions, semantic replay, native resume, working-state, permission, and A2A policy live in embedding clients. |
| Context and fork | Level A clones a declared `RunSpec`; the Runtime executes an explicitly caller-materialized Level B `RunSpec` and records lineage and the executed plan class. Missing Level B provenance fails closed without Level A fallback.                                                                                                                                                                                                                                                                                                                                                                                               | Provider-neutral derivation metadata remains open; provider session recovery is outside the Runtime.                             |
| Runtime identity | Every compatible Hello reports the exact camelCase RuntimeIdentity: logical Runtime ID and persistence class, daemon-instance retry fence, serving build and Rust target facts, exact protocol generation, and a flat numeric capability record. Persistent cold replacement preserves only the Runtime ID; planned exec preserves Runtime and daemon identities.                                                                                                                                                                                                                                                              | Host identity, credentials, Provider identity, endpoint discovery, and capability negotiation are outside this contract.         |
| Persistence      | Optional `--state-dir` mode recovers historical metadata, lineage, terminal state, committed replay, and Runtime identity and supports planned exec-in-place live continuity; default mode remains memory-only.                                                                                                                                                                                                                                                                                                                                                                                                                | Schema migration and online history management are open.                                                                         |

“Durable” always includes client churn. With persistent mode it additionally
includes the declared historical recovery class across cold daemon restart and
live ownership continuity across a planned exec-in-place upgrade. It does not
include crash-time process adoption, host-reboot process continuity, or schema
migration.

## System and ownership model

### System boundary

The daemon is the only process that owns ctxmux Run identity, replay, and
attachment state. It owns native children directly; an imported tmux Run
references runtime state that tmux continues to own.

```text
CLI                  TypeScript host              future editor / automation
 |                         |                                  |
 +----------- public versioned protocol / SDK ----------------+
                              |
                    Unix domain socket (v13)
                              |
                    long-lived ctxmux daemon
                    - RunManager / RunRegistry / Run identity
                    - PTY / child / input writer
                    - lifecycle / output / replay
                    - attachment event delivery
                              |
                    native Backend ---------------- ctxmux-owned PTY child
                    tmux pane adapter ------------- tmux-owned server / pane
                    (required version-lane qualification pending)
```

A client may create, observe, control, or stop a Run. Socket closure removes one attachment; it never means “stop the Run.” This boundary keeps editor restarts, CLI exits, and Integration-host exits from becoming accidental process supervisors.

### Runtime domain model

`RuntimeIdentity` is daemon-authored endpoint and serving-build truth, not Run
metadata. It separates the logical Runtime/store lineage and its explicit
`daemon` or `state_dir` persistence class, current daemon incarnation, opaque
build label, Rust target OS and architecture, exact protocol generation, and a
flat version-number capability record. Persistent cold replacement keeps the
Runtime ID and changes the daemon instance; memory-only cold replacement
changes both; validated planned exec keeps both while build facts may change.
Build facts are diagnostics, not source, binary, host, or credential
attestation.

The daemon advertises only fully implemented endpoint capabilities. These do
not override `RunInfo.capabilities`, current Run state, target identity,
caller-supplied plan validity, external tmux availability, or Integration
capabilities. The exact catalog, mode availability, and numeric domain have one
public owner in [the protocol contract](protocol.md#connection-state).

Runtime identity expectations and capability requirements are client-local
policy. The Rust and TypeScript clients validate Hello on the same connection,
compare the complete caller-retained identity followed by exact key/version
pairs, and only then send a business Request or Attach frame. A mismatch sends
no business frame. The daemon owns no capability negotiation; raw identity and
readiness paths remain available for diagnostics. This is typed endpoint
inspection, not Provider discovery or a dynamic registry.

### Run domain model

`RunSpec` is the launch contract for native Runs: program, arguments, optional
working directory, selected environment additions, initial terminal size, and
ordered opaque workspace, artifact, or context references. An imported tmux
Run has no launch spec because tmux already owns its pane and process.
`RunInfo` exposes identity, immediate parent and actual fork fidelity when
present, Backend identity and capabilities, PID when available, lifecycle
state, retained-output cursors, optional committed `durable_output_bytes`, and
attachment count. For an imported tmux Run, `RunInfo.pid` is the pane PID
observed at import and participates in the target fence; it is not daemon-owned
process authority and ctxmux never signals it.

The implemented lifecycle has three observable states:

```text
start accepted
     |
  running -- owned session becomes empty --> exited(code, signal?)
     |
     +-- interrupt foreground group --> running
     +-- stop quiesces session --> exited(code, signal?)
     |
     +-- later daemon epoch --> interrupted(daemon_restart)
```

`portable-pty` establishes each native child as a POSIX session leader before
exec. The waiter retains the actual child handle and owns that session identity.
It observes terminal state with non-reaping `waitid`; the waitable leader remains
an incarnation anchor until every descendant is gone, and only then is reaped.
On macOS, `interrupt` asks the retained PTY master to generate `SIGINT`; the tty
driver selects its current foreground process group atomically without changing
the Run phase. `stop` fences later controls, sends `SIGTERM` to revalidated
session members, escalates remaining members to `SIGKILL`, reaps the direct
child, and returns only after the session is empty. Its receipt names whether
the graceful or forced phase completed cleanup. Acknowledgement can still
precede public `Exited` publication, so returned `RunInfo` may say `running`
while the owned process scope is already quiescent. A descendant that creates a
new session deliberately crosses this POSIX ownership boundary. In persistent
mode a new daemon epoch converts prior `running` rows to `interrupted {
daemon_restart }`, clears their PID, and exposes no live control. Terminal Runs
remain retained until admission reaches the 128-record ceiling, then the
Registry fences exact fully quiescent candidates; persistent COMMIT removes the
same Runs, replay, and byte-exact keys before publishing the successor.

If native non-reaping status observation fails before yielding a child status, ctxmux does not
pretend the Run exited. It stops polling, transfers the real child handle into
an irreversible native fail-stop owner, closes that Run's live controls, fences
new physical launches, and fails the serving daemon incarnation; `ctxmuxd`
then exits. The Run remains running-but-uncontrollable and ineligible for
same-epoch collection; persistent restart uses the existing `daemon_restart`
reconciliation.

### Ownership split

| Owner        | Responsibilities                                                                                                                                                                                                     |
| ------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Runtime core | Run identity and capabilities, native launch and PTY ownership, tmux observation clients, ordered raw output, attachment and reconnect behavior, public errors, and the persistence required by declared guarantees. |
| tmux         | Imported server, session, window, pane, PTY, process, layout, persistence, and their native lifecycle.                                                                                                               |
| Integration  | Provider-neutral detection, structured launch or RunRecipe materialization, capability declaration, and optional host-local observation. Provider session and resume semantics belong to a higher client.            |
| Client       | Terminal rendering, editing UI, user workflow, multi-Run composition, Agent scheduling and evaluation, and Crucible or MapReduce policy.                                                                             |

## Components and stable boundaries

Each package has one reason to change.

| Component         | Responsibility                                                                                                                                   | Must not own                                        |
| ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------- |
| `ctxmux-protocol` | Rust wire types, RuntimeIdentity shape and capability vocabulary, generation constant, frame limit, serialization, and TypeScript export.        | Live processes or client policy.                    |
| `ctxmux-client`   | Rust connector, client-local Runtime identity/capability requirements, request lifecycle, attachment connection, and typed public errors.        | Daemon state.                                       |
| `ctxmux-daemon`   | RuntimeIdentity advertisement, Unix listener, Run manager, PTYs, children, replay, events, and socket lifecycle.                                 | Agent-specific semantics or UI.                     |
| `ctxmux`          | Human CLI, raw terminal mode, resize forwarding, detach prefix, and connect-or-spawn of `ctxmuxd`.                                               | Direct access to daemon internals.                  |
| `ctxmux-remote`   | One supervised system-OpenSSH `StreamLocal` forward to an owner-host daemon socket, its caller-private local socket, and their bounded cleanup.  | Protocol bytes, identity proof, or lifecycle truth. |
| `@ctxmux/sdk`     | Node connector, client-local Runtime identity/capability requirements, request and attachment APIs, and explicit host-local Integration binding. | Electron, React, an editor, or Run ownership.       |

### Stable boundary

The stable product boundary is the local protocol, not a Rust ABI or Node
native addon. Rust and TypeScript clients can evolve independently while
exercising the same daemon path. The CLI remains a consumer of the public Rust
client rather than a daemon-internal shortcut.

### Standalone runtime boundary

The daemon, CLI, protocol, and SDK form one independently releasable local
Runtime. Their acceptance cannot depend on an editor, Agent harness, provider
CLI, or another repository. A clean installation must be able to activate or
connect to a compatible daemon, start an arbitrary command, detach and
reattach, apply input and resize, replay ordered output or report a gap, wait
for lifecycle change, stop the owned process scope, and inspect the runtime and
Run identity exposed by the current protocol.

Authoritative Run revisions and timestamps, lost-wakeup-safe wait helpers, and
an embeddable activation helper belong to this boundary. Until the
current-guarantees table marks them shipped, they remain target capabilities
rather than implied behavior. Runtime identity and typed capability reporting
are already part of the shipped public boundary above.

The dependency direction is one-way:

```text
standalone CLI          SDK host          editor / Agent product
      |                    |                       |
      +---------- public protocol / SDK ----------+
                              |
                           ctxmuxd
                              |
                    PTY / process / replay
```

`ctxmuxd` never imports or calls an embedding product. A Run remains operable
when an Integration host, editor, or Agent client exits.

Use this ownership test for new capabilities:

> A fact belongs to ctxmux when it is valid for shells, servers, tests,
> benchmarks, scripts, and Agents, and only the PTY, process, Backend, or daemon
> owner can prove it. A claim belongs to the embedding product when it requires
> understanding a Provider, Agent session, permission, message, task, workspace
> policy, or UI.

| ctxmux owns                                                                                       | Embedding products own                                                                                |
| ------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| Runtime endpoint and daemon-incarnation identity                                                  | Provider installation, detection, configuration, and catalog                                          |
| Run identity, lifecycle, timestamps, revision, and owner receipts                                 | Agent session identity, provider-native identity, and semantic resume policy                          |
| PTY/process ownership, ordered bytes, replay, gaps, input, resize, interrupt, and stop            | Prompt readiness, message parsing, permissions, tool calls, Agent status, and evidence interpretation |
| Persistence, retention, attachment, reconnect, and transport reachability                         | Worktree/context selection, product artifacts, scheduling, collaboration, and UI                      |
| Generic `RunSpec`, declared references, lineage, and execution of an explicit fork or resume plan | Provider-specific provenance derivation and materialization of native launch, fork, or resume plans   |
| CLI and SDK activation of a compatible daemon                                                     | Product packaging, account/authentication policy, and product endpoint selection                      |

Runtime evidence must not be promoted into a semantic claim by naming alone.

| Runtime can prove                                   | Runtime cannot prove                                           |
| --------------------------------------------------- | -------------------------------------------------------------- |
| bytes crossed the PTY write boundary                | the target interpreted them as a submitted prompt              |
| ordered output bytes were observed                  | an output fragment is a complete Agent message                 |
| the owned process scope exited or was interrupted   | the Agent completed its task successfully                      |
| one declared Run plan derived from another          | a provider conversation retained its meaning or hidden context |
| an endpoint is reachable, replaced, or unverifiable | an unreachable remote Run has exited                           |

Agent semantic events may cite ctxmux byte ranges, Run revisions, and lineage as
evidence. Their interpretation and settlement remain owned by the client that
understands the Agent.

## Core scenarios and end-to-end paths

The key paths converge in the daemon rather than duplicating runtime logic in each client.

### Start a native Run

1. The CLI ensures a daemon is listening (reusing one, or starting sibling
   `ctxmuxd` at the requested or default socket), then the CLI or SDK
   constructs a `RunSpec`, retains or generates one bounded creation operation
   key, and opens a Unix-socket connection.
2. The connection exchanges an exact protocol-generation handshake.
3. `Request::Start` reaches the daemon-private creation owner. A fixed,
   asynchronously acquired key stripe serializes only possible key collisions;
   a retained matching mapping returns the original Run before launch.
4. Only an unbound leader waits on a Tokio semaphore admitting at most eight
   simultaneous physical launches. The 64 key stripes bound collision state;
   they are not a 64-thread launch limit. Cancellation while awaiting admission
   releases the key stripe and creates no flight or thread.
5. An admitted leader claims a creation flight and reserves one of the same
   eight private rollback-owner slots. In memory-only mode it also preallocates
   the new `RunId` and reserves one Registry publication; at capacity that
   reservation fences and compacts one exact quiescent terminal candidate or
   returns `run_capacity`. It then starts one named, short-lived OS thread that
   owns the semaphore permit, key stripe, request, manager, both reservations,
   and flight guard through launch and publication. Existing cleanup fences can
   exhaust rollback ownership and reject here before spawn. The result returns
   over a Tokio one-shot channel; cancellation after dispatch drops only the
   receiver, so it cannot release the key or abandon a physical launch. No
   persistent blocking-worker pool or custom execution queue is retained for
   this path.
6. The daemon validates the spec, opens a PTY, prepares every fallible reader
   and writer view, and only then spawns the child. It constructs one private
   native-control facade and publication owner before starting the waiter and
   blocking output-reader workers, then transfers the owned child handle to the
   waiter behind one narrow stop-command channel.
7. Native creation therefore has no post-spawn, pre-owner fallible setup
   boundary. In persistent mode the single store actor commits the complete
   running row and byte-exact operation key in one transaction. Failure before
   `COMMIT` requests cleanup from the waiter that exclusively owns the child
   handle. Child
   terminal-and-reaped is necessary but does not reopen the key: reader,
   waiter, control, input, and Run owners must also be quiescent. Otherwise one
   daemon-private, globally eight-slot-bounded cleanup owner retains the
   unpublished Run and an exact-key fence without retaining its random stripe
   or launch permit. The same transfer covers worker-setup failure and creation
   owner unwind. Successful `COMMIT` is the point of no return: even if a
   physical-file postcheck then latches persistence, the manager binds
   persistence and stores `Arc<Run>` plus the key mapping under one
   `RunRegistry` write before returning that error. A retry therefore resolves
   the committed Run instead of spawning again. Closing the request connection
   cannot drop the Run or roll back a published mapping.

### Import a tmux-owned pane

1. A public client selects one explicit tmux socket and discovers live panes.
   Import first acquires the same eight-wide physical-publication flight and
   cleanup owner used by native creation, then preallocates its `RunId` and
   reserves memory-only Registry capacity before starting a Control Mode child.
   A failed import releases the slot only after Control child, reader, waiter,
   and writer cleanup completes; otherwise bounded shutdown reports the
   transferred cleanup owner.
2. The daemon separately validates the tmux client executable and the selected
   server version, then starts a public read-only Control Mode client.
3. Before publication, the Control connection binds the complete import tuple:
   socket path, server PID/start time, session ID, window ID, pane ID, and pane
   PID. The Run represents that pane at that identity; it does not represent a
   whole session or mutable layout.
   If tmux links expose the same pane ID through multiple session/window rows,
   import fails as ambiguous rather than choosing one by row order.
4. tmux keeps ownership of the server, PTY, pane process, layout, and
   persistence. ctxmux owns only its Control client, retained post-import raw
   bytes, and attachment fan-out.
5. Relocation, respawn, pane death, server replacement, or Control transcript
   corruption interrupts the Run explicitly instead of silently following or
   reclassifying the target.
6. Imported Runs are memory-only and read-only. Persistent import, input,
   resize, stop, and fork fail as unsupported capabilities.

### Attach, disconnect, and reattach

1. An attachment uses a dedicated long-lived connection and names `after_byte`.
2. The daemon subscribes to live events before taking the replay snapshot. This closes the replay/live race.
3. The daemon sends an `Attached` header containing `RunInfo`, replay cursors,
   and `truncated`, then streams each retained chunk as an ordered
   `RunEvent::Output` frame. This keeps every encoded frame below the 1 MiB
   transport ceiling even when total retained output is much larger.
4. Rust and TypeScript clients reassemble those bounded frames before returning
   the public snapshot. Live chunks already covered by that snapshot are
   deduplicated by cumulative byte range.
   If terminal state becomes authoritative in the subscribe/snapshot join
   window after an unreplayable tmux observation, the attachment emits
   `ObservationDiscontinuity` before its single snapshot-derived terminal event.
5. Clean detach returns `Detached`; abrupt socket closure drops the attachment guard. Both leave the Run in `RunManager`.
6. A later attachment resumes from its last observed byte cursor or detects that retained output was evicted.

### Planned exec-in-place upgrade

1. Persistent-mode `SIGHUP` creates an owner-only, immediately unlinked handoff
   file and resolves the current executable before changing daemon admission.
2. One reversible request gate changes `Open -> Draining`. Already-admitted
   requests retain a permit through owner completion and response write;
   commands arriving on an existing attachment receive an explicit
   `backend_unavailable` / `not_applied` retry result. New mutations cannot
   cross the extraction boundary.
3. After the active count reaches zero, the daemon preflights every native Run
   together. Every lifecycle must still be watching and every input, child
   command, cursor, and bounded operation-ledger snapshot must be complete. Any
   failure here drops the fence and restores full service before ownership is
   relinquished.
4. Extraction is the point of no return. The old native owner stops its reader,
   relinquishes exactly one child/control owner per Run, flushes the durable
   output barrier, writes the version-2 manifest, clears close-on-exec only on
   the manifest, listener, state lock, and PTY masters, then calls `execve`.
5. The incoming image reuses the Runtime ID, daemon instance, listener and state
   lock, reconciles only non-handed-off running rows, re-adopts the live masters
   and child wait authority, and restores each input cursor, complete settled
   Input ledger, and settled recoverable Stop records. It rebuilds build identity
   and the advertised capability record from the incoming image and active
   persistence mode. Existing attachment transports end; clients reconnect from
   their own output cursor. Failure after extraction is fail-stop, never a partial
   return to service.

### Input, resize, output, and exit

Short-lived operations and attached frames use the same daemon-private native
control owner. Attachment `input`, `resize`, and `stop` frames carry a
connection-local command ID and receive a separate correlated result rather
than a Run event. Input acceptance means the complete payload crossed the PTY
write boundary; resize acceptance reports `resize -> get_size` readback from
the owning PTY; stop acceptance remains distinct from the later `Exited`
event. A lost command-ID-only result has unknown disposition and is never
permission to replay an ordinary Input, Resize, Signal, or Stop operation.
Attachment command IDs remain correlation-only. A caller can recover an exact
retained Stop result only by retrying its complete Recoverable Stop operation
through Decision 017.

Decision 014 defines a separate recoverable short-lived Input operation. It
binds one caller key to the exact Run, daemon incarnation, non-empty byte
payload, and expected input byte cursor. A matching same-incarnation retry
return the original applied byte range without another PTY write; conflicting,
stale-cursor, evicted, or replacement-incarnation attempts fail closed. The
operation ledger is Run-local and bounded. It does not replace attachment
command correlation, make Resize or Signal recoverable, or claim that the child
read or interpreted the bytes. Decision 017 separately defines the narrower
recoverable complete-session Stop contract.

The success vocabulary is deliberately layered:

```text
admitted / pending                         # not a wire success receipt
  -> bytes_applied [start_byte, end_byte)  # ctxmux native Input owner
  -> acknowledged                         # Integration / target protocol
  -> replied or settled                    # Agent harness
```

Only admission through `bytes_applied` belongs to the runtime kernel. Agent
messages, delivery state, semantic acknowledgement, replies, task graphs, and
UI timelines remain outside the daemon.

Each Run admits at most 1,024 queued input commands and 4 MiB of queued input.
Lazy blocking input drains share a daemon-wide eight-worker hard limit and
yield after a bounded completed burst; there is no permanent third thread per
Run. A blocking PTY write has no independent deadline, so eight stalled writes
can delay input progress for other Runs until one owning PTY returns or closes.
Resize and stop do not enter the input queue, so that limitation does not hold a
Tokio worker or consume their control lane. Zero dimensions fail before resize
mutation. A blocking reader assigns each non-empty read one contiguous
half-open cumulative byte range, stores it in the bounded log, then broadcasts it.

The waiter waits for the child and allows the output reader up to one second to finish before publishing `Exited`. This is a bounded drain policy. It is not a proof that arbitrarily large or delayed final output always precedes exit.

### Interactive CLI attach

The CLI writes replay first. When stdin and stdout are terminals, it applies the current size, enters raw mode through an RAII guard, reads terminal bytes on a blocking thread, forwards `SIGWINCH`, and interprets `Ctrl-b d` as detach. A non-TTY attach only follows output.

A checked-in controlling-PTY fixture starts the real CLI, observes raw terminal
mode, forwards input, propagates a master resize through `SIGWINCH`, detaches
with `Ctrl-b d`, compares the complete terminal attributes before and after,
and verifies that the same daemon-owned Run PID remains live. Catchable-signal,
daemon-loss, and unwind restoration paths remain broader qualification work.

### Cross-language client parity

The TypeScript SDK buffers fragmented or coalesced socket data into newline
frames, enforces the frame byte limit, applies bounded inbound backpressure,
and mirrors the Rust request and attachment operations. It runtime-validates
every nested generation-14 server variant before exposing it. Each Rust and
TypeScript Attachment has one inbound router: command results resolve a
bounded pending map while events enter a separately bounded delivery inbox, so
a slow event consumer does not create a competing socket reader or hide a
control receipt. Both clients exact-encode an outbound control before consuming
its attachment command ID; a deterministic local frame rejection is
`not_applied`, while a send whose completion is lost remains `unknown`. Each
Attachment admits only one pending event-consumer call, and daemon EOF after
one terminal event ends that event stream cleanly. Cross-client tests create
and retry Runs with retained keys, reconnect through another client, verify the
same identity and PID, and control the shared Run.

Generated TypeScript types prevent a second handwritten wire schema. Current `u64` fields are still emitted as JavaScript `number`, so the SDK rejects values outside the safe-integer range rather than exposing a rounded cursor. A future exact large-integer representation remains a protocol decision.

## Concurrency, ordering, and failure semantics

The important guarantees are behavioral, not implied by lock types.

- Output byte ranges are allocated under the output-log mutex before broadcast.
- Attachment subscribes before snapshot and suppresses live chunks whose byte range is already in replay.
- A slow attachment that loses only raw output receives `Gap { latest_output_bytes }`; the client must reattach from its own last observed byte cursor. If the skipped interval contains a tmux observation, it receives cursor-free `ObservationDiscontinuity` and ends because byte replay cannot repair that semantic loss. A skipped terminal publication is reconstructed once from authoritative `RunState` before EOF rather than mislabeled as tmux observation loss.
- Attachment command IDs start at one and increase within one connection.
  They provide correlation, not deduplication. A non-increasing ID is fatal
  before mutation; reconnect starts a new ID scope and cannot settle old work.
- First-party Attachments retain at most 64 unresolved commands, including at
  most 32 input commands and 1 MiB of input data. Their event inbox is bounded
  to 256 entries and 1 MiB of byte payload. Local output overflow becomes an
  ordered `Gap`; an unrepresentable non-output loss fails the attachment
  closed instead of being mislabeled as replayable output loss.
- `RunInfo` reads output and lifecycle under separate locks, so it is useful metadata rather than a transactional snapshot of every field.
- In persistent mode `durable_output_bytes` advances only after the store actor
  commits a contiguous replay batch. Live `latest_output_bytes` may be ahead.
- Persistence-capable activation, output recording, and native terminal
  publication serialize through one daemon-private per-Run transition gate,
  then acquire output, state, and persistence in that order for short snapshots.
  Memory-only output takes only its output-log lock before broadcast. Durable
  append/finalize waits hold the transition gate but no public read-path lock.
  The initial append is queued before the binding becomes observable; during a
  durable finalize, status/list/attach continue to see `Running` until the
  receipt returns. Whether a fast child exits before or after activation,
  exactly one owner finalizes the committed running row as terminal. Output
  observed after terminal publication may enter incarnation-local replay and
  the internal broadcast channel, but it is not durable and is not guaranteed
  to an attachment after that attachment receives its terminal event.
- Input, resize, and stop from multiple clients are accepted concurrently and
  serialize only at their owned resources. The current-incarnation native
  facade fences live authority as `Open -> Stopping -> Closed`; durable
  `RunState` is not used as permission to signal, write, or authorize a fresh
  Level B continuation. A product-level multi-writer or resize arbitration
  policy is not defined.
- Start and Fork keys use fixed random-hashed async stripes. A leader resolves
  an existing match or conflict before dispatch, so duplicates occupy neither
  Tokio workers nor creation threads while the unique unbound request launches.
  Unbound leaders then wait asynchronously for one of eight physical-launch
  permits; these Tokio semaphore waiters are not a product-level actor or custom
  queue. In memory-only mode an admitted leader then reserves one projected
  Registry record before spawn. At the 128-record ceiling the same Registry
  write fences the earliest fully quiescent terminal candidate; a missing
  eligible candidate returns `run_capacity` before Backend mutation. A fresh
  Fork materializes its immutable parent input before this reservation and
  releases the parent pin, so admission never carries a hidden long-lived
  lookup owner. The retained Run and successful key mapping share one registry lock;
  a failed unpublished launch releases its private Run and exact-key fence only
  after the daemon-wide cleanup owner proves reap and all output, lifecycle, control,
  input, and Run owners are quiescent. Until then, a matching retry reports
  temporary Backend unavailability, conflicting reuse reports
  `creation_conflict`, and unrelated keys can use released stripes and launch
  permits. A matching published Fork retry is resolved before current parent
  capability or lifecycle checks.
- Daemon shutdown first fences new unbound creation flights, then cleans up tmux
  control owners and drains already-started creation threads within one shared
  deadline. Fencing closes launch admission and wakes queued unbound waiters;
  matching retained-key lookups remain resolvable. After creation flights
  drain, shutdown also waits for transferred unpublished-child cleanup and
  reports every exact-key fence owner and cleanup-owned failure reason without
  echoing the caller-owned key. A
  creation thread has no hard-cancellation mechanism: if it exceeds the
  shutdown deadline, shutdown reports failure but cannot reap that detached
  thread independently. This does not turn shutdown into a general native
  process-tree policy.
- Planned upgrade uses a separate reversible request fence rather than shutdown
  admission. A permit covers dispatch, the owned mutation result, and the final
  response write. Drain timeout or all-owner preflight failure reopens service;
  after extraction, the gate is sealed and any failure is daemon fail-stop.
- Malformed or oversized transport frames can close the connection before a structured protocol error is sent. Explicit error categories cover validly decoded requests and lifecycle failures.
- Native Stop owns one `portable-pty`-created POSIX session. It terminates and
  revalidates every visible local, same-user, non-elevated member while the
  unreaped leader anchors the session ID, then reaps that leader, and reports
  failure unless that scope is empty. The member SID check and numeric signal
  are adjacent syscalls with no wait, lock, allocation, logging, or unrelated
  I/O between them; observation and permission uncertainty fail closed. POSIX
  still permits exit and PID reuse in that syscall gap, so this practical
  contract has a documented residual wrong-process TOCTOU rather than a
  zero-risk incarnation guarantee. Descendants that create another session are
  outside this declared ownership boundary. Daemon `Ctrl-C` still stops the
  listener and drops in-memory ownership; native shutdown policy remains
  separate work.

## Backend and Integration remain independent

A Backend answers where and how a Run executes. An Integration answers what runs inside it and which extra context operations are honest.

The native PTY and tmux pane adapter are Backend implementations even though a
public Backend interface has not been extracted. Importing an existing tmux
pane is a distinct public runtime operation rather than an Integration or
plugin mechanism.

Integrations remain ordinary TypeScript modules exported by explicitly imported
packages; the provider-neutral contract and shell implementation remain in the
Runtime SDK. Agent-specific modules belong to the embedding product that owns
the Provider. The daemon does not discover packages, embed JavaScript, launch
plugin processes, or host a marketplace. If an Integration observer
disappears, the raw daemon-owned Run remains usable.

## Context, fork, and tmux targets

Fork fidelity is capability-declared.

- Level A copies only declared portable inputs. It never claims to clone hidden live-process state.
- Level B adds Integration-provided workspace, artifact, lineage, and native resume or fork information.
- Level C, arbitrary process-memory or undeclared-state cloning, is out of scope.

Level A is implemented by cloning the retained parent's complete immutable
`RunSpec`; references are recorded once in `RunSpec.declared_inputs`, while
`RunInfo.lineage` records only the immediate parent and actual fidelity. Level B
is a caller-materialized execution contract: the provider-neutral host boundary
must produce source-bound provenance and the exact `RunSpec` before raw fork.
Missing, unowned, copied, or cross-Run provenance returns a structured
unsupported result before planner or daemon mutation; it never falls back to
Level A. Provider event parsing, session identifiers, replay interpretation,
and native-resume commands are not daemon or generic SDK truth. Neither level
claims a workspace snapshot or owned artifact store.

tmux compatibility follows the public-adapter boundary. One imported Run maps
to one pane selected through the tmux executable and public Control Mode while
tmux remains its owner. The complete import tuple fails closed on target
change. `tmux_version` names the selected server version; client and server
compatibility are checked separately.

The adapter retains only raw bytes observed after import and marks the missing
prefix as truncated. A tmux pause or source loss remains visible to live and
late attachments; `capture-pane` is never presented as raw history. This slice
is read-only and memory-only, and unsupported native operations fail through
capability checks. It will not reproduce tmux's private socket protocol or
promise that an unmodified tmux client can attach to ctxmux.

## Security, durability, and resource boundaries

The Unix socket is created with mode `0600`. Startup refuses to replace an ordinary file or symlink and removes an existing socket only after it is not accepting connections. Startup stale cleanup revalidates device/inode identity and liveness immediately before unlink and fails closed on an observed replacement. Shutdown retains the device/inode of the socket this daemon bound and removes the published pathname only while it still names that identity; an independently substituted listener is preserved. Pathname recheck and unlink remain separate kernel operations, and a renamed original socket cannot be rediscovered through its old pathname, so an attacker-writable parent directory stays outside the guarantee; authentication beyond filesystem access and peer-credential policy is open.

Each Run retains at most 4 MiB of raw output by byte count, except that one oversized final chunk may exceed that target because the log always retains at least one chunk. Live delivery uses a bounded 256-event broadcast channel. Native input additionally has the per-Run queue and daemon-wide active-drain bounds above. Both memory-only and persistent modes admit at most 128 retained or projected Run records and replace only fenced, terminal, fully quiescent candidates. Persistent replacement removes the exact durable Run, replay, and byte-exact key in the same transaction before the Registry publishes its successor. Attachment admission and a total daemon RSS quota remain open.

[Decision 013](architecture/choices/013-retained-run-resource-governance.md)
owns the shared 128-record Registry ceiling and ownership-safe collection
contract. Persistent mode uses the same ticket and candidate SSOT, with a
spill-disabled cache-resident page proof before launch and exact durable
replacement at COMMIT. T-033 covers the ordinary reduced-capacity correctness
matrix. The source-bound T-005 nightly qualification additionally exercises
both production 128-record modes across three turnover windows, persistent
restart, maximum replay pressure, the existing 1,800-second soak, and bounded
private owner telemetry. This qualifies that declared workload; it does not
add an attachment-admission or total-RSS product guarantee.

`reliability-budgets.json` freezes daemon CPU, peak and steady RSS, retained
bytes, and per-Run RSS/thread/fd slopes for idle and active 1/32/128 Run
workloads. Cleanup requires no live direct child or attachment and no transient
thread growth. Below memory-only capacity, retained terminal state remains
intentional; at capacity, replacement compacts the exact candidate's already
closed native descriptors before starting the successor. Qualification does not
subtract retained history or mislabel it as cleanup-owned leakage. On the
EOF-driven successful-Control path qualified below, an imported tmux Run instead
releases its incarnation-local Control stdin and stdout reader descriptors and
reaps its Control process before `Interrupted` becomes observable. Retaining its
historical Run record therefore has no per-terminal Control descriptor slope;
this cleanup does not imply Run GC.

Persistent mode validates and exclusively locks one owner-only state directory
before socket publication. One bundled SQLite connection on one actor thread
commits starts, contiguous output batches, pruning/accounting, and terminal
transitions; a bounded actor queue backpressures the PTY reader instead of
accumulating an unbounded durable-output backlog. If SQLite reports typed
`DiskFull` while appending output or finalizing a Run, that actor keeps the
exact command at the head of the queue and retries after a short delay; later
durable mutations cannot pass it, and daemon shutdown cancels the wait. Every
other storage, replay, budget, integrity, and owner-invariant failure remains
fail-stop for later durable mutations. Startup performs journal
recovery and exact schema/application validation against the schema-4 format
envelope, then uses bounded, restartable page-admitted transactions to
reconcile old running rows, normalize retained history to 128, and finally
finish serving-epoch publication. Only after operational revalidation can the daemon
publish its socket. Recovered exited or interrupted Runs support list, status, replay
attach, and Level A fork; input, resize, stop, and recovered Level B fork fail
explicitly. The replacement daemon never opens, adopts, attaches to, or signals
a PID from durable metadata.

## Technical decision index

Status is explicit so a target document cannot masquerade as shipped architecture.

| Decision                              | Status                                                         | Record                                                              |
| ------------------------------------- | -------------------------------------------------------------- | ------------------------------------------------------------------- |
| Rust and Tokio long-lived daemon      | accepted                                                       | [001](architecture/choices/001-rust-tokio-daemon.md)                |
| `portable-pty` native Backend         | accepted                                                       | [002](architecture/choices/002-portable-pty-native-backend.md)      |
| Unix socket and NDJSON protocol       | accepted for generation 14                                     | [003](architecture/choices/003-unix-socket-json-lines-protocol.md)  |
| Run lifecycle concurrency             | accepted, incomplete policy                                    | [004](architecture/choices/004-run-lifecycle-concurrency.md)        |
| Ordered bounded raw-output replay     | accepted                                                       | [005](architecture/choices/005-ordered-output-replay.md)            |
| Rust schema and TypeScript codegen    | accepted                                                       | [006](architecture/choices/006-rust-schema-ts-codegen.md)           |
| Node TypeScript SDK                   | accepted                                                       | [007](architecture/choices/007-node-typescript-sdk.md)              |
| `crossterm` interactive CLI           | accepted                                                       | [008](architecture/choices/008-crossterm-interactive-cli.md)        |
| Runtime persistence and recovery      | accepted and implemented                                       | [009](architecture/choices/009-runtime-persistence-recovery.md)     |
| Explicit TypeScript Integrations      | accepted                                                       | [010](architecture/choices/010-explicit-typescript-integrations.md) |
| Context, artifacts, lineage, and fork | accepted                                                       | [011](architecture/choices/011-context-artifact-lineage-fork.md)    |
| tmux Control Mode Backend             | accepted and implemented; version lanes pending                | [012](architecture/choices/012-tmux-control-mode-backend.md)        |
| Retained Run resource governance      | accepted, implemented, and qualified for the declared workload | [013](architecture/choices/013-retained-run-resource-governance.md) |
| Recoverable native Input              | accepted and implemented                                       | [014](architecture/choices/014-recoverable-input-operations.md)     |
| Exec-in-place upgrade continuity      | accepted and implemented in persistent mode                    | [015](architecture/choices/015-exec-in-place-upgrade-continuity.md) |
| Interrupted-Run derivation            | accepted                                                       | [016](architecture/choices/016-interrupted-run-derivation.md)       |
| Recoverable native Stop               | accepted and implemented                                       | [017](architecture/choices/017-recoverable-stop-operations.md)      |
| Remote endpoint over system OpenSSH   | accepted; vertical and client-side endpoint contract delivered | [018](architecture/choices/018-remote-endpoint-transport.md)        |

## Risk-to-fixture traceability

Architecture claims become durable only when their known failure modes have a disposition.

Each decision record contains a `Wrong-case corpus（错题集）` section and a fixture mapping. The [architecture wrong-case casebook](architecture/casebook.md) is the cross-decision index. A retained case must identify its source, failure mechanism, ctxmux invariant, and one of these dispositions:

- active: an executable fixture runs in `scripts/check.sh`;
- covered: an existing test proves the invariant and is linked directly;
- future: the owning capability or deterministic seam is absent and its activation condition is explicit;
- characterization: the failure shape is retained while the product contract or oracle remains undecided;
- rejected: the case does not transfer to ctxmux, with a recorded reason.

The formal docs deliberately do not depend on the local research corpus that seeded these cases; that working material stays a developer-local aid. The retained cases and their dispositions live in tracked sources instead: `fixtures/wrong-cases.json` carries the machine-readable trace with external `source_refs`, and each decision record restates its cases inline. The point is to keep a checkable disposition for every failure mode rather than an untraceable list of web folklore.

The governing rule is compact: terminals are views, Runs are durable, and every stronger claim needs public-behavior evidence.
