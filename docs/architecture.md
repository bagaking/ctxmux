# Architecture

ctxmux makes a Run durable by keeping its runtime ownership in one local daemon. Terminals, CLIs, SDKs, editors, and automations are replaceable views over that Run.

This page is the architecture entrypoint. It distinguishes shipped behavior from target design, follows the important end-to-end paths, and links every critical technical decision to its own record.

## Current guarantees and target boundaries

Current guarantees are deliberately narrower than the product vision.

| Area             | Current                                                                                                                                                                                                                                                                                                     | Target or open                                                                               |
| ---------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| Run lifetime     | A native child survives client disconnects, while ctxmux control of that child and its PTY lasts only for the owning daemon lifetime. Optional `--state-dir` mode recovers historical Run state and committed replay across daemon restart; a prior running row becomes interrupted without live authority. | Live PTY handoff, process adoption, host-reboot continuity, and upgrade continuity are open. |
| Transport        | Versioned NDJSON over an explicitly selected Unix socket.                                                                                                                                                                                                                                                   | Windows transport, discovery, and daemon activation are open.                                |
| Clients          | Rust CLI and dependency-free TypeScript SDK share protocol generation 6, including correlated attachment controls and typed owner receipts; the retained-Run capacity error is declared while its T-027 Registry implementation remains pending.                                                            | Other SDKs appear only for a real client requirement.                                        |
| Attach           | Retained raw bytes plus ordered live events; interactive CLI raw mode and `Ctrl-b d`.                                                                                                                                                                                                                       | Screen reconstruction and a multi-writer policy are open.                                    |
| Backends         | Native `portable-pty`; an implemented read-only public-Control-Mode tmux pane adapter with required version-lane qualification pending.                                                                                                                                                                     | Wider tmux control and other Backends require separate evidence.                             |
| Integrations     | The SDK explicitly binds shell and Codex Integrations; Codex probes and executes native session resume.                                                                                                                                                                                                     | Broader Integration coverage and context capture remain open.                                |
| Context and fork | Level A clones a declared `RunSpec`; Codex Level B resumes a declared session; both record lineage and fidelity.                                                                                                                                                                                            | Workspace snapshots and artifact ownership remain open.                                      |
| Persistence      | Optional `--state-dir` mode recovers historical metadata, lineage, terminal state, and committed replay; default mode remains memory-only.                                                                                                                                                                  | Live PTY handoff, schema migration, and online history management are open.                  |

“Durable” always includes client churn. With persistent mode it additionally
includes the declared historical recovery class across daemon restart; it does
not include live PTY control continuity, process adoption, host-reboot process
continuity, or schema migration.

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
                    Unix domain socket (v6)
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

### Run domain model

`RunSpec` is the launch contract for native Runs: program, arguments, optional
working directory, selected environment additions, initial terminal size, and
ordered opaque workspace, artifact, or context references. An imported tmux
Run has no launch spec because tmux already owns its pane and process.
`RunInfo` exposes identity, immediate parent and actual fork fidelity when
present, Backend identity and capabilities, PID when available, lifecycle
state, retained-output cursors, optional committed `durable_head_seq`, and
attachment count. For an imported tmux Run, `RunInfo.pid` is the pane PID
observed at import and participates in the target fence; it is not daemon-owned
process authority and ctxmux never signals it.

The implemented lifecycle has three observable states:

```text
start accepted
     |
  running -- child wait completes --> exited(code, signal?)
     |
     +-- stop accepted -- asynchronous wait --> exited(code, signal?)
     |
     +-- later daemon epoch --> interrupted(daemon_restart)
```

`stop` sends one termination command to the waiter that owns the direct child handle. On Unix that handle gives `SIGHUP` a short grace period and escalates to a forced kill when the child remains alive. Acknowledgement still precedes public terminal-state publication, so the returned `RunInfo` may say `running`; repeated stop is rejected. The waiter disables further signalling as soon as wait observes exit, before it publishes `Exited`, so a concurrent stop cannot fall back to a cached numeric PID. Descendant or process-tree termination is not promised. In persistent mode a new daemon epoch converts prior `running` rows to `interrupted { daemon_restart }`, clears their PID, and exposes no live control. Memory-only exited Runs remain in the current daemon map indefinitely; persistent history uses the bounded retention policy from decision 009.

### Ownership split

| Owner        | Responsibilities                                                                                                                                                                                                     |
| ------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Runtime core | Run identity and capabilities, native launch and PTY ownership, tmux observation clients, ordered raw output, attachment and reconnect behavior, public errors, and the persistence required by declared guarantees. |
| tmux         | Imported server, session, window, pane, PTY, process, layout, persistence, and their native lifecycle.                                                                                                               |
| Integration  | Tool detection, structured launch and Level B fork planning, capability declaration, and optional host-local semantic events. Shell and Codex native resume are current.                                             |
| Client       | Terminal rendering, editing UI, user workflow, multi-Run composition, Agent scheduling and evaluation, and Crucible or MapReduce policy.                                                                             |

## Components and stable boundaries

Each package has one reason to change.

| Component         | Responsibility                                                                            | Must not own                                  |
| ----------------- | ----------------------------------------------------------------------------------------- | --------------------------------------------- |
| `ctxmux-protocol` | Rust wire types, generation constant, frame limit, serialization, and TypeScript export.  | Live processes or client policy.              |
| `ctxmux-client`   | Rust connector, request lifecycle, attachment connection, and typed public errors.        | Daemon state.                                 |
| `ctxmux-daemon`   | Unix listener, Run manager, PTYs, children, replay, events, and socket lifecycle.         | Agent-specific semantics or UI.               |
| `ctxmux`          | Human CLI, raw terminal mode, resize forwarding, and detach prefix.                       | Direct access to daemon internals.            |
| `@ctxmux/sdk`     | Node connector, request and attachment APIs, and explicit host-local Integration binding. | Electron, React, an editor, or Run ownership. |

### Stable boundary

The stable product boundary is the local protocol, not a Rust ABI or Node native addon. Rust and TypeScript clients can evolve independently while exercising the same daemon path.

## Core scenarios and end-to-end paths

The key paths converge in the daemon rather than duplicating runtime logic in each client.

### Start a native Run

1. The CLI or SDK constructs a `RunSpec`, retains or generates one bounded
   creation operation key, and opens a Unix-socket connection.
2. The connection exchanges an exact protocol-generation handshake.
3. `Request::Start` reaches the daemon-private creation owner. A fixed,
   asynchronously acquired key stripe serializes only possible key collisions;
   a retained matching mapping returns the original Run before launch.
4. Only an unbound leader waits on a Tokio semaphore admitting at most eight
   simultaneous physical launches. The 64 key stripes bound collision state;
   they are not a 64-thread launch limit. Cancellation while awaiting admission
   releases the key stripe and creates no flight or thread.
5. An admitted leader claims a creation flight, reserves one of the same eight
   private rollback-owner slots, and then starts one named, short-lived OS thread
   that owns the semaphore permit, key stripe, request, manager, reservation,
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
   owner unwind. Successful `COMMIT` is the point of no return: even if vacuum
   or physical-file postchecks then latch persistence, the manager binds
   persistence and stores `Arc<Run>` plus the key mapping under one
   `RunRegistry` write before returning that error. A retry therefore resolves
   the committed Run instead of spawning again. Closing the request connection
   cannot drop the Run or roll back a published mapping.

### Import a tmux-owned pane

1. A public client selects one explicit tmux socket and discovers live panes.
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

1. An attachment uses a dedicated long-lived connection and names `after_seq`.
2. The daemon subscribes to live events before taking the replay snapshot. This closes the replay/live race.
3. The daemon sends an `Attached` header containing `RunInfo`, replay cursors,
   and `truncated`, then streams each retained chunk as an ordered
   `RunEvent::Output` frame. This keeps every encoded frame below the 1 MiB
   transport ceiling even when total retained output is much larger.
4. Rust and TypeScript clients reassemble those bounded frames before returning
   the public snapshot. Live chunks already covered by that snapshot are
   deduplicated by sequence.
5. Clean detach returns `Detached`; abrupt socket closure drops the attachment guard. Both leave the Run in `RunManager`.
6. A later attachment resumes from its last observed sequence or detects that retained output was evicted.

### Input, resize, output, and exit

Short-lived operations and attached frames use the same daemon-private native
control owner. Attachment `input`, `resize`, and `stop` frames carry a
connection-local command ID and receive a separate correlated result rather
than a Run event. Input acceptance means the complete payload crossed the PTY
write boundary; resize acceptance reports `resize -> get_size` readback from
the owning PTY; stop acceptance remains distinct from the later `Exited`
event. A lost result is unknown disposition and is never permission to replay
input.

Each Run admits at most 1,024 queued input commands and 4 MiB of queued input.
Lazy blocking input drains share a daemon-wide eight-worker hard limit and
yield after a bounded completed burst; there is no permanent third thread per
Run. A blocking PTY write has no independent deadline, so eight stalled writes
can delay input progress for other Runs until one owning PTY returns or closes.
Resize and stop do not enter the input queue, so that limitation does not hold a
Tokio worker or consume their control lane. Zero dimensions fail before resize
mutation. A blocking reader assigns one monotonically increasing sequence per
read chunk, stores it in the bounded log, then broadcasts it.

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
every nested generation-6 server variant before exposing it. Each Rust and
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

- Output sequence is allocated under the output-log mutex before broadcast.
- Attachment subscribes before snapshot and suppresses live chunks whose sequence is already in replay.
- A slow attachment that overruns the Tokio broadcast buffer receives `Gap { head_seq }`; the client must reattach from its own last observed sequence.
- Attachment command IDs start at one and increase within one connection.
  They provide correlation, not deduplication. A non-increasing ID is fatal
  before mutation; reconnect starts a new ID scope and cannot settle old work.
- First-party Attachments retain at most 64 unresolved commands, including at
  most 32 input commands and 1 MiB of input data. Their event inbox is bounded
  to 256 entries and 1 MiB of byte payload. Local output overflow becomes an
  ordered `Gap`; an unrepresentable non-output loss fails the attachment
  closed instead of being mislabeled as replayable output loss.
- `RunInfo` reads output and lifecycle under separate locks, so it is useful metadata rather than a transactional snapshot of every field.
- In persistent mode `durable_head_seq` advances only after the store actor
  commits a contiguous replay batch. Live `head_seq` may be ahead.
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
  queue. The retained Run and successful key mapping share one registry lock;
  a failed unpublished launch releases its private Run and exact-key fence only
  after the child-handle waiter proves reap and all reader, waiter, control,
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
  reports every exact-key fence owner and waiter-owned failure reason without
  echoing the caller-owned key. A
  creation thread has no hard-cancellation mechanism: if it exceeds the
  shutdown deadline, shutdown reports failure but cannot reap that detached
  thread independently. This does not turn shutdown into a general native
  process-tree policy.
- Malformed or oversized transport frames can close the connection before a structured protocol error is sent. Explicit error categories cover validly decoded requests and lifecycle failures.
- Native stop owns only the direct child handle; process-group, descendant, and orphan policy is not declared. Daemon `Ctrl-C` stops the listener and drops in-memory ownership.

## Backend and Integration remain independent

A Backend answers where and how a Run executes. An Integration answers what runs inside it and which extra context operations are honest.

The native PTY and tmux pane adapter are Backend implementations even though a
public Backend interface has not been extracted. The current Codex Integration
launches a generic native Run; importing an existing tmux pane is a distinct
public runtime operation rather than an Integration or plugin mechanism.

Integrations remain ordinary TypeScript modules exported by explicitly imported packages; the first-party modules currently use the SDK's `integrations` subpath. The daemon does not discover packages, embed JavaScript, launch plugin processes, or host a marketplace. If an Integration observer disappears, the raw daemon-owned Run must remain usable.

## Context, fork, and tmux targets

Fork fidelity is capability-declared.

- Level A copies only declared portable inputs. It never claims to clone hidden live-process state.
- Level B adds Integration-provided workspace, artifact, lineage, and native resume or fork information.
- Level C, arbitrary process-memory or undeclared-state cloning, is out of scope.

Level A is implemented by cloning the retained parent's complete immutable `RunSpec`; references are recorded once in `RunSpec.declared_inputs`, while `RunInfo.lineage` records only the immediate parent and actual fidelity. For Codex Level B, the SDK Attachment owner records the source Run of every live event and replay chunk in a host-local object registry. A parent-scoped registered observer accepts only event/chunk objects issued for that parent, maps its emitted `thread.started` receipt back to the same Run, and rejects missing, unowned, copied-chunk, or cross-Run input before planning or raw fork. Every Integration that advertises Level B must implement both a planner and `levelBForkProvenance`; omission fails closed. The verified receipt materializes `codex exec resume --json` with declared workspace, artifact, and session references, while the real semantic canary proves happy-path continuation. This is accidental-misrouting protection inside the supported SDK API, not authentication against a malicious JavaScript host that can call the raw fork protocol. Shell exposes no Level B capability and rejects the request rather than falling back to Level A. Neither path claims a workspace snapshot or owned artifact store.

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

Each Run retains at most 4 MiB of raw output by byte count, except that one oversized final chunk may exceed that target because the log always retains at least one chunk. Live delivery uses a bounded 256-event broadcast channel. Native input additionally has the per-Run queue and daemon-wide active-drain bounds above. Exited Runs, total Run count, attachment count, and total daemon memory still have no global quotas or GC.

[Decision 013](architecture/choices/013-retained-run-resource-governance.md)
accepts a 128-record Registry ceiling and ownership-safe collection contract for
T-027. It remains target design until the generation-6 public error, Registry,
persistence, and sustained-churn gates land; the current no-GC behavior above
remains the shipped truth in the meantime.

`reliability-budgets.json` freezes daemon CPU, peak and steady RSS, retained
bytes, and per-Run RSS/thread/fd slopes for idle and active 1/32/128 Run
workloads. Cleanup requires no live direct child or attachment and no transient
thread growth. RSS and two daemon-owned descriptors per stopped native Run
remain visible because the retained Run map has no GC; qualification does not
subtract that intentional state or mislabel it as cleanup-owned leakage. On the
EOF-driven successful-Control path qualified below, an imported tmux Run instead
releases its incarnation-local Control stdin and stdout reader descriptors and
reaps its Control process before `Interrupted` becomes observable. Retaining its
historical Run record therefore has no per-terminal Control descriptor slope;
this cleanup does not imply Run GC.

Persistent mode validates and exclusively locks one owner-only state directory
before socket publication. One bundled SQLite connection on one actor thread
commits starts, contiguous output batches, pruning/accounting, and terminal
transitions; a bounded actor queue backpressures the PTY reader instead of
accumulating an unbounded durable-output backlog. Startup performs journal recovery, exact-schema and application
invariant checks, then atomically allocates a new epoch and reconciles old
running rows. Recovered exited or interrupted Runs support list, status, replay
attach, and Level A fork; input, resize, stop, and recovered Level B fork fail
explicitly. The replacement daemon never opens, adopts, attaches to, or signals
a PID from durable metadata.

## Technical decision index

Status is explicit so a target document cannot masquerade as shipped architecture.

| Decision                              | Status                                          | Record                                                              |
| ------------------------------------- | ----------------------------------------------- | ------------------------------------------------------------------- |
| Rust and Tokio long-lived daemon      | accepted                                        | [001](architecture/choices/001-rust-tokio-daemon.md)                |
| `portable-pty` native Backend         | accepted                                        | [002](architecture/choices/002-portable-pty-native-backend.md)      |
| Unix socket and NDJSON protocol       | accepted for generation 6                       | [003](architecture/choices/003-unix-socket-json-lines-protocol.md)  |
| Run lifecycle concurrency             | accepted, incomplete policy                     | [004](architecture/choices/004-run-lifecycle-concurrency.md)        |
| Ordered bounded raw-output replay     | accepted                                        | [005](architecture/choices/005-ordered-output-replay.md)            |
| Rust schema and TypeScript codegen    | accepted                                        | [006](architecture/choices/006-rust-schema-ts-codegen.md)           |
| Node TypeScript SDK                   | accepted                                        | [007](architecture/choices/007-node-typescript-sdk.md)              |
| `crossterm` interactive CLI           | accepted                                        | [008](architecture/choices/008-crossterm-interactive-cli.md)        |
| Runtime persistence and recovery      | accepted and implemented                        | [009](architecture/choices/009-runtime-persistence-recovery.md)     |
| Explicit TypeScript Integrations      | accepted                                        | [010](architecture/choices/010-explicit-typescript-integrations.md) |
| Context, artifacts, lineage, and fork | accepted                                        | [011](architecture/choices/011-context-artifact-lineage-fork.md)    |
| tmux Control Mode Backend             | accepted and implemented; version lanes pending | [012](architecture/choices/012-tmux-control-mode-backend.md)        |
| Retained Run resource governance      | accepted design; implementation pending         | [013](architecture/choices/013-retained-run-resource-governance.md) |

## Risk-to-fixture traceability

Architecture claims become durable only when their known failure modes have a disposition.

Each decision record contains a `Wrong-case corpus（错题集）` section and a fixture mapping. The [architecture wrong-case casebook](architecture/casebook.md) is the cross-decision index. A retained case must identify its source, failure mechanism, ctxmux invariant, and one of these dispositions:

- active: an executable fixture runs in `scripts/check.sh`;
- covered: an existing test proves the invariant and is linked directly;
- future: the owning capability or deterministic seam is absent and its activation condition is explicit;
- characterization: the failure shape is retained while the product contract or oracle remains undecided;
- rejected: the case does not transfer to ctxmux, with a recorded reason.

The source corpus lives under `.bagakit/researcher/`; architecture pages cite it rather than copying an untraceable list of web folklore.

The governing rule is compact: terminals are views, Runs are durable, and every stronger claim needs public-behavior evidence.
