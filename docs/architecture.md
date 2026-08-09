# Architecture

ctxmux makes a Run durable by keeping its runtime ownership in one local daemon. Terminals, CLIs, SDKs, editors, and automations are replaceable views over that Run.

This page is the architecture entrypoint. It distinguishes shipped behavior from target design, follows the important end-to-end paths, and links every critical technical decision to its own record.

## Current guarantees and target boundaries

Current guarantees are deliberately narrower than the product vision.

| Area             | Current                                                                               | Target or open                                                      |
| ---------------- | ------------------------------------------------------------------------------------- | ------------------------------------------------------------------- |
| Run lifetime     | A native PTY child survives CLI and SDK disconnects while its daemon remains alive.   | Restart recovery and upgrade continuity are open.                   |
| Transport        | Versioned NDJSON over an explicitly selected Unix socket.                             | Windows transport, discovery, and daemon activation are open.       |
| Clients          | Rust CLI and dependency-free TypeScript SDK share protocol generation 1.              | Other SDKs appear only for a real client requirement.               |
| Attach           | Retained raw bytes plus ordered live events; interactive CLI raw mode and `Ctrl-b d`. | Screen reconstruction and a multi-writer policy are open.           |
| Backends         | One native `portable-pty` implementation.                                             | A public-surface tmux adapter is provisional.                       |
| Integrations     | No Integration contract exists in code yet.                                           | Explicitly imported TypeScript Integrations are provisional.        |
| Context and fork | `RunSpec` contains launch inputs only.                                                | Level A and Level B context, artifacts, lineage, and fork are open. |
| Persistence      | Run metadata and output live in daemon memory.                                        | Durable metadata, GC, and restart reconciliation are open.          |

“Durable” therefore means durable across client lifetimes today. It does not yet mean durable across daemon restart, host reboot, or binary upgrade.

## System and ownership model

The daemon is the only process that owns live runtime state.

```text
CLI                  TypeScript host              future editor / automation
 |                         |                                  |
 +----------- public versioned protocol / SDK ----------------+
                              |
                    Unix domain socket (v1)
                              |
                    long-lived ctxmux daemon
                    - RunManager / Run identity
                    - PTY / child / input writer
                    - lifecycle / output / replay
                    - attachment event delivery
                              |
                    native Backend (current)
                    tmux Backend (provisional)
                              |
                         local process
```

A client may create, observe, control, or stop a Run. Socket closure removes one attachment; it never means “stop the Run.” This boundary keeps editor restarts, CLI exits, and Integration-host exits from becoming accidental process supervisors.

### Run domain model

`RunSpec` is the launch contract: program, arguments, optional working directory, selected environment additions, and initial terminal size. `RunInfo` exposes identity, PID when available, lifecycle state, retained-output cursors, and attachment count.

The implemented lifecycle has two observable states:

```text
start accepted
     |
  running -- child wait completes --> exited(code, signal?)
     |
     +-- stop accepted -- asynchronous wait --> exited(code, signal?)
```

`stop` acknowledges that the kill request was accepted. The returned `RunInfo` may still say `running`; the waiter publishes the terminal state later. Exited Runs and their retained output remain in the current daemon map indefinitely because collection policy is not implemented.

### Ownership split

| Owner        | Responsibilities                                                                                                                                                                              |
| ------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Runtime core | Run identity and launch spec, PTY and child lifecycle, ordered raw output, attachment and reconnect behavior, public errors, and the persistence required by declared guarantees.             |
| Integration  | Tool detection, launch planning, capability declaration, tool-specific context capture, native resume or fork plans, and optional semantic events. This is target behavior, not current code. |
| Client       | Terminal rendering, editing UI, user workflow, multi-Run composition, Agent scheduling and evaluation, and Crucible or MapReduce policy.                                                      |

## Components and stable boundaries

Each package has one reason to change.

| Component         | Responsibility                                                                           | Must not own                                  |
| ----------------- | ---------------------------------------------------------------------------------------- | --------------------------------------------- |
| `ctxmux-protocol` | Rust wire types, generation constant, frame limit, serialization, and TypeScript export. | Live processes or client policy.              |
| `ctxmux-client`   | Rust connector, request lifecycle, attachment connection, and typed public errors.       | Daemon state.                                 |
| `ctxmux-daemon`   | Unix listener, Run manager, PTYs, children, replay, events, and socket lifecycle.        | Agent-specific semantics or UI.               |
| `ctxmux`          | Human CLI, raw terminal mode, resize forwarding, and detach prefix.                      | Direct access to daemon internals.            |
| `@ctxmux/sdk`     | Node Unix-socket connector and TypeScript request and attachment APIs.                   | Electron, React, an editor, or Run ownership. |

The stable product boundary is the local protocol, not a Rust ABI or Node native addon. Rust and TypeScript clients can evolve independently while exercising the same daemon path.

## Core scenarios and end-to-end paths

The key paths converge in the daemon rather than duplicating runtime logic in each client.

### Start a native Run

1. The CLI or SDK constructs a `RunSpec` and opens a Unix-socket connection.
2. The connection exchanges an exact protocol-generation handshake.
3. `Request::Start` reaches `RunManager::start` and `Run::spawn`.
4. The daemon validates the spec, opens a PTY, spawns the child, retains the master, writer, killer, and identity, then starts blocking reader and waiter threads.
5. The manager stores `Arc<Run>` before returning metadata. Closing the request connection cannot drop the Run.

### Attach, disconnect, and reattach

1. An attachment uses a dedicated long-lived connection and names `after_seq`.
2. The daemon subscribes to live events before taking the replay snapshot. This closes the replay/live race.
3. The snapshot contains `RunInfo`, retained chunks newer than the cursor, and a `truncated` flag.
4. Live chunks already covered by the snapshot are deduplicated by sequence.
5. Clean detach returns `Detached`; abrupt socket closure drops the attachment guard. Both leave the Run in `RunManager`.
6. A later attachment resumes from its last observed sequence or detects that retained output was evicted.

### Input, resize, output, and exit

Short-lived operations and attached frames call the same `Run::input`, `Run::resize`, and `Run::stop` methods. The PTY writer serializes input; resize rejects zero dimensions. A blocking reader assigns one monotonically increasing sequence per read chunk, stores it in the bounded log, then broadcasts it.

The waiter waits for the child and allows the output reader up to one second to finish before publishing `Exited`. This is a bounded drain policy. It is not a proof that arbitrarily large or delayed final output always precedes exit.

### Interactive CLI attach

The CLI writes replay first. When stdin and stdout are terminals, it applies the current size, enters raw mode through an RAII guard, reads terminal bytes on a blocking thread, forwards `SIGWINCH`, and interprets `Ctrl-b d` as detach. A non-TTY attach only follows output.

The prefix router has unit coverage. Raw-mode restoration, resize, and detach have a manual pseudo-terminal smoke record but do not yet have a checked-in PTY-level end-to-end fixture.

### Cross-language client parity

The TypeScript SDK buffers fragmented or coalesced socket data into newline frames, enforces the frame byte limit, applies bounded inbound backpressure, and mirrors the Rust request and attachment operations. It runtime-validates every nested generation-1 server variant before exposing it. The cross-client test creates a Run with one client, disconnects, reconnects with the other, verifies the same PID, and controls the shared Run.

Generated TypeScript types prevent a second handwritten wire schema. Current `u64` fields are still emitted as JavaScript `number`, so the SDK rejects values outside the safe-integer range rather than exposing a rounded cursor. A future exact large-integer representation remains a protocol decision.

## Concurrency, ordering, and failure semantics

The important guarantees are behavioral, not implied by lock types.

- Output sequence is allocated under the output-log mutex before broadcast.
- Attachment subscribes before snapshot and suppresses live chunks whose sequence is already in replay.
- A slow attachment that overruns the Tokio broadcast buffer receives `Gap { head_seq }`; the client must reattach from its own last observed sequence.
- `RunInfo` reads output and lifecycle under separate locks, so it is useful metadata rather than a transactional snapshot of every field.
- Input, resize, and stop from multiple clients are accepted concurrently and serialize only at their owned resources. A product-level multi-writer or resize arbitration policy is not defined.
- Malformed or oversized transport frames can close the connection before a structured protocol error is sent. Explicit error categories cover validly decoded requests and lifecycle failures.
- Daemon `Ctrl-C` stops the listener and drops in-memory ownership. Descendant-process and orphan behavior is not yet a declared guarantee.

## Backend and Integration remain independent

A Backend answers where and how a Run executes. An Integration answers what runs inside it and which extra context operations are honest.

The current native PTY is a Backend implementation even though the public Backend interface has not been extracted. A future Codex Integration may launch through native PTY or tmux without creating two unrelated client APIs.

Integrations remain ordinary TypeScript packages explicitly imported by a host. The daemon does not discover packages, embed JavaScript, launch plugin processes, or host a marketplace. If an Integration observer disappears, the raw daemon-owned Run must remain usable.

## Context, fork, and tmux targets

Fork fidelity is capability-declared.

- Level A copies only declared portable inputs. It never claims to clone hidden live-process state.
- Level B adds Integration-provided workspace, artifact, lineage, and native resume or fork information.
- Level C, arbitrary process-memory or undeclared-state cloning, is out of scope.

A Level B request against a Level A-only Integration must fail closed. The protocol does not implement either level yet.

tmux compatibility follows the public-adapter boundary. ctxmux may use the tmux executable or Control Mode to discover and interact with existing sessions while tmux remains their owner. It will not reproduce tmux's private socket protocol or promise that an unmodified tmux client can attach to ctxmux.

## Security, durability, and resource boundaries

The Unix socket is created with mode `0600`. Startup refuses to replace an ordinary file or symlink and removes an existing socket only after it is not accepting connections. Authentication beyond filesystem access and peer-credential policy is open.

Each Run retains at most 4 MiB of raw output by byte count, except that one oversized final chunk may exceed that target because the log always retains at least one chunk. Live delivery uses a bounded 256-event broadcast channel. Exited Runs, thread count, attachment count, and total daemon memory have no global quotas or GC yet.

Current state is memory-owned. A daemon restart loses Run identities, metadata, replay, and the ability to control PTYs. The persistence decision must distinguish recoverable metadata from live PTY ownership that the replacement process may not be able to reclaim.

## Technical decision index

Status is explicit so a target document cannot masquerade as shipped architecture.

| Decision                              | Status                      | Record                                                              |
| ------------------------------------- | --------------------------- | ------------------------------------------------------------------- |
| Rust and Tokio long-lived daemon      | accepted                    | [001](architecture/choices/001-rust-tokio-daemon.md)                |
| `portable-pty` native Backend         | accepted                    | [002](architecture/choices/002-portable-pty-native-backend.md)      |
| Unix socket and NDJSON protocol       | accepted for generation 1   | [003](architecture/choices/003-unix-socket-json-lines-protocol.md)  |
| Run lifecycle concurrency             | accepted, incomplete policy | [004](architecture/choices/004-run-lifecycle-concurrency.md)        |
| Ordered bounded raw-output replay     | accepted                    | [005](architecture/choices/005-ordered-output-replay.md)            |
| Rust schema and TypeScript codegen    | accepted                    | [006](architecture/choices/006-rust-schema-ts-codegen.md)           |
| Node TypeScript SDK                   | accepted                    | [007](architecture/choices/007-node-typescript-sdk.md)              |
| `crossterm` interactive CLI           | accepted                    | [008](architecture/choices/008-crossterm-interactive-cli.md)        |
| Runtime persistence and recovery      | open                        | [009](architecture/choices/009-runtime-persistence-recovery.md)     |
| Explicit TypeScript Integrations      | provisional                 | [010](architecture/choices/010-explicit-typescript-integrations.md) |
| Context, artifacts, lineage, and fork | open                        | [011](architecture/choices/011-context-artifact-lineage-fork.md)    |
| tmux Control Mode Backend             | provisional                 | [012](architecture/choices/012-tmux-control-mode-backend.md)        |

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
