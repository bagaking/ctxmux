# 004 — Run lifecycle and concurrency model

- Status: accepted implementation; product policy incomplete
- Scope: shared Run state, attachments, mutation serialization, and lifecycle events

## Context

Multiple short requests and long-lived attachments may act on the same Run while blocking output and child-wait work continues. The model must keep client failure local and preserve ordered output without inventing a distributed actor system.

## Decision

`RunManager` retains `Arc<Run>` values behind an `RwLock`. A Run uses narrow standard locks for lifecycle state, output log, PTY master, input writer, and child killer; an atomic counter tracks attachments. Blocking reader and waiter threads update the Run, while a Tokio broadcast channel feeds each attachment task.

Attachment subscribes before taking its replay snapshot. An `AttachmentGuard` decrements the counter on every return path, including transport failure.

## Quality attributes and invariants

- A connection task never owns the Run's last strong reference.
- Attach snapshot and live delivery do not have an uncovered subscribe gap.
- Output sequence allocation and log insertion are one locked operation.
- A dropped attachment eventually decrements the observable count.
- Lifecycle errors are explicit after a Run reaches `exited`.

## Alternatives

- One actor per Run could centralize ordering but adds a mailbox and supervision model before policy requires it.
- One global mutex would simplify reasoning but couple unrelated Runs and I/O paths.
- Client-owned state would violate the durability invariant.

## Known constraints

`RunInfo` is assembled from separate state and output locks, so it is not a transactional snapshot. Concurrent writers and resizers have no product-level arbitration. Stop acknowledgement precedes terminal exit. Broadcast lag reports one `head_seq` but does not automatically replay. Exited Runs are never collected, and daemon shutdown semantics are unspecified.

Poisoned locks recover their inner value; this prevents secondary panics but is not a declared consistency-recovery strategy.

## Wrong-case corpus

Evidence pack: [lifecycle-concurrency track](../../../.bagakit/researcher/topics/engineering/ctxmux-wrong-case-corpus/tracks/lifecycle-concurrency.md), claim `C004`.

- `LC-001` (`d01`, `d02`): confusing the broadcast receiver cursor, daemon head, and caller's last delivered sequence can skip or duplicate recoverable output after lag.
- `LC-002` (`d02`): a terminal event can make the last retained data unreachable if exit closes delivery before replay recovery. Final bytes must remain available through attachment or reattach.
- `LC-003` (`d03`): the waiter can reap a child before public state changes; a concurrent stop that still signals by cached numeric PID risks a reused process identity. This needs a deterministic lifecycle model rather than probabilistic PID churn.

Tokio's historical lag and close bugs are fixed. The transferred risk is ctxmux's composition of broadcast, replay, and lifecycle state, not a claim that current Tokio loses messages.

## Fixture mapping

- Covered now: disconnect and reattach, attachment count release, invalid operations after exit, and exact retained final bytes followed by one terminal event on late attach.
- Candidate: output produced exactly between subscribe and snapshot.
- Candidate: two observers plus concurrent input and resize.
- Candidate: stop racing with output, input, and natural exit.
- Candidate: force a real attachment through broadcast lag, observe `Gap`, and recover through public cursor-based reattach. The current channel-plus-log unit test is mechanism evidence, not this end-to-end oracle.
- Candidate: exited-Run collection and attachment during collection.

## Open questions

- What multi-writer and resize policy is visible to clients?
- Must metadata snapshots become atomic, or can fields declare independent freshness?
- How are exited Runs retained, pinned, exported, and collected?
- What is the daemon shutdown contract for live Run mutations?

## Repository evidence

- `crates/ctxmux-daemon/src/lib.rs`: `RunManager`, `Run`, `AttachmentGuard`, `handle_attachment`
- `crates/ctxmux-daemon/tests/native_lifecycle.rs`
- `packages/sdk/test/client-parity.test.ts`
