# Recover from a transient full durable-state disk

## Problem

Persistent mode currently latches every append or finalize database error for the
rest of the daemon incarnation. That is correct for corruption, conflicting bytes,
broken invariants, and unknown non-idempotent mutations. It is too strong for one
observed case: SQLite returned `SQLITE_FULL`, storage was freed later, but the
daemon kept rejecting every durable mutation with the old error.

The live Run continued producing output while the durable cursor stayed frozen.
Restarting a client could not help because the daemon, PTY, persistence actor, and
latched error all outlived the client.

## Decision

- Classify SQLite's typed `DiskFull` code before converting it to public text.
- Keep the exact failed append/finalize operation inside the single persistence
  actor and retry it with a small bounded delay.
- Preserve the existing 1,024-command queue. When storage remains full, that queue
  backpressures the PTY reader and eventually the child instead of growing an
  unbounded output backlog.
- Do not process a later durable mutation before the failed operation commits.
- Let daemon shutdown stop the retry loop; shutdown is not required to wait for
  storage to become writable.
- Continue to latch every non-`DiskFull` append/finalize error exactly as today.

Append and finalize transactions are safe to retry because replay ranges and
stored bytes are verified exactly and durable cursors advance only after COMMIT.
A retry that observes an already committed unit verifies the same bytes instead of
inserting a second logical range.

## Non-goals

- No second database, journal, actor, Runtime, or salvage path.
- No retry for corruption, conflicting replay, owner-invariant, file-budget, or
  generic I/O errors.
- No promise that the child keeps making progress while the storage needed for its
  declared durability remains unavailable.
- No recovery of bytes already lost by a daemon that ran the old fail-stop code.

## Acceptance

- An injected first `DiskFull` append later commits the complete bytes without
  reopening the daemon or moving the durable cursor backward.
- A terminal finalize blocked by `DiskFull` eventually persists both its final
  replay and terminal state in order.
- A later Start cannot pass the failed output/finalize command.
- The ordinary conflicting-replay fixture still poisons the actor.
- Architecture, protocol, focused Rust tests, and the ordinary repository Gate
  describe and prove the same boundary.
