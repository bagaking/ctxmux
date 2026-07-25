# 009 — Runtime persistence and recovery

- Status: accepted and implemented
- Scope: durability beyond one daemon lifetime

## Context

The current daemon makes Runs independent of clients but stores identity, metadata, lifecycle, and replay only in memory. Daemon restart loses control even if an operating-system process survives. Product language must not hide this boundary.

## Decision

Persistence is an explicit daemon mode selected with one dedicated
operator-owned `--state-dir`. Without that directory, the daemon remains
memory-only and makes no restart claim. With it, ctxmux uses one SQLite database
owned by the daemon through a single persistence actor thread. `rusqlite` with
the bundled maintained SQLite library is the selected implementation: it
provides transactions, rollback/WAL recovery, integrity checks, and bounded
incremental writes without inventing a second file-commit protocol or rewriting
a whole retained replay on every PTY read.

The accepted recovery class is historical Run recovery:

- Durable metadata includes `RunId`, immutable `RunSpec`, lineage, lifecycle
  state, source daemon epoch, output cursors, and persistence timestamps.
- Durable replay is the newest bounded, contiguous window committed by the
  persistence actor. `RunInfo.durable_head_seq` is `None` in memory-only mode
  and the highest committed sequence in persistent mode; durable oldest/head
  cursors and `truncated` describe pruning at the front of that window. Live
  delivery may be ahead of the durable head; after abrupt daemon loss only the
  committed window is promised.
- A recovered exited Run supports `list`, `status`, replay attachment, and
  portable Level A fork. It rejects input, resize, and stop as terminal.
- A row left `running` by an older daemon epoch becomes the explicit terminal
  state `interrupted { reason: daemon_restart }` in one startup transaction.
  Its public PID is cleared. It supports the same historical read/replay and
  Level A behavior as an exited Run, but never claims an exit code.
- Live PTY ownership, child-handle transfer, PID re-adoption, and transparent
  input/resize/stop continuity are unsupported. A replacement daemon never
  opens, attaches to, or signals a process named only by persisted metadata.

Before opening SQLite or publishing its socket, a persistent daemon validates
that the state directory is not a symlink, is owned by the effective user, and
has mode `0700`; a newly created directory receives that mode before any secret
write. It opens an owner-only companion lock file and holds a non-blocking
exclusive standard-library file lock for the daemon lifetime. A second opener
fails with typed `state_in_use`; it cannot allocate an epoch or reconcile Runs
still owned by the first daemon.

While that lock is held, each daemon allocates a fresh UUID epoch. Startup lets
SQLite perform its documented journal recovery, then validates exact schema
version, `PRAGMA quick_check`, and application invariants: typed IDs and JSON,
allowed lifecycle values, non-self lineage, byte totals, strictly contiguous
retained chunk sequences, matching durable oldest/head cursors, and quota
accounting. Epoch creation and all prior-epoch running-to-interrupted changes
commit in one transaction before socket publication. Protocol generation 3 is
pre-stable, so the first schema has no migration, downgrade, reset, salvage, or
compatibility fallback. An unknown version, failed integrity check, or invalid
application invariant is a typed startup failure. Ctxmux performs no repair,
reset, migration, or partial exposure; SQLite recovery writes allowed by its
documented commit algorithm are not described as leaving bytes untouched.

The persistence actor batches ordered output without adding a per-Run thread.
Its bounded 1,024-command queue applies backpressure to the PTY reader instead
of allowing durable-output backlog memory to grow without limit. SQLite WAL
mode and transactions define four indivisible application units:

- start inserts the complete Run row before child publication;
- one output batch inserts its chunks, prunes replay, and advances durable
  oldest/head cursors and byte accounting together;
- a terminal transition commits the final replay batch and exited state in the
  same transaction;
- startup epoch/reconciliation and record eviction with dependent replay removal
  each commit as one transaction.

Output batches may lag live delivery but advance only contiguously. Process crash
or torn WAL recovery therefore yields the previous or next complete unit, never
a lifecycle/cursor/chunk hybrid. A start or fork that cannot reserve one new
record within the immutable record and metadata budgets rejects only that
unpublished Run; because no row was written, the actor continues serving
existing Runs and later admissible starts. Every append or finalize error, and
every serialization, database, I/O, commit, integrity, or owner-invariant
failure still latches the actor, freezes the durable cursor, and rejects later
mutations with a typed persistence error. Already-owned live Runs may still be
explicitly controlled so storage failure does not strand a child behind a false
success.

Retention is part of the format, not deferred GC. The existing 4 MiB per-Run
replay tail remains. Persistent replay has a 256 MiB global logical byte budget;
serialized metadata has a separate 64 MiB logical byte budget; and at most 4,096
Run records are retained. The oldest chunks are pruned across Runs while keeping
each retained replay window contiguous and its truncation cursors exact. The
oldest terminal or interrupted records are removed when either metadata limit is
reached. Running records are not deleted; a start that cannot reserve its full
metadata fails before child publication.

The SQLite page size is 4 KiB and `max_page_count` is 98,304 (384 MiB main
database). One transaction may append at most 8 MiB of WAL frames; output is
split into smaller ordered batches, and one record eviction fits because per-Run
replay is capped at 4 MiB. Before admitting a transaction when current WAL is
above 8 MiB, the actor requires a successful `TRUNCATE` checkpoint with no busy
reader and rechecks a zero-length WAL. It therefore admits a write only when
`current WAL + worst-case transaction <= 16 MiB`, rather than detecting excess
afterward. The shared-memory file has a 4 MiB ceiling. The complete state
directory has a 404 MiB hard file budget plus the small lock file. Incremental
vacuum is part of retention after eviction; inability to return below a logical
or physical limit fails admission rather than widening the budget.

Before SQLite open, existing database, WAL, SHM, and lock paths must be regular
owner-matching files, never symlinks, with no group/other permissions. Newly
created database and sidecars are set to `0600` and revalidated before the first
Run transaction because `RunSpec`, declared references, environment additions,
and output may contain secrets.

## Quality attributes and invariants

- Recovery claims must be proven across real daemon restart or upgrade, not inferred from stored rows.
- Corrupt or stale state fails closed and never attaches to the wrong process.
- Process identity cannot rely on PID alone.
- Cleanup, retention, and recovery use one ownership model.
- A new persistence layer must not move live Run ownership into a client.
- A returned durable cursor names committed replay, not merely queued storage
  work.
- Optional persistence must not change daemon-neutral Run or Integration/Backend
  boundaries.

## Alternatives

- Atomic whole-file JSON snapshots would repeatedly serialize retained replay,
  duplicate commit logic, and create avoidable CPU/write amplification.
- Custom append logs still require checksums, transaction grouping, indexes,
  compaction, directory durability, corruption classification, and concurrency
  policy that SQLite already owns.
- Metadata-only persistence is smaller but loses the raw context required to
  inspect an interrupted or exited Run after restart.
- An always-stable per-Run shim could retain PTY ownership across daemon
  replacement but adds one process and supervision boundary per Run.
- Delegating durability to tmux preserves tmux-owned sessions only; it does not
  recover the native Backend and would merge two extension axes.

## Known constraints

The implemented recovery class is enabled only with `ctxmuxd --state-dir`; the
default remains memory-only. SQLite durability is bounded by its documented
filesystem and flush assumptions and is not evidence of power-loss safety on a
filesystem that violates them.

The state database intentionally contains exact local Run metadata and output,
including environment additions and opaque references. It is not encrypted and
must not live in a shared directory. Logical record/chunk eviction and SQLite
vacuum are not secure erasure on copy-on-write filesystems or SSDs. The supported
whole-store cleanup is: stop the daemon so the exclusive lock is released,
validate the exact dedicated state directory, remove that directory as one
operator action, and rely on the storage medium's own secure-erasure policy when
confidential deletion is required. There is no online partial secret purge.

There is no schema migration in the pre-stable contract. Physical SQLite files
may retain free pages until incremental vacuum/checkpoint, but the exact logical
and file ceilings above remain admission limits.

An old native child may survive daemon death if it ignores PTY hangup. The new
daemon reports its Run as interrupted and never guesses ownership or signals it.
Automatic orphan adoption or cleanup would require a durable platform identity
stronger than PID and is explicitly unsupported. The operator-selected socket
and state paths still do not provide discovery or activation policy.

## Wrong-case corpus

Evidence pack: [persistence-recovery track](../../../.bagakit/researcher/topics/engineering/ctxmux-wrong-case-corpus/tracks/persistence-recovery.md), claim `C009`.

- `PERSIST-01` (`i01`, `i02`): a persisted numeric PID can refer to an unrelated live process after restart. Ambiguous identity must become a non-recoverable typed state, never guessed adoption.
- `PERSIST-02` (`i03`): interruption between state writes, flushes, rename, and directory durability can expose a parseable mixed generation. Recovery must select one validated generation or report corruption.

Linux pidfds demonstrate stable identity within one boot but are neither portable nor durable across restart. SQLite demonstrates the failure class and explicit storage assumptions; it does not mandate SQLite as the implementation.

## Fixture mapping

- Active: a real restart restores exited metadata, lineage, exact
  bounded replay, terminal behavior, and Level A fork with a distinct child.
- Active: daemon kill while a Run is live restores only the committed
  replay window, exposes durable oldest/head/truncation cursors, marks the Run
  interrupted, and never claims live PTY control.
- Active / `PERSIST-01`: a stored running row naming an unrelated live PID is
  reconciled to interrupted; the unrelated process and old orphan are neither
  opened nor signalled.
- Active / `PERSIST-02`: a parseable cursor/chunk mixed generation returns a
  typed startup corruption failure before socket publication or partial Run
  exposure; SQLite transactions own old-or-new commit recovery.
- Active: the 4 MiB per-Run replay boundary, state lock, exact schema version,
  owner-only directory/sidecar modes, and symlink rejection are executable.
- Qualification constants and admission checks cover the 256 MiB replay, 64
  MiB metadata, 4,096-record, 384 MiB main database, 16 MiB WAL, 4 MiB SHM,
  eviction, checkpoint, and complete state-directory boundaries without
  allocating every production ceiling in each ordinary Gate run.
- Future: version migration and rollback fixtures activate only when a second
  schema is actually proposed.

## Open questions

- Does a later product milestone justify a stable per-Run owner or another
  platform mechanism for live PTY handoff? This decision does not pre-design it.
- Which user-facing inspection or deletion command should manage durable history
  once a real client requires it?
- A future schema revision must decide migration and rollback before changing
  the exact-version fail-closed rule.

## Repository evidence

- `crates/ctxmux-protocol/src/lib.rs`: `RunId`
- `crates/ctxmux-daemon/src/persistence.rs`: state-directory owner, single
  SQLite actor, recovery validation, reconciliation, and retention
- `crates/ctxmux-daemon/tests/persistence_recovery.rs`: real restart,
  stale-PID, corruption, lock, permissions, and replay-retention evidence
- `crates/ctxmux-daemon/src/lib.rs`: live and recovered `RunManager` paths
- `docs/protocol.md`: public persistent-mode lifetime boundary
- `docs/roadmap.md`: M3.5 recovery acceptance
- `.bagakit/researcher/topics/engineering/ctxmux-wrong-case-corpus/tracks/persistence-recovery.md`:
  PID reuse and atomic-generation evidence with transfer limits
