# 009 — Runtime persistence and recovery

- Status: accepted and implemented
- Scope: durability beyond one daemon lifetime
- Resource-policy amendment: [019](019-resource-policy-and-honest-qualification.md) replaces population/format quotas and fixed byte ceilings with independent configured budgets; the durability boundaries below remain authoritative.

## Context

The current daemon makes Runs independent of clients but stores identity, metadata, lifecycle, and replay only in memory. Daemon restart loses control even if an operating-system process survives. Product language must not hide this boundary.

## Decision

Persistence is an explicit daemon mode selected with one dedicated
operator-owned `--state-dir`. Without that directory, the daemon remains
memory-only and makes no restart claim. With it, ctxmux uses one SQLite metadata
database and one append-only replay generation directory, both owned by the
daemon through a single persistence actor thread. `rusqlite` with the bundled
maintained SQLite library remains the source of transactional identity,
lifecycle, cursors, and retention indexes. Replay bytes are written and synced
to the generation file before their SQLite coordinates commit; startup truncates
uncommitted tails and removes only unreferenced generations. Compaction first
publishes a synced active destination, then migrates coordinates in bounded,
spill-disabled page-admitted transactions. Each committed extent is authoritative:
a crash may leave both source and destination durably referenced, and recovery
validates and resumes that state before serving.

The accepted recovery class is historical Run recovery:

- Durable metadata includes `RunId`, its byte-exact creation operation key,
  immutable `RunSpec`, lineage, lifecycle state, source daemon epoch, output
  cursors, and persistence timestamps.
- Durable replay is the newest bounded, contiguous window committed by the
  persistence actor. `RunInfo.durable_output_bytes` is `None` in memory-only mode
  and the highest committed output byte in persistent mode; durable first/latest
  cursors and `truncated` describe pruning at the front of that window. Live
  delivery may be ahead of the durable head; after abrupt daemon loss only the
  committed window is promised.
- A recovered exited Run supports `list`, `status`, replay attachment, and
  portable Level A fork. It rejects input, resize, and stop as terminal.
- A row left `running` by an older daemon epoch becomes the explicit terminal
  state `interrupted { reason: daemon_restart }` during bounded, restartable
  startup normalization before socket publication.
  Its public PID is cleared. It supports the same historical read/replay and
  Level A behavior as an exited Run, but never claims an exit code.
- After a cold restart, live PTY ownership, child-handle transfer, PID
  re-adoption, and transparent input/resize/stop continuity are unsupported. A
  replacement daemon never opens, attaches to, or signals a process named only
  by persisted metadata. Decision 015 is the narrow planned-upgrade exception:
  the same daemon process carries actual master descriptors and wait authority
  across `execve`; it does not infer ownership from SQLite.

Before opening SQLite or publishing its socket, a persistent daemon validates
that the state directory is not a symlink, is owned by the effective user, and
has mode `0700`; a newly created directory receives that mode before any secret
write. It opens an owner-only companion lock file and holds a non-blocking
exclusive standard-library file lock for the daemon lifetime. Shutdown
explicitly unlocks it after closing SQLite, so a Run child caught between
`fork` and `exec` cannot extend state ownership through its inherited file
description. A second opener fails with typed `state_in_use`; it cannot allocate
an epoch or reconcile Runs still owned by the first daemon.

While that lock is held, each cold daemon start allocates a fresh UUID epoch.
An intentional exec-in-place upgrade inherits the same locked file description
and epoch because it remains the same live-control owner. Startup lets
SQLite perform its documented journal recovery, then validates exact schema
version, `PRAGMA quick_check`, and application invariants: typed IDs, bounded
creation keys, an exact BINARY unique-key index, and typed JSON, a required
native `RunSpec` accepted by the same semantic validator as live
start and fork, allowed lifecycle values, non-self lineage, byte totals,
strictly contiguous retained byte ranges, matching durable first/latest
cursors, and quota accounting. Schema 5 additionally stores one valid
`runtime_meta.runtime_id` UUID beside the serving epoch. It is created once for
the state-directory lineage and survives cold replacement while the serving
epoch changes; a planned exec reloads both preserved identities from this
existing owner. It is not derived from the state path, socket, PID, host, or
serving epoch. Public Hello therefore reports
`runtimeIdPersistence: "state_dir"`: cold replacement preserves `runtimeId`
and changes `daemonInstanceId`, while validated planned exec preserves both.
The persistent-mode advertised capability record includes the two implemented
`services.*` keys and omits memory-only `tmux.import`; the exact catalog and
numeric semantics remain owned by [the protocol](../../protocol.md#connection-state).
Schema-5 validation has no record-count format ceiling. A valid store that
exceeds the selected operator policy reports resource pressure without evicting
history to fit that policy. Startup normalization uses bounded,
spill-disabled transactions to reconcile prior running rows. Each batch
starts from a zero WAL, proves its cache-resident page charge before COMMIT,
and may be resumed after interruption. A new schema stores its valid UUID as a
bootstrap epoch immediately so an interrupted first open remains structurally
reopenable; an existing store retains its previous epoch during normalization.
In both cases the final startup transaction completes serving-epoch
publication only after normalization, and the socket is published only after
application and operational invariants are revalidated. Protocol generation 18
and persistence schema 5 are pre-stable, so the current schema has no
migration, downgrade, reset, salvage, or compatibility fallback. An unknown
version, failed integrity check, or invalid application invariant is a typed
startup failure. Ctxmux performs no repair, reset, migration, or partial
exposure; SQLite recovery writes allowed by its documented commit algorithm
are not described as leaving bytes untouched.

The persistence actor batches ordered output without adding a per-Run thread.
Its bounded 1,024-command queue applies backpressure to the PTY reader instead
of allowing durable-output backlog memory to grow without limit. SQLite WAL
mode and transactions define four indivisible application units:

- start or fork stages the complete creation-time `Running` row and creation
  key with `pid = NULL` before physical launch, then commits that already
  page-admitted transaction after spawn and before registry publication;
  successful `COMMIT` is its point of no return even when a later file
  postcheck fails;
- one output batch inserts its chunks, prunes replay, and advances durable
  oldest/head cursors and byte accounting together;
- a terminal transition commits the final replay batch, exited state, and the
  actual historical child PID in the same transaction;
- each startup reconciliation prefix, record-eviction prefix with dependent
  replay removal, and final epoch publication commits as one transaction.

Output batches may lag live delivery but advance only contiguously. Process crash
or torn WAL recovery therefore yields the previous or next complete unit, never
a lifecycle/cursor/chunk hybrid. A start or fork that cannot reserve one new
record within the configured record and metadata budgets rejects only that
unpublished Run; because no row was written, the actor continues serving
existing Runs and later admissible starts. A typed SQLite `DiskFull`, write-side
SQLite I/O pressure, or external `StorageFull` from an output append or terminal
finalize is the retryable storage condition: the single actor keeps that exact
unit at the head of its ordered work, waits 50 ms,
and tries again. Its bounded queue then backpressures the PTY reader and child
rather than admitting an unbounded in-memory durability gap. Daemon shutdown
cancels the wait. The actor does not retry generic I/O, corruption, replay
conflict, file-budget, integrity, owner-invariant, or any other database error;
those failures still latch the actor, freeze the durable cursor, and reject
later mutations with a typed persistence error. A post-commit start check also
latches the actor, but its reply carries the committed outcome so the daemon
must publish that Run and key before returning the error; treating it as an
uncommitted rollback would permit a second physical child. Already-owned live
Runs may still be explicitly controlled so storage failure does not strand a
child behind a false success.

Retention cursors and byte accounting are format invariants; the selected
retention budgets are operator policy. The defaults are a 4 MiB per-Run disk
replay tail, 256 MiB aggregate disk replay and 64 MiB resident metadata.
Hot replay has separate budgets. Live and retained Run counts have no default
population quota; optional explicit quotas do not become schema constraints.
The oldest replay prefixes are clipped across Runs, including inside an extent,
while keeping each retained window contiguous and its truncation cursors exact.
The oldest exact terminal or interrupted candidates are removed when
record or metadata admission requires replacement. Because the creation key is
a required column of that same row, retention removes the durable mapping in
the same transaction. Running records are not deleted by ordinary admission; a
start that cannot reserve its full record and metadata burden fails before
child publication.

Replay storage is external to SQLite. `replay_chunks` stores only `run_id`,
byte ranges, generation name, file offset, and byte length; it has no inline
payload fallback. The active generation is owner-only `0600` and named only by
a validated basename. Appends are sequential and synced before the SQLite
transaction commits; generation directory entries are synced before a new
generation name is published. A transaction that fails before its append is
committed truncates its tail; an outer commit with an unknown outcome preserves
the harmless tail so startup normalization can resolve the durable index and
truncate it before the store becomes observable. When a generation exceeds twice
the 256 MiB logical replay budget, compaction creates and syncs a packed
replacement, durably publishes its active name, and copies retained segments in
bounded batches. Payload sync precedes every coordinate COMMIT, which uses the
same measured cache-page WAL proof as Run admission. Source files remain until
all their references have moved. Startup validates non-overlap independently
within each referenced file, truncates only beyond its committed extent tail,
removes only unreferenced files, and resumes an interrupted migration. Packed
offsets rebase; neither lifetime output nor sparse-allocation fragmentation
becomes a new capacity limit. Maintenance runs before append/finalize COMMIT,
so safe storage-pressure retry cannot repeat an already committed lifecycle
transition. An uncertain maintenance COMMIT retains every possibly referenced
file and latches writes even if the underlying error resembles disk-full.
A missing, shortened, overlapping, symlinked, or unreadable referenced segment
still fails startup closed.

Physical page pressure is an independent retention boundary. Before startup
normalization or an allocating persistent mutation, the owner must reclaim
oldest replay prefixes when the main database lacks one admitted transaction's
page headroom. Logical replay below 256 MiB does not prove physical capacity:
small rows and partially occupied pages can exhaust the fixed page ceiling.
Reclamation uses bounded, spill-disabled transactions under the existing WAL
charge proof; it preserves Run/key/spec/lifecycle metadata and the durable head,
and advances the surviving replay floor and truncation fact atomically. It must
work on a valid same-schema database already at its physical limit and survive
reopen without inventing contiguous bytes. The configured physical cap stays fixed during the mutation; no
VACUUM, migration, external database rewrite, or silent Run deletion is part of
this operation. A page-limit exhaustion that cannot make progress is not an
external transient disk-full event and must not monopolize the actor forever.

The SQLite page size is a 4 KiB format constant. `max_page_count` derives from
`database_bytes`; its 384 MiB default funds 98,304 pages. Staged transactions
use `wal_checkpoint_bytes` (8 MiB by default), and total WAL funding is twice
that window. Output, coordinate migration and reclamation split into smaller
ordered batches when their measured page charge requires it. The actor folds
the WAL below the configured checkpoint window, records its admission baseline,
and proves the exact spill-disabled transaction's WAL _growth_ from its
cache-resident page upper bound before COMMIT. Payload length is never a proxy
for modified pages. SHM funding derives from SQLite's 32 KiB WAL-index blocks
and the permitted frame count (64 KiB under the default WAL policy), not an
unrelated 4 MiB ceiling. The complete file budget funds the configured database,
WAL and SHM, an old generation of up to twice retained replay, a packed
replacement of up to retained replay, and one 1 MiB append work unit.
This derives necessary compaction scratch headroom. A valid oversized WAL is
checkpointed after integrity validation during recovery; excess size alone is
resource pressure, never corruption. Exact replacement leaves freed pages reusable
inside the configured main-database ceiling instead of running an uncharged
post-COMMIT incremental vacuum. A failure discovered before COMMIT rejects
admission without publishing a Run. Once replacement and the new Run/key row
commit, a physical-file postcheck failure is a committed error: it latches the
actor, the daemon still publishes the committed Run/key mapping for retry
convergence, and later mutations fail closed rather than widening the budget.

Before SQLite open, existing database, WAL, SHM, lock, and replay-generation
paths must be regular owner-matching files, never symlinks, with no group/other
permissions. Newly
created database and sidecars are set to `0600` and revalidated before the first
Run transaction because `RunSpec`, declared references, environment additions,
and output may contain secrets.

## Quality attributes and invariants

- Recovery claims must be proven across real daemon restart or upgrade, not inferred from stored rows.
- Corrupt or stale state fails closed and never attaches to the wrong process.
- Process identity cannot rely on PID alone.
- Cleanup, retention, and recovery use one ownership model.
- A recovered Run and its creation key are one atomic retention unit; no
  pending reservation or immortal idempotency tombstone exists.
- Persistence-capable native Run activation, output recording, and terminal
  publication use one per-Run transition gate before the
  output-to-state-to-persistence lock order. Creation persists a `Running`
  snapshot with no durable PID even when the child already exited. The actual
  PID is written only with a successful terminal finalize, and whichever side
  observes the other second owns that single finalize. Store waits never retain
  the public read-path locks;
  durable state remains `Running` until finalize returns. A byte observed only
  after terminal publication may enter the current incarnation's memory replay
  and internal broadcast channel, but it is not durable and is not guaranteed
  to an attachment after that attachment receives its terminal event.
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

- `PERSIST-01` (`i01`, `i02`): a persisted numeric PID can refer to an unrelated live process after restart. Ambiguous identity must become a non-recoverable typed state, never guessed adoption.
- `PERSIST-02` (`i03`): interruption between payload sync, directory durability,
  SQLite generation switch, and old-generation cleanup can expose a parseable
  mixed generation. A durably indexed migration prefix is valid; recovery must
  validate all referenced files and exact extent coordinates. Missing or
  inconsistent coordinates fail closed, while legitimate intermediate states
  resume without losing retained bytes.

Linux pidfds demonstrate stable identity within one boot but are neither portable nor durable across restart. SQLite demonstrates the failure class and explicit storage assumptions; it does not mandate SQLite as the implementation.

## Fixture mapping

- Active: a real restart restores exited metadata, lineage, exact
  bounded replay, terminal behavior, and Level A fork with a distinct child.
- Active: daemon kill while a Run is live restores only the committed
  replay window, exposes durable oldest/head/truncation cursors, marks the Run
  interrupted, and never claims live PTY control.
- Active: an intentional persistent-mode `SIGHUP` preserves daemon and child
  PIDs, listener inode, live PTY control, ordered output, and the complete
  recoverable-Input cursor/ledger across a real exec, while existing
  attachments reconnect.
- Active: public Hello observations prove that a cold replacement on the same
  state directory keeps the Runtime ID and changes the daemon instance; both
  endpoints report `runtimeIdPersistence: "state_dir"`, the Rust build target,
  and the exact persistent-mode capability record.
- Active / `PERSIST-01`: a stored running row naming an unrelated live PID is
  reconciled to interrupted; the unrelated process and old orphan are neither
  opened nor signalled.
- Active / `PERSIST-02`: append rollback tails, orphan generations, and a
  parseable cursor/chunk mixed generation are normalized or return a typed
  startup corruption failure before socket publication or partial Run exposure;
  SQLite transactions plus synced payloads and generation directory entries
  own exact committed-coordinate recovery. High-cardinality, fragmented-row,
  mid-copy crash and uncertain-COMMIT fixtures cover this boundary.
- Active: deterministic actor faults translate SQLite `DiskFull`, write-side
  I/O pressure, or external `StorageFull`, retry the same append/finalize before
  later mutation, and stop waiting on shutdown;
  the replay-conflict fixture still latches the actor.
- Active: the 4 MiB per-Run replay boundary, state lock, exact schema version,
  owner-only directory/sidecar modes, and symlink rejection are executable.
- Qualification constants describe operating points, not format or fleet-size
  limits. Admission checks cover configured replay, metadata, record, database,
  staged-WAL, derived SHM and complete state-directory budgets. Held-out recovery
  preserves 5,003 records beyond the historical 4,000/4,096 assumptions, and
  smaller WAL policies exercise adaptive fragmentation/reclamation without
  changing the accepted work or exact recovery oracle.
- Future: version migration and rollback fixtures activate only when a second
  schema is actually proposed.

## Open questions

- Does a later product milestone justify a stable per-Run owner or another
  platform mechanism for live PTY handoff? Answered: no standing per-Run owner.
  [015](015-exec-in-place-upgrade-continuity.md) keeps live control across a
  _planned_ upgrade by carrying the master fd across an `execve`-in-place — the
  same process, so no metadata-named re-adoption and no broker — and
  [016](016-interrupted-run-derivation.md) records the boundary for creating a new Run
  from an explicit derivation plan rather than re-adopting its dead PTY.
  Crash-time live handoff
  and PID adoption remain unsupported.
- Which user-facing inspection or deletion command should manage durable history
  once a real client requires it?
- A future schema revision must decide migration and rollback before changing
  the exact-version fail-closed rule.

## Repository evidence

- `crates/ctxmux-protocol/src/lib.rs`: `RunId` and `RuntimeId`
- `crates/ctxmux-daemon/src/persistence.rs`: state-directory owner, single
  SQLite actor, recovery validation, reconciliation, and retention
- `crates/ctxmux-daemon/tests/persistence_recovery.rs`: real restart,
  stale-PID, corruption, lock, permissions, and replay-retention evidence
- `crates/ctxmux-daemon/src/lib.rs`: live and recovered `RunManager` paths
- `docs/protocol.md`: public persistent-mode lifetime boundary
- `docs/roadmap.md`: M3.5 recovery acceptance
- `fixtures/wrong-cases.json`: `PERSIST-01` and `PERSIST-02` with external
  `source_refs` for PID reuse and atomic-generation evidence and transfer limits
