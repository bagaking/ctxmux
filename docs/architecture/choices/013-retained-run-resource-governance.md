# 013 — Retained Run resource governance

- Status: accepted, implemented, and source-bound sustained qualification
  complete for the declared workload
- Scope: global retained Run admission, operation-key lifetime, collection,
  persistence replacement, and sustained-churn qualification

## Context

Before the memory-only owner in this decision, the daemon retained every
published Run and creation-key mapping for its whole epoch. Memory-only mode
now uses the bound below. Persistent mode uses the same Registry collection
owner and atomically removes the corresponding durable Run, replay, and exact
creation-key mapping before publishing its replacement.

The correction must preserve the stronger owner rules already established by
creation idempotency, unpublished-child rollback, attachment replay, native
child ownership, tmux target fencing, and persistence COMMIT. It must not turn
collection into a public Session, lease service, scheduler, TTL policy, or
general Backend framework.

## Decision

### One operational Run-record ceiling

The production daemon admits at most 128 retained or projected publication
records across native and tmux Runs. Persistent mode uses the same operational
Registry ceiling and couples its exact durable replacement to the Registry
ticket. A publication reservation counts before native spawn or tmux Control
child startup. Its own exact candidate replacement can make that ticket's
projected burden zero, but an uncommitted net release never becomes global
slack. Concurrent reservations therefore cannot publish a 129th Run. This is a
retained-record bound, not a claim that no transient owner can coexist with
those records.

The value 128 preserves the already qualified 1/32/128 live-Run matrix while
avoiding the false safety of reusing SQLite's historical 4,096-row format
envelope. The existing 4 MiB per-Run retention contract therefore derives a
512 MiB live memory-only `OutputLog` payload ceiling without another hot-path
byte quota. Up to 128 live native Runs retain one reader descriptor each; the
daemon-wide owner uses one 8 KiB stack buffer for each sequential ready read,
not one permanent buffer or thread per Run. Persistent mode uses the same live
bound while its 256 MiB durable SQLite
logical replay limit remains independently authoritative.

A retained Run always owns its replay and lifecycle truth, but it does not
retain an empty live-event ring for a viewer that does not exist. The existing
256-event Tokio broadcast ring is allocated on the first Attachment and
released by the last `AttachmentGuard`; concurrent Attachments still share the
same capacity, lag, and fan-out semantics. This removes an unobserved per-Run
allocation without inventing attachment admission or changing the replay
payload bound.

One daemon-private eight-slot overlap owner is shared by native Start/Fork,
tmux import, and transferred T-026 unpublished-child cleanup. A slot is held
from immediately before the physical child or Control client starts until
publication, complete rollback, or transferred cleanup proves full
Backend-local quiescence. Native cleanup requires child reap, closed control,
empty input accounting, and no additional output, lifecycle, input-drain, control,
or Run owner. The exact cleanup-held `Arc<Run>` and its Run-held
`Arc<NativeControlInner>` each retain a base strong count of one. Tmux requires
its corresponding Control child, reader, waiter, and writer completion receipt
to succeed and the cleanup entry to become the sole remaining `Arc<Run>` owner.
Thus at most eight not-yet-published or private-cleanup Runs can overlap the 128
Registry records. Their additional `OutputLog` payload is
bounded by 32 MiB, for a 544 MiB retained-plus-overlap payload bound. The native
owner uses an 8 KiB read buffer. A tmux reader separately bounds both its Control
line and command-block output at 1 MiB, and may briefly hold decoded
notification or output clones; those independent allocations must not be
collapsed into one false per-Run number. The 128-plus-eight owner bounds
multiply each existing local bound, while parser containers, transient clones,
Run metadata, and allocator overhead remain measured RSS rather than being
misrepresented as replay bytes.

SQLite may accept a schema-4 store containing up to 4,096 structurally
valid rows during fail-closed format validation. Bounded, restartable startup
transactions reconcile prior running rows to interrupted, evict the canonical
terminal prefix to 128, and finish serving-epoch publication before socket publication.
The 4,096 value is a legacy format-validation envelope, not a second live
capacity promise. The existing 64 MiB metadata, 256 MiB replay, database, WAL,
SHM, and state-directory limits remain unchanged.

T-026 unpublished-child cleanup is not a published Run record. It keeps its
exact-key fence while retaining one of the shared eight overlap slots above.
Collection neither counts that fence as reusable Registry capacity nor removes,
rewrites, or adopts it. A pre-COMMIT creation failure restores any collection
candidates even when the unpublished child transfers into that private cleanup
owner.

Tmux import takes the same eight-slot physical-overlap owner before its Registry
reservation and Control startup. A failed import releases the slot only after
its Control completion receipt succeeds and the cleanup entry is the sole
remaining `Arc<Run>` owner. Timeout, explicit cleanup failure, a still-held
worker Run owner, or a worker-setup path with no completion receipt transfers
the hidden Run and slot to the same bounded shutdown-visible owner. A failed or
missing receipt may conservatively retain that slot until daemon exit; bounded
fail-stop is preferred to inventing cleanup success or adding a worker
supervisor.

### Registry entry and lookup linearization

The Registry remains the single residency owner. Each entry contains the
`Arc<Run>`, its optional creation key, the persistence-SSOT logical
`metadata_bytes` value or no persistent bytes in memory-only mode, its terminal
collection order, and one state:

```text
Retained
  └─ Registry write lock ─> Collecting(ticket)
                               ├─ abort or pre-COMMIT failure -> Retained
                               └─ memory replace or COMMIT    -> Removed
```

The same `RegistryState` is the SSOT for a bounded reservation table. Each
ticket records the preallocated new `RunId`, keyed request identity or unkeyed
import kind, the new record and logical metadata bytes, exact candidate tickets
with their persistence-SSOT metadata-byte snapshots; each
`Collecting(ticket)` entry points back to that table. Candidates are exclusive
to one ticket. For each uncommitted ticket, the projected record burden is
`max(0, 1 - own_candidate_count)` and the projected persistent-metadata burden
is `max(0, new_metadata_bytes - own_candidate_metadata_bytes)`. Global
projection is the current Registry count or metadata, which still includes
every fenced candidate, plus the sum of those non-negative per-ticket burdens.
An uncommitted ticket's net release is therefore never credited to another
ticket. When that ticket publishes or COMMITs, the same Registry write replaces
its exact candidates, moves the real delta into current state, and removes its
projected burden, so every possible ticket completion order remains within both
ceilings without serializing ordinary launches. An optional multi-candidate
ticket marks the one allowed metadata-pressure prefix. Abort, publication,
and shutdown restore or consume this table under the Registry write lock.
Durable COMMIT occurs under the persistence owner without a Registry lock; its
`Committed` receipt later drives one infallible Registry consume. Physical-start
and durable COMMIT disposition remain with the creation owner and persistence
receipt respectively; the Registry does not duplicate those states. No atomic
counter, SQLite query, or publication thread maintains parallel reservation
truth.

A long-lived lookup pins the Run by cloning its `Arc` while the Registry lock
still observes `Retained`. The collector acquires the Registry write lock,
requires both that the Registry is the Run's only strong owner and that
Backend-local quiescence below has no independent strong owner, then changes
the exact entry to `Collecting(ticket)` in the same critical section. Native
eligibility therefore checks both `Arc<Run>` and `Arc<NativeControlInner>`:
the latter catches a blocked input worker that no longer owns the Run itself.
A lookup either clones first and makes the Run ineligible, or observes the
fence and cannot obtain a new owner. After the fence, closed native control
admits no new input worker; an already scheduled worker keeps the independent
strong count non-quiescent. Attachment count alone is not a pin because lookup
occurs before the attachment guard increments that count.

One-shot `list`, `status`, and matching creation-key resolution may copy
`RunInfo` under the Registry lock without retaining an `Arc`. A matching key
that observes a collection fence receives temporary unavailability; a
conflicting request remains `creation_conflict`. After exact removal, List
omits the Run, Status and Attach return `run_not_found`, a fresh-key Fork of the
old parent returns `run_not_found`, and the collected operation key may elect a
new physical Run.

Candidates are ordered by earliest terminal publication with `RunId` as the
tie-break. `RunManager` owns one daemon-private monotonic terminal ordinal and a
Run claims its value immediately before terminal state publication. Persistent
recovery sorts rows canonically by `terminal_at_ms`, `created_at_ms`, and
`RunId`, assigns fresh ordinals in that order, and starts later ordinals after
them; equal timestamps recover a canonical order, not an unknowable original
same-millisecond order. Lineage is immutable: collecting a parent never
cascades to a child or rewrites the child's dangling historical parent
reference. A retained child's matching Fork retry still resolves before any
current parent lookup.

### Admission and eligibility

Start validates and materializes its request, resolves its operation key, and
reserves publication capacity before physical spawn. A fresh Fork resolves a
matching child first, then pins and materializes its parent before reservation
and spawn. tmux import reserves before starting the Control child. When no
eligible candidate can satisfy record and persistent-metadata projections, the
request fails with `run_capacity` before either mutation boundary.

No Registry reservation or Collecting fence is held while waiting for the
physical-overlap owner. An ordinary Start/Fork or import first resolves and
materializes enough immutable request state to release any parent pin, then
waits cancellably for overlap admission. Active publication slots retain the
existing bounded wait; if all eight slots are retained by long-lived private
cleanup, an unrelated key/import fails `backend_unavailable` before spawn
instead of waiting behind owners with no completion deadline. Only an admitted
request creates its Registry reservation. Capacity rejection then releases the
unused overlap permit. Shutdown fences this admission, wakes cancellable
waiters, and reports retained cleanup permits through their existing owner
receipts.

The publication reservation is an RAII owner of its projected slot, every
candidate ticket, and the exact metadata delta. Its Drop path restores all
candidates on validation, spawn/setup, owner-registration, tmux readiness, panic, or
other exit that has not received a persistence COMMIT disposition. A memory
publication consumes the reservation in the same Registry write that removes
its exact candidates and inserts the new entry. A persistent COMMIT consumes
it through the equivalent exact replace even when the request later reports a
post-COMMIT error. No error path may forget, clone, or partially consume the
reservation.

The Registry reservation and physical-overlap permit are separate owners. It is
safe to restore a candidate and its projected slot after a pre-COMMIT failure;
it is not safe to release an overlap permit merely because stack unwinding or a
request error began. Before physical start, Drop releases the unused permit.
After a native child or tmux Control child starts, publication releases the
permit only after the new Registry entry consumes the projection. A rejected
launch releases it only after full Backend-local quiescence; otherwise the Run
and permit transfer together to the bounded private cleanup owner and remain
visible to shutdown. Native combines the daemon-wide cleanup owner's reap receipt with
closed control, empty input accounting, and no additional output, lifecycle,
input-drain, control, or Run owners beyond the cleanup-held Run and its
Run-held native control. Tmux uses its Control-child, reader, waiter, and writer
completion receipts. Neither path grows into descendant or process-tree
ownership.

A candidate must be exited or interrupted, unfenced, and owned only by the
Registry. Terminal state is necessary but not sufficient:

- native child cleanup has proven reap, the control phase is closed, queued input
  and byte accounting are zero, no input drain owns `NativeControlInner`, and
  the daemon-wide output and lifecycle registrations have closed;
- tmux writer, output reader, waiter, and Control child have all closed;
- the terminal event and any persistence finalize have completed;
- no attachment, control, fresh Fork, or other lookup pin remains;
- a recovered historical Run has no local incarnation control.

After those checks and the Registry fence linearize, a native candidate may
irreversibly compact its closed PTY master and writer before the replacement
spawn. The native cleanup owner has already proved reap, no input worker remains, and every
public terminal operation already rejects, so these descriptors have no
remaining public semantics and are not restored if the reservation later
aborts. The candidate's RunInfo, replay, lineage, key, persistence binding, and
Retained eligibility remain fully restorable. Tmux eligibility likewise
requires its Control descriptors and child to be closed before reservation.
This compaction prevents the first replacement from temporarily adding a new
PTY on top of 128 retained closed PTY owners and is part of the FD oracle; it
does not move or reconstruct an active control owner.

The 64 MiB persistent metadata limit may require more than one candidate even
when record count needs only one replacement. Each candidate still receives
one exact `Collecting(ticket)` fence; one publication reservation may own the
deterministic eligible prefix required by its own projected count and metadata
totals. Extra candidate bytes can make that ticket self-funding, but do not
become global slack until its exact COMMIT removes them. This preserves the
task's exact-candidate fence invariant without pretending a single small row
can always free enough metadata. At most one multi-candidate metadata-pressure
reservation exists at a time; other metadata-pressure requests fail
temporarily instead of introducing a second reservation actor or serializing
all ordinary launches.

### Persistent exact replacement

Decision 009 freezes an 8 MiB per-transaction WAL ceiling and a 16 MiB total
WAL ceiling. Logical replay bytes are not a page-cost proof: cascade deletes,
chunk cardinality, overflow pages, indexes, B-tree rebalancing, freelist and
pointer-map updates can all change the number of dirty SQLite pages. The
persistence owner therefore admits a live replacement from the exact transaction's
cache-resident page set, not from a payload estimate.

The single persistence connection uses a scoped `cache_spill=OFF` guard only
while staging this transaction and restores the prior setting on every commit,
rollback, and error path. Immediately before an exact replacement it releases
unpinned clean cache memory, requires
a successful `TRUNCATE` checkpoint and a zero-length WAL, resets the connection
cache-write and cache-spill counters, and begins one write transaction. It
validates and deletes the Registry-selected exact candidates, including
cascading replay, and inserts the new `Running` row with `pid = NULL`. The
transaction remains uncommitted and the actor remains its sole owner. No child
exists yet.

With spill disabled, SQLite cannot write a dirty page in the middle of that
transaction. After every statement and cursor is finalized, the actor requires
all of the following before it grants physical-launch admission:

- the WAL file remains zero length;
- `SQLITE_DBSTATUS_CACHE_WRITE` and `SQLITE_DBSTATUS_CACHE_SPILL` remain zero;
- `SQLITE_DBSTATUS_CACHE_USED` succeeds on the non-shared single connection;
  and
- the conservative charge below fits the 8 MiB transaction ceiling.

For SQLite page size `P = 4096`, WAL frame header `H = 24`, WAL file header
`W = 32`, and reported cache bytes `M`, the charge is:

```text
cached_page_upper = ceil(M / P)
transaction_charge = W + cached_page_upper * (P + H)
```

This is deliberately an overestimate. In the pinned bundled SQLite, pager
cache accounting is `cached_pages * (P + positive per-page overhead) +
positive pager overhead`, so `ceil(M / P)` is no smaller than the number of
cached pages. Every dirty page is one of those cached pages. With spill off and
no SQL after admission, COMMIT appends at most one frame for each dirty page;
the commit marker is carried by the final page frame. Clean/schema pages and
allocator overhead only make the charge more conservative. At 4 KiB pages the
8 MiB ceiling admits the WAL header plus at most 2,036 frames. Because
every staged replacement starts from a zero-length WAL, the separate 16 MiB
total ceiling is also preserved.

A changed WAL, an unsupported status counter, a nonzero write or spill count,
an over-budget charge, or another pre-COMMIT condition returns `run_capacity`
before durable mutation or physical launch only after rollback is proven. It
does not poison the persistence actor. Failed rollback, unknown connection
state, or failed old-or-new probing is `CommitUnknown`: it retains every
Registry fence, stops current-incarnation admission, and never resumes the
actor. An early logical lower-bound check may reject obviously oversized work
before constructing its page cache, but it cannot admit a request by itself.

Admission returns one affine staged-start owner while the persistence actor
keeps that exact transaction open. The owner either aborts and proves rollback,
or, after native spawn succeeds, requests COMMIT without issuing further SQL.
The actor is intentionally serialized for this short spawn boundary; ordinary
append/finalize commands remain in the existing bounded queue. This avoids a
parallel WAL-charge ledger, a durable reservation, and a general transaction
API.

The durable `Running` row intentionally stores no PID. The live PID remains a
fact of the current child-handle owner. A successful terminal finalize writes
the actual historical PID together with final replay and terminal state in one
transaction. A crash while the row is still `Running` already reconciles it to
`Interrupted` with `pid = NULL`, so no PID becomes restart or signalling
authority.

SQLite no longer chooses a live eviction candidate independently. The Registry
passes the persistence actor an exact terminal candidate list containing each
`RunId` and byte-exact BINARY creation key. One SQLite transaction:

1. verifies every exact candidate and terminal state;
2. deletes those rows and their cascading replay;
3. inserts the new running Run and creation key;
4. verifies record, metadata, replay, and cache-resident page admission; and
5. commits.

The durable disposition must distinguish `NotCommitted(error)`,
`Committed { post_commit_error }`, and `CommitUnknown(error)`. A monotonic
receipt is created as `Pending` before actor enqueue. The actor may decide it
only once as `NotCommitted` or `Committed` and records that decision before
reply delivery; actor or reply-owner loss while it remains `Pending` resolves
to `CommitUnknown`, never to rollback by default. Before COMMIT, SQLite rollback
and Registry fence restoration leave every candidate present; ordinary capacity
rejection does not poison the actor. After COMMIT, including a later
physical-file postcheck or reply-delivery failure, the Registry performs one
exact in-memory replacement with no I/O, await, or fallible result. A retry
therefore resolves the newly committed Run rather than launching again.

A `COMMIT` call error is not automatically pre-COMMIT. After rollback handling,
the persistence owner probes the exact old-or-new unit: all old candidates and
no new row is `NotCommitted`; all exact candidates absent and the new row
present is `Committed`; a hybrid result, failed rollback, or failed probe is
`CommitUnknown`. Unknown disposition restores neither candidates nor creation
admission. The Registry reservation and its candidate/new-key fences transfer
to a daemon fail-stop owner for the rest of the incarnation, while the pending
physical Run and overlap slot use the existing full-quiescence cleanup transfer.
The daemon then stops socket admission so SQLite recovery after restart is the
only durable authority. Unknown disposition must never reopen a key or launch a
replacement child in the same epoch.

A daemon crash before COMMIT recovers all old candidates and no new row. A
crash after COMMIT but before in-memory replacement recovers only the new
Run/key. No collecting ticket, pending lease, or tombstone is durable.

Exact replacement does not run an uncharged post-COMMIT incremental vacuum.
Deleted pages remain reusable inside the already frozen 384 MiB main-database
ceiling. WAL and SHM remain under their independent ceilings, and physical
validation after COMMIT cannot reclassify the durable old-or-new decision.
Startup normalization uses the same spill-disabled page admission in bounded,
restartable transactions before socket publication. An over-budget batch is
reduced deterministically; an individually unprovable replacement fails startup
closed rather than exposing a partially normalized store or returning a public
`run_capacity`.

The proof is source-bound to bundled SQLite 3.53.2 through rusqlite 0.40.2. The
daemon crate remains `unsafe_code = "forbid"`; one private, audited FFI leaf may
expose only the safe connection-status observation needed here. A SQLite or
rusqlite upgrade must re-run the cache-accounting, no-spill, rollback, and final
WAL-frame fixture before the dependency can change.

This implementation supersedes decision 009 only for live Registry
capacity, operational startup normalization, and who selects rows for new-Run
replacement. Decision 009 remains authoritative for schema-4 validation up to
the legacy 4,096-row envelope, the 64 MiB metadata and 256 MiB durable replay
limits, file ceilings, recovery class, and SQLite durability assumptions.

### Public error and protocol generation

Global Registry admission with no eligible projected slot uses the narrow
public error `run_capacity`. `backend_unavailable` remains correct for an
existing T-026 exact-key cleanup fence, a candidate already fenced by another
reservation, daemon shutdown, a failed worker boundary, or an unavailable
external Backend; those cases have an owner but cannot currently serve the
request. `control_backpressure` remains limited to a live control queue. Adding
`run_capacity` is an incompatible schema change, so the implementation advances
the protocol to generation 6 and updates Rust schema/codegen, TypeScript
runtime validation, first-party clients, wrong-case tests, and protocol
documentation together.

The exact error matrix is:

| Situation                                                              | Result                                           |
| ---------------------------------------------------------------------- | ------------------------------------------------ |
| retained key, matching Start/Fork request                              | original `RunInfo`; no admission                 |
| retained or Collecting key, conflicting request                        | `creation_conflict`                              |
| Collecting key, matching request                                       | `backend_unavailable`                            |
| Collecting Run, List or Status                                         | included/current `RunInfo`, copied without a pin |
| Collecting Run, Attach/Input/Resize/Stop/fresh Fork                    | `backend_unavailable` before mutation            |
| no eligible candidate for the projected count or bytes                 | `run_capacity`                                   |
| another multi-candidate metadata reservation owns the exclusive prefix | `backend_unavailable`                            |
| T-026 exact-key fence or daemon shutdown                               | `backend_unavailable`                            |
| committed/removed Run through List                                     | omitted                                          |
| committed/removed Run through Status/Attach/control/fresh Fork         | `run_not_found`                                  |
| old operation key after its prior Run is fully absent                  | unbound; ordinary creation election              |

Copy-only List and Status linearize before exact removal and may be followed by
`run_not_found`; they neither delay collection nor create a hidden owner. Every
operation that needs the Run after releasing the Registry lock must pin and
therefore fails while Collecting.

## Production-scale qualification contract

The first Tracker task proposed for this contract was superseded before its
pressure harness and daemon-private metrics sink were built. The reviewed
machine contract remained frozen, and T-005 later adopted it as the current
source-bound qualification truth instead of inventing a second workload.

T-033 remains the ordinary reduced-capacity correctness oracle: memory-only and
persistent modes each fill four records and cross three complete four-Run
turnover windows, with persistent restart after window two. T-005 adds the
production 128-record turnover, pressure, restart, resource, and ordinary-soak
evidence below. Passing it qualifies only the declared workload and owner
boundaries; it does not create a general daemon RSS or attachment-fan-out
guarantee.

PR tests use a lower private ceiling to cross at least three collection windows
without claiming production-scale evidence. The tracked
`reliability-gc-contract.json` is the machine-readable SSOT for the canonical
seed, helper identity, payloads, concurrency, replay-pressure phase, sampling,
owner and resource ceilings, and profile time budgets. It is frozen before GC
implementation or observation; the harness and policy must consume and validate
it instead of maintaining another numeric table. The contract keeps the 1,800
second nightly soak and reserves separate non-pressure headroom while raising
the nightly supervisor budget to contain the new pressure phase. A timeout may
not be repaired by shrinking the workload or changing a ceiling.

In both memory-only and persistent modes the bounded-churn phase first fills
128 Runs, then completes three full 128-Run turnover windows: at least 512
successful lifecycles per mode. The persistent daemon restarts immediately
after the second turnover window, then proves the third window against
recovered state.

Canonical nightly fixes seed `226004`; the canonical command rejects a seed
override. Every lifecycle uses concurrency eight and one exact `RunSpec`:
`program = process.execPath`, args name the checked-in
`scripts/reliability-gc-child.mjs` plus seed, mode, and lifecycle index, cwd is
the clean repository root, env additions and declared inputs are empty, and
size is 80 columns by 24 rows. Provenance records the helper SHA-256. The helper
computes the lowercase ASCII hex encoding of
`SHA-256(utf8("226004:<mode>:<index>"))` and writes that 64-byte string 64
times, with no newline, for exactly 4,096 PTY-stable bytes before exiting zero.
Bounded phase mode is exactly `memory_only` or `persistent`. Indices are
0..127 for fill, 128..255, 256..383, and 384..511 for the three turnover
windows. The retained key is `gc:<mode>:<index>:<digest-hex>` under the public
length and character rules.

A lifecycle is complete only after public terminal state, no direct child,
exact 4,096-byte replay and byte cursor, and, in persistent mode,
`durable_output_bytes == latest_output_bytes`. Each window uses the fixed seed and schedule;
failures retain the first failing index.

The restart oracle hashes a canonically RunId-sorted array of correlated tuples,
not separately permutable sets. Each tuple contains RunId, exact key or unkeyed
marker, terminal state, lineage, first/latest/durable/truncated byte cursors, and
the ordered `(start_byte, end_byte, length, SHA-256(data))` replay digest. The same tuple
digest must appear before shutdown and after restart. Before shutdown and again
after restart, all 128 exact keys are retried with their canonical requests;
every retry returns the tuple's same RunId and changes neither process count nor
record count. The daemon-private qualification sink also exposes one monotonic
`physical_starts_total` from the creation owner; every retry wave must leave it
unchanged, so a duplicate that starts and exits between process samples cannot
escape the oracle. The harness-owned key field is not accepted as durable proof
without this public retry and cumulative-start evidence.
The counter starts at zero for each recorded daemon incarnation. Fresh fill and
replacement waves must advance it by their exact admitted physical-start count;
retry waves advance it by zero. Restart may reset the counter only while the
receipt records the new daemon epoch, after which recovered-key retries again
leave it at zero.

Each turnover performs exactly 128 replacements and leaves exactly 128 retained
records. For this one-small-record workload, each window records the exact
physical-start, retry, candidate-selection, candidate-evaluation, candidate-fence,
and replacement deltas. Candidate evaluation is bounded by 128 records per
replacement. The final owner snapshot requires 128 retained Runs and keys while
creation flights, publication reservations, collection tickets, overlap and
cleanup owners, children, readers, waiters, input drains, attachments, and tmux
owners are all zero. Earlier proposals for per-window latency percentiles,
growth comparisons, and periodic turnover CPU/RSS sampling are superseded and
are not part of the T-005 claim.

The supplemental replay-pressure phase does not masquerade as another complete
turnover. In each mode it fills 128 terminal native Runs with the contract's
exact 4 MiB ASCII payload, proves the 512 MiB Registry-owned live replay total,
and verifies all public replay bytes and correlated digests in fixed batches of
eight. It then publishes exactly one eight-wide replacement wave, settles every
owner, and repeats the count, byte, digest, and resource oracles. This combines
the maximum retained payload with the maximum physical publication concurrency;
the three-window small-record phase remains the full turnover proof.

Persistent pressure has two explicit domains. Before restart, same-epoch live
`OutputLog` payload is exactly 512 MiB and `durable_output_bytes == latest_output_bytes` even
though SQLite globally retains at most 256 MiB. After restart, the recovered
durable aggregate must lie in the exact native-chunk interval frozen by the
machine contract, every replay is the expected suffix, and a moved oldest
cursor is truncated. Live and recovered tuple digests are intentionally
different domains; the harness must not demand an impossible restart-stable
512 MiB durable set.

The pressure receipt enforces the machine contract's absolute RSS ceilings,
25 ms sampling with bounded gaps, CPU limits, descriptor/thread deltas, queued
persistence bytes, attachment batches, and owner counts. The RSS formula reuses
the pre-observation 3/2 multiplier and 4 MiB quantum from the frozen resource
policy without modifying `reliability-budgets.json`. These are canonical
workload ceilings over Registry-owned payload and named transient owners, not a
hard bound on every daemon allocation: arbitrary attachment fan-out remains
unbounded, and byte retention alone does not provide a small chunk-cardinality
bound. Those broader admission decisions remain outside the shipped Kernel
claim and need separately reviewed work.
Memory peak accounts for retained payload plus the larger of publication
overlap or the fixed replay-clone batch. Persistent peak conservatively accounts
for retained payload, publication overlap, full catch-up/finalize snapshots,
the ordinary append queue, and actor working clones at once. Recovered peak
accounts for durable payload plus the replay-clone batch. No undocumented
ordering assumption subtracts a named owner from those formulas.

The source-bound receipt preserves raw 25 ms RSS samples for replay pressure,
complete Run/key/replay tuples, cumulative physical starts, and final owner
snapshots for bounded churn and pressure. Exact internal counts come from one
daemon-private qualification sink backed directly by the owning Registry,
publication-overlap, T-026 cleanup, reader, waiter, input-drain, and tmux state;
it is enabled only by an inherited harness-owned descriptor, emits counts and
no keys or metadata, and is not a socket request or public admin API. The daemon
marks the descriptor close-on-exec before any Run spawn, uses bounded
non-blocking writes, and never lets a disconnected or backpressured sink block
or alter runtime ownership. The harness drains continuously and fails
qualification on any missing, dropped, disconnected, or malformed snapshot.
Every owner transition also updates daemon-private high-water and cumulative
counters; final boundary snapshots prove quiescent owner convergence.

The ordinary 1,800 second memory-only soak is not shortened after these bounded
workloads. It retains exactly eight live `activeSpec()` Runs, writes 4 KiB to
each Run once per approximately one-second cycle, resizes all Runs every 16
cycles, and opens and closes one attachment every 64 cycles. It does not
perform retained-Run replacement or reuse the bounded-churn helper topology.
After the deadline it proves the declared per-Run retention bound, stops and
settles all eight Runs, and requires zero live children and attachments with no
transient cleanup-thread slope. Final T-005 completion evidence runs
`scripts/check-reliability.sh --profile nightly`; the default smoke command is
not canonical evidence. Historical observe receipts, hashes, measurement
identities, and the numeric ceilings in `reliability-budgets.json` are never
rewritten or rebaselined. The pressure ceilings and raised nightly time budget
live only in the separately source-bound GC contract.

## Quality attributes and invariants

- Admission, not later rollback, prevents an over-capacity physical launch.
- Lookup-to-pin and Retained-to-Collecting share one Registry linearization
  boundary.
- Registry, key mapping, and persistence remove one exact ownership unit.
- COMMIT is the only durable point of no return and cannot be reclassified by a
  later check.
- Running, pinned, attached, incompletely finalized, or locally controlled Runs
  are never collected.
- Output stays on the existing per-Run hot path; no second live byte quota or
  byte-accounting lock is added.
- The policy adds no Agent/session metadata, scheduler, public delete API,
  Backend hierarchy, TTL compatibility layer, or process-tree promise.

## Alternatives

- Keeping every exited Run preserves history but leaves CPU, RSS, descriptor,
  replay, and key state unbounded.
- Reusing 4,096 as the Registry ceiling yields a theoretical 16 GiB memory
  replay bound and thousands of retained descriptors; it is not a credible
  runtime budget.
- Using attachment count as the only pin leaves a collection race between
  Registry lookup and attachment-guard construction.
- Letting SQLite choose eviction independently can delete a row whose live Run
  is pinned or leave Registry and durable key truth divergent.
- A background TTL/LRU actor, public delete API, durable lease, or generic
  Backend collector adds policy and state not required by admission-triggered
  exact replacement.
- Rejecting metadata only after spawn is made rollback-safe by T-026 but fails
  the stronger pre-mutation resource admission goal.

## Known constraints

Collection is admission-triggered; history below 128 is retained and no age or
wall-clock expiration is promised. The 128 ceiling bounds Registry records and
their 512 MiB replay payload; the shared eight-slot overlap owner produces the
separate 544 MiB retained-plus-overlap payload bound above. Neither value bounds
descendant processes from legacy direct-child Stop semantics. Generation 9
introduced complete POSIX-session Stop; a session-escaping
descendant remains deliberately outside the declared owner scope.
The payload ceiling is not a universal daemon RSS claim: extreme short reads
can amplify chunk/Vec metadata, and public attachments clone replay without a
global attachment quota. The canonical pressure workload measures and caps its
declared chunking and eight-wide replay surface only. General attachment
admission, chunk-cardinality policy, manual history deletion, secure erasure,
and arbitrary Backend event replay remain separate decisions.

## Wrong-case corpus

- `GC-02`: treating a pre-COMMIT delete as complete loses history when the
  transaction rolls back; treating a post-COMMIT error as rollback permits a
  duplicate child.

`GC-02` is the source-backed transfer retained in the normalized wrong-case
corpus. The review-derived obligations below come from this accepted owner
contract rather than an external source; they remain in the task and fixture
mapping without masquerading as additional normalized corpus cases.

## Implementation race matrix

- Checking terminal state without an atomic pin boundary removes a Run after
  `get` but before attachment count increments.
- SQLite-selected eviction removes a different row than the Registry fence and
  leaves an immortal or misbound creation key.
- Counting only published entries, or lending one uncommitted ticket's net
  candidate release to another ticket, lets concurrent reservations cross a
  ceiling under an adverse publication order.
- Terminal state can precede output-reader, input-drain, tmux Control, or
  persistence-owner quiescence; dropping the Run then hides a live owner.

These obligations become active only with the implementation and mapped public
or deterministic-owner fixtures.

## Fixture mapping

- Shipped/T-028: memory-only pre-spawn admission, live and pinned rejection,
  lookup-versus-fence, exact key removal and reuse, abort restoration, parent
  materialization, dangling lineage, `run_not_found`, and tmux pre-Control
  admission fixtures.
- Existing native-control and tmux completion fixtures prove the Backend-local
  quiescence oracles reused by memory-only eligibility.
- Implemented/T-029: production exact replacement supports the deterministic
  metadata-pressure candidate prefix; focused fixtures cover one exact
  candidate's identity, pre-COMMIT cleanup, persistence-finalize eligibility,
  same-epoch key replacement, restart convergence, bounded startup
  normalization, real process crash immediately before/after ordinary COMMIT,
  and actor-routed old/new/hybrid COMMIT-error classification. A separate
  multi-candidate crash oracle remains future hardening rather than shipped
  evidence. A separate process also proves a
  committed startup-normalization failure returns before socket publication and
  the next open resumes to the canonical 128 records. T-029 closed with its
  source-bound Task Gate passing.
- T-028: reduced-ceiling concurrent reservations exercise the 127-to-129
  projection invariant under reverse publication order without a production
  process census.
- T-033: one reduced-capacity ordinary oracle crosses three complete turnover
  windows in memory-only and persistent modes, restarts persistent mode after
  window two, and proves exact retained identity, replay, key retry, and
  current-incarnation control boundaries without new qualification machinery.
- T-005: the source-bound canonical nightly fills and turns over 128 records in
  each mode, restarts persistent state, verifies maximum replay pressure and
  the ordinary soak, and fails closed on telemetry or frozen-identity drift.

## Repository evidence

- `crates/ctxmux-daemon/src/creation.rs`: Registry, creation keys, publication,
  and T-026 private cleanup owner
- `crates/ctxmux-daemon/src/lib.rs`: Start, Fork, import, Run lifecycle, and
  attachment lookup paths
- `crates/ctxmux-daemon/src/native_control.rs`: native child, reap, PTY, and
  input-drain ownership
- `crates/ctxmux-daemon/src/persistence.rs`: schema-4 validation, SQLite actor,
  retention, and COMMIT disposition
- `scripts/reliability-qualification.ts`: source-bound resource and soak
  receipts
- `reliability-gc-contract.json`: canonical T-005 workload, helper identity,
  resource ceilings, and time budgets
- `scripts/reliability-gc-child.mjs`: PTY-stable deterministic payload producer
