# Round 25 — the command was in the right queue, in the wrong place

`start` under a loud fleet costs 92.6 ms after R24. Of that, **1.8 ms is work
and 93.0 ms is standing in line** — and this round shows the line is not one the
command was ever short of a place in.

## What R24 left, and what it got half right

R24 shipped `PERSISTENCE_QUEUE_CAPACITY` 64 → 16 and attributed the win to
`wait = depth ÷ drain_rate`. The number and the direction were right. The
mechanism was half-formed, and reading the send path afterwards produced a
competing story worth taking seriously: lifecycle verbs use a **blocking**
`send()` against a bounded `sync_channel(16)` while eight reactor threads push
with `try_send`. On that reading the command never reaches the queue at all —
it starves at *admission*, losing every freed slot to a reactor.

That is not a pedantic distinction. Kafka's KIP-291 separates exactly these two
starvation modes because **they take different fixes**, and is explicit that
prioritising at dequeue cannot help the admission mode.

So it was measured instead of argued.

## The measurement

A probe build stamping `enqueued_at` on the lifecycle command and printing three
numbers per `finalize`: time blocked inside `send()` (SEG-A, admission), time
from enqueue to dequeue (SEG-B, queue position), and the caller's whole wall
time. cn3, 3 rounds × 24 `stop`s per cell, 72 samples per cell.

| cell | SEG-A (admission) | SEG-B (queued) | on-actor | total |
|---|---|---|---|---|
| c=0 | 7 µs (0.3 %) | 17 µs (0.8 %) | 2047 µs (98.8 %) | 2071 µs |
| **c=8** | **1 µs (0.0 %)** | **93035 µs (98.1 %)** | 1826 µs (1.9 %) | 94862 µs |

SEG-B scales **5472×** from a quiet fleet to a loud one. SEG-A does not move and
is one microsecond.

**My pre-registered prediction was that SEG-A would dominate. It is falsified.**
Slots are never scarce; eight reactors do not starve one lifecycle sender. The
wait is plain FIFO position — KIP-291 mode (2), which is the *cheap* branch,
because the command is already admitted and merely standing in the wrong place.

This also settles R24's mechanism properly: the win came from having fewer
predecessors to wait out, not from faster slot turnover.

### The instrument failed first

The first probe was patched onto `Persistence::barrier` and produced six clean
cells of zero — because `stop` never calls `barrier()`; it publishes through
`finalize`. A probe that never fires reads exactly like a mechanism that is not
there, which is the R23 "a knob that never engaged reports flat" failure
repeating. The harness now FATALs when the probe fires fewer than 100 times.

## Two directions the research killed before any code

Both were the plan going in, and both are ruled out on primary sources rather
than taste:

- **A second writing connection to the same SQLite file.** WAL permits exactly
  one writer, enforced by an exclusive `WAL_WRITE_LOCK` in the `-shm` wal-index
  held while frames are appended (sqlite.org/wal.html, /walformat.html). The
  lifecycle `BEGIN IMMEDIATE` would block on that lock for the same interval it
  now spends in our FIFO — the wait **relocates into SQLite's lock layer**, and
  we additionally own `SQLITE_BUSY` retries and lose FIFO ordering. Even
  `BEGIN CONCURRENT`, an off-trunk branch absent from stock rusqlite, "still
  serializes COMMIT commands" — and the commit fsync is precisely what we wait
  behind.
- **A second SQLite file for lifecycle metadata.** In WAL mode a transaction
  spanning attached databases is atomic per-file only, never across the set
  (sqlite.org/lang_attach.html). That buys a crash-consistency obligation —
  startup reconciliation — in exchange for isolation the cheap fix does not need.

## The change

A second `mpsc` channel for lifecycle commands, drained with priority at the
actor's existing dequeue point. `Finalize`, `StageStart` and `RemoveTerminal`
take it. The actor checks it ahead of both its `pending` deque and the append
channel; a `LifecycleWake` token on the append channel breaks it out of a
blocking `recv()` when it is parked.

The wake is `try_send`, never `send`. A blocking wake would put the lifecycle
caller straight back behind the append backlog the channel exists to escape —
the bug reintroduced through the fix. A refused wake is provably harmless:
`try_send` fails only when the append channel is full, and a full append channel
means the actor is not parked, so the loop's next turn finds the command anyway.

**`barrier()` deliberately stays on the append lane.** A barrier's entire
meaning is its FIFO position: it returns when every append enqueued before it
has committed. Overtaking those appends would make it return early and tell
`exec`-in-place that a replay is durable when it is not — the replay gap the
call exists to prevent. The lifecycle lane is for commands that carry their own
bytes; a barrier carries none and is pure ordering.

**Batch size is deliberately unchanged.** Shrinking batches to cut latency is
fsync amplification, and this project has measured its teeth (a zero-length WAL
costs the next commit 1.13 ms; folding costs ~1.6 ms/MiB). The lifecycle command
slips *between* full-size batches; it never makes one smaller.

## Why this does not re-trip the R21 latch

R21 established that lifecycle commands cannot jump the queue, and the reason
was a latch, not a preference. Three edges were checked against the source
rather than reasoned about:

- **`Finalize` is self-sufficient against its own queued appends.**
  `finalize_with_shutdown` begins with `missing_chunks(id, replay)`, which
  filters the finalize's own replay against the database's `durable_output_bytes`
  and commits whatever is not yet durable. It carries its Run's bytes with it. A
  same-Run append still queued behind it is not lost: those bytes are in the
  finalize's replay, and when the append is later dequeued its chunks fail the
  `end_byte > durable_head` test and are skipped.
- **`StageStart` opens a record for a Run that has no appends yet.** Its only
  cross-Run dependency is the eviction candidate set, and a still-running
  candidate is rejected fatally by `validate_exact_candidates` — so a candidate
  is by construction already terminal, its `Finalize` executed rather than
  merely queued.
- **Lifecycle stays FIFO among itself.** One lifecycle channel drained in order
  preserves every lifecycle-vs-lifecycle edge the latch protects. There are no
  priorities *within* lifecycle.

### The hazard is the reverse of the obvious one

The easy worry is that overtaken appends are **lost**. They are not, for the
reason above. The real hazard is that they are **rejected**: `append_replay`
fails an append whose chunks pass `durable_head` once the Run is no longer
`running`, and that error goes through `remember_failure`, which latches
persistence off for every Run in the daemon. Before the lane a finalize sat
behind its Run's appends, so they always landed while the Run was still running
and the guard could not fire. Now it overtakes them.

`a_finalize_that_overtakes_its_runs_appends_does_not_latch` reproduces exactly
that interleaving: it wedges the actor inside one append, sends the finalize
while it is wedged so the finalize is waiting on the lifecycle lane, queues two
more appends behind it, releases, and asserts the daemon did not latch and that
a reopen recovers all the bytes.

### That fixture was blind twice before it was not

Version 1 passed with the lane and passed without it. An ordering probe showed
why: only one append was ever dequeued — the wedged batch swallowed all three,
so no overtake occurred. Version 2 produced a real `append → finalize → append`
order, and the lane-removal mutation *still* did not redden it, because on the
append lane the end state is identical: the finalize is simply dequeued last.

So the fixture was blind-tested against the **defect** rather than against the
lane. Breaking the terminal-append guard made it **RED**, which is the property
that matters: it catches what it exists for.

## Result

12 paired rounds, three arms (BASE-A, a byte-identical BASE-B as the A/A
control, CAND), order rotated per round, four shapes × four verbs, adjudicated
under Holm-Bonferroni across the 16 cells plus the control's own empirical floor.

| shape | verb | BASE | CAND | ratio |
|---|---|---|---|---|
| c=1 | start | 8.67 ms | 6.87 ms | **1.26× faster** |
| c=1 | stop | 9.53 ms | 7.98 ms | **1.19× faster** |
| c=1 | remove | 6.51 ms | 4.94 ms | **1.32× faster** |
| c=2 | start | 10.97 ms | 8.29 ms | **1.32× faster** |
| c=2 | stop | 10.95 ms | 8.68 ms | **1.26× faster** |
| c=2 | remove | 7.34 ms | 5.50 ms | 1.34× (below this run's floor) |
| **c=8** | **start** | **89.41 ms** | **12.07 ms** | **7.41× faster** |
| **c=8** | **stop** | **94.02 ms** | **14.37 ms** | **6.54× faster** |
| **c=8** | **remove** | **103.48 ms** | **7.06 ms** | **14.66× faster** |

Every counted win is 12/12 on the paired sign test (p_holm = 0.008). Nothing
regressed: all four `list` cells and all four `c=0` cells are flat, retained
bytes are within 1.3 %, and no `stop_refused`, `remove_failed` or short
`started` count appeared in any round.

`c=2` `remove` is a real 1.34× improvement that the gate declines to count: the
A/A control was unusually noisy at that one cell this run (floor 0.101 against a
0.012 median), so the effect does not clear the harness's own resolution there.
It is left uncounted rather than argued up — a floor is only worth having if it
also rules against you.

The wins hold at every position within the round (c=8 `start`: 0.133× / 0.133× /
0.137× at positions 0/1/2), so this is not cn3's second-arm bias leaking through
the rotation.

### This was measured twice, because the first binary was not the shipped one

The first 12-round batch ran before a four-edit clippy cleanup — a doc backtick,
`Result<(), SendError>` → `bool` on one private helper, and `continue` → `{}` on
a trailing match arm. All four read as semantically inert. **They produced a
different binary**, confirmed against a determinism control: the same source
rebuilt twice in the same directory is byte-identical, so the difference is real
codegen rather than build noise. The gate was therefore re-run from scratch on
the binary actually being committed, and the table above is that second batch.

The first batch agreed within noise (c=8 start 7.80×, stop 6.21×, remove 12.17×;
same nine cells faster, same zero regressions), so the round has two independent
12-round replications rather than one.

### Against the pre-registration

Prediction 1 said c=8 `stop` would drop below 10 ms, with "above 20 ms means the
batch boundary is not where I think it is". It landed at **14.37 ms** — inside
the stated tolerance but above the floor I predicted. The gap is the residual:
the command still waits out the batch already in flight, and at 1 MiB per
transaction that is several milliseconds by itself. Predictions 2 (`start` and
`remove` improve similarly), 3 (c=0 unchanged), 4 and 5 (throughput and retained
bytes flat) all held.

## What this round still leaves

c=8 `start` is 12.07 ms against tmux's 8.4–10.2 ms. The 10× gap that stood at
the start of this round is gone; what remains is roughly parity, and the next
round has to be priced against tmux directly rather than against our own
baseline.
