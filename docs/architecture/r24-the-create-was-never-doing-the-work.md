# Round 24 — the create was never doing the work

`start` under a loud fleet costs 290 ms. Of that, **7.3 ms is work and 283 ms is
waiting in line.** Every round from R19 to R23 optimised the 7.3 ms.

## What the previous three rounds were actually aiming at

R19 through R23 all targeted the persistence path's own cost: fold cadence, WAL
amplification, row coalescing, fsync batching, retention ceilings. Two shipped,
three were rejected. None of them asked what fraction of a create the
persistence work even _is_.

An earlier segmentation (claim c013) had measured it at a 28 ms client median:
queue 16.6 ms, fold 4.3 ms, body 5.3 ms — the queue already 81 % of the premium.
That number was read as "the queue is one of three terms". It should have been
read as "the other two terms are bounded and this one is not".

## The measurement

A probe build with two `eprintln!` timers — one around `fold_wal_below_ceiling`,
one around the whole `Command::StageStart` arm from dequeue — plus the append
queue depth at dequeue. cn3, 3 rounds × 64 creates, 8 chatty Runs at ~174 MiB/s.
The segments close against the client's wall time in every cell, which is the
precondition for reading any of them.

|                                    | c=0 control       | c=8 loud              |
| ---------------------------------- | ----------------- | --------------------- |
| client wall                        | 6.5 ms            | **290.2 ms**          |
| on-actor work                      | 2.9 ms (45 %)     | **7.3 ms (2.5 %)**    |
| — fold, `wal_checkpoint(TRUNCATE)` | 0.0 ms, fired 0 % | **0.0 ms, fired 7 %** |
| — insert + commit                  | 2.9 ms            | 7.3 ms                |
| residual (queue + IPC)             | 3.6 ms (55 %)     | **282.9 ms (97.5 %)** |
| append queue depth at dequeue      | 0                 | **64 = capacity**     |

tmux on the same host, same batch, same cell: 8.4 → 10.2 ms.

**Our actual work already beats tmux.** 7.3 ms against 8.4-10.2 ms. The entire
40× deficit is queue wait behind other Runs' bytes.

## Three things this refutes

**The fold is not the cost.** A research pass nominated
`PRAGMA wal_checkpoint(TRUNCATE)` as the mechanism, reasoning that TRUNCATE
inherits RESTART's blocking semantics and that its cost is O(bytes since the
last fold), which scales with both fleet size and output — matching the
"requires BOTH" fingerprint exactly. It is a good argument and its cost model is
right: when the fold fires it costs 16-42 ms on an ~8.5 MB WAL, ≈2.4 ms/MiB,
consistent with R19's measured 1.6 ms/MiB. But it fires on **7 % of creates**,
because the 8 MiB ceiling bounds how much can accumulate. Expected contribution:
≈1.4 ms of 290 ms. A correct mechanism, correctly priced, attached to the wrong
share.

**My own previous probe refuted nothing.** It reported two findings, and both
instruments were measuring the wrong thing:

- _"The create is CPU-bound, not sleeping — 149 % of wall in CPU."_
  `/proc/<pid>/stat` is process-wide. During those 358 ms, eight PTY reactor
  threads were running at ~250k reads/s. 149 % is what a busy _daemon_ looks
  like. The create itself was neither sleeping nor computing — it was not
  scheduled at all.
- _"Latency is insensitive to WAL size — 0 kb and 8875 kb both give 358 ms."_
  The WAL was sampled _before_ the create. At 174 MiB/s the fleet refills 8 MiB
  in ~50 ms; the create takes 358 ms. The sample and the fold it was meant to
  price were seven refill cycles apart.

Both errors have the same shape: **the instrument's scope did not match the
thing being measured** — one too wide in space (process vs. operation), one
displaced in time (before vs. during). Neither read as an error. Both produced
a precise number answering a question I had not asked.

**"Segment, don't guess the line" needed a second proof.** I guessed twice more
(backoff sleep, then WAL growth) and was wrong twice. One segmentation landed it.

## The candidate: `PERSISTENCE_QUEUE_CAPACITY` 64 → 16

**Measured: `c=8` start 2.38×, stop 4.30×, remove 4.59× faster, 0/12 rounds
slower on each, nothing else moved. Shipped.**

The reasoning I _used_ to pick it was wrong, and the write-up says so rather
than retrofitting a mechanism to a result. What I claimed:

> `COALESCE_ROW_BYTES` = 64 KiB bounds one append's payload, so 16 appends fill
> a 1 MiB commit and slots 17-64 make a create wait four commits instead of one.

`replay_payload` (`persistence.rs:2043`) sums the chunks of one replay, and a
replay carries one PTY read bounded by `OUTPUT_READ_BUFFER_BYTES` = 8192
(`native_runtime.rs:37`). `COALESCE_ROW_BYTES` bounds a database **row**
assembled inside a transaction (`persistence.rs:5005`) — a different quantity.
At 8 KiB per append a 1 MiB commit holds ~128 appends; at the ~1 KiB typical
size, far more. Both 64 and 16 fit inside one commit, so "four commits instead
of one" does not follow.

The win is real and large; the arithmetic that predicted it is not the reason.
A follow-up probe (6 rounds, arms alternated) measured what actually changes:

|                           | capacity 64 | capacity 16 | ratio     |
| ------------------------- | ----------- | ----------- | --------- |
| create mean               | 410 ms      | 125 ms      | **0.305** |
| writes/s, steady state    | 70738       | 70362       | 0.995     |
| write KB/s, steady state  | 186438      | 186324      | 0.999     |
| **bytes per write**       | **2696**    | **2696**    | **1.000** |
| writes during the creates | 679256      | 205448      | 0.302     |

Both follow-up hypotheses die here. It is **not commit size** — bytes per write
is identical to the byte. It is **not render pressure** — the fleet's throughput
is unchanged at 0.995×/0.999×, so no output is being squeezed out.

What is left is the plainest reading available, and it needs no arithmetic about
commits at all: **a create is a FIFO peer, it waits for whatever is queued ahead
of it to drain, and the drain rate is a hardware constant (~186 MB/s in both
arms). So the wait is proportional to the depth.** Quarter the depth, quarter
the wait:

- predicted from depth alone: 16/64 = **0.250**
- observed latency ratio: **0.305**
- observed writes-during-creates ratio: **0.302**

The residual between 0.250 and 0.305 is the create's own ~7 ms of work, which
does not shrink. Identical rate, identical commit size, one third the duration:
the create stops waiting, and nothing about the append stream changes.

My "slots 17-64 cost four commits" story was not just wrong about the
constant — it was solving a problem that did not need solving. The queue does
not need to be larger than one transaction to hurt; it only needs to be full.

**This removes work rather than moving it.** A refused `Append` is not lost and
not deferred to another queue: `append` uses `try_send` (`persistence.rs:797`),
and the dropped bytes are re-carried by the next render from the still-unmoved
offered watermark. The code's own comment: pressure "degrades into fewer, larger
writes rather than into a stalled fleet."

R22 moved this constant 1024 → 64 and shipped it, reasoning about _escalation
across consecutive stops_ — "depth sets the escalation, not the level". That
remains true. This round moves it again for the reason the probe measured:
depth _is_ the level, because the create's wait is depth ÷ drain rate.

**Why not go lower than 16?** Not for the reason I first gave. I wrote that the
fleet's unchanged throughput meant "the queue still absorbs its bursts at 16,
and at 8 or 4 that headroom comes off the output path" — but the row probe in
the fixture section shows the depth is not buying transaction size _at all_ in
this regime: rows are 1.0004× between the two arms, both pinned at the
`COALESCE_ROW_BYTES` ceiling, because eight reactor threads keep `try_recv`
non-empty and the batch fills to `MAX_TRANSACTION_PAYLOAD_BYTES` regardless. So
the honest answer is arithmetic, not headroom: the wait is `depth ÷ rate` and
the create's own ~7 ms is now the larger term. 16 → 8 is worth about 3 ms
against a 93 ms operation. The knob is spent; what is left is structural.

## Result

**SHIP.** 12 paired rounds on cn3, arms rotated, A/A control byte-identical
(md5 `79785885`), candidate differing by exactly the one constant and its
assertion.

| shape/verb | BASE     | CAND    | ratio            | p_holm |
| ---------- | -------- | ------- | ---------------- | ------ |
| c=8 remove | 455.7 ms | 99.4 ms | **4.59× faster** | 0.008  |
| c=8 stop   | 383.7 ms | 89.1 ms | **4.30× faster** | 0.008  |
| c=8 start  | 221.3 ms | 93.0 ms | **2.38× faster** | 0.008  |

0/12 rounds slower on each. Integrity clean in every cell: all 48 Runs started,
0 stops refused, 0 removes failed. Retained bytes unmoved (`c=8` 34114 vs
34144 KB, 0.999×) — the named rollback risk did not materialise.

## The gate was broken, and the A/A control is what caught it

The first adjudication returned **ROLLBACK**. Two cells fired at p≤0.05: the
candidate's `c=1 remove` (1.026×), and — impossibly — the **A/A control's**
`c=8 start`, where two byte-identical binaries differed 1.245× with 10/12 rounds
in the same direction, p=0.039.

Identical code cannot be 24 % slower than itself. Position within the round was
ruled out (BASE-B was slower at all three positions, so the rotation was not the
cause); host load was 0-1 % in every cell; no orphaned daemons.

The cause is the gate itself. It tests **16 cells** (4 shapes × 4 verbs) and
rolls back if **any** reaches p≤0.05. On pure noise that fires with probability
1 − 0.95¹⁶ = **0.56**. The gate has been in use since R20 and was expected, more
often than not, to manufacture a rollback verdict against a candidate that
changed nothing. Here it nearly killed a 2.4-4.6× win.

Two corrections, both required:

1. **Holm-Bonferroni** across the 16 cells — no independence assumption, which
   matters because the four verbs share a daemon and a fleet. The A/A cell goes
   to p_holm = 0.617; the candidate's `c=1 remove` to 0.083; all three wins
   survive at 0.008.
2. **An empirical floor** — a candidate's effect must exceed what the A/A
   control produces on the _same_ cell. The control's own spread on `c=8 start`
   is 0.245, so nothing smaller than that is attributable there.

The corrected gate was re-verified against five synthetic cases before being
used: it still catches a real 28 % regression, still surfaces a win bought with
30 % less retained data, and no longer vetoes on a 2 % light-cell wobble.
Loosening a gate without re-proving it can still fail is how a gate becomes a
rubber stamp.

**The A/A control's real job is not pricing noise — it is unit-testing the
gate.** Any significant A/A cell means the gate is wrong, and the candidate's
numbers should not be read until it is fixed.

## A test that was asserting against the constant, twice

`many_small_appends_become_few_large_rows` failed on **both** arms — pre-existing
on the shipped source, not caused by the change. Its message says why: _"the
persistence queue refused a fixture append"_. The fixture pushes 400 appends
with no drain and treats the first `try_send` refusal as a failure, which is
designed backpressure, not a defect — the R21 lesson written into a fixture.
It is red at capacity 64 and goes red sooner at 16.

The first repair retried the refusal instead of asserting against it, via
`barrier()`. That forced a commit mid-row and produced **26 rows instead of
≤16** — the fixture reporting its own drain policy as a packing defect. The
second replaced the barrier with `std::thread::yield_now()`, which passed at
capacity 64.

It then failed at capacity 16, with **20 rows**. Two repairs in, the fixture was
still a function of the constant, so the third question was why, and the answer
is in `append_replay`: it flushes its pending buffer **unconditionally** at the
end (`persistence.rs:5084`), because the buffer does not survive the
transaction. **One batch is therefore one row boundary**, and a batch is bounded
by what the actor's `try_recv` loop can pull — which the capacity bounds. Four
rows at 64, twenty at 16, 401 when coalescing is broken: the fixture was
sensitive to coalescing _and_ to a performance knob, and the second sensitivity
is what makes a correctness test weld the knob in place.

**That coupling does not exist in production**, and the question was settled by
measurement rather than by argument, because "row size did not change" is
exactly the kind of claim that is convenient to believe. Six rounds on cn3, arms
alternated, rows read straight out of `replay_chunks` after a clean shutdown:

|                | cap 64 | cap 16 | ratio      |
| -------------- | ------ | ------ | ---------- |
| mean row bytes | 64059  | 64082  | **1.0004** |
| rows           | 510    | 515    | 1.010      |
| write KB/s     | 185517 | 184622 | 0.995      |

Both arms sit pinned at the `COALESCE_ROW_BYTES` ceiling. The fixture is a
single producer that yields the moment it is refused, so the actor empties the
queue and the batch is refill-bound; a real fleet has eight reactor threads at
~70k writes/s keeping `try_recv` non-empty, so the batch fills to
`MAX_TRANSACTION_PAYLOAD_BYTES` and never notices the depth. `db_kb` could not
have answered this — at `c=8` it is 34 MB in every arm because
`PER_RUN_REPLAY_BYTES` pins it, so per-row overhead never reaches that number.

The fix is therefore to remove the queue from the fixture rather than to loosen
its threshold: the 400 reads are offered as **one** `OutputReplay`, which is
exactly the shape `coalesce_batch` hands the writer for a batch belonging to one
Run. Verified green at capacity 16, 64, **and 4** — indifference, not two lucky
points — and still red at 401 rows with `COALESCE_ROW_BYTES` broken to 512 B.

**Named rollback risk, declared before the run:** retained bytes. If 16 slots
cannot keep the actor fed, commits shrink, the admitted byte rate falls, and the
latency win is paid for with data the user asked to keep. A latency win bought
with less retained data is a policy change, not a speedup. Measured at 0.999×,
with the row probe above as the direct check on the mechanism behind it.

**"16 is derived, not tuned" was also wrong.** The derivation claimed
`MAX_TRANSACTION_PAYLOAD_BYTES / COALESCE_ROW_BYTES = 16` and that below it the
batcher starves. It rests on the same false premise as the fixture — that the
depth bounds the batch. Under load it does not: the batch is bounded by the
payload cap, and the rows prove it at 1.0004×. 16 is an empirical stopping
point, not an arithmetic one. What remains true is the conclusion, for a
different reason: the win is `depth ÷ rate`, and depth is already down to where
the create's own ~7 ms dominates, so the next factor of two is worth ~3 ms and
the direction is spent regardless.

## Result

Shipped. Twelve paired rounds under the corrected gate, arms md5-verified:
`c=8 remove` 4.59× (456 → 99 ms), `stop` 4.30× (384 → 89 ms), `start` 2.38×
(221 → 93 ms), each `p_holm = 0.008` with 0/12 rounds slower. Retained bytes
0.999×, integrity counters clean in every cell, and clippy identical to the
unmodified baseline's three known Linux-only errors.

## What this round leaves open

The create is off the worst of the queue, not off the queue. At `c=8` it is
still 93 ms against tmux's 8.4-10.2 ms, and the remaining wait is the same
mechanism at a shallower depth. Depth cannot be lowered further without
charging the fleet, so the next lever has to remove the sharing rather than
shrink it: a second persistence actor with its own connection to the same
SQLite store, so a create's transaction runs concurrently with append commits
instead of behind them.

That has a named routing constraint already established: a `StageStart`
carrying eviction candidates must stay on the append lane, because
`validate_exact_candidates` (`persistence.rs:3344`) rejects a still-running
candidate with a fatal latch, and a create on a separate lane could overtake
that Run's `Finalize`. The signal is already available at the send site —
`candidates.is_empty()` — and the measured shape carries no candidates
(`select_publication_candidates` returns none unless at record capacity or
under metadata pressure), so the fast lane is reachable without touching that
invariant.
