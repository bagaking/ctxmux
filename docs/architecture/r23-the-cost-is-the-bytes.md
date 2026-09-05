# Round 23 — the publication cost is the fleet's own bytes

**Verdict: REJECTED, no code change.** Three attribution probes, three
pre-registered predictions, **all three refuted**. The round produced no
candidate worth gating, and the reason is worth more than a candidate would
have been: the 0.3-0.5 s publication is not overhead. It is the durable write
rate of the output the fleet actually produced.

## What this round set out to do

R22 shipped the correctness half — a `stop` no longer answers `Running` and
breaks the next `remove`. Its explicit non-goal was this round's goal, quoted
from `r22-the-stop-that-stops-lying.md`:

> It does not make publication faster. A 0.3-0.5 s stop under a loud fleet is
> still slow, and journald's other half — batching the fsync off the hot path
> — remains the real fix.

SG1 priced it against the only baseline that counts (tmux 3.3a, same batch, 6
rounds, arms alternating by round):

| shape             | our stop+remove | tmux kill-session |
| ----------------- | --------------- | ----------------- |
| 16 Runs, 8 chatty | 881 ms          | 10.3 ms           |
| 64 Runs, 8 chatty | 1057 ms         | 11.0 ms           |

`stop` and `remove` split almost exactly in half (445 / 430 ms at n=16), which
was the first clue: both traverse the same FIFO actor queue, so this is one
queueing cost seen twice, not two defects.

## Probe 1 — is the batcher firing? Prediction: no. **Refuted.**

`strace -c`, shipped build (md5 79785885, constants verified 10 s / 64), one
`stop` of a **silent** Run beside 8 chatty neighbours at 232k reads/s:

| arm                 | `pwrite64` | `fsync` | pwrite per fsync |
| ------------------- | ---------- | ------- | ---------------- |
| quiet (c0, control) | 4          | 4       | 1                |
| plateau (c8)        | 67,103     | 140     | **479**          |

479 × 4 KiB ≈ **1.87 MiB per commit** — the append batcher is not merely firing,
it **saturates** its 1 MiB payload ceiling with ~2x WAL amplification on top.

I had predicted the opposite (that the batcher's `try_recv` loop usually finds
an empty queue, the same shape as the idle-fold defect). It was wrong, and its
being wrong killed an entire family: **every "batch harder" candidate is dead**,
including the group-commit designs the research recommended. Those were already
weakened by their own caveats — we have a single serial writer, so there is no
follower fleet to amortize an fsync across, and in both PostgreSQL and RocksDB
the follower blocks its thread anyway.

The number I was _not_ looking for: **262 MiB written during one stop**, for a
Run whose own finalize writes nothing. All of it is the neighbours'. And `fsync`
is only 139 ms of syscall time — `pwrite64` is **89%**. The cost was never
durability. It is volume.

## Probe 2 — is it the global eviction ceiling? **The probe could not answer.**

`GLOBAL_REPLAY_BYTES = 256 MiB`; the probe measured 262 MiB per stop. A tempting
match — and precisely the coincidence-as-mechanism error that
`docs/benchmark-comparison-conventions.md` §5.2 exists to prevent. So the ceiling
was varied rather than argued about: three marker-verified builds, order rotated.

| arm   | ceiling | MiB/stop | MiB/commit | **DB size** |
| ----- | ------- | -------- | ---------- | ----------- |
| small | 64 MiB  | 465      | 1.99       | **34.9 MB** |
| base  | 256 MiB | 503      | 1.98       | **34.9 MB** |
| large | 512 MiB | 485      | 1.95       | **35.0 MB** |

Spread 1.08x, and my adjudicator duly printed "appends dominate, the ceiling is
not the lever". **I did not keep that verdict.** The DB-size column — added as
bookkeeping, not as a check — shows every arm retained ~34.9 MB, _below even the
64 MiB arm's ceiling_. `prune_global_replay` never evicted in any arm. The probe
compared three identical no-eviction configurations and correctly found them
identical.

Honestly established: global eviction is not the cost **at this fleet size**,
because it never runs. Nothing is established about eviction when it does. A
flat result from a knob that was never engaged is not evidence about the knob —
the self-ratifying-check shape again, and it survived my own review until a
bookkeeping column contradicted it.

## Probe 3 — row size or the per-Run trim? Prediction: the trim. **Refuted.**

`PER_RUN_REPLAY_BYTES` (4 MiB) × 8 chatty ≈ the 34.9 MB retained, so every
chatty Run sits at its own cap and every append forces `prune_run_replay`. Three
arms, one constant each, markers verified, order rotated, 4 rounds:

| arm     | row     | cap        | stop        | MiB/stop        | DB           | reads/s |
| ------- | ------- | ---------- | ----------- | --------------- | ------------ | ------- |
| ctl     | 64 KiB  | 4 MiB      | 1482 ms     | 257             | 34.9 MB      | 254k    |
| row     | 512 KiB | 4 MiB      | 1243 ms     | 216 (0.84x)     | 34.3 MB      | 229k    |
| **cap** | 64 KiB  | **32 MiB** | **1482 ms** | **261 (1.02x)** | **272.3 MB** | 244k    |

The `cap` arm settles it. **Eight times the retained data, identical bytes
written, identical stop latency.** If trimming were the cost, raising the cap
8x would have cut it; it did nothing. Writes track the fleet's **output rate**,
not the stored set and not the trim:

```
ctl:  257 MiB written /  34.9 MB retained = 7.4x
cap:  261 MiB written / 272.3 MB retained = 1.0x
```

`row` at 0.84x is real but under the 1.25x bar, and it is a **known, already
repriced lead** — R12 landed coalescing, and the memory
`ctxmux-reprice-a-deferred-candidate-before-spending-the-bump` exists because
this exact knob's price moves with the row size. It is not worth a ratchet bump
on a 0.84x that also carries a 0.90x drop in the reads/s it was measured at.

## Why the round closes without a candidate

At ~250k reads/s the fleet produces ~293 MiB/s. About 257 MiB lands durably per
~1.48 s stop window — **~174 MiB/s sustained**, at 27-37 commits/s of ~2 MiB
each. The arithmetic closes on the original question: a 64-deep queue at that
rate is 1.7-2.4 s worst case and ~445 ms typical. **The stop is not slow because
of its own work. It is slow because of its FIFO position behind bytes that were
genuinely asked for.**

One more measurement worth keeping: the fleet _produces_ ~2000-2500 MiB in that
same window while ~490 MiB reaches disk. Backpressure already sheds ~80%. The
persistence layer is not falling behind by accident — it is deliberately outrun,
and what it does commit is the part it promised to keep.

So "make publication faster" was the wrong target. Publication is already
running at the disk's sustained rate for the bytes it must keep. The remaining
levers are not in the commit path at all:

- **retain less** — a policy change, not a performance fix, and it trades away
  the crash-recovery property tmux does not have;
- **write less per byte** — the `row` lead, priced above and not worth the bump;
- **decouple the stop from the queue** — excluded: R21 proved queue-jumping
  latches the persistence layer, and publish-before-finalize breaks recovery
  (a still-`running` row is rewritten to `Interrupted{DaemonRestart}`).

## Excluded by measurement or by primary source this round

- **`synchronous=NORMAL`** — stays rejected. This round's research argues it is
  _theoretically_ ordering-safe (WAL recovery restores a contiguous prefix via
  the cumulative checksum chain, and SQLite guarantees durability across
  _application_ crashes at any `synchronous`). But R16 rejected it on
  **measurement** — c8 `start` +2.68 ms, reproduced in two independent batches.
  Theory does not overturn a measurement.
- **io_uring `IORING_OP_FSYNC`** — excluded before costing a round. Kernel
  `io_uring/sync.c` sets `REQ_F_FORCE_ASYNC` with the comment _"fsync always
  requires a blocking context"_: it relocates the block to an io-wq kernel
  thread rather than removing it. arXiv 2512.04859 measures linked write+fsync
  as _"no improvement over issuing them sequentially"_, and PostgreSQL keeps WAL
  fsync off io_uring deliberately.
- **Group commit / leader-follower** — dead on probe 1's numbers (the batcher is
  already saturated) and inapplicable anyway: one serial writer, no followers.

## Two corrections to my own premises, found by reading the code

Both were in the brief I handed the researchers, so both came back confirmed.
A premise that is never checked is how a round gets spent on the wrong thing.

1. **There is no tokio worker pool and no permit.** I wrote that `finalize`
   blocks "one of 8 shared `ctxmux-native-blocking` workers holding a
   `CleanupPermit`". `native_runtime.rs:1090` spawns a **fresh
   `thread::Builder` per job**; the cap is the loop condition
   `while active.len() < CLEANUP_MAX_ACTIVE`. The fleet-level consequence is the
   same, but the recommended fix — `oneshot` + `.await` to _release a permit_ —
   addresses a structure that does not exist.
2. **The client is serial.** Every harness stops one Run at a time, so there is
   never a second finalize to batch with. Classic group commit amortizes one
   fsync across _concurrent_ writers; we have one.

What survived from the research is narrower and better: journald's ack mechanism
is not batching at all. It snapshots the queue _position_ and replies when the
drain passes it — a statement about what the queue is doing, which is what sent
these three probes at the queue instead of at the fsync.

## The rule this round earned

Three predictions, three refutations, and each refutation closed a family:
batching (saturated), eviction (never runs), trimming (8x the data, same cost).
The predictions were worth writing precisely because they were wrong — a round
that only confirms its author's guesses has not learned where the cost is.

And probe 2 is the sharper lesson: **a knob that never engaged will report
"flat", and flat reads exactly like "not the lever".** The check that caught it
was a column added for bookkeeping. Every future attribution probe must assert
that the mechanism under test actually ran, or its null result means nothing.
