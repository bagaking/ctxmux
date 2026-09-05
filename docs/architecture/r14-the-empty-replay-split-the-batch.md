# Round 14 — an empty replay was splitting the batch

`start` at chatty=8 drops from 32.9 ms to 26.4 ms (−17.6%, 11/12 paired arms,
p=0.006); at chatty=2 from 13.8 to 10.9 ms (−20.8%, 12/12, p=0.0005). No other
verb moves. **Landed.**

The change is eleven lines removed. Getting to them took killing this round's
own leading hypothesis, and then catching a statistical mistake that would have
rolled the win back.

## What the round was aiming at

R13 priced each verb against its own quiet-shape floor and retargeted from
`stop` (47% intrinsic, the smallest pool) to `start` (21.3 ms recoverable) and
`remove` (11.0 ms). It then segmented `start` on the actor thread and found the
queue wait was 81% of the premium — but refused to act, because the
sub-segments missed the parent by 3.8x and the actor was only ~40% busy. Its
closing note recorded one arithmetic fit as a **lead, not a diagnosis**: a fold
inside `admit_transaction_with_shutdown` at the 8 MiB trigger would be ~12.8 ms,
and 4 + 12.8 ≈ 16.6 = the measured wait.

## Killing the lead

The instrument was an **accounting probe** rather than a timer on the suspect.
The enqueueing thread snapshots the actor's cumulative counters into the
command; the actor diffs them at dequeue. Every line then answers "during my
wait, what did the actor actually do" — and it closes by construction, which is
the property R13's segmentation lacked.

For `start` at c8:

| layer             | value                               |
| ----------------- | ----------------------------------- |
| queue wait        | 17.70 ms                            |
| ├ `append_us`     | 16.28 ms = **92.0% of the wait**    |
| │ ├ write         | 3.55 ms                             |
| │ ├ prune         | 0.01 ms (4 loops)                   |
| │ └ **commit**    | **10.70 ms = 87.6% of `append_us`** |
| └ `admit_fold_us` | **0.00 ms**                         |
| own fold          | 5.21 ms                             |
| body              | 11.22 ms                            |

`admit_fold_us = 0.00`. Not "under a third" — zero. The append path never folds
on this workload, because every `start` and `remove` zeroes the WAL
unconditionally in its own prologue, so the WAL never reaches the 8 MiB trigger
from the append side.

**Fourth occurrence in this project of "arithmetic that fits is not a
mechanism"** (after c003, c005, c015). R13 logged it as a lead precisely because
the shape was familiar, and that is what kept the round from being spent on it.

A 16-agent survey ran in parallel on four candidate directions. All four were
adversarially verified by three independent skeptics each; **0 of 4 survived**.
Three of the four had converged on the fold, and their own stated discriminator
was `admit_fold_us` — the measurement refutes them on their own terms. One
dissenting verdict named what the accounting shows: _"the real unaccounted cost
is the drain: 4 sub-budget transactions = 4 fsyncs."_

## Not composing medians

The first read of the probe output looked contradictory: median `txns` = 4 from
a median `batch_calls` = 1, carrying a median 1632 KiB. Four transactions for
1.6 MiB when the budget forces only two.

Those are **four separate order statistics over fifteen samples**, and
cross-multiplying them is meaningless. The fix was to re-emit the joint
distribution, one row per sample. It is unambiguous:

```
empty=0  ->  excess transactions = 0     (15/15 samples, c2 remove)
empty=1  ->  excess transactions = 1-3
```

Every wasted fsync coincides with the `groups.is_empty()` path. Byte-forced
floor is `ceil(bytes / 1 MiB)`; `split_budget` and `split_gap` are 0 throughout,
so the 1 MiB budget and the contiguity check are **not** what splits.

## The mechanism

`activate_persistence_after_publication` ends every create by appending
`replay(0)`. Activation happens microseconds after spawn, so on a fresh Run that
replay carries no chunks. `append_batch_with_shutdown` handled a chunkless
replay by flushing whatever was already collected, then opening a transaction of
its own:

```rust
if groups.is_empty() {
    if !transaction_batch.is_empty() {
        self.append_transaction_with_shutdown(&transaction_batch, None, shutdown)?;
        // ...clear
    }
    self.append_transaction_with_shutdown(&[(*id, replay.clone(), ...)], None, shutdown)?;
    continue;
}
```

Two fsyncs to persist zero output bytes — and the first one carries real output,
1.5–2.9 ms at `synchronous=FULL` on the farm. Under a chatty fleet it lands on
the `start` path once per Run.

The fix is to let it ride along:

```rust
transaction_batch.push((*id, replay.clone(), Arc::clone(durable_head)));
continue;
```

A chunkless replay offers no bytes, so it is contiguous with everything.
`expected_heads` stays put because it advances nothing the next group must be
contiguous against, and `append_replay` derives the durable head from the table
and the chunks rather than from `latest_output_bytes` — so merging one through
`coalesce_batch`'s last-write-wins cannot move a watermark backwards.

Note it is **not** skipped: its `truncated` and `first_available_byte` still
have to land. "Don't append it" is the tempting one-liner and it is wrong.

## Why the previous round was right to reject this, and still missed it

This exact code was reviewed before. Commit `2140122` left a test pinning the
behaviour, with a comment recording that the empty COMMIT measures 0.043 ms
against 0.303 ms for the same path carrying bytes, that it is worth ~0.6% of the
create path, and that **"guarding the call site was investigated and
rejected on that measurement."**

That measurement is correct and still stands. It priced the empty commit's own
cost. What it did not price is the empty replay acting as a **batch splitter** —
the flush it forces is the expensive half, and that cost lives on the appends
around it, not on the empty one.

The same comment also wrote down how it would fail:

> _"if a future change makes this empty append expensive (a dirty page, a schema
> bump, an autocheckpoint), the cost moves but this assertion will not notice —
> re-measure rather than trusting the count alone."_

It predicted its own blind spot, and pointed at the remedy. The lesson is
narrower than "re-measure everything": **a cost attributed to an operation may
actually live in what that operation forces its neighbours to do.** Pricing it
in isolation cannot see that.

The test is rewritten around the behaviour that costs — an empty replay between
two contiguous appends must leave the commit count at 1 — and was verified to
fail against the old arm (3 commits observed, 1 expected).

## The statistics nearly rolled this back

The first A/B ran 6 pairs. Result: `start` −21.1% at 6/6, and **c2 `remove`
+9.7% at 0/6** — a two-sided sign p of 0.031. Under the ratchet that is a
rollback trigger, and there was a credible mechanism for it: fewer, larger
transactions mean a `remove` blocks behind a bigger fsync, which is the R12
shape where the gain and the loss are one mechanism.

It was not rolled back, for one reason: **the A/A control was invalid.** It ran
AA-1 first every time, so it measured cn3's known second-arm-faster order bias
rather than the residual noise of an order-balanced comparison. It was the wrong
null for the A/B it was controlling.

Rerun at 12 pairs with the A/A alternating exactly like the A/B:

| shape | verb   | A/B                   | p      | A/A floor | verdict      |
| ----- | ------ | --------------------- | ------ | --------- | ------------ |
| c2    | start  | 12/12 −2.86 ms −20.8% | 0.0005 | +1.2%     | **improved** |
| c2    | remove | 8/12 −0.22 ms −3.1%   | 0.388  | +3.6%     | no effect    |
| c2    | stop   | 5/12 +0.25 ms +3.9%   | 0.774  | +1.0%     | no effect    |
| c2    | list   | 5/12 +0.01 ms         | 0.774  | +4.0%     | no effect    |
| c8    | start  | 11/12 −5.78 ms −17.6% | 0.006  | −1.6%     | **improved** |
| c8    | remove | 7/12 −0.18 ms −1.4%   | 0.774  | +2.2%     | no effect    |
| c8    | stop   | 6/12 +0.13 ms +1.4%   | 1.000  | +4.1%     | no effect    |
| c8    | list   | 6/12 +0.01 ms         | 1.000  | +1.5%     | no effect    |

The `remove` regression was a thin-sample streak: at 12 pairs it is 8/12 in the
**candidate's** favour. The A/B had to clear both a p<0.05 sign test and the
A/A floor for that same verb; `start` clears both at both shapes, nothing else
clears either.

Worth recording: at n=6 even the c8 `start` win — −12%, 5/6 — was p=0.219, not
significant. **Six pairs could not have justified landing this either.** The
thin sample was not merely a risk of a false rollback; it was insufficient in
both directions.

96 arms, 0 skipped-dirty-host, 0 timeouts, 0 backpressure refusals.

## Absolute numbers

| shape | verb   | base     | after        |
| ----- | ------ | -------- | ------------ |
| c2    | start  | 13.78 ms | **10.89 ms** |
| c2    | stop   | 6.44     | 6.57         |
| c2    | remove | 7.13     | 7.06         |
| c2    | list   | 0.212    | 0.216        |
| c8    | start  | 32.93 ms | **26.36 ms** |
| c8    | stop   | 9.69     | 9.58         |
| c8    | remove | 12.87    | 12.35        |
| c8    | list   | 0.236    | 0.235        |

Against R13's floor pricing, `start` at c8 had 21.3 ms recoverable above its
6.40 ms quiet-shape floor. This takes 6.6 ms of it — **31% of what was
theoretically available**, from deleting eleven lines.

## What is still open

- `commit` remains ~88% of the append work a `start` waits behind. This round
  removed the _excess_ fsyncs; the remaining ones are byte-forced by the 1 MiB
  transaction budget. Lowering the count further means changing the budget or
  the durability contract, neither of which is a free win.
- The unconditional `truncate_wal_to_zero` in both lifecycle prologues is why
  the append path never folds. Making it conditional was surveyed this round and
  every verdict argued the fold cost is per-byte and would simply relocate into
  the queue the verbs wait behind — the R12 shape. Not attempted; recorded here
  because the divergent policy is real and will look tempting again.
- chatty=2/4/8/16 still have **no tmux counterpart**, so no ratio can be
  published for the shape that matters most.
- The long-lived-client caliper still does not exist; warmbench reconnects for
  every verb, so both "calipers" have always measured the same shape.
