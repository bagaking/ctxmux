# Round 19: the WAL never needed to be empty, only bounded

- Status: PENDING — batch running, verdict not yet reached
- Host: cn3 (`n36-001-230`), 64-core Linux, ext4, glibc 2.36, tmux 3.3a
- Table conventions: [benchmark comparison conventions](../benchmark-comparison-conventions.md)

## The target

[Round 18](r18-every-verb-we-lose-is-persistence.md) measured that 85-99% of
every verb we lose to tmux is persistence, and pointed at one call:
`truncate_wal_to_zero` at the head of `start` and `remove`. This round removes
it.

The reason it was there is a real proof obligation, not an accident. ADR 013
requires each staged transaction to prove, before it can launch a child, that it
fits an 8 MiB per-transaction WAL ceiling and that the WAL as a whole stays under
16 MiB. Starting from a zero-length WAL let one comparison of the WAL's
_absolute_ length cover both. It also made the check trivially auditable.

It was expensive for the same reason it was convenient. The output path
deliberately lets the WAL ride up to its 8 MiB fold trigger, so under a chatty
fleet the WAL is near that value essentially always — sampled every 10 ms it was
at zero for 3.7% of samples at chatty=2. Every lifecycle op therefore folded
~8.5 MB at ~1.6 ms/MiB, about 13 ms of pure setup before any work began.

## Why the checkpoint could go

The bound the ADR actually needs is on what one transaction _writes_, not on how
long the file already is. Written as a delta it holds off any baseline:

```
baseline = file_len(wal)   before BEGIN, after folding only if > 8 MiB
require   actual - baseline <= charge        (per-transaction, 8 MiB)
and       baseline + charge <= 16 MiB        (total)
```

The two ceilings close arithmetically rather than by an empty file: the baseline
is at most 8 MiB because anything larger is folded, and no transaction may charge
more than 8 MiB, so the post-COMMIT WAL cannot exceed 16 MiB.

This is not a new mechanism. `admit_transaction_with_shutdown`, on the output
path — far hotter than either lifecycle verb — has always read `file_len`, folded
only above 8 MiB, and proven `current + worst_case <= 16 MiB`. The rejected
"parallel WAL-charge ledger" of ADR 013 is something else: a ledger accumulates
across transactions, while this reads the real file once inside the same
serialized staging window and discards it when the window closes.

## Proving the delta form

The old fixture asserted `actual_wal <= charge` with a load-bearing
`assert_eq!(wal_len, 0)` above it. That form is exactly what stops being true, so
it was replaced rather than supplemented, in `ctxmux-sqlite-status` against the
pinned SQLite 3.53.2:

`cache_bound_covers_spill_disabled_wal_growth_from_any_baseline` sweeps baselines
of 0, 64 KiB, 2 MiB and 8 MiB, and for each asserts

1. `actual - baseline <= charge`,
2. the growth is a whole number of WAL frames, and
3. **the growth is the same at every baseline.**

The third is the real obligation. If a transaction wrote more frames onto a
fuller WAL, the charge would have to grow with WAL residency to stay sound, and
the whole rebase would be unsafe no matter how the first two read. Baseline 0 is
retained as the control: it reproduces the absolute form, so a failure there
means the harness broke rather than the hypothesis.

## The test that had to exist

Five WAL/fold tests already existed. All five stay green if
`fold_wal_below_ceiling` is reverted to an unconditional truncate — they pin the
_idle_ fold, not the lifecycle one. Without a new test the change would have had
no test behind it at all.

`a_lifecycle_verb_folds_only_a_wal_that_is_over_the_ceiling` asserts both halves
together: under the ceiling the baseline is adopted and `checkpoint_attempts`
stays at zero; over it, the fold fires and the WAL reaches zero. Mutation-checked
— reverting the condition turns it red (`MUT_RC=101`, panic on the
`checkpoint_attempts == 0` assertion) while the unmutated tree passes 54/54.

One existing fixture broke, and instructively. `persistence_actor_survives_a_
short_lived_external_checkpoint_reader` needs a busy checkpoint to observe, and
its premise was "every start checkpoints". With the fold conditional, its small
WAL no longer triggered one, and its releaser thread spin-waited forever: the
suite reported 54 passes and then simply stopped. It **hung rather than failed**,
which is worse than a red test, because a hang looks like slowness. The fix grows
the WAL past the ceiling before the reader snapshots — which is also closer to
what the fixture claims to be testing.

## Results

12 pairs per shape, `pop=8`, at chatty 1, 2 and 8. Each round runs an A/B pair
(BASE, CAND) and a matched A/A pair (both BASE), with arm order alternated by
round parity; a tmux arm every fourth round. Statistic is the exact two-sided
sign test on per-pair direction — a spread test called a +24 ms regression
"noise" in round 12, so spread is not used here.

| shape | verb   | pairs | faster | median Δ       | p     | verdict |
| ----- | ------ | ----- | ------ | -------------- | ----- | ------- |
| c1    | start  | 12    | 12/12  | **−3.777 ms**  | 0.000 | FASTER  |
| c1    | remove | 12    | 12/12  | **−2.868 ms**  | 0.000 | FASTER  |
| c1    | stop   | 12    | 5/12   | +0.129 ms      | 0.774 | flat    |
| c1    | list   | 12    | 6/12   | +0.002 ms      | 1.000 | flat    |
| c2    | start  | 12    | 12/12  | **−6.223 ms**  | 0.000 | FASTER  |
| c2    | remove | 12    | 12/12  | **−4.768 ms**  | 0.000 | FASTER  |
| c2    | stop   | 12    | 4/12   | +0.212 ms      | 0.388 | flat    |
| c2    | list   | 12    | 6/12   | +0.002 ms      | 1.000 | flat    |
| c8    | start  | 12    | 12/12  | **−14.670 ms** | 0.000 | FASTER  |
| c8    | remove | 12    | 12/12  | **−9.230 ms**  | 0.000 | FASTER  |
| c8    | stop   | 12    | 8/12   | −1.058 ms      | 0.388 | flat    |
| c8    | list   | 12    | 4/12   | +0.032 ms      | 0.388 | flat    |

36 of 36 pairs faster on `start`, 36 of 36 on `remove`, and the win grows with
the shape — which is what the mechanism predicts, since a busier fleet keeps the
WAL closer to the ceiling the old code folded on every call.

The A/A control is clean on all twelve cells (every p ≥ 0.146), so no verb is
unjudgeable in this batch. Its floor is ±0.25 ms at c1/c2 and ±2.5 ms at c8 —
the c8 floor is larger than the c1 _effect_, which is why the ratchet needs the
control and not just the A/B.

WAL peak is unchanged: BASE 8.43 / 8.50 / 8.76 MB against CAND 8.40 / 8.48 /
8.93 MB by shape. A candidate that merely deferred bytes instead of not writing
them would show here, and does not.

### Against tmux 3.3a, same batch

Ratio of medians, never a sign test: the two systems run different verb sets and
pairing them would be false precision. `stop`+`remove` is compared as a pair
because that is what one tmux `kill-session` does.

| shape | verb        | BASE   | CAND   | tmux   | BASE ratio | CAND ratio |
| ----- | ----------- | ------ | ------ | ------ | ---------- | ---------- |
| c1    | start       | 10.248 | 6.519  | 4.551  | 2.25x      | **1.43x**  |
| c1    | stop+remove | 14.283 | 11.237 | 4.389  | 3.25x      | **2.56x**  |
| c1    | list        | 0.217  | 0.220  | 3.448  | 0.06x      | 0.06x      |
| c2    | start       | 14.805 | 8.884  | 5.180  | 2.86x      | **1.71x**  |
| c2    | stop+remove | 17.821 | 13.648 | 4.564  | 3.90x      | **2.99x**  |
| c2    | list        | 0.242  | 0.246  | 3.653  | 0.07x      | 0.07x      |
| c8    | start       | 38.035 | 22.621 | 11.511 | 3.30x      | **1.97x**  |
| c8    | stop+remove | 29.873 | 21.210 | 11.216 | 2.66x      | **1.89x**  |
| c8    | list        | 0.261  | 0.284  | 9.276  | 0.03x      | 0.03x      |

Taken alone, `remove` reaches 1.01x at c1 and **0.86x at c8** — the first verb
other than `list` to pass tmux in the chatty shape. It does not clear the
teardown column, because our teardown is two verbs and `stop` is untouched.

### The prediction that was wrong

At c1 and c2, `stop` was +0.129 and +0.212 ms — same sign, growing with shape.
There was a mechanism ready for it: the old code zeroed the WAL as a side effect
of every `start` and `remove`, so removing it should leave the WAL nearer the
ceiling, make the _output_ path fold more often, and charge whoever is queued
behind that fold. It predicted the regression should be largest at c8.

c8 came back at **−1.058 ms, 8/12 faster**. The sign inverted exactly where the
effect should have been strongest, so the mechanism does not exist, and the
c1/c2 values are noise — the A/A floor at those shapes (±0.25 ms) already covers
them.

Worth recording because the story was coherent and fit two shapes out of three.
The only reason it did not become the round's conclusion is that it was written
down as a falsifiable prediction _before_ the third shape landed.

## Verdict

**Merged.** Two verbs improve at p=0.000 across all three shapes, nothing
degrades, the A/A control is clean everywhere, and the WAL peak does not move.

The correctness case is independent of the benchmark: the charge proof is now a
delta bounded off the admission baseline, verified against the pinned SQLite at
four baselines including the ceiling itself, with baseline-independence asserted
rather than assumed; the persistence suite is 54/54; and the new guard is
mutation-checked — reverting the fold condition turns it red while the five
pre-existing WAL tests stay green.

What is not fixed: `stop` is untouched (1.55-1.78x tmux) and is now the only verb
still clearly losing. Teardown as a pair stays behind because of it.
