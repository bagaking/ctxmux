# Round 20 — the fsync we rejected, rejected again for a different reason

**Verdict: ROLLED BACK.** `stop`'s tail regresses at chatty=1 (+5.54 ms) and
chatty=2 (+6.79 ms), 1/12 rounds each, p=0.006, against an A/A floor of
±1.6 ms. Three of four verbs improve at every shape; that does not matter under
the ratchet.

The change was one line, `crates/ctxmux-daemon/src/persistence.rs:2225`:

```
-PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA wal_autocheckpoint=0;
+PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA wal_autocheckpoint=0;
```

It was never applied to the tree — both arms were separate builds from
identical source differing in that constant — so the rollback is a no-op. What
follows is what the round bought: the reason the candidate is dead, which is
**not** the reason R16 killed it.

## This candidate was already rejected once, for a reason that no longer held

R16 measured it: stop −40.6%/−45.7% and remove −29.4%/−22.9% at c2/c8 — and
**c8 `start` +2.68 ms, reproduced in two independent batches**. Rolled back.

R16's diagnosis of the one bad cell:

> `start` truncates the whole WAL, and a WAL whose pages were never flushed by
> a per-commit fsync leaves more dirty pages for that truncate to force out.

R19 deleted that unconditional truncate — `fold_wal_below_ceiling` now folds
only above 8 MiB. The veto was conditioned on a code path that no longer exists
in that form, and this project's rule is that "当初为什么这么写" is not
"为什么不能改". Re-pricing it was correct, and the re-pricing **confirmed R16's
veto was obsolete**: `start` improves at every shape now, by 7.9 ms at c8.

It found a different, larger veto underneath.

## The prediction, written before the data

From `.tmp-r20-prereg.md`, before a single arm ran:

> I expect global NORMAL to **look clean on medians and regress on `start`'s
> p90.** R19 made the fold *rarer*, not *cheaper*. At c8 the WAL peaks ~8.5 MB,
> so the fold still fires on some fraction of starts, and on exactly those calls
> NORMAL's unflushed pages must be forced out — the R16 spike, now concentrated
> in the tail instead of the median. **So the gate is p90, not the median.**

**Wrong on the verb, right on the statistic.** The gate *was* p90 — that is what
saved the round, because every median says ship. But the regressed verb was
`stop`, not `start`; `start` p90 improved everywhere. And the regression is
worst at c1, where the WAL peaks at ~300–700 KB and the fold never fires at all.

Second falsified pre-registration in two rounds. It counts as evidence only
because it was written down first.

## What the data says — four shapes × 12 pairs × 4 arms

Medians, exact two-sided sign test, A/A floor from BASE-vs-BASE with the same
alternation:

| shape | start | stop | remove | list |
|---|---|---|---|---|
| c0 | **−0.576** 12/12 | **−0.997** 12/12 | **−0.688** 12/12 | +0.006 (ns) |
| c1 | **−3.182** 12/12 | +0.333 5/12 (ns) | **−3.013** 12/12 | +0.033 (ns) |
| c2 | **−2.778** 12/12 | **−2.367** 12/12 | **−3.147** 12/12 | +0.022 (ns) |
| c8 | **−4.391** 12/12 | **−4.151** 12/12 | **−3.918** 12/12 | **−0.085** 11/12 |

Every median that moves, moves the right way. On medians alone this ships at
all four shapes.

Pooled p90 (180 observations per arm, stratified bootstrap resampling *within*
round, 95% CI):

| shape | start | stop | remove | list |
|---|---|---|---|---|
| c0 | −0.91 [−1.30,−0.51] | −1.01 [−1.81,−0.31] | −0.83 [−1.29,−0.40] | +0.02 |
| c1 | −3.56 [−3.76,−3.38] | **+5.54 [+3.64,+7.22]** | −3.65 [−3.81,−3.41] | −0.00 |
| c2 | −3.23 [−3.58,+9.25] | **+6.79 [+5.77,+7.95]** | −3.40 [−3.85,−3.21] | +0.08 |
| c8 | −7.92 [−16.54,−0.92] | −1.42 [−3.06,+2.10] | −5.91 [−14.32,+7.07] | −0.20 |

A/A on the same statistic spans ±1.6 ms at worst. The c1/c2 `stop` intervals
clear it by 4×.

### The tail is bimodal, not shifted

| shape | arm | p50 | p75 | p90 | p99 | >15 ms | >30 ms |
|---|---|---|---|---|---|---|---|
| c1 | BASE | 7.79 | 8.44 | 9.50 | 20.81 | 6 | 0 |
| c1 | NORMAL | 7.98 | 11.82 | **15.34** | 31.14 | **18** | 2 |
| c2 | BASE | 7.97 | 8.63 | 9.43 | 21.18 | 8 | 0 |
| c2 | NORMAL | **5.63** | 12.72 | **16.19** | 31.83 | **33** | 2 |
| c8 | BASE | 11.26 | 13.24 | 27.01 | 32.55 | 36 | 6 |
| c8 | NORMAL | **6.92** | 22.89 | 25.23 | 29.97 | 55 | **1** |

NORMAL makes the typical `stop` *faster* (c2 7.97→5.63, c8 11.26→6.92) and
roughly triples the number of slow ones. Stops split into two populations. That
is the signature this project keeps meeting: **收益和劣化是同一个机制**.

## Root cause: cheaper commits deepen the queue, and `stop` is what waits on it

First, what is *not* true, because I wrote it down before checking: it is not
that `stop` alone is coupled to the fleet's output. On the unmodified BASE arm
all three verbs slow down as output rises, by similar amounts:

| verb | p50 c0→c8 | p90 c0→c8 | p90/p50 at c8 |
|---|---|---|---|
| start | 4.48 → 9.12 | 5.69 → 16.01 | 1.75 |
| stop | 6.73 → 11.26 | 8.21 → 27.01 | 2.40 |
| remove | 3.52 → 6.61 | 4.26 → 14.69 | 2.22 |

That shared cost is the known persistence cliff, not this round's finding.

What *is* specific to `stop` is the **derivative**: when persistence gets
cheaper, `start` and `remove` get faster and only `stop` gets slower.

- `start` and `remove` block on **their own** commit. NORMAL removes an fsync
  from that commit, so both improve — medians *and* tails, at every shape.
- `stop` blocks on a full reactor sweep whose bottom half scans output
  ([[ctxmux-stop-measures-reactor-sweep-not-poll-intervals]]). Cheaper commits
  let the persistence actor accept work faster, so more output is resident per
  sweep when a `stop` lands on the wrong side of one.

The regression is **non-monotonic in output volume**, and that is what makes the
mechanism identifiable rather than merely plausible:

| chatty | WAL peak | stop p90 Δ | why |
|---|---|---|---|
| 0 | 12 KB | **−1.01** (win) | no output ⇒ no queue to deepen |
| 1 | ~0.3–0.7 MB | **+5.54** (regression) | headroom, and NORMAL fills it |
| 2 | ~4.3 MB | **+6.79** (regression) | headroom, and NORMAL fills it |
| 8 | ~3–4.7 MB | −1.42 (flat) | queue already saturated, nothing left to fill |

The two ends are the proof. **c0 is the positive control**: strip the output and
`stop` improves like the other two verbs, 12/12 on the median and a tail win —
so the regression is not in the commit path NORMAL actually changes. **c1 is the
negative control for the obvious alternative**: its WAL peaks an order of
magnitude below the 8 MiB fold ceiling, so `fold_wal_below_ceiling` never fires,
NORMAL removes syncs and adds none back, and `stop`'s tail regresses hardest
there anyway. Any explanation routed through the fold dies on c1; any
explanation routed through the commit dies on c0. Queue depth survives both.

This is the third time this exact shape has appeared —
[[ctxmux-cheaper-render-makes-remove-slower]],
[[ctxmux-a-faster-reactor-just-deepens-the-queue]], and now this. An
optimization that makes the persistence actor cheaper *per unit of work* buys
latency for the verbs that commit and sells it from the verb that waits behind
the queue those commits feed. It is now safe to treat that as a standing prior
rather than a surprise.

## Why the split form is not the fallback

The obvious rescue — NORMAL for lifecycle commits, FULL for output — makes the
asymmetry *worse*. It speeds up exactly the commits that deepen the queue while
leaving the output path's sync cost intact to keep the sweep long. It also
cannot be built as stated: `synchronous` is connection-scoped in SQLite and both
paths share the connection, so "NORMAL for some statements" is not a thing the
pragma expresses. Not attempted, and not worth a round.

The real target this round exposes is not the pragma at all: **`stop` is the one
verb that gets worse when persistence gets cheaper.** Four shapes agree on the
sign of that derivative. The lever is the per-sweep bound on output scanning,
not the cost of a commit — and the round's own failure is the evidence that
lowering commit cost without bounding the sweep moves latency from one verb to
another rather than removing it.

## Durability, verified anyway

Worth keeping even though the candidate died, because the next fsync candidate
will need it — and because it establishes that **safety was never what killed
this one.**

SQLite documents what NORMAL gives up (pragma.html#pragma_synchronous):

> A transaction committed in WAL mode with synchronous=NORMAL might roll back
> following a power loss or system crash. **Transactions are durable across
> application crashes regardless of the synchronous setting or journal mode.**

ADR 009:212 already disclaims power-loss safety — the only thing FULL buys over
NORMAL in WAL mode — so the contract would have permitted it.

The pre-registered proof was `persistence_recovery`. That suite **fails 6 of 9
identically on the unmodified BASE tree** on this host: same six tests, same
`wait_terminal_within` timeout at line 208, unchanged by
`CTXMUX_TEST_TIME_SCALE=8` or `--test-threads=1`, with `/bin/sh` working and
65535 free PTYs. Red before the change, so it cannot clear the change. Filed
separately; not caused by this round.

So the property was tested directly, both arms, same script — chatty fleet, wait
for a genuinely live WAL, SIGKILL mid-flight, restart on the same state dir:

| arm | WAL at crash | committed | recovered |
|---|---|---|---|
| BASE (FULL) | 4.50 MB | 18 | 18 |
| NORMAL | **8.47 MB** | 22 | 22 |

Zero loss with 8.47 MB of unsynced WAL outstanding.

Two earlier versions of the check were thrown away for measuring nothing: the
first committed into a `wal=0` daemon (proving only that the main database
survives), the second raced its own load generator, which tore the fleet down
inside a fixed sleep so the crash landed on an idle daemon. Both were caught by
an explicit guard, not by inspection — a zero-byte WAL under a chatty fleet is
physically impossible.

## Method notes worth keeping

**The tail estimator was wrong mid-round, and the fix needed no re-run.**
Per-round p90 off 15 reps is the 14th of 15 samples — effectively the
second-worst observation. The A/A control proved it useless here: two runs of
the *same binary* differed by 20 ms on `start_p90`. I had gated the round on a
statistic noisier than the effect being hunted. Because the harness saves raw
per-sample data, pooling all 12 rounds (180 observations, p90 = the 162nd) and
bootstrapping *within* round recovered a usable gate from data already on disk.

**The pooled sample directory silently mixes shapes.** `.tmp-r20-sync.sh` writes
every shape's samples into one `r20-samples/` keyed only by
`R<n>c<chatty><ARM>.txt`, and the adjudicator's regex `^(R\d+)c\d+(\w+)\.txt$`
matches any shape. Once c1 and c2 landed, a tail computed over that directory
would have pooled three workloads into one distribution. Caught before reading
any c1/c2 tail number; fixed by adjudicating against per-shape symlink dirs. The
c8 verdict was re-run against a filtered dir and is unchanged.

**One shape would have shipped this. So would two, or three.** c8 alone reads
`==> SHIP`. So does c0. So does every shape's median table. The veto exists only
because c1 and c2 ran, and c1 — the shape that both vetoes the change and
identifies the mechanism — is the *lightest* workload, the one most tempting to
skip as uninteresting. [[ctxmux-a-mechanism-that-fits-two-shapes-can-die-on-the-third]]
was written after the last time this happened; this round is its second
confirmation and the first time the sweep was run wide enough to catch it before
merge rather than after.
