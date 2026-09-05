# R30 pre-registration — stop resetting a WAL that is already small

Written before any source change. Every threshold, falsifier, and predicted
sign below is fixed at this point; the adjudication reads this file, not a
rationalization written after the numbers land.

## What the research pass said, and what measurement did to it

The R30 research survey ranked five candidates. Two died before any code:

**Candidate 2 (drop the redundant `ensure_listening` ping) — FALSIFIED.**
The CLI opens two connections per verb: `ensure_listening` performs a complete
`ping()` handshake even when the daemon is already up (`crates/ctxmux/src/daemon.rs:36`),
then the command opens a second one. The survey estimated 0.2–0.4 ms (INFER)
and pre-registered the falsifier "if the delta is within the A/A noise floor,
it is not worth shipping."

Measured on cn3 by speaking the wire protocol directly (`.tmp-r30-pingcost.py`,
n=400, warmed):

| path | median | p90 |
|---|---|---|
| connect + hello + close | **0.0419 ms** | 0.0495 ms |
| connect + hello + list + close | 0.0728 ms | 0.0829 ms |

0.042 ms against an A/A noise floor of 0.46 ms — **11× below the floor.** The
survey's estimate was an order of magnitude high. Killed by its own falsifier.

**Candidate 1 (is the CLI floor a benchmark artifact?) — RESOLVED, partly.**
Read from source rather than measured. agentmux holds a long-lived
`CtxmuxClient` (`ctxmux-run-adapter.ts:804`), so it never pays the ~0.53 ms
process spawn — that part of the floor *is* a harness artifact. But the SDK's
`#request` opens a fresh connection and Hello per call
(`packages/sdk/src/client.ts:640`, `:718`), so the consumer does pay one
connect+handshake per verb. At 0.042 ms that is not worth attacking either.

So the whole CLI-floor family is out, and it cost two probes and no code.

## The actual finding: five fsyncs, not one

The survey's per-verb inventory attributed ~2 ms on each of start/remove/stop
to "the COMMIT fsync", singular — but that number came from subtracting
memory-only from persistence-ON. Subtraction names a layer, not a line.

Counted instead (`.tmp-r30-syscount.py`, strace on the daemon, 20 reps per verb,
each verb's syscalls attributed to its own client-observed window):

| verb | wall each | fsyncs per verb | fsync ms | share of wall |
|---|---|---|---|---|
| start (persist) | 9.485 ms | **4.85** | 0.805 each | **41.2%** |
| remove (persist) | 4.938 ms | **3.85** | 0.720 each | **56.1%** |
| start (memory) | 2.903 ms | 0 | — | — |
| remove (memory) | 0.723 ms | 0 | — | — |

Not one barrier. Nearly five. Reducing the *count* of durability barriers is a
different lever from weakening `synchronous`, which R-earlier already vetoed —
one changes how many times we prove durability, the other changes whether we
prove it.

Resolving every fd to a path (`.tmp-r30-fdmap.py`, `strace -y`) shows all of
them on the WAL, and shows what the first one is:

```
one start, client-observed 8.241 ms
  +4.186 ms  pwrite64  state.sqlite3-wal, "7\177\6\202..."   <- 0x377f0682 = WAL HEADER MAGIC
  +6.248 ms  fsync     state.sqlite3-wal   1.9910 ms   <== BARRIER 1
  +6.286 ms  pwrite64  state.sqlite3-wal   (6 page frames)
  +6.537 ms  fsync     state.sqlite3-wal   0.9070 ms   <== BARRIER 2
  +8.018 ms  fsync     state.sqlite3-wal   0.0510 ms   <== BARRIER 3
```

Barrier 1 is the fresh **WAL header** being written and synced on its own, and
it is the most expensive of the three. It exists only because the WAL was reset
to zero between verbs. The WAL file ends the run at 0 bytes.

## Why the WAL keeps getting reset

`idle_fold_wal` (`crates/ctxmux-daemon/src/persistence.rs:1587`) folds whenever
the WAL is merely **non-zero**:

```rust
if !matches!(file_len(&store.wal_path), Ok(bytes) if bytes > 0) {
    return false;
}
store.try_fold_wal_once()   // PRAGMA wal_checkpoint(TRUNCATE)
```

Its justification is real and is documented at the call site: folding costs
~1.6 ms/MiB, so pay it while the actor is idle rather than on a later verb. That
reasoning is correct for a *large* WAL. It omits the other side of the ledger —
truncating to zero imposes a penalty on the next commit.

Priced directly, plain SQLite with the daemon's pinned pragmas (WAL,
`synchronous=FULL`, `wal_autocheckpoint=0`), `.tmp-r30-walfloor.py`, n=60:

| next commit lands on | median | p90 |
|---|---|---|
| a WAL just truncated to zero | 1.9117 ms | 2.5208 ms |
| a WAL already ~256 KiB long | 0.9167 ms | 1.1630 ms |
| **penalty imposed by truncating** | **+0.9950 ms** | |

and the fold's own cost, by size:

| WAL size | fold cost | ms/MiB |
|---|---|---|
| 0.07 MiB | 1.2541 ms | 17.72 |
| 0.26 MiB | 1.4364 ms | 5.54 |
| 1.00 MiB | 2.6143 ms | 2.61 |
| 8.01 MiB | 9.6098 ms | 1.20 |

This matches the known mechanism recorded two rounds ago: a zero-length WAL
forces the next commit to extend the file, which degrades `fdatasync` into a
full `fsync`, plus a separately-synced 32-byte header.

**The trade the current code makes:** in a quiet fleet the WAL after one verb is
a few KiB. The fold spends ~1.25 ms to clear those few KiB, and then charges the
*next* verb ~1.00 ms for the privilege of starting from zero. It is paying twice
to avoid a cost that, at that size, is smaller than either payment.

## The change

Give the idle fold a floor: fold only when the WAL is large enough that folding
it is worth what the reset costs the next commit. Below the floor, leave it
alone — a small WAL is the cheap state to commit onto, not a mess to clean up.

This is the **opposite** direction from the already-rolled-back experiment that
lowered the output-side fold threshold from 8 MiB to 1 MiB and made create
0.7–0.9 ms *slower* by folding 8× more often. That result is evidence for this
one: fewer, larger folds beat more, smaller folds.

Floor value: **256 KiB**. Chosen as the smallest size in the measured table
whose fold cost (1.4364 ms) clearly exceeds the reset penalty (0.9950 ms) with
margin, while being ~32× below the 8 MiB admission ceiling so the arithmetic
that bounds the WAL is untouched.

## Why this does not weaken any proof

Both lifecycle admission proofs already compare the post-commit WAL against a
**delta baseline**, not against zero:

- `persistence.rs:2950` — `wal_bytes != wal_baseline` (staged start)
- `persistence.rs:3263` — `wal_bytes != wal_baseline` (exact removal)

and `fold_wal_below_ceiling` (`:3117`) already only folds above
`WAL_CHECKPOINT_BYTES`, returning whatever baseline it finds. A non-zero
baseline is already a legal, load-bearing, shipped state. The ceilings still
close arithmetically: nothing may charge more than 8 MiB, every path that must
bound the WAL still folds above 8 MiB, so the post-commit absolute still cannot
exceed the 16 MiB total. The floor changes only *when we volunteer* to fold, not
any bound.

## Predictions, written before the data

1. **start and remove get faster in the quiet (c=0) shape**, by roughly the
   reset penalty: −0.5 to −1.0 ms each. This is the shape where we lose to tmux
   worst, which is why it is the target.
2. **Barrier count drops from ~5 to ~4 per verb** on the quiet shape, and the
   ~2 ms header barrier disappears from the trace.
3. **Chatty shapes are unchanged.** Under load the WAL is far above 256 KiB
   whenever the fold runs, so the floor never engages and the code path is
   identical. A change in a chatty cell is evidence of something I have not
   understood — not a bonus.
4. **Idle CPU stays at 0.000%.** The floor makes the fold strictly *less*
   frequent, never more.

Prediction 3 is the one I most expect to be wrong, and it is the one most likely
to reveal a mechanism I have mis-modelled. A previous round was caught by
exactly this: a mechanism that fit two shapes died on the third.

## Falsifiers — any one of these kills the candidate

- **Any** cell degrades beyond its A/A floor with Holm-Bonferroni significance
  across all cells → roll back, document.
- The quiet-shape improvement fails to clear the A/A noise floor (0.46 ms on
  stop) → the mechanism is real but too small to ship; roll back.
- Barrier count per verb does not drop in the post-change trace → I have
  mis-identified barrier 1; the whole story is wrong regardless of timings.
- Idle CPU becomes measurable → the fold is firing more, not less.
- Any new test failure against the control's known-failing set → correctness
  regression, non-negotiable.
- WAL length exceeds 16 MiB in any shape, or any admission proof reports a
  baseline it cannot prove → a bound broke; roll back immediately.

## Artifact risk, flagged in advance

The quiet shape is the cheapest to measure and the easiest to flatter. Two
specific traps:

- Measuring with a WAL that happens to sit above the floor makes the change
  inert and reports "flat" — a knob that never engaged always reports flat. The
  adjudication must confirm the floor actually engaged (fold count drops) before
  believing any null result.
- The same-batch tmux arm is mandatory; alternate arm order, because the second
  arm on cn3 runs systematically faster.
