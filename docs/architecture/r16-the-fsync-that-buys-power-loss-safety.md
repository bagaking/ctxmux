# Round 16: what synchronous=FULL is buying, and why it was rejected

- Status: measured, adjudicated, **REJECTED** — a second batch turned the
  suspected blemish into a confirmed regression
- Host: cn3 (`n36-001-230`), 64-core Linux, ext4, glibc 2.36
- Baseline: `e55e6cb`; test fix landed as `baf8bdd`
- Table conventions: [benchmark comparison conventions](../benchmark-comparison-conventions.md)

## The question

`PRAGMA synchronous=FULL` fsyncs the WAL on every commit. On this host that is
1.5–2.9 ms per commit. In WAL mode it buys exactly one thing over `NORMAL`:
survival of **power loss**. Process death — panic, OOM kill, SIGKILL — is
covered identically by both, because the bytes are already in the kernel and
the daemon dying does not stop the kernel writing them out.

So: does ctxmux promise power-loss safety? Reading its own contract,
`choices/009-runtime-persistence-recovery.md`:

- line 111: "**Process crash** or torn WAL recovery therefore yields the
  previous or next complete unit"
- line 210: SQLite durability "**is not evidence of power-loss safety** on a
  filesystem that violates them"
- line 244: fixture — "**daemon kill** while a Run is live restores only the
  committed replay window"

All three are process-death semantics. Power loss is explicitly disclaimed. FULL
is paying, on every commit, for a property the design document refuses to claim.

## What it costs

`pop=8 reps=15`, alternating order every pair, 12 pairs at chatty=2 and 24 at
chatty=8, with a same-order A/A control (FULL vs FULL) measuring the residual
noise floor for each verb. Verdict rule: sign test p<0.05 **and** the median
shift exceeds that verb's own A/A floor.

| shape | verb     | FULL             | NORMAL    | Δ median        | sign test       | A/A floor | verdict   |
| ----- | -------- | ---------------- | --------- | --------------- | --------------- | --------- | --------- |
| c2    | start    | 11.927 ms        | 10.611 ms | −1.417 (−11.9%) | 10/12, p=0.039  | 0.060     | ✅ win    |
| c2    | stop     | 6.437 ms         | 3.744 ms  | −2.611 (−40.6%) | 12/12, p=0.0005 | 0.160     | ✅ win    |
| c2    | remove   | 7.926 ms         | 5.376 ms  | −2.330 (−29.4%) | 12/12, p=0.0005 | 0.343     | ✅ win    |
| c8    | stop     | 9.568 ms         | 4.996 ms  | −4.376 (−45.7%) | 24/24, p<0.0001 | 0.418     | ✅ win    |
| c8    | remove   | 13.511 ms        | 11.084 ms | −3.096 (−22.9%) | 22/24, p<0.0001 | 0.470     | ✅ win    |
| c8    | start    | 26.360 ms        | 28.512 ms | +1.976          | 8/24, p=0.15    | 0.613     | no effect |
| c2/c8 | list     | 0.211 / 0.263 ms | unchanged | —               | p≥0.39          | —         | no effect |
| c2    | WAL peak | 8.415 MB         | 8.458 MB  | +0.043 MB       | 2/12, p=0.039   | 0.033 MB  | marginal  |
| c8    | WAL peak | 8.621 MB         | 8.549 MB  | −0.058 MB       | 15/24, p=0.31   | 0.076     | no effect |

Folded to the verb pair tmux compares against (`stop`+`remove` = `kill-session`):
c2 14.363 → 9.120 ms (−36.5%), c8 23.079 → 16.080 ms (−30.3%).

## The relocation check, which is the part that matters

Five previous rounds in this project died on "the gain and the loss are the same
mechanism". A per-commit fsync that stops happening has an obvious place to
reappear: the checkpoint, which is no longer amortised across many small syncs.
If NORMAL merely **defers** the flush, the WAL grows and the wait moves.

It does not grow. c8 WAL high-water moved −0.058 MB (15/24, p=0.31) and c2 by
+0.043 MB — 0.5%, barely over its own A/A floor and in a shape where every
latency verb improved. There is no reservoir of deferred work.

The c8 `start` cell is the honest blemish. It fails the sign test outright at
n=24 (8/24, p=0.15), so by the standing rule it is **no effect** — but 16 of 24
pairs leaned the wrong way, and `start` is precisely where deferred work would
land. An attempt to settle it by counting `fsync` syscalls under `strace -c -w`
was **discarded**: the FULL arm also reported `fsyncs=0`, which is impossible,
so the instrument was broken and its output is not evidence either way. That
cell is unresolved.

## Why this did not merge

Both blocking gates were later cleared, and clearing the second one is what
killed the change.

The tmux rerun blocked on cn3's root filesystem being 100% full. That turned out
to be the wrong reading of the host: `/` was full, but `/data00` had 4.2 TB free
and is writable. Only `/tmp` was ever the problem. The rerun was one `mktemp -d
-p` away from being runnable the whole time, which is worth recording as its own
lesson — "the host is unusable" was a conclusion drawn from one directory.

The rerun then produced both a tmux baseline and, in the same batch, a second
independent FULL-vs-NORMAL comparison. See
[round 17](r17-the-spawn-path-is-where-tmux-beats-us.md) for the tmux side.

### The regression, confirmed

The paired candidate arms from that batch (6 rounds per shape, the two modes
adjacent inside each round):

| shape | verb        | FULL   | NORMAL | Δ          | sign | p     |
| ----- | ----------- | ------ | ------ | ---------- | ---- | ----- |
| c1    | start       | 9.280  | 7.355  | **−1.925** | 6/6  | 0.031 |
| c1    | stop+remove | 12.903 | 7.922  | −4.981     | 6/6  | 0.031 |
| c2    | start       | 11.072 | 11.158 | **+0.086** | 2/6  | 0.69  |
| c2    | stop+remove | 14.023 | 9.066  | −4.957     | 6/6  | 0.031 |
| c8    | start       | 23.728 | 26.408 | **+2.680** | 2/6  | 0.69  |
| c8    | stop+remove | 22.247 | 15.233 | −7.014     | 6/6  | 0.031 |

`stop+remove` is a large, clean win at every shape. `start` moves the wrong way
at c2 and c8, and at c8 it is +2.68 ms — 11% worse.

The first batch called that cell "no effect" at p=0.15, which was the correct
call _for one batch_: 8/24 fails the test. But 16 of 24 pairs leaning wrong is
not the same as 12, and this batch independently reproduced the direction at
both c2 and c8. Two batches agreeing on a direction that one batch could not
resolve is evidence the first batch was underpowered, not evidence of noise.

By the ratchet rule — any metric degrading means rollback — R16 is **rejected**.
Not deferred pending better measurement: the measurement happened, and it went
against the change.

### Why start was always the cell to watch

This is the outcome the relocation check was designed to catch, arriving one
level deeper than that check could see. WAL high-water did not grow, so the
bytes are not accumulating. But `start` truncates the whole WAL, and a WAL whose
pages were never flushed by a per-commit fsync leaves more dirty pages for that
truncate to force out. The work is not deferred into a bigger file; it is
deferred into a _dirtier_ one. Same total bytes, worse placement — and `start`
pays for it.

That is the fifth time in this project that a gain and a loss have turned out to
be the same mechanism. The pattern is now specific enough to state as a rule:
when removing a flush helps the verbs that wait on flushes, check the verb that
forces a flush of its own.

## What carries forward

- **The contract argument survives the rejection.** FULL really is paying, on
  every commit, for power-loss safety that `choices/009` explicitly disclaims.
  That remains true and remains worth revisiting — but the way to collect it is
  not a blanket downgrade, because the `start` path depends on someone having
  flushed those pages already. A future attempt should target the flush that
  `start` forces rather than the flushes it depends on.
- The A/B data stands: 12 and 24 pairs with a matched A/A control, plus 6 more
  pairs per shape from the tmux batch, all on a quiesced host.
- `baf8bdd` landed independently: the durability witness asserted byte equality
  between the live and recovered windows, which failed 5 of 12 runs against the
  **unmodified** daemon. Root cause is granularity, not durability — the live
  log evicts whole PTY reads (200–600 B) while the durable side drops whole
  64 KiB coalesced rows, so recovery legitimately trails by up to one row. It
  now asserts suffix identity plus a one-row bound, which is what a crash could
  actually violate.
- The durability witness was never run against NORMAL, and now does not need to
  be. Worth stating plainly so nobody reads this doc as "the durability question
  was answered": it was not. The change was rejected on latency before the
  durability evidence was needed.
