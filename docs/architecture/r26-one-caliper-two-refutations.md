# Round 26 — the caliper, and two beliefs it refuted

**Verdict: MEASUREMENT ROUND. No product code changed.** Two claims this
project had been carrying — one of them mine, written three weeks of rounds
ago — turned out to be false when both systems were finally put on one
instrument.

## Why a round was spent on an instrument

At the end of R25 I wrote:

> c=8 下 start 还剩 12.07 ms, tmux 是 8.4–10.2 ms, 还差 10 倍

The 12.07 came from `.tmp-r24-cell.sh`, the ratchet's own harness. The
8.4–10.2 came from `.tmp-r16-tmux.sh`. Those are different instruments, and
a recorded lesson already said four harnesses disagree by 10x on the same
nominal `chatty=8` (`ctxmux-chatty-count-is-not-the-dose`). I compared two
instruments and attributed the difference to the two systems.

Three confounds, stacked:

| | ratchet harness | the tmux harness |
|---|---|---|
| client in the timed window | Rust `ctxmux` CLI | tmux's C client |
| producer | `/bin/dash`, 64 B, tight loop | `bash`, 32 B, tight loop |
| population at measurement | fleet growing 0→48 | POP=8, settled 1.5 s |
| pairing | paired sign test | ratio of medians, unpaired |

The dose is the `read()` message rate. dash spins faster than bash and the
line is twice as long, so "chatty=8" meant two different physical loads.

The fix is not a better tmux harness. It is **one harness with a backend
branch**: `.tmp-r26-cell.sh` runs tmux through the same `date` calls, the
same producer, the same population, the same loop counts as ctxmux. A
sibling script would have reproduced the defect it was written to fix.

## The gate

12 rounds. Four arms rotated through four positions, because cn3's second arm
is systematically faster and rotating four arms through three slots leaves a
residual bias that manufactures a winner.

Three of the four arms are **byte-identical copies of the same ctxmux
binary**. Their spread is the instrument's resolution, measured in every cell
rather than assumed:

| shape | start | list | stop | remove |
|---|---|---|---|---|
| c=0 | 1.013x | 1.028x | 1.021x | 1.002x |
| c=1 | 1.010x | 1.007x | 1.016x | 1.001x |
| c=2 | 1.020x | 1.022x | 1.016x | 1.043x |
| c=8 | 1.064x | 1.019x | 1.042x | 1.105x |

Health across all 192 cells: 48/48 started everywhere, zero refused stops,
zero failed removes.

## Result — ctxmux (durable) vs tmux 3.3a (in-memory)

| shape | verb | ctxmux | tmux | ratio | A/A floor | sign test |
|---|---|---|---|---|---|---|
| c=0 | start | 6.53 ms | 4.24 ms | 1.54x | 1.013x | 0/12 |
| c=1 | start | 7.03 ms | 4.94 ms | 1.42x | 1.010x | 0/12 |
| c=2 | start | 8.24 ms | 5.91 ms | 1.40x | 1.020x | 0/12 |
| **c=8** | **start** | **12.03 ms** | **10.58 ms** | **1.14x** | 1.064x | 0/12 |
| c=0 | stop+remove | 12.93 ms | 3.86 ms | 3.35x | 1.011x | 0/12 |
| c=8 | stop+remove | 22.47 ms | 10.31 ms | 2.18x | 1.079x | 0/12 |
| c=0 | list | 2.62 ms | 3.34 ms | **0.78x** | 1.028x | 12/12 |
| **c=8** | **list** | **3.01 ms** | **9.45 ms** | **0.32x** | 1.019x | 12/12 |

Every cell 12/12 on the paired sign test, p = 0.0005.

tmux's `kill-session` is an atomic stop+remove, so it is compared against our
sum. Splitting it into a fabricated stop/remove pair would be inventing data.

**tmux at c=8 start is 10.58 ms on this caliper**, agreeing with R18's 11.14
ms — not the 8.4 ms I quoted. The R25 sentence was wrong.

## Refutation 1: the gap closes as load rises

| verb | c=0 | c=1 | c=2 | c=8 |
|---|---|---|---|---|
| start | 1.54x | 1.43x | 1.41x | 1.15x |
| stop+remove | 3.37x | 2.86x | 2.77x | 2.15x |
| list | 0.79x | 0.67x | 0.58x | 0.32x |

Monotone on all three. **We are expensive at baseline and cheap at the
margin** — tmux degrades faster than we do under a loud fleet.

Every ratchet round from R19 to R25 targeted c=8, on the reasoning that the
loud fleet is where the pain is. It is where the pain *was*, against our own
earlier baselines. Against tmux, c=8 is the shape we are **closest** to
parity on, and c=0 — no queue, no fleet, no contention, where neither R24's
depth nor R25's lane does anything — is where we lose by the most.

The lightest shape has vetoed a candidate three times
(`ctxmux-the-lightest-shape-is-the-one-that-vetoes`). This is the first time
it is also the shape most worth optimising.

## Refutation 2: "at equal capability we beat tmux" is only true for start

R18 measured memory-only c=8 start at 3.451 ms against tmux's 11.141 ms and
concluded:

> the reactor, PTY and spawn paths are collectively fast enough to beat tmux
> on every verb. Nothing outside persistence needs optimising to close this gap.

That sentence has been load-bearing ever since — it is why every round since
has looked only inside the persistence layer. But it was measured in a
different batch, so R26 re-ran it as a third backend in the same rotation: the
same binary with `--state-dir` omitted, 8 rounds.

| shape | verb | durable | memory-only | tmux | memory-only vs tmux |
|---|---|---|---|---|---|
| c=0 | start | 6.39 | 3.96 | 4.20 | ctxmux 1.06x faster (8/8) |
| c=8 | start | 11.03 | 6.11 | 10.55 | ctxmux 1.73x faster (8/8) |
| c=0 | list | 2.62 | 2.63 | 3.34 | ctxmux 1.27x faster (8/8) |
| c=8 | list | 3.06 | 3.23 | 9.34 | ctxmux 2.89x faster (8/8) |
| **c=0** | **stop+remove** | 12.79 | **8.20** | **3.87** | **tmux 2.12x faster (0/8)** |
| **c=1** | **stop+remove** | 13.36 | **8.42** | **4.72** | **tmux 1.78x faster (0/8)** |
| **c=2** | **stop+remove** | 14.73 | **8.37** | **5.49** | **tmux 1.53x faster (0/8)** |
| c=8 | stop+remove | 22.20 | 9.71 | 10.37 | ctxmux 1.07x faster (8/8) |

Stable round to round — c=0 memory-only stop+remove ran 7.94–8.62 ms across
all 8 rounds while tmux ran 3.83–4.07 ms. Not noise.

**Strip persistence entirely and we still lose stop+remove to tmux at three
of four shapes.** R18's claim holds for `start` and `list`; it is false for
the teardown path. There is roughly 4.3 ms of non-persistence cost in
memory-only c=0 stop+remove that tmux does not pay, and no round has ever
looked at it, because R18 said there was nothing there to find.

How R18 reached a true measurement and a false generalisation: it measured
`stop` and `remove` separately against a tmux column that only had a
`stop+remove` entry, so the per-verb rows have no tmux number at all. The
aggregate row existed but the conclusion was written about "every verb".

### The persistence share, per verb

| shape | start | stop | remove |
|---|---|---|---|
| c=0 | 38% | 28% | 49% |
| c=8 | 45% | 54% | 59% |

R18 reported 85–99%. The difference is not a contradiction — R18's disk arm
ran at 26.2 ms for c=8 start where ours runs at 11.0, because R24 and R25
removed most of the queue wait that was being counted as persistence. What is
left is a much more even split, and **the majority of teardown cost at the
quiet shapes is no longer persistence at all.**

## What this round did not do

No product code changed, so there is nothing to roll back and nothing to
ship. That was the correct outcome once the caliper showed the baseline was
unsound: the only capability-neutral candidate on the table was ~1.1 ms of
`posix_spawn` work in a vendored dependency, priced against a gap that turned
out to be measured wrong.

I also did not fix the finalize defect found while clearing red tests
(`r26-the-finalize-that-blocks-a-stranger.md`). It needs its own round.

## What R27 should look at, in order

1. **Memory-only `stop+remove` at c=0.** 8.20 ms against tmux's 3.87 ms with
   persistence entirely absent. Largest unexplained gap in the matrix, and the
   one belief that was protecting it is now retracted.
2. **The c=0 fixed floor.** R24's c=0 control put 3.6 ms in a block — accept,
   hello round-trip, per-start thread spawn, oneshot handoff, framing — that
   has never been decomposed. It is fleet-independent, so it sits inside every
   number in this document.
3. Not `posix_spawn`. Not group commit — the lifecycle lane means no append
   flush is in flight when a lifecycle command is served, so there is nothing
   to join (`ctxmux-a-knob-that-never-engaged-reports-flat` waiting to happen).

## The honest framing

Every number above is "ctxmux, which survives a cold restart, against tmux,
which does not". The retained-bytes column reads 5232 KB + 7708 KB WAL for us
and 0 for tmux, and that zero is the capability difference, not a win. tmux
never promised to outlive its own process, and scoring a design choice as a
failure would make the rest of the comparison untrustworthy.
