# R34 — the gate made idle passes free and left the stop passes paying all N

**Outcome: the 81% "unaccounted" slope is found, and my own R33 conclusion was
wrong.** Measured in the product binary by segmenting the owner loop, not by
reading a profile.

## What R33 claimed, and why it was wrong

R33 priced `drive_lifecycle`'s per-entry body with a standalone microbenchmark at
**0.00785 ms/pass** at 256 entries, multiplied by the 4.0-pass-per-stop figure,
and concluded the whole sweep body was capped at 0.031 ms/stop — 8.5% of the
target. On that basis it *retired a class*: "no future round should propose
optimizing the sweep body."

Segmenting the real binary puts the same function at **0.0828 ms/pass** — **10×
the model**. The microbenchmark replicated the loop's *shape* (the `mem::replace`
of a 128 B enum, the `Arc` deref, the command-queue `Mutex`) but omitted the one
thing the loop actually calls out to: `leader_is_terminal_gated()` at
`native_runtime.rs:1023`. The three costs it did model came from R32's profile —
a list of *expensive lines*, not a list of *all lines* — so a cheap-to-sample,
high-frequency syscall never entered the model.

A microbenchmark that is 10× low does not merely under-report; it closes the
correct direction under the label "already excluded."

## Where the owner's CPU actually goes

Segment timers around each stage of `owner_main`, batch-differenced so the fleet
build-up is excluded, median of 4 alternating pairs:

| segment | live=24 | live=256 | slope |
| --- | --- | --- | --- |
| `drain` | 1.29 µs | 1.64 µs | +0.35 |
| **`lifecycle`** | **26.56 µs** | **82.76 µs** | **+56.20** |
| `workers` | 12.22 µs | 14.95 µs | +2.73 |
| `retain` | 0.48 µs | 0.98 µs | +0.50 |
| poll, CPU part | 8.41 µs | 11.76 µs | +3.34 |
| *(poll, blocking wait)* | *3892.86 µs* | *3927.27 µs* | *not CPU* |

`lifecycle` is **89% of the owner's CPU slope**. At 4.0 passes/stop that is
**+0.225 ms/stop** — 62% of the ~0.363 ms being chased, against the 8.5% R33
asserted.

## The mechanism, confirmed by construction

R31 added a pass-wide gate: one `waitid(P_ALL, WNOWAIT)` decides whether *any*
child has exited, and when it says no, all N per-Run peeks return early at
`native_session.rs:403`. That is why R31 won on idle fleets.

**But a stop kills a child.** So on exactly the passes a stop causes, the gate is
open and every entry falls through to a real `waitid(P_PID)`. A second
instrumented build split every pass by gate state and counted the peeks:

| | live=24 | live=256 |
| --- | --- | --- |
| entries walked per pass | 145 | 377 |
| **peeks per gate-open pass** | **144.2** | **376.2** |
| sweep cost, gate-OPEN | 67.6 µs | 194.1 µs |
| sweep cost, gate-SHUT | 21.4 µs | 65.8 µs |
| `waitid(P_ALL)` gate, per pass | 6.65 µs | 20.07 µs |
| open : shut pass ratio | 0.24 | 0.25 |

Peeks per open pass equal the entry count exactly — the 1:1 fall-through is
real — and roughly one gate-open pass occurs per stop, so nothing amortizes it.

## What the control arm adds, beyond the pre-registered theory

The prediction was `232 added peeks × 0.25 µs = +58 µs/pass`; the measured
gate-open slope is **+110.6 µs/pass**, inside the pre-registered 2× band, so the
peek mechanism is confirmed. But the **gate-SHUT arm issues zero peeks and still
grows +36 µs/pass**, which the peek theory does not explain at all. Three costs
scale independently:

1. **the per-Run peeks** — only on gate-open passes; explains the ~82 µs gap
   *between* the two arms, not the whole slope;
2. **per-entry sweep work** — present on every pass including gate-shut ones,
   ~+44 µs/pass at 377 entries. This is the cost R33 modelled at 7.85 µs and is
   therefore still ~5× under even after removing the peeks;
3. **the `P_ALL` gate itself** — advertised in its own doc comment as "one
   syscall for the whole pass", but measured at 6.65 → 20.07 µs, **tripling with
   the fleet**. A constant-cost gate is not constant.

Naming only the peeks would repeat R33's error in the opposite direction.

## Caveats on record

* **The fleet is larger than the knob.** The owner walks 145 and 377 entries at
  `live=24` and `live=256`, because `NativeEntry` rows outlive their Runs
  (`owner_main`'s `retain` keeps rows with pending output or terminal state).
  Per-entry costs must be divided by the walked count, not the configured one.
* **These are instrumented builds.** ~11 clock reads per pass. They are for
  attribution — which segment owns the growth — and any candidate must be priced
  on a clean arm and clear the full F5 gate.
* **The A/A floor still binds.** The gate's empirical floor on
  `stop persist live=256` is 0.112 ms, so a candidate worth less than that needs
  the ns-resolution owner-CPU probe rather than the gate to show a difference.

## Corrections this forces on the record

R33's "Where the slope actually is" table is wrong in two rows and its
conclusion is wrong outright: it told the next round to *stop* looking at the
owner's per-entry work, which is precisely where 89% of the slope lives. The
retired-class claim ("per-entry work in the owner sweep is capped at ~0.03
ms/stop") is withdrawn.
