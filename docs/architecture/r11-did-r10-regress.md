# Round 11: did round 10 regress the heavier shapes?

- Status: measurement record, 2026-09-14
- Host: cn3 (`n36-001-230`), 64-core Linux, ext4
- Binaries: `c56d3c78` (pre-R10) vs `e8daa95a` (R10, merged as `3ed1a1b`)
- Shape: `pop=8 reps=15`, `chatty` producers in the fleet writing continuously
- Harness: `docs/verification/harness/armlib.sh`; conventions
  [§4.3](../benchmark-comparison-conventions.md)

## Why this check existed

Round 10 merged on two shapes: quiet, and one continuous producer. Both improved,
so the ratchet passed it. Afterwards, `chatty=2` turned out to show the same
saturation signature R10 was supposed to have removed — `stop` back at its
saturated value of ~106 ms. That is precisely the failure mode where **looking
only at the shapes that improve** lets a regression merge while looking like a
win, so the ratchet rule required checking the shapes that were skipped.

The ratchet's answer was pre-committed: if `AFTER` were worse at `chatty>=2`,
round 10 rolls back regardless of how good the `chatty=1` number looks.

## The answer: no regression. The cliff predates R10.

`BASE` **cannot serve `chatty=2` at all.**

| shape | arm | start | stop | remove | daemon CPU |
|---|---|---|---|---|---|
| chatty=1 | base | 1014 / 1179 ms | 107 / 107 | 788 / 899 | 58.2 / 64.8 s |
| chatty=1 | after | 20.1 / 21.0 ms | 8.0 / 8.2 | 12.6 / 14.1 | 3.68 / 3.77 s |
| chatty=2 | base | **TIMEOUT ×4** | — | — | ~425 s per arm |
| chatty=2 | after | 49.9–93.3 ms | 68.5–106.7 | 459–975 | 34.0–43.5 s |
| chatty=4 | base | **TIMEOUT** | — | — | 243.6 u / 39.8 s |
| chatty=4 | after | **TIMEOUT** | — | — | 183.3 u / 80.4 s |

Four `BASE` arms at `chatty=2`, both orders, two rounds: **four timeouts at 240 s
with no output at all**, each burning ~300 s utime + ~122 s stime. Four `AFTER`
arms at the same shape: four successes.

The difference is not "faster" — it is **service versus no service**. R10 did not
create the cliff and did not trade heavy shapes for light ones. It moved the
threshold: one producer now fits under it, two do not.

**R10 stands. No rollback.**

## The cliff is still there, one producer further out

At `chatty=4` **both** binaries time out. So R10 relocated the cliff from between
1 and 2 producers to between 2 and 4; it did not remove it.

The two timeouts are not identical. `BASE` burns 243.6 s user / 39.8 s system,
`AFTER` 183.3 s and 181.9 s user against 80.4 s and 77.3 s system across its two
arms — less user work and about twice the system work, reproducible in both
orders, consistent with `AFTER` reaching real I/O where `BASE` spins in
userspace. Both still fail to serve a single verb in 150 s.

No ratio is published for the `chatty=4` row, or for `BASE` at `chatty=2`:
per conventions §2, a system that cannot do the thing at all is not "slower by
N times". A timeout is a capability statement, not a measurement.

This is round 11's target, and it is now located to within one producer.

## What the numbers still say is wrong

`AFTER` at `chatty=2` is not healthy, it is merely alive. `stop` sits at
105.8–106.7 ms in three of four arms — the same saturated value `BASE` shows at
`chatty=1`. **`AFTER` at `chatty=2` sits exactly where `BASE` sat at `chatty=1`.**
The cliff is intact; only its position moved.

`remove` spans 459–975 ms across four arms of one binary at one shape. That
spread is itself the signal: under saturation the verb's cost depends on queue
state at entry, not on the work it does.

## Two corrections to earlier claims

**`stop`'s 106 ms is not five poll intervals.** The arithmetic
(`106 / CHILD_CONTROL_POLL(20 ms) ≈ 5`) fits, and it is wrong — the third time
this project has been fooled by a number that adds up (conventions §5.2).
`wait_for_handoff_ready` (`lib.rs:955`) does poll every 20 ms, but `handoff_ready`
is not a local check: it sends `OwnerCommand::HandoffReady` to the daemon-wide
reactor and blocks for the reply (`native_runtime.rs:450`). The reactor loop runs
`drain_commands` at the **top** (`:739`) and `poll_and_read_outputs` at the
**bottom** (`:786`), so a command arriving during an output sweep waits for the
entire sweep. The probe itself is a linear scan and a `bool` (`:801`). 106 ms is
reactor loop latency.

**The actor and the reactor are not independent.** `poll_and_read_outputs` calls
`run.record_output()` inline (`:1451`), which takes three mutexes and renders a
replay into the persistence queue (`lib.rs:4052`). Persistence saturation
therefore slows the reactor's own sweep, which delays `drain_commands`, which is
what `stop` waits on. This is the coupling that makes the optimizations
super-additive rather than independent.

## A standing gap this surfaced

`idle_fold_wal` folds the WAL only when the actor's queue is **empty**
(`persistence.rs:1484`, called from the `TryRecvError::Empty` branch at `:1573`).
Under a chatty fleet the queue is never empty, so the fold never fires and
`start` pays the full unconditional `truncate_wal_to_zero` (`:2698`) — note the
output path folds only above 8 MiB (`:3907`), while start's is unconditional.

At the measured ~1.6 ms/MiB, the `chatty=1` WAL peak of 8.5 MiB is ~13.6 ms
against an observed create premium of 16.4 ms. The shape has positive feedback:
each start zeroes the WAL, so the fold cost is whatever accumulated since the
last start — slower creates accumulate more, which makes creates slower.

Per conventions §5.2 this is **a hypothesis whose arithmetic has not been
excluded**, not a diagnosis. The discriminator (same fleet with `--state-dir`
omitted, so no WAL, no fold, no actor) is queued.

## Harness defects found and fixed

Everything above is from a **second** set of runs. The first set is void.

Two orphaned `BASE` daemons from an earlier run were pinned at 174% and 178% of a
core, PTY fleets alive, while two different harnesses measured. They disagreed
**30x on the same binary at the same shape** (`remove` 950–1206 ms vs 21–35 ms).
Killing them dropped load from 10.96 to 0.19 — the orphans were the load.

1. `pkill` on a harness script skips its own `kill $dpid`, orphaning the daemon.
2. A SIGTERM'd daemon does not take its PTY children with it.
3. **loadavg is too slow to gate an arm.** With 8 of 64 cores burning, the
   instantaneous busy fraction read 0.1293 while `load1` read 2.45 — under the
   4.0 gate then in force. A guard that never blocks is worse than no guard: it
   stamps "clean" onto dirty data.

Fixed in `armlib.sh`: quiesce on an instantaneous `/proc/stat` busy fraction
before every arm, reap the fleet and verify it, and stamp `busy0=` and
`load=before->after` into every output line so a contaminated arm is recognizable
from the log alone. `check-guard.sh` verifies the guard in both directions —
that it passes when quiet **and blocks when busy** — because the failure mode
here is a guard that silently never fires.
