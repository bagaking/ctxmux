# R21 — the dose was wrong three times, and the defect is not a latency

**Candidate rejected** by its own pre-registered falsifier. No code change ships.
What the round produced instead is a correctness defect, a corrected measurement
axis, and the finding that the gate every prior round was scored on never enters
the regime where any of this happens.

## What was chased, and what it turned out to be

R21 opened on a `stop` step function: 7.6 ms at a quiet daemon rising to a
plateau pinned at ~110 ms. Three things were established by build-level
discriminators, each costing ~15 minutes and each killing a direction:

1. The plateau **is** `TERMINAL_VISIBILITY_GRACE`. Building 100 → 300 ms moved it
   110.3 → 310.2 ms while the sub-knee arm did not move. 200 ms in, 199.9 ms out.
2. The render is **free**. A build that renders nothing (`replay(u64::MAX)`) moved
   the plateau 0.2 ms across four alternating rounds. "Render a delta instead of
   the whole log" died before implementation.
3. A grace is a **give-up, not a cost** — and that is the finding the first two
   were circling.

## The dose, corrected twice

`chatty=N` was used as the dose for three probes running. It is only a dose if
per-Run output rate is fixed, and across harnesses it is not.

Same host, same binary, same `chatty=8`, quiet Runs all `sleep 86400`:

| fleet program | stop |
|---|---|
| `/bin/sh -c 'while :; do echo <64 ch>; done'` (sh → dash) | **110 ms**, 12/12 in 109-114 |
| `/bin/sh -c` + 32 ch | 12.5 ms |
| `bash -c` + 64 ch | 16.7 ms |
| `bash -c` + 32 ch | 12.4 ms |

Neither factor produces the cliff alone; only their product does. The obvious
next reading — aggregate byte rate — is also wrong, and sharply:

| n | shell | reads/s | **B/s** | stop |
|---|---|---|---|---|
| 8 | bash | 108,625 | **275 M** | 24 ms |
| 4 | dash | 139,273 | **312 M** | **110 ms** |
| 8 | dash | 215,952 | 265 M | **110 ms** |

The 312 MB/s cell plateaus and the 275 MB/s cell does not. Aggregate read rate
saturates near 270 MB/s in every loud cell, so bytes cannot separate them.

Sorting all six measured cells by **read()/message rate** is monotone, 6/6:

| reads/s | B/read | stop |
|---|---|---|
| 69,556 | 2105 | 12 ms |
| 80,480 | 2303 | 13 ms |
| 108,625 | 2529 | 24 ms |
| 139,273 | 2238 | 110 ms |
| 182,099 | 1445 | 110 ms |
| 215,952 | 1227 | 110 ms |

Every `read()` is one `record_output`, one append message, one slot of
`PERSISTENCE_QUEUE_CAPACITY`. A queue counted in messages is blind to bytes per
message, which is exactly the insensitivity the data shows.

Two probes died on the way here and are worth naming so they are not re-run: a
`/bin/sh -c` wrapper on the *quiet* Run (12.6 / 13.0 / 13.9 ms across direct,
shell, and exec-away — no effect), and `du` on the state dir as a rate meter
(it reads **negative**; checkpointing shrinks the WAL). The daemon's own
`/proc/<pid>/io` `rchar`/`syscr` is the instrument that works.

## The defect

`t_stop` saturates at 110 ms because the grace gives up there. The real quantity
was never measured by anything in this project. Polling `status` after `stop`
returns, until the Run is no longer `Running`:

| | below knee | at the plateau |
|---|---|---|
| `t_stop` | 13 ms | 110 ms |
| `t_visible` | 3.3 ms | **610 → 1874 ms**, monotone over 10 successive stops |

Publication is 6-17× larger than the clipped instrument could show, and it grows
with every stop: a convoy. And the consequence is not latency. `remove` rejects a
Run whose state still reads `Running` (`creation.rs:2590`):

```
QUIET    stop-then-remove: ok=10 failed=0
PLATEAU  stop-then-remove: ok=0  failed=10
  InvalidRunState: Run ... is still running; stop it before removing
```

That is precisely the shape `a_returned_stop_leaves_the_run_immediately_removable`
(`native_lifecycle.rs:5281`) exists to defend — its own comment calls it a real
agentmux-facing bug, found because "a warm client lands inside" the window a cold
CLI's ~12 ms of process spawn used to hide. The test is green because it runs on
an idle daemon where publication takes 1-6 ms.

So `TERMINAL_VISIBILITY_GRACE`'s doc is incomplete where it says exceeding it
"costs a stale `state` field in one receipt, never a failed Stop". The stale
state field **is** the failure: `remove` reads that same field.

## The candidate, and why it is dead

`PERSISTENCE_QUEUE_CAPACITY: 1_024 → 64`. Depth buys burst absorption only —
appends `try_send` and drop rather than block — so a shallower queue shortens
what a `finalize`'s blocking `sync_channel(0)` round-trip waits behind without
being able to stall the fleet.

Pre-registered before either binary existed: *SHIP requires 0/10 remove failures;
if failures stay at 10/10, the depth is the wrong knob — kill the candidate, do
not tune the number.*

Four rounds, arms alternated by round parity, both binaries marker-verified:

| | remove failures | `t_after` stop 1 → stop 10 |
|---|---|---|
| BASE (1024) | **40/40** | 0.85 → 3.38 s, monotone every round |
| QD (64) | **40/40** | 0.40 → 0.17 s, **no growth** |

The falsifier fires. The candidate is dead, and per its own pre-registration the
number does not get tuned.

The secondary outcome is worth keeping: the queue depth **is** the convoy. QD=64
removes the growth entirely — the 10th stop costs less than the 1st. But a single
finalize still costs 60-400 ms, far past the 100 ms grace, so every stop still
fails the remove that follows it. Depth explains the *escalation*, not the *level*.

## What is now excluded

- **Render a delta** — measured free (0.2 ms of a 110 ms plateau).
- **Publish before finalize** — load-bearing for crash consistency. Recovery
  rewrites any still-`running` row to `Interrupted{DaemonRestart}`
  (`persistence.rs:2588-2617`), so a client that observed `Exited` would find it
  interrupted after a restart. `creation.rs:1370/1381/1390` fails deterministically.
- **Let finalize jump the queue** — already proven to fail here: the Barrier's
  semantics *are* the FIFO position, and jumping latched the whole persistence
  layer daemon-wide.
- **Bound the queue depth** — this round's falsified candidate.

What that leaves is the message rate itself. The reactor reads **1227 bytes per
`read()` against an 8192-byte buffer** at the plateau: the sweep is running so
often that each fd has only a fraction of a buffer waiting. Six times as many
messages as the same bytes need, each one a queue slot a finalize waits behind.
That is the next thing to price, and it is a property of the reactor's sweep
cadence rather than of the persistence layer.

## The gate

`warmbench2` — the instrument every ratchet round from R10 onward has been scored
on — drives the **cheapest** of the four fleets (`bash`, 32 chars) and measures
~11 ms where the expensive shape is 110 ms with a 100% `remove` failure rate. It
does not enter this regime at any `chatty` count it tests. Every "no regression"
verdict this project has issued was issued about the cheap end of the curve.

That does not invalidate the prior rollbacks — a rollback triggered by a real
degradation is still a real degradation — but it means no prior round's *SHIP*
was ever tested against the shape where lifecycle verbs actually break.

## Method notes

Five predictions falsified this round, four of them mine and three before any
code was written: the curve shape (predicted concave, measured a step function),
the blocking site (guessed the queue blocked the owner thread; `append` uses
`try_send`), the render (measured free), the quiet-Run shell wrapper (no effect),
and the dose axis (twice — count, then bytes).

The constraint map also predicted the wrong rejection: it reasoned `remove` would
fail on the `Arc::strong_count != 1` gate at `creation.rs:2596`. The measured
refusal is the `is_running()` gate six lines earlier. Both produce a failed
remove; only one is fixable by changing how long a worker holds an `Arc`. Reading
the actual error string rather than the predicted one is what caught it.
