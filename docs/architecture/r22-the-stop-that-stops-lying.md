# Round 22 — the Stop stops lying, and the remove that follows it succeeds

**Verdict: SHIPPED.** Two constants, one defect closed, no degradation outside the
measured noise floor.

## What was wrong

A `stop` immediately followed by a `remove` failed **every time** under a loud
fleet. Not slowly — it failed:

```
ctxmux request failed (InvalidRunState): Run <id> is still running; stop it before removing
```

`recoverable_stop_response` waited up to `TERMINAL_VISIBILITY_GRACE` (100 ms) for
terminal state to become visible, and when that expired it answered anyway, with
the `state` field as it stood. `validate_removable_entry` (`creation.rs`) reads
that same field. So the timer did not cost latency — it converted **slow** into
**wrong**.

The constant's own doc comment asserted the opposite:

> Exceeding it costs a stale `state` field in one receipt, never a failed Stop.

Locally true — the Stop call itself succeeds — and globally false: it poisons the
caller's next operation. That comment is corrected in this commit, because it was
the root of the misreading, not merely a stale note.

## Why 100 ms was never enough

Publication is 1-6 ms on an idle daemon, which is what the grace was sized for.
But publication can sit behind a durable `finalize`, and under a loud fleet that
finalize is queued behind the appends it must be ordered after. Splitting
`finalize`'s two blocking waits (measurement-only build):

| | `send` (wait for a slot) | `serve` (wait to be served) | queue depth at entry |
|---|---|---|---|
| quiet | 6-10 µs | 1.9-2.9 ms | 0 |
| plateau | 1 µs - 24 ms | **296-485 ms** | **1024 (full)** |

Properly queued, not starved. The crash-consistency requirement doing its job.
Memory-only, same fleet, carries a *higher* message rate (265k vs 221k reads/s)
and has no defect at all — which exonerates the reactor, the PTY path and the Stop
machinery, and killed "batch the reactor's reads" before it cost a round.

## Two symptoms, two mechanisms

The defect had a *level* and a *slope*, and one knob each. Pricing them separately
would have judged a real combination dead — as it nearly did:

| arm | c8 remove | c8 `t_stop` | grows per stop? |
|---|---|---|---|
| BASE (100 ms, 1024) | **0/40** | 110 ms | flat — because it is lying |
| grace 10 s alone | **40/40** | 0.5-3.5 s | **climbs** 1.2 → 3.5 s |
| depth 64 alone (R21) | **0/40** | — | climb gone |
| **both** | **40/40** | 241-487 ms | **flat** (halves 0.92-1.11x) |

Raise the bound → the answer stops being wrong.
Bound the depth → the wait stops escalating.

R21 rejected depth-64 as a failed candidate. It was not a failed candidate; it was
half of one.

### Why depth is the escalation

An audit of this path turned up the mechanism, and with it a second false comment.
`PERSISTENCE_QUEUE_CAPACITY`'s doc claimed that blocking start/finalize/shutdown on
this queue "cannot stall the fleet", on the reasoning that they are per-Run
transitions on their own callers. That holds for start and shutdown. It does not
hold for `finalize`: it is called from `publish_terminal` on one of only **eight**
shared `ctxmux-native-blocking` workers, and a blocked finalize holds its pool
permit for the whole wait. Eight of them blocked together and
`start_worker_jobs`'s `while active.len() < CLEANUP_MAX_ACTIVE` dispatches nothing
further — cleanup or finalize — for *any* Run.

So the queue depth is not just how far the fleet runs ahead of one fsync; it is the
bound on how long a scarce daemon-wide worker is held. That is why cutting it
flattens the escalation, and why the level is a separate problem needing a separate
knob. Both comments are corrected in this commit.

## The confirmatory gate

12 rounds × 4 arms × 4 shapes, arm order rotating by round (this host systematically
favours the later arm — measured 1.41x on remove). **Two of the four arms, AA1 and
AA2, are byte-identical copies of BASE** (md5-verified at run start), so the A/A
comparison measures what the host alone produces on each exact cell and verb.

**Primary — the defect:**

| shape | BASE | AA1 | AA2 | COMBO |
|---|---|---|---|---|
| c0 | 120/120 | 120/120 | 120/120 | 120/120 |
| c1 | 120/120 | 120/120 | 120/120 | 120/120 |
| c2 | 120/120 | 120/120 | 120/120 | 120/120 |
| **c8** | **0/120** | **0/120** | **0/120** | **120/120** |

The A/A arms are the load-bearing rows. Identical bytes to BASE, identical failure:
the defect is in the code, not in the host, the arm order, or the day's load.

**Rollback trigger — every cell against its own A/A floor:**

| shape | verb | COMBO−BASE median | A/A floor | verdict |
|---|---|---|---|---|
| c0 | start / stop / remove | +0.059 / +0.085 / +0.077 ms | 0.560 / 1.133 / 0.540 | flat |
| c1 | start / stop / remove | −0.105 / +0.051 / −0.174 ms | 1.160 / 1.351 / 1.120 | flat |
| c2 | start / stop / remove | −0.139 / +0.253 / +0.441 ms | 0.984 / 2.095 / 1.194 | flat |
| c8 | start | −20.720 ms | 52.343 | flat |

Zero cells degraded. The c1 `stop` signal that looked directional over 4 pairs
(+1.05x, slower in 3/4) resolves at 12 pairs to **+0.051 ms against a 1.351 ms
floor** — 26x smaller than what two identical binaries produce. It was the host.
Three prior rounds were vetoed by the lightest shape; this one is clean there.

## The c8 numbers that look like a regression

```
stop    BASE 111 ms → COMBO 373 ms
remove  BASE 3.9 ms → COMBO 370 ms
```

**BASE completes 0/120 removes at c8.** Its 3.9 ms is a *failed* remove, not a fast
one. Comparing a failed operation's latency against a completed one is not
like-for-like; per `docs/benchmark-comparison-conventions.md` this is a **capability
difference**, not a loss. It is scored as neither a win nor a degradation, and the
adjudicator encodes that rule rather than applying it after seeing the numbers.

The honest statement of the cost: publication at the plateau takes 0.3-0.5 s, it
always did, and the system now reports it instead of answering `Running` and
breaking the next call. c8 `start` also improves 20.7 ms — bounded depth stops the
creation path queueing behind a full queue — but that sits inside the A/A floor and
is claimed as flat, not as a win.

## What this does not do

**It does not make publication faster.** It makes the system stop lying about it.
A 0.3-0.5 s stop under a loud fleet is still slow, and journald's other half —
batching the fsync off the hot path, delivering durability as a completion ordered
after the drained queue rather than as a caller blocking in FIFO — remains the real
fix. That is a separate round with a separate gate.

Both numbers here are chosen, not derived: 10 s is a backstop far past the measured
0.3-3.6 s publication, and 64 is where R21 measured the convoy growth vanish. They
are a bound and a brake, not a design.

## The test

`a_returned_stop_leaves_the_run_immediately_removable` has guarded this invariant
since the original agentmux report, and it was green throughout — it runs on an
idle daemon, where finalize is served in 1-6 ms and the bound is never reached. A
test that cannot fail on the shape where the defect lives is not a guard.

`a_returned_stop_is_removable_while_the_fleet_is_loud` asserts the same invariant
with the persistence queue actually full. Both arms build from identical test
source; only the two constants differ:

| arm | new test | existing test |
|---|---|---|
| 100 ms grace, depth 1024 | **FAILS** — 3/8 attempts refused | passes |
| 10 s grace, depth 64 | passes | passes |

The first version of it passed on both arms, which made it worthless, and the
reason is worth keeping: a four-Run fleet reached only 98k reads/s, and every Stop
settled in 10-28 ms with the receipt already terminal — the bound was never
reached. The farm plateau is ~220k reads/s. Eight Runs crosses into it. A load
shape is not "loud" because it is described that way; it is loud at a measured
rate, and the rate has to be checked rather than assumed.

## Excluded by measurement this round

- render a delta on publication — measured free (0.2 ms of 110 ms);
- publish before finalize — crash consistency: recovery rewrites any still-`running`
  row to `Interrupted{DaemonRestart}`;
- lifecycle commands jumping the queue — latches the persistence layer; libevent's
  own docs confirm strict priority starves and offer no fair mode;
- batching the reactor's reads — the memory-only floor carries more messages with no
  defect, and the 1227-byte reads are kernel-inherent (`N_TTY_BUF_SIZE` is 4096, so
  the 8192-byte userspace buffer was never the binding constraint).
