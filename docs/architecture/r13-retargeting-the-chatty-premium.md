# Round 13 — `stop` was the wrong verb; where the chatty premium actually sits

No code change landed this round, and nothing was rolled back either: the round
spent itself on measurement and closed three directions before any of them
reached a build. What it produced is a retarget.

## The floor: half of `stop` is not recoverable by anything

Every earlier round chased `stop`, because `stop` is where the chatty cliff first
showed up. But `stop` has an intrinsic cost that no queueing fix can return:
`execute_cleanup` sends SIGTERM and waits for the child to actually die and be
reaped, then `await_terminal_visible` waits for publication, and warmbench opens
a fresh connection for every verb.

Running the same binary (`R11-COALESCE`, `bb4bbd8d`) at c0/c2/c8 separates the
two. c0 is the floor: whatever a verb costs with a silent fleet is what it costs
when there is no queue at all.

| verb   | c0 (floor) | c2   | c8   | premium (c8 − c0) | recoverable share |
| ------ | ---------- | ---- | ---- | ----------------- | ----------------- |
| start  | 6.40       | 15.0 | 27.7 | **21.3 ms**       | **77%**           |
| stop   | 5.16       | 5.99 | 9.68 | 4.5 ms            | 47%               |
| remove | 2.79       | 7.54 | 13.8 | **11.0 ms**       | **80%**           |

Six arms, both orders, each pair agreeing within 3% — which also clears cn3's
usual ~1.41x second-arm bias for this shape.

So `stop` is the smallest pot and the least recoverable one. `start` alone has
nearly five times the headroom. Three rounds of attention went to the verb with
the least to give, because the cliff being _visible_ in `stop` was mistaken for
the cost being _in_ `stop`.

## Segmenting the two verbs that do have headroom

`start` and `remove` both run on the persistence actor thread and both route
through the same prologue: `truncate_wal_to_zero` → `release_memory` →
`disable_cache_spill` → body. An in-process probe stamped three stretches on the
actor thread — how long the command sat in the FIFO, how long the fold took, and
everything after — with the probe binary run with logging off as an A/A control
in every arm.

c8, p50 (three independent probe builds, agreeing within 15%):

| verb   | queue    | fold | body | sum  | client-side |
| ------ | -------- | ---- | ---- | ---- | ----------- |
| start  | **16.6** | 4.3  | 5.3  | 26.7 | 28.4        |
| remove | **6.4**  | 3.0  | 2.0  | 11.6 | 12.2        |

The sums close against the client-side medians (within round-trip), so the
segmentation is measuring the right thing.

Against the recoverable premium, queue is **81%** of `start`'s and **69%** of
`remove`'s. The fold — the standing c005 suspect — is real but is about a fifth.

## Two directions closed without a build

**The output sweep (c011).** A command arriving mid-sweep waits out the reactor's
read stretch. Measured: 0.018 ms at c2, 0.060 ms at c8, against a 6.7–8.6 ms
`stop`. 0.7%. Reordering the loop can return at most that.

**The cleanup permit.** `CLEANUP_MAX_ACTIVE = 8` against `POP = 8` looked like a
designed-in contention point. It is not, for two independent reasons, and neither
needed an experiment:

- warmbench's stop loop is **serial** — `for id in &fresh { ...await }` — so there
  is never more than one outstanding `stop`. A limit of 8 cannot bind against a
  concurrency of 1.
- a permit-starved `stop` is refused with `ControlBackpressure` after the 250 ms
  admission deadline, and warmbench counts refusals. Across all 32 arms of the
  round-12 A/B: `stop=0 remove=0`. The admission never timed out, not once.

The arithmetic fit — 8 against 8 — was the whole basis for the suspicion. That is
the third time in this project a coincidence that fits has been mistaken for a
mechanism.

## What is NOT established: why the queue wait is as long as it is

The granularity is striking. At c8 the actor received **121,723 Append commands**
carrying 138.8 MiB, and committed them in **653 transactions**:

|                                        | c2      | c8          |
| -------------------------------------- | ------- | ----------- |
| bytes per Append command               | 0.7 KiB | **1.2 KiB** |
| Append commands merged per transaction | 74      | **186**     |
| depth at a `start`'s enqueue (p50)     | 15      | **273**     |
| transaction cost                       | 1.52 ms | 2.93 ms     |

Every PTY read becomes its own render, its own queue slot, and its own three
mutexes — and 185 of every 186 of those renders are merged away by the actor
regardless. That looks like an obvious lever: fewer, larger appends.

**It does not survive the closure check.** A `start` enqueued behind 273 commands
of 1.2 KiB is behind 328 KiB, which is under the 1 MiB transaction budget, so the
actor should drain all of it in one transaction of roughly 4 ms and serve the
`start` next. The measured wait is 16.6 ms. The sub-segments miss the parent by
**3.8x**, and this project's own rule is that a 4x gap is a noise signal, not a
finding.

The actor is also only ~40% busy, so this is not saturation. Something in the
queue wait is unaccounted for.

One hypothesis fits the arithmetic: `admit_transaction_with_shutdown` folds the
WAL when it is already past 8 MiB, so an Append transaction ahead of the `start`
can itself pay a fold, and 4 ms of drain plus ~12.8 ms of 8 MiB fold is 16.8 ms
against a measured 16.6. The `start` would then pay the fold twice — once through
the append in front of it, once as its own 4.3 ms.

That fit is recorded as a lead, explicitly **not** as a diagnosis. It is the same
shape as c003 and as the permit hypothesis above: arithmetic that lands on the
observed number. The next round's first job is to instrument the fold inside
`admit_transaction` and find out, and only then decide whether the lever is
producer-side coalescing, the fold trigger, or neither.

## What this round changes for the next one

1. Chase `start` and `remove`, not `stop`. `stop` is 47% floor.
2. The queue, not the fold and not the body, is where the premium sits — but the
   queue wait is not yet explained, and acting on the granularity number before
   it closes would be acting on a 3.8x gap.
3. Two more directions are closed: loop reordering (0.7%) and permit admission
   (cannot bind against a serial client, zero refusals observed).

Tree unchanged at `86b5676`; every probe reverted; 243 tests green with the
scaffold in place before it was removed.
