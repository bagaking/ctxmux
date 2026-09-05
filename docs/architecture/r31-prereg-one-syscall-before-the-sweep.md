# R31 pre-registration — one syscall to decide whether the sweep is needed

Written before the change. Predictions and falsifiers are here so the data can
contradict them.

## The finding that killed this round's first candidate

The round opened aiming at teardown, which loses worst to tmux (5.931 ms vs
2.657 ms). Splitting teardown by layer at the fleet size where that loss was
measured (8 live Runs, 16 reps, no strace, interleaved arms):

| verb   | memory-only | persistence ON | persistence costs |
| ------ | ----------- | -------------- | ----------------- |
| start  | 1.009 ms    | 2.360 ms       | +1.351            |
| stop   | 0.844 ms    | 1.961 ms       | +1.117            |
| remove | 0.187 ms    | 1.431 ms       | +1.244            |

| layer                              | stop+remove  | against tmux 2.657 ms |
| ---------------------------------- | ------------ | --------------------- |
| process/PTY teardown (memory-only) | **1.031 ms** | **we win 2.58x**      |
| plus the durable commit (shipped)  | 3.392 ms     | we lose 1.28x         |

**The process-teardown layer already beats tmux by 2.6x.** All 2.361 ms of the
loss (70%) is durable commit — two separate full commits, one in stop's finalize
and one in remove's own transaction, separated by a client round trip so the
actor's Append batching cannot merge them. Both are the promise each verb makes;
tmux is cheap because it makes neither. That share is the price of the contract,
not waste, and R20 already vetoed weakening it (`synchronous=NORMAL` cost c8
start +2.68 ms).

So the teardown gap is not this round's target. Saying so is the result.

## What this round does target

A separate, real defect found on the way: **stop's cost scales with the number
of live Runs.** Records held constant at 256, only the live/terminal split moved:

| condition       | live Runs | stop median  |
| --------------- | --------- | ------------ |
| mostly-terminal | 24        | 0.778 ms     |
| mostly-live     | 256       | **1.213 ms** |

**+0.435 ms (1.56x) from live Runs alone**, same record count, both interleaved
passes agreeing. Confirmed by a fleet sweep: stop 0.860 → 1.686 ms from fleet 4
to 384.

### Mechanism

`drive_lifecycle` peeks _every_ watched leader on every owner edge. The call
site says why: "SIGCHLD is process-wide and carries no pid we consult, so an
exit signal means only 'some watched child may now be terminal' — hence the
whole set is re-peeked." That is correct about the signal and it is why the
timed 20 ms sweep could be removed. But it means an edge caused by a _command_
— every Stop is delivered on one — pays a peek per live Run for an exit that
usually has not happened.

Traced: 9 waitid/stop at fleet 4, 1482 at fleet 384 (165x).

### The change

`waitid(P_ALL, WNOHANG|WNOWAIT)` answers "has any child exited?" in one
syscall. Verified on cn3 rather than assumed:

| property                             | result                                       |
| ------------------------------------ | -------------------------------------------- |
| P_ALL peek names an exited child     | yes, `si_pid` filled                         |
| non-consuming                        | yes — repeat peeks return the same pid       |
| can enumerate others without reaping | **no — stuck on one pid until it is reaped** |
| advances after that pid is reaped    | yes                                          |

The third row rules out the tempting version: a "drain the exits with peeks"
loop does not exist, and a loop that reaps as it goes would move reaping away
from the sequenced `reap_leader`, which is a correctness change for a 0.435 ms
prize. Not that.

What the first row _does_ buy is a gate. Every un-reaped leader is a direct
child of the daemon (`require_waitable_anchor`), so **if the P_ALL peek reports
no exited child, no watched leader can be terminal**, and the whole per-Run peek
loop can be skipped. One syscall replaces N.

The gate only skips the `leader_is_terminal` peek. Command draining, stop
admission, and the cleanup/finalize queueing in the same pass are untouched —
that is what delivers Stop, and skipping it would stall lifecycle work rather
than speed it up.

## Predictions

1. **stop's live-Run slope largely disappears.** The 24→256 live delta falls
   from +0.435 ms to under +0.15 ms. Sign and rough magnitude, not just sign.
2. **waitid calls per stop stop tracking the fleet.** At fleet 384, well under
   the measured 1482 — the gate makes the common edge cost ~1 peek instead of
   one per live Run.
3. **Small fleets are unchanged.** At fleet 8 every verb stays within its A/A
   floor: with few Runs there is nothing to skip, and the gate adds one syscall.
   This is the prediction most likely to fail, because the gate is pure overhead
   in exactly this shape — and it is the shape the tmux comparison uses.
4. **Nothing detects an exit later than before.** The gate is conservative in
   the safe direction: it can only skip when the kernel reports zero exited
   children. Terminal detection latency and idle CPU are unchanged.

## Falsifiers — any one rolls this back

1. Any cell degrades beyond its empirical A/A floor under Holm-Bonferroni.
2. Prediction 1 fails: the live-Run slope does not shrink materially. Then the
   sweep was not the cost and the mechanism story is wrong.
3. Prediction 3 fails: small-fleet verbs get slower. A scaling win paid for by
   the shape we are actually losing is not a win.
4. A new test failure attributable to the change — attributed by mechanism first
   and then by repetition, not by a single gate run (this round's n=5 probe
   called a 4/20-vs-0/20 result a candidate regression).
5. Terminal detection is ever delayed, or an exit is missed, in the fault
   fixtures.
6. The gate ever skips a pass that also had command work to do — it must gate
   the peek only.

## Artifact risks

- **strace is not a measuring instrument here.** This round already misread
  traced wall clock as real: stop looked like 6.011 ms under `strace -f` and is
  0.860 ms clean. All timings come from untraced arms; strace is used only to
  count calls.
- **Calls inside a window are not calls on the critical path.** 1482 waitid at
  10 µs would be 14.8 ms, nine times the measured stop — most of that sweep runs
  on the owner thread concurrently. The live-vs-retained experiment, not the
  syscall count, is what establishes the cost is real; the prize is the measured
  0.435 ms, not anything derived from 165x.
- **A knob that never engages reports "flat".** The gate must be shown to fire:
  record the skip count, and require it to be non-zero in the large-fleet cell
  and to move with the fleet.
- The `remove` path spawns a thread per call and `start_worker_jobs` spawns one
  OS thread per cleanup/finalize job despite a comment calling them "eight
  shared workers". Both are real, both are out of scope here, and neither is
  worth a round at 0.12 ms/stop.
