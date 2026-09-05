# R31 — one syscall before the sweep

Pre-registration: [`r31-prereg-one-syscall-before-the-sweep.md`](./r31-prereg-one-syscall-before-the-sweep.md).
Shipped. `stop` at a 256-Run live fleet: **+0.151 ms, 20/20 paired wins**, no
cell degraded.

## What shipped

`drive_lifecycle` peeked every watched leader on every owner edge, because
SIGCHLD is process-wide and carries no pid we consult. Every Stop command
arrives on such an edge, so `stop` paid for one `waitid` per live Run — work
that scales with the fleet and, on the overwhelming majority of passes, finds
nothing.

R31 puts one `waitid(P_ALL, WEXITED|WNOHANG|WNOWAIT)` in front of the loop.
Every un-reaped leader is a direct child of the daemon — that is what
`require_waitable_anchor` keeps true — so "no child has exited" _proves_ no
watched leader is terminal, and the whole sweep can be skipped.

## The candidate this round started with, and what killed it

The round opened aimed at the largest gap on the board: teardown loses to tmux
2.23x. Splitting the path by layer at fleet=8 — the shape the tmux comparison
uses — ended that plan in one experiment.

| layer                              | stop+remove  | vs tmux 2.657 ms |
| ---------------------------------- | ------------ | ---------------- |
| process/PTY teardown (memory-only) | **1.031 ms** | **we win 2.58x** |
| plus durable commit (shipped)      | 3.392 ms     | we lose 1.28x    |

Per-verb persistence cost: start +1.351, stop +1.117, remove +1.244 ms.

The teardown path was never slow. **70% of the loss is durable commit**, and
specifically _two separate full commits_ — stop's finalize and remove's own
transaction — separated by a client round trip, so the actor's Append-only
batching cannot merge them. Both are promises each verb makes to its caller.
tmux is cheap there because it makes neither. That is the price of the
contract, not waste, and R20 already vetoed weakening it (`synchronous=NORMAL`
bought stop+remove but cost c8 start +2.68 ms, reproduced across two batches).

So "teardown vs tmux" is retired as a gap topic. What replaced it is a
_scaling_ defect, and the distinction matters: holding records fixed at 256 and
varying only **live** Runs from 24 to 256 moved stop **0.778 → 1.213 ms
(+0.435, 1.56x)**, with both interleaved passes agreeing. That slope is the
sweep, and the sweep is what R31 cuts.

## Results

20 pairs × 10 reps, arm order alternating (this host runs the second arm
systematically faster), Holm-Bonferroni across all 12 cells, each cell judged
against its own empirically measured A/A floor.

| cell          | verb   | delta ms   | A/A floor | wins  | holm    | verdict |
| ------------- | ------ | ---------- | --------- | ----- | ------- | ------- |
| large-mem     | stop   | **+0.151** | 0.044     | 20/20 | 0.00002 | **WIN** |
| large-mem     | start  | −0.001     | 0.045     | 9/20  | 1.00000 | flat    |
| large-mem     | remove | +0.002     | 0.021     | 12/20 | 1.00000 | flat    |
| large-persist | stop   | +0.104     | 0.124     | 15/20 | 0.45528 | flat    |
| large-persist | start  | +0.004     | 0.102     | 11/20 | 1.00000 | flat    |
| large-persist | remove | −0.010     | 0.161     | 10/20 | 1.00000 | flat    |
| small-mem     | stop   | −0.012     | 0.038     | 6/20  | 1.00000 | flat    |
| small-mem     | start  | −0.014     | 0.047     | 6/20  | 1.00000 | flat    |
| small-mem     | remove | −0.001     | 0.020     | 9/20  | 1.00000 | flat    |
| small-persist | stop   | +0.039     | 0.172     | 12/20 | 1.00000 | flat    |
| small-persist | start  | −0.037     | 0.139     | 8/20  | 1.00000 | flat    |
| small-persist | remove | +0.033     | 0.107     | 11/20 | 1.00000 | flat    |

(delta = control − candidate; positive means the candidate is faster.)

The shape is the pre-registered prediction, not a lucky draw:

- the win lands **only** in the cell the mechanism is for — a large live fleet;
- `large-persist stop` carries the same sign and a similar magnitude (+0.104)
  but does not clear Holm. Its A/A floor is 0.124 — three times the mem cell's
  — so persistence noise is large enough to bury an effect this size. Reported
  as flat, because that is what the data supports;
- the four small-fleet cells are all inside their floors. The pre-registration
  named these as the likeliest failure, since the gate is pure overhead in
  exactly that shape _and it is the shape the tmux comparison uses_. One added
  syscall is too cheap to measure there — which is the good outcome, but it was
  not the guaranteed one.

## The engagement check

A knob that never engages also reports "flat", and a negative result of that
kind is much harder to notice than a positive one (see
`ctxmux-a-knob-that-never-engaged-reports-flat`). So the call count was
measured separately, at fleet 256:

| arm  | waitid/stop total | of which per-Run (P_PID) |
| ---- | ----------------- | ------------------------ |
| ctrl | 980.2             | 977.9                    |
| cand | 257.4             | **251.2**                |

**0.26x** — the gate fires and removes roughly three quarters of the per-Run
peeks. Counts only, deliberately: strace inflates wall clock several-fold, and
this round already had one conclusion poisoned that way (below). The timing
arms were run untraced and this probe was held until after they finished, so it
could not contend for the host.

## Why the gate is a gate and not a drain

The tempting next step — loop on `P_ALL` to drain the exit queue and turn
O(fleet) into O(actual exits) — **does not exist**. Measured on cn3 with five
exited children: `waitid(P_ALL, WNOWAIT)` reports a pid and genuinely does not
consume it, but it returns _the same pid_ until that child is reaped. Eight
peeks saw one distinct pid.

| property                              | result                                  |
| ------------------------------------- | --------------------------------------- |
| names an exited child                 | yes, `si_pid` is populated              |
| truly non-consuming                   | yes, repeated peeks return the same pid |
| enumerates the others without reaping | **no** — 1 distinct pid across 8 peeks  |
| advances after that one is reaped     | yes                                     |

Peeking-and-reaping would move reaping off the sequenced `reap_leader` path —
a correctness change — for a 0.435 ms target. Not worth it. So P_ALL is used
as a gate: one syscall answering "is there at least one".

## Staleness

The gate is read once per pass and deliberately not refreshed inside the loop.
Both ways it can go stale are safe:

- **stale `true`** (a worker reaped the exit that opened the gate mid-pass) just
  runs the per-Run peeks — exactly the behaviour without the gate;
- **stale `false`** would be the dangerous one: an exit arriving after the read.
  It cannot be lost. That exit raises SIGCHLD, whose relay writes a byte to the
  owner's `UnixStream` self-pipe, so the owner runs another pass with a fresh
  gate. The byte is _buffered_, not edge-triggered, and the owner drains the
  pipe inside `poll_and_read_outputs` — after `drive_lifecycle` has run — so a
  byte written mid-pass survives to wake the next one.

Two implementation constraints, both load-bearing:

1. **Any error answers `true`**, falling back to the per-Run peeks, which own
   the error handling. `ECHILD` is the exception — a definite "no children".
   A gate must never be the thing that hides an exit.
2. **The test probe is consulted before the gate.** Fixtures like
   `leader_probe` simulate a terminal leader with no real exited child behind
   it; gating first would make the sweep skip exactly the Runs those fixtures
   are about — a performance knob silently deciding what the tests can observe
   (cf. `ctxmux-a-fixture-sensitive-to-a-perf-knob-welds-it-shut`).

## What went wrong on the way here

**strace wall clock was treated as a measurement.** From a `strace -f` run I
declared "stop costs 6.011 ms with no persistence at all — more than start".
The clean number is **0.860 ms**: off by 7x. Caught only because a later
untraced scaling pass disagreed. Tracing prices syscall _counts_, never time.

**Blocking calls poisoned the syscall accounting.** Putting `futex` and
`epoll_wait` in the trace set produced "3240% of wall" and negative residues.
A negative residue is the self-check that says the table is unreadable.

**An auto-VERDICT over-reached and was refused.** A probe printed "the sweep is
the lever" from 165x call growth against 1.96x time growth. But 1482 calls at
~10 µs is 14.8 ms against 1.686 ms of wall — most of the sweep runs
concurrently, so the arithmetic refutes the claim. It was replaced with the
live-vs-retained experiment, which gave the defensible +0.435 ms.

**The wire shape was guessed again.** `stop` takes
`{"operation":{"daemon_instance","operation_key","id"}}`, with
`daemon_instance` coming from the hello reply's camelCase `runtime.daemonInstanceId`.
Guessing cost a traced run; reading `Request`/`RecoverableStop` and verifying
the shape with a throwaway script first cost seconds.

**The measured arm drifted from the committed tree.** A comment was added after
the farm arms were synced. Comments cannot reach codegen, but a prior round saw
a cosmetic edit change a binary, so it was proven rather than asserted — and
the control is what makes the proof readable: a no-op recompile of _identical_
source already moves `.text` (`76aa26cc…` → `55fffdf7…`), so this build is not
byte-reproducible. The commented build hashed to `55fffdf7…`, exactly the no-op
rebuild. The comment contributes nothing past recompilation noise.

## What this does not claim

This is a scaling fix, not a gap fix. It removes a term that grows with the
live fleet; it does not move the tmux comparison, which is run at fleet=8 where
the term is negligible. The remaining `stop` cost at scale is dominated by the
durable commit the verb promises, and that is an architectural question about
the protocol contract — not a ratchet round.
