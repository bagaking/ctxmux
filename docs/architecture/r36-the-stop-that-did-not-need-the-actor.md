# R36 — the Stop was waiting for something only `remove` wanted

**Fixed.** The defect filed in
[`r26-the-finalize-that-blocks-a-stranger.md`](r26-the-finalize-that-blocks-a-stranger.md)
is closed, and closed without touching the finalize/publish ordering that doc
warned against. `cargo test -p ctxmux-daemon --lib` is **250 passed / 0 failed**
for the first time in four rounds.

## The one edge

R26 established the mechanism and stopped short of the fix, because the obvious
fix — publish terminal state _before_ the durable finalize — inverts a
deliberately chosen crash-consistency ordering. That framing assumed the Stop
path needed terminal visibility at all.

It did not. The whole coupling was one call:

```rust
// recoverable_stop_response, before
let (run, result) = flight.resolve().await;     // child reaped, off-actor
run.await_terminal_visible(now + TERMINAL_VISIBILITY_GRACE).await;  // <- the edge
```

`flight.resolve()` is where the Stop's actual work happens, and it never touches
the persistence actor. The wait that followed it does, transitively:
`await_terminal_visible` returns when `is_running()` clears, `is_running` reads
the in-memory state that only `publish_terminal_state` writes,
`publish_terminal_state` runs after the blocking `persistence.finalize`, and
`finalize` is a rendezvous on `sync_channel(0)` served by **one** actor thread.
So every Stop queued behind every other Run's durable finalize.

Deleting that one call makes the red fixture pass in **0.09 s**, down from
12.05 s of burning the grace.

## Why the wait existed, and where it belongs

R22 added it for a real defect: agentmux stops a Run and removes it on the next
line, and `remove` read the very state publication writes
(`validate_removable_entry`), so it came back `InvalidRunState`. That is a real
guarantee and it still holds — the wait just belonged on `remove`, the only
caller that ever needed it:

```rust
// RunManager::remove, after
if let Ok(run) = self.pin(id) {
    run.await_reaped_publication(now + TERMINAL_VISIBILITY_GRACE).await;
    drop(run);   // validate_removable_entry requires a unique owner
}
```

This is a relocation, not an elimination. `remove` still pays the latency, and it
already coupled to the same actor through `remove_terminal`, so no new coupling
is introduced. What changes is that a Stop no longer pays for a stranger.

The predicate moved too. The natural reading of "reaped and closed" is
`closed_quiescence_result()`, and it is the wrong gate here: `mark_closed` runs
_after_ the Stop reply is sent (`native_runtime.rs`, `execute_cleanup`), so a
just-stopped Run reads not-quiescent for a few lines and the wait would be
skipped exactly when it is needed. `mark_reaped` runs _before_ the reply, so
`await_reaped_publication` gates on reap instead. A Run whose child is not reaped
is genuinely live, gets no wait, and `remove` refuses it promptly — the
fast-refusal path is preserved by construction, not by a timeout.

## Why this is inside the contract

`docs/protocol.md` already promised it, in the `stop` receipt's own description:

> Public `exited` publication remains a later lifecycle event, so the returned
> `RunInfo` can still say `running` while no owned process remains.

The wire has always allowed the receipt to precede publication. The daemon was
being stricter than its own contract, and paying for that strictness with a
cross-Run stall. Nothing in the protocol changes here.

## What the two R22 tests were actually asserting

Both asserted on `accepted.run.state != Running` — a _proxy_ for "remove will
work", and a proxy the protocol explicitly declines to promise. After the fix
they failed on that proxy alone, never on the `remove` they exist to protect.

They now assert the `remove`. That is the property agentmux needs, it is the
property the protocol permits testing, and it is strictly stronger than the
field check: under mutation the loud-fleet test reports the real
`InvalidRunState` from `remove` on 8/8 attempts, where before it reported
"receipt still reported Running" and never reached the call.

## The guards, watched failing

Both new guards were mutation-tested, because a guard nobody watched fail is not
a guard.

| mutation                                            | guard                                                                 | result                                                       |
| --------------------------------------------------- | --------------------------------------------------------------------- | ------------------------------------------------------------ |
| delete `await_reaped_publication` from `remove`     | `remove_waits_for_the_publication_a_reaped_run_is_owed`               | FAILED — remove refuses instead of waiting                   |
| same                                                | `a_returned_stop_is_removable_while_the_fleet_is_loud`                | FAILED — 8/8 `InvalidRunState` from `remove`                 |
| re-insert the wait into `recoverable_stop_response` | `durable_finalize_keeps_reads_responsive_and_late_output_memory_only` | FAILED at creation.rs:1202, 12.04 s — the original signature |

The new deterministic fixture matters because the two R22 tests reach the window
only when the host cooperates: they make the actor _busy_ and hope. The new one
parks it exactly with `pause_next_finalize`, so the wait is either present or the
test is red. It also pins the decoupling from the other side — its Stop is
asserted to return while the actor is parked.

`lib.rs` was restored from a byte-identical copy after each mutation (md5
`47c15ff853d81aab6cb04caaf1bdd8c5`), since a stale test binary looks exactly like
a surviving mutant.

## What this does NOT do

Crash consistency is untouched. `publish_terminal` still calls `finalize` before
`publish_terminal_state`, so ADR 009's "durable state remains Running until
finalize returns" still holds and recovery still rewrites a still-running durable
row to `Interrupted { DaemonRestart }`. The publish-before-finalize direction
that R26 flagged as needing its own round **was not taken**, and still needs one
if anyone wants it: it would let a client observe `Exited { graceful }` and then
recover as `Interrupted`, a terminal-state downgrade nothing permits. It is also
no longer necessary for this defect.

The latency coupling is not gone from the daemon — `remove` still waits on the
single persistence actor, bounded by `PERSISTENCE_QUEUE_CAPACITY`. Making that
actor concurrent is a separate question this round does not open.

## Where the honest baseline now sits

`cargo test -p ctxmux-daemon --lib`: **250 passed, 0 failed** (was 247/1 on
`c35f621`; +2 net tests from this round's guard and the r26 count drifting with
intervening rounds). `npm run typecheck` is also green again after four
`RunSpec.size` sites left behind by `3dba591` were renamed — so for the first
time the claim "the suite is green" needs no asterisk.
