# One Run's durable finalize blocks another Run's Stop receipt

**Status: FIXED in
[`r29-the-stop-that-did-not-need-the-actor`](r29-the-stop-that-did-not-need-the-actor.md).**
Found while clearing red tests before round 26, and filed here because the round
that fixes it needs the evidence, and because the fixture that catches it had
been red for three rounds while I called those rounds green.

The fix did **not** take the publish-before-finalize direction this doc flagged
below as needing its own round. That direction remains untaken and still needs
one. The Stop path simply did not need terminal visibility at all: the wait was
R22's, added for `remove`, and it now lives on `remove`.

## The symptom

```
tests::creation::durable_finalize_keeps_reads_responsive_and_late_output_memory_only
panicked at crates/ctxmux-daemon/src/tests/creation.rs:1202:10:
  another Run Stop receipt stays bounded during finalize: Elapsed(())
```

The fixture parks the persistence actor inside one Run's `finalize`
(`pause_next_finalize`, creation.rs:1164), then stops a **different, unrelated**
Run and requires the receipt within 2 s. It does not arrive.

## Why this is not the sibling's bug

The other red test in this suite was a fixture welded to a performance knob:
`a_steady_run_offers_only_new_bytes` hardcoded 64 pushes, R22 took
`PERSISTENCE_QUEUE_CAPACITY` to 64, the pushes stopped fitting. Repaired in
`17111bb`.

The obvious move was to assume this one is the same shape — R22 also raised
`TERMINAL_VISIBILITY_GRACE` from 100 ms to 10 s in the same commit
(`dcd33f1`), and 10 s > the fixture's 2 s bound. Sweeping the constant looks
like it confirms that:

| `TERMINAL_VISIBILITY_GRACE` | result | wall |
|---|---|---|
| 100 ms | ok | 5 s |
| 500 ms | ok | 5 s |
| 3 s | **FAILED** | 7 s |
| 10 s | **FAILED** | 15 s |

But read the wall-clock column. If the fixture were merely pinned to a stale
constant, the Stop would return promptly and the 2 s bound would be arbitrary.
Instead **the Stop returns exactly when the grace expires**. Under 2 s it
"passes" by giving up early; over 2 s it blows the bound. The Stop is blocked
for the entire grace, every time, at every setting.

That is the shape R21 already named: the grace expiring is a **give-up, not a
cost** (`ctxmux-the-grace-turns-a-latency-into-a-failed-remove`). Lowering the
grace back to 500 ms would turn this test green while leaving the block exactly
where it is — and would re-open the defect R22 shipped to close, where a Stop
answers with a stale `state` field and poisons the caller's next `remove`.

**Turning this test green by moving the knob would be the third round in a row
that a knob hid this.**

## The mechanism

`publish_terminal` (lib.rs:4387) is the publication path for terminal state.
On a persistent Run it calls:

```rust
persistence.finalize(self.id, pid, replay, terminal.clone());   // lib.rs:4413
let _output = mutex_lock(&self.output);
self.publish_terminal_state(terminal.clone());                  // lib.rs:4418
```

and `Persistence::finalize` is a **blocking rendezvous**:

```rust
let (reply_tx, reply_rx) = mpsc::sync_channel(0);               // persistence.rs:914
... send_lifecycle(Command::Finalize { ..., reply: reply_tx })
let _ = reply_rx.recv();                                        // persistence.rs:930
```

`publish_terminal_state` is what calls `terminal_visible.notify_waiters()`
(lib.rs:4434), and `await_terminal_visible` (lib.rs:4445) — the wait inside the
Stop receipt path at lib.rs:5619 — is the waiter.

So the chain is: the control Run's `Finalize` is a command on the lifecycle
lane. The lane is served by the single persistence actor. The fixture has that
actor parked inside a *different* Run's `Finalize`. The control Run's finalize
is therefore never served, its `publish_terminal` never returns, its terminal
state is never published, the waiter is never notified, and the Stop waits out
the full grace before answering with whatever state it has.

**R25 did not cause this and does not fix it.** R25 gave lifecycle commands
priority *at the dequeue point* — they overtake queued appends. This is not a
queue-position problem: the actor is not choosing what to serve next, it is
stuck inside serving one command. Priority cannot help a server that is busy.
This is KIP-291's mode 1 (blocked at admission), the mode R25's mechanism
explicitly cannot address, appearing on a path we had only ever analysed for
mode 2.

## Why the fixture is right

The sibling test `active_durable_finalize_cannot_extend_native_owner_shutdown`
(creation.rs:1317) parks the same barrier and asserts the native-owner shutdown
path stays live. That path was deliberately protected. This fixture asserts the
same property for four more lanes — input, resize, signal, and Stop — and the
first three pass. Only Stop is blocked.

So the suite already encodes "a parked finalize must not freeze unrelated
lanes" as a product property, and this is the one lane where it does not hold.

## What is actually at stake

The fixture uses a test barrier, but the barrier only *holds open* a window that
exists in production. R22's own measurement (`r22-the-stop-that-stops-lying.md`)
recorded a real finalize waiting **296–485 ms** to be served under a loud fleet
at queue depth 1024. During any such window, a Stop on an unrelated Run is
blocked for up to the full 10 s grace and then answers with a possibly-stale
state — which is precisely the failure R22 shipped to prevent, still reachable
by a different route.

## What this does NOT say

The 12 s the test burns is the fixture waiting out a 10 s grace, not a hang. No
data is lost, nothing deadlocks permanently, and the Stop does eventually
answer. The defect is a latency coupling between unrelated Runs plus a
stale-state answer at the end of it, not a liveness failure.

I have not yet established the fix. The obvious direction — publish terminal
state before the durable finalize rather than after — inverts an ordering that
was chosen deliberately, and `publish_terminal_state`'s own comment says the
ordering is defence in depth. Reversing it needs its own round and its own
crash-consistency argument, not a patch appended to this one.

**R29 update:** that direction was never needed. The Stop path did not require
terminal visibility — the wait it performed was R22's, added for `remove`'s
benefit, and `docs/protocol.md` explicitly permits the receipt to precede
publication. Moving the wait to `remove` closed the defect with the
finalize/publish ordering untouched. Publishing before finalize is still
unexplored and still needs its own crash-consistency round.

## Where the honest baseline now sits

`cargo test -p ctxmux-daemon --lib` on `17111bb`: **244 passed, 1 failed**. That
one failure is this defect. Any future claim that the suite is green must say
so explicitly.

**R29 update:** now **250 passed, 0 failed**. The asterisk is retired.
