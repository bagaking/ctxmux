# R33 — the structural theory is too small, and both research tracks missed it

> **WITHDRAWN BY R34.** The microbenchmark below under-prices its own subject by
> 10×: it models the loop body's shape but omits the call every entry makes to
> `leader_is_terminal_gated()`. Segmenting the product binary puts
> `drive_lifecycle` at 82.76 µs/pass, not 7.85 — **89% of the owner's CPU slope,
> not 8.5%**. The "retires a class" claim and the closing section's instruction
> to stop looking at per-entry work are both wrong. See
> `r34-the-gate-left-the-stop-passes-paying-all-n.md`. What survives: the
> relative ranking within the modelled costs (the Mutex is 62% of _that_ subset)
> and the refutation of the pollfd-rebuild argument.

**Outcome: direction closed before any code was written.** One microbenchmark,
zero product risk, killed a candidate that two independent research tracks had
just spent their whole budget designing.

## What was proposed

R32 ended with a structural theory. Every pass, `drive_lifecycle` walks every
entry and pays:

1. a `mem::replace` of the whole `Lifecycle` enum out and back — sized by its
   largest variant, `WaitingCleanup`, which embeds an entire `Watching`;
2. a `Mutex` acquisition to ask whether any child command is queued;
3. an `Arc` deref to re-read a `RunId`;

and for 255 of 256 entries the answer is "nothing happened". Two research tracks
were commissioned and came back **contradicting each other**:

- **ready-set track**: keep a libev-style `pendings[]` queue plus a per-entry
  dedup bit, so the loop walks only entries with work. Cited libev's
  `ev_feed_event`, epoll's `rdllist`/`ovflist`, tokio's wake-by-token, Redis's
  `fired[]`, and systemd's deliberate O(N) counter-example. Claimed it "removes
  all 255".
- **atomic-flag track**: the ready set cannot pay off, because
  `poll_and_read_outputs` rebuilds a `PollFd` vector over every entry every pass
  — an O(N) floor that survives whatever `drive_lifecycle` does. Recommended
  only replacing the `Mutex` with an encapsulated atomic flag.

Both were well-sourced and internally coherent. Both were arguing about how to
divide a prize neither had measured.

## The measurement

A standalone microbenchmark replicating the loop body's _shape_ — same enum
layout (`Lifecycle` 120 B, largest variant `WaitingCleanup` 112 B), same
`Arc<Inner>` indirection, same `Mutex<State>`, with the `Arc` targets
deliberately scattered across the heap so cache misses are not hidden by
accidental contiguity. All entries idle, which is the case under test.

| variant      | what it prices                                     | slope 24→256, per pass |
| ------------ | -------------------------------------------------- | ---------------------- |
| `full`       | today: replace + Arc deref + lock + take + restore | **+0.00785 ms**        |
| `no_lock`    | same minus the Mutex                               | +0.00297 ms            |
| `flag_arc`   | flag read through the Arc, skip                    | +0.00025 ms            |
| `flag_entry` | flag read from the entry, skip                     | +0.00012 ms            |

The pass multiplier is already measured: **4.0 owner passes per stop, at both
fleet sizes**.

```
0.00785 ms/pass x 4.0 passes/stop = 0.031 ms per stop
```

against a residual slope of **~0.363 ms**. **The entire structural mechanism —
mutex, enum move, Arc deref, all 256 entries — is under 10% of the target.**

Perfectly executing _either_ research track's design wins at most 0.031 ms, and
realistically ~0.019 ms (the mutex share). That is below the F5 gate's A/A floor
on the target cell (0.112 ms): **unmeasurable by the very gate that must approve
it.**

## What the two tracks got right, and why it did not matter

The internal question they disagreed on was real, and the benchmark settles it:

- the Mutex is **62%** of the body's slope — the atomic-flag track was right that
  it is the dominant single item;
- a flag read **through the Arc** leaves only 3% standing, versus 1% for a flag
  in the entry — so the ready-set track's insistence that the skip decision not
  touch the `Arc` is directionally right but worth almost nothing here;
- the atomic-flag track's argument that "the pass is O(N) anyway" was **wrong as
  stated** — the pollfd rebuild was measured at 0.0002 ms/pass, three orders of
  magnitude below what it was being used to dismiss. But its conclusion (don't
  build the ready set) happens to be right, for a completely different reason.

Both tracks reasoned from mechanism to recommendation without ever pricing the
mechanism. That is the same error R32 made twice — and the reason this round
priced first is precisely that R32 made it twice.

## The rule this reinforces

**Price the mechanism before designing the fix, even when the mechanism is
obviously real.** All three costs here are genuinely present, genuinely O(fleet),
and genuinely wasteful; the profile genuinely showed them. None of that makes
them big. A 20-minute microbenchmark with no product risk was enough to close a
direction that would otherwise have cost a full implement-and-gate round, and
would have ended in "flat" — indistinguishable from R32's outcome, and equally
uninformative.

It also retires a class, not just a candidate: **per-entry work in the owner
sweep is capped at ~0.03 ms/stop** by the 4.0-pass multiplier and the measured
per-entry cost. No future round should propose optimizing the sweep body.

> **The paragraph above is withdrawn.** The cap was computed from a model that
> omitted the loop's syscall, and the real figure is ~0.225 ms/stop. The rule the
> round stated — _price the mechanism before designing the fix_ — is right and
> survives; what it got wrong is that **a microbenchmark of a loop body must
> include what the body calls**, or it prices a different function than the one
> named. A model that is 10× low retires the correct direction under the label
> "already excluded," which is far more costly than one that is 10× high.

## Where the slope actually is

**This section is withdrawn — see the banner at the top.** Two rows are wrong by
an order of magnitude, and the conclusion drawn from them points the next round
away from where the cost actually is. R34 replaces it with segment measurements
taken in the product binary:

|                                                             | cost per stop | share of ~0.363 ms |
| ----------------------------------------------------------- | ------------- | ------------------ |
| `poll()` scan                                               | 0.010 ms      | 2.8%               |
| the `waitid` gate (P_ALL, scales 0.4→6.6 µs)                | 0.026 ms      | 7.2%               |
| ~~entire `drive_lifecycle` body, all entries~~ **0.031 ms** | **0.225 ms**  | **62%**            |
| pollfd vector rebuild                                       | 0.001 ms      | 0.3%               |
| ~~unaccounted ~0.295 ms / ~81%~~                            | —             | —                  |

The error was not in the arithmetic but in the model: the microbenchmark priced
the enum move, the `Arc` deref and the `Mutex`, and omitted the per-Run
`waitid` peek that every entry pays whenever the pass-wide gate is open — which
is exactly the passes a stop causes. The `P_ALL` gate row is also understated:
measured per pass it runs 6.65 → 20.07 µs, so it does not hold constant either.
