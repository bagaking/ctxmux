# R32 — a hot line that was not a cost

**Outcome: rolled back.** The change was measured, found inert on two
independent instruments, and reverted. Nothing shipped. What follows is the
reasoning, including the parts that were wrong, because the wrong parts are the
reusable content.

## The target

Post-R31, `stop` still costs more when the fleet is larger: **+0.363 ms from 24
to 256 live Runs**, 6/6 pairs positive. Nothing about stopping one Run should
depend on how many neighbours exist, and unlike the ~70% of teardown that is
durable-commit contract, this is pure waste — which is what made it worth a
round.

## What the round established (these survive)

Each row is a measurement on cn3, and each one permanently removes a candidate.

| Candidate | Verdict | Evidence |
| --- | --- | --- |
| `poll()` per pass | dead | +0.0026 ms/pass × 4 passes = 0.010 ms, 2.8% |
| every per-pass full-fleet walk | bounded | **4.0 owner passes per stop at both 24 and 256** — a flat multiplier, so no per-pass walk can be large |
| the linear `find` in `apply_cleanup_outcome`/`set_lifecycle` | dead | sub-microsecond over 256 small structs |
| waiting (a lock or handoff) | dead | the slope is CPU, not wall-minus-CPU |
| `waitid` call count | dead | counts scale 9.4× (fleet 10.7×), but `waitid(P_PID)` is **flat at 0.2–0.3 µs**, so +233 calls = **0.066 ms**, not 0.93 ms |
| **`native_control.rs:646`, 18.6% of owner samples** | **dead — this round's change** | removing it moved owner CPU by −0.014 ms (4/6 pairs) and wall by −0.012 ms: noise on both |

Also established, and useful beyond this round: the owner thread carries the
whole slope — `ctxmux-native-o` goes **0.234 → 1.247 ms on-CPU per measured
iteration** (see the units note below) between
the two fleet sizes.

## The two instrument failures, and their fixes

**Jiffies cannot measure this.** `/proc/<pid>/stat` reports utime/stime in
jiffies (10 ms on cn3). Over 30 stops that is ±0.33 ms/stop of quantization —
the same size as the effect. Two probes disagreed by 16× (+1.333 vs +0.083
ms/stop) and **both were below their instrument's resolution**.
`/proc/<tid>/schedstat` (nanoseconds) resolved it. A disagreement between two
readings of the same quantity is a prompt to check resolution before checking
the code.

**An end-snapshot loses short-lived threads.** A thread created and destroyed
inside the window never appears in the final snapshot, and the short-lived
blocking workers were exactly the suspects. The sampler now records every tid it
ever sees.

## The mistake: a hot line is not a cost

perf's line-level profile at live=256 put **18.6% of owner-thread samples on
`native_control.rs:646`** — `self.inner.run_id`, a field read through an `Arc`.
The reading was: a field read can only be that expensive if it is a cache miss,
so this is a cold pointer chase per entry per pass. `entry.run_id` already holds
the same value (seeded from that very call in `into_entry`, and neither field is
ever reassigned), so the fix was one line reading warm memory instead of cold.

It did nothing. The F5 gate returned flat on all 12 cells; the ns-resolution
owner-CPU probe, which *can* resolve a 0.1 ms effect, returned −0.014 ms with
4/6 pairs — noise.

The reason is visible once symbol and srcline are printed **together**:

```
18.56%  [.] ctxmux_daemon::native_runtime::owner_main   native_control.rs:646
```

The samples are attributed to the symbol `owner_main` — line 646 is an **inlined
frame**. For a trivial `#[inline]` accessor, the line table maps the instruction
to the accessor's own declaration, even though the cost belongs to the
surrounding code. The line was a *label* for a cache miss, not its cause:
removing one of several derefs of the same cold pointer changes nothing, because
whichever access comes first still pays the miss.

**A line-level profile of an inlined, optimized build names where an instruction
was written, not what makes it slow.** When the symbol column says the samples
belong to a different function than the srcline column, the srcline is a
provenance record, not an attribution.

## Why this was cheap to be wrong about

The prediction and its falsifier were written down before implementing:

> Falsifier: if large-fleet `stop` does not move at all, then … the 18.6% is
> attribution error — the line table pointing at an inlined frame's nearest
> symbol rather than the real cost. That outcome kills the "cold pointer chase"
> reading of the whole profile, not just this line.

That is exactly what happened, so the result was a decision rather than a
puzzle. The same discipline had already killed my `waitid` theory inside this
round: the count scaled 9.4× with the fleet, which is precisely the signature
that invites a fix, and writing *"per-call cost must rise to ~4 µs or the model
is wrong"* first turned a plausible story into a refuted one before any code was
written.

**A count that scales is not a cost that scales. A line that is hot is not a
cost that can be removed.** Both were caught by pre-registering the number that
would prove them wrong.

## One caveat on the gate itself, honestly stated

The F5 gate reported "flat" on all 12 cells, but the cell that should carry the
effect — `stop persist live=256` — has an **A/A floor of 0.112 ms** against a
predicted effect of ~0.1 ms. That gate was underpowered for its own target, so
its "flat" means *cannot resolve*, not *proven zero*. The rollback does not rest
on it; it rests on the ns-resolution owner-CPU probe, which resolves well below
the predicted effect and also found nothing. Had only the gate been run, the
right conclusion would have been "unresolved", not "inert".

## A units correction, found after this doc was first committed

The owner-CPU figures above were originally written as "per stop". They are
not: the schedstat probe's sampling window wraps one **start + stop + remove**
per iteration, so `+1.013 ms` is per *triple*, not per stop. The per-verb split
is not known — the probe never separated them.

Nothing else in this document changes. The rollback rests on a *paired
difference* between two arms measured through the identical window, and the
candidate-killing numbers (poll(), pass count, `waitid` per-call cost) come from
probes with their own correct scopes. Only the absolute figure was mislabelled.

It is worth recording anyway, because it is the same defect this round is about:
**the instrument's scope did not match the thing being named.** That is the
lesson of [[ctxmux-daemon-wide-cpu-cannot-price-one-operation]], committed to
memory long before this round, and I reproduced it while writing the round up.
The next round needs the per-verb split as its prediction base, so it must
re-measure rather than inherit this number.

## What is left for the next round

The slope is real and still unexplained: **+1.013 ms of owner-thread CPU per
measured iteration** (start + stop + remove — see the units note below)
between 24 and 256 live Runs. The per-line reading of the profile is discredited
as *attribution*, but the profile's coarse shape still holds — the samples are
in `owner_main`'s inlined body and in the allocator, and the mechanism that fits
is structural rather than line-sized:

for **every** entry, **every** pass, `drive_lifecycle`

1. `mem::replace`s the whole `Lifecycle` out by value — sized by its largest
   variant, `WaitingCleanup`, which embeds an entire `Watching`;
2. takes a mutex to ask whether any child command is queued;
3. moves the whole thing back;

and for 255 of 256 entries the answer is "nothing happened".

The next attempt should therefore **neutralize the mechanism and watch the slope
collapse**, rather than price another line. Isolated pricing cannot see
externalities, and — as this round shows twice — it cannot distinguish a
correlate from a cause either.

R33's research narrowed this further, and one finding rules an option out before
it is attempted: **the pass is O(N) no matter what we do to `drive_lifecycle`**,
because `poll_and_read_outputs` rebuilds a `PollFd` vector over every entry with
an output fd on every pass. So an intrusive "entries with work" queue — the
structure that would turn the scan into O(work) — cannot collect its own
benefit while that O(N) floor stands, and it would reintroduce a per-entry
atomic as its dedup bit anyway: the same invariant, plus a shared queue, plus
re-queue-and-teardown logic.

That leaves a narrower and safer candidate: keep the O(N) walk, make each step
cheap. The per-entry mutex (~12% of owner samples) becomes an atomic flag cached
inside the lock, and — critically — **the flag is encapsulated in the queue type
rather than maintained by discipline at seven call sites**, so a future producer
cannot forget to set it. The liveness backstop already exists and is already
load-bearing: the buffered self-pipe byte is the sole production wake for child
commands today, so a stale-false flag read degrades to "recovered on the next
byte-driven pass", not to a stranded command.

