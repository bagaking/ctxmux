# R35 — the skip fired on 80% of entries and bought 7 µs

**Outcome: rolled back.** Predicted +0.280 ms/stop, measured +0.007. No metric
degraded, none improved; under the ratchet that is a revert, not a merge.

What makes this round worth a document is that the failure is _diagnosable_. A
flat result usually cannot distinguish "the change does nothing" from "the change
works and the cost is elsewhere". This one can, because the round counted.

## What was built

R34 put `drive_lifecycle` at 89% of the owner's CPU slope: every pass walks every
entry and pays a 128 B enum move, an `Arc` deref and a control `Mutex` before
finding nothing to do. R35 skipped entries with no queued command, no
`pending_stop`, and a shut pass-wide gate — an `AtomicBool` on the control
mirroring `child_commands.is_empty()`, readable without the lock.

## The measurement

Two arms, product binaries, 150 stops per shape, 4 alternating pairs, cn3 idle
(busy fraction 0.002). Owner thread `ctxmux-native-o`, schedstat ns resolution.

| arm  | live=24  | live=256 | slope         |
| ---- | -------- | -------- | ------------- |
| BASE | 0.241 ms | 1.230 ms | **+0.989 ms** |
| CAND | 0.232 ms | 1.214 ms | **+0.982 ms** |

Owner slope removed: **+0.007 ms/stop**. Blocking worker absorbed nothing
(+0.001), so the work did not move — it was never removed. Wall per stop moved
−0.017 ms, inside noise and far under the F5 gate's 0.112 ms A/A floor on the
target cell.

## Why it is not "the skip never fired"

A negative result is harder to catch being wrong than a positive one, so the
predicate was counted rather than assumed. Instrumented build, counts only, no
timing:

|                      | live=24        | live=256        |
| -------------------- | -------------- | --------------- |
| passes (open / shut) | 128 (27 / 101) | 704 (141 / 563) |
| passes per stop      | 5.33           | 4.69            |
| entries visited      | 3 291          | 180 929         |
| **skip rate**        | **76.4%**      | **79.7%**       |
| entries per pass     | 25.7           | 257.0           |

144 269 of 180 929 entry visits were skipped. The predicate works. Skipping four
entries in five removed 0.7% of the slope.

## What that leaves

The per-entry body — enum move, `Mutex`, `run_id()` — is **not** where the
gate-shut passes spend their +44.4 µs/pass. Something the skip still pays per
entry, or the walk itself, is.

The prime suspect is in the skip path I wrote. `has_child_commands()` is
`self.inner.pending_child_commands.load(..)`, and `self.inner` is an
`Arc<NativeControlInner>` — a pointer chase out of the `entries` slice into a
separately allocated control, once per entry. At 257 entries that is 257 likely
cache misses per pass, which is the right order for the slope that survived.

R33's arms had already priced this distinction and the round did not honour it:
a flag read _through the Arc_ left 3.2% of the modelled body standing versus 1.5%
for one reachable from the entry. **This round built `flag_arc` while calling it
`flag_entry`.** The 2× in R33's model looked negligible against costs that turned
out to be the wrong costs entirely; on the real slope that ratio is the whole
result.

## The rule

**A skip is only as cheap as the cheapest fact it needs.** The predicate has to
be answerable from memory the loop is already walking. Reaching through an `Arc`
to decide not to do work costs the same cache miss as doing it, so the skip
re-pays what it is skipping and the saving shows up as noise. Testing this needs
the count, not the clock: the timing said "flat", and only the skip rate said
which kind of flat.

## The next design, and why it is different in kind

Per-entry is the wrong granularity. The owner can decide **per pass**, in O(1):

- one epoch counter, bumped by the three `child_commands` producers, read once
  per pass — unchanged means no command arrived since the last sweep;
- a count of entries that can make progress with no new edge (a `pending_stop`
  awaiting its deadline or a permit, and anything in `WaitingCleanup` awaiting a
  permit), maintained by the sweep, which is the only writer;
- the existing pass-wide gate.

All three cheap and shut ⇒ skip the whole sweep, touching no entry at all. Read
the epoch _before_ sweeping and store it _after_, so a push racing the sweep
leaves them unequal and costs one spurious sweep — never a missed one.

On the measured mix this would skip roughly 400 of 704 passes at live=256
(~65.8 µs each on shut passes ≈ 175 µs/stop), which clears the A/A floor. Two
hazards are already visible and must not be argued away:

- **`WaitingCleanup` is woken by a freed permit, not by an edge.** A worker
  completion pokes the wake pipe but bumps no epoch, so it must be in the blocked
  count or a queued cleanup strands.
- **Fixtures drive terminality through `#[cfg(test)]` leader probes, not real
  exits**, so the fast path has to be disabled under test — which means no
  integration test can reach it. The decision belongs in a pure function that is
  unit-tested exhaustively on its own.

## Caveats

The counting build is instrumented and was used for counts only; every timing
here comes from clean binaries. The 4.69 passes/stop at live=256 differs from
R34's 4.0 because this harness holds the live count steady with an interleaved
start. The owner walks ~257 entries at live=256, again more than the configured
fleet, consistent with R34.
