# R37 — the barrier fenced offered, not read

**Fixed.** `upgrade_preserves_output_across_the_reader_window` goes from **2/12
to 12/12** paired on one host at `CTXMUX_TEST_TIME_SCALE=8`. The defect is real
byte loss across an exec-in-place upgrade, it predates this lineage, and the
test that catches it has been failing roughly 30% of bare runs since
`a7df798` introduced it on 2026-08-23.

## The claim that was one word too strong

`perform_exec_upgrade` takes a durable barrier after extract, and its comment
states the guarantee:

> Draining the FIFO barrier here fences every byte ever **read** before we exec,
> guaranteeing the persisted cursor covers all of them.

The barrier fences every byte ever **offered**. Those are the same set only
while a next push exists to re-offer a dropped one — and the line above the
barrier is what removes it.

Output admission is deliberately lossy. `PersistentRun::append` drops rather
than blocks (it is the daemon-wide reactor thread; blocking it stops every
Run's pty), and `record_output` skips the render outright when
`queue_has_room()` is already false. Both are safe for the same single reason:
only acceptance moves `offered_head`, so **the next push renders from the
unmoved watermark and carries the debt.**

`extract_live_descriptors` sets `entry.output = None` on every entry. There is
no next push. Debt outstanding at that instant is invisible to the barrier,
which returns satisfied, and the exec proceeds over it. Those bytes are then
unrecoverable in both directions: already drained out of the pty kernel buffer,
and the incoming image resumes from the persisted cursor.

## Attribution is quantitative, not correlational

Instrumenting each push with `accepted` / `SKIPPED-render` plus the offered and
durable watermarks, the last push before the barrier predicted the outcome 6/6
— `accepted=true` passed, `SKIPPED-render` failed. Correlation at n=6 is weak
evidence. The arithmetic is not:

```
last push  end=72039  SKIPPED-render      settled=71895   → 144 bytes short
missing=8  runs=1  [(3992, 3999)]         "OUT:burst-003992\r\n" = 18 B
                                          8 x 18 = 144    ✓
```

The shortfall equals the missing lines exactly. That is the mechanism, not a
symptom that resembles it.

### Two failure shapes share one assertion

The test panics on the _first_ missing line, which cannot distinguish them.
Collecting the whole missing set and folding it into contiguous runs separates
them in one run:

| shape            | probe output                                    | meaning                         |
| ---------------- | ----------------------------------------------- | ------------------------------- |
| truncated suffix | `missing=3711 runs=1 [(289,3999)] settled=5241` | stream simply ended at line 288 |
| mid-stream hole  | `missing=9 runs=1 [(1514,1522)] settled=71877`  | real byte loss                  |

A second probe — hold after the settle loop declares done and watch another 20 s
— established the plateau is permanent rather than merely slower than the 600 ms
stability window.

## The fix

Each extracted Run settles its debt between extract and the barrier, blocking
instead of dropping: `Run::offer_outstanding_output_for_handoff` renders from
`next_replay_start()` and hands it to a new `PersistentRun::append_blocking`.

Blocking is affordable exactly where dropping is not. This runs once per
upgrade, past the point of no return, with every reader already stopped — there
is no pty left to starve, which is the entire reason `append` must never block,
and the actor is draining a queue nothing is adding to, so the wait is bounded
by the depth already enqueued. When nothing is outstanding, which is the common
case, the render is empty, `append_blocking` is never reached, and the barrier
behaves exactly as before.

## The guard, and why its first version was worthless

`a_blocking_offer_makes_the_barrier_fence_every_read_byte` drives refuse → offer
once, blocking → barrier, and asserts durability from the **reopened** database
rather than from queue bookkeeping.

Its first version **passed under mutation**: substituting `append` for
`append_blocking` did not fail it, because the fixture released the actor before
making the offer, so a non-blocking append had a free slot and succeeded too. It
now asserts `!durable.queue_has_room()` at the moment of the offer. Mutation
verified in both directions afterwards.

## Evidence

- paired A/B, one host, same binary shape, `CTXMUX_TEST_TIME_SCALE=8`:
  baseline **2 pass / 10 fail of 12**, with the fix **12 pass / 0 fail of 12**
- `cargo test -p ctxmux-daemon --lib persistence::` — 56 / 56
- pre-existence confirmed on pristine `d89ba01` (pre-merge `main`)
- `cargo clippy -p ctxmux-daemon --all-targets` clean

## What this says about the gate

Reproduction is ~30% of bare runs, and the full suite runs this test once per
gate invocation. **A single green gate run is not evidence for a defect of this
shape** — which is how it survived three weeks after its own guard test landed.
Repeat-count runs belong on any test whose subject is a race.
