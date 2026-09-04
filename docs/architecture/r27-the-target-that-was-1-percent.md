# R27 — the target I pre-registered does not exist

**Status: direction closed. No product code was written. The pre-registration
committed in `d70cde5` contains three wrong claims, corrected below.**

R27 pre-registered PUB2 — the `await_terminal_visible` wait at `lib.rs:5619`
that runs after the reap has already completed — as the teardown target, with a
falsifier: proceed only if PUB2 ≥ 0.4 ms. This round measured it. PUB2 is
**0.040 ms**, ten times under its own bar. The falsifier fired on the first
honest measurement.

Worse, the evidence that produced the GO was invalid, and so was the headline
number in the same commit. Both are dissected here because the failure mode is
reusable, not because the arithmetic is interesting.

## 1. The go/no-go probe measured a code path the product never takes

The probe (`pub2.py`) varied only the reap and subtracted:

- Arm A: stop a live `sleep 86400` → signal + reap + PUB2
- Arm B: stop an already-exited `/usr/bin/true` → reap trivial, **"still pays PUB2"**
- claim: `B − list` ≈ PUB2 + hops = an upper bound on the target

The clause in bold is false, and it is the whole argument. `await_terminal_visible`
opens with a short-circuit:

```rust
// lib.rs:4445-4450
async fn await_terminal_visible(&self, deadline: Instant) {
    loop {
        let notified = self.terminal_visible.notified();
        if !self.is_running() {
            return;
        }
```

Arm B let the child exit and then slept 250 ms. Direct observation of the Run
immediately before the stop:

```
run 0: ... exited(0) pid=4511 ...
run 1: ... exited(0) pid=4523 ...
run 2: ... exited(0) pid=4571 ...
```

Already terminal, already published — so `is_running()` is false and the wait
returns without ever waiting. Arm B paid **zero** PUB2. The quantity I called
"an upper bound on PUB2" was an upper bound on a stop that skips PUB2 entirely.

The instrumented run makes it worse: with a stamp around the wait itself, Arm B
produced **no samples at all**. An already-exited Run's stop does not reach
`recoverable_stop_response`; it is a different path. The two arms were never
the same experiment with one variable changed — they were two different
functions.

Measured directly, with a throwaway stamp around the wait (since reverted):

| | median | p90 | max |
|---|---|---|---|
| PUB2, live child (the product case) | **0.040 ms** | 0.059 | 0.064 |
| PUB2, already-exited child | *no samples — different path* | | |

The pre-registered bar was 0.4 ms. **NO-GO.**

This is [[ctxmux-segment-the-path-dont-guess-the-line]] again, one level up: I
did segment rather than guess, but I segmented by *subtracting two client-side
arms* instead of stamping the thing itself. A subtraction is only a measurement
if both arms execute the code being subtracted. Mine did not, and nothing in
the arithmetic could reveal that — the numbers were plausible, stable, and
reproduced across two independent batches. **Two batches agreeing is evidence
of a stable harness, not a valid one.**

## 2. "74% of teardown is the CLI process floor" is a macOS artifact

`d70cde5` leads with that claim and calls it the largest single term in the
gap. It came from a local macOS run: floor 5.08 ms, stop+remove 13.69 ms.

The floor itself was mostly the measuring apparatus. A C `posix_spawn` control
against the Python-`subprocess` harness, same host:

| macOS | C posix_spawn | python subprocess |
|---|---|---|
| `/usr/bin/true` | 4.009 ms | 4.737 ms |
| `ctxmux --help` | 4.183 ms | 5.382 ms |
| `tmux -V` | 4.121 ms | — |

Our CLI sits **0.17 ms** above the OS spawn floor; the ~5 ms is macOS process
creation, not ctxmux. And R26's gap was never measured on macOS. On cn3, where
it was:

| cn3 (C posix_spawn, n=200) | |
|---|---|
| `/bin/true` — OS floor | 0.532 ms |
| `ctxmux --help` | 0.899 ms |
| `tmux -V` | 1.057 ms |

Linux spawn is **7.5× cheaper** than macOS. Our CLI startup is *faster* than
tmux's. The floor cannot be 74% of anything on the host that matters, and
"fusing stop+remove into one verb" — which `d70cde5` records for the owner as
the largest lever, at the cost of a wire-contract break — is priced off that
same macOS number and is not worth a contract break at the real magnitude.

**New instance of [[ctxmux-fullfsync-is-not-the-farm-shape]]:** that memory says
fsync-class optimizations cannot be priced locally. The general rule is wider —
*anything whose cost is dominated by an OS primitive* must be priced on the
target host. Process spawn belongs on that list next to fsync.

## 3. What teardown actually costs on cn3

Same binaries R26 measured with (`r26-c/target/release`), memory-only, c=0,
n=40. `list` is the control: same binary, same connect, same request/response,
trivial daemon work.

| | ms |
|---|---|
| CLI floor (`--help`) | 1.012 |
| `list` (control verb) | 1.307 |
| `start` | 2.232 |
| `stop` | 4.353 |
| `remove` | 1.381 |
| **stop + remove** | **5.734** |
| 2 × list (floor + IPC, paid twice) | 2.614 — **46%** |
| daemon-side teardown work | 3.120 — **54%** |
| — of which `stop` | 3.046 |
| — of which `remove` | **0.074** |

`remove` is 0.074 ms of daemon work. It is finished; there is nothing in it to
optimize, which retroactively confirms the one thing `d70cde5` got right about
`remove_memory`. **The entire addressable target is `stop`'s 3.046 ms.**

## 4. Where stop's 3.046 ms goes — and why it is not PUB2

PUB2 is 0.040 ms, 1.3% of it. The cost is the `/proc` census. A faithful
standalone replica of `members()` on cn3 (868 processes):

| | ms |
|---|---|
| `readdir` of `/proc`, numeric entries | 0.775 |
| `getsid()` × 868 | 0.234 |
| **one census** | **1.009** |

And `strace` on a real stop, counting `openat("/proc")`:

```
  /proc opendir calls during ONE stop : 2
  implied census cost                 : 2.018 ms
  measured daemon-side stop           : 3.046 ms
```

**Two censuses, 2.018 ms, 66% of daemon-side stop.** The segments close against
the parent (2.018 of 3.046, remainder 1.03 ms for signal delivery, the 200 µs
first poll, reap and PUB2), so this is a real decomposition and not a
coincidence — [[ctxmux-segment-sums-must-close]].

R27 classified the census as an untouchable semantic difference (we prove the
whole owned session empty; tmux tracks only `wp->pid`). That framing is what
stopped me looking at it — and it conflates two different questions:

- *what* we prove (whole-session emptiness) — genuinely semantic, keep it
- *how many times per stop* we pay 1.009 ms to prove it — not semantic at all

Only the second is in scope, and it was never examined because the first
answer closed the file.

## Root cause

Three failures, in descending order of how much they cost.

**A subtraction whose arms run different code.** The go/no-go compared two
client-side arms and attributed the difference to a function that one arm never
called. The fix is mechanical and cheap: when the target is a specific region
of code, stamp *that region*, even if it means a throwaway build. The stamped
build took twelve seconds to compile and answered in one run what two batches
of careful subtraction had gotten backwards.

**A locally-measured constant promoted to a headline.** The 74% claim carried a
units caveat in the doc ("absolute milliseconds do not transfer to cn3") and
then was used as though it did — it set the scope decision (fusing the verbs is
"the largest lever"), which is exactly the decision the caveat should have
blocked. A caveat that does not change what you do is decoration.

**A semantic label used as a cost boundary.** "The census is a capability
difference" is true and was the reason I never priced it. Being semantically
required says nothing about frequency; the census turned out to be two thirds
of the target while the thing I did pre-register was 1.3%.

## What this round produced

No product code, no rollback needed — the falsifier fired before any edit. The
ratchet's output is a corrected map:

- PUB2 (0.040 ms) — **closed**, measured at the wait
- `remove` (0.074 ms daemon work) — **closed**, nothing there
- verb fusion — **deprioritised**; priced off a macOS artifact, and the real
  per-invocation cost on cn3 is 1.3 ms, not worth a wire-contract break
- **the census, priced by frequency rather than by necessity** — 2.018 ms of
  stop's 3.046 ms, in scope, unexamined

The open question for R28, stated so it can be refuted rather than assumed:
`signal_members` takes a census to enumerate who to signal, and `wait_quiescent`
takes another immediately after to check emptiness. Whether the second is
redundant, or whether emptiness can be decided without a full host walk when the
first census found only the leader, is a *measurement*, not an argument — and it
must be made on cn3, where the census costs what it costs.

Correcting `d70cde5` matters more than the new lead. A wrong number in a
committed pre-registration outlives the round that wrote it.
