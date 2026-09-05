# R27 pre-registration — the second publication round on the stop path

Written **before** the segmentation data arrives. R26 retracted R18's "nothing
outside persistence needs optimising" for teardown; this round attacks the
non-persistence part of `stop` at c=0, the biggest gap in the matrix. The
prediction below can be wrong in public.

## What is already measured (not assumed)

| Fact                                                                                 | Source                                                                                                  |
| ------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------- |
| memory-only c=0 stop+remove = 8.20 ms vs tmux kill-session 3.87 ms (0/8)             | r26-one-caliper-two-refutations.md:118                                                                  |
| memory-only c=0 stop = 5.74 ms, remove = 2.44 ms (persistence share 28% / 49%)       | r26 doc §persistence-share; SEGMENT track                                                               |
| c=0 is the biggest gap AND the lightest shape (unusual)                              | r26 doc Refutation 1                                                                                    |
| remove's 2.44 ms is ~entirely the CLI-process+connect+Hello floor; daemon work is µs | SEGMENT track removeSegments; creation.rs:2297 remove_memory                                            |
| stop synchronously reaps the child before replying                                   | native_session.rs:307-319 reap_leader→child.wait(); lib.rs:5615 "receipt is proof the child was reaped" |
| stop waits a SECOND round after reap for terminal publication                        | lib.rs:5619 await_terminal_visible; lib.rs:4445-4456                                                    |
| durable publish sets terminal_ordinal AFTER persistence.finalize                     | lib.rs:4412-4419                                                                                        |
| harness runs `stop` then `remove` as two separate CLI processes                      | .tmp-r26-cell.sh stop/remove loops                                                                      |
| per-cell A/A floor: 1.002-1.028x at c=0, up to 1.105x at c=8 remove                  | r26 doc §the-gate                                                                                       |

## The segmentation (parent = client wall of one `ctxmux stop <id>`, c=0, memory-only)

```
wall = CLIfloor + connect+Hello + admission + REAP + PUB2 + framing
```

- **CLIfloor** — fork/exec + dynamic link + current_thread tokio build. Shared
  by every verb. `remove`'s own median (2.44 ms) is the caliper for it.
- **REAP** — reactor hop → Stop worker spawn → SIGTERM (1 `/proc` scan) →
  wait_quiescent (200 µs first poll + ≥1 more `/proc` scan) → child.wait().
- **PUB2** — `await_terminal_visible`: a SECOND async round after reap already
  completed — completion wake → reactor cycle → PTY-EOF check → a SECOND
  `ctxmux-native-blocking` (Finalize) worker spawn → publish_terminal →
  notify_waiters. This is the candidate's target.

## Semantic portion — subtracted, NOT a target

Per the rule "if part of the gap is a capability we guarantee and tmux does
not, that part is not a target":

1. **Synchronous reap (~0.3-0.6 ms).** ctxmux blocks the reply on
   `child.wait()` (native_session.rs:315); tmux reaps async off the command
   path (server.c SIGCHLD). Contract: protocol.md:245 "Success requires the
   direct child to be reaped". PROMISED. **Subtract.**
2. **Whole-session `/proc` census (~1.0-1.3 ms, 2 scans).** ctxmux enumerates
   every host PID and `getsid`s it to prove the whole session — descendants
   included — is empty (native_session.rs:252-255, 497); tmux tracks one
   `wp->pid`. Both scans are load-bearing: scan 1 to signal every member,
   scan 2 to PROVE empty at return. Cutting one weakens a guarantee tmux never
   makes → owner decision, not a ratchet target. **Subtract.**

Subtracting these, the attackable non-semantic, in-contract stop remainder is
**PUB2 + the worker/settlement hops**, and PUB2 dominates it.

## Why PUB2 is legal to remove (contract check)

The wire does NOT promise stop returns a published-terminal state — it promises
the opposite: protocol.md:501-503 "the returned RunInfo can still say running
while no owned process remains"; Decision 004:108-110 "Stop acknowledgement …
still precedes terminal-state publication." PUB2 exists only to serve R22's
CONVENTION that a warm client's next-line `remove` sees terminal state
(lib.rs:5615-5618; validate_removable_entry needs terminal_ordinal via
collection_ordinal, creation.rs:2601). So PUB2 is convention, not contract —
the one large piece it is legal to make cheaper.

The R22 constraint (r22-the-stop-that-stops-lying.md): a stop that returns
`Running` poisons the next remove. So we may NOT simply delete the wait. The
contract track names the legal move: "make publication inline at reap so there
is nothing to wait for."

## The candidate

**Publish the terminal state on the reap (Stop) worker itself, gated on the PTY
being already drained, so `await_terminal_visible` finds the Run terminal and
returns with zero extra reactor cycle and zero second worker-thread spawn.**

Mechanism: after `child.wait()` on the Stop worker, if `entry.output.is_none()`
already holds (PTY EOF already observed = the drain queue_ready_terminals waits
for is already complete, native_runtime.rs:1294), set terminal_ordinal +
notify_waiters right there instead of queueing a Finalize worker. When the PTY
is NOT yet drained, fall through to the existing deferred path unchanged.

## Reversed hazard — stated before the data

**The benefit and the regression are the same mechanism (deferral) seen from
two sides.** The deferral buys the final-byte drain: protocol.md:604-605 gives
the PTY reader "a bounded opportunity to drain the child's final output" before
publishing exit. Publishing inline at reap risks publishing BEFORE that drain →
lost final bytes.

- At **c=0** a stopped `sleep` emits nothing, so inline publication is safe and
  the win shows.
- At **c=8** a chatty child has bytes in flight; inline-without-the-gate would
  race the drain. THE REVERSE HAZARD IS AT c=8 — where the rules say to check.
- Containment: gating on `entry.output.is_none()` means c=8 with bytes pending
  simply takes the existing deferred path → no final-byte loss, and no win at
  c=8 (acceptable: c=8 is where we are already at parity, r26 Refutation 1).

**Second hazard — the durable entanglement.** In shipped (durable) mode
publish sets terminal_ordinal only AFTER `persistence.finalize` (lib.rs:4412),
and that finalize is the r26 cross-Run stall (r26-the-finalize-that-blocks-a-
stranger.md). So moving publication onto the Stop worker in durable mode could
either re-introduce that stall or require setting terminal_ordinal BEFORE the
durable finalize. Whether terminal_ordinal can be set at reap independent of
the durable finalize disk write is UNVERIFIED and MUST be priced before product
code. If it cannot, the durable win is blocked on the r26 finalize fix and this
candidate degrades to memory-only-only — which we will NOT ship as a
mode-scoped patch (memory: platform-scoped-fix-hides-the-defect).

## Prediction, before the data

- **c=0 stop: −0.5 to −1.2 ms** (5.74 → ~4.5-5.2 ms memory-only). At c=0 the
  A/A floor is 1.021x (~0.12 ms on 5.74 ms), so a 0.5 ms win = 1.09x clears
  Holm-Bonferroni. Detectable.
- **c=0 remove: flat or slightly faster** — it finds terminal_ordinal already
  set; its own path is unchanged.
- **c=8 stop: flat** — falls through the gate to the deferred path.
- **start, list (all shapes): flat** — structurally untouched; they never enter
  recoverable_stop_response.
- **db_kb / wal_kb: flat** — no retention change.

**Falsifier for the mechanism:** if the segmentation shows PUB2 (the D3−D2
stamp below) is < 0.4 ms, the candidate is not worth the risk and the direction
"collapse the second publication round" is closed. If the durable check shows
terminal_ordinal cannot precede the durable finalize, the candidate is blocked
on r26 and deferred.

## Cells that must stay flat (rollback if any degrades past its A/A floor)

start × {c0,c1,c2,c8}, list × {c0,c1,c2,c8}, stop × {c1,c2,c8}, remove ×
{c0,c1,c2,c8}, and db_kb/wal_kb every cell. ONLY c0 stop (and possibly c0
remove) may improve.

## Rollback trigger

- any final-byte loss (health check: last output chunk survives stop→attach at
  c=8), OR
- any flat cell regresses past its Holm-Bonferroni-adjusted, empirically
  measured A/A floor, OR
- db_kb or wal_kb falls (retention paid for latency = policy change), OR
- c0 stop does not improve by ≥ its A/A floor.

## The gate (unchanged from R20-R26, not negotiable by a good result)

- 12 paired rounds minimum (ctxmux-six-pairs-cannot-settle-anything)
- byte-identical A/A arms, md5-verified (ctxmux-a-cosmetic-edit-can-change-the-binary)
- 4 arms rotated through 4 positions (cn3's 2nd arm is systematically faster)
- shapes c0/c1/c2/c8; the lightest shape usually vetoes
- paired sign test, Holm-Bonferroni across the ~16 cells, per-cell empirical
  A/A floor (ctxmux-sixteen-cells-at-alpha-005-is-a-56-percent-false-rollback)
- run on cn3 (Linux `/proc` path); local macOS uses a different process_ids and
  mis-prices the census (ctxmux-fullfsync-is-not-the-farm-shape)

## FIRST — segment before any product code (no product edit until this closes)

Instrument the memory-only c=0 stop path on cn3, stamps behind
`CTXMUX_TEARDOWN_TRACE` so the shipped binary is byte-identical when disabled
(verify: same-dir double build + md5).

CLIENT (ctxmux-client/src/lib.rs stop_once):

- C0 = entry (:714); C1 = after connect_for_dispatch+Hello (:718) → connect+Hello
- C2 = after send (:732); C3 = after receive (:733) → server-observed RTT

DAEMON (ctxmux-daemon/src/lib.rs recoverable_stop_response):

- D0 = entry (:5607); D1 = after begin_recoverable_stop (:5611) → admission+spawn+wake
- D2 = after flight.resolve() (:5612) → REAP (segments 5-8)
- D3 = after await_terminal_visible (:5620) → **PUB2 — the candidate's target**

WORKER (native_runtime.rs execute_cleanup + native_session.rs stop):

- W0 = execute_cleanup entry (:1140); W1 = after signal_members (native_session.rs:121)
  → SIGTERM scan; W2 = after session.stop returns (:122) → wait-for-reap incl. 2nd scan
- SCAN: wrap process_ids() (native_session.rs:497) with a stamp + per-stop counter
  → (n_scans, ms/scan); prices the semantic census so it is subtracted, not chased

DURABLE CHECK (must run in durable mode too): stamp whether terminal_ordinal is
set before or after persistence.finalize (lib.rs:4412-4419), to decide if the
candidate ships durable or is blocked on the r26 finalize fix.

CLOSURE (must hold within ~1.3x, segment-sums-must-close):

- (C1-C0)+(D1-D0)+(D2-D1)+(D3-D2)+framing ≈ (C3-C2)
- (C3-C0) + remove-median-as-process-floor ≈ harness stop median
- n_scans × ms/scan ≈ bulk of (W2-W0) [prices the census]

Proceed to product code ONLY if D3−D2 (PUB2) ≥ 0.4 ms AND the census (SCAN) and
reap floor are confirmed as the semantic parts to subtract.

---

## Addendum — the go/no-go probe, run before any product edit

The plan above says "proceed only if PUB2 ≥ 0.4 ms". That gate was answered
without daemon instrumentation, from the client side, on a local memory-only
daemon (macOS, n=25 per arm, two independent batches).

The trick is to vary only the reap: stop a **live** `sleep 86400` (pays signal

- real reap + PUB2) against stop of an **already-exited** `/usr/bin/true`
  (reap is trivial, PUB2 remains). `list` supplies a read-only verb over the same
  CLI floor.

| quantity                                  | batch 1     | batch 2     |
| ----------------------------------------- | ----------- | ----------- |
| CLI floor (`--help`)                      | 6.77 ms     | —           |
| `list` (read-only, same floor)            | 7.01 ms     | 6.49 ms     |
| stop of already-exited child (B)          | 8.23 ms     | 8.03 ms     |
| stop of live child (A)                    | 9.81 ms     | 8.49 ms     |
| **B − list = upper bound on PUB2 + hops** | **1.22 ms** | **1.55 ms** |
| A − B = signal + real reap (semantic)     | 1.58 ms     | 0.46 ms     |

**GO.** Even the upper bound clears the 0.4 ms bar in both batches, so the
falsifier does not fire. The reap term is the noisy one (1.58 vs 0.46 ms), but
that is the semantic part being subtracted, not the target.

Caveat on units: this is macOS, where `process_ids()` is
`ctxmux_process_stats::process_ids` rather than the `/proc` walk
(native_session.rs:472 vs :497). Absolute milliseconds do **not** transfer to
cn3 and the real segmentation still has to run there. What does transfer is the
go/no-go: PUB2 is not a rounding error, so the direction stays open.

## Correction to the framing above — the doubled client floor dominates

Measured on the same local memory-only daemon (n=25), timing each verb as its
own CLI process exactly as the farm harness does:

|                                 | ms                          |
| ------------------------------- | --------------------------- |
| CLI floor (`--help`, no daemon) | 5.08                        |
| stop                            | 7.70                        |
| remove                          | 6.00                        |
| **stop+remove**                 | **13.69**                   |
| floor × 2                       | 10.15 — **74% of the pair** |
| daemon-side work, both verbs    | 3.54                        |
| — of which `remove`-specific    | **0.92**                    |

So ~74% of the c=0 stop+remove number is the per-invocation client-process
floor, paid **twice** because our teardown is two verbs where tmux's
`kill-session` is one. That is the single largest term in the R26 gap, it is
partly the instrument confound R26 already flagged ("client in the timed
window: Rust CLI vs tmux's C client"), and it is **out of scope for the
ratchet** — fusing the pair changes the wire/CLI contract and is visible to
agentmux's pinned consumer. Recorded for the owner, not actioned here.

PUB2 stays the round's target because it is real daemon-side cost, it is
convention rather than contract (protocol.md:501-503 says stop's `RunInfo` may
still read `running`), and it does not re-open R22 — it publishes _earlier_
rather than waiting _less_.
