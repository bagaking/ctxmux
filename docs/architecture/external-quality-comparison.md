# External quality-standard comparison

- Status: reference material, not a commitment
- Scope: how comparable projects and published practice measure performance,
  stability, and test reliability, and where ctxmux stands against them
- Method: read peer source directly (`herdr`), plus published benchmark practice
  for terminal multiplexers, PTY runtimes, and long-lived local daemons

This page exists so ctxmux budgets are calibrated against outside practice
rather than only against themselves. It records what transfers, what does not,
and what ctxmux already does better. It does not create task truth; the
owning plan for benchmark work stays
[testing strategy](../testing-strategy.md#benchmarks-and-performance-regression).

## Where ctxmux already leads

These are not aspirations. Each is enforced today.

| Dimension               | ctxmux                                                                                                                                                                                                                                              | Outside practice                                                                                         |
| ----------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| Per-Run resource slope  | Machine-derived ceilings per cell for CPU, peak/steady RSS, RSS/threads/fds per Run, and retained bytes, at 1/32/128 Runs in idle and active modes (`reliability-budgets.json` budgets section, asserted in `scripts/reliability-qualification.ts`) | Comparable projects publish at most whole-server RSS. A per-unit slope budget is unusual.                |
| Pre-registered evidence | `frozen_before_optimization: true` freezes the baseline before any tuning, and the receipt binds a clean commit and worktree (`scripts/check-reliability.sh`)                                                                                       | Budgets are normally set after tuning, which cannot detect a regression introduced by the tuning itself. |
| Threshold derivation    | `margin_policy` forbids manual margins; every ceiling is derived by exact rational rules in `scripts/reliability-budget-contract.mjs` from three observed rounds                                                                                    | Regression margins are typically hand-picked.                                                            |
| Coverage floors         | Tiered floors (changed-line 90, runtime 85, pure-validator 95) with categorized, evidence-linked exclusions (`coverage-policy.json`)                                                                                                                | Single global percentage is the norm; exclusions rarely carry an evidence pointer.                       |
| Flake policy            | No retry wrapper anywhere; node tests run at `--test-concurrency=1` (`scripts/check.sh`)                                                                                                                                                            | Retry-on-failure is common and hides the defects a suite exists to catch.                                |

## Where ctxmux is behind

One theme: ctxmux measures **resources and correctness**, never **speed**. A
change that doubled echo round-trip time would pass every current gate.

| Dimension                                   | Status                                                                                                                                                                        |
| ------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Input-to-output PTY loopback latency        | Not measured. Listed as required at [testing strategy](../testing-strategy.md#benchmarks-and-performance-regression) and marked `Absent` in that page's own comparison table. |
| Sustained and fan-out throughput            | The fan-out and soak scenarios prove exact-byte delivery, backpressure `Gap`, and retained-byte bounds, but emit no bytes/sec and no tail latency.                            |
| Daemon cold start and Run start-to-accepted | A readiness boundary exists in the fixtures; no time is recorded or bounded.                                                                                                  |
| Percentiles                                 | No latency series exists, so no p50/p95/p99, although the testing strategy demands them.                                                                                      |

The repository has no `benches/` directory and no `criterion` dependency. This
is an unimplemented plan, not an unnoticed gap.

## Practice worth copying

### Replace a wall-clock gate with a deterministic counter

The strongest technique found. `herdr` needed to stop a per-pane cost from
regressing. Instead of asserting `elapsed < budget`, it counts the expensive
call and asserts the count:

```rust
// herdr/src/server/render_stream.rs:600-624
crate::pane::reset_aggregate_input_state_reads();
// ... render 15 panes ...
assert_eq!(crate::pane::aggregate_input_state_reads(), 0);
```

The assertion is exact, machine-independent, and cannot flake under host load,
while still failing the moment the architectural property breaks. `herdr` backs
it with a static scan that rejects banned pane-scaled calls in the render hot
path by file and line.

ctxmux already applies the same philosophy in a different unit: the GC contract
counts exact replacements and settles owners rather than timing windows
([013](choices/013-retained-run-resource-governance.md)). The
lesson is to prefer a counted invariant wherever one can express the property,
and to reach for a timing budget only when nothing countable exists.

### Bound process-spawn concurrency instead of lengthening deadlines

A suite that starts many real daemons in parallel starves its own readiness
waits. Bounding how many startups race is a stronger fix than raising the
budget, because it changes contention rather than tolerance — so a genuinely
slow startup still fails.

This was observed here, not assumed. On a 14-core host under load average 138,
allowing four concurrent daemon startups left five of 46 tests failing on
readiness; allowing two returned 46 of 46. Raising the time multiplier to four
only reduced failures from five to three, which is why the gate rather than the
multiplier is the primary lever.

That comparison was opportunistic rather than controlled, and the limit of that
evidence is worth stating: at load average 40 or below every limit passes, so an
ordinary machine reproduces neither the failure nor the fix. This is itself the
argument for the deterministic-counter approach above — a countable invariant
would have been checkable on any machine, while a contention-dependent failure
is only observable on a machine that happens to be busy. See
`crates/ctxmux-test-support/src/lib.rs`.

### Poll for a condition, never sleep for one

`herdr` polls sockets and frames to a deadline (25ms and 75ms slices), classifies
errors as retryable or not so a real protocol bug fails immediately instead of
timing out, and keeps a small ring of recent frames so a timeout failure prints
the state that preceded it. ctxmux's fixtures already poll rather than sleep;
the diagnostic ring is the part not yet adopted.

## Practice to refuse

Recorded so this comparison does not become a source of cargo-culting.

- **Keystroke-to-pixel latency.** ctxmux is headless and owns PTY bytes, not
  pixels. The transferable metric is byte-to-byte PTY loopback.
- **Hand-picked latency SLAs.** `margin_policy` forbids manual margins. Any
  latency ceiling must be machine-derived through
  `scripts/reliability-budget-contract.mjs` from a fresh baseline.
- **A standalone `criterion` or `hyperfine` binary beside
  `scripts/check-reliability.sh`.** Its numbers would sit outside the receipt
  and provenance pipeline, and `AGENTS.md` requires extending existing behavior
  before adding an abstraction.
- **Test retries.** Adding them would hide exactly the defects the suite exists
  to catch.
- **Quoting a competitor's published figure as a ctxmux target.** Numbers are
  comparable only when the same harness and environment can run both products.
- **Gating on one wall-clock number, or treating a job timeout as a benchmark.**
  A gate needs warm-up, a sample count, and percentiles.
- **Timing the current soak loop as it stands.** It is closed-loop (await an
  operation, then delay), so timing it would be blind to coordinated omission
  and would understate latency under load. Latency load must be issued
  open-loop, at a fixed send cadence, measuring completion lag.
- **Adding aggressive Run GC to make a leak oracle pass.** The measurement
  contract deliberately keeps retained exited-Run state visible because GC is
  unimplemented; normalizing it away would hide the real number.

## Sequencing note

Every latency gap above is a measurement gap, and the testing strategy already
fixes the order: trend enough runs to learn normal variance first, then derive a
per-platform ceiling. Adding a ceiling before that trend exists would repeat, at
a new layer, the same defect that made the integration suite fail under host
load — a budget that encodes one machine's idle luck.
