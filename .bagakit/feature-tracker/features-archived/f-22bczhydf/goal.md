# Feature Goal: Close ctxmux Run-Kernel correctness

Contract: `bagakit.feature-goal.v1`
Feature: `f-22bczhydf`
Convergence: `terminal`
Closure: `state`

Before acting, verify `owner-receipt.json`, then recover current execution from
`state.json` and `tasks.json`. Trust this Feature directory over chat or its
superseded umbrella.

## Prime Directive

Close the finite correctness and retained-resource findings in the shipped
local Run Kernel. Bound memory-only and persistent retained state, stop
unclassified native waiter failures without inventing exit truth, and prove the
result under sustained churn and independent Kernel review.

## Protected Invariants

- `Run` remains the universal core object; the daemon owns runtime identity,
  process or Backend control, lifecycle, replay, and retained state.
- Creation keys, publication reservations, collection fences, and persistence
  COMMIT disposition retain one explicit owner at every mutation boundary.
- Unsupported behavior fails closed. No stale PID signalling, process
  adoption, hidden duplicate launch, silent replay continuity, or Level-B
  downgrade is accepted.
- Implement only owner-local primitives required by current behavior. Do not
  add a background GC actor, Session identity, Backend framework, TTL service,
  general transaction API, compatibility layer, or Agent semantics.
- Correctness evidence exercises the real owner or public boundary. Types,
  mocks, compilation, and prose are insufficient on their own.
- Integrations continue to own semantic continuation and provenance in the SDK
  host. The Kernel owns only capability, caller-materialized fork, lineage, and
  fail-closed runtime enforcement.

## Convergence Contract

- Closure is terminal and state-based: memory-only and persistent retained
  state stay within the declared ceiling; unclassified waiter observation
  failure has a finite fail-closed disposition; sustained churn, restart,
  retry, replay, and resource oracles pass; bounded Kernel review has no open
  P0/P1 finding.
- Tmux product completion remains in `f-224czneed`; composition, activation,
  packaging, platforms, and release qualification remain in `f-225cz7943`;
  peer performance remains in `f-22aczwza9`.
- Benchmark wins, ties, or losses never change this Feature's completion.
- Stop and ask before weakening a shipped guarantee, changing the terminal
  oracle, expanding product scope, publishing externally, or taking an
  irreversible action outside the reviewed Tasks.

## Context References

- `AGENTS.md`
- `docs/vision.md`
- `docs/architecture.md`
- `docs/protocol.md`
- `docs/architecture/choices/004-run-lifecycle-concurrency.md`
- `docs/architecture/choices/009-runtime-persistence-recovery.md`
- `docs/architecture/choices/013-retained-run-resource-governance.md`
- `docs/testing-strategy.md`
