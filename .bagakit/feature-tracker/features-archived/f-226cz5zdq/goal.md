# Feature Goal: Close ctxmux Run-Kernel reliability

Contract: `bagakit.feature-goal.v1`
Feature: `f-226cz5zdq`
Convergence: `terminal`
Closure: `state`

Before acting, verify `owner-receipt.json`, then recover current execution from
`state.json` and `tasks.json`. Trust this Feature directory over stale chat or
review context.

## Prime Directive

Close the already shipped local Run Kernel with the smallest representative
proof that memory-only and persistent retained-state owners converge. Preserve
exact Run/key identity, process ownership, replay, and restart truth without
building a qualification platform around them.

## Completion Oracle

The Feature is complete when its one remaining reviewed Task is done:

- one deterministic ordinary test drives at least three reduced-capacity
  turnover windows through both memory-only and persistent creation paths;
- retained Run/key identity, same-key retry without another physical child,
  and persistent restart convergence are exact;
- one focused independent review finds no open P0/P1 in that changed boundary;
- `scripts/check.sh` passes once from the exact clean revision.

Existing completed Task evidence remains valid. A P0/P1 defect in the changed
retention boundary must be fixed and re-reviewed. P2 cleanup and adjacent work
do not expand this oracle.

## Protected Invariants

- `Run` remains the universal core object and the daemon remains its lifecycle,
  PTY, process, replay, and retained-state owner.
- Integrations and Backends remain separate; unsupported fidelity fails closed.
- Creation keys, collection tickets, persistence COMMIT disposition, and child
  authority retain one explicit owner and cannot create duplicate physical Runs.
- The proof crosses the real Registry and persistence creation paths; mocks,
  types, or declared counters alone are insufficient.
- Among valid implementations, minimize lasting states, owners, APIs,
  abstractions, duplicated truth, migrations, and evidence machinery.

## Non-Goals And Stop Rules

Do not add a metrics sink, public admin API, background GC actor, second budget
model, general transaction framework, 512 MiB pressure matrix, long soak,
cross-platform release matrix, broad domain-review program, benchmark contest,
packaging, activation, tmux completion, SSH, UI, or Agent orchestration for this
Feature. Preserve existing historical tests and frozen receipts, but do not
extend them as a closure condition.

Stop and ask before changing this finite oracle, weakening a shipped guarantee,
expanding supported product scope or platforms, publishing externally, or
taking an irreversible action. Reviewer P2 suggestions are recorded as bounded
residuals rather than converted into new Tasks.

## Authority

- Follow only this Feature's current reviewed Task and owner receipt.
- Keep one implementation writer for a runtime owner; use independent agents
  only for focused read-only review after the source is stable.
- Use failing-first, deterministic, low-cost TDD at the real owner boundary.
- Do not revise this Goal for progress details; Feature Tracker owns execution,
  Gate, evidence, and closeout state.

## Context References

- `AGENTS.md`
- `docs/architecture.md`
- `docs/protocol.md`
- `crates/ctxmux-daemon/src/tests/creation.rs`
- `.bagakit/feature-tracker/features/f-226cz5zdq/artifacts/revision-17-plan-review.md`
