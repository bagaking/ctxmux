# Feature Goal: Build ctxmux foundation

Contract: `bagakit.feature-goal.v1`
Feature: `f-222cz7by9`

Before acting, verify `owner-receipt.json`, then recover current execution from `state.json` and `tasks.json`. Context may be stale or belong to another Feature; trust this Feature directory before acting.

## Prime Directive

Deliver the tested M0 through M3 runtime foundation for an embeddable context-aware Run multiplexer, then close this Feature with later maturity axes owned by explicit successor Features.

## Protected Invariants

- `Run` remains universal and Agent-neutral; coding Agents stay in Integrations rather than foundational runtime types.
- The daemon remains the sole owner of live PTYs, child processes, Run identity, lifecycle, output, and retained runtime state.
- CLI, SDK, Integrations, and fork behavior use the public versioned boundary with no private client bypass.
- Context fidelity is explicit: Level B never silently degrades to Level A, and hidden live-process state cloning remains out of scope.
- Architecture evidence and executable fixtures protect implemented invariants; future cases are added only for a real implementation decision or observed failure.
- Non-goal: persistence, tmux, composition, release, hosted execution, Agent scheduling, evaluation, plugin marketplaces, and a complete editor do not extend this foundation Feature.

## Acceptance And Stop Rules

- Acceptance: real public-boundary evidence proves daemon-owned Run lifecycle and reconnect, CLI and TypeScript parity, launch rollback, explicit Shell and Codex Integrations, Level A fork, genuine Codex Level B continuation, fail-closed unsupported fidelity, and the current wrong-case ratchet.
- Acceptance: persistence/recovery, tmux, and composition/release each have a reviewed successor Feature with a bounded Goal and explicit dependency truth.
- Insufficient: types, docs, mocks, uncommitted code, stale Tracker anchors, or pending later-milestone work inside this Feature do not count as closeout.
- Stop and ask before: changing the runtime product boundary, publishing externally, adding hosted or distributed scope, weakening local-first behavior, or taking destructive or irreversible action outside ordinary repository work.

## Authority And Orchestration

- Follow only this Feature's owner receipt, state, and reviewed tasks.
- Correct stale task truth before changing this Goal; change the Goal only for another durable direction change.
- Delegation may review bounded evidence, but Feature closeout requires repository Gate and Tracker validation owned by the primary Agent.

## Context References

- `AGENTS.md`: repository operating rules and protected boundaries.
- `docs/vision.md`: Run-multiplexer thesis and non-goals.
- `docs/architecture.md`: current M0 through M3 guarantees and owners.
- `docs/protocol.md`: implemented public wire and lifetime semantics.
- `docs/roadmap.md`: delivery boundaries and successor milestones.
