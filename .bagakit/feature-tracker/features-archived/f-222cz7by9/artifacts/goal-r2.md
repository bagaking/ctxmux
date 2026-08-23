# Feature Goal: Build ctxmux foundation

Contract: `bagakit.feature-goal.v1`
Feature: `f-222cz7by9`

Before acting, verify `owner-receipt.json`, then recover current execution from `state.json` and `tasks.json`. Context may be stale or belong to another Feature; trust this Feature directory before acting.

## Prime Directive

Deliver a publishable, embeddable context-aware Run multiplexer so local Runs can outlive their clients, preserve explicitly declared context, and serve CLIs, editors, and automations without each client rebuilding process and Agent integration infrastructure. Mature it with traceable technical decisions and executable failure cases so known runtime mistakes become a continuous engineering ratchet.

## Protected Invariants

- `Run` remains universal and Agent-neutral; coding Agents remain flagship Integrations rather than foundational runtime types.
- The daemon remains the sole owner of live PTYs, child processes, Run identity, lifecycle, output, and durable runtime state.
- Clients, including first-party clients, use the public versioned protocol and SDK boundaries.
- Backend and Integration remain independent extension axes.
- Context and fork fidelity remain capability-declared and fail closed; Level B never silently degrades to Level A, and arbitrary hidden live-process state cloning remains out of scope.
- tmux support uses public tmux integration surfaces and does not reproduce tmux's private client-server wire protocol.
- Architecture records separate current guarantees from target design, and fixtures protect ctxmux invariants rather than dependency-specific incidental behavior.
- Non-goal: do not turn ctxmux into an Agent planner, scheduler, evaluator, team harness, hosted execution platform, plugin marketplace, complete editor, or speculative test framework for capabilities that do not exist.

## Acceptance And Stop Rules

- Acceptance: real end-to-end evidence shows daemon-owned Runs survive client exit; CLI and TypeScript clients use the same public boundary; explicit Integrations add Agent behavior without polluting core Run types; Level A and one genuine Level B fork obey their declared fidelity; the tmux adapter preserves tmux ownership; and a public-API composition example keeps orchestration outside core.
- Acceptance: every critical technical choice has a status-bearing, evidence-linked decision record; retained real-world failure cases trace from source to protected invariant and fixture; currently applicable high-risk fixtures run in the repository gate; and future-capability cases remain explicitly inactive until their owning behavior exists.
- Insufficient: documentation, types, mocks, disconnected package skeletons, a client-owned process manager, unsourced architecture folklore, or case records with no fixture disposition do not count as a mature ctxmux runtime.
- Stop and ask before: changing the final product outcome or protected invariants; adding hosted or distributed scope; publishing packages or releases; using paid external services; weakening privacy or local-first behavior; or taking destructive or otherwise irreversible actions outside ordinary repository implementation.

## Authority And Orchestration

- Follow only this Feature's owner receipt, state, and reviewed tasks.
- Execute tasks in the reviewed dependency order and record task-level gate evidence before marking work done.
- Delegation may parallelize only independent, bounded work; the Feature owner remains responsible for integrating results and verifying public behavior.
- Correct stale Feature task truth before changing this Goal. Revise the Goal only when the durable outcome, invariants, acceptance boundary, or authority changes.
- Continue independent valuable work when one task waits, but do not infer permission to expand scope or publish externally.

## Context References

- `AGENTS.md`: repository operating rules and protected invariants; read before every implementation task.
- `docs/vision.md`: product thesis, users, success definition, brand, and non-goals; read when product meaning or public claims are involved.
- `docs/architecture.md`: current and target ownership, extension, fork, tmux, failure, and decision boundaries; read before architecture or protocol changes.
- `docs/protocol.md`: implemented wire and lifetime semantics; read before protocol, SDK, or fixture changes.
- `docs/roadmap.md`: reviewed milestone order, architecture evidence phase, and acceptance criteria; read when planning, starting, or completing a Feature task.
