# Feature Goal: Make native Stop recoverable across response loss

Contract: `bagakit.feature-goal.v1`
Feature: `f-22gcz4t8v`
Convergence: `terminal`
Closure: `state`

Before acting, verify `owner-receipt.json`, then recover current execution from
`state.json` and `tasks.json`. Context may be stale or belong to another
Feature; trust this Feature directory before acting.

## Prime Directive

Deliver and qualify `native.recoverable_stop: 1` in ctxmux so one
daemon-owned native complete-session Stop result can be recovered after
response loss without executing Stop twice. The terminal deliverable is one
exact ctxmux commit proven through Rust, TypeScript, CLI, attachment, and a real
packed consumer.

## Convergence Contract

- Smallest sufficient closure: one recoverable native Stop public vertical in
  ctxmux across Rust, attachment controls, generated TypeScript, SDK, CLI,
  planned exec, bounded Run collection, and an isolated consumer.
- Oracle: one real Run receives one Stop; the first response is lost; a fresh
  client using the retained original operation recovers the exact
  graceful/forced result; concurrent duplicates join and conflicts never enter
  the Stop owner.
- Scope expansion: route Recoverable Resize/Interrupt, Remote Runtime,
  crash-time or host-reboot adoption, external consumer integration, and
  client-specific semantics outside this Feature.
- Completion: every reviewed Task is done, no P0/P1 remains within the finite
  ctxmux claim, all required Rust, TypeScript, CLI, attachment, repository, and
  packed-consumer gates pass on one exact commit, and that commit advertises
  `native.recoverable_stop: 1`.

## Protected Invariants

- `Run` remains the universal object and the existing complete-session Stop
  state machine remains the only signal, process-session cleanup, direct-child
  reap, and quiescence owner.
- The operation binds one caller-retained key, original daemon incarnation,
  and exact Run. A duplicate joins or replays; conflicting reuse fails before
  mutation; cold daemon replacement never replays an old operation.
- Short and attachment paths converge on one daemon ledger. Attachment command
  IDs remain connection-local correlation and never become idempotency keys.
- Retention is bounded to one admitted Stop operation per retained Run and ends
  with exact Run collection. Planned exec may carry settled same-incarnation
  truth; SQLite cold-restart persistence is not implied.
- No compatibility layer, migration, fallback, general mutation framework,
  second Stop owner, Provider field, AgentSession behavior, Agent status,
  permission, message, or Workbench close transaction is added.
- This Feature does not create, plan, modify, schedule, or qualify AgentMux or
  any other external consumer work. External owners decide whether, when, and
  how to integrate the public ctxmux capability.

## Acceptance And Stop Rules

- Acceptance: deterministic owner tests plus real response-loss, client-crash,
  duplicate, conflict, attachment, planned-exec, collection, generated-client,
  CLI, and packed-consumer evidence prove one Stop effect and one recoverable
  terminal result on the exact advertised ctxmux commit.
- Insufficient: successful socket send, connection-local correlation, a new
  type without a real process oracle, receipt-time guessing, duplicate Stop
  execution, unbounded tombstones, mocks alone, or an external-client
  workaround.
- Stop and ask before changing process ownership scope, adding cold-restart
  exactly-once, weakening conflict or incarnation fences, adding another
  runtime owner/dependency, publishing artifacts, modifying an external
  consumer, or expanding beyond the accepted native Stop result.

## Authority And Orchestration

- Follow only this Feature's owner receipt, current Task, and continuation.
- Do not implement chat-only requirements; use Feature Tracker before acting on
  accepted scope changes.
- Prove the Rust short-request vertical first, then extend the same owner to
  attachment, TypeScript, CLI, planned exec, collection, and qualification.
- Satisfy the accepted outcome first; among valid solutions minimize enduring
  states, owners, APIs, abstractions, duplicated truth, and temporary
  scaffolding.
- Parallel component writers may work in the same tree when their file and
  behavior boundaries are explicit. Each preserves unfamiliar changes,
  precisely stages only its own boundary, and reports gates against the actual
  combined tree; no writer may roll back, clean, or overwrite another writer's
  work.
- The supervising integration owner reviews and rejoins every component commit
  on one exact candidate, resolves overlap before final staging, and reruns all
  mandatory gates on that candidate. A component commit or predecessor green
  gate is not Feature completion evidence.
- Treat the qualified ctxmux commit and capability declaration as the end of
  this Feature. Do not turn external adoption readiness into a downstream plan
  or acceptance dependency.

## Context References

- `AGENTS.md`: protected Runtime and delivery invariants.
- `docs/vision.md`: standalone Runtime product and Agent-neutral boundary.
- `docs/architecture.md`: daemon ownership and client separation.
- `docs/protocol.md`: current complete-session Stop and response-loss truth.
- `docs/roadmap.md`: independently closable delivery order.
- `docs/architecture/choices/014-recoverable-input-operations.md`: proven
  caller-keyed same-incarnation recovery pattern and semantic boundary.
- `docs/architecture/choices/015-exec-in-place-upgrade-continuity.md`: accepted
  planned-exec identity, handoff, and owner-transfer contract.
- `.bagakit/feature-tracker/features/f-22gcz4t8v/artifacts/plan-review.md`:
  user-approved Recoverable Stop decisions and external-consumer boundary.
