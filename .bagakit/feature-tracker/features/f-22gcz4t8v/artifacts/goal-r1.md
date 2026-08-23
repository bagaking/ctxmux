# Feature Goal: Make native Stop recoverable across response loss

Contract: `bagakit.feature-goal.v1`
Feature: `f-22gcz4t8v`
Convergence: `terminal`
Closure: `state`

Before acting, verify `owner-receipt.json`, then recover current execution from
`state.json` and `tasks.json`. Context may be stale or belong to another
Feature; trust this Feature directory before acting.

## Prime Directive

Deliver one daemon-owned native complete-session Stop operation whose exact
terminal result a caller can recover after response loss without executing Stop
twice. This closes the Runtime ambiguity that higher clients cannot prove while
preserving ctxmux as the only owner of process-session cleanup truth.

## Convergence Contract

- Smallest sufficient closure: one recoverable native Stop public vertical
  across Rust, attachment controls, generated TypeScript, SDK, CLI, planned
  exec, bounded Run collection, and an isolated consumer.
- Oracle: one real Run receives one Stop; the first response is lost; a fresh
  client using the retained original operation recovers the exact
  graceful/forced result; concurrent duplicates join and conflicts never enter
  the Stop owner.
- Scope expansion: route Recoverable Resize/Interrupt, Remote Runtime,
  crash-time or host-reboot adoption, Agent semantics, Desktop close behavior,
  publishing, and downstream code deletion to their own Features.
- Completion: every reviewed Task is done, no P0/P1 remains within the finite
  claim, and the complete repository plus packed-consumer gates pass on the
  exact handoff candidate.

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
  second Stop owner, Provider field, Agent status, permission, message,
  Workbench transaction, or speculative distributed surface is added.

## Acceptance And Stop Rules

- Acceptance: deterministic owner tests plus real response-loss, client-crash,
  duplicate, conflict, attachment, planned-exec, collection and packed-consumer
  evidence prove one Stop effect and one recoverable terminal result.
- Insufficient: successful socket send, connection-local correlation, a new
  type without a real process oracle, receipt-time guessing, duplicate Stop
  execution, unbounded tombstones, or a higher-client workaround.
- Stop and ask before changing the process ownership scope, adding cold-restart
  exactly-once, weakening conflict or incarnation fences, adding another
  runtime owner/dependency, publishing artifacts, or expanding the Feature
  beyond the accepted native Stop result.

## Authority And Orchestration

- Follow only this Feature's owner receipt, current Task, and continuation.
- Do not implement chat-only requirements; use Feature Tracker before acting on
  accepted scope changes.
- Prove the Rust short-request vertical first, then extend the same owner to
  attachment, TypeScript, CLI, planned exec, collection, and qualification.
- Satisfy the accepted outcome first; among valid solutions minimize enduring
  states, owners, APIs, abstractions, duplicated truth, and temporary
  scaffolding.
- Keep one integration writer for protocol, daemon, generated clients, docs,
  and Tracker transitions. Parallel Agents may perform read-only review or
  isolated candidate work; one writer integrates and gates the exact result.
- The downstream AgentMux Feature may begin destructive simplification only
  after this Feature produces an exact qualified commit and capability receipt.

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
  user-approved Recoverable Stop decisions and downstream split.
