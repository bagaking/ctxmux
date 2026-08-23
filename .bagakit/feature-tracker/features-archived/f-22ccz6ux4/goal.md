# Feature Goal: Make native Input retry-safe across response loss

Contract: `bagakit.feature-goal.v1`
Feature: `f-22ccz6ux4`
Convergence: `terminal`
Closure: `state`

Before acting, verify `owner-receipt.json`, then recover current execution from
`state.json` and `tasks.json`. Context may be stale or belong to another
Feature; trust this Feature directory before acting.

## Prime Directive

Deliver one native Input operation that a caller can safely recover after
response loss within the same daemon incarnation, without a second PTY write,
and report the exact byte range applied at the daemon-owned PTY boundary. This
lets supervisors distinguish runtime fact from target semantic acknowledgement.

## Convergence Contract

- Smallest sufficient closure: one short-lived native Input public vertical in
  Rust plus generated TypeScript and SDK parity.
- Oracle: a real child receives one payload once, the first response is lost,
  and a fresh client recovers the original range; conflict, stale cursor,
  ambiguous write, and daemon replacement fail closed.
- Scope expansion: route process-group Stop, durable Resize, Signal, SSH,
  release, performance, coverage, tmux, and Agent semantic delivery to their
  own Features or backlog unless they are strictly required by this oracle.
- Completion: both reviewed Tasks are done, the bounded independent review
  has no open P0 or P1 within this claim, and one clean ordinary repository Gate
  passes. No soak, platform matrix, or competitive benchmark is required.

## Protected Invariants

- `Run` remains the universal core object. The daemon owns PTY bytes and input
  operation truth; Integration or Agent harness owns Message, Delivery,
  acknowledgement, Reply, Task, dispatch, DAG, and UI state.
- `AttachmentCommandId` remains connection-local correlation. Recoverable Input
  is a separate short-lived operation and does not generalize Resize, Stop, or
  Signal.
- Fresh clients retain the operation's original daemon incarnation. A new
  daemon never replays uncertain bytes from an older incarnation.
- The existing native Input owner remains the only physical writer. Completed
  result retention is bounded, and partial or ambiguous write never becomes an
  applied range.
- No public general operation framework, persistent input ledger, compatibility
  layer, extra actor, soak harness, metrics sink, or speculative abstraction is
  added.

## Acceptance And Stop Rules

- Acceptance: owner algebra tests, one combined real-PTY lost-response test,
  one live cross-instance fence test, TypeScript parity, bounded review, current
  documentation, and the ordinary Gate prove the finite state oracle.
- Insufficient: successful socket send, connection-local command correlation,
  an unverified type shape, duplicate-prone retry, or a receipt claiming child
  consumption or semantic acknowledgement.
- Stop and ask before weakening existing Input behavior, changing the
  incarnation or crash boundary, adding a new runtime owner or dependency,
  broadening the Feature outcome, publishing externally, or taking an
  irreversible action.

## Authority And Orchestration

- Follow only this Feature's owner receipt, state, and reviewed tasks.
- Before any new optimization or implementation, compare the request with this
  Goal and current Feature task truth; stop on unexplained drift.
- Do not implement a chat-only requirement. First record each accepted new
  requirement in the appropriate reviewed Feature Task through Feature Tracker.
- Prove the cheapest representative user-visible vertical before broad
  horizontal infrastructure.
- Satisfy acceptance first; among valid solutions minimize enduring states,
  owners, APIs, abstractions, duplicated truth, and temporary scaffolding.
- Keep one writer for protocol, daemon, and client runtime implementation.
  Parallel agents may inspect or review independent surfaces but must not edit
  the same runtime owner. Integrate only source-bound findings required by the
  current Task.

## Context References

- `AGENTS.md`: protected product and delivery invariants.
- `docs/architecture/choices/014-recoverable-input-operations.md`: accepted
  operation and failure boundary.
- `.bagakit/grill/runs/local-operation-kernel-boundary/grill-brief.md`: user
  decision that fixes the Feature scope.
- `.bagakit/researcher/topics/engineering/recoverable-run-operations/summaries/synthesis.md`:
  peer comparison and semantic acknowledgement boundary.
