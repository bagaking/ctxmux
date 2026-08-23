# Recoverable native Stop plan review

Status: approved through explicit user delegation on 2026-08-24.

## What

Add `native.recoverable_stop: 1` without replacing the existing complete-session
Stop owner. A caller retains one daemon-instance-bound Stop operation key; an
exact duplicate joins an in-flight operation or replays its exact terminal
result after reconnect instead of executing Stop again.

## Why

The current Stop implementation already owns admission, process-session
cleanup, reap proof, graceful/forced disposition, and mutation fencing. Its
remaining public gap is response-loss ambiguity: a client can lose the receipt
and cannot distinguish a completed Stop from a failed delivery. AgentMux and
other clients should consume Runtime truth instead of maintaining a second
guessing layer for this ambiguity.

## Intended generalization

The operation is valid for every daemon-owned native Run, independent of shell,
test, server, or Agent semantics. Both short-lived and attachment Stop requests
must converge through the same daemon-owned operation record.

## Confirmed contract

- Add the exact Runtime capability key `native.recoverable_stop` at version 1.
- Replace the pre-stable Stop wire/API directly; do not preserve an old Stop
  fallback, alias, migration, or compatibility path.
- One caller-owned UTF-8 operation key is bounded to 128 bytes and binds the
  original daemon incarnation plus exact Run.
- The first valid key admitted for a Run owns its one Stop attempt. The same key
  joins or replays the exact terminal result; another key for that Run, or the
  same key for another Run, returns a typed conflict before mutation.
- The daemon keeps one shared short-request/attachment ledger. The ledger has at
  most one admitted entry per retained Run, is removed with Run collection, and
  permits key reuse only after that collection boundary.
- Planned exec drains in-flight Stop ownership to a settled boundary and carries
  the settled ledger with the preserved daemon instance. Cold replacement
  changes the daemon instance and does not promise Stop-result recovery.
- The existing complete-session Stop state machine and process cleanup remain
  the only mutation owner; no second Stop implementation or general mutation
  framework is introduced.

## Failure boundary and non-goals

- A lost first response may still be reported as `unknown`; retrying the exact
  retained operation is how the caller resolves it.
- Attachment command IDs remain connection-local correlation and never become
  idempotency keys.
- No recoverable Resize, Interrupt, arbitrary Signal, semantic Agent stop,
  Workbench close transaction, Remote Runtime, crash-time child adoption, host
  reboot continuity, or cold-restart exactly-once promise is added.
- The SQLite state schema is not widened merely to persist Stop receipts across
  cold replacement. Planned-exec handoff state owns the accepted continuity.

## Behavior examples

- Two concurrent clients send the same operation: one Stop executes and both
  obtain the same disposition.
- A client sends Stop, loses the response, exits, and a fresh client retries the
  retained operation: the original receipt is returned without another signal.
- A different key targets a Run whose Stop is already admitted: conflict is
  `not_applied` and does not enter the cleanup owner.
- A settled receipt survives a validated planned exec and remains recoverable.
- Run collection removes its Stop record; the old key may then be reused.

## Transfer checks

- A connection-local attachment command ID with the same surface shape must not
  satisfy reconnect recovery.
- A naturally exited Run with no admitted Stop must not fabricate a Stop
  receipt.
- A cold replacement with the same persistent `runtimeId` but another
  `daemonInstanceId` must reject the old operation before mutation.
- A process that escaped the owned POSIX session with `setsid()` remains outside
  the Stop ownership claim; recoverability must not widen process scope.

## Downstream split

AgentMux entropy reduction is a separate downstream Feature in the AgentMux
repository. It may freeze and consume the exact ctxmux commit only after this
Feature qualifies. It owns adapter thinning, AgentSession association, and the
Desktop Workbench close transaction; ctxmux does not absorb those semantics.
The downstream proposal must retain AgentMux activation and wait/revision code
until their separate ctxmux capabilities actually ship.

## Evidence refs

- `AGENTS.md`
- `docs/vision.md`
- `docs/architecture.md`
- `docs/protocol.md`
- `docs/roadmap.md`
- `docs/architecture/choices/014-recoverable-input-operations.md`
- `docs/architecture/choices/015-exec-in-place-upgrade-continuity.md`
- `crates/ctxmux-protocol/src/lib.rs`
- `crates/ctxmux-daemon/src/lib.rs`
- `crates/ctxmux-daemon/tests/native_lifecycle.rs`
