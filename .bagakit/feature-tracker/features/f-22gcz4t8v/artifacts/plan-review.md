# Recoverable native Stop plan review

Status: approved through explicit user delegation on 2026-08-24.

Revision 3 scope correction: the user confirmed that this Feature owns only
delivery and qualification of `native.recoverable_stop: 1` in ctxmux. Its
terminal output is one exact ctxmux commit proven through Rust, TypeScript,
CLI, attachment, and a real packed consumer. That public commit and capability
make external integration possible, but no external consumer Feature, handoff,
task plan, code change, or adoption schedule belongs to this Feature. The
correction removes the earlier AgentMux handoff and downstream acceptance
wording without changing valid Recoverable Stop implementation work.

Revision 2 correction: an independent plan review found that Rust name filters
can exit successfully with zero selected tests and that the package-local SDK
E2E command does not build or inject the required ctxmux binaries. The reviewed
plan therefore requires one repository qualification script that asserts each
Recoverable Stop test selection is non-empty before running it, and uses the
root `npm run test:e2e` entrypoint for real SDK parity. This tightens evidence
only; it does not change the accepted behavior or scope below.

## What

Add `native.recoverable_stop: 1` without replacing the existing complete-session
Stop owner. A caller retains one daemon-instance-bound Stop operation key; an
exact duplicate joins an in-flight operation or replays its exact terminal
result after reconnect instead of executing Stop again.

## Why

The current Stop implementation already owns admission, process-session
cleanup, reap proof, graceful/forced disposition, and mutation fencing. Its
remaining public gap is response-loss ambiguity: a client can lose the receipt
and cannot distinguish a completed Stop from a failed delivery. A public
recoverable operation lets any compatible client consume Runtime truth without
moving client-specific semantics into ctxmux.

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
  AgentSession or Provider behavior, Workbench close transaction, Remote
  Runtime, crash-time child adoption, host reboot continuity, or cold-restart
  exactly-once promise is added.
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

## External consumer boundary

This Feature stops after ctxmux publishes the qualified exact commit and
`native.recoverable_stop: 1` capability. External consumers independently
decide whether, when, and how to integrate it. This Feature does not create,
plan, modify, or schedule AgentMux work. AgentSession association, Provider
semantics, and the Desktop Workbench close transaction remain outside ctxmux.

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
