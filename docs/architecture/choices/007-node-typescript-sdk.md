# 007 — Node TypeScript SDK

- Status: accepted
- Scope: first embeddable non-Rust client

## Context

Editors and local developer tools commonly run TypeScript, while the daemon must remain independent of Electron, React, and any one host. The first SDK needs to prove that a non-Rust process can control the same durable Run without becoming its owner.

## Decision

`@ctxmux/sdk` uses `node:net` and has no runtime dependencies. `CtxmuxClient` is a stateless connector for short requests. `Attachment` retains one live connection for replay, events, input, resize, stop, and detach. Wire declarations are generated from Rust.

The SDK exposes byte input as string or `Uint8Array`; strings are UTF-8 encoded explicitly.

## Quality attributes and invariants

- The package has no Electron, React, editor, or Rust-addon dependency.
- Closing a client or attachment socket never sends stop implicitly.
- CLI-created and SDK-created Runs are mutually observable.
- Frame-size checks use UTF-8 byte length, not JavaScript string length.

## Alternatives

- N-API would couple SDK distribution to native binaries and still require a daemon for lifetime independence.
- Spawning the CLI for each operation would lose a typed streaming attachment API.
- A WebSocket layer would add an extra server surface for a local Unix-only milestone.

## Known constraints

Node 24 or newer and Unix sockets are required. There is no timeout, `AbortSignal`, reconnection helper, request ID, or packaging release. Concurrent `receive()` semantics are not documented.

The SDK validates full nested generation-14 frames, decodes output payloads to
`Uint8Array`, rejects unsafe cursors, and bounds the JSON-lines transport before
pausing the socket. Each Attachment has
one receive pump, a 64-command pending map with separate 32-command/1 MiB input
bounds, and a 256-event/1 MiB delivery inbox. `input()`, `resize()`, and
`stop()` resolve only from their correlated daemon owner receipt. Clean detach
fences new commands, drains pending results, and waits for `Detached`; abrupt
close makes unresolved commands unknown without stopping the Run.

## Wrong-case corpus

- `SDK-01` (`g01`, `g02`, `g04`): a write callback is transport progress, not daemon acceptance; close races can make `drain` impossible and the per-write callback the only error path.
- `SDK-02` (`g01`-`g03`): the permanent `data` listener puts the socket in flowing mode and transfers frames into an unbounded `#lines` array when the consumer pauses.
- `SDK-03` (`g01`): a peer that accepts but never replies leaves handshake, request, or detach pending without a caller-controlled deadline or abort path.

Awaiting each write callback already bounds normal outbound accumulation. The required work is correct terminal-error settlement and bounded inbound buffering, not another speculative outbound queue.

## Fixture mapping

- Covered now: CLI/SDK shared Run, abrupt SDK disconnect, reconnect, correlated
  input/resize/stop, applied size, typed disposition, and invalid-state error.
- Active: clean detach acknowledgement, abrupt-close distinction, FIN/destroy/write-error settlement, bounded slow-consumer delivery, unsafe cursors, and invalid nested frames.
- Active: malformed UTF-8, duplicate JSON members, and invalid JSON close the SDK boundary.
- Covered now: concurrent attachment controls use one reader router, bounded
  pending state, exact result-kind validation, and fail-closed unknown
  disposition on connection loss.
- Future: caller-controlled cancellation, deadlines, and reconnection helpers.

## Open questions

- Which calls accept `AbortSignal` and which errors are retryable?
- How much runtime frame validation is required at a local trust boundary?
- What Node and operating-system matrix is supported at release?

## Repository evidence

- `packages/sdk/src/client.ts`
- `packages/sdk/src/wire.ts`
- `packages/sdk/test/client-parity.test.ts`
- `packages/sdk/package.json`
