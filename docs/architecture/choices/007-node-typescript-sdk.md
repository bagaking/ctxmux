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

The SDK now validates full nested generation-1 frames, rejects unsafe cursors, bounds queued input to 256 frames or 1 MiB before pausing the socket, and waits for `Detached` on clean detach. Short request methods wait for `Response` or `Error`. `Attachment.input()`, `resize()`, and `stop()` currently resolve after their frame's socket-write callback; remote `Accepted` or `Error` remains a separate attachment event because generation 1 has no command correlation ID.

## Wrong-case corpus

Evidence pack: [typescript-sdk track](../../../.bagakit/researcher/topics/engineering/ctxmux-wrong-case-corpus/tracks/typescript-sdk.md), claim `C007`.

- `SDK-01` (`g01`, `g02`, `g04`): a write callback is transport progress, not daemon acceptance; close races can make `drain` impossible and the per-write callback the only error path.
- `SDK-02` (`g01`-`g03`): the permanent `data` listener puts the socket in flowing mode and transfers frames into an unbounded `#lines` array when the consumer pauses.
- `SDK-03` (`g01`): a peer that accepts but never replies leaves handshake, request, or detach pending without a caller-controlled deadline or abort path.

Awaiting each write callback already bounds normal outbound accumulation. The required work is correct terminal-error settlement and bounded inbound buffering, not another speculative outbound queue.

## Fixture mapping

- Covered now: CLI/SDK shared Run, abrupt SDK disconnect, reconnect, input, resize, stop, and typed invalid-state error.
- Active: clean detach acknowledgement, abrupt-close distinction, FIN/destroy/write-error settlement, bounded slow-consumer delivery, unsafe cursors, and invalid nested frames.
- Active: malformed UTF-8, duplicate JSON members, and invalid JSON close the SDK boundary.
- Future: caller-controlled cancellation, deadlines, reconnection, and concurrent attachment operations after their public contracts exist.

## Open questions

- Should attachment mutations gain correlated acknowledgements, or should their promises remain transport-completion APIs with results observed through events?
- Which calls accept `AbortSignal` and which errors are retryable?
- How much runtime frame validation is required at a local trust boundary?
- What Node and operating-system matrix is supported at release?

## Repository evidence

- `packages/sdk/src/client.ts`
- `packages/sdk/src/wire.ts`
- `packages/sdk/test/client-parity.test.ts`
- `packages/sdk/package.json`
