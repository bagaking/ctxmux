# 003 — Unix socket and NDJSON protocol generation 4

- Status: accepted for generation 4; pre-stable
- Scope: local transport, framing, handshake, and public error envelope

## Context

Rust and TypeScript clients need a simple local boundary that survives client replacement and can be inspected without linking language runtimes. The first slice targets Unix and optimizes for a small implementation and explicit version failure.

## Decision

Every connection uses one Unix domain socket and newline-delimited UTF-8 JSON frames. The operator supplies the socket path. The daemon creates it with mode `0600`, refuses non-socket targets, checks whether an existing socket accepts connections, and removes only an inactive socket. Startup stale cleanup rechecks device/inode identity and performs a second live probe before unlink; an observed replacement returns `SocketTargetChanged` without removing it.

The first frame is an exact protocol-generation handshake. Short-lived connections carry one request. Attachment connections carry one metadata snapshot header, bounded ordered replay-output frames through that header's replay head, then bidirectional control frames and live events. One encoded frame is limited to 1 MiB; total retained replay is not required to fit in one frame.

## Quality attributes and invariants

- Version mismatch fails before a request is executed.
- A decoded invalid request receives a typed protocol error.
- Startup stale cleanup and shutdown never intentionally replace or remove an
  ordinary file, symlink, or independently substituted listener.
- Closing a transport connection changes attachment state only.
- Rust and TypeScript enforce the same frame-byte limit.
- Rust and TypeScript finish ordered replay reassembly before exposing the
  public attachment snapshot.
- The wire uses metadata-only `AttachedHeader`/`OutputReplayHeader` types;
  client-only `AttachedSnapshot` cannot be mistaken for the first frame.

## Alternatives

- A private in-process API would not support independent client processes.
- Protobuf would improve compactness and schema tooling but add machinery before protocol behavior is stable.
- MessagePack or CBOR would improve binary payload density but reduce inspectability.
- The private tmux socket protocol is neither stable nor owned by ctxmux.

## Known constraints

The socket has no default discovery or activation policy, peer-credential check, request ID, timeout, cancellation, or Windows equivalent. JSON represents bytes as integer arrays. Startup revalidation closes the known probe-to-unlink replacement schedule. The shutdown guard retains the bound path's device/inode and removes the pathname only while its current socket identity matches. That check still cannot make pathname recheck plus unlink atomic or rediscover an original socket renamed elsewhere, so a hostile writable parent directory is not made safe by it. Malformed, invalid-UTF-8, or oversized frames can terminate the connection at the codec layer without a structured `InvalidRequest` frame.

Protocol generation 4 directly replaces generation 3 by requiring bounded
creation operation keys on Start and Fork and adding a typed creation conflict.
An older peer fails the exact generation handshake before request dispatch;
ctxmux does not provide a generation-3 fallback, migration, alias, or dual encoding.
Compatibility policy is not yet a release guarantee.

## Wrong-case corpus

Evidence pack: [local-protocol track](../../../.bagakit/researcher/topics/engineering/ctxmux-wrong-case-corpus/tracks/local-protocol.md), claim `C003`.

- `LP-01` (`c01`): bind-then-chmod has a window when the parent directory is shared, socket mode is not a portable Unix authentication guarantee, and stale cleanup is a check-then-unlink operation.
- `LP-02` (`c02`, `c03`): an unterminated line can force unbounded buffering without an exact byte cap. Exactly-at-limit and over-limit behavior must match in Rust and Node.
- `LP-03` (`c03`): Node UTF-8 replacement and last-wins duplicate JSON keys can disagree with Rust rejection. A shared malformed-wire corpus must fail before Run mutation.

An owner-only directory and mode `0600` materially reduce the local threat surface. These cases limit portability and race claims; they do not require remote-network authentication machinery.

## Fixture mapping

- Covered now: generation-3 mismatch before request dispatch, wrong lifecycle
  requests, socket mode, active-listener refusal, non-socket and symlink
  refusal.
- Covered now: the exact 1 MiB ceiling, one-byte oversize input with and without a delimiter, bounded closure, and no daemon Run mutation across Rust and Node boundaries.
- Covered now: retained replay larger than one frame is sent as bounded ordered
  output events and reassembled exactly by both public clients.
- Covered now: `fixtures/malformed-protocol-frames.json` drives Rust decode, real-daemon no-mutation, and Node wire checks for malformed JSON, duplicate members at every object level, and invalid UTF-8. A malformed coalesced frame terminates the Node connection before later frames escape.
- Covered now: bounded seeded Rust and TypeScript targets mutate retained
  malformed plus valid Unicode frames. The TypeScript target partitions bytes
  before the newline parser, and every successful typed decode must validate or
  round-trip. The ordinary corpus remains the minimized regression owner.
- Candidate: a dedicated mid-frame disconnect target and coverage-guided or
  sanitizer-backed continuous fuzzing.
- Covered now: a deterministic stale-socket replacement barrier substitutes an
  unrelated live listener after the inactive probe and proves it is preserved.
- Covered now: after the old daemon serves a public request, a real replacement
  listener takes over its pathname; aborting the old server preserves that path
  and new connections still reach the replacement.

## Open questions

- Where does the default socket live, and how is one daemon discovered or started?
- Is filesystem mode sufficient, or must peer credentials be checked?
- Which protocol changes require a generation bump?
- How will Windows transport preserve the same public behavior?
- When are request IDs, deadlines, and cancellation required?

## Repository evidence

- `docs/protocol.md`
- `crates/ctxmux-protocol/src/lib.rs`: frames, errors, `MAX_FRAME_BYTES`
- `crates/ctxmux-protocol/tests/seeded_fuzz.rs`: bounded native byte target
- `crates/ctxmux-daemon/src/lib.rs`: `prepare_socket_path`, `handle_connection`
- `packages/sdk/src/wire.ts`
- `fixtures/malformed-protocol-frames.json`
- `packages/sdk/test/parser-fuzz.test.ts`
- `scripts/run-seeded-qualification.mjs`
