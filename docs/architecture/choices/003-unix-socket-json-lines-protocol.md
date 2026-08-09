# 003 — Unix socket and NDJSON protocol generation 1

- Status: accepted for generation 1; pre-stable
- Scope: local transport, framing, handshake, and public error envelope

## Context

Rust and TypeScript clients need a simple local boundary that survives client replacement and can be inspected without linking language runtimes. The first slice targets Unix and optimizes for a small implementation and explicit version failure.

## Decision

Every connection uses one Unix domain socket and newline-delimited UTF-8 JSON frames. The operator supplies the socket path. The daemon creates it with mode `0600`, refuses non-socket targets, checks whether an existing socket accepts connections, and removes only an inactive socket.

The first frame is an exact protocol-generation handshake. Short-lived connections carry one request. Attachment connections carry one snapshot followed by bidirectional control frames and ordered events. One encoded frame is limited to 1 MiB.

## Quality attributes and invariants

- Version mismatch fails before a request is executed.
- A decoded invalid request receives a typed protocol error.
- Socket cleanup never intentionally replaces an ordinary file or symlink.
- Closing a transport connection changes attachment state only.
- Rust and TypeScript enforce the same frame-byte limit.

## Alternatives

- A private in-process API would not support independent client processes.
- Protobuf would improve compactness and schema tooling but add machinery before protocol behavior is stable.
- MessagePack or CBOR would improve binary payload density but reduce inspectability.
- The private tmux socket protocol is neither stable nor owned by ctxmux.

## Known constraints

The socket has no default discovery or activation policy, peer-credential check, request ID, timeout, cancellation, or Windows equivalent. JSON represents bytes as integer arrays. The stale-socket check and removal are separate filesystem operations. Malformed, invalid-UTF-8, or oversized frames can terminate the connection at the codec layer without a structured `InvalidRequest` frame.

Protocol generation 1 replaces obsolete shapes directly. Compatibility policy is not yet a release guarantee.

## Wrong-case corpus

Evidence pack: [local-protocol track](../../../.bagakit/researcher/topics/engineering/ctxmux-wrong-case-corpus/tracks/local-protocol.md), claim `C003`.

- `LP-01` (`c01`): bind-then-chmod has a window when the parent directory is shared, socket mode is not a portable Unix authentication guarantee, and stale cleanup is a check-then-unlink operation.
- `LP-02` (`c02`, `c03`): an unterminated line can force unbounded buffering without an exact byte cap. Exactly-at-limit and over-limit behavior must match in Rust and Node.
- `LP-03` (`c03`): Node UTF-8 replacement and last-wins duplicate JSON keys can disagree with Rust rejection. A shared malformed-wire corpus must fail before Run mutation.

An owner-only directory and mode `0600` materially reduce the local threat surface. These cases limit portability and race claims; they do not require remote-network authentication machinery.

## Fixture mapping

- Covered now: version mismatch, wrong lifecycle requests, socket mode, active-listener refusal, non-socket and symlink refusal.
- Covered now: the exact 1 MiB ceiling, one-byte oversize input with and without a delimiter, bounded closure, and no daemon Run mutation across Rust and Node boundaries.
- Covered now: `fixtures/malformed-protocol-frames.json` drives Rust decode, real-daemon no-mutation, and Node wire checks for malformed JSON, duplicate members at every object level, and invalid UTF-8. A malformed coalesced frame terminates the Node connection before later frames escape.
- Candidate: fragmented valid frames, Unicode byte-length boundaries, and mid-frame disconnect.
- Candidate: race between stale-socket probe and removal.

## Open questions

- Where does the default socket live, and how is one daemon discovered or started?
- Is filesystem mode sufficient, or must peer credentials be checked?
- Which protocol changes require a generation bump?
- How will Windows transport preserve the same public behavior?
- When are request IDs, deadlines, and cancellation required?

## Repository evidence

- `docs/protocol.md`
- `crates/ctxmux-protocol/src/lib.rs`: frames, errors, `MAX_FRAME_BYTES`
- `crates/ctxmux-daemon/src/lib.rs`: `prepare_socket_path`, `handle_connection`
- `packages/sdk/src/wire.ts`
- `fixtures/malformed-protocol-frames.json`
