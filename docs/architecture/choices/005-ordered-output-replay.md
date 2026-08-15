# 005 — Ordered bounded raw-output replay

- Status: accepted
- Scope: output identity, retention, reconnect, live gaps, and exit ordering

## Context

A reattaching client needs more than future bytes. It needs a cursor, retained history, and an honest signal when history was evicted. Full terminal-screen reconstruction is a separate capability and is not required to prove durable Run ownership.

## Decision

Each non-empty PTY read becomes one `OutputChunk` carrying its contiguous half-open cumulative byte range `[start_byte, end_byte)`. The daemon retains chunks by raw byte count up to a 4 MiB target while always keeping at least one chunk. Attach names the last observed byte cursor and receives first-available/latest-byte cursors plus `truncated` in an initial metadata header. Retained bytes follow as ordered output frames; an interior cursor slices the first returned range, and public clients reassemble the frames before returning the snapshot.

The metadata header and reassembled snapshot are different protocol types.
`AttachedHeader` has no chunks field; `AttachedSnapshot` is a client API result.

Live output is delivered through a bounded broadcast channel. A lagging receiver gets `Gap { latest_output_bytes }` and must reattach using the client's last successfully observed byte cursor.

## Quality attributes and invariants

- Byte ranges are contiguous per Run and start at zero.
- Replay contains only bytes after `after_byte`; it slices a retained chunk when necessary.
- Eviction is explicit through `truncated`; missing history never looks complete.
- Subscribe-before-snapshot plus deduplication prevents replay/live duplication.
- Exited Runs remain attachable for replay and one terminal state event.
- Retained history larger than one transport frame remains exact because replay
  uses the same bounded output-frame representation as live delivery.

## Alternatives

- An unbounded byte log makes one noisy Run a daemon-wide memory hazard.
- A screen model can answer “what is visible now” but loses raw event history and requires terminal emulation.
- File-only output avoids memory pressure but introduces persistence, I/O, corruption, and cleanup policy before restart durability exists.

## Known constraints

Retention is a byte target, not a hard cap when a single chunk exceeds 4 MiB. Output cursor overflow fails inside the daemon-owned log rather than wrapping. The TypeScript schema maps `u64` to JavaScript `number`, so the SDK rejects values beyond `Number.MAX_SAFE_INTEGER`. One-second final drain is bounded. CLI gap output names daemon head but does not preserve or print the client's last observed cursor.

The log stores raw bytes, not the final screen of a full-screen TUI.
Interactive `ctxmux attach` reconstructs a client view from those bytes;
non-interactive attach and the public protocol still return raw replay.

## Wrong-case corpus

Evidence pack: [output-replay track](../../../.bagakit/researcher/topics/engineering/ctxmux-wrong-case-corpus/tracks/output-replay.md), claim `C005`.

- `OR-001` (`e02`): a PTY read can split or coalesce UTF-8, ANSI, NUL, and arbitrary binary data. Only ordered byte concatenation is invariant.
- `OR-002` (`e01`): live broadcast lag becomes silent loss if a client resumes from daemon head instead of its own last confirmed cursor.
- `OR-003` (`e03`): a descendant may retain the PTY slave and write after the direct child exits and the one-second drain expires. The fixture must characterize the chosen direct-child versus PTY-EOF contract.
- `OR-004` (`e04`): screen state has viewport, resize, alternate-screen, and dirty-line failure modes. A rendered screen is not an oracle for raw replay.

Short reads do not preserve application write boundaries. The core promise is byte order, explicit gaps, and honest retention, while screen reconstruction remains a client-layer capability.

## Fixture mapping

- Covered now: exact binary retained replay after exit, one terminal event on late attach, ordered reconnect, and cursor/retention boundary units.
- Covered now: replay larger than the 1 MiB frame ceiling is streamed and
  reassembled exactly across native and TypeScript public clients.
- Candidate: cursors at zero, `oldest - 1`, oldest, head, and future.
- Candidate: retention eviction across many chunks and one oversized chunk.
- Covered now: a real attachment observes `Gap`, disconnects, replays from the caller-owned cursor, and proves contiguous byte ranges plus exact raw-byte continuation through the public protocol.
- Covered now: a 5 MiB final-output workload retains the bounded tail, marks
  truncation, preserves the final marker, and remains attachable.
- Candidate: attach to an already-exited Run.

## Open questions

- What drain guarantee can be proved across PTY implementations?
- How are retention limits configured or capability-reported without adding premature configuration layers?

## Repository evidence

- `crates/ctxmux-daemon/src/lib.rs`: `OutputLog`, `read_output`, `handle_attachment`
- `crates/ctxmux-protocol/src/lib.rs`: `OutputChunk`, `OutputReplay`, `RunEvent`
- `docs/protocol.md`: output and reconnect
- `crates/ctxmux-daemon/tests/native_lifecycle.rs`
