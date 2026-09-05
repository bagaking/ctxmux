# A2A queue fields already exposed per Run

Reader aid for consumers building A2A envelope queues on top of ctxmux Runs.
Not a new ctxmux public contract; ctxmux is a Run multiplexer, not a messaging
layer. Every field named here is already public in
`crates/ctxmux-protocol/src/lib.rs` or `docs/protocol.md` — read those for the
authoritative wording.

## What ctxmux offers per Run

### `RunSummary.retained_output_bytes`

Output bytes the Run is holding in memory right now. Declared at
`crates/ctxmux-protocol/src/lib.rs:1017`. Design intent is the block of
comments immediately above it (`crates/ctxmux-protocol/src/lib.rs:1006-1016`):

> An external harness summing this field across a `List` gets the same
> quantity the daemon caps, so the cap becomes checkable from outside.

A consumer that already lists Runs for its own reasons therefore has the
retained-bytes signal without a second computation. It is not a lifetime total;
see the next field.

### `RunSummary.latest_output_bytes`

Lifetime total, monotonic. `crates/ctxmux-protocol/src/lib.rs:1001-1005`
states it counts bytes that have passed through and never decreases when the
scrollback is trimmed. Do not read it as memory currently held —
`retained_output_bytes` is that quantity.

### `RunInfo` output cursors

`RunInfo` carries three cursors, all in
`crates/ctxmux-protocol/src/lib.rs`:

- `latest_output_bytes: u64` — line 911; total output bytes allocated so far.
- `durable_output_bytes: Option<u64>` — line 914; highest contiguous byte
  committed by the store actor, or `None` when the daemon runs without a state
  directory.
- `first_available_byte: u64` — line 916; first output byte still retained,
  or zero before output.

These three are the byte-cursor triple a consumer needs to place a queue
watermark against ctxmux's own retained window.

### Attach header replay metadata

`docs/architecture.md:319-320` states that the daemon sends an `Attached`
header containing `RunInfo`, replay cursors, and `truncated`. The concrete
wire shape is `OutputReplayHeader` at
`crates/ctxmux-protocol/src/lib.rs:1143-1149`, whose fields are
`first_available_byte`, `latest_output_bytes`, and `truncated: bool`. A
consumer subscribing from a stored cursor learns immediately whether the
window it requested is still retained.

### Live events on the attachment stream

`RunEvent`, declared at `crates/ctxmux-protocol/src/lib.rs:1540-1572`, carries
the variants a queue reader observes:

- `Output { chunk }`
- `Resized { size }`
- `Exited { state }`
- `Interrupted { reason }`
- `Tmux { event }` — Backend-specific observable event that does not change
  generic Run ownership semantics.
- `ObservationDiscontinuity` — one or more non-output observations that have
  no authoritative snapshot were not delivered.
- `Gap { latest_output_bytes }` — raw-output delivery discontinuity.
  `docs/protocol.md:634-638` states this is not a recovery cursor: the caller
  reattaches using its own last observed byte cursor.

### `ControlBackpressure` with `NotApplied`

Live-control admission failure is a first-class disposition, not a crash.
`docs/protocol.md:517-519`:

> The `control_backpressure` code reports bounded live-control admission
> failure with `not_applied`; input saturation must not be represented as
> successful acceptance or allowed to starve resize and stop.

A consumer that offers back-pressured send should treat this the same way:
`not_applied` is retriable, and the enqueue must not be recorded as accepted.

## What ctxmux does NOT own

- Envelope identity, message boundaries, delivery acknowledgement. `AGENTS.md`
  invariant 4 (`AGENTS.md:39-41`) says verbatim:

  > ctxmux is a multiplexer, not an Agent Harness. It does not plan work,
  > schedule teams, judge results, select winners, or own Crucible/MapReduce
  > policy.

  `docs/vision.md:82-87` extends this to messaging directly:

  > ctxmux should make parallel search, Context fork, Crucible, and MapReduce
  > easy to compose. It does not own their scheduling, evaluation, or
  > stopping policy. Message delivery, semantic acknowledgement, reply
  > correlation, and task state likewise remain embedding-client
  > responsibilities rather than daemon Run semantics.

- Principals, sender identity, A2A auth. Not surfaced anywhere in
  `crates/ctxmux-protocol/src/lib.rs`. `docs/architecture.md:581`:

  > authentication beyond filesystem access and peer-credential policy is
  > open.

- Cross-Run message routing. `Run` is the universal core object
  (`AGENTS.md:32`); nothing above the Run boundary is a ctxmux concern.

- Per-envelope idempotency. `docs/protocol.md:458-459`:

  > An attachment command ID provides correlation only. It is not an
  > idempotency key, permission to retry, durable command identity, or
  > deduplication record.

  A consumer building an at-least-once or exactly-once queue must own that
  layer itself.

## Caveats

- `retained_output_bytes` can transiently exceed the per-Run 4 MiB target.
  `docs/architecture.md:583` states:

  > Each Run retains at most 4 MiB of raw output by byte count, except that
  > one oversized final chunk may exceed that target because the log always
  > retains at least one chunk.

  A cap check that treats the target as a hard maximum will false-alarm on
  the tail chunk.

- The number changes continuously. Poll `List` or subscribe on an attachment;
  do not cache the value.

- `durable_output_bytes` is `null` in memory-only mode.
  `docs/protocol.md:616-618`:

  > `RunInfo.durable_output_bytes` is `null` in memory-only mode. In
  > persistent mode it is the highest contiguous output byte committed by the
  > store actor and may lag the live `latest_output_bytes`.

  A queue whose durability watermark reads this field must handle `None`
  explicitly rather than substituting `latest_output_bytes`.

## Cross-references

- `docs/protocol.md`
- `docs/architecture.md`
- `AGENTS.md`
