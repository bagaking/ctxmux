# Recoverable native Input verification

## Reviewed source

- Review revision: `049cddd232fccdcac282ca200ae4b3691581ffd7`
- Implementation commit: `9149f3135f3e32c8cbc8d912eac76fbbe051c999`
- Ordinary T-004 Gate: passed at
  `.bagakit/feature-tracker/features/f-22ccz6ux4/artifacts/gate-T-004-r4-0001.log`
- Supporting corrections are independently revertible: `f03b43f` rebinds
  tree-equivalent reliability evidence after the requested timestamp rewrite;
  `049cddd` updates the stale CLI generation assertion.

## Automated Checks

- Command: `scripts/check.sh` through the Feature Tracker Task Gate.
- Result: Passed on the exact clean source revision; the retained T-004 log is
  `.bagakit/feature-tracker/features/f-22ccz6ux4/artifacts/gate-T-004-r4-0001.log`.
- `crates/ctxmux-daemon/src/native_control.rs`: one writer owns legacy and
  recoverable Input; 256 entries and 1 MiB of retained request bytes bound the
  Run-local ledger; partial, flush, and panic ambiguity returns `unknown` and
  poisons only the Input lane.
- `crates/ctxmux-daemon/tests/native_lifecycle.rs`: a dropped first response,
  fresh-client exact retry, later Input, and child-side `AB` oracle prove no
  duplicate physical write; a separate live daemon proves instance mismatch
  before PTY mutation.
- `crates/ctxmux-protocol/src/lib.rs`, `crates/ctxmux-client/src/lib.rs`, and
  `packages/sdk/src`: Rust remains the wire SSOT; generated TypeScript,
  runtime validation, and both clients agree on generation 7 and the exact
  applied range.
- `docs/protocol.md`, Decision 014, and the SDK README stop the success claim at
  the daemon-owned PTY write boundary. Target read, understanding,
  acknowledgement, and reply remain above ctxmux.

## Manual Checks

- Step: Independently inspect the native owner and protocol/client lanes at
  `049cddd232fccdcac282ca200ae4b3691581ffd7` without editing the worktree.
- Outcome: Both lanes reported no P0 or P1 and no Feature drift; bounded P2
  findings are routed below without widening this closure.

Two read-only lanes independently reviewed the finite changed vertical. Neither
lane edited the worktree or treated the prior Gate as proof by itself.

- Native owner lane: reviewed the single FIFO writer, lock order, checked
  cursor, pending join, completed/unknown retention, partial write/flush/panic
  disposition, double ledger bounds, queued rejection, watch completion, GC
  quiescence, real PTY response-loss oracle, and replacement-daemon fence.
  Result: no P0 or P1; no product drift.
- Protocol/client lane: reviewed generation-7 wire shapes, key and UUID bounds,
  safe integers, Rust/TypeScript validation, exact Run/range/current-cursor
  receipts, error disposition, generated declarations, public SDK parity,
  documentation, and the bytes-applied semantic boundary. Result: no P0 or P1;
  no product drift.

The review found and the implementation owner corrected one P1 before the
implementation commit: a retained old range may be returned with a later
current Run cursor. Both clients now require the requested Run and exact range,
while accepting a current cursor at or beyond the range end. The real PTY and
SDK parity tests cover recovery after later Input without another original
write.

## Non-blocking findings and routing

- A new request may evict completed results before later queue/admission checks
  reject that request. Safety remains fail-closed because an evicted exact
  retry has a stale cursor, but the retained recovery window can shorten. Route
  as a P2 input to the bounded Run-Kernel correctness review if
  `f-22bczhydf/T-004` is activated; do not extend this Feature.
- Rust and TypeScript accept the three new pre-mutation error codes with an
  `unknown` disposition from a faulty peer, although the current daemon always
  emits `not_applied`. Route as future protocol-validator hardening.
- TypeScript rejects a tmux Run with an applied-input cursor; the Rust
  recoverable client proves Run/range/cursor but does not separately assert the
  Native backend. Route with the same future cross-language validator parity
  work.
- One protocol unit-test name still says generation 6 for the capacity error
  introduced in that generation. It is historical naming only; current wire,
  binaries, smoke, and docs all assert generation 7.

No P2 changes the same-incarnation no-duplicate-write claim, so none is a
reason to add compatibility paths, a persistent ledger, another runtime owner,
or more validation to this finite Feature.

## Residual Risks

- Result retention is bounded and memory-only. Cross-daemon exactly-once is
  intentionally unsupported and fails closed through the daemon-instance
  fence.
- Attachment command IDs remain connection-local correlation; ordinary Input,
  Resize, Stop, Signal, and SSH do not inherit this recovery contract.
- `bytes_applied` is not an Agent message acknowledgement, delivery receipt,
  reply, or settlement event.
