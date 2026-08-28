# 014 — Recoverable native Input operations

- Status: accepted and implemented
- Scope: same-incarnation retry of short-lived native Input after response loss

## Context

Generation 5 correlates attachment controls and returns precise owner receipts,
but its command IDs end with the connection. If a response disappears after a
PTY write, a caller cannot tell whether retrying would inject the same bytes a
second time. This is visible in terminal supervisors such as Orca: accepting
characters into a PTY is useful evidence, but it is neither proof that a TUI
submitted them nor proof that another Agent understood a message.

Bounded peer research found that the reviewed tmux, Zellij, WezTerm, and Orca
public mutation surfaces expose actions but not a caller-keyed lost-response
result lookup. Product review selected Input as the smallest independently
valuable ctxmux closure.

## Decision

Generation 7 adds one recoverable short-lived native Input operation. Its
canonical request binds:

- one bounded opaque `InputOperationKey`;
- the exact daemon-incarnation identity advertised by the handshake;
- one `RunId`;
- one non-empty byte payload;
- the expected applied-input byte cursor.

The native Input owner remains the only physical writer. It serializes the
operation with existing input, consumes one contiguous range only after the
complete payload and flush cross the PTY write boundary, and returns
`[start_byte, end_byte)`. The Run's public applied-input cursor advances for
every successful legacy or recoverable PTY write so other input cannot be
silently skipped.

Operation identity is the tuple `(daemon incarnation, RunId, key)`. While that
tuple is pending or retained, a byte-exact matching request joins the operation
or returns its completed result without another write; a different expected
cursor or payload is a typed conflict. Keys on different Runs do not require a
daemon-global index. Clients use a fresh key for every new logical operation.

Key uniqueness is not permanent within an incarnation. After a completed
result is evicted, the old key may identify a new operation only when the caller
also supplies the current cursor. Retrying the original request still carries
its older cursor and fails before mutation, so eviction cannot duplicate its
bytes. This avoids unbounded tombstones or a second identity owner. Empty
recoverable input is rejected because it cannot leave cursor evidence after
result eviction.

The per-Run result ledger is bounded by both entry count and retained request
bytes. Once a successful result is evicted, its original expected cursor is
necessarily behind the Run cursor, so replay fails closed rather than becoming
a new operation. A partial write, flush failure after an uncertain write, or
writer panic consumes or fences that operation, returns `unknown`, and poisons
the input lane; ctxmux does not invent an applied byte range.

The daemon incarnation is random and changes on every cold start. Decision 015
preserves it across an intentional exec-in-place upgrade and therefore carries
the complete settled Run-local ledger, poisoned-lane state, and input cursor in
the validated handoff manifest. A request bound to an earlier cold incarnation
is rejected before Run lookup or PTY mutation. Fresh clients must retain the
operation's original incarnation rather than silently replacing it with the
current handshake value. Ctxmux does not claim cross-crash exactly-once Input:
a PTY write and a SQLite result cannot be committed atomically without a
cooperating target protocol.

## Success boundary

```text
admitted / pending                         # not a wire success receipt
  -> bytes_applied [start_byte, end_byte)  # ctxmux
  -> acknowledged                         # Integration / target protocol
  -> replied or settled                    # Agent harness
```

The daemon owns only admission and `bytes_applied`. Generation 7 does not add a
separate asynchronous `accepted` frame; public success remains the final
applied range. The daemon must not expose Agent,
Message, Delivery, ACK, Reply, Task, dispatch, DAG, or UI-timeline concepts.

## Rejected alternatives

- Treat `accepted=true` or a completed socket write as semantic delivery. A PTY
  has no knowledge of TUI state or message submission.
- Replay all unknown input. This can duplicate irreversible terminal actions.
- Persist the ledger across cold daemon restart. SQLite cannot share an atomic
  commit boundary with an external PTY write. Decision 015's planned exec is
  not a cold restart: the same live owner carries the in-memory ledger and PTY.
- Generalize Input, Resize, Stop, and Signal behind one public transaction
  framework. Their targets, idempotence, results, and failure algebra differ.
- Implement process-group Stop in this Feature. It is valuable but owns a
  separate lifecycle and quiescence contract.

## Evidence

The representative tests use real children and the public client boundary. One
drops the first response after a physical write, reconnects with the exact same
operation, observes the original byte range, and proves from child output that
the payload arrived once. A second performs a real exec-in-place upgrade between
response loss and retry and proves the same range, cursor, and one physical
payload survive. Focused variants prove conflict, cold-daemon replacement,
unknown poisoned-lane handoff, and bounded-ledger stale-cursor rejection. Rust
and TypeScript clients must agree on the generated wire shape and failure
vocabulary.

## Wrong-case corpus

- `INPUT-01` transfers the response-loss duplicate-write failure. It is active
  under T-004: a real child proves that a dropped first response followed by an
  exact fresh-client retry returns the original applied range while the PTY
  observes one payload. Focused owner and public tests cover retained conflict,
  stale cursor, partial-write unknown, and replacement-daemon fencing.
