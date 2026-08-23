# 017 — Recoverable native Stop operations

- Status: accepted and implemented
- Scope: same-incarnation recovery of one native complete-session Stop result
  after response loss

## Context

Native Stop already has a strong owner boundary: admission closes later
mutation, the existing process-session owner performs graceful and forced
cleanup, the direct child is reaped, and success is returned only after the
owned POSIX session is empty. That proves what Stop did, but an ordinary
request or attachment command ID proves nothing after its response connection
is lost. Blind retry can enter a second logical Stop and returns an unrelated
state error instead of settling the first operation.

Recoverable Input cannot be generalized here. Input can use a byte cursor and
a bounded multi-entry Run-local ledger; Stop is a single terminal mutation with
one complete-session owner, one receipt, and a retention lifetime tied to the
Run itself.

## Decision

Generation 12 requires every native Stop path to carry one
`RecoverableStop` containing the original daemon incarnation, a caller-retained
opaque `StopOperationKey`, and the exact `RunId`. Keys are non-empty UTF-8 of at
most 128 bytes and are compared byte-exactly.

The Registry lock atomically orders three facts before mutation:

- one Runtime-global key maps to at most one retained Run;
- one retained Run owns at most one admitted Stop operation;
- the existing Run Stop state machine admits the physical operation once.

An exact same-key/same-Run retry joins the in-flight result cell or replays its
settled receipt. Another key for that Run or the same key for another Run
returns `stop_operation_conflict` with `not_applied` before entering Stop.
Daemon-incarnation mismatch is checked before Run lookup. Short requests,
attachment commands, and the explicit `attach_recoverable_stop` composite call
this same admission boundary; attachment command IDs remain connection-local
correlation only.

The existing complete-session Stop implementation remains the only signal,
process census, cleanup, direct-child reap, and session-quiescence owner. This
decision wraps that owner's admission and result; it adds no second walker,
signal path, or Stop state machine. A settled retry returns the same
`graceful` or `forced` receipt. Its accompanying `RunInfo` is a current metadata
snapshot and may advance from `running` to `exited` between retries.

## Failure and retention algebra

A failure proven `not_applied` releases the key and per-Run record because no
Stop mutation occurred. An accepted receipt or an honestly `unknown` owner
result remains in the record, so response loss cannot authorize another Stop.
Client transport loss remains unknown to that call; a fresh client settles it
only by retrying the complete retained operation.

The ledger is bounded structurally to at most one record per retained Run.
Collection cannot remove a Run while an in-flight settlement pins it and
atomically removes the per-Run record plus global key mapping at the exact Run
collection boundary. The old key may identify another operation only after
that removal.

A validated planned exec preserves the daemon incarnation and carries every
settled accepted or unknown record. Pending settlement aborts the reversible
handoff phase instead of being serialized. Cold replacement creates another
daemon incarnation and does not recover the ledger; an old operation therefore
fails `daemon_instance_mismatch`. ctxmux does not claim cross-crash exactly-once
Stop or adopt a live process from persisted metadata.

## Public surface

The Runtime advertises `native.recoverable_stop: 1` only with the complete
contract. Rust and TypeScript clients prepare a caller-retainable operation and
require it on short Stop, live attachment Stop, and the explicit composite
recover-and-attach call. Ordinary late attachment remains observation-only and
closes after its one terminal event. A caller that must recover through a fresh
attachment carries the complete retained operation in the initial composite
request, so the daemon can resolve the ledger before establishing snapshot,
replay, terminal event, and EOF without a timing window. The CLI accepts an
explicit daemon instance plus operation key for retry, or generates a fresh
operation for one-shot use, and prints the owner-authored Stop disposition.

This is a narrow Stop contract, not a generic recoverable-mutation framework.
Resize and Interrupt remain non-recoverable. AgentSession, Provider, permission,
message, and Desktop Workbench close transactions remain outside ctxmux.

## Rejected alternatives

- Treat attachment command ID or socket write completion as retry evidence.
  Both end with the connection and cannot prove owner settlement.
- Keep every terminal attachment open in case a later Stop command arrives.
  That removes terminal EOF, pins retained Runs, and cannot distinguish an
  ordinary observer from a recovery caller. Recovery intent must be present in
  the initial request.
- Make repeated ordinary Stop return success. That loses operation identity and
  can disguise another caller's Stop or natural exit.
- Reuse Recoverable Input's ledger abstraction. Its cursor, eviction proof, and
  multi-operation lifetime do not match a terminal complete-session mutation.
- Persist Stop receipts across cold restart. The new daemon owns neither the old
  incarnation nor atomic process-side evidence.
- Add another process supervisor or walker. The existing Stop owner already
  defines and proves the supported process scope.

## Evidence

Real native-process tests drop the first response, replace the client, retry the
same operation, and prove one physical Stop effect plus the original receipt.
Focused variants cover concurrent join, different-key and cross-Run conflict,
short/live-attachment/composite convergence, ordinary terminal EOF, forced
disposition, daemon replacement, planned exec continuity, collection pinning,
and post-collection key reuse. Generated TypeScript, CLI, and packed-consumer
tests exercise the same public protocol.
