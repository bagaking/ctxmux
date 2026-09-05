# Case study — replay capacity and Stop completion

## The failure chain

Moving replay out of SQLite removed a storage bottleneck, but exposed two
different lifecycle boundaries: releasing native cleanup capacity and publishing
terminal Run state. Fixing either boundary alone did not satisfy the consumer's
complete operation: Stop, then observe a terminal Run.

This is a causal review, not another protocol specification. The owning
contracts remain [Stop receipts](../protocol.md#control-correlation-and-owner-receipts),
[recoverable Stop](choices/017-recoverable-stop-operations.md), and
[native ownership](choices/001-rust-tokio-daemon.md).

| Symptom                                                 | Established mechanism                                                                                      | Resolution                                                                                 |
| ------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| Replay exhausts the SQLite main-file ceiling            | Payloads, row headers, and indexes compete inside the same bounded file                                    | Store replay in append-only generations; retain transactional window metadata in SQLite    |
| Stop admission is refused while finalization is blocked | Cleanup and finalization share scarce worker capacity                                                      | Release cleanup admission at cleanup completion; give finalizers a separate bounded budget |
| Stop succeeds but List still reports `running`          | Physical cleanup settles before terminal publication; the public response consumes only the cleanup result | Join public success to terminal visibility across all Stop entry points                    |

## The storage boundary

The [capacity investigation](../plans/replay-capacity-beyond-the-384-mib-ceiling.md)
found that bookkeeping occupied 42% of a full 384 MiB main database. Compressing
payloads would improve a constant factor without removing the shared ceiling.
External replay generations change that boundary: SQLite holds the retained
window index and cursors, while files hold the bytes.

The change introduces an explicit file/metadata commit boundary. Payload sync,
generation selection, abandoned-tail truncation, and orphan cleanup must agree
after recovery. It does not remove logical retention or aggregate storage
limits, and no schema migration or compression fallback was added.

## Two lifecycle facts had been treated as one

The native owner proves that the direct child is reaped and the owned session
is empty. The publication owner makes terminal state visible after the
persistence finalizer returns. A storage barrier can separate these facts.

If a finalizer retains a cleanup slot, blocked storage prevents unrelated
process cleanup from being admitted. Separating the worker budgets fixes that
resource dependency. It does not change which fact the public Stop response
waits for.

At `c019b00`, the public response still followed physical cleanup alone. The
consumer could complete every Stop, confirm that the children were gone, then
call List and observe `running`. The protocol permitted this intermediate state;
the consumer expected a stronger completion boundary.

An extra FD sample was a separate observation, not proof that the same resource
was leaked. A single sample cannot distinguish a retained descriptor from one
whose owner is still releasing it. Stable cleanup observations are useful, but
neither widening the FD budget nor inferring lifecycle state from process
absence repairs the public completion contract.

## The assumption that had to change

[R22](r22-the-stop-that-stops-lying.md) protected Stop followed by Remove.
[R26](r26-the-finalize-that-blocks-a-stranger.md) exposed the dependency on a
shared persistence actor. [R36](r36-the-stop-that-did-not-need-the-actor.md)
moved the visibility wait to Remove under the assumption that only removal
needed terminal visibility.

The Stop-then-List reproduction disproved that assumption. R36's measurements
remain evidence for its candidate and contract; its recommendation is not the
current Stop contract. Moving the wait back into only the short-request handler
would also leave attachment Stop and recover-and-attach with different answers.

The correction belongs in `RecoverableStopFlight::resolve`, which all public
Stop paths share. The operation ledger retains physical cleanup exactly once.
Public success additionally requires terminal visibility. An exhausted
publication wait returns `unknown` to that request without replacing the
ledger's accepted cleanup result. The same operation key can confirm publication
without sending another signal. Client disconnect does not own settlement.

This preserves finalization-before-publication ordering. It also preserves a
real cost: public Stop completion can depend on the shared persistence actor.
Separate worker budgets protect process cleanup; they do not make storage
independent or eliminate that dependency.

## Evidence and its limits

| Evidence                                                                                                                                        | What it establishes                                                                                                      |
| ----------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| `stalled_durable_publication_does_not_consume_cleanup_admission` in [creation tests](../../crates/ctxmux-daemon/src/tests/creation.rs)          | All 16 children can be reaped while finalization is parked; public Stop completes and removal succeeds after release     |
| `durable_finalize_keeps_reads_responsive_and_late_output_memory_only` in the same file                                                          | Reads, PTY controls, and native registration remain usable across the storage barrier; terminal state survives reopening |
| `active_durable_finalize_cannot_extend_native_owner_shutdown` in the same file                                                                  | Native owner shutdown does not depend on the parked durable finalizer                                                    |
| `recoverable_stop_response_loss_recovers_from_a_fresh_client` in [native lifecycle tests](../../crates/ctxmux-daemon/tests/native_lifecycle.rs) | A replacement client recovers one physical Stop result and immediately observes terminal state                           |
| Generation recovery fixtures in [persistence tests](../../crates/ctxmux-daemon/src/persistence.rs)                                              | Generation switching, rolled-back tails, and truncated payloads have explicit recovery or rejection outcomes             |

The correction was checked with daemon unit tests, native lifecycle tests,
SDK/CLI E2E, formatting, static analysis, and TypeScript type checking. An
external AgentMux worker also passed with the modified daemon: 16 Runs, 32
attachment cycles, replay truncation, Stop/Input contention, and crash recovery
kept its existing budgets, including one FD per attachment.

That external run was an exploratory artifact substitution. Its inherited
manifest source labels did not attest the substituted build, so it is evidence
for reproducing the consumer behavior, not a release qualification receipt.
The fixture artifacts were restored. The checked-in tests above are the
reproducible guards. Dedicated publication-wait exhaustion and same-key recovery
were not established by the listed tests; the implementation path must not be
presented as separately qualified evidence.

## What to carry forward

- Define success against the caller's next valid operation, not just one
  internal worker's completion.
- Separate resource ownership from response completion. Releasing a cleanup
  slot and acknowledging public Stop are different decisions.
- Park the dependency explicitly in tests; load alone does not guarantee that
  the critical ordering is exercised.
- When a contract changes, preserve experiment results but mark the conclusion
  whose assumption no longer holds.
- Bind validation to its source, inputs, and artifact identity. Passing once
  does not establish broader platform or release guarantees.
