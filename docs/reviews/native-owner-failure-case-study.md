# P0 case study: shared Native owner failure

The incident exposed a shared PTY service failure: active CLI processes were
alive but blocked writing stdout, Native input drains were blocked in kernel
writes, and the shared output owner was absent from repeated samples. Public
Start failed at owner registration. Cached `running` described child lifecycle;
it did not establish usable input or output.

Feature Tracker `f-22vcz84zn` owns repair and integrated acceptance. The resource
audit `f-22tcz9d9r` retains its independent qualification. This report documents
evidence and requirements; it does not declare either Feature complete.
Original process identities, timestamps, paths, stack samples and hash manifests
are retained in private Feature artifacts. Public source excludes those local
environment details. `scripts/check-native-owner-case-evidence.py` verifies the
retained intake and fails when its private evidence is absent.

## Established facts and remaining uncertainty

The intake binds two public observations to the same Runtime, daemon instance
and source `ec637607946718c8bdec0a215b47a72f4a1692da`. Six retained Runs reported
running. Five active CLI main-thread samples showed stdout system-write
blocking; four Native drain samples showed `write_all` blocking. The shared
owner was absent. Stationary cursors alone would not establish a fault for a
quiet process; the independent stacks and registration failure supply the
service evidence. The fleet sampling sent no input, signal, Stop or restart.
A separate public Start probe failed and is attributed separately in the intake.

A default Session-store query initially returned no Sessions. Explicit queries
of the Desktop store succeeded for the queried incident Sessions. There was no
evidence of Session loss. Endpoint, store, Runtime, daemon incarnation and Run
must be bound before interpreting an empty query as data loss.

The first owner-exit trigger remains unknown. The old thread did not retain its
completion result; its stderr consumer was absent. Parsing preceded raw-byte
admission, so the fatal chunk might never have entered replay. A reproduced
parser defect cannot supply the missing incident stack.

The earlier `WAL exceeds 16 MiB` failure is distinct. Its implementation
misclassified valid resource pressure as corruption and could latch mutation
failure. The resource audit repairs that owner. There is no evidence that this
WAL error caused the later Native owner exit. A small later WAL is likewise no
proof of service health or absence of an earlier latch.

## Introduction and amplification

Commit `f42d2fef` introduced one daemon-wide Native output and child owner. Its
resource goal was appropriate: ordinary Runs should not each require permanent
threads. The missing boundary was containment and observable owner completion.
A local exception could end the shared loop while clients continued seeing
child lifecycle and static control capabilities.

Commit `8a538cf0` introduced Basic VT continuation. Its old output order was:

```text
PTY read -> shared owner -> derived parser -> raw admission -> publication
```

A legal parser failure could therefore both lose the just-read bytes and end
service for unrelated Runs. Resize, checkpoint export, retention cuts, recovery
and handoff also invoke derived work and require containment. Catching only
`process()` would leave the other causal paths open.

Exact-source counterexamples established these defects:

| Legal operation                                   | Observed defect                                             |
| ------------------------------------------------- | ----------------------------------------------------------- |
| Save cursor, shrink geometry, restore and write   | Saved cursor remained outside the resized grid and panicked |
| Save cursor, shrink, export checkpoint            | Export traversed an invalid saved position and panicked     |
| Write a wide character into a one-column terminal | Debug arithmetic overflow                                   |

The caught-panic diagnostic harness exited successfully; that exit was not
acceptance. Release behavior must be verified independently. Vendored provenance
alone does not establish whether current upstream has the same defect.

The lack of an output consumer explains the observed backpressure path: CLI
stdout can stop advancing, then PTY input writes can block. The samples do not
establish how many bytes of each message were applied. Shared blocking input
workers also create a separate starvation defect: a blocked Run retains a
worker indefinitely. Increasing the worker count postpones that failure rather
than repairing progress ownership.

## Repairs and evidence boundaries

Commit `216c85a` repairs saved-cursor clipping, legal pending wrap, clipped wide
cells, narrow geometry and single-row wrap. Sixteen owning codec tests passed;
five source reversals produced actual assertion failures and restored controls
passed. These proofs qualify a parser slice, not full terminal fidelity.

Commit `581a5d9`, with preserved logs in `5c2fb50`, admits raw bytes before derived
work and contains Run-local process, resize, export/cut and recovery failures.
A damaged model is discarded and continuation becomes explicitly unavailable.
Original raw output, independently owned input and the other Run remain usable.
The lifecycle guard rejects a genuinely finished owner before PTY creation and
at registration and handoff. It does not manufacture child exit or disable an
independent input lane merely because the output owner stopped.

Independent review verified the author manifest's 90 file hashes and 12 final
source inputs. The finite isolation proof used two real PTYs/Runs and two public
Rust clients, checked ordered nonempty durable bytes and input receipts, retained
Run/child identity across client release and reattach, and exercised raw PTY
Ctrl+C. Five effective source reversals failed; two failed by loss of progress
and three by contract assertions. An equivalent compound-assignment reversal
stayed green and was retained as counterevidence.

Additional held-out qualification found a distinct checkpoint-loss case:
a 2-by-2 parser receives `abcdefgh`, then grows to 3-by-4. The source shows
`ef\ngh`, while checkpoint restoration shows `\ngh`. Old-width wrapped history
is rendered using the new geometry. This failure remains open and must not be
hidden by the original sixteen passing fixtures.

The resource and Native candidates both advertised protocol generation 18 but
have different wire shapes, persistence schemas and handoff formats. Integration
must preserve funded raw replay, durable pagination and complete operation
ledgers, define one exact joined protocol, and reject incompatible transitions
before mutation. Installing one candidate does not qualify the joined candidate.
Semantic Session resumption into a new Run is distinct from preserving an old
PTY or child; unknown input must never be replayed automatically.

## Required completion

| Boundary                               | Acceptance                                                                                              |
| -------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| Raw admission and local derived faults | Fatal chunk remains byte-exact; second real Run continues serving                                       |
| Owner completion                       | Real snapshot/event facts; no cached lifecycle presented as service availability                        |
| Input progression                      | FIFO, real partial/unknown results, same-key recovery, blocked-Run isolation                            |
| Client churn                           | Two clients disconnect/reconnect while the original child survives                                      |
| Ctrl+C                                 | Ordered control byte through the original PTY, with observed process behavior                           |
| Restart classes                        | Client restart, healthy planned exec and cold historical recovery proved separately                     |
| Joined contract                        | Same source/binaries/protocol; complete ledgers and explicit schema/format rejection                    |
| Performance and resources              | Correctness, reliability, throughput, latency, memory, descriptors, threads, storage and recovery costs |
| AgentMux consumption                   | Correct errors/recovery actions, Session/Run identities and retained workspace presentation             |

Terminal history, restore bytes, resize-tail length and input worker constants
require units, a protected finite resource, derivation and truthful pressure.
Qualification workload sizes do not define product capacity. No passing result
may be obtained by dropping bytes, weakening durability, lowering fidelity,
reducing workload, relaxing budgets or skipping failures.

## Prevention

Every derived operation added to a shared owner needs a fault-domain analysis
and a two-real-Run failure proof. Raw facts must survive view failure. Owner
completion needs an actual consumer and must not depend on writable stderr.
Input receipts must be checked against real peer bytes, including uncertain
outcomes. Geometry qualification must combine resize, saved cursor, wrap,
Unicode, byte fragmentation and checkpoint continuation on held-out sequences.

Auto-research objectives must follow these user-visible contracts. Normal-path
throughput, thread count or CPU improvements cannot substitute for availability,
capacity, fidelity and recovery. A green fixture proves only its actual oracle.
The evidence does not establish deliberate reward hacking; the repair is to
remove misleading objectives and strengthen causal ownership.

Original incident trigger, proactive service observations, fair input and final
joined consumer acceptance remain open. Source review, a passing author slice,
installation and complete product acceptance are separate facts.
