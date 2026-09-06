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

A separate causal probe found that infallible stderr diagnostics panic when
their pipe receiver closes. After two real Runs served exact input/output, a
memory-only SIGHUP diagnostic exited the old daemon; fallible writes preserved
the same daemon, Runtime, original Runs and child identities, exact subsequent
bytes and completed public Stop. A real Native read-error probe also preserved
the healthy Run after its diagnostic failed; reversing that diagnostic alone
failed the same probe. This establishes another shared-owner amplification
path, not the missing original incident trigger. A full, open diagnostic pipe
can still block, and truthful public service observations remain required.

The resource and Native candidates both advertised protocol generation 18 but
have different wire shapes, persistence schemas and handoff formats. Integration
must preserve funded raw replay, durable pagination and complete operation
ledgers, define one exact joined protocol, and reject incompatible transitions
before mutation. Installing one candidate does not qualify the joined candidate.
Semantic Session resumption into a new Run is distinct from preserving an old
PTY or child; unknown input must never be replayed automatically.

## Orca implementation comparison

The comparison is pinned to Orca commit
`3135fbbf49fef6199cdf1882eb9a3141af7ff098`. Its inspected source hashes and
read-only review are retained with the private incident evidence. Referenced
Orca tests were read, not run. These source findings do not establish the
original ctxmux incident trigger.

Orca's `pty-write-settlement.ts` distinguishes accepted, refused and
unverifiable transport delivery. Its stream batcher retains Session-local FIFO
while allowing another Session's small output to pass held bulk. Its connection
lifecycle binds actual daemon identity, and its process observation separates
live, unverifiable and exited. These mechanisms support separate service facts,
explicit ambiguity and fair scheduling; transport acceptance still cannot
substitute for ctxmux's completed PTY byte receipt.

The comparison also identifies behavior ctxmux must correct rather than copy:
`SubprocessHandle.write` catches native failure and returns without an applied
result, while the request router can still return success. Derived headless
parsing precedes recording. Startup buffering discards an old text prefix, and
local reattach removes existing viewers. Those paths do not satisfy byte-exact
durable output or concurrent Clients. Synchronous diagnostic logging can still
block on a stalled sink even when errors are caught.

Orca's public xterm shrink tests provide a useful consumer-oriented example.
Their dimensions, history length, batching constants and simulated sockets are
not ctxmux capacity requirements or real PTY fairness evidence. The remaining
ctxmux proof must fill a real non-reading child's input buffer, retain its
original request and confirmed prefix, and demonstrate another original Run's
input, output and Ctrl+C. Screen restoration or a replacement shell cannot
stand in for original child continuity.

## Qualified repairs and remaining failures

A controlled upgrade failure exposed another causal owner boundary: loss of
non-output observations closed an attachment while an admitted input was still
partially written. That destroyed its unique response owner and released upgrade
ownership before the actual result. The repair retains admitted results through
send and flush, explicitly refuses unadmitted controls, and keeps view loss
separate from child death. The original six upgrade cases pass on their bound
candidate; that result does not reconstruct the first incident's missing stack.

A fixed 256-event client queue could also turn view pressure into a protocol
failure, destroying pending input results. The client repair uses separately
configurable payload and envelope windows, preserves the original payload
window, exposes local observation loss and keeps validated admitted results
alive. Actual connected-client tests retain the input result and clean Detach;
a wrongly correlated result still fails. This policy bounds retained view work,
not total process RSS; its additional envelope allowance has a memory cost.

The daemon's fixed command-first polling then starved observation delivery in
the original thousand-input pipeline. Fair polling passes the unchanged byte,
resize and Stop oracle; reinstating only the old priority produces a failure.
Neither the daemon ring size nor its byte budget was increased for that proof.
The ring's remaining population policy is a separate unresolved resource audit.

The finite normal-height repair has macOS and Linux evidence. A clean normal
artifact producer generates the release package. Its actual new daemon is
independently checked by upgrading two original live Runs with two Clients:
identities and child PIDs survive, exact bytes and six hundred colored rows
remain, and shrink, growth, reattachment, Ctrl+C and the second Run's input and
Stop pass both public terminal consumers. This does not qualify all alternate
screen state, already damaged history, or old input pending across that height
upgrade. The existing alternate-screen seed discrepancy remains recorded.

The first complete joined Native lifecycle invocation passed 52 of 55 cases.
Two fixtures incorrectly rejected the candidate generation or legal service
events. Their repairs retain the incompatible-generation rejection, byte and
resize assertions. The third failure exposed a real lifetime race: Stop reported
terminal publication before the Native Entry and its two control holders had
actually retired, so immediate Remove correctly refused collection. Stop now
waits for both publication and physical retirement, with the original deadline
and all collection fences. Reinstating only the old success predicate makes all
eight loud Run removals fail again. A fresh independent invocation then passes
all 55 original lifecycle cases without reducing workload or skipping failures.

The SDK passes 107 unit cases and an independent real-daemon qualification.
Its original thousand-input pipeline retains the default payload and metadata
windows without background event draining. The original saturated input fleet
also passes: the non-reading PTY reports actual queued bytes and write blocking,
while another original Run continues output and Ctrl+C. Unapplied and uncertain
results retain their true dispositions and confirmed prefixes. Two Clients
reconnect to the same children and verify exact output bytes. These observations
do not qualify aggregate client heap cost or replace the failed frozen RSS gate.

The normal release producer initially embedded private build paths in its actual
binaries. Canonical compiler remapping corrects this owner while retaining source
filenames, line numbers and panic context under relative labels. The new clean
release package has no occurrences of the checked private paths or machine
username, including the expanded SDK. Its new binary independently passes the
same two-Run planned-exec and eight-stage public-consumer proof. Debug linker
paths are separately unqualified; no compiled binary was stripped to obtain the
release result.

The qualified height release uses protocol 18, state schema 4 and handoff schema 4. The joined candidate uses protocol 20 and state/handoff schema 6. Its same-
candidate upgrade tests do not establish continuity from the installed release.
A real old-to-joined attempt refuses the unsupported handoff before extraction.
The old executable remains mapped, both original child and Runtime identities
survive, and both Runs still produce exact input/output, reconnect, resize and
Ctrl+C results. This qualifies safe refusal only. Restarting or using a new
namespace cannot substitute for successful live upgrade continuity.

Geometry review also exposed a real mismatch: an alternate-buffer wide lead
survives a narrower viewport in public terminal consumers, while the derived
model displayed a blank. Right-neighbor drawing and erase corrections now
preserve the lead and its attributes; all seventeen revised codec cases pass. Those objectives were
independently adjudicated against public consumers while retaining the original
failed source and oracles. Held-out partial growth can discard its hidden
continuation while keeping the lead; a complete visible wide pair is therefore
not a universal resize invariant. The current single-resize seed still fails
continuation of that state. Four other held-out cases pass; that strict failure
remains recorded. A separate multi-stage prototype reproduces two simple cases
in both public consumers, but broader held-out sequences reject it: accepted
seeds lose normal history or restore a saved cursor to the wrong row. Other
legal sequences remain unsupported. Independent sequences without a checkpoint
also disagree on combining characters and normal-buffer return. These facts
require separating parser, buffer-transition and restoration defects; a new
seed protocol alone cannot establish fidelity. None of the prototype results
qualifies production continuation.

The resource audit distinguishes local seed policy from structural validity.
Both clients previously rejected a valid seed above a hardcoded 32 MiB policy
as a protocol violation; host allocation failure could be misclassified the
same way. The candidate separates structural validation from configurable
per-seed receive policy and fallible allocation. Rust passes all 27 client
cases and a real two-Run, two-Client local-refusal proof: the same children keep
serving raw bytes, default terminal restoration and Ctrl+C. The SDK passes 129
cases, including actual socket assembly across the historical size boundary.
Identity, geometry, chunk and receipt checks remain strict; no Terminal request
silently becomes Raw. These are per-seed consumer proofs. Export checks after
allocation still cannot protect that allocation, and queued seed copies lack
one aggregate lease across cache, sender and recovery. Increasing a fixture or
queue limit would not resolve those remaining owners.

Compiling the complete candidate also exposed a CLI ownership defect: an
unsettled input future borrowed the attachment that detach attempted to move.
The candidate separates the borrowed interactive loop from attachment release.
When an input result is unresolved, leaving closes only the view and warns
against automatic replay. A real controlling-PTY test keeps the blocked Run
and its identity alive, restores local terminal mode, and requires CLI exit
before public Stop releases the blocked write. Replacing only that close with
clean detach fails the unchanged five-second deadline. A final complete CLI
invocation passes 20 cases with strict lint and formatting. An earlier invocation
failed while waiting for input state; its cause is not established. The revised
fixture explicitly observes the child's raw-mode readiness before admitting
the same workload, and retains the original deadline and failure evidence.
Full local stdin queues, unsent buffered input, blocking output sinks and
activation handshakes remain separate client-owner qualification requirements.

A source-bound serialization candidate removes complete base64 and JSON copies,
checks file admission before buffering, flushes before sync and preserves atomic
replacement. Three targeted cases and all five original real checkpoint owner
cases pass. Exact binary JSON is preserved at padding and chunk boundaries, and
one-byte-over-policy failure keeps the old file without abandoned temporaries.
The first standard-buffer cost probe also exposes slower serialization and more
file writes. A separate buffer comparison reduces that overhead, but host latency
and full aggregate funding remain unqualified. Memory savings alone do not sign
this candidate; fsync, byte fidelity and accepted workload are unchanged.

Occasional startup deadline failures, allocation-before-budget paths, fixed
history and checkpoint policies, complete resource costs and final AgentMux
usage remain open. A later successful startup does not erase the failed launch
or establish its cause. The original shared owner's exit trigger is still not
reconstructed from a retained stack.

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

Original incident trigger, complete service and input qualification, and final
joined consumer acceptance remain open. Source review, a passing author slice,
installation and complete product acceptance are separate facts.
