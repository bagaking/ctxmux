# 004 — Run lifecycle and concurrency model

- Status: accepted implementation; product policy incomplete
- Scope: shared Run state, attachments, mutation serialization, and lifecycle events

## Context

Multiple short requests and long-lived attachments may act on the same Run while blocking output and child-wait work continues. The model must keep client failure local and preserve ordered output without inventing a distributed actor system.

## Decision

`RunManager` retains `Arc<Run>` values behind an `RwLock`. A Run uses narrow standard locks for lifecycle state, output log, PTY master, input writer, and the child-command sender; an atomic counter tracks attachments. The waiter thread exclusively owns the child handle, processes stop there, and disables the sender immediately after wait observes exit. If `try_wait` itself fails, the waiter instead transfers the actual handle once into the native control owner's irreversible fail-stop state. The blocking reader and waiter update the Run, while a Tokio broadcast channel feeds each attachment task.

Attachment subscribes before taking its replay snapshot. An `AttachmentGuard` decrements the counter on every return path, including transport failure.

Test builds can pause the attachment owner at three private points: after
subscribe, after snapshot, and after receiving detach but before acknowledgement.
The hook is not a public fault API and cannot change production scheduling.

## Quality attributes and invariants

- A connection task never owns the Run's last strong reference.
- Attach snapshot and live delivery do not have an uncovered subscribe gap.
- Output byte-range allocation and log insertion are one locked operation.
- Memory-only output does not enter the durable transition/state/persistence
  lock path; persistence-capable Runs retain that ordering before binding.
- A dropped attachment eventually decrements the observable count.
- Lifecycle errors are explicit after a Run reaches `exited`.
- A stop after child wait cannot signal by stale numeric identity, even while
  public state publication is deliberately paused.
- The first unclassified native `try_wait` failure ends polling, closes Input,
  Resize, Stop, and Level-B authority with `backend_unavailable`, fences new
  physical launches, and fails the daemon incarnation. It publishes neither
  `Exited` nor `Interrupted`: no portable child status was observed. The
  native control owner retains the real child handle without calling kill,
  wait, or a cached-PID signal, so same-epoch collection and key reuse remain
  impossible. A persistent restart later performs the existing
  `interrupted { daemon_restart }` reconciliation. A Stop already admitted
  before the failure may lose its reply and remain `unknown`; controls begun
  after the fail-stop fence are `not_applied` with `backend_unavailable`.
- One bounded creation key has one async stripe owner; only its unique leader
  can seek physical-launch admission, and successful mapping plus Run
  publication share one registry write. A separate Tokio semaphore admits at
  most eight unique launches: 64 stripes bound hash-collision state, not launch
  concurrency. The leader resolves a retained match or conflict before waiting
  for admission. Waiting is cancellable and creates no flight or OS thread;
  after admission, the leader reserves one of the same eight private
  rollback-owner slots before spawn, and the permit, stripe, reservation, and
  shutdown-flight guards stay with one named short-lived thread. Existing
  cleanup fences may therefore reject a new leader before spawn without
  changing the cancellable ninth-waiter behavior when no cleanup is retained.
  Request cancellation cannot abandon launch.
- Creation prepares every fallible PTY reader and writer view before physical
  launch. Immediately after launch it constructs native control and arms one
  private publication owner before waiter or output-reader worker setup can
  fail or unwind. A persistence rejection before `COMMIT` asks that same Run's
  child-handle waiter to terminate the unpublished child. The waiter's
  `try_wait(Some(_))` receipt proves reap, but the key reopens only after
  reader, waiter, control, input, and Run owners are also quiescent. Until then
  the publication owner transfers an exact-key fence to one private globally
  eight-slot-bounded cleanup owner before releasing the random stripe and
  launch permit. The same transfer covers worker-setup failure and
  creation-owner unwind. The fence owns no public or durable Run identity.
- Shutdown fences new unbound creation flights before Backend cleanup, then
  drains active creation threads, transferred unpublished-child cleanup, and
  tmux control owners against one bounded deadline. The fence closes semaphore
  admission and wakes queued waiters. Unresolved cleanup reports each private
  fence owner and waiter failure reason without echoing its caller-owned key.
  This narrow owner is not an executor, actor, custom queue, or native
  process-tree shutdown policy; the bounded drain cannot hard-cancel or
  independently reap a creation thread that exceeds its deadline.

## Alternatives

- One actor per Run could centralize ordering but adds a mailbox and supervision model before policy requires it.
- One global mutex would simplify reasoning but couple unrelated Runs and I/O paths.
- Client-owned state would violate the durability invariant.

## Known constraints

`RunInfo` is assembled from separate state and output locks, so it is not a
transactional snapshot. Concurrent writers and resizers have no product-level
arbitration. Signal admission and the Stop phase transition share the native
owner lock, so Interrupt is either ordered before Stop or rejected without
application. Stop acknowledgement proves direct-child reap plus an empty owned
session but still precedes terminal-state publication. Broadcast lag reports
one `latest_output_bytes` but does not automatically replay; callers retain
their own recovery cursor. Persistent same-epoch exited Runs are not yet
collected. Shutdown now fences and drains creation and tmux control owners,
while policy for live native children and other Run mutations remains
unspecified.

Memory-only terminal Runs remain retained below the 128-record Registry ceiling
and become eligible for exact admission-triggered replacement only after every
incarnation-local owner is quiescent. Persistent same-epoch Registry collection
remains open. Poisoned locks recover their inner value; this prevents secondary
panics but is not a declared consistency-recovery strategy.

## Wrong-case corpus

Evidence pack: [lifecycle-concurrency track](../../../.bagakit/researcher/topics/engineering/ctxmux-wrong-case-corpus/tracks/lifecycle-concurrency.md), claim `C004`.

- `LC-001` (`d01`, `d02`): confusing the broadcast receiver cursor, daemon head, and caller's last delivered byte can skip or duplicate recoverable output after lag.
- `LC-002` (`d02`): a terminal event can make the last retained data unreachable if exit closes delivery before replay recovery. Final bytes must remain available through attachment or reattach.
- `LC-003` (`d03`): the waiter can reap a child before public state changes; signalling through a cached numeric PID risks a reused process identity. The waiter now removes signalling authority before publication, and a deterministic barrier proves stop rejects during that interval without touching an unrelated process.
- `LC-004`: retrying an unclassified child-status error every 20 ms can retain
  one busy waiter forever, while treating it as exit would fabricate reap and
  permit unsafe key reuse. The first error now transfers the handle into a
  non-collectable fail-stop owner and fails the daemon incarnation.

Tokio's historical lag and close bugs are fixed. The transferred risk is ctxmux's composition of broadcast, replay, and lifecycle state, not a claim that current Tokio loses messages.

## Fixture mapping

- Covered now: disconnect and reattach, attachment count release, invalid operations after exit, and exact retained final bytes followed by one terminal event on late attach.
- Covered now: output produced exactly between subscribe and snapshot appears
  once in replay and is suppressed once from the already-subscribed live queue.
- Covered now: output produced after detach is received but before its
  acknowledgement remains replayable, while the attachment guard settles to
  zero.
- Covered now: output recorded after child wait but before public exit is
  delivered before `Exited` and remains available to a late reattachment.
- Covered now: a seeded public multi-client model races input, resize, and two
  stops. Exactly one stop is accepted; other results are limited to the
  protocol's declared success, exited-state, and owner-I/O outcomes without
  inventing writer or resize arbitration.
- Candidate: broader stop races with hostile output, natural exit, and a
  controllable process/PTY seam under sustained load.
- Covered now: a real socket attachment is paused after snapshot, overruns a bounded live channel, observes `Gap`, and reattaches from the caller-owned cursor with contiguous byte ranges and exact raw bytes.
- Covered now: a child-wait barrier pauses public state publication after signalling authority is removed and proves concurrent stop cannot affect an unrelated process identity.
- Covered now: a fake child returns one status error followed by a tempting
  synthetic exit; ctxmux polls exactly once, sends no kill/wait/clone signal,
  retains the handle, rejects native controls as Backend unavailable, and
  fences creation plus the daemon incarnation.
- Covered now: 32 concurrent duplicate Start requests, abandoned public Start
  and Fork responses, conflicting reuse, failed spawn, and a post-publication
  cancellation barrier converge on one physical Run without making queued
  duplicates occupy worker threads. Deterministic admission fixtures prove an
  eight-launch ceiling, cancellable ninth waiter, permit recovery, and shutdown
  wakeup without counting the waiter as active. An actual-worker shutdown
  fixture proves the creation fence rejects new and pre-fence same-stripe
  unbound waiters while the bounded drain waits for the cancelled request's
  active flight guard to release.
- Covered now: real Start and Level B Fork children cross a deterministic
  post-spawn barrier before an oversized metadata record is rejected before
  `COMMIT`. The waiter-owned reap receipt plus full native-owner quiescence gate
  exact-key reuse; pending matching and conflicting retries launch nothing,
  unrelated keys progress, one later 32-way retry elects one physical leader,
  shutdown reports an unresolved fence owner without echoing its key, the
  persistence actor remains healthy, and an unrelated-process sentinel is
  untouched.
- Covered now: memory-only terminal replacement fences lookup-to-pin atomically,
  keeps copy-only status/list available while Collecting, rejects long-lived
  lookup before mutation, and removes the exact Run/key only with publication.

## Open questions

- What multi-writer and resize policy is visible to clients?
- Must metadata snapshots become atomic, or can fields declare independent freshness?
- How are exited Runs retained, pinned, exported, and collected?
- What is the daemon shutdown contract for live Run mutations?

## Repository evidence

- `crates/ctxmux-daemon/src/lib.rs`: `RunManager`, `Run`, `AttachmentGuard`, `handle_attachment`
- `crates/ctxmux-daemon/tests/native_lifecycle.rs`
- `packages/sdk/test/client-parity.test.ts`
