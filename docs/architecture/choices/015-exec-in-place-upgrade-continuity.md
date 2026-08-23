# 015 — Exec-in-place upgrade continuity

- Status: accepted and implemented for persistent mode
- Scope: keeping live PTY control across an intentional daemon upgrade or
  restart, in persistent mode
- Supersedes: the two-process transactional direction of Feature **f-228**
  (`.bagakit/.../f-228cz55vj`), after the Herdr comparison
  (`f-226.../artifacts/herdr-transfer-review.md`) showed ctxmux's
  SQLite-backed replay does not need Herdr's process-coexistence fd transfer.
  See Alternatives for the accepted cost (no mid-transaction rollback).

## Context

[009](009-runtime-persistence-recovery.md) makes Runs independent of clients and
recovers _historical_ state across daemon restart, but explicitly leaves live
PTY control unrecoverable: a prior `running` row becomes
`interrupted { daemon_restart }`. Its open question asked whether a later
milestone justifies "a stable per-Run owner or another platform mechanism for
live PTY handoff." This record answers it.

The operator action that most often destroys live control is not a crash — it
is upgrading the daemon binary to ship a fix. Restarting `ctxmuxd` turns every
live shell and agent into a tombstone. 009 left the door open with the invariant
"Recovery claims must be proven across real daemon restart **or upgrade**, not
inferred from stored rows."

The master PTY fd lives only in the daemon; if it closes, the child receives
`SIGHUP` and its slave side goes EOF permanently. So live control is possession
of the master fd plus authority to `waitid` the child. Any upgrade that keeps
live control must carry the fd, not re-derive it.

## Decision

The daemon upgrades itself with **`execve` in place**: on `SIGHUP` it replaces
its own binary image while keeping its PID, its open fds, and its child
processes, then re-adopts each live Run in the new image. This is supported
**only in persistent mode** (`--state-dir`); a `SIGHUP` in memory-only mode is a
logged no-op, because without durable metadata the new image cannot rebuild the
`Run` objects. It is not a validation reflex but a capability boundary: the fd
carries the child, SQLite carries the meaning, and memory-only has no SQLite.

`execve` keeps the same PID and the same parent/child relationships — it is not
a fork. The post-exec image remains the parent of every pre-exec child and
retains the right to `waitid` them; only pending `SIGCHLD` disposition resets.
This is why exec-in-place does **not** contradict 009's rule that a replacement
daemon never "opens, attaches to, or signals a process named only by persisted
metadata": the child is named by a **live master fd carried across the exec**
and reap authority is proven by _being the same parent process_, not by a
guessed PID. Identity is possession.

### What crosses the exec

Because `execve` preserves the fd table for descriptors whose close-on-exec flag
is cleared, and runs no destructors, the following survive: the child process,
one PTY master fd per live Run, the listener socket fd, the SQLite state-lock fd,
and one manifest fd. The incoming image duplicates the bidirectional master for
its reader/writer views. The socket inode is never unlinked
(`SocketGuard::drop` does not run) and the children are never fail-stopped (the
owner drop does not run).

Everything else is rebuilt in the new image from persisted state plus the
inherited fds: the tokio runtime, the single daemon-wide native owner thread,
each Run's metadata / lineage / capabilities / durable replay cursor, each live
Run's control owner (wrapping the inherited master fd), output reader, complete
settled recoverable-Input ledger/cursor/poisoned-lane state, and
`NativeSession` SID anchor. The `portable_pty::Child` handle has no PID→Child
constructor, so it is replaced by an **adopted-child handle** that wraps the
bare persisted PID and reaps through `waitid`, preserving the existing
non-reaping-probe-then-reap ordering and the reaped-leader latch that guards
against PID reuse.

Per-connection tokio protocol state and attachment cursors are **not** migrated.
Before extraction, a daemon-wide request gate drains every admitted mutation
through owner completion and response write. Commands arriving on an existing
attachment during that drain receive an explicit retryable `not_applied`
result. Clients then disconnect and reconnect from their last observed byte
cursor. The daemon instance identity is preserved across the exec, so retained
recoverable-Input operations remain addressable after reconnect.

### Handoff and reconciliation

The upgrade carries a version-2 manifest containing the epoch and, for every
live Run, `{ RunId, child PID, master fd, complete settled input state }`. It is
written to an owner-only regular file created inside the state directory and
immediately unlinked, avoiding pipe-capacity deadlock for bounded ledgers while
leaving no pathname. Close-on-exec is cleared on exactly the listener, state
lock, live masters, and manifest file; every other descriptor remains
close-on-exec.

Before changing admission, all fallible file/executable setup is completed.
The request gate then transitions `Open -> Draining`, waits for its permit count
to reach zero, and seals. The native owner preflights every entry before
relinquishing the first: lifecycle must be `Watching`, the master and PID must
exist, and no input, signal, Stop, child command, or pending recoverable
operation may cross the snapshot. Timeout or preflight failure drops the fence
and restores full service. Extraction is the point of no return; later barrier,
serialization, CLOEXEC, or `execve` failure is fail-stop.

Startup reconciliation, which today turns every `running` row into
`interrupted { daemon_restart }`, gains one exclusion: rows whose `RunId` is in
the inherited live set stay `running` and are re-adopted from their inherited
fds; all other `running` rows (a real crash remnant) still become `interrupted`
as before. The discriminator is possession of a validated, unique live
descriptor set, not a stored flag. A malformed schema, duplicate descriptor or
Run, unusable fd, invalid child PID, or inconsistent input ledger fails startup
closed; it does not partially adopt or silently downgrade one Run to historical
reconciliation. With no manifest — the cold/crash path — the live set is empty
and behavior is identical to 009.

The UUID epoch is **preserved** across an exec-in-place, not minted fresh. The
epoch is the persistent-mode daemon-instance identity used to fence client
control; minting a new one would reject every reconnecting client's in-flight
control with a daemon-instance mismatch even though the daemon deliberately
continues to own the PTY. 009's "fresh UUID per daemon" is refined to "fresh per
cold start, preserved across intentional exec-in-place", because exec-in-place is
by definition the same live-control owner.

### State lock continuity

009 holds an exclusive advisory lock on a companion file for the daemon lifetime
so a second opener cannot allocate an epoch or reconcile the first daemon's live
Runs. For our own `execve` the lock fd is **inherited** (close-on-exec cleared
for it alone, re-asserted after adoption), not released and reacquired.
Releasing would open a window in which a concurrent opener acquires the lock,
mints an epoch, and reconciles our still-live Runs to `interrupted`. SQLite
connections are reopened fresh in the new image; only the lock fd is carried, to
keep ownership continuous across the exec.

### Trigger

`SIGHUP`, the conventional daemon-reload signal, re-execing
`std::env::current_exe()`. A wire-frame trigger was rejected because it would
force a protocol generation bump and TypeScript regeneration for no capability
gain; the generation fence covers the wire contract only, and a signal-only
re-exec changes no frame.

## Quality attributes and invariants

- The same child process, PTY, and daemon PID survive an upgrade; live input,
  resize, interrupt, and stop continue to work afterward.
- Live control after upgrade is proven by a real re-exec of the daemon binary,
  never inferred from stored rows.
- A real crash (no manifest) still yields the 009 behavior exactly: every
  `running` row reconciles to `interrupted`; no process is adopted by PID.
- Net permanent per-Run descriptor and thread deltas are zero across an upgrade;
  the native owner remains daemon-wide and singular.
- A failed `execve` degrades to today's shutdown fail-stop semantics and records
  an incarnation failure; it never leaves an un-waited child whose control was
  dropped in a way that `Child::drop` could mistake for reap proof.
- The listener socket inode is never unlinked or re-bound during an upgrade; the
  published endpoint is byte-identical before and after.
- Memory-only mode makes no upgrade-continuity claim and treats `SIGHUP` as a
  no-op.
- Every pre-extract failure is reversible: the current image keeps all Run,
  child, PTY, creation, and tmux ownership and returns to full service. Every
  post-extract failure is fail-stop; no partial owner resumes.
- A preserved daemon instance requires preservation of the complete settled
  recoverable-Input ledger and cursor. A response-loss retry after upgrade
  returns the original range without another physical write.

## Alternatives

- **Spawn a successor process and pass fds with `SCM_RIGHTS`.** This is the
  two-process transactional direction proposed in Feature **f-228** ("preserve
  live Runs across controlled daemon replacement"), modeled on Herdr's live
  handoff (`tests/live_handoff.rs` spawns a replacement server and waits for the
  new pid while the old one still owns the pty). Rejected in favor of
  exec-in-place, and this record **supersedes that direction**: Herdr _must_ use
  two processes because its pty actor holds heavy in-process terminal state
  (`src/pty/actor/unix.rs`: grids, terminal responses, render state) that cannot
  be rebuilt from a store, so its fds must cross a process boundary while both
  live. ctxmux keeps replay in SQLite, not in heavy in-process terminal state,
  so `execve` is cheap and removes the process-coexistence window that is the
  entire reason `SCM_RIGHTS` exists.
  - **Accepted cost.** f-228's acceptance boundary required "failure before
    commit restores the old owner" — a rollback to a still-live quiesced
    predecessor. Exec-in-place **cannot offer that**, and does not try to: once
    `execve` fires the old image is gone, so there is no predecessor to roll
    back to. Its failure model is instead a _pre-exec_ guarantee plus atomicity:
    any failure **before** `execve` leaves the current daemon fully owning every
    fd and simply continuing to serve; `execve` itself is atomic; an `execve`
    that returns (only on error) degrades to today's shutdown fail-stop. We
    accept losing f-228's mid-transaction rollback in exchange for no second
    process, no `SCM_RIGHTS`, and no dual-owner window — the properties that made
    the two-process transaction expensive. f-228's other invariants
    (exactly-one-owner, unchanged Run/PID/PTY identity, ordered output
    continuity, explicit disposition for crossing controls) are preserved by
    the implemented request drain and all-owner extraction boundary.
- **A standing per-Run owner or shim** that always holds the fd so control also
  survives a _crash_. Rejected: one extra process and supervision boundary per
  Run, reversing the daemon-wide-owner performance work and breaking the frozen
  per-Run budget slopes, for the crash case operators do not deliberately
  trigger.
- **Migrate tokio connection state across the exec.** Rejected: high complexity
  for no benefit over reconnect-from-cursor, which already exists and is tested.
- **A `Request::Upgrade` protocol operation as the trigger.** Rejected: forces a
  protocol generation bump; `SIGHUP` is zero wire entropy.

## Known constraints

Exec-in-place covers planned upgrade and restart only. It cannot recover live
control after a crash, because a dead process holds no fd to carry, and it never
adopts a process by PID. Host-reboot and provider-semantic continuation belong
to a higher client; [016](016-interrupted-run-derivation.md) records how ctxmux
executes an explicit generic derivation plan without owning Provider policy.

The re-adopted child is observed through `waitid` on its bare PID; the reaped
latch must start unset in the reconstructed session so PID reuse cannot
re-authorize the census. portable-pty exposes no `MasterPty` constructor from a
raw fd, so the master fd is wrapped in a local adapter that implements resize,
size query, and raw-fd access; the writer is either recovered as its own fd or
written through a fresh handle over the bidirectional master fd.

Recoverable-Input operation history is Run-local and bounded, not
connection-local. The complete settled ledger and applied cursor cross in the
manifest because the preserved daemon instance keeps same-incarnation retry
valid. Pending operations cannot be serialized: the request gate and native
owner preflight must drain them first or abort before extraction.

## Wrong-case corpus

- An incomplete manifest, duplicate fd/Run/input key, zero child PID, stale
  schema, or inconsistent input range must fail the incoming image before any
  partial exposure.
- A blocked crossing mutation must keep extraction fenced until its unique
  response is written; a later old-attachment command is explicitly
  `not_applied` or the connection is closed.
- A failure before extraction must reopen full service with the original child
  and PTY; a failure after extraction must fail-stop.
- Reader activity in the barrier-to-exec window can consume output that neither
  image publishes; extraction must stop the old reader before the durable
  barrier.
- Signal termination after re-adoption must remain a public signal identity,
  not synthetic exit code `128 + signal`.
- Repeated upgrades must not accumulate an fd, thread, second native owner, or
  handoff pathname.

## Fixture mapping

- Covered: a real re-exec of `CARGO_BIN_EXE_ctxmuxd` proves the same child PID
  survives as a live-controllable Run, input echoes past the pre-upgrade cursor,
  a fresh attach from that cursor shows contiguous output with no gap, and the
  per-Run descriptor and thread census is unchanged.
- Covered: a response-loss recoverable Input crosses a real exec and returns its
  original range without a duplicate child-visible write.
- Covered: a blocked real PTY Input crosses `SIGHUP`; a same-attachment command
  observes the drain retry result, the Input ACK precedes resume, and the
  incoming cursor advances exactly once.
- Covered: handoff setup failure before extraction restores status, existing
  attachment control, creation, and Stop on the original child.
- Covered: repeated real upgrades preserve daemon/child PID and listener inode,
  keep settled fd/thread counts equal, and leave no handoff pathname.
- Covered: a `SIGHUP` in memory-only mode is a logged no-op.
- Future: a `running` row not in the live set still reconciles to `interrupted`
  during an upgrade (mixed handed-off and orphaned rows).
- Covered: the second fd-hygiene boundary — an ambient non-CLOEXEC descriptor is
  absent in the re-exec'd image.

## Open questions

- Should an upgrade optionally health-check the new binary before committing the
  handoff, or is fail-stop-on-exec-error sufficient?
- Does a future non-POSIX backend need an equivalent handoff mechanism, or is
  this POSIX-only by construction?

## Repository evidence

- `crates/ctxmux-inherited-fd/src/lib.rs`: the audited raw-fd seam; gains a
  close-on-exec-clearing primitive mirroring `duplicate_cloexec`.
- `crates/ctxmux-daemon/src/lib.rs`: accept loop and shutdown arm, socket bind
  and guard, `RunManager` construction, recovered-Run construction, and the new
  adopt-vs-bind branch and re-adopt path.
- `crates/ctxmux-daemon/src/native_runtime.rs`: the daemon-wide owner, its
  shutdown fail-stop path, and the new live-descriptor extraction command and
  owner rebuild.
- `crates/ctxmux-daemon/src/native_control.rs`: control owner, the close-on-drop
  detach capsule, and the new live-fd extraction API.
- `crates/ctxmux-daemon/src/native_session.rs`: `from_child_pid`, the
  non-reaping probe, the reap, and the reaped-leader latch.
- `crates/ctxmux-daemon/src/persistence.rs`: startup normalization, the
  reconciliation query, epoch allocation, and the live-set exclusion.
- `crates/ctxmux-daemon/src/main.rs`: inherited-fd parsing, extended with the
  handoff manifest and reused epoch.
