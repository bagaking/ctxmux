# Daemon upgrade and semantic-resume continuity — design

- Status: design accepted, implementation pending
- Date: 2026-08-22
- Scope: close the roadmap M3.5 open cell "Live PTY handoff, process
  adoption, host-reboot continuity, and upgrade continuity are open" for the
  two cases that are physically achievable without adopting a process by PID
  guesswork or moving Run ownership into a client.
- Decision records: [015 — exec-in-place upgrade continuity](../architecture/choices/015-exec-in-place-upgrade-continuity.md),
  [016 — semantic resume of interrupted Runs](../architecture/choices/016-semantic-resume.md)
- Answers the open question left by [009 — runtime persistence and recovery](../architecture/choices/009-runtime-persistence-recovery.md):
  "Does a later product milestone justify a stable per-Run owner or another
  platform mechanism for live PTY handoff? This decision does not pre-design it."

## North star

A Run is durable. Today "durable" survives client churn and (in persistent
mode) daemon restart as *historical* state. It does not survive the one event
an operator triggers most often on purpose: **upgrading the daemon binary**.
Restarting `ctxmuxd` to ship a fix turns every live shell and agent into an
`interrupted` tombstone. That is the gap this design closes.

The guiding constraint is verbatim from the user and governs every decision
below:

> 控制系统的熵, 同时保证能力的完善和代码的优雅, 避免过度设计和过度校验.

Concretely: add the *least* new machinery that makes live control survive an
intentional upgrade, reuse the fd-inheritance and reconnect primitives that
already exist, and never build a standing broker or a per-Run supervisor
process — those were evaluated and rejected (see "Rejected alternatives").

## The physics that bounds the whole design

The master PTY fd lives only in the daemon. If the daemon closes it, the child
gets `SIGHUP` and its slave side goes EOF/EIO permanently. So "the process
survived" is not the same as "live control survived" — live control *is*
possession of the master fd plus the authority to `waitid` the child.

This splits the roadmap cell into four distinct problems that must never be
conflated:

| Problem | Physically achievable? | This design |
| --- | --- | --- |
| Planned daemon upgrade / restart, keep live control | **Yes** — the same process can carry the fd across `execve` | **Track A: exec-in-place** |
| Daemon *crash*, keep live control | No — an already-dead process holds no fd to carry; would need an independent standing fd holder | Out of scope (rejected: broker) |
| Adopt a running process by PID after the fact | No — the master fd is unrecoverable and PID reuse is unsafe | Forbidden by 009, unchanged |
| Crash / host reboot, bring the *agent* back | Not the same process, but **semantically** yes | **Track B: semantic resume** |

Track A is `execve`-in-place: the daemon replaces its own binary image while
keeping its PID, its open fds, and its child processes. Track B is semantic
reconstruction into a *new* process, for the case where the live process is
genuinely gone.

### Why exec-in-place is sound (the crux, confirmed against POSIX)

`execve` replaces the process image but keeps the **same PID** and the **same
parent/child relationships** — it does not fork. A process that `execve`s
itself remains the parent of every child it had before, and retains the right
to `wait`/`waitid` for them. Pending `SIGCHLD` disposition resets to default
across exec, but reaping *authority* (being the parent) is unchanged.

This is the fact that makes Track A work and the reason it does **not** violate
009's unsupported clause. 009 forbids a replacement daemon that "opens,
attaches to, or signals a process named **only by persisted metadata**". Track A
does not name the child by persisted metadata — it carries the live master fd
across the exec and proves reap authority by *being the same parent process*.
Identity is possession, not a guessed PID. That distinction is the entire wedge.

## What survives an execve, what is rebuilt, what is lost

Deliberately enumerated, because a single missed fd silently orphans a live
child's terminal.

**Survives the exec (fd table is preserved when CLOEXEC is cleared):**

- the child process (execve does not touch children);
- the PTY master fd and the input-writer fd;
- the listener socket fd (so the endpoint and socket inode never change);
- the SQLite state-lock fd (so no concurrent opener can steal the epoch during
  the exec window — see "State lock" below).

**Rebuilt in the new image from persisted state + inherited fds:**

- the whole tokio runtime and the daemon-wide native owner thread;
- each `Run`'s metadata, lineage, capabilities, and durable replay cursor
  (from SQLite);
- each live Run's `NativeControlOwner` (wrapping the inherited master fd),
  output reader, and `NativeSession` SID anchor (`from_child_pid`);
- the child handle — as a **new `AdoptedChild`** that wraps the bare PID and
  reaps via `waitid`, because `portable_pty::Child` has no PID→Child
  constructor.

**Lost, and intentionally not migrated (over-design to preserve):**

- every per-connection tokio `Framed` protocol state, attachment cursor, and
  pending command — clients briefly disconnect and **reconnect from their last
  byte cursor**, which is already a first-class, tested path;
- recoverable-Input operation ledgers (connection-local by contract already);
- the OwnerWake self-pipe (recreated fresh after exec).

Migrating tokio connection state across an exec would be high-entropy for zero
user-visible benefit. The reconnect model is the low-entropy correct choice.

## Track A — exec-in-place upgrade

### Trigger

**SIGHUP**, the conventional daemon-reload signal. Chosen over a
`Request::Upgrade` protocol op because a wire-frame change would force a
protocol generation bump (9 → 10) and TypeScript schema regeneration for no
capability gain; "the generation fence covers the wire contract only" and a
signal-only re-exec changes no frame. The re-exec target is
`std::env::current_exe()`, and `std::os::unix::process::CommandExt::exec` is
already an in-repo idiom.

A convenience CLI verb (`ctxmux daemon upgrade`, or reuse of the connect path)
may send the signal, but the daemon contract is the signal itself.

### Persistent mode only

Exec-in-place is supported **only** with `--state-dir`. Memory-only mode has no
durable metadata to rebuild the `Run` objects from after the image is wiped, so
it fails closed: a SIGHUP in memory-only mode is a no-op (logged), never a
partial upgrade. This is a capability boundary, not a validation reflex — it
falls straight out of "the fd carries the child, SQLite carries the meaning".

### Flow

1. **Receive SIGHUP** in a new `tokio::select!` arm in the accept loop,
   parallel to the existing `ctrl_c` arm. Memory-only ⇒ log and ignore.
2. **Enter draining.** Stop accepting *new* connections. Do not kill any Run.
   Quiesce tmux control owners exactly as shutdown does (tmux panes are
   observed, not owned; their control channel does not survive and is
   re-observed after exec). In-flight client connections are allowed to end;
   they will reconnect.
3. **Extract live descriptors.** A new native-owner command (distinct from
   `Shutdown`, which fail-stops via `mem::forget`) walks each live native Run
   and yields `{ RunId, child_pid, master_fd, writer_fd, reader_fd }` without
   terminating anything. This needs a new "extract raw fds from a live, Open
   control" API on `NativeControlOwner`, because the existing
   `detach_closed_descriptors_after_owner_fence` gates on the Run being already
   dead.
4. **Clear CLOEXEC** on exactly: every extracted master/writer/reader fd, the
   listener fd, the state-lock fd, and the new handoff fd. Everything else stays
   CLOEXEC (proven by the existing fd-hygiene regression, extended to the
   re-exec'd image as a second boundary).
5. **Ensure SQLite is committed.** The four-unit commit discipline already
   guarantees each Run row is a complete unit; nothing extra to flush. The
   handed-off Runs stay `running` in the database.
6. **Write the handoff manifest** — the `{ RunId → fd numbers }` table plus the
   epoch string to reuse — to the inherited handoff fd (or argv), then
   `execve(current_exe, argv-with-handoff)`. Because execve runs no
   destructors, `SocketGuard::drop` does not unlink the socket and the owner
   drop does not fail-stop the children.
7. **New image adopts.** On startup, detecting a handoff manifest:
   - reopen SQLite, **reuse the passed epoch** instead of minting a fresh UUID
     (so `daemon_instance` is unchanged and clients are not rejected with
     `DaemonInstanceMismatch`);
   - reconcile running rows with the live-set exclusion (see crux below), so
     handed-off Runs stay `running` and any *other* running row (a real crash
     remnant) still becomes `interrupted`;
   - reconstruct the listener from the inherited fd — **skip `prepare_socket_path`
     and `bind`** (re-binding would unlink/rebind the inode, drop in-flight
     clients, and self-`AlreadyRunning`);
   - rebuild the daemon-wide owner thread (singular, never per-Run);
   - for each handed-off Run, re-adopt: wrap the master fd in a `MasterPty`
     adapter, rebuild the writer and reader, synthesize the `AdoptedChild`,
     rebuild `NativeSession::from_child_pid`, and **re-bind live control onto
     the recovered `Run`** via a new `Run::readopt` path.
8. **Clients reconnect** from their last observed byte cursor and the daemon
   replays contiguously with no gap.

### The persistence crux — distinguishing handed-off from orphaned

Startup reconciliation today turns *every* `running` row into
`interrupted { daemon_restart }`, keyed on row state, not epoch. The exec-in-place
change is surgical:

- pass the live RunId set across the exec (proven live by the fds actually
  inherited, not by any stored flag);
- reconciliation becomes `WHERE state_kind='running' AND id NOT IN (:live_set)`;
- the three post-normalization "running == 0" asserts relax to
  "running == |live_set|";
- excluded rows are diverted to the re-adopt path **before** the historical
  recovery loader, and their durable cursor is re-attached so `append`/`finalize`
  continue from the true committed byte (never 0, which would trip the
  gap-rejection).

Crash-safety is preserved by construction: no exec ⇒ no manifest ⇒ empty
live-set ⇒ today's behavior exactly. The discriminator is un-forgeable because
it is "did a usable fd for this RunId arrive on this exec", verified against the
passed set; any RunId in the set without a usable fd falls back to normal
reconciliation.

**Epoch is preserved, not minted**, on an exec-in-place — this is the refinement
009's "fresh UUID per daemon" needs: fresh per *cold* start, preserved across
intentional exec, because the epoch *is* the live-control owner identity and
exec-in-place is by definition the same owner.

### State lock across the exec

009 holds an exclusive flock on a companion file for the daemon lifetime, and
that fd is CLOEXEC so Run children cannot extend state ownership. For our own
execve we **inherit** the lock fd (clear CLOEXEC just for it, re-assert the flag
after adoption) rather than release-and-reacquire. Releasing would open a window
in which a concurrent second opener acquires the lock, mints its own epoch, and
reconciles our *still-live* Runs to `interrupted` — the exact catastrophe. SQLite
connections themselves are reopened fresh in the new image; only the lock fd is
carried, to keep ownership continuous.

## Track B — semantic resume of interrupted Runs

For the case the live process is genuinely gone (crash, host reboot), Track A
cannot apply. The daemon primitives to reconstruct semantically already exist;
what is missing is the verb and the durable provenance.

**Gap:** a recovered `interrupted` Run supports `list`/`status`/`attach`/Level A
fork, but there is no `resume`; Level B (provider resume, e.g. Codex
`exec resume`) is blocked on recovered Runs because (1) it requires live
continuation authority and (2) the session provenance lived only in a live
in-memory `WeakMap`, empty after restart.

**Minimal completion (no new subsystems):**

- one explicit operator-driven `ctxmux resume <run-id>` verb, mirroring the
  existing manual `fork`; no auto-respawn, no daemon-initiated restart;
- **Level A path**: clone the recovered `RunSpec` and start fresh — already
  fully supported (Level A fork of a recovered parent needs no authority);
  honest "process back, no conversation continuity";
- **Level B path**: re-derive the session provenance by running the recovered
  Run's *durable replay* through the integration observer (the Codex observer
  already parses `thread.started`/sessionId from output JSONL), then model
  resume as a plain **Start of the materialized resume spec** with lineage
  recorded back to the interrupted Run — sidestepping the live-authority gate
  honestly, because a materialized spec + durable provenance is exactly the
  declared, inspectable Level B contract.

Explicitly out of scope for B, and unchanged from 009: live PTY handoff, fd
transfer, PID adoption/re-signalling of the old child.

## Rejected alternatives

- **Standing per-Run broker / shim** (a tmux-server-like process that always
  holds the fd so control survives a *crash*). Rejected: adds one process and
  supervision boundary per Run, reverses the peer-performance work that merged
  per-Run threads into a daemon-wide owner, and violates the frozen per-Run
  budget slopes (0.25 fd/thread headroom). High entropy for the crash case,
  which is not what operators trigger. herdr — the comparable tool — also
  deliberately avoids a broker.
- **spawn a successor + SCM_RIGHTS fd passing** (two coexisting processes, pass
  fds over a unix socket). Rejected in favor of exec-in-place: ctxmux keeps its
  replay in SQLite, not in heavy in-process terminal-emulator state, so `execve`
  is cheap and eliminates the entire coexistence-window complexity that forces
  SCM_RIGHTS. SCM_RIGHTS is purely local fd-passing; its cost is process
  coexistence, which exec-in-place does not have.
- **Migrating tokio connection state across the exec.** Rejected: high entropy,
  zero benefit over reconnect-from-cursor, which already exists and is tested.
- **`Request::Upgrade` protocol op as the trigger.** Rejected: forces a protocol
  generation bump and TS regen; SIGHUP is zero wire entropy.

## Budget and safety invariants the implementation must honor

- **Net permanent per-Run fd/thread delta = 0.** Re-adopt the same master /
  writer / reader fds; never dup a fresh permanent copy; keep the owner thread
  daemon-wide and singular. The frozen budget headroom is 0.25 per Run.
- **Cleanup envelope returns to baseline.** Every transient dup / manifest
  buffer released; no orphaned child, attachment, or thread after upgrade.
- **Replay retention unchanged.** The upgrade touches ownership, not the 4 MiB
  per-Run replay tail or the global budgets.
- **Fd hygiene second boundary.** The re-exec'd image must not inherit any fd
  beyond the explicit clear-list; guarded by the analogue of the existing
  ambient-descriptor regression.
- **Failed exec falls back to fail-stop.** If `execve` returns an error, the
  path must degrade to exactly today's shutdown fail-stop semantics (record
  incarnation failure; do not leak un-waited children whose control was
  dropped, which `Child::drop` would falsely treat as reap proof).

## Test strategy

Real re-exec of `CARGO_BIN_EXE_ctxmuxd` (not a shim), in the `native_lifecycle.rs`
style:

1. start daemon (persistent), start an interactive Run, drive I/O to cursor `C0`;
2. open a live attach, read to `C0`;
3. send SIGHUP, wait on the readiness-fd receipt for "resumed N";
4. assert `status(id).pid == pid_before` and the PID is still alive — the same
   child survived **as a live Run** (the sharp contrast with Track B, where it
   would be `interrupted`);
5. `input(id, ...)` and observe the echo past `C0` — master + writer fds
   re-adopted;
6. new `attach(id, C0)` shows contiguous output, **no `RunEvent::Gap`**;
7. tail reliability smoke confirms per-Run fd/thread census unchanged.

A separate memory-only test asserts SIGHUP is a logged no-op (fail-closed).

Track B tests extend the existing restart-recovery harness: after a real
kill+restart the Run is `interrupted`; `resume` produces a new Run with lineage
to it, Level B path re-derives provenance from durable replay.

## Documentation deltas

- `docs/architecture.md`: flip the "upgrade continuity are open" cell to
  supported-in-persistent-mode; add the exec-in-place path to the lifecycle
  narrative; note the fd-possession vs PID-guess distinction.
- `docs/protocol.md`: persistent-mode lifetime boundary gains the upgrade-continuity
  clause; note no generation change (SIGHUP-triggered).
- `docs/roadmap.md`: M3.5 status — upgrade continuity and semantic resume land;
  crash-time live handoff and PID adoption remain unsupported.
- `docs/architecture/choices/009-*.md`: answer the open question, cross-link 015.
- `README.md`: the "Live PTY control is not adopted after restart" note gains
  the upgrade exception.
