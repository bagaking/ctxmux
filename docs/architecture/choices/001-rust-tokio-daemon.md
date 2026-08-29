# 001 — Rust and Tokio long-lived daemon

- Status: accepted
- Scope: runtime ownership and local concurrency host

## Context

A Run must survive the client that started or viewed it. An in-process library cannot provide that guarantee after its host exits, regardless of implementation language.

## Decision

One Rust daemon owns every live native Run. The production daemon uses an
explicit two-worker Tokio runtime for the Unix listener, connection tasks,
signals, bounded broadcast delivery, and cancellable launch admission.

Blocking native ownership stays outside those workers without allocating a
reader and waiter thread for every Run. One daemon-wide native
owner polls all blocking PTY reader descriptors for readiness, performs one
bounded read for each ready Run, observes direct-child status without reaping,
and owns the per-Run child command receivers. A ready descriptor remains
blocking: the duplicate shares the PTY master open-file description with the
writer, so setting `O_NONBLOCK` on it would also change writer semantics.
Readiness therefore precedes every read under one unique owner.

Stop and direct-exit descendant cleanup may block. The native owner hands those
jobs FIFO to at most eight transient cleanup threads, which return the reap or
fail-stop result before terminal publication. Unique Run creation separately
uses a maximum of eight admitted short-lived threads. Neither bound grows with
the number of ordinary live Runs.

The protocol is the stable client boundary. Rust ABI, N-API, and editor-process lifetime are not product boundaries.

## Quality attributes and invariants

- Client disconnect cannot drop daemon-owned Run state.
- The daemon remains Agent-neutral and has no JavaScript runtime.
- Async connection work does not perform blocking PTY reads on Tokio workers.
- An ordinary live native Run adds no permanent operating-system thread.
- PTY EOF or the existing one-second bounded drain precedes terminal-state
  publication, so retained output remains ordered before the terminal event.
- Unsafe Rust is forbidden at the workspace lint boundary.

## Alternatives

- A Rust library embedded by each client fails the independent-lifetime requirement.
- A Node runtime would keep the core tied to Node process lifecycle and native PTY addons.
- A Rust N-API core adds two runtime and distribution surfaces without replacing the need for a daemon.
- Go could host a daemon, but it would not remove the protocol, PTY, or lifetime problems and offers no current project-native advantage.

## Known constraints

Daemon shutdown remains abrupt for live native children: there is no graceful
native Run policy, live restart handoff, separate active-Run quota, global
attachment quota, total RSS quota, or panic isolation contract. The shared
Registry does enforce a 128-record retained/projected Run ceiling with
ownership-safe exact replacement. Optional persistence recovers declared
historical metadata and replay, but not live PTY authority. One daemon-wide
owner thread is part of the fresh-daemon fixed census, so adding ordinary live
Runs does not change the thread count; blocking cleanup can temporarily add at
most eight bounded workers. A stalled cleanup can retain one of those slots.
Creation admission independently limits concurrent launches to eight,
while its bounded shutdown drain cannot hard-cancel a launch thread that
exceeds the deadline. Native-owner shutdown is itself bounded: the owner loop
wakes, detaches already-started blocking cleanup workers, and then quiesces;
the shutdown wrapper joins the loop only if it reaches that point before its
deadline. Queued or still-watched children whose wait authority cannot be
completed are retained fail-stop, while a detached cleanup worker may finish
its own child cleanup without extending daemon shutdown. This does not invent
a graceful live-native-Run shutdown policy.

## Wrong-case corpus

- `DR-001` (`a01`, `a03`): a post-spawn setup failure can return before the child is terminated or reaped. A rejected start must leave no live child, zombie, or published Run id.
- `DR-002` (`a02`, `a03`): blocking PTY work inside an async connection task can make unrelated requests or shutdown unbounded. A deterministic blocked-operation fixture must prove isolation before this becomes a guarantee.
- `DR-003` (`a01`): attachment lifetime must not become child lifetime. The existing same-id and same-PID reconnect test is the permanent regression.

The Tokio pool regression and Rust child-drop contract constrain ownership and
blocking boundaries. They do not by themselves prove native-owner throughput,
panic isolation, or a general daemon resource quota.

## Fixture mapping

- Active: rejected post-spawn reader, writer, output-owner, and wait-owner
  registration transitions terminate and reap the child before returning an
  error in `lib.rs`.
- Covered now: client disconnect and reconnect preserve the same child PID in `native_lifecycle.rs` and `client-parity.test.ts`.
- Candidate: daemon signal, crash, and orphan behavior.
- Covered now: frozen 1/32/128 idle and active resource censuses measure
  per-Run CPU, RSS, thread, and descriptor slopes; ordinary native Runs add
  zero permanent threads, while creation and cleanup admission are each
  independently capped at eight.
- Covered now: memory-only and persistent Registry admission enforce the shared
  128-record retained/projected ceiling and ownership-safe exact replacement.
  Sustained pressure and full resource-plateau qualification remain separate.

## Open questions

- Which shutdown signals get graceful behavior, and what is the deadline?
- Are live children terminated, adopted, or deliberately orphaned when the daemon exits?
- Which per-Run and daemon-wide quotas are public capabilities?
- What platforms must the daemon support before the protocol is stable?

## Repository evidence

- `crates/ctxmux-daemon/src/main.rs`: explicit two-worker production runtime
- `crates/ctxmux-daemon/src/lib.rs`: `serve`, `RunManager`, `Run::spawn`
- `crates/ctxmux-daemon/src/native_runtime.rs`: daemon-wide native owner and
  bounded cleanup handoff
- `Cargo.toml`: product crates, including the daemon, inherit
  `unsafe_code = "forbid"`; the private `ctxmux-sqlite-status` FFI leaf is the
  audited exception required by Decision 013 and exposes no raw handle
- `crates/ctxmux-daemon/tests/native_lifecycle.rs`
- `packages/sdk/test/client-parity.test.ts`
