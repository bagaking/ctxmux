# 001 — Rust and Tokio long-lived daemon

- Status: accepted
- Scope: runtime ownership and local concurrency host

[Decision 019](019-resource-policy-and-honest-qualification.md) owns current
configurable admission-worker and resource policy and supersedes the fixed population
policy historically described here. [Decision 015](015-exec-in-place-upgrade-continuity.md)
owns persistent planned-exec continuity. Neither amendment turns an earlier
resource census into qualification of a different source or workload.

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
jobs FIFO to the configured transient cleanup-worker limit (default eight),
which returns the reap or
fail-stop result before terminal publication. Completion releases cleanup
admission; terminal publication then uses its own bounded finalizer budget,
independent of cleanup admission. Public Stop waits for its own terminal
publication, with an explicit unknown result if the bounded visibility grace
expires; that wait consumes no cleanup slot. Unique Run creation separately
uses its configured admitted short-lived worker limit (default eight). Neither bound grows with
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
native Run policy, global attachment quota, total RSS quota, or general panic
isolation contract. Persistent planned exec preserves live authority only under
Decision 015's compatible preflight and inherited-owner contract; cold restart
recovers historical state and does not reconstruct live PTY authority. Registry
admission follows Decision 019's metadata and host funding plus optional
operator quotas, with ownership-safe exact replacement. There is no default
128-record ceiling. One daemon-wide
owner thread is part of the fresh-daemon fixed census, so adding ordinary live
Runs does not change the thread count; blocking cleanup can temporarily add at
most the configured cleanup workers plus a separate finalizer budget. A
stalled cleanup can retain one cleanup slot, while durable finalization cannot
consume another cleanup slot.
Creation admission independently funds concurrent launches under its configured
worker policy (the current creation and cleanup defaults are eight),
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
- Historical baseline: frozen 1/32/128 idle and active resource censuses measure
  per-Run CPU, RSS, thread, and descriptor slopes for their bound source and
  workloads. These sizes do not define product capacity. Current ordinary
  native Runs add no permanent threads; creation and cleanup have independent
  configurable admission owners. Full resource qualification remains separate.
- Current policy: memory-only and persistent Registry admission share funded
  metadata and optional operator record quotas, with ownership-safe exact
  replacement. Historical 128-record assertions are not the current default;
  Decision 019 retains their original evidence and revised acceptance scope.

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
