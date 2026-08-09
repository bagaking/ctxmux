# 001 — Rust and Tokio long-lived daemon

- Status: accepted
- Scope: runtime ownership and local concurrency host

## Context

A Run must survive the client that started or viewed it. An in-process library cannot provide that guarantee after its host exits, regardless of implementation language.

## Decision

One Rust daemon owns every live native Run. Tokio owns the Unix listener, connection tasks, signals, and bounded broadcast delivery. Blocking PTY reads and child waits run on named operating-system threads because the selected PTY interfaces are blocking.

The protocol is the stable client boundary. Rust ABI, N-API, and editor-process lifetime are not product boundaries.

## Quality attributes and invariants

- Client disconnect cannot drop daemon-owned Run state.
- The daemon remains Agent-neutral and has no JavaScript runtime.
- Async connection work does not perform blocking PTY reads on Tokio workers.
- Unsafe Rust is forbidden at the workspace lint boundary.

## Alternatives

- A Rust library embedded by each client fails the independent-lifetime requirement.
- A Node runtime would keep the core tied to Node process lifecycle and native PTY addons.
- A Rust N-API core adds two runtime and distribution surfaces without replacing the need for a daemon.
- Go could host a daemon, but it would not remove the protocol, PTY, or lifetime problems and offers no current project-native advantage.

## Known constraints

Daemon shutdown is abrupt from the Run model's perspective: there is no graceful Run policy, persistent state, restart handoff, resource quota, or panic isolation contract. One reader thread and one waiter thread are created per native Run.

## Wrong-case corpus

Evidence pack: [daemon-runtime track](../../../.bagakit/researcher/topics/engineering/ctxmux-wrong-case-corpus/tracks/daemon-runtime.md), claim `C001`.

- `DR-001` (`a01`, `a03`): a post-spawn setup failure can return before the child is terminated or reaped. A rejected start must leave no live child, zombie, or published Run id.
- `DR-002` (`a02`, `a03`): blocking PTY work inside an async connection task can make unrelated requests or shutdown unbounded. A deterministic blocked-operation fixture must prove isolation before this becomes a guarantee.
- `DR-003` (`a01`): attachment lifetime must not become child lifetime. The existing same-id and same-PID reconnect test is the permanent regression.

The Tokio pool regression and Rust child-drop contract constrain ownership and blocking boundaries. They do not prove that dedicated per-Run threads are universally superior or supply a safe global thread quota.

## Fixture mapping

- Active: rejected post-spawn reader, writer, output-thread, and waiter-thread setup transitions terminate and reap the child before returning an error in `lib.rs`.
- Covered now: client disconnect and reconnect preserve the same child PID in `native_lifecycle.rs` and `client-parity.test.ts`.
- Candidate: daemon signal, crash, and orphan behavior.
- Candidate: many-Run thread and memory pressure.

## Open questions

- Which shutdown signals get graceful behavior, and what is the deadline?
- Are live children terminated, adopted, or deliberately orphaned when the daemon exits?
- Which per-Run and daemon-wide quotas are public capabilities?
- What platforms must the daemon support before the protocol is stable?

## Repository evidence

- `crates/ctxmux-daemon/src/lib.rs`: `serve`, `RunManager`, `Run::spawn`
- `Cargo.toml`: Tokio features, `unsafe_code = "forbid"`
- `crates/ctxmux-daemon/tests/native_lifecycle.rs`
- `packages/sdk/test/client-parity.test.ts`
