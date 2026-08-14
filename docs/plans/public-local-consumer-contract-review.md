# Public Local Consumer Contract Review

- Status: approved
- Feature: `f-22dczvf38`
- Scope: public native Run behavior needed by one exact-commit local embedding consumer
- Current plan revision: 3

## Decision

This Feature closes four finite ctxmux-owned gaps in owner order:

1. replace output chunk ordinals with cumulative byte cursors across protocol, daemon, CLI and TypeScript SDK;
2. expose public interrupt/signal and a practical POSIX complete-session Stop with real descendant cleanup evidence;
3. produce a reproducible exact-commit SDK/binary consumption artifact and qualify required CI without publishing.
4. let an embedding parent prove that the daemon answering the public socket is the exact child it just started.

These capabilities are public Run infrastructure. They do not mention Agent identities, Provider state, Desktop Views or an embedding application's Adapter. The embedding consumer must use only the resulting public package and binary boundary.

## Protected invariants

- Output cursors are cumulative byte positions. Replay, live output, truncation and Gap use one absolute byte space; chunk ordinals and compatibility aliases are deleted.
- The daemon remains the only process owner. Interrupt and Stop never move signal or descendant ownership into a client.
- On macOS, Interrupt uses the retained PTY master so the kernel selects the current foreground process group. It does not trust a client PID or a separately revalidated numeric PGID.
- Stop covers every process still visible in the `portable-pty`-created POSIX session or fails explicitly; direct-child exit alone is insufficient proof. The supported boundary is local, same-user and non-elevated. A descendant that creates another session deliberately leaves this scope.
- The waitable session leader anchors the SID. Before signalling any ordinary member, the daemon immediately revalidates session membership and performs no waits, locks or unrelated I/O between that check and the signal. Observation and permission uncertainty fail closed.
- POSIX exposes only numeric PID/PGID signalling for arbitrary descendants. A process can exit and its numeric identity can be reused between membership validation and signal delivery, so complete-session Stop has a small residual wrong-process TOCTOU that supported macOS APIs cannot eliminate. The contract and tests must not claim zero risk.
- Exact-commit consumption is reproducible from a clean checkout and immutable Git identity. It does not require npm, crate, GitHub Release or global installation, and it never depends on an adjacent source directory or unrecorded local build.
- Optional daemon bootstrap readiness travels only over a caller-owned inherited descriptor. The daemon writes its public `daemon_instance` only after the Unix listener and permissions are ready; failure to validate or write the requested descriptor fails startup. A filesystem receipt, PID guess, sleep, socket-path response, or matching protocol version cannot substitute for child provenance.
- CLI and TypeScript SDK remain clients of the same versioned public protocol. No copied wire or privileged first-party path is introduced.
- No Agent semantics, AgentMux Adapter, SSH transport, plugin system, orchestration policy, compatibility layer, migration or fallback enters this Feature.

## Completion boundary

The Feature closes only when all four current-plan tasks have public behavior tests, `scripts/check.sh` passes from the final clean commit, required hosted CI is green or any source-owned failure is fixed, and the exact commit/artifact identity is recorded. A type-only field rename, direct-child-only Stop, mock-only signal test, zero-risk POSIX signalling claim, path dependency, unpublished local directory, or socket-only bootstrap probe is insufficient.

## Evidence sources

- `AGENTS.md`
- `docs/architecture.md`
- `docs/protocol.md`
- `docs/roadmap.md`
- current protocol, daemon, CLI and TypeScript SDK source and tests
