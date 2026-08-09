# Public Local Consumer Contract Review

- Status: approved
- Feature: `f-22dczvf38`
- Scope: public native Run behavior needed by one exact-commit local embedding consumer

## Decision

This Feature closes three finite ctxmux-owned gaps in owner order:

1. replace output chunk ordinals with cumulative byte cursors across protocol, daemon, CLI and TypeScript SDK;
2. expose public interrupt/signal and complete process-tree Stop with real descendant cleanup evidence;
3. produce a reproducible exact-commit SDK/binary consumption artifact and qualify required CI without publishing.

These capabilities are public Run infrastructure. They do not mention Agent identities, Provider state, Desktop Views or an embedding application's Adapter. The embedding consumer must use only the resulting public package and binary boundary.

## Protected invariants

- Output cursors are cumulative byte positions. Replay, live output, truncation and Gap use one absolute byte space; chunk ordinals and compatibility aliases are deleted.
- The daemon remains the only process owner. Interrupt and Stop never move signal or descendant ownership into a client.
- Stop covers the complete process tree or fails explicitly; direct-child exit alone is insufficient proof.
- Exact-commit consumption is reproducible from a clean checkout and immutable Git identity. It does not require npm, crate, GitHub Release or global installation, and it never depends on an adjacent source directory or unrecorded local build.
- CLI and TypeScript SDK remain clients of the same versioned public protocol. No copied wire or privileged first-party path is introduced.
- No Agent semantics, AgentMux Adapter, SSH transport, plugin system, orchestration policy, compatibility layer, migration or fallback enters this Feature.

## Completion boundary

The Feature closes only when all three tasks have public behavior tests, `scripts/check.sh` passes from the final clean commit, required hosted CI is green or any source-owned failure is fixed, and the exact commit/artifact identity is recorded. A type-only field rename, direct-child-only Stop, mock-only signal test, path dependency or unpublished local directory is insufficient.

## Evidence sources

- `AGENTS.md`
- `docs/architecture.md`
- `docs/protocol.md`
- `docs/roadmap.md`
- current protocol, daemon, CLI and TypeScript SDK source and tests
