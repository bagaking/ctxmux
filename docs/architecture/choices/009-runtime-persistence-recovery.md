# 009 — Runtime persistence and recovery

- Status: open
- Scope: durability beyond one daemon lifetime

## Context

The current daemon makes Runs independent of clients but stores identity, metadata, lifecycle, and replay only in memory. Daemon restart loses control even if an operating-system process survives. Product language must not hide this boundary.

## Decision

No persistence design is accepted yet. Current `RunId` is explicitly stable only for the lifetime of its owning daemon.

Any future design must separate three recovery classes:

- metadata and lineage that can be persisted and validated;
- output and artifacts that can be replayed after restart;
- live PTY and child ownership that may be impossible to transfer to a replacement process without a dedicated indirection mechanism.

## Quality attributes and invariants

- Recovery claims must be proven across real daemon restart or upgrade, not inferred from stored rows.
- Corrupt or stale state fails closed and never attaches to the wrong process.
- Process identity cannot rely on PID alone.
- Cleanup, retention, and recovery use one ownership model.
- A new persistence layer must not move live Run ownership into a client.

## Alternatives

- Metadata-only persistence can restore history but must not claim live control.
- File-backed logs are simple but need atomicity, quotas, compaction, and corruption handling.
- An always-stable per-Run shim could retain PTY ownership across daemon replacement but adds another process and supervision layer.
- Delegating durability to tmux preserves tmux-owned sessions only; it does not solve native Backend recovery.

## Known constraints

No store, schema, daemon identity, boot reconciliation, orphan policy, GC, or upgrade protocol exists. The current socket path is operator-supplied and does not identify a stable daemon instance.

## Wrong-case corpus

Evidence pack: [persistence-recovery track](../../../.bagakit/researcher/topics/engineering/ctxmux-wrong-case-corpus/tracks/persistence-recovery.md), claim `C009`.

- `PERSIST-01` (`i01`, `i02`): a persisted numeric PID can refer to an unrelated live process after restart. Ambiguous identity must become a non-recoverable typed state, never guessed adoption.
- `PERSIST-02` (`i03`): interruption between state writes, flushes, rename, and directory durability can expose a parseable mixed generation. Recovery must select one validated generation or report corruption.

Linux pidfds demonstrate stable identity within one boot but are neither portable nor durable across restart. SQLite demonstrates the failure class and explicit storage assumptions; it does not mandate SQLite as the implementation.

## Fixture mapping

- Inactive: all restart-recovery fixtures, because the capability is not implemented.
- Candidate activation fixture: metadata survives a clean restart without claiming a live PTY.
- Candidate activation fixture: crash during state write leaves the last valid state readable.
- Candidate activation fixture: PID reuse cannot bind stored state to an unrelated process.
- Candidate activation fixture: upgrade rollback does not split Run ownership.

## Open questions

- Is native live-PTY recovery a product requirement or an explicit unsupported capability?
- Which artifacts require `fsync`, checksums, or transactional replacement?
- What identifies daemon epochs and reconciles orphaned state?
- When and by whom are exited Runs and logs collected?
- Can upgrades drain and hand off without adding a per-Run supervisor process?

## Repository evidence

- `crates/ctxmux-protocol/src/lib.rs`: `RunId`
- `crates/ctxmux-daemon/src/lib.rs`: in-memory `RunManager`
- `docs/protocol.md`: current lifetime boundary
- `docs/roadmap.md`: architecture evidence and deferred durability work
