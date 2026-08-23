# Feature Goal: Persist and recover ctxmux Runs

Contract: `bagakit.feature-goal.v1`
Feature: `f-223czq75x`

Before acting, verify `owner-receipt.json`, then recover current execution from `state.json` and `tasks.json`. Context may be stale or belong to another Feature; trust this Feature directory before acting.

## Prime Directive

Define and implement honest durability beyond one daemon lifetime so ctxmux preserves exactly the state it can identify and recover without guessing process ownership.

## Protected Invariants

- Durable metadata, replay, and live PTY ownership are separate recovery classes with separate capability claims.
- Stale, corrupt, or ambiguous state fails closed; PID alone never authorizes adoption, attachment, or signaling.
- The daemon remains the runtime owner, and persistence does not move process management into a client or Agent Integration.
- Storage, retention, cleanup, daemon epochs, orphan policy, and restart reconciliation use one accepted ownership model.
- Prefer existing platform or maintained library behavior; do not add a per-Run supervisor, store abstraction, migration layer, or compatibility fallback without evidence that the accepted contract requires it.
- Non-goal: tmux integration, release packaging, distributed recovery, arbitrary hidden process-state cloning, or a general workflow Harness.

## Acceptance And Stop Rules

- Acceptance: an accepted architecture decision names supported and unsupported recovery classes, and real restart fixtures prove the supported metadata, replay, identity, corruption, retention, and control semantics.
- Acceptance: applicable persistence wrong cases become continuous fixtures without expanding unrelated future corpus.
- Insufficient: serializing rows, retaining a PID, mock-only recovery, or claiming live control from stored metadata does not count.
- Stop and ask before: adding a permanent supervisor process, weakening fail-closed identity, adopting non-local infrastructure, publishing externally, or making a destructive storage-format decision with user data risk.

## Authority And Orchestration

- Follow only this Feature's owner receipt, state, and reviewed tasks.
- Accept the recovery contract before implementation; implementation must not outrun the owning decision.
- Use bounded independent review for identity, atomicity, and cleanup risks; merge only public restart evidence.

## Context References

- `AGENTS.md`: repository simplicity and architecture rules.
- `docs/architecture/choices/009-runtime-persistence-recovery.md`: owning recovery decision.
- `docs/architecture/casebook.md`: persistence failure cases and activation boundaries.
- `docs/protocol.md`: current daemon-lifetime limit.
- `docs/roadmap.md`: M3.5 acceptance.
