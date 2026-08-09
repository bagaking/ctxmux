# 011 — Context, artifacts, lineage, and fork fidelity

- Status: Level A accepted; Level B open
- Scope: portable Run cloning and Integration-provided continuity

## Context

ctxmux's differentiated value is controlled context continuity: create another Run from declared inputs or richer tool-native state without pretending to clone arbitrary process memory. Fork must be inspectable, capability-driven, and safe to compose into MapReduce or Crucible clients.

## Decision

The target contract has two supported fidelity levels and one non-goal:

- Level A copies declared portable launch inputs and references.
- Level B adds Integration-captured workspace state, artifacts, lineage, and native session resume or fork information.
- Level C, arbitrary live-process memory or undeclared hidden state, is out of scope.

The caller requests a level. The runtime never silently substitutes a lower one.

In generation 2, `RunSpec.declared_inputs` is the sole immutable truth for
ordered workspace, artifact, and context references. Values are non-empty and
opaque; the daemon records them but does not dereference, normalize, copy, or
infer ownership. `RunInfo.lineage` records derivation only: the immediate parent
and fidelity actually executed.

`ForkPlan::LevelA` accepts no replacement spec. The daemon resolves the retained
parent and clones its complete immutable `RunSpec`. `ForkPlan::LevelB` carries a
fully materialized replacement `RunSpec`; the daemon neither merges it with nor
falls back to the parent. An explicitly registered Integration must establish
Level B capability before a client sends that plan. The wire tag alone is not
capability evidence.

## Quality attributes and invariants

- Every fork records parentage and the fidelity actually used.
- References and snapshots are distinguishable.
- Partial failure does not leave a successful-looking child or broken lineage.
- Secrets and machine-local state cross the boundary only when explicitly declared.
- Fork is a runtime primitive; scheduling, evaluation, reduction, and winner selection remain client policy.

## Alternatives

- Treating every fork as command replay is honest Level A but misses Integration value.
- Copying an entire workspace by default is expensive and can leak secrets or unrelated state.
- Pretending to clone live hidden state creates unverifiable fidelity claims.
- Building Crucible or MapReduce inside ctxmux would turn the mux into a Harness.

## Known constraints

References and lineage are daemon-memory-only. There is no workspace snapshot
strategy, artifact store, idempotency key, cleanup protocol, persistence, or
first Level B Integration. Opaque references do not prove existence,
immutability, ownership, portability, inclusion policy, or secret safety.

## Wrong-case corpus

Evidence pack: [context-fork track](../../../.bagakit/researcher/topics/engineering/ctxmux-wrong-case-corpus/tracks/context-fork.md), claim `C011`.

- `FORK-01` (`k01`): commits, default stash, untracked files, and ignored files are different inclusion sets. A fidelity label without an inspectable manifest silently omits material context.
- `FORK-02` (`k02`): a `--shared` clone can borrow objects later deleted by source maintenance. A fork advertised as independent must own its artifacts or declare its dependency.
- `FORK-03` (`k03`): environment credentials and ignored secret files can become durable workspace, manifest, replay, or lineage state unless secrets are classified separately.

Omission is valid when the declared policy excludes that class, and borrowing is valid when dependency is explicit. The failures are undisclosed fidelity loss, false independence, and accidental durability of secrets.

## Fixture mapping

- Covered: the public Rust client and daemon prove that Level A reproduces only
  the complete declared `RunSpec`, records parent plus `level_a`, creates a
  distinct child PID, and leaves parent and child independently usable.
- Candidate activation fixture: unsupported Level B fails without creating a child.
- Candidate activation fixture: partial fork failure removes provisional artifacts and lineage.
- Candidate activation fixture: concurrent retry is idempotent.
- Candidate activation fixture: secret and machine-local path policy fails closed.

## Open questions

- What proves that a Level B Integration preserves more than Level A?
- Which redaction and portability checks apply before fork execution?

## Repository evidence

- `docs/vision.md`: context-aware Run and composition boundary
- `docs/architecture.md`: target fork contract
- `docs/roadmap.md`: M3
- `crates/ctxmux-protocol/src/lib.rs`: `RunSpec`, `ForkPlan`, and `RunLineage`
- `crates/ctxmux-daemon/tests/native_lifecycle.rs`: public Level A behavior
