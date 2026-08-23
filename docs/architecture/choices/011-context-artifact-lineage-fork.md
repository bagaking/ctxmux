# 011 — Context, artifacts, lineage, and fork fidelity

- Status: accepted
- Scope: portable Run cloning and Integration-provided continuity

## Context

ctxmux's differentiated value is controlled context continuity: create another Run from declared inputs or richer tool-native state without pretending to clone arbitrary process memory. Fork must be inspectable, capability-driven, and safe to compose into MapReduce or Crucible clients.

## Decision

The target contract has two supported fidelity levels and one non-goal:

- Level A copies declared portable launch inputs and references.
- Level B executes an Integration-materialized native resume or fork plan with explicitly declared workspace, artifact, and context references.
- Level C, arbitrary live-process memory or undeclared hidden state, is out of scope.

The caller requests a level. The runtime never silently substitutes a lower one.

In generation 6, `RunSpec.declared_inputs` is the sole immutable truth for
ordered workspace, artifact, and context references. Values are non-empty and
opaque; the daemon records them but does not dereference, normalize, copy, or
infer ownership. `RunInfo.lineage` records derivation only: the immediate parent
and fidelity actually executed.

`ForkPlan::LevelA` accepts no replacement spec. The daemon resolves the retained
parent and clones its complete immutable `RunSpec`. `ForkPlan::LevelB` carries a
fully materialized replacement `RunSpec`; the daemon neither merges it with nor
falls back to the parent. The owning host-side Integration or Provider must
establish Level B capability and source-bound provenance before a client sends
that plan. The wire tag alone is not capability evidence, and the daemon does
not infer provider identity from output.

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

References and lineage follow the Run's configured memory or historical
persistence class. There is no workspace snapshot strategy, artifact store, or
cleanup protocol. A bounded creation operation key makes process creation
retry-safe only while its Run is retained; it does not make referenced
workspaces or artifacts idempotent, immutable, or owned.
Opaque references do not prove existence, immutability, ownership, portability,
inclusion policy, or secret safety.

Level B provenance is host-local and source-bound to the parent Run. A generic
SDK helper may preserve source identity across Attachment events and replay,
but the embedding product owns provider parsing and the checked provenance
receipt. Missing or mismatched source identity fails before runtime mutation.
This prevents accidental cross-Run certification through the supported API; it
does not turn the host into a security boundary against callers that bypass the
Integration and invoke raw fork directly.

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
- Required: a synthetic host-owned Provider binds provenance to an exact parent,
  materializes a complete generic replacement `RunSpec`, and records declared
  references plus `level_b` lineage through the public fork path.
- Required: copied, unbound, mismatched, and unrelated-Run provenance creates no
  child and never changes the request to Level A.
- Covered: Shell rejects Level B before any raw fork request or child creation.
- Covered: every Level B Integration requires a provenance hook; missing-hook
  and unrelated-source regressions keep planner/raw fork count zero and create
  no child Run.
- Candidate activation fixture: partial fork failure removes provisional artifacts and lineage.
- Covered: concurrent and abandoned-response Start, Level A, and Level B retries
  converge on one physical child while conflicting key reuse creates none.
- Candidate activation fixture: secret and machine-local path policy fails closed.

## Open questions

- Which redaction and portability checks apply before fork execution?

## Repository evidence

- `docs/vision.md`: context-aware Run and composition boundary
- `docs/architecture.md`: target fork contract
- `docs/roadmap.md`: M3
- `crates/ctxmux-protocol/src/lib.rs`: `RunSpec`, `ForkPlan`, and `RunLineage`
- `crates/ctxmux-daemon/tests/native_lifecycle.rs`: public Level A behavior
- `packages/sdk/test/client-parity.test.ts`: public Provider-neutral Level B behavior
- `packages/sdk/test/shell-integration.test.ts`: unsupported Level B rejection
