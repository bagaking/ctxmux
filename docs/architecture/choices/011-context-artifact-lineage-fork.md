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

References and lineage follow the Run's configured memory or historical
persistence class. There is no workspace snapshot strategy, artifact store, or
cleanup protocol. A bounded creation operation key makes process creation
retry-safe only while its Run is retained; it does not make referenced
workspaces or artifacts idempotent, immutable, or owned.
Opaque references do not prove existence, immutability, ownership, portability,
inclusion policy, or secret safety.

The TypeScript Level B receipt is host-local and source-bound through Attachment
ownership. The SDK records actual live events and replay chunks against their
Run, rejects missing or mismatched source identity at a parent-scoped observer,
and requires every Level B Integration to expose a checked provenance receipt.
This prevents accidental cross-Run certification through the supported API; it
does not turn the JavaScript host into a security boundary against callers that
bypass the Integration and invoke raw fork directly.

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
- Covered: the Codex Integration obtains a session event through a source-bound
  parent observer, rejects copied, unbound, mismatched, and unrelated-Run input
  before raw fork, invokes `exec resume --json` through the public fork path,
  and records workspace, artifact, and session references plus `level_b`
  lineage.
- Scheduled external evidence: a credential-controlled real Codex parent
  establishes a unique fact and a Level B continuation whose prompt omits that
  fact must return it exactly; artifacts retain only hashes, version, timing,
  event names, and lineage.
- Current real-vendor evidence: Codex 0.147.0 passed the same canary locally via
  explicitly authorized CLI login, with distinct parent/child Runs, exact fact
  continuation, `level_b` lineage, and no fatal gap, UTF-8, or record-size
  diagnostic. Non-JSON PTY lines remain visible as aggregate counts.
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
- `packages/sdk/test/client-parity.test.ts`: public Codex Level B behavior
- `packages/sdk/test/shell-integration.test.ts`: unsupported Level B rejection
- `scripts/codex-semantic-canary.ts`: real Codex semantic continuation
