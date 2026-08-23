# 016 — Interrupted-Run derivation

- Status: accepted
- Scope: deriving a new Run from an interrupted persisted Run after the original
  process and live PTY authority are genuinely gone

## Context

[009](009-runtime-persistence-recovery.md) recovers a prior `running` Run as
`interrupted` across a cold daemon restart, and
[015](015-exec-in-place-upgrade-continuity.md) preserves live control across a
planned exec-in-place upgrade. Neither can reattach to a process that died in a
daemon crash or host reboot. The master fd and child-wait authority no longer
exist, and PID re-adoption is forbidden.

A recovered Run still exposes retained metadata, replay, declared inputs, and
lineage. Those Runtime facts are enough to start a new process from a declared
plan. They are not enough for ctxmux to identify a Provider session, interpret
an Agent transcript, or claim that a conversation continues.

An earlier proposal assigned provider replay parsing and native resume
construction to ctxmux and allowed missing Level B provenance to become Level
A. Both assignments violate the Runtime boundary: provider semantics belong to
the embedding client, and a requested fidelity must never be weakened
implicitly.

## Decision

Resume is an explicit derivation that creates a new Run and records lineage to
the interrupted parent. The parent identity, retained output, and terminal
state never change. There is no daemon-initiated respawn.

Any public resume surface must name the requested fidelity. A CLI may expose
the standalone Level A operation; a host-side SDK caller may additionally
supply a fully materialized Level B plan. A bare command must not choose a
weaker fidelity after discovering that the stronger one is unavailable.

### Level A — restart from portable Run inputs

ctxmux resolves the retained interrupted parent, clones its immutable `RunSpec`
and declared references, starts a new physical process, and records Level A
lineage. This restores the declared command, not an Agent conversation or
hidden process state.

Level A is part of the standalone Runtime. It requires no Provider, semantic
observer, or embedding product.

### Level B — execute a caller-materialized continuation plan

The embedding product owns Provider-specific provenance and plan construction.
It may read retained replay or its own Agent session store, resolve the exact
provider-native session, verify Provider capability, and materialize the full
replacement `RunSpec`. ctxmux receives only the generic plan, declared
references, requested fidelity, exact parent identity, and retry-safe operation
identity.

ctxmux validates the generic Runtime contract, creates the child, and records
the derivation. It does not:

- parse Provider-specific output;
- extract or persist provider session identifiers as generic Run fields;
- construct provider-native resume arguments;
- decide whether a Provider conversation is semantically continuous;
- infer Level B from a lineage label or terminal bytes.

`RunInfo.lineage.fidelity = level_b` records that ctxmux executed the explicit
Level B plan supplied by the caller. It is not independent daemon proof that
the Provider restored every part of its hidden context. A higher-level semantic
event may cite the Run lineage and byte ranges as evidence, but the Provider
owner interprets that evidence.

### Failure behavior

If Level B provenance, Provider capability, or a complete materialized spec is
unavailable, the Level B request fails before creating a Run. The caller may
subsequently make a separate, explicit Level A request. ctxmux never turns the
first request into that second operation.

Recovered replay may be truncated. That is an input condition visible to the
embedding product, not permission for ctxmux to guess a provider session or
change fidelity.

## Ownership

| Concern                                                                               | Owner                          |
| ------------------------------------------------------------------------------------- | ------------------------------ |
| interrupted parent identity, retained `RunSpec`, replay bytes, lifecycle, and lineage | ctxmux                         |
| Level A clone, new process creation, retry safety, and generic derivation record      | ctxmux                         |
| Provider session identity and capability                                              | embedding Provider/Integration |
| semantic replay parsing and provenance validation                                     | embedding Provider/Integration |
| provider-native resume arguments and replacement `RunSpec`                            | embedding Provider/Integration |
| whether to retry explicitly as Level A after Level B fails                            | caller or operator             |

## Quality attributes and invariants

- Resume is explicit and operator- or caller-driven; the daemon never respawns
  a Run on its own.
- Every resume creates a new Run with a new identity and recorded lineage to the
  exact interrupted parent.
- The interrupted parent remains immutable and inspectable.
- Level A and Level B are separate requests with separate receipts.
- Level B accepts only a complete caller-materialized plan and never falls back
  to Level A.
- Provider-specific parsing, identity, permissions, messages, and status do not
  enter the daemon protocol or foundational Run types.
- No live PTY handoff, fd transfer, or PID re-adoption of the old child occurs.
- Retry safety binds to the exact parent, fidelity, materialized plan, and
  operation identity so a lost response cannot create another child.

## Alternatives

- **Automatic resume on recovery.** Rejected because it spends resources and
  restarts abandoned work without operator intent.
- **Parse durable replay inside ctxmux.** Rejected because output bytes are
  Runtime evidence, while provider session extraction and transcript semantics
  belong to the Provider owner.
- **Persist provider session identifiers in generic Run state.** Rejected
  because it couples the daemon schema and lifecycle to Agent vendors.
- **Fall back from Level B to Level A.** Rejected because it changes the
  requested fidelity and creates a new physical process under a different
  semantic contract.
- **Re-adopt the prior PID.** Rejected because metadata is not live ownership of
  the master fd or waitable child and may refer to an unrelated process.
- **Treat resume as reattach.** Rejected because the original process is gone;
  a newly created Run has a distinct identity and lineage.

## Known constraints

Level B is available only when an embedding Provider can establish its own
provenance and produce a complete generic Run plan. A plain shell has no
conversation to continue, so Level A is the meaningful standalone operation.
Provider fidelity is bounded by the Provider's native mechanism and remains a
semantic claim outside ctxmux.

The exact wire/API shape for an interrupted-parent derivation is implementation
work. It may reuse the existing fork/start machinery, but it must preserve the
explicit fidelity, exact-parent, retry-safety, and no-fallback invariants above.

## Fixture mapping

- Future: after a real kill and restart, the parent is `interrupted`; explicit
  Level A creates one new Run with Level A lineage and leaves the parent
  unchanged.
- Future: a host supplies a complete Level B `RunSpec`; ctxmux creates one new
  Run with Level B lineage without parsing provider output.
- Future: missing Level B provenance or replacement spec creates no Run and
  returns a structured unsupported or invalid-plan result.
- Future: after that Level B failure, a separate Level A operation creates one
  Run with a different operation identity and an explicit Level A receipt.
- Future: resume of a non-`interrupted` parent, a mismatched parent identity, or
  conflicting operation-key reuse fails before launch.
- Future: abandoning and retrying the same accepted resume request converges on
  one physical child.

## Open questions

- Should the generic derivation record distinguish `fork`, `restart`, and
  `resume` in addition to fidelity?
- Which capability name advertises interrupted-parent Level B execution without
  implying that ctxmux understands Provider semantics?

## Repository evidence

- `AGENTS.md`: fail-closed fork fidelity and Agent-neutral Runtime boundary
- `docs/vision.md`: standalone product and embedding boundary
- `docs/architecture.md`: Runtime ownership and evidence split
- `docs/architecture/choices/009-runtime-persistence-recovery.md`: recovered
  interrupted Runs and prohibition on PID adoption
- `docs/architecture/choices/011-context-artifact-lineage-fork.md`: explicit
  Level A and caller-materialized Level B plans
- `crates/ctxmux-daemon/src/persistence.rs`: retained Run metadata and replay
