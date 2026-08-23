# 010 — Explicit TypeScript Integrations

- Status: accepted; ownership clarified
- Scope: optional host-side Integration contract above the generic Run protocol

## Context

Shells and other tools differ in detection, launch arguments, capabilities, and
optional context operations. Agent products additionally own Provider session
identifiers, resume semantics, permissions, messages, and semantic events. The
daemon must remain Agent-neutral and independent of a JavaScript host, while an
embedding product must not be forced to maintain a second Provider
implementation inside ctxmux.

## Decision

The Integration API generation 2 uses ordinary TypeScript modules imported and
registered explicitly by the embedding host. An Integration may detect a tool,
turn configuration into a portable launch plan, declare capabilities, create a
disposable host-local observer, and materialize an explicit Level B plan.

ctxmux owns the Provider-neutral Integration interface, its binding to the raw
Run client, and Provider-neutral reference conformance such as the shell
Integration. An embedding product owns its Agent-specific Provider modules,
including executable compatibility, provider-native identity, semantic event
parsing, permission behavior, and launch/resume plan materialization. One
deployed stack has one owner for each Agent Provider; ctxmux does not require or
duplicate that Provider in order to run the resulting generic `RunSpec`.

The daemon does not discover npm packages, load JavaScript, start a plugin process, or host a marketplace. A launched Run remains operable through raw Run APIs after its Integration host exits.

## Quality attributes and invariants

- Adding an Agent to an embedding product requires a Provider in that product,
  not a foundational ctxmux Run variant.
- Adding that Agent does not require a ctxmux daemon or
  protocol change; the product's Provider materializes the generic Run plan.
- Integration capabilities are explicit and versioned at their public boundary.
- Missing semantic observation never terminates or hides the raw Run.
- Tool-specific secrets and config are not copied into generic protocol fields by convenience.
- Unsupported fidelity fails closed rather than silently falling back.
- The Integration API is optional. Raw Run operations and the standalone CLI do
  not require an Integration host.

## Alternatives

- Agent-specific daemon types permanently couple runtime evolution to vendor CLIs.
- Dynamic package discovery adds trust, lifecycle, version, and marketplace policy before it creates user value.
- Embedded JavaScript or separate plugin processes widen the runtime and distribution surface.
- Command templates alone cannot express detection, capabilities, semantic events, or native resume honestly.

## Known constraints

The TypeScript SDK owns the Integration API generation 2 interface and explicit
client binding. The shell Integration proves detection and structured launch
planning without semantic or Level B claims.

For session-backed Level B, the owning host must bind provenance to the exact
source Run and materialize the complete replacement `RunSpec` before requesting
runtime mutation. The generic SDK may provide source-binding helpers, but it
does not infer provider session identity. Missing, copied, unowned, or
parent-mismatched provenance fails before planner execution or raw fork. A
caller that wants Level A must issue a separate explicit Level A request.

This is a supported-API ownership check against accidental fabrication and
cross-Run routing, not daemon authentication against a malicious host that can
call raw fork directly. Workspace snapshots, artifact ownership, secrets, and
Provider implementation-version identity remain outside the generic Runtime
contract.

## Wrong-case corpus

Evidence pack: [integrations track](../../../.bagakit/researcher/topics/engineering/ctxmux-wrong-case-corpus/tracks/integrations.md), claim `C010`.

- `INTEGRATION-01` (`j01`): interpolating workspace paths, prompts, or options into one shell string permits metacharacters to become program text. Launch remains structured executable, argv, cwd, and env by default.
- `INTEGRATION-02` (`j02`): executable presence is not semantic compatibility. Version, capability, malformed, and hanging probe paths fail closed before launch. Level B fidelity remains owned by the fork capability record and public behavior proof.

MCP supports the negotiation and timeout principle only. It does not justify JSON-RPC, discovery, a marketplace, or a plugin host for explicitly imported ctxmux Integrations.

## Fixture mapping

- Active: SDK tests cover explicit binding, structured shell planning, a
  no-claim shell observer, and raw Run continuity after observer loss.
- Required: a synthetic host-owned Provider binds provenance to one exact
  parent, materializes a generic Level B `RunSpec`, and creates one child with
  the declared lineage through the public SDK.
- Required: copied, unbound, cross-registration, parent-mismatched, and
  unrelated-Run provenance are rejected before planner or daemon mutation.
- Required: Integration host exits while the raw child and Run remain usable.
- Covered: an Integration that does not declare Level B capability or a planner
  fails before any raw fork request.
- Covered: an Integration that declares Level B but omits a provenance hook
  fails before planner or raw fork; the raw fork count remains zero.
- Required: the standalone shell and raw-Run SDK tests remain green without
  Agent-specific modules in the ctxmux publication.

## Open questions

- Which Provider-neutral context and fidelity data must later cross the daemon
  protocol instead of remaining host-local?
- How is a host-owned Provider version referenced for reproducibility without
  making it daemon state?
- How are secrets redacted from plans, events, logs, and fixtures?

## Repository evidence

- `AGENTS.md`: Integration boundary and project goals
- `docs/vision.md`: Agent-neutral Run thesis
- `docs/roadmap.md`: M2
- `packages/sdk/src/integration.ts`: Provider-neutral Integration interface and
  client binding
- `packages/sdk/src/integrations/shell.ts`: Provider-neutral shell reference
- `packages/sdk/test/client-parity.test.ts`: public Integration launch, Level B
  ownership checks, and raw Run continuity
- `packages/sdk/test/shell-integration.test.ts`: standalone shell Integration
