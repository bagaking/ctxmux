# 010 — Explicit TypeScript Integrations

- Status: provisional
- Scope: Agent and tool semantics above the generic Run protocol

## Context

Codex, Claude Code, TraeX, Hermes, Pi, shells, and other tools differ in detection, launch arguments, session identifiers, resume semantics, permissions, and semantic events. Clients should not reimplement those differences, but the daemon must remain Agent-neutral and independent of a JavaScript host.

## Decision

The target Integration model uses ordinary TypeScript packages imported and registered explicitly by the embedding host. An Integration may detect a tool, turn configuration into a portable launch plan, declare capabilities, capture declared context, and interpret semantic events.

The daemon does not discover npm packages, load JavaScript, start a plugin process, or host a marketplace. A launched Run remains operable through raw Run APIs after its Integration host exits.

## Quality attributes and invariants

- Adding an Agent requires a new Integration, not new foundational Run variants.
- Integration capabilities are explicit and versioned at their public boundary.
- Missing semantic observation never terminates or hides the raw Run.
- Tool-specific secrets and config are not copied into generic protocol fields by convenience.
- Unsupported fidelity fails closed rather than silently falling back.

## Alternatives

- Agent-specific daemon types permanently couple runtime evolution to vendor CLIs.
- Dynamic package discovery adds trust, lifecycle, version, and marketplace policy before it creates user value.
- Embedded JavaScript or separate plugin processes widen the runtime and distribution surface.
- Command templates alone cannot express detection, capabilities, semantic events, or native resume honestly.

## Known constraints

No Integration package or interface exists. The unresolved boundary is how a short-lived TypeScript host supplies launch and context plans while the daemon preserves the resulting Run after that host disappears. Semantic observation, secrets, Integration version identity, and capability negotiation are unimplemented.

## Wrong-case corpus

Evidence pack: [integrations track](../../../.bagakit/researcher/topics/engineering/ctxmux-wrong-case-corpus/tracks/integrations.md), claim `C010`.

- `INTEGRATION-01` (`j01`): interpolating workspace paths, prompts, or options into one shell string permits metacharacters to become program text. Launch remains structured executable, argv, cwd, and env by default.
- `INTEGRATION-02` (`j02`): executable presence is not semantic compatibility. Version, capability, malformed probe, hang, and unknown event paths must fail closed when Level B is requested.

MCP supports the negotiation and timeout principle only. It does not justify JSON-RPC, discovery, a marketplace, or a plugin host for explicitly imported ctxmux Integrations.

## Fixture mapping

- Inactive: Integration fixtures until T-004 implements the contract.
- Candidate activation fixture: Integration host exits while the raw child and Run remain usable.
- Candidate activation fixture: unsupported executable version fails detection explicitly.
- Candidate activation fixture: semantic parser failure preserves raw output and lifecycle.
- Candidate activation fixture: unsupported Level B capability never launches a Level A fork.

## Open questions

- Which Integration methods run before launch, during observation, and after exit?
- Which data crosses the daemon protocol versus remaining host-local?
- How are executable version and Integration version recorded for reproducibility?
- What is the minimum capability set for the first shell and coding-Agent Integrations?
- How are secrets redacted from plans, events, logs, and fixtures?

## Repository evidence

- `AGENTS.md`: Integration boundary and project goals
- `docs/vision.md`: Agent-neutral Run thesis
- `docs/roadmap.md`: M2
- `packages/sdk/`: future TypeScript host surface
