# 010 — Explicit TypeScript Integrations

- Status: accepted
- Scope: Agent and tool semantics above the generic Run protocol

## Context

Codex, Claude Code, TraeX, Hermes, Pi, shells, and other tools differ in detection, launch arguments, session identifiers, resume semantics, permissions, and semantic events. Clients should not reimplement those differences, but the daemon must remain Agent-neutral and independent of a JavaScript host.

## Decision

The Integration API generation 2 uses ordinary TypeScript modules imported and registered explicitly by the embedding host. An Integration may detect a tool, turn configuration into a portable launch plan, declare capabilities, create a disposable host-local semantic observer, and optionally materialize an explicit Level B fork plan.

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

The TypeScript SDK owns the Integration API generation 2 interface and explicit client binding. The shell Integration proves detection and structured launch planning without semantic or Level B claims. The Codex Integration uses bounded `--version`, `exec --help`, and `exec resume --help` probes, launches `codex exec --json`, and materializes `codex exec resume --json` only when native resume support is present. Workspace snapshots, artifact ownership, secrets, and Integration implementation-version identity remain open.

## Wrong-case corpus

Evidence pack: [integrations track](../../../.bagakit/researcher/topics/engineering/ctxmux-wrong-case-corpus/tracks/integrations.md), claim `C010`.

- `INTEGRATION-01` (`j01`): interpolating workspace paths, prompts, or options into one shell string permits metacharacters to become program text. Launch remains structured executable, argv, cwd, and env by default.
- `INTEGRATION-02` (`j02`): executable presence is not semantic compatibility. Version, capability, malformed, and hanging probe paths fail closed before launch. Level B fidelity remains owned by the fork capability record and public behavior proof.

MCP supports the negotiation and timeout principle only. It does not justify JSON-RPC, discovery, a marketplace, or a plugin host for explicitly imported ctxmux Integrations.

## Fixture mapping

- Active: a public Codex recording-child launch proves exact argv and that the raw Run stays usable without an observer.
- Active: the Codex probe matrix covers missing, malformed, incompatible, and hanging executables before Run launch.
- Current: SDK tests also cover explicit binding, structured shell planning, a no-claim shell observer, partitioned Codex JSONL, gaps, and parser diagnostics.
- Candidate activation fixture: Integration host exits while the raw child and Run remain usable.
- Covered: unsupported Level B capability fails before any raw fork request.

## Open questions

- Which context and fidelity data must later cross the daemon protocol instead of remaining host-local?
- How are executable version and Integration version recorded for reproducibility?
- How are secrets redacted from plans, events, logs, and fixtures?

## Repository evidence

- `AGENTS.md`: Integration boundary and project goals
- `docs/vision.md`: Agent-neutral Run thesis
- `docs/roadmap.md`: M2
- `packages/sdk/src/integrations/`: current shell and Codex implementations
- `packages/sdk/test/codex-integration.test.ts`: Codex probe and observer fixtures
- `packages/sdk/test/client-parity.test.ts`: public Integration launch and raw Run continuity
