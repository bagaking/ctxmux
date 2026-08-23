# Feature Goal: Complete the local Runtime embedding contract

Contract: `bagakit.feature-goal.v1`
Feature: `f-22ecztapc`
Convergence: `terminal`
Closure: `state`

Before acting, verify `owner-receipt.json`, then recover current execution from `state.json` and `tasks.json`. Context may be stale or belong to another Feature; trust this Feature directory before acting.

## Prime Directive

Complete ctxmux as an independently installable, Agent-, Provider-, and UI-neutral local Runtime whose daemon, CLI, Rust client, and TypeScript SDK own and expose endpoint identity, authoritative Run observations, race-free waiting, activation, PTY/process/replay, and caller-materialized fork facts. This matters because embedding products must consume one proven Runtime owner instead of reimplementing identity, lifecycle, wait, or activation state machines.

## Convergence Contract

- Smallest sufficient closure: the reviewed Phase 1 local Runtime embedding surface is complete and independently qualified; Remote Runtime and optional derivation metadata remain separate work.
- Oracle or ratchet: every current reviewed Task is `done`, its required gates pass on the same committed candidate, and the standalone qualification evidence proves the public CLI and SDK lifecycle without AgentMux.
- Scope expansion: record a newly accepted requirement in the appropriate reviewed Task before acting; route Remote Runtime, Provider-neutral derivation, Agent-specific semantics, or other adjacent work to their owning Feature or repository.
- Completion or cycle stop: stop when the Feature owner receipt reports complete after all current Tasks and closeout evidence are satisfied; stop earlier only on a canonical blocker or a user decision boundary.

## Protected Invariants

- `Run` remains the universal core object, and the daemon remains the sole owner of PTYs, children, Run identity, lifecycle, output, and durable Runtime state.
- The versioned public protocol is the stable boundary. The CLI and first-party SDKs must use the same public contract available to external clients.
- ctxmux remains a complete standalone Runtime product: without installing or importing AgentMux, its released surfaces can activate the daemon, run arbitrary commands, detach and attach, replay output, input and resize, inspect and wait, stop, and obtain authoritative identity, revision, and time.
- Integration remains Provider-neutral, explicitly imported, and separate from Backend location. A requested Level B operation fails closed unless the caller supplies a fully materialized, provable plan; ctxmux never silently substitutes Level A.
- Provider session discovery, transcript parsing, native resume arguments, Agent status, permissions, messages, A2A, and parent/child settlement remain outside ctxmux.
- Optional capabilities fail explicitly when unsupported; persistence, recovery, attach, wait, activation, and fork claims require real public behavior with a surviving child, not types or mocks alone.
- Non-goal: do not implement Remote Runtime transport, hosted control planes, Relay, account or environment federation, Provider-specific derivation policy, orchestration, an Agent Harness, or a plugin marketplace in this Feature.

## Acceptance And Stop Rules

- Acceptance: Rust, TypeScript, and CLI public consumers on one committed candidate prove the reviewed Integration/Level B boundary, Runtime identity and versioned capabilities, revisioned authoritative observations, lost-wakeup-safe structured waits, race-safe local activation, and the complete standalone lifecycle; repository, reliability, coverage, package, and qualification gates pass without weakened assertions.
- Insufficient: documentation or type definitions without executable public behavior; green private mocks without a real child/PTY boundary; SDK behavior that depends on AgentMux; polling or receipt-time guesses presented as authoritative Runtime facts; automatic Level B downgrade; or evidence from a predecessor candidate.
- Stop and ask before: changing the product category or protected invariants; importing Agent-specific policy; expanding into Phase 2 or another repository's implementation; weakening acceptance or compatibility rules; adding an enduring runtime, discovery, or control plane; publishing, pushing, releasing, or taking an irreversible/destructive action outside the current reviewed Task.

## Authority And Orchestration

- Follow only this Feature's owner receipt, state, and reviewed tasks.
- Before substantial work and after every review, re-read this Goal and the current acceptance evidence.
- Take the smallest action that directly advances that evidence or removes a real blocker. Defer anything not required for the current closure; stop when acceptance and applicable mandatory gates are satisfied.
- Do not implement a chat-only requirement. First record each accepted new requirement in the appropriate reviewed Feature Task through Feature Tracker.
- Prove the cheapest representative user-visible vertical before broad horizontal infrastructure.
- For engineering work, satisfy acceptance first; among valid solutions minimize enduring states, owners, APIs, abstractions, duplicated truth, and temporary scaffolding.
- Keep one integration writer for the current candidate. Parallel subagents may perform read-only research, review, tests, or external-consumer checks; grant additional write authority only in an isolated worktree or a demonstrably disjoint write set with one explicit integration owner.
- Bind every review and gate to the exact current candidate. Preserve prior green results as history, not completion evidence after the candidate changes.
- Update the owning document in the same change when behavior or a durable boundary changes. Do not recreate a second planning truth outside Feature Tracker.
- Do not push, publish, release, or mutate AgentMux. AgentMux consumer migration and its evidence remain owned by that repository and must arrive through an attributable external receipt.

## Context References

- `AGENTS.md`: protected Runtime boundaries and mandatory validation; read before implementation or architecture changes.
- `docs/vision.md`: standalone product position and non-goals; read when scope or product ownership is in question.
- `docs/architecture.md`: owner and component boundaries; read before changing daemon, Integration, Backend, protocol, or SDK responsibilities.
- `docs/protocol.md`: currently implemented public contract; read before protocol, persistence, observation, wait, or recovery changes.
- `docs/roadmap.md`: Phase 1 delivery order and acceptance; read when selecting or completing a Task.
- `docs/architecture/choices/010-explicit-typescript-integrations.md`: Integration versus embedding-product ownership; read for the first remaining Task and later Level B work.
- `docs/architecture/choices/015-exec-in-place-upgrade-continuity.md`: accepted baseline owner-transfer guarantees; read when a later change touches daemon identity, persistence, controls, or recovery.
