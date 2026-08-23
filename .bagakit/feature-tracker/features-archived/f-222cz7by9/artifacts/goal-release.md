# Feature Goal: Prove composition and prepare release

Contract: `bagakit.feature-goal.v1`
Feature: `f-225cz7943`

Before acting, verify `owner-receipt.json`, then recover current execution from `state.json` and `tasks.json`. Context may be stale or belong to another Feature; trust this Feature directory before acting.

## Prime Directive

Prove that public ctxmux APIs support useful fork-and-combine clients without becoming a Harness, then make the binaries and SDK reproducibly release-ready with accurate public claims.

## Protected Invariants

- Composition policy stays in the example client: scheduling, evaluation, reduction, winner selection, and stopping never enter the mux.
- The example uses only public protocol, client, and Integration APIs and does not become a reusable orchestration framework.
- Installation, compatibility, package contents, and README claims match behavior proven by repository Gates.
- Release work consumes the completed persistence and tmux capability boundaries rather than implementing them here.
- Non-goal: external publication, hosted control planes, broad Agent coverage, plugin discovery, or a complete editor.

## Acceptance And Stop Rules

- Acceptance: a bounded deterministic example composes and combines forked Runs through public APIs, while reproducible build, install, package-content, and compatibility checks leave ctxmux ready for an authorized release.
- Insufficient: pseudocode, private daemon access, orchestration inside core, package manifests without install tests, or claims ahead of behavior do not count.
- Stop and ask before: registry publication, Git push, hosted release creation, credential use, paid services, compatibility promises, or any other external mutation.

## Authority And Orchestration

- Follow only this Feature's owner receipt, state, dependencies, and reviewed tasks.
- Do not begin execution until the persistence and tmux Feature dependencies are satisfied.
- Keep composition and packaging as separate task-level rollback boundaries and run the full release Gate before closeout.

## Context References

- `AGENTS.md`: project rules and no-Harness boundary.
- `README.md`: public positioning and installation surface.
- `docs/vision.md`: composition ownership and product success.
- `docs/architecture.md`: stable public boundaries and current guarantees.
- `docs/roadmap.md`: M5 acceptance and dependency boundary.
