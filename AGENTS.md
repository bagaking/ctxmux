# ctxmux Agent Instructions

## Mission

ctxmux is a context-aware Run multiplexer for local processes. It gives shells,
coding agents, dev servers, tests, benchmarks, and scripts one durable runtime
that clients can start, observe, detach from, reattach to, and fork.

The product thesis is:

> Terminals are views. Runs are durable.

The public category description is:

> tmux multiplexes terminals. ctxmux multiplexes runs—with context intact.

## Sources of Truth

Read these before changing product behavior or architecture:

1. `docs/vision.md` — product model, users, scope, and non-goals.
2. `docs/architecture.md` — durable component boundaries and capability model.
3. `docs/protocol.md` — current public daemon protocol and proven limitations.
4. `docs/roadmap.md` — vertical delivery order and acceptance evidence.
5. `.bagakit/feature-tracker/` — current execution truth after it exists.

When a decision changes, update the owning document in the same change. Do not
let chat history, code comments, or a second planning file become product truth.

## Protected Invariants

1. `Run` is the universal core object. Agent-specific concepts must not enter
   the runtime's foundational types.
2. The daemon owns PTYs, child processes, Run identity, lifecycle, output, and
   durable runtime state. A Run must not depend on an editor or SDK process
   staying alive.
3. Clients use public protocol and SDK boundaries. A first-party client must
   not bypass the same API offered to external clients.
4. ctxmux is a multiplexer, not an Agent Harness. It does not plan work,
   schedule teams, judge results, select winners, or own Crucible/MapReduce
   policy.
5. Integrations describe what runs inside a Run. Backends describe where a Run
   executes. Do not merge these two extension axes.
6. Integrations are explicitly imported and registered in the host process.
   Do not add plugin discovery, a marketplace, an embedded JavaScript runtime,
   or isolated plugin processes without a new reviewed architecture decision.
7. Fork capability is explicit and fail-closed:
   - A copies the portable Run specification and declared input references.
   - B adds Integration-provided workspace, artifact, lineage, and native
     session fidelity.
   - C, copying arbitrary live process memory or hidden state, is out of scope.
     A request for B must fail when the Integration cannot provide B; it must not
     silently degrade to A.
8. tmux compatibility means familiar attach/detach behavior plus an adapter
   through tmux's public surfaces. Do not implement or claim compatibility with
   tmux's private client-server wire protocol.

## Architecture Direction

- Prefer a Rust daemon for the long-lived runtime and a TypeScript SDK as the
  first embedding surface.
- Treat the versioned local protocol—not a Rust ABI, Node addon, or UI
  framework—as the stable client boundary.
- Keep the daemon Agent-neutral and independent of Electron, React, and Node.
- Make the CLI a real client of the same public boundary.
- Add another language SDK only when a real client requires it.

These are durable boundaries, not permission to pre-design every module or
protocol field. Defer reversible implementation details until the vertical
slice needs them.

## Delivery Rules

1. Deliver the smallest end-to-end Run lifecycle before broadening the model.
2. Prefer mature, maintained libraries and inspect existing dependencies before
   adding or rewriting functionality.
3. Keep concerns modular, but add an abstraction only when current behavior
   requires it.
4. Do not preserve backward compatibility before the public contract is
   declared stable. Delete obsolete paths instead of adding migrations,
   fallbacks, aliases, or compatibility layers.
5. Do not implement speculative distributed execution, orchestration,
   collaboration servers, plugin marketplaces, or a full editor in the core
   milestone.
6. Use capability declarations for optional behavior. Unsupported capability
   requests must return explicit errors.
7. Test public behavior and owner boundaries, not only private implementation
   shape.

## Required Validation

For every implementation task, use the narrowest applicable checks and record
them in the active Feature Tracker task:

- formatting and static analysis for changed languages;
- unit tests for changed contracts;
- an integration test for any lifecycle or protocol change;
- a public CLI or SDK smoke test for an end-to-end milestone;
- documentation consistency when behavior or boundaries change.

Do not claim attach, persistence, recovery, or fork support from type definitions
or mocks alone. The child process must demonstrably survive the client boundary
being tested.
