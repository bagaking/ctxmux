# Implementation Roadmap

- Status: reviewed
- Review basis: user and Agent design discussion completed on 2026-08-09
- Scope: ctxmux runtime foundation and reference embedding surfaces

This roadmap is ordered by end-to-end proof. A later milestone must not force a
partial earlier milestone to pretend it provides capabilities it does not.

## M0 — Repository foundation

Establish the smallest Rust and TypeScript workspaces, shared quality commands,
CI, contribution entrypoints, and package boundaries consistent with
`docs/architecture.md`.

Acceptance:

- a clean checkout can run the documented formatting, static analysis, build,
  and test commands;
- the repository contains no placeholder plugin framework or unused service;
- package and crate boundaries correspond to an executable vertical slice.

## M1 — Durable native Run

Deliver one real local Run through the public boundary: start, observe, attach,
detach, reattach, resize, send input, receive ordered output and exit state, and
stop.

Acceptance:

- the daemon, CLI, and TypeScript SDK exercise the same versioned boundary;
- a child process demonstrably survives CLI and SDK client exit;
- reconnect receives enough state and subsequent output to continue operating;
- unsupported or invalid lifecycle operations fail explicitly.

## Architecture evidence and failure corpus

Before widening the runtime with Integrations, make the current system legible
and turn known failure modes into a reusable engineering ratchet.

Acceptance:

- `docs/architecture.md` separates current guarantees from target design and
  maps the core scenarios, components, ownership, ordering, and failure paths;
- every critical implemented, provisional, or open technical choice has one
  linked decision document with local code evidence and explicit status;
- each decision document contains a source-backed wrong-case section covering
  real failures, edge cases, and counterevidence rather than generic advice;
- every retained case has a normalized fixture record and traceability to its
  decision, source, invariant, implementation status, and executable test when
  the relevant capability exists;
- applicable high-risk fixtures run in `scripts/check.sh` and CI, while future
  capability cases fail closed as explicit backlog rather than pretend tests.

## M1 hardening — launch rollback

Before adding Integrations, close the native launch transaction at its owning
boundary. Once `spawn_command` succeeds, any later setup failure must terminate
and reap the child before `start` returns an error; no Run identity may be
published for that failed launch.

Acceptance:

- deterministic owner-controlled failure points cover post-spawn reader,
  writer, output-thread, and waiter-thread setup transitions;
- every rejected launch leaves no live child, zombie, or Run in the manager;
- the fix stays inside the native launch owner and does not extract a public
  Backend framework, plugin surface, or general fault-injection system.

## M2 — Integration contract and first Agent

Introduce explicit in-process Integration registration only after the generic
Run lifecycle works. Prove the boundary with a generic shell Integration and
one mainstream coding Agent Integration.

Acceptance:

- adding the Agent Integration does not add Agent-specific fields to the
  daemon's foundational Run types;
- the host explicitly imports and registers Integrations;
- detect, launch planning, capabilities, and normalized events work through
  the TypeScript SDK;
- the raw Run remains usable when no semantic Integration observer is attached.

## M3 — Context, artifacts, lineage, and fork

Add portable Level A fork, then prove Level B with the first Agent Integration.
Keep capability requests explicit and fail closed.

Acceptance:

- Level A forks reproduce only the documented portable Run specification and
  declared inputs;
- each fork records parentage and declared context/artifact references;
- one Integration demonstrates a genuine Level B resume or fork;
- requesting Level B from a Level A-only Integration returns an explicit
  unsupported-capability result.

## M4 — tmux adapter

Connect existing tmux sessions through public tmux integration surfaces while
keeping native Run semantics and tmux implementation details separated.

Acceptance:

- ctxmux can list and attach to a selected existing tmux session through the
  adapter;
- disconnecting the ctxmux client does not kill the tmux session;
- the implementation does not speak tmux's private client-server wire
  protocol;
- documentation names any behavioral differences from the native backend.

## M5 — Composition proof and release

Prove embeddability with a deliberately small reference client or example that
forks Runs and combines results without moving scheduling or evaluation policy
into the core.

Acceptance:

- the example can express a bounded Crucible- or MapReduce-like workflow using
  only public APIs;
- orchestration and evaluation remain client-owned;
- packages and binaries have installation, compatibility, and release
  documentation;
- the public positioning remains accurate for every shipped capability.

## Explicitly deferred

- a complete editor;
- a hosted or distributed control plane;
- plugin discovery, marketplace, or untrusted plugin sandbox;
- arbitrary live-process state cloning;
- broad Integration coverage before one Integration proves Level B value.
