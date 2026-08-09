# Implementation Roadmap

- Status: reviewed
- Review basis: user and Agent design discussion completed on 2026-08-09
- Scope: ctxmux runtime foundation and reference embedding surfaces

This roadmap is ordered by end-to-end proof. A later milestone must not force a
partial earlier milestone to pretend it provides capabilities it does not.

## Delivery boundaries

M0 through M3 form the runtime foundation and close as one delivery boundary.
Persistence and recovery, the tmux adapter, and composition/release each use a
separate Feature after the foundation because they have different owners,
failure models, and rollback boundaries. The release Feature depends on those
capability Features; it does not absorb their implementation work.

Research and the wrong-case corpus are closed baseline work. New cases are
added only when a real implementation decision or observed failure creates a
new invariant; future cases are not expanded speculatively.

## Program ownership and convergence

Reviewed on 2026-08-12 after the reliability program grew beyond one finite
Feature boundary. Correctness, release qualification, and peer performance are
independently closable; one result must not keep an unrelated owner open.

| Owner                                   | Finite closure                                                                                                                                                                                  | Reviewed task topology                                                                                                                         |
| --------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| Run-Kernel correctness (`f-22bczhydf`)  | Bound memory-only and persistent retained state, dispose of unclassified native waiter failure without inventing exit truth, close Kernel P0/P1 findings, and prove the retained-state plateau. | memory-only Registry GC; bounded waiter-failure disposition; persistent exact replacement; bounded Kernel review; retained-state qualification |
| tmux adapter (`f-224czneed`)            | Prove the declared public Control Mode adapter and preserve non-output observation truth across attachment lag.                                                                                 | tmux capability tasks only; no release or Kernel GC work                                                                                       |
| Composition and release (`f-225cz7943`) | Prove public composition, activation, installation, packaging, independent release review, supported platforms, and local release gates.                                                        | composition example; package/release preparation; daemon activation; release qualification                                                     |
| Peer performance (`f-22aczwza9`)        | Run one pre-registered, budget-bounded measure/optimize/remeasure cycle and record honest wins, ties, and losses.                                                                               | comparable harness; measured raw-byte ROI decision; bounded-cycle result                                                                       |
| Recoverable native Input                | Make one native short-lived Input retry-safe after response loss within the same daemon incarnation and report the exact applied PTY byte range.                                                | Rust public vertical; TypeScript parity; bounded review and focused Gate                                                                       |
| Public Local consumer (`f-22dczvf38`)   | Close only the exact-commit local embedding gaps without absorbing consumer semantics or Remote transport.                                                                                     | cumulative output byte cursors; public interrupt and complete process-tree Stop; exact-commit artifacts and required CI qualification           |

The earlier reliability-and-performance umbrella `f-226cz5zdq` is superseded
only after these successor plans and their dependency edges are materialized.
Completed evidence remains historical truth; unfinished work is not marked
done during the transfer. Competitive dominance is an aspiration, not a
correctness or release completion condition.

Each owner writes its own Feature-local `verification.md`. A release summary
may cite already closed successor evidence, but no shared mutable qualification
report is task truth for multiple Features. The performance Feature remains
`proposal_only` until explicitly scheduled, and no planning transition grants
authority to publish packages, Git refs, hosted releases, or benchmark results.

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

### M1 operation hardening — recoverable native Input

Close response-loss ambiguity for one independently valuable mutation before
considering a broader control surface. The operation binds a caller-retained
key to one daemon incarnation, Run, expected byte cursor, and exact non-empty
payload. Matching retry returns the original byte range without another
physical write; stale, conflicting, evicted, cross-incarnation, partial, or
ambiguous cases fail closed.

This milestone does not include durable Resize, arbitrary Signal, owned
process-group Stop, SSH transport, release work, or Agent message semantics.
Process-group Stop follows as a separate lifecycle Feature because its target,
grace, force, and quiescence rules are not Input's mutation algebra.

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

Status: implemented for declared references and Codex-native continuation;
workspace snapshots and artifact storage remain explicitly deferred.

Add portable Level A fork, then prove Level B with the first Agent Integration.
Keep capability requests explicit and fail closed.

Acceptance:

- Level A forks reproduce only the documented portable Run specification and
  declared inputs;
- each fork records parentage and declared context/artifact references;
- one Integration demonstrates a genuine Level B resume or fork;
- requesting Level B from a Level A-only Integration returns an explicit
  unsupported-capability result.

## M3.5 — Persistence and restart recovery

Status: implemented for the declared historical recovery class; live PTY
handoff and process adoption remain unsupported.

First accept a recovery contract that distinguishes durable metadata, replay,
and live PTY ownership. Then implement only the recovery class that can be
identified and proven without adopting a process by PID guesswork or moving Run
ownership into a client.

Acceptance:

- the persistence decision names which state survives daemon restart and which
  live-control claims remain unsupported;
- stored generations are committed and recovered atomically or fail with a
  typed corruption result;
- stale or ambiguous process identity never attaches to or signals an unrelated
  process;
- real restart fixtures prove the accepted recovery class and activate the
  applicable persistence wrong cases;
- retention, cleanup, and orphan policy are explicit for every persisted item.

## M4 — tmux adapter

Status: implemented under Feature `f-224czneed`; required minimum/current
version-lane qualification remains before archive.

Connect existing tmux panes through the public ctxmux protocol while keeping
native Run semantics and tmux implementation details separated.

Acceptance:

- ctxmux can list and attach to a selected live tmux pane, with the complete
  import identity tuple fenced against relocation, respawn, death, and server
  replacement;
- disconnecting the ctxmux client or daemon does not kill the tmux pane,
  session, or server;
- the implementation does not speak tmux's private client-server wire
  protocol;
- read-only controls, raw-since-import replay, source gaps, and memory-only
  import are capability-visible and documented;
- Control Mode corruption, target change, and server loss remain distinct;
- required Ubuntu tmux 3.4 and macOS current-version lanes prove the declared
  minimum/current qualification boundary without claiming every future 3.x
  release.

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
- broad Integration coverage beyond the one proven Level B path.
