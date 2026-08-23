# Implementation Roadmap

- Status: reviewed
- Review basis: completed user and Agent design discussion
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

The reliability program grew beyond one finite Feature boundary. Correctness,
release qualification, and peer performance are
independently closable; one result must not keep an unrelated owner open.

| Owner                                   | Finite closure                                                                                                                                                                                  | Reviewed task topology                                                                                                                         |
| --------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| Run-Kernel correctness (`f-22bczhydf`)  | Bound memory-only and persistent retained state, dispose of unclassified native waiter failure without inventing exit truth, close Kernel P0/P1 findings, and prove the retained-state plateau. | memory-only Registry GC; bounded waiter-failure disposition; persistent exact replacement; bounded Kernel review; retained-state qualification |
| tmux adapter (`f-224czneed`)            | Prove the declared public Control Mode adapter and preserve non-output observation truth across attachment lag.                                                                                 | tmux capability tasks only; no release or Kernel GC work                                                                                       |
| Composition and release (`f-225cz7943`) | Prove public composition, activation, installation, packaging, independent release review, supported platforms, and local release gates.                                                        | composition example; package/release preparation; daemon activation; release qualification                                                     |
| Peer performance (`f-22aczwza9`)        | Run one pre-registered, budget-bounded measure/optimize/remeasure cycle and record honest wins, ties, and losses.                                                                               | comparable harness; measured raw-byte ROI decision; bounded-cycle result                                                                       |
| Recoverable native Input                | Make one native short-lived Input retry-safe after response loss within the same daemon incarnation and report the exact applied PTY byte range.                                                | Rust public vertical; TypeScript parity; bounded review and focused Gate                                                                       |
| Public Local consumer (`f-22dczvf38`)   | Close only the exact-commit local embedding gaps without absorbing consumer semantics or Remote transport.                                                                                      | cumulative output byte cursors; public interrupt and complete process-tree Stop; exact-commit artifacts and required CI qualification          |

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

The frozen AgentMux consumer comparison exposed one
bounded native-owner gap before the broader peer cycle: a fresh daemon used a
host-sized Tokio worker pool, and every live native Run permanently owned one
reader thread plus one waiter thread. Three existing ctxmux resource censuses
independently reproduced approximately 142--145 KiB and two threads per live
Run. `f-22aczwza9/T-004` owns this consumer-triggered remediation before the
broader T-001--T-003 cycle: use a fixed small daemon worker pool and replace
per-Run permanent native workers with daemon-wide owners while preserving
ordered output, direct-child wait/reap authority, Stop, failure disposition,
shutdown, and existing resource budgets. It must not change output event
fan-out, protocol encoding, thresholds, compatibility behavior, or tmux
attachment semantics; those remain with their existing owners. The external
comparison activates the work but does not become a second ctxmux benchmark
truth or authorize publication.

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

This milestone does not include durable Resize, arbitrary Signal, SSH
transport, release work, or Agent message semantics. The later public-local-
consumer Feature owns foreground-group Interrupt and complete POSIX-session
Stop because their target, grace, force, and quiescence rules are not Input's
mutation algebra.

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
  writer, output-owner, and wait-owner registration transitions;
- every rejected launch leaves no live child, zombie, or Run in the manager;
- the fix stays inside the native launch owner and does not extract a public
  Backend framework, plugin surface, or general fault-injection system.

## M2 — Integration contract and first Agent

Keep explicit in-process Integration registration above the generic Run
lifecycle. The durable ctxmux surface owns the Provider-neutral contract and a
generic shell implementation. Agent-specific Provider modules belong to their
embedding product and materialize generic Run plans for ctxmux.

Acceptance:

- adding an Agent to an embedding product does not add Agent-specific fields to
  the daemon's foundational Run types or require a ctxmux protocol change;
- the host explicitly imports and registers Integrations;
- Provider-neutral detection, launch planning, and capabilities work through
  the TypeScript SDK;
- the raw Run remains usable when no semantic Integration observer is attached.
- removing a co-located Agent-specific module does not weaken standalone CLI,
  raw SDK, shell Integration, or Level A conformance.

## M3 — Context, artifacts, lineage, and fork

Status: declared references and Level A are implemented. The generic Level B
submission path exists; a Provider-neutral conformance fixture, workspace
snapshots, and artifact storage remain explicitly deferred.

Add portable Level A fork, then prove that a host can submit one fully
materialized Level B plan through the generic contract. Keep capability
requests explicit and fail closed.

Acceptance:

- Level A forks reproduce only the documented portable Run specification and
  declared inputs;
- each fork records parentage and declared context/artifact references;
- one host-owned Provider or Integration demonstrates a genuine Level B resume
  or fork without adding provider fields or parsing to the daemon;
- requesting Level B from a Level A-only Integration returns an explicit
  unsupported-capability result.

## M3.5 — Persistence and restart recovery

Status: historical recovery and planned exec-in-place continuity are
implemented. In persistent mode, [015](architecture/choices/015-exec-in-place-upgrade-continuity.md)
keeps the daemon PID, listener identity, live Run processes, PTY control,
ordered replay, and recoverable-Input truth across a planned `execve` upgrade.
Cold replacement, daemon crash, and host reboot still interrupt live Runs;
cross-process fd handoff and PID adoption remain unsupported.

[016](architecture/choices/016-interrupted-run-derivation.md) defines explicit
interrupted-parent derivation: ctxmux may clone a portable Level A plan or
execute a complete caller-materialized Level B plan, but it does not parse
Provider sessions, construct native-resume commands, or silently change
fidelity.

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
- a Level B request without host-owned provenance and a complete replacement
  `RunSpec` creates no Run and never falls back to Level A.

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

## Standalone Runtime convergence — Phase 1

Status: accepted for current-tree execution. The Feature Tracker owns live task
state; this roadmap owns delivery order and acceptance.

The phase closes ctxmux as a complete standalone Runtime product before adding
another execution location or Provider-adjacent metadata:

1. **T-000 — delivered baseline.** Bind the current daemon, CLI, SDK,
   persistence, tmux adapter, Level A fork, and planned exec-in-place upgrade to
   one final code/document gate and evidence receipt.
2. **T-001 / T0 — Integration boundary.** Keep Provider-neutral
   `Integration`/`RunRecipe` and shell conformance in ctxmux; remove any
   co-located Agent Provider, session parser, and native-resume implementation
   from the publication. Level B remains explicit and fail-closed.
3. **T-002 / T1 — Runtime identity and capabilities.** Publish a persistent
   `runtimeId`, a `daemonInstanceId` that changes on cold replacement but not a
   same-process planned exec, an explicit identity-persistence discriminator,
   serving-build target facts, and a flat numeric capability record. Clients
   may enforce exact local requirements after Hello and before business
   dispatch. `runtimeId` must not reuse the serving epoch: persistent mode
   binds it to the state-directory lineage, while memory-only mode promises
   stability only for one daemon lifetime.
4. **T-003 / T2 — authoritative Run observations.** Add Run state revision,
   owner-recorded UTC timestamps, and a typed observation envelope. State
   revision, output byte cursor, and delivery-gap position remain separate
   facts, including when an accepted interrupt does not change `RunState`.
5. **T-004 / T3 — race-free waits.** Provide lost-wakeup-safe Rust and
   TypeScript wait helpers over the existing attach-before-snapshot boundary;
   make CLI `wait` reuse the Rust helper. Results distinguish completion,
   timeout/cancel, output gap, and runtime replacement.
6. **T-005 / T4 — activation helper.** Provide a TypeScript-first
   connect-or-activate helper with readiness/Hello identity agreement,
   caller-supplied expected identity/build/capabilities, bounded cleanup of only
   the process it spawned, and client-only disposal. Higher products retain
   artifact pinning, private directories, and product policy.
7. **T-006 — clean-environment independence.** In an isolated environment with
   no Agent client or Provider CLI, install only ctxmux and prove daemon
   activation, start, detach/attach/replay, input, resize, wait, inspect, stop,
   identity/capability reporting, and portable Level A fork/restart through
   public CLI and SDK surfaces.

Phase 1 acceptance is executable and has zero higher-client dependency. An
external package consumer may add evidence, but cannot replace the standalone
gate or move its policy into ctxmux.

## Remote Runtime and bounded derivation — Phase 2

Status: reviewed proposal only; depends on complete Phase 1 evidence and must
not be mixed into the current implementation.

- **T5 — Remote Runtime transport.** Carry the same public protocol to an
  owner-host Runtime, preserving owner-side lifecycle truth and reporting
  transport loss as unverifiable. Do not introduce a hosted control plane,
  Relay, account/environment federation, or remote scheduling policy.
- **T6 — Provider-neutral derivation metadata.** Add a generic derivation kind
  only when a real consumer needs to distinguish fork/restart/resume beyond the
  existing parent/fidelity lineage. Provider session selection, transcript
  trimming, summaries, and native-resume construction remain outside ctxmux.

## Explicitly deferred

- a complete editor;
- a hosted control plane, Relay, account/environment federation, or remote
  scheduling platform;
- plugin discovery, marketplace, or untrusted plugin sandbox;
- arbitrary live-process state cloning;
- broad Integration coverage beyond the one proven Level B path.
