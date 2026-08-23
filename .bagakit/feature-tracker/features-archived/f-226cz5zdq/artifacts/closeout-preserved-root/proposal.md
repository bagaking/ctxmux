# Feature Proposal: f-226cz5zdq

## Why
- Ctxmux already has strong real-daemon evidence for its native happy path, but
  public lag recovery, stop identity, interactive PTY restoration, socket
  races, concurrency, chaos, sustained load, leak freedom, cross-platform
  reach, and performance have incomplete or absent qualification.
- Mature peers each prove only part of the surface. Ctxmux can combine their
  best patterns and close only on stronger, reproducible evidence.

## Goal
- Turn every shipped ctxmux capability into high-confidence evidence across correctness, chaos, stress, concurrency, security, resources, platforms, and performance, then close only after independent functional review and reproducible peer benchmark wins.

## Principle Layer
- What: Treat release confidence as an invariant-to-evidence system spanning
  correctness, failure, interleaving, load, resources, platforms, and
  performance.
- Why: A Run mux can appear healthy while losing bytes, signalling the wrong
  process, leaking resources, corrupting terminal state, or collapsing under
  fan-out; line coverage and ordinary happy-path tests cannot exclude those
  failures.
- Intended generalization: Apply the evidence bar to every implemented ctxmux
  capability, public client, supported platform, and external Backend before
  that capability contributes to release claims.
- Failure boundary: Do not require renderer/UI evidence, Agent orchestration,
  unimplemented capability fixtures, public fault-injection APIs, or benchmark
  comparisons between products that do not share the measured semantics.
- Behavior examples:
  - Force subscribe/snapshot and stop/wait races with barriers, then cross the
    real public boundary for the final oracle.
  - Run replayable chaos, soak, resource census, security, and performance
    workloads in named CI lanes.
  - Pre-register peer workloads and win rules before optimization or result
    inspection.
- Evidence refs:
  - `docs/testing-strategy.md`
  - `docs/architecture.md`
  - `docs/roadmap.md`

## Scope
- In scope: P0 owner-boundary gaps; coverage and platform gates; deterministic
  concurrency/fault seams; fuzz, chaos, stress, soak, security, and leak
  qualification; reproducible peer benchmarks; independent multi-Agent
  functional review; final release evidence.
- Out of scope: New product capabilities, renderer correctness, Agent Harness
  behavior, external publishing, compatibility layers, and speculative tests
  without an implemented contract or executable oracle.

## Acceptance Criteria
- Every shipped public guarantee maps to a gate-reachable oracle at its owning
  boundary on every supported platform.
- The reviewed reliability matrix passes with no unexplained skip, retry,
  leak, gap, P0 finding, or P1 finding.
- Independent Agents cover every functional domain and re-review every
  material correction.
- Ctxmux wins every pre-registered applicable peer benchmark under the same
  reproducible harness; losing metrics keep the Feature open.

## Transfer Checks
- Reject a release where percentage coverage passes after a real PTY or
  lifecycle fixture is removed.
- Reject a benchmark win created by deleting a losing workload, changing the
  semantics, or weakening correctness after results are known.
- Reject a review that assigns many Agents but leaves a functional domain,
  public claim, or material finding without an independent owner and closure
  evidence.
- Do not create a failure for an explicitly unsupported platform or
  unimplemented capability; keep the boundary visible instead.

## Impact
- Code paths: daemon lifecycle and replay, protocol and clients, SDK and
  Integrations, CLI PTY handling, persistence and tmux paths when shipped, CI,
  benchmark harness, and evidence reports.
- Tests: PR-critical real-system tests plus scheduled fuzz, sanitizer, chaos,
  stress/soak, security, leak, platform, and benchmark lanes.
- Rollout notes: Build the evidence system incrementally. Keep heavy lanes out
  of the ordinary local gate while retaining critical owner-boundary tests on
  every pull request.
