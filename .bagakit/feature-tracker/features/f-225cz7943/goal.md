# Feature Goal: Prove composition and prepare release

Contract: `bagakit.feature-goal.v1`
Feature: `f-225cz7943`
Convergence: `terminal`

Before acting, verify `owner-receipt.json`, then recover current execution from
`state.json` and `tasks.json`. Do not begin release execution until Kernel and
tmux dependencies satisfy their declared capability boundaries.

## Prime Directive

Prove that ctxmux composes through public APIs and is reproducibly installable,
activatable, reviewable, and release-ready without moving orchestration into
the mux or publishing externally without explicit authority.

## Protected Invariants

- Composition policy remains client-owned; ctxmux stays a Run multiplexer.
- Activation converges on one compatible local daemon without coupling Run
  lifetime to the activating process or replacing unrelated socket owners.
- Package, platform, and release claims describe only shipped behavior and are
  proven from clean consumers and supported environments.
- Release work does not absorb Kernel correctness, tmux product completion, or
  peer-performance optimization.
- No registry publish, Git push, hosted release, credential mutation, network
  download, or external cost occurs without explicit authorization.

## Convergence Contract

The Feature closes when public composition, activation, installation,
packaging, independent release review, supported-platform qualification, and
release gates pass from one exact source revision. Peer benchmark outcomes do
not affect closure.

Stop and ask before choosing or changing a project license, publishing any
artifact, changing compatibility promises, using credentials, or incurring
external cost.

## Context References

- `AGENTS.md`
- `README.md`
- `docs/vision.md`
- `docs/architecture.md`
- `docs/roadmap.md`
- `docs/testing-strategy.md`
