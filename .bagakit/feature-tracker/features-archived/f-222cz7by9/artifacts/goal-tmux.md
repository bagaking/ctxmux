# Feature Goal: Integrate tmux sessions

Contract: `bagakit.feature-goal.v1`
Feature: `f-224czneed`

Before acting, verify `owner-receipt.json`, then recover current execution from `state.json` and `tasks.json`. Context may be stale or belong to another Feature; trust this Feature directory before acting.

## Prime Directive

Expose existing tmux-owned sessions through ctxmux public Run surfaces so clients gain a stable embeddable view without ctxmux pretending to own or reimplement tmux.

## Protected Invariants

- Integration answers what runs; Backend answers where and how it runs; tmux behavior does not enter Agent-specific foundational types.
- Use the tmux executable or Control Mode and documented public behavior, never the private tmux client-server socket protocol.
- Detach or client death does not terminate the tmux session, and ownership differences from native Runs remain capability-visible.
- Control framing, byte decoding, lag, teardown, and supported-version behavior fail explicitly and preserve raw output honesty.
- Extract no public Backend framework until the real native/tmux duplication proves one is necessary.
- Non-goal: cloning tmux, reproducing its layout UI, scheduling Agents, or changing native Run persistence.

## Acceptance And Stop Rules

- Acceptance: real tmux-session and transcript evidence proves discovery, attach, output, disconnect, and ownership semantics through public ctxmux clients.
- Insufficient: parsing one happy-path command output, mocking tmux ownership, or coupling clients directly to tmux does not count.
- Stop and ask before: using private protocols, broadening the supported tmux/version matrix without evidence, publishing externally, or taking destructive action against existing user sessions.

## Authority And Orchestration

- Follow only this Feature's owner receipt, state, and reviewed tasks.
- Keep parser fixtures deterministic and use real tmux only for lifecycle claims that transcripts cannot prove.
- Delegation may review protocol transcripts, but the primary Agent owns session-safety evidence and final integration.

## Context References

- `AGENTS.md`: repository architecture and simplicity rules.
- `docs/architecture.md`: Backend and Integration separation.
- `docs/architecture/choices/012-tmux-control-mode-backend.md`: owning adapter decision and wrong cases.
- `docs/roadmap.md`: M4 acceptance.
