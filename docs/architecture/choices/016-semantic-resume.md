# 016 — Semantic resume of interrupted Runs

- Status: accepted, implementation pending
- Scope: bringing an agent back after its live process is genuinely gone
  (daemon crash, host reboot), in persistent mode

## Context

[009](009-runtime-persistence-recovery.md) recovers a prior `running` Run as
`interrupted` across a real daemon restart, and [015](015-exec-in-place-upgrade-continuity.md)
keeps live control across a *planned* upgrade. Neither covers the case where the
live process is genuinely gone — a daemon crash or a host reboot — because a dead
process holds no master fd to carry and PID re-adoption is forbidden.

Yet the durable object is the Run, and an operator whose host rebooted overnight
reasonably expects to bring the agent back. A recovered `interrupted` Run today
supports `list`, `status`, `attach` (to its retained tail), and Level A fork,
but there is no way to *continue* it. Two things block continuation:

- there is no `resume` verb; and
- Level B continuation (provider-native resume, e.g. Codex `exec resume`) is
  doubly blocked on a recovered Run — it requires live continuation authority
  the recovered Run does not hold, and the session provenance it needs lived
  only in a live in-memory `WeakMap` that is empty after a restart.

The physics is the boundary. "The process survived" is not "live control
survived": live control is possession of the master fd plus authority to
`waitid` the child, and after a crash neither exists. So resume here is
*semantic* — reconstruct the agent's continuation into a new process — never a
re-attachment to the dead one.

## Decision

Add one explicit, operator-driven `ctxmux resume <run-id>` verb that
reconstructs an `interrupted` Run into a new Run, with lineage recorded back to
the interrupted one. There is no auto-respawn and no daemon-initiated restart;
resume is a deliberate act, mirroring the existing manual `fork`. It reuses the
existing start and fork primitives rather than adding a new subsystem.

### Level A path — process back, no conversation continuity

Clone the recovered Run's `RunSpec` and start it fresh. This is already fully
supported: a Level A fork of a recovered parent needs no continuation authority
because it makes no claim to continue the prior conversation — it re-runs the
same command. The result is honest: the process is back, the prior in-agent
conversation is not.

### Level B path — provider-native continuation

Level B continues the *agent's* conversation through the provider's own resume
mechanism. On a recovered Run it is unblocked without weakening the
live-authority gate, by supplying the two missing inputs from durable state
instead of from live memory:

- **Re-derive provenance from durable replay.** The integration observer that
  extracts a provider session id from live output (the Codex observer parses
  `thread.started` / sessionId from the output JSONL) is re-run over the Run's
  *durable retained replay*. The session id that was captured live is thus
  recovered from persisted bytes, not from the emptied `WeakMap`.
- **Model resume as a Start of a materialized resume spec.** With provenance in
  hand, resume is expressed as a plain `start` of the fully materialized
  provider-resume command (e.g. `codex exec resume <session-id> …`), with
  lineage recorded to the interrupted Run. This sidesteps the live-authority
  gate honestly rather than bypassing it: a materialized spec plus durable
  provenance is exactly the declared, inspectable Level B contract — the same
  contract a live Level B fork satisfies, reached from persisted inputs.

If provenance cannot be re-derived (no session id in the retained replay), Level
B is unavailable for that Run and resume degrades to the Level A path with that
fact surfaced, never silently.

## Quality attributes and invariants

- Resume is explicit and operator-driven; the daemon never respawns a Run on its
  own.
- A resumed Run is a new Run with recorded lineage to the interrupted one; the
  interrupted Run's identity, retained output, and terminal state are not
  mutated by resume.
- Level B provenance is re-derived only from durable retained replay, never from
  live in-memory state, so it works identically after a cold restart.
- Level B never claims live continuation authority over the dead child; it
  materializes a fresh provider-resume spec and starts it.
- When provenance is unavailable, resume falls back to Level A and says so; it
  never fabricates a session id or presents Level A as Level B.
- No live PTY handoff, fd transfer, or PID re-adoption of the old child occurs —
  those remain unsupported per 009.

## Alternatives

- **Automatic resume on recovery.** Rejected: turns a durable-state tool into a
  process supervisor, resurrecting work the operator may have abandoned and
  spending resources without consent. Resume is a deliberate verb.
- **Weakening the Level B live-authority gate for recovered Runs.** Rejected:
  the gate is a correctness boundary. The right move is to *satisfy* the
  contract from durable inputs (materialized spec + re-derived provenance), not
  to remove the check.
- **Persisting the provenance `WeakMap` as new daemon state.** Rejected as
  over-design: the session id is already in the durable replay the daemon
  retains, so re-deriving it on demand adds no new persisted subsystem. This
  keeps entropy down — the SSOT for provenance stays the output the agent
  actually emitted.
- **A dedicated resume wire operation distinct from start.** Rejected: resume is
  a materialized start plus lineage; a separate frame would duplicate the start
  contract and force a protocol generation bump for no new capability.

## Known constraints

Semantic resume depends on the provider actually supporting resume; for a plain
shell there is no conversation to continue and only the Level A path is
meaningful. Level B fidelity is bounded by what the provider's resume mechanism
restores from its session id — ctxmux re-supplies the id, not the provider's
internal state. Provenance re-derivation is only as complete as the retained
replay; if the `thread.started` marker was truncated out of the retained tail,
Level B is unavailable and the fallback applies.

## Wrong-case corpus

To be populated during implementation. Anticipated cases: a recovered Run whose
retained replay no longer contains the session marker (must fall back to Level
A, not fabricate); a resume of a Run that is not actually `interrupted` (must be
rejected, not double-started); a provider that reports the session id in a
format the observer parsed live but truncated in retention; lineage recorded to
an interrupted Run that is itself a resume of an earlier one (chains must remain
inspectable).

## Fixture mapping

- Future: after a real kill-and-restart, the Run is `interrupted`; `resume`
  produces a new Run with lineage to it.
- Future: Level B resume of a recovered Codex Run re-derives the session id from
  durable replay and materializes `exec resume`, with no reliance on live memory.
- Future: resume of a recovered Run whose replay lacks the session marker falls
  back to Level A and surfaces that it did.
- Future: resume of a non-`interrupted` Run is rejected.

## Open questions

- Should resume optionally verify the resumed provider session actually attached
  to the prior conversation, or is materializing the spec sufficient?
- Should a resumed Run's lineage distinguish "resume" from "fork" in status
  output, or is a single lineage edge with a kind tag enough?

## Repository evidence

- `crates/ctxmux-daemon/src/lib.rs`: recovered-Run construction with
  `incarnation_control: None` / `native_runs: None`, the Level A fork clone
  path, the Level B `has_continuation_authority` gate, and `native_control()`
  rejecting historical Runs — the seams the resume verb routes around.
- `crates/ctxmux/src/main.rs`: the command set (no `resume` verb yet) and the
  interrupted-Run advice string.
- `packages/sdk/src/integrations/codex.ts`: `codex exec resume --json`, the
  Level B fork provenance, and `thread.started` / sessionId detection to re-run
  over durable replay.
- `packages/sdk/src/integration.ts`: the in-memory provenance `WeakMap` that is
  empty after restart and is replaced here by durable re-derivation.
- `crates/ctxmux-daemon/src/persistence.rs`: recovered-Run loading and retained
  replay, the durable source the provenance is re-derived from.
- `docs/plans/2026-08-22-daemon-upgrade-continuity-design.md`: full design,
  Track B section.
