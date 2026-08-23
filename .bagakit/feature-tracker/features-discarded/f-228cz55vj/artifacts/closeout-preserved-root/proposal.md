# Feature Proposal: f-228cz55vj

> **Superseded (2026-08-23):** The two-process transactional direction below was
> reconsidered after the Herdr comparison
> (`f-226cz5zdq/artifacts/herdr-transfer-review.md`) and **replaced by
> exec-in-place** in `docs/architecture/choices/015-exec-in-place-upgrade-continuity.md`.
> ctxmux's SQLite-backed replay does not need Herdr's process-coexistence fd
> transfer, so `execve`-in-place is chosen over spawn + `SCM_RIGHTS`. The accepted
> cost is no mid-transaction rollback to a live predecessor (ADR-015 →
> Alternatives). The invariants below other than pre-commit rollback
> (exactly-one-owner, unchanged Run/PID/PTY identity, ordered output, explicit
> disposition for crossing controls) remain binding and are implemented as the
> A11 requirements in `docs/plans/2026-08-22-daemon-upgrade-continuity-implementation.md`.
> This proposal is retained for its acceptance-boundary and Transfer Checks,
> which ADR-015 inherits.

## Why

- Client churn is already survivable, but replacing the owning daemon currently ends live PTY/process authority. Persistent history correctly recovers the old row as `interrupted`; it does not pretend the process is still controllable.
- Controlled binary replacement is narrower than crash recovery, reboot, or PID adoption. A transactional FD handoff may preserve the exact Run without weakening the fail-closed restart model.
- This is a transparent Run-kernel optimization, not Agent Session recovery. AgentMux may independently choose Provider-native resume after an interrupted Run; ctxmux must neither request nor infer that semantic action.

## Goal

- After the current release, determine and prove whether compatible local ctxmux daemon incarnations can transactionally transfer live PTY and child-process authority with one owner, rollback before commit, unchanged Run identity, ordered output continuity, explicit interruption of transient work, and no claim of crash, reboot, remote migration, or Agent recovery.

## Principle Layer

- What: transfer one live Run's current-incarnation control authority between compatible local daemons without restarting or adopting its child.
- Why: planned upgrades should eventually preserve a durable Run while maintaining exactly one process/PTY owner and one ordered output history.
- Intended generalization: controlled local upgrades and coordinated daemon replacement on supported Unix platforms.
- Failure boundary: not crash recovery, host reboot continuity, arbitrary process adoption, remote migration, process-memory copying, Provider-native resume, or preservation of arbitrary in-flight requests.
- Behavior examples:
  - the old daemon quiesces new controls, resolves accepted controls, and transfers one validated PTY master plus a bounded manifest;
  - the replacement validates build/protocol compatibility, Run identity, process identity, FD set, size, cursors and capabilities before ready;
  - failure before commit restores the old owner; after commit the old daemon can never resume authority.
- Evidence refs:
  - `docs/architecture.md`
  - `docs/protocol.md`
  - `docs/architecture/choices/004-run-lifecycle-concurrency.md`
  - `.bagakit/feature-tracker/features/f-226cz5zdq/artifacts/herdr-transfer-review.md`

## Scope

- In scope: ADR and Linux feasibility proof; supported-platform capability; quiesce/validate/ready/commit/rollback transaction; PTY and child identity; RunId, size, output/replay and lifecycle continuity; explicit transient-operation interruption; failure injection and resource bounds.
- Out of scope: crash/reboot recovery, PID adoption, Level C fork, checkpointing, remote host migration, Windows emulation, compatibility fallback, Agent state, AgentMux Semantic Session policy, and arbitrary in-flight request preservation.

## Acceptance Criteria

- A reviewed ADR and feasibility fixture prove that the OS/runtime can transfer the required authority before an executable Task plan is installed.
- The same `RunId`, PID, PTY, terminal size, capabilities and output sequence remain authoritative after commit; consumers observe reconnect to the same Run, not a synthetic exit/restart pair.
- At every injected phase failure exactly one daemon can read, write, resize, stop or close the PTY; no path leaves dual owners or an unowned live FD.
- Accepted input is drained or rejected with known disposition; unknown input is never replayed. Attachments and transient requests receive explicit reconnect/interrupted outcomes.
- Unsupported platform, incompatible build/protocol, wrong manifest, process mismatch or FD mismatch fails before quiescence or rolls back before commit; no fallback restarts or adopts a process.
- Output/replay has no duplicate, silent gap or false full-history claim, and pause time plus FD/RSS/task cleanup is bounded for one, many and failed transfers.
- Integration and Agent metadata remain opaque. AgentMux decides reattach/resume/unavailable from public Run facts; ctxmux does not select or launch a Provider session as part of handoff.

## Transfer Checks

- Unexpected old-daemon death still produces restart interruption; the handoff protocol cannot be invoked retroactively to claim crash authority.
- Failure immediately before commit restores the old owner; failure immediately after commit cannot make the old owner resume.
- A client control crossing quiescence has one evidenced result—accepted/applied or rejected—never guessed or duplicated.
- Non-native, imported tmux, unsupported Backend and unsupported platform targets fail through capability semantics.
- If AgentMux's UI restarts while the Run remains live, ordinary attach already suffices; if the daemon dies without a completed handoff, AgentMux may Provider-resume only under its own verified Semantic Session policy.

## Impact

- Code paths: future daemon ownership transfer, PTY control owner, replacement handshake, attachment interruption and platform FD transfer.
- Tests: transaction state units plus real daemon/process/PTY, phase failures, ordering, identity and resource census.
- Rollout notes: remain proposal-only and post-release. Do not install executable Tasks until the ADR and Linux feasibility evidence satisfy the acceptance boundary.
