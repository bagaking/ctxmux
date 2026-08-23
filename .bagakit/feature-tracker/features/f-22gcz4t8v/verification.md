# Verification Evidence

## Automated Checks

- Command: `scripts/check.sh`
- Result: passed from a clean tree at
  `584dc8aa6c3d9bff343fb343a5a5bcf0d76cb484`. The gate covered Rust and
  TypeScript formatting, static analysis, unit and integration suites, the real
  CLI/SDK daemon vertical, local artifact consumption, protocol wrong cases,
  and the repository reliability smoke qualification.
- Baseline retry evidence: the first clean run at `d4ab09b` exposed two tmux
  deadline flakes; both exact tests passed immediately. A second full run
  reached the formatting gate and found the new roadmap row non-canonical.
  Commit `584dc8a` contains only that formatter result. The next full clean run
  passed, so no Stop behavior was hidden by the retry.
- Gate determinism correction: a later canonical run reproduced a pre-existing
  unit-test race between receipt visibility and the input drain clearing its
  planned-exec crossing flag. The three affected handoff tests now wait for the
  owner-defined handoff-ready boundary with a two-second deadline instead of
  assuming that receipt delivery drains the request gate. The formerly failing
  completed-ledger case passed ten consecutive exact runs; the unknown-ledger
  and crossing-operation cases also passed exactly, with daemon clippy clean.

## Manual Checks

- Step: inspect the generation-10 short request, attachment frame, Rust client,
  TypeScript SDK, daemon dispatch, native owner, and public Stop tests.
- Outcome: both public paths accept only a Run ID (plus an attachment-local
  command ID), enter the existing complete-session Stop owner, and return
  `graceful` or `forced` only after direct-child reap and owned POSIX-session
  quiescence. `Open -> Stopping -> Closed` fences later mutations; repeated
  ordinary Stop is `invalid_run_state`, not receipt recovery.
- Step: compare `docs/protocol.md`, `docs/architecture.md`,
  `packages/sdk/README.md`, and the Runtime capability manifest with the code.
- Outcome: the SSOT consistently states that lost ordinary Stop results are
  `unknown`, attachment command IDs are connection-local, and recovery does
  not generalize from Input to Stop. `native.recoverable_stop` is not yet
  advertised. The roadmap now owns only the approved future delivery boundary.

## Residual Risks

- The accepted implementation gap is unchanged: response loss after Stop
  admission cannot be settled by a fresh connection, while blind replay would
  re-enter a terminal Run and lose the original receipt.
- The existing Stop ownership limit remains unchanged: descendants that enter
  another POSIX session with `setsid()` are outside the claimed scope, and the
  documented PID revalidation syscall gap retains a small TOCTOU risk.
