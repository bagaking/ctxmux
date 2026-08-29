# Verification Evidence

## Candidate
- Commit: `c05a8f1394f00334075a45adeb688d868c868da6`
- Tree: `569befa3ff1c9c214ba32c30de2d48775e4edd12`
- Source worktree: clean when the source-bound observe receipt was captured.

## Automated Checks
- Command: `scripts/check.sh`
- Result: The gate recorded `scripts/check.sh` as failed (command exit 101). The repository suites completed except `ctxmux-daemon --test tmux_adapter`; 30 of 31 tests passed, and `replacement_tmux_socket_path_invalidates_the_import_identity` returned `BackendUnavailable` because the tmux version helper exceeded its execution deadline. T-005 therefore remains gated as failed.
- Command: `scripts/check-reliability.sh`
- Result: Passed with exit 0 on the same candidate. The source-bound smoke qualification completed all required stages without changing the frozen budgets or owner accounting.
- Command: `scripts/check-reliability.sh --profile observe --observation-round 3`
- Result: Passed with source-bound evidence for the frozen idle/active × 1/32/128 census. Fresh-daemon and steady-state thread counts were both 5 in every cell; each Run added 0 permanent threads and 3 FDs. No live children or attachments remained after cleanup.

## Resource Census

| Mode | Runs | RSS slope (KiB/Run) | Threads/Run | FDs/Run | Cleanup retained Runs |
| --- | ---: | ---: | ---: | ---: | ---: |
| idle | 1 | 864 | 0 | 3 | 1 |
| idle | 32 | 68.5 | 0 | 3 | 32 |
| idle | 128 | 21.75 | 0 | 3 | 128 |
| active | 1 | 1376 | 0 | 3 | 1 |
| active | 32 | 144 | 0 | 3 | 32 |
| active | 128 | 52.75 | 0 | 3 | 128 |

## Manual Checks
- Step: Independent read-only review of the daemon-wide native owner, shutdown disposition, census interpretation, and resource accounting.
- Outcome: Reviewer found no P0/P1 finding. The ADR now describes bounded owner-loop shutdown, detachment of active blocking cleanup workers, and fail-stop retention when queued or watched child authority cannot complete; the implementation and regression coverage remain unchanged.

## Residual Risks
- The full repository gate is still blocked by the tmux adapter helper deadline failure above; a stable rerun and timeout diagnosis are required before T-005 can finish.
- Stop admission remains explicitly bounded: the eight cleanup-owner limit can reject a later Stop under pressure. This is observable backpressure, not a relaxed budget or an unrestricted admission claim.
- Observe and smoke receipts are local, source-bound evidence; raw runtime receipts are intentionally not committed because they contain machine-specific execution metadata. External consumer repinning and publishing remain out of scope.
