# Verification Evidence

## Source Identity

- Feature/task: `f-224czneed/T-001`; status is `blocked` with reason class `external_blocker`, not done or archived.
- Exact local HEAD: `b4dcbb355a5940eea839d70dd6736a8d472b4737`.
- Exact tree: `b27dc9cfce3ea66f2eb1bb71337b57d57c26159b`.
- Parent correction commits:
  - `1f4f931b4b43495020df674e85631b4687f1da40` — bounded short-lived tmux operations.
  - `c66916680c8a940343dbe7ddfe751f027a0660ca` — explicit Control stream state and bounded serial correlation.
  - `e6ceb47dd8ad1ccce8012529583da996a6506950` — single terminal-cause arbitration owner.
  - `b4dcbb355a5940eea839d70dd6736a8d472b4737` — terminal Control resource release and FD-slope evidence.

## Automated Checks

- Formal Feature Tracker Gate round 4 ran `scripts/check.sh` on the clean exact HEAD and the current policy-compliant verification artifact above, then passed with exit 0.
- Gate receipt: `.bagakit/feature-tracker/features/f-224czneed/artifacts/gate-T-001-r4-0001.log`.
- Gate receipt SHA-256: `d3b7054da9c212e9e596b17d45ce4fed6a59576299af0f639b65b8036afdc8d9`.
- The Gate wrapper receipt records command/result but does not embed Git identity; this document binds that receipt to the clean commit/tree observed immediately before and after the round.
- Resource correction checks passed on the final diff: Rust formatting, diff check, daemon Clippy with warnings denied, 50 daemon unit tests, 31 tmux adapter tests, Prettier for all changed evidence files, the 36-case wrong-case corpus, CI reachability for 27 suites across two required jobs, and 17 fixture/reachability validator tests.
- Tracker validation and the current execution-owner receipt passed after Gate round 4.

## Independent Reviews

- Immutable audit of `e6ceb47dd8ad1ccce8012529583da996a6506950` passed with no P0-P2 finding; exact parent, tree, two-file scope, message facts, deterministic child-first oracles, and Feature boundaries matched.
- Immutable audit of `b4dcbb355a5940eea839d70dd6736a8d472b4737` passed with no P0-P3 finding; exact parent/tree, seven-file scope, Option-writer ownership, Linux/macOS FD oracle, TMUX-03 local-regression attribution, message facts, and Feature boundaries matched.
- Review corrections were rechecked by the reviewer that raised them: stdout-holder barriers now prove child-exit-before-EOF, and TMUX-03 distinguishes upstream `l03` evidence from the ctxmux-local dead-writer regression.

## Manual Checks

- Step: Reconstruct each correction from its exact commit, parent, tree, diff, commit message, and repository source-of-truth documents.
- Outcome: Independent immutable audits accepted `e6ceb47` and `b4dcbb3` without blocking findings and confirmed no Feature, protocol, native Run, Backend/Integration, or Agent-scope drift.
- Step: Inspect the remote branch and GitHub Actions evidence for the exact final commit.
- Outcome: No required Ubuntu tmux 3.4 or macOS-current receipt exists for `b4dcbb3`; the only observed remote run belongs to old commit `b2bbc7a` and is not acceptable evidence.

## Failed-Round History

- Gate round 2 recorded `scripts/check.sh` exit 101 in `.bagakit/feature-tracker/features/f-224czneed/artifacts/gate-T-001-r2-0001.log` (SHA-256 `17d79111fe3e65e79c57edafeeadb7faacb5fe35ff4c90751c0bff65342f1a74`).
- The wrapper did not retain the failing subcommand output. An immediate direct `scripts/check.sh` rerun passed, so the exact round-2 failure was not recoverable from its receipt and is not relabelled as a proven cause.
- Independent review nevertheless found and reproduced two archive-blocking owner defects: child-exit could outrun transcript classification, and terminal tmux history retained one dead Control writer FD. Commits `e6ceb47` and `b4dcbb3` close those defects with deterministic public oracles; formal round 4 then passed with the current verification headings in place.

## Residual Risks

- Required Ubuntu tmux 3.4 and macOS-current CI receipts do not exist for this exact source. The local branch is ahead of `origin/main`, which still points to `b2bbc7a219753ad2664a438ab89347df180b7d31`.
- The only observed remote CI run is for the old `b2bbc7a` workflow and has no required tmux version lanes; it is not acceptable T-001 evidence.
- Publishing a branch, opening a PR, pushing main, rerunning Actions, or otherwise mutating GitHub was not authorized and was not performed.
- T-001 must remain externally blocked; resume it only after publication authority is available, and do not finish or archive the Feature until the exact required remote lanes record their server versions and complete the canonical Gate.

## Deliberate Non-Goals

- No protocol generation, tmux capability profile, native Run semantics, daemon admission policy, public Backend abstraction, Agent/Integration semantics, Run GC, global quota, or broad CPU/memory optimization changed in these corrections.
