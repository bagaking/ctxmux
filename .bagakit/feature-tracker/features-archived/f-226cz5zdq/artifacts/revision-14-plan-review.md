# Revision 14 convergence review

## Verdict

The prior plan is program-sized and cannot converge as one Feature Goal. It
mixes finite Run-Kernel reliability closure with tmux product completion,
release packaging, open-ended performance optimization, and a requirement to
beat every comparable peer benchmark. Revision 14 keeps all completed evidence
immutable, replaces blocked T-027 with three independently closable retained-
state verticals, and routes adjacent outcomes to their existing or dedicated
Feature owners.

## Closure classification

This Feature is `terminal` with a `state` oracle: retained-state ownership is
bounded in memory-only and persistent modes, its sustained qualification is
green, independent review has no open P0/P1 finding, and the final reliability
qualification passes. Wins, ties, or losses against tmux and Zellij do not
change this closure.

## Task topology

- T-028 implements the accepted memory-only Registry ceiling and collection
  boundary.
- T-029 closes persistent retained-state admission and exact replacement
  without inventing a general transaction or weakening WAL safety.
- T-030 qualifies both modes under the frozen churn and resource contract.
- T-031 routes tmux work to `f-224czneed`, activation/release work to
  `f-225cz7943`, and bounded performance work to proposal `f-22aczwza9`.
- T-006 and T-007 remain the independent review and final reliability closure.

T-028, T-029, and T-030 jointly supersede blocked program-sized T-027. T-031
supersedes the duplicated or frontier tasks T-011, T-014, T-005, T-015, and
T-008. No accepted product outcome is deleted; it receives one smaller owner.

## Entropy rule

No new task enters this Feature unless it is required to satisfy the terminal
state oracle. Adjacent findings route to another Feature. P2 cleanup may be
recorded as accepted residual risk; only P0/P1 findings required by shipped
claims reopen implementation. Among valid fixes, prefer the fewest lasting
states, owners, APIs, duplicated truths, and temporary qualification surfaces.
