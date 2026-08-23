# Revision 17 convergence correction

## Verdict

The prior closure plan protected product correctness but made the qualification
surface larger than the Run Kernel: T-030 required a daemon-private metrics
sink, 512 MiB replay-pressure matrix and 30-minute soak; T-006 required a
domain-wide review program; T-007 required release and platform qualification.
Those are valuable programs, but they are not the smallest proof that the
already shipped 128-record memory and persistent retention owners converge.

The user explicitly chose fast convergence, low project entropy, and reasonable
TDD over exhaustive verification. Revision 17 therefore supersedes T-030,
T-006, and T-007 with one final bounded task. It keeps existing tests, frozen
contracts, and historical receipts intact, but does not build new metrics,
benchmark, soak, release, or review infrastructure in this Feature.

## Required proof

One deterministic ordinary test must exercise three complete turnover windows
through both memory-only and persistent owners at a reduced private capacity.
It must prove the exact retained Run/key set, same-key retry without another
physical child, and persistent restart convergence. The change then receives
one independent review focused on the changed retention boundary and passes
`scripts/check.sh` once from a clean source revision.

P0/P1 findings in the changed boundary remain blocking. P2 cleanup, maximum
replay pressure, long soak, all-platform qualification, release packaging,
tmux completion, and broad public-claim re-review remain outside this Feature.
