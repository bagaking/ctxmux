# T-031 ownership split review

## Verdict

The reliability-and-performance umbrella is not a stable closure boundary.
Its original concise Feature goal still makes peer benchmark wins a release
condition, while the accepted revision-14 Goal and plan correctly define a
finite Run-Kernel closure. The public Tracker surface cannot rewrite that
original concise identity in place.

Use three successor owners instead of another umbrella revision:

- a new Run-Kernel correctness Feature owns memory-only retained-state GC,
  permanent native waiter failure, persistent exact replacement, bounded
  Kernel review, and retained-state qualification;
- existing Feature `f-225cz7943` owns composition, activation, packaging,
  independent release review, platform evidence, and release gates;
- proposal `f-22aczwza9` owns one budget-bounded peer-performance cycle whose
  honest wins, ties, and losses do not block Kernel or release closure.

Feature `f-224czneed` remains the only tmux completion and non-output
observation-loss owner. No successor duplicates that work.

## Migration invariants

- Preserve every completed f-226 Task and blocked T-027 receipt as immutable
  history; never mark the umbrella done.
- Materialize and review all successor plans before closing the umbrella.
- Close f-226 only as `superseded`, with the new Kernel Feature as primary
  replacement, after dependent Features point at the successor.
- Do not activate the performance proposal or publish packages, registries,
  Git refs, or hosted releases during routing.
- Do not rewrite historical archives that fail a newer Tracker schema; that is
  an independent harness migration issue.

## Review basis

- `AGENTS.md`
- `docs/vision.md`
- `docs/architecture.md`
- `docs/protocol.md`
- `docs/roadmap.md`
- `docs/testing-strategy.md`
- `docs/architecture/choices/013-retained-run-resource-governance.md`
- `.bagakit/feature-tracker/features/f-226cz5zdq/goal.md`
- `.bagakit/feature-tracker/features/f-226cz5zdq/tasks.json`
- `.bagakit/feature-tracker/features/f-224czneed/tasks.json`
- `.bagakit/feature-tracker/features/f-225cz7943/tasks.json`
- `.bagakit/feature-tracker/features/f-22aczwza9/state.json`

