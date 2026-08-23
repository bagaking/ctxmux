# Revision 15 convergence review

## Verdict

Revision 14 correctly removed open-ended and duplicated outcomes, but its
routing Task exposed a reproducible persistence lifecycle regression under the
required full repository Gate. The failure is not allowed to disappear behind
a planning-only transition or a test retry.

T-032 supersedes blocked T-031. It preserves the completed ownership routing
and adds exactly one required P0/P1-class correction: restore default-
concurrency persistence lifecycle correctness before retained-state GC work
continues. No unrelated cleanup, timeout relaxation, or new abstraction enters
the closure.

## Failure evidence

- Two Feature Tracker Gate attempts failed in `scripts/check.sh` and remain in
  the T-031 gate logs.
- The complete `persistence_recovery` binary reproduced an exit-code mismatch
  in `exited_run_recovers_metadata_replay_terminal_controls_and_level_a_fork`
  and a terminal-state timeout in
  `persisted_replay_prunes_to_the_exact_per_run_budget_and_recovers_the_tail`.
- The heavy replay test passed when run alone, so the correction must preserve
  default-concurrency behavior rather than merely increasing its timeout or
  serializing the repository Gate.

After T-032, the revision-14 terminal topology remains unchanged: T-028,
T-029, T-030, T-006, and T-007 are the only remaining closure tasks.
