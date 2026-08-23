# Revision 13 task-plan review

## Verdict

Approved after correction. The revision adds two owner-specific tasks without
changing the product model: T-026 corrects the T-012 unpublished-child rollback
boundary, while T-027 introduces bounded retained-Run/key governance before
final reliability qualification.

## Independent review findings applied

- Removed T-012 from the current plan and preserved its execution evidence as
  superseded history owned by T-026.
- Required terminal-and-reaped proof from the child-handle waiter rather than
  `RunState`, elapsed time, cached PID, or logging.
- Allowed unresolved cleanup to transfer into a bounded daemon-private owner
  with an exact-key fence, so unrelated hash stripes and global launch permits
  do not become permanent cleanup locks.
- Made unresolved cleanup globally bounded, shutdown-visible, and protected
  from later GC.
- Defined one hard global Run-record ceiling. Total retained replay is bounded
  by that ceiling and the existing per-Run replay contract; no second byte
  accounting path is added to live output solely for this task.
- Required lookup-to-pin linearization, exact candidate fencing, SQLite
  pre-/post-COMMIT behavior, infallible exact Registry removal, immutable
  descendant lineage, and same-key retry-before-parent lookup.
- Required memory-only and persistent churn across at least three collection
  windows with pre-registered CPU, RSS, retained-state, thread, descriptor, and
  owner-count plateau evidence. Canonical nightly evidence remains 1,800
  seconds; shortened soaks and frozen-budget changes are not completion proof.

## Scope and drift check

Run remains the universal object. Backend and Integration stay separate. The
plan adds no Session identity, persistent pending tombstone, process-tree
policy, public Backend hierarchy, scheduler, SSH transport, runtime identity
negotiation, or Agent semantics.

The output-centric Gap finding is not duplicated as a reliability
implementation task. Its implementation owner remains tmux Feature
`f-224czneed`; reliability T-011 only requires that correction before archive.

## Validation

The reviewed candidate preserves every executed task semantic field, removes
T-012 with explicit T-026 supersession lineage, uses repo-relative verification
refs, and preserves the archived persistence Feature only as a top-level source
while retaining T-022's immutable historical refs.
