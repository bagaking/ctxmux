# Revision 3 review: finish ctxmux native-owner qualification

- Status: approved by the Owner through the supervised Feature-pool cleanup.
- Prior evidence: T-004 was executed and blocked after the implementation
  checkpoint, so its wording, blocker, and evidence remain immutable.
- Supersession: new T-005 owns only the remaining ctxmux qualification and
  supersedes T-004 for current execution.

## Decision

T-005 closes the already implemented native-owner change through the frozen
1/32/128 census, fresh-daemon and zero-per-Run permanent-worker truth, a clean
reliability gate, independent owner-boundary review, and an exact commit.

An external comparison may explain why the work was prioritized, but no
AgentMux pin, repin, receipt, or repository mutation is an acceptance condition.
The broader pre-registered peer cycle remains in T-001 through T-003 and does
not become release or Runtime-correctness truth.

## Preserved boundaries

- Do not rewrite or mark T-004 done.
- Do not relax the frozen census, reliability budgets, lifecycle truth, or
  process-owner boundary.
- Do not add protocol changes, compatibility aliases, fallback paths, or new
  dependencies to finish qualification.
- Publishing benchmark results or artifacts remains outside this Feature.
