# Stop terminal convergence

The [case study](../architecture/case-study-replay-stop-convergence.md) records
the failure chain, superseded assumption, and validation limits. This document
retains the bounded implementation scope.

The user scoped this work to the replay/Stop regressions: “我说的就是刚才发现的
问题, ctxmux 这边的全部解决一下” and “先做一个稳定而收敛的版本”. Other roadmap
milestones and downstream product changes are outside this change.

The c019b00 review reproduced a successful Stop followed by a fresh List that
still reported `running`. Native cleanup had finished, but durable terminal
publication was still queued. The old protocol explicitly allowed this window;
external replay made it easier to encounter. Cleanup admission and durable
finalization already have separate worker budgets and must remain separate.

The implementation decision is to join those facts at the public response:
successful Stop must observe terminal publication before replying on every
public path. The daemon-owned operation ledger still records physical cleanup
once, independent of clients and storage. A bounded publication wait returns
`unknown` when it cannot establish the stronger public postcondition, without
overwriting the retained cleanup result. Retrying the same operation key can
then confirm terminal publication without another signal or cleanup attempt.

Acceptance is a deterministic storage barrier with more Runs than the cleanup
worker budget, continued read/control responsiveness, disconnected-client
recovery, publication-timeout recovery, and real Rust/SDK client tests that
read terminal state immediately after a successful Stop. Replay recovery,
resource budgets, and persistence ordering remain unchanged. No migrations,
compatibility paths, widened budgets, or unrelated features are introduced.

Current semantics belong in `docs/protocol.md` and `docs/architecture.md`;
task state and version-bound verification belong in Feature Tracker.
