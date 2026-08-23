# AgentMux downstream Feature handoff

Status: proposal only. This file is exchange evidence for later materialization
inside the AgentMux Feature Tracker; it is not AgentMux task truth.

## Proposed Feature

Title: Consume ctxmux Runtime truth and remove duplicate ownership

Goal: Pin one qualified ctxmux commit, reduce the AgentMux ctxmux adapter to
type/error/AgentSession translation, delete duplicated Runtime truth, and keep
Desktop View-close transactions in their correct owner.

Start condition: ctxmux Feature `f-22gcz4t8v` is qualified and supplies an exact
commit whose RuntimeIdentity advertises `native.recoverable_stop: 1`.

## Proposed tasks

### D-000 — Freeze the upstream receipt and ownership deletion map

- Pin the exact ctxmux artifact, protocol generation, RuntimeIdentity and
  capability manifest used by AgentMux.
- Map every AgentMux ctxmux adapter field and state machine to one owner:
  ctxmux, AgentMux Core, or Desktop Workbench.
- Mark “ctxmux already owns, therefore AgentMux must delete” separately from
  capabilities that are still planned but not shipped.

### D-001 — Thin the Runtime adapter

- Consume ctxmux `RunId`, `RuntimeIdentity`, versioned capabilities, creation
  keys, Recoverable Input, output byte cursors, Replay/Gap, applied Resize,
  Interrupt, complete-session Stop, persistence, planned-exec continuity,
  capacity/collection, and caller-materialized fork facts directly.
- Delete duplicate Runtime identity, capability guessing, process truth,
  replay/lifecycle caches, and obsolete compatibility models.
- Retain only public type projection, typed error translation, AgentSession
  association, and Provider materialization above the adapter.

### D-002 — Implement the minimal Workbench close transaction

- Keep Tab/Region/Browser/View ownership in Desktop, not ctxmux or AgentMux
  Core.
- Model the smallest transaction that can decide close-view, keep-background,
  request Stop, recover/settle its result, and reconcile UI state.
- Preserve AgentSession, Provider, Permission, Hook, prompt readiness and
  semantic-resume ownership in AgentMux.

### D-003 — Consume Recoverable Stop and delete ambiguity handling

- Require `native.recoverable_stop: 1` on the close path that needs retryable
  settlement.
- Retain the original Stop operation across retries and reconnects.
- Delete AgentMux's temporary unknown-Stop guessing/failure workaround; do not
  replace it with another local idempotency ledger.

### D-004 — Prove deletion and audit residual entropy

- Contract tests prove each deletion preserves public behavior and owner
  boundaries against the pinned ctxmux package.
- A package-consumer test covers lost Stop response, fresh-client recovery,
  View-close settlement, and AgentSession association.
- Finish with duplicate implementation, compatibility path, stale type, dead
  code, and second-SSOT audits.

## Explicit preservation boundary

Do not remove AgentMux activation, Run wait, or receipt-time lifecycle code
merely because ctxmux plans replacements. Delete those paths only after the
separate ctxmux Runtime activation and revision/wait capabilities ship and the
pinned package advertises them.

Do not move Provider session discovery, native resume arguments, semantic
replay, Agent working/waiting/done, Permission, Hook, Inbox, A2A, parent/child
settlement, or Workbench View semantics into ctxmux.

## Materialization guard

The AgentMux repository currently has another current-tree Feature in progress
with uncommitted Desktop work. Create this downstream Feature as
`proposal_only` only after that writer reaches a stable checkpoint or in an
explicitly isolated planning worktree. Do not write its shared Tracker from a
second concurrent writer.
