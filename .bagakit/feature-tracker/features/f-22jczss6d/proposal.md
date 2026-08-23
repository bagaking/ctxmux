# Feature Proposal: f-22jczss6d

## Why

- The second revision-pinned study of DeepSeek Harness, Paseo, Herdr, and Orca
  found useful Runtime mechanisms, but most are already delivered or already
  owned by active ctxmux Features.
- The remaining value is precision: `f-22ecztapc/T-003` and T-004 should make
  revision/time, byte cursors, identity, gaps, collection, and daemon
  replacement unambiguous before implementation continues.
- A separate discussion surface prevents research inference from silently
  changing an in-progress reviewed plan or creating duplicate Features.

## Goal

Discuss and decide how the second DeepSeek Harness, Paseo, Herdr, and Orca
mechanism study should sharpen existing ctxmux Feature contracts without
starting implementation, duplicating active owners, or crossing the
Agent-neutral Run Runtime boundary.

## Principle Layer

- What: a proposal-only decision surface for mapping verified peer mechanisms
  to existing ctxmux owners.
- Why: peer learning should reduce ambiguity and entropy, not manufacture a new
  subsystem every cycle.
- Intended generalization: future peer studies should first classify a
  mechanism as already absorbed, an existing-Feature refinement,
  consumer-triggered, or rejected.
- Failure boundary: this Feature owns no implementation, public contract,
  workspace, task execution, or lifecycle claim. Only a later user-confirmed
  revision of the owning Feature may change execution truth.
- Behavior examples:
  - Run lifecycle revision is daemon-authored and remains separate from raw
    output byte order and attachment delivery gaps.
  - A wait binds exact Runtime and Run identity and returns `collected`, `gap`,
    or `runtime_replaced` rather than guessing continuation.
  - Remote tunnel loss remains `unverifiable`; only the remote owner can change
    RunState or settle Stop.
- Evidence refs:
  - `.bagakit/researcher/topics/engineering/ctxmux-peer-mechanism-distillation/summaries/synthesis.md`
  - `.bagakit/researcher/topics/engineering/ctxmux-peer-mechanism-distillation/claims.md#c012`
  - `.bagakit/researcher/topics/engineering/ctxmux-peer-mechanism-distillation/claims.md#c016`
  - `docs/roadmap.md#standalone-runtime-convergence--phase-1`
  - `.bagakit/feature-tracker/features/f-22ecztapc/tasks.json`
  - `.bagakit/feature-tracker/features/f-22hjbhvt8/tasks.json`

## Scope

- In scope:
  - decide whether to revise `f-22ecztapc/T-003` so daemon-stamped revision is
    the ordering authority, UTC time is owner evidence, and output byte cursor
    plus delivery discontinuity remain separate facts;
  - decide whether to revise `f-22ecztapc/T-004` so waits bind exact
    `runtimeId + daemonInstanceId + runId` and distinguish matched, timeout,
    cancelled, collected, output gap, and runtime replacement;
  - confirm that T-005 activation already has the correct readiness/Hello and
    spawned-child cleanup boundary;
  - record that Orca supplies test-harness technique but no contract delta to
    `f-22hjbhvt8`;
  - decide which trigger-gated ideas remain outside the active pool.
- Out of scope:
  - implementing or starting T-003 through T-005, Remote Runtime, artifact
    ingress, release, or performance work;
  - changing task status, assigning a workspace, creating a code branch, or
    claiming Feature completion;
  - Agent state, prompt readiness, screen matching, Provider timelines,
    permissions, A2A, Relay, accounts, environments, compatibility fallbacks,
    migrations, or a public projection/plugin system.

## Candidate Decisions

| Candidate | Proposed disposition | Existing owner |
| --- | --- | --- |
| Daemon-authored Run revision/time | Sharpen reviewed acceptance; no new Task | `f-22ecztapc/T-003` |
| Identity-qualified race-free waits | Sharpen reviewed acceptance; no new Task | `f-22ecztapc/T-004` |
| Readiness FD equals public Hello | Keep current plan unchanged | `f-22ecztapc/T-005` |
| Pure fold / `asOf` watermark | Use only as private transition/test discipline | `f-22ecztapc/T-003` |
| Owner-host reachability and mixed capability skew | Keep Remote plan revision 1; borrow anti-vacuous test structure later | `f-22hjbhvt8/T-002` |
| Recoverable Input plus wait convenience | Defer until a real non-Agent consumer asks | none |
| Projection cache/journal | Defer until a durable owner journal and multiple costly projections exist | none |
| Terminal screen projection, registry watch, controller/resize leases | Keep consumer-triggered | none |

## Acceptance Criteria

- The user confirms or revises each candidate disposition before any owning
  Feature plan changes.
- Accepted changes are expressed once in the owning Feature's reviewed Task
  plan and owning architecture/protocol document; this proposal does not become
  a second execution SSOT.
- No new implementation Feature is created unless discussion identifies a
  distinct public Run behavior, real consumer, owner, failure model, and
  executable oracle not covered by the current pool.
- Rejected or deferred mechanisms remain out of active Tasks and cannot be
  inferred as requirements from the research workspace.
- This Feature stays `proposal_only` with an empty draft task plan during the
  discussion and is archived or discarded after accepted dispositions are
  transferred to their owners.

## Transfer Checks

- Delayed exit delivery still exposes daemon occurrence time and a newer
  revision than the prior attach snapshot; client receipt time is irrelevant.
- A lifecycle wait cannot be satisfied by the same label on another Run or by
  a replaced daemon; output overflow is a typed byte gap, not a Run transition.
- Activation never accepts a listener-only peer and never kills a daemon it did
  not spawn.
- Remote transport loss publishes no `exited`, `interrupted`, Stop success, or
  local execution fallback.
- Old/new capability tests execute the same named journey in both directions,
  prove every step ran, and fail instead of silently skipping.

## Impact

- Code paths: none in this proposal.
- Tests: none executed by this proposal; the listed checks are candidate
  acceptance language for owning Features.
- Rollout notes: no implementation, package, release, remote action, external
  message, branch, or workspace is authorized by this Feature.
