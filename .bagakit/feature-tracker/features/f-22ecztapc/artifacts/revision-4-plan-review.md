# Feature plan revision 4 review

Status: approved by direct user confirmation on 2026-08-24.

## Decision

The user confirmed all three dispositions from Feature `f-22jczss6d`:

1. revise `f-22ecztapc` to sharpen T-003 and T-004 without changing their IDs,
   scope, dependencies, or execution order;
2. keep every trigger-gated candidate outside the active requirement pool
   until a concrete consumer failure supplies a separate owner and oracle;
3. archive the proposal-only discussion Feature after this owning plan revision
   is installed.

No implementation, task start, workspace change, package action, release, push,
or external message is authorized by this review.

## T-003 confirmed observation contract

- The daemon stamps revision and lifecycle time at the authoritative Run
  transition. SDK receipt time is never substituted for occurrence time.
- Revision is the ordering authority. UTC timestamps are owner-authored display
  and audit evidence, not a monotonic clock or distributed causal time.
- Run state revision advances for the declared lifecycle/backend metadata
  transition set. Raw output byte progress, applied-input progress, attachment
  delivery or count, replay retention, and delivery gaps remain separate facts
  and do not become a high-rate lifecycle sequence.
- Snapshot and live observations join at an exact revision and carry Runtime,
  daemon-incarnation, and Run identity. Persistent recovery cannot regress the
  latest revision or terminal occurrence time.
- A pure or table-driven transition oracle is encouraged where it keeps the
  contract exact, but Phase 1 gains no public projection registry, append-only
  observation journal, checkpoint cache, or plugin surface.

## T-004 confirmed wait contract

- Wait captures its observation cursor before the initial state read, returns
  immediately when the authoritative snapshot matches, and otherwise follows
  the existing attach-before-snapshot boundary without a lost wakeup.
- Every wait binds the exact Runtime and Run, and incarnation-sensitive
  continuation is fenced by `daemonInstanceId`. A same label, recreated Run,
  collected Run, or replacement daemon cannot satisfy the old wait.
- Lifecycle revision and output byte cursors stay distinct. Public results keep
  matched, timeout, cancelled, collected, output gap, and runtime replacement
  separate; timeout or cancellation never stops the Run.
- Rust, TypeScript, and CLI share the same public helper semantics. No busy
  polling, hidden daemon state, Agent predicate, rendered-screen match, new
  daemon wait verb, compatibility layer, or unbounded retry is added.

## Unchanged dispositions

- T-005 keeps its existing readiness-FD/public-Hello identity agreement and
  spawned-child-only cleanup contract.
- Remote Runtime Feature `f-22hjbhvt8` remains at reviewed plan revision 1.
  Orca supplies a future anti-vacuous mixed-version test pattern, not a new Task
  or public compatibility promise.
- Recoverable Input plus wait composition, projection caches/journals, terminal
  screen projection, daemon registry watch, and attachment/controller/resize
  policy remain consumer-triggered and outside active Tasks.

## Evidence

- `.bagakit/feature-tracker/features-archived/f-22jczss6d/artifacts/pass-002-peer-distillation.md`
- `.bagakit/feature-tracker/features-archived/f-22jczss6d/artifacts/closeout-preserved-root/proposal.md`
- `docs/roadmap.md#standalone-runtime-convergence--phase-1`
- `docs/architecture.md#standalone-runtime-boundary`
- `docs/protocol.md#connection-state`
