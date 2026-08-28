# Pass 002 peer mechanism distillation

## Evidence identity

| Repository | Exact revision | Evidence focus |
| --- | --- | --- |
| ctxmux | `1d7d1eb86a68e8e079f75a134a1a9dde28d40f90` | current Runtime and Feature-pool baseline |
| DeepSeek Harness | `b150a551b8d465e31e418e1b2eaf5e79bbb7d28e` | owner-stamped events, pure folds, watermarks, disposable checkpoints |
| Paseo | `7c430777bfb3117eb1d359eddb69235ea308930e` | epoch/sequence cursor algebra, canonical coverage, capability gates |
| Herdr | `bbd7c2094a44fcbcc4a3a3aedef236c4d697d793` | cursor-before-snapshot waits, activation, PTY owner, handoff |
| Orca | `4fd93ead1999dc34e13ac5915693ad8467a39a6e` | owner-host truth, reachability, reconnect, mixed-version qualification |

The study inspected exact owner source, documentation, and checked-in tests.
It did not rerun all peer suites, execute a real ctxmux Remote Runtime, or make
comparative performance claims.

## Executive conclusion

No new ctxmux Runtime subsystem is justified. The high-value mechanisms either
sharpen `f-22ecztapc`, qualify work already delivered, or confirm
`f-22hjbhvt8` without changing its contract.

| Mechanism | Disposition | Owner |
| --- | --- | --- |
| Daemon-authored Run revision and occurrence time | sharpen existing acceptance | `f-22ecztapc/T-003` |
| Pure fold and `asOf` watermark | private transition/test discipline only | `f-22ecztapc/T-003` |
| Identity-qualified race-free waits | sharpen existing acceptance | `f-22ecztapc/T-004` |
| Readiness provenance equal to public Hello | keep current plan | `f-22ecztapc/T-005` |
| Wake-driven PTY ownership | already absorbed; finish qualification | `f-22aczwza9/T-005` |
| Owner-host truth and `unverifiable` | already planned; no contract delta | `f-22hjbhvt8` |
| Bidirectional capability skew | already planned; borrow anti-vacuous test shape | `f-22hjbhvt8/T-002` |
| Versioned capability gate and raw source ranges | already absorbed | current RuntimeIdentity/output contracts |

## Candidate refinements for discussion

### `f-22ecztapc/T-003`

- Stamp revision and lifecycle time where the daemon owner proves the
  transition, never when a client receives it.
- Make revision the ordering authority. UTC time is owner-authored evidence for
  display and audit, not a monotonic or distributed causal clock.
- Enumerate which lifecycle/backend metadata changes advance state revision.
  Raw output remains ordered by byte range, and delivery gaps remain attachment
  facts; neither should accidentally become a high-rate lifecycle sequence.
- Use a pure or table-driven transition oracle when useful, but do not add a
  public projection registry, append-only journal, or checkpoint cache.

Source evidence:

- DeepSeek Harness `packages/core/session/src/types.ts:395-426` and
  `packages/core/session/src/index.ts:564-655`;
- DeepSeek Harness
  `packages/session/session-projection/src/index.ts:55-120,297-493`;
- Paseo `packages/server/src/server/agent/agent-timeline-store-types.ts:3-42`
  and `agent-timeline-store.ts:214-280`.

### `f-22ecztapc/T-004`

- Capture the observation cursor before the initial state read, return when the
  snapshot already matches, then replay/follow without a snapshot-subscribe
  lost wakeup.
- Bind exact `runtimeId + daemonInstanceId + runId` and the relevant revision or
  byte cursor. A same label or another Run cannot satisfy the wait.
- Keep matched, timeout, cancelled, collected, output gap, and runtime
  replacement as distinct typed results. Timeout/cancel affects only the wait.
- Reuse the existing attach-before-snapshot public boundary. Do not add polling,
  Agent predicates, rendered-screen matching, or a daemon wait verb.

Source evidence:

- Herdr `src/api/wait.rs:132-170,348-497` and
  `src/api/event_hub.rs:12-45`;
- Paseo `agent-timeline-store.ts:214-241` and
  `daemon-e2e/timeline-reconnect-contract.e2e.test.ts:60-149`.

### `f-22ecztapc/T-005`

No plan delta is recommended. Herdr's current activation is useful
counterevidence: listener polling plus compatibility preflight on another
connection cannot prove the identity that will receive the business request,
and readiness timeout may leave a detached child.

Keep the existing ctxmux requirements: reuse before spawn, readiness FD
identity equal to the public Hello on the selected connection, fail closed on
fake or replaced endpoints, TERM/KILL/reap only the child spawned by this
attempt, and client disposal that leaves the daemon alive.

Source evidence: Herdr `src/server/autodetect.rs:39-305`.

### `f-22hjbhvt8`

Orca produces a `no-delta` verdict. The reviewed Remote Feature already owns:

- remote daemon authority for PTY, child, lifecycle, replay, and Stop receipt;
- OpenSSH host trust plus exact `runtimeId + runId` reconnect identity;
- tunnel loss as `unverifiable`, never terminal Run truth or local fallback;
- explicit unsupported behavior and mixed capability tests in both skew
  directions.

When T-002 eventually executes, borrow Orca's test technique: two genuinely
distinct builds or fixtures, the same named stateful journey in both
directions, observed-state barriers rather than sleeps, recorded identities and
capabilities, completed-step assertions, and failure instead of silent skips.
This is implementation guidance, not another Task or compatibility promise.

Source evidence:

- Orca `docs/reference/ssh-execution-boundary.md:5-18,32-63`;
- Orca `src/main/runtime/unstopped-pty-verification.ts:19-98`;
- Orca `docs/reference/remote-wire-compatibility.md:31-101`;
- Orca
  `tests/e2e/cross-version-wire/cross-version-terminal-wire.unit.test.ts:57-146`.

## Trigger-gated, not active

- A Recoverable Input plus wait SDK convenience needs a real non-Agent consumer
  and must never call PTY write semantic acknowledgement.
- Projection checkpoints need a durable owner journal plus multiple costly read
  models; current authoritative Run rows do not justify them.
- Terminal screen projection needs a real truncated-replay full-screen failure.
- Daemon registry watch and attachment/controller/resize policies need an
  accepted embedding-client failure before becoming public contracts.

## Explicit rejections

- Agent working/waiting/done, prompt readiness, permissions, messages, tool
  projections, semantic transcripts, and Provider timestamps;
- rendered-screen substring/regex waits;
- another epoch beside Runtime, daemon, Run, revision, and byte identities;
- public projection/plugin infrastructure or a full durable journal in Phase 1;
- listener-only readiness, split-connection identity proof, or receipt-time
  lifecycle facts;
- compatibility aliases, migrations, fallbacks, boolean capability manifests,
  and per-client wire variants while ctxmux remains pre-stable;
- Orca Relay/Account/Environment machinery, Herdr pane/UI state, and a return to
  dual-process `SCM_RIGHTS` handoff without a new owner requirement.

## Decisions requested from the user

1. Should `f-22ecztapc` receive one reviewed plan revision that sharpens T-003
   and T-004 exactly as above without changing task IDs, scope, or order?
2. Should all trigger-gated candidates remain outside the active pool until a
   concrete consumer failure is presented?
3. If both answers are yes, should this discussion Feature be archived after
   the owning plan revision lands?
