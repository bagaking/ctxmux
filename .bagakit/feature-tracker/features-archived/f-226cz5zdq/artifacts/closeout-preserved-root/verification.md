# Verification Evidence

## T-021 — Resource Baseline Runtime Identity

## Automated Checks

- Command: `scripts/check-reliability.sh`
- Result: passed from clean commit
  `6ccb464d5896b08846a2355a712fcb9ce5921840` and tree
  `88f22e1eaa27578bad003b05ce7bda2e5fd44b33`. The command validated the
  source-bound generation-2 policy before its locked build, then passed all
  five chaos, security, stress, resource, and frozen-budget stages. The ignored
  smoke receipt is `target/reliability/smoke/result.json`, SHA-256
  `1150097a19fa4e9146962bea04dc8eebe85fd60757d65b51eed7112af0c1ebb7`;
  it records a clean worktree, the same commit/tree, invocation nonce, harness
  SHA-256 `1fbd40c4f89252a2159335b4610bc502cfa28f5c6e1a7961ffbf6d8977c1ee15`,
  launcher SHA-256
  `946a004a0c634215a8ed3c3e74cd45c9d199fb66d7909184216f982fa3e763ba`,
  daemon SHA-256
  `a0a25f2df43f0bc9a2d04b57242a9397e81c07d903efb5bc3d1af5702fa53f45`,
  locked build, toolchain, host, workload, and ten owner-only `0600` logs.
- Command: `scripts/check.sh`
- Result: passed with exit 0 in the final formal T-021 Tracker Gate. Canonical
  evidence is `artifacts/gate-T-021-r22-0001.log`, SHA-256
  `d3b7054da9c212e9e596b17d45ce4fed6a59576299af0f639b65b8036afdc8d9`.
  The earlier heading-order failure remains at `gate-T-021-r20-0001.log`; the
  later exit-101 tmux scheduling-pressure attempt remains at
  `gate-T-021-r21-0001.log`. Neither failed attempt was overwritten.
- Command: `node --test scripts/reliability-policy.test.mjs`
- Result: passed 81/81 policy and adversarial tests. Together with
  `scripts/ci-reachability.test.mjs`, 90/90 focused tests passed. Policy and CI
  CLIs, strict scripts typecheck, Bash syntax, Prettier, commit-message lint,
  and diff checks also passed.
- Command: exact tmux regressions and full adapter suite.
- Result: the two prior host-load-sensitive cases passed three consecutive
  serial runs each (6/6), then `tmux_adapter` passed 31/31 with one test thread.
  No T-021 tmux runtime or deadline change was made.
- Review: exact commit/tree/message audit of `6ccb464` / `88f22e1` passed with
  zero P0/P1 and no Product, Run, runtime, protocol, Backend, Integration,
  Agent, tmux, or Feature drift.

## Manual Checks

- Step: bind the implementation and observation sequence to immutable Git
  identities instead of a mutable current tree.
- Outcome: commit `69945fe75c6d293892734bde009c28af31ff2fce` added
  generation-2 runtime receipts; `24e32a53605d22739b98cfe86c5ad0b214006bd4`
  pre-registered the source-bound policy and stable derivation contract;
  `21c2e7c6e483782bda3ca6b2b70f5adeb14531ec` froze the three observations and
  all derived budgets; `2e3c09dc97e9ea0e49b95318dd8ab99d901ec3ed`
  made policy admission mandatory for every canonical qualification profile;
  `6ccb464d5896b08846a2355a712fcb9ce5921840` closed the remaining producer
  owner race and bound the complete launcher, preflight owner, invocation,
  receipt bytes, and required Gate.
- Step: replace pathname publication with one bounded qualification artifact
  owner rather than a general attestation/filesystem framework.
- Outcome: preflight traverses the fixed `target/reliability/<profile>` owner
  one no-follow component at a time, records bigint device/inode plus prior
  receipt identity and nonce, and the supervisor re-enters that identity before
  timer or worker creation. Receipt, timeout, and exclusive logs mutate only
  owner-relative basenames; the final verifier re-enters the owner and rechecks
  receipt inode plus SHA-256 after semantic validation. Parent/leaf replacement,
  wrong nonce, stale/future receipt, same-inode byte mutation, and worker cwd
  inheritance have deterministic oracles.
- Step: independently recompute the frozen evidence and its local claim
  boundary.
- Outcome: all three raw observations bind source commit
  `24e32a53605d22739b98cfe86c5ad0b214006bd4`, tree
  `62aee545d8f5af7781b9d77c8ad3fa46a595a89b`, daemon SHA-256
  `a0a25f2df43f0bc9a2d04b57242a9397e81c07d903efb5bc3d1af5702fa53f45`,
  and budget-contract SHA-256
  `27b6daba1103f7a49e138d437969f8860ec3eb488e23c1f97aa677877071d6b5`.
  Receipt SHA-256 values are
  `5307aca98b2ed5889f363d6b99cc22b64e3ca562ddcf5f9a02d3275b8c6f5ad4`,
  `55d030503b69bba00460a1aff8a06e2400905fe1530d243ab20fec9e14e903c0`,
  and `ebb64d7c55a2c1b9aae963d58ce3e1b0a2a6cf59448037b90f5a0bb0bdaf7997`.
  The policy independently matched all 60 recorded maxima and 60
  rational-derived ceilings.
- Step: review performance meaning and cleanup invariants without converting
  the evidence task into a runtime optimization or product-architecture task.
- Outcome: the 128-Run idle maximum is 6.946% of one core, 15,776 KiB peak RSS,
  and 15,376 KiB steady RSS; the active maximum is 265.673% of one core,
  19,648 KiB peak RSS, and 19,392 KiB steady RSS. Each Run adds two threads and
  three descriptors in the measured model; cleanup thread, live-child, and
  attachment deltas are zero. Independent review found no P0/P1 and no Run,
  protocol, Backend, Integration, Agent, or Feature drift. Optimization remains
  assigned to later benchmark/resource Tasks rather than hidden in T-021.

## Residual Risks

- The frozen baseline is a local Darwin arm64 observation with
  `claim_scope=locally_observed` and `binary_source_attestation=false`. Matching
  daemon bytes across the three rounds do not prove a reproducible build or
  binary-to-source attestation.
- Ubuntu, macOS hosted nightly/release, two-hour release soak, and
  other-platform artifacts remain pending with their owning later tasks. This
  task does not claim release qualification.
- `target/reliability/smoke/result.json` is ignored, local supporting evidence.
  Durable baseline truth is the three checked-in fixture receipts plus the
  source-bound budget and policy contract.
- The artifact owner pins an inode, not permanent containment. Another same-UID
  process that directly relocates that inode or mutates its files is outside the
  local evidence boundary; static parent/leaf substitution cannot redirect a
  write into a different victim inode, and final owner drift fails admission.
- Timeout annotation now requires the current invocation nonce and a replaced
  receipt inode. Its three branch outcomes are covered through surrounding
  owner/nonce validation, but a future extraction could add a narrower direct
  unit oracle without widening the runtime or threat model.
- Formal attempts r20 and r21 remain failed history; r22 is the passing
  completion Gate. Hosted qualification remains pending and is not inferred
  from this local Gate.

## T-001 — Coverage And CI Reachability

Review correction: T-001's prose recorded an explicit local coverage run, but
its canonical Tracker gate executed only `scripts/check.sh`. That gate log did
not prove the instrumented lane, and a clean `HEAD` comparison could report no
changed executable lines without proving the changed-line ratchet. Revision 6
therefore supersedes T-001 with T-018, which owns explicit command, base, and
non-empty denominator evidence.

## Manual Checks

- Step: audit every T-001 acceptance item against its current authoritative
  policy, workflow, map, report, command, and documentation evidence.
- Outcome: all four acceptance items are proved locally; hosted runner results
  remain unavailable until submission and cannot be counted as passing.

- Coverage classification: `coverage-policy.json` separates Rust
  runtime/clients, Rust protocol/codegen, hand-written TypeScript SDK, and
  TypeScript protocol validators. Generated declarations and the generation-2
  non-Unix compile boundary are reported with their evidence owners instead of
  diluting a denominator.
- Enforced floors: `scripts/check.sh --coverage` passed on 2026-08-10 with
  87.01% Rust runtime/client, 98.03% Rust protocol/codegen, 91.88% TypeScript
  SDK, 96.57% TypeScript validators, and 100% changed executable lines. The
  reviewed minimums are 85%, 95%, 85%, 95%, and 90%, respectively.
- Platform reach: `.github/workflows/ci.yml` defines a required `critical`
  matrix on `ubuntu-latest` and `macos-latest`, plus a required Ubuntu
  `coverage` job with the pinned Rust coverage tool and full repository gate.
- Job-to-invariant reach: `.github/ci-evidence-map.json` maps 15 discovered
  critical suites to invariants, selection owners, jobs, and platforms. Its
  non-required skipped, ignored, conditional, and schedule-only inventories
  are explicit and empty. `scripts/ci-reachability.mjs` rejects hidden or
  unmapped evidence and workflow/selection drift.
- Lifecycle-proof boundary: `docs/testing-strategy.md` continues to state that
  percentages do not replace owner-boundary, PTY, process, race, resource, or
  platform behavior. Remaining P0 lifecycle gaps stay assigned to later
  reviewed tasks.

## Automated Checks

- `scripts/check.sh`: passed, including formatting, Clippy, all Rust tests,
  public CLI smoke, protocol drift, TypeScript format/type/build, SDK unit
  tests, and cross-client real-daemon E2E tests.
- `scripts/check.sh --coverage`: passed with all reviewed group and changed-line
  thresholds; raw local reports are under the ignored `coverage/` directory.
- `node scripts/ci-reachability.mjs`: passed with 15 suites across two required
  jobs.
- `node --test scripts/*.test.mjs`: 15 policy and fixture tests passed.
- `git diff --check`: passed.

## Residual Risks

- The workflow configuration and local Darwin arm64 gates are verified in this
  worktree. GitHub-hosted Ubuntu and macOS run results cannot exist until the
  change is committed and submitted; a later failing runner is a gate failure,
  not passing evidence.

## T-002 — Owner-Boundary P0 Gaps

Review correction: T-002 proved the startup stale-target replacement fence but
did not prove shutdown ownership. Its original path-only `SocketGuard` could
unlink another live listener that replaced the pathname before the old daemon
exited. Revision 6 therefore supersedes this conclusion with T-016, which owns
the symmetric bound-socket identity fence and real replacement-listener
regression.

## Manual Checks

- Gap and cursor recovery: the real-socket test
  `public_gap_reattaches_from_the_callers_cursor_without_loss_or_duplicates`
  pauses an attachment after its snapshot, drives 32 KiB through a real PTY
  child into a two-event live channel, observes public `Gap`, and reattaches
  from the caller's pre-gap cursor. The replay has contiguous sequence numbers,
  exactly 32 KiB of expected bytes, no truncation, omission, or duplicate.
- Stop ownership and identity: `Run` no longer stores the cloned Unix
  PID-based HUP-only killer. The waiter retains the actual child handle,
  processes one stop command there, and removes signalling authority as soon
  as wait observes exit. The public HUP-ignoring fixture proves escalation and
  eventual direct-child exit; the deterministic wait/publication barrier
  proves a concurrent stop is rejected before public `Exited` without touching
  an unrelated live process. Process-tree termination remains explicitly out
  of scope.
- Interactive CLI: `controlling_pty_attach_restores_terminal_and_leaves_the_run_alive`
  launches the real `ctxmux attach` binary under a controlling PTY, observes
  raw terminal attributes, forwards raw input, resizes the master and observes
  the child PTY at 120 columns by 40 rows, sends `Ctrl-b d`, compares complete
  before/after terminal attributes, and verifies the same Run PID remains
  running with zero attachments. The fixture passed five consecutive focused
  runs after its child was made insensitive to non-contractual shell `WINCH`
  trap interruption.
- Socket replacement: `stale_socket_replacement_race_preserves_the_unrelated_live_target`
  pauses after the inactive probe, replaces the checked stale inode with an
  unrelated live listener, and proves device/inode revalidation returns
  `SocketTargetChanged` without unlinking or binding over that listener. A
  second live probe covers the same target becoming active before cleanup.
- Scope: all deterministic seams remain private and owner-local. No public
  fault API, general scheduler, Backend abstraction, process-tree guarantee,
  or protocol fallback was introduced.
- Documentation: `docs/architecture.md`, `docs/protocol.md`, decisions 002,
  003, 004, 005, and 008, `docs/testing-strategy.md`, and the wrong-case
  dispositions for `PTY-003`, `LP-01`, `LC-003`, and `OR-002` now match the
  implemented owner guarantees and remaining limits.

## Automated Checks

- `scripts/check.sh`: passed with formatting, fixture policy, 16-suite CI
  reachability, Clippy, 29 Rust tests, public CLI smoke, protocol drift,
  TypeScript formatting/type/build, 25 SDK unit tests, and two cross-client
  real-daemon E2Es.
- `scripts/check.sh --coverage`: passed with 92.18% Rust runtime/client,
  98.03% Rust protocol/codegen, 91.88% TypeScript SDK, 96.57% TypeScript
  validators, and 90.95% changed executable lines against floors of 85%, 95%,
  85%, 95%, and 90%.
- `cargo clippy --workspace --all-targets -- -D warnings`: passed.
- `node scripts/check-fixtures.mjs`: passed for all 35 retained wrong cases;
  every activated T-002 anchor is executable and gate-reachable.
- Five consecutive focused executions of
  `cargo test -p ctxmux --test interactive_attach -- --nocapture`: passed.
- `git diff --check`: passed before recording this evidence and is rerun before
  the Tracker gate.

## Residual Risks

- Direct-child stop does not claim process-group or descendant-tree cleanup.
- Stale cleanup revalidates identity and liveness but recheck and unlink are not
  one atomic kernel operation; a hostile writable parent directory and peer
  authentication remain open security boundaries.
- The original T-002 implementation checked only that the shutdown pathname was
  still a socket. T-016 supersedes that incomplete conclusion rather than
  mutating T-002's executed task semantics.
- The controlling-PTY fixture proves ordinary acknowledged detach. Daemon loss,
  recoverable errors, unwind, catchable signals, and non-catchable termination
  remain broader CLI restoration qualification.
- The public Gap fixture proves exact retained continuation. High-volume
  eviction, SDK lag recovery, and global output/resource pressure remain owned
  by later stress, resource, and streaming tasks.
- Local Darwin arm64 evidence is green. GitHub-hosted Ubuntu and macOS results
  remain unavailable until submission and cannot be counted as passing.

## T-009 — Level B Provenance And Real Codex

Status correction: the hermetic recording-child path and explicitly authorized
real Codex semantic continuation prove the happy path, but T-009 did not close
source-bound provenance. The wrapper authenticated semantic event objects after
they passed through a parent-scoped observer; because the raw event carried no
source Run identity, unrelated Run output could be routed through that observer
and certified. An Integration that omitted `levelBForkProvenance` was also
fail-open. Revision 6 supersedes that incomplete conclusion with T-017 rather
than rewriting T-009's executed semantics.

## Manual Checks

- Object-local receipt: a registered Integration wraps semantic observers and
  records each emitted event object against the observer's declared parent in a
  registration-local `WeakMap`. Codex uses `thread.started` as its session
  receipt. Copied, explicitly unbound, cross-registration, and receipt objects
  supplied with another declared parent fail before planner execution or raw
  fork. This does not authenticate the source Run of the raw event.
- Public no-child oracle: the recording-executable E2E creates an unrelated
  real daemon Run, attempts cross-parent and copied-event Level B forks, and
  compares the daemon Run list before and after both rejections. The list is
  unchanged; the valid parent receipt still creates a distinct child with
  `level_b` lineage and exact declared context.
- Probe envelope: local Codex CLI 0.147.0 completed `--version`, `exec --help`,
  and `exec resume --help` in 0.18s, 0.08s, and 0.05s respectively on this
  Darwin arm64 environment. The default SDK budget is now five seconds; a
  1.25-second delayed executable succeeds under the default while an explicit
  25ms timeout remains the deterministic `probe_timeout` case.
- External canary: `scripts/codex-semantic-canary.ts` starts a real parent,
  establishes a random semantic fact, resumes through the verified parent
  event with a child prompt that omits the fact, and requires the exact fact in
  the child agent message. Evidence records only version, elapsed time, Run
  ids, hashes, event names, and lineage; key, session id, fact, prompts, and
  model text are not emitted. `.github/workflows/reliability.yml` pins Codex
  0.147.0 and runs scheduled/manual with a required secret and no retry.
- Real result: the final local run used explicit
  `CTXMUX_ALLOW_CODEX_LOGIN_AUTH=1` with an already authenticated Codex CLI. It
  passed once on Codex 0.147.0 with a 247ms probe, distinct parent/child Run
  ids, `level_b` lineage, `child_prompt_contains_fact=false`,
  `continuation_exact=true`, and `fatal_diagnostics_zero=true`. The artifact is
  `.bagakit/feature-tracker/features/f-226cz5zdq/artifacts/codex-semantic-canary-local.json`.
- Real PTY diagnostics: both parent and child streams contained 32 ordinary
  non-JSON lines beside 5 and 4 valid JSON lines. The observer surfaced these
  as counted `invalid_json` diagnostics. There was no gap, invalid UTF-8,
  oversized record, invalid semantic event, nonzero exit, or loss of the exact
  continuation. The canary treats the first three destructive diagnostics as
  fatal and retains all non-JSON counts rather than silently discarding them.
- External failure policy: executing the canary without either supported key
  and without explicit logged-in-auth opt-in exits nonzero and writes a visible
  redacted `status=fail` artifact. Missing credentials do not skip or turn the
  lane green; login state is never borrowed implicitly.

## Automated Checks

- `scripts/check.sh`: passed with the provenance unit matrix, public daemon
  rejection/no-child E2E, 27 SDK unit tests, two cross-client E2Es, canary
  static typecheck, workflow formatting, and every existing Rust/CLI gate.
- `scripts/check.sh --coverage`: passed with 92.18% Rust runtime/client,
  98.03% Rust protocol/codegen, 92.27% TypeScript SDK, 96.74% TypeScript
  validators, and 91.87% changed executable lines.
- `npm run test:codex-canary` with both key variables removed: failed as
  required and wrote `ctxmux.codex-semantic-canary.v1` evidence with no secret
  content.
- `CTXMUX_ALLOW_CODEX_LOGIN_AUTH=1 ... npm run test:codex-canary`: passed on
  the final script revision and wrote the redacted Feature artifact described
  above without retry.
- `node scripts/check-fixtures.mjs`: passed with 35 cases; delayed default
  probing and the T-002 public Gap owner remain gate-reachable.

## Residual Risks

- T-009 provenance was host-local and protected only against accidental receipt
  fabrication and stale objects. It was neither source-bound nor a security
  boundary against a malicious JavaScript host. T-017 owns the source-bound
  correction; raw fork remains bypassable by an intentionally malicious host.
- The original T-009 path accepted missing `levelBForkProvenance` and could
  certify another Run's output when routed through the parent observer. T-017
  retains these as explicit supersession evidence.
- The five-second probe budget is backed by local measurement plus a delayed
  hermetic envelope, not a broad hosted-platform distribution. The scheduled
  canary records actual version and probe time so later evidence may tighten or
  revise the supported envelope before optimization.
- The checked-in scheduled workflow has not yet produced a GitHub-hosted
  artifact from the repository secret. This does not replace or invalidate the
  passing local real-vendor evidence, but any future scheduled failure is a
  visible external compatibility failure and cannot be retried into green.

## T-003 — Forced Concurrency And Hostile Input

Status: every T-003 acceptance item has local executable evidence. The required
gate contains deterministic owner barriers, a bounded public-operation model,
minimized parser corpora, and seeded parser targets; the scheduled workflow
raises case counts and preserves an exact replay receipt without claiming
sanitizer, chaos, resource, or soak coverage.

## Manual Checks

- Subscribe versus snapshot: the attachment owner pauses immediately after
  subscribing. Output recorded in that interval appears once in replay, the
  already-subscribed duplicate is suppressed, and the next live sequence is
  delivered normally.
- Detach versus output: the owner pauses after receiving detach but before its
  acknowledgement. Output recorded there remains exactly replayable from the
  caller cursor, detach is acknowledged, and the attachment count returns to
  zero.
- Final output versus exit: child wait is paused before public exit. A final
  retained chunk recorded in that interval is delivered before `Exited` and is
  still present for a late attachment. The prior stop-after-wait identity
  barrier continues to prove that stop cannot signal after owned-child wait.
- Public mutation model: separate socket clients race input, resize, and two
  stops against a long-running Run. Exactly one stop succeeds; input and resize
  may only succeed or return the declared exited-state/owner-I/O errors. The
  model intentionally asserts no writer ordering or resize arbitration.
- Parser targets: `crates/ctxmux-protocol/tests/seeded_fuzz.rs` mutates retained
  malformed plus valid typed frames and round-trips every accepted decode.
  `packages/sdk/test/parser-fuzz.test.ts` partitions and mutates NDJSON bytes,
  validates every accepted server value, fuzzes Codex observer bytes/gaps/EOF,
  and replays `fixtures/codex-jsonl-regressions.json` as an ordinary minimized
  corpus. No other future protocol parser is currently active.
- Scheduled evidence: `scripts/run-seeded-qualification.mjs` writes its receipt
  before invoking Rust protocol, public daemon mutation-model, and TypeScript
  wire/Codex targets. It records seed, case counts, tool environment, exact
  argv, exit/signal results, covered owner boundaries, and excluded claims.
  `.github/workflows/reliability.yml` uploads that receipt even on failure with
  no retry.
- Failure retained during qualification: the first full gate exposed the
  controlling-PTY fixture querying resize through the same view-side input
  stream used to trigger and observe `SIGWINCH`; one transient master `EIO`
  obscured the Run resize oracle. Size queries now use an independent public
  Run input request while the CLI remains solely responsible for forwarding
  resize and presenting the child's observed output. The focused controlling
  PTY test then passed ten consecutive executions.

## Automated Checks

- `scripts/check.sh`: passed with 34 Rust tests, 30 SDK unit/schema/parser tests,
  two cross-client real-daemon E2Es, one public CLI smoke, 15 policy tests, and
  all format, Clippy, protocol-drift, fixture, and CI-reachability checks.
- `scripts/check.sh --coverage`: passed with 93.14% Rust runtime/client, 98.03%
  Rust protocol/codegen, 92.75% TypeScript SDK, 96.92% TypeScript protocol
  validators, and 94.53% changed executable lines.
- `CTXMUX_RELIABILITY_SEED=226003 CTXMUX_FUZZ_CASES=64
  CTXMUX_MODEL_CASES=4 npm run test:seeded-qualification`: passed all three
  scheduled commands and wrote a complete local replay receipt. The scheduled
  workflow raises those counts to 10,000 parser cases and 128 mutation-model
  cases.
- Ten consecutive focused executions of
  `cargo test --locked -p ctxmux --test interactive_attach -- --nocapture`:
  passed after separating resize trigger and observation paths.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`,
  `node scripts/check-fixtures.mjs`, `node scripts/ci-reachability.mjs`, and
  `git diff --check`: passed before final Tracker gate execution.

## Residual Risks

- Seeded byte mutation is bounded deterministic fuzz evidence, not
  coverage-guided libFuzzer/Jazzer execution or a native sanitizer. Those
  stronger lanes remain explicit rather than inferred.
- The public mutation model constrains the result set but does not reproduce an
  exact OS thread schedule from seed alone. Owner barriers carry the exact
  known interleavings; broader CPU/I/O-pressure schedules remain later heavy
  qualification.
- Mid-frame disconnect has ordinary close-race coverage but no dedicated
  coverage-guided target. A future active parser must add a target and minimized
  corpus before its capability can count as qualified.
- Hostile child behavior, high-volume concurrent mutation, chaos, CPU/RSS,
  resource cleanup, and soak remain owned by T-004; T-003 does not claim them.

## T-004 — Chaos, Stress, Security, And Resources

Status: the bounded generation-2 reliability matrix, frozen resource budgets,
PR smoke, scheduled nightly lane, and explicit release soak are implemented.
Local Darwin arm64 smoke and the complete 1/32/128 matrix pass; hosted Linux and
macOS receipts remain unavailable until the workflow runs.

## Manual Checks

- Chaos owners: the structured receipt records seed, action trace, process
  trees, per-daemon logs, and cleanup for Integration-host exit, child kill,
  and daemon kill. A Run survives Integration-host exit with the same PID; child
  kill preserves final replay and daemon health; daemon loss preserves no false
  restart guarantee and the harness explicitly removes any surviving child.
- Security negative space: rejected spawn and resize publish no extra Run;
  punctuation remains literal argv rather than shell syntax; environment
  secrets reach only the declared child and do not enter daemon logs; a partial
  long-lived frame and one-byte-oversized frame neither block nor mutate the
  daemon. Existing startup socket identity, mode, PID, descriptor, malformed
  corpus, and stop identity regressions run before the matrix; shutdown socket
  ownership remains assigned to its correction task.
- Stress: a 5 MiB final-output workload retained 4,193,291 bytes, reported
  truncation, and preserved `FINAL-DRAIN`. Fast 1/8/32-way consumers received
  contiguous sequences and exact seed-selected payload bytes; intentionally
  stalled 8/32-way consumers received `Gap`.
  Thirty-two lifecycle cycles restored transient threads and live children to
  baseline while keeping 32 exited Runs visibly retained without GC. A
  32-Run/concurrency-16 start-pressure oracle completed and cleaned up.
- Large replay failure retained: the initial 5 MiB observation exposed a
  single integer-array `attached` frame larger than the 1 MiB transport limit.
  The daemon now sends metadata followed by bounded ordered replay output
  frames; native and TypeScript clients reassemble before returning the
  unchanged public snapshot. Native and SDK regressions cross the frame limit.
- Empty replay failure retained: the broader SDK run exposed mock snapshots
  that advertised `head_seq=1` without a replay chunk, which correctly blocked
  after streamed replay became real. Empty fixtures now advertise head 0, the
  two-frame fixture advertises head 2, and mock teardown owns and closes every
  accepted socket instead of depending on process force-exit.
- Resource diagnosis: the first 128 idle observation failed with Darwin
  `openpty -6`. Replays at launch concurrency 1, 4, and 8 all created, measured,
  stopped, and cleaned 128 Runs. Concurrency 16 reproduced the transient error
  after 123 successful responses. Resource census therefore uses concurrency 8
  without lowering the 128 requirement, while the separate 32/16 oracle keeps
  burst-start pressure covered.
- Frozen baseline: three full pre-optimization observe rounds cover idle and
  active 1/32/128 cells. Maximum observed 128-Run peak RSS was 22,240 KiB idle
  and 26,640 KiB active; active CPU was 272.692% of one core, or about 2.73
  cores. The live per-Run thread/fd slopes were exactly 2/3 in every cell.
  `reliability-budgets.json` freezes RSS ceilings of 32 MiB idle and 48 MiB
  active at 128 Runs, a four-core active CPU ceiling, and tighter slope,
  retention, child, attachment, and transient-thread ceilings.
- Retained-state boundary: stopping every measured Run returned direct children,
  attachments, and transient threads to zero/baseline. Two descriptors per
  exited Run and related RSS remain explicit in each cleanup sample because the
  daemon intentionally retains exited Runs and has no global GC or quotas.
- Lane reach: `scripts/check.sh` runs the frozen-budget smoke. The reliability
  workflow runs a real 30-minute nightly soak inside 45 minutes on Ubuntu and
  macOS, or a two-hour release soak inside three hours after explicit dispatch.
  Both upload the receipt and daemon logs under `if: always()`. Policy tests
  reject missing 1/32/128 budgets, duration drift, platform/job drift, or an
  unreachable smoke command.
- Budget and seed integrity: all three raw observe receipts are versioned under
  `fixtures/reliability/`; the policy gate verifies their declared SHA-256 and
  recomputes each recorded maximum. The receipt seed now selects fan-out
  payload content. A qualification worker runs in its own process group, and a
  1-second fault run proved that the supervisor kills the group, marks the open
  stage failed, records `supervisor.timeout`, and leaves no qualification
  process behind.

## Automated Checks

- `scripts/check.sh`: passed after the review corrections with 35 Rust tests,
  31 SDK unit/schema/parser/backpressure tests, two real daemon E2Es, the public
  CLI smoke, 19 repository policy tests, and the frozen-budget reliability
  smoke. All 11 wrong-case tests completed normally with no cancellation or
  force-exit workaround.
- `scripts/check.sh --coverage`: passed with 93.10% Rust runtime/client, 98.04%
  Rust protocol/codegen, 92.96% TypeScript SDK, 97.09% TypeScript protocol
  validators, and 94.71% changed executable lines.
- `scripts/check-reliability.sh --profile smoke`: passed all chaos, security,
  stress, one-Run idle/active resource, and frozen-budget stages.
- `scripts/check-reliability.sh --profile nightly --soak-seconds 1`: passed the
  complete idle/active 1/32/128 matrix and frozen budgets with the same soak
  loop shortened only for local reachability. The 128 active sample used
  26,112 KiB peak RSS, 261.533% CPU, 8,197 retained bytes per Run, 174.875 KiB
  RSS per Run, two threads and three fds per Run, and zero cleanup children or
  attachments.
- Three `--profile observe` rounds: passed all named stages and all six resource
  cells before budget creation. Their complete receipts are
  `fixtures/reliability/observe-darwin-arm64-r{1,2,3}.json`; hashes and maxima
  are bound by `reliability-budgets.json`.
- `node --test scripts/reliability-policy.test.mjs
  scripts/ci-reachability.test.mjs`, `node scripts/reliability-policy.mjs`,
  `node scripts/ci-reachability.mjs`, `npm run typecheck:scripts`, and shell
  syntax checks: passed.

## Residual Risks

- The local resource baseline is Darwin arm64. The frozen absolute and per-Run
  budgets intentionally include platform margin, but Ubuntu and hosted macOS
  remain failing evidence until their scheduled receipts pass; the workflow
  configuration is not counted as an executed host result.
- The real 30-minute and two-hour durations are CI-owned. Local execution used
  the same loop with one second to prove reachability; it does not substitute
  for the scheduled or release soak receipt.
- Global Run/attachment quotas, exited-Run GC, peer credentials, hostile
  writable-parent atomicity, process-tree shutdown, fd/thread exhaustion
  injection, native sanitizers, and comparative performance benchmarks remain
  explicitly absent or assigned to later reviewed tasks.

## T-016 — Shutdown Socket Identity Fence

## Manual Checks

- The guard now captures the device/inode of the Unix socket pathname after the
  listener is bound. Drop reads the current path and unlinks only when it is
  still a socket with the same identity.
- The owner regression starts the real in-process server, completes a public
  `List` request so the guard is live, renames the old socket pathname, binds an
  unrelated listener at the published path, and proves that listener is
  reachable both before and after aborting the old server task.
- Scope stays inside the daemon socket owner. There is no protocol change,
  dependency, activation policy, retry, or filesystem fallback.

## Automated Checks

- `cargo test -p ctxmux-daemon
  shutdown_preserves_a_replacement_listener_at_the_published_path --
  --nocapture`: passed.
- `cargo clippy -p ctxmux-daemon --all-targets --all-features -- -D warnings`:
  passed.
- `scripts/check.sh`: passed with the new regression among 36 Rust tests, all
  31 SDK unit tests, both cross-process E2Es, the public CLI smoke, wrong-case
  suite, format, Clippy, protocol drift, and reliability smoke.
- Tracker gate
  `.bagakit/feature-tracker/features/f-226cz5zdq/artifacts/gate-T-016-r8-0001.log`:
  passed.

## Residual Risks

- Identity recheck and unlink are separate pathname operations, so a hostile
  writable parent can still race them. If the original socket is renamed, the
  daemon cannot rediscover and remove it through the old pathname. T-016
  protects owner-controlled shutdown; it does not claim an atomic adversarial
  filesystem transaction or peer authentication.

## T-017 — Source-Bound Level B Provenance

## Manual Checks

- `Attachment` now records the source Run for each returned live `RunEvent` and
  retained replay chunk in a private SDK `WeakMap`. A registered observer with
  a declared parent checks that owner record before the Integration parser runs;
  missing, unowned, copied-chunk, or another Run's event fails immediately.
- Every Integration that advertises Level B must now implement both
  `planLevelBFork` and `levelBForkProvenance`. The missing-hook regression proves
  the planner is not called and the raw fork count stays zero.
- The real-daemon Codex E2E creates an unrelated Run that emits a valid
  `thread.started` record through its own Attachment, routes that actual output
  into the parent observer, and receives `IntegrationProvenanceError`. The Run
  list is unchanged across this and the copied/cross-parent rejection cases;
  the valid parent receipt still creates a distinct `level_b` child.
- The daemon protocol and foundational Run types remain Agent-neutral. Source
  ownership is host-local SDK state and is explicitly not authentication against
  a malicious host that bypasses the Integration and calls raw fork.

## Automated Checks

- `npx tsx --test packages/sdk/test/integration.test.ts
  packages/sdk/test/codex-integration.test.ts`: 10/10 passed, including
  missing-hook, wrong-source, copied, unbound, cross-parent, and downgraded-plan
  cases with no raw fork on provenance failure.
- `npm run test:e2e`: 2/2 passed through real daemon and process boundaries,
  including the unrelated Attachment event/no-child oracle and the valid Codex
  continuation.
- `npm run typecheck` and `npm run build`: passed for SDK and scripts, including
  the real semantic canary collector using source-owned replay/live events.
- `scripts/check.sh`: passed with all 36 Rust tests, 31 SDK tests, 2 real-daemon
  E2Es, 11 wrong-case tests, typecheck/build, public CLI smoke, protocol drift,
  Clippy/format, and reliability smoke.
- Tracker gate
  `.bagakit/feature-tracker/features/f-226cz5zdq/artifacts/gate-T-017-r9-0001.log`:
  passed.

## Residual Risks

- The source registry is process-local and protects supported SDK use against
  accidental fabrication or event misrouting. It is not durable provenance,
  daemon authentication, or a security boundary against JavaScript code that
  invokes `client.fork` directly.
- The previously recorded real Codex canary remains valid happy-path semantic
  evidence. Hosted scheduled evidence is still unavailable until CI runs and
  cannot be counted as passing local proof.

## T-018 — Coverage Evidence And Comparison Base

## Manual Checks

- Coverage policy now distinguishes ordinary no-change reporting from retained
  changed-line evidence. `requireChangedLines=false` keeps a clean-tree run
  honest and passing; evidence mode marks a zero executable denominator as a
  policy failure.
- `scripts/check-coverage.sh` passes the explicit
  `CTXMUX_COVERAGE_REQUIRE_CHANGED_LINES` mode and always supplies
  `CTXMUX_COVERAGE_BASE`. The policy prints the selected comparison base before
  the changed-line result.
- The required CI coverage job supplies the pull-request base or previous push
  revision. Pure documentation changes may report no executable denominator;
  any result retained as changed-line proof explicitly enables evidence mode.
  Ordinary coverage remains available without fabricating a changed-line claim.

## Automated Checks

- `node --test scripts/coverage-policy.test.mjs`: 5/5 passed. The matrix proves
  an empty diff passes only in ordinary mode, fails when changed-line evidence
  is required, and a non-empty fully covered executable denominator passes.
- `CTXMUX_COVERAGE_BASE=HEAD CTXMUX_COVERAGE_REQUIRE_CHANGED_LINES=true
  scripts/check.sh --coverage`: passed. The policy printed base `HEAD` and a
  non-empty changed executable denominator of 997 lines, with 947 covered
  (94.98%). Group results were 93.19% Rust runtime/client, 98.04% Rust
  protocol/codegen, 93.13% TypeScript SDK, and 97.09% TypeScript validators.
- Tracker gate
  `.bagakit/feature-tracker/features/f-226cz5zdq/artifacts/gate-T-018-r10-0001.log`:
  passed after the explicit instrumented evidence run.

## Residual Risks

- Local `HEAD` is meaningful for this dirty feature worktree because the full
  implementation diff is uncommitted. After commit, retained evidence must use
  the actual pre-feature or pull-request base; a clean `HEAD` comparison now
  fails in evidence mode instead of being misreported as proof.
- Coverage percentages and changed-line evidence remain floors. They do not
  replace the socket, process, PTY, replay, provenance, resource, or platform
  owner-boundary tests.

## T-010 — Persistence Recovery Feature Closure

## Archived Feature Binding

- Owning Feature: `f-223czq75x`, archived from `done` with plan revision 1,
  both tasks done, and zero blocked/todo tasks. Canonical closure is
  `.bagakit/feature-tracker/features-archived/f-223czq75x/summary.md`.
- Accepted goal revision:
  `983278c336ba241bdc89ca6669ccb1f3f0658606250e4f5ed6ebb1851db63ae0`.
- Implementation Gate:
  `.bagakit/feature-tracker/features-archived/f-223czq75x/artifacts/gate-T-002-r2-0001.log`,
  SHA-256
  `d3b7054da9c212e9e596b17d45ce4fed6a59576299af0f639b65b8036afdc8d9`,
  records `scripts/check.sh => pass`.
- Archived state and task hashes are
  `af0d1066222fd2a5ef12201fb63a0224779c98523bb528c4a0a93f273ff40192`
  and
  `ad6db914c31a25476f9eb3fe5e252ca97810e50b32dbde2fcbc393327d8b7ead`.
  Git base HEAD is `b2bbc7a219753ad2664a438ab89347df180b7d31`; Gate evidence applies to
  the current-tree implementation diff, not to a falsely claimed commit.

## Shipped Boundary

- Protocol generation remains 2 and persistence schema is exact version 1.
  Persistent mode is explicitly selected with `ctxmuxd --state-dir`; default
  daemon startup remains memory-only. The store uses bundled `rusqlite 0.40.2`
  on one actor thread with a bounded 1,024-command queue.
- Recovered historical metadata includes Run identity, exact `RunSpec`,
  immediate lineage, terminal state, committed replay window, and durable
  cursor. Exited and interrupted Runs support list, status, replay attach, and
  Level A fork.
- A prior-epoch running row becomes
  `interrupted { reason: daemon_restart }` with `pid: null`. Input, resize,
  stop, and recovered Level B fork fail. No PID, PTY, child handle, or hidden
  process state is adopted.
- The accepted retention and owner boundaries are enforced: 4 MiB per Run, 256
  MiB global replay, 64 MiB metadata, 4,096 records, 384 MiB main DB, 16 MiB
  WAL, 4 MiB SHM, a 404 MiB state-file total plus the small lock, exact
  same-owner `0700`/`0600` modes, and an exclusive lifetime lock.

## Qualification Evidence

- `PERSIST-01` is active: a real restart fixture substitutes an unrelated live
  PID into a durable running row, then proves reconciliation clears it and all
  live-control requests fail while the unrelated process and old orphan remain
  alive until fixture-owned cleanup.
- `PERSIST-02` is active: a parseable cursor/chunk mixed generation returns
  typed corruption before socket publication or partial Run exposure. Unknown
  schema version, lock contention, unsafe modes, and symlink sidecars also fail
  before publication.
- Real restart restores exited metadata, exact output, terminal event, and
  distinct Level A child lineage. A 4.125 MiB public workload proves the
  retained tail and truncation cursor before and after restart; reduced
  owner-level tests prove global cross-Run pruning and terminal-only record
  eviction at exact limit transitions.
- `.github/ci-evidence-map.json` maps both store-owner and real restart suites
  into required Ubuntu/macOS critical Gate reach and Ubuntu coverage reach.
  `scripts/check.sh` includes those suites before its existing chaos, security,
  resource, and reliability smoke, so the capability cannot be qualified while
  its owning restart evidence is skipped.
- `docs/architecture.md`, `docs/protocol.md`, decision 009, the roadmap,
  testing strategy, casebook, README, Rust/TypeScript protocol types, runtime
  validator, and CLI terminal presentation now describe the same boundary.

## Unsupported and Residual Boundaries

- Live PTY handoff, PID adoption, process-tree recovery, schema migration,
  salvage/reset, remote durability, online secret purge, and encryption remain
  unsupported. Logical deletion and vacuum are not secure erasure.
- SQLite old-or-new commit recovery inherits documented filesystem and flush
  assumptions. The ordinary Gate detects a mixed application generation but
  does not claim arbitrary power-loss correctness on a hostile filesystem.
- High-performance completion is not claimed. Persistent backlog memory is
  bounded by queue backpressure, while CPU/RSS/thread/fd optimization for large
  Run counts remains assigned to T-005, T-015, and T-008.

## T-022 — Post-Archive Persistence Correctness Closure

T-022 supersedes T-010's completion claim without modifying the historical
`f-223czq75x` archive. Persistence implementation remained owned by the separate
hardening Feature; this qualification task only binds its corrected result.

## Archived Owner And Source Binding

- Owning correction Feature: `f-227czdavj`, archived from `done` with both
  tasks complete and no todo or blocked task. Canonical closure is
  `.bagakit/feature-tracker/features-archived/f-227czdavj/summary.md`.
- Admission classification commit:
  `b80a5cf44d1fafc653d9440eb5611e1e57e26d18`.
- Recovered RunSpec validation commit:
  `dc55c94c0331ce3392f982552b90ff91731fca08`.
- The formal full Gate passed from the clean implementation tree
  `cebdb234cd7088a0211d6248fd7ceef37096b38c`. Its local archived log is
  `.bagakit/feature-tracker/features-archived/f-227czdavj/artifacts/gate-T-002-r2-0001.log`
  with SHA-256
  `d3b7054da9c212e9e596b17d45ce4fed6a59576299af0f639b65b8036afdc8d9`.
- Portable source/Gate receipt commit:
  `026e35b3a6f7796e3e3cf0e35ba33554b7e65e2b`. The tracked receipt at
  `docs/verification/f-227czdavj-t002.md` records the full implementation
  commit/tree, clean source identity, command/result, log digest and exact log
  content. Two independent exact-object audits passed with no P0/P1.

## Corrected Shipped Boundary

- Protocol generation is 3; the persistence schema remains exact version 1.
- Expected record or metadata capacity rejection remains local to one
  unpublished start. Append, finalize, serialization, database, I/O, commit,
  integrity, and owner-invariant failures still latch the actor and freeze the
  durable cursor.
- Live start, materialized fork, resize, persistent insert, startup validation,
  and recovery share one daemon-owned native RunSpec semantic owner. Native
  durable rows require a launch spec with a non-empty program, non-zero terminal
  size, and non-empty declared references.
- Parseable but semantically invalid persistent state fails as corruption
  before socket publication. The real restart table proves no rejected Run or
  valid sibling is partially observable; restoring the original spec recovers
  both Runs.
- Recovered Runs remain historical. Input, resize, stop, and Level B fork fail;
  no stored PID becomes live authority.

## Verification And Residual Boundaries

- The clean implementation Gate passed `scripts/check.sh`, including actor
  admission/fatal-latch tests, seven real persistence-recovery tests, Rust and
  TypeScript contracts, public CLI/SDK E2E, tmux adapter regressions, and smoke
  reliability.
- T-022's first formal Gate record
  `artifacts/gate-T-022-r13-0001.log` reported `scripts/check.sh` exit 101 with
  no retained stdout/stderr (SHA-256
  `17d79111fe3e65e79c57edafeeadb7faacb5fe35ff4c90751c0bff65342f1a74`).
  The same clean `026e35b` HEAD then passed one complete manual
  `scripts/check.sh` execution, followed by one controlled formal rerun. The
  rerun record `artifacts/gate-T-022-r14-0001.log` passed with exit 0 (SHA-256
  `d3b7054da9c212e9e596b17d45ce4fed6a59576299af0f639b65b8036afdc8d9`).
  The original failure remains in Tracker history and is not relabeled with an
  invented root cause.
- The correction introduced no live PTY handoff, PID adoption, schema
  migration, salvage/reset, tmux persistence, generic Backend trait, retry
  loop, or TypeScript admission authority.
- T-010's dirty current-tree evidence remains historical and is not counted as
  current completion proof. T-022 binds the correction series above instead.
- Coverage-policy correction, resource-receipt v2, Ubuntu/macOS hosted evidence,
  release readiness, and CPU/RSS/thread/fd optimization remain open in their
  existing Tasks.
- The first T-022 exit 101 is an unresolved one-off Gate observation because
  the Tracker operator did not retain its command output. A repeated failure
  would be a new blocking reliability finding rather than permission to retry
  until green.

## T-020 — Complete Coverage And Changed-Line Qualification

## Source And Policy Binding

- Implementation commit:
  `b8a5e082fa035c3f508a6aa7f72517a1231031ef`; tree:
  `6547465636fea33229aac6d3dfb5978e41587693`; parent:
  `2a64423a9c3b8f83da80b284d628a60ae97bf5c4`.
- The commit is one coverage-policy, CI-reachability, dependency, oracle, and
  owning-document rollback boundary. It changes no runtime, protocol, tmux
  product behavior, Backend/Integration meaning, or coverage floor.
- `coverage-policy.json` fixes seven exact owners and constitutional floors:
  changed lines 90%, runtime owners 85%, and pure validators 95%. Filesystem
  inventory rejects unassigned, multiply assigned, locally excepted, or
  ambiguously encoded hand-written product sources.

## Instrumented Evidence

- Full command:
  `CTXMUX_COVERAGE_BASE=b80a5cf44d1fafc653d9440eb5611e1e57e26d18
  CTXMUX_COVERAGE_CHANGED_LINE_MODE=true
  CTXMUX_COVERAGE_COMPARISON_MODE=direct scripts/check-coverage.sh`.
  It passed on the final source content committed above. After commit, the raw
  reports were re-evaluated from clean `HEAD` `b8a5e08` with the same explicit
  base and policy.
- Rust runtime/clients: 86.44% (2,569/2,972); persistence: 85.35%
  (1,509/1,768); tmux: 85.99% (675/785); RunSpec validator: 100%
  (20/20); protocol/codegen: 98.04% (200/204); TypeScript SDK: 93.36%
  (1,068/1,144); TypeScript protocol validators: 97.91% (704/719).
- Changed executable product lines: 99.32% (147/148). Requested, resolved,
  and effective base were all
  `b80a5cf44d1fafc653d9440eb5611e1e57e26d18`; comparison was `direct`, and
  the base was verified as an ancestor of `HEAD`.
- Raw report SHA-256 values were
  `9d210bfb82c5360f7dc5abc2b109a4d6a9edc0658fa5b8d6122fe3f37b584ed4`
  for `coverage/rust/lcov.info` and
  `57c92cc037fded1b3dc0f941e1fcd5bc8a1023ec52fe80459b4f3fff1d044856`
  for `coverage/typescript/coverage-final.json`. These ignored local reports
  support the receipt but are not portable source truth.

## Adversarial Review And Gate

- The first independent review found silent-green paths for mixed untracked
  product sources, future/non-ancestor direct bases, unsafe diff path quoting,
  checkout source replacement, optionalized tmux evidence, and reduced
  fuzz/model depth. All were corrected with executable negatives before
  commit. A second independent whole-diff review found no remaining P0/P1/P2
  or Feature drift.
- Exact-commit/message audit of `b8a5e08` reproduced the tree and passed 33/33
  JavaScript policy tests, the 35-case fixture validator, CI reachability for
  26 suites across two required jobs, reliability policy, and syntax checks;
  it found no P0/P1/P2, overclaim, broken ref, or documentation gap.
- Formal Tracker Gate
  `artifacts/gate-T-020-r15-0001.log` passed `scripts/check.sh` from the clean
  committed tree. Its SHA-256 is
  `d3b7054da9c212e9e596b17d45ce4fed6a59576299af0f639b65b8036afdc8d9`.
- One earlier manual full-Gate attempt failed during temporary fixture creation
  with an explicit host `ENOSPC` while the volume had about 131 MiB free. Only
  the rebuildable `target/llvm-cov-target` cache was cleaned; the same source
  then passed the complete Gate. No test assertion or product failure was
  relabeled.

## Review Follow-ups

- A later review proposed that CI `auto` mode should report N/A rather than
  fail when a product change has no executable denominator. That proposal is
  the opposite of T-020's reviewed acceptance, so it does not invalidate this
  task or commit. It requires an explicit follow-up task and policy revision;
  it must not be smuggled into T-020 after its Gate.
- The same review confirmed two tmux-owner defects (unbounded synchronous
  external commands and empty-line/EOF ambiguity) plus one architecture-table
  wording drift. They are not coverage changes. The tmux findings must close
  before f-224czneed/T-001 and f-226cz5zdq/T-011 complete; the documentation
  drift requires its own bounded SSOT correction.

## T-023 — Separate CI N/A From Retained Changed-Line Evidence

## Source And Semantic Binding

- Implementation commit:
  `ea711d33626932d16f59175215153a602c438e5c`; tree:
  `028224ef0ed37689081091be84d533e8dc29a4c3`; parent:
  `b8a5e082fa035c3f508a6aa7f72517a1231031ef`.
- The commit changes only the coverage policy, its direct oracles, contributor
  guidance, testing strategy, and CI evidence-map wording. It changes no
  runtime, protocol, tmux product code, Backend/Integration meaning, owner
  assignment, source inventory, comparison fence, or 90/85/95 floor.
- Changed-line results now use the single three-state contract
  `not_applicable | pass | fail`. Auto or ordinary zero-denominator results do
  not expose a boolean percentage pass; explicit retained-evidence mode still
  fails when the denominator is zero.

## Auto N/A And Retained-Evidence Oracles

- The exact commit passed `scripts/check-coverage.sh` with requested, resolved,
  and effective base
  `b8a5e082fa035c3f508a6aa7f72517a1231031ef`, `direct` comparison, and
  `CTXMUX_COVERAGE_CHANGED_LINE_MODE=auto`. All seven owners passed, while the
  changed-line result was exactly `N/A` with no percentage proposition.
- Observed owner results were: Rust runtime/clients 86.44% (2,569/2,972),
  persistence 85.35% (1,509/1,768), tmux 85.99% (675/785), RunSpec validator
  100% (20/20), protocol/codegen 98.04% (200/204), TypeScript SDK 93.36%
  (1,068/1,144), and TypeScript protocol validators 97.91% (704/719).
- The same exact parent comparison in explicit `true` mode failed because this
  task changes no executable Rust or TypeScript product line. That rejection is
  the required negative oracle: no unrelated executable edit was added to
  manufacture retained changed-line evidence.
- Policy tests passed 17/17 and cover documentation-only, comment-only,
  deletion-only, rename, untracked, mixed executable/non-executable, 100/90/80%
  auto outcomes, explicit retained evidence, invalid bases, ancestry, source
  inventory, and fixed owner profiles. N/A output is asserted to contain no
  percentage, while owner and missing-report failures remain visible.
- Raw report SHA-256 values are
  `0ae8fe4ab33bbfb28eaa12d98cf9e845bff168dbb5ef009ac993446e38045bde`
  for `coverage/rust/lcov.info` and
  `f6d6a3dcf751c58963dc64b91cb3d59b5e5aab4a05d177f47d2e6ed1f26f06e7`
  for `coverage/typescript/coverage-final.json`. The ignored reports are local
  supporting evidence, not portable source truth.

## Independent Review And Formal Gate

- The first independent whole-diff and adversarial review found one P2: auto
  N/A still returned an ambiguous patch-level `passed: true`. The implementation
  replaced that boolean with the three-state outcome, and final re-review found
  no P0/P1/P2 or Feature drift.
- The exact amended commit and message were independently audited after the
  correction. The audit confirmed the five-file tree, semantic refs, avoided
  scope, message contract, and absence of machine-local paths or secrets, with
  no remaining finding.
- Formal Tracker Gate
  `artifacts/gate-T-023-r16-0001.log` passed `scripts/check.sh` from clean
  commit `ea711d3`. Its SHA-256 is
  `d3b7054da9c212e9e596b17d45ce4fed6a59576299af0f639b65b8036afdc8d9`.
- T-020 remains immutable historical evidence. T-023 supersedes only its
  auto zero-denominator rule; all seven owners and their frozen floors remain
  in force.

## T-025 — Bind Required CI Gates To One Neutralized Execution Boundary

## Source And Owner Binding

- Implementation commit:
  `642095b7ced520dd4631a3262e7ef303104750e9`; tree:
  `079d73fde662a6f4cd03671eed09915dcd0044c0`; parent:
  `ea711d33626932d16f59175215153a602c438e5c`.
- The commit changes only the required GitHub workflow, CI reachability owner
  and adversarial oracles, evidence-map wording, and testing strategy. Runtime,
  protocol, tmux product behavior, coverage owner assignment, source inventory,
  comparison semantics, and 90/85/95 floors remain unchanged.
- Critical and coverage now finish in one exact inline step. Step environment
  binds `BASH_ENV` and `ENV` to `/dev/null` before Bash startup; the command
  clears startup, repository redirect, counted Git config, HOME/XDG override,
  and dynamic indexed config state before checking event source identity and
  immediately execing the canonical Gate under a profile-free `/bin/bash`.

## Adversarial Oracles And Corrections

- The original validator accepted a prep step that persisted `BASH_ENV` through
  `GITHUB_ENV`. A first shell could leave the fence untouched and a second shell
  could then mutate source or reduce canonical CTXMUX values after the fence.
  The real Bash control reproduces a fuzz-depth change from 512 to 1; same-step
  `unset` without startup neutralization remains unsafe, while the final contract
  preserves 512 and emits no mutation marker.
- First review found counted `GIT_CONFIG_KEY_*` / `GIT_CONFIG_VALUE_*` state
  could point `core.excludesFile` at an ignore-all file and hide a real
  untracked source. The command now clears the count, parameter, global/system,
  and every indexed key/value channel; a real Git fixture proves ordinary,
  poisoned, and restored status behavior.
- Second review found that unsetting global/system config alone re-enabled
  poisoned `HOME/.gitconfig` and `XDG_CONFIG_HOME/git/config`. Both identity Git
  calls now bind empty global/system config per command, and status disables
  excludes, fsmonitor, and the untracked cache. Separate HOME and XDG fixtures
  prove the final report sees the real untracked file.
- Final P2 review narrowed the documentation from a complete-worktree claim to
  the state observed by the neutralized Git report and explicitly excludes
  hostile runners/actions, background mutation, dynamic loaders, complete
  PATH/toolchain attestation, and Git metadata, index-flag, and object-store
  tampering. No general workflow-security helper or framework was added.
- Targeted CI reachability tests passed 9/9, including the mutation table;
  the canonical validator mapped 26 suites across both required jobs. The full
  repository Gate passed after all corrections.

## Independent Audit And Formal Gate

- Final whole-diff and adversarial re-review found no P0/P1/P2 or Feature drift.
  Exact commit/message audit of `642095b` confirmed the five-file tree, refs,
  avoided scope, runtime-to-production oracle binding, and absence of secrets,
  machine-local paths, placeholders, or overclaim.
- Formal Tracker Gate
  `artifacts/gate-T-025-r17-0001.log` passed `scripts/check.sh` from clean
  commit `642095b`. Its SHA-256 is
  `d3b7054da9c212e9e596b17d45ce4fed6a59576299af0f639b65b8036afdc8d9`.
- T-020 and T-023 remain immutable historical evidence. T-025 owns only the
  later required-CI execution-boundary correction.

## T-024 — Reconcile The Shipped Restart-Recovery Guarantee

## Source And Boundary Binding

- Implementation commit:
  `6f5b0f699689f3a1f51cc9861b87d7ec08e887fd`; tree:
  `f6b87efc3b75096c5ab2fde47bb1ee87cc012790`; parent:
  `642095b7ced520dd4631a3262e7ef303104750e9`.
- The commit changes only `docs/architecture.md`. The Run-lifetime row now
  distinguishes shipped historical recovery under optional `--state-dir`
  from daemon-lifetime-only native process and PTY control.
- The architecture table agrees with `docs/protocol.md`, the M3.5 roadmap,
  and architecture decision 009: a previously running durable row becomes
  interrupted after daemon restart, and stored PID or PTY data never becomes
  live authority.
- Live PTY handoff, process adoption, host-reboot continuity, schema
  migration, and upgrade continuity remain open. No runtime, persistence,
  protocol, compatibility, or platform capability was added.

## Independent Audit And Formal Gate

- Independent exact-commit and message review found no P0/P1/P2, Feature
  drift, unsupported guarantee, machine-local path, secret, or stale semantic
  reference in the final amended commit. Git-message craft evidence is bound
  to the final hash in
  `.git/bagakit/git-message-craft/archive/2026-08-10-restart-recovery-ssot-t024-final.md`.
- Formal Tracker Gate
  `artifacts/gate-T-024-r19-0001.log` passed `scripts/check.sh` from clean
  commit `6f5b0f6`. Its SHA-256 is
  `d3b7054da9c212e9e596b17d45ce4fed6a59576299af0f639b65b8036afdc8d9`.

## T-012 — Retry-Safe Run Creation

## Residual Qualification Boundary

- Under deliberately concurrent local build/test pressure, the existing
  `persisted_replay_prunes_to_the_exact_per_run_budget_and_recovers_the_tail`
  fixture twice observed a terminal live head ahead of the durable head. The
  exact test then passed five consecutive standalone repetitions and the full
  persistence suite passed 9/9 serially; no exact persistence-actor error was
  captured.
- The current bounded native output drain can time out before a late output
  reader finishes. A later append against an already-terminal durable row may
  then latch persistence, but this session has not established a deterministic
  ordinary oracle for that sequence. T-012 does not widen into process-tree
  ownership, an unbounded drain, a larger arbitrary timeout, or a new
  source-closed protocol state to mask the uncertainty.
- T-007 qualification owns the follow-up: add a deterministic output-owner
  schedule before changing lifecycle semantics, then decide from evidence
  whether the bounded-drain limitation needs a separate reviewed correction.
  T-012 claims only its creation-key publication, retry convergence, and
  persistence/terminal activation guarantees.

## Source, Resource, And Gate Binding

- Final implementation commit:
  `47702bdc72be7943db3009b3aa1de0823f71efc3`; tree:
  `240fe9ab0ba4107753ea17ad057b6f83bd4d30a9`; parent:
  `6ccb464d5896b08846a2355a712fcb9ce5921840`. The worktree was clean for the
  formal Gate.
- Protocol generation 4 requires one bounded, byte-exact creation operation
  key for Start and Fork. Matching retries resolve the retained Run before
  current parent validation; conflicting reuse returns `creation_conflict`.
  Persistent schema 2 stores the key in the same Run row, so commit, recovery,
  retention, and collection cannot separate the mapping from its Run.
- The daemon uses 64 async stripes only for possible key collisions and a
  separate eight-permit launch admission bound. Unique launch ownership moves
  to a short-lived `ctxmux-create` thread; request cancellation cannot release
  its stripe, permit, or shutdown flight before publication. Shutdown closes
  admission, wakes queued waiters, and drains active flights against the same
  bounded deadline as tmux controls.
- The clean smoke receipt binds the final commit and daemon binary
  `e2cc16cc6eb4ecf0c73bd78aa65268cda2a7b6e9d7f07d481b2bcdfad63fc746`.
  Idle and active one-Run measurements each retained exactly 2 threads/Run and
  3 FDs/Run, with 5,376 KiB and 5,712 KiB peak daemon RSS respectively; every
  smoke stage and frozen budget passed. A non-retained current-tree nightly
  diagnostic also exercised 1/32/128 idle and active Runs at launch concurrency
  8 before commit; it informed optimization but is not substituted for the
  clean source-bound receipt.

## Independent Review And Formal Gate

- Rotating correctness, persistence, SDK, architecture, and resource reviews
  closed all P0/P1 findings. Final exact-commit and commit-message audit found
  no P0/P1/P2, no unrelated user change, and no Session, Agent, SSH, Input
  replay, global GC/quota, process-tree, or Backend-framework drift.
- Formal Tracker Gate `artifacts/gate-T-012-r26-0001.log` passed
  `scripts/check.sh` from clean final commit `47702bd`. Its SHA-256 is
  `d3b7054da9c212e9e596b17d45ce4fed6a59576299af0f639b65b8036afdc8d9`.
- The next planned task remains T-013: correlated attachment controls and
  applied PTY size. It does not inherit permission to add automatic Input
  replay, arbitrary Signal/process-tree policy, SSH transport, or global Run
  resource governance.

## T-013 — Correlated Attachment Controls And Applied PTY Size

## Source And Protocol Binding

- Final implementation commit:
  `3b481adef180e473b87a8a64d1bae22fa1ed8c94`; tree:
  `d553e555f91b966a3ed34eeb95e8e0d5de1d5ce1`; parent:
  `47702bdc72be7943db3009b3aa1de0823f71efc3`. The source worktree was clean
  for the formal Gate and source-bound reliability smoke.
- Protocol generation 5 replaces the obsolete control contract atomically.
  Short requests and attachments share typed `input`, `resize`, and `stop`
  receipts; attachment commands use connection-local increasing IDs and a
  separate `command_result` frame. IDs provide correlation only, never Input
  replay, deduplication, durable command identity, or permission to retry an
  unknown result.
- Every failure names `not_applied` or `unknown`. First-party Rust and
  TypeScript clients exact-encode before consuming an attachment ID, use one
  bounded receive router, validate receipt kind and payload, fence detach until
  pending results drain, and treat terminal-event EOF as clean without hiding
  unresolved control uncertainty.

## Native Owner, Backpressure, And Public Oracles

- The daemon-private `NativeControlOwner` is the current-incarnation authority
  for PTY input, resize, stop, and Level B continuation. `Open -> Stopping ->
  Closed` is independent of durable `RunState`; a same-epoch exited or stopping
  parent cannot use retained control-object presence as live Level B authority.
- Each Run admits at most 1,024 input commands and 4 MiB including an active
  write. Lazy input drains share at most eight daemon-wide blocking workers and
  hand off after 64 completed commands or 256 KiB. Resize and stop use separate
  lanes. A blocking PTY write has no independent deadline, so eight stalled
  writers may delay other Runs' input; the architecture states that limitation
  instead of adding a permanent per-Run thread or weakening the budget.
- The real PTY lifecycle suite passes 13/13. One fixture pipelines 1,000
  fragmented SGR-mouse and bracketed-paste byte chunks, verifies exact receipts
  and PTY bytes, reads back applied size, and orders Stop receipt, final output,
  and `Exited`. A non-reading PTY fixture reaches real daemon backpressure while
  resize and stop remain bounded; backward command IDs fail before mutation,
  and reconnect starts a new ID scope.
- Coverage ownership includes every new Rust and TypeScript module. The
  implementation-tree coverage lane passed all seven owners: Rust
  runtime/clients 90.07%, TypeScript SDK 90.36%, protocol/codegen 97.55%, and
  changed executable lines 90.08% (2,988/3,317) against parent `47702bd`.

## Resource, Review, And Formal Gate

- Clean source-bound smoke receipt
  `target/reliability/smoke/result.json` binds final commit `3b481ad`, tree
  `d553e55`, and daemon SHA-256
  `489312d79b0b8cc167f68bf107a54572b70e4f8b47e199361a2550de8aebce03`.
  Idle and active one-Run measurements retain exactly 2 threads/Run and 3
  FDs/Run, with 5,344 KiB and 5,824 KiB peak RSS respectively; cleanup has no
  live child or attachment. The receipt SHA-256 is
  `84963c8096c36d26fa2353c0a870b47a4a95c698e44af0f09140f2524d1da9a9`.
- Independent native-owner, cross-owner, Rust-client, TypeScript-SDK, and exact
  commit/message reviews found and closed live-authority, input-worker,
  event-waiter, outbound-preflight, terminal-EOF, Gate-mapping, and message-fact
  defects. Final exact audit of `3b481ad` reports no P0/P1/P2, Feature drift,
  omitted source, secret, machine path, or false Tracker binding.
- The first formal T-013 Gate attempt is retained as
  `artifacts/gate-T-013-r27-0001.log`: `scripts/check.sh` exited 101, but the
  Tracker summary did not retain the failing subprocess output and the result
  did not reproduce in the complete direct Gate immediately afterward. It is
  not cited as passing evidence.
- Final formal Tracker Gate
  `artifacts/gate-T-013-r28-0001.log` passed `scripts/check.sh` from clean exact
  commit `3b481ad`. Its SHA-256 is
  `d3b7054da9c212e9e596b17d45ce4fed6a59576299af0f639b65b8036afdc8d9`.
- T-013 does not implement Input idempotent replay, Signal/process-tree stop,
  SSH transport, Runtime Build/Host/Capability negotiation, daemon activation,
  package release, or global Run/key GC. The review-discovered unpublished
  child rollback correction and daemon-wide retained-state governance remain
  separate follow-up Tasks rather than being hidden inside this protocol slice.

## T-026 — Unpublished-Child Rollback Before Creation-Key Reuse

## Source And Owner Binding

- Final implementation commit:
  `4c4230e9423da9bbee2d83ea646dd9000f7afa4d`; tree:
  `c9fd26fc7c12a4b93480075fb93f2ddb94956e66`; parent:
  `3b481adef180e473b87a8a64d1bae22fa1ed8c94`. The worktree was clean for the
  passing formal Gate, source-bound smoke, and exact commit audit.
- The native waiter that owns the child handle remains the only
  terminal-and-reaped authority. A pre-COMMIT persistence rejection either
  proves reap inline or transfers the unpublished Run and exact operation-key
  fence to the daemon-private cleanup owner before the creation stripe and
  launch permit are released.
- Active launch reservations and transferred cleanup share one global
  eight-slot ceiling. A matching retry is explicitly unavailable while its
  fence remains; conflicting reuse remains a creation conflict; eight truly
  pending cleanup owners reject a ninth key before spawn. Reaped fences can be
  pruned across keys, so one stale lookup does not indefinitely block unrelated
  launch progress.
- Shutdown reports an unresolved private unpublished Run, fence, or waiter
  failure without publishing a fake Run or echoing the caller-owned operation
  key. The correction adds no cached-PID signalling, Session identity,
  persistent pending tombstone, process adoption, Backend framework, or
  general fault-injection layer.

## Public Proof And Resource Evidence

- The daemon all-target suite passed with 83 unit tests, 13 real native
  lifecycle tests, 9 persistence recovery tests, and 31 real tmux adapter
  tests. The focused 17-test creation group passed 20/20 consecutive rounds,
  covering real Start and Level B Fork persistence rejection, dead-PID and
  unrelated-process sentinels, same-key retry convergence, cross-key cleanup
  capacity, shutdown reporting, and operation-key privacy.
- Clippy passed with warnings denied. The coverage Gate passed all affected
  owners: Rust runtime/client 90.42%, `creation.rs` 95.07%,
  `native_control.rs` 90.68%, daemon `lib.rs` 90.41%, and retained
  changed-line coverage 91.94% (342/372) against parent `3b481ad`.
- Clean source-bound smoke receipt `target/reliability/smoke/result.json` binds
  commit `4c4230e`, tree `c9fd26f`, harness SHA-256
  `6efea4a72d5fbd9fcd59d197f868e8b337bcf62ea7228acde739de2c0dbdfdc2`,
  and daemon SHA-256
  `f18e36b7699841449f8882a3823c9b485c8cb782c4f77e1067d3f1ad4770b659`.
  The receipt SHA-256 is
  `eb1ceaf2da2b48e2e6592d083b81409505df86b879fdccde3c07154c2eb15925`.

## Independent Review And Formal Gate

- The first formal T-026 Gate attempt is retained as
  `artifacts/gate-T-026-r29-0001.log`: `scripts/check.sh` exited 101, and the
  Tracker summary did not retain subprocess output. Its SHA-256 is
  `17d79111fe3e65e79c57edafeeadb7faacb5fe35ff4c90751c0bff65342f1a74`.
  The failure was not rewritten or cited as passing evidence.
- Final formal Tracker Gate `artifacts/gate-T-026-r30-0001.log` passed
  `scripts/check.sh` from clean exact commit `4c4230e`. Its SHA-256 is
  `d3b7054da9c212e9e596b17d45ce4fed6a59576299af0f639b65b8036afdc8d9`.
- Independent exact-object and commit-message audit verified commit, parent,
  tree, nine-file scope, line references, privacy, and the owner-transfer
  contract. The final verdict was APPROVE with P0/P1/P2 all zero and no
  required amendment.
- T-026 supersedes the incomplete T-012 rollback conclusion only. It does not
  implement global retained Run/key GC, SSH transport, Signal/process-tree
  stop, Input idempotent replay, byte-cursor or generalized Backend event-loss
  semantics, Runtime Build/Host/Capability negotiation, activation, packaging,
  or release. Global resource governance remains T-027; non-output tmux
  observation loss remains the tmux Feature's T-002.

## T-033 — Representative Retained-State Convergence Closure

- Final implementation commit:
  `76a3cbc1a6af9f654b8edc8fafd0e86d5f033de2`; tree:
  `774aed0a2d5020e5d2168d5992786cb85dd8aa55`; parent:
  `5e88c39f01d0d29e9e14dc92bb5b480d00802e1f`. The worktree was clean for the
  formal Gate.
- The failing-first TDD initially failed to compile because the representative
  turnover fixture required an absent physical-spawn oracle. The minimal green
  change added that counter only to `CreationTestHook` and its existing
  test-only post-spawn seams; no production metric, API, actor, or budget model
  was added.
- One ordinary reduced-capacity test passed through memory-only and persistent
  owners, each with a four-Run fill and three complete turnover windows. It
  checks the exact retained Run/key/replay set, fresh-create physical spawn
  count, retry zero-spawn count, durable cursors, and a persistent restart after
  window two with no recovered incarnation control.
- Focused independent review first found that marker-only retry evidence could
  race with a late child write. After the synchronous test-only spawn oracle was
  added, the reviewer declared the P1 closed, found no new P0/P1, and confirmed
  there was no production-metrics or scope expansion. Commit/message audit also
  found no P0/P1 or omitted file.
- The first formal Gate receipt, `artifacts/gate-T-033-r42-0001.log`, records an
  exit 101 without subprocess output. On the same clean revision, workspace
  Clippy and all Rust workspace targets then passed, including 108 daemon unit
  tests, 13 native lifecycle tests, 9 recovery tests, and 31 tmux adapter tests.
  No workaround or extra validation layer was added for the one-off result.
- Final formal Tracker Gate `artifacts/gate-T-033-r43-0001.log` passed
  `scripts/check.sh` from the same clean exact commit. Its SHA-256 is
  `d3b7054da9c212e9e596b17d45ce4fed6a59576299af0f639b65b8036afdc8d9`.
- Bounded residual P2: the preserved, explicitly superseded T-030 historical
  design contains some present-tense qualification prose. It is not current
  Kernel work and does not reopen this finite closure.
