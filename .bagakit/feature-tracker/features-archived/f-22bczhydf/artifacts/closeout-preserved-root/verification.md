# Verification Evidence

## T-002 Native Wait Owner Proof

## Automated Checks
- Command: `cargo test -p ctxmux-daemon --lib native_wait_error_fail_stops_once_without_dropping_or_signalling_child -- --nocapture`; `cargo test -p ctxmux-daemon --lib`; `cargo clippy -p ctxmux-daemon --all-targets -- -D warnings`; `scripts/check.sh` through the Task Gate.
- Result: The focused fake-child oracle, all 112 daemon unit tests, formatting, Clippy, Rust workspace tests, TypeScript unit/E2E, and reliability policy checks passed. The first formal Task Gate reached reliability smoke and then correctly rejected a dirty source-bound invocation; the clean-HEAD Gate result is recorded separately by Feature Tracker.

## Manual Checks
- Step: Independently inspect child-handle ownership, control phase monotonicity, shutdown reporting, public lifecycle truth, and the deterministic fake-child counters without editing the implementation tree.
- Outcome: The first `try_wait` error transfers the real child handle into one irreversible native fail-stop owner, polls exactly once, calls no kill/wait/clone-killer path, publishes no fabricated terminal state, rejects later controls with `backend_unavailable`, fences creation, and makes shutdown report the Run id plus first wait error. Read-only owner review found no open P0/P1.

## Residual Risks
- A Stop admitted immediately before the failure fence may lose its reply and remain `unknown`; controls begun after the fence are `not_applied` with `backend_unavailable`.
- The daemon fails the serving incarnation instead of inventing a same-epoch terminal event. Persistent restart retains the existing `interrupted { daemon_restart }` reconciliation.

## T-004 Gate Stability Correction

### Findings and dispositions

- Reliability-policy timeout: the launcher-envelope fixture performed 14 real
  Bash runs through about 95 Node shebang stub starts. Uncontended individual
  outer runs were below 0.9 seconds, while CPU/filesystem contention produced
  one 5.001-second `spawnSync` timeout. The fixture now keeps the real launcher,
  exact argv/order assertions, NUL-framed records, and the unchanged five-second
  watchdog while using a lightweight Bash stub. Three post-fix focused runs
  completed in 2.80-3.07 seconds for the whole target test.
- Interactive resize observation: direct attachment resize/readback and a real
  child `stty size` lifecycle oracle each passed 10/10 under the same load. The
  failing controlling-PTY trace already contained `40 120`, but repeated probe
  input echo split `SIZE:` from the size bytes. The fixture now disables child
  echo and emits each probe as one `printf`; the resize deadline remains five
  seconds and no runtime resize code changed.
- Public daemon fail-stop: the earlier fake-child test proved the owner state
  but not the shipped socket behavior. One minimal fixture now runs a dedicated
  daemon runtime, attaches through `ctxmux_client`, triggers the same fake wait
  failure, and proves `ClientError::Closed` before any terminal event, a
  `ServerError::Shutdown` containing the Run and first error, and no remaining
  connectable daemon socket.
  No CLI, TypeScript, persistence-restart, or pending-command matrix was added.

### Exact source-bound Gate

- Source: `7ca249abcb4d6464683f542ca9935fce498aba22`, clean tracked worktree.
- Command: `scripts/check.sh`.
- Result: exit 0. The run included 118/118 policy tests, 113/113 daemon unit
  tests, 2/2 interactive CLI tests, 15/15 native lifecycle tests, 9/9
  persistence recovery tests, 31/31 tmux adapter tests, 51/51 TypeScript unit
  tests, 3/3 public E2E tests, and the final source-bound reliability smoke
  receipt.

### Remaining T-004 boundary

- These corrections close the two observed Required-Gate failures and the
  minimal public daemon-exit proof. The T-006 correction Task owns the broader
  bounded Kernel review map after T-004 was closed before that map existed;
  this evidence does not mark unrelated review, retained-state, release, tmux
  qualification, or performance work complete.

## T-003 Persistent Exact-Replacement Proof

### Acceptance evidence

- Pre-launch admission: the persistence actor reaches a zero-WAL baseline,
  disables cache spill, stages the exact candidate transaction, and proves the
  SQLite cache-to-WAL charge before the creation owner may spawn. Metadata and
  page admission failures remain `not_committed`, return `run_capacity`, leave
  candidates and their exact keys intact, and consume no physical start.
- Commit disposition: the affine staged-start receipt is monotonic across
  `not_committed`, `committed`, and `commit_unknown`. Only `not_committed`
  restores Registry candidates; `committed` consumes the exact Registry
  reservation; an unclassifiable result transfers the reservation into the
  incarnation fail-stop fence and never reopens a key.
- Atomic durable unit: candidate Run rows, byte-exact creation keys, cascaded
  replay, metadata accounting, and the successor row change in one SQLite
  transaction. Real subprocess crashes immediately before and after COMMIT
  recover the complete old unit or the complete new unit, never a hybrid.
- Frozen bounds and scope: startup normalization and ordinary creation share
  the accepted record, metadata, replay, database, WAL, SHM, and transaction
  ceilings. The implementation remains one private persistence actor plus one
  affine start owner; it adds no public transaction API, migration, durable
  collection ticket, tombstone, or PID adoption.

### Focused checks and independent review

- Focused commands covered real COMMIT crash recovery, failed-COMMIT
  old/new/hybrid classification, startup normalization, same-epoch exact
  Run/key replacement, post-COMMIT recovery, pre-spawn capacity rejection,
  panic/unwind fencing, and three turnover windows with restart. All eight
  focused tests passed on source `7ca249abcb4d6464683f542ca9935fce498aba22`.
- Three read-only reviewers independently inspected persistence/Registry owner
  transfer, crash and restart oracles, public protocol and documentation,
  frozen resource bounds, and scope. No reviewer found a P0 or demonstrated
  P1 implementation defect. One reviewer proposed additional Cartesian
  fault-injection combinations; these remain non-blocking because the existing
  owner-local failure oracles, manager bridge tests, production
  multi-candidate prefix support, single-candidate exact failure tests, and real
  process-crash recovery already distinguish the required shipped contracts
  without adding a second test-control layer.

### Residual boundary

- The canonical sustained 128-record pressure and resource plateau remains
  owned by T-005. Memory-only public Fork/tmux no-mutation evidence remains
  owned by T-001, and the eight-domain independent Kernel review remains owned
  by T-006.

## T-001 Memory-Only Retained-Run Proof

### Acceptance evidence

- Start, Fork, and tmux import share the Registry-owned projected-capacity
  reservation before physical Backend mutation. Deterministic cap-one oracles
  now prove a valid Fork cannot produce a second native spawn owner and a
  public tmux import cannot enter discovery or Control startup when no exact
  terminal candidate is eligible.
- `RegistryState` is the single truth for Runs, exact creation keys,
  publication reservations, and `Retained`/`Collecting` residency. Status and
  List copy public state; Attach, control, and fresh Fork atomically pin the Run
  or fail explicitly while the Registry lock still protects residency.
- Candidate order is terminal publication ordinal then Run id. Eligibility
  requires the sole Registry `Arc`, terminal state, zero attachments, and
  Backend-local quiescence. Publication removes the exact candidate and key
  and inserts the successor under one Registry write; reservation unwind
  restores the exact fence.
- Public Collecting/removed errors, replay loss, lineage, retry convergence,
  key reuse, cap-one Fork, reverse publication, shutdown, and the reduced-cap
  equivalent of 127-to-129 projection all have executable owner or public
  oracles. Persistent exact replacement is independently closed by T-003, so
  T-001 no longer claims a legacy durable path while that work is pending.
- No background collector, TTL, public delete API, Session identity, Backend
  framework, compatibility layer, or second reservation truth was added.

### Focused checks and independent review

- Nine existing owner/collection tests and the two strengthened no-mutation
  tests passed. Rust formatting and daemon Clippy passed after the test-only
  seam moved native counting to successful `Run::spawn_pending` ownership and
  added the tmux import-start boundary.
- Three read-only reviewers inspected Registry linearization, native/tmux
  quiescence, exact key removal, public protocol behavior, test failure
  sensitivity, and scope. Initial review found that publication-time native
  counting and an unreachable tmux socket could falsely pass; the corrected
  oracles were independently rerun and reviewed with no remaining blocker.

### Residual boundary

- Production 128-record sustained churn and resource plateau remain T-005.
  Persistent replacement is T-003; cross-domain Kernel review is T-006; tmux
  product qualification remains outside this Feature.

## T-006 Bounded Run-Kernel Review

### Review map

| Lens | Independent owner | Evidence inspected | Result |
| --- | --- | --- | --- |
| Native lifecycle | Tesla the 2nd (`t002_waiter_owner_review`) | PTY publication, reader/waiter ordering, stop, exit drain, wait-authority loss, daemon fail-stop | No P0/P1 |
| Registry and keys | Hooke the 2nd (`t002_test_seam_review`) | reservation projection, Retained/Collecting pin, exact key removal, unwind, lock order | No P0/P1 |
| Replay and attachment | Zeno the 2nd (`t002_public_model_review`) | subscribe/snapshot join, sequence deduplication, Gap recovery, bounded queues, detach and terminal ordering | No P0/P1 |
| Protocol and clients | Zeno the 2nd | Rust/TypeScript handshake, validators, single-reader routing, correlated controls, public errors | No P0/P1; one recorded P2 |
| Fork boundary | Zeno the 2nd | Kernel capability, caller-materialized Level B plan, Level A clone, live authority, lineage, fail-closed Backend rejection | No P0/P1 |
| Persistence | Hooke the 2nd | page/WAL admission, affine receipt, exact transaction, COMMIT classification, crash/restart | No P0/P1; two evidence P2s corrected |
| Security | Tesla the 2nd | socket ownership, stale/replacement fencing, malformed transport, cached-PID negative space | No P0/P1; one known routed P2 |
| Resource ownership | Tesla the 2nd | Run/control/reader/waiter/input/creation/cleanup owners, quiescence, bounded lanes, shutdown aggregation | No P0/P1 |

All groups also reviewed product drift, owner count, lock order, failure
disposition, public errors, SSOT, complexity, naming, and deterministic
evidence on exact source `5f91872c869bd200998808d74440e517a2a47e20`.
No reviewer found Agent, scheduling, release, SSH, activation, tmux product
qualification, or performance policy in the Run Kernel. Integration semantic
provenance remains an SDK-host WeakMap and checked receipt; the daemon retains
only Backend capability, caller-materialized plan, lineage, and fail-closed
runtime enforcement.

### Findings and dispositions

- P0: none.
- P1: none.
- P2, corrected SSOT: Decision 013 incorrectly assigned physical-start and
  COMMIT disposition to the Registry ticket. The text now reflects the real
  split: Registry owns residency/projection, creation owns physical start, and
  the persistence receipt owns durable disposition. No redundant runtime state
  was added.
- P2, corrected evidence claim: Decision 013 called its durable fixture
  multi-candidate even though deterministic COMMIT/crash injection uses one
  exact candidate. The text now distinguishes production prefix support from
  the shipped single-candidate failure oracle and leaves a multi-candidate
  crash case as non-blocking hardening.
- P2, corrected protocol history: Decision 003 retained generation-6 wording
  under a generation-7 title. It now states the generation-7 incarnation and
  recoverable-Input delta and the exact generation-6 replacement boundary.
- P2, corrected daemon constraint: Decision 001 still claimed no global
  retained-Run quota. It now records the shipped 128 retained/projected
  Registry ceiling while preserving the absent separate active-Run,
  attachment, and total-RSS quotas and pending plateau qualification.
- P2, routed security residual: Unix `bind` followed by `chmod 0600` is not an
  atomic publication boundary in an attacker-writable parent directory, and
  no peer-credential policy is claimed. Decision 003 and the casebook already
  own this limitation; it does not block the declared owner-controlled local
  path.
- P2, recorded client hardening: Rust and TypeScript attach clients validate the
  generation-7 header shape but do not yet correlate `snapshot.run.id` with the
  requested Run or share a cross-field replay-cursor validator. The shipped
  daemon pins the requested id before constructing the header, so no normal
  daemon wrong-Run path was reproduced. Correlated header validation remains a
  candidate hardening item for review by the owning protocol/client Feature,
  rather than a new requirement in this Kernel closeout.

### Focused evidence and limits

- Tesla reran `native_wait_error_fail_stops_once_without_dropping_or_signalling_child`,
  `native_wait_failure_exits_daemon_without_a_terminal_event`,
  `published_socket_has_owner_only_permissions`, and
  `stale_socket_replacement_race_preserves_the_unrelated_live_target`; all four
  focused tests passed.
- Hooke reran
  `ordinary_exact_replacement_recovers_old_or_new_around_real_commit_crash`,
  `failed_commit_actor_route_distinguishes_old_new_and_hybrid_units`, and
  `persistent_commit_unwind_never_restores_the_registry_reservation`; all three
  focused tests passed.
- Zeno inspected the executable public oracles
  `public_gap_reattaches_from_the_callers_cursor_without_loss_or_duplicates`,
  `output_overflow_gap_precedes_later_tmux_event`,
  `attachment_pipeline_preserves_raw_bytes_applied_size_and_stop_ordering`,
  `level_a_fork_clones_declared_inputs_and_runs_independently`, the Codex
  provenance wrong cases, and the Rust/TypeScript validator and single-reader
  owners. Exact source `5f91872` had already passed the T-001
  `scripts/check.sh` Task Gate, which executes those ordinary suites; this
  review does not present that Gate as a substitute for lens analysis.
- File size alone was not treated as a defect. The review found high internal
  complexity but no second owner truth, speculative Backend interface, general
  transaction API, or lock-order cycle. Owner and RAII names remain aligned
  with their actual failure disposition.
- Tmux version qualification, activation, packaging, platforms, release, peer
  performance, sustained resource plateau, process-tree stop policy, and the
  routed socket residual plus recorded client-hardening candidate remain
  outside this bounded correction review.

## T-005 Retained-State Plateau Qualification

### Exact source and canonical commands

- Source commit: `07329aa66eb9a8356ab8b67ee7eb528991e4ff60`.
- Source tree: `0f7c2c17130d6c975337d9644430c06f4046b750`.
- The v3 receipt records `worktree.clean=true` with no status entries and a
  source-bound locked daemon build from that exact commit and tree.
- `scripts/check.sh` completed with exit 0 on the clean exact source. The
  formal Tracker rerun is retained at
  `artifacts/gate-T-005-r10-0001.log` with SHA-256
  `d3b7054da9c212e9e596b17d45ce4fed6a59576299af0f639b65b8036afdc8d9`.
- `scripts/check-reliability.sh --profile nightly` completed with exit 0 and
  the production v3 verifier reported a source-bound pass. The receipt is
  `target/reliability/nightly/result.json`, SHA-256
  `8c91583c930db94c13954fccb0fb465ae5219c237620dbea3ede7da2793fe5f3`.

### Frozen identity and workload

- Seed: `226004`; time budget: 4200 seconds; ordinary soak: 1800 seconds.
- `reliability-budgets.json` remained
  `3ae8d7bfb91c85c77c5e375f7546444337d3763844f954cdfdc01e9541c3b9e1`.
- `reliability-gc-contract.json` remained
  `92960ffd3369b5f408c3df2846f7fa4170fc1c0023b6c21ac518d04783af8049`.
- `scripts/reliability-gc-child.mjs` remained
  `468e9e63cbee05eb9ba82f80a242580c9bc206e089e21e280b90a1cac8d4a54e`.
- The receipt binds the harness, launcher, locked daemon binary, RSS sampler
  binary and both sampler sources, lockfiles, toolchain, host, measurement
  contract, workload contract, and helper. It also binds 22 complete stats
  logs by individual SHA-256, daemon instance, and final sequence, plus the 23
  daemon-log paths. These raw files remain under
  `target/reliability/nightly/`; no copied or summarized file replaces them.

### Plateau, restart, replay, and resource evidence

- Memory-only and persistent modes each completed one 128-Run fill and three
  128-Run turnover windows: 512 successful lifecycles per mode. Every fill and
  window had exactly 128 physical starts, every retry start delta was zero,
  every window retained 128 Runs, and each window performed exactly 128
  replacements. Candidate evaluation stayed at the declared 128×128 bound per
  window; the final recorded maximum per publication was 128.
- Persistent restart occurred after turnover window 2. The before and recovered
  128-tuple Run/key/state/cursor/chunk/replay digest was exactly
  `7a24980ba281002373dbaecb114b3b398a7f6763059c053a512a25e7af1a0a21`;
  churn then completed window 3 without a duplicate physical start or mixed
  durable unit.
- Both maximum replay-pressure modes proved 128 exact 4 MiB replays before and
  after replacement, totaling 512 MiB at each boundary. All tuples had
  `oldest_seq=1` and `truncated=false`; all 128 before and after tuples were
  independently replay-verified. Fill start delta was 128, replacement start
  delta was 8, and retry start delta was zero in both modes.
- The ordinary soak ran 1801.98 seconds for 1754 cycles with eight live Runs.
  It ended with zero cleanup thread delta, live children, and attachments.
  Both replay-pressure modes ended with 128 retained Runs and exact keys while
  creation flights, publication reservations, collection tickets, overlap and
  cleanup owners, children, readers, waiters, input drains, attachments, and
  tmux owners were all zero. The persistent recovered observation additionally
  recorded zero quiescent CPU, thread, and FD deltas.
- All six receipt stages are `pass`, including the frozen-resource-budget
  stage. Peak replay-pressure RSS was 588528 KiB memory-only and 542624 KiB
  persistent; maximum RSS sample gaps were 33 ms and 51 ms respectively,
  inside the frozen cadence contract.

### Independent read-only review and residual boundary

- A post-run read-only acceptance audit found no open P0/P1 in the T-005
  evidence. The persistence batching commit was separately reviewed as one
  owner-local transaction correction; its message was narrowed to the observed
  pressure failure rather than claiming an unmeasured PTY-loss mechanism.
- This qualification proves the finite Run-Kernel retained-state Goal. It does
  not claim an attachment fan-out quota, total daemon RSS quota, tmux product
  qualification, activation, packaging, SSH, process-tree stop policy, peer
  performance, or Agent semantics. Those remain owned outside this Feature.
