# Verification Evidence

## Qualified Candidate

- Commit: `96b82813f72b1bfb23bbd1bc26c2b7ea73e1d9eb`
- Tree: `f8aa60bbad399d2137a85c50bd04570ecbf73743`
- Verdict: `READY`; independent review found no open P0 or P1 issue in the
  accepted Recoverable Stop claim.
- Capability: the qualified Runtime advertises `native.recoverable_stop: 1`.
- Source state: the exact candidate remained clean throughout qualification;
  its generated build artifacts were removed afterward and no residual test
  process remained.

## Automated Checks

- Command: run the exact-candidate Recoverable Stop owner/parity stages, SDK
  unit/E2E suites, packed consumer, and repository `scripts/check.sh` gate.
- Result: all commands passed on commit `96b82813` / tree `f8aa60bb`; the
  independent qualification found no P0/P1 and left no residual test process.
- `scripts/check-recoverable-stop.sh --stage rust-owner`: passed on the exact
  candidate. The selected real owner/client tests covered duplicate join,
  settled replay, conflict-before-mutation, response loss, fresh-client retry,
  daemon replacement, and one physical complete-session Stop owner entry.
- `scripts/check-recoverable-stop.sh --stage public-parity`: passed on the exact
  candidate. Attachment convergence, short-request recovery, planned-exec
  continuity, collection/key reuse, and process-session cleanup remained green.
- `npm --prefix packages/sdk test` and the repository SDK E2E path: passed on
  the exact candidate, including generated declarations, runtime validation,
  caller-retained operation keys, disconnect recovery, and wrong cases.
- `npm run test:local-consumer`: passed from packed public artifacts without a
  repository-private import. A fresh client recovered the original Stop result
  without a second physical Stop effect.
- `scripts/check.sh`: passed on the exact candidate. This joined the Rust
  workspace, TypeScript unit/E2E suites, CLI/SDK parity, generated contract,
  documentation consistency, CI evidence map, reliability policy, and packed
  consumer gates.
- RSS qualification regression: the fixture now publishes its exact PID before
  readiness and script tests run with `--test-concurrency=1`. Focused RSS tests
  passed 13/13, CI reachability passed 10/10, the relevant reliability policy
  passed 3/3, and the exact script runner passed 151/151 under load above 100.

## Manual Checks

- Step: inspect the exact candidate's operation identity, one-owner Stop
  admission, cross-client recovery, retention/collection, planned-exec
  continuity, public type generation, capability, and documented limits.
- Outcome: the independent reviewer accepted the full finite claim with no
  open P0/P1; every public path joins the same daemon-owned Stop result and no
  compatibility layer or second Stop owner was introduced.
- The review bound operation identity to daemon instance, Run ID, and the
  caller-owned operation key; exact duplicates join or replay one receipt,
  while conflicting reuse is rejected before mutation.
- Short requests and attachment controls converge on the same daemon ledger.
  Attachment command IDs remain connection-local correlation and are not used
  as recovery identity.
- The existing complete-session Stop state machine remains the sole owner of
  signalling, cleanup, reap, and POSIX-session quiescence. Recoverability adds
  no second Stop owner and no generic operation framework.
- The ledger remains bounded to one admitted Stop entry per retained Run,
  collection removes Run and key ownership together, planned exec preserves
  settled recovery within the daemon incarnation, and cold replacement rejects
  the old operation.
- Rust wire types, generated TypeScript declarations, SDK validation, CLI JSON,
  capability documentation, and protocol generation agree without compatibility
  aliases, migrations, or fallback encodings.

## Residual Risks

- Recoverable Resize and Interrupt, Remote Runtime, crash-time or host-reboot
  live-process adoption, AgentSession/Provider semantics, Workbench close
  transactions, package publication, and external consumer adoption are outside
  this Feature.
- The first call can still observe `unknown` when its response is lost after
  admission; recovery requires retrying the retained exact operation within the
  same daemon incarnation.
- Descendants that deliberately escape the owned POSIX session with `setsid()`
  remain outside the complete-session ownership claim, and the documented PID
  revalidation syscall gap retains a small TOCTOU risk.
