# Remote Runtime verification

Implementation checkpoint: `ed50f45` on the retired `feat/f-22hjbhvt8`, the merge
of `main`. That branch history was replaced by six focused commits on `main` with
an identical tree; see "Rebuild onto `main`" for the mapping and for why the SHAs
cited throughout this file are deliberately left pointing at the retired history.

This is an in-progress Feature record, not the final T-004 closeout receipt.
T-005, T-006, T-003, and T-007 are complete with passing Tracker gates. T-008 and
T-004 remain, and both are externally blocked rather than unstarted: T-008 needs
the real-Linux CI job, which fires only on a pull request, and T-004 closes the
documentation SSOT after it.

T-005 delivers the owner-host endpoint and supervision contract. T-006 publishes
`REMOTE_ENDPOINT_CONTRACT_VERSION` on the Rust and TypeScript client surfaces.
T-003 now qualifies the shipped transport against a real Linux owner host through
the real system `ssh` client.

## Automated Checks

- Command: `scripts/check-remote-runtime.sh --stage supervision`
- Result: exit 0. Supervision lane 8 + 7 tests passed. The 7
  (`remote_owner_host_endpoint.rs`) run against a real `ctxmuxd` and a real
  forwarding child process; the 8 are `ctxmux-remote` unit tests, which reach no
  daemon at all — that crate declares no ctxmux dependency, so six are pure
  argument and permission assertions and the two async ones spawn only
  directories and a shell stand-in. The argument-shape lane confirmed
  OpenSSH_10.2p1 accepts the production `-L` StreamLocal argument list.
- Command: `scripts/check-remote-runtime.sh --stage partition`
- Result: exit 0. The lane selected five ignored tests and reported five passes:
  owner-only Stop settlement after tunnel loss, instance-fenced daemon
  replacement, explicit truncation after outage eviction, replay and PID
  continuity, and bidirectional protocol/capability skew using distinct builds.
  Each fixture stops its own Run and tunnel through the public client; the
  absence of leftover helper or workload processes on the owner host was
  established by manual inspection after the lane, not by the lane itself, which
  runs no process census.

Evidence per T-005 acceptance bullet:

| Bullet | State | Evidence |
| --- | --- | --- |
| Reviewed decision records Remote as an endpoint, `RunBackend` unchanged, no component rewrites Backend truth | proven | `docs/architecture/choices/018-remote-endpoint-transport.md`; `RunBackend` is still exactly `Native \| Tmux`, and this Feature changes no protocol generation. `git diff main...HEAD -- crates/ctxmux-protocol/` is **not** empty, and an earlier revision of this row wrongly said it was: the crate gains the `REMOTE_ENDPOINT_CONTRACT_VERSION` constant, its pin test, and the codegen line that exports it to TypeScript. All three are additive client-side facts — no frame, request, event, error code, wire field, or generation changed by this Feature, and the constant reaches no serialized struct. The merged wire is generation 14, raised by main's base64 PTY-output change, which is independent of this Feature; an earlier revision of this row asserted 13 as a current fact and became stale at that merge rather than being wrong when written |
| Bounded supervised tunnel over system OpenSSH `StreamLocal`, caller arguments passed through, no credential touched | proven | `crates/ctxmux-remote/src/lib.rs`; unit tests on the argument list; the argument-shape lane proves the real client accepts it |
| Explicit typed failure rather than prompt or stall; owner-only socket directory removed with the tunnel including abnormal exit | proven | `BatchMode`/`ExitOnForwardFailure` asserted by unit test; `each_tunnel_gets_a_fresh_owner_only_directory`; `shutdown_removes_the_socket_directory_and_process` and `dropping_the_guard_cleans_up` now observe the forwarder's process group dying, not the path teardown just unlinked. An audit found both had named a process claim they could not check: path absence is satisfied by `cleanup_private_dir` alone, and a refused connect at an unlinked path fails with `ENOENT` regardless of whether the forwarder lives. Disabling both kills and `kill_on_drop` left them green before and fails both after, with the leaked forwarder still alive |
| Existing client boundary reaches the owner unchanged; no second RPC layer, wire frame, generation, or recovery state machine | proven for Rust; derived for TypeScript | `forwarded_socket_reaches_the_owner_host_runtime` compares full identity through the tunnel against the owner. The TypeScript SDK addresses a daemon by socket path only, so a forwarded socket needs no SDK change; T-006 owns the cross-language fixture |
| Exact expected identity required; fails closed before any business frame; no local fallback | proven | `a_tunnel_to_another_runtime_fails_closed_before_dispatch` returns `RuntimeIdentityMismatch` and leaves the intended owner untouched |
| Transport loss leaves the remote child running, publishes no lifecycle transition; reattach from the caller's own cursor | proven | `losing_the_tunnel_is_not_lifecycle_truth` (owner still `Running`, same pid); `output_written_while_disconnected_replays_from_the_caller_cursor` |
| Nothing provisioned silently; missing listener is an explicit error | proven | no provisioning code path exists. `a_missing_owner_host_listener_fails_closed` now asserts the failure point the shipped transport has: a `-L` forward to a dead owner binds locally, so the endpoint establishes and the first request is refused for reachability, and the test additionally asserts no listener was created on the caller's behalf and that a retry keeps failing. An earlier revision asserted establishment-time refusal, which only held because the stand-in forwarder pre-checked the remote path; that pre-check is removed, and the mutation that reinstates it fails this test |
| Two required lanes prove the vertical without an SSH boundary | proven | the two lanes above |
| The real-OpenSSH lane is written and reachable, but its evidence belongs to T-003 | proven | `crates/ctxmux-daemon/tests/remote_real_openssh.rs`; `scripts/check-remote-runtime.sh --stage partition` selected five tests and passed 5/5 |

Evidence per T-003 acceptance:

| Acceptance | State | Evidence |
| --- | --- | --- |
| owner-only Stop settlement after tunnel loss | proven | exact retained Stop operation replayed the remote forced disposition; no local receipt or terminal state was synthesized |
| instance-fenced recovery across daemon replacement | proven | restarting the placed owner binary produced a new `daemonInstanceId`; the old operation was rejected before lookup or mutation |
| exact replay and explicit truncation | proven | the real SSH lane replayed from the caller cursor and separately reported truncation after retained history eviction |
| bidirectional version/capability skew | proven | protocol-13 client to protocol-12 owner and protocol-12 client to protocol-13 owner used distinct compiled builds; absent capability failed typed and pre-dispatch |
| exact identity and resource cleanup | proven for identity; manual for the process census | Runtime identity and PID survived reconnect, asserted by the fixture. Every fixture stops its Run through the public client. The "no orphan tunnel, helper, or workload" half is a manual post-lane inspection on the owner host: no automated assertion counts processes, and the only `ps` in the real-SSH fixtures locates the daemon to restart for the fence test |

## Manual Checks

- Step: Mutate each guarded invariant and confirm the owning test fails, then
  revert and confirm the revert. Removed `ExitOnForwardFailure` from the fixed
  option list; removed the socket unlink from cleanup; forced the client's
  expected-identity comparison to always pass.
- Outcome: each mutation failed exactly the test that claims the property — the
  option unit test, both teardown tests, and the identity fail-closed test. No
  assertion is vacuous. All three mutations were reverted and the reverts
  verified. A later audit qualified the teardown half of this claim: removing the
  unlink is caught, but removing the *kill* was not, because both teardown tests
  read the socket path rather than the process. Both now observe the forwarder's
  process group, and the kill mutation fails them; see Post-review corrections.
- Step: Run the production `ssh` argument list against the real system client
  outside the test harness.
- Outcome: it fails at host resolution, never at option parsing, so the stand-in
  forwarder is not concealing a malformed invocation.
- Step: Inspect release output after a clean rebuild.
- Outcome: `target/release` contains `ctxmux` and `ctxmuxd` only. The test-only
  forwarder is excluded because the build selects binaries rather than packages.
- Step: Verify process-group teardown against the real syscalls in isolation.
- Outcome: a group signal reaps a grandchild that a signal to the direct child
  alone leaves running. A fixture modelling ssh's helper topology was written,
  failed to model it faithfully, and was deleted rather than kept as a
  misleading test; the invariant is recorded in decision 018 instead.

## External consumer question

The AgentMux blocker saying that its vendored `@ctxmux/sdk` has no Remote
connector is still valid. This Feature provides the lower layers, but not that
consumer-facing composition:

1. `ctxmux-remote` owns the supervised OpenSSH tunnel and yields a local socket.
2. `@ctxmux/sdk` can use a forwarded socket unchanged and already exposes
   `runtimeInfo()`, exact `expectedRuntimeIdentity`, and fail-closed
   `requiredCapabilities`.
3. Existing Run IDs, cursors, recoverable operations, Runtime identity, and
   capability checks retain their meanings across a partition; T-003 qualifies
   those meanings over real SSH.

The TypeScript package does **not** currently create or supervise the OpenSSH
tunnel, expose a combined host/build/capability endpoint object, or own a second
partition-recovery state machine. Host identity deliberately stays in
caller-side endpoint configuration and OpenSSH host-key policy, outside
`RuntimeIdentity`. T-007 is local Runtime activation and explicitly permits no
SSH fallback, so it does not close this gap. T-004 binds delivered behavior to
documentation and must not invent a connector during closeout.

Therefore `REMOTE_UNSUPPORTED` remains the correct AgentMux result, and AgentMux
must not build a private SSH proxy. Closing that blocker inside this Feature
would require a new reviewed task after T-007 and before T-008/T-004, owned at a
ctxmux public client boundary. It is not hidden inside T-003.

## Post-review corrections

Successive review and audit rounds over the delivered work found five claims that
outran their evidence and two real defects. All are fixed on this branch; each is
recorded here because several were assertions this very record relied on, and
because two of them are the same regression class caught twice — once in a branch
the first fix did not reach, once in a test named for a property it never
observed.

**A crashed launcher was reported at the deadline, not at the crash.** When a
launcher fails, `activateRuntime` searches for the activator that won the socket
path, because a loser must reuse the Runtime that beat it. That search ran to
the caller's deadline regardless of what it observed, so a launcher that died in
180ms surfaced only after the whole budget elapsed — 30s by default, and as
`readiness_timeout` rather than `launcher_exited` whenever the crash landed
after the deadline. The search now reads the socket path as evidence: a path
that exists but does not yet serve Hello means a winner is mid-handshake and
keeps the full deadline, while a path nothing has bound is bounded by a short
grace window that any observation promotes back to the full deadline. Measured:
a 30000ms budget reported `launcher_exited` after 30002ms before and 510ms
after. The first attempt at this fix was wrong in the opposite direction — it
treated an absent path as a syscall-scale race and broke reuse of a late winner
in all three probe lanes — which is why the retained discriminator is what was
observed rather than how long it took.

**The qualification script asserted a false protocol history.** Its header said
this project had been generation 13 since its first commit and therefore
prescribed building the skew binary by patching a constant. The first commit
declares generation 1, the 12-to-13 bump is an ordinary ancestor of main, and
its parent tree builds a genuine generation-12 `ctxmux` and `ctxmuxd` by plain
checkout in 27 seconds — verified by doing it. This matters beyond tidiness: a
patched constant proves only that the current wire disagrees with a number,
while a checked-out generation is a real build of a real past wire, which is
what the skew fixture exists to exercise. The Manual Checks entry above about
the argument list stands, but see the next item for its true reach.

**Two documented invariants overstated their reach.** Readiness by connect
proves this client's forwarder is listening, not that the owner host is alive,
because `-L` binds locally; the Hello exchange immediately after is what attests
the owner, so the system still fails closed. And the argument-shape lane
restates the argument list rather than calling `tunnel_args`, so the builder's
real output is never fed to a real client — two independent assertions guard the
shipped invocation and a change touching both together could still ship a
malformed one. Decision 018 now marks that wrong-case entry partial and names
the mechanical fix it does not yet claim.

**A test asserted the negation of the shipped fail-closed point.** A later audit
lane found that `a_missing_owner_host_listener_fails_closed` required
`connect` to return `TunnelExited` for a dead owner. That is not what the real
client does: `ExitOnForwardFailure` guards the **local** bind for `-L`, so a
`StreamLocal` forward whose remote has no listener still binds locally and stays
up. The assertion passed only because the stand-in forwarder checked
`forward.remote.exists()` and exited before binding — the test was pinned to the
stub, not the product, and the two records above cited it as proof of an
establishment-time guarantee. The fix is at the root rather than in the wording:
the stand-in's pre-check is gone, so it now discovers a dead remote per
connection like the real client, and the test asserts what a consumer actually
relies on — the endpoint establishes, the first request is refused for
reachability, no listener is created on the caller's behalf, and a retry keeps
failing. Reinstating the pre-check makes the new test fail with
`TunnelExited`, so it pins the regression rather than restating a passing
condition. The previous entry below, which treated this as an overstatement in
documentation only, understated it.

**Two teardown tests were named for a process they never observed.** A third
audit round mutated `terminate()` to skip its kill and `Drop` to leak its child,
and both `shutdown_removes_the_socket_directory_and_process` and
`dropping_the_guard_cleans_up` stayed green while a forwarder survived the run.
Each was reading the socket path: after shutdown the first asserts only that the
socket and tunnel directory are gone, which `cleanup_private_dir` satisfies by
itself, and its one liveness check runs *before* shutdown; the second inferred
process death from a refused connect, which teardown's own unlink turns into
`ENOENT` whether or not the forwarder lives. Both now capture the tunnel's group
leader while it is live and poll for that group to disappear, which required a
`leader_pid` accessor — teardown deletes the only path they had been watching, so
there was no honest way to observe it from outside. The same mutation now fails
both at the polling deadline with the leaked forwarder still alive. This was a
test-strength defect, not a product one: an unmutated run leaves zero forwarders,
and `the_tunnel_child_leads_a_group_that_teardown_reaches` already guarded the
group kill, which is why the leak never shipped.

**A crashed launcher stalled for the whole budget when a corpse socket existed.**
The same regression class as the first entry above survived in a branch that
entry did not reach. `activateRuntime`'s search for the activator that won the
socket path treated *any* present path as a winner mid-handshake, but a path
whose connect is refused is a corpse a daemon left behind when it died without
unlinking — evidence against a winner, not for one. Both refused and
accepted-then-dropped probes land on the same `stale` target, so the fix carries
the distinction with it and promotes only an answered path. Measured with a
leftover socket present: reported after 2002ms of a 2000ms budget and 5002ms of
a 5000ms budget before, 689ms and 705ms after, so the delay no longer scales
with the caller's deadline. The existing guard was vacuous for this branch
because its socket path is never created, so only the absent case was ever
exercised; reverting the discriminator fails the added test at 30034ms of a
30000ms budget.

 A
second audit round found the supervision entry claiming all "8 + 7 tests passed
against a real `ctxmuxd`", when the 8 are `ctxmux-remote` unit tests that reach
no daemon — that crate declares no ctxmux dependency, so the claim was not merely
loose but structurally impossible. The same round found the partition entry
claiming "the lane also verified that no tunnel helper or workload process
remained", when the stage function runs no process census at all; the only `ps`
in the real-SSH fixtures locates the daemon to restart for the fence test. Both
lines are corrected above, and the cleanup row now labels the process census as
manual. The counts themselves (8, 7, five ignored, five passes) were accurate;
what outran the evidence was the attribution.

**`detach_output_race` was characterized, not fixed, and does not block merge.**
1624 runs produced 0 failures, including under load average 138. The ack is
flushed at `attachment.rs:401` before the counter decrements in the guard drop
at `lib.rs:4441`, with no await point between, so this is a test-side
synchronization defect over an advisory gauge rather than a product defect.

## Persistence storage-pressure classification

Folded in from a reported AgentMux incident rather than found by review: a
generic `SQLITE_IOERR` entered the persistence actor's failure latch, after which
every mutation was refused, while restarting the daemon against the same state
directory validated and wrote fine. That restart is the proof the two conditions
are different — a latched actor is not a corrupt store — and the disk had
temporarily filled.

The primary code cannot make that distinction: every `SQLITE_IOERR_*` shares the
primary `SystemIoFailure`, so classification now reads the extended code. Four
write-path codes an operator can clear — `SQLITE_IOERR_WRITE`, `_FSYNC`,
`_DIR_FSYNC`, `_TRUNCATE` — join `SQLITE_FULL` and WAL-checkpoint contention as
boundedly retryable. The allowlist is deliberately not "any `SystemIoFailure`":
read, lock, delete, and mmap failures and every unrecognized extended code stay
fail-closed, because retrying an unreadable or corrupt store converts a visible
latch into an unbounded hang.

Retry is safe against double-commit because the append path already refuses to
duplicate: `append_replay` classifies each chunk against `durable_head`, so a
chunk already durable is re-verified byte-for-byte and skipped, a
non-abutting chunk is refused as a gap, and `finalize` updates only a `running`
row, so a second attempt yields `updated != 1`. That was verified in the source
before the retry was allowed to depend on it, then pinned by a test rather than
left as reasoning.

Three tests carry this. One asserts the classification directly, including that
`SQLITE_IOERR_READ`, `SQLITE_CORRUPT`, and `SQLITE_NOTADB` still latch — a
reverse mutation, so the allowlist cannot silently widen. One is the incident
drill the report asked for: inject one I/O failure into a temp state directory,
confirm the mutation is retried rather than latching, reopen the same directory,
and confirm the existing Run recovers and new mutations continue. One replays a
committed append and asserts it verifies instead of duplicating.

**Still open, and deliberately not in this branch:** a consumer sees
`ErrorCode::Persistence` plus a message string, which is exactly the shape that
invites AgentMux to parse error text — the thing it must not do. Publishing the
transient-vs-fail-closed distinction on the wire is a protocol-surface change,
and this branch's premise is that no wire changes. It needs its own reviewed task.

## Automated Checks (post-review candidate)

- Command: `scripts/check.sh`
- Candidate: `d437b80` on `feat/f-22hjbhvt8` — the tip after the audit
  corrections, so the gate covers the rewritten fail-closed test rather than an
  ancestor of it. This is now itself an ancestor: the persistence
  classification, the corpse-socket activation fix, and the teardown-test
  strengthening land after it, so the complete gate must be re-run on the merge
  candidate before readiness. Recorded here as the last full-gate evidence, not
  as the closeout receipt.
- Result: exit 0 as a complete run: 215 script checks and 44 Rust suites passed
  with 0 failures, ending on the reliability receipt rather than mid-stream.
- The completeness check matters here because a previous run of this same
  candidate exited 1 while its background notification reported success. The gate
  reports a formatting failure as `repository check core did not reach its
  completion boundary` with no `✖`, `FAIL`, or non-zero test-result line, and
  piping the exit status through `tee` had reported the pipeline's status instead
  of the gate's. The failure was a Prettier violation in the 018 table row edited
  by the fail-closed correction, fixed in `d437b80`.
- Local evidence on the affected cone, run separately: the endpoint suite passes
  7/7, `ctxmux-remote` plus `ctxmux-protocol` pass 28, and the SDK
  `remote-endpoint` lane passes against a real `ctxmuxd` behind the stand-in
  forwarder.

## Merge with main

`main` had diverged: 31 commits ahead of it on this branch, 7 behind. The merge
is a real merge commit, not a fast-forward, and `main` is now an ancestor of this
branch with nothing left unmerged.

Three conflicts required a decision rather than a side. The protocol test's
import list wanted `OutputChunk` and `REMOTE_ENDPOINT_CONTRACT_VERSION`; both are
used, so it is their union. The architecture guarantees table conflicted whole,
because `main` rewrote every row for generation 14 while this branch had
rewritten one for the owner-host endpoint; `main`'s table is the base with this
branch's `Backends` row applied over it, since the two describe independent
facts. The fixture reachability check existed in two shapes for the same fix, and
`main`'s named helper compares the command's words rather than a prefix, so it
tolerates a strengthening flag in any position — strictly stronger, so it is kept
along with the comment explaining why an exact string match is wrong.

`crates/ctxmux-daemon/src/persistence.rs` merged cleanly despite both sides
touching it: `main` added tests at the end of the module, and the
storage-pressure classification sits in the implementation above.

**The merged wire is generation 14, and this Feature still changes no
generation.** `main` raised it by encoding PTY output as strict padded base64.
The row asserting `PROTOCOL_VERSION` is 13 was a current-state claim that this
merge made stale rather than a claim that was wrong when written; it now states
the durable fact instead.

## Automated Checks (merge candidate)

- Command: `scripts/check.sh`
- Candidate: `6a0e22a` on `feat/f-22hjbhvt8` — the merge of `main` plus its
  record, so this is the first full gate that covers every change in this
  Feature rather than an ancestor of some of them.
- Result: exit 0 as a complete run. 44 Rust suites passed with 0 failures and
  269 script checks passed, ending on `Reliability receipt: smoke qualification
  passed with source-bound evidence` rather than mid-stream.
- Coverage of this round's changes was confirmed by name in the log rather than
  assumed from the summary: the storage-pressure classification test, the
  corpse-socket activation test, both rewritten teardown tests, and the
  owner-host endpoint suite all appear in the passing output.
- The first attempt on this same candidate exited 1 at the very last step with
  `artifact source worktree must be clean`. That was an operator error, not a
  defect: `test:local-consumer` packs artifacts from the worktree to prove a real
  consumer can install them, and the tracker record was still uncommitted when
  the gate started. The check is correct to refuse. Its cost is a whole cycle,
  because it fails after every Rust suite has already passed.
- The harness reported that first run as "exit code 0" while the gate had exited
  1, which is the same trap recorded above: the reported status was the
  wrapper's, and the gate's own failure sentence carries no `✖` or `FAIL`.



- Command: `scripts/check.sh`
- Candidate: `c9bf994` on `feat/f-22hjbhvt8`.
- Result: recorded by the Tracker gate receipt for T-007.
- The activation suite passes 9/9. The added lane asserts *when* a crashed
  launcher is reported rather than which error it produces, and fails on the
  pre-fix code at 30002ms of a 30000ms budget, so it pins the regression rather
  than restating the existing assertion.
- Concurrent convergence holds across 12 consecutive runs of that lane, and
  across four rounds of ten activators driving a deliberately slow shell
  launcher, where every round converged on exactly one Runtime with no
  rejections.

## Automated Checks (T-007 closeout)

- Command: `scripts/check.sh`
- Candidate: `c9bf994` on `feat/f-22hjbhvt8`.
- Result: recorded by the Tracker gate receipt for T-007.
- The activation suite passed 9/9 at that candidate. The added lane asserts
  *when* a crashed launcher is reported rather than which error it produces, and
  fails on the pre-fix code at 30002ms of a 30000ms budget, so it pins the
  regression rather than restating the existing assertion. A later round found
  that lane covered only the absent-socket branch and added a tenth for the
  corpse-socket one; see Post-review corrections.
- Concurrent convergence holds across 12 consecutive runs of that lane, and
  across four rounds of ten activators driving a deliberately slow shell
  launcher, where every round converged on exactly one Runtime with no
  rejections.

## Linux compile finding

The non-macOS `process_ids()` branch previously mapped
`sysinfo::Pid::as_u32` over `HashMap::keys()`. The iterator yields `&Pid`, while
the function item accepts `Pid`, producing E0631 and a consequent E0599. macOS
never compiled that cfg branch. The retained closure fix is
`.map(|pid| pid.as_u32())`.

After the fix, `cargo zigbuild --locked --workspace --all-targets --target
x86_64-unknown-linux-gnu` completed with no additional Linux source failures.
Plain cross-target Cargo remains environment-blocked because bundled SQLite
needs `x86_64-linux-gnu-gcc`; this is why the Zig route is the reproducible local
compile path, not proof of T-008's real-Linux execution requirement.

**Historical:** `T-001` covered this same vertical but also required the
real-OpenSSH evidence. It finished `blocked` because that earlier machine had no
SSH boundary. Revision 3 moved the requirement to T-003 and T-005 supersedes it;
the earlier attempt and its gate log remain in the plan history.

## Rebuild onto `main`

The work was re-committed directly onto `main` as six focused commits rather
than merged as the branch's development history. The tree is identical; only the
commit boundaries changed, so every result recorded above still describes the
same source.

Every SHA cited in this file above this section refers to the retired
`feat/f-22hjbhvt8` history and is deliberately **not** repointed. Those SHAs
name the exact candidates the recorded gate runs actually executed against, and
rewriting them to the new commits would claim runs that never happened on those
commits. They remain readable in the reflog and on `origin/feat/f-22hjbhvt8`.

| Retired SHA | Carried the | Now in |
| --- | --- | --- |
| `0a97cd8` | non-macOS `process_ids()` compile fix | `fix(daemon): compile the non-macOS process enumeration` |
| `d437b80`, `c9bf994`, `6a0e22a`, `ed50f45` | tracker, docs, and fixture corrections plus the `main` merge | `docs: record the owner-host endpoint decision and its evidence` |

The rebuilt commits are separately verifiable rather than only verifiable as a
set: each was checked to leave `scripts/ci-reachability.mjs` satisfied at its own
boundary. That surfaced a real ordering defect during the rebuild — the remote
transport commit initially added two Rust suites while their CI evidence
declarations and the checker change that honors a non-required lane sat in a
later commit, which would have left an unbuildable-gate point in history. Both
were moved into the commit that introduces the suites.

What this section does not claim: no new gate run. The complete-gate evidence
above stands on the retired candidates. Re-running `scripts/check.sh` on the new
`main` tip is required before this Feature's closeout, and T-008's real-Linux
requirement is unaffected.

## Residual Risks

Remaining gates:

- T-008 still requires the complete `scripts/check.sh` gate to reach its final
  completion boundary on a real Linux x86_64 candidate. The successful Zig
  all-workspace/all-targets cross-build proves Linux compilation, including the
  macOS-invisible cfg islands, but does not satisfy real Linux execution.
- The complete gate has been run on the merge candidate `6a0e22a`: exit 0 as a
  complete run, with this round's persistence, activation, and teardown changes
  confirmed present in the passing output by name. What remains for T-008 is that
  the same gate reach its completion boundary on real Linux, not that it be
  re-run here.
- A consumer still cannot tell a transient storage failure from a fail-closed one
  without reading the message string. The daemon now classifies correctly and
  retries internally, so the reported incident cannot recur, but the distinction
  reaches no wire field. Publishing it is a protocol-surface change and needs its
  own reviewed task; until then AgentMux must surface the daemon's own outcome
  rather than inspecting error text or touching the database.
- T-004 must update the documentation SSOT last. Architecture choice 018 and
  `docs/architecture.md` have since been corrected to state real-SSH
  qualification as achieved rather than pending, so the pre-T-003 wording that
  used to sit there is gone; T-004's remaining scope is the roadmap and any
  surface not yet swept, not those two files.

Accepted risks carried into merge:

- The real-SSH partition lane qualifies the build it ran against, not a later
  one. It needs an owner host, a second older build on both ends, and a
  memory-only current daemon, so it runs on demand rather than in PR CI. Treat a
  green partition stage as evidence with a date and re-run it when the wire, the
  endpoint, or the daemon's identity handling changes.
- The shipped `ssh` invocation is guarded by two independent assertions rather
  than one mechanical derivation: unit tests pin `tunnel_args`' exact output, and
  the qualification script feeds a hand-written copy of that list to a real
  client. A change editing the builder and both assertions together could still
  ship a malformed invocation. Decision 018 marks that wrong-case entry partial
  and names the fix it does not yet claim.
- `detach_output_race` is characterized, not fixed. 1624 runs produced 0
  failures, including under load average 138, and the ack is flushed before the
  counter decrements with no await point between, so it is a test-side
  synchronization defect over an advisory gauge. It does not block merge.
- AgentMux's `REMOTE_UNSUPPORTED` blocker stays open by design. This Feature
  ships the lower layers, not a consumer-facing Remote connector; closing it
  needs a separately reviewed task at a ctxmux public client boundary, and must
  not be smuggled into T-004 closeout.
