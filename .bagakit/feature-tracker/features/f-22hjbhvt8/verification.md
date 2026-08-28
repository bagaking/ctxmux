# Remote Runtime verification

Implementation commit: `69ea572` on `feat/f-22hjbhvt8`.

Result: the owner-host endpoint is delivered and its supervision contract passes.
A forwarded socket reaches the same owner Runtime, exact-identity selection is
refused before dispatch when the Runtime differs, transport loss leaves the remote
child running with the same pid and publishes no lifecycle transition, output
written while disconnected replays from the caller's own byte cursor, a missing
owner-host listener fails explicitly, and teardown removes the socket, its
directory, and the forwarding process. `scripts/check.sh` and
`--stage supervision` both exit 0. The shipped transport itself is not yet
qualified: no run has carried the protocol over the real system `ssh` client, and
that evidence is owned by T-003.

## Automated Checks

- Command: `scripts/check-remote-runtime.sh --stage supervision`
- Result: exit 0. Supervision lane 7 + 7 tests passed against a real `ctxmuxd`
  and a real forwarding child process; the argument-shape lane confirmed
  OpenSSH_10.2p1 accepts the production `-L` StreamLocal argument list.
- Command: `scripts/check.sh`
- Result: exit 0 for the complete repository gate, including the reliability
  smoke receipt.
- Command: `scripts/check-remote-runtime.sh --stage partition`
- Result: exit 1, which is the designed behavior with no SSH boundary
  configured. That lane is owned by T-003 and fails rather than skips, so the
  shipped transport cannot be reported as qualified by a run that never spoke
  SSH.

Evidence per T-005 acceptance bullet:

| Bullet | State | Evidence |
| --- | --- | --- |
| Reviewed decision records Remote as an endpoint, `RunBackend` unchanged, no component rewrites Backend truth | proven | `docs/architecture/choices/018-remote-endpoint-transport.md`; `git diff main -- crates/ctxmux-protocol/` is empty |
| Bounded supervised tunnel over system OpenSSH `StreamLocal`, caller arguments passed through, no credential touched | proven | `crates/ctxmux-remote/src/lib.rs`; unit tests on the argument list; the argument-shape lane proves the real client accepts it |
| Explicit typed failure rather than prompt or stall; owner-only socket directory removed with the tunnel including abnormal exit | proven | `BatchMode`/`ExitOnForwardFailure` asserted by unit test; `each_tunnel_gets_a_fresh_owner_only_directory`; `shutdown_removes_the_socket_directory_and_process`; `dropping_the_guard_cleans_up` |
| Existing client boundary reaches the owner unchanged; no second RPC layer, wire frame, generation, or recovery state machine | proven for Rust; derived for TypeScript | `forwarded_socket_reaches_the_owner_host_runtime` compares full identity through the tunnel against the owner. The TypeScript SDK addresses a daemon by socket path only, so a forwarded socket needs no SDK change; T-002 owns the cross-language fixture |
| Exact expected identity required; fails closed before any business frame; no local fallback | proven | `a_tunnel_to_another_runtime_fails_closed_before_dispatch` returns `RuntimeIdentityMismatch` and leaves the intended owner untouched |
| Transport loss leaves the remote child running, publishes no lifecycle transition; reattach from the caller's own cursor | proven | `losing_the_tunnel_is_not_lifecycle_truth` (owner still `Running`, same pid); `output_written_while_disconnected_replays_from_the_caller_cursor` |
| Nothing provisioned silently; missing listener is an explicit error | proven | no provisioning code path exists; `a_missing_owner_host_listener_fails_closed` |
| Two required lanes prove the vertical without an SSH boundary | proven | the two lanes above |
| The real-OpenSSH lane is written and reachable, but its evidence belongs to T-003 | proven | `crates/ctxmux-daemon/tests/remote_real_openssh.rs` exists and is driven by `--stage partition`; this task claims no transport qualification |

## Manual Checks

- Step: Mutate each guarded invariant and confirm the owning test fails, then
  revert and confirm the revert. Removed `ExitOnForwardFailure` from the fixed
  option list; removed the socket unlink from cleanup; forced the client's
  expected-identity comparison to always pass.
- Outcome: each mutation failed exactly the test that claims the property — the
  option unit test, both teardown tests, and the identity fail-closed test. No
  assertion is vacuous. All three mutations were reverted and the reverts
  verified.
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

## Residual Risks

**The shipped transport is unqualified.** Every automated result above comes from
the stand-in forwarder plus the argument-shape guard. No run has carried the
protocol over the real system `ssh` client. T-003 owns that evidence and its lane
fails rather than skips, so this gap cannot be closed silently.

**The TypeScript boundary is derived, not demonstrated.** A forwarded socket
needs no SDK change because the socket path is the entire addressing model, but
no checked-in TypeScript fixture exercises it. T-002 owns that.

**Platform asymmetry is untested.** The local build is macOS on `aarch64`. A
remote Linux owner host would exercise the assumption that endpoint selection and
identity comparison do not require a matching platform — an assumption a
downstream consumer currently hard-pins. T-003's boundary is a remote Linux host
for exactly this reason.

**Not covered here:** probeable `remote.*` capability constants and the public
endpoint surface (T-002); remote Stop receipts under partition, explicit
truncation when retained history is evicted during an outage, and bidirectional
version and capability skew (T-003); documentation and commit closure (T-004).

**Historical:** `T-001` covered this same vertical but also required the
real-OpenSSH evidence. It finished `blocked` because this machine has no SSH
boundary. Revision 3 moved that requirement to T-003 and `T-005` supersedes it;
the earlier attempt and its gate log remain in the plan history.
