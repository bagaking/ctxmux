# Remote Runtime verification

## T-001 — minimal owner-host vertical

Implementation commit: `69ea572` on `feat/f-22hjbhvt8`.

### Status

The vertical is implemented and its supervision evidence passes. The task is
recorded **blocked**, not done, for one honest reason: the real-OpenSSH lane its
acceptance requires cannot run on this machine, because there is no SSH boundary
to an owner host (`ssh localhost` is refused: `Permission denied
(publickey,password,keyboard-interactive)`).

That lane fails rather than skips, by design. Remote must not be reportable as
qualified by a run that never spoke SSH, so the gate stays red until a boundary
exists. Weakening the lane to make the gate green was rejected.

### What passes today

`scripts/check-remote-runtime.sh --stage vertical`, first two lanes:

```
== remote supervision lane ==
  ctxmux-remote unit tests            7 passed
  remote_owner_host_endpoint          7 passed
== real ssh argument shape ==
  OpenSSH_10.2p1, LibreSSL 3.3.6
  accepted the -L StreamLocal argument shape
== real OpenSSH lane ==
  error: the real OpenSSH lane has no destination.   <- blocks this task
```

The complete repository gate `scripts/check.sh` passes (exit 0), including the
reliability smoke receipt.

### Evidence per acceptance bullet

| Bullet | State | Evidence |
| --- | --- | --- |
| Reviewed decision records Remote as an endpoint, `RunBackend` unchanged, no component rewrites Backend truth | proven | `docs/architecture/choices/018-remote-endpoint-transport.md`; `git diff main -- crates/ctxmux-protocol/` is empty |
| Bounded supervised tunnel over system OpenSSH `StreamLocal`, caller arguments passed through, no credential touched | proven | `crates/ctxmux-remote/src/lib.rs`; unit tests on the argument list; the argument-shape lane proves the real client accepts it |
| Explicit typed failure rather than prompt or stall; owner-only socket directory removed with the tunnel including abnormal exit | proven | `BatchMode`/`ExitOnForwardFailure` asserted by unit test; `each_tunnel_gets_a_fresh_owner_only_directory`; `shutdown_removes_the_socket_directory_and_process`; `dropping_the_guard_cleans_up` |
| Existing client boundary reaches the owner unchanged; no second RPC layer, wire frame, generation, or recovery state machine | proven for Rust; derived for TypeScript | `forwarded_socket_reaches_the_owner_host_runtime` compares full identity through the tunnel against the owner. The TypeScript SDK addresses a daemon by socket path only, so a forwarded socket needs no SDK change; T-002 owns the cross-language fixture when it publishes the public endpoint surface |
| Exact expected identity required; fails closed before any business frame; no local fallback | proven | `a_tunnel_to_another_runtime_fails_closed_before_dispatch` returns `RuntimeIdentityMismatch` and leaves the intended owner untouched. Mutating the client's comparison to always pass makes this test fail, so it is not vacuous |
| Transport loss leaves the remote child running, publishes no lifecycle transition; reattach from the caller's own cursor | proven | `losing_the_tunnel_is_not_lifecycle_truth` (owner still `Running`, same pid); `output_written_while_disconnected_replays_from_the_caller_cursor` |
| One real OpenSSH fixture proving survival, reconnect, identity continuity, and replay or truncation; fails loudly when the boundary is absent | **blocked** | `crates/ctxmux-daemon/tests/remote_real_openssh.rs` is written and covers all four, including an `attach` assertion. It has never executed here: no SSH boundary. The fail-loud half is proven — the lane exits non-zero with actionable instructions |
| Nothing provisioned silently; missing listener is an explicit error | proven | no provisioning code path exists; `a_missing_owner_host_listener_fails_closed` |

### How to unblock

Enable a loopback boundary, then run the same command:

```
sudo systemsetup -setremotelogin on
ssh-keygen -t ed25519                       # if no key exists
cat ~/.ssh/id_ed25519.pub >> ~/.ssh/authorized_keys

CTXMUX_REMOTE_SSH_DESTINATION=localhost \
CTXMUX_REMOTE_SOCKET=/path/to/ctxmux.sock \
  scripts/check-remote-runtime.sh --stage vertical
```

A remote Linux owner host is the better boundary, because it also exercises the
platform asymmetry a same-machine loopback hides.

### Test cost

The endpoint suites run in about 1.4 s. They execute one at a time: each starts a
real `ctxmuxd` and a real forwarder, and the shared spawn permit is
process-local, so unserialized they pushed unrelated suites past their readiness
budgets. Serializing removed that interference; the whole file still finishes in
about a second.

### Not proven by this task

Probeable `remote.*` capability constants and the public endpoint surface
(T-002); remote Stop receipts under partition, explicit truncation when retained
history is evicted during an outage, and bidirectional version and capability
skew (T-003); documentation and commit closure (T-004).
