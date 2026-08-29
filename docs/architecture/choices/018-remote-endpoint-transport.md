# 018 — Remote is an endpoint, not a Backend

- Status: accepted; implemented and qualified over a real SSH boundary
- Scope: carrying the existing protocol to an owner-host `ctxmuxd` over the caller's system OpenSSH

## Context

A Run owned by a daemon on another machine should stay durable when the local
client or its network disappears. A downstream consumer asked for this as a
fourth `RunBackend` variant beside `native` and `tmux`, carrying the remote host
identity, the remote daemon's build and protocol version, and a runtime identity
that survives reconnect. Their hard constraints were that connection and
authentication use the caller's own system SSH, that the consumer hold no
credentials, and that partition recovery reuse the same ordered-byte semantics as
local rather than growing a second recovery state machine.

Those requirements are right. The requested shape is not, and the difference
matters enough to record.

## Decision

`RunBackend` remains `native | tmux`. Remote is an endpoint and transport
concern owned above the wire.

### Why not a fourth Backend variant

A Backend names the actual runtime owner of a Run. An owner-host `ctxmuxd` owns a
real PTY and a real child process; from its own vantage that Run _is_ `Native`,
and that is the honest answer. Publishing `backend: "remote"` would require one
of two things, and both are worse than the problem they solve:

1. the owner-host daemon misreporting what it owns, so that a Backend value
   describes the caller's network position rather than the runtime owner; or
2. the local client rewriting a Backend value it received, which makes a client
   the author of wire truth it cannot attest.

Host identity is deliberately outside `RuntimeIdentity`. A daemon cannot attest
its own host, and by the time the first frame is sent, OpenSSH has already proven
host trust with the user's configured policy. Host identity therefore belongs to
caller-side endpoint configuration, and the value re-verified across reconnect is
`runtimeId`, which already exists for exactly this purpose.

A third Backend variant would also force a third `RunCapabilities` profile. The
public capability and replay assertions currently express a two-profile world;
splitting them for a remote variant would fork the replay contract, which is the
mechanism by which a consumer would acquire the second recovery state machine it
asked us to avoid.

### What satisfies the requirements instead

| Requirement                         | Owner                                                                                                                                                                                                                                                        |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| probeable capability                | the client-side `REMOTE_ENDPOINT_CONTRACT_VERSION` constant for what this client can establish, plus the existing flat numeric capability record for what the owner-host daemon can do, enforced by the existing client-local fail-closed pre-dispatch check |
| identity across reconnect           | existing `runtimeId` plus `daemonInstanceId`, compared through the existing exact-identity expectation                                                                                                                                                       |
| connection, credentials, host trust | the system OpenSSH client and the caller's existing configuration                                                                                                                                                                                            |
| ordered bytes, replay, gap          | the local contract, reused verbatim                                                                                                                                                                                                                          |

The last row is the point of the whole design. Because remote is the same
protocol over the same socket contract, `after_byte`, `first_available_byte`,
`truncated`, and `Gap` keep their exact local meanings. There is no second
recovery state machine to keep in agreement, because there is no second
protocol.

### Ownership split

- **The owner-host daemon** owns the PTY, child, Run identity, lifecycle,
  ordered output, replay, and Stop settlement. It is unchanged by this decision.
- **OpenSSH** owns authentication, host-key policy, `~/.ssh/config`,
  `ProxyJump`, and agent forwarding. Caller arguments are passed through so all
  of that keeps working.
- **`ctxmux-remote`** owns one supervised `ssh` process and one caller-private
  local socket. It never parses a protocol byte; it produces a socket path.
- **The existing client** owns identity and capability enforcement against Hello
  on the dispatch connection, exactly as it does locally.

### Transport loss is reachability, never lifecycle

Losing the tunnel means this client cannot currently observe the owner. It does
not mean the Run exited or was interrupted. Reachability is a client-local fact
and is never published as a `RunState` or an `InterruptionReason`. Only the
owner-host daemon may publish lifecycle truth or a Stop receipt.

This is the property that makes remote safe for a consumer to build on: a
network blip must not be indistinguishable from an exit.

### Fixed `ssh` options

Four options are fixed rather than left to the caller:

- `-N` and `-T`, because this connection exists only to carry a forward.
- `-o BatchMode=yes`, because without it a missing credential turns a
  programmatic call into an invisible interactive stall. With it, the caller gets
  a fast explicit failure and can repair their own SSH setup.
- `-o ExitOnForwardFailure=yes`, because without it `ssh` can hold a live session
  whose forwarded socket carries nothing — a socket that looks usable and is not.

Caller arguments are appended after these, and the destination is passed last so
it cannot be parsed as an option. A destination beginning with `-` is refused
outright rather than forwarded into an argument list.

Their **position** is the guarantee, not their presence. OpenSSH resolves a
repeated option to its first occurrence, so a fixed option is unbypassable only
while it precedes `extra_args`: a caller passing `-o BatchMode=no` then loses to
the fixed value instead of overriding it. Extending `extra_args` ahead of the
fixed list would silently reopen both the interactive-stall and inert-forward
wrong-cases, so the unit test asserts the index relationship against a caller
supplying contrary values, not mere membership.

### No provisioning

Nothing is uploaded, installed, version-matched, or spawned on the owner host. A
missing owner-host listener is an explicit error. Deployment stays entirely with
the operator, so no ctxmux code path can silently place or start a binary on
another machine.

## Quality attributes and invariants

- Readiness is proven by connecting to the forwarded socket, never by a delay. A
  path is returned only after it actually accepts a connection. Note the precise
  reach of that: `-L` binds locally, so a successful connect proves this client's
  forwarder is listening, not that the owner-host daemon is alive. The owner is
  proven by the Hello exchange the dispatch connection performs immediately
  after, which is where a dead owner fails closed. Readiness bounds the endpoint;
  it does not attest the far side.
- The forwarded socket lives in an owner-only directory and is removed with its
  tunnel, including on abnormal client exit.
- An unobservable tunnel process is never reported as a healthy one.
- `ssh` is spawned as its own process-group leader and teardown signals the whole
  group. A caller's configuration may add a `ControlMaster` mux or a
  `ProxyCommand` child, and those survive a signal aimed only at the direct
  child, leaving an authenticated channel open after the caller believed the
  endpoint was closed. This one is easy to assert and easy to get wrong: the
  group signal and the group leadership are set in different functions, and for a
  time this record claimed the guarantee while `spawn_tunnel` omitted
  `process_group(0)`, so the signal went to the caller's own group. Nothing in an
  argument list or a passing shutdown test could show that, which is why the
  evidence for it drives a real helper process instead.
- Each tunnel gets its own owner-only directory, created `0o700` in one step
  rather than created and then tightened, and never adopts a directory it did not
  create.
- Shutdown is bounded, so a wedged forwarder cannot hold the caller open.
- No frame, request, event, error code, or protocol generation is added.

## Alternatives

**A fourth `RunBackend` variant, as requested.** Rejected above: it requires
either a daemon that misreports its owner or a client that forges wire truth, and
it forks the capability and replay profile.

**A remote-specific replay or reachability protocol.** Rejected. It would create
the second recovery state machine the consumer explicitly asked us to avoid, and
the local contract already expresses every needed case.

**An SSH library (`ssh2`, `russh`, `libssh2`) instead of the system client.**
Rejected on external evidence. Editors that took this path re-implement
`~/.ssh/config` parsing incompletely and lose `ProxyJump`, agent forwarding, and
host-key verification; the projects that shell out to the system client keep all
of it and hold no credentials. A library would also put ctxmux in the credential
path, which the consumer contract forbids.

**A Relay or second RPC layer.** Rejected as a non-goal. It would add a product
surface, an authentication story, and a deployment story to solve a problem that
one forwarded socket already solves.

**Reporting transport loss as `interrupted`.** Rejected. It would let a network
blip impersonate lifecycle truth, which is precisely the failure a durable Run
model exists to prevent.

## Wrong-case corpus（错题集）

- **A forwarded socket that carries nothing.** Without
  `ExitOnForwardFailure=yes`, a refused forward can leave a live session whose
  socket is inert. _Disposition: active_ — the fixed option is asserted by unit
  test, and readiness is proven by an actual connect. Be exact about the reach:
  `ExitOnForwardFailure` fires when `ssh` cannot establish the forward, which for
  `-L` means the **local** bind. A `StreamLocal` forward whose remote socket has
  no listener still binds locally and stays live, so readiness is satisfied and
  the dead owner surfaces on the first dispatch frame instead. The system fails
  closed either way — at Hello rather than at readiness — and the endpoint
  fixture asserts that later point, since asserting the earlier one would claim a
  strictness the shipped client does not have.
- **An interactive stall in a programmatic call.** A missing credential without
  `BatchMode=yes` waits forever on a prompt no one sees. _Disposition: active_ —
  the fixed option is asserted by unit test, including that it precedes any
  contrary caller argument, since OpenSSH honours the first occurrence.
- **A destination parsed as an option.** A destination like
  `-oProxyCommand=...` would execute a command instead of naming a host.
  _Disposition: active_ — refused at construction, with a test.
- **A leaked forwarder.** A dropped guard that leaves `ssh` running holds an
  authenticated channel open after the caller believes it closed.
  _Disposition: active_ — covered by the drop-cleanup test.
- **Transport loss read as death.** A consumer that treats an unreachable owner
  as an exited Run kills work that is still running. _Disposition: active_ —
  covered by the non-terminal transport-loss test, which asserts the owner still
  reports `Running` with the same PID.
- **A stand-in forwarder hiding a broken argument list.** Testing only against a
  test double could ship a malformed `ssh` invocation. _Disposition: partial_ —
  unit tests assert the builder's exact output and the qualification script
  asserts a real client accepts that shape, but the script restates the list
  rather than calling the builder, so the two can drift together. See the
  qualification boundary.
- **A reused local socket path resolving to a different Runtime.** A stale path
  could reach an unrelated daemon. _Disposition: covered_ — the existing
  exact-identity comparison on the dispatch connection rejects it before any
  business frame.

## Fixture mapping

| Case                                                       | Evidence                                                                                                                                                                                       |
| ---------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| forward reaches the same Runtime                           | `crates/ctxmux-daemon/tests/remote_owner_host_endpoint.rs`                                                                                                                                     |
| transport loss is not lifecycle truth                      | same, `losing_the_tunnel_is_not_lifecycle_truth`                                                                                                                                               |
| cursor replay across a partition                           | same, `output_written_while_disconnected_replays_from_the_caller_cursor`                                                                                                                       |
| missing owner-host listener fails closed                   | same, `a_missing_owner_host_listener_fails_closed` — asserts refusal on first use plus that nothing was provisioned, which is where a `-L` forward actually fails closed                       |
| socket, directory, and process cleanup                     | same, `shutdown_removes_the_socket_directory_and_process` and `dropping_the_guard_cleans_up`                                                                                                   |
| exact-identity selection fails closed through a tunnel     | same, `a_tunnel_to_another_runtime_fails_closed_before_dispatch`                                                                                                                               |
| teardown reaches the helpers, not just the direct child    | `crates/ctxmux-remote/src/lib.rs`, `the_tunnel_child_leads_a_group_that_teardown_reaches` — drives a real background helper, since no argument list can show a process group                   |
| real system OpenSSH carries the vertical, including replay | `crates/ctxmux-daemon/tests/remote_real_openssh.rs`, `real_openssh_carries_the_owner_host_vertical` — qualified against a real owner host; the lane fails rather than skips without a boundary |
| owner Stop settlement survives losing the tunnel           | same, `real_openssh_stop_receipt_survives_tunnel_loss`                                                                                                                                         |
| a replaced daemon incarnation is refused, not adopted      | same, `real_openssh_rejects_a_replaced_daemon_instance` — restarts the real owner daemon rather than forging an identity                                                                       |
| eviction during an outage is reported as truncation        | same, `real_openssh_reports_truncation_after_outage_eviction`                                                                                                                                  |
| version skew is refused in both directions                 | same, `real_openssh_rejects_bidirectional_build_skew` — two genuinely distinct builds, each rejecting the other                                                                                |
| argument, permission, and validation contract              | `crates/ctxmux-remote/src/lib.rs` unit tests                                                                                                                                                   |
| production arguments accepted by real `ssh`                | `scripts/check-remote-runtime.sh --stage supervision`                                                                                                                                          |
| the TypeScript SDK reaches the owner host unchanged        | `packages/sdk/test/remote-endpoint.test.ts`, driven by `--stage capability`                                                                                                                    |

## Qualification boundary

The implemented vertical proves forwarding, exact-identity selection and its
fail-closed rejection, identity continuity, non-terminal transport loss, cursor
replay, fail-closed absence, and cleanup — over the stand-in forwarder. The real
system client is separately confirmed to accept that argument list.

Be precise about how strong that second confirmation is. The qualification
script hand-writes the argument list it feeds to the real `ssh`; it does not
call `tunnel_args`. Two things therefore guard the shipped invocation, and
neither alone is sufficient: unit tests in `crates/ctxmux-remote/src/lib.rs`
assert the builder's exact output, and the script asserts a real client accepts
that shape. A change that edited the builder and both assertions together could
still ship a malformed invocation. Closing that seam mechanically — deriving the
script's argument list from the builder rather than restating it — is the
improvement this record does not yet claim.

Carrying that same vertical over a real SSH boundary is qualified. The lane is
written to fail rather than skip when no boundary exists, and it has now run
against one: a Linux owner host reached over the caller's own SSH configuration,
with the daemon placed as a binary and no repository or toolchain on it. All five
partition fixtures pass there — owner Stop settlement across tunnel loss, refusal
of a replaced daemon incarnation, truncation after outage eviction, cursor replay,
and bidirectional version skew against two genuinely distinct builds.

The skew fixture deserves a note, because it is the one that cannot be faked: it
requires a second build that really speaks the older generation, on both ends. A
forged identity or a hand-written error would prove nothing, so the fixture drives
a separately compiled older client against the current owner daemon, and the
current client against a separately compiled older owner daemon, and requires each
to reject the other.

The probeable endpoint contract is published as the client-side
`REMOTE_ENDPOINT_CONTRACT_VERSION` constant on both public surfaces, proven
across languages by `packages/sdk/test/remote-endpoint.test.ts` under
`--stage capability`. That test also asserts the owner-host daemon advertises no
key describing the caller's network position.

What remains unproven is the boundary's repeatability, not its behavior. The
partition lane needs an owner host, a second older build on both ends, and a
memory-only current daemon, so it runs on demand rather than in PR CI. A run
therefore qualifies the build it ran against; it does not stand in for a later
one. Treat a green partition stage as evidence with a date, and re-run it when
the wire, the endpoint, or the daemon's identity handling changes.

The TypeScript SDK reaches an owner-host Runtime through the same forwarded
socket with no SDK change, because the socket path is the entire addressing
model — proven by the checked-in fixture named above rather than derived from the
design alone.

Relay deployment, account or environment federation, hosted control planes,
remote scheduling, orchestration, Provider sessions, remote binary provisioning,
and derivation metadata remain out of scope.

## Repository evidence

- `crates/ctxmux-remote/src/lib.rs` — the endpoint, its supervision, and its
  validation contract.
- `crates/ctxmux-daemon/src/bin/fake-ssh.rs` — the test-only forwarder that lets
  the supervision contract be proven without an SSH boundary.
- `crates/ctxmux-daemon/tests/remote_owner_host_endpoint.rs` — behavior against a
  real daemon through the stand-in forwarder; required PR evidence.
- `crates/ctxmux-daemon/tests/remote_real_openssh.rs` — the same vertical over the
  real system client. Separate because required evidence must contain no ignored
  test, and this lane needs an SSH boundary a PR runner does not have.
- `scripts/check-remote-runtime.sh` — the staged qualification entrypoint.
