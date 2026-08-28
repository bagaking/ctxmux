# Remote Runtime plan review — revision 2

- Status: approved by the Owner in the supervised session that opened this
  revision.
- Supersedes: revision 1 (`plan-review.md`), which remains historical truth.
- Execution posture: `worktree`; this revision authorizes implementation.

## Why this revision exists

A downstream consumer (AgentMux) asked ctxmux to expose `remote` as a fourth
`RunBackend` variant beside `native` and `tmux`, carrying remote host identity,
the remote `ctxmuxd` build/protocol version, and a runtime identity that can be
re-verified across reconnect. Its stated position is that connection and
authentication must use the caller's existing system SSH, that the consumer must
hold no credentials, and that partition recovery must reuse the same
ordered-byte/replay/gap semantics as local rather than adding a second recovery
state machine.

Revision 1 already chose the transport shape those requirements need. This
revision records the reviewed decision to **deliver the requirement while
rejecting the requested wire shape**, and to unblock execution.

## Decision 1 — Remote is an endpoint, not a fourth Backend

`RunBackend` stays `native | tmux`. Remote becomes an explicit endpoint concern
owned by the client, above the wire.

Reasons, in the order that decided it:

1. A remote `ctxmuxd` owns a real native PTY and child. From its own vantage the
   Run *is* `Native`; that is the honest answer. Publishing
   `backend: "remote"` would require either the daemon to misreport what it owns,
   or the local client to rewrite wire truth it cannot attest. Both violate the
   rule that a Backend names the actual runtime owner.
2. Host identity is deliberately outside `RuntimeIdentity`
   (`docs/architecture.md`, runtime-identity row). A daemon cannot attest its own
   host, and OpenSSH has already proven host trust before the first frame. Host
   identity therefore belongs to caller-side endpoint configuration, with
   `runtimeId` as the value re-verified across reconnect.
3. A third Backend variant forces a third `RunCapabilities` profile. The
   TypeScript validator currently encodes a binary native-versus-tmux capability
   assertion, so a third profile would fork the capability and replay contract —
   which is precisely how the consumer would end up with the two recovery state
   machines it asked us to avoid.

What the consumer actually needs is satisfied without a new wire field:

| Requirement | Owner in this design |
| --- | --- |
| probeable capability constants | new `remote.*` keys in the existing flat numeric capability record; the existing client-local, fail-closed, pre-dispatch requirement check is reused unchanged |
| runtime identity re-verifiable across reconnect | existing `runtimeId` (stable per state-directory lineage) plus `daemonInstanceId` (incarnation fence), compared through the existing exact-identity expectation |
| connection and authentication | system OpenSSH and the caller's existing credentials; ctxmux passes user arguments through and never reads, copies, prompts for, or stores a credential |
| ordered-byte, replay, gap, lease semantics | reused verbatim, because remote is the same protocol over the same socket contract |

## Decision 2 — Transport loss is reachability, never lifecycle

Losing the tunnel means the local client cannot currently observe the owner. It
does not mean the Run exited or was interrupted. Reachability is a client-local
fact and must not be published as a `RunState` or an `InterruptionReason`; only
the remote daemon owner may publish lifecycle truth or a Stop receipt.

## Decision 3 — Select an already-running remote daemon

In scope: map an owner-host `ctxmuxd` Unix socket to one bounded local Unix
socket with system OpenSSH StreamLocal forwarding, then verify identity fail
closed before any business frame.

Explicitly out of scope for this Feature: uploading, installing, or
version-matching a downloaded remote binary, and spawning a remote daemon.
Nothing is provisioned silently. If nothing is listening, or identity does not
match, the operation fails explicitly.

## Decision 4 — Narrow the dependency to the delivered identity contract

Revision 1 blocked this Feature on all of `f-22ecztapc`. Its real prerequisite
is the Runtime identity and capability contract, delivered as that Feature's
`T-002`. The remaining tasks there — revisioned observation metadata, wait
helpers, TypeScript activation, and standalone qualification — are not
load-bearing for a tunnel that reuses the existing client boundary.

The blocking edge is therefore removed and the actual prerequisite is recorded
here as evidence rather than as a whole-Feature gate. This is a narrowing of a
reviewed dependency, not a claim that `f-22ecztapc` is complete; that Feature
keeps its own open tasks and its own closure.

## Task topology

`T-001` closes the smallest end-to-end path: forward, verify, start, detach,
lose the tunnel, reattach by exact identity, and replay or report truncation.
`T-002` and `T-003` are independently verifiable hardening tasks that both
depend on `T-001` and can run concurrently. `T-004` is the integration task that
publishes the public surface and its documentation once the hardening tasks
close.

## Non-goals restated

Relay deployment, account or environment federation, hosted control planes,
remote scheduling, orchestration, Provider sessions, and derivation metadata
remain out of scope. No fallback to local execution is ever attempted.
