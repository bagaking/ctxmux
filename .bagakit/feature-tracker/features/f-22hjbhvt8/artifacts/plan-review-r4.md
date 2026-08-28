# Remote Runtime plan review — revision 4

- Status: approved by the Owner in the supervised session that opened this
  revision.
- Supersedes: revision 3 (`plan-review-r3.md`), which remains historical truth.
- Scope of change: T-002's capability contract only. No delivered evidence is
  revisited and no architecture decision is reopened.

## Why this revision exists

Revision 2 wrote T-002 as "the owner-host daemon advertises one exact `remote.*`
capability key." Implementing it exposed that the sentence asks a daemon to
declare something it cannot know.

Every capability in the current record is a fact about the daemon publishing it:
`native.start`, `tmux.import`, `services.persistent_state`. A daemon reached
through a forwarded socket sees an ordinary local Unix socket connection. It has
no way to distinguish that from a client on the same machine, so it has no basis
for advertising a remote key. The side that forwards is the *local* side.

This is the same error as `RunBackend::Remote`, which revision 2 already rejected:
a value describing the caller's network position does not belong to the party that
cannot observe it. Rejecting it there and accepting it here would be incoherent.

## Decision — the remote endpoint contract is a client-side constant

The daemon capability record is unchanged. No `remote.*` key is added to it, and
no negotiation frame is introduced.

Instead the public Rust and TypeScript surfaces export the endpoint contract
version as a first-class constant, exported the same way the existing capability
constants are. A consumer then probes two separate and honestly-owned facts:

| Question | Answered by |
| --- | --- |
| Can this client library establish an owner-host endpoint, and against which contract version? | the exported endpoint contract constant, a local fact about the linked library |
| What can the owner-host daemon actually do once reached? | the existing `native.*`, `tmux.*`, and `services.*` record, byte-exact and fail-closed before dispatch |

Both are probeable, which is what the consumer asked for: they must not have to
guess. Neither party declares a fact it cannot observe.

The consumer's fail-closed posture is preserved exactly. Their adapter already
compares advertised capability versions for exact equality and refuses to
dispatch on mismatch; reaching a daemon through a tunnel changes nothing about
that path, because the tunnel produces an ordinary socket path and the ordinary
client speaks the unchanged protocol over it.

### What the constant does and does not promise

It names the endpoint contract this library implements: the argument shape, the
readiness rule, the reachability semantics, and the teardown guarantees recorded
in decision 018. It does not promise that a host is reachable, that credentials
work, that a remote daemon is listening, or that any Run is controllable. Those
remain runtime failures with typed errors, not capability questions.

## Consequence for the acceptance criteria

T-002's first and second bullets are rewritten to name the client-side constant
and the two-fact probe. Its remaining bullets — public surface parity, the
"endpoint contract and nothing more" boundary, and the presence/absence and
skew tests — stand unchanged in intent.

T-002 had been started and was released without a gate result, so no executed
evidence is disturbed. It is superseded by `T-006` rather than edited, because a
started task's semantics should not shift underneath its own history.
