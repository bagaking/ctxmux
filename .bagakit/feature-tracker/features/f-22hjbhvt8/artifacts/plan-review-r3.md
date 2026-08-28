# Remote Runtime plan review — revision 3

- Status: approved by the Owner in the supervised session that opened this
  revision.
- Supersedes: revision 2 (`plan-review-r2.md`), which remains historical truth.
- Scope of change: task ownership only. No architecture decision is revisited.

## Why this revision exists

Revision 2 put the real-OpenSSH evidence inside T-001. That was stricter than the
reviewed intent: the Owner had already accepted that this machine has no SSH
boundary and approved delivering the vertical with the real lane written and
failing loudly until a boundary exists. Making that accepted gap a condition of
T-001 turned an acknowledged limitation into a task that could not close, so
T-001 finished `blocked` while every piece of evidence it actually owns passed.

The fix is a boundary correction, not a relaxation. The real-OpenSSH lane keeps
its exact acceptance wording and its fail-loud behavior; it moves to the task
whose job is already qualification against a real environment.

## Decision 1 — T-001 owns the vertical and its supervision contract

T-001 closes when the endpoint works end to end and its supervision contract is
proven: forwarding, exact-identity selection and its fail-closed rejection,
non-terminal transport loss, cursor replay of output written while disconnected,
explicit failure with no owner-host listener, and teardown of the socket,
directory, and process.

Two lanes prove this without an SSH boundary:

1. the supervision lane, which runs a real `ctxmuxd` and a real forwarding child
   process;
2. the argument-shape lane, which proves the real system `ssh` accepts the exact
   argument list the production builder emits, so the stand-in cannot hide a
   malformed invocation.

## Decision 2 — T-003 owns the real-OpenSSH evidence

The real-OpenSSH lane moves to T-003, which already owns qualification against a
real environment: partition recovery, remote Stop receipts, and bidirectional
mixed-capability skew. Those all need a genuine remote owner host, so the SSH
boundary is a T-003 prerequisite rather than a second requirement bolted onto the
vertical.

This is a change of owner, not of standard. The lane still fails rather than
skips when its boundary is absent, and remote still cannot be reported as
qualified by a run that never spoke SSH.

## Decision 3 — the qualification boundary is a remote Linux owner host

The Owner named `sg2` as the available boundary. It is reachable with the
caller's existing credentials, runs Linux on `x86_64`, and its `sshd` reports
`allowstreamlocalforwarding yes`.

A remote Linux host is deliberately preferred over a same-machine loopback: it
exercises the platform asymmetry a loopback hides. The local build is macOS on
`aarch64`, so a Linux owner host proves that endpoint selection and identity
comparison do not quietly assume a matching platform — the exact assumption a
downstream consumer currently hard-pins.

`ctxmuxd` is not installed on that host. Getting a compatible daemon there is
T-003's setup problem, and this Feature still uploads, installs, and spawns
nothing on its own: the operator provisions the owner host, and a missing
listener remains an explicit error.

The Owner set one constraint on that provisioning: place only a compiled
`ctxmuxd` binary on the owner host — do not clone the repository or build there.
A cross-compiled Linux `x86_64` binary plus a socket path is the entire remote
footprint. This keeps the owner host free of a toolchain and source tree, and it
matches the decision boundary: ctxmux never places or starts that binary itself,
so the operator's copy is the only provisioning step.

## What does not change

The architecture decision in revision 2 stands unchanged: `RunBackend` remains
`native | tmux`, remote is an endpoint above the wire, transport loss is
reachability and never lifecycle, and nothing is provisioned silently. T-002 and
T-004 keep their existing scope.
