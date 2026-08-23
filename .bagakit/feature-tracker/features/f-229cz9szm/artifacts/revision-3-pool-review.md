# Revision 3 review: generic authorized artifact consumer

- Status: approved by the Owner through the supervised Feature-pool cleanup.
- Lifecycle: remain `proposal` and `proposal_only`; no task is started.
- Historical artifacts: earlier task-plan files remain unchanged as planning
  history.

## Decision

The current proposal and task plan use an authorized external client or
embedding host as the consumer. ctxmux owns only bounded opaque-byte ingress on
the target Run host: capability, exact Run identity, integrity, atomic
publication, retry disposition, quotas, lifetime, and cleanup.

No named product repository, Agent identity, clipboard implementation,
permission UI, prompt construction, SSH deployment, or conversation behavior
is required for acceptance. Those policies remain with whichever external host
chooses to consume the generic capability.

## Preserved boundaries

- Stage, Run input, and release remain separately correlated operations.
- The receiving ctxmuxd chooses the destination on its own host.
- Native Run is the first capability; unsupported Backends fail closed.
- This rewording does not authorize implementation or broaden the proposal.
