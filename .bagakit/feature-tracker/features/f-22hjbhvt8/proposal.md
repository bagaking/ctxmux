# Feature Proposal: f-22hjbhvt8

## Why

- A Run owned by a remote ctxmuxd should remain durable when the local client
  or transport disappears; transport reachability must not impersonate process
  lifecycle truth.
- System OpenSSH can map a remote Unix socket to a bounded local Unix socket
  through StreamLocal forwarding, allowing ctxmux to reuse its existing public
  protocol and SDK boundary before inventing a new transport stack.
- The remote daemon already owns the facts that matter: PTY, child, Run
  lifecycle, ordered output, replay, and Stop settlement.

## Goal

Carry the existing public ctxmux protocol through a caller-authorized system
OpenSSH StreamLocal tunnel to an owner-host ctxmuxd. Preserve exact Runtime and
Run identity, reconnect, replay or explicit truncation, remote owner receipts,
and capability failure without adding Relay, accounts, a control plane,
orchestration, or derivation metadata.

## Ownership

- The remote ctxmuxd owns the remote PTY, child, Run state, output, replay, and
  Stop receipt.
- OpenSSH owns host authentication, credentials, host-key policy, connection
  setup, and StreamLocal transport mechanics.
- The ctxmux client owns expected Runtime identity, local tunnel lifetime, and
  reconnect policy. Tunnel loss changes reachability to `unverifiable`; it does
  not publish `exited` or `interrupted`.

## Smallest vertical

1. Start or select an owner-host ctxmuxd and map its Unix socket to one bounded
   local Unix socket with system OpenSSH StreamLocal forwarding.
2. Use the unchanged public client boundary to start a Run, detach, lose the
   tunnel, recreate the tunnel, and attach to the exact `runtimeId + runId`.
3. Prove the remote PID is unchanged and recover ordered output through replay,
   or return explicit truncation when retained output is unavailable.
4. Add fail-closed expected-identity, remote Stop-receipt, and mixed-capability
   qualification only after that end-to-end path works.

## Acceptance boundary

- The local tunnel or client may exit while the remote child keeps running.
- Reconnect binds the exact Runtime and Run; another Runtime, SSH host trust
  failure, or mismatched expected identity is rejected before attach or resume.
- Transport loss alone is `unverifiable`, never terminal Run evidence.
- Stop succeeds only with a receipt from the remote daemon owner.
- Capability asymmetry is tested in both directions; an absent or older
  capability fails explicitly without local execution fallback.
- Relay deployment, account or environment federation, remote scheduling,
  orchestration, provider sessions, transcript policy, and generic derivation
  metadata are out of scope.

## Dependencies

- `f-22ecztapc` owns the complete local Runtime identity, observation, wait,
  activation, and standalone evidence required before Remote transport begins.
