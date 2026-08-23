# Remote Runtime transport plan review

- Status: approved by the Owner through the supervised Feature-pool cleanup.
- Replacement: this Remote-only Feature replaces the mixed Remote plus
  derivation proposal `f-22fczqx4j`.
- Execution posture: proposal-only; do not start until `f-22ecztapc` closes its
  local Runtime contract.

## Decision

Use the smallest proven transport shape first: system OpenSSH StreamLocal
forwarding maps the owner-host ctxmuxd Unix socket to a bounded local Unix
socket, and existing public clients continue to speak the ctxmux protocol.

The first task proves client or tunnel loss, exact identity reconnect, Run and
PID continuity, and replay or explicit truncation end to end. The second task
adds wrong-identity, remote Stop-receipt, capability asymmetry, security,
resource, and supported-platform qualification without changing the owner
model.

Provider-neutral derivation metadata has no demonstrated consumer requirement
and is not part of this Feature. It remains deferred until a real consumer can
justify a separately reviewed contract.
