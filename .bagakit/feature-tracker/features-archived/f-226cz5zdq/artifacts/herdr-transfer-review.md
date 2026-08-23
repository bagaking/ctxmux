# Herdr transfer review

## Review boundary

- Herdr revision: `bbd7c2094a44fcbcc4a3a3aedef236c4d697d793`
- ctxmux comparison revision: `c03954e12427d0604cf7398bc0afaef96528f474`
- decision target: current `f-226cz5zdq` qualification work plus genuinely
  distinct future Feature families

Herdr is primary evidence for its own implementation and documented behavior.
It is not independent proof that the same mechanism is faster, more reliable,
or correctly scoped for ctxmux.

## Decision

### Integrate into existing tasks

1. `T-013` should protect a single current-incarnation PTY control owner,
   bounded input backpressure, non-starvable resize/stop control, actual
   PTY-write acknowledgement, applied-size readback, and opaque preservation
   of fragmented terminal input such as SGR mouse and bracketed-paste bytes.
2. `T-014` should make connect-or-activate a visible probe, compatibility,
   launch, readiness, and publish transaction. It must fail closed before
   mutating an incompatible or unrelated socket/daemon target.
3. `T-007` should qualify terminal-interaction transparency through a real
   controlling PTY, including mouse-protocol and bracketed-paste byte
   forwarding plus restoration of the host terminal after every supported
   detach/failure path.

These are behavior contracts. They do not require copying Herdr's module graph
or adding a public actor abstraction.

### Create one separate proposal Feature

Controlled live PTY handoff has intrinsic ctxmux value and a materially
different acceptance boundary from reliability qualification. Preserve it as
a proposal-only Feature for post-release architecture work. Do not turn it
into executable Tasks until the design can prove exactly-one-owner transfer,
rollback, output ordering, child PID and I/O survival, and explicit unsupported
platform behavior.

### Do not add current Tasks

- daemon terminal-screen projection;
- writable-controller leases;
- remote ctxmuxd deployment and SSH forwarding;
- a language-neutral JSON Schema artifact;
- clipboard-image materialization or a general artifact upload store;
- Agent lifecycle, Agent-to-Agent turns, scheduling, or orchestration.

Each remains deferred or belongs to a client/Integration until a concrete
consumer and owner-boundary fixture justify its permanent complexity.

## Additional advantage: mouse interaction and image paste

Herdr combines three different capabilities that should not be collapsed into
one ctxmux requirement.

### Product mouse UI

Herdr is a mouse-first terminal UI: it owns panes, focus, selection, menus,
scrolling, copy mode, and layout. That is useful in Herdr but is presentation
and workspace policy. Ctxmux deliberately makes terminals views and gives
editing UI and multi-Run composition to clients. No core task should reproduce
this TUI.

### Terminal mouse protocol transparency

An application such as Claude Code or Codex can request terminal mouse modes
and receive SGR or related input sequences. Ctxmux input is already defined as
opaque bytes, so the correct kernel-level value is lossless transport and
restoration, not interpreting mouse gestures. The low-entropy ratchet is a real
PTY regression containing fragmented mouse and bracketed-paste sequences.

The limit remains explicit: after raw replay truncation, ctxmux cannot recreate
all terminal modes or a current full-screen grid. That larger screen-projection
capability still requires a concrete late-attach fixture before adoption.

### Clipboard image bridge

Herdr's remote client reads a local clipboard image, sends the bytes to the
remote server, writes a private temporary file, and pastes the resulting path.
The implementation uses a private directory, owner-only files, bounded image
input, per-client cleanup, and stale-file cleanup. This is an artifact bridge,
not terminal byte transport.

Adding it to ctxmux would make the daemon own file materialization, secret
retention, quotas, content validation, cleanup, path visibility, persistence,
and eventually remote placement. Ctxmux currently records opaque artifact
references without owning an artifact store. AgentMux already owns workspace
and editor files and can implement clipboard-image materialization with the
right user context. The current benefit therefore does not exceed the core
complexity.

Transfer trigger: reconsider only when a real non-AgentMux consumer needs a
generic Run artifact-ingress contract and the design has explicit ownership,
permissions, quotas, cleanup, reconnect, persistence, and remote-location
semantics. Do not introduce an `image`-specific Run primitive.

## Additional advantage: any Agent can prompt any Agent

Herdr exposes `agent prompt`, `agent wait`, pane input, pane reads, and an API
socket inherited by managed pane processes. Consequently, one Agent process
can invoke the Herdr CLI/API and target another recognized Agent.

This is a useful Agent-host feature, but it is not a reliable Agent-to-Agent
conversation protocol:

- `agent prompt` writes text plus Enter into the target terminal;
- waits observe Agent lifecycle state rather than a correlated turn result;
- Herdr explicitly does not track individual turns;
- an Agent that was already working may finish an older turn and satisfy the
  wait;
- unsupported Agents fall back to raw pane primitives;
- cross-Agent authorization, trust, prompt provenance, loop prevention, and
  delivery semantics are product policy.

Ctxmux already supplies the neutral substrate: a host can attach to any Run,
write raw bytes, observe ordered output, and use stable Run identity. `T-013`
will make each control operation deterministic. AgentMux should layer Agent
identity, structured turns, lifecycle authority, permissions, routing, and
conversation correlation above that substrate. Adding `Agent`, `message`, or
`conversation` to the daemon would violate the Run-kernel boundary.

## Principle layer

### What

Transfer the smallest generic mechanism that makes Run ownership or terminal
transport more correct. Keep presentation, artifact policy, and Agent semantics
above the protocol.

### Why

Herdr's apparent product advantages are composites. Copying the visible
feature would also copy hidden owners and failure states. Separating the layers
preserves ctxmux's generality and keeps permanent protocol entropy proportional
to intrinsic Run value.

### Intended generalization

Apply the same split to future terminal UI, clipboard, remote deployment,
inter-Run communication, and Agent-host proposals: identify the raw Run
primitive, then leave semantic policy with the first client that needs it.

### Failure boundary

This does not prohibit later screen projection, artifact ingress, or a control
lease. It requires a real consumer, explicit ownership, and public-boundary
acceptance evidence before promotion.

### Transfer checks

- SGR mouse and bracketed-paste bytes reach a generic shell Run unchanged;
  the daemon never needs to know they are mouse or paste events.
- A clipboard image is not retained by ctxmux merely because a client can send
  bytes; artifact ownership must be separately declared.
- One Agent can control another through AgentMux without any Agent field in
  `ctxmux-protocol`.
- Saturated input cannot starve resize or stop, and an accepted input result
  means the owned PTY write boundary succeeded.
- Failed activation never deletes or replaces an unrelated daemon socket.

## Herdr evidence

- Product scope and mouse-first claim:
  `https://github.com/herdrdev/herdr/blob/bbd7c2094a44fcbcc4a3a3aedef236c4d697d793/README.md`
- PTY actor ownership and backpressure:
  `https://github.com/herdrdev/herdr/blob/bbd7c2094a44fcbcc4a3a3aedef236c4d697d793/src/pty/actor/unix.rs`
- Remote activation:
  `https://github.com/herdrdev/herdr/blob/bbd7c2094a44fcbcc4a3a3aedef236c4d697d793/src/remote/attach.rs`
- Clipboard-image staging:
  `https://github.com/herdrdev/herdr/blob/bbd7c2094a44fcbcc4a3a3aedef236c4d697d793/src/server/clipboard_image.rs`
- Agent automation and its turn-tracking limit:
  `https://github.com/herdrdev/herdr/blob/bbd7c2094a44fcbcc4a3a3aedef236c4d697d793/docs/next/website/src/content/docs/agent-automation.mdx`
- Transactional live handoff:
  `https://github.com/herdrdev/herdr/blob/bbd7c2094a44fcbcc4a3a3aedef236c4d697d793/tests/live_handoff.rs`

## Ctxmux evidence

- `AGENTS.md`
- `docs/vision.md`
- `docs/architecture.md`
- `docs/protocol.md`
- `docs/roadmap.md`
- `docs/architecture/choices/011-context-artifact-lineage-fork.md`
- `packages/sdk/README.md`
