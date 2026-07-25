# ctxmux

> Terminals are views. Runs are durable.

**ctxmux is a Run multiplexer for local processes.** It is the runtime layer
between a long-lived local process and the clients that start, observe, detach
from, and reattach to it.

tmux multiplexes terminals. ctxmux keeps Runs alive across client lifetimes.

## Why ctxmux

A shell, coding agent, dev server, test, benchmark, or script should not become
owned by the editor window that happened to launch it. The durable object today
is the Run: its process, PTY I/O, identity, retained output, and lifecycle.
Terminals, editors, CLIs, and automations are replaceable clients.

The current runtime provides one embeddable local boundary that can:

- start and stop local Runs;
- attach, detach, observe, and reconnect;
- stream input, output, status, and lifecycle events;
- fork declared portable inputs with explicit parentage and Level A fidelity;
- expose the same stable boundary to CLIs, editors, and other clients;
- explicitly bind host-local shell and Codex Integrations without changing the
  daemon's Run model;
- resume a declared Codex session through an explicit Level B fork while a
  Level A-only Integration fails before creating a child;
- optionally recover committed historical Run metadata and replay after daemon
  restart without adopting a stale PID;
- discover and observe one existing tmux-owned pane as a read-only,
  raw-since-import, memory-only Run through public Control Mode.

Workspace snapshots, artifact ownership, live PTY restart handoff, writable
tmux control, and a general Backend framework remain target capabilities, not
current guarantees.

Coding agents are the flagship Integration because they make context fidelity
especially valuable. They are not a special primitive in the runtime.

## What ctxmux is not

ctxmux is not an Agent Harness. It does not break down goals, schedule agent
teams, evaluate answers, select winners, or define Crucible and MapReduce
policy. Those are client behaviors that ctxmux should make easy to build.

ctxmux is also not a tmux clone. Its adapter observes a selected existing pane
through the tmux executable and public Control Mode without copying tmux's
private wire protocol or taking ownership of the pane.

## Project shape

The intended long-term boundary is a Rust daemon with a versioned local
protocol, a real CLI client, and a TypeScript SDK with explicitly registered
Integrations. The exact implementation will grow through working vertical
slices rather than speculative framework design.

Project decisions live in:

- [Vision](docs/vision.md)
- [Architecture](docs/architecture.md)
- [Architecture wrong-case casebook](docs/architecture/casebook.md)
- [Local protocol](docs/protocol.md)
- [Test evidence strategy](docs/testing-strategy.md)
- [Roadmap](docs/roadmap.md)

## Native Run quick start

The first native slice currently targets Unix systems and uses an explicit
socket path:

```bash
cargo build --workspace
target/debug/ctxmuxd --socket target/ctxmux.sock
```

Add `--state-dir target/ctxmux-state` to recover historical Run metadata,
lineage, terminal state, and committed replay after daemon restart. The state
directory is dedicated, owner-only local storage. Live PTY control is not
adopted after restart; a previously running Run is reported as interrupted.

In another terminal:

```bash
export CTXMUX_SOCKET=target/ctxmux.sock
run_id=$(target/debug/ctxmux start -- /bin/sh)
printf 'echo hello\n' | target/debug/ctxmux input "$run_id" --stdin
target/debug/ctxmux attach "$run_id"
```

When standard input is a terminal, `attach` enters raw mode, forwards terminal
input, tracks `SIGWINCH` resizes, and detaches with `Ctrl-b d`. When input is not
a terminal, it remains an output follower. Detaching or closing the client does
not stop the Run.

## Read-only tmux pane import

Select one explicit tmux server socket, list its live panes, and import a pane:

```bash
target/debug/ctxmux --socket target/ctxmux.sock tmux-list /path/to/tmux.sock
target/debug/ctxmux --socket target/ctxmux.sock tmux-import /path/to/tmux.sock %0
target/debug/ctxmux --socket target/ctxmux.sock attach <imported-run-id>
```

One imported Run is bound to the pane's complete identity at import: server
epoch, session, window, pane, and pane PID. Target relocation, respawn, death,
or server replacement interrupts the Run instead of silently following a new
target. Public `tmux_version` is the selected server version; client and server
compatibility are checked separately.

If tmux links expose the same pane ID through multiple session/window
associations, discovery reports those rows but import rejects the ambiguous
target. The reported pane PID is import-time identity evidence, not ctxmux
authority to signal the tmux-owned process.

The adapter is intentionally read-only and memory-only. It supports
list/status/attach, rejects input/resize/stop/fork, retains raw bytes only from
the import boundary, and marks unavailable history or a source pause as
truncated. It never uses `capture-pane` as fabricated raw history. Closing a
ctxmux attachment or daemon closes only ctxmux's Control client; tmux keeps the
pane, session, and server alive. Interactive read-only attach still detaches
locally with `Ctrl-b d`.

## TypeScript SDK

The TypeScript SDK speaks the same generated protocol as the Rust CLI:

```ts
import { CtxmuxClient, defineRun } from "@ctxmux/sdk";

const client = new CtxmuxClient({ socketPath: "target/ctxmux.sock" });
const run = await client.start(defineRun("/bin/sh"));
const attachment = await client.attach(run.id);

await attachment.input("echo hello\n");
for await (const event of attachment.events()) {
  if (event.type === "output") {
    process.stdout.write(Uint8Array.from(event.chunk.data));
  }
}
```

See [`packages/sdk/README.md`](packages/sdk/README.md) for the API boundary and
attachment lifecycle.

## Status

The Rust daemon now owns one real native PTY Run lifecycle through the versioned
local protocol. Tests prove that the same child PID survives attachment
disconnect, accepts input and resize after reconnect, preserves ordered final
output, and rejects invalid lifecycle operations explicitly. The Rust CLI and
TypeScript SDK both manage the same daemon-owned Runs through that boundary.

The tmux pane adapter decision is accepted and implemented. Its complete import
identity, server-version semantics, transcript corruption, pause/replay
honesty, first-party CLI and SDK behavior, and tmux ownership pass locally
through deterministic fixtures plus real tmux tests. Required Ubuntu minimum
and macOS current server-version lanes remain before Feature `f-224czneed` can
archive.

Current limits are deliberate and visible: persistent mode recovers committed
historical state but not live PTY control; memory-only tmux imports do not
survive daemon restart; output replay is a bounded 4 MiB raw byte log rather
than a reconstructed terminal screen; and the transport is Unix-only. Those
boundaries are explicit capabilities, not implicit guarantees.
