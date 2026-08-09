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

The current native slice provides one embeddable local runtime that can:

- start and stop local Runs;
- attach, detach, observe, and reconnect;
- stream input, output, status, and lifecycle events;
- expose the same stable boundary to CLIs, editors, and other clients.

The roadmap adds explicit Integrations, portable and high-fidelity fork,
artifacts, lineage, persistence, and a tmux Backend. Those are target
capabilities, not generation-1 protocol guarantees.

Coding agents are the flagship Integration because they make context fidelity
especially valuable. They are not a special primitive in the runtime.

## What ctxmux is not

ctxmux is not an Agent Harness. It does not break down goals, schedule agent
teams, evaluate answers, select winners, or define Crucible and MapReduce
policy. Those are client behaviors that ctxmux should make easy to build.

ctxmux is also not a tmux clone. It keeps the proven attach/detach mental model
and will integrate existing tmux sessions through public tmux surfaces without
copying tmux's private wire protocol.

## Project shape

The intended long-term boundary is a Rust daemon with a versioned local
protocol, a real CLI client, and a TypeScript SDK with explicitly registered
Integrations. The exact implementation will grow through working vertical
slices rather than speculative framework design.

Project decisions live in:

- [Vision](docs/vision.md)
- [Architecture](docs/architecture.md)
- [Architecture wrong-case casebook](docs/architecture/casebook.md)
- [Native protocol](docs/protocol.md)
- [Roadmap](docs/roadmap.md)

## Native Run quick start

The first native slice currently targets Unix systems and uses an explicit
socket path:

```bash
cargo build --workspace
target/debug/ctxmuxd --socket target/ctxmux.sock
```

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

Current limits are deliberate and visible: Run state does not survive daemon
restart; output replay is a bounded 4 MiB raw byte log rather than a reconstructed
terminal screen; and the transport is Unix-only. Those are later tracked
milestones, not implicit guarantees.
