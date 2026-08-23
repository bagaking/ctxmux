# ctxmux

> Terminals are views. Runs are durable.

tmux multiplexes terminals. ctxmux multiplexes **Runs**—local processes that
keep running after you close the client.

You can use it by itself. The daemon and CLI are enough. No Node, no SDK, and
no editor. Unix only. `ctxmux` starts `ctxmuxd` when nothing is listening.

## Standalone quick start

```bash
cargo build --workspace
run_id=$(target/debug/ctxmux start -- sh)
target/debug/ctxmux attach "$run_id"
```

That attach is a real terminal session. Type as usual. `Ctrl-b d` detaches.
The shell keeps running. `ctxmux attach "$run_id"` comes back to the current
screen; it does not replay the CSI history of every redraw.

The CLI uses `$XDG_RUNTIME_DIR/ctxmux/ctxmux.sock` when that directory is set,
otherwise a process-temp path. `--socket` or `CTXMUX_SOCKET` selects a
different daemon. Closing the CLI never stops the Run; only `stop` (or daemon
exit, in memory-only mode) does.

Other commands:

```bash
target/debug/ctxmux list
target/debug/ctxmux status "$run_id"
target/debug/ctxmux stop "$run_id"
```

The short form generates a fresh one-shot Stop identity. When a script must
recover after losing the CLI response, retain the daemon instance from
`ctxmux runtime` and supply one operation key; retry the exact same command:

```bash
target/debug/ctxmux stop \
  --daemon-instance <daemon-instance-uuid> \
  --operation-key deployment-stop-42 \
  "$run_id"
```

Both recovery flags are required together. A successful invocation prints
`stop=graceful` or `stop=forced` from the daemon-owned complete-session receipt.

To keep historical Run metadata across daemon restart, start the daemon
yourself with `--state-dir`:

```bash
target/debug/ctxmuxd --socket target/ctxmux.sock --state-dir target/ctxmux-state
export CTXMUX_SOCKET=target/ctxmux.sock
```

A cold restart or crash does not adopt live PTY control; a previously running
Run is reported as interrupted. In persistent mode, an intentional `SIGHUP`
instead performs an exec-in-place upgrade: the daemon PID, listener, child PID,
PTY control, ordered output cursor, and recoverable-Input retry truth continue
in the incoming image. Existing attachment connections close and reconnect
from their last observed output cursor.

## Why this exists

A shell, coding agent, or dev server should not die with the window that
launched it. The durable object is the Run: process, PTY, identity, retained
output, and lifecycle. Terminals, editors, CLIs, and automations are
replaceable views.

ctxmux is not an Agent Harness. It does not plan work, schedule agents, or
judge results. It is also not a tmux clone: the tmux adapter observes one
existing pane through public Control Mode and does not take ownership of it.

## Product boundary

ctxmux is a complete Runtime product, not an internal service that requires an
editor or Agent framework. Its daemon, CLI, protocol, and SDK form one
standalone surface for arbitrary commands. Embedding products may add Provider
catalogs, Agent sessions, permissions, semantic events, orchestration, or UI,
but those layers remain clients of ctxmux and are never prerequisites for the
daemon or CLI.

The ownership test is straightforward:

- a fact belongs to ctxmux when it is valid for shells, servers, tests, scripts,
  and Agents, and only the PTY, process, Backend, or daemon owner can prove it;
- a claim belongs to the embedding product when it requires understanding a
  Provider, Agent session, permission, message, task, workspace policy, or UI.

ctxmux can prove that bytes crossed the PTY write boundary and that an owned
process scope exited. It cannot prove that an Agent accepted a prompt, finished
a task, or preserved a provider conversation. See the normative split in
[Architecture](docs/architecture.md#standalone-runtime-boundary).

## Embedding

The TypeScript SDK talks to the same daemon over the same protocol as the CLI.
Use it when a host process should drive Runs; use the CLI when you want to
operate them yourself.

```ts
import { CtxmuxClient, createOperationKey, defineRun } from "@ctxmux/sdk";

const client = new CtxmuxClient({ socketPath: "target/ctxmux.sock" });
const run = await client.start(defineRun("sh"), createOperationKey());
const attachment = await client.attach(run.id);
```

See [`packages/sdk/README.md`](packages/sdk/README.md).

## Read-only tmux import

```bash
target/debug/ctxmux tmux-list <tmux-socket>
target/debug/ctxmux tmux-import <tmux-socket> %0
target/debug/ctxmux attach <imported-run-id>
```

Import binds one pane identity at that moment. Input, resize, stop, and fork
are rejected. Closing the ctxmux client leaves the tmux pane running.
`Ctrl-b d` still detaches the local view. Details live in
[Architecture](docs/architecture.md) and [Protocol](docs/protocol.md).

## Docs

- [Vision](docs/vision.md)
- [Architecture](docs/architecture.md)
- [Local protocol](docs/protocol.md)
- [Roadmap](docs/roadmap.md)
- [Contributing](CONTRIBUTING.md)

## Status

Pre-alpha. The daemon owns a real native PTY Run. The CLI and SDK are both
clients of that Run. Interactive attach reconstructs the current screen;
the protocol still carries raw bytes. Transport is Unix sockets. Persistent
mode recovers committed history after cold restart and preserves live PTY
control across a planned exec-in-place `SIGHUP` upgrade. Crash-time adoption and
host-reboot process continuity remain unsupported.
