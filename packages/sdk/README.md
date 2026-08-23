# `@ctxmux/sdk`

TypeScript client for one local ctxmux daemon. It has no runtime dependencies
on Electron, React, an editor, or the Rust implementation.

## Connect and start a Run

```ts
import {
  CtxmuxClient,
  createOperationKey,
  defineRun,
  inputOperationKey,
} from "@ctxmux/sdk";

const client = new CtxmuxClient({ socketPath: "target/ctxmux.sock" });
const operationKey = createOperationKey(); // retain until disposition is known
const run = await client.start(
  defineRun("/bin/sh", {
    cwd: process.cwd(),
    size: { cols: 120, rows: 40 },
  }),
  operationKey,
);
```

`CtxmuxClient` is a stateless connector. Calls such as `start`, `list`,
`status`, `fork`, `input`, `resize`, and `stop` use short-lived protocol connections.
Closing the SDK process does not stop a daemon-owned Run.

`start` and `fork` accept an optional caller-retained creation operation key.
When a connection closes before its response is known, retry the exact request
with that same key: while the Run is retained, ctxmux returns that Run's current
`RunInfo` instead of spawning another process. Different semantics return
`creation_conflict`. Keys are byte-exact, non-empty, well-formed JavaScript
strings whose UTF-8 encoding is at most 128 bytes. Lone UTF-16 surrogates are
rejected instead of being silently replaced during UTF-8 encoding. Keys last
only with the Run in its memory or persistent retention class; they are not
Session IDs, tags, credentials, or a global exactly-once claim. A call that
omits the key gets a fresh UUID and therefore cannot be manually retried after
an uncertain response.

## Recover a native Input result

Retain the complete operation until its disposition is known. If the response
is lost, a fresh client can retry that exact operation against the same daemon
incarnation without writing the bytes a second time:

```ts
const current = await client.status(run.id);
if (current.applied_input_bytes === null) {
  throw new Error("Run has no current native Input cursor");
}

const operation = {
  daemonInstance: await client.daemonInstance(),
  operationKey: inputOperationKey(),
  runId: run.id,
  expectedByte: current.applied_input_bytes,
  data: "continue\n",
};

const reconnected = new CtxmuxClient({ socketPath: "target/ctxmux.sock" });
const { receipt } = await reconnected.recoverableInput(operation);
console.log(receipt.start_byte, receipt.end_byte);
```

The operation is recoverable only while its bounded Run-local result remains
retained and the original daemon incarnation is still serving. Use a fresh key
for each new logical Input. `receipt` proves the exact half-open byte range
applied at the daemon-owned PTY write boundary; it does not prove that the
target process read, understood, acknowledged, or replied to those bytes.

## Discover and observe a tmux-owned pane

Tmux discovery and import use the same public daemon protocol as native Runs:

```ts
const { tmuxVersion, panes } = await client.discoverTmux("/path/to/tmux.sock");
if (panes.length === 0) throw new Error("no live tmux panes");

const run = await client.importTmux(panes[0].socket_path, panes[0].pane_id);
const attachment = await client.attach(run.id);

console.log(tmuxVersion, run.backend, run.capabilities);
for await (const event of attachment.events()) {
  if (event.type === "output") {
    process.stdout.write(Uint8Array.from(event.chunk.data));
  }
}
```

One imported Run represents the selected pane at its complete import identity:
server epoch, session, window, pane, and pane PID. `tmuxVersion` and the
backend's `tmux_version` are reported by the selected server; the daemon checks
the local client executable separately. Target relocation, respawn, death, or
server replacement interrupts the Run rather than silently following it.

The server/session/window/pane fields live in `run.backend`; the pane PID
observed at import is `run.pid`. For tmux that PID is identity evidence, not
ctxmux process authority. A linked pane may appear in multiple discovery rows;
because generation 9 imports by socket path plus pane ID, an ambiguous linked
target is rejected rather than selected by row order.

The tmux slice is read-only and memory-only. `run.spec` is `null`; input,
resize, stop, and both fork levels are unsupported. Replay is
`raw_since_import`, its initial snapshot is truncated, and a later tmux source
pause remains visible as a gap/truncated replay. ctxmux does not call
`capture-pane` to synthesize the missing prefix. Detaching the SDK or stopping
the ctxmux daemon closes only ctxmux's Control Mode client, not the tmux-owned
pane, session, or server.

This pre-stable adapter is implemented, and its real TypeScript,
controlling-PTY CLI, and pause/late-replay gates pass locally. It does not
become release-qualified until the required Ubuntu minimum and macOS current
server-version lanes also pass.

## Bind an Integration explicitly

```ts
import { CtxmuxClient, registerIntegration } from "@ctxmux/sdk";
import { shellIntegration } from "@ctxmux/sdk/integrations";

const client = new CtxmuxClient({ socketPath: "target/ctxmux.sock" });
const shell = registerIntegration(client, shellIntegration);
const run = await shell.start({
  args: ["-i"],
  cwd: process.cwd(),
});
```

Registration binds one imported module to the existing raw client. It performs
no package discovery and owns no Run state. The returned Run remains available
through `client.status`, `client.attach`, and the rest of the raw SDK even if
the Integration observer or its host disappears.
Registered `start` and `forkLevelB` accept one narrow optional object containing
`detection` inputs and `operationKey`. They pass the key unchanged to the
generic Run client; the Integration does not own or reinterpret creation
identity.

The Integration contract is Provider-neutral. Agent products keep Provider
session identifiers, semantic replay parsing, permissions, Agent status, and
native resume construction in their own Provider modules. A Level B-capable
host binds provenance to the exact parent and supplies a complete generic
replacement `RunSpec`; ctxmux executes that plan and records lineage. Missing
provenance fails before mutation and never becomes a Level A request.

## Attach to a Run

```ts
const attachment = await client.attach(run.id);

const input = await attachment.input("printf 'hello\\n'\n");
const resize = await attachment.resize({ cols: 100, rows: 30 });
console.log(input.commandId, input.receipt.written_bytes);
console.log(resize.commandId, resize.receipt.applied_size);

for await (const event of attachment.events()) {
  if (event.type === "output") {
    process.stdout.write(Uint8Array.from(event.chunk.data));
  }
}
```

An `Attachment` owns one persistent client connection, not the Run. Use
`detach()` for a clean protocol detach or `close()` to simulate an abrupt client
disconnect. `detach()` resolves after the daemon acknowledges `Detached`;
`close()` returns immediately without sending that frame. Both leave a live Run
running. `stop()` explicitly terminates it.

On a live attachment, `input()`, `resize()`, `interrupt()`, and `stop()` allocate a
connection-local command ID and resolve only after the daemon returns the
matching owner receipt. One background receive pump demultiplexes command
results from Run events, so a slow event consumer cannot steal an acknowledgement
or block control correlation. Pending controls are bounded to 64 commands, of
which at most 32 may be input, with at most 1 MiB of raw input pending; this
keeps capacity available for resize, interrupt, and stop.

An input receipt proves that all reported bytes reached the daemon-owned PTY
write boundary, not that the child consumed or interpreted them. A resize
receipt contains the size read back from the owning PTY, which may differ from
the request. An interrupt receipt proves the native owner delivered `SIGINT` to
the Run's current foreground process group. A stop receipt reports `graceful`
or `forced` only after the direct child is reaped and the complete owned POSIX
session is empty; the final `exited` event remains a later publication fact.

`CtxmuxClient.input()`, `resize()`, `interrupt()`, and `stop()` expose the same typed receipts
as `{ run, receipt }` over short-lived connections. A rejected control throws
`CtxmuxCommandError` with `not_applied` or `unknown`. If a sent command loses its
unique result, the SDK reports `unknown`, closes that attachment when required,
and never reconnects, replays, or guesses. Callers must not automatically retry
uncertain input. `detach()` first fences new commands, waits for every pending
result, sends `Detach`, and resolves only after the daemon acknowledgement.

`attach(id, afterByte)` resumes ordered output after the last observed cumulative byte cursor.
Inspect `attachment.snapshot.replay.truncated` before assuming the retained
4 MiB replay contains the complete history. Generation 8 represents cursors as
JavaScript numbers, so the SDK rejects values above `Number.MAX_SAFE_INTEGER`
instead of allowing replay positions to round silently.

The daemon sends a metadata-only `attached` header followed by bounded ordered
replay events. `attach()` reassembles those frames before it resolves, so the
public snapshot shape stays unchanged even when retained history exceeds the
1 MiB per-frame transport ceiling.

The generated wire type is `AttachedHeader`, which cannot contain replay
chunks. `AttachedSnapshot` is the SDK result after reassembly, not a legal
first-frame payload.

Every daemon frame is runtime-validated before generated TypeScript types are
exposed. Malformed JSON, invalid UTF-8, duplicate object members, invalid nested
variants, and unsafe cursor integers fail closed with a boundary error.
Live events are queued under fixed count and byte budgets. Only dropped output
may be coalesced into an explicit `gap`; tmux and other non-output events are
never disguised as output loss, and terminal lifecycle gets an independent
reserved slot.

## Protocol source of truth

Types under `src/generated` are generated from `ctxmux-protocol` with `ts-rs`.
Run `scripts/generate-protocol-types.sh` from the repository root after changing
Rust wire types. `scripts/check-protocol-types.sh` verifies that committed
declarations have not drifted.
