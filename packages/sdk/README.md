# `@ctxmux/sdk`

TypeScript client for one local ctxmux daemon. It has no runtime dependencies
on Electron, React, an editor, or the Rust implementation.

## Connect and start a Run

```ts
import {
  CtxmuxClient,
  CtxmuxCommandError,
  CtxmuxUnsupportedCapabilityError,
  RUNTIME_CAPABILITY_NATIVE_RECOVERABLE_STOP,
  RUNTIME_CAPABILITY_NATIVE_START,
  RUNTIME_CAPABILITY_PERSISTENT_STATE,
  createOperationKey,
  defineRun,
  inputOperationKey,
  stopOperationKey,
} from "@ctxmux/sdk";

const client = new CtxmuxClient({
  socketPath: "target/ctxmux.sock",
  requiredCapabilities: { [RUNTIME_CAPABILITY_NATIVE_START]: 1 },
});
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

## Inspect the Runtime endpoint

```ts
const runtime = await client.runtimeInfo();
console.log(
  runtime.daemonInstanceId,
  runtime.runtimeId,
  runtime.runtimeIdPersistence,
  runtime.buildId,
  runtime.protocolGeneration,
  runtime.platform,
  runtime.arch,
  runtime.capabilities,
);
```

`runtimeId` names the logical Runtime. `runtimeIdPersistence` is `daemon` when
that identity lasts one memory-only daemon lifetime and `state_dir` when the
selected state directory preserves it. Persistent cold replacement keeps the
Runtime ID while changing `daemonInstanceId`; memory-only cold replacement
changes both; a validated planned exec keeps both.

`buildId` is an opaque daemon-authored label that may change on exec. Compare
it exactly; do not parse it or treat it as a commit, binary hash, signature, or
attestation. `platform` and `arch` use Rust serving-build vocabulary from
`std::env::consts`: an Apple Silicon macOS build reports `macos` and `aarch64`,
not Node's `darwin` and `arm64`. These are build facts, not host probes.

The flat capability record declares the highest fully implemented version for
each exact endpoint key. The complete initial catalog, mode availability, and
safe-integer domain are owned by [the protocol contract](../../docs/protocol.md#connection-state).
An absent key is unsupported; an advertised version satisfies a requirement
only when it is greater than or equal to the requested version.

Identity and capability requirements are local to one client and are checked
against Hello on the same connection before a business Request or Attach
frame. Retain the exact `RuntimeIdentity` returned by a trusted connection and
pass it as `expectedRuntimeIdentity` to fence later dispatch without a
`runtimeInfo()` preflight race. `runtimeInfo()` itself remains raw inspection,
so a different live Runtime can still be diagnosed:

```ts
const inspector = new CtxmuxClient({ socketPath: "target/ctxmux.sock" });
const observed = await inspector.runtimeInfo();
const persistentClient = new CtxmuxClient({
  socketPath: "target/ctxmux.sock",
  expectedRuntimeIdentity: observed,
  requiredCapabilities: { [RUNTIME_CAPABILITY_PERSISTENT_STATE]: 1 },
});

const current = await persistentClient.runtimeInfo(); // ignores local requirements
try {
  await persistentClient.list(); // requirements apply before dispatch
} catch (error) {
  if (error instanceof CtxmuxUnsupportedCapabilityError) {
    console.error(
      error.code, // "unsupported_capability"
      error.capability,
      error.requiredVersion,
      error.advertisedVersion, // undefined when absent
    );
  }
}
```

Keys are compared exactly; the SDK does not whitelist, normalize, infer them
from an operation, or send them on the wire. A mismatch closes before business
dispatch and never authorizes fallback. Raw inspection still validates the
wire shape and protocol generation. Endpoint capabilities do not guarantee
that a particular Run, target, external tmux server, or caller plan is usable.
`run.capabilities` remains the per-Run Backend truth, while Integration
detection and materialization capabilities remain host-local Integration truth.

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

## Recover a native Stop result

Prepare and retain the complete operation before sending Stop. `prepareStop`
checks `native.recoverable_stop: 1`, captures the exact daemon incarnation, and
does not mutate the Run:

```ts
const stopOperation = await client.prepareStop(run.id, stopOperationKey());

try {
  const { receipt } = await client.stop(stopOperation);
  console.log(receipt.disposition); // "graceful" or "forced"
} catch (error) {
  if (
    !(error instanceof CtxmuxCommandError) ||
    error.disposition !== "unknown"
  ) {
    throw error;
  }
  // Transport loss made the first response unknown; keep the operation.
  const replacement = new CtxmuxClient({
    socketPath: "target/ctxmux.sock",
    requiredCapabilities: {
      [RUNTIME_CAPABILITY_NATIVE_RECOVERABLE_STOP]: 1,
    },
  });
  const { receipt } = await replacement.stop(stopOperation);
  console.log(receipt.disposition);
}
```

The same key and Run join an in-flight Stop or replay its retained receipt. A
different key for that Run, or the same key for another retained Run, throws
`CtxmuxCommandError` with `stop_operation_conflict` before mutation. The record
lasts until the exact Run is collected and survives a validated planned exec;
it does not survive a cold daemon replacement. A new daemon therefore rejects
the old operation with `daemon_instance_mismatch` instead of guessing whether
an old process was stopped.

An attachment uses the same retained operation:

```ts
const attachment = await client.attach(run.id);
const stopOperation = await client.prepareStop(run.id);
const { receipt, commandId } = await attachment.stop(stopOperation);
```

`commandId` correlates the result on that attachment only. If the connection
loses the result, retry `client.stop(stopOperation)` with a fresh client; never
turn the attachment command ID into a retry key.

If the Run may already be terminal and the caller needs both the retained Stop
receipt and a fresh attachment, carry the operation in the initial composite
request:

```ts
const { attachment, stop } = await client.attachRecoverableStop(stopOperation);
console.log(stop.receipt.disposition);
for await (const event of attachment.events()) {
  // retained replay is in attachment.snapshot; this stream ends after terminal
}
```

Ordinary `attach()` stays observation-only and reaches EOF after its one
terminal event. It never waits for a possible later Stop command. The explicit
composite is the race-free path when recovery intent must exist before that EOF.

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
because generation 13 imports by socket path plus pane ID, an ambiguous linked
target is rejected rather than selected by row order.

The tmux slice is read-only and memory-only. `run.spec` is `null`; input,
resize, stop, and both fork levels are unsupported. Replay is
`raw_since_import`, its initial snapshot is truncated, and a later tmux source
pause remains visible as raw-output gap/truncated replay. A live attachment
that actually loses a tmux observation receives cursor-free
`observation_discontinuity` and ends; a later attachment starts a new
observation boundary and does not claim to restore the missing semantics. ctxmux does not call
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
The packaged `@ctxmux/sdk/integrations` subpath contains only the Provider-neutral
shell conformance Integration. Agent- or vendor-specific Integrations belong to
the embedding product that imports them.
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

`forkLevelB` reports fail-closed host-boundary errors as exported structured
classes. `IntegrationUnavailableError` carries the unavailable detection,
`IntegrationCapabilityError` names an undeclared capability,
`IntegrationProvenanceError.reason` is `missing` or `wrong_source`, and
`IntegrationMaterializationError.reason` is `missing_planner` or
`invalid_plan`. None of these paths invokes the raw fork operation: the binding
checks that the materialized Level B plan carries a complete executable
`RunSpec` before forwarding it. The public daemon protocol remains the runtime
authority and also rejects an invalid raw fork without creating a Run.

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
running. `stop(operation)` explicitly terminates it.

On a live attachment, `input()`, `resize()`, `interrupt()`, and
`stop(operation)` allocate a
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

`CtxmuxClient.input()`, `resize()`, `interrupt()`, and `stop(operation)` expose
the same typed receipts
as `{ run, receipt }` over short-lived connections. A rejected control throws
`CtxmuxCommandError` with `not_applied` or `unknown`. If a sent command loses its
unique result, the SDK reports `unknown`, closes that attachment when required,
and never reconnects or guesses from a command ID. Callers must not
automatically retry uncertain ordinary input, resize, or interrupt. Recoverable
Stop is retried only by resending its complete retained operation. `detach()`
first fences new commands, waits for every pending
result, sends `Detach`, and resolves only after the daemon acknowledgement.

`attach(id, afterByte)` resumes ordered output after the last observed cumulative byte cursor.
Inspect `attachment.snapshot.replay.truncated` before assuming the retained
4 MiB replay contains the complete history. Generation 13 represents cursors as
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
Live events are queued under fixed count and byte budgets. Raw-output delivery
loss may be coalesced into an explicit `gap`. A cursor-free
`observation_discontinuity` instead reports missing non-output semantics; tmux
and other non-output events are never disguised as replayable byte loss, and
terminal lifecycle gets an independent reserved slot.

## Protocol source of truth

Types under `src/generated` are generated from `ctxmux-protocol` with `ts-rs`.
Run `scripts/generate-protocol-types.sh` from the repository root after changing
Rust wire types. `scripts/check-protocol-types.sh` verifies that committed
declarations have not drifted.
