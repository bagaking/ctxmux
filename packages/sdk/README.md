# `@ctxmux/sdk`

TypeScript client for one local ctxmux daemon. It has no runtime dependencies
on Electron, React, an editor, or the Rust implementation.

## Connect and start a Run

```ts
import { CtxmuxClient, createOperationKey, defineRun } from "@ctxmux/sdk";

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
because generation 4 imports by socket path plus pane ID, an ambiguous linked
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

The same subpath exports `codexIntegration`. It plans `codex exec --json` only
after bounded version and help probes confirm JSONL support. The default probe
budget is five seconds; callers may supply a smaller positive integer timeout
when their own fail-fast boundary is stricter. Its optional observer converts
complete JSONL records into host-local semantic events; parser diagnostics
never replace or hide raw Run output.

When `exec resume --help` also exposes JSON support, the registered Codex
Integration can materialize an explicit Level B child from a session receipt
issued when a parent-scoped observer sees `thread.started`:

```ts
import {
  codexIntegration,
  isCodexSessionProvenance,
} from "@ctxmux/sdk/integrations";

const codex = registerIntegration(client, codexIntegration);
const observer = codex.createObserver(parent);
let session;
for await (const rawEvent of parentAttachment.events()) {
  session = observer.observe(rawEvent).find(isCodexSessionProvenance);
  if (session !== undefined) break;
}
if (session === undefined) throw new Error("Codex session was not observed");
const child = await codex.forkLevelB(parent, {
  session,
  prompt: "continue from the parent",
  cwd: process.cwd(),
  artifactReferences: ["artifact://review-plan.json"],
});
```

The child records its parent, `level_b` fidelity, and declared workspace,
artifact, and session references. The receipt is host-local and source-bound:
the `Attachment` records every live event and replay chunk against its Run, and
the parent observer rejects missing, copied-chunk, or unrelated-Run input before
semantic parsing. Copied, unbound, cross-registration, and parent-mismatched
receipt objects likewise fail before planner or raw fork. Every Level B
Integration must implement `levelBForkProvenance`; omission fails closed. This
protects supported SDK use from accidental fabrication or misrouting, but it is
not authentication against a malicious host that can bypass the Integration
and call `client.fork` directly. A Level B call on `shellIntegration` raises
`IntegrationCapabilityError`; neither path falls back to Level A.

The real semantic canary requires `OPENAI_API_KEY` or `CODEX_API_KEY`. A local
operator may instead opt in to an already authenticated Codex CLI for one run
with `CTXMUX_ALLOW_CODEX_LOGIN_AUTH=1`; login state is never used implicitly.
The scheduled workflow continues to require its repository secret.
Because native Runs merge PTY output, real Codex may emit non-JSON lines beside
valid JSONL. The canary retains their diagnostic and line-class counts;
`output_gap`, invalid UTF-8, or an oversized semantic record is fatal and cannot
produce a passing artifact.

## Attach to a Run

```ts
const attachment = await client.attach(run.id);

await attachment.input("printf 'hello\\n'\n");
await attachment.resize({ cols: 100, rows: 30 });

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

On a live attachment, `input()`, `resize()`, and `stop()` resolve when Node's
socket write callback completes. The daemon reports remote acceptance or error
through the attachment event stream. Generation 4 has no command correlation
ID, so these promises must not be treated as remote acknowledgements. Short
`CtxmuxClient` request methods do wait for their protocol response.

`attach(id, afterSeq)` resumes ordered output after the last observed sequence.
Inspect `attachment.snapshot.replay.truncated` before assuming the retained
4 MiB replay contains the complete history. Generation 4 represents cursors as
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

## Protocol source of truth

Types under `src/generated` are generated from `ctxmux-protocol` with `ts-rs`.
Run `scripts/generate-protocol-types.sh` from the repository root after changing
Rust wire types. `scripts/check-protocol-types.sh` verifies that committed
declarations have not drifted.
