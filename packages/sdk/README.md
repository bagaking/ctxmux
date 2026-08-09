# `@ctxmux/sdk`

TypeScript client for one local ctxmux daemon. It has no runtime dependencies
on Electron, React, an editor, or the Rust implementation.

## Connect and start a Run

```ts
import { CtxmuxClient, defineRun } from "@ctxmux/sdk";

const client = new CtxmuxClient({ socketPath: "target/ctxmux.sock" });
const run = await client.start(
  defineRun("/bin/sh", {
    cwd: process.cwd(),
    size: { cols: 120, rows: 40 },
  }),
);
```

`CtxmuxClient` is a stateless connector. Calls such as `start`, `list`,
`status`, `input`, `resize`, and `stop` use short-lived protocol connections.
Closing the SDK process does not stop a daemon-owned Run.

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
through the attachment event stream. Generation 1 has no command correlation
ID, so these promises must not be treated as remote acknowledgements. Short
`CtxmuxClient` request methods do wait for their protocol response.

`attach(id, afterSeq)` resumes ordered output after the last observed sequence.
Inspect `attachment.snapshot.replay.truncated` before assuming the retained
4 MiB replay contains the complete history. Generation 1 represents cursors as
JavaScript numbers, so the SDK rejects values above `Number.MAX_SAFE_INTEGER`
instead of allowing replay positions to round silently.

Every daemon frame is runtime-validated before generated TypeScript types are
exposed. Malformed JSON, invalid UTF-8, duplicate object members, invalid nested
variants, and unsafe cursor integers fail closed with a boundary error.

## Protocol source of truth

Types under `src/generated` are generated from `ctxmux-protocol` with `ts-rs`.
Run `scripts/generate-protocol-types.sh` from the repository root after changing
Rust wire types. `scripts/check-protocol-types.sh` verifies that committed
declarations have not drifted.
