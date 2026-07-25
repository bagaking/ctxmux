import assert from "node:assert/strict";
import {
  execFile as execFileCallback,
  spawn,
  type ChildProcess,
} from "node:child_process";
import { chmod, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import test from "node:test";

import {
  Attachment,
  CtxmuxClient,
  CtxmuxProtocolError,
  IntegrationProvenanceError,
  defineRun,
  registerIntegration,
  type IntegrationObserver,
  type RunEvent,
  type RunId,
} from "../src/index.ts";
import {
  codexIntegration,
  isCodexSessionProvenance,
  type CodexSessionProvenance,
  type CodexSemanticEvent,
} from "../src/integrations/index.ts";

const execFile = promisify(execFileCallback);
const daemonBinary = requiredEnvironment("CTXMUXD_BIN");
const cliBinary = requiredEnvironment("CTXMUX_BIN");

test(
  "CLI and TypeScript SDK share one daemon-owned Run across client exits",
  { timeout: 15_000 },
  async (context) => {
    const { client, socketPath } = await startTestDaemon(context);

    const shell = concatShell(
      "printf 'READY\\n';",
      "while IFS= read -r line; do",
      'case "$line" in',
      "size) printf 'SIZE:'; stty size ;;",
      "quit) printf 'OUT:quit\\n'; exit 7 ;;",
      "*) printf 'OUT:%s\\n' \"$line\" ;;",
      "esac; done",
    );
    const startedByCli = await execFile(cliBinary, [
      "--socket",
      socketPath,
      "start",
      "--",
      "/bin/sh",
      "-c",
      shell,
    ]);
    const runId = startedByCli.stdout.trim() as RunId;
    assert.notEqual(runId, "");

    const firstClient = new CtxmuxClient({ socketPath });
    const initialStatus = await firstClient.status(runId);
    const pid = initialStatus.pid;
    assert.equal(initialStatus.state.type, "running");
    assert.notEqual(pid, null);

    const firstAttachment = await firstClient.attach(runId);
    let observed = replayBytes(firstAttachment.snapshot.replay.chunks);
    let lastSequence = firstAttachment.snapshot.replay.head_seq;
    ({ observed, lastSequence } = await waitForOutput(
      firstAttachment,
      observed,
      lastSequence,
      "READY",
    ));
    await step("first attachment input", firstAttachment.input("hello\n"));
    ({ observed, lastSequence } = await waitForOutput(
      firstAttachment,
      observed,
      lastSequence,
      "OUT:hello",
    ));

    firstAttachment.close();
    const statusAfterSdkDisconnect = await waitForNoAttachments(
      new CtxmuxClient({ socketPath }),
      runId,
    );
    assert.equal(statusAfterSdkDisconnect.pid, pid);
    assert.equal(statusAfterSdkDisconnect.state.type, "running");

    const reconnectedClient = new CtxmuxClient({ socketPath });
    await step(
      "resize through reconnected SDK client",
      reconnectedClient.resize(runId, { cols: 120, rows: 40 }),
    );
    const secondAttachment = await reconnectedClient.attach(
      runId,
      lastSequence,
    );
    assert.equal(secondAttachment.snapshot.run.pid, pid);
    assert.equal(secondAttachment.snapshot.replay.truncated, false);
    observed = replayBytes(secondAttachment.snapshot.replay.chunks);
    lastSequence = secondAttachment.snapshot.replay.head_seq;
    await step(
      "second attachment size input",
      secondAttachment.input("size\n"),
    );
    ({ observed, lastSequence } = await waitForOutput(
      secondAttachment,
      observed,
      lastSequence,
      "SIZE:40 120",
    ));
    await step(
      "second attachment quit input",
      secondAttachment.input("quit\n"),
    );
    ({ observed, lastSequence } = await waitForOutput(
      secondAttachment,
      observed,
      lastSequence,
      "OUT:quit",
    ));
    const exit = await waitForExit(secondAttachment);
    assert.deepEqual(exit, { type: "exited", code: 7, signal: null });

    await assert.rejects(
      reconnectedClient.input(runId, "after exit"),
      (error: unknown) =>
        error instanceof CtxmuxProtocolError &&
        error.code === "invalid_run_state",
    );

    const sdkRun = await reconnectedClient.start(
      defineRun("/bin/sh", { args: ["-c", "sleep 30"] }),
    );
    const sdkAttachment = await reconnectedClient.attach(sdkRun.id);
    await sdkAttachment.detach();
    await waitForNoAttachments(reconnectedClient, sdkRun.id);
    const cliStatus = await execFile(cliBinary, [
      "--socket",
      socketPath,
      "status",
      sdkRun.id,
    ]);
    assert.match(
      cliStatus.stdout,
      new RegExp(`^${sdkRun.id}\\trunning\\tpid=`),
    );
    await step("stop SDK-created Run", reconnectedClient.stop(sdkRun.id));
  },
);

test(
  "Codex Integration resumes declared context through a public Level B fork",
  { timeout: 15_000 },
  async (context) => {
    const { client, directory } = await startTestDaemon(context);
    const executable = join(directory, "codex.mjs");
    await writeFile(
      executable,
      `#!/usr/bin/env node
const args = process.argv.slice(2);
if (args[0] === "--version") {
  console.log("codex-cli 0.144.4");
} else if (args[0] === "exec" && args[1] === "--help") {
  console.log("      --json  Print JSONL");
} else if (args[0] === "exec" && args[1] === "resume" && args[2] === "--help") {
  console.log("      --json  Resume as JSONL");
} else if (args[0] === "exec" && args[1] === "--json" && args[2] === "--") {
  console.log(JSON.stringify({ type: "thread.started", thread_id: "session-123", argv: args }));
  setInterval(() => {}, 1_000);
} else if (args[0] === "exec" && args[1] === "resume" && args[2] === "--json" && args[3] === "--") {
  console.log(JSON.stringify({ type: "turn.started", argv: args }));
  setInterval(() => {}, 1_000);
} else {
  process.exitCode = 64;
}
`,
    );
    await chmod(executable, 0o755);
    const prompt = "review 'quoted'; $(touch never)\nthen explain";

    const registered = registerIntegration(client, codexIntegration);
    const run = await registered.start(
      { prompt, cwd: directory },
      { executable },
    );
    assert.ok(run.spec);
    assert.deepEqual(run.spec.args, ["exec", "--json", "--", prompt]);
    assert.deepEqual(run.spec.declared_inputs, [
      { kind: "workspace", reference: directory },
    ]);
    const pid = run.pid;
    assert.notEqual(pid, null);

    const attachment = await client.attach(run.id);
    const expectedRecord = JSON.stringify({
      type: "thread.started",
      thread_id: "session-123",
      argv: ["exec", "--json", "--", prompt],
    });
    const parentOutput = await waitForOutput(
      attachment,
      replayBytes(attachment.snapshot.replay.chunks),
      attachment.snapshot.replay.head_seq,
      expectedRecord,
    );
    assert.match(
      new TextDecoder().decode(parentOutput.observed),
      /session-123/,
    );
    attachment.close();

    const provenanceAttachment = await client.attach(run.id);
    const parentObserver = registered.createObserver(run);
    const session = await waitForCodexSession(
      provenanceAttachment,
      parentObserver,
    );
    const sessionId = session.sessionId;
    const unrelatedRecord = JSON.stringify({
      type: "thread.started",
      thread_id: "from-unrelated-run",
    });
    const unrelated = await client.start(
      defineRun("/bin/sh", {
        args: ["-c", `printf '%s\\n' '${unrelatedRecord}'; sleep 30`],
      }),
    );
    const unrelatedAttachment = await client.attach(unrelated.id);
    const unrelatedOutput = await nextOutputEvent(unrelatedAttachment);
    assert.throws(
      () => parentObserver.observe(unrelatedOutput),
      (error: unknown) => error instanceof IntegrationProvenanceError,
      "another Run's real attachment event must not authenticate the parent",
    );
    const beforeRejectedForks = (await client.list()).map(({ id }) => id);
    const rejectedConfig = {
      session,
      prompt: "must not create a child",
      cwd: directory,
    };
    await assert.rejects(
      registered.forkLevelB(unrelated, rejectedConfig, { executable }),
      (error: unknown) => error instanceof IntegrationProvenanceError,
    );
    await assert.rejects(
      registered.forkLevelB(
        run,
        { ...rejectedConfig, session: { ...session } },
        { executable },
      ),
      (error: unknown) => error instanceof IntegrationProvenanceError,
    );
    assert.deepEqual(
      (await client.list()).map(({ id }) => id),
      beforeRejectedForks,
      "rejected provenance must not create a daemon Run",
    );
    unrelatedAttachment.close();
    await client.stop(unrelated.id);

    provenanceAttachment.close();
    const rawStatus = await waitForNoAttachments(client, run.id);
    assert.equal(rawStatus.pid, pid);
    assert.equal(rawStatus.state.type, "running");
    const continuation = "compare the two candidates";
    const artifactReference = "artifact://review-plan.json";
    const child = await registered.forkLevelB(
      run,
      {
        session,
        prompt: continuation,
        cwd: directory,
        artifactReferences: [artifactReference],
      },
      { executable },
    );
    assert.notEqual(child.id, run.id);
    assert.notEqual(child.pid, pid);
    assert.deepEqual(child.lineage, {
      parent: run.id,
      fidelity: "level_b",
    });
    assert.ok(child.spec);
    assert.deepEqual(child.spec.args, [
      "exec",
      "resume",
      "--json",
      "--",
      sessionId,
      continuation,
    ]);
    assert.deepEqual(child.spec.declared_inputs, [
      { kind: "workspace", reference: directory },
      { kind: "artifact", reference: artifactReference },
      { kind: "context", reference: sessionId },
    ]);

    const childAttachment = await client.attach(child.id);
    const resumedRecord = JSON.stringify({
      type: "turn.started",
      argv: ["exec", "resume", "--json", "--", sessionId, continuation],
    });
    await waitForOutput(
      childAttachment,
      replayBytes(childAttachment.snapshot.replay.chunks),
      childAttachment.snapshot.replay.head_seq,
      resumedRecord,
    );
    childAttachment.close();
    assert.equal((await client.status(run.id)).state.type, "running");
    assert.equal((await client.status(child.id)).state.type, "running");
    await client.stop(run.id);
    await client.stop(child.id);
  },
);

test(
  "TypeScript SDK imports one real tmux pane through the public read-only Run boundary",
  { timeout: 20_000 },
  async (context) => {
    const { client } = await startTestDaemon(context);
    const tmux = await startTmuxFixture(context);
    if (tmux === undefined) {
      return;
    }

    const discovered = await step(
      "discover real tmux pane",
      client.discoverTmux(tmux.socketPath),
    );
    assert.equal(discovered.tmuxVersion, tmux.serverVersion);
    const pane = discovered.panes.find(
      ({ session_id }) => session_id === tmux.sessionId,
    );
    assert.ok(pane, "SDK discovery must expose the selected tmux session");
    assert.equal(pane.socket_path, tmux.socketPath);
    assert.equal(pane.tmux_version, tmux.serverVersion);

    const run = await step(
      "import real tmux pane",
      client.importTmux(tmux.socketPath, pane.pane_id),
    );
    assert.equal(run.backend.type, "tmux");
    if (run.backend.type !== "tmux") {
      throw new Error("imported pane did not expose the tmux backend");
    }
    assert.equal(run.backend.pane_id, pane.pane_id);
    assert.deepEqual(run.capabilities, {
      input: false,
      resize: false,
      stop: false,
      fork_level_a: false,
      fork_level_b: false,
      replay: "raw_since_import",
    });

    const attachment = await step(
      "attach imported tmux pane",
      client.attach(run.id),
    );
    assert.equal(attachment.snapshot.replay.truncated, true);
    assert.deepEqual(attachment.snapshot.replay.chunks, []);
    await tmux.command([
      "send-keys",
      "-t",
      pane.pane_id,
      "sdk-public-output",
      "Enter",
    ]);
    const output = await waitForOutput(
      attachment,
      replayBytes(attachment.snapshot.replay.chunks),
      attachment.snapshot.replay.head_seq,
      "TMUX:sdk-public-output",
    );
    assert.match(text(output.observed), /TMUX:sdk-public-output/);
    await attachment.detach();

    const afterDetach = await waitForNoAttachments(client, run.id);
    assert.equal(afterDetach.state.type, "running");
    assert.equal(await tmux.panePid(pane.pane_id), pane.pane_pid);
    for (const operation of [
      () => client.input(run.id, "must-not-reach-tmux"),
      () => client.resize(run.id, { cols: 100, rows: 40 }),
      () => client.stop(run.id),
    ]) {
      await assert.rejects(
        operation(),
        (error: unknown) =>
          error instanceof CtxmuxProtocolError &&
          error.code === "unsupported_capability",
      );
    }
    assert.equal(await tmux.panePid(pane.pane_id), pane.pane_pid);
  },
);

async function startTestDaemon(context: test.TestContext): Promise<{
  client: CtxmuxClient;
  directory: string;
  socketPath: string;
}> {
  const directory = await mkdtemp(join(tmpdir(), "ctxmux-sdk-"));
  const socketPath = join(directory, "ctxmux.sock");
  const daemon = spawn(daemonBinary, ["--socket", socketPath], {
    stdio: ["ignore", "ignore", "pipe"],
  });
  let daemonError = "";
  daemon.stderr?.on("data", (chunk: Buffer) => {
    daemonError += chunk.toString("utf8");
    process.stderr.write(chunk);
  });
  context.after(async () => {
    await terminate(daemon);
    await rm(directory, { recursive: true, force: true });
  });

  const client = new CtxmuxClient({ socketPath });
  await waitForDaemon(client, daemon, () => daemonError);
  return { client, directory, socketPath };
}

async function startTmuxFixture(context: test.TestContext): Promise<
  | {
      readonly socketPath: string;
      readonly serverVersion: string;
      readonly sessionId: string;
      command(args: readonly string[]): Promise<string>;
      panePid(paneId: string): Promise<number>;
    }
  | undefined
> {
  const executable = process.env.CTXMUX_TMUX_BIN ?? "tmux";
  try {
    await execFile(executable, ["-V"]);
  } catch (error) {
    if (process.env.CTXMUX_REQUIRE_TMUX === "1") {
      throw new Error("required tmux executable is unavailable", {
        cause: error,
      });
    }
    context.diagnostic("skipping real tmux SDK test: tmux is unavailable");
    context.skip("tmux executable is unavailable");
    return undefined;
  }

  const directory = await mkdtemp(join(tmpdir(), "ctxmux-sdk-tmux-"));
  const socketPath = join(directory, "tmux.sock");
  const sessionName = "ctxmux-sdk-target";
  const baseArgs = ["-S", socketPath] as const;
  const command = async (args: readonly string[]): Promise<string> => {
    const result = await execFile(executable, [...baseArgs, ...args]);
    return result.stdout.trim();
  };
  await command([
    "new-session",
    "-d",
    "-s",
    sessionName,
    "/bin/sh",
    "-c",
    concatShell(
      "stty -echo;",
      "printf 'BEFORE-IMPORT\\n';",
      "while IFS= read -r line; do",
      "printf 'TMUX:%s\\n' \"$line\";",
      "done",
    ),
  ]);
  let serverPid: number | undefined;
  context.after(async () => {
    try {
      await command(["kill-server"]);
    } catch (error) {
      if (serverPid === undefined || processIsRunning(serverPid)) {
        throw new Error("failed to stop the tmux SDK fixture server", {
          cause: error,
        });
      }
    }
    if (serverPid !== undefined) {
      await waitForProcessExit(serverPid);
    }
    await rm(directory, { recursive: true, force: true });
  });

  const serverVersion = await command(["display-message", "-p", "#{version}"]);
  serverPid = Number(await command(["display-message", "-p", "#{pid}"]));
  if (!Number.isSafeInteger(serverPid) || serverPid <= 0) {
    throw new Error("tmux SDK fixture returned an invalid server PID");
  }
  const sessionId = await command([
    "display-message",
    "-p",
    "-t",
    sessionName,
    "#{session_id}",
  ]);
  return {
    socketPath,
    serverVersion,
    sessionId,
    command,
    async panePid(paneId: string): Promise<number> {
      return Number(
        await command(["display-message", "-p", "-t", paneId, "#{pane_pid}"]),
      );
    },
  };
}

async function waitForProcessExit(pid: number): Promise<void> {
  const deadline = Date.now() + 5_000;
  while (Date.now() <= deadline) {
    if (!processIsRunning(pid)) {
      return;
    }
    await delay(20);
  }
  throw new Error(`tmux SDK fixture server ${pid} did not exit`);
}

function processIsRunning(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    if (
      typeof error === "object" &&
      error !== null &&
      "code" in error &&
      error.code === "ESRCH"
    ) {
      return false;
    }
    throw error;
  }
}

async function waitForDaemon(
  client: CtxmuxClient,
  daemon: ChildProcess,
  stderr: () => string,
): Promise<void> {
  const deadline = Date.now() + 5_000;
  let lastError: unknown;
  while (Date.now() <= deadline) {
    if (daemon.exitCode !== null) {
      throw new Error(`ctxmuxd exited before startup: ${stderr()}`);
    }
    try {
      await client.ping();
      return;
    } catch (error) {
      lastError = error;
      await delay(20);
    }
  }
  throw new Error(
    `ctxmuxd did not become ready; last client error: ${String(lastError)}; stderr: ${stderr()}`,
  );
}

async function waitForOutput(
  attachment: Attachment,
  initial: Uint8Array,
  initialSequence: number,
  expected: string,
): Promise<{ observed: Uint8Array; lastSequence: number }> {
  let observed = initial;
  let lastSequence = initialSequence;
  const deadline = Date.now() + 5_000;
  while (!text(observed).includes(expected)) {
    if (Date.now() > deadline) {
      throw new Error(
        `timed out waiting for ${expected}; received ${text(observed)}`,
      );
    }
    const event = await attachment.nextEvent();
    assert.notEqual(
      event,
      undefined,
      "attachment closed before expected output",
    );
    if (event?.type === "output") {
      lastSequence = event.chunk.seq;
      observed = append(observed, Uint8Array.from(event.chunk.data));
    } else if (event?.type === "gap") {
      throw new Error(`unexpected output gap at ${event.head_seq}`);
    } else if (event?.type === "exited") {
      throw new Error(
        `Run exited before ${expected}: ${JSON.stringify(event.state)}; received ${JSON.stringify(text(observed))}`,
      );
    }
  }
  return { observed, lastSequence };
}

async function waitForExit(
  attachment: Attachment,
): Promise<Extract<RunEvent, { type: "exited" }>["state"]> {
  const deadline = Date.now() + 5_000;
  while (Date.now() <= deadline) {
    const event = await attachment.nextEvent();
    if (event?.type === "exited") {
      return event.state;
    }
    if (event?.type === "gap") {
      throw new Error(`unexpected output gap at ${event.head_seq}`);
    }
  }
  throw new Error("timed out waiting for Run exit");
}

async function waitForCodexSession(
  attachment: Attachment,
  observer: IntegrationObserver<CodexSemanticEvent>,
): Promise<CodexSessionProvenance> {
  for (const chunk of attachment.snapshot.replay.chunks) {
    const session = observer
      .observe({ type: "output", chunk })
      .find(isCodexSessionProvenance);
    if (session !== undefined) {
      return session;
    }
  }
  const deadline = Date.now() + 5_000;
  while (Date.now() <= deadline) {
    const event = await attachment.nextEvent();
    if (event === undefined || event.type === "exited") {
      break;
    }
    if (event.type === "gap") {
      throw new Error(`unexpected output gap at ${event.head_seq}`);
    }
    const session = observer.observe(event).find(isCodexSessionProvenance);
    if (session !== undefined) {
      return session;
    }
  }
  throw new Error("parent attachment did not emit Codex session provenance");
}

async function nextOutputEvent(
  attachment: Attachment,
): Promise<Extract<RunEvent, { type: "output" }>> {
  const replay = attachment.snapshot.replay.chunks[0];
  if (replay !== undefined) {
    return { type: "output", chunk: replay };
  }
  const deadline = Date.now() + 5_000;
  while (Date.now() <= deadline) {
    const event = await attachment.nextEvent();
    if (event?.type === "output") {
      return event;
    }
    if (event === undefined || event.type === "exited") {
      break;
    }
    if (event.type === "gap") {
      throw new Error(`unexpected output gap at ${event.head_seq}`);
    }
  }
  throw new Error("attachment did not produce an output event");
}

async function waitForNoAttachments(client: CtxmuxClient, id: RunId) {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    const status = await client.status(id);
    if (status.attachments === 0) {
      return status;
    }
    await delay(20);
  }
  throw new Error("daemon did not release disconnected SDK attachment");
}

async function terminate(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null) {
    return;
  }
  child.kill("SIGINT");
  await Promise.race([
    new Promise<void>((resolve) => child.once("exit", () => resolve())),
    delay(1_000).then(() => {
      child.kill("SIGKILL");
    }),
  ]);
}

function replayBytes(
  chunks: readonly { readonly data: readonly number[] }[],
): Uint8Array {
  return Uint8Array.from(chunks.flatMap((chunk) => [...chunk.data]));
}

function append(left: Uint8Array, right: Uint8Array): Uint8Array {
  const output = new Uint8Array(left.length + right.length);
  output.set(left);
  output.set(right, left.length);
  return output;
}

function text(bytes: Uint8Array): string {
  return new TextDecoder().decode(bytes);
}

function concatShell(...parts: readonly string[]): string {
  return parts.join(" ");
}

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function requiredEnvironment(name: string): string {
  const value = process.env[name];
  if (value === undefined || value.length === 0) {
    throw new Error(`${name} is required for the client parity test`);
  }
  return value;
}

async function step<T>(label: string, operation: Promise<T>): Promise<T> {
  try {
    return await operation;
  } catch (cause) {
    throw new Error(`client parity failed during ${label}`, { cause });
  }
}
