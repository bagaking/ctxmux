import assert from "node:assert/strict";
import {
  execFile as execFileCallback,
  spawn,
  type ChildProcess,
} from "node:child_process";
import { once } from "node:events";
import { chmod, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { createConnection } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createInterface } from "node:readline";
import { promisify } from "node:util";
import test from "node:test";

import {
  Attachment,
  CtxmuxClient,
  CtxmuxProtocolError,
  CtxmuxUnsupportedCapabilityError,
  INTEGRATION_API_VERSION,
  IntegrationCapabilityError,
  IntegrationMaterializationError,
  IntegrationProvenanceError,
  IntegrationUnavailableError,
  PROTOCOL_VERSION,
  RUNTIME_CAPABILITY_NATIVE_START,
  RUNTIME_CAPABILITY_PERSISTENT_STATE,
  createOperationKey,
  defineRun,
  inputOperationKey,
  registerIntegration,
  type Integration,
  type IntegrationObserver,
  type IntegrationSemanticEvent,
  type RecoverableStopOperation,
  type RunEvent,
  type RunId,
  type RunInfo,
  type RunSpec,
} from "../src/index.ts";

const execFile = promisify(execFileCallback);
const daemonBinary = requiredEnvironment("CTXMUXD_BIN");
const cliBinary = requiredEnvironment("CTXMUX_BIN");

test(
  "Recoverable Stop replays one receipt after response loss and client replacement",
  { timeout: 15_000 },
  async (context) => {
    const { client, directory, socketPath } = await startTestDaemon(context);
    const markerPath = join(directory, "recoverable-stop-marker");
    const run = await client.start(
      defineRun("/bin/sh", {
        args: [
          "-c",
          concatShell(
            'trap \'printf "stop\\n" >> "$1"; exit 0\' TERM;',
            "printf 'ready\\n' > \"$1\";",
            "while IFS= read -r _line; do :; done",
          ),
          "ctxmux-recoverable-stop",
          markerPath,
        ],
      }),
      createOperationKey("sdk-recoverable-stop-run"),
    );
    const operation = await client.prepareStop(
      run.id,
      "sdk-response-loss-stop",
    );

    await waitForMarker(markerPath, "ready\n");
    await sendStopWithoutReadingResponse(socketPath, operation);
    await waitForMarker(markerPath, "ready\nstop\n");

    const replacementClient = new CtxmuxClient({ socketPath });
    const recovered = await replacementClient.stop(operation);
    assert.deepEqual(recovered.receipt, {
      type: "stop",
      disposition: "graceful",
    });
    assert.deepEqual(
      (await replacementClient.stop(operation)).receipt,
      recovered.receipt,
    );
    assert.equal(await readFile(markerPath, "utf8"), "ready\nstop\n");
  },
);

test(
  "CLI and TypeScript SDK share one daemon-owned Run across client exits",
  { timeout: 15_000 },
  async (context) => {
    const { client, socketPath } = await startTestDaemon(context);
    const runtime = await client.runtimeInfo();
    const cliRuntime = JSON.parse(
      (await execFile(cliBinary, ["--socket", socketPath, "runtime"])).stdout,
    ) as unknown;
    assert.deepEqual(cliRuntime, runtime);
    assert.deepEqual(Object.keys(runtime).sort(), [
      "arch",
      "buildId",
      "capabilities",
      "daemonInstanceId",
      "platform",
      "protocolGeneration",
      "runtimeId",
      "runtimeIdPersistence",
    ]);
    assert.equal(runtime.protocolGeneration, PROTOCOL_VERSION);
    assert.equal(runtime.runtimeIdPersistence, "daemon");
    assert.notEqual(runtime.runtimeId, runtime.daemonInstanceId);
    assert.match(runtime.buildId, /^ctxmuxd\/[^/]+$/u);
    assert.notEqual(runtime.platform, "");
    assert.notEqual(runtime.arch, "");
    assert.deepEqual(runtime.capabilities, {
      "native.execute_materialized_level_b": 1,
      "native.fork_level_a": 1,
      "native.recoverable_input": 1,
      "native.recoverable_stop": 1,
      "native.start": 1,
      "tmux.discover": 1,
      "tmux.import": 1,
    });

    const runsBeforeCapabilityRejection = await client.list();
    for (const [requirements, expectedCapability, expectedAdvertised] of [
      [
        { [RUNTIME_CAPABILITY_PERSISTENT_STATE]: 1 },
        RUNTIME_CAPABILITY_PERSISTENT_STATE,
        undefined,
      ],
      [
        { [RUNTIME_CAPABILITY_NATIVE_START]: 2 },
        RUNTIME_CAPABILITY_NATIVE_START,
        1,
      ],
    ] as const) {
      const guardedClient = new CtxmuxClient({
        socketPath,
        requiredCapabilities: requirements,
      });
      assert.deepEqual(
        await guardedClient.runtimeInfo(),
        runtime,
        "configured runtimeInfo remains raw identity inspection",
      );
      await assert.rejects(
        guardedClient.start(defineRun("/bin/true")),
        (error: unknown) =>
          error instanceof CtxmuxUnsupportedCapabilityError &&
          error.code === "unsupported_capability" &&
          error.capability === expectedCapability &&
          error.advertisedVersion === expectedAdvertised,
      );
      assert.deepEqual(
        await client.list(),
        runsBeforeCapabilityRejection,
        "a client-local capability rejection must not create a real Run",
      );
    }

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
    assert.notEqual(runId, runtime.runtimeId);
    assert.notEqual(runId, runtime.daemonInstanceId);

    const firstClient = new CtxmuxClient({ socketPath });
    const initialStatus = await firstClient.status(runId);
    const pid = initialStatus.pid;
    assert.equal(initialStatus.state.type, "running");
    assert.notEqual(pid, null);

    const firstAttachment = await firstClient.attach(runId);
    let observed = replayBytes(firstAttachment.snapshot.replay.chunks);
    let lastByte = firstAttachment.snapshot.replay.latest_output_bytes;
    ({ observed, lastByte } = await waitForOutput(
      firstAttachment,
      observed,
      lastByte,
      "READY",
    ));
    assert.deepEqual(
      await step("first attachment input", firstAttachment.input("hello\n")),
      {
        commandId: 1,
        receipt: { type: "input", written_bytes: 6 },
      },
    );
    ({ observed, lastByte } = await waitForOutput(
      firstAttachment,
      observed,
      lastByte,
      "OUT:hello",
    ));

    const daemonInstance = await firstClient.daemonInstance();
    const recoverableOperation = {
      daemonInstance,
      operationKey: inputOperationKey("sdk-recoverable-input"),
      runId,
      expectedByte: 6,
      data: "sdk\n",
    } as const;
    const applied = await firstClient.recoverableInput(recoverableOperation);
    assert.deepEqual(applied.receipt, { start_byte: 6, end_byte: 10 });
    assert.deepEqual(await firstAttachment.input("later\n"), {
      commandId: 2,
      receipt: { type: "input", written_bytes: 6 },
    });
    const retriedApplied = await new CtxmuxClient({
      socketPath,
    }).recoverableInput(recoverableOperation);
    assert.deepEqual(retriedApplied.receipt, applied.receipt);
    assert.equal(retriedApplied.run.applied_input_bytes, 16);
    ({ observed, lastByte } = await waitForOutput(
      firstAttachment,
      observed,
      lastByte,
      "OUT:sdk",
    ));
    assert.equal(text(observed).match(/OUT:sdk/g)?.length, 1);
    ({ observed, lastByte } = await waitForOutput(
      firstAttachment,
      observed,
      lastByte,
      "OUT:later",
    ));

    firstAttachment.close();
    const statusAfterSdkDisconnect = await waitForNoAttachments(
      new CtxmuxClient({ socketPath }),
      runId,
    );
    assert.equal(statusAfterSdkDisconnect.pid, pid);
    assert.equal(statusAfterSdkDisconnect.state.type, "running");

    const reconnectedClient = new CtxmuxClient({ socketPath });
    const resized = await step(
      "resize through reconnected SDK client",
      reconnectedClient.resize(runId, { cols: 120, rows: 40 }),
    );
    assert.deepEqual(resized.receipt, {
      type: "resize",
      applied_size: { cols: 120, rows: 40 },
    });
    assert.equal(resized.run.id, runId);
    const secondAttachment = await reconnectedClient.attach(runId, lastByte);
    assert.equal(secondAttachment.snapshot.run.pid, pid);
    assert.equal(secondAttachment.snapshot.replay.truncated, false);
    observed = replayBytes(secondAttachment.snapshot.replay.chunks);
    lastByte = secondAttachment.snapshot.replay.latest_output_bytes;
    assert.deepEqual(
      await step(
        "second attachment size input",
        secondAttachment.input("size\n"),
      ),
      {
        commandId: 1,
        receipt: { type: "input", written_bytes: 5 },
      },
    );
    ({ observed, lastByte } = await waitForOutput(
      secondAttachment,
      observed,
      lastByte,
      "SIZE:40 120",
    ));
    assert.deepEqual(
      await step(
        "second attachment quit input",
        secondAttachment.input("quit\n"),
      ),
      {
        commandId: 2,
        receipt: { type: "input", written_bytes: 5 },
      },
    );
    ({ observed, lastByte } = await waitForOutput(
      secondAttachment,
      observed,
      lastByte,
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

    const sdkSpec = defineRun("/bin/sh", {
      args: [
        "-c",
        "trap '' INT; trap 'exit 0' TERM; printf 'SDK_READY\\n'; while :; do read -r _; done",
      ],
    });
    const sdkStartKey = createOperationKey("sdk-retry-safe-start");
    const sdkRun = await reconnectedClient.start(sdkSpec, sdkStartKey);
    const retriedSdkRun = await reconnectedClient.start(sdkSpec, sdkStartKey);
    assert.equal(retriedSdkRun.id, sdkRun.id);
    assert.equal(retriedSdkRun.pid, sdkRun.pid);
    await assert.rejects(
      reconnectedClient.start(
        defineRun("/bin/sh", { args: ["-c", "exit 9"] }),
        sdkStartKey,
      ),
      (error: unknown) =>
        error instanceof CtxmuxProtocolError &&
        error.code === "creation_conflict",
    );

    const sdkForkKey = createOperationKey("sdk-retry-safe-fork");
    const sdkChild = await reconnectedClient.fork(
      sdkRun.id,
      { type: "level_a" },
      sdkForkKey,
    );
    const retriedSdkChild = await reconnectedClient.fork(
      sdkRun.id,
      { type: "level_a" },
      sdkForkKey,
    );
    assert.equal(retriedSdkChild.id, sdkChild.id);
    assert.equal(retriedSdkChild.pid, sdkChild.pid);
    const sdkChildAttachment = await reconnectedClient.attach(sdkChild.id);
    await waitForOutput(
      sdkChildAttachment,
      replayBytes(sdkChildAttachment.snapshot.replay.chunks),
      sdkChildAttachment.snapshot.replay.latest_output_bytes,
      "SDK_READY",
    );
    await sdkChildAttachment.detach();
    await waitForNoAttachments(reconnectedClient, sdkChild.id);
    const sdkAttachment = await reconnectedClient.attach(sdkRun.id);
    await waitForOutput(
      sdkAttachment,
      replayBytes(sdkAttachment.snapshot.replay.chunks),
      sdkAttachment.snapshot.replay.latest_output_bytes,
      "SDK_READY",
    );
    assert.deepEqual(
      await step("interrupt SDK-created Run", sdkAttachment.interrupt()),
      {
        commandId: 1,
        receipt: { type: "signal", signal: "interrupt" },
      },
    );
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
    assert.deepEqual(
      (
        await step(
          "stop SDK-forked Run",
          reconnectedClient.stop(
            await reconnectedClient.prepareStop(sdkChild.id),
          ),
        )
      ).receipt,
      { type: "stop", disposition: "graceful" },
    );
    assert.deepEqual(
      (
        await step(
          "stop SDK-created Run",
          reconnectedClient.stop(
            await reconnectedClient.prepareStop(sdkRun.id),
          ),
        )
      ).receipt,
      { type: "stop", disposition: "graceful" },
    );
  },
);

test(
  "a host-owned Provider materializes public Level B without a fallback",
  { timeout: 15_000 },
  async (context) => {
    const { client, directory } = await startTestDaemon(context);
    const executable = join(directory, "synthetic-provider.mjs");
    await writeFile(
      executable,
      `#!/usr/bin/env node
const args = process.argv.slice(2);
console.log(JSON.stringify({ type: args[0], argv: args }));
setInterval(() => {}, 1_000);
`,
    );
    await chmod(executable, 0o755);
    const provider = syntheticProviderIntegration();
    const registered = registerIntegration(client, provider);
    const prompt = "review 'quoted'; $(touch never)\nthen explain";
    const parentRecipe = defineRun(executable, {
      args: ["source", prompt],
      cwd: directory,
      declaredInputs: [{ kind: "workspace", reference: directory }],
    });
    const startOptions = {
      detection: { executable },
      operationKey: createOperationKey("synthetic-provider-start"),
    } as const;
    const parent = await registered.start(
      { recipe: parentRecipe },
      startOptions,
    );
    assert.equal(
      (await registered.start({ recipe: parentRecipe }, startOptions)).id,
      parent.id,
    );
    assert.deepEqual(parent.spec, parentRecipe);
    const parentPid = parent.pid;
    assert.notEqual(parentPid, null);

    const exactArgvAttachment = await client.attach(parent.id);
    await waitForOutput(
      exactArgvAttachment,
      replayBytes(exactArgvAttachment.snapshot.replay.chunks),
      exactArgvAttachment.snapshot.replay.latest_output_bytes,
      JSON.stringify({ type: "source", argv: ["source", prompt] }),
    );
    exactArgvAttachment.close();
    const parentAttachment = await client.attach(parent.id);
    const parentObserver = registered.createObserver(parent);
    const provenance = parentObserver.observe(
      await nextOutputEvent(parentAttachment),
    )[0];
    assert.notEqual(provenance, undefined);

    const unrelated = await client.start(
      defineRun("/bin/sh", { args: ["-c", "printf 'unrelated\\n'; sleep 30"] }),
    );
    const unrelatedAttachment = await client.attach(unrelated.id);
    const unrelatedOutput = await nextOutputEvent(unrelatedAttachment);
    assert.throws(
      () => parentObserver.observe(unrelatedOutput),
      (error: unknown) =>
        error instanceof IntegrationProvenanceError &&
        error.reason === "wrong_source",
      "another Run's public event must not authenticate the parent",
    );

    const continuation = "compare the two candidates";
    const contextReference = "synthetic-context:parent";
    const artifactReference = "artifact://review-plan.json";
    const childRecipe = defineRun(executable, {
      args: ["continue", continuation],
      cwd: directory,
      declaredInputs: [
        { kind: "workspace", reference: directory },
        { kind: "artifact", reference: artifactReference },
        { kind: "context", reference: contextReference },
      ],
    });
    const validConfig = {
      provenance: provenance!,
      recipe: childRecipe,
    };
    const levelBOptions = {
      detection: { executable },
      operationKey: createOperationKey("synthetic-provider-level-b"),
    } as const;
    const beforeRejectedForks = (await client.list()).map(({ id }) => id);

    await assert.rejects(
      registered.forkLevelB(unrelated, validConfig, {
        detection: { executable },
      }),
      (error: unknown) =>
        error instanceof IntegrationProvenanceError &&
        error.reason === "wrong_source",
    );
    await assert.rejects(
      registered.forkLevelB(
        parent,
        { ...validConfig, provenance: { ...provenance! } },
        { detection: { executable } },
      ),
      (error: unknown) =>
        error instanceof IntegrationProvenanceError &&
        error.reason === "missing",
    );
    await assert.rejects(
      registered.forkLevelB(parent, validConfig),
      (error: unknown) =>
        error instanceof IntegrationUnavailableError &&
        error.detection.reason === "missing_capability",
    );
    const withoutLevelBCapability = {
      ...provider,
      id: "synthetic-provider-without-level-b",
      async detect() {
        return {
          status: "available" as const,
          executable,
          version: "test-owned",
          capabilities: ["semantic_events" as const],
        };
      },
    };
    await assert.rejects(
      registerIntegration(client, withoutLevelBCapability).forkLevelB(
        parent,
        validConfig,
        { detection: { executable } },
      ),
      (error: unknown) =>
        error instanceof IntegrationCapabilityError &&
        error.capability === "level_b_fork",
    );
    const { levelBForkProvenance: _provenance, ...withoutProvenance } =
      provider;
    await assert.rejects(
      registerIntegration(client, withoutProvenance).forkLevelB(
        parent,
        validConfig,
        { detection: { executable } },
      ),
      (error: unknown) =>
        error instanceof IntegrationProvenanceError &&
        error.reason === "missing",
    );
    const { planLevelBFork: _plan, ...withoutMaterializer } = provider;
    await assert.rejects(
      registerIntegration(client, withoutMaterializer).forkLevelB(
        parent,
        validConfig,
        { detection: { executable } },
      ),
      (error: unknown) =>
        error instanceof IntegrationMaterializationError &&
        error.reason === "missing_planner",
    );
    assert.deepEqual(
      (await client.list()).map(({ id }) => id),
      beforeRejectedForks,
      "every rejected Level B request must create no Run, including Level A",
    );

    parentAttachment.close();
    unrelatedAttachment.close();
    await client.stop(await client.prepareStop(unrelated.id));
    const rawParent = await waitForNoAttachments(client, parent.id);
    assert.equal(rawParent.pid, parentPid);
    assert.equal(rawParent.state.type, "running");

    const child = await registered.forkLevelB(
      parent,
      validConfig,
      levelBOptions,
    );
    const retriedChild = await registered.forkLevelB(
      parent,
      validConfig,
      levelBOptions,
    );
    assert.equal(retriedChild.id, child.id);
    assert.notEqual(child.id, parent.id);
    assert.notEqual(child.pid, parentPid);
    assert.deepEqual(child.lineage, {
      parent: parent.id,
      fidelity: "level_b",
    });
    assert.deepEqual(child.spec, childRecipe);
    const childAttachment = await client.attach(child.id);
    await waitForOutput(
      childAttachment,
      replayBytes(childAttachment.snapshot.replay.chunks),
      childAttachment.snapshot.replay.latest_output_bytes,
      JSON.stringify({
        type: "continue",
        argv: ["continue", continuation],
      }),
    );
    childAttachment.close();
    assert.equal((await client.status(parent.id)).state.type, "running");
    await client.stop(await client.prepareStop(parent.id));
    await client.stop(await client.prepareStop(child.id));
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
      signal: false,
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
      attachment.snapshot.replay.latest_output_bytes,
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
      async () => client.stop(await client.prepareStop(run.id)),
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

async function sendStopWithoutReadingResponse(
  socketPath: string,
  operation: RecoverableStopOperation,
): Promise<void> {
  const socket = createConnection(socketPath);
  await once(socket, "connect");
  const lines = createInterface({ input: socket, crlfDelay: Infinity });
  const iterator = lines[Symbol.asyncIterator]();
  socket.write(
    `${JSON.stringify({ type: "hello", hello: { protocol: PROTOCOL_VERSION } })}\n`,
  );
  const hello = await iterator.next();
  assert.equal(hello.done, false);
  assert.equal(
    (
      JSON.parse(hello.value ?? "null") as {
        readonly runtime?: { readonly daemonInstanceId?: unknown };
      }
    ).runtime?.daemonInstanceId,
    operation.daemonInstance,
  );
  await new Promise<void>((resolve, reject) => {
    socket.write(
      `${JSON.stringify({
        type: "request",
        request: {
          type: "stop",
          operation: {
            daemon_instance: operation.daemonInstance,
            operation_key: operation.operationKey,
            id: operation.runId,
          },
        },
      })}\n`,
      (error) => (error == null ? resolve() : reject(error)),
    );
  });
  lines.close();
  socket.destroy();
}

async function waitForMarker(path: string, expected: string): Promise<void> {
  const deadline = Date.now() + 5_000;
  let observed = "";
  while (Date.now() <= deadline) {
    try {
      observed = await readFile(path, "utf8");
      if (observed === expected) {
        return;
      }
    } catch (error) {
      if (
        typeof error !== "object" ||
        error === null ||
        !("code" in error) ||
        error.code !== "ENOENT"
      ) {
        throw error;
      }
    }
    await delay(20);
  }
  throw new Error(`timed out waiting for Stop marker; observed ${observed}`);
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
  initialByte: number,
  expected: string,
): Promise<{ observed: Uint8Array; lastByte: number }> {
  let observed = initial;
  let lastByte = initialByte;
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
      assert.equal(event.chunk.start_byte, lastByte);
      lastByte = event.chunk.end_byte;
      observed = append(observed, event.chunk.data);
    } else if (event?.type === "gap") {
      throw new Error(`unexpected output gap at ${event.latest_output_bytes}`);
    } else if (event?.type === "exited") {
      throw new Error(
        `Run exited before ${expected}: ${JSON.stringify(event.state)}; received ${JSON.stringify(text(observed))}`,
      );
    }
  }
  return { observed, lastByte };
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
      throw new Error(`unexpected output gap at ${event.latest_output_bytes}`);
    }
  }
  throw new Error("timed out waiting for Run exit");
}

interface SyntheticProviderReceipt extends IntegrationSemanticEvent {
  readonly integrationId: "synthetic-provider";
  readonly name: "source.observed";
  readonly sourceToken: string;
}

interface SyntheticProviderForkConfig {
  readonly provenance: SyntheticProviderReceipt;
  readonly recipe: RunSpec;
}

function syntheticProviderIntegration(): Integration<
  { readonly recipe: RunSpec },
  SyntheticProviderForkConfig,
  SyntheticProviderReceipt
> {
  return {
    id: "synthetic-provider",
    apiVersion: INTEGRATION_API_VERSION,
    async detect(options) {
      if (options?.executable === undefined) {
        return {
          status: "unavailable",
          executable: "synthetic-provider",
          reason: "missing_capability",
        };
      }
      return {
        status: "available",
        executable: options.executable,
        version: "test-owned",
        capabilities: ["semantic_events", "level_b_fork"],
      };
    },
    planLaunch(config) {
      return config.recipe;
    },
    planLevelBFork(_parent, config) {
      return { type: "level_b", spec: config.recipe };
    },
    levelBForkProvenance(config) {
      return config.provenance;
    },
    createObserver(): IntegrationObserver<SyntheticProviderReceipt> {
      return {
        observe(event) {
          if (event.type !== "output") {
            return [];
          }
          return [
            {
              integrationId: "synthetic-provider",
              name: "source.observed",
              sourceToken: `${String(event.chunk.start_byte)}:${String(event.chunk.end_byte)}`,
              data: {},
            },
          ];
        },
      };
    },
  };
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
      throw new Error(`unexpected output gap at ${event.latest_output_bytes}`);
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
  chunks: readonly { readonly data: Uint8Array }[],
): Uint8Array {
  return append(
    new Uint8Array(),
    Buffer.concat(chunks.map((chunk) => Buffer.from(chunk.data))),
  );
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
