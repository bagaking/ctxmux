import assert from "node:assert/strict";
import { spawn, execFileSync, type ChildProcess } from "node:child_process";
import { once } from "node:events";
import { mkdtemp, mkdir, readFile, writeFile } from "node:fs/promises";
import { createConnection } from "node:net";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { createHash } from "node:crypto";
import test from "node:test";
import {
  CtxmuxClient,
  createOperationKey,
  inputOperationKey,
  type RunSpec,
} from "../dist/index.js";

const daemonPath = process.env.CTXMUXD_BIN,
  cliPath = process.env.CTXMUX_BIN;
if (!daemonPath || !cliPath)
  throw Error("select the exact compiled CTXMUXD_BIN and CTXMUX_BIN");
const sha = (bytes: Uint8Array | string) =>
  createHash("sha256").update(bytes).digest("hex");
async function wait<T>(read: () => Promise<T | false>): Promise<T> {
  const deadline = Date.now() + 10_000;
  while (Date.now() < deadline) {
    const value = await read();
    if (value !== false) return value;
    await delay(10);
  }
  throw Error("private observation did not settle");
}

test("fresh SDK and CLI recover original creation without replay, then observe retained history across daemon restart", async () => {
  const directory = await mkdtemp(join(tmpdir(), "ctxmux-creation-public-"));
  const socketPath = join(directory, "r.sock"),
    state = join(directory, "state"),
    marker = join(directory, "spawn-marker");
  await mkdir(state, { mode: 0o700 });
  let daemon: ChildProcess | undefined;
  let cleanupClient: CtxmuxClient | undefined, liveRunId: string | undefined;
  const receipt: Record<string, any> = {
    directory,
    status: "incomplete",
    cleanup: {},
    binaries: {
      daemon: sha(await readFile(daemonPath)),
      cli: sha(await readFile(cliPath)),
      sdk: sha(await readFile(new URL("../dist/client.js", import.meta.url))),
    },
  };
  async function startDaemon() {
    daemon = spawn(
      daemonPath!,
      ["--socket", socketPath, "--state-dir", state],
      { stdio: ["ignore", "ignore", "pipe"] },
    );
    let errors = "";
    daemon.stderr!.on("data", (part) => {
      errors += String(part);
    });
    const client = new CtxmuxClient({ socketPath });
    await wait(async () => {
      if (daemon!.exitCode !== null) throw Error(errors);
      try {
        return await client.runtimeInfo();
      } catch {
        return false;
      }
    });
    return client;
  }
  async function stopDaemon() {
    if (!daemon || daemon.exitCode !== null) return;
    const ended = once(daemon, "exit");
    daemon.kill("SIGINT");
    const [code, signal] = await ended;
    assert.equal(code, 0);
    assert.equal(signal, null);
  }
  try {
    const client = await startDaemon(),
      original = await client.runtimeInfo();
    const key = createOperationKey("lost-original-response"),
      neighbor = createOperationKey("naturally-ended-neighbor");
    const spec: RunSpec = {
      program: "/bin/sh",
      args: [
        "-c",
        'printf started >> "$SPAWN_MARKER"; stty -echo -onlcr; while IFS= read -r line; do printf "%s\\n" "$line"; done',
      ],
      cwd: directory,
      env: { SPAWN_MARKER: marker },
      initial_size: { cols: 80, rows: 24 },
      declared_inputs: [],
    };
    // The old wire deliberately never consumes its Start response. The real
    // child marker proves acceptance before that disposable reader disappears.
    const old = createConnection(socketPath);
    await once(old, "connect");
    old.on("error", () => {});
    let lines = "",
      dispatched = false;
    old.on("data", (part) => {
      lines += part.toString();
      while (lines.includes("\n")) {
        const index = lines.indexOf("\n"),
          value = JSON.parse(lines.slice(0, index));
        lines = lines.slice(index + 1);
        if (value.type === "hello" && !dispatched) {
          dispatched = true;
          old.write(
            JSON.stringify({
              type: "request",
              request: { type: "start", operation_key: key, spec },
            }) + "\n",
          );
        }
      }
    });
    old.write(
      JSON.stringify({
        type: "hello",
        hello: { protocol: original.protocolGeneration },
      }) + "\n",
    );
    await wait(async () => {
      try {
        return (await readFile(marker, "utf8")) === "started" ? true : false;
      } catch {
        return false;
      }
    });
    old.destroy();
    const freshReader = JSON.parse(
      execFileSync(
        process.execPath,
        [
          "--input-type=module",
          "-e",
          "const {CtxmuxClient,createOperationKey}=await import(process.argv[1]); const observed=await new CtxmuxClient({socketPath:process.argv[2]}).observeCreation(createOperationKey(process.argv[3])); console.log(JSON.stringify({pid:process.pid,observed}));",
          new URL("../dist/index.js", import.meta.url).href,
          socketPath,
          key,
        ],
        { encoding: "utf8" },
      ),
    ) as {
      pid: number;
      observed: Awaited<ReturnType<CtxmuxClient["observeCreation"]>>;
    };
    assert.notEqual(freshReader.pid, process.pid);
    const observed = freshReader.observed;
    receipt.freshReaderPid = freshReader.pid;
    assert.equal(observed.runtime.daemonInstanceId, original.daemonInstanceId);
    assert.equal(observed.observation.outcome, "retained");
    if (observed.observation.outcome !== "retained")
      throw Error("retained original required");
    const run = observed.observation.run;
    cleanupClient = client;
    liveRunId = run.id;
    receipt.observed = observed;
    assert.ok(run.pid);
    assert.equal(run.state.type, "running");
    receipt.stage = "fresh-cli-query";
    const fromCli = JSON.parse(
      execFileSync(cliPath, ["--socket", socketPath, "creation", key], {
        encoding: "utf8",
      }),
    );
    assert.equal(fromCli.observation.run.id, run.id);
    assert.equal(fromCli.observation.run.pid, run.pid);
    assert.deepEqual(fromCli.runtime, observed.runtime);
    await assert.rejects(
      client.start(
        { ...spec, env: { ...spec.env, CHANGED: "different" } },
        key,
      ),
      (error) => (error as { code?: string }).code === "creation_conflict",
    );
    assert.equal(await readFile(marker, "utf8"), "started");
    receipt.stage = "original-recoverable-input";
    const expected = "ordered-private-echo",
      ack = await client.recoverableInput({
        runId: run.id,
        daemonInstance: original.daemonInstanceId,
        operationKey: inputOperationKey("original-echo"),
        expectedByte: run.applied_input_bytes!,
        data: expected + "\n",
      });
    receipt.stage = "ordered-replay";
    const echoed = await wait(async () => {
      const attachment = await client.attach(run.id);
      try {
        assert.equal(attachment.snapshot.replay.truncated, false);
        const bytes = Buffer.concat(
          attachment.snapshot.replay.chunks.map((chunk) =>
            Buffer.from(chunk.data),
          ),
        );
        return bytes.includes(Buffer.from(expected + "\n"))
          ? { bytes: bytes.length, sha256: sha(bytes) }
          : false;
      } finally {
        attachment.close();
      }
    });
    receipt.stage = "natural-terminal-neighbor";
    const ended = await client.start(
      { ...spec, args: ["-c", "printf retained-history"], env: {} },
      neighbor,
    );
    await wait(async () => {
      const row = await client.status(ended.id);
      return row.state.type === "exited" && (row.durable_output_bytes ?? 0) > 0
        ? row
        : false;
    });
    const neighborObservation = await client.observeCreation(neighbor);
    assert.equal(neighborObservation.observation.outcome, "retained");
    if (neighborObservation.observation.outcome === "retained")
      assert.equal(neighborObservation.observation.run.id, ended.id);
    await client.stop(await client.prepareStop(run.id));
    await stopDaemon();
    receipt.stage = "cold-runtime-query";
    const restarted = await startDaemon(),
      after = await restarted.observeCreation(neighbor);
    assert.equal(after.runtime.runtimeId, original.runtimeId);
    assert.notEqual(after.runtime.daemonInstanceId, original.daemonInstanceId);
    assert.equal(after.observation.outcome, "retained");
    if (after.observation.outcome !== "retained")
      throw Error("retained history required");
    assert.equal(after.observation.run.id, ended.id);
    assert.equal(after.observation.run.state.type, "exited");
    const history = await restarted.attach(ended.id);
    const bytes = Buffer.concat(
      history.snapshot.replay.chunks.map((chunk) => Buffer.from(chunk.data)),
    );
    history.close();
    assert.equal(bytes.toString(), "retained-history");
    receipt.stage = "retention-removal";
    await restarted.remove(ended.id);
    assert.deepEqual((await restarted.observeCreation(neighbor)).observation, {
      outcome: "not_retained",
      operation_key: neighbor,
    });
    const missingSocket = join(directory, "never-created.sock");
    assert.throws(() =>
      execFileSync(cliPath, ["--socket", missingSocket, "creation", key], {
        stdio: "pipe",
      }),
    );
    await assert.rejects(readFile(missingSocket), { code: "ENOENT" });
    Object.assign(receipt, {
      status: "passed",
      original,
      observed,
      fromCli,
      ack,
      echoed,
      after,
      removed: { outcome: "not_retained", operation_key: neighbor },
      markerSha256: sha(await readFile(marker)),
    });
  } catch (error) {
    receipt.error = {
      name: (error as Error).name,
      message: (error as Error).message,
    };
    throw error;
  } finally {
    receipt.cleanup.errors = [];
    if (daemon?.exitCode === null && cleanupClient && liveRunId) {
      try {
        if ((await cleanupClient.status(liveRunId)).state.type === "running")
          await cleanupClient.stop(await cleanupClient.prepareStop(liveRunId));
      } catch (error) {
        receipt.cleanup.errors.push(String(error));
      }
    }
    try {
      await stopDaemon();
    } catch (error) {
      receipt.cleanup.errors.push(String(error));
    }
    receipt.cleanup.daemonExited = daemon?.exitCode === 0;
    const output =
      process.env.CTXMUX_CREATION_RECEIPT ?? join(directory, "receipt.json");
    await writeFile(output, JSON.stringify(receipt, null, 2) + "\n", {
      mode: 0o600,
    });
    console.log(
      JSON.stringify({
        receipt: resolve(output),
        status: receipt.status,
        cleanup: receipt.cleanup,
      }),
    );
  }
});
