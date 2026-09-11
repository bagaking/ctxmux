import assert from "node:assert/strict";
import { spawn, execFileSync } from "node:child_process";
import { once } from "node:events";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import test from "node:test";
import { CtxmuxClient, type RunId } from "../dist/index.js";

const testDaemon = process.env.CTXMUX_STORAGE_TEST_DAEMON;
const cli = process.env.CTXMUX_BIN;
if (!testDaemon || !cli)
  throw new Error("select the exact candidate test daemon and CLI binaries");

test("SDK and CLI consume storage fault facts from two real Native Runs", async () => {
  const directory = await mkdtemp(join(tmpdir(), "ctxmux-storage-public-"));
  const daemon = spawn(
    testDaemon,
    ["--exact", "tests::storage_observation_fault_subprocess", "--nocapture"],
    {
      env: { ...process.env, CTXMUX_STORAGE_TEST_DIR: directory },
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
  let logs = "";
  daemon.stdout.on("data", (part) => {
    logs += String(part);
  });
  daemon.stderr.on("data", (part) => {
    logs += String(part);
  });
  const exited = once(daemon, "exit");
  try {
    let receipt: { socketPath: string; runIds: RunId[] } | undefined;
    for (let attempt = 0; attempt < 500; attempt++) {
      try {
        receipt = JSON.parse(
          await readFile(join(directory, "ready.json"), "utf8"),
        );
        break;
      } catch {
        if (daemon.exitCode !== null)
          throw new Error(`test daemon ended before its receipt: ${logs}`);
      }
      await delay(10);
    }
    assert.ok(receipt, `real daemon published both Run identities: ${logs}`);
    assert.equal(receipt.runIds.length, 2);
    const client = new CtxmuxClient({ socketPath: receipt.socketPath });
    const rows = [];
    for (const id of receipt.runIds) {
      const before = await client.status(id);
      assert.equal(before.state.type, "running");
      assert.ok(before.pid);
      const result = await client.observeStorage(id);
      assert.equal(result.observation.run_id, id);
      assert.equal(
        result.runtime.capabilities["services.storage_observation"],
        1,
      );
      assert.match(
        result.observation.persistence?.first_failure ?? "",
        /injected failure after durable Run creation commit/u,
      );
      assert.equal(result.observation.persistence?.actor_stopped, false);
      const fromCli = JSON.parse(
        execFileSync(cli, ["--socket", receipt.socketPath, "storage", id], {
          encoding: "utf8",
        }),
      );
      assert.deepEqual(fromCli.runtime, result.runtime);
      assert.equal(fromCli.observation.run_id, id);
      assert.equal(
        fromCli.observation.persistence.first_failure,
        result.observation.persistence?.first_failure,
      );
      assert.equal(
        fromCli.observation.persistence.committed_output_bytes,
        result.observation.persistence?.committed_output_bytes,
      );
      await client.input(id, "public-consumer-after-fault\n");
      let after = before;
      for (
        let attempt = 0;
        attempt < 500 &&
        after.latest_output_bytes <= before.latest_output_bytes;
        attempt++
      ) {
        await delay(10);
        after = await client.status(id);
      }
      assert.equal(after.pid, before.pid);
      assert.equal(after.state.type, "running");
      assert.ok(
        after.latest_output_bytes > before.latest_output_bytes,
        "live public input still reaches the original PTY",
      );
      rows.push({
        id,
        pid: after.pid,
        runtime: result.runtime,
        observation: result.observation,
      });
    }
    const terminal = await client.attachTerminal(receipt.runIds[0]!);
    assert.equal(terminal.snapshot.terminal.type, "basic_vt");
    await terminal.detach();
    const genuineGap = await client.attachTerminal(receipt.runIds[1]!);
    assert.deepEqual(genuineGap.snapshot.terminal, {
      type: "unknown",
      reason: "source_gap",
    });
    await genuineGap.detach();
    if (process.env.CTXMUX_STORAGE_TEST_RECEIPT)
      await writeFile(
        process.env.CTXMUX_STORAGE_TEST_RECEIPT,
        JSON.stringify({ testDaemon, cli, rows }, null, 2),
      );
    await writeFile(
      join(directory, "release"),
      "release isolated fixture only\n",
    );
    const [code, signal] = await exited;
    assert.equal(code, 0, `source-bound daemon completed: ${signal} ${logs}`);
  } finally {
    if (daemon.exitCode === null) {
      daemon.kill("SIGTERM");
      await exited;
    }
    await rm(directory, { recursive: true, force: true });
  }
});
