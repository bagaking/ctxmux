import assert from "node:assert/strict";
import { spawn, spawnSync, type SpawnSyncReturns } from "node:child_process";
import { once } from "node:events";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import test from "node:test";
import {
  CtxmuxClient,
  CtxmuxProtocolError,
  type RunSpec,
} from "../dist/index.js";

const daemonBinary = process.env.CTXMUX_CREATION_TEST_DAEMON;
const cli = process.env.CTXMUX_BIN;
if (!daemonBinary || !cli)
  throw new Error("select the candidate Rust test executable and CLI");

for (const beforeCommit of [false, true]) {
  test(`real creation failure preserves effects ${beforeCommit ? "before" : "after"} COMMIT`, async () => {
    const directory = await mkdtemp(join(tmpdir(), "ctxmux-creation-public-"));
    const daemon = spawn(
      daemonBinary,
      [
        "--exact",
        "tests::creation::creation_failure_public_subprocess",
        "--nocapture",
      ],
      {
        env: {
          ...process.env,
          CTXMUX_CREATION_TEST_DIR: directory,
          ...(beforeCommit ? { CTXMUX_CREATION_BEFORE_COMMIT: "1" } : {}),
        },
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
    let requestFailure: unknown;
    try {
      let socketPath: string | undefined;
      for (let attempt = 0; attempt < 500; attempt++) {
        try {
          socketPath = JSON.parse(
            await readFile(join(directory, "ready.json"), "utf8"),
          ).socketPath;
          break;
        } catch {
          if (daemon.exitCode !== null)
            throw new Error(`fixture ended: ${logs}`);
        }
        await delay(10);
      }
      assert.ok(socketPath, logs);
      const client = new CtxmuxClient({ socketPath });
      const key = "original-creation";
      const spec: RunSpec = {
        program: "/bin/sh",
        args: ["-c", "printf 'one\\n'; cat"],
        env: {},
        cwd: directory,
        initial_size: { cols: 80, rows: 24 },
        declared_inputs: [],
      };
      if (beforeCommit) {
        const rejected: SpawnSyncReturns<string> = spawnSync(
          cli,
          [
            "--socket",
            socketPath,
            "start",
            "--json",
            "--operation-key",
            key,
            "--cwd",
            directory,
            "--",
            spec.program,
            ...spec.args,
          ],
          { encoding: "utf8" },
        );
        assert.equal(rejected.status, 1, rejected.stderr);
        const failure = JSON.parse(rejected.stdout).error;
        assert.equal(failure.code, "persistence");
        assert.deepEqual(failure.creation_failure, {
          operation_key: key,
          disposition: "unknown",
        });
        assert.equal(failure.storage_failure.stage, "start");
        assert.equal(
          failure.storage_failure.transaction_outcome,
          "not_committed",
        );
      } else {
        await assert.rejects(client.start(spec, key), (error: unknown) => {
          assert.ok(error instanceof CtxmuxProtocolError);
          assert.deepEqual(error.creationFailure, {
            operation_key: key,
            disposition: "unknown",
          });
          assert.equal(error.code, "persistence");
          assert.equal(error.storageFailure?.stage, "start");
          assert.equal(error.storageFailure?.transaction_outcome, "committed");
          return true;
        });
      }
      if (!beforeCommit) {
        // The failed reply followed a durable commit. Lookup returns that same
        // real process; the fixture's physical-spawn counter rejects duplicates.
        const original = await client.start(spec, key);
        const again = await client.start(spec, key);
        assert.equal(again.id, original.id);
        assert.equal(again.pid, original.pid);
        assert.equal(original.state.type, "running");
        const observed = await client.observeStorage(original.id);
        assert.ok(observed.observation.persistence?.storage_failure);
        for (const command of [
          [
            "start",
            "--json",
            "--operation-key",
            "refused-start",
            "--",
            "/bin/sh",
            "-c",
            "exit 0",
          ],
          ["fork", "--json", "--operation-key", "refused-fork", original.id],
        ]) {
          const result: SpawnSyncReturns<string> = spawnSync(
            cli,
            ["--socket", socketPath, ...command],
            {
              encoding: "utf8",
            },
          );
          assert.equal(result.status, 1, result.stderr);
          const receipt = JSON.parse(result.stdout);
          assert.equal(
            receipt.error.creation_failure.operation_key,
            command[3],
          );
          assert.equal(
            receipt.error.creation_failure.disposition,
            "not_applied",
          );
          assert.ok(receipt.error.storage_failure);
        }
        await client.input(original.id, "still-the-original\n");
        assert.equal((await client.status(original.id)).pid, original.pid);
      }
    } catch (error) {
      requestFailure = error;
    } finally {
      await writeFile(join(directory, "release"), "done");
      const [code] = await exited;
      if (code !== 0 && requestFailure !== undefined) {
        throw new AggregateError(
          [requestFailure, new Error(logs)],
          `Original request failed: ${String(requestFailure)}; fixture cleanup also failed: ${logs}`,
        );
      }
      assert.equal(code, 0, logs);
      if (requestFailure === undefined)
        await rm(directory, { recursive: true, force: true });
    }
    if (requestFailure !== undefined) throw requestFailure;
  });
}
