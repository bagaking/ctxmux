import assert from "node:assert/strict";
import { spawn, execFileSync, type ChildProcess } from "node:child_process";
import { once } from "node:events";
import { mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import test from "node:test";
import {
  CtxmuxClient,
  CtxmuxProtocolError,
  type RunInfo,
  type RunSpec,
} from "../dist/index.js";

const fixture = process.env.CTXMUX_CREATION_TEST_DAEMON;
const daemonBinary = process.env.CTXMUXD_BIN;
const cli = process.env.CTXMUX_BIN;
if (!fixture || !daemonBinary || !cli)
  throw new Error("select the candidate Native fixture, daemon and CLI");

async function until<T>(read: () => Promise<T | false>): Promise<T> {
  const deadline = Date.now() + 12_000;
  while (Date.now() < deadline) {
    const value = await read();
    if (value !== false) return value;
    await delay(10);
  }
  throw new Error("original recovery did not settle");
}

async function jsonWhenPresent(path: string): Promise<any | false> {
  try {
    return JSON.parse(await readFile(path, "utf8"));
  } catch (error) {
    if ((error as { code?: string }).code === "ENOENT") return false;
    throw error;
  }
}

async function replay(client: CtxmuxClient, id: string): Promise<Buffer> {
  const view = await client.attach(id);
  try {
    assert.equal(view.snapshot.replay.truncated, false);
    let cursor = 0;
    for (const chunk of view.snapshot.replay.chunks) {
      assert.equal(chunk.start_byte, cursor);
      cursor += chunk.data.length;
      assert.equal(chunk.end_byte, cursor);
    }
    return Buffer.concat(
      view.snapshot.replay.chunks.map((chunk) => Buffer.from(chunk.data)),
    );
  } finally {
    view.close();
  }
}

async function retained(client: CtxmuxClient, key: string): Promise<RunInfo> {
  return until(async () => {
    try {
      const result = await client.observeCreation(key);
      assert.equal(
        result.observation.outcome,
        "retained",
        "unknown is never rewritten as absent",
      );
      return result.observation.outcome === "retained"
        ? result.observation.run
        : false;
    } catch (error) {
      if (
        error instanceof CtxmuxProtocolError &&
        error.code === "backend_unavailable"
      )
        return false;
      throw error;
    }
  });
}

for (const committed of [false, true]) {
  for (const naturalExit of [false, true]) {
    test(`original ${committed ? "New" : "Old"} transaction recovers ${naturalExit ? "ended" : "live"} child through independent readers and cold process`, async () => {
      const directory = await mkdtemp(
        join(tmpdir(), "ctxmux-original-recovery-"),
      );
      const marker = join(directory, "physical-spawns");
      const receipt: Record<string, unknown> = {
        directory,
        committed,
        naturalExit,
        status: "incomplete",
      };
      const owner = spawn(
        fixture,
        [
          "--exact",
          "tests::creation_recovery::public_recovery_subprocess",
          "--nocapture",
        ],
        {
          env: {
            ...process.env,
            CTXMUX_CREATION_RECOVERY_DIR: directory,
            ...(!committed ? { CTXMUX_CREATION_RECOVERY_OLD: "1" } : {}),
          },
          stdio: ["ignore", "pipe", "pipe"],
        },
      );
      let logs = "";
      owner.stdout.on("data", (part) => {
        logs += String(part);
      });
      owner.stderr.on("data", (part) => {
        logs += String(part);
      });
      const ownerExit = once(owner, "exit");
      let cold: ChildProcess | undefined;
      let ownerDone = false;
      try {
        const ready = await until(async () => {
          if (owner.exitCode !== null) throw new Error(logs);
          return jsonWhenPresent(join(directory, "ready.json"));
        });
        const client = new CtxmuxClient({ socketPath: ready.socketPath });
        const runtime = await client.runtimeInfo();
        const key = "original-service-operation";
        const spec: RunSpec = {
          program: "/bin/sh",
          args: [
            "-c",
            `stty -echo -onlcr; printf '%s\\n' "$$" >> "$MARKER"; printf 'original\\n'; ${naturalExit ? "exit 7" : "exec cat"}`,
          ],
          cwd: directory,
          env: { MARKER: marker },
          initial_size: { cols: 80, rows: 24 },
          declared_inputs: [],
        };
        // Both Start and a caller-materialized Fork exercise this generic owner.
        const operation = naturalExit
          ? client.fork(ready.parent.id, { type: "level_b", spec }, key)
          : client.start(spec, key);
        await assert.rejects(operation, (error: unknown) => {
          assert.ok(error instanceof CtxmuxProtocolError);
          assert.deepEqual(error.creationFailure, {
            operation_key: key,
            disposition: "unknown",
          });
          assert.equal(error.storageFailure?.transaction_outcome, "unknown");
          return true;
        });
        const pid = Number(
          await until(async () => {
            try {
              return (await readFile(marker, "utf8")).trim() || false;
            } catch (error) {
              if ((error as { code?: string }).code === "ENOENT") return false;
              throw error;
            }
          }),
        );
        for (let check = 0; check < 3; check++) {
          await assert.rejects(
            client.observeCreation(key),
            (error: unknown) =>
              error instanceof CtxmuxProtocolError &&
              error.code === "backend_unavailable",
          );
          await delay(100);
        }
        await client.input(ready.parent.id, "healthy\n");
        await until(
          async () =>
            (await replay(client, ready.parent.id)).toString() ===
            "ready\nhealthy\n",
        );
        assert.equal(
          (await client.status(ready.parent.id)).pid,
          ready.parent.pid,
        );
        if (!naturalExit) process.kill(pid, 0);
        await writeFile(
          join(directory, "release"),
          "original storage is observable",
        );
        const released = await until(() =>
          jsonWhenPresent(join(directory, "released.json")),
        );
        assert.ok(released.probeAttempts >= 2);
        const original = await retained(client, key);
        assert.equal(original.pid, pid);
        if (naturalExit) {
          assert.equal(original.native_service?.input.phase.type, "closed");
          const terminal = await until(async () => {
            const state = await client.status(original.id);
            return state.state.type === "exited" ? state : false;
          });
          assert.deepEqual(terminal.state, {
            type: "exited",
            code: 7,
            signal: null,
          });
        } else {
          assert.equal(original.state.type, "running");
          await client.input(original.id, "continued\n");
        }
        const expected = naturalExit ? "original\n" : "original\ncontinued\n";
        await until(
          async () =>
            (await replay(client, original.id)).toString() === expected,
        );
        const fromCli = JSON.parse(
          execFileSync(cli, ["--socket", ready.socketPath, "creation", key], {
            encoding: "utf8",
          }),
        );
        const freshNode = JSON.parse(
          execFileSync(
            process.execPath,
            [
              "--input-type=module",
              "-e",
              `const {CtxmuxClient}=await import(process.argv[1]); const value=await new CtxmuxClient({socketPath:process.argv[2]}).observeCreation(process.argv[3]); process.stdout.write(JSON.stringify(value));`,
              new URL("../dist/index.js", import.meta.url).href,
              ready.socketPath,
              key,
            ],
            { encoding: "utf8" },
          ),
        );
        for (const read of [fromCli, freshNode]) {
          assert.equal(read.observation.run.id, original.id);
          assert.equal(read.observation.run.pid, pid);
          assert.equal(read.runtime.runtimeId, runtime.runtimeId);
        }
        assert.equal(
          await readFile(marker, "utf8"),
          `${pid}\n`,
          "one physical spawn across every observation",
        );
        if (!naturalExit)
          await client.stop(await client.prepareStop(original.id));
        await writeFile(
          join(directory, "finish"),
          "close exact owner after terminal persistence",
        );
        const [code] = await ownerExit;
        ownerDone = true;
        assert.equal(code, 0, logs);
        const coldSocket = join(directory, "cold.sock");
        cold = spawn(
          daemonBinary,
          ["--socket", coldSocket, "--state-dir", join(directory, "state")],
          { stdio: ["ignore", "ignore", "pipe"] },
        );
        let coldErrors = "";
        cold.stderr?.on("data", (part) => {
          coldErrors += String(part);
        });
        const coldExit = once(cold, "exit");
        const coldClient = new CtxmuxClient({ socketPath: coldSocket });
        await until(async () => {
          if (cold!.exitCode !== null) throw new Error(coldErrors);
          try {
            return await coldClient.runtimeInfo();
          } catch {
            return false;
          }
        });
        const after = await coldClient.observeCreation(key);
        assert.equal(after.runtime.runtimeId, runtime.runtimeId);
        assert.notEqual(
          after.runtime.daemonInstanceId,
          runtime.daemonInstanceId,
        );
        assert.equal(after.observation.outcome, "retained");
        if (after.observation.outcome !== "retained")
          throw new Error("original durable identity required");
        assert.equal(after.observation.run.id, original.id);
        assert.equal(after.observation.run.pid, pid);
        assert.equal(after.observation.run.state.type, "exited");
        assert.equal(
          (await replay(coldClient, original.id)).toString(),
          expected,
        );
        cold.kill("SIGINT");
        assert.equal((await coldExit)[0], 0, coldErrors);
        cold = undefined;
        Object.assign(receipt, {
          status: "passed",
          runtime,
          original,
          after,
          pid,
          probeAttempts: released.probeAttempts,
        });
      } finally {
        if (!ownerDone) {
          await writeFile(
            join(directory, "release"),
            "release owned fault during cleanup",
          );
          await writeFile(join(directory, "finish"), "finish owned fixture");
          await ownerExit;
        }
        if (cold?.exitCode === null) {
          const exited = once(cold, "exit");
          cold.kill("SIGINT");
          await exited;
        }
        await writeFile(
          join(directory, "evidence.json"),
          JSON.stringify({ ...receipt, logs }, null, 2),
        );
      }
    });
  }
}
