import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createConnection, createServer, type Socket } from "node:net";
import test from "node:test";
import {
  CtxmuxClient,
  activateRuntime,
  defineRun,
  type Attachment,
  type RunInfo,
  type RuntimeActivation,
  type RuntimeResourceLimits,
} from "@ctxmux/sdk";

const binary = process.env.CTXMUXD_BIN;
if (binary === undefined)
  throw new Error(
    "CTXMUXD_BIN must bind the compiled private Runtime under test",
  );
const facts: unknown[] = [];

async function privateRuntime(policy: RuntimeResourceLimits = {}) {
  const directory = await mkdtemp(join(tmpdir(), "ctxmux-foreground-"));
  let activation: RuntimeActivation | undefined;
  const runs: RunInfo[] = [];
  try {
    activation = await activateRuntime({
      executable: binary!,
      socketPath: join(directory, "r.sock"),
      stateDir: join(directory, "state"),
      resourceLimits: policy,
      timeoutMs: 5000,
      childDisposition: { mode: "detached", stderr: "pipe" },
    });
    assert.equal(activation.spawned, true);
    assert.ok(activation.childPid !== undefined);
    return {
      activation,
      directory,
      runs,
      async close() {
        const failures: unknown[] = [];
        for (const run of runs) {
          try {
            await activation!.client.stop(
              await activation!.client.prepareStop(run.id),
            );
          } catch (error) {
            failures.push(String(error));
          }
        }
        await activation!.dispose({ shutdown: true });
        await rm(directory, { recursive: true, force: true });
        assert.deepEqual(failures, [], "all owned Runs independently stopped");
      },
    };
  } catch (error) {
    await activation?.dispose({ shutdown: true });
    await rm(directory, { recursive: true, force: true });
    throw error;
  }
}

function outputReader(attachment: Attachment) {
  let cursor = attachment.snapshot.replay.latest_output_bytes;
  let text = Buffer.concat(
    attachment.snapshot.replay.chunks.map((c) => Buffer.from(c.data)),
  ).toString("utf8");
  return async (expected: string) => {
    const deadline = AbortSignal.timeout(5000);
    while (!text.includes(expected)) {
      const event = await Promise.race([
        attachment.nextEvent(),
        new Promise<never>((_, reject) =>
          deadline.addEventListener(
            "abort",
            () => reject(new Error(`output timeout: ${text}`)),
            { once: true },
          ),
        ),
      ]);
      assert.ok(event, "original attachment remains open");
      if (event.type === "output") {
        assert.equal(
          event.chunk.start_byte,
          cursor,
          "ordered bytes are continuous",
        );
        cursor = event.chunk.end_byte;
        text += Buffer.from(event.chunk.data).toString("utf8");
      } else if (event.type === "gap" || event.type === "exited") {
        assert.fail(
          `healthy private Run lost continuous output: ${JSON.stringify(event)}`,
        );
      }
    }
    return { cursor, text };
  };
}

async function ackRun(owner: Awaited<ReturnType<typeof privateRuntime>>) {
  const run = await owner.activation.client.start(
    defineRun("/bin/sh", {
      args: [
        "-c",
        'stty -echo; printf "READY\\n"; while IFS= read -r line; do printf "ACK:%s\\n" "$line"; done',
      ],
      cwd: owner.directory,
    }),
  );
  owner.runs.push(run);
  const attachment = await owner.activation.client.attach(run.id);
  const read = outputReader(attachment);
  await read("READY");
  return { run, attachment, read };
}

test(
  "real original PTY scope and exec generation survive SDK replacement and ordered input",
  { timeout: 20000 },
  async () => {
    const owner = await privateRuntime();
    const attachments: Attachment[] = [];
    try {
      const a = await owner.activation.client.start(
        defineRun("/bin/sh", {
          args: [
            "-c",
            'stty -echo; printf "READY\\n"; IFS= read -r line; exec /bin/cat',
          ],
          cwd: owner.directory,
        }),
      );
      owner.runs.push(a);
      const attachment = await owner.activation.client.attach(a.id);
      attachments.push(attachment);
      const readA = outputReader(attachment);
      await readA("READY");
      const b = await ackRun(owner);
      attachments.push(b.attachment);
      await b.attachment.input("before\n");
      const beforeBytes = await b.read("ACK:before");
      const first = await owner.activation.client.observeForeground({
        runId: a.id,
      });
      assert.deepEqual(first.runtime, owner.activation.runtime);
      assert.equal(first.observation.outcome, "observed");
      if (first.observation.outcome !== "observed")
        assert.fail(JSON.stringify(first));
      assert.equal(first.observation.rootPid, a.pid);
      assert.equal(first.observation.posixSessionId, a.pid);
      assert.ok(first.observation.processes.length > 0);
      assert.deepEqual(
        first.observation.processes.map((p) => p.pid),
        [a.pid],
      );
      const firstScope = first.observation;
      assert.ok(
        firstScope.processes.every(
          (p) => p.sid === a.pid && p.pgid === firstScope.foregroundPgid,
        ),
      );
      const image = first.observation.processes[0]!;
      const replacement = new CtxmuxClient({
        socketPath: join(owner.directory, "r.sock"),
        expectedRuntimeIdentity: first.runtime,
      });
      const other = await replacement.observeForeground({ runId: b.run.id });
      assert.deepEqual(other.runtime, first.runtime);
      assert.equal(other.observation.outcome, "observed");
      if (other.observation.outcome !== "observed")
        assert.fail(JSON.stringify(other));
      assert.deepEqual(
        other.observation.processes.map((p) => p.pid),
        [b.run.pid],
      );
      await attachment.input("exec\n");
      await attachment.input("AFTER_EXEC\n");
      await readA("AFTER_EXEC");
      const exec = await replacement.observeForeground({ runId: a.id });
      assert.equal(exec.observation.outcome, "observed");
      if (exec.observation.outcome !== "observed")
        assert.fail(JSON.stringify(exec));
      const nextImage = exec.observation.processes[0]!;
      assert.equal(nextImage.pid, image.pid);
      assert.equal(nextImage.processIncarnation, image.processIncarnation);
      assert.notEqual(nextImage.executionGeneration, image.executionGeneration);
      assert.notEqual(nextImage.executableImage, image.executableImage);
      await b.attachment.input("after\n");
      const afterBytes = await b.read("ACK:after");
      const afterA = await replacement.status(a.id);
      const afterB = await replacement.status(b.run.id);
      assert.equal(afterA.pid, a.pid);
      assert.equal(afterB.pid, b.run.pid);
      assert.equal(afterA.state.type, "running");
      assert.equal(afterB.state.type, "running");
      assert.ok(afterBytes.cursor > beforeBytes.cursor);
      facts.push({
        scenario: "physical-scope-exec-two-SDK",
        daemonPid: owner.activation.childPid,
        first,
        other,
        exec,
        beforeBytes,
        afterBytes,
        afterA,
        afterB,
      });
    } finally {
      attachments.forEach((a) => a.close());
      await owner.close();
    }
  },
);

test(
  "unfunded actual foreground observation does not affect two original Runs",
  { timeout: 15000 },
  async () => {
    const owner = await privateRuntime({
      foreground_observation_workers: 1,
      foreground_observation_bytes: 1,
    });
    const attachments: Attachment[] = [];
    try {
      const a = await ackRun(owner);
      const b = await ackRun(owner);
      attachments.push(a.attachment, b.attachment);
      const observation = await owner.activation.client.observeForeground({
        runId: a.run.id,
      });
      assert.equal(observation.observation.outcome, "unknown");
      await a.attachment.input("pressure-a\n");
      await b.attachment.input("pressure-b\n");
      const bytesA = await a.read("ACK:pressure-a");
      const bytesB = await b.read("ACK:pressure-b");
      const afterA = await owner.activation.client.status(a.run.id);
      const afterB = await owner.activation.client.status(b.run.id);
      assert.equal(afterA.pid, a.run.pid);
      assert.equal(afterB.pid, b.run.pid);
      assert.equal(afterA.state.type, "running");
      assert.equal(afterB.state.type, "running");
      facts.push({
        scenario: "real-OS-memory-pressure",
        daemonPid: owner.activation.childPid,
        observation,
        bytesA,
        bytesB,
        afterA,
        afterB,
      });
    } finally {
      attachments.forEach((a) => a.close());
      await owner.close();
    }
  },
);

test(
  "actual multi-member foreground remains complete and another private Run stays responsive",
  { timeout: 15000 },
  async () => {
    const owner = await privateRuntime({
      foreground_observation_workers: 1,
      foreground_observation_timeout_ms: 1000,
    });
    const attachments: Attachment[] = [];
    try {
      const run = await owner.activation.client.start(
        defineRun("/bin/sh", {
          args: [
            "-c",
            'stty -echo; for n in 1 2 3 4 5 6 7 8 9 10 11 12; do /bin/sleep 30 & done; printf "READY\\n"; while IFS= read -r line; do printf "ACK:%s\\n" "$line"; done',
          ],
          cwd: owner.directory,
        }),
      );
      owner.runs.push(run);
      const a = await owner.activation.client.attach(run.id);
      attachments.push(a);
      const readA = outputReader(a);
      await readA("READY");
      const b = await ackRun(owner);
      attachments.push(b.attachment);
      const observation = await owner.activation.client.observeForeground({
        runId: run.id,
      });
      facts.push({
        scenario: "real-multi-member-before-assertion",
        observation,
      });
      assert.equal(observation.observation.outcome, "observed");
      if (observation.observation.outcome !== "observed")
        assert.fail(JSON.stringify(observation));
      assert.equal(observation.observation.processes.length, 13);
      assert.equal(
        observation.observation.processes.filter(
          (p) => p.executablePath === "/bin/sleep",
        ).length,
        12,
      );
      await a.input("deadline-a\n");
      await b.attachment.input("deadline-b\n");
      const bytesA = await readA("ACK:deadline-a");
      const bytesB = await b.read("ACK:deadline-b");
      const afterA = await owner.activation.client.status(run.id);
      const afterB = await owner.activation.client.status(b.run.id);
      assert.equal(afterA.pid, run.pid);
      assert.equal(afterB.pid, b.run.pid);
      assert.equal(afterA.state.type, "running");
      assert.equal(afterB.state.type, "running");
      facts.push({
        scenario: "real-OS-multi-member",
        daemonPid: owner.activation.childPid,
        observation,
        bytesA,
        bytesB,
        afterA,
        afterB,
      });
    } finally {
      attachments.forEach((a) => a.close());
      await owner.close();
    }
  },
);

test(
  "disposable SDK read deadline closes its own wire without affecting actual Runs",
  { timeout: 15000 },
  async () => {
    const owner = await privateRuntime();
    const attachments: Attachment[] = [];
    const sockets = new Set<Socket>();
    let actualObservation: unknown;
    let requestCount = 0;
    const sourceSocket = join(owner.directory, "r.sock");
    const proxySocket = join(owner.directory, "observation.sock");
    const proxy = createServer((peer) => {
      sockets.add(peer);
      const source = createConnection(sourceSocket);
      sockets.add(source);
      peer.on("error", () => {});
      source.on("error", () => {});
      peer.on("close", () => source.destroy());
      source.on("close", () => peer.destroy());
      let requestBuffer = "";
      let responseBuffer = "";
      peer.on("data", (chunk) => {
        requestBuffer += chunk.toString("utf8");
        while (requestBuffer.includes("\n")) {
          const end = requestBuffer.indexOf("\n");
          const line = requestBuffer.slice(0, end);
          requestBuffer = requestBuffer.slice(end + 1);
          const frame = JSON.parse(line);
          if (
            frame.type === "request" &&
            frame.request.type === "observe_foreground"
          )
            requestCount++;
          source.write(line + "\n");
        }
      });
      source.on("data", (chunk) => {
        responseBuffer += chunk.toString("utf8");
        while (responseBuffer.includes("\n")) {
          const end = responseBuffer.indexOf("\n");
          const line = responseBuffer.slice(0, end);
          responseBuffer = responseBuffer.slice(end + 1);
          const frame = JSON.parse(line);
          if (
            frame.type === "response" &&
            frame.response.type === "foreground_observation"
          ) {
            actualObservation = frame.response.observation;
            // Test-only withheld response. It is never transformed into an
            // invented observation or sent as a second request to the Runtime.
          } else peer.write(line + "\n");
        }
      });
    });
    await new Promise<void>((resolve) => proxy.listen(proxySocket, resolve));
    try {
      const a = await ackRun(owner);
      const b = await ackRun(owner);
      attachments.push(a.attachment, b.attachment);
      const reader = new CtxmuxClient({
        socketPath: proxySocket,
        expectedRuntimeIdentity: owner.activation.runtime,
        foregroundObservationTimeoutMs: 100,
      });
      await assert.rejects(
        reader.observeForeground({ runId: a.run.id }),
        (error) =>
          error instanceof Error && /abort|closed/iu.test(error.message),
      );
      assert.equal(
        requestCount,
        1,
        "the exact native observation request crossed its original owner",
      );
      assert.ok(
        actualObservation !== undefined,
        "the actual Runtime observation reply was received then held by this test proxy",
      );
      assert.equal(
        (actualObservation as { outcome: string }).outcome,
        "observed",
      );
      await a.attachment.input("wire-a\n");
      await b.attachment.input("wire-b\n");
      const bytesA = await a.read("ACK:wire-a");
      const bytesB = await b.read("ACK:wire-b");
      const afterA = await owner.activation.client.status(a.run.id);
      const afterB = await owner.activation.client.status(b.run.id);
      assert.equal(afterA.pid, a.run.pid);
      assert.equal(afterB.pid, b.run.pid);
      assert.equal(afterA.state.type, "running");
      assert.equal(afterB.state.type, "running");
      facts.push({
        scenario: "actual-Runtime-SDK-local-read-timeout",
        daemonPid: owner.activation.childPid,
        requestCount,
        actualObservation,
        bytesA,
        bytesB,
        afterA,
        afterB,
        osWorkerBlocked: false,
        physicalWorkerLifetimeProof:
          "separate deterministic production-run_job test",
      });
    } finally {
      sockets.forEach((s) => s.destroy());
      await new Promise<void>((resolve) => proxy.close(() => resolve()));
      attachments.forEach((a) => a.close());
      await owner.close();
    }
  },
);

test.after(async () => {
  if (process.env.CTXMUX_FOREGROUND_RECEIPT_PATH) {
    await writeFile(
      process.env.CTXMUX_FOREGROUND_RECEIPT_PATH,
      JSON.stringify(
        { facts, userRuntimeOperations: 0, providerQualification: false },
        null,
        2,
      ),
    );
  }
});
