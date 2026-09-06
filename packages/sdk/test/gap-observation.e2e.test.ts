import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { performance } from "node:perf_hooks";
import test from "node:test";
import {
  activateRuntime,
  CtxmuxClient,
  defineRun,
  type Attachment,
  type RunInfo,
  type RuntimeActivation,
  type AttachmentGapObservation,
} from "@ctxmux/sdk";

const binary = process.env.CTXMUXD_BIN;
if (binary === undefined)
  throw new Error("CTXMUXD_BIN must select the freshly built Gap candidate");

// Representative binary burst: large enough to cross this client's explicit
// view window, smaller than unchanged default live/replay budgets. Not capacity.
const burst = Buffer.from(Array.from({ length: 65536 }, (_, i) => i % 256));
const childProgram = String.raw`
import os, signal, sys, termios
name = sys.argv[1].encode()
attrs = termios.tcgetattr(0)
attrs[1] &= ~termios.ONLCR
attrs[3] &= ~termios.ECHO
termios.tcsetattr(0, termios.TCSANOW, attrs)
def emit(data):
    while data:
        data = data[os.write(1, data):]
def interrupted(signum, frame):
    emit(name + b':CTRL_C\n')
    raise SystemExit(0)
signal.signal(signal.SIGINT, interrupted)
emit(name + b':READY\x00\xff\n')
for line in sys.stdin.buffer:
    if line == b'bulk\n':
        emit(bytes(range(256)) * 256)
    else:
        emit(name + b':' + line)
`;

async function readExact(
  attachment: Attachment,
  through: number,
  prefix: Buffer,
): Promise<Buffer> {
  const parts = [prefix];
  let cursor = prefix.length;
  while (cursor < through) {
    const event = await attachment.nextEvent();
    assert.ok(event !== undefined, "healthy stream remains open");
    if (event.type === "service_changed") {
      assert.equal(event.service.owner.type, "serving");
      continue;
    }
    assert.equal(
      event.type,
      "output",
      "healthy reader has no hidden loss or invented exit",
    );
    if (event.type !== "output") throw new Error("unexpected healthy event");
    assert.equal(event.chunk.start_byte, cursor);
    cursor = event.chunk.end_byte;
    parts.push(Buffer.from(event.chunk.data));
  }
  assert.equal(cursor, through);
  return Buffer.concat(parts);
}

function replay(attachment: Attachment): Buffer {
  return Buffer.concat(
    attachment.snapshot.replay.chunks.map((chunk) => Buffer.from(chunk.data)),
  );
}

function cost(pid: number): {
  daemonRssKiB: number;
  nodeRssBytes: number;
  nodeCpuMicros: number;
} {
  const usage = process.resourceUsage();
  return {
    daemonRssKiB: Number(
      execFileSync("ps", ["-o", "rss=", "-p", String(pid)], {
        encoding: "utf8",
      }).trim(),
    ),
    nodeRssBytes: process.memoryUsage().rss,
    nodeCpuMicros: usage.userCPUTime + usage.systemCPUTime,
  };
}

test(
  "two real Runs and two Clients isolate slow view loss, recover exact bytes and keep controls",
  {
    // Test completion objective for process startup, both live streams and cleanup;
    // it is not a production timeout or a measured throughput acceptance budget.
    timeout: 30000,
  },
  async () => {
    const directory = await mkdtemp(join(tmpdir(), "ctxmux-gap-"));
    let activation: RuntimeActivation | undefined;
    const runs: RunInfo[] = [];
    const views: Attachment[] = [];
    const started = performance.now();
    const gaps: AttachmentGapObservation[] = [];
    try {
      activation = await activateRuntime({
        executable: binary!,
        socketPath: join(directory, "r.sock"),
        timeoutMs: 5000,
        childDisposition: { mode: "detached", stderr: "pipe" },
      });
      assert.ok(activation.childPid !== undefined);
      const runtime = activation.runtime;
      const healthy = new CtxmuxClient({
        socketPath: activation.socketPath,
        expectedRuntimeIdentity: runtime,
      });
      const slow = new CtxmuxClient({
        socketPath: activation.socketPath,
        expectedRuntimeIdentity: runtime,
        // Explicit 32 decoded-byte view window exercises local pressure only.
        // The real owner, healthy client and replay retain their original budgets.
        attachmentViewResources: { payloadBytes: 32 },
      });
      const initialCost = cost(activation.childPid);
      for (const name of ["A", "B"])
        runs.push(
          await healthy.start(
            defineRun("/usr/bin/python3", {
              args: ["-u", "-c", childProgram, name],
            }),
          ),
        );
      assert.notEqual(runs[0]!.id, runs[1]!.id);
      assert.notEqual(runs[0]!.pid, runs[1]!.pid);
      const a = await healthy.attach(runs[0]!.id);
      const b = await healthy.attach(runs[1]!.id);
      views.push(a, b);
      const readyA = Buffer.from("A:READY\x00\xff\n", "latin1");
      const readyB = Buffer.from("B:READY\x00\xff\n", "latin1");
      let bytesA = await readExact(a, readyA.length, replay(a));
      let bytesB = await readExact(b, readyB.length, replay(b));
      assert.deepEqual(bytesA, readyA);
      assert.deepEqual(bytesB, readyB);
      const slowView = await slow.attach(runs[0]!.id, readyA.length);
      views.push(slowView);
      assert.equal(slowView.snapshot.replay.latest_output_bytes, readyA.length);
      const applied = await slowView.input("bulk\n");
      assert.equal(applied.receipt.written_bytes, 5);
      const fullA = Buffer.concat([readyA, burst]);
      bytesA = await readExact(a, fullA.length, bytesA);
      assert.deepEqual(bytesA, fullA);

      let discarded = 0;
      let retained = 0;
      let recovery: number | undefined;
      let observationIdentity: string | undefined;
      while (discarded + retained < burst.length) {
        const event = await slowView.nextEvent();
        assert.ok(event !== undefined);
        if (event.type === "service_changed") continue;
        if (event.type === "output") {
          retained += event.chunk.data.length;
          continue;
        }
        assert.equal(event.type, "gap");
        if (event.type !== "gap") throw new Error("unexpected slow view event");
        assert.deepEqual(event.observation.origins, {
          daemon: false,
          client: true,
        });
        assert.equal(event.causes.client_view_pressure, true);
        assert.equal(event.causes.subscriber_lag, false);
        assert.deepEqual(event.observation.runtime, {
          runtimeId: runtime.runtimeId,
          daemonInstanceId: runtime.daemonInstanceId,
        });
        assert.equal(event.observation.runId, runs[0]!.id);
        assert.ok(event.observation.missingOutputBytes !== null);
        discarded += event.observation.missingOutputBytes;
        recovery ??= event.observation.recoveryAfterByte;
        observationIdentity ??= event.observation.attachmentId;
        assert.equal(event.observation.recoveryAfterByte, recovery);
        assert.equal(event.observation.attachmentId, observationIdentity);
        assert.ok(
          event.observation.recoveryAfterByte < event.latest_output_bytes,
        );
        assert.ok(event.observation.queue.retainedPayloadBytes <= 32);
        assert.ok(
          event.observation.queue.retainedEnvelopeBytes <=
            event.observation.queue.envelopeBudgetBytes,
        );
        gaps.push(event.observation);
      }
      assert.equal(discarded + retained, burst.length);
      assert.ok(discarded > 0);
      assert.ok(recovery !== undefined);

      const ping = {
        daemonInstance: runtime.daemonInstanceId,
        operationKey: "gap-healthy-input",
        runId: runs[1]!.id,
        expectedByte: 0,
        data: "ping\n",
      };
      const first = await healthy.recoverableInput(ping);
      const repeated = await slow.recoverableInput(ping);
      assert.deepEqual(first.receipt, { start_byte: 0, end_byte: 5 });
      assert.deepEqual(repeated.receipt, first.receipt);
      const fullB = Buffer.concat([readyB, Buffer.from("B:ping\n")]);
      bytesB = await readExact(b, fullB.length, bytesB);
      assert.deepEqual(
        bytesB,
        fullB,
        "same-key confirmation must not duplicate physical input",
      );

      await slowView.detach();
      const recovered = await slow.attach(runs[0]!.id, recovery);
      views.push(recovered);
      assert.equal(recovered.snapshot.replay.truncated, false);
      assert.equal(recovered.snapshot.replay.first_available_byte, 0);
      assert.equal(recovered.snapshot.replay.chunks[0]?.start_byte, recovery);
      assert.deepEqual(replay(recovered), fullA.subarray(recovery));
      for (const run of runs) {
        const current = await healthy.status(run.id);
        assert.equal(current.pid, run.pid);
        assert.equal(current.state.type, "running");
        assert.equal(current.native_service?.owner.type, "serving");
      }
      await recovered.detach();
      await a.detach();
      await b.detach();
      const aAgain = await healthy.attach(runs[0]!.id, fullA.length);
      const bAgain = await slow.attach(runs[1]!.id, fullB.length);
      views.push(aAgain, bAgain);
      assert.equal(aAgain.snapshot.run.pid, runs[0]!.pid);
      assert.equal(bAgain.snapshot.run.pid, runs[1]!.pid);
      assert.equal(
        (await healthy.input(runs[0]!.id, new Uint8Array([3]))).receipt
          .written_bytes,
        1,
      );
      await slow.interrupt(runs[1]!.id);
      for (const [view, head, name] of [
        [aAgain, fullA.length, "A"],
        [bAgain, fullB.length, "B"],
      ] as const) {
        const tail = Buffer.from(`${name}:CTRL_C\n`);
        let cursor = head;
        const pieces: Buffer[] = [];
        for (;;) {
          const event = await view.nextEvent();
          assert.ok(event !== undefined);
          if (event.type === "output") {
            assert.equal(event.chunk.start_byte, cursor);
            cursor = event.chunk.end_byte;
            pieces.push(Buffer.from(event.chunk.data));
          } else if (event.type === "exited") {
            assert.equal(event.state.type, "exited");
            break;
          } else assert.equal(event.type, "service_changed");
        }
        assert.deepEqual(Buffer.concat(pieces), tail);
        assert.equal(cursor, head + tail.length);
      }
      for (const run of runs) {
        for (;;) {
          if ((await healthy.status(run.id)).state.type === "exited") break;
          await delay(1);
        }
      }
      const receipt = {
        runtime,
        runs: runs.map(({ id, pid }) => ({ id, pid })),
        byteCounts: {
          a: fullA.length,
          b: fullB.length,
          discarded,
          retained,
          recovery,
        },
        gaps,
        initialCost,
        finalCost: cost(activation.childPid),
        elapsedMs: performance.now() - started,
        qualification:
          "finite public Gap isolation and recovery; macOS/Linux costs require their own full gates",
      };
      if (process.env.CTXMUX_GAP_RECEIPT !== undefined)
        await writeFile(
          process.env.CTXMUX_GAP_RECEIPT,
          JSON.stringify(receipt, null, 2),
        );
      console.log(
        JSON.stringify({
          result: "PASS",
          ...receipt.byteCounts,
          gapObservations: gaps.length,
          elapsedMs: receipt.elapsedMs,
        }),
      );
    } finally {
      for (const view of views) view.close();
      if (activation !== undefined) {
        for (const run of runs) {
          try {
            const client = new CtxmuxClient({
              socketPath: activation.socketPath,
            });
            if ((await client.status(run.id)).state.type === "running")
              await client.stop(await client.prepareStop(run.id));
          } catch {
            /* Retain original failure; owned daemon shutdown also performs cleanup. */
          }
        }
        await activation.shutdown();
      }
      await rm(directory, { recursive: true, force: true });
    }
  },
);
