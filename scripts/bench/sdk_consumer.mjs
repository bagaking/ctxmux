import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { pathToFileURL } from "node:url";
import { performance } from "node:perf_hooks";

const [modulePath, socketPath, fixture] = process.argv.slice(2);
const { CtxmuxClient } = await import(pathToFileURL(modulePath).href);
const client = new CtxmuxClient({ socketPath });
const runtime = await client.runtimeInfo();
const slow = new CtxmuxClient({
  socketPath,
  expectedRuntimeIdentity: runtime,
  attachmentViewResources: { payloadBytes: 32 },
});
const runs = [];
const views = [];
const attempts = [];
const start = performance.now();
const costBefore = { rss: process.memoryUsage().rss, cpu: process.cpuUsage() };
const spec = (label) => ({
  program: fixture,
  args: [label],
  cwd: null,
  env: {},
  initial_size: { rows: 24, cols: 80 },
  declared_inputs: [],
});
const burst = Buffer.from(Array.from({ length: 65536 }, (_, i) => i % 256));
const digest = (data) => createHash("sha256").update(data).digest("hex");
const snapshotRead = new WeakSet();

async function measure(operation, call) {
  const began = performance.now();
  try {
    const result = await call;
    attempts.push({
      operation,
      outcome: "completed",
      elapsed_ms: performance.now() - began,
    });
    return result;
  } catch (error) {
    attempts.push({
      operation,
      outcome: "failed",
      elapsed_ms: performance.now() - began,
      error: String(error),
    });
    throw error;
  }
}

async function attach(owner, run, cursor = 0) {
  const view = await measure("sdk_attach", owner.attach(run.id, cursor));
  assert.equal(view.snapshot.run.id, run.id);
  views.push(view);
  return view;
}

async function readThrough(view, expected, cursor) {
  const parts = [];
  for (const chunk of snapshotRead.has(view)
    ? []
    : view.snapshot.replay.chunks) {
    assert.equal(chunk.start_byte, cursor);
    assert.deepEqual(
      Buffer.from(chunk.data),
      expected.subarray(cursor, chunk.end_byte),
    );
    cursor = chunk.end_byte;
    parts.push(Buffer.from(chunk.data));
  }
  snapshotRead.add(view);
  while (cursor < expected.length) {
    const event = await view.nextEvent();
    assert.ok(event);
    if (event.type === "service_changed" || event.type === "resized") continue;
    assert.equal(event.type, "output");
    assert.equal(event.chunk.start_byte, cursor);
    assert.deepEqual(
      Buffer.from(event.chunk.data),
      expected.subarray(cursor, event.chunk.end_byte),
    );
    cursor = event.chunk.end_byte;
    parts.push(Buffer.from(event.chunk.data));
  }
  assert.equal(cursor, expected.length);
  return { cursor, observedSegmentSha256: digest(Buffer.concat(parts)) };
}

let result;
try {
  runs.push(await client.start(spec("sdkA")), await slow.start(spec("sdkB")));
  const [a, b] = runs;
  assert.notEqual(a.id, b.id);
  assert.notEqual(a.pid, b.pid);
  let expectedA = Buffer.from(`READY sdkA ${a.pid}\n`);
  let expectedB = Buffer.from(`READY sdkB ${b.pid}\n`);
  const goodA = await attach(client, a);
  const goodB = await attach(client, b);
  await readThrough(goodA, expectedA, 0);
  await readThrough(goodB, expectedB, 0);
  const slowA = await attach(slow, a, expectedA.length);
  const oldCursor = expectedA.length;
  const taggedBurst = Buffer.concat([Buffer.from("B sdkA 65536 0\n"), burst]);
  expectedA = Buffer.concat([expectedA, taggedBurst]);
  await measure("sdk_input", client.input(a.id, "B 65536 0\n"));
  await measure(
    "sdk_receiver_verified",
    readThrough(goodA, expectedA, oldCursor),
  );
  let missing = 0;
  const gaps = [];
  while (missing < taggedBurst.length) {
    const event = await slowA.nextEvent();
    assert.ok(event);
    if (event.type === "service_changed") continue;
    if (event.type === "output") {
      missing += event.chunk.data.length;
      continue;
    }
    assert.equal(event.type, "gap");
    assert.equal(event.observation.runId, a.id);
    assert.equal(
      event.observation.runtime.daemonInstanceId,
      runtime.daemonInstanceId,
    );
    assert.equal(event.causes.client_view_pressure, true);
    missing += event.observation.missingOutputBytes;
    gaps.push(event.observation);
  }
  assert.ok(gaps.length);
  const recovered = await attach(slow, a, gaps[0].recoveryAfterByte);
  const replay = await measure(
    "sdk_gap_recovery",
    readThrough(recovered, expectedA, gaps[0].recoveryAfterByte),
  );
  const input = {
    daemonInstance: runtime.daemonInstanceId,
    operationKey: "bench-sdk-input",
    runId: b.id,
    expectedByte: 0,
    data: "P 9\n",
  };
  const one = await slow.recoverableInput(input);
  const two = await client.recoverableInput(input);
  assert.deepEqual(one.range, two.range);
  const previousB = expectedB.length;
  expectedB = Buffer.concat([expectedB, Buffer.from("P sdkB 9\n")]);
  await readThrough(goodB, expectedB, previousB);
  const beforeInterrupt = expectedB.length;
  expectedB = Buffer.concat([expectedB, Buffer.from("INT sdkB\n")]);
  await slow.input(b.id, Uint8Array.of(3));
  await readThrough(goodB, expectedB, beforeInterrupt);
  await goodB.detach();
  const rejoined = await attach(client, b, expectedB.length);
  const next = expectedB.length;
  expectedB = Buffer.concat([expectedB, Buffer.from("P sdkB 10\n")]);
  await rejoined.input("P 10\n");
  await readThrough(rejoined, expectedB, next);
  for (const run of runs) {
    const status = await client.status(run.id);
    assert.equal(status.pid, run.pid);
    assert.equal(status.native_service.owner.type, "serving");
  }
  result = {
    outcome: "completed",
    runtime,
    runIdentities: runs.map(({ id, pid }) => ({ id, pid })),
    gaps,
    replay,
    expectedA_sha256: digest(expectedA),
    expectedB_sha256: digest(expectedB),
  };
} catch (error) {
  result = { outcome: "failed", error: String(error) };
} finally {
  for (const view of views) await view.detach().catch(() => {});
  const cleanup = [];
  for (const run of runs) {
    try {
      await client.stop(await client.prepareStop(run.id));
      await client.remove(run.id);
      cleanup.push({ id: run.id, outcome: "removed" });
    } catch (error) {
      cleanup.push({ id: run.id, outcome: "failed", error: String(error) });
    }
  }
  result.cleanup = cleanup;
  result.attempts = attempts;
  result.wall_ms = performance.now() - start;
  result.costBefore = costBefore;
  result.costAfter = {
    rss: process.memoryUsage().rss,
    cpu: process.cpuUsage(),
  };
  console.log(JSON.stringify(result));
  if (
    result.outcome !== "completed" ||
    cleanup.some((row) => row.outcome !== "removed")
  )
    process.exitCode = 1;
}
