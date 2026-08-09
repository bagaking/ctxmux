import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { chmod, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import test from "node:test";

import {
  nextRssSampleDelay,
  startRssSampler,
} from "./reliability-rss-sampler.mts";

test("RSS sampling survives qualification event-loop stalls", async () => {
  const sampler = await startRssSampler(process.pid, 25, 100);
  const blockStartedAt = Date.now();
  Atomics.wait(
    new Int32Array(new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT)),
    0,
    0,
    175,
  );
  const blockCompletedAt = Date.now();

  await sampler.stop();
  const samples = sampler.samples();
  assert.ok(
    samples.some(
      (sample) =>
        sample.timestamp_ms >= blockStartedAt + 50 &&
        sample.timestamp_ms <= blockCompletedAt,
    ),
    "sampler did not observe RSS while the qualification event loop was blocked",
  );
  assert.ok(
    sampler.maxGapMs() <= 100,
    `sampler gap ${String(sampler.maxGapMs())}ms crossed the frozen contract`,
  );
  await sampler.stop();
});

test("RSS sampling fails closed when the target cannot be observed", async () => {
  await assert.rejects(startRssSampler(process.pid, 25, 24), /maximum gap/u);
  await assert.rejects(
    startRssSampler(99_999_999, 25, 100),
    /cannot sample RSS/u,
  );
});

test("RSS sampling fails closed when the target disappears", async () => {
  const child = spawn(process.execPath, ["-e", "setInterval(() => {}, 1000)"]);
  try {
    const sampler = await startRssSampler(child.pid, 25, 100);
    child.kill("SIGKILL");
    await once(child, "exit");
    await delay(50);
    await assert.rejects(sampler.stop(), /cannot sample RSS/u);
  } finally {
    if (child.exitCode === null) child.kill("SIGKILL");
  }
});

test("RSS sampling bounds and reaps a stuck observation command", async () => {
  const directory = await mkdtemp(join(tmpdir(), "ctxmux-rss-sampler-"));
  const fakePs = join(directory, "ps");
  await writeFile(
    fakePs,
    "#!/bin/sh\ntrap '' TERM\nexec /bin/sleep 10\n",
    "utf8",
  );
  await chmod(fakePs, 0o755);
  const previousPath = process.env.PATH;
  process.env.PATH = `${directory}:${previousPath ?? ""}`;
  try {
    const startedAt = Date.now();
    await assert.rejects(
      startRssSampler(process.pid, 25, 100),
      /cannot sample RSS/u,
    );
    assert.ok(Date.now() - startedAt < 1000, "stuck sampler was not reaped");
  } finally {
    process.env.PATH = previousPath;
    await rm(directory, { recursive: true, force: true });
  }
});

test("RSS sampling anchors cadence to observation start time", () => {
  assert.equal(nextRssSampleDelay(1000, 1010, 25), 15);
  assert.equal(nextRssSampleDelay(1000, 1060, 25), 0);
});
