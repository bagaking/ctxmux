import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { chmod, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import test from "node:test";

import { startRssSampler } from "./reliability-rss-sampler.mts";

const helper = resolve("target/debug/ctxmux-rss-sampler");

async function fixtureHelper(body) {
  const directory = await mkdtemp(join(tmpdir(), "ctxmux-rss-fixture-"));
  const executable = join(directory, "sampler");
  await writeFile(executable, `#!/bin/sh\n${body}\n`, "utf8");
  await chmod(executable, 0o755);
  return { directory, executable };
}

function sampleFrame(overrides = {}) {
  return JSON.stringify({
    schema: "ctxmux.rss-sample.v1",
    timestamp_ms: Date.now() + 500,
    seq: 1,
    rss_kib: 1,
    final_frame: false,
    ...overrides,
  });
}

test("native RSS sampling continues across qualification event-loop stalls", async () => {
  const sampler = await startRssSampler(helper, process.pid, 25, 5_000);
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
    `native sampler gap ${String(sampler.maxGapMs())}ms crossed the frozen cadence`,
  );
  await sampler.stop();
});

test("RSS sampling fails closed when the target cannot be observed", async () => {
  await assert.rejects(
    startRssSampler(helper, process.pid, 25, 24),
    /maximum gap/u,
  );
  await assert.rejects(
    startRssSampler(helper, 99_999_999, 25, 500),
    /target process is unavailable/u,
  );
});

test("RSS sampling fails closed when the helper cannot start", async () => {
  await assert.rejects(
    startRssSampler(
      "/definitely/missing/ctxmux-rss-sampler",
      process.pid,
      25,
      100,
    ),
    /ENOENT/u,
  );
});

test("RSS sampling bounds a stuck first observation by one sample gap", async () => {
  const fixture = await fixtureHelper("exec /bin/sleep 10");
  try {
    const startedAt = Date.now();
    await assert.rejects(
      startRssSampler(fixture.executable, process.pid, 25, 100),
      /first frame in time/u,
    );
    assert.ok(
      Date.now() - startedAt < 175,
      "first observation escaped the 100ms contract",
    );
  } finally {
    await rm(fixture.directory, { recursive: true, force: true });
  }
});

test("RSS sampling rejects a first observation timestamp outside the sample gap", async () => {
  const fixture = await fixtureHelper(
    `printf '%s\\n' '${sampleFrame({ timestamp_ms: Date.now() + 10_000 })}'`,
  );
  try {
    await assert.rejects(
      startRssSampler(fixture.executable, process.pid, 25, 5_000),
      /first observation started outside/u,
    );
  } finally {
    await rm(fixture.directory, { recursive: true, force: true });
  }
});

test("RSS sampling rejects first-frame delivery after the sample deadline", async () => {
  const fixture = await fixtureHelper(
    `sleep 0.2\nprintf '%s\\n' '${sampleFrame({ timestamp_ms: Date.now() + 25 })}'`,
  );
  try {
    await assert.rejects(
      startRssSampler(fixture.executable, process.pid, 25, 100),
      /first frame in time/u,
    );
  } finally {
    await rm(fixture.directory, { recursive: true, force: true });
  }
});

test("RSS sampling rejects a final frame before readiness", async () => {
  const fixture = await fixtureHelper(
    `printf '%s\\n' '${sampleFrame({ final_frame: true })}'`,
  );
  try {
    await assert.rejects(
      startRssSampler(fixture.executable, process.pid, 25, 5_000),
      /before it became ready/u,
    );
  } finally {
    await rm(fixture.directory, { recursive: true, force: true });
  }
});

test("RSS sampling rejects an early final frame after readiness", async () => {
  const timestamp = Date.now() + 500;
  const fixture = await fixtureHelper(
    `printf '%s\\n' '${sampleFrame({ timestamp_ms: timestamp })}' '${sampleFrame({ timestamp_ms: timestamp + 1, seq: 2, final_frame: true })}'`,
  );
  try {
    await assert.rejects(async () => {
      const sampler = await startRssSampler(
        fixture.executable,
        process.pid,
        25,
        5_000,
      );
      await sampler.stop();
    }, /before stop was requested/u);
  } finally {
    await rm(fixture.directory, { recursive: true, force: true });
  }
});

test("RSS sampling fails closed when the target disappears", async () => {
  const child = spawn(process.execPath, ["-e", "setInterval(() => {}, 1000)"]);
  try {
    const sampler = await startRssSampler(helper, child, 25, 100);
    child.kill("SIGKILL");
    await once(child, "exit");
    await delay(50);
    await assert.rejects(sampler.stop(), /RSS sampl/u);
  } finally {
    if (child.exitCode === null) child.kill("SIGKILL");
  }
});

test("RSS sampling uses the target owner to fence same-PID replacement", async () => {
  const target = spawn(process.execPath, ["-e", "setInterval(() => {}, 1000)"]);
  try {
    const sampler = await startRssSampler(helper, target, 25, 100);
    target.kill("SIGKILL");
    await once(target, "exit");
    await assert.rejects(
      sampler.stop(),
      /target exited before sampler completion/u,
    );
  } finally {
    if (target.exitCode === null) target.kill("SIGKILL");
  }
});

test("RSS sampling bounds and reaps a stuck helper", async () => {
  const directory = await mkdtemp(join(tmpdir(), "ctxmux-rss-sampler-"));
  const fakeHelper = join(directory, "sampler");
  await writeFile(
    fakeHelper,
    `#!/bin/sh\ntrap '' TERM\nprintf '%s\\n' '${sampleFrame()}'\nexec /bin/sleep 10\n`,
    "utf8",
  );
  await chmod(fakeHelper, 0o755);
  try {
    const sampler = await startRssSampler(fakeHelper, process.pid, 25, 1_000);
    await assert.rejects(sampler.stop());
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("RSS sampling rejects sequence and timestamp gaps", async () => {
  for (const [secondFrame, expected] of [
    [
      '{"schema":"ctxmux.rss-sample.v1","timestamp_ms":2,"seq":3,"rss_kib":1,"final_frame":false}',
      /sequence gap/u,
    ],
    [
      '{"schema":"ctxmux.rss-sample.v1","timestamp_ms":1002,"seq":2,"rss_kib":1,"final_frame":false}',
      /timestamp gap/u,
    ],
  ]) {
    const timestamp = Date.now() + 500;
    const fixture = await fixtureHelper(
      `printf '%s\\n' '${sampleFrame({ timestamp_ms: timestamp })}' '${secondFrame.replace('"timestamp_ms":2', `"timestamp_ms":${String(timestamp + 1)}`).replace('"timestamp_ms":1002', `"timestamp_ms":${String(timestamp + 1001)}`)}'\nexec /bin/sleep 10`,
    );
    try {
      const sampler = await startRssSampler(
        fixture.executable,
        process.pid,
        25,
        1_000,
      );
      await assert.rejects(sampler.stop(), expected);
    } finally {
      await rm(fixture.directory, { recursive: true, force: true });
    }
  }
});

test("RSS sampling rejects partial EOF", async () => {
  const fixture = await fixtureHelper(`printf '%s' '${sampleFrame()}'`);
  try {
    const sampler = await startRssSampler(
      fixture.executable,
      process.pid,
      25,
      5_000,
    );
    await assert.rejects(sampler.stop(), /partial frame/u);
  } finally {
    await rm(fixture.directory, { recursive: true, force: true });
  }
});
