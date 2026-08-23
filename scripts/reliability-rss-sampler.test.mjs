import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { chmod, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import test from "node:test";

import {
  prepareRssSampler,
  startRssSampler,
} from "./reliability-rss-sampler.mts";

const helper = resolve("target/debug/ctxmux-rss-sampler");

async function fixtureHelper(body) {
  const directory = await mkdtemp(join(tmpdir(), "ctxmux-rss-fixture-"));
  const executable = join(directory, "sampler");
  await writeFile(
    executable,
    `#!/bin/sh\nprintf '%s\\n' "$$" > "$0.pid"\nprintf '%s\\n' 'ctxmux-rss-sampler-ready-v1' >&2\nread start_command\n[ "$start_command" = start ] || exit 2\n${body}\n`,
    "utf8",
  );
  await chmod(executable, 0o755);
  return { directory, executable, pidPath: `${executable}.pid` };
}

async function startTestSampler(executable, target, intervalMs, maximumGapMs) {
  const prepared = await prepareRssSampler(
    executable,
    target,
    intervalMs,
    maximumGapMs,
  );
  return startRssSampler(prepared);
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

function dynamicFrameCommand(
  { timestampOffsetMs = 0, ...overrides } = {},
  newline = true,
) {
  const fields = JSON.stringify({
    schema: "ctxmux.rss-sample.v1",
    seq: 1,
    rss_kib: 1,
    final_frame: false,
    ...overrides,
  });
  return `"${process.execPath}" -e 'const frame=${fields}; frame.timestamp_ms=Date.now()+${String(timestampOffsetMs)}; process.stdout.write(JSON.stringify(frame)${newline ? '+"\\n"' : ""})'`;
}

test("native RSS sampling continues across qualification event-loop stalls", async () => {
  const sampler = await startTestSampler(helper, process.pid, 25, 5_000);
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
    prepareRssSampler(helper, process.pid, 25, 24),
    /maximum gap/u,
  );
  await assert.rejects(
    startTestSampler(helper, 99_999_999, 25, 500),
    /target process is unavailable/u,
  );
});

test("RSS sampling fails closed when the helper cannot start", async () => {
  await assert.rejects(
    prepareRssSampler(
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
    const prepared = await prepareRssSampler(
      fixture.executable,
      process.pid,
      25,
      100,
    );
    const helperPid = Number(await readFile(fixture.pidPath, "utf8"));
    assert.ok(Number.isSafeInteger(helperPid) && helperPid > 0);
    assert.doesNotThrow(() => process.kill(helperPid, 0));
    await assert.rejects(startRssSampler(prepared), /first frame in time/u);
    for (let attempt = 0; attempt < 100; attempt += 1) {
      try {
        process.kill(helperPid, 0);
      } catch (error) {
        assert.match(String(error), /ESRCH/u);
        return;
      }
      await delay(10);
    }
    assert.fail("timed-out RSS helper was not reaped");
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
      startTestSampler(fixture.executable, process.pid, 25, 5_000),
      /first observation started outside/u,
    );
  } finally {
    await rm(fixture.directory, { recursive: true, force: true });
  }
});

test("RSS sampling rejects first-frame delivery after the sample deadline", async () => {
  // This frame must lose to one 100 ms deadline, not gain a second drain window.
  const fixture = await fixtureHelper(`sleep 0.13\n${dynamicFrameCommand()}`);
  try {
    await assert.rejects(
      startTestSampler(fixture.executable, process.pid, 25, 100),
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
      startTestSampler(fixture.executable, process.pid, 25, 5_000),
      /before it became ready/u,
    );
  } finally {
    await rm(fixture.directory, { recursive: true, force: true });
  }
});

test("RSS sampling rejects an early final frame after readiness", async () => {
  const fixture = await fixtureHelper(
    `"${process.execPath}" -e 'const timestamp=Date.now(); const base={schema:"ctxmux.rss-sample.v1",rss_kib:1}; process.stdout.write(JSON.stringify({...base,timestamp_ms:timestamp,seq:1,final_frame:false})+"\\n"+JSON.stringify({...base,timestamp_ms:timestamp+1,seq:2,final_frame:true})+"\\n")'`,
  );
  try {
    await assert.rejects(async () => {
      const sampler = await startTestSampler(
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
    const sampler = await startTestSampler(helper, child, 25, 100);
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
    const sampler = await startTestSampler(helper, target, 25, 100);
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
    `#!/bin/sh\ntrap '' TERM\nprintf '%s\\n' 'ctxmux-rss-sampler-ready-v1' >&2\nread start_command\n${dynamicFrameCommand()}\nexec /bin/sleep 10\n`,
    "utf8",
  );
  await chmod(fakeHelper, 0o755);
  try {
    const sampler = await startTestSampler(fakeHelper, process.pid, 25, 1_000);
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
    const second = secondFrame.includes('"seq":3')
      ? dynamicFrameCommand({ timestampOffsetMs: 1, seq: 3 })
      : dynamicFrameCommand({ timestampOffsetMs: 1001, seq: 2 });
    const fixture = await fixtureHelper(
      `${dynamicFrameCommand()}\n${second}\nexec /bin/sleep 10`,
    );
    try {
      const sampler = await startTestSampler(
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
  const fixture = await fixtureHelper(dynamicFrameCommand({}, false));
  try {
    const sampler = await startTestSampler(
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
