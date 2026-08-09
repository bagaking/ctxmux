import assert from "node:assert/strict";
import test from "node:test";

import {
  startAndWaitForGcRunExit,
  waitForGcRunExit,
} from "./reliability-gc-deadline.mts";
import { startGcRun } from "./reliability-gc-workload.mts";

const identity = {
  mode: "persistent_replay_pressure",
  index: 67,
  operation_key: "gc-pressure:persistent_replay_pressure:67:digest",
};
const runIdentity = {
  ...identity,
  run_id: "00000000-0000-4000-8000-000000000067",
};

const loadedGcFixture = {
  contract: {
    seed: "226004",
    helper: { path: "scripts/reliability-gc-child.mjs" },
    run_spec: { size: { cols: 80, rows: 24 } },
    payload_modes: {
      memory_only: { payload_bytes: 4096, hex_repetitions: 64 },
    },
    replay_pressure: {
      operation_key_template: "gc-pressure:<mode>:<index>:<digest-hex>",
    },
  },
};

test("GC workload does not dispatch start after the owning phase deadline", async () => {
  let startCalls = 0;
  let statusCalls = 0;
  const client = {
    async start() {
      startCalls += 1;
      return { id: runIdentity.run_id };
    },
    async status() {
      statusCalls += 1;
      return { state: { type: "exited", code: 0, signal: null } };
    },
  };

  await assert.rejects(
    startGcRun(
      client,
      process.cwd(),
      loadedGcFixture,
      "memory_only",
      0,
      Date.now() - 1,
    ),
    /complete start.*run_id=not_observed/u,
  );
  assert.equal(startCalls, 0);
  assert.equal(statusCalls, 0);
});

test("GC terminal wait accepts durable publication after thirty seconds inside the phase", async () => {
  let observedAt = 0;
  let calls = 0;
  const client = {
    async status() {
      calls += 1;
      observedAt = calls === 1 ? 31_000 : 32_000;
      return {
        state:
          calls === 1
            ? { type: "running" }
            : { type: "exited", code: 0, signal: null },
      };
    },
  };

  await waitForGcRunExit(client, runIdentity, 40_000, () => observedAt);
  assert.equal(calls, 2);
});

test("GC terminal wait fails at the owning phase deadline with workload identity", async () => {
  let observedAt = 0;
  const client = {
    async status() {
      observedAt += 10;
      return { state: { type: "running" } };
    },
  };

  await assert.rejects(
    waitForGcRunExit(client, runIdentity, 25, () => observedAt),
    (error) => {
      assert.match(error.message, /persistent_replay_pressure/u);
      assert.match(error.message, /index=67/u);
      assert.match(error.message, /operation_key=/u);
      assert.match(error.message, /run_id=/u);
      assert.match(error.message, /last_state=.*running/u);
      assert.match(error.message, /remaining_phase_ms=0/u);
      return true;
    },
  );
});

test("GC terminal wait bounds an in-flight public status request", async () => {
  const startedAt = Date.now();
  const client = {
    async status() {
      return await new Promise(() => undefined);
    },
  };

  await assert.rejects(
    waitForGcRunExit(client, runIdentity, startedAt + 50),
    /last_state="not_observed"/u,
  );
  assert.ok(
    Date.now() - startedAt < 1_000,
    "status escaped the phase deadline",
  );
});

test("GC start and status share one absolute phase deadline", async () => {
  const run = { id: runIdentity.run_id };
  let observedAt = 5;
  const client = {
    async start() {
      observedAt = 20;
      return run;
    },
    async status() {
      observedAt = 31_000;
      return { state: { type: "exited", code: 0, signal: null } };
    },
  };

  assert.equal(
    await startAndWaitForGcRunExit(client, identity, 40_000, () => observedAt),
    run,
  );
});

test("GC start is bounded before a Run id exists", async () => {
  const startedAt = Date.now();
  const client = {
    async start() {
      return await new Promise(() => undefined);
    },
    async status() {
      assert.fail("status must not run before start returns");
    },
  };

  await assert.rejects(
    startAndWaitForGcRunExit(client, identity, startedAt + 50),
    /complete start.*run_id=not_observed/u,
  );
});

test("GC rejects terminal status observed after the absolute deadline", async () => {
  let observedAt = 10;
  const client = {
    async status() {
      observedAt = 41;
      return { state: { type: "exited", code: 0, signal: null } };
    },
  };

  await assert.rejects(
    waitForGcRunExit(client, runIdentity, 40, () => observedAt),
    /complete status.*remaining_phase_ms=0/u,
  );
});

test("GC does not dispatch start after the absolute phase deadline", async () => {
  let starts = 0;
  const client = {
    async start() {
      starts += 1;
      return { id: runIdentity.run_id };
    },
    async status() {
      assert.fail("status must not run after an expired start deadline");
    },
  };

  await assert.rejects(
    startAndWaitForGcRunExit(client, identity, 10, () => 11),
    /complete start.*remaining_phase_ms=0/u,
  );
  assert.equal(starts, 0);
});

test("GC request errors retain workload identity and cause", async () => {
  const cause = new Error("socket closed");
  const client = {
    async start() {
      throw cause;
    },
    async status() {
      assert.fail("status must not run after a failed start");
    },
  };

  await assert.rejects(
    startAndWaitForGcRunExit(client, identity, 20, () => 10),
    (error) => {
      assert.match(error.message, /GC Run start failed.*index=67/u);
      assert.equal(error.cause, cause);
      return true;
    },
  );
});
