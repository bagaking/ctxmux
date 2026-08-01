import assert from "node:assert/strict";
import test from "node:test";

import {
  IntegrationCapabilityError,
  registerIntegration,
  type AvailableIntegrationDetection,
  type RunInfo,
} from "../src/index.ts";
import { shellIntegration } from "../src/integrations/index.ts";

test("shell Integration detects an explicit executable without discovery", async () => {
  assert.deepEqual(await shellIntegration.detect({ executable: "/bin/sh" }), {
    status: "available",
    executable: "/bin/sh",
    version: null,
    capabilities: [],
  });
  assert.deepEqual(
    await shellIntegration.detect({ executable: "/ctxmux/missing-shell" }),
    {
      status: "unavailable",
      executable: "/ctxmux/missing-shell",
      reason: "not_found",
    },
  );
});

test("shell Integration preserves structured argv without implicit evaluation", () => {
  const detection: AvailableIntegrationDetection = {
    status: "available",
    executable: "/bin/sh",
    version: null,
    capabilities: [],
  };
  const args = ["-c", "printf '%s\\n'", "a;$(touch never)\nnext"];

  assert.deepEqual(
    shellIntegration.planLaunch(
      {
        args,
        cwd: "/workspace with spaces",
        env: { DECLARED: "one two" },
        size: { cols: 120, rows: 40 },
      },
      detection,
    ),
    {
      program: "/bin/sh",
      args,
      cwd: "/workspace with spaces",
      env: { DECLARED: "one two" },
      size: { cols: 120, rows: 40 },
      declared_inputs: [],
    },
  );
});

test("shell Integration has a disposable observer with no semantic claims", () => {
  assert.deepEqual(
    shellIntegration
      .createObserver()
      .observe({ type: "output", chunk: { seq: 1, data: [65] } }),
    [],
  );
});

test("shell Integration rejects Level B before a raw fork request", async () => {
  let forks = 0;
  const registered = registerIntegration(
    {
      async start(): Promise<RunInfo> {
        throw new Error("unreachable raw start");
      },
      async fork(): Promise<RunInfo> {
        forks += 1;
        throw new Error("unreachable raw fork");
      },
    },
    shellIntegration,
  );
  const parent: RunInfo = {
    id: "00000000-0000-0000-0000-000000000001",
    spec: {
      program: "/bin/sh",
      args: ["-i"],
      cwd: "/workspace",
      env: {},
      size: { cols: 80, rows: 24 },
      declared_inputs: [],
    },
    lineage: null,
    backend: { type: "native" },
    capabilities: {
      input: true,
      resize: true,
      stop: true,
      fork_level_a: true,
      fork_level_b: true,
      replay: "raw_from_start",
    },
    pid: 123,
    state: { type: "running" },
    head_seq: 0,
    durable_head_seq: null,
    oldest_seq: 0,
    attachments: 0,
  };

  await assert.rejects(
    registered.forkLevelB(parent, undefined, {
      detection: { executable: "/bin/sh" },
    }),
    (error: unknown) =>
      error instanceof IntegrationCapabilityError &&
      error.capability === "level_b_fork",
  );
  assert.equal(forks, 0);
});
