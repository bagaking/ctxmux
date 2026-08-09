import assert from "node:assert/strict";
import test from "node:test";

import {
  INTEGRATION_API_VERSION,
  IntegrationUnavailableError,
  registerIntegration,
} from "../src/index.ts";
import type {
  Integration,
  IntegrationSemanticEvent,
  RunInfo,
  RunSpec,
} from "../src/index.ts";

interface TestEvent extends IntegrationSemanticEvent {
  readonly integrationId: "test";
}

test("registerIntegration binds explicit tool semantics to the raw client", async () => {
  let startedSpec: RunSpec | undefined;
  const client = {
    async start(spec: RunSpec): Promise<RunInfo> {
      startedSpec = spec;
      return {
        id: "00000000-0000-0000-0000-000000000001",
        spec,
        pid: 123,
        state: { type: "running" },
        head_seq: 0,
        oldest_seq: 0,
        attachments: 0,
      };
    },
  };
  const integration: Integration<{ readonly message: string }, TestEvent> = {
    id: "test",
    apiVersion: INTEGRATION_API_VERSION,
    async detect() {
      return {
        status: "available",
        executable: "/bin/echo",
        version: "1.0.0",
        capabilities: ["semantic_events"],
      };
    },
    planLaunch(config, detection) {
      return {
        program: detection.executable,
        args: [config.message],
        cwd: null,
        env: {},
        size: { cols: 80, rows: 24 },
      };
    },
    createObserver() {
      return { observe: () => [] };
    },
  };

  const registered = registerIntegration(client, integration);
  const run = await registered.start({ message: "hello" });

  assert.equal(run.spec, startedSpec);
  assert.deepEqual(startedSpec, {
    program: "/bin/echo",
    args: ["hello"],
    cwd: null,
    env: {},
    size: { cols: 80, rows: 24 },
  });
  assert.notEqual(registered.createObserver(), registered.createObserver());
});

test("registerIntegration fails closed before start when detection is unavailable", async () => {
  let starts = 0;
  const integration: Integration<Record<string, never>, never> = {
    id: "missing",
    apiVersion: INTEGRATION_API_VERSION,
    async detect() {
      return {
        status: "unavailable",
        executable: "/missing",
        reason: "not_found",
      };
    },
    planLaunch() {
      throw new Error("unreachable launch plan");
    },
    createObserver() {
      return { observe: () => [] };
    },
  };
  const registered = registerIntegration(
    {
      async start(): Promise<RunInfo> {
        starts += 1;
        throw new Error("unreachable raw start");
      },
    },
    integration,
  );

  await assert.rejects(
    registered.start({}),
    (error: unknown) =>
      error instanceof IntegrationUnavailableError &&
      error.detection.reason === "not_found",
  );
  assert.equal(starts, 0);
});

test("registerIntegration rejects blank identities and unsupported generations", () => {
  const integration = {
    id: "   ",
    apiVersion: INTEGRATION_API_VERSION,
    async detect() {
      return {
        status: "unavailable" as const,
        executable: "/missing",
        reason: "not_found" as const,
      };
    },
    planLaunch(): RunSpec {
      throw new Error("unreachable launch plan");
    },
    createObserver() {
      return { observe: () => [] };
    },
  };

  assert.throws(
    () =>
      registerIntegration({ start: async () => Promise.reject() }, integration),
    /must not be empty/,
  );

  assert.throws(
    () =>
      registerIntegration(
        { start: async () => Promise.reject() },
        { ...integration, id: "future", apiVersion: 2 as 1 },
      ),
    /unsupported API version 2/,
  );
});
