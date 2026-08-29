import assert from "node:assert/strict";
import test from "node:test";

import {
  INTEGRATION_API_VERSION,
  IntegrationCapabilityError,
  IntegrationMaterializationError,
  IntegrationProvenanceError,
  IntegrationUnavailableError,
  registerIntegration,
} from "../src/index.ts";
import { rememberRunEventSource } from "../src/attachment.ts";
import type {
  Integration,
  IntegrationDetectionOptions,
  IntegrationSemanticEvent,
  LevelBForkPlan,
  CreateOperationKey,
  RunInfo,
  RunSpec,
} from "../src/index.ts";

interface TestEvent extends IntegrationSemanticEvent {
  readonly integrationId: "test";
}

test("registerIntegration binds explicit tool semantics to the raw client", async () => {
  let startedSpec: RunSpec | undefined;
  let startedKey: CreateOperationKey | undefined;
  let detectionOptions: IntegrationDetectionOptions | undefined;
  const client = {
    async start(
      spec: RunSpec,
      operationKey?: CreateOperationKey,
    ): Promise<RunInfo> {
      startedSpec = spec;
      startedKey = operationKey;
      return {
        id: "00000000-0000-0000-0000-000000000001",
        spec,
        lineage: null,
        backend: { type: "native" },
        capabilities: {
          input: true,
          resize: true,
          signal: true,
          stop: true,
          fork_level_a: true,
          fork_level_b: true,
          replay: "raw_from_start",
        },
        pid: 123,
        state: { type: "running" },
        latest_output_bytes: 0,
        durable_output_bytes: null,
        first_available_byte: 0,
        attachments: 0,
        applied_input_bytes: 0,
      };
    },
    async fork(): Promise<RunInfo> {
      throw new Error("unreachable raw fork");
    },
  };
  const integration: Integration<
    { readonly message: string },
    undefined,
    TestEvent
  > = {
    id: "test",
    apiVersion: INTEGRATION_API_VERSION,
    async detect(options) {
      detectionOptions = options;
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
        declared_inputs: [],
      };
    },
    createObserver() {
      return { observe: () => [] };
    },
  };

  const registered = registerIntegration(client, integration);
  const run = await registered.start(
    { message: "hello" },
    {
      detection: { executable: "/explicit/echo", timeoutMs: 250 },
      operationKey: "integration-operation-key",
    },
  );

  assert.equal(run.spec, startedSpec);
  assert.equal(startedKey, "integration-operation-key");
  assert.deepEqual(detectionOptions, {
    executable: "/explicit/echo",
    timeoutMs: 250,
  });
  assert.deepEqual(startedSpec, {
    program: "/bin/echo",
    args: ["hello"],
    cwd: null,
    env: {},
    size: { cols: 80, rows: 24 },
    declared_inputs: [],
  });
  assert.notEqual(registered.createObserver(), registered.createObserver());
});

test("registerIntegration fails closed before start when detection is unavailable", async () => {
  let starts = 0;
  const integration: Integration<Record<string, never>, undefined, never> = {
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
      async fork(): Promise<RunInfo> {
        throw new Error("unreachable raw fork");
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

test("registerIntegration rejects incomplete or downgraded Level B implementations before raw fork", async () => {
  let forks = 0;
  let plans = 0;
  const client = {
    async start(): Promise<RunInfo> {
      throw new Error("unreachable raw start");
    },
    async fork(): Promise<RunInfo> {
      forks += 1;
      throw new Error("unreachable raw fork");
    },
  };
  const withoutPlanner: Integration<Record<string, never>, undefined, never> = {
    id: "claimed-only",
    apiVersion: INTEGRATION_API_VERSION,
    async detect() {
      return {
        status: "available",
        executable: "/claimed-only",
        version: "1.0.0",
        capabilities: ["level_b_fork"],
      };
    },
    planLaunch() {
      throw new Error("unreachable launch plan");
    },
    createObserver() {
      return { observe: () => [] };
    },
  };
  const parent = rootRun();
  const parentSpec = parent.spec;
  assert.ok(parentSpec);

  await assert.rejects(
    registerIntegration(client, withoutPlanner).forkLevelB(parent, undefined),
    (error: unknown) =>
      error instanceof IntegrationMaterializationError &&
      error.reason === "missing_planner",
  );
  const withoutProvenance = {
    ...withoutPlanner,
    id: "missing-provenance",
    planLevelBFork(): LevelBForkPlan {
      plans += 1;
      return { type: "level_b", spec: parentSpec };
    },
  };
  await assert.rejects(
    registerIntegration(client, withoutProvenance).forkLevelB(
      parent,
      undefined,
    ),
    (error: unknown) =>
      error instanceof IntegrationProvenanceError && error.reason === "missing",
  );

  const emitted: TestEvent = {
    integrationId: "test",
    name: "receipt",
    data: {},
  };
  const downgraded: Integration<
    Record<string, never>,
    { readonly receipt: TestEvent },
    TestEvent
  > = {
    ...withoutPlanner,
    id: "downgraded",
    createObserver() {
      return { observe: () => [emitted] };
    },
    levelBForkProvenance(config) {
      return config.receipt;
    },
    planLevelBFork() {
      plans += 1;
      return { type: "level_a" } as unknown as LevelBForkPlan;
    },
  };
  const chunk = { start_byte: 0, end_byte: 1, data: new Uint8Array([65]) };
  rememberRunEventSource({ type: "output", chunk }, parent.id);
  const registered = registerIntegration(client, downgraded);
  const receipt = registered
    .createObserver(parent)
    .observe({ type: "output", chunk })[0];
  assert.notEqual(receipt, undefined);
  await assert.rejects(
    registered.forkLevelB(parent, { receipt: receipt! }),
    (error: unknown) =>
      error instanceof IntegrationMaterializationError &&
      error.reason === "invalid_plan",
  );
  const invalidSpec = {
    ...downgraded,
    id: "invalid-spec",
    planLevelBFork() {
      plans += 1;
      return { type: "level_b", spec: {} as RunSpec } as const;
    },
  };
  const invalidSpecRegistered = registerIntegration(client, invalidSpec);
  const invalidSpecReceipt = invalidSpecRegistered
    .createObserver(parent)
    .observe({ type: "output", chunk })[0];
  assert.notEqual(invalidSpecReceipt, undefined);
  await assert.rejects(
    invalidSpecRegistered.forkLevelB(parent, {
      receipt: invalidSpecReceipt!,
    }),
    (error: unknown) =>
      error instanceof IntegrationMaterializationError &&
      error.reason === "invalid_plan",
  );
  assert.equal(forks, 0);
  assert.equal(plans, 2);
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
      registerIntegration(
        {
          start: async () => Promise.reject(),
          fork: async () => Promise.reject(),
        },
        integration,
      ),
    /must not be empty/,
  );

  assert.throws(
    () =>
      registerIntegration(
        {
          start: async () => Promise.reject(),
          fork: async () => Promise.reject(),
        },
        { ...integration, id: "future", apiVersion: 3 as 2 },
      ),
    /unsupported API version 3/,
  );
});

function rootRun(): RunInfo {
  return {
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
      signal: true,
      stop: true,
      fork_level_a: true,
      fork_level_b: true,
      replay: "raw_from_start",
    },
    pid: 123,
    state: { type: "running" },
    latest_output_bytes: 0,
    durable_output_bytes: null,
    first_available_byte: 0,
    attachments: 0,
    applied_input_bytes: 0,
  };
}
