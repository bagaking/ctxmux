import assert from "node:assert/strict";
import { once } from "node:events";
import { mkdtemp, rm } from "node:fs/promises";
import { createServer, type Socket } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createInterface } from "node:readline";
import test from "node:test";
import {
  CtxmuxClient,
  CtxmuxCommandError,
  CtxmuxInvalidFrameError,
  PROTOCOL_VERSION,
  RUNTIME_CAPABILITY_NATIVE_RECOVERABLE_INPUT,
  inputOperationKey,
  type ControlFailure,
  type DiagnosticsSnapshot,
  type NativeServiceFailure,
  type NativeServiceSnapshot,
  type NativeTerminalFaultStage,
  type RunInfo,
} from "../src/index.ts";
import { validateServerFrame } from "../src/validation.ts";
import { runEventSource } from "../src/attachment.ts";

const ID = "018f47f2-9df7-7f5f-8f2d-d3353f114ae8";
function service(): NativeServiceSnapshot {
  return {
    revision: 0,
    owner: { type: "serving" },
    output: { type: "serving" },
    input: {
      phase: { type: "open" },
      unsettled_commands: 0,
      unsettled_request_bytes: 0,
      write_blocked: false,
      completed_input_bytes: 0,
      current_size: { cols: 80, rows: 24 },
      active_confirmed_bytes: 0,
    },
    terminal_fault: null,
  };
}
function run(): RunInfo {
  return {
    id: ID,
    spec: {
      program: "fixture",
      args: [],
      cwd: null,
      env: {},
      initial_size: { cols: 80, rows: 24 },
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
    pid: 42,
    state: { type: "running" },
    native_service: service(),
    latest_output_bytes: 0,
    durable_output_bytes: null,
    first_available_byte: 0,
    attachments: 0,
    applied_input_bytes: 0,
    current_size: { cols: 80, rows: 24 },
  };
}
function event(value: unknown) {
  return { type: "event", event: { type: "service_changed", service: value } };
}
function status(value: unknown) {
  return { type: "response", response: { type: "status", run: value } };
}
const failures: readonly NativeServiceFailure[] = [
  "owner_stopped",
  "owner_unwound",
  "read_failed",
  "write_failed",
  "historical",
  "control_closed",
  { owner_io_failed: { stage: "poll", os_error: 9 } },
  { owner_io_failed: { stage: "wake_drain", os_error: 9 } },
  { owner_io_failed: { stage: "wake_drain", os_error: null } },
];

test("generation 21 requires native service facts, including historical Runs", () => {
  assert.equal(PROTOCOL_VERSION, 21);
  const live = run();
  const missing: Partial<RunInfo> = { ...live };
  delete missing.native_service;
  assert.throws(
    () => validateServerFrame(status(missing)),
    CtxmuxInvalidFrameError,
  );
  assert.throws(
    () => validateServerFrame(status({ ...live, native_service: null })),
    CtxmuxInvalidFrameError,
  );
  const historical = run();
  historical.state = { type: "interrupted", reason: "daemon_restart" };
  historical.applied_input_bytes = null;
  historical.current_size = null;
  historical.native_service = {
    ...service(),
    owner: { type: "stopped", reason: "historical" },
    output: { type: "unavailable", reason: "historical" },
    input: {
      ...service().input,
      phase: { type: "unavailable", reason: "historical" },
      completed_input_bytes: null,
      current_size: null,
    },
  };
  assert.deepEqual(validateServerFrame(status(historical)), status(historical));
  assert.throws(
    () => validateServerFrame(status({ ...historical, native_service: null })),
    CtxmuxInvalidFrameError,
  );
  const tmux = {
    ...historical,
    spec: null,
    backend: {
      type: "tmux",
      socket_path: "fixture.sock",
      server_pid: 1,
      server_started_at: 0,
      session_id: "$1",
      window_id: "@1",
      pane_id: "%1",
      tmux_version: "fixture",
    },
    capabilities: {
      ...historical.capabilities,
      input: false,
      resize: false,
      signal: false,
      stop: false,
      fork_level_a: false,
      fork_level_b: false,
      replay: "raw_since_import",
    },
    native_service: null,
  };
  assert.deepEqual(validateServerFrame(status(tmux)), status(tmux));
  assert.throws(
    () => validateServerFrame(status({ ...tmux, native_service: service() })),
    CtxmuxInvalidFrameError,
  );
});

test("all declared service statuses and failure reasons remain independent facts", () => {
  for (const type of ["starting", "serving", "draining"] as const) {
    validateServerFrame(event({ ...service(), owner: { type } }));
  }
  for (const type of [
    "pending",
    "serving",
    "backpressured",
    "closed",
  ] as const) {
    validateServerFrame(event({ ...service(), output: { type } }));
  }
  for (const type of ["open", "closed"] as const) {
    validateServerFrame(
      event({ ...service(), input: { ...service().input, phase: { type } } }),
    );
  }
  for (const reason of failures) {
    const value = {
      ...service(),
      owner: { type: "stopped", reason },
      output: { type: "unavailable", reason },
      input: { ...service().input, phase: { type: "unavailable", reason } },
    };
    assert.deepEqual(validateServerFrame(event(value)), event(value));
  }
  const stages: readonly NativeTerminalFaultStage[] = [
    "process",
    "resize",
    "export",
    "recovery",
  ];
  for (const stage of stages) {
    const value = {
      ...run(),
      native_service: {
        ...service(),
        terminal_fault: { stage, through_byte: 0 },
      },
    };
    validateServerFrame(status(value));
    assert.equal(value.state.type, "running");
    assert.equal(value.native_service.owner.type, "serving");
    assert.equal(value.native_service.output.type, "serving");
  }
});

test("service validation rejects missing, extra and undeclared state fields", () => {
  const bad: unknown[] = [
    null,
    {},
    { ...service(), owner: { type: "running" } },
    { ...service(), owner: { type: "serving", reason: "owner_stopped" } },
    { ...service(), owner: { type: "stopped" } },
    { ...service(), owner: { type: "stopped", reason: "healthy" } },
    { ...service(), output: { type: "healthy" } },
    { ...service(), input: { ...service().input, phase: "open" } },
    {
      ...service(),
      input: {
        ...service().input,
        phase: { type: "closed", reason: "historical" },
      },
    },
    { ...service(), input: { ...service().input, write_blocked: 1 } },
    { ...service(), terminal_fault: { stage: "parse", through_byte: 0 } },
    {
      ...service(),
      terminal_fault: { stage: "process", through_byte: 0, ignored: true },
    },
    { ...service(), healthy: true },
  ];
  for (const field of Object.keys(service())) {
    const missing: Record<string, unknown> = { ...service() };
    delete missing[field];
    bad.push(missing);
  }
  for (const field of Object.keys(service().input)) {
    const input: Record<string, unknown> = { ...service().input };
    delete input[field];
    bad.push({ ...service(), input });
  }
  for (const value of bad)
    assert.throws(
      () => validateServerFrame(event(value)),
      CtxmuxInvalidFrameError,
    );
});

test("input geometry is required owner evidence, with explicit historical absence", () => {
  validateServerFrame(
    event({ ...service(), input: { ...service().input, current_size: null } }),
  );
  for (const current_size of [
    { rows: 0, cols: 80 },
    { rows: 24, cols: 0 },
    { rows: 24, cols: 0x10000 },
    { rows: 24, cols: 80, healthy: true },
    "80x24",
  ]) {
    assert.throws(
      () =>
        validateServerFrame(
          event({ ...service(), input: { ...service().input, current_size } }),
        ),
      CtxmuxInvalidFrameError,
    );
  }
});

test("every service counter rejects negative, fractional, unsafe and nonnumeric values", () => {
  for (const bad of [
    -1,
    0.5,
    Number.MAX_SAFE_INTEGER + 1,
    NaN,
    Infinity,
    "0",
    null,
  ]) {
    assert.throws(
      () => validateServerFrame(event({ ...service(), revision: bad })),
      CtxmuxInvalidFrameError,
    );
    for (const field of [
      "unsettled_commands",
      "unsettled_request_bytes",
      "completed_input_bytes",
      "active_confirmed_bytes",
    ] as const) {
      if (field === "completed_input_bytes" && bad === null) continue;
      assert.throws(
        () =>
          validateServerFrame(
            event({
              ...service(),
              input: { ...service().input, [field]: bad },
            }),
          ),
        CtxmuxInvalidFrameError,
      );
    }
    assert.throws(
      () =>
        validateServerFrame(
          event({
            ...service(),
            terminal_fault: { stage: "export", through_byte: bad },
          }),
        ),
      CtxmuxInvalidFrameError,
    );
  }
  const maximum = {
    ...service(),
    revision: Number.MAX_SAFE_INTEGER,
    input: {
      ...service().input,
      unsettled_commands: Number.MAX_SAFE_INTEGER,
      unsettled_request_bytes: Number.MAX_SAFE_INTEGER,
      completed_input_bytes: Number.MAX_SAFE_INTEGER,
      active_confirmed_bytes: Number.MAX_SAFE_INTEGER,
    },
    terminal_fault: {
      stage: "recovery",
      through_byte: Number.MAX_SAFE_INTEGER,
    },
  };
  assert.deepEqual(validateServerFrame(event(maximum)), event(maximum));
  validateServerFrame(
    event({
      ...service(),
      input: { ...service().input, completed_input_bytes: null },
    }),
  );
});

function failureFrame(value: unknown) {
  return {
    type: "response",
    response: { type: "control_rejected", failure: value },
  };
}
test("input failure prefix is required, nullable, exact and never successful", () => {
  const failure: ControlFailure = {
    error: { code: "io", message: "PTY stopped accepting input" },
    disposition: "unknown",
    confirmed_input_bytes: 3,
  };
  assert.deepEqual(
    validateServerFrame(failureFrame(failure)),
    failureFrame(failure),
  );
  validateServerFrame(
    failureFrame({ ...failure, confirmed_input_bytes: null }),
  );
  validateServerFrame(
    failureFrame({
      ...failure,
      disposition: "not_applied",
      confirmed_input_bytes: 0,
    }),
  );
  for (const confirmed_input_bytes of [
    -1,
    0.1,
    Number.MAX_SAFE_INTEGER + 1,
    "3",
    undefined,
  ]) {
    assert.throws(
      () =>
        validateServerFrame(
          failureFrame({ ...failure, confirmed_input_bytes }),
        ),
      CtxmuxInvalidFrameError,
    );
  }
  assert.throws(
    () =>
      validateServerFrame(
        failureFrame({ ...failure, disposition: "not_applied" }),
      ),
    CtxmuxInvalidFrameError,
  );
  const missing: Partial<ControlFailure> = { ...failure };
  delete missing.confirmed_input_bytes;
  assert.throws(
    () => validateServerFrame(failureFrame(missing)),
    CtxmuxInvalidFrameError,
  );
  assert.equal(
    new CtxmuxCommandError("io", "response lost", "unknown").failure
      .confirmed_input_bytes,
    null,
  );
});

async function fixture(
  context: test.TestContext,
  request: unknown,
  frames: readonly unknown[],
  protocolGeneration: number = PROTOCOL_VERSION,
  closeAfterFrames = false,
  capabilities: Record<string, number> = {},
) {
  const directory = await mkdtemp(join(tmpdir(), "ctxmux-sdk-service-"));
  const socketPath = join(directory, "ctxmux.sock");
  const sockets = new Set<Socket>();
  const handlers: Promise<void>[] = [];
  const server = createServer((socket) => {
    sockets.add(socket);
    socket.on("error", () => {});
    socket.once("close", () => sockets.delete(socket));
    const handler = (async () => {
      const lines = createInterface({ input: socket, crlfDelay: Infinity });
      const iterator = lines[Symbol.asyncIterator]();
      const receive = async () => {
        const line = await iterator.next();
        assert.equal(line.done, false);
        return JSON.parse(line.value!);
      };
      const send = (value: unknown) =>
        socket.write(JSON.stringify(value) + "\n");
      assert.deepEqual(await receive(), {
        type: "hello",
        hello: { protocol: PROTOCOL_VERSION },
      });
      const receivedBusinessLines: string[] = [];
      const closed = request === null ? once(socket, "close") : undefined;
      if (request === null)
        lines.on("line", (line) => receivedBusinessLines.push(line));
      send({
        type: "hello",
        runtime: {
          runtimeId: ID,
          daemonInstanceId: ID,
          runtimeIdPersistence: "daemon",
          buildId: "ctxmuxd/fixture",
          protocolGeneration,
          platform: "fixture",
          arch: "fixture",
          capabilities,
        },
      });
      if (request === null) {
        await closed;
        assert.deepEqual(
          receivedBusinessLines,
          [],
          "incompatible Hello must close without a business request",
        );
        return;
      }
      assert.deepEqual(await receive(), { type: "request", request });
      for (const frame of frames) send(frame);
      if (closeAfterFrames) socket.end();
    })();
    handlers.push(handler);
    void handler.catch(() => socket.destroy());
  });
  server.listen(socketPath);
  await once(server, "listening");
  context.after(async () => {
    const closed = once(server, "close");
    server.close();
    for (const socket of sockets) socket.destroy();
    await closed;
    try {
      await Promise.all(handlers);
      assert.equal(
        handlers.length,
        1,
        "the SDK must not create an automatic retry connection",
      );
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  });
  return new CtxmuxClient({ socketPath });
}

test("public SDK retains ordered service changes, output and separate child exit", async (context) => {
  const changed: NativeServiceSnapshot = {
    ...service(),
    revision: 1,
    owner: { type: "stopped", reason: "owner_unwound" },
    input: {
      ...service().input,
      phase: { type: "unavailable", reason: "control_closed" },
    },
    terminal_fault: { stage: "process", through_byte: 0 },
  };
  const snapshot = {
    run: run(),
    replay: {
      first_available_byte: 0,
      latest_output_bytes: 0,
      truncated: false,
    },
    terminal: { type: "not_requested" },
    resize_revision: 0,
  };
  const client = await fixture(
    context,
    { type: "attach", id: ID, after_byte: 0, view: "raw" },
    [
      { type: "attached", snapshot },
      event(changed),
      {
        type: "event",
        event: {
          type: "output",
          chunk: {
            start_byte: 0,
            end_byte: 2,
            data: Buffer.from([0xff, 0]).toString("base64"),
          },
        },
      },
      {
        type: "event",
        event: {
          type: "exited",
          state: { type: "exited", code: 0, signal: null },
        },
      },
    ],
  );
  const attached = await client.attach(ID);
  try {
    const first = await attached.nextEvent();
    assert.deepEqual(first, { type: "service_changed", service: changed });
    assert.equal(runEventSource(first!), ID);
    assert.equal(attached.snapshot.run.state.type, "running");
    const output = await attached.nextEvent();
    assert.deepEqual(output, {
      type: "output",
      chunk: { start_byte: 0, end_byte: 2, data: new Uint8Array([0xff, 0]) },
    });
    assert.deepEqual(await attached.nextEvent(), {
      type: "exited",
      state: { type: "exited", code: 0, signal: null },
    });
    assert.equal(await attached.nextEvent(), undefined);
  } finally {
    attached.close();
  }
});

test("generation 19 Hello cannot dispatch a current-generation business request", async (context) => {
  const client = await fixture(context, null, [], 19);
  await assert.rejects(client.input(ID, "abcd"), /compatible hello/);
});

test("previous-generation Hello cannot dispatch a current-generation business request", async (context) => {
  const client = await fixture(context, null, [], PROTOCOL_VERSION - 1);
  await assert.rejects(client.input(ID, "abcd"), /compatible hello/);
});

test("owner syscall facts reject missing, extra, unknown and invalid errno fields", () => {
  const bad: unknown[] = [
    { owner_io_failed: { stage: "poll" } },
    { owner_io_failed: { os_error: 1 } },
    { owner_io_failed: { stage: "unknown", os_error: 1 } },
    { owner_io_failed: { stage: "poll", os_error: 1, healthy: true } },
    { owner_io_failed: { stage: "poll", os_error: 1 }, healthy: true },
    ...[0, -1, 0x8000_0000, 1.5, "1", Number.NaN, Number.POSITIVE_INFINITY].map(
      (os_error) => ({ owner_io_failed: { stage: "poll", os_error } }),
    ),
  ];
  for (const reason of bad) {
    for (const value of [
      { ...service(), owner: { type: "stopped", reason } },
      { ...service(), output: { type: "unavailable", reason } },
      {
        ...service(),
        input: { ...service().input, phase: { type: "unavailable", reason } },
      },
    ])
      assert.throws(
        () => validateServerFrame(event(value)),
        CtxmuxInvalidFrameError,
      );
  }
});

test("public SDK exposes confirmed failure prefix without retrying input", async (context) => {
  const failure: ControlFailure = {
    error: { code: "io", message: "partial native input" },
    disposition: "unknown",
    confirmed_input_bytes: 3,
  };
  const client = await fixture(
    context,
    { type: "input", id: ID, data: [97, 98, 99, 100] },
    [failureFrame(failure)],
  );
  await assert.rejects(client.input(ID, "abcd"), (error: unknown) => {
    assert(error instanceof CtxmuxCommandError);
    assert.equal(error.disposition, "unknown");
    assert.equal(error.confirmedInputBytes, 3);
    assert.deepEqual(error.failure, failure);
    return true;
  });
});

test("public SDK reports no prefix authority after input response loss", async (context) => {
  const client = await fixture(
    context,
    { type: "input", id: ID, data: [255, 0] },
    [],
    PROTOCOL_VERSION,
    true,
  );
  await assert.rejects(
    client.input(ID, new Uint8Array([255, 0])),
    (error: unknown) => {
      assert(error instanceof CtxmuxCommandError);
      assert.equal(error.disposition, "unknown");
      assert.equal(error.confirmedInputBytes, null);
      assert.equal(error.failure.confirmed_input_bytes, null);
      return true;
    },
  );
});

function diagnosticFacts(): DiagnosticsSnapshot {
  return {
    queue_budget_bytes: 4096,
    record_limit_bytes: 1024,
    funded_bytes: 21,
    formatting_records: 0,
    queued_records: 1,
    active_record_bytes: 5,
    admitted_records: 2,
    written_records: 0,
    written_bytes: 0,
    notice_written_bytes: 0,
    dropped_before_encoding_records: 1,
    dropped_encoded_records: 0,
    discarded_encoded_bytes: 0,
    oversized_records: 0,
    format_failed_records: 0,
    sink_write_failures: 0,
    initialization_failures: 0,
    partially_written_records: 0,
    scoped_panics: 0,
    sink: "writing",
    last_sink_errno: null,
    writer_alive: true,
    shutdown_requested: false,
    counters_saturated: false,
  };
}
function diagnosticFrame(diagnostics: unknown) {
  return { type: "response", response: { type: "diagnostics", diagnostics } };
}
test("diagnostic observations require every fact and never invent atomic equality", () => {
  const facts = diagnosticFacts();
  assert.deepEqual(
    validateServerFrame(diagnosticFrame(facts)),
    diagnosticFrame(facts),
  );
  for (const key of Object.keys(facts)) {
    const missing = { ...facts } as Record<string, unknown>;
    delete missing[key];
    assert.throws(
      () => validateServerFrame(diagnosticFrame(missing)),
      CtxmuxInvalidFrameError,
      key,
    );
  }
  for (const value of [-1, 0.5, Number.MAX_SAFE_INTEGER + 1, "0", null]) {
    assert.throws(
      () =>
        validateServerFrame(
          diagnosticFrame({ ...facts, discarded_encoded_bytes: value }),
        ),
      CtxmuxInvalidFrameError,
    );
  }
  for (const sink of ["healthy", "full", null])
    assert.throws(
      () => validateServerFrame(diagnosticFrame({ ...facts, sink })),
      CtxmuxInvalidFrameError,
    );
  for (const last_sink_errno of [0, -1, 0.1, 0x80000000, "32"])
    assert.throws(
      () => validateServerFrame(diagnosticFrame({ ...facts, last_sink_errno })),
      CtxmuxInvalidFrameError,
    );
  assert.throws(
    () => validateServerFrame(diagnosticFrame({ ...facts, extra: true })),
    CtxmuxInvalidFrameError,
  );
  // Concurrent samples may observe a completed write before its record count,
  // or writer exit before the sink state. No false atomic ledger is required.
  validateServerFrame(
    diagnosticFrame({
      ...facts,
      written_bytes: 100,
      admitted_records: 0,
      writer_alive: false,
    }),
  );
  validateServerFrame(
    diagnosticFrame({
      ...facts,
      sink: "failed",
      initialization_failures: 1,
      last_sink_errno: 24,
      writer_alive: false,
    }),
  );
  assert.throws(
    () =>
      validateServerFrame(
        diagnosticFrame({ ...facts, sink: "not_initialized" }),
      ),
    CtxmuxInvalidFrameError,
  );
});
test("public diagnostics getter preserves blocked sink facts without retry or Run-health inference", async (context) => {
  const facts = diagnosticFacts();
  const client = await fixture(context, { type: "diagnostics" }, [
    diagnosticFrame(facts),
  ]);
  assert.deepEqual(await client.diagnostics(), facts);
});
test("public diagnostics getter rejects omitted initialization outcome", async (context) => {
  const facts: Partial<DiagnosticsSnapshot> = diagnosticFacts();
  delete facts.initialization_failures;
  const client = await fixture(context, { type: "diagnostics" }, [
    diagnosticFrame(facts),
  ]);
  await assert.rejects(client.diagnostics(), CtxmuxInvalidFrameError);
});

test("recoverable lookup pressure preserves Unknown without replaying the request", async (context) => {
  const operation = {
    daemonInstance: ID,
    runId: ID,
    operationKey: inputOperationKey("retry-busy"),
    expectedByte: 42,
    data: new Uint8Array([97]),
  };
  const failure: ControlFailure = {
    error: {
      code: "control_backpressure",
      message: "recoverable ledger currently busy",
    },
    disposition: "unknown",
    confirmed_input_bytes: null,
  };
  const client = await fixture(
    context,
    {
      type: "recoverable_input",
      operation: {
        daemon_instance: ID,
        id: ID,
        operation_key: "retry-busy",
        expected_byte: 42,
        data: [97],
      },
    },
    [failureFrame(failure)],
    PROTOCOL_VERSION,
    false,
    { [RUNTIME_CAPABILITY_NATIVE_RECOVERABLE_INPUT]: 1 },
  );
  await assert.rejects(
    client.recoverableInput(operation),
    (error: unknown) =>
      error instanceof CtxmuxCommandError &&
      error.code === "control_backpressure" &&
      error.disposition === "unknown" &&
      error.confirmedInputBytes === null,
  );
});
