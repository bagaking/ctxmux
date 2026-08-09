import assert from "node:assert/strict";
import { once } from "node:events";
import { readFileSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { createServer, Socket, type Server } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createInterface } from "node:readline";
import test from "node:test";

import {
  CtxmuxClient,
  CtxmuxCommandError,
  CtxmuxInvalidFrameError,
  MAX_FRAME_BYTES,
  PROTOCOL_VERSION,
  inputOperationKey,
  type OutputChunk,
  type ServerFrame,
} from "../src/index.ts";
import { runEventSource } from "../src/attachment.ts";
import { validateServerFrame } from "../src/validation.ts";
import { JsonLinesConnection, WireClosedError } from "../src/wire.ts";

const RUN_ID = "018f47f2-9df7-7f5f-8f2d-d3353f114ae8";
const DAEMON_INSTANCE = "018f47f2-9df7-7f5f-8f2d-d3353f114ae9";
const MALFORMED_PROTOCOL_FRAMES = (
  JSON.parse(
    readFileSync(
      new URL(
        "../../../fixtures/malformed-protocol-frames.json",
        import.meta.url,
      ),
      "utf8",
    ),
  ) as {
    readonly version: number;
    readonly frames: readonly {
      readonly id: string;
      readonly bytes: readonly number[];
    }[];
  }
).frames.map(({ id, bytes }) => ({ id, bytes: Buffer.from(bytes) }));

test("SC-01 rejects unsafe u64 cursors before replay", async (context) => {
  const safeFrame = {
    type: "event",
    event: { type: "gap", latest_output_bytes: Number.MAX_SAFE_INTEGER },
  } satisfies ServerFrame;
  assert.equal(validateServerFrame(safeFrame), safeFrame);

  const rounded = JSON.parse(
    '{"type":"event","event":{"type":"gap","latest_output_bytes":9007199254740993}}',
  ) as unknown;
  assert.throws(
    () => validateServerFrame(rounded),
    (error: unknown) =>
      error instanceof CtxmuxInvalidFrameError &&
      error.path === "$frame.event.latest_output_bytes",
  );

  const daemon = await mockDaemon(context, async (socket) => {
    const peer = new MockPeer(socket);
    await peer.handshake();
    const frame = await peer.receive();
    assert.deepEqual(frame, {
      type: "request",
      request: {
        type: "attach",
        id: RUN_ID,
        after_byte: Number.MAX_SAFE_INTEGER,
      },
    });
    peer.send({
      type: "attached",
      snapshot: attachedHeader(Number.MAX_SAFE_INTEGER),
    });
  });

  const client = new CtxmuxClient({ socketPath: daemon.socketPath });
  const attachment = await client.attach(RUN_ID, Number.MAX_SAFE_INTEGER);
  assert.equal(
    attachment.snapshot.replay.latest_output_bytes,
    Number.MAX_SAFE_INTEGER,
  );
  attachment.close();

  await assert.rejects(
    client.attach(RUN_ID, Number.MAX_SAFE_INTEGER + 1),
    (error: unknown) =>
      error instanceof CtxmuxInvalidFrameError && error.path === "afterByte",
  );
});

test("SC-02 rejects malformed nested runtime frames", () => {
  const mutations: readonly [unknown, string][] = [
    [{ type: "invented" }, "$frame.type"],
    [
      {
        type: "hello",
        protocol: PROTOCOL_VERSION,
        daemon_instance: "not-a-uuid",
      },
      "$frame.daemon_instance",
    ],
    [
      { type: "response", response: { type: "invented" } },
      "$frame.response.type",
    ],
    [
      { type: "response", response: { type: "started", run: null } },
      "$frame.response.run",
    ],
    [
      { type: "response", response: { type: "runs", runs: {} } },
      "$frame.response.runs",
    ],
    [
      {
        type: "response",
        response: { type: "started", run: { ...runInfo(), id: 7 } },
      },
      "$frame.response.run.id",
    ],
    [
      {
        type: "response",
        response: { type: "started", run: { ...runInfo(), id: "invalid" } },
      },
      "$frame.response.run.id",
    ],
    [
      {
        type: "response",
        response: { type: "started", run: { ...runInfo(), pid: -1 } },
      },
      "$frame.response.run.pid",
    ],
    [
      {
        type: "response",
        response: {
          type: "started",
          run: { ...runInfo(), durable_output_bytes: -1 },
        },
      },
      "$frame.response.run.durable_output_bytes",
    ],
    [
      {
        type: "attached",
        snapshot: {
          ...attachedHeader(),
          replay: { ...attachedHeader().replay, truncated: "false" },
        },
      },
      "$frame.snapshot.replay.truncated",
    ],
    [
      {
        type: "attached",
        snapshot: {
          ...attachedHeader(),
          replay: { ...attachedHeader().replay, first_available_byte: "0" },
        },
      },
      "$frame.snapshot.replay.first_available_byte",
    ],
    [
      {
        type: "attached",
        snapshot: {
          ...attachedHeader(),
          replay: { ...attachedHeader().replay, chunks: [] },
        },
      },
      "$frame.snapshot.replay.chunks",
    ],
    [
      {
        type: "event",
        event: {
          type: "output",
          chunk: { start_byte: 0, end_byte: 2, data: [0, 256] },
        },
      },
      "$frame.event.chunk.data[1]",
    ],
    [
      {
        type: "event",
        event: {
          type: "exited",
          state: { type: "exited", code: 0, signal: 9 },
        },
      },
      "$frame.event.state.signal",
    ],
    [
      { type: "event", event: { type: "exited", state: { type: "invented" } } },
      "$frame.event.state.type",
    ],
    [
      {
        type: "event",
        event: { type: "interrupted", reason: "invented" },
      },
      "$frame.event.reason",
    ],
    [
      {
        type: "command_result",
        command_id: 1,
        outcome: {
          type: "accepted",
          receipt: { type: "resize", applied_size: { cols: 0, rows: 24 } },
        },
      },
      "$frame.outcome.receipt.applied_size.cols",
    ],
    [
      {
        type: "response",
        response: {
          type: "forked",
          run: {
            ...runInfo(),
            lineage: { parent: RUN_ID, fidelity: "invented" },
          },
        },
      },
      "$frame.response.run.lineage.fidelity",
    ],
    [
      {
        type: "response",
        response: {
          type: "started",
          run: {
            ...runInfo(),
            spec: {
              ...runInfo().spec,
              declared_inputs: [{ kind: "invented", reference: "x" }],
            },
          },
        },
      },
      "$frame.response.run.spec.declared_inputs[0].kind",
    ],
    [{ type: "event", event: { type: "invented" } }, "$frame.event.type"],
    [
      {
        type: "command_result",
        command_id: 0,
        outcome: { type: "accepted", receipt: { type: "stop" } },
      },
      "$frame.command_id",
    ],
    [
      {
        type: "command_result",
        command_id: 1,
        outcome: {
          type: "accepted",
          receipt: { type: "input", written_bytes: 0x1_0000_0000 },
        },
      },
      "$frame.outcome.receipt.written_bytes",
    ],
    [
      {
        type: "response",
        response: {
          type: "control_rejected",
          failure: {
            error: { code: "control_backpressure", message: "full" },
            disposition: "unknown",
          },
        },
      },
      "$frame.response.failure.disposition",
    ],
    [
      {
        type: "response",
        response: {
          type: "input_applied",
          run: { ...runInfo(), applied_input_bytes: 3 },
          range: { start_byte: Number.MAX_SAFE_INTEGER + 1, end_byte: 3 },
        },
      },
      "$frame.response.range.start_byte",
    ],
    [
      {
        type: "response",
        response: {
          type: "input_applied",
          run: { ...runInfo(), applied_input_bytes: 3 },
          range: { start_byte: 3, end_byte: 3 },
        },
      },
      "$frame.response.range.end_byte",
    ],
    [
      { type: "response", response: { type: "accepted", run: runInfo() } },
      "$frame.response.type",
    ],
    [
      { type: "error", error: { code: "other", message: "no" } },
      "$frame.error.code",
    ],
  ];

  for (const [mutation, expectedPath] of mutations) {
    assert.throws(
      () => validateServerFrame(mutation),
      (error: unknown) =>
        error instanceof CtxmuxInvalidFrameError && error.path === expectedPath,
      JSON.stringify(mutation),
    );
  }
});

test("SC-02 accepts TypeScript-authored server variants and rejects mutations", () => {
  const frames: readonly ServerFrame[] = [
    {
      type: "hello",
      protocol: PROTOCOL_VERSION,
      daemon_instance: DAEMON_INSTANCE,
    },
    { type: "response", response: { type: "started", run: runInfo() } },
    {
      type: "response",
      response: {
        type: "forked",
        run: {
          ...runInfo(),
          lineage: { parent: RUN_ID, fidelity: "level_a" },
        },
      },
    },
    { type: "response", response: { type: "runs", runs: [runInfo()] } },
    { type: "response", response: { type: "status", run: runInfo() } },
    {
      type: "response",
      response: {
        type: "control_accepted",
        run: runInfo(),
        receipt: { type: "input", written_bytes: 3 },
      },
    },
    {
      type: "response",
      response: {
        type: "control_rejected",
        failure: {
          error: { code: "control_backpressure", message: "full" },
          disposition: "not_applied",
        },
      },
    },
    {
      type: "response",
      response: {
        type: "input_applied",
        run: { ...runInfo(), applied_input_bytes: 3 },
        range: { start_byte: 1, end_byte: 3 },
      },
    },
    { type: "attached", snapshot: attachedHeader() },
    {
      type: "event",
      event: {
        type: "output",
        chunk: { start_byte: 0, end_byte: 3, data: [0, 10, 255] },
      },
    },
    {
      type: "event",
      event: {
        type: "exited",
        state: { type: "exited", code: 7, signal: null },
      },
    },
    {
      type: "event",
      event: { type: "interrupted", reason: "daemon_restart" },
    },
    {
      type: "event",
      event: { type: "interrupted", reason: "tmux_protocol_error" },
    },
    { type: "event", event: { type: "gap", latest_output_bytes: 1 } },
    {
      type: "command_result",
      command_id: 1,
      outcome: {
        type: "accepted",
        receipt: {
          type: "resize",
          applied_size: { cols: 100, rows: 30 },
        },
      },
    },
    {
      type: "command_result",
      command_id: 2,
      outcome: {
        type: "rejected",
        failure: {
          error: { code: "invalid_run_state", message: "terminal" },
          disposition: "not_applied",
        },
      },
    },
    { type: "detached" },
    {
      type: "error",
      error: { code: "invalid_request", message: "invalid fixture" },
    },
    {
      type: "error",
      error: { code: "run_capacity", message: "retained Run capacity" },
    },
  ];

  for (const frame of frames) {
    assert.deepEqual(validateServerFrame(structuredClone(frame)), frame);
  }

  const malformedHello = { type: "hello", protocol: 1.5 };
  assert.throws(() => validateServerFrame(malformedHello), {
    name: "CtxmuxInvalidFrameError",
  });
});

test("T-004 rejects recoverable Input receipts that do not prove the request", async (context) => {
  for (const response of [
    {
      type: "input_applied" as const,
      run: {
        ...runInfo(),
        id: "018f47f2-9df7-7f5f-8f2d-d3353f114aea",
        applied_input_bytes: 2,
      },
      range: { start_byte: 1, end_byte: 2 },
    },
    {
      type: "input_applied" as const,
      run: { ...runInfo(), applied_input_bytes: 1 },
      range: { start_byte: 1, end_byte: 2 },
    },
  ]) {
    const daemon = await mockDaemon(context, async (socket) => {
      const peer = new MockPeer(socket);
      await peer.handshake();
      assert.deepEqual(await peer.receive(), {
        type: "request",
        request: {
          type: "recoverable_input",
          operation: {
            daemon_instance: DAEMON_INSTANCE,
            operation_key: "receipt-proof",
            id: RUN_ID,
            expected_byte: 1,
            data: [65],
          },
        },
      });
      peer.send({ type: "response", response });
    });

    await assert.rejects(
      new CtxmuxClient({ socketPath: daemon.socketPath }).recoverableInput({
        daemonInstance: DAEMON_INSTANCE,
        operationKey: inputOperationKey("receipt-proof"),
        runId: RUN_ID,
        expectedByte: 1,
        data: "A",
      }),
      (error: unknown) =>
        error instanceof CtxmuxCommandError &&
        error.code === "internal" &&
        error.disposition === "unknown",
    );
  }
});

test("SC-02 validates tmux-owned and interrupted Run wire contracts", () => {
  const interruptionReasons = [
    "daemon_restart",
    "tmux_server_unavailable",
    "tmux_target_changed",
    "tmux_protocol_error",
  ] as const;
  const frames: readonly ServerFrame[] = [
    {
      type: "response",
      response: {
        type: "tmux_panes",
        tmux_version: "3.6b",
        panes: [tmuxPaneInfo()],
      },
    },
    {
      type: "response",
      response: { type: "imported", run: tmuxRunInfo() },
    },
    {
      type: "response",
      response: {
        type: "runs",
        runs: interruptionReasons.map((reason) => ({
          ...runInfo(),
          pid: null,
          state: { type: "interrupted" as const, reason },
        })),
      },
    },
    {
      type: "event",
      event: {
        type: "tmux",
        event: { type: "session_renamed", name: [0, 65, 255] },
      },
    },
    { type: "event", event: { type: "tmux", event: { type: "paused" } } },
    {
      type: "event",
      event: { type: "tmux", event: { type: "continued" } },
    },
  ];

  for (const frame of frames) {
    assert.deepEqual(validateServerFrame(structuredClone(frame)), frame);
  }

  const native = runInfo();
  const tmux = tmuxRunInfo();
  const pane = tmuxPaneInfo();
  const mutations: readonly [unknown, string][] = [
    [
      {
        type: "event",
        event: {
          type: "exited",
          state: { type: "interrupted", reason: "daemon_restart" },
        },
      },
      "$frame.event.state.type",
    ],
    [
      { type: "event", event: { type: "tmux", event: { type: "invented" } } },
      "$frame.event.event.type",
    ],
    [
      {
        type: "response",
        response: { type: "started", run: { ...native, spec: null } },
      },
      "$frame.response.run.spec",
    ],
    [
      {
        type: "response",
        response: {
          type: "imported",
          run: { ...tmux, spec: native.spec },
        },
      },
      "$frame.response.run.spec",
    ],
    [
      {
        type: "response",
        response: {
          type: "started",
          run: {
            ...native,
            capabilities: { ...native.capabilities, input: false },
          },
        },
      },
      "$frame.response.run.capabilities",
    ],
    [
      {
        type: "response",
        response: {
          type: "imported",
          run: {
            ...tmux,
            capabilities: {
              ...tmux.capabilities,
              replay: "raw_from_start",
            },
          },
        },
      },
      "$frame.response.run.capabilities",
    ],
    [
      {
        type: "response",
        response: {
          type: "started",
          run: { ...native, backend: { type: "invented" } },
        },
      },
      "$frame.response.run.backend.type",
    ],
    [
      {
        type: "response",
        response: {
          type: "imported",
          run: {
            ...tmux,
            backend: { ...tmux.backend, pane_id: "@56" },
          },
        },
      },
      "$frame.response.run.backend.pane_id",
    ],
    [
      {
        type: "response",
        response: {
          type: "tmux_panes",
          tmux_version: "3.6b",
          panes: [{ ...pane, session_id: "12" }],
        },
      },
      "$frame.response.panes[0].session_id",
    ],
    [
      {
        type: "response",
        response: {
          type: "status",
          run: { ...native, attachments: Number.MAX_SAFE_INTEGER + 1 },
        },
      },
      "$frame.response.run.attachments",
    ],
  ];

  for (const [mutation, expectedPath] of mutations) {
    assert.throws(
      () => validateServerFrame(mutation),
      (error: unknown) =>
        error instanceof CtxmuxInvalidFrameError && error.path === expectedPath,
      JSON.stringify(mutation),
    );
  }
});

test("LP-03 rejects malformed UTF-8, duplicate members, and invalid JSON", async (context) => {
  for (const { id, bytes } of MALFORMED_PROTOCOL_FRAMES) {
    const daemon = await mockDaemon(context, async (socket) => {
      const peer = new MockPeer(socket);
      assert.deepEqual(await peer.receive(), {
        type: "hello",
        hello: { protocol: PROTOCOL_VERSION },
      });
      peer.sendRaw(Buffer.concat([bytes, Buffer.from("\n")]));
    });
    await assert.rejects(
      new CtxmuxClient({ socketPath: daemon.socketPath }).ping(),
      (error: unknown) => error instanceof SyntaxError,
      `shared malformed frame ${id} was accepted`,
    );
  }
});

test("LP-02 enforces the exact frame ceiling with and without a delimiter", async () => {
  const exactSocket = new Socket();
  const exactWire = new JsonLinesConnection(exactSocket);
  const exactValue = "x".repeat(MAX_FRAME_BYTES - 2);
  const exactFrame = Buffer.from(`${JSON.stringify(exactValue)}\n`);
  assert.equal(exactFrame.length - 1, MAX_FRAME_BYTES);
  exactSocket.emit("data", exactFrame);
  assert.equal(await exactWire.receive(), exactValue);
  exactWire.close();

  for (const newline of [true, false]) {
    const socket = new Socket();
    const wire = new JsonLinesConnection(socket);
    const receive = wire.receive();
    const bytes = Buffer.alloc(MAX_FRAME_BYTES + 1, 0x78);
    socket.emit(
      "data",
      newline ? Buffer.concat([bytes, Buffer.from("\n")]) : bytes,
    );
    await assert.rejects(
      settleWithin(receive, 1_000),
      RangeError,
      `one-byte-oversize frame with newline=${newline} did not fail boundedly`,
    );
  }
});

test("LP-02 reassembles retained replay streamed across bounded frames", async (context) => {
  const daemon = await mockDaemon(context, async (socket) => {
    const peer = new MockPeer(socket);
    await peer.handshake();
    assert.deepEqual(await peer.receive(), {
      type: "request",
      request: { type: "attach", id: RUN_ID, after_byte: 0 },
    });
    peer.send({
      type: "attached",
      snapshot: {
        ...attachedHeader(5),
        replay: {
          first_available_byte: 0,
          latest_output_bytes: 5,
          truncated: false,
        },
      },
    });
    peer.send({
      type: "event",
      event: {
        type: "output",
        chunk: { start_byte: 0, end_byte: 2, data: [0, 255] },
      },
    });
    peer.send({
      type: "event",
      event: {
        type: "output",
        chunk: { start_byte: 2, end_byte: 5, data: [1, 2, 3] },
      },
    });
  });

  const attachment = await new CtxmuxClient({
    socketPath: daemon.socketPath,
  }).attach(RUN_ID);
  assert.deepEqual(attachment.snapshot.replay.chunks, [
    { start_byte: 0, end_byte: 2, data: [0, 255] },
    { start_byte: 2, end_byte: 5, data: [1, 2, 3] },
  ]);
  attachment.close();
});

test("LP-02 rejects replay overshoot, non-progress, and EOF before the advertised byte boundary", async (context) => {
  for (const [label, chunk] of [
    ["overshoot", { start_byte: 0, end_byte: 2, data: [1, 2] }],
    ["non-progress", { start_byte: 0, end_byte: 0, data: [] }],
  ] satisfies ReadonlyArray<readonly [string, OutputChunk]>) {
    const daemon = await mockDaemon(context, async (socket) => {
      const peer = new MockPeer(socket);
      await peer.handshake();
      await peer.receive();
      peer.send({
        type: "attached",
        snapshot: {
          ...attachedHeader(1),
          replay: {
            first_available_byte: 0,
            latest_output_bytes: 1,
            truncated: false,
          },
        },
      });
      peer.send({ type: "event", event: { type: "output", chunk } });
    });
    await assert.rejects(
      settleWithin(
        new CtxmuxClient({ socketPath: daemon.socketPath }).attach(RUN_ID),
        1_000,
      ),
      label === "overshoot"
        ? /expected ordered replay output/u
        : (error: unknown) =>
            error instanceof CtxmuxInvalidFrameError &&
            error.path === "$frame.event.chunk",
      label,
    );
  }

  const eof = await mockDaemon(context, async (socket) => {
    const peer = new MockPeer(socket);
    await peer.handshake();
    await peer.receive();
    peer.send({
      type: "attached",
      snapshot: {
        ...attachedHeader(1),
        replay: {
          first_available_byte: 0,
          latest_output_bytes: 1,
          truncated: false,
        },
      },
    });
    socket.end();
  });
  await assert.rejects(
    settleWithin(
      new CtxmuxClient({ socketPath: eof.socketPath }).attach(RUN_ID),
      1_000,
    ),
    WireClosedError,
  );
});

test("LP-02 settles a truncated empty replay without inventing a byte range", async (context) => {
  const daemon = await mockDaemon(context, async (socket) => {
    const peer = new MockPeer(socket);
    await peer.handshake();
    assert.deepEqual(await peer.receive(), {
      type: "request",
      request: { type: "attach", id: RUN_ID, after_byte: 0 },
    });
    peer.send({
      type: "attached",
      snapshot: {
        ...attachedHeader(),
        replay: {
          first_available_byte: 0,
          latest_output_bytes: 0,
          truncated: true,
        },
      },
    });
  });

  const attachment = await settleWithin(
    new CtxmuxClient({ socketPath: daemon.socketPath }).attach(RUN_ID),
    1_000,
  );
  assert.equal(attachment.snapshot.replay.truncated, true);
  assert.deepEqual(attachment.snapshot.replay.chunks, []);
  attachment.close();
});

test("LP-02 resumes after a retained source gap from the caller cursor", async (context) => {
  const daemon = await mockDaemon(context, async (socket) => {
    const peer = new MockPeer(socket);
    await peer.handshake();
    assert.deepEqual(await peer.receive(), {
      type: "request",
      request: { type: "attach", id: RUN_ID, after_byte: 2 },
    });
    peer.send({
      type: "attached",
      snapshot: {
        ...attachedHeader(3),
        replay: {
          first_available_byte: 1,
          latest_output_bytes: 3,
          truncated: true,
        },
      },
    });
    peer.send({
      type: "event",
      event: {
        type: "output",
        chunk: { start_byte: 2, end_byte: 3, data: [3] },
      },
    });
  });

  const attachment = await settleWithin(
    new CtxmuxClient({ socketPath: daemon.socketPath }).attach(RUN_ID, 2),
    1_000,
  );
  assert.equal(attachment.snapshot.replay.truncated, true);
  assert.deepEqual(attachment.snapshot.replay.chunks, [
    { start_byte: 2, end_byte: 3, data: [3] },
  ]);
  attachment.close();
});

test("LP-03 fails closed after a malformed coalesced frame", async () => {
  for (const delivery of ["queued", "waiting"] as const) {
    const socket = new Socket();
    const wire = new JsonLinesConnection(socket);
    const payload = Buffer.from('{"broken":,}\n{"valid":true}\n');

    if (delivery === "queued") {
      socket.emit("data", payload);
    }
    const receives = [wire.receive(), wire.receive()] as const;
    if (delivery === "waiting") {
      socket.emit("data", payload);
    }

    const [first, second] = await Promise.allSettled(receives);
    assert.equal(first.status, "rejected", delivery);
    assert.equal(second.status, "rejected", delivery);
    if (first.status === "rejected" && second.status === "rejected") {
      assert.ok(first.reason instanceof SyntaxError, delivery);
      assert.equal(
        second.reason,
        first.reason,
        `${delivery} delivered data after the terminal parse error`,
      );
    }
  }
});

test("T-013 correlates out-of-order attachment controls with typed receipts", async (context) => {
  const daemon = await mockDaemon(context, async (socket) => {
    const peer = new MockPeer(socket);
    await peer.handshake();
    await peer.receive();
    peer.send({ type: "attached", snapshot: attachedHeader() });
    assert.deepEqual(await peer.receive(), {
      type: "input",
      command_id: 1,
      data: [104, 105],
    });
    assert.deepEqual(await peer.receive(), {
      type: "resize",
      command_id: 2,
      size: { cols: 100, rows: 30 },
    });
    assert.deepEqual(await peer.receive(), { type: "stop", command_id: 3 });
    peer.send({
      type: "command_result",
      command_id: 2,
      outcome: {
        type: "accepted",
        receipt: {
          type: "resize",
          applied_size: { cols: 99, rows: 29 },
        },
      },
    });
    peer.send({
      type: "command_result",
      command_id: 1,
      outcome: {
        type: "accepted",
        receipt: { type: "input", written_bytes: 2 },
      },
    });
    peer.send({
      type: "command_result",
      command_id: 3,
      outcome: {
        type: "rejected",
        failure: {
          error: { code: "io", message: "owner result lost" },
          disposition: "unknown",
        },
      },
    });
  });

  const attachment = await new CtxmuxClient({
    socketPath: daemon.socketPath,
  }).attach(RUN_ID);
  const input = attachment.input("hi");
  const resize = attachment.resize({ cols: 100, rows: 30 });
  const stop = attachment.stop();
  assert.deepEqual(await resize, {
    commandId: 2,
    receipt: {
      type: "resize",
      applied_size: { cols: 99, rows: 29 },
    },
  });
  assert.deepEqual(await input, {
    commandId: 1,
    receipt: { type: "input", written_bytes: 2 },
  });
  await assert.rejects(
    stop,
    (error: unknown) =>
      error instanceof CtxmuxCommandError &&
      error.code === "io" &&
      error.disposition === "unknown" &&
      error.commandId === 3,
  );
  attachment.close();
});

test("T-013 makes every pending command unknown on a correlated receipt violation", async (context) => {
  const daemon = await mockDaemon(context, async (socket) => {
    const peer = new MockPeer(socket);
    await peer.handshake();
    await peer.receive();
    peer.send({ type: "attached", snapshot: attachedHeader() });
    await peer.receive();
    await peer.receive();
    peer.send({
      type: "command_result",
      command_id: 1,
      outcome: { type: "accepted", receipt: { type: "stop" } },
    });
  });

  const attachment = await new CtxmuxClient({
    socketPath: daemon.socketPath,
  }).attach(RUN_ID);
  const input = attachment.input("x");
  const resize = attachment.resize({ cols: 80, rows: 24 });
  for (const [promise, commandId] of [
    [input, 1],
    [resize, 2],
  ] as const) {
    await assert.rejects(
      promise,
      (error: unknown) =>
        error instanceof CtxmuxCommandError &&
        error.disposition === "unknown" &&
        error.commandId === commandId,
    );
  }
  await assert.rejects(
    attachment.nextEvent(),
    (error: unknown) => error instanceof CtxmuxInvalidFrameError,
  );
});

test("T-013 reserves attachment capacity for resize and stop after 32 pending inputs", async (context) => {
  const daemon = await mockDaemon(context, async (socket) => {
    const peer = new MockPeer(socket);
    await peer.handshake();
    await peer.receive();
    peer.send({ type: "attached", snapshot: attachedHeader() });
    for (let commandId = 1; commandId <= 32; commandId += 1) {
      assert.deepEqual(await peer.receive(), {
        type: "input",
        command_id: commandId,
        data: [65],
      });
    }
    assert.deepEqual(await peer.receive(), {
      type: "resize",
      command_id: 33,
      size: { cols: 90, rows: 25 },
    });
    assert.deepEqual(await peer.receive(), { type: "stop", command_id: 34 });
    for (let commandId = 1; commandId <= 32; commandId += 1) {
      peer.send({
        type: "command_result",
        command_id: commandId,
        outcome: {
          type: "accepted",
          receipt: { type: "input", written_bytes: 1 },
        },
      });
    }
    peer.send({
      type: "command_result",
      command_id: 33,
      outcome: {
        type: "accepted",
        receipt: {
          type: "resize",
          applied_size: { cols: 90, rows: 25 },
        },
      },
    });
    peer.send({
      type: "command_result",
      command_id: 34,
      outcome: { type: "accepted", receipt: { type: "stop" } },
    });
  });

  const attachment = await new CtxmuxClient({
    socketPath: daemon.socketPath,
  }).attach(RUN_ID);
  const inputs = Array.from({ length: 32 }, () => attachment.input("A"));
  await assert.rejects(
    attachment.input("rejected-before-id"),
    (error: unknown) =>
      error instanceof CtxmuxCommandError &&
      error.code === "control_backpressure" &&
      error.disposition === "not_applied" &&
      error.commandId === undefined,
  );
  const resize = attachment.resize({ cols: 90, rows: 25 });
  const stop = attachment.stop();
  await Promise.all(inputs);
  assert.equal((await resize).commandId, 33);
  assert.equal((await stop).commandId, 34);
  attachment.close();
});

test("T-013 bounds pending input bytes without consuming the rejected command ID", async (context) => {
  const inputBytes = 350_000;
  const daemon = await mockDaemon(context, async (socket) => {
    const peer = new MockPeer(socket);
    await peer.handshake();
    await peer.receive();
    peer.send({ type: "attached", snapshot: attachedHeader() });
    for (let commandId = 1; commandId <= 2; commandId += 1) {
      const frame = await peer.receive();
      assert.equal((frame as { command_id?: unknown }).command_id, commandId);
      assert.equal((frame as { type?: unknown }).type, "input");
      assert.equal(
        (frame as { data?: { length?: unknown } }).data?.length,
        inputBytes,
      );
    }
    assert.deepEqual(await peer.receive(), { type: "stop", command_id: 3 });
    for (let commandId = 1; commandId <= 2; commandId += 1) {
      peer.send({
        type: "command_result",
        command_id: commandId,
        outcome: {
          type: "accepted",
          receipt: { type: "input", written_bytes: inputBytes },
        },
      });
    }
    peer.send({
      type: "command_result",
      command_id: 3,
      outcome: { type: "accepted", receipt: { type: "stop" } },
    });
  });

  const attachment = await new CtxmuxClient({
    socketPath: daemon.socketPath,
  }).attach(RUN_ID);
  const payload = new Uint8Array(inputBytes);
  const first = attachment.input(payload);
  const second = attachment.input(payload);
  await assert.rejects(
    attachment.input(payload),
    (error: unknown) =>
      error instanceof CtxmuxCommandError &&
      error.code === "control_backpressure" &&
      error.disposition === "not_applied" &&
      error.commandId === undefined,
  );
  const stop = attachment.stop();
  await Promise.all([first, second]);
  assert.equal((await stop).commandId, 3);
  attachment.close();
});

test("T-013 treats a duplicate completed result as fatal to unresolved commands", async (context) => {
  const daemon = await mockDaemon(context, async (socket) => {
    const peer = new MockPeer(socket);
    await peer.handshake();
    await peer.receive();
    peer.send({ type: "attached", snapshot: attachedHeader() });
    await peer.receive();
    await peer.receive();
    const accepted = {
      type: "command_result",
      command_id: 1,
      outcome: {
        type: "accepted",
        receipt: { type: "input", written_bytes: 1 },
      },
    } as const;
    peer.send(accepted);
    peer.send(accepted);
  });

  const attachment = await new CtxmuxClient({
    socketPath: daemon.socketPath,
  }).attach(RUN_ID);
  const input = attachment.input("x");
  const resize = attachment.resize({ cols: 80, rows: 24 });
  assert.deepEqual(await input, {
    commandId: 1,
    receipt: { type: "input", written_bytes: 1 },
  });
  await assert.rejects(
    resize,
    (error: unknown) =>
      error instanceof CtxmuxCommandError &&
      error.commandId === 2 &&
      error.disposition === "unknown",
  );
});

test("T-013 rejects an oversize attachment frame before consuming its command ID", async (context) => {
  const daemon = await mockDaemon(context, async (socket) => {
    const peer = new MockPeer(socket);
    await peer.handshake();
    await peer.receive();
    peer.send({ type: "attached", snapshot: attachedHeader() });
    assert.deepEqual(await peer.receive(), { type: "stop", command_id: 1 });
    peer.send({
      type: "command_result",
      command_id: 1,
      outcome: { type: "accepted", receipt: { type: "stop" } },
    });
  });

  const attachment = await new CtxmuxClient({
    socketPath: daemon.socketPath,
  }).attach(RUN_ID);
  await assert.rejects(
    attachment.input(new Uint8Array(300_000).fill(255)),
    (error: unknown) =>
      error instanceof CtxmuxCommandError &&
      error.code === "invalid_request" &&
      error.disposition === "not_applied" &&
      error.commandId === undefined,
  );
  await assert.rejects(
    attachment.input(new Uint8Array(MAX_FRAME_BYTES + 1)),
    (error: unknown) =>
      error instanceof CtxmuxCommandError &&
      error.code === "invalid_request" &&
      error.disposition === "not_applied" &&
      error.commandId === undefined,
  );
  assert.equal((await attachment.stop()).commandId, 1);
  attachment.close();
});

test("T-013 fences detach until pending controls have unique results", async (context) => {
  const resultRelease = deferred<void>();
  const daemon = await mockDaemon(context, async (socket) => {
    const peer = new MockPeer(socket);
    await peer.handshake();
    await peer.receive();
    peer.send({ type: "attached", snapshot: attachedHeader() });
    assert.deepEqual(await peer.receive(), {
      type: "input",
      command_id: 1,
      data: [120],
    });
    await resultRelease.promise;
    peer.send({
      type: "command_result",
      command_id: 1,
      outcome: {
        type: "accepted",
        receipt: { type: "input", written_bytes: 1 },
      },
    });
    assert.deepEqual(await peer.receive(), { type: "detach" });
    peer.send({ type: "detached" });
  });

  const attachment = await new CtxmuxClient({
    socketPath: daemon.socketPath,
  }).attach(RUN_ID);
  const input = attachment.input("x");
  const detaching = attachment.detach();
  await assert.rejects(attachment.stop(), /detaching/);
  resultRelease.resolve();
  await input;
  await detaching;
});

test("T-013 preserves short-control receipts, rejections, and lost-response disposition", async (context) => {
  let connection = 0;
  const daemon = await mockDaemon(context, async (socket) => {
    const peer = new MockPeer(socket);
    await peer.handshake();
    const frame = await peer.receive();
    connection += 1;
    if (connection === 1) {
      assert.deepEqual(frame, {
        type: "request",
        request: { type: "input", id: RUN_ID, data: [104, 105] },
      });
      peer.send({
        type: "response",
        response: {
          type: "control_accepted",
          run: runInfo(),
          receipt: { type: "input", written_bytes: 2 },
        },
      });
    } else if (connection === 2) {
      peer.send({
        type: "response",
        response: {
          type: "control_rejected",
          failure: {
            error: { code: "invalid_run_state", message: "terminal" },
            disposition: "not_applied",
          },
        },
      });
    } else {
      socket.destroy();
    }
  });
  const client = new CtxmuxClient({ socketPath: daemon.socketPath });
  assert.deepEqual(await client.input(RUN_ID, "hi"), {
    run: runInfo(),
    receipt: { type: "input", written_bytes: 2 },
  });
  await assert.rejects(
    client.stop(RUN_ID),
    (error: unknown) =>
      error instanceof CtxmuxCommandError &&
      error.code === "invalid_run_state" &&
      error.disposition === "not_applied" &&
      error.commandId === undefined,
  );
  await assert.rejects(
    client.resize(RUN_ID, { cols: 100, rows: 30 }),
    (error: unknown) =>
      error instanceof CtxmuxCommandError &&
      error.code === "io" &&
      error.disposition === "unknown",
  );
});

test(
  "SDK-01 settles FIN and close races and distinguishes acknowledged detach",
  { timeout: 5_000 },
  async (context) => {
    await testRequestClose(context, "finish");
    await testRequestClose(context, "destroy");

    const detachReceived = deferred<void>();
    const releaseAcknowledgement = deferred<void>();
    const daemon = await mockDaemon(context, async (socket) => {
      const peer = new MockPeer(socket);
      await peer.handshake();
      assert.deepEqual(await peer.receive(), {
        type: "request",
        request: { type: "attach", id: RUN_ID, after_byte: 0 },
      });
      peer.send({ type: "attached", snapshot: attachedHeader() });
      assert.deepEqual(await peer.receive(), { type: "detach" });
      detachReceived.resolve();
      await releaseAcknowledgement.promise;
      peer.send({ type: "detached" });
    });

    const attachment = await new CtxmuxClient({
      socketPath: daemon.socketPath,
    }).attach(RUN_ID);
    let settled = false;
    const detaching = attachment.detach().finally(() => {
      settled = true;
    });
    await detachReceived.promise;
    await delay(20);
    assert.equal(
      settled,
      false,
      "detach resolved before daemon acknowledgement",
    );
    releaseAcknowledgement.resolve();
    await detaching;
    assert.equal(settled, true);

    const abruptPeerClosed = deferred<void>();
    const abruptDaemon = await mockDaemon(context, async (socket) => {
      const peer = new MockPeer(socket);
      await peer.handshake();
      await peer.receive();
      peer.send({ type: "attached", snapshot: attachedHeader() });
      const next = await peer.receiveOptional();
      assert.equal(next, undefined, "abrupt close sent a clean detach frame");
      abruptPeerClosed.resolve();
    });
    const abruptAttachment = await new CtxmuxClient({
      socketPath: abruptDaemon.socketPath,
    }).attach(RUN_ID);
    abruptAttachment.close();
    await abruptPeerClosed.promise;
  },
);

test("SDK-01 rejects the write callback error exactly once", async () => {
  const socket = new Socket();
  const expected = new Error("mock write callback failure");
  let callbackCount = 0;
  Object.defineProperty(socket, "write", {
    configurable: true,
    value: (_payload: string, callback: (error?: Error | null) => void) => {
      queueMicrotask(() => {
        callbackCount += 1;
        callback(expected);
      });
      return false;
    },
  });
  const wire = new JsonLinesConnection(socket);

  await assert.rejects(wire.send({ type: "detach" }), (error) => {
    return error === expected;
  });
  assert.equal(callbackCount, 1);
  await assert.rejects(wire.send({ type: "detach" }), (error) => {
    return error === expected;
  });
});

test("SDK-01 settles a pending receive on an RST-style socket error", async () => {
  const socket = new Socket();
  const wire = new JsonLinesConnection(socket);
  const reset = Object.assign(new Error("mock read ECONNRESET"), {
    code: "ECONNRESET",
  });
  const receiving = wire.receive();

  socket.emit("error", reset);

  await assert.rejects(receiving, (error) => error === reset);
  await assert.rejects(wire.receive(), (error) => error === reset);
  wire.close();
});

test(
  "SDK-02 keeps command results live while bounded output becomes an explicit Gap",
  { timeout: 10_000 },
  async (context) => {
    const frameCount = 20_000;
    const daemon = await mockDaemon(context, async (socket) => {
      const peer = new MockPeer(socket);
      await peer.handshake();
      await peer.receive();
      peer.send({ type: "attached", snapshot: attachedHeader() });
      for (let startByte = 0; startByte < frameCount; startByte += 1) {
        await peer.sendWithBackpressure({
          type: "event",
          event: {
            type: "output",
            chunk: {
              start_byte: startByte,
              end_byte: startByte + 1,
              data: [65],
            },
          },
        });
      }
      assert.deepEqual(await peer.receive(), {
        type: "stop",
        command_id: 1,
      });
      peer.send({
        type: "command_result",
        command_id: 1,
        outcome: { type: "accepted", receipt: { type: "stop" } },
      });
    });

    const attachment = await new CtxmuxClient({
      socketPath: daemon.socketPath,
    }).attach(RUN_ID);
    assert.deepEqual(await attachment.stop(), {
      commandId: 1,
      receipt: { type: "stop" },
    });
    for (let expected = 0; expected < 256; expected += 1) {
      assert.deepEqual(await attachment.nextEvent(), {
        type: "output",
        chunk: { start_byte: expected, end_byte: expected + 1, data: [65] },
      });
    }
    const gap = await attachment.nextEvent();
    assert.deepEqual(gap, {
      type: "gap",
      latest_output_bytes: frameCount,
    });
    assert.equal(
      gap === undefined ? undefined : runEventSource(gap),
      RUN_ID,
      "an SDK-synthesized output Gap must retain its Attachment Run owner",
    );
    attachment.close();
  },
);

test("SDK-02 preserves Gap, tmux, later Gap, and terminal order across saturation", async (context) => {
  const daemon = await mockDaemon(context, async (socket) => {
    const peer = new MockPeer(socket);
    await peer.handshake();
    await peer.receive();
    peer.send({ type: "attached", snapshot: attachedHeader() });
    for (let startByte = 0; startByte < 257; startByte += 1) {
      peer.send({
        type: "event",
        event: {
          type: "output",
          chunk: { start_byte: startByte, end_byte: startByte + 1, data: [65] },
        },
      });
    }
    assert.deepEqual(await peer.receive(), { type: "stop", command_id: 1 });
    peer.send({
      type: "event",
      event: { type: "tmux", event: { type: "paused" } },
    });
    peer.send({
      type: "event",
      event: {
        type: "output",
        chunk: { start_byte: 257, end_byte: 258, data: [66] },
      },
    });
    peer.send({
      type: "event",
      event: {
        type: "exited",
        state: { type: "exited", code: 0, signal: null },
      },
    });
    peer.send({
      type: "command_result",
      command_id: 1,
      outcome: { type: "accepted", receipt: { type: "stop" } },
    });
  });

  const attachment = await new CtxmuxClient({
    socketPath: daemon.socketPath,
  }).attach(RUN_ID);
  await delay(50);
  assert.equal((await attachment.nextEvent())?.type, "output");
  assert.equal((await attachment.nextEvent())?.type, "output");
  await attachment.stop();
  for (let expected = 2; expected < 256; expected += 1) {
    assert.deepEqual(await attachment.nextEvent(), {
      type: "output",
      chunk: { start_byte: expected, end_byte: expected + 1, data: [65] },
    });
  }
  assert.deepEqual(await attachment.nextEvent(), {
    type: "gap",
    latest_output_bytes: 257,
  });
  assert.deepEqual(await attachment.nextEvent(), {
    type: "tmux",
    event: { type: "paused" },
  });
  assert.deepEqual(await attachment.nextEvent(), {
    type: "gap",
    latest_output_bytes: 258,
  });
  assert.deepEqual(await attachment.nextEvent(), {
    type: "exited",
    state: { type: "exited", code: 0, signal: null },
  });
  attachment.close();
});

test("SDK-02 treats daemon EOF after one terminal event as a clean event end", async (context) => {
  const daemon = await mockDaemon(context, async (socket) => {
    const peer = new MockPeer(socket);
    await peer.handshake();
    await peer.receive();
    peer.send({ type: "attached", snapshot: attachedHeader() });
    peer.send({
      type: "event",
      event: {
        type: "exited",
        state: { type: "exited", code: 0, signal: null },
      },
    });
    socket.end();
  });

  const attachment = await new CtxmuxClient({
    socketPath: daemon.socketPath,
  }).attach(RUN_ID);
  assert.deepEqual(await attachment.nextEvent(), {
    type: "exited",
    state: { type: "exited", code: 0, signal: null },
  });
  await delay(20);
  assert.equal(await attachment.nextEvent(), undefined);
});

test("SDK-02 permits only one pending attachment event consumer", async (context) => {
  const daemon = await mockDaemon(context, async (socket) => {
    const peer = new MockPeer(socket);
    await peer.handshake();
    await peer.receive();
    peer.send({ type: "attached", snapshot: attachedHeader() });
    await delay(50);
    peer.send({
      type: "event",
      event: { type: "gap", latest_output_bytes: 7 },
    });
  });

  const attachment = await new CtxmuxClient({
    socketPath: daemon.socketPath,
  }).attach(RUN_ID);
  const first = attachment.nextEvent();
  await assert.rejects(
    attachment.nextEvent(),
    /only one nextEvent\(\) call may be pending/,
  );
  assert.deepEqual(await first, { type: "gap", latest_output_bytes: 7 });
  attachment.close();
});

test(
  "SDK-02 counts near-1MiB tmux rename payloads against the event byte budget",
  { timeout: 10_000 },
  async (context) => {
    const retainedNameBytes = 500_000;
    const overflowNameBytes = 60_000;
    const daemon = await mockDaemon(context, async (socket) => {
      const peer = new MockPeer(socket);
      await peer.handshake();
      await peer.receive();
      peer.send({ type: "attached", snapshot: attachedHeader() });
      const retainedName = Array<number>(retainedNameBytes).fill(0);
      for (let index = 0; index < 2; index += 1) {
        await peer.sendWithBackpressure({
          type: "event",
          event: {
            type: "tmux",
            event: { type: "session_renamed", name: retainedName },
          },
        });
      }
      await peer.sendWithBackpressure({
        type: "event",
        event: {
          type: "tmux",
          event: {
            type: "session_renamed",
            name: Array<number>(overflowNameBytes).fill(1),
          },
        },
      });
    });

    const attachment = await new CtxmuxClient({
      socketPath: daemon.socketPath,
    }).attach(RUN_ID);
    await assert.rejects(
      settleWithin(attachment.stop(), 2_000),
      (error: unknown) =>
        error instanceof CtxmuxCommandError && error.disposition === "unknown",
    );
    for (let index = 0; index < 2; index += 1) {
      const event = await attachment.nextEvent();
      assert.equal(event?.type, "tmux");
      if (event?.type === "tmux") {
        assert.equal(event.event.type, "session_renamed");
        if (event.event.type === "session_renamed") {
          assert.equal(event.event.name.length, retainedNameBytes);
        }
      }
    }
    await assert.rejects(
      attachment.nextEvent(),
      (error: unknown) =>
        error instanceof CtxmuxInvalidFrameError &&
        error.path === "$frame.event",
    );
  },
);

test("SDK-02 fails closed rather than dropping saturated non-output events", async (context) => {
  const daemon = await mockDaemon(context, async (socket) => {
    const peer = new MockPeer(socket);
    await peer.handshake();
    await peer.receive();
    peer.send({ type: "attached", snapshot: attachedHeader() });
    for (let sequence = 1; sequence <= 257; sequence += 1) {
      peer.send({
        type: "event",
        event: { type: "gap", latest_output_bytes: sequence },
      });
    }
  });

  const attachment = await new CtxmuxClient({
    socketPath: daemon.socketPath,
  }).attach(RUN_ID);
  await delay(50);
  for (let expected = 1; expected <= 256; expected += 1) {
    assert.deepEqual(await attachment.nextEvent(), {
      type: "gap",
      latest_output_bytes: expected,
    });
  }
  await assert.rejects(
    attachment.nextEvent(),
    (error: unknown) =>
      error instanceof CtxmuxInvalidFrameError && error.path === "$frame.event",
  );
});

async function testRequestClose(
  context: test.TestContext,
  mode: "finish" | "destroy",
): Promise<void> {
  const daemon = await mockDaemon(context, async (socket) => {
    const peer = new MockPeer(socket);
    await peer.handshake();
    await peer.receive();
    if (mode === "finish") {
      socket.end();
    } else {
      socket.destroy();
    }
  });
  const operation = new CtxmuxClient({
    socketPath: daemon.socketPath,
  }).status(RUN_ID);
  await assert.rejects(settleWithin(operation, 1_000), WireClosedError);
}

class MockPeer {
  readonly #socket: Socket;
  readonly #lines;
  readonly #iterator;

  public constructor(socket: Socket) {
    this.#socket = socket;
    this.#lines = createInterface({ input: socket, crlfDelay: Infinity });
    this.#iterator = this.#lines[Symbol.asyncIterator]();
    socket.on("error", () => undefined);
  }

  public async handshake(): Promise<void> {
    assert.deepEqual(await this.receive(), {
      type: "hello",
      hello: { protocol: PROTOCOL_VERSION },
    });
    this.send({
      type: "hello",
      protocol: PROTOCOL_VERSION,
      daemon_instance: DAEMON_INSTANCE,
    });
  }

  public send(frame: ServerFrame): void {
    this.#socket.write(`${JSON.stringify(frame)}\n`);
  }

  public sendRaw(frame: Uint8Array): void {
    this.#socket.write(frame);
  }

  public async sendWithBackpressure(frame: ServerFrame): Promise<void> {
    if (!this.#socket.write(`${JSON.stringify(frame)}\n`)) {
      await once(this.#socket, "drain");
    }
  }

  public async receive(): Promise<unknown> {
    const result = await this.#iterator.next();
    if (result.done === true) {
      throw new Error("mock client closed before the expected frame");
    }
    return JSON.parse(result.value) as unknown;
  }

  public async receiveOptional(): Promise<unknown | undefined> {
    const result = await this.#iterator.next();
    return result.done === true
      ? undefined
      : (JSON.parse(result.value) as unknown);
  }
}

async function mockDaemon(
  context: test.TestContext,
  handler: (socket: Socket) => Promise<void>,
): Promise<{ readonly socketPath: string }> {
  const directory = await mkdtemp(join(tmpdir(), "ctxmux-sdk-fixture-"));
  const socketPath = join(directory, "daemon.sock");
  const handlerFailures: Promise<void>[] = [];
  const sockets = new Set<Socket>();
  const server = createServer((socket) => {
    sockets.add(socket);
    socket.once("close", () => sockets.delete(socket));
    const failure = handler(socket);
    handlerFailures.push(failure);
    void failure.catch(() => undefined);
  });
  await listen(server, socketPath);
  context.after(async () => {
    await closeServer(server, sockets);
    await Promise.all(handlerFailures);
    await rm(directory, { recursive: true, force: true });
  });
  return { socketPath };
}

async function listen(server: Server, socketPath: string): Promise<void> {
  server.listen(socketPath);
  await once(server, "listening");
}

async function closeServer(
  server: Server,
  sockets: ReadonlySet<Socket>,
): Promise<void> {
  if (!server.listening) {
    return;
  }
  const closed = once(server, "close");
  server.close();
  for (const socket of sockets) socket.destroy();
  await closed;
}

function runInfo() {
  return {
    id: RUN_ID,
    spec: {
      program: "/bin/sh",
      args: ["-c", "printf ok"],
      cwd: null,
      env: { FIXTURE: "1" },
      size: { cols: 80, rows: 24 },
      declared_inputs: [],
    },
    lineage: null,
    backend: { type: "native" as const },
    capabilities: {
      input: true,
      resize: true,
      stop: true,
      fork_level_a: true,
      fork_level_b: true,
      replay: "raw_from_start" as const,
    },
    pid: 42,
    state: { type: "running" as const },
    latest_output_bytes: 1,
    durable_output_bytes: null,
    first_available_byte: 1,
    attachments: 1,
    applied_input_bytes: 0,
  };
}

function tmuxPaneInfo() {
  return {
    socket_path: "/tmp/ctxmux-tmux.sock",
    tmux_version: "3.6b",
    server_pid: 0xffff_ffff,
    server_started_at: Number.MAX_SAFE_INTEGER,
    session_id: "$12",
    window_id: "@34",
    pane_id: "%56",
    pane_pid: 0xffff_ffff,
    size: { cols: 120, rows: 40 },
  };
}

function tmuxRunInfo() {
  const pane = tmuxPaneInfo();
  return {
    ...runInfo(),
    spec: null,
    backend: {
      type: "tmux" as const,
      socket_path: pane.socket_path,
      server_pid: pane.server_pid,
      server_started_at: pane.server_started_at,
      session_id: pane.session_id,
      window_id: pane.window_id,
      pane_id: pane.pane_id,
      tmux_version: pane.tmux_version,
    },
    capabilities: {
      input: false,
      resize: false,
      stop: false,
      fork_level_a: false,
      fork_level_b: false,
      replay: "raw_since_import" as const,
    },
    pid: pane.pane_pid,
    applied_input_bytes: null,
  };
}

function attachedHeader(headSequence = 0) {
  return {
    run: {
      ...runInfo(),
      latest_output_bytes: headSequence,
      first_available_byte: headSequence,
    },
    replay: {
      first_available_byte: headSequence,
      latest_output_bytes: headSequence,
      truncated: false,
    },
  };
}

interface Deferred<T> {
  readonly promise: Promise<T>;
  readonly settled: boolean;
  readonly resolve: (value: T) => void;
  readonly reject: (error: unknown) => void;
}

function deferred<T = void>(): Deferred<T> {
  let resolvePromise!: (value: T) => void;
  let rejectPromise!: (error: unknown) => void;
  const state = { settled: false };
  const promise = new Promise<T>((resolve, reject) => {
    resolvePromise = resolve;
    rejectPromise = reject;
  });
  return {
    promise,
    get settled() {
      return state.settled;
    },
    resolve(value: T) {
      state.settled = true;
      resolvePromise(value);
    },
    reject(error: unknown) {
      state.settled = true;
      rejectPromise(error);
    },
  };
}

async function settleWithin<T>(
  operation: Promise<T>,
  milliseconds: number,
): Promise<T> {
  return await Promise.race([
    operation,
    delay(milliseconds).then(() => {
      throw new Error("operation did not settle");
    }),
  ]);
}

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}
