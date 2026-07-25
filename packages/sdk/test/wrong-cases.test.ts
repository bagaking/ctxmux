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
  CtxmuxInvalidFrameError,
  MAX_FRAME_BYTES,
  PROTOCOL_VERSION,
  type ServerFrame,
} from "../src/index.ts";
import { validateServerFrame } from "../src/validation.ts";
import { JsonLinesConnection, WireClosedError } from "../src/wire.ts";

const RUN_ID = "018f47f2-9df7-7f5f-8f2d-d3353f114ae8";
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
    event: { type: "gap", head_seq: Number.MAX_SAFE_INTEGER },
  } satisfies ServerFrame;
  assert.equal(validateServerFrame(safeFrame), safeFrame);

  const rounded = JSON.parse(
    '{"type":"event","event":{"type":"gap","head_seq":9007199254740993}}',
  ) as unknown;
  assert.throws(
    () => validateServerFrame(rounded),
    (error: unknown) =>
      error instanceof CtxmuxInvalidFrameError &&
      error.path === "$frame.event.head_seq",
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
        after_seq: Number.MAX_SAFE_INTEGER,
      },
    });
    peer.send({
      type: "attached",
      snapshot: attachedHeader(Number.MAX_SAFE_INTEGER),
    });
  });

  const client = new CtxmuxClient({ socketPath: daemon.socketPath });
  const attachment = await client.attach(RUN_ID, Number.MAX_SAFE_INTEGER);
  assert.equal(attachment.snapshot.replay.head_seq, Number.MAX_SAFE_INTEGER);
  attachment.close();

  await assert.rejects(
    client.attach(RUN_ID, Number.MAX_SAFE_INTEGER + 1),
    (error: unknown) =>
      error instanceof CtxmuxInvalidFrameError && error.path === "afterSeq",
  );
});

test("SC-02 rejects malformed nested runtime frames", () => {
  const mutations: readonly [unknown, string][] = [
    [{ type: "invented" }, "$frame.type"],
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
          run: { ...runInfo(), durable_head_seq: -1 },
        },
      },
      "$frame.response.run.durable_head_seq",
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
          replay: { ...attachedHeader().replay, oldest_seq: "0" },
        },
      },
      "$frame.snapshot.replay.oldest_seq",
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
        event: { type: "output", chunk: { seq: 1, data: [0, 256] } },
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
        type: "event",
        event: { type: "accepted", run: { ...runInfo(), spec: {} } },
      },
      "$frame.event.run.spec.program",
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
    { type: "hello", protocol: PROTOCOL_VERSION },
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
    { type: "response", response: { type: "accepted", run: runInfo() } },
    { type: "attached", snapshot: attachedHeader() },
    {
      type: "event",
      event: { type: "output", chunk: { seq: 1, data: [0, 10, 255] } },
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
    { type: "event", event: { type: "gap", head_seq: 1 } },
    { type: "event", event: { type: "accepted", run: runInfo() } },
    { type: "detached" },
    {
      type: "error",
      error: { code: "invalid_request", message: "invalid fixture" },
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
      request: { type: "attach", id: RUN_ID, after_seq: 0 },
    });
    peer.send({
      type: "attached",
      snapshot: {
        ...attachedHeader(2),
        replay: {
          oldest_seq: 1,
          head_seq: 2,
          truncated: false,
        },
      },
    });
    peer.send({
      type: "event",
      event: { type: "output", chunk: { seq: 1, data: [0, 255] } },
    });
    peer.send({
      type: "event",
      event: { type: "output", chunk: { seq: 2, data: [1, 2, 3] } },
    });
  });

  const attachment = await new CtxmuxClient({
    socketPath: daemon.socketPath,
  }).attach(RUN_ID);
  assert.deepEqual(attachment.snapshot.replay.chunks, [
    { seq: 1, data: [0, 255] },
    { seq: 2, data: [1, 2, 3] },
  ]);
  attachment.close();
});

test("LP-02 settles a truncated empty replay without inventing sequence zero", async (context) => {
  const daemon = await mockDaemon(context, async (socket) => {
    const peer = new MockPeer(socket);
    await peer.handshake();
    assert.deepEqual(await peer.receive(), {
      type: "request",
      request: { type: "attach", id: RUN_ID, after_seq: 0 },
    });
    peer.send({
      type: "attached",
      snapshot: {
        ...attachedHeader(),
        replay: { oldest_seq: 0, head_seq: 0, truncated: true },
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
      request: { type: "attach", id: RUN_ID, after_seq: 2 },
    });
    peer.send({
      type: "attached",
      snapshot: {
        ...attachedHeader(3),
        replay: { oldest_seq: 1, head_seq: 3, truncated: true },
      },
    });
    peer.send({
      type: "event",
      event: { type: "output", chunk: { seq: 3, data: [3] } },
    });
  });

  const attachment = await settleWithin(
    new CtxmuxClient({ socketPath: daemon.socketPath }).attach(RUN_ID, 2),
    1_000,
  );
  assert.equal(attachment.snapshot.replay.truncated, true);
  assert.deepEqual(attachment.snapshot.replay.chunks, [{ seq: 3, data: [3] }]);
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
        request: { type: "attach", id: RUN_ID, after_seq: 0 },
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
  "SDK-02 applies bounded inbound backpressure to a slow attachment",
  { timeout: 10_000 },
  async (context) => {
    const frameCount = 20_000;
    const floodStarted = deferred<void>();
    const floodFinished = deferred<void>();
    const daemon = await mockDaemon(context, async (socket) => {
      const peer = new MockPeer(socket);
      await peer.handshake();
      await peer.receive();
      peer.send({ type: "attached", snapshot: attachedHeader() });
      floodStarted.resolve();
      for (let sequence = 1; sequence <= frameCount; sequence += 1) {
        await peer.sendWithBackpressure({
          type: "event",
          event: { type: "gap", head_seq: sequence },
        });
      }
      floodFinished.resolve();
    });

    const attachment = await new CtxmuxClient({
      socketPath: daemon.socketPath,
    }).attach(RUN_ID);
    await floodStarted.promise;
    await delay(50);
    assert.equal(
      floodFinished.settled,
      false,
      "slow consumer was drained into an unbounded JavaScript queue",
    );

    for (let expected = 1; expected <= frameCount; expected += 1) {
      const event = await attachment.nextEvent();
      assert.deepEqual(event, { type: "gap", head_seq: expected });
    }
    await floodFinished.promise;
    attachment.close();
  },
);

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
    this.send({ type: "hello", protocol: PROTOCOL_VERSION });
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
    head_seq: 1,
    durable_head_seq: null,
    oldest_seq: 1,
    attachments: 1,
  };
}

function attachedHeader(headSequence = 0) {
  return {
    run: { ...runInfo(), head_seq: headSequence, oldest_seq: headSequence },
    replay: {
      oldest_seq: headSequence,
      head_seq: headSequence,
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
