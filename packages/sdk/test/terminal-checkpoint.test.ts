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
  PROTOCOL_VERSION,
  CtxmuxInvalidFrameError,
  CtxmuxTerminalSeedResourceError,
  type TerminalSeedLimits,
  type AttachedHeader,
} from "../src/index.ts";

const ID = "018f47f2-9df7-7f5f-8f2d-d3353f114ae8";
const OTHER = "018f47f2-9df7-7f5f-8f2d-d3353f114ae9";
const seed = Buffer.from("\x1bcterminal seed");
type BasicHeader = Omit<AttachedHeader, "terminal"> & {
  terminal: Extract<AttachedHeader["terminal"], { type: "basic_vt" }>;
};
function header(): BasicHeader {
  return {
    run: {
      id: ID,
      spec: {
        program: "/bin/cat",
        args: [],
        cwd: null,
        env: {},
        initial_size: { rows: 4, cols: 12 },
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
      latest_output_bytes: 102,
      durable_output_bytes: 102,
      first_available_byte: 100,
      attachments: 1,
      applied_input_bytes: 0,
      current_size: { rows: 6, cols: 15 },
      native_service: {
        revision: 0,
        owner: { type: "serving" as const },
        output: { type: "serving" as const },
        input: {
          phase: { type: "open" as const },
          unsettled_commands: 0,
          unsettled_request_bytes: 0,
          write_blocked: false,
          completed_input_bytes: 0,
          current_size: { cols: 15, rows: 6 },
          active_confirmed_bytes: 0,
        },
        terminal_fault: null,
      },
    },
    replay: {
      first_available_byte: 100,
      latest_output_bytes: 102,
      truncated: true,
    },
    resize_revision: 2,
    terminal: {
      type: "basic_vt",
      checkpoint: {
        run_id: ID,
        through_byte: 100,
        resize_revision: 0,
        size: { rows: 4, cols: 12 },
        restore_bytes: seed.length,
        restore_size: { rows: 4, cols: 12 },
        restore_scrollback_rows: null,
        resize_after_restore_bytes: 0,
      },
      resizes: [
        { through_byte: 101, resize_revision: 1, size: { rows: 5, cols: 14 } },
        { through_byte: 101, resize_revision: 2, size: { rows: 6, cols: 15 } },
      ],
    },
  };
}
async function fixture(
  context: test.TestContext,
  view: "raw" | "terminal",
  snapshot: unknown,
  frames: Iterable<unknown> = [],
  terminalSeedLimits?: TerminalSeedLimits,
) {
  const directory = await mkdtemp(join(tmpdir(), "ctxmux-sdk-terminal-"));
  const socketPath = join(directory, "ctxmux.sock");
  const sockets = new Set<Socket>();
  const handlers: Promise<void>[] = [];
  const server = createServer((socket) => {
    sockets.add(socket);
    socket.on("error", () => {});
    socket.once("close", () => sockets.delete(socket));
    const handler = (async () => {
      const iterator = createInterface({ input: socket, crlfDelay: Infinity })[
        Symbol.asyncIterator
      ]();
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
      send({
        type: "hello",
        runtime: {
          runtimeId: OTHER,
          daemonInstanceId: ID,
          runtimeIdPersistence: "daemon",
          buildId: "ctxmuxd/fixture",
          protocolGeneration: PROTOCOL_VERSION,
          platform: "darwin",
          arch: "aarch64",
          capabilities: {},
        },
      });
      assert.deepEqual(await receive(), {
        type: "request",
        request: { type: "attach", id: ID, after_byte: 0, view },
      });
      send({ type: "attached", snapshot });
      for (const frame of frames) {
        if (!send(frame)) await once(socket, "drain");
      }
    })();
    handlers.push(handler);
    void handler.catch(() => {});
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
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  });
  return new CtxmuxClient({
    socketPath,
    ...(terminalSeedLimits === undefined ? {} : { terminalSeedLimits }),
  });
}
const chunks = () => [
  {
    type: "terminal_checkpoint_chunk",
    offset: 0,
    data: seed.subarray(0, 4).toString("base64"),
  },
  {
    type: "terminal_checkpoint_chunk",
    offset: 4,
    data: seed.subarray(4).toString("base64"),
  },
  {
    type: "event",
    event: {
      type: "output",
      chunk: {
        start_byte: 100,
        end_byte: 102,
        data: Buffer.from("xy").toString("base64"),
      },
    },
  },
];
test("terminal attach separates exact bounded seed from original bytes and same-byte resizes", async (context) => {
  const expected = header();
  const client = await fixture(context, "terminal", expected, chunks());
  const attached = await client.attachTerminal(ID);
  try {
    assert.deepEqual(attached.snapshot.terminal_restore, new Uint8Array(seed));
    assert.deepEqual(attached.snapshot.terminal, expected.terminal);
    assert.equal(attached.snapshot.resize_revision, 2);
    assert.deepEqual(attached.snapshot.replay.chunks, [
      {
        start_byte: 100,
        end_byte: 102,
        data: new Uint8Array(Buffer.from("xy")),
      },
    ]);
  } finally {
    attached.close();
  }
});
test("terminal restore preserves historical geometry, exact prefix fence and explicit history policy", async (context) => {
  const expected = header();
  expected.terminal.checkpoint.restore_size = { rows: 8, cols: 12 };
  expected.terminal.checkpoint.resize_after_restore_bytes = 5;
  // An explicit history capacity can exceed the initial visible viewport.
  expected.terminal.checkpoint.restore_scrollback_rows =
    expected.terminal.checkpoint.restore_size.rows + 1;
  const client = await fixture(context, "terminal", expected, chunks());
  const attached = await client.attachTerminal(ID);
  try {
    assert.deepEqual(attached.snapshot.terminal, expected.terminal);
    assert.deepEqual(attached.snapshot.terminal_restore, new Uint8Array(seed));
    assert.deepEqual(
      attached.snapshot.replay.chunks[0]?.data,
      new Uint8Array(Buffer.from("xy")),
    );
  } finally {
    attached.close();
  }
});
for (const kind of ["unknown", "unavailable"] as const) {
  test(`terminal attach preserves typed ${kind} and the original retained tail`, async (context) => {
    const expected = {
      ...header(),
      terminal: { type: kind, reason: "origin_unknown" },
    };
    const client = await fixture(
      context,
      "terminal",
      expected,
      chunks().slice(2),
    );
    const attached = await client.attachTerminal(ID);
    try {
      assert.deepEqual(attached.snapshot.terminal, expected.terminal);
      assert.equal(attached.snapshot.terminal_restore.length, 0);
      assert.deepEqual(
        attached.snapshot.replay.chunks[0]?.data,
        new Uint8Array(Buffer.from("xy")),
      );
    } finally {
      attached.close();
    }
  });
}
test("raw attach preserves requested raw replay and never consumes a synthetic seed", async (context) => {
  const expected = { ...header(), terminal: { type: "not_requested" } };
  const client = await fixture(context, "raw", expected, chunks().slice(2));
  const attached = await client.attach(ID);
  try {
    assert.deepEqual(attached.snapshot.terminal, { type: "not_requested" });
    assert.equal(attached.snapshot.terminal_restore.length, 0);
    assert.equal(attached.snapshot.replay.chunks.length, 1);
  } finally {
    attached.close();
  }
});
for (const view of ["raw", "terminal"] as const) {
  test(`${view} attach rejects the other requested view`, async (context) => {
    const expected =
      view === "raw"
        ? header()
        : { ...header(), terminal: { type: "not_requested" } };
    const client = await fixture(context, view, expected, chunks());
    await assert.rejects(
      view === "raw" ? client.attach(ID) : client.attachTerminal(ID),
      /exact Run.*attachment/,
    );
  });
}
for (const [name, mutate] of [
  [
    "missing restore scrollback policy",
    (h: BasicHeader) => {
      Reflect.deleteProperty(h.terminal.checkpoint, "restore_scrollback_rows");
    },
  ],
  [
    "unsafe restore scrollback rows",
    (h: BasicHeader) => {
      h.terminal.checkpoint.restore_scrollback_rows =
        Number.MAX_SAFE_INTEGER + 1;
    },
  ],
  [
    "negative restore scrollback rows",
    (h: BasicHeader) => {
      h.terminal.checkpoint.restore_scrollback_rows = -1;
    },
  ],
  [
    "missing restore grid",
    (h: BasicHeader) => {
      Reflect.deleteProperty(h.terminal.checkpoint, "restore_size");
    },
  ],
  [
    "missing restoration split",
    (h: BasicHeader) => {
      Reflect.deleteProperty(
        h.terminal.checkpoint,
        "resize_after_restore_bytes",
      );
    },
  ],
  [
    "invalid restore grid",
    (h: BasicHeader) => {
      h.terminal.checkpoint.restore_size.rows = 0;
    },
  ],
  [
    "split beyond complete seed",
    (h: BasicHeader) => {
      h.terminal.checkpoint.resize_after_restore_bytes =
        h.terminal.checkpoint.restore_bytes + 1;
    },
  ],
  [
    "negative restoration split",
    (h: BasicHeader) => {
      h.terminal.checkpoint.resize_after_restore_bytes = -1;
    },
  ],

  [
    "foreign Run",
    (h: BasicHeader) => {
      h.terminal.checkpoint.run_id = OTHER;
    },
  ],
  [
    "evicted fence",
    (h: BasicHeader) => {
      h.terminal.checkpoint.through_byte = 99;
    },
  ],
  [
    "unsafe seed length",
    (h: BasicHeader) => {
      h.terminal.checkpoint.restore_bytes = Number.MAX_SAFE_INTEGER + 1;
    },
  ],
  [
    "empty seed",
    (h: BasicHeader) => {
      h.terminal.checkpoint.restore_bytes = 0;
    },
  ],
  [
    "missing resize revision",
    (h: BasicHeader) => {
      h.terminal.resizes[1]!.resize_revision = 3;
    },
  ],
  [
    "incomplete geometry tail",
    (h: BasicHeader) => {
      h.resize_revision = 3;
    },
  ],
] as const) {
  test(`terminal attach rejects ${name} before allocating or consuming seed`, async (context) => {
    const expected = structuredClone(header());
    mutate(expected);
    const client = await fixture(context, "terminal", expected);
    await assert.rejects(
      client.attachTerminal(ID),
      (error) =>
        error instanceof CtxmuxInvalidFrameError &&
        error.path.startsWith("$frame.snapshot.terminal"),
    );
  });
}
test("terminal attach rejects a discontinuous seed offset", async (context) => {
  const frames: unknown[] = chunks();
  frames[1] = {
    type: "terminal_checkpoint_chunk",
    offset: 5,
    data: seed.subarray(4).toString("base64"),
  };
  const client = await fixture(context, "terminal", header(), frames);
  await assert.rejects(
    client.attachTerminal(ID),
    /ordered synthetic terminal seed/,
  );
});

for (const delivered of [0, 1]) {
  test(`terminal attach refuses a missing tail after ${delivered} original bytes`, async (context) => {
    const frames: unknown[] = chunks().slice(0, 2);
    if (delivered === 1)
      frames.push({
        type: "event",
        event: {
          type: "output",
          chunk: {
            start_byte: 100,
            end_byte: 101,
            data: Buffer.from("x").toString("base64"),
          },
        },
      });
    frames.push({
      type: "replay_window",
      first_available_byte: 101 + delivered,
      latest_output_bytes: 102,
    });
    if (delivered === 0)
      frames.push({
        type: "event",
        event: {
          type: "output",
          chunk: {
            start_byte: 101,
            end_byte: 102,
            data: Buffer.from("y").toString("base64"),
          },
        },
      });
    const client = await fixture(context, "terminal", header(), frames);
    await assert.rejects(
      client.attachTerminal(ID),
      /complete terminal checkpoint original tail/,
    );
  });
}

test("raw attachment preserves an explicit advancing window", async (context) => {
  const expected = { ...header(), terminal: { type: "not_requested" } };
  const client = await fixture(context, "raw", expected, [
    {
      type: "replay_window",
      first_available_byte: 101,
      latest_output_bytes: 102,
    },
    {
      type: "event",
      event: {
        type: "output",
        chunk: {
          start_byte: 101,
          end_byte: 102,
          data: Buffer.from("y").toString("base64"),
        },
      },
    },
  ]);
  const attached = await client.attach(ID);
  try {
    assert.equal(attached.snapshot.replay.truncated, true);
    assert.equal(attached.snapshot.replay.first_available_byte, 101);
    assert.deepEqual(
      attached.snapshot.replay.chunks[0]?.data,
      new Uint8Array(Buffer.from("y")),
    );
  } finally {
    attached.close();
  }
});

// The old oversized-seed fixture is a historical local policy, not malformed wire.
test("terminal attach reports the historical oversized seed as a local receive limit", async (context) => {
  const expected = header();
  const requested = 32 * 1024 * 1024 + 1;
  expected.terminal.checkpoint.restore_bytes = requested;
  const client = await fixture(context, "terminal", expected);
  await assert.rejects(client.attachTerminal(ID), (error) => {
    assert.ok(error instanceof CtxmuxTerminalSeedResourceError);
    assert.equal(error.reason, "restore_limit");
    assert.equal(error.runId, ID);
    assert.equal(error.requestedBytes, requested);
    assert.equal(error.limitBytes, requested - 1);
    assert.equal(error.recovery, "attach_raw_or_review_local_resources");
    assert.equal(error.cause, undefined);
    return true;
  });
});

function* largeSeedFrames(payload: Buffer): Iterable<unknown> {
  // Each base64 JSON frame fits the existing 1 MiB wire unit.
  for (let offset = 0; offset < payload.length; offset += 512 * 1024) {
    yield {
      type: "terminal_checkpoint_chunk",
      offset,
      data: payload.subarray(offset, offset + 512 * 1024).toString("base64"),
    };
  }
  yield chunks()[2];
}
for (const extraByte of [0, 1]) {
  test(`terminal consumer assembles exact ${32 * 1024 * 1024 + extraByte} bytes over real socket frames`, async (context) => {
    const expected = header();
    const payload = Buffer.alloc(32 * 1024 * 1024 + extraByte);
    for (let index = 0; index < payload.length; index += 1)
      payload[index] = index & 255;
    expected.terminal.checkpoint.restore_bytes = payload.length;
    const client = await fixture(
      context,
      "terminal",
      expected,
      largeSeedFrames(payload),
      extraByte === 0 ? undefined : { restoreBytes: payload.length },
    );
    const attached = await client.attachTerminal(ID);
    try {
      assert.deepEqual(
        attached.snapshot.terminal_restore,
        new Uint8Array(payload.buffer, payload.byteOffset, payload.byteLength),
      );
      assert.deepEqual(
        attached.snapshot.replay.chunks[0]?.data,
        new Uint8Array(Buffer.from("xy")),
      );
      assert.deepEqual(attached.snapshot.terminal, expected.terminal);
    } finally {
      attached.close();
    }
  });
}

test("structurally legal but unrepresentable seed reports actual local allocation rejection", async (context) => {
  const expected = header();
  // This exceeds the engine's representable ArrayBuffer, without allocating
  // physical memory or attempting an ordinary OOM/overcommit experiment.
  expected.terminal.checkpoint.restore_bytes = Number.MAX_SAFE_INTEGER;
  const client = await fixture(context, "terminal", expected, [], {
    restoreBytes: Number.MAX_SAFE_INTEGER,
  });
  await assert.rejects(client.attachTerminal(ID), (error) => {
    assert.ok(error instanceof CtxmuxTerminalSeedResourceError);
    assert.equal(error.reason, "allocation");
    assert.equal(error.requestedBytes, Number.MAX_SAFE_INTEGER);
    assert.equal(error.limitBytes, Number.MAX_SAFE_INTEGER);
    assert.ok(error.cause instanceof RangeError);
    return true;
  });
});

function longGeometryTail(): BasicHeader {
  const expected = header();
  expected.resize_revision = 1025;
  expected.terminal.resizes = Array.from({ length: 1025 }, (_, index) => ({
    through_byte: 101,
    resize_revision: index + 1,
    size: { rows: 6, cols: 15 },
  }));
  return expected;
}
test("terminal consumer accepts more than the historical 1024 contiguous resizes", async (context) => {
  const expected = longGeometryTail();
  const client = await fixture(context, "terminal", expected, chunks());
  const attached = await client.attachTerminal(ID);
  try {
    assert.deepEqual(attached.snapshot.terminal, expected.terminal);
    assert.deepEqual(attached.snapshot.terminal_restore, new Uint8Array(seed));
  } finally {
    attached.close();
  }
});
for (const [name, mutate] of [
  [
    "foreign identity",
    (h: BasicHeader) => {
      h.terminal.checkpoint.run_id = OTHER;
    },
  ],
  [
    "unsafe byte length",
    (h: BasicHeader) => {
      h.terminal.checkpoint.restore_bytes = Number.MAX_SAFE_INTEGER + 1;
    },
  ],
  [
    "revision hole beyond 1024",
    (h: BasicHeader) => {
      h.terminal.resizes[1024]!.resize_revision += 1;
    },
  ],
  [
    "backward byte fence beyond 1024",
    (h: BasicHeader) => {
      h.terminal.resizes[1024]!.through_byte = 100;
    },
  ],
] as const) {
  test(`terminal structural ${name} wins over local resource pressure`, async (context) => {
    const expected = longGeometryTail();
    mutate(expected);
    const client = await fixture(context, "terminal", expected, [], {
      restoreBytes: 0,
    });
    await assert.rejects(client.attachTerminal(ID), CtxmuxInvalidFrameError);
  });
}
test("zero seed receive limit preserves raw attachment and exact original replay", async (context) => {
  const expected = { ...header(), terminal: { type: "not_requested" } };
  const client = await fixture(context, "raw", expected, chunks().slice(2), {
    restoreBytes: 0,
  });
  const attached = await client.attach(ID);
  try {
    assert.equal(attached.snapshot.terminal_restore.length, 0);
    assert.deepEqual(
      attached.snapshot.replay.chunks[0]?.data,
      new Uint8Array(Buffer.from("xy")),
    );
  } finally {
    attached.close();
  }
});
test("terminal seed receive policy copies the caller's limit", async (context) => {
  const limits = { restoreBytes: seed.length };
  const client = await fixture(context, "terminal", header(), chunks(), limits);
  limits.restoreBytes = 0;
  const attached = await client.attachTerminal(ID);
  try {
    assert.deepEqual(attached.snapshot.terminal_restore, new Uint8Array(seed));
  } finally {
    attached.close();
  }
});
for (const invalidLimit of [
  null,
  [],
  { other: 1 },
  { restoreBytes: null },
  { restoreBytes: -1 },
  { restoreBytes: 0.5 },
  { restoreBytes: Number.MAX_SAFE_INTEGER + 1 },
  { restoreBytes: "32" },
  { restoreBytes: Infinity },
  { restoreBytes: NaN },
]) {
  test(`terminal seed limits reject invalid runtime configuration ${JSON.stringify(invalidLimit)}`, () => {
    assert.throws(
      () =>
        new CtxmuxClient({
          socketPath: "unused-validation-socket",
          terminalSeedLimits: invalidLimit as unknown as TerminalSeedLimits,
        }),
      TypeError,
    );
  });
}
