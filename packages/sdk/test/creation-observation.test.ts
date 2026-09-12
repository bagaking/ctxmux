import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { createServer, type Socket } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import {
  CtxmuxClient,
  CtxmuxInvalidFrameError,
  CtxmuxUnsupportedCapabilityError,
  PROTOCOL_VERSION,
  RUNTIME_CAPABILITY_CREATION_OBSERVATION,
  createOperationKey,
  type RuntimeIdentity,
  type RunInfo,
} from "../src/index.ts";
import { validateServerFrame } from "../src/validation.ts";

const key = createOperationKey("original-creation");
const runtime: RuntimeIdentity = {
  daemonInstanceId: "ea519054-5e27-4ed3-acbc-a0c947e82aeb",
  runtimeId: "2e6b89bf-e193-4a1f-8c9e-e55c9b66dcb2",
  runtimeIdPersistence: "state_dir",
  buildId: "ctxmuxd/test",
  protocolGeneration: PROTOCOL_VERSION,
  platform: "macos",
  arch: "aarch64",
  capabilities: { [RUNTIME_CAPABILITY_CREATION_OBSERVATION]: 1 },
};
const run: RunInfo = {
  id: "fbd067ef-4437-4e11-9450-eac87e699f61",
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
  native_service: {
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
  },
  latest_output_bytes: 0,
  durable_output_bytes: null,
  first_available_byte: 0,
  attachments: 0,
  applied_input_bytes: 0,
  current_size: { cols: 80, rows: 24 },
};
function reply(socket: Socket, value: unknown) {
  socket.write(JSON.stringify(value) + "\n");
}
function observed(observation: unknown) {
  return {
    type: "response",
    response: { type: "creation_observation", observation },
  };
}
async function endpoint(handle: (frame: any, socket: Socket) => void) {
  const directory = await mkdtemp(join(tmpdir(), "ctxmux-creation-wire-"));
  const socketPath = join(directory, "r.sock"),
    sockets = new Set<Socket>();
  const server = createServer((socket) => {
    sockets.add(socket);
    socket.on("error", () => {});
    socket.on("close", () => sockets.delete(socket));
    let pending = "";
    socket.on("data", (part) => {
      pending += part.toString();
      while (pending.includes("\n")) {
        const index = pending.indexOf("\n"),
          frame = JSON.parse(pending.slice(0, index));
        pending = pending.slice(index + 1);
        handle(frame, socket);
      }
    });
  });
  await new Promise<void>((resolve) => server.listen(socketPath, resolve));
  return {
    socketPath,
    close: async () => {
      sockets.forEach((s) => s.destroy());
      await new Promise<void>((resolve) => server.close(() => resolve()));
      await rm(directory, { recursive: true });
    },
  };
}

test("creation observation validates retained and absent results without negative receipt inference", () => {
  for (const observation of [
    { outcome: "retained", operation_key: key, run },
    { outcome: "not_retained", operation_key: key },
  ]) {
    assert.deepEqual(
      validateServerFrame(observed(observation)),
      observed(observation),
    );
  }
  for (const observation of [
    { outcome: "not_applied", operation_key: key },
    { outcome: "not_retained", operation_key: "" },
    { outcome: "not_retained", operation_key: "界".repeat(43) },
    { outcome: "not_retained", operation_key: key, run },
    { outcome: "retained", operation_key: key },
  ])
    assert.throws(
      () => validateServerFrame(observed(observation)),
      CtxmuxInvalidFrameError,
    );
});

test("creation observation binds the echoed key and Hello from its only dispatch connection", async () => {
  const requests: unknown[] = [];
  let handshakes = 0;
  const peer = await endpoint((frame, socket) => {
    if (frame.type === "hello") {
      handshakes++;
      reply(socket, { type: "hello", runtime });
    } else {
      requests.push(frame.request);
      reply(socket, observed({ outcome: "retained", operation_key: key, run }));
    }
  });
  try {
    const result = await new CtxmuxClient({
      socketPath: peer.socketPath,
    }).observeCreation(key);
    assert.deepEqual(result, {
      runtime,
      observation: { outcome: "retained", operation_key: key, run },
    });
    assert.equal(handshakes, 1);
    assert.deepEqual(requests, [
      { type: "observe_creation", operation_key: key },
    ]);
  } finally {
    await peer.close();
  }
  const wrong = await endpoint((frame, socket) =>
    reply(
      socket,
      frame.type === "hello"
        ? { type: "hello", runtime }
        : observed({ outcome: "not_retained", operation_key: "neighbor" }),
    ),
  );
  try {
    await assert.rejects(
      new CtxmuxClient({ socketPath: wrong.socketPath }).observeCreation(key),
      /exact creation observation/u,
    );
  } finally {
    await wrong.close();
  }
});

test("missing creation capability rejects only this observation before dispatch", async () => {
  const requests: string[] = [];
  const peer = await endpoint((frame, socket) => {
    if (frame.type === "hello")
      reply(socket, {
        type: "hello",
        runtime: { ...runtime, capabilities: {} },
      });
    else {
      requests.push(frame.request.type);
      reply(socket, { type: "response", response: { type: "status", run } });
    }
  });
  try {
    const client = new CtxmuxClient({ socketPath: peer.socketPath });
    await assert.rejects(
      client.observeCreation(key),
      CtxmuxUnsupportedCapabilityError,
    );
    assert.equal((await client.status(run.id)).pid, run.pid);
    assert.deepEqual(requests, ["status"]);
  } finally {
    await peer.close();
  }
});

test("cancelled creation reader leaves the client and original Run observation usable", async () => {
  const controller = new AbortController(),
    requests: string[] = [];
  const peer = await endpoint((frame, socket) => {
    if (frame.type === "hello") reply(socket, { type: "hello", runtime });
    else {
      requests.push(frame.request.type);
      if (frame.request.type === "observe_creation") controller.abort();
      else
        reply(socket, { type: "response", response: { type: "status", run } });
    }
  });
  try {
    const client = new CtxmuxClient({ socketPath: peer.socketPath });
    await assert.rejects(
      client.observeCreation(key, { signal: controller.signal }),
    );
    assert.equal((await client.status(run.id)).pid, run.pid);
    assert.deepEqual(requests, ["observe_creation", "status"]);
  } finally {
    await peer.close();
  }
});
