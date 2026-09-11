import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { createServer, type Socket } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import {
  CtxmuxClient,
  CtxmuxUnsupportedCapabilityError,
  CtxmuxInvalidFrameError,
  PROTOCOL_VERSION,
  RUNTIME_CAPABILITY_STORAGE_OBSERVATION,
  type RunStorageObservation,
  type RuntimeIdentity,
} from "../src/index.ts";
import { validateServerFrame } from "../src/validation.ts";
const runId = "fbd067ef-4437-4e11-9450-eac87e699f61";
const runtime: RuntimeIdentity = {
  daemonInstanceId: "ea519054-5e27-4ed3-acbc-a0c947e82aeb",
  runtimeId: "2e6b89bf-e193-4a1f-8c9e-e55c9b66dcb2",
  runtimeIdPersistence: "state_dir",
  buildId: "ctxmuxd/test",
  protocolGeneration: PROTOCOL_VERSION,
  platform: "macos",
  arch: "aarch64",
  capabilities: { [RUNTIME_CAPABILITY_STORAGE_OBSERVATION]: 1 },
};
const observation: RunStorageObservation = {
  run_id: runId,
  latest_output_bytes: 50,
  memory_first_available_byte: 25,
  persistence: {
    first_failure: "unknown COMMIT",
    actor_stopped: false,
    queued_append_commands: 0,
    offered_output_bytes: 40,
    committed_output_bytes: 30,
  },
  policy: {
    run_output_bytes: 25,
    hot_output_bytes: 100,
    durable_run_output_bytes: 25,
    durable_replay_bytes: 100,
    database_bytes: 4096,
    wal_checkpoint_bytes: 4096,
    terminal_history_rows: 10000,
    terminal_checkpoint_bytes: 33554432,
  },
};
function frame(value: unknown) {
  return {
    type: "response",
    response: { type: "storage_observation", observation: value },
  };
}
async function endpoint(reply: (frame: any, socket: Socket) => void) {
  const directory = await mkdtemp(join(tmpdir(), "ctxmux-storage-wire-"));
  const socketPath = join(directory, "r.sock");
  const sockets = new Set<Socket>();
  const server = createServer((socket) => {
    sockets.add(socket);
    socket.on("error", () => {});
    socket.on("close", () => sockets.delete(socket));
    let pending = "";
    socket.on("data", (chunk) => {
      pending += chunk.toString("utf8");
      while (pending.includes("\n")) {
        const end = pending.indexOf("\n");
        const line = pending.slice(0, end);
        pending = pending.slice(end + 1);
        reply(JSON.parse(line), socket);
      }
    });
  });
  await new Promise<void>((resolve) => server.listen(socketPath, resolve));
  return {
    socketPath,
    async close() {
      sockets.forEach((s) => s.destroy());
      await new Promise<void>((resolve) => server.close(() => resolve()));
      await rm(directory, { recursive: true, force: true });
    },
  };
}
const send = (socket: Socket, value: unknown) =>
  socket.write(JSON.stringify(value) + "\n");
test("storage observation keeps nullable persistence and independent cursor facts explicit", () => {
  assert.deepEqual(validateServerFrame(frame(observation)), frame(observation));
  assert.doesNotThrow(() =>
    validateServerFrame(frame({ ...observation, persistence: null })),
  );
  for (const value of [
    { ...observation, persistence: undefined },
    { ...observation, memory_first_available_byte: 51 },
    {
      ...observation,
      persistence: { ...observation.persistence, first_failure: undefined },
    },
    { ...observation, policy: { ...observation.policy, extra: 1 } },
    { ...observation, latest_output_bytes: Number.MAX_SAFE_INTEGER + 1 },
  ])
    assert.throws(
      () => validateServerFrame(frame(value)),
      CtxmuxInvalidFrameError,
    );
  // An unknown COMMIT can leave disk ahead; no guessed relation rejects facts.
  assert.doesNotThrow(() =>
    validateServerFrame(
      frame({
        ...observation,
        persistence: {
          ...observation.persistence,
          offered_output_bytes: 30,
          committed_output_bytes: 40,
        },
      }),
    ),
  );
});
test("optional storage capability rejects before dispatch and preserves ordinary Hello", async () => {
  let requests = 0;
  const owner = await endpoint((message, socket) => {
    if (message.type === "hello")
      send(socket, {
        type: "hello",
        runtime: { ...runtime, capabilities: {} },
      });
    else requests++;
  });
  try {
    const client = new CtxmuxClient({ socketPath: owner.socketPath });
    await assert.rejects(
      client.observeStorage(runId),
      CtxmuxUnsupportedCapabilityError,
    );
    await client.ping();
    assert.equal(requests, 0);
  } finally {
    await owner.close();
  }
});
test("storage observation uses one exact Hello owner and refuses another Run", async () => {
  let hellos = 0;
  let requests = 0;
  let returned = runId;
  const owner = await endpoint((message, socket) => {
    if (message.type === "hello") {
      hellos++;
      send(socket, { type: "hello", runtime });
    } else {
      requests++;
      assert.deepEqual(message.request, { type: "observe_storage", id: runId });
      send(socket, frame({ ...observation, run_id: returned }));
    }
  });
  try {
    const client = new CtxmuxClient({ socketPath: owner.socketPath });
    assert.deepEqual(await client.observeStorage(runId), {
      runtime,
      observation,
    });
    assert.equal(hellos, 1);
    assert.equal(requests, 1);
    returned = "b339a9fe-d516-4194-a0db-de81cd71b079";
    await assert.rejects(
      client.observeStorage(runId),
      /exact Run storage observation/u,
    );
    const wrongOwner = new CtxmuxClient({
      socketPath: owner.socketPath,
      expectedRuntimeIdentity: { ...runtime, buildId: "different-owner" },
    });
    await assert.rejects(
      wrongOwner.observeStorage(runId),
      new RegExp("identity", "u"),
    );
    assert.equal(
      requests,
      2,
      "identity mismatch refuses before business dispatch",
    );
  } finally {
    await owner.close();
  }
});
