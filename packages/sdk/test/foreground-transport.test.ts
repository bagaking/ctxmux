import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { createServer, type Socket } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import {
  CtxmuxClient,
  PROTOCOL_VERSION,
  type RuntimeIdentity,
} from "@ctxmux/sdk";

const runId = "fbd067ef-4437-4e11-9450-eac87e699f61";
const runtime: RuntimeIdentity = {
  daemonInstanceId: "ea519054-5e27-4ed3-acbc-a0c947e82aeb",
  runtimeId: "2e6b89bf-e193-4a1f-8c9e-e55c9b66dcb2",
  runtimeIdPersistence: "state_dir",
  buildId: "ctxmuxd/test",
  protocolGeneration: PROTOCOL_VERSION,
  platform: "macos",
  arch: "aarch64",
  capabilities: { "native.start": 1 },
};

async function endpoint(reply: (frame: any, socket: Socket) => void) {
  const directory = await mkdtemp(join(tmpdir(), "ctxmux-foreground-wire-"));
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
function send(socket: Socket, value: unknown) {
  socket.write(JSON.stringify(value) + "\n");
}

test("missing optional observation capability leaves the original connection contract usable", async () => {
  let hellos = 0;
  let requests = 0;
  const owner = await endpoint((frame, socket) => {
    if (frame.type === "hello") {
      hellos++;
      send(socket, { type: "hello", runtime });
    } else requests++;
  });
  try {
    const client = new CtxmuxClient({ socketPath: owner.socketPath });
    assert.deepEqual(await client.observeForeground({ runId }), {
      runtime,
      observation: {
        outcome: "unsupported",
        runId,
        reason: "capability-missing",
      },
    });
    await client.ping();
    assert.equal(hellos, 2);
    assert.equal(requests, 0);
  } finally {
    await owner.close();
  }
});

test("observation accepts only its requested Run from the same Hello connection", async () => {
  let hellos = 0;
  let requests = 0;
  const identity = {
    ...runtime,
    capabilities: {
      ...runtime.capabilities,
      "native.foreground_observation": 1,
    },
  };
  const owner = await endpoint((frame, socket) => {
    if (frame.type === "hello") {
      hellos++;
      send(socket, { type: "hello", runtime: identity });
    } else {
      requests++;
      send(socket, {
        type: "response",
        response: {
          type: "foreground_observation",
          observation: {
            outcome: "unknown",
            runId: "b339a9fe-d516-4194-a0db-de81cd71b079",
            reason: "stale-scope",
          },
        },
      });
    }
  });
  try {
    await assert.rejects(
      new CtxmuxClient({ socketPath: owner.socketPath }).observeForeground({
        runId,
      }),
      /exact Run foreground observation/u,
    );
    assert.equal(hellos, 1);
    assert.equal(requests, 1);
    await assert.rejects(
      new CtxmuxClient({
        socketPath: owner.socketPath,
        expectedRuntimeIdentity: runtime,
      }).observeForeground({ runId }),
      /identity/u,
    );
    assert.equal(hellos, 2);
    assert.equal(
      requests,
      1,
      "wrong exact Runtime is rejected before observation dispatch",
    );
  } finally {
    await owner.close();
  }
});

test("physical scope rejects empty, duplicate, foreign and guessed image facts", async () => {
  const observed = {
    outcome: "observed",
    runId,
    startedAtMs: 12,
    completedAtMs: 13,
    rootPid: 123,
    rootIncarnation: "000000000000002b",
    posixSessionId: 123,
    foregroundPgid: 456,
    processes: [
      {
        pid: 456,
        processIncarnation: "000000000000003c",
        executionGeneration: "00000001",
        pgid: 456,
        sid: 123,
        executablePath: "/opt/example/native",
        executableImage: "0123456789abcdef0123456789abcdef",
      },
    ],
  };
  const frame = (observation: unknown) => ({
    type: "response",
    response: { type: "foreground_observation", observation },
  });
  const invalid = [
    { ...observed, processes: [] },
    { ...observed, processes: [observed.processes[0], observed.processes[0]] },
    { ...observed, processes: [{ ...observed.processes[0], sid: 789 }] },
    { ...observed, processes: [{ ...observed.processes[0], pgid: 789 }] },
    { ...observed, rootPid: 789 },
    { ...observed, completedAtMs: 11 },
    {
      ...observed,
      processes: [{ ...observed.processes[0], executionGeneration: "guess" }],
    },
    {
      ...observed,
      processes: [
        {
          ...observed.processes[0],
          executableImage: "00000000000000000000000000000000",
        },
      ],
    },
    {
      ...observed,
      processes: [{ ...observed.processes[0], executablePath: "" }],
    },
    { ...observed, argv: ["agent"] },
    { outcome: "unknown", runId, reason: "probably-agent" },
  ];
  assert.equal(invalid.length, 11);
  let current: unknown = observed;
  let requests = 0;
  const owner = await endpoint((incoming, socket) => {
    if (incoming.type === "hello")
      send(socket, {
        type: "hello",
        runtime: {
          ...runtime,
          capabilities: {
            ...runtime.capabilities,
            "native.foreground_observation": 1,
          },
        },
      });
    else {
      requests++;
      send(socket, frame(current));
    }
  });
  try {
    const client = new CtxmuxClient({ socketPath: owner.socketPath });
    const valid = await client.observeForeground({ runId });
    assert.deepEqual(valid.observation, observed);
    for (const invalidObservation of invalid) {
      current = invalidObservation;
      await assert.rejects(client.observeForeground({ runId }));
    }
    assert.equal(
      requests,
      12,
      "every nonempty counter crossed the compiled public SDK",
    );
  } finally {
    await owner.close();
  }
});
