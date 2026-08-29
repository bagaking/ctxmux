// The TypeScript SDK reaching an owner-host Runtime through a forwarded socket.
//
// The claim under test is that this needs no SDK change: a daemon is addressed by
// socket path, so forwarding that path is invisible to the client. That is easy to
// assert and easy to get wrong, so it is proven here against a real `ctxmuxd` and
// a real forwarding child process rather than derived from the argument shape.
//
// The forwarder is the same stand-in the Rust tests use. It speaks the production
// `-L <local>:<remote> -N <destination>` contract, so this file exercises the real
// argument shape without needing an SSH boundary. The shipped transport is the
// system `ssh` client, qualified separately by the real-OpenSSH lane.

import assert from "node:assert/strict";
import { spawn, type ChildProcess } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";

import {
  CtxmuxClient,
  CtxmuxUnsupportedCapabilityError,
  REMOTE_ENDPOINT_CONTRACT_VERSION,
  RUNTIME_CAPABILITY_NATIVE_START,
  type RuntimeIdentity,
} from "../src/index.ts";

function requiredEnvironment(name: string): string {
  const value = process.env[name];
  if (value === undefined || value.length === 0) {
    throw new Error(`${name} must be set to run this test`);
  }
  return value;
}

const daemonBinary = requiredEnvironment("CTXMUXD_BIN");
const forwarderBinary = requiredEnvironment("CTXMUX_FAKE_SSH_BIN");

const delay = async (milliseconds: number): Promise<void> =>
  await new Promise((resolve) => {
    setTimeout(resolve, milliseconds);
  });

/// Wait for a socket to answer, bounded, rather than assuming a delay is enough.
async function waitForSocket(
  socketPath: string,
  child: ChildProcess,
  label: string,
): Promise<void> {
  const deadline = Date.now() + 15_000;
  let lastError: unknown;
  while (Date.now() <= deadline) {
    if (child.exitCode !== null) {
      throw new Error(`${label} exited before becoming ready`);
    }
    try {
      await new CtxmuxClient({ socketPath }).ping();
      return;
    } catch (error) {
      lastError = error;
      await delay(20);
    }
  }
  throw new Error(`${label} never answered: ${String(lastError)}`);
}

async function reap(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null) return;
  child.kill("SIGKILL");
  await new Promise<void>((resolve) => {
    child.once("exit", () => resolve());
  });
}

test("a forwarded socket reaches the owner-host Runtime with no SDK change", async () => {
  const directory = await mkdtemp(path.join(tmpdir(), "ctxmux-remote-sdk-"));
  const ownerSocket = path.join(directory, "owner-host.sock");
  const forwardedSocket = path.join(directory, "forwarded.sock");
  let owner: ChildProcess | undefined;
  let forwarder: ChildProcess | undefined;

  try {
    owner = spawn(daemonBinary, ["--socket", ownerSocket], {
      stdio: ["ignore", "ignore", "pipe"],
    });
    await waitForSocket(ownerSocket, owner, "owner-host ctxmuxd");

    // The exact production argument shape, destination last.
    forwarder = spawn(
      forwarderBinary,
      [
        "-N",
        "-T",
        "-o",
        "BatchMode=yes",
        "-o",
        "ExitOnForwardFailure=yes",
        "-L",
        `${forwardedSocket}:${ownerSocket}`,
        "owner-host.test",
      ],
      { stdio: ["ignore", "ignore", "pipe"] },
    );
    await waitForSocket(forwardedSocket, forwarder, "forwarded socket");

    const direct: RuntimeIdentity = await new CtxmuxClient({
      socketPath: ownerSocket,
    }).runtimeInfo();
    const throughTunnel: RuntimeIdentity = await new CtxmuxClient({
      socketPath: forwardedSocket,
    }).runtimeInfo();

    assert.deepEqual(
      throughTunnel,
      direct,
      "the tunnel must reach the same Runtime, not a different endpoint",
    );

    // The daemon cannot know it is being reached through a tunnel, so it must not
    // advertise anything about the caller's network position. The endpoint
    // contract is a client-side fact instead.
    const remoteKeys = Object.keys(throughTunnel.capabilities).filter((key) =>
      key.startsWith("remote."),
    );
    assert.deepEqual(
      remoteKeys,
      [],
      "a daemon must not advertise a capability describing the caller's position",
    );
    assert.equal(REMOTE_ENDPOINT_CONTRACT_VERSION, 1);

    // Capability enforcement is unchanged through a tunnel: a requirement the
    // daemon does not satisfy is refused before any business frame.
    await assert.rejects(
      async () =>
        await new CtxmuxClient({
          socketPath: forwardedSocket,
          requiredCapabilities: {
            [RUNTIME_CAPABILITY_NATIVE_START]: Number.MAX_SAFE_INTEGER,
          },
        }).list(),
      CtxmuxUnsupportedCapabilityError,
      "an unsatisfiable capability must fail closed through the tunnel",
    );

    // A satisfiable requirement still dispatches, so the rejection above is the
    // capability check rather than the tunnel being unusable.
    const runs = await new CtxmuxClient({
      socketPath: forwardedSocket,
      requiredCapabilities: { [RUNTIME_CAPABILITY_NATIVE_START]: 1 },
    }).list();
    assert.ok(Array.isArray(runs));
  } finally {
    if (forwarder !== undefined) await reap(forwarder);
    if (owner !== undefined) await reap(owner);
    await rm(directory, { recursive: true, force: true });
  }
});
