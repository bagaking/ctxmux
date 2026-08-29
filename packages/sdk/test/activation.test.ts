import assert from "node:assert/strict";
import {
  execFile as execFileCallback,
  spawn,
  type ChildProcess,
} from "node:child_process";
import { once } from "node:events";
import {
  chmod,
  lstat,
  mkdir,
  mkdtemp,
  readFile,
  rm,
  symlink,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { promisify } from "node:util";
import test from "node:test";

import {
  Attachment,
  CtxmuxActivationConflictError,
  CtxmuxActivationError,
  CtxmuxActivationLaunchError,
  CtxmuxActivationOwnershipError,
  CtxmuxActivationReadinessError,
  CtxmuxActivationTargetError,
  CtxmuxClient,
  activateRuntime,
  defineRun,
  type RuntimeActivation,
} from "../src/index.ts";

const execFile = promisify(execFileCallback);
const repositoryRoot = resolve(import.meta.dirname, "../../..");
const daemonBinary =
  process.env.CTXMUXD_BIN ?? join(repositoryRoot, "target/debug/ctxmuxd");
const testTimeScale = readTestTimeScale();

/**
 * Mirror ctxmux-test-support::scaled for TypeScript waits.
 *
 * Only budgets that bound how long a wanted outcome may take go through this
 * helper. A deadline whose elapsing is the assertion (the timeout-launcher
 * case below) stays literal.
 */
function scaled(baseMs: number): number {
  const value = baseMs * testTimeScale;
  return Number.isSafeInteger(value) ? value : Number.MAX_SAFE_INTEGER;
}

function readTestTimeScale(): number {
  const raw = process.env.CTXMUX_TEST_TIME_SCALE;
  if (raw === undefined) return 1;
  if (!/^\+?\d+$/u.test(raw)) {
    throw new Error(
      `CTXMUX_TEST_TIME_SCALE must be an unsigned integer, got ${JSON.stringify(raw)}`,
    );
  }
  const parsed = Number(raw);
  if (!Number.isSafeInteger(parsed)) {
    throw new Error(
      `CTXMUX_TEST_TIME_SCALE is too large for a safe JavaScript integer, got ${JSON.stringify(raw)}`,
    );
  }
  return Math.max(parsed, 1);
}

test(
  "activation starts one Runtime, detaches a Run, and reconnects with replay",
  { timeout: scaled(20_000) },
  async (context) => {
    const directory = await temporaryDirectory(
      context,
      "ctxmux-sdk-activation-",
    );
    const socketPath = join(directory, "ctxmux.sock");
    let activation: RuntimeActivation | undefined;
    context.after(async () => {
      await shutdownOwned(activation);
    });

    activation = await activateRuntime({
      executable: daemonBinary,
      socketPath,
      timeoutMs: scaled(5_000),
      childDisposition: { mode: "detached", stderr: "pipe" },
      env: { CTXMUX_ACTIVATION_TEST: "one" },
    });
    assert.equal(activation.spawned, true);
    assert.ok(activation.childPid !== undefined && activation.childPid > 0);

    const run = await activation.client.start(
      defineRun("/bin/sh", {
        args: [
          "-c",
          "printf 'ACTIVATION_REPLAY\\n'; while IFS= read -r line; do printf 'ACTIVATION:%s\\n' \"$line\"; done",
        ],
        cwd: directory,
      }),
    );
    const firstAttachment = await activation.client.attach(run.id);
    await waitForOutput(firstAttachment, "ACTIVATION_REPLAY");
    firstAttachment.close();
    await waitForNoAttachments(activation.client, run.id);

    await activation.dispose();
    assert.equal(activation.disposed, true);
    assert.equal(
      await activation.client.status(run.id).then((value) => value.state.type),
      "running",
    );

    const reconnected = new CtxmuxClient({ socketPath });
    const secondAttachment = await reconnected.attach(run.id);
    const replay = replayText(secondAttachment);
    assert.match(replay, /ACTIVATION_REPLAY/);
    await secondAttachment.input("reconnected\n");
    await waitForOutput(secondAttachment, "ACTIVATION:reconnected");
    secondAttachment.close();
  },
);

test(
  "live compatible Runtime is reused and incompatible requirements fail closed",
  { timeout: scaled(20_000) },
  async (context) => {
    const directory = await temporaryDirectory(
      context,
      "ctxmux-sdk-activation-",
    );
    const socketPath = join(directory, "ctxmux.sock");
    let owner: RuntimeActivation | undefined;
    context.after(async () => {
      await shutdownOwned(owner);
    });

    owner = await activateRuntime({
      executable: daemonBinary,
      socketPath,
      timeoutMs: scaled(5_000),
    });
    const reused = await activateRuntime({
      executable: "/does/not/need/to/be/used",
      socketPath,
      expectedRuntimeIdentity: owner.runtime,
      expectedBuildId: owner.runtime.buildId,
      requiredCapabilities: { "native.start": 1 },
      timeoutMs: scaled(1_000),
    });
    assert.equal(reused.spawned, false);
    assert.equal(reused.childPid, undefined);
    assert.deepEqual(reused.runtime, owner.runtime);
    await reused.dispose();
    await assert.rejects(
      reused.shutdown(),
      (error: unknown) =>
        error instanceof CtxmuxActivationOwnershipError &&
        error.code === "runtime_conflict",
    );

    const wrongIdentity = { ...owner.runtime, buildId: "ctxmuxd/other" };
    await assert.rejects(
      activateRuntime({
        executable: daemonBinary,
        socketPath,
        expectedRuntimeIdentity: wrongIdentity,
        timeoutMs: scaled(1_000),
      }),
      (error: unknown) =>
        error instanceof CtxmuxActivationConflictError &&
        error.reason === "identity_mismatch" &&
        error.actual?.daemonInstanceId === owner?.runtime.daemonInstanceId,
    );
    await assert.rejects(
      activateRuntime({
        executable: daemonBinary,
        socketPath,
        expectedBuildId: "ctxmuxd/other",
        timeoutMs: scaled(1_000),
      }),
      (error: unknown) =>
        error instanceof CtxmuxActivationConflictError &&
        error.reason === "build_mismatch" &&
        error.actual?.buildId === owner?.runtime.buildId,
    );
    await assert.rejects(
      activateRuntime({
        executable: daemonBinary,
        socketPath,
        requiredCapabilities: { "native.start": 2 },
        timeoutMs: scaled(1_000),
      }),
      (error: unknown) =>
        error instanceof CtxmuxActivationConflictError &&
        error.reason === "capability_mismatch" &&
        error.capability === "native.start" &&
        error.requiredVersion === 2 &&
        error.advertisedVersion === 1,
    );

    await owner.client.ping();
  },
);

test(
  "concurrent activators converge on one public Runtime",
  { timeout: scaled(20_000) },
  async (context) => {
    const directory = await temporaryDirectory(
      context,
      "ctxmux-sdk-activation-",
    );
    const socketPath = join(directory, "ctxmux.sock");
    const activations = await Promise.all(
      Array.from({ length: 10 }, () =>
        activateRuntime({
          executable: daemonBinary,
          socketPath,
          timeoutMs: scaled(8_000),
        }),
      ),
    );
    context.after(async () => {
      for (const activation of activations) {
        await shutdownOwned(activation);
      }
    });

    const instances = new Set(
      activations.map((activation) => activation.runtime.daemonInstanceId),
    );
    assert.equal(instances.size, 1);
    assert.equal(
      activations.filter((activation) => activation.spawned).length,
      1,
    );
    const winner = activations.find((activation) => activation.spawned);
    assert.ok(winner);
    for (const activation of activations) {
      assert.deepEqual(activation.runtime, winner.runtime);
      await activation.client.ping();
    }
  },
);

test(
  "a stale Unix socket is replaced only by the daemon's guarded startup",
  { timeout: scaled(20_000) },
  async (context) => {
    const directory = await temporaryDirectory(
      context,
      "ctxmux-sdk-activation-",
    );
    const socketPath = join(directory, "ctxmux.sock");
    await createStaleSocket(socketPath);
    assert.equal((await lstat(socketPath)).isSocket(), true);

    let activation: RuntimeActivation | undefined;
    context.after(async () => {
      await shutdownOwned(activation);
    });
    activation = await activateRuntime({
      executable: daemonBinary,
      socketPath,
      timeoutMs: scaled(5_000),
    });
    assert.equal(activation.spawned, true);
    await activation.client.ping();
    assert.equal((await lstat(socketPath)).isSocket(), true);
  },
);

test(
  "ordinary files, directories, and symlinks remain unchanged",
  { timeout: scaled(20_000) },
  async (context) => {
    const directory = await temporaryDirectory(
      context,
      "ctxmux-sdk-activation-",
    );
    const ordinary = join(directory, "ordinary");
    await writeFile(ordinary, "do not replace\n");
    const nested = join(directory, "nested");
    await writeFile(nested, "symlink target\n");
    const link = join(directory, "link");
    await symlink(nested, link);
    const folder = join(directory, "folder");
    await mkdir(folder);

    for (const [path, kind] of [
      [ordinary, "ordinary_file"],
      [link, "symlink"],
      [folder, "directory"],
    ] as const) {
      const before = await lstat(path);
      await assert.rejects(
        activateRuntime({
          executable: daemonBinary,
          socketPath: path,
          timeoutMs: scaled(500),
        }),
        (error: unknown) =>
          error instanceof CtxmuxActivationTargetError &&
          error.code === "unsafe_target" &&
          error.targetKind === kind,
      );
      const after = await lstat(path);
      assert.equal(after.dev, before.dev);
      assert.equal(after.ino, before.ino);
      if (kind === "ordinary_file") {
        assert.equal(await readFile(path, "utf8"), "do not replace\n");
      }
    }
  },
);

test(
  "permission failures and incompatible live targets are not terminated",
  { timeout: scaled(20_000) },
  async (context) => {
    const directory = await temporaryDirectory(
      context,
      "ctxmux-sdk-activation-",
    );
    const socketPath = join(directory, "ctxmux.sock");
    let owner: RuntimeActivation | undefined;
    context.after(async () => {
      await shutdownOwned(owner);
    });
    owner = await activateRuntime({
      executable: daemonBinary,
      socketPath,
      timeoutMs: scaled(5_000),
    });

    await chmod(socketPath, 0o000);
    try {
      await assert.rejects(
        activateRuntime({
          executable: daemonBinary,
          socketPath,
          timeoutMs: scaled(1_000),
        }),
        (error: unknown) =>
          error instanceof CtxmuxActivationTargetError &&
          error.code === "permission_denied" &&
          error.targetKind === "permission_denied",
      );
    } finally {
      await chmod(socketPath, 0o600);
    }
    await owner.client.ping();

    await assert.rejects(
      activateRuntime({
        executable: daemonBinary,
        socketPath,
        expectedBuildId: "ctxmuxd/unrelated",
        timeoutMs: scaled(1_000),
      }),
      (error: unknown) =>
        error instanceof CtxmuxActivationConflictError &&
        error.reason === "build_mismatch",
    );
    await owner.client.ping();
  },
);

test(
  "timeout, crashed launchers, and readiness mismatch clean only owned startup",
  // This test drives four launchers in sequence, and each one's budget must
  // clear worst-case spawn latency rather than race it. Those budgets sum to
  // more than the 25s the other tests use, so the outer timeout is raised to
  // stay a backstop against a genuine hang instead of a second race.
  { timeout: scaled(60_000) },
  async (context) => {
    const directory = await temporaryDirectory(
      context,
      "ctxmux-sdk-activation-",
    );
    const unrelatedDirectory = await temporaryDirectory(
      context,
      "ctxmux-sdk-activation-peer-",
    );
    const unrelatedSocket = join(unrelatedDirectory, "ctxmux.sock");
    let unrelated: RuntimeActivation | undefined;
    context.after(async () => {
      await shutdownOwned(unrelated);
    });
    unrelated = await activateRuntime({
      executable: daemonBinary,
      socketPath: unrelatedSocket,
      timeoutMs: scaled(5_000),
    });

    const timeoutSocket = join(directory, "timeout.sock");
    const pidFile = join(directory, "timeout.pid");
    const timeoutLauncher = await launcher(
      directory,
      "hang-launcher.sh",
      `echo $$ > "$CTXMUX_ACTIVATION_PID_FILE"
trap '' INT TERM
sleep 30`,
    );
    // Ordering, not budget, is what makes this test deterministic. The launcher
    // records its pid and only then hangs; the assertions below need that pid to
    // prove teardown reached the process. If the readiness deadline fires while
    // the shell is still starting, activation correctly kills the group before
    // the pid is ever written, and no later wait can recover it — the file is
    // gone for good, so the test fails on a missing side effect rather than on
    // the cleanup it means to check. Waiting for the pid *here*, before the
    // budget can elapse, removes the race instead of widening it. The launcher
    // ignores INT/TERM so it still never becomes ready, and the readiness
    // timeout below is still the thing being asserted.
    const pidWritten = waitForCondition(() => exists(pidFile));
    // The budget must clear worst-case shell startup, measured at 104-709ms
    // here, so the timeout is reached by a launcher that is genuinely up and
    // refusing to signal readiness rather than by one still being spawned.
    await assert.rejects(
      activateRuntime({
        executable: timeoutLauncher,
        socketPath: timeoutSocket,
        timeoutMs: scaled(3_000),
        env: { CTXMUX_ACTIVATION_PID_FILE: pidFile },
      }),
      (error: unknown) =>
        error instanceof CtxmuxActivationReadinessError &&
        error.code === "readiness_timeout",
    );
    await pidWritten;
    const timedOutPid = Number(await readFile(pidFile, "utf8"));
    await waitForCondition(() => !processIsRunning(timedOutPid));
    assert.equal(await exists(timeoutSocket), false);
    await unrelated.client.ping();

    const crashSocket = join(directory, "crash.sock");
    const crashLauncher = await launcher(
      directory,
      "crash-launcher.sh",
      "printf 'launcher failed\\n' >&2\nexit 23",
    );
    // Same startup race as the budget above, seen from the other side. The
    // crash itself is detected correctly and fast — `exit` arrives ~180ms in on
    // a warm machine — but spawning this launcher has been measured taking
    // 2.0-2.6s under load, and a crash reported after the deadline loses to the
    // timeout. That produced `readiness_timeout` instead of `launcher_exited`
    // once every few runs, which reads like a logic bug in the crash path and is
    // not one. The budget must clear worst-case spawn latency so the assertion
    // is about which error the crash produces, never about who won the race.
    await assert.rejects(
      activateRuntime({
        executable: crashLauncher,
        socketPath: crashSocket,
        timeoutMs: scaled(10_000),
      }),
      (error: unknown) =>
        error instanceof CtxmuxActivationLaunchError &&
        error.code === "launcher_exited" &&
        error.exitCode === 23 &&
        error.stderr?.includes("launcher failed") === true,
    );
    assert.equal(await exists(crashSocket), false);
    await unrelated.client.ping();

    const mismatchSocket = join(directory, "mismatch.sock");
    const mismatchLauncher = await launcher(
      directory,
      "mismatch-launcher.sh",
      `printf '%s\\n' '{"schema":"ctxmux.daemon-ready.v1","daemon_instance":"00000000-0000-4000-8000-000000000000"}' >&3
exec 3>&-
socket=''
while [ "$#" -gt 0 ]; do
  if [ "$1" = "--socket" ]; then socket="$2"; shift 2; else shift; fi
done
exec "$CTXMUXD_REAL" --socket "$socket"`,
    );
    await assert.rejects(
      activateRuntime({
        executable: mismatchLauncher,
        socketPath: mismatchSocket,
        timeoutMs: scaled(5_000),
        env: { CTXMUXD_REAL: daemonBinary },
      }),
      (error: unknown) =>
        error instanceof CtxmuxActivationReadinessError &&
        error.code === "readiness_mismatch" &&
        error.readinessKind === "mismatch" &&
        error.readinessInstance === "00000000-0000-4000-8000-000000000000" &&
        error.runtimeInstance !== undefined,
    );
    assert.equal(await exists(mismatchSocket), false);
    await unrelated.client.ping();
  },
);

test(
  "a crashed launcher with no winner is reported without spending the deadline",
  { timeout: scaled(40_000) },
  async (context) => {
    const directory = await temporaryDirectory(
      context,
      "ctxmux-sdk-activation-",
    );
    const crashLauncher = await launcher(
      directory,
      "deadline-crash-launcher.sh",
      "printf 'launcher failed\\n' >&2\nexit 23",
    );

    // The sibling crash assertion above proves *which* error a crash produces.
    // This one proves *when*: the search for a concurrent winner used to run to
    // the caller's deadline whenever the socket path stayed absent, so a
    // launcher that died in milliseconds was reported only once the whole
    // budget had elapsed — a default activation turned a 200ms fact into a 30s
    // wait. The bound below is deliberately far above the measured cost
    // (0.5-1.0s) and far under the budget it guards, so it fails on the
    // regression rather than on scheduling noise.
    //
    // A literal budget is correct here: this asserts the deadline is NOT spent,
    // so scaling it would scale the very quantity under test.
    const generousBudget = 30_000;
    const started = performance.now();
    await assert.rejects(
      activateRuntime({
        executable: crashLauncher,
        socketPath: join(directory, "deadline-crash.sock"),
        timeoutMs: generousBudget,
      }),
      (error: unknown) =>
        error instanceof CtxmuxActivationLaunchError &&
        error.code === "launcher_exited" &&
        error.exitCode === 23,
    );
    const elapsed = performance.now() - started;
    assert.ok(
      elapsed < scaled(10_000),
      `crashed launcher reported after ${Math.round(elapsed)}ms of a ${generousBudget}ms budget`,
    );
  },
);

test(
  "a crashed launcher is reported promptly even when a corpse socket is present",
  { timeout: scaled(40_000) },
  async (context) => {
    const directory = await temporaryDirectory(
      context,
      "ctxmux-sdk-activation-",
    );
    const socketPath = join(directory, "corpse.sock");
    const crashLauncher = await launcher(
      directory,
      "corpse-crash-launcher.sh",
      "printf 'launcher failed\\n' >&2\nexit 23",
    );

    // The sibling test above covers only an absent path, because the path it
    // names is never created. That left the more common shape uncovered: a
    // daemon killed without unlinking leaves a socket file behind, so the next
    // activation sees a path that exists. Connecting to it is *refused*, which
    // means nothing is listening — but the winner search used to treat any
    // present path as a winner mid-handshake and promote itself to the caller's
    // full deadline. The stall then scaled with the budget (a 5s budget took
    // 5002ms, a default 30s one took ~30s) on a realistic compound failure.
    await createStaleSocket(socketPath);
    assert.equal((await lstat(socketPath)).isSocket(), true);

    // A literal budget is correct here for the same reason as the sibling test:
    // this asserts the deadline is NOT spent, so scaling it would scale the very
    // quantity under test.
    const generousBudget = 30_000;
    const started = performance.now();
    await assert.rejects(
      activateRuntime({
        executable: crashLauncher,
        socketPath,
        timeoutMs: generousBudget,
      }),
      (error: unknown) =>
        error instanceof CtxmuxActivationLaunchError &&
        error.code === "launcher_exited" &&
        error.exitCode === 23,
    );
    const elapsed = performance.now() - started;
    assert.ok(
      elapsed < scaled(10_000),
      `crashed launcher with a corpse socket reported after ${Math.round(elapsed)}ms of a ${generousBudget}ms budget`,
    );
  },
);

test(
  "a clean consumer process can exit while its daemon-owned Run survives",
  { timeout: scaled(25_000) },
  async (context) => {
    const directory = await temporaryDirectory(
      context,
      "ctxmux-sdk-activation-",
    );
    const socketPath = join(directory, "ctxmux.sock");
    const sourcePath = join(repositoryRoot, "packages/sdk/src/index.ts");
    const childSource = `
import { activateRuntime, defineRun } from ${JSON.stringify(sourcePath)};
const activation = await activateRuntime({
  executable: process.env.CTXMUXD_BIN,
  socketPath: process.env.CTXMUX_ACTIVATION_SOCKET,
  timeoutMs: ${scaled(8_000)},
});
const run = await activation.client.start(defineRun("/bin/sh", {
  args: ["-c", "printf 'CHILD_REPLAY\\\\n'; while IFS= read -r line; do printf 'CHILD:%s\\\\n' \\\"$line\\\"; done"],
}));
console.log(JSON.stringify({
  runId: run.id,
  daemonPid: activation.childPid,
  daemonInstanceId: activation.runtime.daemonInstanceId,
}));
await activation.dispose();
`;
    const result = await execFile(
      process.execPath,
      ["--import", "tsx", "--input-type=module", "--eval", childSource],
      {
        cwd: repositoryRoot,
        env: {
          ...process.env,
          CTXMUXD_BIN: daemonBinary,
          CTXMUX_ACTIVATION_SOCKET: socketPath,
        },
      },
    );
    const record = JSON.parse(result.stdout.trim()) as {
      readonly runId: string;
      readonly daemonPid: number;
      readonly daemonInstanceId: string;
    };
    assert.ok(record.daemonPid > 0);
    assert.notEqual(record.runId, "");

    const fresh = new CtxmuxClient({ socketPath });
    const attachment = await fresh.attach(record.runId);
    await waitForOutput(attachment, "CHILD_REPLAY");
    await attachment.input("later\n");
    await waitForOutput(attachment, "CHILD:later");
    attachment.close();
    assert.equal(
      (await fresh.runtimeInfo()).daemonInstanceId,
      record.daemonInstanceId,
    );

    killProcessGroup(record.daemonPid);
    await waitForCondition(() => !processIsRunning(record.daemonPid));
  },
);

async function temporaryDirectory(
  context: test.TestContext,
  prefix: string,
): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), prefix));
  context.after(async () => {
    await rm(directory, { recursive: true, force: true });
  });
  return directory;
}

async function launcher(
  directory: string,
  name: string,
  body: string,
): Promise<string> {
  const path = join(directory, name);
  await writeFile(path, `#!/bin/sh\nset -eu\n${body}\n`);
  await chmod(path, 0o755);
  return path;
}

async function createStaleSocket(socketPath: string): Promise<void> {
  const child = spawn(
    process.execPath,
    [
      "--input-type=module",
      "--eval",
      `import { createServer } from "node:net";
const server = createServer();
server.listen(process.env.CTXMUX_STALE_SOCKET, () => process.stdout.write("ready\\n"));
setInterval(() => {}, 1000);`,
    ],
    {
      env: { ...process.env, CTXMUX_STALE_SOCKET: socketPath },
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
  assert.ok(child.stdout);
  await waitForStreamText(child.stdout, "ready");
  child.kill("SIGKILL");
  await waitForExit(child);
}

async function waitForStreamText(
  stream: NodeJS.ReadableStream,
  expected: string,
): Promise<void> {
  let content = "";
  await new Promise<void>((resolvePromise, reject) => {
    const onData = (chunk: Buffer | string): void => {
      content += typeof chunk === "string" ? chunk : chunk.toString("utf8");
      if (content.includes(expected)) {
        stream.off("data", onData);
        stream.off("error", onError);
        resolvePromise();
      }
    };
    const onError = (error: Error): void => {
      stream.off("data", onData);
      reject(error);
    };
    stream.on("data", onData);
    stream.once("error", onError);
  });
}

async function waitForExit(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) return;
  await once(child, "exit");
}

async function shutdownOwned(
  activation: RuntimeActivation | undefined,
): Promise<void> {
  if (activation?.spawned === true) {
    await activation.shutdown().catch(() => {});
  }
}

async function waitForNoAttachments(
  client: CtxmuxClient,
  runId: string,
): Promise<void> {
  await waitForCondition(
    async () => (await client.status(runId)).attachments === 0,
  );
}

async function waitForOutput(
  attachment: Attachment,
  expected: string,
): Promise<string> {
  let observed = replayText(attachment);
  const deadline = Date.now() + scaled(5_000);
  while (!observed.includes(expected)) {
    if (Date.now() > deadline) {
      throw new Error(
        `timed out waiting for ${expected}; observed ${observed}`,
      );
    }
    const event = await attachment.nextEvent();
    if (event === undefined) {
      throw new Error(`attachment closed before ${expected}`);
    }
    if (event.type === "output") {
      observed += Buffer.from(event.chunk.data).toString("utf8");
    } else if (event.type === "gap") {
      throw new Error(`unexpected output gap at ${event.latest_output_bytes}`);
    } else if (event.type === "exited") {
      throw new Error(`Run exited before ${expected}`);
    }
  }
  return observed;
}

function replayText(attachment: Attachment): string {
  return attachment.snapshot.replay.chunks
    .map((chunk) => Buffer.from(chunk.data).toString("utf8"))
    .join("");
}

async function waitForCondition(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs = scaled(5_000),
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() <= deadline) {
    if (await predicate()) return;
    await delay(20);
  }
  throw new Error("observed condition did not become true before deadline");
}

async function exists(path: string): Promise<boolean> {
  try {
    await lstat(path);
    return true;
  } catch (error) {
    if (
      typeof error === "object" &&
      error !== null &&
      "code" in error &&
      error.code === "ENOENT"
    ) {
      return false;
    }
    throw error;
  }
}

function processIsRunning(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    if (
      typeof error === "object" &&
      error !== null &&
      "code" in error &&
      error.code === "ESRCH"
    ) {
      return false;
    }
    throw error;
  }
}

function killProcessGroup(pid: number): void {
  try {
    process.kill(-pid, "SIGINT");
  } catch (error) {
    if (
      typeof error === "object" &&
      error !== null &&
      "code" in error &&
      error.code === "ESRCH"
    ) {
      return;
    }
    process.kill(pid, "SIGINT");
  }
}

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolvePromise) =>
    setTimeout(resolvePromise, milliseconds),
  );
}
