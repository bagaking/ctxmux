import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  LOCAL_ARTIFACT_SCHEMA,
  MAX_BINARY_BYTES,
  MAX_MANIFEST_BYTES,
  MAX_SDK_ARCHIVE_BYTES,
  MAX_SDK_ENTRIES,
  MAX_SDK_UNPACKED_BYTES,
} from "./build-local-artifacts.mjs";

const root = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const builder = path.join(root, "scripts/build-local-artifacts.mjs");
const COMMAND_OUTPUT_LIMIT = 8 * 1024 * 1024;
const SHA256_PATTERN = /^[0-9a-f]{64}$/u;
const GIT_OBJECT_PATTERN = /^[0-9a-f]{40}$/u;

function run(command, args, { cwd, environment = process.env, timeout } = {}) {
  const result = spawnSync(command, args, {
    cwd,
    env: environment,
    encoding: "utf8",
    maxBuffer: COMMAND_OUTPUT_LIMIT,
    timeout,
  });
  if (result.error !== undefined) {
    throw new Error(`failed to run ${command}: ${result.error.message}`);
  }
  if (result.status !== 0) {
    throw new Error(
      `${command} ${args.join(" ")} failed (${String(result.status)}): ${result.stderr.trim() || result.stdout.trim() || "no diagnostic"}`,
    );
  }
  return result.stdout;
}

function sha256(filename) {
  return createHash("sha256").update(fs.readFileSync(filename)).digest("hex");
}

function exactKeys(value, keys, label) {
  assert.equal(value !== null && typeof value === "object", true, label);
  assert.deepEqual(Object.keys(value), keys, `${label} keys`);
}

function regularArtifact(rootDirectory, descriptor, maximumBytes, mode) {
  exactKeys(descriptor, ["path", "sha256", "bytes", "mode"], "artifact");
  assert.equal(path.isAbsolute(descriptor.path), false);
  assert.equal(descriptor.path.includes(".."), false);
  assert.match(descriptor.sha256, SHA256_PATTERN);
  assert.equal(Number.isSafeInteger(descriptor.bytes), true);
  assert.equal(descriptor.bytes > 0 && descriptor.bytes <= maximumBytes, true);
  assert.equal(descriptor.mode, mode);
  const absolute = path.join(rootDirectory, descriptor.path);
  assert.equal(path.relative(rootDirectory, absolute).startsWith(".."), false);
  const stat = fs.lstatSync(absolute);
  assert.equal(stat.isSymbolicLink(), false);
  assert.equal(stat.isFile(), true);
  assert.equal(stat.size, descriptor.bytes);
  assert.equal(sha256(absolute), descriptor.sha256);
  assert.equal(stat.mode & 0o777, mode === "0755" ? 0o755 : 0o644);
  return absolute;
}

function artifactFiles(directory) {
  const found = [];
  const pending = [directory];
  while (pending.length > 0) {
    const current = pending.pop();
    for (const entry of fs.readdirSync(current, { withFileTypes: true })) {
      const absolute = path.join(current, entry.name);
      const relative = path
        .relative(directory, absolute)
        .replaceAll(path.sep, "/");
      assert.equal(
        entry.isSymbolicLink(),
        false,
        `artifact symlink: ${relative}`,
      );
      if (entry.isDirectory()) pending.push(absolute);
      else {
        assert.equal(entry.isFile(), true, `non-file artifact: ${relative}`);
        found.push(relative);
      }
    }
  }
  return found.sort();
}

function verifyArtifactSet(directory) {
  const manifestPath = path.join(directory, "manifest.json");
  const manifestBytes = fs.readFileSync(manifestPath);
  assert.equal(manifestBytes.length > 0, true);
  assert.equal(manifestBytes.length <= MAX_MANIFEST_BYTES, true);
  const manifest = JSON.parse(manifestBytes.toString("utf8"));
  exactKeys(
    manifest,
    [
      "schema",
      "source",
      "product",
      "support",
      "build",
      "sdk",
      "binaries",
      "determinism",
    ],
    "manifest",
  );
  assert.equal(manifest.schema, LOCAL_ARTIFACT_SCHEMA);
  exactKeys(
    manifest.source,
    ["commit", "tree", "commit_time_unix", "worktree_clean"],
    "source",
  );
  assert.match(manifest.source.commit, GIT_OBJECT_PATTERN);
  assert.match(manifest.source.tree, GIT_OBJECT_PATTERN);
  assert.match(manifest.source.commit_time_unix, /^(0|[1-9][0-9]*)$/u);
  assert.equal(manifest.source.worktree_clean, true);
  assert.equal(
    manifest.source.commit,
    run("/usr/bin/git", ["rev-parse", "HEAD"], { cwd: root }).trim(),
  );
  assert.equal(
    manifest.source.tree,
    run("/usr/bin/git", ["rev-parse", "HEAD^{tree}"], { cwd: root }).trim(),
  );

  exactKeys(manifest.product, ["version", "protocol"], "product");
  assert.match(manifest.product.version, /^[0-9]+\.[0-9]+\.[0-9]+$/u);
  assert.equal(Number.isSafeInteger(manifest.product.protocol), true);
  exactKeys(
    manifest.support,
    ["platform", "architecture", "rust_target", "transport"],
    "support",
  );
  assert.equal(manifest.support.platform, process.platform);
  assert.equal(manifest.support.architecture, process.arch);
  assert.equal(manifest.support.transport, "unix");
  assert.equal(typeof manifest.support.rust_target, "string");
  assert.equal(manifest.support.rust_target.length > 0, true);
  exactKeys(
    manifest.build,
    ["profile", "locked", "source_date_epoch", "rustc", "cargo", "node", "npm"],
    "build",
  );
  assert.equal(manifest.build.profile, "release");
  assert.equal(manifest.build.locked, true);
  assert.equal(
    manifest.build.source_date_epoch,
    manifest.source.commit_time_unix,
  );
  for (const field of ["rustc", "cargo", "node", "npm"]) {
    assert.equal(typeof manifest.build[field], "string");
    assert.equal(manifest.build[field].length > 0, true);
  }

  exactKeys(
    manifest.sdk,
    ["name", "version", "protocol", "entry_count", "unpacked_bytes", "archive"],
    "sdk",
  );
  assert.equal(manifest.sdk.name, "@ctxmux/sdk");
  assert.equal(manifest.sdk.protocol, manifest.product.protocol);
  assert.equal(
    Number.isSafeInteger(manifest.sdk.entry_count) &&
      manifest.sdk.entry_count > 0 &&
      manifest.sdk.entry_count <= MAX_SDK_ENTRIES,
    true,
  );
  assert.equal(
    Number.isSafeInteger(manifest.sdk.unpacked_bytes) &&
      manifest.sdk.unpacked_bytes > 0 &&
      manifest.sdk.unpacked_bytes <= MAX_SDK_UNPACKED_BYTES,
    true,
  );
  const sdkArchive = regularArtifact(
    directory,
    manifest.sdk.archive,
    MAX_SDK_ARCHIVE_BYTES,
    "0644",
  );

  assert.equal(Array.isArray(manifest.binaries), true);
  assert.equal(manifest.binaries.length, 2);
  const binaries = new Map();
  for (const descriptor of manifest.binaries) {
    exactKeys(
      descriptor,
      ["name", "version", "protocol", "path", "sha256", "bytes", "mode"],
      "binary",
    );
    assert.equal(["ctxmux", "ctxmuxd"].includes(descriptor.name), true);
    assert.equal(binaries.has(descriptor.name), false);
    assert.equal(descriptor.version, manifest.product.version);
    assert.equal(descriptor.protocol, manifest.product.protocol);
    binaries.set(
      descriptor.name,
      regularArtifact(
        directory,
        {
          path: descriptor.path,
          sha256: descriptor.sha256,
          bytes: descriptor.bytes,
          mode: descriptor.mode,
        },
        MAX_BINARY_BYTES,
        "0755",
      ),
    );
  }
  assert.deepEqual([...binaries.keys()].sort(), ["ctxmux", "ctxmuxd"]);
  exactKeys(manifest.determinism, ["sdk_archive", "binaries"], "determinism");
  assert.equal(typeof manifest.determinism.sdk_archive, "string");
  assert.equal(typeof manifest.determinism.binaries, "string");
  assert.deepEqual(
    artifactFiles(directory),
    [
      manifest.sdk.archive.path,
      "bin/ctxmux",
      "bin/ctxmuxd",
      "manifest.json",
    ].sort(),
  );
  return { manifest, manifestBytes, sdkArchive, binaries };
}

function installEnvironment(cacheDirectory) {
  const environment = { ...process.env };
  for (const name of Object.keys(environment)) {
    if (
      name === "NODE_OPTIONS" ||
      name === "NODE_PATH" ||
      name.toLowerCase().startsWith("npm_config_")
    ) {
      delete environment[name];
    }
  }
  environment.npm_config_audit = "false";
  environment.npm_config_cache = cacheDirectory;
  environment.npm_config_fund = "false";
  environment.npm_config_globalconfig = path.join(
    cacheDirectory,
    ".npmrc-global",
  );
  environment.npm_config_ignore_scripts = "true";
  environment.npm_config_update_notifier = "false";
  environment.npm_config_userconfig = path.join(cacheDirectory, ".npmrc-user");
  return environment;
}

function assertInstalledPackageIsSelfContained(consumerDirectory, sourceRoot) {
  const packageRoot = path.join(
    consumerDirectory,
    "node_modules",
    "@ctxmux",
    "sdk",
  );
  const packageDocument = JSON.parse(
    fs.readFileSync(path.join(packageRoot, "package.json"), "utf8"),
  );
  for (const section of [
    "dependencies",
    "optionalDependencies",
    "peerDependencies",
  ]) {
    for (const value of Object.values(packageDocument[section] ?? {})) {
      assert.equal(/^(file|link):/u.test(value), false);
      assert.equal(path.isAbsolute(value), false);
    }
  }
  const sourceBytes = Buffer.from(sourceRoot);
  const pending = [packageRoot];
  while (pending.length > 0) {
    const current = pending.pop();
    for (const entry of fs.readdirSync(current, { withFileTypes: true })) {
      const absolute = path.join(current, entry.name);
      assert.equal(entry.isSymbolicLink(), false);
      if (entry.isDirectory()) pending.push(absolute);
      else if (entry.isFile()) {
        assert.equal(fs.readFileSync(absolute).includes(sourceBytes), false);
      } else {
        assert.fail(`installed SDK contains a non-file entry: ${absolute}`);
      }
    }
  }
}

const CONSUMER_SOURCE = String.raw`
import assert from "node:assert/strict";
import { execFile as execFileCallback, spawn } from "node:child_process";
import { promisify } from "node:util";
import { setTimeout as delay } from "node:timers/promises";

import {
  CtxmuxClient,
  INTEGRATION_API_VERSION,
  IntegrationCapabilityError,
  IntegrationMaterializationError,
  IntegrationProvenanceError,
  IntegrationUnavailableError,
  PROTOCOL_VERSION,
  defineRun,
  registerIntegration,
} from "@ctxmux/sdk";

const execFile = promisify(execFileCallback);
const daemonBinary = process.env.CTXMUXD_BIN;
const cliBinary = process.env.CTXMUX_BIN;
const socketPath = process.env.CTXMUX_SOCKET_PATH;
const expectedProtocol = Number(process.env.CTXMUX_EXPECTED_PROTOCOL);
const expectedVersion = process.env.CTXMUX_EXPECTED_VERSION;
assert.equal(PROTOCOL_VERSION, expectedProtocol);

const cliVersion = (await execFile(cliBinary, ["--version"])).stdout.trim();
const daemonVersion = (await execFile(daemonBinary, ["--version"])).stdout.trim();
assert.equal(cliVersion, "ctxmux " + expectedVersion + " (protocol " + expectedProtocol + ")");
assert.equal(daemonVersion, "ctxmuxd " + expectedVersion + " (protocol " + expectedProtocol + ")");

const daemon = spawn(daemonBinary, ["--socket", socketPath, "--readiness-fd", "3"], {
  stdio: ["ignore", "ignore", "pipe", "pipe"],
});
let daemonStderr = "";
daemon.stderr.setEncoding("utf8");
daemon.stderr.on("data", (chunk) => {
  daemonStderr += chunk;
});
const client = new CtxmuxClient({ socketPath });
const readinessStream = daemon.stdio[3];
assert(readinessStream);

const readiness = new Promise((resolve, reject) => {
  let content = "";
  const timeout = setTimeout(() => {
    readinessStream.destroy();
    reject(new Error("artifact daemon readiness receipt timed out"));
  }, 5_000);
  readinessStream.setEncoding("utf8");
  readinessStream.on("data", (chunk) => {
    content += chunk;
    assert(Buffer.byteLength(content) <= 8 * 1024);
    const newline = content.indexOf("\n");
    if (newline < 0) return;
    clearTimeout(timeout);
    readinessStream.destroy();
    assert.equal(content.slice(newline + 1).trim(), "");
    resolve(JSON.parse(content.slice(0, newline)));
  });
  readinessStream.once("error", reject);
  readinessStream.once("close", () => {
    if (!content.includes("\n")) reject(new Error("artifact daemon readiness receipt closed early"));
  });
});

function replayBytes(chunks) {
  return Uint8Array.from(chunks.flatMap((chunk) => [...chunk.data]));
}

function append(left, right) {
  const output = new Uint8Array(left.length + right.length);
  output.set(left);
  output.set(right, left.length);
  return output;
}

function text(bytes) {
  return new TextDecoder().decode(bytes);
}

async function waitForDaemon() {
  const receipt = await readiness;
  assert.deepEqual(Object.keys(receipt).sort(), ["daemon_instance", "schema"]);
  assert.equal(receipt.schema, "ctxmux.daemon-ready.v1");
  const deadline = Date.now() + 5_000;
  let lastError;
  while (Date.now() <= deadline) {
    if (daemon.exitCode !== null) {
      throw new Error("artifact daemon exited before readiness: " + daemonStderr);
    }
    try {
      assert.equal(await client.daemonInstance(), receipt.daemon_instance);
      return;
    } catch (error) {
      lastError = error;
      await delay(20);
    }
  }
  throw new Error("artifact daemon did not become ready: " + String(lastError));
}

async function waitForText(attachment, observed, lastByte, expected) {
  const deadline = Date.now() + 5_000;
  while (!text(observed).includes(expected)) {
    if (Date.now() > deadline) {
      throw new Error("timed out waiting for " + expected + "; got " + text(observed));
    }
    const event = await attachment.nextEvent();
    assert.notEqual(event, undefined);
    if (event.type === "output") {
      assert.equal(event.chunk.start_byte, lastByte);
      lastByte = event.chunk.end_byte;
      observed = append(observed, Uint8Array.from(event.chunk.data));
    } else if (event.type === "gap") {
      throw new Error("unexpected artifact-consumer output gap");
    } else if (event.type === "exited" || event.type === "interrupted") {
      throw new Error("artifact Run ended before " + expected);
    }
  }
  return { observed, lastByte };
}

async function waitForExit(attachment) {
  const deadline = Date.now() + 5_000;
  while (Date.now() <= deadline) {
    const event = await attachment.nextEvent();
    if (event?.type === "exited") return event.state;
    if (event?.type === "gap" || event?.type === "interrupted") {
      throw new Error("unexpected terminal artifact-consumer event");
    }
  }
  throw new Error("timed out waiting for artifact Run exit");
}

async function nextOutput(attachment) {
  const replay = attachment.snapshot.replay.chunks[0];
  if (replay !== undefined) return { type: "output", chunk: replay };
  const deadline = Date.now() + 5_000;
  while (Date.now() <= deadline) {
    const event = await attachment.nextEvent();
    if (event?.type === "output") return event;
    if (event === undefined || event.type === "exited" || event.type === "interrupted") break;
    if (event.type === "gap") throw new Error("unexpected Provider output gap");
  }
  throw new Error("Provider Run produced no output");
}

function syntheticProviderIntegration() {
  return {
    id: "synthetic-provider",
    apiVersion: INTEGRATION_API_VERSION,
    async detect(options = {}) {
      if (options.executable === undefined) {
        return {
          status: "unavailable",
          executable: "synthetic-provider",
          reason: "missing_capability",
        };
      }
      return {
        status: "available",
        executable: options.executable,
        version: "consumer-owned",
        capabilities: ["semantic_events", "level_b_fork"],
      };
    },
    planLaunch(config) {
      return config.recipe;
    },
    planLevelBFork(_parent, config) {
      return { type: "level_b", spec: config.recipe };
    },
    levelBForkProvenance(config) {
      return config.provenance;
    },
    createObserver() {
      return {
        observe(event) {
          if (event.type !== "output") return [];
          return [{
            integrationId: "synthetic-provider",
            name: "source.observed",
            data: {
              range: [event.chunk.start_byte, event.chunk.end_byte],
            },
          }];
        },
      };
    },
  };
}

async function stopDaemon() {
  if (daemon.exitCode !== null) return;
  daemon.kill("SIGINT");
  await Promise.race([
    new Promise((resolve) => daemon.once("exit", resolve)),
    delay(2_000).then(() => daemon.kill("SIGKILL")),
  ]);
}

try {
  await waitForDaemon();
  const runtime = await client.runtimeInfo();
  assert.deepEqual(Object.keys(runtime).sort(), [
    "arch",
    "buildId",
    "capabilities",
    "daemonInstanceId",
    "platform",
    "protocolGeneration",
    "runtimeId",
    "runtimeIdPersistence",
  ]);
  assert.equal(runtime.daemonInstanceId, (await readiness).daemon_instance);
  assert.notEqual(runtime.runtimeId, runtime.daemonInstanceId);
  assert.equal(runtime.runtimeIdPersistence, "daemon");
  assert.equal(runtime.buildId, "ctxmuxd/" + expectedVersion);
  assert.equal(runtime.protocolGeneration, expectedProtocol);
  assert.notEqual(runtime.platform, "");
  assert.notEqual(runtime.arch, "");
  assert.deepEqual(runtime.capabilities, {
    "native.execute_materialized_level_b": 1,
    "native.fork_level_a": 1,
    "native.recoverable_input": 1,
    "native.start": 1,
    "tmux.discover": 1,
    "tmux.import": 1,
  });
  assert.deepEqual(
    JSON.parse(
      (await execFile(cliBinary, ["--socket", socketPath, "runtime"])).stdout,
    ),
    runtime,
  );
  const run = await client.start(
    defineRun("/bin/sh", {
      args: [
        "-c",
        "trap '' INT; trap 'exit 0' TERM; printf 'ARTIFACT_READY\\n'; while IFS= read -r line; do printf 'ARTIFACT:%s\\n' \"$line\"; done",
      ],
    }),
  );
  assert.notEqual(run.id, runtime.runtimeId);
  assert.notEqual(run.id, runtime.daemonInstanceId);
  const status = await client.status(run.id);
  assert.equal(status.state.type, "running");
  assert.notEqual(status.pid, null);

  const attachment = await client.attach(run.id, 0);
  let observed = replayBytes(attachment.snapshot.replay.chunks);
  let lastByte = attachment.snapshot.replay.latest_output_bytes;
  ({ observed, lastByte } = await waitForText(
    attachment,
    observed,
    lastByte,
    "ARTIFACT_READY",
  ));
  assert.deepEqual((await attachment.input("ping\n")).receipt, {
    type: "input",
    written_bytes: 5,
  });
  ({ observed, lastByte } = await waitForText(
    attachment,
    observed,
    lastByte,
    "ARTIFACT:ping",
  ));
  assert.deepEqual((await attachment.interrupt()).receipt, {
    type: "signal",
    signal: "interrupt",
  });
  assert.deepEqual((await attachment.input("after\n")).receipt, {
    type: "input",
    written_bytes: 6,
  });
  ({ observed, lastByte } = await waitForText(
    attachment,
    observed,
    lastByte,
    "ARTIFACT:after",
  ));
  await attachment.detach();
  assert.equal((await client.status(run.id)).state.type, "running");

  const replayAttachment = await client.attach(run.id, 0);
  const replay = text(replayBytes(replayAttachment.snapshot.replay.chunks));
  assert.match(replay, /ARTIFACT_READY/u);
  assert.match(replay, /ARTIFACT:ping/u);
  assert.match(replay, /ARTIFACT:after/u);
  const cliStatus = (await execFile(cliBinary, ["--socket", socketPath, "status", run.id])).stdout;
  assert.match(cliStatus, new RegExp("^" + run.id + "\\trunning\\tpid="));
  assert.deepEqual((await replayAttachment.stop()).receipt, {
    type: "stop",
    disposition: "graceful",
  });
  assert.deepEqual(await waitForExit(replayAttachment), {
    type: "exited",
    code: 0,
    signal: null,
  });
  assert.equal((await client.status(run.id)).state.type, "exited");

  const provider = syntheticProviderIntegration();
  const registered = registerIntegration(client, provider);
  const detection = { executable: "/bin/sh" };
  const parentRecipe = defineRun("/bin/sh", {
    args: ["-c", "trap 'exit 0' TERM; printf 'PROVIDER_PARENT\\n'; while :; do sleep 1; done"],
    declaredInputs: [{ kind: "workspace", reference: "consumer-workspace" }],
  });
  const parent = await registered.start(
    { recipe: parentRecipe },
    { detection },
  );
  const parentAttachment = await client.attach(parent.id);
  const provenance = registered
    .createObserver(parent)
    .observe(await nextOutput(parentAttachment))[0];
  assert.notEqual(provenance, undefined);

  const unrelated = await client.start(
    defineRun("/bin/sh", {
      args: ["-c", "trap 'exit 0' TERM; printf 'UNRELATED\\n'; while :; do sleep 1; done"],
    }),
  );
  const unrelatedAttachment = await client.attach(unrelated.id);
  const unrelatedEvent = await nextOutput(unrelatedAttachment);
  assert.throws(
    () => registered.createObserver(parent).observe(unrelatedEvent),
    (error) =>
      error instanceof IntegrationProvenanceError &&
      error.reason === "wrong_source",
  );

  const childRecipe = defineRun("/bin/sh", {
    args: ["-c", "trap 'exit 0' TERM; printf 'PROVIDER_CHILD\\n'; while :; do sleep 1; done"],
    declaredInputs: [
      { kind: "workspace", reference: "consumer-workspace" },
      { kind: "artifact", reference: "artifact://consumer-proof" },
      { kind: "context", reference: "synthetic-context:parent" },
    ],
  });
  const levelB = { provenance, recipe: childRecipe };
  const beforeRejected = (await client.list()).map(({ id }) => id);
  await assert.rejects(
    registered.forkLevelB(parent, {
      ...levelB,
      provenance: { ...provenance },
    }, { detection }),
    (error) =>
      error instanceof IntegrationProvenanceError && error.reason === "missing",
  );
  await assert.rejects(
    registered.forkLevelB(unrelated, levelB, { detection }),
    (error) =>
      error instanceof IntegrationProvenanceError &&
      error.reason === "wrong_source",
  );
  await assert.rejects(
    registered.forkLevelB(parent, levelB),
    (error) =>
      error instanceof IntegrationUnavailableError &&
      error.detection.reason === "missing_capability",
  );
  const withoutLevelBCapability = {
    ...provider,
    id: "synthetic-provider-without-level-b",
    async detect() {
      return {
        status: "available",
        executable: "/bin/sh",
        version: "consumer-owned",
        capabilities: ["semantic_events"],
      };
    },
  };
  await assert.rejects(
    registerIntegration(client, withoutLevelBCapability).forkLevelB(
      parent,
      levelB,
      { detection },
    ),
    (error) =>
      error instanceof IntegrationCapabilityError &&
      error.capability === "level_b_fork",
  );
  const { levelBForkProvenance: _provenance, ...withoutProvenance } = provider;
  await assert.rejects(
    registerIntegration(client, withoutProvenance).forkLevelB(
      parent,
      levelB,
      { detection },
    ),
    (error) =>
      error instanceof IntegrationProvenanceError && error.reason === "missing",
  );
  const { planLevelBFork: _plan, ...withoutMaterializer } = provider;
  await assert.rejects(
    registerIntegration(client, withoutMaterializer).forkLevelB(
      parent,
      levelB,
      { detection },
    ),
    (error) =>
      error instanceof IntegrationMaterializationError &&
      error.reason === "missing_planner",
  );
  assert.deepEqual(
    (await client.list()).map(({ id }) => id),
    beforeRejected,
    "fail-closed Level B cases created no child Run or Level A fallback",
  );

  unrelatedAttachment.close();
  await client.stop(unrelated.id);
  parentAttachment.close();
  const child = await registered.forkLevelB(parent, levelB, { detection });
  assert.deepEqual(child.lineage, { parent: parent.id, fidelity: "level_b" });
  assert.deepEqual(child.spec, childRecipe);
  const childAttachment = await client.attach(child.id);
  await waitForText(
    childAttachment,
    replayBytes(childAttachment.snapshot.replay.chunks),
    childAttachment.snapshot.replay.latest_output_bytes,
    "PROVIDER_CHILD",
  );
  childAttachment.close();
  assert.equal((await client.status(parent.id)).state.type, "running");
  await client.stop(parent.id);
  await client.stop(child.id);
} finally {
  await stopDaemon();
}

if (daemonStderr.length > 0) process.stderr.write(daemonStderr);
process.stdout.write("isolated artifact consumer passed\n");
`;

const CONSUMER_TYPESCRIPT_SOURCE = String.raw`
import {
  CtxmuxClient,
  INTEGRATION_API_VERSION,
  IntegrationCapabilityError,
  IntegrationMaterializationError,
  IntegrationProvenanceError,
  IntegrationUnavailableError,
  registerIntegration,
  type Integration,
  type IntegrationSemanticEvent,
  type RunInfo,
  type RunSpec,
  type RuntimeIdentity,
} from "@ctxmux/sdk";
import { shellIntegration } from "@ctxmux/sdk/integrations";

interface ProviderReceipt extends IntegrationSemanticEvent {
  readonly integrationId: "consumer-provider";
  readonly name: "source.observed";
}

interface ProviderForkConfig {
  readonly provenance: ProviderReceipt;
  readonly recipe: RunSpec;
}

const provider: Integration<
  { readonly recipe: RunSpec },
  ProviderForkConfig,
  ProviderReceipt
> = {
  id: "consumer-provider",
  apiVersion: INTEGRATION_API_VERSION,
  async detect() {
    return {
      status: "available",
      executable: "/bin/sh",
      version: "consumer-owned",
      capabilities: ["semantic_events", "level_b_fork"],
    };
  },
  planLaunch(config) {
    return config.recipe;
  },
  planLevelBFork(_parent, config) {
    return { type: "level_b", spec: config.recipe };
  },
  levelBForkProvenance(config) {
    return config.provenance;
  },
  createObserver() {
    return { observe: () => [] };
  },
};

declare const client: CtxmuxClient;
declare const parent: RunInfo;
declare const config: ProviderForkConfig;
const runtimeInfo: Promise<RuntimeIdentity> = client.runtimeInfo();
const capabilities: RuntimeIdentity["capabilities"] = {
  "native.start": 1,
};
const registered = registerIntegration(client, provider);
const child: Promise<RunInfo> = registered.forkLevelB(parent, config);
const publicErrors: readonly Error[] = [
  new IntegrationUnavailableError("consumer-provider", {
    status: "unavailable",
    executable: "/missing",
    reason: "not_found",
  }),
  new IntegrationCapabilityError("consumer-provider", "level_b_fork"),
  new IntegrationProvenanceError("consumer-provider", parent.id, "wrong_source"),
  new IntegrationMaterializationError("consumer-provider", "missing_planner"),
];
void child;
void publicErrors;
void runtimeInfo;
void capabilities;
void shellIntegration;
`;

async function runIsolatedConsumer(consumerDirectory, args, environment) {
  await new Promise((resolve, reject) => {
    const child = spawn(process.execPath, args, {
      cwd: consumerDirectory,
      env: environment,
      detached: true,
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    const capture = (current, chunk) => {
      const next = current + chunk.toString("utf8");
      if (Buffer.byteLength(next) > COMMAND_OUTPUT_LIMIT) {
        process.kill(-child.pid, "SIGKILL");
        reject(new Error("isolated consumer output exceeded its bound"));
        return current;
      }
      return next;
    };
    child.stdout.on("data", (chunk) => {
      stdout = capture(stdout, chunk);
    });
    child.stderr.on("data", (chunk) => {
      stderr = capture(stderr, chunk);
    });
    const timeout = setTimeout(() => {
      process.kill(-child.pid, "SIGKILL");
      reject(new Error("isolated artifact consumer exceeded 20 seconds"));
    }, 20_000);
    child.once("error", (error) => {
      clearTimeout(timeout);
      reject(error);
    });
    child.once("exit", (code, signal) => {
      clearTimeout(timeout);
      if (code === 0) resolve();
      else {
        reject(
          new Error(
            `isolated artifact consumer failed (${String(code)}/${String(signal)}): ${stderr || stdout}`,
          ),
        );
      }
    });
  });
}

async function main() {
  const temporaryRoot = fs.realpathSync(
    fs.mkdtempSync(path.join(fs.realpathSync("/tmp"), "ctxmux-consumer-")),
  );
  try {
    assert.equal(path.relative(root, temporaryRoot).startsWith(".."), true);
    const firstOutput = path.join(temporaryRoot, "artifacts-a");
    const secondOutput = path.join(temporaryRoot, "artifacts-b");
    run(process.execPath, [builder, firstOutput], { cwd: root });
    run(process.execPath, [builder, secondOutput], { cwd: root });
    const first = verifyArtifactSet(firstOutput);
    const second = verifyArtifactSet(secondOutput);
    assert.deepEqual(first.manifestBytes, second.manifestBytes);
    assert.deepEqual(first.manifest, second.manifest);

    const consumerDirectory = path.join(temporaryRoot, "consumer");
    fs.mkdirSync(consumerDirectory);
    fs.mkdirSync(path.join(consumerDirectory, "bin"));
    fs.mkdirSync(path.join(consumerDirectory, "npm-cache"));
    fs.mkdirSync(path.join(consumerDirectory, "tmp"));
    const sdkArchive = path.join(
      consumerDirectory,
      path.basename(first.sdkArchive),
    );
    fs.copyFileSync(first.sdkArchive, sdkArchive);
    fs.chmodSync(sdkArchive, 0o644);
    for (const [name, source] of first.binaries) {
      const destination = path.join(consumerDirectory, "bin", name);
      fs.copyFileSync(source, destination);
      fs.chmodSync(destination, 0o755);
    }
    const packageDocument = `${JSON.stringify(
      {
        name: "ctxmux-local-consumer-fixture",
        private: true,
        type: "module",
      },
      null,
      2,
    )}\n`;
    fs.writeFileSync(
      path.join(consumerDirectory, "package.json"),
      packageDocument,
    );
    const environment = installEnvironment(
      path.join(consumerDirectory, "npm-cache"),
    );
    run(
      "npm",
      [
        "install",
        "--offline",
        "--ignore-scripts",
        "--no-audit",
        "--no-fund",
        "--no-package-lock",
        "--no-save",
        sdkArchive,
      ],
      { cwd: consumerDirectory, environment, timeout: 20_000 },
    );
    assert.equal(
      fs.readFileSync(path.join(consumerDirectory, "package.json"), "utf8"),
      packageDocument,
    );
    assert.equal(
      fs.existsSync(path.join(consumerDirectory, "package-lock.json")),
      false,
    );
    assertInstalledPackageIsSelfContained(consumerDirectory, root);
    const consumerTypeScript = path.join(
      consumerDirectory,
      "consumer-types.mts",
    );
    fs.writeFileSync(consumerTypeScript, CONSUMER_TYPESCRIPT_SOURCE, {
      mode: 0o644,
    });
    run(
      process.execPath,
      [
        path.join(root, "node_modules", "typescript", "bin", "tsc"),
        "--noEmit",
        "--target",
        "ES2024",
        "--module",
        "NodeNext",
        "--moduleResolution",
        "NodeNext",
        "--strict",
        "--noUncheckedIndexedAccess",
        "--exactOptionalPropertyTypes",
        consumerTypeScript,
      ],
      { cwd: consumerDirectory, environment, timeout: 20_000 },
    );
    const consumerScript = path.join(consumerDirectory, "consumer.mjs");
    fs.writeFileSync(consumerScript, CONSUMER_SOURCE, { mode: 0o644 });
    const consumerEnvironment = { ...environment };
    delete consumerEnvironment.NODE_OPTIONS;
    delete consumerEnvironment.NODE_PATH;
    consumerEnvironment.CTXMUX_BIN = path.join(
      consumerDirectory,
      "bin",
      "ctxmux",
    );
    consumerEnvironment.CTXMUXD_BIN = path.join(
      consumerDirectory,
      "bin",
      "ctxmuxd",
    );
    consumerEnvironment.CTXMUX_EXPECTED_PROTOCOL = String(
      first.manifest.product.protocol,
    );
    consumerEnvironment.CTXMUX_EXPECTED_VERSION =
      first.manifest.product.version;
    consumerEnvironment.CTXMUX_SOCKET_PATH = path.join(
      consumerDirectory,
      "ctxmux.sock",
    );
    consumerEnvironment.TMPDIR = path.join(consumerDirectory, "tmp");
    await runIsolatedConsumer(
      consumerDirectory,
      [
        "--permission",
        `--allow-fs-read=${consumerDirectory}`,
        "--allow-child-process",
        consumerScript,
      ],
      consumerEnvironment,
    );
    process.stdout.write(
      `local artifact consumer passed commit=${first.manifest.source.commit} target=${first.manifest.support.rust_target}\n`,
    );
  } finally {
    fs.rmSync(temporaryRoot, { recursive: true, force: true });
  }
}

main().catch((error) => {
  process.stderr.write(
    `${error instanceof Error ? (error.stack ?? error.message) : String(error)}\n`,
  );
  process.exitCode = 1;
});
