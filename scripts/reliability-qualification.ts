#!/usr/bin/env node

import assert from "node:assert/strict";
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { createHash } from "node:crypto";
import {
  createWriteStream,
  existsSync,
  readFileSync,
  readdirSync,
  type WriteStream,
} from "node:fs";
import { mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { createConnection, type Socket } from "node:net";
import { arch, cpus, platform, release, tmpdir } from "node:os";
import { basename, dirname, join, relative, resolve } from "node:path";
import { finished } from "node:stream/promises";
import { setTimeout as delay } from "node:timers/promises";

import {
  CtxmuxClient,
  CtxmuxProtocolError,
  MAX_FRAME_BYTES,
  PROTOCOL_VERSION,
  registerIntegration,
  type RunId,
  type RunInfo,
  type RunSpec,
} from "../packages/sdk/src/index.ts";
import { shellIntegration } from "../packages/sdk/src/integrations/shell.ts";
import {
  assertInheritedArtifactOwner,
  enterCanonicalArtifactOwner,
  openFreshOwnedFile,
  parseQualificationPreflight,
  readOwnedJson,
  type QualificationPreflight,
  writeOwnedJsonAtomically,
} from "./reliability-artifact-owner.mts";

type QualificationProfile = "smoke" | "nightly" | "release" | "observe";
type QualificationStage = "all" | "resource-census";
type WorkloadMode = "idle" | "active";

interface QualificationPolicy {
  readonly schema: "ctxmux.reliability-qualification-policy.v1";
  readonly profiles: Readonly<
    Record<
      QualificationProfile,
      {
        readonly time_budget_seconds: number;
        readonly soak_seconds: number;
        readonly resource_counts: readonly number[];
      }
    >
  >;
  readonly resource_start_concurrency: number;
  readonly seed_controls: readonly string[];
}

export const QUALIFICATION_POLICY_SOURCE = String.raw`{
  "schema": "ctxmux.reliability-qualification-policy.v1",
  "profiles": {
    "smoke": { "time_budget_seconds": 60, "soak_seconds": 0, "resource_counts": [1] },
    "nightly": { "time_budget_seconds": 2700, "soak_seconds": 1800, "resource_counts": [1, 32, 128] },
    "release": { "time_budget_seconds": 10800, "soak_seconds": 7200, "resource_counts": [1, 32, 128] },
    "observe": { "time_budget_seconds": 2700, "soak_seconds": 0, "resource_counts": [1, 32, 128] }
  },
  "resource_start_concurrency": 8,
  "seed_controls": ["fanout payload byte", "secret marker"]
}`;
const QUALIFICATION_POLICY = JSON.parse(
  QUALIFICATION_POLICY_SOURCE,
) as QualificationPolicy;

interface ProcessSample {
  readonly rss_kib: number;
  readonly cpu_seconds: number;
  readonly threads: number;
  readonly fds: number;
  readonly descendants: readonly ProcessTreeEntry[];
}

interface ProcessTreeEntry {
  readonly pid: number;
  readonly ppid: number;
  readonly state: string;
  readonly command: string;
}

interface ResourceMeasurement {
  readonly mode: WorkloadMode;
  readonly runs: number;
  readonly baseline: ProcessSample;
  readonly steady: ProcessSample;
  readonly cleanup: ProcessSample;
  readonly peak_rss_kib: number;
  readonly peak_rss_sample_count: number;
  readonly peak_rss_sample_interval_ms: number;
  readonly cpu_core_percent: number;
  readonly retained_output_bytes: number;
  readonly retained_output_bytes_per_run: number;
  readonly rss_kib_per_run: number;
  readonly threads_per_run: number;
  readonly fds_per_run: number;
  readonly cleanup_rss_kib_delta: number;
  readonly cleanup_fds_delta: number;
  readonly cleanup_retained_runs: number;
  readonly cleanup_live_children: number;
  readonly cleanup_attachments: number;
  readonly intentional_retained_state_without_gc: true;
}

interface ResourceBudget {
  readonly max_cpu_core_percent: number;
  readonly max_peak_rss_kib: number;
  readonly max_steady_rss_kib: number;
  readonly max_retained_output_bytes_per_run: number;
  readonly max_rss_kib_per_run: number;
  readonly max_threads_per_run: number;
  readonly max_fds_per_run: number;
  readonly max_cleanup_threads_delta: number;
  readonly max_cleanup_live_children: number;
  readonly max_cleanup_attachments: number;
}

interface BudgetFile {
  readonly schema: "ctxmux.reliability-budgets.v1";
  readonly frozen_before_optimization: true;
  readonly measurement_contract: {
    readonly cpu: string;
    readonly rss: string;
    readonly slopes: string;
    readonly cleanup: string;
  };
  readonly budgets: Readonly<
    Record<WorkloadMode, Readonly<Record<string, ResourceBudget>>>
  >;
}

interface StageResult {
  readonly id: string;
  readonly status: "pass" | "fail";
  readonly started_at: string;
  readonly completed_at: string;
  readonly result?: unknown;
  readonly error?: string;
}

interface FileIdentity {
  readonly path: string;
  readonly sha256: string;
}

interface QualificationProvenance {
  readonly claim_scope: "locally_observed";
  readonly binary_source_attestation: false;
  readonly source: {
    readonly commit: string;
    readonly tree: string;
    readonly worktree: {
      readonly status_format: "git-status-porcelain-v1-z";
      readonly clean: boolean;
      readonly entries: readonly string[];
    };
  };
  readonly harness: FileIdentity;
  readonly launcher: FileIdentity;
  readonly daemon: FileIdentity;
  readonly lockfiles: readonly FileIdentity[];
  readonly build: {
    readonly cwd: ".";
    readonly argv: readonly string[];
    readonly source_commit: string;
    readonly source_tree: string;
    readonly worktree_clean: boolean;
    readonly target_directory: string;
    readonly daemon_path: string;
    readonly locked: boolean;
  };
  readonly toolchain: {
    readonly rustc_version_verbose: string;
    readonly cargo_version: string;
    readonly node_version: string;
  };
  readonly measurement_contract_encoding: "json-stringify-utf8";
  readonly measurement_contract_sha256: string;
}

interface QualificationReceipt {
  readonly schema: "ctxmux.reliability-qualification.v2";
  status: "running" | "pass" | "fail";
  readonly profile: QualificationProfile;
  readonly observation_round: number | null;
  readonly seed: number;
  readonly recorded_at: string;
  completed_at: string | null;
  readonly time_budget_seconds: number;
  readonly environment: Record<string, unknown>;
  readonly provenance: QualificationProvenance;
  readonly declared_limits: Record<string, unknown>;
  readonly action_trace: Array<Record<string, unknown>>;
  readonly stages: StageResult[];
  readonly daemon_logs: string[];
  error: string | null;
}

interface QualificationOptions {
  readonly profile: QualificationProfile;
  readonly observationRound: number | null;
  readonly stage: QualificationStage;
  readonly seed: number;
  readonly artifactDirectory: string;
  readonly preflight: QualificationPreflight;
  readonly timeBudgetSeconds: number;
  readonly resourceCounts: readonly number[];
  readonly resourceModes: readonly WorkloadMode[];
  readonly resourceStartConcurrency: number;
  readonly soakSeconds: number;
}

interface RssSampler {
  readonly peak: () => number;
  readonly sampleCount: () => number;
  readonly stop: () => Promise<void>;
}

const harnessPath = resolve(
  process.argv[1] ?? "scripts/reliability-qualification.ts",
);
const root = resolve(dirname(harnessPath), "..");
const launcherPath = resolve(root, "scripts/check-reliability.sh");
const fixedBuildTargetDirectory = "target/reliability/provenance-build";
const fixedDaemonPath = `${fixedBuildTargetDirectory}/debug/ctxmuxd`;
const fixedBuildArgv = [
  "cargo",
  "build",
  "--locked",
  "--quiet",
  "--package",
  "ctxmux-daemon",
  "--target-dir",
  fixedBuildTargetDirectory,
] as const;
const daemonBinary = resolve(root, process.env.CTXMUXD_BIN ?? fixedDaemonPath);
const budgetPath = resolve(root, "reliability-budgets.json");

async function main(): Promise<void> {
  if (process.argv[2] === "--integration-host") {
    await integrationHost(process.argv[3], process.argv[4]);
  } else if (process.env.CTXMUX_RELIABILITY_WORKER === "1") {
    await qualify(parseOptions());
  } else {
    await superviseQualification(parseOptions());
  }
}

async function superviseQualification(
  options: QualificationOptions,
): Promise<void> {
  const worker = spawn(
    process.execPath,
    ["--import", "tsx", harnessPath, ...process.argv.slice(2)],
    {
      detached: true,
      stdio: "inherit",
      env: { ...process.env, CTXMUX_RELIABILITY_WORKER: "1" },
    },
  );
  assert.notEqual(worker.pid, undefined, "qualification worker has no PID");
  const workerPid = worker.pid!;
  let timedOut = false;
  const timeout = setTimeout(() => {
    timedOut = true;
    killProcessGroup(workerPid, "SIGKILL");
  }, options.timeBudgetSeconds * 1000);
  const forwardSignal = (signal: NodeJS.Signals): void => {
    killProcessGroup(workerPid, signal);
  };
  const onInterrupt = (): void => forwardSignal("SIGINT");
  const onTerminate = (): void => forwardSignal("SIGTERM");
  process.once("SIGINT", onInterrupt);
  process.once("SIGTERM", onTerminate);
  try {
    const status = await waitForProcess(
      worker,
      options.timeBudgetSeconds * 1000 + 10_000,
    );
    if (timedOut) {
      recordSupervisorTimeout(options);
      process.exitCode = 1;
    } else if (status.code !== 0) {
      process.exitCode = status.code ?? 1;
    }
  } finally {
    clearTimeout(timeout);
    process.off("SIGINT", onInterrupt);
    process.off("SIGTERM", onTerminate);
  }
}

function killProcessGroup(pid: number, signal: NodeJS.Signals): void {
  try {
    process.kill(-pid, signal);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error;
  }
}

function recordSupervisorTimeout(options: QualificationOptions): void {
  const message = `qualification exceeded its hard ${options.timeBudgetSeconds}s time budget`;
  let receipt: QualificationReceipt;
  try {
    const current = readOwnedJson<QualificationReceipt>("result.json");
    const captured = current.value.action_trace.find(
      (entry) => entry.action === "provenance.captured",
    );
    const preexistingIdentity = options.preflight.preexisting_receipt_identity;
    if (
      captured?.invocation_nonce !== options.preflight.invocation_nonce ||
      (preexistingIdentity !== null &&
        current.identity.dev === preexistingIdentity.dev &&
        current.identity.ino === preexistingIdentity.ino)
    ) {
      return;
    }
    receipt = current.value;
  } catch {
    return;
  }
  const completedStages = new Set(receipt.stages.map(({ id }) => id));
  const openStage = receipt.action_trace.findLast(
    (entry) =>
      entry.action === "stage.start" &&
      typeof entry.id === "string" &&
      !completedStages.has(entry.id),
  );
  if (openStage !== undefined && typeof openStage.id === "string") {
    receipt.stages.push({
      id: openStage.id,
      status: "fail",
      started_at:
        typeof openStage.timestamp === "string"
          ? openStage.timestamp
          : receipt.recorded_at,
      completed_at: new Date().toISOString(),
      error: message,
    });
  }
  receipt.status = "fail";
  receipt.completed_at = new Date().toISOString();
  receipt.error = message;
  receipt.action_trace.push({
    timestamp: receipt.completed_at,
    action: "supervisor.timeout",
    time_budget_seconds: options.timeBudgetSeconds,
  });
  writeOwnedJsonAtomically("result.json", receipt);
}

async function qualify(options: QualificationOptions): Promise<void> {
  const provenance = captureProvenance();
  const cpu = cpus();
  const receipt: QualificationReceipt = {
    schema: "ctxmux.reliability-qualification.v2",
    status: "running",
    profile: options.profile,
    observation_round: options.observationRound,
    seed: options.seed,
    recorded_at: new Date().toISOString(),
    completed_at: null,
    time_budget_seconds: options.timeBudgetSeconds,
    environment: {
      os: platform(),
      os_release: release(),
      architecture: arch(),
      logical_cpus: cpu.length,
      cpu_model: cpu[0]?.model ?? "unknown",
    },
    provenance,
    declared_limits: {
      frame_bytes: MAX_FRAME_BYTES,
      retained_output_bytes_per_run: 4 * 1024 * 1024,
      live_event_capacity: 256,
      global_run_quota: null,
      global_attachment_quota: null,
      exited_run_gc: null,
      qualification_stage: options.stage,
      resource_counts: options.resourceCounts,
      resource_modes: options.resourceModes,
      resource_start_concurrency: options.resourceStartConcurrency,
      peak_rss_sample_interval_ms: 25,
      soak_seconds: options.soakSeconds,
      seed_controls: [...QUALIFICATION_POLICY.seed_controls],
      note: "Absent global quotas and GC remain visible; the harness bounds are qualification workloads, not product limits.",
    },
    action_trace: [],
    stages: [],
    daemon_logs: [],
    error: null,
  };
  const deadline = Date.now() + options.timeBudgetSeconds * 1000;
  const writeReceipt = (): void => {
    writeOwnedJsonAtomically("result.json", receipt);
  };
  const trace = (
    action: string,
    detail: Record<string, unknown> = {},
  ): void => {
    receipt.action_trace.push({
      timestamp: new Date().toISOString(),
      action,
      ...detail,
    });
    writeReceipt();
  };
  const stage = async <T>(id: string, run: () => Promise<T>): Promise<T> => {
    assert.ok(
      Date.now() < deadline,
      `qualification time budget expired before ${id}`,
    );
    const started = new Date().toISOString();
    trace("stage.start", { id });
    try {
      const result = await run();
      receipt.stages.push({
        id,
        status: "pass",
        started_at: started,
        completed_at: new Date().toISOString(),
        result,
      });
      trace("stage.pass", { id });
      return result;
    } catch (error) {
      receipt.stages.push({
        id,
        status: "fail",
        started_at: started,
        completed_at: new Date().toISOString(),
        error: errorText(error),
      });
      trace("stage.fail", { id, error: errorText(error) });
      throw error;
    }
  };

  writeReceipt();
  trace("provenance.captured", {
    source_commit: provenance.source.commit,
    source_tree: provenance.source.tree,
    worktree_clean: provenance.source.worktree.clean,
    harness_sha256: provenance.harness.sha256,
    launcher_sha256: provenance.launcher.sha256,
    daemon_sha256: provenance.daemon.sha256,
    measurement_contract_sha256: provenance.measurement_contract_sha256,
    invocation_nonce: options.preflight.invocation_nonce,
  });
  try {
    assertQualificationProvenance(options, provenance);
    trace("provenance.verified", {
      observation_round: options.observationRound,
    });
    if (options.stage === "all") {
      await stage("chaos-owner-matrix", () =>
        runChaosOwnerMatrix(options, receipt, trace),
      );
      await stage("security-negative-space", () =>
        runSecurityMatrix(options, receipt, trace),
      );
      await stage("stress-and-soak", () =>
        runStressMatrix(options, receipt, trace),
      );
    }
    const resources = await stage("resource-census", () =>
      runResourceCensus(options, receipt, trace),
    );
    if (options.profile !== "observe") {
      const budgets = readBudgets();
      await stage("frozen-resource-budgets", async () => {
        for (const measurement of resources) {
          assertResourceBudget(measurement, budgetFor(budgets, measurement));
        }
        return {
          budget_file: portablePath(budgetPath),
          measurements: resources.length,
        };
      });
    }
    assertQualificationProvenance(options, provenance);
    trace("provenance.reverified", {
      daemon_sha256: provenance.daemon.sha256,
    });
    receipt.status = "pass";
  } catch (error) {
    receipt.status = "fail";
    receipt.error = errorText(error);
    process.exitCode = 1;
  } finally {
    receipt.completed_at = new Date().toISOString();
    writeReceipt();
  }
}

async function runChaosOwnerMatrix(
  options: QualificationOptions,
  receipt: QualificationReceipt,
  trace: (action: string, detail?: Record<string, unknown>) => void,
): Promise<unknown> {
  const results: Record<string, unknown> = {};

  {
    const daemon = await DaemonFixture.start(
      "integration-host",
      options,
      receipt,
    );
    try {
      const handoffPath = join(daemon.directory, "integration-run.json");
      trace("chaos.integration_host.spawn", { daemon_pid: daemon.pid });
      const child = spawn(
        process.execPath,
        [
          "--import",
          "tsx",
          resolve(root, "scripts/reliability-qualification.ts"),
          "--integration-host",
          daemon.socketPath,
          handoffPath,
        ],
        { cwd: root, stdio: ["ignore", "ignore", "pipe"] },
      );
      let childStderr = "";
      child.stderr?.on("data", (chunk: Buffer) => {
        childStderr += chunk.toString("utf8");
      });
      const status = await waitForProcess(child, 10_000);
      assert.equal(status.code, 0, `Integration host failed: ${childStderr}`);
      const run = JSON.parse(await readFile(handoffPath, "utf8")) as {
        readonly id: RunId;
        readonly pid: number;
      };
      const observed = await daemon.client.status(run.id);
      assert.equal(observed.pid, run.pid);
      assert.equal(observed.state.type, "running");
      const processTreeBeforeCleanup = processTree(daemon.pid);
      trace("chaos.integration_host.survived", {
        daemon_pid: daemon.pid,
        child_pid: run.pid,
        process_tree: processTreeBeforeCleanup,
      });
      await daemon.client.stop(run.id);
      await waitForRunExit(daemon.client, run.id);
      await waitForNoLiveChildren(daemon.pid, 5_000);
      results.integration_host_exit = {
        run_id: run.id,
        pid: run.pid,
        same_pid_after_host_exit: true,
        process_tree_before_cleanup: processTreeBeforeCleanup,
        cleanup_live_children: processTree(daemon.pid).length,
      };
    } finally {
      await daemon.close();
    }
  }

  {
    const daemon = await DaemonFixture.start("child-kill", options, receipt);
    try {
      const run = await daemon.client.start(
        shellSpec("printf 'BEFORE-KILL'; while :; do printf x; sleep 1; done"),
      );
      const pid = requiredPid(run);
      await waitForReplay(daemon.client, run.id, (bytes) =>
        bytes.includes(Buffer.from("BEFORE-KILL")),
      );
      const processTreeBeforeKill = processTree(daemon.pid);
      trace("chaos.child.kill", {
        daemon_pid: daemon.pid,
        child_pid: pid,
        process_tree: processTreeBeforeKill,
      });
      process.kill(pid, "SIGKILL");
      await waitForRunExit(daemon.client, run.id);
      await waitForNoLiveChildren(daemon.pid, 5_000);
      await daemon.client.ping();
      const replay = await replayBytes(daemon.client, run.id);
      assert.ok(replay.includes(Buffer.from("BEFORE-KILL")));
      results.child_kill = {
        child_pid: pid,
        process_tree_before_kill: processTreeBeforeKill,
        final_output_replayable: true,
        daemon_healthy: true,
        cleanup_live_children: processTree(daemon.pid).length,
      };
    } finally {
      await daemon.close();
    }
  }

  {
    const daemon = await DaemonFixture.start("daemon-kill", options, receipt);
    const run = await daemon.client.start(idleSpec());
    const childPid = requiredPid(run);
    const before = processTree(daemon.pid);
    trace("chaos.daemon.kill", {
      daemon_pid: daemon.pid,
      child_pid: childPid,
      process_tree: before,
    });
    await daemon.kill("SIGKILL");
    await assert.rejects(daemon.client.ping());
    const survived = processExists(childPid);
    if (survived) {
      process.kill(childPid, "SIGKILL");
      await waitForProcessGone(childPid, 5_000);
    }
    results.daemon_kill = {
      run_id_lost_as_declared: true,
      child_survived_daemon_kill: survived,
      harness_cleanup: processExists(childPid) ? "failed" : "complete",
      process_tree_before: before,
    };
    assert.equal(
      processExists(childPid),
      false,
      "daemon-kill fixture leaked its child",
    );
  }

  return results;
}

async function runSecurityMatrix(
  options: QualificationOptions,
  receipt: QualificationReceipt,
  trace: (action: string, detail?: Record<string, unknown>) => void,
): Promise<unknown> {
  const daemon = await DaemonFixture.start("security", options, receipt);
  try {
    const before = await daemon.client.list();
    await assert.rejects(
      daemon.client.start({
        ...idleSpec(),
        program: "/ctxmux/missing-executable",
      }),
      (error: unknown) =>
        error instanceof CtxmuxProtocolError && error.code === "spawn_failed",
    );
    assert.equal((await daemon.client.list()).length, before.length);

    const run = await daemon.client.start(idleSpec());
    await assert.rejects(
      daemon.client.resize(run.id, { cols: 0, rows: 24 }),
      (error: unknown) =>
        error instanceof CtxmuxProtocolError &&
        error.code === "invalid_request",
    );
    assert.equal((await daemon.client.list()).length, before.length + 1);
    await daemon.client.stop(run.id);
    await waitForRunExit(daemon.client, run.id);

    const markerPath = join(daemon.directory, "argv-injection-marker");
    const literal = `;$(touch ${markerPath})`;
    const argvRun = await daemon.client.start({
      program: "/usr/bin/printf",
      args: ["%s", literal],
      cwd: null,
      env: {},
      size: { cols: 80, rows: 24 },
      declared_inputs: [],
    });
    await waitForRunExit(daemon.client, argvRun.id);
    assert.equal(
      (await replayBytes(daemon.client, argvRun.id)).toString(),
      literal,
    );
    assert.equal(
      existsSync(markerPath),
      false,
      "argv punctuation executed as shell syntax",
    );

    const secret = `ctxmux-secret-${options.seed.toString(16)}`;
    const secretRun = await daemon.client.start({
      ...shellSpec(
        'test "$CTXMUX_SECRET" = "$CTXMUX_EXPECTED" && printf SECRET-BOUNDARY-OK',
      ),
      env: { CTXMUX_SECRET: secret, CTXMUX_EXPECTED: secret },
    });
    await waitForRunExit(daemon.client, secretRun.id);
    assert.equal(
      (await replayBytes(daemon.client, secretRun.id)).toString(),
      "SECRET-BOUNDARY-OK",
    );
    assert.equal(readFileSync(daemon.logPath, "utf8").includes(secret), false);

    const longLived = await openProtocolSocket(daemon.socketPath);
    longLived.write('{"type":"request","request":');
    await delay(50);
    await daemon.client.ping();
    assert.equal((await daemon.client.list()).length, before.length + 3);
    longLived.destroy();

    const oversized = await openProtocolSocket(daemon.socketPath);
    const closed = waitForSocketClose(oversized, 5_000);
    oversized.write(Buffer.alloc(MAX_FRAME_BYTES + 1, 0x78));
    await closed;
    assert.equal((await daemon.client.list()).length, before.length + 3);
    trace("security.negative_space", {
      rejected_spawn_published_run: false,
      invalid_resize_added_run: false,
      argv_executed_shell: false,
      secret_logged: false,
      long_lived_partial_blocked_daemon: false,
      oversized_frame_mutated_daemon: false,
    });

    return {
      socket_mode: (await stat(daemon.socketPath)).mode & 0o777,
      malformed_and_long_lived_frames: "no mutation",
      argv_and_environment: "structured and redacted",
      absent_denial_of_service_quotas: ["Runs", "attachments", "exited Runs"],
    };
  } finally {
    await daemon.close();
  }
}

async function runStressMatrix(
  options: QualificationOptions,
  receipt: QualificationReceipt,
  trace: (action: string, detail?: Record<string, unknown>) => void,
): Promise<unknown> {
  const results: Record<string, unknown> = {};
  const retentionBytes =
    options.profile === "smoke" ? 512 * 1024 : 5 * 1024 * 1024;
  {
    const daemon = await DaemonFixture.start("final-output", options, receipt);
    try {
      const run = await daemon.client.start(
        shellSpec(`head -c ${retentionBytes} /dev/zero; printf FINAL-DRAIN`),
      );
      await waitForRunExit(daemon.client, run.id, 20_000);
      const attachment = await daemon.client.attach(run.id);
      const bytes = Buffer.concat(
        attachment.snapshot.replay.chunks.map((chunk) =>
          Buffer.from(chunk.data),
        ),
      );
      attachment.close();
      assert.ok(bytes.subarray(-11).equals(Buffer.from("FINAL-DRAIN")));
      if (retentionBytes > 4 * 1024 * 1024) {
        assert.equal(attachment.snapshot.replay.truncated, true);
        assert.ok(bytes.length <= 4 * 1024 * 1024 + 8192);
      }
      results.replay_and_final_drain = {
        produced_bytes: retentionBytes + 11,
        retained_bytes: bytes.length,
        truncated: attachment.snapshot.replay.truncated,
        final_marker: true,
      };
    } finally {
      await daemon.close();
    }
  }

  const fanouts = options.profile === "smoke" ? [1] : [1, 8, 32];
  results.fanout = [];
  for (const fanout of fanouts) {
    const fanoutResult = await runFanoutScenario(
      fanout,
      options,
      receipt,
      trace,
    );
    (results.fanout as unknown[]).push(fanoutResult);
  }

  results.concurrent_start_pressure = await runConcurrentStartPressure(
    options,
    receipt,
    trace,
  );

  const churnCycles =
    options.profile === "smoke" ? 4 : options.profile === "release" ? 128 : 32;
  const daemon = await DaemonFixture.start("lifecycle-churn", options, receipt);
  try {
    const baseline = sampleProcess(daemon.pid);
    for (let index = 0; index < churnCycles; index += 1) {
      const run = await daemon.client.start(activeSpec());
      await waitForReplay(daemon.client, run.id, (bytes) =>
        bytes.includes(Buffer.from("READY")),
      );
      const attachment = await daemon.client.attach(run.id);
      await attachment.resize({ cols: 80 + (index % 20), rows: 24 });
      await attachment.input(`churn-${index}`);
      await attachment.detach();
      await daemon.client.stop(run.id);
      await waitForRunExit(daemon.client, run.id);
    }
    await waitForNoLiveChildren(daemon.pid, 10_000);
    const cleanup = sampleProcess(daemon.pid);
    assert.ok(cleanup.threads <= baseline.threads + 2);
    assert.equal(cleanup.descendants.length, 0);
    assert.ok(
      (await daemon.client.list()).every((run) => run.attachments === 0),
    );
    results.lifecycle_churn = {
      cycles: churnCycles,
      cleanup_threads_delta: cleanup.threads - baseline.threads,
      live_children: cleanup.descendants.length,
      retained_runs: (await daemon.client.list()).length,
      retained_runs_are_intentional_without_gc: true,
    };
  } finally {
    await daemon.close();
  }
  if (options.soakSeconds > 0) {
    results.soak = await runSoakScenario(options, receipt, trace);
  } else {
    results.soak = {
      duration_seconds: 0,
      skipped_by_profile: true,
    };
  }
  return results;
}

async function runConcurrentStartPressure(
  options: QualificationOptions,
  receipt: QualificationReceipt,
  trace: (action: string, detail?: Record<string, unknown>) => void,
): Promise<unknown> {
  const count = options.profile === "smoke" ? 8 : 32;
  const concurrency = options.profile === "smoke" ? 4 : 16;
  const daemon = await DaemonFixture.start(
    `concurrent-start-${count}`,
    options,
    receipt,
  );
  try {
    const starts = await mapLimitUntilFailure(
      Array.from({ length: count }, (_, index) => index),
      concurrency,
      async () => daemon.client.start(idleSpec()),
    );
    if (starts.failure !== undefined) {
      throw new Error(
        `concurrent start pressure failed after ${starts.outputs.length}/${count}: ${errorText(starts.failure.error)}`,
        { cause: starts.failure.error },
      );
    }
    await mapLimit(starts.outputs, concurrency, async (run) => {
      await daemon.client.stop(run.id);
    });
    await mapLimit(starts.outputs, concurrency, async (run) =>
      waitForRunExit(daemon.client, run.id),
    );
    await waitForNoLiveChildren(daemon.pid, 10_000);
    const cleanup = sampleProcess(daemon.pid);
    const result = {
      runs: count,
      start_concurrency: concurrency,
      successful_start_responses: starts.outputs.length,
      cleanup_live_children: cleanup.descendants.length,
    };
    trace("stress.concurrent_start", result);
    return result;
  } finally {
    await daemon.close();
  }
}

async function runSoakScenario(
  options: QualificationOptions,
  receipt: QualificationReceipt,
  trace: (action: string, detail?: Record<string, unknown>) => void,
): Promise<unknown> {
  const daemon = await DaemonFixture.start("soak", options, receipt);
  const runCount = 8;
  const sampler = startRssSampler(daemon.pid, 250);
  try {
    const baseline = sampleProcess(daemon.pid);
    const runs = await mapLimit(
      Array.from({ length: runCount }, (_, index) => index),
      options.resourceStartConcurrency,
      async () => daemon.client.start(activeSpec()),
    );
    await mapLimit(runs, runCount, async (run) => {
      await waitForReplay(daemon.client, run.id, (bytes) =>
        bytes.includes(Buffer.from("READY")),
      );
    });
    const startedAt = performance.now();
    const deadline = Date.now() + options.soakSeconds * 1000;
    let cycles = 0;
    while (Date.now() < deadline) {
      await mapLimit(runs, runCount, async (run) => {
        await daemon.client.input(run.id, Buffer.alloc(4096, 0x61));
      });
      if (cycles % 16 === 0) {
        await mapLimit(runs, runCount, async (run, index) => {
          await daemon.client.resize(run.id, {
            cols: 80 + ((cycles + index) % 40),
            rows: 24 + ((cycles + index) % 10),
          });
        });
      }
      if (cycles % 64 === 0) {
        const attachment = await daemon.client.attach(
          runs[cycles % runCount]!.id,
        );
        attachment.close();
      }
      cycles += 1;
      await delay(1_000);
    }
    const retainedOutputBytes = await retainedBytes(daemon.client, runs);
    assert.ok(
      retainedOutputBytes <= runCount * (4 * 1024 * 1024 + 8192),
      `soak retained ${retainedOutputBytes} bytes beyond the declared per-Run bound`,
    );
    await mapLimit(runs, runCount, async (run) => {
      await daemon.client.stop(run.id);
    });
    await mapLimit(runs, runCount, async (run) =>
      waitForRunExit(daemon.client, run.id),
    );
    await waitForNoLiveChildren(daemon.pid, 10_000);
    await waitForAttachmentCounts(daemon.client, runs, 0);
    const cleanup = sampleProcess(daemon.pid);
    assert.ok(cleanup.threads <= baseline.threads + 2);
    assert.equal(cleanup.descendants.length, 0);
    await sampler.stop();
    const result = {
      configured_duration_seconds: options.soakSeconds,
      elapsed_seconds: round((performance.now() - startedAt) / 1000, 3),
      cycles,
      active_runs: runCount,
      retained_output_bytes: retainedOutputBytes,
      peak_rss_kib: sampler.peak(),
      cleanup_threads_delta: cleanup.threads - baseline.threads,
      cleanup_live_children: cleanup.descendants.length,
      retained_runs_are_intentional_without_gc: true,
    };
    trace("stress.soak", result);
    return result;
  } finally {
    await sampler.stop();
    await daemon.close();
  }
}

async function runFanoutScenario(
  fanout: number,
  options: QualificationOptions,
  receipt: QualificationReceipt,
  trace: (action: string, detail?: Record<string, unknown>) => void,
): Promise<unknown> {
  const daemon = await DaemonFixture.start(
    `fanout-${fanout}`,
    options,
    receipt,
  );
  try {
    const run = await daemon.client.start(activeSpec());
    await waitForReplay(daemon.client, run.id, (bytes) =>
      bytes.includes(Buffer.from("READY")),
    );
    const attachments = await Promise.all(
      Array.from({ length: fanout }, () => daemon.client.attach(run.id)),
    );
    const slow = fanout > 1 ? attachments.at(-1)! : undefined;
    const fast = slow === undefined ? attachments : attachments.slice(0, -1);
    const payloadBytes =
      fanout === 32
        ? 4 * 1024 * 1024
        : fanout === 8
          ? 2 * 1024 * 1024
          : 256 * 1024;
    const payloadByte = seededPayloadByte(options.seed, fanout);
    const consumers = fast.map((attachment) =>
      consumeExactOutput(attachment, payloadBytes, payloadByte),
    );
    const writer = fast[0] ?? slow!;
    const chunk = Buffer.alloc(4096, payloadByte);
    for (let sent = 0; sent < payloadBytes; sent += chunk.length) {
      await writer.input(chunk);
    }
    const fastReceipts = await withDeadline(
      Promise.all(consumers),
      30_000,
      `fast fanout ${fanout}`,
    );

    let slowGap = false;
    if (slow !== undefined) {
      slowGap = await consumeUntilGap(slow, 30_000);
      assert.equal(slowGap, true, `slow fanout ${fanout} did not report Gap`);
    }
    trace("stress.fanout", {
      fanout,
      payload_bytes: payloadBytes,
      payload_byte: payloadByte,
      fast_receipts: fastReceipts,
      slow_gap: slowGap,
    });
    for (const attachment of attachments) attachment.close();
    await daemon.client.stop(run.id);
    await waitForRunExit(daemon.client, run.id);
    return {
      fanout,
      payload_bytes: payloadBytes,
      payload_byte: payloadByte,
      fast_exact: true,
      fast_receipts: fastReceipts,
      slow_gap: slowGap,
    };
  } finally {
    await daemon.close();
  }
}

async function runResourceCensus(
  options: QualificationOptions,
  receipt: QualificationReceipt,
  trace: (action: string, detail?: Record<string, unknown>) => void,
): Promise<readonly ResourceMeasurement[]> {
  const measurements: ResourceMeasurement[] = [];
  for (const mode of options.resourceModes) {
    for (const count of options.resourceCounts) {
      const measurement = await measureResources(
        mode,
        count,
        options,
        receipt,
        trace,
      );
      measurements.push(measurement);
      trace(
        "resource.measurement",
        measurement as unknown as Record<string, unknown>,
      );
    }
  }
  return measurements;
}

async function measureResources(
  mode: WorkloadMode,
  count: number,
  options: QualificationOptions,
  receipt: QualificationReceipt,
  trace: (action: string, detail?: Record<string, unknown>) => void,
): Promise<ResourceMeasurement> {
  const daemon = await DaemonFixture.start(
    `resources-${mode}-${count}`,
    options,
    receipt,
  );
  const peakSampler = startRssSampler(daemon.pid, 25);
  try {
    const baseline = sampleProcess(daemon.pid);
    const startResult = await mapLimitUntilFailure(
      Array.from({ length: count }, (_, index) => index),
      options.resourceStartConcurrency,
      async () =>
        daemon.client.start(mode === "active" ? activeSpec() : idleSpec()),
    );
    const runs = startResult.outputs;
    if (startResult.failure !== undefined) {
      const publishedRuns = await daemon.client.list().catch(() => []);
      const failureSample = sampleProcess(daemon.pid);
      trace("resource.start.failure", {
        mode,
        requested_runs: count,
        attempted_starts: startResult.attempted,
        successful_start_responses: runs.length,
        published_runs: publishedRuns.length,
        start_concurrency: options.resourceStartConcurrency,
        failed_index: startResult.failure.index,
        error: errorText(startResult.failure.error),
        process_sample: failureSample,
      });
      throw new Error(
        `${mode}/${count} Run creation failed after ${runs.length} successful responses with concurrency ${options.resourceStartConcurrency}: ${errorText(startResult.failure.error)}`,
        { cause: startResult.failure.error },
      );
    }
    if (mode === "active") {
      await mapLimit(runs, 16, async (run) => {
        await waitForReplay(daemon.client, run.id, (bytes) =>
          bytes.includes(Buffer.from("READY")),
        );
      });
      await mapLimit(runs, 16, async (run, index) => {
        await daemon.client.resize(run.id, {
          cols: 80 + (index % 40),
          rows: 24 + (index % 10),
        });
        await daemon.client.input(
          run.id,
          Buffer.alloc(4096, 0x41 + (index % 20)),
        );
      });
    }

    await waitForAttachmentCounts(daemon.client, runs, 0);
    const cpuBefore = sampleProcess(daemon.pid);
    const wallStart = performance.now();
    if (mode === "active") {
      for (let round = 0; round < 2; round += 1) {
        await mapLimit(runs, 16, async (run) => {
          await daemon.client.input(run.id, Buffer.alloc(2048, 0x61 + round));
        });
      }
    }
    await delay(options.profile === "smoke" ? 250 : 750);
    const steady = sampleProcess(daemon.pid);
    const elapsedSeconds = (performance.now() - wallStart) / 1000;
    const retainedOutputBytes = await retainedBytes(daemon.client, runs);

    await mapLimit(runs, 16, async (run) => {
      await daemon.client.stop(run.id);
    });
    await mapLimit(runs, 16, async (run) =>
      waitForRunExit(daemon.client, run.id),
    );
    await waitForNoLiveChildren(daemon.pid, 10_000);
    await waitForAttachmentCounts(daemon.client, runs, 0);
    await delay(100);
    const cleanup = sampleProcess(daemon.pid);
    const cleanupAttachments = (await daemon.client.list()).reduce(
      (sum, run) => sum + run.attachments,
      0,
    );
    const cleanupRetainedRuns = (await daemon.client.list()).length;
    await peakSampler.stop();
    const divide = (value: number): number => round(value / count, 3);
    return {
      mode,
      runs: count,
      baseline,
      steady,
      cleanup,
      peak_rss_kib: peakSampler.peak(),
      peak_rss_sample_count: peakSampler.sampleCount(),
      peak_rss_sample_interval_ms: 25,
      cpu_core_percent: round(
        ((steady.cpu_seconds - cpuBefore.cpu_seconds) / elapsedSeconds) * 100,
        3,
      ),
      retained_output_bytes: retainedOutputBytes,
      retained_output_bytes_per_run: divide(retainedOutputBytes),
      rss_kib_per_run: divide(Math.max(0, steady.rss_kib - baseline.rss_kib)),
      threads_per_run: divide(Math.max(0, steady.threads - baseline.threads)),
      fds_per_run: divide(Math.max(0, steady.fds - baseline.fds)),
      cleanup_rss_kib_delta: Math.max(0, cleanup.rss_kib - baseline.rss_kib),
      cleanup_fds_delta: Math.max(0, cleanup.fds - baseline.fds),
      cleanup_retained_runs: cleanupRetainedRuns,
      cleanup_live_children: cleanup.descendants.length,
      cleanup_attachments: cleanupAttachments,
      intentional_retained_state_without_gc: true,
    };
  } finally {
    await peakSampler.stop();
    await daemon.close();
  }
}

class DaemonFixture {
  public readonly client: CtxmuxClient;
  #closed = false;
  #logClosed = false;

  private constructor(
    public readonly label: string,
    public readonly directory: string,
    public readonly socketPath: string,
    public readonly logPath: string,
    public readonly child: ChildProcess,
    private readonly log: WriteStream,
  ) {
    this.client = new CtxmuxClient({ socketPath });
  }

  public get pid(): number {
    assert.notEqual(this.child.pid, undefined);
    return this.child.pid!;
  }

  public static async start(
    label: string,
    options: QualificationOptions,
    receipt: QualificationReceipt,
  ): Promise<DaemonFixture> {
    assert.deepEqual(
      fileIdentity(daemonBinary),
      receipt.provenance.daemon,
      "ctxmux daemon bytes changed during qualification",
    );
    const directory = await mkdtemp(join(tmpdir(), `ctxmux-${label}-`));
    const socketPath = join(directory, "ctxmux.sock");
    const logName = `${options.preflight.invocation_nonce}-${sanitize(label)}-daemon.log`;
    const log = createWriteStream(logName, {
      fd: openFreshOwnedFile(logName),
      autoClose: true,
    });
    const child = spawn(daemonBinary, ["--socket", socketPath], {
      cwd: root,
      stdio: ["ignore", "pipe", "pipe"],
    });
    child.stdout?.pipe(log, { end: false });
    child.stderr?.pipe(log, { end: false });
    const fixture = new DaemonFixture(
      label,
      directory,
      socketPath,
      logName,
      child,
      log,
    );
    receipt.daemon_logs.push(
      portablePath(join(options.artifactDirectory, logName)),
    );
    try {
      await withDeadline(
        poll(async () => {
          if (child.exitCode !== null) {
            throw new Error(
              `ctxmuxd ${label} exited during readiness: ${child.exitCode}`,
            );
          }
          try {
            await fixture.client.ping();
            return true;
          } catch {
            return false;
          }
        }),
        5_000,
        `daemon readiness ${label}`,
      );
      return fixture;
    } catch (error) {
      await fixture.close();
      throw error;
    }
  }

  public async kill(signal: NodeJS.Signals): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    this.child.kill(signal);
    await waitForProcess(this.child, 5_000);
    await this.closeLog();
    await rm(this.directory, { recursive: true, force: true });
  }

  public async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    if (this.child.exitCode === null) {
      this.child.kill("SIGINT");
      try {
        await waitForProcess(this.child, 2_000);
      } catch {
        this.child.kill("SIGKILL");
        await waitForProcess(this.child, 5_000);
      }
    }
    await this.closeLog();
    await rm(this.directory, { recursive: true, force: true });
  }

  private async closeLog(): Promise<void> {
    if (this.#logClosed) return;
    this.#logClosed = true;
    this.child.stdout?.unpipe(this.log);
    this.child.stderr?.unpipe(this.log);
    this.log.end();
    await withDeadline(finished(this.log), 5_000, `daemon log ${this.label}`);
  }
}

async function integrationHost(
  socketPath: string | undefined,
  outputPath: string | undefined,
) {
  assert.ok(
    socketPath && outputPath,
    "integration host requires socket and output path",
  );
  const client = new CtxmuxClient({ socketPath });
  const shell = registerIntegration(client, shellIntegration);
  const run = await shell.start(
    { args: ["-c", "exec /bin/cat"] },
    { detection: { executable: "/bin/sh" } },
  );
  await writeFile(
    outputPath,
    `${JSON.stringify({ id: run.id, pid: requiredPid(run) })}\n`,
  );
}

function parseOptions(): QualificationOptions {
  assertKnownOptions();
  const rawProfile = optionValue("--profile") ?? "smoke";
  assert.ok(
    rawProfile === "smoke" ||
      rawProfile === "nightly" ||
      rawProfile === "release" ||
      rawProfile === "observe",
    `invalid reliability profile: ${String(rawProfile)}`,
  );
  const profile = rawProfile as QualificationProfile;
  const rawObservationRound = optionValue("--observation-round");
  const observationRound =
    rawObservationRound === undefined
      ? null
      : positiveInteger("--observation-round", rawObservationRound);
  const rawStage = optionValue("--stage") ?? "all";
  assert.ok(
    rawStage === "all" || rawStage === "resource-census",
    `invalid qualification stage: ${rawStage}`,
  );
  const stage = rawStage as QualificationStage;
  const seed = positiveInteger(
    "CTXMUX_RELIABILITY_SEED",
    process.env.CTXMUX_RELIABILITY_SEED ?? "226004",
  );
  const profilePolicy = QUALIFICATION_POLICY.profiles[profile];
  const timeBudgetSeconds = positiveInteger(
    "CTXMUX_RELIABILITY_TIME_BUDGET_SECONDS",
    process.env.CTXMUX_RELIABILITY_TIME_BUDGET_SECONDS ??
      String(profilePolicy.time_budget_seconds),
  );
  const artifactDirectory = resolve(
    root,
    process.env.CTXMUX_RELIABILITY_ARTIFACT_DIR ??
      `target/reliability/${profile}`,
  );
  const evidencePath = resolve(
    root,
    process.env.CTXMUX_RELIABILITY_EVIDENCE ??
      join(artifactDirectory, "result.json"),
  );
  const canonicalArtifactDirectory = resolve(
    root,
    `target/reliability/${profile}`,
  );
  assert.equal(
    artifactDirectory,
    canonicalArtifactDirectory,
    "CTXMUX_RELIABILITY_ARTIFACT_DIR must resolve to the canonical profile directory",
  );
  assert.equal(
    evidencePath,
    join(canonicalArtifactDirectory, "result.json"),
    "CTXMUX_RELIABILITY_EVIDENCE must resolve to the canonical profile receipt",
  );
  const preflight = parseQualificationPreflight(
    process.env.CTXMUX_RELIABILITY_PREFLIGHT,
    profile,
  );
  if (process.env.CTXMUX_RELIABILITY_WORKER === "1") {
    assertInheritedArtifactOwner(preflight.artifact_owner_identity);
  } else {
    enterCanonicalArtifactOwner({
      root,
      profile,
      expectedIdentity: preflight.artifact_owner_identity,
      create: false,
    });
  }
  const resourceCounts = positiveIntegerList(
    "--resource-counts",
    optionValue("--resource-counts"),
    profilePolicy.resource_counts,
  );
  const resourceModes = workloadModeList(optionValue("--resource-modes"));
  const resourceStartConcurrency = positiveInteger(
    "--resource-start-concurrency",
    optionValue("--resource-start-concurrency") ??
      String(QUALIFICATION_POLICY.resource_start_concurrency),
  );
  assert.ok(
    resourceStartConcurrency <= 128,
    "--resource-start-concurrency must be at most 128",
  );
  const soakSeconds = nonNegativeInteger(
    "--soak-seconds",
    optionValue("--soak-seconds") ?? String(profilePolicy.soak_seconds),
  );
  assert.ok(
    soakSeconds === 0 || soakSeconds + 10 * 60 < timeBudgetSeconds,
    "soak duration must leave at least ten minutes inside the qualification time budget",
  );
  return {
    profile,
    observationRound,
    stage,
    seed,
    artifactDirectory,
    preflight,
    timeBudgetSeconds,
    resourceCounts,
    resourceModes,
    resourceStartConcurrency,
    soakSeconds,
  };
}

function assertKnownOptions(): void {
  const names = new Set([
    "--profile",
    "--observation-round",
    "--stage",
    "--resource-counts",
    "--resource-modes",
    "--resource-start-concurrency",
    "--soak-seconds",
  ]);
  const args = process.argv.slice(2);
  for (let index = 0; index < args.length; index += 2) {
    const name = args[index];
    assert.ok(name !== undefined && names.has(name), `unknown option: ${name}`);
    const value = args[index + 1];
    assert.ok(
      value !== undefined && !value.startsWith("--"),
      `${name} needs a value`,
    );
  }
}

function optionValue(name: string): string | undefined {
  const indexes = process.argv.flatMap((value, index) =>
    value === name ? [index] : [],
  );
  assert.ok(indexes.length <= 1, `${name} may be provided only once`);
  const index = indexes[0];
  if (index === undefined) return undefined;
  const value = process.argv[index + 1];
  assert.ok(
    value !== undefined && !value.startsWith("--"),
    `${name} needs a value`,
  );
  return value;
}

function positiveIntegerList(
  name: string,
  raw: string | undefined,
  fallback: readonly number[],
): readonly number[] {
  if (raw === undefined) return fallback;
  const values = raw.split(",").map((value) => positiveInteger(name, value));
  assert.equal(
    new Set(values).size,
    values.length,
    `${name} contains duplicates`,
  );
  return values;
}

function workloadModeList(raw: string | undefined): readonly WorkloadMode[] {
  if (raw === undefined) return ["idle", "active"];
  const values = raw.split(",");
  assert.ok(values.length > 0, "--resource-modes needs at least one mode");
  assert.ok(
    values.every((value) => value === "idle" || value === "active"),
    "--resource-modes accepts only idle,active",
  );
  assert.equal(
    new Set(values).size,
    values.length,
    "--resource-modes contains duplicates",
  );
  return values as WorkloadMode[];
}

function captureProvenance(): QualificationProvenance {
  const budgets = JSON.parse(readFileSync(budgetPath, "utf8")) as {
    readonly measurement_contract?: unknown;
  };
  assert.notEqual(
    budgets.measurement_contract,
    undefined,
    "reliability budgets have no measurement contract",
  );
  const buildArgv = buildArgvFromEnvironment();
  const targetDirectory = process.env.CTXMUX_RELIABILITY_BUILD_TARGET_DIR ?? "";
  return {
    claim_scope: "locally_observed",
    binary_source_attestation: false,
    source: captureSourceIdentity(),
    harness: fileIdentity(harnessPath),
    launcher: fileIdentity(launcherPath),
    daemon: fileIdentity(daemonBinary),
    lockfiles: [
      fileIdentity(resolve(root, "Cargo.lock")),
      fileIdentity(resolve(root, "package-lock.json")),
    ],
    build: {
      cwd: ".",
      argv: buildArgv,
      source_commit: process.env.CTXMUX_RELIABILITY_BUILD_SOURCE_COMMIT ?? "",
      source_tree: process.env.CTXMUX_RELIABILITY_BUILD_SOURCE_TREE ?? "",
      worktree_clean:
        process.env.CTXMUX_RELIABILITY_BUILD_WORKTREE_CLEAN === "true",
      target_directory: targetDirectory,
      daemon_path: provenancePath(daemonBinary),
      locked: buildArgv.includes("--locked"),
    },
    toolchain: {
      rustc_version_verbose: commandOutput("rustc", [
        "--version",
        "--verbose",
      ]).trim(),
      cargo_version: commandOutput("cargo", ["--version"]).trim(),
      node_version: process.version,
    },
    measurement_contract_encoding: "json-stringify-utf8",
    measurement_contract_sha256: sha256(
      JSON.stringify(budgets.measurement_contract),
    ),
  };
}

function captureSourceIdentity(): QualificationProvenance["source"] {
  const entries = commandOutput("git", [
    "status",
    "--porcelain=v1",
    "-z",
    "--untracked-files=all",
  ])
    .split("\0")
    .filter((entry) => entry.length > 0);
  return {
    commit: commandOutput("git", ["rev-parse", "HEAD"]).trim(),
    tree: commandOutput("git", ["rev-parse", "HEAD^{tree}"]).trim(),
    worktree: {
      status_format: "git-status-porcelain-v1-z",
      clean: entries.length === 0,
      entries,
    },
  };
}

function buildArgvFromEnvironment(): readonly string[] {
  const raw = process.env.CTXMUX_RELIABILITY_BUILD_ARGV_JSON;
  if (raw === undefined) return [];
  try {
    const value = JSON.parse(raw) as unknown;
    return Array.isArray(value) &&
      value.every((item) => typeof item === "string")
      ? value
      : [];
  } catch {
    return [];
  }
}

function fileIdentity(path: string): FileIdentity {
  return {
    path: provenancePath(path),
    sha256: sha256(readFileSync(path)),
  };
}

function provenancePath(path: string): string {
  const absolute = resolve(path);
  const portable = relative(root, absolute).replaceAll("\\", "/");
  return portable === ".." || portable.startsWith("../") ? absolute : portable;
}

function sha256(value: string | Buffer): string {
  return createHash("sha256").update(value).digest("hex");
}

function assertQualificationProvenance(
  options: QualificationOptions,
  provenance: QualificationProvenance,
): void {
  assert.deepEqual(
    provenance.build.argv,
    fixedBuildArgv,
    "qualification must use the fixed locked daemon build argv",
  );
  assert.equal(
    provenance.build.target_directory,
    fixedBuildTargetDirectory,
    "qualification must use the fixed provenance build directory",
  );
  assert.equal(provenance.build.daemon_path, fixedDaemonPath);
  assert.equal(provenance.build.locked, true);
  assert.equal(
    provenance.build.source_commit,
    provenance.source.commit,
    "source commit changed after the locked daemon build",
  );
  assert.equal(
    provenance.build.source_tree,
    provenance.source.tree,
    "source tree changed after the locked daemon build",
  );
  assert.equal(
    provenance.build.worktree_clean,
    provenance.source.worktree.clean,
    "worktree clean state changed after the locked daemon build",
  );
  assert.equal(provenance.harness.path, "scripts/reliability-qualification.ts");
  assert.equal(provenance.launcher.path, "scripts/check-reliability.sh");
  assert.equal(provenance.daemon.path, fixedDaemonPath);
  assert.deepEqual(
    provenance.lockfiles.map(({ path }) => path),
    ["Cargo.lock", "package-lock.json"],
  );

  assert.deepEqual(
    provenance.source,
    captureSourceIdentity(),
    "source identity changed after provenance capture",
  );
  assert.deepEqual(provenance.harness, fileIdentity(harnessPath));
  assert.deepEqual(provenance.launcher, fileIdentity(launcherPath));
  assert.deepEqual(provenance.daemon, fileIdentity(daemonBinary));
  assert.deepEqual(provenance.lockfiles, [
    fileIdentity(resolve(root, "Cargo.lock")),
    fileIdentity(resolve(root, "package-lock.json")),
  ]);
  const budgets = JSON.parse(readFileSync(budgetPath, "utf8")) as {
    readonly measurement_contract?: unknown;
  };
  assert.notEqual(budgets.measurement_contract, undefined);
  assert.equal(
    provenance.measurement_contract_sha256,
    sha256(JSON.stringify(budgets.measurement_contract)),
    "measurement contract changed after provenance capture",
  );

  if (options.profile === "observe") {
    assert.ok(
      options.observationRound !== null &&
        options.observationRound >= 1 &&
        options.observationRound <= 3,
      "observe requires --observation-round 1, 2, or 3",
    );
    assert.equal(
      provenance.source.worktree.clean,
      true,
      "observe requires a clean worktree",
    );
    assert.equal(options.stage, "all", "observe requires --stage all");
    assert.deepEqual(
      options.resourceCounts,
      [1, 32, 128],
      "observe requires the complete 1/32/128 resource matrix",
    );
    assert.deepEqual(
      options.resourceModes,
      ["idle", "active"],
      "observe requires the complete idle/active resource matrix",
    );
    assert.equal(
      options.resourceStartConcurrency,
      8,
      "observe requires resource start concurrency 8",
    );
    assert.equal(options.soakSeconds, 0, "observe must not run a time soak");
  } else {
    assert.equal(
      options.observationRound,
      null,
      "--observation-round is reserved for observe",
    );
  }
}

function readBudgets(): BudgetFile {
  const value = JSON.parse(readFileSync(budgetPath, "utf8")) as BudgetFile;
  assert.equal(value.schema, "ctxmux.reliability-budgets.v1");
  assert.equal(value.frozen_before_optimization, true);
  return value;
}

function budgetFor(
  budgets: BudgetFile,
  measurement: ResourceMeasurement,
): ResourceBudget {
  const budget = budgets.budgets[measurement.mode][String(measurement.runs)];
  assert.notEqual(
    budget,
    undefined,
    `missing ${measurement.mode}/${measurement.runs} resource budget`,
  );
  return budget!;
}

function assertResourceBudget(
  measurement: ResourceMeasurement,
  budget: ResourceBudget,
): void {
  const checks: ReadonlyArray<[string, number, number]> = [
    [
      "cpu_core_percent",
      measurement.cpu_core_percent,
      budget.max_cpu_core_percent,
    ],
    ["peak_rss_kib", measurement.peak_rss_kib, budget.max_peak_rss_kib],
    ["steady_rss_kib", measurement.steady.rss_kib, budget.max_steady_rss_kib],
    [
      "retained_output_bytes_per_run",
      measurement.retained_output_bytes_per_run,
      budget.max_retained_output_bytes_per_run,
    ],
    [
      "rss_kib_per_run",
      measurement.rss_kib_per_run,
      budget.max_rss_kib_per_run,
    ],
    [
      "threads_per_run",
      measurement.threads_per_run,
      budget.max_threads_per_run,
    ],
    ["fds_per_run", measurement.fds_per_run, budget.max_fds_per_run],
    [
      "cleanup_threads_delta",
      Math.max(0, measurement.cleanup.threads - measurement.baseline.threads),
      budget.max_cleanup_threads_delta,
    ],
    [
      "cleanup_live_children",
      measurement.cleanup_live_children,
      budget.max_cleanup_live_children,
    ],
    [
      "cleanup_attachments",
      measurement.cleanup_attachments,
      budget.max_cleanup_attachments,
    ],
  ];
  for (const [name, actual, maximum] of checks) {
    assert.ok(
      actual <= maximum,
      `${measurement.mode}/${measurement.runs} ${name}=${actual} exceeds frozen ${maximum}`,
    );
  }
}

function sampleProcess(pid: number): ProcessSample {
  const ps = commandOutput("ps", [
    "-o",
    "rss=",
    "-o",
    "time=",
    "-p",
    String(pid),
  ]);
  const fields = ps.trim().split(/\s+/u);
  assert.ok(
    fields.length >= 2,
    `cannot parse process sample for ${pid}: ${ps}`,
  );
  return {
    rss_kib: Number.parseInt(fields[0]!, 10),
    cpu_seconds: parseCpuTime(fields.at(-1)!),
    threads: countThreads(pid),
    fds: countFileDescriptors(pid),
    descendants: processTree(pid),
  };
}

function sampleRssKiB(pid: number): number {
  const raw = commandOutput("ps", ["-o", "rss=", "-p", String(pid)]).trim();
  const rss = Number.parseInt(raw, 10);
  assert.ok(Number.isFinite(rss), `cannot parse RSS sample for ${pid}: ${raw}`);
  return rss;
}

function startRssSampler(pid: number, intervalMs: number): RssSampler {
  let peakRss = sampleRssKiB(pid);
  let samples = 1;
  let stopped = false;
  let samplingError: unknown;
  const loop = (async () => {
    while (!stopped) {
      await delay(intervalMs);
      if (stopped) break;
      try {
        peakRss = Math.max(peakRss, sampleRssKiB(pid));
        samples += 1;
      } catch (error) {
        samplingError = error;
        stopped = true;
      }
    }
  })();
  return {
    peak: () => peakRss,
    sampleCount: () => samples,
    stop: async () => {
      stopped = true;
      await loop;
      if (samplingError !== undefined) throw samplingError;
    },
  };
}

function countThreads(pid: number): number {
  if (platform() === "linux") return readdirSync(`/proc/${pid}/task`).length;
  const output = commandOutput("ps", ["-M", "-p", String(pid)]);
  return Math.max(0, output.trim().split("\n").length - 1);
}

function countFileDescriptors(pid: number): number {
  if (platform() === "linux") return readdirSync(`/proc/${pid}/fd`).length;
  const output = commandOutput("lsof", ["-a", "-p", String(pid), "-Fn"]);
  return output.split("\n").filter((line) => /^f/u.test(line)).length;
}

function processTree(rootPid: number): ProcessTreeEntry[] {
  const rows = commandOutput("ps", ["-axo", "pid=,ppid=,state=,comm="])
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => {
      const match = /^(\d+)\s+(\d+)\s+(\S+)\s+(.*)$/u.exec(line);
      if (match === null) return undefined;
      return {
        pid: Number(match[1]),
        ppid: Number(match[2]),
        state: match[3]!,
        command: match[4]!,
      };
    })
    .filter((value): value is ProcessTreeEntry => value !== undefined);
  const descendants: ProcessTreeEntry[] = [];
  const parents = new Set([rootPid]);
  let changed = true;
  while (changed) {
    changed = false;
    for (const row of rows) {
      if (parents.has(row.ppid) && !parents.has(row.pid)) {
        parents.add(row.pid);
        descendants.push(row);
        changed = true;
      }
    }
  }
  return descendants;
}

function idleSpec(): RunSpec {
  return {
    program: "/bin/cat",
    args: [],
    cwd: null,
    env: {},
    size: { cols: 80, rows: 24 },
    declared_inputs: [],
  };
}

function activeSpec(): RunSpec {
  return shellSpec("stty raw -echo; printf READY; exec /bin/cat");
}

function shellSpec(script: string): RunSpec {
  return {
    program: "/bin/sh",
    args: ["-c", script],
    cwd: null,
    env: {},
    size: { cols: 80, rows: 24 },
    declared_inputs: [],
  };
}

async function replayBytes(client: CtxmuxClient, id: RunId): Promise<Buffer> {
  const attachment = await client.attach(id);
  const bytes = Buffer.concat(
    attachment.snapshot.replay.chunks.map((chunk) => Buffer.from(chunk.data)),
  );
  attachment.close();
  return bytes;
}

async function retainedBytes(
  client: CtxmuxClient,
  runs: readonly RunInfo[],
): Promise<number> {
  const values = await mapLimit(
    runs,
    16,
    async (run) => (await replayBytes(client, run.id)).length,
  );
  return values.reduce((sum, value) => sum + value, 0);
}

async function waitForReplay(
  client: CtxmuxClient,
  id: RunId,
  predicate: (bytes: Buffer) => boolean,
  timeoutMs = 10_000,
): Promise<void> {
  await withDeadline(
    poll(async () => predicate(await replayBytes(client, id))),
    timeoutMs,
    `Run ${id} replay predicate`,
  );
}

async function waitForRunExit(
  client: CtxmuxClient,
  id: RunId,
  timeoutMs = 10_000,
) {
  await withDeadline(
    poll(async () => (await client.status(id)).state.type === "exited"),
    timeoutMs,
    `Run ${id} exit`,
  );
}

async function waitForAttachmentCounts(
  client: CtxmuxClient,
  runs: readonly RunInfo[],
  expected: number,
) {
  await withDeadline(
    poll(async () => {
      const statuses = await mapLimit(runs, 16, async (run) =>
        client.status(run.id),
      );
      return statuses.every((run) => run.attachments === expected);
    }),
    10_000,
    `attachment count ${expected}`,
  );
}

async function waitForNoLiveChildren(pid: number, timeoutMs: number) {
  await withDeadline(
    poll(async () => processTree(pid).length === 0),
    timeoutMs,
    `daemon ${pid} child cleanup`,
  );
}

async function consumeExactOutput(
  attachment: Awaited<ReturnType<CtxmuxClient["attach"]>>,
  expectedBytes: number,
  expectedByte: number,
): Promise<{
  readonly bytes: number;
  readonly chunks: number;
  readonly first_seq: number | null;
  readonly last_seq: number;
}> {
  let observed = 0;
  let chunks = 0;
  let expectedSequence = attachment.snapshot.replay.head_seq + 1;
  const firstSequence = expectedSequence;
  while (observed < expectedBytes) {
    const event = await attachment.nextEvent();
    assert.notEqual(
      event,
      undefined,
      "fast attachment closed before workload completion",
    );
    if (event?.type === "output") {
      assert.equal(
        event.chunk.seq,
        expectedSequence,
        "fast attachment output sequence was duplicated or skipped",
      );
      assert.ok(
        event.chunk.data.length <= expectedBytes - observed,
        "fast attachment produced more bytes than the fixed workload",
      );
      assert.ok(
        event.chunk.data.every((byte) => byte === expectedByte),
        "fast attachment changed the seeded payload content",
      );
      observed += event.chunk.data.length;
      chunks += 1;
      expectedSequence += 1;
    }
    if (event?.type === "gap")
      assert.fail(`fast attachment reported Gap at ${event.head_seq}`);
    if (event?.type === "exited")
      assert.fail("Run exited before fanout workload completed");
  }
  assert.equal(observed, expectedBytes);
  return {
    bytes: observed,
    chunks,
    first_seq: chunks === 0 ? null : firstSequence,
    last_seq: expectedSequence - 1,
  };
}

function seededPayloadByte(seed: number, salt: number): number {
  let value = (seed ^ Math.imul(salt, 0x9e37_79b1)) >>> 0;
  value ^= value << 13;
  value ^= value >>> 17;
  value ^= value << 5;
  return 0x41 + ((value >>> 0) % 26);
}

async function consumeUntilGap(
  attachment: Awaited<ReturnType<CtxmuxClient["attach"]>>,
  timeoutMs: number,
): Promise<boolean> {
  return withDeadline(
    (async () => {
      while (true) {
        const event = await attachment.nextEvent();
        if (event === undefined || event.type === "exited") return false;
        if (event.type === "gap") return true;
      }
    })(),
    timeoutMs,
    "slow attachment Gap",
  );
}

async function openProtocolSocket(socketPath: string): Promise<Socket> {
  const socket = createConnection({ path: socketPath });
  await withDeadline(
    new Promise<void>((resolveReady, reject) => {
      socket.once("connect", resolveReady);
      socket.once("error", reject);
    }),
    5_000,
    "raw socket connect",
  );
  socket.write(
    `${JSON.stringify({ type: "hello", hello: { protocol: PROTOCOL_VERSION } })}\n`,
  );
  const line = await readSocketLine(socket, 5_000);
  const hello = JSON.parse(line) as {
    readonly type?: string;
    readonly protocol?: number;
  };
  assert.deepEqual(hello, { type: "hello", protocol: PROTOCOL_VERSION });
  return socket;
}

async function readSocketLine(
  socket: Socket,
  timeoutMs: number,
): Promise<string> {
  return withDeadline(
    new Promise<string>((resolveLine, reject) => {
      let buffered = Buffer.alloc(0);
      const onData = (chunk: Buffer): void => {
        buffered = Buffer.concat([buffered, chunk]);
        const newline = buffered.indexOf(0x0a);
        if (newline >= 0) {
          socket.off("data", onData);
          resolveLine(buffered.subarray(0, newline).toString("utf8"));
        }
      };
      socket.on("data", onData);
      socket.once("error", reject);
    }),
    timeoutMs,
    "raw socket line",
  );
}

async function waitForSocketClose(
  socket: Socket,
  timeoutMs: number,
): Promise<void> {
  return withDeadline(
    new Promise<void>((resolveClosed, reject) => {
      socket.once("close", () => resolveClosed());
      socket.once("error", (error) => {
        if ((error as NodeJS.ErrnoException).code === "ECONNRESET")
          resolveClosed();
        else reject(error);
      });
    }),
    timeoutMs,
    "raw socket close",
  );
}

async function poll(check: () => Promise<boolean>): Promise<void> {
  while (!(await check())) await delay(10);
}

async function withDeadline<T>(
  promise: Promise<T>,
  timeoutMs: number,
  label: string,
): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timer = setTimeout(
          () => reject(new Error(`${label} timed out after ${timeoutMs}ms`)),
          timeoutMs,
        );
      }),
    ]);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}

async function mapLimit<Input, Output>(
  values: readonly Input[],
  limit: number,
  action: (value: Input, index: number) => Promise<Output>,
): Promise<Output[]> {
  const outputs = new Array<Output>(values.length);
  let cursor = 0;
  const workers = Array.from(
    { length: Math.min(limit, values.length) },
    async () => {
      while (cursor < values.length) {
        const index = cursor;
        cursor += 1;
        outputs[index] = await action(values[index]!, index);
      }
    },
  );
  await Promise.all(workers);
  return outputs;
}

async function mapLimitUntilFailure<Input, Output>(
  values: readonly Input[],
  limit: number,
  action: (value: Input, index: number) => Promise<Output>,
): Promise<{
  readonly outputs: Output[];
  readonly attempted: number;
  readonly failure?: { readonly index: number; readonly error: unknown };
}> {
  const outputs = new Array<Output | undefined>(values.length);
  let cursor = 0;
  let failure: { readonly index: number; readonly error: unknown } | undefined;
  const workers = Array.from(
    { length: Math.min(limit, values.length) },
    async () => {
      while (cursor < values.length && failure === undefined) {
        const index = cursor;
        cursor += 1;
        try {
          outputs[index] = await action(values[index]!, index);
        } catch (error) {
          failure ??= { index, error };
        }
      }
    },
  );
  await Promise.all(workers);
  const result = {
    outputs: outputs.filter((value): value is Output => value !== undefined),
    attempted: cursor,
  };
  return failure === undefined ? result : { ...result, failure };
}

async function waitForProcess(
  child: ChildProcess,
  timeoutMs: number,
): Promise<{
  readonly code: number | null;
  readonly signal: NodeJS.Signals | null;
}> {
  if (child.exitCode !== null || child.signalCode !== null) {
    return { code: child.exitCode, signal: child.signalCode };
  }
  return withDeadline(
    new Promise((resolveExit, reject) => {
      child.once("exit", (code, signal) => resolveExit({ code, signal }));
      child.once("error", reject);
    }),
    timeoutMs,
    `process ${child.pid ?? "unknown"} exit`,
  );
}

async function waitForProcessGone(
  pid: number,
  timeoutMs: number,
): Promise<void> {
  await withDeadline(
    poll(async () => !processExists(pid)),
    timeoutMs,
    `process ${pid} cleanup`,
  );
}

function processExists(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

function requiredPid(run: RunInfo): number {
  assert.notEqual(run.pid, null, `Run ${run.id} has no native PID`);
  return run.pid!;
}

function parseCpuTime(value: string): number {
  const dayParts = value.split("-");
  const days = dayParts.length === 2 ? Number(dayParts[0]) : 0;
  const clock = dayParts.at(-1)!.split(":").map(Number);
  assert.ok(clock.every(Number.isFinite), `invalid process CPU time: ${value}`);
  const seconds = clock.at(-1)!;
  const minutes = clock.length >= 2 ? clock.at(-2)! : 0;
  const hours = clock.length >= 3 ? clock.at(-3)! : 0;
  return days * 86400 + hours * 3600 + minutes * 60 + seconds;
}

function commandOutput(command: string, args: readonly string[]): string {
  const result = spawnSync(command, [...args], { cwd: root, encoding: "utf8" });
  assert.equal(
    result.status,
    0,
    `${command} ${args.join(" ")} failed: ${result.stderr || result.error?.message || "unknown"}`,
  );
  return result.stdout;
}

function positiveInteger(name: string, value: string): number {
  const parsed = Number.parseInt(value, 10);
  assert.ok(
    Number.isSafeInteger(parsed) && parsed > 0 && String(parsed) === value,
    `${name} must be a canonical positive integer`,
  );
  return parsed;
}

function nonNegativeInteger(name: string, value: string): number {
  const parsed = Number.parseInt(value, 10);
  assert.ok(
    Number.isSafeInteger(parsed) && parsed >= 0 && String(parsed) === value,
    `${name} must be a canonical non-negative integer`,
  );
  return parsed;
}

function portablePath(path: string): string {
  const portable = relative(root, path).replaceAll("\\", "/");
  return portable.startsWith("../") ? basename(path) : portable;
}

function sanitize(value: string): string {
  return value.replaceAll(/[^a-z0-9-]+/giu, "-").toLowerCase();
}

function round(value: number, digits: number): number {
  const factor = 10 ** digits;
  return Math.round(value * factor) / factor;
}

function errorText(error: unknown): string {
  return error instanceof Error
    ? `${error.name}: ${error.message}`
    : String(error);
}

void main().catch((error: unknown) => {
  console.error(errorText(error));
  process.exitCode = 1;
});
