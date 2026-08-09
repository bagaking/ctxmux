import assert from "node:assert/strict";
import { execFileSync, spawn, spawnSync } from "node:child_process";
import crypto from "node:crypto";
import { once } from "node:events";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { pathToFileURL } from "node:url";

import {
  deriveBudgetCeiling,
  loadBaselineReceipts,
  loadSourceSnapshots,
  prepareQualificationEvidencePath,
  qualificationPolicyFromHarness,
  validateQualificationArtifacts,
  validateQualificationInvocationIdentity,
  validatePassingQualificationReceiptV3,
  validateReliabilityPolicy,
} from "./reliability-policy.mjs";
import {
  POLICY_SOURCE_PATHS,
  SOURCE_FILE_PATHS,
  validatePassingObservationReceipt,
  validatePassingQualificationReceipt,
} from "./reliability-baseline-policy.mjs";
import { deriveObservedMaxima } from "./reliability-budget-contract.mjs";
import {
  enterCanonicalArtifactOwner,
  openFreshOwnedFile,
  parseQualificationPreflight,
  readOwnedJson,
  writeOwnedJsonAtomically,
} from "./reliability-artifact-owner.mts";
import { loadReliabilityGcContract } from "./reliability-gc-contract.mts";

const root = path.resolve(import.meta.dirname, "..");
const receiptPaths = [
  "fixtures/reliability/observe-darwin-arm64-r1.json",
  "fixtures/reliability/observe-darwin-arm64-r2.json",
  "fixtures/reliability/observe-darwin-arm64-r3.json",
];
const stageIds = [
  "chaos-owner-matrix",
  "security-negative-space",
  "stress-and-soak",
  "resource-census",
];
const observedFields = [
  "cpu_core_percent",
  "peak_rss_kib",
  "steady_rss_kib",
  "retained_output_bytes_per_run",
  "rss_kib_per_run",
  "threads_per_run",
  "fds_per_run",
  "cleanup_threads_delta",
  "cleanup_live_children",
  "cleanup_attachments",
];
const invocationNonce = "ab".repeat(32);

function actualInputs() {
  const budgets = JSON.parse(
    fs.readFileSync(path.join(root, "reliability-budgets.json"), "utf8"),
  );
  const baselineReceipts = loadBaselineReceipts(root, budgets);
  return {
    budgets,
    baselineReceipts,
    sourceSnapshots: loadSourceSnapshots(root, baselineReceipts),
    currentPolicyHashes: Object.fromEntries(
      POLICY_SOURCE_PATHS.map((filePath) => [
        filePath,
        crypto
          .createHash("sha256")
          .update(fs.readFileSync(path.join(root, filePath)))
          .digest("hex"),
      ]),
    ),
    workflow: fs.readFileSync(
      path.join(root, ".github", "workflows", "reliability.yml"),
      "utf8",
    ),
    checkScript: fs.readFileSync(
      path.join(root, "scripts", "check.sh"),
      "utf8",
    ),
    qualificationScript: fs.readFileSync(
      path.join(root, "scripts", "check-reliability.sh"),
      "utf8",
    ),
    harnessSource: fs.readFileSync(
      path.join(root, "scripts", "reliability-qualification.ts"),
      "utf8",
    ),
  };
}

function syntheticSourceSnapshots(receipts, commit) {
  const tree = execFileSync("git", ["rev-parse", `${commit}^{tree}`], {
    cwd: root,
    encoding: "utf8",
  }).trim();
  const fileHashes = Object.fromEntries([
    ...SOURCE_FILE_PATHS.map((filePath) => [
      filePath,
      crypto
        .createHash("sha256")
        .update(
          execFileSync("git", ["show", `${commit}:${filePath}`], { cwd: root }),
        )
        .digest("hex"),
    ]),
    ...POLICY_SOURCE_PATHS.map((filePath) => [
      filePath,
      crypto
        .createHash("sha256")
        .update(fs.readFileSync(path.join(root, filePath)))
        .digest("hex"),
    ]),
  ]);
  return receipts.map(({ path: receiptPath }) => ({
    path: receiptPath,
    commit,
    reachableFromHead: true,
    tree,
    fileHashes,
  }));
}

function buildV2Template() {
  const inputs = actualInputs();
  const commit = execFileSync("git", ["rev-parse", "HEAD"], {
    cwd: root,
    encoding: "utf8",
  }).trim();
  const bootstrap = receiptPaths.map((receiptPath) => ({
    path: receiptPath,
    value: {
      schema: "ctxmux.reliability-qualification.v2",
      provenance: { source: { commit } },
    },
  }));
  const policyModulesAreCommitted =
    spawnSync("git", ["cat-file", "-e", `HEAD:${POLICY_SOURCE_PATHS.at(-1)}`], {
      cwd: root,
    }).status === 0;
  const snapshots = policyModulesAreCommitted
    ? loadSourceSnapshots(root, bootstrap)
    : syntheticSourceSnapshots(bootstrap, commit);
  assert.equal(snapshots.length, 3);
  assert.ok(snapshots.every(({ error }) => error === undefined));
  const firstSnapshot = snapshots[0];
  const environment = {
    os: "darwin",
    os_release: "test-release",
    architecture: "arm64",
    logical_cpus: 8,
    cpu_model: "synthetic-test-cpu",
  };
  inputs.budgets.observation_baseline.environment = environment;
  inputs.budgets.frozen_at = "2026-08-12T00:00:00.000Z";
  inputs.budgets.observation_baseline.rounds = 3;
  inputs.budgets.observation_baseline.resource_start_concurrency = 8;
  inputs.budgets.observation_baseline.peak_rss_sample_interval_ms = 25;
  inputs.budgets.observation_baseline.policy_contracts =
    POLICY_SOURCE_PATHS.map((filePath) => ({
      path: filePath,
      sha256: firstSnapshot.fileHashes[filePath],
    }));
  inputs.currentPolicyHashes = Object.fromEntries(
    POLICY_SOURCE_PATHS.map((filePath) => [
      filePath,
      firstSnapshot.fileHashes[filePath],
    ]),
  );

  const contractHash = crypto
    .createHash("sha256")
    .update(JSON.stringify(inputs.budgets.measurement_contract))
    .digest("hex");
  const provenance = {
    claim_scope: "locally_observed",
    binary_source_attestation: false,
    source: {
      commit,
      tree: firstSnapshot.tree,
      worktree: {
        status_format: "git-status-porcelain-v1-z",
        clean: true,
        entries: [],
      },
    },
    harness: {
      path: "scripts/reliability-qualification.ts",
      sha256: firstSnapshot.fileHashes["scripts/reliability-qualification.ts"],
    },
    launcher: {
      path: "scripts/check-reliability.sh",
      sha256: firstSnapshot.fileHashes["scripts/check-reliability.sh"],
    },
    daemon: {
      path: "target/reliability/provenance-build/debug/ctxmuxd",
      sha256: "d".repeat(64),
    },
    lockfiles: [
      {
        path: "Cargo.lock",
        sha256: firstSnapshot.fileHashes["Cargo.lock"],
      },
      {
        path: "package-lock.json",
        sha256: firstSnapshot.fileHashes["package-lock.json"],
      },
    ],
    build: {
      cwd: ".",
      argv: [
        "cargo",
        "build",
        "--locked",
        "--quiet",
        "--package",
        "ctxmux-daemon",
        "--target-dir",
        "target/reliability/provenance-build",
      ],
      source_commit: commit,
      source_tree: firstSnapshot.tree,
      worktree_clean: true,
      target_directory: "target/reliability/provenance-build",
      daemon_path: "target/reliability/provenance-build/debug/ctxmuxd",
      locked: true,
    },
    toolchain: {
      rustc_version_verbose: "rustc synthetic verbose",
      cargo_version: "cargo synthetic",
      node_version: "v99.0.0",
    },
    measurement_contract_encoding: "json-stringify-utf8",
    measurement_contract_sha256: contractHash,
  };
  const declaredLimits = {
    frame_bytes: 1024 * 1024,
    retained_output_bytes_per_run: 4 * 1024 * 1024,
    live_event_capacity: 256,
    global_run_quota: null,
    global_attachment_quota: null,
    exited_run_gc: null,
    qualification_stage: "all",
    resource_counts: [1, 32, 128],
    resource_modes: ["idle", "active"],
    resource_start_concurrency: 8,
    peak_rss_sample_interval_ms: 25,
    soak_seconds: 0,
    seed_controls: ["fanout payload byte", "secret marker"],
    note: "Synthetic policy fixture; not an observation.",
  };
  const receipts = [1, 2, 3].map((round, index) => {
    const cells = ["idle", "active"].flatMap((mode) =>
      [1, 32, 128].map((runs) => resourceCell(mode, runs, round)),
    );
    const stages = stageIds.map((id, stageIndex) => ({
      id,
      status: "pass",
      started_at: `2026-08-11T00:0${index}:0${stageIndex + 1}.000Z`,
      completed_at: `2026-08-11T00:0${index}:0${stageIndex + 1}.500Z`,
      result: id === "resource-census" ? cells : { synthetic: true },
    }));
    const actionTrace = [
      {
        timestamp: `2026-08-11T00:0${index}:00.000Z`,
        action: "provenance.captured",
        source_commit: provenance.source.commit,
        worktree_clean: true,
        harness_sha256: provenance.harness.sha256,
        launcher_sha256: provenance.launcher.sha256,
        daemon_sha256: provenance.daemon.sha256,
        measurement_contract_sha256: provenance.measurement_contract_sha256,
      },
      {
        timestamp: `2026-08-11T00:0${index}:00.100Z`,
        action: "provenance.verified",
        observation_round: round,
      },
      ...stageIds.flatMap((id, stageIndex) => [
        {
          timestamp: `2026-08-11T00:0${index}:0${stageIndex + 1}.000Z`,
          action: "stage.start",
          id,
        },
        {
          timestamp: `2026-08-11T00:0${index}:0${stageIndex + 1}.500Z`,
          action: "stage.pass",
          id,
        },
      ]),
      {
        timestamp: `2026-08-11T00:0${index}:09.000Z`,
        action: "provenance.reverified",
        daemon_sha256: provenance.daemon.sha256,
      },
    ];
    return {
      path: receiptPaths[index],
      sha256: String(round).repeat(64),
      value: {
        schema: "ctxmux.reliability-qualification.v2",
        status: "pass",
        profile: "observe",
        observation_round: round,
        seed: 226004,
        recorded_at: `2026-08-11T00:0${index}:00.000Z`,
        completed_at: `2026-08-11T00:0${index}:10.000Z`,
        time_budget_seconds: 2700,
        environment: structuredClone(environment),
        provenance: structuredClone(provenance),
        declared_limits: structuredClone(declaredLimits),
        action_trace: actionTrace,
        stages,
        daemon_logs: ["target/reliability/observe/synthetic-daemon.log"],
        error: null,
      },
    };
  });
  inputs.baselineReceipts = receipts;
  inputs.sourceSnapshots = snapshots;
  inputs.budgets.observation_baseline.raw_receipts = receipts.map(
    ({ path: receiptPath, sha256 }) => ({ path: receiptPath, sha256 }),
  );
  inputs.budgets.observation_baseline.observed_maxima =
    observedMaxima(receipts);
  for (const mode of ["idle", "active"]) {
    for (const count of ["1", "32", "128"]) {
      const maxima =
        inputs.budgets.observation_baseline.observed_maxima[mode][count];
      inputs.budgets.budgets[mode][count] = Object.fromEntries(
        observedFields.map((field) => [
          `max_${field}`,
          deriveBudgetCeiling(field, maxima[field]),
        ]),
      );
    }
  }
  return inputs;
}

function passingSmokeReceiptFixture() {
  const inputs = buildV2Template();
  const receipt = structuredClone(inputs.baselineReceipts[0]);
  receipt.value.profile = "smoke";
  receipt.value.observation_round = null;
  receipt.value.time_budget_seconds = 60;
  receipt.value.declared_limits.resource_counts = [1];
  receipt.value.declared_limits.soak_seconds = 0;
  const resourceStage = receipt.value.stages.find(
    ({ id }) => id === "resource-census",
  );
  resourceStage.result = resourceStage.result.filter(({ runs }) => runs === 1);
  const frozenStage = {
    id: "frozen-resource-budgets",
    status: "pass",
    started_at: "2026-08-11T00:00:08.000Z",
    completed_at: "2026-08-11T00:00:08.500Z",
    result: { synthetic: true },
  };
  receipt.value.stages.push(frozenStage);
  const reverified = receipt.value.action_trace.pop();
  receipt.value.action_trace.push(
    {
      timestamp: frozenStage.started_at,
      action: "stage.start",
      id: frozenStage.id,
    },
    {
      timestamp: frozenStage.completed_at,
      action: "stage.pass",
      id: frozenStage.id,
    },
    reverified,
  );
  receipt.value.action_trace.find(
    ({ action }) => action === "provenance.verified",
  ).observation_round = null;
  receipt.value.action_trace.find(
    ({ action }) => action === "provenance.captured",
  ).invocation_nonce = invocationNonce;
  const policyErrors = [];
  const qualificationPolicy = qualificationPolicyFromHarness(
    inputs.harnessSource,
    policyErrors,
  );
  assert.deepEqual(policyErrors, []);
  return {
    receipt,
    notBefore: "2026-08-10T23:59:59.999Z",
    verifiedAt: "2026-08-11T00:00:11.000Z",
    qualificationPolicy,
    snapshot: {
      ...structuredClone(inputs.sourceSnapshots[0]),
      path: receipt.path,
    },
    budgets: inputs.budgets,
    policyContracts: inputs.budgets.observation_baseline.policy_contracts,
    invocationNonce,
  };
}

function passingNightlyV3ReceiptFixture() {
  const inputs = buildV2Template();
  const gc = loadReliabilityGcContract(root);
  const qualificationErrors = [];
  const qualificationPolicy = qualificationPolicyFromHarness(
    inputs.harnessSource,
    qualificationErrors,
  );
  assert.deepEqual(qualificationErrors, []);
  const receipt = structuredClone(inputs.baselineReceipts[0]);
  const provenance = receipt.value.provenance;
  provenance.workload_contract = structuredClone(gc.workload_contract);
  provenance.workload_helper = structuredClone(gc.workload_helper);
  const epoch = (suffix) => ({
    daemon_instance: `00000000-0000-0000-0000-${suffix.padStart(12, "0")}`,
    seq: 2,
    current: {
      retained_runs: 128,
      creation_keys: 128,
      creation_flights: 0,
      publication_reservations: 0,
      collecting_tickets: 0,
      overlap_owners: 0,
      cleanup_owners: 0,
      direct_children: 0,
      readers: 0,
      waiters: 0,
      input_drains: 0,
      attachments: 0,
      tmux_owners: 0,
    },
    high_water: {
      retained_runs: 128,
      creation_keys: 128,
      creation_flights: 8,
      publication_reservations: 8,
      collecting_tickets: 8,
      overlap_owners: 8,
      cleanup_owners: 0,
      direct_children: 8,
      readers: 8,
      waiters: 8,
      input_drains: 0,
      attachments: 8,
      tmux_owners: 0,
    },
    cumulative: {
      physical_starts_total: 512,
      candidate_selections_total: 384,
      candidate_evaluations_total: 384,
      candidate_evaluations_max: 128,
      candidate_fences_total: 0,
      exact_replacements_total: 384,
    },
  });
  const digest = (
    mode,
    payloadBytes,
    totalReplayBytes = 128 * payloadBytes,
    recovered = false,
    firstIndex = 0,
  ) => {
    const base = Math.floor(totalReplayBytes / 128);
    const remainder = totalReplayBytes % 128;
    const tuples = Array.from({ length: 128 }, (_, offset) => {
      const index = firstIndex + offset;
      const replayBytes = base + (offset < remainder ? 1 : 0);
      const sourceDigest = crypto
        .createHash("sha256")
        .update(`226004:${mode}:${String(index)}`, "utf8")
        .digest("hex");
      const operationKey = `gc-pressure:${mode}:${String(index)}:${sourceDigest}`;
      const suffixOffset =
        (payloadBytes - replayBytes) % sourceDigest.length;
      const suffix = Buffer.from(
        sourceDigest
          .repeat(
            Math.ceil(
              (suffixOffset + replayBytes) / sourceDigest.length,
            ),
          )
          .slice(suffixOffset, suffixOffset + replayBytes),
        "ascii",
      );
      const replaySha256 = crypto
        .createHash("sha256")
        .update(suffix)
        .digest("hex");
      const chunkIdentity = (seq, start, bytes) => ({
        seq,
        bytes,
        sha256: crypto
          .createHash("sha256")
          .update(
            Buffer.from(
              sourceDigest
                .repeat(
                  Math.ceil(
                    ((start % sourceDigest.length) + bytes) /
                      sourceDigest.length,
                  ),
                )
                .slice(
                  start % sourceDigest.length,
                  (start % sourceDigest.length) + bytes,
                ),
              "ascii",
            ),
          )
          .digest("hex"),
      });
      const firstChunkBytes = Math.floor(replayBytes / 2);
      return {
        run_id: `00000000-0000-4000-8000-${index.toString(16).padStart(12, "0")}`,
        operation_key: operationKey,
        lineage: null,
        state: { type: "exited", code: 0, signal: null },
        head_seq: 2,
        durable_head_seq: mode.startsWith("persistent")
          ? 2
          : null,
        oldest_seq: recovered ? 2 : 1,
        replay_bytes: replayBytes,
        replay_sha256: replaySha256,
        chunks: recovered
          ? [
              chunkIdentity(
                2,
                payloadBytes - replayBytes,
                replayBytes,
              ),
            ]
          : [
              chunkIdentity(1, 0, firstChunkBytes),
              chunkIdentity(
                2,
                firstChunkBytes,
                replayBytes - firstChunkBytes,
              ),
            ],
        truncated: recovered,
      };
    });
    return {
      count: tuples.length,
      total_replay_bytes: totalReplayBytes,
      sha256: crypto
        .createHash("sha256")
        .update(JSON.stringify(tuples))
        .digest("hex"),
      tuples,
    };
  };
  const turnovers = [1, 2, 3].map((window) => ({
    window,
    retained_runs: 128,
    physical_start_delta: 128,
    retry_physical_start_delta: 0,
    candidate_selections_delta: 128,
    candidate_evaluations_delta: 16_384,
    candidate_fences_delta: 128,
    exact_replacements_delta: 128,
  }));
  const churn = ["memory_only", "persistent"].map((mode, index) => ({
    mode,
    successful_lifecycles: 512,
    fill_physical_start_delta: 128,
    turnovers: structuredClone(turnovers),
    restart:
      mode === "persistent"
        ? {
            after_window: 2,
            before: digest(mode, 4096, 128 * 4096, false, 256),
            after: digest(mode, 4096, 128 * 4096, false, 256),
            new_incarnation_initial_physical_starts: 0,
          }
        : null,
    retained: digest(mode, 4096, 128 * 4096, false, 384),
    epochs: mode === "persistent" ? [epoch("1"), epoch("2")] : [epoch("3")],
  }));
  const rssSamples = [
    { timestamp_ms: 1_000, rss_kib: 1 },
    { timestamp_ms: 1_025, rss_kib: 1 },
  ];
  const pressure = ["memory_replay_pressure", "persistent_replay_pressure"].map(
    (mode, index) => ({
      mode,
      before: digest(mode, 4_194_304),
      after: digest(mode, 4_194_304, 128 * 4_194_304, false, 8),
      fill_physical_start_delta: 128,
      replacement_physical_start_delta: 8,
      retry_physical_start_delta: 0,
      replay_verification_runs_before_replacement: 128,
      replay_verification_runs_after_replacement: 128,
      peak_rss_kib: 1,
      max_rss_sample_gap_ms: 25,
      rss_samples: structuredClone(rssSamples),
      average_cpu_core_percent: 0,
      quiescent_cpu_core_percent: 0,
      peak_thread_delta: 0,
      peak_fd_delta: 0,
      recovered:
        mode === "persistent_replay_pressure"
          ? {
              replay: digest(mode, 4_194_304, 268_427_265, true, 8),
              retry_physical_start_delta: 0,
              steady_rss_kib: 1,
              peak_rss_kib: 1,
              max_rss_sample_gap_ms: 25,
              rss_samples: structuredClone(rssSamples),
              quiescent_cpu_core_percent: 0,
              quiescent_thread_delta: 0,
              quiescent_fd_delta: 0,
            }
          : null,
      epochs:
        mode === "persistent_replay_pressure"
          ? [epoch(String(index + 4)), epoch(String(index + 5))]
          : [epoch(String(index + 4))],
    }),
  );
  const retainedResult = {
    bounded_churn: churn,
    replay_pressure: pressure,
  };
  const soakResult = {
    configured_duration_seconds: 1800,
    elapsed_seconds: 1800,
    cycles: 1800,
    active_runs: 8,
    retained_output_bytes: 1,
    peak_rss_kib: 1,
    cleanup_threads_delta: 0,
    cleanup_live_children: 0,
    cleanup_attachments: 0,
    rss_samples: structuredClone(rssSamples),
    max_rss_sample_gap_ms: 25,
    retained_runs_are_intentional_without_gc: true,
  };
  const stageIdsV3 = [
    "chaos-owner-matrix",
    "security-negative-space",
    "stress-and-soak",
    "retained-state-plateau",
    "resource-census",
    "frozen-resource-budgets",
  ];
  const originalResourceResult = receipt.value.stages.find(
    ({ id }) => id === "resource-census",
  ).result;
  receipt.value.schema = "ctxmux.reliability-qualification.v3";
  receipt.value.profile = "nightly";
  receipt.value.observation_round = null;
  receipt.value.time_budget_seconds = 4200;
  receipt.value.recorded_at = "2026-08-11T00:00:00.000Z";
  receipt.value.completed_at = "2026-08-11T00:31:00.000Z";
  receipt.value.error = null;
  receipt.value.daemon_logs = ["target/reliability/nightly/daemon.log"];
  receipt.value.stats_logs = [
    {
      path: "target/reliability/nightly/stats.ndjson",
      sha256: "b".repeat(64),
      daemon_instance: "00000000-0000-0000-0000-000000000001",
      final_seq: 2,
    },
  ];
  Object.assign(receipt.value.declared_limits, {
    global_run_quota: 128,
    exited_run_gc: "exact_terminal_replacement",
    qualification_stage: "all",
    resource_counts: [1, 32, 128],
    resource_modes: ["idle", "active"],
    resource_start_concurrency: 8,
    soak_seconds: 1800,
  });
  receipt.value.stages = stageIdsV3.map((id, index) => ({
    id,
    status: "pass",
    started_at: `2026-08-11T00:00:0${String(index + 1)}.000Z`,
    completed_at: `2026-08-11T00:00:0${String(index + 1)}.500Z`,
    result:
      id === "retained-state-plateau"
        ? retainedResult
        : id === "stress-and-soak"
          ? { soak: soakResult }
          : id === "resource-census"
            ? originalResourceResult
            : { synthetic: true },
  }));
  const stressStage = receipt.value.stages.find(
    ({ id }) => id === "stress-and-soak",
  );
  stressStage.started_at = "2026-08-11T00:00:03.000Z";
  stressStage.completed_at = "2026-08-11T00:30:03.000Z";
  for (const [id, second] of [
    ["retained-state-plateau", 4],
    ["resource-census", 5],
    ["frozen-resource-budgets", 6],
  ]) {
    const stage = receipt.value.stages.find((candidate) => candidate.id === id);
    stage.started_at = `2026-08-11T00:30:0${String(second)}.000Z`;
    stage.completed_at = `2026-08-11T00:30:0${String(second)}.500Z`;
  }
  const captured = {
    timestamp: "2026-08-11T00:00:00.000Z",
    action: "provenance.captured",
    source_commit: provenance.source.commit,
    worktree_clean: true,
    harness_sha256: provenance.harness.sha256,
    launcher_sha256: provenance.launcher.sha256,
    daemon_sha256: provenance.daemon.sha256,
    measurement_contract_sha256: provenance.measurement_contract_sha256,
    workload_contract: provenance.workload_contract,
    workload_helper: provenance.workload_helper,
    invocation_nonce: invocationNonce,
  };
  const trace = [
    captured,
    {
      timestamp: "2026-08-11T00:00:00.100Z",
      action: "provenance.verified",
      observation_round: null,
    },
  ];
  for (const [index, id] of stageIdsV3.entries()) {
    const afterSoak = index >= 3;
    trace.push({
      timestamp: `2026-08-11T00:${afterSoak ? "30" : "00"}:0${String(index + 1)}.000Z`,
      action: "stage.start",
      id,
    });
    if (id === "retained-state-plateau") {
      for (const mode of ["memory_only", "persistent"]) {
        for (const window of [1, 2, 3]) {
          trace.push({
            timestamp: "2026-08-11T00:30:04.100Z",
            action: "gc.turnover",
            mode,
            window,
          });
        }
      }
      for (const mode of [
        "memory_replay_pressure",
        "persistent_replay_pressure",
      ]) {
        trace.push({
          timestamp: "2026-08-11T00:30:04.200Z",
          action: "gc.replay_pressure",
          mode,
        });
      }
    }
    if (id === "stress-and-soak") {
      trace.push({
        timestamp: "2026-08-11T00:30:02.000Z",
        action: "stress.soak",
        configured_duration_seconds: 1800,
        elapsed_seconds: 1800,
      });
    }
    trace.push({
      timestamp:
        id === "stress-and-soak"
          ? "2026-08-11T00:30:03.000Z"
          : `2026-08-11T00:${afterSoak ? "30" : "00"}:0${String(index + 1)}.500Z`,
      action: "stage.pass",
      id,
    });
  }
  trace.push({
    timestamp: "2026-08-11T00:30:10.000Z",
    action: "provenance.reverified",
    daemon_sha256: provenance.daemon.sha256,
    workload_contract: provenance.workload_contract,
    workload_helper: provenance.workload_helper,
  });
  receipt.value.action_trace = trace;
  return {
    receipt,
    qualificationPolicy,
    gc,
    budgets: inputs.budgets,
    preflight: {
      invocation_nonce: invocationNonce,
      workload_contract: structuredClone(gc.workload_contract),
      workload_helper: structuredClone(gc.workload_helper),
    },
    notBefore: "2026-08-10T23:59:59.999Z",
    verifiedAt: "2026-08-11T00:31:01.000Z",
  };
}

function resourceCell(mode, runs, round) {
  const baseline = {
    rss_kib: 1000,
    cpu_seconds: 1,
    threads: 2,
    fds: 4,
    descendants: [],
  };
  const steady = {
    rss_kib: baseline.rss_kib + runs * (100 + round),
    cpu_seconds: 2,
    threads: baseline.threads + runs,
    fds: baseline.fds + 2 * runs,
    descendants: [],
  };
  const cleanup = {
    rss_kib: 1000 + round,
    cpu_seconds: 3,
    threads: baseline.threads + round,
    fds: baseline.fds,
    descendants: [],
  };
  const retainedPerRun = mode === "active" ? 4096 + round : 0;
  return {
    mode,
    runs,
    baseline,
    steady,
    cleanup,
    peak_rss_kib: steady.rss_kib + 100,
    peak_rss_sample_count: 10,
    peak_rss_sample_interval_ms: 25,
    cpu_core_percent: (mode === "active" ? 10 : 1) + round,
    retained_output_bytes: retainedPerRun * runs,
    retained_output_bytes_per_run: retainedPerRun,
    rss_kib_per_run: 100 + round,
    threads_per_run: 1,
    fds_per_run: 2,
    cleanup_rss_kib_delta: round,
    cleanup_fds_delta: 0,
    cleanup_retained_runs: runs,
    cleanup_live_children: 0,
    cleanup_attachments: 0,
    intentional_retained_state_without_gc: true,
  };
}

function observedMaxima(receipts) {
  const result = { idle: {}, active: {} };
  for (const mode of ["idle", "active"]) {
    for (const count of ["1", "32", "128"]) {
      const cells = receipts.map(({ value }) =>
        value.stages
          .find(({ id }) => id === "resource-census")
          .result.find(
            (cell) => cell.mode === mode && String(cell.runs) === count,
          ),
      );
      result[mode][count] = deriveObservedMaxima(cells);
    }
  }
  return result;
}

const v2Template = buildV2Template();
const v2Inputs = () => structuredClone(v2Template);
const errorsFor = (mutate) => {
  const inputs = v2Inputs();
  mutate(inputs);
  return validateReliabilityPolicy(inputs);
};

test("accepts the checked-in source-bound v2 baseline", () => {
  const inputs = actualInputs();
  assert.deepEqual(validateReliabilityPolicy(inputs), []);
  assert.ok(
    inputs.baselineReceipts.every(
      ({ value }) => value.schema === "ctxmux.reliability-qualification.v2",
    ),
  );
  assert.equal(inputs.sourceSnapshots.length, 3);
  assert.ok(inputs.sourceSnapshots.every(({ error }) => error === undefined));
});

test("accepts a complete synthetic source-bound v2 baseline", () => {
  assert.deepEqual(validateReliabilityPolicy(v2Inputs()), []);
  assert.equal(v2Template.sourceSnapshots.length, 3);
  assert.ok(
    v2Template.sourceSnapshots.every(
      ({ reachableFromHead, tree, fileHashes }) =>
        reachableFromHead === true &&
        /^[0-9a-f]{40}$/u.test(tree) &&
        Object.keys(fileHashes).length === 5,
    ),
  );
});

test("validates a complete source-bound qualification receipt postcondition", () => {
  let fixture = passingSmokeReceiptFixture();
  const validate = () =>
    validatePassingQualificationReceipt({
      receiptPath: fixture.receipt.path,
      value: fixture.receipt.value,
      expectedProfile: "smoke",
      qualificationPolicy: fixture.qualificationPolicy,
      snapshot: fixture.snapshot,
      budgets: fixture.budgets,
      policyContracts: fixture.policyContracts,
      notBefore: fixture.notBefore,
      verifiedAt: fixture.verifiedAt,
      invocationNonce: fixture.invocationNonce,
    });
  assert.deepEqual(validate(), []);
  assert.deepEqual(
    validateQualificationInvocationIdentity(fixture.receipt.value, {
      commit: fixture.receipt.value.provenance.source.commit,
      tree: fixture.receipt.value.provenance.source.tree,
      clean: true,
    }),
    [],
  );
  assert.ok(
    validateQualificationInvocationIdentity(fixture.receipt.value, {
      commit: "0".repeat(40),
      tree: fixture.receipt.value.provenance.source.tree,
      clean: true,
    })[0].includes("exact clean current source"),
  );
  fixture.receipt.value.status = "running";
  fixture.receipt.value.stages.pop();
  const errors = validate();
  assert.ok(errors.some((error) => error.includes("pass smoke")));
  assert.ok(errors.some((error) => error.includes("exactly 5 stages")));

  fixture = passingSmokeReceiptFixture();
  fixture.receipt.value.declared_limits.soak_seconds = 1;
  fixture.receipt.value.profile = "release";
  const weakenedErrors = validate();
  assert.ok(weakenedErrors.some((error) => error.includes("pass smoke")));
  assert.ok(weakenedErrors.some((error) => error.includes("workload is not")));

  fixture = passingSmokeReceiptFixture();
  fixture.receipt.value.completed_at = "2026-08-10T23:59:59.000Z";
  fixture.receipt.value.stages[0].completed_at = "2026-08-10T23:59:59.000Z";
  const chronologyErrors = validate();
  assert.ok(
    chronologyErrors.some((error) => error.includes("start and completion")),
  );
  assert.ok(
    chronologyErrors.some((error) => error.includes("pass with timestamps")),
  );

  fixture = passingSmokeReceiptFixture();
  fixture.notBefore = "2026-08-11T00:00:00.001Z";
  assert.ok(validate().some((error) => error.includes("current invocation")));

  fixture = passingSmokeReceiptFixture();
  fixture.receipt.value.action_trace.find(
    ({ action }) => action === "provenance.captured",
  ).invocation_nonce = "cd".repeat(32);
  assert.ok(validate().some((error) => error.includes("invocation nonce")));

  for (const action of [
    "stage.fail",
    "supervisor.timeout",
    "synthetic.unknown",
  ]) {
    fixture = passingSmokeReceiptFixture();
    fixture.receipt.value.action_trace.push({
      timestamp: fixture.receipt.value.completed_at,
      action,
      id: "resource-census",
      error: "synthetic contradiction",
    });
    assert.ok(
      validate().some((error) => error.includes("failure action")),
      action,
    );
  }

  fixture = passingSmokeReceiptFixture();
  fixture.receipt.value.action_trace[2].timestamp = "2026-08-11T00:00:09.500Z";
  assert.ok(validate().some((error) => error.includes("chronology")));

  fixture = passingSmokeReceiptFixture();
  fixture.receipt.value.stages[0].started_at = "2026-08-10T23:59:59.000Z";
  assert.ok(validate().some((error) => error.includes("receipt interval")));

  fixture = passingSmokeReceiptFixture();
  for (const field of ["recorded_at", "completed_at"]) {
    fixture.receipt.value[field] = fixture.receipt.value[field].replace(
      "2026-",
      "2126-",
    );
  }
  for (const entry of fixture.receipt.value.action_trace) {
    entry.timestamp = entry.timestamp.replace("2026-", "2126-");
  }
  for (const stage of fixture.receipt.value.stages) {
    stage.started_at = stage.started_at.replace("2026-", "2126-");
    stage.completed_at = stage.completed_at.replace("2026-", "2126-");
  }
  assert.ok(validate().some((error) => error.includes("current invocation")));
});

test("production v3 verifier is mutation-sensitive to the frozen GC evidence", () => {
  const fixture = passingNightlyV3ReceiptFixture();
  const rehashTupleEvidence = (evidence) => {
    evidence.count = evidence.tuples.length;
    evidence.total_replay_bytes = evidence.tuples.reduce(
      (sum, tuple) => sum + tuple.replay_bytes,
      0,
    );
    evidence.sha256 = crypto
      .createHash("sha256")
      .update(JSON.stringify(evidence.tuples))
      .digest("hex");
  };
  const validate = (value = fixture.receipt.value) =>
    validatePassingQualificationReceiptV3({
      receiptPath: fixture.receipt.path,
      value,
      expectedProfile: "nightly",
      qualificationPolicy: fixture.qualificationPolicy,
      gc: fixture.gc,
      preflight: fixture.preflight,
      budgets: fixture.budgets,
      notBefore: fixture.notBefore,
      verifiedAt: fixture.verifiedAt,
    });
  assert.deepEqual(validate(), []);
  for (const [label, mutate] of [
    ["stats logs", (value) => value.stats_logs.pop()],
    [
      "workload identity",
      (value) => (value.provenance.workload_contract.sha256 = "f".repeat(64)),
    ],
    [
      "GC stage",
      (value) =>
        value.stages
          .find(({ id }) => id === "retained-state-plateau")
          .result.bounded_churn[0].turnovers.pop(),
    ],
    [
      "GC trace",
      (value) => {
        const index = value.action_trace.findIndex(
          ({ action }) => action === "gc.turnover",
        );
        value.action_trace.splice(index, 1);
      },
    ],
    [
      "frozen budget",
      (value) =>
        (value.stages.find(
          ({ id }) => id === "retained-state-plateau",
        ).result.replay_pressure[0].peak_rss_kib = 1_000_000),
    ],
    [
      "owner boundary",
      (value) =>
        (value.stages.find(
          ({ id }) => id === "retained-state-plateau",
        ).result.bounded_churn[0].epochs[0].current.waiters = 1),
    ],
    [
      "RSS sample series",
      (value) =>
        value.stages
          .find(({ id }) => id === "retained-state-plateau")
          .result.replay_pressure[0].rss_samples.pop(),
    ],
    [
      "duplicate GC mode",
      (value) =>
        (value.stages.find(
          ({ id }) => id === "retained-state-plateau",
        ).result.bounded_churn[1].mode = "memory_only"),
    ],
    [
      "shortened soak",
      (value) => {
        value.stages.find(
          ({ id }) => id === "stress-and-soak",
        ).result.soak.elapsed_seconds = 1799;
        value.action_trace.find(
          ({ action }) => action === "stress.soak",
        ).elapsed_seconds = 1799;
      },
    ],
    [
      "shortened soak wall clock",
      (value) => {
        value.stages.find(({ id }) => id === "stress-and-soak").completed_at =
          "2026-08-11T00:30:02.000Z";
      },
    ],
    [
      "turnover transition counter",
      (value) => {
        value.stages.find(
          ({ id }) => id === "retained-state-plateau",
        ).result.bounded_churn[0].turnovers[0].candidate_fences_delta = 0;
      },
    ],
    [
      "pressure before replay",
      (value) => {
        value.stages.find(
          ({ id }) => id === "retained-state-plateau",
        ).result.replay_pressure[0].before.total_replay_bytes -= 1;
      },
    ],
    [
      "bounded replay digest",
      (value) => {
        const evidence = value.stages.find(
          ({ id }) => id === "retained-state-plateau",
        ).result.bounded_churn[0].retained;
        evidence.tuples[0].replay_sha256 = "f".repeat(64);
        rehashTupleEvidence(evidence);
      },
    ],
    [
      "bounded missing tuple with rehashed envelope",
      (value) => {
        const evidence = value.stages.find(
          ({ id }) => id === "retained-state-plateau",
        ).result.bounded_churn[0].retained;
        evidence.tuples.pop();
        rehashTupleEvidence(evidence);
      },
    ],
    [
      "bounded duplicate tuple with rehashed envelope",
      (value) => {
        const evidence = value.stages.find(
          ({ id }) => id === "retained-state-plateau",
        ).result.bounded_churn[0].retained;
        evidence.tuples[1] = structuredClone(evidence.tuples[0]);
        rehashTupleEvidence(evidence);
      },
    ],
    [
      "forged chunk with rehashed envelope",
      (value) => {
        const evidence = value.stages.find(
          ({ id }) => id === "retained-state-plateau",
        ).result.replay_pressure[0].after;
        evidence.tuples[0].chunks[0].sha256 = "f".repeat(64);
        rehashTupleEvidence(evidence);
      },
    ],
    [
      "chunk sequence with rehashed envelope",
      (value) => {
        const evidence = value.stages.find(
          ({ id }) => id === "retained-state-plateau",
        ).result.replay_pressure[0].after;
        evidence.tuples[0].chunks[1].seq += 1;
        rehashTupleEvidence(evidence);
      },
    ],
    [
      "live pressure digest",
      (value) => {
        value.stages.find(
          ({ id }) => id === "retained-state-plateau",
        ).result.replay_pressure[0].after.sha256 = "f".repeat(64);
      },
    ],
    [
      "recovered suffix digest",
      (value) => {
        const evidence = value.stages.find(
          ({ id }) => id === "retained-state-plateau",
        ).result.replay_pressure[1].recovered.replay;
        evidence.tuples[0].replay_sha256 = "f".repeat(64);
        rehashTupleEvidence(evidence);
      },
    ],
    [
      "recovered identity swap with rehashed envelope",
      (value) => {
        const evidence = value.stages.find(
          ({ id }) => id === "retained-state-plateau",
        ).result.replay_pressure[1].recovered.replay;
        const first = evidence.tuples[0].run_id;
        evidence.tuples[0].run_id = evidence.tuples[1].run_id;
        evidence.tuples[1].run_id = first;
        rehashTupleEvidence(evidence);
      },
    ],
    [
      "recovered truncation",
      (value) => {
        const evidence = value.stages.find(
          ({ id }) => id === "retained-state-plateau",
        ).result.replay_pressure[1].recovered.replay;
        evidence.tuples[0].truncated = false;
        rehashTupleEvidence(evidence);
      },
    ],
    [
      "resource budget",
      (value) => {
        value.stages.find(
          ({ id }) => id === "resource-census",
        ).result[0].steady.rss_kib = 1_000_000;
      },
    ],
    [
      "recovered resource budget",
      (value) => {
        value.stages.find(
          ({ id }) => id === "retained-state-plateau",
        ).result.replay_pressure[1].recovered.quiescent_thread_delta = -1;
      },
    ],
    [
      "environment",
      (value) => {
        value.environment.logical_cpus = 0;
      },
    ],
    [
      "toolchain",
      (value) => {
        value.provenance.toolchain.node_version = "";
      },
    ],
    [
      "epoch cardinality",
      (value) => {
        value.stages
          .find(({ id }) => id === "retained-state-plateau")
          .result.bounded_churn[0].epochs.push(
            structuredClone(
              value.stages.find(({ id }) => id === "retained-state-plateau")
                .result.bounded_churn[0].epochs[0],
            ),
          );
      },
    ],
  ]) {
    const value = structuredClone(fixture.receipt.value);
    mutate(value);
    assert.notDeepEqual(validate(value), [], label);
  }
});

test("validates a fresh complete observe receipt postcondition", () => {
  const inputs = buildV2Template();
  const receipt = inputs.baselineReceipts[0];
  receipt.value.action_trace.find(
    ({ action }) => action === "provenance.captured",
  ).invocation_nonce = invocationNonce;
  const validate = (notBefore = "2026-08-10T23:59:59.999Z") =>
    validatePassingObservationReceipt({
      receiptPath: receipt.path,
      value: receipt.value,
      snapshot: inputs.sourceSnapshots[0],
      budgets: inputs.budgets,
      policyContracts: inputs.budgets.observation_baseline.policy_contracts,
      notBefore,
      verifiedAt: "2026-08-11T00:00:11.000Z",
      invocationNonce,
    });
  assert.deepEqual(validate(), []);
  assert.ok(
    validate("2026-08-11T00:00:00.001Z").some((error) =>
      error.includes("current invocation"),
    ),
  );
  receipt.value.observation_round = null;
  assert.ok(validate().some((error) => error.includes("observation round")));
});

test("qualification artifact logs are unique regular files inside their owner", () => {
  const temporaryRoot = fs.mkdtempSync(
    path.join(os.tmpdir(), "ctxmux-reliability-artifacts-"),
  );
  const artifactDirectory = path.join(temporaryRoot, "artifacts");
  const receiptPath = path.join(artifactDirectory, "result.json");
  const logPath = path.join(artifactDirectory, "daemon.log");
  const outsidePath = path.join(temporaryRoot, "outside.log");
  fs.mkdirSync(artifactDirectory, { recursive: true });
  fs.writeFileSync(receiptPath, "{}");
  fs.writeFileSync(logPath, "");
  fs.writeFileSync(outsidePath, "");
  const previousDirectory = process.cwd();
  process.chdir(artifactDirectory);
  const validate = (daemonLogs) =>
    validateQualificationArtifacts({
      root: temporaryRoot,
      resolvedReceiptPath: receiptPath,
      value: { daemon_logs: daemonLogs },
    });
  try {
    assert.deepEqual(validate(["artifacts/daemon.log"]), []);
    const receiptIdentity = fs.statSync(receiptPath);
    const serializedReceiptIdentity = {
      dev: String(receiptIdentity.dev),
      ino: String(receiptIdentity.ino),
    };
    const receiptSha256 = crypto
      .createHash("sha256")
      .update(fs.readFileSync(receiptPath))
      .digest("hex");
    assert.deepEqual(
      validateQualificationArtifacts({
        root: temporaryRoot,
        resolvedReceiptPath: receiptPath,
        value: { daemon_logs: ["artifacts/daemon.log"] },
        expectedReceiptIdentity: serializedReceiptIdentity,
        expectedReceiptSha256: receiptSha256,
      }),
      [],
    );
    fs.writeFileSync(receiptPath, '{"mutated":true}');
    assert.ok(
      validateQualificationArtifacts({
        root: temporaryRoot,
        resolvedReceiptPath: receiptPath,
        value: { daemon_logs: ["artifacts/daemon.log"] },
        expectedReceiptIdentity: serializedReceiptIdentity,
        expectedReceiptSha256: receiptSha256,
      }).some((error) => error.includes("receipt is unavailable")),
    );
    fs.writeFileSync(receiptPath, "{}");
    assert.ok(
      validateQualificationArtifacts({
        root: temporaryRoot,
        resolvedReceiptPath: receiptPath,
        value: { daemon_logs: ["artifacts/daemon.log"] },
        expectedReceiptIdentity: {
          dev: String(receiptIdentity.dev),
          ino: String(receiptIdentity.ino + 1),
        },
      }).some((error) => error.includes("receipt is unavailable")),
    );
    assert.ok(
      validateQualificationArtifacts({
        root: temporaryRoot,
        resolvedReceiptPath: receiptPath,
        value: { daemon_logs: ["artifacts/daemon.log"] },
        expectedReceiptIdentity: serializedReceiptIdentity,
        preexistingReceiptIdentity: serializedReceiptIdentity,
      }).some((error) => error.includes("receipt is unavailable")),
    );
    assert.deepEqual(
      validateQualificationArtifacts({
        root: temporaryRoot,
        resolvedReceiptPath: receiptPath,
        value: { daemon_logs: ["artifacts/daemon.log"] },
        expectedReceiptIdentity: serializedReceiptIdentity,
        preexistingReceiptIdentity: {
          dev: serializedReceiptIdentity.dev,
          ino: String(receiptIdentity.ino + 1),
        },
      }),
      [],
    );
    for (const invalidLogs of [
      ["outside.log"],
      ["artifacts"],
      ["artifacts/result.json"],
      ["artifacts/logs/daemon.log"],
      ["artifacts/daemon.log", "artifacts/daemon.log"],
    ]) {
      assert.ok(validate(invalidLogs)[0].includes("unavailable"));
    }
    fs.rmSync(receiptPath);
    fs.symlinkSync(outsidePath, receiptPath);
    assert.ok(
      validate(["artifacts/daemon.log"]).some((error) =>
        error.includes("receipt is unavailable"),
      ),
    );
  } finally {
    process.chdir(previousDirectory);
    fs.rmSync(temporaryRoot, { force: true, recursive: true });
  }
});

test("qualification evidence preparation preserves old bytes and binds one owner", () => {
  const temporaryRoot = fs.mkdtempSync(
    path.join(os.tmpdir(), "ctxmux-reliability-prepare-"),
  );
  const outsideRoot = fs.mkdtempSync(
    path.join(os.tmpdir(), "ctxmux-reliability-outside-"),
  );
  const canonicalDirectory = path.join(
    temporaryRoot,
    "target",
    "reliability",
    "smoke",
  );
  const canonicalPath = path.join(canonicalDirectory, "result.json");
  const userDirectory = path.join(temporaryRoot, "docs");
  const userPath = path.join(userDirectory, "result.json");
  const outsidePath = path.join(outsideRoot, "result.json");
  const previousDirectory = process.cwd();
  const prepare = (requestedPath, profile) => {
    fs.copyFileSync(
      path.join(root, "reliability-gc-contract.json"),
      path.join(temporaryRoot, "reliability-gc-contract.json"),
    );
    fs.mkdirSync(path.join(temporaryRoot, "scripts"), { recursive: true });
    fs.copyFileSync(
      path.join(root, "scripts", "reliability-gc-child.mjs"),
      path.join(temporaryRoot, "scripts", "reliability-gc-child.mjs"),
    );
    if (!fs.existsSync(path.join(temporaryRoot, ".git"))) {
      execFileSync("git", ["init", "--quiet"], { cwd: temporaryRoot });
      execFileSync(
        "git",
        [
          "-c",
          "user.name=ctxmux fixture",
          "-c",
          "user.email=fixture@ctxmux.invalid",
          "add",
          "reliability-gc-contract.json",
          "scripts/reliability-gc-child.mjs",
        ],
        { cwd: temporaryRoot },
      );
      execFileSync(
        "git",
        [
          "-c",
          "user.name=ctxmux fixture",
          "-c",
          "user.email=fixture@ctxmux.invalid",
          "commit",
          "--quiet",
          "-m",
          "fixture",
        ],
        { cwd: temporaryRoot },
      );
    }
    process.chdir(temporaryRoot);
    try {
      return prepareQualificationEvidencePath(
        temporaryRoot,
        requestedPath,
        profile,
      );
    } finally {
      process.chdir(previousDirectory);
    }
  };
  try {
    fs.mkdirSync(canonicalDirectory, { recursive: true });
    fs.writeFileSync(canonicalPath, "stale");
    const beforeMetadata = fs.statSync(canonicalPath);
    const callStartedAt = Date.now();
    const prepared = prepare("target/reliability/smoke/result.json", "smoke");
    const callCompletedAt = Date.now();
    assert.equal(prepared.resolvedEvidencePath, canonicalPath);
    assert.ok(
      Date.parse(prepared.preflight.not_before) >= callStartedAt &&
        Date.parse(prepared.preflight.not_before) <= callCompletedAt,
    );
    assert.equal(fs.readFileSync(canonicalPath, "utf8"), "stale");
    assert.deepEqual(prepared.preflight.preexisting_receipt_identity, {
      dev: String(beforeMetadata.dev),
      ino: String(beforeMetadata.ino),
    });
    assert.deepEqual(
      parseQualificationPreflight(JSON.stringify(prepared.preflight), "smoke"),
      prepared.preflight,
    );
    const gc = loadReliabilityGcContract(temporaryRoot);
    assert.deepEqual(
      prepared.preflight.workload_contract,
      gc.workload_contract,
    );
    assert.deepEqual(prepared.preflight.workload_helper, gc.workload_helper);
    const afterMetadata = fs.statSync(canonicalPath);
    assert.equal(afterMetadata.dev, beforeMetadata.dev);
    assert.equal(afterMetadata.ino, beforeMetadata.ino);
    assert.equal(afterMetadata.mtimeMs, beforeMetadata.mtimeMs);

    const missingReleaseDirectory = path.join(
      temporaryRoot,
      "target",
      "reliability",
      "release",
    );
    const missingRelease = prepare(
      "target/reliability/release/result.json",
      "release",
    );
    assert.equal(fs.existsSync(missingReleaseDirectory), true);
    assert.equal(missingRelease.preflight.preexisting_receipt_identity, null);

    const observePath = path.join(
      temporaryRoot,
      "target",
      "reliability",
      "observe",
      "result.json",
    );
    fs.mkdirSync(path.dirname(observePath), { recursive: true });
    fs.writeFileSync(observePath, "stale-observe");
    prepare("target/reliability/observe/result.json", "observe");
    assert.equal(fs.readFileSync(observePath, "utf8"), "stale-observe");

    fs.mkdirSync(userDirectory);
    fs.writeFileSync(userPath, "user-owned");
    assert.throws(() => prepare("docs/result.json", "smoke"));
    assert.equal(fs.readFileSync(userPath, "utf8"), "user-owned");

    fs.rmSync(canonicalDirectory, { recursive: true });
    fs.writeFileSync(outsidePath, "outside-owned");
    fs.symlinkSync(outsideRoot, canonicalDirectory);
    assert.throws(() => prepare(canonicalPath, "smoke"));
    assert.equal(fs.readFileSync(outsidePath, "utf8"), "outside-owned");
  } finally {
    fs.rmSync(temporaryRoot, { force: true, recursive: true });
    fs.rmSync(outsideRoot, { force: true, recursive: true });
  }
});

test("held artifact owner prevents parent and log symlinks from redirecting writes", () => {
  const temporaryRoot = fs.mkdtempSync(
    path.join(os.tmpdir(), "ctxmux-reliability-owner-"),
  );
  const outsideRoot = fs.mkdtempSync(
    path.join(os.tmpdir(), "ctxmux-reliability-owner-outside-"),
  );
  const canonicalDirectory = path.join(
    temporaryRoot,
    "target",
    "reliability",
    "smoke",
  );
  const heldDirectory = path.join(
    temporaryRoot,
    "target",
    "reliability",
    "smoke-held",
  );
  const outsideReceipt = path.join(outsideRoot, "result.json");
  const outsideLog = path.join(outsideRoot, "outside.log");
  const previousDirectory = process.cwd();
  fs.writeFileSync(outsideReceipt, '{"outside":true}\n');
  fs.writeFileSync(outsideLog, "outside-log\n");
  try {
    process.chdir(temporaryRoot);
    const ownerIdentity = enterCanonicalArtifactOwner({
      root: temporaryRoot,
      profile: "smoke",
      create: true,
    });
    writeOwnedJsonAtomically("result.json", { generation: "before" });
    fs.symlinkSync(outsideLog, "blocked-daemon.log");
    assert.throws(() => openFreshOwnedFile("blocked-daemon.log"));
    assert.equal(fs.readFileSync(outsideLog, "utf8"), "outside-log\n");

    fs.renameSync(canonicalDirectory, heldDirectory);
    fs.symlinkSync(outsideRoot, canonicalDirectory);
    writeOwnedJsonAtomically("result.json", { generation: "after" });
    assert.deepEqual(readOwnedJson("result.json").value, {
      generation: "after",
    });
    assert.equal(fs.readFileSync(outsideReceipt, "utf8"), '{"outside":true}\n');

    process.chdir(temporaryRoot);
    assert.throws(() =>
      enterCanonicalArtifactOwner({
        root: temporaryRoot,
        profile: "smoke",
        expectedIdentity: ownerIdentity,
        create: false,
      }),
    );
    assert.equal(fs.readFileSync(outsideReceipt, "utf8"), '{"outside":true}\n');
  } finally {
    process.chdir(previousDirectory);
    fs.rmSync(temporaryRoot, { force: true, recursive: true });
    fs.rmSync(outsideRoot, { force: true, recursive: true });
  }
});

test("qualification worker inherits the held artifact owner across path replacement", async () => {
  const temporaryRoot = fs.mkdtempSync(
    path.join(os.tmpdir(), "ctxmux-reliability-worker-owner-"),
  );
  const outsideRoot = fs.mkdtempSync(
    path.join(os.tmpdir(), "ctxmux-reliability-worker-outside-"),
  );
  const canonicalDirectory = path.join(
    temporaryRoot,
    "target",
    "reliability",
    "smoke",
  );
  const heldDirectory = `${canonicalDirectory}-held`;
  const outsideReceipt = path.join(outsideRoot, "result.json");
  const previousDirectory = process.cwd();
  let child;
  fs.writeFileSync(outsideReceipt, '{"outside":true}\n');
  try {
    process.chdir(temporaryRoot);
    const ownerIdentity = enterCanonicalArtifactOwner({
      root: temporaryRoot,
      profile: "smoke",
      create: true,
    });
    const moduleUrl = pathToFileURL(
      path.join(root, "scripts", "reliability-artifact-owner.mts"),
    ).href;
    child = spawn(
      process.execPath,
      [
        "--input-type=module",
        "--eval",
        `import { assertInheritedArtifactOwner, writeOwnedJsonAtomically } from ${JSON.stringify(moduleUrl)};
         assertInheritedArtifactOwner(JSON.parse(process.env.CTXMUX_TEST_OWNER));
         process.stdout.write("READY\\n");
         process.stdin.once("data", () => {
           writeOwnedJsonAtomically("result.json", { worker: "held" });
           process.stdout.write("DONE\\n", () => process.exit(0));
         });`,
      ],
      {
        env: {
          ...process.env,
          CTXMUX_TEST_OWNER: JSON.stringify(ownerIdentity),
        },
        stdio: ["pipe", "pipe", "pipe"],
      },
    );
    const [ready] = await once(child.stdout, "data");
    assert.equal(ready.toString(), "READY\n");
    fs.renameSync(canonicalDirectory, heldDirectory);
    fs.symlinkSync(outsideRoot, canonicalDirectory);
    const exited = once(child, "exit");
    child.stdin.write("publish\n");
    const [done] = await once(child.stdout, "data");
    assert.equal(done.toString(), "DONE\n");
    const [status] = await exited;
    assert.equal(status, 0);
    assert.deepEqual(
      JSON.parse(fs.readFileSync(path.join(heldDirectory, "result.json"))),
      { worker: "held" },
    );
    assert.equal(fs.readFileSync(outsideReceipt, "utf8"), '{"outside":true}\n');
  } finally {
    if (child?.exitCode === null) child.kill("SIGKILL");
    process.chdir(previousDirectory);
    fs.rmSync(temporaryRoot, { force: true, recursive: true });
    fs.rmSync(outsideRoot, { force: true, recursive: true });
  }
});

test("derives every pre-registered ceiling without floating-point drift", () => {
  assert.equal(deriveBudgetCeiling("cpu_core_percent", 0), 5);
  assert.equal(deriveBudgetCeiling("cpu_core_percent", 10), 15);
  assert.equal(deriveBudgetCeiling("cpu_core_percent", 10.001), 20);
  assert.equal(deriveBudgetCeiling("peak_rss_kib", 0), 8192);
  assert.equal(deriveBudgetCeiling("peak_rss_kib", 8192), 12288);
  assert.equal(deriveBudgetCeiling("retained_output_bytes_per_run", 1), 4096);
  assert.equal(
    deriveBudgetCeiling("retained_output_bytes_per_run", 4096),
    8192,
  );
  assert.equal(deriveBudgetCeiling("rss_kib_per_run", 0.1), 256);
  assert.equal(deriveBudgetCeiling("threads_per_run", 0), 0.25);
  assert.equal(deriveBudgetCeiling("threads_per_run", 1.005), 1.5);
  assert.equal(deriveBudgetCeiling("fds_per_run", 1), 1.25);
  assert.equal(deriveBudgetCeiling("cleanup_threads_delta", 0), 1);
  assert.equal(deriveBudgetCeiling("cleanup_threads_delta", 1), 2);
  assert.equal(deriveBudgetCeiling("cleanup_live_children", 0.1), 1);
  assert.equal(deriveBudgetCeiling("cleanup_attachments", 0), 0);
});

test("maps all three raw round cells to the ten governed maxima", () => {
  const maxima = deriveObservedMaxima(
    [1, 2, 3].map((round) => resourceCell("active", 1, round)),
  );
  assert.deepEqual(maxima, {
    cpu_core_percent: 13,
    peak_rss_kib: 1203,
    steady_rss_kib: 1103,
    retained_output_bytes_per_run: 4099,
    rss_kib_per_run: 103,
    threads_per_run: 1,
    fds_per_run: 2,
    cleanup_threads_delta: 3,
    cleanup_live_children: 0,
    cleanup_attachments: 0,
  });
});

test("rejects an unfrozen or incomplete high-Run-count budget", () => {
  const inputs = actualInputs();
  inputs.budgets.frozen_before_optimization = false;
  delete inputs.budgets.budgets.active["128"];
  const errors = validateReliabilityPolicy(inputs);
  assert.ok(errors.some((error) => error.includes("before optimization")));
  assert.ok(errors.some((error) => error.includes("1/32/128")));
});

test("rejects a present but malformed budget cell", () => {
  const inputs = actualInputs();
  inputs.budgets.budgets.active["128"] = null;
  const errors = validateReliabilityPolicy(inputs);
  assert.ok(errors.some((error) => error.includes("budget must be an object")));
});

test("rejects changed or misreported source-bound v2 evidence", () => {
  const inputs = actualInputs();
  inputs.baselineReceipts[0].sha256 = "0".repeat(64);
  inputs.budgets.observation_baseline.observed_maxima.active[
    "128"
  ].peak_rss_kib = 1;
  const errors = validateReliabilityPolicy(inputs);
  assert.ok(errors.some((error) => error.includes("hash drifted")));
  assert.ok(errors.some((error) => error.includes("raw maximum")));
});

test("rejects unreachable smoke, release, and qualification profile policy", () => {
  const inputs = actualInputs();
  inputs.checkScript = inputs.checkScript.replace(
    "scripts/check-reliability.sh --profile smoke",
    "true",
  );
  inputs.workflow = inputs.workflow.replace(
    "scripts/check-reliability.sh --profile release",
    "true",
  );
  inputs.harnessSource = inputs.harnessSource
    .replace('"soak_seconds": 7200', '"soak_seconds": 0')
    .concat('\n// decoy only: "soak_seconds": 7200\n');
  const errors = validateReliabilityPolicy(inputs);
  assert.ok(errors.some((error) => error.includes("reliability smoke")));
  assert.ok(errors.some((error) => error.includes("release-soak")));
  assert.ok(errors.some((error) => error.includes("profile contract")));
});

test("requires one executable smoke command at the end of the required check", () => {
  const inputs = actualInputs();
  const smokeCommand = "scripts/check-reliability.sh --profile smoke";
  const coreCompletion = `printf '%s\\n' "$ctxmux_check_completion_nonce" > "$ctxmux_check_completion_marker"`;
  const mutations = [
    (source) => {
      const coreStart = source.indexOf("ctxmux_check_core() (");
      const coreEnd = source.indexOf(coreCompletion, coreStart);
      assert.ok(coreStart >= 0 && coreEnd > coreStart);
      return `${source.slice(0, coreStart + "ctxmux_check_core() (".length)}\n${source.slice(coreEnd)}`;
    },
    (source) =>
      source.replace(
        "set -euo pipefail",
        "set -euo pipefail\nexec /usr/bin/true",
      ),
    (source) => source.replace(smokeCommand, `# ${smokeCommand}`),
    (source) => source.replace(smokeCommand, `${smokeCommand} || true`),
    (source) =>
      source.replace(
        "trap ctxmux_check_completion_guard EXIT",
        "# trap ctxmux_check_completion_guard EXIT",
      ),
    (source) =>
      source.replace(
        `${smokeCommand}\nctxmux_check_completed=true`,
        `ctxmux_check_completed=true\n${smokeCommand}`,
      ),
    (source) =>
      source
        .replace(`${coreCompletion}\n)`, ")")
        .replace(
          "ctxmux_check_core() (\nset -euo pipefail\ntrap - EXIT",
          `ctxmux_check_core() (\nset -euo pipefail\ntrap - EXIT\n${coreCompletion}`,
        ),
    (source) =>
      source.replace(
        `${coreCompletion}\n)\n\nctxmux_check_state_dir=`,
        `${coreCompletion}\n)\nctxmux_check_core() { printf '%s\\n' "$ctxmux_check_completion_nonce" > "$ctxmux_check_completion_marker"; }\nfunction scripts/check-reliability.sh { return 0; }\nctxmux_check_state_dir=`,
      ),
  ];
  for (const mutate of mutations) {
    const errors = validateReliabilityPolicy({
      ...inputs,
      checkScript: mutate(inputs.checkScript),
    });
    assert.ok(
      errors.some((error) =>
        error.includes("does not reach reliability smoke"),
      ),
    );
  }
});

test("required check supervisor rejects early exit and exec from its core", () => {
  const temporaryRoot = fs.mkdtempSync(
    path.join(os.tmpdir(), "ctxmux-check-completion-"),
  );
  const scriptsDirectory = path.join(temporaryRoot, "scripts");
  const checkPath = path.join(scriptsDirectory, "check.sh");
  fs.mkdirSync(scriptsDirectory);
  try {
    for (const { earlyControlTransfer, expectedStatus, expectedError } of [
      {
        earlyControlTransfer: "exit 0",
        expectedStatus: 1,
        expectedError: /did not publish its completion token/u,
      },
      {
        earlyControlTransfer: "exec /usr/bin/true",
        expectedStatus: 1,
        expectedError: /did not publish its completion token/u,
      },
      {
        earlyControlTransfer: "exit 73",
        expectedStatus: 73,
        expectedError: /did not reach its completion boundary/u,
      },
    ]) {
      fs.writeFileSync(
        checkPath,
        fs
          .readFileSync(path.join(root, "scripts", "check.sh"), "utf8")
          .replace(
            "ctxmux_check_core() (\nset -euo pipefail\ntrap - EXIT",
            `ctxmux_check_core() (\nset -euo pipefail\ntrap - EXIT\n${earlyControlTransfer}`,
          ),
        { mode: 0o755 },
      );
      const result = spawnSync(
        "/bin/bash",
        ["--noprofile", "--norc", checkPath],
        {
          cwd: temporaryRoot,
          encoding: "utf8",
          timeout: 5_000,
        },
      );
      assert.equal(result.signal, null, earlyControlTransfer);
      assert.equal(result.status, expectedStatus, earlyControlTransfer);
      assert.match(result.stderr, expectedError, earlyControlTransfer);
    }
    fs.copyFileSync(path.join(root, "scripts", "check.sh"), checkPath);
    fs.chmodSync(checkPath, 0o755);
    const invalidUsage = spawnSync(
      "/bin/bash",
      ["--noprofile", "--norc", checkPath, "--unexpected"],
      {
        cwd: temporaryRoot,
        encoding: "utf8",
        timeout: 5_000,
      },
    );
    assert.equal(invalidUsage.signal, null);
    assert.equal(invalidUsage.status, 2);
    assert.match(invalidUsage.stderr, /usage: scripts\/check\.sh/u);
  } finally {
    fs.rmSync(temporaryRoot, { force: true, recursive: true });
  }
});

test("rejects shallow or bypassed source-bound workflow qualification", () => {
  const inputs = actualInputs();
  const mutations = [
    ["shallow", "          fetch-depth: 1"],
    ["comment-only", "          # fetch-depth: 0"],
    ["inline decoy", "          fetch-depth: 1 # fetch-depth: 0"],
    [
      "relocated token",
      "          fetch-depth: 1\n        name: fetch-depth: 0",
    ],
  ];
  for (const [label, replacement] of mutations) {
    const errors = validateReliabilityPolicy({
      ...inputs,
      workflow: inputs.workflow.replaceAll(
        "          fetch-depth: 0",
        replacement,
      ),
    });
    assert.ok(
      errors.some((error) => error.includes("reliability-nightly")),
      label,
    );
    assert.ok(
      errors.some((error) => error.includes("release-soak")),
      label,
    );
  }

  const checkout = `      - uses: actions/checkout@v4
        with:
          fetch-depth: 0`;
  const structuralMutations = [
    [
      "extra checkout input",
      (source) =>
        source.replaceAll(
          "          fetch-depth: 0",
          "          fetch-depth: 0\n          ref: refs/heads/main",
        ),
    ],
    [
      "conditional checkout",
      (source) =>
        source.replaceAll(
          checkout,
          `      - if: false
        uses: actions/checkout@v4
        with:
          fetch-depth: 0`,
        ),
    ],
    [
      "multiple checkouts",
      (source) => source.replaceAll(checkout, `${checkout}\n${checkout}`),
    ],
    [
      "checkout after qualification",
      (source) => {
        let mutated = source.replaceAll(`${checkout}\n\n`, "");
        for (const command of [
          "scripts/check-reliability.sh --profile nightly",
          "scripts/check-reliability.sh --profile release",
        ]) {
          mutated = mutated.replace(
            `        run: ${command}`,
            `        run: ${command}\n${checkout}`,
          );
        }
        return mutated;
      },
    ],
    [
      "conditional qualification",
      (source) =>
        source.replaceAll(
          "        run: scripts/check-reliability.sh --profile",
          "        if: false\n        run: scripts/check-reliability.sh --profile",
        ),
    ],
    [
      "custom qualification shell",
      (source) =>
        source
          .replaceAll(
            "        run: scripts/check-reliability.sh --profile nightly",
            "        run: scripts/check-reliability.sh --profile nightly\n        shell: /bin/true {0}",
          )
          .replaceAll(
            "        run: scripts/check-reliability.sh --profile release",
            "        run: scripts/check-reliability.sh --profile release\n        shell: /bin/true {0}",
          ),
    ],
    [
      "workflow default shell",
      (source) =>
        source.replace(
          "jobs:\n",
          "defaults:\n  run:\n    shell: /bin/true {0}\njobs:\n",
        ),
    ],
    [
      "job default shell",
      (source) =>
        source
          .replace(
            "  reliability-nightly:\n",
            "  reliability-nightly:\n    defaults:\n      run:\n        shell: /bin/true {0}\n",
          )
          .replace(
            "  release-soak:\n",
            "  release-soak:\n    defaults:\n      run:\n        shell: /bin/true {0}\n",
          ),
    ],
    [
      "commented lane condition",
      (source) =>
        source
          .replace(
            "if: github.event_name == 'schedule' || inputs.qualification == 'nightly'",
            "if: false # github.event_name == 'schedule' || inputs.qualification == 'nightly'",
          )
          .replace(
            "if: github.event_name == 'workflow_dispatch' && inputs.qualification == 'release'",
            "if: false # github.event_name == 'workflow_dispatch' && inputs.qualification == 'release'",
          ),
    ],
    [
      "job environment injection",
      (source) =>
        source
          .replace(
            "      CTXMUX_RELIABILITY_EVIDENCE: ${{ github.workspace }}/target/reliability/nightly/result.json",
            "      CTXMUX_RELIABILITY_EVIDENCE: ${{ github.workspace }}/target/reliability/nightly/result.json\n      PATH: /tmp/decoy",
          )
          .replace(
            "      CTXMUX_RELIABILITY_EVIDENCE: ${{ github.workspace }}/target/reliability/release/result.json",
            "      CTXMUX_RELIABILITY_EVIDENCE: ${{ github.workspace }}/target/reliability/release/result.json\n      NODE_OPTIONS: --require=/tmp/decoy",
          ),
    ],
    [
      "workflow environment injection",
      (source) =>
        source.replace("jobs:\n", "env:\n  BASH_ENV: /tmp/decoy\njobs:\n"),
    ],
    [
      "preceding GITHUB_ENV injection",
      (source) =>
        source.replaceAll(
          "      - name: Run ",
          '      - name: Poison inherited environment\n        run: echo "BASH_ENV=/tmp/ctxmux-startup-poison" >> "$GITHUB_ENV"\n\n      - name: Run ',
        ),
    ],
    [
      "preceding GITHUB_PATH injection",
      (source) =>
        source.replaceAll(
          "      - name: Run ",
          '      - name: Poison inherited path\n        run: echo "/tmp/ctxmux-fake-bin" >> "$GITHUB_PATH"\n\n      - name: Run ',
        ),
    ],
    [
      "dependency install scripts enabled",
      (source) => source.replaceAll("npm ci --ignore-scripts", "npm ci"),
    ],
    [
      "missing receipts tolerated",
      (source) =>
        source.replaceAll(
          "if-no-files-found: error",
          "if-no-files-found: warn",
        ),
    ],
  ];
  for (const [label, mutate] of structuralMutations) {
    const errors = validateReliabilityPolicy({
      ...inputs,
      workflow: mutate(inputs.workflow),
    });
    assert.ok(
      errors.some((error) => error.includes("reliability-nightly")),
      label,
    );
    assert.ok(
      errors.some((error) => error.includes("release-soak")),
      label,
    );
  }
});

test("rejects commented or altered reliability triggers", () => {
  const inputs = actualInputs();
  const mutations = [
    [
      "comment-only schedule",
      (source) =>
        source.replace(
          '  schedule:\n    - cron: "17 3 * * *"',
          '  # schedule:\n  #   - cron: "17 3 * * *"',
        ),
    ],
    [
      "comment-only dispatch input",
      (source) => {
        const start = source.indexOf("  workflow_dispatch:\n");
        const end = source.indexOf("\n\npermissions:", start);
        assert.ok(start >= 0 && end > start);
        return `${source.slice(0, start)}  workflow_dispatch: {} # inputs.qualification == 'release'${source.slice(end)}`;
      },
    ],
    [
      "altered dispatch options",
      (source) =>
        source.replace(
          "          - release",
          "          - staging # - release",
        ),
    ],
  ];
  for (const [label, mutate] of mutations) {
    const errors = validateReliabilityPolicy({
      ...inputs,
      workflow: mutate(inputs.workflow),
    });
    assert.ok(
      errors.some((error) => error.includes("canonical schedule and dispatch")),
      label,
    );
  }
});

test("requires an executable policy check before the qualification build", () => {
  const policyLine = "node scripts/reliability-policy.mjs";
  const buildLine = '"${ctxmux_reliability_build_argv[@]}"';
  const mutations = [
    (source) => source.replace(policyLine, "true"),
    (source) => source.replace(policyLine, `# ${policyLine}`),
    (source) =>
      source
        .replace(`${policyLine}\n`, "")
        .replace(buildLine, `${buildLine}\n${policyLine}`),
    (source) => source.replace(buildLine, `${buildLine}\nexit 0`),
  ];
  for (const mutate of mutations) {
    const inputs = actualInputs();
    inputs.qualificationScript = mutate(inputs.qualificationScript);
    const errors = validateReliabilityPolicy(inputs);
    assert.ok(
      errors.some(
        (error) =>
          error.includes("before its locked build") ||
          error.includes("complete launcher envelope"),
      ),
    );
  }
});

test("qualification gates every profile through final harness dispatch", () => {
  const temporaryRoot = fs.mkdtempSync(
    path.join(os.tmpdir(), "ctxmux-reliability-policy-"),
  );
  const sentinelLog = path.join(temporaryRoot, "admission.log");
  const stubDirectory = path.join(temporaryRoot, "bin");
  const launcherDirectory = path.join(temporaryRoot, "scripts");
  const launcherPath = path.join(launcherDirectory, "check-reliability.sh");
  fs.mkdirSync(stubDirectory);
  fs.mkdirSync(launcherDirectory);
  fs.copyFileSync(
    path.join(root, "scripts", "check-reliability.sh"),
    launcherPath,
  );
  fs.chmodSync(launcherPath, 0o755);
  const preflightToken = (profile) =>
    JSON.stringify({
      schema: "ctxmux.reliability-preflight.v2",
      profile,
      not_before: "2026-08-11T00:00:00.000Z",
      invocation_nonce: "ab".repeat(32),
      artifact_owner_identity: { dev: "1", ino: "2" },
      preexisting_receipt_identity: null,
    });
  // Keep the real Bash launcher boundary while avoiding roughly 95 Node cold
  // starts in this one policy-envelope fixture. NUL framing preserves argv
  // exactly without making the stub responsible for JSON escaping.
  const stubSource = String.raw`#!/bin/bash
set -euo pipefail

command=\${0##*/}
args=("$@")
{
  printf '%s\0%s\0' "$command" "$#"
  printf '%s\0' "$@"
} >> "$CTXMUX_SENTINEL_LOG"

valid_profile() {
  case "$1" in
    smoke|nightly|release|observe) return 0 ;;
    *) return 1 ;;
  esac
}

preflight_token() {
  printf '{"schema":"ctxmux.reliability-preflight.v2","profile":"%s","not_before":"2026-08-11T00:00:00.000Z","invocation_nonce":"%s","artifact_owner_identity":{"dev":"1","ino":"2"},"preexisting_receipt_identity":null}' "$1" "$(printf 'ab%.0s' {1..32})"
}

absolute_path() {
  case "$1" in
    /*) printf '%s' "$1" ;;
    *) printf '%s/%s' "$CTXMUX_SENTINEL_ROOT" "$1" ;;
  esac
}

if [[ \${CTXMUX_SENTINEL_PHASE:-} == failure ]]; then
  if [[ $command == node && $# -eq 1 && \${args[0]} == scripts/reliability-policy.mjs ]]; then
    exit 73
  fi
  exit 99
fi

if [[ $command == node && $# -eq 1 && \${args[0]} == scripts/reliability-policy.mjs ]]; then
  exit 0
fi

if [[ $command == node && $# -eq 5 && \${args[0]} == scripts/reliability-policy.mjs && \${args[1]} == --prepare-qualification-evidence && \${args[3]} == --profile ]] &&
  valid_profile "\${args[4]}" &&
  [[ $(absolute_path "\${args[2]}") == $(absolute_path "target/reliability/\${args[4]}/result.json") ]]; then
  preflight_token "\${args[4]}"
  printf '\n'
  exit 0
fi

if [[ \${CTXMUX_SENTINEL_PHASE:-} == qualification && $command == node && $# -eq 7 && \${args[0]} == scripts/reliability-policy.mjs && \${args[1]} == --qualification-receipt && \${args[3]} == --profile && \${args[5]} == --preflight ]] &&
  valid_profile "\${args[4]}"; then
  expected_evidence=\${CTXMUX_RELIABILITY_EVIDENCE:-\${CTXMUX_RELIABILITY_ARTIFACT_DIR:-target/reliability/\${args[4]}}/result.json}
  if [[ \${args[2]} == "$expected_evidence" && \${args[6]} == "$(preflight_token "\${args[4]}")" ]]; then
    exit 83
  fi
fi

if [[ $command == git && $# -eq 2 && \${args[0]} == rev-parse && \${args[1]} == HEAD ]]; then
  printf '1111111111111111111111111111111111111111\n'
  exit 0
fi
if [[ $command == git && $# -eq 2 && \${args[0]} == rev-parse && \${args[1]} == 'HEAD^{tree}' ]]; then
  printf '2222222222222222222222222222222222222222\n'
  exit 0
fi
if [[ $command == git && $# -eq 3 && \${args[0]} == status && \${args[1]} == --porcelain=v1 && \${args[2]} == --untracked-files=all ]]; then
  exit 0
fi

if [[ $command == cargo && $# -eq 7 && \${args[0]} == build && \${args[1]} == --locked && \${args[2]} == --quiet && \${args[3]} == --package && \${args[4]} == ctxmux-daemon && \${args[5]} == --target-dir && \${args[6]} == target/reliability/provenance-build ]]; then
  if [[ \${CTXMUX_SENTINEL_PHASE:-} == build ]]; then
    exit 79
  fi
  if [[ \${CTXMUX_SENTINEL_PHASE:-} == qualification ]]; then
    daemon=target/reliability/provenance-build/debug/ctxmuxd
    mkdir -p "\${daemon%/*}"
    : > "$daemon"
    chmod 755 "$daemon"
    exit 0
  fi
  exit 97
fi

if [[ \${CTXMUX_SENTINEL_PHASE:-} == qualification ]]; then
  if [[ $command == cargo && \${args[0]:-} == test ]]; then
    exit 0
  fi
  if [[ $command == node && $# -eq 4 && \${args[0]} == --import && \${args[1]} == tsx && \${args[2]} == --test && \${args[3]} == packages/sdk/test/wrong-cases.test.ts ]]; then
    exit 0
  fi
  if [[ $command == node && \${args[0]:-} == -e ]]; then
    printf '["cargo","build","--locked","--quiet","--package","ctxmux-daemon","--target-dir","target/reliability/provenance-build"]'
    exit 0
  fi
  if [[ $command == node && $# -eq 5 && \${args[0]} == --import && \${args[1]} == tsx && \${args[2]} == scripts/reliability-qualification.ts && \${args[3]} == --profile ]] &&
    valid_profile "\${args[4]}" &&
    [[ \${CTXMUX_RELIABILITY_PREFLIGHT:-} == "$(preflight_token "\${args[4]}")" ]]; then
    exit 0
  fi
fi
exit 98
`.replaceAll("\\${", "${");
  for (const command of ["node", "git", "cargo"]) {
    fs.writeFileSync(path.join(stubDirectory, command), stubSource, {
      mode: 0o755,
    });
  }
  const run = (
    profile,
    phase,
    selectedLauncher = launcherPath,
    environment = {},
  ) =>
    spawnSync(
      "/bin/bash",
      ["--noprofile", "--norc", selectedLauncher, "--profile", profile],
      {
        cwd: temporaryRoot,
        encoding: "utf8",
        env: {
          BASH_ENV: "/dev/null",
          CTXMUX_SENTINEL_LOG: sentinelLog,
          CTXMUX_SENTINEL_PHASE: phase,
          CTXMUX_SENTINEL_ROOT: fs.realpathSync(temporaryRoot),
          ENV: "/dev/null",
          PATH: `${stubDirectory}${path.delimiter}${process.env.PATH ?? ""}`,
          ...environment,
        },
        timeout: 5_000,
      },
    );
  const readInvocations = () => {
    const fields = fs.readFileSync(sentinelLog, "utf8").split("\0");
    assert.equal(fields.pop(), "", "sentinel log ends at a record boundary");
    const invocations = [];
    for (let index = 0; index < fields.length;) {
      const command = fields[index++];
      const count = Number.parseInt(fields[index++], 10);
      assert.ok(Number.isSafeInteger(count) && count >= 0);
      invocations.push({ command, args: fields.slice(index, index + count) });
      index += count;
    }
    return invocations;
  };
  const profiles = ["smoke", "nightly", "release", "observe"];
  const policyInvocation = {
    command: "node",
    args: ["scripts/reliability-policy.mjs"],
  };
  const buildInvocation = {
    command: "cargo",
    args: [
      "build",
      "--locked",
      "--quiet",
      "--package",
      "ctxmux-daemon",
      "--target-dir",
      "target/reliability/provenance-build",
    ],
  };
  const buildInvocations = (
    profile,
    evidencePath = `target/reliability/${profile}/result.json`,
  ) => [
    policyInvocation,
    {
      command: "node",
      args: [
        "scripts/reliability-policy.mjs",
        "--prepare-qualification-evidence",
        evidencePath,
        "--profile",
        profile,
      ],
    },
    { command: "git", args: ["rev-parse", "HEAD"] },
    { command: "git", args: ["rev-parse", "HEAD^{tree}"] },
    {
      command: "git",
      args: ["status", "--porcelain=v1", "--untracked-files=all"],
    },
    buildInvocation,
  ];
  const qualificationInvocations = (
    profile,
    evidencePath = `target/reliability/${profile}/result.json`,
  ) => [
    ...buildInvocations(profile, evidencePath),
    {
      command: "cargo",
      args: [
        "test",
        "--locked",
        "--quiet",
        "--package",
        "ctxmux-daemon",
        "socket_path",
      ],
    },
    {
      command: "cargo",
      args: [
        "test",
        "--locked",
        "--quiet",
        "--package",
        "ctxmux-daemon",
        "stop_after_wait_disables_signalling_before_state_publication",
      ],
    },
    {
      command: "cargo",
      args: [
        "test",
        "--locked",
        "--quiet",
        "--package",
        "ctxmux-daemon",
        "--test",
        "native_lifecycle",
        "protocol_frame_ceiling_and_duplicate_names_fail_before_run_mutation",
      ],
    },
    {
      command: "node",
      args: [
        "--import",
        "tsx",
        "--test",
        "packages/sdk/test/wrong-cases.test.ts",
      ],
    },
    {
      command: "node",
      args: [
        "-e",
        "process.stdout.write(JSON.stringify(process.argv.slice(1)))",
        buildInvocation.command,
        ...buildInvocation.args,
      ],
    },
    {
      command: "node",
      args: [
        "--import",
        "tsx",
        "scripts/reliability-qualification.ts",
        "--profile",
        profile,
      ],
    },
    {
      command: "node",
      args: [
        "scripts/reliability-policy.mjs",
        "--qualification-receipt",
        evidencePath,
        "--profile",
        profile,
        "--preflight",
        preflightToken(profile),
      ],
    },
  ];

  try {
    for (const profile of profiles) {
      fs.rmSync(sentinelLog, { force: true });
      const result = run(profile, "failure");
      assert.equal(result.signal, null, profile);
      assert.equal(result.status, 73, `${profile}: ${result.stderr}`);
      assert.deepEqual(readInvocations(), [policyInvocation], profile);
    }

    for (const profile of profiles) {
      fs.rmSync(sentinelLog, { force: true });
      const result = run(profile, "build");
      assert.equal(result.signal, null, profile);
      assert.equal(result.status, 79, `${profile}: ${result.stderr}`);
      assert.deepEqual(readInvocations(), buildInvocations(profile), profile);
    }

    for (const profile of profiles) {
      fs.rmSync(sentinelLog, { force: true });
      const result = run(profile, "qualification");
      assert.equal(result.signal, null, profile);
      assert.equal(result.status, 83, `${profile}: ${result.stderr}`);
      assert.deepEqual(
        readInvocations(),
        qualificationInvocations(profile),
        profile,
      );
    }

    const absoluteCanonicalArtifactDirectory = path.join(
      fs.realpathSync(temporaryRoot),
      "target",
      "reliability",
      "smoke",
    );
    fs.rmSync(sentinelLog, { force: true });
    const absoluteCanonicalResult = run(
      "smoke",
      "qualification",
      launcherPath,
      {
        CTXMUX_RELIABILITY_ARTIFACT_DIR: absoluteCanonicalArtifactDirectory,
      },
    );
    assert.equal(absoluteCanonicalResult.signal, null);
    assert.equal(
      absoluteCanonicalResult.status,
      83,
      `${absoluteCanonicalResult.stderr}\n${fs.readFileSync(sentinelLog, "utf8")}`,
    );
    assert.deepEqual(
      readInvocations(),
      qualificationInvocations(
        "smoke",
        path.join(absoluteCanonicalArtifactDirectory, "result.json"),
      ),
    );

    const unsafeArtifactDirectory = path.join(
      temporaryRoot,
      "custom-artifacts",
    );
    const unsafeEvidencePath = path.join(
      unsafeArtifactDirectory,
      "result.json",
    );
    fs.rmSync(sentinelLog, { force: true });
    const unsafeArtifactResult = run("smoke", "qualification", launcherPath, {
      CTXMUX_RELIABILITY_ARTIFACT_DIR: unsafeArtifactDirectory,
    });
    assert.equal(unsafeArtifactResult.signal, null);
    assert.equal(unsafeArtifactResult.status, 98);
    assert.deepEqual(readInvocations(), [
      policyInvocation,
      {
        command: "node",
        args: [
          "scripts/reliability-policy.mjs",
          "--prepare-qualification-evidence",
          unsafeEvidencePath,
          "--profile",
          "smoke",
        ],
      },
    ]);

    const earlyExitLauncher = path.join(
      launcherDirectory,
      "check-reliability-early-exit.sh",
    );
    fs.writeFileSync(
      earlyExitLauncher,
      fs
        .readFileSync(launcherPath, "utf8")
        .replace(
          '"${ctxmux_reliability_build_argv[@]}"',
          '"${ctxmux_reliability_build_argv[@]}"\nexit 0',
        ),
      { mode: 0o755 },
    );
    const earlyExitInputs = actualInputs();
    earlyExitInputs.qualificationScript = fs.readFileSync(
      earlyExitLauncher,
      "utf8",
    );
    assert.ok(
      validateReliabilityPolicy(earlyExitInputs).some((error) =>
        error.includes("complete launcher envelope"),
      ),
    );
  } finally {
    fs.rmSync(temporaryRoot, { force: true, recursive: true });
  }
});

test("rejects mixed v1/v2 baseline receipts", () => {
  const inputs = actualInputs();
  inputs.baselineReceipts[0].value.schema =
    "ctxmux.reliability-qualification.v1";
  const errors = validateReliabilityPolicy(inputs);
  assert.ok(errors.some((error) => error.includes("must not mix")));
});

test("rejects an all-v1 observation baseline", () => {
  const inputs = actualInputs();
  for (const receipt of inputs.baselineReceipts) {
    receipt.value.schema = "ctxmux.reliability-qualification.v1";
  }
  const errors = validateReliabilityPolicy(inputs);
  assert.ok(errors.some((error) => error.includes("requires source-bound v2")));
});

test("rejects an unknown observation receipt generation", () => {
  const inputs = actualInputs();
  for (const receipt of inputs.baselineReceipts) {
    receipt.value.schema = "ctxmux.reliability-qualification.v3";
  }
  const errors = validateReliabilityPolicy(inputs);
  assert.ok(errors.some((error) => error.includes("requires source-bound v2")));
});

test("rejects missing, dirty, unreachable, and hash-drifted v2 provenance", async (t) => {
  await t.test("missing", () => {
    const errors = errorsFor(({ baselineReceipts }) => {
      delete baselineReceipts[0].value.provenance.daemon;
    });
    assert.ok(errors.some((error) => error.includes("provenance")));
  });
  await t.test("dirty", () => {
    const errors = errorsFor(({ baselineReceipts }) => {
      baselineReceipts[0].value.provenance.source.worktree.clean = false;
      baselineReceipts[0].value.provenance.source.worktree.entries = [
        " M scripts/reliability-qualification.ts",
      ];
    });
    assert.ok(errors.some((error) => error.includes("worktree must be clean")));
  });
  await t.test("unreachable", () => {
    const errors = errorsFor(({ sourceSnapshots }) => {
      sourceSnapshots[0].reachableFromHead = false;
    });
    assert.ok(errors.some((error) => error.includes("current-HEAD ancestor")));
  });
  await t.test("receipt hash drift", () => {
    const errors = errorsFor(({ baselineReceipts }) => {
      baselineReceipts[0].sha256 = "f".repeat(64);
    });
    assert.ok(errors.some((error) => error.includes("hash drifted")));
  });
  await t.test("Git blob hash drift", () => {
    const errors = errorsFor(({ baselineReceipts }) => {
      baselineReceipts[0].value.provenance.harness.sha256 = "f".repeat(64);
    });
    assert.ok(
      errors.some((error) =>
        error.includes(
          "source hash drifted for scripts/reliability-qualification.ts",
        ),
      ),
    );
  });
  await t.test("derivation policy drift", () => {
    const errors = errorsFor((inputs) => {
      inputs.currentPolicyHashes[POLICY_SOURCE_PATHS[0]] = "f".repeat(64);
    });
    assert.ok(errors.some((error) => error.includes("budget contract")));
  });
});

test("rejects mixed source, harness, binary, toolchain, host, seed, workload, and measurement identity", async (t) => {
  const mutations = [
    [
      "source",
      (receipt) => (receipt.provenance.source.commit = "a".repeat(40)),
    ],
    [
      "harness",
      (receipt) => (receipt.provenance.harness.sha256 = "a".repeat(64)),
    ],
    [
      "binary",
      (receipt) => (receipt.provenance.daemon.sha256 = "a".repeat(64)),
    ],
    [
      "lockfile",
      (receipt) => (receipt.provenance.lockfiles[0].sha256 = "a".repeat(64)),
    ],
    [
      "toolchain",
      (receipt) => (receipt.provenance.toolchain.cargo_version = "other"),
    ],
    ["host", (receipt) => (receipt.environment.cpu_model = "other")],
    ["seed", (receipt) => (receipt.seed += 1)],
    ["workload", (receipt) => (receipt.declared_limits.note = "other")],
    [
      "measurement",
      (receipt) =>
        (receipt.provenance.measurement_contract_sha256 = "a".repeat(64)),
    ],
  ];
  for (const [label, mutate] of mutations) {
    await t.test(label, () => {
      const errors = errorsFor(({ baselineReceipts }) => {
        mutate(baselineReceipts[1].value);
      });
      assert.ok(errors.length > 0, `${label} mutation passed`);
      assert.ok(
        errors.some(
          (error) =>
            error.includes("must share") ||
            error.includes("hash drifted") ||
            error.includes("ancestor"),
        ),
      );
    });
  }
});

test("rejects duplicate rounds, duplicate receipts, paths, and hashes", async (t) => {
  await t.test("round", () => {
    const errors = errorsFor(({ baselineReceipts }) => {
      baselineReceipts[1].value.observation_round = 1;
    });
    assert.ok(errors.some((error) => error.includes("rounds must be exactly")));
  });
  await t.test("round does not match fixture path", () => {
    const errors = errorsFor(({ baselineReceipts }) => {
      baselineReceipts[0].value.observation_round = 3;
      baselineReceipts[2].value.observation_round = 1;
    });
    assert.ok(
      errors.some((error) => error.includes("must carry observation round")),
    );
  });
  await t.test("loaded receipt", () => {
    const errors = errorsFor((inputs) => {
      inputs.baselineReceipts[1] = structuredClone(inputs.baselineReceipts[0]);
    });
    assert.ok(errors.some((error) => error.includes("duplicate receipt")));
  });
  await t.test("declared path", () => {
    const errors = errorsFor(({ budgets }) => {
      budgets.observation_baseline.raw_receipts[1].path =
        budgets.observation_baseline.raw_receipts[0].path;
    });
    assert.ok(errors.some((error) => error.includes("paths must be unique")));
  });
  await t.test("declared hash", () => {
    const errors = errorsFor(({ budgets }) => {
      budgets.observation_baseline.raw_receipts[1].sha256 =
        budgets.observation_baseline.raw_receipts[0].sha256;
    });
    assert.ok(errors.some((error) => error.includes("hashes must be unique")));
  });
});

test("rejects non-canonical receipt paths before filesystem access", () => {
  const inputs = v2Inputs();
  const unsafe =
    "fixtures/reliability/../reliability/observe-darwin-arm64-r1.json";
  inputs.budgets.observation_baseline.raw_receipts[0].path = unsafe;
  inputs.baselineReceipts[0].path = unsafe;
  inputs.sourceSnapshots[0].path = unsafe;
  const errors = validateReliabilityPolicy(inputs);
  assert.ok(errors.some((error) => error.includes("not canonical")));

  const budgets = structuredClone(inputs.budgets);
  budgets.observation_baseline.raw_receipts[0].path = "../../outside.json";
  assert.throws(
    () => loadBaselineReceipts(root, budgets),
    /refusing to read non-canonical receipt/u,
  );
});

test("rejects missing, duplicate, and malformed resource cells", async (t) => {
  const cells = (inputs) =>
    inputs.baselineReceipts[0].value.stages.find(
      ({ id }) => id === "resource-census",
    ).result;
  await t.test("missing", () => {
    const errors = errorsFor((inputs) => cells(inputs).pop());
    assert.ok(errors.some((error) => error.includes("exactly six cells")));
  });
  await t.test("duplicate", () => {
    const errors = errorsFor((inputs) => {
      const values = cells(inputs);
      values[5] = structuredClone(values[0]);
    });
    assert.ok(errors.some((error) => error.includes("must be unique")));
  });
  await t.test("malformed nested sample", () => {
    const errors = errorsFor((inputs) => {
      delete cells(inputs)[0].cleanup.threads;
    });
    assert.ok(errors.some((error) => error.includes("process sample")));
  });
  for (const [label, value] of [
    ["missing steady sample", undefined],
    ["null steady sample", null],
  ]) {
    await t.test(label, () => {
      const errors = errorsFor((inputs) => {
        const cell = cells(inputs)[0];
        if (value === undefined) delete cell.steady;
        else cell.steady = value;
      });
      assert.ok(
        errors.some(
          (error) =>
            error.includes("resource cell") || error.includes("process sample"),
        ),
      );
    });
  }
  await t.test("null cell", () => {
    const errors = errorsFor((inputs) => {
      cells(inputs)[0] = null;
    });
    assert.ok(errors.some((error) => error.includes("resource cell")));
  });
  await t.test("non-finite derived metric", () => {
    const errors = errorsFor((inputs) => {
      cells(inputs)[0].threads_per_run = Number.NaN;
    });
    assert.ok(errors.some((error) => error.includes("finite/non-negative")));
  });
});

test("rejects drift in every v2 observed maximum", async (t) => {
  for (const field of observedFields) {
    await t.test(field, () => {
      const errors = errorsFor(({ budgets }) => {
        budgets.observation_baseline.observed_maxima.active["128"][field] += 1;
      });
      assert.ok(
        errors.some(
          (error) => error.includes(field) && error.includes("raw maximum"),
        ),
      );
    });
  }
});

test("rejects deterministic ceiling inflation", () => {
  const errors = errorsFor(({ budgets }) => {
    budgets.budgets.active["128"].max_peak_rss_kib += 4096;
  });
  assert.ok(
    errors.some(
      (error) =>
        error.includes("max_peak_rss_kib") &&
        error.includes("deterministic ceiling"),
    ),
  );
});

test("is deletion-sensitive across receipt, provenance, samples, maxima, and ceilings", async (t) => {
  const mutations = [
    [
      "receipt",
      ({ baselineReceipts }) => delete baselineReceipts[0].value.seed,
    ],
    [
      "provenance",
      ({ baselineReceipts }) =>
        delete baselineReceipts[0].value.provenance.measurement_contract_sha256,
    ],
    [
      "sample",
      ({ baselineReceipts }) => {
        const stage = baselineReceipts[0].value.stages.find(
          ({ id }) => id === "resource-census",
        );
        delete stage.result[0].baseline.fds;
      },
    ],
    [
      "maximum",
      ({ budgets }) =>
        delete budgets.observation_baseline.observed_maxima.idle["1"]
          .cleanup_attachments,
    ],
    [
      "ceiling",
      ({ budgets }) => delete budgets.budgets.idle["1"].max_cleanup_attachments,
    ],
  ];
  for (const [label, mutate] of mutations) {
    await t.test(label, () => {
      assert.ok(errorsFor(mutate).length > 0, `${label} deletion passed`);
    });
  }
});

test("rejects partial stages, unfenced provenance, and unlocked builds", async (t) => {
  await t.test("partial stages", () => {
    const errors = errorsFor(({ baselineReceipts }) => {
      baselineReceipts[0].value.stages.splice(0, 1);
    });
    assert.ok(errors.some((error) => error.includes("exactly four stages")));
  });
  await t.test("unfenced provenance", () => {
    const errors = errorsFor(({ baselineReceipts }) => {
      baselineReceipts[0].value.action_trace.splice(0, 1);
    });
    assert.ok(errors.some((error) => error.includes("fence all four stages")));
  });
  await t.test("unlocked build", () => {
    const errors = errorsFor(({ baselineReceipts }) => {
      baselineReceipts[0].value.provenance.build.locked = false;
    });
    assert.ok(errors.some((error) => error.includes("fixed locked")));
  });
  await t.test("wrong daemon target", () => {
    const errors = errorsFor(({ baselineReceipts }) => {
      baselineReceipts[0].value.provenance.build.daemon_path =
        "target/debug/ctxmuxd";
    });
    assert.ok(errors.some((error) => error.includes("fixed locked")));
  });
});

test("rejects observation and freeze chronology contradictions", async (t) => {
  await t.test("completion before start", () => {
    const errors = errorsFor(({ baselineReceipts }) => {
      baselineReceipts[0].value.completed_at = "2026-08-10T00:00:00.000Z";
    });
    assert.ok(
      errors.some((error) => error.includes("before the budget freeze")),
    );
  });
  await t.test("freeze before completion", () => {
    const errors = errorsFor(({ budgets }) => {
      budgets.frozen_at = "2026-08-10T00:00:00.000Z";
    });
    assert.ok(
      errors.some((error) => error.includes("before the budget freeze")),
    );
  });
});
