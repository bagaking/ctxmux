import { spawnSync } from "node:child_process";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { isDeepStrictEqual } from "node:util";

import {
  canonicalFixturePath,
  EXPECTED_RECEIPT_PATHS,
  HASH_PATTERN,
  isObject,
  POLICY_SOURCE_PATHS,
  sameMembers,
  SNAPSHOT_FILE_PATHS,
  validateQualificationEnvironment,
  validateQualificationChronology,
  validateQualificationResourceCells,
  validateQualificationToolchain,
  validateQualificationWorkload,
  validatePassingObservationReceipt,
  validatePassingQualificationReceipt,
  validateSourceBoundBaseline,
} from "./reliability-baseline-policy.mjs";
import {
  BUDGET_FIELDS,
  COUNTS,
  MODES,
} from "./reliability-budget-contract.mjs";
import {
  canonicalCheckoutPrecedesCommand,
  parseWorkflow,
} from "./ci-reachability.mjs";
import {
  createQualificationPreflight,
  enterCanonicalArtifactOwner,
  existingOwnedFileIdentity,
  parseQualificationPreflight,
  readOwnedFile,
  readOwnedJson,
} from "./reliability-artifact-owner.mts";
import {
  assertReliabilityGcIdentities,
  loadReliabilityGcContract,
} from "./reliability-gc-contract.mts";
import { validateQualificationStatsArtifact } from "./reliability-gc-stats.mts";

export { deriveBudgetCeiling } from "./reliability-budget-contract.mjs";

const GIT_OBJECT_PATTERN = /^[0-9a-f]{40}$/u;
const EXPECTED_CHECK_CORE_SHA256 =
  "6701ec274df3af84aae25bc7e21bb9d3c9c7ff939c2e3b618024bb6b0bb97b54";
const EXPECTED_QUALIFICATION_LAUNCHER_SHA256 =
  "ea4b034e70736db01d40e61dc530d81efdc1752f455f56697c93c222b4e11f9b";
const EXPECTED_QUALIFICATION_POLICY = {
  schema: "ctxmux.reliability-qualification-policy.v1",
  profiles: {
    smoke: {
      time_budget_seconds: 60,
      soak_seconds: 0,
      resource_counts: [1],
    },
    nightly: {
      time_budget_seconds: 70 * 60,
      soak_seconds: 30 * 60,
      resource_counts: [1, 32, 128],
    },
    release: {
      time_budget_seconds: 3 * 60 * 60,
      soak_seconds: 2 * 60 * 60,
      resource_counts: [1, 32, 128],
    },
    observe: {
      time_budget_seconds: 45 * 60,
      soak_seconds: 0,
      resource_counts: [1, 32, 128],
    },
  },
  resource_start_concurrency: 8,
  seed_controls: ["fanout payload byte", "secret marker"],
};
const QUALIFICATION_POLICY_PATTERN =
  /export const QUALIFICATION_POLICY_SOURCE = String\.raw`(?<policy>[\s\S]*?)`;/gu;

export function qualificationPolicyFromHarness(harnessSource, errors) {
  const matches = [
    ...(harnessSource ?? "").matchAll(QUALIFICATION_POLICY_PATTERN),
  ];
  if (matches.length !== 1) {
    errors.push(
      "qualification harness must contain one structured runtime policy contract",
    );
    return undefined;
  }
  try {
    return JSON.parse(matches[0].groups?.policy ?? "");
  } catch {
    errors.push("qualification runtime policy contract must be valid JSON");
    return undefined;
  }
}

export function validateQualificationArtifacts({
  root,
  resolvedReceiptPath,
  value,
  expectedProfile,
  expectedReceiptIdentity,
  expectedReceiptSha256,
  preexistingReceiptIdentity,
}) {
  const errors = [];
  const artifactDirectory = path.dirname(resolvedReceiptPath);
  if (
    expectedProfile !== undefined &&
    resolvedReceiptPath !==
      path.join(
        path.resolve(root),
        "target",
        "reliability",
        expectedProfile,
        "result.json",
      )
  ) {
    errors.push("qualification receipt must use its canonical profile path");
    return errors;
  }
  let receiptIdentity;
  try {
    const receiptFile = readOwnedFile("result.json");
    receiptIdentity = receiptFile.identity;
    if (
      (expectedReceiptIdentity !== undefined &&
        (receiptIdentity.dev !== expectedReceiptIdentity.dev ||
          receiptIdentity.ino !== expectedReceiptIdentity.ino)) ||
      (expectedReceiptSha256 !== undefined &&
        receiptFile.sha256 !== expectedReceiptSha256)
    ) {
      throw new Error("receipt identity or bytes changed after validation");
    }
    if (
      preexistingReceiptIdentity !== undefined &&
      preexistingReceiptIdentity !== null &&
      receiptIdentity.dev === preexistingReceiptIdentity.dev &&
      receiptIdentity.ino === preexistingReceiptIdentity.ino
    ) {
      throw new Error("receipt was not replaced after preflight");
    }
  } catch {
    errors.push(
      "qualification receipt is unavailable inside its artifact owner",
    );
    return errors;
  }
  const seen = new Set();
  const statsSummaries = [];
  const gcContract =
    value?.schema === "ctxmux.reliability-qualification.v3"
      ? loadReliabilityGcContract(root)
      : null;
  const artifactSets = [
    ["daemon", value?.daemon_logs, false],
    ["stats", value?.stats_logs, true],
  ];
  for (const [kind, paths, validateStats] of artifactSets) {
    if (
      kind === "stats" &&
      value?.schema !== "ctxmux.reliability-qualification.v3" &&
      paths === undefined
    ) {
      continue;
    }
    if (!Array.isArray(paths) || paths.length === 0) {
      errors.push(`qualification ${kind} logs must be a non-empty array`);
      continue;
    }
    for (const declared of paths) {
      const logPath = validateStats ? declared?.path : declared;
      if (typeof logPath !== "string") {
        errors.push(`qualification ${kind} log declaration is malformed`);
        continue;
      }
      const resolvedLogPath = path.resolve(root, logPath);
      const artifactRelativeLog = path.relative(
        artifactDirectory,
        resolvedLogPath,
      );
      const logName = path.basename(resolvedLogPath);
      let logIdentity;
      let logBytes;
      try {
        const owned = readOwnedFile(logName);
        logIdentity = owned.identity;
        logBytes = owned.bytes;
      } catch {
        logIdentity = undefined;
      }
      if (
        !canonicalFixturePath(logPath) ||
        path.dirname(resolvedLogPath) !== artifactDirectory ||
        artifactRelativeLog !== logName ||
        logIdentity === undefined ||
        logIdentity === null ||
        (receiptIdentity !== undefined &&
          logIdentity.dev === receiptIdentity.dev &&
          logIdentity.ino === receiptIdentity.ino) ||
        seen.has(resolvedLogPath)
      ) {
        errors.push(`qualification ${kind} log is unavailable: ${logPath}`);
      } else if (validateStats) {
        try {
          const summary = validateQualificationStatsArtifact(logBytes);
          if (gcContract === null) {
            throw new Error("stats logs require the v3 qualification contract");
          }
          const maxGap = Number(
            gcContract.contract.replay_pressure.sampling
              .max_owner_sample_gap_ms,
          );
          if (summary.max_sample_gap_ms > maxGap) {
            throw new Error(
              `producer gap ${String(summary.max_sample_gap_ms)}ms exceeds ${String(maxGap)}ms`,
            );
          }
          if (
            !isObject(declared) ||
            !sameMembers(Object.keys(declared), [
              "path",
              "sha256",
              "daemon_instance",
              "final_seq",
            ]) ||
            declared.sha256 !==
              crypto.createHash("sha256").update(logBytes).digest("hex") ||
            declared.daemon_instance !== summary.daemon_instance ||
            declared.final_seq !== summary.last_seq
          ) {
            throw new Error("declared stats identity differs from owned bytes");
          }
          statsSummaries.push(summary);
        } catch (error) {
          errors.push(
            `qualification stats log is invalid: ${logPath}: ${error instanceof Error ? error.message : String(error)}`,
          );
        }
      }
      seen.add(resolvedLogPath);
    }
  }
  if (value?.schema === "ctxmux.reliability-qualification.v3") {
    const actualEpochs = statsSummaries.map(
      ({ daemon_instance }) => daemon_instance,
    );
    const summariesByEpoch = new Map(
      statsSummaries.map((summary) => [summary.daemon_instance, summary]),
    );
    const requiredEpochs = gcEpochsFromReceipt(value);
    if (new Set(actualEpochs).size !== actualEpochs.length) {
      errors.push("qualification stats logs repeat a daemon instance");
    }
    if (new Set(requiredEpochs).size !== requiredEpochs.length) {
      errors.push("qualification GC epochs repeat a daemon instance");
    }
    for (const daemonInstance of requiredEpochs) {
      if (!actualEpochs.includes(daemonInstance)) {
        errors.push(
          `qualification GC epoch has no owned stats log: ${daemonInstance}`,
        );
      }
    }
    for (const epoch of gcEpochReceipts(value)) {
      const artifact = summariesByEpoch.get(epoch.daemon_instance);
      if (
        artifact === undefined ||
        !isDeepStrictEqual(artifact.final.current, epoch.current) ||
        !isDeepStrictEqual(artifact.final.high_water, epoch.high_water) ||
        !isDeepStrictEqual(artifact.final.cumulative, epoch.cumulative)
      ) {
        errors.push(
          `qualification GC epoch differs from its final stats frame: ${String(epoch.daemon_instance)}`,
        );
      }
    }
  }
  return errors;
}

function gcEpochsFromReceipt(value) {
  return gcEpochReceipts(value)
    .map(({ daemon_instance }) => daemon_instance)
    .filter((value) => typeof value === "string");
}

function gcEpochReceipts(value) {
  const result = value?.stages?.find(
    ({ id }) => id === "retained-state-plateau",
  )?.result;
  if (!isObject(result)) return [];
  return [result.bounded_churn, result.replay_pressure]
    .flatMap((modes) => (Array.isArray(modes) ? modes : []))
    .flatMap(({ epochs }) => (Array.isArray(epochs) ? epochs : []));
}

function resolveCanonicalQualificationEvidencePath(
  root,
  requestedPath,
  expectedProfile,
) {
  const resolvedRoot = path.resolve(root);
  const resolvedEvidencePath = path.resolve(resolvedRoot, requestedPath);
  const canonicalEvidencePath = path.join(
    resolvedRoot,
    "target",
    "reliability",
    expectedProfile,
    "result.json",
  );
  const rootRelativeEvidence = path.relative(
    resolvedRoot,
    resolvedEvidencePath,
  );
  if (
    resolvedEvidencePath !== canonicalEvidencePath ||
    rootRelativeEvidence === "" ||
    rootRelativeEvidence === ".." ||
    rootRelativeEvidence.startsWith(`..${path.sep}`) ||
    path.isAbsolute(rootRelativeEvidence) ||
    path.basename(resolvedEvidencePath) !== "result.json"
  ) {
    throw new Error(
      "qualification evidence must use its canonical profile-owned result path",
    );
  }
  return resolvedEvidencePath;
}

export function prepareQualificationEvidencePath(
  root,
  requestedPath,
  expectedProfile,
) {
  const resolvedRoot = path.resolve(root);
  const resolvedEvidencePath = resolveCanonicalQualificationEvidencePath(
    resolvedRoot,
    requestedPath,
    expectedProfile,
  );
  const artifactOwnerIdentity = enterCanonicalArtifactOwner({
    root: resolvedRoot,
    profile: expectedProfile,
    create: true,
  });
  const preexistingReceiptIdentity = existingOwnedFileIdentity("result.json");
  const gcContract = loadReliabilityGcContract(resolvedRoot);
  const preflight = createQualificationPreflight(
    expectedProfile,
    artifactOwnerIdentity,
    preexistingReceiptIdentity,
    gcContract.workload_contract,
    gcContract.workload_helper,
  );
  return {
    resolvedEvidencePath,
    preflight,
  };
}

function validateQualificationPolicyContract(qualificationPolicy, errors) {
  if (!isDeepStrictEqual(qualificationPolicy, EXPECTED_QUALIFICATION_POLICY)) {
    errors.push(
      "qualification runtime policy must match the canonical profile contract",
    );
  }
}

export function validateQualificationInvocationIdentity(value, current) {
  if (
    current.clean !== true ||
    value?.provenance?.source?.commit !== current.commit ||
    value?.provenance?.source?.tree !== current.tree ||
    value?.provenance?.build?.source_commit !== current.commit ||
    value?.provenance?.build?.source_tree !== current.tree
  ) {
    return [
      "qualification receipt must bind the exact clean current source invocation",
    ];
  }
  return [];
}

function validTimestamp(value) {
  return typeof value === "string" && Number.isFinite(Date.parse(value));
}

const V3_RECEIPT_FIELDS = [
  "schema",
  "status",
  "profile",
  "observation_round",
  "seed",
  "recorded_at",
  "completed_at",
  "time_budget_seconds",
  "environment",
  "provenance",
  "declared_limits",
  "action_trace",
  "stages",
  "daemon_logs",
  "stats_logs",
  "error",
];
const V3_PROVENANCE_FIELDS = [
  "claim_scope",
  "binary_source_attestation",
  "source",
  "harness",
  "launcher",
  "daemon",
  "rss_sampler",
  "rss_sampler_sources",
  "lockfiles",
  "build",
  "toolchain",
  "measurement_contract_encoding",
  "measurement_contract_sha256",
  "workload_contract",
  "workload_helper",
];
const V3_BASE_STAGE_IDS = [
  "chaos-owner-matrix",
  "security-negative-space",
  "stress-and-soak",
];
const V3_PASSING_ACTIONS = new Set([
  "provenance.captured",
  "provenance.verified",
  "provenance.reverified",
  "stage.start",
  "stage.pass",
  "chaos.integration_host.spawn",
  "chaos.integration_host.survived",
  "chaos.child.kill",
  "chaos.daemon.kill",
  "security.negative_space",
  "stress.concurrent_start",
  "stress.soak",
  "stress.fanout",
  "resource.measurement",
  "gc.turnover",
  "gc.replay_pressure",
]);

/** Validate receipts emitted by the current qualification harness. Historical
 * source-bound v2 observation receipts continue to use the frozen validator
 * in reliability-baseline-policy.mjs. */
export function validatePassingQualificationReceiptV3({
  receiptPath,
  value,
  expectedProfile,
  qualificationPolicy,
  gc,
  preflight,
  notBefore,
  verifiedAt,
  current,
  budgets,
}) {
  const errors = [];
  const expect = (condition, message) => {
    if (!condition) errors.push(`v3 ${message}: ${receiptPath}`);
  };
  if (!isObject(value) || !sameMembers(Object.keys(value), V3_RECEIPT_FIELDS)) {
    errors.push(`v3 receipt fields are not exact: ${receiptPath}`);
    return errors;
  }
  const profilePolicy = qualificationPolicy?.profiles?.[expectedProfile];
  expect(
    value.schema === "ctxmux.reliability-qualification.v3" &&
      value.status === "pass" &&
      value.profile === expectedProfile &&
      value.error === null,
    `receipt must pass ${expectedProfile}`,
  );
  expect(
    Number.isSafeInteger(value.seed) &&
      value.seed === Number(gc.contract.seed) &&
      value.time_budget_seconds ===
        gc.contract.profile_time_budgets_seconds[expectedProfile],
    "receipt seed/time budget drifted from the GC contract",
  );
  expect(
    validTimestamp(value.recorded_at) &&
      validTimestamp(value.completed_at) &&
      validTimestamp(notBefore) &&
      validTimestamp(verifiedAt) &&
      Date.parse(value.recorded_at) >= Date.parse(notBefore) &&
      Date.parse(value.recorded_at) <= Date.parse(value.completed_at) &&
      Date.parse(value.completed_at) <= Date.parse(verifiedAt) &&
      Date.parse(value.completed_at) - Date.parse(value.recorded_at) <=
        value.time_budget_seconds * 1000,
    "receipt is not a fresh monotonic invocation",
  );
  validateQualificationEnvironment(
    value.environment,
    receiptPath,
    errors,
    "v3",
    true,
  );
  expect(
    expectedProfile === "observe"
      ? [1, 2, 3].includes(value.observation_round)
      : value.observation_round === null,
    "observation round does not match the profile",
  );
  expect(
    Array.isArray(value.daemon_logs) &&
      value.daemon_logs.length > 0 &&
      value.daemon_logs.every(canonicalFixturePath) &&
      Array.isArray(value.stats_logs) &&
      value.stats_logs.length > 0 &&
      value.stats_logs.length <= value.daemon_logs.length &&
      value.stats_logs.every(
        (entry) => isObject(entry) && canonicalFixturePath(entry.path),
      ),
    "daemon/stats artifact lists are incomplete",
  );

  const provenance = value.provenance;
  expect(
    isObject(provenance) &&
      sameMembers(Object.keys(provenance), V3_PROVENANCE_FIELDS) &&
      provenance.claim_scope === "locally_observed" &&
      provenance.binary_source_attestation === false &&
      isDeepStrictEqual(provenance.workload_contract, gc.workload_contract) &&
      isDeepStrictEqual(provenance.workload_helper, gc.workload_helper) &&
      isDeepStrictEqual(
        provenance.workload_contract,
        preflight.workload_contract,
      ) &&
      isDeepStrictEqual(provenance.workload_helper, preflight.workload_helper),
    "provenance does not bind the frozen workload identities",
  );
  expect(
    isObject(provenance?.source) &&
      GIT_OBJECT_PATTERN.test(provenance.source.commit ?? "") &&
      GIT_OBJECT_PATTERN.test(provenance.source.tree ?? "") &&
      isDeepStrictEqual(provenance.source.worktree, {
        status_format: "git-status-porcelain-v1-z",
        clean: true,
        entries: [],
      }) &&
      provenance?.build?.source_commit === provenance.source.commit &&
      provenance?.build?.source_tree === provenance.source.tree &&
      provenance?.build?.worktree_clean === true &&
      provenance?.build?.locked === true,
    "source/build provenance is not exact and clean",
  );
  expect(
    [
      provenance?.harness,
      provenance?.launcher,
      provenance?.daemon,
      provenance?.rss_sampler,
    ].every(validFileIdentity) &&
      Array.isArray(provenance?.rss_sampler_sources) &&
      provenance.rss_sampler_sources.length === 2 &&
      provenance.rss_sampler_sources.every(validFileIdentity) &&
      isDeepStrictEqual(
        provenance.rss_sampler_sources.map(({ path }) => path),
        [
          "crates/ctxmux-rss-sampler/src/main.rs",
          "crates/ctxmux-process-stats/src/lib.rs",
        ],
      ) &&
      Array.isArray(provenance?.lockfiles) &&
      provenance.lockfiles.length === 2 &&
      provenance.lockfiles.every(validFileIdentity),
    "file provenance is malformed",
  );
  validateQualificationToolchain(
    provenance?.toolchain,
    receiptPath,
    errors,
    "v3",
  );
  expect(
    provenance?.measurement_contract_encoding === "json-stringify-utf8" &&
      provenance?.measurement_contract_sha256 ===
        crypto
          .createHash("sha256")
          .update(JSON.stringify(budgets?.measurement_contract))
          .digest("hex"),
    "toolchain or measurement contract is malformed",
  );
  if (current !== undefined) {
    expect(
      provenance.source.commit === current.commit &&
        provenance.source.tree === current.tree &&
        current.clean === true &&
        isDeepStrictEqual(provenance.harness, current.harness) &&
        isDeepStrictEqual(provenance.launcher, current.launcher) &&
        isDeepStrictEqual(provenance.daemon, current.daemon) &&
        isDeepStrictEqual(provenance.rss_sampler, current.rss_sampler) &&
        isDeepStrictEqual(
          provenance.rss_sampler_sources,
          current.rss_sampler_sources,
        ) &&
        isDeepStrictEqual(provenance.lockfiles, current.lockfiles) &&
        provenance.measurement_contract_sha256 ===
          current.measurement_contract_sha256,
      "source, binary, lockfile, or measurement bytes differ from the current invocation",
    );
  }
  expect(
    isDeepStrictEqual(provenance?.build, {
      cwd: ".",
      argv: [
        "cargo",
        "build",
        "--locked",
        "--quiet",
        "--package",
        "ctxmux-daemon",
        "--package",
        "ctxmux-rss-sampler",
        "--target-dir",
        "target/reliability/provenance-build",
      ],
      source_commit: provenance?.source?.commit,
      source_tree: provenance?.source?.tree,
      worktree_clean: true,
      target_directory: "target/reliability/provenance-build",
      daemon_path: "target/reliability/provenance-build/debug/ctxmuxd",
      locked: true,
    }),
    "build provenance does not match the fixed locked envelope",
  );

  const limits = value.declared_limits;
  validateQualificationWorkload(
    limits,
    receiptPath,
    errors,
    {
      global_run_quota: gc.contract.bounded_churn.run_ceiling,
      exited_run_gc: "exact_terminal_replacement",
      qualification_stage: "all",
      resource_counts: profilePolicy?.resource_counts,
      soak_seconds: profilePolicy?.soak_seconds,
      resource_start_concurrency: gc.contract.bounded_churn.concurrency,
      seed_controls: qualificationPolicy?.seed_controls,
    },
    "v3",
  );
  const expectedStages = [
    ...V3_BASE_STAGE_IDS,
    ...(expectedProfile === "nightly" || expectedProfile === "release"
      ? ["retained-state-plateau"]
      : []),
    "resource-census",
    ...(expectedProfile === "observe" ? [] : ["frozen-resource-budgets"]),
  ];
  expect(
    Array.isArray(value.stages) &&
      isDeepStrictEqual(
        value.stages.map((stage) => stage?.id),
        expectedStages,
      ) &&
      value.stages.every(
        (stage) =>
          isObject(stage) &&
          sameMembers(Object.keys(stage), [
            "id",
            "status",
            "started_at",
            "completed_at",
            "result",
          ]) &&
          stage.status === "pass" &&
          validTimestamp(stage.started_at) &&
          validTimestamp(stage.completed_at) &&
          Date.parse(stage.started_at) <= Date.parse(stage.completed_at),
      ),
    "stages are incomplete, out of order, or not passing",
  );
  validateV3Trace(value, expectedStages, provenance, preflight, expect);
  validateQualificationChronology(
    value,
    receiptPath,
    expectedStages,
    errors,
    "v3",
  );
  validateV3ResourceStage(
    value.stages?.find((stage) => stage?.id === "resource-census")?.result,
    profilePolicy?.resource_counts,
    budgets,
    expect,
  );
  validateV3Soak(value, expectedProfile, profilePolicy?.soak_seconds, expect);
  if (expectedStages.includes("retained-state-plateau")) {
    const retainedStage = value.stages.find(
      ({ id }) => id === "retained-state-plateau",
    );
    expect(
      Date.parse(retainedStage?.completed_at) -
        Date.parse(retainedStage?.started_at) <=
        gc.contract.replay_pressure.time_budgets_seconds.total * 1000,
      "retained-state stage exceeded its total time budget",
    );
    validateV3GcStage(retainedStage?.result, gc.contract, expect);
  }
  return errors;
}

function validateV3Soak(value, profile, expectedSeconds, expect) {
  const stress = value.stages?.find(
    ({ id }) => id === "stress-and-soak",
  )?.result;
  if (profile !== "nightly" && profile !== "release") {
    expect(
      stress?.soak?.duration_seconds === 0,
      "short profile ran a time soak",
    );
    return;
  }
  const soak = stress?.soak;
  const stressStage = value.stages?.find(({ id }) => id === "stress-and-soak");
  const actions = value.action_trace.filter(
    ({ action }) => action === "stress.soak",
  );
  expect(
    actions.length === 1 &&
      actions[0].configured_duration_seconds === expectedSeconds &&
      actions[0].elapsed_seconds === soak?.elapsed_seconds &&
      soak?.configured_duration_seconds === expectedSeconds &&
      soak?.elapsed_seconds >= expectedSeconds &&
      Date.parse(stressStage?.completed_at) -
        Date.parse(stressStage?.started_at) >=
        expectedSeconds * 1000 &&
      Number.isSafeInteger(soak?.cycles) &&
      soak.cycles > 0 &&
      soak?.active_runs === 8 &&
      soak?.cleanup_live_children === 0 &&
      soak?.cleanup_attachments === 0 &&
      validRssSeries(soak?.rss_samples, soak?.max_rss_sample_gap_ms, 1000),
    "ordinary soak is shortened, missing, or has no cleanup/resource evidence",
  );
}

function validateV3Trace(value, expectedStages, provenance, preflight, expect) {
  const trace = value.action_trace;
  if (!Array.isArray(trace)) {
    expect(false, "action trace is missing");
    return;
  }
  expect(
    trace.every(
      (entry) =>
        isObject(entry) &&
        validTimestamp(entry.timestamp) &&
        V3_PASSING_ACTIONS.has(entry.action),
    ),
    "action trace contains a malformed or non-passing action",
  );
  const indexes = (action) =>
    trace.flatMap((entry, index) => (entry.action === action ? [index] : []));
  const captured = indexes("provenance.captured");
  const verified = indexes("provenance.verified");
  const reverified = indexes("provenance.reverified");
  const starts = trace.filter((entry) => entry.action === "stage.start");
  const passes = trace.filter((entry) => entry.action === "stage.pass");
  expect(
    captured.length === 1 &&
      verified.length === 1 &&
      reverified.length === 1 &&
      trace[captured[0]]?.invocation_nonce === preflight.invocation_nonce &&
      isDeepStrictEqual(
        trace[captured[0]]?.workload_contract,
        provenance.workload_contract,
      ) &&
      isDeepStrictEqual(
        trace[captured[0]]?.workload_helper,
        provenance.workload_helper,
      ) &&
      trace[captured[0]]?.source_commit === provenance.source.commit &&
      trace[captured[0]]?.worktree_clean === true &&
      trace[captured[0]]?.harness_sha256 === provenance.harness.sha256 &&
      trace[captured[0]]?.launcher_sha256 === provenance.launcher.sha256 &&
      trace[captured[0]]?.daemon_sha256 === provenance.daemon.sha256 &&
      trace[captured[0]]?.rss_sampler_sha256 ===
        provenance.rss_sampler.sha256 &&
      isDeepStrictEqual(
        trace[captured[0]]?.rss_sampler_sources,
        provenance.rss_sampler_sources,
      ) &&
      trace[captured[0]]?.measurement_contract_sha256 ===
        provenance.measurement_contract_sha256 &&
      trace[reverified[0]]?.daemon_sha256 === provenance.daemon.sha256 &&
      trace[reverified[0]]?.rss_sampler_sha256 ===
        provenance.rss_sampler.sha256 &&
      isDeepStrictEqual(
        trace[reverified[0]]?.rss_sampler_sources,
        provenance.rss_sampler_sources,
      ) &&
      isDeepStrictEqual(
        trace[reverified[0]]?.workload_contract,
        provenance.workload_contract,
      ) &&
      isDeepStrictEqual(
        trace[reverified[0]]?.workload_helper,
        provenance.workload_helper,
      ) &&
      isDeepStrictEqual(
        starts.map(({ id }) => id),
        expectedStages,
      ) &&
      isDeepStrictEqual(
        passes.map(({ id }) => id),
        expectedStages,
      ) &&
      captured[0] < verified[0] &&
      verified[0] < indexes("stage.start")[0] &&
      reverified[0] > indexes("stage.pass").at(-1),
    "action trace does not fence the invocation and canonical stages",
  );
  if (expectedStages.includes("retained-state-plateau")) {
    const turnovers = trace.filter(({ action }) => action === "gc.turnover");
    const pressure = trace.filter(
      ({ action }) => action === "gc.replay_pressure",
    );
    expect(
      turnovers.length === 6 &&
        sameMembers(
          turnovers.map(({ mode, window }) => `${mode}/${String(window)}`),
          ["memory_only", "persistent"].flatMap((mode) =>
            [1, 2, 3].map((window) => `${mode}/${String(window)}`),
          ),
        ) &&
        pressure.length === 2 &&
        sameMembers(
          pressure.map(({ mode }) => mode),
          ["memory_replay_pressure", "persistent_replay_pressure"],
        ),
      "GC trace does not cover both modes, three turnovers, and replay pressure",
    );
  }
}

function validateV3ResourceStage(cells, expectedCounts, budgets, expect) {
  const structuralErrors = [];
  validateQualificationResourceCells(
    cells,
    "current qualification receipt",
    structuralErrors,
    expectedCounts ?? [],
    "v3",
    true,
  );
  expect(
    structuralErrors.length === 0 &&
      Array.isArray(cells) &&
      cells.every((cell) => validV3ResourceCell(cell, expectedCounts, budgets)),
    "resource census does not cover the canonical cells, derived values, and frozen budgets",
  );
}

function validV3ResourceCell(cell, expectedCounts, budgets) {
  if (!(expectedCounts ?? []).includes(cell.runs)) return false;
  const budget = budgets?.budgets?.[cell.mode]?.[String(cell.runs)];
  return (
    isObject(budget) &&
    cell.cpu_core_percent <= budget.max_cpu_core_percent &&
    cell.peak_rss_kib <= budget.max_peak_rss_kib &&
    cell.steady.rss_kib <= budget.max_steady_rss_kib &&
    cell.retained_output_bytes_per_run <=
      budget.max_retained_output_bytes_per_run &&
    cell.rss_kib_per_run <= budget.max_rss_kib_per_run &&
    cell.threads_per_run <= budget.max_threads_per_run &&
    cell.fds_per_run <= budget.max_fds_per_run &&
    Math.max(0, cell.cleanup.threads - cell.baseline.threads) <=
      budget.max_cleanup_threads_delta &&
    cell.cleanup_live_children <= budget.max_cleanup_live_children &&
    cell.cleanup_attachments <= budget.max_cleanup_attachments
  );
}

const GC_TUPLE_FIELDS =
  "run_id operation_key lineage state latest_output_bytes durable_output_bytes first_available_byte replay_bytes replay_sha256 chunks truncated".split(
    " ",
  );
const GC_CHUNK_FIELDS = "start_byte end_byte bytes sha256".split(" ");
const GC_TUPLE_EVIDENCE_FIELDS = "count total_replay_bytes sha256 tuples".split(
  " ",
);
const CANONICAL_UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/iu;

function validateGcTupleEvidence(evidence, expected) {
  try {
    const exactIndices =
      expected.firstIndex === undefined
        ? null
        : new Set(
            Array.from(
              { length: expected.count },
              (_, offset) => expected.firstIndex + offset,
            ),
          );
    const runIds = new Set();
    const operationKeys = new Set();
    const observedIndices = new Set();
    const liveByRun = new Map(
      (expected.live?.tuples ?? []).map((tuple) => [tuple.run_id, tuple]),
    );
    return (
      isObject(evidence) &&
      sameMembers(Object.keys(evidence), GC_TUPLE_EVIDENCE_FIELDS) &&
      HASH_PATTERN.test(evidence.sha256 ?? "") &&
      Array.isArray(evidence.tuples) &&
      evidence.count === expected.count &&
      evidence.count === evidence.tuples.length &&
      (expected.totalReplayBytes === undefined ||
        evidence.total_replay_bytes === expected.totalReplayBytes) &&
      evidence.total_replay_bytes ===
        evidence.tuples.reduce((sum, tuple) => sum + tuple.replay_bytes, 0) &&
      evidence.sha256 ===
        crypto
          .createHash("sha256")
          .update(JSON.stringify(evidence.tuples))
          .digest("hex") &&
      evidence.tuples.every(
        (tuple, index) =>
          index === 0 ||
          evidence.tuples[index - 1].run_id.localeCompare(tuple.run_id) < 0,
      ) &&
      evidence.tuples.every((tuple) => {
        if (
          !isObject(tuple) ||
          !sameMembers(Object.keys(tuple), GC_TUPLE_FIELDS)
        )
          return false;
        const marker = /^gc-pressure:([^:]+):(\d+):([0-9a-f]{64})$/u.exec(
          tuple.operation_key,
        );
        const index = Number(marker?.[2]);
        if (
          marker === null ||
          marker[1] !== expected.mode ||
          !Number.isSafeInteger(index) ||
          String(index) !== marker[2] ||
          index < expected.minimumIndex ||
          index > expected.maximumIndex ||
          (exactIndices !== null && !exactIndices.has(index)) ||
          !CANONICAL_UUID_PATTERN.test(tuple.run_id ?? "") ||
          runIds.has(tuple.run_id) ||
          operationKeys.has(tuple.operation_key) ||
          observedIndices.has(index) ||
          tuple.lineage !== null ||
          !isDeepStrictEqual(tuple.state, {
            type: "exited",
            code: 0,
            signal: null,
          }) ||
          ![
            tuple.latest_output_bytes,
            tuple.first_available_byte,
            tuple.replay_bytes,
          ].every((value) => Number.isSafeInteger(value) && value >= 0) ||
          tuple.replay_bytes > expected.payloadBytes ||
          typeof tuple.truncated !== "boolean" ||
          !Array.isArray(tuple.chunks) ||
          tuple.chunks.length === 0
        )
          return false;
        runIds.add(tuple.run_id);
        operationKeys.add(tuple.operation_key);
        observedIndices.add(index);
        const sourceDigest = crypto
          .createHash("sha256")
          .update(`${expected.seed}:${expected.mode}:${marker[2]}`, "utf8")
          .digest("hex");
        if (marker[3] !== sourceDigest) return false;
        const persistent = expected.mode.startsWith("persistent");
        if (
          (persistent &&
            tuple.durable_output_bytes !== tuple.latest_output_bytes) ||
          (!persistent && tuple.durable_output_bytes !== null) ||
          tuple.first_available_byte > tuple.latest_output_bytes ||
          tuple.chunks[0]?.start_byte !== tuple.first_available_byte ||
          tuple.chunks.at(-1)?.end_byte !== tuple.latest_output_bytes
        )
          return false;
        let replayOffset = expected.payloadBytes - tuple.replay_bytes;
        let replayBytes = 0;
        for (const [chunkIndex, chunk] of tuple.chunks.entries()) {
          if (
            !isObject(chunk) ||
            !sameMembers(Object.keys(chunk), GC_CHUNK_FIELDS) ||
            !Number.isSafeInteger(chunk.start_byte) ||
            chunk.start_byte < 0 ||
            !Number.isSafeInteger(chunk.end_byte) ||
            chunk.end_byte <= chunk.start_byte ||
            !Number.isSafeInteger(chunk.bytes) ||
            chunk.bytes <= 0 ||
            chunk.bytes > tuple.replay_bytes - replayBytes ||
            (expected.maxChunkBytes !== undefined &&
              chunk.bytes > expected.maxChunkBytes) ||
            !HASH_PATTERN.test(chunk.sha256 ?? "") ||
            chunk.end_byte - chunk.start_byte !== chunk.bytes ||
            (chunkIndex > 0 &&
              chunk.start_byte !== tuple.chunks[chunkIndex - 1].end_byte) ||
            chunk.sha256 !==
              repeatedDigestSliceSha256(sourceDigest, replayOffset, chunk.bytes)
          )
            return false;
          replayOffset += chunk.bytes;
          replayBytes += chunk.bytes;
        }
        return (
          replayBytes === tuple.replay_bytes &&
          tuple.replay_sha256 ===
            repeatedDigestSliceSha256(
              sourceDigest,
              expected.payloadBytes - tuple.replay_bytes,
              tuple.replay_bytes,
            ) &&
          (!expected.exactReplay ||
            (tuple.replay_bytes === expected.payloadBytes &&
              tuple.truncated === false &&
              tuple.first_available_byte === 0 &&
              tuple.latest_output_bytes === expected.payloadBytes)) &&
          recoveredTupleMatchesLive(tuple, liveByRun)
        );
      }) &&
      observedIndices.size === expected.count
    );
  } catch {
    return false;
  }
}

function repeatedDigestSliceSha256(digest, offset, bytes) {
  const start = offset % digest.length;
  const cacheKey = `${digest}:${String(start)}:${String(bytes)}`;
  const cached = GC_REPLAY_DIGEST_CACHE.get(cacheKey);
  if (cached !== undefined) return cached;
  const hash = crypto.createHash("sha256");
  let remaining = bytes;
  if (start !== 0 && remaining > 0) {
    const prefix = digest.slice(start, start + remaining);
    hash.update(prefix, "ascii");
    remaining -= prefix.length;
  }
  const block = digest.repeat(1024);
  while (remaining >= block.length) {
    hash.update(block, "ascii");
    remaining -= block.length;
  }
  if (remaining > 0) {
    hash.update(
      digest.repeat(Math.ceil(remaining / digest.length)).slice(0, remaining),
      "ascii",
    );
  }
  const sha256 = hash.digest("hex");
  if (GC_REPLAY_DIGEST_CACHE.size >= 4096) GC_REPLAY_DIGEST_CACHE.clear();
  GC_REPLAY_DIGEST_CACHE.set(cacheKey, sha256);
  return sha256;
}

const GC_REPLAY_DIGEST_CACHE = new Map();

function recoveredTupleMatchesLive(tuple, liveByRun) {
  if (liveByRun.size === 0) return true;
  const live = liveByRun.get(tuple.run_id);
  const liveSuffix = live?.chunks.filter(
    (chunk) => chunk.start_byte >= tuple.first_available_byte,
  );
  return (
    live !== undefined &&
    tuple.operation_key === live.operation_key &&
    isDeepStrictEqual(tuple.lineage, live.lineage) &&
    isDeepStrictEqual(tuple.state, live.state) &&
    tuple.latest_output_bytes === live.latest_output_bytes &&
    tuple.durable_output_bytes === live.durable_output_bytes &&
    tuple.first_available_byte >= live.first_available_byte &&
    tuple.truncated ===
      (live.truncated ||
        tuple.first_available_byte > live.first_available_byte) &&
    isDeepStrictEqual(tuple.chunks, liveSuffix)
  );
}

function exactGcReplayTransition(
  before,
  after,
  firstReplacement,
  lastReplacement,
) {
  if (!Array.isArray(before?.tuples) || !Array.isArray(after?.tuples)) {
    return false;
  }
  const beforeByKey = new Map(
    before.tuples.map((tuple) => [tuple.operation_key, tuple]),
  );
  const afterByKey = new Map(
    after.tuples.map((tuple) => [tuple.operation_key, tuple]),
  );
  const replacementIndices = new Set(
    Array.from(
      { length: lastReplacement - firstReplacement + 1 },
      (_, offset) => firstReplacement + offset,
    ),
  );
  const afterReplacementIndices = new Set(
    after.tuples.flatMap((tuple) => {
      const match = /^gc-pressure:[^:]+:(\d+):/u.exec(tuple.operation_key);
      const index = Number(match?.[1]);
      return replacementIndices.has(index) ? [index] : [];
    }),
  );
  const sharedKeys = [...beforeByKey.keys()].filter((key) =>
    afterByKey.has(key),
  );
  const beforeRunIds = new Set(before.tuples.map(({ run_id }) => run_id));
  return (
    afterReplacementIndices.size === replacementIndices.size &&
    [...replacementIndices].every((index) =>
      afterReplacementIndices.has(index),
    ) &&
    sharedKeys.length === before.tuples.length - replacementIndices.size &&
    sharedKeys.every((key) =>
      isDeepStrictEqual(beforeByKey.get(key), afterByKey.get(key)),
    ) &&
    after.tuples
      .filter(({ operation_key }) => !beforeByKey.has(operation_key))
      .every(({ run_id }) => !beforeRunIds.has(run_id))
  );
}

function boundedGcRunIdsAreUnique(mode) {
  const batches = [
    mode?.fill,
    ...(mode?.turnovers ?? []).map(({ replay }) => replay),
  ];
  const runIds = batches.flatMap((evidence) =>
    Array.isArray(evidence?.tuples)
      ? evidence.tuples.map(({ run_id }) => run_id)
      : [],
  );
  return (
    runIds.length ===
      batches.reduce((count, evidence) => count + (evidence?.count ?? 0), 0) &&
    new Set(runIds).size === runIds.length
  );
}

function validateV3GcStage(result, contract, expect) {
  const churn = result?.bounded_churn;
  const pressure = result?.replay_pressure;
  expect(
    isObject(result) &&
      sameMembers(Object.keys(result), ["bounded_churn", "replay_pressure"]) &&
      Array.isArray(churn) &&
      churn.length === 2 &&
      Array.isArray(pressure) &&
      pressure.length === 2,
    "retained-state stage has no two-mode churn/pressure result",
  );
  expect(
    sameMembers(
      (Array.isArray(churn) ? churn : []).map(({ mode }) => mode),
      ["memory_only", "persistent"],
    ) &&
      sameMembers(
        (Array.isArray(pressure) ? pressure : []).map(({ mode }) => mode),
        ["memory_replay_pressure", "persistent_replay_pressure"],
      ),
    "retained-state stage mode sets are not exact",
  );
  for (const mode of Array.isArray(churn) ? churn : []) {
    const persistent = mode?.mode === "persistent";
    const payloadBytes = contract.payload_modes[mode?.mode]?.payload_bytes;
    expect(
      ["memory_only", "persistent"].includes(mode?.mode) &&
        mode.successful_lifecycles ===
          contract.bounded_churn.successful_lifecycles_per_mode &&
        mode.fill_physical_start_delta ===
          contract.bounded_churn.physical_start_deltas.fill &&
        boundedGcRunIdsAreUnique(mode) &&
        validateGcTupleEvidence(mode.fill, {
          payloadBytes,
          exactReplay: true,
          seed: contract.seed,
          mode: mode.mode,
          count: contract.bounded_churn.run_ceiling,
          totalReplayBytes: contract.bounded_churn.run_ceiling * payloadBytes,
          firstIndex: 0,
          minimumIndex: 0,
          maximumIndex:
            contract.bounded_churn.successful_lifecycles_per_mode - 1,
        }) &&
        Array.isArray(mode.turnovers) &&
        mode.turnovers.length === contract.bounded_churn.turnover_windows &&
        mode.turnovers.every((window, index) => {
          const firstIndex =
            contract.bounded_churn.fill_runs +
            index * contract.bounded_churn.replacements_per_window;
          return (
            window.window === index + 1 &&
            window.retained_runs === contract.bounded_churn.run_ceiling &&
            window.physical_start_delta ===
              contract.bounded_churn.physical_start_deltas
                .each_turnover_window &&
            window.retry_physical_start_delta ===
              contract.bounded_churn.physical_start_deltas.retry_wave &&
            window.candidate_selections_delta ===
              contract.bounded_churn.replacements_per_window &&
            window.candidate_evaluations_delta ===
              contract.bounded_churn.replacements_per_window *
                contract.bounded_churn.run_ceiling &&
            window.candidate_fences_delta ===
              contract.bounded_churn.replacements_per_window &&
            window.exact_replacements_delta ===
              contract.bounded_churn.replacements_per_window &&
            validateGcTupleEvidence(window.replay, {
              payloadBytes,
              exactReplay: true,
              seed: contract.seed,
              mode: mode.mode,
              count: contract.bounded_churn.run_ceiling,
              totalReplayBytes:
                contract.bounded_churn.run_ceiling * payloadBytes,
              firstIndex,
              minimumIndex: 0,
              maximumIndex:
                contract.bounded_churn.successful_lifecycles_per_mode - 1,
            })
          );
        }) &&
        (persistent
          ? mode.restart?.after_window ===
              contract.bounded_churn.persistent_restart_after_window &&
            isDeepStrictEqual(
              mode.restart.before,
              mode.turnovers[
                contract.bounded_churn.persistent_restart_after_window - 1
              ]?.replay,
            ) &&
            isDeepStrictEqual(mode.restart.before, mode.restart.after) &&
            validateGcTupleEvidence(mode.restart.before, {
              payloadBytes,
              exactReplay: true,
              seed: contract.seed,
              mode: mode.mode,
              count: contract.bounded_churn.run_ceiling,
              totalReplayBytes:
                contract.bounded_churn.run_ceiling * payloadBytes,
              firstIndex:
                contract.bounded_churn.fill_runs +
                (contract.bounded_churn.persistent_restart_after_window - 1) *
                  contract.bounded_churn.replacements_per_window,
              minimumIndex: 0,
              maximumIndex:
                contract.bounded_churn.successful_lifecycles_per_mode - 1,
            }) &&
            mode.restart.new_incarnation_initial_physical_starts ===
              contract.bounded_churn.physical_start_deltas
                .new_daemon_incarnation_initial
          : mode.restart === null) &&
        validOwnerEpochs(mode.epochs, contract, persistent ? 2 : 1),
      `bounded churn evidence is incomplete for ${String(mode?.mode)}`,
    );
  }
  for (const mode of Array.isArray(pressure) ? pressure : []) {
    const persistent = mode?.mode === "persistent_replay_pressure";
    const budget = contract.replay_pressure.resource_budgets;
    const payloadBytes = contract.payload_modes[mode?.mode]?.payload_bytes;
    expect(
      ["memory_replay_pressure", "persistent_replay_pressure"].includes(
        mode?.mode,
      ) &&
        validateGcTupleEvidence(mode.before, {
          payloadBytes,
          exactReplay: true,
          seed: contract.seed,
          mode: mode.mode,
          count:
            contract.replay_pressure
              .public_replay_verification_runs_before_replacement,
          totalReplayBytes:
            contract.replay_pressure.live_retained_payload_bytes,
          firstIndex: contract.replay_pressure.fill_indices.first,
          minimumIndex: contract.replay_pressure.fill_indices.first,
          maximumIndex: contract.replay_pressure.fill_indices.last,
          maxChunkBytes: persistent
            ? contract.replay_pressure.persistent_native_chunk_max_bytes
            : undefined,
        }) &&
        mode.before?.count ===
          contract.replay_pressure
            .public_replay_verification_runs_before_replacement &&
        mode.before?.total_replay_bytes ===
          contract.replay_pressure.live_retained_payload_bytes &&
        validateGcTupleEvidence(mode.after, {
          payloadBytes,
          exactReplay: true,
          seed: contract.seed,
          mode: mode.mode,
          count:
            contract.replay_pressure
              .public_replay_verification_runs_after_replacement,
          totalReplayBytes:
            contract.replay_pressure.live_retained_payload_bytes,
          minimumIndex: contract.replay_pressure.fill_indices.first,
          maximumIndex: contract.replay_pressure.replacement_indices.last,
          maxChunkBytes: persistent
            ? contract.replay_pressure.persistent_native_chunk_max_bytes
            : undefined,
        }) &&
        mode.after?.count ===
          contract.replay_pressure
            .public_replay_verification_runs_after_replacement &&
        mode.after?.total_replay_bytes ===
          contract.replay_pressure.live_retained_payload_bytes &&
        exactGcReplayTransition(
          mode.before,
          mode.after,
          contract.replay_pressure.replacement_indices.first,
          contract.replay_pressure.replacement_indices.last,
        ) &&
        mode.fill_physical_start_delta ===
          contract.replay_pressure.owner_budgets.physical_starts_fill_delta &&
        mode.replacement_physical_start_delta ===
          contract.replay_pressure.owner_budgets
            .physical_starts_replacement_delta &&
        mode.retry_physical_start_delta ===
          contract.replay_pressure.owner_budgets.physical_starts_retry_delta &&
        mode.max_rss_sample_gap_ms <=
          contract.replay_pressure.sampling.max_rss_sample_gap_ms &&
        finiteNonNegative(mode.peak_rss_kib) &&
        finiteNonNegative(mode.average_cpu_core_percent) &&
        finiteNonNegative(mode.quiescent_cpu_core_percent) &&
        finiteNonNegative(mode.peak_thread_delta) &&
        finiteNonNegative(mode.peak_fd_delta) &&
        validRssSeries(
          mode.rss_samples,
          mode.max_rss_sample_gap_ms,
          contract.replay_pressure.sampling.max_rss_sample_gap_ms,
        ) &&
        mode.peak_rss_kib <=
          (persistent
            ? budget.persistent_peak_rss_kib
            : budget.memory_peak_rss_kib) &&
        mode.average_cpu_core_percent <=
          (persistent
            ? budget.persistent_average_cpu_core_percent
            : budget.memory_average_cpu_core_percent) &&
        mode.quiescent_cpu_core_percent <= budget.quiescent_cpu_core_percent &&
        mode.peak_thread_delta <= budget.peak_thread_delta &&
        mode.peak_fd_delta <= budget.peak_fd_delta &&
        (persistent
          ? mode.recovered?.replay?.count ===
              contract.replay_pressure
                .require_exact_retained_run_and_key_count &&
            validateGcTupleEvidence(mode.recovered.replay, {
              payloadBytes,
              exactReplay: false,
              seed: contract.seed,
              mode: mode.mode,
              count:
                contract.replay_pressure
                  .require_exact_retained_run_and_key_count,
              minimumIndex: contract.replay_pressure.fill_indices.first,
              maximumIndex: contract.replay_pressure.replacement_indices.last,
              live: mode.after,
              maxChunkBytes:
                contract.replay_pressure.persistent_native_chunk_max_bytes,
            }) &&
            mode.recovered.replay.total_replay_bytes >=
              contract.replay_pressure.persistent_recovered_replay_min_bytes &&
            mode.recovered.replay.total_replay_bytes <=
              contract.replay_pressure.persistent_durable_replay_max_bytes &&
            mode.recovered.retry_physical_start_delta === 0 &&
            finiteNonNegative(mode.recovered.steady_rss_kib) &&
            mode.recovered.steady_rss_kib <=
              budget.persistent_recovered_steady_rss_kib &&
            finiteNonNegative(mode.recovered.peak_rss_kib) &&
            mode.recovered.peak_rss_kib <=
              budget.persistent_recovered_peak_rss_kib &&
            finiteNonNegative(mode.recovered.quiescent_cpu_core_percent) &&
            mode.recovered.quiescent_cpu_core_percent <=
              budget.quiescent_cpu_core_percent &&
            finiteNonNegative(mode.recovered.quiescent_thread_delta) &&
            mode.recovered.quiescent_thread_delta <=
              budget.quiescent_thread_delta &&
            finiteNonNegative(mode.recovered.quiescent_fd_delta) &&
            mode.recovered.quiescent_fd_delta <= budget.quiescent_fd_delta &&
            validRssSeries(
              mode.recovered.rss_samples,
              mode.recovered.max_rss_sample_gap_ms,
              contract.replay_pressure.sampling.max_rss_sample_gap_ms,
            )
          : mode.recovered === null) &&
        validOwnerEpochs(mode.epochs, contract, persistent ? 2 : 1),
      `replay-pressure evidence is incomplete for ${String(mode?.mode)}`,
    );
  }
}

function validRssSeries(samples, recordedMaxGap, contractMaxGap) {
  if (!Array.isArray(samples) || samples.length < 2) return false;
  const gaps = samples
    .slice(1)
    .map(
      (sample, index) => sample?.timestamp_ms - samples[index]?.timestamp_ms,
    );
  return (
    samples.every(
      (sample) =>
        Number.isSafeInteger(sample?.timestamp_ms) &&
        finiteNonNegative(sample?.rss_kib),
    ) &&
    gaps.every((gap) => Number.isFinite(gap) && gap >= 0) &&
    Math.max(...gaps) === recordedMaxGap &&
    recordedMaxGap <= contractMaxGap
  );
}

function validOwnerEpochs(epochs, contract, expectedCount) {
  const boundaries = [
    "creation_flights",
    "publication_reservations",
    "collecting_tickets",
    "overlap_owners",
    "cleanup_owners",
    "direct_children",
    "readers",
    "waiters",
    "input_drains",
    "attachments",
    "tmux_owners",
  ];
  const highWater = {
    direct_children: "max_children",
    readers: "max_readers",
    waiters: "max_waiters",
    publication_reservations: "max_publication_reservations",
    collecting_tickets: "max_collecting_tickets",
    overlap_owners: "max_overlap_owners",
    attachments: "max_attachments_during_replay",
  };
  return (
    Array.isArray(epochs) &&
    epochs.length === expectedCount &&
    new Set(epochs.map(({ daemon_instance }) => daemon_instance)).size ===
      epochs.length &&
    epochs.every(
      (epoch) =>
        epoch?.current?.retained_runs === contract.bounded_churn.run_ceiling &&
        epoch?.current?.creation_keys === contract.bounded_churn.run_ceiling &&
        boundaries.every((name) => epoch?.current?.[name] === 0) &&
        Object.entries(highWater).every(
          ([name, budget]) =>
            finiteNonNegative(epoch?.high_water?.[name]) &&
            epoch.high_water[name] <=
              contract.replay_pressure.owner_budgets[budget],
        ) &&
        epoch?.high_water?.tmux_owners === 0 &&
        epoch?.cumulative?.candidate_evaluations_max <=
          contract.bounded_churn.run_ceiling,
    )
  );
}

function validFileIdentity(value) {
  return (
    isObject(value) &&
    sameMembers(Object.keys(value), ["path", "sha256"]) &&
    canonicalFixturePath(value.path) &&
    HASH_PATTERN.test(value.sha256 ?? "")
  );
}

function currentQualificationIdentity(root, source, budgets) {
  const identity = (filePath) => ({
    path: filePath,
    sha256: crypto
      .createHash("sha256")
      .update(fs.readFileSync(path.join(root, filePath)))
      .digest("hex"),
  });
  return {
    ...source,
    harness: identity("scripts/reliability-qualification.ts"),
    launcher: identity("scripts/check-reliability.sh"),
    daemon: identity("target/reliability/provenance-build/debug/ctxmuxd"),
    rss_sampler: identity(
      "target/reliability/provenance-build/debug/ctxmux-rss-sampler",
    ),
    rss_sampler_sources: [
      identity("crates/ctxmux-rss-sampler/src/main.rs"),
      identity("crates/ctxmux-process-stats/src/lib.rs"),
    ],
    lockfiles: [identity("Cargo.lock"), identity("package-lock.json")],
    measurement_contract_sha256: crypto
      .createHash("sha256")
      .update(JSON.stringify(budgets.measurement_contract))
      .digest("hex"),
  };
}

function finiteNonNegative(value) {
  return Number.isFinite(value) && value >= 0;
}

function hasExactEntries(value, expected) {
  return (
    isObject(value) &&
    sameMembers(Object.keys(value), Object.keys(expected)) &&
    Object.entries(expected).every(([key, expectedValue]) =>
      Object.is(value[key], expectedValue),
    )
  );
}

function hasCanonicalReliabilityTriggers(workflowObject) {
  const triggers = workflowObject?.on;
  const schedule = triggers?.schedule;
  const dispatch = triggers?.workflow_dispatch;
  const inputs = dispatch?.inputs;
  const qualification = inputs?.qualification;
  return (
    isObject(triggers) &&
    sameMembers(Object.keys(triggers), ["schedule", "workflow_dispatch"]) &&
    Array.isArray(schedule) &&
    schedule.length === 1 &&
    hasExactEntries(schedule[0], { cron: "17 3 * * *" }) &&
    isObject(dispatch) &&
    sameMembers(Object.keys(dispatch), ["inputs"]) &&
    isObject(inputs) &&
    sameMembers(Object.keys(inputs), ["qualification"]) &&
    isObject(qualification) &&
    sameMembers(Object.keys(qualification), [
      "description",
      "required",
      "default",
      "type",
      "options",
    ]) &&
    qualification.description === "Reliability qualification lane" &&
    qualification.required === true &&
    qualification.default === "nightly" &&
    qualification.type === "choice" &&
    Array.isArray(qualification.options) &&
    sameMembers(qualification.options, ["nightly", "release"])
  );
}

function hasCanonicalLaneSteps(workflowJob, lane, commandStep) {
  const steps = workflowJob?.steps;
  if (!Array.isArray(steps) || steps.length !== 6) return false;
  const [, setupNode, installRust, installJavaScript, run, upload] = steps;
  return (
    run === commandStep &&
    canonicalCheckoutPrecedesCommand(workflowJob, commandStep, 0) &&
    isObject(setupNode) &&
    sameMembers(Object.keys(setupNode), ["uses", "with"]) &&
    setupNode.uses === "actions/setup-node@v4" &&
    hasExactEntries(setupNode.with, { "node-version": 24, cache: "npm" }) &&
    hasExactEntries(installRust, {
      name: "Install Rust toolchain",
      run: "rustup show active-toolchain",
    }) &&
    hasExactEntries(installJavaScript, {
      name: "Install JavaScript dependencies",
      run: "npm ci --ignore-scripts",
    }) &&
    isObject(run) &&
    sameMembers(Object.keys(run), ["name", "env", "run"]) &&
    run.name === lane.name &&
    run.run === lane.command &&
    hasExactEntries(run.env, { BASH_ENV: "/dev/null", ENV: "/dev/null" }) &&
    isObject(upload) &&
    sameMembers(Object.keys(upload), ["name", "if", "uses", "with"]) &&
    upload.name === lane.uploadName &&
    upload.if === "always()" &&
    upload.uses === "actions/upload-artifact@v4" &&
    hasExactEntries(upload.with, {
      name: lane.artifactName,
      path: lane.artifactPath,
      "if-no-files-found": "error",
      "retention-days": lane.retentionDays,
    })
  );
}

export function validateReliabilityPolicy({
  budgets,
  baselineReceipts,
  sourceSnapshots = [],
  currentPolicyHashes,
  workflow,
  checkScript,
  qualificationScript,
  harnessSource,
}) {
  const errors = [];
  const qualificationPolicy = qualificationPolicyFromHarness(
    harnessSource,
    errors,
  );
  validateQualificationPolicyContract(qualificationPolicy, errors);
  validateBudgetShape(budgets, errors);
  const references = validateReceiptReferences(budgets, errors);
  const receipts = resolveReceipts(references, baselineReceipts, errors);
  if (receipts.length === 3) {
    const schemas = new Set(receipts.map(({ value }) => value?.schema));
    if (schemas.size !== 1) {
      errors.push("observation baseline must not mix receipt generations");
    } else if (schemas.has("ctxmux.reliability-qualification.v2")) {
      validateSourceBoundBaseline({
        budgets,
        receipts,
        sourceSnapshots,
        currentPolicyHashes,
        errors,
      });
    } else {
      errors.push(
        `unsupported observation receipt schema ${[...schemas][0]}; the frozen baseline requires source-bound v2 receipts`,
      );
    }
  }
  validateReachability(
    {
      workflow,
      checkScript,
      qualificationScript,
      harnessSource,
    },
    errors,
  );
  return errors;
}

function validateBudgetShape(budgets, errors) {
  if (budgets.schema !== "ctxmux.reliability-budgets.v1") {
    errors.push(`unsupported reliability budget schema ${budgets.schema}`);
  }
  if (budgets.frozen_before_optimization !== true) {
    errors.push("resource budgets must be frozen before optimization");
  }
  if (!validTimestamp(budgets.frozen_at)) {
    errors.push("resource budgets need a valid frozen_at timestamp");
  }
  const baseline = budgets.observation_baseline;
  if (!isObject(baseline) || baseline.profile !== "observe") {
    errors.push("resource budgets need an observe baseline");
  }
  if (baseline?.rounds !== 3) {
    errors.push("resource budgets require exactly three observe rounds");
  }
  if (
    baseline?.resource_start_concurrency !== 8 ||
    baseline?.peak_rss_sample_interval_ms !== 25
  ) {
    errors.push(
      "observation baseline must use concurrency 8 and 25 ms RSS samples",
    );
  }
  if (!sameMembers(Object.keys(budgets.budgets ?? {}), MODES)) {
    errors.push("resource budgets must cover exactly idle and active modes");
  }
  for (const mode of MODES) {
    const byCount = budgets.budgets?.[mode] ?? {};
    if (!sameMembers(Object.keys(byCount), COUNTS)) {
      errors.push(`${mode} budgets must cover exactly 1/32/128 Runs`);
    }
    for (const count of COUNTS) {
      const budget = byCount[count];
      if (!isObject(budget)) {
        errors.push(`${mode}/${count} budget must be an object`);
        continue;
      }
      if (!sameMembers(Object.keys(budget), BUDGET_FIELDS)) {
        errors.push(
          `${mode}/${count} has an incomplete or unknown budget field`,
        );
      }
      for (const field of BUDGET_FIELDS) {
        if (!finiteNonNegative(budget[field])) {
          errors.push(
            `${mode}/${count} ${field} must be finite and non-negative`,
          );
        }
      }
    }
  }
  const contract = budgets.measurement_contract;
  if (
    !isObject(contract) ||
    !sameMembers(Object.keys(contract), ["cpu", "rss", "slopes", "cleanup"]) ||
    !Object.values(contract).every(
      (value) => typeof value === "string" && value.trim().length > 0,
    )
  ) {
    errors.push(
      "measurement contract must contain non-empty cpu/rss/slopes/cleanup",
    );
  }
}

function validateReceiptReferences(budgets, errors) {
  const references = budgets.observation_baseline?.raw_receipts;
  if (!Array.isArray(references) || references.length !== 3) {
    errors.push(
      "observation baseline must retain exactly three raw receipt refs",
    );
    return [];
  }
  const paths = [];
  const hashes = [];
  for (const [index, reference] of references.entries()) {
    if (!isObject(reference)) {
      errors.push(`raw observation receipt ref ${index + 1} must be an object`);
      continue;
    }
    if (
      !canonicalFixturePath(reference.path) ||
      reference.path !== EXPECTED_RECEIPT_PATHS[index]
    ) {
      errors.push(
        `raw observation receipt path is not canonical: ${reference.path}`,
      );
    }
    if (!HASH_PATTERN.test(reference.sha256 ?? "")) {
      errors.push(
        `raw observation receipt ${reference.path} needs exact SHA-256`,
      );
    }
    paths.push(reference.path);
    hashes.push(reference.sha256);
  }
  if (new Set(paths).size !== paths.length) {
    errors.push("raw observation receipt paths must be unique");
  }
  if (new Set(hashes).size !== hashes.length) {
    errors.push("raw observation receipt hashes must be unique");
  }
  return references.filter(isObject);
}

function resolveReceipts(references, receipts, errors) {
  if (
    !Array.isArray(receipts) ||
    receipts.some((receipt) => !isObject(receipt))
  ) {
    errors.push("loaded raw observation receipts must be objects");
    return [];
  }
  const paths = receipts.map(({ path: receiptPath }) => receiptPath);
  if (new Set(paths).size !== paths.length) {
    errors.push("loaded raw observation receipts contain a duplicate receipt");
  }
  const ordered = [];
  for (const reference of references) {
    const matches = receipts.filter(
      (receipt) => receipt.path === reference.path,
    );
    if (matches.length !== 1) {
      errors.push(
        `raw observation receipt is missing or duplicated: ${reference.path}`,
      );
      continue;
    }
    if (matches[0].sha256 !== reference.sha256) {
      errors.push(`raw observation receipt hash drifted: ${reference.path}`);
    }
    ordered.push(matches[0]);
  }
  if (receipts.length !== references.length) {
    errors.push("loaded raw observation receipts must match the declared refs");
  }
  return ordered;
}

function validateReachability(
  { workflow, checkScript, qualificationScript },
  errors,
) {
  const smokeCommand = "scripts/check-reliability.sh --profile smoke";
  const completionStart = "ctxmux_check_completed=false";
  const completionGuard = "trap ctxmux_check_completion_guard EXIT";
  const completionEnd = "ctxmux_check_completed=true";
  const coreCompletion = `printf '%s\\n' "$ctxmux_check_completion_nonce" > "$ctxmux_check_completion_marker"`;
  const checkLines =
    checkScript?.split(/\r?\n/u).map((line) => line.trim()) ?? [];
  const executableCheckLines = checkLines.filter((line) => line.length > 0);
  const checkPrefix = [
    "#!/usr/bin/env bash",
    "set -euo pipefail",
    'cd "$(dirname "${BASH_SOURCE[0]}")/.."',
    completionStart,
    "ctxmux_check_state_dir=",
    "ctxmux_check_completion_marker=",
    "ctxmux_check_completion_nonce=",
    "ctxmux_check_cleanup() {",
    "if [[ -n $ctxmux_check_completion_marker ]]",
    "then",
    'rm -f -- "$ctxmux_check_completion_marker"',
    "ctxmux_check_completion_marker=",
    "fi",
    "if [[ -n $ctxmux_check_state_dir ]]",
    "then",
    'rmdir -- "$ctxmux_check_state_dir" 2>/dev/null || true',
    "ctxmux_check_state_dir=",
    "fi",
    "}",
    "ctxmux_check_completion_guard() {",
    "ctxmux_check_exit_status=$?",
    "trap - EXIT",
    "ctxmux_check_cleanup",
    "if [[ $ctxmux_check_completed != true && $ctxmux_check_exit_status -eq 0 ]]",
    "then",
    'echo "repository check exited before its final reliability smoke" >&2',
    "exit 1",
    "fi",
    'exit "$ctxmux_check_exit_status"',
    "}",
    completionGuard,
    "ctxmux_check_core() (",
  ];
  const supervisorTail = [
    'ctxmux_check_state_dir=$(mktemp -d "${TMPDIR:-/tmp}/ctxmux-check.XXXXXX")',
    "ctxmux_check_completion_marker=$ctxmux_check_state_dir/completed",
    'ctxmux_check_completion_nonce="$$-$RANDOM-$RANDOM"',
    "set +e",
    'ctxmux_check_core "$@"',
    "ctxmux_check_core_status=$?",
    "set -e",
    "if [[ $ctxmux_check_core_status -ne 0 ]]",
    "then",
    'echo "repository check core did not reach its completion boundary" >&2',
    "ctxmux_check_cleanup",
    'exit "$ctxmux_check_core_status"',
    "fi",
    'if [[ ! -f $ctxmux_check_completion_marker || $(< "$ctxmux_check_completion_marker") != "$ctxmux_check_completion_nonce" ]]',
    "then",
    'echo "repository check core did not publish its completion token" >&2',
    "ctxmux_check_cleanup",
    "exit 1",
    "fi",
    "ctxmux_check_cleanup",
    smokeCommand,
    completionEnd,
  ];
  const supervisorStart = executableCheckLines.lastIndexOf(supervisorTail[0]);
  const coreStart = executableCheckLines.indexOf("ctxmux_check_core() (");
  const coreEnd = executableCheckLines.indexOf(coreCompletion);
  const coreHash =
    coreStart >= 0 && coreEnd > coreStart
      ? crypto
          .createHash("sha256")
          .update(executableCheckLines.slice(coreStart + 1, coreEnd).join("\n"))
          .digest("hex")
      : undefined;
  if (
    checkLines.filter((line) => line === smokeCommand).length !== 1 ||
    checkLines.filter((line) => line === completionStart).length !== 1 ||
    checkLines.filter((line) => line === completionGuard).length !== 1 ||
    checkLines.filter((line) => line === completionEnd).length !== 1 ||
    checkLines.filter((line) => line === "ctxmux_check_core() (").length !==
      1 ||
    checkLines.filter((line) => line === coreCompletion).length !== 1 ||
    !checkLines.includes("ctxmux_check_completion_guard() {") ||
    !checkLines.includes(
      'echo "repository check exited before its final reliability smoke" >&2',
    ) ||
    !checkLines.includes("trap - EXIT") ||
    !checkLines.includes("exit 1") ||
    !(
      checkLines.indexOf(completionStart) <
        checkLines.indexOf(completionGuard) &&
      checkLines.indexOf(completionGuard) <
        checkLines.indexOf("ctxmux_check_core() (") &&
      checkLines.indexOf("ctxmux_check_core() (") <
        checkLines.indexOf(coreCompletion)
    ) ||
    executableCheckLines[executableCheckLines.indexOf(coreCompletion) + 1] !==
      ")" ||
    executableCheckLines[executableCheckLines.indexOf(coreCompletion) + 2] !==
      supervisorTail[0] ||
    coreHash !== EXPECTED_CHECK_CORE_SHA256 ||
    executableCheckLines.indexOf("ctxmux_check_core() (") !==
      executableCheckLines.indexOf(completionGuard) + 1 ||
    !isDeepStrictEqual(
      executableCheckLines.slice(0, checkPrefix.length),
      checkPrefix,
    ) ||
    supervisorStart < 0 ||
    !isDeepStrictEqual(
      executableCheckLines.slice(supervisorStart),
      supervisorTail,
    )
  ) {
    errors.push(
      "the required check does not reach reliability smoke through its completion exit guard",
    );
  }
  const qualificationLines =
    qualificationScript?.split(/\r?\n/).map((line) => line.trim()) ?? [];
  const executableQualificationLines = qualificationLines.filter(
    (line) => line.length > 0,
  );
  const qualificationLauncherHash = crypto
    .createHash("sha256")
    .update(executableQualificationLines.join("\n"))
    .digest("hex");
  if (qualificationLauncherHash !== EXPECTED_QUALIFICATION_LAUNCHER_SHA256) {
    errors.push(
      "the reliability qualification must match its complete launcher envelope",
    );
  }
  const policyLine = qualificationLines.indexOf(
    "node scripts/reliability-policy.mjs",
  );
  const buildLine = qualificationLines.indexOf(
    '"${ctxmux_reliability_build_argv[@]}"',
  );
  if (policyLine < 0 || buildLine < 0 || policyLine >= buildLine) {
    errors.push(
      "the reliability qualification does not validate its policy before its locked build",
    );
  }
  const workflowObject = parseWorkflow(workflow ?? "", errors);
  if (!hasCanonicalReliabilityTriggers(workflowObject)) {
    errors.push(
      "reliability workflow must bind the canonical schedule and dispatch qualification input",
    );
  }
  for (const lane of [
    {
      id: "reliability-nightly",
      name: "Run nightly reliability and GC qualification",
      command: "scripts/check-reliability.sh --profile nightly",
      condition:
        "github.event_name == 'schedule' || inputs.qualification == 'nightly'",
      timeoutMinutes: 90,
      environment: {
        CTXMUX_RELIABILITY_ARTIFACT_DIR:
          "${{ github.workspace }}/target/reliability/nightly",
        CTXMUX_RELIABILITY_EVIDENCE:
          "${{ github.workspace }}/target/reliability/nightly/result.json",
      },
      uploadName: "Preserve nightly receipt and daemon logs",
      artifactName: "reliability-nightly-${{ matrix.os }}-${{ github.run_id }}",
      artifactPath: "target/reliability/nightly",
      retentionDays: 30,
    },
    {
      id: "release-soak",
      name: "Run two-hour release soak inside a three-hour qualification budget",
      command: "scripts/check-reliability.sh --profile release",
      condition:
        "github.event_name == 'workflow_dispatch' && inputs.qualification == 'release'",
      timeoutMinutes: 210,
      environment: {
        CTXMUX_RELIABILITY_ARTIFACT_DIR:
          "${{ github.workspace }}/target/reliability/release",
        CTXMUX_RELIABILITY_EVIDENCE:
          "${{ github.workspace }}/target/reliability/release/result.json",
      },
      uploadName: "Preserve release receipt and daemon logs",
      artifactName: "reliability-release-${{ matrix.os }}-${{ github.run_id }}",
      artifactPath: "target/reliability/release",
      retentionDays: 90,
    },
  ]) {
    const workflowJob = workflowObject?.jobs?.[lane.id];
    const commandSteps = Array.isArray(workflowJob?.steps)
      ? workflowJob.steps.filter((step) => step?.run === lane.command)
      : [];
    const commandStep = commandSteps[0];
    const strategy = workflowJob?.strategy;
    const matrix = strategy?.matrix;
    if (
      Object.hasOwn(workflowObject ?? {}, "defaults") ||
      Object.hasOwn(workflowObject ?? {}, "env") ||
      Object.hasOwn(workflowJob ?? {}, "defaults") ||
      !sameMembers(Object.keys(workflowJob ?? {}), [
        "if",
        "strategy",
        "runs-on",
        "timeout-minutes",
        "env",
        "steps",
      ]) ||
      workflowJob?.if !== lane.condition ||
      workflowJob?.["runs-on"] !== "${{ matrix.os }}" ||
      workflowJob?.["timeout-minutes"] !== lane.timeoutMinutes ||
      !hasExactEntries(workflowJob?.env, lane.environment) ||
      !isObject(strategy) ||
      !sameMembers(Object.keys(strategy), ["fail-fast", "matrix"]) ||
      strategy["fail-fast"] !== false ||
      !isObject(matrix) ||
      !sameMembers(Object.keys(matrix), ["os"]) ||
      !Array.isArray(matrix.os) ||
      !sameMembers(matrix.os, ["ubuntu-latest", "macos-latest"]) ||
      commandSteps.length !== 1 ||
      !commandStep ||
      !hasCanonicalLaneSteps(workflowJob, lane, commandStep)
    ) {
      errors.push(
        `${lane.id} must use its exact canonical step envelope and unconditional qualification command`,
      );
    }
  }
  if (/continue-on-error\s*:\s*true/u.test(workflow)) {
    errors.push("reliability evidence must not continue on error");
  }
}

export function loadBaselineReceipts(root, budgets) {
  return (budgets.observation_baseline?.raw_receipts ?? []).map(
    (reference, index) => {
      if (
        !canonicalFixturePath(reference?.path) ||
        reference.path !== EXPECTED_RECEIPT_PATHS[index] ||
        !HASH_PATTERN.test(reference?.sha256 ?? "")
      ) {
        throw new Error(
          `refusing to read non-canonical receipt ${reference?.path}`,
        );
      }
      const bytes = fs.readFileSync(path.join(root, reference.path));
      return {
        path: reference.path,
        sha256: crypto.createHash("sha256").update(bytes).digest("hex"),
        value: JSON.parse(bytes.toString("utf8")),
      };
    },
  );
}

export function loadSourceSnapshots(root, receipts) {
  return receipts
    .filter(
      ({ value }) => value?.schema === "ctxmux.reliability-qualification.v2",
    )
    .map((receipt) => loadSourceSnapshot(root, receipt));
}

function loadSourceSnapshot(root, receipt) {
  const commit = receipt.value?.provenance?.source?.commit;
  if (!GIT_OBJECT_PATTERN.test(commit ?? "")) {
    return { path: receipt.path, commit, error: "invalid source commit" };
  }
  const existence = git(root, ["cat-file", "-e", `${commit}^{commit}`]);
  if (existence.status !== 0) {
    return { path: receipt.path, commit, error: existence.error };
  }
  const reachability = git(
    root,
    ["merge-base", "--is-ancestor", commit, "HEAD"],
    [0, 1],
  );
  if (reachability.status !== 0) {
    return {
      path: receipt.path,
      commit,
      reachableFromHead: false,
      error: reachability.status === 1 ? undefined : reachability.error,
    };
  }
  const tree = git(root, ["rev-parse", `${commit}^{tree}`]);
  if (tree.status !== 0) {
    return { path: receipt.path, commit, error: tree.error };
  }
  const fileHashes = {};
  for (const filePath of SNAPSHOT_FILE_PATHS) {
    const result = git(root, ["show", `${commit}:${filePath}`]);
    if (result.status !== 0) {
      return { path: receipt.path, commit, error: result.error };
    }
    fileHashes[filePath] = crypto
      .createHash("sha256")
      .update(result.stdout)
      .digest("hex");
  }
  return {
    path: receipt.path,
    commit,
    reachableFromHead: true,
    tree: tree.stdout.toString("utf8").trim(),
    fileHashes,
  };
}

function git(root, args, acceptedStatuses = [0]) {
  const result = spawnSync("git", args, {
    cwd: root,
    encoding: null,
    maxBuffer: 16 * 1024 * 1024,
    timeout: 2_000,
  });
  const status = result.status ?? -1;
  return {
    status,
    stdout: result.stdout ?? Buffer.alloc(0),
    error: acceptedStatuses.includes(status)
      ? undefined
      : result.stderr?.toString("utf8").trim() ||
        result.error?.message ||
        `git ${args[0]} failed`,
  };
}

function currentPolicyHashes(root) {
  return Object.fromEntries(
    POLICY_SOURCE_PATHS.map((filePath) => [
      filePath,
      crypto
        .createHash("sha256")
        .update(fs.readFileSync(path.join(root, filePath)))
        .digest("hex"),
    ]),
  );
}

function main() {
  const args = process.argv.slice(2);
  if (args[0] === "--prepare-qualification-evidence") {
    prepareQualificationEvidence(args);
    return;
  }
  if (args[0] === "--qualification-receipt") {
    verifyQualificationReceipt(args);
    return;
  }
  if (args.length > 1) {
    throw new Error("usage: reliability-policy.mjs [root]");
  }
  const root = path.resolve(args[0] ?? ".");
  const budgets = JSON.parse(
    fs.readFileSync(path.join(root, "reliability-budgets.json"), "utf8"),
  );
  const baselineReceipts = loadBaselineReceipts(root, budgets);
  const errors = validateReliabilityPolicy({
    budgets,
    baselineReceipts,
    sourceSnapshots: loadSourceSnapshots(root, baselineReceipts),
    currentPolicyHashes: currentPolicyHashes(root),
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
  });
  if (errors.length > 0) {
    for (const error of errors) console.error(`Reliability policy: ${error}`);
    process.exitCode = 1;
  } else {
    console.log(
      "Reliability policy: source-bound v2 observations and deterministic ceilings are valid",
    );
  }
}

function prepareQualificationEvidence(args) {
  if (
    args.length !== 4 ||
    args[2] !== "--profile" ||
    !["smoke", "nightly", "release", "observe"].includes(args[3])
  ) {
    throw new Error(
      "usage: reliability-policy.mjs --prepare-qualification-evidence <path> --profile <smoke|nightly|release|observe>",
    );
  }
  const prepared = prepareQualificationEvidencePath(
    process.cwd(),
    args[1],
    args[3],
  );
  console.log(JSON.stringify(prepared.preflight));
}

function verifyQualificationReceipt(args) {
  if (
    args.length !== 6 ||
    args[2] !== "--profile" ||
    !["smoke", "nightly", "release", "observe"].includes(args[3]) ||
    args[4] !== "--preflight"
  ) {
    throw new Error(
      "usage: reliability-policy.mjs --qualification-receipt <path> --profile <smoke|nightly|release|observe> --preflight <token>",
    );
  }
  const root = process.cwd();
  const requestedPath = args[1];
  const expectedProfile = args[3];
  const preflight = parseQualificationPreflight(args[5], expectedProfile);
  assertReliabilityGcIdentities(loadReliabilityGcContract(root), preflight);
  const receiptPath = requestedPath;
  const resolvedReceiptPath = resolveCanonicalQualificationEvidencePath(
    root,
    requestedPath,
    expectedProfile,
  );
  const budgets = JSON.parse(
    fs.readFileSync(path.join(root, "reliability-budgets.json"), "utf8"),
  );
  enterCanonicalArtifactOwner({
    root,
    profile: expectedProfile,
    expectedIdentity: preflight.artifact_owner_identity,
    create: false,
  });
  const {
    value,
    identity: receiptIdentity,
    sha256: receiptSha256,
  } = readOwnedJson("result.json");
  const policyErrors = [];
  const qualificationPolicy = qualificationPolicyFromHarness(
    fs.readFileSync(
      path.join(root, "scripts", "reliability-qualification.ts"),
      "utf8",
    ),
    policyErrors,
  );
  validateQualificationPolicyContract(qualificationPolicy, policyErrors);
  const head = git(root, ["rev-parse", "HEAD"]);
  const tree = git(root, ["rev-parse", "HEAD^{tree}"]);
  const status = git(root, [
    "status",
    "--porcelain=v1",
    "--untracked-files=all",
  ]);
  const currentCommit = head.stdout.toString("utf8").trim();
  const currentTree = tree.stdout.toString("utf8").trim();
  if (head.status !== 0 || tree.status !== 0 || status.status !== 0) {
    policyErrors.push(
      "qualification receipt could not resolve the current source invocation",
    );
  } else {
    policyErrors.push(
      ...validateQualificationInvocationIdentity(value, {
        commit: currentCommit,
        tree: currentTree,
        clean: status.stdout.length === 0,
      }),
    );
  }
  const receiptValidation = {
    receiptPath,
    value,
    snapshot: loadSourceSnapshot(root, { path: receiptPath, value }),
    budgets,
    policyContracts: budgets.observation_baseline?.policy_contracts ?? [],
    notBefore: preflight.not_before,
    verifiedAt: new Date().toISOString(),
    invocationNonce: preflight.invocation_nonce,
  };
  const gc = loadReliabilityGcContract(root);
  let semanticErrors;
  if (value?.schema !== "ctxmux.reliability-qualification.v3") {
    semanticErrors = [
      "current qualification receipts must use the source-bound v3 schema",
    ];
  } else {
    semanticErrors = validatePassingQualificationReceiptV3({
      ...receiptValidation,
      expectedProfile,
      qualificationPolicy,
      gc,
      preflight,
      budgets,
      current: currentQualificationIdentity(
        root,
        {
          commit: currentCommit,
          tree: currentTree,
          clean: status.stdout.length === 0,
        },
        budgets,
      ),
    });
  }
  const errors = [
    ...policyErrors,
    ...semanticErrors,
    ...validateQualificationArtifacts({
      root,
      resolvedReceiptPath,
      value,
      expectedProfile,
      expectedReceiptIdentity: receiptIdentity,
      expectedReceiptSha256: receiptSha256,
      preexistingReceiptIdentity: preflight.preexisting_receipt_identity,
    }),
  ];
  if (errors.length > 0) {
    for (const error of errors) console.error(`Reliability receipt: ${error}`);
    process.exitCode = 1;
    return;
  }
  console.log(
    `Reliability receipt: ${expectedProfile} qualification passed with source-bound evidence`,
  );
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  main();
}
