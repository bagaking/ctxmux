import crypto from "node:crypto";
import path from "node:path";
import { isDeepStrictEqual } from "node:util";

import {
  COUNTS,
  deriveBudgetCeiling,
  deriveObservedMaxima,
  MODES,
  OBSERVED_FIELDS,
} from "./reliability-budget-contract.mjs";

export const EXPECTED_RECEIPT_PATHS = [
  "fixtures/reliability/observe-darwin-arm64-r1.json",
  "fixtures/reliability/observe-darwin-arm64-r2.json",
  "fixtures/reliability/observe-darwin-arm64-r3.json",
];
export const SOURCE_FILE_PATHS = [
  "scripts/reliability-qualification.ts",
  "scripts/check-reliability.sh",
  "Cargo.lock",
  "package-lock.json",
];
export const POLICY_SOURCE_PATHS = ["scripts/reliability-budget-contract.mjs"];
export const SNAPSHOT_FILE_PATHS = [
  ...SOURCE_FILE_PATHS,
  ...POLICY_SOURCE_PATHS,
];
export const HASH_PATTERN = /^[0-9a-f]{64}$/u;

const GIT_OBJECT_PATTERN = /^[0-9a-f]{40}$/u;
const EXPECTED_STAGE_IDS =
  "chaos-owner-matrix security-negative-space stress-and-soak resource-census".split(
    " ",
  );
const QUALIFICATION_STAGE_IDS = [
  ...EXPECTED_STAGE_IDS,
  "frozen-resource-budgets",
];
const PASSING_TRACE_ACTIONS = new Set([
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
]);
const FIXED_BUILD_TARGET = "target/reliability/provenance-build";
const FIXED_DAEMON_PATH = `${FIXED_BUILD_TARGET}/debug/ctxmuxd`;
const FIXED_BUILD_ARGV = [
  "cargo",
  "build",
  "--locked",
  "--quiet",
  "--package",
  "ctxmux-daemon",
  "--target-dir",
  FIXED_BUILD_TARGET,
];
const RECEIPT_FIELDS =
  "schema status profile observation_round seed recorded_at completed_at time_budget_seconds environment provenance declared_limits action_trace stages daemon_logs error".split(
    " ",
  );
const WORKLOAD_FIELDS =
  "frame_bytes retained_output_bytes_per_run live_event_capacity global_run_quota global_attachment_quota exited_run_gc qualification_stage resource_counts resource_modes resource_start_concurrency peak_rss_sample_interval_ms soak_seconds seed_controls note".split(
    " ",
  );
const PROVENANCE_FIELDS =
  "claim_scope binary_source_attestation source harness launcher daemon lockfiles build toolchain measurement_contract_encoding measurement_contract_sha256".split(
    " ",
  );
const RESOURCE_FIELDS =
  "mode runs baseline steady cleanup peak_rss_kib peak_rss_sample_count peak_rss_sample_interval_ms cpu_core_percent retained_output_bytes retained_output_bytes_per_run rss_kib_per_run threads_per_run fds_per_run cleanup_rss_kib_delta cleanup_fds_delta cleanup_retained_runs cleanup_live_children cleanup_attachments intentional_retained_state_without_gc".split(
    " ",
  );
const RESOURCE_NUMERIC_FIELDS =
  "peak_rss_kib cpu_core_percent retained_output_bytes retained_output_bytes_per_run rss_kib_per_run threads_per_run fds_per_run cleanup_rss_kib_delta cleanup_fds_delta cleanup_retained_runs cleanup_live_children cleanup_attachments".split(
    " ",
  );

export function sameMembers(left, right) {
  return (
    left.length === right.length &&
    [...left].sort().every((value, index) => value === [...right].sort()[index])
  );
}

export function isObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

export function canonicalFixturePath(value) {
  return (
    typeof value === "string" &&
    value.length > 0 &&
    !value.includes("\\") &&
    !path.posix.isAbsolute(value) &&
    !path.win32.isAbsolute(value) &&
    !value.split("/").includes("..") &&
    path.posix.normalize(value) === value
  );
}

function exactObject(value, keys, label, errors) {
  if (!isObject(value) || !sameMembers(Object.keys(value), keys)) {
    errors.push(`${label} must contain exactly ${keys.join(", ")}`);
    return false;
  }
  return true;
}

function expect(errors, condition, message) {
  if (!condition) errors.push(message);
  return condition;
}

function finiteNonNegative(value) {
  return Number.isFinite(value) && value >= 0;
}

function positiveInteger(value) {
  return Number.isInteger(value) && value > 0;
}

function nonEmptyString(value) {
  return typeof value === "string" && value.trim().length > 0;
}

function validTimestamp(value) {
  return typeof value === "string" && Number.isFinite(Date.parse(value));
}

export function validateSourceBoundBaseline({
  budgets,
  receipts,
  sourceSnapshots,
  currentPolicyHashes,
  errors,
}) {
  const policyContracts = validatePolicyContracts(
    budgets.observation_baseline?.policy_contracts,
    currentPolicyHashes,
    errors,
  );
  const snapshots = Array.isArray(sourceSnapshots) ? sourceSnapshots : [];
  expect(
    errors,
    Array.isArray(sourceSnapshots),
    "v2 Git source snapshots must be an array",
  );
  const snapshotsByPath = new Map();
  for (const snapshot of snapshots) {
    if (snapshotsByPath.has(snapshot.path)) {
      errors.push(`source snapshot is duplicated: ${snapshot.path}`);
    }
    snapshotsByPath.set(snapshot.path, snapshot);
  }
  expect(
    errors,
    snapshots.length === 3,
    "every v2 receipt requires an independent Git source snapshot",
  );

  const rounds = [];
  for (const receipt of receipts) {
    const expectedRound = EXPECTED_RECEIPT_PATHS.indexOf(receipt.path) + 1;
    expect(
      errors,
      receipt.value?.observation_round === expectedRound,
      `v2 receipt ${receipt.path} must carry observation round ${expectedRound}`,
    );
    const recordedAt = Date.parse(receipt.value?.recorded_at);
    const completedAt = Date.parse(receipt.value?.completed_at);
    expect(
      errors,
      Number.isFinite(recordedAt) &&
        recordedAt <= completedAt &&
        completedAt <= Date.parse(budgets.frozen_at),
      `v2 receipt ${receipt.path} must complete before the budget freeze`,
    );
    const measurements = validateV2Receipt(
      receipt,
      snapshotsByPath.get(receipt.path),
      budgets,
      policyContracts,
      errors,
    );
    if (measurements !== undefined) rounds.push(measurements);
  }
  expect(
    errors,
    isDeepStrictEqual(
      receipts.map(({ value }) => value?.observation_round).sort(),
      [1, 2, 3],
    ),
    "v2 observation rounds must be exactly 1, 2, and 3 without duplicates",
  );
  validateSameRoundIdentity(receipts, budgets, errors);
  if (rounds.length === 3) validateMaximaAndCeilings(rounds, budgets, errors);
}

function validatePolicyContracts(contracts, currentHashes, errors) {
  let valid =
    Array.isArray(contracts) && contracts.length === POLICY_SOURCE_PATHS.length;
  if (valid) {
    valid = contracts.every(
      (contract, index) =>
        exactObject(
          contract,
          ["path", "sha256"],
          `v2 policy contract ${index + 1}`,
          errors,
        ) &&
        contract.path === POLICY_SOURCE_PATHS[index] &&
        HASH_PATTERN.test(contract.sha256 ?? "") &&
        currentHashes?.[contract.path] === contract.sha256,
    );
  }
  expect(
    errors,
    valid,
    "v2 baseline must bind the current stable budget contract bytes",
  );
  return valid ? contracts : [];
}

function validateSameRoundIdentity(receipts, budgets, errors) {
  const first = receipts[0]?.value;
  for (const { path: receiptPath, value } of receipts.slice(1)) {
    for (const [field, label] of [
      ["seed", "seed"],
      ["time_budget_seconds", "time budget"],
      ["environment", "environment"],
      ["declared_limits", "workload"],
      ["provenance", "source/harness/binary/toolchain provenance"],
    ]) {
      expect(
        errors,
        isDeepStrictEqual(value?.[field], first?.[field]),
        `v2 rounds must share the same ${label}: ${receiptPath}`,
      );
    }
  }
  expect(
    errors,
    first !== undefined &&
      isDeepStrictEqual(
        budgets.observation_baseline?.environment,
        first.environment,
      ),
    "budget baseline environment must exactly match the v2 observation host",
  );
}

function validateMaximaAndCeilings(rounds, budgets, errors) {
  for (const mode of MODES) {
    for (const count of COUNTS) {
      const cells = rounds.map((measurements) =>
        measurements.find(
          (cell) => cell.mode === mode && String(cell.runs) === count,
        ),
      );
      const maxima = deriveObservedMaxima(cells);
      const recorded =
        budgets.observation_baseline?.observed_maxima?.[mode]?.[count];
      if (
        !exactObject(
          recorded,
          OBSERVED_FIELDS,
          `v2 maxima ${mode}/${count}`,
          errors,
        )
      ) {
        continue;
      }
      for (const field of OBSERVED_FIELDS) {
        if (!finiteNonNegative(maxima[field])) {
          errors.push(`raw ${mode}/${count} ${field} has no finite maximum`);
          continue;
        }
        expect(
          errors,
          recorded[field] === maxima[field],
          `recorded ${mode}/${count} ${field}=${recorded[field]} does not match raw maximum ${maxima[field]}`,
        );
        const budgetField = `max_${field}`;
        const expected = deriveBudgetCeiling(field, maxima[field]);
        expect(
          errors,
          budgets.budgets?.[mode]?.[count]?.[budgetField] === expected,
          `${mode}/${count} ${budgetField} must equal deterministic ceiling ${expected}`,
        );
      }
    }
  }
}

function validateV2Receipt(
  receipt,
  snapshot,
  budgets,
  policyContracts,
  errors,
  invocationNonce,
) {
  const { path: receiptPath, value } = receipt;
  if (!validateReceiptEnvelope(value, receiptPath, "observe", errors))
    return undefined;
  expect(
    errors,
    [1, 2, 3].includes(value.observation_round),
    `v2 receipt has invalid observation round: ${receiptPath}`,
  );
  validateQualificationEnvironment(value.environment, receiptPath, errors);
  validateQualificationWorkload(value.declared_limits, receiptPath, errors);
  validateProvenance(
    value.provenance,
    snapshot,
    budgets,
    policyContracts,
    receiptPath,
    errors,
  );
  validateTrace(
    value.action_trace,
    value.provenance,
    value.observation_round,
    receiptPath,
    errors,
    EXPECTED_STAGE_IDS,
    invocationNonce,
  );
  const measurements = validateStages(value.stages, receiptPath, errors);
  validateQualificationChronology(
    value,
    receiptPath,
    EXPECTED_STAGE_IDS,
    errors,
  );
  return measurements;
}

export function validatePassingObservationReceipt({
  receiptPath,
  value,
  snapshot,
  budgets,
  policyContracts,
  notBefore,
  verifiedAt,
  invocationNonce,
}) {
  const errors = [];
  validateV2Receipt(
    { path: receiptPath, value },
    snapshot,
    budgets,
    policyContracts,
    errors,
    invocationNonce,
  );
  validateReceiptFreshness(value, notBefore, verifiedAt, receiptPath, errors);
  return errors;
}

export function validatePassingQualificationReceipt({
  receiptPath,
  value,
  expectedProfile,
  qualificationPolicy,
  snapshot,
  budgets,
  policyContracts,
  notBefore,
  verifiedAt,
  invocationNonce,
}) {
  const errors = [];
  if (!validateReceiptEnvelope(value, receiptPath, expectedProfile, errors))
    return errors;
  const profilePolicy = qualificationPolicy?.profiles?.[expectedProfile];
  expect(
    errors,
    value.observation_round === null &&
      value.time_budget_seconds === profilePolicy?.time_budget_seconds,
    `v2 ${expectedProfile} receipt must use its canonical time budget without an observation round: ${receiptPath}`,
  );
  validateQualificationEnvironment(value.environment, receiptPath, errors);
  validateQualificationWorkload(value.declared_limits, receiptPath, errors, {
    resource_counts: profilePolicy?.resource_counts,
    soak_seconds: profilePolicy?.soak_seconds,
    resource_start_concurrency: qualificationPolicy?.resource_start_concurrency,
    seed_controls: qualificationPolicy?.seed_controls,
  });
  validateProvenance(
    value.provenance,
    snapshot,
    budgets,
    policyContracts,
    receiptPath,
    errors,
  );
  validateTrace(
    value.action_trace,
    value.provenance,
    value.observation_round,
    receiptPath,
    errors,
    QUALIFICATION_STAGE_IDS,
    invocationNonce,
  );
  validateStages(
    value.stages,
    receiptPath,
    errors,
    QUALIFICATION_STAGE_IDS,
    profilePolicy?.resource_counts,
  );
  validateQualificationChronology(
    value,
    receiptPath,
    QUALIFICATION_STAGE_IDS,
    errors,
  );
  validateReceiptFreshness(value, notBefore, verifiedAt, receiptPath, errors);
  return errors;
}

function validateReceiptFreshness(
  value,
  notBefore,
  verifiedAt,
  receiptPath,
  errors,
) {
  expect(
    errors,
    validTimestamp(notBefore) &&
      validTimestamp(verifiedAt) &&
      validTimestamp(value?.recorded_at) &&
      validTimestamp(value?.completed_at) &&
      Date.parse(value.recorded_at) >= Date.parse(notBefore) &&
      Date.parse(value.completed_at) <= Date.parse(verifiedAt),
    `v2 receipt must be produced by the current invocation: ${receiptPath}`,
  );
}

function validateReceiptEnvelope(value, receiptPath, expectedProfile, errors) {
  if (
    !exactObject(value, RECEIPT_FIELDS, `v2 receipt ${receiptPath}`, errors)
  ) {
    return false;
  }
  expect(
    errors,
    value.schema === "ctxmux.reliability-qualification.v2" &&
      value.status === "pass" &&
      value.profile === expectedProfile,
    `v2 receipt must use the v2 schema and pass ${expectedProfile}: ${receiptPath}`,
  );
  expect(
    errors,
    positiveInteger(value.seed) && positiveInteger(value.time_budget_seconds),
    `v2 receipt needs a positive seed and time budget: ${receiptPath}`,
  );
  expect(
    errors,
    validTimestamp(value.recorded_at) &&
      validTimestamp(value.completed_at) &&
      Date.parse(value.recorded_at) <= Date.parse(value.completed_at),
    `v2 receipt needs valid start and completion timestamps: ${receiptPath}`,
  );
  expect(
    errors,
    value.error === null,
    `passing v2 receipt has an error: ${receiptPath}`,
  );
  expect(
    errors,
    Array.isArray(value.action_trace) &&
      Array.isArray(value.daemon_logs) &&
      value.daemon_logs.length > 0 &&
      value.daemon_logs.every(nonEmptyString),
    `v2 receipt needs action trace and non-empty daemon logs: ${receiptPath}`,
  );
  return true;
}

function validateTrace(
  trace,
  provenance,
  round,
  receiptPath,
  errors,
  expectedStageIds = EXPECTED_STAGE_IDS,
  invocationNonce,
) {
  if (!Array.isArray(trace)) return;
  if (
    trace.some(
      (entry) =>
        !isObject(entry) ||
        !validTimestamp(entry.timestamp) ||
        !nonEmptyString(entry.action),
    )
  ) {
    errors.push(`v2 action trace entries are malformed: ${receiptPath}`);
    return;
  }
  expect(
    errors,
    trace.every((entry) => PASSING_TRACE_ACTIONS.has(entry.action)),
    `v2 passing receipt action trace contains a failure action or unknown action: ${receiptPath}`,
  );
  const indexes = (action) =>
    trace.flatMap((entry, index) => (entry.action === action ? [index] : []));
  const captured = indexes("provenance.captured");
  const verified = indexes("provenance.verified");
  const reverified = indexes("provenance.reverified");
  const starts = indexes("stage.start");
  const passes = indexes("stage.pass");
  const fenced =
    captured.length === 1 &&
    verified.length === 1 &&
    reverified.length === 1 &&
    starts.length === expectedStageIds.length &&
    passes.length === expectedStageIds.length &&
    captured[0] < verified[0] &&
    verified[0] < Math.min(...starts) &&
    reverified[0] > Math.max(...passes);
  if (!fenced) {
    errors.push(
      expectedStageIds === EXPECTED_STAGE_IDS
        ? `v2 action trace does not fence all four stages: ${receiptPath}`
        : `v2 action trace does not fence every expected stage: ${receiptPath}`,
    );
    return;
  }
  expect(
    errors,
    trace[captured[0]].source_commit === provenance?.source?.commit &&
      trace[captured[0]].worktree_clean === true &&
      trace[verified[0]].observation_round === round &&
      trace[reverified[0]].daemon_sha256 === provenance?.daemon?.sha256 &&
      isDeepStrictEqual(
        starts.map((index) => trace[index].id),
        expectedStageIds,
      ) &&
      isDeepStrictEqual(
        passes.map((index) => trace[index].id),
        expectedStageIds,
      ),
    `v2 action trace provenance or stage order drifted: ${receiptPath}`,
  );
  if (invocationNonce !== undefined) {
    expect(
      errors,
      trace[captured[0]].invocation_nonce === invocationNonce,
      `v2 action trace must bind the current invocation nonce: ${receiptPath}`,
    );
  }
}

export function validateQualificationChronology(
  value,
  receiptPath,
  expectedStageIds,
  errors,
  schemaLabel = "v2",
) {
  const receiptStart = Date.parse(value?.recorded_at);
  const receiptEnd = Date.parse(value?.completed_at);
  const trace = value?.action_trace;
  const stages = value?.stages;
  if (
    !Number.isFinite(receiptStart) ||
    !Number.isFinite(receiptEnd) ||
    !Array.isArray(trace) ||
    trace.some((entry) => !validTimestamp(entry?.timestamp)) ||
    !Array.isArray(stages) ||
    stages.length !== expectedStageIds.length ||
    stages.some(
      (stage) =>
        !validTimestamp(stage?.started_at) ||
        !validTimestamp(stage?.completed_at),
    )
  ) {
    return;
  }
  const traceTimes = trace.map((entry) => Date.parse(entry.timestamp));
  expect(
    errors,
    traceTimes.every(
      (timestamp, index) =>
        timestamp >= receiptStart &&
        timestamp <= receiptEnd &&
        (index === 0 || timestamp >= traceTimes[index - 1]),
    ),
    `${schemaLabel} action trace chronology must be monotonic inside the receipt interval: ${receiptPath}`,
  );

  let previousStageCompletion = receiptStart;
  let stageChronologyIsValid = true;
  for (const stageId of expectedStageIds) {
    const stage = stages.find(({ id }) => id === stageId);
    const startTrace = trace.find(
      (entry) => entry.action === "stage.start" && entry.id === stageId,
    );
    const passTrace = trace.find(
      (entry) => entry.action === "stage.pass" && entry.id === stageId,
    );
    if (
      stage === undefined ||
      startTrace === undefined ||
      passTrace === undefined
    ) {
      stageChronologyIsValid = false;
      continue;
    }
    const stageStart = Date.parse(stage.started_at);
    const stageCompletion = Date.parse(stage.completed_at);
    const traceStart = Date.parse(startTrace.timestamp);
    const tracePass = Date.parse(passTrace.timestamp);
    stageChronologyIsValid =
      stageChronologyIsValid &&
      stageStart >= receiptStart &&
      stageStart >= previousStageCompletion &&
      stageStart <= traceStart &&
      traceStart <= stageCompletion &&
      stageCompletion <= tracePass &&
      tracePass <= receiptEnd;
    previousStageCompletion = stageCompletion;
  }
  expect(
    errors,
    stageChronologyIsValid,
    `${schemaLabel} stage chronology must stay inside the receipt interval and its trace fence: ${receiptPath}`,
  );
}

export function validateQualificationEnvironment(
  environment,
  receiptPath,
  errors,
  schemaLabel = "v2",
  requireSafeIntegers = false,
) {
  if (
    !exactObject(
      environment,
      ["os", "os_release", "architecture", "logical_cpus", "cpu_model"],
      `${schemaLabel} environment ${receiptPath}`,
      errors,
    )
  )
    return;
  expect(
    errors,
    ["os", "os_release", "architecture", "cpu_model"].every((field) =>
      nonEmptyString(environment[field]),
    ) &&
      positiveInteger(environment.logical_cpus) &&
      (!requireSafeIntegers || Number.isSafeInteger(environment.logical_cpus)),
    `${schemaLabel} environment fields must be non-empty: ${receiptPath}`,
  );
}

export function validateQualificationToolchain(
  toolchain,
  receiptPath,
  errors,
  schemaLabel = "v2",
) {
  expect(
    errors,
    exactObject(
      toolchain,
      ["rustc_version_verbose", "cargo_version", "node_version"],
      `${schemaLabel} toolchain ${receiptPath}`,
      errors,
    ) && Object.values(toolchain).every(nonEmptyString),
    `${schemaLabel} toolchain fields must be non-empty: ${receiptPath}`,
  );
}

export function validateQualificationWorkload(
  limits,
  receiptPath,
  errors,
  overrides = {},
  schemaLabel = "v2",
) {
  if (
    !exactObject(
      limits,
      WORKLOAD_FIELDS,
      `${schemaLabel} workload ${receiptPath}`,
      errors,
    )
  ) {
    return;
  }
  const expected = {
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
    ...overrides,
  };
  expect(
    errors,
    Object.entries(expected).every(([field, value]) =>
      isDeepStrictEqual(limits[field], value),
    ) && nonEmptyString(limits.note),
    `${schemaLabel} workload is not the canonical qualification matrix: ${receiptPath}`,
  );
}

function validateProvenance(
  provenance,
  snapshot,
  budgets,
  policyContracts,
  receiptPath,
  errors,
) {
  if (
    !exactObject(
      provenance,
      PROVENANCE_FIELDS,
      `v2 provenance ${receiptPath}`,
      errors,
    )
  ) {
    return;
  }
  expect(
    errors,
    provenance.claim_scope === "locally_observed" &&
      provenance.binary_source_attestation === false,
    `v2 provenance must be a local observation without binary attestation: ${receiptPath}`,
  );
  if (
    !exactObject(
      provenance.source,
      ["commit", "tree", "worktree"],
      `v2 source ${receiptPath}`,
      errors,
    )
  ) {
    return;
  }
  expect(
    errors,
    GIT_OBJECT_PATTERN.test(provenance.source.commit ?? "") &&
      GIT_OBJECT_PATTERN.test(provenance.source.tree ?? ""),
    `v2 source commit and tree must be exact 40-hex objects: ${receiptPath}`,
  );
  expect(
    errors,
    isDeepStrictEqual(provenance.source.worktree, {
      status_format: "git-status-porcelain-v1-z",
      clean: true,
      entries: [],
    }),
    `v2 source worktree must be clean with empty entries: ${receiptPath}`,
  );
  for (const [field, expectedPath] of [
    ["harness", "scripts/reliability-qualification.ts"],
    ["launcher", "scripts/check-reliability.sh"],
    ["daemon", FIXED_DAEMON_PATH],
  ]) {
    validateFileIdentity(
      provenance[field],
      expectedPath,
      field,
      receiptPath,
      errors,
    );
  }
  if (
    !Array.isArray(provenance.lockfiles) ||
    provenance.lockfiles.length !== 2
  ) {
    errors.push(`v2 provenance requires exactly two lockfiles: ${receiptPath}`);
  } else {
    validateFileIdentity(
      provenance.lockfiles[0],
      "Cargo.lock",
      "lockfile",
      receiptPath,
      errors,
    );
    validateFileIdentity(
      provenance.lockfiles[1],
      "package-lock.json",
      "lockfile",
      receiptPath,
      errors,
    );
  }
  expect(
    errors,
    isDeepStrictEqual(provenance.build, {
      cwd: ".",
      argv: FIXED_BUILD_ARGV,
      source_commit: provenance.source.commit,
      source_tree: provenance.source.tree,
      worktree_clean: true,
      target_directory: FIXED_BUILD_TARGET,
      daemon_path: FIXED_DAEMON_PATH,
      locked: true,
    }),
    `v2 build must use the fixed locked source-bound daemon path: ${receiptPath}`,
  );
  validateQualificationToolchain(provenance.toolchain, receiptPath, errors);
  const contractHash = crypto
    .createHash("sha256")
    .update(JSON.stringify(budgets.measurement_contract))
    .digest("hex");
  expect(
    errors,
    provenance.measurement_contract_encoding === "json-stringify-utf8" &&
      provenance.measurement_contract_sha256 === contractHash,
    `v2 measurement contract hash drifted: ${receiptPath}`,
  );
  validateSourceSnapshot(
    provenance,
    snapshot,
    policyContracts,
    receiptPath,
    errors,
  );
}

function validateFileIdentity(
  identity,
  expectedPath,
  label,
  receiptPath,
  errors,
) {
  if (
    !exactObject(
      identity,
      ["path", "sha256"],
      `v2 ${label} ${receiptPath}`,
      errors,
    )
  ) {
    return;
  }
  expect(
    errors,
    identity.path === expectedPath &&
      canonicalFixturePath(identity.path) &&
      HASH_PATTERN.test(identity.sha256 ?? ""),
    `v2 ${label} identity is invalid: ${receiptPath}`,
  );
}

function validateSourceSnapshot(
  provenance,
  snapshot,
  policyContracts,
  receiptPath,
  errors,
) {
  if (!isObject(snapshot) || snapshot.error !== undefined) {
    errors.push(`v2 Git source snapshot is unavailable: ${receiptPath}`);
    return;
  }
  expect(
    errors,
    snapshot.path === receiptPath &&
      snapshot.commit === provenance.source.commit &&
      snapshot.reachableFromHead === true,
    `v2 source commit is not the verified current-HEAD ancestor: ${receiptPath}`,
  );
  expect(
    errors,
    snapshot.tree === provenance.source.tree,
    `v2 source tree hash drifted from Git: ${receiptPath}`,
  );
  const lockfiles = Array.isArray(provenance.lockfiles)
    ? provenance.lockfiles
    : [];
  const sourceIdentities = [
    provenance.harness,
    provenance.launcher,
    ...lockfiles,
  ];
  for (const filePath of SOURCE_FILE_PATHS) {
    const recorded = sourceIdentities.find(
      (item) => item?.path === filePath,
    )?.sha256;
    expect(
      errors,
      HASH_PATTERN.test(snapshot.fileHashes?.[filePath] ?? "") &&
        snapshot.fileHashes[filePath] === recorded,
      `v2 source hash drifted for ${filePath}: ${receiptPath}`,
    );
  }
  for (const contract of policyContracts) {
    expect(
      errors,
      snapshot.fileHashes?.[contract.path] === contract.sha256,
      `v2 policy source hash drifted for ${contract.path}: ${receiptPath}`,
    );
  }
}

function validateStages(
  stages,
  receiptPath,
  errors,
  expectedStageIds = EXPECTED_STAGE_IDS,
  expectedResourceCounts = COUNTS.map(Number),
) {
  if (!Array.isArray(stages) || stages.length !== expectedStageIds.length) {
    errors.push(
      expectedStageIds === EXPECTED_STAGE_IDS
        ? `v2 receipt must contain exactly four stages: ${receiptPath}`
        : `v2 receipt must contain exactly ${expectedStageIds.length} stages: ${receiptPath}`,
    );
    return undefined;
  }
  expect(
    errors,
    isDeepStrictEqual(
      stages.map((stage) => stage?.id),
      expectedStageIds,
    ),
    `v2 receipt stages are incomplete or out of order: ${receiptPath}`,
  );
  for (const stage of stages) {
    if (
      !exactObject(
        stage,
        ["id", "status", "started_at", "completed_at", "result"],
        `v2 stage ${stage?.id}`,
        errors,
      )
    ) {
      continue;
    }
    expect(
      errors,
      stage.status === "pass" &&
        validTimestamp(stage.started_at) &&
        validTimestamp(stage.completed_at) &&
        Date.parse(stage.started_at) <= Date.parse(stage.completed_at),
      `v2 stage ${stage.id} must pass with timestamps: ${receiptPath}`,
    );
  }
  const resourceStages = stages.filter(
    (stage) => stage?.id === "resource-census",
  );
  return resourceStages.length === 1
    ? validateQualificationResourceCells(
        resourceStages[0].result,
        receiptPath,
        errors,
        expectedResourceCounts,
      )
    : undefined;
}

export function validateQualificationResourceCells(
  cells,
  receiptPath,
  errors,
  expectedResourceCounts,
  schemaLabel = "v2",
  requireSafeIntegers = false,
) {
  const expectedLength = MODES.length * expectedResourceCounts.length;
  if (!Array.isArray(cells) || cells.length !== expectedLength) {
    errors.push(
      expectedLength === 6
        ? `${schemaLabel} resource census must contain exactly six cells: ${receiptPath}`
        : `${schemaLabel} resource census must contain exactly ${expectedLength} cells: ${receiptPath}`,
    );
    return undefined;
  }
  const identities = cells.map((cell) => `${cell?.mode}/${cell?.runs}`);
  const expected = MODES.flatMap((mode) =>
    expectedResourceCounts.map((count) => `${mode}/${count}`),
  );
  let valid = expect(
    errors,
    sameMembers(identities, expected) &&
      new Set(identities).size === expectedLength,
    `${schemaLabel} resource cells must be unique and match the canonical mode/count matrix: ${receiptPath}`,
  );
  for (const cell of cells)
    valid =
      validateResourceCell(
        cell,
        receiptPath,
        errors,
        expectedResourceCounts,
        schemaLabel,
        requireSafeIntegers,
      ) && valid;
  return valid ? cells : undefined;
}

function validateResourceCell(
  cell,
  receiptPath,
  errors,
  expectedResourceCounts,
  schemaLabel,
  requireSafeIntegers,
) {
  const label = `${cell?.mode}/${cell?.runs} in ${receiptPath}`;
  if (
    !exactObject(
      cell,
      RESOURCE_FIELDS,
      `${schemaLabel} resource cell ${label}`,
      errors,
    )
  ) {
    return false;
  }
  let valid = expect(
    errors,
    MODES.includes(cell.mode) && expectedResourceCounts.includes(cell.runs),
    `${schemaLabel} resource cell has an unknown identity: ${label}`,
  );
  for (const name of ["baseline", "steady", "cleanup"]) {
    valid =
      validateProcessSample(
        cell[name],
        `${label} ${name}`,
        errors,
        schemaLabel,
        requireSafeIntegers,
      ) && valid;
  }
  for (const field of RESOURCE_NUMERIC_FIELDS) {
    valid =
      expect(
        errors,
        finiteNonNegative(cell[field]),
        `${schemaLabel} resource cell ${label} ${field} is not finite/non-negative`,
      ) && valid;
  }
  valid =
    expect(
      errors,
      positiveInteger(cell.peak_rss_sample_count) &&
        (!requireSafeIntegers ||
          Number.isSafeInteger(cell.peak_rss_sample_count)) &&
        cell.peak_rss_sample_interval_ms === 25 &&
        cell.intentional_retained_state_without_gc === true,
      `${schemaLabel} resource cell ${label} has an invalid sampling/no-GC contract`,
    ) && valid;
  if (!valid || !positiveInteger(cell.runs)) return false;
  const divide = (value) => Math.round((value / cell.runs) * 1000) / 1000;
  const derived = {
    retained_output_bytes_per_run: divide(cell.retained_output_bytes),
    rss_kib_per_run: divide(
      Math.max(0, cell.steady.rss_kib - cell.baseline.rss_kib),
    ),
    threads_per_run: divide(
      Math.max(0, cell.steady.threads - cell.baseline.threads),
    ),
    fds_per_run: divide(Math.max(0, cell.steady.fds - cell.baseline.fds)),
    cleanup_live_children: cell.cleanup.descendants.length,
  };
  for (const [field, expected] of Object.entries(derived)) {
    valid =
      expect(
        errors,
        cell[field] === expected,
        `${schemaLabel} resource cell ${label} ${field} is not derived from samples`,
      ) && valid;
  }
  return valid;
}

function validateProcessSample(
  sample,
  label,
  errors,
  schemaLabel,
  requireSafeIntegers,
) {
  if (
    !exactObject(
      sample,
      ["rss_kib", "cpu_seconds", "threads", "fds", "descendants"],
      `${schemaLabel} process sample ${label}`,
      errors,
    )
  ) {
    return false;
  }
  let valid = ["rss_kib", "cpu_seconds", "threads", "fds"].every((field) =>
    finiteNonNegative(sample[field]),
  );
  valid =
    valid && Number.isInteger(sample.threads) && Number.isInteger(sample.fds);
  valid =
    valid &&
    (!requireSafeIntegers ||
      (Number.isSafeInteger(sample.threads) &&
        Number.isSafeInteger(sample.fds)));
  valid = valid && Array.isArray(sample.descendants);
  expect(errors, valid, `${schemaLabel} process sample ${label} is malformed`);
  return valid;
}
