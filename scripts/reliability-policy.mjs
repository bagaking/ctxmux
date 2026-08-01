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

export { deriveBudgetCeiling } from "./reliability-budget-contract.mjs";

const GIT_OBJECT_PATTERN = /^[0-9a-f]{40}$/u;
const EXPECTED_CHECK_CORE_SHA256 =
  "b3ae60d8b72e02baeb1cdf3c466057d4df9d26ae0072a95648c44d4720b61b84";
const EXPECTED_QUALIFICATION_LAUNCHER_SHA256 =
  "a041719262dc1187848b9b7f66899c50554464700c2b51411943d4a689329a4f";
const EXPECTED_QUALIFICATION_POLICY = {
  schema: "ctxmux.reliability-qualification-policy.v1",
  profiles: {
    smoke: {
      time_budget_seconds: 60,
      soak_seconds: 0,
      resource_counts: [1],
    },
    nightly: {
      time_budget_seconds: 45 * 60,
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
  for (const logPath of Array.isArray(value?.daemon_logs)
    ? value.daemon_logs
    : []) {
    const resolvedLogPath = path.resolve(root, logPath);
    const artifactRelativeLog = path.relative(
      artifactDirectory,
      resolvedLogPath,
    );
    const logName = path.basename(resolvedLogPath);
    let logIdentity;
    try {
      logIdentity = existingOwnedFileIdentity(logName);
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
      errors.push(`qualification daemon log is unavailable: ${logPath}`);
    }
    seen.add(resolvedLogPath);
  }
  return errors;
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
  const preflight = createQualificationPreflight(
    expectedProfile,
    artifactOwnerIdentity,
    preexistingReceiptIdentity,
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
      name: "Run 30-minute chaos, stress, resource, and leak qualification",
      command: "scripts/check-reliability.sh --profile nightly",
      condition:
        "github.event_name == 'schedule' || inputs.qualification == 'nightly'",
      timeoutMinutes: 60,
      environment: {
        CTXMUX_RELIABILITY_SEED: "${{ github.run_id }}",
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
        CTXMUX_RELIABILITY_SEED: "${{ github.run_id }}",
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
  const semanticErrors =
    expectedProfile === "observe"
      ? validatePassingObservationReceipt(receiptValidation)
      : validatePassingQualificationReceipt({
          ...receiptValidation,
          expectedProfile,
          qualificationPolicy,
        });
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
