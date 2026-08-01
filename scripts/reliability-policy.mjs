import { spawnSync } from "node:child_process";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";

import {
  canonicalFixturePath,
  EXPECTED_RECEIPT_PATHS,
  HASH_PATTERN,
  isObject,
  POLICY_SOURCE_PATHS,
  sameMembers,
  SNAPSHOT_FILE_PATHS,
  validateSourceBoundBaseline,
} from "./reliability-baseline-policy.mjs";
import {
  BUDGET_FIELDS,
  COUNTS,
  MODES,
} from "./reliability-budget-contract.mjs";

export { deriveBudgetCeiling } from "./reliability-budget-contract.mjs";

const GIT_OBJECT_PATTERN = /^[0-9a-f]{40}$/u;

function validTimestamp(value) {
  return typeof value === "string" && Number.isFinite(Date.parse(value));
}

function finiteNonNegative(value) {
  return Number.isFinite(value) && value >= 0;
}

function jobBlock(workflow, id) {
  const jobsStart = workflow.search(/^jobs:\s*$/mu);
  if (jobsStart < 0) return undefined;
  const jobs = workflow.slice(jobsStart);
  const matches = [...jobs.matchAll(/^ {2}([a-z][a-z0-9_-]*):\s*$/gmu)];
  const index = matches.findIndex((match) => match[1] === id);
  if (index < 0) return undefined;
  return jobs.slice(
    matches[index].index,
    matches[index + 1]?.index ?? jobs.length,
  );
}

export function validateReliabilityPolicy({
  budgets,
  baselineReceipts,
  sourceSnapshots = [],
  currentPolicyHashes,
  workflow,
  checkScript,
  harnessSource,
}) {
  const errors = [];
  validateBudgetShape(budgets, errors);
  const references = validateReceiptReferences(budgets, errors);
  const receipts = resolveReceipts(references, baselineReceipts, errors);
  if (receipts.length === 3) {
    const schemas = new Set(receipts.map(({ value }) => value?.schema));
    if (schemas.size !== 1) {
      errors.push("observation baseline must not mix v1 and v2 receipts");
    } else if (schemas.has("ctxmux.reliability-qualification.v1")) {
      validateLegacyBaseline(budgets, receipts, errors);
    } else if (schemas.has("ctxmux.reliability-qualification.v2")) {
      validateSourceBoundBaseline({
        budgets,
        receipts,
        sourceSnapshots,
        currentPolicyHashes,
        errors,
      });
    } else {
      errors.push(`unsupported observation receipt schema ${[...schemas][0]}`);
    }
  }
  validateReachability({ workflow, checkScript, harnessSource }, errors);
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

// Temporary transition only: an all-v1 baseline keeps the existing Gate
// operational, but never satisfies T-021 and cannot mix with v2.
function validateLegacyBaseline(budgets, receipts, errors) {
  const rounds = [];
  for (const receipt of receipts) {
    if (
      receipt.value?.status !== "pass" ||
      receipt.value?.profile !== "observe"
    ) {
      errors.push(`legacy observation receipt did not pass: ${receipt.path}`);
    }
    const resources = receipt.value?.stages?.find(
      (stage) => stage.id === "resource-census" && stage.status === "pass",
    )?.result;
    if (!Array.isArray(resources) || resources.length !== 6) {
      errors.push(
        `legacy observation receipt lacks six cells: ${receipt.path}`,
      );
    } else {
      rounds.push(resources);
    }
  }
  if (rounds.length !== 3) return;
  for (const mode of MODES) {
    for (const count of COUNTS) {
      const cells = rounds.map((measurements) =>
        measurements.find(
          (cell) => cell.mode === mode && String(cell.runs) === count,
        ),
      );
      if (cells.some((cell) => cell === undefined)) {
        errors.push(`legacy observation receipts are missing ${mode}/${count}`);
        continue;
      }
      const recorded =
        budgets.observation_baseline?.observed_maxima?.[mode]?.[count];
      const maxima = {
        cpu_core_percent: Math.max(
          ...cells.map((cell) => cell.cpu_core_percent),
        ),
        peak_rss_kib: Math.max(...cells.map((cell) => cell.peak_rss_kib)),
        steady_rss_kib: Math.max(...cells.map((cell) => cell.steady?.rss_kib)),
        rss_kib_per_run: Math.max(...cells.map((cell) => cell.rss_kib_per_run)),
      };
      for (const [field, maximum] of Object.entries(maxima)) {
        if (recorded?.[field] !== maximum) {
          errors.push(
            `recorded ${mode}/${count} ${field} does not match raw maximum`,
          );
        }
      }
    }
  }
}

function validateReachability(
  { workflow, checkScript, harnessSource },
  errors,
) {
  if (!checkScript.includes("scripts/check-reliability.sh --profile smoke")) {
    errors.push("the required check does not reach reliability smoke");
  }
  for (const lane of [
    {
      id: "reliability-nightly",
      tokens: [
        "scripts/check-reliability.sh --profile nightly",
        "timeout-minutes: 60",
        "path: target/reliability/nightly",
      ],
    },
    {
      id: "release-soak",
      tokens: [
        "scripts/check-reliability.sh --profile release",
        "timeout-minutes: 210",
        "path: target/reliability/release",
      ],
    },
  ]) {
    const block = jobBlock(workflow, lane.id);
    if (!block) {
      errors.push(`reliability workflow is missing ${lane.id}`);
      continue;
    }
    for (const token of [
      "ubuntu-latest",
      "macos-latest",
      "if: always()",
      ...lane.tokens,
    ]) {
      if (!block.includes(token)) errors.push(`${lane.id} is missing ${token}`);
    }
  }
  if (!workflow.includes('cron: "17 3 * * *"')) {
    errors.push("nightly reliability has no explicit schedule");
  }
  if (!workflow.includes("inputs.qualification == 'release'")) {
    errors.push("release soak is not restricted to explicit dispatch");
  }
  if (/continue-on-error\s*:\s*true/u.test(workflow)) {
    errors.push("reliability evidence must not continue on error");
  }
  for (const token of [
    "nightly: 30 * 60",
    "release: 2 * 60 * 60",
    "nightly: 45 * 60",
    "release: 3 * 60 * 60",
    'optionValue("--resource-start-concurrency") ?? "8"',
    'action: "supervisor.timeout"',
    "process.kill(-pid, signal)",
    "consumeExactOutput(attachment, payloadBytes, payloadByte)",
    'seed_controls: ["fanout payload byte", "secret marker"]',
  ]) {
    if (!harnessSource.includes(token)) {
      errors.push(
        `qualification harness is missing frozen lane policy: ${token}`,
      );
    }
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
  const root = path.resolve(process.argv[2] ?? ".");
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
    harnessSource: fs.readFileSync(
      path.join(root, "scripts", "reliability-qualification.ts"),
      "utf8",
    ),
  });
  if (errors.length > 0) {
    for (const error of errors) console.error(`Reliability policy: ${error}`);
    process.exitCode = 1;
  } else if (
    baselineReceipts.every(
      ({ value }) => value.schema === "ctxmux.reliability-qualification.v1",
    )
  ) {
    console.log(
      "Reliability policy: legacy v1 baseline accepted for transition; only a complete source-bound v2 baseline satisfies T-021",
    );
  } else {
    console.log(
      "Reliability policy: source-bound v2 observations and deterministic ceilings are valid",
    );
  }
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  main();
}
