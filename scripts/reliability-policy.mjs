import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";

const MODES = ["idle", "active"];
const COUNTS = ["1", "32", "128"];
const BUDGET_FIELDS = [
  "max_cpu_core_percent",
  "max_peak_rss_kib",
  "max_steady_rss_kib",
  "max_retained_output_bytes_per_run",
  "max_rss_kib_per_run",
  "max_threads_per_run",
  "max_fds_per_run",
  "max_cleanup_threads_delta",
  "max_cleanup_live_children",
  "max_cleanup_attachments",
];

function sameMembers(left, right) {
  return (
    left.length === right.length &&
    [...left].sort().every((value, index) => value === [...right].sort()[index])
  );
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
  workflow,
  checkScript,
  harnessSource,
}) {
  const errors = [];
  if (budgets.schema !== "ctxmux.reliability-budgets.v1") {
    errors.push(`unsupported reliability budget schema ${budgets.schema}`);
  }
  if (budgets.frozen_before_optimization !== true) {
    errors.push("resource budgets must be frozen before optimization");
  }
  if (!Number.isFinite(Date.parse(budgets.frozen_at))) {
    errors.push("resource budgets need a valid frozen_at timestamp");
  }
  const baseline = budgets.observation_baseline;
  if (!baseline || baseline.profile !== "observe" || baseline.rounds < 3) {
    errors.push("resource budgets need at least three clean observe rounds");
  }
  const rawReceiptRefs = baseline?.raw_receipts;
  if (
    !Array.isArray(rawReceiptRefs) ||
    rawReceiptRefs.length !== baseline.rounds
  ) {
    errors.push(
      "observation baseline must retain one raw receipt ref per round",
    );
  }
  if (!Number.isInteger(baseline?.resource_start_concurrency)) {
    errors.push("observation baseline must record resource start concurrency");
  }
  if (!Number.isInteger(baseline?.peak_rss_sample_interval_ms)) {
    errors.push(
      "observation baseline must record the peak RSS sample interval",
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
      if (!budget) continue;
      if (!sameMembers(Object.keys(budget), BUDGET_FIELDS)) {
        errors.push(
          `${mode}/${count} has an incomplete or unknown budget field`,
        );
      }
      for (const field of BUDGET_FIELDS) {
        const value = budget[field];
        if (!Number.isFinite(value) || value < 0) {
          errors.push(
            `${mode}/${count} ${field} must be finite and non-negative`,
          );
        }
      }
    }
  }
  for (const field of ["cpu", "rss", "slopes", "cleanup"]) {
    if (!budgets.measurement_contract?.[field]) {
      errors.push(`measurement contract is missing ${field}`);
    }
  }

  const receiptsByPath = new Map(
    (baselineReceipts ?? []).map((receipt) => [receipt.path, receipt]),
  );
  const observedReceipts = [];
  for (const reference of rawReceiptRefs ?? []) {
    if (
      typeof reference?.path !== "string" ||
      !reference.path.startsWith("fixtures/reliability/") ||
      typeof reference.sha256 !== "string"
    ) {
      errors.push(
        "raw observation receipt refs must be durable fixture paths with SHA-256",
      );
      continue;
    }
    const receipt = receiptsByPath.get(reference.path);
    if (!receipt) {
      errors.push(`raw observation receipt is missing: ${reference.path}`);
      continue;
    }
    if (receipt.sha256 !== reference.sha256) {
      errors.push(`raw observation receipt hash drifted: ${reference.path}`);
    }
    if (
      receipt.value.status !== "pass" ||
      receipt.value.profile !== "observe"
    ) {
      errors.push(
        `raw observation receipt did not pass observe: ${reference.path}`,
      );
    }
    const resourceStage = receipt.value.stages?.find(
      (stage) => stage.id === "resource-census" && stage.status === "pass",
    );
    if (
      !Array.isArray(resourceStage?.result) ||
      resourceStage.result.length !== 6
    ) {
      errors.push(
        `raw observation receipt lacks six resource cells: ${reference.path}`,
      );
      continue;
    }
    observedReceipts.push(resourceStage.result);
  }
  if (observedReceipts.length === baseline?.rounds) {
    const maximaFields = [
      "cpu_core_percent",
      "peak_rss_kib",
      "rss_kib_per_run",
    ];
    for (const mode of MODES) {
      for (const count of COUNTS) {
        const cells = observedReceipts.map((measurements) =>
          measurements.find(
            (measurement) =>
              measurement.mode === mode && String(measurement.runs) === count,
          ),
        );
        if (cells.some((cell) => cell === undefined)) {
          errors.push(`raw observation receipts are missing ${mode}/${count}`);
          continue;
        }
        const recorded = baseline.observed_maxima?.[mode]?.[count];
        for (const field of maximaFields) {
          const derived = Math.max(...cells.map((cell) => cell[field]));
          if (recorded?.[field] !== derived) {
            errors.push(
              `recorded ${mode}/${count} ${field}=${recorded?.[field]} does not match raw maximum ${derived}`,
            );
          }
        }
        const steady = Math.max(...cells.map((cell) => cell.steady.rss_kib));
        if (recorded?.steady_rss_kib !== steady) {
          errors.push(
            `recorded ${mode}/${count} steady_rss_kib=${recorded?.steady_rss_kib} does not match raw maximum ${steady}`,
          );
        }
      }
    }
  }

  if (!checkScript.includes("scripts/check-reliability.sh --profile smoke")) {
    errors.push("the required check does not reach reliability smoke");
  }
  const lanes = [
    {
      id: "reliability-nightly",
      command: "scripts/check-reliability.sh --profile nightly",
      timeout: "timeout-minutes: 60",
      artifact: "path: target/reliability/nightly",
    },
    {
      id: "release-soak",
      command: "scripts/check-reliability.sh --profile release",
      timeout: "timeout-minutes: 210",
      artifact: "path: target/reliability/release",
    },
  ];
  for (const lane of lanes) {
    const block = jobBlock(workflow, lane.id);
    if (!block) {
      errors.push(`reliability workflow is missing ${lane.id}`);
      continue;
    }
    for (const token of [
      "ubuntu-latest",
      "macos-latest",
      lane.command,
      lane.timeout,
      lane.artifact,
      "if: always()",
    ]) {
      if (!block.includes(token)) {
        errors.push(`${lane.id} is missing ${token}`);
      }
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
  return errors;
}

function main() {
  const root = path.resolve(process.argv[2] ?? ".");
  const budgets = JSON.parse(
    fs.readFileSync(path.join(root, "reliability-budgets.json"), "utf8"),
  );
  const inputs = {
    budgets,
    baselineReceipts: loadBaselineReceipts(root, budgets),
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
  };
  const errors = validateReliabilityPolicy(inputs);
  if (errors.length > 0) {
    for (const error of errors) console.error(`Reliability policy: ${error}`);
    process.exitCode = 1;
  } else {
    console.log(
      "Reliability policy: smoke, nightly, release, and 1/32/128 budgets are reachable",
    );
  }
}

export function loadBaselineReceipts(root, budgets) {
  return (budgets.observation_baseline?.raw_receipts ?? []).map((reference) => {
    const bytes = fs.readFileSync(path.join(root, reference.path));
    return {
      path: reference.path,
      sha256: crypto.createHash("sha256").update(bytes).digest("hex"),
      value: JSON.parse(bytes.toString("utf8")),
    };
  });
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  main();
}
