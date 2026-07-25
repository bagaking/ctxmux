import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

import {
  loadBaselineReceipts,
  validateReliabilityPolicy,
} from "./reliability-policy.mjs";

const root = path.resolve(import.meta.dirname, "..");

function actualInputs() {
  const budgets = JSON.parse(
    fs.readFileSync(path.join(root, "reliability-budgets.json"), "utf8"),
  );
  return {
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
}

test("accepts the frozen resource budgets and reachable reliability lanes", () => {
  assert.deepEqual(validateReliabilityPolicy(actualInputs()), []);
});

test("rejects an unfrozen or incomplete high-Run-count budget", () => {
  const inputs = actualInputs();
  inputs.budgets.frozen_before_optimization = false;
  delete inputs.budgets.budgets.active["128"];
  const errors = validateReliabilityPolicy(inputs);
  assert.ok(errors.some((error) => error.includes("before optimization")));
  assert.ok(errors.some((error) => error.includes("1/32/128")));
});

test("rejects changed or misreported raw observation evidence", () => {
  const inputs = actualInputs();
  inputs.baselineReceipts[0].sha256 = "0".repeat(64);
  inputs.budgets.observation_baseline.observed_maxima.active[
    "128"
  ].peak_rss_kib = 1;
  const errors = validateReliabilityPolicy(inputs);
  assert.ok(errors.some((error) => error.includes("hash drifted")));
  assert.ok(errors.some((error) => error.includes("raw maximum")));
});

test("rejects unreachable smoke, nightly, release, and duration policy", () => {
  const inputs = actualInputs();
  inputs.checkScript = inputs.checkScript.replace(
    "scripts/check-reliability.sh --profile smoke",
    "true",
  );
  inputs.workflow = inputs.workflow.replace(
    "scripts/check-reliability.sh --profile release",
    "true",
  );
  inputs.harnessSource = inputs.harnessSource.replace(
    "release: 2 * 60 * 60",
    "release: 60",
  );
  const errors = validateReliabilityPolicy(inputs);
  assert.ok(errors.some((error) => error.includes("reliability smoke")));
  assert.ok(errors.some((error) => error.includes("release-soak")));
  assert.ok(errors.some((error) => error.includes("frozen lane policy")));
});
