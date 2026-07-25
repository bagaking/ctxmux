import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import {
  discoverCriticalTests,
  validateCiReachability,
} from "./ci-reachability.mjs";

function createFixture(t) {
  const root = fs.mkdtempSync(
    path.join(os.tmpdir(), "ctxmux-ci-reachability-"),
  );
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  for (const directory of ["crates/demo/src", "packages/sdk/test", "scripts"]) {
    fs.mkdirSync(path.join(root, directory), { recursive: true });
  }
  fs.writeFileSync(
    path.join(root, "crates/demo/src/lib.rs"),
    "#[test]\nfn works() {}\n",
  );
  fs.writeFileSync(
    path.join(root, "packages/sdk/test/client.test.ts"),
    'test("works", () => {});\n',
  );
  fs.writeFileSync(
    path.join(root, "scripts/gate.test.mjs"),
    'test("works", () => {});\n',
  );
  fs.writeFileSync(
    path.join(root, "scripts/check.sh"),
    "cargo test --workspace --all-targets\nnode --test scripts/*.test.mjs\n",
  );

  const jobs = [
    {
      id: "critical",
      platforms: ["ubuntu-latest", "macos-latest"],
      command: "scripts/check.sh",
      required: true,
    },
    {
      id: "coverage",
      platforms: ["ubuntu-latest"],
      command: "scripts/check.sh --coverage",
      required: true,
    },
  ];
  const reach = jobs.map(({ id: job, platforms }) => ({ job, platforms }));
  const suites = [
    ["rust", "crates/demo/src/lib.rs", "cargo test --workspace --all-targets"],
    [
      "typescript",
      "packages/sdk/test/client.test.ts",
      "node --test scripts/*.test.mjs",
    ],
    ["scripts", "scripts/gate.test.mjs", "node --test scripts/*.test.mjs"],
  ].map(([id, suitePath, selectionAnchor]) => ({
    id,
    kind: "test",
    path: suitePath,
    invariants: [`${id} invariant`],
    selection_ref: "scripts/check.sh",
    selection_anchor: selectionAnchor,
    reach,
  }));
  const map = {
    schema: "ctxmux.ci-evidence-map.v1",
    jobs,
    suites,
    non_required_evidence: {
      skipped: [],
      ignored: [],
      conditional: [],
      schedule_only: [],
    },
  };
  const workflow = `on:
  pull_request:
  push:
jobs:
  critical:
    strategy:
      matrix:
        os: [ubuntu-latest, macos-latest]
    steps:
      - run: scripts/check.sh
  coverage:
    runs-on: ubuntu-latest
    steps:
      - run: scripts/check.sh --coverage
`;
  return { root, map, workflow };
}

test("discovers every checked-in Rust, TypeScript, and script test surface", (t) => {
  const { root } = createFixture(t);
  assert.deepEqual([...discoverCriticalTests(root)].sort(), [
    "crates/demo/src/lib.rs",
    "packages/sdk/test/client.test.ts",
    "scripts/gate.test.mjs",
  ]);
});

test("accepts complete required-job, platform, invariant, and selection reach", (t) => {
  const fixture = createFixture(t);
  assert.deepEqual(validateCiReachability(fixture), []);
});

test("rejects unmapped and skipped critical tests", (t) => {
  const fixture = createFixture(t);
  fixture.map.suites = fixture.map.suites.filter(
    ({ id }) => id !== "typescript",
  );
  fs.writeFileSync(
    path.join(fixture.root, "scripts/gate.test.mjs"),
    'test.skip("hidden", () => {});\n',
  );
  const errors = validateCiReachability(fixture);
  assert.ok(
    errors.some((error) => error.includes("has no job-to-invariant mapping")),
  );
  assert.ok(errors.some((error) => error.includes("skipped or todo evidence")));
});

test("rejects workflow and selection drift instead of trusting map prose", (t) => {
  const fixture = createFixture(t);
  fixture.map.suites[0].selection_anchor = "cargo nextest run";
  fixture.workflow = fixture.workflow.replace("macos-latest", "windows-latest");
  const errors = validateCiReachability(fixture);
  assert.ok(
    errors.some((error) => error.includes("does not reach macos-latest")),
  );
  assert.ok(
    errors.some((error) => error.includes("selection anchor is unreachable")),
  );
});
