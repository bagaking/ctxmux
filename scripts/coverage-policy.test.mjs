import assert from "node:assert/strict";
import test from "node:test";

import {
  evaluateChangedLines,
  evaluateGroups,
  parseChangedLines,
  parseIstanbul,
  parseLcov,
} from "./coverage-policy.mjs";

const root = "/repo";
const policy = {
  changed_line_minimum: 90,
  groups: [
    {
      id: "rust",
      language: "rust",
      minimum_line_percent: 50,
      paths: ["src/lib.rs"],
    },
    {
      id: "typescript",
      language: "typescript",
      minimum_line_percent: 50,
      paths: ["src/client.ts"],
    },
  ],
};

function typescriptCoverage(path, counts) {
  const statementMap = {};
  const statements = {};
  counts.forEach((count, index) => {
    const line = index + 1;
    statementMap[index] = {
      start: { line, column: 0 },
      end: { line, column: 1 },
    };
    statements[index] = count;
  });
  return {
    path,
    all: false,
    statementMap,
    s: statements,
    fnMap: {},
    f: {},
    branchMap: {},
    b: {},
  };
}

test("parses LCOV and Istanbul line evidence into portable repository paths", () => {
  const rust = parseLcov(
    "TN:\nSF:/repo/src/lib.rs\nDA:1,2\nDA:2,0\nLF:2\nLH:1\nend_of_record\n",
    root,
  );
  const typescript = parseIstanbul(
    {
      "/repo/src/client.ts": typescriptCoverage("/repo/src/client.ts", [1, 0]),
    },
    root,
  );

  assert.deepEqual(
    [...rust.get("src/lib.rs")],
    [
      [1, 2],
      [2, 0],
    ],
  );
  assert.deepEqual(
    [...typescript.get("src/client.ts")],
    [
      [1, 1],
      [2, 0],
    ],
  );
  const result = evaluateGroups(policy, { rust, typescript });
  assert.deepEqual(result.errors, []);
  assert.equal(result.results[0].percent, 50);
  assert.equal(result.results[1].percent, 50);
});

test("fails closed for unclassified and missing coverage files", () => {
  const result = evaluateGroups(policy, {
    rust: new Map([
      ["src/lib.rs", new Map([[1, 1]])],
      ["src/unowned.rs", new Map([[1, 1]])],
    ]),
    typescript: new Map(),
  });

  assert.ok(
    result.errors.some((error) =>
      error.includes("src/unowned.rs is assigned to 0"),
    ),
  );
  assert.ok(
    result.errors.some((error) =>
      error.includes("missing coverage for src/client.ts"),
    ),
  );
});

test("changed-line coverage counts only executable added lines in owned product files", () => {
  const changed = parseChangedLines(`
diff --git a/src/lib.rs b/src/lib.rs
--- a/src/lib.rs
+++ b/src/lib.rs
@@ -1 +1,3 @@
diff --git a/src/client.ts b/src/client.ts
--- a/src/client.ts
+++ b/src/client.ts
@@ -4,0 +5,2 @@
diff --git a/docs/readme.md b/docs/readme.md
--- a/docs/readme.md
+++ b/docs/readme.md
@@ -0,0 +1 @@
`);
  const result = evaluateChangedLines(
    policy,
    {
      rust: new Map([
        [
          "src/lib.rs",
          new Map([
            [1, 1],
            [2, 0],
          ]),
        ],
      ]),
      typescript: new Map([
        [
          "src/client.ts",
          new Map([
            [5, 1],
            [6, 1],
          ]),
        ],
      ]),
    },
    changed,
  );

  assert.equal(result.covered, 3);
  assert.equal(result.total, 4);
  assert.equal(result.percent, 75);
  assert.equal(result.passed, false);
});

test("changed-line coverage is not fabricated when a diff has no executable product lines", () => {
  const reports = {
    rust: new Map([["src/lib.rs", new Map([[10, 1]])]]),
    typescript: new Map([["src/client.ts", new Map([[10, 1]])]]),
  };
  const changed = parseChangedLines("");
  const result = evaluateChangedLines(policy, reports, changed);

  assert.equal(result.percent, null);
  assert.equal(result.evidence_missing, false);
  assert.equal(result.passed, true);

  const required = evaluateChangedLines(policy, reports, changed, true);
  assert.equal(required.percent, null);
  assert.equal(required.evidence_missing, true);
  assert.equal(required.passed, false);
});

test("required changed-line evidence passes only with a non-empty executable denominator", () => {
  const result = evaluateChangedLines(
    policy,
    {
      rust: new Map([["src/lib.rs", new Map([[10, 1]])]]),
      typescript: new Map([["src/client.ts", new Map([[10, 1]])]]),
    },
    parseChangedLines(`
diff --git a/src/lib.rs b/src/lib.rs
--- a/src/lib.rs
+++ b/src/lib.rs
@@ -9,0 +10 @@
`),
    true,
  );

  assert.equal(result.total, 1);
  assert.equal(result.percent, 100);
  assert.equal(result.evidence_missing, false);
  assert.equal(result.passed, true);
});
