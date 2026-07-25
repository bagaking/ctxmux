import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import {
  changedProductFiles,
  collectGitChanges,
  discoverRepositoryFiles,
  evaluateChangedLines,
  evaluateGroups,
  evaluateSourceInventory,
  includeUntrackedExecutableLines,
  parseChangedLines,
  parseIstanbul,
  parseLcov,
  resolveComparisonBase,
} from "./coverage-policy.mjs";

const root = "/repo";
const policy = {
  schema: "ctxmux.coverage-policy.v2",
  floors: {
    changed_line_percent: 90,
    runtime_line_percent: 85,
    pure_validator_line_percent: 95,
  },
  source_inventory: {
    includes: [
      { language: "rust", glob: "src/**/*.rs" },
      { language: "typescript", glob: "src/**/*.ts" },
    ],
    exclusions: [
      {
        id: "generated",
        category: "generated",
        language: "typescript",
        glob: "src/generated/**",
        reason: "generated fixture",
        evidence: "generate.sh",
      },
    ],
  },
  groups: [
    {
      id: "rust",
      language: "rust",
      floor_class: "runtime",
      paths: ["src/lib.rs"],
    },
    {
      id: "typescript",
      language: "typescript",
      floor_class: "runtime",
      paths: ["src/client.ts"],
    },
  ],
  reported_exclusions: [],
};
const sourceFiles = [
  "src/lib.rs",
  "src/client.ts",
  "src/generated/protocol.ts",
];

function inventory(candidate = policy, files = sourceFiles) {
  return evaluateSourceInventory(candidate, files);
}

function typescriptCoverage(filename, counts) {
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
    path: filename,
    all: false,
    statementMap,
    s: statements,
    fnMap: {},
    f: {},
    branchMap: {},
    b: {},
  };
}

function git(repo, args) {
  return execFileSync("git", args, {
    cwd: repo,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  }).trim();
}

function createGitFixture(t) {
  const repo = fs.mkdtempSync(
    path.join(os.tmpdir(), "ctxmux-coverage-policy-"),
  );
  t.after(() => fs.rmSync(repo, { recursive: true, force: true }));
  git(repo, ["init", "-b", "main"]);
  git(repo, ["config", "user.email", "coverage@example.invalid"]);
  git(repo, ["config", "user.name", "Coverage Test"]);
  fs.mkdirSync(path.join(repo, "src"), { recursive: true });
  fs.mkdirSync(path.join(repo, "docs"), { recursive: true });
  fs.writeFileSync(path.join(repo, "src", "lib.rs"), "fn covered() {}\n");
  fs.writeFileSync(path.join(repo, "docs", "readme.md"), "baseline\n");
  git(repo, ["add", "."]);
  git(repo, ["commit", "-m", "baseline"]);
  return { repo, base: git(repo, ["rev-parse", "HEAD"]) };
}

function evaluateAutoGitChanges(repo, base, reports, candidate = policy) {
  const resolution = resolveComparisonBase({
    root: repo,
    base,
    comparison: "direct",
    mode: "auto",
  });
  const changes = collectGitChanges(repo, resolution);
  const productFiles = changedProductFiles(candidate, changes.changed_files);
  const result = evaluateChangedLines(
    candidate,
    reports,
    includeUntrackedExecutableLines(
      parseChangedLines(changes.patch),
      reports,
      changes.untracked_files,
    ),
    { mode: "auto", productChanged: productFiles.size > 0 },
  );
  return { productFiles, result };
}

test("parses LCOV and Istanbul line evidence into portable repository paths", () => {
  const rust = parseLcov(
    "TN:\nSF:/repo/src/lib.rs\nDA:1,2\nDA:2,2\nLF:2\nLH:2\nend_of_record\n",
    root,
  );
  const typescript = parseIstanbul(
    {
      "/repo/src/client.ts": typescriptCoverage("/repo/src/client.ts", [1, 1]),
    },
    root,
  );
  const sourceInventory = inventory();
  assert.deepEqual(sourceInventory.errors, []);

  assert.deepEqual(
    [...rust.get("src/lib.rs")],
    [
      [1, 2],
      [2, 2],
    ],
  );
  const result = evaluateGroups(policy, { rust, typescript }, sourceInventory);
  assert.deepEqual(result.errors, []);
  assert.equal(result.results[0].percent, 100);
  assert.equal(result.results[1].percent, 100);
});

test("inventory requires every source to have one owner or one typed exclusion", () => {
  const complete = inventory();
  assert.deepEqual(complete.errors, []);
  assert.equal(
    complete.files.get("src/generated/protocol.ts").exclusion.id,
    "generated",
  );

  const unowned = inventory(policy, [...sourceFiles, "src/unowned.rs"]);
  assert.ok(
    unowned.errors.some((error) =>
      error.includes("src/unowned.rs is assigned to 0 rust coverage groups"),
    ),
  );

  const unsafePath = inventory(policy, [...sourceFiles, "src/space name.rs"]);
  assert.ok(
    unsafePath.errors.some((error) =>
      error.includes(
        "product source path uses unsupported coverage characters",
      ),
    ),
  );

  const overlapPolicy = structuredClone(policy);
  overlapPolicy.groups.push({
    id: "overlap",
    language: "rust",
    floor_class: "runtime",
    paths: ["src/lib.rs"],
  });
  const overlap = inventory(overlapPolicy);
  assert.ok(
    overlap.errors.some((error) =>
      error.includes("assigned to 2 policy groups"),
    ),
  );
});

test("real floors cannot be lowered or bypassed with a local exception", () => {
  const lowered = structuredClone(policy);
  lowered.floors.runtime_line_percent = 84;
  lowered.groups[0].minimum_line_percent = 50;
  lowered.groups[0].exception = { reason: "make the gate green" };
  const result = inventory(lowered);
  assert.ok(
    result.errors.some((error) =>
      error.includes("runtime_line_percent must remain 85"),
    ),
  );
  assert.ok(
    result.errors.some((error) =>
      error.includes("without a local threshold or exception"),
    ),
  );
});

test("fails closed for reported files outside inventory and missing reports", () => {
  const result = evaluateGroups(
    policy,
    {
      rust: new Map([
        ["src/lib.rs", new Map([[1, 1]])],
        ["test/fixture.rs", new Map([[1, 1]])],
      ]),
      typescript: new Map(),
    },
    inventory(),
  );
  assert.ok(
    result.errors.some((error) =>
      error.includes("test/fixture.rs is reported but is outside"),
    ),
  );
  assert.ok(
    result.errors.some((error) =>
      error.includes("missing coverage for src/client.ts"),
    ),
  );
});

test("changed-line coverage counts only executable added product lines", () => {
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
    { mode: "auto", productChanged: true },
  );

  assert.equal(result.covered, 3);
  assert.equal(result.total, 4);
  assert.equal(result.percent, 75);
  assert.equal(result.passed, false);
});

test("false, true, and auto distinguish reporting from required evidence", () => {
  const reports = {
    rust: new Map([["src/lib.rs", new Map([[10, 1]])]]),
    typescript: new Map([["src/client.ts", new Map([[10, 1]])]]),
  };
  const changed = parseChangedLines("");

  const ordinary = evaluateChangedLines(policy, reports, changed, {
    mode: "false",
  });
  assert.equal(ordinary.percent, null);
  assert.equal(ordinary.evidence_required, false);
  assert.equal(ordinary.passed, true);

  const explicit = evaluateChangedLines(policy, reports, changed, {
    mode: "true",
  });
  assert.equal(explicit.evidence_missing, true);
  assert.equal(explicit.passed, false);

  const documentation = evaluateChangedLines(policy, reports, changed, {
    mode: "auto",
    productChanged: false,
  });
  assert.equal(documentation.evidence_required, false);
  assert.equal(documentation.passed, true);

  const commentOnlyProduct = evaluateChangedLines(policy, reports, changed, {
    mode: "auto",
    productChanged: true,
  });
  assert.equal(commentOnlyProduct.evidence_missing, true);
  assert.equal(commentOnlyProduct.passed, false);
});

test("required changed-line evidence passes only with a non-empty denominator", () => {
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
    { mode: "true", productChanged: true },
  );

  assert.equal(result.total, 1);
  assert.equal(result.percent, 100);
  assert.equal(result.evidence_missing, false);
  assert.equal(result.passed, true);
});

test("base resolution rejects missing evidence inputs, zero SHA, HEAD, and invalid objects", (t) => {
  const { repo, base } = createGitFixture(t);
  fs.appendFileSync(path.join(repo, "src", "lib.rs"), "fn second() {}\n");
  git(repo, ["add", "."]);
  git(repo, ["commit", "-m", "second"]);

  assert.throws(
    () => resolveComparisonBase({ root: repo, mode: "true" }),
    /requires an explicit base/u,
  );
  assert.throws(
    () =>
      resolveComparisonBase({
        root: repo,
        base,
        mode: "true",
      }),
    /requires an explicit comparison mode/u,
  );
  assert.throws(
    () =>
      resolveComparisonBase({
        root: repo,
        base: "0000000000000000000000000000000000000000",
        comparison: "direct",
        mode: "true",
      }),
    /zero SHA/u,
  );
  assert.throws(
    () =>
      resolveComparisonBase({
        root: repo,
        base: "missing",
        comparison: "direct",
        mode: "true",
      }),
    /not a valid commit/u,
  );
  assert.throws(
    () =>
      resolveComparisonBase({
        root: repo,
        base: "HEAD",
        comparison: "direct",
        mode: "true",
      }),
    /base different from HEAD/u,
  );

  const direct = resolveComparisonBase({
    root: repo,
    base,
    comparison: "direct",
    mode: "true",
  });
  assert.equal(direct.resolved_base, base);
  assert.equal(direct.effective_base, base);
  const merged = resolveComparisonBase({
    root: repo,
    base,
    comparison: "merge-base",
    mode: "auto",
  });
  assert.equal(merged.effective_base, base);
});

test("merge-base comparison rejects an unrelated valid commit", (t) => {
  const { repo } = createGitFixture(t);
  git(repo, ["checkout", "--orphan", "unrelated"]);
  git(repo, ["rm", "-rf", "."]);
  fs.writeFileSync(path.join(repo, "unrelated.txt"), "unrelated\n");
  git(repo, ["add", "."]);
  git(repo, ["commit", "-m", "unrelated"]);
  const unrelated = git(repo, ["rev-parse", "HEAD"]);
  git(repo, ["checkout", "main"]);

  assert.throws(
    () =>
      resolveComparisonBase({
        root: repo,
        base: unrelated,
        comparison: "merge-base",
        mode: "auto",
      }),
    /has no merge base with HEAD/u,
  );
});

test("merge-base evidence rejects a future descendant that resolves to HEAD", (t) => {
  const { repo, base } = createGitFixture(t);
  fs.appendFileSync(path.join(repo, "src", "lib.rs"), "fn future() {}\n");
  git(repo, ["add", "."]);
  git(repo, ["commit", "-m", "future product change"]);
  const future = git(repo, ["rev-parse", "HEAD"]);
  git(repo, ["checkout", "--detach", base]);

  for (const mode of ["auto", "true"]) {
    assert.throws(
      () =>
        resolveComparisonBase({
          root: repo,
          base: future,
          comparison: "merge-base",
          mode,
        }),
      new RegExp(
        `${mode} changed-line mode requires an effective base different from HEAD`,
        "u",
      ),
    );
  }
});

test("direct evidence rejects a future descendant instead of hiding current changes", (t) => {
  const { repo, base } = createGitFixture(t);
  fs.appendFileSync(path.join(repo, "src", "lib.rs"), "fn future() {}\n");
  git(repo, ["add", "."]);
  git(repo, ["commit", "-m", "future product change"]);
  const future = git(repo, ["rev-parse", "HEAD"]);
  git(repo, ["checkout", "--detach", base]);

  for (const mode of ["auto", "true"]) {
    assert.throws(
      () =>
        resolveComparisonBase({
          root: repo,
          base: future,
          comparison: "direct",
          mode,
        }),
      new RegExp(
        `${mode} direct comparison requires the base to be an ancestor of HEAD`,
        "u",
      ),
    );
  }
});

test("name-status observes documentation, comment, deletion, rename, and untracked paths", (t) => {
  const { repo } = createGitFixture(t);
  fs.appendFileSync(path.join(repo, "docs", "readme.md"), "docs only\n");
  fs.appendFileSync(path.join(repo, "src", "lib.rs"), "// comment only\n");
  fs.renameSync(
    path.join(repo, "src", "lib.rs"),
    path.join(repo, "src", "renamed.rs"),
  );
  fs.writeFileSync(
    path.join(repo, "src", "untracked.rs"),
    "fn new_file() {}\n",
  );
  const resolution = resolveComparisonBase({ root: repo, mode: "false" });
  const changes = collectGitChanges(repo, resolution);

  assert.ok(changes.changed_files.has("docs/readme.md"));
  assert.ok(changes.changed_files.has("src/lib.rs"));
  assert.ok(changes.changed_files.has("src/renamed.rs"));
  assert.ok(changes.changed_files.has("src/untracked.rs"));
  assert.deepEqual(
    [...changedProductFiles(policy, changes.changed_files)].sort(),
    ["src/lib.rs", "src/renamed.rs", "src/untracked.rs"],
  );
});

test("auto mode passes documentation-only changes and fails zero-denominator product changes", (t) => {
  const cases = [
    {
      label: "documentation only",
      change(repo) {
        fs.appendFileSync(path.join(repo, "docs", "readme.md"), "docs only\n");
        git(repo, ["add", "."]);
        git(repo, ["commit", "-m", "documentation only"]);
      },
      productChanged: false,
      passed: true,
    },
    {
      label: "product comment only",
      change(repo) {
        fs.appendFileSync(
          path.join(repo, "src", "lib.rs"),
          "// comment only\n",
        );
        git(repo, ["add", "."]);
        git(repo, ["commit", "-m", "product comment only"]);
      },
      productChanged: true,
      passed: false,
    },
    {
      label: "deleted product source",
      change(repo) {
        fs.unlinkSync(path.join(repo, "src", "lib.rs"));
        git(repo, ["add", "."]);
        git(repo, ["commit", "-m", "delete product source"]);
      },
      productChanged: true,
      passed: false,
    },
    {
      label: "untracked product source",
      change(repo) {
        fs.appendFileSync(
          path.join(repo, "docs", "readme.md"),
          "advance HEAD\n",
        );
        git(repo, ["add", "."]);
        git(repo, ["commit", "-m", "advance HEAD"]);
        fs.writeFileSync(
          path.join(repo, "src", "untracked.rs"),
          "fn untracked() {}\n",
        );
      },
      productChanged: true,
      passed: false,
    },
  ];
  const reports = {
    rust: new Map([["src/lib.rs", new Map([[1, 1]])]]),
    typescript: new Map(),
  };

  for (const fixtureCase of cases) {
    const { repo, base } = createGitFixture(t);
    fixtureCase.change(repo);
    const { productFiles, result } = evaluateAutoGitChanges(
      repo,
      base,
      reports,
    );

    assert.equal(
      productFiles.size > 0,
      fixtureCase.productChanged,
      fixtureCase.label,
    );
    assert.equal(result.total, 0, fixtureCase.label);
    assert.equal(result.percent, null, fixtureCase.label);
    assert.equal(result.evidence_required, fixtureCase.productChanged);
    assert.equal(result.evidence_missing, fixtureCase.productChanged);
    assert.equal(result.passed, fixtureCase.passed, fixtureCase.label);
  }
});

test("tracked diff keeps current worktree coordinates across committed and dirty changes", (t) => {
  const { repo, base } = createGitFixture(t);
  fs.writeFileSync(path.join(repo, "src", "lib.rs"), "fn committed() {}\n");
  git(repo, ["add", "."]);
  git(repo, ["commit", "-m", "committed product change"]);
  fs.writeFileSync(
    path.join(repo, "src", "lib.rs"),
    "fn working() {}\nfn committed() {}\n",
  );

  const resolution = resolveComparisonBase({
    root: repo,
    base,
    comparison: "direct",
    mode: "true",
  });
  const changes = collectGitChanges(repo, resolution);
  const changed = parseChangedLines(changes.patch);
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
      typescript: new Map(),
    },
    changed,
    { mode: "true", productChanged: true },
  );

  assert.deepEqual([...changed.get("src/lib.rs")], [1, 2]);
  assert.equal(result.percent, 50);
  assert.equal(result.passed, false);
});

test("mixed tracked and untracked product lines share one changed-line denominator", (t) => {
  const { repo, base } = createGitFixture(t);
  fs.writeFileSync(
    path.join(repo, "src", "lib.rs"),
    "fn covered() {}\nfn tracked() {}\n",
  );
  git(repo, ["add", "src/lib.rs"]);
  git(repo, ["commit", "-m", "tracked product change"]);
  fs.writeFileSync(
    path.join(repo, "src", "untracked.rs"),
    "fn one() {}\nfn two() {}\nfn three() {}\nfn four() {}\n",
  );
  const candidate = structuredClone(policy);
  candidate.groups[0].paths.push("src/untracked.rs");
  const reports = {
    rust: new Map([
      [
        "src/lib.rs",
        new Map([
          [1, 1],
          [2, 1],
        ]),
      ],
      [
        "src/untracked.rs",
        new Map([
          [1, 0],
          [2, 0],
          [3, 0],
          [4, 0],
        ]),
      ],
    ]),
    typescript: new Map(),
  };

  const { productFiles, result } = evaluateAutoGitChanges(
    repo,
    base,
    reports,
    candidate,
  );

  assert.deepEqual([...productFiles].sort(), [
    "src/lib.rs",
    "src/untracked.rs",
  ]);
  assert.equal(result.covered, 1);
  assert.equal(result.total, 5);
  assert.equal(result.percent, 20);
  assert.equal(result.passed, false);
});

test("checked-in policy fixes every current product owner and floor", () => {
  const repositoryRoot = path.resolve(import.meta.dirname, "..");
  const checkedIn = JSON.parse(
    fs.readFileSync(path.join(repositoryRoot, "coverage-policy.json"), "utf8"),
  );
  const checkedInventory = evaluateSourceInventory(
    checkedIn,
    discoverRepositoryFiles(repositoryRoot),
  );
  assert.deepEqual(checkedInventory.errors, []);
  assert.deepEqual(
    checkedIn.groups.map(
      ({ id, language, floor_class: floorClass, paths }) => ({
        id,
        language,
        floor_class: floorClass,
        paths,
      }),
    ),
    [
      {
        id: "rust-runtime-and-clients",
        language: "rust",
        floor_class: "runtime",
        paths: [
          "crates/ctxmux-daemon/src/lib.rs",
          "crates/ctxmux-daemon/src/main.rs",
          "crates/ctxmux-client/src/lib.rs",
          "crates/ctxmux/src/main.rs",
        ],
      },
      {
        id: "rust-persistence",
        language: "rust",
        floor_class: "runtime",
        paths: ["crates/ctxmux-daemon/src/persistence.rs"],
      },
      {
        id: "rust-tmux",
        language: "rust",
        floor_class: "runtime",
        paths: ["crates/ctxmux-daemon/src/tmux.rs"],
      },
      {
        id: "rust-run-spec-validator",
        language: "rust",
        floor_class: "pure_validator",
        paths: ["crates/ctxmux-daemon/src/run_spec.rs"],
      },
      {
        id: "rust-protocol-and-codegen",
        language: "rust",
        floor_class: "pure_validator",
        paths: [
          "crates/ctxmux-protocol/src/lib.rs",
          "crates/ctxmux-protocol/src/bin/export-types.rs",
        ],
      },
      {
        id: "typescript-sdk",
        language: "typescript",
        floor_class: "runtime",
        paths: [
          "packages/sdk/src/client.ts",
          "packages/sdk/src/index.ts",
          "packages/sdk/src/integration.ts",
          "packages/sdk/src/integrations/codex.ts",
          "packages/sdk/src/integrations/index.ts",
          "packages/sdk/src/integrations/shell.ts",
        ],
      },
      {
        id: "typescript-protocol-validators",
        language: "typescript",
        floor_class: "pure_validator",
        paths: ["packages/sdk/src/validation.ts", "packages/sdk/src/wire.ts"],
      },
    ],
  );
  assert.deepEqual(checkedIn.source_inventory.includes, [
    { language: "rust", glob: "crates/*/src/**/*.rs" },
    { language: "typescript", glob: "packages/sdk/src/**/*.ts" },
  ]);
  assert.deepEqual(
    checkedIn.source_inventory.exclusions.map(
      ({ id, category, language, glob }) => ({
        id,
        category,
        language,
        glob,
      }),
    ),
    [
      {
        id: "generated-typescript-protocol",
        category: "generated",
        language: "typescript",
        glob: "packages/sdk/src/generated/**",
      },
    ],
  );
});
