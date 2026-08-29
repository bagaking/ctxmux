import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import test from "node:test";

import {
  loadCurrentFeatureTaskIds,
  loadFixtureTestTargetContext,
  trackedActivationTaskError,
  validateFixtureTestReference,
} from "./fixture-validation.mjs";

function write(root, path, contents) {
  const target = join(root, path);
  mkdirSync(dirname(target), { recursive: true });
  writeFileSync(target, contents);
}

function setup(context) {
  const root = mkdtempSync(join(tmpdir(), "ctxmux-fixture-validator-"));
  context.after(() => rmSync(root, { force: true, recursive: true }));
  write(
    root,
    "scripts/check.sh",
    "cargo test --workspace --all-targets\nscripts/check-protocol-types.sh\nnpm test\n",
  );
  write(root, "Cargo.toml", '[workspace]\nmembers = ["crates/demo"]\n');
  write(root, "crates/demo/Cargo.toml", '[package]\nname = "demo"\n');
  write(
    root,
    "package.json",
    JSON.stringify({
      scripts: {
        test: "npm run test:unit",
        "test:unit": "npm run test --workspaces --if-present",
      },
    }),
  );
  write(
    root,
    "packages/sdk/package.json",
    JSON.stringify({
      name: "@ctxmux/sdk",
      scripts: { test: "tsx --test test/included.test.ts" },
    }),
  );
  return root;
}

function fixtureErrors(root, path, anchor) {
  return validateFixtureTestReference(loadFixtureTestTargetContext(root), {
    anchor,
    path,
  }).join("\n");
}

test("rejects a Rust anchor found only in a comment", (context) => {
  const root = setup(context);
  write(root, "crates/demo/src/lib.rs", "// #[test]\n// fn fake() {}\n");
  assert.match(
    fixtureErrors(root, "crates/demo/src/lib.rs", "fake"),
    /not a Rust/u,
  );
});

test("rejects a test in an unexecuted Rust source", (context) => {
  const root = setup(context);
  write(root, "crates/demo/src/unused.rs", "#[test]\nfn unused() {}\n");
  assert.match(
    fixtureErrors(root, "crates/demo/src/unused.rs", "unused"),
    /not reachable from a Cargo/u,
  );
});

test("accepts a Rust test in a module reachable from the crate root", (context) => {
  const root = setup(context);
  write(root, "crates/demo/src/lib.rs", "mod parser;\n");
  write(
    root,
    "crates/demo/src/parser.rs",
    "#[test]\nfn exact_parser_contract() {}\n",
  );
  assert.equal(
    fixtureErrors(root, "crates/demo/src/parser.rs", "exact_parser_contract"),
    "",
  );
});

test("rejects ignored Rust fixture anchors", (context) => {
  const root = setup(context);
  write(
    root,
    "crates/demo/src/lib.rs",
    [
      "#[test]",
      '#[ignore = "not part of the gate"]',
      "fn ignored_after_test() {}",
      "",
      "#[ignore]",
      "#[test]",
      "fn ignored_before_test() {}",
      "",
    ].join("\n"),
  );
  assert.match(
    fixtureErrors(root, "crates/demo/src/lib.rs", "ignored_after_test"),
    /not a Rust/u,
  );
  assert.match(
    fixtureErrors(root, "crates/demo/src/lib.rs", "ignored_before_test"),
    /not a Rust/u,
  );
});

test("requires a declared TypeScript test selected by a reachable runner", (context) => {
  const root = setup(context);
  write(
    root,
    "packages/sdk/test/included.test.ts",
    '// test("COMMENT-01 fake", () => {});\ntest("REAL-01 runs", () => {});\n',
  );
  write(
    root,
    "packages/sdk/test/unlisted.test.ts",
    'test("REAL-02 runs", () => {});\n',
  );
  assert.match(
    fixtureErrors(root, "packages/sdk/test/included.test.ts", "COMMENT-01"),
    /not the prefix/u,
  );
  assert.match(
    fixtureErrors(root, "packages/sdk/test/unlisted.test.ts", "REAL-02"),
    /no gate-reachable/u,
  );
});

test("rejects shell comments and scripts outside check.sh", (context) => {
  const root = setup(context);
  write(
    root,
    "scripts/check-protocol-types.sh",
    "# scripts/comment-only.sh\nscripts/generate-types.sh\n",
  );
  write(root, "scripts/unexecuted.sh", "scripts/real-command.sh\n");
  assert.match(
    fixtureErrors(root, "scripts/check-protocol-types.sh", "comment-only"),
    /not a top-level command/u,
  );
  assert.match(
    fixtureErrors(root, "scripts/unexecuted.sh", "real-command"),
    /not directly executed/u,
  );
});

test("rejects test commands removed from check.sh", (context) => {
  const root = setup(context);
  write(
    root,
    "scripts/check.sh",
    "# cargo test --workspace --all-targets\nnpm test\n",
  );
  assert.match(
    loadFixtureTestTargetContext(root).errors.join("\n"),
    /must directly execute `cargo test/u,
  );
});

test("accepts required cargo test options in check.sh", (context) => {
  const root = setup(context);
  write(
    root,
    "scripts/check.sh",
    "cargo test --workspace --all-targets --no-fail-fast\nnpm test\n",
  );
  assert.deepEqual(loadFixtureTestTargetContext(root).errors, []);
});

test("requires T-nnn activation owners in the current Feature", (context) => {
  const root = setup(context);
  write(
    root,
    ".bagakit/feature-tracker/index/features.json",
    JSON.stringify({
      features: [{ feat_id: "f-current", status: "in_progress" }],
    }),
  );
  write(
    root,
    ".bagakit/feature-tracker/features/f-current/tasks.json",
    JSON.stringify({ feat_id: "f-current", tasks: [{ id: "T-004" }] }),
  );
  write(
    root,
    ".bagakit/feature-tracker/features/f-current/state.json",
    JSON.stringify({ feat_id: "f-current" }),
  );
  const registry = loadCurrentFeatureTaskIds(root);
  assert.equal(trackedActivationTaskError(registry.ids, "T-004"), null);
  assert.match(
    trackedActivationTaskError(registry.ids, "T-999"),
    /does not exist/u,
  );
});
