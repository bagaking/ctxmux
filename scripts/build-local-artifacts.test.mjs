import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

import { sourceIdentity } from "./build-local-artifacts.mjs";

const root = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const builder = path.join(root, "scripts/build-local-artifacts.mjs");

function run(command, args, cwd) {
  const result = spawnSync(command, args, {
    cwd,
    encoding: "utf8",
  });
  assert.equal(
    result.status,
    0,
    `${command} ${args.join(" ")} failed: ${result.stderr}`,
  );
  return result;
}

test("local artifact command binds one clean Git identity and rejects dirty input", () => {
  const directory = fs.mkdtempSync(
    path.join(os.tmpdir(), "ctxmux-artifact-dirty-"),
  );
  try {
    fs.mkdirSync(path.join(directory, "scripts"));
    fs.copyFileSync(
      builder,
      path.join(directory, "scripts/build-local-artifacts.mjs"),
    );
    run("/usr/bin/git", ["init", "--quiet"], directory);
    run("/usr/bin/git", ["config", "user.name", "ctxmux fixture"], directory);
    run(
      "/usr/bin/git",
      ["config", "user.email", "ctxmux-fixture@example.invalid"],
      directory,
    );
    run(
      "/usr/bin/git",
      ["add", "scripts/build-local-artifacts.mjs"],
      directory,
    );
    run("/usr/bin/git", ["commit", "--quiet", "-m", "fixture"], directory);

    const identity = sourceIdentity(directory);
    assert.match(identity.commit, /^[0-9a-f]{40}$/u);
    assert.match(identity.tree, /^[0-9a-f]{40}$/u);
    assert.match(identity.commit_time_unix, /^(0|[1-9][0-9]*)$/u);

    fs.writeFileSync(path.join(directory, "dirty.txt"), "dirty\n");
    assert.throws(
      () => sourceIdentity(directory),
      /artifact source worktree must be clean/u,
    );
    const command = spawnSync(
      process.execPath,
      [
        path.join(directory, "scripts/build-local-artifacts.mjs"),
        path.join(directory, "artifacts"),
      ],
      { cwd: directory, encoding: "utf8" },
    );
    assert.equal(
      command.status,
      1,
      `stdout=${command.stdout} stderr=${command.stderr}`,
    );
    assert.match(command.stderr, /artifact source worktree must be clean/u);
    assert.equal(fs.existsSync(path.join(directory, "artifacts")), false);
  } finally {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});
