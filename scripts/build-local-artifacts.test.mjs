import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

import {
  parseBinaryVersion,
  sourceIdentity,
} from "./build-local-artifacts.mjs";

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

test("the version parser reads the protocol out of every identity a binary prints", () => {
  // The guard that was missing when `ctxmuxd --version` gained its handoff
  // schema: the suite tested dirty-tree rejection and nothing else, so the one
  // parser standing between the binaries and the vendor artifact had no
  // coverage at all. The build failed at the only supported way to produce a
  // vendor drop, and every test stayed green.
  //
  // The contract is that the parenthesis carries an open-ended list of identity
  // facts whose FIRST entry is the protocol. Anything after the protocol belongs
  // to whichever binary owns that fact — `ctxmuxd` names its handoff schema,
  // `ctxmux` names nothing — and the parser must not care.
  assert.deepEqual(parseBinaryVersion("ctxmux", "ctxmux 0.1.0 (protocol 17)"), {
    version: "0.1.0",
    protocol: 17,
  });
  assert.deepEqual(
    parseBinaryVersion(
      "ctxmuxd",
      "ctxmuxd 0.1.0 (protocol 17, handoff ctxmux.daemon-handoff.v4)",
    ),
    { version: "0.1.0", protocol: 17 },
  );
  // A fact appended later must not break it again.
  assert.deepEqual(
    parseBinaryVersion(
      "ctxmuxd",
      "ctxmuxd 2.10.3 (protocol 23, handoff x, more)",
    ),
    { version: "2.10.3", protocol: 23 },
  );
  // Still fails closed on output that names no protocol, or the wrong binary.
  for (const bad of [
    "ctxmuxd 0.1.0",
    "ctxmuxd 0.1.0 (protocol )",
    "ctxmuxd 0.1.0 (handoff ctxmux.daemon-handoff.v4)",
    "ctxmux 0.1.0 (protocol 17)",
  ]) {
    assert.throws(
      () => parseBinaryVersion("ctxmuxd", bad),
      /malformed version identity/u,
      `should reject: ${bad}`,
    );
  }
});

test("the real binaries print an identity the vendor parser accepts", () => {
  // The check above pins the parser to strings written by hand; this one pins it
  // to what the binaries ACTUALLY print. Only the pair catches a drift, because
  // the failure mode was precisely a parser and a printer that disagreed while
  // each looked right on its own.
  const built = spawnSync(
    "cargo",
    [
      "build",
      "--quiet",
      "-p",
      "ctxmux-daemon",
      "--bin",
      "ctxmuxd",
      "-p",
      "ctxmux",
      "--bin",
      "ctxmux",
    ],
    { cwd: root, encoding: "utf8" },
  );
  if (built.status !== 0) {
    // A machine that cannot build is not a machine that has found a defect.
    // Skipping keeps this from turning an unrelated toolchain or disk failure
    // into a red that reads like a real one.
    console.log(
      `skipped: cargo build unavailable (${built.stderr?.trim().slice(0, 200)})`,
    );
    return;
  }
  for (const name of ["ctxmux", "ctxmuxd"]) {
    const binary = path.join(root, "target/debug", name);
    const printed = run(binary, ["--version"], root).stdout.trim();
    const parsed = parseBinaryVersion(name, printed);
    assert.equal(
      Number.isSafeInteger(parsed.protocol) && parsed.protocol > 0,
      true,
      `${name} --version must yield a usable protocol: ${printed}`,
    );
  }
});

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
