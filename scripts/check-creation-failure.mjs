import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import {
  cpSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const evidence = mkdtempSync(join(root, "target", "creation-failure-proof-"));
const env = { ...process.env, CARGO_INCREMENTAL: "0" };
const results = [];
const hash = (file) =>
  createHash("sha256").update(readFileSync(file)).digest("hex");

function run(name, command, args, options = {}) {
  const result = spawnSync(command, args, {
    cwd: root,
    env,
    encoding: "utf8",
    maxBuffer: 16 * 1024 * 1024,
    ...options,
  });
  writeFileSync(
    join(evidence, `${name}.log`),
    `${result.stdout ?? ""}${result.stderr ?? ""}`,
  );
  results.push({ name, command, args, status: result.status });
  assert.ifError(result.error);
  return result;
}

function passes(name, command, args, options) {
  const result = run(name, command, args, options);
  assert.equal(result.status, 0, `${name}: ${result.stderr}\n${result.stdout}`);
  return result.stdout;
}

function cargoExecutable(name, args, target) {
  const output = passes(name, "cargo", [
    ...args,
    "--offline",
    "--message-format=json",
  ]);
  const artifacts = output
    .trim()
    .split("\n")
    .map((line) => JSON.parse(line))
    .filter((item) => item.reason === "compiler-artifact" && item.executable);
  const executable = artifacts.find((item) => item.target.name === target);
  assert.ok(executable, `cargo did not identify ${target}`);
  return executable.executable;
}

const inputs = passes("source-inputs", "git", [
  "ls-files",
  "--cached",
  "--others",
  "--exclude-standard",
  "crates",
  "packages/sdk/src",
  "packages/sdk/test/creation-failure.e2e.test.ts",
  "scripts/check-creation-failure.mjs",
  "Cargo.lock",
  "Cargo.toml",
])
  .trim()
  .split("\n");
const sourceHashes = Object.fromEntries(
  inputs.map((file) => [file, hash(join(root, file))]),
);
const daemon = cargoExecutable(
  "daemon-build",
  ["test", "-p", "ctxmux-daemon", "--lib", "--no-run"],
  "ctxmux_daemon",
);
const cli = cargoExecutable("cli-build", ["build", "-p", "ctxmux"], "ctxmux");
passes("sdk-build", "npm", ["run", "build", "--workspace", "@ctxmux/sdk"]);
const testEnv = {
  ...env,
  CTXMUX_CREATION_TEST_DAEMON: daemon,
  CTXMUX_BIN: cli,
};
const tsx = join(root, "node_modules", ".bin", "tsx");
const testPath = "test/creation-failure.e2e.test.ts";
const candidate = passes(
  "candidate",
  tsx,
  ["--test", "--test-reporter=tap", testPath],
  {
    cwd: join(root, "packages", "sdk"),
    env: testEnv,
  },
);
assert.match(candidate, /# tests 2\b/u);
assert.match(candidate, /# pass 2\b/u);

const variant = join(evidence, "mutant");
mkdirSync(variant);
for (const file of [
  "src",
  "test",
  "package.json",
  "tsconfig.json",
  "tsconfig.build.json",
])
  cpSync(join(root, "packages", "sdk", file), join(variant, file), {
    recursive: true,
  });
symlinkSync(join(root, "node_modules"), join(variant, "node_modules"), "dir");
const control = join(variant, "src", "control.ts");
const source = readFileSync(control, "utf8");
const original = "new CtxmuxProtocolError(error.code, error.message, error)";
assert.equal(source.split(original).length, 2, "one loaded mutation anchor");
writeFileSync(
  control,
  source.replace(
    original,
    "new CtxmuxProtocolError(error.code, error.message)",
  ),
);
passes("mutant-build", join(root, "node_modules", ".bin", "tsc"), [
  "-p",
  join(variant, "tsconfig.build.json"),
]);
const mutant = run("mutant", tsx, ["--test", "--test-reporter=tap", testPath], {
  cwd: variant,
  env: testEnv,
});
assert.equal(mutant.status, 1, "dropping original owner facts must fail");
assert.ok(mutant.stdout.includes("ERR_ASSERTION"));
assert.match(mutant.stdout, /# tests 2\b/u);
assert.match(mutant.stdout, /# fail 1\b/u);
for (const [file, expected] of Object.entries(sourceHashes))
  assert.equal(hash(join(root, file)), expected, `source drift: ${file}`);
writeFileSync(
  join(evidence, "receipt.json"),
  JSON.stringify(
    {
      sourceHashes,
      artifacts: Object.fromEntries(
        [
          daemon,
          cli,
          join(root, "packages", "sdk", "dist", "control.js"),
          join(variant, "dist", "control.js"),
        ].map((file) => [file, hash(file)]),
      ),
      mutation:
        "discard structured creation and storage facts in SDK conversion",
      results,
    },
    null,
    2,
  ),
);
process.stdout.write(`Creation failure contract verified: ${evidence}\n`);
