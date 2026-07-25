#!/usr/bin/env node

import { spawnSync } from "node:child_process";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const seed = positiveInteger(
  "CTXMUX_RELIABILITY_SEED",
  process.env.CTXMUX_RELIABILITY_SEED ?? "827541837",
);
const fuzzCases = positiveInteger(
  "CTXMUX_FUZZ_CASES",
  process.env.CTXMUX_FUZZ_CASES ?? "10000",
);
const modelCases = positiveInteger(
  "CTXMUX_MODEL_CASES",
  process.env.CTXMUX_MODEL_CASES ?? "128",
);
const evidencePath = resolve(
  root,
  process.env.CTXMUX_SEEDED_EVIDENCE ??
    "target/reliability/seeded-qualification.json",
);
const sharedEnvironment = {
  ...process.env,
  CTXMUX_FUZZ_SEED: String(seed),
  CTXMUX_FUZZ_CASES: String(fuzzCases),
  CTXMUX_MODEL_SEED: String(seed),
  CTXMUX_MODEL_CASES: String(modelCases),
};
const commands = [
  {
    id: "native-protocol-fuzz",
    command: "cargo",
    args: [
      "test",
      "--locked",
      "-p",
      "ctxmux-protocol",
      "seeded_native_protocol_fuzz_target_is_total_and_round_trips_valid_frames",
      "--",
      "--nocapture",
    ],
    boundary:
      "ctxmux-protocol byte cap, duplicate-member guard, serde decode, and typed round-trip; no daemon or PTY claim",
  },
  {
    id: "multi-client-mutation-model",
    command: "cargo",
    args: [
      "test",
      "--locked",
      "-p",
      "ctxmux-daemon",
      "seeded_multi_client_mutation_model_accepts_only_declared_outcomes",
      "--",
      "--nocapture",
    ],
    boundary:
      "real socket clients racing input, resize, and direct-child stop against one daemon Run; no writer or resize arbitration claim",
  },
  {
    id: "typescript-wire-codex-fuzz",
    command: process.execPath,
    args: [
      "--import",
      "tsx",
      "--test",
      "packages/sdk/test/parser-fuzz.test.ts",
    ],
    boundary:
      "TypeScript NDJSON framing/runtime validation and Codex JSONL observation; no external Codex semantic-continuation claim",
  },
];

const receipt = {
  schema: "ctxmux.seeded-qualification.v1",
  status: "running",
  recorded_at: new Date().toISOString(),
  completed_at: null,
  seed,
  fuzz_cases: fuzzCases,
  model_cases: modelCases,
  environment: {
    os: process.platform,
    architecture: process.arch,
    node: process.version,
    rustc: toolVersion("rustc", ["--version"]),
  },
  excluded_boundaries: [
    "This lane is seeded parser and protocol-model evidence, not a sanitizer or data-race detector.",
    "PTY backend portability, chaos, resource census, and long-running soak remain separately owned qualification work.",
  ],
  commands: commands.map(({ id, command, args, boundary }) => ({
    id,
    command,
    args,
    boundary,
  })),
  results: [],
};

writeReceipt();
for (const entry of commands) {
  const startedAt = new Date().toISOString();
  const result = spawnSync(entry.command, entry.args, {
    cwd: root,
    env: sharedEnvironment,
    stdio: "inherit",
  });
  const exitCode = result.status ?? 1;
  receipt.results.push({
    id: entry.id,
    started_at: startedAt,
    completed_at: new Date().toISOString(),
    exit_code: exitCode,
    signal: result.signal ?? null,
  });
  if (exitCode !== 0) {
    receipt.status = "fail";
    receipt.completed_at = new Date().toISOString();
    writeReceipt();
    process.exitCode = exitCode;
    break;
  }
  writeReceipt();
}

if (receipt.status === "running") {
  receipt.status = "pass";
  receipt.completed_at = new Date().toISOString();
  writeReceipt();
}

function positiveInteger(name, value) {
  const parsed = Number.parseInt(value, 10);
  if (
    !Number.isSafeInteger(parsed) ||
    parsed <= 0 ||
    String(parsed) !== value
  ) {
    throw new TypeError(`${name} must be a canonical positive integer`);
  }
  return parsed;
}

function toolVersion(command, args) {
  const result = spawnSync(command, args, { cwd: root, encoding: "utf8" });
  return result.status === 0 ? result.stdout.trim() : "unavailable";
}

function writeReceipt() {
  mkdirSync(dirname(evidencePath), { recursive: true });
  writeFileSync(evidencePath, `${JSON.stringify(receipt, null, 2)}\n`);
}
