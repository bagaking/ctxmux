#!/usr/bin/env node

import { createHash } from "node:crypto";

import contract from "../reliability-gc-contract.json" with { type: "json" };

const [seed, mode, indexText, ...extra] = process.argv.slice(2);

try {
  if (extra.length > 0 || seed === undefined || mode === undefined) {
    throw new TypeError("expected seed, mode, and lifecycle index");
  }
  if (seed !== contract.seed) {
    throw new TypeError("seed does not match the canonical GC contract");
  }
  const modeContract = contract.payload_modes[mode];
  if (modeContract === undefined) {
    throw new TypeError("mode is outside the canonical GC contract");
  }
  if (indexText === undefined || !/^(?:0|[1-9][0-9]*)$/u.test(indexText)) {
    throw new TypeError(
      "lifecycle index must be a canonical non-negative integer",
    );
  }
  const index = Number(indexText);
  if (!Number.isSafeInteger(index)) {
    throw new TypeError("lifecycle index exceeds the safe integer range");
  }

  const digestHex = createHash("sha256")
    .update(`${seed}:${mode}:${index}`, "utf8")
    .digest("hex");
  if (!/^[0-9a-f]{64}$/u.test(digestHex)) {
    throw new Error("SHA-256 hex encoding violated its fixed ASCII contract");
  }

  let remaining = modeContract.hex_repetitions;
  while (remaining > 0) {
    const repetitions = Math.min(remaining, 1024);
    await writeAscii(digestHex.repeat(repetitions));
    remaining -= repetitions;
  }
} catch (error) {
  console.error(
    `ctxmux reliability GC child failed: ${error instanceof Error ? error.message : String(error)}`,
  );
  process.exitCode = 1;
}

function writeAscii(value) {
  return new Promise((resolve, reject) => {
    process.stdout.write(value, "ascii", (error) => {
      if (error) reject(error);
      else resolve();
    });
  });
}
