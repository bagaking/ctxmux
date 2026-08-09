import assert from "node:assert/strict";
import test from "node:test";

import { PROTOCOL_VERSION, defineRun, versionInfo } from "../src/index.ts";

test("versionInfo uses the shared SDK protocol generation", () => {
  assert.deepEqual(versionInfo("0.0.0"), {
    product: "0.0.0",
    protocol: PROTOCOL_VERSION,
  });
});

test("versionInfo rejects an empty product version", () => {
  assert.throws(() => versionInfo(""), /must not be empty/);
});

test("defineRun fills only portable protocol defaults", () => {
  assert.deepEqual(defineRun("/bin/sh"), {
    program: "/bin/sh",
    args: [],
    cwd: null,
    env: {},
    size: { cols: 80, rows: 24 },
    declared_inputs: [],
  });
});
