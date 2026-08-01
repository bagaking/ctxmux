import assert from "node:assert/strict";
import test from "node:test";

import {
  MAX_CREATE_OPERATION_KEY_BYTES,
  PROTOCOL_VERSION,
  createOperationKey,
  defineRun,
  versionInfo,
} from "../src/index.ts";

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

test("createOperationKey enforces the exact UTF-8 byte ceiling", () => {
  assert.equal(
    createOperationKey("x".repeat(MAX_CREATE_OPERATION_KEY_BYTES)).length,
    128,
  );
  assert.throws(() => createOperationKey(""), /must not be empty/);
  assert.throws(
    () => createOperationKey("x".repeat(MAX_CREATE_OPERATION_KEY_BYTES + 1)),
    /maximum is 128/,
  );
  assert.doesNotThrow(() => createOperationKey("界".repeat(42)));
  assert.throws(() => createOperationKey("界".repeat(43)), /129 bytes/);
  assert.throws(() => createOperationKey("\ud800"), /well-formed UTF-16/);
  assert.throws(() => createOperationKey("\udc00"), /well-formed UTF-16/);
  assert.throws(() => createOperationKey("\ud800x"), /well-formed UTF-16/);
  assert.doesNotThrow(() => createOperationKey(`${"x".repeat(124)}😀`));
  assert.throws(() => createOperationKey(`${"x".repeat(125)}😀`), /129 bytes/);
});

test("createOperationKey rejects non-string values before coercion", () => {
  for (const value of [42, null, { key: "valid-looking" }]) {
    assert.throws(
      () => createOperationKey(value as unknown as string),
      /operation key must be a string/,
    );
  }
});
