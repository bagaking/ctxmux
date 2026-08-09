import assert from "node:assert/strict";
import test from "node:test";

import type { AvailableIntegrationDetection } from "../src/index.ts";
import { shellIntegration } from "../src/integrations/index.ts";

test("shell Integration detects an explicit executable without discovery", async () => {
  assert.deepEqual(await shellIntegration.detect({ executable: "/bin/sh" }), {
    status: "available",
    executable: "/bin/sh",
    version: null,
    capabilities: [],
  });
  assert.deepEqual(
    await shellIntegration.detect({ executable: "/ctxmux/missing-shell" }),
    {
      status: "unavailable",
      executable: "/ctxmux/missing-shell",
      reason: "not_found",
    },
  );
});

test("shell Integration preserves structured argv without implicit evaluation", () => {
  const detection: AvailableIntegrationDetection = {
    status: "available",
    executable: "/bin/sh",
    version: null,
    capabilities: [],
  };
  const args = ["-c", "printf '%s\\n'", "a;$(touch never)\nnext"];

  assert.deepEqual(
    shellIntegration.planLaunch(
      {
        args,
        cwd: "/workspace with spaces",
        env: { DECLARED: "one two" },
        size: { cols: 120, rows: 40 },
      },
      detection,
    ),
    {
      program: "/bin/sh",
      args,
      cwd: "/workspace with spaces",
      env: { DECLARED: "one two" },
      size: { cols: 120, rows: 40 },
    },
  );
});

test("shell Integration has a disposable observer with no semantic claims", () => {
  assert.deepEqual(
    shellIntegration
      .createObserver()
      .observe({ type: "output", chunk: { seq: 1, data: [65] } }),
    [],
  );
});
