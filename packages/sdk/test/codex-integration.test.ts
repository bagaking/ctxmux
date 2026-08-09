import assert from "node:assert/strict";
import { chmod, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test, { type TestContext } from "node:test";

import type { AvailableIntegrationDetection, RunEvent } from "../src/index.ts";
import {
  codexIntegration,
  type CodexSemanticEvent,
} from "../src/integrations/index.ts";

test("Codex detection fails closed across missing, malformed, incompatible, and hanging probes", async (context) => {
  const compatible = await executable(
    context,
    probeProgram("codex-cli 0.144.4", "      --json  Print JSONL"),
  );
  assert.deepEqual(await codexIntegration.detect({ executable: compatible }), {
    status: "available",
    executable: compatible,
    version: "0.144.4",
    capabilities: ["semantic_events"],
  });

  assert.deepEqual(
    await codexIntegration.detect({
      executable: join(tmpdir(), "ctxmux-missing-codex"),
    }),
    {
      status: "unavailable",
      executable: join(tmpdir(), "ctxmux-missing-codex"),
      reason: "not_found",
    },
  );

  const malformed = await executable(
    context,
    probeProgram("surprising version output", "      --json"),
  );
  assert.deepEqual(await codexIntegration.detect({ executable: malformed }), {
    status: "unavailable",
    executable: malformed,
    reason: "invalid_version",
  });

  const incompatible = await executable(
    context,
    probeProgram("codex-cli 0.144.4", "      --color"),
  );
  assert.deepEqual(
    await codexIntegration.detect({ executable: incompatible }),
    {
      status: "unavailable",
      executable: incompatible,
      reason: "missing_capability",
    },
  );

  const hanging = await executable(context, "setInterval(() => {}, 1_000);");
  assert.deepEqual(
    await codexIntegration.detect({ executable: hanging, timeoutMs: 25 }),
    {
      status: "unavailable",
      executable: hanging,
      reason: "probe_timeout",
    },
  );

  const failed = "/usr/bin/false";
  assert.deepEqual(await codexIntegration.detect({ executable: failed }), {
    status: "unavailable",
    executable: failed,
    reason: "probe_failed",
  });
});

test("Codex launch planning keeps the prompt in one exact argv value", () => {
  const detection: AvailableIntegrationDetection = {
    status: "available",
    executable: "/opt/codex",
    version: "0.144.4",
    capabilities: ["semantic_events"],
  };
  const prompt = "review 'quoted'; $(touch never)\nthen explain";

  assert.deepEqual(
    codexIntegration.planLaunch(
      {
        prompt,
        cwd: "/workspace with spaces",
        env: { DECLARED: "one two" },
        size: { cols: 120, rows: 40 },
      },
      detection,
    ),
    {
      program: "/opt/codex",
      args: ["exec", "--json", "--", prompt],
      cwd: "/workspace with spaces",
      env: { DECLARED: "one two" },
      size: { cols: 120, rows: 40 },
      declared_inputs: [],
    },
  );
});

test("Codex observer normalizes partitioned JSONL and isolates parser failures", () => {
  const observer = codexIntegration.createObserver();
  const jsonl = [
    JSON.stringify({ type: "thread.started", thread_id: "线程-1" }),
    JSON.stringify({ type: "turn.completed", usage: { total_tokens: 12 } }),
  ]
    .join("\n")
    .concat("\n");
  const events = [...new TextEncoder().encode(jsonl)].flatMap((byte, index) =>
    observer.observe(output(index + 1, [byte])),
  );

  assert.deepEqual(
    events.map((event) => event.name),
    ["thread.started", "turn.completed"],
  );
  assert.equal(events[0]?.data.thread_id, "线程-1");

  assert.deepEqual(
    observer.observe(output(999, [...new TextEncoder().encode("not json\n")])),
    [diagnostic("invalid_json")],
  );
  observer.observe(output(1_000, [0xe4]));
  assert.deepEqual(observer.observe({ type: "gap", head_seq: 1_001 }), [
    diagnostic("output_gap"),
  ]);
  assert.deepEqual(observer.observe(output(1_002, [0xff, 0x0a])), [
    diagnostic("invalid_utf8"),
  ]);

  const oversizedDiagnostics: CodexSemanticEvent[] = [];
  for (let index = 0; index < 129; index += 1) {
    oversizedDiagnostics.push(
      ...observer.observe(output(2_000 + index, Array(8192).fill(0x78))),
    );
  }
  assert.deepEqual(oversizedDiagnostics, [diagnostic("record_too_large")]);

  const final = JSON.stringify({ type: "item.completed", item: { id: "x" } });
  observer.observe(output(1_003, [...new TextEncoder().encode(final)]));
  assert.deepEqual(
    observer.observe({
      type: "exited",
      state: { type: "exited", code: 0, signal: null },
    }),
    [
      {
        integrationId: "codex",
        name: "item.completed",
        data: { type: "item.completed", item: { id: "x" } },
      },
    ],
  );
});

function output(seq: number, data: number[]): RunEvent {
  return { type: "output", chunk: { seq, data } };
}

function diagnostic(reason: string) {
  return {
    integrationId: "codex",
    name: "integration.parse_error",
    data: { reason },
  };
}

async function executable(context: TestContext, body: string): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), "ctxmux-codex-probe-"));
  const path = join(directory, "codex.mjs");
  await writeFile(path, `#!/usr/bin/env node\n${body}\n`);
  await chmod(path, 0o755);
  context.after(() => rm(directory, { recursive: true, force: true }));
  return path;
}

function probeProgram(version: string, help: string): string {
  return `
const args = process.argv.slice(2);
if (args[0] === "--version") {
  process.stdout.write(${JSON.stringify(`${version}\n`)});
} else if (args[0] === "exec" && args[1] === "--help") {
  process.stdout.write(${JSON.stringify(`${help}\n`)});
} else {
  process.exitCode = 64;
}`;
}
