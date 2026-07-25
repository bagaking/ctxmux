import assert from "node:assert/strict";
import { chmod, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test, { type TestContext } from "node:test";

import type {
  AvailableIntegrationDetection,
  RunEvent,
  RunInfo,
} from "../src/index.ts";
import {
  Attachment,
  IntegrationProvenanceError,
  registerIntegration,
} from "../src/index.ts";
import {
  codexIntegration,
  isCodexSessionProvenance,
  type CodexSessionProvenance,
  type CodexSemanticEvent,
} from "../src/integrations/index.ts";

const FIXTURE_PROBE_TIMEOUT_MS = 5_000;

test("Codex default probing tolerates the supported cold-start envelope", async (context) => {
  const delayed = await executable(
    context,
    probeProgram(
      "codex-cli 0.144.4",
      "      --json  Print JSONL",
      "      --json  Resume as JSONL",
      1_250,
    ),
  );

  assert.deepEqual(await codexIntegration.detect({ executable: delayed }), {
    status: "available",
    executable: delayed,
    version: "0.144.4",
    capabilities: ["semantic_events", "level_b_fork"],
  });
});

test("Codex detection fails closed across missing, malformed, incompatible, and hanging probes", async (context) => {
  const compatible = await executable(
    context,
    probeProgram(
      "codex-cli 0.144.4",
      "      --json  Print JSONL",
      "      --json  Resume as JSONL",
    ),
  );
  assert.deepEqual(
    await codexIntegration.detect({
      executable: compatible,
      timeoutMs: FIXTURE_PROBE_TIMEOUT_MS,
    }),
    {
      status: "available",
      executable: compatible,
      version: "0.144.4",
      capabilities: ["semantic_events", "level_b_fork"],
    },
  );

  const semanticOnly = await executable(
    context,
    probeProgram("codex-cli 0.144.4", "      --json  Print JSONL"),
  );
  assert.deepEqual(
    await codexIntegration.detect({
      executable: semanticOnly,
      timeoutMs: FIXTURE_PROBE_TIMEOUT_MS,
    }),
    {
      status: "available",
      executable: semanticOnly,
      version: "0.144.4",
      capabilities: ["semantic_events"],
    },
  );

  assert.deepEqual(
    await codexIntegration.detect({
      executable: join(tmpdir(), "ctxmux-missing-codex"),
      timeoutMs: FIXTURE_PROBE_TIMEOUT_MS,
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
  assert.deepEqual(
    await codexIntegration.detect({
      executable: malformed,
      timeoutMs: FIXTURE_PROBE_TIMEOUT_MS,
    }),
    {
      status: "unavailable",
      executable: malformed,
      reason: "invalid_version",
    },
  );

  const incompatible = await executable(
    context,
    probeProgram("codex-cli 0.144.4", "      --color"),
  );
  assert.deepEqual(
    await codexIntegration.detect({
      executable: incompatible,
      timeoutMs: FIXTURE_PROBE_TIMEOUT_MS,
    }),
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
  assert.deepEqual(
    await codexIntegration.detect({
      executable: failed,
      timeoutMs: FIXTURE_PROBE_TIMEOUT_MS,
    }),
    {
      status: "unavailable",
      executable: failed,
      reason: "probe_failed",
    },
  );
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
      declared_inputs: [
        { kind: "workspace", reference: "/workspace with spaces" },
      ],
    },
  );
});

test("Codex Level B planning resumes one declared native session", () => {
  const detection: AvailableIntegrationDetection = {
    status: "available",
    executable: "/opt/codex",
    version: "0.144.4",
    capabilities: ["semantic_events", "level_b_fork"],
  };
  const parent: RunInfo = {
    id: "00000000-0000-0000-0000-000000000001",
    spec: {
      program: "/opt/codex",
      args: ["exec", "--json", "--", "first"],
      cwd: "/workspace with spaces",
      env: {},
      size: { cols: 80, rows: 24 },
      declared_inputs: [
        { kind: "workspace", reference: "/workspace with spaces" },
      ],
    },
    lineage: null,
    backend: { type: "native" },
    capabilities: {
      input: true,
      resize: true,
      stop: true,
      fork_level_a: true,
      fork_level_b: true,
      replay: "raw_from_start",
    },
    pid: 123,
    state: { type: "running" },
    head_seq: 1,
    durable_head_seq: null,
    oldest_seq: 1,
    attachments: 0,
  };
  const prompt = "continue 'exactly'; $(touch never)";
  const observedSession = codexIntegration
    .createObserver()
    .observe(
      output(1, [
        ...new TextEncoder().encode(
          `${JSON.stringify({ type: "thread.started", thread_id: "session-123" })}\n`,
        ),
      ]),
    )[0];
  assert.notEqual(observedSession, undefined);
  assert.equal(isCodexSessionProvenance(observedSession!), true);

  assert.deepEqual(
    codexIntegration.planLevelBFork?.(
      parent,
      {
        session: observedSession as CodexSessionProvenance,
        prompt,
        cwd: "/workspace with spaces",
        env: { DECLARED: "one two" },
        size: { cols: 120, rows: 40 },
        artifactReferences: ["artifact://plan.json"],
      },
      detection,
    ),
    {
      type: "level_b",
      spec: {
        program: "/opt/codex",
        args: ["exec", "resume", "--json", "--", "session-123", prompt],
        cwd: "/workspace with spaces",
        env: { DECLARED: "one two" },
        size: { cols: 120, rows: 40 },
        declared_inputs: [
          { kind: "workspace", reference: "/workspace with spaces" },
          { kind: "artifact", reference: "artifact://plan.json" },
          { kind: "context", reference: "session-123" },
        ],
      },
    },
  );
});

test("Codex Level B rejects unrelated and unverifiable provenance before raw fork", async (context) => {
  const executablePath = await executable(
    context,
    probeProgram(
      "codex-cli 0.144.4",
      "      --json  Print JSONL",
      "      --json  Resume as JSONL",
    ),
  );
  const parent = rootRun("00000000-0000-0000-0000-000000000001");
  const unrelated = rootRun("00000000-0000-0000-0000-000000000002");
  let rawForks = 0;
  const client = {
    async start(): Promise<RunInfo> {
      throw new Error("unreachable raw start");
    },
    async fork(): Promise<RunInfo> {
      rawForks += 1;
      throw new Error("unreachable raw fork");
    },
  };
  const registered = registerIntegration(client, codexIntegration);
  const ownedOutput = sourcedOutput(parent, 1, [
    ...new TextEncoder().encode(
      `${JSON.stringify({ type: "thread.started", thread_id: "session-123" })}\n`,
    ),
  ]);
  if (ownedOutput.type !== "output") {
    throw new Error("source fixture returned a non-output event");
  }
  assert.throws(
    () =>
      registered.createObserver(parent).observe({
        type: "output",
        chunk: {
          seq: ownedOutput.chunk.seq,
          data: [...ownedOutput.chunk.data],
        },
      }),
    (error: unknown) => error instanceof IntegrationProvenanceError,
  );
  assert.throws(
    () =>
      registered
        .createObserver(parent)
        .observe(
          sourcedOutput(unrelated, 1, [
            ...new TextEncoder().encode(
              `${JSON.stringify({ type: "thread.started", thread_id: "wrong-source" })}\n`,
            ),
          ]),
        ),
    (error: unknown) => error instanceof IntegrationProvenanceError,
  );
  const observedSession = registered
    .createObserver(parent)
    .observe(ownedOutput)[0];
  assert.notEqual(observedSession, undefined);
  assert.equal(isCodexSessionProvenance(observedSession!), true);
  const session = observedSession as CodexSessionProvenance;
  const config = { session, prompt: "continue", cwd: "/workspace" };

  await assert.rejects(
    registered.forkLevelB(unrelated, config, { executable: executablePath }),
    (error: unknown) => error instanceof IntegrationProvenanceError,
  );
  await assert.rejects(
    registered.forkLevelB(
      parent,
      { ...config, session: { ...session } },
      { executable: executablePath },
    ),
    (error: unknown) => error instanceof IntegrationProvenanceError,
  );
  const unboundSession = registered
    .createObserver()
    .observe(
      output(2, [
        ...new TextEncoder().encode(
          `${JSON.stringify({ type: "thread.started", thread_id: "session-unbound" })}\n`,
        ),
      ]),
    )[0];
  assert.notEqual(unboundSession, undefined);
  assert.equal(isCodexSessionProvenance(unboundSession!), true);
  await assert.rejects(
    registered.forkLevelB(
      parent,
      { ...config, session: unboundSession as CodexSessionProvenance },
      { executable: executablePath },
    ),
    (error: unknown) => error instanceof IntegrationProvenanceError,
  );
  await assert.rejects(
    registerIntegration(client, codexIntegration).forkLevelB(parent, config, {
      executable: executablePath,
    }),
    (error: unknown) => error instanceof IntegrationProvenanceError,
  );
  assert.equal(rawForks, 0);
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

function sourcedOutput(run: RunInfo, seq: number, data: number[]): RunEvent {
  const event = output(seq, data);
  if (event.type !== "output") {
    throw new Error("output fixture returned a non-output event");
  }
  new Attachment({} as never, {
    run,
    replay: {
      chunks: [event.chunk],
      oldest_seq: seq,
      head_seq: seq,
      truncated: false,
    },
  });
  return event;
}

function diagnostic(reason: string) {
  return {
    integrationId: "codex",
    name: "integration.parse_error",
    data: { reason },
  };
}

function rootRun(id: RunInfo["id"]): RunInfo {
  return {
    id,
    spec: {
      program: "/opt/codex",
      args: ["exec", "--json", "--", "first"],
      cwd: "/workspace",
      env: {},
      size: { cols: 80, rows: 24 },
      declared_inputs: [{ kind: "workspace", reference: "/workspace" }],
    },
    lineage: null,
    backend: { type: "native" },
    capabilities: {
      input: true,
      resize: true,
      stop: true,
      fork_level_a: true,
      fork_level_b: true,
      replay: "raw_from_start",
    },
    pid: 123,
    state: { type: "running" },
    head_seq: 1,
    durable_head_seq: null,
    oldest_seq: 1,
    attachments: 0,
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

function probeProgram(
  version: string,
  help: string,
  resumeHelp = "",
  versionDelayMs = 0,
): string {
  return `
const args = process.argv.slice(2);
if (args[0] === "--version") {
  setTimeout(() => process.stdout.write(${JSON.stringify(`${version}\n`)}), ${versionDelayMs});
} else if (args[0] === "exec" && args[1] === "--help") {
  process.stdout.write(${JSON.stringify(`${help}\n`)});
} else if (args[0] === "exec" && args[1] === "resume" && args[2] === "--help") {
  process.stdout.write(${JSON.stringify(`${resumeHelp}\n`)});
} else {
  process.exitCode = 64;
}`;
}
