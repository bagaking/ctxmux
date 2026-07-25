import assert from "node:assert/strict";
import { spawn, type ChildProcess } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";

import {
  Attachment,
  CtxmuxClient,
  registerIntegration,
  type IntegrationObserver,
  type RunEvent,
} from "../packages/sdk/src/index.ts";
import {
  codexIntegration,
  isCodexSessionProvenance,
  type CodexSemanticEvent,
} from "../packages/sdk/src/integrations/index.ts";

const RUN_TIMEOUT_MS = 180_000;
const DAEMON_READY_TIMEOUT_MS = 10_000;
const evidencePath = resolve(
  process.env.CTXMUX_CODEX_CANARY_EVIDENCE ?? "target/codex-canary/result.json",
);
const redactions = [
  process.env.OPENAI_API_KEY,
  process.env.CODEX_API_KEY,
].filter((value): value is string => value !== undefined && value.length > 0);

interface CanaryEvidence {
  readonly schema: "ctxmux.codex-semantic-canary.v1";
  readonly status: "pass" | "fail";
  readonly recorded_at: string;
  readonly codex_version?: string;
  readonly credential_mode?: "api_key" | "codex_login";
  readonly probe_elapsed_ms?: number;
  readonly parent_run_id?: string;
  readonly child_run_id?: string;
  readonly session_id_sha256?: string;
  readonly semantic_fact_sha256?: string;
  readonly child_prompt_contains_fact?: boolean;
  readonly continuation_exact?: boolean;
  readonly fatal_diagnostics_zero?: boolean;
  readonly lineage_fidelity?: string;
  readonly parent_event_names?: readonly string[];
  readonly child_event_names?: readonly string[];
  readonly parent_diagnostic_counts?: Readonly<Record<string, number>>;
  readonly child_diagnostic_counts?: Readonly<Record<string, number>>;
  readonly parent_output_line_classes?: Readonly<Record<string, number>>;
  readonly child_output_line_classes?: Readonly<Record<string, number>>;
  readonly error?: string;
}

let daemon: ChildProcess | undefined;
let fixtureDirectory: string | undefined;

void execute();

async function execute(): Promise<void> {
  try {
    const evidence = await runCanary();
    await writeEvidence(evidence);
    process.stdout.write(`${JSON.stringify(evidence)}\n`);
  } catch (error) {
    const evidence: CanaryEvidence = {
      schema: "ctxmux.codex-semantic-canary.v1",
      status: "fail",
      recorded_at: new Date().toISOString(),
      error: redact(error instanceof Error ? error.message : String(error)),
    };
    await writeEvidence(evidence);
    process.stderr.write(`${JSON.stringify(evidence)}\n`);
    process.exitCode = 1;
  } finally {
    if (daemon !== undefined) {
      await terminate(daemon);
    }
    if (fixtureDirectory !== undefined) {
      await rm(fixtureDirectory, { recursive: true, force: true });
    }
  }
}

async function runCanary(): Promise<CanaryEvidence> {
  const loginOptIn = process.env.CTXMUX_ALLOW_CODEX_LOGIN_AUTH;
  if (loginOptIn !== undefined && loginOptIn !== "0" && loginOptIn !== "1") {
    throw new Error("CTXMUX_ALLOW_CODEX_LOGIN_AUTH must be 0 or 1");
  }
  const credentialMode =
    redactions.length > 0
      ? ("api_key" as const)
      : loginOptIn === "1"
        ? ("codex_login" as const)
        : undefined;
  if (credentialMode === undefined) {
    throw new Error(
      "real Codex canary requires OPENAI_API_KEY, CODEX_API_KEY, or explicit CTXMUX_ALLOW_CODEX_LOGIN_AUTH=1",
    );
  }
  const daemonBinary = requiredEnvironment("CTXMUXD_BIN");
  fixtureDirectory = await mkdtemp(join(tmpdir(), "ctxmux-codex-canary-"));
  const socketPath = join(fixtureDirectory, "ctxmux.sock");
  let daemonStderr = "";
  daemon = spawn(daemonBinary, ["--socket", socketPath], {
    stdio: ["ignore", "ignore", "pipe"],
  });
  daemon.stderr?.on("data", (chunk: Buffer) => {
    daemonStderr += chunk.toString("utf8");
  });

  const client = new CtxmuxClient({ socketPath });
  await waitForDaemon(client, daemon, () => daemonStderr);
  const registered = registerIntegration(client, codexIntegration);
  const probeStarted = performance.now();
  const detection = await registered.detect();
  const probeElapsedMs = Math.round(performance.now() - probeStarted);
  if (detection.status !== "available") {
    throw new Error(`Codex detection failed: ${detection.reason}`);
  }
  if (!detection.capabilities.includes("level_b_fork")) {
    throw new Error("Codex detection did not establish level_b_fork");
  }

  const semanticFact = `ctxmux-canary-${randomUUID()}`;
  redactions.push(semanticFact);
  const parent = await withTimeout(
    registered.start({
      prompt: `Remember this exact nonce for the next turn: ${semanticFact}. Reply exactly ACK.`,
      cwd: process.cwd(),
    }),
    RUN_TIMEOUT_MS,
    "start real Codex parent",
  );
  const parentAttachment = await withTimeout(
    client.attach(parent.id),
    DAEMON_READY_TIMEOUT_MS,
    "attach real Codex parent",
  );
  const parentCollection = await collectSemanticEvents(
    parentAttachment,
    registered.createObserver(parent),
  );
  const parentEvents = parentCollection.events;
  assertNoFatalDiagnostics(parentEvents, "parent");
  const session = parentEvents.find(isCodexSessionProvenance);
  if (session === undefined) {
    throw new Error("real Codex parent emitted no thread.started session");
  }
  redactions.push(session.sessionId);

  const childPrompt =
    "Without using tools or files, reply with only the exact nonce from the previous turn.";
  assert.equal(childPrompt.includes(semanticFact), false);
  const child = await withTimeout(
    registered.forkLevelB(parent, {
      session,
      prompt: childPrompt,
      cwd: process.cwd(),
    }),
    RUN_TIMEOUT_MS,
    "start real Codex continuation",
  );
  assert.deepEqual(child.lineage, {
    parent: parent.id,
    fidelity: "level_b",
  });
  assert.equal(JSON.stringify(child.spec).includes(semanticFact), false);
  const childAttachment = await withTimeout(
    client.attach(child.id),
    DAEMON_READY_TIMEOUT_MS,
    "attach real Codex continuation",
  );
  const childCollection = await collectSemanticEvents(
    childAttachment,
    registered.createObserver(child),
  );
  const childEvents = childCollection.events;
  assertNoFatalDiagnostics(childEvents, "child");
  const continuation = lastAgentMessage(childEvents);
  if (continuation?.trim() !== semanticFact) {
    throw new Error(
      "real Codex continuation did not return the exact parent fact",
    );
  }

  return {
    schema: "ctxmux.codex-semantic-canary.v1",
    status: "pass",
    recorded_at: new Date().toISOString(),
    codex_version: detection.version ?? "unknown",
    credential_mode: credentialMode,
    probe_elapsed_ms: probeElapsedMs,
    parent_run_id: parent.id,
    child_run_id: child.id,
    session_id_sha256: sha256(session.sessionId),
    semantic_fact_sha256: sha256(semanticFact),
    child_prompt_contains_fact: false,
    continuation_exact: true,
    fatal_diagnostics_zero: true,
    lineage_fidelity: child.lineage?.fidelity,
    parent_event_names: uniqueEventNames(parentEvents),
    child_event_names: uniqueEventNames(childEvents),
    parent_diagnostic_counts: diagnosticCounts(parentEvents),
    child_diagnostic_counts: diagnosticCounts(childEvents),
    parent_output_line_classes: classifyOutputLines(parentCollection.rawBytes),
    child_output_line_classes: classifyOutputLines(childCollection.rawBytes),
  };
}

function assertNoFatalDiagnostics(
  events: readonly CodexSemanticEvent[],
  owner: string,
): void {
  const fatalReasons = new Set([
    "output_gap",
    "invalid_utf8",
    "record_too_large",
  ]);
  for (const event of events) {
    if (
      event.name === "integration.parse_error" &&
      typeof event.data.reason === "string" &&
      fatalReasons.has(event.data.reason)
    ) {
      throw new Error(
        `real Codex ${owner} emitted fatal semantic diagnostic ${event.data.reason}`,
      );
    }
  }
}

function uniqueEventNames(
  events: readonly CodexSemanticEvent[],
): readonly string[] {
  return [...new Set(events.map(({ name }) => name))];
}

function diagnosticCounts(
  events: readonly CodexSemanticEvent[],
): Readonly<Record<string, number>> {
  const counts: Record<string, number> = {};
  for (const event of events) {
    if (event.name !== "integration.parse_error") {
      continue;
    }
    const reason =
      typeof event.data.reason === "string" ? event.data.reason : "unknown";
    counts[reason] = (counts[reason] ?? 0) + 1;
  }
  return counts;
}

async function collectSemanticEvents(
  attachment: Attachment,
  observer: IntegrationObserver<CodexSemanticEvent>,
): Promise<{
  readonly events: readonly CodexSemanticEvent[];
  readonly rawBytes: Uint8Array<ArrayBufferLike>;
}> {
  const events: CodexSemanticEvent[] = [];
  let rawBytes: Uint8Array<ArrayBufferLike> = new Uint8Array();
  for (const chunk of attachment.snapshot.replay.chunks) {
    rawBytes = appendBytes(rawBytes, Uint8Array.from(chunk.data));
    events.push(...observer.observe({ type: "output", chunk }));
  }
  const deadline = Date.now() + RUN_TIMEOUT_MS;
  while (Date.now() <= deadline) {
    const event = await withTimeout(
      attachment.nextEvent(),
      Math.max(1, deadline - Date.now()),
      "wait for real Codex event",
    );
    if (event === undefined) {
      break;
    }
    if (event.type === "output") {
      rawBytes = appendBytes(rawBytes, Uint8Array.from(event.chunk.data));
    }
    events.push(...observer.observe(event));
    if (event.type === "gap") {
      throw new Error(
        `real Codex canary observed output gap at ${event.head_seq}`,
      );
    }
    if (event.type === "exited") {
      if (event.state.type !== "exited" || event.state.code !== 0) {
        throw new Error(
          event.state.type === "exited"
            ? `real Codex Run exited unsuccessfully: code=${event.state.code} signal=${event.state.signal ?? "none"}`
            : "real Codex exited event carried a running state",
        );
      }
      return { events, rawBytes };
    }
  }
  throw new Error("real Codex Run did not reach terminal state");
}

function classifyOutputLines(
  rawBytes: Uint8Array<ArrayBufferLike>,
): Readonly<Record<string, number>> {
  const counts: Record<string, number> = {};
  const lines = new TextDecoder().decode(rawBytes).split("\n");
  for (const rawLine of lines) {
    const line = rawLine.endsWith("\r") ? rawLine.slice(0, -1) : rawLine;
    const stripped = stripAnsi(line);
    const classification =
      line.length === 0
        ? "blank"
        : isJson(line)
          ? "json"
          : stripped !== line && isJson(stripped)
            ? "json_after_ansi_strip"
            : stripped !== line
              ? "ansi_non_json"
              : "other_non_json";
    counts[classification] = (counts[classification] ?? 0) + 1;
  }
  return counts;
}

function stripAnsi(value: string): string {
  return value
    .replaceAll(/\u001B\][^\u0007]*(?:\u0007|\u001B\\)/gu, "")
    .replaceAll(/\u001B\[[0-?]*[ -/]*[@-~]/gu, "");
}

function isJson(value: string): boolean {
  try {
    JSON.parse(value);
    return true;
  } catch {
    return false;
  }
}

function appendBytes(
  left: Uint8Array<ArrayBufferLike>,
  right: Uint8Array<ArrayBufferLike>,
): Uint8Array<ArrayBufferLike> {
  const output = new Uint8Array(left.length + right.length);
  output.set(left);
  output.set(right, left.length);
  return output;
}

function lastAgentMessage(
  events: readonly CodexSemanticEvent[],
): string | undefined {
  for (const event of [...events].reverse()) {
    const item = event.data.item;
    if (
      isRecord(item) &&
      item.type === "agent_message" &&
      typeof item.text === "string"
    ) {
      return item.text;
    }
  }
  return undefined;
}

async function waitForDaemon(
  client: CtxmuxClient,
  child: ChildProcess,
  stderr: () => string,
): Promise<void> {
  const deadline = Date.now() + DAEMON_READY_TIMEOUT_MS;
  while (Date.now() <= deadline) {
    if (child.exitCode !== null) {
      throw new Error(
        `ctxmuxd exited before canary startup; stderr_sha256=${sha256(stderr())}`,
      );
    }
    try {
      await client.ping();
      return;
    } catch {
      await delay(20);
    }
  }
  throw new Error(
    `ctxmuxd did not become ready; stderr_sha256=${sha256(stderr())}`,
  );
}

async function terminate(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null) {
    return;
  }
  child.kill("SIGINT");
  await Promise.race([
    new Promise<void>((resolveExit) => child.once("exit", () => resolveExit())),
    delay(2_000).then(() => {
      child.kill("SIGKILL");
    }),
  ]);
}

async function withTimeout<T>(
  operation: Promise<T>,
  timeoutMs: number,
  label: string,
): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      operation,
      new Promise<never>((_resolve, reject) => {
        timer = setTimeout(
          () => reject(new Error(`${label} exceeded ${timeoutMs}ms`)),
          timeoutMs,
        );
      }),
    ]);
  } finally {
    if (timer !== undefined) {
      clearTimeout(timer);
    }
  }
}

async function writeEvidence(evidence: CanaryEvidence): Promise<void> {
  await mkdir(dirname(evidencePath), { recursive: true });
  await writeFile(evidencePath, `${JSON.stringify(evidence, null, 2)}\n`, {
    mode: 0o600,
  });
}

function redact(value: string): string {
  return redactions.reduce(
    (redacted, secret) => redacted.replaceAll(secret, "[REDACTED]"),
    value,
  );
}

function sha256(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

function requiredEnvironment(name: string): string {
  const value = process.env[name];
  if (value === undefined || value.length === 0) {
    throw new Error(`${name} is required for the real Codex canary`);
  }
  return value;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolveDelay) => setTimeout(resolveDelay, milliseconds));
}
