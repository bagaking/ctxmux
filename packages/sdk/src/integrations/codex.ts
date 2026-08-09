import { execFile, type ExecFileException } from "node:child_process";

import type {
  Integration,
  IntegrationCapability,
  IntegrationDetection,
  IntegrationDetectionOptions,
  IntegrationObserver,
  IntegrationSemanticEvent,
} from "../integration.js";
import { INTEGRATION_API_VERSION } from "../integration.js";
import type { RunEvent } from "../generated/RunEvent.js";
import type { RunSpec } from "../generated/RunSpec.js";
import type { TerminalSize } from "../generated/TerminalSize.js";

const DEFAULT_PROBE_TIMEOUT_MS = 1_000;
const MAX_PROBE_BUFFER_BYTES = 64 * 1024;
const MAX_SEMANTIC_RECORD_CHARS = 1024 * 1024;
const VERSION_PATTERN =
  /^codex-cli (\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?)\s*$/u;
const JSON_CAPABILITY_PATTERN = /^\s*--json(?:\s|$)/mu;

/** Minimal inputs for one machine-readable Codex Run. */
export interface CodexRunConfig {
  readonly prompt: string;
  readonly cwd: string;
  readonly env?: Readonly<Record<string, string>>;
  readonly size?: TerminalSize;
}

/** Inputs for one Codex-native session continuation. */
export interface CodexForkConfig extends CodexRunConfig {
  readonly sessionId: string;
  readonly artifactReferences?: readonly string[];
}

/** Host-local event normalized from one valid Codex JSONL record. */
export interface CodexSemanticEvent extends IntegrationSemanticEvent {
  readonly integrationId: "codex";
}

/** Explicit Codex exec Integration; interactive resume and fork are separate capabilities. */
export const codexIntegration: Integration<
  CodexRunConfig,
  CodexForkConfig,
  CodexSemanticEvent
> = {
  id: "codex",
  apiVersion: INTEGRATION_API_VERSION,
  async detect(
    options: IntegrationDetectionOptions = {},
  ): Promise<IntegrationDetection> {
    const executable = options.executable ?? "codex";
    const timeoutMs = probeTimeout(options.timeoutMs);
    const versionProbe = await probe(executable, ["--version"], timeoutMs);
    if (versionProbe.status === "unavailable") {
      return {
        status: "unavailable",
        executable,
        reason: versionProbe.reason,
      };
    }

    const version = VERSION_PATTERN.exec(versionProbe.stdout)?.[1];
    if (version === undefined) {
      return { status: "unavailable", executable, reason: "invalid_version" };
    }

    const capabilityProbe = await probe(
      executable,
      ["exec", "--help"],
      timeoutMs,
    );
    if (capabilityProbe.status === "unavailable") {
      return {
        status: "unavailable",
        executable,
        reason: capabilityProbe.reason,
      };
    }
    if (!JSON_CAPABILITY_PATTERN.test(capabilityProbe.stdout)) {
      return {
        status: "unavailable",
        executable,
        reason: "missing_capability",
      };
    }

    const capabilities: IntegrationCapability[] = ["semantic_events"];
    const resumeProbe = await probe(
      executable,
      ["exec", "resume", "--help"],
      timeoutMs,
    );
    if (
      resumeProbe.status === "ok" &&
      JSON_CAPABILITY_PATTERN.test(resumeProbe.stdout)
    ) {
      capabilities.push("level_b_fork");
    }

    return {
      status: "available",
      executable,
      version,
      capabilities,
    };
  },
  planLaunch(config, detection): RunSpec {
    if (config.prompt.trim().length === 0) {
      throw new TypeError("Codex prompt must not be empty");
    }
    if (config.cwd.trim().length === 0) {
      throw new TypeError("Codex cwd must not be empty");
    }
    if (!detection.capabilities.includes("semantic_events")) {
      throw new TypeError("Codex detection did not establish --json support");
    }
    return {
      program: detection.executable,
      args: ["exec", "--json", "--", config.prompt],
      cwd: config.cwd,
      env: { ...(config.env ?? {}) },
      size: config.size ?? { cols: 80, rows: 24 },
      declared_inputs: [{ kind: "workspace", reference: config.cwd }],
    };
  },
  planLevelBFork(_parent, config, detection) {
    if (config.sessionId.trim().length === 0) {
      throw new TypeError("Codex session id must not be empty");
    }
    if (config.prompt.trim().length === 0) {
      throw new TypeError("Codex prompt must not be empty");
    }
    if (config.cwd.trim().length === 0) {
      throw new TypeError("Codex cwd must not be empty");
    }
    if (!detection.capabilities.includes("level_b_fork")) {
      throw new TypeError(
        "Codex detection did not establish exec resume --json support",
      );
    }
    const artifactReferences = config.artifactReferences ?? [];
    if (artifactReferences.some((reference) => reference.length === 0)) {
      throw new TypeError("Codex artifact references must not be empty");
    }
    return {
      type: "level_b",
      spec: {
        program: detection.executable,
        args: [
          "exec",
          "resume",
          "--json",
          "--",
          config.sessionId,
          config.prompt,
        ],
        cwd: config.cwd,
        env: { ...(config.env ?? {}) },
        size: config.size ?? { cols: 80, rows: 24 },
        declared_inputs: [
          { kind: "workspace", reference: config.cwd },
          ...artifactReferences.map((reference) => ({
            kind: "artifact" as const,
            reference,
          })),
          { kind: "context", reference: config.sessionId },
        ],
      },
    };
  },
  createObserver: createCodexObserver,
};

type ProbeResult =
  | { readonly status: "ok"; readonly stdout: string }
  | {
      readonly status: "unavailable";
      readonly reason: "not_found" | "probe_timeout" | "probe_failed";
    };

function probe(
  executable: string,
  args: readonly string[],
  timeoutMs: number,
): Promise<ProbeResult> {
  return new Promise((resolve) => {
    execFile(
      executable,
      [...args],
      {
        encoding: "utf8",
        killSignal: "SIGKILL",
        maxBuffer: MAX_PROBE_BUFFER_BYTES,
        timeout: timeoutMs,
      },
      (error: ExecFileException | null, stdout: string) => {
        if (error === null) {
          resolve({ status: "ok", stdout });
          return;
        }
        if (error.code === "ENOENT") {
          resolve({ status: "unavailable", reason: "not_found" });
          return;
        }
        const timedOut =
          error.killed &&
          error.signal === "SIGKILL" &&
          typeof error.code !== "string" &&
          typeof error.code !== "number";
        resolve({
          status: "unavailable",
          reason: timedOut ? "probe_timeout" : "probe_failed",
        });
      },
    );
  });
}

function probeTimeout(value: number | undefined): number {
  const timeoutMs = value ?? DEFAULT_PROBE_TIMEOUT_MS;
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs <= 0) {
    throw new TypeError("Integration probe timeout must be a positive integer");
  }
  return timeoutMs;
}

function createCodexObserver(): IntegrationObserver<CodexSemanticEvent> {
  let decoder = new TextDecoder("utf-8", { fatal: true });
  let buffered = "";

  const reset = (): void => {
    decoder = new TextDecoder("utf-8", { fatal: true });
    buffered = "";
  };

  return {
    observe(event: RunEvent): readonly CodexSemanticEvent[] {
      if (event.type === "gap") {
        reset();
        return [diagnostic("output_gap")];
      }
      if (event.type !== "output" && event.type !== "exited") {
        return [];
      }

      try {
        if (event.type === "output") {
          buffered += decoder.decode(Uint8Array.from(event.chunk.data), {
            stream: true,
          });
          const events = drainLines();
          if (buffered.length > MAX_SEMANTIC_RECORD_CHARS) {
            reset();
            return [...events, diagnostic("record_too_large")];
          }
          return events;
        }

        buffered += decoder.decode();
        if (buffered.length === 0) {
          reset();
          return [];
        }
        const finalEvent = parseLine(stripCarriageReturn(buffered));
        reset();
        return [finalEvent];
      } catch {
        reset();
        return [diagnostic("invalid_utf8")];
      }
    },
  };

  function drainLines(): readonly CodexSemanticEvent[] {
    const events: CodexSemanticEvent[] = [];
    let newline = buffered.indexOf("\n");
    while (newline >= 0) {
      events.push(parseLine(stripCarriageReturn(buffered.slice(0, newline))));
      buffered = buffered.slice(newline + 1);
      newline = buffered.indexOf("\n");
    }
    return events;
  }
}

function parseLine(line: string): CodexSemanticEvent {
  let value: unknown;
  try {
    value = JSON.parse(line);
  } catch {
    return diagnostic("invalid_json");
  }
  if (
    !isRecord(value) ||
    typeof value.type !== "string" ||
    value.type.length === 0
  ) {
    return diagnostic("invalid_event");
  }
  return { integrationId: "codex", name: value.type, data: value };
}

function diagnostic(reason: string): CodexSemanticEvent {
  return {
    integrationId: "codex",
    name: "integration.parse_error",
    data: { reason },
  };
}

function stripCarriageReturn(value: string): string {
  return value.endsWith("\r") ? value.slice(0, -1) : value;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
