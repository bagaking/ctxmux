import { constants } from "node:fs";
import { access } from "node:fs/promises";

import type {
  Integration,
  IntegrationDetection,
  IntegrationDetectionOptions,
} from "../integration.js";
import { INTEGRATION_API_VERSION } from "../integration.js";
import type { RunSpec } from "../generated/RunSpec.js";
import type { TerminalSize } from "../generated/TerminalSize.js";

/** Portable inputs for one generic shell Run. */
export interface ShellRunConfig {
  readonly args?: readonly string[];
  readonly cwd?: string;
  readonly env?: Readonly<Record<string, string>>;
  readonly size?: TerminalSize;
}

/** Explicit generic-shell Integration with no semantic observer capability. */
export const shellIntegration: Integration<ShellRunConfig, never> = {
  id: "shell",
  apiVersion: INTEGRATION_API_VERSION,
  async detect(
    options: IntegrationDetectionOptions = {},
  ): Promise<IntegrationDetection> {
    const executable = options.executable ?? "/bin/sh";
    try {
      await access(executable, constants.X_OK);
      return {
        status: "available",
        executable,
        version: null,
        capabilities: [],
      };
    } catch (error) {
      const code = (error as NodeJS.ErrnoException).code;
      return {
        status: "unavailable",
        executable,
        reason:
          code === "ENOENT" || code === "ENOTDIR"
            ? "not_found"
            : "probe_failed",
      };
    }
  },
  planLaunch(config, detection): RunSpec {
    return {
      program: detection.executable,
      args: [...(config.args ?? ["-i"])],
      cwd: config.cwd ?? null,
      env: { ...(config.env ?? {}) },
      size: config.size ?? { cols: 80, rows: 24 },
    };
  },
  createObserver() {
    return { observe: () => [] };
  },
};
