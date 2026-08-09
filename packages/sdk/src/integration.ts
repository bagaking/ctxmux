import type { CtxmuxClient } from "./client.js";
import type { RunEvent } from "./generated/RunEvent.js";
import type { RunInfo } from "./generated/RunInfo.js";
import type { RunSpec } from "./generated/RunSpec.js";

/** Current public contract generation for host-local Integrations. */
export const INTEGRATION_API_VERSION = 1 as const;

/** Optional behavior that an Integration can honestly provide. */
export type IntegrationCapability = "semantic_events";

/** Why a tool probe did not establish a compatible Integration. */
export type IntegrationUnavailableReason =
  | "not_found"
  | "probe_timeout"
  | "probe_failed"
  | "invalid_version"
  | "missing_capability";

/** Host-local inputs for bounded tool detection. */
export interface IntegrationDetectionOptions {
  readonly executable?: string;
  readonly timeoutMs?: number;
}

/** A probe established the executable and its declared capabilities. */
export interface AvailableIntegrationDetection {
  readonly status: "available";
  readonly executable: string;
  readonly version: string | null;
  readonly capabilities: readonly IntegrationCapability[];
}

/** A probe failed closed without starting a Run. */
export interface UnavailableIntegrationDetection {
  readonly status: "unavailable";
  readonly executable: string;
  readonly reason: IntegrationUnavailableReason;
}

export type IntegrationDetection =
  AvailableIntegrationDetection | UnavailableIntegrationDetection;

/** Normalized meaning derived by a host without replacing the raw Run event. */
export interface IntegrationSemanticEvent {
  readonly integrationId: string;
  readonly name: string;
  readonly data: Readonly<Record<string, unknown>>;
}

/** One disposable, host-local semantic view over raw Run events. */
export interface IntegrationObserver<Event extends IntegrationSemanticEvent> {
  observe(event: RunEvent): readonly Event[];
}

/** Explicit tool semantics layered above the generic Run protocol. */
export interface Integration<Config, Event extends IntegrationSemanticEvent> {
  readonly id: string;
  readonly apiVersion: typeof INTEGRATION_API_VERSION;
  detect(options?: IntegrationDetectionOptions): Promise<IntegrationDetection>;
  planLaunch(config: Config, detection: AvailableIntegrationDetection): RunSpec;
  createObserver(): IntegrationObserver<Event>;
}

/** One explicitly imported Integration bound to one existing SDK client. */
export interface RegisteredIntegration<
  Config,
  Event extends IntegrationSemanticEvent,
> {
  readonly integration: Integration<Config, Event>;
  detect(options?: IntegrationDetectionOptions): Promise<IntegrationDetection>;
  start(
    config: Config,
    options?: IntegrationDetectionOptions,
  ): Promise<RunInfo>;
  createObserver(): IntegrationObserver<Event>;
}

/** Detection failed before an Integration could honestly plan a Run. */
export class IntegrationUnavailableError extends Error {
  public readonly detection: UnavailableIntegrationDetection;

  public constructor(
    integrationId: string,
    detection: UnavailableIntegrationDetection,
  ) {
    super(`Integration ${integrationId} is unavailable: ${detection.reason}`);
    this.name = "IntegrationUnavailableError";
    this.detection = detection;
  }
}

/**
 * Bind an explicitly imported Integration to the same public client used by
 * raw Runs. Registration performs no discovery and owns no daemon state.
 */
export function registerIntegration<
  Config,
  Event extends IntegrationSemanticEvent,
>(
  client: Pick<CtxmuxClient, "start">,
  integration: Integration<Config, Event>,
): RegisteredIntegration<Config, Event> {
  if (integration.id.trim().length === 0) {
    throw new TypeError("Integration id must not be empty");
  }
  if (integration.apiVersion !== INTEGRATION_API_VERSION) {
    throw new TypeError(
      `Integration ${integration.id} uses unsupported API version ${String(integration.apiVersion)}`,
    );
  }

  const detect = (
    options?: IntegrationDetectionOptions,
  ): Promise<IntegrationDetection> => integration.detect(options);

  return {
    integration,
    detect,
    async start(config, options) {
      const detection = await detect(options);
      if (detection.status === "unavailable") {
        throw new IntegrationUnavailableError(integration.id, detection);
      }
      return client.start(integration.planLaunch(config, detection));
    },
    createObserver() {
      return integration.createObserver();
    },
  };
}
