import { runEventSource, type CtxmuxClient } from "./client.js";
import type { CreateOperationKey } from "./generated/CreateOperationKey.js";
import type { ForkPlan } from "./generated/ForkPlan.js";
import type { RunEvent } from "./generated/RunEvent.js";
import type { RunInfo } from "./generated/RunInfo.js";
import type { RunSpec } from "./generated/RunSpec.js";

/** Current public contract generation for host-local Integrations. */
export const INTEGRATION_API_VERSION = 2 as const;

/** Optional behavior that an Integration can honestly provide. */
export type IntegrationCapability = "semantic_events" | "level_b_fork";

/** A materialized Level B plan that cannot silently encode Level A. */
export type LevelBForkPlan = Extract<ForkPlan, { readonly type: "level_b" }>;

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

/** Optional host inputs and retry identity for one Integration operation. */
export interface IntegrationOperationOptions {
  readonly detection?: IntegrationDetectionOptions;
  readonly operationKey?: CreateOperationKey;
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
export interface Integration<
  LaunchConfig,
  ForkConfig,
  Event extends IntegrationSemanticEvent,
> {
  readonly id: string;
  readonly apiVersion: typeof INTEGRATION_API_VERSION;
  detect(options?: IntegrationDetectionOptions): Promise<IntegrationDetection>;
  planLaunch(
    config: LaunchConfig,
    detection: AvailableIntegrationDetection,
  ): RunSpec;
  planLevelBFork?(
    parent: RunInfo,
    config: ForkConfig,
    detection: AvailableIntegrationDetection,
  ): LevelBForkPlan;
  levelBForkProvenance?(config: ForkConfig): object;
  createObserver(): IntegrationObserver<Event>;
}

/** One explicitly imported Integration bound to one existing SDK client. */
export interface RegisteredIntegration<
  LaunchConfig,
  ForkConfig,
  Event extends IntegrationSemanticEvent,
> {
  readonly integration: Integration<LaunchConfig, ForkConfig, Event>;
  detect(options?: IntegrationDetectionOptions): Promise<IntegrationDetection>;
  start(
    config: LaunchConfig,
    options?: IntegrationOperationOptions,
  ): Promise<RunInfo>;
  forkLevelB(
    parent: RunInfo,
    config: ForkConfig,
    options?: IntegrationOperationOptions,
  ): Promise<RunInfo>;
  createObserver(parent?: RunInfo): IntegrationObserver<Event>;
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

/** An available Integration cannot honestly provide the requested behavior. */
export class IntegrationCapabilityError extends Error {
  public readonly integrationId: string;
  public readonly capability: IntegrationCapability;

  public constructor(integrationId: string, capability: IntegrationCapability) {
    super(`Integration ${integrationId} does not provide ${capability}`);
    this.name = "IntegrationCapabilityError";
    this.integrationId = integrationId;
    this.capability = capability;
  }
}

/** A Level B plan did not carry evidence observed from its declared parent. */
export class IntegrationProvenanceError extends Error {
  public readonly integrationId: string;
  public readonly parentId: RunInfo["id"];

  public constructor(integrationId: string, parentId: RunInfo["id"]) {
    super(
      `Integration ${integrationId} has no verified Level B provenance for parent Run ${parentId}`,
    );
    this.name = "IntegrationProvenanceError";
    this.integrationId = integrationId;
    this.parentId = parentId;
  }
}

/**
 * Bind an explicitly imported Integration to the same public client used by
 * raw Runs. Registration performs no discovery and owns no daemon state.
 */
export function registerIntegration<
  LaunchConfig,
  ForkConfig,
  Event extends IntegrationSemanticEvent,
>(
  client: Pick<CtxmuxClient, "start" | "fork">,
  integration: Integration<LaunchConfig, ForkConfig, Event>,
): RegisteredIntegration<LaunchConfig, ForkConfig, Event> {
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
  const observedProvenance = new WeakMap<object, RunInfo["id"]>();

  return {
    integration,
    detect,
    async start(config, options) {
      const detection = await detect(options?.detection);
      if (detection.status === "unavailable") {
        throw new IntegrationUnavailableError(integration.id, detection);
      }
      return client.start(
        integration.planLaunch(config, detection),
        options?.operationKey,
      );
    },
    async forkLevelB(parent, config, options) {
      const detection = await detect(options?.detection);
      if (detection.status === "unavailable") {
        throw new IntegrationUnavailableError(integration.id, detection);
      }
      if (
        !detection.capabilities.includes("level_b_fork") ||
        integration.planLevelBFork === undefined ||
        integration.levelBForkProvenance === undefined
      ) {
        throw new IntegrationCapabilityError(integration.id, "level_b_fork");
      }
      const provenance = integration.levelBForkProvenance(config);
      if (
        typeof provenance !== "object" ||
        provenance === null ||
        observedProvenance.get(provenance) !== parent.id
      ) {
        throw new IntegrationProvenanceError(integration.id, parent.id);
      }
      const plan: unknown = integration.planLevelBFork(
        parent,
        config,
        detection,
      );
      if (
        typeof plan !== "object" ||
        plan === null ||
        !("type" in plan) ||
        plan.type !== "level_b"
      ) {
        throw new TypeError(
          `Integration ${integration.id} returned a non-Level-B fork plan`,
        );
      }
      return client.fork(
        parent.id,
        plan as LevelBForkPlan,
        options?.operationKey,
      );
    },
    createObserver(parent) {
      const observer = integration.createObserver();
      return {
        observe(event) {
          if (parent !== undefined && runEventSource(event) !== parent.id) {
            throw new IntegrationProvenanceError(integration.id, parent.id);
          }
          const observed = observer.observe(event);
          if (parent !== undefined) {
            for (const semanticEvent of observed) {
              observedProvenance.set(semanticEvent, parent.id);
            }
          }
          return observed;
        },
      };
    },
  };
}
