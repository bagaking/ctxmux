export {
  Attachment,
  CtxmuxClient,
  CtxmuxInvalidFrameError,
  CtxmuxProtocolError,
} from "./client.js";
export type { ByteInput, CtxmuxClientOptions } from "./client.js";
export {
  INTEGRATION_API_VERSION,
  IntegrationCapabilityError,
  IntegrationProvenanceError,
  IntegrationUnavailableError,
  registerIntegration,
} from "./integration.js";
export type {
  AvailableIntegrationDetection,
  Integration,
  IntegrationCapability,
  IntegrationDetection,
  IntegrationDetectionOptions,
  IntegrationObserver,
  IntegrationSemanticEvent,
  IntegrationUnavailableReason,
  LevelBForkPlan,
  RegisteredIntegration,
  UnavailableIntegrationDetection,
} from "./integration.js";
export { MAX_FRAME_BYTES, PROTOCOL_VERSION } from "./generated/constants.js";
export type { AttachedHeader } from "./generated/AttachedHeader.js";
export type { AttachedSnapshot } from "./generated/AttachedSnapshot.js";
export type { ClientFrame } from "./generated/ClientFrame.js";
export type { ErrorCode } from "./generated/ErrorCode.js";
export type { ForkFidelity } from "./generated/ForkFidelity.js";
export type { ForkPlan } from "./generated/ForkPlan.js";
export type { InterruptionReason } from "./generated/InterruptionReason.js";
export type { OutputChunk } from "./generated/OutputChunk.js";
export type { OutputReplay } from "./generated/OutputReplay.js";
export type { OutputReplayHeader } from "./generated/OutputReplayHeader.js";
export type { ProtocolError } from "./generated/ProtocolError.js";
export type { ReplayCapability } from "./generated/ReplayCapability.js";
export type { Request } from "./generated/Request.js";
export type { Response } from "./generated/Response.js";
export type { RunBackend } from "./generated/RunBackend.js";
export type { RunCapabilities } from "./generated/RunCapabilities.js";
export type { RunEvent } from "./generated/RunEvent.js";
export type { RunId } from "./generated/RunId.js";
export type { RunInfo } from "./generated/RunInfo.js";
export type { RunInputKind } from "./generated/RunInputKind.js";
export type { RunInputReference } from "./generated/RunInputReference.js";
export type { RunLineage } from "./generated/RunLineage.js";
export type { RunSpec } from "./generated/RunSpec.js";
export type { RunState } from "./generated/RunState.js";
export type { ServerFrame } from "./generated/ServerFrame.js";
export type { TerminalSize } from "./generated/TerminalSize.js";
export type { TmuxPaneInfo } from "./generated/TmuxPaneInfo.js";
export type { TmuxRunEvent } from "./generated/TmuxRunEvent.js";

import { PROTOCOL_VERSION } from "./generated/constants.js";
import type { RunSpec } from "./generated/RunSpec.js";
import type { RunInputReference } from "./generated/RunInputReference.js";
import type { TerminalSize } from "./generated/TerminalSize.js";

/** Version information reported by an SDK client boundary. */
export interface VersionInfo {
  readonly product: string;
  readonly protocol: typeof PROTOCOL_VERSION;
}

/** Construct SDK version information without creating runtime state. */
export function versionInfo(product: string): VersionInfo {
  if (product.length === 0) {
    throw new TypeError("product version must not be empty");
  }

  return { product, protocol: PROTOCOL_VERSION };
}

/** Fill the portable defaults required by protocol generation 2. */
export function defineRun(
  program: string,
  options: {
    readonly args?: readonly string[];
    readonly cwd?: string | null;
    readonly env?: Readonly<Record<string, string>>;
    readonly size?: TerminalSize;
    readonly declaredInputs?: readonly RunInputReference[];
  } = {},
): RunSpec {
  if (program.length === 0) {
    throw new TypeError("Run program must not be empty");
  }
  return {
    program,
    args: [...(options.args ?? [])],
    cwd: options.cwd ?? null,
    env: { ...(options.env ?? {}) },
    size: options.size ?? { cols: 80, rows: 24 },
    declared_inputs: [...(options.declaredInputs ?? [])],
  };
}
