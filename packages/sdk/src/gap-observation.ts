import type { OutputGapCauses } from "./generated/OutputGapCauses.js";
import type { RunEvent } from "./generated/RunEvent.js";
import type { RunId } from "./generated/RunId.js";
import type { RuntimeIdentity } from "./generated/RuntimeIdentity.js";

/** Retained logical bytes sampled at the last Gap observation, not V8 RSS.
 * High water includes previously admitted envelopes; the attempted marker is
 * admitted afterwards under the same envelope budget. */
export interface AttachmentGapQueue {
  readonly payloadBudgetBytes: number;
  readonly envelopeBudgetBytes: number;
  readonly retainedPayloadBytes: number;
  readonly retainedEnvelopeBytes: number;
  readonly payloadHighWaterBytes: number;
  readonly envelopeHighWaterBytes: number;
}

/** Exact decoded Output discarded by this client; no daemon loss is inferred. */
export interface AttachmentGapLocalPressure {
  readonly payloadLimitHit: boolean;
  readonly envelopeLimitHit: boolean;
  readonly attemptedPayloadBytes: number;
  readonly attemptedEnvelopeBytes: number;
  readonly droppedOutputBytes: number | null;
  readonly countersSaturated: boolean;
}

export interface AttachmentGapObservation {
  readonly attachmentId: string;
  /** Same dispatch connection's Hello, null only for manually constructed wires. */
  readonly runtime: Pick<
    RuntimeIdentity,
    "runtimeId" | "daemonInstanceId"
  > | null;
  readonly runId: RunId;
  readonly origins: { readonly daemon: boolean; readonly client: boolean };
  readonly firstObservedAtUnixMs: number;
  readonly lastObservedAtUnixMs: number;
  readonly deliveredAtUnixMs: number | null;
  /** Actual Output received by the SDK, including the initial replay. A head,
   * not proof of uninterrupted delivery or of application rendering. Null when
   * this Attachment has decoded no original Output, regardless of header head. */
  readonly receivedOutputHeadByte: number | null;
  /** End of the continuous original Output handed out by this Attachment API,
   * including available replay. Null if no continuous prefix was handed out;
   * a later disconnected suffix and synthetic seeds do not advance it. */
  readonly deliveredThroughByte: number | null;
  /** Caller-supplied replay baseline, not an Output receipt by this Attachment. */
  readonly requestedAfterByte: number;
  /** Available replay/delivery cursor, or the caller baseline when empty.
   * Header heads and Gap never advance it. Snapshot truncation remains explicit. */
  readonly recoveryAfterByte: number;
  /** Unknown when any daemon Gap is included; local decoded drops are exact
   * until safe-integer saturation. This is not permanent source loss. */
  readonly missingOutputBytes: number | null;
  readonly queue: AttachmentGapQueue;
  readonly localPressure: AttachmentGapLocalPressure | null;
}

export type AttachmentGapEvent = Extract<RunEvent, { type: "gap" }> & {
  readonly observation: AttachmentGapObservation;
};

export type AttachmentEvent =
  Exclude<RunEvent, { type: "gap" }> | AttachmentGapEvent;

/** @internal Fixed cause set: aggregation cannot grow with Gap population. */
export function emptyGapCauses(): OutputGapCauses {
  return {
    live_event_pressure: false,
    subscriber_lag: false,
    source_discontinuity: false,
    terminal_catchup: false,
    geometry_lag: false,
    client_view_pressure: false,
    unknown: false,
  };
}

/** @internal Union observed facts; never replace an earlier origin. */
export function unionGapCauses(
  left: OutputGapCauses,
  right: OutputGapCauses,
): OutputGapCauses {
  return {
    live_event_pressure: left.live_event_pressure || right.live_event_pressure,
    subscriber_lag: left.subscriber_lag || right.subscriber_lag,
    source_discontinuity:
      left.source_discontinuity || right.source_discontinuity,
    terminal_catchup: left.terminal_catchup || right.terminal_catchup,
    geometry_lag: left.geometry_lag || right.geometry_lag,
    client_view_pressure:
      left.client_view_pressure || right.client_view_pressure,
    unknown: left.unknown || right.unknown,
  };
}
