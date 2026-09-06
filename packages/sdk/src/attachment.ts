import { randomUUID } from "node:crypto";
import {
  asError,
  bytes,
  commandError,
  CtxmuxCommandError,
  decodeReceipt,
  protocolError,
  type AttachmentControlAccepted,
  type ByteInput,
  type InputReceipt,
  type ResizeReceipt,
  type SignalReceipt,
  type StopReceipt,
} from "./control.js";
import { MAX_FRAME_BYTES } from "./generated/constants.js";
import type { AttachedSnapshot } from "./generated/AttachedSnapshot.js";
import type { AttachmentCommandId } from "./generated/AttachmentCommandId.js";
import type { ClientFrame } from "./generated/ClientFrame.js";
import type { ControlReceipt } from "./generated/ControlReceipt.js";
import type { ErrorCode } from "./generated/ErrorCode.js";
import type { RunEvent } from "./generated/RunEvent.js";
import type { RunId } from "./generated/RunId.js";
import type { RuntimeIdentity } from "./generated/RuntimeIdentity.js";
import type { ServerFrame } from "./generated/ServerFrame.js";
import type { TerminalSize } from "./generated/TerminalSize.js";
import {
  encodeRecoverableStop,
  type RecoverableStopOperation,
} from "./stop-operation.js";
import { CtxmuxInvalidFrameError, validateServerFrame } from "./validation.js";
import { encodeJsonLine, WireClosedError } from "./wire.js";
import {
  emptyGapCauses,
  unionGapCauses,
  type AttachmentEvent,
  type AttachmentGapEvent,
  type AttachmentGapLocalPressure,
} from "./gap-observation.js";

const MAX_ATTACHMENT_COMMAND_ID = 0xffff_ffff;
// Per-attachment pipeline windows bound queued envelopes and payload while a
// peer is slow. Count limits cover empty/small commands; byte limits cover large
// commands. Exhaustion is explicit backpressure before enqueue, with recoverable
// command identity preserved. These windows do not cap fleet/lifetime work;
// independent short-client requests use the same public protocol.
const MAX_PENDING_COMMANDS = 64;
const MAX_PENDING_INPUT_COMMANDS = 32;
const MAX_PENDING_INPUT_BYTES = 1024 * 1024;
/** Per-attachment retained logical representation, not V8 heap/RSS. Binary
 * payload and UTF-8 JSON metadata have independent windows. Defaults derive
 * from the admitted protocol payload unit, not an event population requirement.
 * Transient decoding, engine headers/backing stores, and caller-owned returned
 * events are outside this accounting and require independent cost measurement.
 */
export interface AttachmentViewResources {
  readonly payloadBytes?: number;
  readonly envelopeBytes?: number;
}
export interface AttachmentViewResourcePolicy {
  readonly payloadBytes: number;
  readonly envelopeBytes: number;
}
/** @internal Validate and snapshot the caller's operating policy. */
export function attachmentViewResourcePolicy(
  resources: AttachmentViewResources = {},
): AttachmentViewResourcePolicy {
  const payloadBytes = resources.payloadBytes ?? MAX_FRAME_BYTES;
  const envelopeBytes = resources.envelopeBytes ?? MAX_FRAME_BYTES;
  for (const [name, value] of Object.entries({ payloadBytes, envelopeBytes })) {
    if (!Number.isSafeInteger(value) || value < 0)
      throw new TypeError(`${name} must be a nonnegative safe integer`);
  }
  return Object.freeze({ payloadBytes, envelopeBytes });
}
/** Local view loss; the Run and admitted results remain live. Drain the retained
 * prefix, detach and reattach for a fresh view. Missing metadata is not replayable.
 * One bounded out-of-budget error cell reports an exhausted view budget.
 */
export class CtxmuxAttachmentObservationUnavailableError extends Error {
  public readonly recovery = "detach_and_reattach";
  #lostEvents = 0;
  #lostOutputBytes = 0;
  #lossCountersSaturated = false;
  public readonly runId: RunId;
  public readonly resource: "envelope_bytes" | "payload_bytes";
  public readonly budgets: AttachmentViewResourcePolicy;
  public readonly retainedPayloadBytes: number;
  public readonly retainedEnvelopeBytes: number;
  public constructor(
    runId: RunId,
    resource: "envelope_bytes" | "payload_bytes",
    budgets: AttachmentViewResourcePolicy,
    retainedPayloadBytes: number,
    retainedEnvelopeBytes: number,
  ) {
    super(
      `attachment observation unavailable: local ${resource} budget exhausted; detach and reattach`,
    );
    this.name = "CtxmuxAttachmentObservationUnavailableError";
    this.runId = runId;
    this.resource = resource;
    this.budgets = budgets;
    this.retainedPayloadBytes = retainedPayloadBytes;
    this.retainedEnvelopeBytes = retainedEnvelopeBytes;
  }
  public get lostEvents(): number {
    return this.#lostEvents;
  }
  public get lostOutputBytes(): number {
    return this.#lostOutputBytes;
  }
  public get lossCountersSaturated(): boolean {
    return this.#lossCountersSaturated;
  }
  /** @internal Counts known discarded decoded bytes; saturation stays explicit. */
  public recordDrop(event: RunEvent): void {
    const n = event.type === "output" ? event.chunk.data.length : 0;
    if (
      this.#lostEvents === Number.MAX_SAFE_INTEGER ||
      n > Number.MAX_SAFE_INTEGER - this.#lostOutputBytes
    )
      this.#lossCountersSaturated = true;
    this.#lostEvents = Math.min(Number.MAX_SAFE_INTEGER, this.#lostEvents + 1);
    this.#lostOutputBytes = Math.min(
      Number.MAX_SAFE_INTEGER,
      this.#lostOutputBytes + n,
    );
  }
}

interface AttachmentWire {
  send(value: unknown): Promise<void>;
  sendEncoded(payload: string): Promise<void>;
  receive(): Promise<unknown>;
  close(): void;
}

const runEventSources = new WeakMap<object, RunId>();

/** @internal Bind one event to the Attachment owner used by Integration tests. */
export function rememberRunEventSource(event: RunEvent, runId: RunId): void {
  runEventSources.set(event, runId);
  if (event.type === "output") {
    runEventSources.set(event.chunk, runId);
  }
}

/** @internal Source identity retained by the Attachment that owned an event. */
export function runEventSource(event: RunEvent): RunId | undefined {
  return (
    runEventSources.get(event) ??
    (event.type === "output" ? runEventSources.get(event.chunk) : undefined)
  );
}

/** Live TypeScript attachment to one daemon-owned Run. */
export class Attachment {
  readonly #wire: AttachmentWire;
  readonly #pending = new Map<AttachmentCommandId, PendingCommand>();
  readonly #events: (QueuedEvent | undefined)[] = [];
  #eventHead = 0;
  readonly #viewResources: AttachmentViewResourcePolicy;
  #viewError: CtxmuxAttachmentObservationUnavailableError | undefined;
  #queuedEnvelopeBytes = 0;
  #payloadHighWaterBytes = 0;
  #envelopeHighWaterBytes = 0;
  #receivedOutputHeadByte: number | null;
  #deliveredThroughByte: number | null;
  #recoveryAfterByte: number;
  readonly #requestedAfterByte: number;
  #continuityLost = false;
  readonly #attachmentId = randomUUID();
  readonly #runtime: Pick<
    RuntimeIdentity,
    "runtimeId" | "daemonInstanceId"
  > | null;
  #state: AttachmentState = "open";
  #nextCommandId: AttachmentCommandId | undefined = 1;
  #pendingInputCommands = 0;
  #pendingInputBytes = 0;
  #queuedEventBytes = 0;
  #eventWaiter: EventWaiter | undefined;
  #pendingOutputGap: QueuedEvent | undefined;
  #terminalEvent: QueuedEvent | undefined;
  #terminalSeen = false;
  #observationDiscontinuitySeen = false;
  #eventStreamEnded = false;
  #eventError: Error | undefined;
  #pendingDrainedResolve: (() => void) | undefined;
  #detachAcknowledgementResolve: (() => void) | undefined;
  #detachAcknowledgementReject: ((error: Error) => void) | undefined;
  #detachPromise: Promise<void> | undefined;
  public readonly snapshot: AttachedSnapshot;

  public constructor(
    wire: AttachmentWire,
    snapshot: AttachedSnapshot,
    resources: AttachmentViewResources = {},
    runtime?: RuntimeIdentity,
    requestedAfterByte = 0,
  ) {
    this.#viewResources = attachmentViewResourcePolicy(resources);
    this.#wire = wire;
    this.snapshot = snapshot;
    // Headers and synthetic terminal seeds advertise a position; only original
    // decoded replay chunks prove Output receipt and API delivery. An empty
    // retained window must not manufacture a receipt at the owner's head.
    const replayHead = snapshot.replay.chunks.at(-1)?.end_byte ?? null;
    this.#receivedOutputHeadByte = replayHead;
    this.#deliveredThroughByte = replayHead;
    this.#requestedAfterByte = requestedAfterByte;
    this.#recoveryAfterByte = replayHead ?? requestedAfterByte;
    this.#runtime =
      runtime === undefined
        ? null
        : Object.freeze({
            runtimeId: runtime.runtimeId,
            daemonInstanceId: runtime.daemonInstanceId,
          });
    for (const chunk of snapshot.replay.chunks) {
      runEventSources.set(chunk, snapshot.run.id);
    }
    void this.#receivePump();
  }

  public input(
    data: ByteInput,
  ): Promise<AttachmentControlAccepted<InputReceipt>> {
    if (this.#viewError !== undefined)
      return Promise.reject(this.#viewControlError());
    let payload: number[];
    try {
      payload = bytes(data);
    } catch (error) {
      return Promise.reject(error);
    }
    return this.#command<InputReceipt>(
      "input",
      payload.length,
      (commandId) => ({
        type: "input",
        command_id: commandId,
        data: payload,
      }),
    );
  }

  public resize(
    size: TerminalSize,
  ): Promise<AttachmentControlAccepted<ResizeReceipt>> {
    return this.#command<ResizeReceipt>("resize", 0, (commandId) => ({
      type: "resize",
      command_id: commandId,
      size,
    }));
  }

  public stop(
    operation: RecoverableStopOperation,
  ): Promise<AttachmentControlAccepted<StopReceipt>> {
    return this.#command<StopReceipt>("stop", 0, (commandId) => ({
      type: "stop",
      command_id: commandId,
      operation: encodeRecoverableStop(operation),
    }));
  }

  public interrupt(): Promise<AttachmentControlAccepted<SignalReceipt>> {
    return this.#command<SignalReceipt>("signal", 0, (commandId) => ({
      type: "signal",
      command_id: commandId,
      signal: "interrupt",
    }));
  }

  public detach(): Promise<void> {
    this.#detachPromise ??= this.#detachCleanly();
    return this.#detachPromise;
  }

  /** Abruptly close this client attachment without affecting its Run. */
  public close(): void {
    if (this.#state === "closed") {
      return;
    }
    this.#state = "closed";
    this.#failPendingUnknown("attachment closed locally", "io");
    this.#finishEvents();
    this.#rejectDetach(
      new Error("attachment closed before detach acknowledgement"),
    );
    this.#wire.close();
  }

  public async nextEvent(): Promise<AttachmentEvent | undefined> {
    const queued = this.#events[this.#eventHead];
    if (queued !== undefined) {
      this.#events[this.#eventHead++] = undefined;
      // Amortized work proportional to consumed entries, without a slot cap.
      if (this.#eventHead >= this.#events.length - this.#eventHead) {
        this.#events.splice(0, this.#eventHead);
        this.#eventHead = 0;
      }
    }
    if (queued !== undefined) {
      this.#releaseEvent(queued);
      return this.#deliverEvent(queued.event);
    }
    if (this.#pendingOutputGap !== undefined) {
      const gap = this.#pendingOutputGap;
      this.#pendingOutputGap = undefined;
      this.#releaseEvent(gap);
      return this.#deliverEvent(gap.event);
    }
    if (this.#terminalEvent !== undefined) {
      const terminal = this.#terminalEvent;
      this.#terminalEvent = undefined;
      this.#eventStreamEnded = true;
      this.#releaseEvent(terminal);
      return this.#deliverEvent(terminal.event);
    }
    if (this.#eventError !== undefined) {
      throw this.#eventError;
    }
    if (this.#viewError !== undefined) throw this.#viewError;
    if (this.#eventStreamEnded || this.#state === "closed") {
      return undefined;
    }
    if (this.#eventWaiter !== undefined) {
      throw new Error(
        "only one nextEvent() call may be pending per attachment",
      );
    }
    return await new Promise<AttachmentEvent | undefined>((resolve, reject) => {
      this.#eventWaiter = { resolve, reject };
    });
  }

  public async *events(): AsyncGenerator<AttachmentEvent, void, void> {
    while (true) {
      const event = await this.nextEvent();
      if (event === undefined) {
        return;
      }
      yield event;
      if (event.type === "exited" || event.type === "interrupted") {
        return;
      }
    }
  }

  async #detachCleanly(): Promise<void> {
    if (this.#state === "closed") {
      throw new Error("attachment is already closed");
    }
    this.#state = "detaching";
    await this.#waitForPendingCommands();
    if (this.#isClosed()) {
      throw this.#eventError ?? new Error("attachment closed while detaching");
    }
    const acknowledgement = new Promise<void>((resolve, reject) => {
      this.#detachAcknowledgementResolve = resolve;
      this.#detachAcknowledgementReject = reject;
    });
    try {
      await this.#wire.send({ type: "detach" } satisfies ClientFrame);
    } catch (error) {
      this.#terminate(asError(error), "io");
    }
    await acknowledgement;
  }

  #command<R extends ControlReceipt>(
    kind: R["type"],
    inputBytes: number,
    frame: (commandId: AttachmentCommandId) => ClientFrame,
  ): Promise<AttachmentControlAccepted<R>> {
    if (this.#viewError !== undefined)
      return Promise.reject(this.#viewControlError());
    if (this.#state !== "open") {
      return Promise.reject(
        new Error(
          this.#state === "detaching"
            ? "attachment is detaching"
            : "attachment is closed",
        ),
      );
    }
    if (this.#nextCommandId === undefined) {
      return Promise.reject(new Error("attachment command IDs are exhausted"));
    }
    const localLimit = this.#admissionLimit(kind, inputBytes);
    if (localLimit !== undefined) {
      return Promise.reject(
        new CtxmuxCommandError(
          "control_backpressure",
          `attachment command rejected by local ${localLimit} bound`,
          "not_applied",
        ),
      );
    }

    const commandId = this.#nextCommandId;
    let encodedFrame: string;
    try {
      encodedFrame = encodeJsonLine(frame(commandId));
    } catch (error) {
      return Promise.reject(
        new CtxmuxCommandError(
          "invalid_request",
          asError(error).message,
          "not_applied",
        ),
      );
    }
    this.#nextCommandId =
      commandId === MAX_ATTACHMENT_COMMAND_ID ? undefined : commandId + 1;
    const promise = new Promise<AttachmentControlAccepted<R>>(
      (resolve, reject) => {
        this.#pending.set(commandId, {
          kind,
          inputBytes,
          resolve: (receipt) => resolve({ commandId, receipt: receipt as R }),
          reject,
        });
      },
    );
    if (kind === "input") {
      this.#pendingInputCommands += 1;
      this.#pendingInputBytes += inputBytes;
    }
    void this.#sendCommand(encodedFrame);
    return promise;
  }

  #admissionLimit(
    kind: ControlReceipt["type"],
    inputBytes: number,
  ): string | undefined {
    if (this.#pending.size >= MAX_PENDING_COMMANDS) {
      return "total pending commands";
    }
    if (
      kind === "input" &&
      this.#pendingInputCommands >= MAX_PENDING_INPUT_COMMANDS
    ) {
      return "pending input commands";
    }
    if (
      kind === "input" &&
      inputBytes > MAX_PENDING_INPUT_BYTES - this.#pendingInputBytes
    ) {
      return "pending input bytes";
    }
    return undefined;
  }

  async #sendCommand(encodedFrame: string): Promise<void> {
    try {
      await this.#wire.sendEncoded(encodedFrame);
    } catch (error) {
      this.#terminate(asError(error), "io");
    }
  }

  async #receivePump(): Promise<void> {
    try {
      while (this.#state !== "closed") {
        const frame = validateServerFrame(await this.#wire.receive());
        switch (frame.type) {
          case "event":
            if (!this.#offerEvent(frame.event)) {
              return;
            }
            break;
          case "command_result":
            if (!this.#settleCommand(frame.command_id, frame.outcome)) {
              return;
            }
            break;
          case "detached":
            if (this.#state !== "detaching" || this.#pending.size !== 0) {
              this.#protocolViolation(
                "detach acknowledgement arrived outside a drained detach",
                "$frame.detached",
              );
              return;
            }
            this.#state = "closed";
            this.#finishEvents();
            this.#detachAcknowledgementResolve?.();
            this.#clearDetachAcknowledgement();
            this.#wire.close();
            return;
          case "error":
            this.#terminate(protocolError(frame.error), frame.error.code);
            return;
          default:
            this.#protocolViolation(
              `unexpected ${frame.type} frame on an attachment`,
              "$frame.type",
            );
            return;
        }
      }
    } catch (error) {
      if (this.#state !== "closed") {
        const terminalError = asError(error);
        this.#terminate(
          terminalError,
          terminalError instanceof CtxmuxInvalidFrameError ||
            terminalError instanceof SyntaxError
            ? "internal"
            : "io",
          (this.#terminalSeen || this.#observationDiscontinuitySeen) &&
            terminalError instanceof WireClosedError,
        );
      }
    }
  }

  #settleCommand(
    commandId: AttachmentCommandId,
    outcome: Extract<
      ServerFrame,
      { readonly type: "command_result" }
    >["outcome"],
  ): boolean {
    const pending = this.#pending.get(commandId);
    if (pending === undefined) {
      this.#protocolViolation(
        "command result ID is unknown or already completed",
        "$frame.command_result.command_id",
      );
      return false;
    }
    if (outcome.type === "rejected") {
      this.#removePending(commandId, pending);
      pending.reject(commandError(outcome.failure, commandId));
      return true;
    }
    let receipt: ControlReceipt;
    try {
      receipt = decodeReceipt(
        pending.kind,
        outcome.receipt,
        pending.inputBytes,
      );
    } catch (error) {
      this.#protocolViolation(asError(error).message, "$frame.command_result");
      return false;
    }
    this.#removePending(commandId, pending);
    pending.resolve(receipt);
    return true;
  }

  #removePending(
    commandId: AttachmentCommandId,
    pending: PendingCommand,
  ): void {
    this.#pending.delete(commandId);
    if (pending.kind === "input") {
      this.#pendingInputCommands -= 1;
      this.#pendingInputBytes -= pending.inputBytes;
    }
    if (this.#pending.size === 0) {
      this.#pendingDrainedResolve?.();
      this.#pendingDrainedResolve = undefined;
    }
  }

  #waitForPendingCommands(): Promise<void> {
    if (this.#pending.size === 0) {
      return Promise.resolve();
    }
    return new Promise((resolve) => {
      this.#pendingDrainedResolve = resolve;
    });
  }

  #offerEvent(event: RunEvent): boolean {
    rememberRunEventSource(event, this.snapshot.run.id);
    const terminal = event.type === "exited" || event.type === "interrupted";
    if (this.#terminalSeen) {
      this.#protocolViolation(
        "attachment delivered an event after terminal state",
        "$frame.event",
      );
      return false;
    }
    if (!terminal && this.#observationDiscontinuitySeen) {
      this.#protocolViolation(
        "attachment delivered a non-terminal event after observation discontinuity",
        "$frame.event",
      );
      return false;
    }
    if (terminal) this.#terminalSeen = true;
    if (event.type === "observation_discontinuity")
      this.#observationDiscontinuitySeen = true;
    if (event.type === "output")
      this.#receivedOutputHeadByte = Math.max(
        this.#receivedOutputHeadByte ?? 0,
        event.chunk.end_byte,
      );
    // A local view failure never stops strict validation or the wire/ACK owner.
    if (this.#viewError !== undefined) {
      this.#viewError.recordDrop(event);
      return true;
    }
    if (
      this.#queueEmpty() &&
      this.#pendingOutputGap === undefined &&
      this.#eventWaiter !== undefined
    ) {
      const waiter = this.#eventWaiter;
      this.#eventWaiter = undefined;
      if (terminal) this.#eventStreamEnded = true;
      waiter.resolve(
        this.#deliverEvent(
          event.type === "gap" ? this.#gapEvent(event) : event,
        ),
      );
      return true;
    }
    if (event.type === "gap") {
      this.#extendPendingOutputGap(event);
      return true;
    }
    const retained: QueuedEvent = {
      event,
      bytes: eventBytes(event),
      envelopeBytes: eventEnvelopeBytes(event),
    };
    if (!this.#eventCapacity(retained)) {
      if (event.type === "output") this.#extendPendingOutputGap(event);
      else
        this.#loseObservation(
          event,
          retained.bytes >
            this.#viewResources.payloadBytes - this.#queuedEventBytes
            ? "payload_bytes"
            : "envelope_bytes",
        );
      return true;
    }
    if (terminal) {
      this.#terminalEvent = retained;
      this.#chargeEvent(retained);
      return true;
    }
    if (this.#pendingOutputGap !== undefined) {
      // Transfer funded gap ownership without reserving it a second time.
      this.#events.push(this.#pendingOutputGap);
      this.#pendingOutputGap = undefined;
    }
    this.#events.push(retained);
    this.#chargeEvent(retained);
    return true;
  }
  #queueEmpty(): boolean {
    return this.#eventHead === this.#events.length;
  }
  #eventCapacity(event: QueuedEvent): boolean {
    return (
      event.bytes <=
        this.#viewResources.payloadBytes - this.#queuedEventBytes &&
      event.envelopeBytes <=
        this.#viewResources.envelopeBytes - this.#queuedEnvelopeBytes
    );
  }
  #chargeEvent(event: QueuedEvent): void {
    this.#queuedEventBytes += event.bytes;
    this.#queuedEnvelopeBytes += event.envelopeBytes;
    this.#payloadHighWaterBytes = Math.max(
      this.#payloadHighWaterBytes,
      this.#queuedEventBytes,
    );
    this.#envelopeHighWaterBytes = Math.max(
      this.#envelopeHighWaterBytes,
      this.#queuedEnvelopeBytes,
    );
  }
  #releaseEvent(event: QueuedEvent): void {
    this.#queuedEventBytes -= event.bytes;
    this.#queuedEnvelopeBytes -= event.envelopeBytes;
  }
  #extendPendingOutputGap(
    dropped: Extract<RunEvent, { type: "gap" | "output" }>,
  ): void {
    const previous = this.#pendingOutputGap;
    const event = this.#gapEvent(
      dropped,
      previous?.event.type === "gap" ? previous.event : undefined,
    );
    const envelopeBytes = eventEnvelopeBytes(event);
    const additional = envelopeBytes - (previous?.envelopeBytes ?? 0);
    if (
      additional >
      this.#viewResources.envelopeBytes - this.#queuedEnvelopeBytes
    ) {
      this.#loseObservation(dropped);
      return;
    }
    rememberRunEventSource(event, this.snapshot.run.id);
    this.#queuedEnvelopeBytes += additional;
    this.#envelopeHighWaterBytes = Math.max(
      this.#envelopeHighWaterBytes,
      this.#queuedEnvelopeBytes,
    );
    this.#pendingOutputGap = { event, bytes: 0, envelopeBytes };
  }

  #gapEvent(
    dropped: Extract<RunEvent, { type: "gap" | "output" }>,
    previous?: AttachmentGapEvent,
  ): AttachmentGapEvent {
    const prior = previous?.observation;
    const daemon = dropped.type === "gap";
    const now = Date.now();
    let localPressure: AttachmentGapLocalPressure | null =
      prior?.localPressure ?? null;
    if (!daemon) {
      const attemptedPayloadBytes = eventBytes(dropped);
      const attemptedEnvelopeBytes = eventEnvelopeBytes(dropped);
      const oldBytes = localPressure?.droppedOutputBytes ?? 0;
      const saturated =
        localPressure?.countersSaturated === true ||
        attemptedPayloadBytes > Number.MAX_SAFE_INTEGER - oldBytes;
      localPressure = {
        payloadLimitHit:
          localPressure?.payloadLimitHit === true ||
          attemptedPayloadBytes >
            this.#viewResources.payloadBytes - this.#queuedEventBytes,
        envelopeLimitHit:
          localPressure?.envelopeLimitHit === true ||
          attemptedEnvelopeBytes >
            this.#viewResources.envelopeBytes - this.#queuedEnvelopeBytes,
        attemptedPayloadBytes,
        attemptedEnvelopeBytes,
        droppedOutputBytes: saturated ? null : oldBytes + attemptedPayloadBytes,
        countersSaturated: saturated,
      };
    }
    const origins = {
      daemon: prior?.origins.daemon === true || daemon,
      client: prior?.origins.client === true || !daemon,
    };
    const ownCauses = daemon
      ? dropped.causes
      : { ...emptyGapCauses(), client_view_pressure: true };
    return {
      type: "gap",
      latest_output_bytes: Math.max(
        previous?.latest_output_bytes ?? 0,
        daemon ? dropped.latest_output_bytes : dropped.chunk.end_byte,
      ),
      causes: unionGapCauses(previous?.causes ?? emptyGapCauses(), ownCauses),
      observation: {
        attachmentId: this.#attachmentId,
        runtime: this.#runtime,
        runId: this.snapshot.run.id,
        origins,
        firstObservedAtUnixMs: prior?.firstObservedAtUnixMs ?? now,
        lastObservedAtUnixMs: now,
        deliveredAtUnixMs: null,
        receivedOutputHeadByte: this.#receivedOutputHeadByte,
        deliveredThroughByte: this.#deliveredThroughByte,
        requestedAfterByte: this.#requestedAfterByte,
        recoveryAfterByte: this.#recoveryAfterByte,
        missingOutputBytes: origins.daemon
          ? null
          : (localPressure?.droppedOutputBytes ?? null),
        queue: {
          payloadBudgetBytes: this.#viewResources.payloadBytes,
          envelopeBudgetBytes: this.#viewResources.envelopeBytes,
          retainedPayloadBytes: this.#queuedEventBytes,
          retainedEnvelopeBytes: this.#queuedEnvelopeBytes,
          payloadHighWaterBytes: this.#payloadHighWaterBytes,
          envelopeHighWaterBytes: this.#envelopeHighWaterBytes,
        },
        localPressure,
      },
    };
  }

  #deliverEvent(event: AttachmentEvent): AttachmentEvent {
    if (event.type === "output" && !this.#continuityLost) {
      if (event.chunk.start_byte > this.#recoveryAfterByte)
        this.#continuityLost = true;
      else {
        this.#deliveredThroughByte = Math.max(
          this.#deliveredThroughByte ?? 0,
          event.chunk.end_byte,
        );
        this.#recoveryAfterByte = Math.max(
          this.#recoveryAfterByte,
          event.chunk.end_byte,
        );
      }
    }
    if (event.type !== "gap") return event;
    this.#continuityLost = true;
    const delivered: AttachmentGapEvent = {
      ...event,
      observation: {
        ...event.observation,
        receivedOutputHeadByte: this.#receivedOutputHeadByte,
        deliveredAtUnixMs: Date.now(),
        deliveredThroughByte: this.#deliveredThroughByte,
        recoveryAfterByte: this.#recoveryAfterByte,
      },
    };
    rememberRunEventSource(delivered, this.snapshot.run.id);
    return delivered;
  }
  #loseObservation(
    event: RunEvent,
    resource: "envelope_bytes" | "payload_bytes" = "envelope_bytes",
  ): void {
    this.#viewError ??= new CtxmuxAttachmentObservationUnavailableError(
      this.snapshot.run.id,
      resource,
      this.#viewResources,
      this.#queuedEventBytes,
      this.#queuedEnvelopeBytes,
    );
    this.#viewError.recordDrop(event);
    this.#finishEvents();
  }
  #viewControlError(): CtxmuxCommandError {
    return new CtxmuxCommandError(
      "control_backpressure",
      "attachment observation unavailable locally; detach and reattach before new control",
      "not_applied",
    );
  }

  #protocolViolation(message: string, path: string): void {
    this.#terminate(new CtxmuxInvalidFrameError(path, message), "internal");
  }

  #terminate(error: Error, code: ErrorCode, cleanEventEof = false): void {
    if (this.#state === "closed") {
      return;
    }
    this.#state = "closed";
    this.#failPendingUnknown(error.message, code);
    if (!cleanEventEof) {
      this.#eventError = error;
    }
    this.#finishEvents();
    this.#rejectDetach(error);
    this.#wire.close();
  }

  #failPendingUnknown(message: string, code: ErrorCode): void {
    const pending = [...this.#pending.entries()];
    this.#pending.clear();
    this.#pendingInputCommands = 0;
    this.#pendingInputBytes = 0;
    this.#pendingDrainedResolve?.();
    this.#pendingDrainedResolve = undefined;
    for (const [commandId, command] of pending) {
      command.reject(
        new CtxmuxCommandError(code, message, "unknown", commandId),
      );
    }
  }

  #finishEvents(): void {
    if (
      this.#queueEmpty() &&
      this.#pendingOutputGap === undefined &&
      this.#terminalEvent === undefined &&
      this.#eventWaiter !== undefined
    ) {
      const waiter = this.#eventWaiter;
      this.#eventWaiter = undefined;
      if (this.#eventError === undefined && this.#viewError === undefined) {
        waiter.resolve(undefined);
      } else {
        waiter.reject(this.#eventError ?? this.#viewError!);
      }
    }
    if (this.#eventError === undefined && this.#viewError === undefined) {
      this.#eventStreamEnded = true;
    }
  }

  #rejectDetach(error: Error): void {
    this.#detachAcknowledgementReject?.(error);
    this.#clearDetachAcknowledgement();
  }

  #clearDetachAcknowledgement(): void {
    this.#detachAcknowledgementResolve = undefined;
    this.#detachAcknowledgementReject = undefined;
  }

  #isClosed(): boolean {
    return this.#state === "closed";
  }
}

type AttachmentState = "open" | "detaching" | "closed";

interface PendingCommand {
  readonly kind: ControlReceipt["type"];
  readonly inputBytes: number;
  readonly resolve: (receipt: ControlReceipt) => void;
  readonly reject: (error: Error) => void;
}

interface QueuedEvent {
  readonly event: AttachmentEvent;
  readonly bytes: number;
  readonly envelopeBytes: number;
}

interface EventWaiter {
  readonly resolve: (event: AttachmentEvent | undefined) => void;
  readonly reject: (error: Error) => void;
}

function eventBytes(event: RunEvent): number {
  if (event.type === "output") {
    return event.chunk.data.length;
  }
  if (event.type === "tmux" && event.event.type === "session_renamed") {
    return event.event.name.length;
  }
  return 0;
}

// UTF-8 JSON metadata excludes binary payload; no exact JS heap claim is made.
function eventEnvelopeBytes(event: RunEvent): number {
  const metadata =
    event.type === "output"
      ? { ...event, chunk: { ...event.chunk, data: "" } }
      : event.type === "tmux" && event.event.type === "session_renamed"
        ? { ...event, event: { ...event.event, name: [] } }
        : event;
  return Buffer.byteLength(JSON.stringify(metadata), "utf8");
}
