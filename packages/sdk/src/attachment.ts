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
  type StopReceipt,
} from "./control.js";
import type { AttachedSnapshot } from "./generated/AttachedSnapshot.js";
import type { AttachmentCommandId } from "./generated/AttachmentCommandId.js";
import type { ClientFrame } from "./generated/ClientFrame.js";
import type { ControlReceipt } from "./generated/ControlReceipt.js";
import type { ErrorCode } from "./generated/ErrorCode.js";
import type { RunEvent } from "./generated/RunEvent.js";
import type { RunId } from "./generated/RunId.js";
import type { ServerFrame } from "./generated/ServerFrame.js";
import type { TerminalSize } from "./generated/TerminalSize.js";
import { CtxmuxInvalidFrameError, validateServerFrame } from "./validation.js";
import {
  encodeJsonLine,
  JsonLinesConnection,
  WireClosedError,
} from "./wire.js";

const MAX_ATTACHMENT_COMMAND_ID = 0xffff_ffff;
const MAX_PENDING_COMMANDS = 64;
const MAX_PENDING_INPUT_COMMANDS = 32;
const MAX_PENDING_INPUT_BYTES = 1024 * 1024;
const MAX_QUEUED_EVENTS = 256;
const MAX_QUEUED_EVENT_BYTES = 1024 * 1024;

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
  readonly #wire: JsonLinesConnection;
  readonly #pending = new Map<AttachmentCommandId, PendingCommand>();
  readonly #events: QueuedEvent[] = [];
  #state: AttachmentState = "open";
  #nextCommandId: AttachmentCommandId | undefined = 1;
  #pendingInputCommands = 0;
  #pendingInputBytes = 0;
  #queuedEventBytes = 0;
  #eventWaiter: EventWaiter | undefined;
  #pendingOutputGap: Extract<RunEvent, { readonly type: "gap" }> | undefined;
  #terminalEvent: RunEvent | undefined;
  #terminalSeen = false;
  #eventStreamEnded = false;
  #eventError: Error | undefined;
  #pendingDrainedResolve: (() => void) | undefined;
  #detachAcknowledgementResolve: (() => void) | undefined;
  #detachAcknowledgementReject: ((error: Error) => void) | undefined;
  #detachPromise: Promise<void> | undefined;
  public readonly snapshot: AttachedSnapshot;

  public constructor(wire: JsonLinesConnection, snapshot: AttachedSnapshot) {
    this.#wire = wire;
    this.snapshot = snapshot;
    for (const chunk of snapshot.replay.chunks) {
      runEventSources.set(chunk, snapshot.run.id);
    }
    void this.#receivePump();
  }

  public input(
    data: ByteInput,
  ): Promise<AttachmentControlAccepted<InputReceipt>> {
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

  public stop(): Promise<AttachmentControlAccepted<StopReceipt>> {
    return this.#command<StopReceipt>("stop", 0, (commandId) => ({
      type: "stop",
      command_id: commandId,
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

  public async nextEvent(): Promise<RunEvent | undefined> {
    const queued = this.#events.shift();
    if (queued !== undefined) {
      this.#queuedEventBytes -= queued.bytes;
      return queued.event;
    }
    if (this.#pendingOutputGap !== undefined) {
      const gap = this.#pendingOutputGap;
      this.#pendingOutputGap = undefined;
      return gap;
    }
    if (this.#terminalEvent !== undefined) {
      const terminal = this.#terminalEvent;
      this.#terminalEvent = undefined;
      this.#eventStreamEnded = true;
      return terminal;
    }
    if (this.#eventError !== undefined) {
      throw this.#eventError;
    }
    if (this.#eventStreamEnded || this.#state === "closed") {
      return undefined;
    }
    if (this.#eventWaiter !== undefined) {
      throw new Error(
        "only one nextEvent() call may be pending per attachment",
      );
    }
    return await new Promise<RunEvent | undefined>((resolve, reject) => {
      this.#eventWaiter = { resolve, reject };
    });
  }

  public async *events(): AsyncGenerator<RunEvent, void, void> {
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
          this.#terminalSeen && terminalError instanceof WireClosedError,
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
    if (event.type === "exited" || event.type === "interrupted") {
      if (this.#terminalSeen) {
        this.#protocolViolation(
          "attachment delivered more than one terminal event",
          "$frame.event",
        );
        return false;
      }
      this.#terminalSeen = true;
      if (
        this.#events.length === 0 &&
        this.#pendingOutputGap === undefined &&
        this.#eventWaiter !== undefined
      ) {
        const waiter = this.#eventWaiter;
        this.#eventWaiter = undefined;
        this.#eventStreamEnded = true;
        waiter.resolve(event);
      } else {
        this.#terminalEvent = event;
      }
      return true;
    }
    if (this.#terminalSeen) {
      this.#protocolViolation(
        "attachment delivered an event after terminal state",
        "$frame.event",
      );
      return false;
    }
    if (this.#events.length === 0 && this.#eventWaiter !== undefined) {
      const waiter = this.#eventWaiter;
      this.#eventWaiter = undefined;
      waiter.resolve(event);
      return true;
    }

    const eventWeight = eventBytes(event);
    if (event.type === "output" && this.#pendingOutputGap !== undefined) {
      if (this.#eventCapacity(eventWeight, 2)) {
        this.#enqueueEvent({ event: this.#pendingOutputGap, bytes: 0 });
        this.#pendingOutputGap = undefined;
        this.#enqueueEvent({ event, bytes: eventWeight });
      } else {
        this.#extendPendingOutputGap(event.chunk.seq);
      }
      return true;
    }
    if (event.type !== "output" && this.#pendingOutputGap !== undefined) {
      if (!this.#eventCapacity(eventWeight, 2)) {
        this.#protocolViolation(
          "attachment event queue cannot retain an output gap before a non-output event",
          "$frame.event",
        );
        return false;
      }
      this.#enqueueEvent({ event: this.#pendingOutputGap, bytes: 0 });
      this.#pendingOutputGap = undefined;
      this.#enqueueEvent({ event, bytes: eventWeight });
      return true;
    }
    if (this.#eventCapacity(eventWeight, 1)) {
      this.#enqueueEvent({ event, bytes: eventWeight });
      return true;
    }
    if (event.type === "output") {
      this.#extendPendingOutputGap(event.chunk.seq);
      return true;
    }
    this.#protocolViolation(
      "attachment event queue cannot retain a non-output event",
      "$frame.event",
    );
    return false;
  }

  #eventCapacity(bytes: number, additionalEvents: number): boolean {
    return (
      this.#events.length + additionalEvents <= MAX_QUEUED_EVENTS &&
      bytes <= MAX_QUEUED_EVENT_BYTES - this.#queuedEventBytes
    );
  }

  #enqueueEvent(event: QueuedEvent): void {
    this.#events.push(event);
    this.#queuedEventBytes += event.bytes;
  }

  #extendPendingOutputGap(headSequence: number): void {
    this.#pendingOutputGap = {
      type: "gap",
      head_seq: Math.max(this.#pendingOutputGap?.head_seq ?? 0, headSequence),
    };
    rememberRunEventSource(this.#pendingOutputGap, this.snapshot.run.id);
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
      this.#events.length === 0 &&
      this.#pendingOutputGap === undefined &&
      this.#terminalEvent === undefined &&
      this.#eventWaiter !== undefined
    ) {
      const waiter = this.#eventWaiter;
      this.#eventWaiter = undefined;
      if (this.#eventError === undefined) {
        waiter.resolve(undefined);
      } else {
        waiter.reject(this.#eventError);
      }
    }
    if (this.#eventError === undefined) {
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
  readonly event: RunEvent;
  readonly bytes: number;
}

interface EventWaiter {
  readonly resolve: (event: RunEvent | undefined) => void;
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
