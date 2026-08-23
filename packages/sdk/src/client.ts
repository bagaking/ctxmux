import { randomUUID } from "node:crypto";

import { Attachment } from "./attachment.js";
import {
  asError,
  bytes,
  commandError,
  CtxmuxCommandError,
  CtxmuxProtocolError,
  decodeInputReceipt,
  decodeResizeReceipt,
  decodeSignalReceipt,
  decodeShortControl,
  decodeStopReceipt,
  protocolError,
  type ByteInput,
  type ControlAccepted,
  type InputReceipt,
  type ResizeReceipt,
  type SignalReceipt,
  type StopReceipt,
} from "./control.js";
import type { AttachedSnapshot } from "./generated/AttachedSnapshot.js";
import type { AppliedInputRange } from "./generated/AppliedInputRange.js";
import type { ClientFrame } from "./generated/ClientFrame.js";
import type { CreateOperationKey } from "./generated/CreateOperationKey.js";
import type { DaemonInstanceId } from "./generated/DaemonInstanceId.js";
import type { ForkPlan } from "./generated/ForkPlan.js";
import type { InputOperationKey } from "./generated/InputOperationKey.js";
import {
  MAX_CREATE_OPERATION_KEY_BYTES,
  MAX_INPUT_OPERATION_KEY_BYTES,
  PROTOCOL_VERSION,
} from "./generated/constants.js";
import type { Request } from "./generated/Request.js";
import type { Response } from "./generated/Response.js";
import type { RunId } from "./generated/RunId.js";
import type { RunInfo } from "./generated/RunInfo.js";
import type { RunSpec } from "./generated/RunSpec.js";
import type { RuntimeDescription } from "./generated/RuntimeDescription.js";
import type { ServerFrame } from "./generated/ServerFrame.js";
import type { TerminalSize } from "./generated/TerminalSize.js";
import type { TmuxPaneInfo } from "./generated/TmuxPaneInfo.js";
import {
  CtxmuxInvalidFrameError,
  validateCursor,
  validateServerFrame,
} from "./validation.js";
import { encodeJsonLine, JsonLinesConnection } from "./wire.js";

export interface CtxmuxClientOptions {
  readonly socketPath: string;
}

export interface RecoverableInputOperation {
  readonly daemonInstance: DaemonInstanceId;
  readonly operationKey: InputOperationKey;
  readonly runId: RunId;
  readonly expectedByte: number;
  readonly data: ByteInput;
}

/** Validate or generate one caller-retained Run creation operation key. */
export function createOperationKey(
  value: string = randomUUID(),
): CreateOperationKey {
  if (typeof value !== "string") {
    throw new TypeError("Run creation operation key must be a string");
  }
  if (!isWellFormedUtf16(value)) {
    throw new TypeError(
      "Run creation operation key must be well-formed UTF-16",
    );
  }
  const byteLength = new TextEncoder().encode(value).byteLength;
  if (byteLength === 0) {
    throw new TypeError("Run creation operation key must not be empty");
  }
  if (byteLength > MAX_CREATE_OPERATION_KEY_BYTES) {
    throw new TypeError(
      `Run creation operation key is ${String(byteLength)} bytes; maximum is ${String(MAX_CREATE_OPERATION_KEY_BYTES)}`,
    );
  }
  return value;
}

/** Validate or generate one caller-retained native Input operation key. */
export function inputOperationKey(
  value: string = randomUUID(),
): InputOperationKey {
  if (typeof value !== "string" || !isWellFormedUtf16(value)) {
    throw new TypeError(
      "native Input operation key must be well-formed UTF-16",
    );
  }
  const byteLength = new TextEncoder().encode(value).byteLength;
  if (byteLength === 0) {
    throw new TypeError("native Input operation key must not be empty");
  }
  if (byteLength > MAX_INPUT_OPERATION_KEY_BYTES) {
    throw new TypeError(
      `native Input operation key is ${String(byteLength)} bytes; maximum is ${String(MAX_INPUT_OPERATION_KEY_BYTES)}`,
    );
  }
  return value;
}

function isWellFormedUtf16(value: string): boolean {
  for (let index = 0; index < value.length; index += 1) {
    const codeUnit = value.charCodeAt(index);
    if (codeUnit >= 0xd800 && codeUnit <= 0xdbff) {
      const next = value.charCodeAt(index + 1);
      if (index + 1 >= value.length || next < 0xdc00 || next > 0xdfff) {
        return false;
      }
      index += 1;
    } else if (codeUnit >= 0xdc00 && codeUnit <= 0xdfff) {
      return false;
    }
  }
  return true;
}

/** Stateless connector to one local ctxmux daemon. */
export class CtxmuxClient {
  readonly #socketPath: string;

  public constructor(options: CtxmuxClientOptions) {
    if (options.socketPath.length === 0) {
      throw new TypeError("socketPath must not be empty");
    }
    this.#socketPath = options.socketPath;
  }

  public async ping(): Promise<void> {
    const { wire } = await this.#connect();
    wire.close();
  }

  public async daemonInstance(): Promise<DaemonInstanceId> {
    return (await this.runtimeInfo()).daemon_instance_id;
  }

  public async runtimeInfo(): Promise<RuntimeDescription> {
    const { wire, runtime } = await this.#connect();
    wire.close();
    return runtime;
  }

  public async start(
    spec: RunSpec,
    operationKey: CreateOperationKey = createOperationKey(),
  ): Promise<RunInfo> {
    const response = await this.#request({
      type: "start",
      operation_key: createOperationKey(operationKey),
      spec,
    });
    if (response.type !== "started") {
      throw unexpected("started response", response.type);
    }
    return response.run;
  }

  public async discoverTmux(socketPath: string): Promise<{
    readonly tmuxVersion: string;
    readonly panes: readonly TmuxPaneInfo[];
  }> {
    if (socketPath.length === 0) {
      throw new TypeError("tmux socketPath must not be empty");
    }
    const response = await this.#request({
      type: "discover_tmux",
      socket_path: socketPath,
    });
    if (response.type !== "tmux_panes") {
      throw unexpected("tmux panes response", response.type);
    }
    return { tmuxVersion: response.tmux_version, panes: response.panes };
  }

  public async importTmux(
    socketPath: string,
    paneId: string,
  ): Promise<RunInfo> {
    if (socketPath.length === 0 || paneId.length === 0) {
      throw new TypeError("tmux socketPath and paneId must not be empty");
    }
    const response = await this.#request({
      type: "import_tmux",
      socket_path: socketPath,
      pane_id: paneId,
    });
    if (response.type !== "imported") {
      throw unexpected("imported response", response.type);
    }
    return response.run;
  }

  public async fork(
    parent: RunId,
    plan: ForkPlan,
    operationKey: CreateOperationKey = createOperationKey(),
  ): Promise<RunInfo> {
    const response = await this.#request({
      type: "fork",
      operation_key: createOperationKey(operationKey),
      parent,
      plan,
    });
    if (response.type !== "forked") {
      throw unexpected("forked response", response.type);
    }
    return response.run;
  }

  public async list(): Promise<readonly RunInfo[]> {
    const response = await this.#request({ type: "list" });
    if (response.type !== "runs") {
      throw unexpected("runs response", response.type);
    }
    return response.runs;
  }

  public async status(id: RunId): Promise<RunInfo> {
    const response = await this.#request({ type: "status", id });
    if (response.type !== "status") {
      throw unexpected("status response", response.type);
    }
    return response.run;
  }

  public async input(
    id: RunId,
    data: ByteInput,
  ): Promise<ControlAccepted<InputReceipt>> {
    const payload = bytes(data);
    const response = await this.#controlRequest({
      type: "input",
      id,
      data: payload,
    });
    return decodeShortControl(response, (receipt) =>
      decodeInputReceipt(receipt, payload.length),
    );
  }

  public async recoverableInput(
    operation: RecoverableInputOperation,
  ): Promise<ControlAccepted<AppliedInputRange>> {
    validateCursor(operation.expectedByte, "expectedByte");
    const payload = bytes(operation.data);
    if (payload.length === 0) {
      throw new TypeError("recoverable native Input must not be empty");
    }
    const response = await this.#controlRequest({
      type: "recoverable_input",
      operation: {
        daemon_instance: operation.daemonInstance,
        operation_key: inputOperationKey(operation.operationKey),
        id: operation.runId,
        expected_byte: operation.expectedByte,
        data: payload,
      },
    });
    if (response.type === "control_rejected") {
      throw commandError(response.failure);
    }
    if (response.type !== "input_applied") {
      throw new CtxmuxCommandError(
        "internal",
        `expected input_applied response, received ${response.type}`,
        "unknown",
      );
    }
    const expectedEnd = operation.expectedByte + payload.length;
    if (
      !Number.isSafeInteger(expectedEnd) ||
      response.run.id !== operation.runId ||
      response.range.start_byte !== operation.expectedByte ||
      response.range.end_byte !== expectedEnd ||
      response.run.applied_input_bytes === null ||
      response.run.applied_input_bytes < expectedEnd
    ) {
      throw new CtxmuxCommandError(
        "internal",
        "recoverable Input Run, range, or cursor does not prove its request",
        "unknown",
      );
    }
    return { run: response.run, receipt: response.range };
  }

  public async resize(
    id: RunId,
    size: TerminalSize,
  ): Promise<ControlAccepted<ResizeReceipt>> {
    return decodeShortControl(
      await this.#controlRequest({ type: "resize", id, size }),
      decodeResizeReceipt,
    );
  }

  public async stop(id: RunId): Promise<ControlAccepted<StopReceipt>> {
    return decodeShortControl(
      await this.#controlRequest({ type: "stop", id }),
      decodeStopReceipt,
    );
  }

  public async interrupt(id: RunId): Promise<ControlAccepted<SignalReceipt>> {
    return decodeShortControl(
      await this.#controlRequest({
        type: "signal",
        id,
        signal: "interrupt",
      }),
      decodeSignalReceipt,
    );
  }

  public async attach(id: RunId, afterByte = 0): Promise<Attachment> {
    validateCursor(afterByte, "afterByte");
    const { wire } = await this.#connect();
    try {
      await wire.send({
        type: "request",
        request: { type: "attach", id, after_byte: afterByte },
      } satisfies ClientFrame);
      const frame = serverFrame(await wire.receive());
      if (frame.type === "error") {
        throw protocolError(frame.error);
      }
      if (frame.type !== "attached") {
        throw unexpected("attached snapshot", frame.type);
      }
      const snapshot = await receiveReplay(wire, afterByte, frame.snapshot);
      return new Attachment(wire, snapshot);
    } catch (error) {
      wire.close();
      throw error;
    }
  }

  async #request(request: Request): Promise<Response> {
    const { wire } = await this.#connect();
    try {
      await wire.send({ type: "request", request } satisfies ClientFrame);
      const frame = serverFrame(await wire.receive());
      if (frame.type === "error") {
        throw protocolError(frame.error);
      }
      if (frame.type !== "response") {
        throw unexpected("request response", frame.type);
      }
      return frame.response;
    } finally {
      wire.close();
    }
  }

  async #controlRequest(request: Request): Promise<Response> {
    let wire: JsonLinesConnection;
    try {
      ({ wire } = await this.#connect());
    } catch (error) {
      throw new CtxmuxCommandError(
        error instanceof CtxmuxProtocolError ? error.code : "io",
        asError(error).message,
        "not_applied",
      );
    }
    try {
      let encodedFrame: string;
      try {
        encodedFrame = encodeJsonLine({
          type: "request",
          request,
        } satisfies ClientFrame);
      } catch (error) {
        throw new CtxmuxCommandError(
          "invalid_request",
          asError(error).message,
          "not_applied",
        );
      }
      let frame: ServerFrame;
      try {
        await wire.sendEncoded(encodedFrame);
        frame = serverFrame(await wire.receive());
      } catch (error) {
        if (error instanceof CtxmuxCommandError) {
          throw error;
        }
        throw new CtxmuxCommandError(
          error instanceof CtxmuxInvalidFrameError ? "internal" : "io",
          asError(error).message,
          "unknown",
        );
      }
      if (frame.type === "error") {
        throw new CtxmuxCommandError(
          frame.error.code,
          frame.error.message,
          "unknown",
        );
      }
      if (frame.type !== "response") {
        throw new CtxmuxCommandError(
          "internal",
          `expected request response, received ${frame.type}`,
          "unknown",
        );
      }
      return frame.response;
    } finally {
      wire.close();
    }
  }

  async #connect(): Promise<{
    readonly wire: JsonLinesConnection;
    readonly runtime: RuntimeDescription;
  }> {
    const wire = await JsonLinesConnection.connect(this.#socketPath);
    try {
      await wire.send({
        type: "hello",
        hello: { protocol: PROTOCOL_VERSION },
      } satisfies ClientFrame);
      const frame = serverFrame(await wire.receive());
      if (frame.type === "error") {
        throw protocolError(frame.error);
      }
      if (
        frame.type !== "hello" ||
        frame.runtime.protocol_generation !== PROTOCOL_VERSION
      ) {
        throw unexpected("compatible hello", frame.type);
      }
      return { wire, runtime: frame.runtime };
    } catch (error) {
      wire.close();
      throw error;
    }
  }
}

async function receiveReplay(
  wire: JsonLinesConnection,
  afterByte: number,
  header: Extract<ServerFrame, { readonly type: "attached" }>["snapshot"],
): Promise<AttachedSnapshot> {
  const chunks: AttachedSnapshot["replay"]["chunks"] = [];
  if (afterByte >= header.replay.latest_output_bytes) {
    return {
      run: header.run,
      replay: { ...header.replay, chunks },
    };
  }
  let expectedByte = Math.max(afterByte, header.replay.first_available_byte);
  while (expectedByte < header.replay.latest_output_bytes) {
    const frame = serverFrame(await wire.receive());
    if (frame.type === "error") {
      throw protocolError(frame.error);
    }
    if (
      frame.type !== "event" ||
      frame.event.type !== "output" ||
      frame.event.chunk.start_byte !== expectedByte ||
      frame.event.chunk.end_byte > header.replay.latest_output_bytes
    ) {
      throw unexpected("ordered replay output", frame.type);
    }
    chunks.push(frame.event.chunk);
    expectedByte = frame.event.chunk.end_byte;
  }
  return {
    run: header.run,
    replay: { ...header.replay, chunks },
  };
}

function unexpected(expected: string, actual: string): Error {
  return new Error(`expected ${expected}, received ${actual}`);
}

function serverFrame(value: unknown): ServerFrame {
  return validateServerFrame(value);
}

export {
  Attachment,
  CtxmuxCommandError,
  CtxmuxInvalidFrameError,
  CtxmuxProtocolError,
};
export type {
  AttachmentControlAccepted,
  ByteInput,
  ControlAccepted,
  InputReceipt,
  ResizeReceipt,
  SignalReceipt,
  StopReceipt,
} from "./control.js";
