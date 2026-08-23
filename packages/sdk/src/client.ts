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
  RUNTIME_CAPABILITY_NATIVE_RECOVERABLE_STOP,
} from "./generated/constants.js";
import type { Request } from "./generated/Request.js";
import type { Response } from "./generated/Response.js";
import type { RunId } from "./generated/RunId.js";
import type { RunInfo } from "./generated/RunInfo.js";
import type { RunSpec } from "./generated/RunSpec.js";
import type { RuntimeIdentity } from "./generated/RuntimeIdentity.js";
import type { ServerFrame } from "./generated/ServerFrame.js";
import type { TerminalSize } from "./generated/TerminalSize.js";
import type { TmuxPaneInfo } from "./generated/TmuxPaneInfo.js";
import {
  copyRequiredRuntimeCapabilities,
  CtxmuxInvalidFrameError,
  validateCursor,
  validateServerFrame,
} from "./validation.js";
import { encodeJsonLine, JsonLinesConnection } from "./wire.js";
import {
  encodeRecoverableStop,
  type RecoverableStopOperation,
  stopOperationKey,
} from "./stop-operation.js";

export interface CtxmuxClientOptions {
  readonly socketPath: string;
  /** Exact Runtime identity required before business dispatch. */
  readonly expectedRuntimeIdentity?: RuntimeIdentity;
  /** Exact Runtime capability versions required before business dispatch. */
  readonly requiredCapabilities?: RuntimeCapabilityRequirements;
}

/** Exact Runtime capability versions required before business dispatch. */
export type RuntimeCapabilityRequirements = Readonly<Record<string, number>>;

/** The dispatch connection reached a different Runtime than the caller retained. */
export class CtxmuxRuntimeIdentityMismatchError extends Error {
  public readonly expected: RuntimeIdentity;
  public readonly actual: RuntimeIdentity;

  public constructor(expected: RuntimeIdentity, actual: RuntimeIdentity) {
    super(
      `reachable Runtime identity ${actual.runtimeId}/${actual.daemonInstanceId} does not match expected ${expected.runtimeId}/${expected.daemonInstanceId}`,
    );
    this.name = "CtxmuxRuntimeIdentityMismatchError";
    this.expected = copyRuntimeIdentity(expected);
    this.actual = copyRuntimeIdentity(actual);
  }
}

/** A client-local Runtime capability precondition is not satisfied. */
export class CtxmuxUnsupportedCapabilityError extends CtxmuxProtocolError {
  public readonly capability: string;
  public readonly requiredVersion: number;
  public readonly advertisedVersion: number | undefined;

  public constructor(
    capability: string,
    requiredVersion: number,
    advertisedVersion: number | undefined,
  ) {
    super(
      "unsupported_capability",
      `unsupported Runtime capability ${JSON.stringify(capability)}: required ${String(requiredVersion)}, advertised ${advertisedVersion === undefined ? "absent" : String(advertisedVersion)}`,
    );
    this.name = "CtxmuxUnsupportedCapabilityError";
    this.capability = capability;
    this.requiredVersion = requiredVersion;
    this.advertisedVersion = advertisedVersion;
  }
}

export interface RecoverableInputOperation {
  readonly daemonInstance: DaemonInstanceId;
  readonly operationKey: InputOperationKey;
  readonly runId: RunId;
  readonly expectedByte: number;
  readonly data: ByteInput;
}

/** One explicit recoverable Stop followed by an attachment to its exact Run. */
export interface RecoverableStopAttachment {
  readonly attachment: Attachment;
  readonly stop: ControlAccepted<StopReceipt>;
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
  readonly #expectedRuntimeIdentity: RuntimeIdentity | undefined;
  readonly #requiredCapabilities: ReadonlyMap<string, number>;

  public constructor(options: CtxmuxClientOptions) {
    if (options.socketPath.length === 0) {
      throw new TypeError("socketPath must not be empty");
    }
    this.#socketPath = options.socketPath;
    this.#expectedRuntimeIdentity = copyExpectedRuntimeIdentity(
      options.expectedRuntimeIdentity,
    );
    this.#requiredCapabilities = copyRequiredRuntimeCapabilities(
      options.requiredCapabilities,
    );
  }

  public async ping(): Promise<void> {
    const { wire } = await this.#connect();
    wire.close();
  }

  public async daemonInstance(): Promise<DaemonInstanceId> {
    return (await this.runtimeInfo()).daemonInstanceId;
  }

  public async runtimeInfo(): Promise<RuntimeIdentity> {
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

  /** Prepare one caller-retained Stop operation without applying it. */
  public async prepareStop(
    id: RunId,
    operationKey = stopOperationKey(),
  ): Promise<RecoverableStopOperation> {
    const runtime = await this.runtimeInfo();
    const advertisedVersion = Object.hasOwn(
      runtime.capabilities,
      RUNTIME_CAPABILITY_NATIVE_RECOVERABLE_STOP,
    )
      ? runtime.capabilities[RUNTIME_CAPABILITY_NATIVE_RECOVERABLE_STOP]
      : undefined;
    if (advertisedVersion === undefined || advertisedVersion < 1) {
      throw new CtxmuxUnsupportedCapabilityError(
        RUNTIME_CAPABILITY_NATIVE_RECOVERABLE_STOP,
        1,
        advertisedVersion,
      );
    }
    return {
      daemonInstance: runtime.daemonInstanceId,
      operationKey: stopOperationKey(operationKey),
      runId: id,
    };
  }

  /** Apply or recover one caller-retained complete-session Stop. */
  public async stop(
    operation: RecoverableStopOperation,
  ): Promise<ControlAccepted<StopReceipt>> {
    const response = decodeShortControl(
      await this.#controlRequest({
        type: "stop",
        operation: encodeRecoverableStop(operation),
      }),
      decodeStopReceipt,
    );
    if (response.run.id !== operation.runId) {
      throw new CtxmuxCommandError(
        "internal",
        "recoverable Stop response names another Run",
        "unknown",
      );
    }
    return response;
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
    const { wire } = await this.#connectForDispatch();
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

  /**
   * Apply or recover one Stop operation and attach to its exact Run without
   * racing the ordinary terminal attachment EOF.
   */
  public async attachRecoverableStop(
    operation: RecoverableStopOperation,
    afterByte = 0,
  ): Promise<RecoverableStopAttachment> {
    validateCursor(afterByte, "afterByte");
    let wire: JsonLinesConnection;
    try {
      ({ wire } = await this.#connectForDispatch());
    } catch (error) {
      if (isDispatchPreconditionError(error)) {
        throw error;
      }
      throw new CtxmuxCommandError(
        error instanceof CtxmuxProtocolError ? error.code : "io",
        asError(error).message,
        "not_applied",
      );
    }

    let encodedFrame: string;
    try {
      encodedFrame = encodeJsonLine({
        type: "request",
        request: {
          type: "attach_recoverable_stop",
          operation: encodeRecoverableStop(operation),
          after_byte: afterByte,
        },
      } satisfies ClientFrame);
    } catch (error) {
      wire.close();
      throw new CtxmuxCommandError(
        "invalid_request",
        asError(error).message,
        "not_applied",
      );
    }

    try {
      await wire.sendEncoded(encodedFrame);
      const first = serverFrame(await wire.receive());
      if (first.type === "response") {
        decodeShortControl(first.response, decodeStopReceipt);
        throw new CtxmuxCommandError(
          "internal",
          "recoverable Stop was accepted without an attachment snapshot",
          "unknown",
        );
      }
      if (first.type === "error") {
        throw new CtxmuxCommandError(
          first.error.code,
          first.error.message,
          "unknown",
        );
      }
      if (first.type !== "attached") {
        throw new CtxmuxCommandError(
          "internal",
          `expected recoverable Stop attachment snapshot, received ${first.type}`,
          "unknown",
        );
      }

      const snapshot = await receiveReplay(wire, afterByte, first.snapshot);
      const result = serverFrame(await wire.receive());
      if (result.type === "error") {
        throw new CtxmuxCommandError(
          result.error.code,
          result.error.message,
          "unknown",
        );
      }
      if (result.type !== "response") {
        throw new CtxmuxCommandError(
          "internal",
          `expected recoverable Stop result, received ${result.type}`,
          "unknown",
        );
      }
      if (result.response.type !== "control_accepted") {
        throw new CtxmuxCommandError(
          "internal",
          "recoverable Stop attachment was rejected after its snapshot",
          "unknown",
        );
      }
      const stop = decodeShortControl(result.response, decodeStopReceipt);
      if (
        stop.run.id !== operation.runId ||
        snapshot.run.id !== operation.runId
      ) {
        throw new CtxmuxCommandError(
          "internal",
          "recoverable Stop attachment names another Run",
          "unknown",
        );
      }
      return {
        attachment: new Attachment(wire, snapshot),
        stop,
      };
    } catch (error) {
      wire.close();
      if (error instanceof CtxmuxCommandError) {
        throw error;
      }
      throw new CtxmuxCommandError(
        error instanceof CtxmuxInvalidFrameError ? "internal" : "io",
        asError(error).message,
        "unknown",
      );
    }
  }

  async #request(request: Request): Promise<Response> {
    const { wire } = await this.#connectForDispatch();
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
      ({ wire } = await this.#connectForDispatch());
    } catch (error) {
      if (isDispatchPreconditionError(error)) {
        throw error;
      }
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
    readonly runtime: RuntimeIdentity;
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
        frame.runtime.protocolGeneration !== PROTOCOL_VERSION
      ) {
        throw unexpected("compatible hello", frame.type);
      }
      return { wire, runtime: frame.runtime };
    } catch (error) {
      wire.close();
      throw error;
    }
  }

  async #connectForDispatch(): Promise<{
    readonly wire: JsonLinesConnection;
    readonly runtime: RuntimeIdentity;
  }> {
    const connection = await this.#connect();
    try {
      if (
        this.#expectedRuntimeIdentity !== undefined &&
        !runtimeIdentitiesEqual(
          connection.runtime,
          this.#expectedRuntimeIdentity,
        )
      ) {
        throw new CtxmuxRuntimeIdentityMismatchError(
          this.#expectedRuntimeIdentity,
          connection.runtime,
        );
      }
      for (const [capability, requiredVersion] of this.#requiredCapabilities) {
        const advertisedVersion = Object.hasOwn(
          connection.runtime.capabilities,
          capability,
        )
          ? connection.runtime.capabilities[capability]
          : undefined;
        if (
          advertisedVersion === undefined ||
          advertisedVersion < requiredVersion
        ) {
          throw new CtxmuxUnsupportedCapabilityError(
            capability,
            requiredVersion,
            advertisedVersion,
          );
        }
      }
      return connection;
    } catch (error) {
      connection.wire.close();
      throw error;
    }
  }
}

function copyExpectedRuntimeIdentity(
  expected: RuntimeIdentity | undefined,
): RuntimeIdentity | undefined {
  if (expected === undefined) {
    return undefined;
  }
  const frame = validateServerFrame({ type: "hello", runtime: expected });
  if (frame.type !== "hello") {
    throw new TypeError("expectedRuntimeIdentity must be a Runtime identity");
  }
  return copyRuntimeIdentity(frame.runtime);
}

function copyRuntimeIdentity(runtime: RuntimeIdentity): RuntimeIdentity {
  return {
    ...runtime,
    capabilities: { ...runtime.capabilities },
  };
}

function runtimeIdentitiesEqual(
  actual: RuntimeIdentity,
  expected: RuntimeIdentity,
): boolean {
  const actualCapabilities = Object.entries(actual.capabilities);
  return (
    actual.daemonInstanceId === expected.daemonInstanceId &&
    actual.runtimeId === expected.runtimeId &&
    actual.runtimeIdPersistence === expected.runtimeIdPersistence &&
    actual.buildId === expected.buildId &&
    actual.protocolGeneration === expected.protocolGeneration &&
    actual.platform === expected.platform &&
    actual.arch === expected.arch &&
    actualCapabilities.length === Object.keys(expected.capabilities).length &&
    actualCapabilities.every(
      ([capability, version]) =>
        Object.hasOwn(expected.capabilities, capability) &&
        expected.capabilities[capability] === version,
    )
  );
}

function isDispatchPreconditionError(
  error: unknown,
): error is
  CtxmuxRuntimeIdentityMismatchError | CtxmuxUnsupportedCapabilityError {
  return (
    error instanceof CtxmuxRuntimeIdentityMismatchError ||
    error instanceof CtxmuxUnsupportedCapabilityError
  );
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
