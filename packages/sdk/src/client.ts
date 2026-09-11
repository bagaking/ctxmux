import { randomUUID } from "node:crypto";

import {
  Attachment,
  attachmentViewResourcePolicy,
  type AttachmentViewResources,
  type AttachmentViewResourcePolicy,
} from "./attachment.js";
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
import type { RunStorageObservation } from "./generated/RunStorageObservation.js";
import type { RunForegroundObservation } from "./generated/RunForegroundObservation.js";
import type { InputOperationKey } from "./generated/InputOperationKey.js";
import {
  MAX_CREATE_OPERATION_KEY_BYTES,
  MAX_INPUT_OPERATION_KEY_BYTES,
  PROTOCOL_VERSION,
  RUNTIME_CAPABILITY_NATIVE_RECOVERABLE_STOP,
  RUNTIME_CAPABILITY_FOREGROUND_OBSERVATION,
  RUNTIME_CAPABILITY_STORAGE_OBSERVATION,
} from "./generated/constants.js";
import type { Request } from "./generated/Request.js";
import type { Response } from "./generated/Response.js";
import type { RunId } from "./generated/RunId.js";
import type { RunInfo } from "./generated/RunInfo.js";
import type { RunSpec } from "./generated/RunSpec.js";
import type { RunSummary } from "./generated/RunSummary.js";
import type { DiagnosticsSnapshot } from "./generated/DiagnosticsSnapshot.js";
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

/** Per-seed assembly policy; not an aggregate retained heap or RSS limit. */
export interface TerminalSeedLimits {
  /** Maximum decoded synthetic seed bytes; defaults to the historical 32 MiB receive policy. */
  readonly restoreBytes?: number;
}

/** A structurally valid terminal seed exceeds this consumer's resources. */
export class CtxmuxTerminalSeedResourceError extends Error {
  public readonly runId: RunId;
  public readonly reason: "restore_limit" | "allocation";
  public readonly requestedBytes: number;
  public readonly limitBytes: number;
  public readonly recovery = "attach_raw_or_review_local_resources";

  public constructor(
    runId: RunId,
    reason: "restore_limit" | "allocation",
    requestedBytes: number,
    limitBytes: number,
    cause?: unknown,
  ) {
    super(
      `terminal seed for Run ${runId} is unavailable to this consumer: ${reason}; requested ${String(requestedBytes)} bytes, receive limit ${String(limitBytes)} bytes`,
      { cause },
    );
    this.name = "CtxmuxTerminalSeedResourceError";
    this.runId = runId;
    this.reason = reason;
    this.requestedBytes = requestedBytes;
    this.limitBytes = limitBytes;
  }
}

export interface CtxmuxClientOptions {
  readonly socketPath: string;
  /** Local synthetic seed assembly policy; independent of live view budgets. */
  readonly terminalSeedLimits?: TerminalSeedLimits;
  /** Retained view payload/metadata policy; not a process heap limit. */
  readonly attachmentViewResources?: AttachmentViewResources;
  /** Exact Runtime identity required before business dispatch. */
  readonly expectedRuntimeIdentity?: RuntimeIdentity;
  /** Exact Runtime capability versions required before business dispatch. */
  readonly requiredCapabilities?: RuntimeCapabilityRequirements;
  /** Local read wait in milliseconds for this disposable observation wire.
   * Defaults to 30 seconds. Expiry never cancels a Run or an issued OS job. */
  readonly foregroundObservationTimeoutMs?: number;
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

/**
 * One page of retained Run summaries returned by {@link CtxmuxClient.listPage}.
 * `nextCursor` is a non-null {@link RunId} when more Runs may follow (reissue
 * with `after` set to it) and `null` at the end of the fleet.
 */
export interface RunPage {
  readonly runs: readonly RunSummary[];
  readonly nextCursor: RunId | null;
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
  readonly #terminalSeedRestoreBytes: number;
  readonly #attachmentViewResources: AttachmentViewResourcePolicy;
  readonly #expectedRuntimeIdentity: RuntimeIdentity | undefined;
  readonly #requiredCapabilities: ReadonlyMap<string, number>;
  readonly #foregroundObservationTimeoutMs: number;

  public constructor(options: CtxmuxClientOptions) {
    if (options.socketPath.length === 0) {
      throw new TypeError("socketPath must not be empty");
    }
    this.#terminalSeedRestoreBytes = terminalSeedRestoreBytes(
      options.terminalSeedLimits,
    );
    this.#attachmentViewResources = attachmentViewResourcePolicy(
      options.attachmentViewResources,
    );
    this.#socketPath = options.socketPath;
    this.#expectedRuntimeIdentity = copyExpectedRuntimeIdentity(
      options.expectedRuntimeIdentity,
    );
    this.#requiredCapabilities = copyRequiredRuntimeCapabilities(
      options.requiredCapabilities,
    );
    const observationWait = options.foregroundObservationTimeoutMs ?? 30_000;
    if (
      !Number.isSafeInteger(observationWait) ||
      observationWait <= 0 ||
      observationWait > 0x7fff_ffff
    ) {
      throw new TypeError(
        "foregroundObservationTimeoutMs must fit a positive local timer",
      );
    }
    this.#foregroundObservationTimeoutMs = observationWait;
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

  /** Observe the daemon sink; these facts do not determine Run availability. */
  public async diagnostics(): Promise<DiagnosticsSnapshot> {
    const response = await this.#request({ type: "diagnostics" });
    if (response.type !== "diagnostics") {
      throw unexpected("diagnostics response", response.type);
    }
    return response.diagnostics;
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

  /**
   * Enumerate every retained Run as a whole-fleet snapshot.
   *
   * This preserves the "give me everything" contract callers had before
   * enumeration was paged: it walks the cursor internally and concatenates the
   * pages, so a caller that just wants the whole fleet keeps one call. The rows
   * are thin {@link RunSummary} values (identity, backend kind, pid, state,
   * output bytes, attachments); a caller that needs a Run's launch spec,
   * lineage, or capabilities reads its full {@link RunInfo} with
   * {@link CtxmuxClient.status}.
   *
   * Because the walk spans several requests, it is a best-effort snapshot, not
   * an atomic one: a Run that exists for the whole walk appears exactly once,
   * but Runs created or removed while paging may or may not appear.
   */
  public async list(): Promise<readonly RunSummary[]> {
    const runs: RunSummary[] = [];
    let after: RunId | null = null;
    for (;;) {
      const page = await this.listPage(after);
      runs.push(...page.runs);
      if (page.nextCursor === null) {
        return runs;
      }
      after = page.nextCursor;
    }
  }

  /**
   * Fetch one page of retained Run summaries.
   *
   * `after` is an exclusive {@link RunId} cursor (`null` starts at the first
   * Run) and `limit` requests a page size the daemon clamps to its maximum
   * (`null` or `0` means the clamped maximum). The returned page carries the
   * rows and a `nextCursor`: a non-null id means more Runs may follow — reissue
   * with `after` set to it — and `null` means this page reached the end of the
   * fleet. Prefer {@link CtxmuxClient.list} for a whole-fleet snapshot; reach
   * for this when a caller wants to bound memory, show progress, or stop early.
   */
  public async listPage(
    after: RunId | null = null,
    limit: number | null = null,
  ): Promise<RunPage> {
    const response = await this.#request({ type: "list", after, limit });
    if (response.type !== "runs") {
      throw unexpected("runs response", response.type);
    }
    return { runs: response.runs, nextCursor: response.next_cursor };
  }

  public async status(id: RunId): Promise<RunInfo> {
    const response = await this.#request({ type: "status", id });
    if (response.type !== "status") {
      throw unexpected("status response", response.type);
    }
    return response.run;
  }

  /** Independent storage owner facts, not a durability receipt or health verdict. */
  public async observeStorage(runId: RunId): Promise<{
    readonly runtime: RuntimeIdentity;
    readonly observation: RunStorageObservation;
  }> {
    const { wire, runtime } = await this.#connectForDispatch();
    try {
      const advertised =
        runtime.capabilities[RUNTIME_CAPABILITY_STORAGE_OBSERVATION];
      if ((advertised ?? 0) < 1) {
        throw new CtxmuxUnsupportedCapabilityError(
          RUNTIME_CAPABILITY_STORAGE_OBSERVATION,
          1,
          advertised,
        );
      }
      await wire.send({
        type: "request",
        request: { type: "observe_storage", id: runId },
      } satisfies ClientFrame);
      const frame = serverFrame(await wire.receive());
      if (frame.type === "error") throw protocolError(frame.error);
      if (
        frame.type !== "response" ||
        frame.response.type !== "storage_observation" ||
        frame.response.observation.run_id !== runId
      ) {
        throw unexpected("exact Run storage observation", frame.type);
      }
      return { runtime, observation: frame.response.observation };
    } finally {
      wire.close();
    }
  }

  /** One-shot physical facts from the SAME connection's Hello identity. */
  public async observeForeground({
    runId,
  }: {
    readonly runId: RunId;
  }): Promise<{
    readonly runtime: RuntimeIdentity;
    readonly observation: RunForegroundObservation;
  }> {
    // This disposable observation wire has a bounded local read wait. Its
    // abort never disposes a shared client, an attachment or the original Run.
    const { wire, runtime } = await this.#connectForDispatch(
      AbortSignal.timeout(this.#foregroundObservationTimeoutMs),
    );
    try {
      if (
        (runtime.capabilities[RUNTIME_CAPABILITY_FOREGROUND_OBSERVATION] ?? 0) <
        1
      ) {
        return {
          runtime,
          observation: {
            outcome: "unsupported",
            runId,
            reason: "capability-missing",
          },
        };
      }
      await wire.send({
        type: "request",
        request: { type: "observe_foreground", runId },
      } satisfies ClientFrame);
      const frame = serverFrame(await wire.receive());
      if (frame.type === "error") throw protocolError(frame.error);
      if (
        frame.type !== "response" ||
        frame.response.type !== "foreground_observation" ||
        frame.response.observation.runId !== runId
      ) {
        throw unexpected("exact Run foreground observation", frame.type);
      }
      return { runtime, observation: frame.response.observation };
    } finally {
      wire.close();
    }
  }

  /**
   * Reclaim one already-terminal, unpinned Run so its retained record slot
   * returns to the daemon's budget. This never forces teardown: a running or
   * attached Run is refused with a typed protocol error, and removing an
   * unknown or already-removed id reports `run_not_found`, so a retry is
   * idempotent.
   */
  public async remove(id: RunId): Promise<void> {
    const response = await this.#request({ type: "remove", id });
    if (response.type !== "removed") {
      throw unexpected("removed response", response.type);
    }
    if (response.id !== id) {
      throw unexpected(`removed response for ${id}`, response.id);
    }
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
    return await this.#attach(id, afterByte, "raw");
  }

  /** Restore a terminal from the daemon's basic state and only its original tail. */
  public async attachTerminal(id: RunId, afterByte = 0): Promise<Attachment> {
    return await this.#attach(id, afterByte, "terminal");
  }

  async #attach(
    id: RunId,
    afterByte: number,
    view: "raw" | "terminal",
  ): Promise<Attachment> {
    validateCursor(afterByte, "afterByte");
    const { wire, runtime } = await this.#connectForDispatch();
    try {
      await wire.send({
        type: "request",
        request: { type: "attach", id, after_byte: afterByte, view },
      } satisfies ClientFrame);
      const frame = serverFrame(await wire.receive());
      if (frame.type === "error") {
        throw protocolError(frame.error);
      }
      if (frame.type !== "attached") {
        throw unexpected("attached snapshot", frame.type);
      }
      if (
        frame.snapshot.run.id !== id ||
        (frame.snapshot.terminal.type === "not_requested") !== (view === "raw")
      ) {
        throw unexpected(
          `exact Run ${view} attachment`,
          frame.snapshot.terminal.type,
        );
      }
      const snapshot = await receiveReplay(
        wire,
        afterByte,
        frame.snapshot,
        this.#terminalSeedRestoreBytes,
      );
      return new Attachment(
        wire,
        snapshot,
        this.#attachmentViewResources,
        runtime,
        afterByte,
      );
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
    let runtime: RuntimeIdentity;
    try {
      ({ wire, runtime } = await this.#connectForDispatch());
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

      if (first.snapshot.terminal.type !== "not_requested") {
        throw unexpected(
          "raw recoverable Stop attachment",
          first.snapshot.terminal.type,
        );
      }

      const snapshot = await receiveReplay(
        wire,
        afterByte,
        first.snapshot,
        this.#terminalSeedRestoreBytes,
      );
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
        attachment: new Attachment(
          wire,
          snapshot,
          this.#attachmentViewResources,
          runtime,
          afterByte,
        ),
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

  async #connect(signal?: AbortSignal): Promise<{
    readonly wire: JsonLinesConnection;
    readonly runtime: RuntimeIdentity;
  }> {
    const wire = await JsonLinesConnection.connect(this.#socketPath, signal);
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

  async #connectForDispatch(signal?: AbortSignal): Promise<{
    readonly wire: JsonLinesConnection;
    readonly runtime: RuntimeIdentity;
  }> {
    const connection = await this.#connect(signal);
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

function terminalSeedRestoreBytes(
  limits: TerminalSeedLimits | undefined,
): number {
  if (limits === undefined) return 32 * 1024 * 1024;
  if (limits === null || typeof limits !== "object" || Array.isArray(limits)) {
    throw new TypeError("terminalSeedLimits must be an object");
  }
  if (Object.keys(limits).some((key) => key !== "restoreBytes")) {
    throw new TypeError("terminalSeedLimits contains an unknown limit");
  }
  const value =
    limits.restoreBytes === undefined ? 32 * 1024 * 1024 : limits.restoreBytes;
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    throw new TypeError(
      "terminalSeedLimits.restoreBytes must be a nonnegative safe integer byte count",
    );
  }
  return value;
}

async function receiveReplay(
  wire: JsonLinesConnection,
  afterByte: number,
  header: Extract<ServerFrame, { readonly type: "attached" }>["snapshot"],
  restoreByteLimit: number,
): Promise<AttachedSnapshot> {
  let replay = { ...header.replay };
  let run = { ...header.run };
  let terminalRestore = new Uint8Array(0);
  if (header.terminal.type === "basic_vt") {
    const checkpoint = header.terminal.checkpoint;
    if (checkpoint.restore_bytes > restoreByteLimit) {
      throw new CtxmuxTerminalSeedResourceError(
        header.run.id,
        "restore_limit",
        checkpoint.restore_bytes,
        restoreByteLimit,
      );
    }
    // Only allocation errors belong to local resources. Frame validation and
    // ordered byte assembly below retain their existing protocol semantics.
    try {
      terminalRestore = new Uint8Array(checkpoint.restore_bytes);
    } catch (cause) {
      throw new CtxmuxTerminalSeedResourceError(
        header.run.id,
        "allocation",
        checkpoint.restore_bytes,
        restoreByteLimit,
        cause,
      );
    }
    let offset = 0;
    while (offset < terminalRestore.length) {
      const frame = serverFrame(await wire.receive());
      if (frame.type === "error") throw protocolError(frame.error);
      if (
        frame.type !== "terminal_checkpoint_chunk" ||
        frame.offset !== offset ||
        frame.data.length === 0 ||
        frame.data.length > terminalRestore.length - offset
      ) {
        throw unexpected("ordered synthetic terminal seed", frame.type);
      }
      terminalRestore.set(frame.data, offset);
      offset += frame.data.length;
    }
    afterByte = checkpoint.through_byte;
  }
  const chunks: AttachedSnapshot["replay"]["chunks"] = [];
  if (afterByte >= header.replay.latest_output_bytes) {
    return {
      run: header.run,
      terminal: header.terminal,
      terminal_restore: terminalRestore,
      resize_revision: header.resize_revision,
      replay: { ...header.replay, chunks },
    };
  }
  let expectedByte = Math.max(afterByte, header.replay.first_available_byte);
  while (expectedByte < header.replay.latest_output_bytes) {
    const frame = serverFrame(await wire.receive());
    if (frame.type === "error") {
      throw protocolError(frame.error);
    }
    if (frame.type === "replay_window") {
      if (
        frame.latest_output_bytes !== header.replay.latest_output_bytes ||
        frame.first_available_byte <= expectedByte ||
        frame.first_available_byte > frame.latest_output_bytes
      )
        throw unexpected(
          "a strictly newer replay floor through the advertised head",
          frame.type,
        );
      if (header.terminal.type === "basic_vt") {
        throw unexpected(
          "complete terminal checkpoint original tail",
          frame.type,
        );
      }
      chunks.length = 0;
      expectedByte = frame.first_available_byte;
      replay = {
        ...replay,
        first_available_byte: expectedByte,
        truncated: true,
      };
      run = { ...run, first_available_byte: expectedByte };
      continue;
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
    run,
    terminal: header.terminal,
    terminal_restore: terminalRestore,
    resize_revision: header.resize_revision,
    replay: { ...replay, chunks },
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
