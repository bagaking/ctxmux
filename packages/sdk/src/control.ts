import type { AttachmentCommandId } from "./generated/AttachmentCommandId.js";
import type { CommandDisposition } from "./generated/CommandDisposition.js";
import type { ControlFailure } from "./generated/ControlFailure.js";
import type { ControlReceipt } from "./generated/ControlReceipt.js";
import type { ErrorCode } from "./generated/ErrorCode.js";
import { MAX_FRAME_BYTES } from "./generated/constants.js";
import type { Response } from "./generated/Response.js";
import type { RunInfo } from "./generated/RunInfo.js";
import { CtxmuxInvalidFrameError } from "./validation.js";

export type ByteInput = string | Uint8Array;
export type InputReceipt = Extract<ControlReceipt, { readonly type: "input" }>;
export type ResizeReceipt = Extract<
  ControlReceipt,
  { readonly type: "resize" }
>;
export type StopReceipt = Extract<ControlReceipt, { readonly type: "stop" }>;

/** One short-lived control accepted at its named daemon owner boundary. */
export interface ControlAccepted<R> {
  readonly run: RunInfo;
  readonly receipt: R;
}

/** One attachment command accepted with its connection-local correlation ID. */
export interface AttachmentControlAccepted<R extends ControlReceipt> {
  readonly commandId: AttachmentCommandId;
  readonly receipt: R;
}

export class CtxmuxProtocolError extends Error {
  public readonly code: ErrorCode;

  public constructor(code: ErrorCode, message: string) {
    super(message);
    this.name = "CtxmuxProtocolError";
    this.code = code;
  }
}

/** A control command failed with an explicit retry-safety disposition. */
export class CtxmuxCommandError extends CtxmuxProtocolError {
  public readonly failure: ControlFailure;
  public readonly disposition: CommandDisposition;
  public readonly commandId: AttachmentCommandId | undefined;

  public constructor(
    code: ErrorCode,
    message: string,
    disposition: CommandDisposition,
    commandId?: AttachmentCommandId,
  ) {
    super(code, message);
    this.name = "CtxmuxCommandError";
    this.disposition = disposition;
    this.commandId = commandId;
    this.failure = { error: { code, message }, disposition };
  }
}

export function bytes(input: ByteInput): number[] {
  const encoded =
    typeof input === "string" ? new TextEncoder().encode(input) : input;
  if (encoded.byteLength > MAX_FRAME_BYTES) {
    throw new CtxmuxCommandError(
      "invalid_request",
      `ctxmux input exceeds ${String(MAX_FRAME_BYTES)} raw bytes`,
      "not_applied",
    );
  }
  return Array.from(encoded);
}

export function decodeShortControl<R extends ControlReceipt>(
  response: Response,
  decode: (receipt: ControlReceipt) => R,
): ControlAccepted<R> {
  if (response.type === "control_rejected") {
    throw commandError(response.failure);
  }
  if (response.type !== "control_accepted") {
    throw new CtxmuxCommandError(
      "internal",
      `expected correlated control response, received ${response.type}`,
      "unknown",
    );
  }
  try {
    return { run: response.run, receipt: decode(response.receipt) };
  } catch (error) {
    throw new CtxmuxCommandError("internal", asError(error).message, "unknown");
  }
}

export function decodeReceipt(
  kind: ControlReceipt["type"],
  receipt: ControlReceipt,
  inputBytes: number,
): ControlReceipt {
  switch (kind) {
    case "input":
      return decodeInputReceipt(receipt, inputBytes);
    case "resize":
      return decodeResizeReceipt(receipt);
    case "stop":
      return decodeStopReceipt(receipt);
  }
}

export function decodeInputReceipt(
  receipt: ControlReceipt,
  expectedBytes: number,
): InputReceipt {
  if (receipt.type !== "input") {
    throw invalidReceipt("input returned another receipt kind");
  }
  if (receipt.written_bytes !== expectedBytes) {
    throw invalidReceipt(
      "input receipt byte count differs from the command payload",
    );
  }
  return receipt;
}

export function decodeResizeReceipt(receipt: ControlReceipt): ResizeReceipt {
  if (receipt.type !== "resize") {
    throw invalidReceipt("resize returned another receipt kind");
  }
  if (receipt.applied_size.cols === 0 || receipt.applied_size.rows === 0) {
    throw invalidReceipt("resize receipt reported a zero applied dimension");
  }
  return receipt;
}

export function decodeStopReceipt(receipt: ControlReceipt): StopReceipt {
  if (receipt.type !== "stop") {
    throw invalidReceipt("stop returned another receipt kind");
  }
  return receipt;
}

function invalidReceipt(expected: string): CtxmuxInvalidFrameError {
  return new CtxmuxInvalidFrameError("$frame.response.receipt", expected);
}

export function commandError(
  failure: ControlFailure,
  commandId?: AttachmentCommandId,
): CtxmuxCommandError {
  return new CtxmuxCommandError(
    failure.error.code,
    failure.error.message,
    failure.disposition,
    commandId,
  );
}

export function protocolError(error: {
  readonly code: ErrorCode;
  readonly message: string;
}): CtxmuxProtocolError {
  return new CtxmuxProtocolError(error.code, error.message);
}

export function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}
