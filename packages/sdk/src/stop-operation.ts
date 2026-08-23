import { randomUUID } from "node:crypto";

import type { DaemonInstanceId } from "./generated/DaemonInstanceId.js";
import { MAX_STOP_OPERATION_KEY_BYTES } from "./generated/constants.js";
import type { RecoverableStop } from "./generated/RecoverableStop.js";
import type { RunId } from "./generated/RunId.js";
import type { StopOperationKey } from "./generated/StopOperationKey.js";

/** Caller-retained identity for one recoverable complete-session Stop. */
export interface RecoverableStopOperation {
  readonly daemonInstance: DaemonInstanceId;
  readonly operationKey: StopOperationKey;
  readonly runId: RunId;
}

/** Validate or generate one caller-retained native Stop operation key. */
export function stopOperationKey(
  value: string = randomUUID(),
): StopOperationKey {
  if (typeof value !== "string") {
    throw new TypeError("native Stop operation key must be a string");
  }
  if (!isWellFormedUtf16(value)) {
    throw new TypeError("native Stop operation key must be well-formed UTF-16");
  }
  const byteLength = new TextEncoder().encode(value).byteLength;
  if (byteLength === 0) {
    throw new TypeError("native Stop operation key must not be empty");
  }
  if (byteLength > MAX_STOP_OPERATION_KEY_BYTES) {
    throw new TypeError(
      `native Stop operation key is ${String(byteLength)} bytes; maximum is ${String(MAX_STOP_OPERATION_KEY_BYTES)}`,
    );
  }
  return value;
}

/** @internal Encode and validate one public Stop operation for the wire. */
export function encodeRecoverableStop(
  operation: RecoverableStopOperation,
): RecoverableStop {
  return {
    daemon_instance: operation.daemonInstance,
    operation_key: stopOperationKey(operation.operationKey),
    id: operation.runId,
  };
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
