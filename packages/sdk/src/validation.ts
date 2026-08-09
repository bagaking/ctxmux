import type { ErrorCode } from "./generated/ErrorCode.js";
import type { ServerFrame } from "./generated/ServerFrame.js";

const ERROR_CODES: ReadonlySet<ErrorCode> = new Set([
  "version_mismatch",
  "invalid_request",
  "run_not_found",
  "invalid_run_state",
  "spawn_failed",
  "io",
  "internal",
]);

const CANONICAL_RUN_ID =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

/** A daemon frame failed the runtime half of the generation-2 wire contract. */
export class CtxmuxInvalidFrameError extends TypeError {
  public readonly path: string;

  public constructor(path: string, expected: string) {
    super(`invalid ctxmux daemon frame at ${path}: expected ${expected}`);
    this.name = "CtxmuxInvalidFrameError";
    this.path = path;
  }
}

/** Validate an untyped JSON value before exposing it as a generated wire type. */
export function validateServerFrame(value: unknown): ServerFrame {
  const frame = record(value, "$frame");
  switch (discriminant(frame, "$frame")) {
    case "hello":
      unsignedInteger(frame.protocol, "$frame.protocol", 0xffff);
      break;
    case "response":
      response(frame.response, "$frame.response");
      break;
    case "attached":
      attachedSnapshot(frame.snapshot, "$frame.snapshot");
      break;
    case "event":
      runEvent(frame.event, "$frame.event");
      break;
    case "detached":
      break;
    case "error":
      protocolError(frame.error, "$frame.error");
      break;
    default:
      throw invalid("$frame.type", "a known server-frame discriminant");
  }
  return value as ServerFrame;
}

/** Reject a generation-2 u64 before JavaScript can round a replay cursor. */
export function validateCursor(value: number, path: string): void {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw invalid(path, "a non-negative safe integer cursor");
  }
}

function response(value: unknown, path: string): void {
  const valueRecord = record(value, path);
  switch (discriminant(valueRecord, path)) {
    case "started":
    case "forked":
    case "status":
    case "accepted":
      runInfo(valueRecord.run, `${path}.run`);
      return;
    case "runs":
      array(valueRecord.runs, `${path}.runs`).forEach((run, index) =>
        runInfo(run, `${path}.runs[${index}]`),
      );
      return;
    default:
      throw invalid(`${path}.type`, "a known response discriminant");
  }
}

function attachedSnapshot(value: unknown, path: string): void {
  const snapshot = record(value, path);
  runInfo(snapshot.run, `${path}.run`);
  outputReplay(snapshot.replay, `${path}.replay`);
}

function runEvent(value: unknown, path: string): void {
  const event = record(value, path);
  switch (discriminant(event, path)) {
    case "output":
      outputChunk(event.chunk, `${path}.chunk`);
      return;
    case "exited":
      runState(event.state, `${path}.state`);
      return;
    case "gap":
      validateCursorValue(event.head_seq, `${path}.head_seq`);
      return;
    case "accepted":
      runInfo(event.run, `${path}.run`);
      return;
    default:
      throw invalid(`${path}.type`, "a known Run-event discriminant");
  }
}

function runInfo(value: unknown, path: string): void {
  const run = record(value, path);
  runId(run.id, `${path}.id`);
  runSpec(run.spec, `${path}.spec`);
  if (run.lineage !== null) {
    runLineage(run.lineage, `${path}.lineage`);
  }
  if (run.pid !== null) {
    unsignedInteger(run.pid, `${path}.pid`, 0xffff_ffff);
  }
  runState(run.state, `${path}.state`);
  validateCursorValue(run.head_seq, `${path}.head_seq`);
  validateCursorValue(run.oldest_seq, `${path}.oldest_seq`);
  safeUnsignedInteger(run.attachments, `${path}.attachments`);
}

function runSpec(value: unknown, path: string): void {
  const spec = record(value, path);
  string(spec.program, `${path}.program`);
  array(spec.args, `${path}.args`).forEach((argument, index) =>
    string(argument, `${path}.args[${index}]`),
  );
  if (spec.cwd !== null) {
    string(spec.cwd, `${path}.cwd`);
  }
  const environment = record(spec.env, `${path}.env`);
  for (const [name, environmentValue] of Object.entries(environment)) {
    string(environmentValue, `${path}.env.${name}`);
  }
  terminalSize(spec.size, `${path}.size`);
  array(spec.declared_inputs, `${path}.declared_inputs`).forEach(
    (input, index) =>
      runInputReference(input, `${path}.declared_inputs[${index}]`),
  );
}

function runInputReference(value: unknown, path: string): void {
  const input = record(value, path);
  const kind = string(input.kind, `${path}.kind`);
  if (kind !== "workspace" && kind !== "artifact" && kind !== "context") {
    throw invalid(`${path}.kind`, "a known Run input kind");
  }
  string(input.reference, `${path}.reference`);
}

function runLineage(value: unknown, path: string): void {
  const lineage = record(value, path);
  runId(lineage.parent, `${path}.parent`);
  const fidelity = string(lineage.fidelity, `${path}.fidelity`);
  if (fidelity !== "level_a" && fidelity !== "level_b") {
    throw invalid(`${path}.fidelity`, "a known fork fidelity");
  }
}

function runState(value: unknown, path: string): void {
  const state = record(value, path);
  switch (discriminant(state, path)) {
    case "running":
      return;
    case "exited":
      unsignedInteger(state.code, `${path}.code`, 0xffff_ffff);
      if (state.signal !== null) {
        string(state.signal, `${path}.signal`);
      }
      return;
    default:
      throw invalid(`${path}.type`, "a known Run-state discriminant");
  }
}

function outputReplay(value: unknown, path: string): void {
  const replay = record(value, path);
  array(replay.chunks, `${path}.chunks`).forEach((chunk, index) =>
    outputChunk(chunk, `${path}.chunks[${index}]`),
  );
  validateCursorValue(replay.oldest_seq, `${path}.oldest_seq`);
  validateCursorValue(replay.head_seq, `${path}.head_seq`);
  boolean(replay.truncated, `${path}.truncated`);
}

function outputChunk(value: unknown, path: string): void {
  const chunk = record(value, path);
  validateCursorValue(chunk.seq, `${path}.seq`);
  array(chunk.data, `${path}.data`).forEach((byte, index) =>
    unsignedInteger(byte, `${path}.data[${index}]`, 0xff),
  );
}

function terminalSize(value: unknown, path: string): void {
  const size = record(value, path);
  unsignedInteger(size.cols, `${path}.cols`, 0xffff);
  unsignedInteger(size.rows, `${path}.rows`, 0xffff);
}

function protocolError(value: unknown, path: string): void {
  const error = record(value, path);
  const code = string(error.code, `${path}.code`);
  if (!ERROR_CODES.has(code as ErrorCode)) {
    throw invalid(`${path}.code`, "a known protocol error code");
  }
  string(error.message, `${path}.message`);
}

function runId(value: unknown, path: string): void {
  const id = string(value, path);
  if (!CANONICAL_RUN_ID.test(id)) {
    throw invalid(path, "a canonical UUID Run id");
  }
}

function discriminant(value: Record<string, unknown>, path: string): string {
  return string(value.type, `${path}.type`);
}

function record(value: unknown, path: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw invalid(path, "an object");
  }
  return value as Record<string, unknown>;
}

function array(value: unknown, path: string): readonly unknown[] {
  if (!Array.isArray(value)) {
    throw invalid(path, "an array");
  }
  return value;
}

function string(value: unknown, path: string): string {
  if (typeof value !== "string") {
    throw invalid(path, "a string");
  }
  return value;
}

function boolean(value: unknown, path: string): void {
  if (typeof value !== "boolean") {
    throw invalid(path, "a boolean");
  }
}

function validateCursorValue(value: unknown, path: string): void {
  if (typeof value !== "number") {
    throw invalid(path, "a non-negative safe integer cursor");
  }
  validateCursor(value, path);
}

function safeUnsignedInteger(value: unknown, path: string): void {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    throw invalid(path, "a non-negative safe integer");
  }
}

function unsignedInteger(value: unknown, path: string, maximum: number): void {
  if (
    typeof value !== "number" ||
    !Number.isInteger(value) ||
    value < 0 ||
    value > maximum
  ) {
    throw invalid(path, `an integer from 0 through ${maximum}`);
  }
}

function invalid(path: string, expected: string): CtxmuxInvalidFrameError {
  return new CtxmuxInvalidFrameError(path, expected);
}
