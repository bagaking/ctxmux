import type { ErrorCode } from "./generated/ErrorCode.js";
import type { RunSpec } from "./generated/RunSpec.js";
import type { ServerFrame } from "./generated/ServerFrame.js";
import {
  MAX_RUNTIME_BUILD_ID_BYTES,
  MAX_RUNTIME_CAPABILITY_VERSION,
} from "./generated/constants.js";
import { isParsedJsonObject, jsonNumberSource } from "./wire.js";

const ERROR_CODES: ReadonlySet<ErrorCode> = new Set([
  "version_mismatch",
  "invalid_request",
  "run_not_found",
  "invalid_run_state",
  "spawn_failed",
  "io",
  "persistence",
  "backend_unavailable",
  "unsupported_backend_version",
  "unsupported_capability",
  "target_changed",
  "creation_conflict",
  "input_operation_conflict",
  "input_cursor_mismatch",
  "daemon_instance_mismatch",
  "run_capacity",
  "control_backpressure",
  "internal",
]);

const CANONICAL_RUN_ID =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

/** A daemon frame failed the runtime half of the generation-10 wire contract. */
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
      runtimeIdentity(frame.runtime, "$frame.runtime");
      break;
    case "response":
      response(frame.response, "$frame.response");
      break;
    case "attached":
      attachedHeader(frame.snapshot, "$frame.snapshot");
      break;
    case "event":
      runEvent(frame.event, "$frame.event");
      break;
    case "command_result":
      attachmentCommandId(frame.command_id, "$frame.command_id");
      controlOutcome(frame.outcome, "$frame.outcome");
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

function runtimeIdentity(value: unknown, path: string): void {
  const runtime = record(value, path);
  exactFields(runtime, path, [
    "daemonInstanceId",
    "runtimeId",
    "runtimeIdPersistence",
    "buildId",
    "protocolGeneration",
    "platform",
    "arch",
    "capabilities",
  ]);
  canonicalUuid(runtime.daemonInstanceId, `${path}.daemonInstanceId`);
  canonicalUuid(runtime.runtimeId, `${path}.runtimeId`);
  if (
    runtime.runtimeIdPersistence !== "daemon" &&
    runtime.runtimeIdPersistence !== "state_dir"
  ) {
    throw invalid(`${path}.runtimeIdPersistence`, '"daemon" or "state_dir"');
  }
  const buildId = string(runtime.buildId, `${path}.buildId`);
  const buildIdBytes = new TextEncoder().encode(buildId).byteLength;
  if (buildIdBytes === 0 || buildIdBytes > MAX_RUNTIME_BUILD_ID_BYTES) {
    throw invalid(
      `${path}.buildId`,
      `a non-empty UTF-8 string of at most ${String(MAX_RUNTIME_BUILD_ID_BYTES)} bytes`,
    );
  }
  unsignedInteger(
    runtime.protocolGeneration,
    `${path}.protocolGeneration`,
    0xffff,
  );
  nonEmptyString(runtime.platform, `${path}.platform`);
  nonEmptyString(runtime.arch, `${path}.arch`);
  runtimeCapabilities(runtime.capabilities, `${path}.capabilities`);
}

function runtimeCapabilities(value: unknown, path: string): void {
  const capabilities = record(value, path);
  const parsedFromWire = isParsedJsonObject(capabilities);
  for (const [key, version] of Object.entries(capabilities)) {
    const source = parsedFromWire
      ? jsonNumberSource(capabilities, key)
      : undefined;
    if (
      !isRuntimeCapabilityVersion(version) ||
      (parsedFromWire &&
        (source === undefined ||
          parseRuntimeCapabilityVersionSource(source) !== version))
    ) {
      throw invalid(`${path}.${key}`, "a positive safe integer version");
    }
  }
}

/** Validate and snapshot client-local Runtime capability requirements. */
export function copyRequiredRuntimeCapabilities(
  value: unknown,
): ReadonlyMap<string, number> {
  if (value === undefined) {
    return new Map();
  }
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new TypeError(
      "requiredCapabilities must be a string-to-number record",
    );
  }
  const result = new Map<string, number>();
  for (const [capability, version] of Object.entries(value)) {
    if (!isRuntimeCapabilityVersion(version)) {
      throw new TypeError(
        `requiredCapabilities.${capability} must be a positive safe integer version`,
      );
    }
    result.set(capability, version);
  }
  return result;
}

function isRuntimeCapabilityVersion(value: unknown): value is number {
  return (
    typeof value === "number" &&
    Number.isSafeInteger(value) &&
    value > 0 &&
    value <= MAX_RUNTIME_CAPABILITY_VERSION
  );
}

function parseRuntimeCapabilityVersionSource(
  source: string,
): number | undefined {
  const match = /^(0|[1-9]\d*)(?:\.(\d+))?(?:[eE]([+-]?)(\d+))?$/u.exec(source);
  if (match === null) {
    return undefined;
  }

  const integer = match[1] ?? "";
  const fraction = match[2] ?? "";
  const exponentDigits = match[4] ?? "0";
  let exponent = Number(exponentDigits);
  if (!Number.isSafeInteger(exponent)) {
    return undefined;
  }
  if (match[3] === "-") {
    exponent = -exponent;
  }

  let digits = `${integer}${fraction}`;
  if (!/[1-9]/u.test(digits)) {
    return undefined;
  }
  const decimalShift = exponent - fraction.length;
  if (decimalShift < 0) {
    const removed = -decimalShift;
    if (
      removed >= digits.length ||
      !/^0*$/u.test(digits.slice(digits.length - removed))
    ) {
      return undefined;
    }
    digits = digits.slice(0, digits.length - removed);
  } else {
    digits = digits.replace(/^0+/u, "");
    if (digits.length + decimalShift > 16) {
      return undefined;
    }
    digits += "0".repeat(decimalShift);
  }

  const normalized = digits.replace(/^0+/u, "");
  const version = Number(normalized);
  return isRuntimeCapabilityVersion(version) ? version : undefined;
}

function nonEmptyString(value: unknown, path: string): string {
  const result = string(value, path);
  if (result.length === 0) {
    throw invalid(path, "a non-empty string");
  }
  return result;
}

/** Reject a generation-10 u64 before JavaScript can round a replay cursor. */
export function validateCursor(value: number, path: string): void {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw invalid(path, "a non-negative safe integer cursor");
  }
}

/** Validate one complete executable Run specification before client mutation. */
export function validateRunSpec(value: unknown, path = "$runSpec"): RunSpec {
  runSpec(value, path);
  return value as RunSpec;
}

function response(value: unknown, path: string): void {
  const valueRecord = record(value, path);
  switch (discriminant(valueRecord, path)) {
    case "started":
    case "imported":
    case "forked":
    case "status":
      runInfo(valueRecord.run, `${path}.run`);
      return;
    case "control_accepted":
      runInfo(valueRecord.run, `${path}.run`);
      controlReceipt(valueRecord.receipt, `${path}.receipt`);
      return;
    case "control_rejected":
      controlFailure(valueRecord.failure, `${path}.failure`);
      return;
    case "input_applied":
      runInfo(valueRecord.run, `${path}.run`);
      appliedInputRange(valueRecord.range, `${path}.range`);
      return;
    case "tmux_panes":
      string(valueRecord.tmux_version, `${path}.tmux_version`);
      array(valueRecord.panes, `${path}.panes`).forEach((pane, index) =>
        tmuxPaneInfo(pane, `${path}.panes[${index}]`),
      );
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

function attachedHeader(value: unknown, path: string): void {
  const header = record(value, path);
  runInfo(header.run, `${path}.run`);
  outputReplayHeader(header.replay, `${path}.replay`);
}

function runEvent(value: unknown, path: string): void {
  const event = record(value, path);
  switch (discriminant(event, path)) {
    case "output":
      outputChunk(event.chunk, `${path}.chunk`);
      return;
    case "exited":
      if (runState(event.state, `${path}.state`) !== "exited") {
        throw invalid(`${path}.state.type`, '"exited"');
      }
      return;
    case "interrupted":
      interruptionReason(event.reason, `${path}.reason`);
      return;
    case "tmux":
      tmuxRunEvent(event.event, `${path}.event`);
      return;
    case "gap":
      validateCursorValue(
        event.latest_output_bytes,
        `${path}.latest_output_bytes`,
      );
      return;
    default:
      throw invalid(`${path}.type`, "a known Run-event discriminant");
  }
}

function runInfo(value: unknown, path: string): void {
  const run = record(value, path);
  runId(run.id, `${path}.id`);
  if (run.spec !== null) {
    runSpec(run.spec, `${path}.spec`);
  }
  if (run.lineage !== null) {
    runLineage(run.lineage, `${path}.lineage`);
  }
  if (run.pid !== null) {
    unsignedInteger(run.pid, `${path}.pid`, 0xffff_ffff);
  }
  const backend = runBackend(run.backend, `${path}.backend`);
  runCapabilities(run.capabilities, `${path}.capabilities`, backend);
  if (backend === "native" && run.spec === null) {
    throw invalid(`${path}.spec`, "a native Run specification");
  }
  if (backend === "tmux" && run.spec !== null) {
    throw invalid(`${path}.spec`, "null for an imported tmux pane");
  }
  runState(run.state, `${path}.state`);
  validateCursorValue(run.latest_output_bytes, `${path}.latest_output_bytes`);
  if (run.durable_output_bytes !== null) {
    validateCursorValue(
      run.durable_output_bytes,
      `${path}.durable_output_bytes`,
    );
  }
  validateCursorValue(run.first_available_byte, `${path}.first_available_byte`);
  safeUnsignedInteger(run.attachments, `${path}.attachments`);
  if (run.applied_input_bytes !== null) {
    validateCursorValue(run.applied_input_bytes, `${path}.applied_input_bytes`);
  }
  if (backend === "native" && run.applied_input_bytes === null) {
    const state = record(run.state, `${path}.state`);
    if (state.type !== "exited" && state.type !== "interrupted") {
      throw invalid(
        `${path}.applied_input_bytes`,
        "a cursor for a current-incarnation running native Run",
      );
    }
  }
  if (backend === "tmux" && run.applied_input_bytes !== null) {
    throw invalid(`${path}.applied_input_bytes`, "null for a tmux Run");
  }
}

function appliedInputRange(value: unknown, path: string): void {
  const range = record(value, path);
  validateCursorValue(range.start_byte, `${path}.start_byte`);
  validateCursorValue(range.end_byte, `${path}.end_byte`);
  if ((range.end_byte as number) <= (range.start_byte as number)) {
    throw invalid(`${path}.end_byte`, "greater than start_byte");
  }
}

function runSpec(value: unknown, path: string): void {
  const spec = record(value, path);
  if (string(spec.program, `${path}.program`).length === 0) {
    throw invalid(`${path}.program`, "a non-empty string");
  }
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
  terminalSize(spec.size, `${path}.size`, true);
  array(spec.declared_inputs, `${path}.declared_inputs`).forEach(
    (input, index) =>
      runInputReference(input, `${path}.declared_inputs[${index}]`),
  );
}

function runBackend(value: unknown, path: string): "native" | "tmux" {
  const backend = record(value, path);
  switch (discriminant(backend, path)) {
    case "native":
      return "native";
    case "tmux":
      string(backend.socket_path, `${path}.socket_path`);
      unsignedInteger(backend.server_pid, `${path}.server_pid`, 0xffff_ffff);
      validateCursorValue(
        backend.server_started_at,
        `${path}.server_started_at`,
      );
      tmuxId(backend.session_id, "$", `${path}.session_id`);
      tmuxId(backend.window_id, "@", `${path}.window_id`);
      tmuxId(backend.pane_id, "%", `${path}.pane_id`);
      string(backend.tmux_version, `${path}.tmux_version`);
      return "tmux";
    default:
      throw invalid(`${path}.type`, "a known Run backend");
  }
}

function runCapabilities(
  value: unknown,
  path: string,
  backend: "native" | "tmux",
): void {
  const capabilities = record(value, path);
  const input = boolean(capabilities.input, `${path}.input`);
  const resize = boolean(capabilities.resize, `${path}.resize`);
  const signal = boolean(capabilities.signal, `${path}.signal`);
  const stop = boolean(capabilities.stop, `${path}.stop`);
  const forkLevelA = boolean(capabilities.fork_level_a, `${path}.fork_level_a`);
  const forkLevelB = boolean(capabilities.fork_level_b, `${path}.fork_level_b`);
  const replay = string(capabilities.replay, `${path}.replay`);
  const native = backend === "native";
  if (
    input !== native ||
    resize !== native ||
    signal !== native ||
    stop !== native ||
    forkLevelA !== native ||
    forkLevelB !== native ||
    replay !== (native ? "raw_from_start" : "raw_since_import")
  ) {
    throw invalid(path, `capabilities for the ${backend} backend`);
  }
}

function tmuxPaneInfo(value: unknown, path: string): void {
  const pane = record(value, path);
  string(pane.socket_path, `${path}.socket_path`);
  string(pane.tmux_version, `${path}.tmux_version`);
  unsignedInteger(pane.server_pid, `${path}.server_pid`, 0xffff_ffff);
  validateCursorValue(pane.server_started_at, `${path}.server_started_at`);
  tmuxId(pane.session_id, "$", `${path}.session_id`);
  tmuxId(pane.window_id, "@", `${path}.window_id`);
  tmuxId(pane.pane_id, "%", `${path}.pane_id`);
  unsignedInteger(pane.pane_pid, `${path}.pane_pid`, 0xffff_ffff);
  terminalSize(pane.size, `${path}.size`);
}

function tmuxRunEvent(value: unknown, path: string): void {
  const event = record(value, path);
  switch (discriminant(event, path)) {
    case "session_renamed":
      array(event.name, `${path}.name`).forEach((byte, index) =>
        unsignedInteger(byte, `${path}.name[${index}]`, 0xff),
      );
      return;
    case "paused":
    case "continued":
      return;
    default:
      throw invalid(`${path}.type`, "a known tmux Run event");
  }
}

function runInputReference(value: unknown, path: string): void {
  const input = record(value, path);
  const kind = string(input.kind, `${path}.kind`);
  if (kind !== "workspace" && kind !== "artifact" && kind !== "context") {
    throw invalid(`${path}.kind`, "a known Run input kind");
  }
  if (string(input.reference, `${path}.reference`).length === 0) {
    throw invalid(`${path}.reference`, "a non-empty string");
  }
}

function runLineage(value: unknown, path: string): void {
  const lineage = record(value, path);
  runId(lineage.parent, `${path}.parent`);
  const fidelity = string(lineage.fidelity, `${path}.fidelity`);
  if (fidelity !== "level_a" && fidelity !== "level_b") {
    throw invalid(`${path}.fidelity`, "a known fork fidelity");
  }
}

function runState(
  value: unknown,
  path: string,
): "running" | "exited" | "interrupted" {
  const state = record(value, path);
  switch (discriminant(state, path)) {
    case "running":
      return "running";
    case "exited":
      unsignedInteger(state.code, `${path}.code`, 0xffff_ffff);
      if (state.signal !== null) {
        string(state.signal, `${path}.signal`);
      }
      return "exited";
    case "interrupted":
      interruptionReason(state.reason, `${path}.reason`);
      return "interrupted";
    default:
      throw invalid(`${path}.type`, "a known Run-state discriminant");
  }
}

function interruptionReason(value: unknown, path: string): void {
  const reason = string(value, path);
  if (
    reason !== "daemon_restart" &&
    reason !== "tmux_server_unavailable" &&
    reason !== "tmux_target_changed" &&
    reason !== "tmux_protocol_error"
  ) {
    throw invalid(path, "a known interruption reason");
  }
}

function outputReplayHeader(value: unknown, path: string): void {
  const replay = record(value, path);
  if ("chunks" in replay) {
    throw invalid(`${path}.chunks`, "absent from the metadata-only header");
  }
  validateCursorValue(
    replay.first_available_byte,
    `${path}.first_available_byte`,
  );
  validateCursorValue(
    replay.latest_output_bytes,
    `${path}.latest_output_bytes`,
  );
  boolean(replay.truncated, `${path}.truncated`);
}

function outputChunk(value: unknown, path: string): void {
  const chunk = record(value, path);
  validateCursorValue(chunk.start_byte, `${path}.start_byte`);
  validateCursorValue(chunk.end_byte, `${path}.end_byte`);
  const data = array(chunk.data, `${path}.data`);
  data.forEach((byte, index) =>
    unsignedInteger(byte, `${path}.data[${index}]`, 0xff),
  );
  if (
    (chunk.end_byte as number) <= (chunk.start_byte as number) ||
    (chunk.end_byte as number) - (chunk.start_byte as number) !== data.length
  ) {
    throw invalid(path, "a non-empty byte range matching data length");
  }
}

function attachmentCommandId(value: unknown, path: string): void {
  unsignedInteger(value, path, 0xffff_ffff, 1);
}

function controlOutcome(value: unknown, path: string): void {
  const outcome = record(value, path);
  switch (discriminant(outcome, path)) {
    case "accepted":
      controlReceipt(outcome.receipt, `${path}.receipt`);
      return;
    case "rejected":
      controlFailure(outcome.failure, `${path}.failure`);
      return;
    default:
      throw invalid(`${path}.type`, "a known control outcome");
  }
}

function controlReceipt(value: unknown, path: string): void {
  const receipt = record(value, path);
  switch (discriminant(receipt, path)) {
    case "input":
      unsignedInteger(
        receipt.written_bytes,
        `${path}.written_bytes`,
        0xffff_ffff,
      );
      return;
    case "resize":
      terminalSize(receipt.applied_size, `${path}.applied_size`, true);
      return;
    case "signal":
      if (string(receipt.signal, `${path}.signal`) !== "interrupt") {
        throw invalid(`${path}.signal`, '"interrupt"');
      }
      return;
    case "stop":
      if (
        string(receipt.disposition, `${path}.disposition`) !== "graceful" &&
        receipt.disposition !== "forced"
      ) {
        throw invalid(`${path}.disposition`, '"graceful" or "forced"');
      }
      return;
    default:
      throw invalid(`${path}.type`, "a known control receipt");
  }
}

function controlFailure(value: unknown, path: string): void {
  const failure = record(value, path);
  protocolError(failure.error, `${path}.error`);
  const disposition = string(failure.disposition, `${path}.disposition`);
  if (disposition !== "not_applied" && disposition !== "unknown") {
    throw invalid(`${path}.disposition`, "a known command disposition");
  }
  const error = record(failure.error, `${path}.error`);
  if (error.code === "control_backpressure" && disposition !== "not_applied") {
    throw invalid(
      `${path}.disposition`,
      '"not_applied" for control backpressure',
    );
  }
}

function terminalSize(value: unknown, path: string, nonzero = false): void {
  const size = record(value, path);
  const minimum = nonzero ? 1 : 0;
  unsignedInteger(size.cols, `${path}.cols`, 0xffff, minimum);
  unsignedInteger(size.rows, `${path}.rows`, 0xffff, minimum);
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
  canonicalUuid(value, path, "a canonical UUID Run id");
}

function canonicalUuid(
  value: unknown,
  path: string,
  expected = "a canonical UUID",
): void {
  const id = string(value, path);
  if (!CANONICAL_RUN_ID.test(id)) {
    throw invalid(path, expected);
  }
}

function tmuxId(value: unknown, prefix: "$" | "@" | "%", path: string): void {
  const id = string(value, path);
  if (!new RegExp(`^\\${prefix}[0-9]+$`).test(id)) {
    throw invalid(path, `a ${prefix}-prefixed tmux ID`);
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

function exactFields(
  value: Record<string, unknown>,
  path: string,
  expected: readonly string[],
): void {
  const allowed = new Set(expected);
  for (const field of Object.keys(value)) {
    if (!allowed.has(field)) {
      throw invalid(`${path}.${field}`, "a declared Runtime field");
    }
  }
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

function boolean(value: unknown, path: string): boolean {
  if (typeof value !== "boolean") {
    throw invalid(path, "a boolean");
  }
  return value;
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

function unsignedInteger(
  value: unknown,
  path: string,
  maximum: number,
  minimum = 0,
): void {
  if (
    typeof value !== "number" ||
    !Number.isInteger(value) ||
    value < minimum ||
    value > maximum
  ) {
    throw invalid(path, `an integer from ${minimum} through ${maximum}`);
  }
}

function invalid(path: string, expected: string): CtxmuxInvalidFrameError {
  return new CtxmuxInvalidFrameError(path, expected);
}
