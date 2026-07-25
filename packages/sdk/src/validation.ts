import type { ErrorCode } from "./generated/ErrorCode.js";
import type { ServerFrame } from "./generated/ServerFrame.js";

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
      attachedHeader(frame.snapshot, "$frame.snapshot");
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
    case "imported":
    case "forked":
    case "status":
    case "accepted":
      runInfo(valueRecord.run, `${path}.run`);
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
  validateCursorValue(run.head_seq, `${path}.head_seq`);
  if (run.durable_head_seq !== null) {
    validateCursorValue(run.durable_head_seq, `${path}.durable_head_seq`);
  }
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
  const stop = boolean(capabilities.stop, `${path}.stop`);
  const forkLevelA = boolean(capabilities.fork_level_a, `${path}.fork_level_a`);
  const forkLevelB = boolean(capabilities.fork_level_b, `${path}.fork_level_b`);
  const replay = string(capabilities.replay, `${path}.replay`);
  const native = backend === "native";
  if (
    input !== native ||
    resize !== native ||
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
