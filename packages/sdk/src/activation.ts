import {
  spawn,
  type ChildProcess,
  type SpawnOptions,
} from "node:child_process";
import { lstat, unlink } from "node:fs/promises";
import type { Stats } from "node:fs";
import { createConnection, type Socket } from "node:net";

import {
  CtxmuxClient,
  type CtxmuxClientOptions,
  type RuntimeCapabilityRequirements,
} from "./client.js";
import { asError, CtxmuxProtocolError, protocolError } from "./control.js";
import {
  MAX_RUNTIME_BUILD_ID_BYTES,
  PROTOCOL_VERSION,
} from "./generated/constants.js";
import type { RuntimeIdentity } from "./generated/RuntimeIdentity.js";
import { CtxmuxInvalidFrameError, validateServerFrame } from "./validation.js";
import { JsonLinesConnection, parseJsonFrame } from "./wire.js";

/** Default budget for one local Runtime activation. */
export const DEFAULT_RUNTIME_ACTIVATION_TIMEOUT_MS = 30_000 as const;

/** Bounded grace used to reap only a child spawned by this helper. */
export const RUNTIME_ACTIVATION_SHUTDOWN_GRACE_MS = 500 as const;

/**
 * Bounded observation budget after forced termination.
 *
 * A launcher may deliberately ignore both graceful signals.  Once the owned
 * process group has been SIGKILLed, the remaining work is OS delivery and
 * Node's child reaping observation; that phase needs more scheduling slack
 * than either polite rung while remaining finite for callers.
 */
const RUNTIME_ACTIVATION_FORCE_REAP_GRACE_MS = 5_000 as const;

/**
 * Bounded observation budget for a winner that has not bound the socket path.
 *
 * Reached only after this call's own launcher has already failed, where the
 * sole remaining question is whether a third activator's bind lands in the same
 * instant. That is a syscall-scale race, not a startup-scale one, so it is
 * budgeted separately from the caller's deadline.
 */
const RUNTIME_ACTIVATION_ABSENT_WINNER_GRACE_MS = 500 as const;

const READY_SCHEMA = "ctxmux.daemon-ready.v1";
const MAX_READY_BYTES = 16 * 1024;
const READY_POLL_MS = 25;
const MAX_DIAGNOSTIC_BYTES = 16 * 1024;
const UUID =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/iu;

/**
 * The signal name a terminated launcher reports.
 *
 * Structurally `NodeJS.Signals`, restated here because this type reaches the
 * published surface through `CtxmuxActivationLaunchError`. The package declares
 * no dependencies, so a consumer typechecks it without `@types/node` installed;
 * naming the global directly would make every such consumer fail to compile.
 * Node's own union is assignable to `string`, so a caller that does have the
 * Node types keeps passing its values unchanged.
 */
export type RuntimeActivationSignal = string;

/** Environment values passed to a spawned daemon. */
export type RuntimeActivationEnvironment = Readonly<
  Record<string, string | undefined>
>;

/**
 * Child handling requested by an embedding caller. Both modes start an
 * OS-detached daemon; `inherit` only changes where launcher diagnostics go.
 */
export type RuntimeChildDisposition = "detached" | "inherit";

/** Explicit child lifecycle and stdio choices. */
export interface RuntimeChildDispositionOptions {
  readonly mode?: RuntimeChildDisposition;
  readonly stdout?: "ignore" | "inherit";
  readonly stderr?: "ignore" | "inherit" | "pipe";
}

/** Inputs for connecting to or activating one local ctxmux Runtime. */
export interface RuntimeActivationOptions {
  /** Explicit `ctxmuxd` executable path or command. */
  readonly executable: string;
  /** Explicit local Unix socket path. */
  readonly socketPath: string;
  /** Optional dedicated ctxmux persistent state directory. */
  readonly stateDir?: string;
  /** Spawn environment overlay; this object never mutates `process.env`. */
  readonly env?: RuntimeActivationEnvironment;
  /** Absolute epoch-millisecond deadline, Date, or small duration in ms. */
  readonly deadline?: number | Date;
  /** Duration convenience when no absolute deadline is available. */
  readonly timeoutMs?: number;
  /** Explicit detached child/stdout/stderr behavior. */
  readonly childDisposition?:
    RuntimeChildDisposition | RuntimeChildDispositionOptions;
  /** Exact Runtime identity required before selection. */
  readonly expectedRuntimeIdentity?: RuntimeIdentity;
  /** Exact opaque serving build label required before selection. */
  readonly expectedBuildId?: string;
  /** Minimum exact-key capability versions required before selection. */
  readonly requiredCapabilities?: RuntimeCapabilityRequirements;
}

/** Machine-readable activation failure categories. */
export type RuntimeActivationErrorCode =
  | "unsafe_target"
  | "permission_denied"
  | "runtime_conflict"
  | "spawn_failed"
  | "launcher_exited"
  | "readiness_timeout"
  | "readiness_invalid"
  | "readiness_closed"
  | "readiness_mismatch"
  | "cleanup_failed"
  | "unsupported_platform";

/** Why a socket target was not safe to activate. */
export type RuntimeActivationTargetKind =
  | "ordinary_file"
  | "directory"
  | "symlink"
  | "permission_denied"
  | "target_changed";

/** Why a reachable endpoint could not be selected. */
export type RuntimeActivationConflictReason =
  | "identity_mismatch"
  | "build_mismatch"
  | "capability_mismatch"
  | "protocol_mismatch"
  | "probe_timeout";

/** Base error for one local Runtime activation transaction. */
export class CtxmuxActivationError extends Error {
  public readonly code: RuntimeActivationErrorCode;
  public readonly socketPath: string;

  public constructor(
    code: RuntimeActivationErrorCode,
    message: string,
    socketPath: string,
    cause?: unknown,
  ) {
    super(message);
    this.name = "CtxmuxActivationError";
    this.code = code;
    this.socketPath = socketPath;
    if (cause !== undefined) this.cause = cause;
  }
}

/** An ordinary file, symlink, permission boundary, or target race failed closed. */
export class CtxmuxActivationTargetError extends CtxmuxActivationError {
  public readonly targetKind: RuntimeActivationTargetKind;

  public constructor(
    socketPath: string,
    targetKind: RuntimeActivationTargetKind,
    detail?: string,
    cause?: unknown,
  ) {
    super(
      targetKind === "permission_denied"
        ? "permission_denied"
        : "unsafe_target",
      `cannot activate ${socketPath}: ${targetKind}${
        detail === undefined ? "" : ` (${detail})`
      }`,
      socketPath,
      cause,
    );
    this.name = "CtxmuxActivationTargetError";
    this.targetKind = targetKind;
  }
}

/** A live endpoint does not satisfy the requested Runtime contract. */
export class CtxmuxActivationConflictError extends CtxmuxActivationError {
  public readonly reason: RuntimeActivationConflictReason;
  public readonly actual: RuntimeIdentity | undefined;
  public readonly expected: RuntimeIdentity | undefined;
  public readonly capability: string | undefined;
  public readonly requiredVersion: number | undefined;
  public readonly advertisedVersion: number | undefined;

  public constructor(
    socketPath: string,
    reason: RuntimeActivationConflictReason,
    detail: string,
    values: {
      readonly actual?: RuntimeIdentity | undefined;
      readonly expected?: RuntimeIdentity | undefined;
      readonly capability?: string | undefined;
      readonly requiredVersion?: number | undefined;
      readonly advertisedVersion?: number | undefined;
      readonly cause?: unknown;
    } = {},
  ) {
    super(
      "runtime_conflict",
      `cannot activate ${socketPath}: ${detail}`,
      socketPath,
      values.cause,
    );
    this.name = "CtxmuxActivationConflictError";
    this.reason = reason;
    this.actual =
      values.actual === undefined
        ? undefined
        : copyRuntimeIdentity(values.actual);
    this.expected =
      values.expected === undefined
        ? undefined
        : copyRuntimeIdentity(values.expected);
    this.capability = values.capability;
    this.requiredVersion = values.requiredVersion;
    this.advertisedVersion = values.advertisedVersion;
  }
}

/** The selected executable could not start or exited before readiness. */
export class CtxmuxActivationLaunchError extends CtxmuxActivationError {
  public readonly executable: string;
  public readonly exitCode: number | null | undefined;
  public readonly signal: RuntimeActivationSignal | null | undefined;
  public readonly stderr: string | undefined;

  public constructor(
    code: "spawn_failed" | "launcher_exited",
    executable: string,
    socketPath: string,
    detail: string,
    values: {
      readonly exitCode?: number | null | undefined;
      readonly signal?: RuntimeActivationSignal | null | undefined;
      readonly stderr?: string | undefined;
      readonly cause?: unknown;
    } = {},
  ) {
    super(code, `${executable}: ${detail}`, socketPath, values.cause);
    this.name = "CtxmuxActivationLaunchError";
    this.executable = executable;
    this.exitCode = values.exitCode;
    this.signal = values.signal;
    this.stderr = values.stderr;
  }
}

/** Readiness was absent, malformed, or disagreed with public Hello. */
export class CtxmuxActivationReadinessError extends CtxmuxActivationError {
  public readonly readinessKind: "timeout" | "invalid" | "mismatch" | "closed";
  public readonly readinessInstance: string | undefined;
  public readonly runtimeInstance: string | undefined;

  public constructor(
    socketPath: string,
    readinessKind: "timeout" | "invalid" | "mismatch" | "closed",
    detail: string,
    values: {
      readonly readinessInstance?: string | undefined;
      readonly runtimeInstance?: string | undefined;
      readonly cause?: unknown;
    } = {},
  ) {
    super(
      readinessKind === "timeout"
        ? "readiness_timeout"
        : readinessKind === "mismatch"
          ? "readiness_mismatch"
          : readinessKind === "closed"
            ? "readiness_closed"
            : "readiness_invalid",
      `cannot activate ${socketPath}: ${detail}`,
      socketPath,
      values.cause,
    );
    this.name = "CtxmuxActivationReadinessError";
    this.readinessKind = readinessKind;
    this.readinessInstance = values.readinessInstance;
    this.runtimeInstance = values.runtimeInstance;
  }
}

/** A reused daemon cannot be terminated through an activation handle. */
export class CtxmuxActivationOwnershipError extends CtxmuxActivationError {
  public constructor(socketPath: string) {
    super(
      "runtime_conflict",
      `cannot shut down ${socketPath}: this activation did not spawn that daemon`,
      socketPath,
    );
    this.name = "CtxmuxActivationOwnershipError";
  }
}

/** Options for releasing one activation handle. */
export interface RuntimeActivationDisposeOptions {
  /** Explicitly terminate only the child proven to be spawned by this handle. */
  readonly shutdown?: boolean;
}

interface Deadline {
  readonly expiresAt: number;
  remaining(): number;
}

interface SocketIdentity {
  readonly device: number;
  readonly inode: number;
}

interface NormalizedOptions {
  readonly executable: string;
  readonly socketPath: string;
  readonly stateDir: string | undefined;
  readonly env: NodeJS.ProcessEnv | undefined;
  readonly deadline: Deadline;
  readonly child: Required<RuntimeChildDispositionOptions>;
  readonly expectedRuntimeIdentity: RuntimeIdentity | undefined;
  readonly expectedBuildId: string | undefined;
  readonly requiredCapabilities: RuntimeCapabilityRequirements;
}

interface LiveTarget {
  readonly kind: "live";
  readonly runtime: RuntimeIdentity;
  readonly socketIdentity: SocketIdentity;
}

interface MissingTarget {
  readonly kind: "missing";
}

interface StaleTarget {
  readonly kind: "stale";
  readonly socketIdentity: SocketIdentity;
  /**
   * Whether the probe was refused outright rather than accepted and then cut
   * short. A refusal (`ECONNREFUSED`, `ENOENT`, `ENXIO`) means nothing is
   * listening on the path, which is evidence *against* a concurrent winner. A
   * connection that was accepted and then dropped (`ECONNRESET`, `EPIPE`,
   * `ENOTCONN`) means something is bound but has not finished its handshake,
   * which is evidence *for* one. Both land on `stale`, so the distinction has
   * to travel with it.
   */
  readonly refused: boolean;
}

type Target = LiveTarget | MissingTarget | StaleTarget;

interface ReadyRecord {
  readonly daemonInstance: string;
}

interface ChildExit {
  readonly code: number | null;
  readonly signal: NodeJS.Signals | null;
}

interface DiagnosticCapture {
  read(): string | undefined;
  unref(): void;
}

class ProbeTimeoutError extends Error {
  public constructor(message: string) {
    super(message);
    this.name = "ProbeTimeoutError";
  }
}

class ProbeUnavailableError extends Error {
  public constructor(message: string, cause?: unknown) {
    super(message);
    this.name = "ProbeUnavailableError";
    if (cause !== undefined) this.cause = cause;
  }
}

class ChildSpawnError extends Error {
  public constructor(message: string, cause?: unknown) {
    super(message);
    this.name = "ChildSpawnError";
    if (cause !== undefined) this.cause = cause;
  }
}

class ChildExitedBeforeReadyError extends Error {
  public readonly exit: ChildExit;

  public constructor(exit: ChildExit) {
    super("daemon child exited before readiness");
    this.name = "ChildExitedBeforeReadyError";
    this.exit = exit;
  }
}

type RuntimeActivationInternals = {
  readonly client: CtxmuxClient;
  readonly runtime: RuntimeIdentity;
  readonly socketPath: string;
  readonly spawned: boolean;
  readonly child?: ChildProcess | undefined;
  readonly socketIdentity?: SocketIdentity | undefined;
};

/**
 * Module-private constructor for {@link RuntimeActivation}, assigned by that
 * class's static block. `activateRuntime` is the only producer of an
 * activation, so the constructor is private, which also keeps `ChildProcess`
 * out of the emitted declaration: this package declares no dependencies, and a
 * consumer typechecks its `.d.ts` without `@types/node` installed.
 */
let createRuntimeActivation: (
  values: RuntimeActivationInternals,
) => RuntimeActivation;

/** A selected Runtime and a normal identity-fenced SDK client. */
export class RuntimeActivation {
  public readonly client: CtxmuxClient;
  public readonly runtime: RuntimeIdentity;
  public readonly socketPath: string;
  public readonly spawned: boolean;
  public readonly childPid: number | undefined;

  readonly #child: ChildProcess | undefined;
  readonly #socketIdentity: SocketIdentity | undefined;
  #shutdownPromise: Promise<void> | undefined;
  #disposed = false;

  private constructor(values: RuntimeActivationInternals) {
    this.client = values.client;
    this.runtime = copyRuntimeIdentity(values.runtime);
    this.socketPath = values.socketPath;
    this.spawned = values.spawned;
    this.childPid = values.child?.pid;
    this.#child = values.child;
    this.#socketIdentity = values.socketIdentity;
  }

  static {
    createRuntimeActivation = (values) => new RuntimeActivation(values);
  }

  /** Client-only disposal leaves the daemon and its Runs running by default. */
  public async dispose(
    options: RuntimeActivationDisposeOptions = {},
  ): Promise<void> {
    this.#disposed = true;
    if (options.shutdown === true) await this.shutdown();
  }

  /** Explicitly terminate and reap only the daemon child this handle spawned. */
  public async shutdown(): Promise<void> {
    if (!this.spawned || this.#child === undefined) {
      throw new CtxmuxActivationOwnershipError(this.socketPath);
    }
    this.#shutdownPromise ??= terminateOwnedChild(
      this.#child,
      this.socketPath,
      this.#socketIdentity,
    );
    await this.#shutdownPromise;
  }

  public get disposed(): boolean {
    return this.#disposed;
  }
}

/**
 * Connect to a compatible local Runtime or activate `ctxmuxd` and require the
 * spawned child's readiness record to agree with ordinary public Hello.
 */
export async function activateRuntime(
  options: RuntimeActivationOptions,
): Promise<RuntimeActivation> {
  const normalized = normalizeOptions(options);
  if (process.platform === "win32") {
    throw new CtxmuxActivationError(
      "unsupported_platform",
      "local Runtime activation requires Unix sockets",
      normalized.socketPath,
    );
  }

  const first = await inspectTarget(normalized.socketPath, normalized.deadline);
  if (first.kind === "live") {
    verifyCompatibility(first.runtime, normalized);
    return activationFor(
      normalized,
      first.runtime,
      false,
      undefined,
      first.socketIdentity,
    );
  }
  if (normalized.deadline.remaining() <= 0) {
    throw readinessTimeout(
      normalized.socketPath,
      "deadline elapsed before launch",
    );
  }

  let child: ChildProcess;
  try {
    child = spawnDaemon(normalized);
  } catch (error) {
    throw new CtxmuxActivationLaunchError(
      "spawn_failed",
      normalized.executable,
      normalized.socketPath,
      `failed to start: ${asError(error).message}`,
      { cause: error },
    );
  }
  const diagnostics = captureDiagnostics(child, normalized.child.stderr);
  let readyWasPublished = false;
  let ownedSocketIdentity: SocketIdentity | undefined;
  try {
    const readinessStream = child.stdio[3];
    if (readinessStream === null || readinessStream === undefined) {
      throw new CtxmuxActivationReadinessError(
        normalized.socketPath,
        "invalid",
        "daemon launcher did not expose readiness fd 3",
      );
    }
    const ready = await readReadyRecord(
      child,
      readinessStream,
      normalized.socketPath,
      normalized.deadline,
    );
    readyWasPublished = true;
    const runtime = await awaitMatchingHello(
      child,
      normalized.socketPath,
      ready,
      normalized.deadline,
    );
    ownedSocketIdentity = await currentSocketIdentity(normalized.socketPath);
    if (ownedSocketIdentity === undefined) {
      throw new CtxmuxActivationReadinessError(
        normalized.socketPath,
        "mismatch",
        "matching Hello succeeded but the socket target changed",
        {
          readinessInstance: ready.daemonInstance,
          runtimeInstance: runtime.daemonInstanceId,
        },
      );
    }
    verifyCompatibility(runtime, normalized);
    detachChild(child, readinessStream, diagnostics);
    return activationFor(normalized, runtime, true, child, ownedSocketIdentity);
  } catch (error) {
    // A concurrent activator can win after this call's initial probe but before
    // its child binds. A fresh public Hello is the only condition that converts
    // that loss into reuse; no path is unlinked here.
    let concurrentError: unknown;
    if (!readyWasPublished && isEarlyLauncherFailure(error)) {
      let winner: LiveTarget | undefined;
      try {
        winner = await probeConcurrentWinner(normalized);
      } catch (winnerFailure) {
        // Preserve a typed conflict/target decision, but still reap the child
        // this transaction spawned before returning it to the caller.
        concurrentError = winnerFailure;
      }
      if (winner !== undefined) {
        await terminateOwnedChild(
          child,
          normalized.socketPath,
          ownedSocketIdentity,
        );
        return activationFor(
          normalized,
          winner.runtime,
          false,
          undefined,
          winner.socketIdentity,
        );
      }
    }

    const failure = activationFailure(
      concurrentError ?? error,
      normalized,
      diagnostics.read(),
    );
    try {
      await terminateOwnedChild(
        child,
        normalized.socketPath,
        ownedSocketIdentity,
      );
    } catch (cleanupError) {
      if (failure.cause === undefined) failure.cause = cleanupError;
    }
    throw failure;
  }
}

/** Same operation named for its connect-or-activate transaction. */
export const connectOrActivate = activateRuntime;

function normalizeOptions(
  options: RuntimeActivationOptions,
): NormalizedOptions {
  if (options === null || typeof options !== "object") {
    throw new TypeError("Runtime activation options must be an object");
  }
  if (
    typeof options.executable !== "string" ||
    options.executable.length === 0
  ) {
    throw new TypeError("activation executable must be a non-empty string");
  }
  if (
    typeof options.socketPath !== "string" ||
    options.socketPath.length === 0
  ) {
    throw new TypeError("activation socketPath must be a non-empty string");
  }
  if (
    options.stateDir !== undefined &&
    (typeof options.stateDir !== "string" || options.stateDir.length === 0)
  ) {
    throw new TypeError("activation stateDir must be a non-empty string");
  }
  if (options.expectedBuildId !== undefined) {
    if (
      typeof options.expectedBuildId !== "string" ||
      options.expectedBuildId.length === 0
    ) {
      throw new TypeError(
        "activation expectedBuildId must be a non-empty string",
      );
    }
    const bytes = new TextEncoder().encode(options.expectedBuildId).byteLength;
    if (bytes > MAX_RUNTIME_BUILD_ID_BYTES) {
      throw new TypeError(
        `activation expectedBuildId is ${String(bytes)} bytes; maximum is ${String(MAX_RUNTIME_BUILD_ID_BYTES)}`,
      );
    }
  }

  // Constructing a client validates expected identity/capability records before
  // any filesystem or process mutation. Raw probes below intentionally bypass
  // those local requirements until the actual Runtime can be reported.
  const validationOptions: CtxmuxClientOptions = {
    socketPath: options.socketPath,
    ...(options.expectedRuntimeIdentity === undefined
      ? {}
      : { expectedRuntimeIdentity: options.expectedRuntimeIdentity }),
    ...(options.requiredCapabilities === undefined
      ? {}
      : { requiredCapabilities: options.requiredCapabilities }),
  };
  new CtxmuxClient(validationOptions);

  return {
    executable: options.executable,
    socketPath: options.socketPath,
    stateDir: options.stateDir,
    env: normalizeEnvironment(options.env),
    deadline: normalizeDeadline(options.deadline, options.timeoutMs),
    child: normalizeChildDisposition(options.childDisposition),
    expectedRuntimeIdentity:
      options.expectedRuntimeIdentity === undefined
        ? undefined
        : copyRuntimeIdentity(options.expectedRuntimeIdentity),
    expectedBuildId: options.expectedBuildId,
    requiredCapabilities: { ...(options.requiredCapabilities ?? {}) },
  };
}

function normalizeEnvironment(
  environment: RuntimeActivationEnvironment | undefined,
): NodeJS.ProcessEnv | undefined {
  if (environment === undefined) return undefined;
  if (
    typeof environment !== "object" ||
    environment === null ||
    Array.isArray(environment)
  ) {
    throw new TypeError("activation env must be a string-to-string record");
  }
  const result: NodeJS.ProcessEnv = { ...process.env };
  for (const [key, value] of Object.entries(environment)) {
    if (typeof value !== "string" && value !== undefined) {
      throw new TypeError(
        `activation env.${key} must be a string or undefined`,
      );
    }
    if (value === undefined) delete result[key];
    else result[key] = value;
  }
  return result;
}

function normalizeDeadline(
  deadline: number | Date | undefined,
  timeoutMs: number | undefined,
): Deadline {
  if (deadline !== undefined && timeoutMs !== undefined) {
    throw new TypeError(
      "activation deadline and timeoutMs are mutually exclusive",
    );
  }
  let durationMs: number;
  if (deadline instanceof Date) {
    durationMs = deadline.getTime() - Date.now();
  } else if (deadline !== undefined) {
    if (!Number.isFinite(deadline) || deadline < 0) {
      throw new TypeError(
        "activation deadline must be a non-negative finite number",
      );
    }
    // Epoch milliseconds are currently above 10^12; smaller values are useful
    // explicit duration budgets in tests and small embedding utilities.
    durationMs = deadline > 1e12 ? deadline - Date.now() : deadline;
  } else if (timeoutMs !== undefined) {
    durationMs = timeoutMs;
  } else {
    durationMs = DEFAULT_RUNTIME_ACTIVATION_TIMEOUT_MS;
  }
  if (!Number.isFinite(durationMs) || durationMs < 0) {
    throw new TypeError(
      "activation deadline must resolve to a non-negative duration",
    );
  }
  return deadlineAfter(durationMs);
}

function normalizeChildDisposition(
  disposition: RuntimeActivationOptions["childDisposition"],
): Required<RuntimeChildDispositionOptions> {
  if (disposition === undefined || disposition === "detached") {
    return { mode: "detached", stdout: "ignore", stderr: "pipe" };
  }
  if (disposition === "inherit") {
    return { mode: "inherit", stdout: "inherit", stderr: "inherit" };
  }
  if (
    typeof disposition !== "object" ||
    disposition === null ||
    Array.isArray(disposition)
  ) {
    throw new TypeError(
      "activation childDisposition must be detached, inherit, or an object",
    );
  }
  const mode = disposition.mode ?? "detached";
  const stdout =
    disposition.stdout ?? (mode === "inherit" ? "inherit" : "ignore");
  const stderr =
    disposition.stderr ?? (mode === "inherit" ? "inherit" : "pipe");
  if (mode !== "detached" && mode !== "inherit") {
    throw new TypeError(
      "activation childDisposition.mode must be detached or inherit",
    );
  }
  if (stdout !== "ignore" && stdout !== "inherit") {
    throw new TypeError("activation child stdout must be ignore or inherit");
  }
  if (stderr !== "ignore" && stderr !== "inherit" && stderr !== "pipe") {
    throw new TypeError(
      "activation child stderr must be ignore, inherit, or pipe",
    );
  }
  return { mode, stdout, stderr };
}

async function inspectTarget(
  socketPath: string,
  deadline: Deadline,
): Promise<Target> {
  const first = await targetMetadata(socketPath);
  if (first === undefined) return { kind: "missing" };
  requireSocketTarget(socketPath, first);
  const identity = socketIdentity(first);

  let runtime: RuntimeIdentity;
  try {
    runtime = await probeRuntime(socketPath, deadline);
  } catch (error) {
    if (error instanceof CtxmuxActivationTargetError) throw error;
    if (!isUnavailableProbe(error)) throw probeConflict(socketPath, error);
    const second = await targetMetadata(socketPath);
    if (second === undefined) return { kind: "missing" };
    requireSocketTarget(socketPath, second);
    if (!sameIdentity(identity, socketIdentity(second))) {
      throw new CtxmuxActivationTargetError(
        socketPath,
        "target_changed",
        "socket inode changed during the inactive probe",
      );
    }
    return {
      kind: "stale",
      socketIdentity: identity,
      refused: isRefusedProbe(error),
    };
  }

  const second = await targetMetadata(socketPath);
  if (
    second === undefined ||
    !second.isSocket() ||
    !sameIdentity(identity, socketIdentity(second))
  ) {
    throw new CtxmuxActivationTargetError(
      socketPath,
      "target_changed",
      "socket inode changed after Hello",
    );
  }
  return { kind: "live", runtime, socketIdentity: identity };
}

async function targetMetadata(socketPath: string): Promise<Stats | undefined> {
  try {
    return await lstat(socketPath);
  } catch (error) {
    const code = nodeErrorCode(error);
    if (code === "ENOENT" || code === "ENOTDIR") return undefined;
    if (code === "EACCES" || code === "EPERM") {
      throw new CtxmuxActivationTargetError(
        socketPath,
        "permission_denied",
        asError(error).message,
        error,
      );
    }
    throw new CtxmuxActivationTargetError(
      socketPath,
      "target_changed",
      asError(error).message,
      error,
    );
  }
}

function requireSocketTarget(socketPath: string, metadata: Stats): void {
  if (metadata.isSymbolicLink()) {
    throw new CtxmuxActivationTargetError(
      socketPath,
      "symlink",
      "symlinks are never followed",
    );
  }
  if (metadata.isDirectory()) {
    throw new CtxmuxActivationTargetError(socketPath, "directory");
  }
  if (!metadata.isSocket()) {
    throw new CtxmuxActivationTargetError(
      socketPath,
      "ordinary_file",
      "target is not a Unix socket",
    );
  }
}

function probeConflict(
  socketPath: string,
  error: unknown,
): CtxmuxActivationError {
  if (error instanceof ProbeTimeoutError) {
    return new CtxmuxActivationConflictError(
      socketPath,
      "probe_timeout",
      "socket did not prove a compatible Hello before the deadline",
      { cause: error },
    );
  }
  if (
    error instanceof CtxmuxProtocolError ||
    error instanceof CtxmuxInvalidFrameError
  ) {
    return new CtxmuxActivationConflictError(
      socketPath,
      "protocol_mismatch",
      `reachable endpoint is incompatible (${error.message})`,
      { cause: error },
    );
  }
  return new CtxmuxActivationConflictError(
    socketPath,
    "protocol_mismatch",
    `reachable endpoint did not provide a compatible Runtime (${asError(error).message})`,
    { cause: error },
  );
}

async function probeRuntime(
  socketPath: string,
  deadline: Deadline,
): Promise<RuntimeIdentity> {
  const remaining = deadline.remaining();
  if (remaining <= 0)
    throw new ProbeTimeoutError("Runtime probe deadline elapsed");

  let connection: {
    readonly socket: Socket;
    readonly wire: JsonLinesConnection;
  };
  try {
    connection = await connectProbe(socketPath, remaining);
  } catch (error) {
    if (error instanceof ProbeTimeoutError) throw error;
    if (connectionPermissionCode(nodeErrorCode(error))) {
      throw new CtxmuxActivationTargetError(
        socketPath,
        "permission_denied",
        asError(error).message,
        error,
      );
    }
    if (connectionUnavailableCode(nodeErrorCode(error))) {
      throw new ProbeUnavailableError(asError(error).message, error);
    }
    throw error;
  }

  const { socket, wire } = connection;
  let timedOut = false;
  const timer = setTimeout(() => {
    timedOut = true;
    wire.close();
  }, timerMs(deadline.remaining()));
  try {
    await wire.send({ type: "hello", hello: { protocol: PROTOCOL_VERSION } });
    const frame = validateServerFrame(await wire.receive());
    if (frame.type === "error") throw protocolError(frame.error);
    if (
      frame.type !== "hello" ||
      frame.runtime.protocolGeneration !== PROTOCOL_VERSION
    ) {
      throw new CtxmuxProtocolError(
        "version_mismatch",
        `expected protocol generation ${String(PROTOCOL_VERSION)} Hello`,
      );
    }
    return copyRuntimeIdentity(frame.runtime);
  } catch (error) {
    if (timedOut) throw new ProbeTimeoutError("Runtime Hello deadline elapsed");
    throw error;
  } finally {
    clearTimeout(timer);
    socket.setTimeout(0);
    wire.close();
  }
}

async function connectProbe(
  socketPath: string,
  timeoutMs: number,
): Promise<{ readonly socket: Socket; readonly wire: JsonLinesConnection }> {
  const socket = createConnection({ path: socketPath });
  return await new Promise((resolve, reject) => {
    let settled = false;
    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      socket.destroy();
      reject(new ProbeTimeoutError("Runtime socket connect deadline elapsed"));
    }, timerMs(timeoutMs));
    const onError = (error: Error): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      reject(error);
    };
    socket.once("error", onError);
    socket.once("connect", () => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      socket.off("error", onError);
      resolve({ socket, wire: new JsonLinesConnection(socket) });
    });
  });
}

function spawnDaemon(options: NormalizedOptions): ChildProcess {
  const args = ["--socket", options.socketPath];
  if (options.stateDir !== undefined)
    args.push("--state-dir", options.stateDir);
  args.push("--readiness-fd", "3");
  const stdio: SpawnOptions["stdio"] = [
    "ignore",
    options.child.stdout,
    options.child.stderr,
    "pipe",
  ];
  try {
    return spawn(options.executable, args, {
      detached: true,
      env: options.env,
      stdio,
      windowsHide: true,
    });
  } catch (error) {
    throw new ChildSpawnError(asError(error).message, error);
  }
}

function captureDiagnostics(
  child: ChildProcess,
  mode: Required<RuntimeChildDispositionOptions>["stderr"],
): DiagnosticCapture {
  if (mode !== "pipe" || child.stderr === null) {
    return { read: () => undefined, unref: () => {} };
  }
  const stream = child.stderr;
  let content = "";
  let truncated = false;
  stream.setEncoding("utf8");
  stream.on("data", (chunk: string) => {
    const room = MAX_DIAGNOSTIC_BYTES - Buffer.byteLength(content);
    if (room <= 0) {
      truncated = true;
      return;
    }
    const bytes = Buffer.from(chunk);
    content += bytes.subarray(0, room).toString("utf8");
    if (bytes.length > room) truncated = true;
  });
  stream.resume();
  return {
    read: () =>
      content.length === 0 && !truncated
        ? undefined
        : `${content}${truncated ? "\n[stderr truncated]" : ""}`,
    unref: () => (stream as typeof stream & { unref?: () => void }).unref?.(),
  };
}

async function readReadyRecord(
  child: ChildProcess,
  stream: NonNullable<ChildProcess["stdio"][number]>,
  socketPath: string,
  deadline: Deadline,
): Promise<ReadyRecord> {
  const remaining = deadline.remaining();
  if (remaining <= 0) throw readinessTimeout(socketPath, "deadline elapsed");

  return await new Promise((resolve, reject) => {
    let content = Buffer.alloc(0);
    let settled = false;
    let readinessClosed = false;
    const timer = setTimeout(
      () =>
        finish(
          readinessTimeout(socketPath, "daemon did not publish readiness"),
        ),
      timerMs(remaining),
    );
    const cleanup = (): void => {
      clearTimeout(timer);
      stream.off("data", onData);
      stream.off("end", onClosed);
      stream.off("close", onClosed);
      stream.off("error", onStreamError);
      child.off("error", onChildError);
      child.off("exit", onExit);
      child.off("close", onChildClose);
    };
    const finish = (value: ReadyRecord | Error): void => {
      if (settled) return;
      settled = true;
      cleanup();
      if (value instanceof Error) reject(value);
      else resolve(value);
    };
    const onChildError = (error: Error): void =>
      finish(new ChildSpawnError(error.message, error));
    const onExit = (code: number | null, signal: NodeJS.Signals | null): void =>
      finish(new ChildExitedBeforeReadyError({ code, signal }));
    const onChildClose = (): void => {
      if (!readinessClosed || settled) return;
      const exit = childExit(child);
      finish(
        exit === undefined
          ? new CtxmuxActivationReadinessError(
              socketPath,
              "closed",
              "readiness channel closed before publication",
            )
          : new ChildExitedBeforeReadyError(exit),
      );
    };
    const onClosed = (): void => {
      if (settled) return;
      const exit = childExit(child);
      if (exit !== undefined) {
        finish(new ChildExitedBeforeReadyError(exit));
        return;
      }
      // Node may close an individual stdio stream before it publishes the
      // child's exit status. Wait for the ChildProcess `close` observation so a
      // genuinely crashed launcher is reported as a launch failure rather than
      // an ambiguous readiness close. A live child that intentionally closes
      // fd 3 remains bounded by the outer readiness deadline.
      readinessClosed = true;
      const laterExit = childExit(child);
      if (laterExit !== undefined) {
        finish(new ChildExitedBeforeReadyError(laterExit));
      }
    };
    const onStreamError = (error: Error): void =>
      finish(
        new CtxmuxActivationReadinessError(
          socketPath,
          "closed",
          `readiness channel failed: ${error.message}`,
          { cause: error },
        ),
      );
    const onData = (chunk: Buffer | string): void => {
      content = Buffer.concat([
        content,
        typeof chunk === "string" ? Buffer.from(chunk) : chunk,
      ]);
      if (content.length > MAX_READY_BYTES) {
        finish(
          new CtxmuxActivationReadinessError(
            socketPath,
            "invalid",
            `readiness record exceeds ${String(MAX_READY_BYTES)} bytes`,
          ),
        );
        return;
      }
      const newline = content.indexOf(0x0a);
      if (newline < 0) return;
      if (content.subarray(newline + 1).length > 0) {
        finish(
          new CtxmuxActivationReadinessError(
            socketPath,
            "invalid",
            "readiness channel contained more than one record",
          ),
        );
        return;
      }
      try {
        finish(
          parseReadyRecord(
            parseJsonFrame(content.subarray(0, newline).toString("utf8")),
          ),
        );
      } catch (error) {
        finish(
          new CtxmuxActivationReadinessError(
            socketPath,
            "invalid",
            `invalid readiness record: ${asError(error).message}`,
            { cause: error },
          ),
        );
      }
    };
    stream.on("data", onData);
    stream.once("end", onClosed);
    stream.once("close", onClosed);
    stream.once("error", onStreamError);
    child.once("error", onChildError);
    child.once("exit", onExit);
    child.once("close", onChildClose);
  });
}

function parseReadyRecord(value: unknown): ReadyRecord {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new TypeError("readiness record must be an object");
  }
  const record = value as Record<string, unknown>;
  const keys = Object.keys(record).sort();
  if (
    keys.length !== 2 ||
    keys[0] !== "daemon_instance" ||
    keys[1] !== "schema" ||
    record.schema !== READY_SCHEMA ||
    typeof record.daemon_instance !== "string" ||
    !UUID.test(record.daemon_instance)
  ) {
    throw new TypeError(
      `expected ${READY_SCHEMA} with one canonical daemon_instance`,
    );
  }
  return { daemonInstance: record.daemon_instance };
}

async function awaitMatchingHello(
  child: ChildProcess,
  socketPath: string,
  ready: ReadyRecord,
  deadline: Deadline,
): Promise<RuntimeIdentity> {
  let lastError: unknown;
  while (deadline.remaining() > 0) {
    const exit = childExit(child);
    if (exit !== undefined) {
      throw new CtxmuxActivationReadinessError(
        socketPath,
        "closed",
        "daemon exited after readiness but before matching Hello",
        { readinessInstance: ready.daemonInstance },
      );
    }
    try {
      const runtime = await probeRuntime(socketPath, deadline);
      if (runtime.daemonInstanceId !== ready.daemonInstance) {
        throw new CtxmuxActivationReadinessError(
          socketPath,
          "mismatch",
          `readiness instance ${ready.daemonInstance} does not match Hello ${runtime.daemonInstanceId}`,
          {
            readinessInstance: ready.daemonInstance,
            runtimeInstance: runtime.daemonInstanceId,
          },
        );
      }
      return runtime;
    } catch (error) {
      if (error instanceof CtxmuxActivationError) throw error;
      lastError = error;
      if (!isUnavailableProbe(error)) {
        throw new CtxmuxActivationReadinessError(
          socketPath,
          "mismatch",
          `readiness could not be matched to Hello: ${asError(error).message}`,
          { readinessInstance: ready.daemonInstance, cause: error },
        );
      }
    }
    await waitForObservedChange(deadline);
  }
  throw readinessTimeout(
    socketPath,
    `matching Hello did not arrive${
      lastError === undefined ? "" : ` (${asError(lastError).message})`
    }`,
    ready.daemonInstance,
  );
}

function verifyCompatibility(
  runtime: RuntimeIdentity,
  options: NormalizedOptions,
): void {
  if (
    options.expectedRuntimeIdentity !== undefined &&
    !runtimeIdentitiesEqual(runtime, options.expectedRuntimeIdentity)
  ) {
    throw new CtxmuxActivationConflictError(
      options.socketPath,
      "identity_mismatch",
      `Runtime ${runtime.runtimeId}/${runtime.daemonInstanceId} does not match the expected identity`,
      { actual: runtime, expected: options.expectedRuntimeIdentity },
    );
  }
  if (
    options.expectedBuildId !== undefined &&
    runtime.buildId !== options.expectedBuildId
  ) {
    throw new CtxmuxActivationConflictError(
      options.socketPath,
      "build_mismatch",
      `Runtime build ${runtime.buildId} does not match ${options.expectedBuildId}`,
      { actual: runtime },
    );
  }
  for (const [capability, requiredVersion] of Object.entries(
    options.requiredCapabilities,
  )) {
    const advertisedVersion = Object.hasOwn(runtime.capabilities, capability)
      ? runtime.capabilities[capability]
      : undefined;
    if (
      advertisedVersion === undefined ||
      advertisedVersion < requiredVersion
    ) {
      throw new CtxmuxActivationConflictError(
        options.socketPath,
        "capability_mismatch",
        `Runtime capability ${JSON.stringify(capability)} requires ${String(requiredVersion)}, advertised ${
          advertisedVersion === undefined ? "absent" : String(advertisedVersion)
        }`,
        {
          actual: runtime,
          capability,
          requiredVersion,
          advertisedVersion,
        },
      );
    }
  }
}

/**
 * Look for an activator that won the socket path this call's launcher lost.
 *
 * Reached only after this call's own launcher already failed, so the caller's
 * remaining budget is no longer bounding a startup — it is bounding a question
 * that two distinct observations answer very differently:
 *
 * - A socket path that exists but does not yet serve Hello is positive evidence
 *   that a winner bound it and is mid-handshake. That deserves the caller's
 *   full remaining deadline, because the winner is real and merely unfinished.
 *   "Does not yet serve Hello" means the connection was accepted and then lost,
 *   not refused: a path whose connect is *refused* has nothing listening on it,
 *   which is a corpse socket a dead daemon left behind and evidence against a
 *   winner rather than for one.
 * - A socket path that does not exist means nobody has bound it. A winner can
 *   still appear, but only by winning a bind this call already lost, which is a
 *   syscall-scale race rather than a startup-scale one. Spending the caller's
 *   whole deadline on it converts a launcher crash knowable in milliseconds
 *   into a timeout reported tens of seconds later, and reports the wrong error
 *   with it.
 *
 * So an absent *or refused* path is bounded by its own small grace window, and
 * observing a path that actually answered promotes the search back to the
 * caller's deadline. Waiting is never skipped outright: a winner whose bind
 * lands within the grace window is still found and still reused.
 */
async function probeConcurrentWinner(
  options: NormalizedOptions,
): Promise<LiveTarget | undefined> {
  const absentWinnerDeadline = deadlineAfter(
    Math.min(
      RUNTIME_ACTIVATION_ABSENT_WINNER_GRACE_MS,
      options.deadline.remaining(),
    ),
  );
  let observedTarget = false;
  while (options.deadline.remaining() > 0) {
    try {
      const target = await inspectTarget(options.socketPath, options.deadline);
      if (target.kind === "live") {
        verifyCompatibility(target.runtime, options);
        return target;
      }
      // A losing launcher can observe the winner's socket inode before the
      // winner has completed its Hello handshake. Keep observing that public
      // condition until the shared deadline instead of guessing from timing.
      //
      // A refused probe is the opposite evidence and must not promote: a socket
      // file whose connect is refused is a corpse left by a daemon that died
      // without unlinking, which says nobody is serving. Treating it as a winner
      // mid-handshake is what made a crashed launcher plus a leftover socket
      // spend the caller's whole budget.
      if (target.kind !== "missing" && !target.refused) observedTarget = true;
      if (!observedTarget && absentWinnerDeadline.remaining() <= 0) {
        return undefined;
      }
      await waitForObservedChange(
        observedTarget ? options.deadline : absentWinnerDeadline,
      );
    } catch (error) {
      if (
        error instanceof CtxmuxActivationConflictError ||
        error instanceof CtxmuxActivationTargetError
      ) {
        throw error;
      }
      return undefined;
    }
  }
  return undefined;
}

function activationFailure(
  error: unknown,
  options: NormalizedOptions,
  stderr: string | undefined,
): CtxmuxActivationError {
  if (error instanceof CtxmuxActivationError) return error;
  if (error instanceof ChildSpawnError) {
    return new CtxmuxActivationLaunchError(
      "spawn_failed",
      options.executable,
      options.socketPath,
      `failed to start: ${error.message}`,
      { stderr, cause: error },
    );
  }
  if (error instanceof ChildExitedBeforeReadyError) {
    return new CtxmuxActivationLaunchError(
      "launcher_exited",
      options.executable,
      options.socketPath,
      "exited before publishing readiness",
      {
        exitCode: error.exit.code,
        signal: error.exit.signal,
        stderr,
        cause: error,
      },
    );
  }
  return new CtxmuxActivationLaunchError(
    "launcher_exited",
    options.executable,
    options.socketPath,
    asError(error).message,
    { stderr, cause: error },
  );
}

function isEarlyLauncherFailure(error: unknown): boolean {
  return (
    error instanceof ChildSpawnError ||
    error instanceof ChildExitedBeforeReadyError ||
    (error instanceof CtxmuxActivationReadinessError &&
      error.readinessKind === "closed")
  );
}

function activationFor(
  options: NormalizedOptions,
  runtime: RuntimeIdentity,
  spawned: boolean,
  child: ChildProcess | undefined,
  identity: SocketIdentity | undefined,
): RuntimeActivation {
  return createRuntimeActivation({
    client: new CtxmuxClient({
      socketPath: options.socketPath,
      expectedRuntimeIdentity: runtime,
      requiredCapabilities: options.requiredCapabilities,
    }),
    runtime,
    socketPath: options.socketPath,
    spawned,
    ...(child === undefined ? {} : { child }),
    ...(identity === undefined ? {} : { socketIdentity: identity }),
  });
}

async function terminateOwnedChild(
  child: ChildProcess,
  socketPath: string,
  identity: SocketIdentity | undefined,
): Promise<void> {
  if (childExit(child) === undefined) {
    signalOwnedGroup(child, "SIGINT");
    if (
      (await waitForChildExit(child, RUNTIME_ACTIVATION_SHUTDOWN_GRACE_MS)) ===
      undefined
    ) {
      signalOwnedGroup(child, "SIGTERM");
      if (
        (await waitForChildExit(
          child,
          RUNTIME_ACTIVATION_SHUTDOWN_GRACE_MS,
        )) === undefined
      ) {
        signalOwnedGroup(child, "SIGKILL");
        if (
          (await waitForChildExit(
            child,
            RUNTIME_ACTIVATION_FORCE_REAP_GRACE_MS,
          )) === undefined
        ) {
          throw new CtxmuxActivationError(
            "cleanup_failed",
            `could not reap spawned daemon for ${socketPath}`,
            socketPath,
          );
        }
      }
    }
  }
  await removeProvenOwnedSocket(socketPath, identity);
}

function waitForChildExit(
  child: ChildProcess,
  timeoutMs: number,
): Promise<ChildExit | undefined> {
  const existing = childExit(child);
  if (existing !== undefined) return Promise.resolve(existing);
  return new Promise((resolve) => {
    let settled = false;
    const onExit = (
      code: number | null,
      signal: NodeJS.Signals | null,
    ): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolve({ code, signal });
    };
    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      child.off("exit", onExit);
      resolve(undefined);
    }, timerMs(timeoutMs));
    child.once("exit", onExit);
  });
}

function childExit(child: ChildProcess): ChildExit | undefined {
  return child.exitCode !== null || child.signalCode !== null
    ? { code: child.exitCode, signal: child.signalCode }
    : undefined;
}

function signalOwnedGroup(child: ChildProcess, signal: NodeJS.Signals): void {
  if (childExit(child) !== undefined || child.pid === undefined) return;
  if (process.platform !== "win32") {
    try {
      process.kill(-child.pid, signal);
      return;
    } catch (error) {
      if (nodeErrorCode(error) === "ESRCH") return;
    }
  }
  try {
    child.kill(signal);
  } catch {
    // A raced already-dead child is covered by exit observation.
  }
}

async function removeProvenOwnedSocket(
  socketPath: string,
  identity: SocketIdentity | undefined,
): Promise<void> {
  if (identity === undefined) return;
  const current = await targetMetadata(socketPath);
  if (
    current === undefined ||
    !current.isSocket() ||
    !sameIdentity(identity, socketIdentity(current))
  ) {
    return;
  }
  // If another live Runtime has somehow acquired the same inode, cleanup loses.
  try {
    await probeRuntime(socketPath, deadlineAfter(100));
    return;
  } catch (error) {
    if (!isUnavailableProbe(error)) return;
  }
  try {
    await unlink(socketPath);
  } catch (error) {
    if (nodeErrorCode(error) !== "ENOENT") {
      throw new CtxmuxActivationError(
        "cleanup_failed",
        `could not remove proven stale socket ${socketPath}: ${asError(error).message}`,
        socketPath,
        error,
      );
    }
  }
}

function detachChild(
  child: ChildProcess,
  readinessStream: NonNullable<ChildProcess["stdio"][number]>,
  diagnostics: DiagnosticCapture,
): void {
  diagnostics.unref();
  child.unref();
  const stream = readinessStream as typeof readinessStream & {
    unref?: () => void;
    destroy?: () => void;
  };
  stream.unref?.();
  stream.destroy?.();
}

async function currentSocketIdentity(
  socketPath: string,
): Promise<SocketIdentity | undefined> {
  const metadata = await targetMetadata(socketPath);
  return metadata?.isSocket() === true ? socketIdentity(metadata) : undefined;
}

function socketIdentity(metadata: Stats): SocketIdentity {
  return { device: Number(metadata.dev), inode: Number(metadata.ino) };
}

function sameIdentity(left: SocketIdentity, right: SocketIdentity): boolean {
  return left.device === right.device && left.inode === right.inode;
}

function copyRuntimeIdentity(runtime: RuntimeIdentity): RuntimeIdentity {
  return { ...runtime, capabilities: { ...runtime.capabilities } };
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
      ([key, version]) =>
        Object.hasOwn(expected.capabilities, key) &&
        expected.capabilities[key] === version,
    )
  );
}

function isUnavailableProbe(error: unknown): boolean {
  return error instanceof ProbeUnavailableError;
}

/**
 * Whether an unavailable probe was refused outright rather than accepted and
 * then cut short.
 *
 * `ECONNREFUSED`/`ENOENT`/`ENXIO` mean the kernel found nothing listening at
 * the path, so a socket file present with one of these is a corpse. The rest
 * mean a connection was established and then lost, which is what a server
 * mid-startup looks like.
 */
function isRefusedProbe(error: unknown): boolean {
  const code = nodeErrorCode(
    error instanceof ProbeUnavailableError ? error.cause : error,
  );
  return code === "ECONNREFUSED" || code === "ENOENT" || code === "ENXIO";
}

function connectionUnavailableCode(code: string | undefined): boolean {
  return (
    code === "ENOENT" ||
    code === "ECONNREFUSED" ||
    code === "ECONNRESET" ||
    code === "EPIPE" ||
    code === "ENOTCONN" ||
    code === "ENXIO"
  );
}

function connectionPermissionCode(code: string | undefined): boolean {
  return code === "EACCES" || code === "EPERM";
}

function nodeErrorCode(error: unknown): string | undefined {
  if (typeof error !== "object" || error === null || !("code" in error)) {
    return undefined;
  }
  const code = (error as { readonly code?: unknown }).code;
  return typeof code === "string" ? code : undefined;
}

function readinessTimeout(
  socketPath: string,
  detail: string,
  readinessInstance?: string,
): CtxmuxActivationReadinessError {
  return new CtxmuxActivationReadinessError(socketPath, "timeout", detail, {
    readinessInstance,
  });
}

function monotonicNow(): number {
  return typeof performance === "undefined" ? Date.now() : performance.now();
}

function deadlineAfter(durationMs: number): Deadline {
  const expiresAt = monotonicNow() + durationMs;
  return {
    expiresAt,
    remaining: () => Math.max(0, expiresAt - monotonicNow()),
  };
}

function timerMs(value: number): number {
  return Math.max(1, Math.min(2_147_483_647, Math.ceil(value)));
}

async function waitForObservedChange(deadline: Deadline): Promise<void> {
  const remaining = deadline.remaining();
  if (remaining <= 0) return;
  await new Promise<void>((resolve) => {
    setTimeout(resolve, Math.min(READY_POLL_MS, timerMs(remaining)));
  });
}
