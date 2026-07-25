import type { AttachedSnapshot } from "./generated/AttachedSnapshot.js";
import type { ClientFrame } from "./generated/ClientFrame.js";
import type { ErrorCode } from "./generated/ErrorCode.js";
import type { ForkPlan } from "./generated/ForkPlan.js";
import { PROTOCOL_VERSION } from "./generated/constants.js";
import type { Request } from "./generated/Request.js";
import type { Response } from "./generated/Response.js";
import type { RunEvent } from "./generated/RunEvent.js";
import type { RunId } from "./generated/RunId.js";
import type { RunInfo } from "./generated/RunInfo.js";
import type { RunSpec } from "./generated/RunSpec.js";
import type { ServerFrame } from "./generated/ServerFrame.js";
import type { TerminalSize } from "./generated/TerminalSize.js";
import type { TmuxPaneInfo } from "./generated/TmuxPaneInfo.js";
import {
  CtxmuxInvalidFrameError,
  validateCursor,
  validateServerFrame,
} from "./validation.js";
import { JsonLinesConnection } from "./wire.js";

const runEventSources = new WeakMap<object, RunId>();

function rememberRunEventSource(event: RunEvent, runId: RunId): void {
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

export interface CtxmuxClientOptions {
  readonly socketPath: string;
}

export type ByteInput = string | Uint8Array;

export class CtxmuxProtocolError extends Error {
  public readonly code: ErrorCode;

  public constructor(code: ErrorCode, message: string) {
    super(message);
    this.name = "CtxmuxProtocolError";
    this.code = code;
  }
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
    const wire = await this.#connect();
    wire.close();
  }

  public async start(spec: RunSpec): Promise<RunInfo> {
    const response = await this.#request({ type: "start", spec });
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

  public async fork(parent: RunId, plan: ForkPlan): Promise<RunInfo> {
    const response = await this.#request({ type: "fork", parent, plan });
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

  public async input(id: RunId, data: ByteInput): Promise<RunInfo> {
    const response = await this.#request({
      type: "input",
      id,
      data: bytes(data),
    });
    return accepted(response);
  }

  public async resize(id: RunId, size: TerminalSize): Promise<RunInfo> {
    return accepted(await this.#request({ type: "resize", id, size }));
  }

  public async stop(id: RunId): Promise<RunInfo> {
    return accepted(await this.#request({ type: "stop", id }));
  }

  public async attach(id: RunId, afterSeq = 0): Promise<Attachment> {
    validateCursor(afterSeq, "afterSeq");
    const wire = await this.#connect();
    try {
      await wire.send({
        type: "request",
        request: { type: "attach", id, after_seq: afterSeq },
      } satisfies ClientFrame);
      const frame = serverFrame(await wire.receive());
      if (frame.type === "error") {
        throw protocolError(frame.error);
      }
      if (frame.type !== "attached") {
        throw unexpected("attached snapshot", frame.type);
      }
      const snapshot = await receiveReplay(wire, afterSeq, frame.snapshot);
      return new Attachment(wire, snapshot);
    } catch (error) {
      wire.close();
      throw error;
    }
  }

  async #request(request: Request): Promise<Response> {
    const wire = await this.#connect();
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

  async #connect(): Promise<JsonLinesConnection> {
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
      if (frame.type !== "hello" || frame.protocol !== PROTOCOL_VERSION) {
        throw unexpected("compatible hello", frame.type);
      }
      return wire;
    } catch (error) {
      wire.close();
      throw error;
    }
  }
}

async function receiveReplay(
  wire: JsonLinesConnection,
  afterSeq: number,
  header: Extract<ServerFrame, { readonly type: "attached" }>["snapshot"],
): Promise<AttachedSnapshot> {
  const chunks: AttachedSnapshot["replay"]["chunks"] = [];
  if (afterSeq >= header.replay.head_seq) {
    return {
      run: header.run,
      replay: { ...header.replay, chunks },
    };
  }
  let nextSequence = Math.max(afterSeq, header.replay.oldest_seq - 1);
  while (nextSequence < header.replay.head_seq) {
    const frame = serverFrame(await wire.receive());
    if (frame.type === "error") {
      throw protocolError(frame.error);
    }
    if (
      frame.type !== "event" ||
      frame.event.type !== "output" ||
      frame.event.chunk.seq !== nextSequence + 1
    ) {
      throw unexpected("ordered replay output", frame.type);
    }
    chunks.push(frame.event.chunk);
    nextSequence = frame.event.chunk.seq;
  }
  return {
    run: header.run,
    replay: { ...header.replay, chunks },
  };
}

/** Live TypeScript attachment to one daemon-owned Run. */
export class Attachment {
  readonly #wire: JsonLinesConnection;
  #detachPromise: Promise<void> | undefined;
  public readonly snapshot: AttachedSnapshot;

  public constructor(wire: JsonLinesConnection, snapshot: AttachedSnapshot) {
    this.#wire = wire;
    this.snapshot = snapshot;
    for (const chunk of snapshot.replay.chunks) {
      runEventSources.set(chunk, snapshot.run.id);
    }
  }

  public async input(data: ByteInput): Promise<void> {
    await this.#wire.send({
      type: "input",
      data: bytes(data),
    } satisfies ClientFrame);
  }

  public async resize(size: TerminalSize): Promise<void> {
    await this.#wire.send({ type: "resize", size } satisfies ClientFrame);
  }

  public async stop(): Promise<void> {
    await this.#wire.send({ type: "stop" } satisfies ClientFrame);
  }

  public detach(): Promise<void> {
    this.#detachPromise ??= this.#detachCleanly();
    return this.#detachPromise;
  }

  /** Abruptly close this client attachment without affecting its Run. */
  public close(): void {
    this.#wire.close();
  }

  public async nextEvent(): Promise<RunEvent | undefined> {
    let frame: ServerFrame;
    try {
      frame = serverFrame(await this.#wire.receive());
    } catch (error) {
      this.#wire.close();
      throw error;
    }
    if (frame.type === "event") {
      rememberRunEventSource(frame.event, this.snapshot.run.id);
      return frame.event;
    }
    if (frame.type === "detached") {
      this.#wire.close();
      return undefined;
    }
    if (frame.type === "error") {
      throw protocolError(frame.error);
    }
    throw unexpected("attachment event", frame.type);
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
    await this.#wire.send({ type: "detach" } satisfies ClientFrame);
    try {
      while (true) {
        const frame = serverFrame(await this.#wire.receive());
        if (frame.type === "detached") {
          this.#wire.close();
          return;
        }
        if (frame.type === "event") {
          continue;
        }
        if (frame.type === "error") {
          throw protocolError(frame.error);
        }
        throw unexpected("detached frame", frame.type);
      }
    } catch (error) {
      this.#wire.close();
      throw error;
    }
  }
}

function bytes(input: ByteInput): number[] {
  return Array.from(
    typeof input === "string" ? new TextEncoder().encode(input) : input,
  );
}

function accepted(response: Response): RunInfo {
  if (response.type !== "accepted") {
    throw unexpected("accepted response", response.type);
  }
  return response.run;
}

function protocolError(error: {
  readonly code: ErrorCode;
  readonly message: string;
}): Error {
  return new CtxmuxProtocolError(error.code, error.message);
}

function unexpected(expected: string, actual: string): Error {
  return new Error(`expected ${expected}, received ${actual}`);
}

function serverFrame(value: unknown): ServerFrame {
  return validateServerFrame(value);
}

export { CtxmuxInvalidFrameError };
