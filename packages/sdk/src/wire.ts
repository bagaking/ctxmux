import { createConnection, type Socket } from "node:net";

import { MAX_FRAME_BYTES } from "./generated/constants.js";

interface WaitingLine {
  readonly resolve: (line: string) => void;
  readonly reject: (error: Error) => void;
}

interface QueuedLine {
  readonly value: string;
  readonly bytes: number;
}

interface PendingWrite {
  readonly reject: (error: Error) => void;
}

const MAX_QUEUED_LINES = 256;
const MAX_QUEUED_BYTES = MAX_FRAME_BYTES;
const UTF8_DECODER = new TextDecoder("utf-8", { fatal: true });

export class WireClosedError extends Error {
  public constructor(message = "ctxmux daemon closed the connection") {
    super(message);
    this.name = "WireClosedError";
  }
}

/** One newline-delimited JSON connection to a local ctxmux daemon. */
export class JsonLinesConnection {
  readonly #socket: Socket;
  #buffer: Buffer<ArrayBufferLike> = Buffer.alloc(0);
  readonly #lines: QueuedLine[] = [];
  #queuedBytes = 0;
  readonly #waiting: WaitingLine[] = [];
  readonly #pendingWrites = new Set<PendingWrite>();
  #terminalError: Error | undefined;
  #paused = false;

  /** Wrap a connected socket. Public within this internal module for owner tests. */
  public constructor(socket: Socket) {
    this.#socket = socket;
    socket.on("data", (chunk: Buffer) => this.#accept(chunk));
    socket.once("end", () => this.#terminate(new WireClosedError()));
    socket.once("error", (error) => this.#terminate(error));
    socket.once("close", () =>
      this.#terminate(new WireClosedError("ctxmux transport closed")),
    );
  }

  public static async connect(
    socketPath: string,
  ): Promise<JsonLinesConnection> {
    if (socketPath.length === 0) {
      throw new TypeError("socketPath must not be empty");
    }
    return await new Promise((resolve, reject) => {
      const socket = createConnection({ path: socketPath });
      const onError = (error: Error): void => reject(error);
      socket.once("error", onError);
      socket.once("connect", () => {
        socket.off("error", onError);
        resolve(new JsonLinesConnection(socket));
      });
    });
  }

  public async send(value: unknown): Promise<void> {
    if (this.#terminalError !== undefined) {
      throw this.#terminalError;
    }
    const payload = `${JSON.stringify(value)}\n`;
    if (Buffer.byteLength(payload) - 1 > MAX_FRAME_BYTES) {
      throw new RangeError(`ctxmux frame exceeds ${MAX_FRAME_BYTES} bytes`);
    }
    await new Promise<void>((resolve, reject) => {
      const pending: PendingWrite = { reject };
      this.#pendingWrites.add(pending);
      this.#socket.write(payload, (error?: Error | null) => {
        this.#pendingWrites.delete(pending);
        if (error !== undefined && error !== null) {
          this.#terminate(error);
          reject(this.#terminalError ?? error);
          return;
        }
        if (this.#terminalError !== undefined) {
          reject(this.#terminalError);
          return;
        }
        resolve();
      });
    });
  }

  public async receive(): Promise<unknown> {
    const queued = this.#lines.shift();
    if (queued !== undefined) {
      this.#queuedBytes -= queued.bytes;
      this.#pump();
      return this.#parse(queued.value);
    }
    if (this.#terminalError !== undefined) {
      throw this.#terminalError;
    }
    return await new Promise((resolve, reject) => {
      this.#waiting.push({
        resolve: (nextLine) => {
          try {
            resolve(this.#parse(nextLine));
          } catch (error) {
            reject(asError(error));
          }
        },
        reject,
      });
    });
  }

  public close(): void {
    this.#socket.destroy();
    this.#terminate(new WireClosedError("ctxmux client closed the connection"));
  }

  #accept(chunk: Buffer): void {
    if (this.#terminalError !== undefined) {
      return;
    }
    this.#buffer =
      this.#buffer.length === 0 ? chunk : Buffer.concat([this.#buffer, chunk]);
    this.#pump();
  }

  #pump(): void {
    if (this.#terminalError !== undefined) {
      return;
    }
    let newline = this.#buffer.indexOf(0x0a);
    while (newline >= 0) {
      if (newline > MAX_FRAME_BYTES) {
        this.#terminate(
          new RangeError(`ctxmux frame exceeds ${MAX_FRAME_BYTES} bytes`),
        );
        this.#socket.destroy();
        return;
      }
      const waiting = this.#waiting[0];
      if (
        waiting === undefined &&
        (this.#lines.length >= MAX_QUEUED_LINES ||
          (this.#lines.length > 0 &&
            this.#queuedBytes + newline > MAX_QUEUED_BYTES))
      ) {
        this.#pause();
        return;
      }
      let line: string;
      try {
        line = UTF8_DECODER.decode(this.#buffer.subarray(0, newline));
      } catch (error) {
        this.#terminate(
          new SyntaxError(
            `invalid ctxmux UTF-8 frame: ${asError(error).message}`,
          ),
        );
        this.#socket.destroy();
        return;
      }
      this.#buffer = this.#buffer.subarray(newline + 1);
      const activeWaiting = this.#waiting.shift();
      if (activeWaiting === undefined) {
        this.#lines.push({ value: line, bytes: newline });
        this.#queuedBytes += newline;
      } else {
        activeWaiting.resolve(line);
        if (this.#terminalError !== undefined) {
          return;
        }
      }
      newline = this.#buffer.indexOf(0x0a);
    }
    if (this.#buffer.length > MAX_FRAME_BYTES) {
      this.#terminate(
        new RangeError(`ctxmux frame exceeds ${MAX_FRAME_BYTES} bytes`),
      );
      this.#socket.destroy();
      return;
    }
    if (
      this.#paused &&
      this.#lines.length < MAX_QUEUED_LINES &&
      this.#queuedBytes < MAX_QUEUED_BYTES
    ) {
      this.#paused = false;
      this.#socket.resume();
    }
  }

  #pause(): void {
    if (!this.#paused) {
      this.#paused = true;
      this.#socket.pause();
    }
  }

  #parse(line: string): unknown {
    try {
      return parseJson(line);
    } catch (error) {
      const terminalError = asError(error);
      this.#buffer = Buffer.alloc(0);
      this.#lines.length = 0;
      this.#queuedBytes = 0;
      this.#terminate(terminalError);
      this.#socket.destroy();
      throw terminalError;
    }
  }

  #terminate(error: Error): void {
    if (this.#terminalError !== undefined) {
      return;
    }
    this.#terminalError = error;
    for (const pending of this.#pendingWrites) {
      pending.reject(error);
    }
    this.#pendingWrites.clear();
    for (const waiting of this.#waiting.splice(0)) {
      waiting.reject(error);
    }
  }
}

function parseJson(line: string): unknown {
  try {
    rejectDuplicateObjectMembers(line);
    return JSON.parse(line) as unknown;
  } catch (error) {
    throw new SyntaxError(
      `invalid ctxmux JSON frame: ${asError(error).message}`,
    );
  }
}

function rejectDuplicateObjectMembers(value: string): void {
  const stack: Array<
    { readonly kind: "array" } | { readonly kind: "object"; keys: Set<string> }
  > = [];
  for (let index = 0; index < value.length; index += 1) {
    switch (value[index]) {
      case "{":
        stack.push({ kind: "object", keys: new Set() });
        break;
      case "[":
        stack.push({ kind: "array" });
        break;
      case "}":
      case "]":
        stack.pop();
        break;
      case '"': {
        const start = index;
        index = stringEnd(value, index);
        const context = stack.at(-1);
        if (
          context?.kind !== "object" ||
          value[nextNonWhitespace(value, index + 1)] !== ":"
        ) {
          break;
        }
        const key = JSON.parse(value.slice(start, index + 1)) as unknown;
        if (typeof key !== "string") {
          break;
        }
        if (context.keys.has(key)) {
          throw new SyntaxError(
            `duplicate object member ${JSON.stringify(key)}`,
          );
        }
        context.keys.add(key);
        break;
      }
    }
  }
}

function stringEnd(value: string, openingQuote: number): number {
  for (let index = openingQuote + 1; index < value.length; index += 1) {
    if (value[index] === "\\") {
      index += 1;
    } else if (value[index] === '"') {
      return index;
    }
  }
  return value.length;
}

function nextNonWhitespace(value: string, start: number): number {
  let index = start;
  while (/\s/u.test(value[index] ?? "")) {
    index += 1;
  }
  return index;
}

function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}
