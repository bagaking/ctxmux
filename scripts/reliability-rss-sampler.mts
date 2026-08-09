import assert from "node:assert/strict";
import {
  spawn,
  type ChildProcess,
  type ChildProcessWithoutNullStreams,
} from "node:child_process";
import { createInterface } from "node:readline";

export interface TimedRssSample {
  readonly timestamp_ms: number;
  readonly rss_kib: number;
}

export interface RssSampler {
  readonly peak: () => number;
  readonly sampleCount: () => number;
  readonly samples: () => readonly TimedRssSample[];
  readonly maxGapMs: () => number;
  readonly stop: () => Promise<void>;
}

export interface PreparedRssSampler {
  readonly start: () => Promise<RssSampler>;
}

interface NativeFrame extends TimedRssSample {
  readonly schema: "ctxmux.rss-sample.v1";
  readonly seq: number;
  readonly final_frame: boolean;
}

const FRAME_FIELDS = [
  "schema",
  "timestamp_ms",
  "seq",
  "rss_kib",
  "final_frame",
] as const;
const HELPER_REAP_TIMEOUT_MS = 5_000;
const HELPER_STARTUP_TIMEOUT_MS = 5_000;
const HELPER_READY_LINE = "ctxmux-rss-sampler-ready-v1";

export async function prepareRssSampler(
  helperBinary: string,
  target: number | ChildProcess,
  intervalMs: number,
  maximumGapMs: number,
): Promise<PreparedRssSampler> {
  const pid = typeof target === "number" ? target : target.pid;
  assert.ok(
    pid !== undefined && Number.isSafeInteger(pid) && pid > 0,
    "RSS sampler PID is invalid",
  );
  assert.ok(
    Number.isSafeInteger(intervalMs) && intervalMs > 0,
    "RSS sampler interval is invalid",
  );
  assert.ok(
    Number.isSafeInteger(maximumGapMs) && maximumGapMs >= intervalMs,
    "RSS sampler maximum gap is invalid",
  );
  const child = spawn(
    helperBinary,
    [
      "--pid",
      String(pid),
      "--interval-ms",
      String(intervalMs),
      "--max-gap-ms",
      String(maximumGapMs),
    ],
    { stdio: ["pipe", "pipe", "pipe"] },
  );
  return prepareNativeSampler(
    child,
    maximumGapMs,
    typeof target === "number" ? undefined : target,
  );
}

export function startRssSampler(
  prepared: PreparedRssSampler,
): Promise<RssSampler> {
  return prepared.start();
}

async function prepareNativeSampler(
  child: ChildProcessWithoutNullStreams,
  maximumGapMs: number,
  target: ChildProcess | undefined,
): Promise<PreparedRssSampler> {
  const frames: NativeFrame[] = [];
  let stderr = "";
  let stdoutEndedWithNewline = true;
  let failure: Error | undefined;
  let stopping: Promise<void> | undefined;
  let stopRequested = false;
  let observationWindowStartedAtMs: number | undefined;
  const helperReady = Promise.withResolvers<void>();
  const ready = Promise.withResolvers<void>();
  const finalFrame = Promise.withResolvers<void>();
  const closed = Promise.withResolvers<number | null>();
  const outputClosed = Promise.withResolvers<void>();
  void helperReady.promise.catch(() => undefined);
  void ready.promise.catch(() => undefined);
  const fail = (error: Error): void => {
    failure ??= error;
    helperReady.reject(failure);
    ready.reject(failure);
  };
  const onTargetExit = (): void => {
    fail(new Error("RSS sampling target exited before sampler completion"));
    child.kill("SIGKILL");
  };
  target?.once("exit", onTargetExit);
  child.stderr.setEncoding("utf8");
  const stderrLines = createInterface({
    input: child.stderr,
    crlfDelay: Infinity,
  });
  stderrLines.on("line", (line) => {
    if (line === HELPER_READY_LINE) helperReady.resolve();
  });
  child.stderr.on("data", (chunk: string) => {
    stderr += chunk;
  });
  child.stdin.on("error", (error: NodeJS.ErrnoException) => {
    if (error.code !== "EPIPE") fail(error);
  });
  child.stdout.on("data", (chunk: Buffer) => {
    stdoutEndedWithNewline = chunk.at(-1) === 0x0a;
  });
  child.once("error", fail);
  child.once("close", (code) => {
    if (!stopRequested) {
      fail(
        new Error(
          `RSS sampler terminated before stop was requested with code ${String(code)}: ${stderr.trim()}`,
        ),
      );
    }
    closed.resolve(code);
  });
  const lines = createInterface({ input: child.stdout, crlfDelay: Infinity });
  lines.once("close", () => outputClosed.resolve());
  lines.on("line", (line) => {
    try {
      const frame = validateFrame(
        JSON.parse(line) as unknown,
        frames,
        maximumGapMs,
        stopRequested,
      );
      if (frames.length === 0) {
        assert.notEqual(
          observationWindowStartedAtMs,
          undefined,
          "RSS sampler emitted before start",
        );
        const observationDeadlineMs =
          observationWindowStartedAtMs! + maximumGapMs;
        assert.ok(
          frame.timestamp_ms >= observationWindowStartedAtMs! &&
            frame.timestamp_ms <= observationDeadlineMs &&
            Date.now() <= observationDeadlineMs,
          "RSS sampler first observation started outside its contract",
        );
      }
      frames.push(frame);
      if (frame.final_frame) finalFrame.resolve();
      if (frames.length === 1) {
        assert.equal(
          frame.final_frame,
          false,
          "RSS sampler ended before it became ready",
        );
        ready.resolve();
      }
    } catch (error) {
      fail(error instanceof Error ? error : new Error(String(error)));
      child.kill("SIGKILL");
    }
  });

  const awaitBounded = async (
    promise: Promise<void>,
    timeoutMs: number,
    message: string,
  ): Promise<void> => {
    const timeout = setTimeout(() => {
      fail(new Error(message));
      child.kill("SIGKILL");
    }, timeoutMs);
    await promise.finally(() => clearTimeout(timeout));
  };

  try {
    await Promise.race([
      awaitBounded(
        helperReady.promise,
        HELPER_STARTUP_TIMEOUT_MS,
        "RSS sampler helper did not become ready in time",
      ),
      outputClosed.promise.then(async () => {
        const code = await closed.promise;
        throw new Error(
          `RSS sampler exited before helper readiness with code ${String(code)}: ${stderr.trim()}`,
        );
      }),
    ]);
  } catch (error) {
    if (child.exitCode === null) child.kill("SIGKILL");
    await Promise.all([closed.promise, outputClosed.promise]);
    target?.off("exit", onTargetExit);
    throw error;
  }

  const stop = async (): Promise<void> => {
    stopRequested = true;
    try {
      if (failure === undefined && child.exitCode === null)
        child.stdin.end("stop\n");
      const finalTimeout = setTimeout(
        () => child.kill("SIGKILL"),
        maximumGapMs,
      );
      await Promise.race([
        finalFrame.promise,
        closed.promise.then(() => undefined),
      ]).finally(() => clearTimeout(finalTimeout));
      const reapTimeout = setTimeout(
        () => child.kill("SIGKILL"),
        HELPER_REAP_TIMEOUT_MS,
      );
      let hardReapTimeout: NodeJS.Timeout | undefined;
      const [code] = await Promise.race([
        Promise.all([closed.promise, outputClosed.promise]),
        new Promise<never>((_, reject) => {
          hardReapTimeout = setTimeout(
            () =>
              reject(new Error("RSS sampler did not close and reap in time")),
            HELPER_REAP_TIMEOUT_MS * 2,
          );
        }),
      ]).finally(() => {
        clearTimeout(reapTimeout);
        clearTimeout(hardReapTimeout);
      });
      if (failure !== undefined) throw failure;
      assert.equal(code, 0, `RSS sampler failed: ${stderr.trim()}`);
      assert.equal(
        stdoutEndedWithNewline,
        true,
        "RSS sampler ended with a partial frame",
      );
      assert.equal(
        frames.filter(({ final_frame }) => final_frame).length,
        1,
        "RSS sampler omitted its single final frame",
      );
      assert.equal(frames.at(-1)?.final_frame, true);
    } finally {
      target?.off("exit", onTargetExit);
    }
  };

  const start = async (): Promise<RssSampler> => {
    assert.equal(
      observationWindowStartedAtMs,
      undefined,
      "RSS sampler preparation can start only once",
    );
    observationWindowStartedAtMs = Date.now();
    child.stdin.write("start\n");
    try {
      await Promise.race([
        awaitBounded(
          ready.promise,
          maximumGapMs,
          "RSS sampler did not emit its first frame in time",
        ),
        outputClosed.promise.then(async () => {
          const code = await closed.promise;
          throw new Error(
            `RSS sampler exited before first observation with code ${String(code)}: ${stderr.trim()}`,
          );
        }),
      ]);
    } catch (error) {
      if (child.exitCode === null) child.kill("SIGKILL");
      target?.off("exit", onTargetExit);
      void Promise.all([closed.promise, outputClosed.promise]);
      throw error;
    }
    return sampler;
  };
  const sampler: RssSampler = {
    peak: () =>
      frames.reduce((maximum, sample) => Math.max(maximum, sample.rss_kib), 0),
    sampleCount: () => frames.length,
    samples: () =>
      frames.map(({ timestamp_ms, rss_kib }) => ({ timestamp_ms, rss_kib })),
    maxGapMs: () => maxSampleGap(frames),
    stop: () => {
      stopping ??= stop();
      return stopping;
    },
  };
  return { start };
}

function validateFrame(
  value: unknown,
  frames: readonly NativeFrame[],
  maximumGapMs: number,
  allowFinalFrame: boolean,
): NativeFrame {
  assert.ok(
    value !== null && typeof value === "object" && !Array.isArray(value),
  );
  const frame = value as Record<string, unknown>;
  assert.deepEqual(Object.keys(frame).sort(), [...FRAME_FIELDS].sort());
  assert.equal(frame.schema, "ctxmux.rss-sample.v1");
  assert.ok(
    Number.isSafeInteger(frame.timestamp_ms) && Number(frame.timestamp_ms) >= 0,
  );
  assert.ok(Number.isSafeInteger(frame.rss_kib) && Number(frame.rss_kib) > 0);
  assert.equal(frame.seq, frames.length + 1, "RSS sampler sequence gap");
  assert.equal(typeof frame.final_frame, "boolean");
  if (frame.final_frame === true && !allowFinalFrame) {
    assert.fail(
      frames.length === 0
        ? "RSS sampler ended before it became ready"
        : "RSS sampler ended before stop was requested",
    );
  }
  assert.ok(
    !frames.some(({ final_frame }) => final_frame),
    "RSS sampler emitted after final frame",
  );
  const previous = frames.at(-1);
  if (previous !== undefined) {
    assert.ok(
      Number(frame.timestamp_ms) >= previous.timestamp_ms &&
        Number(frame.timestamp_ms) - previous.timestamp_ms <= maximumGapMs,
      "RSS sampler timestamp gap exceeded its contract",
    );
  }
  return frame as unknown as NativeFrame;
}

function maxSampleGap(samples: readonly TimedRssSample[]): number {
  return samples
    .slice(1)
    .reduce(
      (maximum, sample, index) =>
        Math.max(maximum, sample.timestamp_ms - samples[index]!.timestamp_ms),
      0,
    );
}
