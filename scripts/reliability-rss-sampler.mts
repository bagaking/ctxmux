import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import {
  isMainThread,
  parentPort,
  Worker,
  workerData,
} from "node:worker_threads";

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

export function nextRssSampleDelay(
  previousStartedAt: number,
  now: number,
  intervalMs: number,
): number {
  return Math.max(0, previousStartedAt + intervalMs - now);
}

interface SamplerWorkerData {
  readonly pid: number;
  readonly interval_ms: number;
  readonly observation_timeout_ms: number;
}

interface ReadyMessage {
  readonly type: "ready";
}

interface StoppedMessage {
  readonly type: "stopped";
  readonly samples: readonly TimedRssSample[];
}

interface ErrorMessage {
  readonly type: "error";
  readonly error: string;
}

type WorkerMessage = ReadyMessage | StoppedMessage | ErrorMessage;

export async function startRssSampler(
  pid: number,
  intervalMs: number,
  maximumGapMs: number,
): Promise<RssSampler> {
  assert.ok(Number.isSafeInteger(pid) && pid > 0, "RSS sampler PID is invalid");
  assert.ok(
    Number.isSafeInteger(intervalMs) && intervalMs > 0,
    "RSS sampler interval is invalid",
  );
  assert.ok(
    Number.isSafeInteger(maximumGapMs) && maximumGapMs >= intervalMs,
    "RSS sampler maximum gap is invalid",
  );
  const worker = new Worker(new URL(import.meta.url), {
    workerData: {
      pid,
      interval_ms: intervalMs,
      observation_timeout_ms: maximumGapMs,
    } satisfies SamplerWorkerData,
  });
  let samples: readonly TimedRssSample[] = [];
  let stopped: Promise<void> | undefined;
  let workerFailure: Error | undefined;
  let finalReceived = false;
  const ready = Promise.withResolvers<void>();
  const finished = Promise.withResolvers<void>();
  const exited = Promise.withResolvers<void>();
  const fail = (error: Error): void => {
    workerFailure ??= error;
    ready.reject(workerFailure);
    finished.reject(workerFailure);
  };
  worker.on("message", (message: WorkerMessage) => {
    if (message.type === "ready") {
      ready.resolve();
    } else if (message.type === "error") {
      fail(new Error(message.error));
    } else {
      samples = message.samples;
      finalReceived = true;
      finished.resolve();
    }
  });
  worker.once("error", fail);
  worker.once("exit", (code) => {
    if (code !== 0) {
      fail(new Error(`RSS sampler worker exited with code ${code}`));
    } else if (!finalReceived) {
      fail(new Error("RSS sampler worker exited without a final sample"));
    }
    exited.resolve();
  });
  void ready.promise.catch(() => undefined);
  void finished.promise.catch(() => undefined);
  try {
    await ready.promise;
  } catch (error) {
    await exited.promise;
    throw error;
  }

  const stop = async (): Promise<void> => {
    if (workerFailure === undefined) worker.postMessage("stop");
    try {
      await finished.promise;
    } finally {
      await exited.promise;
    }
    if (workerFailure !== undefined) throw workerFailure;
  };

  return {
    peak: () =>
      samples.reduce((maximum, sample) => Math.max(maximum, sample.rss_kib), 0),
    sampleCount: () => samples.length,
    samples: () => samples,
    maxGapMs: () => maxSampleGap(samples),
    stop: () => {
      stopped ??= stop();
      return stopped;
    },
  };
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

function sampleRssKiB(pid: number, timeoutMs: number): TimedRssSample {
  const timestamp = Date.now();
  const result = spawnSync("ps", ["-o", "rss=", "-p", String(pid)], {
    encoding: "utf8",
    killSignal: "SIGKILL",
    timeout: timeoutMs,
  });
  const rss = Number.parseInt(result.stdout.trim(), 10);
  assert.ok(
    result.status === 0 && Number.isFinite(rss),
    `cannot sample RSS for ${pid}: ${result.stderr || result.error?.message || result.stdout || "unknown"}`,
  );
  return { timestamp_ms: timestamp, rss_kib: rss };
}

async function runSamplerWorker(): Promise<void> {
  assert.notEqual(parentPort, null, "RSS sampler worker has no parent port");
  const port = parentPort!;
  const data = workerData as SamplerWorkerData;
  const samples: TimedRssSample[] = [];
  let timer: NodeJS.Timeout | undefined;
  let stopping = false;

  const fail = (error: unknown): void => {
    const message = error instanceof Error ? error.message : String(error);
    port.postMessage({ type: "error", error: message } satisfies ErrorMessage);
    port.close();
  };
  const sample = (): void => {
    samples.push(sampleRssKiB(data.pid, data.observation_timeout_ms));
  };
  const schedule = (previousStartedAt: number): void => {
    const delayMs = nextRssSampleDelay(
      previousStartedAt,
      Date.now(),
      data.interval_ms,
    );
    timer = setTimeout(() => {
      if (stopping) return;
      try {
        sample();
        schedule(samples.at(-1)!.timestamp_ms);
      } catch (error) {
        stopping = true;
        fail(error);
      }
    }, delayMs);
  };

  try {
    sample();
    port.postMessage({ type: "ready" } satisfies ReadyMessage);
    schedule(samples.at(-1)!.timestamp_ms);
    port.once("message", (message: unknown) => {
      if (message !== "stop" || stopping) return;
      stopping = true;
      if (timer !== undefined) clearTimeout(timer);
      try {
        sample();
        port.postMessage({
          type: "stopped",
          samples,
        } satisfies StoppedMessage);
      } catch (error) {
        fail(error);
      }
      port.close();
    });
  } catch (error) {
    fail(error);
  }
}

if (!isMainThread) void runSamplerWorker();
