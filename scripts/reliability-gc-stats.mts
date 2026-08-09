import assert from "node:assert/strict";
import type { Readable } from "node:stream";

export const GC_STAT_GAUGES = [
  "retained_runs",
  "creation_keys",
  "creation_flights",
  "publication_reservations",
  "collecting_tickets",
  "overlap_owners",
  "cleanup_owners",
  "direct_children",
  "readers",
  "waiters",
  "input_drains",
  "attachments",
  "tmux_owners",
] as const;

export const GC_STAT_COUNTERS = [
  "physical_starts_total",
  "candidate_selections_total",
  "candidate_evaluations_total",
  "candidate_evaluations_max",
  "candidate_fences_total",
  "exact_replacements_total",
] as const;

type GaugeName = (typeof GC_STAT_GAUGES)[number];
type CounterName = (typeof GC_STAT_COUNTERS)[number];

export interface QualificationStatsSample {
  readonly timestamp_ms: number;
  readonly daemon_instance: string;
  readonly seq: number;
  readonly current: Readonly<Record<GaugeName, number>>;
  readonly high_water: Readonly<Record<GaugeName, number>>;
  readonly cumulative: Readonly<Record<CounterName, number>>;
}

export interface QualificationStatsArtifactSummary {
  readonly daemon_instance: string;
  readonly frames: number;
  readonly first_seq: 1;
  readonly last_seq: number;
  readonly max_sample_gap_ms: number;
  readonly final: QualificationStatsSample;
}

interface QualificationStatsFrame {
  readonly schema: "ctxmux.qualification-stats.v1";
  readonly timestamp_unix_ms: number;
  readonly daemon_instance: string;
  readonly seq: number;
  readonly final: boolean;
  readonly dropped_total: number;
  readonly current: readonly number[];
  readonly high_water: readonly number[];
  readonly cumulative: readonly number[];
}

interface ReceivedFrame {
  readonly frame: QualificationStatsFrame;
  readonly timestamp_ms: number;
}

const FRAME_FIELDS = [
  "schema",
  "timestamp_unix_ms",
  "daemon_instance",
  "seq",
  "final",
  "dropped_total",
  "current",
  "high_water",
  "cumulative",
] as const;

export class QualificationStatsCollector {
  readonly #raw: Buffer[] = [];
  readonly #frames: ReceivedFrame[] = [];
  readonly #ended: Promise<void>;
  #resolveEnd!: () => void;
  #buffer = Buffer.alloc(0);
  #failure: Error | null = null;
  #closing = false;
  #expectedDaemonInstance: string | null = null;

  public constructor(stream: Readable) {
    this.#ended = new Promise((resolve) => {
      this.#resolveEnd = resolve;
    });
    stream.on("data", (chunk: Buffer) => this.#consume(Buffer.from(chunk)));
    stream.once("error", (error) => {
      this.#fail(
        new Error(`qualification stats stream failed: ${error.message}`),
      );
      this.#resolveEnd();
    });
    stream.once("end", () => {
      if (!this.#closing) {
        this.#fail(
          new Error("qualification stats reached EOF while daemon was live"),
        );
      }
      if (this.#buffer.length !== 0) {
        this.#fail(new Error("qualification stats ended with a partial frame"));
      }
      this.#resolveEnd();
    });
  }

  public bindDaemonInstance(daemonInstance: string): void {
    assert.match(daemonInstance, /^[0-9a-f-]{36}$/u);
    this.#expectedDaemonInstance = daemonInstance;
    for (const { frame } of this.#frames) this.#validateInstance(frame);
  }

  public markClosing(): void {
    this.#closing = true;
  }

  public latest(): QualificationStatsSample {
    this.#throwIfFailed();
    const received = this.#frames.at(-1);
    assert.notEqual(
      received,
      undefined,
      "qualification stats emitted no frame",
    );
    return sample(received!);
  }

  public samples(): readonly QualificationStatsSample[] {
    this.#throwIfFailed();
    return this.#frames.map(sample);
  }

  public latestAfter(timestampMs: number): QualificationStatsSample | null {
    this.#throwIfFailed();
    const received = this.#frames.findLast(
      ({ timestamp_ms }) => timestamp_ms > timestampMs,
    );
    return received === undefined ? null : sample(received);
  }

  public rawBytes(): Buffer {
    return Buffer.concat(this.#raw);
  }

  public async finish(requireFinal = true): Promise<QualificationStatsSample> {
    await this.#ended;
    this.#throwIfFailed();
    const latest = this.#frames.at(-1);
    assert.notEqual(latest, undefined, "qualification stats emitted no frame");
    if (requireFinal) {
      assert.equal(
        latest!.frame.final,
        true,
        "qualification stats omitted final snapshot",
      );
    }
    assert.equal(
      this.#frames.filter(({ frame }) => frame.final).length,
      requireFinal ? 1 : 0,
      "qualification stats final snapshot count drifted",
    );
    return sample(latest!);
  }

  #consume(chunk: Buffer): void {
    this.#raw.push(chunk);
    this.#buffer = Buffer.concat([this.#buffer, chunk]);
    for (;;) {
      const newline = this.#buffer.indexOf(0x0a);
      if (newline < 0) return;
      const line = this.#buffer.subarray(0, newline);
      this.#buffer = this.#buffer.subarray(newline + 1);
      try {
        assert.ok(line.length > 0, "qualification stats frame is empty");
        const frame = JSON.parse(line.toString("utf8")) as unknown;
        this.#accept(frame);
      } catch (error) {
        this.#fail(
          error instanceof Error
            ? error
            : new Error(`qualification stats parse failed: ${String(error)}`),
        );
      }
    }
  }

  #accept(value: unknown): void {
    const frame = validateFrame(
      value,
      this.#frames,
      this.#expectedDaemonInstance,
    );
    this.#frames.push({ frame, timestamp_ms: frame.timestamp_unix_ms });
  }

  #validateInstance(frame: QualificationStatsFrame): void {
    if (this.#expectedDaemonInstance !== null) {
      assert.equal(
        frame.daemon_instance,
        this.#expectedDaemonInstance,
        "qualification stats daemon instance differs from public Hello",
      );
    }
  }

  #fail(error: Error): void {
    this.#failure ??= error;
  }

  #throwIfFailed(): void {
    if (this.#failure !== null) throw this.#failure;
  }
}

/** Revalidate one persisted raw telemetry artifact with the live collector's
 * exact frame rules. The final verifier uses this after ownership checks so a
 * truncated, dropped, reordered, or post-final log cannot become evidence. */
export function validateQualificationStatsArtifact(
  bytes: Buffer,
): QualificationStatsArtifactSummary {
  assert.ok(bytes.length > 0, "qualification stats artifact is empty");
  assert.equal(
    bytes.at(-1),
    0x0a,
    "qualification stats artifact ended with a partial frame",
  );
  const frames: ReceivedFrame[] = [];
  for (const line of bytes.toString("utf8").split("\n").slice(0, -1)) {
    assert.ok(line.length > 0, "qualification stats frame is empty");
    const value = JSON.parse(line) as unknown;
    const frame = validateFrame(value, frames, null);
    frames.push({ frame, timestamp_ms: frame.timestamp_unix_ms });
  }
  const final = frames.at(-1);
  assert.notEqual(final, undefined, "qualification stats emitted no frame");
  assert.equal(
    final!.frame.final,
    true,
    "qualification stats omitted final snapshot",
  );
  assert.equal(
    frames.filter(({ frame }) => frame.final).length,
    1,
    "qualification stats final snapshot count drifted",
  );
  return {
    daemon_instance: final!.frame.daemon_instance,
    frames: frames.length,
    first_seq: 1,
    last_seq: final!.frame.seq,
    max_sample_gap_ms: maxSampleGap(frames),
    final: sample(final!),
  };
}

function validateFrame(
  value: unknown,
  frames: readonly ReceivedFrame[],
  expectedDaemonInstance: string | null,
): QualificationStatsFrame {
  assert.ok(isObject(value), "qualification stats frame must be an object");
  assert.deepEqual(
    Object.keys(value).sort(),
    [...FRAME_FIELDS].sort(),
    "qualification stats frame fields drifted",
  );
  assert.equal(value.schema, "ctxmux.qualification-stats.v1");
  assert.ok(nonNegativeSafeInteger(value.timestamp_unix_ms));
  assert.equal(typeof value.daemon_instance, "string");
  assert.equal(typeof value.final, "boolean");
  for (const [name, field, length] of [
    ["current", value.current, GC_STAT_GAUGES.length],
    ["high_water", value.high_water, GC_STAT_GAUGES.length],
    ["cumulative", value.cumulative, GC_STAT_COUNTERS.length],
  ] as const) {
    assert.ok(
      Array.isArray(field) &&
        field.length === length &&
        field.every(nonNegativeSafeInteger),
      `qualification stats ${name} is malformed`,
    );
  }
  assert.ok(nonNegativeSafeInteger(value.dropped_total));
  assert.equal(
    value.dropped_total,
    0,
    "qualification stats dropped observations",
  );
  const expectedSequence = (frames.at(-1)?.frame.seq ?? 0) + 1;
  assert.equal(value.seq, expectedSequence, "qualification stats sequence gap");
  const previous = frames.at(-1)?.frame;
  if (previous !== undefined) {
    assert.ok(
      value.timestamp_unix_ms >= previous.timestamp_unix_ms,
      "qualification stats timestamp rolled back",
    );
    for (const [name, current, prior] of [
      ["high_water", value.high_water, previous.high_water],
      ["cumulative", value.cumulative, previous.cumulative],
    ] as const) {
      assert.ok(
        (current as number[]).every((entry, index) => entry >= prior[index]!),
        `qualification stats ${name} rolled back`,
      );
    }
  }
  assert.ok(
    (value.current as number[]).every(
      (entry, index) => entry <= (value.high_water as number[])[index]!,
    ),
    "qualification stats current exceeds high water",
  );
  assert.ok(
    !frames.some(({ frame }) => frame.final),
    "qualification stats emitted data after final snapshot",
  );
  const frame = value as unknown as QualificationStatsFrame;
  if (expectedDaemonInstance !== null) {
    assert.equal(
      frame.daemon_instance,
      expectedDaemonInstance,
      "qualification stats daemon instance differs from public Hello",
    );
  }
  return frame;
}

function maxSampleGap(frames: readonly ReceivedFrame[]): number {
  return frames
    .slice(1)
    .reduce(
      (maximum, frame, index) =>
        Math.max(maximum, frame.timestamp_ms - frames[index]!.timestamp_ms),
      0,
    );
}

function sample(received: ReceivedFrame): QualificationStatsSample {
  const { frame } = received;
  return {
    timestamp_ms: received.timestamp_ms,
    daemon_instance: frame.daemon_instance,
    seq: frame.seq,
    current: named(GC_STAT_GAUGES, frame.current),
    high_water: named(GC_STAT_GAUGES, frame.high_water),
    cumulative: named(GC_STAT_COUNTERS, frame.cumulative),
  };
}

function named<Name extends string>(
  names: readonly Name[],
  values: readonly number[],
): Readonly<Record<Name, number>> {
  return Object.fromEntries(
    names.map((name, index) => [name, values[index]]),
  ) as Readonly<Record<Name, number>>;
}

function nonNegativeSafeInteger(value: unknown): value is number {
  return Number.isSafeInteger(value) && (value as number) >= 0;
}

function isObject(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
