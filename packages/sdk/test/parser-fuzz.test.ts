import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { Socket } from "node:net";
import test from "node:test";

import type { RunEvent } from "../src/generated/RunEvent.ts";
import { codexIntegration } from "../src/integrations/codex.ts";
import type { CodexSemanticEvent } from "../src/integrations/codex.ts";
import { validateServerFrame } from "../src/validation.ts";
import { JsonLinesConnection, parseJsonFrame } from "../src/wire.ts";

interface CodexRegressionCase {
  readonly id: string;
  readonly chunks: readonly (readonly number[])[];
  readonly terminal: "none" | "gap" | "exit";
  readonly expected_names: readonly string[];
  readonly expected_diagnostics: readonly string[];
}

const MALFORMED_PROTOCOL_SEEDS = (
  JSON.parse(
    readFileSync(
      new URL(
        "../../../fixtures/malformed-protocol-frames.json",
        import.meta.url,
      ),
      "utf8",
    ),
  ) as {
    readonly frames: readonly { readonly bytes: readonly number[] }[];
  }
).frames.map(({ bytes }) => Buffer.from(bytes));

const WIRE_SEEDS = [
  ...MALFORMED_PROTOCOL_SEEDS,
  Buffer.from('{"type":"detached"}'),
  Buffer.from(
    '{"type":"error","error":{"code":"invalid_request","message":"线程"}}',
  ),
];

const CODEX_REGRESSIONS = (
  JSON.parse(
    readFileSync(
      new URL(
        "../../../fixtures/codex-jsonl-regressions.json",
        import.meta.url,
      ),
      "utf8",
    ),
  ) as {
    readonly schema: string;
    readonly cases: readonly CodexRegressionCase[];
  }
).cases;

test("Codex JSONL minimized regression corpus remains exact", () => {
  for (const regression of CODEX_REGRESSIONS) {
    const observer = codexIntegration.createObserver();
    const events: CodexSemanticEvent[] = [];
    for (const [index, chunk] of regression.chunks.entries()) {
      events.push(...observer.observe(output(index + 1, chunk)));
    }
    if (regression.terminal === "gap") {
      events.push(
        ...observer.observe({ type: "gap", latest_output_bytes: 10_000 }),
      );
    } else if (regression.terminal === "exit") {
      events.push(...observer.observe(exited()));
    }

    assert.deepEqual(
      events.map(({ name }) => name),
      regression.expected_names,
      regression.id,
    );
    assert.deepEqual(
      events
        .filter(({ name }) => name === "integration.parse_error")
        .map(({ data }) => data.reason),
      regression.expected_diagnostics,
      regression.id,
    );
  }
});

test("seeded TypeScript wire validation fuzz target settles every byte stream", async () => {
  const settings = fuzzSettings();
  const random = new Random(settings.seed);

  for (let caseIndex = 0; caseIndex < settings.cases; caseIndex += 1) {
    const bytes =
      caseIndex % 3 === 0
        ? mutateSeed(WIRE_SEEDS[random.integer(WIRE_SEEDS.length)]!, random)
        : random.bytes(random.integer(2049));
    const payload = Buffer.concat([bytes, Buffer.from("\n")]);
    const socket = new Socket();
    const wire = new JsonLinesConnection(socket);
    const received = wire.receive();

    if (caseIndex % 5 === 0) {
      socket.emit("data", payload);
    } else {
      let offset = 0;
      while (offset < payload.length) {
        const size = 1 + random.integer(Math.min(64, payload.length - offset));
        socket.emit("data", payload.subarray(offset, offset + size));
        offset += size;
      }
    }

    try {
      const value = await received;
      const reparsed = parseJsonFrame(JSON.stringify(value));
      try {
        validateServerFrame(reparsed);
      } catch (error) {
        assert.ok(error instanceof Error, replay(settings, caseIndex));
      }
    } catch (error) {
      assert.ok(error instanceof Error, replay(settings, caseIndex));
    } finally {
      wire.close();
    }
  }
  console.log(
    `TypeScript wire fuzz replay: seed=${settings.seed} cases=${settings.cases}`,
  );
});

test("seeded Codex JSONL observer fuzz target stays bounded and diagnostic", () => {
  const settings = fuzzSettings();
  const random = new Random(settings.seed ^ 0x434f_4445);
  const codexSeeds = CODEX_REGRESSIONS.map(({ chunks }) =>
    Buffer.concat(chunks.map((chunk) => Buffer.from(chunk))),
  );

  for (let caseIndex = 0; caseIndex < settings.cases; caseIndex += 1) {
    const observer = codexIntegration.createObserver();
    const bytes =
      caseIndex % 3 === 0
        ? mutateSeed(codexSeeds[random.integer(codexSeeds.length)]!, random)
        : random.bytes(random.integer(4097));
    const events: CodexSemanticEvent[] = [];
    let offset = 0;
    let sequence = 1;

    try {
      while (offset < bytes.length) {
        const size = 1 + random.integer(Math.min(128, bytes.length - offset));
        events.push(
          ...observer.observe(
            output(sequence, bytes.subarray(offset, offset + size)),
          ),
        );
        offset += size;
        sequence += 1;
        if (random.integer(257) === 0) {
          events.push(
            ...observer.observe({ type: "gap", latest_output_bytes: sequence }),
          );
        }
      }
      events.push(...observer.observe(exited()));
    } catch (error) {
      assert.fail(`${replay(settings, caseIndex)}: ${String(error)}`);
    }

    for (const event of events) {
      assert.equal(event.integrationId, "codex", replay(settings, caseIndex));
      assert.equal(typeof event.name, "string", replay(settings, caseIndex));
      assert.doesNotThrow(
        () => JSON.stringify(event),
        replay(settings, caseIndex),
      );
      if (event.name === "integration.parse_error") {
        assert.ok(
          [
            "output_gap",
            "invalid_json",
            "invalid_event",
            "record_too_large",
            "invalid_utf8",
          ].includes(String(event.data.reason)),
          replay(settings, caseIndex),
        );
      }
    }
  }
  console.log(
    `Codex JSONL fuzz replay: seed=${settings.seed} cases=${settings.cases}`,
  );
});

function output(startByte: number, data: ArrayLike<number>): RunEvent {
  const bytes = Array.from(data);
  return {
    type: "output",
    chunk: {
      start_byte: startByte,
      end_byte: startByte + bytes.length,
      data: bytes,
    },
  };
}

function exited(): RunEvent {
  return {
    type: "exited",
    state: { type: "exited", code: 0, signal: null },
  };
}

function mutateSeed(seed: Buffer, random: Random): Buffer {
  const bytes = [...seed];
  const mutations = 1 + random.integer(8);
  for (let index = 0; index < mutations; index += 1) {
    switch (random.integer(3)) {
      case 0:
        if (bytes.length > 0) {
          bytes[random.integer(bytes.length)]! ^= random.integer(256);
        }
        break;
      case 1:
        bytes.splice(random.integer(bytes.length + 1), 0, random.integer(256));
        break;
      default:
        if (bytes.length > 0) bytes.splice(random.integer(bytes.length), 1);
    }
  }
  return Buffer.from(bytes);
}

interface FuzzSettings {
  readonly seed: number;
  readonly cases: number;
}

function fuzzSettings(): FuzzSettings {
  const rawSeed = process.env.CTXMUX_FUZZ_SEED ?? "827541837";
  const rawCases = process.env.CTXMUX_FUZZ_CASES ?? "256";
  const seed = Number.parseInt(rawSeed, 10);
  const cases = Number.parseInt(rawCases, 10);
  if (!Number.isSafeInteger(seed) || seed < 0) {
    throw new TypeError("CTXMUX_FUZZ_SEED must be a non-negative integer");
  }
  if (!Number.isSafeInteger(cases) || cases <= 0) {
    throw new TypeError("CTXMUX_FUZZ_CASES must be a positive integer");
  }
  return { seed: seed >>> 0, cases };
}

function replay(settings: FuzzSettings, caseIndex: number): string {
  return `seed=${settings.seed} case=${caseIndex}`;
}

class Random {
  #state: number;

  public constructor(seed: number) {
    this.#state = seed === 0 ? 0x9e37_79b9 : seed >>> 0;
  }

  public integer(exclusiveMaximum: number): number {
    assert.ok(exclusiveMaximum > 0);
    let value = this.#state;
    value ^= value << 13;
    value ^= value >>> 17;
    value ^= value << 5;
    this.#state = value >>> 0;
    return this.#state % exclusiveMaximum;
  }

  public bytes(length: number): Buffer {
    const bytes = Buffer.alloc(length);
    for (let index = 0; index < length; index += 1) {
      bytes[index] = this.integer(256);
    }
    return bytes;
  }
}
