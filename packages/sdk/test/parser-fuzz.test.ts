import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { Socket } from "node:net";
import test from "node:test";

import { validateServerFrame } from "../src/validation.ts";
import { JsonLinesConnection, parseJsonFrame } from "../src/wire.ts";

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
