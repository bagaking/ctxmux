import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { resolve } from "node:path";

import type {
  CreateOperationKey,
  CtxmuxClient,
  RunId,
  RunInfo,
  RunSpec,
} from "../packages/sdk/src/index.ts";
import type { LoadedReliabilityGcContract } from "./reliability-gc-contract.mts";
import { startAndWaitForGcRunExit } from "./reliability-gc-deadline.mts";

export interface GcRunExpectation {
  readonly mode: string;
  readonly index: number;
  readonly operation_key: CreateOperationKey;
  readonly run_id: RunId;
  readonly payload_bytes: number;
  readonly payload_sha256: string;
}

export interface GcRunTuple {
  readonly run_id: RunId;
  readonly operation_key: CreateOperationKey;
  readonly lineage: RunInfo["lineage"];
  readonly state: RunInfo["state"];
  readonly head_seq: number;
  readonly durable_head_seq: number | null;
  readonly oldest_seq: number;
  readonly replay_bytes: number;
  readonly replay_sha256: string;
  readonly chunks: readonly {
    readonly seq: number;
    readonly bytes: number;
    readonly sha256: string;
  }[];
  readonly truncated: boolean;
}

export function gcSpec(
  root: string,
  loaded: LoadedReliabilityGcContract,
  mode: string,
  index: number,
): RunSpec {
  const contract = loaded.contract;
  assert.notEqual(
    contract.payload_modes[mode],
    undefined,
    `unknown GC mode ${mode}`,
  );
  return {
    program: process.execPath,
    args: [contract.helper.path, contract.seed, mode, String(index)],
    cwd: resolve(root),
    env: {},
    size: contract.run_spec.size,
    declared_inputs: [],
  };
}

export function gcOperationKey(
  loaded: LoadedReliabilityGcContract,
  mode: string,
  index: number,
): CreateOperationKey {
  const digest = sourceDigest(loaded, mode, index);
  return loaded.contract.replay_pressure.operation_key_template
    .replace("<mode>", mode)
    .replace("<index>", String(index))
    .replace("<digest-hex>", digest);
}

export async function startGcRun(
  client: CtxmuxClient,
  root: string,
  loaded: LoadedReliabilityGcContract,
  mode: string,
  index: number,
  phaseDeadline: number,
): Promise<GcRunExpectation> {
  const spec = gcSpec(root, loaded, mode, index);
  const operationKey = gcOperationKey(loaded, mode, index);
  const run = await startAndWaitForGcRunExit(
    {
      start: () => client.start(spec, operationKey),
      status: (id) => client.status(id as RunId),
    },
    {
      mode,
      index,
      operation_key: operationKey,
    },
    phaseDeadline,
  );
  const payload = payloadIdentity(loaded, mode, index);
  return {
    mode,
    index,
    operation_key: operationKey,
    run_id: run.id,
    payload_bytes: payload.bytes,
    payload_sha256: payload.sha256,
  };
}

export async function retryGcRun(
  client: CtxmuxClient,
  root: string,
  loaded: LoadedReliabilityGcContract,
  expected: GcRunExpectation,
): Promise<void> {
  const run = await client.start(
    gcSpec(root, loaded, expected.mode, expected.index),
    expected.operation_key,
  );
  assert.equal(
    run.id,
    expected.run_id,
    "same-key GC retry started another Run",
  );
}

export async function gcTuple(
  client: CtxmuxClient,
  expected: GcRunExpectation,
): Promise<GcRunTuple> {
  const run = await client.status(expected.run_id);
  const attachment = await client.attach(expected.run_id);
  try {
    const replay = attachment.snapshot.replay;
    assert.equal(replay.head_seq, run.head_seq);
    assert.equal(replay.oldest_seq, run.oldest_seq);
    assert.ok(
      replay.chunks.every(
        (chunk, index) =>
          index === 0 || chunk.seq === replay.chunks[index - 1]!.seq + 1,
      ),
      "GC replay sequence is not contiguous",
    );
    const bytes = Buffer.concat(
      replay.chunks.map((chunk) => Buffer.from(chunk.data)),
    );
    if (loadedRequireDurableHead(expected)) {
      assert.equal(
        run.durable_head_seq,
        run.head_seq,
        "GC live durable cursor did not reach the observed head",
      );
    }
    assert.ok(
      bytes.length <= expected.payload_bytes,
      "GC replay exceeds the helper payload",
    );
    assertExpectedSuffix(bytes, expected);
    return {
      run_id: run.id,
      operation_key: expected.operation_key,
      lineage: run.lineage,
      state: run.state,
      head_seq: run.head_seq,
      durable_head_seq: run.durable_head_seq,
      oldest_seq: run.oldest_seq,
      replay_bytes: bytes.length,
      replay_sha256: createHash("sha256").update(bytes).digest("hex"),
      chunks: replay.chunks.map((chunk) => ({
        seq: chunk.seq,
        bytes: chunk.data.length,
        sha256: createHash("sha256")
          .update(Buffer.from(chunk.data))
          .digest("hex"),
      })),
      truncated: replay.truncated,
    };
  } finally {
    attachment.close();
  }
}

function loadedRequireDurableHead(expected: GcRunExpectation): boolean {
  return expected.mode === "persistent_replay_pressure";
}

export function sortedTuples(
  tuples: readonly GcRunTuple[],
): readonly GcRunTuple[] {
  return [...tuples].sort((left, right) =>
    left.run_id.localeCompare(right.run_id),
  );
}

function payloadIdentity(
  loaded: LoadedReliabilityGcContract,
  mode: string,
  index: number,
): { readonly bytes: number; readonly sha256: string } {
  const modeContract = loaded.contract.payload_modes[mode];
  assert.notEqual(modeContract, undefined, `unknown GC mode ${mode}`);
  const digest = sourceDigest(loaded, mode, index);
  const hash = createHash("sha256");
  for (let remaining = modeContract!.hex_repetitions; remaining > 0;) {
    const repetitions = Math.min(remaining, 1024);
    hash.update(digest.repeat(repetitions), "ascii");
    remaining -= repetitions;
  }
  return { bytes: modeContract!.payload_bytes, sha256: hash.digest("hex") };
}

function assertExpectedSuffix(bytes: Buffer, expected: GcRunExpectation): void {
  assert.equal(
    createHash("sha256").update(bytes).digest("hex"),
    expectedGcReplaySha256(expected, bytes.length),
    "GC replay digest differs from the exact expected helper suffix",
  );
}

export function expectedGcReplaySha256(
  expected: GcRunExpectation,
  replayBytes: number,
): string {
  assert.ok(
    replayBytes >= 0 && replayBytes <= expected.payload_bytes,
    "GC replay length is outside the helper payload",
  );
  return expectedGcReplaySha256FromKey(
    expected.operation_key,
    expected.payload_bytes,
    replayBytes,
  );
}

export function expectedGcReplaySha256FromKey(
  operationKey: string,
  payloadBytes: number,
  replayBytes: number,
): string {
  assert.ok(
    replayBytes >= 0 && replayBytes <= payloadBytes,
    "GC replay length is outside the helper payload",
  );
  const digest = sourceDigestFromOperationKey(operationKey);
  const offset = (payloadBytes - replayBytes) % digest.length;
  const expectedSuffix = Buffer.from(
    digest
      .repeat(Math.ceil((offset + replayBytes) / digest.length))
      .slice(offset, offset + replayBytes),
    "ascii",
  );
  return createHash("sha256").update(expectedSuffix).digest("hex");
}

function sourceDigest(
  loaded: LoadedReliabilityGcContract,
  mode: string,
  index: number,
): string {
  return createHash("sha256")
    .update(`${loaded.contract.seed}:${mode}:${String(index)}`, "utf8")
    .digest("hex");
}

function sourceDigestFromOperationKey(operationKey: string): string {
  const marker = /^gc-pressure:[^:]+:\d+:([0-9a-f]{64})$/u.exec(operationKey);
  assert.notEqual(
    marker,
    null,
    "GC operation key no longer carries its source digest",
  );
  return marker![1]!;
}
