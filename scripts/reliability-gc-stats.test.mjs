import assert from "node:assert/strict";
import { PassThrough } from "node:stream";
import test from "node:test";

import {
  GC_STAT_COUNTERS,
  GC_STAT_GAUGES,
  QualificationStatsCollector,
  validateQualificationStatsArtifact,
} from "./reliability-gc-stats.mts";

const INSTANCE = "00000000-0000-0000-0000-000000000001";

test("collector accepts a contiguous final owner snapshot", async () => {
  const stream = new PassThrough();
  const collector = new QualificationStatsCollector(stream);
  collector.bindDaemonInstance(INSTANCE);
  stream.write(frame(1));
  collector.markClosing();
  stream.end(frame(2, { final: true }));
  const last = await collector.finish();
  assert.equal(last.seq, 2);
  assert.equal(last.current.retained_runs, 0);
  assert.equal(last.cumulative.physical_starts_total, 0);
});

test("collector rejects sequence gaps, drops, malformed fields, and partial EOF", async () => {
  for (const mutation of [
    () => frame(2),
    () => frame(1, { dropped_total: 1 }),
    () => {
      const value = JSON.parse(frame(1));
      value.cumulative.pop();
      return `${JSON.stringify(value)}\n`;
    },
    () => frame(1).slice(0, -2),
  ]) {
    const stream = new PassThrough();
    const collector = new QualificationStatsCollector(stream);
    collector.markClosing();
    stream.end(mutation());
    await assert.rejects(() => collector.finish());
  }
});

test("collector rejects EOF before shutdown and missing final snapshot", async () => {
  {
    const stream = new PassThrough();
    const collector = new QualificationStatsCollector(stream);
    stream.end(frame(1));
    await assert.rejects(() => collector.finish(), /while daemon was live/u);
  }
  {
    const stream = new PassThrough();
    const collector = new QualificationStatsCollector(stream);
    collector.markClosing();
    stream.end(frame(1));
    await assert.rejects(() => collector.finish(), /omitted final/u);
  }
});

test("collector rejects high-water and cumulative rollback", async () => {
  for (const mutation of [
    () => {
      const first = JSON.parse(frame(1));
      first.high_water[0] = 2;
      const second = JSON.parse(frame(2, { final: true }));
      second.high_water[0] = 1;
      return `${JSON.stringify(first)}\n${JSON.stringify(second)}\n`;
    },
    () => {
      const first = JSON.parse(frame(1));
      first.cumulative[0] = 2;
      const second = JSON.parse(frame(2, { final: true }));
      second.cumulative[0] = 1;
      return `${JSON.stringify(first)}\n${JSON.stringify(second)}\n`;
    },
  ]) {
    const stream = new PassThrough();
    const collector = new QualificationStatsCollector(stream);
    collector.markClosing();
    stream.end(mutation());
    await assert.rejects(() => collector.finish(), /rolled back/u);
  }
});

test("persisted stats artifacts use the same final and sequence oracle", () => {
  const bytes = Buffer.from(`${frame(1)}${frame(2, { final: true })}`);
  const summary = validateQualificationStatsArtifact(bytes);
  assert.equal(summary.first_seq, 1);
  assert.equal(summary.last_seq, 2);
  assert.equal(summary.frames, 2);
  assert.equal(summary.max_sample_gap_ms, 1);
  for (const mutation of [
    bytes.subarray(0, bytes.length - 1),
    Buffer.from(frame(1)),
    Buffer.from(`${frame(1)}${frame(3, { final: true })}`),
  ]) {
    assert.throws(() => validateQualificationStatsArtifact(mutation));
  }
});

function frame(seq, overrides = {}) {
  return `${JSON.stringify({
    schema: "ctxmux.qualification-stats.v1",
    timestamp_unix_ms: seq,
    daemon_instance: INSTANCE,
    seq,
    final: false,
    dropped_total: 0,
    current: Array(GC_STAT_GAUGES.length).fill(0),
    high_water: Array(GC_STAT_GAUGES.length).fill(0),
    cumulative: Array(GC_STAT_COUNTERS.length).fill(0),
    ...overrides,
  })}\n`;
}
