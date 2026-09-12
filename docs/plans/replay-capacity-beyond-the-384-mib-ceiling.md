# Replay capacity past the SQLite payload ceiling: historical measurements

Status: historical evidence. Current resource policy is owned by
[decision 019](../architecture/choices/019-resource-policy-and-honest-qualification.md).
Current storage optimization objectives are owned by the
[replay storage benchmark contract](../replay-storage-benchmark.md). The earlier
compression ranking and cursor objection below were not an end-to-end codec
comparison and do not remain accepted reasons to defer compression.

## Verdict

The original review ranked three candidates from one old inline-payload store.
Only its measured observations are preserved as evidence; the ranking is not a
current optimization decision.

| #   | Change                             | Measured return                               | Raises the ceiling?               |
| --- | ---------------------------------- | --------------------------------------------- | --------------------------------- |
| 1   | Coalesce replay rows before commit | ~40% of the file back                         | no, but the file holds ~2x        |
| 2   | Move cold replay out of SQLite     | main-database ceiling no longer caps payloads | **yes, for the SQLite main file** |
| 3   | Compress chunk payloads            | 2.1x on 58% of the file                       | no — a constant                   |

1 and 2 are implemented. Candidate 1 coalesces rows before commit. Candidate 2
stores replay payloads in append-only generation files and leaves SQLite with
the durable window index. Compression is not implemented. Its active evaluation
now measures actual append, index, read, capacity and recovery costs; being a
constant-factor improvement is not a reason to dismiss a useful optimization.

## The measurement

A production store wedged at exactly `DATABASE_MAX_PAGES` (one
host, 198 Runs, 170 of them terminal), read `immutable=1` while its daemon
served:

```
file                402,653,184 B = 384 MiB   (98,304 / 98,304 pages, freelist 0)
  chunk payload     222.2 MiB  (58%)
  everything else   161.8 MiB  (42%)
replay_chunks rows  762,048    mean 306 B/row, median 82 B
```

`dbstat`:

| object                             | size      |
| ---------------------------------- | --------- |
| `replay_chunks` (table)            | 306.8 MiB |
| `sqlite_autoindex_replay_chunks_1` | 37.7 MiB  |
| `replay_chunks_run_start_byte`     | 37.7 MiB  |
| `runs` + its indexes               | 1.4 MiB   |

The duplicate explicit `(run_id, start_byte)` index in this table describes the
pre-schema-5 production wedge. The new schema keeps only SQLite's unique
constraint index and stores replay payloads outside the database.
That historical immutable read did not establish live-WAL visibility. New
grounding uses a SQLite read-only connection with WAL visibility instead.

**42% of the ceiling stores the fact that we stored something.** 762,048 rows
at a median of 82 B; the two indexes alone cost 75.4 MiB to index 222 MiB of
payload. The store is not full of output. It is full of row headers.

## 1 — Coalesce rows before commit

PTY output arrives in whatever sizes `read()` returns, and each one becomes a
row. A median of 82 B carries ~224 B of row header, page slack and two index
entries: **the bookkeeping is ~2.7x the byte it protects.**

Buffer per Run and commit at a target row size (~64 KiB) or a latency bound,
whichever comes first. Same bytes, ~1/500th the rows, and both indexes shrink
with the row count.

Expected: ~40% of the file returned, at roughly 250 MiB of payload per 384 MiB
instead of 222 MiB — and, more usefully, far more headroom per admitted write.

Cost: a bounded in-memory buffer per live Run, and a latency bound so an idle
Run's last bytes are not held hostage to a size target that will never be
reached. That bound is the load-bearing decision — get it wrong and a quiet
Run's final output is invisible to replay until it exits.

Does not raise the ceiling. Makes the existing one hold about twice as much.

## 2 — Move cold replay out of SQLite

This is the one that answers "why is there a limit at all".

SQLite earns its cost on data that needs transactions and random mutation.
Replay is append-only, read sequentially from a cursor, and never modified
after the write. That is the shape of a file.

One shared append-only generation file serves every Run, plus one small index
row per retained segment (`run_id, start_byte, end_byte, generation, offset,
length`). Moving payload does not itself collapse index rows; the current local
census still has roughly 480,000 extent records. The
generation is owner-only, payload bytes are synced before the SQLite row
commits, and the active generation name is part of `runtime_meta`.

What it buys:

- the SQLite main-database ceiling stops bounding payloads; logical replay and
  aggregate state-directory limits remain explicit retention policy;
- payload leaves SQLite; actual headers and index cost still require measurement;
- larger indexed compressed frames can use repetition across old record boundaries;
  the historical 4.3x concatenated result below is not production qualification;
- **deleting a terminal Run's history no longer copies payloads inside SQLite.**
  The index delete is small, and the next generation compaction reclaims the
  unreferenced bytes without requiring a main-database page allocation. On
  the original single-row delete against the wedged store hung 25 s and applied
  nothing: freeing space required space.

That last point is not a side benefit. It removes the failure mode that
`c168c0a` currently has to work around from inside the allocation path.

The consistency rule stays small: a failed transaction truncates its writer
tail; an outer commit with an unknown outcome preserves a harmless tail for
startup resolution; startup truncates any unreferenced tail, removes orphan
generations, and fails closed when a referenced segment is missing, short, or
overlapping. Once a generation exceeds twice the logical replay budget,
compaction writes a new generation, syncs its bytes and directory entry,
switches all offsets in one SQLite transaction, and unlinks the old generation.
This paragraph describes the old schema-5 boundary; the current protocol and
decision 019 own the supported schema and incremental coordinate migration.
There is no implicit compatibility or state-recreation action in this report.

## 3 — Historical compression sample and corrected interpretation

Measured with zlib level 6 over 500 randomly sampled live chunks (seed 7):

```
per-chunk        2.1x     <- each sampled chunk independently
concatenated     4.3x     <- the sampled chunks combined
```

The sample suggests that resetting compression history on every tiny chunk
loses useful repetition. It does not establish that compressing across record
boundaries defeats arbitrary-cursor replay: independent frames plus an index
can serve ranges, and a continuing frame can preserve history across flushes.
Read amplification, memory, latency and recovery still need qualification.

It also only addresses the 58% that is payload. The 42% of headers and indexes
is incompressible by this route. Whole-file effect: roughly 1.5x.

Compression does not create unlimited capacity. A measured constant-factor
capacity or throughput gain can still be useful and must be assessed across all
dimensions. The new local geometry pilot and acceptance matrix are linked from
the storage contract; neither the old 4.3x number nor a codec-only timing proves
whole-store capacity, fast tail reads or negligible implementation cost.

## Not in scope

Raising `DATABASE_MAX_BYTES`. It moves the wall without changing what hits it,
and 42% of whatever number is chosen still goes to row overhead. The historical
file budgets (`GLOBAL_REPLAY_BYTES`, `METADATA_BYTES`, `STATE_FILES_MAX_BYTES`)
do not override decision 019's configurable owner funding. This old report
cannot turn a convenient constant into a product capacity requirement.

## Provenance

Triggered by the original production wedge: `start` failed deterministically
with `WireClosedError` while reads served in 6 ms and all 28 live Runs stayed
healthy. `c168c0a` fixed the _reaction_ to a full store — reclaim before
allocating, instead of refusing the write. It deliberately did not touch the
ceiling or the row shape. This document covers what it left.

Numbers here are from one host and one workload. Before committing to
candidate 1's row-size target, re-measure the row-size distribution on at least
one other real store: a workload of large paste-heavy output would show a very
different median and change the target.
