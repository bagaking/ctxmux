# Replay capacity past the 384 MiB ceiling: measured, ranked, and one rejection

## Verdict

Three candidates, ranked by measured return. Compression — the intuitive
answer — is the one that loses.

| # | Change | Measured return | Raises the ceiling? |
|---|--------|-----------------|---------------------|
| 1 | Coalesce replay rows before commit | ~40% of the file back | no, but the file holds ~2x |
| 2 | Move cold replay out of SQLite | ceiling becomes the disk | **yes** |
| 3 | Compress chunk payloads | 2.1x on 58% of the file | no — a constant |

Do 1 first (smallest change, immediate), 2 for the actual ceiling, and 3 only
after 2, where it is nearly free and where the ratio is 4.3x rather than 2.1x.

## The measurement

A production store wedged at exactly `DATABASE_MAX_PAGES` (2026-09-21, one
host, 198 Runs, 170 of them terminal), read `immutable=1` while its daemon
served:

```
file                402,653,184 B = 384 MiB   (98,304 / 98,304 pages, freelist 0)
  chunk payload     222.2 MiB  (58%)
  everything else   161.8 MiB  (42%)
replay_chunks rows  762,048    mean 306 B/row, median 82 B
```

`dbstat`:

| object | size |
|--------|------|
| `replay_chunks` (table) | 306.8 MiB |
| `sqlite_autoindex_replay_chunks_1` | 37.7 MiB |
| `replay_chunks_run_start_byte` | 37.7 MiB |
| `runs` + its indexes | 1.4 MiB |

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

Per Run: one append-only file, plus a small index row per segment
(`run_id, start_byte, end_byte, path, offset`). Hundreds of index rows replace
762,048 data rows.

What it buys:

- the ceiling stops being a constant — retention becomes a disk-space policy,
  not a `max_page_count`;
- the 161.8 MiB of headers and indexes largely disappears;
- a cold file compresses as a whole (4.3x measured, below) instead of per
  82-byte chunk (2.1x);
- **deleting a terminal Run's history becomes `unlink`, not a write
  transaction.** On 2026-09-21 a single-row delete against the wedged store
  hung 25 s and applied nothing: freeing space required space. A file delete
  has no such dependency.

That last point is not a side benefit. It removes the failure mode that
`c168c0a` currently has to work around from inside the allocation path.

Cost: two durable stores to keep consistent, crash-recovery for partially
written segments, and an orphan-file sweep. This is the real work — it is a
persistence-layer change, not an optimization.

## 3 — Compression, and why it ranks last

Measured with zlib level 6 over 500 randomly sampled live chunks (seed 7):

```
per-chunk        2.1x     <- what a chunked store can actually apply
concatenated     4.3x     <- what a sequential file could apply
```

Terminal output compresses well in bulk and badly in 82-byte pieces — the
dictionary never warms up before the chunk ends. The 4.3x figure requires
compressing across chunk boundaries, which defeats the one thing replay exists
to do: slice from an arbitrary cursor.

It also only addresses the 58% that is payload. The 42% of headers and indexes
is incompressible by this route. Whole-file effect: roughly 1.5x.

**Compression is a constant factor; Run count and output are linear.** 2.1x is
bought back by opening twice as many Runs. A bound that does not grow with use
comes from eviction (which exists) or from a ceiling that is not a constant
(candidate 2) — never from a ratio.

After candidate 2, compression applies to whole cold files at 4.3x, sequential
reads absorb the decompression, and it costs almost nothing to add. That is the
order.

## Not in scope

Raising `DATABASE_MAX_BYTES`. It moves the wall without changing what hits it,
and 42% of whatever number is chosen still goes to row overhead. The frozen
file budgets (`GLOBAL_REPLAY_BYTES`, `METADATA_BYTES`, `STATE_FILES_MAX_BYTES`)
are a deliberate contract with the host; candidate 2 replaces the contract
rather than inflating it.

## Provenance

Triggered by a production wedge on 2026-09-21: `start` failed deterministically
with `WireClosedError` while reads served in 6 ms and all 28 live Runs stayed
healthy. `c168c0a` fixed the *reaction* to a full store — reclaim before
allocating, instead of refusing the write. It deliberately did not touch the
ceiling or the row shape. This document covers what it left.

Numbers here are from one host and one workload. Before committing to
candidate 1's row-size target, re-measure the row-size distribution on at least
one other real store: a workload of large paste-heavy output would show a very
different median and change the target.
