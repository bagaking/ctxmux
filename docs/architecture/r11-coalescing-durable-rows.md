# Round 11: the amplification is per-row overhead, and it is packable

- Status: measurement record + change rationale, 2026-09-14
- Host: cn3 (`n36-001-230`), 64-core Linux, ext4
- Harness: `docs/verification/harness/armlib.sh`; conventions
  [§4.3](../benchmark-comparison-conventions.md)
- Change: `a66b714`

## Where round 10 left it

[The cliff is a WAL fold throughput ceiling](r11-fold-throughput-ceiling.md):
folds saturate at ~57 MB/s while output keeps rising, and past that the WAL
cannot drain as fast as it fills. The target was stated there as "reduce how
many bytes need folding per unit of real output" — but nothing yet said what
those bytes _were_, and the two possible answers call for opposite fixes.

## The number that decides it

Amplification: WAL bytes folded per byte of real output. Measured on the same
binary with no rebuild, by sampling `sum(durable_output_bytes)` at 0.5 Hz
alongside the 20 Hz WAL sawtooth. Four arms per rate, both orders, two rounds:

| chatty | AMP   | real out  | fold      | mean on-disk chunk |
| ------ | ----- | --------- | --------- | ------------------ |
| 1      | 2.08x | 16.9 MB/s | 34.9 MB/s | 609 B              |
| 2      | 2.66x | 20.2 MB/s | 54.5 MB/s | 196 B              |
| 4      | 2.41x | 23.9 MB/s | 57.3 MB/s | 2138 B             |

So the ~57 MB/s fold ceiling is a **~24 MB/s real-output ceiling**, and
amplification is the divisor. Cut it and the ceiling rises by the same factor —
which makes this a lever rather than a wall.

Read `sum(durable_output_bytes)` with `max()` over the arm, never the last
sample: `warmbench` removes its fleet when it finishes, which drops every sum to
zero.

## Attribution: it is per-ROW, not per-page

My first guess was page padding — 4 KiB pages carrying 253 B chunks — which
would only be fixable by writing fewer bytes. Ablation against a replica of the
real schema says otherwise. Byte counts are portable even where this laptop's
fsync timing is not: what is being measured is how many pages SQLite dirties,
which depends on the schema and the insert pattern, not the disk.

| row size | AMP       |
| -------- | --------- |
| 208 B    | 2.51x     |
| 256 B    | 2.27x     |
| 4 KiB    | 1.38x     |
| 64 KiB   | **1.07x** |
| 256 KiB  | 1.04x     |

A row costs the same fixed overhead whether it carries 200 bytes or 200 KiB: a
record header, a 36-byte `run_id`, three integers, and an entry in the
`UNIQUE(run_id, start_byte)` index. At 200 B of payload that overhead is most of
the row.

The other two ablations are noise — dropping `AUTOINCREMENT` is worth 0.2%, and
skipping the per-transaction `runs` update 0.2%. Both were diagnostics, not
candidates; `ordinal` must stay monotonic for the global pruner.

## The change

Bytes inside one transaction are already proven contiguous per Run by
`is_fresh_contiguous`, so they can share a row. Coalescing changes **how the
bytes are packed, not what is stored** — no schema change, no
`SCHEMA_VERSION` bump, same `end_byte - start_byte == length(data)` invariant on
every row.

64 KiB is the knee: 53% of the available 54%, while an eviction still moves only
1.6% of the 4 MiB per-Run cap.

Coalescing is per _transaction_, not per append. `append_replay` runs once per
queued append, so a buffer scoped to one call would flush immediately and never
span the appends that actually share a transaction — which is exactly where the
fragmentation is. `coalesce_batch` merges each Run's appends first.

## Two things it breaks, both found by test

**A re-sent append names a slice, not a row.** Verification looked a row up by
exact `start_byte`. With packing, a re-sent range normally sits in the middle of
a much larger row, so that lookup finds nothing and reports honest durable bytes
as lost — which `remember_failure` latches daemon-wide.
`stored_range_matches` finds the containing row and compares the overlap.

**The daemon-wide ceiling stopped being enforceable.** The global pruner evicts
a whole row and skips a Run's last row, so no Run is emptied for another's
pressure. With one row per Run — now the ordinary case — there is no candidate
at all, and the 256 MiB ceiling silently returned `Corrupt` instead.
`trim_oldest_row` cuts a row's front in place.

That second fix has a subtlety worth recording, because the obvious version is
wrong in a way that still passes a "the ceiling is met" assertion. Trimming the
_globally oldest_ row mirrors the whole-row path, but `ordinal` is an insertion
counter and an in-place trim does not change it: the row just trimmed still
sorts oldest, so it is trimmed again and again down to one byte while its
neighbour keeps everything. Shedding the _largest_ Run toward an equal share is
max-min fair and terminates for the same reason it is fair — the share is
floored, so the largest Run is strictly above it whenever the total is over.

I nearly shipped a `.max(1)` that would have papered over this: it kept the loop
from spinning while leaving the unfairness in place. The termination bug and the
fairness bug were the same bug.

## Every new test was verified by mutation

A test that has never failed has not been shown to test anything.

| mutation                          | test                                                  | what it did                |
| --------------------------------- | ----------------------------------------------------- | -------------------------- |
| `COALESCE_ROW_BYTES = 1`          | `many_small_appends_become_few_large_rows`            | 401 rows, not ≤16          |
| lookup back to exact `start_byte` | `a_resent_append_verifies_against_a_slice_of_its_row` | latched on honest bytes    |
| trim ordered by `ordinal`         | `the_global_ceiling_is_met_without_gutting_one_run`   | ceiling unmet, 1000 vs 800 |

The slice test also re-sends the same range with _different_ bytes and requires
that to fail: without it, a lookup that accepts anything it finds would pass.

## Result: merged

64 guarded arms, 4 per cell, both orders, every arm verified idle
(`busy0` 0.0024-0.0278). Medians; ratio > 1 means R11 is faster.

| shape | start             | p90       | list      | stop       | remove      | CPU s      |
| ----- | ----------------- | --------- | --------- | ---------- | ----------- | ---------- |
| c0    | 0.98x             | 1.01x     | 1.02x     | 1.00x      | 1.01x       | 1.00x      |
| c1    | **1.93x**         | 1.85x     | 1.17x     | 1.29x      | **1.93x**   | 1.46x      |
| c2    | **5.07x**         | 12.88x    | 0.91x     | **16.39x** | **126.90x** | 13.13x     |
| c4    | TIMEOUT → 22.1 ms | → 26.6 ms | → 0.23 ms | → 8.3 ms   | → 10.4 ms   | **77.63x** |

Absolute numbers for the two shapes that decide it:

| shape | verb   | R10                     | R11               |
| ----- | ------ | ----------------------- | ----------------- |
| c2    | start  | 75.4 ms                 | 14.9 ms           |
| c2    | stop   | 106.5 ms                | 6.5 ms            |
| c2    | remove | 985.0 ms                | 7.8 ms            |
| c4    | —      | TIMEOUT ×4, 261.6 s CPU | served, 3.4 s CPU |

**`chatty=4` is the headline.** It timed out on both arms of round 10 and timed
out on all four base arms here, burning ~180 s user + ~80 s system each. R11
serves it in 22 ms with 3.4 s of CPU. That is service versus no service, not
faster versus slower — the cliff moved past the shape that defined it.

`chatty=2`'s scatter is gone too. Its base arms ranged 51-101 ms on `start` and
35-2000 ms on `remove`, which is the signature of a shape sitting exactly at the
fold ceiling; R11's four arms hold 12.8-16.8 ms and 7.2-8.3 ms.

One refusal appeared in the entire sweep — a `stop` under backpressure, on a
_base_ arm. R11 recorded zero across all 28 of its arms.

### The two cells below 1.00x

The ratchet rolls back on any degradation, so both were checked against the
spread within each arm's own four runs rather than eyeballed:

| cell       | delta     | within-arm spread | verdict |
| ---------- | --------- | ----------------- | ------- |
| c0 `start` | +0.137 ms | 0.349 ms          | noise   |
| c2 `list`  | +0.021 ms | 0.092 ms          | noise   |

Both deltas are smaller than the run-to-run spread of the arms they come from.
`c0` is the control shape and it should be flat: a quiet fleet has almost
nothing to pack, so anything but ~1.00x there would mean the change had moved
cost around rather than removed it.

### Durability was checked separately

A speed-up that came from storing less is not a speed-up. Against a chatty
fleet, both binaries were stopped and their state read directly:

|     | rows       | mean row         | window                      |
| --- | ---------- | ---------------- | --------------------------- |
| R10 | 6706, 7519 | 625 B, 557 B     | contiguous, fully accounted |
| R11 | 135, 133   | 30.9 KB, 31.4 KB | contiguous, fully accounted |

Every row abuts the next, the concatenated length equals the window width, and
`replay_bytes` matches both. The first version of this check ran the _quiet_
fixture and compared two empty databases — it passed, and proved nothing. A
check that cannot distinguish "correct" from "no data" is not a check.

## Historical follow-up (resolved in schema 5)

The schema creates `replay_chunks_run_start_byte` on `(run_id, start_byte)`
while `UNIQUE(run_id, start_byte)` has already built an autoindex on exactly
those columns. `EXPLAIN QUERY PLAN` shows the planner picking one or the other
per query and no query needing both, yet every insert writes both b-trees.
Ablation puts it at 12.6% of WAL bytes before coalescing. Schema 5 removes the
duplicate index while moving payloads out of SQLite. The pre-stable schema has
no migration path, so older stores are rejected rather than carrying both index
shapes.
