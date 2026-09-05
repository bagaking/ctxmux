# Round 12: the reactor is the new ceiling, and making it faster made things worse

- Status: **rolled back**. Failure record + root cause, 2026-09-14
- Host: cn3 (`n36-001-230`), 64-core Linux, ext4
- Harness: `docs/verification/harness/armlib.sh`; conventions
  [§4.3](../benchmark-comparison-conventions.md)
- Base: `92cfadf` (round 11 coalescing). Nothing merged.

## What round 11 left, and what actually happened to it

Round 11 cut WAL amplification from ~2.4x to ~1.05x by packing contiguous
output into 64 KiB rows. The prediction was that real output would rise by that
factor. It did, exactly — and then stopped somewhere new:

| chatty | AMP        | real out | fold    |
| ------ | ---------- | -------- | ------- |
| 2      | 1.87x      | 24 MB/s  | 45 MB/s |
| 4      | 0.95-1.23x | 53 MB/s  | 53 MB/s |
| 8      | 0.90-1.08x | 52 MB/s  | 54 MB/s |
| 16     | 1.06-1.13x | 53 MB/s  | 57 MB/s |

Amplification is spent: there is no factor left to reclaim. And chatty 4, 8 and
16 deliver the _same_ 53 MB/s, which is a hard ceiling rather than a workload
levelling off.

## The candidate round 11 named is dead

Round 11 deferred dropping `replay_chunks_run_start_byte`, the index that
duplicates the autoindex `UNIQUE(run_id, start_byte)` already builds, worth
12.6% of WAL bytes. That number was measured against ~200 B rows. Re-priced
against the row sizes coalescing actually produces:

| row size | AMP with dup | without | the index costs |
| -------- | ------------ | ------- | --------------- |
| 208 B    | 6.65x        | 5.01x   | 24.6%           |
| 4 KiB    | 1.87x        | 1.66x   | 11.0%           |
| 32 KiB   | 1.30x        | 1.24x   | 4.5%            |
| 64 KiB   | 1.26x        | 1.22x   | **3.6%**        |

Rows are now ~31 KB on cn3, so the standing candidate is worth 3.6%, not 12.6%.
That does not buy a `SCHEMA_VERSION` bump. Dropped, not deferred.

The general lesson is worth more than the specific number: **a deferred
candidate's value was measured in the world before the change that deferred
it.** Re-price it before spending the bump.

## Where the ceiling moved

Per-thread CPU, eight guarded arms, both orders, every arm idle at start:

| shape | `ctxmux-native-o` (reactor) | `ctxmux-persiste` (actor) |
| ----- | --------------------------- | ------------------------- |
| c2    | **98.7% / 98.5%**           | 37.4% / 36.4%             |
| c8    | **97.6% / 97.7%**           | 54.5% / 53.6%             |

Rounds 10 and 11 fixed the persistence layer, and it is no longer the binding
constraint: the actor has half a core spare. The daemon-wide reactor thread is
pinned. 53 MB/s is what one saturated thread delivers.

Per-thread was necessary, not decorative. A daemon-wide total of ~150% cannot
distinguish one saturated thread from eight at 19% each, and those are opposite
diagnoses.

`perf --call-graph dwarf` on that one thread, symbols restored by rebuilding
with `CARGO_PROFILE_RELEASE_DEBUG=1` (the shipped binary is stripped, and a
stripped profile is just a list of addresses):

| symbol                                              | self  | inclusive |
| --------------------------------------------------- | ----- | --------- |
| `OutputLog::replay::{{closure}}` → `retained_after` | —     | **29.3%** |
| `Vec::from_iter` (the `collect`)                    | 22.4% | 23.1%     |
| `OutputLog::push`                                   | 1.3%  | 6.3%      |

`OutputLog::replay` owns more than half the thread that owns the ceiling.

## The change, and why it looked right

`replay(after_byte)` filtered the whole deque from the FRONT, while the hot
caller — `record_output` rendering from the newest accepted append — wants the
chunk at the BACK. At the 4 MiB per-Run cap and a few hundred bytes per PTY
read that is ~10k chunks visited per push, all but the last discarded.

`chunks` is sorted and gapless by construction (`push` appends,
both trims only pop the front), so the dead prefix is a `partition_point` away:

```rust
fn first_chunk_after(&self, after_byte: u64) -> usize {
    self.chunks.partition_point(|chunk| chunk.end_byte <= after_byte)
}
```

244 lib tests green, clippy clean. The new test compares every cursor over a
400-chunk log against the linear filter it replaced, before and after a trim,
and was **verified by mutation**: `after_byte.saturating_add(1)` in the
predicate fails it at cursor 6.

The optimization worked. Reactor user time at c8 fell from 1.79-1.84 s to
0.81-1.15 s — a ~40% cut, right where the profile said it would land.

## Why it was rolled back anyway

| shape | verb   | R11     | R12      | pairs lost |
| ----- | ------ | ------- | -------- | ---------- |
| c2    | stop   | 7.01 ms | 9.27 ms  | **4/4**    |
| c2    | remove | 7.76 ms | 11.65 ms | **4/4**    |
| c8    | start  | 35.5 ms | 46.1 ms  | **4/4**    |
| c8    | stop   | 9.76 ms | 21.0 ms  | **4/4**    |
| c8    | remove | 13.1 ms | 16.3 ms  | **4/4**    |
| c16   | stop   | 10.8 ms | 35.2 ms  | **4/4**    |
| c16   | remove | 14.6 ms | 23.7 ms  | **4/4**    |

Every one of those loses all four pairs in **both orders**, which beats the
largest known confound on this host (the second arm runs ~1.41x faster). `c0`,
the control, is flat — as it must be, since a quiet fleet has no prefix to skip.

### A methodology correction, recorded because it nearly inverted the verdict

My first aggregator called almost all of this "noise": it compared each delta
against the pooled within-arm spread, and the after-arms scatter so widely on
their own that a +24 ms median hid under a 36 ms spread. That test asks the
wrong question. The fixture runs base and after **adjacently in both orders**,
so the arms are PAIRS, and the statistic is the sign of each pair — not the
magnitude against a pooled spread. Round 11's two sub-1.0 cells were genuinely
noise by the spread test _and_ split their pairs; these lose every pair.

**A spread test on paired data throws the pairing away**, and pairing is the
only reason a noisy host is measurable at all.

## Root cause: the gain and the loss are the same mechanism

|                    | R11 base    | R12 candidate   |
| ------------------ | ----------- | --------------- |
| reactor utime (c8) | 1.79-1.84 s | **0.81-1.15 s** |
| reactor stime (c8) | 0.54-0.64 s | **1.76-1.95 s** |
| reactor total      | 97.6%       | 89.9-92.0%      |
| persistence actor  | 54.5%       | **60.7-62.1%**  |
| bytes written      | 378 MB      | **421-455 MB**  |

The cycles the binary search saved were not returned to the host. The reactor
spent them **reading more PTY output** — system time tripled and 12% more bytes
reached the disk in the same window. A cheaper render does not make the thread
idle; it makes it a faster producer, and everything downstream gets a deeper
queue. `stop` and `remove` block on that queue.

So the change did exactly what it was designed to do, and that is _why_ the
lifecycle verbs got worse. This is not a bug in the patch to be fixed by a
better patch.

**This is the second time this exact shape has appeared.** Round 9 made
rendering 11x cheaper and `remove` got 2.16x slower, for the same reason. The
first instance was read as a surprise. It is not a surprise; it is the
structure of this system: _the reactor is the fleet's rate limiter, and
speeding it up without bounding what it admits converts latency headroom into
queue depth._

## What this implies for the next round

The cheap-render family of optimizations is exhausted as a way to improve the
chatty shape, and the reason is structural rather than incidental. Any future
change that makes the reactor cheaper per byte must be paired with something
that keeps the extra throughput from landing in the persistence queue —
otherwise it will reproduce this result. Candidates, none measured:

- **Bound admission, not render cost.** The reactor already skips the render
  when `queue_has_room()` is false. It does not slow down _reading_, so the
  fleet's producers are never told to wait. Backpressure that reaches the PTY
  is the missing piece.
- **Give the lifecycle verbs a path that does not queue behind output.** This
  was examined in round 8 and rejected — `Barrier` semantics make the FIFO
  position the guarantee — but the rejection predates the actor having half a
  core spare.
- **Split the reactor.** One thread for all PTYs is the ceiling by
  construction. This is an architecture change, not a tuning change, and it
  needs its own round.

## Still open, unchanged from round 11

Chatty rows in the comparison table still have no tmux counterpart, so no ratio
can be published for them. The long-lived-client caliper still does not exist.
