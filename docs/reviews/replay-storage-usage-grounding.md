# Local replay usage grounding

This is anonymous, read-only evidence from one installed local Runtime. It
informs the [storage benchmark contract](../replay-storage-benchmark.md); it is
not a product capacity limit, a natural workload census or a performance SLA.
Raw receipts remain private under `.bagakit/storage-grounding/`. No terminal
content, command, personal identifier, machine path or operational wall-clock
timestamp is published here.

## Method and source boundary

The process census found one installed product daemon and five development
fixture daemons. Only the installed product store supplied these workload
statistics; fixture populations were excluded. SQLite was opened with
`mode=ro` and `query_only`, retaining live WAL visibility. Numeric queries ran
inside a short read transaction; filesystem measurements were separate and
are explicitly not atomic with it. Neither public mutating operations nor
SQL writes, checkpointing, signals, restart or user input were performed.

The initial snapshot was bracketed by an installed manifest and matching binary
hash for source `9044f62`. During the later census/30-second passive observation,
another authorized owner replaced the on-disk package with `f28dff8`. Hello kept
the same daemon identity and `ctxmuxd/0.1.0` buildId. That version-based field and
same PID cannot identify a same-process exec. The installation owner's subsequent
kernel code-signature observation matched the mapped `9044f62` image and
distinguished it from the installed `f28dff8` image; the owner reported no native
exec or cold replacement. That independent receipt resolves the on-disk/source
ambiguity without treating Hello as a binary hash. This remains observational
workload evidence, not controlled throughput or planned-exec acceptance.
Repository HEAD does not describe the executing artifact; default-quota
simplification was not inferred to be active.

The reusable census is `scripts/bench/ground_storage.py`. Its later SQL snapshot
took about 1.4 seconds including the grouped extent scan; observations themselves
can affect a live host. The small content geometry pilot ran afterward, with
source coordinates from the initial snapshot and exact raw-byte checks. It did
not write terminal content into artifacts. Observer CPU/I/O and host effects
are not removed to turn these observations into a controlled baseline.

## Observed shape

| Fact                      | Initial snapshot                     | Later census                         | Interpretation                                                                |
| ------------------------- | ------------------------------------ | ------------------------------------ | ----------------------------------------------------------------------------- |
| Retained Runs             | 75                                   | 76                                   | Includes terminal records; this is not the live population                    |
| Stored lifecycle          | 15 running, 8 exited, 52 interrupted | 15 running, 9 exited, 52 interrupted | Running is not proof of input/output service availability                     |
| Lifetime output heads     | 936,849,488 B                        | 944,495,117 B                        | Summed lifetime cursors, not current physical storage                         |
| Currently retained bytes  | 162,616,259 B                        | 163,508,237 B                        | About 155–156 MiB available historical payload                                |
| Retained extent rows      | 481,133                              | 480,294                              | Index population remains substantial after externalizing payload              |
| Extent median / p90 / p99 | 203 / 453 / 4,796 B                  | 204 / 455 / 4,760 B                  | Many small recorded extents; producer/PTY arrival sizes remain unknown        |
| Extents at most 512 B     | 92.36% of rows; 54.71% of bytes      | 92.29% of rows; 54.59% of bytes      | Bulk-only compression tests miss the common append geometry                   |
| Runs with retired prefix  | 35                                   | 36                                   | Historical output is retention-censored                                       |
| Largest retained Run      | 4,194,304 B                          | 4,194,304 B                          | Installed policy/window; not a legitimate Session output limit                |
| Largest lifetime head     | 109,685,630 B                        | 109,685,630 B                        | One Run already produced over 100 MiB; larger history workloads are justified |

Both snapshot populations and individual values are retained; the new census
does not overwrite the earlier observation. Initial extents had no overlapping
physical ranges or raw-length mismatches. Later indexed bytes exactly matched
summed retained bytes, and referenced files were present and sufficiently long
at filesystem inspection. This is metadata consistency, not a complete read
verification of the user history.

An independently sampled SQLite `dbstat` assigned 68,239,360 B to the replay
coordinate table and 26,914,816 B to its unique index: about 90.75 MiB combined.
The main database file was 101,572,608 B; its later page view included 1,367 free
4096-byte pages. These are different accounting views, not additive costs.
The later replay generation file was 492,040,243 B, versus 163,508,237 B of live
indexed payload. It includes retired/unreferenced bytes pending reclamation.
Compressed payload alone would leave index, WAL and compaction costs untouched.
Measure their aggregate rather than extrapolating a whole-store win from a codec
ratio. The physical snapshot is not a corruption diagnosis or an exact garbage
reclamation promise.

Six passive samples over approximately 30 seconds, at five-second resolution,
observed 9–10 advancing durable heads and 66–67 unchanged records per interval.
Same-identity head increments totaled 767,792 B; no observed head regressed and
no records entered or left during that window. This is committed progress, not
producer throughput, live latency, timer cadence, fsync count or attachment read
demand. The observation does not prove that all quiet Runs were healthy or idle.

A subsequent public paged List observed 76 Runs: 64 with no attachment, 11 with
one, and one with two, totaling 13 attachments. This supports detached storage
and the common single-view case in addition to fan-out qualification. It is
one attachment snapshot, not the number of distinct clients over time, read
frequency or a reason to reduce the required 1/8/32-client benchmark lanes.

## Content geometry exploration

Twelve Runs were selected at evenly spaced retained-size ranks, alternating
retained prefix and tail, with an observation cap of 1 MiB per selected Run.
The sample contained 7,154,811 B across 23,945 original recorded fragments.
The cap selects exploratory data and is never a production history limit.
zlib runtime 1.2.12, level 6, one repetition, in-memory processing:

| Geometry                                               | Encoded payload bytes | Raw / encoded ratio |
| ------------------------------------------------------ | --------------------- | ------------------- |
| Each recorded fragment independently                   | 2,735,206             | 2.62×               |
| Independent 64 KiB frames                              | 623,321               | 11.48×              |
| Independent 256 KiB frames                             | 548,370               | 13.05×              |
| Independent 1 MiB frames                               | 530,009               | 13.50×              |
| Continuing stream, flush after every recorded fragment | 718,132               | 9.96×               |

Every arm decoded to exactly the original selected bytes. The continuing-stream
arm retained compression history across flushes and finalized each sample at
the end. It did not recompress earlier output. These results refute the blanket
assumption that useful compression requires one unseekable whole-file stream
or that small flushed appends must reset compression history.

They do not select zlib over LZ4/Zstd, establish confidence intervals, test disk
I/O or crash recovery, or prove a user read-speed improvement. Recorded fragments
are not ten-millisecond producer timestamps. The selected content is censored
by existing retention, has no independently held-out codec comparison here,
and may contain much more repetition than another legitimate workload.
Only payload compression was measured; none of these ratios is a whole-store
capacity gain.

## Optimization implications and missing evidence

1. Preserve compression history across small appends while testing independent
   indexed frames for byte-cursor and tail reads. This has local supporting
   evidence; frame size remains an experimental choice.
2. Make transaction-scoped payload synchronization and index density explicit
   controls. Hundreds of thousands of coordinate rows and current per-extent
   sync calls are separate causal costs that codec selection cannot remove.
3. Fund active contexts and decoded caches globally. A large frame per sparse
   Run must not imply a preallocated large buffer per retained Run.
4. Qualify actual whole-store and compaction cost. Retired generation bytes and
   SQLite metadata can dominate after successful payload compression.
5. Include long histories, many sparse producers and mixed quiet/advancing Runs.
   Preserve the already accepted large fleet and continuous-output workload;
   one local desktop does not set the project's scale goal.

Actual read-frequency, requested range sizes, cache reuse, attachment churn,
producer arrival timestamps and per-transaction sync counts are not available
from these records. Public consumer code establishes cursor replay and terminal
checkpoint-plus-suffix paths, but does not establish their statistical frequency.
Collect that evidence from opt-in client/owner instrumentation or an isolated
production-shaped benchmark, never infer it from database rows. Held-out binary,
low-repetition, sparse and failure cases must accompany further tuning.
