# Replay storage benchmark contract

Status: accepted qualification requirements; production compression and its
end-to-end qualification are not implemented. This specializes the
[Run benchmark standard](benchmark-standard.md), which retains ownership of
measurement, source binding, failures and the nine performance dimensions.
The [protocol](protocol.md) continues to own byte cursors, retention, durability
and lifecycle semantics. No codec, frame size or new policy default is selected
by this document.

## User outcome and grounding

The user goal is to retain more useful original history and serve repeated
reads efficiently, while preserving live responsiveness, ordered exact bytes,
Run ownership and the accepted persistence guarantee. Sequential immutable
output is a suitable candidate for compression, but cold bulk throughput alone
cannot establish this outcome.

The [local store census and geometry experiment](reviews/replay-storage-usage-grounding.md)
found predominantly small recorded extents, substantial index cost, several
simultaneously advancing Runs, many quiet records, and much larger lifetime
output than the retained windows. Its bulk and continuing-context compression
results justify exploring both geometries. They do not qualify a production
codec, the natural output distribution, access frequency or a fleet size.

Classify benchmark inputs explicitly:

| Input class                 | Role                                                                                                                                    | What it cannot establish                                                                               |
| --------------------------- | --------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| Semantic invariants         | Independent expected bytes, half-open ranges, identities, committed watermarks and fail-closed mutation outcomes                        | A historical packet or row size is not an invariant                                                    |
| Observed workloads          | Anonymous local size distributions and passive head movement; public consumer continuation paths                                        | Retention-censored records do not describe the original stream; one host is not the product population |
| Regression cases            | Tiny timer-flushed writes, arbitrary tail cursor, partial final frame, active exec, corruption, uncertain commit and a healthy neighbor | A passing microbenchmark does not close lifecycle or recovery                                          |
| Historical characterization | Existing raw storage and the earlier zlib trials                                                                                        | Old row boundaries, file ceilings or compression rankings must not constrain a better implementation   |
| Held-out workloads          | Separate seeds and Run/corpus partitions fixed before tuning; incompressible and low-repetition content                                 | A trained dictionary tested on its training bytes is not independent evidence                          |

Private terminal contents stay private. Publish anonymous counts, geometry,
method and aggregate results. A behavioral baseline uses independently generated
bytes; private recorded content is optional additional evidence. Identify the
selected serving artifact separately from repository HEAD and an on-disk
replacement. Hello's current version-based buildId cannot distinguish two
same-version builds or prove a planned-exec source transition.

## Separate the boundaries

Distinguish PTY reads, live delivery packets, persistence transactions,
compression blocks/frames, file generations and client wire frames. None
implicitly sets the size of another. A timer flush need not close a frame;
continued compression can retain previous history without rewriting old bytes.
Zstd explicitly distinguishes flush and end in its
[streaming API](https://github.com/facebook/zstd/blob/dev/lib/zstd.h).
Independent frames with an index can support byte-range replay; the
[seekable format](https://github.com/facebook/zstd/blob/dev/contrib/seekable_format/zstd_seekable_compression_format.md)
illustrates this layout. These library capabilities are design inputs, not
proof of ctxmux recovery behavior.

Freeze the existing operating policy in paired storage comparisons. In
particular, compression must not gain its score by extending the durability
collection window, waiting for a frame to fill, reducing live delivery or
making fewer bytes durable. The existing 10 ms collection setting is a
historical operating point, not a 10 ms end-to-end guarantee. Any policy change
is a separately named experiment with user benefit and costs.

## Measurements to add

| Boundary                  | Required measurements                                                                                                                                                        | Behavioral oracle                                                                                                                            |
| ------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| Physical capacity         | Raw retained bytes; encoded payload; live index/table pages; free pages; WAL/SHM; checkpoints; allocated directory bytes; retired/unreferenced generations; compaction peak  | Every retained raw range is readable exactly; count the complete store, not only the compressed payload                                      |
| Append and commit         | Compression CPU; queued/active work; accepted-to-committed lag; actual file writes/syncs and SQLite commits; opens/metadata calls; bytes written per committed raw byte      | Payload is durably published before its index/head; known rollback and unknown-COMMIT outcomes remain distinct                               |
| Read service              | First verified byte and completion latency; verified raw throughput; requested/read/decoded bytes; frame lookups; cache hits/misses and evictions; concurrent decode sharing | Arbitrary cursor and requested length return exact original bytes, including split UTF-8 and ANSI sequences                                  |
| Resource ownership        | Aggregate and marginal RSS/PSS, CPU, fds, contexts, allocated input/output/decode/cache capacity; worker/queue occupancy; physical write amplification                       | Idle Run count must not reserve a full frame/context each; permitted allocations and actual resident cost are separately reported            |
| Fairness and availability | Per-Run/client distributions; independently scheduled sparse output/input/Ctrl+C; owner service and exact input results during floods/reads/maintenance                      | A second real Run/client continues serving through Run-local codec/derived failures; report the actual blast radius of shared storage faults |
| Recovery and churn        | Incomplete-tail startup, state validation, exact committed prefix, startup cost, planned-exec interruption, generation cleanup, remove/retention cost                        | Active exec preserves original Run/child identities; cold replacement recovers historical bytes only, with no invented live revival          |

For every ratio state its numerator and denominator. Payload compression is raw
bytes divided by encoded bytes. Whole-store cost is actual allocated bytes
divided by currently verified retained raw bytes; include metadata, garbage,
working generations and cache accounting rather than treating these as free.
Do not infer fsync counts from file size or process write bytes. Instrument the
owner or use qualified syscall evidence, and report observer overhead.

Logical retention and physical disk funding are separate comparison axes:

- **Equal history:** both arms must retain the same independently verified raw
  ranges under the same durability and resource policy. This isolates storage
  cost, write service and read behavior.
- **Equal physical funding:** compare useful verified history and service at the
  same actual directory allowance. This measures capacity. Record every logical
  retention setting, admission refusal, truncation and working-space requirement.

A codec saving does not increase history when an unchanged raw-byte quota
already clips it. A larger logical quota is a policy arm, not a codec speedup.
Repacking older frames incurs additional writes and peak storage: report that
cost instead of calling the physical path write-once.

## Pre-registered comparison matrix

Before execution freeze the selected cells, repetitions, randomized/rotated arm
order, deadlines, offered schedule, source/build identities, payloads, policy,
cache state and measurement tools in the existing campaign receipt. Every cell
gets an outcome, including unexecuted or unavailable cells. A covering matrix
may replace the full Cartesian product if its interactions and omissions are
explicit. Preserve the original fleet and soak qualification unchanged.

### Candidate controls and geometry

Compare the selected raw production owner, raw owner with transaction-scoped
payload sync, independently compressed frames, and continuing compression with
timer flush. This separates codec benefit from sync/index improvements. Closed
frames and active frames have separate correctness and recovery evidence.

The initial codec exploration includes LZ4, LZ4HC, Zstd and zlib as a historical
control. Record library version, settings, window, dictionary, frame overhead,
checksum, context memory and finalization. Start with maintained defaults and
predeclared CPU/ratio alternatives. An unexplained level is not a production
default. Raw representation for incompressible data is a lossless candidate,
with selection overhead and observable representation included in results.

An initial frame sweep of 64 KiB, 256 KiB, 1 MiB and 4 MiB explores geometrically
larger units above the observed tiny extents, including the current append work
unit and larger disk units. These are experiment coordinates, not format,
population or history limits. Expand or narrow the candidate search with recorded
evidence; never remove a failing acceptance workload. Report gains that plateau
as well as tail/decode/working-set costs. Delivery remains within the public
wire-frame bound regardless of disk frame size.

### Write shapes

- Replay-sized fragments near the observed 128/512/1024-byte ranges, with
  both paced and bursty schedules; these are recorded-extent shapes, not a claim
  about native PTY read sizes or exact producer arrival times.
- Large binary/text/ANSI bursts, repeated TUI redraws, low-repetition output and
  incompressible deterministic bytes; do not optimize only repetitive terminal
  content or a training corpus.
- A quiet Run's short final suffix, long idle then append, output below every
  frame target, and time-flushed continuing frames. Visibility and committed
  progress cannot depend on filling the target.
- Many simultaneous small producers interleaved in the shared file; asymmetric
  mixed Runs; one high-volume Run; start/exit/stop/remove during append and
  compaction. Preserve the original 4000-Run/1000-stream soak and its held-out
  frontier; local usage does not authorize reducing it.

### Read shapes

| Shape                       | Required contrast                                                                                                                   |
| --------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| Sequential retained history | Full replay and paged traversal; one decoder continuing across pages versus repeated decode work                                    |
| Arbitrary cursor and tail   | Inside a frame, at its boundary, and near the latest committed byte; short reads and a large requested suffix                       |
| Terminal continuation       | Checkpoint plus raw suffix, output/resize ordering, and an active final frame                                                       |
| Repeated readers            | Fresh clients and returning clients on the same Run; 1/8/32 fan-out; separate Runs sharing the decode budget                        |
| Mixed service               | Slow reader beside fast reader; historical reads during live output, input and Ctrl+C; retention advancing during replay            |
| Cache state                 | No application decode cache, cold process after private restart, and warmed repeated reads; disclose OS page-cache state separately |

For short-read exploration select 1-byte, 4 KiB, 64 KiB and 1 MiB requests to
expose boundary correctness and read amplification. These request coordinates
do not become API limits. Measure decoded bytes per requested verified byte and
first-byte latency; a fast full-frame decoder can still give poor tail service.
Never globally purge the user host's caches to manufacture a cold sample.
Where true OS-cold evidence is unavailable, say so.

### Failure, durability and lifecycle cases

Use private daemons and owner-controlled fault points. Never signal, restart,
resize or send input to the user Runtime to obtain qualification.

Cover codec failure before output, partial compressed writes, flush before file
sync, file sync before index commit, proven rollback, unknown COMMIT, response
loss after commit, interrupted frame closure, planned exec with an active frame,
and interrupted generation compaction. Previously committed ranges must remain
exact. Committed extents carry enough integrity and publication information to
distinguish a recoverable unfinished tail from missing/corrupt committed bytes.
Do not treat an incomplete library frame as automatically lost history, or
silently accept a referenced corrupt frame. Validate declared lengths, offsets,
checksums and decoder resource demands before trusting them.

During a local codec/reader/derived-view failure, qualify two real Runs and two
public clients, exact input/output and Ctrl+C. Record shared-store corruption or
uncertain commits according to the protocol's actual fail-closed boundary,
separately from a Run-local error. A storage fence cannot be concealed by cached
Running or converted into a child-exit claim.

Process-crash injection qualifies process recovery, not physical power-loss
durability. Retain the established file/directory and SQLite FULL durability
ordering unless an explicit product decision changes it. Planned exec may close
an active frame at the owner barrier; it must not depend on preserving opaque
library memory across binary generations.

## Acceptance and optimization objective

Correctness, truthful outcomes, owner containment, durability and the original
accepted workloads are hard requirements. Preserve all existing qualified
regression budgets. Do not create a new tolerance by multiplying this pilot's
worst result, and do not use a fixed compression ratio as a pass condition.

Evaluate paired, repeated measurements as a vector: whole-store capacity,
append/commit cost, cold and warm read service, tail responsiveness, CPU,
resident/working memory, fairness and recovery. Publish wins, ties, losses and
unqualified cells. A weighted aggregate score cannot hide a regression. State
uncertainty and any necessary tradeoff before selecting a production candidate.
Exploration can proceed before the full fleet join, but only the exact joined
candidate can claim end-to-end qualification. Existing failed soak receipts
remain failures until the original workload passes on that candidate.
