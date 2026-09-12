# Transaction-owned replay sync qualification

The first storage optimization removes redundant durable file synchronization.
It keeps original output bytes, SQLite `FULL` durability, the existing collection
window and all resource policies. It adds no codec, protocol field or capacity
ceiling. The owner is the single persistence actor, not the PTY reader or a client.

## Cause and implementation

The raw baseline is commit `6e27fdc289cdfbde9c41b5eea4722ba4dc30e2f0`.
It already groups output into transactions, but creates a separate replay writer
for each participating Run. Every row reads the file length, appends bytes and
calls `sync_data`. Many small independent Runs therefore synchronize the same
shared file repeatedly before a single SQLite commit.

The output transaction now opens that file once, obtains its initial offset and
passes the writer to each Run's existing replay logic. Successful writes advance
the offset; one sync follows all staged payload writes and precedes index commit.
Watermarks still publish only after confirmed commit. A proven rollback can
truncate the unindexed tail. An uncertain commit, failed rollback or failed
post-commit inspection preserves every possibly indexed byte and requires the
existing explicit failure/recovery path. A destructor cannot make that decision.
The handle closes before adaptive transaction splitting retries work.

This removes the redundant per-Run writer, per-row stat/sync and writer-owned
rollback state. It keeps pruning, overlap verification, admission, SQLite commit
classification and source-gap reporting at their existing owners. Empty payload
issues no replay sync. A metadata-only output settlement now holds one transient
replay descriptor; that is a small additional open/validation cost rather than a
new permanent descriptor or worker per Run.

## Matched local measurements

These are macOS arm64 qualification probes, not fleet capacity requirements.
Each arm starts 32 real private Runs with two client objects and original raw-byte
oracles. A FIFO releases simultaneous child output. Before each timed release,
the arm acknowledgments are byte-verified and durable. The completion clock stops
after all output is delivered and public status observes every original Run's
durable head at the expected byte count. This includes generator, full frame
recording and status observation overhead; it is not a direct fsync latency.

Three replicas use counter/candidate, candidate/counter, counter/candidate order.
Each replica contains eight short-message releases and two binary bursts. The
short-message summary first takes each replica's median, then the median of the
three replicas. Burst summaries use the three complete burst observations.
Deadlines and offered workloads are identical, and no timed arm uses the syscall
observer. All full samples, failures and cleanup evidence remain in the private
Feature Tracker packet.

| Offered output at release                           | Baseline durable completion median | Candidate median | Reduction | Baseline / candidate worst replica |
| --------------------------------------------------- | ---------------------------------: | ---------------: | --------: | ---------------------------------: |
| 32 short messages                                   |                          314.40 ms |         26.87 ms |    91.45% |                  384.58 / 31.63 ms |
| 262,157 binary bytes per Run, plus headers          |                        2,074.51 ms |        403.28 ms |    80.56% |               2,311.65 / 700.42 ms |
| Held-out 524,311 binary bytes per Run, plus headers |                        3,546.92 ms |        905.84 ms |    74.46% |             3,620.59 / 1,207.33 ms |

Live delivery medians change from 2.65 to 2.20 ms, 500.19 to 396.89 ms and
1,048.87 to 898.88 ms respectively. The main improvement is durable catch-up;
these three replicas do not establish a fleet SLA or a tail-latency percentile.

Separate observation arms execute the real payload synchronization syscall and
record its successful return for the actual replay descriptor. On this Rust/OS
combination, `sync_data` uses `fcntl(F_FULLFSYNC)`, as the
[Rust implementation](https://github.com/rust-lang/rust/blob/1.96.0/library/std/src/sys/fs/unix.rs)
specifies. This is a syscall count, not a count of physical flash writes.

| Observation release                         | Baseline payload sync calls | Candidate calls |
| ------------------------------------------- | --------------------------: | --------------: |
| Each of eight 32-Run short-message releases |                          32 |               1 |
| First binary burst                          |                         162 |               9 |
| Held-out binary burst                       |                         291 |              17 |

Transaction counts and row boundaries depend on actual arrival geometry, so the
burst counts are observations rather than fixed acceptance targets. The owner
test independently proves one successful sync for one transaction containing
four rows across two Runs; an empty replay proves zero syncs.

## Costs and limits

These measurements do not show improvement in every dimension. At the final
burst boundary, the largest observed daemon RSS is 355,248 KiB in the baseline
and 363,504 KiB in the candidate, a 2.32% increase. Boundary RSS is not peak RSS;
the faster arm also reaches that boundary sooner. The maximum observed allocated
store size at that boundary is 25,432,064 versus 25,579,520 bytes, a 144 KiB
increase. These include the database, WAL/SHM, replay files and checkpoint files
present at that observation, and must not be described as compression savings.
The checkpoint/row/WAL arrangement is time-dependent. This probe cannot assign
those differences solely to the writer change.

The daemon's cumulative CPU-time median at the final boundary is 3.65 versus
1.20 seconds; startup and earlier phases are included. CPU, RSS and allocation
figures describe this local probe, not worst-case host funding or a production
capacity certificate. Keeping the old per-row owner is the measured counterfactual;
extending the collection window or weakening synchronization was rejected because
neither is needed to obtain the improvement. Fleet peak resources, read cost,
fairness and sustained I/O still require the [storage benchmark matrix](../replay-storage-benchmark.md).

## Behavioral and failure proof

- All 86 persistence unit tests pass on the selected candidate, including the
  original adaptive page admission, compaction, fragmented retention, overlap,
  backpressure, uncertain commit and cold recovery cases.
- A new two-Run binary test forces four physical rows, proves one file open and
  one successful payload sync, then reopens and compares every ordered byte.
- Sync failure and proven rollback restore the previous tail and leave the
  same store usable for the identical retry. Already-committed error and
  post-commit inspection failure preserve the indexed suffix without advancing
  the reported watermark; cold recovery reads its exact bytes.
- A SQLite temporary trigger performs a real automatic rollback during replay
  insertion. The owner's subsequent real `ROLLBACK` fails. The mutation reports
  that uncertainty, leaves the payload tail and watermarks intact, and cold
  recovery removes only unindexed bytes while preserving both original prefixes.
- Public counter and candidate probes each use two original real Runs and two
  client objects. They verify held-out binary output, a binary input echo
  containing all byte values, exact applied-input receipts, Ctrl+C, disconnect/
  reconnect, original Run/PID/spec identity and cold history recovery with a new
  daemon epoch and unchanged runtime identity. A third private Run proves two
  interactive CLI input/Ctrl+C/attach/detach rounds. All cleanup uses normal
  public Stop/Remove; no user Run is contacted or replaced.
- An independent decoder checks every recorded raw output extent against bytes
  regenerated from the offered workload, plus continuity and recovery streams.
  It checks all ten accepted public, geometry and timing packets rather than
  accepting their success flags. The two clients are a protocol/transport proof,
  not a new AgentMux UI acceptance claim.
- Formatting and strict Clippy pass after separately repairing the pre-existing
  tmux pause fixture's function-length annotation and unchecked receipt-length
  conversion. Its gap, continuation and healthy-Run oracle stays unchanged.

Counter and candidate have separate build-output directories. Actual compilation
logs and frozen executable hashes bind executed artifacts to selected sources.
Initial sample mistakes, static failures, a public proof with incomplete source
binding, and observer attempts that watched the wrong syscall or mismatched clock
origins are retained and excluded. The clock correction separates elapsed-time
measurement from the shared OS clock used to place syscall events; it does not
alter any duration, workload or success budget.

This closes the transaction-sync slice. It does not qualify physical power-loss
behavior, lossless compression, arbitrary-cursor compressed reads, exec upgrade,
the original Linux fleet/soak, or downstream AgentMux integration. Those keep
their existing acceptance owners and failures.
