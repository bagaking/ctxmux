# 019 — Resource policy and honest qualification

- Status: accepted and implemented; local validation recorded in Feature Tracker, scale qualification remains separate
- Scope: daemon resource ownership, storage admission, recovery, and qualification objectives
- Supersedes: population/format quota and fixed budget policy in decisions 009 and 013; preserves their owner boundaries and historical evidence

## Decision

Run count is not a durable-format invariant. Production defaults have no fixed
128, 4096, or 4000 Run ceiling. Live admission funds actual native ownership and
host descriptors, independently of retained terminal history. The kernel can
still refuse a PTY. Retained records fund resident metadata; an operator may
also set `live_runs` or `retained_runs`. A terminal candidate cannot fund a live
slot until its physical owners have closed. Existing exact-key replacement,
commit-unknown fences, and fully quiescent candidate rules remain authoritative.

`ctxmuxd --resource-limits '<JSON>'` overrides `CTXMUX_RESOURCE_LIMITS`; omitted
fields use the table below. Unknown fields, zero budgets, representation overflow,
incomplete database pages and an unfunded WAL frame fail before owner creation.
The TypeScript SDK exposes the same policy as activation `resourceLimits`.
Policy applies when a daemon starts; connecting to an existing Runtime does not
reconfigure it. Planned exec preserves the complete established policy in its
versioned handoff manifest and explicit incoming CLI arguments.

| Field                       | Default | Owner and consequence                                                                                                          |
| --------------------------- | ------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `live_runs`                 | null    | Optional operator live-owner quota; host FD/PTY funding still applies                                                          |
| `retained_runs`             | null    | Optional retained/projected record quota; exact quiescent replacement at pressure                                              |
| `metadata_bytes`            | 64 MiB  | Registry serialized metadata, actual String/Vec capacities, conservatively funded BTreeMap nodes and fixed owner/index costs   |
| `hot_output_bytes`          | 1 GiB   | Aggregate hot replay payload; reclaim offered history and backpressure only unoffered persistent bytes                         |
| `run_output_bytes`          | follows hot aggregate | Optional independent per-Run hot replay quota; omitted quota follows `hot_output_bytes`                                       |
| `live_event_bytes`          | 64 MiB  | Shared broadcast backing and one leased payload across receivers; attach refusal, Gap or observation discontinuity at pressure |
| `durable_replay_bytes`      | 256 MiB | Aggregate disk replay history, independently of hot cache                                                                      |
| `durable_run_output_bytes`  | follows disk aggregate | Optional independent per-Run disk quota; omitted quota follows `durable_replay_bytes`, with a monotone lifetime head           |
| `database_bytes`            | 384 MiB | SQLite page funding, not a Run-count format limit                                                                              |
| `wal_checkpoint_bytes`      | 8 MiB   | Maximum staged transaction page charge and committed baseline checkpoint trigger                                               |
| `control_state_bytes`       | 128 MiB | Shared Input payload/result and Stop receipt leases; refusal before effects, duplicate lookup before new admission             |
| `handoff_input_bytes`       | 128 MiB | Input receipt payload carried intact across exec                                                                               |
| `handoff_diagnostic_bytes`  | 16 MiB  | Aggregate handoff diagnostics                                                                                                  |
| `handoff_bytes`             | 256 MiB | Serialized manifest admission before relinquishing native ownership                                                            |
| `creation_workers`          | 8       | Concurrent launch/physical-overlap owners; awaited admission, host FD reserve follows this policy                              |
| `input_turn_commands`       | 64      | Completed commands before yielding a reactor turn; pending original commands remain owned                                      |
| `input_turn_bytes`          | 256 KiB | Written bytes before yielding a reactor turn; not a payload limit                                                              |
| `stop_admission_timeout_ms` | 250 ms  | Cleanup-slot acquisition before a Stop side effect; explicit refusal on unavailable capacity                                   |
| `diagnostic_queue_bytes`    | 64 MiB  | Shared formatting, queued and active diagnostic allocations; producers never wait for stderr                                   |
| `diagnostic_record_bytes`   | 1 MiB   | One diagnostic record's configurable format budget; whole-record refusal is observable                                         |
| `cleanup_workers`           | 8       | Blocking session cleanup workers                                                                                               |
| `finalize_workers`          | 8       | Blocking durable terminal publication workers                                                                                  |
| `input_queue_commands`      | 1024    | Per-Run pending input commands, additionally byte-funded                                                                       |
| `input_queue_bytes`         | 4 MiB   | Per-Run pending input payload                                                                                                  |
| `input_result_entries`      | 256     | Retained duplicate-result ledger window                                                                                        |
| `input_result_bytes`        | 1 MiB   | Per-Run retained input request payload, additionally globally funded                                                           |
| `tmux_discovery_bytes`      | 128 KiB | Bounded discovery subprocess output; explicit configurable rejection                                                           |

Per-Run history has no independent default 4 MiB ceiling. Omitting the two
per-Run quota fields derives their effective allowance from their respective
aggregate owner; an explicit quota remains an operator choice. Input queue
capacity is independent and keeps its own byte budget: pending input is work
not yet applied, unlike already observed output history.

The existing 1 GiB hot and 256 MiB durable aggregate defaults are still historical
operating points, not suitable-scale claims. Removing the per-Run ceiling does
not qualify those aggregate defaults for large fleets, provide permanent
archival, or establish a throughput improvement. The original qualification
policies, including any explicit 4 MiB per-Run quota, remain frozen in their
historical workloads; a changed default is a distinct capacity-policy arm.

These defaults are operating points carried forward from the repository's
existing resource qualification, not universal optima or auto-research goals.
They are independently configurable; no default is forced above a fixture's
working set. Immutable recovered specs shed deserialize growth slack so the same
admitted policy funds cold recovery and planned exec. Resource policy bounds owned logical allocations/work, not total
process RSS: allocator overhead, library/runtime state, kernel buffers and
socket transport also require measured RSS/FD/CPU qualification. A lower cost
only counts as improvement when the same work and truth guarantees pass.

## Independent history owners and terminal settlement

Hot replay and durable replay retain independent windows. A final replay can
legitimately span a prefix storage has already committed and retired. Terminal
settlement skips only that retired prefix, keeps strict equality verification
of retained overlaps and commits the contiguous new suffix. It first drains
the terminating Run's accepted queued prefix, without a timed collection wait
or an unrelated global drain. Ordinary replay mutation and unknown COMMIT
outcomes retain their existing fail-closed behavior. Qualification sizes and
retention probe sizes do not change production budgets.

## Storage and recovery

SQLite format arithmetic has a separate source: this store uses 4096-byte pages,
a 32-byte WAL header and 24-byte frame headers. The total WAL allowance is twice
the configured page-charge/checkpoint window. SQLite's WAL index uses 32 KiB
blocks, 4062 frame slots in its first block and 4096 thereafter. SHM funding is
derived from the permitted WAL frame count rather than an unrelated 4 MiB cap.
The state-file allowance funds the database, WAL, SHM, three replay windows
(source, replacement, retained payload) and one 1 MiB append work unit.

A valid oversized WAL is checkpointed during recovery; its size does not make
it corrupt. Structural checks still reject invalid schema, unsafe file ownership,
missing/short referenced data, invalid JSON/cursors and inconsistent accounting.
Valid retained state that does not fit a new operator policy is preserved and
reported as resource pressure with the required budget. Startup does not evict
history merely to fit a smaller policy.

Every coordinate migration, replay-prefix reclamation and output metadata write
uses spill-disabled staging, exact dirty-cache page measurement, proven rollback
on insufficient funding and adaptive batch reduction. Payload bytes are synced
before publishing their coordinates. Unknown COMMIT/rollback outcomes stop all
later mutation and maintenance; recovery reads SQLite's durable truth. Files
which may have been published survive an uncertain result. Unreferenced file
retirement is deferred cleanup; actual directory bytes still gate later append
funding. Completed migration cannot be turned back into a migration obligation
by an unlink failure.

Hot-cache eviction cannot remove unoffered output. A full append queue pauses
the affected reader; queue-space wakes re-offer quiet debt. The native owner checks funding for its fixed 8 KiB read buffer without
trimming history. EOF/EIO changes no retained window; an actual read reclaims
only its actual cost, preserving the exact funded suffix. Cache blocks are at
most 64 KiB and a logical head cursor avoids recopying a block on tiny trims.
The cache quota counts available payload; one partially consumed block and Vec
allocation slack are bounded working storage, included in measured RSS. Ordinary
append/handoff offers copy at most 64 KiB, and advance their offered watermark
only through those copied bytes. Cache trimming never tells the disk actor to
trim otherwise available durable history. Fatal persistence preserves the last
confirmed durable head, marks an observation discontinuity and permits actual
child reap/terminal publication; failed appends explicitly refuse acceptance.
Known durable failure rejects upgrade before descriptor extraction. Upgrade storage
waits remain Ctrl-C cancellable without a deadline that discards durability. It cannot strand an already-reaped Run forever
behind a budget it will never be able to commit.

Cold recovery hydrates at most the configured hot payload budget. Attach reads
older available durable bytes in bounded actor-owned pages. Protocol generation
18 adds `replay_window { first_available_byte, latest_output_bytes }` during
initial replay: if concurrent retention advances the floor, both clients discard
the partial assembly and resume the newer contiguous suffix through the original
advertised head, marking truncation. It is never encoded as a live Gap recovery
cursor. Source-gap facts are retained with the private durable lifecycle JSON
envelope at atomic terminal publication; the public RunState remains a lifecycle
enum. Source facts are validated before any recovery mutation. Retention clips
exact prefixes inside extents; allocation and fairness work units must not discard
otherwise funded bytes. The joined protocol-20 Native observation contract uses schema 6; valid schema-5 stores are explicitly unsupported. No migration or corruption relabeling is implied.

## Numeric audit rules

Each numeric bound must identify units, its owner, source and pressure behavior.
The remaining numbers have distinct jobs:

- Protocol frame/key/cursor ranges (1 MiB frame, 128-byte operation keys, JSON safe integer range) bound one peer message or identity; paged List/replay remove fleet-size coupling.
- 8 KiB PTY reads, 64 KiB cache/replay/coalescing blocks, 1 MiB storage work units and 128-row startup batches bound transient work. Adaptive reduction must handle a smaller configured page budget.
- 64 creation lock stripes distribute key contention; they neither allocate one worker per stripe nor limit distinct Runs.
- Configurable 64-command/256 KiB input bursts yield the sole nonblocking reactor for fairness; queued operations remain owned after a yield.
- FD baseline 16 and attachment headroom 64 provision ordinary service descriptors; three descriptors per native live owner and creation overlap come from actual owners. These are headroom, not an attachment or Run population promise.
- Control/cleanup/upgrade deadlines bound acknowledgement or owner transitions; they do not convert unknown effects into not-applied results. The one-second terminal-output deadline applies to an idle surviving writer: readable or pressure-paused finite output continues draining, and an idle cutoff reports a source gap.
- Bounded retry/backoff numbers apply to typed transient conflicts; unknown commit is never retried.
- Client attachment windows (64 commands, 32 Inputs, 256 events, 1 MiB payload) bound one slow view, including empty envelopes. Refusal occurs before enqueue with explicit backpressure; known or unknown effect is preserved. They do not cap Run count, lifetime work or independent short requests. Activation/readiness/SSH keepalive bounds address owner cleanup and peer liveness; configurable activation/remote deadlines remain caller policy.
- tmux Control Mode uses a 1 MiB line/block payload parser budget plus 32K empty-line envelopes (at most 768 KiB Vec headers on 64-bit hosts); successive Run output notifications stream independently. Native discovery output is configurable.
- Telemetry queue/frame sizes bound optional qualification instrumentation. Missing or dropped instrumentation cannot be a passing resource receipt.

tmux control-client exit is not server-death evidence. The minimum supported
tmux 3.4 can close that client when its target session disappears while other
sessions survive. After a ready reader reports natural control loss, the
termination owner reuses public discovery with its existing four-second owner
deadline, three-second subprocess bound, configured capture budget and group
kill/reap. A proven socket/epoch/target change is `tmux_target_changed`; a
failed query or an unchanged target with a lost observation connection is
`tmux_server_unavailable`. Missing sockets alone do not prove replacement.
Malformed framing, confirmed target changes and shutdown retain priority, and
neither disposition automatically reattaches. The unrecognized
`no-detach-on-destroy` client flag is removed; user session options are untouched.
The causal supported-version behavior is defined by the upstream
[3.4 session destruction](https://github.com/tmux/tmux/blob/3.4/server-fn.c#L403-L426)
and [client flag parser](https://github.com/tmux/tmux/blob/3.4/server-client.c#L2996-L3018).

A full Linux host census includes kernel tasks with a successful `getsid` result
of zero. They have no user session and cannot belong to a positive Run SID.
The census uses the existing portable-pty dependency's raw-safe nix query and
represents this result separately from syscall errors; permission failures still
fail closed. Before orphan waitpid consumes any status, each candidate must still match the
exact Run SID; the full-host fallback is not an ownership proof. This adds one
session query per possible orphan and avoids waiting on unrelated host PIDs.
Direct-child non-reaping observation and exact reaping are unchanged.
This avoids rustix's unchecked positive-Pid conversion of that kernel result,
which panics in debug and is invalid in release, without narrowing the census.

## Qualification objectives

An execution receipt must bind the selected source to the actual executed
artifact. Private candidate and counterfactual source variants use separate
build-output directories. Reusing existing shared outputs requires scoped
derived-cache invalidation, an observed rebuild of the selected source and
executed-binary identity evidence. Source hashes and cached build success alone
are insufficient: a Native owner reversal reused a counterfactual test binary
after source restoration, producing the same three failures. The failed
restoration remains evidence; an actual candidate rebuild and a distinct binary
then passed those same three cases. This isolation costs build storage and
recompilation time; it changes no runtime budget, workload or acceptance oracle.

The old 1/32/128 baselines, GC 128 records plus eight overlap, 4 MiB pressure
payloads, three turnovers and replay digests remain frozen historical workloads.
GC supplies its record/byte policy explicitly on initial start and restart. Tests
must not infer production limits from those fixtures or reduce work to pass.
Every reliability qualification launch also receives a complete fixed CLI
resource policy, overriding ambient resource configuration. Receipts declare
the population-unbounded base policy separately from the historical GC-only
128-record policy. The verifier rejects missing, altered or mis-scoped budgets;
it must not report the GC workload size as a production/global Run quota.
An independent canonical policy digest prevents the producer and verifier from
silently moving the resource envelope together while keeping old cost ceilings.
Changing that qualification policy requires reviewed independent observations;
the digest is an evaluation fence, not a production resource limit.

Functional process fixtures use a build-owned native target for version probes
and an in-place interpreter launcher for fake tmux scripts. This removes a new
shebang executable inode from the fixture startup dependency while preserving
actual production Command/probe/timeout/kill-reap paths, PID/process-group and
pipe ownership, full fake protocol behaviors, and unexpected-exec failure.
It changes no product executable or qualification workload. The host's observed
fresh-script loader delays and unsuccessful fsync/preflight hypotheses remain
diagnostics, not a proven operating-system root cause or passing qualification.
An FD-inheritance fixture declares its own 4000-Run workload when selecting
the raise-only branch, then checks the actual child's exact inherited ceiling;
the population-unbounded default may legitimately need a hard-limit clamp.
A cold-reopen fixture awaits listener cancellation and connection-owner release
before closing its persistence owner. A scheduled `abort()` alone is not proof
that the state lock has been released.
The census counter self-test extracts the actual producer function and observes
separate process owners, so sampler helpers cannot be mistaken for Run children.
It requires exactly zero or two children and reaps its owned fixture PIDs;
removing the empty-match guard or flattening the count to zero must fail.
The existing 30-second fixture sleeps are fail-safe lifetimes, not observation
deadlines or runtime limits; the fixture terminates and waits for them itself.

Linux validation additionally exposed a producer mismatch: public List fields
are tab-separated, so readiness/completion compare complete `head=` fields.
A source-extracted counterexample distinguishes `head=1` from `head=11` and an
absent field. Exact per-Run byte digests and all 4000 admissions remain required.
Ceiling refusal recognizes the CLI's exact `ClientError::Protocol` rendering,
including `(RunCapacity)`, rather than the wire spelling `run_capacity` or a
resource phrase in an unrelated message. An unexpected refusal at the declared
quota boundary fails the cell; it cannot disappear into a null observation.
The actual-producer counterexample includes the captured 4000-Run quota error
and rejects foreign error codes that merely mention capacity.
Readiness also proves live capacity: distinct Running leaders must exist in
Linux procfs, be non-zombies and belong to the exact daemon. A historical head
or exact replay alone cannot certify a live fleet. `live_runs_confirmed` is
required by the judge; missing or incomplete ownership evidence fails even when
all replay digests match. The source-extracted Linux regression uses actual
live, exited, foreign-owner, duplicated and zombie processes.
Every failed census stops its owned Runs through the public protocol, joins its
RSS sampler and retires the exact daemon. Failure cleanup's Stop batch shares
a ten-second deadline with group termination of a hung driver; the daemon then
has the existing ten-second teardown observation window. These are cleanup
lifecycle deadlines, never capacity or performance acceptance thresholds. A
forced termination preserves failure and cannot manufacture passing leak zeros.
A real Linux hanging-client test executes the actual cleanup function and
checks both original-failure preservation and successful-body/failed-cleanup
propagation. macOS explicitly skips that Linux-specific process oracle.

Held-Control-output fixtures transfer the actual stdout writer through
`SCM_RIGHTS` to a test-owned close-on-exec descriptor. They retain it until the
direct Control child has demonstrably disappeared, then close it to deliver EOF.
A foreground Python standard-library sender is waited by the shell; no orphan
process is used as a pipe barrier. A separate Linux helper-descendant regression
requires exact PID disappearance across natural EOF and daemon shutdown, while
the independent tmux-owned Pane remains alive.

Fleet `observe` proposes thresholds without a verdict and refuses overwrite.
Acceptance requires `--thresholds <file> --baseline-ref <full-commit>`: the artifact
must match that previously committed ancestor byte for byte. Budgets are recomputed
from frozen maxima; modified ceilings and a candidate's own source observations
are refused. Source identity covers build/evaluation inputs, excluding receipt and
documentation artifacts, and is captured before build then rechecked after build
and measurement. The receipt reports the anchored baseline identity.

The complete byte/worker/queue policy is fixed explicitly by the workload, rather
than inheriting candidate defaults. Binding includes binary/source hashes, the
producer/verdict/derivation code, resource policy, machine identity, kernel, CPU
count, architecture and OS. Every Run must complete fixed raw input and byte-exact
terminal replay; input failure is fatal. Terminal replay time is measured so a
cache reduction cannot hide cost outside the judged window.

Host identity is collected from initialized `/etc/machine-id` before measurement.
Its 32 lowercase hexadecimal characters encode a nonzero 128-bit identity,
as defined by [machine-id(5)](https://www.freedesktop.org/software/systemd/man/latest/machine-id.html).
The collector validates the original value and hashes its canonical newline form;
missing files, empty or uninitialized values and failed SSH reads cannot become
valid fingerprints. The judge also refuses the legacy empty-input digest.
Ordinary initialized host fingerprints retain their value. A generic container
image without initialized identity cannot qualify a comparative host baseline.

Physical host identity does not bind its allocation. Each cell additionally
fingerprints the ready daemon's complete soft/hard RLIMIT, CPU/NUMA affinity,
and the static cgroup v2 configuration from its leaf through every visible
ancestor. This includes CPU bandwidth/weight, effective cpusets, memory,
process and IO limits; the raw canonical text is retained with the cell.
Ready-PID inspection matters because the daemon funds and raises its own
descriptor allowance. Numeric values remain kernel-rendered strings, preserving
large limits without JavaScript number rounding. PID and cgroup directory names
do not become comparison criteria. Configuration is re-read after work/replay;
changes or read failures invalidate the cell. All baseline rounds, tiers and
modes must share one execution environment, and every receipt cell must match
that independently frozen environment.

The [kernel cgroup contract](https://docs.kernel.org/admin-guide/cgroup-v2.html)
defines ancestor constraints and namespaced views. A visible mount root with
the non-root `cgroup.events` interface is a namespace/subtree boundary, not the
kernel hierarchy root. Partial/legacy hierarchy observations remain functional
evidence but cannot derive or pass comparative performance qualification;
that collector must run in a host namespace with the complete hierarchy visible.
This qualifies the observed allocation and sampled costs; shared-host load and
unobserved instantaneous peaks still do not become statistical guarantees.

RSS uses a 50 ms sampling period across fill, work and replay; sample failure,
missing final coverage or a gap above five periods invalidates the measurement.
This qualifies the sampled peak at that resolution, not an instantaneous peak.
Active completion time and total daemon CPU milliseconds are judged separately;
CPU percentage describes utilization and cannot penalize faster equal-CPU work.
Extra timing comparisons use a predeclared 50% host-noise allowance over three
independent baseline rounds, floored at the 0.001 ms serialization quantum;
CPU uses two measured clock ticks for endpoint quantization. This is a comparison
policy, not a statistical confidence claim. Idle CPU, List latency, memory,
descriptors, threads, admission and teardown retain independent checks. The
1000 ms List guard remains an absolute stall backstop beside frozen comparisons.

A Mac self-test is harness evidence, not Linux 4000-Run acceptance. Revised
fixture semantics invalidate old fleet thresholds; observations must be reviewed
and frozen independently before any new scale-performance acceptance claim.

## Wrong-case corpus（错题集）

The inherited [PERSIST-02 corpus](../../../fixtures/wrong-cases.json) keeps
structural corruption fail-closed while accepting valid partial-generation
migration. This decision adds direct regressions at the same owner boundaries:

- 200,000 one-byte extents and the fragmented held-out workload preserve exact
  recovery within admitted WAL transactions.
- Physical COMMIT/ROLLBACK with lost responses, and post-COMMIT WAL stat failure,
  preserve potentially referenced generations and forbid subsequent mutation.
- Small independent hot/durable budgets, exact partial-extent retention and a
  moving initial replay window preserve every available byte through public clients.
- Real SIGHUP/SIGINT exercise both an indefinitely retrying storage barrier and
  cancellation after its successful completion. Cancellation and exec share one
  final decision gate.
- Shared broadcast payload/envelope leases survive receiver fanout and ring
  eviction. Unfunded markers carry no heap payload and report discontinuity.

These tests live in `crates/ctxmux-daemon/src/persistence.rs`,
`crates/ctxmux-daemon/src/lib.rs` and `packages/sdk/test/activation.test.ts`.
The Feature-local verification links their final command receipts; no historical
benchmark receipt becomes evidence for this revised scale objective.

## Open terminal-state resource qualification

The joined Basic VT candidate removes the silent six-codepoint cell ceiling and
preserves legal one-row geometry and retained content through resize. Dynamic
combining storage has a measured base-cell cost and overflow allocations; it is
not free or an unlimited-host promise. The remaining 10,000 history rows,
32 MiB restore allowance, 1,024 resize fences and doubled checkpoint-file bound
still need explicit configurable owner funding and failure evidence. They do
not define product capacity and are not qualified by old fixture literals.
Full terminal-memory, encoding/reflow work and disk-checkpoint accounting remain
open acceptance work. A raw transport pass cannot close this resource boundary.

Custom reliability observations no longer have an unrelated 128-concurrent
request cap. Integer representation still applies; canonical profiles retain
their original frozen concurrency and complete workload. This changes neither
Run admission nor a passing cost threshold. Host/kernel or actual daemon policy
refusals remain observed failures with the original requested work recorded.
