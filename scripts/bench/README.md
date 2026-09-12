# Public Run benchmark

This harness measures the [Run benchmark standard](../../docs/benchmark-standard.md)
through protocol generation 22. It starts its own daemon and deterministic C
PTY children; it never connects to a user Runtime. Run it on a qualified Linux
host to obtain `/proc` and cgroup resource measurements. macOS runs can verify
behavior but leave Linux resource fields unavailable.

Storage batching/compression qualification additionally follows the
[storage contract](../../docs/replay-storage-benchmark.md). The existing fleet
harness does not yet measure codec, tail-read amplification, decode-cache cost
or every crash point in that contract. Keep those cells explicitly unqualified.

For an explicitly authorized read-only census of an existing store:

```sh
python3 scripts/bench/ground_storage.py \
  --database "$GROUNDING_DATABASE" --output "$GROUNDING_REPORT" \
  --observe-seconds 30 --interval-seconds 5
```

The output contains aggregate numeric facts, not Run IDs, terminal bytes,
commands, machine paths or wall-clock times. Write it outside the observed
Runtime directory. The collector rejects an existing output to preserve prior
observations, uses SQLite read-only/WAL visibility, closes each observation
connection, and never checkpoints or mutates the Runtime. It supports the
current schema-6 layout only; another schema is unqualified, not called corrupt.
The optional observation is committed head movement, not read demand, service
health, producer rate or exact-source performance. Extent histograms describe
stored records, not PTY reads. Physical file stats are not atomic with SQL.
The sampling window and histogram boundaries select evidence, not production
limits. Bind the selected serving artifact externally; a version string alone
cannot identify a build, particularly during installation or planned exec.

```sh
python3 -m unittest discover -s scripts/bench -v
cc -std=c11 -O2 -Wall -Wextra -Werror scripts/bench/run_fixture.c -o "$BENCH_FIXTURE"
python3 scripts/bench/run_benchmark.py \
  --daemon "$BENCH_DAEMON" --cli "$BENCH_CLI" --fixture "$BENCH_FIXTURE" \
  --source-dir "$BENCH_SOURCE" --output "$BENCH_ARTIFACTS" \
  --sdk-module "$BENCH_SDK_MODULE"
```

Build the selected source into a separate fresh output directory and retain the
build log and binary hashes. The executable and source options do not themselves
prove the build relationship. The harness records its plan before starting the
first daemon. Defaults select both persistence modes, three repeats of
128/512/2048/4000, mixed and active phases, a 30-minute soak and a separate 8192
default-policy observation. These values select an experiment, not a product
ceiling. CLI flags make workload shapes, observation windows and resource
sampling explicit. A small pilot is a harness check, not full qualification.

Build the selected SDK and retain its compiler output and emitted module hashes;
`BENCH_SDK_MODULE` points to its `dist/index.js`. Without it the SDK cell is
explicitly `not_executed`, so that invocation cannot qualify SDK usability.

Artifacts include losslessly compressed private raw frames, all attempts, source/binary/fixture hashes,
per-view exact-byte facts, resource trajectories, cell outcomes and cleanup.
Sampling runs in a worker thread and raw-frame compression in an owned child;
their CPU, RSS/PSS, sampling duration and pipe waiting costs remain visible.
Durable file sizes and allocated storage are sampled separately from process
I/O. Child RSS sums can count shared pages repeatedly. Missing measurements
remain explicit, and sampler failures cannot produce a passing campaign.
Success quantiles are explicitly conditional; timed-out requests remain censored
observations. Gap recovery resumes only output from the last verified cursor;
ordinary input is never retried. The scene also exercises two actual SDK clients,
a deliberately constrained slow SDK view, interactive CLI attach/input/Ctrl+C/
detach/rejoin in a real PTY, Level A fork, a narrow terminal seed, planned exec
and cold historical recovery. Terminal seed metadata alone does not qualify
consumer grid rendering. Run the complementary daemon/codec fault and lifecycle
tests and publish their results. This harness does not qualify AgentMux UI,
remote transport, tmux, Level B or provider semantics.

Binary bursts carry Run identity before their independent byte pattern. A
separate scene writes and verifies a full binary input echo, including every
byte value. When admission fails, the original requested population and all
errors are retained, and an already accepted Run is probed for isolation.
Unsafe cleanup stops further execution and gives each remaining planned cell
an explicit blocked disposition; these cells never count as executed or passing.

Record the default policy separately from explicitly funded policy arms.
`--resource-limits` forwards a JSON object to the public daemon option and
records the actual launch arguments. It does not modify any offered workload,
byte oracle, timeout or durability assertion. Preserve the original default
failures when a second arm funds more shared event memory. Per-Run arming
results and framed refusals are retained before the cell-level failure.

Before cleanup starts, `workload.private.json`, byte-oracle facts and latency
summaries preserve the completed workload. This snapshot explicitly leaves
cleanup unqualified. Only the final cell result joins workload and lifecycle
cleanup; a passing traffic phase cannot conceal failed Stop or removal.
