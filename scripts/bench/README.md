# Public Run benchmark

This harness measures the [Run benchmark standard](../../docs/benchmark-standard.md)
through protocol generation 22. It starts its own daemon and deterministic C
PTY children; it never connects to a user Runtime. Run it on a qualified Linux
host to obtain `/proc` and cgroup resource measurements. macOS runs can verify
behavior but leave Linux resource fields unavailable.

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
