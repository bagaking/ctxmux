// Measurement core for scripts/check-remote-cost.sh.
//
// Timing lives here rather than in the shell because portable sub-millisecond
// timing does not exist in bash: macOS `date` has no `%N`, and the quantity that
// matters most in this harness — readiness-poll dead time — is a sub-50ms
// figure. `performance.now()` is what the rest of this repository already uses
// to measure latency, so this reuses that rather than adding a second timing
// story.
//
// Every reported figure is an observed sample. Percentiles are nearest-rank with
// no interpolation, matching scripts/reliability-qualification.ts, so a p95 is a
// measurement that actually happened rather than a number between two that did.

import { spawn } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { writeFile } from "node:fs/promises";
import { connect, createServer } from "node:net";
import { arch, platform, tmpdir, cpus, totalmem } from "node:os";
import { join } from "node:path";
import { performance } from "node:perf_hooks";
import process from "node:process";

/// The readiness poll interval the endpoint actually uses.
///
/// This mirrors READY_POLL in crates/ctxmux-remote/src/lib.rs. It is duplicated
/// rather than imported because a shell-invoked Node script cannot read a Rust
/// constant, and the drift guard is the assertion in `verifyPollInterval` below
/// rather than a comment asking a reader to remember.
const READY_POLL_MS = 50;

/// Ceiling for one establishment sample before the harness declares it stuck.
///
/// Well above the observed distribution, because a slow sample is data while a
/// hung one is a defect, and this must tell them apart.
const SAMPLE_TIMEOUT_MS = 10_000;

/// Fewest samples that may be reported as a distribution.
///
/// Below this a p95 is just the maximum wearing a different name, so the harness
/// refuses rather than publishing a figure that cannot mean what it says.
const MIN_SAMPLES = 5;

/// Payload size for one steady-state throughput sample.
const THROUGHPUT_BYTES = 4 * 1024 * 1024;

/// Concurrency levels compared to expose the marginal per-tunnel cost.
const FANOUT_LEVELS = [1, 2, 4, 8];

function parseArgs(argv) {
  const options = {
    forwarder: "",
    stage: "all",
    samples: 30,
    json: "",
    selfTest: false,
  };
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    const takeValue = (name) => {
      const value = argv[index + 1];
      if (value === undefined) {
        throw new Error(`${name} requires a value`);
      }
      index += 1;
      return value;
    };
    switch (arg) {
      case "--forwarder":
        options.forwarder = takeValue(arg);
        break;
      case "--stage":
        options.stage = takeValue(arg);
        break;
      case "--samples":
        options.samples = Number(takeValue(arg));
        break;
      case "--json":
        options.json = takeValue(arg);
        break;
      case "--self-test":
        options.selfTest = true;
        break;
      default:
        throw new Error(`unknown argument '${arg}'`);
    }
  }
  return options;
}

function percentile(values, fraction) {
  if (values.length === 0) {
    throw new Error("percentile needs at least one sample");
  }
  const sorted = [...values].sort((left, right) => left - right);
  const rank = Math.ceil(fraction * sorted.length);
  const index = Math.min(Math.max(rank, 1), sorted.length) - 1;
  return sorted[index];
}

const round = (value, digits = 3) => Number(value.toFixed(digits));

/// Summarise a latency series, refusing a series too short to summarise.
///
/// The guard is the point: a silent zero or a one-sample "p95" read as a fast
/// result is the specific way a cost harness lies, so this fails instead.
function summarise(values, label) {
  if (values.length < MIN_SAMPLES) {
    throw new Error(
      `${label}: ${values.length} samples is below the ${MIN_SAMPLES}-sample floor, ` +
        "so a distribution cannot be reported honestly",
    );
  }
  for (const value of values) {
    if (!Number.isFinite(value) || value < 0) {
      throw new Error(
        `${label}: refusing to report an unusable sample (${value})`,
      );
    }
  }
  return {
    samples: values.length,
    min_ms: round(Math.min(...values)),
    p50_ms: round(percentile(values, 0.5)),
    p95_ms: round(percentile(values, 0.95)),
    p99_ms: round(percentile(values, 0.99)),
    max_ms: round(Math.max(...values)),
  };
}

/// Prove the timing source can actually resolve the quantities being reported.
///
/// Dead time is a sub-50ms figure. A clock too coarse to see it would report
/// zeros that read as "no dead time" rather than as "cannot measure", which is
/// exactly the silent-zero failure this harness must not have.
function verifyTimingSource() {
  const first = performance.now();
  let second = first;
  let spins = 0;
  while (second === first && spins < 10_000_000) {
    second = performance.now();
    spins += 1;
  }
  if (second === first) {
    throw new Error(
      "the timing source did not advance, so no latency figure here would be real",
    );
  }
  const resolution = second - first;
  if (!Number.isFinite(resolution) || resolution <= 0) {
    throw new Error(
      `the timing source reported an unusable resolution (${resolution})`,
    );
  }
  if (resolution > READY_POLL_MS / 10) {
    throw new Error(
      `the timing source resolves to ${resolution}ms, too coarse to measure ` +
        `dead time against a ${READY_POLL_MS}ms poll`,
    );
  }
  return round(resolution, 6);
}

/// Guard the duplicated poll interval against drift from the Rust owner.
///
/// The dead-time figure is only meaningful while this constant matches the one
/// the endpoint actually polls at, so read the owner and compare rather than
/// trusting the copy.
async function verifyPollInterval() {
  const source = await import("node:fs/promises").then((fs) =>
    fs.readFile("crates/ctxmux-remote/src/lib.rs", "utf8"),
  );
  const match =
    /const READY_POLL: Duration = Duration::from_millis\((\d+)\)/u.exec(source);
  if (!match) {
    throw new Error(
      "could not find READY_POLL in crates/ctxmux-remote/src/lib.rs, so the " +
        "dead-time figure cannot be tied to the interval the endpoint polls at",
    );
  }
  const owner = Number(match[1]);
  if (owner !== READY_POLL_MS) {
    throw new Error(
      `READY_POLL is ${owner}ms in the endpoint but ${READY_POLL_MS}ms here; ` +
        "the dead-time figure would describe an interval nothing polls at",
    );
  }
  return owner;
}

function startOwnerListener(socketPath) {
  return new Promise((resolve, reject) => {
    // Echo, so a throughput sample can observe a full round trip rather than
    // measuring only how fast the kernel accepts writes into a buffer.
    const server = createServer((socket) => socket.pipe(socket));
    server.once("error", reject);
    server.listen(socketPath, () => resolve(server));
  });
}

function tryConnect(socketPath) {
  return new Promise((resolve) => {
    const socket = connect(socketPath);
    socket.once("connect", () => {
      socket.destroy();
      resolve(true);
    });
    socket.once("error", () => {
      socket.destroy();
      resolve(false);
    });
  });
}

/// Spawn the forwarder with the production argument shape.
///
/// The shape is the one crates/ctxmux-remote builds, so a change there that
/// broke the invocation would show up here instead of being hidden behind a
/// bespoke test path.
function spawnForwarder(forwarder, localSocket, ownerSocket) {
  const child = spawn(
    forwarder,
    [
      "-N",
      "-T",
      "-o",
      "BatchMode=yes",
      "-o",
      "ExitOnForwardFailure=yes",
      "-o",
      "ServerAliveInterval=10",
      "-o",
      "ServerAliveCountMax=3",
      "-L",
      `${localSocket}:${ownerSocket}`,
      "owner-host.cost-harness",
    ],
    { stdio: "ignore" },
  );
  // A spawn failure arrives asynchronously on 'error', not as a throw. Left
  // unhandled it becomes an uncaught exception that kills the process, which
  // would make an unrunnable forwarder look like a harness crash instead of the
  // clear refusal this harness promises. Capture it so the polling loops can
  // report it as the measurement failure it is.
  child.spawnFailure = null;
  child.once("error", (error) => {
    child.spawnFailure = error;
  });
  return child;
}

/// Whether a spawned forwarder can no longer become ready.
///
/// Either it failed to start at all or it has already exited; both mean no
/// further polling can succeed, so the caller should stop and report.
function forwarderIsGone(child) {
  return (
    child.spawnFailure !== null ||
    child.exitCode !== null ||
    child.signalCode !== null
  );
}

function forwarderFailureReason(child) {
  if (child.spawnFailure !== null) {
    return `the forwarder could not be started: ${child.spawnFailure.message}`;
  }
  return (
    `the forwarder exited before its forward became usable (code ${child.exitCode}, ` +
    `signal ${child.signalCode})`
  );
}

async function reap(child) {
  if (forwarderIsGone(child)) {
    return;
  }
  child.kill("SIGKILL");
  await new Promise((resolve) => child.once("exit", resolve));
}

/// One establishment sample: spawn, then find the instant the forward is usable.
///
/// The inner loop polls as fast as it can rather than at the endpoint's 50ms, so
/// it observes the true readiness instant. The endpoint's *reported* latency is
/// then derived arithmetically. Measuring the true instant and computing the
/// polled one is the only way to separate the two; polling at 50ms would make
/// the dead time invisible in the very measurement meant to expose it.
async function measureEstablishment(forwarder, dir, ownerSocket, index) {
  const localSocket = join(dir, `establish-${index}.sock`);
  const startedAt = performance.now();
  const child = spawnForwarder(forwarder, localSocket, ownerSocket);
  try {
    for (;;) {
      if (await tryConnect(localSocket)) {
        return performance.now() - startedAt;
      }
      if (forwarderIsGone(child)) {
        throw new Error(forwarderFailureReason(child));
      }
      if (performance.now() - startedAt > SAMPLE_TIMEOUT_MS) {
        throw new Error(
          `an establishment sample exceeded ${SAMPLE_TIMEOUT_MS}ms, which is a stuck ` +
            "forwarder rather than a slow one",
        );
      }
    }
  } finally {
    await reap(child);
    rmSync(localSocket, { force: true });
  }
}

/// What a poll of the given granularity would have reported for a true instant.
///
/// The endpoint checks, then sleeps, so an instant landing between two checks is
/// reported at the next one. A sample already at a multiple is reported there.
function polledObservation(trueMs, intervalMs) {
  return Math.ceil(trueMs / intervalMs) * intervalMs;
}

async function establishmentStage(forwarder, dir, ownerSocket, samples) {
  const pollInterval = await verifyPollInterval();
  const trueLatencies = [];
  for (let index = 0; index < samples; index += 1) {
    trueLatencies.push(
      await measureEstablishment(forwarder, dir, ownerSocket, index),
    );
  }
  const polled = trueLatencies.map((value) =>
    polledObservation(value, pollInterval),
  );
  const deadTime = trueLatencies.map((value, index) => polled[index] - value);
  const trueSummary = summarise(trueLatencies, "establishment");
  const polledSummary = summarise(polled, "establishment as polled");
  return {
    poll_interval_ms: pollInterval,
    // What the forward actually costs.
    true_establishment: trueSummary,
    // What a caller of the endpoint currently observes.
    reported_establishment: polledSummary,
    // The gap between them, which is the poll's contribution and nothing else.
    readiness_poll_dead_time: summarise(deadTime, "readiness-poll dead time"),
    dead_time_share_of_reported_p50: round(
      1 - trueSummary.p50_ms / polledSummary.p50_ms,
      4,
    ),
  };
}

/// One throughput sample through an established forward.
async function measureThroughput(forwarder, dir, ownerSocket, index) {
  const localSocket = join(dir, `throughput-${index}.sock`);
  const child = spawnForwarder(forwarder, localSocket, ownerSocket);
  try {
    const deadline = performance.now() + SAMPLE_TIMEOUT_MS;
    while (!(await tryConnect(localSocket))) {
      if (forwarderIsGone(child)) {
        throw new Error(forwarderFailureReason(child));
      }
      if (performance.now() > deadline) {
        throw new Error("a throughput sample never reached a usable forward");
      }
    }
    const payload = Buffer.alloc(THROUGHPUT_BYTES, 0x61);
    const socket = connect(localSocket);
    await new Promise((resolve, reject) => {
      socket.once("connect", resolve);
      socket.once("error", reject);
    });
    const startedAt = performance.now();
    let echoed = 0;
    const finished = new Promise((resolve, reject) => {
      socket.on("data", (chunk) => {
        echoed += chunk.length;
        if (echoed >= payload.length) {
          resolve();
        }
      });
      socket.once("error", reject);
      socket.once("close", () => {
        if (echoed < payload.length) {
          reject(
            new Error("the forward closed before the payload round-tripped"),
          );
        }
      });
    });
    socket.write(payload);
    await finished;
    const elapsed = performance.now() - startedAt;
    socket.destroy();
    if (elapsed <= 0) {
      throw new Error("a throughput sample reported non-positive elapsed time");
    }
    // Round trip, so the bytes crossed the forward twice.
    return (payload.length * 2) / (elapsed / 1000) / (1024 * 1024);
  } finally {
    await reap(child);
    rmSync(localSocket, { force: true });
  }
}

async function throughputStage(forwarder, dir, ownerSocket, samples) {
  const rates = [];
  for (let index = 0; index < samples; index += 1) {
    rates.push(await measureThroughput(forwarder, dir, ownerSocket, index));
  }
  if (rates.length < MIN_SAMPLES) {
    throw new Error("throughput: too few samples to report");
  }
  for (const rate of rates) {
    if (!Number.isFinite(rate) || rate <= 0) {
      throw new Error(
        `throughput: refusing to report a non-positive rate (${rate})`,
      );
    }
  }
  return {
    samples: rates.length,
    payload_bytes_per_sample: THROUGHPUT_BYTES,
    // Sorted ascending by percentile, so p50 is the median rate.
    p50_mib_per_s: round(percentile(rates, 0.5), 2),
    p95_mib_per_s: round(percentile(rates, 0.95), 2),
    min_mib_per_s: round(Math.min(...rates), 2),
    max_mib_per_s: round(Math.max(...rates), 2),
  };
}

/// Establish `level` tunnels concurrently and report the wall clock it took.
async function measureFanoutLevel(forwarder, dir, ownerSocket, level, index) {
  const children = [];
  const sockets = [];
  try {
    const startedAt = performance.now();
    for (let slot = 0; slot < level; slot += 1) {
      const localSocket = join(dir, `fanout-${index}-${level}-${slot}.sock`);
      sockets.push(localSocket);
      children.push(spawnForwarder(forwarder, localSocket, ownerSocket));
    }
    await Promise.all(
      sockets.map(async (socketPath, slot) => {
        const child = children[slot];
        const deadline = performance.now() + SAMPLE_TIMEOUT_MS;
        for (;;) {
          if (await tryConnect(socketPath)) {
            return;
          }
          if (forwarderIsGone(child)) {
            throw new Error(
              `a fanout tunnel at level ${level}: ${forwarderFailureReason(child)}`,
            );
          }
          if (performance.now() > deadline) {
            throw new Error(
              `a fanout tunnel at level ${level} never became usable`,
            );
          }
        }
      }),
    );
    return performance.now() - startedAt;
  } finally {
    await Promise.all(children.map(reap));
    for (const socketPath of sockets) {
      rmSync(socketPath, { force: true });
    }
  }
}

async function fanoutStage(forwarder, dir, ownerSocket, samples) {
  const levels = [];
  for (const level of FANOUT_LEVELS) {
    const durations = [];
    for (let index = 0; index < samples; index += 1) {
      durations.push(
        await measureFanoutLevel(forwarder, dir, ownerSocket, level, index),
      );
    }
    const summary = summarise(durations, `fanout at ${level} concurrent`);
    levels.push({
      concurrent_tunnels: level,
      wall_clock: summary,
      per_tunnel_p50_ms: round(summary.p50_ms / level),
    });
  }
  const single = levels[0];
  const widest = levels[levels.length - 1];
  return {
    levels,
    // The reuse decision turns on this: a per-tunnel cost that stays flat as
    // concurrency rises means each tunnel pays its own way and pooling would buy
    // little; one that climbs means contention a shared connection could avoid.
    per_tunnel_p50_ratio_widest_over_single: round(
      widest.per_tunnel_p50_ms / single.per_tunnel_p50_ms,
      3,
    ),
  };
}

/// Prove the harness fails loudly instead of reporting a silent zero.
///
/// Each case is a way this harness could have lied. A self-test that only
/// checked the happy path would leave exactly the failure mode a cost report
/// must not have: a plausible-looking number with nothing behind it.
async function selfTest(forwarder) {
  const failures = [];
  const expectFailure = async (name, run) => {
    try {
      await run();
    } catch (error) {
      console.log(`  ok    ${name}: refused with "${error.message}"`);
      return;
    }
    failures.push(name);
    console.log(`  FAIL  ${name}: accepted what it must refuse`);
  };
  const expectSuccess = async (name, run) => {
    try {
      const value = await run();
      console.log(`  ok    ${name}: ${value}`);
    } catch (error) {
      failures.push(name);
      console.log(`  FAIL  ${name}: ${error.message}`);
    }
  };

  await expectSuccess(
    "timing source resolves finer than the poll",
    () => `${verifyTimingSource()}ms resolution`,
  );
  await expectSuccess(
    "poll interval matches the endpoint",
    async () => `READY_POLL=${await verifyPollInterval()}ms`,
  );

  await expectFailure("a sample set below the floor is refused", () => {
    summarise([1, 2], "self-test");
  });
  await expectFailure("a non-finite sample is refused", () => {
    summarise([1, 2, 3, 4, Number.NaN], "self-test");
  });
  await expectFailure("a negative sample is refused", () => {
    summarise([1, 2, 3, 4, -1], "self-test");
  });
  await expectFailure("an absent forwarder is refused", async () => {
    const dir = mkdtempSync(join(tmpdir(), "ctxmux-cost-selftest."));
    try {
      const ownerSocket = join(dir, "owner.sock");
      const server = await startOwnerListener(ownerSocket);
      try {
        await measureEstablishment(
          join(dir, "does-not-exist-forwarder"),
          dir,
          ownerSocket,
          0,
        );
      } finally {
        server.close();
      }
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  // The dead-time arithmetic is the harness's central claim, so check it against
  // hand-worked values rather than trusting the expression.
  await expectSuccess("polled observation rounds up to the next check", () => {
    const cases = [
      [3, 50, 50],
      [50, 50, 50],
      [51, 50, 100],
      [99.9, 50, 100],
    ];
    for (const [trueMs, interval, expected] of cases) {
      const actual = polledObservation(trueMs, interval);
      if (actual !== expected) {
        throw new Error(
          `polledObservation(${trueMs}, ${interval}) was ${actual}, want ${expected}`,
        );
      }
    }
    return `${cases.length} cases`;
  });

  // And prove the real measurement path produces a usable distribution, so a
  // self-test pass means the harness can actually measure, not merely refuse.
  await expectSuccess(
    "the establishment path yields a real distribution",
    async () => {
      const dir = mkdtempSync(join(tmpdir(), "ctxmux-cost-selftest."));
      try {
        const ownerSocket = join(dir, "owner.sock");
        const server = await startOwnerListener(ownerSocket);
        try {
          const report = await establishmentStage(
            forwarder,
            dir,
            ownerSocket,
            MIN_SAMPLES,
          );
          if (report.true_establishment.p50_ms <= 0) {
            throw new Error(
              "a zero establishment p50 would be a silent zero, not a fast result",
            );
          }
          return `p50 ${report.true_establishment.p50_ms}ms over ${MIN_SAMPLES} samples`;
        } finally {
          server.close();
        }
      } finally {
        rmSync(dir, { recursive: true, force: true });
      }
    },
  );

  if (failures.length > 0) {
    throw new Error(
      `${failures.length} self-test case(s) failed: ${failures.join(", ")}`,
    );
  }
  console.log(`self-test passed: the harness refuses what it cannot measure`);
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  if (!options.forwarder) {
    throw new Error("--forwarder is required");
  }

  if (options.selfTest) {
    await selfTest(options.forwarder);
    return;
  }

  if (!Number.isInteger(options.samples) || options.samples < MIN_SAMPLES) {
    throw new Error(
      `--samples must be an integer of at least ${MIN_SAMPLES}, got ${options.samples}`,
    );
  }

  const timingResolutionMs = verifyTimingSource();

  // Naming the lane is part of the result. A stand-in number presented as a real
  // one is worse than no number, because it would be cited later as if it
  // described the shipped transport.
  const destination = process.env.CTXMUX_REMOTE_SSH_DESTINATION ?? "";
  const lane = destination
    ? {
        forwarder: "real system OpenSSH client",
        comparable_to_stand_in: false,
        note:
          "Numbers include authentication, crypto, and network round trips, so they " +
          "describe this SSH boundary and not the stand-in.",
      }
    : {
        forwarder:
          "stand-in fake-ssh (Unix-socket relay, no crypto, no network)",
        comparable_to_stand_in: true,
        note:
          "No authentication, crypto, or network is involved, so establishment here " +
          "is a floor for the real client rather than a prediction of it. Set " +
          "CTXMUX_REMOTE_SSH_DESTINATION to measure a real boundary.",
      };
  if (destination) {
    // The real lane needs an owner-side socket path; without it a run would
    // silently measure the stand-in while claiming the real client.
    if (!process.env.CTXMUX_REMOTE_SOCKET) {
      throw new Error(
        "CTXMUX_REMOTE_SSH_DESTINATION is set but CTXMUX_REMOTE_SOCKET is not, so the " +
          "real-client lane has no owner-side path. Refusing rather than reporting " +
          "stand-in numbers as real-client ones.",
      );
    }
    throw new Error(
      "the real-client cost lane is not implemented in this change: it needs an SSH " +
        "boundary this harness cannot provision, and reporting the stand-in under a " +
        "real-client label is the one outcome worse than reporting nothing",
    );
  }

  const dir = mkdtempSync(join(tmpdir(), "ctxmux-remote-cost."));
  const ownerSocket = join(dir, "owner.sock");
  const server = await startOwnerListener(ownerSocket);
  const report = {
    environment: {
      platform: platform(),
      arch: arch(),
      cpus: cpus().length,
      total_memory_bytes: totalmem(),
      node: process.version,
      timing_resolution_ms: timingResolutionMs,
    },
    lane,
    samples_per_quantity: options.samples,
    stages: {},
  };

  try {
    const wants = (stage) => options.stage === "all" || options.stage === stage;
    if (wants("establishment")) {
      report.stages.establishment = await establishmentStage(
        options.forwarder,
        dir,
        ownerSocket,
        options.samples,
      );
    }
    if (wants("throughput")) {
      report.stages.throughput = await throughputStage(
        options.forwarder,
        dir,
        ownerSocket,
        options.samples,
      );
    }
    if (wants("fanout")) {
      report.stages.fanout = await fanoutStage(
        options.forwarder,
        dir,
        ownerSocket,
        options.samples,
      );
    }
  } finally {
    server.close();
    rmSync(dir, { recursive: true, force: true });
  }

  if (Object.keys(report.stages).length === 0) {
    throw new Error(
      "no stage produced a measurement, so there is nothing to report",
    );
  }

  const rendered = JSON.stringify(report, null, 2);
  console.log(rendered);
  if (options.json) {
    await writeFile(options.json, `${rendered}\n`, "utf8");
    console.log(`report written to ${options.json}`);
  }
}

try {
  await main();
} catch (error) {
  console.error(`check-remote-cost: ${error.message}`);
  process.exitCode = 1;
}
