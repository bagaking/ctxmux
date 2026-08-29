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

/// The readiness poll ceiling the endpoint actually uses.
///
/// This mirrors READY_POLL in crates/ctxmux-remote/src/lib.rs. It is duplicated
/// rather than imported because a shell-invoked Node script cannot read a Rust
/// constant, and the drift guard is the assertion in `verifyPollSchedule` below
/// rather than a comment asking a reader to remember.
const READY_POLL_MS = 50;

/// The endpoint's first readiness probe interval, doubled up to the ceiling.
///
/// The endpoint backs off rather than polling on a fixed period, so modelling a
/// fixed one would attribute dead time to a schedule nothing runs.
const READY_POLL_MIN_MS = 1;

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

/// Guard the duplicated poll schedule against drift from the Rust owner.
///
/// The dead-time figure is only meaningful while this schedule matches the one
/// the endpoint actually polls on, so read the owner and compare rather than
/// trusting the copy. Both ends of the backoff are checked: a changed ceiling or
/// a changed first interval would each make the reported dead time describe a
/// schedule nothing polls on.
async function verifyPollSchedule() {
  const source = await import("node:fs/promises").then((fs) =>
    fs.readFile("crates/ctxmux-remote/src/lib.rs", "utf8"),
  );
  const readConstant = (name, expected) => {
    const pattern = new RegExp(
      `const ${name}: Duration = Duration::from_millis\\((\\d+)\\)`,
      "u",
    );
    const match = pattern.exec(source);
    if (!match) {
      throw new Error(
        `could not find ${name} in crates/ctxmux-remote/src/lib.rs, so the ` +
          "dead-time figure cannot be tied to the schedule the endpoint polls on",
      );
    }
    const owner = Number(match[1]);
    if (owner !== expected) {
      throw new Error(
        `${name} is ${owner}ms in the endpoint but ${expected}ms here; the ` +
          "dead-time figure would describe a schedule nothing polls on",
      );
    }
    return owner;
  };
  return {
    ceiling_ms: readConstant("READY_POLL", READY_POLL_MS),
    first_ms: readConstant("READY_POLL_MIN", READY_POLL_MIN_MS),
  };
}

/// The crate quotes this harness's figures; refuse to let them lose their lane.
///
/// A millisecond figure in the endpoint's documentation is only honest while it
/// says which machine and which forwarder produced it. That caveat is prose, so
/// nothing but a check keeps it alive: a later edit can delete the paragraph and
/// leave the numbers reading like a budget any host should meet, which is
/// exactly the claim this harness refuses to make in its own report. So the
/// guard is symmetrical with `verifyPollSchedule` — that one keeps the harness
/// honest about the endpoint, this one keeps the endpoint honest about the
/// harness.
///
/// It deliberately checks for the caveat rather than for specific numbers.
/// Pinning the values here would make every re-measurement a two-file edit and
/// would tempt the next author to update the copy instead of the reasoning.
/// The required phrases are plain substrings rather than patterns, so the checks
/// stay readable and cannot be mistaken for paths by tooling that scans this
/// file.
async function verifyQuotedFiguresNameTheirLane() {
  const path = "crates/ctxmux-remote/src/lib.rs";
  const source = await import("node:fs/promises").then((fs) =>
    fs.readFile(path, "utf8"),
  );
  // Only the crate-level docs make the promise; a figure inside an item's docs
  // is covered by the same section, which is why it must exist at all.
  const docs = source
    .split("\n")
    .filter((line) => line.startsWith("//!"))
    .join("\n");
  const quotesAFigure = /\b\d+\.\d+ms\b/u.test(docs) || /\b\d+ms\b/u.test(docs);
  if (!quotesAFigure) {
    // Nothing quoted, nothing to qualify. Not a failure: a crate that stops
    // citing measurements does not owe a caveat about them.
    return "no measured figure is quoted in the crate docs";
  }
  const required = [
    ["the stand-in lane", "stand-in"],
    ["the measuring platform", "darwin"],
    ["the measuring architecture", "arm64"],
    ["the harness that produced them", "check-remote-cost"],
    ["that one machine's number is not a claim about another", "not a claim"],
  ];
  const missing = required
    .filter(([, phrase]) => !docs.includes(phrase))
    .map(([name]) => name);
  if (missing.length > 0) {
    throw new Error(
      `${path} quotes a measured figure but its crate docs no longer name ` +
        `${missing.join(", ")}; a number that has lost its lane reads as a ` +
        "budget every host should meet, which is a claim this harness refuses " +
        "to make about a machine it never measured",
    );
  }
  return "quoted figures still name their lane";
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

/// What the endpoint's backoff schedule would have reported for a true instant.
///
/// The endpoint checks, then sleeps its current interval, then doubles it up to
/// the ceiling. So walk that schedule until a check lands at or after the true
/// readiness instant, and return where that check happened. Modelling a fixed
/// interval instead would attribute dead time to a schedule the endpoint stopped
/// using, which is the specific way this figure could go quietly wrong.
///
/// `firstMs` of zero or a non-doubling schedule would not terminate, so both
/// bounds come from the verified owner constants rather than from arguments a
/// caller could pick freely.
function polledObservation(trueMs, firstMs, ceilingMs) {
  let checkedAt = 0;
  let interval = firstMs;
  while (checkedAt < trueMs) {
    checkedAt += interval;
    interval = Math.min(interval * 2, ceilingMs);
  }
  return checkedAt;
}

async function establishmentStage(forwarder, dir, ownerSocket, samples) {
  const schedule = await verifyPollSchedule();
  const trueLatencies = [];
  for (let index = 0; index < samples; index += 1) {
    trueLatencies.push(
      await measureEstablishment(forwarder, dir, ownerSocket, index),
    );
  }
  const polled = trueLatencies.map((value) =>
    polledObservation(value, schedule.first_ms, schedule.ceiling_ms),
  );
  const deadTime = trueLatencies.map((value, index) => polled[index] - value);
  const trueSummary = summarise(trueLatencies, "establishment");
  const polledSummary = summarise(polled, "establishment as polled");
  // What a fixed poll at the ceiling would have reported, so the backoff's
  // effect on this metric is visible in the same report rather than needing a
  // reader to remember a previous run's numbers.
  const atCeiling = trueLatencies.map((value) =>
    polledObservation(value, schedule.ceiling_ms, schedule.ceiling_ms),
  );
  const ceilingSummary = summarise(
    atCeiling,
    "establishment at a fixed ceiling",
  );
  return {
    poll_schedule: {
      first_ms: schedule.first_ms,
      ceiling_ms: schedule.ceiling_ms,
      shape: "double the interval each attempt, capped at the ceiling",
    },
    // What the forward actually costs.
    true_establishment: trueSummary,
    // What a caller of the endpoint observes.
    reported_establishment: polledSummary,
    // The gap between them, which is the poll's contribution and nothing else.
    readiness_poll_dead_time: summarise(deadTime, "readiness-poll dead time"),
    dead_time_share_of_reported_p50: round(
      1 - trueSummary.p50_ms / polledSummary.p50_ms,
      4,
    ),
    // The comparison the backoff was chosen by.
    if_polled_at_a_fixed_ceiling: {
      reported_establishment: ceilingSummary,
      dead_time_share_of_reported_p50: round(
        1 - trueSummary.p50_ms / ceilingSummary.p50_ms,
        4,
      ),
    },
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
  await expectSuccess("poll schedule matches the endpoint", async () => {
    const schedule = await verifyPollSchedule();
    return `READY_POLL_MIN=${schedule.first_ms}ms -> READY_POLL=${schedule.ceiling_ms}ms`;
  });
  await expectSuccess(
    "figures quoted by the endpoint still name their lane",
    () => verifyQuotedFiguresNameTheirLane(),
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
  // hand-worked values rather than trusting the expression. The backoff from 1ms
  // sleeps 1, 2, 4, 8, 16, 32, then 50, so checks land at 1, 3, 7, 15, 31, 63,
  // 113, ... and each case below is the first of those at or after the true
  // instant. Working these out by hand is the point: the first draft of this
  // table had 62ms landing at 111 rather than 63, and the case caught it.
  await expectSuccess(
    "polled observation walks the endpoint's backoff schedule",
    () => {
      const cases = [
        // [trueMs, firstMs, ceilingMs, expected]
        [0.5, 1, 50, 1],
        [1, 1, 50, 1],
        [3.418, 1, 50, 7],
        [7, 1, 50, 7],
        [8, 1, 50, 15],
        [32, 1, 50, 63],
        [62, 1, 50, 63],
        [64, 1, 50, 113],
        // A fixed schedule is the degenerate case where first equals the ceiling,
        // which is what the comparison figure in the report relies on.
        [3, 50, 50, 50],
        [51, 50, 50, 100],
      ];
      for (const [trueMs, firstMs, ceilingMs, expected] of cases) {
        const actual = polledObservation(trueMs, firstMs, ceilingMs);
        if (actual !== expected) {
          throw new Error(
            `polledObservation(${trueMs}, ${firstMs}, ${ceilingMs}) was ${actual}, ` +
              `want ${expected}`,
          );
        }
      }
      return `${cases.length} cases`;
    },
  );

  // The backoff must actually beat a fixed ceiling on the metric it was chosen
  // by, or the change it justified was not an improvement.
  await expectSuccess("the backoff reports a fast forward sooner", () => {
    const fast = 3.418;
    const backoff = polledObservation(fast, READY_POLL_MIN_MS, READY_POLL_MS);
    const fixed = polledObservation(fast, READY_POLL_MS, READY_POLL_MS);
    if (!(backoff < fixed)) {
      throw new Error(
        `backoff reported ${backoff}ms and a fixed ceiling ${fixed}ms; the backoff ` +
          "must report a fast forward sooner or it buys nothing",
      );
    }
    return `${fast}ms readiness reported at ${backoff}ms, not ${fixed}ms`;
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
