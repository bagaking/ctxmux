// Fleet observations are proposals, never a candidate's acceptance criteria.
// Accept reads frozen thresholds and verifies workload, environment and source
// identities. Smaller resource costs pass only with the full byte-exact work.

import { execFileSync } from "node:child_process";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { createHash } from "node:crypto";
import {
  readFileSync,
  writeFileSync,
  mkdtempSync,
  mkdirSync,
  rmSync,
} from "node:fs";
import { tmpdir } from "node:os";
import process from "node:process";

import {
  MODES,
  OBSERVED_FIELDS,
  deriveBudgetCeiling,
  deriveObservedMaxima,
} from "./reliability-budget-contract.mjs";

/// Fleet tiers this harness renders a verdict at.
///
/// 128 is load-bearing: it is the one tier the existing darwin gate also
/// qualifies, so it is the only tier whose farm numbers can be cross-checked
/// against an independently trusted measurement. The three larger tiers are
/// where the product actually operates and where no gate has ever spoken.
const TIERS = [128, 512, 2048, 4000];

/// The tier that overlaps the existing gate, singled out for the cross-check.
const OVERLAP_TIER = 128;

/// Explicit policy used by this qualification workload. Production has no
/// population ceiling; this exercises operator admission at 4000 + one request.
const DAEMON_ADMISSION_CAP = 4000;

/// Wall-clock ceiling for enumerating the whole fleet, in milliseconds.
///
/// Fixed in advance rather than derived. Every other cost ceiling in this file
/// is derived from observation because RSS and CPU legitimately depend on the
/// host, but latency is the one dimension where deriving the bar would defeat
/// the check: the regression this harness exists to catch (#49, List degrading
/// past ~1000 Runs) shows up as a *rising* latency, and a ceiling derived from
/// that rise ratifies it. Set two orders of magnitude above anything measured —
/// the farm curve runs 8.2 ms at 128 to 26.5 ms at 4000, and tmux lists 300
/// sessions in 8.1 ms — so it never fires on host noise, only on a stall a
/// caller would feel.
const LIST_LATENCY_CEILING_MS = 1000;

/// Observation rounds per tier before a threshold may be derived.
///
/// Three is the contract's own rule (deriveObservedMaxima requires exactly
/// three round cells): a single round cannot separate a typical cost from an
/// outlier, and the darwin baseline is itself three rounds. Fewer would let a
/// lucky low reading set a ceiling the fleet then fails to meet in production.
const ROUNDS = 3;

/// The host class these farm thresholds are bound to.
///
/// Recorded the way observation_baseline.environment records it AND enforced:
/// a receipt whose platform/arch does not match is refused rather than checked
/// against thresholds derived on a different kernel. The farm node is Linux
/// x86_64; the darwin baseline is arm64 macOS, and the two are not
/// interchangeable for a descriptor or CPU ceiling.
const REQUIRED_HOST_CLASS = { platform: "linux", arch: "x64" };

/// Fields whose farm/darwin agreement is asserted at the overlap tier.
///
/// These are the structural per-Run costs. They are platform-invariant by
/// design — the daemon's descriptor and thread cost per Run comes from its own
/// architecture (ADR 013 / the SIGCHLD reuse proof), not from the kernel it
/// runs on — so a farm that measures a different value at 128 is measuring
/// something the gate is not. RSS and CPU are deliberately excluded here: they
/// legitimately differ by platform and are bounded per-tier by the derived
/// ceilings instead.
///
/// Invariant across platforms is not invariant across versions. `threads_per_run`
/// was 2 when the darwin baseline was frozen and is 0 now, because #52 replaced
/// the per-Run wake source with one shared SIGCHLD handler. A frozen baseline is
/// the point — it is what makes a regression visible — but it means this check
/// also fires when the daemon improves, which is why the comparison reports
/// which direction it moved rather than assuming the worse one.
const OVERLAP_INVARIANT_FIELDS = ["fds_per_run", "threads_per_run"];

/// What the darwin gate holds at 128, for the overlap cross-check.
///
/// These are read from the committed budget's observed maxima rather than
/// retyped, so a change to the darwin baseline cannot leave this stale. Loaded
/// lazily so the pure derivation paths and the self-test do not require the
/// file to be present.
function darwinOverlapMaxima(root, mode) {
  const budgets = JSON.parse(
    readFileSync([root, "reliability-budgets.json"].join("/"), "utf8"),
  );
  const cell =
    budgets?.observation_baseline?.observed_maxima?.[mode]?.[
      String(OVERLAP_TIER)
    ];
  if (cell === undefined) {
    throw new Error(
      `the darwin baseline has no ${mode} ${OVERLAP_TIER} cell to cross-check against`,
    );
  }
  return cell;
}

function parseArgs(argv) {
  const options = {
    mode: "",
    tiersPath: "",
    thresholdsPath: "",
    baselineRef: "",
    out: "",
    selfTest: false,
    root: ".",
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
      case "--mode":
        options.mode = takeValue(arg);
        break;
      case "--observations":
        options.tiersPath = takeValue(arg);
        break;
      case "--baseline-ref":
        options.baselineRef = takeValue(arg);
        break;
      case "--thresholds":
        options.thresholdsPath = takeValue(arg);
        break;
      case "--out":
        options.out = takeValue(arg);
        break;
      case "--root":
        options.root = takeValue(arg);
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

const round = (value, digits = 3) => Number(value.toFixed(digits));

/// A host class rendered as "os-arch" for error messages.
///
/// Deliberately hyphen-joined rather than slash-joined: a slash between two
/// interpolations reads to a path scanner as a leaked absolute path, and this
/// label is descriptive text, not a path.
const hostClassLabel = (host) => `${host.os}-${host.architecture}`;

/// Confirm a census cell carries every field the derivation and verdict read.
///
/// A cell missing a field would otherwise reach deriveObservedMaxima as an
/// undefined that Math.max turns into NaN — a silent corruption of the very
/// ceiling this harness exists to compute. Refusing here turns that into the
/// loud failure the precedent requires.
function assertCompleteCell(cell, label) {
  if (cell === null || typeof cell !== "object") {
    throw new Error(`${label}: census cell is missing entirely`);
  }
  for (const field of [
    "list_latency_ms",
    "replay_wall_ms",
    "active_wall_ms",
    "active_cpu_ms",
    "cpu_tick_ms",
  ]) {
    if (
      !Number.isFinite(cell[field]) ||
      cell[field] < 0 ||
      (field === "cpu_tick_ms" && cell[field] === 0)
    )
      throw new Error(`${label}: invalid ${field}`);
  }
  const directFields = OBSERVED_FIELDS.filter(
    (field) => field !== "steady_rss_kib" && field !== "cleanup_threads_delta",
  );
  for (const field of directFields) {
    if (!Number.isFinite(cell[field]) || cell[field] < 0) {
      throw new Error(
        `${label}: field ${field} is not a finite non-negative number (${cell[field]})`,
      );
    }
  }
  if (!Number.isFinite(cell.steady?.rss_kib) || cell.steady.rss_kib < 0) {
    throw new Error(
      `${label}: steady.rss_kib is not a finite non-negative number (${cell.steady?.rss_kib})`,
    );
  }
  for (const gate of ["baseline", "cleanup"]) {
    if (!Number.isFinite(cell[gate]?.threads) || cell[gate].threads < 0) {
      throw new Error(
        `${label}: ${gate}.threads is not a finite non-negative number (${cell[gate]?.threads})`,
      );
    }
  }
}

/// Turn three census rounds for one tier into governed observed maxima.
///
/// This is deriveObservedMaxima from the frozen contract, applied to
/// farm-derived cells rather than the darwin fixtures. The rules are identical;
/// only the observations differ. The result is the same field set the darwin
/// baseline records, so the thresholds computed from it are directly
/// comparable in shape.
function observedMaximaForTier(rounds, tier, mode) {
  if (!Array.isArray(rounds) || rounds.length !== ROUNDS) {
    throw new Error(
      `tier ${tier} ${mode}: expected exactly ${ROUNDS} observation rounds, got ${
        Array.isArray(rounds) ? rounds.length : "none"
      }`,
    );
  }
  rounds.forEach((cell, index) =>
    assertCompleteCell(cell, `tier ${tier} ${mode} round ${index + 1}`),
  );
  for (const cell of rounds) {
    const workload = judgeWorkload(cell, Number(tier), mode);
    if (!workload.pass) throw new Error(workload.reason);
    assertSampling(cell);
    assertExecutionEnvironment(cell.execution_environment, false);
    assertSameExecutionEnvironment(
      rounds[0].execution_environment,
      cell.execution_environment,
    );
  }
  const maxima = deriveObservedMaxima(rounds);
  for (const field of [
    "list_latency_ms",
    "replay_wall_ms",
    "cpu_tick_ms",
    ...(mode === "active" ? ["active_wall_ms", "active_cpu_ms"] : []),
  ]) {
    const values = rounds.map((cell) => cell[field]);
    if (!values.every((value) => Number.isFinite(value) && value >= 0))
      throw new Error(`invalid ${field}`);
    maxima[field] = Math.max(...values);
  }
  return maxima;
}

/// Fields whose only correct value is zero, whatever the fleet was observed to
/// leak.
///
/// deriveBudgetCeiling derives a ceiling from the observation, which is right
/// for a cost that legitimately depends on the host (memory, CPU, descriptors)
/// and wrong for a leak: a teardown that stranded N children would have its
/// ceiling derived as N and ratify itself. That is not hypothetical — the
/// 2026-09-06 farm run derived cleanup_live_children ceilings of 129/513/2049
/// at the 128/512/2048 tiers, one per Run leaked, and passed every one. The
/// darwin baseline records 0 here, which is what a working teardown produces.
///
/// These stay pinned to 0 so a leak fails the verdict instead of setting the
/// standard for it.
const ABSOLUTE_ZERO_FIELDS = Object.freeze([
  "cleanup_live_children",
  "cleanup_attachments",
  "cleanup_threads_delta",
]);

/// Derive the full ceiling set for one tier/mode from its observed maxima.
///
/// Cost ceilings are deriveBudgetCeiling(field, observed) — the same rational
/// rule the darwin budget is pinned to. Additional timing/CPU totals use
/// the predeclared host-noise/measurement-quantum policy below, frozen with
/// the contract. A candidate cannot edit the derived ceilings. Leak fields (ABSOLUTE_ZERO_FIELDS) are not derived at all: their
/// ceiling is the constant 0, which no observation may raise.
function ceilingsForTier(maxima) {
  const ceilings = {};
  for (const field of OBSERVED_FIELDS) {
    ceilings[`max_${field}`] = ABSOLUTE_ZERO_FIELDS.includes(field)
      ? 0
      : deriveBudgetCeiling(field, maxima[field]);
  }
  // A predeclared host-noise policy, not a confidence interval: retain the
  // existing cost contract's 50% margin. Timing is serialized to 0.001 ms;
  // CPU deltas have two endpoint tick quantization errors. Utilization percent
  // describes parallelism and never penalizes faster completion of equal work.
  for (const field of [
    "list_latency_ms",
    "replay_wall_ms",
    "active_wall_ms",
    "active_cpu_ms",
  ]) {
    if (maxima[field] !== undefined)
      ceilings[`max_${field}`] = Math.max(
        maxima[field] * 1.5,
        field === "active_cpu_ms" ? 2 * maxima.cpu_tick_ms : 0.001,
      );
  }
  return ceilings;
}

/// Build the complete farm thresholds document from per-tier observation rounds.
///
/// `observations` is { host, modes: { idle: { "128": [c1,c2,c3], ... }, ... } }.
/// The output carries the host identity it was derived on so a later check can
/// enforce it, plus the observed maxima and the derived ceilings, mirroring
/// reliability-budgets.json's observation_baseline / budgets split without ever
/// being that file.
function deriveThresholds(observations) {
  assertHostClass(observations.host, "observation host");
  let executionEnvironment;
  const observedMaxima = {};
  const budgets = {};
  for (const mode of MODES) {
    observedMaxima[mode] = {};
    budgets[mode] = {};
    for (const tier of TIERS) {
      const rounds = observations.modes?.[mode]?.[String(tier)];
      const maxima = observedMaximaForTier(rounds, tier, mode);
      for (const cell of rounds) {
        assertExecutionEnvironment(cell.execution_environment, true);
        executionEnvironment ??= cell.execution_environment;
        assertSameExecutionEnvironment(
          executionEnvironment,
          cell.execution_environment,
        );
      }
      observedMaxima[mode][String(tier)] = maxima;
      budgets[mode][String(tier)] = ceilingsForTier(maxima);
    }
  }
  assertProvenance(observations.provenance);
  assertResourcePolicy(observations.resource_policy);
  const contractSha = measurementContractSha();
  return {
    schema: "ctxmux.fleet-scale-thresholds.v2",
    derived_from: {
      provenance: observations.provenance,
      resource_policy: FLEET_RESOURCE_POLICY,
      execution_environment: executionEnvironment,
      rounds: ROUNDS,
      tiers: TIERS,
      host: observations.host,
      measurement_contract_sha256: contractSha,
      note:
        "Ceilings are deriveBudgetCeiling(field, observed) over three farm rounds, " +
        "the same rule the darwin budget uses. This file is not reliability-budgets.json " +
        "and carries no darwin numbers; it is bound to the host class above and enforced against it.",
    },
    observed_maxima: observedMaxima,
    budgets,
  };
}

/// A stable fingerprint of the imported derivation rules.
///
/// Not the darwin measurement-contract hash (that lives in the budget file);
/// this is a self-check that the rules this harness derived against are the
/// ones it later verifies against, so a thresholds file cannot be silently
/// re-pointed at a different contract.
const FLEET_RESOURCE_POLICY = Object.freeze({
  live_runs: 4000,
  retained_runs: null,
  hot_output_bytes: 1073741824,
  live_event_bytes: 67108864,
  run_output_bytes: 4194304,
  metadata_bytes: 67108864,
  durable_replay_bytes: 268435456,
  durable_run_output_bytes: 4194304,
  database_bytes: 402653184,
  wal_checkpoint_bytes: 8388608,
  handoff_input_bytes: 134217728,
  handoff_diagnostic_bytes: 16777216,
  handoff_bytes: 268435456,
  control_state_bytes: 134217728,
  creation_workers: 8,
  input_workers: 8,
  cleanup_workers: 8,
  finalize_workers: 8,
  input_queue_commands: 1024,
  input_queue_bytes: 4194304,
  input_result_entries: 256,
  input_result_bytes: 1048576,
  tmux_discovery_bytes: 131072,
});
function measurementContractSha() {
  const hash = createHash("sha256");
  for (const name of [
    "reliability-budget-contract.mjs",
    "fleet-scale-measure.mjs",
    "check-fleet-scale.sh",
  ])
    hash
      .update(name)
      .update(readFileSync(new URL(`./${name}`, import.meta.url)));
  return hash.digest("hex");
}
function sourceIdentity(root = process.cwd()) {
  const names = execFileSync(
    "git",
    ["ls-files", "--cached", "--others", "--exclude-standard", "-z"],
    { encoding: "utf8", cwd: root },
  )
    .split("\0")
    .filter((name) =>
      /^(crates\/|third_party\/|\.cargo\/|build\.rs$|packages\/sdk\/src\/|scripts\/|Cargo\.(toml|lock)$|package(-lock)?\.json$|rust-toolchain\.toml$)/u.test(
        name,
      ),
    )
    .sort();
  const hash = createHash("sha256");
  for (const name of names)
    hash
      .update(name)
      .update("\0")
      .update(readFileSync(path.join(root, name)));
  return hash.digest("hex");
}
function assertResourcePolicy(policy) {
  if (
    !policy ||
    Object.keys(policy).length !== Object.keys(FLEET_RESOURCE_POLICY).length ||
    Object.entries(FLEET_RESOURCE_POLICY).some(
      ([key, value]) => policy[key] !== value,
    )
  )
    throw new Error("complete qualification resource policy changed");
}
function assertSampling(cell) {
  const sampling = cell?.rss_sampling;
  if (
    sampling?.complete !== true ||
    sampling.samples < 2 ||
    !Number.isFinite(sampling.max_gap_ms) ||
    sampling.max_gap_ms > 250 ||
    sampling.max_gap_ms < 0
  ) {
    throw new Error(
      "RSS sampling did not cover the complete workload within five 50 ms periods",
    );
  }
}
function assertExecutionEnvironment(environment, requireComplete) {
  if (
    environment?.schema !== "ctxmux.fleet-execution-environment.v1" ||
    !/^[a-f0-9]{64}$/u.test(environment.sha256 ?? "") ||
    environment.sha256 === createHash("sha256").update("").digest("hex") ||
    typeof environment.complete_cgroup_hierarchy !== "boolean"
  )
    throw new Error(
      "actual daemon execution environment is missing or invalid",
    );
  const raw = Buffer.from(environment.canonical_text_base64 ?? "", "base64");
  if (
    raw.length === 0 ||
    raw.toString("base64") !== environment.canonical_text_base64 ||
    createHash("sha256").update(raw).digest("hex") !== environment.sha256 ||
    !raw
      .toString("utf8")
      .startsWith("ctxmux.fleet-execution-environment.v1\n") ||
    !raw
      .toString("utf8")
      .endsWith(
        `complete_cgroup_hierarchy=${environment.complete_cgroup_hierarchy}\n`,
      )
  )
    throw new Error(
      "execution environment canonical evidence is missing or differs from its fingerprint",
    );
  if (requireComplete && !environment.complete_cgroup_hierarchy)
    throw new Error(
      "comparative performance requires the daemon's complete cgroup hierarchy; collect from the host namespace",
    );
}
function assertSameExecutionEnvironment(expected, actual) {
  assertExecutionEnvironment(expected, false);
  assertExecutionEnvironment(actual, false);
  if (
    expected.sha256 !== actual.sha256 ||
    expected.complete_cgroup_hierarchy !== actual.complete_cgroup_hierarchy
  )
    throw new Error(
      "actual daemon execution environment changed; kernel resource allocations cannot be scored as code improvements",
    );
}
function assertThresholdDerivation(thresholds) {
  assertExecutionEnvironment(
    thresholds.derived_from?.execution_environment,
    true,
  );
  if (
    thresholds.derived_from?.rounds !== ROUNDS ||
    JSON.stringify(thresholds.derived_from?.tiers) !== JSON.stringify(TIERS)
  )
    throw new Error("baseline workload tiers/rounds changed");
  for (const mode of MODES)
    for (const tier of TIERS) {
      const maxima = thresholds.observed_maxima?.[mode]?.[String(tier)];
      if (
        !maxima ||
        JSON.stringify(ceilingsForTier(maxima)) !==
          JSON.stringify(thresholds.budgets?.[mode]?.[String(tier)])
      )
        throw new Error(
          `baseline ceilings were modified after derivation: ${mode}/${tier}`,
        );
    }
}
function loadFrozenThresholds(filename, baselineRef) {
  if (!/^[a-f0-9]{40}$/u.test(baselineRef ?? ""))
    throw new Error(
      "accept requires a predeclared full --baseline-ref commit identity",
    );
  execFileSync("git", ["merge-base", "--is-ancestor", baselineRef, "HEAD"]);
  const root = execFileSync("git", ["rev-parse", "--show-toplevel"], {
    encoding: "utf8",
  }).trim();
  const relative = path.relative(root, path.resolve(filename));
  if (relative.startsWith("..") || path.isAbsolute(relative))
    throw new Error("frozen baseline must be a committed repository artifact");
  const frozen = execFileSync("git", ["show", `${baselineRef}:${relative}`]);
  const current = readFileSync(filename);
  if (!frozen.equals(current))
    throw new Error("baseline differs from the predeclared committed artifact");
  return {
    thresholds: JSON.parse(frozen),
    baselineAnchor: {
      commit: baselineRef,
      artifact_sha256: createHash("sha256").update(frozen).digest("hex"),
    },
  };
}
function assertProvenance(provenance) {
  for (const field of ["daemon_sha256", "client_sha256", "source_sha256"])
    if (!/^[a-f0-9]{64}$/.test(provenance?.[field] ?? ""))
      throw new Error(`missing binary/source identity ${field}`);
}
function judgeWorkload(cell, tier, mode) {
  const work = cell.workload;
  const input = mode === "active" ? 4096 : 0;
  const pass =
    work?.input_bytes_per_run === input &&
    work?.expected_output_bytes_per_run === input + 1 &&
    work?.driver_concurrency === 8 &&
    work?.live_runs_confirmed === tier &&
    work?.completed_runs === tier &&
    work?.byte_exact_runs === tier;
  return {
    pass,
    ...(pass
      ? {}
      : {
          reason: `tier ${tier} ${mode}: byte-exact fixed workload was incomplete or changed`,
        }),
  };
}

function assertHostClass(host, label) {
  if (host === null || typeof host !== "object") {
    throw new Error(`${label}: host identity is missing`);
  }
  for (const field of ["os", "os_release", "architecture", "logical_cpus"]) {
    if (
      host[field] === undefined ||
      host[field] === null ||
      host[field] === ""
    ) {
      throw new Error(`${label}: host identity field ${field} is empty`);
    }
  }
  if (!Number.isInteger(host.logical_cpus) || host.logical_cpus <= 0) {
    throw new Error(
      `${label}: host logical_cpus must be a positive integer (${host.logical_cpus})`,
    );
  }
  if (
    host.os !== REQUIRED_HOST_CLASS.platform ||
    host.architecture !== REQUIRED_HOST_CLASS.arch
  ) {
    throw new Error(
      `${label}: host class ${hostClassLabel(host)} does not match the ` +
        `required ${REQUIRED_HOST_CLASS.platform}-${REQUIRED_HOST_CLASS.arch}; farm thresholds ` +
        "are bound to their host and refuse a receipt from another kernel, the way the darwin " +
        "baseline should have refused a Linux one instead of gating it with a 3.25 fds ceiling",
    );
  }
}

// machine-id(5): one nonzero, lowercase 128-bit value rendered as 32 hex
// characters. Missing/uninitialized image identity cannot bind a host baseline.
// Canonical LF preserves the fingerprint of ordinary systemd machine-id files.
function machineIdentity(raw) {
  if (typeof raw !== "string" || !/^[a-f0-9]{32}\n?$/.test(raw))
    throw new Error("measurement host has no valid initialized machine-id");
  const id = raw.endsWith("\n") ? raw.slice(0, -1) : raw;
  if (id === "0".repeat(32))
    throw new Error(
      "measurement host has an uninitialized all-zero machine-id",
    );
  return createHash("sha256").update(`${id}\n`).digest("hex");
}

/// Enforce that two host identities are the same class before comparing numbers.
function assertSameHostClass(derivedHost, receiptHost) {
  assertHostClass(derivedHost, "thresholds host");
  assertHostClass(receiptHost, "receipt host");
  if (
    derivedHost.os !== receiptHost.os ||
    derivedHost.architecture !== receiptHost.architecture ||
    derivedHost.os_release !== receiptHost.os_release ||
    derivedHost.logical_cpus !== receiptHost.logical_cpus ||
    !/^[a-f0-9]{64}$/.test(derivedHost.machine_id_sha256 ?? "") ||
    derivedHost.machine_id_sha256 ===
      createHash("sha256").update("").digest("hex") ||
    derivedHost.machine_id_sha256 !== receiptHost.machine_id_sha256
  ) {
    throw new Error(
      `receipt host ${hostClassLabel(receiptHost)} does not match the ` +
        `thresholds host ${hostClassLabel(derivedHost)}; a measurement from a ` +
        "different host class cannot be judged against these thresholds",
    );
  }
}

/// The admission-cap precondition, checked before a tier's numbers are trusted.
///
/// This workload requests an explicit 4000-Run operator quota. The daemon also
/// funds actual native descriptors from RLIMIT_NOFILE; production defaults have
/// no fixed population ceiling. Descriptor pressure below the requested tier
/// is explicit, rather than silently reducing the accepted work or its quota.
/// At the workload's operator quota, one further request must refuse with
/// run_capacity. So a tier is reachable only where the host funds that many
/// descriptors; a tier the host cannot fund is a BLOCKED PRECONDITION. The
/// measuring side records how many Runs it actually admitted; if that is short
/// of the tier, it is reported loudly — not a fleet that quietly came up small
/// and produced flattering per-Run numbers.
function assertTierWasReached(cell, tier, mode) {
  const admitted = cell?.admitted_runs;
  if (!Number.isInteger(admitted) || admitted < 0) {
    throw new Error(
      `tier ${tier} ${mode}: census cell does not record admitted_runs, so a short ` +
        "fleet cannot be told from a full one",
    );
  }
  if (admitted < tier) {
    throw new Error(
      `tier ${tier} ${mode}: the daemon admitted only ${admitted} of ${tier} Runs. Live ` +
        "admission uses the explicit operator policy and actual RLIMIT_NOFILE descriptor " +
        "funding; a tier this host cannot fund descriptors for is a blocked " +
        "precondition rather than a measurable tier. Refusing to score a fleet that never " +
        "reached its target size.",
    );
  }
}

/// Compare one tier/mode measurement against its derived ceilings.
///
/// Returns a per-field verdict list. A NaN, negative, or missing measurement is
/// a failure with an explicit reason rather than a comparison that quietly
/// passes because NaN <= x is false handled up front.
function judgeCell(cell, ceilings, tier, mode) {
  assertCompleteCell(cell, `verdict tier ${tier} ${mode}`);
  assertTierWasReached(cell, tier, mode);
  const readings = readingsForCell(cell);
  const checks = [];
  for (const field of OBSERVED_FIELDS) {
    const value = readings[field];
    const ceiling = ceilings[`max_${field}`];
    if (!Number.isFinite(ceiling)) {
      throw new Error(
        `tier ${tier} ${mode}: no derived ceiling for ${field}, refusing to render a verdict`,
      );
    }
    const pass = Number.isFinite(value) && value >= 0 && value <= ceiling;
    checks.push({
      field,
      value,
      ceiling,
      pass,
      ...(pass
        ? {}
        : {
            reason: !Number.isFinite(value)
              ? "measurement is not a finite number"
              : value < 0
                ? "measurement is negative"
                : `measurement ${value} exceeds ceiling ${ceiling}`,
          }),
    });
  }
  return checks;
}

/// Retention is measured from retained=, never mapped from lifetime head=.
function readingsForCell(cell) {
  return {
    cpu_core_percent: cell.cpu_core_percent,
    peak_rss_kib: cell.peak_rss_kib,
    steady_rss_kib: cell.steady.rss_kib,
    retained_output_bytes_per_run: cell.retained_output_bytes_per_run,
    rss_kib_per_run: cell.rss_kib_per_run,
    threads_per_run: cell.threads_per_run,
    fds_per_run: cell.fds_per_run,
    cleanup_threads_delta: Math.max(
      0,
      cell.cleanup.threads - cell.baseline.threads,
    ),
    cleanup_live_children: cell.cleanup_live_children,
    cleanup_attachments: cell.cleanup_attachments,
  };
}

/// The 128-tier cross-check against the darwin gate.
///
/// The structural per-Run costs (descriptors, threads) are platform-invariant
/// by design, so the farm and darwin must agree on them at 128. Disagreement
/// means one of two things, and the check is only useful if it says which.
/// This returns the comparison so the report states it in the output rather
/// than hiding it.
///
/// AGREEMENT IS TO WITHIN A FEW WHOLE UNITS ACROSS THE FLEET, NOT TO THE
/// PRINTED DECIMAL. The census computes these as (steady - baseline) / admitted
/// and prints three decimals, so two effects move the quotient without any
/// per-Run cost changing: a fixed descriptor the daemon happens to hold at the
/// steady sample but not at the baseline (a log file, an accepted control
/// socket, an inherited pipe), and the rounding of the printed value itself.
/// Exact float equality would call that a structural disagreement and forfeit
/// the whole receipt over one descriptor that has nothing to do with per-Run
/// cost.
///
/// The budget is set by the size of the gap it must sit in. At the 128 tier one
/// stray fixed descriptor is 1 unit and print rounding adds at most 0.0005*128
/// = 0.064 more; a genuine off-by-one in FDS_PER_RUN is 128 units, and the
/// stale darwin thread baseline is 256. Four units admits the fixed overhead
/// with room for the rounding while staying 32x below the smallest real defect,
/// and that margin only widens at larger tiers. Fixed overhead is not waved
/// through unexamined either — absolute descriptor and thread cost is still
/// graded per tier by the derived ceilings. This check asks the narrower
/// question those cannot: do the two harnesses disagree about what one Run
/// costs?
const OVERLAP_UNIT_TOLERANCE = 4;

/// One field's farm/darwin comparison. Pure, so the self-test can exercise
/// every branch without a budgets file on disk.
function compareOverlapField(field, farmValue, darwinValue, fleet) {
  if (
    ![farmValue, darwinValue].every(
      (value) => Number.isFinite(value) && value >= 0,
    ) ||
    !Number.isInteger(fleet) ||
    fleet <= 0
  ) {
    throw new Error(`invalid resource observation for ${field}`);
  }
  const signedDelta = (farmValue - darwinValue) * fleet;
  const fleetDelta = Math.abs(signedDelta);
  const agree = signedDelta < OVERLAP_UNIT_TOLERANCE;
  return {
    field,
    farm: farmValue,
    darwin: darwinValue,
    fleet_delta_units: Number(fleetDelta.toFixed(3)),
    agree,
    direction: signedDelta > 0 ? "above" : signedDelta < 0 ? "below" : "equal",
    ...(signedDelta <= -OVERLAP_UNIT_TOLERANCE
      ? {
          improvement: true,
          reason: `resource cost below the historical baseline by ${fleetDelta.toFixed(1)} fleet units; correctness and workload checks remain required`,
        }
      : agree
        ? {}
        : {
            reason: `farm ${field} exceeds the historical baseline by ${fleetDelta.toFixed(1)} fleet units; investigate a regression or measurement mismatch`,
          }),
  };
}

function overlapCrossCheck(root, receipt, mode) {
  const farmCell = receipt.modes?.[mode]?.[String(OVERLAP_TIER)];
  assertCompleteCell(farmCell, `overlap cross-check ${mode}`);
  const farm = readingsForCell(farmCell);
  const darwin = darwinOverlapMaxima(root, mode);
  const fleet = farmCell.admitted_runs;
  const comparisons = OVERLAP_INVARIANT_FIELDS.map((field) =>
    compareOverlapField(field, farm[field], darwin[field], fleet),
  );
  return {
    tier: OVERLAP_TIER,
    mode,
    platform_invariant_fields: OVERLAP_INVARIANT_FIELDS,
    fleet_unit_tolerance: OVERLAP_UNIT_TOLERANCE,
    comparisons,
    agrees: comparisons.every((entry) => entry.agree),
  };
}

/// Name what actually failed across the tier verdicts.
///
/// A tier verdict is the conjunction of its ceiling checks, its List verdict
/// and its admission verdict, so `!pass` alone does not say which. This used
/// to report "exceeded a derived ceiling" unconditionally: the 2026-09-06 farm
/// run refused with that sentence while every one of its ceiling checks
/// passed, and the real cause was the admission predicate. A wrong reason is
/// worse than no reason — it sends the reader looking for a regression that
/// does not exist.
function tierRefusalReasons(tierVerdicts) {
  const reasons = [];
  const breaches = tierVerdicts.flatMap((entry) =>
    entry.checks
      .filter((check) => !check.pass)
      .map((check) => `${entry.tier} ${entry.mode} ${check.field}`),
  );
  if (breaches.length > 0) {
    reasons.push(`a derived ceiling was exceeded: ${breaches.join(", ")}`);
  }
  const VERDICT_SUFFIX = "_verdict";
  for (const entry of tierVerdicts) {
    // Every named sub-verdict a cell carries, not a hand-listed pair. A verdict
    // whose reason is not collected here still fails the cell — `pass` is
    // computed independently — but it fails it silently, with the top-level
    // refusal saying nothing about why. Deriving the list from the cell means
    // adding a sub-verdict cannot leave the reader without an explanation.
    const verdicts = Object.entries(entry).filter(([key]) =>
      key.endsWith(VERDICT_SUFFIX),
    );
    for (const [key, verdict] of verdicts) {
      if (verdict && !verdict.pass) {
        const name = key.slice(0, -VERDICT_SUFFIX.length);
        reasons.push(
          verdict.reason ?? `tier ${entry.tier} ${entry.mode}: ${name} failed`,
        );
      }
    }
  }
  return reasons;
}

/// Render the full acceptance verdict for a receipt against a thresholds file.
///
/// Fails closed: any tier/mode that cannot be judged (missing cell, unreached
/// tier, host mismatch, contract drift) is a nonzero exit, not an omission.
function renderVerdict({ thresholds, receipt, root, baselineAnchor }) {
  if (thresholds?.schema !== "ctxmux.fleet-scale-thresholds.v2") {
    throw new Error(
      `thresholds file has unexpected schema ${JSON.stringify(thresholds?.schema)}`,
    );
  }
  if (
    !/^[a-f0-9]{40}$/u.test(baselineAnchor?.commit ?? "") ||
    !/^[a-f0-9]{64}$/u.test(baselineAnchor?.artifact_sha256 ?? "")
  )
    throw new Error("missing frozen baseline anchor");
  assertThresholdDerivation(thresholds);
  assertSameHostClass(thresholds.derived_from?.host, receipt?.host);
  assertProvenance(thresholds.derived_from.provenance);
  assertProvenance(receipt.provenance);
  assertResourcePolicy(thresholds.derived_from.resource_policy);
  assertResourcePolicy(receipt.resource_policy);
  if (
    thresholds.derived_from.provenance.source_sha256 ===
    receipt.provenance.source_sha256
  )
    throw new Error(
      "candidate cannot certify itself from its own observations",
    );
  const expectedSha = measurementContractSha();
  if (thresholds.derived_from?.measurement_contract_sha256 !== expectedSha) {
    throw new Error(
      "thresholds were derived against a different derivation contract than this harness " +
        "imports; refusing to judge measurements against ceilings whose rules have drifted",
    );
  }

  const tierVerdicts = [];
  for (const mode of MODES) {
    for (const tier of TIERS) {
      const cell = receipt.modes?.[mode]?.[String(tier)];
      const ceilings = thresholds.budgets?.[mode]?.[String(tier)];
      if (ceilings === undefined) {
        throw new Error(
          `thresholds file has no ceilings for ${mode} tier ${tier}; cannot render a verdict`,
        );
      }
      assertSampling(cell);
      assertExecutionEnvironment(cell.execution_environment, true);
      assertSameExecutionEnvironment(
        thresholds.derived_from.execution_environment,
        cell.execution_environment,
      );
      if (
        cell.cpu_tick_ms !==
        thresholds.observed_maxima[mode][String(tier)].cpu_tick_ms
      )
        throw new Error("CPU measurement resolution changed");
      const checks = judgeCell(cell, ceilings, tier, mode);
      for (const field of [
        "list_latency_ms",
        "replay_wall_ms",
        ...(mode === "active" ? ["active_wall_ms", "active_cpu_ms"] : []),
      ]) {
        const value = cell[field],
          ceiling = ceilings[`max_${field}`];
        checks.push({
          field,
          value,
          ceiling,
          pass:
            Number.isFinite(value) &&
            value >= 0 &&
            Number.isFinite(ceiling) &&
            value <= ceiling,
        });
      }
      const workload = judgeWorkload(cell, tier, mode);
      tierVerdicts.push({
        tier,
        mode,
        checks,
        workload,
        admitted_runs: cell.admitted_runs,
        list_latency_ms: cell.list_latency_ms ?? null,
        list_success: cell.list_success ?? null,
        admission_at_ceiling: cell.admission_at_ceiling ?? null,
        // Reported, deliberately NOT judged, and the distinction matters.
        //
        // This is the sum of `head=` across the fleet: each Run's cumulative
        // lifetime output counter. It is not retention. `OutputLog` evicts
        // chunks past OUTPUT_RETENTION_BYTES and the fleet-wide budget trims
        // further, so retained bytes can fall while this only ever rises.
        // Grading it against the 1 GiB cap of #47 would fail a healthy daemon
        // as soon as its Runs had *emitted* a gigabyte, whenever emitted.
        // `aggregate_retained_bytes`, judged below, is the quantity that cap
        // actually bounds; this one stays context.
        aggregate_output_bytes_lifetime:
          cell.aggregate_output_bytes_lifetime ?? null,
        aggregate_retained_bytes: cell.aggregate_retained_bytes ?? null,
        retention_verdict: judgeRetentionBudget(cell),
        pass:
          workload.pass &&
          checks.every((entry) => entry.pass) &&
          judgeListBehaviour(cell, tier, mode).pass &&
          judgeAdmissionBehaviour(cell, tier, mode).pass &&
          judgeRetentionBudget(cell).pass,
        list_verdict: judgeListBehaviour(cell, tier, mode),
        admission_verdict: judgeAdmissionBehaviour(cell, tier, mode),
      });
    }
  }

  const overlaps = MODES.map((mode) => overlapCrossCheck(root, receipt, mode));
  const overlapAgrees = overlaps.every((entry) => entry.agrees);
  const tiersPass = tierVerdicts.every((entry) => entry.pass);
  return {
    schema: "ctxmux.fleet-scale-verdict.v1",
    provenance: receipt.provenance,
    baseline_provenance: thresholds.derived_from.provenance,
    resource_policy: FLEET_RESOURCE_POLICY,
    execution_environment: thresholds.derived_from.execution_environment,
    metric_scope: [
      "byte_exact_output",
      "admission",
      "teardown",
      "idle_cpu",
      "active_cpu",
      "active_completion_time",
      "list_latency",
      "sampled_peak_rss",
      "steady_rss",
      "fd",
      "thread",
      "hot_replay_bytes",
    ],
    host: receipt.host,
    thresholds_host: thresholds.derived_from.host,
    tiers: tierVerdicts,
    overlap_cross_check: overlaps,
    accepted: tiersPass && overlapAgrees,
    baseline_anchor: baselineAnchor,
    ...(tiersPass && overlapAgrees
      ? {}
      : {
          refusal_reasons: [
            ...tierRefusalReasons(tierVerdicts),
            // Carry the per-field reason up rather than restating a generic
            // "disagreed" line. The two directions have opposite remedies —
            // above the baseline means investigate the daemon, below it means
            // re-baseline darwin — and a summary that erases the difference
            // points the reader at the wrong one.
            ...overlaps.flatMap((entry) =>
              entry.comparisons
                .filter((comparison) => !comparison.agree)
                .map(
                  (comparison) =>
                    `${entry.mode} ${OVERLAP_TIER}: ${comparison.reason}`,
                ),
            ),
          ],
        }),
  };
}

/// Verdict on List latency and success at a tier.
///
/// #49: List must not silently drop past the frame cap. Success must be true
/// and latency a finite non-negative number; a missing or false success is a
/// failure, because a fleet that cannot be enumerated is not an accepted fleet.
function judgeListBehaviour(cell, tier, mode) {
  const success = cell.list_success;
  const latency = cell.list_latency_ms;
  // A census whose daemon died partway reports zeros that all read as passing:
  // no children to leak, no stat file so idle CPU computes as 0.000, and a
  // failed List so the teardown loop stops nothing and counts no failures. That
  // cell is not merely green, it outscores a healthy one. The census records
  // liveness explicitly for exactly this reason, so a cell that does not carry
  // the field is a cell whose zeros were never corroborated — absent fails the
  // same as false, or an older receipt would slip through unexamined.
  if (cell.daemon_alive_after_census !== true) {
    return {
      pass: false,
      reason:
        cell.daemon_alive_after_census === undefined
          ? `tier ${tier} ${mode}: census did not record daemon liveness, so its zero readings are not evidence`
          : `tier ${tier} ${mode}: the census daemon was dead after the census, so every reading in this cell describes a dead process`,
    };
  }
  // A teardown that could not stop its Runs invalidates every cleanup reading
  // in this cell, so it is reported here rather than left to be inferred from
  // a leak counter that would otherwise look like a product defect.
  const stopFailures = cell.cleanup_stop_failures;
  if (Number.isFinite(stopFailures) && stopFailures > 0) {
    return {
      pass: false,
      reason: `tier ${tier} ${mode}: teardown failed to stop ${stopFailures} Run(s), so the cleanup readings are not evidence`,
    };
  }
  if (success !== true) {
    return {
      pass: false,
      reason: `tier ${tier} ${mode}: List did not succeed across the whole fleet (success=${success})`,
    };
  }
  if (!Number.isFinite(latency) || latency < 0) {
    return {
      pass: false,
      reason: `tier ${tier} ${mode}: List latency is not a finite non-negative number (${latency})`,
    };
  }
  // Grading latency only for finiteness let the one regression this harness was
  // built to catch pass: #49 was List degrading past ~1000 Runs, and a decay to
  // whole seconds satisfies "is a number". A *derived* ceiling is wrong here for
  // the reason leak ceilings were wrong — it would ratify whatever we measured.
  // The bound is therefore fixed in advance and set where no healthy host lands:
  // the farm's own curve runs 8.2 ms at 128 to 26.5 ms at 4000, and tmux at 300
  // sessions lists in 8.1 ms, so a whole second is two orders of magnitude past
  // anything observed while still being unambiguously a caller-visible stall.
  if (latency > LIST_LATENCY_CEILING_MS) {
    return {
      pass: false,
      reason: `tier ${tier} ${mode}: List took ${round(latency)} ms, past the ${LIST_LATENCY_CEILING_MS} ms ceiling — enumerating the fleet is a caller-visible stall`,
    };
  }
  return { pass: true, latency_ms: round(latency) };
}

/// The retained-byte envelope explicitly passed by FLEET_RESOURCE_POLICY.
///
/// This is a fixed workload promise, independent of production defaults and
/// measured cost. Cost ceilings also come from independent frozen observations;
/// a candidate cannot authorize itself. Changing the workload resource envelope
/// requires new observations, rather than silently reusing the old baseline.
const RETENTION_BUDGET_CEILING_BYTES = 1024 * 1024 * 1024;

/// Verdict on the fleet-wide retained-byte cap of #47.
///
/// The daemon promises that the sum of what every Run is holding stays at or
/// under RETENTION_BUDGET_BYTES, trimming across Run boundaries to keep it
/// there. This re-derives that sum from the listing rows — independently of the
/// daemon's own running total — and refuses if the fleet is over.
///
/// A cell that carries no `aggregate_retained_bytes` at all is NOT waved
/// through. Absence used to be the normal case (the field was not on the wire),
/// so treating it as "nothing to check" is exactly how this cap went unproven
/// through an entire campaign. Now that every listing row carries it, a missing
/// value means the census could not read it, and an unmeasured cap fails.
function judgeRetentionBudget(cell) {
  const retained = cell.aggregate_retained_bytes;
  if (typeof retained !== "number" || !Number.isFinite(retained)) {
    return {
      pass: false,
      reason:
        "aggregate_retained_bytes is absent from this cell, so the daemon-wide " +
        "retention cap was not measured. Every listing row carries retained= " +
        "now, so absence means the census could not read it — an unmeasured " +
        "cap is not a satisfied one",
    };
  }
  if (retained < 0) {
    return {
      pass: false,
      reason: `aggregate_retained_bytes is ${retained}; retained bytes cannot be negative, so the sum is corrupt`,
    };
  }
  if (retained > RETENTION_BUDGET_CEILING_BYTES) {
    return {
      pass: false,
      retained_bytes: retained,
      ceiling_bytes: RETENTION_BUDGET_CEILING_BYTES,
      reason:
        `the fleet is holding ${retained} retained output bytes, past the ` +
        `${RETENTION_BUDGET_CEILING_BYTES} byte daemon-wide cap. Cross-Run ` +
        "reclamation is meant to hold this line no matter how many Runs stream " +
        "at once; over it, an agent runtime's memory grows with fleet size " +
        "instead of staying bounded",
    };
  }
  return {
    pass: true,
    retained_bytes: retained,
    ceiling_bytes: RETENTION_BUDGET_CEILING_BYTES,
  };
}

/// Verdict on admission behaviour at the descriptor ceiling.
///
/// The daemon must refuse excess Runs cleanly with run_capacity and never hit
/// EMFILE ("Too many open files"). A cell that reports an EMFILE, or does not
/// report a clean refusal at all, fails: opaque descriptor exhaustion is the
/// exact failure ADR 013's budget exists to prevent.
function judgeAdmissionBehaviour(cell, tier, mode) {
  const admission = cell.admission_at_ceiling;
  if (admission === null || admission === undefined) {
    // Only the tier at the daemon's own cap exercises refusal; below it there
    // is nothing to refuse, so absence is acceptable there and only there.
    //
    // That cap is this workload's explicit live_runs policy, not the overlap tier. This predicate
    // used to read `tier <= OVERLAP_TIER` (128) — the deleted count cap — so
    // the 512 and 2048 tiers, which sit below the real ceiling and cannot
    // refuse anything, were failed for not refusing.
    if (tier < DAEMON_ADMISSION_CAP) {
      return {
        pass: true,
        note: "no ceiling refusal exercised below the daemon cap",
      };
    }
    return {
      pass: false,
      reason: `tier ${tier} ${mode}: admission behaviour at the ceiling was not observed`,
    };
  }
  if (
    admission.emfile === true ||
    new RegExp("too many open files", "iu").test(admission.error ?? "")
  ) {
    return {
      pass: false,
      reason: `tier ${tier} ${mode}: the daemon hit EMFILE at the ceiling instead of refusing cleanly`,
    };
  }
  if (admission.refused_cleanly !== true) {
    return {
      pass: false,
      reason: `tier ${tier} ${mode}: excess admission was not refused cleanly (${JSON.stringify(admission)})`,
    };
  }
  return { pass: true, refused_with: admission.error_code ?? "run_capacity" };
}

/// Prove the harness fails loudly instead of rendering a false verdict.
///
/// Each case is a way this harness could have accepted a fleet it did not
/// measure. A self-test that only exercised the happy path would leave exactly
/// the failure mode a farm acceptance harness must not have: a green verdict
/// with nothing behind it. This runs on any host in seconds without a fleet.
function selfTest() {
  const failures = [];
  const expectFailure = (name, run) => {
    try {
      run();
    } catch (error) {
      console.log(
        `  ok    ${name}: refused with "${error.message.split("\n")[0]}"`,
      );
      return;
    }
    failures.push(name);
    console.log(`  FAIL  ${name}: accepted what it must refuse`);
  };
  const expectSuccess = (name, run) => {
    try {
      const value = run();
      console.log(`  ok    ${name}: ${value}`);
    } catch (error) {
      failures.push(name);
      console.log(`  FAIL  ${name}: ${error.message.split("\n")[0]}`);
    }
  };

  const environmentFixture = (complete = true, allocation = "fixed") => {
    const raw = `ctxmux.fleet-execution-environment.v1\nfixture allocation=${allocation}\ncomplete_cgroup_hierarchy=${complete}\n`;
    return {
      schema: "ctxmux.fleet-execution-environment.v1",
      sha256: createHash("sha256").update(raw).digest("hex"),
      canonical_text_base64: Buffer.from(raw).toString("base64"),
      complete_cgroup_hierarchy: complete,
    };
  };
  const goodCell = (overrides = {}) => ({
    execution_environment: environmentFixture(),
    workload: {
      input_bytes_per_run: 0,
      expected_output_bytes_per_run: 1,
      driver_concurrency: 8,
      live_runs_confirmed: 128,
      completed_runs: 128,
      byte_exact_runs: 128,
    },
    active_wall_ms: 10,
    active_cpu_core_percent: 1,
    active_cpu_ms: 1,
    cpu_tick_ms: 10,
    replay_wall_ms: 10,
    rss_sampling: { complete: true, samples: 100, max_gap_ms: 50 },
    cpu_core_percent: 1,
    peak_rss_kib: 10000,
    retained_output_bytes_per_run: 0,
    rss_kib_per_run: 100,
    threads_per_run: 2,
    fds_per_run: 3,
    cleanup_live_children: 0,
    cleanup_attachments: 0,
    daemon_alive_after_census: true,
    steady: { rss_kib: 9000 },
    baseline: { threads: 8 },
    cleanup: { threads: 8 },
    admitted_runs: 128,
    list_success: true,
    list_latency_ms: 5,
    aggregate_output_bytes_lifetime: 0,
    // A healthy fleet well under the 1 GiB daemon-wide cap.
    aggregate_retained_bytes: 64 * 1024 * 1024,
    admission_at_ceiling: {
      refused_cleanly: true,
      emfile: false,
      error_code: "run_capacity",
    },
    ...overrides,
  });

  expectSuccess(
    "the actual daemon collector binds limits and all visible ancestors",
    () => {
      const producer = readFileSync(
        new URL("./check-fleet-scale.sh", import.meta.url),
        "utf8",
      );
      const begin = producer.indexOf("collect_execution_environment() {");
      const end = producer.indexOf("\n}\n", begin) + 3;
      if (begin < 0 || end < 3)
        throw new Error("execution collector not found");
      const directory = mkdtempSync(
        path.join(tmpdir(), "ctxmux-execution-envelope-"),
      );
      const put = (name, value) => {
        mkdirSync(path.dirname(path.join(directory, name)), {
          recursive: true,
        });
        writeFileSync(path.join(directory, name), value);
      };
      try {
        put(
          "proc/self/mountinfo",
          `0 0 0:0 / ${directory}/cgroup rw - cgroup2 cgroup rw\n`,
        );
        put("proc/42/cgroup", "0::/parent/leaf\n");
        put(
          "proc/42/limits",
          "Limit Soft Hard Units\nMax open files 65536 1048576 files\n",
        );
        put(
          "proc/42/status",
          "Cpus_allowed_list:\t0-7\nMems_allowed_list:\t0\n",
        );
        for (const name of ["cgroup", "cgroup/parent", "cgroup/parent/leaf"])
          put(`${name}/cgroup.controllers`, "cpu cpuset memory pids io\n");
        const configs = [
          ["cgroup/parent/leaf/cpu.max", "800000 100000\n", "max 100000\n"],
          ["cgroup/parent/cpu.max", "max 100000\n", "200000 100000\n"],
          ["cgroup/parent/leaf/memory.max", "17179869184\n", "max\n"],
          ["cgroup/parent/memory.max", "max\n", "8589934592\n"],
          ["cgroup/parent/leaf/pids.max", "8192\n", "max\n"],
          [
            "proc/42/limits",
            "Limit Soft Hard Units\nMax open files 65536 1048576 files\n",
            "Limit Soft Hard Units\nMax open files 32768 1048576 files\n",
          ],
          [
            "proc/42/status",
            "Cpus_allowed_list:\t0-7\nMems_allowed_list:\t0\n",
            "Cpus_allowed_list:\t0-3\nMems_allowed_list:\t0\n",
          ],
        ];
        for (const [name, before] of configs) put(name, before);
        const collect = () =>
          JSON.parse(
            execFileSync(
              "bash",
              [
                "-euo",
                "pipefail",
                "-c",
                `proc=$1\n${producer.slice(begin, end)}\ncollect_execution_environment 42 "$2"`,
                "_",
                path.join(directory, "proc"),
                path.join(directory, "environment.txt"),
              ],
              { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] },
            ),
          );
        let writeFailureRejected = false;
        try {
          execFileSync(
            "bash",
            [
              "-euo",
              "pipefail",
              "-c",
              `proc=$1\n${producer.slice(begin, end)}\nprintf() { if [[ \${2-} == 'Max open files '* ]]; then return 1; fi; builtin printf "$@"; }\nif collect_execution_environment 42 "$2"; then exit 0; else exit 7; fi`,
              "_",
              path.join(directory, "proc"),
              path.join(directory, "environment.txt"),
            ],
            { stdio: "pipe" },
          );
        } catch {
          writeFailureRejected = true;
        }
        if (!writeFailureRejected)
          throw new Error(
            "a failed intermediate manifest write produced passing evidence",
          );
        const baseline = collect();
        assertExecutionEnvironment(baseline, true);
        for (const changed of [
          { ...baseline, canonical_text_base64: undefined },
          {
            ...baseline,
            canonical_text_base64: baseline.canonical_text_base64 + "!",
          },
          { ...baseline, sha256: "1".repeat(64) },
          { ...baseline, complete_cgroup_hierarchy: false },
        ]) {
          let refused = false;
          try {
            assertExecutionEnvironment(changed, false);
          } catch {
            refused = true;
          }
          if (!refused)
            throw new Error(
              "missing, edited or mis-scoped canonical evidence accepted",
            );
        }

        for (const [name, before, after] of configs) {
          put(name, after);
          const changed = collect();
          if (changed.sha256 === baseline.sha256)
            throw new Error(`unbound configuration: ${name}`);
          let rejected = false;
          try {
            assertSameExecutionEnvironment(baseline, changed);
          } catch {
            rejected = true;
          }
          if (!rejected)
            throw new Error(`changed allocation accepted: ${name}`);
          put(name, before);
        }
        put("cgroup/cgroup.events", "populated 1\n");
        const hidden = collect();
        if (hidden.complete_cgroup_hierarchy !== false)
          throw new Error("namespace root accepted as full hierarchy");
        let refused = false;
        try {
          assertExecutionEnvironment(hidden, true);
        } catch {
          refused = true;
        }
        if (!refused)
          throw new Error("hidden ancestors qualified comparative performance");
        rmSync(path.join(directory, "cgroup/parent/cgroup.controllers"));
        try {
          collect();
          throw new Error("missing ancestor silently accepted");
        } catch (error) {
          if (error.message === "missing ancestor silently accepted")
            throw error;
        }
        return "real producer: quota, ancestor quota, memory, pids, affinity and ready-PID RLIMIT changes refused; hidden/missing ancestors fail comparative qualification";
      } finally {
        rmSync(directory, { recursive: true, force: true });
      }
    },
  );

  expectSuccess(
    "the real census recognizes only the CLI quota error code",
    () => {
      const producer = readFileSync(
        new URL("./check-fleet-scale.sh", import.meta.url),
        "utf8",
      );
      const begin = producer.indexOf("is_run_capacity_error() {");
      const end = producer.indexOf("\n}\n", begin) + 3;
      if (begin < 0 || end < 3)
        throw new Error("census admission classifier not found");
      const classifier = producer.slice(begin, end);
      for (const [message, expected] of [
        [
          "ctxmux: ctxmux request failed (RunCapacity): physical live Run resources exhausted at 4000 slots",
          "true",
        ],
        [
          "ctxmux: ctxmux request failed (BackendUnavailable): live Run capacity helper failed",
          "false",
        ],
        [
          "ctxmux: ctxmux request failed (BackendUnavailable): RunCapacity diagnostic text",
          "false",
        ],
        ["ctxmux: failed to connect to ctxmux daemon", "false"],
      ]) {
        const actual = execFileSync(
          "bash",
          [
            "-euo",
            "pipefail",
            "-c",
            classifier +
              '\nif is_run_capacity_error "$1"; then printf true; else printf false; fi',
            "_",
            message,
          ],
          { encoding: "utf8" },
        );
        if (actual !== expected)
          throw new Error(
            `quota error ${JSON.stringify(message)}: expected ${expected}, got ${actual}`,
          );
      }
      return "captured real quota error; unrelated capacity text cannot pass";
    },
  );

  expectSuccess(
    "the real census counts exact tab-separated output heads",
    () => {
      const producer = readFileSync(
        new URL("./check-fleet-scale.sh", import.meta.url),
        "utf8",
      );
      const begin = producer.indexOf("count_output_heads() {");
      const end = producer.indexOf("\n}\n", begin) + 3;
      if (begin < 0 || end < 3) throw new Error("census counter not found");
      const counter = producer.slice(begin, end);
      const listing =
        "run1\tRunning\thead=1\tretained=1\nrun2\tRunning\thead=11\tretained=11\nrun3\tRunning\tretained=1\nrun4\tRunning\thead=1\n";
      for (const [expected, count] of [
        [1, 2],
        [11, 1],
        [4097, 0],
      ]) {
        const actual = execFileSync(
          "bash",
          [
            "-euo",
            "pipefail",
            "-c",
            counter + `\ncount_output_heads ${expected}`,
          ],
          { input: listing, encoding: "utf8" },
        ).trim();
        if (actual !== String(count))
          throw new Error(`head=${expected}: expected ${count}, got ${actual}`);
      }
    },
  );

  expectSuccess("the actual shell producer emits the judged contract", () => {
    const producer = readFileSync(
      new URL("./check-fleet-scale.sh", import.meta.url),
      "utf8",
    );
    const begin = producer.indexOf("\nprintf '{'\n");
    const end = producer.indexOf("\nCENSUS\n", begin);
    if (begin < 0 || end < 0) throw new Error("census formatter not found");
    const assignments = `
execution_environment='${JSON.stringify(goodCell().execution_environment)}'
input_bytes=0 expected_bytes=1 live_runs_confirmed=128 completed_runs=128 byte_exact_runs=128
active_wall_ms=10 active_cpu_percent=1 active_cpu_ms=1 cpu_tick_ms=10 replay_wall_ms=10
sampling_complete=true sampling_count=100 sampling_max_gap=50 admitted=128 daemon_alive_after_census=true
cpu_core_percent=1 peak_rss=10000 retained_per_run=1 rss_per_run=100
threads_per_run=2 fds_per_run=3 cleanup_children=0 stop_failures=0
cleanup_attachments=0 steady_rss=9000 baseline_threads=8 cleanup_threads=8
list_latency=5 list_success=true aggregate_bytes=128 aggregate_retained_bytes=128
refused_clean=0 emfile=0
`;
    const cell = JSON.parse(
      execFileSync(
        "bash",
        ["-euo", "pipefail", "-c", assignments + producer.slice(begin, end)],
        { encoding: "utf8" },
      ),
    );
    assertCompleteCell(cell, "real producer");
    if (
      !judgeWorkload(cell, 128, "idle").pass ||
      !judgeListBehaviour(cell, 128, "idle").pass
    ) {
      throw new Error("producer and judge disagree");
    }
    return "census printf output, not a hand-written JSON fixture";
  });
  expectFailure(
    "exact replay without live PID ownership cannot establish capacity",
    () => {
      const cell = goodCell();
      cell.workload.live_runs_confirmed = 0;
      observedMaximaForTier([cell, cell, cell], 128, "idle");
    },
  );
  expectFailure("missing live PID evidence cannot establish capacity", () => {
    const cell = goodCell();
    delete cell.workload.live_runs_confirmed;
    observedMaximaForTier([cell, cell, cell], 128, "idle");
  });
  expectFailure(
    "zero completed active work cannot establish a baseline",
    () => {
      const cell = goodCell({
        workload: {
          input_bytes_per_run: 4096,
          expected_output_bytes_per_run: 4097,
          driver_concurrency: 8,
          live_runs_confirmed: 0,
          completed_runs: 0,
          byte_exact_runs: 0,
        },
      });
      observedMaximaForTier([cell, cell, cell], 128, "active");
    },
  );
  expectSuccess(
    "high candidate cost fails independently frozen ceilings",
    () => {
      const ceilings = ceilingsForTier(
        observedMaximaForTier(
          [goodCell(), goodCell(), goodCell()],
          128,
          "idle",
        ),
      );
      const checks = judgeCell(
        goodCell({ cpu_core_percent: 600, peak_rss_kib: 8 * 1024 * 1024 }),
        ceilings,
        128,
        "idle",
      );
      for (const field of ["cpu_core_percent", "peak_rss_kib"]) {
        if (checks.find((check) => check.field === field)?.pass !== false)
          throw new Error(`${field} regression self-certified`);
      }
      return "600% CPU and 8 GiB RSS cannot redefine their acceptance bar";
    },
  );

  expectFailure("a partial RSS sample cannot certify peak memory", () =>
    assertSampling(
      goodCell({ rss_sampling: { complete: true, samples: 1, max_gap_ms: 0 } }),
    ),
  );
  expectSuccess("faster equal-CPU work is a performance improvement", () => {
    const baseline = goodCell({
      active_wall_ms: 10000,
      active_cpu_ms: 1000,
      workload: {
        input_bytes_per_run: 4096,
        expected_output_bytes_per_run: 4097,
        driver_concurrency: 8,
        live_runs_confirmed: 128,
        completed_runs: 128,
        byte_exact_runs: 128,
      },
    });
    const ceilings = ceilingsForTier(
      observedMaximaForTier([baseline, baseline, baseline], 128, "active"),
    );
    if (1000 > ceilings.max_active_wall_ms || 1000 > ceilings.max_active_cpu_ms)
      throw new Error("equal CPU work rejected for higher utilization");
    return "ten times faster with equal total CPU remains eligible";
  });
  expectFailure("accept cannot use an uncommitted or unanchored baseline", () =>
    loadFrozenThresholds("unused", "HEAD"),
  );

  expectSuccess(
    "source identity includes local PTY dependency and Cargo configuration",
    () => {
      const directory = mkdtempSync(
        path.join(tmpdir(), "ctxmux-source-identity-"),
      );
      try {
        execFileSync("git", ["init", "--quiet", directory]);
        execFileSync("mkdir", [
          "-p",
          path.join(directory, "third_party/portable-pty/src"),
          path.join(directory, ".cargo"),
          path.join(directory, "docs"),
        ]);
        writeFileSync(
          path.join(directory, "third_party/portable-pty/src/lib.rs"),
          "first",
        );
        writeFileSync(path.join(directory, ".cargo/config.toml"), "first");
        const first = sourceIdentity(directory);
        writeFileSync(
          path.join(directory, "third_party/portable-pty/src/lib.rs"),
          "changed",
        );
        const second = sourceIdentity(directory);
        writeFileSync(path.join(directory, ".cargo/config.toml"), "changed");
        const third = sourceIdentity(directory);
        writeFileSync(path.join(directory, "docs/baseline.json"), "receipt");
        if (
          first === second ||
          second === third ||
          third !== sourceIdentity(directory)
        )
          throw new Error(
            "build input or receipt exclusion is not bound correctly",
          );
        return "PTY and Cargo changes alter identity; receipt artifacts cannot fake a new candidate";
      } finally {
        rmSync(directory, { recursive: true, force: true });
      }
    },
  );

  expectSuccess("frozen artifact identity detects real file changes", () => {
    const directory = mkdtempSync(
      path.join(tmpdir(), "ctxmux-frozen-baseline-"),
    );
    const previous = process.cwd();
    try {
      process.chdir(directory);
      execFileSync("git", ["init", "--quiet"]);
      writeFileSync("baseline.json", '{"example":1}\n');
      execFileSync("git", ["add", "baseline.json"]);
      execFileSync("git", [
        "-c",
        "commit.gpgsign=false",
        "-c",
        "user.name=Fixture",
        "-c",
        "user.email=fixture@example.invalid",
        "commit",
        "--quiet",
        "-m",
        "Freeze baseline",
      ]);
      const ref = execFileSync("git", ["rev-parse", "HEAD"], {
        encoding: "utf8",
      }).trim();
      loadFrozenThresholds("baseline.json", ref);
      writeFileSync("baseline.json", '{"example":2}\n');
      let rejected = false;
      try {
        loadFrozenThresholds("baseline.json", ref);
      } catch {
        rejected = true;
      }
      if (!rejected)
        throw new Error("edited baseline passed committed identity");
      return "actual Git artifact accepted, modified artifact refused";
    } finally {
      process.chdir(previous);
      rmSync(directory, { recursive: true, force: true });
    }
  });
  expectSuccess(
    "full verdict accepts valid work and refuses edited ceilings or self-certification",
    () => {
      const host = {
        os: "linux",
        os_release: "fixture-kernel",
        architecture: "x64",
        logical_cpus: 64,
        machine_id_sha256: "a".repeat(64),
      };
      const provenance = {
        daemon_sha256: "b".repeat(64),
        client_sha256: "c".repeat(64),
        source_sha256: "d".repeat(64),
      };
      const observations = {
        host,
        provenance,
        resource_policy: FLEET_RESOURCE_POLICY,
        modes: {},
      };
      const receipt = {
        host,
        provenance: { ...provenance, source_sha256: "e".repeat(64) },
        resource_policy: FLEET_RESOURCE_POLICY,
        modes: {},
      };
      for (const mode of MODES) {
        observations.modes[mode] = {};
        receipt.modes[mode] = {};
        for (const tier of TIERS) {
          const input = mode === "active" ? 4096 : 0;
          const cell = goodCell({
            admitted_runs: tier,
            workload: {
              input_bytes_per_run: input,
              expected_output_bytes_per_run: input + 1,
              driver_concurrency: 8,
              live_runs_confirmed: tier,
              completed_runs: tier,
              byte_exact_runs: tier,
            },
          });
          observations.modes[mode][tier] = [cell, cell, cell];
          receipt.modes[mode][tier] = cell;
        }
      }
      const thresholds = deriveThresholds(observations);
      const baselineAnchor = {
        commit: "f".repeat(40),
        artifact_sha256: "f".repeat(64),
      };
      if (
        !renderVerdict({ thresholds, receipt, root: ".", baselineAnchor })
          .accepted
      )
        throw new Error("complete fixed work failed its frozen baseline");
      for (const environment of [
        undefined,
        { ...goodCell().execution_environment, sha256: "1".repeat(64) },
        {
          ...goodCell().execution_environment,
          complete_cgroup_hierarchy: false,
        },
      ]) {
        const changed = structuredClone(receipt);
        changed.modes.active["4000"].execution_environment = environment;
        let refused = false;
        try {
          renderVerdict({
            thresholds,
            receipt: changed,
            root: ".",
            baselineAnchor,
          });
        } catch {
          refused = true;
        }
        if (!refused)
          throw new Error(
            "full verdict accepted changed or missing execution allocation",
          );
      }
      const changedRounds = structuredClone(observations);
      changedRounds.modes.active["4000"][2].execution_environment.sha256 =
        "1".repeat(64);
      try {
        deriveThresholds(changedRounds);
        throw new Error("mixed allocations derived a baseline");
      } catch (error) {
        if (error.message === "mixed allocations derived a baseline")
          throw error;
      }
      const edited = structuredClone(thresholds);
      edited.budgets.idle["128"].max_peak_rss_kib = 8_000_000;
      try {
        assertThresholdDerivation(edited);
        throw new Error("ceiling edit accepted");
      } catch (error) {
        if (!error.message.includes("modified after derivation")) throw error;
      }
      try {
        renderVerdict({
          thresholds,
          receipt: { ...receipt, provenance },
          root: ".",
          baselineAnchor,
        });
        throw new Error("self-certification accepted");
      } catch (error) {
        if (!error.message.includes("cannot certify itself")) throw error;
      }
      return "full production judge, independent identity and deterministic ceilings";
    },
  );

  // The derivation rules really compute ceilings from farm observations.
  expectSuccess("derivation yields a ceiling from three rounds", () => {
    const maxima = observedMaximaForTier(
      [goodCell(), goodCell(), goodCell({ fds_per_run: 3 })],
      128,
      "idle",
    );
    const ceilings = ceilingsForTier(maxima);
    if (ceilings.max_fds_per_run !== 3.25) {
      throw new Error(
        `expected fds ceiling 3.25, got ${ceilings.max_fds_per_run}`,
      );
    }
    return `fds ceiling ${ceilings.max_fds_per_run} from observed ${maxima.fds_per_run}`;
  });

  // The imported rule matches the frozen contract on a known point.
  expectSuccess("imported ceiling rule matches the frozen contract", () => {
    const cpu = deriveBudgetCeiling("cpu_core_percent", 21);
    if (cpu !== 35) throw new Error(`expected cpu ceiling 35, got ${cpu}`);
    return `cpu 21 -> ${cpu}`;
  });

  // Silent-zero refusals: the central discipline.
  expectFailure("fewer than three rounds is refused", () => {
    observedMaximaForTier([goodCell(), goodCell()], 128, "idle");
  });
  expectFailure("a NaN measurement is refused, not scored as a pass", () => {
    assertCompleteCell(goodCell({ fds_per_run: Number.NaN }), "self-test");
  });
  expectFailure("a negative measurement is refused", () => {
    assertCompleteCell(goodCell({ cpu_core_percent: -1 }), "self-test");
  });
  expectFailure("a missing steady rss is refused", () => {
    assertCompleteCell(goodCell({ steady: {} }), "self-test");
  });

  // The admission-cap precondition: a short fleet is not scored.
  expectFailure("a tier that never reached its size is refused", () => {
    judgeCell(
      goodCell({ admitted_runs: 64 }),
      ceilingsForTier(
        observedMaximaForTier(
          [goodCell(), goodCell(), goodCell()],
          128,
          "idle",
        ),
      ),
      512,
      "idle",
    );
  });
  expectFailure("a cell with no admitted_runs is refused", () => {
    const cell = goodCell();
    delete cell.admitted_runs;
    judgeCell(
      cell,
      ceilingsForTier(
        observedMaximaForTier(
          [goodCell(), goodCell(), goodCell()],
          128,
          "idle",
        ),
      ),
      128,
      "idle",
    );
  });

  // Host identity is ENFORCED, not merely recorded.
  expectFailure(
    "a darwin host for a linux-bound thresholds file is refused",
    () => {
      assertHostClass(
        {
          os: "darwin",
          os_release: "25.3.0",
          architecture: "arm64",
          logical_cpus: 14,
        },
        "self-test",
      );
    },
  );
  expectSuccess("a linux x64 host is accepted", () => {
    assertHostClass(
      {
        os: "linux",
        os_release: "5.15.0",
        architecture: "x64",
        logical_cpus: 64,
      },
      "self-test",
    );
    return "linux/x64 accepted";
  });
  expectSuccess(
    "initialized host identity preserves its canonical fingerprint",
    () => {
      const id = "0123456789abcdef0123456789abcdef";
      const expected = createHash("sha256").update(`${id}\n`).digest("hex");
      if (
        machineIdentity(id) !== expected ||
        machineIdentity(`${id}\n`) !== expected
      )
        throw new Error(
          "valid host identity changed its canonical fingerprint",
        );
      return "nonzero 128-bit identity, with or without its final LF";
    },
  );
  for (const raw of [
    "",
    "\n",
    "uninitialized\n",
    "0".repeat(32),
    "a".repeat(31),
    "g".repeat(32),
    `${"a".repeat(32)}\n\n`,
  ]) {
    expectFailure(
      "uninitialized or malformed measurement host identity is refused",
      () => machineIdentity(raw),
    );
  }
  expectFailure(
    "matching empty-input fingerprints cannot bind two hosts",
    () => {
      const host = {
        os: "linux",
        architecture: "x64",
        os_release: "same-kernel",
        logical_cpus: 64,
        machine_id_sha256: createHash("sha256").update("").digest("hex"),
      };
      assertSameHostClass(host, { ...host });
    },
  );
  expectSuccess(
    "actual shell host collector refuses empty identity and failed SSH",
    () => {
      const producer = readFileSync(
        new URL("./check-fleet-scale.sh", import.meta.url),
        "utf8",
      );
      const start = producer.indexOf("\nctxmux_fleet_remote_host_id=$(\n");
      const end = producer.indexOf("\nctxmux_fleet_daemon_sha=", start);
      if (start < 0 || end < 0)
        throw new Error("host identity collector not found");
      const script =
        `ctxmux_fleet_dest=fixture
ssh() { [[ $2 == 'cat /etc/machine-id' ]] || return 99; printf '%s' "$CTXMUX_TEST_MACHINE_ID"; return "$CTXMUX_TEST_SSH_STATUS"; }
` +
        producer.slice(start, end) +
        '\nprintf "%s" "$ctxmux_fleet_remote_host_id"\n';
      const id = "0123456789abcdef0123456789abcdef\n";
      const run = (raw, status) =>
        execFileSync("bash", ["-euo", "pipefail", "-c", script], {
          encoding: "utf8",
          env: {
            ...process.env,
            CTXMUX_TEST_MACHINE_ID: raw,
            CTXMUX_TEST_SSH_STATUS: String(status),
          },
          stdio: ["ignore", "pipe", "pipe"],
        });
      if (run(id, 0) !== machineIdentity(id))
        throw new Error("collector lost valid identity");
      for (const [raw, status] of [
        ["", 0],
        [id, 7],
      ]) {
        let refused = false;
        try {
          run(raw, status);
        } catch {
          refused = true;
        }
        if (!refused)
          throw new Error("collector accepted absent identity or failed SSH");
      }
      return "exact producer pipeline preserves valid identity and propagates both failures";
    },
  );
  expectFailure("a receipt from another host class is refused", () => {
    assertSameHostClass(
      {
        os: "linux",
        os_release: "5.15.0",
        architecture: "x64",
        logical_cpus: 64,
      },
      {
        os: "darwin",
        os_release: "25.3.0",
        architecture: "arm64",
        logical_cpus: 14,
      },
    );
  });

  // The overlap cross-check: agreement is per whole descriptor/thread across
  // the fleet, and the two disagreement directions carry opposite remedies.
  expectSuccess("a rounding-scale overlap delta still agrees", () => {
    // One stray daemon descriptor at 128 Runs prints as 3.008/Run. Exact
    // equality called that a structural disagreement and forfeited the receipt.
    const entry = compareOverlapField("fds_per_run", 3.008, 3, 128);
    if (!entry.agree) {
      throw new Error(`one stray descriptor was treated as disagreement`);
    }
    return `${entry.fleet_delta_units} fleet units < ${OVERLAP_UNIT_TOLERANCE}`;
  });
  expectSuccess("a real per-Run off-by-one is still caught", () => {
    // The case the tolerance must not swallow: FDS_PER_RUN actually 4.
    const entry = compareOverlapField("fds_per_run", 4, 3, 128);
    if (entry.agree) throw new Error("an extra fd per Run was not caught");
    if (entry.direction !== "above") {
      throw new Error(`expected direction above, got ${entry.direction}`);
    }
    return `${entry.fleet_delta_units} fleet units, direction ${entry.direction}`;
  });
  expectSuccess("the tolerance keeps its margin below a real defect", () => {
    // The comment above justifies 4 units by the gap between fixed overhead
    // (~1 unit) and the smallest real per-Run defect (128 units at this tier).
    // Pin that reasoning: a tolerance raised toward the defect would start
    // admitting off-by-ones silently, and nothing else in the file would fail.
    const smallestRealDefect = compareOverlapField(
      "fds_per_run",
      4,
      3,
      OVERLAP_TIER,
    ).fleet_delta_units;
    if (OVERLAP_UNIT_TOLERANCE * 8 > smallestRealDefect) {
      throw new Error(
        `tolerance ${OVERLAP_UNIT_TOLERANCE} is within 8x of a real ` +
          `${smallestRealDefect}-unit per-Run defect`,
      );
    }
    return `${OVERLAP_UNIT_TOLERANCE} units vs ${smallestRealDefect}-unit defect`;
  });
  expectSuccess("lower cost is an improvement under the same workload", () => {
    const entry = compareOverlapField("threads_per_run", 0, 2, 128);
    if (!entry.agree || !entry.improvement || entry.direction !== "below") {
      throw new Error("an actual resource improvement was penalized");
    }
    return `${entry.fleet_delta_units} fewer fleet units; frozen evidence preserved`;
  });
  expectSuccess("invalid low observations never become improvements", () => {
    for (const value of [-1, NaN, undefined]) {
      let rejected = false;
      try {
        compareOverlapField("threads_per_run", value, 2, 128);
      } catch {
        rejected = true;
      }
      if (!rejected) throw new Error("invalid observation was rewarded");
    }
    return "negative, NaN, and missing resource observations rejected";
  });

  // The fleet-wide retention cap of #47. Unlike every other ceiling here, this
  // one is hardcoded from the daemon source rather than derived from the run,
  // so these cases also guard that property.
  expectSuccess("a fleet under the retention cap passes", () => {
    const verdict = judgeRetentionBudget(
      goodCell({ aggregate_retained_bytes: 900 * 1024 * 1024 }),
    );
    if (!verdict.pass)
      throw new Error(`under-cap fleet failed: ${verdict.reason}`);
    return "900 MiB retained is under the 1 GiB cap";
  });
  expectSuccess("a fleet over the retention cap fails", () => {
    const verdict = judgeRetentionBudget(
      goodCell({ aggregate_retained_bytes: 1024 * 1024 * 1024 + 1 }),
    );
    if (verdict.pass) throw new Error("an over-cap fleet was scored as a pass");
    if (!verdict.reason.includes("daemon-wide cap")) {
      throw new Error(`unexpected reason: ${verdict.reason}`);
    }
    return "one byte over the cap refuses";
  });
  expectSuccess(
    "a huge lifetime total with small retention still passes",
    () => {
      // The exact confusion this field was added to prevent. A long-lived fleet
      // has emitted far more than the cap while holding almost nothing; grading
      // the lifetime total would fail a perfectly healthy daemon.
      const verdict = judgeRetentionBudget(
        goodCell({
          aggregate_output_bytes_lifetime: 500 * 1024 * 1024 * 1024,
          aggregate_retained_bytes: 32 * 1024 * 1024,
        }),
      );
      if (!verdict.pass) {
        throw new Error(
          `an aged but well-trimmed fleet failed: ${verdict.reason}`,
        );
      }
      return "500 GiB emitted, 32 MiB held, passes";
    },
  );
  expectSuccess("an unmeasured retention total fails closed", () => {
    // Absence was the normal case before the wire carried this field, which is
    // how the cap went unproven through a whole campaign. It must not read as
    // "nothing to check".
    const cell = goodCell();
    delete cell.aggregate_retained_bytes;
    const verdict = judgeRetentionBudget(cell);
    if (verdict.pass) {
      throw new Error("a cell with no retention measurement was passed");
    }
    return "missing measurement refuses";
  });
  expectSuccess("the retention ceiling matches the daemon constant", () => {
    // Pins the mirrored constant. If RETENTION_BUDGET_BYTES moves in
    // retention.rs and this does not, the gate silently grades against a stale
    // promise -- passing a daemon that exceeds its real cap, or failing one
    // that does not.
    if (RETENTION_BUDGET_CEILING_BYTES !== 1024 * 1024 * 1024) {
      throw new Error(
        `ceiling drifted to ${RETENTION_BUDGET_CEILING_BYTES}; update ` +
          "crates/ctxmux-daemon/src/retention.rs and this constant together",
      );
    }
    return "1 GiB, mirrored from retention.rs";
  });
  expectSuccess(
    "a failing retention verdict reaches the refusal reasons",
    () => {
      // The aggregator used to iterate a hand-listed pair of sub-verdicts, so a
      // new one would fail its cell with no explanation at the top level.
      const reasons = tierRefusalReasons([
        {
          tier: 128,
          mode: "idle",
          checks: [],
          list_verdict: { pass: true },
          admission_verdict: { pass: true },
          retention_verdict: {
            pass: false,
            reason: "over the daemon-wide cap",
          },
        },
      ]);
      if (
        !reasons.some((reason) => reason.includes("over the daemon-wide cap"))
      ) {
        throw new Error(
          `retention refusal was not surfaced; got ${JSON.stringify(reasons)}`,
        );
      }
      return "the reason is carried up";
    },
  );

  // Verdict wiring: a passing cell passes, a breaching cell fails.
  expectSuccess("a within-ceiling cell passes its verdict", () => {
    const ceilings = ceilingsForTier(
      observedMaximaForTier([goodCell(), goodCell(), goodCell()], 128, "idle"),
    );
    const checks = judgeCell(goodCell(), ceilings, 128, "idle");
    if (!checks.every((entry) => entry.pass)) {
      throw new Error("a within-ceiling cell was not passed");
    }
    return `${checks.length} fields within ceilings`;
  });
  expectSuccess("a breaching cell fails its verdict", () => {
    const ceilings = ceilingsForTier(
      observedMaximaForTier([goodCell(), goodCell(), goodCell()], 128, "idle"),
    );
    const checks = judgeCell(
      goodCell({ fds_per_run: 9 }),
      ceilings,
      128,
      "idle",
    );
    const fd = checks.find((entry) => entry.field === "fds_per_run");
    if (fd.pass) throw new Error("an fds breach was scored as a pass");
    return `fds ${fd.value} > ceiling ${fd.ceiling} correctly failed`;
  });

  // List and admission behaviour verdicts.
  expectFailure("List that did not succeed fails the tier", () => {
    const verdict = judgeListBehaviour(
      goodCell({ list_success: false }),
      2048,
      "idle",
    );
    if (!verdict.pass) throw new Error(verdict.reason);
  });
  expectFailure("EMFILE at the ceiling fails the tier", () => {
    const verdict = judgeAdmissionBehaviour(
      goodCell({
        admission_at_ceiling: { emfile: true, error: "Too many open files" },
      }),
      4000,
      "idle",
    );
    if (!verdict.pass) throw new Error(verdict.reason);
  });
  expectSuccess("a clean run_capacity refusal passes admission", () => {
    const verdict = judgeAdmissionBehaviour(goodCell(), 128, "idle");
    if (!verdict.pass) throw new Error("a clean refusal was not accepted");
    return `refused with ${verdict.refused_with}`;
  });

  // A tier below the daemon's cap cannot refuse anything, so an absent
  // admission observation is correct there. This predicate once read
  // `tier <= OVERLAP_TIER`, which failed 512 and 2048 for sitting under a
  // ceiling they were never meant to reach.
  expectSuccess("a tier below the daemon cap need not refuse", () => {
    const results = [512, 2048].map((tier) =>
      judgeAdmissionBehaviour(
        goodCell({ admission_at_ceiling: null }),
        tier,
        "idle",
      ),
    );
    if (!results.every((verdict) => verdict.pass)) {
      throw new Error("a sub-cap tier was failed for not refusing");
    }
    return "512 and 2048 pass without a ceiling refusal";
  });
  expectFailure("the cap tier must still observe a refusal", () => {
    const verdict = judgeAdmissionBehaviour(
      goodCell({ admission_at_ceiling: null }),
      4000,
      "idle",
    );
    if (!verdict.pass) throw new Error(verdict.reason);
  });

  // The leak fields are pinned to 0 and no observation may raise them. Before
  // this, deriveBudgetCeiling turned an observed leak into its own ceiling:
  // the 2026-09-06 farm run derived 129/513/2049 stranded children at the
  // 128/512/2048 tiers and passed all three.
  expectSuccess("leak ceilings are zero, not derived from the leak", () => {
    const leakyRound = goodCell({
      cleanup_live_children: 4000,
      cleanup_attachments: 17,
    });
    const leaked = ceilingsForTier(
      observedMaximaForTier([leakyRound, leakyRound, leakyRound], 128, "idle"),
    );
    for (const field of ABSOLUTE_ZERO_FIELDS) {
      if (leaked[`max_${field}`] !== 0) {
        throw new Error(
          `${field} ceiling was ${leaked[`max_${field}`]}, not 0 — the leak set its own standard`,
        );
      }
    }
    return "4000 stranded children still yield a ceiling of 0";
  });
  expectFailure("a nonzero leak fails the tier verdict", () => {
    const ceilings = ceilingsForTier(
      observedMaximaForTier([goodCell(), goodCell(), goodCell()], 128, "idle"),
    );
    const checks = judgeCell(
      goodCell({ cleanup_live_children: 1 }),
      ceilings,
      128,
      "idle",
    );
    const leak = checks.find(
      (check) => check.field === "cleanup_live_children",
    );
    if (!leak) throw new Error("cleanup_live_children was not judged at all");
    if (!leak.pass) {
      throw new Error(
        `cleanup_live_children 1 > ceiling ${leak.ceiling} correctly failed`,
      );
    }
  });

  expectFailure("a teardown that could not stop Runs fails the tier", () => {
    const verdict = judgeListBehaviour(
      goodCell({ cleanup_stop_failures: 3 }),
      2048,
      "idle",
    );
    if (!verdict.pass) throw new Error(verdict.reason);
  });

  // The census daemon dying mid-run is indistinguishable from a perfect run by
  // every other field: no children to leak, no stat file so idle CPU reads
  // 0.000, and a failed List so nothing is stopped and no failure is counted.
  // These two cases are what stop that cell from being the best-looking one in
  // the receipt.
  expectFailure("a census whose daemon died fails the tier", () => {
    const verdict = judgeListBehaviour(
      goodCell({ daemon_alive_after_census: false }),
      2048,
      "idle",
    );
    if (!verdict.pass) throw new Error(verdict.reason);
  });

  expectFailure("a cell that never recorded daemon liveness fails", () => {
    const cell = goodCell();
    delete cell.daemon_alive_after_census;
    const verdict = judgeListBehaviour(cell, 2048, "idle");
    if (!verdict.pass) throw new Error(verdict.reason);
  });

  // #49 was List degrading past ~1000 Runs. Grading latency for finiteness only
  // meant a decay to whole seconds still passed, so the harness could not catch
  // the regression it was built for.
  expectFailure("a List that stalls for seconds fails the tier", () => {
    const verdict = judgeListBehaviour(
      goodCell({ list_latency_ms: 5000 }),
      4000,
      "idle",
    );
    if (!verdict.pass) throw new Error(verdict.reason);
  });

  // The ceiling must not be so tight that real farm readings trip it: the
  // measured curve tops out at 26.5 ms at 4000 Runs.
  expectSuccess("a realistic farm List latency still passes", () => {
    const verdict = judgeListBehaviour(
      goodCell({ list_latency_ms: 26.5 }),
      4000,
      "idle",
    );
    if (!verdict.pass) throw new Error(verdict.reason);
    return `26.5 ms accepted under the ${LIST_LATENCY_CEILING_MS} ms ceiling`;
  });

  if (failures.length > 0) {
    throw new Error(
      `${failures.length} self-test case(s) failed: ${failures.join(", ")}`,
    );
  }
  console.log(
    "self-test passed: the harness refuses short fleets, silent zeros, foreign hosts, and ceiling breaches",
  );
}

function main() {
  const options = parseArgs(process.argv.slice(2));

  if (options.selfTest) {
    selfTest();
    return;
  }

  if (options.mode === "resource-policy") {
    console.log(JSON.stringify(FLEET_RESOURCE_POLICY));
    return;
  }
  if (options.mode === "verify-baseline") {
    const { thresholds } = loadFrozenThresholds(
      options.thresholdsPath,
      options.baselineRef,
    );
    assertThresholdDerivation(thresholds);
    assertResourcePolicy(thresholds.derived_from.resource_policy);
    return;
  }
  if (options.mode === "derive") {
    if (!options.tiersPath || !options.out) {
      throw new Error("derive requires --observations <path> and --out <path>");
    }
    const observations = JSON.parse(readFileSync(options.tiersPath, "utf8"));
    const thresholds = deriveThresholds(observations);
    writeFileSync(
      options.out,
      `${JSON.stringify(thresholds, null, 2)}\n`,
      "utf8",
    );
    console.log(`fleet-scale thresholds derived and written to ${options.out}`);
    return;
  }

  if (options.mode === "verdict") {
    if (!options.thresholdsPath || !options.tiersPath) {
      throw new Error(
        "verdict requires --thresholds <path> and --observations <path>",
      );
    }
    const { thresholds, baselineAnchor } = loadFrozenThresholds(
      options.thresholdsPath,
      options.baselineRef,
    );
    const receipt = JSON.parse(readFileSync(options.tiersPath, "utf8"));
    const verdict = renderVerdict({
      thresholds,
      receipt,
      root: options.root,
      baselineAnchor,
    });
    const rendered = JSON.stringify(verdict, null, 2);
    console.log(rendered);
    if (options.out) {
      writeFileSync(options.out, `${rendered}\n`, "utf8");
      console.log(`verdict written to ${options.out}`);
    }
    if (!verdict.accepted) {
      throw new Error(
        `fleet-scale acceptance FAILED: ${(verdict.refusal_reasons ?? ["see verdict"]).join("; ")}`,
      );
    }
    console.log(
      "fleet-scale acceptance PASSED across all tiers and the darwin overlap cross-check",
    );
    return;
  }

  if (options.mode === "smoke-check") {
    // Prove the drive/census/derivation path ran end to end on this host, at a
    // smoke-only tier that is deliberately not one of the acceptance TIERS. The
    // real per-tier verdict is the accept profile's job; the smoke's job is the
    // plumbing, so it asserts the census cells are complete and the derivation
    // rules produce ceilings from them.
    if (!options.tiersPath) {
      throw new Error("smoke-check requires --observations <path>");
    }
    const smokeTier = String(process.env.CTXMUX_FLEET_SMOKE_TIER ?? "");
    if (smokeTier === "") {
      throw new Error(
        "smoke-check requires CTXMUX_FLEET_SMOKE_TIER in the environment",
      );
    }
    const observations = JSON.parse(readFileSync(options.tiersPath, "utf8"));
    assertHostClass(observations.host, "smoke host");
    for (const mode of MODES) {
      const rounds = observations.modes?.[mode]?.[smokeTier];
      const maxima = observedMaximaForTier(rounds, smokeTier, mode);
      const ceilings = ceilingsForTier(maxima);
      console.log(
        `smoke ${mode}: derived ${Object.keys(ceilings).length} ceilings from ${rounds.length} rounds; ` +
          `fds ceiling ${ceilings.max_fds_per_run}`,
      );
    }
    console.log(
      "fleet-scale smoke passed: drive, census, and derivation ran end to end on this host",
    );
    return;
  }

  throw new Error(
    `unknown mode ${JSON.stringify(options.mode)}; expected --mode derive|verdict|smoke-check or --self-test`,
  );
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  try {
    main();
  } catch (error) {
    console.error(`check-fleet-scale: ${error.message}`);
    process.exitCode = 1;
  }
}

export {
  machineIdentity,
  sourceIdentity,
  judgeWorkload,
  FLEET_RESOURCE_POLICY,
  assertThresholdDerivation,
  loadFrozenThresholds,
  measurementContractSha,
  deriveThresholds,
  renderVerdict,
  observedMaximaForTier,
  ceilingsForTier,
  judgeCell,
  assertHostClass,
  assertTierWasReached,
  TIERS,
  ROUNDS,
};
