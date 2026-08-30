// Measurement and verdict core for scripts/check-fleet-scale.sh.
//
// This is the acceptance half the repository was missing. Several scripts
// already MEASURE fleet resource behaviour and emit tables; none renders a
// pass/fail VERDICT. "The farm results" have therefore never existed as a
// judgement, and no claim that large-scale behaviour was accepted is currently
// supportable. This module makes the verdict exist: it derives thresholds from
// farm-host observations with the same rules the darwin budget uses, then
// compares fresh measurements against them and exits nonzero when they are not
// met.
//
// It reuses the DERIVATION RULES of scripts/reliability-budget-contract.mjs by
// importing deriveBudgetCeiling and deriveObservedMaxima. It does not edit that
// module and does not touch its frozen COUNTS ["1","32","128"] — editing either
// would rehash the measurement contract and invalidate every committed darwin
// baseline bound to it (reliability-baseline-policy.mjs:834-843). This harness
// keeps its OWN thresholds file and its OWN receipts, and never writes
// reliability-budgets.json.
//
// Two disciplines are load-bearing and mirror the house precedent
// (scripts/remote-cost-measure.mjs):
//
//   Fail loudly, never silently.  A fleet acceptance harness that reported a
//   zero where it should have refused would accept a fleet it never measured.
//   Every quantity that cannot be honestly produced is a refusal, not a zero,
//   and --self-test proves each refusal fires. A tier the daemon will not admit
//   (see the admission-cap note below) is a blocked precondition, reported as
//   such, not an empty measurement.
//
//   Name the lane.  Every number says which host produced it and at which tier.
//   The 128 tier overlaps the existing darwin gate on purpose: if the farm's
//   128 numbers disagree with the darwin baseline beyond a stated platform
//   delta, the harness is measuring something different from the gate and its
//   larger tiers cannot be trusted. That cross-check is emitted in the report,
//   not buried in a comment.
//
// HOST IDENTITY IS ENFORCED, NOT MERELY RECORDED. The darwin baseline records
// os: darwin and never checks it, which is exactly how a darwin-derived
// fds_per_run ceiling of 3.25 came to gate every Linux PR. This module refuses
// a receipt whose host class does not match the thresholds it is checked
// against.

import { createHash } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
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
/// These are the platform-invariant structural costs: the daemon holds three
/// descriptors per Run and two threads per Run by design on every platform
/// (ADR 013 / the SIGCHLD reuse proof), so a farm that measures a different
/// value at 128 is measuring something the gate is not. RSS and CPU are
/// deliberately excluded here — they legitimately differ by platform and are
/// bounded per-tier by the derived ceilings instead.
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
  return deriveObservedMaxima(rounds);
}

/// Derive the full ceiling set for one tier/mode from its observed maxima.
///
/// Each ceiling is deriveBudgetCeiling(field, observed) — the same rational
/// rule the darwin budget is pinned to. No manual margin is applied; a
/// hand-written ceiling is exactly the post-observation edit the contract
/// forbids.
function ceilingsForTier(maxima) {
  const ceilings = {};
  for (const field of OBSERVED_FIELDS) {
    ceilings[`max_${field}`] = deriveBudgetCeiling(field, maxima[field]);
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
  const observedMaxima = {};
  const budgets = {};
  for (const mode of MODES) {
    observedMaxima[mode] = {};
    budgets[mode] = {};
    for (const tier of TIERS) {
      const rounds = observations.modes?.[mode]?.[String(tier)];
      const maxima = observedMaximaForTier(rounds, tier, mode);
      observedMaxima[mode][String(tier)] = maxima;
      budgets[mode][String(tier)] = ceilingsForTier(maxima);
    }
  }
  const contractSha = measurementContractSha();
  return {
    schema: "ctxmux.fleet-scale-thresholds.v1",
    derived_from: {
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
function measurementContractSha() {
  const source = readFileSync(
    new URL("./reliability-budget-contract.mjs", import.meta.url),
    "utf8",
  );
  return createHash("sha256").update(source).digest("hex");
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

/// Enforce that two host identities are the same class before comparing numbers.
function assertSameHostClass(derivedHost, receiptHost) {
  assertHostClass(derivedHost, "thresholds host");
  assertHostClass(receiptHost, "receipt host");
  if (
    derivedHost.os !== receiptHost.os ||
    derivedHost.architecture !== receiptHost.architecture
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
/// The shipped daemon admits at most MAX_RETAINED_RUNS (128) LIVE Runs: startup
/// clamps record capacity to min(configured 128, fd-funded ceiling) and
/// admission refuses beyond it with run_capacity. So a tier above 128 can only
/// be reached by a daemon built or configured to raise that cap. The measuring
/// side records how many Runs it actually admitted; if that is short of the
/// tier, the tier is a BLOCKED PRECONDITION — reported loudly — not a fleet
/// that quietly came up small and produced flattering per-Run numbers.
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
      `tier ${tier} ${mode}: the daemon admitted only ${admitted} of ${tier} Runs. The shipped ` +
        "daemon caps live Runs at 128 (MAX_RETAINED_RUNS); a tier above it needs a daemon whose " +
        "record cap is raised, or this is a blocked precondition rather than a measurable tier. " +
        "Refusing to score a fleet that never reached its target size.",
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

/// Flatten a census cell into the OBSERVED_FIELDS the ceilings are keyed by.
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
/// by design, so the farm and darwin must agree on them at 128 within a stated
/// delta of zero. Disagreement means the harness is measuring something the
/// gate is not, which forfeits any trust in the larger tiers. This returns the
/// comparison so the report can state it in the output rather than hide it.
function overlapCrossCheck(root, receipt, mode) {
  const farmCell = receipt.modes?.[mode]?.[String(OVERLAP_TIER)];
  assertCompleteCell(farmCell, `overlap cross-check ${mode}`);
  const farm = readingsForCell(farmCell);
  const darwin = darwinOverlapMaxima(root, mode);
  const comparisons = OVERLAP_INVARIANT_FIELDS.map((field) => {
    const farmValue = farm[field];
    const darwinValue = darwin[field];
    const agree = farmValue === darwinValue;
    return {
      field,
      farm: farmValue,
      darwin: darwinValue,
      agree,
      ...(agree
        ? {}
        : {
            reason:
              `farm ${field} ${farmValue} at ${OVERLAP_TIER} disagrees with the darwin ` +
              `baseline ${darwinValue}; these per-Run costs are platform-invariant by ` +
              "design, so a mismatch means the two harnesses do not measure the same thing " +
              "and the larger farm tiers cannot be trusted",
          }),
    };
  });
  return {
    tier: OVERLAP_TIER,
    mode,
    platform_invariant_fields: OVERLAP_INVARIANT_FIELDS,
    delta_allowed: 0,
    comparisons,
    agrees: comparisons.every((entry) => entry.agree),
  };
}

/// Render the full acceptance verdict for a receipt against a thresholds file.
///
/// Fails closed: any tier/mode that cannot be judged (missing cell, unreached
/// tier, host mismatch, contract drift) is a nonzero exit, not an omission.
function renderVerdict({ thresholds, receipt, root }) {
  if (thresholds?.schema !== "ctxmux.fleet-scale-thresholds.v1") {
    throw new Error(
      `thresholds file has unexpected schema ${JSON.stringify(thresholds?.schema)}`,
    );
  }
  assertSameHostClass(thresholds.derived_from?.host, receipt?.host);
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
      const checks = judgeCell(cell, ceilings, tier, mode);
      tierVerdicts.push({
        tier,
        mode,
        checks,
        admitted_runs: cell.admitted_runs,
        list_latency_ms: cell.list_latency_ms ?? null,
        list_success: cell.list_success ?? null,
        admission_at_ceiling: cell.admission_at_ceiling ?? null,
        aggregate_retained_bytes: cell.aggregate_retained_bytes ?? null,
        pass:
          checks.every((entry) => entry.pass) &&
          judgeListBehaviour(cell, tier, mode).pass &&
          judgeAdmissionBehaviour(cell, tier, mode).pass,
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
    host: receipt.host,
    thresholds_host: thresholds.derived_from.host,
    tiers: tierVerdicts,
    overlap_cross_check: overlaps,
    accepted: tiersPass && overlapAgrees,
    ...(tiersPass && overlapAgrees
      ? {}
      : {
          refusal_reasons: [
            ...(tiersPass
              ? []
              : ["one or more tier/mode cells exceeded a derived ceiling"]),
            ...(overlapAgrees
              ? []
              : [
                  "the 128 tier disagreed with the darwin gate on a platform-invariant per-Run cost",
                ]),
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
  return { pass: true, latency_ms: round(latency) };
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
    if (tier <= OVERLAP_TIER) {
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

  const goodCell = (overrides = {}) => ({
    cpu_core_percent: 1,
    peak_rss_kib: 10000,
    retained_output_bytes_per_run: 0,
    rss_kib_per_run: 100,
    threads_per_run: 2,
    fds_per_run: 3,
    cleanup_live_children: 0,
    cleanup_attachments: 0,
    steady: { rss_kib: 9000 },
    baseline: { threads: 8 },
    cleanup: { threads: 8 },
    admitted_runs: 128,
    list_success: true,
    list_latency_ms: 5,
    aggregate_retained_bytes: 0,
    admission_at_ceiling: {
      refused_cleanly: true,
      emfile: false,
      error_code: "run_capacity",
    },
    ...overrides,
  });

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
    const thresholds = JSON.parse(readFileSync(options.thresholdsPath, "utf8"));
    const receipt = JSON.parse(readFileSync(options.tiersPath, "utf8"));
    const verdict = renderVerdict({ thresholds, receipt, root: options.root });
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

try {
  main();
} catch (error) {
  console.error(`check-fleet-scale: ${error.message}`);
  process.exitCode = 1;
}

export {
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
