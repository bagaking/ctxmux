import { createHash } from "node:crypto";
import { isDeepStrictEqual } from "node:util";

function record(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

const ENVIRONMENT_FIELDS = [
  "os",
  "os_release",
  "architecture",
  "logical_cpus",
  "cpu_model",
] as const;
const WORKLOAD_FIELDS = [
  "frame_bytes",
  "retained_output_bytes_per_run",
  "live_event_capacity",
  "global_run_quota",
  "global_attachment_quota",
  "resource_start_concurrency",
  "peak_rss_sample_interval_ms",
  "seed_controls",
] as const;

function buildProfile(value: unknown): string | undefined {
  const args = record(value).argv;
  if (!Array.isArray(args) || args[0] !== "cargo" || args[1] !== "build")
    return undefined;
  const profileIndex = args.indexOf("--profile");
  if (profileIndex !== -1)
    return typeof args[profileIndex + 1] === "string"
      ? args[profileIndex + 1]
      : undefined;
  if (args.some((arg) => typeof arg !== "string")) return undefined;
  if (args.some((arg: string) => arg.startsWith("--profile=")))
    return args.find((arg: string) => arg.startsWith("--profile="))?.slice(10);
  return args.includes("--release") ? "release" : "dev";
}

/** Existing receipts describe a host class, not physical host identity. This
 * fence refuses incompatible empirical comparisons; it does not establish
 * full workload equivalence or a performance improvement. Candidate source
 * may differ from the independently frozen reference source. */
export function empiricalBudgetApplicabilityErrors({
  budgets,
  baselineReceipts,
  value,
  qualificationEnvironment,
}: {
  budgets: unknown;
  baselineReceipts:
    readonly { path: string; sha256: string; value: unknown }[] | undefined;
  value: unknown;
  qualificationEnvironment: unknown;
}): string[] {
  const errors: string[] = [];
  const reject = (reason: string): void => {
    errors.push(`empirical resource baseline is not applicable: ${reason}`);
  };
  const baseline = record(record(budgets).observation_baseline);
  const refs = baseline.raw_receipts;
  if (
    !Array.isArray(refs) ||
    refs.length !== 3 ||
    baselineReceipts?.length !== 3
  ) {
    reject("three independently frozen reference receipts are required");
    return errors;
  }
  const candidate = record(value);
  if (
    qualificationEnvironment === undefined ||
    !isDeepStrictEqual(candidate.environment, qualificationEnvironment)
  )
    reject(
      "receipt environment does not match the independent qualification environment",
    );
  const candidateEnvironment = record(candidate.environment);
  const baselineEnvironment = record(baseline.environment);
  for (const field of ENVIRONMENT_FIELDS) {
    if (
      baselineEnvironment[field] === undefined ||
      !isDeepStrictEqual(
        candidateEnvironment[field],
        baselineEnvironment[field],
      )
    )
      reject(`observation environment ${field} differs or is missing`);
  }
  const measurementContract = record(budgets).measurement_contract;
  const contractHash =
    measurementContract === undefined
      ? undefined
      : createHash("sha256")
          .update(JSON.stringify(measurementContract))
          .digest("hex");
  const candidateProvenance = record(candidate.provenance);
  if (
    contractHash === undefined ||
    candidateProvenance.measurement_contract_sha256 !== contractHash
  )
    reject("candidate measurement contract is missing or differs");
  const candidateWorkload = record(candidate.declared_limits);
  const profile = buildProfile(candidateProvenance.build);
  for (const [index, receipt] of baselineReceipts.entries()) {
    const reference = record(refs[index]);
    const observed = record(receipt.value);
    if (
      reference.path !== receipt.path ||
      reference.sha256 !== receipt.sha256 ||
      !/^[0-9a-f]{64}$/u.test(receipt.sha256) ||
      observed.status !== "pass" ||
      observed.profile !== "observe" ||
      observed.observation_round !== index + 1
    ) {
      reject(`reference receipt ${index + 1} is missing or mismatched`);
      continue;
    }
    if (!isDeepStrictEqual(observed.environment, baseline.environment))
      reject(`reference receipt ${index + 1} environment differs`);
    const provenance = record(observed.provenance);
    if (provenance.measurement_contract_sha256 !== contractHash)
      reject(`reference receipt ${index + 1} measurement contract differs`);
    if (profile === undefined || buildProfile(provenance.build) !== profile)
      reject(
        `reference receipt ${index + 1} build profile differs or is missing`,
      );
    const workload = record(observed.declared_limits);
    for (const field of WORKLOAD_FIELDS) {
      if (
        workload[field] === undefined ||
        !isDeepStrictEqual(candidateWorkload[field], workload[field])
      )
        reject(
          `reference receipt ${index + 1} workload ${field} differs or is missing`,
        );
    }
    for (const field of ["resource_counts", "resource_modes"] as const) {
      const requested = candidateWorkload[field];
      const covered = workload[field];
      if (
        !Array.isArray(requested) ||
        requested.length === 0 ||
        !Array.isArray(covered) ||
        !requested.every((cell) => covered.includes(cell))
      )
        reject(`reference receipt ${index + 1} does not cover ${field}`);
    }
  }
  return errors;
}
