import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { isAbsolute, posix, resolve, win32 } from "node:path";

export const GC_CONTRACT_PATH = "reliability-gc-contract.json";
export const GC_CONTRACT_SHA256 =
  "92960ffd3369b5f408c3df2846f7fa4170fc1c0023b6c21ac518d04783af8049";

const SHA256_PATTERN = /^[0-9a-f]{64}$/u;

export interface GcFileIdentity {
  readonly path: string;
  readonly sha256: string;
}

export interface ReliabilityGcContract {
  readonly schema: "ctxmux.reliability-gc-contract.v1";
  readonly frozen_before_implementation: true;
  readonly seed: string;
  readonly helper: GcFileIdentity & {
    readonly encoding: "lowercase_ascii_sha256_hex";
    readonly writes_newline: false;
  };
  readonly run_spec: {
    readonly program: string;
    readonly args: readonly string[];
    readonly cwd: string;
    readonly env: Readonly<Record<string, string>>;
    readonly size: { readonly cols: number; readonly rows: number };
    readonly declared_inputs: readonly string[];
  };
  readonly payload_modes: Readonly<
    Record<
      string,
      {
        readonly phase: string;
        readonly payload_bytes: number;
        readonly hex_repetitions: number;
      }
    >
  >;
  readonly bounded_churn: Readonly<Record<string, unknown>> & {
    readonly run_ceiling: number;
    readonly fill_runs: number;
    readonly turnover_windows: number;
    readonly replacements_per_window: number;
    readonly successful_lifecycles_per_mode: number;
    readonly concurrency: number;
    readonly persistent_restart_after_window: number;
    readonly physical_start_deltas: Readonly<Record<string, number>>;
  };
  readonly replay_pressure: Readonly<Record<string, unknown>> & {
    readonly profiles: readonly string[];
    readonly fill_runs: number;
    readonly replacement_runs: number;
    readonly concurrency: number;
    readonly public_replay_batch_size: number;
    readonly public_replay_verification_runs_before_replacement: number;
    readonly public_replay_verification_runs_after_replacement: number;
    readonly require_exact_replay_digest: boolean;
    readonly require_exact_retained_run_and_key_count: number;
    readonly require_retry_without_physical_start: boolean;
    readonly quiescent_seconds: number;
    readonly fill_indices: { readonly first: number; readonly last: number };
    readonly replacement_indices: {
      readonly first: number;
      readonly last: number;
    };
    readonly operation_key_template: string;
    readonly retry_wave_keys_before_restart: number;
    readonly retry_wave_keys_after_restart: number;
    readonly require_live_durable_head_equals_head: boolean;
    readonly live_retained_payload_bytes: number;
    readonly max_overlap_or_replay_clone_bytes: number;
    readonly retained_plus_overlap_payload_bytes: number;
    readonly persistent_durable_replay_max_bytes: number;
    readonly persistent_recovered_replay_min_bytes: number;
    readonly persistent_native_chunk_max_bytes: number;
    readonly resource_budgets: Readonly<Record<string, number>>;
    readonly owner_budgets: Readonly<Record<string, string | number | boolean>>;
    readonly sampling: Readonly<Record<string, string | number | boolean>>;
    readonly time_budgets_seconds: {
      readonly total: number;
      readonly memory_only: number;
      readonly persistent: number;
    };
    readonly persistent_restart: Readonly<Record<string, boolean>>;
  };
  readonly profile_time_budgets_seconds: Readonly<Record<string, number>>;
  readonly ci_job_timeout_minutes: Readonly<Record<string, number>>;
  readonly implementation_policy_requires_git_blob_fence: true;
}

export interface GcResourceBudgets {
  readonly live_steady_rss_kib: number;
  readonly memory_peak_rss_kib: number;
  readonly persistent_peak_rss_kib: number;
  readonly persistent_recovered_steady_rss_kib: number;
  readonly persistent_recovered_peak_rss_kib: number;
  readonly memory_average_cpu_core_percent: number;
  readonly persistent_average_cpu_core_percent: number;
  readonly quiescent_cpu_core_percent: number;
  readonly peak_thread_delta: number;
  readonly quiescent_thread_delta: number;
  readonly peak_fd_delta: number;
  readonly quiescent_fd_delta: number;
}

export function gcResourceBudgets(
  loaded: LoadedReliabilityGcContract,
): GcResourceBudgets {
  return loaded.contract.replay_pressure
    .resource_budgets as unknown as GcResourceBudgets;
}

export interface LoadedReliabilityGcContract {
  readonly contract: ReliabilityGcContract;
  readonly workload_contract: GcFileIdentity;
  readonly workload_helper: GcFileIdentity;
}

export function assertReliabilityGcIdentities(
  loaded: LoadedReliabilityGcContract,
  identities: {
    readonly workload_contract: GcFileIdentity;
    readonly workload_helper: GcFileIdentity;
  },
): void {
  assert.deepEqual(
    identities.workload_contract,
    loaded.workload_contract,
    "qualification workload contract identity drifted after preflight",
  );
  assert.deepEqual(
    identities.workload_helper,
    loaded.workload_helper,
    "qualification workload helper identity drifted after preflight",
  );
}

export function assertCanonicalGcQualificationInvocation(
  profile: string,
  argv: readonly string[],
  environment: NodeJS.ProcessEnv,
): void {
  if (profile !== "nightly" && profile !== "release") return;
  for (const name of [
    "CTXMUX_RELIABILITY_SEED",
    "CTXMUX_RELIABILITY_TIME_BUDGET_SECONDS",
  ]) {
    assert.equal(
      environment[name],
      undefined,
      `${name} is forbidden for canonical ${profile} qualification`,
    );
  }
  const forbidden = new Set([
    "--stage",
    "--resource-counts",
    "--resource-modes",
    "--resource-start-concurrency",
    "--soak-seconds",
  ]);
  assert.ok(
    !argv.some((value) => forbidden.has(value)),
    `canonical ${profile} qualification rejects workload override options`,
  );
}

export function loadReliabilityGcContract(
  root: string,
): LoadedReliabilityGcContract {
  const contractPath = resolve(root, GC_CONTRACT_PATH);
  const contractBytes = readFileSync(contractPath);
  const contractSha256 = sha256(contractBytes);
  assert.equal(
    contractSha256,
    GC_CONTRACT_SHA256,
    "reliability GC contract bytes drifted from the frozen pre-implementation contract",
  );
  const contract = JSON.parse(
    contractBytes.toString("utf8"),
  ) as ReliabilityGcContract;
  assert.equal(contract.schema, "ctxmux.reliability-gc-contract.v1");
  assert.equal(contract.frozen_before_implementation, true);
  assert.equal(contract.implementation_policy_requires_git_blob_fence, true);
  assert.match(contract.seed, /^(?:0|[1-9][0-9]*)$/u);
  assertPortableRepositoryPath(contract.helper.path, "GC helper");
  assert.match(contract.helper.sha256, SHA256_PATTERN);

  const helperBytes = readFileSync(resolve(root, contract.helper.path));
  const helperSha256 = sha256(helperBytes);
  assert.equal(
    helperSha256,
    contract.helper.sha256,
    "reliability GC helper bytes drifted from the frozen contract",
  );

  assertHeadBlob(root, GC_CONTRACT_PATH, contractBytes);
  assertHeadBlob(root, contract.helper.path, helperBytes);

  return {
    contract,
    workload_contract: {
      path: GC_CONTRACT_PATH,
      sha256: contractSha256,
    },
    workload_helper: {
      path: contract.helper.path,
      sha256: helperSha256,
    },
  };
}

function assertHeadBlob(
  root: string,
  path: string,
  worktreeBytes: Buffer,
): void {
  const result = spawnSync("git", ["show", `HEAD:${path}`], {
    cwd: root,
    encoding: null,
    maxBuffer: 32 * 1024 * 1024,
    timeout: 5_000,
  });
  assert.equal(
    result.status,
    0,
    `cannot read source-bound Git blob for ${path}: ${result.stderr?.toString("utf8").trim() || result.error?.message || "unknown"}`,
  );
  assert.equal(
    Buffer.compare(result.stdout ?? Buffer.alloc(0), worktreeBytes),
    0,
    `worktree bytes differ from HEAD:${path}`,
  );
}

function assertPortableRepositoryPath(path: string, label: string): void {
  assert.ok(
    path.length > 0 &&
      !path.includes("\\") &&
      !isAbsolute(path) &&
      !win32.isAbsolute(path) &&
      !path.split("/").includes("..") &&
      posix.normalize(path) === path,
    `${label} path must be a portable repository-relative path`,
  );
}

function sha256(bytes: Buffer): string {
  return createHash("sha256").update(bytes).digest("hex");
}
