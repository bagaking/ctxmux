import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { parse } from "yaml";

import contract from "../reliability-gc-contract.json" with { type: "json" };

const MIB = 1024 * 1024;
const repositoryUrl = new URL("../", import.meta.url);
const helperUrl = new URL(contract.helper.path, repositoryUrl);
const helperPath = fileURLToPath(helperUrl);

test("GC contract fixes internally consistent payload and resource ceilings", () => {
  assert.equal(contract.schema, "ctxmux.reliability-gc-contract.v1");
  assert.equal(contract.frozen_before_implementation, true);
  assert.equal(contract.helper.path, "scripts/reliability-gc-child.mjs");
  assert.deepEqual(contract.run_spec, {
    program: "process.execPath",
    args: ["scripts/reliability-gc-child.mjs", "<seed>", "<mode>", "<index>"],
    cwd: "clean_repository_root",
    env: {},
    size: { cols: 80, rows: 24 },
    declared_inputs: [],
  });

  for (const mode of Object.values(contract.payload_modes)) {
    assert.equal(mode.payload_bytes, 64 * mode.hex_repetitions);
  }

  const pressure = contract.replay_pressure;
  const pressureBytes =
    contract.payload_modes.memory_replay_pressure.payload_bytes;
  assert.equal(pressureBytes, 4 * MIB);
  assert.equal(
    pressure.live_retained_payload_bytes,
    pressure.fill_runs * pressureBytes,
  );
  assert.equal(
    pressure.public_replay_verification_runs_before_replacement,
    pressure.fill_runs,
  );
  assert.equal(
    pressure.public_replay_verification_runs_after_replacement,
    pressure.fill_runs,
  );
  assert.equal(
    pressure.fill_indices.last - pressure.fill_indices.first + 1,
    pressure.fill_runs,
  );
  assert.equal(
    pressure.replacement_indices.last - pressure.replacement_indices.first + 1,
    pressure.replacement_runs,
  );
  assert.equal(
    pressure.operation_key_template,
    "gc-pressure:<mode>:<index>:<digest-hex>",
  );
  assert.equal(pressure.retry_wave_keys_before_restart, pressure.fill_runs);
  assert.equal(pressure.retry_wave_keys_after_restart, pressure.fill_runs);
  assert.equal(pressure.require_live_durable_head_equals_head, true);
  assert.equal(
    pressure.require_exact_retained_run_and_key_count,
    pressure.fill_runs,
  );
  assert.equal(pressure.persistent_restart.after_replacement_wave, true);
  assert.equal(
    pressure.max_overlap_or_replay_clone_bytes,
    pressure.concurrency * pressureBytes,
  );
  assert.equal(
    pressure.retained_plus_overlap_payload_bytes,
    pressure.live_retained_payload_bytes +
      pressure.max_overlap_or_replay_clone_bytes,
  );
  assert.equal(
    pressure.persistent_recovered_replay_min_bytes,
    pressure.persistent_durable_replay_max_bytes -
      pressure.persistent_native_chunk_max_bytes +
      1,
  );
  assert.equal(
    pressure.persistent_transient.total_bytes,
    pressure.persistent_transient.catchup_and_finalize_snapshot_bytes +
      pressure.persistent_transient.ordinary_append_queue_bytes +
      pressure.persistent_transient.actor_working_bytes,
  );

  const rss = pressure.resource_budgets;
  const formula = pressure.rss_formula;
  assert.equal(
    rss.live_steady_rss_kib,
    rssCeiling(formula.idle_128_base_kib, 512 * 1024, formula.quantum_kib),
  );
  assert.equal(
    rss.memory_peak_rss_kib,
    rssCeiling(
      formula.active_128_base_kib,
      (pressure.live_retained_payload_bytes +
        pressure.max_overlap_or_replay_clone_bytes) /
        1024,
      formula.quantum_kib,
    ),
  );
  assert.equal(
    rss.persistent_peak_rss_kib,
    rssCeiling(
      formula.active_128_base_kib,
      (pressure.live_retained_payload_bytes +
        pressure.max_overlap_or_replay_clone_bytes +
        pressure.persistent_transient.total_bytes) /
        1024,
      formula.quantum_kib,
    ),
  );
  assert.equal(
    rss.persistent_recovered_steady_rss_kib,
    rssCeiling(formula.idle_128_base_kib, 256 * 1024, formula.quantum_kib),
  );
  assert.equal(
    rss.persistent_recovered_peak_rss_kib,
    rssCeiling(
      formula.active_128_base_kib,
      (pressure.persistent_durable_replay_max_bytes +
        pressure.max_overlap_or_replay_clone_bytes) /
        1024,
      formula.quantum_kib,
    ),
  );

  assert.equal(
    pressure.time_budgets_seconds.total,
    pressure.time_budgets_seconds.memory_only +
      pressure.time_budgets_seconds.persistent,
  );
  const profiles = contract.profile_time_budgets_seconds;
  assert.ok(
    profiles.nightly_soak +
      pressure.time_budgets_seconds.total +
      profiles.minimum_non_pressure_headroom <
      profiles.nightly,
  );
  assert.ok(
    profiles.release_soak +
      pressure.time_budgets_seconds.total +
      profiles.minimum_non_pressure_headroom <
      profiles.release,
  );
  assert.ok(contract.ci_job_timeout_minutes.nightly * 60 > profiles.nightly);
  assert.ok(contract.ci_job_timeout_minutes.release * 60 > profiles.release);
  const workflow = parse(
    readFileSync(
      new URL(".github/workflows/reliability.yml", repositoryUrl),
      "utf8",
    ),
  );
  assert.equal(
    workflow.jobs["reliability-nightly"]["timeout-minutes"],
    contract.ci_job_timeout_minutes.nightly,
  );
  assert.equal(
    workflow.jobs["release-soak"]["timeout-minutes"],
    contract.ci_job_timeout_minutes.release,
  );
  assert.equal(
    pressure.owner_budgets.physical_starts_fill_delta,
    pressure.fill_runs,
  );
  assert.equal(
    pressure.owner_budgets.physical_starts_replacement_delta,
    pressure.replacement_runs,
  );
  assert.equal(pressure.owner_budgets.physical_starts_retry_delta, 0);
  assert.equal(
    pressure.owner_budgets.physical_starts_total_scope,
    "daemon_incarnation",
  );
  assert.equal(
    pressure.owner_budgets.physical_starts_total_epoch_changes_on_restart,
    true,
  );
  assert.equal(
    pressure.sampling.owner_maxima_source,
    "daemon_transition_high_water_counters",
  );
  assert.ok(
    pressure.resource_budgets.peak_thread_delta >= 3 * pressure.concurrency,
  );
  assert.deepEqual(pressure.measurement_semantics, {
    rss: "absolute_daemon_only_from_spawn_through_shutdown",
    stage_cpu:
      "daemon_only_core_normalized_delta_from_before_first_dispatch_through_final_post_replacement_boundary",
    quiescent_cpu: "daemon_only_core_normalized_over_each_five_second_dwell",
    threads_and_fds: "daemon_process_delta_from_post_readiness_baseline",
    owner_counts: "absolute_daemon_transition_high_water_and_boundary_counts",
    excludes: ["client", "helper_children", "sqlite_file_bytes"],
  });
});

test("GC helper is source-bound and emits PTY-stable exact ASCII payloads", () => {
  const helperBytes = readFileSync(helperPath);
  assert.equal(
    createHash("sha256").update(helperBytes).digest("hex"),
    contract.helper.sha256,
  );

  for (const [mode, index] of [
    ["memory_only", "5"],
    ["persistent", "2"],
    ["memory_only_soak", "0"],
    ["memory_replay_pressure", "0"],
    ["persistent_replay_pressure", "0"],
  ]) {
    const result = spawnSync(
      process.execPath,
      [helperPath, contract.seed, mode, index],
      { encoding: null, maxBuffer: 5 * MIB },
    );
    assert.equal(result.status, 0, result.stderr.toString("utf8"));
    const modeContract = contract.payload_modes[mode];
    const digestHex = createHash("sha256")
      .update(`${contract.seed}:${mode}:${index}`, "utf8")
      .digest("hex");
    assert.equal(result.stdout.length, modeContract.payload_bytes);
    assert.equal(
      result.stdout.equals(
        Buffer.from(digestHex.repeat(modeContract.hex_repetitions), "ascii"),
      ),
      true,
    );
    assert.match(result.stdout.toString("ascii"), /^[0-9a-f]+$/u);
    assert.equal(result.stdout.includes(0x0a), false);
    assert.equal(result.stdout.includes(0x0d), false);
  }
});

test("GC helper rejects every input outside the frozen invocation", () => {
  for (const args of [
    [],
    ["other", "memory_only", "0"],
    [contract.seed, "other", "0"],
    [contract.seed, "memory_only", "00"],
    [contract.seed, "memory_only", "-1"],
    [contract.seed, "memory_only", "0", "extra"],
  ]) {
    const result = spawnSync(process.execPath, [helperPath, ...args], {
      encoding: "utf8",
    });
    assert.notEqual(result.status, 0, args.join(" "));
    assert.equal(result.stdout, "");
  }
});

function rssCeiling(baseKib, logicalKib, quantumKib) {
  const raw = baseKib + (3 * logicalKib) / 2;
  return Math.ceil(raw / quantumKib) * quantumKib;
}
