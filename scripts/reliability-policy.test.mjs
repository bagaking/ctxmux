import assert from "node:assert/strict";
import { execFileSync, spawnSync } from "node:child_process";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

import {
  deriveBudgetCeiling,
  loadBaselineReceipts,
  loadSourceSnapshots,
  validateReliabilityPolicy,
} from "./reliability-policy.mjs";
import {
  POLICY_SOURCE_PATHS,
  SOURCE_FILE_PATHS,
} from "./reliability-baseline-policy.mjs";
import { deriveObservedMaxima } from "./reliability-budget-contract.mjs";

const root = path.resolve(import.meta.dirname, "..");
const receiptPaths = [
  "fixtures/reliability/observe-darwin-arm64-r1.json",
  "fixtures/reliability/observe-darwin-arm64-r2.json",
  "fixtures/reliability/observe-darwin-arm64-r3.json",
];
const stageIds = [
  "chaos-owner-matrix",
  "security-negative-space",
  "stress-and-soak",
  "resource-census",
];
const observedFields = [
  "cpu_core_percent",
  "peak_rss_kib",
  "steady_rss_kib",
  "retained_output_bytes_per_run",
  "rss_kib_per_run",
  "threads_per_run",
  "fds_per_run",
  "cleanup_threads_delta",
  "cleanup_live_children",
  "cleanup_attachments",
];

function actualInputs() {
  const budgets = JSON.parse(
    fs.readFileSync(path.join(root, "reliability-budgets.json"), "utf8"),
  );
  const baselineReceipts = loadBaselineReceipts(root, budgets);
  return {
    budgets,
    baselineReceipts,
    sourceSnapshots: loadSourceSnapshots(root, baselineReceipts),
    currentPolicyHashes: Object.fromEntries(
      POLICY_SOURCE_PATHS.map((filePath) => [
        filePath,
        crypto
          .createHash("sha256")
          .update(fs.readFileSync(path.join(root, filePath)))
          .digest("hex"),
      ]),
    ),
    workflow: fs.readFileSync(
      path.join(root, ".github", "workflows", "reliability.yml"),
      "utf8",
    ),
    checkScript: fs.readFileSync(
      path.join(root, "scripts", "check.sh"),
      "utf8",
    ),
    harnessSource: fs.readFileSync(
      path.join(root, "scripts", "reliability-qualification.ts"),
      "utf8",
    ),
  };
}

function syntheticSourceSnapshots(receipts, commit) {
  const tree = execFileSync("git", ["rev-parse", `${commit}^{tree}`], {
    cwd: root,
    encoding: "utf8",
  }).trim();
  const fileHashes = Object.fromEntries([
    ...SOURCE_FILE_PATHS.map((filePath) => [
      filePath,
      crypto
        .createHash("sha256")
        .update(
          execFileSync("git", ["show", `${commit}:${filePath}`], { cwd: root }),
        )
        .digest("hex"),
    ]),
    ...POLICY_SOURCE_PATHS.map((filePath) => [
      filePath,
      crypto
        .createHash("sha256")
        .update(fs.readFileSync(path.join(root, filePath)))
        .digest("hex"),
    ]),
  ]);
  return receipts.map(({ path: receiptPath }) => ({
    path: receiptPath,
    commit,
    reachableFromHead: true,
    tree,
    fileHashes,
  }));
}

function buildV2Template() {
  const inputs = actualInputs();
  const commit = execFileSync("git", ["rev-parse", "HEAD"], {
    cwd: root,
    encoding: "utf8",
  }).trim();
  const bootstrap = receiptPaths.map((receiptPath) => ({
    path: receiptPath,
    value: {
      schema: "ctxmux.reliability-qualification.v2",
      provenance: { source: { commit } },
    },
  }));
  const policyModulesAreCommitted =
    spawnSync("git", ["cat-file", "-e", `HEAD:${POLICY_SOURCE_PATHS.at(-1)}`], {
      cwd: root,
    }).status === 0;
  const snapshots = policyModulesAreCommitted
    ? loadSourceSnapshots(root, bootstrap)
    : syntheticSourceSnapshots(bootstrap, commit);
  assert.equal(snapshots.length, 3);
  assert.ok(snapshots.every(({ error }) => error === undefined));
  const firstSnapshot = snapshots[0];
  const environment = {
    os: "darwin",
    os_release: "test-release",
    architecture: "arm64",
    logical_cpus: 8,
    cpu_model: "synthetic-test-cpu",
  };
  inputs.budgets.observation_baseline.environment = environment;
  inputs.budgets.frozen_at = "2026-08-12T00:00:00.000Z";
  inputs.budgets.observation_baseline.rounds = 3;
  inputs.budgets.observation_baseline.resource_start_concurrency = 8;
  inputs.budgets.observation_baseline.peak_rss_sample_interval_ms = 25;
  inputs.budgets.observation_baseline.policy_contracts =
    POLICY_SOURCE_PATHS.map((filePath) => ({
      path: filePath,
      sha256: firstSnapshot.fileHashes[filePath],
    }));
  inputs.currentPolicyHashes = Object.fromEntries(
    POLICY_SOURCE_PATHS.map((filePath) => [
      filePath,
      firstSnapshot.fileHashes[filePath],
    ]),
  );

  const contractHash = crypto
    .createHash("sha256")
    .update(JSON.stringify(inputs.budgets.measurement_contract))
    .digest("hex");
  const provenance = {
    claim_scope: "locally_observed",
    binary_source_attestation: false,
    source: {
      commit,
      tree: firstSnapshot.tree,
      worktree: {
        status_format: "git-status-porcelain-v1-z",
        clean: true,
        entries: [],
      },
    },
    harness: {
      path: "scripts/reliability-qualification.ts",
      sha256: firstSnapshot.fileHashes["scripts/reliability-qualification.ts"],
    },
    launcher: {
      path: "scripts/check-reliability.sh",
      sha256: firstSnapshot.fileHashes["scripts/check-reliability.sh"],
    },
    daemon: {
      path: "target/reliability/provenance-build/debug/ctxmuxd",
      sha256: "d".repeat(64),
    },
    lockfiles: [
      {
        path: "Cargo.lock",
        sha256: firstSnapshot.fileHashes["Cargo.lock"],
      },
      {
        path: "package-lock.json",
        sha256: firstSnapshot.fileHashes["package-lock.json"],
      },
    ],
    build: {
      cwd: ".",
      argv: [
        "cargo",
        "build",
        "--locked",
        "--quiet",
        "--package",
        "ctxmux-daemon",
        "--target-dir",
        "target/reliability/provenance-build",
      ],
      source_commit: commit,
      source_tree: firstSnapshot.tree,
      worktree_clean: true,
      target_directory: "target/reliability/provenance-build",
      daemon_path: "target/reliability/provenance-build/debug/ctxmuxd",
      locked: true,
    },
    toolchain: {
      rustc_version_verbose: "rustc synthetic verbose",
      cargo_version: "cargo synthetic",
      node_version: "v99.0.0",
    },
    measurement_contract_encoding: "json-stringify-utf8",
    measurement_contract_sha256: contractHash,
  };
  const declaredLimits = {
    frame_bytes: 1024 * 1024,
    retained_output_bytes_per_run: 4 * 1024 * 1024,
    live_event_capacity: 256,
    global_run_quota: null,
    global_attachment_quota: null,
    exited_run_gc: null,
    qualification_stage: "all",
    resource_counts: [1, 32, 128],
    resource_modes: ["idle", "active"],
    resource_start_concurrency: 8,
    peak_rss_sample_interval_ms: 25,
    soak_seconds: 0,
    seed_controls: ["fanout payload byte", "secret marker"],
    note: "Synthetic policy fixture; not an observation.",
  };
  const receipts = [1, 2, 3].map((round, index) => {
    const cells = ["idle", "active"].flatMap((mode) =>
      [1, 32, 128].map((runs) => resourceCell(mode, runs, round)),
    );
    const stages = stageIds.map((id) => ({
      id,
      status: "pass",
      started_at: `2026-08-11T00:0${index}:00.000Z`,
      completed_at: `2026-08-11T00:0${index}:01.000Z`,
      result: id === "resource-census" ? cells : { synthetic: true },
    }));
    const actionTrace = [
      {
        timestamp: `2026-08-11T00:0${index}:00.000Z`,
        action: "provenance.captured",
        source_commit: provenance.source.commit,
        worktree_clean: true,
        harness_sha256: provenance.harness.sha256,
        launcher_sha256: provenance.launcher.sha256,
        daemon_sha256: provenance.daemon.sha256,
        measurement_contract_sha256: provenance.measurement_contract_sha256,
      },
      {
        timestamp: `2026-08-11T00:0${index}:00.100Z`,
        action: "provenance.verified",
        observation_round: round,
      },
      ...stageIds.flatMap((id, stageIndex) => [
        {
          timestamp: `2026-08-11T00:0${index}:0${stageIndex + 1}.000Z`,
          action: "stage.start",
          id,
        },
        {
          timestamp: `2026-08-11T00:0${index}:0${stageIndex + 1}.500Z`,
          action: "stage.pass",
          id,
        },
      ]),
      {
        timestamp: `2026-08-11T00:0${index}:09.000Z`,
        action: "provenance.reverified",
        daemon_sha256: provenance.daemon.sha256,
      },
    ];
    return {
      path: receiptPaths[index],
      sha256: String(round).repeat(64),
      value: {
        schema: "ctxmux.reliability-qualification.v2",
        status: "pass",
        profile: "observe",
        observation_round: round,
        seed: 226004,
        recorded_at: `2026-08-11T00:0${index}:00.000Z`,
        completed_at: `2026-08-11T00:0${index}:10.000Z`,
        time_budget_seconds: 2700,
        environment: structuredClone(environment),
        provenance: structuredClone(provenance),
        declared_limits: structuredClone(declaredLimits),
        action_trace: actionTrace,
        stages,
        daemon_logs: ["target/reliability/observe/synthetic-daemon.log"],
        error: null,
      },
    };
  });
  inputs.baselineReceipts = receipts;
  inputs.sourceSnapshots = snapshots;
  inputs.budgets.observation_baseline.raw_receipts = receipts.map(
    ({ path: receiptPath, sha256 }) => ({ path: receiptPath, sha256 }),
  );
  inputs.budgets.observation_baseline.observed_maxima =
    observedMaxima(receipts);
  for (const mode of ["idle", "active"]) {
    for (const count of ["1", "32", "128"]) {
      const maxima =
        inputs.budgets.observation_baseline.observed_maxima[mode][count];
      inputs.budgets.budgets[mode][count] = Object.fromEntries(
        observedFields.map((field) => [
          `max_${field}`,
          deriveBudgetCeiling(field, maxima[field]),
        ]),
      );
    }
  }
  return inputs;
}

function resourceCell(mode, runs, round) {
  const baseline = {
    rss_kib: 1000,
    cpu_seconds: 1,
    threads: 2,
    fds: 4,
    descendants: [],
  };
  const steady = {
    rss_kib: baseline.rss_kib + runs * (100 + round),
    cpu_seconds: 2,
    threads: baseline.threads + runs,
    fds: baseline.fds + 2 * runs,
    descendants: [],
  };
  const cleanup = {
    rss_kib: 1000 + round,
    cpu_seconds: 3,
    threads: baseline.threads + round,
    fds: baseline.fds,
    descendants: [],
  };
  const retainedPerRun = mode === "active" ? 4096 + round : 0;
  return {
    mode,
    runs,
    baseline,
    steady,
    cleanup,
    peak_rss_kib: steady.rss_kib + 100,
    peak_rss_sample_count: 10,
    peak_rss_sample_interval_ms: 25,
    cpu_core_percent: (mode === "active" ? 10 : 1) + round,
    retained_output_bytes: retainedPerRun * runs,
    retained_output_bytes_per_run: retainedPerRun,
    rss_kib_per_run: 100 + round,
    threads_per_run: 1,
    fds_per_run: 2,
    cleanup_rss_kib_delta: round,
    cleanup_fds_delta: 0,
    cleanup_retained_runs: runs,
    cleanup_live_children: 0,
    cleanup_attachments: 0,
    intentional_retained_state_without_gc: true,
  };
}

function observedMaxima(receipts) {
  const result = { idle: {}, active: {} };
  for (const mode of ["idle", "active"]) {
    for (const count of ["1", "32", "128"]) {
      const cells = receipts.map(({ value }) =>
        value.stages
          .find(({ id }) => id === "resource-census")
          .result.find(
            (cell) => cell.mode === mode && String(cell.runs) === count,
          ),
      );
      result[mode][count] = deriveObservedMaxima(cells);
    }
  }
  return result;
}

const v2Template = buildV2Template();
const v2Inputs = () => structuredClone(v2Template);
const errorsFor = (mutate) => {
  const inputs = v2Inputs();
  mutate(inputs);
  return validateReliabilityPolicy(inputs);
};

test("accepts the checked-in source-bound v2 baseline", () => {
  const inputs = actualInputs();
  assert.deepEqual(validateReliabilityPolicy(inputs), []);
  assert.ok(
    inputs.baselineReceipts.every(
      ({ value }) => value.schema === "ctxmux.reliability-qualification.v2",
    ),
  );
  assert.equal(inputs.sourceSnapshots.length, 3);
  assert.ok(inputs.sourceSnapshots.every(({ error }) => error === undefined));
});

test("accepts a complete synthetic source-bound v2 baseline", () => {
  assert.deepEqual(validateReliabilityPolicy(v2Inputs()), []);
  assert.equal(v2Template.sourceSnapshots.length, 3);
  assert.ok(
    v2Template.sourceSnapshots.every(
      ({ reachableFromHead, tree, fileHashes }) =>
        reachableFromHead === true &&
        /^[0-9a-f]{40}$/u.test(tree) &&
        Object.keys(fileHashes).length === 5,
    ),
  );
});

test("derives every pre-registered ceiling without floating-point drift", () => {
  assert.equal(deriveBudgetCeiling("cpu_core_percent", 0), 5);
  assert.equal(deriveBudgetCeiling("cpu_core_percent", 10), 15);
  assert.equal(deriveBudgetCeiling("cpu_core_percent", 10.001), 20);
  assert.equal(deriveBudgetCeiling("peak_rss_kib", 0), 8192);
  assert.equal(deriveBudgetCeiling("peak_rss_kib", 8192), 12288);
  assert.equal(deriveBudgetCeiling("retained_output_bytes_per_run", 1), 4096);
  assert.equal(
    deriveBudgetCeiling("retained_output_bytes_per_run", 4096),
    8192,
  );
  assert.equal(deriveBudgetCeiling("rss_kib_per_run", 0.1), 256);
  assert.equal(deriveBudgetCeiling("threads_per_run", 0), 0.25);
  assert.equal(deriveBudgetCeiling("threads_per_run", 1.005), 1.5);
  assert.equal(deriveBudgetCeiling("fds_per_run", 1), 1.25);
  assert.equal(deriveBudgetCeiling("cleanup_threads_delta", 0), 1);
  assert.equal(deriveBudgetCeiling("cleanup_threads_delta", 1), 2);
  assert.equal(deriveBudgetCeiling("cleanup_live_children", 0.1), 1);
  assert.equal(deriveBudgetCeiling("cleanup_attachments", 0), 0);
});

test("maps all three raw round cells to the ten governed maxima", () => {
  const maxima = deriveObservedMaxima(
    [1, 2, 3].map((round) => resourceCell("active", 1, round)),
  );
  assert.deepEqual(maxima, {
    cpu_core_percent: 13,
    peak_rss_kib: 1203,
    steady_rss_kib: 1103,
    retained_output_bytes_per_run: 4099,
    rss_kib_per_run: 103,
    threads_per_run: 1,
    fds_per_run: 2,
    cleanup_threads_delta: 3,
    cleanup_live_children: 0,
    cleanup_attachments: 0,
  });
});

test("rejects an unfrozen or incomplete high-Run-count budget", () => {
  const inputs = actualInputs();
  inputs.budgets.frozen_before_optimization = false;
  delete inputs.budgets.budgets.active["128"];
  const errors = validateReliabilityPolicy(inputs);
  assert.ok(errors.some((error) => error.includes("before optimization")));
  assert.ok(errors.some((error) => error.includes("1/32/128")));
});

test("rejects a present but malformed budget cell", () => {
  const inputs = actualInputs();
  inputs.budgets.budgets.active["128"] = null;
  const errors = validateReliabilityPolicy(inputs);
  assert.ok(errors.some((error) => error.includes("budget must be an object")));
});

test("rejects changed or misreported source-bound v2 evidence", () => {
  const inputs = actualInputs();
  inputs.baselineReceipts[0].sha256 = "0".repeat(64);
  inputs.budgets.observation_baseline.observed_maxima.active[
    "128"
  ].peak_rss_kib = 1;
  const errors = validateReliabilityPolicy(inputs);
  assert.ok(errors.some((error) => error.includes("hash drifted")));
  assert.ok(errors.some((error) => error.includes("raw maximum")));
});

test("rejects unreachable smoke, nightly, release, and duration policy", () => {
  const inputs = actualInputs();
  inputs.checkScript = inputs.checkScript.replace(
    "scripts/check-reliability.sh --profile smoke",
    "true",
  );
  inputs.workflow = inputs.workflow.replace(
    "scripts/check-reliability.sh --profile release",
    "true",
  );
  inputs.harnessSource = inputs.harnessSource.replace(
    "release: 2 * 60 * 60",
    "release: 60",
  );
  const errors = validateReliabilityPolicy(inputs);
  assert.ok(errors.some((error) => error.includes("reliability smoke")));
  assert.ok(errors.some((error) => error.includes("release-soak")));
  assert.ok(errors.some((error) => error.includes("frozen lane policy")));
});

test("rejects mixed v1/v2 baseline receipts", () => {
  const inputs = actualInputs();
  inputs.baselineReceipts[0].value.schema =
    "ctxmux.reliability-qualification.v1";
  const errors = validateReliabilityPolicy(inputs);
  assert.ok(errors.some((error) => error.includes("must not mix")));
});

test("rejects an all-v1 observation baseline", () => {
  const inputs = actualInputs();
  for (const receipt of inputs.baselineReceipts) {
    receipt.value.schema = "ctxmux.reliability-qualification.v1";
  }
  const errors = validateReliabilityPolicy(inputs);
  assert.ok(errors.some((error) => error.includes("requires source-bound v2")));
});

test("rejects an unknown observation receipt generation", () => {
  const inputs = actualInputs();
  for (const receipt of inputs.baselineReceipts) {
    receipt.value.schema = "ctxmux.reliability-qualification.v3";
  }
  const errors = validateReliabilityPolicy(inputs);
  assert.ok(errors.some((error) => error.includes("requires source-bound v2")));
});

test("rejects missing, dirty, unreachable, and hash-drifted v2 provenance", async (t) => {
  await t.test("missing", () => {
    const errors = errorsFor(({ baselineReceipts }) => {
      delete baselineReceipts[0].value.provenance.daemon;
    });
    assert.ok(errors.some((error) => error.includes("provenance")));
  });
  await t.test("dirty", () => {
    const errors = errorsFor(({ baselineReceipts }) => {
      baselineReceipts[0].value.provenance.source.worktree.clean = false;
      baselineReceipts[0].value.provenance.source.worktree.entries = [
        " M scripts/reliability-qualification.ts",
      ];
    });
    assert.ok(errors.some((error) => error.includes("worktree must be clean")));
  });
  await t.test("unreachable", () => {
    const errors = errorsFor(({ sourceSnapshots }) => {
      sourceSnapshots[0].reachableFromHead = false;
    });
    assert.ok(errors.some((error) => error.includes("current-HEAD ancestor")));
  });
  await t.test("receipt hash drift", () => {
    const errors = errorsFor(({ baselineReceipts }) => {
      baselineReceipts[0].sha256 = "f".repeat(64);
    });
    assert.ok(errors.some((error) => error.includes("hash drifted")));
  });
  await t.test("Git blob hash drift", () => {
    const errors = errorsFor(({ baselineReceipts }) => {
      baselineReceipts[0].value.provenance.harness.sha256 = "f".repeat(64);
    });
    assert.ok(
      errors.some((error) =>
        error.includes(
          "source hash drifted for scripts/reliability-qualification.ts",
        ),
      ),
    );
  });
  await t.test("derivation policy drift", () => {
    const errors = errorsFor((inputs) => {
      inputs.currentPolicyHashes[POLICY_SOURCE_PATHS[0]] = "f".repeat(64);
    });
    assert.ok(errors.some((error) => error.includes("budget contract")));
  });
});

test("rejects mixed source, harness, binary, toolchain, host, seed, workload, and measurement identity", async (t) => {
  const mutations = [
    [
      "source",
      (receipt) => (receipt.provenance.source.commit = "a".repeat(40)),
    ],
    [
      "harness",
      (receipt) => (receipt.provenance.harness.sha256 = "a".repeat(64)),
    ],
    [
      "binary",
      (receipt) => (receipt.provenance.daemon.sha256 = "a".repeat(64)),
    ],
    [
      "lockfile",
      (receipt) => (receipt.provenance.lockfiles[0].sha256 = "a".repeat(64)),
    ],
    [
      "toolchain",
      (receipt) => (receipt.provenance.toolchain.cargo_version = "other"),
    ],
    ["host", (receipt) => (receipt.environment.cpu_model = "other")],
    ["seed", (receipt) => (receipt.seed += 1)],
    ["workload", (receipt) => (receipt.declared_limits.note = "other")],
    [
      "measurement",
      (receipt) =>
        (receipt.provenance.measurement_contract_sha256 = "a".repeat(64)),
    ],
  ];
  for (const [label, mutate] of mutations) {
    await t.test(label, () => {
      const errors = errorsFor(({ baselineReceipts }) => {
        mutate(baselineReceipts[1].value);
      });
      assert.ok(errors.length > 0, `${label} mutation passed`);
      assert.ok(
        errors.some(
          (error) =>
            error.includes("must share") ||
            error.includes("hash drifted") ||
            error.includes("ancestor"),
        ),
      );
    });
  }
});

test("rejects duplicate rounds, duplicate receipts, paths, and hashes", async (t) => {
  await t.test("round", () => {
    const errors = errorsFor(({ baselineReceipts }) => {
      baselineReceipts[1].value.observation_round = 1;
    });
    assert.ok(errors.some((error) => error.includes("rounds must be exactly")));
  });
  await t.test("round does not match fixture path", () => {
    const errors = errorsFor(({ baselineReceipts }) => {
      baselineReceipts[0].value.observation_round = 3;
      baselineReceipts[2].value.observation_round = 1;
    });
    assert.ok(
      errors.some((error) => error.includes("must carry observation round")),
    );
  });
  await t.test("loaded receipt", () => {
    const errors = errorsFor((inputs) => {
      inputs.baselineReceipts[1] = structuredClone(inputs.baselineReceipts[0]);
    });
    assert.ok(errors.some((error) => error.includes("duplicate receipt")));
  });
  await t.test("declared path", () => {
    const errors = errorsFor(({ budgets }) => {
      budgets.observation_baseline.raw_receipts[1].path =
        budgets.observation_baseline.raw_receipts[0].path;
    });
    assert.ok(errors.some((error) => error.includes("paths must be unique")));
  });
  await t.test("declared hash", () => {
    const errors = errorsFor(({ budgets }) => {
      budgets.observation_baseline.raw_receipts[1].sha256 =
        budgets.observation_baseline.raw_receipts[0].sha256;
    });
    assert.ok(errors.some((error) => error.includes("hashes must be unique")));
  });
});

test("rejects non-canonical receipt paths before filesystem access", () => {
  const inputs = v2Inputs();
  const unsafe =
    "fixtures/reliability/../reliability/observe-darwin-arm64-r1.json";
  inputs.budgets.observation_baseline.raw_receipts[0].path = unsafe;
  inputs.baselineReceipts[0].path = unsafe;
  inputs.sourceSnapshots[0].path = unsafe;
  const errors = validateReliabilityPolicy(inputs);
  assert.ok(errors.some((error) => error.includes("not canonical")));

  const budgets = structuredClone(inputs.budgets);
  budgets.observation_baseline.raw_receipts[0].path = "../../outside.json";
  assert.throws(
    () => loadBaselineReceipts(root, budgets),
    /refusing to read non-canonical receipt/u,
  );
});

test("rejects missing, duplicate, and malformed resource cells", async (t) => {
  const cells = (inputs) =>
    inputs.baselineReceipts[0].value.stages.find(
      ({ id }) => id === "resource-census",
    ).result;
  await t.test("missing", () => {
    const errors = errorsFor((inputs) => cells(inputs).pop());
    assert.ok(errors.some((error) => error.includes("exactly six cells")));
  });
  await t.test("duplicate", () => {
    const errors = errorsFor((inputs) => {
      const values = cells(inputs);
      values[5] = structuredClone(values[0]);
    });
    assert.ok(errors.some((error) => error.includes("must be unique")));
  });
  await t.test("malformed nested sample", () => {
    const errors = errorsFor((inputs) => {
      delete cells(inputs)[0].cleanup.threads;
    });
    assert.ok(errors.some((error) => error.includes("process sample")));
  });
  for (const [label, value] of [
    ["missing steady sample", undefined],
    ["null steady sample", null],
  ]) {
    await t.test(label, () => {
      const errors = errorsFor((inputs) => {
        const cell = cells(inputs)[0];
        if (value === undefined) delete cell.steady;
        else cell.steady = value;
      });
      assert.ok(
        errors.some(
          (error) =>
            error.includes("resource cell") || error.includes("process sample"),
        ),
      );
    });
  }
  await t.test("null cell", () => {
    const errors = errorsFor((inputs) => {
      cells(inputs)[0] = null;
    });
    assert.ok(errors.some((error) => error.includes("resource cell")));
  });
  await t.test("non-finite derived metric", () => {
    const errors = errorsFor((inputs) => {
      cells(inputs)[0].threads_per_run = Number.NaN;
    });
    assert.ok(errors.some((error) => error.includes("finite/non-negative")));
  });
});

test("rejects drift in every v2 observed maximum", async (t) => {
  for (const field of observedFields) {
    await t.test(field, () => {
      const errors = errorsFor(({ budgets }) => {
        budgets.observation_baseline.observed_maxima.active["128"][field] += 1;
      });
      assert.ok(
        errors.some(
          (error) => error.includes(field) && error.includes("raw maximum"),
        ),
      );
    });
  }
});

test("rejects deterministic ceiling inflation", () => {
  const errors = errorsFor(({ budgets }) => {
    budgets.budgets.active["128"].max_peak_rss_kib += 4096;
  });
  assert.ok(
    errors.some(
      (error) =>
        error.includes("max_peak_rss_kib") &&
        error.includes("deterministic ceiling"),
    ),
  );
});

test("is deletion-sensitive across receipt, provenance, samples, maxima, and ceilings", async (t) => {
  const mutations = [
    [
      "receipt",
      ({ baselineReceipts }) => delete baselineReceipts[0].value.seed,
    ],
    [
      "provenance",
      ({ baselineReceipts }) =>
        delete baselineReceipts[0].value.provenance.measurement_contract_sha256,
    ],
    [
      "sample",
      ({ baselineReceipts }) => {
        const stage = baselineReceipts[0].value.stages.find(
          ({ id }) => id === "resource-census",
        );
        delete stage.result[0].baseline.fds;
      },
    ],
    [
      "maximum",
      ({ budgets }) =>
        delete budgets.observation_baseline.observed_maxima.idle["1"]
          .cleanup_attachments,
    ],
    [
      "ceiling",
      ({ budgets }) => delete budgets.budgets.idle["1"].max_cleanup_attachments,
    ],
  ];
  for (const [label, mutate] of mutations) {
    await t.test(label, () => {
      assert.ok(errorsFor(mutate).length > 0, `${label} deletion passed`);
    });
  }
});

test("rejects partial stages, unfenced provenance, and unlocked builds", async (t) => {
  await t.test("partial stages", () => {
    const errors = errorsFor(({ baselineReceipts }) => {
      baselineReceipts[0].value.stages.splice(0, 1);
    });
    assert.ok(errors.some((error) => error.includes("exactly four stages")));
  });
  await t.test("unfenced provenance", () => {
    const errors = errorsFor(({ baselineReceipts }) => {
      baselineReceipts[0].value.action_trace.splice(0, 1);
    });
    assert.ok(errors.some((error) => error.includes("fence all four stages")));
  });
  await t.test("unlocked build", () => {
    const errors = errorsFor(({ baselineReceipts }) => {
      baselineReceipts[0].value.provenance.build.locked = false;
    });
    assert.ok(errors.some((error) => error.includes("fixed locked")));
  });
  await t.test("wrong daemon target", () => {
    const errors = errorsFor(({ baselineReceipts }) => {
      baselineReceipts[0].value.provenance.build.daemon_path =
        "target/debug/ctxmuxd";
    });
    assert.ok(errors.some((error) => error.includes("fixed locked")));
  });
});

test("rejects observation and freeze chronology contradictions", async (t) => {
  await t.test("completion before start", () => {
    const errors = errorsFor(({ baselineReceipts }) => {
      baselineReceipts[0].value.completed_at = "2026-08-10T00:00:00.000Z";
    });
    assert.ok(
      errors.some((error) => error.includes("before the budget freeze")),
    );
  });
  await t.test("freeze before completion", () => {
    const errors = errorsFor(({ budgets }) => {
      budgets.frozen_at = "2026-08-10T00:00:00.000Z";
    });
    assert.ok(
      errors.some((error) => error.includes("before the budget freeze")),
    );
  });
});
