import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { isAlias, isMap, isPair, isScalar, isSeq, parseDocument } from "yaml";

const requiredCoverageBase =
  "${{ github.event.pull_request.base.sha || github.event.before }}";
const requiredCoverageComparison =
  "${{ github.event_name == 'pull_request' && 'merge-base' || 'direct' }}";
const requiredJobTrigger = "pull_request and push to main";
const requiredJobCommands = new Map([
  ["critical", "scripts/check.sh"],
  ["coverage", "scripts/check.sh --coverage"],
]);
const requiredJobPlatforms = new Map([
  ["critical", ["ubuntu-24.04", "macos-15"]],
  ["coverage", ["ubuntu-24.04"]],
]);
const requiredCheckoutAction = "actions/checkout@v4";
const requiredGateEnvironment = new Map([
  [
    "critical",
    {
      CTXMUX_REQUIRE_TMUX: "1",
      CTXMUX_TMUX_BIN: "tmux",
      CTXMUX_TMUX_QUALIFICATION: "${{ matrix.tmux_lane }}",
      CTXMUX_FUZZ_CASES: "512",
      CTXMUX_MODEL_CASES: "8",
    },
  ],
  [
    "coverage",
    {
      CTXMUX_REQUIRE_TMUX: "1",
      CTXMUX_TMUX_BIN: "tmux",
      CTXMUX_TMUX_QUALIFICATION: "minimum-3.4",
      CTXMUX_FUZZ_CASES: "512",
      CTXMUX_MODEL_CASES: "8",
    },
  ],
]);
const requiredCriticalMatrix = [
  { os: "ubuntu-24.04", tmux_lane: "minimum-3.4" },
  { os: "macos-15", tmux_lane: "current" },
];
const requiredStartupEnvironment = {
  BASH_ENV: "/dev/null",
  ENV: "/dev/null",
};
const requiredEnvironmentNeutralizationCommand =
  'unset BASH_ENV ENV GIT_DIR GIT_WORK_TREE GIT_COMMON_DIR GIT_INDEX_FILE GIT_OBJECT_DIRECTORY GIT_ALTERNATE_OBJECT_DIRECTORIES GIT_CONFIG_COUNT GIT_CONFIG_PARAMETERS GIT_CONFIG_GLOBAL GIT_CONFIG_SYSTEM GIT_CONFIG_NOSYSTEM "${!GIT_CONFIG_KEY_@}" "${!GIT_CONFIG_VALUE_@}"';
const requiredGitCommand =
  "GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_SYSTEM=/dev/null GIT_CONFIG_NOSYSTEM=1 /usr/bin/git";
const requiredGitStatusCommand = `${requiredGitCommand} -c core.excludesFile=/dev/null -c core.fsmonitor=false -c core.untrackedCache=false status --porcelain --untracked-files=all`;
const requiredEventSha = '"${{ github.sha }}"';
const requiredSourceIdentityCommand =
  requiredEnvironmentNeutralizationCommand +
  ` && test "$(${requiredGitCommand} rev-parse HEAD)" = ${requiredEventSha} && test -z "$(${requiredGitStatusCommand})"`;

function requiredGateRun(command) {
  return `${requiredSourceIdentityCommand} && exec /bin/bash --noprofile --norc ${command}`;
}

function walk(root) {
  const files = [];
  for (const entry of fs.readdirSync(root, { withFileTypes: true })) {
    const current = path.join(root, entry.name);
    if (entry.isDirectory()) files.push(...walk(current));
    else if (entry.isFile()) files.push(current);
  }
  return files;
}

function portable(root, filename) {
  return path.relative(root, filename).replaceAll(path.sep, "/");
}

export function discoverCriticalTests(root) {
  const discovered = new Set();
  for (const filename of walk(path.join(root, "crates"))) {
    if (!filename.endsWith(".rs")) continue;
    const source = fs.readFileSync(filename, "utf8");
    if (/^\s*#\[(?:tokio::)?test(?:\([^\]]*\))?\]/mu.test(source)) {
      discovered.add(portable(root, filename));
    }
  }
  for (const directory of [
    path.join(root, "packages", "sdk", "test"),
    path.join(root, "scripts"),
  ]) {
    for (const filename of walk(directory)) {
      if (filename.endsWith(".test.ts") || filename.endsWith(".test.mjs")) {
        discovered.add(portable(root, filename));
      }
    }
  }
  return discovered;
}

function sameMembers(left, right) {
  if (!Array.isArray(left) || !Array.isArray(right)) return false;
  return (
    left.length === right.length &&
    [...left].sort().every((value, index) => value === [...right].sort()[index])
  );
}

function sameRecords(left, right) {
  if (!Array.isArray(left) || !Array.isArray(right)) return false;
  return (
    left.length === right.length &&
    right.every((expected) =>
      left.some((candidate) => {
        const value = record(candidate);
        return (
          value &&
          sameMembers(Object.keys(value), Object.keys(expected)) &&
          Object.entries(expected).every(
            ([name, expectedValue]) => value[name] === expectedValue,
          )
        );
      }),
    )
  );
}

function record(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value
    : undefined;
}

function inspectYamlNode(node, errors) {
  if (node === null || node === undefined || isScalar(node)) return;
  if (isAlias(node)) {
    errors.push("workflow YAML must not contain aliases");
    return;
  }
  if (isMap(node)) {
    for (const pair of node.items) {
      if (!isPair(pair) || !isScalar(pair.key)) {
        errors.push("workflow YAML mapping keys must be scalar strings");
        continue;
      }
      if (typeof pair.key.value !== "string") {
        errors.push("workflow YAML mapping keys must be strings");
      } else if (pair.key.value === "<<") {
        errors.push("workflow YAML must not contain merge keys");
      }
      inspectYamlNode(pair.value, errors);
    }
    return;
  }
  if (isSeq(node)) {
    for (const item of node.items) inspectYamlNode(item, errors);
  }
}

function parseWorkflow(workflow, errors) {
  let document;
  try {
    document = parseDocument(workflow, {
      merge: false,
      prettyErrors: false,
      strict: true,
      uniqueKeys: true,
    });
  } catch (error) {
    errors.push(
      `workflow YAML parse failed: ${error instanceof Error ? error.message : String(error)}`,
    );
    return undefined;
  }
  for (const diagnostic of [...document.errors, ...document.warnings]) {
    errors.push(`workflow YAML parse failed: ${diagnostic.message}`);
  }
  const errorCount = errors.length;
  inspectYamlNode(document.contents, errors);
  if (errors.length > errorCount || document.errors.length > 0)
    return undefined;
  let value;
  try {
    value = document.toJS({ maxAliasCount: 0 });
  } catch (error) {
    errors.push(
      `workflow YAML conversion failed: ${error instanceof Error ? error.message : String(error)}`,
    );
    return undefined;
  }
  const workflowObject = record(value);
  if (!workflowObject) errors.push("workflow YAML root must be a mapping");
  return workflowObject;
}

function jobSteps(job) {
  return Array.isArray(job?.steps) ? job.steps.map(record).filter(Boolean) : [];
}

function commandSteps(job, command) {
  const run = requiredGateRun(command);
  return jobSteps(job).filter((step) => step.run === run);
}

function canonicalCheckoutPrecedesCommand(job, commandStep, fetchDepth) {
  const steps = jobSteps(job);
  const checkouts = steps.filter(
    (step) =>
      typeof step.uses === "string" &&
      step.uses.startsWith("actions/checkout@"),
  );
  const checkout = checkouts[0];
  const expectedKeys = fetchDepth === undefined ? ["uses"] : ["uses", "with"];
  const checkoutWith = record(checkout?.with);
  return (
    checkouts.length === 1 &&
    checkout?.uses === requiredCheckoutAction &&
    sameMembers(Object.keys(checkout), expectedKeys) &&
    (fetchDepth === undefined ||
      (checkoutWith !== undefined &&
        sameMembers(Object.keys(checkoutWith), ["fetch-depth"]) &&
        checkoutWith["fetch-depth"] === fetchDepth)) &&
    steps.indexOf(checkout) < steps.indexOf(commandStep)
  );
}

function validatesCoverageEnvironment(steps, expected) {
  return steps.some((step) =>
    Object.entries(expected).every(
      ([name, value]) => record(step.env)?.[name] === value,
    ),
  );
}

function jobMatrix(job) {
  return record(record(job?.strategy)?.matrix);
}

function validateRequiredGateEnvironment(jobId, workflowJob, runSteps, errors) {
  const expected = requiredGateEnvironment.get(jobId);
  if (!expected) return;
  const jobEnvironment = record(workflowJob.env);
  const commandEnvironment = record(runSteps[0]?.env);
  const allowedCommandNames = [
    ...Object.keys(expected),
    ...Object.keys(requiredStartupEnvironment),
    ...(jobId === "coverage"
      ? [
          "CTXMUX_COVERAGE_BASE",
          "CTXMUX_COVERAGE_CHANGED_LINE_MODE",
          "CTXMUX_COVERAGE_COMPARISON_MODE",
        ]
      : []),
  ];
  if (
    !jobEnvironment ||
    !sameMembers(Object.keys(jobEnvironment), Object.keys(expected)) ||
    !Object.entries(expected).every(
      ([name, value]) => jobEnvironment[name] === value,
    )
  ) {
    errors.push(
      `required workflow job ${jobId} must bind the canonical Gate environment at job scope`,
    );
  }
  if (
    runSteps.length !== 1 ||
    !commandEnvironment ||
    !sameMembers(Object.keys(commandEnvironment), allowedCommandNames) ||
    !Object.entries({ ...expected, ...requiredStartupEnvironment }).every(
      ([name, value]) => commandEnvironment[name] === value,
    )
  ) {
    errors.push(
      `required workflow job ${jobId} must bind the canonical Gate environment to its command step`,
    );
  }
  if (jobId === "critical") {
    const matrix = jobMatrix(workflowJob);
    if (
      !matrix ||
      !sameMembers(Object.keys(matrix), ["include"]) ||
      !sameRecords(matrix.include, requiredCriticalMatrix)
    ) {
      errors.push(
        "required workflow job critical must bind canonical tmux qualification lanes",
      );
    }
  }
}

function jobPlatforms(job) {
  const runsOn = job?.["runs-on"];
  if (!runsOn) return new Set();
  if (runsOn !== "${{ matrix.os }}") return new Set([runsOn]);
  const matrix = jobMatrix(job);
  if (!matrix) return new Set();
  const platforms = new Set();
  if (Array.isArray(matrix.os)) {
    for (const platform of matrix.os) platforms.add(platform);
  }
  if (Array.isArray(matrix.include)) {
    for (const entry of matrix.include) {
      const platform = record(entry)?.os;
      if (typeof platform === "string") platforms.add(platform);
    }
  }
  return platforms;
}

function validateWorkflowTriggers(workflow, errors) {
  const events = record(workflow?.on);
  if (
    !events ||
    !sameMembers(Object.keys(events), ["pull_request", "push"]) ||
    events.pull_request !== null
  ) {
    errors.push("workflow must run for every pull request");
  }
  const push = record(events?.push);
  if (
    !push ||
    !sameMembers(Object.keys(push), ["branches"]) ||
    !sameMembers(push.branches, ["main"])
  ) {
    errors.push("workflow push trigger must target only main");
  }
  if (workflow && Object.hasOwn(workflow, "defaults")) {
    errors.push("workflow must not customize required run defaults");
  }
}

function validateCoverageJobContract(mappedJob, workflowJob, runSteps, errors) {
  const contract = mappedJob.coverage_contract;
  if (!contract) {
    errors.push(`mapped coverage job ${mappedJob.id} has no coverage contract`);
    return;
  }
  if (
    contract.checkout_fetch_depth !== 0 ||
    runSteps.length !== 1 ||
    !canonicalCheckoutPrecedesCommand(
      workflowJob,
      runSteps[0],
      contract.checkout_fetch_depth,
    )
  ) {
    errors.push(
      `workflow job ${mappedJob.id} must have one prior unconditional full-history checkout`,
    );
  }
  if (
    contract.base !== requiredCoverageBase ||
    !validatesCoverageEnvironment(runSteps, {
      CTXMUX_COVERAGE_BASE: contract.base,
    })
  ) {
    errors.push(
      `workflow job ${mappedJob.id} does not bind the pull-request or push coverage base`,
    );
  }
  if (
    contract.changed_line_mode !== "auto" ||
    !validatesCoverageEnvironment(runSteps, {
      CTXMUX_COVERAGE_CHANGED_LINE_MODE: contract.changed_line_mode,
    })
  ) {
    errors.push(
      `workflow job ${mappedJob.id} does not enable changed-line auto mode`,
    );
  }
  if (
    contract.comparison_mode !== requiredCoverageComparison ||
    !validatesCoverageEnvironment(runSteps, {
      CTXMUX_COVERAGE_COMPARISON_MODE: contract.comparison_mode,
    })
  ) {
    errors.push(
      `workflow job ${mappedJob.id} does not select event-specific comparison semantics`,
    );
  }
  const expectedEnvironment = {
    CTXMUX_COVERAGE_BASE: contract.base,
    CTXMUX_COVERAGE_CHANGED_LINE_MODE: contract.changed_line_mode,
    CTXMUX_COVERAGE_COMPARISON_MODE: contract.comparison_mode,
  };
  if (
    runSteps.length > 0 &&
    !validatesCoverageEnvironment(runSteps, expectedEnvironment)
  ) {
    errors.push(
      `workflow job ${mappedJob.id} must bind the complete coverage environment to one command step`,
    );
  }
}

export function validateCiReachability({ root, map, workflow }) {
  const errors = [];
  if (map.schema !== "ctxmux.ci-evidence-map.v1") {
    errors.push(
      `unsupported CI evidence-map schema ${JSON.stringify(map.schema)}`,
    );
  }
  const jobs = new Map();
  for (const job of map.jobs ?? []) {
    if (jobs.has(job.id)) errors.push(`duplicate mapped job ${job.id}`);
    jobs.set(job.id, job);
  }
  for (const id of requiredJobCommands.keys()) {
    if (!jobs.has(id))
      errors.push(`CI evidence map is missing required job ${id}`);
  }
  const workflowDocument = parseWorkflow(workflow, errors);
  const workflowJobs = record(workflowDocument?.jobs) ?? {};
  for (const job of jobs.values()) {
    const workflowJob = record(workflowJobs[job.id]);
    if (!workflowJob) {
      errors.push(`workflow is missing mapped job ${job.id}`);
      continue;
    }
    const reachedPlatforms = jobPlatforms(workflowJob);
    const canonicalPlatforms = requiredJobPlatforms.get(job.id);
    if (
      job.required === true &&
      (!canonicalPlatforms || !sameMembers(job.platforms, canonicalPlatforms))
    ) {
      errors.push(
        `mapped required job ${job.id} must declare canonical platforms ${JSON.stringify(canonicalPlatforms)}`,
      );
    }
    for (const platform of job.platforms) {
      if (!reachedPlatforms.has(platform)) {
        errors.push(`workflow job ${job.id} does not reach ${platform}`);
      }
    }
    const requiredCommand = requiredJobCommands.get(job.id);
    const expectedCommand = requiredCommand ?? job.command;
    if (job.required === true) {
      if (!requiredCommand) {
        errors.push(
          `CI evidence map declares unexpected required job ${job.id}`,
        );
      } else if (job.command !== requiredCommand) {
        errors.push(
          `mapped required job ${job.id} must run canonical command ${JSON.stringify(requiredCommand)}`,
        );
      }
    }
    const matchingSteps = commandSteps(workflowJob, expectedCommand);
    const unconditionalSteps = matchingSteps.filter(
      (step) => !Object.hasOwn(step, "if"),
    );
    if (unconditionalSteps.length === 0) {
      if (matchingSteps.length > 0) {
        errors.push(
          `workflow job ${job.id} must not conditionally skip its command`,
        );
      } else {
        errors.push(`workflow job ${job.id} does not run ${expectedCommand}`);
      }
    } else if (matchingSteps.length !== 1) {
      errors.push(
        `workflow job ${job.id} must have exactly one canonical command step`,
      );
    }
    if (
      unconditionalSteps.some(
        (step) =>
          Object.hasOwn(step, "shell") ||
          Object.hasOwn(step, "working-directory"),
      ) ||
      Object.hasOwn(workflowJob, "defaults")
    ) {
      errors.push(
        `workflow job ${job.id} canonical command must use default shell and working directory`,
      );
    }
    if (job.required !== true) {
      errors.push(`mapped job ${job.id} is not required`);
    } else {
      if (job.trigger !== requiredJobTrigger) {
        errors.push(
          `mapped required job ${job.id} must declare ${requiredJobTrigger}`,
        );
      }
      if (Object.hasOwn(workflowJob, "if")) {
        errors.push(
          `required workflow job ${job.id} must not have a job-level condition`,
        );
      }
      if (Object.hasOwn(workflowJob, "needs")) {
        errors.push(
          `required workflow job ${job.id} must not depend on other jobs`,
        );
      }
      if (Object.hasOwn(jobMatrix(workflowJob) ?? {}, "exclude")) {
        errors.push(
          `required workflow job ${job.id} must not exclude matrix lanes`,
        );
      }
      const checkoutDepth = job.id === "coverage" ? 0 : undefined;
      if (
        unconditionalSteps.length !== 1 ||
        !canonicalCheckoutPrecedesCommand(
          workflowJob,
          unconditionalSteps[0],
          checkoutDepth,
        )
      ) {
        errors.push(
          `required workflow job ${job.id} must use one prior canonical source checkout`,
        );
      }
      if (
        unconditionalSteps.length !== 1 ||
        jobSteps(workflowJob).at(-1) !== unconditionalSteps[0]
      ) {
        errors.push(
          `required workflow job ${job.id} must run its exact startup-neutralized source fence and Gate as the final step`,
        );
      }
      validateRequiredGateEnvironment(
        job.id,
        workflowJob,
        unconditionalSteps,
        errors,
      );
      if (
        Object.hasOwn(workflowJob, "continue-on-error") ||
        jobSteps(workflowJob).some((step) =>
          Object.hasOwn(step, "continue-on-error"),
        )
      ) {
        errors.push(
          `required workflow job ${job.id} must not continue on error`,
        );
      }
    }
    if (job.id === "coverage") {
      validateCoverageJobContract(job, workflowJob, unconditionalSteps, errors);
    }
  }
  validateWorkflowTriggers(workflowDocument, errors);

  const suitePaths = new Map();
  const requiredJobs = [...jobs.values()]
    .filter(({ required }) => required)
    .map(({ id }) => id);
  for (const suite of map.suites ?? []) {
    if (suitePaths.has(suite.path)) {
      errors.push(`critical suite path ${suite.path} is mapped more than once`);
    }
    suitePaths.set(suite.path, suite);
    const suitePath = path.join(root, suite.path);
    if (!fs.existsSync(suitePath))
      errors.push(`mapped suite path does not exist: ${suite.path}`);
    if (!Array.isArray(suite.invariants) || suite.invariants.length === 0) {
      errors.push(`suite ${suite.id} has no mapped invariant`);
    }
    const selectionPath = path.join(root, suite.selection_ref);
    if (!fs.existsSync(selectionPath)) {
      errors.push(
        `suite ${suite.id} selection owner is missing: ${suite.selection_ref}`,
      );
    } else if (
      !fs.readFileSync(selectionPath, "utf8").includes(suite.selection_anchor)
    ) {
      errors.push(
        `suite ${suite.id} selection anchor is unreachable from ${suite.selection_ref}: ${suite.selection_anchor}`,
      );
    }

    const reachedJobs = (suite.reach ?? []).map(({ job }) => job);
    if (!sameMembers(reachedJobs, requiredJobs)) {
      errors.push(`suite ${suite.id} does not reach every required job`);
    }
    for (const reach of suite.reach ?? []) {
      const job = jobs.get(reach.job);
      if (!job) {
        errors.push(`suite ${suite.id} reaches unknown job ${reach.job}`);
      } else if (!sameMembers(reach.platforms, job.platforms)) {
        errors.push(
          `suite ${suite.id} has incomplete platform reach in job ${reach.job}`,
        );
      }
    }
  }

  for (const discovered of discoverCriticalTests(root)) {
    if (!suitePaths.has(discovered)) {
      errors.push(
        `checked-in critical test has no job-to-invariant mapping: ${discovered}`,
      );
    }
  }

  for (const suite of suitePaths.values()) {
    if (suite.kind !== "test" || !fs.existsSync(path.join(root, suite.path)))
      continue;
    const source = fs.readFileSync(path.join(root, suite.path), "utf8");
    if (
      suite.path.endsWith(".rs") &&
      /^\s*#\[\s*ignore(?:\s*=|\s*\])/mu.test(source)
    ) {
      errors.push(`mapped Rust suite contains ignored evidence: ${suite.path}`);
    }
    if (
      (suite.path.endsWith(".ts") || suite.path.endsWith(".mjs")) &&
      /^\s*(?:test|it|describe)\.(?:skip|todo)\s*\(/mu.test(source)
    ) {
      errors.push(
        `mapped JavaScript suite contains skipped or todo evidence: ${suite.path}`,
      );
    }
  }

  const nonRequired = map.non_required_evidence ?? {};
  for (const category of [
    "skipped",
    "ignored",
    "conditional",
    "schedule_only",
  ]) {
    if (!Array.isArray(nonRequired[category])) {
      errors.push(
        `non_required_evidence.${category} must be an explicit array`,
      );
      continue;
    }
    for (const item of nonRequired[category]) {
      if (!item.id || !item.reason) {
        errors.push(
          `non-required ${category} evidence must declare id and reason`,
        );
      }
    }
  }
  return errors;
}

function main() {
  const root = path.resolve(process.argv[2] ?? ".");
  const map = JSON.parse(
    fs.readFileSync(path.join(root, ".github", "ci-evidence-map.json"), "utf8"),
  );
  const workflow = fs.readFileSync(
    path.join(root, ".github", "workflows", "ci.yml"),
    "utf8",
  );
  const errors = validateCiReachability({ root, map, workflow });
  if (errors.length > 0) {
    for (const error of errors) console.error(`CI reachability: ${error}`);
    process.exitCode = 1;
  } else {
    console.log(
      `CI reachability: ${map.suites.length} suites mapped across ${map.jobs.length} required jobs`,
    );
  }
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  main();
}
