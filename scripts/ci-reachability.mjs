import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";

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

function jobBlocks(workflow) {
  const jobsStart = workflow.search(/^jobs:\s*$/mu);
  if (jobsStart < 0) return new Map();
  const jobs = workflow.slice(jobsStart);
  const matches = [...jobs.matchAll(/^ {2}([a-z][a-z0-9_-]*):\s*$/gmu)];
  return new Map(
    matches.map((match, index) => [
      match[1],
      jobs.slice(match.index, matches[index + 1]?.index ?? jobs.length),
    ]),
  );
}

function sameMembers(left, right) {
  return (
    left.length === right.length &&
    [...left].sort().every((value, index) => value === [...right].sort()[index])
  );
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
  const workflowJobs = jobBlocks(workflow);
  for (const job of jobs.values()) {
    const block = workflowJobs.get(job.id);
    if (!block) {
      errors.push(`workflow is missing mapped job ${job.id}`);
      continue;
    }
    for (const platform of job.platforms) {
      if (!block.includes(platform)) {
        errors.push(`workflow job ${job.id} does not reach ${platform}`);
      }
    }
    if (!block.includes(job.command)) {
      errors.push(`workflow job ${job.id} does not run ${job.command}`);
    }
    if (job.required !== true)
      errors.push(`mapped job ${job.id} is not required`);
  }
  if (/continue-on-error\s*:\s*true/u.test(workflow)) {
    errors.push("required workflow evidence must not continue on error");
  }
  if (
    !/^\s{2}pull_request:\s*$/mu.test(workflow) ||
    !/^\s{2}push:\s*$/mu.test(workflow)
  ) {
    errors.push("workflow must run for pull requests and pushes");
  }

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
