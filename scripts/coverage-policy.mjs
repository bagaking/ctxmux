import { execFileSync } from "node:child_process";
import fs from "node:fs";
import { createRequire } from "node:module";
import path from "node:path";
import { pathToFileURL } from "node:url";

const require = createRequire(import.meta.url);
const { createCoverageMap } = require("istanbul-lib-coverage");

const REQUIRED_FLOORS = Object.freeze({
  changed_line_percent: 90,
  runtime_line_percent: 85,
  pure_validator_line_percent: 95,
});
const CHANGED_LINE_MODES = new Set(["false", "true", "auto"]);
const COMPARISON_MODES = new Set(["merge-base", "direct"]);

function repoRelative(root, filename) {
  const absolute = path.isAbsolute(filename)
    ? filename
    : path.resolve(root, filename);
  const relative = path.relative(root, absolute).replaceAll(path.sep, "/");
  if (
    relative === ".." ||
    relative.startsWith("../") ||
    path.isAbsolute(relative)
  ) {
    throw new Error(`coverage path escapes the repository: ${filename}`);
  }
  return relative;
}

export function parseLcov(source, root) {
  const files = new Map();
  let current;
  for (const line of source.split(/\r?\n/u)) {
    if (line.startsWith("SF:")) {
      current = repoRelative(root, line.slice(3));
      if (!files.has(current)) files.set(current, new Map());
    } else if (line.startsWith("DA:") && current) {
      const [lineNumber, count] = line.slice(3).split(",", 2).map(Number);
      files.get(current).set(lineNumber, count);
    } else if (line === "end_of_record") {
      current = undefined;
    }
  }
  return files;
}

export function parseIstanbul(document, root) {
  const files = new Map();
  const coverage = createCoverageMap(document);
  for (const filename of coverage.files()) {
    const relative = repoRelative(root, filename);
    const lineCoverage = coverage.fileCoverageFor(filename).getLineCoverage();
    files.set(
      relative,
      new Map(
        Object.entries(lineCoverage).map(([line, count]) => [
          Number(line),
          count,
        ]),
      ),
    );
  }
  return files;
}

function summarizeLines(lines) {
  let covered = 0;
  for (const count of lines.values()) covered += Number(count > 0);
  const total = lines.size;
  return {
    covered,
    total,
    percent: total === 0 ? null : (covered / total) * 100,
  };
}

function floorFor(policy, floorClass) {
  if (floorClass === "runtime") return policy.floors?.runtime_line_percent;
  if (floorClass === "pure_validator") {
    return policy.floors?.pure_validator_line_percent;
  }
  return undefined;
}

function matchesGlob(filename, pattern) {
  return path.matchesGlob(filename, pattern);
}

function inventoryEntry(policy, filename) {
  const includes = (policy.source_inventory?.includes ?? []).filter(
    ({ glob }) => matchesGlob(filename, glob),
  );
  if (includes.length === 0) return undefined;
  const languages = [...new Set(includes.map(({ language }) => language))];
  const exclusions = (policy.source_inventory?.exclusions ?? []).filter(
    ({ glob, language }) =>
      languages.includes(language) && matchesGlob(filename, glob),
  );
  return { languages, exclusions };
}

export function evaluateSourceInventory(policy, repositoryFiles) {
  const errors = [];
  const files = new Map();

  if (policy.schema !== "ctxmux.coverage-policy.v2") {
    errors.push(
      `unsupported coverage policy schema ${JSON.stringify(policy.schema)}`,
    );
  }
  for (const [name, required] of Object.entries(REQUIRED_FLOORS)) {
    if (policy.floors?.[name] !== required) {
      errors.push(`coverage floor ${name} must remain ${required}`);
    }
  }
  if (!Array.isArray(policy.source_inventory?.includes)) {
    errors.push("source_inventory.includes must be an array");
  }
  if (!Array.isArray(policy.source_inventory?.exclusions)) {
    errors.push("source_inventory.exclusions must be an array");
  }

  const groupIds = new Set();
  const groupPaths = new Map();
  for (const group of policy.groups ?? []) {
    if (groupIds.has(group.id))
      errors.push(`duplicate coverage group ${group.id}`);
    groupIds.add(group.id);
    if (floorFor(policy, group.floor_class) === undefined) {
      errors.push(
        `${group.id} has unsupported floor class ${JSON.stringify(group.floor_class)}`,
      );
    }
    if (Object.hasOwn(group, "minimum_line_percent") || group.exception) {
      errors.push(
        `${group.id} must use its floor class without a local threshold or exception`,
      );
    }
    for (const filename of group.paths ?? []) {
      const owners = groupPaths.get(filename) ?? [];
      owners.push(group.id);
      groupPaths.set(filename, owners);
    }
  }
  for (const [filename, owners] of groupPaths) {
    if (owners.length !== 1) {
      errors.push(
        `${filename} is assigned to ${owners.length} policy groups: ${owners.join(", ")}`,
      );
    }
  }

  for (const filename of repositoryFiles) {
    const entry = inventoryEntry(policy, filename);
    if (!entry) continue;
    if (!/^[A-Za-z0-9._/-]+$/u.test(filename)) {
      errors.push(
        `product source path uses unsupported coverage characters: ${JSON.stringify(filename)}`,
      );
    }
    if (entry.languages.length !== 1) {
      errors.push(
        `${filename} matches ${entry.languages.length} source inventory languages`,
      );
      continue;
    }
    if (entry.exclusions.length > 1) {
      errors.push(
        `${filename} matches more than one source inventory exclusion`,
      );
    }
    const language = entry.languages[0];
    const exclusion = entry.exclusions[0];
    const owners = (policy.groups ?? []).filter(
      (group) => group.language === language && group.paths.includes(filename),
    );
    if (exclusion && owners.length > 0) {
      errors.push(
        `${filename} is both excluded and assigned to a coverage group`,
      );
    } else if (!exclusion && owners.length !== 1) {
      errors.push(
        `${filename} is assigned to ${owners.length} ${language} coverage groups (expected exactly one)`,
      );
    }
    files.set(filename, { language, exclusion });
  }

  for (const [filename] of groupPaths) {
    if (!files.has(filename)) {
      errors.push(
        `${filename} is not a discovered hand-written product source`,
      );
    }
  }
  for (const exclusion of policy.source_inventory?.exclusions ?? []) {
    const matched = [...files.values()].some(
      (entry) => entry.exclusion?.id === exclusion.id,
    );
    if (!matched) {
      errors.push(
        `source inventory exclusion ${exclusion.id} matches no files`,
      );
    }
    if (!exclusion.category || !exclusion.reason || !exclusion.evidence) {
      errors.push(
        `source inventory exclusion ${exclusion.id} must declare category, reason, and evidence`,
      );
    }
  }

  return { files, errors };
}

export function evaluateGroups(policy, reports, inventory) {
  const results = policy.groups.map((group) => ({
    ...group,
    minimum_line_percent: floorFor(policy, group.floor_class) ?? 0,
    files: [],
    covered: 0,
    total: 0,
  }));
  const resultById = new Map(results.map((result) => [result.id, result]));
  const errors = [];

  for (const [language, files] of Object.entries(reports)) {
    for (const [filename, lines] of files) {
      const source = inventory.files.get(filename);
      if (source?.exclusion) continue;
      if (!source) {
        errors.push(
          `${filename} is reported but is outside the product source inventory`,
        );
        continue;
      }
      const matches = policy.groups.filter(
        (group) =>
          group.language === language && group.paths.includes(filename),
      );
      if (matches.length !== 1) {
        errors.push(
          `${filename} is assigned to ${matches.length} ${language} coverage groups (expected exactly one)`,
        );
        continue;
      }
      const summary = summarizeLines(lines);
      const result = resultById.get(matches[0].id);
      result.files.push({ filename, ...summary });
      result.covered += summary.covered;
      result.total += summary.total;
    }
  }

  for (const result of results) {
    for (const expected of result.paths) {
      if (!result.files.some(({ filename }) => filename === expected)) {
        errors.push(`${result.id} is missing coverage for ${expected}`);
      }
    }
    result.percent =
      result.total === 0 ? null : (result.covered / result.total) * 100;
    result.passed =
      result.percent !== null && result.percent >= result.minimum_line_percent;
    if (!result.passed) {
      const measured =
        result.percent === null
          ? "no executable lines"
          : `${result.percent.toFixed(2)}%`;
      errors.push(
        `${result.id} line coverage ${measured} is below ${result.minimum_line_percent}%`,
      );
    }
  }

  return { results, errors };
}

export function parseChangedLines(diff) {
  const changed = new Map();
  let filename;
  for (const line of diff.split(/\r?\n/u)) {
    if (line.startsWith("+++ b/")) {
      filename = line.slice(6);
      if (!changed.has(filename)) changed.set(filename, new Set());
      continue;
    }
    const hunk = /^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@/u.exec(line);
    if (!hunk || !filename) continue;
    const start = Number(hunk[1]);
    const count = hunk[2] === undefined ? 1 : Number(hunk[2]);
    for (let offset = 0; offset < count; offset += 1) {
      changed.get(filename).add(start + offset);
    }
  }
  return changed;
}

function parseChangedLineMode(value) {
  if (CHANGED_LINE_MODES.has(value)) return value;
  throw new Error(
    `changed-line mode must be false, true, or auto, received ${value}`,
  );
}

export function evaluateChangedLines(
  policy,
  reports,
  changed,
  { mode = "false", productChanged = false } = {},
) {
  const parsedMode = parseChangedLineMode(mode);
  const sourcePaths = new Set(policy.groups.flatMap((group) => group.paths));
  const allCoverage = new Map([
    ...(reports.rust ?? []),
    ...(reports.typescript ?? []),
  ]);
  let covered = 0;
  let total = 0;
  const files = [];

  for (const [filename, changedLines] of changed) {
    if (!sourcePaths.has(filename)) continue;
    const coverage = allCoverage.get(filename);
    if (!coverage) continue;
    let fileCovered = 0;
    let fileTotal = 0;
    for (const line of changedLines) {
      if (!coverage.has(line)) continue;
      fileTotal += 1;
      fileCovered += Number(coverage.get(line) > 0);
    }
    if (fileTotal > 0) {
      files.push({ filename, covered: fileCovered, total: fileTotal });
      covered += fileCovered;
      total += fileTotal;
    }
  }

  const percent = total === 0 ? null : (covered / total) * 100;
  const evidenceRequired = parsedMode === "true";
  const evidenceMissing = evidenceRequired && percent === null;
  const outcome =
    percent === null
      ? evidenceMissing
        ? "fail"
        : "not_applicable"
      : percent >= policy.floors.changed_line_percent
        ? "pass"
        : "fail";
  return {
    files,
    covered,
    total,
    percent,
    mode: parsedMode,
    product_changed: productChanged,
    evidence_required: evidenceRequired,
    evidence_missing: evidenceMissing,
    outcome,
  };
}

function gitOutput(root, args) {
  return execFileSync("git", args, {
    cwd: root,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  }).trim();
}

function resolveCommit(root, revision, label) {
  try {
    return gitOutput(root, [
      "rev-parse",
      "--verify",
      "--end-of-options",
      `${revision}^{commit}`,
    ]);
  } catch {
    throw new Error(
      `${label} ${JSON.stringify(revision)} is not a valid commit`,
    );
  }
}

function isAncestor(root, ancestor, descendant) {
  try {
    execFileSync("git", ["merge-base", "--is-ancestor", ancestor, descendant], {
      cwd: root,
      stdio: ["ignore", "ignore", "pipe"],
    });
    return true;
  } catch (error) {
    if (
      error !== null &&
      typeof error === "object" &&
      "status" in error &&
      error.status === 1
    ) {
      return false;
    }
    throw new Error("failed to verify coverage base ancestry");
  }
}

export function resolveComparisonBase({
  root,
  base,
  comparison,
  mode = "false",
}) {
  const parsedMode = parseChangedLineMode(mode);
  const baseWasProvided = base !== undefined;
  const comparisonWasProvided = comparison !== undefined;
  if (parsedMode !== "false" && !baseWasProvided) {
    throw new Error(
      `${parsedMode} changed-line mode requires an explicit base`,
    );
  }
  if (parsedMode !== "false" && !comparisonWasProvided) {
    throw new Error(
      `${parsedMode} changed-line mode requires an explicit comparison mode`,
    );
  }

  const requestedBase = base ?? "HEAD";
  if (requestedBase.length === 0 || /^0+$/u.test(requestedBase)) {
    throw new Error("coverage comparison base must not be empty or a zero SHA");
  }
  const comparisonMode = comparison ?? "direct";
  if (!COMPARISON_MODES.has(comparisonMode)) {
    throw new Error(
      `comparison mode must be merge-base or direct, received ${comparisonMode}`,
    );
  }

  const head = resolveCommit(root, "HEAD", "HEAD");
  const resolvedBase = resolveCommit(
    root,
    requestedBase,
    "coverage comparison base",
  );
  if (parsedMode !== "false" && resolvedBase === head) {
    throw new Error(
      `${parsedMode} changed-line mode requires a base different from HEAD`,
    );
  }
  if (
    parsedMode !== "false" &&
    comparisonMode === "direct" &&
    !isAncestor(root, resolvedBase, head)
  ) {
    throw new Error(
      `${parsedMode} direct comparison requires the base to be an ancestor of HEAD`,
    );
  }

  let effectiveBase = resolvedBase;
  if (comparisonMode === "merge-base") {
    try {
      effectiveBase = gitOutput(root, ["merge-base", resolvedBase, head]);
    } catch {
      throw new Error(
        `coverage comparison base ${resolvedBase} has no merge base with HEAD`,
      );
    }
    if (!effectiveBase) {
      throw new Error(
        `coverage comparison base ${resolvedBase} has no merge base with HEAD`,
      );
    }
  }
  if (parsedMode !== "false" && effectiveBase === head) {
    throw new Error(
      `${parsedMode} changed-line mode requires an effective base different from HEAD`,
    );
  }

  return {
    mode: parsedMode,
    comparison: comparisonMode,
    requested_base: requestedBase,
    resolved_base: resolvedBase,
    effective_base: effectiveBase,
    head,
  };
}

function nullSeparated(output) {
  return output.split("\0").filter(Boolean);
}

function gitDiff(root, args) {
  return execFileSync("git", args, {
    cwd: root,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  });
}

export function collectGitChanges(root, resolution) {
  const patchArgs = ["diff", "--unified=0", "--no-ext-diff", "--no-renames"];
  const nameArgs = ["diff", "--name-only", "-z", "--no-renames"];
  const trackedPatch = gitDiff(root, [...patchArgs, resolution.effective_base]);
  const trackedNames = nullSeparated(
    gitDiff(root, [...nameArgs, resolution.effective_base]),
  );
  const untracked = nullSeparated(
    gitDiff(root, ["ls-files", "--others", "--exclude-standard", "-z"]),
  );
  return {
    patch: trackedPatch,
    changed_files: new Set([...trackedNames, ...untracked]),
    untracked_files: new Set(untracked),
  };
}

export function includeUntrackedExecutableLines(
  changed,
  reports,
  untrackedFiles,
) {
  const allCoverage = new Map([
    ...(reports.rust ?? []),
    ...(reports.typescript ?? []),
  ]);
  for (const filename of untrackedFiles) {
    const coverage = allCoverage.get(filename);
    if (coverage) changed.set(filename, new Set(coverage.keys()));
  }
  return changed;
}

export function changedProductFiles(policy, changedFiles) {
  return new Set(
    [...changedFiles].filter((filename) => {
      const entry = inventoryEntry(policy, filename);
      return entry !== undefined && entry.exclusions.length === 0;
    }),
  );
}

export function discoverRepositoryFiles(root) {
  return nullSeparated(
    gitDiff(root, [
      "ls-files",
      "--cached",
      "--others",
      "--exclude-standard",
      "-z",
    ]),
  );
}

function parseArguments(argv) {
  const values = new Map();
  for (let index = 0; index < argv.length; index += 2) {
    const name = argv[index];
    const value = argv[index + 1];
    if (!name?.startsWith("--") || value === undefined) {
      throw new Error(`expected --name value arguments, got ${argv.join(" ")}`);
    }
    values.set(name.slice(2), value);
  }
  for (const required of ["policy", "rust-lcov", "typescript-json"]) {
    if (!values.has(required)) throw new Error(`missing --${required}`);
  }
  return values;
}

function printGroup(result) {
  const status = result.passed ? "PASS" : "FAIL";
  const measured =
    result.percent === null
      ? "no executable lines"
      : `${result.percent.toFixed(2)}% (${result.covered}/${result.total})`;
  console.log(
    `${status} ${result.id}: ${measured}, ${result.floor_class} floor ${result.minimum_line_percent}%`,
  );
  for (const file of result.files.sort((left, right) =>
    left.filename.localeCompare(right.filename),
  )) {
    const fileMeasured =
      file.percent === null
        ? "no executable lines"
        : `${file.percent.toFixed(2)}% (${file.covered}/${file.total})`;
    console.log(`  ${file.filename}: ${fileMeasured}`);
  }
}

function printChangedLines(patch, policy) {
  if (patch.percent === null) {
    const status = patch.outcome === "not_applicable" ? "N/A" : "FAIL";
    const reason = patch.evidence_missing
      ? "explicit retained evidence requires a nonzero executable denominator"
      : patch.product_changed
        ? "product source changed but no executable denominator was reported"
        : "no product source changed with an executable denominator";
    console.log(`${status} changed lines (${patch.mode}): ${reason}`);
  } else {
    console.log(
      `${patch.outcome === "pass" ? "PASS" : "FAIL"} changed lines (${patch.mode}): ${patch.percent.toFixed(2)}% (${patch.covered}/${patch.total}, minimum ${policy.floors.changed_line_percent}%)`,
    );
  }
}

export function runPolicy({
  root,
  policy,
  rustLcov,
  typescriptJson,
  base,
  comparison,
  changedLineMode = "false",
}) {
  const reports = {
    rust: parseLcov(rustLcov, root),
    typescript: parseIstanbul(typescriptJson, root),
  };
  const repositoryFiles = discoverRepositoryFiles(root);
  const inventory = evaluateSourceInventory(policy, repositoryFiles);
  const grouped = evaluateGroups(policy, reports, inventory);
  for (const result of grouped.results) printGroup(result);

  console.log("Reported coverage exclusions:");
  for (const exclusion of policy.source_inventory.exclusions) {
    console.log(
      `  ${exclusion.category}/${exclusion.id}: ${exclusion.glob} (${exclusion.evidence})`,
    );
  }
  for (const exclusion of policy.reported_exclusions) {
    console.log(
      `  ${exclusion.category}/${exclusion.id}: ${exclusion.paths.join(", ")} (${exclusion.evidence})`,
    );
  }

  const resolution = resolveComparisonBase({
    root,
    base,
    comparison,
    mode: changedLineMode,
  });
  console.log(`Changed-line mode: ${resolution.mode}`);
  console.log(`Changed-line comparison: ${resolution.comparison}`);
  console.log(`Changed-line requested base: ${resolution.requested_base}`);
  console.log(`Changed-line resolved base: ${resolution.resolved_base}`);
  console.log(`Changed-line effective base: ${resolution.effective_base}`);
  console.log(`Changed-line HEAD: ${resolution.head}`);

  const changes = collectGitChanges(root, resolution);
  const changed = includeUntrackedExecutableLines(
    parseChangedLines(changes.patch),
    reports,
    changes.untracked_files,
  );
  const productChanged =
    changedProductFiles(policy, changes.changed_files).size > 0;
  const patch = evaluateChangedLines(policy, reports, changed, {
    mode: resolution.mode,
    productChanged,
  });
  printChangedLines(patch, policy);

  const errors = [...inventory.errors, ...grouped.errors];
  if (patch.outcome === "fail") {
    if (patch.evidence_missing) {
      errors.push(
        "changed-line evidence was required but the comparison contained no changed executable product lines",
      );
    } else {
      errors.push(
        `changed-line coverage ${patch.percent.toFixed(2)}% is below ${policy.floors.changed_line_percent}%`,
      );
    }
  }
  return { reports, inventory, grouped, resolution, patch, errors };
}

function main() {
  try {
    const args = parseArguments(process.argv.slice(2));
    const root = path.resolve(args.get("root") ?? ".");
    const policy = JSON.parse(
      fs.readFileSync(path.resolve(root, args.get("policy")), "utf8"),
    );
    const result = runPolicy({
      root,
      policy,
      rustLcov: fs.readFileSync(
        path.resolve(root, args.get("rust-lcov")),
        "utf8",
      ),
      typescriptJson: JSON.parse(
        fs.readFileSync(
          path.resolve(root, args.get("typescript-json")),
          "utf8",
        ),
      ),
      base: args.get("base") ?? process.env.CTXMUX_COVERAGE_BASE,
      comparison:
        args.get("comparison-mode") ??
        process.env.CTXMUX_COVERAGE_COMPARISON_MODE,
      changedLineMode:
        args.get("changed-line-mode") ??
        process.env.CTXMUX_COVERAGE_CHANGED_LINE_MODE ??
        "false",
    });
    if (result.errors.length > 0) {
      for (const error of result.errors) {
        console.error(`coverage policy: ${error}`);
      }
      process.exitCode = 1;
    }
  } catch (error) {
    console.error(
      `coverage policy: ${error instanceof Error ? error.message : String(error)}`,
    );
    process.exitCode = 1;
  }
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  main();
}
