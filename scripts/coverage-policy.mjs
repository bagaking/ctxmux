import { execFileSync } from "node:child_process";
import fs from "node:fs";
import { createRequire } from "node:module";
import path from "node:path";
import { pathToFileURL } from "node:url";

const require = createRequire(import.meta.url);
const { createCoverageMap } = require("istanbul-lib-coverage");

function repoRelative(root, filename) {
  const relative = path
    .relative(root, path.resolve(filename))
    .replaceAll(path.sep, "/");
  if (relative === ".." || relative.startsWith("../")) {
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
      files.set(current, new Map());
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
  for (const count of lines.values()) {
    covered += Number(count > 0);
  }
  const total = lines.size;
  return {
    covered,
    total,
    percent: total === 0 ? 100 : (covered / total) * 100,
  };
}

export function evaluateGroups(policy, reports) {
  const results = policy.groups.map((group) => ({
    ...group,
    files: [],
    covered: 0,
    total: 0,
  }));
  const resultById = new Map(results.map((result) => [result.id, result]));
  const errors = [];

  for (const [language, files] of Object.entries(reports)) {
    for (const [filename, lines] of files) {
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
      result.total === 0 ? 0 : (result.covered / result.total) * 100;
    result.passed = result.percent >= result.minimum_line_percent;
    if (!result.passed) {
      errors.push(
        `${result.id} line coverage ${result.percent.toFixed(2)}% is below ${result.minimum_line_percent}%`,
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

export function evaluateChangedLines(
  policy,
  reports,
  changed,
  requireChangedLines = false,
) {
  const sourcePaths = new Set(policy.groups.flatMap((group) => group.paths));
  const allCoverage = new Map([...reports.rust, ...reports.typescript]);
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
  const evidenceMissing = requireChangedLines && percent === null;
  return {
    files,
    covered,
    total,
    percent,
    evidence_required: requireChangedLines,
    evidence_missing: evidenceMissing,
    passed:
      !evidenceMissing &&
      (percent === null || percent >= policy.changed_line_minimum),
  };
}

function gitDiff(root, base, paths) {
  const common = ["diff", "--unified=0", "--no-ext-diff"];
  let resolvedBase = base;
  if (!resolvedBase || /^0+$/u.test(resolvedBase)) {
    try {
      resolvedBase = execFileSync("git", ["rev-parse", "HEAD^"], {
        cwd: root,
        encoding: "utf8",
      }).trim();
    } catch {
      resolvedBase = "HEAD";
    }
  }
  const committed =
    resolvedBase === "HEAD"
      ? ""
      : execFileSync(
          "git",
          [...common, `${resolvedBase}...HEAD`, "--", ...paths],
          {
            cwd: root,
            encoding: "utf8",
          },
        );
  const working = execFileSync("git", [...common, "HEAD", "--", ...paths], {
    cwd: root,
    encoding: "utf8",
  });
  return `${committed}\n${working}`;
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

function parseBoolean(value, label) {
  if (value === "true") return true;
  if (value === "false" || value === undefined) return false;
  throw new Error(`${label} must be true or false, received ${value}`);
}

function printGroup(result) {
  const status = result.passed ? "PASS" : "FAIL";
  console.log(
    `${status} ${result.id}: ${result.percent.toFixed(2)}% (${result.covered}/${result.total}, minimum ${result.minimum_line_percent}%)`,
  );
  for (const file of result.files.sort((left, right) =>
    left.filename.localeCompare(right.filename),
  )) {
    console.log(
      `  ${file.filename}: ${file.percent.toFixed(2)}% (${file.covered}/${file.total})`,
    );
  }
}

export function runPolicy({
  root,
  policy,
  rustLcov,
  typescriptJson,
  base = "HEAD",
  requireChangedLines = false,
}) {
  const reports = {
    rust: parseLcov(rustLcov, root),
    typescript: parseIstanbul(typescriptJson, root),
  };
  const grouped = evaluateGroups(policy, reports);
  for (const result of grouped.results) printGroup(result);

  console.log("Reported coverage exclusions:");
  for (const exclusion of policy.reported_exclusions) {
    console.log(
      `  ${exclusion.category}/${exclusion.id}: ${exclusion.paths.join(", ")} (${exclusion.evidence})`,
    );
  }

  const sourcePaths = policy.groups.flatMap((group) => group.paths);
  console.log(`Changed-line comparison base: ${base}`);
  const changed = parseChangedLines(gitDiff(root, base, sourcePaths));
  const patch = evaluateChangedLines(
    policy,
    reports,
    changed,
    requireChangedLines,
  );
  if (patch.percent === null) {
    console.log(
      `${patch.passed ? "PASS" : "FAIL"} changed lines: no changed executable product lines`,
    );
  } else {
    console.log(
      `${patch.passed ? "PASS" : "FAIL"} changed lines: ${patch.percent.toFixed(2)}% (${patch.covered}/${patch.total}, minimum ${policy.changed_line_minimum}%)`,
    );
  }

  const errors = [...grouped.errors];
  if (patch.evidence_missing) {
    errors.push(
      "changed-line evidence was required but the comparison contained no changed executable product lines",
    );
  } else if (!patch.passed) {
    errors.push(
      `changed-line coverage ${patch.percent.toFixed(2)}% is below ${policy.changed_line_minimum}%`,
    );
  }
  return { reports, grouped, patch, errors };
}

function main() {
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
      fs.readFileSync(path.resolve(root, args.get("typescript-json")), "utf8"),
    ),
    base: args.get("base") ?? process.env.CTXMUX_COVERAGE_BASE ?? "HEAD",
    requireChangedLines: parseBoolean(
      args.get("require-changed-lines") ??
        process.env.CTXMUX_COVERAGE_REQUIRE_CHANGED_LINES,
      "require-changed-lines",
    ),
  });
  if (result.errors.length > 0) {
    for (const error of result.errors)
      console.error(`coverage policy: ${error}`);
    process.exitCode = 1;
  }
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  main();
}
