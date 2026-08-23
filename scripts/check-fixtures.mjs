#!/usr/bin/env node

import { readFileSync, readdirSync, statSync } from "node:fs";
import { dirname, isAbsolute, posix, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";

import {
  loadCurrentFeatureTaskIds,
  loadFixtureTestTargetContext,
  trackedActivationTaskError,
  validateFixtureTestReference,
} from "./fixture-validation.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const corpusPath = "fixtures/wrong-cases.json";
const choiceRoot = "docs/architecture/choices";
const allowedDispositions = new Set([
  "active",
  "characterization",
  "covered",
  "future",
  "rejected",
]);
const allowedPlatforms = new Set([
  "linux",
  "macos",
  "node",
  "portable",
  "rust",
  "unix",
]);
const errors = [];

function fail(message) {
  errors.push(message);
}

function isObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function exactKeys(value, expected, label) {
  if (!isObject(value)) {
    fail(`${label} must be an object`);
    return false;
  }

  const actual = Object.keys(value).sort();
  const wanted = [...expected].sort();
  if (actual.join("\0") !== wanted.join("\0")) {
    fail(
      `${label} keys drifted: expected [${wanted.join(", ")}], got [${actual.join(", ")}]`,
    );
    return false;
  }
  return true;
}

function nonEmptyString(value, label) {
  if (
    typeof value !== "string" ||
    value.trim() !== value ||
    value.length === 0
  ) {
    fail(`${label} must be a non-empty trimmed string`);
    return false;
  }
  return true;
}

function uniqueStrings(value, label, pattern) {
  if (!Array.isArray(value) || value.length === 0) {
    fail(`${label} must be a non-empty array`);
    return false;
  }

  const seen = new Set();
  let valid = true;
  for (const [index, item] of value.entries()) {
    if (!nonEmptyString(item, `${label}[${index}]`)) {
      valid = false;
      continue;
    }
    if (pattern && !pattern.test(item)) {
      fail(`${label}[${index}] has invalid value ${JSON.stringify(item)}`);
      valid = false;
    }
    if (seen.has(item)) {
      fail(`${label} contains duplicate ${JSON.stringify(item)}`);
      valid = false;
    }
    seen.add(item);
  }
  return valid;
}

function repositoryFile(reference, label, allowedPrefixes) {
  if (!nonEmptyString(reference, label)) return null;
  if (
    isAbsolute(reference) ||
    reference.includes("\\") ||
    posix.normalize(reference) !== reference ||
    reference.startsWith("../")
  ) {
    fail(
      `${label} must be a normalized repository-relative path: ${JSON.stringify(reference)}`,
    );
    return null;
  }
  if (
    allowedPrefixes &&
    !allowedPrefixes.some((prefix) => reference.startsWith(prefix))
  ) {
    fail(
      `${label} is outside its allowed repository roots: ${JSON.stringify(reference)}`,
    );
    return null;
  }

  const absolute = resolve(root, reference);
  if (absolute !== root && !absolute.startsWith(`${root}${sep}`)) {
    fail(`${label} escapes the repository: ${JSON.stringify(reference)}`);
    return null;
  }

  try {
    if (!statSync(absolute).isFile()) {
      fail(`${label} is not a regular file: ${JSON.stringify(reference)}`);
      return null;
    }
  } catch {
    fail(`${label} does not exist: ${JSON.stringify(reference)}`);
    return null;
  }
  return absolute;
}

const fixtureTestTargets = loadFixtureTestTargetContext(root);
for (const error of fixtureTestTargets.errors) {
  fail(`fixture test gate: ${error}`);
}

let corpus;
try {
  corpus = JSON.parse(readFileSync(resolve(root, corpusPath), "utf8"));
} catch (error) {
  console.error(`fixture corpus is not readable JSON: ${error.message}`);
  process.exit(1);
}

exactKeys(corpus, ["cases", "schema"], "corpus");
if (corpus.schema !== "ctxmux.wrong-case-corpus.v1") {
  fail(`corpus.schema must be "ctxmux.wrong-case-corpus.v1"`);
}
if (!Array.isArray(corpus.cases)) {
  fail("corpus.cases must be an array");
}

const hasTrackedActivationTask =
  Array.isArray(corpus.cases) &&
  corpus.cases.some(
    (item) =>
      isObject(item) &&
      typeof item.activation_task === "string" &&
      /^T-\d{3}$/u.test(item.activation_task),
  );
const currentFeatureTasks = hasTrackedActivationTask
  ? loadCurrentFeatureTaskIds(root)
  : { errors: [], ids: new Set() };
for (const error of currentFeatureTasks.errors) {
  fail(`activation task registry: ${error}`);
}

const commonKeys = [
  "action",
  "choice",
  "disposition",
  "failure_mechanism",
  "id",
  "invariant",
  "oracle",
  "platform",
  "preconditions",
  "source_ids",
  "source_refs",
  "tags",
];
const expectedChoiceFiles = readdirSync(resolve(root, choiceRoot))
  .filter((name) => /^\d{3}-[a-z0-9-]+\.md$/.test(name))
  .sort();
const corpusChoices = new Set();
const caseIds = new Set();
const sourceRegistry = new Map();
const counts = new Map([...allowedDispositions].map((value) => [value, 0]));

if (Array.isArray(corpus.cases)) {
  if (corpus.cases.length !== 41) {
    fail(
      `corpus must contain all 41 retained cases, got ${corpus.cases.length}`,
    );
  }

  for (const [index, item] of corpus.cases.entries()) {
    const label = `cases[${index}]`;
    if (!isObject(item)) {
      fail(`${label} must be an object`);
      continue;
    }

    const disposition = item.disposition;
    let conditionalKeys = [];
    if (disposition === "active" || disposition === "covered") {
      conditionalKeys = ["test_refs"];
    } else if (
      disposition === "future" ||
      disposition === "characterization" ||
      disposition === "rejected"
    ) {
      conditionalKeys = ["activation_task", "reason"];
    }
    exactKeys(item, [...commonKeys, ...conditionalKeys], label);

    if (
      nonEmptyString(item.id, `${label}.id`) &&
      !/^[A-Z]+(?:-[A-Z]+)*-\d{2,3}$/.test(item.id)
    ) {
      fail(
        `${label}.id has invalid case-id syntax: ${JSON.stringify(item.id)}`,
      );
    }
    if (caseIds.has(item.id))
      fail(`duplicate case id ${JSON.stringify(item.id)}`);
    caseIds.add(item.id);

    if (!allowedDispositions.has(disposition)) {
      fail(`${label}.disposition is invalid: ${JSON.stringify(disposition)}`);
    } else {
      counts.set(disposition, counts.get(disposition) + 1);
    }

    if (nonEmptyString(item.choice, `${label}.choice`)) {
      if (!/^\d{3}-[a-z0-9-]+$/.test(item.choice)) {
        fail(
          `${label}.choice has invalid syntax: ${JSON.stringify(item.choice)}`,
        );
      }
      const choiceRef = `${choiceRoot}/${item.choice}.md`;
      repositoryFile(choiceRef, `${label}.choice`, [`${choiceRoot}/`]);
      corpusChoices.add(`${item.choice}.md`);
    }

    for (const field of [
      "failure_mechanism",
      "invariant",
      "preconditions",
      "action",
      "oracle",
    ]) {
      nonEmptyString(item[field], `${label}.${field}`);
    }

    const validSourceIds = uniqueStrings(
      item.source_ids,
      `${label}.source_ids`,
      /^[a-n]\d{2}$/,
    );
    if (!Array.isArray(item.source_refs) || item.source_refs.length === 0) {
      fail(`${label}.source_refs must be a non-empty array`);
    } else if (
      validSourceIds &&
      item.source_refs.length !== item.source_ids.length
    ) {
      fail(`${label}.source_refs must align one-to-one with source_ids`);
    }
    if (Array.isArray(item.source_refs)) {
      const seenRefs = new Set();
      for (const [sourceIndex, reference] of item.source_refs.entries()) {
        const sourceId = Array.isArray(item.source_ids)
          ? item.source_ids[sourceIndex]
          : undefined;
        nonEmptyString(reference, `${label}.source_refs[${sourceIndex}]`);
        if (seenRefs.has(reference)) {
          fail(
            `${label}.source_refs contains duplicate ${JSON.stringify(reference)}`,
          );
        }
        seenRefs.add(reference);
        try {
          const sourceUrl = new URL(reference);
          if (
            sourceUrl.protocol !== "https:" ||
            sourceUrl.username !== "" ||
            sourceUrl.password !== ""
          ) {
            fail(
              `${label}.source_refs[${sourceIndex}] must be a credential-free HTTPS URL`,
            );
          }
        } catch {
          fail(`${label}.source_refs[${sourceIndex}] is not a valid URL`);
        }
        if (sourceId) {
          const prior = sourceRegistry.get(sourceId);
          if (prior !== undefined && prior !== reference) {
            fail(
              `${label}.source_refs[${sourceIndex}] changes ${sourceId} from ${JSON.stringify(prior)} to ${JSON.stringify(reference)}`,
            );
          }
          sourceRegistry.set(sourceId, reference);
        }
      }
    }

    if (uniqueStrings(item.platform, `${label}.platform`)) {
      for (const platform of item.platform) {
        if (!allowedPlatforms.has(platform)) {
          fail(
            `${label}.platform contains unsupported value ${JSON.stringify(platform)}`,
          );
        }
      }
    }
    uniqueStrings(item.tags, `${label}.tags`, /^[a-z0-9]+(?:-[a-z0-9]+)*$/);

    if (disposition === "active" || disposition === "covered") {
      if (!Array.isArray(item.test_refs) || item.test_refs.length === 0) {
        fail(`${label}.test_refs must be non-empty for ${disposition} cases`);
      } else {
        const seenTestRefs = new Set();
        for (const [testIndex, testRef] of item.test_refs.entries()) {
          const testLabel = `${label}.test_refs[${testIndex}]`;
          if (!exactKeys(testRef, ["anchor", "path"], testLabel)) continue;
          const key = `${testRef.path}\0${testRef.anchor}`;
          if (seenTestRefs.has(key))
            fail(`${label}.test_refs contains a duplicate path and anchor`);
          seenTestRefs.add(key);
          const absolute = repositoryFile(testRef.path, `${testLabel}.path`, [
            "crates/",
            "packages/",
            "scripts/",
          ]);
          if (
            nonEmptyString(testRef.anchor, `${testLabel}.anchor`) &&
            absolute
          ) {
            for (const error of validateFixtureTestReference(
              fixtureTestTargets,
              testRef,
            )) {
              fail(`${testLabel}: ${error}`);
            }
          }
        }
      }
    } else if (disposition === "future" || disposition === "characterization") {
      if (nonEmptyString(item.activation_task, `${label}.activation_task`)) {
        if (
          !/^(?:T-\d{3}|future:[a-z0-9]+(?:-[a-z0-9]+)*)$/.test(
            item.activation_task,
          )
        ) {
          fail(
            `${label}.activation_task has invalid syntax: ${JSON.stringify(item.activation_task)}`,
          );
        } else if (
          trackedActivationTaskError(
            currentFeatureTasks.ids,
            item.activation_task,
          ) !== null
        ) {
          fail(
            `${label}.activation_task does not exist in the current Feature Tracker tasks: ${JSON.stringify(item.activation_task)}`,
          );
        }
      }
      nonEmptyString(item.reason, `${label}.reason`);
    } else if (disposition === "rejected") {
      if (item.activation_task !== null) {
        fail(`${label}.activation_task must be null for rejected cases`);
      }
      nonEmptyString(item.reason, `${label}.reason`);
    }
  }
}

const actualChoiceFiles = [...corpusChoices].sort();
if (actualChoiceFiles.join("\0") !== expectedChoiceFiles.join("\0")) {
  fail(
    `choice coverage drifted: expected [${expectedChoiceFiles.join(", ")}], got [${actualChoiceFiles.join(", ")}]`,
  );
}
if (sourceRegistry.size !== 41) {
  fail(
    `source coverage drifted: expected 41 retained source ids, got ${sourceRegistry.size}`,
  );
}

if (errors.length > 0) {
  console.error(
    `fixture corpus validation failed with ${errors.length} error(s):`,
  );
  for (const error of errors) console.error(`- ${error}`);
  process.exit(1);
}

const countSummary = [...counts]
  .sort(([left], [right]) => left.localeCompare(right))
  .map(([disposition, count]) => `${disposition}=${count}`)
  .join(", ");
console.log(
  `fixture corpus ok: ${corpus.cases.length} cases (${countSummary})`,
);
