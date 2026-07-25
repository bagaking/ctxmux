import { existsSync, readFileSync } from "node:fs";
import { basename, posix, resolve } from "node:path";

function readText(path, errors, label) {
  try {
    return readFileSync(path, "utf8");
  } catch (error) {
    errors.push(`${label} is not readable: ${error.message}`);
    return null;
  }
}

function readJson(path, errors, label) {
  const source = readText(path, errors, label);
  if (source === null) return null;
  try {
    return JSON.parse(source);
  } catch (error) {
    errors.push(`${label} is not valid JSON: ${error.message}`);
    return null;
  }
}

function topLevelCommands(source) {
  return source
    .split(/\r?\n/u)
    .filter((line) => line.length > 0 && !/^\s/u.test(line))
    .map((line) => line.trim())
    .filter(
      (line) =>
        !line.startsWith("#") &&
        !line.startsWith("set ") &&
        !line.startsWith("cd "),
    );
}

function collectWorkspaceScripts(scripts, name, result, visited = new Set()) {
  if (visited.has(name) || typeof scripts[name] !== "string") return;
  visited.add(name);
  for (const segment of scripts[name].split(/\s*&&\s*/u)) {
    const words = segment.trim().split(/\s+/u);
    const npm = words.indexOf("npm");
    if (npm === -1 || words[npm + 1] !== "run") continue;
    const child = words[npm + 2];
    const all = words.indexOf("--workspaces", npm + 3);
    const one = words.indexOf("--workspace", npm + 3);
    if (all !== -1 && words.includes("--if-present")) {
      result.all.add(child);
    } else if (one !== -1 && words[one + 1]) {
      const packageScripts = result.packages.get(words[one + 1]) ?? new Set();
      packageScripts.add(child);
      result.packages.set(words[one + 1], packageScripts);
    } else {
      collectWorkspaceScripts(scripts, child, result, visited);
    }
  }
}

export function loadFixtureTestTargetContext(root) {
  const errors = [];
  const check = readText(resolve(root, "scripts/check.sh"), errors, "check.sh");
  const commands = check === null ? [] : topLevelCommands(check);
  if (!commands.includes("cargo test --workspace --all-targets")) {
    errors.push(
      "check.sh must directly execute `cargo test --workspace --all-targets`",
    );
  }
  if (!commands.includes("npm test")) {
    errors.push("check.sh must directly execute `npm test`");
  }

  const cargo = readText(resolve(root, "Cargo.toml"), errors, "Cargo.toml");
  const membersBlock = cargo?.match(/\bmembers\s*=\s*\[([\s\S]*?)\]/u);
  const members = new Set(
    [...(membersBlock?.[1].matchAll(/"([^"]+)"/gu) ?? [])].map(
      (match) => match[1],
    ),
  );
  if (membersBlock === null)
    errors.push("Cargo workspace members are not explicit");

  const rootPackage = readJson(
    resolve(root, "package.json"),
    errors,
    "package.json",
  );
  const workspaceScripts = { all: new Set(), packages: new Map() };
  if (rootPackage?.scripts) {
    collectWorkspaceScripts(rootPackage.scripts, "test", workspaceScripts);
  }
  if (workspaceScripts.all.size === 0 && workspaceScripts.packages.size === 0) {
    errors.push(
      "package.json scripts.test does not reach a workspace test script",
    );
  }
  return {
    commands: new Set(commands),
    errors,
    members,
    root,
    workspaceScripts,
  };
}

function withoutComments(source) {
  return source
    .replace(/\/\*[\s\S]*?\*\//gu, "")
    .split(/\r?\n/u)
    .filter((line) => !/^\s*\/\//u.test(line))
    .join("\n");
}

function hasRustTest(source, anchor) {
  if (!/^[a-z_][a-z0-9_]*$/u.test(anchor)) return false;
  const escaped = anchor.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
  const lines = withoutComments(source).split("\n");
  const definition = new RegExp(
    `^\\s*(?:async\\s+)?fn\\s+${escaped}\\s*\\(`,
    "u",
  );
  const attribute = /^\s*#\[(?:tokio::)?test(?:\([^\]]*\))?\]\s*$/u;
  for (let line = 0; line < lines.length; line += 1) {
    if (!definition.test(lines[line])) continue;
    for (let prior = Math.max(0, line - 12); prior < line; prior += 1) {
      if (!attribute.test(lines[prior])) continue;
      const gap = lines.slice(prior + 1, line).join("\n");
      let attributeStart = prior;
      while (attributeStart > 0) {
        const previous = lines[attributeStart - 1];
        if (previous.trim() === "" || /^\s*#\[[^\]]*\]\s*$/u.test(previous)) {
          attributeStart -= 1;
        } else {
          break;
        }
      }
      const attributes = lines.slice(attributeStart, line).join("\n");
      const ignored = /^\s*#\[\s*ignore(?:\s*=\s*"[^"]*")?\s*\]\s*$/mu.test(
        attributes,
      );
      if (gap.length <= 512 && !/[;{}]/u.test(gap) && !ignored) return true;
    }
  }
  return false;
}

function hasTypeScriptTest(source, anchor) {
  const escaped = anchor.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
  return new RegExp(
    `(?:^|\\n)\\s*test\\s*\\(\\s*["']${escaped}(?=\\s|["':;,.!?(){}-])`,
    "u",
  ).test(withoutComments(source));
}

function reachableRustModules(context, cratePath) {
  const reachable = new Set();
  const queue = ["src/lib.rs", "src/main.rs"].filter((relative) =>
    existsSync(resolve(context.root, cratePath, relative)),
  );
  while (queue.length > 0) {
    const relative = queue.shift();
    if (relative === undefined || reachable.has(relative)) continue;
    reachable.add(relative);
    const source = withoutComments(
      readFileSync(resolve(context.root, cratePath, relative), "utf8"),
    );
    const filename = posix.basename(relative);
    const moduleRoot =
      filename === "lib.rs" || filename === "main.rs" || filename === "mod.rs"
        ? posix.dirname(relative)
        : posix.join(posix.dirname(relative), filename.slice(0, -".rs".length));
    for (const match of source.matchAll(
      /(?:^|\n)\s*(?:pub(?:\([^)]*\))?\s+)?mod\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*;/gu,
    )) {
      const name = match[1];
      const candidates = [
        posix.join(moduleRoot, `${name}.rs`),
        posix.join(moduleRoot, name, "mod.rs"),
      ];
      const found = candidates.filter((candidate) =>
        existsSync(resolve(context.root, cratePath, candidate)),
      );
      if (found.length === 1) queue.push(found[0]);
    }
  }
  return reachable;
}

function rustErrors(context, reference, source) {
  const errors = [];
  const target = reference.path.match(
    /^(crates\/[^/]+)\/(src\/.+\.rs|tests\/[^/]+\.rs)$/u,
  );
  if (target === null || !context.members.has(target[1])) {
    return ["Rust path is not a Cargo workspace test target"];
  }
  const directTarget = /^(?:src\/(?:lib|main)\.rs|tests\/[^/]+\.rs)$/u.test(
    target[2],
  );
  if (
    !directTarget &&
    !reachableRustModules(context, target[1]).has(target[2])
  ) {
    return ["Rust path is not reachable from a Cargo workspace test target"];
  }
  const manifest = readText(
    resolve(context.root, target[1], "Cargo.toml"),
    errors,
    `${target[1]}/Cargo.toml`,
  );
  if (
    manifest !== null &&
    /\b(?:autotests|test)\s*=\s*false\b/u.test(manifest)
  ) {
    errors.push("the owning Cargo target disables tests");
  }
  if (!hasRustTest(source, reference.anchor)) {
    errors.push(
      `anchor ${JSON.stringify(reference.anchor)} is not a Rust #[test] function`,
    );
  }
  return errors;
}

function typeScriptErrors(context, reference, source) {
  const target = reference.path.match(/^(packages\/[^/]+)\/(.+\.test\.ts)$/u);
  if (target === null)
    return ["TypeScript path is not a workspace .test.ts target"];
  const errors = [];
  const packagePath = `${target[1]}/package.json`;
  const packageJson = readJson(
    resolve(context.root, packagePath),
    errors,
    packagePath,
  );
  if (packageJson === null) return errors;
  const selected = new Set(context.workspaceScripts.all);
  for (const script of context.workspaceScripts.packages.get(
    packageJson.name,
  ) ?? []) {
    selected.add(script);
  }
  const relativePath = posix.relative(target[1], reference.path);
  const reachable = [...selected].some((name) => {
    const words = packageJson.scripts?.[name]?.split(/\s+/u) ?? [];
    return (
      words[0] === "tsx" &&
      words.includes("--test") &&
      words.includes(relativePath)
    );
  });
  if (!reachable)
    errors.push(
      `${packagePath} has no gate-reachable script selecting ${relativePath}`,
    );
  if (!hasTypeScriptTest(source, reference.anchor)) {
    errors.push(
      `anchor ${JSON.stringify(reference.anchor)} is not the prefix of a declared node:test title`,
    );
  }
  return errors;
}

export function validateFixtureTestReference(context, reference) {
  const source = readFileSync(resolve(context.root, reference.path), "utf8");
  if (reference.path.endsWith(".rs"))
    return rustErrors(context, reference, source);
  if (reference.path.endsWith(".test.ts")) {
    return typeScriptErrors(context, reference, source);
  }
  if (reference.path.endsWith(".sh")) {
    const errors = [];
    if (!context.commands.has(reference.path)) {
      errors.push(
        "shell fixture script is not directly executed by scripts/check.sh",
      );
    }
    const invoked = topLevelCommands(source).some(
      (line) =>
        basename(line.split(/\s+/u)[0]).replace(/\.sh$/u, "") ===
        reference.anchor,
    );
    if (!invoked) {
      errors.push(
        `anchor ${JSON.stringify(reference.anchor)} is not a top-level command`,
      );
    }
    return errors;
  }
  return ["fixture path has no supported test-runner mapping"];
}

export function loadCurrentFeatureTaskIds(root) {
  const errors = [];
  const index = readJson(
    resolve(root, ".bagakit/feature-tracker/index/features.json"),
    errors,
    "Feature Tracker index",
  );
  const current =
    index?.features?.filter((feature) => feature.status === "in_progress") ??
    [];
  if (current.length !== 1) {
    errors.push(
      `Feature Tracker must have one in-progress Feature, found ${current.length}`,
    );
    return { errors, ids: new Set() };
  }
  const featureId = current[0].feat_id;
  const featureRoot = resolve(
    root,
    ".bagakit/feature-tracker/features",
    featureId,
  );
  const tasks = readJson(
    resolve(featureRoot, "tasks.json"),
    errors,
    `${featureId} tasks`,
  );
  const state = readJson(
    resolve(featureRoot, "state.json"),
    errors,
    `${featureId} state`,
  );
  if (tasks?.feat_id !== featureId || state?.feat_id !== featureId) {
    errors.push(`current Feature Tracker files do not agree on ${featureId}`);
  }
  const ids = new Set(tasks?.tasks?.map((task) => task.id) ?? []);
  if (
    [...ids].some((id) => !/^T-\d{3}$/u.test(id)) ||
    ids.size !== (tasks?.tasks?.length ?? -1)
  ) {
    errors.push(`${featureId} tasks contain invalid or duplicate ids`);
  }
  return { errors, ids };
}

export function trackedActivationTaskError(taskIds, activationTask) {
  if (/^T-\d{3}$/u.test(activationTask) && !taskIds.has(activationTask)) {
    return `${activationTask} does not exist in the current Feature Tracker tasks`;
  }
  return null;
}
