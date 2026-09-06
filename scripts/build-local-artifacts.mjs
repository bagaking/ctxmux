import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

export const LOCAL_ARTIFACT_SCHEMA = "ctxmux.local-artifacts.v1";
export const MAX_MANIFEST_BYTES = 64 * 1024;
export const MAX_SDK_ARCHIVE_BYTES = 4 * 1024 * 1024;
export const MAX_SDK_UNPACKED_BYTES = 8 * 1024 * 1024;
export const MAX_SDK_ENTRIES = 512;
export const MAX_BINARY_BYTES = 128 * 1024 * 1024;

const SCRIPT_PATH = fileURLToPath(import.meta.url);
const DEFAULT_ROOT = path.dirname(path.dirname(SCRIPT_PATH));
const GIT_OBJECT_PATTERN = /^[0-9a-f]{40}$/u;
const COMMAND_OUTPUT_LIMIT = 8 * 1024 * 1024;
const SUPPORTED_PLATFORMS = new Set(["darwin", "linux"]);

export function canonicalEnvironment(ambientEnvironment = process.env) {
  const environment = { ...ambientEnvironment };
  for (const name of Object.keys(environment)) {
    if (
      name === "BASH_ENV" ||
      name === "ENV" ||
      name === "RUSTFLAGS" ||
      name === "CARGO_ENCODED_RUSTFLAGS" ||
      name === "CARGO_BUILD_RUSTFLAGS" ||
      name === "RUSTC" ||
      name === "CARGO_BUILD_RUSTC" ||
      name === "CARGO_BUILD_RUSTC_WRAPPER" ||
      name === "CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER" ||
      name === "RUSTC_WRAPPER" ||
      name === "RUSTC_WORKSPACE_WRAPPER" ||
      (name.startsWith("CARGO_TARGET_") && name.endsWith("_RUSTFLAGS")) ||
      name === "GIT_DIR" ||
      name === "GIT_WORK_TREE" ||
      name === "GIT_COMMON_DIR" ||
      name === "GIT_INDEX_FILE" ||
      name === "GIT_OBJECT_DIRECTORY" ||
      name === "GIT_ALTERNATE_OBJECT_DIRECTORIES" ||
      name === "GIT_CONFIG_COUNT" ||
      name === "GIT_CONFIG_PARAMETERS" ||
      name === "GIT_CONFIG_GLOBAL" ||
      name === "GIT_CONFIG_SYSTEM" ||
      name === "GIT_CONFIG_NOSYSTEM" ||
      name.startsWith("GIT_CONFIG_KEY_") ||
      name.startsWith("GIT_CONFIG_VALUE_")
    ) {
      delete environment[name];
    }
  }
  environment.GIT_CONFIG_GLOBAL = "/dev/null";
  environment.GIT_CONFIG_SYSTEM = "/dev/null";
  environment.GIT_CONFIG_NOSYSTEM = "1";
  return environment;
}

// rustc applies the last matching textual prefix. Directory separators prevent
// an adjacent checkout or cache name from being mistaken for this directory.
// Complete arguments are encoded for Cargo, so spaces and equals signs in FROM
// never pass through shell splitting. These private build inputs are not facts
// in the consumer manifest.
export function pathRemapFlags({
  sourceRoot,
  homeDirectory,
  cargoHomeDirectory,
  rustupHomeDirectory,
  sysroot,
  buildDirectory = path.join(sourceRoot, "target"),
}) {
  const directories = [
    [homeDirectory, "build-home"],
    [rustupHomeDirectory, "rust-toolchains"],
    [cargoHomeDirectory, "rust-dependencies"],
    [sysroot, "rust-sysroot"],
    [buildDirectory, "ctxmux-build"],
    [sourceRoot, "ctxmux-source"],
  ].map(([directory, label]) => {
    const resolved = path.resolve(sourceRoot, directory);
    // A fresh build directory can be absent beneath an aliased parent. Resolve
    // the existing ancestor first so future generated files are mapped too.
    let ancestor = resolved;
    while (!fs.existsSync(ancestor)) ancestor = path.dirname(ancestor);
    const real = path.join(
      fs.realpathSync(ancestor),
      path.relative(ancestor, resolved),
    );
    return { resolved, real, label };
  });
  const prefixes = new Map();
  for (const { resolved, real, label } of directories) {
    const aliases = new Set([resolved, real]);
    // On hosts with an aliased parent (for example a temporary directory), a
    // cache alias can resolve beneath that parent's real path. Cover the same
    // child beneath each declared parent spelling without scanning the cache.
    for (const parent of directories) {
      if (real.startsWith(`${parent.real}${path.sep}`)) {
        aliases.add(`${parent.resolved}${real.slice(parent.real.length)}`);
      }
    }
    for (const prefix of aliases) {
      prefixes.set(`${prefix}${path.sep}`, `${label}/`);
    }
  }
  return [...prefixes.entries()]
    .sort(([left], [right]) => left.length - right.length)
    .map(([prefix, label]) => `--remap-path-prefix=${prefix}=${label}`);
}

export function buildEnvironment(
  sourceDateEpoch,
  npmConfigRoot,
  {
    root = DEFAULT_ROOT,
    ambientEnvironment = process.env,
    pathRoots,
    buildDirectory = path.join(root, "target"),
  } = {},
) {
  const environment = canonicalEnvironment(ambientEnvironment);
  for (const name of Object.keys(environment)) {
    if (
      name.toLowerCase().startsWith("npm_config_") ||
      name === "CARGO_BUILD_TARGET" ||
      name === "CARGO_ENCODED_RUSTFLAGS" ||
      name === "CARGO_TARGET_DIR" ||
      name === "RUSTC_WRAPPER" ||
      name === "RUSTFLAGS"
    ) {
      delete environment[name];
    }
  }
  // Probe and Cargo use the same PATH-selected compiler. Empty wrappers also
  // override Cargo configuration; merely deleting ambient keys does not.
  environment.RUSTC = "rustc";
  environment.RUSTC_WRAPPER = "";
  environment.RUSTC_WORKSPACE_WRAPPER = "";
  const homeDirectory = os.homedir();
  const roots = pathRoots ?? {
    sourceRoot: root,
    buildDirectory,
    homeDirectory,
    cargoHomeDirectory:
      environment.CARGO_HOME ?? path.join(homeDirectory, ".cargo"),
    rustupHomeDirectory:
      environment.RUSTUP_HOME ?? path.join(homeDirectory, ".rustup"),
    // A registered toolchain may live outside both RUSTUP_HOME and HOME.
    sysroot: String(
      runChecked(environment.RUSTC, ["--print", "sysroot"], {
        cwd: root,
        environment,
      }),
    ).trim(),
  };
  environment.CARGO_ENCODED_RUSTFLAGS = pathRemapFlags(roots).join("\u001f");
  environment.CARGO_INCREMENTAL = "0";
  environment.SOURCE_DATE_EPOCH = sourceDateEpoch;
  environment.npm_config_audit = "false";
  environment.npm_config_fund = "false";
  environment.npm_config_globalconfig = path.join(
    npmConfigRoot,
    ".npmrc-global",
  );
  environment.npm_config_ignore_scripts = "true";
  environment.npm_config_update_notifier = "false";
  environment.npm_config_userconfig = path.join(npmConfigRoot, ".npmrc-user");
  return environment;
}

function runChecked(command, args, { cwd, environment, encoding = "utf8" }) {
  const result = spawnSync(command, args, {
    cwd,
    env: environment,
    encoding,
    maxBuffer: COMMAND_OUTPUT_LIMIT,
  });
  if (result.error !== undefined) {
    throw new Error(`failed to start ${command}: ${result.error.message}`);
  }
  if (result.status !== 0) {
    const stderr =
      typeof result.stderr === "string"
        ? result.stderr.trim()
        : Buffer.from(result.stderr ?? [])
            .toString("utf8")
            .trim();
    const stdout =
      typeof result.stdout === "string"
        ? result.stdout.trim()
        : Buffer.from(result.stdout ?? [])
            .toString("utf8")
            .trim();
    throw new Error(
      `${command} ${args.join(" ")} failed (${String(result.status)}): ${stderr || stdout || "no diagnostic"}`,
    );
  }
  return result.stdout;
}

function git(root, args, encoding = "utf8") {
  return runChecked(
    "/usr/bin/git",
    [
      "-c",
      "core.excludesFile=/dev/null",
      "-c",
      "core.fsmonitor=false",
      "-c",
      "core.untrackedCache=false",
      ...args,
    ],
    {
      cwd: root,
      environment: canonicalEnvironment(),
      encoding,
    },
  );
}

export function sourceIdentity(root = DEFAULT_ROOT) {
  const resolvedRoot = fs.realpathSync(root);
  const topLevel = String(
    git(resolvedRoot, ["rev-parse", "--show-toplevel"]),
  ).trim();
  if (fs.realpathSync(topLevel) !== resolvedRoot) {
    throw new Error(
      `artifact source root differs from Git top level: ${topLevel}`,
    );
  }
  const status = git(
    resolvedRoot,
    [
      "status",
      "--porcelain=v1",
      "-z",
      "--untracked-files=all",
      "--ignore-submodules=none",
    ],
    null,
  );
  if (!Buffer.isBuffer(status) || status.length !== 0) {
    throw new Error("artifact source worktree must be clean");
  }
  const commit = String(git(resolvedRoot, ["rev-parse", "HEAD"])).trim();
  const tree = String(git(resolvedRoot, ["rev-parse", "HEAD^{tree}"])).trim();
  const commitTimeUnix = String(
    git(resolvedRoot, ["show", "-s", "--format=%ct", "HEAD"]),
  ).trim();
  if (
    !GIT_OBJECT_PATTERN.test(commit) ||
    !GIT_OBJECT_PATTERN.test(tree) ||
    !/^(0|[1-9][0-9]*)$/u.test(commitTimeUnix)
  ) {
    throw new Error("Git returned a malformed source identity");
  }
  return { commit, tree, commit_time_unix: commitTimeUnix };
}

function assertSameSource(before, after) {
  if (
    before.commit !== after.commit ||
    before.tree !== after.tree ||
    before.commit_time_unix !== after.commit_time_unix
  ) {
    throw new Error("artifact source identity changed during the build");
  }
}

function prepareOutput(outputArgument) {
  if (typeof outputArgument !== "string" || outputArgument.length === 0) {
    throw new Error("artifact output directory is required");
  }
  const output = path.resolve(outputArgument);
  if (
    output === path.parse(output).root ||
    output === fs.realpathSync(DEFAULT_ROOT)
  ) {
    throw new Error("artifact output directory is too broad");
  }
  if (fs.existsSync(output)) {
    throw new Error(`artifact output already exists: ${output}`);
  }
  fs.mkdirSync(path.dirname(output), { recursive: true });
  return output;
}

function sha256(bytes) {
  return createHash("sha256").update(bytes).digest("hex");
}

function artifactDescriptor(root, relativePath, maximumBytes, executable) {
  const absolutePath = path.join(root, relativePath);
  const stat = fs.lstatSync(absolutePath);
  if (stat.isSymbolicLink() || !stat.isFile()) {
    throw new Error(`artifact is not a regular file: ${relativePath}`);
  }
  if (stat.size <= 0 || stat.size > maximumBytes) {
    throw new Error(`artifact size is outside its bound: ${relativePath}`);
  }
  const mode = stat.mode & 0o777;
  if ((executable && mode !== 0o755) || (!executable && mode !== 0o644)) {
    throw new Error(`artifact mode is not canonical: ${relativePath}`);
  }
  return {
    path: relativePath,
    sha256: sha256(fs.readFileSync(absolutePath)),
    bytes: stat.size,
    mode: executable ? "0755" : "0644",
  };
}

// The parenthesis holds an open-ended list of identity facts, not just the
// protocol. `ctxmuxd` names its handoff schema there too, and only `ctxmuxd`
// does, so this parser must read the protocol without asserting what follows it.
// Pinning `)` to the protocol digits is what broke the vendor build when the
// schema was added: the binaries were correct and this regex was not.
const BINARY_VERSION = (name) =>
  new RegExp(
    `^${name} ([0-9]+\\.[0-9]+\\.[0-9]+) \\(protocol ([0-9]+)[,)]`,
    "u",
  );

export function parseBinaryVersion(name, output) {
  const match = BINARY_VERSION(name).exec(output.trim());
  if (match === null) {
    throw new Error(`${name} returned a malformed version identity`);
  }
  return { version: match[1], protocol: Number(match[2]) };
}

function rustToolchain(root, environment) {
  const verbose = String(
    runChecked(environment.RUSTC, ["-vV"], { cwd: root, environment }),
  ).trim();
  const host = /^host: (?<host>[^\n]+)$/mu.exec(verbose)?.groups?.host;
  const release = /^release: (?<release>[^\n]+)$/mu.exec(verbose)?.groups
    ?.release;
  if (host === undefined || release === undefined) {
    throw new Error("rustc -vV omitted release or host identity");
  }
  return {
    rustc: release,
    cargo: String(
      runChecked("cargo", ["--version"], { cwd: root, environment }),
    ).trim(),
    target: host,
  };
}

// The caller owns this previously absent directory. A fresh source-specific
// build avoids binding an old default-target binary to a new source receipt.
export function buildNativeArtifacts(root, environment, targetDirectory) {
  fs.mkdirSync(targetDirectory);
  const toolchain = rustToolchain(root, environment);
  runChecked(
    "cargo",
    [
      "build",
      "--locked",
      "--release",
      "--target-dir",
      targetDirectory,
      "--target",
      toolchain.target,
      "--config",
      `build.build-dir=${JSON.stringify(targetDirectory)}`,
      "--bin",
      "ctxmux",
      "--bin",
      "ctxmuxd",
    ],
    { cwd: root, environment },
  );
  return {
    directory: path.join(targetDirectory, toolchain.target, "release"),
    toolchain,
  };
}

export async function buildLocalArtifacts({
  root = DEFAULT_ROOT,
  output: outputArgument,
} = {}) {
  const resolvedRoot = fs.realpathSync(root);
  const source = sourceIdentity(resolvedRoot);
  const output = prepareOutput(outputArgument);
  const stage = fs.mkdtempSync(
    path.join(path.dirname(output), ".ctxmux-local-artifacts-"),
  );
  try {
    if (!SUPPORTED_PLATFORMS.has(process.platform)) {
      throw new Error(
        `unsupported local artifact platform: ${process.platform}`,
      );
    }
    const targetDirectory = path.join(stage, ".native-build");
    const environment = buildEnvironment(source.commit_time_unix, stage, {
      root: resolvedRoot,
      buildDirectory: targetDirectory,
    });
    const native = buildNativeArtifacts(
      resolvedRoot,
      environment,
      targetDirectory,
    );
    runChecked("npm", ["run", "build", "--workspace", "@ctxmux/sdk"], {
      cwd: resolvedRoot,
      environment,
    });

    const packageDocument = JSON.parse(
      fs.readFileSync(
        path.join(resolvedRoot, "packages/sdk/package.json"),
        "utf8",
      ),
    );
    const packResult = JSON.parse(
      String(
        runChecked(
          "npm",
          [
            "pack",
            "--workspace",
            "@ctxmux/sdk",
            "--pack-destination",
            stage,
            "--ignore-scripts",
            "--json",
          ],
          { cwd: resolvedRoot, environment },
        ),
      ),
    );
    if (
      !Array.isArray(packResult) ||
      packResult.length !== 1 ||
      packResult[0]?.name !== packageDocument.name ||
      packResult[0]?.version !== packageDocument.version ||
      typeof packResult[0]?.filename !== "string" ||
      !Number.isSafeInteger(packResult[0]?.entryCount) ||
      packResult[0].entryCount <= 0 ||
      packResult[0].entryCount > MAX_SDK_ENTRIES ||
      !Number.isSafeInteger(packResult[0]?.unpackedSize) ||
      packResult[0].unpackedSize <= 0 ||
      packResult[0].unpackedSize > MAX_SDK_UNPACKED_BYTES
    ) {
      throw new Error("npm pack returned an invalid or unbounded SDK package");
    }

    const binDirectory = path.join(stage, "bin");
    fs.mkdirSync(binDirectory);
    for (const name of ["ctxmux", "ctxmuxd"]) {
      fs.copyFileSync(
        path.join(native.directory, name),
        path.join(binDirectory, name),
      );
      fs.chmodSync(path.join(binDirectory, name), 0o755);
    }
    const sdkArchivePath = packResult[0].filename;
    fs.chmodSync(path.join(stage, sdkArchivePath), 0o644);

    const ctxmuxIdentity = parseBinaryVersion(
      "ctxmux",
      String(
        runChecked(path.join(binDirectory, "ctxmux"), ["--version"], {
          cwd: stage,
          environment,
        }),
      ),
    );
    const daemonIdentity = parseBinaryVersion(
      "ctxmuxd",
      String(
        runChecked(path.join(binDirectory, "ctxmuxd"), ["--version"], {
          cwd: stage,
          environment,
        }),
      ),
    );
    const sdk = await import(
      `${pathToFileURL(path.join(resolvedRoot, "packages/sdk/dist/index.js")).href}?commit=${source.commit}`
    );
    if (
      !Number.isSafeInteger(sdk.PROTOCOL_VERSION) ||
      sdk.PROTOCOL_VERSION !== ctxmuxIdentity.protocol ||
      sdk.PROTOCOL_VERSION !== daemonIdentity.protocol ||
      ctxmuxIdentity.version !== daemonIdentity.version
    ) {
      throw new Error("SDK and binary build identities disagree");
    }
    // Retire only this producer-owned build cache before publishing the bundle.
    fs.rmSync(targetDirectory, { recursive: true });
    const toolchain = native.toolchain;
    const sdkArchive = artifactDescriptor(
      stage,
      sdkArchivePath,
      MAX_SDK_ARCHIVE_BYTES,
      false,
    );
    const binaries = ["ctxmux", "ctxmuxd"].map((name) => ({
      name,
      version: ctxmuxIdentity.version,
      protocol: sdk.PROTOCOL_VERSION,
      ...artifactDescriptor(stage, `bin/${name}`, MAX_BINARY_BYTES, true),
    }));
    const manifest = {
      schema: LOCAL_ARTIFACT_SCHEMA,
      source: {
        ...source,
        worktree_clean: true,
      },
      product: {
        version: ctxmuxIdentity.version,
        protocol: sdk.PROTOCOL_VERSION,
      },
      support: {
        platform: process.platform,
        architecture: process.arch,
        rust_target: toolchain.target,
        transport: "unix",
      },
      build: {
        profile: "release",
        locked: true,
        source_date_epoch: source.commit_time_unix,
        rustc: toolchain.rustc,
        cargo: toolchain.cargo,
        node: process.version,
        npm: String(
          runChecked("npm", ["--version"], {
            cwd: resolvedRoot,
            environment,
          }),
        ).trim(),
      },
      sdk: {
        name: packageDocument.name,
        version: packageDocument.version,
        protocol: sdk.PROTOCOL_VERSION,
        entry_count: packResult[0].entryCount,
        unpacked_bytes: packResult[0].unpackedSize,
        archive: sdkArchive,
      },
      binaries,
      determinism: {
        sdk_archive: "byte-reproducible for the bound source and npm identity",
        binaries:
          "content-addressed for the bound source, target, profile, and Rust toolchain",
      },
    };
    const manifestBytes = Buffer.from(`${JSON.stringify(manifest, null, 2)}\n`);
    if (manifestBytes.length > MAX_MANIFEST_BYTES) {
      throw new Error("local artifact manifest exceeds its byte bound");
    }
    fs.writeFileSync(path.join(stage, "manifest.json"), manifestBytes, {
      mode: 0o644,
      flag: "wx",
    });
    assertSameSource(source, sourceIdentity(resolvedRoot));
    if (fs.existsSync(output)) {
      throw new Error(`artifact output appeared during the build: ${output}`);
    }
    fs.renameSync(stage, output);
    return manifest;
  } catch (error) {
    fs.rmSync(stage, { recursive: true, force: true });
    throw error;
  }
}

async function main() {
  if (process.argv.length !== 3) {
    throw new Error(
      "usage: node scripts/build-local-artifacts.mjs <output-directory>",
    );
  }
  const manifest = await buildLocalArtifacts({ output: process.argv[2] });
  process.stdout.write(
    `${path.resolve(process.argv[2], "manifest.json")} ${manifest.source.commit}\n`,
  );
}

if (
  process.argv[1] !== undefined &&
  fs.realpathSync(SCRIPT_PATH) === fs.realpathSync(process.argv[1])
) {
  main().catch((error) => {
    process.stderr.write(
      `${error instanceof Error ? error.message : String(error)}\n`,
    );
    process.exitCode = 1;
  });
}
