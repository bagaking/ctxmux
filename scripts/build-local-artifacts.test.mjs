import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

import {
  buildEnvironment,
  buildNativeArtifacts,
  canonicalEnvironment,
  pathRemapFlags,
  parseBinaryVersion,
  sourceIdentity,
} from "./build-local-artifacts.mjs";

const root = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const builder = path.join(root, "scripts/build-local-artifacts.mjs");

function run(command, args, cwd) {
  const result = spawnSync(command, args, {
    cwd,
    encoding: "utf8",
  });
  assert.equal(
    result.status,
    0,
    `${command} ${args.join(" ")} failed: ${result.stderr}`,
  );
  return result;
}

function temporaryPathRoots(directory) {
  const homeDirectory = path.join(directory, "home space=fixture");
  const roots = {
    sourceRoot: path.join(homeDirectory, "source space=checkout"),
    homeDirectory,
    cargoHomeDirectory: path.join(homeDirectory, "cache space=registry"),
    rustupHomeDirectory: path.join(homeDirectory, "toolchains"),
    sysroot: path.join(directory, "external toolchain=fixture"),
  };
  for (const directoryPath of Object.values(roots)) {
    fs.mkdirSync(directoryPath, { recursive: true });
  }
  return roots;
}

test("the producer owns Rust flags and preserves unrelated environment inputs", () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "ctxmux-build-env-"));
  try {
    const pathRoots = temporaryPathRoots(directory);
    const ambientEnvironment = {
      ...process.env,
      RUSTFLAGS: "--cfg injected_rustflags",
      CARGO_ENCODED_RUSTFLAGS: "--cfg\u001finjected_encoded",
      CARGO_BUILD_RUSTFLAGS: "--cfg injected_build",
      RUSTC: "injected-compiler",
      CARGO_BUILD_RUSTC: "injected-build-compiler",
      CARGO_BUILD_RUSTC_WRAPPER: "injected-build-wrapper",
      CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER: "injected-build-workspace-wrapper",
      CARGO_TARGET_TEST_TARGET_RUSTFLAGS: "--cfg injected_target",
      RUSTC_WRAPPER: "injected-wrapper",
      RUSTC_WORKSPACE_WRAPPER: "injected-workspace-wrapper",
      CARGO_TARGET_DIR: "injected-target",
      npm_config_userconfig: "injected-npm-config",
      CTXMUX_FIXTURE_INPUT: "preserved",
    };
    const canonical = canonicalEnvironment(ambientEnvironment);
    for (const key of [
      "RUSTFLAGS",
      "CARGO_ENCODED_RUSTFLAGS",
      "CARGO_BUILD_RUSTFLAGS",
      "RUSTC",
      "CARGO_BUILD_RUSTC",
      "CARGO_BUILD_RUSTC_WRAPPER",
      "CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER",
      "CARGO_TARGET_TEST_TARGET_RUSTFLAGS",
      "RUSTC_WRAPPER",
      "RUSTC_WORKSPACE_WRAPPER",
    ]) {
      assert.equal(canonical[key], undefined, key);
    }
    const environment = buildEnvironment("0", directory, {
      pathRoots,
      ambientEnvironment,
    });
    assert.deepEqual(
      environment.CARGO_ENCODED_RUSTFLAGS.split("\u001f"),
      pathRemapFlags(pathRoots),
    );
    assert.equal(environment.CARGO_TARGET_DIR, undefined);
    assert.equal(environment.RUSTC, "rustc");
    assert.equal(environment.RUSTC_WRAPPER, "");
    assert.equal(environment.RUSTC_WORKSPACE_WRAPPER, "");
    assert.equal(environment.CTXMUX_FIXTURE_INPUT, "preserved");
    assert.equal(environment.HOME, process.env.HOME);
    assert.equal(environment.CARGO_INCREMENTAL, "0");
    assert.equal(ambientEnvironment.RUSTFLAGS, "--cfg injected_rustflags");
  } finally {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

function cargoFixture(directory, marker) {
  fs.mkdirSync(path.join(directory, "src"), { recursive: true });
  fs.writeFileSync(
    path.join(directory, "Cargo.toml"),
    '[package]\nname = "ctxmux-artifact-fixture"\nversion = "0.1.0"\nedition = "2024"\n[[bin]]\nname = "ctxmux"\npath = "src/main.rs"\n[[bin]]\nname = "ctxmuxd"\npath = "src/main.rs"\n',
  );
  fs.writeFileSync(
    path.join(directory, "src/main.rs"),
    `fn main() {
    if std::env::args().any(|arg| arg == "--probe-source") {
        println!(${JSON.stringify(marker)});
    } else {
        println!("{} 0.1.0 (protocol 21)", env!("CARGO_BIN_NAME"));
    }
}\n`,
  );
}

const COMPILER_OVERRIDES = [
  "RUSTC",
  "CARGO_BUILD_RUSTC",
  "RUSTC_WRAPPER",
  "CARGO_BUILD_RUSTC_WRAPPER",
  "RUSTC_WORKSPACE_WRAPPER",
  "CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER",
];

test("actual Cargo compiler and wrapper overrides cannot replace the probed compiler", (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "ctxmux-compiler-"));
  try {
    cargoFixture(directory, "current");
    const missing = path.join(directory, "missing-compiler");
    const baseline = { ...process.env };
    for (const key of COMPILER_OVERRIDES) delete baseline[key];
    for (const key of COMPILER_OVERRIDES) {
      const result = spawnSync("cargo", ["check", "--offline", "--quiet"], {
        cwd: directory,
        encoding: "utf8",
        env: {
          ...baseline,
          [key]: missing,
          CARGO_TARGET_DIR: path.join(directory, `counterfactual-${key}`),
        },
      });
      assert.notEqual(result.status, 0, key);
      assert.ok(result.stderr.includes("missing-compiler"), result.stderr);
    }
    fs.mkdirSync(path.join(directory, ".cargo"));
    fs.writeFileSync(
      path.join(directory, ".cargo/config.toml"),
      `[build]\nrustc = ${JSON.stringify(missing)}\nrustc-wrapper = ${JSON.stringify(missing)}\nrustc-workspace-wrapper = ${JSON.stringify(missing)}\n`,
    );
    const environment = buildEnvironment("0", directory, {
      root: directory,
      ambientEnvironment: {
        ...baseline,
        ...Object.fromEntries(COMPILER_OVERRIDES.map((key) => [key, missing])),
      },
    });
    const result = spawnSync("cargo", ["check", "--offline", "--quiet"], {
      cwd: directory,
      encoding: "utf8",
      env: environment,
    });
    assert.equal(result.status, 0, result.stderr);
    t.diagnostic(
      `all ${COMPILER_OVERRIDES.length} actual override counterexamples failed; producer-owned compiler and empty wrappers passed with conflicting config`,
    );
  } finally {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

test("native artifacts come from the owned build despite config redirection and same-version stale binaries", (t) => {
  const directory = fs.mkdtempSync(
    path.join(os.tmpdir(), "ctxmux-owned-build-"),
  );
  try {
    cargoFixture(directory, "historical");
    const environment = buildEnvironment("0", directory, { root: directory });
    const initial = spawnSync("cargo", ["build", "--offline", "--release"], {
      cwd: directory,
      env: environment,
      encoding: "utf8",
    });
    assert.equal(initial.status, 0, initial.stderr);
    const stale = path.join(directory, "target/release/ctxmux");
    const staleBytes = fs.readFileSync(stale);
    cargoFixture(directory, "current");
    const redirected = path.join(directory, "redirected");
    fs.mkdirSync(path.join(directory, ".cargo"));
    const config = path.join(directory, ".cargo/config.toml");
    fs.writeFileSync(
      config,
      `[build]\ntarget-dir = ${JSON.stringify(redirected)}\n`,
    );
    const misbound = spawnSync("cargo", ["build", "--offline", "--release"], {
      cwd: directory,
      env: environment,
      encoding: "utf8",
    });
    assert.equal(misbound.status, 0, misbound.stderr);
    const actual = path.join(redirected, "release/ctxmux");
    assert.equal(
      run(actual, ["--version"], directory).stdout,
      run(stale, ["--version"], directory).stdout,
    );
    assert.equal(
      run(stale, ["--probe-source"], directory).stdout,
      "historical\n",
    );
    assert.equal(
      run(actual, ["--probe-source"], directory).stdout,
      "current\n",
    );
    fs.writeFileSync(
      config,
      `[build]\ntarget-dir = ${JSON.stringify(redirected)}\nbuild-dir = ${JSON.stringify(redirected)}\ntarget = "ctxmux-missing-target"\n`,
    );
    const owned = path.join(directory, "owned space=build");
    const built = buildNativeArtifacts(directory, environment, owned);
    for (const name of ["ctxmux", "ctxmuxd"]) {
      assert.equal(
        run(path.join(built.directory, name), ["--probe-source"], directory)
          .stdout,
        "current\n",
      );
    }
    assert.equal(
      built.directory,
      path.join(owned, built.toolchain.target, "release"),
    );
    assert.deepEqual(fs.readFileSync(stale), staleBytes);
    assert.throws(() => buildNativeArtifacts(directory, environment, owned), {
      code: "EEXIST",
    });
    t.diagnostic(
      "same-version redirected build reproduces stale-source copy; owned fresh host build returns current source and preserves original target",
    );
  } finally {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

test("real rustc remaps source, cache aliases and external toolchain paths without losing diagnostics", (t) => {
  const directory = fs.mkdtempSync(
    path.join(os.tmpdir(), "ctxmux-rust-remap-"),
  );
  try {
    const roots = temporaryPathRoots(directory);
    const cacheAlias = path.join(directory, "cache alias=fixture");
    fs.symlinkSync(roots.cargoHomeDirectory, cacheAlias, "dir");
    const buildAliasParent = path.join(directory, "build parent alias=fixture");
    fs.symlinkSync(roots.sysroot, buildAliasParent, "dir");
    const buildDirectory = path.join(buildAliasParent, "new outputs=fixture");
    const pathRoots = {
      ...roots,
      cargoHomeDirectory: cacheAlias,
      buildDirectory,
    };
    const earlyFlags = pathRemapFlags(pathRoots);
    fs.mkdirSync(buildDirectory);
    const realBuildDirectory = fs.realpathSync(buildDirectory);
    const generated = path.join(realBuildDirectory, "generated.rs");
    const dependency = path.join(roots.cargoHomeDirectory, "dependency.rs");
    const toolchain = path.join(roots.sysroot, "toolchain.rs");
    const homeSource = path.join(roots.homeDirectory, "shared.rs");
    const rustupSource = path.join(roots.rustupHomeDirectory, "shared.rs");
    for (const filename of [
      dependency,
      toolchain,
      homeSource,
      rustupSource,
      generated,
    ]) {
      fs.writeFileSync(
        filename,
        "pub fn location() -> &'static str { file!() }\n",
      );
    }
    const source = path.join(roots.sourceRoot, "main.rs");
    fs.writeFileSync(
      source,
      `#[path = ${JSON.stringify(dependency)}] mod dependency; #[path = ${JSON.stringify(generated)}] mod generated;
#[path = ${JSON.stringify(path.join(cacheAlias, "dependency.rs"))}] mod alias;
#[path = ${JSON.stringify(toolchain)}] mod toolchain;
#[path = ${JSON.stringify(homeSource)}] mod shared;
#[path = ${JSON.stringify(rustupSource)}] mod rustup;
fn main() {
    println!("{}\\n{}\\n{}\\n{}\\n{}\\n{}\\n{}", file!(), dependency::location(), alias::location(), toolchain::location(), shared::location(), rustup::location(), generated::location());
    if std::env::args().any(|arg| arg == "panic") { panic!("retained diagnostic context"); }
}
`,
    );
    const environment = canonicalEnvironment();
    function compile(name, flags) {
      const binary = path.join(directory, name);
      const result = spawnSync("rustc", [source, "-o", binary, ...flags], {
        cwd: roots.sourceRoot,
        env: environment,
        encoding: "utf8",
      });
      assert.equal(result.status, 0, `rustc failed: ${result.stderr}`);
      return binary;
    }
    const unmapped = compile("unmapped", []);
    const baseline = run(unmapped, [], directory).stdout;
    assert.ok(baseline.includes(roots.sourceRoot));
    assert.ok(baseline.includes(roots.cargoHomeDirectory));
    assert.ok(baseline.includes(roots.sysroot));
    assert.ok(
      fs.readFileSync(unmapped).includes(Buffer.from(roots.homeDirectory)),
    );

    const flags = buildEnvironment("0", directory, {
      pathRoots,
    }).CARGO_ENCODED_RUSTFLAGS.split("\u001f");
    assert.deepEqual(flags, earlyFlags);
    const mapped = compile("mapped", flags);
    assert.equal(
      run(mapped, [], directory).stdout,
      "ctxmux-source/main.rs\nrust-dependencies/dependency.rs\nrust-dependencies/dependency.rs\nrust-sysroot/toolchain.rs\nbuild-home/shared.rs\nrust-toolchains/shared.rs\nctxmux-build/generated.rs\n",
    );
    const mappedBytes = fs.readFileSync(mapped);
    const unmappedBytes = fs.readFileSync(unmapped);
    const countPrefix = (bytes, prefix) => {
      const needle = Buffer.from(prefix);
      let count = 0;
      let offset = bytes.indexOf(needle);
      while (offset !== -1) {
        count += 1;
        offset = bytes.indexOf(needle, offset + needle.length);
      }
      return count;
    };
    const privacyCounts = Object.fromEntries(
      Object.entries({
        ...roots,
        cacheAlias,
        buildDirectory,
        realBuildDirectory,
      }).map(([label, prefix]) => [
        label,
        {
          unmapped: countPrefix(unmappedBytes, prefix),
          mapped: countPrefix(mappedBytes, prefix),
        },
      ]),
    );
    t.diagnostic(
      JSON.stringify({
        compiler: run("rustc", ["--version"], roots.sourceRoot).stdout.trim(),
        unmapped_sha256: createHash("sha256")
          .update(unmappedBytes)
          .digest("hex"),
        mapped_sha256: createHash("sha256").update(mappedBytes).digest("hex"),
        privacy_counts: privacyCounts,
      }),
    );
    for (const prefix of [
      ...Object.values(roots),
      cacheAlias,
      buildDirectory,
      realBuildDirectory,
    ]) {
      assert.equal(mappedBytes.includes(Buffer.from(prefix)), false);
    }
    const panic = spawnSync(mapped, ["panic"], {
      cwd: directory,
      encoding: "utf8",
    });
    assert.notEqual(panic.status, 0);
    assert.match(panic.stderr, /ctxmux-source\/main\.rs:8:/u);
    assert.match(panic.stderr, /retained diagnostic context/u);
    assert.equal(panic.stderr.includes(directory), false);

    // Executing the reversed flags proves last-match precedence in the actual
    // compiler, rather than assuming that sorting changes anything observable.
    const reversed = compile("reversed", [...flags].reverse());
    assert.match(
      run(reversed, [], directory).stdout,
      /^build-home\/source space=checkout\/main\.rs\n/u,
    );
  } finally {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

test("the version parser reads the protocol out of every identity a binary prints", () => {
  // The guard that was missing when `ctxmuxd --version` gained its handoff
  // schema: the suite tested dirty-tree rejection and nothing else, so the one
  // parser standing between the binaries and the vendor artifact had no
  // coverage at all. The build failed at the only supported way to produce a
  // vendor drop, and every test stayed green.
  //
  // The contract is that the parenthesis carries an open-ended list of identity
  // facts whose FIRST entry is the protocol. Anything after the protocol belongs
  // to whichever binary owns that fact — `ctxmuxd` names its handoff schema,
  // `ctxmux` names nothing — and the parser must not care.
  assert.deepEqual(parseBinaryVersion("ctxmux", "ctxmux 0.1.0 (protocol 17)"), {
    version: "0.1.0",
    protocol: 17,
  });
  assert.deepEqual(
    parseBinaryVersion(
      "ctxmuxd",
      "ctxmuxd 0.1.0 (protocol 17, handoff ctxmux.daemon-handoff.v4)",
    ),
    { version: "0.1.0", protocol: 17 },
  );
  // A fact appended later must not break it again.
  assert.deepEqual(
    parseBinaryVersion(
      "ctxmuxd",
      "ctxmuxd 2.10.3 (protocol 23, handoff x, more)",
    ),
    { version: "2.10.3", protocol: 23 },
  );
  // Still fails closed on output that names no protocol, or the wrong binary.
  for (const bad of [
    "ctxmuxd 0.1.0",
    "ctxmuxd 0.1.0 (protocol )",
    "ctxmuxd 0.1.0 (handoff ctxmux.daemon-handoff.v4)",
    "ctxmux 0.1.0 (protocol 17)",
  ]) {
    assert.throws(
      () => parseBinaryVersion("ctxmuxd", bad),
      /malformed version identity/u,
      `should reject: ${bad}`,
    );
  }
});

test("the real binaries print an identity the vendor parser accepts", () => {
  // The check above pins the parser to strings written by hand; this one pins it
  // to what the binaries ACTUALLY print. Only the pair catches a drift, because
  // the failure mode was precisely a parser and a printer that disagreed while
  // each looked right on its own.
  const built = spawnSync(
    "cargo",
    [
      "build",
      "--quiet",
      "-p",
      "ctxmux-daemon",
      "--bin",
      "ctxmuxd",
      "-p",
      "ctxmux",
      "--bin",
      "ctxmux",
    ],
    {
      cwd: root,
      encoding: "utf8",
      env: buildEnvironment("0", os.tmpdir(), { root }),
    },
  );
  assert.equal(
    built.status,
    0,
    `real binary build failed: ${built.error?.message ?? ""}\n${built.stderr}\n${built.stdout}`,
  );
  for (const name of ["ctxmux", "ctxmuxd"]) {
    const binary = path.join(root, "target/debug", name);
    const printed = run(binary, ["--version"], root).stdout.trim();
    const parsed = parseBinaryVersion(name, printed);
    assert.equal(
      Number.isSafeInteger(parsed.protocol) && parsed.protocol > 0,
      true,
      `${name} --version must yield a usable protocol: ${printed}`,
    );
  }
});

test("local artifact command binds one clean Git identity and rejects dirty input", () => {
  const directory = fs.mkdtempSync(
    path.join(os.tmpdir(), "ctxmux-artifact-dirty-"),
  );
  try {
    fs.mkdirSync(path.join(directory, "scripts"));
    fs.copyFileSync(
      builder,
      path.join(directory, "scripts/build-local-artifacts.mjs"),
    );
    run("/usr/bin/git", ["init", "--quiet"], directory);
    run("/usr/bin/git", ["config", "user.name", "ctxmux fixture"], directory);
    run(
      "/usr/bin/git",
      ["config", "user.email", "ctxmux-fixture@example.invalid"],
      directory,
    );
    run(
      "/usr/bin/git",
      ["add", "scripts/build-local-artifacts.mjs"],
      directory,
    );
    run("/usr/bin/git", ["commit", "--quiet", "-m", "fixture"], directory);

    const identity = sourceIdentity(directory);
    assert.match(identity.commit, /^[0-9a-f]{40}$/u);
    assert.match(identity.tree, /^[0-9a-f]{40}$/u);
    assert.match(identity.commit_time_unix, /^(0|[1-9][0-9]*)$/u);

    fs.writeFileSync(path.join(directory, "dirty.txt"), "dirty\n");
    assert.throws(
      () => sourceIdentity(directory),
      /artifact source worktree must be clean/u,
    );
    const command = spawnSync(
      process.execPath,
      [
        path.join(directory, "scripts/build-local-artifacts.mjs"),
        path.join(directory, "artifacts"),
      ],
      { cwd: directory, encoding: "utf8" },
    );
    assert.equal(
      command.status,
      1,
      `stdout=${command.stdout} stderr=${command.stderr}`,
    );
    assert.match(command.stderr, /artifact source worktree must be clean/u);
    assert.equal(fs.existsSync(path.join(directory, "artifacts")), false);
  } finally {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});
