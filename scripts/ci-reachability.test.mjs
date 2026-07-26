import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import {
  discoverCriticalTests,
  validateCiReachability,
} from "./ci-reachability.mjs";

const coverageBase =
  "${{ github.event.pull_request.base.sha || github.event.before }}";
const coverageComparison =
  "${{ github.event_name == 'pull_request' && 'merge-base' || 'direct' }}";
const environmentNeutralizationCommand =
  'unset BASH_ENV ENV GIT_DIR GIT_WORK_TREE GIT_COMMON_DIR GIT_INDEX_FILE GIT_OBJECT_DIRECTORY GIT_ALTERNATE_OBJECT_DIRECTORIES GIT_CONFIG_COUNT GIT_CONFIG_PARAMETERS GIT_CONFIG_GLOBAL GIT_CONFIG_SYSTEM GIT_CONFIG_NOSYSTEM "${!GIT_CONFIG_KEY_@}" "${!GIT_CONFIG_VALUE_@}"';
const gitCommand =
  "GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_SYSTEM=/dev/null GIT_CONFIG_NOSYSTEM=1 /usr/bin/git";
const gitStatusCommand = `${gitCommand} -c core.excludesFile=/dev/null -c core.fsmonitor=false -c core.untrackedCache=false status --porcelain --untracked-files=all`;
const eventSha = '"${{ github.sha }}"';
const sourceIdentityCommand =
  environmentNeutralizationCommand +
  ` && test "$(${gitCommand} rev-parse HEAD)" = ${eventSha} && test -z "$(${gitStatusCommand})"`;

function gateRun(command) {
  return `${sourceIdentityCommand} && exec /bin/bash --noprofile --norc ${command}`;
}

const criticalGateRun = gateRun("scripts/check.sh");
const coverageGateRun = gateRun("scripts/check.sh --coverage");
const criticalRunLine = `      - run: ${criticalGateRun}`;
const coverageRunLine = `      - run: ${coverageGateRun}`;

function createFixture(t) {
  const root = fs.mkdtempSync(
    path.join(os.tmpdir(), "ctxmux-ci-reachability-"),
  );
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  for (const directory of ["crates/demo/src", "packages/sdk/test", "scripts"]) {
    fs.mkdirSync(path.join(root, directory), { recursive: true });
  }
  fs.writeFileSync(
    path.join(root, "crates/demo/src/lib.rs"),
    "#[test]\nfn works() {}\n",
  );
  fs.writeFileSync(
    path.join(root, "packages/sdk/test/client.test.ts"),
    'test("works", () => {});\n',
  );
  fs.writeFileSync(
    path.join(root, "scripts/gate.test.mjs"),
    'test("works", () => {});\n',
  );
  fs.writeFileSync(
    path.join(root, "scripts/check.sh"),
    "cargo test --workspace --all-targets\nnode --test scripts/*.test.mjs\n",
  );

  const jobs = [
    {
      id: "critical",
      platforms: ["ubuntu-24.04", "macos-15"],
      command: "scripts/check.sh",
      trigger: "pull_request and push to main",
      required: true,
    },
    {
      id: "coverage",
      platforms: ["ubuntu-24.04"],
      command: "scripts/check.sh --coverage",
      coverage_contract: {
        checkout_fetch_depth: 0,
        base: coverageBase,
        changed_line_mode: "auto",
        comparison_mode: coverageComparison,
      },
      trigger: "pull_request and push to main",
      required: true,
    },
  ];
  const reach = jobs.map(({ id: job, platforms }) => ({ job, platforms }));
  const suites = [
    ["rust", "crates/demo/src/lib.rs", "cargo test --workspace --all-targets"],
    [
      "typescript",
      "packages/sdk/test/client.test.ts",
      "node --test scripts/*.test.mjs",
    ],
    ["scripts", "scripts/gate.test.mjs", "node --test scripts/*.test.mjs"],
  ].map(([id, suitePath, selectionAnchor]) => ({
    id,
    kind: "test",
    path: suitePath,
    invariants: [`${id} invariant`],
    selection_ref: "scripts/check.sh",
    selection_anchor: selectionAnchor,
    reach,
  }));
  const map = {
    schema: "ctxmux.ci-evidence-map.v1",
    jobs,
    suites,
    non_required_evidence: {
      skipped: [],
      ignored: [],
      conditional: [],
      schedule_only: [],
    },
  };
  const workflow = `on:
  pull_request:
  push:
    branches: [main]
jobs:
  critical:
    strategy:
      matrix:
        include:
          - os: ubuntu-24.04
            tmux_lane: minimum-3.4
          - os: macos-15
            tmux_lane: current
    runs-on: \${{ matrix.os }}
    env:
      CTXMUX_FUZZ_CASES: "512"
      CTXMUX_MODEL_CASES: "8"
      CTXMUX_REQUIRE_TMUX: "1"
      CTXMUX_TMUX_BIN: tmux
      CTXMUX_TMUX_QUALIFICATION: \${{ matrix.tmux_lane }}
    steps:
      - uses: actions/checkout@v4
      - run: ${criticalGateRun}
        env:
          BASH_ENV: /dev/null
          CTXMUX_FUZZ_CASES: "512"
          CTXMUX_MODEL_CASES: "8"
          CTXMUX_REQUIRE_TMUX: "1"
          CTXMUX_TMUX_BIN: tmux
          CTXMUX_TMUX_QUALIFICATION: \${{ matrix.tmux_lane }}
          ENV: /dev/null
  coverage:
    runs-on: ubuntu-24.04
    env:
      CTXMUX_FUZZ_CASES: "512"
      CTXMUX_MODEL_CASES: "8"
      CTXMUX_REQUIRE_TMUX: "1"
      CTXMUX_TMUX_BIN: tmux
      CTXMUX_TMUX_QUALIFICATION: minimum-3.4
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0
      - run: ${coverageGateRun}
        env:
          BASH_ENV: /dev/null
          CTXMUX_COVERAGE_BASE: \${{ github.event.pull_request.base.sha || github.event.before }}
          CTXMUX_COVERAGE_CHANGED_LINE_MODE: auto
          CTXMUX_COVERAGE_COMPARISON_MODE: \${{ github.event_name == 'pull_request' && 'merge-base' || 'direct' }}
          CTXMUX_FUZZ_CASES: "512"
          CTXMUX_MODEL_CASES: "8"
          CTXMUX_REQUIRE_TMUX: "1"
          CTXMUX_TMUX_BIN: tmux
          CTXMUX_TMUX_QUALIFICATION: minimum-3.4
          ENV: /dev/null
`;
  return { root, map, workflow };
}

test("discovers every checked-in Rust, TypeScript, and script test surface", (t) => {
  const { root } = createFixture(t);
  assert.deepEqual([...discoverCriticalTests(root)].sort(), [
    "crates/demo/src/lib.rs",
    "packages/sdk/test/client.test.ts",
    "scripts/gate.test.mjs",
  ]);
});

test("accepts complete required-job, platform, invariant, and selection reach", (t) => {
  const fixture = createFixture(t);
  assert.deepEqual(validateCiReachability(fixture), []);
});

test("canonical final steps override persisted shell startup paths", (t) => {
  const fixture = createFixture(t);
  const poison =
    '      - run: echo "BASH_ENV=/tmp/ctxmux-startup-poison" >> "$GITHUB_ENV"';
  fixture.workflow = fixture.workflow.replace(
    criticalRunLine,
    `${poison}\n${criticalRunLine}`,
  );
  fixture.workflow = fixture.workflow.replace(
    coverageRunLine,
    `${poison}\n${coverageRunLine}`,
  );

  assert.deepEqual(validateCiReachability(fixture), []);
});

test("neutralized final step blocks delayed Bash startup mutation", (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "ctxmux-bash-startup-"));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const hook = path.join(root, "bash-env.sh");
  const marker = path.join(root, "first-shell-complete");
  const mutation = path.join(root, "mutation-observed");
  fs.writeFileSync(
    hook,
    `if [[ -e "$CTXMUX_STARTUP_MARKER" ]]; then
  export CTXMUX_FUZZ_CASES=1
  : > "$CTXMUX_STARTUP_MUTATION"
else
  : > "$CTXMUX_STARTUP_MARKER"
fi
`,
  );
  const poisonedEnvironment = {
    ...process.env,
    BASH_ENV: hook,
    CTXMUX_FUZZ_CASES: "512",
    CTXMUX_STARTUP_MARKER: marker,
    CTXMUX_STARTUP_MUTATION: mutation,
  };
  const observeDepth = 'printf %s "$CTXMUX_FUZZ_CASES"';
  const finalStepBody = `${environmentNeutralizationCommand} && exec /bin/bash --noprofile --norc -c 'printf %s "$CTXMUX_FUZZ_CASES"'`;
  const bash = (body, environment) =>
    execFileSync("/bin/bash", ["-e", "-c", body], {
      encoding: "utf8",
      env: environment,
      stdio: ["ignore", "pipe", "pipe"],
    });

  assert.equal(bash(observeDepth, poisonedEnvironment), "512");
  assert.equal(fs.existsSync(mutation), false);
  assert.equal(bash(observeDepth, poisonedEnvironment), "1");
  assert.equal(fs.existsSync(mutation), true);

  fs.rmSync(mutation);
  assert.equal(bash(finalStepBody, poisonedEnvironment), "1");
  assert.equal(fs.existsSync(mutation), true);

  fs.rmSync(mutation);
  const neutralizedEnvironment = {
    ...poisonedEnvironment,
    BASH_ENV: "/dev/null",
    ENV: "/dev/null",
    CTXMUX_FUZZ_CASES: "512",
  };
  assert.equal(bash(finalStepBody, neutralizedEnvironment), "512");
  assert.equal(fs.existsSync(mutation), false);
});

test("neutralized final step restores Git status hidden by config environment", (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "ctxmux-git-config-"));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const repo = path.join(root, "repo");
  const excludes = path.join(root, "ignore-all");
  fs.mkdirSync(repo);
  fs.writeFileSync(excludes, "*\n");
  fs.writeFileSync(path.join(repo, "untracked.txt"), "must remain visible\n");
  execFileSync("/usr/bin/git", ["init", "--quiet"], { cwd: repo });
  const statusArguments = ["status", "--porcelain", "--untracked-files=all"];
  const status = (environment) =>
    execFileSync("/usr/bin/git", statusArguments, {
      cwd: repo,
      encoding: "utf8",
      env: environment,
      stdio: ["ignore", "pipe", "pipe"],
    });
  const ordinary = status(process.env);
  assert.match(ordinary, /^\?\? untracked\.txt$/mu);

  const poisonedEnvironment = {
    ...process.env,
    GIT_CONFIG_COUNT: "1",
    GIT_CONFIG_KEY_0: "core.excludesFile",
    GIT_CONFIG_KEY_17: "unused.extra.key",
    GIT_CONFIG_VALUE_0: excludes,
    GIT_CONFIG_VALUE_17: "unused",
  };
  assert.equal(status(poisonedEnvironment), "");

  const nestedStatusProbe =
    environmentNeutralizationCommand +
    " && exec /bin/bash --noprofile --norc -c 'test -z \"${GIT_CONFIG_COUNT+x}${GIT_CONFIG_KEY_0+x}${GIT_CONFIG_KEY_17+x}${GIT_CONFIG_VALUE_0+x}${GIT_CONFIG_VALUE_17+x}\" && /usr/bin/git status --porcelain --untracked-files=all'";
  const restored = execFileSync(
    "/bin/bash",
    ["--noprofile", "--norc", "-e", "-c", nestedStatusProbe],
    {
      cwd: repo,
      encoding: "utf8",
      env: poisonedEnvironment,
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
  assert.equal(restored, ordinary);
});

test("safe identity Git ignores HOME and XDG global excludes", (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "ctxmux-git-home-"));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const repo = path.join(root, "repo");
  const excludes = path.join(root, "ignore-all");
  const cleanHome = path.join(root, "clean-home");
  const cleanXdg = path.join(root, "clean-xdg");
  const poisonedHome = path.join(root, "poisoned-home");
  const poisonedXdg = path.join(root, "poisoned-xdg");
  for (const directory of [
    repo,
    cleanHome,
    cleanXdg,
    poisonedHome,
    path.join(poisonedXdg, "git"),
  ]) {
    fs.mkdirSync(directory, { recursive: true });
  }
  fs.writeFileSync(excludes, "*\n");
  const globalConfig = `[core]\n\texcludesFile = ${excludes}\n`;
  fs.writeFileSync(path.join(poisonedHome, ".gitconfig"), globalConfig);
  fs.writeFileSync(path.join(poisonedXdg, "git", "config"), globalConfig);
  fs.writeFileSync(path.join(repo, "untracked.txt"), "must remain visible\n");
  execFileSync("/usr/bin/git", ["init", "--quiet"], { cwd: repo });
  const cleanEnvironment = {
    ...process.env,
    HOME: cleanHome,
    XDG_CONFIG_HOME: cleanXdg,
  };
  const runStatus = (body, environment) =>
    execFileSync("/bin/bash", ["--noprofile", "--norc", "-e", "-c", body], {
      cwd: repo,
      encoding: "utf8",
      env: environment,
      stdio: ["ignore", "pipe", "pipe"],
    });
  const ordinary = runStatus(
    "/usr/bin/git status --porcelain --untracked-files=all",
    cleanEnvironment,
  );
  assert.match(ordinary, /^\?\? untracked\.txt$/mu);

  const cases = [
    {
      label: "HOME/.gitconfig",
      environment: { ...cleanEnvironment, HOME: poisonedHome },
    },
    {
      label: "XDG_CONFIG_HOME/git/config",
      environment: {
        ...cleanEnvironment,
        XDG_CONFIG_HOME: poisonedXdg,
      },
    },
  ];
  for (const fixtureCase of cases) {
    assert.equal(
      runStatus(
        "/usr/bin/git status --porcelain --untracked-files=all",
        fixtureCase.environment,
      ),
      "",
      fixtureCase.label,
    );
    assert.equal(
      runStatus(
        `${environmentNeutralizationCommand} && /usr/bin/git status --porcelain --untracked-files=all`,
        fixtureCase.environment,
      ),
      "",
      `${fixtureCase.label} bypasses unset-only neutralization`,
    );
    assert.equal(
      runStatus(
        `${environmentNeutralizationCommand} && ${gitStatusCommand}`,
        fixtureCase.environment,
      ),
      ordinary,
      `${fixtureCase.label} is ignored by safe identity Git`,
    );
  }
});

test("rejects unmapped and skipped critical tests", (t) => {
  const fixture = createFixture(t);
  fixture.map.suites = fixture.map.suites.filter(
    ({ id }) => id !== "typescript",
  );
  fs.writeFileSync(
    path.join(fixture.root, "scripts/gate.test.mjs"),
    'test.skip("hidden", () => {});\n',
  );
  const errors = validateCiReachability(fixture);
  assert.ok(
    errors.some((error) => error.includes("has no job-to-invariant mapping")),
  );
  assert.ok(errors.some((error) => error.includes("skipped or todo evidence")));
});

test("rejects workflow and selection drift instead of trusting map prose", (t) => {
  const fixture = createFixture(t);
  fixture.map.suites[0].selection_anchor = "cargo nextest run";
  fixture.workflow = fixture.workflow.replace("macos-15", "windows-latest");
  const errors = validateCiReachability(fixture);
  assert.ok(errors.some((error) => error.includes("does not reach macos-15")));
  assert.ok(
    errors.some((error) => error.includes("selection anchor is unreachable")),
  );
});

test("rejects unreachable or weakened coverage comparison reach", (t) => {
  const cases = [
    {
      label: "missing map contract",
      mutate(fixture) {
        delete fixture.map.jobs.find(({ id }) => id === "coverage")
          .coverage_contract;
      },
      expected: "has no coverage contract",
    },
    {
      label: "map cannot erase a canonical required job",
      mutate(fixture) {
        fixture.map.jobs = fixture.map.jobs.filter(
          ({ id }) => id !== "coverage",
        );
        for (const suite of fixture.map.suites) {
          suite.reach = suite.reach.filter(({ job }) => job !== "coverage");
        }
      },
      expected: "CI evidence map is missing required job coverage",
    },
    {
      label: "map suites and workflow cannot erase the macOS critical lane",
      mutate(fixture) {
        fixture.map.jobs.find(({ id }) => id === "critical").platforms = [
          "ubuntu-24.04",
        ];
        for (const suite of fixture.map.suites) {
          suite.reach.find(({ job }) => job === "critical").platforms = [
            "ubuntu-24.04",
          ];
        }
        fixture.workflow = fixture.workflow.replace(
          "os: [ubuntu-24.04, macos-15]",
          "os: [ubuntu-24.04]",
        );
      },
      expected:
        'mapped required job critical must declare canonical platforms ["ubuntu-24.04","macos-15"]',
    },
    {
      label: "matrix exclude cannot erase a declared macOS lane",
      mutate(fixture) {
        fixture.workflow = fixture.workflow.replace(
          `          - os: macos-15
            tmux_lane: current`,
          `          - os: macos-15
            tmux_lane: current
        exclude:
          - os: macos-15`,
        );
      },
      expected: "must not exclude matrix lanes",
    },
    {
      label: "coverage platform cannot be collaboratively replaced",
      mutate(fixture) {
        fixture.map.jobs.find(({ id }) => id === "coverage").platforms = [
          "macos-15",
        ];
        for (const suite of fixture.map.suites) {
          suite.reach.find(({ job }) => job === "coverage").platforms = [
            "macos-15",
          ];
        }
        fixture.workflow = fixture.workflow.replace(
          "  coverage:\n    runs-on: ubuntu-24.04",
          "  coverage:\n    runs-on: macos-15",
        );
      },
      expected:
        'mapped required job coverage must declare canonical platforms ["ubuntu-24.04"]',
    },
    {
      label: "shallow checkout",
      mutate(fixture) {
        fixture.workflow = fixture.workflow.replace(
          "fetch-depth: 0",
          "fetch-depth: 1",
        );
      },
      expected: "must have one prior unconditional full-history checkout",
    },
    {
      label: "coverage checkout cannot select another ref",
      mutate(fixture) {
        fixture.workflow = fixture.workflow.replace(
          "          fetch-depth: 0",
          "          fetch-depth: 0\n          ref: ${{ github.event.pull_request.base.sha }}",
        );
      },
      expected: "must have one prior unconditional full-history checkout",
    },
    {
      label: "coverage checkout cannot select another repository",
      mutate(fixture) {
        fixture.workflow = fixture.workflow.replace(
          "          fetch-depth: 0",
          "          fetch-depth: 0\n          repository: owner/decoy",
        );
      },
      expected: "must have one prior unconditional full-history checkout",
    },
    {
      label: "coverage checkout cannot move the source root",
      mutate(fixture) {
        fixture.workflow = fixture.workflow.replace(
          "          fetch-depth: 0",
          "          fetch-depth: 0\n          path: decoy",
        );
      },
      expected: "must have one prior unconditional full-history checkout",
    },
    {
      label: "source identity fence and Gate cannot be split across steps",
      mutate(fixture) {
        fixture.workflow = fixture.workflow.replace(
          coverageRunLine,
          `      - run: ${sourceIdentityCommand}
      - run: exec /bin/bash --noprofile --norc scripts/check.sh --coverage`,
        );
      },
      expected:
        "must run its exact startup-neutralized source fence and Gate as the final step",
    },
    {
      label: "critical source identity fence and Gate cannot be split",
      mutate(fixture) {
        fixture.workflow = fixture.workflow.replace(
          criticalRunLine,
          `      - run: ${sourceIdentityCommand}
      - run: exec /bin/bash --noprofile --norc scripts/check.sh`,
        );
      },
      expected:
        "required workflow job critical must run its exact startup-neutralized source fence and Gate as the final step",
    },
    {
      label: "source identity fence must check both HEAD and worktree",
      mutate(fixture) {
        fixture.workflow = fixture.workflow.replace(
          coverageRunLine,
          `      - run: ${coverageGateRun.replace(
            ` && test -z "$(${gitStatusCommand})"`,
            "",
          )}`,
        );
      },
      expected: "does not run scripts/check.sh --coverage",
    },
    {
      label: "critical Gate requires BASH_ENV at step scope",
      mutate(fixture) {
        fixture.workflow = fixture.workflow.replace(
          `${criticalRunLine}\n        env:\n          BASH_ENV: /dev/null\n`,
          `${criticalRunLine}\n        env:\n`,
        );
      },
      expected:
        "required workflow job critical must bind the canonical Gate environment to its command step",
    },
    {
      label: "critical Gate rejects a changed ENV startup path",
      mutate(fixture) {
        fixture.workflow = fixture.workflow.replace(
          "          CTXMUX_TMUX_QUALIFICATION: ${{ matrix.tmux_lane }}\n          ENV: /dev/null",
          "          CTXMUX_TMUX_QUALIFICATION: ${{ matrix.tmux_lane }}\n          ENV: /tmp/poison",
        );
      },
      expected:
        "required workflow job critical must bind the canonical Gate environment to its command step",
    },
    {
      label: "coverage Gate rejects a changed BASH_ENV startup path",
      mutate(fixture) {
        fixture.workflow = fixture.workflow.replace(
          `${coverageRunLine}\n        env:\n          BASH_ENV: /dev/null`,
          `${coverageRunLine}\n        env:\n          BASH_ENV: /tmp/poison`,
        );
      },
      expected:
        "required workflow job coverage must bind the canonical Gate environment to its command step",
    },
    {
      label: "coverage Gate requires ENV at step scope",
      mutate(fixture) {
        fixture.workflow = fixture.workflow.replace(
          "          CTXMUX_TMUX_QUALIFICATION: minimum-3.4\n          ENV: /dev/null\n",
          "          CTXMUX_TMUX_QUALIFICATION: minimum-3.4\n",
        );
      },
      expected:
        "required workflow job coverage must bind the canonical Gate environment to its command step",
    },
    {
      label: "source identity fence must unset Git redirection variables",
      mutate(fixture) {
        fixture.workflow = fixture.workflow.replace(
          coverageRunLine,
          `      - run: ${coverageGateRun.replace(" GIT_COMMON_DIR", "")}`,
        );
      },
      expected: "does not run scripts/check.sh --coverage",
    },
    {
      label: "source identity fence must unset GIT_CONFIG_COUNT",
      mutate(fixture) {
        fixture.workflow = fixture.workflow.replace(
          coverageRunLine,
          `      - run: ${coverageGateRun.replace(" GIT_CONFIG_COUNT", "")}`,
        );
      },
      expected: "does not run scripts/check.sh --coverage",
    },
    {
      label: "source identity fence must unset GIT_CONFIG_PARAMETERS",
      mutate(fixture) {
        fixture.workflow = fixture.workflow.replace(
          coverageRunLine,
          `      - run: ${coverageGateRun.replace(
            " GIT_CONFIG_PARAMETERS",
            "",
          )}`,
        );
      },
      expected: "does not run scripts/check.sh --coverage",
    },
    ...[
      ["global config path", "GIT_CONFIG_GLOBAL=/dev/null "],
      ["system config path", "GIT_CONFIG_SYSTEM=/dev/null "],
      ["system config suppression", "GIT_CONFIG_NOSYSTEM=1 "],
      ["excludes override", "-c core.excludesFile=/dev/null "],
      ["fsmonitor override", "-c core.fsmonitor=false "],
      ["untracked-cache override", "-c core.untrackedCache=false "],
    ].map(([label, binding]) => ({
      label: `safe identity Git requires its ${label}`,
      mutate(fixture) {
        fixture.workflow = fixture.workflow.replace(
          coverageRunLine,
          `      - run: ${coverageGateRun.replace(binding, "")}`,
        );
      },
      expected: "does not run scripts/check.sh --coverage",
    })),
    {
      label: "source identity fence must use platform Git",
      mutate(fixture) {
        fixture.workflow = fixture.workflow.replace(
          coverageRunLine,
          `      - run: ${coverageGateRun.replaceAll("/usr/bin/git", "git")}`,
        );
      },
      expected: "does not run scripts/check.sh --coverage",
    },
    {
      label: "source identity fence must use the workflow event SHA",
      mutate(fixture) {
        fixture.workflow = fixture.workflow.replace(
          coverageRunLine,
          `      - run: ${coverageGateRun.replace(
            '"${{ github.sha }}"',
            '"$GITHUB_SHA"',
          )}`,
        );
      },
      expected: "does not run scripts/check.sh --coverage",
    },
    {
      label: "required Gate must remain the final workflow step",
      mutate(fixture) {
        fixture.workflow += "      - run: echo after-gate\n";
      },
      expected:
        "must run its exact startup-neutralized source fence and Gate as the final step",
    },
    {
      label: "repository helper cannot self-verify the required Gate",
      mutate(fixture) {
        fixture.workflow = fixture.workflow.replace(
          coverageRunLine,
          "      - run: scripts/ci-gate-helper.sh --coverage",
        );
      },
      expected: "does not run scripts/check.sh --coverage",
    },
    {
      label: "repository helper cannot self-verify the critical Gate",
      mutate(fixture) {
        fixture.workflow = fixture.workflow.replace(
          criticalRunLine,
          "      - run: scripts/ci-gate-helper.sh",
        );
      },
      expected: "workflow job critical does not run scripts/check.sh",
    },
    {
      label: "critical matrix cannot weaken the minimum tmux lane",
      mutate(fixture) {
        fixture.workflow = fixture.workflow.replace(
          "tmux_lane: minimum-3.4",
          "tmux_lane: optional",
        );
      },
      expected: "must bind canonical tmux qualification lanes",
    },
    {
      label: "job environment cannot make real tmux optional",
      mutate(fixture) {
        fixture.workflow = fixture.workflow.replace(
          "      CTXMUX_TMUX_QUALIFICATION: ${{ matrix.tmux_lane }}",
          "      CTXMUX_TMUX_QUALIFICATION: optional",
        );
      },
      expected: "must bind the canonical Gate environment at job scope",
    },
    {
      label: "command environment cannot disable required tmux",
      mutate(fixture) {
        fixture.workflow = fixture.workflow.replace(
          `${coverageRunLine}
        env:
          BASH_ENV: /dev/null
          CTXMUX_COVERAGE_BASE: \${{ github.event.pull_request.base.sha || github.event.before }}
          CTXMUX_COVERAGE_CHANGED_LINE_MODE: auto
          CTXMUX_COVERAGE_COMPARISON_MODE: \${{ github.event_name == 'pull_request' && 'merge-base' || 'direct' }}
          CTXMUX_FUZZ_CASES: "512"
          CTXMUX_MODEL_CASES: "8"
          CTXMUX_REQUIRE_TMUX: "1"
          CTXMUX_TMUX_BIN: tmux
          CTXMUX_TMUX_QUALIFICATION: minimum-3.4
          ENV: /dev/null`,
          `${coverageRunLine}
        env:
          BASH_ENV: /dev/null
          CTXMUX_COVERAGE_BASE: \${{ github.event.pull_request.base.sha || github.event.before }}
          CTXMUX_COVERAGE_CHANGED_LINE_MODE: auto
          CTXMUX_COVERAGE_COMPARISON_MODE: \${{ github.event_name == 'pull_request' && 'merge-base' || 'direct' }}
          CTXMUX_FUZZ_CASES: "512"
          CTXMUX_MODEL_CASES: "8"
          CTXMUX_REQUIRE_TMUX: "0"
          CTXMUX_TMUX_BIN: missing-tmux
          CTXMUX_TMUX_QUALIFICATION: optional
          ENV: /dev/null`,
        );
      },
      expected: "must bind the canonical Gate environment to its command step",
    },
    {
      label: "command environment cannot reduce fuzz and model depth",
      mutate(fixture) {
        fixture.workflow = fixture.workflow.replace(
          `${criticalRunLine}
        env:
          BASH_ENV: /dev/null
          CTXMUX_FUZZ_CASES: "512"
          CTXMUX_MODEL_CASES: "8"
          CTXMUX_REQUIRE_TMUX: "1"
          CTXMUX_TMUX_BIN: tmux
          CTXMUX_TMUX_QUALIFICATION: \${{ matrix.tmux_lane }}
          ENV: /dev/null`,
          `${criticalRunLine}
        env:
          BASH_ENV: /dev/null
          CTXMUX_FUZZ_CASES: "1"
          CTXMUX_MODEL_CASES: "1"
          CTXMUX_REQUIRE_TMUX: "1"
          CTXMUX_TMUX_BIN: tmux
          CTXMUX_TMUX_QUALIFICATION: \${{ matrix.tmux_lane }}
          ENV: /dev/null`,
        );
      },
      expected: "must bind the canonical Gate environment to its command step",
    },
    {
      label: "wrong event base",
      mutate(fixture) {
        fixture.workflow = fixture.workflow.replace(
          "github.event.pull_request.base.sha || github.event.before",
          "github.sha",
        );
      },
      expected: "does not bind the pull-request or push coverage base",
    },
    {
      label: "map and workflow agree on the wrong event base",
      mutate(fixture) {
        fixture.map.jobs.find(
          ({ id }) => id === "coverage",
        ).coverage_contract.base = "${{ github.sha }}";
        fixture.workflow = fixture.workflow.replace(
          coverageBase,
          "${{ github.sha }}",
        );
      },
      expected: "does not bind the pull-request or push coverage base",
    },
    {
      label: "ordinary mode in CI",
      mutate(fixture) {
        fixture.workflow = fixture.workflow.replace(
          "CTXMUX_COVERAGE_CHANGED_LINE_MODE: auto",
          "CTXMUX_COVERAGE_CHANGED_LINE_MODE: false",
        );
      },
      expected: "does not enable changed-line auto mode",
    },
    {
      label: "one comparison mode for every event",
      mutate(fixture) {
        fixture.workflow = fixture.workflow.replace(
          coverageComparison,
          "direct",
        );
      },
      expected: "does not select event-specific comparison semantics",
    },
    {
      label: "map and workflow agree on one comparison mode",
      mutate(fixture) {
        fixture.map.jobs.find(
          ({ id }) => id === "coverage",
        ).coverage_contract.comparison_mode = "direct";
        fixture.workflow = fixture.workflow.replace(
          coverageComparison,
          "direct",
        );
      },
      expected: "does not select event-specific comparison semantics",
    },
    {
      label: "job-level condition",
      mutate(fixture) {
        fixture.workflow = fixture.workflow.replace(
          "  coverage:\n",
          "  coverage:\n    if: false\n",
        );
      },
      expected: "must not have a job-level condition",
    },
    {
      label: "quoted job-level condition",
      mutate(fixture) {
        fixture.workflow = fixture.workflow.replace(
          "  coverage:\n",
          '  coverage:\n    "if": false\n',
        );
      },
      expected: "must not have a job-level condition",
    },
    {
      label: "required job cannot inherit a skipped dependency",
      mutate(fixture) {
        fixture.workflow = fixture.workflow.replace(
          "jobs:\n",
          `jobs:
  prep:
    if: false
    runs-on: ubuntu-24.04
    steps:
      - run: echo skipped
`,
        );
        fixture.workflow = fixture.workflow.replace(
          "  coverage:\n",
          "  coverage:\n    needs: prep\n",
        );
      },
      expected: "must not depend on other jobs",
    },
    {
      label: "Unicode-escaped job condition key is normalized",
      mutate(fixture) {
        fixture.workflow = fixture.workflow.replace(
          "  coverage:\n",
          '  coverage:\n    "\\u0069f": false\n',
        );
      },
      expected: "must not have a job-level condition",
    },
    {
      label: "coverage command condition",
      mutate(fixture) {
        fixture.workflow = fixture.workflow.replace(
          coverageRunLine,
          `      - if: false
        run: ${coverageGateRun}`,
        );
      },
      expected: "must not conditionally skip its command",
    },
    {
      label: "quoted coverage command condition",
      mutate(fixture) {
        fixture.workflow = fixture.workflow.replace(
          coverageRunLine,
          `      - "if": false
        run: ${coverageGateRun}`,
        );
      },
      expected: "must not conditionally skip its command",
    },
    {
      label: "coverage prose and checkout env do not prove execution",
      mutate(fixture) {
        fixture.workflow = fixture.workflow.replace(
          coverageRunLine,
          "      - name: scripts/check.sh --coverage\n        run: echo coverage-not-executed",
        );
      },
      expected: "does not run scripts/check.sh --coverage",
    },
    {
      label: "map and workflow cannot replace the canonical coverage command",
      mutate(fixture) {
        fixture.map.jobs.find(({ id }) => id === "coverage").command =
          "echo coverage";
        fixture.workflow = fixture.workflow.replace(
          coverageRunLine,
          `      - run: ${gateRun("echo coverage")}`,
        );
      },
      expected:
        'mapped required job coverage must run canonical command "scripts/check.sh --coverage"',
    },
    {
      label: "coverage command cannot select a custom shell",
      mutate(fixture) {
        fixture.workflow = fixture.workflow.replace(
          coverageRunLine,
          `${coverageRunLine}
        shell: bash {0}`,
        );
      },
      expected: "must use default shell and working directory",
    },
    {
      label: "Unicode-escaped shell key is normalized",
      mutate(fixture) {
        fixture.workflow = fixture.workflow.replace(
          coverageRunLine,
          `${coverageRunLine}
        "\\u0073hell": bash {0}`,
        );
      },
      expected: "must use default shell and working directory",
    },
    {
      label: "coverage command cannot select a working directory",
      mutate(fixture) {
        fixture.workflow = fixture.workflow.replace(
          coverageRunLine,
          `${coverageRunLine}
        working-directory: scripts`,
        );
      },
      expected: "must use default shell and working directory",
    },
    {
      label: "protected coverage env must stay on the command step",
      mutate(fixture) {
        const stepEnvironment = `          CTXMUX_COVERAGE_BASE: \${{ github.event.pull_request.base.sha || github.event.before }}
          CTXMUX_COVERAGE_CHANGED_LINE_MODE: auto
          CTXMUX_COVERAGE_COMPARISON_MODE: \${{ github.event_name == 'pull_request' && 'merge-base' || 'direct' }}
`;
        fixture.workflow = fixture.workflow.replace(stepEnvironment, "");
        fixture.workflow = fixture.workflow.replace(
          "      CTXMUX_TMUX_QUALIFICATION: minimum-3.4\n    steps:",
          `      CTXMUX_TMUX_QUALIFICATION: minimum-3.4
${stepEnvironment.replaceAll("          ", "      ")}    steps:`,
        );
      },
      expected: "does not bind the pull-request or push coverage base",
    },
    {
      label: "full-history checkout after the command is only a decoy",
      mutate(fixture) {
        fixture.workflow = fixture.workflow.replace(
          "fetch-depth: 0",
          "fetch-depth: 1",
        );
        const marker = "          CTXMUX_TMUX_QUALIFICATION: minimum-3.4\n";
        const insertion = fixture.workflow.lastIndexOf(marker) + marker.length;
        fixture.workflow = `${fixture.workflow.slice(0, insertion)}      - uses: actions/checkout@v4
        with:
          fetch-depth: 0
${fixture.workflow.slice(insertion)}`;
      },
      expected: "must have one prior unconditional full-history checkout",
    },
    {
      label: "map and workflow cannot agree on a release-only trigger",
      mutate(fixture) {
        for (const job of fixture.map.jobs) {
          job.trigger = "pull_request and push to release";
        }
        fixture.workflow = fixture.workflow.replace(
          "    branches: [main]",
          "    branches: [release]",
        );
      },
      expected: "workflow push trigger must target only main",
    },
    {
      label: "main push cannot be narrowed by paths",
      mutate(fixture) {
        fixture.workflow = fixture.workflow.replace(
          "    branches: [main]",
          "    branches: [main]\n    paths: [packages/**]",
        );
      },
      expected: "workflow push trigger must target only main",
    },
    {
      label: "main push cannot be narrowed by paths-ignore",
      mutate(fixture) {
        fixture.workflow = fixture.workflow.replace(
          "    branches: [main]",
          "    branches: [main]\n    paths-ignore: [docs/**]",
        );
      },
      expected: "workflow push trigger must target only main",
    },
    {
      label: "required map job cannot weaken its trigger prose",
      mutate(fixture) {
        fixture.map.jobs.find(({ id }) => id === "coverage").trigger =
          "pull_request and push to release";
      },
      expected:
        "mapped required job coverage must declare pull_request and push to main",
    },
    {
      label: "required command cannot continue on error",
      mutate(fixture) {
        fixture.workflow = fixture.workflow.replace(
          coverageRunLine,
          `${coverageRunLine}
        continue-on-error: true`,
        );
      },
      expected: "must not continue on error",
    },
    {
      label: "quoted continue-on-error key cannot wash the command green",
      mutate(fixture) {
        fixture.workflow = fixture.workflow.replace(
          coverageRunLine,
          `${coverageRunLine}
        "continue-on-error": true`,
        );
      },
      expected: "must not continue on error",
    },
    {
      label: "Unicode-escaped continue-on-error key is normalized",
      mutate(fixture) {
        fixture.workflow = fixture.workflow.replace(
          coverageRunLine,
          `${coverageRunLine}
        "continue-on-\\u0065rror": false`,
        );
      },
      expected: "must not continue on error",
    },
    {
      label: "dynamic continue-on-error cannot wash the command green",
      mutate(fixture) {
        fixture.workflow = fixture.workflow.replace(
          coverageRunLine,
          `${coverageRunLine}
        continue-on-error: \${{ true }}`,
        );
      },
      expected: "must not continue on error",
    },
    {
      label: "quoted job continue-on-error key is rejected",
      mutate(fixture) {
        fixture.workflow = fixture.workflow.replace(
          "  coverage:\n",
          '  coverage:\n    "continue-on-error": false\n',
        );
      },
      expected: "must not continue on error",
    },
    {
      label: "duplicate YAML keys fail before reachability evaluation",
      mutate(fixture) {
        fixture.workflow = fixture.workflow.replace(
          coverageRunLine,
          `${coverageRunLine}
        run: echo duplicate`,
        );
      },
      expected: "workflow YAML parse failed",
    },
    {
      label: "YAML aliases are rejected",
      mutate(fixture) {
        fixture.workflow = fixture.workflow.replace(
          "        env:\n          CTXMUX_COVERAGE_BASE:",
          "        env: &coverage_env\n          CTXMUX_COVERAGE_BASE:",
        );
        fixture.workflow = fixture.workflow.replace(
          "      - uses: actions/checkout@v4\n        with:",
          "      - uses: actions/checkout@v4\n        env: *coverage_env\n        with:",
        );
      },
      expected: "workflow YAML must not contain aliases",
    },
    {
      label: "YAML merge keys are rejected",
      mutate(fixture) {
        fixture.workflow = fixture.workflow.replace(
          "  coverage:\n",
          "  coverage:\n    <<: {timeout-minutes: 30}\n",
        );
      },
      expected: "workflow YAML must not contain merge keys",
    },
  ];

  for (const fixtureCase of cases) {
    const fixture = createFixture(t);
    fixtureCase.mutate(fixture);
    const errors = validateCiReachability(fixture);
    assert.ok(
      errors.some((error) => error.includes(fixtureCase.expected)),
      `${fixtureCase.label}: ${errors.join("; ")}`,
    );
  }
});
