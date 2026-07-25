# Contributing

ctxmux is pre-alpha. Prefer one working vertical slice over speculative
abstractions, compatibility layers, or broad plugin machinery.

## Prerequisites

- Rust 1.96 with `rustfmt` and `clippy` (selected by `rust-toolchain.toml`)
- Node.js 24 or newer
- npm 11 or newer
- tmux 3.4 or a released 3.x version for the real adapter suite

## Setup

```bash
npm ci
scripts/check.sh
```

`scripts/check.sh` is the repository-owned quality boundary used locally and in
CI. It formats-checks, analyzes, builds, tests, and runs the initialized public
entrypoints.

Required CI does not treat an unavailable tmux executable as passing evidence.
The Ubuntu 24.04 lane installs tmux and asserts an actual 3.4 server; the macOS
15 lane installs the current package and asserts the selected server version is
a released 3.4-through-3.x version. Those lanes qualify the versions they run,
not every future 3.x release. A local machine without tmux may still run
non-tmux checks, but its skipped real-session tests cannot be retained as tmux
acceptance evidence.

To run the required coverage ratchet locally, install the pinned wrapper and
LLVM tools, then select coverage mode:

```bash
rustup component add llvm-tools-preview
cargo install --locked --version 0.8.7 cargo-llvm-cov
scripts/check.sh --coverage
```

The gate reports the reviewed Rust runtime/client, Rust protocol/codegen,
TypeScript SDK, validator, and changed-line groups from `coverage-policy.json`.
Generated declarations and platform-impossible branches remain explicit rather
than diluting those denominators. CI suite/platform ownership is recorded in
`.github/ci-evidence-map.json` and checked by the same gate.

An ordinary clean-tree run may honestly report that there are no changed
executable product lines. When the result will be retained as changed-line
evidence, require a meaningful base and a non-empty denominator explicitly:

```bash
CTXMUX_COVERAGE_BASE=HEAD \
CTXMUX_COVERAGE_REQUIRE_CHANGED_LINES=true \
scripts/check.sh --coverage
```

CI supplies the pull-request base or prior push revision. A pure documentation
change may still report no executable product lines honestly. Any run retained
as proof of the changed-line ratchet must enable evidence mode; a zero
denominator then fails instead of being promoted into proof.

The default gate also runs the bounded reliability smoke against the frozen
one-Run CPU, RSS, retention, thread, fd, and cleanup budgets. To reproduce the
full resource matrix without a long soak, run:

```bash
scripts/check-reliability.sh --profile nightly --soak-seconds 1
```

The scheduled `nightly` profile uses a real 30-minute soak; explicit `release`
dispatch uses two hours. `observe` bypasses budget assertions only for a new
pre-optimization baseline. Resource-only diagnosis is available through
`--stage resource-census`, `--resource-counts`, `--resource-modes`, and
`--resource-start-concurrency`. Every run writes a structured receipt and
daemon logs under `target/reliability/`. The harness runs inside a supervised
process group; `time_budget_seconds` is a hard kill boundary, not just receipt
metadata. Frozen baseline receipts are checked in under `fixtures/reliability/`
and hash-verified by `scripts/reliability-policy.mjs`.

## Protocol declarations

Rust types in `crates/ctxmux-protocol` are the wire schema source of truth.
After changing them, regenerate the TypeScript declarations:

```bash
scripts/generate-protocol-types.sh
```

To check generated declarations without modifying the working tree:

```bash
scripts/check-protocol-types.sh
```

The full repository check runs the drift check automatically.

## Wrong-case fixtures

`fixtures/wrong-cases.json` is the machine-readable trace from researched
failure cases to architecture choices and executable tests. Validate it with:

```bash
node scripts/check-fixtures.mjs
```

Active and covered cases must name checked-in test anchors. Future and
characterization cases must name an activation owner and reason; do not add an
ignored test as a substitute. `scripts/check.sh` runs the corpus validator and
all current Rust and TypeScript fixtures.

## Before changing behavior

Read `AGENTS.md`, `docs/vision.md`, `docs/architecture.md`, `docs/protocol.md`,
and the active Feature Tracker task. Update the owning document when product,
architecture, or protocol meaning changes.

Do not claim lifecycle behavior from a type or mock. Attach, persistence,
recovery, and fork milestones require integration evidence using real child
processes through the public client boundary.
