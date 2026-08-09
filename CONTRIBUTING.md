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

The gate reports seven reviewed owners from `coverage-policy.json`: Rust
runtime/clients, persistence, tmux, RunSpec validation, protocol/codegen, the
hand-written TypeScript SDK, and TypeScript protocol validation. Runtime owners
hold the ordinary 85% floor and pure validators hold 95%. Generated declarations
and platform-impossible branches remain explicit rather than diluting those
denominators. CI suite/platform ownership is recorded in
`.github/ci-evidence-map.json` and checked by the same gate.

An ordinary clean-tree run may honestly report that there are no changed
executable product lines. When the result will be retained as changed-line
evidence, require a meaningful base and a non-empty denominator explicitly:

```bash
BASE_COMMIT=HEAD^
CTXMUX_COVERAGE_BASE="$BASE_COMMIT" \
CTXMUX_COVERAGE_CHANGED_LINE_MODE=true \
CTXMUX_COVERAGE_COMPARISON_MODE=direct \
scripts/check.sh --coverage
```

Changed-line mode accepts `false`, `true`, or `auto`. `false` is ordinary
reporting. `true` is explicit retained evidence and fails on a zero executable
denominator. Required CI uses `auto`: a nonzero executable denominator must meet
the 90% floor, while documentation-only, comment-only, or deletion-only changes
report changed-line coverage as N/A. N/A is not retained changed-line proof;
filesystem inventory and every owner floor still run and may fail the Gate. CI
supplies the pull-request base with merge-base comparison or the prior push
revision with direct comparison. A run retained as changed-line proof must use
an explicit base plus `true` and `direct`, as above. `HEAD^` makes the example
executable for a one-commit comparison. Retained evidence for a larger change
must replace it with that work's actual pre-change revision. The base must
resolve in the current repository and must not be `HEAD`; the evidence policy
rejects `HEAD` as a zero-distance base. A direct evidence base must also be an
ancestor of `HEAD`; future or unrelated commits fail closed. New untracked
product sources are counted from all executable lines reported for that file
rather than disappearing from the changed-line denominator.

The default gate also runs the bounded reliability smoke against the frozen
one-Run CPU, RSS, retention, thread, fd, and cleanup budgets. To reproduce the
full resource matrix without a long soak, run:

```bash
scripts/check-reliability.sh --profile nightly
```

The scheduled `nightly` profile uses a real 30-minute soak; explicit `release`
dispatch uses two hours. `observe` bypasses budget assertions only for a new
pre-optimization baseline. Smoke may use focused `--stage resource-census`,
`--resource-counts`,
`--resource-modes`, and `--resource-start-concurrency` seams. Nightly and
release reject workload reductions and run the frozen GC contract. Every run
writes a structured receipt plus daemon and private owner-stat logs under
`target/reliability/`. The harness runs inside a supervised
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
