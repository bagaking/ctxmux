# Contributing

ctxmux is pre-alpha. Prefer one working vertical slice over speculative
abstractions, compatibility layers, or broad plugin machinery.

## Prerequisites

- Rust 1.96 with `rustfmt` and `clippy` (selected by `rust-toolchain.toml`)
- Node.js 24 or newer
- npm 11 or newer

## Setup

```bash
npm ci
scripts/check.sh
```

`scripts/check.sh` is the repository-owned quality boundary used locally and in
CI. It formats-checks, analyzes, builds, tests, and runs the initialized public
entrypoints.

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
