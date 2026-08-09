#!/usr/bin/env bash

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

node --test scripts/check-fixtures.test.mjs
node scripts/check-fixtures.mjs

cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets
cargo run --quiet --package ctxmux -- --version
cargo run --quiet --package ctxmux-daemon -- --version

scripts/check-protocol-types.sh
npm run format:check
npm run typecheck
npm run build
npm test
