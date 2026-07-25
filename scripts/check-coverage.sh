#!/usr/bin/env bash

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

command -v cargo-llvm-cov >/dev/null 2>&1 || {
  echo "coverage requires cargo-llvm-cov 0.8.7 (cargo install --locked --version 0.8.7 cargo-llvm-cov)" >&2
  exit 1
}
[[ $(cargo llvm-cov --version) == "cargo-llvm-cov 0.8.7" ]] || {
  echo "coverage requires exactly cargo-llvm-cov 0.8.7" >&2
  exit 1
}

ctxmux_coverage_codegen_dir=$(mktemp -d)
ctxmux_coverage_codegen_block=$(mktemp)
trap 'rm -rf "$ctxmux_coverage_codegen_dir"; rm -f "$ctxmux_coverage_codegen_block"' EXIT

mkdir -p coverage/rust coverage/typescript

cargo llvm-cov clean --workspace
cargo llvm-cov test --workspace --all-targets --all-features --no-report
cargo llvm-cov run --no-clean --package ctxmux --bin ctxmux -- --version >/dev/null
cargo llvm-cov run --no-clean --package ctxmux-protocol --bin export-types -- \
  "$ctxmux_coverage_codegen_dir"

export LLVM_PROFILE_FILE="$PWD/target/llvm-cov-target/ctxmux-%p-%14m.profraw"
if "$PWD/target/llvm-cov-target/debug/export-types" >/dev/null 2>&1
then
  echo "export-types unexpectedly accepted a missing output directory" >&2
  exit 1
fi
if "$PWD/target/llvm-cov-target/debug/export-types" "$ctxmux_coverage_codegen_block" >/dev/null 2>&1
then
  echo "export-types unexpectedly accepted an ordinary file as its output directory" >&2
  exit 1
fi
CTXMUX_BIN="$PWD/target/llvm-cov-target/debug/ctxmux" \
  CTXMUXD_BIN="$PWD/target/llvm-cov-target/debug/ctxmuxd" \
  scripts/smoke-cli.sh

cargo llvm-cov report --lcov --output-path coverage/rust/lcov.info
cargo llvm-cov report --json --output-path coverage/rust/coverage.json

npx c8 --clean --all \
  --reports-dir=coverage/typescript \
  --temp-directory=coverage/typescript/tmp \
  --reporter=text \
  --reporter=json \
  --include='packages/sdk/src/**/*.ts' \
  --exclude='packages/sdk/src/generated/**' \
  npm test

node scripts/coverage-policy.mjs \
  --root . \
  --policy coverage-policy.json \
  --rust-lcov coverage/rust/lcov.info \
  --typescript-json coverage/typescript/coverage-final.json \
  --base "${CTXMUX_COVERAGE_BASE:-HEAD}" \
  --require-changed-lines "${CTXMUX_COVERAGE_REQUIRE_CHANGED_LINES:-false}"
