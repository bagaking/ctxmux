#!/usr/bin/env bash

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

output=${1:-packages/sdk/src/generated}
cargo run --quiet --package ctxmux-protocol --bin export-types -- "$output"
npx prettier --write "$output" >/dev/null
