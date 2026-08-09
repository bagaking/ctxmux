#!/usr/bin/env bash

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

ctxmux_codegen_dir=$(mktemp -d)
trap 'rm -rf "$ctxmux_codegen_dir"' EXIT

scripts/generate-protocol-types.sh "$ctxmux_codegen_dir"
diff -ru packages/sdk/src/generated "$ctxmux_codegen_dir"
