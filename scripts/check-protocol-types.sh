#!/usr/bin/env bash

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

ctxmux_codegen_dir=$(mktemp -d)
trap 'rm -rf "$ctxmux_codegen_dir"' EXIT

scripts/generate-protocol-types.sh "$ctxmux_codegen_dir"
diff -ru packages/sdk/src/generated "$ctxmux_codegen_dir"

# The protocol.md header states a live generation number, and Source-of-Truth
# #3 (AGENTS.md) requires it to track the code. It rotted once already
# (generations 15-17 shipped under a title still reading 14). Derive both
# numbers from their authoritative sources and compare -- never pin a literal
# here, or this guard becomes the next stale copy (see the smoke-cli.sh note).
protocol_source=crates/ctxmux-protocol/src/lib.rs
protocol_doc=docs/protocol.md

code_generation=$(
  sed -n 's/.*PROTOCOL_VERSION: u16 = \([0-9]\{1,\}\);.*/\1/p' "$protocol_source"
)
[[ -n "$code_generation" ]] ||
  { echo "cannot read PROTOCOL_VERSION from $protocol_source" >&2; exit 1; }

doc_generation=$(
  sed -n 's/^# Local Protocol Generation \([0-9]\{1,\}\)$/\1/p' "$protocol_doc"
)
[[ -n "$doc_generation" ]] ||
  { echo "cannot read the generation from the $protocol_doc header" >&2; exit 1; }

[[ "$doc_generation" == "$code_generation" ]] ||
  {
    echo "$protocol_doc header declares generation $doc_generation but \
$protocol_source is $code_generation; update the header to match the code" >&2
    exit 1
  }
