#!/usr/bin/env bash

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

ctxmux_recoverable_stop_stage=
if [[ ${1:-} == "--stage" ]]
then
  ctxmux_recoverable_stop_stage=${2:-}
  shift 2
fi
if [[ $# -ne 0 ]]
then
  echo "usage: scripts/check-recoverable-stop.sh --stage <rust-owner|public-parity>" >&2
  exit 2
fi

ctxmux_assert_selection() {
  ctxmux_selection_name=$1
  ctxmux_selection_pattern=$2
  ctxmux_selection_list=$3
  ctxmux_selection=$(
    printf '%s\n' "$ctxmux_selection_list" | rg "$ctxmux_selection_pattern" || true
  )
  if [[ -z $ctxmux_selection ]]
  then
    echo "recoverable Stop selection is empty: $ctxmux_selection_name ($ctxmux_selection_pattern)" >&2
    exit 1
  fi
  printf 'recoverable Stop selection %s:\n%s\n' "$ctxmux_selection_name" "$ctxmux_selection"
}

ctxmux_protocol_tests=$(cargo test --locked --package ctxmux-protocol -- --list)
ctxmux_daemon_unit_tests=$(cargo test --locked --package ctxmux-daemon --lib -- --list)
ctxmux_native_tests=$(
  cargo test --locked --package ctxmux-daemon --test native_lifecycle -- --list
)
ctxmux_assert_selection \
  "protocol owner" \
  '^tests::.*(recoverable_operations|recoverable_stop).*: test$' \
  "$ctxmux_protocol_tests"
ctxmux_assert_selection \
  "daemon settlement owner" \
  '^tests::recoverable_stop_.*: test$' \
  "$ctxmux_daemon_unit_tests"
ctxmux_assert_selection \
  "Rust public owner" \
  '^recoverable_stop_.*: test$' \
  "$ctxmux_native_tests"

case "$ctxmux_recoverable_stop_stage" in
rust-owner)
  cargo test --locked --package ctxmux-protocol recoverable_stop
  cargo test --locked --package ctxmux-protocol recoverable_operations
  cargo test --locked --package ctxmux-daemon --lib recoverable_stop
  cargo test --locked --package ctxmux-daemon --test native_lifecycle recoverable_stop -- \
    --nocapture --test-threads=1
  ;;
public-parity)
  ctxmux_assert_selection \
    "attachment parity" \
    '^recoverable_stop_attachment_.*: test$' \
    "$ctxmux_native_tests"
  ctxmux_assert_selection \
    "planned-exec parity" \
    '^recoverable_stop_planned_exec_.*: test$' \
    "$ctxmux_native_tests"
  ctxmux_assert_selection \
    "collection boundary" \
    '^tests::creation::recoverable_stop_collection_.*: test$' \
    "$ctxmux_daemon_unit_tests"
  cargo test --locked --package ctxmux-daemon --lib recoverable_stop_collection -- --nocapture
  cargo test --locked --package ctxmux-daemon --test native_lifecycle recoverable_stop -- --nocapture
  npm run test:e2e
  ;;
*)
  echo "usage: scripts/check-recoverable-stop.sh --stage <rust-owner|public-parity>" >&2
  exit 2
  ;;
esac
