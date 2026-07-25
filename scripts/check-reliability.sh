#!/usr/bin/env bash

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

ctxmux_reliability_profile=smoke
if [[ ${1:-} == "--profile" ]]
then
  ctxmux_reliability_profile=${2:-}
  shift 2
fi
case "$ctxmux_reliability_profile" in
  smoke|nightly|release|observe) ;;
  *)
    echo "invalid reliability profile: $ctxmux_reliability_profile" >&2
    exit 2
    ;;
esac

cargo build --locked --quiet --package ctxmux-daemon
cargo test --locked --quiet --package ctxmux-daemon socket_path
cargo test --locked --quiet --package ctxmux-daemon stop_after_wait_disables_signalling_before_state_publication
cargo test --locked --quiet --package ctxmux-daemon --test native_lifecycle protocol_frame_ceiling_and_duplicate_names_fail_before_run_mutation
node --import tsx --test packages/sdk/test/wrong-cases.test.ts
node --import tsx scripts/reliability-qualification.ts \
  --profile "$ctxmux_reliability_profile" \
  "$@"
