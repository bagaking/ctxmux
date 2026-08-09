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

ctxmux_reliability_artifact_dir=${CTXMUX_RELIABILITY_ARTIFACT_DIR:-target/reliability/$ctxmux_reliability_profile}
ctxmux_reliability_evidence=${CTXMUX_RELIABILITY_EVIDENCE:-$ctxmux_reliability_artifact_dir/result.json}
node scripts/reliability-policy.mjs
ctxmux_reliability_preflight=$(
  node scripts/reliability-policy.mjs \
    --prepare-qualification-evidence "$ctxmux_reliability_evidence" \
    --profile "$ctxmux_reliability_profile"
)

ctxmux_reliability_build_target_dir=target/reliability/provenance-build
ctxmux_reliability_daemon_bin=$ctxmux_reliability_build_target_dir/debug/ctxmuxd
ctxmux_reliability_rss_sampler_bin=$ctxmux_reliability_build_target_dir/debug/ctxmux-rss-sampler
ctxmux_reliability_build_argv=(
  cargo
  build
  --locked
  --quiet
  --package
  ctxmux-daemon
  --package
  ctxmux-rss-sampler
  --target-dir
  "$ctxmux_reliability_build_target_dir"
)
ctxmux_reliability_build_source_commit=$(git rev-parse HEAD)
ctxmux_reliability_build_source_tree=$(git rev-parse 'HEAD^{tree}')
if [[ -z $(git status --porcelain=v1 --untracked-files=all) ]]
then
  ctxmux_reliability_build_worktree_clean=true
else
  ctxmux_reliability_build_worktree_clean=false
fi
rm -f -- "$ctxmux_reliability_daemon_bin"
rm -f -- "$ctxmux_reliability_rss_sampler_bin"
"${ctxmux_reliability_build_argv[@]}"
if [[ ! -x $ctxmux_reliability_daemon_bin ]]
then
  echo "locked build did not produce $ctxmux_reliability_daemon_bin" >&2
  exit 1
fi
if [[ ! -x $ctxmux_reliability_rss_sampler_bin ]]
then
  echo "locked build did not produce $ctxmux_reliability_rss_sampler_bin" >&2
  exit 1
fi
cargo test --locked --quiet --package ctxmux-daemon socket_path
cargo test --locked --quiet --package ctxmux-daemon stop_after_wait_disables_signalling_before_state_publication
cargo test --locked --quiet --package ctxmux-daemon --test native_lifecycle protocol_frame_ceiling_and_duplicate_names_fail_before_run_mutation
node --import tsx --test packages/sdk/test/wrong-cases.test.ts
ctxmux_reliability_build_argv_json=$(
  node -e 'process.stdout.write(JSON.stringify(process.argv.slice(1)))' \
    "${ctxmux_reliability_build_argv[@]}"
)
CTXMUXD_BIN="$PWD/$ctxmux_reliability_daemon_bin" \
CTXMUX_RSS_SAMPLER_BIN="$PWD/$ctxmux_reliability_rss_sampler_bin" \
CTXMUX_RELIABILITY_BUILD_ARGV_JSON="$ctxmux_reliability_build_argv_json" \
CTXMUX_RELIABILITY_BUILD_SOURCE_COMMIT="$ctxmux_reliability_build_source_commit" \
CTXMUX_RELIABILITY_BUILD_SOURCE_TREE="$ctxmux_reliability_build_source_tree" \
CTXMUX_RELIABILITY_BUILD_WORKTREE_CLEAN="$ctxmux_reliability_build_worktree_clean" \
CTXMUX_RELIABILITY_BUILD_TARGET_DIR="$ctxmux_reliability_build_target_dir" \
CTXMUX_RELIABILITY_PREFLIGHT="$ctxmux_reliability_preflight" \
  node --import tsx scripts/reliability-qualification.ts \
  --profile "$ctxmux_reliability_profile" \
  "$@"
node scripts/reliability-policy.mjs \
  --qualification-receipt "$ctxmux_reliability_evidence" \
  --profile "$ctxmux_reliability_profile" \
  --preflight "$ctxmux_reliability_preflight"
