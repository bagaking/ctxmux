#!/usr/bin/env bash

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

ctxmux_check_completed=false
ctxmux_check_state_dir=
ctxmux_check_completion_marker=
ctxmux_check_completion_nonce=
ctxmux_check_cleanup() {
  if [[ -n $ctxmux_check_completion_marker ]]
  then
    rm -f -- "$ctxmux_check_completion_marker"
    ctxmux_check_completion_marker=
  fi
  if [[ -n $ctxmux_check_state_dir ]]
  then
    rmdir -- "$ctxmux_check_state_dir" 2>/dev/null || true
    ctxmux_check_state_dir=
  fi
}
ctxmux_check_completion_guard() {
  ctxmux_check_exit_status=$?
  trap - EXIT
  ctxmux_check_cleanup
  if [[ $ctxmux_check_completed != true && $ctxmux_check_exit_status -eq 0 ]]
  then
    echo "repository check exited before its final reliability smoke" >&2
    exit 1
  fi
  exit "$ctxmux_check_exit_status"
}
trap ctxmux_check_completion_guard EXIT

ctxmux_check_core() (
set -euo pipefail
trap - EXIT

ctxmux_check_coverage=false
if [[ ${1:-} == "--coverage" ]]
then
  ctxmux_check_coverage=true
  shift
fi
if [[ $# -ne 0 ]]
then
  echo "usage: scripts/check.sh [--coverage]" >&2
  exit 2
fi

ctxmux_tmux_qualification=${CTXMUX_TMUX_QUALIFICATION:-optional}
case "$ctxmux_tmux_qualification" in
optional)
  ;;
minimum-3.4|current)
  command -v tmux >/dev/null 2>&1 || {
    echo "tmux qualification requires the tmux executable" >&2
    exit 1
  }
  ctxmux_tmux_client_version=$(tmux -V)
  ctxmux_tmux_probe_name="ctxmux-check-$$-$RANDOM"
  tmux -L "$ctxmux_tmux_probe_name" new-session -d -s ctxmux-check-probe
  ctxmux_tmux_server_version=$(
    tmux -L "$ctxmux_tmux_probe_name" display-message -p '#{version}'
  )
  tmux -L "$ctxmux_tmux_probe_name" kill-server
  [[ $ctxmux_tmux_client_version == "tmux $ctxmux_tmux_server_version" ]] || {
    echo "tmux client/server version mismatch: client=$ctxmux_tmux_client_version server=$ctxmux_tmux_server_version" >&2
    exit 1
  }
  if [[ $ctxmux_tmux_qualification == minimum-3.4 ]]
  then
    [[ $ctxmux_tmux_server_version == 3.4 ]] || {
      echo "minimum tmux qualification requires server 3.4, got $ctxmux_tmux_server_version" >&2
      exit 1
    }
  elif [[ ! $ctxmux_tmux_server_version =~ ^3\.([4-9]|[1-9][0-9]+)[a-z]?$ ]]
  then
    echo "current tmux server $ctxmux_tmux_server_version is outside released 3.4 through 3.x" >&2
    exit 1
  fi
  echo "qualified tmux lane=$ctxmux_tmux_qualification client=$ctxmux_tmux_client_version server=$ctxmux_tmux_server_version"
  ;;
*)
  echo "CTXMUX_TMUX_QUALIFICATION must be optional, minimum-3.4, or current" >&2
  exit 2
  ;;
esac

cargo build --locked --quiet --package ctxmux-rss-sampler
node --test --test-concurrency=1 scripts/*.test.mjs
node scripts/check-fixtures.mjs
node scripts/ci-reachability.mjs
node scripts/reliability-policy.mjs

cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
if [[ $ctxmux_check_coverage == true ]]
then
scripts/check-coverage.sh
else
# Run every test binary before reporting. Without --no-fail-fast cargo stops at
# the first failing binary, so one failure hides every later binary's result and
# a second, unrelated defect stays invisible until the first is fixed. This is
# not a retry: a failure still fails the gate, it just fails with the complete
# picture.
cargo test --workspace --all-targets --no-fail-fast
cargo run --quiet --package ctxmux -- --version
cargo run --quiet --package ctxmux-daemon -- --version
scripts/smoke-cli.sh
fi

scripts/check-protocol-types.sh
npm run format:check
npm run typecheck
npm run build
npm run test:local-consumer
if [[ $ctxmux_check_coverage == false ]]
then
npm test
fi

printf '%s\n' "$ctxmux_check_completion_nonce" > "$ctxmux_check_completion_marker"
)

ctxmux_check_state_dir=$(mktemp -d "${TMPDIR:-/tmp}/ctxmux-check.XXXXXX")
ctxmux_check_completion_marker=$ctxmux_check_state_dir/completed
ctxmux_check_completion_nonce="$$-$RANDOM-$RANDOM"
set +e
ctxmux_check_core "$@"
ctxmux_check_core_status=$?
set -e
if [[ $ctxmux_check_core_status -ne 0 ]]
then
  echo "repository check core did not reach its completion boundary" >&2
  ctxmux_check_cleanup
  exit "$ctxmux_check_core_status"
fi
if [[ ! -f $ctxmux_check_completion_marker || $(< "$ctxmux_check_completion_marker") != "$ctxmux_check_completion_nonce" ]]
then
  echo "repository check core did not publish its completion token" >&2
  ctxmux_check_cleanup
  exit 1
fi
ctxmux_check_cleanup

scripts/check-reliability.sh --profile smoke
ctxmux_check_completed=true
