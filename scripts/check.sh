#!/usr/bin/env bash

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

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

node --test scripts/*.test.mjs
node scripts/check-fixtures.mjs
node scripts/ci-reachability.mjs
node scripts/reliability-policy.mjs

cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
if [[ $ctxmux_check_coverage == true ]]
then
scripts/check-coverage.sh
else
cargo test --workspace --all-targets
cargo run --quiet --package ctxmux -- --version
cargo run --quiet --package ctxmux-daemon -- --version
scripts/smoke-cli.sh
fi

scripts/check-protocol-types.sh
npm run format:check
npm run typecheck
npm run build
if [[ $ctxmux_check_coverage == false ]]
then
npm test
fi

scripts/check-reliability.sh --profile smoke
