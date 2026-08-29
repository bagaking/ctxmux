#!/usr/bin/env bash

# Measure what the owner-host tunnel costs, so a decision about it can cite a
# number instead of an intuition.
#
# This harness exists because this crate has no code on the tunnel's data path.
# The endpoint spawns the system OpenSSH client, waits for its forward to carry
# traffic, and kills the process group; steady-state throughput is therefore the
# system client's, not ours. What *is* ours is the establishment path and the
# readiness observation, and those were previously described only by assumption.
#
# It reports four quantities:
#
#   establishment  a distribution over repeated spawn-to-usable samples, not one
#                  reading, because a single sample cannot distinguish a typical
#                  cost from an outlier and this path is visibly bimodal.
#   dead time      how much of the *reported* establishment latency is an
#                  artifact of the readiness poll granularity rather than real
#                  handshake work. This is the number a decision about the poll
#                  interval needs, and nothing else in the repository produces it.
#   throughput     steady-state bytes through one established forward.
#   fanout         the marginal cost of each additional concurrent tunnel, which
#                  is what a connection-reuse decision turns on.
#
# Stages are separable because the two decisions downstream of this harness need
# different halves of it, and running the whole thing to answer half a question
# wastes the slowest part.
#
# Which forwarder produced the numbers is always stated in the output. The
# stand-in is a Unix-socket relay with no crypto and no network, so its
# establishment cost is a floor, not a prediction of the real client's. Numbers
# from the two lanes are not comparable and the report says so rather than
# letting a reader average them.

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

ctxmux_remote_cost_usage() {
  cat >&2 <<'EOF'
usage: scripts/check-remote-cost.sh [--stage <stage>] [--samples <n>] [--json <path>]
       scripts/check-remote-cost.sh --self-test

stages:
  establishment  spawn-to-usable latency distribution and readiness-poll dead time
  throughput     steady-state bytes through one established forward
  fanout         marginal cost of each additional concurrent tunnel
  all            every stage (default)

  --samples <n>  samples per measured quantity (default 30, minimum 5)
  --json <path>  also write the machine-readable report to this path
  --self-test    prove the harness fails loudly rather than reporting a silent
                 zero, then exit

The real-client lane runs when CTXMUX_REMOTE_SSH_DESTINATION is set, and needs
CTXMUX_REMOTE_SOCKET for the owner-side path. Absent that destination this
harness measures the stand-in forwarder and labels every figure accordingly; it
never presents a stand-in number as a real-client one.
EOF
}

ctxmux_remote_cost_stage=all
ctxmux_remote_cost_samples=30
ctxmux_remote_cost_json=
ctxmux_remote_cost_self_test=false

while [[ $# -gt 0 ]]
do
  case $1 in
  --stage)
    if [[ $# -lt 2 ]]
    then
      echo "error: --stage requires a value" >&2
      ctxmux_remote_cost_usage
      exit 2
    fi
    ctxmux_remote_cost_stage=$2
    shift 2
    ;;
  --samples)
    if [[ $# -lt 2 ]]
    then
      echo "error: --samples requires a value" >&2
      ctxmux_remote_cost_usage
      exit 2
    fi
    ctxmux_remote_cost_samples=$2
    shift 2
    ;;
  --json)
    if [[ $# -lt 2 ]]
    then
      echo "error: --json requires a value" >&2
      ctxmux_remote_cost_usage
      exit 2
    fi
    ctxmux_remote_cost_json=$2
    shift 2
    ;;
  --self-test)
    ctxmux_remote_cost_self_test=true
    shift
    ;;
  -h | --help)
    ctxmux_remote_cost_usage
    exit 0
    ;;
  *)
    echo "error: unknown argument '$1'" >&2
    ctxmux_remote_cost_usage
    exit 2
    ;;
  esac
done

case $ctxmux_remote_cost_stage in
establishment | throughput | fanout | all) ;;
*)
  echo "error: unknown stage '$ctxmux_remote_cost_stage'" >&2
  ctxmux_remote_cost_usage
  exit 2
  ;;
esac

# The forwarder is the shipped stand-in rather than a bespoke one written here.
# A second fake would be a second contract to keep in agreement with the
# production argument builder, and it is the builder that must stay under test.
ctxmux_remote_cost_forwarder=$PWD/target/debug/fake-ssh
if [[ ! -x $ctxmux_remote_cost_forwarder ]]
then
  echo "== building the stand-in forwarder =="
  cargo build --locked --quiet --package ctxmux-daemon --bins
fi
if [[ ! -x $ctxmux_remote_cost_forwarder ]]
then
  echo "error: the stand-in forwarder is missing after a build: $ctxmux_remote_cost_forwarder" >&2
  echo "A cost report with no forwarder would be a report about nothing." >&2
  exit 1
fi

if [[ $ctxmux_remote_cost_self_test == true ]]
then
  echo "== harness self-test =="
  exec node scripts/remote-cost-measure.mjs \
    --self-test \
    --forwarder "$ctxmux_remote_cost_forwarder"
fi

ctxmux_remote_cost_args=(
  --forwarder "$ctxmux_remote_cost_forwarder"
  --stage "$ctxmux_remote_cost_stage"
  --samples "$ctxmux_remote_cost_samples"
)
if [[ -n $ctxmux_remote_cost_json ]]
then
  ctxmux_remote_cost_args+=(--json "$ctxmux_remote_cost_json")
fi

echo "== remote cost: stage '$ctxmux_remote_cost_stage' =="
node scripts/remote-cost-measure.mjs "${ctxmux_remote_cost_args[@]}"
echo "remote cost stage '$ctxmux_remote_cost_stage' reported"
