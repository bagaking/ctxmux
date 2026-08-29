#!/usr/bin/env bash

# Remote Runtime qualification.
#
# Stages:
#   --stage supervision  the minimal owner-host vertical (T-001)
#   --stage capability   probeable remote capability constants (T-002)
#   --stage partition    partition recovery, real OpenSSH, mixed capabilities (T-003)
#
# `supervision` proves the vertical without needing an SSH boundary, through two
# lanes that are both required:
#
#   1. A supervision lane running a real ctxmuxd and a real forwarding child
#      process. It proves readiness observation, owner-only socket placement,
#      identity selection and its fail-closed rejection, non-terminal transport
#      loss, cursor replay, and teardown.
#   2. An argument-shape lane proving the real system ssh client accepts the exact
#      argument list the production builder emits, so the stand-in cannot hide a
#      malformed invocation.
#
# `partition` owns the real-OpenSSH lane, which proves the shipped transport
# itself. It FAILS when its boundary is unavailable rather than skipping: a silent
# skip would let "remote works" be claimed by a run that never spoke SSH. Set
# CTXMUX_REMOTE_SSH_DESTINATION and CTXMUX_REMOTE_SOCKET to select the boundary;
# the current owner socket must be served by a memory-only daemon (no --state-dir)
# because the skew lane proves the absent persistent-state capability.
#
# The skew fixture needs a second build that really speaks the previous protocol
# generation, on BOTH ends. History holds one, so check it out rather than
# patching a constant: the generation that precedes the current one is whatever
# `git log -S` reports as its bump, and its parent is an ordinary ancestor of
# main. A checked-out older generation is a real build of a real past wire; a
# sed-patched constant only proves the current wire disagrees with a number.
#
#   bump=$(git log --format=%H -S"PROTOCOL_VERSION: u16 = $(
#     grep -oE 'PROTOCOL_VERSION: u16 = [0-9]+' crates/ctxmux-protocol/src/lib.rs |
#     grep -oE '[0-9]+$')" -- crates/ctxmux-protocol/src/lib.rs | tail -1)
#   git worktree add --detach /tmp/ctxmux-prev "$bump~1"
#   (cd /tmp/ctxmux-prev && cargo build --locked \
#      --package ctxmux --bin ctxmux --package ctxmux-daemon --bin ctxmuxd)
#
# `/tmp/ctxmux-prev/target/debug/ctxmux` is CTXMUX_REMOTE_OLD_CLIENT. Place that
# tree's `ctxmuxd` on the owner host and serve it on a second socket as
# CTXMUX_REMOTE_OLD_SOCKET. Nothing is cloned or compiled on the owner host —
# deployment stays a binary placement, exactly as the no-provisioning rule says.

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

ctxmux_remote_stage=

while [[ $# -gt 0 ]]
do
  case "$1" in
  --stage)
    [[ $# -ge 2 ]] || {
      echo "usage: scripts/check-remote-runtime.sh --stage <supervision|capability|partition>" >&2
      exit 2
    }
    ctxmux_remote_stage=$2
    shift 2
    ;;
  *)
    echo "unknown argument: $1" >&2
    echo "usage: scripts/check-remote-runtime.sh --stage <supervision|capability|partition>" >&2
    exit 2
    ;;
  esac
done

[[ -n $ctxmux_remote_stage ]] || {
  echo "usage: scripts/check-remote-runtime.sh --stage <supervision|capability|partition>" >&2
  exit 2
}

ctxmux_remote_supervision_lane() {
  echo "== remote supervision lane =="
  cargo test --locked --package ctxmux-remote
  cargo test --locked --package ctxmux-daemon --test remote_owner_host_endpoint
}

# Prove the production argument shape is accepted by the real ssh client.
#
# This is a cheap, environment-independent guard against the supervision lane's
# forwarder hiding a malformed argument list: a real `ssh` must fail at the host,
# never at option parsing.
ctxmux_remote_argument_shape() {
  echo "== real ssh argument shape =="
  command -v ssh >/dev/null 2>&1 || {
    echo "the system ssh client is required" >&2
    exit 1
  }
  ssh -V
  local probe_dir
  probe_dir=$(mktemp -d "${TMPDIR:-/tmp}/ctxmux-remote-argshape.XXXXXX")
  local output
  output=$(
    ssh -N -T \
      -o BatchMode=yes \
      -o ExitOnForwardFailure=yes \
      -L "$probe_dir/local.sock:/run/ctxmux/ctxmux.sock" \
      ctxmux-argument-shape-probe.invalid 2>&1 || true
  )
  rmdir -- "$probe_dir" 2>/dev/null || true
  if grep -qiE 'usage:|unknown option|bad (local )?forwarding specification' <<<"$output"
  then
    echo "the system ssh client rejected the endpoint argument shape:" >&2
    echo "$output" >&2
    exit 1
  fi
  echo "accepted the -L StreamLocal argument shape"
}

# Prove the endpoint contract is probeable from both public surfaces, and that no
# party advertises a fact it cannot observe.
ctxmux_remote_capability_lane() {
  echo "== remote endpoint contract =="
  cargo test --locked --package ctxmux-protocol remote_endpoint

  # The generated TypeScript constant must match the Rust source of truth rather
  # than being a second hand-written declaration.
  scripts/check-protocol-types.sh

  # A real ctxmuxd behind a real forwarder, driven by the TypeScript SDK: proves a
  # forwarded socket needs no SDK change, that the daemon advertises no remote key,
  # and that capability rejection still happens before dispatch.
  cargo build --locked --quiet --package ctxmux-daemon --bins
  CTXMUXD_BIN="$PWD/target/debug/ctxmuxd" \
  CTXMUX_FAKE_SSH_BIN="$PWD/target/debug/fake-ssh" \
    npx --no-install tsx --test packages/sdk/test/remote-endpoint.test.ts
}

ctxmux_remote_openssh_lane() {
  echo "== real OpenSSH lane =="
  if [[ -z ${CTXMUX_REMOTE_SSH_DESTINATION:-} ]]
  then
    cat >&2 <<'EOF'
error: the real OpenSSH lane has no destination.

Remote cannot be reported as qualified by a run that never spoke SSH, so this
lane fails instead of skipping.

Set CTXMUX_REMOTE_SSH_DESTINATION to an SSH destination whose owner host runs a
ctxmuxd, and CTXMUX_REMOTE_SOCKET to that daemon's memory-only socket path
(the current daemon must not use --state-dir for the capability-skew assertion).
For a local loopback boundary, enable Remote Login and authorize your own key, then:

  CTXMUX_REMOTE_SSH_DESTINATION=<owner-host> \
  CTXMUX_REMOTE_SOCKET=/path/to/ctxmux.sock \
  CTXMUX_REMOTE_OLD_SOCKET=/path/to/older-ctxmux.sock \
  CTXMUX_REMOTE_OLD_CLIENT=/path/to/older-ctxmux \
  CTXMUX_REMOTE_DAEMON_BINARY=/path/to/ctxmuxd \
    scripts/check-remote-runtime.sh --stage partition
EOF
    exit 1
  fi
  : "${CTXMUX_REMOTE_SOCKET:?CTXMUX_REMOTE_SOCKET must name a memory-only owner-host ctxmuxd socket (no --state-dir)}"
  : "${CTXMUX_REMOTE_OLD_SOCKET:?CTXMUX_REMOTE_OLD_SOCKET must name the owner-host socket served by the older build}"
  : "${CTXMUX_REMOTE_OLD_CLIENT:?CTXMUX_REMOTE_OLD_CLIENT must point to the compiled older ctxmux client}"
  : "${CTXMUX_REMOTE_DAEMON_BINARY:?CTXMUX_REMOTE_DAEMON_BINARY must point to the compiled daemon binary provisioned on the owner host}"

  # Fail closed on an unusable boundary rather than letting the test binary
  # report an ambiguous error later.
  ssh -o BatchMode=yes -o ConnectTimeout=10 \
    "$CTXMUX_REMOTE_SSH_DESTINATION" true || {
    echo "cannot reach $CTXMUX_REMOTE_SSH_DESTINATION with the caller's existing SSH credentials" >&2
    exit 1
  }

  # The real-OpenSSH tests live in their own binary. Naming the wrong one would
  # select zero tests and still exit 0 — a silent skip claimed as qualification,
  # which is exactly what this stage exists to prevent. So require the run to
  # report at least one passing test rather than trusting the exit code alone.
  local result
  result=$(
    cargo test --locked --package ctxmux-daemon \
      --test remote_real_openssh -- --ignored --test-threads=1 \
      | tee /dev/stderr
  )
  if ! grep -qE '^test result: ok\. [1-9][0-9]* passed' <<<"$result"
  then
    echo "the real-OpenSSH lane selected no passing test; remote cannot be reported as qualified" >&2
    exit 1
  fi
}

case "$ctxmux_remote_stage" in
supervision)
  ctxmux_remote_supervision_lane
  ctxmux_remote_argument_shape
  ;;
capability)
  ctxmux_remote_capability_lane
  ;;
partition)
  ctxmux_remote_openssh_lane
  ;;
*)
  echo "unknown stage: $ctxmux_remote_stage" >&2
  exit 2
  ;;
esac

echo "remote runtime stage '$ctxmux_remote_stage' passed"
