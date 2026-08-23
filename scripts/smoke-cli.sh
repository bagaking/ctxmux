#!/usr/bin/env bash

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

ctxmux_cli_bin=${CTXMUX_BIN:-"$PWD/target/debug/ctxmux"}
ctxmux_daemon_bin=${CTXMUXD_BIN:-"$PWD/target/debug/ctxmuxd"}
ctxmux_cli_tmp=$(cd "$(mktemp -d)" && pwd -P)
ctxmux_cli_socket="$ctxmux_cli_tmp/ctxmux/ctxmux.sock"
ctxmux_cli_daemon_log="$ctxmux_cli_tmp/ctxmuxd.log"
ctxmux_cli_daemon_pid=

mkdir -p "$(dirname "$ctxmux_cli_socket")"

fail() {
  echo "ctxmux CLI smoke: $*" >&2
  exit 1
}

cleanup() {
  ctxmux_cli_status=$?
  trap - EXIT
  if [[ -n "$ctxmux_cli_daemon_pid" ]] && kill -0 "$ctxmux_cli_daemon_pid" 2>/dev/null
  then
    kill -INT "$ctxmux_cli_daemon_pid" 2>/dev/null || true
    wait "$ctxmux_cli_daemon_pid" 2>/dev/null || true
  fi
  if [[ $ctxmux_cli_status -eq 0 ]]
  then
    rm -rf "$ctxmux_cli_tmp"
  else
    echo "ctxmux CLI smoke daemon log ($ctxmux_cli_daemon_log):" >&2
    sed -n '1,240p' "$ctxmux_cli_daemon_log" >&2 || true
    echo "ctxmux CLI smoke preserved failure directory: $ctxmux_cli_tmp" >&2
  fi
  exit "$ctxmux_cli_status"
}

expect_contains() {
  ctxmux_cli_haystack=$1
  ctxmux_cli_needle=$2
  [[ "$ctxmux_cli_haystack" == *"$ctxmux_cli_needle"* ]] ||
    fail "expected output to contain $(printf '%q' "$ctxmux_cli_needle"), got $(printf '%q' "$ctxmux_cli_haystack")"
}

expect_failure() {
  ctxmux_cli_expected=$1
  shift
  set +e
  ctxmux_cli_output=$("$@" 2>&1)
  ctxmux_cli_exit=$?
  set -e
  [[ $ctxmux_cli_exit -ne 0 ]] || fail "expected command to fail: $*"
  expect_contains "$ctxmux_cli_output" "$ctxmux_cli_expected"
}

trap cleanup EXIT

[[ -x "$ctxmux_cli_bin" ]] || fail "missing executable $ctxmux_cli_bin"
[[ -x "$ctxmux_daemon_bin" ]] || fail "missing executable $ctxmux_daemon_bin"

"$ctxmux_daemon_bin" --socket "$ctxmux_cli_socket" >"$ctxmux_cli_daemon_log" 2>&1 &
ctxmux_cli_daemon_pid=$!

ctxmux_cli_ready=false
for _ in $(seq 1 100)
do
  if "$ctxmux_cli_bin" --socket "$ctxmux_cli_socket" ping >/dev/null 2>&1
  then
    ctxmux_cli_ready=true
    break
  fi
  if ! kill -0 "$ctxmux_cli_daemon_pid" 2>/dev/null
  then
    fail "daemon exited before readiness"
  fi
  sleep 0.02
done
[[ $ctxmux_cli_ready == true ]] || fail "daemon did not become ready"

ctxmux_cli_run=$(
  "$ctxmux_cli_bin" --socket "$ctxmux_cli_socket" start -- /bin/sh -c \
    "read line; printf 'OUT:%s\\n' \"\$line\""
)
[[ "$ctxmux_cli_run" =~ ^[0-9a-f-]{36}$ ]] || fail "start returned an invalid Run id: $ctxmux_cli_run"

ctxmux_cli_status=$("$ctxmux_cli_bin" --socket "$ctxmux_cli_socket" status "$ctxmux_cli_run")
expect_contains "$ctxmux_cli_status" $'\trunning\t'
"$ctxmux_cli_bin" --socket "$ctxmux_cli_socket" resize "$ctxmux_cli_run" 100 40 >/dev/null
"$ctxmux_cli_bin" --socket "$ctxmux_cli_socket" input "$ctxmux_cli_run" $'hello\n' >/dev/null
ctxmux_cli_output=$("$ctxmux_cli_bin" --socket "$ctxmux_cli_socket" attach "$ctxmux_cli_run" 0)
expect_contains "$ctxmux_cli_output" "OUT:hello"

ctxmux_cli_stdin_run=$(
  "$ctxmux_cli_bin" --socket "$ctxmux_cli_socket" start -- /bin/sh -c \
    "read line; printf 'STDIN:%s\\n' \"\$line\""
)
printf 'streamed\n' | "$ctxmux_cli_bin" --socket "$ctxmux_cli_socket" input "$ctxmux_cli_stdin_run" --stdin
ctxmux_cli_stdin_output=$("$ctxmux_cli_bin" --socket "$ctxmux_cli_socket" attach "$ctxmux_cli_stdin_run")
expect_contains "$ctxmux_cli_stdin_output" "STDIN:streamed"

ctxmux_cli_sized_run=$(
  "$ctxmux_cli_bin" --socket "$ctxmux_cli_socket" start \
    --cwd "$ctxmux_cli_tmp" --cols 100 --rows 40 -- /bin/sh -c \
    "printf 'PWD:%s\\nSIZE:' \"\$PWD\"; stty size"
)
ctxmux_cli_sized_output=$("$ctxmux_cli_bin" --socket "$ctxmux_cli_socket" attach "$ctxmux_cli_sized_run")
expect_contains "$ctxmux_cli_sized_output" "PWD:$ctxmux_cli_tmp"
expect_contains "$ctxmux_cli_sized_output" "SIZE:40 100"

ctxmux_cli_parent=$("$ctxmux_cli_bin" --socket "$ctxmux_cli_socket" start --operation-key smoke-parent -- /bin/sh -c "printf parent")
ctxmux_cli_parent_retry=$("$ctxmux_cli_bin" --socket "$ctxmux_cli_socket" start --operation-key smoke-parent -- /bin/sh -c "printf parent")
[[ $ctxmux_cli_parent_retry == "$ctxmux_cli_parent" ]] || fail "Start retry returned a different Run"
ctxmux_cli_fork_line=$("$ctxmux_cli_bin" --socket "$ctxmux_cli_socket" fork --operation-key smoke-fork "$ctxmux_cli_parent")
ctxmux_cli_child=${ctxmux_cli_fork_line%%$'\t'*}
expect_contains "$ctxmux_cli_fork_line" "lineage=$ctxmux_cli_parent:level_a"
ctxmux_cli_fork_retry=$("$ctxmux_cli_bin" --socket "$ctxmux_cli_socket" fork --operation-key smoke-fork "$ctxmux_cli_parent")
[[ ${ctxmux_cli_fork_retry%%$'\t'*} == "$ctxmux_cli_child" ]] || fail "Fork retry returned a different Run"
"$ctxmux_cli_bin" --socket "$ctxmux_cli_socket" status "$ctxmux_cli_child" >/dev/null

ctxmux_cli_stop_run=$("$ctxmux_cli_bin" --socket "$ctxmux_cli_socket" start -- /bin/sh -c "trap '' INT; printf 'INT-READY\\n'; sleep 30")
ctxmux_cli_interrupt_ready=false
for _ in $(seq 1 100)
do
  ctxmux_cli_stop_status=$("$ctxmux_cli_bin" --socket "$ctxmux_cli_socket" status "$ctxmux_cli_stop_run")
  if [[ $ctxmux_cli_stop_status =~ head=[1-9][0-9]* ]]
  then
    ctxmux_cli_interrupt_ready=true
    break
  fi
  sleep 0.02
done
[[ $ctxmux_cli_interrupt_ready == true ]] || fail "interrupt fixture did not become ready"
"$ctxmux_cli_bin" --socket "$ctxmux_cli_socket" interrupt "$ctxmux_cli_stop_run" >/dev/null
"$ctxmux_cli_bin" --socket "$ctxmux_cli_socket" stop "$ctxmux_cli_stop_run" >/dev/null
ctxmux_cli_stopped=false
for _ in $(seq 1 100)
do
  ctxmux_cli_stop_status=$("$ctxmux_cli_bin" --socket "$ctxmux_cli_socket" status "$ctxmux_cli_stop_run")
  if [[ "$ctxmux_cli_stop_status" == *$'\texited('* ]]
  then
    ctxmux_cli_stopped=true
    break
  fi
  sleep 0.02
done
[[ $ctxmux_cli_stopped == true ]] || fail "stopped Run did not exit"

ctxmux_cli_list=$(CTXMUX_SOCKET="$ctxmux_cli_socket" "$ctxmux_cli_bin" list)
expect_contains "$ctxmux_cli_list" "$ctxmux_cli_run"
expect_contains "$ctxmux_cli_list" "$ctxmux_cli_child"
expect_contains "$("$ctxmux_cli_bin" --version)" "protocol 9"
expect_contains "$("$ctxmux_daemon_bin" --version)" "protocol 9"

ctxmux_cli_default_list=$(env -u CTXMUX_SOCKET XDG_RUNTIME_DIR="$ctxmux_cli_tmp" "$ctxmux_cli_bin" list)
expect_contains "$ctxmux_cli_default_list" "$ctxmux_cli_run"
expect_failure "unknown command" "$ctxmux_cli_bin" --socket "$ctxmux_cli_socket" unknown
expect_failure "invalid columns" "$ctxmux_cli_bin" --socket "$ctxmux_cli_socket" start --cols invalid -- /bin/sh
expect_failure "missing program" "$ctxmux_cli_bin" --socket "$ctxmux_cli_socket" start --
expect_failure "must not be empty" "$ctxmux_cli_bin" --socket "$ctxmux_cli_socket" start --operation-key "" -- /bin/true
expect_failure "invalid Run id" "$ctxmux_cli_bin" --socket "$ctxmux_cli_socket" status invalid
expect_failure "unexpected arguments" "$ctxmux_cli_bin" --socket "$ctxmux_cli_socket" list extra
expect_failure "invalid output byte cursor" "$ctxmux_cli_bin" --socket "$ctxmux_cli_socket" attach "$ctxmux_cli_run" invalid
expect_failure "greater than zero" "$ctxmux_cli_bin" --socket "$ctxmux_cli_socket" resize "$ctxmux_cli_run" 0 40
expect_failure "usage: ctxmuxd" "$ctxmux_daemon_bin" --socket
expect_failure "usage: ctxmuxd" "$ctxmux_daemon_bin" invalid
