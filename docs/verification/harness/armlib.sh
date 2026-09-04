#!/bin/bash
# Shared benchmark arm runner with host-quiescence guards.
#
# Source this, then call `run_guarded <tag> <binary> <chatty> <timeout> [yes|no]`.
# Requires POP and REPS exported, and WARMBENCH pointing at the driver binary.
#
# WHY THIS EXISTS (2026-09-14). Two harnesses disagreed 30x on the SAME binary at
# the SAME shape (remove 950-1206 ms vs 21-35 ms, chatty=2, one build). Neither
# was right: two orphaned daemons from an earlier run were pinned at 174% and
# 178% of a core, with their PTY fleets still spinning, while both harnesses
# measured. Load average was 10.96 on a box whose numbers are fsync- and
# scheduler-sensitive. Killing the orphans dropped it to 0.19 -- the orphans WERE
# the load.
#
# Two defects, both in the harness, both invisible in the output:
#   1. pkill on a harness script skips its own `kill $dpid` teardown, so the
#      daemon it had spawned is orphaned rather than reaped.
#   2. A SIGTERM'd daemon does NOT take its PTY children with it. The
#      `sh -c while :; do sleep 30; done` fleet outlives it and keeps running.
#
# The deeper failure is that a contaminated arm looked exactly like a clean one.
# No field in the output could have revealed it, and `ps` is long gone by the
# time anyone reads the log. So:
#   * every arm refuses to start until the host is quiet, and says why it waited
#   * every arm reaps the fleet, not just the daemon, and verifies the reap
#   * every arm stamps busy-fraction and load INTO ITS OWN OUTPUT LINE, so a
#     contaminated measurement is self-evident afterwards from the log alone
#
# See docs/benchmark-comparison-conventions.md section 4.3.
set -u

WARMBENCH="${WARMBENCH:-/tmp/warmbench}"

# A shared box has other tenants, so demanding a truly idle host would never run.
# Demanding the absence of OUR OWN leaked load is both achievable and the thing
# that actually corrupted the numbers.
#
# Load average is NOT sufficient on its own: it is a ~1-minute decaying average,
# so it reads calm for the first seconds of a heavy arm and stays high for a
# minute after one ends. Measured: with 8 of 64 cores deliberately burning, the
# instantaneous busy fraction was 0.1293 while load1 still read 2.45 -- under a
# 4.0 gate, so the gate would have waved that arm straight through. Gate on the
# instantaneous /proc/stat fraction; keep loadavg only as a coarse secondary.
QUIESCE_MAX_LOAD="${QUIESCE_MAX_LOAD:-8.0}"
QUIESCE_MAX_BUSY="${QUIESCE_MAX_BUSY:-0.06}"
QUIESCE_TIMEOUT="${QUIESCE_TIMEOUT:-300}"

load1() { cut -d' ' -f1 < /proc/loadavg; }

# Fraction of all cores busy over a 1 s window, from /proc/stat. Unlike loadavg
# this has no memory of what the host was doing a minute ago.
busy_frac() {
  local a b idle_a idle_b tot_a tot_b
  read -r _ a < /proc/stat
  set -- $a; idle_a=$4; tot_a=0; for v in "$@"; do tot_a=$((tot_a + v)); done
  sleep 1
  read -r _ b < /proc/stat
  set -- $b; idle_b=$4; tot_b=0; for v in "$@"; do tot_b=$((tot_b + v)); done
  local dt=$((tot_b - tot_a)) di=$((idle_b - idle_a))
  [ "$dt" -le 0 ] && { echo 1; return; }
  echo "scale=4; 1 - $di / $dt" | bc -l
}

# Patterns used to reap our own leftovers. Both must match OUR processes only --
# a loose pattern here reaps another tenant's work. DAEMON_PAT matches the test
# binaries under measurement; FLEET_PAT matches the exact child shape warmbench
# spawns. If you change how either is launched, change these with it, or the
# reap silently becomes a no-op and the orphans come back.
DAEMON_PAT="${DAEMON_PAT:-linux-ctxmuxd-}"
FLEET_PAT="${FLEET_PAT:-while :; do sleep 30; done}"

# Reap anything we leaked earlier, including from a previous aborted run.
reap_our_processes() {
  pkill -f "$DAEMON_PAT" 2>/dev/null
  sleep 0.5
  pkill -9 -f "$DAEMON_PAT" 2>/dev/null
  # The PTY fleet outlives its daemon, so it needs its own sweep.
  pkill -9 -f "$FLEET_PAT" 2>/dev/null
  sleep 0.5
}

# Refuse to measure on a dirty host. Returns 1 if it gave up, and the caller
# records that rather than measuring anyway.
wait_for_quiet() {
  local waited=0 l bf
  reap_our_processes
  while :; do
    bf=$(busy_frac)   # takes 1 s, so the loop self-paces
    l=$(load1)
    if [ "$(echo "$bf < $QUIESCE_MAX_BUSY && $l < $QUIESCE_MAX_LOAD" | bc -l)" = 1 ]; then
      [ "$waited" -gt 0 ] && echo "    (waited ${waited}s for host to settle: busy=$bf load=$l)"
      return 0
    fi
    if [ "$waited" -ge "$QUIESCE_TIMEOUT" ]; then
      echo "    !! HOST NEVER WENT QUIET: busy=$bf load=$l after ${waited}s -- arm SKIPPED, not measured"
      return 1
    fi
    sleep 2
    waited=$((waited + 3))
  done
}

# Kill the daemon AND its fleet, then verify. An unverified teardown is what
# produced the 174% orphans in the first place.
teardown() {
  local dpid="$1"
  kill "$dpid" 2>/dev/null
  local waited=0
  while kill -0 "$dpid" 2>/dev/null && [ "$waited" -lt 50 ]; do
    sleep 0.1; waited=$((waited + 1))
  done
  kill -9 "$dpid" 2>/dev/null
  wait "$dpid" 2>/dev/null
  pkill -9 -f "$FLEET_PAT" 2>/dev/null
  sleep 0.3
  local strays
  strays=$(pgrep -cf "$FLEET_PAT" 2>/dev/null || true)
  [ -z "$strays" ] && strays=0
  if [ "$strays" -gt 0 ]; then
    echo "    !! $strays fleet processes survived teardown"
  fi
}

# run_guarded <tag> <binary> <chatty> <timeout> [persist=yes|no]
run_guarded() {
  local tag="$1" bin="$2" chatty="$3" budget="$4" persist="${5:-yes}"
  local dir sock dpid out rc u0 s0 u1 s1 lb la bb

  if ! wait_for_quiet; then
    echo "$tag SKIPPED-DIRTY-HOST"
    return
  fi
  lb=$(load1); bb=$(busy_frac)

  dir=$(mktemp -d); sock="$dir/s.sock"
  if [ "$persist" = yes ]; then
    "$bin" --socket "$sock" --state-dir "$dir/state" >"$dir/d.log" 2>&1 &
  else
    "$bin" --socket "$sock" >"$dir/d.log" 2>&1 &
  fi
  dpid=$!
  for _ in $(seq 800); do [ -S "$sock" ] && break; sleep 0.02; done
  if [ ! -S "$sock" ]; then
    echo "$tag DAEMON-NEVER-LISTENED"; teardown "$dpid"; rm -rf "$dir"; return
  fi

  read -r _ _ _ _ _ _ _ _ _ _ _ _ _ u0 s0 _ < /proc/$dpid/stat
  out=$(timeout "$budget" "$WARMBENCH" "$sock" "$POP" "$REPS" "$chatty" 2>&1 | tail -1)
  rc=$?
  read -r _ _ _ _ _ _ _ _ _ _ _ _ _ u1 s1 _ < /proc/$dpid/stat 2>/dev/null || { u1=$u0; s1=$s0; }
  la=$(load1)

  if [ -z "$out" ]; then
    echo "$tag TIMEOUT-${budget}s-NO-OUTPUT (rc=$rc) busy0=$bb load=$lb->$la"
  else
    echo "$tag $out busy0=$bb load=$lb->$la"
  fi
  echo "$tag   utime=$(echo "($u1-$u0)/100" | bc -l | cut -c1-6)s stime=$(echo "($s1-$s0)/100" | bc -l | cut -c1-6)s"
  if [ -s "$dir/d.log" ]; then
    echo "$tag   daemon said: $(head -3 "$dir/d.log" | tr '\n' ' | ')"
  fi

  teardown "$dpid"
  rm -rf "$dir"
}
