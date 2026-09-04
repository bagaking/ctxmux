#!/bin/bash
# Self-check for armlib.sh's quiescence guard.
#
# A guard that never blocks is WORSE than no guard: it stamps "clean" onto dirty
# data. That is exactly how loadavg failed -- it read 2.45 while 8 of 64 cores
# were burning, under the 4.0 gate in force at the time, so every arm sailed
# through. So the guard must be verified in BOTH directions, not just the happy
# one: quiet reads ~0, busy reads high, and quiesce actually waits.
#
# Run on the measurement host:  bash docs/verification/harness/check-guard.sh
set -u
. "$(dirname "$0")/armlib.sh"

fail() { echo "FAIL: $*"; exit 1; }

idle=$(busy_frac)
[ "$(echo "$idle < $QUIESCE_MAX_BUSY" | bc -l)" = 1 ] \
  || fail "host was not idle to begin with (busy=$idle); rerun on a quiet host"
echo "ok: idle busy_frac=$idle"

# Burn 8 cores. On any host with more than ~8 cores that is well over the 0.06
# gate, and on a smaller host it is more so.
for _ in $(seq 8); do (end=$((SECONDS+25)); while [ $SECONDS -lt $end ]; do :; done) & done
sleep 2

loaded=$(busy_frac)
[ "$(echo "$loaded > $QUIESCE_MAX_BUSY" | bc -l)" = 1 ] \
  || fail "guard cannot see 8 burning cores (busy=$loaded); it would pass dirty arms"
echo "ok: loaded busy_frac=$loaded (loadavg meanwhile reads $(load1) -- this is why loadavg alone is not the gate)"

start=$SECONDS
wait_for_quiet || fail "quiesce gave up entirely"
waited=$((SECONDS - start))
[ "$waited" -ge 10 ] || fail "quiesce returned after only ${waited}s; it did not actually wait out the burn"
echo "ok: quiesce waited ${waited}s for the burn to end"

wait
echo "PASS: the guard blocks on a busy host and releases on a quiet one"
