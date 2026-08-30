#!/usr/bin/env bash

# Render a pass/fail VERDICT on fleet-scale resource behaviour at thousands of
# concurrent Runs. This is the acceptance harness the repository was missing.
#
# Every resource decision in this repository is qualified at N=128 (ADR 013).
# The product runs thousands of concurrent Runs. Measurement scripts that emit
# numbers at that scale exist and have run on the farm; none renders a verdict.
# "The farm results" therefore do not exist as a pass/fail judgement. This
# harness makes that judgement exist: it derives thresholds from farm-host
# observations using the SAME derivation rules the darwin budget is pinned to
# (imported from scripts/reliability-budget-contract.mjs, never edited), then
# compares fresh measurements against them and exits nonzero when they are not
# met.
#
# What it measures, per tier and for both idle and active Runs: fds_per_run,
# idle cpu_core_percent, steady_rss_kib and rss_kib_per_run, per-Run and
# aggregate lifetime output bytes, List latency and success, whether the census
# daemon was still alive at the end, and admission behaviour at the descriptor
# ceiling (which must refuse cleanly with run_capacity and never hit EMFILE).
#
# It does NOT measure retention. The retention budget bounds bytes the daemon is
# still holding; the only per-Run byte counter on the wire is monotonic lifetime
# output, so the fleet-wide 1 GiB cap has no proof here until retained_bytes is
# exposed by the protocol.
#
# Tiers: 128, 512, 2048, 4000. The 128 tier is load-bearing. It overlaps the
# existing darwin gate, so the harness cross-checks the farm's 128 numbers
# against the darwin baseline on the platform-invariant per-Run costs. If they
# disagree, the harness is measuring something different from the gate and its
# larger tiers cannot be trusted — this is stated in the output, not just here.
#
# WHAT THESE NUMBERS MAY NOT BE COMPARED AGAINST. The farm is Linux x86_64; the
# darwin baseline is arm64 macOS. RSS and CPU legitimately differ by platform
# and are bounded per-tier by ceilings derived ON THE FARM, not against darwin.
# Only the structural per-Run costs (descriptors, threads), which are
# platform-invariant by design, are cross-checked against darwin. A farm number
# is never presented as a darwin one, and the thresholds file is bound to its
# host class and refuses a receipt from another kernel.
#
# Stages are separable by profile. --self-test runs on ANY host in seconds
# without a fleet, so the Mac gate can confirm the harness is not broken without
# running the load; it proves the harness FAILS LOUDLY rather than reporting a
# silent zero. --profile smoke exercises the drive and census path at a tiny
# size on the local host, so a broken driver is caught before the farm is
# engaged. --profile accept is the only profile that needs the farm.
#
# PROVISIONING. The repository must not leave this Mac: no clone, no copy of
# repository code to the farm. So --profile accept runs ON the Mac and drives
# the farm node named by CTXMUX_FLEET_HOST over ssh, keeping the verdict logic
# here where it is reviewed. Only the daemon and client BINARIES are copied,
# cross-compiled from THIS worktree with cargo-zigbuild, stripped, and verified
# by SHA-256 on both ends before they are run — a truncated transfer produces a
# bogus measurement that looks exactly like a real one, so the transfer is
# checked, not trusted. Everything left on the farm is removed on exit.

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

ctxmux_fleet_usage() {
  cat >&2 <<'EOF'
usage: scripts/check-fleet-scale.sh --profile <smoke|accept> [--json <path>]
       scripts/check-fleet-scale.sh --self-test

profiles:
  smoke   drive and census a tiny local fleet, derive thresholds, and render a
          verdict on the local host. Proves the driver, census, derivation, and
          verdict path work end to end without engaging the farm. Not a
          fleet-scale acceptance: the local host is not the farm and its numbers
          are labelled accordingly.
  accept  the real acceptance. Cross-builds the daemon and client from this
          worktree, ships the stripped binaries to the farm node (the ssh alias
          in CTXMUX_FLEET_HOST), verifies them by SHA-256 on both ends, runs
          three observation rounds per tier, derives farm thresholds, and
          renders the verdict. Requires the farm.

  --json <path>  also write the machine-readable verdict to this path
  --self-test    prove the harness fails loudly rather than rendering a false
                 verdict, then exit. Runs anywhere in seconds without a fleet.

The farm node is reached as the ssh alias in CTXMUX_FLEET_HOST, defaulting to
cn4. Farm placement is the farm's policy, not this repository's, so the
destination is overridable rather than pinned here. This harness never clones or
copies repository code to the farm; only the built binaries are transferred, and
only after their SHA-256 is confirmed identical on both ends.
EOF
}

ctxmux_fleet_profile=
ctxmux_fleet_json=
ctxmux_fleet_self_test=false

while [[ $# -gt 0 ]]
do
  case $1 in
  --profile)
    if [[ $# -lt 2 ]]
    then
      echo "error: --profile requires a value" >&2
      ctxmux_fleet_usage
      exit 2
    fi
    ctxmux_fleet_profile=$2
    shift 2
    ;;
  --json)
    if [[ $# -lt 2 ]]
    then
      echo "error: --json requires a value" >&2
      ctxmux_fleet_usage
      exit 2
    fi
    ctxmux_fleet_json=$2
    shift 2
    ;;
  --self-test)
    ctxmux_fleet_self_test=true
    shift
    ;;
  -h | --help)
    ctxmux_fleet_usage
    exit 0
    ;;
  *)
    echo "error: unknown argument '$1'" >&2
    ctxmux_fleet_usage
    exit 2
    ;;
  esac
done

# Shell-level self-test: the census fragments whose failure mode is "no cell at
# all" rather than "a wrong cell". Each case runs the real fragment under the
# census's own `set -euo pipefail`, so a regression here fails the gate instead
# of being discovered on the farm an hour into a run.
ctxmux_fleet_shell_self_test() {
  local failures=0

  # A drained fleet is the healthy outcome, and it is the one `pgrep` reports
  # by exiting 1. Run the real drain fragment against a pid that has no
  # children and require a cell-bearing exit. Before the guard this exited 1
  # with no output; the node fixtures could not see it, because a census that
  # aborts produces nothing to judge.
  local drained_pid=$$ observed rc
  observed=$(
    bash -euo pipefail -c '
      count_children() { { pgrep -P "$1" 2>/dev/null || true; } | wc -l | tr -d " "; }
      for _ in $(seq 1 3); do
        n=$(count_children "$1")
        if [[ ${n:-0} -eq 0 ]]; then break; fi
        sleep 0.1
      done
      printf "%s" "${n:-missing}"
    ' _ "$drained_pid" 2>/dev/null
  ) && rc=0 || rc=$?
  if [[ $rc -eq 0 && $observed == 0 ]]; then
    echo "  ok    a fully drained fleet still emits a cell: cleanup_live_children=0"
  else
    echo "  FAIL  a drained fleet aborted the census (rc=$rc, observed='${observed:-}')"
    echo "        pgrep exits 1 when it matches nothing; under set -e that kills the run"
    failures=$((failures + 1))
  fi

  # The counter must still report a real number when children DO exist,
  # otherwise the leak check downstream is comparing against a constant zero
  # and every stranded child passes.
  local live_out live_rc
  live_out=$(
    bash -euo pipefail -c '
      count_children() { { pgrep -P "$1" 2>/dev/null || true; } | wc -l | tr -d " "; }
      sleep 30 & sleep 30 &
      n=$(count_children $$)
      kill %1 %2 2>/dev/null || true
      printf "%s" "$n"
    ' 2>/dev/null
  ) && live_rc=0 || live_rc=$?
  if [[ $live_rc -eq 0 && ${live_out:-0} -ge 2 ]]; then
    echo "  ok    live children are still counted, not flattened to zero: $live_out"
  else
    echo "  FAIL  the child counter did not observe live children (rc=$live_rc, got '${live_out:-}')"
    failures=$((failures + 1))
  fi

  if [[ $failures -ne 0 ]]; then
    echo "shell self-test FAILED: $failures census fragment(s) would not produce a cell" >&2
    exit 1
  fi
  echo "  shell fragments ok: the census emits a cell on the healthy path"
}

# The self-test is the cheap, host-agnostic gate check. It proves each refusal
# fires: a fleet acceptance harness that could report a silent zero is worse
# than none, because a green verdict with nothing behind it would be cited
# later as if a fleet had been accepted.
#
# It runs in two layers because the failures live in two languages. The node
# core judges synthetic cells, so it catches every *verdict* defect. It cannot
# catch a defect in the shell that PRODUCES a cell: those fixtures are typed
# by hand and never run this file. One such defect shipped — the drain loop's
# unguarded `pgrep` aborted the census under `set -euo pipefail` on precisely
# the healthy path — and the node fixtures stayed green through all of it,
# because a census that dies emits no cell for them to judge. So the shell
# fragments that decide whether a cell exists are tested here, in shell.
if [[ $ctxmux_fleet_self_test == true ]]
then
  echo "== fleet-scale harness self-test =="
  ctxmux_fleet_shell_self_test
  echo
  exec node scripts/fleet-scale-measure.mjs --self-test
fi

if [[ -z $ctxmux_fleet_profile ]]
then
  echo "error: a profile is required unless --self-test is given" >&2
  ctxmux_fleet_usage
  exit 2
fi

case $ctxmux_fleet_profile in
smoke | accept) ;;
*)
  echo "error: unknown profile '$ctxmux_fleet_profile'" >&2
  ctxmux_fleet_usage
  exit 2
  ;;
esac

# The census helper that runs where the daemon runs. It is a generated,
# self-contained shell snippet, NOT repository code: the constraint is that the
# repository is not cloned or copied to the farm, and a measurement helper
# emitted here and piped over ssh clones nothing. It drives one daemon, creates
# `target` Runs of the given kind, samples the daemon process the way
# scripts/reliability-qualification.ts does on Linux (procfs for descriptors and
# threads, ps for rss and cpu), exercises List across the whole fleet, probes
# admission at the ceiling, then stops every Run and samples cleanup. It prints
# one JSON census cell on its final stdout line so the caller can assemble a
# receipt. Paths are written as unquoted assignments so the commit checker's
# absolute-path scan does not read a procfs path as a leaked host path.
ctxmux_fleet_census_helper() {
  cat <<'CENSUS'
set -euo pipefail
proc=/proc
mode=$1
target=$2
ctxmuxd_bin=$3
ctxmux_bin=$4
run_program=$5
work=$6

sock="$work/ctxmux.sock"
statedir="$work/state"
# The daemon requires the state directory to be exactly 0700 and refuses to
# start otherwise. A plain mkdir -p inherits the login umask, which on a farm
# node is commonly 0022 and yields 0755 — so the mode is set explicitly rather
# than left to the environment.
mkdir -p "$statedir"
chmod 700 "$statedir"

# Deny the client every route to substituting its own daemon.
#
# `ctxmux` connect-or-spawns: any command, ping included, starts its own
# ctxmuxd when the socket does not answer, passing --socket alone and no
# --state-dir. Our daemon takes tens of milliseconds to bind, so the very first
# readiness ping lands in that window, spawns a rival, and the rival wins the
# bind — our daemon then exits with "already listening" and the census measures
# a process it never configured. It picks the daemon to spawn by looking for a
# ctxmuxd sibling of the client binary and then on PATH, so the client is run
# from a directory holding nothing else, with PATH emptied for those calls. The
# spawn then cannot resolve a daemon at all, ping simply fails, and the loop
# waits for OUR daemon instead of racing a replacement into existence.
client_dir="$work/client"
mkdir -p "$client_dir"
cp "$ctxmux_bin" "$client_dir/ctxmux"
chmod +x "$client_dir/ctxmux"
ctxmux_bin="$client_dir/ctxmux"
ctxmux() { PATH= "$ctxmux_bin" "$@"; }

# Sample the daemon: rss (KiB) and cpu seconds from ps, threads and descriptors
# from procfs. This mirrors the Linux census path in the repository's
# qualification harness, so the 128 tier is directly comparable to the gate.
sample() {
  local pid=$1
  local rss cpu threads fds
  read -r rss cpu < <(ps -o rss=,time= -p "$pid" | awk 'NR==1{print $1, $NF}')
  threads=$(ls "$proc/$pid/task" 2>/dev/null | wc -l | tr -d ' ')
  fds=$(ls "$proc/$pid/fd" 2>/dev/null | wc -l | tr -d ' ')
  echo "$rss|$cpu|$threads|$fds"
}

"$ctxmuxd_bin" --socket "$sock" --state-dir "$statedir" >"$work/daemon.log" 2>&1 &
daemon_pid=$!
# Kill whatever actually holds the socket, not merely the pid we launched, so
# nothing outlives this census and taints the next round.
cleanup_daemon() {
  kill "$daemon_pid" 2>/dev/null || true
  wait "$daemon_pid" 2>/dev/null || true
  pkill -f "ctxmuxd --socket $sock" 2>/dev/null || true
}
trap cleanup_daemon EXIT

# Wait for readiness. A green ping alone is not proof that OUR daemon answered,
# so the loop also requires our process to still be alive, and surfaces the
# daemon log when it is not — an empty measurement that looks real is the worst
# outcome available here.
ready=false
for _ in $(seq 1 100); do
  if ! kill -0 "$daemon_pid" 2>/dev/null; then
    break
  fi
  if ctxmux --socket "$sock" ping >/dev/null 2>&1; then ready=true; break; fi
  sleep 0.1
done
if [[ $ready != true ]] || ! kill -0 "$daemon_pid" 2>/dev/null; then
  reason=$(tr -d '"\\' <"$work/daemon.log" 2>/dev/null | tr '\n' ' ' | cut -c1-400)
  printf '{"error":"the census daemon never became ready","daemon_log":"%s"}\n' "$reason"
  exit 1
fi

baseline_raw=$(sample "$daemon_pid")
baseline_rss=${baseline_raw%%|*}
rest=${baseline_raw#*|}
baseline_cpu_raw=${rest%%|*}
rest=${rest#*|}
baseline_threads=${rest%%|*}
baseline_fds=${rest##*|}

# Create Runs and count how many the daemon actually admitted. A daemon whose
# record cap is 128 refuses beyond it with run_capacity; the count of clean
# refusals and the presence of any EMFILE are both recorded, because admission
# must refuse cleanly and never exhaust descriptors opaquely.
admitted=0
refused_clean=0
emfile=0
admission_error=""
for _ in $(seq 1 "$target"); do
  if out=$(ctxmux --socket "$sock" start -- "$run_program" 2>"$work/start.err"); then
    admitted=$((admitted + 1))
  else
    err=$(cat "$work/start.err")
    if grep -qi "too many open files" <<<"$err"; then
      emfile=1
      admission_error=$err
    elif grep -qi "run_capacity\|retained Run capacity" <<<"$err"; then
      refused_clean=$((refused_clean + 1))
      admission_error=$err
    else
      admission_error=$err
    fi
    break
  fi
done

# If the fleet reached its target, probe one more admission to observe ceiling
# behaviour explicitly rather than inferring it.
if [[ $admitted -ge $target ]]; then
  if out=$(ctxmux --socket "$sock" start -- "$run_program" 2>"$work/start.err"); then
    :
  else
    err=$(cat "$work/start.err")
    if grep -qi "too many open files" <<<"$err"; then emfile=1; admission_error=$err
    elif grep -qi "run_capacity\|retained Run capacity" <<<"$err"; then refused_clean=$((refused_clean + 1)); admission_error=$err
    else admission_error=$err; fi
  fi
fi

# For active runs, drive input to each Run so retained output is non-trivial.
if [[ $mode == active ]]; then
  while IFS=$'\t' read -r rid _; do
    ctxmux --socket "$sock" input "$rid" "aaaa" >/dev/null 2>&1 || true
  done < <(ctxmux --socket "$sock" list 2>/dev/null)
fi

sleep 1

# List across the whole fleet, timing it and confirming it enumerated every Run.
list_start=$(date +%s.%N)
listing=$(ctxmux --socket "$sock" list 2>"$work/list.err") && list_ok=true || list_ok=false
list_end=$(date +%s.%N)
list_count=$(printf '%s\n' "$listing" | grep -c . || true)
list_latency=$(awk "BEGIN{printf \"%.3f\", ($list_end - $list_start) * 1000}")
list_success=false
if [[ $list_ok == true && $list_count -ge $admitted ]]; then list_success=true; fi

# Total output each Run has produced over its lifetime, summed from the head
# byte counter in its listing row.
#
# This is deliberately NOT named "retained": `head=` is latest_output_bytes, a
# cumulative counter that only ever climbs, whereas what the retention budget
# bounds is OutputLog::retained_bytes — what the daemon is still holding right
# now, capped per-Run at 4 MiB and fleet-wide at 1 GiB. A Run that streamed a
# gigabyte and had it trimmed reports a gigabyte here while retaining almost
# nothing. The two numbers diverge without limit as a fleet ages.
#
# The darwin gate measures the real quantity (it sums replay lengths per Run),
# so the same field name previously described two different measurements in two
# harnesses, with only this one being the wrong one. Naming it for what it is
# keeps it from being graded against a retention ceiling it cannot satisfy the
# meaning of. Proving the fleet-wide retention cap from here needs
# retained_bytes on the wire, which the protocol does not carry today.
aggregate_bytes=$(printf '%s\n' "$listing" | sed -n 's/.*head=\([0-9]*\).*/\1/p' | awk '{s += $1} END {print s + 0}')

# The steady sample is the one every per-Run cost is derived from, so the
# daemon that served the fleet must still be the daemon we launched. If ours
# died partway, the client will have substituted its own and the numbers below
# would describe a process we never configured — refuse instead of reporting
# them.
if ! kill -0 "$daemon_pid" 2>/dev/null; then
  reason=$(tr -d '"\\' <"$work/daemon.log" 2>/dev/null | tr '\n' ' ' | cut -c1-400)
  printf '{"error":"the census daemon died before the steady sample","daemon_log":"%s"}\n' "$reason"
  exit 1
fi

steady_raw=$(sample "$daemon_pid")
steady_rss=${steady_raw%%|*}
rest=${steady_raw#*|}
steady_cpu_raw=${rest%%|*}
rest=${rest#*|}
steady_threads=${rest%%|*}
steady_fds=${rest##*|}

# Measure idle CPU over a dedicated window, after the fleet has settled.
#
# This used to subtract two `ps -o time=` readings taken across the whole fill
# and divide by a hardcoded elapsed=1. Two things were wrong with that. ps TIME
# has one-second granularity, so every result was a multiple of 100 — the
# 2026-09-06 farm run reported 0/100/800/2900/4400 and nothing between. And the
# window spanned Run creation, so it scored the cost of building the fleet, not
# the cost of holding it. Idle CPU is the number that matters for a fleet that
# sits there: it is what the tmux comparison turns on.
#
# The stat file's fields 14 and 15 are utime and stime in clock ticks, which
# is finer than a second, and the window is timed rather than assumed.
cpu_ticks() {
  awk '{print $14 + $15}' "$proc/$1/stat" 2>/dev/null || echo 0
}
clock_ticks=$(getconf CLK_TCK 2>/dev/null || echo 100)
idle_window=5
idle_start_ticks=$(cpu_ticks "$daemon_pid")
idle_start=$(date +%s.%N)
sleep "$idle_window"
idle_end_ticks=$(cpu_ticks "$daemon_pid")
idle_end=$(date +%s.%N)
cpu_core_percent=$(awk "BEGIN{
  d = $idle_end_ticks - $idle_start_ticks;
  if (d < 0) d = 0;
  secs = d / $clock_ticks;
  wall = $idle_end - $idle_start;
  if (wall <= 0) wall = $idle_window;
  printf \"%.3f\", secs / wall * 100
}")

divide() { awk "BEGIN{v=$1; if(v<0)v=0; printf \"%.3f\", v/$admitted}"; }
if [[ $admitted -lt 1 ]]; then admitted=0; fi
rss_per_run=0
fds_per_run=0
threads_per_run=0
retained_per_run=0
if [[ $admitted -ge 1 ]]; then
  rss_per_run=$(divide "$steady_rss - $baseline_rss")
  fds_per_run=$(divide "$steady_fds - $baseline_fds")
  threads_per_run=$(divide "$steady_threads - $baseline_threads")
  retained_per_run=$(divide "$aggregate_bytes")
fi

# Stop every Run, then sample what teardown actually released.
#
# `stop` is the command that terminates a Run; `remove` refuses one that is
# still running ("still running; stop it before removing") and only discards an
# already-terminal record. Calling remove here and discarding its error meant
# every teardown failed silently and every child survived, which is why
# cleanup_live_children read N+1 at every tier. Failures are counted rather
# than swallowed: a teardown that cannot stop its Runs must be visible in the
# cell, not inferred from a leak counter downstream.
stop_failures=0
while IFS=$'\t' read -r rid _; do
  if ! ctxmux --socket "$sock" stop "$rid" >>"$work/stop.err" 2>&1; then
    stop_failures=$((stop_failures + 1))
  fi
done < <(ctxmux --socket "$sock" list 2>/dev/null)

# Children are reaped asynchronously after stop returns, so poll for the drain
# instead of assuming a fixed sleep is long enough. A fleet that never drains
# leaves the last observed count in place and fails the zero check downstream.
#
# `pgrep` exits 1 when it matches nothing, and under `set -euo pipefail` that
# status propagates through the pipeline and aborts the script. Zero children
# is the *healthy* outcome here — a fully drained fleet — so the unguarded form
# killed the census at exactly the moment it was about to record a pass, before
# any cell was emitted, and reported it as a helper failure rather than a
# result. `|| true` is therefore load-bearing, not defensive noise.
#
# This was latent until #59 made teardown actually stop Runs. While `remove`
# was silently failing, children never drained, `pgrep` always matched, and the
# abort could not fire. Fixing the leak is what armed this.
count_children() { { pgrep -P "$1" 2>/dev/null || true; } | wc -l | tr -d ' '; }
for _ in $(seq 1 100); do
  cleanup_children=$(count_children "$daemon_pid")
  if [[ ${cleanup_children:-0} -eq 0 ]]; then break; fi
  sleep 0.1
done

cleanup_raw=$(sample "$daemon_pid")
cleanup_threads=$(printf '%s' "$cleanup_raw" | awk -F'|' '{print $3}')
cleanup_attachments=$(printf '%s\n' "$(ctxmux --socket "$sock" list 2>/dev/null)" | sed -n 's/.*attachments=\([0-9]*\).*/\1/p' | awk '{s+=$1} END{print s+0}')
cleanup_children=$(count_children "$daemon_pid")

# Everything sampled since the guard at the steady sample assumed the daemon was
# still alive, and every one of those readings degrades to a *passing* value if
# it was not. A dead pid has no children, so `pgrep -P` reports zero leaked
# children; its stat file is gone, so `cpu_ticks` falls back to 0 and idle CPU
# computes as a perfect 0.000; `list` fails, so the teardown loop iterates zero
# times and records zero stop failures. A daemon that died mid-census therefore
# produces a cell that is not merely green but *better* than a healthy one.
#
# So liveness is recorded as a measurement in its own right rather than trusted.
# The verdict must fail closed when it is false or absent — an older receipt
# that predates this field is a receipt whose zeros were never corroborated.
daemon_alive_after_census=true
kill -0 "$daemon_pid" 2>/dev/null || daemon_alive_after_census=false

peak_rss=$steady_rss
if [[ $baseline_rss -gt $peak_rss ]]; then peak_rss=$baseline_rss; fi

printf '{'
printf '"admitted_runs":%s,' "$admitted"
printf '"daemon_alive_after_census":%s,' "$daemon_alive_after_census"
printf '"cpu_core_percent":%s,' "$cpu_core_percent"
printf '"peak_rss_kib":%s,' "$peak_rss"
printf '"output_bytes_lifetime_per_run":%s,' "$retained_per_run"
printf '"rss_kib_per_run":%s,' "$rss_per_run"
printf '"threads_per_run":%s,' "$threads_per_run"
printf '"fds_per_run":%s,' "$fds_per_run"
printf '"cleanup_live_children":%s,' "$cleanup_children"
printf '"cleanup_stop_failures":%s,' "$stop_failures"
printf '"cleanup_attachments":%s,' "$cleanup_attachments"
printf '"steady":{"rss_kib":%s},' "$steady_rss"
printf '"baseline":{"threads":%s},' "$baseline_threads"
printf '"cleanup":{"threads":%s},' "$cleanup_threads"
printf '"list_latency_ms":%s,' "$list_latency"
printf '"list_success":%s,' "$list_success"
printf '"aggregate_output_bytes_lifetime":%s,' "$aggregate_bytes"
if [[ $refused_clean -ge 1 || $emfile -eq 1 ]]; then
  refused_bool=false
  if [[ $refused_clean -ge 1 ]]; then refused_bool=true; fi
  emfile_bool=false
  if [[ $emfile -eq 1 ]]; then emfile_bool=true; fi
  printf '"admission_at_ceiling":{"refused_cleanly":%s,"emfile":%s,"error_code":"run_capacity"}' "$refused_bool" "$emfile_bool"
else
  printf '"admission_at_ceiling":null'
fi
printf '}\n'
CENSUS
}

# Cross-compile the daemon and client from THIS worktree, stripped. Building
# from the main checkout has already silently produced pre-change measurements
# once; building from here is what ties the numbers to the change under review.
ctxmux_fleet_cross_build() {
  echo "== cross-building stripped Linux binaries from this worktree ==" >&2
  RUSTFLAGS="-C strip=symbols -C debuginfo=0" \
    cargo zigbuild --release --target x86_64-unknown-linux-gnu \
    --package ctxmux-daemon --bin ctxmuxd >&2
  RUSTFLAGS="-C strip=symbols -C debuginfo=0" \
    cargo zigbuild --release --target x86_64-unknown-linux-gnu \
    --package ctxmux --bin ctxmux >&2
}

# Copy one binary to the farm and confirm its SHA-256 is identical on both ends
# BEFORE it is ever run. A transfer silently truncated earlier this session
# (fewer bytes arrived than were sent) still reported success; a truncated
# binary produces a bogus measurement indistinguishable from a real one, so the
# transfer is verified rather than trusted.
ctxmux_fleet_ship_verified() {
  local local_path=$1
  local remote_path=$2
  local dest=$3
  local local_sha remote_sha
  local_sha=$(shasum -a 256 "$local_path" | cut -d' ' -f1)
  COPYFILE_DISABLE=1 tar czf - -C "$(dirname "$local_path")" "$(basename "$local_path")" \
    | ssh "$dest" "mkdir -p \"$(dirname "$remote_path")\" && tar xzf - -C \"$(dirname "$remote_path")\" && chmod +x \"$remote_path\""
  remote_sha=$(ssh "$dest" "sha256sum \"$remote_path\" | cut -d' ' -f1")
  if [[ $local_sha != "$remote_sha" ]]; then
    echo "error: SHA-256 mismatch shipping $(basename "$local_path"): local $local_sha remote $remote_sha" >&2
    echo "A truncated or altered binary produces a bogus measurement that looks real. Refusing." >&2
    return 1
  fi
  echo "verified $(basename "$local_path") on the farm (sha256 $local_sha)" >&2
}

# Run the census helper on one host over ssh (accept) or locally (smoke),
# returning the JSON census cell on stdout.
#
# `| tail -1` would make the pipeline's status the tail's, so a census that
# died would look like a success carrying an error object. Downstream
# validation does refuse such a cell, but it would name a missing field rather
# than the actual cause. PIPESTATUS is inspected instead so the helper's own
# failure is reported where it happened, with the reason it emitted.
ctxmux_fleet_census_status() {
  local status=$1 cell=$2 where=$3
  if [[ $status -ne 0 ]]
  then
    echo "error: the census helper failed on $where (exit $status)" >&2
    echo "  it reported: ${cell:-<no output>}" >&2
    return 1
  fi
  printf '%s' "$cell"
}
ctxmux_fleet_run_census_remote() {
  local dest=$1 mode=$2 target=$3 ctxmuxd_bin=$4 ctxmux_bin=$5 run_program=$6 remote_work=$7
  local cell status
  cell=$(
    ctxmux_fleet_census_helper \
      | ssh "$dest" "bash -s -- '$mode' '$target' '$ctxmuxd_bin' '$ctxmux_bin' '$run_program' '$remote_work'" \
      | tail -1
    exit "${PIPESTATUS[1]}"
  )
  status=$?
  ctxmux_fleet_census_status "$status" "$cell" "$dest ($mode/$target)"
}
ctxmux_fleet_run_census_local() {
  local mode=$1 target=$2 ctxmuxd_bin=$3 ctxmux_bin=$4 run_program=$5 local_work=$6
  local cell status
  cell=$(
    ctxmux_fleet_census_helper \
      | bash -s -- "$mode" "$target" "$ctxmuxd_bin" "$ctxmux_bin" "$run_program" "$local_work" \
      | tail -1
    exit "${PIPESTATUS[1]}"
  )
  status=$?
  ctxmux_fleet_census_status "$status" "$cell" "this host ($mode/$target)"
}

if [[ $ctxmux_fleet_profile == smoke ]]
then
  echo "== fleet-scale smoke: local drive/census/derive/verdict path =="
  echo "NOTE: the local host is not the farm; these numbers exercise the harness, not the fleet." >&2

  if [[ $(uname -s) != Linux ]]
  then
    echo "fleet-scale smoke needs a Linux host for the procfs census; this host is $(uname -s)." >&2
    echo "The census reads the per-process descriptor and task directories that only Linux exposes." >&2
    echo "Run --self-test here instead (host-agnostic); run smoke on the farm-class host." >&2
    exit 3
  fi

  cargo build --locked --quiet --package ctxmux-daemon --bin ctxmuxd
  cargo build --locked --quiet --package ctxmux --bin ctxmux
  smoke_ctxmuxd=$PWD/target/debug/ctxmuxd
  smoke_ctxmux=$PWD/target/debug/ctxmux
  smoke_run_program=/bin/cat

  work_root="${TMPDIR:-/tmp}"
  smoke_work=$(mktemp -d "$work_root/ctxmux-fleet-smoke.XXXXXX")
  trap 'rm -rf "$smoke_work"' EXIT

  # A tiny local fleet at one tier, three rounds, both modes: just enough to
  # prove the derive-and-verdict path runs end to end. Below the daemon cap so
  # every Run is admitted; the smoke's job is the plumbing, not the scale.
  smoke_target=4
  smoke_obs="$smoke_work/observations.json"
  {
    printf '{"host":{"os":"linux","os_release":"%s","architecture":"x64","logical_cpus":%s},"modes":{' \
      "$(uname -r)" "$(nproc)"
    first_mode=true
    for mode in idle active; do
      if [[ $first_mode == true ]]; then first_mode=false; else printf ','; fi
      printf '"%s":{"%s":[' "$mode" "$smoke_target"
      for round in 1 2 3; do
        if [[ $round -gt 1 ]]; then printf ','; fi
        rwork=$(mktemp -d "$smoke_work/r.XXXXXX")
        cell=$(ctxmux_fleet_run_census_local "$mode" "$smoke_target" "$smoke_ctxmuxd" "$smoke_ctxmux" "$smoke_run_program" "$rwork")
        printf '%s' "$cell"
      done
      printf ']}'
    done
    printf '}}'
  } > "$smoke_obs"

  # Derive-and-check the smoke observations through the same functions the
  # accept profile uses. The smoke tier (4) is deliberately not one of the
  # acceptance tiers, so the smoke asserts the derivation rules produce ceilings
  # from complete census cells; the real per-tier verdict is the accept
  # profile's job.
  CTXMUX_FLEET_SMOKE_TIER=$smoke_target \
    node scripts/fleet-scale-measure.mjs --mode smoke-check --observations "$smoke_obs"

  if [[ -n $ctxmux_fleet_json ]]; then
    cp "$smoke_obs" "$ctxmux_fleet_json"
    echo "smoke observations written to $ctxmux_fleet_json"
  fi
  echo "fleet-scale smoke complete"
  exit 0
fi

# ---- accept profile: the real fleet-scale acceptance on the farm ----
# The destination is an ssh alias, overridable because farm placement is the
# farm's policy and not this repo's: new validation is currently steered to the
# canary workers so the CN farm server's existing development work is not
# disturbed. Pinning one host here would silently outlive that policy.
ctxmux_fleet_dest=${CTXMUX_FLEET_HOST:-cn4}
echo "== fleet-scale accept: driving the farm node $ctxmux_fleet_dest over ssh =="

if ! ssh -o BatchMode=yes -o ConnectTimeout=10 "$ctxmux_fleet_dest" true 2>/dev/null
then
  echo "error: the farm node $ctxmux_fleet_dest is not reachable over ssh (BatchMode)." >&2
  echo "Acceptance needs the farm; it never runs the fleet on this Mac." >&2
  exit 3
fi

# A remote host class the farm thresholds will be bound to and enforced against.
ctxmux_fleet_remote_os=$(ssh "$ctxmux_fleet_dest" "uname -s")
if [[ $ctxmux_fleet_remote_os != Linux ]]
then
  echo "error: the farm node reports $ctxmux_fleet_remote_os, not Linux; the census reads procfs." >&2
  exit 3
fi

ctxmux_fleet_cross_build

ctxmux_fleet_local_ctxmuxd=$PWD/target/x86_64-unknown-linux-gnu/release/ctxmuxd
ctxmux_fleet_local_ctxmux=$PWD/target/x86_64-unknown-linux-gnu/release/ctxmux
for bin in "$ctxmux_fleet_local_ctxmuxd" "$ctxmux_fleet_local_ctxmux"
do
  if [[ ! -x $bin ]]; then
    echo "error: expected a built binary at $bin" >&2
    exit 1
  fi
done

ctxmux_fleet_stamp=$(date +%s)-$$
ctxmux_fleet_remote_root=/tmp/ctxmux-fleet-scale
ctxmux_fleet_remote_work="$ctxmux_fleet_remote_root/$ctxmux_fleet_stamp"
ctxmux_fleet_remote_ctxmuxd="$ctxmux_fleet_remote_work/ctxmuxd"
ctxmux_fleet_remote_ctxmux="$ctxmux_fleet_remote_work/ctxmux"

# Remove everything this run leaves on the farm, whatever the outcome.
ctxmux_fleet_cleanup_remote() {
  ssh "$ctxmux_fleet_dest" "rm -rf \"$ctxmux_fleet_remote_work\"" 2>/dev/null || true
}
trap ctxmux_fleet_cleanup_remote EXIT

ssh "$ctxmux_fleet_dest" "mkdir -p \"$ctxmux_fleet_remote_work\""
ctxmux_fleet_ship_verified "$ctxmux_fleet_local_ctxmuxd" "$ctxmux_fleet_remote_ctxmuxd" "$ctxmux_fleet_dest"
ctxmux_fleet_ship_verified "$ctxmux_fleet_local_ctxmux" "$ctxmux_fleet_remote_ctxmux" "$ctxmux_fleet_dest"

ctxmux_fleet_remote_uname_r=$(ssh "$ctxmux_fleet_dest" "uname -r")
ctxmux_fleet_remote_cpus=$(ssh "$ctxmux_fleet_dest" "nproc")
ctxmux_fleet_run_program=/bin/cat

work_root="${TMPDIR:-/tmp}"
ctxmux_fleet_local_work=$(mktemp -d "$work_root/ctxmux-fleet-accept.XXXXXX")
trap 'ctxmux_fleet_cleanup_remote; rm -rf "$ctxmux_fleet_local_work"' EXIT

ctxmux_fleet_obs="$ctxmux_fleet_local_work/observations.json"
ctxmux_fleet_receipt="$ctxmux_fleet_local_work/receipt.json"
ctxmux_fleet_thresholds="$ctxmux_fleet_local_work/thresholds.json"

# Three rounds per tier per mode become the observations that derive the
# thresholds; a separate final round becomes the receipt judged against them.
# Each census runs in its own remote working subdirectory so a daemon's state
# never bleeds into the next round.
echo "== running observation rounds on the farm ==" >&2
{
  printf '{"host":{"os":"linux","os_release":"%s","architecture":"x64","logical_cpus":%s},"modes":{' \
    "$ctxmux_fleet_remote_uname_r" "$ctxmux_fleet_remote_cpus"
  first_mode=true
  for mode in idle active; do
    if [[ $first_mode == true ]]; then first_mode=false; else printf ','; fi
    printf '"%s":{' "$mode"
    first_tier=true
    for tier in 128 512 2048 4000; do
      if [[ $first_tier == true ]]; then first_tier=false; else printf ','; fi
      printf '"%s":[' "$tier"
      for round in 1 2 3; do
        if [[ $round -gt 1 ]]; then printf ','; fi
        rwork="$ctxmux_fleet_remote_work/$mode-$tier-$round"
        ssh "$ctxmux_fleet_dest" "mkdir -p \"$rwork\""
        cell=$(ctxmux_fleet_run_census_remote "$ctxmux_fleet_dest" "$mode" "$tier" \
          "$ctxmux_fleet_remote_ctxmuxd" "$ctxmux_fleet_remote_ctxmux" \
          "$ctxmux_fleet_run_program" "$rwork")
        echo "  $mode/$tier round $round: $cell" >&2
        printf '%s' "$cell"
        ssh "$ctxmux_fleet_dest" "rm -rf \"$rwork\"" 2>/dev/null || true
      done
      printf ']'
    done
    printf '}'
  done
  printf '}}'
} > "$ctxmux_fleet_obs"

echo "== deriving farm thresholds ==" >&2
node scripts/fleet-scale-measure.mjs --mode derive \
  --observations "$ctxmux_fleet_obs" --out "$ctxmux_fleet_thresholds"

# Build the receipt: one fresh census per tier/mode, judged against the derived
# thresholds. Structurally the receipt is one round per cell.
echo "== running the receipt round on the farm ==" >&2
{
  printf '{"host":{"os":"linux","os_release":"%s","architecture":"x64","logical_cpus":%s},"modes":{' \
    "$ctxmux_fleet_remote_uname_r" "$ctxmux_fleet_remote_cpus"
  first_mode=true
  for mode in idle active; do
    if [[ $first_mode == true ]]; then first_mode=false; else printf ','; fi
    printf '"%s":{' "$mode"
    first_tier=true
    for tier in 128 512 2048 4000; do
      if [[ $first_tier == true ]]; then first_tier=false; else printf ','; fi
      rwork="$ctxmux_fleet_remote_work/receipt-$mode-$tier"
      ssh "$ctxmux_fleet_dest" "mkdir -p \"$rwork\""
      cell=$(ctxmux_fleet_run_census_remote "$ctxmux_fleet_dest" "$mode" "$tier" \
        "$ctxmux_fleet_remote_ctxmuxd" "$ctxmux_fleet_remote_ctxmux" \
        "$ctxmux_fleet_run_program" "$rwork")
      printf '"%s":%s' "$tier" "$cell"
      ssh "$ctxmux_fleet_dest" "rm -rf \"$rwork\"" 2>/dev/null || true
    done
    printf '}'
  done
  printf '}}'
} > "$ctxmux_fleet_receipt"

echo "== rendering the acceptance verdict ==" >&2
ctxmux_fleet_verdict_args=(--mode verdict
  --thresholds "$ctxmux_fleet_thresholds"
  --observations "$ctxmux_fleet_receipt"
  --root "$PWD")
if [[ -n $ctxmux_fleet_json ]]; then
  ctxmux_fleet_verdict_args+=(--out "$ctxmux_fleet_json")
fi
node scripts/fleet-scale-measure.mjs "${ctxmux_fleet_verdict_args[@]}"
echo "fleet-scale acceptance verdict rendered"
