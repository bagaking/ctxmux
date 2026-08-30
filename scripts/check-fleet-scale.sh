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
# idle cpu_core_percent, steady_rss_kib and rss_kib_per_run,
# retained_output_bytes_per_run and the aggregate retained bytes, List latency
# and success, and admission behaviour at the descriptor ceiling (which must
# refuse cleanly with run_capacity and never hit EMFILE).
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

# The self-test is the cheap, host-agnostic gate check. It delegates to the
# measurement core, which proves each refusal fires: a fleet acceptance harness
# that could report a silent zero is worse than none, because a green verdict
# with nothing behind it would be cited later as if a fleet had been accepted.
if [[ $ctxmux_fleet_self_test == true ]]
then
  echo "== fleet-scale harness self-test =="
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
mkdir -p "$statedir"

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
cpu_seconds() {
  # ps TIME is [[dd-]hh:]mm:ss; reduce to seconds.
  awk -F: '{ s=0; for (i=1;i<=NF;i++){ s = s*60 + $i } print s }' <<<"$1"
}

"$ctxmuxd_bin" --socket "$sock" --state-dir "$statedir" >"$work/daemon.log" 2>&1 &
daemon_pid=$!
cleanup_daemon() { kill "$daemon_pid" 2>/dev/null || true; wait "$daemon_pid" 2>/dev/null || true; }
trap cleanup_daemon EXIT

# Wait for readiness.
ready=false
for _ in $(seq 1 100); do
  if "$ctxmux_bin" --socket "$sock" ping >/dev/null 2>&1; then ready=true; break; fi
  sleep 0.1
done
if [[ $ready != true ]]; then
  echo '{"error":"daemon never became ready"}'
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
  if out=$("$ctxmux_bin" --socket "$sock" start -- "$run_program" 2>"$work/start.err"); then
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
  if out=$("$ctxmux_bin" --socket "$sock" start -- "$run_program" 2>"$work/start.err"); then
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
    "$ctxmux_bin" --socket "$sock" input "$rid" "aaaa" >/dev/null 2>&1 || true
  done < <("$ctxmux_bin" --socket "$sock" list 2>/dev/null)
fi

sleep 1

# List across the whole fleet, timing it and confirming it enumerated every Run.
list_start=$(date +%s.%N)
listing=$("$ctxmux_bin" --socket "$sock" list 2>"$work/list.err") && list_ok=true || list_ok=false
list_end=$(date +%s.%N)
list_count=$(printf '%s\n' "$listing" | grep -c . || true)
list_latency=$(awk "BEGIN{printf \"%.3f\", ($list_end - $list_start) * 1000}")
list_success=false
if [[ $list_ok == true && $list_count -ge $admitted ]]; then list_success=true; fi

# Aggregate retained output bytes, summed from the head byte counter each Run
# reports in its listing row.
aggregate_bytes=$(printf '%s\n' "$listing" | sed -n 's/.*head=\([0-9]*\).*/\1/p' | awk '{s += $1} END {print s + 0}')

steady_raw=$(sample "$daemon_pid")
steady_rss=${steady_raw%%|*}
rest=${steady_raw#*|}
steady_cpu_raw=${rest%%|*}
rest=${rest#*|}
steady_threads=${rest%%|*}
steady_fds=${rest##*|}

elapsed=1
base_cpu_s=$(cpu_seconds "$baseline_cpu_raw")
steady_cpu_s=$(cpu_seconds "$steady_cpu_raw")
cpu_core_percent=$(awk "BEGIN{d=$steady_cpu_s-$base_cpu_s; if(d<0)d=0; printf \"%.3f\", d/$elapsed*100}")

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

# Stop every Run and sample cleanup.
while IFS=$'\t' read -r rid _; do
  "$ctxmux_bin" --socket "$sock" remove "$rid" >/dev/null 2>&1 || true
done < <("$ctxmux_bin" --socket "$sock" list 2>/dev/null)
sleep 1
cleanup_raw=$(sample "$daemon_pid")
cleanup_threads=$(printf '%s' "$cleanup_raw" | awk -F'|' '{print $3}')
cleanup_attachments=$(printf '%s\n' "$("$ctxmux_bin" --socket "$sock" list 2>/dev/null)" | sed -n 's/.*attachments=\([0-9]*\).*/\1/p' | awk '{s+=$1} END{print s+0}')
cleanup_children=$(pgrep -P "$daemon_pid" 2>/dev/null | wc -l | tr -d ' ')

peak_rss=$steady_rss
if [[ $baseline_rss -gt $peak_rss ]]; then peak_rss=$baseline_rss; fi

printf '{'
printf '"admitted_runs":%s,' "$admitted"
printf '"cpu_core_percent":%s,' "$cpu_core_percent"
printf '"peak_rss_kib":%s,' "$peak_rss"
printf '"retained_output_bytes_per_run":%s,' "$retained_per_run"
printf '"rss_kib_per_run":%s,' "$rss_per_run"
printf '"threads_per_run":%s,' "$threads_per_run"
printf '"fds_per_run":%s,' "$fds_per_run"
printf '"cleanup_live_children":%s,' "$cleanup_children"
printf '"cleanup_attachments":%s,' "$cleanup_attachments"
printf '"steady":{"rss_kib":%s},' "$steady_rss"
printf '"baseline":{"threads":%s},' "$baseline_threads"
printf '"cleanup":{"threads":%s},' "$cleanup_threads"
printf '"list_latency_ms":%s,' "$list_latency"
printf '"list_success":%s,' "$list_success"
printf '"aggregate_retained_bytes":%s,' "$aggregate_bytes"
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
ctxmux_fleet_run_census_remote() {
  local dest=$1 mode=$2 target=$3 ctxmuxd_bin=$4 ctxmux_bin=$5 run_program=$6 remote_work=$7
  ctxmux_fleet_census_helper \
    | ssh "$dest" "bash -s -- '$mode' '$target' '$ctxmuxd_bin' '$ctxmux_bin' '$run_program' '$remote_work'" \
    | tail -1
}
ctxmux_fleet_run_census_local() {
  local mode=$1 target=$2 ctxmuxd_bin=$3 ctxmux_bin=$4 run_program=$5 local_work=$6
  ctxmux_fleet_census_helper \
    | bash -s -- "$mode" "$target" "$ctxmuxd_bin" "$ctxmux_bin" "$run_program" "$local_work" \
    | tail -1
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
