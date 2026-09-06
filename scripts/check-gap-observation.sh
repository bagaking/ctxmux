#!/usr/bin/env bash

# T-009 contract gate. Only private test Runs are created. Every invocation
# uses a fresh Rust output directory; counterfactual SDK source/output is separate.
# Receipts, including expected counterfactual failures, are retained on failure.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
ctxmux_gap_root=$PWD
ctxmux_gap_receipts=${CTXMUX_GAP_GATE_OUTPUT:-"$PWD/target/gap-observation-gates"}
mkdir -p "$ctxmux_gap_receipts"
ctxmux_gap_receipts=$(cd "$ctxmux_gap_receipts" && pwd -P)
ctxmux_gap_attempt=$(mktemp -d "$ctxmux_gap_receipts/attempt-XXXXXXXX")
export CARGO_TARGET_DIR="$ctxmux_gap_attempt/native-build"
# Avoid storing a second incremental copy of every crate in this one-shot lane.
# This is a build-cache policy, not a Run workload/resource policy.
export CARGO_INCREMENTAL=0
echo "Gap gate receipt: $ctxmux_gap_attempt"

run_check() {
  local name=$1
  shift
  if "$@" >"$ctxmux_gap_attempt/$name.log" 2>&1; then
    echo "PASS $name"
  else
    cat "$ctxmux_gap_attempt/$name.log" >&2
    echo "FAIL $name; preserved at $ctxmux_gap_attempt" >&2
    return 1
  fi
}

rust_tests() {
  local name=$1
  shift
  run_check "$name" cargo test --locked "$@"
  python3 - "$ctxmux_gap_attempt/$name.log" <<'PY'
import re, sys
log = open(sys.argv[1], encoding="utf-8").read()
results = re.findall(r"test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored", log)
assert results and sum(int(r[0]) for r in results) > 0, "zero Rust tests is not qualification"
assert all(int(r[1]) == 0 and int(r[2]) == 0 for r in results), "failed or ignored gate test"
PY
}

run_check rust-format cargo fmt --all --check
run_check prettier node node_modules/prettier/bin/prettier.cjs --check \
  packages/sdk/src/attachment.ts packages/sdk/src/client.ts \
  packages/sdk/src/gap-observation.ts packages/sdk/src/index.ts \
  packages/sdk/src/validation.ts packages/sdk/src/generated \
  packages/sdk/package.json packages/sdk/test/gap-observation.test.ts \
  packages/sdk/test/gap-observation.e2e.test.ts \
  packages/sdk/test/native-service.test.ts packages/sdk/test/wrong-cases.test.ts \
  docs/architecture.md docs/protocol.md docs/roadmap.md
run_check rust-static cargo clippy --locked --package ctxmux-protocol \
  --package ctxmux-client --package ctxmux-daemon --package ctxmux \
  --all-targets -- -D warnings
rust_tests protocol --package ctxmux-protocol --lib
rust_tests client --package ctxmux-client --lib
rust_tests daemon-gap --package ctxmux-daemon --lib gap_
rust_tests daemon-lag --package ctxmux-daemon --lib lag_
rust_tests daemon-terminal-join --package ctxmux-daemon --lib terminal_snapshot_
rust_tests daemon-replay-join --package ctxmux-daemon --lib subscribe_snapshot_join_
run_check tmux-available tmux -V
rust_tests daemon-tmux-source --package ctxmux-daemon --test tmux_adapter \
  public_pause_emits_exact_gap_and_requests_control_mode_continue -- --exact
rust_tests cli --package ctxmux --bin ctxmux
run_check generated-contract bash scripts/check-protocol-types.sh
run_check sdk-build node node_modules/typescript/bin/tsc -p packages/sdk/tsconfig.build.json
run_check sdk-static node node_modules/typescript/bin/tsc -p packages/sdk/tsconfig.json --noEmit
run_check sdk-units node node_modules/tsx/dist/cli.mjs --test \
  packages/sdk/test/index.test.ts packages/sdk/test/integration.test.ts \
  packages/sdk/test/shell-integration.test.ts packages/sdk/test/wrong-cases.test.ts \
  packages/sdk/test/parser-fuzz.test.ts packages/sdk/test/terminal-checkpoint.test.ts \
  packages/sdk/test/native-service.test.ts packages/sdk/test/foreground-transport.test.ts \
  packages/sdk/test/gap-observation.test.ts

# Reverse just cause union in a private source copy. Same tests, budgets and
# workload must reject last-origin-wins in both arrival orders.
ctxmux_gap_counter="$ctxmux_gap_attempt/last-origin-counterfactual"
mkdir -p "$ctxmux_gap_counter/packages/sdk"
cp -R packages/sdk/src packages/sdk/test "$ctxmux_gap_counter/packages/sdk/"
cp packages/sdk/tsconfig*.json "$ctxmux_gap_counter/packages/sdk/"
cp packages/sdk/package.json "$ctxmux_gap_counter/packages/sdk/"
ln -s "$ctxmux_gap_root/node_modules" "$ctxmux_gap_counter/node_modules"
python3 - "$ctxmux_gap_counter/packages/sdk/src/gap-observation.ts" <<'PY'
from pathlib import Path
import sys
p = Path(sys.argv[1]); source = p.read_text()
start = source.index("  return {", source.index("export function unionGapCauses("))
p.write_text(source[:start] + "  return right;\n}\n")
PY
run_check counterfactual-build node node_modules/typescript/bin/tsc \
  -p "$ctxmux_gap_counter/packages/sdk/tsconfig.build.json"
if node node_modules/tsx/dist/cli.mjs --test \
  "$ctxmux_gap_counter/packages/sdk/test/gap-observation.test.ts" \
  >"$ctxmux_gap_attempt/counterfactual-red.log" 2>&1; then
  echo "counterfactual unexpectedly passed; cause oracle is ineffective" >&2
  exit 1
fi
python3 - "$ctxmux_gap_attempt/counterfactual-red.log" <<'PY'
from pathlib import Path
import sys
log = Path(sys.argv[1]).read_text()
for order in ("daemon_then_client", "client_then_daemon"):
    assert f"✖ mixed Gap keeps both origins in {order}" in log, "missing causal assertion RED"
assert "false !== true" in log, "counterfactual must fail the cause oracle, not loading"
PY
echo "PASS source-counterfactual (both mixed-origin orders rejected)"

run_check native-build cargo build --locked --bin ctxmux --bin ctxmuxd
export CTXMUX_BIN="$CARGO_TARGET_DIR/debug/ctxmux"
export CTXMUXD_BIN="$CARGO_TARGET_DIR/debug/ctxmuxd"
export CTXMUX_GAP_RECEIPT="$ctxmux_gap_attempt/two-run.private.json"
run_check public-sdk-two-run node node_modules/tsx/dist/cli.mjs --test \
  packages/sdk/test/gap-observation.e2e.test.ts
run_check public-cli bash scripts/smoke-cli.sh
python3 - "$ctxmux_gap_attempt" "$ctxmux_gap_root" <<'PY'
from pathlib import Path
import hashlib, json, os, subprocess, sys
out, root = map(Path, sys.argv[1:])
for name in ("sdk-units", "public-sdk-two-run"):
    log = (out / (name + ".log")).read_text()
    assert "ℹ fail 0" in log and "ℹ skipped 0" in log and "ℹ tests 0\n" not in log, "invalid SDK qualification"
def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()
def git(*args): return subprocess.check_output(["git", "-C", str(root), *args]).decode().strip()
# A shared checkout's index may contain someone else's staged changes. Bind
# executed source to actual files as well as HEAD/diff; never reset their index.
source_files = hashlib.sha256()
different_from_commit = []
for row in subprocess.check_output(["git", "-C", str(root), "ls-tree", "-r", "-z", "HEAD"]).split(b"\0"):
    if not row: continue
    metadata, name = row.split(b"\t", 1)
    mode, kind, expected = metadata.split()
    if kind != b"blob": continue
    p = root / os.fsdecode(name)
    data = os.fsencode(os.readlink(p)) if p.is_symlink() else p.read_bytes()
    actual = hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest().encode()
    source_files.update(name + b"\0" + actual + b"\0")
    if actual != expected: different_from_commit.append(os.fsdecode(name))
binding = {
    "sourceCommit": git("rev-parse", "HEAD"),
    "sourceTree": git("rev-parse", "HEAD^{tree}"),
    "trackedDiffSha256": hashlib.sha256(subprocess.check_output(["git", "-C", str(root), "diff", "HEAD", "--binary"])).hexdigest(),
    "actualTrackedSourceSha256": source_files.hexdigest(),
    "actualTrackedFilesMatchCommit": not different_from_commit,
    "actualTrackedFilesDifferentFromCommit": different_from_commit,
    "daemonSha256": sha(out / "native-build/debug/ctxmuxd"),
    "cliSha256": sha(out / "native-build/debug/ctxmux"),
    "sdkIndexSha256": sha(root / "packages/sdk/dist/index.js"),
    "sdkAttachmentSha256": sha(root / "packages/sdk/dist/attachment.js"),
    "daemonVersion": subprocess.check_output([str(out / "native-build/debug/ctxmuxd"), "--version"]).decode().strip(),
    "qualification": "T-009 private 2-Run/2-Client; not whole-fleet or full P00 sign-off",
}
(out / "source-binding.private.json").write_text(json.dumps(binding, indent=2) + "\n")
PY
echo "PASS Gap contract gate; receipt: $ctxmux_gap_attempt"
