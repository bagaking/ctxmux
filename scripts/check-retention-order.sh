#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

# Freeze both source variants and use distinct, initially empty Cargo outputs.
# All PTY operations use the unit fixture's private socket and owned children.
python3 - <<'PY'
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

root = Path.cwd()
base = root / ".bagakit/feature-tracker/features/f-22vcz84zn/artifacts/retention-order"
base.mkdir(parents=True, exist_ok=True)
attempt = Path(tempfile.mkdtemp(prefix="gate-", dir=base))
print(f"retention-order evidence: {attempt.relative_to(root)}", flush=True)
source_files = [Path(name) for name in ("Cargo.toml", "Cargo.lock", "rust-toolchain.toml")]
for directory in ("crates", "third_party", "fixtures"):
    source_files += sorted(path.relative_to(root) for path in (root / directory).rglob("*")
                           if path.is_file())

def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

manifest = {str(path): digest(root / path) for path in source_files}
(attempt / "source-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
receipt = {"head": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
           "source_manifest_sha256": digest(attempt / "source-manifest.json"), "variants": {}}
(attempt / "tracked.diff").write_bytes(subprocess.check_output(["git", "diff", "HEAD"]))
(attempt / "gate-script.sh").write_bytes((root / "scripts/check-retention-order.sh").read_bytes())

relative_owner = Path("crates/ctxmux-daemon/src/retention.rs")
candidate_text = (root / relative_owner).read_text()
start = candidate_text.index("        victims.sort_by_cached_key(")
end = candidate_text.index("\n\n        let mut remaining", start)
old_sort = """        victims.sort_by(|a, b| {
            let a_except = a.run_id() == except;
            let b_except = b.run_id() == except;
            a_except
                .cmp(&b_except)
                .then_with(|| a.is_attached().cmp(&b.is_attached()))
                .then_with(|| b.retained_output_bytes().cmp(&a.retained_output_bytes()))
                .then_with(|| a.run_id().to_string().cmp(&b.run_id().to_string()))
        });"""
test_name = "retention::tests::changing_observations_cannot_change_one_reclamation_order"
public_name = "retention::tests::two_real_runs_keep_serving_across_retention_pressure_and_reconnect"

def run(source, label, args):
    result = subprocess.run(args, cwd=source, env=env, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True)
    (attempt / f"{label}.log").write_text(result.stdout)
    print(f"{label}: exit={result.returncode}", flush=True)
    if result.returncode:
        print(result.stdout[-6000:], flush=True)
    return result

for variant in ("candidate", "inverse"):
    source = attempt / variant / "source"
    for relative in source_files:
        target = source / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(root / relative, target)
    if variant == "inverse":
        (source / relative_owner).write_text(candidate_text[:start] + old_sort + candidate_text[end:])
    target_dir = attempt / variant / "build"
    assert not target_dir.exists(), "a variant must actually rebuild in a fresh output"
    env = dict(os.environ, CARGO_TARGET_DIR=str(target_dir), CARGO_INCREMENTAL="0",
               CARGO_PROFILE_DEV_DEBUG="0", CARGO_PROFILE_TEST_DEBUG="0")
    command = ["cargo", "test", "--locked", "-p", "ctxmux-daemon", "--lib", "--no-run",
               "--message-format=json"]
    built = run(source, f"{variant}-build", command)
    assert built.returncode == 0, "compilation failure cannot prove the counterexample"
    artifacts = [json.loads(line) for line in built.stdout.splitlines() if line.startswith("{")]
    executables = [Path(item["executable"]) for item in artifacts
                   if item.get("reason") == "compiler-artifact" and item.get("executable")
                   and item["target"]["name"] == "ctxmux_daemon" and item["profile"]["test"]]
    assert len(executables) == 1, "exactly one freshly compiled owner-test executable"
    binary = executables[0]
    binding = {"owner_sha256": digest(source / relative_owner),
               "binary": str(binary.relative_to(attempt)), "binary_sha256": digest(binary)}
    selected = run(source, f"{variant}-selection", [str(binary), "retention::tests", "--list"])
    assert selected.returncode == 0 and test_name + ": test" in selected.stdout
    assert public_name + ": test" in selected.stdout, "no empty public proof selection"
    if variant == "candidate":
        tested = run(source, "candidate-tests", [str(binary), "retention::tests", "--nocapture"])
        assert tested.returncode == 0 and "9 passed; 0 failed" in tested.stdout
        assert tested.stdout.count("retention public proof:") == 2
        assert tested.stdout.count("retention cleanup proof:") == 2
        for label, args in (
            ("candidate-format", ["cargo", "fmt", "--all", "--", "--check"]),
            ("candidate-clippy", ["cargo", "clippy", "--locked", "-p", "ctxmux-daemon",
                                  "--lib", "--tests", "--", "-D", "warnings"]),
        ):
            assert run(source, label, args).returncode == 0, label
    else:
        tested = run(source, "inverse-tests", [str(binary), test_name, "--exact", "--nocapture"])
        assert tested.returncode != 0 and f"{test_name} ... FAILED" in tested.stdout
        assert "1 failed" in tested.stdout and "0 passed" in tested.stdout
        binding["counterexample_exit"] = tested.returncode
    receipt["variants"][variant] = binding
    (attempt / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")

# Production use is outside retention's definition/test file, and the real
# public-client test above executes this path under actual shared pressure.
callers = run(root, "production-callers", ["rg", "-n", "reclaim_excess|turn.record",
              "crates/ctxmux-daemon/src/native_output.rs", "crates/ctxmux-daemon/src/native_runtime.rs"])
assert callers.returncode == 0 and "reclaim_excess(run.id)" in callers.stdout
assert "turn.record(buffer[..read].to_vec())" in callers.stdout
assert manifest == {str(path): digest(root / path) for path in source_files}, "source changed during gate"
receipt["result"] = "pass"
(attempt / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
print("PASS: fixed-order contract, two real Runs, source-bound inverse RED, format and clippy", flush=True)
PY
