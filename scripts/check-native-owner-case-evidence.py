#!/usr/bin/env python3
"""Verify retained private incident evidence, without touching a serving Runtime.

This is a historical-evidence gate. It does not qualify the Native repair,
current Run recovery, or a product workload limit. Sample/test counts describe
the attached observations rather than production capacity requirements.
"""

import hashlib
import json
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
EVIDENCE = ROOT / ".bagakit/feature-tracker/features/f-22vcz84zn/artifacts/private-native-owner-incident"


def read(name):
    return json.loads((EVIDENCE / name).read_text())


def require(condition, message):
    if not condition:
        raise ValueError(message)


def main():
    incident = read("incident-evidence.json")
    require(incident["status"] == "investigation-and-repair-in-progress",
            "Historical intake must not impersonate a recovery receipt")
    for record in incident["files"]:
        path = EVIDENCE / record["file"]
        require(path.parent == EVIDENCE, "Evidence path escapes its bundle")
        require(hashlib.sha256(path.read_bytes()).hexdigest() == record["sha256"],
                f"Evidence changed: {record['file']}")

    native = read("native-observations.json")
    start = read("public-start-failure.json")
    for key in ("runtimeId", "daemonInstanceId", "protocolGeneration"):
        require(native["runtime"][key] == start["runtime"][key],
                f"Start diagnosis and fleet sample disagree on {key}")
    require(start["response"]["type"] == "error", "Start error is missing")
    require(start["response"]["error"]["code"] == "spawn_failed",
            "Start error class changed")
    require("native owner stopped before registration" in
            start["response"]["error"]["message"], "Owner-stop diagnosis is missing")
    require(all(value == 0 for value in native["controls"].values()),
            "The live fleet sampling was not read-only")

    first, second = native["observations"]
    a = {run["id"]: run for run in first["runs"]}
    b = {run["id"]: run for run in second["runs"]}
    require(a.keys() == b.keys(), "The sample fleet changed")
    require(len(a) == native["running_count"], "Running census disagrees")
    for run_id in a:
        for key in ("pid", "state", "latest_output_bytes", "durable_output_bytes",
                    "applied_input_bytes"):
            require(a[run_id][key] == b[run_id][key],
                    f"Stationary-sample claim differs: {run_id}/{key}")

    identities = read("process-identity.json")["runs"]
    cli = {record["pid"]: record for record in incident["cli_observations"]}
    require(len(cli) == len(incident["active_agent_sessions"]),
            "Active Agent and actual CLI sample censuses disagree")
    for session in incident["active_agent_sessions"]:
        run_id = session["run_id"]
        require(run_id in a, "Session does not map to the observed Native fleet")
        identity = next(row for row in identities if row["run_id"] == run_id)
        require(identity["leader"]["pid"] == a[run_id]["pid"],
                "Public Run and OS leader identity disagree")
        children = [child for child in identity["direct_children"]
                    if child["pid"] in cli]
        require(len(children) == 1, "CLI process attribution is ambiguous")
        observation = cli[children[0]["pid"]]
        text = (EVIDENCE / observation["sample_excerpt"]).read_text()
        require("codex-main" in text and
                re.search(r"\bwrite\s+\(in libsystem_kernel", text) and
                any(marker in text for marker in ("Stdout", "stdio", "BufWriter")),
                "Actual CLI main-thread stdout blocking is missing")
        require(observation["main_thread_stdout_system_write"],
                "CLI interpretation disagrees with its actual stack")

    sample = (EVIDENCE / "native-sample.txt").read_text()
    require("ctxmux-native-owner" not in sample, "Named owner is present in sample")
    drains = re.findall(r"Thread_[^\n]+: ctxmux-input-drain\n(.*?)(?=\n\s*\d+ "
                        r"Thread_|\nTotal number in stack)", sample, re.S)
    require(drains and all("InputDrainGate::run_worker" in block and
                           "write_all" in block and
                           re.search(r"\bwrite\s+\(in libsystem_kernel", block)
                           for block in drains),
            "Native drain blocking claim is not supported by the thread sections")

    counterexample = read("parser-observed-receipt.json")
    require(counterexample["sourceCommit"] == incident["incident_source_commit"],
            "Counterexample source differs from serving-source receipt")
    panic_log = (EVIDENCE / "parser-parser-observed.log").read_text()
    require(all(case.endswith(": PANIC") and case in panic_log
                for case in counterexample["cases"]),
            "Counterexample panics are not in the attached actual log")
    require("not GREEN" in counterexample["meaning"],
            "Caught defect harness is incorrectly described as acceptance")

    proof = read("parser-fix-verification.json")
    require(proof["baselineCommit"] == incident["incident_source_commit"],
            "Parser proof uses a different baseline")
    controls = []
    reversals = []
    for run in proof["runs"]:
        text = (EVIDENCE / f"parser-fix-{run['name']}.log").read_text()
        require(hashlib.sha256(text.encode()).hexdigest() == run["logSha256"],
                "Author raw log digest disagrees with proof receipt")
        require(run["collected"] > 0 and
                f"running {run['collected']} tests" in text,
                "No collected owning tests in author proof")
        if run["exitCode"] == 0:
            require("test result: ok." in text and not run["assertionRed"],
                    "Control receipt is not a real GREEN")
            controls.append(run)
        else:
            require(run["assertionRed"] and "test result: FAILED." in text and
                    run["expectedFailingTests"], "Nonzero exit is not an assertion RED")
            require(all(re.search(r"test\s+" + re.escape(name) +
                                  r"\s+\.\.\.\s+FAILED", text)
                        for name in run["expectedFailingTests"]),
                    "Expected source-reversal failure was not actually observed")
            if run["name"] != "exact-ec-counterexample":
                reversals.append(run)
    require(controls and reversals and
            any(run["name"] == "control-restored" for run in controls),
            "Source reversal proof lacks restored control")

    store = read("explicit-store-authority.json")
    known_sessions = {row["session"] for row in store["observations"]
                      if row["ok"] and row["returncode"] == 0}
    expected_sessions = {row["session"] for row in store["observations"]}
    active_sessions = {row["session_id"] for row in incident["active_agent_sessions"]}
    require(expected_sessions and expected_sessions <= active_sessions and
            known_sessions == expected_sessions,
            "Every explicit Desktop store query must succeed for an observed Session")
    require(incident["unknowns"], "Unresolved incident evidence was concealed")
    print(json.dumps({"result": "pass", "scope": "historical incident evidence only",
                      "active_cli_samples": len(cli), "blocked_drain_samples": len(drains),
                      "parser_source_reversal_reds": len(reversals),
                      "recovery_claim": "not qualified"}))


if __name__ == "__main__":
    main()
