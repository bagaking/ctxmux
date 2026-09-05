import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";

test("shell census observes separate owners and rejects a broken counter", () => {
  const producer = readFileSync(
    new URL("./check-fleet-scale.sh", import.meta.url),
    "utf8",
  );
  const begin = producer.indexOf("ctxmux_fleet_shell_self_test() {");
  const end = producer.indexOf("\n}\n", begin) + 3;
  const counter = producer.match(/^count_children\(\) \{ .*$/m)?.[0];
  assert.ok(begin >= 0 && end > begin && counter);
  const directory = mkdtempSync(path.join(tmpdir(), "ctxmux-counter-owner-"));
  try {
    for (const [name, body, expectedStatus] of [
      ["production", counter, 0],
      ["unguarded-empty-match", counter.replace(" || true", ""), 1],
      ["flattened-zero", "count_children() { printf 0; }", 1],
    ]) {
      const source = path.join(directory, name);
      writeFileSync(source, body + "\n");
      const result = spawnSync(
        "bash",
        [
          "-euo",
          "pipefail",
          "-c",
          producer.slice(begin, end) + '\nctxmux_fleet_shell_self_test "$1"',
          "_",
          source,
        ],
        { encoding: "utf8", timeout: 10000 },
      );
      assert.ifError(result.error);
      assert.equal(
        result.status,
        expectedStatus,
        name + result.stdout + result.stderr,
      );
      if (expectedStatus === 0) {
        assert.match(result.stdout, /cleanup_live_children=0/);
        assert.match(
          result.stdout,
          /exactly two owned live children are counted and reaped: 2/,
        );
      }
    }
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test(
  "execution collector reads the actual leader limits and preserves exact cleanup",
  {
    skip:
      process.platform !== "linux"
        ? "requires actual Linux procfs and cgroup configuration"
        : false,
  },
  () => {
    const producer = readFileSync(
      new URL("./check-fleet-scale.sh", import.meta.url),
      "utf8",
    );
    const begin = producer.indexOf("collect_execution_environment() {");
    const end = producer.indexOf("\n}\n", begin) + 3;
    assert.ok(begin >= 0 && end > begin);
    const directory = mkdtempSync(
      path.join(tmpdir(), "ctxmux-census-environment-"),
    );
    try {
      const script = `
set -euo pipefail
proc=/proc work=$1 child=
cleanup() { if [[ -n $child ]]; then kill "$child" 2>/dev/null || true; wait "$child" 2>/dev/null || true; fi; }
trap cleanup EXIT
${producer.slice(begin, end)}
ulimit -Sn 65536
sleep 30 &
child=$! first=$!
collect_execution_environment "$child" "$work/first.txt"
grep -q '^Max open files 65536 ' "$work/first.txt"
kill "$child"; wait "$child" || true
child=
ulimit -Sn 32768
sleep 30 &
child=$! second=$!
collect_execution_environment "$child" "$work/second.txt"
grep -q '^Max open files 32768 ' "$work/second.txt"
kill "$child"; wait "$child" || true
child=
[[ ! -e /proc/$first/stat && ! -e /proc/$second/stat ]]
`;
      const result = spawnSync("bash", ["-c", script, "_", directory], {
        encoding: "utf8",
        timeout: 10000,
      });
      assert.ifError(result.error);
      assert.equal(result.status, 0, result.stdout + result.stderr);
      const [first, second] = result.stdout
        .trim()
        .split("\n")
        .map((line) => JSON.parse(line));
      assert.notEqual(
        first.sha256,
        second.sha256,
        "different actual NOFILE allocation must not bind the same environment",
      );
      assert.equal(first.schema, "ctxmux.fleet-execution-environment.v1");
      assert.equal(typeof first.complete_cgroup_hierarchy, "boolean");
      for (const [environment, name] of [
        [first, "first.txt"],
        [second, "second.txt"],
      ]) {
        const raw = readFileSync(path.join(directory, name));
        assert.deepEqual(
          Buffer.from(environment.canonical_text_base64, "base64"),
          raw,
        );
        assert.equal(
          createHash("sha256").update(raw).digest("hex"),
          environment.sha256,
        );
      }
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  },
);

// The census is a Linux procfs/GNU-timeout workload. Exercise its actual EXIT
// owner with a hanging executable, rather than duplicating cleanup logic.
test(
  "failed census bounds hanging Stop, joins its sampler, and preserves failure",
  {
    skip:
      process.platform !== "linux"
        ? "requires the actual Linux census tools"
        : false,
  },
  () => {
    const producer = readFileSync(
      new URL("./check-fleet-scale.sh", import.meta.url),
      "utf8",
    );
    const begin = producer.indexOf("cleanup_daemon() {");
    const end = producer.indexOf("\ntrap cleanup_daemon EXIT", begin);
    assert.ok(begin >= 0 && end > begin);
    const cleanup = producer.slice(begin, end);
    for (const originalStatus of [7, 0]) {
      const directory = mkdtempSync(
        path.join(tmpdir(), "ctxmux-census-cleanup-"),
      );
      try {
        const client = path.join(directory, "hanging-client");
        writeFileSync(
          client,
          "#!/bin/bash\nprintf '%s\\n' $$ > \"${0%/*}/client.pid\"\n/bin/sleep 30\n",
          { mode: 0o700 },
        );
        writeFileSync(path.join(directory, "run-ids"), "one-owned-run\n");
        const script = `
set -euo pipefail
work=$1 ctxmux_bin=$2 sock=$1/unused.sock proc=/proc runs_stopped=false
python3 -c 'import pathlib,signal,sys,time
signal.signal(signal.SIGINT,lambda *_:sys.exit(0))
pathlib.Path(sys.argv[1]).write_text("ready")
while True: time.sleep(0.01)' "$work/daemon-ready" &
daemon_pid=$!
for _ in $(seq 1 100); do [[ -e "$work/daemon-ready" ]] && break; sleep 0.01; done
[[ -e "$work/daemon-ready" ]]
(while [[ ! -e "$work/sampler-stop" ]]; do sleep 0.01; done) &
sampler_pid=$!
printf '%s %s\\n' "$daemon_pid" "$sampler_pid" > "$work/owner.pids"
${cleanup}
trap cleanup_daemon EXIT
exit "$3"
`;
        const started = Date.now();
        const result = spawnSync(
          "bash",
          ["-c", script, "_", directory, client, String(originalStatus)],
          { encoding: "utf8", timeout: 25000 },
        );
        assert.ifError(result.error);
        assert.equal(result.status, originalStatus || 1, result.stderr);
        assert.ok(
          Date.now() - started < 20000,
          "hanging Stop consumed more than the shared cleanup window",
        );
        assert.match(
          readFileSync(path.join(directory, "failure-cleanup.log"), "utf8"),
          new RegExp(`original_exit=${originalStatus} cleanup_failed=1`),
        );
        const pids = [
          readFileSync(path.join(directory, "owner.pids"), "utf8"),
          readFileSync(path.join(directory, "client.pid"), "utf8"),
        ]
          .join(" ")
          .trim()
          .split(/\s+/);
        const retirementDeadline = started + 20000;
        for (const pid of pids) {
          while (true) {
            try {
              process.kill(Number(pid), 0);
            } catch (error) {
              assert.equal(error.code, "ESRCH");
              break;
            }
            assert.ok(
              Date.now() < retirementDeadline,
              `owned process ${pid} survived cleanup`,
            );
            Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 10);
          }
        }
      } finally {
        rmSync(directory, { recursive: true, force: true });
      }
    }
  },
);

test(
  "real census requires distinct live leaders owned by its daemon, including zombie rejection",
  {
    skip:
      process.platform !== "linux"
        ? "requires actual Linux procfs process states"
        : false,
  },
  () => {
    const producer = readFileSync(
      new URL("./check-fleet-scale.sh", import.meta.url),
      "utf8",
    );
    const begin = producer.indexOf("confirm_live_runs() {");
    const end = producer.indexOf("\n}\n", begin) + 3;
    assert.ok(begin >= 0 && end > begin);
    const confirmation = producer.slice(begin, end);
    const directory = mkdtempSync(path.join(tmpdir(), "ctxmux-census-live-"));
    try {
      const script = `
set -euo pipefail
proc=/proc work=$1 child= zombie_owner=
cleanup() {
  if [[ -n $child ]]; then kill "$child" 2>/dev/null || true; wait "$child" 2>/dev/null || true; fi
  if [[ -n $zombie_owner ]]; then kill "$zombie_owner" 2>/dev/null || true; wait "$zombie_owner" 2>/dev/null || true; fi
}
trap cleanup EXIT
${confirmation}
sleep 30 &
child=$!
printf -v row 'one\\trunning\\tpid=%s\\thead=1' "$child"
[[ $(confirm_live_runs "$row" 1 "$$") == 1 ]]
printf -v exited_row 'one\\texited\\tpid=%s\\thead=1' "$child"
! confirm_live_runs "$exited_row" 1 "$$"
! confirm_live_runs "$row" 1 "$PPID"
! confirm_live_runs "$row"$'\\n'"$row" 2 "$$"
kill "$child"; wait "$child" || true
! confirm_live_runs "$row" 1 "$$"
child=
python3 -c 'import os,pathlib,signal,sys,time
child=os.fork()
if child==0: os._exit(0)
def stop(signum, frame): raise SystemExit(0)
signal.signal(signal.SIGTERM, stop)
try:
 pathlib.Path(sys.argv[1]).write_text(str(os.getpid())+" "+str(child))
 time.sleep(30)
finally:
 os.waitpid(child, 0)' "$work/zombie.pids" &
zombie_owner=$!
for _ in $(seq 1 100); do [[ -s "$work/zombie.pids" ]] && break; sleep 0.01; done
read -r owner zombie < "$work/zombie.pids" || [[ -n $zombie ]]
[[ $owner == "$zombie_owner" ]]
for _ in $(seq 1 100); do
  state=$(awk '{print $3}' "/proc/$zombie/stat")
  [[ $state == Z ]] && break
  sleep 0.01
done
[[ $state == Z ]]
printf -v row 'one\\trunning\\tpid=%s\\thead=1' "$zombie"
! confirm_live_runs "$row" 1 "$owner"
kill "$zombie_owner"; wait "$zombie_owner"
zombie_owner=
[[ ! -e /proc/$owner/stat && ! -e /proc/$zombie/stat ]]
`;
      const result = spawnSync("bash", ["-c", script, "_", directory], {
        encoding: "utf8",
        timeout: 25000,
      });
      assert.ifError(result.error);
      assert.equal(result.status, 0, result.stdout + result.stderr);
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  },
);
