import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const root = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const flags = new Set(process.argv.slice(2));
const option = (name) => process.argv.find((arg) => arg.startsWith(`${name}=`))?.slice(name.length + 1);
assert(flags.has("--native") && flags.has("--consumer"), "both native owner and external SDK are required");
const output = path.resolve(option("--output") ?? path.join(root, ".tmp", "terminal-history-gap", randomUUID()));
fs.mkdirSync(output, { recursive: true });
const digest = (filename) => createHash("sha256").update(fs.readFileSync(filename)).digest("hex");
// Separate source-specific native output, without rebuildable incremental and
// debugger data filling the user's shared filesystem. This is a test build
// property; published release artifacts use the standard builder separately.
const buildEnvironment = { ...process.env, CARGO_INCREMENTAL: "0", CARGO_PROFILE_DEV_DEBUG: "0", CARGO_PROFILE_TEST_DEBUG: "0" };
function replayBytes(chunks) {
  let cursor = chunks[0]?.start_byte;
  for (const chunk of chunks) {
    assert.equal(chunk.start_byte, cursor, "external consumer joins actual ordered chunks");
    cursor += chunk.data.length;
    assert.equal(chunk.end_byte, cursor);
  }
  return Buffer.concat(chunks.map((chunk) => Buffer.from(chunk.data)));
}
function command(program, args, cwd, log) {
  const result = spawnSync(program, args, { cwd, env: buildEnvironment, encoding: "utf8", maxBuffer: 32 * 1024 * 1024 });
  fs.writeFileSync(log, `${result.stdout ?? ""}${result.stderr ?? ""}`);
  assert.equal(result.error, undefined, program);
  assert.equal(result.status, 0, `${program}: ${log}`);
  return result.stdout.trim();
}
async function until(probe, label, timeout = 15_000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    const result = await probe();
    if (result) return result;
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  assert.fail(label);
}
const owned = ["crates/ctxmux-daemon/src/lib.rs", "crates/ctxmux-daemon/src/persistence.rs", "docs/architecture.md", "docs/protocol.md", "crates/ctxmux-daemon/tests/tmux_adapter.rs"];
const workingBefore = Object.fromEntries(owned.map((filename) => [filename, digest(path.join(root, filename))]));
const base = command("git", ["rev-parse", "HEAD"], root, path.join(output, "source-head.log"));
const selected = Object.fromEntries(owned.map((filename) => {
  const bytes = spawnSync("git", ["show", `${base}:${filename}`], { cwd: root, maxBuffer: 16 * 1024 * 1024 });
  assert.equal(bytes.status, 0, `complete Main source ${filename}`);
  return [filename, createHash("sha256").update(bytes.stdout).digest("hex")];
}));
const cases = flags.has("--mutations") ? ["candidate", "history-invalidates-terminal", "source-keeps-stale-terminal"] : ["candidate"];
const receipts = [];
const sdkOption = option("--sdk");
const daemonOption = option("--daemon");
assert(!daemonOption || !flags.has("--mutations"), "artifact checks cannot pretend to mutate a published binary");

for (const name of cases) {
  const directory = path.join(output, name);
  fs.mkdirSync(directory);
  const source = path.join(directory, "source");
  let daemonBinary = daemonOption;
  let sdkPath = sdkOption;
  fs.mkdirSync(source);
    const archive = path.join(directory, "source.tar");
    command("git", ["archive", "--output", archive, base], root, path.join(directory, "archive.log"));
    command("tar", ["-xf", archive, "-C", source], root, path.join(directory, "extract.log"));
  if (!daemonBinary) {
    const implementation = path.join(source, owned[0]);
    let text = fs.readFileSync(implementation, "utf8");
    if (name === "history-invalidates-terminal") {
      const needle = "output.mark_history_gap();";
      assert.equal(text.split(needle).length, 2);
      text = text.replace(needle, "output.mark_source_gap();");
    } else if (name === "source-keeps-stale-terminal") {
      const needle = "        self.terminal = None;\n        self.terminal_absence = TerminalCheckpointUnavailableReason::SourceGap;\n";
      assert.equal(text.split(needle).length, 2);
      text = text.replace(needle, "");
    }
    fs.writeFileSync(implementation, text);
    const target = path.join(directory, "target");
    process.stdout.write(`${name}: build separate native output\n`);
    command("cargo", ["build", "--locked", "-p", "ctxmux-daemon", "-p", "ctxmux-test-support", "--bins", "--target-dir", target], source, path.join(directory, "build.log"));
    daemonBinary = path.join(target, "debug", "ctxmuxd");
    fs.symlinkSync(path.join(root, "node_modules"), path.join(source, "node_modules"), "dir");
    command("npm", ["run", "build", "--workspace", "@ctxmux/sdk"], source, path.join(directory, "sdk-build.log"));
    sdkPath = path.join(source, "packages", "sdk", "dist", "index.js");
  }
  // Cargo test can relink its CARGO_BIN_EXE output with test feature unions.
  // Freeze the exact native build before any consumer executes or relinks it.
  const frozenImage = path.join(directory, "native-ctxmuxd");
  fs.copyFileSync(path.resolve(daemonBinary), frozenImage);
  daemonBinary = frozenImage;
  const { CtxmuxClient, defineRun } = await import(pathToFileURL(path.resolve(sdkPath)).href);
  const socket = path.join(fs.mkdtempSync(path.join(os.tmpdir(), "ctxmux-gap-")), "runtime.sock");
  const state = path.join(directory, "state");
  const wrapper = "import os,resource,signal,sys\nsignal.signal(signal.SIGXFSZ,signal.SIG_IGN)\nresource.setrlimit(resource.RLIMIT_FSIZE,(4*1024*1024,4*1024*1024))\nos.execv(sys.argv[1],sys.argv[1:])";
  const daemon = spawn("/usr/bin/python3", ["-c", wrapper, path.resolve(daemonBinary), "--socket", socket, "--state-dir", state], { stdio: ["ignore", "ignore", "pipe"] });
  let stderr = "";
  daemon.stderr.on("data", (bytes) => { stderr += bytes.toString(); });
  const client = new CtxmuxClient({ socketPath: socket });
  const runs = [];
  let failure;
  const report = { name, base, selected, nativeTestBuild: { profile: "dev", incremental: false, debugInfo: false }, implementationSha256: digest(path.join(source, owned[0])), daemonSha256: digest(path.resolve(daemonBinary)), sdkSha256: digest(path.resolve(sdkPath)) };
  try {
    await until(() => fs.existsSync(socket), "private daemon socket");
    report.runtime = await client.runtimeInfo();
    const script = (label) => `import os,signal,termios\na=termios.tcgetattr(0);a[3]&=~(termios.ICANON|termios.ECHO);a[1]&=~termios.ONLCR;a[6][termios.VMIN]=1;a[6][termios.VTIME]=0;termios.tcsetattr(0,termios.TCSANOW,a)\nsignal.signal(signal.SIGINT,lambda s,f:exit(0))\nos.write(1,b'${label}:READY\\r\\n')\nwhile True:\n b=os.read(0,1)\n if b==b'F':\n  for i in range(96):os.write(1,b'x'*65536)\n os.write(1,b'\\x1b[2J\\x1b[H${label}:'+b+b'\\r\\n')\n`;
    for (const label of ["A", "B"]) runs.push(await client.start(defineRun("/usr/bin/python3", { args: ["-u", "-c", script(label)], initialSize: { rows: 4, cols: 40 } })));
    await until(async () => (await client.status(runs[1].id)).latest_output_bytes > 0, "both real producers ready");
    let attachment = await client.attachTerminal(runs[0].id);
    assert.equal(attachment.snapshot.terminal.type, "basic_vt", "initial live continuation");
    await attachment.detach();
    await client.input(runs[0].id, "F");
    await until(async () => (await client.status(runs[0].id)).latest_output_bytes > 6 * 1024 * 1024, "complete original burst admitted", 30_000);
    // Inspect the authoritative public mutation error. An optional diagnostic
    // sink need not print a persistence failure for the daemon to stay usable.
    let persistenceFailure;
    try { runs.push(await client.start(defineRun("/bin/true"))); }
    catch (error) { persistenceFailure = error; }
    assert(persistenceFailure, "actual private append failure refuses later durable mutations");
    assert.match(persistenceFailure.message, /File too large|os error 27|file too large/i, "actual private EFBIG append failure");
    report.persistenceFailure = persistenceFailure.message;
    await client.input(runs[0].id, "z");
    await until(async () => {
      const status = await client.status(runs[0].id);
      const view = await client.attach(runs[0].id, status.first_available_byte);
      const text = Buffer.from(replayBytes(view.snapshot.replay.chunks)).toString();
      await view.detach();
      return text.endsWith("A:z\r\n");
    }, "ordered post-failure marker");
    const status = await client.status(runs[0].id);
    const raw = await client.attach(runs[0].id, status.first_available_byte);
    assert.equal(raw.snapshot.replay.truncated, true, "real history gap remains visible");
    assert(status.durable_output_bytes < status.latest_output_bytes, "durability lag remains truthful");
    report.history = raw.snapshot.run;
    await raw.detach();
    attachment = await client.attachTerminal(runs[0].id, status.first_available_byte);
    assert.equal(attachment.snapshot.terminal.type, "basic_vt", "history failure preserves intact current VT");
    assert(Buffer.from(attachment.snapshot.terminal_restore).toString().includes("A:z"), "latest frame belongs to the original ordered source");
    assert.equal(attachment.snapshot.run.id, runs[0].id);
    assert.equal(attachment.snapshot.run.pid, runs[0].pid);
    assert.equal(attachment.snapshot.run.native_service.owner.type, "serving");
    assert.equal(attachment.snapshot.run.native_service.input.phase.type, "open");
    await attachment.detach();
    await client.input(runs[1].id, "b");
    await until(async () => {
      const view = await client.attachTerminal(runs[1].id);
      const intact = view.snapshot.terminal.type === "basic_vt" && Buffer.from(view.snapshot.terminal_restore).toString().includes("B:b");
      await view.detach();
      return intact;
    }, "second real Run continues serving");
    report.historyFailurePassed = true;
  } catch (error) { failure = error; }
  finally {
    // Only this fixture's exact public Run identities and private daemon are
    // cleaned. Persistence failure may refuse final history settlement.
    for (const run of runs) { try { await client.input(run.id, Buffer.from([3])); } catch {} }
    daemon.kill("SIGTERM");
    await until(() => daemon.exitCode !== null || daemon.signalCode !== null, "private daemon cleanup").catch(() => daemon.kill("SIGKILL"));
    fs.writeFileSync(path.join(directory, "daemon.stderr"), stderr);
    fs.writeFileSync(path.join(directory, "receipt.json"), JSON.stringify({ ...report, status: failure ? "failed" : "passed", failure: failure?.stack }, null, 2) + "\n");
    fs.rmSync(path.dirname(socket), { recursive: true, force: true });
  }
  if (name === "candidate") { if (failure) throw failure; }
  else if (name === "history-invalidates-terminal") {
    assert(failure, `${name} must fail the actual public behavior`);
    assert.match(failure.message, /history failure preserves intact current VT/, "specific counterfactual boundary");
  }
  if (!failure) {
    const target = path.join(directory, "target");
    const libLog = path.join(directory, "known-model-source-gap.log");
    const args = ["test", "--locked", "-p", "ctxmux-daemon", "--target-dir", target, "--lib", "tests::history_gap_distinction_keeps_true_source_gap_unknown", "--", "--exact", "--nocapture"];
    const result = spawnSync("cargo", args, { cwd: source, env: buildEnvironment, encoding: "utf8", maxBuffer: 32 * 1024 * 1024 });
    const raw = `${result.stdout ?? ""}${result.stderr ?? ""}`;
    fs.writeFileSync(libLog, raw);
    assert.equal(result.error, undefined);
    assert.match(raw, /running 1 test/, "nonempty known-model owning test");
    if (name === "source-keeps-stale-terminal") {
      assert.equal(result.status, 101, "true source loss must reject retained stale VT");
      assert.match(raw, /history_gap_distinction_keeps_true_source_gap_unknown .*FAILED/);
      assert.match(raw, /BasicVt/);
      failure = new Error("true source gap cannot keep stale terminal");
    } else {
      assert.equal(result.status, 0, libLog);
      assert.match(raw, /1 passed; 0 failed/);
      // This build-owned test launcher is not the production tmux binary.
      // Its first OS executable validation is qualified separately; the public
      // adapter retains its original short deadline and all gap assertions.
      const preflightLog = path.join(directory, "native-fixture-preflight.log");
      command("cargo", ["test", "--locked", "-p", "ctxmux-test-support", "--target-dir", target,
        "--lib", "tests::native_fixture_probe_declares_only_the_selected_schema_and_rejects_exec",
        "--", "--exact", "--nocapture"], source, preflightLog);
      const preflightRaw = fs.readFileSync(preflightLog, "utf8");
      assert.match(preflightRaw, /running 1 test/);
      assert.match(preflightRaw, /1 passed; 0 failed/);
      const fixtureOf = (raw) => {
        const binary = raw.match(/Running unittests[^\n]* \(([^)]+)\)/)?.[1]
          ?? raw.match(/Running tests\/[^\n]* \(([^)]+)\)/)?.[1];
        assert(binary, "actual test executable is recorded");
        const bytes = fs.readFileSync(path.resolve(source, binary));
        const images = [];
        const build = path.join(target, "debug", "build");
        for (const entry of fs.readdirSync(build)) {
          if (!entry.startsWith("ctxmux-test-support-")) continue;
          const outputFile = path.join(build, entry, "output");
          if (!fs.existsSync(outputFile)) continue;
          const image = fs.readFileSync(outputFile, "utf8").match(/cargo:rustc-env=CTXMUX_FIXTURE_EXECUTABLE=([^\n]+)/)?.[1];
          if (image && bytes.includes(Buffer.from(image))) images.push(image);
        }
        assert.equal(new Set(images).size, 1, "one exact compile-time native fixture image");
        return { path: images[0], sha256: digest(images[0]) };
      };
      const preflightImage = fixtureOf(preflightRaw);
      const protocolLog = path.join(directory, "public-backend-source-gap.log");
      const protocolResult = spawnSync("cargo", ["test", "--locked", "-p", "ctxmux-daemon", "--target-dir", target, "--test", "tmux_adapter", "public_pause_emits_exact_gap_and_requests_control_mode_continue", "--", "--exact", "--nocapture"], {
        cwd: source, encoding: "utf8", maxBuffer: 32 * 1024 * 1024,
        env: { ...buildEnvironment, CTXMUX_TEST_HISTORY_GAP_DAEMON: path.resolve(daemonBinary) },
      });
      const protocolRaw = `${protocolResult.stdout ?? ""}${protocolResult.stderr ?? ""}`;
      fs.writeFileSync(protocolLog, protocolRaw);
      assert.equal(protocolResult.error, undefined);
      assert.match(protocolRaw, /running 1 test/, "nonempty independent backend gap consumer");
      assert.equal(protocolResult.status, 0, protocolLog);
      assert.match(protocolRaw, /1 passed; 0 failed/);
      assert.deepEqual(fixtureOf(protocolRaw), preflightImage, "public backend consumes the exact preflighted fixture image");
      fs.copyFileSync(preflightImage.path, path.join(directory, "executed-native-fixture"));
      fs.writeFileSync(path.join(directory, "fixture-preflight.json"), JSON.stringify({
        ...preflightImage, publicAdapterDeadlineChanged: false, coldR3FailurePreserved: true,
        scope: "build-owned fixture readiness, not production cold-start budget acceptance",
      }, null, 2) + "\n");
    }
    fs.writeFileSync(path.join(directory, "gap-boundaries.json"), JSON.stringify({
      knownModel: "two real Native Runs; owning mark_output_source_gap; public late continuation",
      backend: name === "candidate" ? "independent Control protocol %pause; production source gap; public terminal unknown; second original Native Run input and resize" : undefined,
      counterfactualRed: Boolean(failure), implementationSha256: report.implementationSha256, daemonSha256: report.daemonSha256,
    }, null, 2) + "\n");
  }
  // Retain the exact executed image and all raw/source evidence. Cargo's
  // closed per-variant object cache is rebuildable and must not fill the shared
  // filesystem while the other independent owners qualify their work.
  const retainedImage = path.join(directory, "executed-ctxmuxd");
  fs.copyFileSync(path.resolve(daemonBinary), retainedImage);
  assert.equal(digest(retainedImage), report.daemonSha256);
  const target = path.join(directory, "target");
  const preservedTests = [];
  for (const filename of ["known-model-source-gap.log", "public-backend-source-gap.log"]) {
    const log = path.join(directory, filename);
    if (!fs.existsSync(log)) continue;
    const match = fs.readFileSync(log, "utf8").match(/Running (?:unittests[^\n]*|tests\/[^\n]*) \(([^)]+)\)/);
    if (!match) continue;
    const executable = path.resolve(source, match[1]);
    assert(executable.startsWith(target + path.sep), "only this closed private test output is retained");
    const retained = path.join(directory, path.basename(executable));
    fs.copyFileSync(executable, retained);
    preservedTests.push({ path: path.basename(retained), sha256: digest(retained) });
  }
  fs.writeFileSync(path.join(directory, "executed-artifacts.json"), JSON.stringify({ daemon: { path: path.basename(retainedImage), sha256: report.daemonSha256 }, tests: preservedTests, removedClosedObjectCache: "target", sourceAndRawReportsRetained: true }, null, 2) + "\n");
  if (fs.existsSync(target)) { assert(!fs.lstatSync(target).isSymbolicLink()); fs.rmSync(target, { recursive: true }); }
  receipts.push({ name, status: failure ? "counterfactual-red" : "passed", receipt: path.relative(output, path.join(directory, "receipt.json")) });
}
for (const filename of owned) assert.equal(digest(path.join(root, filename)), workingBefore[filename], "shared source remained intact");
fs.writeFileSync(path.join(output, "summary.json"), JSON.stringify({ status: "passed", base, selected, receipts }, null, 2) + "\n");
process.stdout.write(`${output}\n`);
