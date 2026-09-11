import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const evidence = path.join(root, '.tmp/terminal-faint-proof');
fs.mkdirSync(evidence, { recursive: true });
const xterm = process.env.CTXMUX_XTERM_MODULE;
assert.ok(xterm && fs.statSync(xterm).isFile(), 'CTXMUX_XTERM_MODULE must name the actual consumer xterm module');
const files = ['attrs', 'cell', 'screen', 'term'].map(name => `third_party/vt100/src/${name}.rs`);
const originals = new Map(files.map(name => [name, fs.readFileSync(path.join(root, name), 'utf8')]));
const digest = () => createHash('sha256').update(files.map(name => fs.readFileSync(path.join(root, name))).reduce((a, b) => Buffer.concat([a, b]), Buffer.alloc(0))).digest('hex');
const report = { schema: 'ctxmux.terminal-faint-proof.v1', passed: false, sourceSha256: digest(), mutations: [], userRunTouched: false };
function command(name, program, args, env = process.env, expected = 0) {
  const result = spawnSync(program, args, { cwd: root, env, encoding: 'utf8', maxBuffer: 8 * 1024 * 1024 });
  fs.writeFileSync(path.join(evidence, `${name}.log`), result.stdout + result.stderr);
  assert.ifError(result.error);
  if (expected === 0) assert.equal(result.status, 0, `${name}: see ${path.relative(root, evidence)}/${name}.log`);
  else assert.ok(result.status !== 0, `${name} must refute the production mutation`);
  return result.stdout;
}
function candidate(suffix) {
  const unit = command(`unit-${suffix}`, 'cargo', ['test', '-p', 'ctxmux-daemon', '--lib', 'terminal_checkpoint::tests']);
  assert.match(unit, /2 passed; 0 failed/u, 'both nonempty intensity/layout tests execute');
  const native = command(`native-${suffix}`, 'cargo', ['test', '-p', 'ctxmux-daemon', '--test', 'native_terminal_checkpoint_codec', '--test', 'native_terminal_checkpoint_owner']);
  assert.match(native, /18 passed; 0 failed/u);
  assert.match(native, /6 passed; 0 failed/u, 'real resize/exec and two-Run owner tests execute');
  const target = JSON.parse(command(`xterm-${suffix}`, process.execPath, ['crates/ctxmux-daemon/tests/fixtures/native_checkpoint_xterm.cjs', '.tmp/basic-codec/xterm-inputs.json', xterm]));
  assert.equal(target.fixtureCount, 7);
  assert.equal(target.basicPassed, true);
  return target;
}
try {
  const caller = fs.readFileSync(path.join(root, 'crates/ctxmux-daemon/src/terminal_checkpoint.rs'), 'utf8');
  assert.match(caller, /self\.parser\.basic_restore_geometry_checkpoint\(\)/u, 'Runtime production owner calls the parser checkpoint');
  const parser = fs.readFileSync(path.join(root, 'third_party/vt100/src/parser.rs'), 'utf8');
  assert.match(parser, /\.basic_restore_formatted\(/u, 'public parser enters the screen serializer');
  const row = fs.readFileSync(path.join(root, 'third_party/vt100/src/row.rs'), 'utf8');
  assert.match(row, /write_escape_code_diff\(/u, 'production row diff consumes logical attributes');
  report.sourceCallers = ['terminal_checkpoint.rs -> Parser checkpoint', 'parser.rs -> Screen serializer', 'row.rs -> Attrs diff'];
  command('format', 'cargo', ['fmt', '--all', '--', '--check']);
  command('static', 'cargo', ['clippy', '-p', 'ctxmux-daemon', '--lib', '--tests', '--', '-D', 'warnings']);
  report.consumer = candidate('candidate');
  const variants = [
    ['ignore-sgr2', files[2], '&[2] => self.attrs.set_faint(true)', '&[2] => self.attrs.set_faint(false)'],
    ['keep-faint-on-sgr22', files[2], 'self.attrs.set_faint(false);', '/* deliberately omit faint reset */'],
    ['omit-checkpoint-faint', files[3], 'if self.faint == Some(true)', 'if false'],
    ['lose-surviving-intensity', files[0], 'attrs.bold(self.bold()).faint(self.faint())', 'attrs.bold(false).faint(false)'],
    ['grow-cell', files[1], 'attrs: crate::attrs::Attrs,', 'attrs: crate::attrs::Attrs,\n    memory_regression: u8,'],
  ];
  for (const [name, file, before, after] of variants) {
    const source = originals.get(file);
    assert.equal(source.split(before).length, 2, `${name}: one exact production mutation`);
    fs.writeFileSync(path.join(root, file), source.replace(before, after));
    const mutantSha256 = digest();
    const env = { ...process.env, CARGO_TARGET_DIR: path.join(evidence, 'mutants', name, 'target') };
    console.log(`Refuting ${name} in a separate build output`);
    try {
      const refutation = command(name, 'cargo', ['test', '-p', 'ctxmux-daemon', '--lib', 'terminal_checkpoint::tests'], env, 1);
      assert.match(refutation, /test result: FAILED\..*\d+ failed/u, 'a real owning assertion, not a compiler error, must reject each mutation');
      if (name === 'ignore-sgr2') {
        const native = command(`${name}-native`, 'cargo', ['test', '-p', 'ctxmux-daemon', '--test', 'native_terminal_checkpoint_codec', '--test', 'native_terminal_checkpoint_owner'], env, 1);
        assert.match(native, /native_.*FAILED/u, 'actual private daemon owner must refute dropped intensity');
        command(`${name}-xterm`, process.execPath, ['crates/ctxmux-daemon/tests/fixtures/native_checkpoint_xterm.cjs', '.tmp/basic-codec/xterm-inputs.json', xterm], process.env, 1);
      }
      report.mutations.push({ name, sourceSha256: mutantSha256, rejected: true });
    } finally {
      fs.writeFileSync(path.join(root, file), source);
      // These are disposable private mutation outputs, never Runtime state.
      // Keep refutation logs and source identities without retaining five caches.
      fs.rmSync(env.CARGO_TARGET_DIR, { recursive: true, force: true });
    }
  }
  assert.equal(report.mutations.length, 5);
  assert.equal(digest(), report.sourceSha256);
  report.restoredConsumer = candidate('restored');
  report.passed = true;
} finally {
  for (const [name, source] of originals) fs.writeFileSync(path.join(root, name), source);
  fs.writeFileSync(path.join(evidence, 'verification.json'), JSON.stringify(report, null, 2) + '\n');
}
