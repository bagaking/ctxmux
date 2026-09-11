import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseBinaryVersion, sourceIdentity } from './build-local-artifacts.mjs';

const root = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const source = sourceIdentity(root);
const digest = file => createHash('sha256').update(fs.readFileSync(file)).digest('hex');
const artifactPaths = [process.env.CTXMUX_FAINT_DARWIN_ARTIFACTS, process.env.CTXMUX_FAINT_LINUX_ARTIFACTS];
assert.ok(artifactPaths.every(Boolean), 'provide both native artifact directories');
const manifests = artifactPaths.map((directory, index) => {
  const manifest = JSON.parse(fs.readFileSync(path.join(directory, 'manifest.json')));
  assert.equal(manifest.schema, 'ctxmux.local-artifacts.v1');
  assert.deepEqual(manifest.source, { ...source, worktree_clean: true }, 'artifacts must bind this exact committed clean source');
  assert.equal(manifest.support.platform, index === 0 ? 'darwin' : 'linux');
  assert.equal(manifest.support.architecture, index === 0 ? 'arm64' : 'x64');
  assert.equal(manifest.build.profile, 'release');
  assert.equal(manifest.build.locked, true);
  assert.deepEqual(manifest.binaries.map(binary => binary.name).sort(), ['ctxmux', 'ctxmuxd']);
  for (const artifact of [manifest.sdk.archive, ...manifest.binaries]) {
    assert.ok(!path.isAbsolute(artifact.path) && !artifact.path.split('/').includes('..'));
    const file = path.join(directory, artifact.path), stat = fs.lstatSync(file);
    assert.equal(stat.isSymbolicLink(), false);
    assert.equal(stat.size, artifact.bytes);
    assert.equal(digest(file), artifact.sha256);
    assert.equal(stat.mode & 0o777, Number.parseInt(artifact.mode, 8));
  }
  if (manifest.support.platform === process.platform && manifest.support.architecture === process.arch) {
    for (const binary of manifest.binaries) {
      const result = spawnSync(path.join(directory, binary.path), ['--version'], { encoding: 'utf8' });
      assert.ifError(result.error);
      assert.equal(result.status, 0);
      assert.deepEqual(parseBinaryVersion(binary.name, result.stdout), manifest.product);
    }
  }
  return manifest;
});
assert.deepEqual(manifests[0].product, manifests[1].product);
assert.deepEqual(manifests[0].sdk, manifests[1].sdk, 'one exact SDK archive and protocol for both native targets');
const evidence = path.join(root, '.tmp/terminal-faint-artifacts');
fs.mkdirSync(evidence, { recursive: true });
fs.writeFileSync(path.join(evidence, 'verification.json'), JSON.stringify({
  schema: 'ctxmux.terminal-faint-artifact-proof.v1', passed: true, source,
  manifests: artifactPaths.map(directory => ({ sha256: digest(path.join(directory, 'manifest.json')) })),
  platforms: manifests.map(manifest => manifest.support),
  nativeBuildRule: 'Each standard builder executes both native binaries before publishing its manifest; this check re-executes the local target.',
  userRunTouched: false,
}, null, 2) + '\n');
console.log('Both native artifacts and the shared SDK bind the exact committed Runtime source.');
