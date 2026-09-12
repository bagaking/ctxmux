#!/usr/bin/env bash
set -euo pipefail
# Resolve a caller's symlink before selecting the source tree to freeze.
ctxmux_readiness_script=$(python3 -c 'import pathlib,sys; print(pathlib.Path(sys.argv[1]).resolve())' "${BASH_SOURCE[0]}")
cd "$(dirname "$ctxmux_readiness_script")/.."

python3 - <<'PY'
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

root = Path.cwd()
common = Path(subprocess.check_output(['git', 'rev-parse', '--git-common-dir'], text=True).strip()).resolve()
base = common.parent / '.bagakit/feature-tracker/features/f-22vcz84zn/artifacts/readiness-observer'
base.mkdir(parents=True, exist_ok=True)
attempt = Path(tempfile.mkdtemp(prefix='gate-', dir=base))
print(f'readiness observer evidence: {attempt}', flush=True)
files = [Path(name) for name in ('Cargo.toml', 'Cargo.lock', 'rust-toolchain.toml')]
for directory in ('crates', 'third_party', 'fixtures'):
    files += sorted(p.relative_to(root) for p in (root / directory).rglob('*') if p.is_file())

def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

manifest = {str(p): digest(root / p) for p in files}
(attempt / 'source-manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
(attempt / 'tracked.diff').write_bytes(subprocess.check_output(['git', 'diff', 'HEAD']))
receipt = {'head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
           'source_root': str(root), 'source_manifest_sha256': digest(attempt / 'source-manifest.json'),
           'variants': {}}
owner = Path('crates/ctxmux-daemon/src/lib.rs')
text = (root / owner).read_text()
start = text.index('        if let Err(error) = readiness\n')
end = text.index('\n    }\n\n    let mut sighup', start)
fatal = '''        readiness
            .write_all(&record)
            .and_then(|()| readiness.write_all(b"\\n"))
            .and_then(|()| readiness.flush())
            .map_err(|source| ServerError::io("<readiness-fd>", source))?;'''

for variant in ('candidate', 'inverse'):
    source = attempt / variant / 'source'
    for relative in files:
        target = source / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(root / relative, target)
    if variant == 'inverse':
        (source / owner).write_text(text[:start] + fatal + text[end:])
    target_dir = attempt / variant / 'build'
    assert not target_dir.exists(), 'variant must actually compile in fresh output'
    env = dict(os.environ, CARGO_TARGET_DIR=str(target_dir), CARGO_INCREMENTAL='0',
               CARGO_PROFILE_DEV_DEBUG='0', CARGO_PROFILE_TEST_DEBUG='0')

    def run(label, args):
        result = subprocess.run(args, cwd=source, env=env, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, text=True)
        (attempt / f'{variant}-{label}.log').write_text(result.stdout)
        print(f'{variant}-{label}: exit={result.returncode}', flush=True)
        if result.returncode:
            print(result.stdout[-4000:], flush=True)
        return result

    built = run('build', ['cargo', 'test', '--locked', '-p', 'ctxmux-daemon',
                         '--test', 'readiness_observer', '--no-run', '--message-format=json'])
    assert built.returncode == 0, 'compile failure is not behavioral inverse RED'
    artifacts = [json.loads(line) for line in built.stdout.splitlines() if line.startswith('{')]
    binaries = {item['target']['name']: Path(item['executable']) for item in artifacts
                if item.get('reason') == 'compiler-artifact' and item.get('executable')}
    test = binaries['readiness_observer']
    daemon = binaries['ctxmuxd']
    binding = {'owner_sha256': digest(source / owner), 'test_binary_sha256': digest(test),
               'daemon_binary_sha256': digest(daemon), 'test_binary': str(test),
               'daemon_binary': str(daemon)}
    selected = run('selection', [str(test), '--list'])
    assert selected.returncode == 0 and '2 tests, 0 benchmarks' in selected.stdout
    tested = run('public-tests', [str(test), '--nocapture'])
    if variant == 'candidate':
        assert tested.returncode == 0 and '2 passed; 0 failed' in tested.stdout
        assert tested.stdout.count('readiness public proof:') == 2
        assert run('descriptor-validation', ['cargo', 'test', '--locked', '-p', 'ctxmux-daemon',
                   '--test', 'native_lifecycle', 'readiness', '--', '--nocapture']).returncode == 0
        assert run('format', ['cargo', 'fmt', '--all', '--', '--check']).returncode == 0
        assert run('clippy', ['cargo', 'clippy', '--locked', '-p', 'ctxmux-daemon',
                   '--test', 'readiness_observer', '--test', 'native_lifecycle',
                   '--', '-D', 'warnings']).returncode == 0
    else:
        assert tested.returncode != 0 and '0 passed; 2 failed' in tested.stdout
        assert tested.stdout.count('departed readiness observer terminated the initialized Runtime') == 2
        binding['counterexample_exit'] = tested.returncode
    receipt['variants'][variant] = binding
    (attempt / 'receipt.json').write_text(json.dumps(receipt, indent=2) + '\n')

callers = subprocess.run(['rg', '-n', 'ctxmux_daemon::serve_configured',
                         'crates/ctxmux-daemon/src/main.rs'], cwd=root, capture_output=True, text=True)
(attempt / 'production-callers.log').write_text(callers.stdout)
assert callers.returncode == 0 and callers.stdout.strip(), 'production caller must be outside lib.rs'
assert manifest == {str(p): digest(root / p) for p in files}, 'selected source changed during proof'
receipt['result'] = 'pass'
(attempt / 'receipt.json').write_text(json.dumps(receipt, indent=2) + '\n')
print('PASS: departed readiness observers, source-bound fatal inverse RED, callers, format and clippy', flush=True)
PY
