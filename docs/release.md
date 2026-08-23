# Local artifact and release boundary

## Current scope

Ctxmux can produce a local, content-addressed consumer set from one clean Git
commit. This is packaging evidence, not a registry or hosted release. The
command does not publish an npm package, crate, Git ref, GitHub Release, global
installation, or downloaded binary.

Run from the repository root with an output directory that does not already
exist:

```bash
npm run build:local-artifacts -- <output-directory>
```

The command fails before building when the canonical Git worktree is dirty. It
neutralizes ambient Git, npm, Cargo-target and Rust-flag overrides, builds the
two Rust binaries with the locked release profile, builds and packs the existing
`@ctxmux/sdk` workspace, and then rechecks the same clean source identity before
publishing the output directory.

The output contains exactly:

```text
manifest.json
ctxmux-sdk-<sdk-version>.tgz
bin/ctxmux
bin/ctxmuxd
```

`manifest.json` binds the full Git commit and tree, commit time, clean-worktree
fact, ctxmux version, protocol generation, platform, architecture, Rust target,
release/locked build policy, Rust/Node/npm toolchain identity, SDK package
identity and entry/size bounds, and SHA-256, size, and canonical mode of every
artifact. The manifest is capped at 64 KiB; the SDK archive, unpacked SDK,
entry count, and each binary have checked ceilings. Output is staged and is
renamed into place only after all checks pass; an existing destination is never
merged with a new build.

## Independent local consumption

`npm run test:local-consumer` is the required public-consumer oracle. It makes
two artifact sets from the same clean identity and requires byte-identical
manifests and artifact hashes. It then creates a temporary directory outside
the checkout, copies only the produced tarball and binaries, and installs the
SDK with:

```bash
npm install --offline --ignore-scripts --no-package-lock --no-save <sdk-tarball>
```

The fixture keeps the consumer `package.json` unchanged, rejects installed
`file:` or `link:` dependencies, rejects symlinks and embedded absolute source
paths in the SDK, and creates no package lock. Its Node process may read only
the isolated consumer directory and may spawn child processes; it cannot read
the ctxmux checkout. From that boundary it starts the packaged daemon and uses
the packaged SDK to prove start, status, input, attach, retained replay,
Interrupt, and graceful Stop. The packaged CLI is version-checked and also
queries the same Run through the public protocol.

Consumers can use the same shape without saving a path dependency: install the
tarball into their chosen staging environment with `--no-save`, place the two
binaries in an application-owned directory, verify every manifest hash, and
pass the chosen daemon socket to `CtxmuxClient`. An activator that needs exact
spawn provenance passes one caller-owned inherited descriptor through
`ctxmuxd --readiness-fd <fd>` and accepts the socket only when its one readiness
record matches the public SDK hello instance. Daemon activation, installation
location, upgrade, logging, and cleanup policy remain consumer responsibilities;
this artifact set does not silently download or mutate global state.

## Determinism and platform meaning

The SDK tarball is required to be byte-reproducible for the bound source and npm
identity. Repeated assembly from one locked build must reproduce the complete
manifest and all hashes. Rust binaries are content-addressed for the declared
source, target, release profile, and Rust toolchain; ctxmux does not claim that
different toolchains, targets, linkers, SDKs, or build roots produce identical
binary bytes. Those unavoidable inputs are explicit instead of hidden behind a
generic “reproducible build” claim.

The manifest qualifies only its recorded `darwin` or `linux` platform,
architecture, Rust target, and Unix-socket transport. It is not a universal or
cross-compiled bundle, installer, compatibility layer, or remote backend.

## Required hosted CI evidence

The latest published required run available during the audit is
[CI run 31722805068](https://github.com/bagaking/ctxmux/actions/runs/31722805068)
for commit `0f7f598ff706736a57d319de27f739fe222002c2`. GitHub's public jobs and check
annotations establish:

- macOS 15 installed tmux successfully, then `Check workspace` failed with exit
  1;
- Ubuntu 24.04 installed tmux successfully, then `Check workspace` failed with
  exit 101;
- Ubuntu coverage installed its toolchain and tmux successfully, then
  `Check coverage policy` failed with exit 101;
- the ambient untrusted `aws/tap` and Node 20 action notices were warnings, not
  the failing steps.

The public API exposes only the step conclusions and generic exit annotations;
GitHub requires a signed-in identity for the job logs, so a deeper cause cannot
be reconstructed from public evidence. That run also predates the current local
Feature commit chain. Local success is not substituted for hosted success: the
final exact commit is release-qualified only after the required macOS, Ubuntu,
and coverage jobs run on that same commit and all pass. This task does not have
authority to push the commit or rerun/publish CI, so the hosted run URL remains
an explicit final external gate rather than a fabricated green result.
