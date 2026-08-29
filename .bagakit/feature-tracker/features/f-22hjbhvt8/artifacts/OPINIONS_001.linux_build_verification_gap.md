# Observation — `main` does not compile for Linux, and a fix was already written

- Recorded: 2026-08-29, during `f-22hjbhvt8` revision 5 execution. Revised the
  same day once a local Linux compile was actually produced, which narrowed the
  root cause and retired the claim that no local route existed.
- Source: independent read-only forensic audit commissioned by the supervisor
  after a Linux cross-build for the remote qualification host failed to compile.
- Status: **outside this Feature's scope.** Recorded here because this Feature
  discovered it; the fix path is an Owner decision, not a Remote Runtime task.

## The fact

`crates/ctxmux-daemon/src/native_session.rs` contains, behind
`#[cfg(not(target_os = "macos"))]`:

```rust
Ok(system.processes().keys().map(sysinfo::Pid::as_u32).collect())
```

`Pid::as_u32` takes `self`; `HashMap::keys()` yields `&Pid`. The function-item
path therefore needs `FnMut(&Pid) -> u32` and does not type-check. This is
present verbatim in both local `main` (`e0f3d69`) and `origin/main` (`a089708`)
at the time of writing — verified by reading both trees, not inferred.

`crates/ctxmux-daemon/src/lib.rs:31` declares `mod native_session;` with no cfg,
so on any Linux target this is a hard compile failure for the daemon crate. It is
not a warning, a lint, or a test failure.

## How it survived

Two independent causes, and the second is the one worth keeping.

**It was a refactor bug, not a dependency break.** `sysinfo` has been pinned at
`0.39.x` throughout. The call site compiled when introduced in `c6f5514`, where
it mapped by value over `Option<Pid>`. Commit `6af8f5e`
("perf(daemon): bound macOS session census memory", 2026-08-10) extracted
`process_ids()` and rewrote the map over `keys()`, which changed the receiver from
a value to a reference. 115 commits have since landed on `main` with the break in
place.

**The local Gate structurally cannot see it.** On macOS, `scripts/check.sh`
compiles the `target_os = "macos"` branch and gives a green result. The broken
branch is compiled only by hosted CI. `.github/workflows/ci.yml` is correctly
wired for this — `ubuntu-24.04` appears in both the `critical` and `coverage`
jobs, both run the full `check.sh`, and `scripts/ci-reachability.mjs` actively
forbids skipping, excluding, or `continue-on-error` on required jobs. So CI would
catch this the moment it runs. The audit found no recorded green hosted run on
any commit at or after `6af8f5e`.

That distinction matters: this is a hole in verification *execution*, not in CI
design. Nothing needs redesigning; something needs running.

## A fix already existed and never merged

`origin/fix/linux-pid-as-u32` (`4d32574`, 2026-08-15) applies the identical
one-line closure form. It is a single commit, diverged from `main`, and was never
merged — while `origin/main` continued advancing until 2026-08-24.

So someone hit this nine days before this Feature did, wrote the correct fix, and
it did not reach `main`. The recurrence is the signal, not the line.

## Scale of the unverified surface

Approximately 17 `cfg` blocks are never compiled by a macOS build:

| Location | Blocks |
| --- | --- |
| `crates/ctxmux-daemon/src/native_control.rs` | 9 — the largest island, never compiled on this machine |
| `crates/ctxmux-daemon/src/native_session.rs` | 5, including the defect |
| `crates/ctxmux-daemon/src/native_runtime.rs` | 2 |
| `crates/ctxmux-daemon/src/adopted_pty.rs` | 1 |
| `crates/ctxmux-rss-sampler/src/main.rs` | several |

Target-conditional dependencies compound it: `sysinfo` is linked only under
`not(macos)`, while `ctxmux-process-stats` and `ctxmux-pty-signal` are linked only
under `macos`. Linux-only test helpers in `native_lifecycle.rs` and
`tmux_adapter.rs` read procfs and build only on Linux.

An audit of "are there other Linux errors" run *before* the fix cannot answer the
question: rustc stops after it cannot compile an item, so everything downstream
was never type-checked. A complete Linux compile is required to make that claim,
and the run recorded in the next section makes it: with the fix in place, every
one of these islands type-checks. The downstream surface was clean; only the one
line was broken.

## A local route does prove a Linux build, once zig is present

`cargo check --target x86_64-unknown-linux-gnu` fails inside `cc-rs` looking for
`x86_64-linux-gnu-gcc`, because `rusqlite` bundles SQLite and needs a cross C
toolchain. The rustc target is installed; the C toolchain is not. Docker is
installed but its daemon hangs, so it is not a route either.

`cargo-zigbuild` is the route that works, because zig supplies the cross C
compiler:

```
PATH="<zig-0.16.0-dir>:$PATH" cargo zigbuild --locked \
  --target x86_64-unknown-linux-gnu --workspace --all-targets
```

Run on 2026-08-29 against this Feature's branch: the whole workspace, all 11
crates and every test target, compiled with **0 errors and 0 warnings**.

Two checks make that result meaningful rather than vacuous. `sysinfo` appears in
`target/x86_64-unknown-linux-gnu/debug/deps/`, and it links only under
`not(macos)` — so the gated islands were genuinely compiled. And reintroducing
the original `.map(sysinfo::Pid::as_u32)` reproduces the exact `main` failure
(`expected fn(&sysinfo::Pid) -> _`), which proves the route type-checks that
specific line rather than reaching a cached result.

So the defect behind the defect is narrower than first recorded: the repository
is not missing a *possible* local Linux build, it is missing a *routine* one.
Nothing in the toolchain needs inventing; a documented command needs running
before merge. T-008 requires a real-Linux *execution* of the complete gate, which
the cross-build above does not supply: it proves the workspace compiles for
Linux, including the cfg islands macOS never sees, and nothing about running the
suites there. T-008 stays open on that basis. Whether a local Linux compile
becomes a standing repository guarantee — a script, a `check.sh` stage, or a
pre-merge hook — is a separate decision the Owner owns, and this Feature
deliberately does not make it.

## What this Feature did and did not do

Did: fixed the line on the feature branch (`0a97cd8`), because the remote
qualification host needs a daemon that compiles for Linux, and produced a
complete Linux compile of the workspace. That compile is evidence *for* T-008,
not its closure: T-008 asks for the gate to run on real Linux.

Did not: fix `main`, merge `origin/fix/linux-pid-as-u32`, or change CI. Landing a
`main` repair inside a transport Feature would blur attribution for both. The
supervisor surfaced it to the Owner instead.
