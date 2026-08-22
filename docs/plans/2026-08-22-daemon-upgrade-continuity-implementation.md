# Daemon Upgrade & Semantic-Resume Continuity — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make a live PTY Run survive an intentional daemon upgrade/restart via `execve`-in-place (Track A), and let a crashed/rebooted Run be brought back semantically via an explicit `resume` verb (Track B).

**Architecture:** On `SIGHUP` (persistent mode only) the daemon drains new connections, extracts the raw fds of every live native Run, clears close-on-exec on exactly those fds plus the listener and state-lock fds, writes a `{RunId→fds}` + epoch handoff manifest, and `execve`s its own binary. The new image reuses the passed epoch, reconstructs the listener from the inherited fd, excludes the handed-off RunIds from the `running→interrupted` reconciliation, and re-adopts each Run's control from the inherited fds (child reaped via `waitid` on the bare PID). Track B adds a `ctxmux resume <id>` verb that clones a recovered Run's spec (Level A) or re-derives provider provenance from durable replay and materializes a provider-resume start (Level B).

**Tech Stack:** Rust (tokio, portable-pty 0.9, rusqlite/SQLite WAL, rustix, libc), Unix-socket NDJSON protocol (generation 9), TypeScript SDK (Codex integration).

**Design SSOT:** `docs/plans/2026-08-22-daemon-upgrade-continuity-design.md`
**Decision records:** `docs/architecture/choices/015-exec-in-place-upgrade-continuity.md` (Track A), `docs/architecture/choices/016-semantic-resume.md` (Track B)

**Guiding constraint (verbatim, applies to every task):** 控制系统的熵, 同时保证能力的完善和代码的优雅, 避免过度设计和过度校验. Add the *least* machinery; reuse fd-inheritance and reconnect-from-cursor; never build a broker or per-Run supervisor.

---

## How to work this plan

- Each task is TDD: write the failing test, run it to see it fail for the *expected* reason, write the minimal code, run it green, commit.
- Run all commands from the repo root: `/Users/bytedance/proj/priv/bagaking/ctxmux`.
- Rust workspace commands: `cargo test -p <crate>`, `cargo build --workspace`, `cargo clippy --workspace --all-targets`, `cargo fmt`.
- The daemon crate is `#![forbid(unsafe_code)]` **except** the `ctxmux-inherited-fd` leaf crate, which is the *only* place raw-fd `unsafe` may live. Any new raw-fd manipulation (clearing CLOEXEC, wrapping a raw master fd) goes through that crate or a similarly audited leaf — never inline in the daemon.
- Commit after every green task. Keep commits small.
- Reliability budgets are frozen: net permanent per-Run fd/thread delta across an upgrade MUST be zero. `reliability-budgets.json` and `scripts/reliability-budget-contract.mjs` are the contract; do not raise the slopes.
- **Track A must land and be green before Track B starts** — B reuses the recovered-Run plumbing A hardens.

---

# TRACK A — exec-in-place upgrade continuity

## Task A1: `clear_cloexec` primitive in the audited fd leaf

**Why:** `execve` only preserves fds whose close-on-exec flag is *cleared*. Today `ctxmux-inherited-fd` only ever *sets* CLOEXEC (`duplicate_cloexec`). We need the subtractive counterpart, in the one crate allowed `unsafe`.

**Files:**
- Modify: `crates/ctxmux-inherited-fd/src/lib.rs`
- Test: same file's `#[cfg(test)] mod tests`

**Step 1: Write the failing test**

Add to the tests module in `crates/ctxmux-inherited-fd/src/lib.rs`:

```rust
#[test]
fn clear_cloexec_removes_the_flag_in_place() {
    use rustix::io::{FdFlags, fcntl_getfd};
    let (reader, writer) = pipe().unwrap();
    // Duplicated fds start CLOEXEC-set (see duplicate_cloexec).
    let owned = duplicate_cloexec(writer.as_raw_fd()).unwrap();
    assert!(fcntl_getfd(&owned).unwrap().contains(FdFlags::CLOEXEC));
    super::clear_cloexec(owned.as_raw_fd()).unwrap();
    assert!(!fcntl_getfd(&owned).unwrap().contains(FdFlags::CLOEXEC));
    drop(owned);
    drop(reader);
    drop(writer);
}

#[test]
fn clear_cloexec_rejects_standard_descriptors() {
    assert_eq!(
        super::clear_cloexec(2).unwrap_err().kind(),
        std::io::ErrorKind::InvalidInput
    );
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p ctxmux-inherited-fd clear_cloexec`
Expected: FAIL — `cannot find function clear_cloexec in module super`.

**Step 3: Write minimal implementation**

Add to `crates/ctxmux-inherited-fd/src/lib.rs` (mirror the guard and `#[allow(unsafe_code)]` style of `duplicate_cloexec`):

```rust
/// Clear the close-on-exec flag on a descriptor so it survives `execve`.
///
/// The caller retains ownership; this only mutates the descriptor's flags.
///
/// # Errors
///
/// Rejects standard descriptors and returns an operating-system error when the
/// flag read or write fails.
#[allow(unsafe_code)]
pub fn clear_cloexec(raw_fd: RawFd) -> std::io::Result<()> {
    if raw_fd < 3 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "descriptor must be at least 3",
        ));
    }
    let flags = unsafe { libc::fcntl(raw_fd, libc::F_GETFD) };
    if flags < 0 {
        return Err(std::io::Error::last_os_error());
    }
    if unsafe { libc::fcntl(raw_fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) } < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test -p ctxmux-inherited-fd clear_cloexec`
Expected: PASS (both tests).

**Step 5: Commit**

```bash
git add crates/ctxmux-inherited-fd/src/lib.rs
git commit -m "feat(inherited-fd): add clear_cloexec for execve fd survival"
```

---

## Task A2: Handoff manifest type + serialization

**Why:** The upgrade must carry `{RunId → (child_pid, master_fd)}` plus the epoch string across the exec. Keep it a small, self-describing, versioned struct serialized to a single inherited fd (not argv — argv length is bounded and fd numbers leak into `ps`).

> **Architecture finding (2026-08-22, confirmed by live-substrate exploration):** For a native Run the master, the output reader, and the input writer are three fd *numbers* over ONE open file description (the pty master OFD): `reader = duplicate_cloexec(master)` and `writer = master.take_writer()`. So the manifest carries ONLY the master fd per Run — the incoming image re-derives the reader (re-dup cloexec) and writer (write to the master) exactly as the spawn seam does. Carrying `reader_fd` would be redundant; carrying `writer_fd` is impossible anyway (the portable_pty writer is a `Box<dyn Write + Send>` with no recoverable raw fd). This is the lower-entropy shape and is what shipped.

**Files:**
- Create: `crates/ctxmux-daemon/src/handoff.rs`
- Modify: `crates/ctxmux-daemon/src/lib.rs` (add `mod handoff;`)
- Test: inline `#[cfg(test)]` in `handoff.rs`

**Step 1: Write the failing test**

Create `crates/ctxmux-daemon/src/handoff.rs`:

```rust
//! The upgrade handoff manifest carried across `execve`-in-place.
//!
//! Serialized to a single inherited descriptor by the outgoing image and read
//! back by the incoming image. Versioned so a mismatched upgrade fails closed
//! rather than misreading fd numbers.

use std::os::fd::RawFd;

use serde::{Deserialize, Serialize};

use ctxmux_protocol::RunId;

pub const HANDOFF_SCHEMA: &str = "ctxmux.daemon-handoff.v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HandoffManifest {
    pub schema: String,
    pub epoch: String,
    pub runs: Vec<HandoffRun>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HandoffRun {
    pub run_id: RunId,
    pub child_pid: i32,
    pub master_fd: RawFd,
}

impl HandoffManifest {
    pub fn new(epoch: String, runs: Vec<HandoffRun>) -> Self {
        Self { schema: HANDOFF_SCHEMA.to_string(), epoch, runs }
    }

    /// Every fd number this manifest expects to survive the exec.
    pub fn all_fds(&self) -> Vec<RawFd> {
        self.runs.iter().map(|r| r.master_fd).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_through_json_and_lists_all_fds() {
        let manifest = HandoffManifest::new(
            "epoch-xyz".to_string(),
            vec![HandoffRun {
                run_id: RunId::new(),
                child_pid: 4321,
                master_fd: 7,
            }],
        );
        let bytes = serde_json::to_vec(&manifest).unwrap();
        let parsed: HandoffManifest = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed, manifest);
        assert_eq!(parsed.all_fds(), vec![7]);
        assert_eq!(parsed.schema, HANDOFF_SCHEMA);
    }
}
```

> Check `RunId` has `RunId::new()` and derives `Serialize`/`Deserialize` in `crates/ctxmux-protocol/src/lib.rs`. If the constructor is named differently, use the actual one and adjust the test.

**Step 2: Run test to verify it fails**

Run: `cargo test -p ctxmux-daemon handoff::`
Expected: FAIL — `handoff` module not found / unresolved import.

**Step 3: Write minimal implementation**

Add `mod handoff;` near the other `mod` declarations at the top of `crates/ctxmux-daemon/src/lib.rs`. The file created in Step 1 is the implementation.

**Step 4: Run test to verify it passes**

Run: `cargo test -p ctxmux-daemon handoff::`
Expected: PASS.

**Step 5: Commit**

```bash
git add crates/ctxmux-daemon/src/handoff.rs crates/ctxmux-daemon/src/lib.rs
git commit -m "feat(daemon): add versioned exec-in-place handoff manifest"
```

---

## Task A3: `--handoff-fd` CLI parsing in `ctxmuxd`

**Why:** The incoming image needs to know which inherited fd carries the manifest. Add a `--handoff-fd <fd>` flag mirroring `--readiness-fd`. Unlike the other inherited fds, this one must be readable *without* being made nonblocking (we read the whole manifest once), and must **not** be re-CLOEXEC'd until after we've read it — but the leaf's `duplicate_cloexec` re-sets CLOEXEC on the original, which is fine because we read the *duplicate* before exec finishes. Keep it simple: parse to a `RawFd`, thread through `serve`.

**Files:**
- Modify: `crates/ctxmux-daemon/src/main.rs:10-127` (usage string, arg loop, `serve` signature)
- Modify: `crates/ctxmux-daemon/src/lib.rs` (serve entry points accept `handoff_fd: Option<OwnedFd>`)

**Step 1: Write the failing test**

Add a CLI-level test. `main.rs` has no test module today, so assert via behavior in the integration harness instead — but first add a focused unit check that the manifest reader parses a written manifest. Put it in `handoff.rs`:

```rust
#[test]
fn reads_manifest_from_a_pipe_fd() {
    use std::io::Write;
    use std::os::fd::AsRawFd;
    let (reader, mut writer) = rustix::pipe::pipe().map(|(r, w)| {
        (std::fs::File::from(r), std::fs::File::from(w))
    }).unwrap();
    let manifest = HandoffManifest::new("e".into(), vec![]);
    writer.write_all(&serde_json::to_vec(&manifest).unwrap()).unwrap();
    writer.write_all(b"\n").unwrap();
    drop(writer);
    let parsed = super::read_manifest(reader.as_raw_fd()).unwrap();
    assert_eq!(parsed, manifest);
    drop(reader);
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p ctxmux-daemon handoff::reads_manifest`
Expected: FAIL — `read_manifest` not found.

**Step 3: Write minimal implementation**

In `handoff.rs` add a reader that takes ownership of the raw fd, reads one NDJSON line, and parses:

```rust
/// Read one manifest line from an inherited descriptor, consuming it.
///
/// # Errors
///
/// Returns an error if the descriptor cannot be read or the line is not a
/// valid, current-schema manifest.
pub fn read_manifest(raw_fd: RawFd) -> std::io::Result<HandoffManifest> {
    use std::io::Read;
    // SAFETY boundary: ownership of the inherited fd is taken here so it is
    // closed when this returns. This is a plain read of an fd the parent set up.
    let mut file = {
        use std::os::fd::FromRawFd;
        #[allow(unsafe_code)]
        unsafe { std::fs::File::from_raw_fd(raw_fd) }
    };
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)?;
    let manifest: HandoffManifest = serde_json::from_slice(
        buf.split(|&b| b == b'\n').next().unwrap_or(&[]),
    )
    .map_err(std::io::Error::other)?;
    if manifest.schema != HANDOFF_SCHEMA {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "unknown handoff manifest schema",
        ));
    }
    Ok(manifest)
}
```

> The `#![forbid(unsafe_code)]` at the daemon crate root will reject the inline `unsafe`. Two clean options — pick per the leaf-crate rule: (a) move `read_manifest`'s `File::from_raw_fd` into `ctxmux-inherited-fd` as `owned_file_from_raw(fd) -> File`, or (b) if the daemon crate uses `#![deny(unsafe_code)]` with a targeted `#[allow]`, keep it but audit-comment it. **Prefer (a)** to keep all raw-fd unsafe in the leaf. Adjust the test import accordingly.

Then wire `--handoff-fd` in `main.rs`: extend the `if flag == "--qualification-stats-fd" || flag == "--readiness-fd"` branch to also accept `--handoff-fd` into a third `handoff_fd: Option<RawFd>`, update `usage()`, thread it into `serve(...)` and the `serve_with_state_dir_and_inherited_descriptors` call. Memory-only `serve_with_inherited_descriptors` should reject a handoff fd with a clear error (handoff requires `--state-dir`).

**Step 4: Run test to verify it passes**

Run: `cargo test -p ctxmux-daemon handoff::reads_manifest && cargo build --workspace`
Expected: PASS + clean build.

**Step 5: Commit**

```bash
git add crates/ctxmux-daemon/src/main.rs crates/ctxmux-daemon/src/lib.rs crates/ctxmux-daemon/src/handoff.rs crates/ctxmux-inherited-fd/src/lib.rs
git commit -m "feat(daemon): parse --handoff-fd and read the upgrade manifest"
```

---

## Task A4: Surface the live master fd from an Open native control

**Why:** The upgrade must read each live Run's pty master fd without terminating it. Per the A2 architecture finding, only the master fd is carried — the incoming image re-derives the reader and writer from it. Today `detach_closed_descriptors_after_owner_fence` (native_control.rs) only works once the Run's phase is `Closed`, and the `PtyControl` trait (native_control.rs:355) exposes **no** raw-fd accessor. This task adds exactly that accessor and surfaces it from the open control.

> **Ownership boundary (from live-substrate exploration — corrects the original plan):** the child PID is **not** reachable from `NativeControlOwner`. It lives on the daemon-wide owner's `Watching { child, session, control }` (native_runtime.rs:529-533) — `NativeSession` holds the pid, and the `Child` trait exposes `process_id() -> Option<u32>`. The control owner holds only the pty + writer + reap state. Therefore A4 surfaces **only the master fd** at the control level; pairing it with the child pid is A5's job (the owner is the only actor that sees child + session + control together). Do not try to return a `child_pid` from `NativeControlOwner` — it does not have one.
>
> **Fd-flow facts:** the `PtyControl` trait has no `as_raw_fd`; the concrete `PortablePtyControl(Box<dyn MasterPty + Send>)` wraps a `MasterPty` whose `as_raw_fd() -> Option<RawFd>` *is* available (already used for macOS `interrupt_foreground` at native_control.rs:377-381). So A4 adds a `master_raw_fd(&self) -> Option<RawFd>` method to the `PtyControl` trait (delegating to `self.0.as_raw_fd()` for `PortablePtyControl`; the A7 adapter will return `Some(fd)`), plus a `NativeControlOwner::master_raw_fd()` accessor that reads it through the pty mutex. The writer fd is unrecoverable (boxed `dyn Write`) and the reader fd lives on `OutputOwner.reader` in native_runtime.rs — neither is needed.

**Files:**
- Modify: `crates/ctxmux-daemon/src/native_control.rs` (add `master_raw_fd()` to the `PtyControl` trait + `PortablePtyControl`/`TestPty` impls; add `pub(crate) fn master_raw_fd(&self) -> Option<RawFd>` on `NativeControlOwner`)
- Test: `crates/ctxmux-daemon/src/tests/creation.rs` (start a native Run, read its master fd through the owner, assert it is valid and the Run stays controllable)

**Step 1: Write the failing test**

In the daemon's in-crate test module that already spins up a native Run (see `src/tests/creation.rs`), add:

```rust
#[tokio::test]
async fn master_raw_fd_exposes_the_live_master_without_closing() {
    // Arrange: start a real native Run (reuse the harness helper already used
    // by creation.rs — e.g. spawn `sh`).
    let run = /* start a native run via the existing test helper */;
    let control = /* obtain &NativeControlOwner for run */;

    let master_fd = control.master_raw_fd().expect("open control exposes its master fd");
    assert!(master_fd >= 0);

    // The Run is still alive and controllable afterward (the fd was only read).
    control.write_input(b"echo hi\n").await.expect("still writable");
}
```

> Fill the `/* ... */` from the existing helper in `src/tests/creation.rs`. Read that file first to copy the exact setup idiom (how it obtains the `NativeControlOwner` for a started Run).

**Step 2: Run test to verify it fails**

Run: `cargo test -p ctxmux-daemon master_raw_fd_exposes`
Expected: FAIL — `master_raw_fd` not found.

**Step 3: Write minimal implementation**

In `native_control.rs`: add `fn master_raw_fd(&self) -> Option<std::os::fd::RawFd>` to the `PtyControl` trait (native_control.rs:355), implemented for `PortablePtyControl` by delegating to the inner `MasterPty::as_raw_fd()`, and for the `#[cfg(test)] TestPty` (native_control.rs:417) returning `None`. Then add `pub(crate) fn master_raw_fd(&self) -> Option<RawFd>` on `NativeControlOwner` that locks the pty mutex and calls the trait method on the `Some` control (returns `None` if the pty is already detached). Do **not** dup — return the borrowed raw fd number; the caller (A5→A11) clears CLOEXEC on it just before exec, which is correct because after exec the same fd-table entry persists.

**Step 4: Run test to verify it passes**

Run: `cargo test -p ctxmux-daemon master_raw_fd_exposes`
Expected: PASS.

**Step 5: Commit**

```bash
git add crates/ctxmux-daemon/src/native_control.rs crates/ctxmux-daemon/src/tests/creation.rs
git commit -m "feat(daemon): surface the live master fd from an open native control"
```

---

## Task A5: Native-owner `ExtractForHandoff` command

**Why:** The daemon-wide native owner (native_runtime.rs) is the only actor that can walk every live native Run *and* the only actor that sees `Watching { child, session, control }` together — so it is the only place the master fd (from `control.master_raw_fd()`, A4) can be paired with the child pid (from `session`/`child.process_id()`). A4 deliberately does not return a pid; this task assembles the full `{child_pid, master_fd}` record. Add a command that, distinct from `Shutdown` (which fail-stops via `mem::forget`), returns the `LiveDescriptors` for every live Run *without terminating anything*, and — critically — relinquishes the owner's reap/close authority so the fds survive into the exec instead of being closed when the owner thread later drops.

> **This task defines `LiveDescriptors`** (A4 only exposes the fd; the pid is not visible there):
> ```rust
> #[derive(Debug, Clone, Copy)]
> pub(crate) struct LiveDescriptors {
>     pub run_id: RunId,
>     pub child_pid: u32,
>     pub master_fd: std::os::fd::RawFd,
> }
> ```
> Each field's source is a different member of the same `Watching`: `run_id` from the registration key, `child_pid` from `session`/`child.process_id()`, `master_fd` from `control.master_raw_fd()`. A Run whose `master_raw_fd()` returns `None` (pty already detached) is skipped — it is not live for handoff purposes.

**Files:**
- Modify: `crates/ctxmux-daemon/src/native_runtime.rs` (owner command enum, handler, `LiveDescriptors` struct, `preserve_shutdown_authority`/`mem::forget` analogue)
- Test: `crates/ctxmux-daemon/tests/native_lifecycle.rs`

**Step 1: Write the failing test**

In `crates/ctxmux-daemon/tests/native_lifecycle.rs`, add a test that starts two native Runs, calls the new `extract_for_handoff()` on the owner handle, and asserts it returns two `LiveDescriptors` — each with a nonzero `child_pid`, a `master_fd >= 0`, and the two `run_id`s matching the started Runs — and that both child PIDs are still alive (`kill(pid, 0)` succeeds) afterward.

**Step 2: Run test to verify it fails**

Run: `cargo test -p ctxmux-daemon --test native_lifecycle extract_for_handoff`
Expected: FAIL — method not found.

**Step 3: Write minimal implementation**

Add an `OwnerCommand::ExtractForHandoff { respond }` variant. Its handler walks each live `Watching`, building one `LiveDescriptors` per Run by pairing `control.master_raw_fd()` (A4) with the child pid read from `session`/`child.process_id()` and the registration's `run_id` (skip any Run whose `master_raw_fd()` is `None`). It then transitions each registration into a "handed-off" state that, on owner-thread teardown, does **not** close the fds and does **not** reap the child — the same `mem::forget`/authority-preservation discipline `preserve_shutdown_authority` already uses (native_runtime.rs ~:1248). Return the vector to the caller.

**Step 4: Run test to verify it passes**

Run: `cargo test -p ctxmux-daemon --test native_lifecycle extract_for_handoff`
Expected: PASS.

**Step 5: Commit**

```bash
git add crates/ctxmux-daemon/src/native_runtime.rs crates/ctxmux-daemon/tests/native_lifecycle.rs
git commit -m "feat(daemon): owner command to extract live descriptors for handoff"
```

---

## Task A6: `AdoptedChild` — reap a re-adopted child by PID via `waitid`

**Why:** `portable_pty::Child` has no PID→Child constructor. After exec, the child must be reaped through `waitid(P_PID, pid, WNOWAIT)` probe then reap, preserving the existing non-reaping-probe-then-reap ordering and the reaped-leader latch. native_session.rs already has `from_child_pid` (:42), `leader_is_terminal` (waitid+NOWAIT, :274), `reap_leader` (:291), and the `leader_reaped` latch (:302). This task builds the child handle that plugs into that path.

**Files:**
- Modify: `crates/ctxmux-daemon/src/native_session.rs` (or a new `adopted_child.rs`)
- Test: `crates/ctxmux-daemon/tests/native_lifecycle.rs` — spawn a child, adopt by PID, assert probe-then-reap and latch behavior

**Step 1: Write the failing test**

Spawn a short-lived process (e.g. `sh -c 'exit 7'`) via a helper that yields its PID, construct an `AdoptedChild::from_pid(pid)`, assert: (a) before exit, the non-reaping probe reports "still running"; (b) after exit, probe reports terminal, reap returns exit status 7; (c) the reaped latch flips and a second reap is refused/no-op; (d) the latch starts **unset** so PID reuse cannot pre-authorize.

**Step 2: Run test to verify it fails**

Run: `cargo test -p ctxmux-daemon --test native_lifecycle adopted_child`
Expected: FAIL — `AdoptedChild` not found.

**Step 3: Write minimal implementation**

Implement `AdoptedChild` wrapping a bare `pid`, using the same `waitid` calls native_session.rs already uses. Route it through `NativeSession::from_child_pid` so the SID anchor and reaped-leader latch are shared. Keep any raw `libc::waitid` in the audited path native_session.rs already established (it already does waitid), not a new unsafe site.

**Step 4: Run test to verify it passes**

Run: `cargo test -p ctxmux-daemon --test native_lifecycle adopted_child`
Expected: PASS.

**Step 5: Commit**

```bash
git add crates/ctxmux-daemon/src/native_session.rs crates/ctxmux-daemon/tests/native_lifecycle.rs
git commit -m "feat(daemon): adopt a child by pid and reap it via waitid"
```

---

## Task A7: `MasterPty` adapter over a raw master fd

**Why:** After exec, the master fd is a bare number; portable-pty exposes no constructor from a raw fd. We need a local adapter implementing resize, size query, raw-fd access, and a writer over the bidirectional master fd, so the recovered control owner behaves exactly like a spawned one.

**Files:**
- Create: `crates/ctxmux-daemon/src/adopted_pty.rs`
- Modify: `crates/ctxmux-daemon/src/native_control.rs` (accept the adapter where it holds `PortablePtyControl`)
- Modify: root `Cargo.toml` (enable rustix's `termios` feature — see Step 3)
- Test: inline + `native_lifecycle.rs`

**Step 1: Write the failing test**

Open a real pty pair (via portable-pty's `native_pty_system().openpty(...)` — lib.rs already does this ~:2183), take the master's raw fd and wrap it in an owned handle **without** consuming the original (dup it: `ctxmux_inherited_fd::duplicate_cloexec`), construct `AdoptedMasterPty::from_owned_fd(owned)`, then assert: `resize()` succeeds, `get_size()` returns the set size (round-trip, mirroring the A4 floor test), `master_raw_fd()` returns the fd, and a write reaches the slave. Keep the original master and the slave alive for the duration so the pair is not torn down.

**Step 2: Run test to verify it fails**

Run: `cargo test -p ctxmux-daemon adopted_pty`
Expected: FAIL — module/type not found.

**Step 3: Write minimal implementation**

⚠️ **Constraint (do not violate):** the daemon crate inherits `unsafe_code = "forbid"` from the workspace root (`Cargo.toml [workspace.lints.rust] unsafe_code = "forbid"`, applied via `[lints] workspace = true`). `forbid` **cannot** be re-permitted with `#[allow(unsafe_code)]` — the earlier draft of this step was wrong. All fd/ioctl work must go through **safe** wrappers:

- `AdoptedMasterPty` holds an `OwnedFd` (constructed by the caller via the audited `ctxmux-inherited-fd` seam — `owned_from_raw` / `duplicate_cloexec` — never `unsafe { OwnedFd::from_raw_fd(..) }` in the daemon). Constructor is `from_owned_fd(OwnedFd)`, not `from_raw_fd`.
- resize / size use **rustix's safe termios wrappers** `rustix::termios::{tcgetwinsize, tcsetwinsize}` (returning/taking `Winsize`), not raw `ioctl`. These require the `termios` feature on the rustix dependency, which is **not currently enabled** — add `features = ["termios", …existing]` to the root `Cargo.toml` rustix entry. Map `Winsize ↔ PtySize`: `ws_row↔rows, ws_col↔cols, ws_xpixel↔pixel_width, ws_ypixel↔pixel_height`.
- I/O uses `std::fs::File`/`rustix::io::{read,write}` over the fd — all safe.
- Implement the **`PtyControl` trait** (native_control.rs:355-366), which is **platform-split**: both platforms need `resize`, `get_size`, `master_raw_fd`; additionally macOS needs `interrupt_foreground(&self) -> io::Result<()>` (delegate to `ctxmux_pty_signal::interrupt_foreground(raw_fd)`) and non-macOS needs `foreground_process_group(&self) -> Option<u32>` (via `rustix::termios::tcgetpgrp(fd)`). Mirror `PortablePtyControl`'s exact cfg split, or the adapter won't satisfy the trait on both targets.

**Step 4: Run test to verify it passes**

Run: `cargo test -p ctxmux-daemon adopted_pty` and `cargo clippy -p ctxmux-daemon --all-targets --all-features -- -D warnings`
Expected: PASS, clippy clean (no `unsafe_code` violation).

**Step 5: Commit**

```bash
git add crates/ctxmux-daemon/src/adopted_pty.rs crates/ctxmux-daemon/src/native_control.rs Cargo.toml crates/ctxmux-daemon/src/lib.rs
git commit -m "feat(daemon): MasterPty adapter over an inherited owned master fd"
```

---

## Task A8: Persistence — live-set exclusion in reconciliation + epoch reuse

**Why:** Startup reconciliation turns every `running` row into `interrupted{daemon_restart}` (persistence.rs Reconcile UPDATE ~:1566). Exec-in-place must exclude the handed-off RunIds and reuse the passed epoch instead of minting a fresh UUID (persistence.rs `Uuid::new_v4()` ~:1192; PublishEpoch ~:1607). The three post-normalization "running == 0" asserts (~:1361, :1368, :1657) must relax to "running == |live_set|".

**Files:**
- Modify: `crates/ctxmux-daemon/src/persistence.rs` (open/normalize path accepts `Option<{epoch, live_set: HashSet<RunId>}>`; reconciliation query gains `AND id NOT IN (:live_set)`; epoch allocation reuses when provided)
- Test: `crates/ctxmux-daemon/tests/persistence_recovery.rs`

**Step 1: Write the failing test**

Extend `persistence_recovery.rs`: seed a state dir with two `running` rows. Reopen with a handoff hint `{epoch: <orig>, live_set: {row_A}}`. Assert: row_A stays `running`, row_B becomes `interrupted{daemon_restart}`, and `daemon_instance()` equals the passed epoch (not a fresh UUID). Add a second assertion: reopen with `None` (crash path) → both rows `interrupted`, fresh epoch (today's behavior, unchanged).

**Step 2: Run test to verify it fails**

Run: `cargo test -p ctxmux-daemon --test persistence_recovery live_set`
Expected: FAIL — the open/normalize function has no live-set parameter.

**Step 3: Write minimal implementation**

Thread an `Option<HandoffHint>` into the persistence open/normalize entry. When `Some`, reuse the epoch and add the `NOT IN (:live_set)` clause; relax the three asserts to `running == live_set.len()`. When `None`, behavior is byte-identical to today. Keep the interrupted-with-pid corruption guard (~:3036) intact — handed-off rows keep their pid and stay running, which the guard must allow only under the live-set exclusion.

**Step 4: Run test to verify it passes**

Run: `cargo test -p ctxmux-daemon --test persistence_recovery`
Expected: PASS (new + all existing recovery tests still green).

**Step 5: Commit**

```bash
git add crates/ctxmux-daemon/src/persistence.rs crates/ctxmux-daemon/tests/persistence_recovery.rs
git commit -m "feat(daemon): exclude handed-off runs from reconciliation, reuse epoch"
```

---

## Task A9: `Run::readopt` — re-bind live control onto a recovered Run

**Why:** A recovered Run is built with `incarnation_control: None` / `native_runs: None` (lib.rs `Run::recover` ~:2476). Handed-off Runs must instead get live control re-bound from the inherited fds, with the durable replay cursor re-attached so `append`/`finalize` continue from the true committed byte (never 0, which trips gap-rejection).

**Files:**
- Modify: `crates/ctxmux-daemon/src/lib.rs` (add `Run::readopt(...)` building control from `AdoptedMasterPty` (over the single inherited master fd) + `AdoptedChild` + `NativeSession::from_child_pid`)
- Test: `crates/ctxmux-daemon/tests/native_lifecycle.rs` (unit-level: construct a recovered Run, readopt from a live pty pair, assert it reports `running` and accepts input)

**Step 1: Write the failing test**

Construct a recovered Run row, open a real pty with a live child, and call `Run::readopt` with the master fd + child pid. Assert `status()` is `running` with the child's pid, `input()` echoes, and the durable cursor is non-zero (continues, no gap).

**Step 2: Run test to verify it fails**

Run: `cargo test -p ctxmux-daemon --test native_lifecycle readopt`
Expected: FAIL — `readopt` not found.

**Step 3: Write minimal implementation**

Implement `Run::readopt` composing A6 (`AdoptedChild`) and A7 (`AdoptedMasterPty` over the single inherited master fd), re-deriving the reader and writer from that master exactly as the spawn seam does (reader = `duplicate_cloexec(master)`, writer = the adapter's write path over the master) — the writer and reader are **not** separately inherited fds. Register the Run with the daemon-wide owner exactly like a fresh Run but without spawning. Re-attach the durable cursor from the recovered row.

**Step 4: Run test to verify it passes**

Run: `cargo test -p ctxmux-daemon --test native_lifecycle readopt`
Expected: PASS.

**Step 5: Commit**

```bash
git add crates/ctxmux-daemon/src/lib.rs crates/ctxmux-daemon/tests/native_lifecycle.rs
git commit -m "feat(daemon): readopt live control onto a recovered run"
```

**Landed (2026-08-23):** `6c163a2` (readopt + `NativeControlOwner::new_adopted` shim over `new_with_pty`), then `661b64a` (fix — see finding below).

- **Plan inaccuracy corrected — test lives in lib.rs, not `native_lifecycle.rs`.** `Run` is a private struct, so the integration-test crate cannot reach `Run::readopt`. The test `readopt_rebinds_live_control_and_continues_the_durable_cursor` lives inline in lib.rs's `#[cfg(test)] mod tests` alongside the other direct-`Run` tests. (Also: `Persistence::open` requires the state dir at mode 0700, so the fixture points it at a fresh non-existent subpath that `prepare_state_dir` creates, not the tempdir root.)

> **Review finding (2026-08-23, disputed — kept over an adversarial refutation): `readopt` must thread the manager-shared `wait_failure` and `input_drains`, never fabricate them.** The first cut passed `NativeWaitFailure::default()` and a fresh `InputDrainGate::with_stats(...)`. An adversarial verify pass *refuted* this as a defect at medium confidence, reasoning that `readopt` has no production caller yet. That reasoning is scope-correct for the A9 commit in isolation but wrong for the end-state (面向终态): **A12 is precisely the caller.** Confirmed against the spawn seam (lib.rs `create_unique` ~:962-968), which threads `input_drains: self.native_input_drains.clone()` and `wait_failure: NativeWaitFailure { creation_flights: self.creation_flights.clone(), incarnation_failure: self.incarnation_failure.clone() }`. A `default()` `NativeWaitFailure` carries a **detached** `IncarnationFailure` that the serve loop's fail-stop arm (lib.rs:318, `manager.incarnation_failure.wait()`) never watches — so a re-adopted run that lost child-wait authority would record into a dead-end and the daemon would silently skip its required fail-stop, for exactly the runs that just survived an upgrade. The fresh gate likewise escaped the daemon-wide input-drain budget. Fix `661b64a` adds both as required parameters (no defaults remain in the production body), making the wait-failure wiring structurally guaranteed and proving the input-drain wiring load-bearing (the Defect-2 assertion goes red against the old fabricated gate). **Lesson: "refuted because uncalled" is not a pass when the calling task is already on the plan — judge helper signatures against their intended caller, not their current callers.**

---

## Task A10: Listener from inherited fd + state-lock fd inheritance

**Why:** The new image must reconstruct the `UnixListener` from the inherited socket fd and **skip** `prepare_socket_path`+`bind` (re-binding unlinks/rebinds the inode, drops clients, self-`AlreadyRunning`). The state-lock fd must be inherited (CLOEXEC cleared for it alone, re-asserted after adoption) so no concurrent opener steals the epoch during the exec window (009 state-lock, ~:46-51).

**Files:**
- Modify: `crates/ctxmux-daemon/src/lib.rs` (`serve_with_manager` accepts an optional pre-bound listener fd; a `listener_from_raw_fd` path)
- Modify: `crates/ctxmux-daemon/src/persistence.rs` (state-lock open path accepts an inherited lock fd instead of re-locking)
- Test: `crates/ctxmux-daemon/tests/native_lifecycle.rs`

**Step 1: Write the failing test**

Bind a `UnixListener`, extract its raw fd with CLOEXEC cleared, and assert `serve`'s listener-reconstruction path adopts it without calling `bind` (assert the socket inode's identity is unchanged: same `stat` `st_ino` before/after). For the state lock, assert that reopening persistence with an inherited lock fd does not block on the flock (it's the same fd, already held).

**Step 2: Run test to verify it fails**

Run: `cargo test -p ctxmux-daemon --test native_lifecycle inherited_listener`
Expected: FAIL — no listener-from-fd path.

**Step 3: Write minimal implementation**

Add `UnixListener::from_std(std::os::unix::net::UnixListener::from_raw_fd(fd))` behind the audited leaf, gated on the handoff manifest being present. Skip `prepare_socket_path`/`bind`/`set_permissions` and skip `SocketGuard::new`'s unlink-and-recreate (but keep the guard's *drop* so a genuine final shutdown still unlinks — construct the guard in an "adopted, do-not-recreate" mode). Thread the inherited state-lock fd into persistence open.

**Step 4: Run test to verify it passes**

Run: `cargo test -p ctxmux-daemon --test native_lifecycle inherited_listener`
Expected: PASS.

**Step 5: Commit**

```bash
git add crates/ctxmux-daemon/src/lib.rs crates/ctxmux-daemon/src/persistence.rs crates/ctxmux-daemon/tests/native_lifecycle.rs
git commit -m "feat(daemon): adopt inherited listener and state-lock across exec"
```

**Landed (2026-08-23):** `b89dd62`. Adversarial review (fd-safety / spec / test-integrity / end-state lenses) returned **0 findings** — clean, no fix needed (contrast with A9, which took a follow-up fix).

- **Plan inaccuracies corrected (as with A9):**
  - **Tests live inline, not in `native_lifecycle.rs`.** `adopt_listener` (lib.rs) and `StateLockGuard`/`open_with_handoff` (persistence.rs) are private, so the integration-test crate cannot reach them. The listener test `adopt_listener_reuses_the_socket_inode_without_rebinding` is inline in lib.rs's `#[cfg(test)] mod tests`; the state-lock test `reopening_with_inherited_lock_fd_does_not_self_deadlock` is inline in persistence.rs's test module.
  - **No "adopted, do-not-recreate SocketGuard mode" was added.** The plan (Step 3) called for one, but `SocketGuard::new` already only stats+records identity — it never unlinks/recreates on construction — so it is correct for the adopted path as-is; its `Drop` still unlinks on a genuine final shutdown. Over-specification dropped (避免过度设计).
  - **Listener reconstruction uses the safe `From<OwnedFd>`, not `from_raw_fd`.** The plan's `UnixListener::from_raw_fd` is an `unsafe fn` — forbidden here (`unsafe_code = "forbid"`). The audited path is leaf `owned_from_raw` → `std::os::unix::net::UnixListener::from(OwnedFd)` → `set_nonblocking` → `tokio::from_std`.
  - **`handoff.rs` was extended too** (not in the plan's file list): `HandoffManifest` gains top-level `listener_fd` + `state_lock_fd`, and `all_fds()` returns `[listener_fd, state_lock_fd, ...run master_fds]` so A11 clears CLOEXEC on the full set and A14 can census it.

- **State-lock adoption is the load-bearing subtlety.** `flock` is per open-file-description: the outgoing image's lock survives exec on the inherited fd, so a fresh `open()` + `try_lock()` (today's `StateStore::open` path) self-deadlocks (`WouldBlock` → `StateInUse`) against the process's *own* lock. Adoption threads `state_lock_fd: Option<OwnedFd>` through the existing `HandoffHint` channel and, when `Some`, builds `File::from(fd)` + `StateLockGuard::adopt` (skips the re-lock). The deadlock test proves **both** halves — the contrast (`Persistence::open` while held → `StateInUse`) and the adopt path (`open_with_handoff` with a dup'd, OFD-sharing fd → `Ok`) — so it goes red against a naive re-lock adopt. The pre-existing `state_lock_release_does_not_wait_for_an_inherited_file_description` (persistence.rs) confirms `Drop`'s explicit `File::unlock` releases the OFD lock even with a live `try_clone`'d sibling, so adopt-path teardown is sound.

- **Deferred to A11/A12 by design (verified as correct scope, not gaps):** surfacing the *outgoing* state-lock raw fd is A11's job — the lock `File` is sealed inside the persistence actor thread as `StateStore._state_lock` with no raw-fd accessor. Constructing the incoming `HandoffHint` (epoch + live_set + `owned_from_raw(manifest.state_lock_fd)`) and calling `open_with_handoff`, plus feeding `manifest.listener_fd` to `adopt_listener`, is A12's wiring. `Persistence::open_with_handoff` is the seam A12 calls; it is `#[cfg_attr(not(test), allow(dead_code))]` until then.

---

## Task A11: SIGHUP arm — drain, extract, clear CLOEXEC, write manifest, execve

**Why:** This is the trigger that ties A1–A10 together on the *outgoing* side. Add a `tokio::signal::unix::signal(SignalKind::hangup())` arm to the `serve_with_manager` `select!` (lib.rs:281), parallel to the `ctrl_c` arm.

**Files:**
- Modify: `crates/ctxmux-daemon/src/lib.rs:280-307` (add SIGHUP arm + `perform_exec_upgrade`)
- Test: covered by the integration test in A12 (a real re-exec); add a memory-only unit assertion here that SIGHUP is a no-op.

**Step 1: Write the failing test**

Add to `native_lifecycle.rs` a memory-only test: start a memory-only daemon, send SIGHUP, assert the daemon keeps running and logs a no-op (Run still `running`, same pid). (The persistent-mode real-exec assertion is A12.)

**Step 2: Run test to verify it fails**

Run: `cargo test -p ctxmux-daemon --test native_lifecycle sighup_memory_only_noop`
Expected: FAIL — SIGHUP currently has default disposition (terminates the process).

**Step 3: Write minimal implementation**

Add the arm. On SIGHUP:
- if memory-only (no persistence) → log `"SIGHUP ignored: upgrade continuity requires --state-dir"` and continue the loop;
- else: stop accepting new connections; quiesce tmux control owners as shutdown does; call owner `extract_for_handoff()` (A5); build `HandoffManifest` (A2) with the current epoch and the extracted descriptors; `clear_cloexec` (A1) on every manifest fd + listener fd + state-lock fd + the handoff pipe's write end; write the manifest to the handoff pipe; then `execve` via `std::os::unix::process::CommandExt::exec` on `std::env::current_exe()` with argv `--socket … --state-dir … --handoff-fd <n>` (+ readiness/qualification fds if present).
- On `execve` returning an error (it only returns on failure): fall back to today's shutdown fail-stop (`shutdown_owned_controls`, record incarnation failure) — never leak un-waited children.

**Verified-finding constraints (from the A1–A6 adversarial review; each is a required behavior, not a nicety):**
- **Output barrier before extract (f04).** `extract_for_handoff` swaps `Watching→Done` and `mem::forget`s the control, so the owner's output-reader loop stops draining the master the instant extraction runs. Any bytes buffered in the reader but not yet committed to the durable replay cursor at that moment would be a silent gap the incoming image cannot recover (it resumes from the persisted cursor). **Before** extracting, drive a final durable-commit barrier so the persisted cursor covers every byte read so far; only then extract. Assert in A13 that a fresh attach from the pre-upgrade cursor shows contiguous output with no gap and no duplicate.
- **Extract-then-exec-failure is fail-stop, never dual-ownership (f10).** Once `extract_for_handoff` has run, the children/controls are `mem::forget`-ed — the outgoing image no longer owns them and their fds are marked to survive the exec. If `execve` then fails, there is **no in-image owner to roll back to**; the only correct outcome is fail-stop (record incarnation failure and exit) so the fds are reclaimed by process death, never a path that resumes serving with forgotten controls. Do the fallible, reversible work (drain, barrier, manifest build, cloexec clear, manifest write) **before** extract where possible; treat extract as the point of no return that must be immediately followed by `execve`.
- **CLOEXEC clear ordering + drain (f14).** Clearing CLOEXEC must happen *after* the manifest is finalized and *immediately before* `execve`, on exactly {manifest master fds, listener, state-lock, handoff write end} — nothing else. `duplicate_cloexec` re-sets CLOEXEC on the original fd, so any dup taken for the manifest must not leave the intended-to-survive fd close-on-exec; clear it last. The handoff pipe's write end must be closed by the time `read_manifest` runs in the incoming image (it blocks in `read_to_end` on EOF) — but since `execve` replaces this image, the write end survives into the new image and must be closed there after the manifest is read, or the read hangs. A14 owns the second fd-hygiene boundary that proves no ambient non-CLOEXEC fd leaks in.

**Step 4: Run test to verify it passes**

Run: `cargo test -p ctxmux-daemon --test native_lifecycle sighup_memory_only_noop`
Expected: PASS.

**Step 5: Commit**

```bash
git add crates/ctxmux-daemon/src/lib.rs crates/ctxmux-daemon/tests/native_lifecycle.rs
git commit -m "feat(daemon): SIGHUP drains and execs in place, memory-only is a no-op"
```

---

## Task A12: Incoming-image adopt-on-startup wiring

**Why:** Connect the manifest reader (A3) to the adopt path: when a manifest is present, reuse epoch + live-set exclusion (A8), reconstruct listener + state lock (A10), and `readopt` each Run (A9) *before* the historical recovery loader runs.

**Files:**
- Modify: `crates/ctxmux-daemon/src/lib.rs` (serve-with-state-dir path branches on handoff manifest present)
- Test: A13 (the real end-to-end re-exec).

**Step 1–4:** No new isolated test; this is the integration wiring proven by A13. Build must stay clean:

Run: `cargo build --workspace && cargo clippy --workspace --all-targets`
Expected: no errors, no new warnings.

**Step 5: Commit**

```bash
git add crates/ctxmux-daemon/src/lib.rs
git commit -m "feat(daemon): adopt handed-off runs on startup from the manifest"
```

---

## Task A13: End-to-end real re-exec integration test (the headline proof)

**Why:** This is the acceptance test for Track A and the M3.5 upgrade-continuity claim. It must use a real re-exec of `CARGO_BIN_EXE_ctxmuxd`, not a shim.

**Files:**
- Test: `crates/ctxmux-daemon/tests/native_lifecycle.rs` (extend `TestDaemon` with a `sighup()` and readiness-resume receipt)

**Step 1: Write the failing test**

```
1. start daemon (persistent, with readiness-fd), start an interactive Run (sh), drive I/O to cursor C0;
2. open a live attach, read to C0, record child pid P0;
3. send SIGHUP; wait on a readiness/log receipt that the new image resumed N runs;
4. assert status(id).pid == P0 AND kill(P0, 0) succeeds — same child survived AS A LIVE RUN;
5. input(id, "echo resumed\n") and observe the echo past C0 — master + writer re-adopted;
6. new attach(id, after_byte=C0) shows contiguous output, NO RunEvent::Gap;
7. tail reliability smoke: per-Run fd/thread census unchanged vs a never-upgraded daemon.
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p ctxmux-daemon --test native_lifecycle upgrade_preserves_live_run`
Expected: FAIL initially (whatever seam is still incomplete).

**Step 3: Make it pass**

Iterate across A1–A12 until green. This test is the integration ratchet; fix the *cause*, never weaken the assertions (no relaxing `pid == P0`, no accepting a `Gap`).

**Step 4: Run the full daemon suite**

Run: `cargo test -p ctxmux-daemon`
Expected: PASS, including all pre-existing lifecycle/recovery tests.

**Step 5: Commit**

```bash
git add crates/ctxmux-daemon/tests/native_lifecycle.rs
git commit -m "test(daemon): prove live run survives a real execve upgrade"
```

---

## Task A14: fd-hygiene second boundary + budget contract

**Why:** The re-exec'd image must inherit *nothing* beyond the explicit clear-list. There is an existing ambient-descriptor regression; extend it to the post-exec image. And confirm the frozen per-Run fd/thread budget is unchanged.

**Files:**
- Test: extend the existing fd-hygiene test (find it: `grep -rn "CLOEXEC\|ambient\|fd.hygiene\|proc/self/fd" crates/ctxmux-daemon/tests`)
- Verify: `node scripts/reliability-budget-contract.mjs` (or the documented invocation) passes unchanged.

**Step 1: Write the failing test**

Add an assertion that after an upgrade, `/proc/self/fd` (Linux) or the equivalent census the harness already uses contains only the expected descriptors — the manifest fds, listener, state lock, standard streams — and no ambient leak.

> Note: this repo targets Unix incl. macOS; if the existing hygiene test is Linux-gated, gate the new assertion the same way.

**Step 2: Run test to verify it fails (if a gap exists) or passes**

Run: `cargo test -p ctxmux-daemon fd_hygiene`
Expected: reveals any leaked fd; fix by tightening the clear-list.

**Step 3: Fix any leak**

Ensure `clear_cloexec` is called on *exactly* the clear-list and nothing dups a permanent extra copy.

**Step 4: Verify budgets**

Run: `node scripts/reliability-budget-contract.mjs`
Expected: PASS — `max_threads_per_run` / `max_fds_per_run` slopes unchanged; net per-Run delta across upgrade is 0.

**Step 5: Commit**

```bash
git add crates/ctxmux-daemon/tests reliability-budgets.json
git commit -m "test(daemon): second fd-hygiene boundary across the re-exec image"
```

---

## Task A15: Track A documentation flips (now honest — capability shipped)

**Why:** Only now that Track A is green may the shipped-behavior docs claim it.

**Files:**
- Modify: `docs/architecture.md:13` (guarantees cell → upgrade continuity supported in persistent mode via exec-in-place)
- Modify: `docs/architecture.md` lifecycle narrative (add the exec-in-place path; fd-possession vs PID-guess distinction)
- Modify: `docs/protocol.md` (persistent-mode lifetime boundary gains the upgrade-continuity clause; note no protocol generation change — SIGHUP-triggered)
- Modify: `README.md:44-45` (the "Live PTY control is not adopted after restart" note gains the planned-upgrade exception)

**Steps:** Edit each; then `grep -rn "upgrade continuity are open" docs/` returns nothing stale. Commit:

```bash
git add docs/architecture.md docs/protocol.md README.md
git commit -m "docs: upgrade continuity is supported in persistent mode"
```

---

# TRACK B — semantic resume of interrupted Runs

## Task B1: `resume` verb — Level A (clone recovered spec)

**Why:** Add `ctxmux resume <run-id>` mirroring `fork`. Level A clones the recovered `RunSpec` and starts fresh with lineage — already supported machinery, just no verb.

**Files:**
- Modify: `crates/ctxmux/src/main.rs` (command set ~:74-88; add `resume`)
- Modify: `crates/ctxmux-client/src/lib.rs` (a `resume` client call, or express as start-of-cloned-spec + lineage)
- Modify: `crates/ctxmux-daemon/src/lib.rs` (resume request → clone recovered spec, record lineage)
- Test: `crates/ctxmux-daemon/tests/persistence_recovery.rs`

**Step 1: Write the failing test**

After a real kill+restart leaving Run `interrupted`, call resume; assert a new Run is created, `running`, with recorded lineage to the interrupted Run, and the interrupted Run is unchanged.

**Step 2:** Run: `cargo test -p ctxmux-daemon --test persistence_recovery resume_level_a` → FAIL (no resume).

**Step 3:** Implement resume as a materialized start of the recovered spec + lineage edge. Reject resume of a non-`interrupted` Run.

**Step 4:** Run the test → PASS.

**Step 5: Commit**

```bash
git add crates/ctxmux/src/main.rs crates/ctxmux-client/src/lib.rs crates/ctxmux-daemon/src/lib.rs crates/ctxmux-daemon/tests/persistence_recovery.rs
git commit -m "feat: resume verb reconstructs an interrupted run (level A)"
```

---

## Task B2: Re-derive Level B provenance from durable replay

**Why:** Level B needs the provider session id, which lived only in a live in-memory `WeakMap` (empty after restart). Re-derive it by running the Codex observer over the recovered Run's *durable retained replay*.

**Files:**
- Modify: `packages/sdk/src/integrations/codex.ts` (expose a pure "parse session id from replay bytes" using the existing `thread.started` detection ~:97-107)
- Modify: `packages/sdk/src/integration.ts` (provenance lookup falls back to durable re-derivation when the WeakMap misses)
- Test: `packages/sdk` test suite (feed a captured JSONL replay, assert session id extracted)

**Step 1:** Write a failing SDK test feeding a recorded Codex replay tail and asserting the session id is extracted with no live state.

**Step 2:** Run the SDK test (`pnpm -C packages/sdk test` or the documented command) → FAIL.

**Step 3:** Implement the pure re-derivation and the WeakMap-miss fallback.

**Step 4:** Test → PASS.

**Step 5: Commit**

```bash
git add packages/sdk/src/integrations/codex.ts packages/sdk/src/integration.ts packages/sdk/**/*.test.ts
git commit -m "feat(sdk): re-derive codex resume provenance from durable replay"
```

---

## Task B3: Level B resume path — materialize provider-resume start

**Why:** With provenance re-derived (B2), model Level B resume as a plain start of the materialized `codex exec resume <session-id> …` spec with lineage, honoring the Level B contract from durable inputs. If provenance is unavailable, degrade to Level A and surface that fact.

**Files:**
- Modify: `packages/sdk` resume orchestration; `crates/ctxmux-daemon/src/lib.rs` resume path threads the materialized spec.
- Test: SDK + daemon integration.

**Step 1:** Failing test: resume a recovered Codex Run → new Run's spec is the materialized `exec resume` with the re-derived session id; resume of a Run whose replay lacks the marker → Level A fallback, explicitly reported.

**Step 2:** Run → FAIL.
**Step 3:** Implement.
**Step 4:** Run → PASS.
**Step 5: Commit**

```bash
git add packages/sdk crates/ctxmux-daemon/src/lib.rs
git commit -m "feat: level B resume materializes provider-resume from durable provenance"
```

---

## Task B4: Track B documentation

**Files:**
- Modify: `docs/roadmap.md` M3.5 (semantic resume shipped; crash-time live handoff + PID adoption remain unsupported)
- Modify: `docs/architecture.md` (resume in the lifecycle narrative; Level A vs Level B honesty)
- Modify: `README.md` (mention `resume`)

**Commit:**

```bash
git add docs/roadmap.md docs/architecture.md README.md
git commit -m "docs: semantic resume of interrupted runs"
```

---

## Final gate (both tracks)

Run the whole suite and the documented check script:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
node scripts/reliability-budget-contract.mjs
bash scripts/check.sh   # if this is the repo's canonical gate; confirm the name
```

Expected: all green; per-Run fd/thread budgets unchanged; the real-re-exec test and the restart-resume test both pass. Update `docs/plans/2026-08-22-daemon-upgrade-continuity-design.md` status to "implemented" only when this gate is green.

---

## Task ordering & dependencies

- A1 → A2 → A3 (fd primitive → manifest → CLI reader)
- A4 → A5 (live-fd read → owner extract command)
- A6, A7 independent (child adopt; pty adapter) — can interleave
- A8 independent (persistence) — can start early
- A9 needs A6+A7; A10 independent; A11 needs A1+A2+A5; A12 needs A3+A8+A9+A10
- **A13 needs everything A1–A12** (the ratchet)
- A14, A15 after A13
- Track B (B1–B4) strictly after A13 green.

Independent tasks (A4-family, A6, A7, A8, A10) are the natural parallelization seams if executing with concurrent subagents.
