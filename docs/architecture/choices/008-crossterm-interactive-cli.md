# 008 — `crossterm` interactive CLI

- Status: accepted
- Scope: operator-facing attach, raw mode, resize, and detach UX

## Context

The CLI is both a real client and the simplest proof of tmux-like attach and detach behavior. Interactive input needs raw terminal mode, window-size propagation, lossless prefix handling, and reliable restoration when the attachment ends.

## Decision

When stdin and stdout are terminals, `ctxmux attach` reconstructs the current
visible screen from retained raw bytes, paints that still frame under
synchronized output (`CSI ? 2026`), applies terminal size, enables raw mode
with a drop guard, reads input on one blocking thread, observes `SIGWINCH`,
and selects between input, resize, and live Run events. `Ctrl-b d` detaches.
Other `Ctrl-b` combinations are forwarded byte-for-byte. Live output after the
initial paint remains raw passthrough.

When either stream is not a terminal, or the Run has already exited, attach
writes retained raw bytes and never reconstructs a screen.

## Quality attributes and invariants

- Raw mode is restored on ordinary Rust error and return paths after the guard is created.
- The detach prefix is not forwarded to the child.
- A split prefix sequence across reads has the same meaning as one read.
- Zero-sized terminal reports retain the Run's previous valid size.
- Detach waits for the Rust client's daemon acknowledgement.

## Alternatives

- Cooked-mode input cannot faithfully drive shells and TUIs.
- Putting a terminal emulator in the daemon would replace raw replay with a
  screen oracle. The CLI reconstructs a view; `OutputLog` stays raw bytes.
- Reusing tmux for the native path would avoid proving ctxmux's own client boundary.

## Known constraints

A checked-in controlling-PTY test covers raw-mode entry, input, `SIGWINCH`
resize propagation, acknowledged detach, exact terminal restoration for that
ordinary detach path, and survival of the same daemon-owned Run PID.
Signal-driven process termination, daemon loss, and unwind remain broader
restoration cases. The background stdin thread cannot be cancelled cleanly and
may remain blocked until process exit. Output backpressure blocks the
interactive loop while stdout is written.

On `Gap`, the CLI tells the user the daemon's latest output byte but does not
track and print the last successfully displayed byte cursor needed for a
precise reattach command.
On `ObservationDiscontinuity`, it exits nonzero with a distinct diagnostic:
byte replay cannot reconstruct the missing non-output semantics. The raw-mode
guard still owns ordinary terminal restoration on that error path.

## Wrong-case corpus

- `CLI-01` (`h01`, `h02`): raw mode is process-external state. A real macOS PTY fixture attempt restored stable settings but changed transient `PENDIN`; the case remains inactive until the project chooses a direct termios owner or a reviewed normalization policy. `SIGKILL` and abort still cannot be promised away by a drop guard.
- `CLI-02` (`h02`): cleanup must restore the user's meaningful prior termios state, not guessed defaults such as `stty sane`. “Byte-identical” is not yet a valid oracle on macOS because the driver can change transient `PENDIN` during raw-to-canonical restoration.
- `CLI-03` (`h03`): prefix meaning must be independent of read partitioning and EOF must not strand a trailing prefix. Full tmux key-table semantics remain outside the current promise.

`RawModeGuard` should already protect ordinary errors and unwinding. Real-PTY fixtures verify that behavior; they do not justify an impossible universal restoration claim after non-catchable termination.

## Fixture mapping

- Covered now: prefix router, trailing prefix, non-detach forwarding, and zero-size fallback.
- Covered now: checked-in controlling-PTY attach, raw input, `SIGWINCH` resize,
  `Ctrl-b d`, terminal restoration, surviving Run identity, and current-screen
  reconstruction that drops erased CSI history.
- Future: restoration across daemon loss, recoverable errors, unwind, and the
  reviewed transient-termios policy.
- Future: raw restoration on signals after the catchable-signal policy is defined.
- Covered: exhaustive split-prefix, trailing-prefix, unknown sequence, and detach-byte suppression.
- Candidate: resize propagation under repeated `SIGWINCH` and slow stdout.

## Open questions

- Which signals must be handled explicitly to restore terminal state?
- Should stdin be integrated through async file descriptors on supported Unix systems?
- What read-only and multi-attacher terminal-ownership modes are required?
- How does the CLI persist its last observed byte cursor for gap recovery?

## Repository evidence

- `crates/ctxmux/src/main.rs`: `attach`, `PrefixRouter`, `RawModeGuard`
- `crates/ctxmux/src/screen.rs`: interactive current-screen reconstruction
- `crates/ctxmux/tests/interactive_attach.rs`
- `crates/ctxmux-client/src/lib.rs`: `Attachment::detach`
- `Cargo.toml`: `crossterm`, `vt100`
