# 002 — `portable-pty` native Backend

- Status: accepted
- Scope: current local PTY and child-process implementation

## Context

Interactive shells and coding Agents require terminal semantics: a PTY, terminal size, raw bytes, child status, and a way to stop the process. Reimplementing platform PTY setup would add security and portability risk before ctxmux has a unique need.

## Decision

The native Backend uses `portable-pty`. The daemon opens a platform-native PTY,
configures a `CommandBuilder`, spawns the child on the slave, and retains the
master and writer. On POSIX, `portable-pty` calls `setsid()` before exec, making
the direct child the Run session leader. The waiter thread retains the actual
child handle, uses non-reaping `waitid` to keep its terminal incarnation as the
session anchor through descendant cleanup, and receives Signal and Stop through one owner-local channel;
ctxmux does not signal from a client task or from persisted PID metadata.

Raw PTY bytes are the runtime truth. UTF-8 decoding and terminal-screen interpretation remain client concerns.

Native children receive `TERM=xterm-256color` and `COLORTERM=truecolor`
unless the `RunSpec` already sets those names. The overlay is spawn identity
so a Run does not inherit the daemon host terminal; it is not a screen model.

## Quality attributes and invariants

- The daemon, not the client, retains every PTY handle required to operate the Run.
- Input is byte-preserving; output may contain NUL, split UTF-8, and terminal control sequences.
- Resize rejects zero rows or columns.
- An exited Run rejects input, resize, and repeated stop explicitly.
- Interrupt delivers `SIGINT` only to the PTY foreground process group after
  proving that group still belongs to the owned session.
- Stop means bounded graceful then forced termination of the complete owned
  session. Success requires direct-child reap and an empty session, and reports
  whether force was required.

## Alternatives

- Direct `forkpty` or ConPTY bindings would duplicate maintained platform work.
- Pipes do not provide terminal behavior expected by shells and TUIs.
- Making tmux the only Backend would transfer ownership and installation policy to tmux rather than establish a native baseline.

## Known constraints

The complete-tree contract is exactly one POSIX session. A descendant that
calls `setsid()` explicitly crosses that boundary; ctxmux does not claim host-
wide ancestry control. Session enumeration is an operating-system snapshot, so
every PID is revalidated against the owned SID immediately before signalling.
On macOS the daemon obtains only a bounded PID snapshot through the existing
audited libproc leaf; a full buffer is retried with bounded growth and never
accepted as complete. Only `ESRCH` proves a vanished census member; other SID
lookup failures reject the operation. The leader stays waitable until an empty
descendant census is proven, so its numeric SID cannot be reused while signal
authority exists.
Exit-code and signal mapping still depend on `portable-pty`. Windows is excluded
by the current Unix-socket transport before PTY portability is exercised.

The waiter allows the reader one second to finish. This is a bounded drain, not an unbounded final-output guarantee.

## Wrong-case corpus

Evidence pack: [native-pty track](../../../.bagakit/researcher/topics/engineering/ctxmux-wrong-case-corpus/tracks/native-pty.md), claim `C002`.

- `PTY-001` (`b01`): an inherited blocked signal mask once made a launched shell ignore terminal interrupts. The case remains inactive until a safe checked-in launcher seam can test the `portable-pty` boundary without adding test-only unsafe/native code.
- `PTY-002` (`b02`): ambient non-CLOEXEC descriptors leaked authority and garbage into a PTY child. A sentinel descriptor must be absent unless an inheritance API explicitly declares it.
- `PTY-003` (`b03`): retaining the cloned Unix killer sent only `SIGHUP`, so a HUP-ignoring child remained running after `stop` was accepted. The waiter now invokes kill on the owned child handle, whose Unix implementation escalates when HUP is ignored.

The signal-mask and descriptor bugs are fixed upstream. They justify dependency-contract fixtures, not copied spawn code. HUP remains useful for cleanup; the error is equating successful delivery with termination.

## Fixture mapping

- Covered now: native child `TERM`/`COLORTERM` identity with explicit `RunSpec.env` override.
- Covered now: real input, resize through `stty size`, exit, invalid dimensions,
  foreground-group Interrupt, repeated Stop, stubborn child and descendants,
  forced disposition, unrelated-process and reused-numeric-identity safety,
  saturated-census failure, and concurrent Interrupt/Stop/natural exit.
- Active: controlled ambient descriptor is absent in the native child.
- Active: binary output, NUL, and split UTF-8 remain exact through replay.
- Characterization: high-volume or delayed descendant output at exit.
- Future: inherited signal mask and alternate-screen presentation boundaries.

## Open questions

- Should a future non-POSIX Backend expose another owner scope with equivalent
  complete-tree semantics?
- What signals beyond Interrupt have portable, Agent-neutral value?
- What behavior is portable across Unix and Windows?
- When does PTY reader failure become public Run state rather than a daemon log?

## Repository evidence

- `crates/ctxmux-daemon/src/native_spawn_env.rs`: default `TERM` / `COLORTERM`
- `crates/ctxmux-daemon/src/lib.rs`: `Run::spawn`, `read_output`, `Run::resize`, `Run::stop`
- `crates/ctxmux-daemon/tests/native_lifecycle.rs`
- `crates/ctxmux-daemon/tests/native_terminal_identity.rs`
- `Cargo.toml`: `portable-pty`
