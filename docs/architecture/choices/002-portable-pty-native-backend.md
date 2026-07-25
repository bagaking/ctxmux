# 002 — `portable-pty` native Backend

- Status: accepted
- Scope: current local PTY and child-process implementation

## Context

Interactive shells and coding Agents require terminal semantics: a PTY, terminal size, raw bytes, child status, and a way to stop the process. Reimplementing platform PTY setup would add security and portability risk before ctxmux has a unique need.

## Decision

The native Backend uses `portable-pty`. The daemon opens a platform-native PTY, configures a `CommandBuilder`, spawns the child on the slave, and retains the master and writer. The waiter thread retains the actual owned child handle and receives stop commands through one owner-local channel; ctxmux does not signal a cached numeric PID from another thread.

Raw PTY bytes are the runtime truth. UTF-8 decoding and terminal-screen interpretation remain client concerns.

## Quality attributes and invariants

- The daemon, not the client, retains every PTY handle required to operate the Run.
- Input is byte-preserving; output may contain NUL, split UTF-8, and terminal control sequences.
- Resize rejects zero rows or columns.
- An exited Run rejects input, resize, and repeated stop explicitly.
- Stop means eventual termination of the owned direct child, not process-group
  or descendant-tree termination. On Unix the native child handle escalates
  from `SIGHUP` to a forced kill when the child ignores HUP.

## Alternatives

- Direct `forkpty` or ConPTY bindings would duplicate maintained platform work.
- Pipes do not provide terminal behavior expected by shells and TUIs.
- Making tmux the only Backend would transfer ownership and installation policy to tmux rather than establish a native baseline.

## Known constraints

The current code deliberately has no process-group or descendant-kill contract. Direct-child termination delegates its HUP grace period and forced-kill escalation to `portable-pty`; real tests cover a HUP-ignoring child and a wait/publication identity barrier. Exit-code and signal mapping depend on the library. Windows is excluded by the current Unix-socket transport before PTY portability is exercised.

The waiter allows the reader one second to finish. This is a bounded drain, not an unbounded final-output guarantee.

## Wrong-case corpus

Evidence pack: [native-pty track](../../../.bagakit/researcher/topics/engineering/ctxmux-wrong-case-corpus/tracks/native-pty.md), claim `C002`.

- `PTY-001` (`b01`): an inherited blocked signal mask once made a launched shell ignore terminal interrupts. The case remains inactive until a safe checked-in launcher seam can test the `portable-pty` boundary without adding test-only unsafe/native code.
- `PTY-002` (`b02`): ambient non-CLOEXEC descriptors leaked authority and garbage into a PTY child. A sentinel descriptor must be absent unless an inheritance API explicitly declares it.
- `PTY-003` (`b03`): retaining the cloned Unix killer sent only `SIGHUP`, so a HUP-ignoring child remained running after `stop` was accepted. The waiter now invokes kill on the owned child handle, whose Unix implementation escalates when HUP is ignored.

The signal-mask and descriptor bugs are fixed upstream. They justify dependency-contract fixtures, not copied spawn code. HUP remains useful for cleanup; the error is equating successful delivery with termination.

## Fixture mapping

- Covered now: real input, resize through `stty size`, exit, invalid dimensions, repeated stop, and eventual direct-child termination when HUP is ignored.
- Active: controlled ambient descriptor is absent in the native child.
- Active: binary output, NUL, and split UTF-8 remain exact through replay.
- Characterization: high-volume or delayed descendant output at exit.
- Future: inherited signal mask, process-tree termination, and alternate-screen presentation boundaries. Process-tree termination would require a separately reviewed contract rather than being inferred from direct-child stop.

## Open questions

- What separately declared capability, if any, should add process-group or descendant-tree termination beyond current direct-child stop?
- What terminal signals must the Backend expose or forward?
- What behavior is portable across Unix and Windows?
- When does PTY reader failure become public Run state rather than a daemon log?

## Repository evidence

- `crates/ctxmux-daemon/src/lib.rs`: `Run::spawn`, `read_output`, `Run::resize`, `Run::stop`
- `crates/ctxmux-daemon/tests/native_lifecycle.rs`
- `Cargo.toml`: `portable-pty`
