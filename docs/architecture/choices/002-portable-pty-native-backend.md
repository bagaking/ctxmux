# 002 — `portable-pty` native Backend

- Status: accepted
- Scope: current local PTY and child-process implementation

## Context

Interactive shells and coding Agents require terminal semantics: a PTY, terminal size, raw bytes, child status, and a way to stop the process. Reimplementing platform PTY setup would add security and portability risk before ctxmux has a unique need.

## Decision

The native Backend uses `portable-pty`. The daemon opens a platform-native PTY, configures a `CommandBuilder`, spawns the child on the slave, and retains the master, writer, reader, and cloned child killer.

Raw PTY bytes are the runtime truth. UTF-8 decoding and terminal-screen interpretation remain client concerns.

## Quality attributes and invariants

- The daemon, not the client, retains every PTY handle required to operate the Run.
- Input is byte-preserving; output may contain NUL, split UTF-8, and terminal control sequences.
- Resize rejects zero rows or columns.
- An exited Run rejects input, resize, and repeated stop explicitly.

## Alternatives

- Direct `forkpty` or ConPTY bindings would duplicate maintained platform work.
- Pipes do not provide terminal behavior expected by shells and TUIs.
- Making tmux the only Backend would transfer ownership and installation policy to tmux rather than establish a native baseline.

## Known constraints

The current code has no declared process-group or descendant-kill contract. `ChildKiller::kill` behavior is delegated to `portable-pty`; only the direct child's observable exit is tested. Exit-code and signal mapping depend on the library. Windows is excluded by the current Unix-socket transport before PTY portability is exercised.

The waiter allows the reader one second to finish. This is a bounded drain, not an unbounded final-output guarantee.

## Wrong-case corpus

Evidence pack: [native-pty track](../../../.bagakit/researcher/topics/engineering/ctxmux-wrong-case-corpus/tracks/native-pty.md), claim `C002`.

- `PTY-001` (`b01`): an inherited blocked signal mask once made a launched shell ignore terminal interrupts. The case remains inactive until a safe checked-in launcher seam can test the `portable-pty` boundary without adding test-only unsafe/native code.
- `PTY-002` (`b02`): ambient non-CLOEXEC descriptors leaked authority and garbage into a PTY child. A sentinel descriptor must be absent unless an inheritance API explicitly declares it.
- `PTY-003` (`b03`): ctxmux retains a cloned Unix killer that sends only `SIGHUP`; a HUP-ignoring child can remain running after `stop` is accepted. This is a characterization until stop semantics are chosen.

The signal-mask and descriptor bugs are fixed upstream. They justify dependency-contract fixtures, not copied spawn code. HUP remains useful for cleanup; the error is equating successful delivery with termination.

## Fixture mapping

- Covered now: real input, resize through `stty size`, exit, invalid dimensions, and repeated stop.
- Active: controlled ambient descriptor is absent in the native child.
- Active: binary output, NUL, and split UTF-8 remain exact through replay.
- Characterization: high-volume or delayed descendant output at exit.
- Future: inherited signal mask, process-tree termination, and alternate-screen presentation boundaries.

## Open questions

- Does stop mean direct child, process group, session, or declared tree?
- What terminal signals must the Backend expose or forward?
- What behavior is portable across Unix and Windows?
- When does PTY reader failure become public Run state rather than a daemon log?

## Repository evidence

- `crates/ctxmux-daemon/src/lib.rs`: `Run::spawn`, `read_output`, `Run::resize`, `Run::stop`
- `crates/ctxmux-daemon/tests/native_lifecycle.rs`
- `Cargo.toml`: `portable-pty`
