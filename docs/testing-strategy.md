# Test Evidence Strategy

## Purpose

Ctxmux owns long-lived processes, PTYs, output history, and control across client
lifetimes. A defect can leak a process, lose bytes, signal the wrong PID, leave a
terminal corrupted, or silently weaken fork fidelity. The test standard must
therefore be higher than “the suite is green” or “line coverage is high.”

The release question is:

> For every guarantee ctxmux publishes, what executable evidence proves the
> owner boundary, failure behavior, concurrency behavior, resource bound, and
> supported platform?

This document records the 2026-08-10 audit and the target evidence system. It is
a testing policy and adoption sequence, not a claim that every target lane is
already implemented.

## What coverage means

Ctxmux uses a vector of evidence instead of one coverage percentage:

1. **Contract coverage:** every public guarantee maps to an executable oracle.
2. **Owner-boundary coverage:** lifecycle claims cross the real daemon, process,
   PTY, socket, or external-tool boundary that owns the behavior.
3. **Input-space coverage:** corpora, boundary enumeration, properties, and
   fuzzing explore malformed and fragmented input.
4. **Interleaving coverage:** barriers, model checks, and seeded schedules force
   races instead of hoping normal tests encounter them.
5. **Failure coverage:** deterministic fault points and process-level chaos
   prove rollback, fail-closed behavior, and recovery.
6. **Load and resource coverage:** stress and soak tests assert throughput,
   bounded memory, cleanup, and eventual quiescence.
7. **Platform coverage:** a test counts only on the operating systems and jobs
   that actually execute it.
8. **Code coverage:** line and changed-line coverage remain useful floors for
   unexercised logic, but cannot upgrade a mock into lifecycle evidence.

Test counts, snapshots, and aggregate line coverage are secondary inventory.
One real same-PID reconnect fixture can carry more product evidence than dozens
of shape tests; one missing PID-reuse or replay race can invalidate a central
guarantee even when every line executed.

## Current ctxmux audit

The repository gate is `scripts/check.sh`. It runs formatting, Clippy, Rust
tests, protocol generation drift, TypeScript formatting/typechecking/build,
SDK tests, and two cross-language real-daemon tests. GitHub runs that gate for
pushes and pull requests on one `ubuntu-latest` job. The full gate also passed
locally on Darwin arm64 during this audit.

There is currently no checked-in line-coverage reporter or threshold, fuzzer,
property-test framework, deterministic scheduler, sanitizer job, benchmark,
resource census, load runner, or durable macOS CI job.

| Contract area                                    | Strongest current evidence                                                                                  | Confidence                              | Important gap                                                                                      |
| ------------------------------------------------ | ----------------------------------------------------------------------------------------------------------- | --------------------------------------- | -------------------------------------------------------------------------------------------------- |
| Daemon-owned Run survives client disconnect      | Real Rust daemon/PTY E2E plus CLI/TypeScript cross-language E2E; same PID is controlled after reconnect     | Strong for normal timing                | No Integration-host process-exit fixture and no restart guarantee                                  |
| Start, input, resize, output, exit, detach, stop | Real shell child through public Unix-socket clients                                                         | Strong for the happy lifecycle          | Concurrent mutations, hostile child, process tree, and stop-after-wait/PID-reuse race are unproved |
| Rejected post-spawn setup                        | Deterministic owner seam with a real child proves kill, reap, and no published Run                          | Strong owner-level evidence             | Not driven through a public request; final handoff failure is not forced                           |
| Ordered binary replay after exit                 | Real PTY replay preserves NUL, invalid UTF-8, control bytes, and terminal exit                              | Strong for retained late replay         | Public live lag → `Gap` → cursor reattach is missing                                               |
| 4 MiB retention and truncation arithmetic        | `OutputLog` units cover cursor boundaries and oversized chunks                                              | Strong arithmetic, weak system evidence | No real high-volume daemon/SDK fixture crosses retention                                           |
| Protocol generation and malformed frames         | Shared corpus reaches Rust decode, real daemon, and Node validation; exact frame cap is checked             | Strong for retained cases               | No continuous fuzzing or Rust-produced all-variant golden matrix                                   |
| Socket path safety and mode `0600`               | Sequential path-type, live-listener, stale-socket, and permission tests                                     | Good for ordinary startup               | Stale-path swap/TOCTOU and peer-identity policy are untested                                       |
| SDK backpressure and close races                 | Deterministic mock socket tests, including 20,000 queued frames, plus real clean detach                     | Strong for SDK queue logic              | Real daemon lag recovery and silent-peer cancellation are absent                                   |
| Level A fork                                     | Real daemon proves copied `RunSpec`, declared inputs, lineage, distinct PID, and independent control        | Strong                                  | Post-spawn fork failure is not forced through the public boundary                                  |
| Codex Level B                                    | Cross-language E2E proves exact planned argv, lineage, and distinct real child using a recording executable | Strong adapter proof only               | It does not prove a real Codex session retains prior semantic context                              |
| Interactive CLI attach                           | Prefix-state units and one archived manual macOS PTY characterization                                       | Weak                                    | Checked-in PTY E2E for raw mode, resize, detach, restoration, and Run survival is absent           |
| Future tmux Backend and restart recovery         | Architecture decisions and future cases only                                                                | Not implemented                         | Their activation suites must precede capability claims                                             |

The audit found 24 Rust tests, 25 TypeScript unit/mock/schema tests, two
TypeScript-to-Rust real-daemon E2Es, and seven fixture-validator tests. These
counts describe gate composition only; they are not a maturity score.

## Competitive evidence

The comparison uses immutable upstream revisions:

- tmux [`851c5a9`](https://github.com/tmux/tmux/commit/851c5a933d4838c32ad06c248b2ba975d106149c)
- Zellij [`5254e4f`](https://github.com/zellij-org/zellij/commit/5254e4fc1dd784ef872644190dc5e2bcb0981bed)
- WezTerm/`portable-pty` [`4b1c3c1`](https://github.com/wezterm/wezterm/commit/4b1c3c151eb530e569f867e1461693c56fe89695)
- Alacritty [`1b2b36a`](https://github.com/alacritty/alacritty/commit/1b2b36a64e88068ad02c95fad00ee2fad31c00bf)

“Absent” below means no first-party target was found in the inspected pinned
tree, manifests, and workflows. “Unknown” means the available test cannot prove
the property. Neither label excludes private CI or downstream testing.

| Dimension                    | ctxmux now                                                     | tmux                                                                        | Zellij                                                          | WezTerm / `portable-pty`                                                    | Alacritty                                                            |
| ---------------------------- | -------------------------------------------------------------- | --------------------------------------------------------------------------- | --------------------------------------------------------------- | --------------------------------------------------------------------------- | -------------------------------------------------------------------- |
| Real owner-boundary E2E      | Strong but narrow native Run slice                             | Very broad real server/client/PTY shell regressions                         | Strong Linux real-PTY E2E                                       | Remote SSH PTY exists; local backend lifecycle proof is thin                | One Windows child-exit case; mux lifecycle is out of scope           |
| Deterministic whole-app seam | Partial owner fault seam and SDK mocks                         | No general fake OS harness found                                            | Strong fake PTY plus real server/client threads and IPC         | Injectable PTY interface exists, but no corresponding lifecycle suite found | Terminal model is deterministic, not a mux seam                      |
| Replay/golden evidence       | Malformed corpus and exact raw-byte replay                     | Many exact state/transcript checks and screen goldens                       | `insta` snapshots for fake and real E2E                         | Parser action snapshots                                                     | Strong recorded bytes → semantic terminal-grid goldens               |
| Chaos/fault injection        | Partial launch faults and transport failures                   | Scenario faults: killed/stalled clients, permission errors, unread FIFO     | Graceful quit/resurrection only; no systematic chaos            | No lifecycle chaos target found                                             | One child-kill path; no chaos target                                 |
| Stress/soak                  | Mock 20,000-frame queue and small replay units                 | Bounded reflow, lifecycle, and output-backlog stresses                      | No dedicated load/soak target found                             | No PTY/mux load target found                                                | No process/PTY load target found                                     |
| Concurrency/race             | Weak; key replay and stop races are not forced                 | Real black-box concurrency regressions, but no deterministic scheduler/TSan | Good deterministic multi-client tests; no scheduler exploration | Concurrent production code, little matching test evidence                   | One race-sensitive Windows watcher test                              |
| Fuzz/sanitizer/security      | Malformed corpus and socket/argv tests; no fuzzer/sanitizer CI | Four parser fuzz targets; ASan regression build; fuzz not run there         | No first-party fuzz/sanitizer target found                      | No first-party fuzz/sanitizer target found                                  | No first-party fuzz/sanitizer target found                           |
| Resource leaks               | Launch rollback and limited attachment/fd checks               | Cleanup proxies and ASan; no explicit FD/zombie/RSS census                  | Functional joins/count settling; direct leak freedom unknown    | Careful implementation/drop behavior; direct leak freedom unknown           | Direct handle/process leak freedom unknown                           |
| Benchmark/performance        | Absent                                                         | No regression benchmark found                                               | No PTY/mux benchmark found                                      | Adjacent cell/width/range benches only; no PTY lifecycle benchmark          | No relevant benchmark found                                          |
| Platform evidence            | Required Ubuntu CI; local macOS pass is not durable CI         | Nightly/manual Ubuntu x64 + macOS arm64                                     | Unit matrix is broad; whole-app and real PTY are Linux-only     | Broadest cargo CI: Linux, macOS, Windows; local PTY behavior still unproved | Push/PR tests on Windows + macOS; Linux absent in inspected workflow |
| Submission reach             | Critical suite on every push/PR                                | Full regression is daily/manual, not push/PR                                | Unit, fake-PTY integration, and real-PTY E2E on push/PR         | Cross-platform cargo tests on relevant changes                              | Cargo tests on every push/PR                                         |

### What to borrow

- From [tmux's harness](https://github.com/tmux/tmux/blob/851c5a933d4838c32ad06c248b2ba975d106149c/regress/Makefile): private runtime namespaces, empty environments, real processes, exact structured or byte oracles, bounded negative-event windows, and per-fixture failure logs. Do not copy its serial glob, sleep-heavy synchronization, or schedule-only submission protection.
- From [Zellij's fake PTY](https://github.com/zellij-org/zellij/blob/5254e4fc1dd784ef872644190dc5e2bcb0981bed/zellij-integration-tests/src/fake_pty.rs) and [real-PTY workflow](https://github.com/zellij-org/zellij/blob/5254e4fc1dd784ef872644190dc5e2bcb0981bed/.github/workflows/e2e.yml): deterministic whole-application interleavings plus a smaller real-system contract suite. Neither substitutes for the other.
- From [`portable-pty`'s interface](https://github.com/wezterm/wezterm/blob/4b1c3c151eb530e569f867e1461693c56fe89695/pty/src/lib.rs): explicit spawn/read/write/resize/wait/kill contracts. Do not infer tested portability from API shape or compilation.
- From [Alacritty's reference harness](https://github.com/alacritty/alacritty/blob/1b2b36a64e88068ad02c95fad00ee2fad31c00bf/alacritty_terminal/tests/ref.rs): recorded byte streams and semantic expected state. Use this for protocol and Integration reducers, not as evidence for process identity or attachment lifecycle.

The peers are strongest in different layers; none combines broad real lifecycle,
deterministic interleavings, fuzz/sanitizers, systematic chaos/load, resource
census, performance budgets, and cross-platform live PTY evidence. Ctxmux should
combine the useful patterns and explicitly cover that shared gap.

## Target test architecture

### Layer 1 — pure contracts

Run on every pull request and every supported platform:

- lifecycle state transitions and error codes;
- replay retention/cursor arithmetic;
- prefix routing, frame encoding, schema validation, and capability gating;
- exhaustive input partitioning at every byte boundary for bounded fixtures;
- property tests for state machines and serialization round trips.

The oracle is exact typed state, error, bytes, or declared capability. Snapshots
are appropriate only when the owner semantics are stable and reviewable.

### Layer 2 — deterministic owner seams

Use the smallest test-only seam that can stop an owner transition at a precise
barrier:

- process spawned but reader/writer/waiter ownership not complete;
- attachment subscribed but replay snapshot not captured;
- child wait complete but terminal state not published;
- socket target checked but not removed/bound;
- client frame accepted but downstream consumer stalled;
- future persistence generation written but not committed.

The seam must remain owner-local and test-only. Do not build a general public
fault-injection or plugin framework. Each fault has a deterministic seed or
barrier and asserts cleanup, no partial publication, exact error, and continued
daemon health.

### Layer 3 — real daemon/process/PTY contracts

A small public-boundary suite must run on every claimed platform:

- spawn, bidirectional binary I/O, resize, nonzero exit, stop, EOF and final
  output ordering;
- client crash/disconnect, clean detach, same-PID reattach, and multiple
  independent clients;
- raw-mode entry/restoration and `SIGWINCH` through a real controlling PTY;
- output beyond retention, slow consumers, explicit `Gap`, cursor recovery, and
  exact-once replay/live joining;
- rejected start/fork, hostile child, and daemon/client termination with no
  leaked child, fd, socket, or attachment.

Readiness uses protocol checkpoints or bounded state polling, never an
unexplained sleep. Every fixture receives a private socket, directory, process
namespace, fixed geometry, explicit environment, deadline, and failure trace.

### Layer 4 — parser and compatibility corpora

Keep versioned corpora for:

- native request/response/event frames;
- arbitrary fragmentation, coalescing, invalid UTF-8, duplicate keys, extreme
  numbers, nesting, exact size limits, and early close;
- Codex and future Agent JSONL/event reduction;
- future tmux Control Mode guards, notifications, escaping, interleaving, and
  supported-version behavior.

Every production crash or rejected edge case becomes a small deterministic
regression. Fuzz-discovered seeds are minimized and checked into the ordinary
corpus. Future, unimplemented capabilities do not create pretend fixtures.

## Top-level reliability lanes

### Chaos and fault testing

Chaos is a reproducible failure schedule, not random process killing without an
oracle. Cover:

- kill or disconnect CLI/SDK/Integration hosts before, during, and after each
  request acknowledgement;
- kill the child during output, resize, input, and detach;
- stall client reads and child reads independently;
- force partial frames, reset sockets, invalid socket targets, fd exhaustion,
  thread/task creation failure, and future persistence write failure;
- send signal/resize storms with a recorded seed;
- when restart recovery exists, kill the daemon at every durable transition and
  prove either the declared recovery class or explicit fail-closed loss;
- when the tmux Backend exists, kill the control client/server and prove pane
  ownership and resynchronization semantics.

Each run records the seed, action trace, daemon log, process tree, and resource
census. A failure must replay as one ordinary test before it is considered
fixed.

### Stress and soak testing

Use bounded, named workloads rather than one “stress” command:

- output flood below, at, and above replay retention;
- 1/8/32 attachments with fast, slow, paused, and reconnecting consumers;
- 1/32/128 concurrent Runs, subject to the declared product quota;
- repeated start/attach/detach/fork/stop/exit churn;
- rapid resize and multi-client input under output load;
- high final-output volume and delayed PTY EOF;
- a short PR smoke, a 30–60 minute nightly soak, and a multi-hour release soak.

The oracle includes no deadlock, no unexplained gap, exact declared gap/truncate
behavior, bounded shutdown, stable daemon health, and resource deltas within
documented bounds.

### Concurrency and race testing

Use three complementary techniques:

1. Barriers force known races such as subscribe/snapshot, stop/wait, and
   detach/output.
2. A small model/state-machine test generates concurrent public operations and
   compares observed results with only the semantics the protocol promises.
3. Seeded real-system schedules repeat multi-client operations under CPU and
   I/O pressure; failing seeds are retained.

Loom or an equivalent scheduler belongs only around a genuinely small extracted
owner state machine. ThreadSanitizer can be a scheduled native/dependency smoke,
but it is not a replacement for protocol-level race oracles.

### Benchmarks and performance regression

Create a repository-owned harness for:

- daemon cold start and Run start-to-accepted latency;
- start-to-first-output and input-to-output latency;
- attach/replay latency at 0, 64 KiB, 1 MiB, and the retention limit;
- sustained input/output throughput;
- fan-out throughput and tail latency for 1/8/32 attachments;
- stop-to-exit and exit-drain latency;
- CPU, RSS, fds, threads, and memory per idle/active Run;
- future tmux adapter and Integration probe overhead.

Record toolchain, commit, OS, architecture, CPU, workload, warm-up, sample count,
and p50/p95/p99 or confidence interval. First trend enough runs to measure normal
variance; only then define per-platform regression budgets. A job timeout or
one wall-clock number is not a benchmark. Competitor numbers are comparable
only when the same harness and environment can execute both products.

### Security testing

The security matrix covers:

- Unix-socket mode, parent-directory trust, symlink/ordinary/live/stale target
  replacement, peer identity, and bind/chmod races;
- malformed, oversized, fragmented, deeply nested, duplicated, and long-lived
  protocol input with no mutation before validation;
- command/argv, cwd, environment, descriptor, signal, PID-reuse, and process
  group boundaries;
- secret-bearing context/artifact references, output retention, logs, and
  future persistence redaction;
- denial of service through Runs, attachments, output, frames, pending requests,
  and retained exited Runs;
- dependency/advisory scans and scheduled parser fuzz/sanitizer runs.

Every rejected request asserts both the error and negative space: no child, Run,
file replacement, unrelated signal, credential disclosure, or daemon death.

### Resource-leak testing

Measure before and after repeated lifecycle cycles:

- child and zombie processes;
- open fds/handles and sockets;
- threads and asynchronous tasks where observable;
- RSS/heap and retained-output bytes;
- Run, attachment, waiter, and future Backend objects.

The test must distinguish intentional retained state from leakage. Exited Runs
cannot pass a leak oracle until GC/quota behavior is declared; the test should
make that open policy visible rather than normalize unbounded growth.

## Gate topology

| Lane                  | Trigger                                                                | Required evidence                                                                                                                                              | Failure policy                                                                |
| --------------------- | ---------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------- |
| PR critical           | Every pull request; Linux and macOS for the native contract            | Static checks, units/properties, malformed corpus, critical real daemon/PTY lifecycle, deterministic replay/stop/socket races, security smoke, coverage report | Blocking; no retries that hide deterministic failure                          |
| PR changed-platform   | Changes to platform, PTY, protocol, SDK, Integration, or Backend paths | The owning platform and contract subset selected through explicit path/job mapping                                                                             | Blocking                                                                      |
| Nightly reliability   | Scheduled                                                              | Seeded fuzz, sanitizer/race smoke, chaos matrix, load/leak run, benchmark trend, larger platform/tool/version matrix                                           | Blocking for release readiness; preserve seeds and artifacts                  |
| Release qualification | Release candidate                                                      | Multi-hour soak, supported OS/architecture matrix, minimum/current external tool compatibility, recovery and upgrade policy tests                              | Blocks release                                                                |
| External canary       | Scheduled and credential-controlled                                    | Real Codex/Agent semantic continuation and other vendor-dependent contracts                                                                                    | Never substitutes for hermetic adapter tests; narrows claims when unavailable |

The full critical lifecycle suite should remain on pull requests even if the
nightly suite grows. Tmux is a warning here: its broad regression suite is
excellent, but the inspected workflow reaches it only daily or manually.

### Code-coverage ratchet

Add Rust and TypeScript coverage reporting as an auxiliary PR artifact. After a
baseline is measured and exclusions are reviewed, adopt these initial floors:

- at least 90% changed-line coverage;
- at least 85% line coverage for hand-written runtime/client/SDK code;
- at least 95% line coverage for pure protocol codecs and runtime validators;
- generated declarations, fixtures, and platform-impossible branches are
  reported separately rather than silently diluting the denominator.

Exceptions require a linked owner-boundary test or a documented unreachable
branch. The invariant matrix still blocks a release when the percentage passes
but required behavioral evidence is absent.

## Adoption sequence

### P0 — close false-confidence gaps

1. Add real public `Gap` → cursor reattach recovery across retention and one
   exact-once replay/live boundary.
2. Decide stop/process-tree semantics and force HUP-ignoring-child plus
   stop-after-wait/PID-identity races.
3. Check in the interactive CLI controlling-PTY fixture for raw mode, input,
   resize, detach, restoration, and surviving Run identity.
4. Force the socket stale-target swap race and assert no unrelated replacement.
5. Add durable macOS CI beside Ubuntu and publish job-to-invariant selection.
6. Add coverage reporting in observation mode, establish the baseline, then
   enable the ratchet.

### P1 — establish the reliability system

1. Add the small controllable process/PTY seam and a model for multi-client
   lifecycle operations.
2. Add native/SDK high-volume retention, slow-consumer, concurrent mutation,
   and resource-census suites.
3. Add native protocol and Integration fuzz targets; run deterministic corpora
   on PRs and time-boxed fuzz/sanitizer jobs nightly.
4. Add named chaos scenarios, a short load/soak lane, and replayable seeds.
5. Add the benchmark harness and trend results before setting regression
   budgets.
6. Generate Rust-authored protocol goldens consumed by the TypeScript SDK.

### P2 — qualify capability-specific risk

1. Gate persistence/recovery with kill-at-every-transition, corrupt/torn state,
   and wrong-PID adoption fixtures before claiming restart continuity.
2. Gate the tmux Backend across its supported minimum/current versions with
   real Control Mode framing, capture/live joins, slow consumers, detach under
   load, server loss, and pane PID survival.
3. Add controlled real-Agent compatibility canaries for fidelity claims while
   keeping hermetic recording executables for argv/protocol correctness.
4. Expand architecture and release matrices only when ctxmux declares support
   for those platforms or backends.

This sequence deliberately reuses the existing gate, corpus, lifecycle harness,
and launch-transition seam. It does not require a new test framework for every
dimension, a public fault API, speculative future fixtures, or a terminal
renderer.

## Evidence sources

- [Ctxmux native lifecycle tests](../crates/ctxmux-daemon/tests/native_lifecycle.rs)
- [Ctxmux SDK parity tests](../packages/sdk/test/client-parity.test.ts)
- [Ctxmux CI](../.github/workflows/ci.yml) and [gate](../scripts/check.sh)
- [tmux regression harness](https://github.com/tmux/tmux/blob/851c5a933d4838c32ad06c248b2ba975d106149c/regress/Makefile), [workflow](https://github.com/tmux/tmux/blob/851c5a933d4838c32ad06c248b2ba975d106149c/.github/workflows/regress.yml), and [fuzz targets](https://github.com/tmux/tmux/blob/851c5a933d4838c32ad06c248b2ba975d106149c/Makefile.am#L254-L269)
- [Zellij fake PTY](https://github.com/zellij-org/zellij/blob/5254e4fc1dd784ef872644190dc5e2bcb0981bed/zellij-integration-tests/src/fake_pty.rs) and [real-PTY E2E workflow](https://github.com/zellij-org/zellij/blob/5254e4fc1dd784ef872644190dc5e2bcb0981bed/.github/workflows/e2e.yml)
- [`portable-pty` contract](https://github.com/wezterm/wezterm/blob/4b1c3c151eb530e569f867e1461693c56fe89695/pty/src/lib.rs) and [WezTerm workflows](https://github.com/wezterm/wezterm/tree/4b1c3c151eb530e569f867e1461693c56fe89695/.github/workflows)
- [Alacritty terminal reference harness](https://github.com/alacritty/alacritty/blob/1b2b36a64e88068ad02c95fad00ee2fad31c00bf/alacritty_terminal/tests/ref.rs) and [CI](https://github.com/alacritty/alacritty/blob/1b2b36a64e88068ad02c95fad00ee2fad31c00bf/.github/workflows/ci.yml)
