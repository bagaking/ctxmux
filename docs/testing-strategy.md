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
SDK tests, two cross-language real-daemon tests, and a public CLI lifecycle
smoke plus the bounded reliability smoke. GitHub runs the full critical gate for pushes and pull requests on both
fixed `ubuntu-24.04` and `macos-15` runners. A required Ubuntu coverage job runs the same
test owners under Rust and TypeScript instrumentation.

The checked-in coverage ratchet and CI reachability map are implemented. The
critical gate now includes owner-local subscribe/snapshot, detach/output, and
wait/exit barriers, a seeded public multi-client mutation model, shared parser
regression corpora, and bounded seeded Rust/TypeScript/Integration parser
targets. The same harness now covers Integration-host exit, child and daemon
kill, hostile frames and launch inputs, high-volume final drain, fan-out lag,
concurrent start, lifecycle churn, and idle/active resource census. The
scheduled reliability workflow raises parser/model counts and runs 30-minute
nightly or explicitly dispatched two-hour release soak profiles on Linux and
macOS while preserving receipts and daemon logs. Coverage-guided fuzzing,
native sanitizers, comparative benchmarks, peer-identity policy, memory-mode
global quotas, and Run GC remain absent.

| Contract area                                    | Strongest current evidence                                                                                                                                                                             | Confidence                                                  | Important gap                                                                                                         |
| ------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| Daemon-owned Run survives client disconnect      | Real Rust daemon/PTY E2E plus CLI/TypeScript cross-language E2E and Integration-host process exit; same PID is controlled after reconnect                                                              | Strong for current daemon lifetime                          | Daemon restart remains a separately owned, unshipped guarantee                                                        |
| Start, input, resize, output, exit, detach, stop | Real shell child, HUP-ignoring stop, four owner barriers, and a seeded public input/resize/stop result model                                                                                           | Strong for declared direct-child lifecycle and forced joins | Hostile child, undefined writer/resize arbitration, and process-tree policy beyond the direct child remain open       |
| Rejected post-spawn setup                        | Deterministic owner seam with a real child proves kill, reap, and no published Run                                                                                                                     | Strong owner-level evidence                                 | Not driven through a public request; final handoff failure is not forced                                              |
| Ordered binary replay after exit                 | Real PTY replay, forced public `Gap`, caller-cursor reattach, and multi-frame native/SDK replay preserve exact ordered bytes                                                                           | Strong for retained replay and live lag                     | Incremental raw-byte encoding performance remains unqualified                                                         |
| 4 MiB retention and truncation arithmetic        | `OutputLog` units plus live and restarted persistent public workloads prove bounded retained tail, truncation, final marker, and attach reassembly                                                     | Strong arithmetic and real-system evidence                  | The 256 MiB global replay quota applies only to persistent mode; memory-only Runs still have no global quota          |
| Protocol generation and malformed frames         | Shared corpus reaches Rust decode, real daemon, and Node validation; exact frame cap plus seeded Rust and TypeScript byte targets are checked                                                          | Strong for retained and seeded cases                        | No coverage-guided continuous fuzzer, sanitizer, or Rust-produced all-variant golden matrix                           |
| Socket path safety and mode `0600`               | Sequential targets plus forced startup and shutdown replacement prove identity recheck, live-listener preservation, and mode                                                                           | Strong for checked owner-controlled schedules               | Hostile writable-parent atomicity, renamed-original cleanup, and peer identity remain open                            |
| SDK backpressure and close races                 | Deterministic mock socket tests, including 20,000 queued frames, plus real 8/32-way fast/slow fan-out with explicit `Gap`                                                                              | Strong for SDK queue and real lag behavior                  | Silent-peer cancellation and request deadlines are absent                                                             |
| Chaos, security, and resources                   | Named process kills, malformed/oversized/long-lived frames, argv/env/secret negatives, concurrent start, churn, and idle/active 1/32/128 census                                                        | Strong for the bounded native generation-3 matrix           | Hostile writable-parent atomicity, fd/thread exhaustion injection, global quotas, and exited-Run GC are open          |
| Level A fork                                     | Real daemon proves copied `RunSpec`, declared inputs, lineage, distinct PID, and independent control                                                                                                   | Strong                                                      | Post-spawn fork failure is not forced through the public boundary                                                     |
| Codex Level B                                    | Source-bound receipt negatives, unrelated-Run public E2E, minimized JSONL corpus, seeded observer target, and a redacted real-Codex continuation artifact                                              | Strong supported-API owner, parser, and semantic proof      | Host-local provenance is not authentication; scheduled hosted evidence remains separate                               |
| Interactive CLI attach                           | Checked-in controlling-PTY E2E proves raw input, `SIGWINCH`, detach, exact restoration, and same-PID survival                                                                                          | Strong for ordinary detach on Unix                          | Daemon-loss, error, unwind, and catchable-signal restoration remain open                                              |
| Persistent historical recovery                   | Real daemon restart restores exited replay/lineage and Level A fork; live rows become interrupted; stale unrelated PID, lock, schema, corrupt generation, modes, symlinks, and retention fail closed   | Strong for the declared historical class                    | Kill-at-every-SQLite-transition injection, power-loss filesystems, live PTY handoff, and migration remain unsupported |
| Read-only tmux pane adapter                      | Transcript parser; real-session discovery/import and exact post-import bytes; complete tuple, corruption, pause/late-replay, ownership and detach fixtures; TypeScript and controlling-PTY CLI clients | Implemented; required version lanes pending                 | Ubuntu minimum and macOS current CI must produce their asserted server-version evidence before Feature archive        |

Exact suite reach is machine-checked by `.github/ci-evidence-map.json`; test
counts describe gate composition only and are not a maturity score.

## Competitive evidence

The comparison uses immutable upstream revisions:

- tmux [`851c5a9`](https://github.com/tmux/tmux/commit/851c5a933d4838c32ad06c248b2ba975d106149c)
- Zellij [`5254e4f`](https://github.com/zellij-org/zellij/commit/5254e4fc1dd784ef872644190dc5e2bcb0981bed)
- WezTerm/`portable-pty` [`4b1c3c1`](https://github.com/wezterm/wezterm/commit/4b1c3c151eb530e569f867e1461693c56fe89695)
- Alacritty [`1b2b36a`](https://github.com/alacritty/alacritty/commit/1b2b36a64e88068ad02c95fad00ee2fad31c00bf)

“Absent” below means no first-party target was found in the inspected pinned
tree, manifests, and workflows. “Unknown” means the available test cannot prove
the property. Neither label excludes private CI or downstream testing.

| Dimension                    | ctxmux now                                                                                                                                                 | tmux                                                                        | Zellij                                                          | WezTerm / `portable-pty`                                                    | Alacritty                                                            |
| ---------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------- | --------------------------------------------------------------- | --------------------------------------------------------------------------- | -------------------------------------------------------------------- |
| Real owner-boundary E2E      | Strong but narrow native Run slice                                                                                                                         | Very broad real server/client/PTY shell regressions                         | Strong Linux real-PTY E2E                                       | Remote SSH PTY exists; local backend lifecycle proof is thin                | One Windows child-exit case; mux lifecycle is out of scope           |
| Deterministic whole-app seam | Partial owner fault seam and SDK mocks                                                                                                                     | No general fake OS harness found                                            | Strong fake PTY plus real server/client threads and IPC         | Injectable PTY interface exists, but no corresponding lifecycle suite found | Terminal model is deterministic, not a mux seam                      |
| Replay/golden evidence       | Malformed corpus and exact raw-byte replay                                                                                                                 | Many exact state/transcript checks and screen goldens                       | `insta` snapshots for fake and real E2E                         | Parser action snapshots                                                     | Strong recorded bytes → semantic terminal-grid goldens               |
| Chaos/fault injection        | Named Integration-host, child, and daemon loss plus transport and launch negatives                                                                         | Scenario faults: killed/stalled clients, permission errors, unread FIFO     | Graceful quit/resurrection only; no systematic chaos            | No lifecycle chaos target found                                             | One child-kill path; no chaos target                                 |
| Stress/soak                  | Bounded replay/final-drain, 1/8/32 fan-out, start pressure, churn, 1/32/128 resources, and scheduled 30-minute/2-hour soak profiles                        | Bounded reflow, lifecycle, and output-backlog stresses                      | No dedicated load/soak target found                             | No PTY/mux load target found                                                | No process/PTY load target found                                     |
| Concurrency/race             | Owner barriers force subscribe/snapshot, detach/output, final-output/exit, and stop/wait; a seeded public mutation model constrains only declared outcomes | Real black-box concurrency regressions, but no deterministic scheduler/TSan | Good deterministic multi-client tests; no scheduler exploration | Concurrent production code, little matching test evidence                   | One race-sensitive Windows watcher test                              |
| Fuzz/sanitizer/security      | Malformed corpus and socket/argv tests; no fuzzer/sanitizer CI                                                                                             | Four parser fuzz targets; ASan regression build; fuzz not run there         | No first-party fuzz/sanitizer target found                      | No first-party fuzz/sanitizer target found                                  | No first-party fuzz/sanitizer target found                           |
| Resource leaks               | Frozen CPU/RSS/retention and per-Run thread/fd budgets with child/attachment/thread cleanup oracles; no-GC state remains visible                           | Cleanup proxies and ASan; no explicit FD/zombie/RSS census                  | Functional joins/count settling; direct leak freedom unknown    | Careful implementation/drop behavior; direct leak freedom unknown           | Direct handle/process leak freedom unknown                           |
| Benchmark/performance        | Absent                                                                                                                                                     | No regression benchmark found                                               | No PTY/mux benchmark found                                      | Adjacent cell/width/range benches only; no PTY lifecycle benchmark          | No relevant benchmark found                                          |
| Platform evidence            | Required Ubuntu 24.04 and macOS 15 critical CI; coverage instrumentation and tmux 3.4 assertion are required on Ubuntu                                     | Nightly/manual Ubuntu x64 + macOS arm64                                     | Unit matrix is broad; whole-app and real PTY are Linux-only     | Broadest cargo CI: Linux, macOS, Windows; local PTY behavior still unproved | Push/PR tests on Windows + macOS; Linux absent in inspected workflow |
| Submission reach             | Critical suite on every push/PR                                                                                                                            | Full regression is daily/manual, not push/PR                                | Unit, fake-PTY integration, and real-PTY E2E on push/PR         | Cross-platform cargo tests on relevant changes                              | Cargo tests on every push/PR                                         |

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
- persistence transaction admitted but not committed.

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

The tmux public-boundary suite additionally requires:

- mixed live/dead discovery where an unrelated dead pane cannot hide a live
  target;
- full import-tuple changes for session, window, pane PID, and server epoch,
  including reused tmux IDs;
- client/server version mismatch plus exact selected-server version reporting;
- post-readiness malformed and oversized Control records reported as
  `tmux_protocol_error`, not server loss;
- pause followed by new output, caller-cursor reattach, late replay marked
  truncated, exact surviving bytes, and a still-live tmux pane;
- queued-output detach and ctxmux shutdown that close only ctxmux-owned Control
  clients;
- first-party Rust, CLI controlling-PTY, and TypeScript SDK behavior through
  the same public daemon protocol.

A test that returns success when `tmux` is missing is diagnostic-only. Required
CI installs tmux first, asserts the lane's actual selected-server version, and
fails before the suite if either condition is absent.

### Layer 4 — parser and compatibility corpora

Keep versioned corpora for:

- native request/response/event frames;
- arbitrary fragmentation, coalescing, invalid UTF-8, duplicate keys, extreme
  numbers, nesting, exact size limits, and early close;
- Codex and future Agent JSONL/event reduction;
- tmux Control Mode guards, notifications, escaping, interleaving, malformed
  post-readiness records, and qualified-version behavior.

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
  thread/task creation failure, and persistence write failure;
- send signal/resize storms with a recorded seed;
- when restart recovery exists, kill the daemon at every durable transition and
  prove either the declared recovery class or explicit fail-closed loss;
- kill the tmux Control client/server and prove pane ownership, distinct
  interruption meaning, and resynchronization semantics.

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

Fast fan-out consumers must prove contiguous output sequences and exact seeded
payload bytes, not only total byte count. The qualification seed selects that
payload so the structured receipt replays a material stress input. A supervisor
runs the complete qualification in its own process group and force-terminates
that group when `time_budget_seconds` expires; an incomplete stage becomes an
explicit failed receipt rather than relying only on the CI job timeout.

High-Run-count qualification measures both idle and active workloads at
1/32/128 Runs. After a clean observation baseline and before optimization
results guide changes, freeze core-normalized daemon CPU, peak and steady-state
RSS, retained-output bytes, and per-Run RSS/thread/fd slopes. Repeated lifecycle
churn must return cleanup-owned resources to the documented baseline envelope;
intentional retained state and the current absence of global Run GC or quotas
remain visible rather than being subtracted from the result.

`reliability-budgets.json` freezes the six idle/active × 1/32/128 cells from
three Darwin arm64 observation rounds before optimization. CPU is percent of
one logical core; RSS peak is sampled every 25 ms. Resource census starts Runs
with concurrency 8: on the observation host, 128 idle Runs completed at launch
concurrency 1/4/8, while concurrency 16 reproduced Darwin `openpty -6` after
123 successful responses. This is recorded as burst-start pressure rather than
misreported as a 128-Run steady-state limit. A separate 32-Run/concurrency-16
oracle preserves concurrent-start stress without weakening the 128 census.
The three complete observation receipts live under `fixtures/reliability/`.
The policy gate verifies each SHA-256 and recomputes every recorded maximum
from those durable raw receipts before accepting the budget file.

The PR profile runs the full named matrix with the one-Run resource cells and
no time soak. Nightly uses the complete resource matrix plus a real 30-minute
active soak inside a 45-minute harness budget. Explicit release dispatch uses a
two-hour soak inside a three-hour harness budget. Both scheduled profiles run
on Ubuntu and macOS and upload the structured receipt plus every daemon log even
when the gate fails.

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

The current bounded model races public input, resize, and direct-child stop
through separate socket clients. It accepts only success, the declared
post-exit rejection, or an owner I/O failure where those outcomes are honest;
it does not infer byte ordering or resize arbitration. The scheduled runner
records seed, case counts, exact commands, environment, covered owner
boundaries, and excluded claims before execution, so a failure retains replay
information even when a later command never runs.

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

| Lane                  | Trigger                                                                | Required evidence                                                                                                                                                                     | Failure policy                                                                |
| --------------------- | ---------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------- |
| PR critical           | Every pull request; Linux and macOS for the native contract            | Static checks, units/properties, malformed corpus, critical real daemon/PTY lifecycle, deterministic replay/stop/socket races, bounded chaos/security/resource smoke, coverage report | Blocking; no retries that hide deterministic failure                          |
| PR changed-platform   | Changes to platform, PTY, protocol, SDK, Integration, or Backend paths | The owning platform and contract subset selected through explicit path/job mapping                                                                                                    | Blocking                                                                      |
| Nightly reliability   | Scheduled                                                              | Seeded fuzz/model receipt plus Linux/macOS chaos, full 1/32/128 resource matrix, and 30-minute load/leak soak; sanitizer and benchmark trend remain open                              | Blocking for release readiness; preserve seeds and artifacts                  |
| Release qualification | Explicit release dispatch                                              | Linux/macOS two-hour soak plus the same frozen budgets; external-tool, recovery, and upgrade qualification remain with their owning tasks                                             | Blocks release                                                                |
| External canary       | Scheduled and credential-controlled                                    | Real Codex/Agent semantic continuation and other vendor-dependent contracts                                                                                                           | Never substitutes for hermetic adapter tests; narrows claims when unavailable |

The full critical lifecycle suite should remain on pull requests even if the
nightly suite grows. Tmux is a warning here: its broad regression suite is
excellent, but upstream tmux's inspected workflow reaches it only daily or
manually.

### Code-coverage ratchet

Rust and TypeScript coverage reporting is a required PR job. The enforced
initial floors are:

- at least 90% changed-line coverage;
- at least 85% line coverage for hand-written runtime/client/SDK code;
- at least 95% line coverage for pure protocol codecs and runtime validators;
- generated declarations, fixtures, and platform-impossible branches are
  reported separately rather than silently diluting the denominator.

Exceptions require a linked owner-boundary test or a documented unreachable
branch. The invariant matrix still blocks a release when the percentage passes
but required behavioral evidence is absent.

`scripts/check.sh --coverage` owns the reproducible local gate. It emits raw
LCOV/JSON under the ignored `coverage/` directory and validates seven reviewed
owner groups in `coverage-policy.json`. The earlier four-group Darwin
observation is superseded by the fresh T-020 seven-owner baseline:

| Coverage owner                 | Floor | Fresh T-020 observation |
| ------------------------------ | ----: | ----------------------- |
| Rust runtime and clients       |   85% | 86.44% (2,569/2,972)    |
| Rust persistence               |   85% | 85.35% (1,509/1,768)    |
| Rust tmux adapter              |   85% | 85.99% (675/785)        |
| Rust RunSpec validator         |   95% | 100.00% (20/20)         |
| Rust protocol and codegen      |   95% | 98.04% (200/204)        |
| Hand-written TypeScript SDK    |   85% | 93.36% (1,068/1,144)    |
| TypeScript protocol validators |   95% | 97.91% (704/719)        |

The fresh retained run compared the current tree directly with audited commit
`b80a5cf44d1fafc653d9440eb5611e1e57e26d18`; changed executable product lines
passed at 99.32% (147/148) against the 90% floor. Persistence and tmux remain
independent ordinary 85% owners; neither is omitted, diluted into the runtime
aggregate, nor labeled as an exception. Per-file results remain visible even
when a group passes, so lower CLI or validator coverage cannot disappear inside
one repository aggregate.

Generated TypeScript protocol declarations are excluded from the denominator
and remain protected by the Rust-owner drift check. The `cfg(not(unix))`
compile failures are reported as platform-impossible because generation 3
declares a Unix transport. No test fixture or hand-written runtime file is
silently excluded. Changed-line mode is explicit:

- `false` performs ordinary reporting and may report no executable product
  denominator;
- `true` is retained evidence and requires a nonzero executable denominator;
- `auto` is the required CI mode: a nonzero executable denominator must meet
  the 90% floor, while documentation-only, comment-only, deletion-only, or other
  zero-executable-denominator changes report N/A.

Auto N/A means no changed-line proposition exists for that diff. It is not a
passing changed-line percentage and cannot be retained as changed-line evidence.
The filesystem inventory, every owner group, missing-report checks, and the
ordinary repository Gate still execute, so N/A does not exempt product sources
or weaken the fixed owner floors. This avoids incentivizing an unrelated
executable edit merely to manufacture a denominator.

Required CI supplies the pull-request base with `merge-base` comparison or the
prior push revision with direct comparison. An empty, zero, or invalid revision
fails closed rather than falling back to `HEAD` or `HEAD^`; a direct evidence
base must also be an ancestor of `HEAD`. Untracked owned sources contribute all
reported executable lines to the current-tree denominator. Product source paths
outside the policy's safe POSIX character set fail inventory validation instead
of relying on ambiguous unified-diff quoting. Retained local evidence sets an
explicit base,
`CTXMUX_COVERAGE_CHANGED_LINE_MODE=true`, and
`CTXMUX_COVERAGE_COMPARISON_MODE=direct`, and records the base, comparison,
every owner result, and nonzero denominator.

`.github/ci-evidence-map.json` maps every discovered Rust, TypeScript, script,
and public-smoke suite to invariants, selection owners, required jobs, and
platforms. It also binds the coverage job to full Git history, event-specific
base and comparison semantics, and `auto` mode. `scripts/ci-reachability.mjs`
rejects unmapped tests, hidden `skip`/`ignore`, workflow or selector drift,
conditional required jobs, non-executing command or environment prose, trigger
drift, weakened coverage reach, incomplete platform reach, and unclassified
skipped, conditional, ignored, or schedule-only evidence. Required jobs use one
canonical repository checkout, then end in one inline Gate step whose startup
environment binds `BASH_ENV` and `ENV` to `/dev/null`. That step clears the
startup variables, common Git repository redirection variables, and Git config
override channels including counted `GIT_CONFIG_KEY_*` / `GIT_CONFIG_VALUE_*`
pairs. Both identity commands bind global and system Git configuration to
`/dev/null`; the worktree check additionally overrides `core.excludesFile`,
disables `core.fsmonitor`, and disables `core.untrackedCache`. It then uses the
neutralized `/usr/bin/git` report to verify the worktree state it observes and
`HEAD` against `${{ github.sha }}`, and immediately execs the existing Gate
under `/bin/bash --noprofile --norc`. The same final step rebinds the required tmux
executable/profile and fuzz/model case counts, so a prior step cannot use
persisted shell startup, counted config, `HOME/.gitconfig`, or
`XDG_CONFIG_HOME/git/config` state to cross a post-fence runner boundary, hide
untracked source, or silently downgrade mapped evidence. This boundary does not
claim hostile-runner, arbitrary-action, background-process, dynamic-loader,
complete PATH/toolchain, Git metadata or object-store tampering (including
repository-local excludes and index flags), Windows, or self-hosted-runner
attestation.

## Adoption sequence

### P0 — close false-confidence gaps

1. Add real public `Gap` → cursor reattach recovery across retention and one
   exact-once replay/live boundary.
2. Decide stop/process-tree semantics and force HUP-ignoring-child plus
   stop-after-wait/PID-identity races.
3. Check in the interactive CLI controlling-PTY fixture for raw mode, input,
   resize, detach, restoration, and surviving Run identity.
4. Force the socket startup stale-target swap race and assert no unrelated
   replacement; separately owner-fence shutdown unlink against a replacement
   listener.
5. Add durable macOS CI beside Ubuntu and publish job-to-invariant selection.
6. Add coverage reporting in observation mode, establish the baseline, then
   enable the ratchet.

Items 1 through 4 are implemented by the public Gap/cursor fixture, owned-child
stop and wait/publication barrier, controlling-PTY CLI fixture, and startup
plus shutdown socket-identity barriers. Items 5 and 6 are implemented by the
required `critical`/`coverage` jobs, `.github/ci-evidence-map.json`, and
`coverage-policy.json`. Coverage does not substitute for the owner-boundary
fixtures.

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

The deterministic owner barriers, public mutation model, native/TypeScript/
Codex seeded targets, minimized corpora, high-volume replay/fan-out, named chaos
matrix, resource census, frozen budgets, and scheduled soak profiles implement
the bounded portions of items 1 through 4. Coverage-guided fuzzing, sanitizer
coverage, the broader controllable PTY seam, benchmark trends, high-volume
mutation, and Rust-authored all-variant goldens remain open rather than being
inferred from this lane.

### P2 — qualify capability-specific risk

1. Gate persistence/recovery with kill-at-every-transition, corrupt/torn state,
   and wrong-PID adoption fixtures before claiming restart continuity.
2. Gate the tmux Backend across its qualified minimum/current versions with
   real Control Mode framing, raw-since-import joins, slow consumers, detach
   under load, server loss, and pane PID survival. The minimum lane pins Ubuntu
   tmux 3.4 and asserts the selected server version; the macOS lane installs
   the current package and records/asserts the selected server version. This
   qualifies those two versions, not every future 3.x release.
3. Close Level B provenance before broadening the claim: a continuation must
   bind to a session created for the declared parent Run, and an unrelated or
   unverifiable session must fail before a raw fork request can create a Run.
4. Keep hermetic recording executables for exact argv, protocol, lineage, and
   failure regressions, and add a credential-controlled real Codex canary that
   proves a unique fact established in the parent session remains observable in
   the Level B continuation.
5. Measure real Codex cold-start behavior and make the default availability
   probe tolerant of the supported startup envelope. An explicit timeout must
   still fail closed deterministically; retries must not turn a semantic or
   availability failure green.
6. Expand architecture and release matrices only when ctxmux declares support
   for those platforms or backends.

### Capability Feature closure before final qualification

Persistence/recovery and the tmux adapter keep their implementation truth in
their existing Feature Tracker Features because they have different owners and
failure models. Reliability qualification must nevertheless keep the following
closure obligations explicit:

- `f-223czq75x` is archived after completing its accepted contract and
  implementation tasks and passing real restart, corruption, stale-identity,
  retention, cleanup, and applicable wrong-case gates. Final qualification now
  binds that archived evidence and keeps unsupported live continuity excluded.
- `f-224czneed` completes its tmux public-Control-Mode adapter task, passes the
  complete-identity, qualified-version, framing-corruption, pause/late-replay,
  detach, server-loss, first-party-client, and process-ownership gates, and is
  formally archived before tmux behavior is counted in final qualification or
  peer comparisons.
- `f-226cz5zdq` owns the cross-Feature closure and qualification evidence, not a
  duplicate persistence or tmux implementation plan. The qualification report
  records each archived Feature, exact shipped boundary, versions and evidence,
  and keeps unsupported recovery or Backend semantics excluded.

### Embeddable Run Kernel adoption closure

Consumer demand may reveal a missing kernel contract, but it does not redefine
ctxmux around one consumer. Candidate work enters qualification under this
order:

1. include it when the daemon, PTY, protocol, SDK, lifecycle, or raw-stream
   owner boundary gives ctxmux itself a clear correctness, reliability,
   performance, or operability benefit;
2. when it merely fits the Run model, require an explicit benefit-to-state,
   protocol, implementation, and test-complexity argument;
3. reject it when it moves Agent semantics, orchestration, workspace policy,
   terminal rendering, or speculative remote-product behavior into the mux.

The current clean-consumer audit admits four bounded gaps:

- **Retry-safe Run creation:** a caller-supplied operation key may make `start`
  and `fork` converge after response loss, but `RunId` remains the physical
  incarnation. The same key with a different canonical request conflicts. Key
  retention follows Run retention and recovery; this must not grow into mutable
  tags, metadata CRUD, a second Session identity, or silent attach-or-create.
- **Observable attachment controls:** persistent input, resize, and stop need
  bounded command correlation and exact accepted/error results. Acceptance
  proves only what the PTY owner completed; it does not claim a child consumed
  input. Resize reports the applied terminal size, and stop acceptance remains
  distinct from final `Exited`.
- **Race-safe local activation:** a framework-neutral consumer needs one
  explicit connect-or-activate contract with readiness, version compatibility,
  concurrent-start, socket safety, daemon ownership, logging, and cleanup
  semantics. It must not silently download a binary, mutate global state, bind
  ctxmux lifetime to Electron, or imply SSH/remote support.
- **Earned streaming optimization:** freeze an incremental raw-byte consumer
  workload before changing generation-3 integer-array encoding. Adopt a new
  versioned representation only when the measured end-to-end gain exceeds its
  protocol and validation cost while exact bytes, sequence, replay, `Gap`, frame
  limits, backpressure, security, and resource bounds remain green. A measured
  no-change decision is preferable to an elegant-looking rewrite without ROI.

Agent catalogs, ACP, Hooks and their durability, Agent activity/status,
Workspace/Git/worktree metadata, xterm screen snapshots, and remote ctxmuxd/SSH
deployment remain consumer or future-Backend responsibilities. Process-tree
shutdown, daemon restart recovery, packaging, and release claims stay in their
already-owned tasks rather than being duplicated here.

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
