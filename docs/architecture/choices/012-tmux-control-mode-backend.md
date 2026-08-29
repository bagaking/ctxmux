# 012 — tmux public-surface Backend

- Status: accepted and implemented; required version-lane qualification pending
- Scope: adapting existing tmux-owned panes without private wire compatibility

## Context

Users already have durable work in tmux. ctxmux should expose that work through
its Run model without taking ownership, reimplementing tmux, or coupling the
native Backend to tmux internals.

## Decision

The adapter uses the selected `tmux` executable and public Control Mode. tmux
continues to own its server, sessions, windows, panes, PTYs, processes, and
persistence. ctxmux owns only its Control Mode client and one in-memory Run
record.

One imported ctxmux Run represents one pane selected at import time. Import is
fail-closed against the complete observed identity tuple:

```text
explicit socket path
+ server PID and server start time
+ session ID
+ window ID
+ pane ID
+ pane PID
```

The stable target is the selected pane in the observed server epoch, not a
session name or window layout. Session and window membership, pane death, pane
respawn, or server replacement cannot silently turn the Run into a different
target. Until a separately reviewed follow-target contract exists, a change to
any member of the imported tuple interrupts the Run explicitly.

Tmux links can expose one pane ID through multiple session/window associations.
Discovery reports those rows, but the generation-14 import request names only
socket path plus pane ID. Import therefore fails closed unless that pair
resolves to exactly one tuple; it never chooses an association by output order.

Public identity is split across existing generic fields rather than duplicated:
`RunBackend::Tmux` carries the server/session/window/pane identity and
`RunInfo.pid` carries the pane PID observed at import. For a tmux Run that PID
is identity evidence only, never ctxmux authority to signal the process.

`tmux_version` means the version reported by the selected tmux server. The
client executable and selected server are identified and validated separately because an
installed client may connect to a server created by a different tmux binary.
The implementation accepts released tmux 3.4 through 3.x, but CI qualification
is narrower: a pinned Ubuntu minimum lane proves 3.4, while a pinned macOS
runner proves the current package version after asserting the actual server
version. Versions not exercised by those lanes remain supported-contract
candidates, not release-qualified evidence.

Imported panes are deliberately read-only in this slice:

- `list`, `status`, and `attach` are supported;
- `input`, `resize`, `stop`, and Level A or B `fork` fail with
  `unsupported_capability`;
- replay is `raw_since_import`, never raw-from-process-start;
- the first attachment reports `truncated` because pre-import bytes are
  unavailable;
- pause or source loss remains visible to late attachments as raw replay
  truncation and to live attachments as an output `Gap`; only actual fan-out
  loss of a tmux observation produces `ObservationDiscontinuity`;
- import is memory-only and is rejected when ctxmux runs with `--state-dir`.

The adapter does not use `capture-pane` to manufacture raw history. A screen
snapshot is terminal state, not an ordered byte stream, and no atomic public
capture-plus-subscribe operation exists.

Control Mode transcript corruption has its own public failure meaning,
`tmux_protocol_error`. A malformed escape, oversized record, invalid command
block, or other framing violation after import is not reported as ordinary
server unavailability.
An empty LF or CRLF record remains a record rather than masquerading as EOF;
EOF inside an open command block is transcript corruption. Adapter commands
use one bounded serial tracker: at most one identity probe and one continue
request may be pending, command-result numbers must advance, and only the
single pre-session attach bootstrap result is accepted without an adapter
command. This is the correlation needed by the current identity and continue
probes, not a general strong command-correlation facility.

ctxmux will not implement tmux's private client-server socket protocol and will
not promise that an unmodified `tmux attach` command can connect to a ctxmux
daemon.

## Quality attributes and invariants

- ctxmux client disconnect and daemon shutdown terminate only ctxmux-owned
  Control Mode clients; they never kill the tmux pane, session, or server.
- Target replacement, relocation, respawn, death, and server loss are explicit
  public interruption or import errors.
- Unsupported native semantics are capability-visible instead of emulated.
- Control Mode framing and octal byte decoding are bounded and exact.
- Short-lived executable probes have one owner deadline and bounded stdout and
  stderr capture. Timeout or overflow terminates the helper process group and
  reaps its direct child without blocking unrelated daemon requests.
- An EOF-driven terminal path whose Control cleanup succeeds releases the
  ctxmux-owned stdin and stdout reader descriptors and reaps the Control process
  before publishing `Interrupted`. Historical Run status and terminal attachment
  remain available without retaining a live Control descriptor, and cleanup does
  not terminate a still-live tmux-owned pane.
- A paused or lagged source never becomes a falsely continuous replay, and a
  dropped tmux observation is never relabeled as byte-replay evidence.
- Native Runs do not acquire a tmux dependency.

## Alternatives

- Speaking the private tmux wire protocol couples ctxmux to version-specific
  internals.
- Mapping one Run to a whole session or window confuses stable process
  observation with mutable tmux layout.
- Shelling out only to `capture-pane` cannot provide a complete ordered live
  stream.
- Requiring tmux for every Run makes an external tool the core runtime owner.
- Extracting a general Backend framework before native/tmux duplication proves
  it necessary adds speculative policy to the runtime core.

## Wrong-case corpus

- `TMUX-01` (`l01`, `l02`): Control Mode payload is octal-escaped bytes, can
  be invalid UTF-8, and is interleaved with empty records, notifications,
  command blocks, and numbered results whose ownership must fail closed.
- `TMUX-02` (`l01`, `l02`): tmux can pause or terminate a slow control client.
  Recovery must expose raw-output gap and non-output observation discontinuity
  separately and cannot relabel screen state as raw history.
- `TMUX-03` (`l03`) transfers only the upstream queued-output detach/teardown
  failure: ctxmux must close only its incarnation-local adapter resources while
  preserving exact queued replay and leaving any still-live pane tmux-owned.
  Retaining a dead Control writer after interruption is a ctxmux-local regression
  extension under the same dangling-ownership case, not a failure derived from
  `l03`; its EOF-driven successful-cleanup fixture requires release before
  terminal publication while historical status and terminal attachment remain
  available.
- `TMUX-04` (cross-track `a01`, `a02`): executable probes can wait without a
  cleanup owner, and started blocking work is not cancelled by dropping its
  async handle. Probe time, capture, and rollback therefore remain explicitly
  bounded inside the tmux owner.

Pane IDs are stable only within one tmux server lifetime. The server epoch and
the rest of the import tuple therefore participate in the target fence.

## Fixture mapping

- Transcript parser fixtures prove command/notification separation, empty-line
  and EOF distinction, command-block completion, octal decoding, invalid UTF-8
  preservation, and bounded malformed input.
- Deterministic executable fixtures prove version and pane discovery deadlines,
  dual-pipe capture limits, helper cleanup, request isolation, and failed-import
  rollback before publication.
- Real-session fixtures prove discovery, raw-since-import output, target and
  server loss, multi-client detach, queued-output teardown with exact replay,
  and tmux ownership.
- First-party TypeScript and controlling-PTY CLI fixtures prove public
  discover/import/attach, read-only rejection, ordinary input suppression,
  external output, `Ctrl-b d`, terminal restoration, detach, and pane survival.
- Public deterministic fixtures prove bootstrap and bounded command-result
  tracking, EOF classification, blank-line output continuity, and pause-storm
  deduplication. The public pause fixture also proves post-pause output,
  caller-cursor reattach, late replay truncation, exact surviving bytes, and
  pane survival. Tiny-channel mixed-event coverage forces output plus every
  current tmux observation across attachment lag and proves an explicit
  discontinuity before one terminal boundary. A deterministic fake Control
  fixture with an independent pane
  process sentinel proves repeated public import and EOF-driven successful
  cleanup: historical status and terminal attachment remain available, the
  sentinel remains alive, and the daemon descriptor census returns exactly to
  its baseline after every qualified cleanup.
- Required CI must fail when tmux is missing or the lane's server-version
  assertion does not hold.

## Qualification boundary

Complete identity fencing, precise protocol corruption, the full `TMUX-02`
public late-replay oracle, and first-party clients are implemented and pass
locally. That evidence combines deterministic identity/protocol/pause fixtures
with real tmux ownership, TypeScript, and controlling-PTY CLI tests. Feature
`f-224czneed` remains open until the complete repository gate passes and the
required Ubuntu minimum and macOS current lanes produce their real
server-version evidence.

## Repository evidence

- `crates/ctxmux-daemon/src/tmux.rs`: executable boundary and Control Mode
  parser
- `crates/ctxmux-daemon/tests/tmux_adapter.rs`: transcript and real-session
  behavior
- `crates/ctxmux/tests/interactive_attach.rs`: read-only controlling-PTY CLI
  detach and pane ownership
- `packages/sdk/test/client-parity.test.ts`: real TypeScript discovery, import,
  attach, capability, detach, and pane-survival behavior
- `fixtures/tmux-control-mode.json`: checked-in byte/transcript corpus
- `fixtures/wrong-cases.json`: active `TMUX-01` through `TMUX-04`
- `.github/ci-evidence-map.json`: required job and platform reachability
- `docs/protocol.md`: public generation-14 behavior
