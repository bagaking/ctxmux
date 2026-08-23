# Verification Evidence

## T-001 — Accepted Restart Recovery Contract

## Automated Checks

- `npm run format:check` and `git diff --check`: passed after the accepted
  decision and verification updates.
- `scripts/check.sh`: passed through the formal Tracker gate, proving the
  accepted decision does not drift current memory-only behavior or repository
  evidence.
- Tracker gate
  `.bagakit/feature-tracker/features/f-223czq75x/artifacts/gate-T-001-r1-0001.log`:
  passed.

## Manual Checks

- Recovery classes: the accepted decision separates durable historical
  metadata/lineage, a committed contiguous replay window with durable
  oldest/head/truncation cursors, and explicitly unsupported live PTY/child
  ownership. Memory-only mode remains available and makes no restart claim.
- Identity and epoch: one process-lifetime exclusive state-directory lock is
  acquired before SQLite mutation or socket publication. A second daemon fails
  typed without allocating an epoch. Epoch creation and prior-epoch
  running-to-interrupted reconciliation share one startup transaction; PID is
  cleared and the replacement never opens, adopts, attaches to, or signals a
  process named by stored metadata.
- Atomicity and corruption: one maintained bundled SQLite store, one writer
  actor, and WAL transactions define explicit atomic units for start, output plus
  cursor/pruning, terminal plus final replay, epoch/reconciliation, and record
  eviction. Exact schema, SQLite integrity, and application cursor/chunk/JSON/
  lineage/quota invariants are validated before publication. Unknown or corrupt
  state fails typed and is neither reset nor salvaged by ctxmux fallback.
- Replay honesty: `durable_head_seq` distinguishes committed contiguous output
  from live delivery that may be ahead. Terminal state and final replay share a
  transaction; abrupt loss promises only the committed retained window.
- Retention and cleanup: the contract fixes 4 MiB per Run, 256 MiB replay, 64
  MiB metadata, 4,096 records, a 384 MiB main DB, 16 MiB WAL, 4 MiB SHM, ordered
  tail pruning, terminal-history eviction, checkpoint/vacuum behavior, and
  fail-before-publication when a running record cannot be reserved. WAL
  admission caps one transaction at 8 MiB of frames and requires a successful
  zero-length truncate checkpoint before current plus worst-case append could
  exceed 16 MiB.
- Orphans and secrets: a surviving HUP-ignoring child remains an explicit orphan
  and is never guessed back into control. Exact spec/env/references/output are
  sensitive durable data, so a non-symlink, same-owner `0700` state directory,
  regular `0600` DB/WAL/SHM/lock files, and pre-write validation are part of the
  contract. Logical eviction is not secure erase; whole-store deletion requires
  a stopped daemon and the exact dedicated directory. Encryption is not claimed.
- Scope: no per-Run shim/supervisor, schema migration, remote store, tmux
  coupling, Integration semantics, client-owned process control, or live PTY
  handoff was introduced or claimed.
- Independent bounded review initially blocked on state ownership, transaction
  units/application invariants, exact physical limits/replay-window wording,
  and secret-sidecar cleanup. After explicit lifetime lock, atomic units, exact
  quota/admission numbers, and owner-only/secure-erasure boundaries were added,
  the final re-review returned PASS.

## Residual Risks at T-001 Close

- The decision is accepted but not implemented. Until T-002 passes real restart
  and wrong-case fixtures, `docs/architecture.md` and `docs/protocol.md` continue
  to describe current daemon state as memory-owned.
- SQLite durability inherits its documented filesystem/flush assumptions.
  Hostile filesystems, migration to a future schema, orphan adoption, encrypted
  state, and transparent live-control recovery remain unsupported.

## T-002 — Implemented Historical Recovery Class

## Automated Checks

- `cargo clippy --workspace --all-targets --all-features -- -D warnings`:
  passed with the persistence actor, recovered Run branch, protocol changes,
  and all new tests.
- `cargo test -p ctxmux-daemon --test persistence_recovery`: six real-daemon
  tests passed for exited recovery, interrupted reconciliation, stale unrelated
  PID safety, per-Run replay retention, state lock/version handling, corrupt
  generation rejection, and unsafe state paths.
- `cargo test -p ctxmux-daemon persistence::tests`: three store-owner tests
  passed for exact logical/physical constants, terminal-only record eviction,
  and global replay pruning that preserves a contiguous tail per Run.
- Existing Rust workspace and TypeScript SDK tests passed after the protocol
  increment. TypeScript runtime validation accepts `interrupted` and
  `durable_head_seq`, rejects invalid variants/cursors, and still passes the
  hostile-frame corpus.
- `node scripts/check-fixtures.mjs`: passed with `PERSIST-01` and `PERSIST-02`
  activated against checked-in restart fixtures.
- `node scripts/ci-reachability.mjs`: passed with the store-owner and public
  restart suites mapped to required Linux/macOS critical and Ubuntu coverage
  jobs.

## Public and Owner-Boundary Evidence

- Optionality: `ctxmuxd --socket <path>` retains the memory-only path and
  reports `durable_head_seq: null`; `--state-dir <dedicated-directory>` opens
  one actor thread and one bundled SQLite connection before socket publication.
- Restart: a real daemon is killed and restarted against the same state
  directory. Exited metadata, exact `RunSpec`, lineage, committed replay,
  terminal event, and Level A fork survive with a distinct child Run.
- Ownership: a live row's stored PID is replaced with an unrelated live process
  before restart. Startup clears the PID and reports
  `interrupted { daemon_restart }`; input, resize, stop, and recovered Level B
  fork fail, while both the unrelated process and old HUP-ignoring orphan remain
  alive until fixture-owned cleanup.
- Atomicity/corruption: SQLite owns transaction recovery. A separately injected
  parseable cursor/chunk hybrid fails typed startup validation before the socket
  or any partial Run is exposed; an unknown exact schema version also fails
  before publication.
- Retention: production constants remain 4 MiB per Run, 256 MiB global replay,
  64 MiB metadata, 4,096 records, 384 MiB main DB, 16 MiB WAL, 4 MiB SHM, and
  404 MiB total state files plus the small lock. A public output workload proves
  the 4 MiB tail across restart; reduced owner seams prove global oldest-chunk
  pruning and terminal-only record eviction without allocating every production
  ceiling in the ordinary Gate.
- Security: a process-lifetime exclusive lock rejects a second daemon; the
  state directory is exact same-owner `0700`; database, WAL, SHM, and lock are
  regular same-owner `0600` files; directory and database symlinks fail before
  socket publication and leave their targets untouched.

## Residual Risks

- No live PTY/child control, PID adoption, schema migration, salvage/reset,
  online secret purge, encryption, remote store, or process-tree recovery is
  claimed.
- SQLite durability remains conditional on its documented filesystem and flush
  assumptions. The ordinary Gate proves real process restart and detected mixed
  state, not arbitrary power-loss behavior on a hostile filesystem.
- Persistence write-failure injection at every internal SQLite transition and
  a full production-size 404 MiB allocation are not ordinary-Gate workloads;
  exact admission constants, transaction boundaries, corruption checks, and
  bounded reduced oracles are checked instead.
