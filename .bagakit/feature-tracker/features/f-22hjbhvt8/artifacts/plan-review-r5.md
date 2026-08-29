# Remote Runtime plan review — revision 5

- Status: approved by the Owner in the supervised session that opened this
  revision.
- Supersedes: revision 4 (`plan-review-r4.md`), which remains historical truth.
- Scope of change: two added tasks and one added serial checkpoint. No delivered
  evidence is revisited and no architecture decision is reopened.

## Why this revision exists

Two things changed after revision 4 closed T-006.

First, the real-OpenSSH boundary became available. A Linux `x86_64` owner host
(`sg2`) now runs a placed `ctxmuxd` binary, so T-003's qualification can run
instead of failing closed for lack of a boundary.

Second, that Linux cross-build immediately exposed a compile error the local
macOS build could never see: `native_session.rs`'s
`#[cfg(not(target_os = "macos"))] fn process_ids()` did not type-check, because
`Pid::as_u32` takes `self` while `HashMap::keys()` yields references. The line
was fixed in `0a97cd8`.

The line mattered less than what it revealed. Local verification is macOS-only,
so every `cfg` branch macOS does not compile is unverified before merge. Fixing
the line without closing that gap would verify the fix with the same blind spot
that admitted it.

## Decision 1 — the Owner-directed capacity addition

The Owner directed that high-ROI work from other Features be pulled into this
topology rather than waiting behind it, and that concurrency rise without
multiplying worktrees.

`f-22ecztapc` T-005, race-safe TypeScript Runtime activation, is admitted here as
**T-007**, with its acceptance criteria carried over verbatim. It is not admitted
because it is adjacent in name. It is admitted because:

- its `depends_on` is empty and it shares no file with the remote work;
- it lives entirely in the TypeScript SDK, where the remote endpoint just proved
  the same shape it needs — bounded readiness polling against an observed
  condition, typed failure, and no fixed sleep standing in for a real state;
- the SDK currently exports no activation helper at all, so this is new surface
  rather than a second way to do something that already exists.

`f-22ecztapc` T-003 was considered and **rejected**. It stamps a lifecycle
revision into the daemon's public wire contract. That collides directly with the
remote work's contract surface, and merging a wire change beside a transport
change would make a failure in either hard to attribute.

`f-225cz7943` T-002 was considered and deferred. Its reproducible-build concern
genuinely overlaps Decision 2, but it also edits `scripts/` and CI, which the
integration owner is actively converging.

Both remain owned by their own Features. This revision borrows one task; it does
not absorb a Feature or create a second plan for one.

## Decision 2 — a real-Linux gate is a merge condition

**T-008** requires the complete repository gate to pass on real Linux, executed
there rather than cross-compiled, before this Feature merges.

This is a new condition, not a restatement. `.github/workflows/ci.yml` already
declares `ubuntu-24.04`, so the question of whether CI would have caught the
`cfg` defect is under separate investigation; the answer changes how T-008 is
satisfied but not whether it is required. A local gate that cannot compile half
the platform-conditional code is not a complete gate.

The Owner's provisioning constraint from revision 3 still binds: only a compiled
binary may be placed on the owner host. If closing this gap needs a different
route than `sg2`, that route is proposed and justified rather than taken by
quietly cloning a repository there.

## Decision 3 — serial checkpoints fix the candidate

The Owner directed that commits fix the work at intervals, arranged for
efficiency rather than ceremony. Each task commits when its own acceptance
passes; the full repository gate runs where parallel branches rejoin, not before
every commit. That keeps a small change from triggering a whole-repository
regression, which is a standing project principle, while still ensuring no
verified result sits only in an uncommitted tree.

`0a97cd8` is the first such checkpoint and already landed.

## What does not change

The architecture decision stands: `RunBackend` remains `native | tmux`, remote is
an endpoint above the wire, transport loss is reachability and never lifecycle,
the endpoint contract is a client-side constant and never a daemon capability,
and nothing is provisioned silently. T-003 and T-004 keep their existing scope
and acceptance.
