# f-22pczwrzr: superseded on main by externalized replay (audit)

## Summary

Feature `f-22pczwrzr` ("Recover bounded persistent capacity before Run
mutations") landed candidate `c168c0a` on branch `feat/f-22pczwrzr`.
The branch subsequently measured capacity (`d42494f`), externalized replay
payloads from SQLite (`c5f6f45`), and hardened generation recovery (`aa590f1`).
The latter mechanism commits were reshaped onto `main` as `2f03c55` and
`2677f76`. Author dates are Git metadata and do not establish causal order.
The ADR 009 invariant "Physical page pressure is an independent retention
boundary" survives verbatim on `main`; its implementation changed.
This historical audit does not modify Tracker state, rewrite branches, or
claim `c168c0a` is buggy. Current qualification belongs to the active Tracker.
The original timestamp-bearing report is retained in private audit evidence.

## Source lineage

Each referenced SHA was checked as a Git commit. Paired subjects match;
source identities, rather than workstation timestamps, bind this audit.

| Feature SHA | Main SHA            | Subject                                                                       |
| ----------- | ------------------- | ----------------------------------------------------------------------------- |
| `c168c0a`   | feature branch only | fix(daemon): reclaim replay prefixes before the page limit refuses a write    |
| `d42494f`   | `63583b0`           | docs: measure replay capacity past the 384 MiB ceiling, rank compression last |
| `c5f6f45`   | `2f03c55`           | fix(persistence): externalize replay payloads from sqlite                     |
| `aa590f1`   | `2677f76`           | fix(persistence): harden replay generation recovery                           |

`git branch -a --contains c168c0a` reports `feat/f-22pczwrzr` and
`merge-tonight`; it does not report `main`. `git branch -a --contains
2f03c55` and `... 2677f76` both report `main` and `remotes/origin/main`.

`git diff --stat` of each rebased pair shows only `d42494f` vs
`63583b0` differs (2 lines in an unrelated architecture doc); `c5f6f45`
vs `2f03c55` and `aa590f1` vs `2677f76` are byte-identical trees.
`backup/pre-reshape-aa590f1` points at `aa590f1`
(`git for-each-ref`).

## Behavioral equivalence

Four claims from the feature goal, each grounded in code on `main`
(`crates/ctxmux-daemon/src/persistence.rs`, HEAD `a9db2c5`).

### (a) Reclaim before write on page pressure

On `main`, the eviction sweep is called on every finalize:

```
persistence.rs:4187        let _ = prune_global_replay(&transaction)?;
```

This line sits inside `fn append_transaction_with_shutdown`
(persistence.rs:4152), which is the write-transaction path for output
and terminal batches. The callee dispatches to
`prune_global_replay_to(GLOBAL_REPLAY_BYTES)`
(persistence.rs:5970-5972).

Replay payloads no longer live inside SQLite; they live in a separate
generation file. `StateStore` carries the active generation as
`replay_file: String` (persistence.rs:2278). Startup selects or creates
that file at persistence.rs:2411-2415, and `fn normalize_replay_files`
(persistence.rs:4307) trims an interrupted append tail before the store
becomes observable.

Where `c168c0a` reclaimed _reactively when the SQLite page ceiling
refused a write_, `main` prevents the refusal by keeping the bulk of the
bytes out of the page-limited file: the SQLite side stores only
`run_id`, byte ranges, generation name, file offset, and byte length
(ADR 009 lines 149-152).

### (b) Durable head preserved; last row never dropped

`PersistentRun` holds `durable_head: Arc<AtomicU64>`
(persistence.rs:750). The doc-comment on `offered_head` at
persistence.rs:756-767 contrasts the two watermarks and explains why
`durable_head` remains distinct, and why both are cloned into every
handle for one Run's binding.

The global eviction sweep documents the "never empty a Run" invariant
verbatim at persistence.rs:5974-5987:

> Shed bytes across Runs until the daemon-wide replay total fits.
>
> Eviction is by row in ordinal order — oldest bytes daemon-wide first —
> and a Run's LAST row is never dropped, so no Run is emptied to serve
> another's pressure.
>
> Coalescing made that "never drop the last row" rule load-bearing in a
> way it was not before. A Run used to hold hundreds of small rows, so
> there was almost always one to drop; now a Run commonly holds a
> single 64 KiB row, and if every Run holds one, no candidate exists at
> all and the ceiling cannot be enforced. So the last row is not
> skipped, it is TRIMMED: its front is cut back in place, which sheds
> exactly the bytes needed and keeps the Run's window contiguous. A row
> is only ever shortened from the front, never emptied, which is what
> preserves the invariant the skip was there to protect.

The trim path is `fn trim_oldest_row` (persistence.rs:6048); the
row-drop path is the loop inside `fn prune_global_replay_to`
(persistence.rs:5988-6025).

### (c) Reopen validation

`main` validates state twice on open — once against the raw connection
(persistence.rs:2458), and again after normalization and physical checks
(persistence.rs:2486):

- `fn validate_application_state` (persistence.rs:4869) walks every
  `runs` row and rebuilds counts and creation-key uniqueness from
  durable columns.
- `fn normalize_replay_files` (persistence.rs:4307) truncates a
  post-crash append tail against the durable index and refuses to open
  a generation file shorter than its references.
- `fn validate_physical_limits` (persistence.rs:6248) enforces the
  fixed file ceilings, called both on open (persistence.rs:2478) and
  after each admitted transaction (persistence.rs:4296 inside
  `fn validate_files`).

### (d) Fixed file limits unchanged

The four ceilings the feature goal names are declared on `main` at
their original values:

- `const DATABASE_MAX_BYTES: u64 = 384 * 1024 * 1024;`
  (persistence.rs:38)
- `const WAL_CHECKPOINT_BYTES: u64 = 8 * 1024 * 1024;`
  (persistence.rs:40)
- `const WAL_MAX_BYTES: u64 = 16 * 1024 * 1024;` (persistence.rs:63)
- `const STATE_FILES_MAX_BYTES: u64 = 768 * 1024 * 1024;`
  (persistence.rs:70)

The 768 MiB state-directory budget replaces the old 404 MiB aggregate
that mistook the SQLite ceiling plus WAL and SHM for the whole state
directory (persistence.rs:65-70). Replay payloads now occupy their own
generation file inside that 768 MiB envelope; the 384 MiB, 8 MiB, and
16 MiB numbers are unchanged.

## Invariant survives, mechanism changes

The ADR 009 paragraph at
`docs/architecture/choices/009-runtime-persistence-recovery.md:163-176`
reads verbatim on `main`:

> Physical page pressure is an independent retention boundary. Before
> startup normalization or an allocating persistent mutation, the owner
> must reclaim oldest replay prefixes when the main database lacks one
> admitted transaction's page headroom. Logical replay below 256 MiB
> does not prove physical capacity: small rows and partially occupied
> pages can exhaust the fixed page ceiling. Reclamation uses bounded,
> spill-disabled transactions under the existing WAL charge proof; it
> preserves Run/key/spec/lifecycle metadata and the durable head, and
> advances the surviving replay floor and truncation fact atomically.
> It must work on a valid same-schema database already at its physical
> limit and survive reopen without inventing contiguous bytes. The
> physical cap stays fixed; no VACUUM, migration, external database
> rewrite, or silent Run deletion is part of this operation. A
> page-limit exhaustion that cannot make progress is not an external
> transient disk-full event and must not monopolize the actor forever.

This paragraph is in `main`. What changed after `c168c0a` is the
implementation that satisfies it: on `feat/f-22pczwrzr` reclamation ran
_inside_ SQLite (deleting whole replay-chunk prefixes in the same
database that hit its page ceiling); on `main` the replay payload never
lives inside SQLite at all, so the 384 MiB page ceiling is no longer
the write-path bottleneck for retained output. The reclamation-before-
mutate hook is still present (persistence.rs:4187), and the invariant's
guarantees — durable head preserved, Run/key/spec/lifecycle metadata
preserved, physical cap fixed, no VACUUM or silent Run deletion — are
carried by the eviction sweep and the frozen ceilings cited above.

## Consequence for f-22pczwrzr closeout

The feature goal, as recorded in
`.bagakit/feature-tracker/features/f-22pczwrzr/state.json`, is:

> Existing healthy Runs remain usable when bounded retained replay
> reaches the physical database page ceiling; persistence reclaims only
> replay prefixes with honest cursors and gaps before new writes,
> preserving Run metadata and fixed file limits across live use and
> restart.

This goal is met on `main` today by a different mechanism than the
candidate SHA the tracker recorded. When the tracker closeout is
executed by the owner, a `documentation-disposition=verified_current`
citing this file is the disposition the code and the ADR support. This
document does not itself execute that closeout.

## Non-claims

- Not a claim that `c168c0a` is buggy. The author (`bagaking`) chose a
  different mechanism about 3h11m later on the same branch (`c5f6f45`,
  later reshaped onto `main` as `2f03c55`); both approaches target the
  same invariant.
- Not a modification of tracker state. `state.json` still records
  `candidate_sha: c168c0ab9cd849bfade68461b62684982c71f688`; the
  tracker closeout, if run, is the owner's action.
- Not a rewrite or deletion of any branch. `feat/f-22pczwrzr` and its
  `backup/pre-reshape-*` peers remain reachable.
