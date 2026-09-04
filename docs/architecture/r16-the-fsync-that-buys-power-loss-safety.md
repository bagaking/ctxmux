# Round 16: what synchronous=FULL is buying, and why it did not land

- Status: measured, adjudicated, NOT merged — blocked on evidence, not on doubt
- Host: cn3 (`n36-001-230`), 64-core Linux, ext4, glibc 2.36
- Baseline: `e55e6cb`; test fix landed as `baf8bdd`
- Table conventions: [benchmark comparison conventions](../benchmark-comparison-conventions.md)

## The question

`PRAGMA synchronous=FULL` fsyncs the WAL on every commit. On this host that is
1.5–2.9 ms per commit. In WAL mode it buys exactly one thing over `NORMAL`:
survival of **power loss**. Process death — panic, OOM kill, SIGKILL — is
covered identically by both, because the bytes are already in the kernel and
the daemon dying does not stop the kernel writing them out.

So: does ctxmux promise power-loss safety? Reading its own contract,
`choices/009-runtime-persistence-recovery.md`:

- line 111: "**Process crash** or torn WAL recovery therefore yields the
  previous or next complete unit"
- line 210: SQLite durability "**is not evidence of power-loss safety** on a
  filesystem that violates them"
- line 244: fixture — "**daemon kill** while a Run is live restores only the
  committed replay window"

All three are process-death semantics. Power loss is explicitly disclaimed. FULL
is paying, on every commit, for a property the design document refuses to claim.

## What it costs

`pop=8 reps=15`, alternating order every pair, 12 pairs at chatty=2 and 24 at
chatty=8, with a same-order A/A control (FULL vs FULL) measuring the residual
noise floor for each verb. Verdict rule: sign test p<0.05 **and** the median
shift exceeds that verb's own A/A floor.

| shape | verb | FULL | NORMAL | Δ median | sign test | A/A floor | verdict |
|---|---|---|---|---|---|---|---|
| c2 | start | 11.927 ms | 10.611 ms | −1.417 (−11.9%) | 10/12, p=0.039 | 0.060 | ✅ win |
| c2 | stop | 6.437 ms | 3.744 ms | −2.611 (−40.6%) | 12/12, p=0.0005 | 0.160 | ✅ win |
| c2 | remove | 7.926 ms | 5.376 ms | −2.330 (−29.4%) | 12/12, p=0.0005 | 0.343 | ✅ win |
| c8 | stop | 9.568 ms | 4.996 ms | −4.376 (−45.7%) | 24/24, p<0.0001 | 0.418 | ✅ win |
| c8 | remove | 13.511 ms | 11.084 ms | −3.096 (−22.9%) | 22/24, p<0.0001 | 0.470 | ✅ win |
| c8 | start | 26.360 ms | 28.512 ms | +1.976 | 8/24, p=0.15 | 0.613 | no effect |
| c2/c8 | list | 0.211 / 0.263 ms | unchanged | — | p≥0.39 | — | no effect |
| c2 | WAL peak | 8.415 MB | 8.458 MB | +0.043 MB | 2/12, p=0.039 | 0.033 MB | marginal |
| c8 | WAL peak | 8.621 MB | 8.549 MB | −0.058 MB | 15/24, p=0.31 | 0.076 | no effect |

Folded to the verb pair tmux compares against (`stop`+`remove` = `kill-session`):
c2 14.363 → 9.120 ms (−36.5%), c8 23.079 → 16.080 ms (−30.3%).

## The relocation check, which is the part that matters

Five previous rounds in this project died on "the gain and the loss are the same
mechanism". A per-commit fsync that stops happening has an obvious place to
reappear: the checkpoint, which is no longer amortised across many small syncs.
If NORMAL merely **defers** the flush, the WAL grows and the wait moves.

It does not grow. c8 WAL high-water moved −0.058 MB (15/24, p=0.31) and c2 by
+0.043 MB — 0.5%, barely over its own A/A floor and in a shape where every
latency verb improved. There is no reservoir of deferred work.

The c8 `start` cell is the honest blemish. It fails the sign test outright at
n=24 (8/24, p=0.15), so by the standing rule it is **no effect** — but 16 of 24
pairs leaned the wrong way, and `start` is precisely where deferred work would
land. An attempt to settle it by counting `fsync` syscalls under `strace -c -w`
was **discarded**: the FULL arm also reported `fsyncs=0`, which is impossible,
so the instrument was broken and its output is not evidence either way. That
cell is unresolved.

## Why this did not merge

Two gates, both unmet, neither about the numbers above:

1. **The durability witness could not be run.** `persistence_recovery.rs` is the
   evidence that the *promised* property survives, and it only runs on macOS
   here (cn3 has no Rust toolchain). That machine had `syspolicyd` pinned at
   99% CPU for four hours, stalling every freshly-linked binary at
   `_dyld_start`. Compilation succeeded in 11.8 s; execution never started. Not
   a flake to retry — an unusable host.
2. **No same-batch tmux comparison exists.** The published table
   (`chatty-shape-vs-tmux.md`) measured ctxmux at `3ed1a1b`, before R11 and R14,
   both of which moved `start` materially. Citing "no regression" against it
   would compare two moments rather than two systems (conventions §4.1). The
   rerun was written and ready; cn3's root filesystem is **100% full, 0 bytes
   free**, and a SQLite benchmark with no space to write a WAL measures
   disk-full error paths, not performance. Deleting 186 MB of my own stale probe
   binaries reclaimed 135 MB, which other tenants consumed within seconds.

The measurement is strong and the contract argument is sound. Neither is
sufficient: a durability change needs proof that durability held, and a
"no regression" claim needs a baseline from the same batch.

## What carries forward

- The A/B data stands and does not need re-running; it is 12 and 24 pairs with
  a matched A/A control on a quiesced host.
- The unresolved c8 `start` lean needs a *working* instrument. The syscall count
  is the right idea; `strace -c` on a multi-threaded daemon was the wrong tool.
- `baf8bdd` landed independently: the durability witness asserted byte equality
  between the live and recovered windows, which failed 5 of 12 runs against the
  **unmodified** daemon. Root cause is granularity, not durability — the live
  log evicts whole PTY reads (200–600 B) while the durable side drops whole
  64 KiB coalesced rows, so recovery legitimately trails by up to one row. It
  now asserts suffix identity plus a one-row bound, which is what a crash could
  actually violate.
