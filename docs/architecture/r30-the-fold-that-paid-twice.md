# R30 — the fold that paid twice

Shipped: the idle WAL fold now declines to truncate a WAL smaller than 256 KiB.
On the quiet persistence path this removes roughly two of every three
durability barriers per lifecycle verb and makes `start`, `stop`, and `remove`
each ~0.7–0.9 ms faster, with no cell degraded anywhere in the matrix.

Pre-registration: [`r30-prereg-the-fold-that-pays-twice.md`](r30-prereg-the-fold-that-pays-twice.md).
Every number below was predicted there before it was measured.

## What shipped

One constant and one comparison, in `crates/ctxmux-daemon/src/persistence.rs`:

```rust
const WAL_IDLE_FOLD_FLOOR_BYTES: u64 = 256 * 1024;

// in idle_fold_wal, replacing `if bytes > 0`
if !matches!(file_len(&store.wal_path), Ok(bytes) if bytes >= WAL_IDLE_FOLD_FLOOR_BYTES) {
    return false;
}
```

That is the whole behavioural change. The rest of the diff is the four fixtures
that pinned the old zero threshold, re-expressed against the floor.

## Two candidates died before any code was written

The research pass ranked five directions. The top two both dissolved under a
single probe each, which is the cheapest possible outcome and the reason to run
probes before writing code rather than after.

**"Drop the redundant `ensure_listening` handshake."** The CLI really does open
two connections per verb — `ensure_listening` performs a complete `ping()`
round trip even when the daemon is already up, then the command opens a second
one. The survey estimated 0.2–0.4 ms. Measured against the shipping daemon by
speaking the wire protocol directly (n=400, warmed):

| path | median | p90 |
|---|---|---|
| connect + hello + close | **0.0419 ms** | 0.0495 ms |
| connect + hello + list + close | 0.0728 ms | 0.0829 ms |

0.042 ms against an A/A noise floor of 0.46 ms — **eleven times below the
floor**, and an order of magnitude below the estimate. Killed by its own
pre-registered falsifier.

**"Is the CLI floor a benchmark artifact?"** Partly, and the answer came from
reading rather than measuring. agentmux holds a long-lived `CtxmuxClient`
(`ctxmux-run-adapter.ts:804`), so it never pays the ~0.53 ms process spawn the
harness pays. But the SDK's `#request` opens a fresh connection and Hello per
call (`packages/sdk/src/client.ts:640`, `:718`), so the consumer does pay one
handshake per verb. At 0.042 ms neither half is worth attacking.

## The inventory was wrong about the fsync

The per-verb research inventory attributed ~2 ms on each of start/remove/stop to
"the COMMIT fsync" — singular. That number came from subtracting memory-only
from persistence-ON, and **subtraction names a layer, not a line.**

Counted instead, with strace on the daemon and each syscall attributed to the
client-observed window of the verb that caused it:

| verb | wall each | fsyncs per verb | ms each | share of wall |
|---|---|---|---|---|
| start (persist) | 9.485 ms | **4.85** | 0.805 | **41.2%** |
| remove (persist) | 4.938 ms | **3.85** | 0.720 | **56.1%** |
| start (memory) | 2.903 ms | 0 | — | — |
| remove (memory) | 0.723 ms | 0 | — | — |

Not one barrier. Nearly five. That distinction is what made this round
possible: reducing *how many times* we prove durability is a different lever
from weakening `synchronous`, which an earlier round already tried and vetoed.
One changes the count of proofs, the other changes whether there is a proof.

Resolving every fd to its path shows all of them on the WAL, and shows what the
most expensive one is:

```
one start, client-observed 8.241 ms
  +4.186 ms  pwrite64  state.sqlite3-wal  "7\177\6\202..."  <- 0x377f0682, the WAL header magic
  +6.248 ms  fsync     state.sqlite3-wal  1.9910 ms   <== BARRIER 1
  +6.286 ms  pwrite64  state.sqlite3-wal  (six page frames)
  +6.537 ms  fsync     state.sqlite3-wal  0.9070 ms   <== BARRIER 2
  +8.018 ms  fsync     state.sqlite3-wal  0.0510 ms   <== BARRIER 3
```

Barrier 1 is a freshly written WAL **header** being synced on its own, and it is
the most expensive of the three. It exists only because the WAL had been reset
to zero since the previous verb.

## Why a small WAL kept getting reset

`idle_fold_wal` folded whenever the WAL was merely non-zero. The justification
at the call site is real: folding costs ~1.6 ms/MiB, so pay it while the actor
is idle rather than on a later verb. That reasoning is correct for a large WAL.
It omits the other side of the ledger.

Priced directly, plain SQLite with the daemon's pinned pragmas (n=60):

| next commit lands on | median | p90 |
|---|---|---|
| a WAL just truncated to zero | 1.9117 ms | 2.5208 ms |
| a WAL already ~256 KiB long | 0.9167 ms | 1.1630 ms |
| **penalty the truncation imposes** | **+0.9950 ms** | |

and the fold's own cost by size: 1.25 ms at 0.07 MiB, 1.44 ms at 0.26 MiB,
2.61 ms at 1 MiB, 9.61 ms at 8 MiB.

So on a quiet fleet the old code spent ~1.25 ms clearing a few KiB, then charged
the *next* verb ~1.00 ms for the privilege of starting from zero — **paying
twice to avoid a cost smaller than either payment.** Under load the same trade
is the good one the call site describes, because by then the WAL is megabytes.

This is the mirror image of an experiment two rounds ago that lowered the
*output-side* fold threshold from 8 MiB to 1 MiB and made create 0.7–0.9 ms
slower by folding eight times as often. That rollback is evidence for this
change: fewer, larger folds beat more, smaller ones. Same axis, opposite
direction, and the earlier result predicted the sign of this one.

## The ratchet gate

Paired cells, alternating arm order (cn3's second arm runs systematically
faster), sign test against an empirical A/A floor measured the same way and at
the same cost. Four shapes, 16 cells, Holm-Bonferroni across all of them.

**c=0, persistence ON — the target shape** (20 pairs, after the final rebuild):

| verb | base | cand | delta | A/A | wins | p |
|---|---|---|---|---|---|---|
| start | 4.496 | 3.583 | **+0.914** | 0.100 | 20/20 | <1e-5 |
| stop | 3.961 | 3.257 | **+0.704** | 0.122 | 20/20 | <1e-5 |
| remove | 3.557 | 2.708 | **+0.849** | 0.166 | 20/20 | <1e-5 |
| list | 1.313 | 1.304 | +0.009 | 0.021 | 7/20 | 0.26 |

Every other cell — c=1 persistence, c=8 persistence, c=0 memory-only — came back
flat under Holm-Bonferroni against its own A/A floor. **No cell degraded.**

## The predictions, and how they held

All four were written down before the data existed.

1. **start and remove faster by roughly the reset penalty (−0.5 to −1.0 ms).**
   Held: +0.914, +0.849, and stop +0.704 — the predicted magnitude, not merely
   the predicted sign.
2. **Barrier count drops and the header barrier disappears.** Held:
   3.00 → 1.20 per start, 2.60 → 1.10 per remove.
3. **Chatty shapes unchanged.** Held. c=8 start +0.373 against an A/A floor of
   0.368, stop −0.020 against 0.426 — noise in both directions, as predicted,
   because under load the WAL is megabytes whenever the fold runs and the floor
   never engages.
4. **Idle CPU stays at 0.000%.** Held: 0.0000% on both arms across a 60 s quiet
   window, including the new case where the candidate deliberately *keeps*
   61,832 WAL bytes instead of truncating them.

Prediction 3 was the one I flagged in advance as most likely to be wrong, on the
grounds that a mechanism fitting two shapes has died on a third before. It
survived.

The engagement check matters as much as the timings: the candidate's final WAL
sits at 65,952 bytes where the control's sits at 0. A floor that never fired
would report "flat" and be indistinguishable from a floor that fired and did not
help — so the WAL length is what proves the knob was actually turned.

## Against tmux, same batch, same host

Rotated arm order across three arms so none is systematically last. tmux has no
persistence to switch on, so it is compared against ctxmux **as shipped, with
persistence ON** — that is the honest gap, not the flattering one.

| 优先级 | # | 维度 | 这一维在测什么 | 什么时候真的咬人 | 子维度指标 | 我方 | 对手 | 比值 | 判定 |
|---|---|---|---|---|---|---|---|---|---|
| P0 | 1 | 创建延迟 | 起一个新 Run 到可用的端到端时间 | 舰队频繁拉起短任务时,直接叠加到每个任务的墙钟 | start 中位 (ms, c=0 持久化) | 3.550 | 3.046 | 1.17x | ❌ 输 |
| P0 | 2 | 拆除延迟 | 停止并回收一个 Run 的完整代价 | 高 churn 场景下与创建同频发生 | stop+remove 中位 (ms) | 5.931 | 2.657 | 2.23x | ❌ 输 |
| P1 | 3 | 枚举延迟 | 列出全部 Run 的代价 | 控制面轮询、UI 刷新 | list 中位 (ms) | 1.282 | 1.734 | 0.74x | ✅ 赢 |
| P1 | 4 | 耐久性 | 进程崩溃后 Run 记录是否还在 | daemon 崩溃或重启后要恢复舰队 | 崩溃后可恢复 | 是 | 否 | — | 能力差异 |

本轮把 start 对 tmux 的差距从 1.47x 收到 **1.17x**(关闭了 64.7% 的差距),
teardown 从 2.92x 收到 **2.23x**(关闭 35.8%)。list 一直是赢的,本轮未动。

第 4 行是能力差异而非性能差异:tmux 全树 grep `fsync|fdatasync|sqlite|O_SYNC`
零命中,它的 new-session 是 `xcalloc` + 红黑树插入 + `forkpty`,kill-session 是
`close(fd)`。它在 c=0 便宜,恰恰因为它不提供 ctxmux 卖的那一件东西。所以剩下的
差距里有一部分是**契约的价格**,不是可优化的浪费。

## Tests, lint, format

Full workspace suite on cn3, both arms:

| arm | passed | failed |
|---|---|---|
| control | 436 | 16 |
| candidate | 435 | 17 |

The failure *sets* differ by one test in each direction: the candidate failed
`fresh_level_a_fork_materializes_then_releases_its_parent_before_reservation`,
the control failed `multiple_terminal_candidates_replace_earliest_run_and_its_exact_key`.
Both are pre-existing flaky tests. Running the daemon lib 20 times per arm:

| test | control failed | candidate failed |
|---|---|---|
| `fresh_level_a_fork_materializes_…_before_reservation` | **4/20** | **0/20** |
| `multiple_terminal_candidates_replace_earliest_run_…` | 8/20 | 6/20 |

Per-run suite failure counts were 4–8 on the control and 4–6 on the candidate —
the same distribution, wandering. **Candidate-only failures: none.** The one
test the single gate run pinned on the candidate failed *zero* times out of
twenty on that arm and four times on the control: not merely insignificant, the
opposite sign. How that was nearly called a regression is the next section.

Linux clippy: candidate 0 errors, control 0 errors — parity, after fixing two
`u64 as usize` casts I introduced in the new fixtures. `cargo fmt --check`
reports 10 diffs in `persistence.rs`, all of which reproduce on unmodified HEAD.

Mutation check: reverting the floor to `if bytes > 0` turns
`an_idle_fold_skips_a_wal_below_the_floor` red with the right message
("a WAL under the floor must be left alone, not truncated"); restoring it turns
green, with md5 verification on the source both ways.

## Two probes that failed silently, and what they cost

Both belong in the record because both produced a confident, wrong answer that
looked exactly like a right one.

**A test-attribution probe that never ran a test.** To decide whether the
differing failure was a regression, I ran each test in isolation with
`--exact`. It reported 10/10 failures on *both* arms — for tests that had each
passed once in the full suite. The isolated run measured a different condition
than the gate did, so I rewrote it to run the suite. The rewrite passed
`--test creation`, which is not a test target: those tests live in the daemon
lib. cargo errored out, my regex matched nothing, and the probe printed
"0/5 failed" for every cell and concluded "pre-existing flakiness, not a
regression." **A command that never ran scored as a clean pass.** The tell was a
`-1` in the per-run summary, which is the only reason I looked.

That is the same failure shape as an earlier round's byte-comparison script that
compared two empty databases and printed PASS. The fix is the same one: a check
that cannot fail on empty input is not a check. The third version fails loudly
when a run produces no parsable result, and refuses to score it.

**An underpowered verdict stated as a conclusion.** The corrected probe, at
n=5, reported 0/5 for the control and 2/5 for the candidate and printed
`*** CANDIDATE-ONLY FAILURE -- roll back ***`. That threshold was wrong: against
a suite whose failure count swings between 4 and 8 on *both* arms, 0/5 vs 2/5 is
p≈0.44 — indistinguishable from noise. The script stated a verdict its own
sample size could not support.

At n=20 the same comparison came back **4/20 control, 0/20 candidate** — not
just insignificant but the opposite sign. A probe that would have rolled back a
change that measurably improves three verbs, on the strength of five samples of
a coin.

Reading the test settles what the statistics could not, and should have been the
first move: `fresh_level_a_fork...` builds a `RunManager::default()` with no
state directory, so persistence is off and no WAL exists. `idle_fold_wal` runs
only on the persistence actor thread. **There is no contact surface between the
change and the test** — it is a `multi_thread` fixture spinning on
`Arc::strong_count`, i.e. a worker-shutdown race.

The lesson is not "run more reps." It is that a probe should state what its
sample can support, and that a mechanism argument — is there even a path from
the change to the symptom? — is often cheaper and stronger than more samples.
Here it was also *right* while twenty times the data was still being collected.

## What I would do differently

The research pass's headline numbers were estimates presented alongside measured
ones, and the ranked list put a 0.042 ms candidate at #2 because it was
estimated at 0.2–0.4 ms. Both top candidates cost one probe each to kill. That
was cheap, but it would have been cheaper to demand a measured floor before
ranking anything — an estimate and a measurement should not sort into the same
list.

The real win came from disbelieving a number in that same inventory: "~2 ms, the
COMMIT fsync." It was arrived at by subtraction and it was wrong about the
count. Counting the syscalls took one script and turned a vetoed direction
(weaken the fsync) into an unexplored one (need fewer of them).
