# ctxmux vs tmux under continuous output

- Status: measurement record, current as of 2026-09-15 (`98bfcc5`)
- Host: cn3 (`n36-001-230`), 64-core Linux, ext4, tmux 3.3a
- Shape: `pop=8 reps=15`, at chatty 1, 2 and 8 — that many Runs in the fleet
  producing output continuously, the rest idle
- Table conventions: [benchmark comparison conventions](../benchmark-comparison-conventions.md)

Every previous ctxmux-vs-tmux table used a `/bin/sleep` fleet — fully quiet,
which is our most favourable shape. That omission mattered more after round 10,
because the whole 43.9x win of that round lives in the chatty shape: the shape
we could not compare had become the shape we optimize for. This page is that
missing arm.

**Rerun 2026-09-15.** The previous numbers were measured at `3ed1a1b`, before
round 11 (64 KiB coalesced rows) and round 14 (empty replay joins the open
transaction), both of which moved `start` materially. Quoting a pre-fix number
in a summary table is what the conventions forbid (§7.2), so both arms were
re-measured in one batch. `start` at chatty=1 improved 22.556 -> 9.280 ms; the
verdicts did not change. chatty=2 and chatty=8 are new — they are the shapes
every round since R10 has been tuned against, and they previously had no tmux
arm at all.

## The table

| 优先级 | # | 维度 | 这一维在测什么 | 什么时候真的咬人 | 子维度指标 | 我方 | 对手 | 比值 | 判定 |
|---|---|---|---|---|---|---|---|---|---|
| 🔴 高 | 1 | 开新会话 | 有会话在不停刷屏时，再开一个要等多久 | 每次开会话都付。agentmux 开一个会话卡 9.3 ms，tmux 只要 4.4 ms | start 中位数 (c1) | 9.280 ms | 4.367 ms | 2.13x | ❌ 输 |
| 🔴 高 | 2 | 关掉一个会话 | 有会话在刷屏时，把另一个会话停掉并清理干净要多久 | 批量清理时线性累加：清 100 个会话我们多花 0.88 秒 | stop+remove vs kill-session (c1) | 12.903 ms | 4.063 ms | 3.18x | ❌ 输 |
| 🟢 低 | 3 | 列出所有会话 | 有会话在刷屏时，拿到全部会话清单要多久 | 交互式工具每次刷新都要列一遍，这是最高频的动词 | list 中位数 (c1) | 0.214 ms | 3.152 ms | 0.07x | ✅ 赢 14.7x |
| 🔴 高 | 4 | 刷屏压力下会不会塌 | 从 1 个刷屏涨到 8 个，两边各自劣化多少 | 决定这是常数劣势还是会随规模放大 | start c1→c8 | 9.280 → 23.728 ms (2.56x) | 4.367 → 11.303 ms (2.59x) | 2.10x | ❌ 输但不放大 |
| 🟡 中 | 5 | 刷屏本身要多花多少 | 同一个二进制，从全静默换成有人刷屏，开会话贵了多少 | 决定这笔钱是设计代价还是缺陷 | start 安静→话多 | 6.4 → 9.280 ms | 4.4 → 4.367 ms | — | 语义差异 |
| 🔴 高 | 6 | 程序吐出去的字节还能不能一个不差读回来 | 进程结束后还能不能重放、按字节游标续读 | tmux 没有对应物 | 保留字节 / 连续性 | 4 MiB / 无空洞 | 渲染后的网格 | — | 能力差异 |

全部三个形状：

| shape | start 我方 | start tmux | 比值 | stop+remove 我方 | kill-session tmux | 比值 | list 我方 | list tmux | 比值 |
|---|---|---|---|---|---|---|---|---|---|
| c1 | 9.280 | 4.367 | 2.13x ❌ | 12.903 | 4.063 | 3.18x ❌ | 0.214 | 3.152 | 14.7x ✅ |
| c2 | 11.072 | 4.922 | 2.25x ❌ | 14.023 | 4.419 | 3.17x ❌ | 0.214 | 3.545 | 16.5x ✅ |
| c8 | 23.728 | 11.303 | 2.10x ❌ | 22.247 | 10.597 | 2.10x ❌ | 0.273 | 9.224 | 33.7x ✅ |

### Alignment notes

- **第 2 行是语义对齐后的数**。我们的 `stop` + `remove` 两步才等于 tmux 的
  `kill-session` 一步，所以按配对比。单看 `stop` (6.111 ms) 或单看 `remove`
  (6.873 ms) 都会低估我们的劣势。
- **第 4、5 行不出跨系统比值**。它们比的是同一个二进制的两种形状。
- **第 6 行是能力差异，不是失分**。tmux 保留渲染后的网格，我们保留原始字节流；
  tmux 从未承诺活过自己的进程。把设计选择记成失败会让整份对比失去可信度。
- **这是中位数之比，不是显著性结论**。两边是不同的程序、动词集也不同，强行配对
  做检验是假精确。同批次保证的是主机状态可比，不是样本可配对。

## Fairness

Both arms ran in one script, interleaved, same host state. Per conventions §4:

- **同批次**：6 个 ctxmux 臂和 6 个 tmux 臂在同一次运行里交错产生。
- **同宽限期**：两边的排空轮询都是 `150 × 0.2s`，两边残留都是 0。给 tmux 更短的
  宽限期会凭空造出一个"tmux 泄漏进程"的假缺陷。
- **顺序平衡**：这台机器上后跑的臂系统性更快（同一二进制 remove 差 1.41 倍），
  所以每轮跑 ctxmux→tmux 和 tmux→ctxmux 各一次。实测两个方向一致。
- **子进程可区分**：ctxmux 跑 `sleep 30` 循环，tmux 跑 `sleep 100000`，
  `pgrep` 不会串台。
- **主机稳定性**：两边 spread 都约 1.1x，这个一致性本身就是"主机状态可比"的证据。
- **每个臂前都静默门禁**：用 `/proc/stat` 的瞬时忙碌比例，不用 loadavg（衰减平均
  会在真忙时读作 2.45，挡不住脏臂）。全批次峰值 busy 0.60%。
- **完整性标记全绿**：0 个臂被判脏跳过、0 次背压拒绝、0 个残留进程。这三项任一
  非零都会让整批作废——之前两个孤儿 daemon 让同一个二进制在两套 harness 上差
  30 倍。

## What this does and does not say

**进步是真的**：round 10 之前 chatty create 是 976 ms，对 tmux 落后约 223 倍；
`3ed1a1b` 时落后 5.16 倍；R11 和 R14 之后落后 2.13 倍。

**但"赢了"仍然是错觉**。round 10 的 43.9x 是**对我们自己的旧基线**量的，不是对
tmux。在这个形状下除了 `list`，我们仍然全面落后。任何"提升 N 倍"的结论都要再问
一句：对自己的旧版本，还是对竞品？

**落后的钱花在哪：85-99% 在持久化层**。这不是推测，是量出来的——同一批次里把
`--state-dir` 去掉跑 memory-only，持久化 actor 整个消失而 reactor、PTY、spawn
路径逐字节不变：

| shape | verb | disk | memory-only | 持久化占比 |
|---|---|---|---|---|
| c2 | start | 12.122 | 1.822 | 85.0% |
| c2 | remove | 8.170 | 0.145 | 98.2% |
| c8 | start | 26.244 | 3.451 | 86.9% |
| c8 | remove | 14.032 | 0.152 | 98.9% |
| c8 | list | 0.228 | 0.212 | 7.0% |

我们输的每一个动词都是 85-99% 持久化；我们赢 15-33 倍的 `list` 只有 7%。详见
[round 18](r18-every-verb-we-lose-is-persistence.md)。

**但 memory-only 不是可以拿来对比的配置**。它不写 WAL、不写 replay 行、不提交
终态，1.822 ms 是归因地板而不是目标。说"我们能比 tmux 快 2.6 倍"是假话——那个
配置根本不提供产品的核心保证。它真正证明的是：reactor、PTY、spawn 三条路径合起来
已经足够快，差距全部在持久化层，别处不需要优化。

**赢的那一项不小**：`list` 快 14.7-33.7 倍，而且 list 是交互式工具里最高频的
动词。之前在安静形状下量到的是 2.70 vs 3.10 ms（1.15x），话多形状下差距反而
拉大——tmux 的 list 随输出压力劣化（3.15 → 9.22 ms），我们的几乎不动
（0.214 → 0.273 ms）。

## 引用哪一组

对外引用**这一组（话多）**。安静形状那组（`start 6.7 / 4.5`，见
[per-verb split](../testing-strategy.md#benchmarks-and-performance-regression) 相关记录）
**对我们有利且不代表真实负载**，引用时必须标注。
