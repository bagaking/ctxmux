# ctxmux vs tmux under continuous output

- Status: measurement record, current as of 2026-09-14 (`3ed1a1b`)
- Host: cn3 (`n36-001-230`), 64-core Linux, ext4, tmux 3.3a
- Shape: `pop=8 chatty=1 reps=15`, one Run in the fleet producing output
  continuously, the rest idle
- Table conventions: [benchmark comparison conventions](../benchmark-comparison-conventions.md)

Every previous ctxmux-vs-tmux table used a `/bin/sleep` fleet — fully quiet,
which is our most favourable shape. That omission mattered more after round 10,
because the whole 43.9x win of that round lives in the chatty shape: the shape
we could not compare had become the shape we optimize for. This page is that
missing arm.

## The table

| 优先级 | # | 维度 | 这一维在测什么 | 什么时候真的咬人 | 子维度指标 | 我方 | 对手 | 比值 | 判定 |
|---|---|---|---|---|---|---|---|---|---|
| 🔴 高 | 1 | 开新会话 | 有一个会话在不停刷屏时，再开一个要等多久 | 每次开会话都付。agentmux 开一个会话就卡 22 ms，而 tmux 只要 4.4 ms | start 中位数 | 22.556 ms | 4.375 ms | 5.16x | ❌ 输 |
| 🔴 高 | 2 | 关掉一个会话 | 有会话在刷屏时，把另一个会话停掉并清理干净要多久 | 批量清理时线性累加：清 100 个会话我们多花 1.9 秒 | stop+remove vs kill-session | 22.931 ms | 4.134 ms | 5.55x | ❌ 输 |
| 🟢 低 | 3 | 列出所有会话 | 有会话在刷屏时，拿到全部会话清单要多久 | 交互式工具每次刷新都要列一遍，这是最高频的动词 | list 中位数 | 0.219 ms | 3.186 ms | 0.07x | ✅ 赢 14.5x |
| 🟡 中 | 4 | 刷屏本身要多花多少 | 同一个二进制，从全静默换成有人刷屏，开会话贵了多少 | 决定这笔钱是设计代价还是缺陷 | start 安静→话多 | 6.4 → 22.556 ms | 4.4 → 4.375 ms | — | 语义差异 |
| 🔴 高 | 5 | 程序吐出去的字节还能不能一个不差读回来 | 进程结束后还能不能重放、按字节游标续读 | tmux 没有对应物 | 保留字节 / 连续性 | 4 MiB / 无空洞 | 渲染后的网格 | — | 能力差异 |

### Alignment notes

- **第 2 行是语义对齐后的数**。我们的 `stop` + `remove` 两步才等于 tmux 的
  `kill-session` 一步，所以按配对比。单看 `stop` (8.806 ms) 或单看 `remove`
  (14.126 ms) 都会低估我们的劣势。
- **第 4 行不出比值**。它比的是同一个二进制的两种形状，不是两个系统。
- **第 5 行是能力差异，不是失分**。tmux 保留渲染后的网格，我们保留原始字节流；
  tmux 从未承诺活过自己的进程。把设计选择记成失败会让整份对比失去可信度。

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

## What this does and does not say

**进步是真的**：round 10 之前 chatty create 是 976 ms，对 tmux 落后约 223 倍；
现在落后 5.16 倍。

**但"赢了"是错觉**。round 10 的 43.9x 是**对我们自己的旧基线**量的，不是对 tmux。
在这个形状下除了 `list`，我们仍然全面落后。任何"提升 N 倍"的结论都要再问一句：
对自己的旧版本，还是对竞品？

**落后的钱花在哪**：tmux 几乎不受刷屏影响（4.4 ms 对 4.375 ms，纹丝不动），因为它
存渲染后的网格、不 fsync。我们存原始字节流并且每次提交都 fsync，所以话多形状要多付
16.2 ms。这是设计选择换来的能力（第 5 行），但代价必须如实计入对比。

**赢的那一项不小**：`list` 快 14.5 倍，而且 list 是交互式工具里最高频的动词。
之前在安静形状下量到的是 2.70 vs 3.10 ms（1.15x），话多形状下差距反而拉大——
tmux 的 list 随输出压力劣化，我们的不会。

## 引用哪一组

对外引用**这一组（话多）**。安静形状那组（`start 6.7 / 4.5`，见
[per-verb split](../testing-strategy.md#benchmarks-and-performance-regression) 相关记录）
**对我们有利且不代表真实负载**，引用时必须标注。
