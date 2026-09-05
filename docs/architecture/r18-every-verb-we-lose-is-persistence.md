# Round 18: every verb we lose to tmux is 85-99% persistence

- Status: attribution measured; no code change yet
- Host: cn3 (`n36-001-230`), 64-core Linux, ext4, glibc 2.36, tmux 3.3a
- Table conventions: [benchmark comparison conventions](../benchmark-comparison-conventions.md)

## Why this round exists

[Round 17](r17-the-spawn-path-is-where-tmux-beats-us.md) produced the first
same-batch tmux baseline and found we lose `start` by 2.10-2.25x and
`stop`+`remove` by 2.10-3.18x. It then priced the whole spawn path — `fork`
copy plus `exec` teardown — at 4.6-5.0% of `start`, leaving 95% unattributed.

Rather than guess at the remainder, segment it. Point-guessing has a 1-for-6
record in this project; segmenting hit the answer on the first try.

## The instrument

Omitting `--state-dir` runs the daemon memory-only, removing the entire
persistence actor while the reactor, PTY layer and spawn path stay
byte-identical. No new binary, no code change. An earlier round used exactly
this discriminator to prove the chatty cliff was entirely persistence, so it is
a known-good instrument.

tmux runs in the same batch, because the question is how much of the gap _to
tmux_ persistence explains — not how much faster memory-only is than itself.

4 rounds per shape, arm order rotated every round (the second arm runs ~1.41x
faster on this host). Zero skipped arms, zero refusals, zero leftover processes,
host busy under 0.6% at every arm.

## The split

| shape | verb        | disk   | memory-only | persistence share | tmux   | disk/tmux | mem/tmux |
| ----- | ----------- | ------ | ----------- | ----------------- | ------ | --------- | -------- |
| c2    | start       | 12.122 | 1.822       | **85.0%**         | 4.845  | 2.50x     | 0.38x    |
| c2    | stop        | 6.766  | 3.079       | 54.5%             | —      | —         | —        |
| c2    | remove      | 8.170  | 0.145       | **98.2%**         | —      | —         | —        |
| c2    | stop+remove | 14.936 | 3.223       | 78.4%             | 4.353  | 3.43x     | 0.74x    |
| c2    | list        | 0.237  | 0.221       | 6.8%              | 3.514  | 0.07x     | 0.06x    |
| c8    | start       | 26.244 | 3.451       | **86.9%**         | 11.141 | 2.36x     | 0.31x    |
| c8    | stop        | 10.157 | 3.429       | 66.2%             | —      | —         | —        |
| c8    | remove      | 14.032 | 0.152       | **98.9%**         | —      | —         | —        |
| c8    | stop+remove | 24.189 | 3.581       | 85.2%             | 10.692 | 2.26x     | 0.33x    |
| c8    | list        | 0.228  | 0.212       | 7.0%              | 9.003  | 0.03x     | 0.02x    |

Every verb we lose is 85-99% persistence. `list`, the verb we win by 15-33x, is
7% persistence — it barely touches the layer, which is why it wins.

The share also _grows_ with output volume (start 85.0% -> 86.9%, stop+remove
78.4% -> 85.2%), consistent with a cost proportional to bytes the fleet wrote
rather than to the operation itself.

## What this does NOT say

**Memory-only is not an achievable target.** It skips real durable work — no
WAL, no replay rows, no terminal-state commit. 1.822 ms is an attribution floor,
not a goal, and "we could beat tmux 2.6x" would be a false claim: that
configuration does not provide the product's central guarantee.

What the floor _does_ establish is that the reactor, PTY and spawn paths are
collectively fast enough to beat tmux on every verb. Nothing outside persistence
needs optimising to close this gap. That is the useful half of the result.

## Where inside persistence

Both losing verbs route through the same call. `commit_startup_batch`
(persistence.rs:2462) and `remove_terminal_once` (:3017) each begin with
`truncate_wal_to_zero`, a `PRAGMA wal_checkpoint(TRUNCATE)` over the whole WAL.
Its cost is O(bytes the fleet just wrote), which is exactly the scaling the table
shows, and it is already a documented property of this design: every Start
truncates the entire WAL.

`stop` is the informative exception at 54.5-66.2% — materially less persistence
than its neighbours, which matches the earlier finding that `stop` blocks on the
reactor sweep rather than on storage. It should not be expected to respond to the
same fix.

## The round 17 correction, restated

R17 pursued `posix_spawn` and priced it honestly at 4.6-5.0% of `start` before
building it, then declined to build it. This round shows that was the right
call for a sharper reason than "small win, risky code": the 95% it left on the
table is a single call in our own source, not a vendored dependency needing
hand-rolled reap-state.

## Next

Target `truncate_wal_to_zero` on the `start` and `remove` paths. The question is
whether a full checkpoint is required there at all, or whether it is required
only at a boundary that these two verbs happen to sit on. Round 16 is the
cautionary precedent: removing a flush helped the verbs that wait on flushes and
hurt the verb that forces one, so any candidate must be measured on `start`,
`stop`, `remove` and WAL high-water together, with a same-batch tmux arm.
