# Round 11: the cliff is entirely in the persistence layer

- Status: measurement record, 2026-09-14
- Host: cn3 (`n36-001-230`), 64-core Linux, ext4
- Binary: `e8daa95a` (R10, merged as `3ed1a1b`) — **one binary, both arms**
- Shape: `pop=8 reps=15`, `chatty` producers writing continuously
- Harness: `docs/verification/harness/armlib.sh`; conventions
  [§4.3](../benchmark-comparison-conventions.md)

## The question

[The regression check](r11-did-r10-regress.md) established that the chatty cliff
predates R10 and survived it, moving from between 1 and 2 producers to between 2
and 4. It did not establish *where* the cliff lives.

The standing suspect was the create path's zero-WAL proof obligation. But
conventions §5.2 says arithmetic that fits has only failed to be excluded, and
there was already evidence against it: `warmbench` reports `refusals=0`, while
retry exhaustion returns `WalCheckpointBusy`, which would surface as a refusal.

## The discriminator

Run the same fleet with `--state-dir` omitted, so the daemon runs `MemoryOnly`:
no WAL, no fold, no actor, no FIFO. Same binary, same reactor, same poll loop,
same PTYs — `record_output` simply takes the `MemoryOnly` branch
(`lib.rs:4057`) and pushes to the in-memory deque instead of rendering a replay.

No code change required.

## The answer

| shape | metric | persistent | memory-only | ratio |
|---|---|---|---|---|
| chatty=1 | start | 23.6 / 23.0 ms | 1.38 / 1.34 ms | 17x |
| chatty=1 | remove | 15.0 / 15.0 ms | 0.119 / 0.123 ms | **125x** |
| chatty=1 | daemon CPU | 3.19 s | **0.20 s** | 16x |
| chatty=2 | start | 44.0 / 69.2 ms | 1.71 / 1.80 ms | 26–40x |
| chatty=2 | remove | 33.3 / 1731.9 ms | 0.134 / 0.145 ms | **248–11900x** |
| chatty=4 | start | **TIMEOUT 150 s** | 2.08 / 2.00 ms | — |
| chatty=4 | remove | **TIMEOUT 150 s** | 0.158 / 0.149 ms | — |

**Memory-only clears `chatty=4` outright** — 2.0 ms creates where the persistent
build cannot serve a single verb in 150 s. The cliff does not exist without
persistence.

A second signal is at least as informative as the ratios: the two persistent
`chatty=2` arms differ **52x from each other** on a verifiably idle host (`remove`
33.3 vs 1731.9 ms; daemon CPU 5.05 vs 43.2 s), while the memory-only arms are
flat to three digits (0.134 / 0.145 ms). **The instability itself lives only on
the persistent side**, which is what saturation looks like — cost depends on
queue state at entry rather than on the work the verb does.

## What this refutes

This kills a hypothesis I had been building toward, and it is worth being
explicit about that rather than quietly dropping it.

From `poll_and_read_outputs` calling `run.record_output()` inline
(`native_runtime.rs:1451`), I had inferred that the reactor's output sweep might
itself be the cliff. It is not. Memory-only runs **the same reactor, the same
poll loop, the same PTY reads** and shows no cliff at all. The reactor and the
PTY path are exonerated.

The coupling itself is still real — `stop` waits on `drain_commands`, which
cannot run until the sweep finishes, which is why `stop` reads ~106 ms under
saturation. But that coupling is a **consequence** of persistence being slow, not
its cause. Every reactor-side remedy is off the table.

## Where round 11 aims

Inside the persistence layer, one candidate is already located.

`idle_fold_wal` (`persistence.rs:1484`) folds the WAL only from the actor's
`TryRecvError::Empty` branch (`:1573`) — i.e. only when the queue is empty, which
under a chatty fleet it never is. So `start` pays the full unconditional
`truncate_wal_to_zero` (`:2698`), while the output path folds only above 8 MiB
(`:3907`).

This is not a defect in the fold. Using `try_fold_wal_once` rather than the
retrying variant is correct: sleeping up to 550 ms on the actor thread would be
charged to whatever command arrives next. The gap is that **"must not sleep" was
implemented as "only when the queue is empty"**.

At the measured ~1.6 ms/MiB, the `chatty=1` WAL peak of 8.5 MiB is ~13.6 ms
against an observed create premium of 16.4 ms. The shape also has positive
feedback: each start zeroes the WAL, so the fold pays for whatever accumulated
since the last start — slower creates accumulate more, which makes creates
slower, which is the shape of a cliff.

Per conventions §5.2 that arithmetic is **a hypothesis that has not been
excluded, not a diagnosis** — the same trap that made `106 / 20 ≈ 5` look like an
explanation for `stop`. The next probe separates the fold from the rest of the
persistence path before any code changes.
