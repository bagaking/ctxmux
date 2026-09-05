# Round 11: the cliff is a WAL fold throughput ceiling

- Status: measurement record, 2026-09-14
- Host: cn3 (`n36-001-230`), 64-core Linux, ext4
- Binary: `e8daa95a` (R10, merged as `3ed1a1b`) — one binary throughout
- Shape: `pop=8 reps=15`, WAL sampled at 20 Hz for the whole arm
- Harness: `docs/verification/harness/armlib.sh`; conventions
  [§4.3](../benchmark-comparison-conventions.md)

## What was already settled

[The cliff is entirely in the persistence layer](r11-cliff-is-persistence.md):
memory-only clears `chatty=4` at 2.0 ms where the persistent build times out at
150 s. Open question: which part of that layer.

## The discriminator

Every fold zeroes the WAL, so the bytes a fold must write back are the bytes
accumulated since the previous one. Hold the fleet fixed, vary only the output
rate, and sample the WAL file directly throughout:

- **fold-bound** → the WAL visibly sawtooths, and either the peak or the
  frequency scales with rate
- **fixed per-create cost** → the premium is flat once any output exists and the
  WAL does not track the rate

No code change, no rebuild.

## The answer: the fold rate saturates at ~57 MB/s

Four arms per rate, both orders, two rounds. Fold counts normalized by arm
duration (sample count ÷ 20 Hz) — raw counts are not comparable, since the
`chatty=4` arms ran 144 s while `chatty=1` finished in 2.3 s.

| chatty | secs  | folds | folds/s | mean peak | **MB/s folded** | start   |
| ------ | ----- | ----- | ------- | --------- | --------------- | ------- |
| 0      | 0.3   | 0     | 0.00    | —         | 0.0             | 6.1 ms  |
| 1      | 2.3   | 12    | 5.27    | 6.79 MB   | 35.8            | 23.0 ms |
| 2      | 17.9  | 130   | 7.27    | 7.56 MB   | **55.0**        | 59.5 ms |
| 4      | 143.9 | 986   | 6.85    | 8.20 MB   | **56.1**        | TIMEOUT |

Two things move together and one does not:

- **The peak is pinned** at the 8 MiB threshold (6.79 → 8.20 MB). Folds are
  triggered by `wal_bytes > WAL_CHECKPOINT_BYTES` in
  `admit_transaction_with_shutdown` (`persistence.rs:3907`), so the WAL simply
  refills to the ceiling and is cut back, over and over.
- **The count scales with rate** — 12 → 130 → 986.
- **The throughput does not.** Between `chatty=2` and `chatty=4` the fleet's
  output doubles while folded bytes per second stay flat to within 2%
  (55.0 → 56.1 MB/s).

That flat line is the cliff. Past ~57 MB/s the WAL cannot be drained as fast as
it fills, every verb queues behind a fold that cannot go faster, and the daemon
stops serving. `chatty=2` sits **right at** the ceiling, which is why its arms
scatter 52x on an idle host; `chatty=4` is past it.

## Why the fold moves so few useful bytes

`PAGE_SIZE_BYTES` is 4 KiB (`:33`) and a WAL frame is `24 + 4096` bytes (`:105`).
Measured on-disk chunks average 253–579 B. Each append dirties a page to carry a
few hundred bytes, so a full 8 MiB fold writes back ~2048 pages for a small
fraction of that in real data. The ceiling is therefore not a disk limit — it is
the cost of moving mostly-empty pages.

## A correction, and a refuted refutation

**My own first reading was wrong.** I initially attributed the fold trigger to the
admission charge, `payload × 4 + 1 MiB` (`:3810`). It is not: that charge only
gates against the 16 MiB ceiling at `:3911`. The fold at `:3908` fires on the
WAL file's actual size. Recorded because the wrong version briefly looked
convincing.

**The research workflow's verdict was also wrong, and more interestingly so.** It
ran 9 candidates and returned **zero survivors**, refuting the fold candidate on
this reasoning: after R10 the actor uses only 6.6% of a core, so its queue is
empty most of the time, so `idle_fold_wal` fires constantly, so the WAL tail must
stay under 1 MiB — its own stated falsifier.

Measured peaks are 6.5–8.3 MB, **6–8x above that threshold**. Every file:line
citation in the refutation is correct and the chain is carefully argued; the
conclusion is still false. The flaw is inferring queue emptiness from CPU
percentage — 6.6% of a core is equally consistent with frequent, very short busy
periods, under which `try_recv` almost always finds a command waiting and the
`Empty` branch that triggers the idle fold is rarely reached.

This is the second time in this project that **zero survivors** has been a
suspicious signal rather than a passing one. An adversarial verifier's
_refutations_ need checking against measurement exactly as its confirmations do,
and a well-argued refutation is the harder one to doubt.

## Round 11's target

Raise the fold throughput ceiling, or stop needing to fold at that rate.

The constraint any candidate must keep: `start`'s admission model proves its own
WAL page quota, and that measurement is only exact from a zero baseline — which
is why `drive_staged_start` folds unconditionally (`persistence.rs:2698`) while
the output path folds only above 8 MiB (`:3907`). That asymmetry is deliberate,
pinned by `a_drained_queue_folds_the_wal_before_the_next_start_needs_it`
(`:6283`). It is a real proof obligation, not an incidental cost.

Note also what the fold rate implies about the existing idle fold: at 5–7 folds
per second the actor _is_ reaching its `Empty` branch regularly — the fold is
firing, just not fast enough to keep the WAL below the ceiling under load. So the
remedy is not "make the idle fold fire more often"; it is to reduce how many
bytes need folding per unit of real output.
