# Refcounting the retained chunk: measured, rejected, and why

## Verdict

Rolled back. `OutputChunk.data: Vec<u8> -> bytes::Bytes` made the operation it
targeted 11.1x cheaper and still lost, because the cost it removed was not the
system's bottleneck — it was the system's throttle.

Measured on cn3 (`n36-001-230`, 64-core Linux, ext4), three interleaved A/B
rounds per shape plus an A/A control at each shape.

## What was changed

`OutputChunk.data` became `bytes::Bytes`. `bytes 1.12.1` was already in the tree
transitively via tokio, so this added no new dependency version. The wire form
was unchanged: `output_chunk_bytes` still emits strict padded base64 (its
`serialize` takes `&[u8]`, and `Bytes` derefs), and the `#[ts(type =
"Uint8Array")]` override keeps the generated TypeScript identical.

The target was one line. `retained_after` renders a mid-chunk suffix on every
catch-up:

```rust
data: chunk.data.get(offset..)?.to_vec(),   // before: a copy
data: chunk.data.slice(offset..),           // after: a refcount bump
```

A catch-up render walks every retained chunk. At a chatty Run's real measured
shape — 8458 chunks averaging 496 B for a 4 MiB log — that is 8458 allocations
per push, on the single reactor thread, for every push while a catch-up is owed.
A standalone microbenchmark on cn3 with that exact shape: **2.140 ms -> 0.193 ms
per render, 11.1x cheaper.** The mechanism was real and the magnitude was real.

## What the A/B showed

Quiet shape (pop=8, chatty=0) — every delta inside the A/A noise floor:

| metric | BEFORE (3 rounds) | AFTER (3 rounds) | A/A control |
|---|---|---|---|
| start (ms) | 6.231 / 6.247 / 6.250 | 6.127 / 6.169 / 6.183 | 6.027 / 6.210 |
| list (ms) | 0.186 / 0.170 / 0.177 | 0.180 / 0.172 / 0.174 | 0.179 / 0.175 |
| stop (ms) | 4.988 / 5.147 / 5.106 | 5.098 / 5.046 / 5.099 | 5.055 / 4.964 |
| remove (ms) | 2.620 / 2.694 / 2.720 | 2.692 / 2.651 / 2.609 | 2.613 / 2.624 |

Chatty shape (pop=8, chatty=1) — `remove` degrades, with zero interval overlap:

| metric | BEFORE (3 rounds) | AFTER (3 rounds) | A/A control |
|---|---|---|---|
| start (ms) | 1566.9 / 1429.9 / 1272.4 | 1117.1 / 1110.4 / 1574.2 | 1435.9 / 1613.7 |
| list (ms) | 0.179 / 0.175 / 0.180 | 0.191 / 0.180 / 0.280 | 0.206 / 0.251 |
| stop (ms) | 109.9 / 108.7 / 108.6 | 107.2 / 106.7 / 106.7 | 109.7 / 109.3 |
| **remove (ms)** | **886.5 / 963.1 / 679.6** | **1914.0 / 1755.8 / 2237.8** | **817.3 / 837.8** |

Every AFTER `remove` sample is above every BEFORE sample and above both A/A
samples. The A/A spread is ~21 ms; the gap is ~1000 ms. This is not noise.
Ratio ~2.16x.

`start` looks improved in two of three rounds and then returns to baseline in
the third (1574 ms). Its round-to-round variance is larger than the apparent
gain, so it does not qualify as a single-metric improvement.

pop=8 chatty=2 timed out at 180 s on **both** arms. That is a pre-existing
scaling wall, equal on both sides, and contributes nothing to the verdict.

## Root cause: the win and the loss are the same mechanism

There is one FIFO channel carrying every durable command — `StageStart`
(create), `Append` (output), `RemoveTerminal`, `Finalize`, `Barrier` — drained
by a single actor thread. The two classes of producer use it with opposite
semantics:

- output `Append` uses non-blocking `try_send` and is dropped on a full queue,
  with the debt re-armed for the next catch-up (`persistence.rs:705`);
- `remove_terminal` uses a **blocking** `send` and then waits on the reply
  (`persistence.rs:1064`).

So a remove's latency is proportional to *how many appends are actually sitting
in the queue* when it arrives. Making the render 11.1x cheaper does not reduce
the work the actor must do; it lets the reactor thread **offer appends 11.1x
faster**. The queue gets deeper, and the blocking remove waits behind more of
it.

This is not a defect in the Bytes migration. The migration did exactly what it
was designed to do. It moved the bottleneck from "render CPU on the reactor
thread" to "persistence queue depth", and `remove` is the verb that pays for
queue depth.

## What would have to change for this to land

The refcount is still the right representation — it is strictly less work for
the same result. What blocks it is that the throughput it unlocks has nowhere
to go. Landing it requires first removing the coupling that turns extra output
throughput into remove latency:

- give lifecycle commands a reserved share of the actor's attention rather than
  a pure FIFO position (note: a previous round established that *jumping* the
  queue is wrong — `Barrier`'s semantics are its FIFO position — so this must be
  a quota, not a bypass); or
- stop making `remove_terminal` block on the shared queue at all.

Either is a larger change than this one, and each needs its own A/B. Until one
of them exists, refcounting the payload makes one verb faster and another
slower, which the ratchet rejects.

## Measurement defects found and fixed on the way

**The benchmark client panicked instead of measuring.** Under a chatty fleet the
daemon answers `stop` with `ControlBackpressure` / `disposition: NotApplied`:

```
Run ... cannot stop: all eight native cleanup owners remained occupied
through Stop admission
```

`CLEANUP_MAX_ACTIVE = 8` (`native_runtime.rs:35`) against
`STOP_ADMISSION_TIMEOUT = 250ms` (`native_control.rs:33`). That is the product
working as designed, and `NotApplied` is explicitly retryable — but the harness
used `.expect("stop")`, so the workload the benchmark exists to measure became a
crash. The harness's `tail -1` then discarded the panic message and left only
the backtrace hint, so six arms reported as unexplained failures. Every verb is
now retry-until-accepted **and prints its refusal count**, because timing only
the accepted call hides a rejection storm — which is itself the signal.

The first chatty datapoint collected before this fix (`start 1225.857 ms`,
against a 1963 ms baseline) read as an improvement. It was an artifact: it was
the one arm that happened not to panic, measured against arms whose samples were
truncated by the crash. It is withdrawn.

**The "long-lived client" caliper is not one.** `Client::request` calls
`connect_for_dispatch` per verb (`ctxmux-client/src/lib.rs:962`) — every
`start`/`list`/`stop`/`remove` opens a fresh Unix socket, does one Hello plus one
request, and drops it. The harness comment claims one long-lived connection. So
the two-caliper discipline (cold CLI vs long-lived client = agentmux) has only
ever been measuring the cold-CLI side twice, and the agentmux-shaped caliper
does not currently exist. Fixing that is a prerequisite for any claim about the
long-lived shape; it does not affect the A/B above, because both arms used the
identical client.
