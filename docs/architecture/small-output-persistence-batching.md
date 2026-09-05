# Small-output persistence batching

The schema-4 macOS deployment review identified duplicate index maintenance and
small transactions. Current baseline `5ea86238571ce5a148162e69ee3dc36630865955`
already uses schema 5: replay payloads are external files and `replay_chunks`
has only `UNIQUE(run_id, start_byte)`, without the duplicate explicit index.
This qualification changes the remaining cause: the actor formerly drained
only already-queued Appends and committed immediately when the queue was empty.

## Actor behavior

The first collected Append starts a fixed 10 ms deadline. Later Appends can
join across empty-queue intervals but never extend the deadline. The existing
1 MiB payload ceiling also stops collection. Collection owns no store
transaction, replay writer, per-Run worker or extra queue. An empty actor still
blocks indefinitely; it has no periodic batching timer.

Barriers and shutdown end collection. Lifecycle wakeups end it too, and the
collector checks the lifecycle lane before each dequeue because a wake can be
refused when the append queue is full. One lifecycle command taken there is
preserved ahead of newer lifecycle commands on the next actor turn, retaining
their FIFO order. Collected output commits before dispatching that command.

Live delivery continues independently. Payload sync and SQLite WAL `FULL`
commit retain ownership of the durable cursor. Finalization still writes its
missing output and terminal state before publishing terminal state. Storage
retry, checkpoint and commit costs remain outside the collection deadline.
The window is an internal implementation default, not a public latency SLA or
new configuration surface.

## Measurement

[`scripts/measure-small-output.py`](../../scripts/measure-small-output.py)
used protocol 17 for the historical measurements below and independent macOS daemons, sockets and temporary state
directories. Each Python child enters raw PTY mode and signals readiness before
input opens its output gate. Every line contains a shared monotonic timestamp;
the harness samples committed heads through Status and receives live output
through Attach. After SIGKILL and reopen, it compares retained replay bytes
with the original stream, including output beyond the 4 MiB per-Run limit.
The tool now defaults to the workspace's public protocol generation and records
it in new reports; `--protocol` selects an exact historical binary contract for
an explicit comparison arm. It never silently negotiates a weaker generation.
The original workloads and stored observations are unchanged.

The [raw measurements](evidence/small-output-persistence-batching-20261001.json)
bind each executable by SHA-256. Final comparisons alternate baseline and the
candidate three times, without concurrent builds; table values are medians.
Process write accounting includes replay, SQLite and checkpoint work. CPU
excludes child-process CPU and converts macOS Mach ticks with the host timebase.
WAL markers are sampled every 1 ms, without checksum validation: they can miss
short generations and are **lower bounds**, not exact durable commit counts.
Status polling every 10 ms adds observation delay to durable latency. Start
cost is outside process counter deltas; sampled WAL markers also include
pre-existing startup frames. A final 100 ms settling period includes the idle
checkpoint in both arms. Other host workloads and storage noise remain.

| Output fixture              | Output bytes | Process writes/output byte, baseline → 10 ms | Write change | CPU change |
| --------------------------- | -----------: | -------------------------------------------: | -----------: | ---------: |
| 600 × 128 B, 3 ms spacing   |       76,800 |                                89.71 → 48.69 |       −45.7% |     −31.8% |
| 12 × 128 B, 200 ms spacing  |        1,536 |                              197.33 → 197.33 |           0% |      +8.9% |
| Four concurrent paced Runs  |      307,200 |                                11.81 → 10.55 |       −10.7% |      +4.8% |
| 2,048 × 4 KiB, 1 ms spacing |    8,388,608 |                                  1.84 → 1.37 |       −25.2% |     −14.9% |

Paced live-output p95 is 0.205 → 0.146 ms; observed durable p95 is
68.363 → 50.520 ms. Sampled commit markers are 190 → 112. Terminal-event
latency is 13.853 → 17.897 ms for paced output. Sparse output has no batching
opportunity: its write cost is unchanged and measured CPU rises by about 3 ms
over the entire fixture. The four-Run fixture also has no measured CPU win.
This is a measured tradeoff rather than an across-the-board win.

The final repetitions had substantial storage latency variation in both arms:
one sparse candidate sample observes durable p95 of 3.34 s and terminal delay
of 1.40 s, despite live-output p95 of 0.16 ms. That sample remains in the raw
evidence. An earlier pre-review series had less storage noise and paced write
savings around 61%; the table uses the final corrected executable rather than
selecting the quieter series. Neither the collection deadline nor these local
medians establish a storage or terminal latency bound.

The exploratory 25/50 ms sweep records paced write accounting of 2.63/1.75 MB
and observed durable p95 of 53/75 ms. Those single-pass samples predate the
refused-wake correction and do not qualify lifecycle latency or provide a
controlled latency ranking against the final executable. The selected 10 ms
window is the shortest tested window with material savings; it bounds the added
collection allowance while retaining the useful reduction. These results do not predict savings in the
older schema-4 deployment, and do not separate retention/checkpoint costs or
qualify a large fleet, Linux storage, archival compression, or debug export.

## Behavioral proof

The actor tests prove one actual commit across an observed empty-queue interval,
barrier and lifecycle early flush, a refused lifecycle wake with a full append
queue, finite progress during silence and continuous arrivals, and recovery
before versus after committing buffered output. The full-queue test also fails
on the intended boundary when the lifecycle check is removed.

The public SDK test detaches, lets a real Run produce paced output and wait for
more input, waits for the quiet tail to become durable, kills the daemon, and
checks the exact recovered bytes and interrupted terminal event. Existing
terminal recovery, storage retry, retention, and planned exec-upgrade tests
remain required qualification alongside static checks and a public CLI smoke.
