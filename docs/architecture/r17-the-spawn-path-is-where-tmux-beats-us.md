# Round 17: the spawn path is where tmux beats us

- Status: measured and attributed; the fix is specified but not yet written
- Host: cn3 (`n36-001-230`), 64-core Linux 5.15, glibc 2.36, tmux 3.3a
- Table conventions: [benchmark comparison conventions](../benchmark-comparison-conventions.md)

## The baseline that should have existed sooner

Every round from R10 onward has been tuned against our own previous numbers. The
standing instruction was to compare against tmux on the farm, and not doing so
meant sixteen rounds of measuring whether we had improved rather than whether we
were good. That is now fixed — and the answer is worse than the internal trend
suggested.

Same batch, arms interleaved, order balanced every round (this host runs the
second arm ~1.41x faster, so a fixed order manufactures a winner). Six arms per
side per shape. Zero skipped arms, zero refusals, zero leftover processes, host
busy under 0.6% at every arm. Production config (`synchronous=FULL`).

| shape | verb | ctxmux | tmux | ratio | who |
|---|---|---|---|---|---|
| c1 | start | 9.280 | 4.367 | 2.13x | **tmux** |
| c1 | list | 0.214 | 3.152 | 0.07x | ctxmux 14.7x |
| c1 | stop+remove | 12.903 | 4.063 | 3.18x | **tmux** |
| c2 | start | 11.072 | 4.922 | 2.25x | **tmux** |
| c2 | list | 0.214 | 3.545 | 0.06x | ctxmux 16.5x |
| c2 | stop+remove | 14.023 | 4.419 | 3.17x | **tmux** |
| c8 | start | 23.728 | 11.303 | 2.10x | **tmux** |
| c8 | list | 0.273 | 9.224 | 0.03x | ctxmux 33.7x |
| c8 | stop+remove | 22.247 | 10.597 | 2.10x | **tmux** |

`stop+remove` is summed because tmux's `kill-session` does both; the components
are `stop` 6.111/6.458/9.352 and `remove` 6.873/7.543/12.521. This is a ratio of
medians across unpaired arms, not a significance claim — the two systems are
different programs with different verb sets, so pairing them would be fiction.

We win `list` by 15-34x and lose every lifecycle verb by 2.1-3.2x. The loss is
remarkably stable across a 8x change in output volume, which is itself
informative: whatever we pay extra, we pay per operation, not per byte.

## Where the start cost actually is

`start` forks and execs a child under a PTY. Round 15 established that fork cost
scales with **resident** memory at ~20 us/MiB, and that portable-pty's `pre_exec`
forces Rust's std off its `posix_spawn` fast path onto `fork`+`exec`. That
explained part of the chatty growth and left 1.1-1.8 ms unattributed.

Two mechanisms can physically reach a millisecond. They were separated by
measurement rather than argument, cheapest first.

**Excluded: O(VMA count).** `vm_area_dup` and `anon_vma_fork` run once per VMA at
~100-300 ns. Sampling `/proc/<pid>/maps` while the fleet ran, three rounds:

| chatty | VMAs | RSS | VmPTE | AnonHugePages |
|---|---|---|---|---|
| 0 | 70-71 | 7.5 MB | 92-100 kB | 0 |
| 2 | 72-73 | 22-23 MB | 124 kB | 0 |
| 8 | 76-79 | 55 MB | 188 kB | 0 |

VMA count moves 11% while resident moves 7.3x. Eight extra VMAs buy at most
2.4 us against a 2.05-2.76 ms gap — three orders of magnitude short. Dead.
`AnonHugePages=0` throughout also confirms the new bytes are plain 4K pages, so
the per-page slope applies in full rather than being amortised over 2 MB PMDs.

**Confirmed: the child's `exit_mmap` teardown, and it is the larger half.** After
`fork` the child owns a COW copy of the parent's mm. `execve` then runs
`begin_new_exec` -> `exec_mmap` -> `mmput(old_mm)` -> `exit_mmap`, walking every
inherited PTE and freeing the copied page tables — *before* `do_close_on_exec`.
That ordering is the whole point: Rust's `Command::spawn` blocks reading a
CLOEXEC pipe that only closes at `do_close_on_exec`, so the child's teardown sits
inside the parent's measured spawn. Had the ordering been reversed the cost would
be off the critical path and invisible.

Timing `fork` return, CLOEXEC-pipe EOF and `waitpid` separately, sweeping
resident ballast, five rounds with the size order reversed on alternate rounds
(a monotone sweep on a shared host lets background drift impersonate a slope in
N, and a slope in N is exactly the claim):

| resident | fork | +teardown | spawn total |
|---|---|---|---|
| 0 MiB | 64 us | 213 us | 275 us |
| 16 MiB | 208 us | 532 us | 741 us |
| 32 MiB | 312 us | 776 us | 1090 us |
| 64 MiB | 513 us | 1120 us | 1633 us |
| 128 MiB | 905 us | 1750 us | 2655 us |

| boundary | slope | share |
|---|---|---|
| `fork` (`copy_pte_range`) | **6.43 us/MiB** | 36% |
| exec teardown (`exit_mmap`) | **11.52 us/MiB** | 64% |
| spawn total (what Rust waits on) | **17.95 us/MiB** | 100% |

Round-to-round spread at each size is ~3%, and reversing the order changed
nothing. Teardown is real and is 1.79x the copy.

## What posix_spawn is actually worth

The theory says `posix_spawn` removes *both* halves, because glibc uses
`CLONE_VM|CLONE_VFORK` — the child never gets a private mm, so there is nothing
to copy and nothing to tear down. This project has been wrong about exactly this
kind of kernel-internals claim before while the arithmetic looked fine, so it
was measured on the same host with the same ballast:

| resident | `posix_spawn` | `fork`+`exec` |
|---|---|---|
| 0 MiB | 121.3 us | 275 us |
| 16 MiB | 120.5 us | 741 us |
| 32 MiB | 116.3 us | 1090 us |
| 64 MiB | 124.9 us | 1633 us |
| 128 MiB | 124.5 us | 2655 us |

Slope **0.041 us/MiB** against 17.95. Flat, as predicted — 21x at 128 MiB. Both
halves go, not one.

## The honest shortfall

The spawn law does not explain the whole gap, and claiming it does would be the
fit-the-arithmetic error this project keeps making.

The daemon's resident growth from c0 to c8 is 47.5 MiB. At 17.95 us/MiB that is
**0.85 ms** against a 2.05-2.76 ms per-op gap. Spawn is roughly **a third**. The
remainder is a different mechanism and must not be attributed here until it is
measured on its own. Recording the miss explicitly because a partial explanation
that reads as complete is how the previous four misattributions happened.

## The blocker, and why it is gone

`posix_spawn` cannot run arbitrary child code, so every `pre_exec` job needs an
attribute equivalent. Four map cleanly:

| pre_exec job | posix_spawn equivalent |
|---|---|
| signals to `SIG_DFL` | `POSIX_SPAWN_SETSIGDEF` |
| empty sigmask | `POSIX_SPAWN_SETSIGMASK` |
| `setsid()` | `POSIX_SPAWN_SETSID` (glibc 2.26+) |
| `close_random_fds()` | nothing needed — it already only *marks* fds CLOEXEC, and the kernel closes those at `execve` regardless |
| `ioctl(0, TIOCSCTTY)` | **no attribute exists** |

There is no `POSIX_SPAWN_SETCTTY`. The fallback relies on a Linux rule: a session
leader with no controlling terminal acquires one by *opening* a tty without
`O_NOCTTY`. That only works if glibc applies `SETSID` before file actions — an
ordering question, and a silent one, because a PTY child with no ctty still runs,
still prints, still exits 0. What breaks is job control and SIGWINCH on resize,
which no throughput benchmark would notice. A benchmark-invisible correctness
regression traded for latency is the worst possible outcome of a perf round, so
it was checked before any library code was written.

Result: candidate arm reports `ctty=YES` with `tcgetsid == mysid` — the child
leads its own session and owns the pty. The deliberate control arm (SETSID with
`dup2` only, no ctty-granting open) reports `ctty=NONE (ENXIO)`. The check can
fail and did fail where it should, so it is not vacuous.

`POSIX_SPAWN_SETSID` + `addopen` of the slave replaces `TIOCSCTTY`. The one
blocker is gone.

## What this costs to build

Not free, and worth stating before committing. `SlavePty::spawn_command` returns
`Box<dyn Child + Send + Sync>`, so a raw-pid implementation is structurally
possible — but `Child` requires `try_wait`, `wait`, `process_id`, `kill` and
`clone_killer`, which means hand-rolling reap-state and the already-reaped race
that `std::process::Child` currently handles.

That is real surface area for ~0.85 ms of a 2.05-2.76 ms gap. It is still the
right next move, because it is the only *identified* mechanism, it is worth
2.1x on the verb where tmux beats us 2.1-2.25x, and its benefit grows with
resident memory — which grows with fleet size, the direction this project cares
about. But it should be built knowing it closes a third of the gap, not all of
it.

## Next

1. Implement `posix_spawn` in `third_party/portable-pty/src/unix.rs` behind the
   attribute set above, keeping the `fork`+`exec` path for any configuration the
   attributes cannot express.
2. Re-run this exact batch. The tmux arm is the acceptance criterion now, not our
   own previous number.
3. Attribute the remaining two thirds of `start`, which is still unexplained and
   should not be assumed to be more of the same.
