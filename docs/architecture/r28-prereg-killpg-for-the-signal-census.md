# R28 pre-registration — replace the enumerate-to-signal census with `killpg`

Written before any product edit. R27 died because its falsifier was measured
wrong, so this document states, in advance, what would close the direction and
how each number will be obtained.

## The measured target

cn3, R26's own binaries, memory-only, c=0, n=40, 868 host processes:

| | ms |
|---|---|
| stop + remove | 5.734 |
| two CLI invocations (floor + IPC) | 2.614 (46%) |
| daemon-side work | 3.120 (54%) |
| — `remove` | 0.074 — **closed, nothing there** |
| — `stop` | **3.046 — the whole target** |
| two `/proc` censuses within it | 2.018 (66%) |
| one census (readdir 0.775 + 868×getsid 0.234) | 1.009 |

## Which census is which — settled by observation, not argument

I hypothesised that the census immediately after signalling was structurally
futile (the `QUIESCENCE_FIRST_POLL` comment says that check "races the kernel
and essentially always loses"). A research pass argued the opposite. Both were
partly wrong; `strace -tt` on one real stop decides it:

```
.416443  openat("/proc")            <- census #1  (signal_members enumerating)
.439976  kill(1865722, SIGTERM)     <- the signal, AFTER the walk
.440103  clock_nanosleep(200us)     <- QUIESCENCE_FIRST_POLL
.440390  waitid(...CLD_KILLED...)   <- leader already dead
.440425  openat("/proc")            <- census #2  (wait_quiescent confirming)
.461000  waitid / wait4             <- reap: the confirm SUCCEEDED
```

Facts established:

1. **Census #2 is the confirm and it succeeds** — the reap follows it directly.
   My "futile check" reading is refuted: it is not paid for nothing. Reordering
   or deleting it saves nothing and would break the proof.
2. **Census #1 exists only to build a list of PIDs to signal.** The signal it
   ultimately sends, in the common case, is one `kill()` to the leader.
3. The leader is already `CLD_KILLED` *before* census #2 begins, so census #2
   walks all 868 processes purely to establish that **no descendants** exist.

(Absolute times above are strace-inflated — each `getsid` costs ~25 µs under
ptrace against ~0.3 µs untraced. The *order* is what this trace is for; the
1.009 ms per census comes from the untraced replica.)

## The candidate

Replace `signal_members`' enumerate-then-kill-each with a single
`kill(-pgid, sig)` — `rustix::process::kill_process_group`, already imported in
this file for `interrupt()` under a `cfg(not(macos))` gate.

`portable-pty` makes the spawned child a session leader, so pid == sid == pgid
(`native_session.rs:33-35`). One syscall signals the entire process group with
no enumeration. **Census #2, the confirm, is unchanged** — the whole-session
guarantee is still proved by observation, exactly as today.

Expected saving: 1.009 ms of stop's 3.046 ms daemon-side cost (**33% of the
target, ~18% of teardown**), with the confirm census left intact.

### Why pgid reuse is already safe

`leader_is_terminal` observes the leader with `WaitIdOptions::NOWAIT`
(`native_session.rs:296`) and `reap_leader` runs only at the very end. The
leader therefore remains an unreaped zombie in its own process group for the
entire window in which we signal. A pgid cannot be recycled while any process —
zombie included — remains in the group, so `kill(-leader)` cannot leak onto a
stranger. This is the same invariant `require_waitable_anchor` already enforces;
the candidate adds no new guard.

### The one real behavioural gap

`killpg` signals the *group*; today's code signals the *session*. The delta is
exactly: a descendant that called `setpgid` (left the group) but not `setsid`
(stayed in the session). Today it is signalled; under a naive killpg it is not.

- A `setsid` descendant leaves the session entirely and is owned by **neither**
  the current code nor the candidate. No delta — it was never covered.
- The `setpgid`-in-session straggler is the entire risk, and the confirm census
  already enumerates session members. So: after `killpg`, if the confirm finds
  survivors, signal *those specific PIDs* through the existing per-member
  `getsid`-revalidate-then-`kill` path before escalating to KILL.

Common case: 1 `killpg` + 1 confirm census. Straggler case: + a targeted pass.
**In no case is "empty" reported over a live session** — the confirm is
untouched, so the guarantee cannot silently weaken.

## Falsifiers, pre-registered

R27's lesson: a subtraction is only a measurement if both arms run the code
being subtracted. Every check below observes the mechanism directly.

**F1 — the win must appear as a syscall count, not as wall-clock.**
`strace -f -e trace=openat,kill` one stop of a live Run on cn3 at comparable
load. Required: `openat("/proc")` drops **2 → 1**, and exactly one
`kill(-<leader>, SIGTERM)` appears. If the census count does not drop, the
candidate did not engage — **stop, do not look at timings** (a knob that never
engaged reports "flat").

**F2 — the guarantee fixture, and it must FAIL first.**
A leader that forks (a) a child calling `setpgid(0,0)`, staying in-session, and
(b) a child calling `setsid()`. Three arms, checked with `kill -0`:
- current code: (a) is killed
- killpg **without** the fallback: (a) **survives** — this regression MUST be
  observed, or the fixture is not exercising the gap and proves nothing
- killpg **with** the confirm-fallback: (a) is killed

Nobody is expected to own (b). If arm 2 does not leave (a) alive, the fixture is
wrong — fix the fixture, not the theory. *(Mutation-testing discipline: a test
that cannot go red has not been shown to test anything.)*

**F3 — paired timing, alternating arm order.**
≥12 pairs, arms alternated (cn3's second arm runs systematically faster), sign
test, against an empirical A/A floor measured the same way. Expect ~1.0 ms
median improvement on `stop`. If the paired delta sits inside the A/A floor it
is not real.

**F4 — the ratchet gate.** `start`, `list`, `remove` and stop under c=8 must not
degrade. Any degradation on any metric → roll back and write the post-mortem.

## Out of scope, and why

**cgroup v2** (`cgroup.kill` + `cgroup.events:populated` + inotify) would remove
*both* censuses and is strictly stronger — it catches `setsid` escapees too. It
is deferred, not rejected, on two constraints: it needs a delegated cgroup
subtree (a non-root daemon on a bare host gets EPERM), and placing the child in
its cgroup means changing the spawn path, which `portable-pty` does not expose.

One shell line decides whether it is even available, and it costs nothing to
run before R29 designs anything:

```
stat -fc %T /sys/fs/cgroup                       # must be cgroup2fs
mkdir <delegated-root>/probe && echo $$ > <delegated-root>/probe/cgroup.procs
```

EPERM → closed on this host.

**pidfd** is closed for this question: it is per-process, cannot signal a group,
and cannot enumerate a session. It addresses leader confirmation, which is
already cheap.

**Cheaper enumeration APIs** are closed: `/proc/<pid>/task/<tid>/children` is
per-parent and needs a recursive walk; the netlink proc connector needs elevated
privilege and a live membership cache; BPF task iteration needs CAP_BPF. None
answers "who is in session S" for under 1 ms as an ordinary user.

## Portability

`kill(-pgid, sig)` is POSIX and behaves identically on macOS, so the signal path
needs **no `cfg` split** — only the existing import gate widened. The confirm
census on macOS keeps using `ctxmux_process_stats::process_ids`. No macOS
regression is expected, and F4 runs on both.
