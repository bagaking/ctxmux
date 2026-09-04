# R28 — one syscall replaces a host walk

**Status: SHIP.** `stop` is 1.079 ms faster on cn3 (4.287 → 3.203 ms, −25%),
no other metric moved, and the guarantee is held by a test that a mutation
proves can fail.

R27 established the map: teardown's addressable cost is `stop`'s 3.046 ms of
daemon-side work, of which 2.018 ms is two `/proc` censuses at 1.009 ms each
(868 processes). `remove` is 0.074 ms — finished. This round removes one of the
two censuses.

## Which census was removable

`strace -tt` on one real stop, before any edit:

```
.416443  openat("/proc")            <- census #1, signal_members enumerating
.439976  kill(<leader>, SIGTERM)    <- the signal, AFTER the walk
.440103  clock_nanosleep(200us)     <- QUIESCENCE_FIRST_POLL
.440390  waitid(...CLD_KILLED...)   <- leader already dead
.440425  openat("/proc")            <- census #2, the confirm
.461000  waitid / wait4             <- reap: the confirm SUCCEEDED
```

Census #2 is the proof of session-emptiness and it *wins* — the reap follows it
directly. It is what makes a returned Stop mean "nothing of this Run is left",
and it stays.

Census #1 existed only to build the list of PIDs to signal. In the common case
that list is one entry: the leader. We walked 868 processes to discover that.

`portable-pty` makes the child a session leader before exec, so the leader's PID
is also the initial process-group ID. `kill(-leader, sig)` reaches the whole
group in one syscall, with no enumeration.

## Result

cn3, 20 paired cells per arm, arm order alternated every pair, sign test against
an empirical A/A floor measured the same way:

| verb | base ms | cand ms | delta | A/A floor | sign p | verdict |
|---|---|---|---|---|---|---|
| **stop** | 4.287 | **3.203** | **+1.079** | 0.078 | <0.0001 | **faster** |
| start | 2.186 | 2.182 | +0.007 | 0.081 | 0.82 | inside floor |
| remove | 1.356 | 1.352 | +0.004 | 0.034 | 0.26 | inside floor |
| list | 1.274 | 1.286 | +0.002 | 0.033 | 0.82 | inside floor |

20/20 pairs moved the same direction, and the effect is 14× the noise floor.
Predicted 1.009 ms from the standalone census replica; measured 1.079 ms.

The ratchet gate, two further shapes, same paired discipline (14 pairs):

| shape | stop | start | remove | list |
|---|---|---|---|---|
| c=8 chatty, memory-only | **+0.582** (p=0.0001) | −0.149 (p=0.42, ns) | −0.006 | −0.019 |
| c=0, persistence ON | **+0.794** (p=0.0001) | +0.066 | +0.025 | +0.010 |

Everything not `stop` sits inside its A/A floor or fails significance. The
`start` −0.149 ms at c=8 is not significant and reverses sign at c=0 — noise,
not a mechanism. **No metric degraded in any shape.**

## The guarantee, and the test that holds it

`killpg` signals the *group*; the old code signalled the *session*. Those differ
by exactly one class: a descendant that called `setpgid` (left the group) but
not `setsid` (stayed in the session).

Measured before writing any product code, with a C fixture (F2):

| strategy | plain child | setpgid child | setsid child |
|---|---|---|---|
| session census (old) | dead | **dead** | ALIVE |
| killpg alone (naive) | dead | **ALIVE** | ALIVE |
| killpg + sweep (shipped) | dead | **dead** | ALIVE |

The middle row is the regression, and it had to be *observed* before the fix was
worth writing. The `setsid` child survives all three — it left the session and
was never owned by either design, which bounds what "whole-session" has ever
meant here.

The fix: `wait_quiescent` already enumerates session members to prove emptiness,
so anything it still sees has outlived a group-wide signal and is by definition
a group escapee. Signalling those specific PIDs through the existing
`getsid`-revalidate-then-`kill` path costs no extra walk. Common case: one
`killpg` + one confirm census. Escapee case: + a targeted pass. In no case is
"empty" reported over a live session — the confirm is untouched.

`stopping_reaches_a_descendant_that_left_the_process_group` is the permanent
guard. Mutation-checked, because a test that cannot fail proves nothing:

- sweep present → passes
- sweep deleted from `wait_quiescent` → **FAILED**, at the `stop()` call

## Why pgid reuse needs no new guard

`leader_is_terminal` observes the leader with `WaitIdOptions::NOWAIT` and
`reap_leader` runs only at the very end, so the leader stays an unreaped zombie
in its own process group for the entire signalling window. A process-group ID
cannot be recycled while any member — zombie included — remains. This is the
invariant `require_waitable_anchor` already enforces; `signal_members` calls it
before signalling, as the census path did.

## Two things this round nearly got wrong

**The first patch was a no-op that looked like a win.** F1 — count `/proc` walks
under strace before believing any timing — caught it: censuses stayed at 2 even
though `kill(-pid, SIGTERM)` was firing. The original condition was
`leader_is_terminal()? && self.members(false)?.is_empty()`, and the `&&`
short-circuit is load-bearing: while the leader is alive, no census is taken at
all. Hoisting `members()` out to reuse its list added back exactly the walk I
had removed. Net saving would have been zero, and the paired timing would have
reported "flat" for a knob that never engaged.

**The first fixture could not produce the escapee it tested for.** It used
`sh -c 'set -m; sleep 600 &'`, expecting job control to put the background job
in its own group. A session with no controlling terminal turns job control off
("can't access tty"), so every process stayed in the leader's group. The test's
own guard caught this and failed rather than passing vacuously — the escapee
must now call `setpgid` itself.

Both were caught by pre-registered checks rather than by inspection, which is
the whole point of writing them down before the edit.

## Still open

`stop` is now 3.203 ms, of which ~1.0 ms is the remaining confirm census. That
one is irreducible under `/proc`: proving a session empty requires observing its
members gone. Removing it needs a different mechanism — cgroup v2's
`cgroup.events:populated` gives emptiness as an *event* with no walk, and
`cgroup.kill` would also catch `setsid` escapees, strengthening the guarantee.
It is gated on a delegated cgroup subtree and a spawn-path change
`portable-pty` does not expose. One shell line decides whether it is even
available on the farm host before R29 designs anything:

```
stat -fc %T /sys/fs/cgroup                       # must be cgroup2fs
mkdir <delegated-root>/probe && echo $$ > <delegated-root>/probe/cgroup.procs
```
