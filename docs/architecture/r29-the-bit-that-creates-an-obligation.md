# R29 — the bit that answers the question also creates the obligation

Result doc for the round pre-registered in
[`r29-prereg-emptiness-from-our-own-tree.md`](./r29-prereg-emptiness-from-our-own-tree.md).

## What shipped

`wait_quiescent`'s emptiness proof no longer walks every process on the host.
It descends the leader's own subtree instead, pruning at any child that has
left the session. The pre-registration has the measurements; the short version
is 215× cheaper at one Run and 23× at the 128-Run cap, and `stop` improves in
every shape with no other verb degrading.

The interesting part of this round was not the optimisation. It was what the
optimisation dragged in behind it.

## The subreaper bit is load-bearing twice, and I only designed for once

The subtree sweep is only equivalent to the host walk while every session
member is reachable from the daemon. A descendant whose parent exits first
would reparent to init — leaving our subtree while *staying in the session* —
so the sweep would go blind exactly where the census still saw it. Arming
`PR_SET_CHILD_SUBREAPER` closes that: the orphan reparents to us instead.

That much was pre-registered, implemented, and proved by F2's fixture.

What was not pre-registered: **arming the bit also transfers the reaping duty.**
Before it was set, an orphan reparented to init, which reaps instantly and the
session drained on its own. After it was set, nothing in the daemon ever calls
`wait` for that orphan. It stays a zombie — and a zombie still answers
`getsid`, which F3 had already measured and written down.

So the census counted the zombie as a live session member and `stop()` polled
to its deadline before reporting a session that was, in fact, empty.

Measured on cn3, reproducing the failing shape outside cargo:

```
session 3717639 members before stop: [3717639, 3717640, 3717641]

=== after the full stop sequence ===
  pid 3717639  state=gone  getsid=None
  pid 3717640  state=Z     getsid=3717639  ppid_is_us=True
  pid 3717641  state=Z     getsid=3717639  ppid_is_us=True

members() would still return: [3717640, 3717641]
after reaping our own children, members() returns: []
```

The fix is one call, placed in `members()` — the census reaps before it counts,
because the census is the thing whose correctness depends on it. Putting the
reaper anywhere else (it started life on the serve loop's SIGCHLD arm) makes
emptiness depend on another task winning a race.

## The same bit broke a second, unrelated path

`tmux::short_command` spawns a helper in its own process group, SIGKILLs the
group on timeout, and `wait`s the direct child. A grandchild orphaned when the
helper exits used to reparent to init. With the bit armed it reparents to *us*,
and nothing waits for it. `assert_process_gone` uses `kill(pid, 0)`, which
succeeds on a zombie, so the test failed — correctly. In production the same
orphan would be a permanent zombie holding a PID slot for the life of the
daemon.

This is the part worth remembering: **a process-wide flag armed for one
subsystem changed the behaviour of an unrelated one.** Nothing in the
short-command path mentions subreapers, sessions, or Runs. The coupling is the
flag.

## Four wrong hypotheses, and what each one cost

I got the mechanism wrong four times before instrumenting. Recording them
because the *pattern* is the lesson, not the individual errors.

| # | Hypothesis | How it died |
|---|---|---|
| 1 | `setsid --wait X` forks X, so we adopt the wrapper and resolve the wrong session | Traced it: the adopted pid **is** the session leader, and the sweep returned exactly the host walk's answer. Difference empty. |
| 2 | The reaper steals statuses from any non-session-leader child | Narrowed it to same-session only. 9 failures → 5. Right direction, wrong mechanism. |
| 3 | An anchor pointing at the test process degenerates the filter | Added a "must be a real Run session" guard. Failure count did not move at all. |
| 4 | Concurrent tests race the reaper for the same zombie | Ran the suite single-threaded. Still failed. Concurrency was never involved. |

Every one of those was a plausible story I could tell from reading the code.
The thing that actually settled it took two minutes: run the suite
single-threaded (kills #4), then bisect to a **two-test** repro —
`process_census_sees_...` followed by `timeout_kills_same_group_...`. The first
arms the process-wide bit; the second is the victim. Cause and effect, visible
in one command.

The real defect in hypothesis #3 is subtler and worth stating: I assumed a
`cargo test` process would not satisfy `pid == getsid(pid)`. On cn3 a
non-interactive ssh command **is** a session leader, so the guard passed and
changed nothing. I had asserted a property of the environment without checking
it — on the same host I had already run six probes against.

## Why the fix is `waitpgid` blocking, not `NOHANG`

First attempt used `WNOHANG` and did not work. The orphan is reparented
asynchronously, as its own parent exits, so a non-blocking drain returns
`None` before the straggler has arrived and leaves the zombie behind. My
hand-trace had a `sleep(0.3)` in it that hid this exactly.

Blocking is safe here only because every member has already been SIGKILLed,
which no handler can catch and no stop can defer — so each wait resolves as
fast as the kernel delivers it, and `ECHILD` ends the loop. That reasoning is
now a comment at the call site, because the safety is not local to the code.

## Test evidence

Control is the candidate tree with only the three changed files reverted to
`HEAD`, built and run in the same batch on the same host. Same-batch matters:
the pre-existing `r29-base` tree reports the same 48 failures, so the control
is not flattering the candidate.

| arm | passed | failed |
|---|---|---|
| control (HEAD) | 199 | 48 |
| candidate | 244 | 4 |

The candidate's 4 failures are a strict subset of the control's 48 — `comm -23`
on the sorted failure lists is empty, so the round introduces **zero** new
failures and fixes 44.

Those 44 are not a feature of this change so much as a side effect worth
recording: **the host walk panics on kernel threads.** rustix's `getsid`
wraps its return in `Pid::from_raw_unchecked`, whose `debug_assert!(raw > 0)`
fires on `0` — and on cn3, 588 of 917 processes are kernel threads that return
exactly that.

```
getsid == 0 : 588
getsid <  0 : 0
getsid >  0 : 329
```

Every test that walked the host in a debug build was one kernel thread away
from a panic. The subtree sweep never enumerates kernel threads, so the class
of failure disappears. This is a latent defect on `main` that this round
happens to route around; it is **not** fixed, and a debug-build host walk
anywhere else will still hit it.

## Mutation check

Deleting the reap call from `members()` turns both orphan tests red with the
original failure text, and restoring it turns them green. The restored file's
md5 matches the pre-mutation snapshot, so the green run is the shipping source
and not a stale binary.

```
stop must drain the whole session, orphans included:
  "native session 3759474 remained live after graceful and forced Stop phases"
stop must drain the whole session, group escapees included:
  "native session 3759482 remained live after graceful and forced Stop phases"
```

The tests observe the consequence — a session that will not drain — rather than
re-deriving the logic under test, which is what makes the mutation visible.

## What I would do differently

Three of the four wrong hypotheses were reached by reading code and reasoning
forward. All three would have been killed in under five minutes by the cheap
discriminators I eventually ran: single-threaded, then bisect to the smallest
failing pair. Reading is how you form the hypothesis; it is not how you test it.

The generalisable rule from this round, though, is about the flag itself:
**when a change arms a process-wide attribute, its blast radius is the process,
not the subsystem.** The place to look for fallout is every other path that
spawns a child — which is exactly where the second defect was, and exactly
where I was not looking.
