# R29 pre-registration — answer emptiness from our own tree, not the host's

**Target:** the confirm census in `wait_quiescent`, the 1.0 ms that R28 left
behind. `stop` is 3.203 ms of daemon-side work; this is a third of it.

Written before any product edit, so the falsifiers can fire against a claim
that is already on the record.

## The pre-registered direction is closed by the host, not by the design

R28 pre-registered cgroup v2 for this round: `cgroup.events:populated` gives
emptiness as an _event_, and `cgroup.kill` would also catch `setsid` escapees.
The availability gate was one shell line. It ran first, and the physics came
back better than hoped:

| measured on cn3 (kernel 5.15.120, 886–924 processes) |                                                                     |
| ---------------------------------------------------- | ------------------------------------------------------------------- |
| `/sys/fs/cgroup` type                                | `cgroup2fs`, `nsdelegate`                                           |
| `cgroup.kill` present                                | yes (≥ 5.14)                                                        |
| `pread(cgroup.events)`, fd kept open                 | **0.0005 ms**                                                       |
| the `/proc` census it would replace                  | **1.047 ms** — 2237×                                                |
| does an unreaped zombie hold `populated` at 1?       | **no** — it drops to 0 while the leader is still an unreaped zombie |
| `mkdir` + `write(cgroup.procs)` on the start path    | 0.0546 ms, under start's 0.081 ms A/A floor                         |

That zombie answer is the one that mattered: our design deliberately keeps the
leader an unreaped zombie through the signalling window so its pgid cannot be
recycled, and `populated` ignores zombies, so the mechanism is compatible.

**It dies on deployment.** The daemon is autospawned by the CLI
(`crates/ctxmux/src/daemon.rs:70`), so it inherits the CLI's cgroup. Under ssh
that is a root-owned `session-N.scope`:

```
my cgroup: user.slice/user-<uid>.slice/session-<id>.scope
mkdir in OWN scope                          : EACCES
mkdir under app.slice                       : OK        (we own that directory)
MOVE a session-scope process into app.slice : EACCES
```

cgroup v2 migration requires write permission on the **common ancestor** of
source and destination. That ancestor is `user-<uid>.slice`, owned by root. The
daemon can create a delegated directory and still be unable to put anything
into it — including itself. Working around this means the daemon must be
_started_ inside a delegated scope (`systemd-run --user --scope -p
Delegate=yes`, or a unit file), which is a deployment contract ctxmux does not
have and cannot assume: no systemd user manager, no lingering, no dbus, and the
route is simply unavailable.

A fallback would mean carrying both mechanisms forever, and the `/proc` path
would still have to be correct and tested. Not worth it for a mechanism we
cannot rely on. **Direction closed — on the host, not on the idea.**

The numbers above are kept because they are the answer to "is cgroup v2 worth
revisiting": yes, immediately, if ctxmux ever ships as a systemd user service.

## What replaces it

The census asks "does any process on this host belong to the leader's session?"
and answers it by walking all ~900 processes. But **every member of that session
is a descendant of the leader** — a session is inherited across `fork`, and only
`setsid` leaves it, which by definition removes the process from the session we
are proving empty. So the question is answerable from our own subtree.

`PR_SET_CHILD_SUBREAPER` makes the daemon the reparenting target for every
orphan in its tree, so descendants cannot escape to `init` and out of view.
`/proc/<pid>/task/<tid>/children` then enumerates them directly.

Measured on cn3, same host and same conditions as the census:

|                               | 0 children | 8 children |
| ----------------------------- | ---------- | ---------- |
| host `/proc` census (shipped) | 1.129 ms   | 1.130 ms   |
| one `children` read           | 0.0026     | 0.0041     |
| direct children, all threads  | 0.0052     | 0.0068     |
| **full descendant sweep**     | **0.0052** | **0.0496** |
| ratio                         | **215×**   | **23×**    |

The sweep is proportional to _our_ tree, not the host's. That is the whole
point: today's cost is set by how busy the machine is
([[ctxmux-collinear-variable-defeats-attribution]] — one Run beside 3000
unrelated processes measured 352.7 ms).

For completeness, the lazier option was priced and rejected: replacing
`std::fs::read_dir` with a raw `getdents64` and a 256 KiB buffer saves
**0.090 ms of 1.056** (8.7%). The cost is the kernel materialising 886 dentries,
not Rust's allocations. Not a lever.

## Falsifiers, in order. Any one fires ⇒ the round stops and this doc records why.

**F1 — the mechanism must engage (counts before timings).**
`strace -c` a real `stop`. `openat("/proc")` for the _host walk_ must go 1 → 0.
The `/proc/<pid>/task/.../children` reads must appear. If the walk is still
there, no timing is looked at. This is the check that caught R28's no-op patch
([[ctxmux-a-short-circuit-can-be-load-bearing]]).

**F2 — the guarantee must not narrow. Measured with a fixture, before product code.**
Today's confirm sees any process in the session, however it got there. The
subtree sweep sees any process descended from the leader. The two differ
exactly on a process that is _in the session but not in our tree_.

Fixture run on cn3 before writing any product code. A session leader forks one
of each escape: a plain descendant, one that calls `setpgid`, one that calls
`setsid`, and one whose parent exits immediately (an orphan).

```
pid       state   sid       pgid      ppid    note
2845622   S       2845622   2845622   2845621
2845623   S       2845622   2845622   2845622
2845624   S       2845622   2845624   2845622   left-group
2845625   S       2845625   2845625   2845622   left-session
2845627   S       2845622   2845622   2845622
2845628   S       2845622   2845622   2845621   <- the orphan

census (host walk, session filter)  : [622, 623, 624, 627, 628]
sweep  (our subtree, session filter): [622, 623, 624, 627, 628]
seen by census but NOT by sweep : []
seen by sweep but NOT by census : []
```

**Identical coverage.** The `setsid` child (2845625) is invisible to both, which
is the pre-existing boundary of "whole-session" and is unchanged. The orphan
(2845628) is the case that proves the `prctl` is load-bearing: its parent
exited, so without `PR_SET_CHILD_SUBREAPER` it would have reparented to init
and left our subtree while remaining in the session — the sweep would have gone
blind exactly where the census still saw it.

The remaining row is the one no fixture can enumerate: _a process that joined
the session without descending from the leader_. It cannot exist —
`setsid(2)` only ever creates a **new** session with the caller as leader, and
there is no call that moves a process into an existing session. The guarantee
therefore rests on that kernel invariant, stated here so it can be challenged.

**F3 — zombies must not deadlock the proof.** Measured already and it is the
sharp edge: an unreaped zombie **still reports its session** (`getsid` on a
state-`Z` process succeeds). As a subreaper the daemon inherits orphans, and any
it fails to reap become zombies inside its own tree — which the sweep would
count as alive, so `stop` would spin to its deadline and return "not quiescent"
for a session that is genuinely empty. The candidate must reap what it inherits,
and the fixture must show an orphaned grandchild being reaped rather than
accumulating. A `stop` that hangs where the old code returned is a degradation
even if every timing improves.

**F4 — subreaper is process-wide, the proof is per-Run.** `PR_SET_CHILD_SUBREAPER`
is a property of the daemon, not of a Run. Inherited orphans from Run A appear
in the same tree as Run B's. The sweep must therefore still filter by
`getsid(pid) == leader_pid` — the `getsid` half (0.172 ms) stays; only the
enumeration half changes. Any design that drops the session filter is wrong.

**F4b — the cold-recovered Run is not a counterexample.** `arm_native_exit_relay`
warns that cold-recovered Runs are "children of the dead previous process,
reparented to init" — i.e. outside this daemon's subtree, where the sweep would
be blind. Checked in the code rather than assumed: `Run::recover`
(`lib.rs:3716`) sets `incarnation_control: None`, and `native_control()`
(`lib.rs:4188`) rejects such a Run with `InvalidRunState` — "cannot control
historical Run". A Run with no native control never reaches `stop`'s
`wait_quiescent`, so the census only ever runs for sessions this daemon
spawned, which are exactly its descendants. `readopt` (the exec-in-place path)
_does_ keep native control, and those children are still ours across `execve` —
same pid, same parent. If either fact changes, this falsifier fires.

**F5 — the ratchet gate.** cn3, ≥ 20 paired cells, arm order alternated every
pair, sign test against an empirical A/A floor measured the same way, in three
shapes (c=0 memory-only, c=8 chatty, c=0 persistence-on). Every non-`stop` verb
must sit inside its own A/A floor. Any degradation anywhere ⇒ roll back and
this doc becomes the post-mortem.

Prediction, written before the data: `stop` improves by **0.9–1.1 ms**
(1.129 census → ~0.005 sweep, minus the retained `getsid` filter over a much
smaller list). `start`, `remove`, `list` move by less than their floors.
Recording it now so the mechanism can be wrong in a way I can see
([[ctxmux-a-mechanism-that-fits-two-shapes-can-die-on-the-third]]).

## What this costs if it ships

One `prctl` at daemon startup and a reap path for inherited orphans. No
delegation, no spawn-path change, no new dependency, no wire-contract change,
and `portable-pty` is untouched. The daemon already runs a process-wide SIGCHLD
relay (`lib.rs:506`), which is the natural place for the reaping F3 demands.
