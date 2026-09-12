# New Runs and managed services stay unconfirmed after a persistence fault

This is a source-grounded incident analysis of ctxmux source
`ad69f4715dec3b6f41d22118deb7c0962bed8bc5`, protocol generation 22. The observed
daemon identity matched its local owner receipt and that receipt's artifact
identity matched the installed manifest. The executable mapping had moved with
an older application bundle; a fresh hash of the running image was unavailable.
The observations below are a historical sample, not a claim about current
service health. Local paths, Run identities, operation identities and private
payloads are deliberately omitted.

The incident is **P0**: a shared durable writer rejects new work, has no in-band
recovery path for its latched failure, and existing live output advances beyond
its confirmed durable head. This document records findings and proposed repair
boundaries. It does not claim a deployed fix or a recovered user Runtime.

## Symptoms and confirmed scope

The embedding client reported:

```text
Takeover is not confirmed
Agent start is unconfirmed. Inspect the original operation;
no start or input was resent.
ctxmux durable state rejected a mutation:
ctxmux durable state database failed: unable to open database file
```

A later creation check retained its prepared identity because no single
matching Run was confirmed. A managed-service operation also reported the same
durable-state error and remained unconfirmed under its original operation.
The user clarified that **many**, rather than all, ctxmux-dependent operations
were failing, including the Capsule server and connector servers. The Capsule
failure was traced to a managed service Run. Connector symptoms are user
observations; their exact failing operation and Native dependency must be
established separately rather than inferred from the word "server". ctxmux
owns generic Runs; service business state and Agent-specific recovery remain
the client's responsibility. Where an operation needs durable Run admission,
that is a real dependency, not an observation gate that can simply be bypassed.

The user's suspected trigger was: “感觉磁盘一满 ctxmux 就很容易出问题无法自愈”.
The filesystem was almost full when inspected, with roughly 2 GiB available.
That supports a storage-pressure hypothesis; it does **not** establish the
original syscall, file, SQLite extended result or operating-system error.
`SQLITE_CANTOPEN` alone cannot distinguish full storage, permissions, an invalid
path or another open failure. The original first-failure observation preserved
only its message.

| Observation | What it establishes | What it does not establish |
| --- | --- | --- |
| Public `runtime`, `list` and `storage` returned successfully | Some authoritative read paths remained available | Every mutation, input path or managed service was healthy |
| Storage reported `actor_stopped=false`, zero queued append commands and the same `first_failure` | A live actor retained a persistence failure | Progress, an empty durability backlog or a repaired database |
| List contained 1,514 Runs, including 45 `running` | The daemon still reported live Runs | Every Agent accepted input, or its Provider saved context |
| One live Run: latest 21,433,648; offered 8,015,417; committed 8,014,652; memory floor 0 | 13,418,996 bytes (about 12.8 MiB) had no confirmed durable acknowledgement | All those bytes were already lost, or the same debt existed on every Run |
| Database, WAL and SHM paths existed and the daemon held them open | The main paths had not disappeared at inspection time | The original failing path was the main database, or the connection could commit now |

A protective UI sentence such as “healthy Agents remain available” expresses
the intended preservation boundary. It is not proof of current input/output
availability. No test input, service restart, new Start, SQLite repair or user
Run termination was performed during this investigation.

## Causal chain and amplification

```text
SQLite-reported open failure (syscall/path/errno unknown)
  → error is outside the transient-storage classification
  → first failure becomes a shared, process-lifetime string latch
  ├─ later durable Start/service mutations are refused before database work
  ├─ append/finalize stop advancing confirmed durable state
  │    → live output continues, durability debt grows
  │    → failed persistence no longer protects the unsaved hot-history suffix
  │    → continued pressure can turn recoverable bytes into an explicit Gap
  ├─ structured creation disposition is not carried through the error boundary
  │    → higher clients retain the original prepared operation as unknown
  └─ upgrade preflight refuses unresolved persistence failure/debt
       → the normal live upgrade path cannot clear the old failure
```

The Gap branch is a demonstrated source path, **not** evidence that this sampled
Run had already evicted its outstanding bytes: its observed memory floor was
still zero. Likewise, an unknown COMMIT can leave disk ahead of the last
confirmed watermark; a difference in byte counters is not itself proof of
physical loss. These distinctions matter before any recovery action.

The source mechanisms are:

- [`persistence.rs`](../../crates/ctxmux-daemon/src/persistence.rs):
  `is_transient_storage` recognizes SQLite FULL, selected write-side IOERR,
  filesystem StorageFull and checkpoint busy, but not CANTOPEN.
  `remember_failure` stores the first error in `Mutex<Option<String>>`;
  there is no same-owner reconciliation/clear transition. `prepare_start` and
  `Command::StageStart` reject later Starts without another database attempt.
  `PersistentRun::append` and `finalize` stop durable progression after failure.
- [`native_output.rs`](../../crates/ctxmux-daemon/src/native_output.rs) and
  [`lib.rs`](../../crates/ctxmux-daemon/src/lib.rs): failed persistence changes
  the protected output boundary to `u64::MAX`. Live PTY output continues and
  history can be reclaimed; `record_output_locked` marks the history Gap.
  Healthy live terminal presentation is not evidence of durable replay.
- `lib.rs::prepare_creation_publication` and
  `cleanup_failed_persistent_creation` turn different persistence outcomes into
  ordinary `ProtocolError` messages. The embedding client cannot reconstruct
  the exact disposition from the wording or from a missing Run in a list.
- The planned-upgrade path checks persistence failure/debt before extracting
  live ownership. Refusing this upgrade protects PTYs; bypassing that check
  would not preserve the unconfirmed work.

There are two distinct outcomes at Start. `NotCommitted` describes the durable
transaction. It does not always mean the child never ran: a later stage can
have spawned a process before persistence fails and cleanup occurs. A
pre-spawn rejection can prove no launch; a post-spawn rollback cannot erase
external effects. Recovery must preserve both facts, rather than mapping all
`NotCommitted` errors to permission to launch again.

## Root design failure

The writer already has transaction dispositions, commit witnesses and a
separate capacity-debt path. Its fatal branch nevertheless converges on one
shared process-lifetime message latch. The distinction between storage
availability, original transaction outcome, retained unconfirmed work and
later admission is not carried into a recoverable owner state or complete
public result on that branch. This protects against false durable success,
but has no forward transition for conditions that become safe again.

The consuming client's observation boundary is a separate concern. Its Capsule
`inspect` path can converge an already-recorded desired state and therefore
start a prepared service; the investigation did not invoke it as a read-only
check. The connector hosting path inspected in the client uses a semantic Host
and its own connection store, while creating/restoring Agents and delivering
input can subsequently require Native operations. This supports the user's
report of broad dependency impact, but does not prove every connector-server
failure was caused by this database latch. A safe original-operation query
must declare its effects and return owner facts, rather than turning a check
into another launch. These client findings do not add a second recovery owner
to ctxmux.

The defect is therefore broader than an incomplete error-code allowlist:

1. **Recovery is not a complete owner lifecycle.** Waiting for recognized
   pressure and cold-opening saved state exist; reconciling a failed live
   writer with its original PTYs, debt and operations does not.
2. **Fault scope expands without preserving enough recovery evidence.** A
   storage failure can legitimately affect a shared database, but a global
   string cannot explain which mutation, retained bytes, derived view or
   operation is uncertain. Dropped actor work and unprotected hot output make
   blindly clearing the string unsafe.
3. **Outcome information is weakened at the public boundary.** The Native
   owner knows more than its client can query. The client correctly refuses
   blind replay, but cannot complete the original business operation with the
   weaker information it receives.
4. **Repair depends on the failed facility.** Normal live upgrade requires
   persistence to be settled. A failure that prevents settlement also prevents
   using that upgrade path as an escape. This is a repair dependency that must
   be designed explicitly, not removed by weakening upgrade checks.

The existing [persistence contract](../architecture.md#persistent-state-and-database-capacity)
already requires preserving identities and accepted debt under recoverable
capacity pressure. Tests cover FULL, selected IOERR, rollback, reopening and
history recovery. Those guarantees did not cover this unrecognized open-failure
path through same-owner recovery and the consuming service/creation operation.
This is a gap between the stated recovery principle and its tested fault
families, not evidence that SQLite transaction durability failed.

## How the failure model evolved

The history below comes from `git log -S`, `git log -L` over the classifier and
latch, and source inspection at the named revisions. It describes the coverage
of changes, not the authors' intentions or unrecorded review decisions.

| Source | Change and covered mechanism | Remaining boundary relevant here |
| --- | --- | --- |
| `28dc61a277e49b7ae4e19253f790e8d940d50b4f` | Introduced persistent recovery and `remember_failure` | First failure stayed latched without a same-owner recovery transition |
| `220da466642a9fd2d356460f3d7075cfcccb0f06` | Mainline merge brought in typed DiskFull and its retry path | This is an integration point, not attribution of the original implementation |
| `223f6e0e788fa4ab84f769076f4509b9e529a171` | Unified FULL and checkpoint-pressure handling under `is_transient_storage` | Other open failures remained outside the classifier |
| `8a0c0597115012f9ce69dfb1f684444517370889` | Retained extended IOERR codes and added selected write-side retries | Bare/other I/O errors still latched; no captured system errno or live reconciliation |
| `a5c0afba90daf4a1b229b6e48e6c56015e9db089` | Externalized replay bodies and recognized filesystem StorageFull | Body/index ordering and recovery remained the same owner's obligation |
| `d2bc03c1773d58b83da3701e269fdb0f590491dc` | Preserved the original FULL after SQLite had already rolled back | Corrected one transaction-classification error, not all I/O recovery |
| `78cee50bd1dbfd0ef34b362b08a99165050e17b6` | Reclaimed history under a fixed SQLite page capacity while retaining accepted output | External-fault classification and online reconciliation remained separate |
| `f1345ff428fadb581f8271e2a3456fec390d5d87` | Derived storage funding from actual filesystem resources | Observed free space could still be consumed by another writer |
| `ad69f4715dec3b6f41d22118deb7c0962bed8bc5` | Bounded retention/accounting work, yielded to lifecycle work and retained capacity debt | The first-failure latch and missing live recovery path remained |

These changes repaired genuine capacity and ordering defects. The recurring
gap is that improvements stayed inside recognized error/retry paths, while an
unrecognized but potentially removable condition still crossed a permanent
failure boundary. The durable lesson is to specify recovery ownership and
outcomes for the fault family, then challenge the boundary with an unrecognized
or differently surfaced failure. Extending a code allowlist alone does not do
that.

## Storage selection and architectural alternatives

The current design already separates replay bodies from SQLite. The database
owns Runtime/Run identities, creation keys, immutable specifications, lifecycle
and replay coordinates. Replay files contain bodies, not all the facts needed
to reconstruct that database. Payload sync precedes SQL commit; durable heads
advance only after commit confirmation. Keep those established ordering proofs.

| Direction | Benefit | Cost and decision |
| --- | --- | --- |
| Keep SQLite and external replay; repair the persistence owner's recovery contract | Reuses transactions, identity constraints, exact commit proofs and the current saved format | Recommended direction for this fault family; still requires debt preservation, typed outcomes and same-owner recovery |
| Make a self-describing segmented journal authoritative, with rebuildable indexes | Can make index reconstruction and whole-segment reclamation explicit | A genuine alternative only after measuring the need; requires complete identity/lifecycle/operation records, commit markers, checksums, publication, GC and crash proofs. Existing replay cannot simply be renamed into this role |
| Replace the engine with RocksDB or another transactional store | May offer useful error classification, recovery primitives or a better measured workload fit | Does not solve full storage, PTY ownership, cross-file ordering or unknown process effects; requires re-proving constraints, resources and recovery. No evidence here justifies the replacement |

SQLite exposes both [extended result codes](https://www.sqlite.org/rescode.html)
and an attempt to obtain the [underlying system error](https://www.sqlite.org/c3ref/system_errno.html).
Capture those at the failing connection before another action overwrites the
evidence. Its [autocommit observation](https://www.sqlite.org/c3ref/get_autocommit.html)
can identify an automatically ended transaction, but an ended transaction after
an attempted COMMIT is not proof of either commit or rollback. Preserve the
existing exact disposition proof.

[RocksDB's recovery contract](https://github.com/facebook/rocksdb/wiki/Background-Error-Handling)
is useful design evidence: it distinguishes error severities and supports
qualified recovery instead of simply ignoring an error. It does not make
all writes independent; a [column-family write stall](https://github.com/facebook/rocksdb/wiki/Write-Stalls)
can affect the whole database. Separating databases or processes on the same
full filesystem likewise does not create physical capacity. These are
comparisons of contracts, not benchmarks or qualification of another engine.

Change the storage authority only if, under the same workload, durability and
resource envelope, the repaired owner still cannot provide the required
recovery, reclamation or isolation and an alternative demonstrably can. Normal
throughput alone is insufficient evidence. New shims, per-Run databases or a
second repair daemon are not prerequisites for the present correction.

## Containment and recovery boundaries

At this source there is no demonstrated in-band way to recover the already
latched live writer. Freeing storage removes one possible external condition;
it does not clear the latch or recover discarded work. Do not delete/truncate
the DB/WAL, start an empty namespace, resend unknown Start/Input, or use SIGHUP
as a repair experiment. A cold restart reopens and validates the original
persisted store; valid committed data can include a COMMIT the previous owner
never acknowledged. It cannot guarantee the unsaved live tail, marks old live
Runs interrupted and does not preserve their PTYs.

Recovering this old incident and preventing the next incident are separate
deliveries. First preserve reachable output and exact identities using the
original public boundaries, determine the available evidence and permitted
loss/continuity boundary, and only then select a concrete recovery action.
This case does not establish a safe live upgrade for the already-failed daemon.
Provider context recovery, Capsule/connector business state and user workspace
restoration remain their respective owners' responsibilities.

The normative design requirements are in
[Recoverability and antifragility](../architecture.md#recoverability-and-antifragility).
They require a real recovery transition, scoped faults, conserved debt, useful
public outcomes and an explicitly funded loss/backpressure policy. They do not
promise unlimited persistence on a full disk.

## Required proof and remaining gaps

The following is the acceptance direction for an implementation, **not executed
validation of a fix**. Extend the existing owner fault seams and
[`persistence_recovery.rs`](../../crates/ctxmux-daemon/tests/persistence_recovery.rs),
then exercise the same public CLI/SDK paths used by consuming clients.

| Fault or transition | Required observable result |
| --- | --- |
| Temporary open/write/sync failure, with resources later restored | Same owner, Runtime/Run identity and live PID; original debt settles; a second Run can be created without restarting the first |
| CANTOPEN caused by an invalid path or wrong ownership | Exact refusal and retained original state; no empty replacement database or blind retry storm |
| Failure before spawn versus after spawn; lost reply | Public original-operation query preserves durable result, actual launch/cleanup and uncertainty separately; no duplicate process launch or input |
| COMMIT response uncertainty; recovery fails again | Original data and witness remain; no optimistic success, blind rollback-tail truncation or repeated external effect |
| Prolonged shortage with quiet and noisy Runs | Bounded debt/memory, fair service, explicit pressure or Gap, accurate durable heads; local debt does not require draining unrelated Runs first |
| Same configured capacity during reclamation and cold restart | Original saved data is used, with no larger quota or reseeded fixture; recovery and necessary settlement can make progress within their funded work space |
| Repeated exhaustion/release cycles plus held-out faults | Repeated recovery converges without leaked grants, growing queues or forgotten lifecycle work |
| Existing failed writer plus proposed upgrade | Either a proven full transfer of unresolved work and live ownership, or truthful refusal; merely removing preflight must fail the test |
| Agent creation and at least two managed-service consumers | Same Native receipts reach public client/CLI and actual consumer; failed observation does not invent a new operation or bypass necessary durability |

Use bounded isolated storage and controlled I/O injection; never fill the user's
host disk. [SQLite's fault-testing approach](https://www.sqlite.org/testing.html)
is a useful model for one-shot, persistent and recovery-time faults, but ctxmux
must additionally prove its external replay, process and public-operation
boundaries. A meaningful counterfactual must break recovery/debt/receipt wiring
and make the corresponding behavior test fail.

Remaining gaps: the original syscall/errno and precise failed transaction are
unknown; the user-reported service failures were not converted into test writes;
the amount of unconfirmed output across all Runs is unmeasured; the complete
new recovery path, protocol and safe old-instance repair have not been
implemented or qualified. Independent source review and documentation checks
do not close these implementation gaps.
