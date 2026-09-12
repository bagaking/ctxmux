# Runtime policy ownership refactor

This implementation follows the source review in
[runtime-defense-and-configuration-audit.md](runtime-defense-and-configuration-audit.md).
The resource Feature `f-22tcz9d9r/T-014`–`T-016` owns source-bound command logs,
executed artifact identities and task gates. This review describes the local
owner contract; it does not qualify whole-daemon memory, fleet scale or soak.

## Separate startup choices from established facts

`ResourceConfig` parses sparse startup choices once. Omission selects a default
or an owner-derived allowance; explicit JSON null remains distinct from omission.
A shared field catalogue generates both startup choices and the immutable
`ResourceLimits` snapshot, preventing type/name drift without duplicating JSON
parsing. Unknown names, duplicates and invalid types fail before owner creation.

The snapshot requires every field, including nullable quotas. Exec restoration
cannot silently re-evaluate a missing established setting on the incoming host.
The private manifest changes from v8 to v9 because two fields are removed;
public protocol generation 22 is unchanged. Target schema mismatch refuses
before extraction. There is no old-field alias or migration path.

## Derive observation funding and retain its real owner

Omitted observation memory follows the selected worker width at one maximum
protocol frame per job. The checked relation applies to both host defaults and
explicit worker choices. Explicit memory remains exact; the owner divides it
among slots, bounds each result to one frame, and can report budget-exhausted
unknown. Timeout or caller departure does not cancel an already issued OS job
or release its physical work/memory permits early. Input/Stop keep their own
owners and progress.

## Carry control state under the original admission owner

`handoff_input_bytes` and `handoff_diagnostic_bytes` imposed additional transfer
quotas on data already admitted by `control_state_bytes`. They are removed from
Rust, SDK and current launch policies. Restoration still validates complete
Input byte ranges, duplicate keys, unknown-prefix fences, per-Run result
retention, Stop disposition and aggregate actual control charges. Full error
messages remain outcome facts rather than a 4 KiB reserve or 16 MiB transfer cap.

The encoded `handoff_bytes` allowance remains an independent staging/read budget.
Its bounded writer completes preflight before owners are extracted, and its
reader uses an explicit established allowance. A smaller configured allowance
can refuse exec; it never discards receipts or replaces a Run to make it fit.
Further derivation of encoding/terminal allocations remains separate owner work.

## Local proof and its limits

Checks use a dedicated Cargo target directory, real compiler invocations and
executed artifact identities. Failed checks are retained separately. The initial
real-process test and CLI smoke exposed old capability oracles that omitted the
already implemented storage-observation capability; both now require that
capability rather than ignoring unexpected keys. The SDK disk-default
fixture also requires a filesystem large enough to prove allowances above the
historical 256 MiB cap. A low-space host run failed that environmental premise;
its log is preserved. Validation uses an isolated APFS test image backed by a
volume with sufficient space, with the original assertion and workload intact.
The installed low-space Runtime remains a separate truthful pressure sample,
not a passing large-filesystem qualification.

- Rust resource tests remove each snapshot field in turn, distinguish omission
  from explicit null, reject duplicates/obsolete fields, derive worker funding,
  preserve explicit allowances and reject overflow.
- Observation owner tests fund actual slots/results and prove real two-Run
  service while timed-out or disconnected observation work retains its permits.
- Handoff tests preserve complete multi-Run Input/Stop state at exact control
  funding, refuse one byte below funding, and preserve a diagnostic larger than
  the former 16 MiB transfer quota. A held-out 129-Run, one-MiB-per-Run
  admission fixture crosses the former 128 MiB Input transfer quota under exact
  control funding. These are regression cases, not production capacities.
  Encoded pressure refuses without shedding.
- A persistent public-client fixture loses an Input response, disconnects,
  performs real exec, retries the same operation and checks the original child
  PID, Runtime identity, policy and exact captured child bytes.
- A second real-process fixture forces encoded-file pressure before extraction;
  two original children retain their PIDs, exact retry receipts, existing
  attachments and fresh child output after the refusal.
- SDK activation rejects obsolete quotas before effects, applies startup policy
  through the public boundary, and checks nullable disk policy and durable
  replay after cold restart. Formatting, Clippy and TypeScript checks accompany
  the changed owners. The CLI smoke uses the selected built executable.

A read-only sample of the installed user Runtime contained 212 retained Runs,
18 running, about 455 MiB of hot history and 2.9 GiB of lifetime output. Its
EPIPE-failed diagnostic sink coexisted with responsive public queries and
non-stopped sampled persistence owners. This supports retaining independent
sink fault containment; it is neither an incident-cause diagnosis nor a capacity
or sustained-progress qualification. No existing user Run/state is changed.

The current qualification policy removes only the two transfer-quota fields;
all remaining explicit values, workload sizes, time budgets and byte oracles
stay unchanged. Foreground observation settings remain host-derived as before;
the TypeScript policy type now describes sparse startup choices correctly.
The previous independent `aad59bfd…` policy pin was already out of sync with
the selected pre-refactor producer at `a623f11` (`dce194e5…`). A source comparison
proves that only the two transfer fields are removed; an independent reconstruction
test pins all remaining values to that actual producer. The new policy hash
(`7ec1d882…`) is changed deliberately. The stale pin is preserved as evidence,
not asserted to describe a qualified current envelope. Historical fixtures,
receipts and baselines are preserved and are not requalified by this change.
New scale/soak observations, funded terminal/parser allocation and further
advanced-interface reduction remain open in their existing Feature owners.
Ordinary use requires no resource tuning and has two optional history choices,
as emphasized in [README](../../README.md#configuration-choose-persistence-then-use-the-defaults).
