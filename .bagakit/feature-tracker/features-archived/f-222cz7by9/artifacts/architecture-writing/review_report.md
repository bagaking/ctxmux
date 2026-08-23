# Architecture documentation review

## Delivery mapping

- Publication narrative: `docs/architecture.md`
- Execution appendix: `docs/architecture/choices/`
- Evidence handoff: `.bagakit/researcher/` and `docs/architecture/casebook.md` during T-009
- Memory handoff: this report

The fixed `article.md` envelope is intentionally mapped to repository-native architecture paths. Duplicating the same architecture text under an additional article filename would create a second source of truth.

## Budget and baseline

- Profile: infrastructure reference
- Target: one architecture entrypoint, twelve decision records, at least five concrete lifecycle paths, one component map, and one decision index
- Keep: daemon ownership, Agent-neutral Run, Backend/Integration separation, fail-closed fork fidelity, public tmux boundary
- Add: current-versus-target status, real code paths, concurrency and failure semantics, decision status, wrong-case and fixture hooks
- Tighten: bounded final drain, asynchronous stop, daemon-lifetime durability, TypeScript runtime validation, and `u64` mapping must be described without stronger guarantees than the implementation proves

## Release gate

- No unresolved capability claim may be presented as current behavior.
- Every decision record must name a status and repository evidence.
- The wrong-case sections may state that external research has not yet been accepted during T-008; T-009 must replace that state with cited cases before the corpus gate can pass.
- The architecture entrypoint must link every decision record and keep implementation detail out of the product invariants.

## Current review status

- Status: approve for T-010
- Objective evidence: `docs/architecture.md`; twelve status-bearing records under `docs/architecture/choices/`; 38 preserved sources across all twelve choices; 35 cases in `fixtures/wrong-cases.json`; and the repository gate
- External case coverage is complete for this bounded pass. Capability-dependent cases remain explicitly inactive until their activation owners exist.

## Warning review

- De-AI-tone lint reported no failures. Its dash-overuse warning on the overview counts Markdown tables and ASCII diagrams; review found no repeated em-dash rhetoric, so the warning is accepted as a structural false positive.
- List-density warnings are accepted for inventories, ordered lifecycle paths, invariants, fixture candidates, and open questions. Causal claims remain in prose and tables rather than being replaced by slogan bullets.
- The generic H2-count, paragraph-shape, and flat-outline checks target articles. The entrypoint and decision records are navigable references with stable, repeated sections, so compressing them to the article profile would hide decision paths and fixture ownership.
- The `harness` warning in the fork record is an accepted technical term: that sentence names the orchestration boundary the project intentionally does not own.
- Parentheses were removed from the repeated wrong-case section heading after the final hard-gate pass. No unresolved placeholder or unimplemented capability claim is presented as shipped behavior.
- The final fixture pass added a fail-closed coalesced-frame regression: after malformed JSON, neither queued nor concurrently awaited later frames can escape the terminal connection error.
- Layer review found three fixture claims whose anchors proved only adjacent mechanisms. `LC-001`, `OR-002`, and `SC-03` are now future with exact activation blockers; the corpus closes with 10 active, 20 future, 2 covered, 2 characterization, and 1 rejected case.
- Rust and Node now consume one raw malformed-frame corpus. Rust rejects duplicate members inside typed fields, maps, and ignored nested objects before typed decode, and the real daemon proves that none of those frames mutates Run state.
- `SDK-01` now states the generation-1 acknowledgement boundary exactly: short requests and clean detach wait for server frames, while attached mutation promises represent socket-write completion and expose remote results through events.

## Agent gate

- Execution clarity: 9/10
- Trigger precision: 9/10
- Standalone integrity: 9/10
- Information architecture: 9/10
- Evidence package density: 8/10
- Publish suitability: 8/10
- First-draft readiness: 9/10
- Memorability: 8/10
- Decision: approve; no open P1 finding
