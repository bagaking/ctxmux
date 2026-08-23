# Architecture writing route

- Target reader: a maintainer or embedding-client author who needs to change ctxmux without violating Run ownership or overstating maturity.
- Expected action: locate the owning component and decision record, follow the real lifecycle path, and identify the fixture that protects the relevant invariant.
- Scope: current runtime architecture, target boundaries, core scenarios, decision inventory, and risk-to-fixture traceability.
- Out of scope: implementation tutorials, full API reference, Agent orchestration policy, and claims about unimplemented capabilities.
- Success signal: a reader can distinguish current from target behavior and trace a claim from architecture to code, decision, wrong case, and fixture disposition.

## Route memo

- title_promise: ctxmux makes Runs durable by keeping ownership in one daemon and making every client a replaceable view.
- first_question: what exists today, and which process owns it after a client disappears?
- evidence_movement: current guarantees and code paths first; target extension boundaries second; external failures and fixtures in linked records.
- chapter_movement: deployment map -> Run ownership -> lifecycle paths -> failure semantics -> extension axes -> decisions -> fixture traceability.
- exit_move: the reader knows where to change behavior and what evidence must change with it.

## Writing route

- Scene: `S2_synthesis` over code, protocol, tests, and product documents.
- Chosen angle: `claim-define-boundary-mechanism`.
- Runner-up: `difficulty-map`.
- Why chosen: the primary risk is confusing current and target ownership boundaries; the difficulty inventory belongs in decision records.
- Evidence shape: repository symbols and public-behavior tests support current claims; roadmap and status-bearing decisions identify target behavior.
