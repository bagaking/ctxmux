# Feature Proposal: f-229cz9szm

## Why

- A Run may need an image, patch, fixture, or other bounded byte object that a client currently holds in memory. PTY input can carry a reference but cannot materialize those bytes on the Run host.
- If every embedding client creates temporary files independently, path safety, atomicity, quotas, retry disposition, lifetime, and cleanup drift into multiple insecure implementations.
- An authorized external client or embedding host is the consumer. Its source selection, consent, preview, application semantics, and transport policy remain above ctxmux; ctxmux exposes only a generic Run-host artifact capability.

## Goal

- Let an authenticated client explicitly stage bounded opaque bytes beside one live native Run and receive a Run-host reference, while ctxmux owns capability enforcement, integrity, resource accounting, atomic publication, retry disposition, lifetime, and cleanup without acquiring clipboard, image, Agent, SSH-deployment, or conversation semantics.

## Principle Layer

- What: a client-authorized, capability-gated artifact ingress scoped to the daemon and live Run that will consume the bytes.
- Why: context bytes need a trustworthy bridge into the Run host, but the mux must not become the authority for why the bytes were selected or how an Agent should use them.
- Intended generalization: clipboard images, dropped files, generated patches, screenshots, fixtures, and other bounded opaque inputs for native Runs; another Backend may support placement only after it proves the same contract.
- Failure boundary: ctxmux never reads clipboard state, decides consent, interprets media, builds prompts, deploys over SSH, moves artifacts between hosts or Runs, or claims isolation from hostile same-UID processes.
- Behavior examples:
  - an embedding host obtains caller authorization and sends bounded bytes through `@ctxmux/sdk` to the ctxmuxd that already owns the target Run;
  - ctxmux validates the live Run and capability before upload, atomically publishes into a daemon-private area, and returns `ArtifactId + RunHostPath`;
  - the embedding host separately sends that path through correlated Run input and later releases the artifact;
  - a terminal-state Run, imported tmux Run, incompatible daemon, or unsupported Backend rejects before consuming bytes.
- Evidence refs:
  - `docs/vision.md`
  - `docs/architecture.md`
  - `docs/architecture/choices/011-context-artifact-lineage-fork.md`
  - `.bagakit/feature-tracker/features/f-226cz5zdq/artifacts/herdr-transfer-review.md`

## Scope

- In scope: protocol capability and typed outcomes; native-Run-local staging; bounded streaming; operation-key retry disposition; owner-only storage; exact digest; atomic commit; leases, release, TTL/quota eviction, Run-terminal cleanup and startup cleanup; Rust/TypeScript/CLI public surfaces.
- Out of scope: OS clipboard access, consent UI, image decoding or preview, Agent prompt construction, SSH deployment or remote filesystem APIs, workspace sync, cross-Run transfer, imported tmux write support, marketplace, and cross-user sandboxing.

## Acceptance Criteria

- The target is one exact live `RunId` on the receiving daemon. ctxmux chooses the physical path; the client cannot provide a destination, reuse the artifact for another Run, or stage through an unsupported Backend.
- Upload uses bounded frames, declared length and digest, an opaque operation key, and a queryable committed/failed/unknown disposition. Same key plus identical Run and content converges; conflicting reuse fails without mutation.
- Success is returned only after exact-byte validation and atomic publication. Pre-commit failure publishes nothing; post-commit response loss can be queried without retransmitting bytes or creating a duplicate artifact.
- Hard per-upload, per-Run, concurrent and aggregate limits, owner-only permissions, leases, TTL, explicit release, Run-terminal cleanup and daemon-start cleanup are mandatory and cannot be weakened by caller policy.
- SDK and CLI keep stage, input, and release as separately correlated outcomes. A staged path does not prove that a Run consumed it, and failed input does not falsify a committed artifact.
- Documentation states host locality precisely: connecting to a ctxmuxd stages on that daemon's host. A higher layer may reach a remote ctxmuxd through its own authenticated transport, but ctxmux does not own SSH installation, forwarding, or credentials.
- A real native Run reads exact source bytes through the returned path; unsupported and terminal-state Runs reject before upload. Same-UID hostile child isolation is explicitly not promised.

## Transfer Checks

- PNG, arbitrary binary, fragmented non-UTF-8, misleading extensions and Unicode logical names use the same envelope and integrity rules.
- Caller denial in the embedding host produces no ctxmux request; caller approval cannot relax daemon bounds or permissions.
- Disconnect before commit publishes nothing; disconnect after commit preserves the documented lease and permits operation-status recovery.
- Wrong digest, duplicate operation key with different content, traversal, symlink replacement, expired Run, quota pressure and late frames fail closed.
- Provider-native resume creates a new Run: an artifact scoped to the old Run is not silently carried forward; the client must ensure the target Run first and then stage.
- Agent-to-Agent messaging remains entirely above this feature; no Agent, message, turn, permission, or reply type enters ctxmux.

## Impact

- Code paths: protocol capability and upload/status/release frames, daemon artifact owner, Run/resource accounting, Rust client, TypeScript SDK, CLI, docs and Backend capability declarations.
- Tests: parser/property, real filesystem, idempotent response-loss, lifecycle/cleanup, quota/load/resource, native Run E2E, SDK/CLI clean-consumer, and unsupported negative-space tests.
- Rollout notes: execute only after the current reliability Feature supplies correlated controls and resource accounting. Ship the native Run slice first; future Backends earn capability independently.
