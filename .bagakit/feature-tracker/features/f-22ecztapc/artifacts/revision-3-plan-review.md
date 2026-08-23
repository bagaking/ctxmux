# Feature plan revision 3 review

Status: approved by direct user selection on 2026-08-23.

## T-002 confirmed capability and build-target contract

Plan revision 2 remains the minimum `RuntimeIdentity` shape and lifecycle
contract. The user additionally confirmed these previously open choices:

- The initial flat capability-key catalog is:
  - `native.start`
  - `native.recoverable_input`
  - `native.fork_level_a`
  - `native.execute_materialized_level_b`
  - `tmux.discover`
  - `tmux.import`
  - `services.persistent_state`
  - `services.planned_exec_upgrade_continuity`
- A capability version is a JavaScript-safe positive integer in
  `1..=9_007_199_254_740_991`. Version `1` names the currently implemented
  contract for each advertised initial key. Absence remains unsupported; a
  request above the advertised version remains unsupported.
- `platform` and `arch` use Rust build-target vocabulary directly from
  `std::env::consts::OS` and `std::env::consts::ARCH`. For example, an Apple
  Silicon macOS build advertises `macos` and `aarch64`; these values are not
  Node/release names such as `darwin` and `arm64`.
- Capability requirements are client-local compatibility preconditions, not
  wire negotiation. The Rust client builder and TypeScript client options may
  carry exact required key/version pairs. After Hello and before sending a
  business request or Attach frame, the client compares the requirements with
  the advertised record and returns an explicit `unsupported_capability`
  result for an absent key or an advertised version below the requested one.
- Raw identity inspection and readiness remain available: Rust `ping` and
  `runtime_info`, TypeScript `runtimeInfo`, and CLI auto-start readiness read
  Hello without applying configured business-dispatch requirements. This lets
  callers diagnose an incompatible live Runtime without replacing it.
- Requirement keys are compared exactly and are not whitelisted, normalized,
  inferred from platform or executable state, or mapped automatically from
  operations. No requirement is added to ClientHello or any Request frame.

Changing the confirmed fields, discriminators, initial flat keys, safe-integer
version semantics, Rust target vocabulary, or client-local pre-dispatch
requirement boundary requires new explicit user confirmation and a later
reviewed Feature Tracker plan revision before implementation.
