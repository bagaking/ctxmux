# Persistence hardening Gate receipt

This receipt binds the post-archive persistence correction to a reconstructable
Git source tree. It is qualification evidence only: Feature and Task lifecycle
truth remains owned by Feature Tracker, and this file does not define a reusable
attestation format.

## Qualified source

- Feature: `f-227czdavj`
- Task: `T-002`
- Implementation commit: `dc55c94c0331ce3392f982552b90ff91731fca08`
- Implementation tree: `cebdb234cd7088a0211d6248fd7ceef37096b38c`
- Parent commit: `b80a5cf44d1fafc653d9440eb5611e1e57e26d18`
- Source status before the formal Gate: tracked worktree, index, and the
  non-ignored untracked set were empty (`git status --porcelain=v1` produced no
  output).

The qualified source can be reconstructed with:

```sh
git switch --detach dc55c94c0331ce3392f982552b90ff91731fca08
git rev-parse HEAD^{tree}
```

The second command must print
`cebdb234cd7088a0211d6248fd7ceef37096b38c`.

## Formal Gate

- Command: `scripts/check.sh`
- Result: pass (exit 0)
- Tracker Gate record mtime: `2026-08-10T19:31:04+0800`
- Execution platform: `Darwin arm64`
- Toolchain observations: `rustc 1.96.0`, `node v24.14.1`, `tmux 3.6b`
- Tracker Gate log SHA-256:
  `d3b7054da9c212e9e596b17d45ce4fed6a59576299af0f639b65b8036afdc8d9`

The ignored Tracker log contained exactly:

```text
project_type=non_ui
execution_root=.
workspace_mode=current_tree
execution_detail=current_tree uses the tracker root
result=pass
commands:
- scripts/check.sh => pass (0)
```

The Gate exercised the shared RunSpec validation, persistent insert invariant,
seven real persistence-recovery tests (including semantic corruption before
socket publication), the full Rust workspace, TypeScript validation and build,
public SDK/CLI end-to-end tests, and the smoke reliability lane.

## Evidence boundary

This receipt proves that the implementation commit and tree above passed the
declared repository Gate from a clean source state. The receipt commit itself
adds only this evidence document and is not substituted for the qualified tree.

It does not claim Ubuntu tmux 3.4 evidence, release readiness, performance
qualification, schema migration, salvage, tmux persistence, PID adoption, or
live PTY handoff. Those remain owned by their existing Features and Tasks.
