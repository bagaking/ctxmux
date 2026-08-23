# Revision 4 review: ctxmux-owned composition and release

- Status: approved by the Owner through the supervised Feature-pool cleanup.
- Scope: recalibrate only unstarted tasks T-003 and T-004 and add the
  Recoverable Stop dependency.
- Prior evidence: no current task has execution or gate evidence, so the
  unstarted task wording may be replaced in place without erasing history.

## Decision

Composition and release remain a ctxmux Feature. T-003 consumes the Phase 1
activation contract from `f-22ecztapc` and proves it through ctxmux-owned clean
consumers. T-004 consumes the exact ctxmux owner evidence from Phase 1 and
Recoverable Stop, then qualifies ctxmux packages, binaries, claims, supported
platforms, and release gates.

An AgentMux checkout, receipt, version pin, or repin is not acceptance evidence
for this repository. No external product repository is required to close this
Feature, and release work does not absorb either upstream implementation.

## Preserved boundaries

- Composition policy stays in the deliberately small example client.
- Daemon activation is consumed, not reimplemented.
- Package publication, Git push, hosted release, license choice, credentials,
  and external cost still require explicit Owner authority.
- Kernel, tmux, Phase 1, and Recoverable Stop capability truth remain with
  their own Feature owners.
