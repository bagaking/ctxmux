# Revision 3 task-plan review

## Verdict

Approved. T-001 remains an immutable blocked task with its original Gate and
external Ubuntu/macOS receipt blocker. New T-002 owns the discovered tmux
observation-lag correction and does not supersede or erase T-001.

## Finding and owner

The daemon broadcast carries raw output and tmux `session_renamed`, `paused`,
and `continued` observations, but lag currently always emits output-centric
`Gap { head_seq }`. Byte replay cannot reconstruct lost non-output observations.
That is a tmux Backend qualification gap at the generic attachment-delivery
boundary, not T-013 control correlation and not retained-Run GC.

## Required boundary

- `Gap { head_seq }` directs byte-cursor replay and permits explicit
  truncation; it does not authenticate recovery of Backend observations.
- Lost non-output observations must be reconstructible from declared state or
  cause an explicit fail-closed discontinuity.
- Tiny-channel mixed-event fixtures cover raw replay, every current tmux
  observation, terminal ordering, and tmux pane survival.
- Incompatible wire changes advance the protocol generation without fallback.
- No durable event bus, public Backend trait, Agent state, or tmux private wire
  protocol is introduced.

T-002 local completion cannot replace T-001's required Ubuntu tmux 3.4 and
macOS-current receipts. T-001 must be re-gated from the corrected commit before
archive.
