# 012 — tmux public-surface Backend

- Status: provisional
- Scope: adapting existing tmux-owned sessions without private wire compatibility

## Context

Users already have durable sessions in tmux. ctxmux should expose them through its client model without taking ownership, reimplementing tmux, or coupling the native Backend to tmux internals.

## Decision

The target adapter uses the tmux executable and public Control Mode. tmux remains the owner of its server, sessions, windows, panes, PTYs, and persistence. ctxmux maps supported behavior into Backend capabilities and documents differences from native Runs.

ctxmux will not implement tmux's private client-server socket protocol and will not promise that an unmodified `tmux attach` command can connect to a ctxmux daemon.

## Quality attributes and invariants

- ctxmux client disconnect never kills the tmux session.
- Session disappearance, rename, and pane changes are explicit observable events or errors.
- Unsupported native semantics are capability-visible instead of emulated incorrectly.
- tmux output and command escaping are parsed according to public documented formats.
- Native Runs do not acquire a tmux dependency.

## Alternatives

- Speaking the private tmux wire protocol couples ctxmux to version-specific internals.
- Shelling out only to `capture-pane` cannot provide a complete ordered live stream.
- Requiring tmux for every Run makes an external tool the core runtime owner.
- Importing tmux semantics into the generic Run model would leak Backend details into clients.

## Known constraints

No Backend interface or tmux code exists. Mapping Run identity to sessions, windows, and panes is undecided. Control Mode escaping, `%` notifications, command correlation, initial capture, resize ownership, multi-client behavior, and the supported tmux version matrix all require real fixtures.

## Wrong-case corpus

Evidence pack: [tmux-backend track](../../../.bagakit/researcher/topics/engineering/ctxmux-wrong-case-corpus/tracks/tmux-backend.md), claim `C012`.

- `TMUX-01` (`l01`, `l02`): Control Mode payload is octal-escaped bytes, can be invalid UTF-8, and is interleaved with notifications and extensible arguments. Framing and pane bytes need separate parsers.
- `TMUX-02` (`l01`, `l02`): tmux can pause or terminate a slow control client. Resync must expose a gap and cannot relabel `capture-pane` screen state as lossless raw history.
- `TMUX-03` (`l03`): upstream commit `800837ff` fixed queued-output detach order that left a dangling pointer. Detach-under-load belongs in the supported-version matrix.

Pane IDs are stable only within one tmux server lifetime. Control Mode documentation also lacks an atomic capture-plus-subscribe primitive, so the initial capture/live join remains an open Backend algorithm rather than an assumed guarantee.

## Fixture mapping

- Inactive: all tmux fixtures until T-006 implements the adapter.
- Candidate activation fixture: discover and attach to a real tmux session.
- Candidate activation fixture: output containing percent signs, newlines, and control characters.
- Candidate activation fixture: session rename, pane exit, and server disappearance.
- Candidate activation fixture: multiple ctxmux clients detach without terminating tmux.
- Candidate activation fixture: supported minimum and current tmux versions.

## Open questions

- Is one ctxmux Run a tmux session, window, pane, or explicit adapter target?
- How are initial `capture-pane` state and live Control Mode output joined without gaps?
- Who owns terminal size when several clients attach?
- Which native operations are unsupported, weaker, or differently ordered?
- How are tmux server epochs and target identity recorded?

## Repository evidence

- `docs/architecture.md`: Backend and tmux boundary
- `docs/roadmap.md`: M4
- tmux public references will be preserved under `.bagakit/researcher/` during T-009
