# ctxmux native VT export extension

This source is vt100 0.15.2 (MIT) with a narrow public basic-state exporter.
`CTXMUX-PROVENANCE.json` binds every copied original file and the published
registry checksum. Parsing, grids, cells and row formatting remain vt100's;
ctxmux does not introduce another emulator.

- `Parser::is_ground()` delegates to the pinned VTE boundary query. Partial
  control strings and UTF-8 are not complete boundaries.
- `Parser::basic_checkpoint()` returns source rows/cols and restore bytes only
  at a complete boundary. The restore seed is consumed at that source grid on
  a fresh view before the original ordered byte/resize tail.
- The seed contains actual normal retained rows, current normal screen and,
  when active, the alternate screen. Current cursor/rendition, margins/origin,
  the normal return cursor and supported input modes (including tracking and
  SGR encoding) are formatted by the existing library owner.
- CUP-only repaint does not create scrollback. A formatted seed is not raw Run
  output, does not advance an output cursor, and must never reach PTY input.
- Byte-zero provenance and byte/resize ordering belong to the ctxmux Run owner.
  Starting a parser from an evicted suffix cannot establish them.

This API declares basic VT, not complete xterm serialization. Saved charset,
extension stacks and unsupported terminal features remain outside this contract.
The separate full-continuation task owns their precision. The normal/alternate,
SGR, wrapping, margins and source-grid results are independently checked against
the public xterm target; native self-roundtrip alone is not the target proof.

Alternate buffer entry inherits the actual current cursor/origin; plain exit carries the current cursor back. Saved normal registers remain separate for 1049 return. This corrects a public xterm basic drawing mismatch, without exporting unsupported saved extension stacks.

The public scrollback viewport iterator takes no more than screen-height rows
and saturates the remaining current-row count when the offset is deeper than
the viewport. This corrects the upstream unsigned subtraction underflow on
`Parser::set_scrollback` followed by `Screen::contents`, proven by the actual
native attachment owner test with normal history deeper than screen height.

Positive-grid resize must leave the active drawing cursor inside the resized
viewport. Hidden saved positions remain separate registers; Restore safely
projects them into the current viewport without overwriting their future
position. Normal saved positions follow their actual retained-history row across height
changes; restoring still maps them into the current legal viewport. Actual
history eviction and alternate physical clipping remain distinct operations.
Height-only resize preserves saved pending-wrap state. A zero-length change
in reflowed history does not erase a hidden saved row. Plain alternate-buffer
exit clears drawing contents while preserving saved position and origin mode;
full buffer clear resets those registers. A
resize that cuts a wide character removes its incomplete leading cell while
preserving that cell's rendition; complete wide pairs and other cells remain.
A character wider than the entire grid is ignored without moving the cursor,
so a one-column terminal remains usable for subsequent narrow characters.
Autowrap on a one-row grid must retain the actual wrapped history row and
continue drawing on the remaining row. These are parser/grid invariants, not
client retries or a second emulator, and are proved through the public parser,
cell state, checkpoint and subsequent original bytes.

The saved-column clipping follows the existing mature pattern in
[xterm.js Buffer.resize](https://github.com/xtermjs/xterm.js/blob/master/src/common/buffer/Buffer.ts).
[xterm.js InputHandler.print](https://github.com/xtermjs/xterm.js/blob/master/src/common/InputHandler.ts)
requires at least two columns; the one-column contract above is the native
library's explicit boundary, not a claim of xterm one-column equivalence. It
does not reject a one-column Run or alter its original output bytes.

Saved-origin restoration also has a public-consumer version difference: the
pinned older consumer clears origin across plain alternate-buffer switching,
while the newer consumer and the native saved-register contract preserve it.
The finite origin regression preserves native semantics; it does not claim
equivalence across both consumer versions.

Owning verification: `cargo test -p ctxmux-daemon --test
native_terminal_checkpoint_codec -- --nocapture`. This fixed target asserts
normal/alternate saved registers, cell rendition and complete wide pairs,
actual one-row history, same-size pending wrap, and checkpoint/original-tail
continuation. The exact-ec counterexample, four source reversals and restored
control results are recorded in `CTXMUX-RESIZE-VERIFICATION.json`.

## Joined geometry and Unicode candidate

The joined library reflows retained normal logical rows on column changes,
moves real rows into/out of history on height changes, and keeps alternate
cells hidden by a narrow viewport until an actual later resize/edit removes
them. Erasure distinguishes a row's incoming and outgoing wrap relations.
Cells retain combining characters beyond the old six-codepoint array through
the existing pinned SmallVec dependency; ordinary inline storage is a layout
optimization, not a text-length capacity limit. Overflow storage is released
on replacement/erasure. Its memory and allocation pressure remain host costs.

`basic_restore_geometry_checkpoint()` separates source geometry from prefix
restore geometry and final-state bytes. Optional temporary consumer scrollback
metadata expresses an incoming wrap whose actual predecessor was evicted.
Consumers restore their original policy after the prefix, apply actual source
geometry, then final bytes and original continuation. A physical terminal
cannot be resized implicitly to emulate this capability.

Private qualification binds the v13 library to the preserved v12 public
inputs and owning/held-out debug/release proofs. The old
`CTXMUX-RESIZE-VERIFICATION.json` remains historical slice evidence and does
not attest these newer files. Two full small-history public xterm reflow
sequences still have strict original-oracle failures: the consumer loses
retained content which the candidate preserves. Diagnostic working-history
experiments do not turn these failures into original-contract passes.
Daemon allocation funding and complete joined lifecycle/consumer acceptance
are separate open work. This extension does not claim full xterm fidelity.

## Cell rendition storage

Rendition stores two complete three-byte color payloads and one byte containing
four independent style bits and the two-bit tag of each Color variant. This
seven-byte representation removes enum padding from each Cell. It preserves all
256 indexed colors, full RGB colors, independent styles and default reset
semantics. Grapheme payload, wide pairs, history rows and parser behavior retain
their existing contracts; this is a storage representation change. Owning
verification exercises public SGR color/style/reset behavior and the existing
checkpoint, resize, Unicode and external xterm qualification suites. Whole-Run
resource and latency effects require executed workload evidence.

## Exact implicit default cells

Rows retain their semantic width independently of their stored cell prefix.
Untouched default cells read as complete default Cells, and drawing materializes
the required prefix. When a normal row enters history, only its exact default
suffix becomes implicit and unused allocation is released. Blank rows, explicit
spaces, all rendition bits, wide partners, combining payloads, wrapping and
alternate cells retained outside the viewport preserve their meaning. Normal
default-fill resizing changes semantic width without expanding untouched cells
in every historical row; it still removes a wide lead clipped at the actual
right edge. Reflow and physical-line editing materialize the same cells they
require. Alternate retained cells continue to use their physical-line path.

This representation does not reduce the retained row policy, shorten graphemes
or change checkpoint bytes. Dense rows and styled blanks retain their full
storage costs. Public parser tests cover historical cell state, restoring a
checkpoint, bringing history back into the live viewport, editing its tail and
resizing again. Matched private dense/sparse workloads compare complete restore
bytes and geometry, not only visible text. These proofs do not provide aggregate
daemon funding or qualify the original fleet soak; those remain separate owners.
