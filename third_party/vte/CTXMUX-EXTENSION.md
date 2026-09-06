# ctxmux complete-boundary query

Pinned vte 0.11.1, Apache-2.0 OR MIT. Original file hashes and the published
registry checksum are in `CTXMUX-PROVENANCE.json`; the original licenses are
kept alongside the source.

The only runtime change is public `Parser::is_ground()`. It returns true only
for the library's Ground state, so unfinished control strings and the actual
out-of-band UTF-8 state cannot be discarded by a state-only checkpoint. Parser
state/carry remains owned by VTE, not inferred from bytes by a client.
