//! Interactive attach view: reconstruct the current screen from raw replay.

use ctxmux_protocol::TerminalSize;

const SYNC_BEGIN: &[u8] = b"\x1b[?2026h";
const SYNC_END: &[u8] = b"\x1b[?2026l";

/// Reconstruct the visible screen for one interactive attach paint.
///
/// The daemon still retains raw bytes. This view collapses CSI history so a
/// late attach paints one still frame instead of replaying every redraw.
pub fn reconstruct(replay: &[u8], size: TerminalSize) -> Vec<u8> {
    let rows = size.rows.max(1);
    let cols = size.cols.max(1);
    let mut parser = vt100::Parser::new(rows, cols, 0);
    parser.process(replay);
    let formatted = parser.screen().state_formatted();
    let mut frame = Vec::with_capacity(formatted.len() + SYNC_BEGIN.len() + SYNC_END.len());
    frame.extend_from_slice(SYNC_BEGIN);
    frame.extend(formatted);
    frame.extend_from_slice(SYNC_END);
    frame
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconstruction_drops_erased_history() {
        let view = reconstruct(
            b"STALE\n\x1b[2J\x1b[HREADY",
            TerminalSize { cols: 80, rows: 24 },
        );
        assert!(
            !contains(&view, b"STALE"),
            "erased history leaked into the attach view: {}",
            String::from_utf8_lossy(&view)
        );
        assert!(
            contains(&view, b"READY"),
            "current screen missing from the attach view: {}",
            String::from_utf8_lossy(&view)
        );
        assert!(
            view.starts_with(SYNC_BEGIN) && view.ends_with(SYNC_END),
            "attach paint was not wrapped in synchronized output: {}",
            String::from_utf8_lossy(&view)
        );
    }

    #[test]
    fn reconstruction_keeps_the_latest_alternate_screen() {
        let replay = b"OLD\x1b[?1049h\x1b[2J\x1b[HALT";
        let view = reconstruct(replay, TerminalSize { cols: 40, rows: 12 });
        assert!(
            !contains(&view, b"OLD"),
            "primary-screen history leaked after alternate-screen entry: {}",
            String::from_utf8_lossy(&view)
        );
        assert!(
            contains(&view, b"ALT"),
            "alternate screen missing from the attach view: {}",
            String::from_utf8_lossy(&view)
        );
    }

    fn contains(haystack: &[u8], needle: &[u8]) -> bool {
        haystack
            .windows(needle.len())
            .any(|window| window == needle)
    }
}
