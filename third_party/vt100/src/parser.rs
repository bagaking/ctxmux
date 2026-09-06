/// A parser for terminal output which produces an in-memory representation of
/// the terminal contents.
pub struct Parser {
    parser: vte::Parser,
    screen: crate::screen::Screen,
}

/// A basic VT restore seed, written to a fresh terminal at this source grid.
///
/// Contains actual normal scrollback, both screens when alternate is active,
/// current cursor/rendition, margins/origin and the supported input modes.
/// This is not a serialization of unsupported extensions or parser carry.
#[derive(Clone, Debug)]
pub struct BasicCheckpoint {
    pub rows: u16,
    pub cols: u16,
    pub restore_bytes: Vec<u8>,
}

/// Declared geometry required to restore retained alternate cells before the
/// public emulator is resized to the actual source view. This is emulator
/// geometry, never permission to resize a user's physical terminal.
#[derive(Clone, Debug)]
pub struct BasicRestoreGeometryCheckpoint {
    pub source_rows: u16,
    pub source_cols: u16,
    pub restore: BasicCheckpoint,
    pub final_bytes: Vec<u8>,
    /// Temporary consumer history retention required to evict the encoder's
    /// predecessor after setting the first retained row's incoming wrap flag.
    /// Restore the consumer's original policy immediately after seed bytes.
    pub restore_scrollback_rows: Option<usize>,
}

impl Parser {
    /// Whether the parser has no unfinished control sequence or UTF-8 input.
    #[must_use]
    pub fn is_ground(&self) -> bool {
        self.parser.is_ground()
    }

    /// Export the supported basic VT state only at a complete parse boundary.
    ///
    /// The caller owns the byte/resize fence and byte-zero provenance. A parser
    /// created from an arbitrary suffix does not acquire that provenance here.
    #[must_use]
    pub fn basic_checkpoint(&self) -> Option<BasicCheckpoint> {
        self.is_ground().then(|| {
            let (rows, cols) = self.screen.size();
            BasicCheckpoint {
                rows,
                cols,
                restore_bytes: self.screen.basic_restore_formatted(),
            }
        })
    }

    #[must_use]
    pub fn basic_restore_geometry_checkpoint(&self) -> Option<BasicRestoreGeometryCheckpoint> {
        self.is_ground().then(|| {
            let (source_rows, source_cols) = self.screen.size();
            let restored = self.screen.restore_geometry();
            let (rows, cols) = restored.size();
            BasicRestoreGeometryCheckpoint {
                final_bytes: self.screen.basic_final_restore_formatted(),
                restore_scrollback_rows: restored.restore_scrollback_budget(),
                source_rows,
                source_cols,
                restore: BasicCheckpoint {
                    rows,
                    cols,
                    restore_bytes: restored.basic_restore_formatted(),
                },
            }
        })
    }

    /// Creates a new terminal parser of the given size and with the given
    /// amount of scrollback.
    #[must_use]
    pub fn new(rows: u16, cols: u16, scrollback_len: usize) -> Self {
        Self {
            parser: vte::Parser::new(),
            screen: crate::screen::Screen::new(crate::grid::Size { rows, cols }, scrollback_len),
        }
    }

    /// Processes the contents of the given byte string, and updates the
    /// in-memory terminal state.
    pub fn process(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.parser.advance(&mut self.screen, *byte);
        }
    }

    /// Resizes the terminal.
    pub fn set_size(&mut self, rows: u16, cols: u16) {
        self.screen.set_size(rows, cols);
    }

    /// Returns the current normal-history retention policy in rows.
    #[must_use]
    pub fn scrollback_limit(&self) -> usize {
        self.screen.normal_scrollback_limit()
    }

    /// Changes normal-history retention policy, returning the number of rows
    /// evicted by this operation. This is distinct from selecting a view offset.
    /// A restore consumer can reinstate its original policy after the seed's
    /// temporary predecessor has been evicted.
    pub fn set_scrollback_limit(&mut self, limit: usize) -> usize {
        self.screen.set_scrollback_limit(limit)
    }

    /// Scrolls to the given position in the scrollback.
    ///
    /// This position indicates the offset from the top of the screen, and
    /// should be `0` to put the normal screen in view.
    ///
    /// This affects the return values of methods called on `parser.screen()`:
    /// for instance, `parser.screen().cell(0, 0)` will return the top left
    /// corner of the screen after taking the scrollback offset into account.
    /// It does not affect `parser.process()` at all.
    ///
    /// The value given will be clamped to the actual size of the scrollback.
    pub fn set_scrollback(&mut self, rows: usize) {
        self.screen.set_scrollback(rows);
    }

    /// Returns a reference to a `Screen` object containing the terminal
    /// state.
    #[must_use]
    pub fn screen(&self) -> &crate::screen::Screen {
        &self.screen
    }
}

impl Default for Parser {
    /// Returns a parser with dimensions 80x24 and no scrollback.
    fn default() -> Self {
        Self::new(24, 80, 0)
    }
}

impl std::io::Write for Parser {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.process(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
