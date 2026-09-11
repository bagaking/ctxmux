//! One derived terminal model, ordered by the Run's existing output lock.

use ctxmux_protocol::{
    OutputReplay, RunId, TerminalCheckpointHeader, TerminalCheckpointUnavailableReason as Reason,
    TerminalContinuation, TerminalResize, TerminalSize,
};
use serde::{Deserialize, Serialize};

/// VT state is derived from the original bytes. A library unwind invalidates that
/// one model; it must not unwind the shared PTY owner or decide a child's lifecycle.
pub(crate) fn derive_terminal<T>(derive: impl FnOnce() -> T) -> Option<T> {
    crate::diagnostics::catch_native_unwind(std::panic::AssertUnwindSafe(derive)).ok()
}

pub(crate) const HISTORY_ROWS: usize = 10_000;
pub(crate) const MAX_RESTORE_BYTES: usize = 32 * 1024 * 1024;
const MAX_RESIZE_TAIL: usize = 1024;

/// A Run-keyed derived seed; original output remains solely in OutputLog/SQLite.
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct StoredCheckpoint {
    pub(crate) checkpoint: TerminalCheckpointHeader,
    #[serde(with = "checkpoint_bytes")]
    pub(crate) restore: Vec<u8>,
    pub(crate) resizes: Vec<TerminalResize>,
}

pub(crate) struct TerminalModel {
    parser: vt100::Parser,
    checkpoint: Option<StoredCheckpoint>,
    resize_revision: u64,
    geometry_valid: bool,
    #[cfg(test)]
    fail_next_process: bool,
}

impl TerminalModel {
    pub(crate) fn new(id: RunId, size: TerminalSize) -> Self {
        let mut model = Self {
            parser: vt100::Parser::new(size.rows, size.cols, HISTORY_ROWS),
            checkpoint: None,
            resize_revision: 0,
            geometry_valid: true,
            #[cfg(test)]
            fail_next_process: false,
        };
        model.cut(id, 0);
        model
    }

    pub(crate) fn process(&mut self, data: &[u8]) {
        self.parser.process(data);
        #[cfg(test)]
        assert!(
            !std::mem::take(&mut self.fail_next_process),
            "owner-local terminal derivation failure after raw admission"
        );
    }

    #[cfg(test)]
    pub(crate) fn fail_next_process_for_test(&mut self) {
        self.fail_next_process = true;
    }
    pub(crate) const fn resize_revision(&self) -> u64 {
        self.resize_revision
    }
    pub(crate) fn size(&self) -> TerminalSize {
        let (rows, cols) = self.parser.screen().size();
        TerminalSize { rows, cols }
    }

    pub(crate) fn resize(&mut self, size: TerminalSize, through_byte: u64) -> TerminalResize {
        self.resize_revision += 1;
        self.parser.set_size(size.rows, size.cols);
        let resize = TerminalResize {
            through_byte,
            resize_revision: self.resize_revision,
            size,
        };
        if let Some(checkpoint) = &mut self.checkpoint {
            if checkpoint.resizes.len() == MAX_RESIZE_TAIL {
                self.geometry_valid = false;
            } else {
                checkpoint.resizes.push(resize.clone());
            }
        }
        resize
    }

    /// Export only on attach/barrier or when raw eviction would uncover a fence.
    pub(crate) fn cut(&mut self, id: RunId, through_byte: u64) -> bool {
        let Some(seed) = self.parser.basic_restore_geometry_checkpoint() else {
            return false;
        };
        let prefix_bytes = seed.restore.restore_bytes.len();
        let total_bytes = prefix_bytes.checked_add(seed.final_bytes.len());
        if total_bytes.is_none_or(|bytes| bytes > MAX_RESTORE_BYTES) {
            return false;
        }
        let mut restore = seed.restore.restore_bytes;
        restore.extend_from_slice(&seed.final_bytes);
        self.checkpoint = Some(StoredCheckpoint {
            checkpoint: TerminalCheckpointHeader {
                run_id: id,
                through_byte,
                resize_revision: self.resize_revision,
                size: TerminalSize {
                    rows: seed.source_rows,
                    cols: seed.source_cols,
                },
                restore_size: TerminalSize {
                    rows: seed.restore.rows,
                    cols: seed.restore.cols,
                },
                restore_scrollback_rows: seed.restore_scrollback_rows.map(|rows| rows as u64),
                resize_after_restore_bytes: prefix_bytes as u64,
                restore_bytes: restore.len() as u64,
            },
            restore,
            resizes: Vec::new(),
        });
        self.geometry_valid = true;
        true
    }

    pub(crate) fn retention_cut(&mut self, id: RunId, first: u64, latest: u64) -> bool {
        if self
            .checkpoint
            .as_ref()
            .is_none_or(|c| c.checkpoint.through_byte < first)
        {
            self.cut(id, latest)
        } else {
            false
        }
    }

    pub(crate) fn continuation(
        &mut self,
        id: RunId,
        first: u64,
        latest: u64,
    ) -> (TerminalContinuation, Vec<u8>) {
        self.cut(id, latest);
        let Some(saved) = &self.checkpoint else {
            return (
                TerminalContinuation::Unavailable {
                    reason: Reason::CheckpointTooLarge,
                },
                Vec::new(),
            );
        };
        if !self.geometry_valid {
            return (
                TerminalContinuation::Unavailable {
                    reason: Reason::InvalidCheckpoint,
                },
                Vec::new(),
            );
        }
        if saved.checkpoint.through_byte < first {
            return (
                TerminalContinuation::Unavailable {
                    reason: Reason::TailEvicted,
                },
                Vec::new(),
            );
        }
        (
            TerminalContinuation::BasicVt {
                checkpoint: saved.checkpoint.clone(),
                resizes: saved.resizes.clone(),
            },
            saved.restore.clone(),
        )
    }

    pub(crate) fn stored(&self) -> Option<StoredCheckpoint> {
        self.geometry_valid
            .then(|| self.checkpoint.clone())
            .flatten()
    }

    /// A file is authoritative only if its fence and all original tail survived.
    pub(crate) fn recover(
        id: RunId,
        saved: StoredCheckpoint,
        replay: &OutputReplay,
    ) -> Option<Self> {
        let cp = &saved.checkpoint;
        if cp.run_id != id
            || cp.size.rows == 0
            || cp.size.cols == 0
            || cp.restore_size.rows == 0
            || cp.restore_size.cols == 0
            || cp.resize_after_restore_bytes > cp.restore_bytes
            || cp.restore_bytes != saved.restore.len() as u64
            || saved.restore.len() > MAX_RESTORE_BYTES
            || cp.through_byte < replay.first_available_byte
            || cp.through_byte > replay.latest_output_bytes
            || saved.resizes.len() > MAX_RESIZE_TAIL
        {
            return None;
        }
        let restore_history = cp
            .restore_scrollback_rows
            .map(usize::try_from)
            .transpose()
            .ok()?
            .unwrap_or(HISTORY_ROWS);
        let mut model = Self {
            parser: vt100::Parser::new(cp.restore_size.rows, cp.restore_size.cols, restore_history),
            checkpoint: None,
            resize_revision: cp.resize_revision,
            geometry_valid: true,
            #[cfg(test)]
            fail_next_process: false,
        };
        let split = usize::try_from(cp.resize_after_restore_bytes).ok()?;
        model.parser.process(&saved.restore[..split]);
        if !model.parser.is_ground() {
            return None;
        }
        // The encoder predecessor has already been evicted by the temporary
        // prefix policy. Reinstate the owner's policy before actual reflow.
        model.parser.set_scrollback_limit(HISTORY_ROWS);
        if cp.restore_size != cp.size {
            model.parser.set_size(cp.size.rows, cp.size.cols);
        }
        model.parser.process(&saved.restore[split..]);
        if !model.parser.is_ground() {
            return None;
        }
        let mut cursor = cp.through_byte;
        let mut resizes = saved.resizes.iter().peekable();
        for chunk in &replay.chunks {
            if chunk.end_byte <= cursor {
                continue;
            }
            if chunk.start_byte > cursor {
                return None;
            }
            while cursor < chunk.end_byte {
                while let Some(resize) = resizes.peek().filter(|r| r.through_byte == cursor) {
                    if resize.resize_revision != model.resize_revision + 1
                        || resize.size.rows == 0
                        || resize.size.cols == 0
                    {
                        return None;
                    }
                    model.parser.set_size(resize.size.rows, resize.size.cols);
                    model.resize_revision = resize.resize_revision;
                    resizes.next();
                }
                if resizes.peek().is_some_and(|r| r.through_byte < cursor) {
                    return None;
                }
                let end = resizes
                    .peek()
                    .map_or(chunk.end_byte, |r| r.through_byte.min(chunk.end_byte));
                let a = usize::try_from(cursor - chunk.start_byte).ok()?;
                let b = usize::try_from(end - chunk.start_byte).ok()?;
                model.parser.process(chunk.data.get(a..b)?);
                cursor = end;
            }
        }
        if cursor != replay.latest_output_bytes {
            return None;
        }
        for resize in resizes {
            if resize.through_byte != cursor
                || resize.resize_revision != model.resize_revision + 1
                || resize.size.rows == 0
                || resize.size.cols == 0
            {
                return None;
            }
            model.parser.set_size(resize.size.rows, resize.size.cols);
            model.resize_revision = resize.resize_revision;
        }
        model.checkpoint = Some(saved);
        Some(model)
    }
}

mod checkpoint_bytes {
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    use serde::{Deserialize, Deserializer, Serializer, de::Error as _};
    pub(super) fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(&base64::display::Base64Display::new(bytes, &STANDARD))
    }
    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<u8>, D::Error> {
        STANDARD
            .decode(String::deserialize(deserializer)?)
            .map_err(D::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn public_sgr_preserves_all_indexed_colors_and_independent_styles() {
        for index in 0..=u8::MAX {
            for style in 0..32_u8 {
                let mut sgr = vec!["0".to_owned()];
                for (bit, code) in [(1, "1"), (2, "3"), (4, "4"), (8, "7"), (16, "2")] {
                    if style & bit != 0 {
                        sgr.push(code.to_owned());
                    }
                }
                let rgb = (index, u8::MAX - index, index.rotate_left(3));
                let bytes = format!(
                    "\x1b[{};38;5;{index};48;2;{};{};{}mX\x1b[39mY\x1b[49;22;23;24;27mZ",
                    sgr.join(";"),
                    rgb.0,
                    rgb.1,
                    rgb.2,
                );
                let mut parser = vt100::Parser::new(1, 3, 0);
                parser.process(bytes.as_bytes());
                for (column, foreground) in
                    [(0, vt100::Color::Idx(index)), (1, vt100::Color::Default)]
                {
                    let cell = parser.screen().cell(0, column).unwrap();
                    assert_eq!(cell.fgcolor(), foreground);
                    assert_eq!(cell.bgcolor(), vt100::Color::Rgb(rgb.0, rgb.1, rgb.2));
                    assert_eq!(
                        [
                            cell.bold(),
                            cell.italic(),
                            cell.underline(),
                            cell.inverse(),
                            cell.faint()
                        ],
                        [
                            style & 1 != 0,
                            style & 2 != 0,
                            style & 4 != 0,
                            style & 8 != 0,
                            style & 16 != 0
                        ]
                    );
                }
                let mut plain = vt100::Parser::new(1, 1, 0);
                plain.process(b"Z");
                assert_eq!(parser.screen().cell(0, 2), plain.screen().cell(0, 0));
            }
        }
    }
    #[test]
    fn faint_checkpoint_preserves_intensity_wide_geometry_and_cell_memory() {
        // Both supported native targets retain the pre-faint Cell layout. This
        // guards allocation size rather than changing a resource-policy limit.
        assert_eq!(std::mem::size_of::<vt100::Cell>(), 32);
        let mut parser = vt100::Parser::new(4, 20, 20);
        parser.process(b"\x1b[1;2mA\x1b[22mB");
        assert!(parser.screen().cell(0, 0).unwrap().faint());
        assert!(parser.screen().cell(0, 0).unwrap().bold());
        assert!(!parser.screen().cell(0, 1).unwrap().faint());
        assert!(!parser.screen().cell(0, 1).unwrap().bold());
        parser.process("\x1b[2m界\x1b[22mplain".as_bytes());
        assert!(parser.screen().cell(0, 2).unwrap().is_wide());
        assert!(parser.screen().cell(0, 2).unwrap().faint());
        assert!(parser.screen().cell(0, 3).unwrap().is_wide_continuation());
        let seed = parser.basic_checkpoint().unwrap();
        let mut restored = vt100::Parser::new(4, 20, 20);
        restored.process(&seed.restore_bytes);
        for row in 0..4 {
            for col in 0..20 {
                assert_eq!(
                    parser.screen().cell(row, col),
                    restored.screen().cell(row, col)
                );
            }
        }
        // All sixteen intensity transitions exercise SGR22's shared reset.
        for old in 0..4 {
            for new in 0..4 {
                let sgr = |value| {
                    format!(
                        "\x1b[0{}{}mX",
                        if value & 1 != 0 { ";1" } else { "" },
                        if value & 2 != 0 { ";2" } else { "" }
                    )
                };
                let mut reference = vt100::Parser::new(1, 2, 0);
                reference.process(sgr(old).as_bytes());
                let previous = reference.screen().clone();
                reference.process(b"\x1b[H");
                reference.process(sgr(new).as_bytes());
                let diff = reference.screen().contents_diff(&previous);
                let mut target = vt100::Parser::new(1, 2, 0);
                target.process(sgr(old).as_bytes());
                target.process(&diff);
                assert_eq!(reference.screen().cell(0, 0), target.screen().cell(0, 0));
            }
        }
    }
}
