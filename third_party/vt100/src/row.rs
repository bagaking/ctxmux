use crate::term::BufWrite as _;

static EMPTY_CELL: std::sync::LazyLock<crate::cell::Cell> =
    std::sync::LazyLock::new(crate::cell::Cell::default);

#[derive(Clone, Debug)]
pub struct Row {
    cells: Vec<crate::cell::Cell>,
    cols: u16,
    retained: Option<Vec<crate::cell::Cell>>,
    wrapped: bool,
}

impl Row {
    pub fn new(cols: u16) -> Self {
        Self {
            cells: Vec::with_capacity(usize::from(cols)),
            cols,
            retained: None,
            wrapped: false,
        }
    }

    fn cols(&self) -> u16 {
        self.cols
    }

    pub fn clear(&mut self, attrs: crate::attrs::Attrs) {
        if let Some(last) = self.cols().checked_sub(1) {
            self.clear_wide(last, attrs);
        }
        if attrs == crate::attrs::Attrs::default() {
            self.cells.clear();
        } else {
            self.materialize();
            for cell in &mut self.cells {
                cell.clear(attrs);
            }
        }
        self.wrapped = false;
    }

    fn cells(&self) -> impl Iterator<Item = &crate::cell::Cell> {
        self.cells
            .iter()
            .chain(std::iter::repeat(&*EMPTY_CELL).take(usize::from(self.cols) - self.cells.len()))
    }

    pub fn retained_cols(&self) -> u16 {
        self.retained
            .as_ref()
            .map_or(self.cols(), |cells| u16::try_from(cells.len()).unwrap())
    }

    pub fn into_cells(mut self) -> Vec<crate::cell::Cell> {
        self.materialize();
        self.cells
    }

    /// Only exact default cells are implicit. Width, wrapping, colors, wide
    /// partners and every retained physical cell keep their original meaning.
    /// Live rows reserve their width without initializing untouched defaults;
    /// bulk mutation materializes them, and history releases unused capacity.
    pub fn compact_history(&mut self) {
        if self.retained.is_some() {
            return;
        }
        let explicit = self
            .cells
            .iter()
            .rposition(|cell| cell != &*EMPTY_CELL)
            .map_or(0, |index| index + 1);
        self.cells.truncate(explicit);
        self.cells.shrink_to_fit();
    }

    fn materialize(&mut self) {
        self.cells
            .resize(usize::from(self.cols), crate::cell::Cell::default());
    }

    pub fn content_length(&self) -> usize {
        self.cells
            .iter()
            .rposition(|cell| cell.has_contents() || cell.is_wide_continuation())
            .map_or(0, |index| index + 1)
    }

    pub fn get(&self, col: u16) -> Option<&crate::cell::Cell> {
        if col >= self.cols {
            None
        } else {
            Some(self.cells.get(usize::from(col)).unwrap_or(&EMPTY_CELL))
        }
    }

    pub fn get_mut(&mut self, col: u16) -> Option<&mut crate::cell::Cell> {
        if col >= self.cols {
            return None;
        }
        let required = usize::from(col) + 1;
        if self.cells.len() < required {
            self.cells.resize(required, crate::cell::Cell::default());
        }
        self.cells.get_mut(usize::from(col))
    }

    pub fn erase(&mut self, i: u16, attrs: crate::attrs::Attrs) {
        self.clear_wide(i, attrs);
        self.get_mut(i).unwrap().clear(attrs);
        // Erasing cells does not undo the already observed automatic line
        // continuation. Public xterm retains that relationship after ECH.
    }

    pub fn resize(&mut self, len: u16, cell: crate::cell::Cell) {
        self.materialize();
        self.cols = len;
        self.retained = None;
        self.cells.resize(usize::from(len), cell);
        self.wrapped = false;
        // A clipped wide pair cannot leave a leading half at the right edge:
        // every drawing/erase/formatter consumer relies on its next cell.
        if let Some(last_cell) = self.cells.last_mut() {
            if last_cell.is_wide() {
                last_cell.clear(*last_cell.attrs());
            }
        }
    }

    /// Retain alternate-buffer cells hidden by a narrower viewport. Drawing
    /// sees the real wide lead at the right edge even when its continuation is
    /// outside the viewport. Growing exposes an untouched retained pair again.
    pub fn resize_retaining(&mut self, cols: u16) {
        self.materialize();
        let mut retained = self.retained.take().unwrap_or_default();
        retained.resize(
            retained.len().max(self.cells.len()),
            crate::cell::Cell::default(),
        );
        for (index, cell) in self.cells.iter().enumerate() {
            retained[index] = cell.clone();
        }
        // A real growth resizes the alternate physical lines to the requested
        // width. It consumes retained cells within that width and crops any
        // still farther cells, matching the public emulator's resize contract.
        if cols > self.cols() {
            // Actual physical growth can crop a farther continuation while
            // retaining its right-edge lead. The public alternate buffer keeps
            // that lead; later growth fills the discarded partner with blank.
            retained.truncate(usize::from(cols));
        }
        retained.resize(
            retained.len().max(usize::from(cols)),
            crate::cell::Cell::default(),
        );
        self.cells = retained[..usize::from(cols)].to_vec();
        self.cols = cols;
        if retained.len() > usize::from(cols) {
            self.retained = Some(retained);
        }
    }

    /// ICH/DCH shift the complete retained physical line in one pass; count is
    /// clipped to that finite line, independent of the current view width.
    pub fn edit_cells(&mut self, col: u16, count: u16, attrs: crate::attrs::Attrs, insert: bool) {
        let visible = self.cols();
        let physical = self.retained_cols();
        self.resize_retaining(physical);
        let start = usize::from(col);
        let count = usize::from(count).min(self.cells.len().saturating_sub(start));
        let length = self.cells.len();
        let mut fill = crate::cell::Cell::default();
        fill.clear(attrs);
        if insert {
            self.cells
                .splice(start..start, std::iter::repeat_n(fill.clone(), count));
            self.cells.truncate(length);
        } else {
            self.cells.drain(start..start + count);
            self.cells.resize(length, fill);
        }
        for i in 0..self.cells.len() {
            if (self.cells[i].is_wide()
                && (i + 1 == self.cells.len() || !self.cells[i + 1].is_wide_continuation()))
                || (self.cells[i].is_wide_continuation()
                    && (i == 0 || !self.cells[i - 1].is_wide()))
            {
                self.cells[i].clear(attrs);
            }
        }
        self.resize_retaining(visible);
    }

    pub fn wrap(&mut self, wrap: bool) {
        self.wrapped = wrap;
    }

    pub fn wrapped(&self) -> bool {
        self.wrapped
    }

    /// The alternate viewport may end on a wide lead whose continuation is
    /// retained off-right. Pair cleanup must reach that real physical cell,
    /// without making ordinary drawing positions extend beyond the viewport.
    pub fn wide_partner_mut(&mut self, col: u16) -> Option<&mut crate::cell::Cell> {
        let cell = self.get(col)?;
        let other = if cell.is_wide() {
            usize::from(col) + 1
        } else if cell.is_wide_continuation() {
            usize::from(col).checked_sub(1)?
        } else {
            return None;
        };
        if other < usize::from(self.cols) {
            self.get_mut(u16::try_from(other).unwrap())
        } else {
            self.retained
                .as_mut()
                .and_then(|cells| cells.get_mut(other))
        }
    }

    pub fn clear_wide(&mut self, col: u16, attrs: crate::attrs::Attrs) {
        if let Some(other) = self.wide_partner_mut(col) {
            other.clear(attrs);
        }
    }

    pub fn write_contents(&self, contents: &mut String, start: u16, width: u16, wrapping: bool) {
        let mut prev_was_wide = false;

        let mut prev_col = start;
        for (col, cell) in self
            .cells()
            .enumerate()
            .skip(usize::from(start))
            .take(usize::from(width))
        {
            if prev_was_wide {
                prev_was_wide = false;
                continue;
            }
            prev_was_wide = cell.is_wide();

            // we limit the number of cols to a u16 (see Size)
            let col: u16 = col.try_into().unwrap();
            if cell.has_contents() {
                for _ in 0..(col - prev_col) {
                    contents.push(' ');
                }
                prev_col += col - prev_col;

                contents.push_str(&cell.contents());
                prev_col += if cell.is_wide() { 2 } else { 1 };
            }
        }
        if prev_col == start && wrapping {
            contents.push('\n');
        }
    }

    pub fn write_contents_formatted(
        &self,
        contents: &mut Vec<u8>,
        start: u16,
        width: u16,
        row: u16,
        wrapping: bool,
        prev_pos: Option<crate::grid::Pos>,
        prev_attrs: Option<crate::attrs::Attrs>,
    ) -> (crate::grid::Pos, crate::attrs::Attrs) {
        let mut prev_was_wide = false;
        let default_cell = crate::cell::Cell::default();

        let mut prev_pos = prev_pos.unwrap_or_else(|| {
            if wrapping {
                crate::grid::Pos {
                    row: row - 1,
                    col: self.cols(),
                }
            } else {
                crate::grid::Pos { row, col: start }
            }
        });
        let mut prev_attrs = prev_attrs.unwrap_or_default();

        let first_cell = self.get(start).unwrap();
        if wrapping && first_cell == &default_cell {
            let default_attrs = default_cell.attrs();
            if &prev_attrs != default_attrs {
                default_attrs.write_escape_code_diff(contents, &prev_attrs);
                prev_attrs = *default_attrs;
            }
            contents.push(b' ');
            crate::term::Backspace::default().write_buf(contents);
            crate::term::EraseChar::new(1).write_buf(contents);
            prev_pos = crate::grid::Pos { row, col: 0 };
        }

        let mut erase: Option<(u16, &crate::attrs::Attrs)> = None;
        for (col, cell) in self
            .cells()
            .enumerate()
            .skip(usize::from(start))
            .take(usize::from(width))
        {
            if prev_was_wide {
                prev_was_wide = false;
                continue;
            }
            prev_was_wide = cell.is_wide();

            // we limit the number of cols to a u16 (see Size)
            let col: u16 = col.try_into().unwrap();
            let pos = crate::grid::Pos { row, col };

            if let Some((prev_col, attrs)) = erase {
                if cell.has_contents() || cell.attrs() != attrs {
                    let new_pos = crate::grid::Pos { row, col: prev_col };
                    if wrapping && prev_pos.row + 1 == new_pos.row && prev_pos.col >= self.cols() {
                        if new_pos.col > 0 {
                            contents.extend(" ".repeat(usize::from(new_pos.col)).as_bytes());
                        } else {
                            contents.extend(b" ");
                            crate::term::Backspace::default().write_buf(contents);
                        }
                    } else {
                        crate::term::MoveFromTo::new(prev_pos, new_pos).write_buf(contents);
                    }
                    prev_pos = new_pos;
                    if &prev_attrs != attrs {
                        attrs.write_escape_code_diff(contents, &prev_attrs);
                        prev_attrs = *attrs;
                    }
                    crate::term::EraseChar::new(pos.col - prev_col).write_buf(contents);
                    erase = None;
                }
            }

            if cell != &default_cell {
                let attrs = cell.attrs();
                if cell.has_contents() {
                    if pos != prev_pos {
                        if !wrapping
                            || prev_pos.row + 1 != pos.row
                            || prev_pos.col < self.cols() - u16::from(cell.is_wide())
                            || pos.col != 0
                        {
                            crate::term::MoveFromTo::new(prev_pos, pos).write_buf(contents);
                        }
                        prev_pos = pos;
                    }

                    if &prev_attrs != attrs {
                        attrs.write_escape_code_diff(contents, &prev_attrs);
                        prev_attrs = *attrs;
                    }

                    prev_pos.col += if cell.is_wide() { 2 } else { 1 };
                    let cell_contents = cell.contents();
                    contents.extend(cell_contents.as_bytes());
                } else if erase.is_none() {
                    erase = Some((pos.col, attrs));
                }
            }
        }
        if let Some((prev_col, attrs)) = erase {
            let new_pos = crate::grid::Pos { row, col: prev_col };
            if wrapping && prev_pos.row + 1 == new_pos.row && prev_pos.col >= self.cols() {
                if new_pos.col > 0 {
                    contents.extend(" ".repeat(usize::from(new_pos.col)).as_bytes());
                } else {
                    contents.extend(b" ");
                    crate::term::Backspace::default().write_buf(contents);
                }
            } else {
                crate::term::MoveFromTo::new(prev_pos, new_pos).write_buf(contents);
            }
            prev_pos = new_pos;
            if &prev_attrs != attrs {
                attrs.write_escape_code_diff(contents, &prev_attrs);
                prev_attrs = *attrs;
            }
            crate::term::ClearRowForward::default().write_buf(contents);
        }

        (prev_pos, prev_attrs)
    }

    // while it's true that most of the logic in this is identical to
    // write_contents_formatted, i can't figure out how to break out the
    // common parts without making things noticeably slower.
    pub fn write_contents_diff(
        &self,
        contents: &mut Vec<u8>,
        prev: &Self,
        start: u16,
        width: u16,
        row: u16,
        wrapping: bool,
        prev_wrapping: bool,
        mut prev_pos: crate::grid::Pos,
        mut prev_attrs: crate::attrs::Attrs,
    ) -> (crate::grid::Pos, crate::attrs::Attrs) {
        let mut prev_was_wide = false;

        let first_cell = self.get(start).unwrap();
        let prev_first_cell = prev.get(start).unwrap();
        if wrapping
            && !prev_wrapping
            && first_cell == prev_first_cell
            && prev_pos.row + 1 == row
            && prev_pos.col >= self.cols() - u16::from(prev_first_cell.is_wide())
        {
            let first_cell_attrs = first_cell.attrs();
            if &prev_attrs != first_cell_attrs {
                first_cell_attrs.write_escape_code_diff(contents, &prev_attrs);
                prev_attrs = *first_cell_attrs;
            }
            let mut cell_contents = prev_first_cell.contents();
            let need_erase = if cell_contents.is_empty() {
                cell_contents = " ".to_string();
                true
            } else {
                false
            };
            contents.extend(cell_contents.as_bytes());
            crate::term::Backspace::default().write_buf(contents);
            if prev_first_cell.is_wide() {
                crate::term::Backspace::default().write_buf(contents);
            }
            if need_erase {
                crate::term::EraseChar::new(1).write_buf(contents);
            }
            prev_pos = crate::grid::Pos { row, col: 0 };
        }

        let mut erase: Option<(u16, &crate::attrs::Attrs)> = None;
        for (col, (cell, prev_cell)) in self
            .cells()
            .zip(prev.cells())
            .enumerate()
            .skip(usize::from(start))
            .take(usize::from(width))
        {
            if prev_was_wide {
                prev_was_wide = false;
                continue;
            }
            prev_was_wide = cell.is_wide();

            // we limit the number of cols to a u16 (see Size)
            let col: u16 = col.try_into().unwrap();
            let pos = crate::grid::Pos { row, col };

            if let Some((prev_col, attrs)) = erase {
                if cell.has_contents() || cell.attrs() != attrs {
                    let new_pos = crate::grid::Pos { row, col: prev_col };
                    if wrapping && prev_pos.row + 1 == new_pos.row && prev_pos.col >= self.cols() {
                        if new_pos.col > 0 {
                            contents.extend(" ".repeat(usize::from(new_pos.col)).as_bytes());
                        } else {
                            contents.extend(b" ");
                            crate::term::Backspace::default().write_buf(contents);
                        }
                    } else {
                        crate::term::MoveFromTo::new(prev_pos, new_pos).write_buf(contents);
                    }
                    prev_pos = new_pos;
                    if &prev_attrs != attrs {
                        attrs.write_escape_code_diff(contents, &prev_attrs);
                        prev_attrs = *attrs;
                    }
                    crate::term::EraseChar::new(pos.col - prev_col).write_buf(contents);
                    erase = None;
                }
            }

            if cell != prev_cell {
                let attrs = cell.attrs();
                if cell.has_contents() {
                    if pos != prev_pos {
                        if !wrapping
                            || prev_pos.row + 1 != pos.row
                            || prev_pos.col < self.cols() - u16::from(cell.is_wide())
                            || pos.col != 0
                        {
                            crate::term::MoveFromTo::new(prev_pos, pos).write_buf(contents);
                        }
                        prev_pos = pos;
                    }

                    if &prev_attrs != attrs {
                        attrs.write_escape_code_diff(contents, &prev_attrs);
                        prev_attrs = *attrs;
                    }

                    prev_pos.col += if cell.is_wide() { 2 } else { 1 };
                    contents.extend(cell.contents().as_bytes());
                } else if erase.is_none() {
                    erase = Some((pos.col, attrs));
                }
            }
        }
        if let Some((prev_col, attrs)) = erase {
            let new_pos = crate::grid::Pos { row, col: prev_col };
            if wrapping && prev_pos.row + 1 == new_pos.row && prev_pos.col >= self.cols() {
                if new_pos.col > 0 {
                    contents.extend(" ".repeat(usize::from(new_pos.col)).as_bytes());
                } else {
                    contents.extend(b" ");
                    crate::term::Backspace::default().write_buf(contents);
                }
            } else {
                crate::term::MoveFromTo::new(prev_pos, new_pos).write_buf(contents);
            }
            prev_pos = new_pos;
            if &prev_attrs != attrs {
                attrs.write_escape_code_diff(contents, &prev_attrs);
                prev_attrs = *attrs;
            }
            crate::term::ClearRowForward::default().write_buf(contents);
        }

        // if this row is going from wrapped to not wrapped, we need to erase
        // and redraw the last character to break wrapping. if this row is
        // wrapped, we need to redraw the last character without erasing it to
        // position the cursor after the end of the line correctly so that
        // drawing the next line can just start writing and be wrapped.
        if (!self.wrapped && prev.wrapped) || (!prev.wrapped && self.wrapped) {
            let end_pos = if self.get(self.cols() - 1).unwrap().is_wide_continuation() {
                crate::grid::Pos {
                    row,
                    col: self.cols() - 2,
                }
            } else {
                crate::grid::Pos {
                    row,
                    col: self.cols() - 1,
                }
            };
            crate::term::MoveFromTo::new(prev_pos, end_pos).write_buf(contents);
            prev_pos = end_pos;
            if !self.wrapped {
                crate::term::EraseChar::new(1).write_buf(contents);
            }
            let end_cell = self.get(end_pos.col).unwrap();
            if end_cell.has_contents() {
                let attrs = end_cell.attrs();
                if &prev_attrs != attrs {
                    attrs.write_escape_code_diff(contents, &prev_attrs);
                    prev_attrs = *attrs;
                }
                contents.extend(end_cell.contents().as_bytes());
                prev_pos.col += if end_cell.is_wide() { 2 } else { 1 };
            }
        }

        (prev_pos, prev_attrs)
    }
}
