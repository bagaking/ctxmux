use crate::term::BufWrite as _;

#[derive(Clone, Debug)]
pub struct Grid {
    size: Size,
    normal: bool,
    first_row_continuation: bool,
    erase_attrs: crate::attrs::Attrs,
    pos: Pos,
    saved_pos: Pos,
    saved_row_absolute: usize,
    rows: Vec<crate::row::Row>,
    scroll_top: u16,
    scroll_bottom: u16,
    origin_mode: bool,
    saved_origin_mode: bool,
    scrollback: std::collections::VecDeque<crate::row::Row>,
    scrollback_len: usize,
    scrollback_offset: usize,
}

impl Grid {
    pub fn new(size: Size, scrollback_len: usize, normal: bool) -> Self {
        Self {
            size,
            normal,
            first_row_continuation: false,
            erase_attrs: crate::attrs::Attrs::default(),
            pos: Pos::default(),
            saved_pos: Pos::default(),
            saved_row_absolute: 0,
            rows: vec![],
            scroll_top: 0,
            scroll_bottom: size.rows - 1,
            origin_mode: false,
            saved_origin_mode: false,
            scrollback: std::collections::VecDeque::new(),
            scrollback_len,
            scrollback_offset: 0,
        }
    }

    pub fn set_erase_attrs(&mut self, attrs: crate::attrs::Attrs) {
        self.erase_attrs = attrs;
    }
    pub fn allocate_rows(&mut self) {
        if self.rows.is_empty() {
            let attrs = if self.normal {
                crate::attrs::Attrs::default()
            } else {
                self.erase_attrs
            };
            self.rows = (0..self.size.rows)
                .map(|_| {
                    let mut row = crate::row::Row::new(self.size.cols);
                    row.clear(attrs);
                    row
                })
                .collect();
        }
    }
    fn new_row(&self) -> crate::row::Row {
        let mut row = crate::row::Row::new(self.size.cols);
        row.clear(self.erase_attrs);
        row
    }

    pub fn clear(&mut self) {
        self.clear_contents();
        self.saved_pos = Pos::default();
        self.saved_row_absolute = 0;
        self.saved_origin_mode = false;
    }

    pub fn clear_contents(&mut self) {
        self.pos = Pos::default();
        let erase_attrs = self.erase_attrs;
        for row in self.drawing_rows_mut() {
            row.clear(erase_attrs);
        }
        self.scroll_top = 0;
        self.scroll_bottom = self.size.rows - 1;
        self.origin_mode = false;
    }

    pub fn size(&self) -> Size {
        self.size
    }

    pub fn set_size(&mut self, size: Size) {
        if size == self.size {
            return;
        }
        // A larger viewport can expose retained normal rows above a cursor at
        // the bottom. Move each row once; repeated front insertion would make
        // a large legal geometry change quadratic in the viewport height.
        let old_rows = self.rows.len();
        let new_rows = usize::from(size.rows);
        if new_rows > old_rows {
            let additional = new_rows - old_rows;
            let pulled = if usize::from(self.pos.row) + 1 == old_rows {
                additional.min(self.scrollback.len())
            } else {
                0
            };
            let mut rows = Vec::with_capacity(new_rows);
            for _ in 0..pulled {
                rows.push(self.scrollback.pop_back().unwrap());
            }
            rows.reverse();
            rows.append(&mut self.rows);
            rows.extend(
                std::iter::repeat_with(|| crate::row::Row::new(size.cols))
                    .take(additional - pulled),
            );
            self.rows = rows;
            self.pos.row += u16::try_from(pulled).unwrap();
        } else {
            // Keep the cursor's row when shrinking. Rows above it become normal
            // history; rows below the cursor are the cropped viewport suffix.
            let removed = old_rows - new_rows;
            let below = old_rows.saturating_sub(usize::from(self.pos.row) + 1);
            let cropped = removed.min(below);
            self.rows.truncate(old_rows - cropped);
            let historical = removed - cropped;
            if self.scrollback_len > 0 {
                self.scrollback
                    .extend(self.rows.drain(..historical).map(|mut row| {
                        row.compact_history();
                        row
                    }));
                while self.scrollback.len() > self.scrollback_len {
                    self.first_row_continuation = self.scrollback.pop_front().unwrap().wrapped();
                    self.saved_row_absolute = self.saved_row_absolute.saturating_sub(1);
                }
            } else {
                if historical > 0 && self.normal {
                    self.first_row_continuation = self.rows[historical - 1].wrapped();
                }
                self.rows.drain(..historical);
            }
            self.pos.row = self
                .pos
                .row
                .saturating_sub(u16::try_from(historical).unwrap());
        }
        if size.cols != self.size.cols && self.normal {
            self.reflow_columns(size.cols, size.rows);
        }
        self.saved_pos.row = u16::try_from(
            self.saved_row_absolute
                .saturating_sub(self.scrollback.len())
                .min(usize::from(size.rows - 1)),
        )
        .unwrap();
        if size.cols != self.size.cols {
            self.saved_pos.col = self.saved_pos.col.min(size.cols - 1);
        }
        for row in self.scrollback.iter_mut().chain(self.rows.iter_mut()) {
            let wrapped = row.wrapped();
            if self.normal {
                row.resize(size.cols, crate::cell::Cell::default());
            } else {
                row.resize_retaining(size.cols);
            }
            row.wrap(wrapped);
        }
        self.size = size;
        self.scroll_top = 0;
        self.scroll_bottom = size.rows - 1;
        self.scrollback_offset = self.scrollback_offset.min(self.scrollback.len());
        self.row_clamp_top(false);
        self.row_clamp_bottom(false);
        self.col_clamp();
    }

    fn reflow_columns(&mut self, cols: u16, rows: u16) {
        let old_cols = usize::from(self.size.cols);
        let old_history = self.scrollback.len();
        let old_cursor = old_history + usize::from(self.pos.row);
        let mut input: std::collections::VecDeque<_> = self
            .scrollback
            .drain(..)
            .chain(self.rows.drain(..))
            .collect();
        let old_length = input.len();
        let mut output = Vec::with_capacity(old_length);
        let shrinking = usize::from(cols) < old_cols;
        let mut consumed = 0;
        let mut shrinking_cursor = old_cursor;
        while !input.is_empty() {
            let mut group = Vec::new();
            let start;
            let end;
            if shrinking {
                // Shrink processes logical lines from the bottom. Reflowing a
                // later line moves the actual cursor's absolute buffer row,
                // which changes whether an earlier line is cursor-owned.
                group.push(input.pop_back().unwrap());
                while input.back().is_some_and(crate::row::Row::wrapped) {
                    group.push(input.pop_back().unwrap());
                }
                group.reverse();
                start = input.len();
                end = start + group.len();
            } else {
                loop {
                    let row = input.pop_front().unwrap();
                    let continues = row.wrapped() && !input.is_empty();
                    group.push(row);
                    if !continues {
                        break;
                    }
                }
                start = consumed;
                consumed += group.len();
                end = consumed;
            }
            let cursor = if shrinking {
                shrinking_cursor
            } else {
                old_cursor
            };
            // Like the mature target, preserve the logical line containing the
            // current cursor; the running application owns its redraw.
            if cursor >= start && cursor < end {
                if shrinking {
                    output.extend(group.into_iter().rev());
                } else {
                    output.extend(group);
                }
                continue;
            }
            let output_start = output.len();
            let old_group_length = group.len();
            let last = group.len() - 1;
            let mut cells = Vec::new();
            for (index, row) in group.into_iter().enumerate() {
                let count = if index == last {
                    row.content_length()
                } else {
                    old_cols
                };
                cells.extend(row.into_cells().into_iter().take(count));
            }
            let mut row = crate::row::Row::new(cols);
            let mut col = 0;
            let mut cells = cells.into_iter().peekable();
            while let Some(cell) = cells.next() {
                if cell.is_wide() && cols == 1 {
                    // The existing positive one-column rendering boundary
                    // cannot represent a wide pair; original Run bytes remain.
                    if cells.peek().is_some_and(|next| next.is_wide_continuation()) {
                        cells.next();
                    }
                    continue;
                }
                if col == cols || (cell.is_wide() && col + 1 == cols) {
                    row.wrap(true);
                    output.push(row);
                    row = crate::row::Row::new(cols);
                    col = 0;
                }
                *row.get_mut(col).unwrap() = cell;
                col += 1;
            }
            output.push(row);
            if shrinking {
                let added = (output.len() - output_start).saturating_sub(old_group_length);
                shrinking_cursor = shrinking_cursor
                    .saturating_add(added)
                    .min(self.scrollback_len.saturating_add(usize::from(rows - 1)));
                output[output_start..].reverse();
            }
        }
        if shrinking {
            output.reverse();
        }
        let new_length = output.len();
        let mut history;
        if new_length < old_length {
            let removed = old_length - new_length;
            self.saved_row_absolute = self.saved_row_absolute.saturating_sub(removed);
            history = old_history.saturating_sub(removed);
            self.pos.row = self
                .pos
                .row
                .saturating_sub(u16::try_from(removed.saturating_sub(old_history)).unwrap());
            output.extend(
                std::iter::repeat_with(|| crate::row::Row::new(cols))
                    .take(usize::from(rows).saturating_sub(output.len())),
            );
        } else {
            let additional = new_length - old_length;
            let viewport_room = if old_history == 0 {
                usize::from(rows - 1 - self.pos.row.min(rows - 1))
            } else {
                0
            };
            let in_view = additional.min(viewport_room);
            if in_view > 0 {
                output.truncate(output.len() - in_view);
                self.pos.row += u16::try_from(in_view).unwrap();
            }
            history = old_history + additional - in_view;
            if additional > 0 {
                self.saved_row_absolute =
                    (self.saved_row_absolute + additional).min(history + usize::from(rows - 1));
            }
        }
        // Preserve the existing configured history policy, without another cap.
        let evicted = history.saturating_sub(self.scrollback_len);
        if evicted > 0 {
            self.first_row_continuation = output[evicted - 1].wrapped();
            output.drain(..evicted);
            self.saved_row_absolute = self.saved_row_absolute.saturating_sub(evicted);
            history -= evicted;
        }
        self.rows = output.split_off(history);
        self.scrollback.extend(output.into_iter().map(|mut row| {
            row.compact_history();
            row
        }));
    }

    pub fn set_scrollback_limit(&mut self, limit: usize) -> usize {
        self.scrollback_len = limit;
        let evicted = self.scrollback.len().saturating_sub(limit);
        for _ in 0..evicted {
            self.first_row_continuation = self.scrollback.pop_front().unwrap().wrapped();
        }
        self.saved_row_absolute = self.saved_row_absolute.saturating_sub(evicted);
        self.scrollback_offset = self.scrollback_offset.min(self.scrollback.len());
        evicted
    }

    pub fn restore_scrollback_budget(&self) -> Option<usize> {
        (self.normal && self.first_row_continuation).then_some(self.scrollback.len())
    }

    pub fn retained_cols(&self) -> u16 {
        self.rows
            .iter()
            .map(crate::row::Row::retained_cols)
            .max()
            .unwrap_or(self.size.cols)
    }

    pub fn pos(&self) -> Pos {
        self.pos
    }

    /// A buffer activation carries the drawing cursor and current origin mode;
    /// saved return registers remain owned by their original grid.
    pub fn inherit_cursor(&mut self, source: &Self) {
        self.pos = source.pos;
        self.origin_mode = source.origin_mode;
    }

    pub fn set_pos(&mut self, mut pos: Pos) {
        if self.origin_mode {
            pos.row = pos.row.saturating_add(self.scroll_top);
        }
        self.pos = pos;
        self.row_clamp_top(self.origin_mode);
        self.row_clamp_bottom(self.origin_mode);
        self.col_clamp();
    }

    pub fn save_cursor(&mut self) {
        self.saved_pos = self.pos;
        self.saved_row_absolute = self.scrollback.len() + usize::from(self.pos.row);
        self.saved_origin_mode = self.origin_mode;
    }

    pub fn restore_cursor(&mut self) {
        self.pos = self.saved_pos;
        self.pos.row = u16::try_from(
            self.saved_row_absolute
                .saturating_sub(self.scrollback.len())
                .min(usize::from(self.size.rows - 1)),
        )
        .unwrap();
        self.origin_mode = self.saved_origin_mode;
    }

    pub fn visible_rows(&self) -> impl Iterator<Item = &crate::row::Row> {
        let scrollback_len = self.scrollback.len();
        let rows_len = self.rows.len();
        self.scrollback
            .iter()
            .skip(scrollback_len - self.scrollback_offset)
            .take(rows_len)
            .chain(
                self.rows
                    .iter()
                    .take(rows_len.saturating_sub(self.scrollback_offset)),
            )
    }

    pub fn drawing_rows(&self) -> impl Iterator<Item = &crate::row::Row> {
        self.rows.iter()
    }

    pub fn drawing_rows_mut(&mut self) -> impl Iterator<Item = &mut crate::row::Row> {
        self.rows.iter_mut()
    }

    pub fn visible_row(&self, row: u16) -> Option<&crate::row::Row> {
        self.visible_rows().nth(usize::from(row))
    }

    pub fn drawing_row(&self, row: u16) -> Option<&crate::row::Row> {
        self.drawing_rows().nth(usize::from(row))
    }

    pub fn drawing_row_mut(&mut self, row: u16) -> Option<&mut crate::row::Row> {
        self.drawing_rows_mut().nth(usize::from(row))
    }

    pub fn current_row_mut(&mut self) -> &mut crate::row::Row {
        self.drawing_row_mut(self.pos.row)
            // we assume self.pos.row is always valid
            .unwrap()
    }

    pub fn visible_cell(&self, pos: Pos) -> Option<&crate::cell::Cell> {
        self.visible_row(pos.row).and_then(|r| r.get(pos.col))
    }

    pub fn drawing_cell(&self, pos: Pos) -> Option<&crate::cell::Cell> {
        self.drawing_row(pos.row).and_then(|r| r.get(pos.col))
    }

    pub fn drawing_cell_mut(&mut self, pos: Pos) -> Option<&mut crate::cell::Cell> {
        self.drawing_row_mut(pos.row)
            .and_then(|r| r.get_mut(pos.col))
    }

    pub fn scrollback_len(&self) -> usize {
        self.scrollback_len
    }

    pub fn scrollback(&self) -> usize {
        self.scrollback_offset
    }

    pub fn set_scrollback(&mut self, rows: usize) {
        self.scrollback_offset = rows.min(self.scrollback.len());
    }

    pub fn write_contents(&self, contents: &mut String) {
        let mut wrapping = false;
        for row in self.visible_rows() {
            row.write_contents(contents, 0, self.size.cols, wrapping);
            if !row.wrapped() {
                contents.push('\n');
            }
            wrapping = row.wrapped();
        }

        while contents.ends_with('\n') {
            contents.truncate(contents.len() - 1);
        }
    }

    pub fn write_contents_formatted(&self, contents: &mut Vec<u8>) -> crate::attrs::Attrs {
        crate::term::ClearAttrs::default().write_buf(contents);
        crate::term::ClearScreen::default().write_buf(contents);

        let mut prev_attrs = crate::attrs::Attrs::default();
        let mut prev_pos = Pos::default();
        let mut wrapping = false;
        for (i, row) in self.visible_rows().enumerate() {
            // we limit the number of cols to a u16 (see Size), so
            // visible_rows() can never return more rows than will fit
            let i = i.try_into().unwrap();
            let (new_pos, new_attrs) = row.write_contents_formatted(
                contents,
                0,
                self.size.cols,
                i,
                wrapping,
                Some(prev_pos),
                Some(prev_attrs),
            );
            prev_pos = new_pos;
            prev_attrs = new_attrs;
            wrapping = row.wrapped();
        }

        self.write_cursor_position_formatted(contents, Some(prev_pos), Some(prev_attrs));

        prev_attrs
    }

    /// Paint actual retained rows at the final grid. Synthetic wrap helpers are
    /// erased before completion; they are never original Run output.
    pub fn write_full_contents_formatted(&self, contents: &mut Vec<u8>) {
        crate::term::ClearAttrs::default().write_buf(contents);
        crate::term::ClearScreen::default().write_buf(contents);
        let bottom = self.size.rows - 1;
        let mut attrs = crate::attrs::Attrs::default();
        let mut pos = Pos::default();
        let mut rows = self.scrollback.iter().chain(&self.rows).peekable();
        let mut row_index = 0;
        if self.normal && self.first_row_continuation {
            // This preceding row is an encoder control, not retained output.
            // The declared restore history count evicts it during this seed.
            contents.extend(std::iter::repeat_n(b'-', usize::from(self.size.cols) + 1));
            contents.extend_from_slice(b"\x1b[1D\x1b[1X");
            row_index = 1.min(bottom);
            pos = Pos {
                row: row_index,
                col: 0,
            };
        }
        while let Some(row) = rows.next() {
            let painted = row.write_contents_formatted(
                contents,
                0,
                self.size.cols,
                row_index,
                false,
                Some(pos),
                Some(attrs),
            );
            pos = painted.0;
            attrs = painted.1;
            if rows.peek().is_none() {
                break;
            }
            if row.wrapped() {
                let last_content = (0..self.size.cols)
                    .rev()
                    .find(|col| {
                        row.get(*col)
                            .is_some_and(|cell| cell.has_contents() || cell.is_wide_continuation())
                    })
                    .map_or(0, |col| col + 1);
                let padding = self.size.cols - last_content;
                if padding > 0 {
                    crate::term::MoveTo::new(Pos {
                        row: row_index,
                        col: last_content,
                    })
                    .write_buf(contents);
                }
                // A real wide lead can perform its own overflow without a
                // temporary padding glyph. This also works in one row, where
                // the previous row is already inaccessible in history.
                let next_lead = rows.peek().and_then(|next| next.get(0));
                if padding == 1 && next_lead.is_some_and(|cell| cell.is_wide()) {
                    let lead = next_lead.unwrap();
                    if row.get(self.size.cols - 1).unwrap().attrs() == lead.attrs() {
                        lead.attrs().write_escape_code_diff(contents, &attrs);
                        contents.extend_from_slice(lead.contents().as_bytes());
                        let next_row = (row_index + 1).min(bottom);
                        crate::term::MoveTo::new(Pos {
                            row: next_row,
                            col: 0,
                        })
                        .write_buf(contents);
                        row_index = next_row;
                        pos = Pos {
                            row: next_row,
                            col: 0,
                        };
                        attrs = *lead.attrs();
                        continue;
                    }
                }
                crate::term::ClearAttrs::default().write_buf(contents);
                // Existing serialize-addon uses the same real-wrap/erase idea.
                // Width minus content plus one is exactly the cursor overflow.
                contents.extend(std::iter::repeat_n(b'-', usize::from(padding) + 1));
                contents.extend_from_slice(b"\x1b[1D\x1b[1X");
                let next_row = (row_index + 1).min(bottom);
                if padding > 0 && self.size.rows > 1 {
                    let previous_row = if row_index == bottom {
                        bottom - 1
                    } else {
                        row_index
                    };
                    for col in last_content..self.size.cols {
                        crate::term::MoveTo::new(Pos {
                            row: previous_row,
                            col,
                        })
                        .write_buf(contents);
                        let cell = row.get(col).unwrap();
                        cell.attrs()
                            .write_escape_code_diff(contents, &crate::attrs::Attrs::default());
                        crate::term::EraseChar::new(1).write_buf(contents);
                        crate::term::ClearAttrs::default().write_buf(contents);
                    }
                    crate::term::MoveTo::new(Pos {
                        row: next_row,
                        col: 0,
                    })
                    .write_buf(contents);
                }
                row_index = next_row;
                pos = Pos {
                    row: next_row,
                    col: 0,
                };
                attrs = crate::attrs::Attrs::default();
            } else {
                crate::term::MoveTo::new(Pos {
                    row: row_index,
                    col: 0,
                })
                .write_buf(contents);
                crate::term::ClearAttrs::default().write_buf(contents);
                attrs = crate::attrs::Attrs::default();
                crate::term::Crlf::default().write_buf(contents);
                row_index = (row_index + 1).min(bottom);
                pos = Pos {
                    row: row_index,
                    col: 0,
                };
            }
        }
        self.write_cursor_position_formatted(contents, Some(pos), Some(attrs));
    }

    /// Restore margins/origin followed by the absolute drawing cursor.
    pub fn write_drawing_state_formatted(&self, contents: &mut Vec<u8>) {
        self.write_region_formatted(contents, self.origin_mode);
        self.write_cursor_at(contents, None, None, self.pos, self.origin_mode);
    }

    fn write_region_formatted(&self, contents: &mut Vec<u8>, origin: bool) {
        contents.extend_from_slice(
            format!("\x1b[{};{}r", self.scroll_top + 1, self.scroll_bottom + 1).as_bytes(),
        );
        contents.extend_from_slice(if origin { b"\x1b[?6h" } else { b"\x1b[?6l" });
    }

    pub fn write_saved_cursor_formatted(&self, contents: &mut Vec<u8>, attrs: crate::attrs::Attrs) {
        self.write_region_formatted(contents, self.saved_origin_mode);
        let mut saved = self.saved_pos;
        saved.row = u16::try_from(
            self.saved_row_absolute
                .saturating_sub(self.scrollback.len())
                .min(usize::from(self.size.rows - 1)),
        )
        .unwrap();
        self.write_cursor_at(contents, None, None, saved, self.saved_origin_mode);
        crate::term::ClearAttrs::default().write_buf(contents);
        attrs.write_escape_code_diff(contents, &crate::attrs::Attrs::default());
        crate::term::SaveCursor::default().write_buf(contents);
    }

    pub fn write_contents_diff(
        &self,
        contents: &mut Vec<u8>,
        prev: &Self,
        mut prev_attrs: crate::attrs::Attrs,
    ) -> crate::attrs::Attrs {
        let mut prev_pos = prev.pos;
        let mut wrapping = false;
        let mut prev_wrapping = false;
        for (i, (row, prev_row)) in self.visible_rows().zip(prev.visible_rows()).enumerate() {
            // we limit the number of cols to a u16 (see Size), so
            // visible_rows() can never return more rows than will fit
            let i = i.try_into().unwrap();
            let (new_pos, new_attrs) = row.write_contents_diff(
                contents,
                prev_row,
                0,
                self.size.cols,
                i,
                wrapping,
                prev_wrapping,
                prev_pos,
                prev_attrs,
            );
            prev_pos = new_pos;
            prev_attrs = new_attrs;
            wrapping = row.wrapped();
            prev_wrapping = prev_row.wrapped();
        }

        self.write_cursor_position_formatted(contents, Some(prev_pos), Some(prev_attrs));

        prev_attrs
    }

    pub fn write_cursor_position_formatted(
        &self,
        contents: &mut Vec<u8>,
        prev_pos: Option<Pos>,
        prev_attrs: Option<crate::attrs::Attrs>,
    ) {
        self.write_cursor_at(contents, prev_pos, prev_attrs, self.pos, false);
    }

    fn write_cursor_at(
        &self,
        contents: &mut Vec<u8>,
        prev_pos: Option<Pos>,
        prev_attrs: Option<crate::attrs::Attrs>,
        cursor_pos: Pos,
        origin: bool,
    ) {
        // Cells remain addressed absolutely; only terminal CUP coordinates
        // become margin-relative. No full-history clone is needed to position.
        let terminal_pos = |pos: Pos| Pos {
            row: if origin {
                pos.row.saturating_sub(self.scroll_top)
            } else {
                pos.row
            },
            col: pos.col,
        };
        let prev_attrs = prev_attrs.unwrap_or_default();
        // writing a character to the last column of a row doesn't wrap the
        // cursor immediately - it waits until the next character is actually
        // drawn. it is only possible for the cursor to have this kind of
        // position after drawing a character though, so if we end in this
        // position, we need to redraw the character at the end of the row.
        if prev_pos != Some(cursor_pos) && cursor_pos.col >= self.size.cols {
            let mut pos = Pos {
                row: cursor_pos.row,
                col: self.size.cols - 1,
            };
            if self
                .drawing_cell(pos)
                // we assume cursor_pos.row is always valid, and self.size.cols
                // - 1 is always a valid column
                .unwrap()
                .is_wide_continuation()
            {
                pos.col = self.size.cols - 2;
            }
            let cell =
                // we assume cursor_pos.row is always valid, and self.size.cols
                // - 2 must be a valid column because self.size.cols - 1 is
                // always valid and we just checked that the cell at
                // self.size.cols - 1 is a wide continuation character, which
                // means that the first half of the wide character must be
                // before it
                self.drawing_cell(pos).unwrap();
            if cell.has_contents() {
                if let Some(prev_pos) = prev_pos {
                    crate::term::MoveFromTo::new(terminal_pos(prev_pos), terminal_pos(pos))
                        .write_buf(contents);
                } else {
                    crate::term::MoveTo::new(terminal_pos(pos)).write_buf(contents);
                }
                cell.attrs().write_escape_code_diff(contents, &prev_attrs);
                contents.extend(cell.contents().as_bytes());
                prev_attrs.write_escape_code_diff(contents, cell.attrs());
            } else {
                // if the cell doesn't have contents, we can't have gotten
                // here by drawing a character in the last column. this means
                // that as far as i'm aware, we have to have reached here from
                // a newline when we were already after the end of an earlier
                // row. in the case where we are already after the end of an
                // earlier row, we can just write a few newlines, otherwise we
                // also need to do the same as above to get ourselves to after
                // the end of a row.
                let mut found = false;
                for i in (0..cursor_pos.row).rev() {
                    pos.row = i;
                    pos.col = self.size.cols - 1;
                    if self
                        .drawing_cell(pos)
                        // i is always less than cursor_pos.row, which we assume
                        // to be always valid, so it must also be valid.
                        // self.size.cols - 1 is always a valid col.
                        .unwrap()
                        .is_wide_continuation()
                    {
                        pos.col = self.size.cols - 2;
                    }
                    let cell = self
                        .drawing_cell(pos)
                        // i is always less than cursor_pos.row, which we assume
                        // to be always valid, so it must also be valid.
                        // self.size.cols - 2 is valid because self.size.cols
                        // - 1 is always valid, and col gets set to
                        // self.size.cols - 2 when the cell at self.size.cols
                        // - 1 is a wide continuation character, meaning that
                        // the first half of the wide character must be before
                        // it
                        .unwrap();
                    if cell.has_contents() {
                        if let Some(prev_pos) = prev_pos {
                            if prev_pos.row != i || prev_pos.col < self.size.cols {
                                crate::term::MoveFromTo::new(
                                    terminal_pos(prev_pos),
                                    terminal_pos(pos),
                                )
                                .write_buf(contents);
                                cell.attrs().write_escape_code_diff(contents, &prev_attrs);
                                contents.extend(cell.contents().as_bytes());
                                prev_attrs.write_escape_code_diff(contents, cell.attrs());
                            }
                        } else {
                            crate::term::MoveTo::new(terminal_pos(pos)).write_buf(contents);
                            cell.attrs().write_escape_code_diff(contents, &prev_attrs);
                            contents.extend(cell.contents().as_bytes());
                            prev_attrs.write_escape_code_diff(contents, cell.attrs());
                        }
                        contents.extend("\n".repeat(usize::from(cursor_pos.row - i)).as_bytes());
                        found = true;
                        break;
                    }
                }

                // this can happen if you get the cursor off the end of a row,
                // and then do something to clear the end of the current row
                // without moving the cursor (IL, DL, ED, EL, etc). we know
                // there can't be something in the last column because we
                // would have caught that above, so it should be safe to
                // overwrite it.
                if !found {
                    pos = Pos {
                        row: cursor_pos.row,
                        col: self.size.cols - 1,
                    };
                    if let Some(prev_pos) = prev_pos {
                        crate::term::MoveFromTo::new(terminal_pos(prev_pos), terminal_pos(pos))
                            .write_buf(contents);
                    } else {
                        crate::term::MoveTo::new(terminal_pos(pos)).write_buf(contents);
                    }
                    contents.push(b' ');
                    // we know that the cell has no contents, but it still may
                    // have drawing attributes (background color, etc)
                    let end_cell = self
                        .drawing_cell(pos)
                        // we assume cursor_pos.row is always valid, and
                        // self.size.cols - 1 is always a valid column
                        .unwrap();
                    end_cell
                        .attrs()
                        .write_escape_code_diff(contents, &prev_attrs);
                    crate::term::SaveCursor::default().write_buf(contents);
                    crate::term::Backspace::default().write_buf(contents);
                    crate::term::EraseChar::new(1).write_buf(contents);
                    crate::term::RestoreCursor::default().write_buf(contents);
                    prev_attrs.write_escape_code_diff(contents, end_cell.attrs());
                }
            }
        } else if let Some(prev_pos) = prev_pos {
            crate::term::MoveFromTo::new(terminal_pos(prev_pos), terminal_pos(cursor_pos))
                .write_buf(contents);
        } else {
            crate::term::MoveTo::new(terminal_pos(cursor_pos)).write_buf(contents);
        }
    }

    // Row::wrapped is the outgoing relation to the next physical row. Erasing
    // this row clears its incoming relation, not that next row's incoming flag.
    fn clear_incoming_wrap(&mut self, row: u16) {
        if row > 0 {
            self.rows[usize::from(row - 1)].wrap(false);
        } else if let Some(previous) = self.scrollback.back_mut() {
            previous.wrap(false);
        } else {
            self.first_row_continuation = false;
        }
    }

    fn erase_full_row(&mut self, row: u16, attrs: crate::attrs::Attrs) {
        self.clear_incoming_wrap(row);
        let target = &mut self.rows[usize::from(row)];
        let outgoing = target.wrapped();
        target.clear(attrs);
        target.wrap(outgoing);
    }

    pub fn erase_all(&mut self, attrs: crate::attrs::Attrs) {
        for row in 0..self.size.rows {
            self.erase_full_row(row, attrs);
        }
    }

    pub fn erase_all_forward(&mut self, attrs: crate::attrs::Attrs) {
        for row in self.pos.row + 1..self.size.rows {
            self.erase_full_row(row, attrs);
        }
        self.erase_row_forward(attrs);
    }

    pub fn erase_all_backward(&mut self, attrs: crate::attrs::Attrs) {
        for row in 0..self.pos.row {
            self.erase_full_row(row, attrs);
        }
        self.clear_incoming_wrap(self.pos.row);
        self.erase_row_backward(attrs);
    }

    pub fn erase_row(&mut self, attrs: crate::attrs::Attrs) {
        self.erase_full_row(self.pos.row, attrs);
    }

    pub fn erase_row_forward(&mut self, attrs: crate::attrs::Attrs) {
        let size = self.size;
        let pos = self.pos;
        if pos.col == 0 {
            self.clear_incoming_wrap(pos.row);
        }
        let row = self.current_row_mut();
        for col in pos.col..size.cols {
            row.erase(col, attrs);
        }
    }

    pub fn erase_row_backward(&mut self, attrs: crate::attrs::Attrs) {
        let size = self.size;
        let pos = self.pos;
        let row = self.current_row_mut();
        for col in 0..=pos.col.min(size.cols - 1) {
            row.erase(col, attrs);
        }
    }

    pub fn insert_cells(&mut self, count: u16) {
        self.col_clamp();
        let col = self.pos.col;
        let attrs = self.erase_attrs;
        self.current_row_mut().edit_cells(col, count, attrs, true);
    }
    pub fn delete_cells(&mut self, count: u16) {
        self.col_clamp();
        let col = self.pos.col;
        let attrs = self.erase_attrs;
        self.current_row_mut().edit_cells(col, count, attrs, false);
    }

    pub fn erase_cells(&mut self, count: u16, attrs: crate::attrs::Attrs) {
        let size = self.size;
        let pos = self.pos;
        let row = self.current_row_mut();
        for col in pos.col..((pos.col.saturating_add(count)).min(size.cols)) {
            row.erase(col, attrs);
        }
    }

    pub fn insert_lines(&mut self, count: u16) {
        for _ in 0..count {
            self.rows.remove(usize::from(self.scroll_bottom));
            self.rows.insert(usize::from(self.pos.row), self.new_row());
            // self.scroll_bottom is maintained to always be a valid row
            self.rows[usize::from(self.scroll_bottom)].wrap(false);
        }
    }

    pub fn delete_lines(&mut self, count: u16) {
        for _ in 0..(count.min(self.size.rows - self.pos.row)) {
            self.rows
                .insert(usize::from(self.scroll_bottom) + 1, self.new_row());
            self.rows.remove(usize::from(self.pos.row));
        }
    }

    pub fn scroll_up(&mut self, count: u16) {
        for _ in 0..(count.min(self.size.rows - self.scroll_top)) {
            self.rows
                .insert(usize::from(self.scroll_bottom) + 1, self.new_row());
            let removed = self.rows.remove(usize::from(self.scroll_top));
            if self.normal && self.scrollback_len == 0 && !self.scroll_region_active() {
                self.first_row_continuation = removed.wrapped();
            }
            if self.scrollback_len > 0 && !self.scroll_region_active() {
                let mut removed = removed;
                removed.compact_history();
                self.scrollback.push_back(removed);
                while self.scrollback.len() > self.scrollback_len {
                    self.first_row_continuation = self.scrollback.pop_front().unwrap().wrapped();
                    self.saved_row_absolute = self.saved_row_absolute.saturating_sub(1);
                }
                if self.scrollback_offset > 0 {
                    self.scrollback_offset = self.scrollback.len().min(self.scrollback_offset + 1);
                }
            }
        }
    }

    pub fn scroll_down(&mut self, count: u16) {
        for _ in 0..count {
            self.rows.remove(usize::from(self.scroll_bottom));
            self.rows
                .insert(usize::from(self.scroll_top), self.new_row());
            // self.scroll_bottom is maintained to always be a valid row
            self.rows[usize::from(self.scroll_bottom)].wrap(false);
        }
    }

    pub fn set_scroll_region(&mut self, top: u16, bottom: u16) {
        let bottom = bottom.min(self.size().rows - 1);
        if top < bottom {
            self.scroll_top = top;
            self.scroll_bottom = bottom;
        } else {
            self.scroll_top = 0;
            self.scroll_bottom = self.size().rows - 1;
        }
        self.pos.row = self.scroll_top;
        self.pos.col = 0;
    }

    fn in_scroll_region(&self) -> bool {
        self.pos.row >= self.scroll_top && self.pos.row <= self.scroll_bottom
    }

    fn scroll_region_active(&self) -> bool {
        self.scroll_top != 0 || self.scroll_bottom != self.size.rows - 1
    }

    pub fn set_origin_mode(&mut self, mode: bool) {
        self.origin_mode = mode;
        self.set_pos(Pos { row: 0, col: 0 });
    }

    pub fn row_inc_clamp(&mut self, count: u16) {
        let in_scroll_region = self.in_scroll_region();
        self.pos.row = self.pos.row.saturating_add(count);
        self.row_clamp_bottom(in_scroll_region);
    }

    pub fn row_inc_scroll(&mut self, count: u16) -> u16 {
        let in_scroll_region = self.in_scroll_region();
        self.pos.row = self.pos.row.saturating_add(count);
        let lines = self.row_clamp_bottom(in_scroll_region);
        if in_scroll_region {
            self.scroll_up(lines);
            lines
        } else {
            0
        }
    }

    pub fn row_dec_clamp(&mut self, count: u16) {
        let in_scroll_region = self.in_scroll_region();
        self.pos.row = self.pos.row.saturating_sub(count);
        self.row_clamp_top(in_scroll_region);
    }

    pub fn row_dec_scroll(&mut self, count: u16) {
        let in_scroll_region = self.in_scroll_region();
        // need to account for clamping by both row_clamp_top and by
        // saturating_sub
        let extra_lines = if count > self.pos.row {
            count - self.pos.row
        } else {
            0
        };
        self.pos.row = self.pos.row.saturating_sub(count);
        let lines = self.row_clamp_top(in_scroll_region);
        self.scroll_down(lines + extra_lines);
    }

    pub fn row_set(&mut self, i: u16) {
        self.pos.row = i;
        self.row_clamp();
    }

    pub fn col_inc(&mut self, count: u16) {
        self.pos.col = self.pos.col.saturating_add(count);
    }

    pub fn col_inc_clamp(&mut self, count: u16) {
        self.pos.col = self.pos.col.saturating_add(count);
        self.col_clamp();
    }

    pub fn col_dec(&mut self, count: u16) {
        self.pos.col = self.pos.col.saturating_sub(count);
    }

    pub fn col_tab(&mut self) {
        self.pos.col -= self.pos.col % 8;
        self.pos.col += 8;
        self.col_clamp();
    }

    pub fn col_set(&mut self, i: u16) {
        self.pos.col = i;
        self.col_clamp();
    }

    pub fn col_wrap(&mut self, width: u16, wrap: bool) {
        if self.pos.col > self.size.cols - width {
            // Mark the actual row before scrolling can move it to history.
            // In a one-row grid there is no previous drawing row afterwards.
            let can_wrap = self.pos.row < self.size.rows - 1 || self.in_scroll_region();
            self.current_row_mut().wrap(wrap && can_wrap);
            self.pos.col = 0;
            self.row_inc_scroll(1);
        }
    }

    fn row_clamp_top(&mut self, limit_to_scroll_region: bool) -> u16 {
        if limit_to_scroll_region && self.pos.row < self.scroll_top {
            let rows = self.scroll_top - self.pos.row;
            self.pos.row = self.scroll_top;
            rows
        } else {
            0
        }
    }

    fn row_clamp_bottom(&mut self, limit_to_scroll_region: bool) -> u16 {
        let bottom = if limit_to_scroll_region {
            self.scroll_bottom
        } else {
            self.size.rows - 1
        };
        if self.pos.row > bottom {
            let rows = self.pos.row - bottom;
            self.pos.row = bottom;
            rows
        } else {
            0
        }
    }

    fn row_clamp(&mut self) {
        if self.pos.row > self.size.rows - 1 {
            self.pos.row = self.size.rows - 1;
        }
    }

    fn col_clamp(&mut self) {
        if self.pos.col > self.size.cols - 1 {
            self.pos.col = self.size.cols - 1;
        }
    }
}

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub struct Size {
    pub rows: u16,
    pub cols: u16,
}

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub struct Pos {
    pub row: u16,
    pub col: u16,
}
