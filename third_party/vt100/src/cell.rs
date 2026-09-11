use unicode_width::UnicodeWidthChar as _;

// Inline storage uses the same byte payload as a Vec descriptor: the
// capacity word plus inline char slots. This is a layout optimization only;
// SmallVec spills into owned storage without a grapheme-length ceiling.
const INLINE_CODEPOINTS: usize =
    (std::mem::size_of::<Vec<char>>() - std::mem::size_of::<usize>()) / std::mem::size_of::<char>();

/// Represents a single terminal cell.
#[derive(Clone, Debug, Default, Eq)]
pub struct Cell {
    contents: smallvec::SmallVec<[char; INLINE_CODEPOINTS]>,
    attrs: crate::attrs::Attrs,
}

impl PartialEq<Self> for Cell {
    fn eq(&self, other: &Self) -> bool {
        self.attrs.cell_flags() == other.attrs.cell_flags()
            && self.attrs == other.attrs
            && self.contents == other.contents
    }
}

impl Cell {
    // Release uncommon overflow storage when the old glyph is replaced or
    // erased. Ordinary inline cells keep their allocation-free fast path.
    fn reset_contents(&mut self) {
        if self.contents.spilled() {
            self.contents = smallvec::SmallVec::new();
        } else {
            self.contents.clear();
        }
    }

    pub(crate) fn set(&mut self, c: char, mut a: crate::attrs::Attrs) {
        self.reset_contents();
        self.contents.push(c);
        a.clear_cell_flags();
        self.attrs = a;
        self.set_wide(c.width().unwrap_or(1) > 1);
    }

    pub(crate) fn append(&mut self, c: char) {
        if self.contents.is_empty() {
            self.contents.push(' ');
        }
        self.contents.push(c);
    }

    pub(crate) fn clear(&mut self, mut attrs: crate::attrs::Attrs) {
        self.reset_contents();
        attrs.clear_cell_flags();
        self.attrs = attrs;
    }

    /// Heap payload owned by this cell, excluding allocator bookkeeping.
    #[must_use]
    pub fn heap_payload_bytes(&self) -> usize {
        if self.contents.spilled() {
            self.contents.capacity() * std::mem::size_of::<char>()
        } else {
            0
        }
    }

    /// Returns the text contents of the cell.
    ///
    /// Can include multiple unicode characters if combining characters are
    /// used, but will contain at most one character with a non-zero character
    /// width.
    #[must_use]
    pub fn contents(&self) -> String {
        self.contents.iter().copied().collect()
    }

    /// Returns whether the cell contains any text data.
    #[must_use]
    pub fn has_contents(&self) -> bool {
        !self.contents.is_empty()
    }

    /// Returns whether the text data in the cell represents a wide character.
    #[must_use]
    pub fn is_wide(&self) -> bool {
        self.attrs.is_wide()
    }

    /// Returns whether the cell contains the second half of a wide character
    /// (in other words, whether the previous cell in the row contains a wide
    /// character)
    #[must_use]
    pub fn is_wide_continuation(&self) -> bool {
        self.attrs.is_wide_continuation()
    }

    fn set_wide(&mut self, wide: bool) {
        self.attrs.set_wide(wide);
    }

    pub(crate) fn set_wide_continuation(&mut self, wide: bool) {
        self.attrs.set_wide_continuation(wide);
    }

    pub(crate) fn attrs(&self) -> &crate::attrs::Attrs {
        &self.attrs
    }

    /// Returns the foreground color of the cell.
    #[must_use]
    pub fn fgcolor(&self) -> crate::attrs::Color {
        self.attrs.fgcolor()
    }

    /// Returns the background color of the cell.
    #[must_use]
    pub fn bgcolor(&self) -> crate::attrs::Color {
        self.attrs.bgcolor()
    }

    /// Returns whether this cell uses faint (SGR2) intensity.
    #[must_use]
    pub fn faint(&self) -> bool {
        self.attrs.faint()
    }

    /// Returns whether the cell should be rendered with the bold text
    /// attribute.
    #[must_use]
    pub fn bold(&self) -> bool {
        self.attrs.bold()
    }

    /// Returns whether the cell should be rendered with the italic text
    /// attribute.
    #[must_use]
    pub fn italic(&self) -> bool {
        self.attrs.italic()
    }

    /// Returns whether the cell should be rendered with the underlined text
    /// attribute.
    #[must_use]
    pub fn underline(&self) -> bool {
        self.attrs.underline()
    }

    /// Returns whether the cell should be rendered with the inverse text
    /// attribute.
    #[must_use]
    pub fn inverse(&self) -> bool {
        self.attrs.inverse()
    }
}
