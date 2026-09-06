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
    flags: u8,
    attrs: crate::attrs::Attrs,
}

impl PartialEq<Self> for Cell {
    fn eq(&self, other: &Self) -> bool {
        self.flags == other.flags && self.attrs == other.attrs && self.contents == other.contents
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

    pub(crate) fn set(&mut self, c: char, a: crate::attrs::Attrs) {
        self.reset_contents();
        self.contents.push(c);
        self.flags = 0;
        self.set_wide(c.width().unwrap_or(1) > 1);
        self.attrs = a;
    }

    pub(crate) fn append(&mut self, c: char) {
        if self.contents.is_empty() {
            self.contents.push(' ');
        }
        self.contents.push(c);
    }

    pub(crate) fn clear(&mut self, attrs: crate::attrs::Attrs) {
        self.reset_contents();
        self.flags = 0;
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
        self.flags & 0x80 == 0x80
    }

    /// Returns whether the cell contains the second half of a wide character
    /// (in other words, whether the previous cell in the row contains a wide
    /// character)
    #[must_use]
    pub fn is_wide_continuation(&self) -> bool {
        self.flags & 0x40 == 0x40
    }

    fn set_wide(&mut self, wide: bool) {
        if wide {
            self.flags |= 0x80;
        } else {
            self.flags &= 0x7f;
        }
    }

    pub(crate) fn set_wide_continuation(&mut self, wide: bool) {
        if wide {
            self.flags |= 0x40;
        } else {
            self.flags &= 0xbf;
        }
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
