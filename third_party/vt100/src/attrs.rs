use crate::term::BufWrite as _;

/// Represents a foreground or background color for cells.
#[derive(Eq, PartialEq, Debug, Copy, Clone)]
pub enum Color {
    /// The default terminal color.
    Default,

    /// An indexed terminal color.
    Idx(u8),

    /// An RGB terminal color. The parameters are (red, green, blue).
    Rgb(u8, u8, u8),
}

impl Default for Color {
    fn default() -> Self {
        Self::Default
    }
}

const TEXT_MODE_BOLD: u16 = 0b0000_0001;
const TEXT_MODE_ITALIC: u16 = 0b0000_0010;
const TEXT_MODE_UNDERLINE: u16 = 0b0000_0100;
const TEXT_MODE_INVERSE: u16 = 0b0000_1000;

// Five independent styles and two color tags fit in nine bits. The two wide
// cell flags share the unused high bits: Attrs plus cell metadata still occupies
// eight bytes, preserving the original Cell allocation without losing RGB bits.
const TEXT_MODE_FAINT: u16 = 1 << 4;
const FOREGROUND_KIND_SHIFT: u16 = 5;
const BACKGROUND_KIND_SHIFT: u16 = 7;
const COLOR_KIND_MASK: u16 = 0b11;
const STYLE_COLOR_MASK: u16 = (1 << 9) - 1;
const CELL_WIDE: u16 = 1 << 9;
const CELL_CONTINUATION: u16 = 1 << 10;

#[derive(Default, Clone, Copy, Eq, Debug)]
pub struct Attrs {
    foreground: [u8; 3],
    background: [u8; 3],
    mode: u16,
}

// Cell geometry is deliberately not a text attribute. Diff/erase callers compare
// logical attributes, while Cell equality separately compares the geometry bits.
impl PartialEq for Attrs {
    fn eq(&self, other: &Self) -> bool {
        self.foreground == other.foreground
            && self.background == other.background
            && self.mode & STYLE_COLOR_MASK == other.mode & STYLE_COLOR_MASK
    }
}

impl Attrs {
    pub(crate) fn cell_flags(&self) -> u16 {
        self.mode & (CELL_WIDE | CELL_CONTINUATION)
    }
    pub(crate) fn clear_cell_flags(&mut self) {
        self.mode &= STYLE_COLOR_MASK;
    }
    pub(crate) fn is_wide(&self) -> bool {
        self.mode & CELL_WIDE != 0
    }
    pub(crate) fn is_wide_continuation(&self) -> bool {
        self.mode & CELL_CONTINUATION != 0
    }
    pub(crate) fn set_wide(&mut self, wide: bool) {
        if wide {
            self.mode |= CELL_WIDE;
        } else {
            self.mode &= !CELL_WIDE;
        }
    }
    pub(crate) fn set_wide_continuation(&mut self, wide: bool) {
        if wide {
            self.mode |= CELL_CONTINUATION;
        } else {
            self.mode &= !CELL_CONTINUATION;
        }
    }
    pub fn faint(&self) -> bool {
        self.mode & TEXT_MODE_FAINT != 0
    }
    pub fn set_faint(&mut self, faint: bool) {
        if faint {
            self.mode |= TEXT_MODE_FAINT;
        } else {
            self.mode &= !TEXT_MODE_FAINT;
        }
    }
    fn decode_color(payload: [u8; 3], kind: u16) -> Color {
        match kind {
            0 => Color::Default,
            1 => Color::Idx(payload[0]),
            2 => Color::Rgb(payload[0], payload[1], payload[2]),
            _ => unreachable!("only the three Color variants can be encoded"),
        }
    }

    fn encode_color(color: Color) -> ([u8; 3], u16) {
        match color {
            Color::Default => ([0; 3], 0),
            Color::Idx(index) => ([index, 0, 0], 1),
            Color::Rgb(red, green, blue) => ([red, green, blue], 2),
        }
    }

    pub fn fgcolor(&self) -> Color {
        Self::decode_color(
            self.foreground,
            (self.mode >> FOREGROUND_KIND_SHIFT) & COLOR_KIND_MASK,
        )
    }

    pub fn bgcolor(&self) -> Color {
        Self::decode_color(
            self.background,
            (self.mode >> BACKGROUND_KIND_SHIFT) & COLOR_KIND_MASK,
        )
    }

    pub fn set_fgcolor(&mut self, color: Color) {
        let (payload, kind) = Self::encode_color(color);
        self.foreground = payload;
        self.mode = (self.mode & !(COLOR_KIND_MASK << FOREGROUND_KIND_SHIFT))
            | (kind << FOREGROUND_KIND_SHIFT);
    }

    pub fn set_bgcolor(&mut self, color: Color) {
        let (payload, kind) = Self::encode_color(color);
        self.background = payload;
        self.mode = (self.mode & !(COLOR_KIND_MASK << BACKGROUND_KIND_SHIFT))
            | (kind << BACKGROUND_KIND_SHIFT);
    }

    pub fn bold(&self) -> bool {
        self.mode & TEXT_MODE_BOLD != 0
    }

    pub fn set_bold(&mut self, bold: bool) {
        if bold {
            self.mode |= TEXT_MODE_BOLD;
        } else {
            self.mode &= !TEXT_MODE_BOLD;
        }
    }

    pub fn italic(&self) -> bool {
        self.mode & TEXT_MODE_ITALIC != 0
    }

    pub fn set_italic(&mut self, italic: bool) {
        if italic {
            self.mode |= TEXT_MODE_ITALIC;
        } else {
            self.mode &= !TEXT_MODE_ITALIC;
        }
    }

    pub fn underline(&self) -> bool {
        self.mode & TEXT_MODE_UNDERLINE != 0
    }

    pub fn set_underline(&mut self, underline: bool) {
        if underline {
            self.mode |= TEXT_MODE_UNDERLINE;
        } else {
            self.mode &= !TEXT_MODE_UNDERLINE;
        }
    }

    pub fn inverse(&self) -> bool {
        self.mode & TEXT_MODE_INVERSE != 0
    }

    pub fn set_inverse(&mut self, inverse: bool) {
        if inverse {
            self.mode |= TEXT_MODE_INVERSE;
        } else {
            self.mode &= !TEXT_MODE_INVERSE;
        }
    }

    pub fn write_escape_code_diff(&self, contents: &mut Vec<u8>, other: &Self) {
        if self != other && self == &Self::default() {
            crate::term::ClearAttrs::default().write_buf(contents);
            return;
        }

        let attrs = crate::term::Attrs::default();

        let attrs = if self.fgcolor() == other.fgcolor() {
            attrs
        } else {
            attrs.fgcolor(self.fgcolor())
        };
        let attrs = if self.bgcolor() == other.bgcolor() {
            attrs
        } else {
            attrs.bgcolor(self.bgcolor())
        };
        let reset_intensity = (!self.bold() && other.bold()) || (!self.faint() && other.faint());
        let mut attrs = if reset_intensity {
            attrs.bold(self.bold()).faint(self.faint())
        } else {
            attrs
        };
        if !reset_intensity && self.bold() != other.bold() {
            attrs = attrs.bold(self.bold());
        }
        if !reset_intensity && self.faint() != other.faint() {
            attrs = attrs.faint(self.faint());
        }
        let attrs = if self.italic() == other.italic() {
            attrs
        } else {
            attrs.italic(self.italic())
        };
        let attrs = if self.underline() == other.underline() {
            attrs
        } else {
            attrs.underline(self.underline())
        };
        let attrs = if self.inverse() == other.inverse() {
            attrs
        } else {
            attrs.inverse(self.inverse())
        };

        attrs.write_buf(contents);
    }
}
