//! Public-parser regression examples, not terminal capacity requirements.
//! The viewport is physical; a saved column at its width is a legal pending
//! wrap until the width changes. Height-only resize must preserve that state.

use vt100::{Color, Parser};

const SAVE_AT_EDGE: &[u8] = b"\x1b[24;80H\x1b[31m\x1b7\x1b[H";

fn assert_edge_write(parser: &Parser) {
    let screen = parser.screen();
    assert_eq!(screen.cursor_position(), (4, 10));
    let cell = screen.cell(4, 9).expect("resized bottom-right cell");
    assert_eq!(cell.contents(), "X");
    assert_eq!(cell.fgcolor(), Color::Idx(1));
    assert!(!screen.state_formatted().is_empty());
}

#[test]
fn normal_saved_cursor_is_clipped_before_restore_and_text() {
    let mut parser = Parser::new(24, 80, 0);
    parser.process(SAVE_AT_EDGE);
    parser.set_size(5, 10);
    parser.process(b"\x1b8X");
    assert_edge_write(&parser);
}

#[test]
fn shrink_then_grow_preserves_the_hidden_saved_row() {
    let mut parser = Parser::new(24, 80, 0);
    parser.process(SAVE_AT_EDGE);
    parser.set_size(5, 10);
    parser.set_size(24, 80);
    parser.process(b"\x1b8X");
    assert_eq!(parser.screen().cursor_position(), (23, 10));
    let cell = parser.screen().cell(23, 9).unwrap();
    assert_eq!(cell.contents(), "X");
    assert_eq!(cell.fgcolor(), Color::Idx(1));
    assert_eq!(parser.screen().cell(4, 9).unwrap().contents(), "");
}

#[test]
fn restore_while_small_does_not_overwrite_the_hidden_saved_row() {
    let mut parser = Parser::new(24, 80, 0);
    parser.process(SAVE_AT_EDGE);
    parser.set_size(5, 10);
    parser.process(b"\x1b8X");
    assert_edge_write(&parser);
    parser.set_size(24, 80);
    parser.process(b"\x1b8Y");
    assert_eq!(parser.screen().cursor_position(), (23, 10));
    assert_eq!(parser.screen().cell(4, 9).unwrap().contents(), "X");
    let cell = parser.screen().cell(23, 9).unwrap();
    assert_eq!(cell.contents(), "Y");
    assert_eq!(cell.fgcolor(), Color::Idx(1));
}

#[test]
fn inactive_normal_saved_cursor_is_clipped_during_alternate_resize() {
    let mut parser = Parser::new(24, 80, 0);
    parser.process(b"\x1b[24;80H\x1b[31m\x1b[?1049h\x1b[H");
    parser.set_size(5, 10);
    parser.process(b"\x1b[?1049lX");
    assert!(!parser.screen().alternate_screen());
    assert_edge_write(&parser);
}

#[test]
fn active_alternate_saved_cursor_is_clipped_before_restore() {
    let mut parser = Parser::new(24, 80, 0);
    parser.process(b"\x1b[?47h");
    parser.process(SAVE_AT_EDGE);
    parser.set_size(5, 10);
    parser.process(b"\x1b8X");
    assert!(parser.screen().alternate_screen());
    assert_edge_write(&parser);
}

#[test]
fn inactive_alternate_saved_cursor_is_clipped_before_reentry() {
    let mut parser = Parser::new(24, 80, 0);
    parser.process(b"\x1b[?47h");
    parser.process(SAVE_AT_EDGE);
    parser.process(b"\x1b[?47l");
    parser.set_size(5, 10);
    parser.process(b"\x1b[?47h\x1b8X");
    assert!(parser.screen().alternate_screen());
    assert_edge_write(&parser);
}

#[test]
fn fragmented_save_resize_restore_keeps_the_same_cells_and_attributes() {
    let mut parser = Parser::new(24, 80, 0);
    for byte in SAVE_AT_EDGE {
        parser.process(std::slice::from_ref(byte));
    }
    parser.set_size(5, 10);
    for byte in b"\x1b8X" {
        parser.process(std::slice::from_ref(byte));
    }
    assert_edge_write(&parser);
}

fn assert_saved_wrap_survives_height_resize(old_rows: u16, new_rows: u16) {
    let mut parser = Parser::new(old_rows, 4, 0);
    parser.process(b"ABCD\x1b7\x1b[H");
    parser.set_size(new_rows, 4);
    parser.process(b"\x1b8E");
    let screen = parser.screen();
    // Plain-text export joins soft-wrapped rows. Cells and row_wrapped below
    // independently prove that E occupies the next physical row.
    assert_eq!(screen.contents(), "ABCDE");
    assert_eq!(screen.cursor_position(), (1, 1));
    assert!(screen.row_wrapped(0));
    assert_eq!(screen.cell(0, 3).unwrap().contents(), "D");
    assert_eq!(screen.cell(1, 0).unwrap().contents(), "E");
}

#[test]
fn same_size_keeps_saved_pending_wrap() {
    assert_saved_wrap_survives_height_resize(3, 3);
}

#[test]
fn height_growth_keeps_saved_pending_wrap() {
    assert_saved_wrap_survives_height_resize(2, 3);
}

#[test]
fn height_shrink_keeps_saved_pending_wrap() {
    assert_saved_wrap_survives_height_resize(3, 2);
}

#[test]
fn width_shrink_clips_a_saved_pending_wrap_to_the_new_last_cell() {
    let mut parser = Parser::new(3, 4, 0);
    parser.process(b"ABCD\x1b7\x1b[H");
    parser.set_size(3, 2);
    parser.process(b"\x1b8X");
    assert_eq!(parser.screen().contents(), "AX");
    assert_eq!(parser.screen().cursor_position(), (0, 2));
}

#[test]
fn growth_keeps_an_in_bounds_saved_cursor_and_pen() {
    let mut parser = Parser::new(5, 10, 0);
    parser.process(b"\x1b[3;4H\x1b[32m\x1b7\x1b[H\x1b[0m");
    parser.set_size(24, 80);
    parser.process(b"\x1b8X");
    assert_eq!(parser.screen().cursor_position(), (2, 4));
    let cell = parser.screen().cell(2, 3).unwrap();
    assert_eq!(cell.contents(), "X");
    assert_eq!(cell.fgcolor(), Color::Idx(2));
}

#[test]
fn restore_bounds_the_absolute_saved_row_without_adding_origin_twice() {
    let mut parser = Parser::new(6, 8, 0);
    parser.process(b"\x1b[2;5r\x1b[?6h\x1b[4;3H\x1b7\x1b[?6l\x1b[H");
    parser.set_size(3, 8);
    parser.process(b"\x1b8X");
    assert_eq!(parser.screen().cursor_position(), (2, 3));
    assert_eq!(parser.screen().cell(2, 2).unwrap().contents(), "X");
    parser.set_size(6, 8);
    parser.process(b"\x1b8Y");
    assert_eq!(parser.screen().cursor_position(), (4, 3));
    assert_eq!(parser.screen().cell(4, 2).unwrap().contents(), "Y");
}

#[test]
fn plain_alternate_switch_preserves_saved_origin_register() {
    let mut parser = Parser::new(24, 80, 0);
    parser.process(
        b"\x1b[?47h\x1b[2;4r\x1b[?6h\x1b7\x1b[?6l\x1b[?47l\x1b[?47h\x1b[2;4r\x1b8\x1b[1;1HX",
    );
    assert_eq!(parser.screen().cursor_position(), (1, 1));
    assert_eq!(parser.screen().cell(1, 0).unwrap().contents(), "X");
    assert_eq!(parser.screen().cell(0, 0).unwrap().contents(), "");
}
