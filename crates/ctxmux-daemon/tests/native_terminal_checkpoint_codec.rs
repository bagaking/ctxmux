//! Public pinned-library codec proof. No daemon, PTY, or user Run is started.
use vt100::{MouseProtocolEncoding, MouseProtocolMode, Parser};

fn restored(source: &Parser) -> Parser {
    let seed = source
        .basic_restore_geometry_checkpoint()
        .expect("complete parser boundary");
    let policy = source.scrollback_limit();
    let mut target = Parser::new(
        seed.restore.rows,
        seed.restore.cols,
        seed.restore_scrollback_rows.unwrap_or(policy),
    );
    target.process(&seed.restore.restore_bytes);
    target.set_scrollback_limit(policy);
    target.set_size(seed.source_rows, seed.source_cols);
    target.process(&seed.final_bytes);
    assert!(target.is_ground(), "restore must itself be complete");
    assert_eq!(
        target.scrollback_limit(),
        policy,
        "original retention policy"
    );
    target
}

fn assert_same(source: &Parser, target: &Parser) {
    let a = source.screen();
    let b = target.screen();
    assert_eq!(a.size(), b.size());
    assert_eq!(a.contents(), b.contents(), "visible rows");
    assert_eq!(a.cursor_position(), b.cursor_position(), "drawing cursor");
    assert_eq!(a.alternate_screen(), b.alternate_screen());
    assert_eq!(a.mouse_protocol_mode(), b.mouse_protocol_mode());
    assert_eq!(a.mouse_protocol_encoding(), b.mouse_protocol_encoding());
    assert_eq!(a.application_cursor(), b.application_cursor());
    assert_eq!(a.application_keypad(), b.application_keypad());
    assert_eq!(a.bracketed_paste(), b.bracketed_paste());
    assert_eq!(a.attributes_formatted(), b.attributes_formatted());
    for row in 0..a.size().0 {
        assert_eq!(a.row_wrapped(row), b.row_wrapped(row), "wrapped row {row}");
        for col in 0..a.size().1 {
            assert_eq!(a.cell(row, col), b.cell(row, col), "cell {row}/{col}");
        }
    }
}

#[test]
fn normal_real_history_survives_and_cup_redraws_do_not_create_history() {
    let mut source = Parser::new(4, 20, 100);
    source.process(b"one\r\ntwo\r\nthree\r\nfour\r\nfive\r\nsix");
    let mut target = restored(&source);
    assert_same(&source, &target);
    source.set_scrollback(100);
    target.set_scrollback(100);
    assert_eq!(source.screen().scrollback(), 2);
    assert_eq!(target.screen().scrollback(), 2);
    assert_same(&source, &target);
    assert!(source.screen().contents().starts_with("one\ntwo"));

    let mut repaint = Parser::new(4, 20, 100);
    for _ in 0..100 {
        repaint.process(b"\x1b[Hlive\x1b[K");
    }
    let mut repaint_target = restored(&repaint);
    repaint_target.set_scrollback(100);
    assert_eq!(
        repaint_target.screen().scrollback(),
        0,
        "no invented frames"
    );
    assert_same(&repaint, &repaint_target);
}

#[test]
fn alternate_sgr_and_normal_history_continue_through_real_alt_exit() {
    let mut source = Parser::new(4, 20, 100);
    source.process(b"one\r\ntwo\r\nthree\r\nfour\r\nfive\r\nsix\x1b[?1049h\x1b[?1003h\x1b[?1006h\x1b[?1h\x1b=\x1b[?2004h\x1b[32mALT\x1b[3;5H");
    let mut target = restored(&source);
    assert_same(&source, &target);
    assert_eq!(
        target.screen().mouse_protocol_mode(),
        MouseProtocolMode::AnyMotion
    );
    assert_eq!(
        target.screen().mouse_protocol_encoding(),
        MouseProtocolEncoding::Sgr
    );
    let tail = b"Z\x1b[?1049l\x1b[?1003l\x1b[?1006lQ";
    source.process(tail);
    target.process(tail);
    assert_same(&source, &target);
    source.set_scrollback(100);
    target.set_scrollback(100);
    assert_eq!(target.screen().scrollback(), 2);
    assert_same(&source, &target);
}

#[test]
fn incomplete_control_and_utf8_never_export_a_state_only_seed() {
    let pairs: &[(&[u8], &[u8])] = &[
        (b"\x1b[3", b"1mR"),
        (&[0xe4, 0xb8], &[0xad]),
        (b"\x1b]2;title", b"\x07R"),
        (b"\x1bP$q", b"m\x1b\\R"),
    ];
    for (prefix, tail) in pairs {
        let mut source = Parser::new(4, 20, 100);
        source.process(b"known");
        let previous = source.basic_checkpoint().expect("previous complete seed");
        source.process(prefix);
        assert!(!source.is_ground());
        assert!(
            source.basic_checkpoint().is_none(),
            "must retain actual carry"
        );
        let mut target = Parser::new(4, 20, 100);
        target.process(&previous.restore_bytes);
        target.process(prefix);
        source.process(tail);
        target.process(tail);
        assert!(source.is_ground());
        assert_same(&source, &target);
    }
}

#[test]
fn wrapped_history_and_pending_wrap_continue_without_extra_lf() {
    let mut source = Parser::new(3, 6, 100);
    source.process(b"abcdefghijklmnopqrstuvwx");
    let mut target = restored(&source);
    assert_same(&source, &target);
    source.process(b"Y");
    target.process(b"Y");
    assert_same(&source, &target);
    source.set_scrollback(100);
    target.set_scrollback(100);
    assert_eq!(target.screen().scrollback(), source.screen().scrollback());
    assert!(target.screen().scrollback() > 0);
    assert_same(&source, &target);
}

#[test]
fn blank_history_rows_and_wide_wrapped_cells_are_not_collapsed() {
    let mut source = Parser::new(3, 6, 100);
    source.process("first\r\n\r\nabc中xabcdefQ".as_bytes());
    let mut target = restored(&source);
    assert_same(&source, &target);
    source.set_scrollback(100);
    target.set_scrollback(100);
    assert!(source.screen().scrollback() >= 2);
    assert_eq!(target.screen().scrollback(), source.screen().scrollback());
    assert_same(&source, &target);
}

#[test]
fn origin_and_scroll_margins_keep_later_scroll_inside_the_region() {
    let mut source = Parser::new(6, 12, 100);
    source.process(b"top\x1b[6;1Hbottom\x1b[2;5r\x1b[?6h\x1b[3;2H\x1b[31mM");
    let mut target = restored(&source);
    assert_same(&source, &target);
    source.process(b"\r\nA\r\nB\r\nC");
    target.process(b"\r\nA\r\nB\r\nC");
    assert_same(&source, &target);
    assert!(target.screen().contents().starts_with("top"));
    assert!(target.screen().contents().ends_with("bottom"));
}

#[test]
fn public_source_grid_and_cursor_pen_survive_supported_resize() {
    let mut source = Parser::new(4, 12, 100);
    source.process(b"left\r\nright\x1b[31;44;1m");
    source.set_size(6, 16);
    source.process(b"\x1b[5;3H");
    let seed = source.basic_checkpoint().expect("complete");
    assert_eq!((seed.rows, seed.cols), (6, 16));
    let mut target = restored(&source);
    source.process(b"Z");
    target.process(b"Z");
    assert_same(&source, &target);
}

#[test]
fn shrink_clips_saved_normal_cursor_before_checkpoint_and_later_drawing() {
    let mut source = Parser::new(24, 80, 10);
    source.process(b"TOP\x1b[24;80H\x1b[31m\x1b7\x1b[H");
    source.set_size(5, 10);
    let mut target = restored(&source);
    assert_same(&source, &target);
    source.process(b"\x1b8X");
    target.process(b"\x1b8X");
    assert_eq!(source.screen().cursor_position(), (4, 10));
    let cell = source.screen().cell(4, 9).expect("clipped saved cell");
    assert_eq!(cell.contents(), "X");
    assert_eq!(cell.fgcolor(), vt100::Color::Idx(1));
    assert_same(&source, &target);
}

#[test]
fn saved_normal_cursor_follows_retained_history_when_grid_grows_again() {
    let mut source = Parser::new(24, 80, 10);
    source.process(b"\x1b[24;80H\x1b7");
    source.set_size(5, 10);
    source.set_size(24, 80);
    source.process(b"\x1b8X");
    assert_eq!(source.screen().cursor_position(), (14, 10));
    assert_eq!(source.screen().cell(14, 9).unwrap().contents(), "X");
    assert_eq!(source.screen().cell(23, 79).unwrap().contents(), "");
    assert_same(&source, &restored(&source));
}

#[test]
fn alternate_return_register_uses_actual_normal_history_retention() {
    let mut source = Parser::new(24, 80, 10);
    source.process(b"NORMAL\x1b[24;80H\x1b[31m\x1b[?1049h\x1b[H\x1b[32mALT");
    source.set_size(5, 10);
    let mut target = restored(&source);
    assert!(source.screen().alternate_screen());
    assert_eq!(source.screen().contents(), "ALT");
    assert_same(&source, &target);
    source.process(b"\x1b[?1049lX");
    target.process(b"\x1b[?1049lX");
    assert!(!source.screen().alternate_screen());
    assert_eq!(source.screen().rows(0, 10).next().unwrap(), "");
    assert!(
        !source.screen().contents().contains("NORMAL"),
        "evicted under the original ten-row history policy"
    );
    let cell = source.screen().cell(4, 9).unwrap();
    assert_eq!(cell.contents(), "X");
    assert_eq!(cell.fgcolor(), vt100::Color::Idx(1));
    assert_same(&source, &target);
}

#[test]
fn shrink_clips_active_alternate_saved_cursor_and_keeps_both_screens() {
    let mut source = Parser::new(24, 80, 10);
    source.process(b"NORMAL\x1b[?47hALT\x1b[24;80H\x1b7\x1b[H");
    source.set_size(5, 10);
    source.process(b"\x1b8X");
    assert_eq!(source.screen().cursor_position(), (4, 10));
    assert_eq!(source.screen().cell(4, 9).unwrap().contents(), "X");
    let mut target = restored(&source);
    assert_same(&source, &target);
    source.process(b"\x1b[?47l\x1b[H");
    target.process(b"\x1b[?47l\x1b[H");
    assert_eq!(source.screen().contents(), "NORMAL");
    assert_same(&source, &target);
}

#[test]
fn resize_preserves_normal_reflow_and_retained_alternate_wide_cells() {
    for alternate in [false, true] {
        let mut source = Parser::new(2, 4, 10);
        if alternate {
            source.process(b"\x1b[?47h");
        }
        source.process("\x1b[31mab中\x1b[2;1H中Z".as_bytes());
        source.set_size(2, 3);
        let cut = source.screen().cell(0, 2).unwrap();
        if alternate {
            assert_eq!(cut.contents(), "中");
            assert!(cut.is_wide(), "ALT retains the off-right wide lead");
            assert_eq!(cut.fgcolor(), vt100::Color::Idx(1));
        } else {
            assert_eq!(cut.contents(), "");
            assert!(!cut.is_wide());
            assert_eq!(cut.fgcolor(), vt100::Color::Default);
            assert_eq!(source.screen().contents(), "中\n中Z");
        }
        assert!(!cut.is_wide_continuation());
        assert!(source.screen().cell(1, 0).unwrap().is_wide());
        assert!(source.screen().cell(1, 1).unwrap().is_wide_continuation());
        let mut target = restored(&source);
        assert_same(&source, &target);
        source.process(b"\x1b[1;3HX");
        target.process(b"\x1b[1;3HX");
        assert_eq!(
            source.screen().contents(),
            if alternate { "abX\n中Z" } else { "中X\n中Z" }
        );
        assert_same(&source, &target);
        source.set_size(2, 4);
        target.set_size(2, 4);
        assert_same(&source, &target);
    }
}

#[test]
fn one_column_ignores_unrenderable_wide_chars_and_continues_narrow_drawing() {
    let mut source = Parser::new(5, 1, 10);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        source.process("中".as_bytes());
    }));
    assert!(
        result.is_ok(),
        "positive one-column grid must accept output"
    );
    assert_eq!(source.screen().cursor_position(), (0, 0));
    assert_eq!(source.screen().contents(), "");
    source.process("A文B".as_bytes());
    assert_eq!(source.screen().contents(), "AB");
    assert_eq!(source.screen().cursor_position(), (1, 1));
    assert!(source.screen().row_wrapped(0));
    let mut target = restored(&source);
    assert_same(&source, &target);
    source.process(b"C");
    target.process(b"C");
    assert_eq!(source.screen().contents(), "ABC");
    assert_same(&source, &target);
}

#[test]
fn one_row_wrap_retains_the_actual_history_row_and_checkpoint_continues() {
    let mut source = Parser::new(1, 1, 10);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        source.process(b"AB");
    }));
    assert!(result.is_ok(), "positive one-row grid must accept autowrap");
    assert_eq!(source.screen().contents(), "B");
    assert_eq!(source.screen().cursor_position(), (0, 1));
    assert!(!source.screen().row_wrapped(0));
    let mut target = restored(&source);
    assert_same(&source, &target);
    source.process(b"C");
    target.process(b"C");
    assert_eq!(source.screen().contents(), "C");
    source.set_scrollback(10);
    target.set_scrollback(10);
    assert_eq!(source.screen().scrollback(), 2);
    assert_eq!(source.screen().contents(), "A");
    assert!(source.screen().row_wrapped(0));
    assert_same(&source, &target);
    source.set_scrollback(1);
    target.set_scrollback(1);
    assert_eq!(source.screen().contents(), "B");
    assert!(source.screen().row_wrapped(0));
    assert_same(&source, &target);
}

#[test]
fn same_size_save_restore_preserves_existing_pending_wrap() {
    let mut source = Parser::new(3, 4, 10);
    source.process(b"abcd\x1b7\x1b[H");
    source.set_size(3, 4);
    let mut target = restored(&source);
    source.process(b"\x1b8E");
    target.process(b"\x1b8E");
    assert_eq!(source.screen().contents(), "abcdE");
    assert_eq!(source.screen().cursor_position(), (1, 1));
    assert!(source.screen().row_wrapped(0));
    assert_same(&source, &target);
}

#[test]
fn identical_geometry_keeps_existing_wrap_on_both_screens() {
    let mut source = Parser::new(2, 2, 20);
    let mut control = Parser::new(2, 2, 20);
    source.process(b"abcdef\x1b[?47huvwxyz");
    control.process(b"abcdef\x1b[?47huvwxyz");
    assert!(source.screen().row_wrapped(0));
    source.set_size(2, 2);
    assert!(source.screen().row_wrapped(0));
    assert_same(&source, &control);
    source.process(b"\x1b[?47l");
    control.process(b"\x1b[?47l");
    assert!(source.screen().row_wrapped(0));
    source.process(b"Q");
    control.process(b"Q");
    assert_same(&source, &control);
}

#[test]
fn retained_history_preserves_default_cells_styles_spaces_and_long_graphemes() {
    let cases = [
        "short".to_owned(),
        String::new(),
        "\x1b[48;2;73;19;201m\x1b[2K\x1b[0m".to_owned(),
        " ".to_owned(),
        "\x1b[38;2;7;93;201m中e\u{301}\u{327}\x1b[0m".to_owned(),
        format!("e{}", "\u{301}".repeat(100)),
    ];
    for line in cases {
        let mut expected = Parser::new(2, 12, 8);
        expected.process(line.as_bytes());
        let mut source = Parser::new(2, 12, 8);
        source.process(format!("{line}\r\n").repeat(6).as_bytes());
        let mut target = restored(&source);
        source.set_scrollback(usize::MAX);
        target.set_scrollback(usize::MAX);
        assert_eq!(source.screen().scrollback(), 5);
        assert_same(&source, &target);
        for col in 0..12 {
            assert_eq!(
                source.screen().cell(0, col),
                expected.screen().cell(0, col),
                "historical cell {col}, line {line:?}"
            );
        }
        // Real height growth pulls the same historical physical rows back into
        // the live viewport. Mutating an untouched column must remain ordinary
        // drawing, including editing and erasing the formerly blank tail.
        source.set_scrollback(0);
        target.set_scrollback(0);
        source.set_size(7, 12);
        target.set_size(7, 12);
        assert_same(&source, &target);
        let tail = b"\x1b[1;12HX\x1b[1;9H\x1b[2@Y\x1b[1;10H\x1b[2P\x1b[1;11H\x1b[2X";
        source.process(tail);
        target.process(tail);
        assert_eq!(source.screen().cell(0, 8).unwrap().contents(), "Y");
        assert_eq!(source.screen().cell(0, 10).unwrap().contents(), "");
        assert_same(&source, &target);
        source.set_size(4, 8);
        target.set_size(4, 8);
        assert_same(&source, &target);
        source.set_size(7, 16);
        target.set_size(7, 16);
        assert_same(&source, &target);
    }
}

#[test]
fn write_nonempty_public_xterm_inputs_for_independent_target_proof() {
    let cases = [
        ("normal history",4,20,b"\x1b[1;2mone\r\ntwo\r\nthree\x1b[22m\r\nfour\r\n\x1b[2mfive\r\nsix".to_vec(), b"\x1b[22m\r\nseven".to_vec()),
        ("alternate sgr and real exit",4,20,b"\x1b[2mone\r\ntwo\r\nthree\r\nfour\r\nfive\r\nsix\x1b[?1049h\x1b[?1003h\x1b[?1006h\x1b[?1h\x1b=\x1b[?2004h\x1b[1;2;32mALT\x1b[3;5H".to_vec(), b"\x1b[22mZ\x1b[?1049l\x1b[?1003l\x1b[?1006lQ".to_vec()),
        ("CUP only no manufactured history",4,20,b"\x1b[Hlive\x1b[K".repeat(100), b"\x1b[2;1Hnext".to_vec()),
        ("normal wrapped history pending wrap",3,6,b"abcdefghijklmnopqrstuvwx".to_vec(), b"Y".to_vec()),
        ("origin scroll margins",6,12,b"top\x1b[6;1Hbottom\x1b[2;5r\x1b[?6h\x1b[3;2H\x1b[31mM".to_vec(), b"\r\nA\r\nB\r\nC".to_vec()),
        ("blank and wide wrapped history",3,6,"first\r\n\r\nabc中xabcdefQ".as_bytes().to_vec(), b"R".to_vec()),
        ("normal origin pending wrap through alternate",6,8,b"one\r\n\r\ntwo\r\nthree\r\nfour\r\nfive\r\nsix\r\nseven\x1b[2;5r\x1b[?6h\x1b[3;1H\x1b[31mabcdefgh\x1b[?1049h\x1b[?1003h\x1b[?1006h\x1b[32mALT".to_vec(), b"\x1b[?1049lQ".to_vec()),
    ];
    let intensity_transitions: Vec<_> = (0..4).flat_map(|old| (0..4).map(move |new| {
        let text = |value| format!("\x1b[0{}{}mX", if value & 1 != 0 { ";1" } else { "" }, if value & 2 != 0 { ";2" } else { "" });
        let prefix = text(old).into_bytes();
        let next = format!("\x1b[H{}", text(new)).into_bytes();
        let mut source = Parser::new(1, 2, 0);
        source.process(&prefix);
        let previous = source.screen().clone();
        source.process(&next);
        serde_json::json!({"old":old,"new":new,"prefix":prefix,"next":next,"diff":source.screen().contents_diff(&previous)})
    })).collect();
    assert_eq!(intensity_transitions.len(), 16);
    let fixtures: Vec<_> = cases.into_iter().map(|(name,rows,cols,prefix,tail)| {
        let mut source = Parser::new(rows,cols,100);
        source.process(&prefix);
        let seed = source.basic_checkpoint().expect("complete fixture prefix");
        assert!(!seed.restore_bytes.is_empty());
        serde_json::json!({"name":name,"rows":rows,"cols":cols,"prefix":prefix,"tail":tail,"seed":seed.restore_bytes,"intensityTransitions":intensity_transitions})
    }).collect();
    assert_eq!(fixtures.len(), 7);
    let directory = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/basic-codec");
    std::fs::create_dir_all(&directory).expect("create ignored proof output");
    std::fs::write(
        directory.join("xterm-inputs.json"),
        serde_json::to_vec_pretty(&fixtures).expect("encode fixtures"),
    )
    .expect("write synthetic proof inputs");
}
