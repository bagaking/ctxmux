use std::{
    io::{BufRead, BufReader, Write},
    process::{Command, Stdio},
};

use serde_json::Value;

#[test]
fn helper_has_no_descendants_and_reaps_after_one_final_frame() {
    let mut helper = Command::new(env!("CARGO_BIN_EXE_ctxmux-rss-sampler"))
        .args([
            "--pid",
            &std::process::id().to_string(),
            "--interval-ms",
            "25",
            "--max-gap-ms",
            "100",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let helper_pid = helper.id();
    let mut output = BufReader::new(helper.stdout.take().unwrap());
    helper
        .stdin
        .as_mut()
        .unwrap()
        .write_all(b"start\n")
        .unwrap();
    let mut frames = Vec::new();
    for _ in 0..4 {
        let mut line = String::new();
        output.read_line(&mut line).unwrap();
        frames.push(serde_json::from_str::<Value>(&line).unwrap());
    }
    assert!(process_children(helper_pid).is_empty());
    helper.stdin.take().unwrap().write_all(b"stop\n").unwrap();
    for line in output.lines() {
        frames.push(serde_json::from_str(&line.unwrap()).unwrap());
    }
    assert!(helper.wait().unwrap().success());
    assert_eq!(
        frames
            .iter()
            .filter(|frame| frame["final_frame"] == true)
            .count(),
        1
    );
    assert_eq!(frames.last().unwrap()["final_frame"], true);
    assert!(frames.windows(2).all(|pair| {
        pair[1]["seq"].as_u64().unwrap() == pair[0]["seq"].as_u64().unwrap() + 1
            && pair[1]["timestamp_ms"].as_u64().unwrap() - pair[0]["timestamp_ms"].as_u64().unwrap()
                <= 100
    }));
}

fn process_children(parent: u32) -> Vec<u32> {
    let output = Command::new("ps")
        .args(["-axo", "pid=,ppid="])
        .output()
        .unwrap();
    assert!(output.status.success());
    String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .filter_map(|line| {
            let mut fields = line.split_ascii_whitespace();
            let child_pid = fields.next()?.parse::<u32>().ok()?;
            let parent_pid = fields.next()?.parse::<u32>().ok()?;
            (parent_pid == parent).then_some(child_pid)
        })
        .collect()
}

#[test]
fn helper_fails_when_target_disappears() {
    let mut target = Command::new("sh")
        .args(["-c", "while :; do sleep 1; done"])
        .spawn()
        .unwrap();
    let mut helper = Command::new(env!("CARGO_BIN_EXE_ctxmux-rss-sampler"))
        .args([
            "--pid",
            &target.id().to_string(),
            "--interval-ms",
            "25",
            "--max-gap-ms",
            "100",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut first = String::new();
    helper
        .stdin
        .as_mut()
        .unwrap()
        .write_all(b"start\n")
        .unwrap();
    BufReader::new(helper.stdout.take().unwrap())
        .read_line(&mut first)
        .unwrap();
    assert!(!first.is_empty());
    target.kill().unwrap();
    target.wait().unwrap();
    assert!(!helper.wait().unwrap().success());
}
