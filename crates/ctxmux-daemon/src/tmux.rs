use std::{
    env,
    ffi::OsString,
    fs,
    io::{self, BufRead},
    os::unix::fs::{FileTypeExt, MetadataExt},
    process::{Child, ChildStdin, ChildStdout, Command, Stdio},
    time::{Duration, Instant},
};

use ctxmux_protocol::{ErrorCode, ProtocolError, TerminalSize, TmuxPaneInfo};

use self::short_command::{BoundedOutput, CaptureLimits};

mod short_command;

pub(crate) const MAX_CONTROL_LINE_BYTES: usize = 1024 * 1024;
const MAX_COMMAND_BLOCK_LINES: usize = 32 * 1024;
const MINIMUM_TMUX_MAJOR: u32 = 3;
const MINIMUM_TMUX_MINOR: u32 = 4;
const SHORT_COMMAND_TIMEOUT: Duration = Duration::from_secs(3);
const VERSION_STDOUT_BYTES: usize = 4 * 1024;
const DISCOVERY_STDOUT_BYTES: usize = 128 * 1024;
const SHORT_COMMAND_STDERR_BYTES: usize = 16 * 1024;
const PANE_FORMAT: &str = concat!(
    "#{version}\t#{pid}\t#{start_time}\t#{session_id}\t#{window_id}\t#{pane_id}\t",
    "#{pane_pid}\t#{pane_width}\t#{pane_height}\t#{pane_dead}"
);
const TARGET_IDENTITY_FORMAT: &str =
    "#{pid}\t#{start_time}\t#{session_id}\t#{window_id}\t#{pane_id}\t#{pane_pid}";

pub(crate) struct TmuxDiscovery {
    pub(crate) version: String,
    pub(crate) panes: Vec<TmuxPaneInfo>,
    socket_identity: SocketIdentity,
}

pub(crate) struct PendingControl {
    child: Option<Child>,
    stdin: Option<ChildStdin>,
    stdout: Option<ChildStdout>,
    pub(crate) target: TmuxPaneInfo,
    pub(crate) socket_identity: SocketIdentity,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SocketIdentity {
    device: u64,
    inode: u64,
}

impl PendingControl {
    pub(crate) fn child_id(&self) -> u32 {
        self.child.as_ref().expect("tmux child is present").id()
    }

    pub(crate) fn take_stdin(&mut self) -> ChildStdin {
        self.stdin.take().expect("tmux control stdin is present")
    }

    pub(crate) fn take_stdout(&mut self) -> ChildStdout {
        self.stdout.take().expect("tmux control stdout is present")
    }

    pub(crate) fn take_child(&mut self) -> Child {
        self.child.take().expect("tmux child is present")
    }
}

impl Drop for PendingControl {
    fn drop(&mut self) {
        if let Some(child) = &mut self.child {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

pub(crate) fn discover(
    socket_path: &str,
    deadline: Instant,
) -> Result<TmuxDiscovery, ProtocolError> {
    ensure_before_deadline(deadline, "tmux pane discovery")?;
    if socket_path.is_empty() {
        return Err(ProtocolError::new(
            ErrorCode::InvalidRequest,
            "tmux socket path must not be empty",
        ));
    }
    let executable = executable();
    read_supported_client_version(&executable, deadline)?;
    let socket_identity = read_socket_identity(socket_path)?;
    let mut command = base_command(&executable);
    command
        .arg("-S")
        .arg(socket_path)
        .args(["list-panes", "-a", "-F", PANE_FORMAT]);
    let output = run_short_command(
        &mut command,
        deadline,
        DISCOVERY_STDOUT_BYTES,
        "run tmux pane discovery",
    )?;
    ensure_before_deadline(deadline, "tmux pane discovery")?;
    if !output.status.success() {
        return Err(ProtocolError::new(
            ErrorCode::BackendUnavailable,
            format!(
                "tmux pane discovery failed: {}",
                bounded_stderr(&output.stderr)
            ),
        ));
    }
    if current_socket_identity(socket_path).ok() != Some(socket_identity) {
        return Err(ProtocolError::new(
            ErrorCode::TargetChanged,
            "tmux socket changed during pane discovery",
        ));
    }
    let socket_path = socket_path.to_owned();
    let mut version = None;
    let mut panes = Vec::new();
    for line in output
        .stdout
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let parsed = parse_pane_line(line, &socket_path)?;
        if version
            .as_ref()
            .is_some_and(|version| version != &parsed.version)
        {
            return Err(ProtocolError::new(
                ErrorCode::BackendUnavailable,
                "tmux pane discovery returned inconsistent server versions",
            ));
        }
        version.get_or_insert(parsed.version);
        if let Some(pane) = parsed.pane {
            panes.push(pane);
        }
    }
    let version = version.ok_or_else(|| {
        ProtocolError::new(
            ErrorCode::BackendUnavailable,
            "tmux pane discovery returned no target server rows",
        )
    })?;
    ensure_before_deadline(deadline, "tmux pane discovery")?;
    Ok(TmuxDiscovery {
        version,
        panes,
        socket_identity,
    })
}

pub(crate) fn spawn_control(
    socket_path: &str,
    pane_id: &str,
    discovery_deadline: Instant,
) -> Result<PendingControl, ProtocolError> {
    validate_pane_id(pane_id)?;
    let discovery = discover(socket_path, discovery_deadline)?;
    let socket_identity = discovery.socket_identity;
    let mut matches = discovery
        .panes
        .into_iter()
        .filter(|pane| pane.pane_id == pane_id);
    let target = matches.next().ok_or_else(|| {
        ProtocolError::new(
            ErrorCode::TargetChanged,
            format!("tmux pane {pane_id} does not exist"),
        )
    })?;
    if matches.next().is_some() {
        return Err(ProtocolError::new(
            ErrorCode::TargetChanged,
            format!(
                "tmux pane {pane_id} is linked into multiple targets and cannot be imported unambiguously"
            ),
        ));
    }
    let executable = executable();
    let mut child = base_command(&executable)
        .arg("-S")
        .arg(socket_path)
        .arg("-C")
        .args(["attach-session", "-t", &target.session_id, "-f"])
        .arg("read-only,ignore-size,no-detach-on-destroy,pause-after=1")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|error| backend_error("start tmux Control Mode client", error))?;
    if current_socket_identity(socket_path).ok() != Some(socket_identity) {
        let _ = child.kill();
        let _ = child.wait();
        return Err(ProtocolError::new(
            ErrorCode::TargetChanged,
            "tmux socket changed while starting the Control Mode client",
        ));
    }
    let stdin = child.stdin.take().ok_or_else(|| {
        ProtocolError::new(
            ErrorCode::BackendUnavailable,
            "tmux Control Mode client has no stdin",
        )
    })?;
    let stdout = child.stdout.take().ok_or_else(|| {
        ProtocolError::new(
            ErrorCode::BackendUnavailable,
            "tmux Control Mode client has no stdout",
        )
    })?;
    Ok(PendingControl {
        child: Some(child),
        stdin: Some(stdin),
        stdout: Some(stdout),
        target,
        socket_identity,
    })
}

fn executable() -> OsString {
    env::var_os("CTXMUX_TMUX_BIN").unwrap_or_else(|| OsString::from("tmux"))
}

fn base_command(executable: &OsString) -> Command {
    let mut command = Command::new(executable);
    command.env_remove("TMUX").env_remove("TMUX_PANE");
    if env::var_os("TERM").is_none() {
        command.env("TERM", "xterm-256color");
    }
    command
}

fn read_supported_client_version(
    executable: &OsString,
    deadline: Instant,
) -> Result<(), ProtocolError> {
    let mut command = base_command(executable);
    command.arg("-V");
    let output = run_short_command(
        &mut command,
        deadline,
        VERSION_STDOUT_BYTES,
        "read tmux version",
    )?;
    ensure_before_deadline(deadline, "tmux client version probe")?;
    if !output.status.success() {
        return Err(ProtocolError::new(
            ErrorCode::BackendUnavailable,
            format!(
                "tmux version probe failed: {}",
                bounded_stderr(&output.stderr)
            ),
        ));
    }
    let value = std::str::from_utf8(&output.stdout)
        .map_err(|_| {
            ProtocolError::new(
                ErrorCode::UnsupportedBackendVersion,
                "tmux version output is not UTF-8",
            )
        })?
        .trim();
    let version = value.strip_prefix("tmux ").ok_or_else(|| {
        ProtocolError::new(
            ErrorCode::UnsupportedBackendVersion,
            format!("unrecognized tmux version output {value:?}"),
        )
    })?;
    validate_supported_version(version, "client")?;
    Ok(())
}

fn run_short_command(
    command: &mut Command,
    owner_deadline: Instant,
    stdout_bytes: usize,
    action: &str,
) -> Result<BoundedOutput, ProtocolError> {
    let command_deadline = owner_deadline.min(Instant::now() + SHORT_COMMAND_TIMEOUT);
    short_command::run(
        command,
        command_deadline,
        CaptureLimits {
            stdout_bytes,
            stderr_bytes: SHORT_COMMAND_STDERR_BYTES,
        },
    )
    .map_err(|error| backend_error(action, error))
}

fn ensure_before_deadline(deadline: Instant, action: &str) -> Result<(), ProtocolError> {
    if Instant::now() < deadline {
        Ok(())
    } else {
        Err(ProtocolError::new(
            ErrorCode::BackendUnavailable,
            format!("{action} exceeded its execution deadline"),
        ))
    }
}

fn validate_supported_version(version: &str, owner: &str) -> Result<(), ProtocolError> {
    let parsed = ParsedVersion::parse(version).ok_or_else(|| {
        ProtocolError::new(
            ErrorCode::UnsupportedBackendVersion,
            format!("unrecognized tmux {owner} version {version:?}"),
        )
    })?;
    if parsed.major != MINIMUM_TMUX_MAJOR || parsed.minor < MINIMUM_TMUX_MINOR {
        return Err(ProtocolError::new(
            ErrorCode::UnsupportedBackendVersion,
            format!("tmux {owner} {version} is unsupported; ctxmux requires tmux 3.4 through 3.x"),
        ));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ParsedVersion {
    major: u32,
    minor: u32,
}

impl ParsedVersion {
    fn parse(value: &str) -> Option<Self> {
        let (major, remainder) = value.split_once('.')?;
        if major.is_empty() || !major.bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }
        let minor_digits = remainder.bytes().take_while(u8::is_ascii_digit).count();
        if minor_digits == 0 {
            return None;
        }
        let suffix = &remainder.as_bytes()[minor_digits..];
        if !(suffix.is_empty() || matches!(suffix, [byte] if byte.is_ascii_lowercase())) {
            return None;
        }
        Some(Self {
            major: major.parse().ok()?,
            minor: remainder[..minor_digits].parse().ok()?,
        })
    }
}

struct ParsedPaneLine {
    version: String,
    pane: Option<TmuxPaneInfo>,
}

fn parse_pane_line(line: &[u8], socket_path: &str) -> Result<ParsedPaneLine, ProtocolError> {
    let fields = line.split(|byte| *byte == b'\t').collect::<Vec<_>>();
    if fields.len() != 10 {
        return Err(ProtocolError::new(
            ErrorCode::BackendUnavailable,
            "tmux pane discovery returned an unexpected field count",
        ));
    }
    let version = std::str::from_utf8(fields[0])
        .map_err(|_| {
            ProtocolError::new(
                ErrorCode::UnsupportedBackendVersion,
                "tmux server version is not UTF-8",
            )
        })?
        .to_owned();
    validate_supported_version(&version, "server")?;
    if fields[9] == b"1" {
        return Ok(ParsedPaneLine {
            version,
            pane: None,
        });
    }
    if fields[9] != b"0" {
        return Err(ProtocolError::new(
            ErrorCode::BackendUnavailable,
            "tmux pane discovery returned an invalid dead-pane flag",
        ));
    }
    let server_pid = parse_u32(fields[1], "server PID")?;
    let server_started_at = parse_u64(fields[2], "server start time")?;
    let session_id = parse_id(fields[3], b'$', "session")?;
    let window_id = parse_id(fields[4], b'@', "window")?;
    let pane_target = parse_id(fields[5], b'%', "pane")?;
    let process_id = parse_u32(fields[6], "pane PID")?;
    let cols = parse_u16(fields[7], "pane columns")?;
    let rows = parse_u16(fields[8], "pane rows")?;
    if cols == 0 || rows == 0 {
        return Err(ProtocolError::new(
            ErrorCode::BackendUnavailable,
            "tmux pane dimensions must be nonzero",
        ));
    }
    Ok(ParsedPaneLine {
        version: version.clone(),
        pane: Some(TmuxPaneInfo {
            socket_path: socket_path.to_owned(),
            tmux_version: version,
            server_pid,
            server_started_at,
            session_id,
            window_id,
            pane_id: pane_target,
            pane_pid: process_id,
            size: TerminalSize { cols, rows },
        }),
    })
}

fn parse_id(value: &[u8], prefix: u8, label: &str) -> Result<String, ProtocolError> {
    if value.first() != Some(&prefix)
        || value.len() == 1
        || !value[1..].iter().all(u8::is_ascii_digit)
    {
        return Err(ProtocolError::new(
            ErrorCode::BackendUnavailable,
            format!("tmux returned an invalid {label} ID"),
        ));
    }
    String::from_utf8(value.to_vec()).map_err(|_| {
        ProtocolError::new(
            ErrorCode::BackendUnavailable,
            format!("tmux returned a non-UTF-8 {label} ID"),
        )
    })
}

fn validate_pane_id(value: &str) -> Result<(), ProtocolError> {
    if value.strip_prefix('%').is_some_and(|digits| {
        !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit())
    }) {
        Ok(())
    } else {
        Err(ProtocolError::new(
            ErrorCode::InvalidRequest,
            "tmux pane ID must be '%' followed by decimal digits",
        ))
    }
}

fn read_socket_identity(socket_path: &str) -> Result<SocketIdentity, ProtocolError> {
    current_socket_identity(socket_path).map_err(|error| {
        ProtocolError::new(
            ErrorCode::BackendUnavailable,
            format!("failed to inspect tmux socket {socket_path:?}: {error}"),
        )
    })
}

fn current_socket_identity(socket_path: &str) -> io::Result<SocketIdentity> {
    let metadata = fs::metadata(socket_path)?;
    if !metadata.file_type().is_socket() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "tmux socket path does not name a Unix socket",
        ));
    }
    Ok(SocketIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    })
}

pub(crate) fn socket_identity_matches(socket_path: &str, expected: SocketIdentity) -> bool {
    current_socket_identity(socket_path).ok() == Some(expected)
}

#[derive(Debug, Eq, PartialEq)]
struct TargetIdentity {
    server_pid: u32,
    server_started_at: u64,
    session_id: String,
    window_id: String,
    pane_id: String,
    pane_pid: u32,
}

impl From<&TmuxPaneInfo> for TargetIdentity {
    fn from(target: &TmuxPaneInfo) -> Self {
        Self {
            server_pid: target.server_pid,
            server_started_at: target.server_started_at,
            session_id: target.session_id.clone(),
            window_id: target.window_id.clone(),
            pane_id: target.pane_id.clone(),
            pane_pid: target.pane_pid,
        }
    }
}

pub(crate) fn target_probe_command(pane_id: &str) -> String {
    format!("display-message -p -t {pane_id} '{TARGET_IDENTITY_FORMAT}'\n")
}

pub(crate) fn target_identity_matches(
    target: &TmuxPaneInfo,
    output: &[u8],
) -> Result<bool, String> {
    let fields = output.split(|byte| *byte == b'\t').collect::<Vec<_>>();
    if fields.len() != 6 {
        return Err("tmux target probe returned an unexpected field count".to_owned());
    }
    let found = TargetIdentity {
        server_pid: parse_control_number(fields[0], "server PID")?,
        server_started_at: parse_control_number(fields[1], "server start time")?,
        session_id: control_id(fields[2], b'$', "session")?,
        window_id: control_id(fields[3], b'@', "window")?,
        pane_id: control_id(fields[4], b'%', "pane")?,
        pane_pid: parse_control_number(fields[5], "pane PID")?,
    };
    Ok(found == TargetIdentity::from(target))
}

fn parse_control_number<T>(value: &[u8], label: &str) -> Result<T, String>
where
    T: std::str::FromStr,
{
    std::str::from_utf8(value)
        .ok()
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| format!("tmux target probe returned an invalid {label}"))
}

fn parse_u16(value: &[u8], label: &str) -> Result<u16, ProtocolError> {
    parse_number(value, label)
}

fn parse_u32(value: &[u8], label: &str) -> Result<u32, ProtocolError> {
    parse_number(value, label)
}

fn parse_u64(value: &[u8], label: &str) -> Result<u64, ProtocolError> {
    parse_number(value, label)
}

fn parse_number<T>(value: &[u8], label: &str) -> Result<T, ProtocolError>
where
    T: std::str::FromStr,
{
    std::str::from_utf8(value)
        .ok()
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| {
            ProtocolError::new(
                ErrorCode::BackendUnavailable,
                format!("tmux returned an invalid {label}"),
            )
        })
}

fn backend_error(action: &str, error: impl std::fmt::Display) -> ProtocolError {
    ProtocolError::new(
        ErrorCode::BackendUnavailable,
        format!("failed to {action}: {error}"),
    )
}

fn bounded_stderr(stderr: &[u8]) -> String {
    const LIMIT: usize = 4096;
    let mut value = String::from_utf8_lossy(&stderr[..stderr.len().min(LIMIT)])
        .trim()
        .to_owned();
    if stderr.len() > LIMIT {
        value.push_str(" [truncated]");
    }
    value
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ControlItem {
    Output {
        pane_id: String,
        data: Vec<u8>,
        age_millis: Option<u64>,
    },
    CommandResult {
        number: u64,
        success: bool,
        output: Vec<Vec<u8>>,
    },
    SessionChanged {
        session_id: String,
    },
    SessionRenamed {
        session_id: String,
        name: Vec<u8>,
    },
    WindowClosed {
        window_id: String,
    },
    Paused {
        pane_id: String,
    },
    Continued {
        pane_id: String,
    },
    Exit,
    Notification,
}

#[derive(Default)]
pub(crate) struct ControlParser {
    block: Option<CommandBlock>,
}

struct CommandBlock {
    time: u64,
    number: u64,
    flags: u64,
    payload_bytes: usize,
    line_count: usize,
    output: Vec<Vec<u8>>,
}

impl CommandBlock {
    fn push_output(&mut self, line: &[u8]) -> Result<(), String> {
        if self.line_count >= MAX_COMMAND_BLOCK_LINES {
            return Err("tmux command block has too many output lines".to_owned());
        }
        let payload_bytes = self
            .payload_bytes
            .checked_add(line.len())
            .ok_or_else(|| "tmux command block output exceeds 1 MiB".to_owned())?;
        if payload_bytes > MAX_CONTROL_LINE_BYTES {
            return Err("tmux command block output exceeds 1 MiB".to_owned());
        }
        self.payload_bytes = payload_bytes;
        self.line_count += 1;
        self.output.push(line.to_vec());
        Ok(())
    }
}

impl ControlParser {
    pub(crate) fn parse_line(&mut self, line: &[u8]) -> Result<Option<ControlItem>, String> {
        if let Some(block) = &mut self.block {
            if line.starts_with(b"%begin ") {
                return Err("nested tmux command block".to_owned());
            }
            if line.starts_with(b"%end ") || line.starts_with(b"%error ") {
                let success = line.starts_with(b"%end ");
                let header = parse_block_header(if success { b"%end " } else { b"%error " }, line)?;
                if (block.time, block.number, block.flags) != header {
                    return Err("tmux command block terminator does not match begin".to_owned());
                }
                let completed = self.block.take().expect("tmux block is present");
                return Ok(Some(ControlItem::CommandResult {
                    number: completed.number,
                    success,
                    output: completed.output,
                }));
            }
            block.push_output(line)?;
            return Ok(None);
        }

        if line.is_empty() {
            return Ok(None);
        }
        if line.starts_with(b"%begin ") {
            let (time, number, flags) = parse_block_header(b"%begin ", line)?;
            self.block = Some(CommandBlock {
                time,
                number,
                flags,
                payload_bytes: 0,
                line_count: 0,
                output: Vec::new(),
            });
            return Ok(None);
        }
        if let Some(rest) = line.strip_prefix(b"%output ") {
            let (pane, value) = split_once(rest, b' ')
                .ok_or_else(|| "malformed tmux output notification".to_owned())?;
            return Ok(Some(ControlItem::Output {
                pane_id: control_id(pane, b'%', "pane")?,
                data: decode_octal(value)?,
                age_millis: None,
            }));
        }
        if let Some(rest) = line.strip_prefix(b"%extended-output ") {
            let separator = rest
                .windows(3)
                .position(|window| window == b" : ")
                .ok_or_else(|| "malformed tmux extended-output separator".to_owned())?;
            let header = &rest[..separator];
            let value = &rest[separator + 3..];
            let mut fields = header.split(|byte| *byte == b' ');
            let pane = fields
                .next()
                .ok_or_else(|| "tmux extended-output has no pane".to_owned())?;
            let age = fields
                .next()
                .and_then(|value| std::str::from_utf8(value).ok())
                .and_then(|value| value.parse().ok())
                .ok_or_else(|| "tmux extended-output has invalid age".to_owned())?;
            return Ok(Some(ControlItem::Output {
                pane_id: control_id(pane, b'%', "pane")?,
                data: decode_octal(value)?,
                age_millis: Some(age),
            }));
        }
        if let Some(rest) = line.strip_prefix(b"%session-changed ") {
            let (session, _) = split_once(rest, b' ')
                .ok_or_else(|| "malformed tmux session-changed notification".to_owned())?;
            return Ok(Some(ControlItem::SessionChanged {
                session_id: control_id(session, b'$', "session")?,
            }));
        }
        if let Some(rest) = line.strip_prefix(b"%session-renamed ") {
            return parse_session_renamed(rest).map(Some);
        }
        if let Some(window) = line.strip_prefix(b"%window-close ") {
            return Ok(Some(ControlItem::WindowClosed {
                window_id: control_id(window, b'@', "window")?,
            }));
        }
        if let Some(pane) = line.strip_prefix(b"%pause ") {
            return Ok(Some(ControlItem::Paused {
                pane_id: control_id(pane, b'%', "pane")?,
            }));
        }
        if let Some(pane) = line.strip_prefix(b"%continue ") {
            return Ok(Some(ControlItem::Continued {
                pane_id: control_id(pane, b'%', "pane")?,
            }));
        }
        if line == b"%exit" || line.starts_with(b"%exit ") {
            return Ok(Some(ControlItem::Exit));
        }
        reject_stray_block_terminator(line)?;
        if line.starts_with(b"%") {
            return Ok(Some(ControlItem::Notification));
        }
        Err("tmux emitted an unframed control line".to_owned())
    }
}

fn reject_stray_block_terminator(line: &[u8]) -> Result<(), String> {
    if line.starts_with(b"%end ") || line.starts_with(b"%error ") {
        Err("tmux command block terminator without begin".to_owned())
    } else {
        Ok(())
    }
}

fn parse_session_renamed(value: &[u8]) -> Result<ControlItem, String> {
    let (session, name) = split_once(value, b' ')
        .ok_or_else(|| "malformed tmux session-renamed notification".to_owned())?;
    Ok(ControlItem::SessionRenamed {
        session_id: control_id(session, b'$', "session")?,
        name: decode_octal(name)?,
    })
}

fn parse_block_header(prefix: &[u8], line: &[u8]) -> Result<(u64, u64, u64), String> {
    let fields = line
        .strip_prefix(prefix)
        .ok_or_else(|| "invalid tmux command block prefix".to_owned())?
        .split(|byte| *byte == b' ')
        .collect::<Vec<_>>();
    if fields.len() != 3 {
        return Err("invalid tmux command block header".to_owned());
    }
    Ok((
        ascii_u64(fields[0])?,
        ascii_u64(fields[1])?,
        ascii_u64(fields[2])?,
    ))
}

fn ascii_u64(value: &[u8]) -> Result<u64, String> {
    std::str::from_utf8(value)
        .ok()
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| "invalid tmux control integer".to_owned())
}

fn control_id(value: &[u8], prefix: u8, label: &str) -> Result<String, String> {
    if value.first() != Some(&prefix)
        || value.len() == 1
        || !value[1..].iter().all(u8::is_ascii_digit)
    {
        return Err(format!("invalid tmux {label} ID"));
    }
    String::from_utf8(value.to_vec()).map_err(|_| format!("non-UTF-8 tmux {label} ID"))
}

fn decode_octal(value: &[u8]) -> Result<Vec<u8>, String> {
    let mut decoded = Vec::with_capacity(value.len());
    let mut index = 0;
    while index < value.len() {
        if value[index] != b'\\' {
            decoded.push(value[index]);
            index += 1;
            continue;
        }
        let digits = value
            .get(index + 1..index + 4)
            .ok_or_else(|| "truncated tmux octal escape".to_owned())?;
        if !digits.iter().all(|digit| matches!(digit, b'0'..=b'7')) {
            return Err("invalid tmux octal escape".to_owned());
        }
        let octal = u16::from(digits[0] - b'0') * 64
            + u16::from(digits[1] - b'0') * 8
            + u16::from(digits[2] - b'0');
        decoded.push(
            u8::try_from(octal).map_err(|_| "tmux octal escape exceeds one byte".to_owned())?,
        );
        index += 4;
    }
    Ok(decoded)
}

fn split_once(value: &[u8], separator: u8) -> Option<(&[u8], &[u8])> {
    let index = value.iter().position(|byte| *byte == separator)?;
    Some((&value[..index], &value[index + 1..]))
}

pub(crate) fn read_bounded_line(
    reader: &mut impl BufRead,
    line: &mut Vec<u8>,
) -> io::Result<usize> {
    line.clear();
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return Ok(line.len());
        }
        let take = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(available.len(), |index| index + 1);
        if line.len().saturating_add(take) > MAX_CONTROL_LINE_BYTES {
            reader.consume(take);
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "tmux Control Mode line exceeds 1 MiB",
            ));
        }
        line.extend_from_slice(&available[..take]);
        reader.consume(take);
        if line.last() == Some(&b'\n') {
            line.pop();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            return Ok(line.len());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::BufReader;

    use serde_json::Value;

    use super::{ControlItem, ControlParser, ParsedVersion, read_bounded_line};

    fn transcript_fixture() -> Value {
        let fixture: Value =
            serde_json::from_str(include_str!("../../../fixtures/tmux-control-mode.json"))
                .expect("decode checked-in tmux Control Mode fixture");
        assert_eq!(fixture["schema"], "ctxmux.tmux-control-mode-transcript.v1");
        fixture
    }

    fn fixture_bytes(value: &Value) -> Vec<u8> {
        value
            .as_array()
            .expect("fixture byte sequence is an array")
            .iter()
            .map(|byte| {
                u8::try_from(byte.as_u64().expect("fixture byte is an unsigned integer"))
                    .expect("fixture byte is in range")
            })
            .collect()
    }

    #[test]
    fn released_version_parser_accepts_only_exact_release_grammar() {
        assert_eq!(
            ParsedVersion::parse("3.6b"),
            Some(ParsedVersion { major: 3, minor: 6 })
        );
        assert_eq!(
            ParsedVersion::parse("3.5a"),
            Some(ParsedVersion { major: 3, minor: 5 })
        );
        assert_eq!(
            ParsedVersion::parse("3.4"),
            Some(ParsedVersion { major: 3, minor: 4 })
        );
        for invalid in [
            "",
            "3",
            ".4",
            "3.",
            "next-3.6",
            "3.6-dev",
            "3.4anything",
            "3.4ab",
            "3.4.1",
            "3.4A",
        ] {
            assert_eq!(
                ParsedVersion::parse(invalid),
                None,
                "accepted invalid released version {invalid:?}"
            );
        }
    }

    #[test]
    fn transcript_parser_separates_blocks_notifications_and_exact_pane_bytes() {
        let fixture = transcript_fixture();
        let mut parser = ControlParser::default();
        let mut items = Vec::new();
        for line in fixture["valid_transcript"]["lines"]
            .as_array()
            .expect("valid transcript lines are an array")
        {
            if let Some(item) = parser
                .parse_line(
                    line.as_str()
                        .expect("valid transcript line is a string")
                        .as_bytes(),
                )
                .expect("parse transcript line")
            {
                items.push(item);
            }
        }
        let expected = &fixture["valid_transcript"]["expected"];
        assert_eq!(
            items,
            vec![
                ControlItem::CommandResult {
                    number: 7,
                    success: true,
                    output: vec![b"%output is command text inside a block".to_vec()],
                },
                ControlItem::SessionChanged {
                    session_id: "$1".to_owned(),
                },
                ControlItem::Output {
                    pane_id: "%2".to_owned(),
                    data: fixture_bytes(&expected["primary_output_bytes"]),
                    age_millis: None,
                },
                ControlItem::Output {
                    pane_id: "%2".to_owned(),
                    data: fixture_bytes(&expected["extended_output_bytes"]),
                    age_millis: Some(42),
                },
                ControlItem::SessionRenamed {
                    session_id: "$1".to_owned(),
                    name: fixture_bytes(&expected["session_name_bytes"]),
                },
                ControlItem::SessionRenamed {
                    session_id: "$9".to_owned(),
                    name: fixture_bytes(&expected["unrelated_session_name_bytes"]),
                },
                ControlItem::WindowClosed {
                    window_id: "@4".to_owned(),
                },
                ControlItem::Paused {
                    pane_id: "%2".to_owned(),
                },
                ControlItem::Continued {
                    pane_id: "%2".to_owned(),
                },
                ControlItem::Notification,
                ControlItem::Exit,
            ]
        );
    }

    #[test]
    fn malformed_or_oversized_control_records_fail_boundedly() {
        let fixture = transcript_fixture();
        assert_malformed_transcript_cases(&fixture);
        assert_generated_malformed_cases(&fixture);
        assert_bounded_line_cases(&fixture);
    }

    fn assert_malformed_transcript_cases(fixture: &Value) {
        for case in fixture["malformed_transcripts"]
            .as_array()
            .expect("malformed transcripts are an array")
        {
            let case_id = case["id"].as_str().expect("malformed case has an ID");
            let mut parser = ControlParser::default();
            let error = case["lines"]
                .as_array()
                .expect("malformed transcript lines are an array")
                .iter()
                .find_map(|line| {
                    parser
                        .parse_line(
                            line.as_str()
                                .expect("malformed transcript line is a string")
                                .as_bytes(),
                        )
                        .err()
                });
            assert_eq!(
                error.as_deref(),
                case["expected_error"].as_str(),
                "malformed fixture {case_id}"
            );
        }
    }

    fn assert_generated_malformed_cases(fixture: &Value) {
        for case in fixture["generated_malformed_transcripts"]
            .as_array()
            .expect("generated malformed transcripts are an array")
        {
            let case_id = case["id"]
                .as_str()
                .expect("generated malformed case has an ID");
            let mut parser = ControlParser::default();
            assert_eq!(
                parser
                    .parse_line(
                        case["begin"]
                            .as_str()
                            .expect("generated malformed case has a begin line")
                            .as_bytes(),
                    )
                    .expect("parse generated command block begin"),
                None
            );
            let line = vec![
                u8::try_from(
                    case["line_byte"]
                        .as_u64()
                        .expect("generated malformed case has a line byte"),
                )
                .expect("generated malformed line byte is in range");
                usize::try_from(
                    case["line_bytes"]
                        .as_u64()
                        .expect("generated malformed case has a line size"),
                )
                .expect("generated malformed line size fits usize")
            ];
            let repeat = usize::try_from(
                case["repeat"]
                    .as_u64()
                    .expect("generated malformed case has a repeat count"),
            )
            .expect("generated malformed repeat count fits usize");
            let mut error = None;
            for _ in 0..repeat {
                match parser.parse_line(&line) {
                    Ok(None) => {}
                    Ok(Some(item)) => {
                        panic!("generated malformed fixture {case_id} emitted {item:?}")
                    }
                    Err(found) => {
                        error = Some(found);
                        break;
                    }
                }
            }
            assert_eq!(
                error.as_deref(),
                case["expected_error"].as_str(),
                "generated malformed fixture {case_id}"
            );
        }
    }

    fn assert_bounded_line_cases(fixture: &Value) {
        for case in fixture["bounded_lines"]
            .as_array()
            .expect("bounded lines are an array")
        {
            let case_id = case["id"].as_str().expect("bounded case has an ID");
            let payload_bytes = usize::try_from(
                case["payload_bytes"]
                    .as_u64()
                    .expect("bounded case has a byte count"),
            )
            .expect("bounded case byte count fits usize");
            let mut bytes = vec![b'x'; payload_bytes];
            match case["terminator"]
                .as_str()
                .expect("bounded case has a terminator")
            {
                "none" => {}
                "lf" => bytes.push(b'\n'),
                "crlf" => bytes.extend_from_slice(b"\r\n"),
                terminator => panic!("unknown fixture terminator {terminator:?}"),
            }
            let mut reader = BufReader::new(bytes.as_slice());
            let mut line = Vec::new();
            let result = read_bounded_line(&mut reader, &mut line);
            if case["accepted"]
                .as_bool()
                .expect("bounded case has an accepted flag")
            {
                assert_eq!(
                    result.expect("accept bounded fixture line"),
                    payload_bytes,
                    "bounded fixture {case_id}"
                );
                assert_eq!(line.len(), payload_bytes, "bounded fixture {case_id}");
            } else {
                let error = result.expect_err("reject oversized fixture line");
                assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
                assert_eq!(error.to_string(), "tmux Control Mode line exceeds 1 MiB");
                assert!(
                    line.len() <= super::MAX_CONTROL_LINE_BYTES,
                    "bounded fixture {case_id} retained too much input"
                );
            }
        }
    }
}
