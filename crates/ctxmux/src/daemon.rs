use std::{
    env,
    ffi::OsStr,
    io,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::{Duration, Instant},
};

use ctxmux_client::{Client, ClientError};

const READY_TIMEOUT: Duration = Duration::from_secs(15);
const READY_POLL: Duration = Duration::from_millis(50);

pub(crate) fn default_socket_path() -> PathBuf {
    default_socket_path_from(
        env::var_os("XDG_RUNTIME_DIR").filter(|value| !value.is_empty()),
        &env::temp_dir(),
    )
}

pub(crate) fn default_socket_path_from(
    xdg_runtime_dir: Option<impl AsRef<OsStr>>,
    temp_dir: &Path,
) -> PathBuf {
    match xdg_runtime_dir {
        Some(dir) => PathBuf::from(dir.as_ref())
            .join("ctxmux")
            .join("ctxmux.sock"),
        None => temp_dir.join("ctxmux").join("ctxmux.sock"),
    }
}

pub(crate) async fn ensure_listening(socket: &Path) -> Result<(), String> {
    let client = Client::new(socket);
    match client.ping().await {
        Ok(()) => return Ok(()),
        Err(error) if needs_spawn(&error) => {}
        Err(error) => return Err(error.to_string()),
    }
    spawn_daemon(socket)?;
    let deadline = Instant::now() + READY_TIMEOUT;
    loop {
        match client.ping().await {
            Ok(()) => return Ok(()),
            Err(error) if needs_spawn(&error) => {}
            Err(error) => return Err(error.to_string()),
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "ctxmuxd did not become ready at {} within {}s",
                socket.display(),
                READY_TIMEOUT.as_secs()
            ));
        }
        tokio::time::sleep(READY_POLL).await;
    }
}

fn needs_spawn(error: &ClientError) -> bool {
    match error {
        ClientError::Connect { source, .. } => matches!(
            source.kind(),
            io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
        ),
        _ => false,
    }
}

fn spawn_daemon(socket: &Path) -> Result<(), String> {
    let bin = daemon_executable()?;
    let mut command = Command::new(&bin);
    command
        .arg("--socket")
        .arg(socket)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    command
        .spawn()
        .map_err(|error| format!("failed to start {}: {error}", bin.display()))?;
    Ok(())
}

fn daemon_executable() -> Result<PathBuf, String> {
    let exe = env::current_exe().map_err(|error| format!("failed to locate ctxmux: {error}"))?;
    let sibling = exe.with_file_name("ctxmuxd");
    if sibling.is_file() {
        return Ok(sibling);
    }
    find_in_path("ctxmuxd")
        .ok_or_else(|| format!("ctxmuxd not found next to {} or on PATH", exe.display()))
}

fn find_in_path(name: &str) -> Option<PathBuf> {
    let path = env::var_os("PATH")?;
    env::split_paths(&path).find_map(|dir| {
        let candidate = dir.join(name);
        candidate.is_file().then_some(candidate)
    })
}

#[cfg(test)]
mod tests {
    use super::default_socket_path_from;
    use std::path::Path;

    #[test]
    fn default_socket_prefers_xdg_runtime_dir() {
        let path = default_socket_path_from(Some("/tmp/runtime"), Path::new("/var/tmp"));
        assert_eq!(path, Path::new("/tmp/runtime/ctxmux/ctxmux.sock"));
    }

    #[test]
    fn default_socket_falls_back_to_process_temp() {
        let path = default_socket_path_from(None::<&str>, Path::new("/var/tmp/ctxmux-test"));
        assert_eq!(path, Path::new("/var/tmp/ctxmux-test/ctxmux/ctxmux.sock"));
    }
}
