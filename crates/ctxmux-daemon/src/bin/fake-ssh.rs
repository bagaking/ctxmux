//! Test-only stand-in for `ssh -L <local>:<remote> -N <destination>`.
//!
//! The real endpoint runs the maintained system OpenSSH client. That is the
//! shipped behavior and the real fixture exercises it. This binary exists only
//! so the *supervision* contract — readiness, owner-only socket placement,
//! process teardown, cleanup, and the fact that losing the forwarder is not
//! lifecycle truth — can be proven on a machine with no SSH loopback, and in
//! CI, without weakening those assertions.
//!
//! It deliberately accepts the same argument shape as the real client, ignoring
//! the options it does not need, so the production argument builder stays under
//! test rather than being bypassed by a bespoke test path.
//!
//! It also matches the real client on *when* a dead owner is discovered, which
//! is easy to get wrong in the strict direction. `ExitOnForwardFailure` fires
//! when `ssh` cannot establish the forward, and for `-L` that means the local
//! bind: a `StreamLocal` forward whose remote socket has no listener still binds
//! locally and stays up, and the absence surfaces per connection. So a failed
//! bind exits this process, while an unreachable remote is relayed as a closed
//! connection rather than pre-checked. An earlier version refused to start when
//! the remote path was missing, which made an endpoint test assert a stricter
//! fail-closed point than the shipped transport has.
//!
//! It authenticates nothing and must never be used outside tests.

use std::{
    env,
    path::{Path, PathBuf},
    process::ExitCode,
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
};

fn main() -> ExitCode {
    let Some(forward) = parse_forward(env::args_os().skip(1).map(Into::into).collect()) else {
        eprintln!("fake-ssh: expected -L <local-socket>:<remote-socket>");
        return ExitCode::FAILURE;
    };

    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("fake-ssh: failed to build runtime: {error}");
            return ExitCode::FAILURE;
        }
    };
    match runtime.block_on(serve(&forward.local, &forward.remote)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("fake-ssh: {error}");
            ExitCode::FAILURE
        }
    }
}

struct Forward {
    local: PathBuf,
    remote: PathBuf,
}

/// Extract the `-L local:remote` pair, ignoring every other argument.
fn parse_forward(args: Vec<PathBuf>) -> Option<Forward> {
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        if arg.as_os_str() != "-L" {
            continue;
        }
        let spec = iter.next()?;
        let rendered = spec.to_string_lossy().into_owned();
        // The production builder refuses a remote path containing ':', so the
        // last separator unambiguously splits the pair.
        let (local, remote) = rendered.rsplit_once(':')?;
        if local.is_empty() || remote.is_empty() {
            return None;
        }
        return Some(Forward {
            local: PathBuf::from(local),
            remote: PathBuf::from(remote),
        });
    }
    None
}

async fn serve(local: &Path, remote: &Path) -> Result<(), String> {
    let listener = UnixListener::bind(local)
        .map_err(|error| format!("failed to bind {}: {error}", local.display()))?;
    loop {
        let (client, _) = listener
            .accept()
            .await
            .map_err(|error| format!("failed to accept on {}: {error}", local.display()))?;
        let remote = remote.to_path_buf();
        tokio::spawn(async move {
            if let Ok(upstream) = UnixStream::connect(&remote).await {
                relay(client, upstream).await;
            }
        });
    }
}

/// Copy bytes in both directions until either side closes.
///
/// `copy_bidirectional` would stop as soon as one direction ends, which can cut
/// a reply short; the daemon protocol expects each half to close independently.
async fn relay(client: UnixStream, upstream: UnixStream) {
    let (mut client_read, mut client_write) = client.into_split();
    let (mut upstream_read, mut upstream_write) = upstream.into_split();
    let to_upstream = tokio::spawn(async move {
        pump(&mut client_read, &mut upstream_write).await;
    });
    let to_client = tokio::spawn(async move {
        pump(&mut upstream_read, &mut client_write).await;
    });
    let _ = to_upstream.await;
    let _ = to_client.await;
}

async fn pump<R, W>(reader: &mut R, writer: &mut W)
where
    R: AsyncReadExt + Unpin,
    W: AsyncWriteExt + Unpin,
{
    let mut buffer = vec![0_u8; 16 * 1024];
    loop {
        match reader.read(&mut buffer).await {
            Ok(0) | Err(_) => break,
            Ok(read) => {
                if writer.write_all(&buffer[..read]).await.is_err() {
                    break;
                }
            }
        }
    }
    let _ = writer.shutdown().await;
}
