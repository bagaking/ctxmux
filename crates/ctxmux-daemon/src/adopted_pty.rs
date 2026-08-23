//! Adapter that presents an inherited raw PTY master fd through the same
//! control surface as a freshly spawned `portable_pty` master.
//!
//! `portable_pty` exposes no constructor to rebuild a `MasterPty` from a bare
//! descriptor. After the daemon `execve`s itself in place, each live PTY Run is
//! re-adopted from its inherited raw master fd. [`AdoptedMasterPty`] wraps that
//! descriptor as an [`OwnedFd`] and drives it with rustix's safe `termios`
//! wrappers, so the recovered control owner behaves exactly like a freshly
//! spawned one — resize, size read-back, and foreground signalling all go
//! straight to the live kernel descriptor.
//!
//! The wrapper holds an [`OwnedFd`], never a raw integer: the descriptor is
//! closed exactly once, on drop. The daemon crate keeps `unsafe_code =
//! "forbid"`, so every fd operation here routes through a safe wrapper — the
//! audited `ctxmux_inherited_fd` seam produces the `OwnedFd`, and
//! `rustix::termios` performs the ioctls.

use std::io;
use std::os::fd::{AsRawFd, OwnedFd, RawFd};

use portable_pty::PtySize;
use rustix::termios::{Winsize, tcgetwinsize, tcsetwinsize};

/// A PTY master control surface backed by an inherited, owned descriptor.
///
/// Constructed from an [`OwnedFd`] duplicated across the exec-in-place handoff
/// (via `ctxmux_inherited_fd`), it exposes the same operations a freshly
/// spawned `portable_pty` master offers: [`resize`](Self::resize),
/// [`get_size`](Self::get_size), and platform-split foreground signalling.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct AdoptedMasterPty {
    fd: OwnedFd,
}

#[cfg_attr(not(test), allow(dead_code))]
impl AdoptedMasterPty {
    /// Adopt an already-owned inherited master descriptor.
    ///
    /// The `OwnedFd` must be obtained through the audited
    /// `ctxmux_inherited_fd` seam (`duplicate_cloexec` /
    /// `claim_inherited_process_fd`);
    /// this type never converts a raw integer itself.
    pub(crate) fn from_owned_fd(fd: OwnedFd) -> Self {
        Self { fd }
    }

    /// Resize the live terminal by writing the window size to the master.
    ///
    /// # Errors
    ///
    /// Returns the operating-system error when the `TIOCSWINSZ` ioctl fails —
    /// notably `ENOTTY` if the descriptor is not a tty master.
    pub(crate) fn resize(&self, size: PtySize) -> io::Result<()> {
        tcsetwinsize(&self.fd, winsize_from(size))?;
        Ok(())
    }

    /// Read the current window size back from the live master.
    ///
    /// # Errors
    ///
    /// Returns the operating-system error when the `TIOCGWINSZ` ioctl fails.
    pub(crate) fn get_size(&self) -> io::Result<PtySize> {
        let winsize = tcgetwinsize(&self.fd)?;
        Ok(pty_size_from(winsize))
    }

    /// Borrowed raw fd number of the adopted master. Always available while
    /// the wrapper is alive; no dup and no ownership transfer. The
    /// `PtyControl` glue wraps this in `Some` to satisfy the trait's
    /// possibly-detached contract.
    pub(crate) fn master_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }

    /// Ask the retained macOS master to deliver `SIGINT` to its current
    /// foreground process group.
    ///
    /// # Errors
    ///
    /// Returns the operating-system error when the descriptor is not a signal
    /// -able PTY master (delegates to `ctxmux_pty_signal::interrupt_foreground`).
    #[cfg(target_os = "macos")]
    pub(crate) fn interrupt_foreground(&self) -> io::Result<()> {
        ctxmux_pty_signal::interrupt_foreground(self.fd.as_raw_fd())
    }

    /// Foreground process group of the adopted tty, or `None` when the tty has
    /// no signalable foreground group.
    #[cfg(not(target_os = "macos"))]
    pub(crate) fn foreground_process_group(&self) -> Option<u32> {
        rustix::termios::tcgetpgrp(&self.fd)
            .ok()
            .and_then(|pid| u32::try_from(pid.as_raw_nonzero().get()).ok())
    }
}

/// Map a `portable_pty` size onto the kernel `winsize` layout.
fn winsize_from(size: PtySize) -> Winsize {
    Winsize {
        ws_row: size.rows,
        ws_col: size.cols,
        ws_xpixel: size.pixel_width,
        ws_ypixel: size.pixel_height,
    }
}

/// Map a kernel `winsize` back onto a `portable_pty` size.
fn pty_size_from(winsize: Winsize) -> PtySize {
    PtySize {
        rows: winsize.ws_row,
        cols: winsize.ws_col,
        pixel_width: winsize.ws_xpixel,
        pixel_height: winsize.ws_ypixel,
    }
}

#[cfg(test)]
mod tests {
    use portable_pty::PtySize;

    use super::AdoptedMasterPty;

    #[test]
    fn adopts_a_live_master_fd_and_round_trips_a_resize() {
        let pair = portable_pty::native_pty_system()
            .openpty(PtySize {
                rows: 24,
                cols: 80,
                pixel_width: 0,
                pixel_height: 0,
            })
            .unwrap();
        // Keep the slave end alive so the kernel pty pair is not torn down.
        let _slave = pair.slave;

        let raw = pair.master.as_raw_fd().unwrap();
        // Dup the master into an owned handle WITHOUT consuming `pair.master`.
        let owned = ctxmux_inherited_fd::duplicate_cloexec(raw).unwrap();

        let adopted = AdoptedMasterPty::from_owned_fd(owned);

        // A passing resize + read-back proves this is the *live* descriptor,
        // not a cached value: a plain pipe fd would return ENOTTY here.
        assert!(
            adopted
                .resize(PtySize {
                    rows: 40,
                    cols: 132,
                    pixel_width: 0,
                    pixel_height: 0,
                })
                .is_ok(),
            "resizing a live tty master must succeed"
        );

        let size = adopted.get_size().unwrap();
        assert_eq!(size.rows, 40);
        assert_eq!(size.cols, 132);

        // `from_owned_fd` retained the dup'd descriptor, distinct from the
        // original master it was duplicated from.
        assert_ne!(adopted.master_raw_fd(), raw);

        // `pair.master` (and `_slave`) stay in scope to end of the test so the
        // kernel pty pair is not torn down mid round-trip; the dup already
        // keeps the master open independently.
        let _master = pair.master;
    }
}
