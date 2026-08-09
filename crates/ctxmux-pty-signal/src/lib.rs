//! Narrow audited PTY foreground signalling for ctxmux.
//!
//! Product crates keep `unsafe_code = "forbid"`. This private leaf owns the
//! target-local `TIOCSIG` call that asks the macOS tty driver to select and
//! signal the retained PTY's current foreground process group atomically.

#![deny(unsafe_code)]

#[cfg(target_os = "macos")]
use std::{io, os::fd::RawFd};

/// Ask the retained macOS PTY master to deliver `SIGINT` to its current
/// foreground process group.
///
/// # Errors
///
/// Returns an OS error when the descriptor is invalid, is not a PTY master, or
/// the tty no longer has a signalable foreground process group.
#[cfg(target_os = "macos")]
#[allow(unsafe_code)]
pub fn interrupt_foreground(raw_fd: RawFd) -> io::Result<()> {
    if raw_fd < 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "PTY descriptor must not be negative",
        ));
    }
    // SAFETY: ioctl borrows the descriptor for this call. TIOCSIG consumes the
    // integer signal value and does not retain a userspace pointer.
    let result = unsafe { libc::ioctl(raw_fd, libc::TIOCSIG.into(), libc::SIGINT) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(all(test, target_os = "macos"))]
mod tests {
    #[test]
    fn negative_descriptor_is_rejected_before_ioctl() {
        let error = super::interrupt_foreground(-1).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    }
}
