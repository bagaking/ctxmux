//! Narrow audited duplication for a harness-inherited descriptor.
//!
//! The daemon crate keeps `unsafe_code = "forbid"`. This private leaf owns the
//! one raw-descriptor conversion that the process-spawn contract requires.

#![deny(unsafe_code)]

use std::os::fd::{FromRawFd, OwnedFd, RawFd};

use rustix::fs::{OFlags, fcntl_getfl, fcntl_setfl};

/// Duplicate one process descriptor and fence both copies from `exec`.
///
/// # Errors
///
/// Rejects standard or closed descriptors. The caller retains ownership of the
/// original descriptor; this function owns only the duplicate it returns.
#[allow(unsafe_code)]
pub fn duplicate_cloexec(raw_fd: RawFd) -> std::io::Result<OwnedFd> {
    if raw_fd < 3 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "descriptor must be at least 3",
        ));
    }
    // F_DUPFD_CLOEXEC returns a fresh descriptor number. Calling fcntl on a
    // raw process descriptor does not claim or close the caller's owner.
    let duplicated = unsafe { libc::fcntl(raw_fd, libc::F_DUPFD_CLOEXEC, 3) };
    if duplicated < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: a successful F_DUPFD_CLOEXEC created this fresh descriptor and
    // no other Rust value owns the duplicate.
    let owned = unsafe { OwnedFd::from_raw_fd(duplicated) };
    let original_flags = unsafe { libc::fcntl(raw_fd, libc::F_GETFD) };
    if original_flags < 0 {
        return Err(std::io::Error::last_os_error());
    }
    if unsafe { libc::fcntl(raw_fd, libc::F_SETFD, original_flags | libc::FD_CLOEXEC) } < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(owned)
}

/// Duplicate one process-start descriptor, make its shared open-file
/// description nonblocking, and fence both copies from `exec`.
///
/// # Errors
///
/// Returns an operating-system error when duplication or flag changes fail.
pub fn duplicate_nonblocking_cloexec(raw_fd: RawFd) -> std::io::Result<OwnedFd> {
    let owned = duplicate_cloexec(raw_fd)?;
    let status_flags = fcntl_getfl(&owned)?;
    fcntl_setfl(&owned, status_flags | OFlags::NONBLOCK)?;
    Ok(owned)
}

/// Clear the close-on-exec flag on a descriptor so it survives `execve`.
///
/// The caller retains ownership; this only mutates the descriptor's flags.
///
/// # Errors
///
/// Rejects standard descriptors and returns an operating-system error when the
/// flag read or write fails.
#[allow(unsafe_code)]
pub fn clear_cloexec(raw_fd: RawFd) -> std::io::Result<()> {
    if raw_fd < 3 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "descriptor must be at least 3",
        ));
    }
    let flags = unsafe { libc::fcntl(raw_fd, libc::F_GETFD) };
    if flags < 0 {
        return Err(std::io::Error::last_os_error());
    }
    if unsafe { libc::fcntl(raw_fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) } < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{io::Read, os::fd::AsRawFd};

    use rustix::{
        fs::{OFlags, fcntl_getfl},
        io::{FdFlags, fcntl_getfd},
        pipe::pipe,
    };

    use super::{duplicate_cloexec, duplicate_nonblocking_cloexec};

    #[test]
    fn blocking_duplicate_preserves_shared_status_flags() {
        let (reader, writer) = pipe().unwrap();
        let owned = duplicate_cloexec(writer.as_raw_fd()).unwrap();
        assert!(!fcntl_getfl(&owned).unwrap().contains(OFlags::NONBLOCK));
        assert!(!fcntl_getfl(&writer).unwrap().contains(OFlags::NONBLOCK));
        assert!(fcntl_getfd(&owned).unwrap().contains(FdFlags::CLOEXEC));
        assert!(fcntl_getfd(&writer).unwrap().contains(FdFlags::CLOEXEC));
        drop(owned);
        drop(reader);
    }

    #[test]
    fn inherited_descriptor_becomes_nonblocking_and_cloexec() {
        let (reader, writer) = pipe().unwrap();
        let owned = duplicate_nonblocking_cloexec(writer.as_raw_fd()).unwrap();
        assert!(fcntl_getfd(&owned).unwrap().contains(FdFlags::CLOEXEC));
        assert!(fcntl_getfd(&writer).unwrap().contains(FdFlags::CLOEXEC));
        assert!(fcntl_getfl(&owned).unwrap().contains(OFlags::NONBLOCK));
        assert!(owned.as_raw_fd() >= 3);
        drop(owned);
        rustix::io::write(&writer, b"still-owned").unwrap();
        drop(writer);
        let mut bytes = Vec::new();
        std::fs::File::from(reader).read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, b"still-owned");
    }

    #[test]
    fn standard_descriptors_are_rejected_without_taking_ownership() {
        assert_eq!(
            duplicate_nonblocking_cloexec(2).unwrap_err().kind(),
            std::io::ErrorKind::InvalidInput
        );
        assert_eq!(
            duplicate_nonblocking_cloexec(1_000_000)
                .unwrap_err()
                .raw_os_error(),
            Some(libc::EBADF)
        );
    }

    #[test]
    fn clear_cloexec_removes_the_flag_in_place() {
        let (reader, writer) = pipe().unwrap();
        // Duplicated fds start CLOEXEC-set (see duplicate_cloexec).
        let owned = duplicate_cloexec(writer.as_raw_fd()).unwrap();
        assert!(fcntl_getfd(&owned).unwrap().contains(FdFlags::CLOEXEC));
        super::clear_cloexec(owned.as_raw_fd()).unwrap();
        assert!(!fcntl_getfd(&owned).unwrap().contains(FdFlags::CLOEXEC));
        drop(owned);
        drop(reader);
        drop(writer);
    }

    #[test]
    fn clear_cloexec_rejects_standard_descriptors() {
        assert_eq!(
            super::clear_cloexec(2).unwrap_err().kind(),
            std::io::ErrorKind::InvalidInput
        );
    }
}
