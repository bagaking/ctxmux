//! Narrow audited ownership transfer for a harness-inherited descriptor.
//!
//! The daemon crate keeps `unsafe_code = "forbid"`. This private leaf owns the
//! one raw-descriptor conversion that the process-spawn contract requires.

#![deny(unsafe_code)]

use std::os::fd::{FromRawFd, OwnedFd, RawFd};

use rustix::{
    fs::{OFlags, fcntl_getfl, fcntl_setfl},
    io::{FdFlags, fcntl_getfd, fcntl_setfd},
};

/// Take one inherited descriptor and fence it from every later `exec`.
///
/// # Errors
///
/// Rejects standard descriptors and any descriptor whose flags cannot be
/// observed or changed before the daemon can spawn a Run.
#[allow(unsafe_code)]
pub fn take_nonblocking_cloexec(raw_fd: RawFd) -> std::io::Result<OwnedFd> {
    if raw_fd < 3 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "inherited descriptor must be at least 3",
        ));
    }
    // SAFETY: the private daemon CLI transfers this inherited descriptor
    // exactly once. Successful construction makes `OwnedFd` its only owner.
    let owned = unsafe { OwnedFd::from_raw_fd(raw_fd) };
    let fd_flags = fcntl_getfd(&owned)?;
    fcntl_setfd(&owned, fd_flags | FdFlags::CLOEXEC)?;
    let status_flags = fcntl_getfl(&owned)?;
    fcntl_setfl(&owned, status_flags | OFlags::NONBLOCK)?;
    Ok(owned)
}

#[cfg(test)]
mod tests {
    use std::os::fd::{AsRawFd, IntoRawFd};

    use rustix::{
        fs::{OFlags, fcntl_getfl},
        io::{FdFlags, fcntl_getfd},
        pipe::pipe,
    };

    use super::take_nonblocking_cloexec;

    #[test]
    fn inherited_descriptor_becomes_nonblocking_and_cloexec() {
        let (_reader, writer) = pipe().unwrap();
        let owned = take_nonblocking_cloexec(writer.into_raw_fd()).unwrap();
        assert!(fcntl_getfd(&owned).unwrap().contains(FdFlags::CLOEXEC));
        assert!(fcntl_getfl(&owned).unwrap().contains(OFlags::NONBLOCK));
        assert!(owned.as_raw_fd() >= 3);
    }

    #[test]
    fn standard_descriptors_are_rejected_without_taking_ownership() {
        assert_eq!(
            take_nonblocking_cloexec(2).unwrap_err().kind(),
            std::io::ErrorKind::InvalidInput
        );
    }
}
