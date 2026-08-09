//! Narrow audited macOS process-statistics access for qualification helpers.
//!
//! Product crates keep `unsafe_code = "forbid"`. This private leaf owns the
//! target-local `proc_pidinfo` calls needed to avoid enumerating every system
//! process for each RSS observation.

#![deny(unsafe_code)]

#[cfg(target_os = "macos")]
mod macos {
    use std::{ffi::c_void, io, mem::MaybeUninit};

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub struct ProcessIdentity {
        started_at_seconds: u64,
        started_at_microseconds: u64,
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub struct ProcessStats {
        pub identity: ProcessIdentity,
        pub resident_bytes: u64,
    }

    /// Reads one process without enumerating unrelated system PIDs.
    ///
    /// The BSD identity is read before and after the task counters so a PID
    /// replacement cannot combine the old owner's identity with new RSS.
    ///
    /// # Errors
    ///
    /// Returns an OS error when the target is absent, inaccessible, replaced
    /// during observation, or does not return the exact expected structures.
    pub fn process_stats(pid: u32) -> io::Result<ProcessStats> {
        let pid = i32::try_from(pid)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "PID exceeds i32"))?;
        let before = read_bsd_info(pid)?;
        let task = read_task_info(pid)?;
        let after = read_bsd_info(pid)?;
        let observed_identity = identity(pid, &before)?;
        if observed_identity != identity(pid, &after)? {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "process incarnation changed during observation",
            ));
        }
        Ok(ProcessStats {
            identity: observed_identity,
            resident_bytes: task.pti_resident_size,
        })
    }

    fn identity(pid: i32, info: &libc::proc_bsdinfo) -> io::Result<ProcessIdentity> {
        if info.pbi_pid != pid.cast_unsigned() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "process identity did not match PID",
            ));
        }
        Ok(ProcessIdentity {
            started_at_seconds: info.pbi_start_tvsec,
            started_at_microseconds: info.pbi_start_tvusec,
        })
    }

    #[allow(unsafe_code)]
    fn read_bsd_info(pid: i32) -> io::Result<libc::proc_bsdinfo> {
        let mut value = MaybeUninit::<libc::proc_bsdinfo>::uninit();
        let expected = std::mem::size_of::<libc::proc_bsdinfo>();
        let actual = unsafe {
            libc::proc_pidinfo(
                pid,
                libc::PROC_PIDTBSDINFO,
                0,
                value.as_mut_ptr().cast::<c_void>(),
                i32::try_from(expected).expect("process info structure fits i32"),
            )
        };
        if actual != i32::try_from(expected).expect("process info structure fits i32") {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: PROC_PIDTBSDINFO returned the exact proc_bsdinfo byte size.
        Ok(unsafe { value.assume_init() })
    }

    #[allow(unsafe_code)]
    fn read_task_info(pid: i32) -> io::Result<libc::proc_taskinfo> {
        let mut value = MaybeUninit::<libc::proc_taskinfo>::uninit();
        let expected = std::mem::size_of::<libc::proc_taskinfo>();
        let actual = unsafe {
            libc::proc_pidinfo(
                pid,
                libc::PROC_PIDTASKINFO,
                0,
                value.as_mut_ptr().cast::<c_void>(),
                i32::try_from(expected).expect("process info structure fits i32"),
            )
        };
        if actual != i32::try_from(expected).expect("process info structure fits i32") {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: PROC_PIDTASKINFO returned the exact proc_taskinfo byte size.
        Ok(unsafe { value.assume_init() })
    }

    #[cfg(test)]
    mod tests {
        use std::{process::Command, time::Instant};

        #[test]
        fn self_stats_match_ps_and_keep_one_identity() {
            let first = super::process_stats(std::process::id()).unwrap();
            let second = super::process_stats(std::process::id()).unwrap();
            assert_eq!(first.identity, second.identity);
            assert!(first.resident_bytes > 0);

            let ps = Command::new("ps")
                .args(["-o", "rss=", "-p", &std::process::id().to_string()])
                .output()
                .unwrap();
            assert!(ps.status.success());
            let ps_bytes = String::from_utf8(ps.stdout)
                .unwrap()
                .trim()
                .parse::<u64>()
                .unwrap()
                * 1024;
            assert!(first.resident_bytes.abs_diff(ps_bytes) <= 4 * 1024 * 1024);
        }

        #[test]
        fn one_pid_reads_meet_the_representative_gap_without_enumeration() {
            let mut maximum = std::time::Duration::ZERO;
            for _ in 0..200 {
                let started = Instant::now();
                super::process_stats(std::process::id()).unwrap();
                maximum = maximum.max(started.elapsed());
            }
            assert!(maximum <= std::time::Duration::from_millis(100));
        }

        #[test]
        fn unavailable_pid_fails_closed() {
            assert!(super::process_stats(i32::MAX.cast_unsigned()).is_err());
        }
    }
}

#[cfg(target_os = "macos")]
pub use macos::{ProcessIdentity, ProcessStats, process_stats};

#[cfg(all(test, not(target_os = "macos")))]
mod tests {
    #[test]
    fn process_statistics_leaf_is_explicitly_macos_only() {
        assert_ne!(std::env::consts::OS, "macos");
    }
}
