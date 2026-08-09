//! Narrow audited macOS process access for ctxmux runtime and qualification.
//!
//! Product crates keep `unsafe_code = "forbid"`. This private leaf owns the
//! target-local libproc calls needed for one-PID RSS observations and bounded
//! process-ID enumeration without linking a full system-inspection runtime.

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

    /// Returns one snapshot of every positive process ID visible to the caller.
    ///
    /// # Errors
    ///
    /// Returns an OS error when libproc cannot report or fill a complete
    /// snapshot. A process-list growth race fails closed instead of returning a
    /// potentially truncated owner set.
    #[allow(unsafe_code)]
    pub fn process_ids() -> io::Result<Vec<u32>> {
        collect_process_ids(reported_process_capacity()?, |pids| {
            let buffer_bytes = pids
                .len()
                .checked_mul(std::mem::size_of::<libc::pid_t>())
                .and_then(|bytes| i32::try_from(bytes).ok())
                .ok_or_else(|| io::Error::other("process list byte size overflow"))?;
            let actual =
                unsafe { libc::proc_listallpids(pids.as_mut_ptr().cast::<c_void>(), buffer_bytes) };
            if actual < 1 {
                return Err(io::Error::last_os_error());
            }
            usize::try_from(actual).map_err(|_| io::Error::other("process list count was negative"))
        })
    }

    #[allow(unsafe_code)]
    fn reported_process_capacity() -> io::Result<usize> {
        let reported = unsafe { libc::proc_listallpids(std::ptr::null_mut(), 0) };
        if reported < 1 {
            return Err(io::Error::last_os_error());
        }
        usize::try_from(reported)
            .ok()
            .and_then(|count| count.checked_mul(2))
            .ok_or_else(|| io::Error::other("process list capacity overflow"))
    }

    fn collect_process_ids(
        mut capacity: usize,
        mut fill: impl FnMut(&mut [libc::pid_t]) -> io::Result<usize>,
    ) -> io::Result<Vec<u32>> {
        const MAX_ATTEMPTS: usize = 3;
        for _ in 0..MAX_ATTEMPTS {
            let mut pids = vec![0; capacity];
            let actual = fill(&mut pids)?;
            if actual < capacity {
                pids.truncate(actual);
                return Ok(pids
                    .into_iter()
                    .filter_map(|pid| u32::try_from(pid).ok().filter(|pid| *pid > 0))
                    .collect());
            }
            capacity = capacity
                .checked_mul(2)
                .ok_or_else(|| io::Error::other("process list capacity overflow"))?;
        }
        Err(io::Error::other(
            "process list remained saturated across bounded snapshot retries",
        ))
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
        fn process_ids_include_self_without_duplicates() {
            let pids = super::process_ids().unwrap();
            assert!(pids.contains(&std::process::id()));
            assert!(pids.iter().all(|pid| *pid > 0));
            let mut unique = pids.clone();
            unique.sort_unstable();
            unique.dedup();
            assert_eq!(pids.len(), unique.len());
        }

        #[test]
        fn saturated_process_snapshots_retry_then_require_spare_capacity() {
            let mut capacities = Vec::new();
            let pids = super::collect_process_ids(2, |buffer| {
                capacities.push(buffer.len());
                if buffer.len() < 8 {
                    Ok(buffer.len())
                } else {
                    buffer[..2].copy_from_slice(&[17, 23]);
                    Ok(2)
                }
            })
            .unwrap();
            assert_eq!(capacities, [2, 4, 8]);
            assert_eq!(pids, [17, 23]);

            let mut attempts = 0;
            let error = super::collect_process_ids(2, |buffer| {
                attempts += 1;
                Ok(buffer.len())
            })
            .unwrap_err();
            assert_eq!(attempts, 3);
            assert!(error.to_string().contains("remained saturated"));
        }

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
pub use macos::{ProcessIdentity, ProcessStats, process_ids, process_stats};

#[cfg(all(test, not(target_os = "macos")))]
mod tests {
    #[test]
    fn process_statistics_leaf_is_explicitly_macos_only() {
        assert_ne!(std::env::consts::OS, "macos");
    }
}
