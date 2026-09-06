//! Read-only Darwin foreground facts. No host census, signals, argv or reap.

use std::{ffi::c_void, fmt::Write as _, io, mem::MaybeUninit, time::Instant};

// Apple XNU bsd/sys/proc_info_private.h, PROC_PIDUNIQIDENTIFIERINFO.
// Exact-size support is checked at every read; an unavailable selector is not
// replaced with birth time or executable names.
const UNIQUE_IDENTIFIER_INFO: i32 = 17;

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct UniqueIdentifierInfo {
    uuid: [u8; 16],
    unique_id: u64,
    parent_unique_id: u64,
    id_version: i32,
    parent_id_version: i32,
    reserved: [u64; 2],
}
const _: () = assert!(std::mem::size_of::<UniqueIdentifierInfo>() == 56);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ForegroundProcess {
    pub pid: u32,
    pub incarnation: String,
    pub execution_generation: String,
    pub pgid: u32,
    pub sid: u32,
    pub executable_path: String,
    pub executable_image: String,
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::other(message)
}

fn positive(value: i32) -> io::Result<u32> {
    u32::try_from(value)
        .ok()
        .filter(|v| *v > 0)
        .ok_or_else(|| invalid("physical process scope is unavailable"))
}

#[allow(unsafe_code)]
fn unique_identity(pid: i32) -> io::Result<UniqueIdentifierInfo> {
    let mut value = MaybeUninit::<UniqueIdentifierInfo>::uninit();
    let size =
        i32::try_from(std::mem::size_of::<UniqueIdentifierInfo>()).expect("audited size fits i32");
    // SAFETY: selector 17 writes exactly the audited 56-byte structure. No
    // value is assumed initialized unless the kernel returns that exact size.
    let read = unsafe {
        libc::proc_pidinfo(
            pid,
            UNIQUE_IDENTIFIER_INFO,
            0,
            value.as_mut_ptr().cast::<c_void>(),
            size,
        )
    };
    if read != size {
        let error = io::Error::last_os_error();
        return Err(
            if matches!(error.raw_os_error(), Some(libc::EINVAL | libc::ENOTSUP)) {
                io::Error::new(
                    io::ErrorKind::Unsupported,
                    "execution generation selector is unsupported",
                )
            } else {
                error
            },
        );
    }
    Ok(unsafe { value.assume_init() })
}

/// Read only the physical incarnation, allowing an exec of that same process.
///
/// # Errors
/// Returns an error if the PID is unavailable or its incarnation changes.
pub fn process_incarnation(pid: u32) -> io::Result<String> {
    let raw = i32::try_from(pid).map_err(|_| invalid("PID exceeds platform range"))?;
    let first = unique_identity(raw)?.unique_id;
    if first == 0 || unique_identity(raw)?.unique_id != first {
        return Err(invalid("root process incarnation changed"));
    }
    Ok(format!("{first:016x}"))
}

/// Observe one PID only; kernel incarnation and exec identity fence all reads.
///
/// # Errors
/// Returns an error on unavailable, replaced or unsupported physical facts.
#[allow(unsafe_code)]
pub fn execution_identity(pid: u32) -> io::Result<ForegroundProcess> {
    let raw = i32::try_from(pid).map_err(|_| invalid("PID exceeds platform range"))?;
    let before = unique_identity(raw)?;
    let mut bsd = MaybeUninit::<libc::proc_bsdinfo>::uninit();
    let size = i32::try_from(std::mem::size_of::<libc::proc_bsdinfo>())
        .map_err(|_| invalid("BSD structure exceeds platform range"))?;
    // SAFETY: exact libproc BSD structure buffer; checked before initialization.
    if unsafe {
        libc::proc_pidinfo(
            raw,
            libc::PROC_PIDTBSDINFO,
            0,
            bsd.as_mut_ptr().cast::<c_void>(),
            size,
        )
    } != size
    {
        return Err(io::Error::last_os_error());
    }
    let bsd = unsafe { bsd.assume_init() };
    let sid = positive(unsafe { libc::getsid(raw) })?;
    let mut path = vec![0_u8; libc::PROC_PIDPATHINFO_MAXSIZE as usize];
    // SAFETY: path points at a live correctly sized output buffer.
    let read = unsafe {
        libc::proc_pidpath(
            raw,
            path.as_mut_ptr().cast::<c_void>(),
            u32::try_from(path.len()).map_err(|_| invalid("path buffer exceeds platform range"))?,
        )
    };
    if read <= 0 {
        return Err(io::Error::last_os_error());
    }
    let after = unique_identity(raw)?;
    if before.unique_id != after.unique_id
        || before.id_version != after.id_version
        || before.uuid != after.uuid
        || bsd.pbi_pid != pid
    {
        return Err(invalid(
            "process incarnation or exec changed during observation",
        ));
    }
    let end = path
        .iter()
        .position(|byte| *byte == 0)
        .ok_or_else(|| invalid("executable path is incomplete"))?;
    let executable_path = std::str::from_utf8(&path[..end])
        .map_err(|_| invalid("executable path is not UTF-8"))?
        .to_owned();
    if executable_path.is_empty() || before.unique_id == 0 || before.uuid == [0; 16] {
        return Err(invalid("actual executable entity is unavailable"));
    }
    let mut executable_image = String::with_capacity(32);
    for byte in before.uuid {
        write!(&mut executable_image, "{byte:02x}")
            .map_err(|_| invalid("executable image encoding failed"))?;
    }
    Ok(ForegroundProcess {
        pid,
        incarnation: format!("{:016x}", before.unique_id),
        execution_generation: format!("{:08x}", before.id_version.cast_unsigned()),
        pgid: positive(
            i32::try_from(bsd.pbi_pgid).map_err(|_| invalid("PGID exceeds platform range"))?,
        )?,
        sid,
        executable_path,
        executable_image,
    })
}

const PER_MEMBER_BYTES: usize =
    std::mem::size_of::<ForegroundProcess>() + 256 + 3 * std::mem::size_of::<u32>() + 512;
const TEMPORARY_BYTES: usize = 2 * libc::PROC_PIDPATHINFO_MAXSIZE as usize
    + std::mem::size_of::<libc::proc_bsdinfo>()
    + 2 * std::mem::size_of::<UniqueIdentifierInfo>()
    + 5 * std::mem::size_of::<Vec<u32>>()
    + 512;

fn observation_bytes(members: usize, path_bytes: usize) -> io::Result<usize> {
    // Logical owned capacities: DTOs/opaque strings, all three concurrent ID
    // buffers, plus conservative JSON metadata and actual escaped paths.
    members
        .checked_mul(PER_MEMBER_BYTES)
        .and_then(|n| {
            path_bytes
                .checked_mul(7)
                .and_then(|paths| n.checked_add(paths))
        })
        .and_then(|n| n.checked_add(TEMPORARY_BYTES))
        .ok_or_else(|| invalid("foreground observation byte size overflow"))
}

/// Minimum owned buffers for observing one physical member, before any OS read.
/// Actual members and retained/escaped paths still require their full budget.
#[must_use]
pub const fn foreground_minimum_bytes() -> usize {
    PER_MEMBER_BYTES + TEMPORARY_BYTES
}

#[allow(unsafe_code)]
fn group_ids(
    pgid: u32,
    memory_bytes: usize,
    expected_members: Option<usize>,
) -> io::Result<Vec<u32>> {
    let pgid = i32::try_from(pgid).map_err(|_| invalid("PGID exceeds platform range"))?;
    // Darwin's NULL libproc query reports total host process capacity, not
    // this group's size. Allocate only from this job's already admitted bytes.
    let base = observation_bytes(0, 0)?;
    let per_member = observation_bytes(1, 0)? - base;
    let maximum = memory_bytes.checked_sub(base).map_or(0, |n| n / per_member);
    if maximum == 0 || expected_members.is_some_and(|n| n > maximum) {
        return Err(io::Error::new(
            io::ErrorKind::OutOfMemory,
            "foreground observation memory pressure",
        ));
    }
    // The second read needs only the observed member count and one spare.
    // Saturation/growth is unknown, never a truncated population or a retry.
    let count = expected_members.unwrap_or(maximum);
    let capacity = count
        .checked_add(1)
        .ok_or_else(|| invalid("group capacity overflow"))?;
    let bytes = capacity
        .checked_mul(std::mem::size_of::<libc::pid_t>())
        .ok_or_else(|| invalid("group byte size overflow"))?;
    if bytes > memory_bytes {
        return Err(io::Error::new(
            io::ErrorKind::OutOfMemory,
            "foreground observation memory pressure",
        ));
    }
    let mut pids = vec![0_i32; capacity];
    // SAFETY: live buffer with its exact checked byte size, scoped to one PGID.
    let actual = positive(unsafe {
        libc::proc_listpgrppids(
            pgid,
            pids.as_mut_ptr().cast::<c_void>(),
            i32::try_from(bytes).map_err(|_| invalid("group buffer exceeds platform range"))?,
        )
    })? as usize;
    if actual >= capacity {
        return Err(invalid("foreground membership changed or truncated"));
    }
    if expected_members.is_some_and(|expected| expected != actual) {
        return Err(invalid("foreground membership changed"));
    }
    pids.truncate(actual);
    let mut ids = Vec::with_capacity(actual);
    for pid in pids {
        ids.push(positive(pid)?);
    }
    ids.sort_unstable();
    if ids.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(invalid("duplicate foreground member"));
    }
    Ok(ids)
}

/// Read a complete, stable member set of one owned foreground group.
///
/// The caller provides a real admitted memory/deadline budget, not a fixture
/// population cap. The original PTY/root scope is revalidated by its owner.
///
/// # Errors
/// Unavailable, changed, incomplete or over-budget facts fail the observation.
pub fn foreground_group(
    pgid: u32,
    sid: u32,
    memory_bytes: usize,
    deadline: Instant,
) -> io::Result<Vec<ForegroundProcess>> {
    let ids = group_ids(pgid, memory_bytes, None)?;
    let mut retained_path_bytes = 0_usize;
    let mut processes = Vec::with_capacity(ids.len());
    for pid in &ids {
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "foreground observation deadline elapsed",
            ));
        }
        let process = execution_identity(*pid)?;
        if process.pgid != pgid || process.sid != sid {
            return Err(invalid("foreground member left owned scope"));
        }
        retained_path_bytes = retained_path_bytes
            .checked_add(process.executable_path.capacity())
            .ok_or_else(|| invalid("foreground path byte size overflow"))?;
        if observation_bytes(ids.len(), retained_path_bytes)? > memory_bytes {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "foreground observation memory pressure",
            ));
        }
        processes.push(process);
    }
    if ids != group_ids(pgid, memory_bytes, Some(ids.len()))? {
        return Err(invalid("foreground membership changed"));
    }
    for previous in &processes {
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "foreground observation deadline elapsed",
            ));
        }
        if execution_identity(previous.pid)? != *previous {
            return Err(invalid("foreground execution changed"));
        }
    }
    Ok(processes)
}

#[cfg(test)]
mod tests {
    #[test]
    fn own_execution_has_real_opaque_identity() {
        let first = super::execution_identity(std::process::id()).unwrap();
        let second = super::execution_identity(std::process::id()).unwrap();
        assert_eq!(first, second);
        assert!(!first.incarnation.is_empty());
        assert!(!first.execution_generation.is_empty());
        assert!(!first.executable_path.is_empty());
        assert_ne!(first.executable_image, "00000000000000000000000000000000");
    }

    #[test]
    fn unavailable_process_is_not_inferred_from_name_or_birth() {
        assert!(super::execution_identity(i32::MAX.cast_unsigned()).is_err());
    }
}
