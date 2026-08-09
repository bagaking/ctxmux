//! Narrow audited access to `SQLite` connection cache counters.
//!
//! `rusqlite` 0.40 does not expose `sqlite3_db_status64`. Keeping the raw
//! handle access in this private leaf lets the daemon itself retain its
//! workspace-wide `unsafe_code = "forbid"` boundary.

#![deny(unsafe_code)]

use rusqlite::{Connection, Error, ffi};

/// `SQLite` version whose pager-accounting implementation backs the WAL proof.
pub const PROVEN_SQLITE_VERSION: i32 = 3_053_002;
const _: () = assert!(ffi::SQLITE_VERSION_NUMBER == PROVEN_SQLITE_VERSION);

/// Pager-cache observations used by the spill-disabled WAL admission proof.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CacheStatus {
    /// Approximate bytes owned by every pager cache on this connection.
    pub used_bytes: u64,
    /// Pager cache writes since the counter was last reset.
    pub writes: u64,
    /// Dirty-page spills since the counter was last reset.
    pub spills: u64,
}

/// Reset the cache-write and dirty-spill counters before staged admission.
///
/// # Errors
///
/// Returns `SQLite`'s typed failure when this pinned library does not support a
/// requested status verb or cannot observe the connection.
pub fn reset_cache_io(connection: &Connection) -> Result<(), Error> {
    ensure_proven_version()?;
    read_status(connection, ffi::SQLITE_DBSTATUS_CACHE_WRITE, true)?;
    read_status(connection, ffi::SQLITE_DBSTATUS_CACHE_SPILL, true)?;
    Ok(())
}

/// Read the cache footprint and the I/O counters used by staged admission.
///
/// # Errors
///
/// Returns `SQLite`'s typed failure when the runtime `SQLite` version or required
/// connection-status observations differ from the source-bound proof.
pub fn cache_admission_snapshot(connection: &Connection) -> Result<CacheStatus, Error> {
    ensure_proven_version()?;
    Ok(CacheStatus {
        used_bytes: read_status(connection, ffi::SQLITE_DBSTATUS_CACHE_USED, false)?,
        writes: read_status(connection, ffi::SQLITE_DBSTATUS_CACHE_WRITE, false)?,
        spills: read_status(connection, ffi::SQLITE_DBSTATUS_CACHE_SPILL, false)?,
    })
}

fn ensure_proven_version() -> Result<(), Error> {
    let actual = rusqlite::version_number();
    if actual == PROVEN_SQLITE_VERSION {
        return Ok(());
    }
    Err(Error::SqliteFailure(
        ffi::Error::new(ffi::SQLITE_MISMATCH),
        Some(format!(
            "SQLite runtime version {actual} differs from proven version {PROVEN_SQLITE_VERSION}"
        )),
    ))
}

#[allow(unsafe_code)]
fn read_status(connection: &Connection, verb: i32, reset: bool) -> Result<u64, Error> {
    let mut current = 0_i64;
    let mut high_water = 0_i64;
    // SAFETY: `connection` keeps its SQLite handle alive for this call.
    // `sqlite3_db_status64` only reads/resets counters under SQLite's own
    // connection mutex and neither stores the pointer nor takes ownership.
    let result = unsafe {
        ffi::sqlite3_db_status64(
            connection.handle(),
            verb,
            &raw mut current,
            &raw mut high_water,
            i32::from(reset),
        )
    };
    if result != ffi::SQLITE_OK {
        return Err(Error::SqliteFailure(ffi::Error::new(result), None));
    }
    u64::try_from(current).map_err(|_| {
        Error::SqliteFailure(
            ffi::Error::new(ffi::SQLITE_CORRUPT),
            Some(format!(
                "SQLite status verb {verb} returned a negative value"
            )),
        )
    })
}

#[cfg(test)]
mod tests {
    use std::fs;

    use rusqlite::Connection;
    use tempfile::tempdir;

    use super::{PROVEN_SQLITE_VERSION, cache_admission_snapshot, reset_cache_io};

    #[test]
    fn pinned_sqlite_exposes_nonnegative_cache_counters() {
        assert_eq!(rusqlite::version_number(), PROVEN_SQLITE_VERSION);
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE facts(value BLOB); INSERT INTO facts VALUES (zeroblob(4096));",
            )
            .unwrap();
        reset_cache_io(&connection).unwrap();
        let observed = cache_admission_snapshot(&connection).unwrap();

        assert!(observed.used_bytes >= 4096);
    }

    #[test]
    fn cache_bound_covers_final_spill_disabled_wal() {
        const PAGE_BYTES: u64 = 4096;
        const FRAME_BYTES: u64 = PAGE_BYTES + 24;
        const WAL_HEADER_BYTES: u64 = 32;

        let directory = tempdir().unwrap();
        let database = directory.path().join("proof.sqlite3");
        let wal = directory.path().join("proof.sqlite3-wal");
        let connection = Connection::open(&database).unwrap();
        connection
            .execute_batch(
                "PRAGMA page_size=4096;
                 PRAGMA journal_mode=WAL;
                 PRAGMA wal_autocheckpoint=0;
                 PRAGMA cache_spill=OFF;
                 CREATE TABLE facts(id INTEGER PRIMARY KEY, value BLOB);
                 PRAGMA wal_checkpoint(TRUNCATE);",
            )
            .unwrap();
        let page_size: i64 = connection
            .pragma_query_value(None, "page_size", |row| row.get(0))
            .unwrap();
        assert_eq!(page_size, i64::try_from(PAGE_BYTES).unwrap());
        connection.release_memory().unwrap();
        reset_cache_io(&connection).unwrap();
        connection
            .execute_batch(
                "BEGIN IMMEDIATE;
                 WITH RECURSIVE n(value) AS (
                   VALUES(1) UNION ALL SELECT value + 1 FROM n WHERE value < 4000
                 )
                 INSERT INTO facts(id, value) SELECT value, zeroblob(1024) FROM n;",
            )
            .unwrap();

        let snapshot = cache_admission_snapshot(&connection).unwrap();
        assert_eq!(snapshot.writes, 0);
        assert_eq!(snapshot.spills, 0);
        assert_eq!(fs::metadata(&wal).map_or(0, |metadata| metadata.len()), 0);
        let cached_page_upper = snapshot.used_bytes.div_ceil(PAGE_BYTES);
        let bound = WAL_HEADER_BYTES + cached_page_upper * FRAME_BYTES;
        assert!(bound <= 8 * 1024 * 1024);

        connection.execute_batch("COMMIT;").unwrap();
        let actual = fs::metadata(&wal).unwrap().len();
        let frames = actual
            .checked_sub(WAL_HEADER_BYTES)
            .expect("committed WAL contains its 32-byte header");
        assert_eq!(frames % FRAME_BYTES, 0);
        assert!(actual <= bound, "actual WAL {actual} exceeds bound {bound}");
    }
}
