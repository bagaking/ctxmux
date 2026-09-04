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

    const PAGE_BYTES: u64 = 4096;
    const FRAME_BYTES: u64 = PAGE_BYTES + 24;
    const WAL_HEADER_BYTES: u64 = 32;
    const CHECKPOINT_CEILING: u64 = 8 * 1024 * 1024;
    const TOTAL_CEILING: u64 = 16 * 1024 * 1024;

    /// One staged spill-disabled transaction on a WAL that already carries
    /// `baseline_target` bytes of frames.
    ///
    /// With `overlap`, the baseline also holds pages the measured transaction
    /// dirties again; without it, the baseline is entirely disjoint and a
    /// `baseline_target` of 0 reaches the commit on a genuinely empty WAL.
    ///
    /// Returns `(baseline, bound, actual)`: the WAL length admission would
    /// have recorded, the charge derived from the pre-COMMIT cache footprint,
    /// and the WAL length after COMMIT.
    fn staged_transaction_on_baseline(baseline_target: u64, overlap: bool) -> (u64, u64, u64) {
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
                 CREATE TABLE ballast(id INTEGER PRIMARY KEY, value BLOB);
                 PRAGMA wal_checkpoint(TRUNCATE);",
            )
            .unwrap();
        let page_size: i64 = connection
            .pragma_query_value(None, "page_size", |row| row.get(0))
            .unwrap();
        assert_eq!(page_size, i64::try_from(PAGE_BYTES).unwrap());

        // Grow the WAL with writes that are *not* the measured transaction, so
        // the measured one genuinely appends after existing frames. A missing
        // WAL is a broken fixture, not an empty one: reporting it as zero would
        // let every later assertion pass against a file that is not there.
        let wal_len = || {
            fs::metadata(&wal)
                .expect("the WAL exists once journal_mode=WAL is set")
                .len()
        };

        // Seed `facts` itself so the baseline holds pages the measured
        // transaction dirties again -- its root and interior pages. A disjoint
        // baseline only proves that residency of *unrelated* pages is free;
        // production's lifecycle transaction overlaps pages the chatty output
        // path already wrote, so the proof has to cover that too.
        //
        // The row count is fixed rather than scaled to the target: more rows in
        // `facts` means a deeper B-tree, and the measured insert would touch
        // extra interior pages for reasons that have nothing to do with WAL
        // residency. Measured against the pinned amalgamation, seeding this way
        // holds at 1342 frames from a 440 KiB baseline to an 8.4 MiB one, while
        // growing the baseline with rows in `facts` instead moves it 1339->1343
        // -- that variant would fail this test for B-tree shape, not residency.
        if overlap {
            for id in 0..64i64 {
                connection
                    .execute(
                        "INSERT INTO facts(id, value) VALUES (?1, randomblob(1024));",
                        [1_000_000 + id],
                    )
                    .unwrap();
            }
            assert!(
                wal_len() > 0,
                "the seed must leave `facts` pages resident in the WAL"
            );
        }

        while wal_len() < baseline_target {
            connection
                .execute_batch("INSERT INTO ballast(value) VALUES (zeroblob(65536));")
                .unwrap();
        }
        let baseline = wal_len();

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
        assert_eq!(
            wal_len(),
            baseline,
            "a spill-disabled transaction writes nothing before COMMIT"
        );
        let cached_page_upper = snapshot.used_bytes.div_ceil(PAGE_BYTES);
        let bound = WAL_HEADER_BYTES + cached_page_upper * FRAME_BYTES;

        connection.execute_batch("COMMIT;").unwrap();
        (baseline, bound, wal_len())
    }

    /// The charge bounds the transaction's WAL *growth*, not the WAL's absolute
    /// length.
    ///
    /// The lifecycle verbs no longer checkpoint to zero before staging — that
    /// checkpoint was 85-99% of why `start` and `remove` lose to tmux under a
    /// chatty fleet — so the daemon proves `actual - baseline <= charge` off
    /// whatever baseline admission left behind. Baseline 0 is the control: it
    /// reproduces the absolute form this test used to assert, so a failure
    /// there means the harness broke rather than the hypothesis.
    ///
    /// Both sweeps run: a disjoint baseline (only unrelated frames) and an
    /// overlapping one (the baseline also holds `facts` pages the measured
    /// transaction dirties again, which is the shape production presents). The
    /// invariant is checked *within* each sweep — the two modes write slightly
    /// different frame counts because seeding changes the B-tree, and comparing
    /// across them would test shape rather than residency.
    ///
    /// 8 MiB is the largest baseline the daemon can present, because
    /// `fold_wal_below_ceiling` checkpoints anything above it.
    #[test]
    fn cache_bound_covers_spill_disabled_wal_growth_from_any_baseline() {
        for overlap in [false, true] {
            let mut previous_delta = None;
            for baseline_target in [0, 64 * 1024, 2 * 1024 * 1024, CHECKPOINT_CEILING] {
                let (baseline, bound, actual) =
                    staged_transaction_on_baseline(baseline_target, overlap);
                assert!(
                    baseline >= baseline_target,
                    "baseline {baseline} never reached target {baseline_target}"
                );
                assert!(bound <= CHECKPOINT_CEILING, "charge {bound} exceeds 8 MiB");
                assert!(
                    baseline.saturating_add(bound) <= TOTAL_CEILING,
                    "baseline {baseline} plus charge {bound} exceeds the 16 MiB total"
                );

                let delta = actual
                    .checked_sub(baseline)
                    .expect("the WAL grows monotonically across a commit");
                // An empty WAL has no header yet, so the first commit onto one
                // pays for it; every later commit appends frames only.
                let frames_written = if baseline == 0 {
                    delta
                        .checked_sub(WAL_HEADER_BYTES)
                        .expect("a commit onto an empty WAL writes its 32-byte header")
                } else {
                    delta
                };
                assert_eq!(
                    frames_written % FRAME_BYTES,
                    0,
                    "growth is a whole number of frames"
                );
                assert!(
                    delta <= bound,
                    "WAL grew {delta} from baseline {baseline}, exceeding bound {bound}"
                );
                // The frames written must also not depend on the baseline, or
                // the charge would have to grow with WAL residency to stay
                // sound.
                if let Some(previous) = previous_delta {
                    assert_eq!(
                        frames_written, previous,
                        "the same transaction wrote {frames_written} frame bytes off baseline \
                         {baseline} but {previous} off a smaller one (overlap={overlap})"
                    );
                }
                previous_delta = Some(frames_written);
            }
        }
    }
}
