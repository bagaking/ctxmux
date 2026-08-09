use std::{
    os::fd::OwnedFd,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::{self, SyncSender, TrySendError},
    },
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use rustix::io::write;
use serde_json::json;

const METRIC_COUNT: usize = 13;
const MAX_FRAME_BYTES: usize = 2048;
const SNAPSHOT_QUEUE_CAPACITY: usize = 1;

#[derive(Clone, Copy, Debug)]
pub(crate) enum Gauge {
    RetainedRuns = 0,
    CreationKeys = 1,
    CreationFlights = 2,
    PublicationReservations = 3,
    CollectingTickets = 4,
    OverlapOwners = 5,
    CleanupOwners = 6,
    DirectChildren = 7,
    Readers = 8,
    Waiters = 9,
    InputDrains = 10,
    Attachments = 11,
    TmuxOwners = 12,
}

#[derive(Clone, Default)]
pub(crate) struct QualificationStats {
    inner: Option<Arc<Inner>>,
}

struct Inner {
    daemon_instance: String,
    sender: SyncSender<()>,
    writer: Mutex<Option<thread::JoinHandle<()>>>,
    finishing: AtomicBool,
    dropped_total: AtomicU64,
    current: [AtomicU64; METRIC_COUNT],
    high_water: [AtomicU64; METRIC_COUNT],
    physical_starts_total: AtomicU64,
    candidate_selections_total: AtomicU64,
    candidate_evaluations_total: AtomicU64,
    candidate_evaluations_max: AtomicU64,
    candidate_fences_total: AtomicU64,
    exact_replacements_total: AtomicU64,
    active_snapshot_writers: AtomicU64,
    snapshot_revision: AtomicU64,
}

#[must_use = "a qualification gauge must follow its real owner until Drop"]
pub(crate) struct GaugeGuard {
    stats: QualificationStats,
    gauge: Gauge,
}

impl QualificationStats {
    pub(crate) fn from_optional_inherited_fd(
        sink: Option<OwnedFd>,
        daemon_instance: impl Into<String>,
    ) -> std::io::Result<Self> {
        sink.map_or_else(
            || Ok(Self::default()),
            |sink| Self::from_sink(sink, daemon_instance),
        )
    }

    /// Take one harness-owned descriptor and start its bounded single writer.
    /// The raw descriptor becomes nonblocking and close-on-exec before any Run
    /// can spawn. Runtime owner transitions only touch atomics and `try_send`.
    pub(crate) fn from_sink(
        sink: OwnedFd,
        daemon_instance: impl Into<String>,
    ) -> std::io::Result<Self> {
        let (sender, receiver) = mpsc::sync_channel(SNAPSHOT_QUEUE_CAPACITY);
        let inner = Arc::new(Inner {
            daemon_instance: daemon_instance.into(),
            sender,
            writer: Mutex::new(None),
            finishing: AtomicBool::new(false),
            dropped_total: AtomicU64::new(0),
            current: std::array::from_fn(|_| AtomicU64::new(0)),
            high_water: std::array::from_fn(|_| AtomicU64::new(0)),
            physical_starts_total: AtomicU64::new(0),
            candidate_selections_total: AtomicU64::new(0),
            candidate_evaluations_total: AtomicU64::new(0),
            candidate_evaluations_max: AtomicU64::new(0),
            candidate_fences_total: AtomicU64::new(0),
            exact_replacements_total: AtomicU64::new(0),
            active_snapshot_writers: AtomicU64::new(0),
            snapshot_revision: AtomicU64::new(0),
        });
        let writer_inner = Arc::downgrade(&inner);
        let writer = thread::Builder::new()
            .name("ctxmux-qualification-stats".to_owned())
            .spawn(move || writer_main(&writer_inner, &receiver, &sink))?;
        *lock(&inner.writer) = Some(writer);
        let stats = Self { inner: Some(inner) };
        stats.request_snapshot();
        Ok(stats)
    }

    pub(crate) fn guard(&self, gauge: Gauge) -> GaugeGuard {
        self.adjust(gauge, 1);
        GaugeGuard {
            stats: self.clone(),
            gauge,
        }
    }

    pub(crate) fn set(&self, gauge: Gauge, value: usize) {
        self.set_many(&[(gauge, value)]);
    }

    /// Mirror exact owner values without taking a telemetry lock. Callers may
    /// invoke this while holding their own owner lock; encoding and I/O happen
    /// only on the single writer thread.
    pub(crate) fn set_many(&self, values: &[(Gauge, usize)]) {
        let Some(inner) = &self.inner else { return };
        begin_snapshot_write(inner);
        for (gauge, value) in values {
            let index = *gauge as usize;
            let value = *value as u64;
            inner.current[index].store(value, Ordering::Release);
            inner.high_water[index].fetch_max(value, Ordering::AcqRel);
        }
        end_snapshot_write(inner);
        self.request_snapshot();
    }

    fn adjust(&self, gauge: Gauge, delta: i64) {
        let Some(inner) = &self.inner else { return };
        begin_snapshot_write(inner);
        let index = gauge as usize;
        let value = if delta >= 0 {
            inner.current[index]
                .fetch_add(delta.unsigned_abs(), Ordering::AcqRel)
                .checked_add(delta.unsigned_abs())
                .expect("qualification gauge does not overflow")
        } else {
            inner.current[index]
                .fetch_sub(delta.unsigned_abs(), Ordering::AcqRel)
                .checked_sub(delta.unsigned_abs())
                .expect("qualification gauge owner drops exactly once")
        };
        inner.high_water[index].fetch_max(value, Ordering::AcqRel);
        end_snapshot_write(inner);
        self.request_snapshot();
    }

    pub(crate) fn record_physical_start(&self) {
        self.increment(|inner| &inner.physical_starts_total, 1);
    }

    pub(crate) fn record_candidate_selection(&self, evaluated: usize) {
        let Some(inner) = &self.inner else { return };
        begin_snapshot_write(inner);
        checked_fetch_add(&inner.candidate_selections_total, 1);
        checked_fetch_add(&inner.candidate_evaluations_total, evaluated as u64);
        inner
            .candidate_evaluations_max
            .fetch_max(evaluated as u64, Ordering::AcqRel);
        end_snapshot_write(inner);
        self.request_snapshot();
    }

    pub(crate) fn record_candidate_fences(&self, count: usize) {
        self.increment(|inner| &inner.candidate_fences_total, count as u64);
    }

    pub(crate) fn record_exact_replacements(&self, count: usize) {
        self.increment(|inner| &inner.exact_replacements_total, count as u64);
    }

    /// Request the final quiescent snapshot and join the single writer. A
    /// queued wake observes the final flag, so a full channel remains bounded.
    pub(crate) fn finish(&self) {
        let Some(inner) = &self.inner else { return };
        if inner.finishing.swap(true, Ordering::AcqRel) {
            return;
        }
        self.request_snapshot();
        if let Some(writer) = lock(&inner.writer).take() {
            let _ = writer.join();
        }
    }

    fn increment(&self, counter: impl FnOnce(&Inner) -> &AtomicU64, delta: u64) {
        let Some(inner) = &self.inner else { return };
        begin_snapshot_write(inner);
        checked_fetch_add(counter(inner), delta);
        end_snapshot_write(inner);
        self.request_snapshot();
    }

    /// Wake the writer after atomics changed. A full one-slot channel already
    /// carries a wake, so coalescing it loses no current, high-water, or
    /// cumulative state. Disconnection is the only dropped observation.
    fn request_snapshot(&self) {
        let Some(inner) = &self.inner else {
            return;
        };
        match inner.sender.try_send(()) {
            Ok(()) | Err(TrySendError::Full(())) => {}
            Err(TrySendError::Disconnected(())) => {
                checked_fetch_add(&inner.dropped_total, 1);
            }
        }
    }
}

impl Drop for GaugeGuard {
    fn drop(&mut self) {
        self.stats.adjust(self.gauge, -1);
    }
}

fn writer_main(inner: &Weak<Inner>, receiver: &mpsc::Receiver<()>, sink: &OwnedFd) {
    let mut sequence = 0_u64;
    loop {
        match receiver.recv_timeout(Duration::from_secs(1)) {
            Ok(()) | Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => return,
        }
        let Some(inner) = inner.upgrade() else {
            return;
        };
        sequence = sequence
            .checked_add(1)
            .expect("qualification sequence does not overflow");
        let final_snapshot = inner.finishing.load(Ordering::Acquire);
        if !write_snapshot(&inner, sink, sequence, final_snapshot) {
            return;
        }
        if final_snapshot {
            return;
        }
    }
}

fn write_snapshot(inner: &Inner, sink: &OwnedFd, sequence: u64, final_snapshot: bool) -> bool {
    // Timestamp before reading the owner atomics. The qualification harness can
    // therefore use a later producer timestamp as a one-way visibility barrier
    // for a completed public operation without adding a runtime control API.
    let timestamp_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("qualification clock is after the Unix epoch")
        .as_millis();
    let (current, high_water, cumulative) = coherent_values(inner);
    let mut frame = serde_json::to_vec(&json!({
        "schema": "ctxmux.qualification-stats.v1",
        "timestamp_unix_ms": timestamp_unix_ms,
        "daemon_instance": inner.daemon_instance,
        "seq": sequence,
        "final": final_snapshot,
        "dropped_total": inner.dropped_total.load(Ordering::Acquire),
        "current": current,
        "high_water": high_water,
        "cumulative": cumulative,
    }))
    .expect("qualification stats are serializable");
    frame.push(b'\n');
    if frame.len() > MAX_FRAME_BYTES {
        return false;
    }
    matches!(write(sink, &frame), Ok(written) if written == frame.len())
}

fn coherent_values(inner: &Inner) -> (Vec<u64>, Vec<u64>, [u64; 6]) {
    loop {
        if inner.active_snapshot_writers.load(Ordering::Acquire) != 0 {
            thread::yield_now();
            continue;
        }
        let revision = inner.snapshot_revision.load(Ordering::Acquire);
        let current = named_values(&inner.current);
        let high_water = named_values(&inner.high_water);
        let cumulative = [
            inner.physical_starts_total.load(Ordering::Acquire),
            inner.candidate_selections_total.load(Ordering::Acquire),
            inner.candidate_evaluations_total.load(Ordering::Acquire),
            inner.candidate_evaluations_max.load(Ordering::Acquire),
            inner.candidate_fences_total.load(Ordering::Acquire),
            inner.exact_replacements_total.load(Ordering::Acquire),
        ];
        if inner.active_snapshot_writers.load(Ordering::Acquire) == 0
            && inner.snapshot_revision.load(Ordering::Acquire) == revision
        {
            return (current, high_water, cumulative);
        }
    }
}

fn named_values(values: &[AtomicU64; METRIC_COUNT]) -> Vec<u64> {
    values
        .iter()
        .map(|value| value.load(Ordering::Acquire))
        .collect()
}

fn checked_fetch_add(counter: &AtomicU64, delta: u64) {
    counter
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
            value.checked_add(delta)
        })
        .expect("qualification counter does not overflow");
}

fn begin_snapshot_write(inner: &Inner) {
    checked_fetch_add(&inner.active_snapshot_writers, 1);
}

fn end_snapshot_write(inner: &Inner) {
    checked_fetch_add(&inner.snapshot_revision, 1);
    inner
        .active_snapshot_writers
        .fetch_sub(1, Ordering::Release);
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use std::{
        io::{BufRead, BufReader},
        os::unix::net::UnixStream,
        sync::{
            Mutex,
            atomic::{AtomicBool, AtomicU64, Ordering},
            mpsc,
        },
    };

    use serde_json::json;

    use super::{Gauge, Inner, MAX_FRAME_BYTES, QualificationStats, named_values};

    #[test]
    fn transition_frames_preserve_pulses_and_restart_resets_epoch() {
        let (reader, writer) = UnixStream::pair().expect("create stats stream");
        let stats = QualificationStats::from_sink(writer.into(), "epoch-one")
            .expect("open qualification stats");
        {
            let _child = stats.guard(Gauge::DirectChildren);
            stats.record_physical_start();
            stats.record_candidate_selection(7);
            stats.record_candidate_fences(1);
            stats.record_exact_replacements(1);
        }
        stats.finish();

        let frames = BufReader::new(reader)
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(&line.unwrap()).unwrap())
            .collect::<Vec<_>>();
        let last = frames.last().expect("last transition frame");
        assert_eq!(last["daemon_instance"], "epoch-one");
        assert_eq!(last["final"], true);
        assert_eq!(last["current"][Gauge::DirectChildren as usize], 0);
        assert_eq!(last["high_water"][Gauge::DirectChildren as usize], 1);
        assert_eq!(last["cumulative"][0], 1);
        assert_eq!(last["cumulative"][1], 1);
        assert_eq!(last["cumulative"][2], 7);
        assert_eq!(last["cumulative"][3], 7);
        assert_eq!(last["cumulative"][4], 1);
        assert_eq!(last["cumulative"][5], 1);
        assert_eq!(last["dropped_total"], 0);
        assert!(
            frames
                .windows(2)
                .all(|pair| pair[1]["seq"].as_u64() > pair[0]["seq"].as_u64())
        );

        let (reader, writer) = UnixStream::pair().expect("create restart stream");
        let restarted = QualificationStats::from_sink(writer.into(), "epoch-two")
            .expect("open restarted qualification stats");
        restarted.finish();
        let restart_frame: serde_json::Value = serde_json::from_str(
            &BufReader::new(reader)
                .lines()
                .next()
                .expect("restart frame")
                .unwrap(),
        )
        .unwrap();
        assert_eq!(restart_frame["daemon_instance"], "epoch-two");
        assert_eq!(restart_frame["seq"], 1);
        assert_eq!(restart_frame["cumulative"][0], 0);
    }

    #[test]
    fn disconnected_sink_never_blocks_runtime_owner_updates() {
        let (reader, writer) = UnixStream::pair().expect("create stats stream");
        let stats = QualificationStats::from_sink(writer.into(), "epoch")
            .expect("open qualification stats");
        drop(reader);
        for _ in 0..2048 {
            let _guard = stats.guard(Gauge::Attachments);
        }
        stats.finish();
    }

    #[test]
    fn backpressured_pipe_fails_without_blocking_owner_updates_or_faking_final() {
        use std::io::Read;

        use rustix::{
            fs::{OFlags, fcntl_getfl, fcntl_setfl},
            io::{Errno, write},
            pipe::{PIPE_BUF, pipe},
        };

        let (reader, writer) = pipe().expect("create stats pipe");
        let flags = fcntl_getfl(&writer).expect("read pipe flags");
        fcntl_setfl(&writer, flags | OFlags::NONBLOCK).expect("set pipe nonblocking");
        let filler = vec![b'x'; PIPE_BUF];
        loop {
            match write(&writer, &filler) {
                Ok(_) => {}
                Err(Errno::AGAIN) => break,
                Err(error) => panic!("fill stats pipe: {error}"),
            }
        }
        let stats =
            QualificationStats::from_sink(writer, "epoch").expect("open qualification stats");
        for value in 0..100_000 {
            stats.set(Gauge::RetainedRuns, value % 129);
        }
        stats.finish();

        let mut bytes = Vec::new();
        std::fs::File::from(reader)
            .read_to_end(&mut bytes)
            .expect("drain filled stats pipe");
        assert!(!bytes.is_empty());
        assert!(
            !bytes
                .windows(b"\"final\":true".len())
                .any(|window| { window == b"\"final\":true" })
        );
    }

    #[test]
    fn complete_u64_counter_domain_fits_the_private_frame_bound() {
        let inner = Inner {
            daemon_instance: "00000000-0000-0000-0000-000000000000".to_owned(),
            sender: mpsc::sync_channel(1).0,
            writer: Mutex::new(None),
            finishing: AtomicBool::new(false),
            dropped_total: AtomicU64::new(u64::MAX),
            current: std::array::from_fn(|_| AtomicU64::new(u64::MAX)),
            high_water: std::array::from_fn(|_| AtomicU64::new(u64::MAX)),
            physical_starts_total: AtomicU64::new(u64::MAX),
            candidate_selections_total: AtomicU64::new(u64::MAX),
            candidate_evaluations_total: AtomicU64::new(u64::MAX),
            candidate_evaluations_max: AtomicU64::new(u64::MAX),
            candidate_fences_total: AtomicU64::new(u64::MAX),
            exact_replacements_total: AtomicU64::new(u64::MAX),
            active_snapshot_writers: AtomicU64::new(0),
            snapshot_revision: AtomicU64::new(0),
        };
        let mut frame = serde_json::to_vec(&json!({
            "schema": "ctxmux.qualification-stats.v1",
            "timestamp_unix_ms": u64::MAX,
            "daemon_instance": inner.daemon_instance,
            "seq": u64::MAX,
            "final": true,
            "dropped_total": inner.dropped_total.load(Ordering::Acquire),
            "current": named_values(&inner.current),
            "high_water": named_values(&inner.high_water),
            "cumulative": vec![u64::MAX; 6],
        }))
        .unwrap();
        frame.push(b'\n');
        assert!(frame.len() <= MAX_FRAME_BYTES);
    }
}
