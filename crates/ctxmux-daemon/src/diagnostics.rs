//! One daemon-owned diagnostic sink, outside Native and async service owners.
//!
//! A blocked inherited stderr may stall only this writer. Admission never waits
//! for the sink or a full record queue. The queue has no independent population
//! limit: every formatting, queued and active record owns a byte reservation.
//! Allocator overhead, the channel implementation and one thread/descriptor are
//! separate finite costs; this accounting is not an RSS qualification oracle.

use std::{
    cell::Cell,
    fmt,
    fs::File,
    io::{self, Write},
    mem::size_of,
    panic::{self, UnwindSafe},
    sync::{
        Arc, Once, OnceLock,
        atomic::{AtomicBool, AtomicI32, AtomicU8, AtomicU64, AtomicUsize, Ordering},
        mpsc,
    },
    thread,
};

use ctxmux_protocol::{DiagnosticsSinkState, DiagnosticsSnapshot};

/// Operator-owned limits; this module invents no workload-derived defaults.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DiagnosticLimits {
    pub(crate) queue_bytes: usize,
    pub(crate) record_bytes: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DiagnosticAdmission {
    Accepted,
    NotInitialized,
    BudgetPressure,
    Oversized,
    FormatFailed,
    SinkUnavailable,
    Shutdown,
}

#[derive(Default)]
struct Counters {
    admitted: AtomicU64,
    written: AtomicU64,
    written_bytes: AtomicU64,
    notice_bytes: AtomicU64,
    before_encoding: AtomicU64,
    encoded_drops: AtomicU64,
    discarded_bytes: AtomicU64,
    oversized: AtomicU64,
    format_failed: AtomicU64,
    write_failed: AtomicU64,
    partial_records: AtomicU64,
    saturated: AtomicBool,
}

impl Counters {
    fn add(&self, counter: &AtomicU64, amount: u64) {
        add_saturating(counter, amount, &self.saturated);
    }
}

fn add_saturating(counter: &AtomicU64, amount: u64, saturated: &AtomicBool) {
    // The closure always returns Some: this is a saturating fetch-add.
    if let Ok(previous) = counter.fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
        Some(value.saturating_add(amount))
    }) && previous.checked_add(amount).is_none()
    {
        saturated.store(true, Ordering::Release);
    }
}

struct State {
    limits: DiagnosticLimits,
    funded: AtomicUsize,
    formatting: AtomicUsize,
    queued: AtomicUsize,
    active_bytes: AtomicUsize,
    writer_alive: AtomicBool,
    shutdown: AtomicBool,
    sink: AtomicU8,
    last_errno: AtomicI32,
    counters: Counters,
}

// State discriminants are private representations, not timers or capacity knobs.
const STARTING: u8 = 0;
const IDLE: u8 = 1;
const WRITING: u8 = 2;
const FAILED: u8 = 3;
const STOPPED: u8 = 4;

impl State {
    fn reserve(self: &Arc<Self>, bytes: usize) -> Option<Reservation> {
        self.funded
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes)
                    .filter(|next| *next <= self.limits.queue_bytes)
            })
            .ok()?;
        Some(Reservation {
            state: Arc::clone(self),
            bytes,
        })
    }

    fn snapshot(&self) -> DiagnosticsSnapshot {
        let counters = &self.counters;
        let errno = self.last_errno.load(Ordering::Acquire);
        DiagnosticsSnapshot {
            queue_budget_bytes: self.limits.queue_bytes,
            record_limit_bytes: self.limits.record_bytes,
            funded_bytes: self.funded.load(Ordering::Acquire),
            formatting_records: self.formatting.load(Ordering::Acquire),
            queued_records: self.queued.load(Ordering::Acquire),
            active_record_bytes: self.active_bytes.load(Ordering::Acquire),
            admitted_records: counters.admitted.load(Ordering::Acquire),
            written_records: counters.written.load(Ordering::Acquire),
            written_bytes: counters.written_bytes.load(Ordering::Acquire),
            notice_written_bytes: counters.notice_bytes.load(Ordering::Acquire),
            dropped_before_encoding_records: counters
                .before_encoding
                .load(Ordering::Acquire)
                .saturating_add(UNINITIALIZED_RECORDS.load(Ordering::Acquire)),
            dropped_encoded_records: counters.encoded_drops.load(Ordering::Acquire),
            discarded_encoded_bytes: counters.discarded_bytes.load(Ordering::Acquire),
            oversized_records: counters.oversized.load(Ordering::Acquire),
            format_failed_records: counters.format_failed.load(Ordering::Acquire),
            sink_write_failures: counters.write_failed.load(Ordering::Acquire),
            initialization_failures: 0,
            partially_written_records: counters.partial_records.load(Ordering::Acquire),
            scoped_panics: SCOPED_PANICS.load(Ordering::Acquire),
            sink: match self.sink.load(Ordering::Acquire) {
                STARTING => DiagnosticsSinkState::Starting,
                IDLE => DiagnosticsSinkState::Idle,
                WRITING => DiagnosticsSinkState::Writing,
                FAILED => DiagnosticsSinkState::Failed,
                _ => DiagnosticsSinkState::Stopped,
            },
            last_sink_errno: (errno != 0).then_some(errno),
            writer_alive: self.writer_alive.load(Ordering::Acquire),
            shutdown_requested: self.shutdown.load(Ordering::Acquire),
            counters_saturated: counters.saturated.load(Ordering::Acquire)
                || GLOBAL_COUNTERS_SATURATED.load(Ordering::Acquire)
                || counters
                    .before_encoding
                    .load(Ordering::Acquire)
                    .checked_add(UNINITIALIZED_RECORDS.load(Ordering::Acquire))
                    .is_none(),
        }
    }
}

struct Reservation {
    state: Arc<State>,
    bytes: usize,
}

impl Reservation {
    fn retain_actual(&mut self, bytes: usize) -> bool {
        if bytes <= self.bytes {
            self.state
                .funded
                .fetch_sub(self.bytes - bytes, Ordering::AcqRel);
            self.bytes = bytes;
            true
        } else if let Some(mut extra) = self.state.reserve(bytes - self.bytes) {
            // Transfer this additional reservation without releasing its bytes.
            self.bytes = bytes;
            extra.bytes = 0;
            true
        } else {
            false
        }
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.state.funded.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

struct Record {
    bytes: Vec<u8>,
    funding: Reservation,
    queued: bool,
    settled: bool,
}

impl Record {
    fn received(&mut self) {
        if self.queued {
            self.funding.state.queued.fetch_sub(1, Ordering::AcqRel);
            self.queued = false;
        }
    }

    fn discard(&mut self) {
        if !self.settled {
            let counters = &self.funding.state.counters;
            counters.add(&counters.encoded_drops, 1);
            counters.add(&counters.discarded_bytes, self.bytes.len() as u64);
            self.settled = true;
        }
    }
}

impl Drop for Record {
    fn drop(&mut self) {
        // Receiver retirement can discard a successfully enqueued record that
        // raced the writer's final drain. The record owns truthful settlement
        // even when no writer ever receives it; funding alone is insufficient.
        self.received();
        self.discard();
    }
}

enum Message {
    Record(Record),
    Shutdown,
}

// Charging every record its actual buffer and envelope means even empty
// messages cannot create an unfunded, unbounded population in the channel.
const ENVELOPE_BYTES: usize = size_of::<Message>();

struct Diagnostics {
    state: Arc<State>,
    sender: mpsc::Sender<Message>,
}

impl Diagnostics {
    fn start(file: File, limits: DiagnosticLimits) -> io::Result<Self> {
        validate_limits(limits)?;
        let state = Arc::new(State {
            limits,
            funded: AtomicUsize::new(0),
            formatting: AtomicUsize::new(0),
            queued: AtomicUsize::new(0),
            active_bytes: AtomicUsize::new(0),
            writer_alive: AtomicBool::new(false),
            shutdown: AtomicBool::new(false),
            sink: AtomicU8::new(STARTING),
            last_errno: AtomicI32::new(0),
            counters: Counters::default(),
        });
        let (sender, receiver) = mpsc::channel();
        let owner_state = Arc::clone(&state);
        let handle = thread::Builder::new()
            .name("ctxmux-diagnostics".to_owned())
            .spawn(move || writer_main(file, receiver, &owner_state))?;
        // The descriptor belongs exclusively to the thread. Shutdown sends a
        // stop request but never joins a writer blocked in an inherited pipe.
        drop(handle);
        Ok(Self { state, sender })
    }

    fn record(&self, arguments: fmt::Arguments<'_>) -> DiagnosticAdmission {
        let state = &self.state;
        let counters = &state.counters;
        if state.shutdown.load(Ordering::Acquire) {
            counters.add(&counters.before_encoding, 1);
            return DiagnosticAdmission::Shutdown;
        }
        if state.sink.load(Ordering::Acquire) == FAILED {
            counters.add(&counters.before_encoding, 1);
            return DiagnosticAdmission::SinkUnavailable;
        }
        let required = state.limits.record_bytes + ENVELOPE_BYTES;
        let Some(mut funding) = state.reserve(required) else {
            counters.add(&counters.before_encoding, 1);
            return DiagnosticAdmission::BudgetPressure;
        };
        state.formatting.fetch_add(1, Ordering::AcqRel);
        let formatting = FormattingGuard(state);
        let mut formatted = BoundedFormat {
            bytes: Vec::new(),
            limit: state.limits.record_bytes,
            oversized: false,
            counters,
            completed: false,
        };
        let result = fmt::write(&mut formatted, arguments)
            .and_then(|()| fmt::Write::write_str(&mut formatted, "\n"));
        formatted.completed = true;
        if result.is_err() {
            counters.add(&counters.discarded_bytes, formatted.bytes.len() as u64);
            if formatted.oversized {
                counters.add(&counters.oversized, 1);
                return DiagnosticAdmission::Oversized;
            }
            counters.add(&counters.format_failed, 1);
            return DiagnosticAdmission::FormatFailed;
        }
        if !funding.retain_actual(formatted.bytes.capacity() + ENVELOPE_BYTES) {
            counters.add(&counters.encoded_drops, 1);
            counters.add(&counters.discarded_bytes, formatted.bytes.len() as u64);
            return DiagnosticAdmission::BudgetPressure;
        }
        drop(formatting);
        // No sender waits for receiver capacity. Actual resident ownership was
        // funded before encoding and stays with the queued/active record.
        state.queued.fetch_add(1, Ordering::AcqRel);
        match self.sender.send(Message::Record(Record {
            bytes: std::mem::take(&mut formatted.bytes),
            funding,
            queued: true,
            settled: false,
        })) {
            Ok(()) => {
                counters.add(&counters.admitted, 1);
                DiagnosticAdmission::Accepted
            }
            Err(error) => {
                // The returned message still owns queue and byte-loss facts.
                drop(error);
                DiagnosticAdmission::SinkUnavailable
            }
        }
    }

    fn shutdown(&self) -> DiagnosticsSnapshot {
        if !self.state.shutdown.swap(true, Ordering::AcqRel) {
            // One fixed control envelope per process, independent of record
            // funding; never a blocking send or join on a full sink.
            let _ = self.sender.send(Message::Shutdown);
        }
        self.state.snapshot()
    }
}

struct FormattingGuard<'a>(&'a State);
impl Drop for FormattingGuard<'_> {
    fn drop(&mut self) {
        self.0.formatting.fetch_sub(1, Ordering::AcqRel);
    }
}

struct BoundedFormat<'a> {
    bytes: Vec<u8>,
    limit: usize,
    oversized: bool,
    counters: &'a Counters,
    completed: bool,
}

impl Drop for BoundedFormat<'_> {
    fn drop(&mut self) {
        // A panic hook itself runs while thread::panicking() is true. Only
        // an interrupted formatter owns loss; a completed hook record does not.
        if !self.completed && thread::panicking() {
            self.counters.add(&self.counters.format_failed, 1);
            self.counters
                .add(&self.counters.discarded_bytes, self.bytes.len() as u64);
        }
    }
}

impl fmt::Write for BoundedFormat<'_> {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        if value.len() > self.limit - self.bytes.len() {
            self.oversized = true;
            return Err(fmt::Error);
        }
        self.bytes
            .try_reserve_exact(value.len())
            .map_err(|_| fmt::Error)?;
        self.bytes.extend_from_slice(value.as_bytes());
        Ok(())
    }
}

pub(crate) fn validate_limits(limits: DiagnosticLimits) -> io::Result<()> {
    if limits.record_bytes == 0
        || limits
            .record_bytes
            .checked_add(ENVELOPE_BYTES)
            .is_none_or(|required| required > limits.queue_bytes)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "diagnostic queue must fund one positive-size record and its envelope",
        ));
    }
    Ok(())
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
struct Losses {
    unformatted: u64,
    encoded: u64,
    discarded_bytes: u64,
    oversized: u64,
    format_failed: u64,
    write_failed: u64,
}

fn losses(state: &State) -> Losses {
    let counters = &state.counters;
    Losses {
        unformatted: counters
            .before_encoding
            .load(Ordering::Acquire)
            .saturating_add(UNINITIALIZED_RECORDS.load(Ordering::Acquire)),
        encoded: counters.encoded_drops.load(Ordering::Acquire),
        discarded_bytes: counters.discarded_bytes.load(Ordering::Acquire),
        oversized: counters.oversized.load(Ordering::Acquire),
        format_failed: counters.format_failed.load(Ordering::Acquire),
        write_failed: counters.write_failed.load(Ordering::Acquire),
    }
}

struct CountedSink<'a> {
    file: &'a mut File,
    state: &'a State,
    notice: bool,
    confirmed: usize,
}

fn wait_writable(file: &File) -> io::Result<()> {
    use rustix::{
        event::{PollFd, PollFlags, poll},
        io::Errno,
    };
    let mut ready = [PollFd::new(file, PollFlags::OUT)];
    loop {
        match poll(&mut ready, None) {
            Ok(_) if ready[0].revents().contains(PollFlags::NVAL) => {
                // Some host device types cannot be polled; report that observed
                // readiness failure rather than inventing a closed descriptor.
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "diagnostic sink readiness polling unavailable",
                ));
            }
            Ok(_) => return Ok(()),
            Err(Errno::INTR) => {}
            Err(error) => return Err(error.into()),
        }
    }
}

impl Write for CountedSink<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let count = loop {
            match self.file.write(bytes) {
                Ok(count) => break count,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    // A legal inherited nonblocking sink has applied zero bytes
                    // for this call. Wait only in this dedicated writer, then
                    // continue the same remaining suffix without changing OFD flags.
                    wait_writable(self.file)?;
                }
                Err(error) => return Err(error),
            }
        };
        self.confirmed += count;
        self.state
            .counters
            .add(&self.state.counters.written_bytes, count as u64);
        if self.notice {
            self.state
                .counters
                .add(&self.state.counters.notice_bytes, count as u64);
        }
        Ok(count)
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn writer_main(mut file: File, receiver: mpsc::Receiver<Message>, state: &State) {
    state.writer_alive.store(true, Ordering::Release);
    state.sink.store(IDLE, Ordering::Release);
    let _alive = WriterAliveGuard(state);
    let mut reported = Losses::default();
    while let Ok(message) = receiver.recv() {
        let Message::Record(mut record) = message else {
            break;
        };
        record.received();
        if state.shutdown.load(Ordering::Acquire) {
            record.discard();
            continue;
        }
        state
            .active_bytes
            .store(record.bytes.len(), Ordering::Release);
        state.sink.store(WRITING, Ordering::Release);
        let current = losses(state);
        if current != reported {
            let mut sink = CountedSink {
                file: &mut file,
                state,
                notice: true,
                confirmed: 0,
            };
            // Direct formatting uses no second record buffer. Only this sink
            // owner may block; the notice explicitly distinguishes unknown
            // source lengths from known discarded encodings.
            if let Err(error) = writeln!(
                sink,
                "ctxmuxd: diagnostic-incomplete unformatted_records={} encoded_drops={} known_discarded_bytes={} oversized_records={} format_failed_records={} sink_write_failures={} unformatted_or_oversized_source_lengths=unknown",
                current.unformatted,
                current.encoded,
                current.discarded_bytes,
                current.oversized,
                current.format_failed,
                current.write_failed
            ) {
                record.discard();
                sink_failed(state, &error);
                break;
            }
            reported = current;
        }
        let mut sink = CountedSink {
            file: &mut file,
            state,
            notice: false,
            confirmed: 0,
        };
        if let Err(error) = sink.write_all(&record.bytes) {
            if sink.confirmed == 0 {
                state.counters.add(&state.counters.encoded_drops, 1);
            } else {
                state.counters.add(&state.counters.partial_records, 1);
            }
            state.counters.add(
                &state.counters.discarded_bytes,
                (record.bytes.len() - sink.confirmed) as u64,
            );
            record.settled = true;
            sink_failed(state, &error);
            break;
        }
        record.settled = true;
        state.counters.add(&state.counters.written, 1);
        state.active_bytes.store(0, Ordering::Release);
        state.sink.store(IDLE, Ordering::Release);
    }
    // Includes records racing admission with stop or a hard sink failure.
    for message in receiver.try_iter() {
        if let Message::Record(mut record) = message {
            record.received();
            record.discard();
        }
    }
    // Drop must settle late accepted records as well as disconnect senders.
    drop(receiver);
    state.active_bytes.store(0, Ordering::Release);
    if state.sink.load(Ordering::Acquire) != FAILED {
        state.sink.store(STOPPED, Ordering::Release);
    }
}

fn sink_failed(state: &State, error: &io::Error) {
    state.counters.add(&state.counters.write_failed, 1);
    state
        .last_errno
        .store(error.raw_os_error().unwrap_or(0), Ordering::Release);
    state.sink.store(FAILED, Ordering::Release);
}

struct WriterAliveGuard<'a>(&'a State);
impl Drop for WriterAliveGuard<'_> {
    fn drop(&mut self) {
        self.0.writer_alive.store(false, Ordering::Release);
    }
}

struct Initialization {
    limits: DiagnosticLimits,
    result: Result<Diagnostics, (io::ErrorKind, Option<i32>)>,
}

static GLOBAL: OnceLock<Initialization> = OnceLock::new();
static HOOK: Once = Once::new();
static UNINITIALIZED_RECORDS: AtomicU64 = AtomicU64::new(0);
static SCOPED_PANICS: AtomicU64 = AtomicU64::new(0);
static GLOBAL_COUNTERS_SATURATED: AtomicBool = AtomicBool::new(false);
thread_local! { static NATIVE_UNWIND_SCOPE: Cell<bool> = const { Cell::new(false) }; }

pub(crate) fn initialize(limits: DiagnosticLimits) -> io::Result<()> {
    validate_limits(limits)?;
    install_hook();
    let init = GLOBAL.get_or_init(|| {
        // Descriptor duplication never changes the inherited open-file
        // description's status flags or takes the process-wide stderr lock.
        // POSIX standard descriptors are stdin=0, stdout=1, stderr=2. Start
        // after them so a standard-fd hole never aliases this owned sink with
        // a later host stdio replacement; this is identity, not a capacity cap.
        let result = rustix::io::fcntl_dupfd_cloexec(io::stderr(), 3)
            .map_err(io::Error::from)
            .and_then(|fd| Diagnostics::start(File::from(fd), limits))
            .map_err(|error| (error.kind(), error.raw_os_error()));
        Initialization { limits, result }
    });
    if init.limits != limits {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "diagnostic singleton already has another resource policy",
        ));
    }
    match &init.result {
        Ok(_) => Ok(()),
        Err((_kind, Some(errno))) => Err(io::Error::from_raw_os_error(*errno)),
        Err((kind, None)) => Err(io::Error::new(
            *kind,
            "diagnostic sink initialization failed",
        )),
    }
}

pub(crate) fn record(arguments: fmt::Arguments<'_>) -> DiagnosticAdmission {
    if let Some(Initialization {
        result: Ok(writer), ..
    }) = GLOBAL.get()
    {
        writer.record(arguments)
    } else {
        add_saturating(&UNINITIALIZED_RECORDS, 1, &GLOBAL_COUNTERS_SATURATED);
        if GLOBAL.get().is_some() {
            DiagnosticAdmission::SinkUnavailable
        } else {
            DiagnosticAdmission::NotInitialized
        }
    }
}

pub(crate) fn snapshot() -> DiagnosticsSnapshot {
    if let Some(Initialization {
        result: Ok(writer), ..
    }) = GLOBAL.get()
    {
        return writer.state.snapshot();
    }
    let failed = GLOBAL.get().filter(|init| init.result.is_err());
    DiagnosticsSnapshot {
        queue_budget_bytes: GLOBAL.get().map_or(0, |init| init.limits.queue_bytes),
        record_limit_bytes: GLOBAL.get().map_or(0, |init| init.limits.record_bytes),
        funded_bytes: 0,
        formatting_records: 0,
        queued_records: 0,
        active_record_bytes: 0,
        admitted_records: 0,
        written_records: 0,
        written_bytes: 0,
        notice_written_bytes: 0,
        dropped_before_encoding_records: UNINITIALIZED_RECORDS.load(Ordering::Acquire),
        dropped_encoded_records: 0,
        discarded_encoded_bytes: 0,
        oversized_records: 0,
        format_failed_records: 0,
        sink_write_failures: 0,
        initialization_failures: u64::from(failed.is_some()),
        partially_written_records: 0,
        scoped_panics: SCOPED_PANICS.load(Ordering::Acquire),
        sink: if failed.is_some() {
            DiagnosticsSinkState::Failed
        } else {
            DiagnosticsSinkState::NotInitialized
        },
        last_sink_errno: failed
            .and_then(|init| init.result.as_ref().err())
            .and_then(|(_, errno)| *errno),
        writer_alive: false,
        shutdown_requested: false,
        counters_saturated: GLOBAL_COUNTERS_SATURATED.load(Ordering::Acquire),
    }
}

/// Request owner shutdown without joining a sink blocked in a host descriptor.
/// Pending/active observations remain truthful until the writer actually exits.
pub(crate) fn shutdown() -> DiagnosticsSnapshot {
    if let Some(Initialization {
        result: Ok(writer), ..
    }) = GLOBAL.get()
    {
        writer.shutdown()
    } else {
        snapshot()
    }
}

fn install_hook() {
    HOOK.call_once(|| {
        let previous = panic::take_hook();
        panic::set_hook(Box::new(move |info| {
            if NATIVE_UNWIND_SCOPE.try_with(Cell::get).unwrap_or(false) {
                add_saturating(&SCOPED_PANICS, 1, &GLOBAL_COUNTERS_SATURATED);
                let _ = record(format_args!("ctxmuxd: native/derived panic: {info}"));
            } else {
                previous(info);
            }
        }));
    });
}

struct ScopeGuard(bool);
impl Drop for ScopeGuard {
    fn drop(&mut self) {
        NATIVE_UNWIND_SCOPE.set(self.0);
    }
}

/// The caller still owns the unwind result and its public failure publication.
/// Only this explicitly entered daemon-owned Native/derived scope avoids the
/// previous hook's synchronous stderr write before `catch_unwind` can return.
pub(crate) fn catch_native_unwind<F: FnOnce() -> T + UnwindSafe, T>(
    operation: F,
) -> thread::Result<T> {
    install_hook();
    let _scope = ScopeGuard(NATIVE_UNWIND_SCOPE.replace(true));
    panic::catch_unwind(operation)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustix::{
        fs::{OFlags, fcntl_getfl, fcntl_setfl},
        io::Errno,
        pipe::pipe,
    };
    use std::{
        os::fd::OwnedFd,
        time::{Duration, Instant},
    };

    // The unchanged private public-proof arrival deadline, not runtime policy.
    const ARRIVAL_BUDGET: Duration = Duration::from_secs(2);
    fn await_fact(mut predicate: impl FnMut() -> bool) {
        let deadline = Instant::now() + ARRIVAL_BUDGET;
        while !predicate() {
            assert!(
                Instant::now() < deadline,
                "diagnostic owner fact did not arrive"
            );
            thread::yield_now();
        }
    }

    fn full_private_pipe() -> (OwnedFd, OwnedFd, usize) {
        let (reader, writer) = pipe().unwrap();
        for fd in [&reader, &writer] {
            rustix::io::fcntl_setfd(fd, rustix::io::FdFlags::CLOEXEC).unwrap();
            fcntl_setfl(fd, fcntl_getfl(fd).unwrap() | OFlags::NONBLOCK).unwrap();
        }
        let mut filled = 0;
        loop {
            match rustix::io::write(&writer, b"F") {
                Ok(1) => filled += 1,
                Err(Errno::AGAIN) => break,
                other => panic!("private pipe fill failed: {other:?}"),
            }
        }
        // The helper changes only its fresh private pipe before inheritance.
        // The writer under test receives blocking flags and must preserve them.
        fcntl_setfl(&writer, fcntl_getfl(&writer).unwrap() & !OFlags::NONBLOCK).unwrap();
        assert!(filled > 0);
        (reader, writer, filled)
    }

    fn drain_available(reader: &OwnedFd, collected: &mut Vec<u8>) {
        loop {
            let mut byte = [0];
            match rustix::io::read(reader, &mut byte) {
                Ok(0) | Err(Errno::AGAIN) => break,
                Ok(1) => collected.push(byte[0]),
                other => panic!("private diagnostic drain failed: {other:?}"),
            }
        }
    }

    #[test]
    fn blocked_sink_funds_actual_capacity_and_exposes_loss_after_recovery() {
        let (reader, writer, filled) = full_private_pipe();
        let inspect = rustix::io::fcntl_dupfd_cloexec(&writer, 0).unwrap();
        let original_flags = fcntl_getfl(&inspect).unwrap();
        // Single-record limit is a representative test payload allowance.
        // The total is derived from envelope and payload ownership, not a
        // hidden population cap or a production default.
        let record_bytes = b"first\n".len() * 16;
        let limits = DiagnosticLimits {
            queue_bytes: (record_bytes + ENVELOPE_BYTES) * 3,
            record_bytes,
        };
        let logger = Diagnostics::start(File::from(writer), limits).unwrap();
        assert_eq!(
            logger.record(format_args!("first")),
            DiagnosticAdmission::Accepted
        );
        await_fact(|| logger.state.snapshot().sink == DiagnosticsSinkState::Writing);
        let first = logger.state.snapshot();
        assert_eq!(first.written_bytes, 0);
        assert_eq!(first.funded_bytes, b"first\n".len() + ENVELOPE_BYTES);
        assert_eq!(first.active_record_bytes, b"first\n".len());
        assert_eq!(fcntl_getfl(&inspect).unwrap(), original_flags);
        loop {
            match logger.record(format_args!("next")) {
                DiagnosticAdmission::Accepted => {}
                DiagnosticAdmission::BudgetPressure => break,
                other => panic!("unexpected diagnostic admission: {other:?}"),
            }
        }
        let pressured = logger.state.snapshot();
        assert!(pressured.dropped_before_encoding_records > first.dropped_before_encoding_records);
        assert!(pressured.funded_bytes <= limits.queue_bytes);
        assert!(pressured.queued_records > 0);
        let mut observed = Vec::new();
        await_fact(|| {
            drain_available(&reader, &mut observed);
            logger.state.snapshot().funded_bytes == 0
        });
        drain_available(&reader, &mut observed);
        assert!(observed[..filled].iter().all(|byte| *byte == b'F'));
        let recovered = String::from_utf8(observed[filled..].to_vec()).unwrap();
        assert!(recovered.contains("first\n"));
        assert!(recovered.contains("diagnostic-incomplete"));
        assert!(recovered.contains("unformatted_or_oversized_source_lengths=unknown"));
        assert!(logger.state.snapshot().notice_written_bytes > 0);
        logger.shutdown();
        await_fact(|| !logger.state.snapshot().writer_alive);
        assert_eq!(logger.state.snapshot().funded_bytes, 0);
        assert_eq!(fcntl_getfl(&inspect).unwrap(), original_flags);
    }

    #[test]
    fn concurrent_formatting_is_funded_before_encoding_and_returns_unused_allowance() {
        struct Paused {
            started: mpsc::Sender<()>,
            release: mpsc::Receiver<()>,
        }
        impl fmt::Display for Paused {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.started.send(()).unwrap();
                self.release.recv().unwrap();
                formatter.write_str("held")
            }
        }
        let (reader, writer, _) = full_private_pipe();
        let record_bytes = b"held\n".len() * 16;
        let limits = DiagnosticLimits {
            queue_bytes: record_bytes + ENVELOPE_BYTES,
            record_bytes,
        };
        let logger = Arc::new(Diagnostics::start(File::from(writer), limits).unwrap());
        let (started, ready) = mpsc::channel();
        let (release, resume) = mpsc::channel();
        let formatting_logger = Arc::clone(&logger);
        let formatter = thread::spawn(move || {
            formatting_logger.record(format_args!(
                "{}",
                Paused {
                    started,
                    release: resume
                }
            ))
        });
        ready.recv_timeout(ARRIVAL_BUDGET).unwrap();
        let temporary = logger.state.snapshot();
        assert_eq!(temporary.formatting_records, 1);
        assert_eq!(temporary.funded_bytes, limits.queue_bytes);
        assert_eq!(temporary.queued_records, 0);
        assert_eq!(
            logger.record(format_args!("second")),
            DiagnosticAdmission::BudgetPressure
        );
        release.send(()).unwrap();
        assert_eq!(formatter.join().unwrap(), DiagnosticAdmission::Accepted);
        await_fact(|| logger.state.snapshot().sink == DiagnosticsSinkState::Writing);
        let retained = logger.state.snapshot();
        assert_eq!(retained.formatting_records, 0);
        assert_eq!(retained.funded_bytes, b"held\n".len() + ENVELOPE_BYTES);
        assert_eq!(retained.active_record_bytes, b"held\n".len());
        drop(reader);
        await_fact(|| !logger.state.snapshot().writer_alive);
        assert_eq!(logger.state.snapshot().funded_bytes, 0);
    }

    #[test]
    fn inherited_nonblocking_full_sink_preserves_suffix_and_shutdown_ownership() {
        let (reader, writer, filled) = full_private_pipe();
        fcntl_setfl(&writer, fcntl_getfl(&writer).unwrap() | OFlags::NONBLOCK).unwrap();
        let inspect = rustix::io::fcntl_dupfd_cloexec(&writer, 0).unwrap();
        let flags = fcntl_getfl(&inspect).unwrap();
        let alphabet = b"abcdefghijklmnopqrstuvwxyz";
        let payload: String = (0..filled * 2)
            .map(|index| char::from(alphabet[index % alphabet.len()]))
            .collect();
        let record_bytes = payload.len() + b"\n".len();
        let logger = Diagnostics::start(
            File::from(writer),
            DiagnosticLimits {
                queue_bytes: record_bytes + ENVELOPE_BYTES,
                record_bytes,
            },
        )
        .unwrap();
        assert_eq!(
            logger.record(format_args!("{payload}")),
            DiagnosticAdmission::Accepted
        );
        await_fact(|| logger.state.snapshot().sink == DiagnosticsSinkState::Writing);
        let blocked = logger.shutdown();
        assert!(blocked.writer_alive);
        assert!(blocked.shutdown_requested);
        assert_eq!(blocked.written_bytes, 0);
        assert_eq!(blocked.sink_write_failures, 0);
        assert_eq!(blocked.active_record_bytes, record_bytes);
        assert!(blocked.funded_bytes > 0);
        assert_eq!(fcntl_getfl(&inspect).unwrap(), flags);
        let mut observed = Vec::new();
        await_fact(|| {
            drain_available(&reader, &mut observed);
            !logger.state.snapshot().writer_alive
        });
        drain_available(&reader, &mut observed);
        assert!(observed[..filled].iter().all(|byte| *byte == b"F"[0]));
        let mut expected = payload.into_bytes();
        expected.push(b"\n"[0]);
        assert_eq!(&observed[filled..], expected);
        let completed = logger.state.snapshot();
        assert_eq!(completed.written_bytes, record_bytes as u64);
        assert_eq!(completed.written_records, 1);
        assert_eq!(completed.funded_bytes, 0);
        assert_eq!(completed.sink_write_failures, 0);
        assert_eq!(completed.sink, DiagnosticsSinkState::Stopped);
        assert_eq!(fcntl_getfl(&inspect).unwrap(), flags);
    }

    #[test]
    fn accepted_record_dropped_by_receiver_retirement_settles_loss_and_queue() {
        let payload = b"accepted but never received\n";
        let limits = DiagnosticLimits {
            queue_bytes: payload.len() + ENVELOPE_BYTES,
            record_bytes: payload.len(),
        };
        let logger = Diagnostics::start(
            File::options().write(true).open("/dev/null").unwrap(),
            limits,
        )
        .unwrap();
        let (sender, receiver) = mpsc::channel();
        let funding = logger.state.reserve(limits.queue_bytes).unwrap();
        logger.state.queued.fetch_add(1, Ordering::AcqRel);
        // This successful send is precisely the channel ownership boundary:
        // retirement may follow a final empty try_iter before the writer can
        // observe this accepted late record. No writer is permitted to settle it.
        assert!(
            sender
                .send(Message::Record(Record {
                    bytes: payload.to_vec(),
                    funding,
                    queued: true,
                    settled: false
                }))
                .is_ok()
        );
        logger
            .state
            .counters
            .add(&logger.state.counters.admitted, 1);
        drop(receiver);
        let retired = logger.state.snapshot();
        assert_eq!(retired.queued_records, 0);
        assert_eq!(retired.funded_bytes, 0);
        assert_eq!(retired.admitted_records, 1);
        assert_eq!(retired.written_records, 0);
        assert_eq!(retired.dropped_encoded_records, 1);
        assert_eq!(retired.discarded_encoded_bytes, payload.len() as u64);
        logger.shutdown();
        await_fact(|| {
            !logger.state.snapshot().writer_alive
                && logger.state.snapshot().sink == DiagnosticsSinkState::Stopped
        });
    }

    #[test]
    fn closed_sink_is_observable_and_retires_record_funding() {
        let (reader, writer) = pipe().unwrap();
        drop(reader);
        let record_bytes = b"closed\n".len();
        let logger = Diagnostics::start(
            File::from(writer),
            DiagnosticLimits {
                queue_bytes: record_bytes + ENVELOPE_BYTES,
                record_bytes,
            },
        )
        .unwrap();
        assert_eq!(
            logger.record(format_args!("closed")),
            DiagnosticAdmission::Accepted
        );
        await_fact(|| {
            logger.state.snapshot().sink == DiagnosticsSinkState::Failed
                && !logger.state.snapshot().writer_alive
        });
        let failed = logger.state.snapshot();
        assert_eq!(failed.sink_write_failures, 1);
        assert_eq!(failed.written_bytes, 0);
        assert_eq!(failed.dropped_encoded_records, 1);
        assert_eq!(failed.discarded_encoded_bytes, record_bytes as u64);
        assert_eq!(failed.funded_bytes, 0);
        assert_eq!(failed.last_sink_errno, Some(Errno::PIPE.raw_os_error()));
        assert_eq!(
            logger.record(format_args!("again")),
            DiagnosticAdmission::SinkUnavailable
        );
    }

    #[test]
    fn shutdown_never_joins_a_writer_blocked_in_an_open_pipe() {
        let (reader, writer, _) = full_private_pipe();
        let record_bytes = b"blocked\n".len();
        let logger = Diagnostics::start(
            File::from(writer),
            DiagnosticLimits {
                queue_bytes: record_bytes + ENVELOPE_BYTES,
                record_bytes,
            },
        )
        .unwrap();
        assert_eq!(
            logger.record(format_args!("blocked")),
            DiagnosticAdmission::Accepted
        );
        await_fact(|| logger.state.snapshot().sink == DiagnosticsSinkState::Writing);
        let stopped = logger.shutdown();
        assert!(stopped.shutdown_requested);
        assert!(stopped.writer_alive);
        assert!(
            stopped.funded_bytes > 0,
            "blocked record is still owned, not fictitiously freed"
        );
        assert_eq!(
            logger.record(format_args!("later")),
            DiagnosticAdmission::Shutdown
        );
        drop(reader);
        await_fact(|| !logger.state.snapshot().writer_alive);
        assert_eq!(logger.state.snapshot().funded_bytes, 0);
    }

    #[test]
    fn oversize_and_formatter_failures_do_not_publish_complete_looking_prefixes() {
        use std::io::{Read, Seek};
        struct Fails;
        impl fmt::Display for Fails {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("partial")?;
                Err(fmt::Error)
            }
        }
        let file = tempfile::tempfile().unwrap();
        let mut inspect = file.try_clone().unwrap();
        let record_bytes = b"partial\n".len();
        let logger = Diagnostics::start(
            file,
            DiagnosticLimits {
                queue_bytes: record_bytes + ENVELOPE_BYTES,
                record_bytes,
            },
        )
        .unwrap();
        assert_eq!(
            logger.record(format_args!("too-long-record")),
            DiagnosticAdmission::Oversized
        );
        assert_eq!(
            logger.record(format_args!("{Fails}")),
            DiagnosticAdmission::FormatFailed
        );
        let facts = logger.state.snapshot();
        assert_eq!(facts.oversized_records, 1);
        assert_eq!(facts.format_failed_records, 1);
        assert_eq!(facts.discarded_encoded_bytes, b"partial".len() as u64);
        assert_eq!(facts.funded_bytes, 0);
        logger.shutdown();
        await_fact(|| !logger.state.snapshot().writer_alive);
        inspect.rewind().unwrap();
        let mut written = Vec::new();
        inspect.read_to_end(&mut written).unwrap();
        assert!(written.is_empty());
    }
    #[test]
    fn scoped_panic_child() {
        static PREVIOUS_HOOK_CALLS: AtomicUsize = AtomicUsize::new(0);
        struct Panics;
        impl fmt::Display for Panics {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("partial")?;
                panic!("formatter unwind");
            }
        }
        let Some(mode) = std::env::var_os("CTXMUX_PRIVATE_DIAGNOSTIC_PANIC_CHILD") else {
            return;
        };
        if mode == "init_failure" {
            use rustix::process::{Resource, getrlimit, setrlimit};
            let mut limit = getrlimit(Resource::Nofile);
            // Representative hostile test host: bound only this private child
            // to a small descriptor allowance, then observe real EMFILE. The
            // value is not product capacity or a diagnostic acceptance oracle.
            limit.current = Some(limit.current.unwrap_or(32).min(32));
            setrlimit(Resource::Nofile, limit).unwrap();
            let mut descriptors = Vec::new();
            loop {
                match File::open("/dev/null") {
                    Ok(file) => descriptors.push(file),
                    Err(error) => {
                        assert_eq!(
                            error.raw_os_error(),
                            Some(rustix::io::Errno::MFILE.raw_os_error())
                        );
                        break;
                    }
                }
            }
            let limits = DiagnosticLimits {
                queue_bytes: 4096,
                record_bytes: 1024,
            };
            let error = initialize(limits).unwrap_err();
            assert_eq!(
                error.raw_os_error(),
                Some(rustix::io::Errno::MFILE.raw_os_error())
            );
            let failed = snapshot();
            assert_eq!(failed.sink, DiagnosticsSinkState::Failed);
            assert_eq!(failed.initialization_failures, 1);
            assert_eq!(failed.last_sink_errno, error.raw_os_error());
            assert_eq!(failed.funded_bytes, 0);
            assert!(!failed.writer_alive);
            drop(descriptors);
            assert_eq!(
                record(format_args!("sink unavailable")),
                DiagnosticAdmission::SinkUnavailable
            );
            assert_eq!(snapshot().dropped_before_encoding_records, 1);
            assert!(
                initialize(limits).is_err(),
                "no fabricated healthy state after failed initialization"
            );
            let conflict = initialize(DiagnosticLimits {
                queue_bytes: limits.queue_bytes + 1,
                ..limits
            })
            .unwrap_err();
            assert_eq!(
                conflict.kind(),
                io::ErrorKind::AlreadyExists,
                "different singleton policy must be explicit"
            );
            println!("INITIALIZATION_FAILURE_OBSERVED_WITHOUT_HEALTH_FABRICATION");
            return;
        }
        panic::set_hook(Box::new(|_| {
            PREVIOUS_HOOK_CALLS.fetch_add(1, Ordering::AcqRel);
        }));
        initialize(DiagnosticLimits {
            queue_bytes: 4096,
            record_bytes: 1024,
        })
        .unwrap();
        assert!(catch_native_unwind(|| panic!("owned Native failure")).is_err());
        assert_eq!(PREVIOUS_HOOK_CALLS.load(Ordering::Acquire), 0);
        assert!(catch_native_unwind(|| record(format_args!("{Panics}"))).is_err());
        assert_eq!(PREVIOUS_HOOK_CALLS.load(Ordering::Acquire), 0);
        let facts = snapshot();
        println!(
            "SCOPED_PANIC_FACTS {}",
            serde_json::to_string(&facts).unwrap()
        );
        assert_eq!(facts.scoped_panics, 2);
        assert_eq!(facts.format_failed_records, 1);
        assert_eq!(facts.discarded_encoded_bytes, b"partial".len() as u64);
        assert_eq!(facts.formatting_records, 0);
        assert_eq!(facts.written_bytes, 0);
        assert!(panic::catch_unwind(|| panic!("outside protected scope")).is_err());
        assert_eq!(
            PREVIOUS_HOOK_CALLS.load(Ordering::Acquire),
            1,
            "previous hook preserved outside scope"
        );
        let facts = shutdown();
        assert!(facts.shutdown_requested);
        println!("SCOPED_NATIVE_UNWIND_RETURNED_PREVIOUS_HOOK_PRESERVED");
    }

    #[test]
    fn scoped_panic_hook_never_writes_synchronously_to_full_stderr() {
        private_diagnostic_child(
            "scope",
            "SCOPED_NATIVE_UNWIND_RETURNED_PREVIOUS_HOOK_PRESERVED",
        );
    }

    #[test]
    fn failed_initialization_and_policy_conflict_remain_observable() {
        private_diagnostic_child(
            "init_failure",
            "INITIALIZATION_FAILURE_OBSERVED_WITHOUT_HEALTH_FABRICATION",
        );
    }

    fn private_diagnostic_child(mode: &str, expected: &str) {
        use std::io::Read;
        use std::process::{Command, Stdio};
        let (reader, writer, _) = full_private_pipe();
        let inspect = rustix::io::fcntl_dupfd_cloexec(&writer, 0).unwrap();
        let flags = fcntl_getfl(&inspect).unwrap();
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "diagnostics::tests::scoped_panic_child",
                "--nocapture",
            ])
            .env("CTXMUX_PRIVATE_DIAGNOSTIC_PANIC_CHILD", mode)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::from(File::from(writer)))
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + ARRIVAL_BUDGET;
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            if std::time::Instant::now() >= deadline {
                // This exact child was spawned here; production is never looked up.
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("private panic child blocked on its full diagnostic sink");
            }
            thread::yield_now();
        };
        let mut output = String::new();
        child
            .stdout
            .take()
            .unwrap()
            .read_to_string(&mut output)
            .unwrap();
        assert!(
            status.success(),
            "private child status {status}; stdout: {output}"
        );
        assert!(output.contains(expected));
        assert_eq!(fcntl_getfl(&inspect).unwrap(), flags);
        drop(reader);
    }
}
