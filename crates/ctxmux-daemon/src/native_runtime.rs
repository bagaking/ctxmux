//! Daemon-wide ownership of native PTY input, output and child lifecycle work.

use std::{
    collections::{HashMap, VecDeque},
    fs::File,
    io::{self, Read, Write},
    os::unix::net::UnixStream,
    panic::AssertUnwindSafe,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

use ctxmux_protocol::{
    CommandDisposition, ControlFailure, ErrorCode, NativeServiceFailure, ProtocolError, RunId,
    RunState,
};
use portable_pty::Child;
use rustix::{
    event::{PollFd, PollFlags, Timespec, poll},
    io::Errno,
};

use crate::{
    CHILD_CONTROL_POLL, NativeWaitFailure, PendingChild, Run, STOP_FORCED_TIMEOUT,
    STOP_GRACEFUL_TIMEOUT, exit_state, mutex_lock,
    native_control::{ChildCommand, HandoffInputState, NativeControlOwner, StopOwnerResult},
    native_session::NativeSession,
    qualification_stats::GaugeGuard,
};

// Persistence finalization is serialized by the persistence actor. Keep a
// separate bounded handoff budget so blocked publication cannot consume native
// cleanup admission; the two limits describe different resources.
const OUTPUT_DRAIN_TIMEOUT: Duration = Duration::from_secs(1);
pub(crate) const OUTPUT_READ_BUFFER_BYTES: usize = 8192;

type AfterWait = Box<dyn FnOnce() + Send + 'static>;

#[derive(Clone)]
pub(crate) struct OwnerWake {
    writer: Arc<Mutex<Option<UnixStream>>>,
}

impl OwnerWake {
    fn pair() -> io::Result<(Self, UnixStream)> {
        let (reader, writer) = UnixStream::pair()?;
        reader.set_nonblocking(true)?;
        writer.set_nonblocking(true)?;
        Ok((
            Self {
                writer: Arc::new(Mutex::new(Some(writer))),
            },
            reader,
        ))
    }

    fn unavailable() -> Self {
        Self {
            writer: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) fn wake(&self) {
        let mut writer_owner = mutex_lock(&self.writer);
        let Some(writer) = writer_owner.as_mut() else {
            return;
        };
        match writer.write(&[1]) {
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(error) if error.kind() == io::ErrorKind::BrokenPipe => {
                *writer_owner = None;
            }
            Err(error) => {
                let _ = crate::diagnostics::record(format_args!(
                    "ctxmuxd native owner wake failed: {error}"
                ));
                writer.shutdown(std::net::Shutdown::Both).ok();
                *writer_owner = None;
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct CleanupAdmission {
    inner: Arc<CleanupAdmissionInner>,
}

struct CleanupAdmissionInner {
    active: AtomicUsize,
    max_active: usize,
    wake: OwnerWake,
}

pub(crate) struct CleanupPermit {
    inner: Arc<CleanupAdmissionInner>,
}

impl CleanupAdmission {
    fn new(max_active: usize, wake: OwnerWake) -> Self {
        Self {
            inner: Arc::new(CleanupAdmissionInner {
                active: AtomicUsize::new(0),
                max_active,
                wake,
            }),
        }
    }

    pub(crate) fn try_acquire(&self) -> Option<CleanupPermit> {
        let mut active = self.inner.active.load(Ordering::Acquire);
        loop {
            if active >= self.inner.max_active {
                return None;
            }
            match self.inner.active.compare_exchange_weak(
                active,
                active + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Some(CleanupPermit {
                        inner: Arc::clone(&self.inner),
                    });
                }
                Err(observed) => active = observed,
            }
        }
    }
}

impl Drop for CleanupPermit {
    fn drop(&mut self) {
        let previous = self.inner.active.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "cleanup admission cannot underflow");
        self.inner.wake.wake();
    }
}

/// One daemon-wide owner. Ordinary Runs add entries, never permanent threads.
#[derive(Clone)]
pub(crate) struct NativeRunOwner {
    inner: Arc<OwnerInner>,
}

struct OwnerInner {
    state: Arc<Mutex<OwnerState>>,
    completion: Arc<Mutex<Option<NativeServiceFailure>>>,
    #[cfg_attr(not(test), allow(dead_code))]
    completion_finished: Arc<AtomicBool>,
    wake: OwnerWake,
    // Set once by `serve` when it attaches the process-wide SIGCHLD relay that
    // wakes this owner on child exits. While false, the owner arms a timed
    // backstop sweep so a natural exit is still detected; while true, the owner
    // relies purely on the relay and blocks with no timer between exits. See
    // `poll_deadline` for the full rationale — this is NOT a production safety
    // net, it exists because unit tests construct owners without `serve`.
    signal_driven: Arc<AtomicBool>,
    // Only read through the `#[cfg(test)]` accessor below; the live owner thread
    // captures its own clone (`owner_cleanup`) before this struct is built. Same
    // test-only retention as `diagnostics`.
    #[cfg_attr(not(test), allow(dead_code))]
    cleanup_admission: CleanupAdmission,
    #[cfg_attr(not(test), allow(dead_code))]
    diagnostics: Arc<OwnerDiagnostics>,
}

#[derive(Default)]
struct OwnerDiagnostics {
    poll_returns: AtomicUsize,
    lifecycle_probes: AtomicUsize,
    registrations: AtomicUsize,
    fail_next_worker_spawn: AtomicUsize,
}

#[cfg(test)]
pub(crate) struct OwnerDiagnosticSnapshot {
    pub(crate) poll_returns: usize,
    pub(crate) lifecycle_probes: usize,
    pub(crate) registrations: usize,
}

enum OwnerState {
    Running {
        commands: mpsc::SyncSender<OwnerCommand>,
        thread: thread::JoinHandle<()>,
    },
    Failed(String),
}

/// Live descriptors of one native Run, paired for exec-in-place handoff: the
/// pty master fd number and the child pid that a post-exec daemon re-adopts.
/// Produced by [`NativeRunOwner::extract_for_handoff`] on the shipped SIGHUP
/// exec-in-place path and consumed by `perform_exec_upgrade` to build the
/// handoff manifest.
#[derive(Debug, Clone)]
pub(crate) struct LiveDescriptors {
    pub run_id: RunId,
    pub child_pid: u32,
    pub master_fd: std::os::fd::RawFd,
    pub input_state: HandoffInputState,
}

type HandoffPreflight = Box<dyn FnOnce(&[LiveDescriptors]) -> Result<(), String> + Send>;

enum OwnerCommand {
    Register(NativeRunRegistration),
    HandoffReady {
        run_id: RunId,
        respond: mpsc::Sender<bool>,
    },
    ExtractForHandoff {
        preflight: HandoffPreflight,
        respond: mpsc::Sender<Result<Vec<LiveDescriptors>, String>>,
    },
    Shutdown,
    #[cfg(test)]
    UnwindForTest,
    #[cfg(test)]
    ProbePendingStopForTest {
        run_id: RunId,
        respond: mpsc::Sender<bool>,
    },
}

pub(crate) struct NativeRunRegistration {
    run: Weak<Run>,
    reader: Option<File>,
    child: Option<PendingChild>,
    session: Option<NativeSession>,
    control: NativeControlOwner,
    wait_failure: NativeWaitFailure,
    after_wait: Option<AfterWait>,
    reader_guard: Option<GaugeGuard>,
    waiter_guard: Option<GaugeGuard>,
}

pub(crate) struct NativeRegistrationError {
    message: String,
    registration: Box<NativeRunRegistration>,
}

impl NativeRegistrationError {
    pub(crate) fn into_parts(self) -> (String, NativeRunRegistration) {
        (self.message, *self.registration)
    }
}

impl NativeRunRegistration {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        run: &Arc<Run>,
        reader: File,
        child: PendingChild,
        session: NativeSession,
        control: NativeControlOwner,
        wait_failure: NativeWaitFailure,
        after_wait: impl FnOnce() + Send + 'static,
        reader_guard: GaugeGuard,
        waiter_guard: GaugeGuard,
    ) -> Self {
        Self {
            run: Arc::downgrade(run),
            reader: Some(reader),
            child: Some(child),
            session: Some(session),
            control,
            wait_failure,
            after_wait: Some(Box::new(after_wait)),
            reader_guard: Some(reader_guard),
            waiter_guard: Some(waiter_guard),
        }
    }

    fn into_entry(mut self) -> NativeEntry {
        let child = self
            .child
            .take()
            .expect("native registration owns one child")
            .into_child();
        let child = NativeChildAuthority(Arc::new(Mutex::new(Some(child))));
        let control = self.control.clone();
        NativeEntry {
            run_id: control.run_id(),
            run: self.run.clone(),
            control: control.clone(),
            _child_authority: child.clone(),
            output: Some(OutputOwner {
                paused: false,
                pending_offer: false,
                reader: self
                    .reader
                    .take()
                    .expect("native registration owns one reader"),
                _control: control.clone(),
                _guard: self
                    .reader_guard
                    .take()
                    .expect("native registration owns one reader guard"),
            }),
            lifecycle: Lifecycle::Watching(Watching {
                child,
                pending_stop: None,
                session: self
                    .session
                    .take()
                    .expect("native registration owns one session"),
                control,
                wait_failure: self.wait_failure.clone(),
                _guard: self
                    .waiter_guard
                    .take()
                    .expect("native registration owns one waiter guard"),
            }),
            after_wait: self.after_wait.take(),
            wait_failure: self.wait_failure.clone(),
            terminal: None,
        }
    }
}

impl Drop for NativeRunRegistration {
    fn drop(&mut self) {
        if self.child.is_some() {
            self.control.mark_closed();
        }
    }
}

impl Default for NativeRunOwner {
    fn default() -> Self {
        Self::with_resources(crate::ResourceLimits::DEFAULT)
    }
}

impl NativeRunOwner {
    pub(crate) fn with_resources(resources: crate::ResourceLimits) -> Self {
        let (commands, receiver) = mpsc::sync_channel(resources.creation_workers);
        let (wake, wake_reader) = match OwnerWake::pair() {
            Ok(pair) => pair,
            Err(error) => {
                return Self {
                    inner: Arc::new(OwnerInner {
                        state: Arc::new(Mutex::new(OwnerState::Failed(format!(
                            "failed to create daemon-wide native owner wake pipe: {error}"
                        )))),
                        completion: Arc::new(Mutex::new(Some(NativeServiceFailure::OwnerStopped))),
                        completion_finished: Arc::new(AtomicBool::new(true)),
                        wake: OwnerWake::unavailable(),
                        signal_driven: Arc::new(AtomicBool::new(false)),
                        cleanup_admission: CleanupAdmission::new(
                            resources.cleanup_workers,
                            OwnerWake::unavailable(),
                        ),
                        diagnostics: Arc::new(OwnerDiagnostics::default()),
                    }),
                };
            }
        };
        let cleanup_admission = CleanupAdmission::new(resources.cleanup_workers, wake.clone());
        let diagnostics = Arc::new(OwnerDiagnostics::default());
        // Default false: a freshly constructed owner arms the timed backstop so
        // any construction site (all of which are unit tests today) detects a
        // natural exit without a relay. `serve` flips it via `mark_signal_driven`
        // once the SIGCHLD relay is attached.
        let signal_driven = Arc::new(AtomicBool::new(false));
        let owner_wake = wake.clone();
        let owner_cleanup = cleanup_admission.clone();
        let owner_diagnostics = Arc::clone(&diagnostics);
        let owner_signal_driven = Arc::clone(&signal_driven);
        let completion = Arc::new(Mutex::new(None));
        let owner_completion = Arc::clone(&completion);
        let completion_finished = Arc::new(AtomicBool::new(false));
        let owner_finished = Arc::clone(&completion_finished);
        let state = Arc::new(Mutex::new(OwnerState::Failed(
            "native owner starting".to_owned(),
        )));
        let owner_state = Arc::clone(&state);
        let (started_tx, started_rx) = mpsc::channel();
        let initial_state = match thread::Builder::new()
            .name("ctxmux-native-owner".to_owned())
            .spawn(move || {
                if started_rx.recv().is_err() {
                    return;
                }
                owner_main(
                    &receiver,
                    wake_reader,
                    &owner_wake,
                    &owner_cleanup,
                    &owner_diagnostics,
                    &owner_signal_driven,
                    resources,
                    &owner_completion,
                    &owner_state,
                    &owner_finished,
                );
            }) {
            Ok(thread) => OwnerState::Running { commands, thread },
            Err(error) => {
                OwnerState::Failed(format!("failed to start daemon-wide native owner: {error}"))
            }
        };
        *mutex_lock(&state) = initial_state;
        let _ = started_tx.send(());
        Self {
            inner: Arc::new(OwnerInner {
                state,
                completion,
                completion_finished,
                wake,
                signal_driven,
                cleanup_admission,
                diagnostics,
            }),
        }
    }
}

impl NativeRunOwner {
    /// Completion is independent of command admission and its state mutex.
    /// No blocked producer can prevent the owner from publishing its failure.
    pub(crate) fn ensure_running(&self) -> Result<(), String> {
        if let Some(reason) = &*mutex_lock(&self.inner.completion) {
            return Err(format!("daemon-wide native owner stopped: {reason:?}"));
        }
        let mut state = mutex_lock(&self.inner.state);
        if matches!(&*state, OwnerState::Running { thread, .. } if thread.is_finished()) {
            *state = OwnerState::Failed("daemon-wide native owner stopped".to_owned());
        }
        match &*state {
            OwnerState::Running { .. } => Ok(()),
            OwnerState::Failed(message) => Err(message.clone()),
        }
    }

    pub(crate) fn owner_wake(&self) -> OwnerWake {
        self.inner.wake.clone()
    }

    /// Declare that a process-wide SIGCHLD relay now wakes this owner on child
    /// exits, so it can stop arming the timed backstop sweep and block purely on
    /// the relay between exits. Idempotent. `serve` calls this once, right after
    /// registering the relay and before it services requests; a following
    /// `owner_wake().wake()` (the exec-window catch-up) makes the owner observe
    /// the flag and shed the deadline on its next cycle.
    pub(crate) fn mark_signal_driven(&self) {
        self.inner.signal_driven.store(true, Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) fn cleanup_admission(&self) -> CleanupAdmission {
        self.inner.cleanup_admission.clone()
    }

    pub(crate) fn register(
        &self,
        registration: NativeRunRegistration,
    ) -> Result<(), NativeRegistrationError> {
        if let Err(message) = self.ensure_running() {
            return Err(NativeRegistrationError {
                message,
                registration: Box::new(registration),
            });
        }
        let commands = {
            let state = mutex_lock(&self.inner.state);
            match &*state {
                OwnerState::Running { commands, .. } => commands.clone(),
                OwnerState::Failed(message) => {
                    return Err(NativeRegistrationError {
                        message: message.clone(),
                        registration: Box::new(registration),
                    });
                }
            }
        };
        // Wake before a potentially blocking channel send: a full command
        // queue must not leave its only consumer asleep in poll.
        self.inner.wake.wake();
        let result = commands.send(OwnerCommand::Register(registration));
        self.inner.wake.wake();
        result.map_err(|error| NativeRegistrationError {
            message: "daemon-wide native owner stopped before registration".to_owned(),
            registration: Box::new(match error.0 {
                OwnerCommand::Register(registration) => registration,
                _ => unreachable!("registration send returns its registration"),
            }),
        })
    }

    /// Return the live pty master fd and child pid for every watched native
    /// Run, relinquishing the owner's reap/close authority for each so the
    /// child survives (unreaped) and its master fd stays open past a future
    /// exec-in-place. Called on the shipped SIGHUP exec-in-place path from
    /// `perform_exec_upgrade`, past the point of no return.
    #[cfg(test)]
    pub(crate) fn extract_for_handoff(&self) -> Result<Vec<LiveDescriptors>, String> {
        self.extract_for_handoff_after_preflight(Box::new(|_| Ok(())))
    }

    pub(crate) fn extract_for_handoff_after_preflight(
        &self,
        preflight: HandoffPreflight,
    ) -> Result<Vec<LiveDescriptors>, String> {
        self.ensure_running()?;
        let commands = {
            let state = mutex_lock(&self.inner.state);
            match &*state {
                OwnerState::Running { commands, .. } => commands.clone(),
                OwnerState::Failed(message) => return Err(message.clone()),
            }
        };
        let (tx, rx) = mpsc::channel();
        self.inner.wake.wake();
        if commands
            .send(OwnerCommand::ExtractForHandoff {
                preflight,
                respond: tx,
            })
            .is_err()
        {
            return Err("daemon-wide native owner stopped before handoff extraction".to_owned());
        }
        self.inner.wake.wake();
        drop(commands);
        rx.recv().map_err(|_| {
            "daemon-wide native owner stopped without a handoff extraction result".to_owned()
        })?
    }

    pub(crate) fn handoff_ready(&self, run_id: RunId) -> Result<bool, String> {
        self.ensure_running()?;
        let commands = {
            let state = mutex_lock(&self.inner.state);
            match &*state {
                OwnerState::Running { commands, .. } => commands.clone(),
                OwnerState::Failed(message) => return Err(message.clone()),
            }
        };
        let (tx, rx) = mpsc::channel();
        self.inner.wake.wake();
        commands
            .send(OwnerCommand::HandoffReady {
                run_id,
                respond: tx,
            })
            .map_err(|_| "daemon-wide native owner stopped before handoff probe".to_owned())?;
        self.inner.wake.wake();
        drop(commands);
        rx.recv()
            .map_err(|_| "daemon-wide native owner dropped the handoff probe".to_owned())
    }

    #[cfg(test)]
    pub(crate) fn register_for_test(
        &self,
        run: &Arc<Run>,
        child: Box<dyn Child + Send + Sync>,
        session: NativeSession,
        control: NativeControlOwner,
        wait_failure: NativeWaitFailure,
        after_wait: impl FnOnce() + Send + 'static,
    ) -> Result<(), NativeRegistrationError> {
        self.register_for_test_with_reader(
            run,
            File::open("/dev/null").expect("open test native output EOF"),
            child,
            session,
            control,
            wait_failure,
            after_wait,
        )
    }

    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn register_for_test_with_reader(
        &self,
        run: &Arc<Run>,
        reader: File,
        child: Box<dyn Child + Send + Sync>,
        session: NativeSession,
        control: NativeControlOwner,
        wait_failure: NativeWaitFailure,
        after_wait: impl FnOnce() + Send + 'static,
    ) -> Result<(), NativeRegistrationError> {
        let mut child = PendingChild::new(child);
        child.bind_reap_control(control.clone());
        let stats = crate::qualification_stats::QualificationStats::default();
        self.register(NativeRunRegistration::new(
            run,
            reader,
            child,
            session,
            control,
            wait_failure,
            after_wait,
            stats.guard(crate::qualification_stats::Gauge::Readers),
            stats.guard(crate::qualification_stats::Gauge::Waiters),
        ))
    }

    #[cfg(test)]
    pub(crate) fn shutdown(&self, deadline: Instant) -> Result<(), String> {
        let commands = {
            let state = mutex_lock(&self.inner.state);
            let OwnerState::Running { commands, .. } = &*state else {
                return Ok(());
            };
            commands.clone()
        };
        self.inner.wake.wake();
        let _ = commands.send(OwnerCommand::Shutdown);
        self.inner.wake.wake();
        drop(commands);
        while !self.inner.completion_finished.load(Ordering::Acquire) && Instant::now() < deadline {
            thread::sleep(
                Duration::from_millis(1).min(deadline.saturating_duration_since(Instant::now())),
            );
        }
        if self.inner.completion_finished.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err("timed out waiting for daemon-wide native owner shutdown".to_owned())
        }
    }

    #[cfg(test)]
    pub(crate) fn diagnostic_snapshot(&self) -> OwnerDiagnosticSnapshot {
        OwnerDiagnosticSnapshot {
            poll_returns: self.inner.diagnostics.poll_returns.load(Ordering::Acquire),
            lifecycle_probes: self
                .inner
                .diagnostics
                .lifecycle_probes
                .load(Ordering::Acquire),
            registrations: self.inner.diagnostics.registrations.load(Ordering::Acquire),
        }
    }

    #[cfg(test)]
    pub(crate) fn fail_next_worker_spawn(&self) {
        self.inner
            .diagnostics
            .fail_next_worker_spawn
            .store(1, Ordering::Release);
        self.inner.wake.wake();
    }
}

impl Drop for OwnerInner {
    fn drop(&mut self) {
        let previous = {
            let mut state = mutex_lock(&self.state);
            std::mem::replace(
                &mut *state,
                OwnerState::Failed("native owner stopped".to_owned()),
            )
        };
        let OwnerState::Running { commands, thread } = previous else {
            return;
        };
        self.wake.wake();
        let _ = commands.send(OwnerCommand::Shutdown);
        self.wake.wake();
        drop(commands);
        if thread.is_finished() {
            let _ = thread.join();
        } else {
            drop(thread);
        }
    }
}

pub(crate) const fn resident_runtime_owner_bytes() -> usize {
    // Known heap payload of the shared child authority plus Arc's strong and
    // weak reference counters, in bytes. Opaque child/allocator costs are
    // separately qualified as host RSS, never hidden by this static charge.
    std::mem::size_of::<NativeEntry>()
        + std::mem::size_of::<Mutex<Option<Box<dyn Child + Send + Sync>>>>()
        + std::mem::size_of::<[AtomicUsize; 2]>()
}

struct NativeEntry {
    run_id: RunId,
    run: Weak<Run>,
    control: NativeControlOwner,
    // Remains outside the owner unwind scope even while lifecycle work moves.
    _child_authority: NativeChildAuthority,
    output: Option<OutputOwner>,
    lifecycle: Lifecycle,
    after_wait: Option<AfterWait>,
    wait_failure: NativeWaitFailure,
    terminal: Option<PendingTerminal>,
}

struct OutputOwner {
    reader: File,
    paused: bool,
    pending_offer: bool,
    _control: NativeControlOwner,
    _guard: GaugeGuard,
}

enum Lifecycle {
    Watching(Watching),
    WaitingCleanup(WaitingCleanup),
    Queued,
    Cleaning,
    Finalizing,
    AuthorityLost(NativeControlOwner),
    Done,
}

/// The Entry and a transient lifecycle/cleanup job share the actual child
/// holder. Moving Watching cannot make an unwind call portable `Child::drop`.
#[derive(Clone)]
struct NativeChildAuthority(Arc<Mutex<Option<Box<dyn Child + Send + Sync>>>>);

impl NativeChildAuthority {
    fn process_id(&self) -> Option<u32> {
        mutex_lock(&self.0)
            .as_ref()
            .and_then(|child| child.process_id())
    }

    fn with_mut<R>(&self, operation: impl FnOnce(&mut (dyn Child + Send + Sync)) -> R) -> R {
        let mut child = mutex_lock(&self.0);
        operation(child.as_mut().expect("one owned Native child").as_mut())
    }

    fn into_child(self) -> Box<dyn Child + Send + Sync> {
        mutex_lock(&self.0)
            .take()
            .expect("Native child authority transferred once")
    }
}

struct Watching {
    child: NativeChildAuthority,
    pending_stop: Option<PendingStopAdmission>,
    session: NativeSession,
    control: NativeControlOwner,
    wait_failure: NativeWaitFailure,
    _guard: GaugeGuard,
}

struct PendingStopAdmission {
    reply: tokio::sync::oneshot::Sender<StopOwnerResult>,
    deadline: Instant,
}

struct PendingTerminal {
    state: RunState,
    deadline: Instant,
}

enum CleanupKind {
    Stop(tokio::sync::oneshot::Sender<StopOwnerResult>),
    Unpublished,
    Natural {
        stop: Option<tokio::sync::oneshot::Sender<StopOwnerResult>>,
    },
}

struct CleanupJob {
    run_id: RunId,
    watching: Watching,
    kind: CleanupKind,
    after_wait: Option<AfterWait>,
    _permit: CleanupPermit,
}

struct WaitingCleanup {
    watching: Watching,
    kind: CleanupKind,
    after_wait: Option<AfterWait>,
}

enum WorkerJob {
    Cleanup(CleanupJob),
    Finalize(FinalizeJob),
}

struct FinalizeJob {
    run_id: RunId,
    run: Arc<Run>,
    state: RunState,
    wait_failure: NativeWaitFailure,
}

struct CleanupCompletion {
    job_id: u64,
    run_id: RunId,
    outcome: WorkerOutcome,
}

enum WorkerOutcome {
    Cleanup(CleanupOutcome),
    Finalized,
}

enum CleanupOutcome {
    Terminal {
        state: RunState,
    },
    Resume {
        watching: Watching,
        after_wait: Option<AfterWait>,
    },
    AuthorityLost {
        control: NativeControlOwner,
    },
}

fn stop_admission_failure(run_id: RunId, detail: &str) -> ControlFailure {
    ControlFailure {
        error: ProtocolError::new(
            ErrorCode::ControlBackpressure,
            format!("Run {run_id} cannot stop: {detail}"),
        ),
        disposition: CommandDisposition::NotApplied,
        confirmed_input_bytes: None,
    }
}

#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "one owner loop keeps actual authority, completion watch, and panic containment in the same auditable scope"
)]
fn owner_main(
    commands: &mpsc::Receiver<OwnerCommand>,
    mut wake_reader: UnixStream,
    wake: &OwnerWake,
    cleanup_admission: &CleanupAdmission,
    diagnostics: &OwnerDiagnostics,
    signal_driven: &AtomicBool,
    resources: crate::ResourceLimits,
    owner_completion: &Mutex<Option<NativeServiceFailure>>,
    owner_state: &Mutex<OwnerState>,
    owner_finished: &AtomicBool,
) {
    let (completion_tx, completion_rx) = mpsc::channel();
    let mut entries = Vec::<NativeEntry>::new();
    let mut queued = VecDeque::<WorkerJob>::new();
    let mut active = HashMap::<u64, thread::JoinHandle<()>>::new();
    let mut active_cleanups = 0_usize;
    let mut active_finalizers = 0_usize;
    let mut next_job_id = 0_u64;
    // Start "woken" so the first iteration drains the registrations that raced
    // the thread's startup. Thereafter a pass runs only on a real edge: the wake
    // pipe fired (a command, a worker completion, a registration, or the
    // process-wide SIGCHLD relay in `serve`), or an armed poll deadline came due
    // (a pending Stop's admission window, or — while no relay is attached — the
    // timed backstop). Once `serve` marks the owner signal-driven there is no
    // free-running 20 ms tick, so an idle watched Run costs no wakeups at all,
    // which is the whole point of replacing the timed `waitid` sweep with SIGCHLD
    // readiness.
    let mut owner_woken = true;
    let mut fair_start = 0_usize;

    // Entries and admitted cleanup jobs stay outside the unwind scope. A
    // failed derived operation must not drop a child or its PTY authority.
    let result = crate::diagnostics::catch_native_unwind(AssertUnwindSafe(|| {
        loop {
            if owner_woken {
                if drain_commands(commands, &mut entries, diagnostics) {
                    detach_active_workers(&mut active);
                    drain_completions(
                        &completion_rx,
                        &mut entries,
                        &mut active,
                        &mut active_cleanups,
                        &mut active_finalizers,
                    );
                    return;
                }
                drain_completions(
                    &completion_rx,
                    &mut entries,
                    &mut active,
                    &mut active_cleanups,
                    &mut active_finalizers,
                );
                // Peek every watched leader on each edge. SIGCHLD is process-wide and
                // carries no pid we consult, so an exit signal means only "some
                // watched child may now be terminal" — hence the whole set is
                // re-peeked. Coalescing a burst of exits into one edge is therefore
                // correct, not a bug: the single sweep observes every leader that
                // turned terminal. The peek is a non-reaping `waitid(WNOWAIT)` (see
                // `leader_is_terminal`), so running it on a command or completion edge
                // too is idempotent and never consumes an exit status ahead of the
                // sequenced `reap_leader`.
                diagnostics.lifecycle_probes.fetch_add(1, Ordering::AcqRel);
                drive_lifecycle(&mut entries, &mut queued, cleanup_admission);
            }
            start_worker_jobs(
                &mut queued,
                &mut active,
                &mut active_cleanups,
                &mut active_finalizers,
                &completion_tx,
                wake,
                diagnostics,
                &mut entries,
                &mut next_job_id,
                resources,
            );
            queue_ready_terminals(&mut entries, &mut queued);
            start_worker_jobs(
                &mut queued,
                &mut active,
                &mut active_cleanups,
                &mut active_finalizers,
                &completion_tx,
                wake,
                diagnostics,
                &mut entries,
                &mut next_job_id,
                resources,
            );
            let mut closed_runs = Vec::new();
            entries.retain(|entry| {
                let retained = entry.output.is_some()
                    || !matches!(
                        entry.lifecycle,
                        Lifecycle::Done | Lifecycle::AuthorityLost(_)
                    )
                    || entry.terminal.is_some();
                if !retained {
                    closed_runs.push(entry.run.clone());
                }
                retained
            });
            // Entry Drop releases the actual reader/control holders first.
            // Cleanup cannot pass the strong-count fence before that boundary,
            // and public metadata reads must not be its implicit retry lane.
            for weak in closed_runs {
                if let Some(run) = weak.upgrade() {
                    run.native_entry_retired();
                }
            }
            owner_woken = match poll_and_read_outputs(
                &mut entries,
                &mut wake_reader,
                signal_driven,
                diagnostics,
                resources,
                &mut fair_start,
            ) {
                Ok(woken) => woken,
                Err(()) => return,
            };
        }
    }));
    let reason = if result.is_err() {
        NativeServiceFailure::OwnerUnwound
    } else {
        NativeServiceFailure::OwnerStopped
    };
    *mutex_lock(owner_completion) = Some(reason);
    // Remove the sole retained Sender before draining. Every producer borrowed
    // its own Sender without holding this mutex; blocked sends now finish as
    // we drain, and disconnect proves no accepted Register can still race Drop.
    *mutex_lock(owner_state) =
        OwnerState::Failed(format!("daemon-wide native owner stopped: {reason:?}"));
    // Producer lifetime is not service lifetime. Publish the retained failure
    // and settle commands before waiting for borrowed senders to retire.
    for entry in &mut entries {
        complete_stopped_entry(entry, reason);
    }
    // Accepted registrations retain authority even if their producer sent only
    // after completion. Fence each arrival before waiting for another command.
    while let Ok(command) = commands.recv() {
        match command {
            OwnerCommand::Register(registration) => {
                let mut entry = registration.into_entry();
                complete_stopped_entry(&mut entry, reason);
                entries.push(entry);
            }
            OwnerCommand::HandoffReady { respond, .. } => {
                drop(respond);
            }
            OwnerCommand::ExtractForHandoff { respond, .. } => {
                let _ = respond.send(Err(
                    "native owner stopped before handoff extraction".to_owned()
                ));
            }
            OwnerCommand::Shutdown => {}
            #[cfg(test)]
            OwnerCommand::UnwindForTest => {}
            #[cfg(test)]
            OwnerCommand::ProbePendingStopForTest { respond, .. } => {
                let _ = respond.send(false);
            }
        }
    }
    preserve_shutdown_authority(&mut entries, &mut queued);
    owner_finished.store(true, Ordering::Release);
}

fn complete_stopped_entry(entry: &mut NativeEntry, reason: NativeServiceFailure) {
    if let Lifecycle::Watching(watching) = &mut entry.lifecycle
        && let Some(pending) = watching.pending_stop.take()
    {
        let _ = pending
            .reply
            .send(StopOwnerResult::Rejected(ControlFailure {
                error: ProtocolError::new(
                    ErrorCode::BackendUnavailable,
                    format!(
                        "Run {} Native owner stopped before Stop admission",
                        entry.run_id
                    ),
                ),
                disposition: CommandDisposition::NotApplied,
                confirmed_input_bytes: None,
            }));
    }
    if let Some(run) = entry.run.upgrade() {
        run.native_owner_stopped(reason);
    }
    entry.control.fence_owner_loss(reason);
}

fn drain_commands(
    commands: &mpsc::Receiver<OwnerCommand>,
    entries: &mut Vec<NativeEntry>,
    diagnostics: &OwnerDiagnostics,
) -> bool {
    loop {
        match commands.try_recv() {
            Ok(OwnerCommand::Register(registration)) => {
                let entry = registration.into_entry();
                if let Some(run) = entry.run.upgrade() {
                    run.native_owner_ready();
                }
                entries.push(entry);
                diagnostics.registrations.fetch_add(1, Ordering::AcqRel);
            }
            Ok(OwnerCommand::HandoffReady { run_id, respond }) => {
                let ready = entries
                    .iter()
                    .find(|entry| entry.run_id == run_id)
                    .is_none_or(|entry| matches!(entry.lifecycle, Lifecycle::Watching(_)));
                let _ = respond.send(ready);
            }
            Ok(OwnerCommand::ExtractForHandoff { preflight, respond }) => {
                let _ = respond.send(extract_live_descriptors(entries, preflight));
            }
            Ok(OwnerCommand::Shutdown) | Err(mpsc::TryRecvError::Disconnected) => return true,
            #[cfg(test)]
            Ok(OwnerCommand::UnwindForTest) => panic!("Native owner completion probe"),
            #[cfg(test)]
            Ok(OwnerCommand::ProbePendingStopForTest { run_id, respond }) => {
                let pending = entries.iter().any(|entry| {
                    entry.run_id == run_id
                        && matches!(&entry.lifecycle, Lifecycle::Watching(watching)
                        if watching.pending_stop.is_some())
                });
                let _ = respond.send(pending);
            }
            Err(mpsc::TryRecvError::Empty) => return false,
        }
    }
}

/// Collect the live pty master fd + child pid for every watched Run and
/// relinquish the owner's reap/close authority so both survive a future exec.
/// Mirrors `retain_unwaited_child`'s authority discipline (`mem::forget` the
/// control), but handoff is not a failure: no `wait_failure.record` and no
/// `mark_wait_authority_lost` — just forget child and control so each lives on.
fn extract_live_descriptors(
    entries: &mut [NativeEntry],
    preflight: HandoffPreflight,
) -> Result<Vec<LiveDescriptors>, String> {
    // Validate every entry before relinquishing the first owner. A terminal
    // publication or cleanup already in flight is allowed to finish in the old
    // image; this SIGHUP attempt aborts and can be retried. Silently skipping it
    // would reconcile a still-owned child as historical in the incoming image.
    let descriptors = entries
        .iter()
        .map(|entry| {
            let Lifecycle::Watching(watching) = &entry.lifecycle else {
                return Err(format!(
                    "Run {} native lifecycle is crossing terminal cleanup",
                    entry.run_id
                ));
            };
            let master_fd = watching.control.master_raw_fd().ok_or_else(|| {
                format!("Run {} has no live PTY master for handoff", entry.run_id)
            })?;
            let child_pid = watching
                .child
                .process_id()
                .ok_or_else(|| format!("Run {} has no live child pid for handoff", entry.run_id))?;
            let input_state = watching.control.handoff_input_state()?;
            Ok(LiveDescriptors {
                run_id: entry.run_id,
                child_pid,
                master_fd,
                input_state,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;

    preflight(&descriptors)?;
    for entry in entries {
        if let Some(run) = entry.run.upgrade() {
            run.native_owner_draining();
        }
        let Lifecycle::Watching(watching) =
            std::mem::replace(&mut entry.lifecycle, Lifecycle::Done)
        else {
            unreachable!("lifecycle was Watching under the same borrow")
        };
        // Retain both across the future exec: forgetting the child stops
        // portable_pty's `Child::drop` from reaping/killing it, and forgetting
        // the control keeps an `Arc<NativeControlInner>` clone alive forever so
        // the pty master box is never dropped and the master fd stays open.
        std::mem::forget(watching.child);
        std::mem::forget(watching.control);
        // Stop the owner from reading this master after extract. `entry.output`
        // holds a CLOEXEC *dup* of the master (created at spawn via
        // `duplicate_cloexec`), so dropping it closes ONLY the reader-side dup —
        // the master handed off in the manifest stays open via the forgotten
        // control Arc above. With the reader gone and lifecycle now `Done`, the
        // owner-loop retain predicate drops this entry, so no further `Append`
        // can be enqueued for it. The durable barrier taken after extract then
        // covers every byte ever read (f04), and unread kernel-buffer bytes
        // remain for the incoming image to read starting at the persisted
        // cursor.
        entry.output = None;
    }
    Ok(descriptors)
}

#[allow(
    clippy::too_many_lines,
    reason = "one production state transition keeps command, natural-exit, and cleanup-admission ordering auditable"
)]
fn drive_lifecycle(
    entries: &mut [NativeEntry],
    queued: &mut VecDeque<WorkerJob>,
    cleanup_admission: &CleanupAdmission,
) {
    // One syscall for the whole pass: if no child of this process has exited,
    // no watched leader can be terminal, and the per-Run peeks below are all
    // going to answer `false`. Most passes are like that -- every command edge
    // runs one, and a Stop command is delivered on one -- so this replaces N
    // peeks with one. See `NativeSession::any_child_exited`.
    //
    // Read once, and deliberately not refreshed inside the loop. Both ways it
    // can go stale during a pass are safe:
    //
    // * stale `true` (a worker reaped the exit that opened the gate) just runs
    //   the per-Run peeks, which is exactly the behaviour without this gate;
    // * stale `false` would be the dangerous one -- an exit arriving after the
    //   read -- but that exit raises SIGCHLD, whose relay writes a self-pipe
    //   byte, so the owner runs another pass with a fresh gate. The byte is
    //   buffered rather than edge-triggered, so it cannot be lost by arriving
    //   mid-pass.
    let any_child_exited = NativeSession::any_child_exited();
    'entries: for entry in entries {
        let lifecycle = std::mem::replace(&mut entry.lifecycle, Lifecycle::Queued);
        let mut watching = match lifecycle {
            Lifecycle::WaitingCleanup(waiting) => {
                let Some(permit) = cleanup_admission.try_acquire() else {
                    entry.lifecycle = Lifecycle::WaitingCleanup(waiting);
                    continue;
                };
                queued.push_back(WorkerJob::Cleanup(CleanupJob {
                    run_id: waiting.watching.control.run_id(),
                    watching: waiting.watching,
                    kind: waiting.kind,
                    after_wait: waiting.after_wait,
                    _permit: permit,
                }));
                continue;
            }
            Lifecycle::Watching(watching) => watching,
            other => {
                entry.lifecycle = other;
                continue;
            }
        };
        let run_id = watching.control.run_id();
        let control = watching.control.clone();
        let Some(mut turn) = control.try_turn() else {
            entry.lifecycle = Lifecycle::Watching(watching);
            continue;
        };
        let mut cleanup = None::<(CleanupKind, CleanupPermit)>;
        for command in turn.drain_child_commands() {
            match command {
                #[cfg(not(target_os = "macos"))]
                ChildCommand::Signal {
                    signal: ctxmux_protocol::RunSignal::Interrupt,
                    foreground_group,
                    reply,
                } => {
                    let _ = reply.send(watching.session.interrupt(foreground_group));
                }
                ChildCommand::Stop { reply, deadline } => {
                    if watching.pending_stop.is_none() {
                        watching.pending_stop = Some(PendingStopAdmission { reply, deadline });
                    } else {
                        let _ = reply.send(StopOwnerResult::Rejected(stop_admission_failure(
                            run_id,
                            "multiple Stop commands crossed one native owner fence",
                        )));
                    }
                }
                ChildCommand::CleanupUnpublished => {
                    if cleanup.is_none() {
                        let Some(permit) = cleanup_admission.try_acquire() else {
                            entry.lifecycle = Lifecycle::WaitingCleanup(WaitingCleanup {
                                watching,
                                kind: CleanupKind::Unpublished,
                                after_wait: entry.after_wait.take(),
                            });
                            continue 'entries;
                        };
                        cleanup = Some((CleanupKind::Unpublished, permit));
                    }
                }
            }
        }
        if let Some((kind, permit)) = cleanup {
            queued.push_back(WorkerJob::Cleanup(CleanupJob {
                run_id,
                watching,
                kind,
                after_wait: entry.after_wait.take(),
                _permit: permit,
            }));
            continue;
        }

        if watching
            .pending_stop
            .as_ref()
            .is_some_and(|pending| Instant::now() >= pending.deadline)
        {
            let pending = watching
                .pending_stop
                .take()
                .expect("expired pending Stop remains present");
            turn.reject_pending_stop();
            let _ = pending
                .reply
                .send(StopOwnerResult::Rejected(stop_admission_failure(
                    run_id,
                    "native Stop admission deadline elapsed before an owner turn could commit Stop",
                )));
        }
        if watching.pending_stop.is_some()
            && let Some(permit) = cleanup_admission.try_acquire()
        {
            let pending = watching
                .pending_stop
                .take()
                .expect("pending Stop remains present before commit");
            match turn.commit_pending_stop() {
                Ok(()) => {
                    queued.push_back(WorkerJob::Cleanup(CleanupJob {
                        run_id,
                        watching,
                        kind: CleanupKind::Stop(pending.reply),
                        after_wait: entry.after_wait.take(),
                        _permit: permit,
                    }));
                    continue;
                }
                Err(failure) => {
                    let _ = pending.reply.send(StopOwnerResult::Rejected(failure));
                }
            }
        }

        // Peek the leader's terminal state without reaping. Reached on every
        // owner edge now that the timed sweep is gone; a SIGCHLD edge is what
        // makes this observe a fresh exit, but a command/completion edge peeks
        // just as safely (idempotent WNOWAIT). The pass-wide gate skips the
        // syscall entirely when the kernel says no child has exited at all.
        // Child observation owns no control mutation. Release the admission
        // guard before the kernel probe; commands accepted across this edge
        // are fenced below before the actual child is transferred to cleanup.
        drop(turn);
        match watching.session.leader_is_terminal_gated(any_child_exited) {
            Ok(false) => entry.lifecycle = Lifecycle::Watching(watching),
            Ok(true) => {
                let Some(mut turn) = control.try_turn() else {
                    entry.lifecycle = Lifecycle::Watching(watching);
                    continue;
                };
                let Some(permit) = cleanup_admission.try_acquire() else {
                    entry.lifecycle = Lifecycle::Watching(watching);
                    continue;
                };
                let pending = turn.fence_child_commands();
                let mut stop = watching.pending_stop.take().map(|pending| pending.reply);
                for command in pending {
                    match command {
                        #[cfg(not(target_os = "macos"))]
                        ChildCommand::Signal { reply, .. } => {
                            let _ = reply.send(Err(
                                "native session leader exited before interrupt".to_owned(),
                            ));
                        }
                        ChildCommand::Stop { reply, deadline: _ } => {
                            if let Some(previous) = stop.replace(reply) {
                                let _ = previous.send(StopOwnerResult::Rejected(
                                    stop_admission_failure(
                                        run_id,
                                        "multiple Stop commands crossed one native owner fence",
                                    ),
                                ));
                            }
                        }
                        ChildCommand::CleanupUnpublished => {}
                    }
                }
                queued.push_back(WorkerJob::Cleanup(CleanupJob {
                    run_id,
                    watching,
                    kind: CleanupKind::Natural { stop },
                    after_wait: entry.after_wait.take(),
                    _permit: permit,
                }));
            }
            Err(error) => {
                watching
                    .control
                    .mark_wait_authority_lost(error.clone(), watching.child.into_child());
                watching.wait_failure.record(run_id, &error);
                entry.after_wait.take();
                entry.lifecycle = Lifecycle::AuthorityLost(watching.control);
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn start_worker_jobs(
    queued: &mut VecDeque<WorkerJob>,
    active: &mut HashMap<u64, thread::JoinHandle<()>>,
    active_cleanups: &mut usize,
    active_finalizers: &mut usize,
    completion_tx: &mpsc::Sender<CleanupCompletion>,
    wake: &OwnerWake,
    diagnostics: &OwnerDiagnostics,
    entries: &mut [NativeEntry],
    next_job_id: &mut u64,
    resources: crate::ResourceLimits,
) {
    while let Some(index) = queued.iter().position(|job| match job {
        WorkerJob::Cleanup(_) => *active_cleanups < resources.cleanup_workers,
        WorkerJob::Finalize(_) => *active_finalizers < resources.finalize_workers,
    }) {
        let job = queued
            .remove(index)
            .expect("eligible native worker job remains queued");
        *next_job_id = next_job_id.checked_add(1).expect("cleanup job id overflow");
        let job_id = *next_job_id;
        let run_id = match &job {
            WorkerJob::Cleanup(job) => job.run_id,
            WorkerJob::Finalize(job) => job.run_id,
        };
        let worker_lifecycle = match &job {
            WorkerJob::Cleanup(_) => Lifecycle::Cleaning,
            WorkerJob::Finalize(_) => Lifecycle::Finalizing,
        };
        let holder = Arc::new(Mutex::new(Some(job)));
        let worker_holder = Arc::clone(&holder);
        let completion_tx = completion_tx.clone();
        let completion_wake = wake.clone();
        let fail_spawn = diagnostics
            .fail_next_worker_spawn
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok();
        let spawn = if fail_spawn {
            Err(io::Error::other("injected native worker spawn failure"))
        } else {
            thread::Builder::new()
                .name("ctxmux-native-blocking".to_owned())
                .spawn(move || {
                    let job = mutex_lock(&worker_holder)
                        .take()
                        .expect("cleanup worker takes exactly one job");
                    let outcome = match job {
                        WorkerJob::Cleanup(job) => WorkerOutcome::Cleanup(execute_cleanup(job)),
                        WorkerJob::Finalize(job) => {
                            job.run.publish_terminal(job.state);
                            WorkerOutcome::Finalized
                        }
                    };
                    let _ = completion_tx.send(CleanupCompletion {
                        job_id,
                        run_id,
                        outcome,
                    });
                    completion_wake.wake();
                })
        };
        match spawn {
            Ok(handle) => {
                let previous = active.insert(job_id, handle);
                debug_assert!(previous.is_none());
                match &worker_lifecycle {
                    Lifecycle::Cleaning => *active_cleanups += 1,
                    Lifecycle::Finalizing => *active_finalizers += 1,
                    _ => unreachable!("native worker job has a lifecycle owner"),
                }
                set_lifecycle(entries, run_id, worker_lifecycle);
            }
            Err(error) => {
                let job = mutex_lock(&holder)
                    .take()
                    .expect("failed spawn leaves cleanup job with caller");
                match job {
                    WorkerJob::Cleanup(job) => {
                        let outcome = fail_cleanup_spawn(job, &error);
                        apply_cleanup_outcome(entries, run_id, outcome);
                    }
                    WorkerJob::Finalize(job) => {
                        job.wait_failure.record(
                            run_id,
                            &format!("failed to start native terminal finalizer: {error}"),
                        );
                        set_lifecycle(entries, run_id, Lifecycle::Done);
                    }
                }
            }
        }
    }
}

fn execute_cleanup(mut job: CleanupJob) -> CleanupOutcome {
    let result = job.watching.child.with_mut(|child| match &mut job.kind {
        CleanupKind::Stop(_) | CleanupKind::Unpublished => {
            job.watching
                .session
                .stop(child, STOP_GRACEFUL_TIMEOUT, STOP_FORCED_TIMEOUT)
        }
        CleanupKind::Natural { .. } => job
            .watching
            .session
            .finish_after_direct_exit(child, Instant::now() + STOP_FORCED_TIMEOUT)
            .map(|(status, disposition)| (disposition, status)),
    });

    match result {
        Ok((disposition, status)) => {
            job.watching.control.mark_reaped();
            match job.kind {
                CleanupKind::Stop(reply) => {
                    let _ = reply.send(StopOwnerResult::Accepted(disposition));
                }
                CleanupKind::Natural { stop } => {
                    if let Some(reply) = stop {
                        let _ = reply.send(StopOwnerResult::Accepted(disposition));
                    }
                }
                CleanupKind::Unpublished => {}
            }
            job.watching.control.mark_closed();
            if let Some(after_wait) = job.after_wait.take() {
                after_wait();
            }
            CleanupOutcome::Terminal {
                state: exit_state(&status),
            }
        }
        Err(error) => match job.kind {
            CleanupKind::Natural { stop } => {
                if let Some(reply) = stop {
                    let _ = reply.send(StopOwnerResult::Unknown(error.clone()));
                }
                job.watching
                    .control
                    .mark_wait_authority_lost(error.clone(), job.watching.child.into_child());
                job.watching.wait_failure.record(job.run_id, &error);
                CleanupOutcome::AuthorityLost {
                    control: job.watching.control,
                }
            }
            CleanupKind::Stop(reply) => {
                let _ = reply.send(StopOwnerResult::Unknown(error));
                CleanupOutcome::Resume {
                    watching: job.watching,
                    after_wait: job.after_wait,
                }
            }
            CleanupKind::Unpublished => {
                job.watching.control.record_cleanup_error(format!(
                    "failed to stop unpublished Run session: {error}"
                ));
                CleanupOutcome::Resume {
                    watching: job.watching,
                    after_wait: job.after_wait,
                }
            }
        },
    }
}

fn fail_cleanup_spawn(mut job: CleanupJob, error: &io::Error) -> CleanupOutcome {
    let message = format!("failed to start bounded native cleanup owner: {error}");
    match job.kind {
        CleanupKind::Stop(reply) => {
            let _ = reply.send(StopOwnerResult::Unknown(message.clone()));
        }
        CleanupKind::Natural { stop } => {
            if let Some(reply) = stop {
                let _ = reply.send(StopOwnerResult::Unknown(message.clone()));
            }
        }
        CleanupKind::Unpublished => job.watching.control.record_cleanup_error(message.clone()),
    }
    job.watching
        .control
        .mark_wait_authority_lost(message.clone(), job.watching.child.into_child());
    job.watching.wait_failure.record(job.run_id, &message);
    job.after_wait.take();
    CleanupOutcome::AuthorityLost {
        control: job.watching.control,
    }
}

fn drain_completions(
    completions: &mpsc::Receiver<CleanupCompletion>,
    entries: &mut [NativeEntry],
    active: &mut HashMap<u64, thread::JoinHandle<()>>,
    active_cleanups: &mut usize,
    active_finalizers: &mut usize,
) {
    while let Ok(completion) = completions.try_recv() {
        if let Some(worker) = active.remove(&completion.job_id) {
            let _ = worker.join();
        }
        match &completion.outcome {
            WorkerOutcome::Cleanup(_) => {
                *active_cleanups = active_cleanups
                    .checked_sub(1)
                    .expect("native cleanup worker count cannot underflow");
            }
            WorkerOutcome::Finalized => {
                *active_finalizers = active_finalizers
                    .checked_sub(1)
                    .expect("native finalizer worker count cannot underflow");
            }
        }
        match completion.outcome {
            WorkerOutcome::Cleanup(outcome) => {
                apply_cleanup_outcome(entries, completion.run_id, outcome);
            }
            WorkerOutcome::Finalized => {
                set_lifecycle(entries, completion.run_id, Lifecycle::Done);
            }
        }
    }
}

fn apply_cleanup_outcome(entries: &mut [NativeEntry], run_id: RunId, outcome: CleanupOutcome) {
    let Some(entry) = entries.iter_mut().find(|entry| entry.run_id == run_id) else {
        return;
    };
    match outcome {
        CleanupOutcome::Terminal { state } => {
            entry.lifecycle = Lifecycle::Done;
            entry.terminal = Some(PendingTerminal {
                state,
                deadline: Instant::now() + OUTPUT_DRAIN_TIMEOUT,
            });
        }
        CleanupOutcome::Resume {
            watching,
            after_wait,
        } => {
            entry.lifecycle = Lifecycle::Watching(watching);
            entry.after_wait = after_wait;
        }
        CleanupOutcome::AuthorityLost { control } => {
            entry.lifecycle = Lifecycle::AuthorityLost(control);
        }
    }
}

fn set_lifecycle(entries: &mut [NativeEntry], run_id: RunId, lifecycle: Lifecycle) {
    if let Some(entry) = entries.iter_mut().find(|entry| entry.run_id == run_id) {
        entry.lifecycle = lifecycle;
    }
}

fn queue_ready_terminals(entries: &mut [NativeEntry], queued: &mut VecDeque<WorkerJob>) {
    for entry in entries {
        let drain_expired = entry
            .terminal
            .as_ref()
            .is_some_and(|terminal| Instant::now() >= terminal.deadline);
        let ready = entry.output.is_none() || drain_expired;
        if !ready {
            continue;
        }
        if drain_expired && let Some(output) = &entry.output {
            // This is an idle-writer deadline, never a deadline for consuming a
            // finite unread tail. A ready PTY or paused durable reader retains
            // its output owner and gets another drain turn.
            let mut readiness = [PollFd::new(&output.reader, PollFlags::IN)];
            let ready_now =
                poll(&mut readiness, Some(&Timespec::default())).is_ok_and(|count| count > 0);
            let Some(run) = entry.run.upgrade() else {
                continue;
            };
            let Some(mut turn) = run.try_output_turn() else {
                // Busy is not source loss. Only its actual guard release can
                // wake this reader; do not publish Gap or drop unread bytes.
                if let Some(terminal) = entry.terminal.as_mut() {
                    terminal.deadline = Instant::now() + OUTPUT_DRAIN_TIMEOUT;
                }
                continue;
            };
            let durable_paused = turn.capacity() == 0;
            if ready_now || durable_paused {
                if let Some(terminal) = entry.terminal.as_mut() {
                    terminal.deadline = Instant::now() + OUTPUT_DRAIN_TIMEOUT;
                }
                continue;
            }
            let latest_output_bytes = turn.mark_source_gap();
            run.publish_event(crate::RunEvent::Gap {
                latest_output_bytes,
            });
            drop(turn);
            entry.output = None;
        }
        let Some(terminal) = entry.terminal.take() else {
            continue;
        };
        if let Some(run) = entry.run.upgrade() {
            queued.push_back(WorkerJob::Finalize(FinalizeJob {
                run_id: entry.run_id,
                run,
                state: terminal.state,
                wait_failure: entry.wait_failure.clone(),
            }));
            entry.lifecycle = Lifecycle::Queued;
        }
    }
}

/// The poll timeout for one owner cycle: the earliest of any pending-Stop
/// admission deadline, any terminal output-drain deadline, and — only while no
/// SIGCHLD relay is attached — a timed backstop that re-peeks watched leaders.
/// `None` blocks until an fd or the wake pipe fires.
///
/// Once `serve` has marked the owner signal-driven, a plain `Watching` entry
/// arms no deadline: its exit arrives as the process-wide SIGCHLD relay poking
/// the wake pipe, and a missed/coalesced signal degrades to "detected on the
/// next wake" (see `owner_main`), never to a stranded Run. That is the steady
/// idle state this change creates — no 50 Hz timer, zero wakeups per idle Run.
///
/// THE BACKSTOP IS NOT A PRODUCTION SAFETY NET. Production correctness rests on
/// two things, neither of which is this timer: (1) the kernel delivers SIGCHLD
/// for every child transition of a child this process owns, and the relay's
/// full-set peek turns one delivery into detection of every pending exit; (2)
/// `serve` fires one catch-up wake after attaching the relay, closing the
/// exec-in-place window in which an adopted child could have become a zombie
/// before the new image registered its handler. The backstop exists solely
/// because unit tests construct owners WITHOUT `serve` (so no relay ever fires):
/// there it restores the pre-change timed detection so those owners still reap a
/// natural exit. A future non-test construction site that forgets to attach a
/// relay likewise stays correct rather than silently stranding Runs — which is
/// why the default is backstop-armed and `serve` opts out, not the reverse.
fn poll_deadline(entries: &[NativeEntry], signal_driven: &AtomicBool) -> Option<Instant> {
    let obligation_deadline = entries
        .iter()
        .flat_map(|entry| {
            let stop_deadline = match &entry.lifecycle {
                Lifecycle::Watching(watching) => watching
                    .pending_stop
                    .as_ref()
                    .map(|pending| pending.deadline),
                _ => None,
            };
            let terminal_deadline = entry.terminal.as_ref().map(|terminal| terminal.deadline);
            [stop_deadline, terminal_deadline]
        })
        .flatten()
        .min();
    // Backstop: while no relay is attached, arm the old 20 ms cadence for any
    // watched Run so a natural exit is still peeked. Suppressed the moment the
    // owner is signal-driven, restoring the `None` idle deadline in production.
    let backstop = (!signal_driven.load(Ordering::Acquire)
        && entries
            .iter()
            .any(|entry| matches!(entry.lifecycle, Lifecycle::Watching(_))))
    .then(|| Instant::now() + CHILD_CONTROL_POLL);
    match (obligation_deadline, backstop) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(deadline), None) | (None, Some(deadline)) => Some(deadline),
        (None, None) => None,
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "one poll turn keeps fd borrowing, pressure admission and read ownership ordered"
)]
fn poll_and_read_outputs(
    entries: &mut [NativeEntry],
    wake_reader: &mut UnixStream,
    signal_driven: &AtomicBool,
    diagnostics: &OwnerDiagnostics,
    resources: crate::ResourceLimits,
    fair_start: &mut usize,
) -> Result<bool, ()> {
    let deadline = poll_deadline(entries, signal_driven);
    let mut input_files = Vec::new();
    for (index, entry) in entries.iter_mut().enumerate() {
        entry
            .control
            .progress_empty_input(resources.input_turn_commands);
        if let Some(file) = entry.control.input_poll_file() {
            input_files.push((index, file));
        }
        if let Some(output) = &mut entry.output
            && (output.paused || output.pending_offer)
            && let Some(run) = entry.run.upgrade()
        {
            if let Some(mut turn) = run.try_output_turn() {
                output.paused = turn.capacity() == 0;
                output.pending_offer = turn.has_unoffered();
            } else {
                output.paused = true;
            }
            run.native_output_pressure(output.paused);
        }
    }
    // Each descriptor is a borrow of an existing owned FD. Input and output
    // may share an OFD, so every syscall handles WouldBlock as readiness loss.
    let mut poll_fds = vec![PollFd::new(&*wake_reader, PollFlags::IN)];
    let mut indices = Vec::new();
    for (index, entry) in entries.iter().enumerate() {
        if let Some(output) = &entry.output
            && !output.paused
        {
            poll_fds.push(PollFd::new(&output.reader, PollFlags::IN));
            indices.push((index, false));
        }
    }
    for (index, file) in &input_files {
        poll_fds.push(PollFd::new(&**file, PollFlags::OUT));
        indices.push((*index, true));
    }
    let timeout = deadline.map(|deadline| {
        Timespec::try_from(deadline.saturating_duration_since(Instant::now()))
            .expect("native poll duration fits Timespec")
    });
    let poll_result = poll(&mut poll_fds, timeout.as_ref());
    diagnostics.poll_returns.fetch_add(1, Ordering::AcqRel);
    let mut ready = vec![(false, false); entries.len()];
    let owner_woken = match poll_result {
        Ok(count) => {
            let woken = count == 0
                || poll_fds[0]
                    .revents()
                    .intersects(PollFlags::IN | PollFlags::HUP | PollFlags::ERR | PollFlags::NVAL);
            for (fd, (index, input)) in poll_fds.iter().skip(1).zip(indices) {
                let expected = if input { PollFlags::OUT } else { PollFlags::IN };
                if fd
                    .revents()
                    .intersects(expected | PollFlags::HUP | PollFlags::ERR | PollFlags::NVAL)
                {
                    if input {
                        ready[index].1 = true;
                    } else {
                        ready[index].0 = true;
                    }
                }
            }
            woken
        }
        Err(Errno::INTR) => false,
        Err(error) => {
            let _ = crate::diagnostics::record(format_args!(
                "ctxmuxd daemon-wide native poll failed: {error}"
            ));
            return Err(());
        }
    };
    drop(poll_fds);
    drop(input_files);
    if owner_woken {
        let mut buffer = [0_u8; 64];
        loop {
            match wake_reader.read(&mut buffer) {
                Ok(0) => return Err(()),
                Ok(_) => {}
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) => {
                    let _ = crate::diagnostics::record(format_args!(
                        "ctxmuxd native owner wake drain failed: {error}"
                    ));
                    return Err(());
                }
            }
        }
    }
    let count = entries.len();
    let start = if count == 0 { 0 } else { *fair_start % count };
    for offset in 0..count {
        let index = (start + offset) % count;
        let (output_ready, input_ready) = ready[index];
        // One configurable quantum per ready Run; a large request remains
        // admitted and keeps its full byte charge through all later turns.
        if input_ready {
            entries[index]
                .control
                .progress_input(resources.input_turn_commands, resources.input_turn_bytes);
        }
        if !output_ready {
            continue;
        }
        let Some(run) = entries[index].run.upgrade() else {
            entries[index].output = None;
            continue;
        };
        // Acquire the complete output admission turn before touching the PTY.
        // A slow export or persistence transition stays local to this Run.
        let Some(mut turn) = run.try_output_turn() else {
            if let Some(output) = &mut entries[index].output {
                output.paused = true;
            }
            run.native_output_pressure(true);
            continue;
        };
        let capacity = turn.capacity().min(OUTPUT_READ_BUFFER_BYTES);
        run.native_output_pressure(capacity == 0);
        if capacity == 0 {
            if let Some(output) = &mut entries[index].output {
                output.paused = true;
            }
            continue;
        }
        let Some(output) = entries[index].output.as_mut() else {
            continue;
        };
        let mut buffer = [0_u8; OUTPUT_READ_BUFFER_BYTES];
        match output.reader.read(&mut buffer[..capacity]) {
            Ok(0) => {
                run.native_output_closed(None);
                entries[index].output = None;
            }
            Ok(read) => {
                output.pending_offer = turn.record(buffer[..read].to_vec());
                if let Some(terminal) = entries[index].terminal.as_mut() {
                    terminal.deadline = Instant::now() + OUTPUT_DRAIN_TIMEOUT;
                }
            }
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock
                ) => {}
            Err(error) if error.raw_os_error() == Some(Errno::IO.raw_os_error()) => {
                run.native_output_closed(None);
                entries[index].output = None;
            }
            Err(error) => {
                run.native_output_closed(Some(&error.to_string()));
                let _ = crate::diagnostics::record(format_args!(
                    "ctxmuxd PTY read failed for {}: {error}",
                    run.id
                ));
                entries[index].output = None;
            }
        }
    }
    if count > 0 {
        *fair_start = (start + 1) % count;
    }
    Ok(owner_woken)
}

fn detach_active_workers(active: &mut HashMap<u64, thread::JoinHandle<()>>) {
    active.clear();
}

fn preserve_shutdown_authority(entries: &mut Vec<NativeEntry>, queued: &mut VecDeque<WorkerJob>) {
    // Owner completion is not evidence of waitid failure or child exit. Keep
    // the actual holders (including unread PTY bytes) alive until process exit;
    // publishing unavailable above is the only completion claim we can make.
    // This retained cost is real and is not silently reclaimed as a reaped Run.
    for job in queued.drain(..) {
        if let WorkerJob::Cleanup(job) = job {
            job.watching
                .control
                .retain_owner_stopped_child(job.watching.child.into_child());
            std::mem::forget(job.watching.control);
        }
    }
    for mut entry in entries.drain(..) {
        if entry.output.is_none()
            && entry.terminal.is_none()
            && matches!(entry.lifecycle, Lifecycle::Finalizing)
        {
            // Cleanup already reaped the child and drained the reader before
            // Finalizing. The detached worker owns only terminal publication;
            // release this actual entry before announcing physical retirement.
            let run = entry.run.clone();
            drop(entry);
            if let Some(run) = run.upgrade() {
                run.native_entry_retired();
            }
            continue;
        }
        let lifecycle = std::mem::replace(&mut entry.lifecycle, Lifecycle::Done);
        match lifecycle {
            Lifecycle::Watching(watching) => {
                watching
                    .control
                    .retain_owner_stopped_child(watching.child.into_child());
            }
            Lifecycle::WaitingCleanup(waiting) => {
                waiting
                    .watching
                    .control
                    .retain_owner_stopped_child(waiting.watching.child.into_child());
            }
            Lifecycle::AuthorityLost(control) => std::mem::forget(control),
            _ => {}
        }
        std::mem::forget(entry);
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{self, Write},
        os::{fd::OwnedFd, unix::net::UnixStream},
        sync::{
            Arc, Condvar, Mutex,
            atomic::{AtomicUsize, Ordering},
            mpsc,
        },
        time::{Duration, Instant},
    };

    use ctxmux_protocol::{
        CommandDisposition, ErrorCode, InputOperationKey, RunId, RunState, TerminalSize,
    };
    use portable_pty::{Child, ChildKiller, ExitStatus};

    use super::NativeRunOwner;
    use crate::{
        NativeWaitFailure, Run, native_control::NativeControlOwner, native_session::NativeSession,
    };

    fn finished_owner_with_open_receiver() -> (NativeRunOwner, mpsc::Receiver<super::OwnerCommand>)
    {
        let owner = NativeRunOwner::default();
        owner
            .shutdown(Instant::now() + Duration::from_secs(2))
            .unwrap();
        // A real completed thread with a still-open receiver: successful send
        // alone cannot establish that a service is consuming its commands.
        let (commands, receiver) =
            mpsc::sync_channel(crate::ResourceLimits::DEFAULT.creation_workers);
        let thread = std::thread::spawn(|| {});
        let deadline = Instant::now() + Duration::from_secs(2);
        while !thread.is_finished() {
            assert!(Instant::now() < deadline);
            std::thread::yield_now();
        }
        *crate::mutex_lock(&owner.inner.completion) = None;
        *crate::mutex_lock(&owner.inner.state) = super::OwnerState::Running { commands, thread };
        (owner, receiver)
    }

    #[test]
    fn finished_owner_is_not_reported_running_even_while_command_receiver_exists() {
        let (owner, receiver) = finished_owner_with_open_receiver();
        assert_eq!(
            owner.ensure_running(),
            Err("daemon-wide native owner stopped".to_owned())
        );
        assert!(matches!(
            &*crate::mutex_lock(&owner.inner.state),
            super::OwnerState::Failed(_)
        ));
        assert!(
            owner
                .handoff_ready(RunId::new())
                .unwrap_err()
                .contains("owner stopped")
        );
        assert!(
            owner
                .extract_for_handoff()
                .unwrap_err()
                .contains("owner stopped")
        );
        drop(receiver);
    }

    #[test]
    fn finished_owner_rejects_handoff_before_waiting_for_an_open_receiver() {
        for extract in [false, true] {
            let (owner, receiver) = finished_owner_with_open_receiver();
            let (result_tx, result_rx) = mpsc::channel();
            let request = std::thread::spawn(move || {
                let result = if extract {
                    owner.extract_for_handoff().map(|_| ())
                } else {
                    owner.handoff_ready(RunId::new()).map(|_| ())
                };
                result_tx.send(result).unwrap();
            });
            let result = result_rx.recv_timeout(Duration::from_secs(2));
            // Release the private receiver even if the guarded request stalls.
            // This wakes its send/reply boundary so no failing test leaks a thread.
            drop(receiver);
            request.join().unwrap();
            assert!(
                result
                    .expect("finished owner must reject without receiver cleanup")
                    .unwrap_err()
                    .contains("owner stopped")
            );
        }
    }

    #[test]
    fn finished_owner_rejects_registration_without_queuing_it() {
        let (owner, receiver) = finished_owner_with_open_receiver();
        let id = RunId::new();
        let failure = NativeWaitFailure::default();
        let (control, run) = test_run(&owner, id, failure.clone());
        let result = owner.register_for_test(
            &run,
            Box::new(WatchingChild),
            watching_session(Arc::new(AtomicUsize::new(0))),
            control,
            failure,
            || {},
        );
        let (message, registration) = result
            .expect_err("completed owner cannot accept registration")
            .into_parts();
        assert!(message.contains("owner stopped"));
        assert!(!matches!(
            receiver.try_recv(),
            Ok(super::OwnerCommand::Register(_))
        ));
        drop(registration);
    }

    #[test]
    fn live_owner_remains_available_after_ordinary_clone_release() {
        let owner = NativeRunOwner::default();
        drop(owner.clone());
        assert_eq!(owner.ensure_running(), Ok(()));
        owner
            .shutdown(Instant::now() + Duration::from_secs(2))
            .unwrap();
    }

    #[derive(Debug)]
    struct WatchingChild;

    impl Child for WatchingChild {
        fn try_wait(&mut self) -> std::io::Result<Option<ExitStatus>> {
            Ok(None)
        }

        fn wait(&mut self) -> std::io::Result<ExitStatus> {
            Ok(ExitStatus::with_exit_code(0))
        }

        fn process_id(&self) -> Option<u32> {
            Some(42)
        }

        #[cfg(windows)]
        fn as_raw_handle(&self) -> Option<std::os::windows::io::RawHandle> {
            None
        }
    }

    impl ChildKiller for WatchingChild {
        fn kill(&mut self) -> std::io::Result<()> {
            Ok(())
        }

        fn clone_killer(&self) -> Box<dyn ChildKiller + Send + Sync> {
            Box::new(Self)
        }
    }

    fn watching_session(probes: Arc<AtomicUsize>) -> NativeSession {
        NativeSession::from_child_pid(42)
            .unwrap()
            .with_leader_probe_for_test(Arc::new(move || {
                probes.fetch_add(1, Ordering::AcqRel);
                Ok(false)
            }))
    }

    fn test_run(
        owner: &NativeRunOwner,
        id: RunId,
        failure: NativeWaitFailure,
    ) -> (NativeControlOwner, Arc<Run>) {
        let control = NativeControlOwner::new_for_wait_test(id, owner.owner_wake());
        let run = Run::new_native_for_owner_test(id, control.clone(), owner.clone(), failure);
        (control, run)
    }

    #[test]
    fn zero_entry_owner_blocks_until_an_explicit_wake() {
        let owner = NativeRunOwner::default();
        let before = owner.diagnostic_snapshot();
        std::thread::sleep(Duration::from_millis(100));
        let after = owner.diagnostic_snapshot();
        assert_eq!(after.poll_returns, before.poll_returns);
        owner
            .shutdown(Instant::now() + Duration::from_secs(1))
            .expect("wake and stop idle production owner");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cleanup_spawn_failure_fail_stops_the_production_owner() {
        let owner = NativeRunOwner::default();
        let failure = NativeWaitFailure::default();
        let id = RunId::new();
        let (control, run) = test_run(&owner, id, failure.clone());
        owner
            .register_for_test(
                &run,
                Box::new(WatchingChild),
                watching_session(Arc::new(AtomicUsize::new(0))),
                control.clone(),
                failure,
                || {},
            )
            .map_err(|error| error.into_parts().0)
            .expect("register production cleanup failure fixture");
        owner.fail_next_worker_spawn();
        let error = control
            .begin_stop()
            .expect("reserve cleanup before Stop mutation")
            .resolve(Duration::from_secs(1))
            .await
            .expect_err("injected worker spawn failure is explicit");
        assert_eq!(error.disposition, CommandDisposition::Unknown);
        assert_eq!(error.error.code, ErrorCode::Io);
        tokio::time::timeout(Duration::from_secs(1), async {
            while !control.retains_failed_child() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("spawn failure transfers child authority fail-stop");
    }

    fn spawn_actual_service_run(
        owner: &NativeRunOwner,
        script: &str,
        failure: NativeWaitFailure,
    ) -> Arc<Run> {
        Run::spawn_with_hooks(
            crate::NativeSpawnConfig {
                id: RunId::new(),
                spec: ctxmux_protocol::RunSpec {
                    program: "/bin/sh".to_owned(),
                    args: vec!["-c".to_owned(), script.to_owned()],
                    cwd: None,
                    env: std::collections::BTreeMap::default(),
                    initial_size: TerminalSize::default(),
                    declared_inputs: Vec::new(),
                },
                lineage: None,
                persistence_mode: crate::PersistenceMode::MemoryOnly,
                live_event_capacity: crate::LIVE_EVENT_CAPACITY,
                input_drains: crate::native_control::InputDrainGate::default(),
                native_runs: owner.clone(),
                terminal_publications: crate::TerminalPublicationOwner::default(),
                wait_failure: failure,
                qualification_stats: crate::qualification_stats::QualificationStats::default(),
                retention_budget: crate::RetentionBudget::production(),
            },
            |run| run,
            |_, _| Ok(()),
            || {},
        )
        .unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn normal_exit_releases_native_descriptors_without_metadata_polling() {
        let owner = NativeRunOwner::default();
        let run =
            spawn_actual_service_run(&owner, "printf READY; exit 0", NativeWaitFailure::default());
        let Some(crate::RunControl::Native(control)) = &run.incarnation_control else {
            panic!("Native control");
        };
        let deadline = Instant::now() + Duration::from_secs(5);
        while control.master_raw_fd().is_some() {
            assert!(
                Instant::now() < deadline,
                "actual Entry Drop must release closed master"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert!(matches!(run.info().state, RunState::Exited { .. }));
        let bytes = run
            .lock_owner(&run.output)
            .replay(0)
            .chunks
            .into_iter()
            .flat_map(|chunk| chunk.data)
            .collect::<Vec<_>>();
        assert_eq!(bytes, b"READY");
        assert!(control.closed_quiescence_result().is_ok());
        owner
            .shutdown(Instant::now() + Duration::from_secs(2))
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(
        clippy::too_many_lines,
        reason = "one held-owner fixture proves the original Run waits, a second real Run serves, actual unlock wakes, and funded disconnect cancellation"
    )]
    async fn held_control_metadata_does_not_block_another_real_run_or_busy_poll() {
        let owner = NativeRunOwner::default();
        let slow = spawn_actual_service_run(
            &owner,
            "stty raw -echo; printf READY; exec /bin/cat",
            NativeWaitFailure::default(),
        );
        let healthy = spawn_actual_service_run(
            &owner,
            "stty raw -echo; printf READY; exec /bin/cat",
            NativeWaitFailure::default(),
        );
        let Some(crate::RunControl::Native(slow_control)) = &slow.incarnation_control else {
            panic!("Native control");
        };
        let Some(crate::RunControl::Native(healthy_control)) = &healthy.incarnation_control else {
            panic!("Native control");
        };
        let deadline = Instant::now() + Duration::from_secs(5);
        while slow.info().latest_output_bytes < 5 || healthy.info().latest_output_bytes < 5 {
            assert!(Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        // No relay backstop may mask a missed cooperative unlock wake.
        owner.mark_signal_driven();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let held_control = slow_control.clone();
        let holder = std::thread::spawn(move || {
            held_control.with_metadata(|_, _| {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            });
        });
        entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        owner.owner_wake().wake();
        // All commands below target the genuinely held Run, before effects.
        // They must return without consuming either Tokio worker or modifying
        // the queued bytes/geometry. A recoverable key cannot be declared new
        // until the owner has inspected its ledger.
        for failure in [
            slow_control
                .begin_input(b"must-not-apply".to_vec())
                .err()
                .unwrap(),
            slow_control.begin_stop().err().unwrap(),
            slow_control
                .begin_signal(ctxmux_protocol::RunSignal::Interrupt)
                .err()
                .unwrap(),
            slow_control
                .resize(ctxmux_protocol::TerminalSize { cols: 91, rows: 31 }, |_| {
                    panic!("busy resize must not publish or apply")
                })
                .err()
                .unwrap(),
        ] {
            assert_eq!(failure.error.code, ErrorCode::ControlBackpressure);
            assert_eq!(failure.disposition, CommandDisposition::NotApplied);
            assert_eq!(failure.confirmed_input_bytes, None);
        }
        let unresolved = slow_control
            .begin_recoverable_input(
                InputOperationKey::new("held-peer-unknown-key").unwrap(),
                0,
                b"never-replayed-while-busy".to_vec(),
            )
            .err()
            .unwrap();
        assert_eq!(unresolved.error.code, ErrorCode::ControlBackpressure);
        assert_eq!(unresolved.disposition, CommandDisposition::Unknown);
        assert_eq!(unresolved.confirmed_input_bytes, None);
        let waiting_control = slow_control.clone();
        let waiting = tokio::spawn(async move {
            waiting_control
                .begin_recoverable_input_async(
                    InputOperationKey::new("held-peer-actual-unlock").unwrap(),
                    0,
                    b"slow-after-actual-unlock".to_vec(),
                )
                .await
                .unwrap()
                .resolve()
                .await
        });
        let cancelled_control = slow_control.clone();
        let cancelled = tokio::spawn(async move {
            cancelled_control
                .begin_input_async(b"cancelled-before-admission".to_vec())
                .await
        });
        tokio::task::yield_now().await;
        let data = b"healthy-under-control-pressure";
        let outcome = tokio::time::timeout(
            Duration::from_secs(2),
            healthy_control
                .begin_recoverable_input(
                    InputOperationKey::new("healthy-with-held-peer").unwrap(),
                    0,
                    data.to_vec(),
                )
                .unwrap()
                .resolve(),
        )
        .await;
        // Always release the real held control before asserting, even on RED,
        // so a regression cannot leave an unjoinable owner or test child.
        if outcome.is_err() {
            release_tx.send(()).unwrap();
            holder.join().unwrap();
            panic!("one held Run control blocked healthy Run input");
        }
        outcome.unwrap().unwrap();
        while healthy.info().latest_output_bytes < 5 + data.len() as u64 {
            assert!(Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let bytes = healthy
            .lock_owner(&healthy.output)
            .replay(0)
            .chunks
            .into_iter()
            .flat_map(|chunk| chunk.data)
            .collect::<Vec<_>>();
        assert_eq!(&bytes[5..], data);
        tokio::time::sleep(Duration::from_millis(50)).await;
        let stable = owner.diagnostic_snapshot();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            owner.diagnostic_snapshot().poll_returns,
            stable.poll_returns,
            "a held state mutex must not introduce timer or ready-fd busy polling"
        );
        assert!(
            !waiting.is_finished(),
            "same-Run request waits, without being rejected"
        );
        assert!(
            !cancelled.is_finished(),
            "unadmitted future remains cancellable"
        );
        cancelled.abort();
        assert!(cancelled.await.unwrap_err().is_cancelled());
        release_tx.send(()).unwrap();
        holder.join().unwrap();
        while owner.diagnostic_snapshot().lifecycle_probes <= stable.lifecycle_probes {
            assert!(
                Instant::now() < deadline,
                "actual unlock must wake the deferred owner without another request"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let receipt = tokio::time::timeout(Duration::from_secs(2), waiting)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(receipt.start_byte, 0);
        assert_eq!(receipt.end_byte, b"slow-after-actual-unlock".len() as u64);
        while slow.info().latest_output_bytes < 5 + receipt.end_byte {
            assert!(Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let bytes = slow
            .lock_owner(&slow.output)
            .replay(0)
            .chunks
            .into_iter()
            .flat_map(|chunk| chunk.data)
            .collect::<Vec<_>>();
        assert_eq!(bytes, b"READYslow-after-actual-unlock");
        for control in [slow_control, healthy_control] {
            control
                .begin_stop()
                .unwrap()
                .resolve(Duration::from_secs(5))
                .await
                .unwrap();
        }
        owner
            .shutdown(Instant::now() + Duration::from_secs(2))
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn real_partial_input_stop_fences_exact_prefix_and_never_reports_whole_success() {
        let owner = NativeRunOwner::default();
        let run = spawn_actual_service_run(
            &owner,
            "stty raw -echo; printf READY; exec /bin/sleep 30",
            NativeWaitFailure::default(),
        );
        let Some(crate::RunControl::Native(control)) = &run.incarnation_control else {
            panic!("Native control");
        };
        let deadline = Instant::now() + Duration::from_secs(5);
        while run.info().latest_output_bytes < 5 {
            assert!(Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let data = vec![0x61; 128 * 1024 + 3];
        let first = control
            .begin_recoverable_input(
                InputOperationKey::new("partial-before-stop").unwrap(),
                0,
                data.clone(),
            )
            .unwrap();
        let second = control
            .begin_recoverable_input(
                InputOperationKey::new("unattempted-after-partial").unwrap(),
                data.len() as u64,
                vec![0x62; 17],
            )
            .unwrap();
        while !control.input_service().write_blocked
            || control.input_service().active_confirmed_bytes == 0
        {
            assert!(Instant::now() < deadline, "real PTY fills before Stop");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let confirmed = control.input_service().active_confirmed_bytes;
        assert!(confirmed < data.len());
        let stop = control.begin_stop().unwrap();
        let failure = tokio::time::timeout(Duration::from_secs(3), first.resolve())
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(failure.disposition, CommandDisposition::Unknown);
        assert_eq!(failure.confirmed_input_bytes, Some(confirmed));
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), second.resolve())
                .await
                .unwrap()
                .unwrap_err()
                .disposition,
            CommandDisposition::NotApplied
        );
        assert_eq!(control.input_service().completed_input_bytes, Some(0));
        assert_eq!(control.input_service().unsettled_request_bytes, 0);
        let retained = control
            .begin_recoverable_input(
                InputOperationKey::new("partial-before-stop").unwrap(),
                0,
                data,
            )
            .unwrap()
            .resolve()
            .await
            .unwrap_err();
        assert_eq!(
            retained, failure,
            "duplicate never replays a written prefix or guesses suffix delivery"
        );
        stop.resolve(Duration::from_secs(5)).await.unwrap();
        owner
            .shutdown(Instant::now() + Duration::from_secs(2))
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(
        clippy::too_many_lines,
        reason = "one causal owner-unwind fixture keeps both original child identities, unavailable service facts, exact input uncertainty, and retained authority together"
    )]
    async fn real_owner_unwind_publishes_unavailable_and_retains_both_actual_children() {
        use ctxmux_protocol::{
            NativeInputPhase, NativeOutputStatus, NativeOwnerStatus, NativeServiceFailure,
        };
        let owner = NativeRunOwner::default();
        let failure = NativeWaitFailure::default();
        let runs = (0..2)
            .map(|_| {
                spawn_actual_service_run(
                    &owner,
                    "stty raw -echo; printf READY; exec /bin/sleep 30",
                    failure.clone(),
                )
            })
            .collect::<Vec<_>>();
        let deadline = Instant::now() + Duration::from_secs(5);
        while runs.iter().any(|run| run.info().latest_output_bytes < 5) {
            assert!(Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let ids = runs
            .iter()
            .map(|run| (run.id, run.info().pid.unwrap()))
            .collect::<Vec<_>>();
        assert_ne!(ids[0], ids[1]);
        let controls = runs
            .iter()
            .map(|run| match &run.incarnation_control {
                Some(crate::RunControl::Native(control)) => control.clone(),
                _ => panic!("Native control"),
            })
            .collect::<Vec<_>>();
        let data = vec![0x61; 128 * 1024 + 3];
        let pending = controls[1]
            .begin_recoverable_input(InputOperationKey::new("unwind-partial").unwrap(), 0, data)
            .unwrap();
        while !controls[1].input_service().write_blocked
            || controls[1].input_service().active_confirmed_bytes == 0
        {
            assert!(Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let confirmed = controls[1].input_service().active_confirmed_bytes;
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let held = controls[0].clone();
        let holder = std::thread::spawn(move || {
            held.with_metadata(|_, _| {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            });
        });
        entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let commands = {
            let state = crate::mutex_lock(&owner.inner.state);
            let super::OwnerState::Running { commands, .. } = &*state else {
                panic!("live owner");
            };
            commands.clone()
        };
        owner.owner_wake().wake();
        commands.send(super::OwnerCommand::UnwindForTest).unwrap();
        owner.owner_wake().wake();
        drop(commands);
        let completed_while_busy = tokio::time::timeout(Duration::from_secs(2), async {
            while !owner.inner.completion_finished.load(Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await;
        // Completion and public failure facts must be independent of one held
        // control. Release this test holder before a RED assertion as cleanup.
        if completed_while_busy.is_err() {
            release_tx.send(()).unwrap();
            holder.join().unwrap();
            panic!("held peer control blocked owner completion and other Run faults");
        }
        for run in &runs {
            let service = run.native_service.as_ref().unwrap().snapshot();
            assert_eq!(
                service.owner,
                NativeOwnerStatus::Stopped {
                    reason: NativeServiceFailure::OwnerUnwound
                }
            );
            assert_eq!(
                service.input.phase,
                NativeInputPhase::Unavailable {
                    reason: NativeServiceFailure::OwnerUnwound
                }
            );
        }
        assert!(controls[0].begin_input(b"not-sent".to_vec()).is_err());
        assert!(controls[0].begin_stop().is_err());
        let busy_recoverable = controls[0]
            .begin_recoverable_input(
                InputOperationKey::new("busy-unknown-ledger").unwrap(),
                0,
                vec![1],
            )
            .unwrap_err();
        assert_eq!(busy_recoverable.disposition, CommandDisposition::Unknown);
        assert_eq!(busy_recoverable.confirmed_input_bytes, None);
        let unsettled = tokio::time::timeout(Duration::from_secs(2), pending.resolve())
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(unsettled.disposition, CommandDisposition::Unknown);
        assert_eq!(unsettled.confirmed_input_bytes, Some(confirmed));
        release_tx.send(()).unwrap();
        holder.join().unwrap();
        assert_eq!(
            *crate::mutex_lock(&owner.inner.completion),
            Some(NativeServiceFailure::OwnerUnwound)
        );
        assert!(owner.ensure_running().is_err());
        assert!(
            failure.incarnation_failure.message().is_none(),
            "owner unwind is not a fabricated waitid failure"
        );
        for (index, run) in runs.iter().enumerate() {
            let info = run.info();
            assert_eq!((info.id, info.pid.unwrap()), ids[index]);
            assert!(
                info.state.is_running(),
                "no child lifecycle event is invented"
            );
            let service = info.native_service.unwrap();
            assert_eq!(
                service.owner,
                NativeOwnerStatus::Stopped {
                    reason: NativeServiceFailure::OwnerUnwound
                }
            );
            assert_eq!(
                service.output,
                NativeOutputStatus::Unavailable {
                    reason: NativeServiceFailure::OwnerUnwound
                }
            );
            assert_eq!(
                service.input.phase,
                NativeInputPhase::Unavailable {
                    reason: NativeServiceFailure::OwnerUnwound
                }
            );
            let Some(crate::RunControl::Native(control)) = &run.incarnation_control else {
                panic!("Native control");
            };
            assert!(control.retains_failed_child());
            assert!(control.wait_authority_failure().is_none());
            assert!(control.begin_input(b"not-sent".to_vec()).is_err());
            assert!(control.begin_stop().is_err());
            assert!(owner.handoff_ready(info.id).is_err());
            let mut session = NativeSession::from_child_pid(ids[index].1).unwrap();
            assert!(
                !session.leader_is_terminal().unwrap(),
                "actual child survives owner unwind"
            );
            // Explicit cleanup of test-created children only, using their actual
            // retained holder; production completion above performs no Stop.
            control
                .cleanup_retained_owner_child_for_test(&mut session)
                .unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(
        clippy::too_many_lines,
        reason = "one causal owner-unwind fixture keeps both original child identities, unavailable service facts, exact input uncertainty, and retained authority together"
    )]
    async fn real_owner_completion_publishes_before_producer_release() {
        use ctxmux_protocol::{
            NativeInputPhase, NativeOutputStatus, NativeOwnerStatus, NativeServiceFailure,
        };
        let owner = NativeRunOwner::default();
        let failure = NativeWaitFailure::default();
        let runs = (0..2)
            .map(|_| {
                spawn_actual_service_run(
                    &owner,
                    "stty raw -echo; printf READY; exec /bin/sleep 30",
                    failure.clone(),
                )
            })
            .collect::<Vec<_>>();
        let deadline = Instant::now() + Duration::from_secs(5);
        while runs.iter().any(|run| run.info().latest_output_bytes < 5) {
            assert!(Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let ids = runs
            .iter()
            .map(|run| (run.id, run.info().pid.unwrap()))
            .collect::<Vec<_>>();
        assert_ne!(ids[0], ids[1]);
        let controls = runs
            .iter()
            .map(|run| match &run.incarnation_control {
                Some(crate::RunControl::Native(control)) => control.clone(),
                _ => panic!("Native control"),
            })
            .collect::<Vec<_>>();
        let data = vec![0x61; 128 * 1024 + 3];
        let pending = controls[1]
            .begin_recoverable_input(InputOperationKey::new("unwind-partial").unwrap(), 0, data)
            .unwrap();
        while !controls[1].input_service().write_blocked
            || controls[1].input_service().active_confirmed_bytes == 0
        {
            assert!(Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let confirmed = controls[1].input_service().active_confirmed_bytes;
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let held = controls[0].clone();
        let holder = std::thread::spawn(move || {
            held.with_metadata(|_, _| {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            });
        });
        entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let commands = {
            let state = crate::mutex_lock(&owner.inner.state);
            let super::OwnerState::Running { commands, .. } = &*state else {
                panic!("live owner");
            };
            commands.clone()
        };
        owner.owner_wake().wake();
        commands.send(super::OwnerCommand::UnwindForTest).unwrap();
        owner.owner_wake().wake();

        // A borrowed producer can be descheduled before its send or reply.
        // Existing Runs need real failure facts before that producer retires.
        let before_release = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let facts = runs
                    .iter()
                    .map(|run| run.native_service.as_ref().unwrap().snapshot())
                    .collect::<Vec<_>>();
                if facts
                    .iter()
                    .all(|fact| matches!(fact.owner, NativeOwnerStatus::Stopped { .. }))
                {
                    return facts;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await;
        let captured_before_release = runs
            .iter()
            .map(|run| run.native_service.as_ref().unwrap().snapshot())
            .collect::<Vec<_>>();
        println!(
            "{}",
            serde_json::json!({
                "completion_before_release": *crate::mutex_lock(&owner.inner.completion),
                "service_before_release": captured_before_release,
                "service_progress_before_release": before_release.is_ok()
            })
        );
        let mut pending_result = Box::pin(pending.resolve());
        let settled_before_release =
            tokio::time::timeout(Duration::from_secs(2), &mut pending_result).await;
        let refused_before_release = controls[1].begin_input(b"must-refuse".to_vec()).is_err();
        println!(
            "{}",
            serde_json::json!({
                "input_settled_before_release": settled_before_release.is_ok(),
                "new_input_refused_before_release": refused_before_release
            })
        );
        // Release the producer and finish exact fixture cleanup before asserting
        // RED; an assertion must never abandon either private child.
        drop(commands);
        let completed_while_busy = tokio::time::timeout(Duration::from_secs(2), async {
            while !owner.inner.completion_finished.load(Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await;
        if completed_while_busy.is_err() {
            release_tx.send(()).unwrap();
            holder.join().unwrap();
            panic!("held peer control blocked owner completion and other Run faults");
        }
        for run in &runs {
            let service = run.native_service.as_ref().unwrap().snapshot();
            assert_eq!(
                service.owner,
                NativeOwnerStatus::Stopped {
                    reason: NativeServiceFailure::OwnerUnwound
                }
            );
            assert_eq!(
                service.input.phase,
                NativeInputPhase::Unavailable {
                    reason: NativeServiceFailure::OwnerUnwound
                }
            );
        }
        assert!(controls[0].begin_input(b"not-sent".to_vec()).is_err());
        assert!(controls[0].begin_stop().is_err());
        let busy_recoverable = controls[0]
            .begin_recoverable_input(
                InputOperationKey::new("busy-unknown-ledger").unwrap(),
                0,
                vec![1],
            )
            .unwrap_err();
        assert_eq!(busy_recoverable.disposition, CommandDisposition::Unknown);
        assert_eq!(busy_recoverable.confirmed_input_bytes, None);
        let was_settled_before_release = settled_before_release.is_ok();
        let unsettled = match settled_before_release {
            Ok(result) => result.unwrap_err(),
            Err(_) => tokio::time::timeout(Duration::from_secs(2), pending_result)
                .await
                .unwrap()
                .unwrap_err(),
        };
        assert_eq!(unsettled.disposition, CommandDisposition::Unknown);
        assert_eq!(unsettled.confirmed_input_bytes, Some(confirmed));
        release_tx.send(()).unwrap();
        holder.join().unwrap();
        assert_eq!(
            *crate::mutex_lock(&owner.inner.completion),
            Some(NativeServiceFailure::OwnerUnwound)
        );
        assert!(owner.ensure_running().is_err());
        assert!(
            failure.incarnation_failure.message().is_none(),
            "owner unwind is not a fabricated waitid failure"
        );
        for (index, run) in runs.iter().enumerate() {
            let info = run.info();
            assert_eq!((info.id, info.pid.unwrap()), ids[index]);
            assert!(
                info.state.is_running(),
                "no child lifecycle event is invented"
            );
            let service = info.native_service.unwrap();
            assert_eq!(
                service.owner,
                NativeOwnerStatus::Stopped {
                    reason: NativeServiceFailure::OwnerUnwound
                }
            );
            assert_eq!(
                service.output,
                NativeOutputStatus::Unavailable {
                    reason: NativeServiceFailure::OwnerUnwound
                }
            );
            assert_eq!(
                service.input.phase,
                NativeInputPhase::Unavailable {
                    reason: NativeServiceFailure::OwnerUnwound
                }
            );
            let Some(crate::RunControl::Native(control)) = &run.incarnation_control else {
                panic!("Native control");
            };
            assert!(control.retains_failed_child());
            assert!(control.wait_authority_failure().is_none());
            assert!(control.begin_input(b"not-sent".to_vec()).is_err());
            assert!(control.begin_stop().is_err());
            assert!(owner.handoff_ready(info.id).is_err());
            let mut session = NativeSession::from_child_pid(ids[index].1).unwrap();
            assert!(
                !session.leader_is_terminal().unwrap(),
                "actual child survives owner unwind"
            );
            // Explicit cleanup of test-created children only, using their actual
            // retained holder; production completion above performs no Stop.
            control
                .cleanup_retained_owner_child_for_test(&mut session)
                .unwrap();
        }
        println!(
            "{}",
            serde_json::json!({"actual_private_children_cleaned": 2,
            "confirmed_prefix": confirmed, "retained_failure": unsettled})
        );
        assert!(
            before_release.is_ok(),
            "existing public service facts waited for a borrowed producer to retire"
        );
        assert!(
            was_settled_before_release,
            "original partial result waited for producer release"
        );
        assert!(
            refused_before_release,
            "failed owner still admitted input before producer release"
        );
        for facts in captured_before_release {
            assert_eq!(
                facts.owner,
                NativeOwnerStatus::Stopped {
                    reason: NativeServiceFailure::OwnerUnwound
                }
            );
            assert_eq!(
                facts.output,
                NativeOutputStatus::Unavailable {
                    reason: NativeServiceFailure::OwnerUnwound
                }
            );
            assert_eq!(
                facts.input.phase,
                NativeInputPhase::Unavailable {
                    reason: NativeServiceFailure::OwnerUnwound
                }
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(
        clippy::too_many_lines,
        reason = "one real ten-Run fixture preserves every original blocked input and validates healthy input, output, CtrlC, FIFO completion, and funded recovery"
    )]
    async fn real_blocked_ptys_preserve_fifo_and_serve_another_run_without_input_workers() {
        use crate::{
            NativeSpawnConfig, PersistenceMode, RetentionBudget, TerminalPublicationOwner,
            native_control::InputDrainGate, qualification_stats::QualificationStats,
        };
        use ctxmux_protocol::{AppliedInputRange, RunSpec};

        // Nine blocked PTYs exceed the old eight blocking-worker slots. These
        // are a held-out regression population, not a product Run ceiling.
        const BLOCKED_RUNS: usize = 9;
        const FIRST_BYTES: usize = 128 * 1024 + 3;
        const SECOND_BYTES: usize = 257;
        let directory = tempfile::tempdir().unwrap();
        let owner = NativeRunOwner::default();
        let gate = InputDrainGate::default();
        let stats = QualificationStats::default();
        let retention_budget = RetentionBudget::production();
        let mut fixtures = Vec::new();
        for index in 0..=BLOCKED_RUNS {
            let fifo = directory.path().join(format!("read-gate-{index}"));
            let args = if index < BLOCKED_RUNS {
                assert!(
                    std::process::Command::new("mkfifo")
                        .arg(&fifo)
                        .status()
                        .unwrap()
                        .success()
                );
                vec!["-c".to_owned(),
                    r#"stty raw -echo; printf READY; IFS= read -r gate < "$1"; dd bs=1 count="$2" 2>/dev/null"#.to_owned(),
                    "read-gated-pty".to_owned(), fifo.to_string_lossy().into_owned(),
                    (FIRST_BYTES + SECOND_BYTES).to_string()]
            } else {
                vec![
                    "-c".to_owned(),
                    "stty -echo; printf READY; exec /bin/cat".to_owned(),
                ]
            };
            let run = Run::spawn_with_hooks(
                NativeSpawnConfig {
                    id: RunId::new(),
                    spec: RunSpec {
                        program: "/bin/sh".to_owned(),
                        args,
                        cwd: None,
                        env: std::collections::BTreeMap::default(),
                        initial_size: TerminalSize::default(),
                        declared_inputs: Vec::new(),
                    },
                    lineage: None,
                    persistence_mode: PersistenceMode::MemoryOnly,
                    live_event_capacity: crate::LIVE_EVENT_CAPACITY,
                    input_drains: gate.clone(),
                    native_runs: owner.clone(),
                    terminal_publications: TerminalPublicationOwner::default(),
                    wait_failure: NativeWaitFailure::default(),
                    qualification_stats: stats.clone(),
                    retention_budget: retention_budget.clone(),
                },
                |run| run,
                |_, _| Ok(()),
                || {},
            )
            .unwrap();
            let Some(crate::RunControl::Native(control)) = &run.incarnation_control else {
                panic!("actual Native control");
            };
            let control = control.clone();
            assert!(run.info().pid.is_some());
            fixtures.push((run, control, fifo));
        }
        let deadline = Instant::now() + Duration::from_secs(15);
        while fixtures
            .iter()
            .any(|(run, ..)| run.info().latest_output_bytes < 5)
        {
            assert!(
                Instant::now() < deadline,
                "real children complete terminal setup"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let first = (0..FIRST_BYTES)
            .map(|index| u8::try_from(index % 251).unwrap())
            .collect::<Vec<_>>();
        let second = vec![0xfa; SECOND_BYTES];
        let mut pending = Vec::new();
        for (_, control, ..) in fixtures.iter().take(BLOCKED_RUNS) {
            let one = control
                .begin_recoverable_input_async(
                    InputOperationKey::new("blocked-first").unwrap(),
                    0,
                    first.clone(),
                )
                .await
                .unwrap();
            let two = control
                .begin_recoverable_input_async(
                    InputOperationKey::new("blocked-second").unwrap(),
                    FIRST_BYTES as u64,
                    second.clone(),
                )
                .await
                .unwrap();
            pending.push((one, two));
        }
        while fixtures.iter().take(BLOCKED_RUNS).any(|(_, control, ..)| {
            let input = control.input_service();
            !input.write_blocked || input.active_confirmed_bytes == 0
        }) {
            assert!(
                Instant::now() < deadline,
                "each actual non-reading PTY reaches kernel write pressure"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        for (_, control, ..) in fixtures.iter().take(BLOCKED_RUNS) {
            let input = control.input_service();
            assert_eq!(input.unsettled_commands, 2);
            assert_eq!(input.unsettled_request_bytes, FIRST_BYTES + SECOND_BYTES);
            assert_eq!(input.completed_input_bytes, Some(0));
            assert!(input.active_confirmed_bytes < FIRST_BYTES);
            assert!(
                control.handoff_input_state().is_err(),
                "partial input cannot be handed off as completed"
            );
        }
        let (healthy, healthy_control, ..) = &fixtures[BLOCKED_RUNS];
        let healthy_data = b"healthy-input\n";
        let range = tokio::time::timeout(Duration::from_secs(3), async {
            healthy_control
                .begin_recoverable_input_async(
                    InputOperationKey::new("healthy-input").unwrap(),
                    0,
                    healthy_data.to_vec(),
                )
                .await
                .unwrap()
                .resolve()
                .await
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            range,
            AppliedInputRange {
                start_byte: 0,
                end_byte: healthy_data.len() as u64
            }
        );
        while healthy.info().latest_output_bytes < 5 + b"healthy-input\r\n".len() as u64 {
            assert!(
                Instant::now() < deadline,
                "healthy Run output continues under unrelated PTY pressure"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let healthy_output = healthy
            .lock_owner(&healthy.output)
            .replay(0)
            .chunks
            .into_iter()
            .flat_map(|chunk| chunk.data)
            .collect::<Vec<_>>();
        assert_eq!(healthy_output, b"READYhealthy-input\r\n");
        let ctrlc = tokio::time::timeout(Duration::from_secs(3), async {
            healthy_control
                .begin_recoverable_input_async(
                    InputOperationKey::new("healthy-ctrl-c").unwrap(),
                    healthy_data.len() as u64,
                    vec![3],
                )
                .await
                .unwrap()
                .resolve()
                .await
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(ctrlc.start_byte, healthy_data.len() as u64);
        assert_eq!(ctrlc.end_byte, healthy_data.len() as u64 + 1);
        while healthy.info().state.is_running() {
            assert!(
                Instant::now() < deadline,
                "actual terminal Ctrl+C reaches healthy child"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        // Release every original blocked child. No request is dropped or
        // reduced to obtain fairness; verify both completion ranges and bytes.
        for (_, _, fifo) in fixtures.iter().take(BLOCKED_RUNS) {
            std::fs::write(fifo, b"read-now\n").unwrap();
        }
        let mut expected = b"READY".to_vec();
        expected.extend_from_slice(&first);
        expected.extend_from_slice(&second);
        for (index, (one, two)) in pending.into_iter().enumerate() {
            let one = tokio::time::timeout(Duration::from_secs(15), one.resolve())
                .await
                .unwrap()
                .unwrap();
            let two = tokio::time::timeout(Duration::from_secs(15), two.resolve())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                one,
                AppliedInputRange {
                    start_byte: 0,
                    end_byte: FIRST_BYTES as u64
                }
            );
            assert_eq!(
                two,
                AppliedInputRange {
                    start_byte: FIRST_BYTES as u64,
                    end_byte: (FIRST_BYTES + SECOND_BYTES) as u64
                }
            );
            let (run, control, ..) = &fixtures[index];
            let output_deadline = Instant::now() + Duration::from_secs(15);
            while run.info().latest_output_bytes < expected.len() as u64
                || run.info().state.is_running()
            {
                assert!(
                    Instant::now() < output_deadline,
                    "released child completes original exact byte workload"
                );
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            let actual = run
                .lock_owner(&run.output)
                .replay(0)
                .chunks
                .into_iter()
                .flat_map(|chunk| chunk.data)
                .collect::<Vec<_>>();
            assert_eq!(
                actual, expected,
                "PTY bytes preserve FIFO and binary contents"
            );
            assert_eq!(
                control.input_service().completed_input_bytes,
                Some((FIRST_BYTES + SECOND_BYTES) as u64)
            );
            assert_eq!(control.input_service().unsettled_request_bytes, 0);
        }
        owner
            .shutdown(Instant::now() + Duration::from_secs(2))
            .unwrap();
    }

    #[test]
    fn queued_cleanup_shutdown_is_bounded_and_retains_authority() {
        let owner = NativeRunOwner::default();
        let admission = owner.cleanup_admission();
        let permits = (0..8)
            .map(|_| admission.try_acquire().expect("fill cleanup admission"))
            .collect::<Vec<_>>();
        let failure = NativeWaitFailure::default();
        let id = RunId::new();
        let (control, run) = test_run(&owner, id, failure.clone());
        owner
            .register_for_test(
                &run,
                Box::new(WatchingChild),
                watching_session(Arc::new(AtomicUsize::new(0))),
                control.clone(),
                failure,
                || {},
            )
            .map_err(|error| error.into_parts().0)
            .expect("register queued cleanup fixture");
        control
            .cleanup_unpublished()
            .expect("queue unpublished cleanup behind full admission");
        std::thread::sleep(Duration::from_millis(30));
        let started = Instant::now();
        owner
            .shutdown(Instant::now() + Duration::from_secs(1))
            .expect("queued owner observes shutdown wake");
        assert!(started.elapsed() < Duration::from_millis(250));
        assert!(control.retains_failed_child());
        drop(permits);
    }

    #[test]
    fn noisy_output_does_not_multiply_wait_probes_by_chunk_and_run() {
        const RUNS: usize = 128;
        const OUTPUT_BYTES: usize = 2 * 1024 * 1024;

        let owner = NativeRunOwner::default();
        let probes = Arc::new(AtomicUsize::new(0));
        let mut runs = Vec::with_capacity(RUNS);
        let mut noisy_writer = None;
        for index in 0..RUNS {
            let failure = NativeWaitFailure::default();
            let id = RunId::new();
            let (control, run) = test_run(&owner, id, failure.clone());
            let reader = if index == 0 {
                let (reader, writer) = UnixStream::pair().expect("create noisy PTY surrogate");
                noisy_writer = Some(writer);
                let reader: OwnedFd = reader.into();
                std::fs::File::from(reader)
            } else {
                std::fs::File::open("/dev/null").expect("open quiet reader")
            };
            owner
                .register_for_test_with_reader(
                    &run,
                    reader,
                    Box::new(WatchingChild),
                    watching_session(Arc::clone(&probes)),
                    control,
                    failure,
                    || {},
                )
                .map_err(|error| error.into_parts().0)
                .expect("register production pacing fixture");
            runs.push(run);
        }

        let baseline = owner.diagnostic_snapshot();
        let baseline_probes = probes.load(Ordering::Acquire);
        let mut writer = noisy_writer.expect("retain noisy writer");
        let producer = std::thread::spawn(move || {
            writer
                .write_all(&vec![7; OUTPUT_BYTES])
                .expect("write hostile output");
        });
        let deadline = Instant::now() + Duration::from_secs(4);
        while runs[0].info().latest_output_bytes < OUTPUT_BYTES as u64 {
            assert!(
                Instant::now() < deadline,
                "owner did not drain hostile output"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        producer.join().expect("join hostile output producer");
        let after = owner.diagnostic_snapshot();
        let lifecycle_ticks = after.lifecycle_probes - baseline.lifecycle_probes;
        let probe_calls = probes.load(Ordering::Acquire) - baseline_probes;
        assert!(
            lifecycle_ticks < 128,
            "lifecycle polling followed output chunks"
        );
        assert!(
            probe_calls <= lifecycle_ticks.saturating_add(1) * RUNS,
            "one output chunk triggered more than one complete lifecycle pass"
        );
    }

    // A leader probe that reports terminal only after `live` is cleared, and
    // counts every peek. Models a child that exits when we say so, so a peek
    // before the "exit" sees a live leader and a peek after sees a terminal one.
    fn event_driven_session(peeks: Arc<AtomicUsize>, live: Arc<AtomicUsize>) -> NativeSession {
        NativeSession::from_child_pid(42)
            .unwrap()
            .with_leader_probe_for_test(Arc::new(move || {
                peeks.fetch_add(1, Ordering::AcqRel);
                Ok(live.load(Ordering::Acquire) == 0)
            }))
    }

    #[test]
    fn idle_watched_runs_do_not_arm_the_timed_sweep() {
        // The measured cost was one waitid peek per watched Run every 20 ms. With
        // exit detection moved to the process-wide SIGCHLD relay (modelled here by
        // the absence of any wake), an idle watched Run must arm neither a timed
        // lifecycle pass nor a terminal peek, and the owner must sit blocked in
        // poll rather than spin — the wakeup no longer scales with Run count.
        const RUNS: usize = 128;

        let owner = NativeRunOwner::default();
        // Model the production configuration: `serve` marks the owner
        // signal-driven, which sheds the timed backstop. Without this the owner
        // would (correctly) keep the 20 ms backstop and this test would measure
        // the fallback path, not the event path it means to pin.
        owner.mark_signal_driven();
        let probes = Arc::new(AtomicUsize::new(0));
        let mut runs = Vec::with_capacity(RUNS);
        for _ in 0..RUNS {
            let failure = NativeWaitFailure::default();
            let id = RunId::new();
            let (control, run) = test_run(&owner, id, failure.clone());
            owner
                .register_for_test(
                    &run,
                    Box::new(WatchingChild),
                    watching_session(Arc::clone(&probes)),
                    control,
                    failure,
                    || {},
                )
                .map_err(|error| error.into_parts().0)
                .expect("register idle watched fixture");
            runs.push(run);
        }

        // Let every registration drain and the owner settle into its blocking
        // poll, then measure across a window many multiples of the old 20 ms
        // sweep. `register_for_test` opens /dev/null as the reader, which reports
        // EOF-readable once and is then dropped from the poll set, so a settled
        // idle owner holds only watched entries with no armed deadline.
        let deadline = Instant::now() + Duration::from_secs(2);
        while owner.diagnostic_snapshot().registrations < RUNS {
            assert!(Instant::now() < deadline, "owner drained all registrations");
            std::thread::sleep(Duration::from_millis(1));
        }
        let baseline = owner.diagnostic_snapshot();
        let baseline_probes = probes.load(Ordering::Acquire);
        std::thread::sleep(Duration::from_millis(400));
        let after = owner.diagnostic_snapshot();

        assert_eq!(
            probes.load(Ordering::Acquire) - baseline_probes,
            0,
            "an idle watched Run ran a terminal peek without any exit readiness"
        );
        // 400 ms across 128 idle Runs would be ~2500 timed wakeups under the old
        // sweep. The owner must be genuinely blocked in poll, not spinning: allow
        // a small constant for registration settle races only.
        assert!(
            after.poll_returns - baseline.poll_returns <= 2,
            "idle watched owner is not blocked in poll: {} extra poll returns",
            after.poll_returns - baseline.poll_returns
        );
        assert!(
            after.lifecycle_probes - baseline.lifecycle_probes <= 2,
            "idle watched owner ran timed lifecycle passes: {} extra",
            after.lifecycle_probes - baseline.lifecycle_probes
        );

        owner
            .shutdown(Instant::now() + Duration::from_secs(1))
            .expect("shut down idle watched owner");
    }

    #[test]
    fn a_wake_after_exit_detects_it_through_the_peek_then_reap_path() {
        // Prove the event path is load-bearing: with no wake the owner never peeks
        // (it would every 20 ms under the old sweep), and the moment a wake arrives
        // after the leader turns terminal, the same peek-then-reap path publishes
        // the exit. `owner_wake()` stands in for the SIGCHLD relay `serve` pokes.
        let owner = NativeRunOwner::default();
        // Production configuration: signal-driven, so the backstop is shed and a
        // peek happens only on a wake — which is exactly what this test asserts.
        owner.mark_signal_driven();
        let peeks = Arc::new(AtomicUsize::new(0));
        let live = Arc::new(AtomicUsize::new(1));
        let failure = NativeWaitFailure::default();
        let id = RunId::new();
        let (control, run) = test_run(&owner, id, failure.clone());
        owner
            .register_for_test(
                &run,
                Box::new(WatchingChild),
                event_driven_session(Arc::clone(&peeks), Arc::clone(&live)),
                control,
                failure,
                || {},
            )
            .map_err(|error| error.into_parts().0)
            .expect("register event-driven exit fixture");

        // Settle past registration, then confirm that without any further wake the
        // owner performs no terminal peek across several old-sweep periods. One
        // registration-edge peek is expected; nothing after it.
        let deadline = Instant::now() + Duration::from_secs(2);
        while owner.diagnostic_snapshot().registrations < 1 {
            assert!(Instant::now() < deadline, "owner drained the registration");
            std::thread::sleep(Duration::from_millis(1));
        }
        let settled_peeks = peeks.load(Ordering::Acquire);
        std::thread::sleep(Duration::from_millis(200));
        assert_eq!(
            peeks.load(Ordering::Acquire),
            settled_peeks,
            "owner peeked terminality without a wake (timed sweep still armed)"
        );
        assert!(run.info().state.is_running(), "Run exited before its wake");

        // Mark the leader terminal and wake the owner: the wake alone must drive
        // detection through the WNOWAIT peek and the sequenced reap.
        live.store(0, Ordering::Release);
        owner.owner_wake().wake();

        let deadline = Instant::now() + Duration::from_secs(2);
        while run.info().state.is_running() {
            assert!(
                Instant::now() < deadline,
                "a wake after exit did not drive terminal detection"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(
            matches!(run.info().state, ctxmux_protocol::RunState::Exited { .. }),
            "Run reached a non-exit terminal state: {:?}",
            run.info().state
        );
        assert!(
            peeks.load(Ordering::Acquire) > settled_peeks,
            "terminal publication bypassed the WNOWAIT peek"
        );

        owner
            .shutdown(Instant::now() + Duration::from_secs(1))
            .expect("shut down owner after event-driven exit");
    }

    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "one real two-child handoff fixture verifies original identities and live descriptor ownership before and after extract"
    )]
    fn extract_for_handoff_returns_live_descriptors_and_leaves_children_running() {
        use std::{collections::HashSet, process::Command};

        use portable_pty::{CommandBuilder, PtySize, native_pty_system};

        use crate::native_control::InputDrainGate;

        let owner = NativeRunOwner::default();
        let mut expected_run_ids = Vec::new();
        let mut pids = Vec::new();
        let mut runs = Vec::new();
        // Keep the slave ends alive so the real pty pairs are not torn down.
        let mut slaves = Vec::new();

        for _ in 0..2 {
            let id = RunId::new();
            let failure = NativeWaitFailure::default();
            let pair = native_pty_system()
                .openpty(PtySize {
                    rows: 24,
                    cols: 80,
                    pixel_width: 0,
                    pixel_height: 0,
                })
                .expect("open real pty for handoff fixture");
            let writer = std::fs::File::from(
                ctxmux_inherited_fd::duplicate_nonblocking_cloexec(
                    pair.master.as_raw_fd().expect("master fd"),
                )
                .expect("duplicate unbuffered PTY input"),
            );
            let mut command = CommandBuilder::new("/bin/sleep");
            command.arg("30");
            let child = pair
                .slave
                .spawn_command(command)
                .expect("spawn /bin/sleep on the pty slave");
            let pid = child.process_id().expect("real child exposes a pid");
            let session = NativeSession::from_child_pid(pid).expect("session from real child pid");
            let control = NativeControlOwner::new(
                id,
                pair.master,
                writer,
                InputDrainGate::default(),
                owner.owner_wake(),
            );
            let run =
                Run::new_native_for_owner_test(id, control.clone(), owner.clone(), failure.clone());
            owner
                .register_for_test(&run, child, session, control, failure, || {})
                .map_err(|error| error.into_parts().0)
                .expect("register real handoff fixture");
            expected_run_ids.push(id);
            pids.push(pid);
            runs.push(run);
            slaves.push(pair.slave);
        }

        let deadline = Instant::now() + Duration::from_secs(2);
        while owner.diagnostic_snapshot().registrations < 2 {
            assert!(
                Instant::now() < deadline,
                "owner did not drain the two registrations"
            );
            std::thread::sleep(Duration::from_millis(1));
        }

        let descriptors = owner
            .extract_for_handoff()
            .expect("live native entries are handoff-ready");
        assert_eq!(descriptors.len(), 2, "one descriptor per live native Run");
        for descriptor in &descriptors {
            assert!(
                descriptor.child_pid != 0,
                "handoff descriptor carries a pid"
            );
            assert!(
                descriptor.master_fd >= 0,
                "handoff descriptor carries a live master fd"
            );
        }
        let returned_ids: HashSet<RunId> = descriptors.iter().map(|d| d.run_id).collect();
        let expected_ids: HashSet<RunId> = expected_run_ids.iter().copied().collect();
        assert_eq!(returned_ids, expected_ids, "descriptors cover both Runs");

        for pid in &pids {
            assert!(
                Command::new("kill")
                    .args(["-0", &pid.to_string()])
                    .status()
                    .expect("probe child liveness with kill -0")
                    .success(),
                "child {pid} must survive the handoff extraction"
            );
        }

        // Handoff extraction is single-shot: it relinquishes each live Run
        // (Watching -> Done, forgetting the child and control handles), so a
        // second extraction on the same owner surfaces nothing. This pins the
        // "descriptors transfer exactly once" contract the incoming image
        // relies on — a Run cannot be handed to two successors.
        let second = owner
            .extract_for_handoff()
            .expect("already-extracted owner remains readable");
        assert!(
            second.is_empty(),
            "a second extraction relinquishes nothing; got {} descriptors",
            second.len()
        );

        for pid in &pids {
            let _ = Command::new("kill")
                .args(["-KILL", &pid.to_string()])
                .status();
        }
        owner
            .shutdown(Instant::now() + Duration::from_secs(2))
            .expect("shut down owner after handoff");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(
        clippy::too_many_lines,
        reason = "one fixture keeps the two-entry preflight and no-prefix-relinquish proof auditable"
    )]
    async fn handoff_preflight_rejects_all_entries_before_relinquishing_any_owner() {
        use std::process::Command;

        use portable_pty::{CommandBuilder, PtySize, native_pty_system};

        use crate::native_control::InputDrainGate;

        struct BlockingWriter {
            started: mpsc::SyncSender<()>,
            release: Arc<(Mutex<bool>, Condvar)>,
        }

        impl Write for BlockingWriter {
            fn write(&mut self, data: &[u8]) -> io::Result<usize> {
                let _ = self.started.send(());
                let (released, changed) = &*self.release;
                let mut released = released
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                while !*released {
                    released = changed
                        .wait(released)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                }
                Ok(data.len())
            }

            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        let owner = NativeRunOwner::default();
        let mut runs = Vec::new();
        let mut controls = Vec::new();
        let mut pids = Vec::new();
        let mut slaves = Vec::new();
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let release = Arc::new((Mutex::new(false), Condvar::new()));

        for index in 0..2 {
            let id = RunId::new();
            let failure = NativeWaitFailure::default();
            let pair = native_pty_system()
                .openpty(PtySize {
                    rows: 24,
                    cols: 80,
                    pixel_width: 0,
                    pixel_height: 0,
                })
                .expect("open real pty for all-owner preflight fixture");
            let mut command = CommandBuilder::new("/bin/sleep");
            command.arg("30");
            let child = pair
                .slave
                .spawn_command(command)
                .expect("spawn handoff preflight child");
            let pid = child.process_id().expect("handoff child exposes pid");
            let session = NativeSession::from_child_pid(pid).expect("bind real child session");
            let writer: Box<dyn Write + Send> = if index == 0 {
                Box::new(io::sink())
            } else {
                Box::new(BlockingWriter {
                    started: started_tx.clone(),
                    release: Arc::clone(&release),
                })
            };
            let control = NativeControlOwner::new_opaque_for_owner_test(
                id,
                pair.master,
                writer,
                InputDrainGate::default(),
                owner.owner_wake(),
            );
            let run =
                Run::new_native_for_owner_test(id, control.clone(), owner.clone(), failure.clone());
            owner
                .register_for_test(&run, child, session, control.clone(), failure, || {})
                .map_err(|error| error.into_parts().0)
                .expect("register handoff preflight fixture");
            runs.push(run);
            controls.push(control);
            pids.push(pid);
            slaves.push(pair.slave);
        }
        let deadline = Instant::now() + Duration::from_secs(2);
        while owner.diagnostic_snapshot().registrations < 2 {
            assert!(Instant::now() < deadline, "owner drains registrations");
            std::thread::sleep(Duration::from_millis(1));
        }

        let pending = controls[1]
            .begin_recoverable_input(
                InputOperationKey::new("runtime-handoff-pending").unwrap(),
                0,
                b"A".to_vec(),
            )
            .expect("admit crossing mutation");
        started_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("crossing mutation reaches blocking writer");
        let failure = owner
            .extract_for_handoff()
            .expect_err("one crossing owner rejects the complete extraction set");
        assert!(failure.contains(&controls[1].run_id().to_string()));

        // The first entry passed preflight before the second failed. It must
        // still own its PTY, proving validation did not relinquish a prefix.
        controls[0]
            .resize(TerminalSize { rows: 25, cols: 81 }, |_| {})
            .expect("earlier owner remains live after later preflight failure");
        for pid in &pids {
            assert!(
                Command::new("kill")
                    .args(["-0", &pid.to_string()])
                    .status()
                    .expect("probe child after rejected extraction")
                    .success()
            );
        }

        let (released, changed) = &*release;
        *released
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        changed.notify_all();
        pending.resolve().await.expect("crossing mutation settles");
        let deadline = Instant::now() + Duration::from_secs(2);
        while controls[1].handoff_input_state().is_err() {
            assert!(Instant::now() < deadline, "input owner becomes settled");
            tokio::task::yield_now().await;
        }
        assert_eq!(
            owner
                .extract_for_handoff()
                .expect("both complete owners extract together")
                .len(),
            2
        );

        for pid in &pids {
            let _ = Command::new("kill")
                .args(["-KILL", &pid.to_string()])
                .status();
        }
        owner
            .shutdown(Instant::now() + Duration::from_secs(2))
            .expect("stop owner after successful extraction");
    }
    async fn start_public_echo_run(
        client: &ctxmux_client::Client,
        marker: &[u8],
    ) -> (ctxmux_protocol::RunInfo, ctxmux_client::Attachment) {
        use ctxmux_client::replay_bytes;
        use ctxmux_protocol::RunSpec;
        let info = client
            .start(RunSpec {
                program: "/bin/sh".to_owned(),
                args: vec![
                    "-c".to_owned(),
                    "stty raw -echo; printf READY; exec /bin/cat".to_owned(),
                ],
                cwd: None,
                env: std::collections::BTreeMap::new(),
                initial_size: TerminalSize::default(),
                declared_inputs: Vec::new(),
            })
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while client.status(info.id).await.unwrap().latest_output_bytes < 5 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        let receipt = client.input(info.id, marker.to_vec()).await.unwrap();
        assert_eq!(
            usize::try_from(receipt.receipt.written_bytes).unwrap(),
            marker.len()
        );
        let expected = [b"READY".as_slice(), marker].concat();
        tokio::time::timeout(Duration::from_secs(5), async {
            while client.status(info.id).await.unwrap().latest_output_bytes
                < u64::try_from(expected.len()).unwrap()
            {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        let (attachment, snapshot) = client.attach(info.id, 0).await.unwrap();
        assert_eq!(replay_bytes(&snapshot.replay.chunks), expected);
        assert_eq!((snapshot.run.id, snapshot.run.pid), (info.id, info.pid));
        (info, attachment)
    }

    fn borrow_owner_commands(owner: &NativeRunOwner) -> mpsc::SyncSender<super::OwnerCommand> {
        let state = crate::mutex_lock(&owner.inner.state);
        let super::OwnerState::Running { commands, .. } = &*state else {
            panic!("live owner")
        };
        commands.clone()
    }

    async fn await_owner_retirement(owner: &NativeRunOwner) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while !owner.inner.completion_finished.load(Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
    }

    fn read_private_retained_output(
        run: &Arc<Run>,
        reader: &mut std::fs::File,
        bytes: &mut Vec<u8>,
        calls: &mut usize,
    ) -> bool {
        use std::io::Read;
        let mut buffer = [0_u8; super::OUTPUT_READ_BUFFER_BYTES];
        loop {
            *calls += 1;
            match reader.read(&mut buffer) {
                Ok(0) => return true,
                Ok(read) => {
                    bytes.extend_from_slice(&buffer[..read]);
                    run.record_output(buffer[..read].to_vec());
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => return false,
                Err(error)
                    if error.raw_os_error() == Some(rustix::io::Errno::IO.raw_os_error()) =>
                {
                    return true;
                }
                Err(error) => panic!("actual retained PTY read failed: {error}"),
            }
        }
    }

    fn await_private_output_tail(
        run: &Arc<Run>,
        reader: &mut std::fs::File,
        bytes: &mut Vec<u8>,
        calls: &mut usize,
    ) {
        let deadline = Instant::now() + super::OUTPUT_DRAIN_TIMEOUT;
        while !read_private_retained_output(run, reader, bytes, calls) {
            assert!(
                Instant::now() < deadline,
                "private reaped child left an unsettled output tail"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    fn cleanup_with_actual_retained_reader(
        run: &Arc<Run>,
        control: &NativeControlOwner,
        mut session: NativeSession,
        reader: &mut std::fs::File,
        read_enabled: bool,
        expected_raw: Option<&[u8]>,
    ) -> (Result<(), String>, NativeSession) {
        let before = ctxmux_client::replay_bytes(&run.lock_owner(&run.output).replay(0).chunks);
        let actual_control = control.clone();
        let cleanup_worker = std::thread::spawn(move || {
            let started = Instant::now();
            let result = actual_control.cleanup_retained_owner_child_for_test(&mut session);
            (result, session, started.elapsed())
        });
        let mut bytes = Vec::new();
        let mut calls = 0;
        while !cleanup_worker.is_finished() {
            if read_enabled {
                read_private_retained_output(run, reader, &mut bytes, &mut calls);
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        let (result, mut session, elapsed) = cleanup_worker.join().unwrap();
        println!(
            "{}",
            serde_json::json!({"first_cleanup_result": result,
            "first_cleanup_elapsed_us": elapsed.as_micros(),
            "read_enabled": read_enabled, "read_calls_before_first_result": calls,
            "exact_bytes_before_first_result": bytes, "private_extra_threads": 1,
            "private_extra_reader_fds": 1})
        );
        // A first success still joins the entire actual output tail. A failed
        // inverse can clean its private child, but retains its first failure.
        let deadline = Instant::now() + super::OUTPUT_DRAIN_TIMEOUT;
        while !read_private_retained_output(run, reader, &mut bytes, &mut calls) {
            if result.is_err() {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "successful Stop left an unsettled output tail"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        if result.is_err() {
            let retry = control.cleanup_retained_owner_child_for_test(&mut session);
            println!(
                "{}",
                serde_json::json!({"private_inverse_cleanup_retry": retry})
            );
            if retry.is_ok() {
                await_private_output_tail(run, reader, &mut bytes, &mut calls);
            }
        }
        let replay = ctxmux_client::replay_bytes(&run.lock_owner(&run.output).replay(0).chunks);
        println!(
            "{}",
            serde_json::json!({"retained_real_raw_replay": replay,
            "all_actual_private_read_bytes": bytes, "all_read_calls": calls,
            "first_cleanup_result_retained": result})
        );
        assert_eq!(
            replay,
            [before, bytes].concat(),
            "every actual byte is admitted once and in order"
        );
        if let Some(expected) = expected_raw {
            assert_eq!(replay, expected);
        }
        (result, session)
    }

    fn cleanup_actual_retained_child(run: &Arc<Run>, expected_raw: Option<&[u8]>) {
        let info = run.info();
        let Some(crate::RunControl::Native(control)) = &run.incarnation_control else {
            panic!("Native control")
        };
        let session = NativeSession::from_child_pid(info.pid.unwrap()).unwrap();
        assert!(!session.leader_is_terminal().unwrap());
        let mut reader = control.retained_pty_reader_for_test().unwrap();
        let (first_result, _) = cleanup_with_actual_retained_reader(
            run,
            control,
            session,
            &mut reader,
            true,
            expected_raw,
        );
        first_result.expect(
            "actual first private cleanup with its output reader and unchanged Stop budgets",
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn public_clients_observe_owner_loss_before_producer_release() {
        use ctxmux_client::Client;
        use ctxmux_protocol::{NativeOwnerStatus, NativeServiceFailure, RunEvent};

        let manager = Arc::new(crate::RunManager::default());
        let server = crate::tests::InProcessServer::start(Arc::clone(&manager));
        let second_client = Client::new(server.directory.path().join("ctxmux.sock"));
        let runtime = server.client.runtime_info().await.unwrap();
        assert_eq!(runtime, second_client.runtime_info().await.unwrap());
        let mut infos = Vec::new();
        let mut attachments = Vec::new();
        for (client, marker) in [
            (&server.client, b"FIRST\n".as_slice()),
            (&second_client, b"SECOND\n".as_slice()),
        ] {
            let (info, attachment) = start_public_echo_run(client, marker).await;
            attachments.push(attachment);
            infos.push(info);
        }
        assert_ne!(infos[0].id, infos[1].id);
        assert_ne!(infos[0].pid, infos[1].pid);
        let owner = &manager.native_runs;
        let commands = borrow_owner_commands(owner);
        commands.send(super::OwnerCommand::UnwindForTest).unwrap();
        owner.owner_wake().wake();
        let observed = tokio::time::timeout(Duration::from_secs(2), async {
            for (client, info, attachment) in [
                (&server.client, &infos[0], &attachments[0]),
                (&second_client, &infos[1], &attachments[1]),
            ] {
                loop {
                    if let Some(RunEvent::ServiceChanged { service }) =
                        attachment.next_event().await.unwrap()
                        && matches!(
                            service.owner,
                            NativeOwnerStatus::Stopped {
                                reason: NativeServiceFailure::OwnerUnwound
                            }
                        )
                    {
                        break;
                    }
                }
                let status = client.status(info.id).await.unwrap();
                assert_eq!((status.id, status.pid), (info.id, info.pid));
                assert!(status.state.is_running());
                assert_eq!(
                    status.native_service.unwrap().owner,
                    NativeOwnerStatus::Stopped {
                        reason: NativeServiceFailure::OwnerUnwound
                    }
                );
            }
        })
        .await;
        let physically_finished_before_release =
            owner.inner.completion_finished.load(Ordering::Acquire);
        drop(commands);
        await_owner_retirement(owner).await;
        for ((info, attachment), marker) in infos
            .iter()
            .zip(attachments)
            .zip([b"FIRST\n".as_slice(), b"SECOND\n".as_slice()])
        {
            attachment.detach().await.unwrap();
            let (_, replay) = second_client.attach(info.id, 0).await.unwrap();
            assert_eq!((replay.run.id, replay.run.pid), (info.id, info.pid));
            let run = manager.get(info.id).unwrap();
            let expected = [b"READY".as_slice(), marker].concat();
            cleanup_actual_retained_child(&run, Some(&expected));
        }
        println!(
            "{}",
            serde_json::json!({
                "public_clients": 2, "actual_runs": 2, "ordered_peer_echo_bytes": [11, 12],
                "public_events_status_before_release": observed.is_ok(),
                "physical_retirement_before_release": physically_finished_before_release,
                "actual_private_children_cleaned": 2
            })
        );
        assert!(
            observed.is_ok(),
            "public events and Status waited for producer release"
        );
        assert!(
            !physically_finished_before_release,
            "pending producer lifetime was hidden"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn extracted_stop_settles_before_producer_release() {
        let owner = NativeRunOwner::default();
        let admission = owner.cleanup_admission();
        let permits = (0..crate::ResourceLimits::DEFAULT.cleanup_workers)
            .map(|_| admission.try_acquire().unwrap())
            .collect::<Vec<_>>();
        assert!(admission.try_acquire().is_none());
        let run = spawn_actual_service_run(
            &owner,
            "stty raw -echo; printf READY; exec /bin/cat",
            NativeWaitFailure::default(),
        );
        let control = match &run.incarnation_control {
            Some(crate::RunControl::Native(control)) => control.clone(),
            _ => panic!("Native control"),
        };
        let stop = control.begin_stop().unwrap();
        let commands = borrow_owner_commands(&owner);
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let (tx, rx) = mpsc::channel();
            commands
                .send(super::OwnerCommand::ProbePendingStopForTest {
                    run_id: run.id,
                    respond: tx,
                })
                .unwrap();
            owner.owner_wake().wake();
            if rx.recv_timeout(Duration::from_secs(1)).unwrap() {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "real Stop did not enter Watching.pending_stop"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        commands.send(super::OwnerCommand::UnwindForTest).unwrap();
        owner.owner_wake().wake();
        let mut result = Box::pin(stop.resolve(Duration::from_secs(5)));
        let before_release = tokio::time::timeout(Duration::from_secs(2), &mut result).await;
        let settled_before_release = before_release.is_ok();
        drop(commands);
        await_owner_retirement(&owner).await;
        let failure = match before_release {
            Ok(result) => result.unwrap_err(),
            Err(_) => result.await.unwrap_err(),
        };
        let info = run.info();
        assert!(info.state.is_running());
        let mut session = NativeSession::from_child_pid(info.pid.unwrap()).unwrap();
        assert!(!session.leader_is_terminal().unwrap());
        println!(
            "{}",
            serde_json::json!({"private_stop_cleanup_start": info.pid,
            "stop_settled_before_release": settled_before_release, "failure": failure,
            "original_stop_admission_budget_ms": crate::ResourceLimits::DEFAULT.stop_admission_timeout_ms})
        );
        let mut reader = control.retained_pty_reader_for_test().unwrap();
        let (cleanup, returned_session) =
            cleanup_with_actual_retained_reader(&run, &control, session, &mut reader, true, None);
        session = returned_session;
        println!(
            "{}",
            serde_json::json!({"private_stop_cleanup_result": cleanup,
            "private_pid": info.pid, "leader_terminal_after_cleanup": session.leader_is_terminal()})
        );
        drop(permits);
        println!(
            "{}",
            serde_json::json!({"actual_extracted_pending_stop": true,
            "stop_settled_before_release": settled_before_release, "failure": failure,
            "original_child_alive_before_cleanup": true, "actual_private_children_cleaned": usize::from(cleanup.is_ok()),
            "original_stop_admission_budget_ms": crate::ResourceLimits::DEFAULT.stop_admission_timeout_ms})
        );
        assert!(
            settled_before_release,
            "extracted Stop result waited for producer release"
        );
        assert_eq!(failure.disposition, CommandDisposition::NotApplied);
        assert_eq!(failure.error.code, ErrorCode::BackendUnavailable);
        assert_eq!(failure.confirmed_input_bytes, None);
        cleanup.expect("actual private child cleanup with unchanged Stop budgets");
    }
    fn capture_actual_registration(
        owner: &NativeRunOwner,
        commands: &mpsc::SyncSender<super::OwnerCommand>,
    ) -> (Arc<Run>, super::NativeRunRegistration) {
        // Capture an actual spawn registration at the producer's accepted-send
        // boundary, while its real owner thread and channel remain alive.
        let (capture, registrations) =
            mpsc::sync_channel(crate::ResourceLimits::DEFAULT.creation_workers);
        {
            let mut state = crate::mutex_lock(&owner.inner.state);
            let super::OwnerState::Running { commands, .. } = &mut *state else {
                panic!("live owner")
            };
            *commands = capture;
        }
        let late = spawn_actual_service_run(
            owner,
            "stty raw -echo; printf READY; exec /bin/cat",
            NativeWaitFailure::default(),
        );
        let super::OwnerCommand::Register(registration) = registrations.recv().unwrap() else {
            panic!("real registration")
        };
        {
            let mut state = crate::mutex_lock(&owner.inner.state);
            let super::OwnerState::Running {
                commands: retained, ..
            } = &mut *state
            else {
                panic!("live owner")
            };
            *retained = commands.clone();
        }
        (late, registration)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn accepted_late_registration_is_fenced_before_producer_release() {
        use ctxmux_protocol::{NativeOwnerStatus, NativeServiceFailure};
        let owner = NativeRunOwner::default();
        let first = spawn_actual_service_run(
            &owner,
            "stty raw -echo; printf READY; exec /bin/cat",
            NativeWaitFailure::default(),
        );
        let commands = borrow_owner_commands(&owner);
        let (late, registration) = capture_actual_registration(&owner, &commands);
        commands.send(super::OwnerCommand::UnwindForTest).unwrap();
        owner.owner_wake().wake();
        tokio::time::timeout(Duration::from_secs(2), async {
            while crate::mutex_lock(&owner.inner.completion).is_none() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        commands
            .send(super::OwnerCommand::Register(registration))
            .unwrap();
        let (ready_tx, ready_rx) = mpsc::channel();
        commands
            .send(super::OwnerCommand::HandoffReady {
                run_id: late.id,
                respond: ready_tx,
            })
            .unwrap();
        let preflight_calls = Arc::new(AtomicUsize::new(0));
        let calls = Arc::clone(&preflight_calls);
        let (extract_tx, extract_rx) = mpsc::channel();
        commands
            .send(super::OwnerCommand::ExtractForHandoff {
                preflight: Box::new(move |_| {
                    calls.fetch_add(1, Ordering::AcqRel);
                    Ok(())
                }),
                respond: extract_tx,
            })
            .unwrap();
        let extract = extract_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(
            extract
                .unwrap_err()
                .contains("stopped before handoff extraction")
        );
        assert!(matches!(
            ready_rx.recv_timeout(Duration::from_secs(2)),
            Err(mpsc::RecvTimeoutError::Disconnected)
        ));
        assert_eq!(preflight_calls.load(Ordering::Acquire), 0);
        let service_before_release = late.native_service.as_ref().unwrap().snapshot();
        let Some(crate::RunControl::Native(control)) = &late.incarnation_control else {
            panic!("Native control")
        };
        let new_input_refused = control.begin_input(b"must-refuse".to_vec()).is_err();
        let physical_retirement_before_release =
            owner.inner.completion_finished.load(Ordering::Acquire);
        let ids = [first.info(), late.info()];
        assert_ne!(ids[0].id, ids[1].id);
        assert_ne!(ids[0].pid, ids[1].pid);
        drop(commands);
        await_owner_retirement(&owner).await;
        cleanup_actual_retained_child(&first, None);
        cleanup_actual_retained_child(&late, None);
        println!(
            "{}",
            serde_json::json!({"actual_late_registration": true,
            "service_before_release": service_before_release,
            "new_input_refused_before_release": new_input_refused,
            "physical_retirement_before_release": physical_retirement_before_release,
            "handoff_preflight_calls": 0, "actual_private_children_cleaned": 2})
        );
        assert_eq!(
            service_before_release.owner,
            NativeOwnerStatus::Stopped {
                reason: NativeServiceFailure::OwnerUnwound
            }
        );
        assert!(new_input_refused);
        assert!(!physical_retirement_before_release);
    }
}
