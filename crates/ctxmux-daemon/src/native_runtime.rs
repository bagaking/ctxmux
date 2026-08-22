//! Daemon-wide ownership of native PTY output and child lifecycle work.

use std::{
    collections::{HashMap, VecDeque},
    fs::File,
    io::{self, Read, Write},
    os::unix::net::UnixStream,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

use ctxmux_protocol::{
    CommandDisposition, ControlFailure, ErrorCode, ProtocolError, RunId, RunState,
};
use portable_pty::Child;
use rustix::{
    event::{PollFd, PollFlags, Timespec, poll},
    io::Errno,
};

use crate::{
    CHILD_CONTROL_POLL, NativeWaitFailure, PendingChild, Run, STOP_FORCED_TIMEOUT,
    STOP_GRACEFUL_TIMEOUT, exit_state, mutex_lock,
    native_control::{ChildCommand, NativeControlOwner, StopOwnerResult},
    native_session::NativeSession,
    qualification_stats::GaugeGuard,
};

const REGISTRATION_CAPACITY: usize = 8;
const CLEANUP_MAX_ACTIVE: usize = 8;
const OUTPUT_DRAIN_TIMEOUT: Duration = Duration::from_secs(1);
const OUTPUT_READ_BUFFER_BYTES: usize = 8192;

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
                eprintln!("ctxmuxd native owner wake failed: {error}");
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
    state: Mutex<OwnerState>,
    wake: OwnerWake,
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
/// The fields have no production reader until the SIGHUP path lands; they are
/// read only through the `#[cfg(test)]` handoff test today.
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, Copy)]
pub(crate) struct LiveDescriptors {
    pub run_id: RunId,
    pub child_pid: u32,
    pub master_fd: std::os::fd::RawFd,
}

enum OwnerCommand {
    Register(NativeRunRegistration),
    ExtractForHandoff {
        respond: mpsc::Sender<Vec<LiveDescriptors>>,
    },
    Shutdown,
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
        let control = self.control.clone();
        NativeEntry {
            run_id: control.run_id(),
            run: self.run.clone(),
            output: Some(OutputOwner {
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
        let (commands, receiver) = mpsc::sync_channel(REGISTRATION_CAPACITY);
        let (wake, wake_reader) = match OwnerWake::pair() {
            Ok(pair) => pair,
            Err(error) => {
                return Self {
                    inner: Arc::new(OwnerInner {
                        state: Mutex::new(OwnerState::Failed(format!(
                            "failed to create daemon-wide native owner wake pipe: {error}"
                        ))),
                        wake: OwnerWake::unavailable(),
                        cleanup_admission: CleanupAdmission::new(
                            CLEANUP_MAX_ACTIVE,
                            OwnerWake::unavailable(),
                        ),
                        diagnostics: Arc::new(OwnerDiagnostics::default()),
                    }),
                };
            }
        };
        let cleanup_admission = CleanupAdmission::new(CLEANUP_MAX_ACTIVE, wake.clone());
        let diagnostics = Arc::new(OwnerDiagnostics::default());
        let owner_wake = wake.clone();
        let owner_cleanup = cleanup_admission.clone();
        let owner_diagnostics = Arc::clone(&diagnostics);
        let state = match thread::Builder::new()
            .name("ctxmux-native-owner".to_owned())
            .spawn(move || {
                owner_main(
                    &receiver,
                    wake_reader,
                    &owner_wake,
                    &owner_cleanup,
                    &owner_diagnostics,
                );
            }) {
            Ok(thread) => OwnerState::Running { commands, thread },
            Err(error) => {
                OwnerState::Failed(format!("failed to start daemon-wide native owner: {error}"))
            }
        };
        Self {
            inner: Arc::new(OwnerInner {
                state: Mutex::new(state),
                wake,
                cleanup_admission,
                diagnostics,
            }),
        }
    }
}

impl NativeRunOwner {
    pub(crate) fn owner_wake(&self) -> OwnerWake {
        self.inner.wake.clone()
    }

    #[cfg(test)]
    pub(crate) fn cleanup_admission(&self) -> CleanupAdmission {
        self.inner.cleanup_admission.clone()
    }

    pub(crate) fn register(
        &self,
        registration: NativeRunRegistration,
    ) -> Result<(), NativeRegistrationError> {
        let state = mutex_lock(&self.inner.state);
        match &*state {
            OwnerState::Running { commands, .. } => {
                let result = commands.send(OwnerCommand::Register(registration));
                self.inner.wake.wake();
                result.map_err(|error| NativeRegistrationError {
                    message: "daemon-wide native owner stopped before registration".to_owned(),
                    registration: Box::new(match error.0 {
                        OwnerCommand::Register(registration) => registration,
                        OwnerCommand::ExtractForHandoff { .. } | OwnerCommand::Shutdown => {
                            unreachable!("registration send returns its registration")
                        }
                    }),
                })
            }
            OwnerState::Failed(message) => Err(NativeRegistrationError {
                message: message.clone(),
                registration: Box::new(registration),
            }),
        }
    }

    /// Return the live pty master fd and child pid for every watched native
    /// Run, relinquishing the owner's reap/close authority for each so the
    /// child survives (unreaped) and its master fd stays open past a future
    /// exec-in-place. No production caller exists until the SIGHUP path lands.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn extract_for_handoff(&self) -> Vec<LiveDescriptors> {
        let commands = {
            let state = mutex_lock(&self.inner.state);
            match &*state {
                OwnerState::Running { commands, .. } => commands.clone(),
                OwnerState::Failed(_) => return Vec::new(),
            }
        };
        let (tx, rx) = mpsc::channel();
        if commands
            .send(OwnerCommand::ExtractForHandoff { respond: tx })
            .is_err()
        {
            return Vec::new();
        }
        self.inner.wake.wake();
        rx.recv().unwrap_or_default()
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
        let mut state = mutex_lock(&self.inner.state);
        let OwnerState::Running {
            commands, thread, ..
        } = &*state
        else {
            return Ok(());
        };
        let _ = commands.send(OwnerCommand::Shutdown);
        self.inner.wake.wake();
        while !thread.is_finished() && Instant::now() < deadline {
            thread::sleep(
                Duration::from_millis(1).min(deadline.saturating_duration_since(Instant::now())),
            );
        }
        let finished = thread.is_finished();
        let previous = std::mem::replace(
            &mut *state,
            OwnerState::Failed("native owner stopped".to_owned()),
        );
        let OwnerState::Running { thread, .. } = previous else {
            unreachable!("native owner state was checked under the same lock")
        };
        if finished {
            let _ = thread.join();
            Ok(())
        } else {
            drop(thread);
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
        let state = self
            .state
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let OwnerState::Running { commands, thread } =
            std::mem::replace(state, OwnerState::Failed("native owner stopped".to_owned()))
        else {
            return;
        };
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

struct NativeEntry {
    run_id: RunId,
    run: Weak<Run>,
    output: Option<OutputOwner>,
    lifecycle: Lifecycle,
    after_wait: Option<AfterWait>,
    wait_failure: NativeWaitFailure,
    terminal: Option<PendingTerminal>,
}

struct OutputOwner {
    reader: File,
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

struct Watching {
    child: Box<dyn Child + Send + Sync>,
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
    permit: CleanupPermit,
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
    permit: CleanupPermit,
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
    _permit: CleanupPermit,
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
        permit: CleanupPermit,
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
    }
}

fn owner_main(
    commands: &mpsc::Receiver<OwnerCommand>,
    mut wake_reader: UnixStream,
    wake: &OwnerWake,
    cleanup_admission: &CleanupAdmission,
    diagnostics: &OwnerDiagnostics,
) {
    let (completion_tx, completion_rx) = mpsc::channel();
    let mut entries = Vec::<NativeEntry>::new();
    let mut queued = VecDeque::<WorkerJob>::new();
    let mut active = HashMap::<u64, thread::JoinHandle<()>>::new();
    let mut next_job_id = 0_u64;
    let mut next_lifecycle_probe = Instant::now();
    let mut owner_woken = true;

    loop {
        if owner_woken && drain_commands(commands, &mut entries, diagnostics) {
            detach_active_workers(&mut active);
            drain_completions(&completion_rx, &mut entries, &mut active);
            preserve_shutdown_authority(&mut entries, &mut queued);
            return;
        }
        if owner_woken {
            drain_completions(&completion_rx, &mut entries, &mut active);
            drive_lifecycle(&mut entries, &mut queued, cleanup_admission, false);
        }
        let now = Instant::now();
        if now >= next_lifecycle_probe {
            diagnostics.lifecycle_probes.fetch_add(1, Ordering::AcqRel);
            drive_lifecycle(&mut entries, &mut queued, cleanup_admission, true);
            next_lifecycle_probe = now + CHILD_CONTROL_POLL;
        }
        start_worker_jobs(
            &mut queued,
            &mut active,
            &completion_tx,
            wake,
            diagnostics,
            &mut entries,
            &mut next_job_id,
        );
        queue_ready_terminals(&mut entries, &mut queued);
        start_worker_jobs(
            &mut queued,
            &mut active,
            &completion_tx,
            wake,
            diagnostics,
            &mut entries,
            &mut next_job_id,
        );
        entries.retain(|entry| {
            entry.output.is_some()
                || !matches!(
                    entry.lifecycle,
                    Lifecycle::Done | Lifecycle::AuthorityLost(_)
                )
                || entry.terminal.is_some()
        });
        owner_woken = poll_and_read_outputs(
            &mut entries,
            &mut wake_reader,
            next_lifecycle_probe,
            diagnostics,
        );
    }
}

fn drain_commands(
    commands: &mpsc::Receiver<OwnerCommand>,
    entries: &mut Vec<NativeEntry>,
    diagnostics: &OwnerDiagnostics,
) -> bool {
    loop {
        match commands.try_recv() {
            Ok(OwnerCommand::Register(registration)) => {
                entries.push(registration.into_entry());
                diagnostics.registrations.fetch_add(1, Ordering::AcqRel);
            }
            Ok(OwnerCommand::ExtractForHandoff { respond }) => {
                let _ = respond.send(extract_live_descriptors(entries));
            }
            Ok(OwnerCommand::Shutdown) | Err(mpsc::TryRecvError::Disconnected) => return true,
            Err(mpsc::TryRecvError::Empty) => return false,
        }
    }
}

/// Collect the live pty master fd + child pid for every watched Run and
/// relinquish the owner's reap/close authority so both survive a future exec.
/// Mirrors `retain_unwaited_child`'s authority discipline (`mem::forget` the
/// control), but handoff is not a failure: no `wait_failure.record` and no
/// `mark_wait_authority_lost` — just forget child and control so each lives on.
fn extract_live_descriptors(entries: &mut [NativeEntry]) -> Vec<LiveDescriptors> {
    let mut descriptors = Vec::new();
    for entry in entries {
        // Peek before replacing: only Runs that are actually live for handoff
        // (still watching, master fd present, child pid known) are relinquished.
        let Lifecycle::Watching(watching) = &entry.lifecycle else {
            continue;
        };
        let Some(master_fd) = watching.control.master_raw_fd() else {
            continue;
        };
        let Some(child_pid) = watching.child.process_id() else {
            continue;
        };
        descriptors.push(LiveDescriptors {
            run_id: entry.run_id,
            child_pid,
            master_fd,
        });
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
    }
    descriptors
}

#[allow(
    clippy::too_many_lines,
    reason = "one production state transition keeps command, natural-exit, and cleanup-admission ordering auditable"
)]
fn drive_lifecycle(
    entries: &mut [NativeEntry],
    queued: &mut VecDeque<WorkerJob>,
    cleanup_admission: &CleanupAdmission,
    probe_lifecycle: bool,
) {
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
                    permit,
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
        let mut cleanup = None::<(CleanupKind, CleanupPermit)>;
        for command in watching.control.drain_child_commands() {
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
                permit,
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
            watching.control.reject_pending_stop();
            let _ = pending
                .reply
                .send(StopOwnerResult::Rejected(stop_admission_failure(
                    run_id,
                    "all eight native cleanup owners remained occupied through Stop admission",
                )));
        }
        if watching.pending_stop.is_some()
            && let Some(permit) = cleanup_admission.try_acquire()
        {
            let pending = watching
                .pending_stop
                .take()
                .expect("pending Stop remains present before commit");
            match watching.control.commit_pending_stop() {
                Ok(()) => {
                    queued.push_back(WorkerJob::Cleanup(CleanupJob {
                        run_id,
                        watching,
                        kind: CleanupKind::Stop(pending.reply),
                        after_wait: entry.after_wait.take(),
                        permit,
                    }));
                    continue;
                }
                Err(failure) => {
                    let _ = pending.reply.send(StopOwnerResult::Rejected(failure));
                }
            }
        }

        if !probe_lifecycle {
            entry.lifecycle = Lifecycle::Watching(watching);
            continue;
        }

        match watching.session.leader_is_terminal() {
            Ok(false) => entry.lifecycle = Lifecycle::Watching(watching),
            Ok(true) => {
                let Some(permit) = cleanup_admission.try_acquire() else {
                    entry.lifecycle = Lifecycle::Watching(watching);
                    continue;
                };
                let pending = watching.control.fence_child_commands();
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
                    permit,
                }));
            }
            Err(error) => {
                watching
                    .control
                    .mark_wait_authority_lost(error.clone(), watching.child);
                watching.wait_failure.record(run_id, &error);
                entry.after_wait.take();
                entry.lifecycle = Lifecycle::AuthorityLost(watching.control);
            }
        }
    }
}

fn start_worker_jobs(
    queued: &mut VecDeque<WorkerJob>,
    active: &mut HashMap<u64, thread::JoinHandle<()>>,
    completion_tx: &mpsc::Sender<CleanupCompletion>,
    wake: &OwnerWake,
    diagnostics: &OwnerDiagnostics,
    entries: &mut [NativeEntry],
    next_job_id: &mut u64,
) {
    while active.len() < CLEANUP_MAX_ACTIVE {
        let Some(job) = queued.pop_front() else {
            return;
        };
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
    let result = match &mut job.kind {
        CleanupKind::Stop(_) | CleanupKind::Unpublished => job.watching.session.stop(
            job.watching.child.as_mut(),
            STOP_GRACEFUL_TIMEOUT,
            STOP_FORCED_TIMEOUT,
        ),
        CleanupKind::Natural { .. } => job
            .watching
            .session
            .finish_after_direct_exit(
                job.watching.child.as_mut(),
                Instant::now() + STOP_FORCED_TIMEOUT,
            )
            .map(|(status, disposition)| (disposition, status)),
    };

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
                permit: job.permit,
            }
        }
        Err(error) => match job.kind {
            CleanupKind::Natural { stop } => {
                if let Some(reply) = stop {
                    let _ = reply.send(StopOwnerResult::Unknown(error.clone()));
                }
                job.watching
                    .control
                    .mark_wait_authority_lost(error.clone(), job.watching.child);
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
        .mark_wait_authority_lost(message.clone(), job.watching.child);
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
) {
    while let Ok(completion) = completions.try_recv() {
        if let Some(worker) = active.remove(&completion.job_id) {
            let _ = worker.join();
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
        CleanupOutcome::Terminal { state, permit } => {
            entry.lifecycle = Lifecycle::Done;
            entry.terminal = Some(PendingTerminal {
                state,
                deadline: Instant::now() + OUTPUT_DRAIN_TIMEOUT,
                permit,
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
        if drain_expired {
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
                _permit: terminal.permit,
            }));
            entry.lifecycle = Lifecycle::Queued;
        }
    }
}

fn poll_and_read_outputs(
    entries: &mut [NativeEntry],
    wake_reader: &mut UnixStream,
    next_lifecycle_probe: Instant,
    diagnostics: &OwnerDiagnostics,
) -> bool {
    let mut poll_fds = vec![PollFd::new(&*wake_reader, PollFlags::IN)];
    let mut indices = Vec::new();
    for (index, entry) in entries.iter().enumerate() {
        if let Some(output) = &entry.output {
            poll_fds.push(PollFd::new(&output.reader, PollFlags::IN));
            indices.push(index);
        }
    }
    let lifecycle_deadline = entries
        .iter()
        .any(|entry| matches!(entry.lifecycle, Lifecycle::Watching(_)))
        .then_some(next_lifecycle_probe);
    let terminal_deadline = entries
        .iter()
        .filter_map(|entry| entry.terminal.as_ref().map(|terminal| terminal.deadline))
        .min();
    let deadline = match (lifecycle_deadline, terminal_deadline) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(deadline), None) | (None, Some(deadline)) => Some(deadline),
        (None, None) => None,
    };
    let timeout = deadline.map(|deadline| {
        Timespec::try_from(deadline.saturating_duration_since(Instant::now()))
            .expect("native poll duration fits Timespec")
    });
    let poll_result = poll(&mut poll_fds, timeout.as_ref());
    diagnostics.poll_returns.fetch_add(1, Ordering::AcqRel);
    let mut owner_woken = false;
    let ready = match poll_result {
        Ok(_) => {
            owner_woken = poll_fds[0]
                .revents()
                .intersects(PollFlags::IN | PollFlags::HUP | PollFlags::ERR | PollFlags::NVAL);
            poll_fds
                .iter()
                .skip(1)
                .zip(indices)
                .filter_map(|(fd, index)| {
                    fd.revents()
                        .intersects(
                            PollFlags::IN | PollFlags::HUP | PollFlags::ERR | PollFlags::NVAL,
                        )
                        .then_some(index)
                })
                .collect::<Vec<_>>()
        }
        Err(Errno::INTR) => Vec::new(),
        Err(error) => {
            eprintln!("ctxmuxd daemon-wide native output poll failed: {error}");
            Vec::new()
        }
    };
    drop(poll_fds);

    if owner_woken {
        let mut buffer = [0_u8; 64];
        loop {
            match wake_reader.read(&mut buffer) {
                Ok(0) => break,
                Ok(_) => {}
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) => {
                    eprintln!("ctxmuxd native owner wake drain failed: {error}");
                    break;
                }
            }
        }
    }

    for index in ready {
        let Some(output) = entries[index].output.as_mut() else {
            continue;
        };
        let mut buffer = [0_u8; OUTPUT_READ_BUFFER_BYTES];
        match output.reader.read(&mut buffer) {
            Ok(0) => entries[index].output = None,
            Ok(read) => {
                if let Some(run) = entries[index].run.upgrade() {
                    run.record_output(buffer[..read].to_vec());
                } else {
                    entries[index].output = None;
                }
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) if error.raw_os_error() == Some(Errno::IO.raw_os_error()) => {
                entries[index].output = None;
            }
            Err(error) => {
                if let Some(run) = entries[index].run.upgrade() {
                    eprintln!("ctxmuxd PTY read failed for {}: {error}", run.id);
                }
                entries[index].output = None;
            }
        }
    }
    owner_woken
}

fn detach_active_workers(active: &mut HashMap<u64, thread::JoinHandle<()>>) {
    active.clear();
}

fn preserve_shutdown_authority(entries: &mut [NativeEntry], queued: &mut VecDeque<WorkerJob>) {
    let message = "daemon-wide native owner stopped before child wait completed".to_owned();
    for job in queued.drain(..) {
        if let WorkerJob::Cleanup(job) = job {
            retain_unwaited_child(job.watching, &message);
        }
    }
    for entry in entries {
        let lifecycle = std::mem::replace(&mut entry.lifecycle, Lifecycle::Done);
        match lifecycle {
            Lifecycle::Watching(watching) => retain_unwaited_child(watching, &message),
            Lifecycle::WaitingCleanup(waiting) => {
                retain_unwaited_child(waiting.watching, &message);
            }
            Lifecycle::AuthorityLost(control) => std::mem::forget(control),
            Lifecycle::Queued | Lifecycle::Cleaning | Lifecycle::Finalizing | Lifecycle::Done => {}
        }
    }
}

fn retain_unwaited_child(watching: Watching, message: &str) {
    watching
        .control
        .mark_wait_authority_lost(message.to_owned(), watching.child);
    watching
        .wait_failure
        .record(watching.control.run_id(), message);
    // Daemon shutdown intentionally has no native Stop policy. Retain the
    // fail-stop owner until process exit instead of letting `Child::drop`
    // masquerade as wait/reap proof.
    std::mem::forget(watching.control);
}

#[cfg(test)]
mod tests {
    use std::{
        io::Write,
        os::{fd::OwnedFd, unix::net::UnixStream},
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::{Duration, Instant},
    };

    use ctxmux_protocol::{CommandDisposition, ErrorCode, RunId};
    use portable_pty::{Child, ChildKiller, ExitStatus};

    use super::NativeRunOwner;
    use crate::{
        NativeWaitFailure, Run, native_control::NativeControlOwner, native_session::NativeSession,
    };

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

    #[test]
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
            let writer = pair.master.take_writer().expect("take real pty writer");
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

        let descriptors = owner.extract_for_handoff();
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
        let second = owner.extract_for_handoff();
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
}
