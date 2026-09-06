//! One-shot, Run-neutral observation through original Native control.
//! This owner admits only temporary read jobs; it owns no Run or process map.

use crate::{
    Run, RunControl,
    resources::{ByteBudget, BytePermit, ResourceLimits},
};
use ctxmux_protocol::{ForegroundProcess, MAX_FRAME_BYTES, RunForegroundObservation, RunId};
use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::Semaphore;

pub(crate) struct ForegroundObservationOwner {
    slots: Arc<Semaphore>,
    memory: ByteBudget,
    bytes_per_job: usize,
    timeout: Duration,
}

/// The original response owns its admitted memory through DTO/encoding/send.
/// OS concurrency is released independently when its worker actually exits.
pub(crate) struct ForegroundObservationResult {
    pub(crate) observation: RunForegroundObservation,
    memory: Option<BytePermit>,
}

impl std::fmt::Debug for ForegroundObservationResult {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ForegroundObservationResult")
            .field("observation", &self.observation)
            .field("funded", &self.memory.is_some())
            .finish()
    }
}
impl ForegroundObservationResult {
    fn unfunded(observation: RunForegroundObservation) -> Self {
        Self {
            observation,
            memory: None,
        }
    }
    pub(crate) fn into_parts(self) -> (RunForegroundObservation, Option<BytePermit>) {
        (self.observation, self.memory)
    }
}

fn unknown(run_id: RunId, reason: &str) -> RunForegroundObservation {
    RunForegroundObservation::Unknown {
        run_id,
        reason: reason.to_owned(),
    }
}

fn unsupported(run_id: RunId, reason: &str) -> RunForegroundObservation {
    RunForegroundObservation::Unsupported {
        run_id,
        reason: reason.to_owned(),
    }
}

fn timestamp_ms() -> Option<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|elapsed| u64::try_from(elapsed.as_millis()).ok())
}

impl ForegroundObservationOwner {
    pub(crate) fn new(resources: ResourceLimits) -> Self {
        Self {
            slots: Arc::new(Semaphore::new(resources.foreground_observation_workers)),
            memory: ByteBudget::new(resources.foreground_observation_bytes),
            bytes_per_job: usize::try_from(
                resources.foreground_observation_bytes
                    / resources.foreground_observation_workers as u64,
            )
            .expect("validated observation byte budget fits the host")
            .min(MAX_FRAME_BYTES),
            timeout: Duration::from_millis(resources.foreground_observation_timeout_ms),
        }
    }

    pub(crate) async fn observe(&self, run: Arc<Run>) -> ForegroundObservationResult {
        let id = run.id;
        let Some(RunControl::Native(control)) = &run.incarnation_control else {
            return ForegroundObservationResult::unfunded(unsupported(id, "backend-unsupported"));
        };
        #[cfg(target_os = "macos")]
        if self.bytes_per_job < ctxmux_process_stats::foreground_minimum_bytes() {
            return ForegroundObservationResult::unfunded(unknown(id, "budget-exhausted"));
        }
        let control = control.clone();
        self.run_job(id, move |bytes, deadline, started| {
            let _run = run;
            #[cfg(target_os = "macos")]
            {
                let Some(before) = control.foreground_scope() else {
                    return unknown(id, "owner-unavailable");
                };
                let processes = match ctxmux_process_stats::foreground_group(
                    before.2, before.0, bytes, deadline,
                ) {
                    Ok(processes) => processes,
                    Err(error) if error.kind() == std::io::ErrorKind::Unsupported => {
                        return unsupported(id, "execution-evidence-unsupported");
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::OutOfMemory => {
                        return unknown(id, "budget-exhausted");
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::TimedOut => {
                        return unknown(id, "deadline-exceeded");
                    }
                    Err(_) => return unknown(id, "observation-unavailable"),
                };
                if Instant::now() >= deadline {
                    return unknown(id, "deadline-exceeded");
                }
                if control.foreground_scope().as_ref() != Some(&before) {
                    return unknown(id, "stale-scope");
                }
                let Some(completed) = timestamp_ms() else {
                    return unknown(id, "observation-unavailable");
                };
                if completed < started {
                    return unknown(id, "observation-unavailable");
                }
                RunForegroundObservation::Observed {
                    run_id: id,
                    started_at_ms: started,
                    completed_at_ms: completed,
                    root_pid: before.0,
                    root_incarnation: before.1,
                    posix_session_id: before.0,
                    foreground_pgid: before.2,
                    processes: processes
                        .into_iter()
                        .map(|p| ForegroundProcess {
                            pid: p.pid,
                            process_incarnation: p.incarnation,
                            execution_generation: p.execution_generation,
                            pgid: p.pgid,
                            sid: p.sid,
                            executable_path: p.executable_path,
                            executable_image: p.executable_image,
                        })
                        .collect(),
                }
            }
            #[cfg(not(target_os = "macos"))]
            {
                let _ = (control, bytes, deadline, started);
                unsupported(id, "backend-unsupported")
            }
        })
        .await
    }

    async fn run_job(
        &self,
        id: RunId,
        job: impl FnOnce(usize, Instant, u64) -> RunForegroundObservation + Send + 'static,
    ) -> ForegroundObservationResult {
        if self.bytes_per_job == 0 {
            return ForegroundObservationResult::unfunded(unknown(id, "budget-exhausted"));
        }
        let Ok(slot) = Arc::clone(&self.slots).try_acquire_owned() else {
            return ForegroundObservationResult::unfunded(unknown(id, "budget-exhausted"));
        };
        let Some(memory) = self.memory.reserve(self.bytes_per_job) else {
            return ForegroundObservationResult::unfunded(unknown(id, "budget-exhausted"));
        };
        let bytes = self.bytes_per_job;
        let deadline = Instant::now() + self.timeout;
        let Some(started) = timestamp_ms() else {
            return ForegroundObservationResult::unfunded(unknown(id, "observation-unavailable"));
        };
        // These are worker-owned, not requester-owned. Cancellation or timeout
        // cannot recycle a slot or its memory while physical work still lives.
        let worker = tokio::task::spawn_blocking(move || {
            let _slot = slot;
            let observation = job(bytes, deadline, started);
            ForegroundObservationResult {
                observation,
                memory: Some(memory),
            }
        });
        match tokio::time::timeout(self.timeout, worker).await {
            Ok(Ok(observation)) => observation,
            Ok(Err(_)) => {
                ForegroundObservationResult::unfunded(unknown(id, "observation-unavailable"))
            }
            Err(_) => ForegroundObservationResult::unfunded(unknown(id, "deadline-exceeded")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt as _;

    #[cfg(target_os = "macos")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn retained_nonempty_foreground_result_stays_funded_until_response_release() {
        let resources = ResourceLimits {
            foreground_observation_workers: 1,
            ..ResourceLimits::DEFAULT
        };
        let manager = Arc::new(crate::RunManager::with_instance_stats_and_resources(
            ctxmux_protocol::DaemonInstanceId::new(),
            crate::QualificationStats::default(),
            resources,
        ));
        let server = crate::tests::InProcessServer::start(Arc::clone(&manager));
        let run = server
            .client
            .start(ctxmux_protocol::RunSpec {
                program: "/bin/sh".to_owned(),
                args: vec![
                    "-c".to_owned(),
                    "printf 'READY\\n'; while read -r line; do printf '%s\\n' \"$line\"; done"
                        .to_owned(),
                ],
                cwd: None,
                env: std::collections::BTreeMap::new(),
                initial_size: ctxmux_protocol::TerminalSize::default(),
                declared_inputs: Vec::new(),
            })
            .await
            .unwrap();
        let (attachment, snapshot) = server.client.attach(run.id, 0).await.unwrap();
        let mut ready: Vec<u8> = snapshot
            .replay
            .chunks
            .iter()
            .flat_map(|chunk| chunk.data.iter().copied())
            .collect();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !String::from_utf8_lossy(&ready).contains("READY") {
            if let Some(ctxmux_protocol::RunEvent::Output { chunk }) =
                tokio::time::timeout_at(deadline, attachment.next_event())
                    .await
                    .unwrap()
                    .unwrap()
            {
                ready.extend(chunk.data);
            }
        }
        attachment.detach().await.unwrap();
        let (socket, _peer) = tokio::net::UnixStream::pair().unwrap();
        let mut wire = tokio_util::codec::Framed::new(socket, crate::codec());
        let retained = crate::execute_connected_request(
            &manager,
            &mut wire,
            ctxmux_protocol::Request::ObserveForeground { run_id: run.id },
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();
        let next = manager
            .foreground_observations
            .observe(manager.pin(run.id).unwrap())
            .await;
        let used = manager.foreground_observations.memory.used();
        // The public Rust client independently consumes Hello and this exact
        // Run response on one dispatch connection, without touching the PTY.
        let (runtime, public_observation) = server.client.observe_foreground(run.id).await.unwrap();
        eprintln!(
            "held_response={:?}; next={:?}; actual_bytes={used}",
            retained.response, next
        );
        server
            .client
            .stop(crate::tests::fresh_stop(&server.client, run.id).await)
            .await
            .unwrap();
        match &retained.response {
            ctxmux_protocol::Response::ForegroundObservation {
                observation:
                    RunForegroundObservation::Observed {
                        root_pid,
                        processes,
                        ..
                    },
            } => {
                assert_eq!(Some(*root_pid), run.pid);
                assert_eq!(
                    processes.iter().map(|p| p.pid).collect::<Vec<_>>(),
                    vec![run.pid.unwrap()]
                );
            }
            other => panic!("real nonempty Native result required: {other:?}"),
        }
        assert_eq!(
            used, manager.foreground_observations.bytes_per_job as u64,
            "live DTO/encoding must retain its admitted bytes"
        );
        assert_eq!(next.observation, unknown(run.id, "budget-exhausted"));
        assert_eq!(runtime.daemon_instance_id, manager.daemon_instance);
        assert_eq!(public_observation, unknown(run.id, "budget-exhausted"));
        drop(retained);
        assert_eq!(manager.foreground_observations.memory.used(), 0);
    }

    #[cfg(target_os = "macos")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires an explicitly bound older real daemon image"]
    async fn actual_old_target_is_refused_before_extract_and_private_runs_keep_acknowledging() {
        assert!(
            std::env::var_os("CTXMUX_TEST_UPGRADE_TARGET").is_some(),
            "bind the exact older image"
        );
        let server = crate::tests::InProcessServer::start(Arc::new(
            crate::RunManager::with_instance_stats_and_resources(
                ctxmux_protocol::DaemonInstanceId::new(),
                crate::QualificationStats::default(),
                ResourceLimits::DEFAULT,
            ),
        ));
        let mut runs = Vec::new();
        for _ in 0..2 {
            runs.push(server.client.start(ctxmux_protocol::RunSpec {
                program: "/bin/sh".to_owned(),
                args: vec!["-c".to_owned(), "stty -echo; while IFS= read -r line; do printf 'ACK:%s\\n' \"$line\"; done".to_owned()],
                cwd: None, env: std::collections::BTreeMap::new(),
                initial_size: ctxmux_protocol::TerminalSize::default(), declared_inputs: Vec::new(),
            }).await.unwrap());
        }
        let state = tempfile::tempdir().unwrap();
        std::fs::set_permissions(state.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let refusal = crate::prepare_exec_upgrade(state.path()).unwrap_err();
        let mut observations = Vec::new();
        for (index, run) in runs.iter().enumerate() {
            let before = server.client.status(run.id).await.unwrap();
            let (attachment, snapshot) = server.client.attach(run.id, 0).await.unwrap();
            let expected = format!("ACK:preflight-{index}");
            server
                .client
                .input(run.id, format!("preflight-{index}\n").into_bytes())
                .await
                .unwrap();
            let mut bytes: Vec<u8> = snapshot
                .replay
                .chunks
                .iter()
                .flat_map(|chunk| chunk.data.iter().copied())
                .collect();
            let mut cursor = snapshot.replay.latest_output_bytes;
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            while !String::from_utf8_lossy(&bytes).contains(&expected) {
                match tokio::time::timeout_at(deadline, attachment.next_event())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap()
                {
                    ctxmux_protocol::RunEvent::Output { chunk } => {
                        assert_eq!(
                            chunk.start_byte, cursor,
                            "original ordered output continues"
                        );
                        cursor = chunk.end_byte;
                        bytes.extend(chunk.data);
                    }
                    ctxmux_protocol::RunEvent::ServiceChanged { .. } => {}
                    event => panic!("private Run continuity failed: {event:?}"),
                }
            }
            let after = server.client.status(run.id).await.unwrap();
            attachment.detach().await.unwrap();
            observations.push((before, after, bytes, expected));
        }
        for run in &runs {
            server
                .client
                .stop(crate::tests::fresh_stop(&server.client, run.id).await)
                .await
                .unwrap();
        }
        let reason = match refusal {
            crate::UpgradeAbort::BeforeExtract(crate::ServerError::Shutdown { failures }) => {
                failures
            }
            crate::UpgradeAbort::BeforeExtract(error) => {
                panic!("wrong reversible preflight refusal: {error}")
            }
            crate::UpgradeAbort::AfterExtract(error) => {
                panic!("unexpected irreversible refusal: {error}")
            }
        };
        assert!(
            reason.contains("accepts handoff schema")
                && reason.contains("ctxmux.daemon-handoff.v7")
        );
        assert_eq!(observations.len(), 2);
        for (before, after, bytes, expected) in observations {
            assert_eq!(before.id, after.id);
            assert_eq!(before.pid, after.pid);
            assert!(before.state.is_running() && after.state.is_running());
            assert!(String::from_utf8_lossy(&bytes).contains(&expected));
            eprintln!(
                "old_target_rejected={reason}; before={before:?}; after={after:?}; ack={expected}"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(
        clippy::too_many_lines,
        reason = "one real two-Run test proves timeout and cancellation retain physical permits and owner identity"
    )]
    async fn timed_out_or_closed_requester_keeps_physical_worker_permits() {
        let manager = Arc::new(crate::RunManager::with_instance_stats_and_resources(
            ctxmux_protocol::DaemonInstanceId::new(),
            crate::QualificationStats::default(),
            ResourceLimits {
                foreground_observation_workers: 1,
                foreground_observation_timeout_ms: 20,
                ..ResourceLimits::DEFAULT
            },
        ));
        let server = crate::tests::InProcessServer::start(Arc::clone(&manager));
        let mut runs = Vec::new();
        for _ in 0..2 {
            runs.push(server.client.start(ctxmux_protocol::RunSpec {
                program: "/bin/sh".to_owned(),
                args: vec!["-c".to_owned(), "stty -echo; while IFS= read -r line; do printf 'ACK:%s\\n' \"$line\"; done".to_owned()],
                cwd: None, env: std::collections::BTreeMap::new(),
                initial_size: ctxmux_protocol::TerminalSize::default(), declared_inputs: Vec::new(),
            }).await.unwrap());
        }
        let owner = &manager.foreground_observations;
        let id = runs[0].id;
        for cancel_requester in [false, true] {
            let (entered, ready) = tokio::sync::oneshot::channel();
            let (release, blocked) = std::sync::mpsc::channel();
            let (exited, drained) = tokio::sync::oneshot::channel();
            let request = Arc::clone(&manager);
            let requester = tokio::spawn(async move {
                request
                    .foreground_observations
                    .run_job(id, move |_, _, _| {
                        let _ = entered.send(());
                        blocked.recv().unwrap();
                        let _ = exited.send(());
                        unknown(id, "observation-unavailable")
                    })
                    .await
            });
            ready.await.unwrap();
            if cancel_requester {
                requester.abort();
                assert!(requester.await.unwrap_err().is_cancelled());
            } else {
                assert_eq!(
                    requester.await.unwrap().observation,
                    unknown(id, "deadline-exceeded")
                );
            }
            assert_eq!(
                owner.slots.available_permits(),
                0,
                "request ending does not end its physical worker"
            );
            assert_eq!(owner.memory.used(), owner.bytes_per_job as u64);
            let refusal = owner
                .run_job(id, |_, _, _| panic!("over-budget OS job must not start"))
                .await;
            assert_eq!(refusal.observation, unknown(id, "budget-exhausted"));
            for (index, run) in runs.iter().enumerate() {
                let before = server.client.status(run.id).await.unwrap();
                let (attachment, snapshot) = server.client.attach(run.id, 0).await.unwrap();
                let token = format!("blocked-{cancel_requester}-{index}");
                let expected = format!("ACK:{token}");
                server
                    .client
                    .input(run.id, format!("{token}\n").into_bytes())
                    .await
                    .unwrap();
                let mut bytes: Vec<u8> = snapshot
                    .replay
                    .chunks
                    .iter()
                    .flat_map(|chunk| chunk.data.iter().copied())
                    .collect();
                let mut cursor = snapshot.replay.latest_output_bytes;
                let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
                while !String::from_utf8_lossy(&bytes).contains(&expected) {
                    match tokio::time::timeout_at(deadline, attachment.next_event())
                        .await
                        .unwrap()
                        .unwrap()
                        .unwrap()
                    {
                        ctxmux_protocol::RunEvent::Output { chunk } => {
                            assert_eq!(chunk.start_byte, cursor);
                            cursor = chunk.end_byte;
                            bytes.extend(chunk.data);
                        }
                        ctxmux_protocol::RunEvent::ServiceChanged { .. } => {}
                        event => panic!("healthy private Run changed: {event:?}"),
                    }
                }
                let after = server.client.status(run.id).await.unwrap();
                attachment.detach().await.unwrap();
                assert_eq!(before.pid, after.pid);
                assert_eq!(before.id, after.id);
                assert!(after.state.is_running());
                assert_eq!(owner.slots.available_permits(), 0);
                assert_eq!(owner.memory.used(), owner.bytes_per_job as u64);
                eprintln!(
                    "blocked_worker_private_ack={expected}; id={}; pid={:?}; cursor={cursor}; funded={}",
                    run.id,
                    after.pid,
                    owner.memory.used()
                );
            }
            release.send(()).unwrap();
            drained.await.unwrap();
            tokio::time::timeout(Duration::from_secs(1), async {
                while owner.slots.available_permits() == 0 || owner.memory.used() != 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert_eq!(owner.slots.available_permits(), 1);
            assert_eq!(owner.memory.used(), 0);
        }
        for run in &runs {
            server
                .client
                .stop(crate::tests::fresh_stop(&server.client, run.id).await)
                .await
                .unwrap();
        }
    }
}
