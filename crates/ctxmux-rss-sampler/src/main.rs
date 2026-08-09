use std::{
    env,
    io::{BufRead, Write},
    process::ExitCode,
    sync::mpsc,
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde::Serialize;
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};

struct RssObservation {
    started_at: Instant,
    timestamp_ms: u64,
    rss_kib: u64,
}

#[derive(Serialize)]
struct RssSample {
    schema: &'static str,
    timestamp_ms: u64,
    seq: u64,
    rss_kib: u64,
    final_frame: bool,
}

enum SamplerCommand {
    Stop,
    Invalid(String),
}

fn main() -> ExitCode {
    match parse_args().and_then(|(pid, interval, maximum_gap)| {
        run_rss_sampler(pid, interval, maximum_gap).map_err(|error| error.to_string())
    }) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("ctxmux-rss-sampler: {error}");
            ExitCode::FAILURE
        }
    }
}

fn parse_args() -> Result<(u32, Duration, Duration), String> {
    let args: Vec<_> = env::args_os().skip(1).collect();
    if args.len() != 6
        || args[0] != "--pid"
        || args[2] != "--interval-ms"
        || args[4] != "--max-gap-ms"
    {
        return Err("expected --pid <pid> --interval-ms <ms> --max-gap-ms <ms>".to_owned());
    }
    let parse = |index: usize, label: &str| {
        args[index]
            .to_string_lossy()
            .parse::<u64>()
            .map_err(|error| format!("invalid {label}: {error}"))
    };
    let pid = u32::try_from(parse(1, "PID")?).map_err(|error| error.to_string())?;
    let interval = Duration::from_millis(parse(3, "sampling interval")?);
    let maximum_gap = Duration::from_millis(parse(5, "maximum sample gap")?);
    if interval.is_zero() || maximum_gap < interval {
        return Err("RSS sampling cadence is invalid".to_owned());
    }
    Ok((pid, interval, maximum_gap))
}

fn run_rss_sampler(pid: u32, interval: Duration, maximum_gap: Duration) -> std::io::Result<()> {
    let (sender, receiver) = mpsc::sync_channel(1);
    thread::Builder::new()
        .name("ctxmux-rss-stop".to_owned())
        .spawn(move || {
            let mut line = String::new();
            let command = match std::io::stdin().lock().read_line(&mut line) {
                Ok(_) if line == "stop\n" => SamplerCommand::Stop,
                Ok(0) => SamplerCommand::Invalid("stdin reached EOF".to_owned()),
                Ok(_) => SamplerCommand::Invalid("command is not exact stop line".to_owned()),
                Err(error) => SamplerCommand::Invalid(format!("stdin failed: {error}")),
            };
            let _ = sender.send(command);
        })?;

    let pid = Pid::from_u32(pid);
    let mut system = System::new();
    let refresh = ProcessRefreshKind::nothing().with_memory();
    let mut output = std::io::BufWriter::new(std::io::stdout().lock());
    let mut sequence = 0_u64;
    let mut previous_started_at = None;
    let mut target_start_time = None;
    let mut write_sample = |final_frame: bool| -> std::io::Result<Instant> {
        let observation = observe_rss(previous_started_at, maximum_gap, || {
            if system.refresh_processes_specifics(ProcessesToUpdate::Some(&[pid]), true, refresh)
                != 1
            {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "target process is unavailable",
                ));
            }
            let process = system.process(pid).ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::NotFound, "target vanished")
            })?;
            pin_process_incarnation(&mut target_start_time, process.start_time())?;
            Ok(process.memory() / 1024)
        })?;
        sequence = sequence
            .checked_add(1)
            .ok_or_else(|| std::io::Error::other("sample sequence overflow"))?;
        serde_json::to_writer(
            &mut output,
            &RssSample {
                schema: "ctxmux.rss-sample.v1",
                timestamp_ms: observation.timestamp_ms,
                seq: sequence,
                rss_kib: observation.rss_kib,
                final_frame,
            },
        )?;
        output.write_all(b"\n")?;
        output.flush()?;
        previous_started_at = Some(observation.started_at);
        Ok(observation.started_at)
    };

    let mut last_started_at = write_sample(false)?;
    loop {
        let wait = (last_started_at + interval).saturating_duration_since(Instant::now());
        match receiver.recv_timeout(wait) {
            Ok(SamplerCommand::Stop) => {
                write_sample(true)?;
                return Ok(());
            }
            Ok(SamplerCommand::Invalid(message)) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    message,
                ));
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                last_started_at = write_sample(false)?;
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "command owner disconnected",
                ));
            }
        }
    }
}

fn pin_process_incarnation(
    expected_start_time: &mut Option<u64>,
    observed_start_time: u64,
) -> std::io::Result<()> {
    match *expected_start_time {
        Some(expected) if expected != observed_start_time => Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "target process incarnation changed",
        )),
        Some(_) => Ok(()),
        None => {
            *expected_start_time = Some(observed_start_time);
            Ok(())
        }
    }
}

fn observe_rss(
    previous_started_at: Option<Instant>,
    maximum_gap: Duration,
    measure: impl FnOnce() -> std::io::Result<u64>,
) -> std::io::Result<RssObservation> {
    let started_at = Instant::now();
    if previous_started_at.is_some_and(|previous| started_at.duration_since(previous) > maximum_gap)
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "exceeded maximum start-to-start gap",
        ));
    }
    let timestamp_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| std::io::Error::other("clock predates Unix epoch"))?
        .as_millis()
        .try_into()
        .map_err(|_| std::io::Error::other("sample timestamp overflow"))?;
    let rss_kib = measure()?;
    if started_at.elapsed() > maximum_gap {
        return Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "observation exceeded maximum gap",
        ));
    }
    if rss_kib == 0 {
        return Err(std::io::Error::other("target RSS is zero"));
    }
    Ok(RssObservation {
        started_at,
        timestamp_ms,
        rss_kib,
    })
}

#[cfg(test)]
mod tests {
    use std::{
        process::Command,
        sync::mpsc,
        thread,
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    };

    use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};

    #[test]
    fn single_pid_memory_refresh_meets_representative_cadence_and_ps_scale() {
        let pid = Pid::from_u32(std::process::id());
        let refresh = ProcessRefreshKind::nothing().with_memory();
        let mut system = System::new();
        let mut previous = None;
        let mut maximum_gap_ms = 0_u128;
        for _ in 0..200 {
            let started = Instant::now();
            if let Some(previous) = previous {
                maximum_gap_ms = maximum_gap_ms.max(started.duration_since(previous).as_millis());
            }
            assert_eq!(
                system.refresh_processes_specifics(ProcessesToUpdate::Some(&[pid]), true, refresh,),
                1
            );
            previous = Some(started);
            std::thread::sleep(std::time::Duration::from_millis(25));
        }
        assert!(
            maximum_gap_ms <= 100,
            "native RSS gap was {maximum_gap_ms}ms"
        );
        let native_kib = system.process(pid).unwrap().memory() / 1024;
        let ps = Command::new("ps")
            .args(["-o", "rss=", "-p", &std::process::id().to_string()])
            .output()
            .unwrap();
        assert!(ps.status.success());
        let ps_kib = String::from_utf8(ps.stdout)
            .unwrap()
            .trim()
            .parse::<u64>()
            .unwrap();
        assert!(native_kib.abs_diff(ps_kib) <= 4096);
    }

    #[test]
    fn receipt_timestamp_remains_the_start_boundary_across_observation_durations() {
        for duration in [Duration::from_millis(5), Duration::from_millis(35)] {
            let (entered_sender, entered_receiver) = mpsc::channel();
            let (resume_sender, resume_receiver) = mpsc::channel();
            let observation = thread::spawn(move || {
                super::observe_rss(None, Duration::from_millis(100), || {
                    entered_sender.send(()).unwrap();
                    resume_receiver.recv().unwrap();
                    Ok(1)
                })
                .unwrap()
            });
            entered_receiver.recv().unwrap();
            let measurement_in_progress_ms: u64 = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis()
                .try_into()
                .unwrap();
            thread::sleep(duration);
            resume_sender.send(()).unwrap();

            assert!(observation.join().unwrap().timestamp_ms <= measurement_in_progress_ms);
        }
    }

    #[test]
    fn first_observation_duration_is_bounded_by_the_maximum_gap() {
        let Err(error) = super::observe_rss(None, Duration::from_millis(5), || {
            thread::sleep(Duration::from_millis(25));
            Ok(1)
        }) else {
            panic!("slow first observation was accepted");
        };
        assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
    }

    #[test]
    fn process_start_time_fences_pid_reuse() {
        let mut expected = None;
        super::pin_process_incarnation(&mut expected, 41).unwrap();
        super::pin_process_incarnation(&mut expected, 41).unwrap();
        assert_eq!(expected, Some(41));
        assert_eq!(
            super::pin_process_incarnation(&mut expected, 42)
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::NotFound
        );
    }
}
