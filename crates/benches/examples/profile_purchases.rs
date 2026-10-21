//! Fixed fresh-world batches for counters and externally gated perf sampling.
use benches::stateful::purchases;
use std::{
    env,
    fs::{File, OpenOptions},
    hint::black_box,
    io::{BufRead, BufReader, Write},
    time::Instant,
};

fn number(name: &str, default: usize, positive: bool) -> usize {
    let value = env::var(name)
        .map(|s| s.parse().unwrap_or_else(|_| panic!("invalid {name}")))
        .unwrap_or(default);
    assert!(!positive || value > 0, "{name} must be positive");
    value
}

struct PerfControl {
    control: File,
    acknowledgement: BufReader<File>,
}

impl PerfControl {
    fn open() -> std::io::Result<Option<Self>> {
        match (
            env::var_os("PROFILE_PERF_CONTROL"),
            env::var_os("PROFILE_PERF_ACK"),
        ) {
            (Some(control), Some(ack)) => Ok(Some(Self {
                control: OpenOptions::new().write(true).open(control)?,
                acknowledgement: BufReader::new(File::open(ack)?),
            })),
            (None, None) => Ok(None),
            _ => Err(std::io::Error::other(
                "set both PROFILE_PERF_CONTROL and PROFILE_PERF_ACK",
            )),
        }
    }

    fn command(&mut self, command: &str) -> std::io::Result<()> {
        writeln!(self.control, "{command}")?;
        self.control.flush()?;
        let mut response = String::new();
        self.acknowledgement.read_line(&mut response)?;
        // Some perf versions include a trailing NUL after the newline.
        if response.trim_matches(|ch: char| ch.is_whitespace() || ch == '\0') != "ack" {
            return Err(std::io::Error::other(format!(
                "unexpected perf acknowledgement: {response:?}"
            )));
        }
        Ok(())
    }
}

fn main() -> std::io::Result<()> {
    let workers = number("PROFILE_WORKERS", 8, true);
    let requests = number("PROFILE_REQUESTS", 1000, true);
    let per_owner = number("PROFILE_PER_OWNER", 1, true);
    let work = u32::try_from(number("PROFILE_WORK", 0, false)).expect("PROFILE_WORK must fit u32");
    let rounds = number("PROFILE_ROUNDS", 50, true);
    let warmup = number("PROFILE_WARMUP", 2, false);
    let width = number("PROFILE_DISPATCH_BATCH", 16, true);
    let mut control = PerfControl::open()?;
    println!(
        "config\tworkers={workers}\trequests={requests}\tper_owner={per_owner}\twork={work}\trounds={rounds}\twarmup={warmup}\tbatch={width}\tsampled={}",
        control.is_some()
    );
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .thread_name("porridge-worker")
        .build()?;
    runtime.block_on(async {
        for iteration in 0..rounds.checked_add(warmup).expect("round count overflow") {
            let bowl = purchases::create(requests.div_ceil(per_owner), u64::MAX, u64::MAX).await;
            bowl.set_dispatch_batch_size(width);
            let profiles = bowl.profile_all().await;
            // The third existing bucket times hook commits only; ordinary
            // streamed commits must be attributed with the CPU profile.
            let before = bowl::bowl_debug_buckets();
            let measured = iteration >= warmup;
            if measured && let Some(control) = &mut control {
                control.command("enable")?;
            }

            let start = Instant::now();
            let responses = purchases::batch(&bowl, requests, per_owner, work).await;
            let elapsed = start.elapsed();

            if measured && let Some(control) = &mut control {
                control.command("disable")?;
            }
            let after = bowl::bowl_debug_buckets();
            // Outside the measured/sampled region: catch incomplete work without
            // charging checks or output formatting to the engine's hot path.
            assert_eq!(responses.len(), requests);
            assert!(responses.iter().all(|response| response.accepted));
            black_box(responses);
            if measured {
                let round = iteration - warmup;
                println!(
                    "sample\t{round}\t{requests}\t{}\t{}\t{}\t{}\t{}",
                    elapsed.as_nanos(),
                    after.0 - before.0,
                    after.1 - before.1,
                    after.2 - before.2,
                    after.3 - before.3
                );
                for (initial, final_) in profiles.into_iter().zip(bowl.profile_all().await) {
                    println!(
                        "system\t{round}\t{}\t{}\t{}\t{}",
                        final_.name,
                        final_.runs - initial.runs,
                        final_.plan_nanos - initial.plan_nanos,
                        final_.run_nanos - initial.run_nanos
                    );
                }
            }
        }
        Ok(())
    })
}
