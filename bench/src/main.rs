//! bstk-bench: a load generator for beanstalkd-compatible servers.
//!
//! ```text
//! bstk-bench --addr HOST:PORT --conns N --duration SECS --scenario <name>
//!            [--body-size B] [--pipeline D] [--threads T] [--json]
//! ```
//!
//! Scenarios:
//! * `put-reserve-delete`: every connection loops put -> reserve -> delete on
//!   its own tube.
//! * `producers-consumers`: N/2 connections put into a shared tube, the rest
//!   reserve (`reserve-with-timeout 1`) and delete from it. After the
//!   deadline the consumers drain the backlog (not measured).
//! * `put-only`: every connection puts into a shared tube; afterwards all
//!   jobs are drained (not measured).
//!
//! `--pipeline D` sends D commands of the same kind back to back before
//! reading their replies (D puts, then D reserves, then D deletes). The
//! latency of each op is measured from the batch write to its reply.
//!
//! Any unexpected reply, or no reply within 10 s, fails the run (exit 1).
//! At the end the server's `stats` must show zero ready, reserved, delayed
//! and buried jobs. Server CPU is derived from `rusage-utime` +
//! `rusage-stime` in `stats`, sampled at the start and at the deadline.

mod client;
mod latency;

use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use clap::{Parser, ValueEnum};
use tokio::task::JoinSet;
use tokio::time::Instant;

use client::{Client, Reply, Result, stats_f64, stats_u64, unexpected};
use latency::{Op, Recorder};

const TTR: u32 = 60;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Scenario {
    PutReserveDelete,
    ProducersConsumers,
    PutOnly,
}

impl Scenario {
    fn name(self) -> &'static str {
        match self {
            Scenario::PutReserveDelete => "put-reserve-delete",
            Scenario::ProducersConsumers => "producers-consumers",
            Scenario::PutOnly => "put-only",
        }
    }
}

/// Load generator for beanstalkd-compatible servers.
#[derive(Debug, Parser)]
#[command(name = "bstk-bench")]
struct Args {
    /// Server address.
    #[arg(long, default_value = "127.0.0.1:11300")]
    addr: String,
    /// Number of client connections.
    #[arg(long, default_value_t = 10)]
    conns: usize,
    /// Measured duration in seconds.
    #[arg(long, default_value_t = 10)]
    duration: u64,
    /// Workload to run.
    #[arg(long, value_enum)]
    scenario: Scenario,
    /// Job body size in bytes.
    #[arg(long, default_value_t = 16)]
    body_size: usize,
    /// Commands sent back to back before reading replies.
    #[arg(long, default_value_t = 1)]
    pipeline: usize,
    /// Tokio worker threads for the load generator (default: all cores).
    #[arg(long)]
    threads: Option<usize>,
    /// Also print a one-line JSON summary (for scripts).
    #[arg(long)]
    json: bool,
}

/// What one connection task reports back.
#[derive(Default)]
struct TaskResult {
    rec: Recorder,
    /// `reserve-with-timeout` calls that timed out during the measured
    /// window (consumers only; not an error).
    idle_timeouts: u64,
}

fn main() -> ExitCode {
    let args = Args::parse();
    if args.conns == 0 || args.pipeline == 0 || args.duration == 0 {
        eprintln!("bstk-bench: --conns, --pipeline and --duration must be > 0");
        return ExitCode::from(2);
    }
    if args.scenario == Scenario::ProducersConsumers && args.conns < 2 {
        eprintln!("bstk-bench: producers-consumers needs --conns >= 2");
        return ExitCode::from(2);
    }
    let mut rt = tokio::runtime::Builder::new_multi_thread();
    rt.enable_all();
    if let Some(t) = args.threads {
        rt.worker_threads(t.max(1));
    }
    let rt = match rt.build() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("bstk-bench: cannot start runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    match rt.block_on(run(&args)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("bstk-bench: FAILED: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run(args: &Args) -> Result<()> {
    let body: Arc<[u8]> = vec![b'x'; args.body_size].into();
    let tag = std::process::id();
    let mut monitor = Client::connect(&args.addr).await?;

    // Connect and configure every client before starting the clock.
    let mut clients = Vec::with_capacity(args.conns);
    for i in 0..args.conns {
        let mut c = Client::connect(&args.addr).await?;
        let tube = match args.scenario {
            Scenario::PutReserveDelete => format!("bench-{tag}-{i}"),
            Scenario::ProducersConsumers | Scenario::PutOnly => format!("bench-{tag}-shared"),
        };
        c.use_and_watch_only(&tube).await?;
        clients.push(c);
    }

    let stats_start = monitor.stats().await?;
    let start = Instant::now();
    let deadline = start + Duration::from_secs(args.duration);
    let producers = args.conns / 2;
    let producers_left = Arc::new(AtomicUsize::new(producers));

    let mut set: JoinSet<Result<TaskResult>> = JoinSet::new();
    for (i, c) in clients.into_iter().enumerate() {
        let body = Arc::clone(&body);
        let depth = args.pipeline;
        match args.scenario {
            Scenario::PutReserveDelete => {
                set.spawn(put_reserve_delete(c, deadline, body, depth));
            }
            Scenario::PutOnly => {
                set.spawn(put_only(c, deadline, body, depth));
            }
            Scenario::ProducersConsumers if i < producers => {
                let left = Arc::clone(&producers_left);
                set.spawn(async move {
                    let res = put_only(c, deadline, body, depth).await;
                    // Count the producer as done even when it failed, so
                    // consumers never wait for it forever.
                    left.fetch_sub(1, Ordering::SeqCst);
                    res
                });
            }
            Scenario::ProducersConsumers => {
                let left = Arc::clone(&producers_left);
                set.spawn(consumer(c, deadline, left));
            }
        }
    }

    tokio::time::sleep_until(deadline).await;
    let stats_deadline = monitor.stats().await?;
    let elapsed = start.elapsed().as_secs_f64();

    let mut rec = Recorder::default();
    let mut idle_timeouts = 0;
    let mut first_err = None;
    while let Some(joined) = set.join_next().await {
        match joined {
            Ok(Ok(r)) => {
                rec.merge(r.rec);
                idle_timeouts += r.idle_timeouts;
            }
            Ok(Err(e)) => {
                first_err.get_or_insert_with(|| e.to_string());
            }
            Err(e) => {
                first_err.get_or_insert_with(|| format!("task panicked: {e}"));
            }
        }
    }
    if let Some(e) = first_err {
        return Err(e.into());
    }
    let finished = start.elapsed().as_secs_f64();

    if args.scenario == Scenario::PutOnly {
        drain(&args.addr, args.conns, args.pipeline, tag).await?;
    }
    let drain_secs = start.elapsed().as_secs_f64() - finished;

    let stats_end = monitor.stats().await?;
    let mut leftover = Vec::new();
    for key in [
        "current-jobs-ready",
        "current-jobs-reserved",
        "current-jobs-delayed",
        "current-jobs-buried",
    ] {
        match stats_u64(&stats_end, key) {
            Some(0) => {}
            Some(n) => leftover.push(format!("{key}: {n}")),
            None => leftover.push(format!("{key}: missing from stats")),
        }
    }

    let cpu = |s: &str| -> Option<f64> {
        Some(stats_f64(s, "rusage-utime")? + stats_f64(s, "rusage-stime")?)
    };
    let cpu_pct = match (cpu(&stats_start), cpu(&stats_deadline)) {
        (Some(a), Some(b)) => Some(100.0 * (b - a) / elapsed),
        _ => None,
    };
    let backlog = stats_u64(&stats_deadline, "current-jobs-ready");

    report(
        args,
        &mut rec,
        elapsed,
        cpu_pct,
        backlog,
        idle_timeouts,
        drain_secs,
    );

    if leftover.is_empty() {
        println!("verify: server is empty (ready/reserved/delayed/buried all 0)");
        Ok(())
    } else {
        Err(format!("server not empty after run: {}", leftover.join(", ")).into())
    }
}

/// Measured window: only ops completed before the deadline count.
fn in_window(deadline: Instant) -> bool {
    Instant::now() < deadline
}

async fn put_batch(
    c: &mut Client,
    rec: &mut Recorder,
    body: &[u8],
    depth: usize,
    ids: &mut Vec<u64>,
) -> Result<()> {
    for _ in 0..depth {
        c.queue_put(0, 0, TTR, body);
    }
    let t0 = Instant::now();
    c.flush().await?;
    for _ in 0..depth {
        match c.read_reply().await? {
            Reply::Inserted(id) => {
                rec.record(Op::Put, t0.elapsed());
                ids.push(id);
            }
            r => return Err(unexpected("put", r)),
        }
    }
    Ok(())
}

async fn delete_batch(c: &mut Client, rec: &mut Recorder, ids: &[u64]) -> Result<()> {
    for id in ids {
        c.queue(&format!("delete {id}"));
    }
    let t0 = Instant::now();
    c.flush().await?;
    for _ in ids {
        match c.read_reply().await? {
            Reply::Deleted => rec.record(Op::Delete, t0.elapsed()),
            r => return Err(unexpected("delete", r)),
        }
    }
    Ok(())
}

async fn put_reserve_delete(
    mut c: Client,
    deadline: Instant,
    body: Arc<[u8]>,
    depth: usize,
) -> Result<TaskResult> {
    let mut res = TaskResult::default();
    let mut ids = Vec::with_capacity(depth);
    while in_window(deadline) {
        ids.clear();
        put_batch(&mut c, &mut res.rec, &body, depth, &mut ids).await?;

        // The tube is private to this connection, so exactly our jobs are
        // ready and every reserve succeeds immediately.
        for _ in 0..depth {
            c.queue("reserve");
        }
        let t0 = Instant::now();
        c.flush().await?;
        let mut reserved = Vec::with_capacity(depth);
        for _ in 0..depth {
            match c.read_reply().await? {
                Reply::Reserved { id, body_len } if body_len == body.len() => {
                    res.rec.record(Op::Reserve, t0.elapsed());
                    reserved.push(id);
                }
                r => return Err(unexpected("reserve", r)),
            }
        }
        reserved.sort_unstable();
        if reserved != ids {
            return Err(format!("reserved {reserved:?}, expected own jobs {ids:?}").into());
        }
        delete_batch(&mut c, &mut res.rec, &reserved).await?;
    }
    Ok(res)
}

async fn put_only(
    mut c: Client,
    deadline: Instant,
    body: Arc<[u8]>,
    depth: usize,
) -> Result<TaskResult> {
    let mut res = TaskResult::default();
    let mut ids = Vec::with_capacity(depth);
    while in_window(deadline) {
        ids.clear();
        put_batch(&mut c, &mut res.rec, &body, depth, &mut ids).await?;
    }
    Ok(res)
}

/// Reserves and deletes until the deadline has passed, all producers are
/// done, and the tube has stayed empty for a full `reserve-with-timeout 1`.
/// Ops after the deadline drain the backlog and are not recorded.
async fn consumer(
    mut c: Client,
    deadline: Instant,
    producers_left: Arc<AtomicUsize>,
) -> Result<TaskResult> {
    let mut res = TaskResult::default();
    loop {
        // Read before reserving: if all producers were already done when a
        // reserve times out, no job can appear any more.
        let producers_done = producers_left.load(Ordering::SeqCst) == 0;
        let t0 = Instant::now();
        match c.call("reserve-with-timeout 1").await? {
            Reply::Reserved { id, .. } => {
                let measured = in_window(deadline);
                if measured {
                    res.rec.record(Op::Reserve, t0.elapsed());
                }
                let t1 = Instant::now();
                match c.call(&format!("delete {id}")).await? {
                    Reply::Deleted => {
                        if measured && in_window(deadline) {
                            res.rec.record(Op::Delete, t1.elapsed());
                        }
                    }
                    r => return Err(unexpected("delete", r)),
                }
            }
            Reply::TimedOut if producers_done => return Ok(res),
            Reply::TimedOut => {
                if in_window(deadline) {
                    res.idle_timeouts += 1;
                }
            }
            r => return Err(unexpected("reserve-with-timeout", r)),
        }
    }
}

/// Deletes every job left in the shared put-only tube, using `conns`
/// fresh connections with pipelined `reserve-with-timeout 0` + `delete`.
async fn drain(addr: &str, conns: usize, depth: usize, tag: u32) -> Result<()> {
    let tube = format!("bench-{tag}-shared");
    let mut set: JoinSet<Result<()>> = JoinSet::new();
    for _ in 0..conns {
        let addr = addr.to_string();
        let tube = tube.clone();
        set.spawn(async move {
            let mut c = Client::connect(&addr).await?;
            c.use_and_watch_only(&tube).await?;
            let mut rec = Recorder::default();
            loop {
                for _ in 0..depth {
                    c.queue("reserve-with-timeout 0");
                }
                c.flush().await?;
                let mut ids = Vec::with_capacity(depth);
                let mut empty = false;
                for _ in 0..depth {
                    match c.read_reply().await? {
                        Reply::Reserved { id, .. } => ids.push(id),
                        Reply::TimedOut => empty = true,
                        r => return Err(unexpected("reserve-with-timeout", r)),
                    }
                }
                delete_batch(&mut c, &mut rec, &ids).await?;
                if empty {
                    return Ok(());
                }
            }
        });
    }
    while let Some(joined) = set.join_next().await {
        joined.map_err(|e| format!("drain task panicked: {e}"))??;
    }
    Ok(())
}

fn us(nanos: u64) -> f64 {
    nanos as f64 / 1000.0
}

fn report(
    args: &Args,
    rec: &mut Recorder,
    elapsed: f64,
    cpu_pct: Option<f64>,
    backlog: Option<u64>,
    idle_timeouts: u64,
    drain_secs: f64,
) {
    let total = rec.total();
    let ops_per_sec = total as f64 / elapsed;
    println!(
        "scenario={} addr={} conns={} duration={}s body={}B pipeline={}",
        args.scenario.name(),
        args.addr,
        args.conns,
        args.duration,
        args.body_size,
        args.pipeline
    );
    println!("total: {total} ops in {elapsed:.2}s = {ops_per_sec:.0} ops/s");
    println!(
        "{:<8} {:>10} {:>12} {:>10} {:>10} {:>10} {:>10}",
        "op", "count", "ops/s", "p50 us", "p99 us", "p999 us", "max us"
    );
    let mut json_ops = Vec::new();
    for op in Op::ALL {
        let count = rec.count(op);
        if let Some(s) = rec.summary(op) {
            println!(
                "{:<8} {:>10} {:>12.0} {:>10.1} {:>10.1} {:>10.1} {:>10.1}",
                op.name(),
                s.count,
                count as f64 / elapsed,
                us(s.p50),
                us(s.p99),
                us(s.p999),
                us(s.max)
            );
            json_ops.push(format!(
                "\"{n}_count\":{},\"{n}_p50_us\":{:.1},\"{n}_p99_us\":{:.1},\"{n}_p999_us\":{:.1}",
                s.count,
                us(s.p50),
                us(s.p99),
                us(s.p999),
                n = op.name()
            ));
        }
    }
    if let Some(pct) = cpu_pct {
        println!("server cpu: {pct:.0}% of one core (rusage utime+stime over the measured window)");
    }
    if let Some(b) = backlog
        && args.scenario != Scenario::PutReserveDelete
    {
        println!("ready backlog at deadline: {b} jobs");
    }
    if args.scenario == Scenario::ProducersConsumers {
        println!("consumer idle timeouts (not errors): {idle_timeouts}");
    }
    if args.scenario == Scenario::PutOnly {
        println!("drain: {drain_secs:.2}s (not measured)");
    }
    if args.json {
        let cpu = cpu_pct.map_or_else(|| "null".to_string(), |p| format!("{p:.1}"));
        println!(
            "JSON {{\"scenario\":\"{}\",\"conns\":{},\"body_size\":{},\"pipeline\":{},\"duration_s\":{:.3},\"ops\":{},\"ops_per_sec\":{:.1},\"server_cpu_pct\":{},{}}}",
            args.scenario.name(),
            args.conns,
            args.body_size,
            args.pipeline,
            elapsed,
            total,
            ops_per_sec,
            cpu,
            json_ops.join(",")
        );
    }
}
