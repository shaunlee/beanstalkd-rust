//! bstk-bench: a load generator for beanstalkd-compatible servers.
//!
//! ```text
//! bstk-bench --addr HOST:PORT --conns N --duration SECS --scenario <name>
//!            [--body-size B] [--pipeline D] [--threads T] [--json]
//!            [--tls --ca PEM [--server-name NAME]
//!             [--client-cert PEM --client-key PEM] [--token TOKEN]]
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
//! Scaling load (any scenario): `--idle-conns N` opens N extra connections
//! that stay idle for the whole run, and `--delayed-tubes M` creates M
//! extra tubes that each hold one job delayed by an hour. Both are set up
//! before the clock starts; the delayed jobs are deleted after the run.
//! The soft `RLIMIT_NOFILE` is raised as far as needed (up to the hard
//! limit).
//!
//! TLS (for beanstalkd-rs TLS listeners, or a server behind a TLS proxy):
//! `--tls` makes every connection a TLS connection (tokio-rustls, the
//! aws-lc-rs provider) that verifies the server against `--ca` (server
//! name: `--server-name`, default the host part of `--addr`);
//! `--client-cert` / `--client-key` present a client certificate (mTLS);
//! `--token` sends `auth <token>` on every connection before anything
//! else. Connections are set up before the clock starts, so handshakes are
//! not measured. `--idle-conns` is plain TCP only.
//!
//! Any unexpected reply, or no reply within 10 s, fails the run (exit 1).
//! At the end the server's `stats` must show zero ready, reserved, delayed
//! and buried jobs. Server CPU is derived from `rusage-utime` +
//! `rusage-stime` in `stats`, sampled at the start and at the deadline.

mod client;
mod latency;

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use clap::{Parser, ValueEnum};
use tokio::task::JoinSet;
use tokio::time::Instant;

use client::{Client, Reply, Result, Target, TlsOptions, stats_f64, stats_u64, unexpected};
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
    /// Extra connections kept open and idle during the run.
    #[arg(long, default_value_t = 0)]
    idle_conns: usize,
    /// Extra tubes, each holding one job delayed by `DELAYED_JOB_SECS`.
    #[arg(long, default_value_t = 0)]
    delayed_tubes: usize,
    /// Connect with TLS (requires --ca).
    #[arg(long, requires = "ca")]
    tls: bool,
    /// PEM CA bundle that verifies the server certificate.
    #[arg(long, requires = "tls")]
    ca: Option<PathBuf>,
    /// Server name to verify (default: the host part of --addr).
    #[arg(long, requires = "tls")]
    server_name: Option<String>,
    /// PEM client certificate chain (mTLS).
    #[arg(long, requires_all = ["tls", "client_key"])]
    client_cert: Option<PathBuf>,
    /// PEM client private key (mTLS).
    #[arg(long, requires = "client_cert")]
    client_key: Option<PathBuf>,
    /// Token sent with `auth` on every connection (requires --tls).
    #[arg(long, requires = "tls")]
    token: Option<String>,
}

impl Args {
    fn target(&self) -> Result<Target> {
        if !self.tls {
            return Ok(Target::plain(&self.addr));
        }
        let Some(ca) = &self.ca else {
            return Err("--tls needs --ca".into());
        };
        let server_name = match &self.server_name {
            Some(n) => n.clone(),
            None => {
                let (host, _) = self
                    .addr
                    .rsplit_once(':')
                    .ok_or_else(|| format!("--addr {}: expected HOST:PORT", self.addr))?;
                host.trim_start_matches('[')
                    .trim_end_matches(']')
                    .to_owned()
            }
        };
        let client_cert = match (&self.client_cert, &self.client_key) {
            (Some(c), Some(k)) => Some((c.as_path(), k.as_path())),
            _ => None,
        };
        Target::new(
            &self.addr,
            Some(TlsOptions {
                ca,
                server_name,
                client_cert,
            }),
            self.token.clone(),
        )
    }
}

/// Delay of the jobs created by `--delayed-tubes`: far beyond any run.
const DELAYED_JOB_SECS: u32 = 3600;
/// Connections used to create the `--delayed-tubes` jobs and to delete
/// them afterwards.
const SETUP_CONNS: usize = 8;
/// Concurrent connection attempts while opening `--idle-conns` (the
/// listen backlog is small on macOS).
const CONNECT_CONCURRENCY: usize = 32;
/// Retries per idle connection attempt.
const CONNECT_RETRIES: u64 = 50;
/// How long the server may take to register every idle connection.
const SETUP_TIMEOUT: Duration = Duration::from_secs(60);
/// Commands pipelined per write during setup and cleanup.
const SETUP_BATCH: usize = 256;

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
    if args.tls && args.idle_conns > 0 {
        eprintln!("bstk-bench: --idle-conns is not supported with --tls");
        return ExitCode::from(2);
    }
    if args.scenario == Scenario::ProducersConsumers && args.conns < 2 {
        eprintln!("bstk-bench: producers-consumers needs --conns >= 2");
        return ExitCode::from(2);
    }
    raise_nofile_limit(args.conns + args.idle_conns + SETUP_CONNS + 256);
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
    let target = Arc::new(args.target()?);
    let mut monitor = Client::connect(&target).await?;

    // Idle connections first: the reference's per-event work grows with
    // the number of tubes, which slows down its accept loop.
    let idle = open_idle(&args.addr, args.idle_conns).await?;
    if args.idle_conns > 0 {
        // Accepted connections reach the server's stats asynchronously.
        let want = args.idle_conns as u64;
        let give_up = Instant::now() + SETUP_TIMEOUT;
        loop {
            let s = monitor.stats().await?;
            let conns = stats_u64(&s, "current-connections").unwrap_or(0);
            if conns >= want {
                break;
            }
            if Instant::now() >= give_up {
                return Err(format!(
                    "server reports {conns} connections, expected at least {want} idle ones"
                )
                .into());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    let delayed_ids = make_delayed_tubes(&target, args.delayed_tubes, tag, &body).await?;

    // Connect and configure every client before starting the clock.
    let mut clients = Vec::with_capacity(args.conns);
    for i in 0..args.conns {
        let mut c = Client::connect(&target).await?;
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
        drain(&target, args.conns, args.pipeline, tag).await?;
    }
    let drain_secs = start.elapsed().as_secs_f64() - finished;
    delete_ids(&target, &delayed_ids).await?;
    drop(idle);

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

/// Best effort: raises the soft `RLIMIT_NOFILE` to at least `want` (capped
/// at the hard limit).
fn raise_nofile_limit(want: usize) {
    use nix::sys::resource::{Resource, getrlimit, setrlimit};
    let want = want as u64;
    if let Ok((soft, hard)) = getrlimit(Resource::RLIMIT_NOFILE)
        && soft < want
        && let Err(e) = setrlimit(Resource::RLIMIT_NOFILE, want.min(hard), hard)
    {
        eprintln!("bstk-bench: cannot raise RLIMIT_NOFILE from {soft} to {want}: {e}");
    }
}

/// Opens `n` connections that are only held open (never used).
async fn open_idle(addr: &str, n: usize) -> Result<Vec<tokio::net::TcpStream>> {
    let mut idle = Vec::with_capacity(n);
    let mut left = n;
    while left > 0 {
        let batch = left.min(CONNECT_CONCURRENCY);
        let mut set: JoinSet<std::io::Result<tokio::net::TcpStream>> = JoinSet::new();
        for _ in 0..batch {
            let addr = addr.to_string();
            set.spawn(async move {
                let stream = connect_with_retry(&addr).await?;
                // Close with RST, not FIN: 10,000 client sockets in
                // TIME_WAIT would exhaust the ephemeral port range for the
                // next run.
                stream.set_zero_linger()?;
                Ok(stream)
            });
        }
        while let Some(joined) = set.join_next().await {
            let stream = joined
                .map_err(|e| format!("connect task panicked: {e}"))?
                .map_err(|e| format!("idle connection {}: {e}", idle.len()))?;
            idle.push(stream);
        }
        left -= batch;
    }
    Ok(idle)
}

/// Connects, retrying a few times: a burst of connects can overflow the
/// server's listen backlog, which shows up as a reset or refusal.
async fn connect_with_retry(addr: &str) -> std::io::Result<tokio::net::TcpStream> {
    let mut attempt = 0;
    loop {
        match tokio::net::TcpStream::connect(addr).await {
            Ok(s) => return Ok(s),
            Err(_) if attempt < CONNECT_RETRIES => {
                attempt += 1;
                tokio::time::sleep(Duration::from_millis((20 * attempt).min(200))).await;
            }
            Err(e) => return Err(e),
        }
    }
}

/// Creates `n` tubes named `bench-{tag}-delayed-{i}`, each holding one job
/// delayed by `DELAYED_JOB_SECS`; returns the job ids.
async fn make_delayed_tubes(
    target: &Arc<Target>,
    n: usize,
    tag: u32,
    body: &[u8],
) -> Result<Vec<u64>> {
    let mut set: JoinSet<Result<Vec<u64>>> = JoinSet::new();
    for k in 0..SETUP_CONNS.min(n) {
        let target = Arc::clone(target);
        let body = body.to_vec();
        set.spawn(async move {
            let mut c = Client::connect(&target).await?;
            let mine: Vec<usize> = (k..n).step_by(SETUP_CONNS).collect();
            let mut ids = Vec::with_capacity(mine.len());
            for chunk in mine.chunks(SETUP_BATCH) {
                for i in chunk {
                    c.queue(&format!("use bench-{tag}-delayed-{i}"));
                    c.queue_put(0, DELAYED_JOB_SECS, TTR, &body);
                }
                c.flush().await?;
                for _ in chunk {
                    match c.read_reply().await? {
                        Reply::Using => {}
                        r => return Err(unexpected("use", r)),
                    }
                    match c.read_reply().await? {
                        Reply::Inserted(id) => ids.push(id),
                        r => return Err(unexpected("put", r)),
                    }
                }
            }
            Ok(ids)
        });
    }
    let mut ids = Vec::with_capacity(n);
    while let Some(joined) = set.join_next().await {
        ids.extend(joined.map_err(|e| format!("setup task panicked: {e}"))??);
    }
    Ok(ids)
}

/// Deletes the given jobs (pipelined, over `SETUP_CONNS` connections).
async fn delete_ids(target: &Arc<Target>, ids: &[u64]) -> Result<()> {
    let mut set: JoinSet<Result<()>> = JoinSet::new();
    for k in 0..SETUP_CONNS.min(ids.len()) {
        let target = Arc::clone(target);
        let mine: Vec<u64> = ids.iter().copied().skip(k).step_by(SETUP_CONNS).collect();
        set.spawn(async move {
            let mut c = Client::connect(&target).await?;
            let mut rec = Recorder::default();
            for chunk in mine.chunks(SETUP_BATCH) {
                delete_batch(&mut c, &mut rec, chunk).await?;
            }
            Ok(())
        });
    }
    while let Some(joined) = set.join_next().await {
        joined.map_err(|e| format!("cleanup task panicked: {e}"))??;
    }
    Ok(())
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
async fn drain(target: &Arc<Target>, conns: usize, depth: usize, tag: u32) -> Result<()> {
    let tube = format!("bench-{tag}-shared");
    let mut set: JoinSet<Result<()>> = JoinSet::new();
    for _ in 0..conns {
        let target = Arc::clone(target);
        let tube = tube.clone();
        set.spawn(async move {
            let mut c = Client::connect(&target).await?;
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
        "scenario={} addr={} transport={} conns={} duration={}s body={}B pipeline={} idle-conns={} delayed-tubes={}",
        args.scenario.name(),
        args.addr,
        match (args.tls, args.client_cert.is_some(), args.token.is_some()) {
            (false, _, _) => "tcp",
            (true, true, true) => "tls+mtls+token",
            (true, true, false) => "tls+mtls",
            (true, false, true) => "tls+token",
            (true, false, false) => "tls",
        },
        args.conns,
        args.duration,
        args.body_size,
        args.pipeline,
        args.idle_conns,
        args.delayed_tubes
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
            "JSON {{\"scenario\":\"{}\",\"conns\":{},\"body_size\":{},\"pipeline\":{},\"idle_conns\":{},\"delayed_tubes\":{},\"duration_s\":{:.3},\"ops\":{},\"ops_per_sec\":{:.1},\"server_cpu_pct\":{},{}}}",
            args.scenario.name(),
            args.conns,
            args.body_size,
            args.pipeline,
            args.idle_conns,
            args.delayed_tubes,
            elapsed,
            total,
            ops_per_sec,
            cpu,
            json_ops.join(",")
        );
    }
}
