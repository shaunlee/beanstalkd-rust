//! Multi-process chaos (see `bstk_chaos::mp`). Needs the `beanstalkd-rs`
//! binary (`cargo build -p bstk-server`, or `BSTK_RS_BIN`).
//!
//! - `cargo test -p bstk-chaos --test multiprocess`: one short run;
//! - full run: `BSTK_CHAOS_MP_RUNS=100 cargo test -p bstk-chaos --test
//!   multiprocess full -- --ignored --nocapture` (optionally
//!   `BSTK_CHAOS_MP_FIRST`, `BSTK_CHAOS_MP_JOBS`, `BSTK_CHAOS_DIR` to keep
//!   the logs of failed runs there, `BSTK_CHAOS_NO_WIPE=1` to leave wipes out);
//! - membership scenarios (P6-T7, `bstk_chaos::mp::membership`):
//!   `BSTK_CHAOS_MP_RUNS=20 cargo test -p bstk-chaos --test multiprocess
//!   membership -- --ignored --exact --nocapture` (same variables; runs last
//!   40 to 70 s);
//! - replay: `BSTK_CHAOS_MP_SEED=<seed> cargo test -p bstk-chaos --test
//!   multiprocess replay -- --ignored --nocapture` (`churn_seed` for a seed
//!   of the membership scenarios).

#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use bstk_chaos::mp::{MpConfig, MpOutcome, run};
use bstk_raft::sim::SimRng;

static SERIAL: Mutex<()> = Mutex::new(());

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok().and_then(|v| v.parse().ok())
}

fn run_one(cfg: MpConfig) -> MpOutcome {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let out = rt.block_on(run(cfg));
    rt.shutdown_background();
    out
}

fn duration(seed: u64) -> Duration {
    Duration::from_secs(SimRng::new(seed ^ 0xD0).range(20, 60))
}

#[test]
fn smoke() {
    let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let o = run_one(MpConfig::from_seed(1, Duration::from_secs(8)));
    eprintln!("{}", o.describe());
    eprintln!("{}", o.report.summary().lines().next().unwrap_or_default());
    assert!(o.passed(), "{}", o.describe());
}

fn membership_duration(seed: u64) -> Duration {
    Duration::from_secs(SimRng::new(seed ^ 0xD1).range(40, 70))
}

#[test]
#[ignore = "full multi-process chaos run; see the module docs"]
fn full() {
    run_many(100, 100, |seed| MpConfig::from_seed(seed, duration(seed)));
}

#[test]
#[ignore = "multi-process membership chaos run; see the module docs"]
fn membership() {
    run_many(500, 20, |seed| {
        MpConfig::membership_from_seed(seed, membership_duration(seed))
    });
}

fn run_many(first: u64, count: u64, make: impl Fn(u64) -> MpConfig + Sync) {
    let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let first = env_u64("BSTK_CHAOS_MP_FIRST").unwrap_or(first);
    let count = env_u64("BSTK_CHAOS_MP_RUNS").unwrap_or(count);
    let jobs = env_u64("BSTK_CHAOS_MP_JOBS").unwrap_or(1) as usize;
    let started = Instant::now();
    let seeds: Vec<u64> = (first..first + count).collect();
    let outs = bstk_chaos::parallel(seeds, jobs, |seed| {
        let o = run_one(make(seed));
        eprintln!(
            "mp seed {seed}: {} in {:.1?} ({:?} faults), {}",
            if o.passed() { "ok" } else { "FAILED" },
            o.wall,
            o.cfg.duration,
            o.report.summary().lines().next().unwrap_or_default()
        );
        if !o.passed() {
            eprintln!("{}", o.describe());
        }
        o
    });
    let mut mix: BTreeMap<String, u64> = BTreeMap::new();
    for o in &outs {
        for (k, v) in &o.faults {
            *mix.entry(k.clone()).or_default() += v;
        }
    }
    let failed: Vec<u64> = outs
        .iter()
        .filter(|o| !o.passed())
        .map(|o| o.cfg.seed)
        .collect();
    let ops: usize = outs.iter().map(|o| o.report.ops).sum();
    eprintln!(
        "{} runs in {:.1?}: {} failed {failed:?}; {ops} ops checked; fault mix: {mix:?}",
        outs.len(),
        started.elapsed(),
        failed.len()
    );
    assert!(failed.is_empty(), "failed seeds: {failed:?}");
}

#[test]
#[ignore = "replays BSTK_CHAOS_MP_SEED"]
fn replay() {
    replay_with(duration, MpConfig::from_seed);
}

#[test]
#[ignore = "replays BSTK_CHAOS_MP_SEED of the membership scenarios"]
fn churn_seed() {
    replay_with(membership_duration, MpConfig::membership_from_seed);
}

fn replay_with(default: fn(u64) -> Duration, make: fn(u64, Duration) -> MpConfig) {
    let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let seed = env_u64("BSTK_CHAOS_MP_SEED").expect("set BSTK_CHAOS_MP_SEED");
    let d = env_u64("BSTK_CHAOS_MP_SECS").map_or_else(|| default(seed), Duration::from_secs);
    let o = run_one(make(seed, d));
    eprintln!("{}", o.describe());
    eprintln!("{}", o.report.summary());
    for e in &o.events {
        eprintln!("  {e}");
    }
    if std::env::var("BSTK_CHAOS_DUMP").is_ok() {
        eprintln!("{}", o.history.dump());
    }
    assert!(o.passed());
}

#[test]
fn raft_and_forwards_use_the_configured_peer_addresses() {
    let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let r = rt.block_on(bstk_chaos::mp::check_peer_addresses());
    rt.shutdown_background();
    eprintln!("{r:?}");
    assert!(r.is_ok(), "{r:?}");
}
