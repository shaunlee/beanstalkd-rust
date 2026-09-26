//! Multi-process chaos (see `bstk_chaos::mp`). Needs the `beanstalkd-rs`
//! binary (`cargo build -p bstk-server`, or `BSTK_RS_BIN`).
//!
//! - `cargo test -p bstk-chaos --test multiprocess`: one short run;
//! - full run: `BSTK_CHAOS_MP_RUNS=100 cargo test -p bstk-chaos --test
//!   multiprocess full -- --ignored --nocapture` (optionally
//!   `BSTK_CHAOS_MP_FIRST`, `BSTK_CHAOS_MP_JOBS`, `BSTK_CHAOS_DIR` to keep
//!   the logs of failed runs there, `BSTK_CHAOS_NO_WIPE=1` to leave wipes out);
//! - replay: `BSTK_CHAOS_MP_SEED=<seed> cargo test -p bstk-chaos --test
//!   multiprocess replay -- --ignored --nocapture`.

#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use bstk_chaos::mp::{MpConfig, MpOutcome, run};
use bstk_raft::sim::SimRng;

/// Multi-process runs share the machine: one at a time unless asked.
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

/// Duration of the fault schedule of `seed`: 20–60 s.
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

#[test]
#[ignore = "full multi-process chaos run; see the module docs"]
fn full() {
    let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let first = env_u64("BSTK_CHAOS_MP_FIRST").unwrap_or(100);
    let count = env_u64("BSTK_CHAOS_MP_RUNS").unwrap_or(100);
    let jobs = env_u64("BSTK_CHAOS_MP_JOBS").unwrap_or(1) as usize;
    let started = Instant::now();
    let seeds: Vec<u64> = (first..first + count).collect();
    let outs = bstk_chaos::parallel(seeds, jobs, |seed| {
        let o = run_one(MpConfig::from_seed(seed, duration(seed)));
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
    let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let seed = env_u64("BSTK_CHAOS_MP_SEED").expect("set BSTK_CHAOS_MP_SEED");
    let d = env_u64("BSTK_CHAOS_MP_SECS").map_or_else(|| duration(seed), Duration::from_secs);
    let o = run_one(MpConfig::from_seed(seed, d));
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
