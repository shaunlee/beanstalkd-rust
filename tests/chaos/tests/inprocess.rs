//! In-process chaos (see `bstk_chaos::inproc`).
//!
//! - `cargo test -p bstk-chaos --test inprocess`: a few seeds (fast);
//! - full run: `BSTK_CHAOS_SEEDS=1000 cargo test --release -p bstk-chaos
//!   --test inprocess full -- --ignored --nocapture` (optionally
//!   `BSTK_CHAOS_FIRST`, `BSTK_CHAOS_JOBS`);
//! - replay one seed: `BSTK_CHAOS_SEED=<seed> cargo test -p bstk-chaos
//!   --test inprocess replay -- --ignored --nocapture`.

#![allow(clippy::unwrap_used)]

use std::time::Instant;

use bstk_chaos::inproc::{Outcome, RunConfig, run};

/// Runs one seed on its own current-thread runtime with paused time.
fn run_seed(cfg: RunConfig) -> Outcome {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .unwrap();
    let out = rt.block_on(run(cfg));
    rt.shutdown_background();
    out
}

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok().and_then(|v| v.parse().ok())
}

fn jobs() -> usize {
    env_u64("BSTK_CHAOS_JOBS")
        .map(|j| j as usize)
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(4, |n| n.get()))
}

fn run_many(first: u64, count: u64, verbose: bool) -> Vec<Outcome> {
    let started = Instant::now();
    let seeds: Vec<u64> = (first..first + count).collect();
    let outs = bstk_chaos::parallel(seeds, jobs(), |seed| {
        let t = Instant::now();
        let o = run_seed(RunConfig::from_seed(seed));
        if verbose || !o.passed() {
            eprintln!(
                "seed {seed}: {} in {:.1?} wall / {:.1?} virtual, {}",
                if o.passed() { "ok" } else { "FAILED" },
                t.elapsed(),
                o.virtual_time,
                o.report.summary().lines().next().unwrap_or_default()
            );
        }
        o
    });
    let failed: Vec<&Outcome> = outs.iter().filter(|o| !o.passed()).collect();
    let mut mix: std::collections::BTreeMap<String, u64> = Default::default();
    let mut nodes5 = 0;
    for o in &outs {
        if o.cfg.nodes == 5 {
            nodes5 += 1;
        }
        for (k, v) in &o.stats {
            if k == "max-term" {
                let e = mix.entry(k.clone()).or_default();
                *e = (*e).max(*v);
            } else {
                *mix.entry(k.clone()).or_default() += v;
            }
        }
    }
    let ops: usize = outs.iter().map(|o| o.report.ops).sum();
    let jobs: usize = outs.iter().map(|o| o.report.jobs).sum();
    eprintln!(
        "{} seeds ({}..{}, {nodes5} with 5 nodes) in {:.1?}: {} failed; {ops} ops / {jobs} jobs \
         checked; mix: {mix:?}",
        outs.len(),
        first,
        first + count,
        started.elapsed(),
        failed.len()
    );
    for o in &failed {
        eprintln!("{}", o.describe());
    }
    outs
}

#[test]
fn smoke() {
    let outs = run_many(0, 4, false);
    let failed: Vec<u64> = outs
        .iter()
        .filter(|o| !o.passed())
        .map(|o| o.cfg.seed)
        .collect();
    assert!(failed.is_empty(), "failed seeds: {failed:?}");
}

#[test]
#[ignore = "full chaos run; see the module docs"]
fn full() {
    let first = env_u64("BSTK_CHAOS_FIRST").unwrap_or(1000);
    let count = env_u64("BSTK_CHAOS_SEEDS").unwrap_or(1000);
    let outs = run_many(first, count, std::env::var("BSTK_CHAOS_VERBOSE").is_ok());
    let failed: Vec<u64> = outs
        .iter()
        .filter(|o| !o.passed())
        .map(|o| o.cfg.seed)
        .collect();
    assert!(failed.is_empty(), "failed seeds: {failed:?}");
}

#[test]
#[ignore = "replays BSTK_CHAOS_SEED"]
fn replay() {
    let seed = env_u64("BSTK_CHAOS_SEED").expect("set BSTK_CHAOS_SEED");
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .try_init();
    let t = Instant::now();
    let o = run_seed(RunConfig::from_seed(seed));
    eprintln!("{}", o.describe());
    eprintln!("{} in {:.1?} wall", o.report.summary(), t.elapsed());
    for e in &o.events {
        eprintln!("  {e}");
    }
    if std::env::var("BSTK_CHAOS_DUMP").is_ok() {
        eprintln!("{}", o.history.dump());
    }
    assert!(o.passed());
}
