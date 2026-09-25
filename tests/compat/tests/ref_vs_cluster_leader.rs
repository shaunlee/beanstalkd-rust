//! Cluster differential mode, clients on the leader: every case in `cases/`
//! runs with the reference as server A and a fresh 3-node `beanstalkd-rs`
//! cluster as server B, every client connection going to the leader (as of the cluster's start or last restart). See
//! `bstk_compat::cluster` for startup and restart semantics and
//! docs/COMPAT.md, "Cluster mode", for the masks and exclusions.

use std::time::Instant;

use bstk_compat::cluster::{ClusterTarget, exclusion_for};
use bstk_compat::runner::{
    RunOptions, default_parallelism, default_ref_bin, default_rs_bin, discover_cases, run_all_with,
};
use bstk_compat::summarize;

#[test]
fn ref_vs_cluster_leader_all_cases_pass() {
    let ref_bin = default_ref_bin();
    let rs_bin = default_rs_bin();
    assert!(
        ref_bin.exists(),
        "reference beanstalkd binary not found at {}; run scripts/build-ref.sh first",
        ref_bin.display()
    );
    assert!(
        rs_bin.exists(),
        "beanstalkd-rs binary not found at {}; build crates/server first",
        rs_bin.display()
    );

    let cases = discover_cases().expect("failed to list tests/compat/cases");
    assert!(!cases.is_empty(), "expected at least one .bt case file");
    let opts = RunOptions {
        cluster_b: Some(ClusterTarget::Leader),
        ..RunOptions::default()
    };
    let started = Instant::now();
    let results = run_all_with(&cases, &ref_bin, &rs_bin, default_parallelism(), opts);
    println!(
        "ref_vs_cluster_leader: {} case(s) in {:.1?}",
        results.len(),
        started.elapsed()
    );

    // Excluded cases must fail exactly for their stated reason (the
    // cluster refuses the configuration); everything else must pass.
    let (excluded, checked): (Vec<_>, Vec<_>) = results
        .into_iter()
        .partition(|r| exclusion_for(&r.path).is_some());
    for r in &excluded {
        let ex = exclusion_for(&r.path).expect("partitioned on exclusion_for");
        let err = r.harness_error.as_deref().unwrap_or("");
        assert!(
            err.contains(ex.refusal),
            "excluded case {} ({}) no longer fails with \"{}\"; remove its exclusion \
             if it now passes. Result: {}",
            ex.case,
            ex.reason,
            ex.refusal,
            bstk_compat::format_case_report(r)
        );
        println!("excluded (verified): {}: {}", ex.case, ex.reason);
    }
    let (passed, failed) = summarize(&checked);
    assert_eq!(
        failed,
        0,
        "{failed} of {} ref-vs-cluster (leader) case(s) failed (see report above); {passed} passed",
        passed + failed
    );
}
