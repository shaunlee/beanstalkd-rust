//! Run every case in `cases/` with the reference C beanstalkd as server A
//! and `beanstalkd-rs` as server B. This is the real differential
//! compatibility suite; it is enabled once `bstk-server` exists (task T4)
//! and driven to 100% pass during task T5.

use std::path::{Path, PathBuf};

use bstk_compat::runner::{
    case_declares_binlog, default_parallelism, default_ref_bin, default_rs_bin, discover_cases,
    run_all,
};
use bstk_compat::summarize;

fn bins() -> (PathBuf, PathBuf) {
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
    (ref_bin, rs_bin)
}

fn run_and_check(cases: &[PathBuf], ref_bin: &Path, rs_bin: &Path, what: &str) {
    assert!(!cases.is_empty(), "no {what} cases found");
    let results = run_all(cases, ref_bin, rs_bin, default_parallelism());
    let (passed, failed) = summarize(&results);
    assert_eq!(
        failed,
        0,
        "{failed} of {} ref-vs-rs {what} case(s) failed (see report above); {passed} passed",
        passed + failed
    );
}

#[test]
fn ref_vs_rs_all_cases_pass() {
    let (ref_bin, rs_bin) = bins();
    let cases = discover_cases().expect("failed to list tests/compat/cases");
    run_and_check(&cases, &ref_bin, &rs_bin, "all");
}

/// The cases that declare `!binlog` (restart / crash recovery).
#[test]
fn ref_vs_rs_binlog_cases_pass() {
    let (ref_bin, rs_bin) = bins();
    let mut cases = discover_cases().expect("failed to list tests/compat/cases");
    cases.retain(|p| case_declares_binlog(p));
    run_and_check(&cases, &ref_bin, &rs_bin, "!binlog");
}
