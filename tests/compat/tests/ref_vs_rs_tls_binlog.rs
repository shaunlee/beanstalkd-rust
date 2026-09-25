//! TLS differential mode plus global binlog mode against `beanstalkd-rs`:
//! every case in `cases/` runs with the reference as server A over
//! plaintext and `beanstalkd-rs` as server B over TLS (B started with a
//! generated `--config` file declaring one TLS listener, throwaway
//! certificate), each server with its own fresh `-b <dir>`.

use bstk_compat::runner::{
    RunOptions, default_parallelism, default_ref_bin, default_rs_bin, discover_cases, run_all_with,
};
use bstk_compat::summarize;

#[test]
fn ref_vs_rs_tls_binlog_all_cases_pass() {
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
        tls_b: true,
        force_binlog: true,
        ..RunOptions::default()
    };
    let results = run_all_with(&cases, &ref_bin, &rs_bin, default_parallelism(), opts);
    let (passed, failed) = summarize(&results);

    assert_eq!(
        failed,
        0,
        "{failed} of {} ref-vs-rs TLS+binlog-mode case(s) failed (see report above); {passed} passed",
        passed + failed
    );
}
