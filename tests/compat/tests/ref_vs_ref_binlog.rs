//! Self-test of the harness in global binlog mode: every case in `cases/`
//! runs with the reference on both sides, each server with its own fresh
//! `-b <dir>` (as if every case declared `!binlog`). Must pass 100%.

use bstk_compat::runner::{
    RunOptions, default_parallelism, default_ref_bin, discover_cases, run_all_with,
};
use bstk_compat::summarize;

#[test]
fn ref_vs_ref_binlog_all_cases_pass() {
    let ref_bin = default_ref_bin();
    assert!(
        ref_bin.exists(),
        "reference beanstalkd binary not found at {}; run scripts/build-ref.sh first \
         (this must FAIL, not skip, when the reference binary is missing)",
        ref_bin.display()
    );

    let cases = discover_cases().expect("failed to list tests/compat/cases");
    assert!(!cases.is_empty(), "expected at least one .bt case file");

    let opts = RunOptions {
        force_binlog: true,
        ..RunOptions::default()
    };
    let results = run_all_with(&cases, &ref_bin, &ref_bin, default_parallelism(), opts);
    let (passed, failed) = summarize(&results);

    assert_eq!(
        failed,
        0,
        "{failed} of {} ref-vs-ref binlog-mode case(s) failed (see report above); {passed} passed",
        passed + failed
    );
}
