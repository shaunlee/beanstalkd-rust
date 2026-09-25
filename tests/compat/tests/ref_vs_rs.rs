//! Run every case in `cases/` with the reference C beanstalkd as server A
//! and `beanstalkd-rs` as server B. This is the real differential
//! compatibility suite; it is enabled once `bstk-server` exists (task T4)
//! and driven to 100% pass during task T5.

use bstk_compat::runner::{
    default_parallelism, default_ref_bin, default_rs_bin, discover_cases, run_all,
};
use bstk_compat::summarize;

#[test]
fn ref_vs_rs_all_cases_pass() {
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
    let results = run_all(&cases, &ref_bin, &rs_bin, default_parallelism());
    let (passed, failed) = summarize(&results);

    assert_eq!(
        failed,
        0,
        "{failed} of {} ref-vs-rs case(s) failed (see report above); {passed} passed",
        passed + failed
    );
}
