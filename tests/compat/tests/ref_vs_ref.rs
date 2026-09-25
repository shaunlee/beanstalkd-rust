//! Self-test of the differential harness: run every case in `cases/` with
//! the reference C beanstalkd on *both* sides. This must pass 100% of the
//! time; a failure here means the harness itself is unreliable (flaky
//! timing, parser bug, masking bug, ...), not a real client/server
//! incompatibility.

use bstk_compat::runner::{default_parallelism, default_ref_bin, discover_cases, run_all};
use bstk_compat::summarize;

#[test]
fn ref_vs_ref_all_cases_pass() {
    let ref_bin = default_ref_bin();
    assert!(
        ref_bin.exists(),
        "reference beanstalkd binary not found at {}; run scripts/build-ref.sh first \
         (this must FAIL, not skip, when the reference binary is missing)",
        ref_bin.display()
    );

    let cases = discover_cases().expect("failed to list tests/compat/cases");
    assert!(
        !cases.is_empty(),
        "expected at least one .bt case file under tests/compat/cases"
    );

    let results = run_all(&cases, &ref_bin, &ref_bin, default_parallelism());
    let (passed, failed) = summarize(&results);

    assert_eq!(
        failed,
        0,
        "{failed} of {} ref-vs-ref case(s) failed (see report above); {passed} passed",
        passed + failed
    );
}
