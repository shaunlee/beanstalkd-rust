//! Validation of the harness's TLS path without a TLS-capable server: every
//! case in `cases/` runs with the reference as server A over plaintext and
//! the reference behind `stunnel` as server B (stunnel terminates TLS on
//! B's public port and forwards plaintext to the reference on a private
//! port; every connection to B is a TLS client connection trusting the
//! run's throwaway CA). No extra masks apply. Must pass 100%.
//!
//! Needs a `stunnel` binary (`BSTK_STUNNEL_BIN`, the usual install
//! locations, or `PATH`). Without one the tests print why and pass without
//! running anything; set `BSTK_REQUIRE_STUNNEL=1` to make that a failure.

use bstk_compat::runner::{
    RunOptions, default_parallelism, default_ref_bin, discover_cases, find_stunnel, run_all_with,
};
use bstk_compat::summarize;

fn run(force_binlog: bool, what: &str) {
    if find_stunnel().is_none() {
        let require =
            std::env::var("BSTK_REQUIRE_STUNNEL").is_ok_and(|v| !v.is_empty() && v != "0");
        assert!(
            !require,
            "BSTK_REQUIRE_STUNNEL is set but no stunnel binary was found"
        );
        // Written to the stderr handle directly: `eprintln!` output of a
        // passing test is captured and never shown.
        use std::io::Write as _;
        let _ = writeln!(
            std::io::stderr(),
            "SKIPPED ref_vs_stunnel_ref ({what}): no stunnel binary found \
             (set BSTK_STUNNEL_BIN or install stunnel, e.g. `brew install stunnel`)"
        );
        return;
    }
    let ref_bin = default_ref_bin();
    assert!(
        ref_bin.exists(),
        "reference beanstalkd binary not found at {}; run scripts/build-ref.sh first",
        ref_bin.display()
    );

    let cases = discover_cases().expect("failed to list tests/compat/cases");
    assert!(!cases.is_empty(), "expected at least one .bt case file");
    let opts = RunOptions {
        stunnel_b: true,
        force_binlog,
        ..RunOptions::default()
    };
    let results = run_all_with(&cases, &ref_bin, &ref_bin, default_parallelism(), opts);
    let (passed, failed) = summarize(&results);

    assert_eq!(
        failed,
        0,
        "{failed} of {} ref-vs-stunnel-ref {what} case(s) failed (see report above); {passed} passed",
        passed + failed
    );
}

#[test]
fn ref_vs_stunnel_ref_all_cases_pass() {
    run(false, "all");
}

/// The same with every server given its own fresh `-b <dir>`.
#[test]
fn ref_vs_stunnel_ref_binlog_all_cases_pass() {
    run(true, "binlog-mode");
}
