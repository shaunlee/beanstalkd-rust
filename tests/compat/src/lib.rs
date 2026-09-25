//! Differential test harness against the reference C beanstalkd (task T3).
//!
//! This crate spawns two server processes running the same case script (a
//! `.bt` file, see [`dsl`]) and diffs their byte-for-byte responses after
//! masking volatile fields (pid, uptime, rusage, ...; see [`mask`]).
//!
//! Entry points:
//! - [`runner::run_case_pair`] runs one case against two binaries.
//! - [`runner::run_all`] runs a whole corpus in parallel.
//! - [`runner::discover_cases`] lists the `.bt` files under `cases/`.
//! - [`runner::default_ref_bin`] / [`runner::default_rs_bin`] resolve the
//!   default binary paths (overridable via `BSTK_REF_BIN` / `BSTK_RS_BIN`).
//!
//! TLS mode ([`runner::RunOptions::tls_b`], `BSTK_COMPAT_TLS=1`) runs
//! server B behind a TLS listener of its own while server A stays
//! plaintext; [`runner::RunOptions::stunnel_b`] instead puts server B behind
//! `stunnel` (used to validate the TLS path with the reference on both
//! sides). See [`server`] and [`conn`] for how each DSL action maps onto
//! TLS.
//!
//! Cluster mode ([`runner::RunOptions::cluster_b`], `BSTK_COMPAT_CLUSTER=
//! leader|follower`) runs server B as a 3-node `beanstalkd-rs` Raft cluster
//! and connects every client to its leader or to one follower; see
//! [`cluster`] for the startup and restart semantics.

pub mod cluster;
pub mod compare;
pub mod conn;
pub mod dsl;
pub mod escape;
pub mod mask;
pub mod runner;
pub mod server;
pub mod tls;

pub use compare::Mismatch;
pub use runner::CaseResult;

/// Render a full, human-readable report for one case result: the case file,
/// then every mismatch with its step line, the last command sent, and the
/// expected (server A) vs actual (server B) outcome.
pub fn format_case_report(result: &CaseResult) -> String {
    use std::fmt::Write as _;

    let mut out = String::new();
    if let Some(err) = &result.harness_error {
        let _ = writeln!(out, "{}: HARNESS ERROR: {err}", result.path.display());
        return out;
    }
    if result.mismatches.is_empty() {
        let _ = writeln!(out, "{}: PASS", result.path.display());
        return out;
    }
    let _ = writeln!(
        out,
        "{}: FAIL ({} mismatch(es))",
        result.path.display(),
        result.mismatches.len()
    );
    for m in &result.mismatches {
        let _ = writeln!(out, "  line {}: {}", m.line, m.step_desc);
        if let Some((send_line, send_desc)) = &m.last_send {
            let _ = writeln!(out, "    last send (line {send_line}): \"{send_desc}\"");
        }
        let _ = writeln!(out, "    expected (A): {}", m.expected);
        let _ = writeln!(out, "    actual   (B): {}", m.actual);
    }
    out
}

/// Summarize a batch of case results as `(passed, failed)` counts and print
/// a one-line-per-case summary plus full reports for failures.
pub fn summarize(results: &[CaseResult]) -> (usize, usize) {
    let mut passed = 0usize;
    let mut failed = 0usize;
    for r in results {
        if r.passed() {
            passed += 1;
        } else {
            failed += 1;
            print!("{}", format_case_report(r));
        }
    }
    (passed, failed)
}
