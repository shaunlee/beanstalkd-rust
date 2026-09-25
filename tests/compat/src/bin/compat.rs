//! CLI for manually running the differential compatibility test harness.
//!
//! ```text
//! compat [--a BIN] [--b BIN] [--filter substr] [cases...]
//! ```
//!
//! - `--a BIN` / `--b BIN` override the two server binaries (default:
//!   `BSTK_REF_BIN`/reference build vs `BSTK_RS_BIN`/`beanstalkd-rs` build).
//! - `--filter substr` only runs cases whose file name contains `substr`.
//! - `cases...` are explicit `.bt` file paths (or bare names resolved
//!   against `tests/compat/cases/`); if omitted, every case is run.

use std::path::PathBuf;
use std::process::ExitCode;

use bstk_compat::runner::{
    cases_dir, default_parallelism, default_ref_bin, default_rs_bin, discover_cases, run_all,
};
use bstk_compat::summarize;

fn resolve_case_arg(arg: &str) -> PathBuf {
    let direct = PathBuf::from(arg);
    if direct.exists() {
        return direct;
    }
    if arg.ends_with(".bt") {
        cases_dir().join(arg)
    } else {
        cases_dir().join(format!("{arg}.bt"))
    }
}

fn main() -> ExitCode {
    let mut bin_a = default_ref_bin();
    let mut bin_b = default_rs_bin();
    let mut filter: Option<String> = None;
    let mut explicit_cases: Vec<String> = Vec::new();

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--a" => {
                bin_a = PathBuf::from(args.next().expect("--a requires a value"));
            }
            "--b" => {
                bin_b = PathBuf::from(args.next().expect("--b requires a value"));
            }
            "--filter" => {
                filter = Some(args.next().expect("--filter requires a value"));
            }
            "-h" | "--help" => {
                println!("compat [--a BIN] [--b BIN] [--filter substr] [cases...]");
                return ExitCode::SUCCESS;
            }
            other => explicit_cases.push(other.to_string()),
        }
    }

    let mut cases: Vec<PathBuf> = if explicit_cases.is_empty() {
        discover_cases().expect("failed to list cases directory")
    } else {
        explicit_cases.iter().map(|s| resolve_case_arg(s)).collect()
    };

    if let Some(f) = &filter {
        cases.retain(|p| p.to_string_lossy().contains(f.as_str()));
    }

    if cases.is_empty() {
        eprintln!("no cases matched");
        return ExitCode::FAILURE;
    }

    println!(
        "running {} case(s): A={} B={}",
        cases.len(),
        bin_a.display(),
        bin_b.display()
    );

    let results = run_all(&cases, &bin_a, &bin_b, default_parallelism());
    let (passed, failed) = summarize(&results);
    println!("---");
    println!(
        "{passed} passed, {failed} failed, {} total",
        passed + failed
    );

    if failed == 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
