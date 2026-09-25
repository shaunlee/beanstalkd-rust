//! CLI for manually running the differential compatibility test harness.
//!
//! ```text
//! compat [--a BIN] [--b BIN] [--binlog] [--tls-b] [--stunnel-b] [--show]
//!        [--filter substr] [cases...]
//! ```
//!
//! - `--a BIN` / `--b BIN` override the two server binaries (default:
//!   `BSTK_REF_BIN`/reference build vs `BSTK_RS_BIN`/`beanstalkd-rs` build).
//! - `--binlog` runs every case with a fresh per-server `-b <dir>` (global
//!   binlog mode; also enabled by `BSTK_COMPAT_BINLOG=1`).
//! - `--tls-b` connects to server B over TLS: B is started with a generated
//!   `--config` file declaring one TLS listener (throwaway CA and server
//!   certificate); server A stays plaintext (also enabled by
//!   `BSTK_COMPAT_TLS=1`). Needs a server B with TLS listeners.
//! - `--stunnel-b` runs server B plaintext behind `stunnel`, which
//!   terminates TLS on B's port (validates the TLS path with the reference
//!   as B; `stunnel` from `BSTK_STUNNEL_BIN`, the usual install locations,
//!   or `PATH`).
//! - `--show` prints server A's masked transcript for every case (useful to
//!   see what the reference actually replied when writing a case).
//! - `--filter substr` only runs cases whose file name contains `substr`.
//! - `cases...` are explicit `.bt` file paths (or bare names resolved
//!   against `tests/compat/cases/`); if omitted, every case is run.

use std::path::PathBuf;
use std::process::ExitCode;

use bstk_compat::runner::{
    RunOptions, cases_dir, default_parallelism, default_ref_bin, default_rs_bin, discover_cases,
    run_all_with,
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
    let mut opts = RunOptions::from_env();

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
            "--binlog" => opts.force_binlog = true,
            "--tls-b" => opts.tls_b = true,
            "--stunnel-b" => opts.stunnel_b = true,
            "--show" => opts.keep_transcript = true,
            "-h" | "--help" => {
                println!(
                    "compat [--a BIN] [--b BIN] [--binlog] [--tls-b] [--stunnel-b] [--show] \
                     [--filter substr] [cases...]"
                );
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

    let mut modes = Vec::new();
    if opts.force_binlog {
        modes.push("binlog mode");
    }
    if opts.stunnel_b {
        modes.push("B over TLS via stunnel");
    } else if opts.tls_b {
        modes.push("B over TLS");
    }
    println!(
        "running {} case(s): A={} B={}{}",
        cases.len(),
        bin_a.display(),
        bin_b.display(),
        if modes.is_empty() {
            String::new()
        } else {
            format!(" ({})", modes.join(", "))
        }
    );

    let results = run_all_with(&cases, &bin_a, &bin_b, default_parallelism(), opts);
    if opts.keep_transcript {
        for r in &results {
            println!("=== {}", r.path.display());
            for (line, step, outcome) in &r.transcript {
                println!("{line:>4}  {step:<40}  {outcome}");
            }
        }
    }
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
