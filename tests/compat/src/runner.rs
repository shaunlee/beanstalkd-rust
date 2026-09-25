//! Top-level orchestration: default binary paths, running a single case
//! against a pair of servers, and running a whole corpus in parallel.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;

use crate::compare::{Mismatch, compare_with, format_outcome};
use crate::conn::execute;
use crate::dsl::{CaseFile, parse_case_file};
use crate::mask::MaskMode;
use crate::server::{ServerConfig, spawn};

/// Environment variable enabling the global binlog mode in the `compat`
/// CLI (any value other than empty or `0`).
pub const BINLOG_ENV: &str = "BSTK_COMPAT_BINLOG";

/// Options that apply to every case of a run.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RunOptions {
    /// Global binlog mode: give every server its own fresh `-b <dir>`, as if
    /// every case declared `!binlog` (cases that already do are unchanged).
    pub force_binlog: bool,
    /// Keep server A's (masked) transcript in [`CaseResult::transcript`].
    pub keep_transcript: bool,
}

impl RunOptions {
    /// Default options, with `force_binlog` taken from [`BINLOG_ENV`].
    pub fn from_env() -> Self {
        let force_binlog = std::env::var(BINLOG_ENV)
            .map(|v| !v.is_empty() && v != "0")
            .unwrap_or(false);
        RunOptions {
            force_binlog,
            ..RunOptions::default()
        }
    }
}

/// Repository root, computed at compile time from this crate's manifest
/// directory (`tests/compat`).
fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
}

/// Default path to the reference C beanstalkd binary, overridable via the
/// `BSTK_REF_BIN` environment variable.
pub fn default_ref_bin() -> PathBuf {
    if let Ok(p) = std::env::var("BSTK_REF_BIN") {
        return PathBuf::from(p);
    }
    repo_root().join(".ref/beanstalkd/beanstalkd")
}

/// Default path to the `beanstalkd-rs` binary, overridable via the
/// `BSTK_RS_BIN` environment variable. Honors `CARGO_TARGET_DIR` when set,
/// since sibling agents may build into a non-default target directory.
pub fn default_rs_bin() -> PathBuf {
    if let Ok(p) = std::env::var("BSTK_RS_BIN") {
        return PathBuf::from(p);
    }
    let target_dir = std::env::var("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| repo_root().join("target"));
    target_dir.join("debug").join("beanstalkd-rs")
}

/// The `tests/compat/cases` directory.
pub fn cases_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("cases")
}

/// List all `*.bt` case files under `cases_dir()`, sorted by path.
pub fn discover_cases() -> std::io::Result<Vec<PathBuf>> {
    let mut cases = Vec::new();
    for entry in std::fs::read_dir(cases_dir())? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some("bt") {
            cases.push(path);
        }
    }
    cases.sort();
    Ok(cases)
}

/// The outcome of running one case against a pair of servers.
pub struct CaseResult {
    pub name: String,
    pub path: PathBuf,
    /// `None` if the case ran to completion; `Some` describes a harness-level
    /// failure (parse error, missing binary, spawn failure) that prevented
    /// the case from being executed at all. Such a case always counts as a
    /// failure.
    pub harness_error: Option<String>,
    pub mismatches: Vec<Mismatch>,
    /// Server A's transcript as `(line, step, outcome)`, masked; only
    /// filled when [`RunOptions::keep_transcript`] is set.
    pub transcript: Vec<(u32, String, String)>,
}

impl CaseResult {
    pub fn passed(&self) -> bool {
        self.harness_error.is_none() && self.mismatches.is_empty()
    }
}

fn error_result(path: &Path, message: String) -> CaseResult {
    CaseResult {
        name: path
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| path.to_string_lossy().into_owned()),
        path: path.to_path_buf(),
        harness_error: Some(message),
        mismatches: Vec::new(),
        transcript: Vec::new(),
    }
}

/// Run a single case file against server binaries `bin_a` and `bin_b`.
///
/// A missing binary is a hard, loud failure (never a skip): the reference
/// binary in particular must always be present (built by
/// `scripts/build-ref.sh`).
pub fn run_case_pair(case_path: &Path, bin_a: &Path, bin_b: &Path) -> CaseResult {
    run_case_pair_with(case_path, bin_a, bin_b, RunOptions::default())
}

/// Like [`run_case_pair`], with explicit [`RunOptions`].
pub fn run_case_pair_with(
    case_path: &Path,
    bin_a: &Path,
    bin_b: &Path,
    opts: RunOptions,
) -> CaseResult {
    let case: CaseFile = match parse_case_file(case_path) {
        Ok(c) => c,
        Err(e) => return error_result(case_path, format!("parse error: {e}")),
    };

    if !bin_a.exists() {
        return error_result(
            case_path,
            format!("server A binary not found: {}", bin_a.display()),
        );
    }
    if !bin_b.exists() {
        return error_result(
            case_path,
            format!("server B binary not found: {}", bin_b.display()),
        );
    }

    let binlog = case.binlog || opts.force_binlog;
    let mode = MaskMode { binlog };
    let cfg = |bin| ServerConfig {
        bin,
        extra_args: &case.extra_args,
        binlog,
    };
    let mut server_a = match spawn(&cfg(bin_a)) {
        Ok(s) => s,
        Err(e) => return error_result(case_path, format!("failed to start server A: {e}")),
    };
    let mut server_b = match spawn(&cfg(bin_b)) {
        Ok(s) => s,
        Err(e) => return error_result(case_path, format!("failed to start server B: {e}")),
    };

    let steps = &case.steps;

    let (outcomes_a, outcomes_b) = thread::scope(|scope| {
        let handle_a = scope.spawn(|| execute(steps, &mut server_a));
        let handle_b = scope.spawn(|| execute(steps, &mut server_b));
        (
            handle_a.join().expect("server A execution thread panicked"),
            handle_b.join().expect("server B execution thread panicked"),
        )
    });

    // Servers are killed (and binlog directories removed) here, before we
    // finish comparing.
    drop(server_a);
    drop(server_b);

    let mismatches = compare_with(steps, &outcomes_a, &outcomes_b, mode);
    let transcript = if opts.keep_transcript {
        steps
            .iter()
            .zip(&outcomes_a)
            .map(|(step, o)| (step.line, step.kind.describe(), format_outcome(o, mode)))
            .collect()
    } else {
        Vec::new()
    };
    CaseResult {
        name: case.name(),
        path: case_path.to_path_buf(),
        harness_error: None,
        mismatches,
        transcript,
    }
}

/// Run every case in `case_paths` against `bin_a`/`bin_b`, using up to
/// `max_parallel` worker threads. Each case gets a fresh pair of server
/// processes.
pub fn run_all(
    case_paths: &[PathBuf],
    bin_a: &Path,
    bin_b: &Path,
    max_parallel: usize,
) -> Vec<CaseResult> {
    run_all_with(
        case_paths,
        bin_a,
        bin_b,
        max_parallel,
        RunOptions::default(),
    )
}

/// Like [`run_all`], with explicit [`RunOptions`].
pub fn run_all_with(
    case_paths: &[PathBuf],
    bin_a: &Path,
    bin_b: &Path,
    max_parallel: usize,
    opts: RunOptions,
) -> Vec<CaseResult> {
    let max_parallel = max_parallel.max(1);
    let queue: Arc<Mutex<Vec<PathBuf>>> = Arc::new(Mutex::new(case_paths.to_vec()));
    let results: Arc<Mutex<Vec<CaseResult>>> = Arc::new(Mutex::new(Vec::new()));

    thread::scope(|scope| {
        for _ in 0..max_parallel {
            let queue = Arc::clone(&queue);
            let results = Arc::clone(&results);
            scope.spawn(move || {
                loop {
                    let next = { queue.lock().expect("case queue lock poisoned").pop() };
                    let Some(path) = next else { break };
                    let result = run_case_pair_with(&path, bin_a, bin_b, opts);
                    results.lock().expect("results lock poisoned").push(result);
                }
            });
        }
    });

    let mut results = Arc::try_unwrap(results)
        .unwrap_or_else(|_| panic!("worker threads should have finished by now"))
        .into_inner()
        .expect("results lock poisoned");
    results.sort_by(|a, b| a.path.cmp(&b.path));
    results
}

/// Whether the case file at `path` declares `!binlog`. A case that fails to
/// parse counts as not declaring it (running it reports the parse error).
pub fn case_declares_binlog(path: &Path) -> bool {
    parse_case_file(path).map(|c| c.binlog).unwrap_or(false)
}

/// A reasonable default worker count: enough to keep several case pairs
/// in flight, capped to avoid spawning an excessive number of processes.
/// Overridable via `BSTK_COMPAT_JOBS` (mainly for diagnosing flakiness).
pub fn default_parallelism() -> usize {
    if let Ok(v) = std::env::var("BSTK_COMPAT_JOBS")
        && let Ok(n) = v.parse::<usize>()
    {
        return n.max(1);
    }
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        .min(8)
}
