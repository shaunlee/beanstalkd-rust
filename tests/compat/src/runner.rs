//! Top-level orchestration: default binary paths, running a single case
//! against a pair of servers, and running a whole corpus in parallel.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;

use crate::compare::{Mismatch, compare_with, format_outcome};
use crate::conn::execute;
use crate::dsl::{CaseFile, parse_case_file};
use crate::mask::MaskMode;
use crate::server::{ServerConfig, ServerTransport, spawn};
use crate::tls::TlsMaterial;

/// Environment variable enabling the global binlog mode in the `compat`
/// CLI (any value other than empty or `0`).
pub const BINLOG_ENV: &str = "BSTK_COMPAT_BINLOG";

/// Environment variable enabling TLS mode for server B in the `compat` CLI
/// (any value other than empty or `0`); see [`RunOptions::tls_b`].
pub const TLS_ENV: &str = "BSTK_COMPAT_TLS";

/// Environment variable overriding the path of the `stunnel` binary used by
/// [`RunOptions::stunnel_b`].
pub const STUNNEL_BIN_ENV: &str = "BSTK_STUNNEL_BIN";

/// Options that apply to every case of a run.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RunOptions {
    /// Global binlog mode: give every server its own fresh `-b <dir>`, as if
    /// every case declared `!binlog` (cases that already do are unchanged).
    pub force_binlog: bool,
    /// Keep server A's (masked) transcript in [`CaseResult::transcript`].
    pub keep_transcript: bool,
    /// TLS mode: server B is started with a generated `--config` file
    /// declaring one TLS listener (throwaway certificate, generated once
    /// per run) and every connection to it is a TLS client connection.
    /// Server A stays plaintext. Requires server B to support TLS
    /// listeners (`beanstalkd-rs`, from task P2-T4).
    pub tls_b: bool,
    /// Stunnel mode: server B runs plaintext behind `stunnel`, which
    /// terminates TLS on B's public port; every connection to B is a TLS
    /// client connection. Meant for validating the harness's TLS path with
    /// the reference as server B. Takes precedence over `tls_b`. No extra
    /// masks apply: stunnel opens exactly one backend connection per client
    /// connection (and none for the readiness check), so even the
    /// connection counts in `stats` match.
    pub stunnel_b: bool,
}

impl RunOptions {
    /// Default options, with `force_binlog` taken from [`BINLOG_ENV`] and
    /// `tls_b` from [`TLS_ENV`].
    pub fn from_env() -> Self {
        RunOptions {
            force_binlog: env_flag(BINLOG_ENV),
            tls_b: env_flag(TLS_ENV),
            ..RunOptions::default()
        }
    }
}

fn env_flag(name: &str) -> bool {
    std::env::var(name)
        .map(|v| !v.is_empty() && v != "0")
        .unwrap_or(false)
}

/// Locate a `stunnel` binary: [`STUNNEL_BIN_ENV`] if set, else the usual
/// install locations, else the first `stunnel` on `PATH`.
pub fn find_stunnel() -> Option<PathBuf> {
    if let Ok(p) = std::env::var(STUNNEL_BIN_ENV) {
        return Some(PathBuf::from(p));
    }
    let known = [
        "/opt/homebrew/bin/stunnel",
        "/usr/local/bin/stunnel",
        "/usr/bin/stunnel",
        "/usr/sbin/stunnel",
    ];
    let on_path = std::env::var_os("PATH")
        .map(|p| {
            std::env::split_paths(&p)
                .map(|d| d.join("stunnel"))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    known
        .iter()
        .map(PathBuf::from)
        .chain(on_path)
        .find(|p| p.is_file())
}

/// Per-run shared state derived from [`RunOptions`]: the transport of
/// server B (with its TLS material, generated once per run), or the error
/// that prevented setting it up.
struct RunContext {
    opts: RunOptions,
    transport_b: Result<ServerTransport, String>,
}

impl RunContext {
    fn new(opts: RunOptions) -> Self {
        let transport_b = if opts.stunnel_b {
            match find_stunnel() {
                Some(stunnel) if !stunnel.is_file() => {
                    Err(format!("stunnel binary not found: {}", stunnel.display()))
                }
                Some(stunnel) => TlsMaterial::generate().map(|m| ServerTransport::Stunnel {
                    stunnel,
                    material: Arc::new(m),
                }),
                None => Err(format!(
                    "stunnel mode requested but no stunnel binary was found \
                     (set {STUNNEL_BIN_ENV} or install stunnel)"
                )),
            }
        } else if opts.tls_b {
            TlsMaterial::generate().map(|m| ServerTransport::Tls(Arc::new(m)))
        } else {
            Ok(ServerTransport::Plain)
        };
        RunContext { opts, transport_b }
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
    run_case_pair_ctx(case_path, bin_a, bin_b, &RunContext::new(opts))
}

fn run_case_pair_ctx(case_path: &Path, bin_a: &Path, bin_b: &Path, ctx: &RunContext) -> CaseResult {
    let opts = ctx.opts;
    let transport_b = match &ctx.transport_b {
        Ok(t) => t.clone(),
        Err(e) => return error_result(case_path, format!("TLS setup for server B failed: {e}")),
    };
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
    // No transport-specific masks: TLS (and stunnel) must be invisible in
    // every response, including the connection counts in `stats`.
    let mode = MaskMode { binlog };
    let cfg = |bin, transport| ServerConfig {
        bin,
        extra_args: &case.extra_args,
        binlog,
        transport,
    };
    let mut server_a = match spawn(&cfg(bin_a, ServerTransport::Plain)) {
        Ok(s) => s,
        Err(e) => return error_result(case_path, format!("failed to start server A: {e}")),
    };
    let mut server_b = match spawn(&cfg(bin_b, transport_b)) {
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
    let ctx = RunContext::new(opts);
    let ctx = &ctx;
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
                    let result = run_case_pair_ctx(&path, bin_a, bin_b, ctx);
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
