//! Rendering of an engine monitoring [`Snapshot`] for the HTTP endpoints
//! (docs/PLAN.md §5.3 decision 5): Prometheus text exposition format 0.0.4
//! for `/metrics` and a read-only JSON document for `/admin`.
//!
//! Both renderers are pure functions of the snapshot, so every value they
//! print is exactly what `stats` / `stats-tube` report at the same instant.
//!
//! # Prometheus metrics
//!
//! This mapping is public API for operators; do not rename metrics
//! lightly. Counters end in `_total`; everything else is a gauge.
//!
//! ## Server (`stats`)
//!
//! | stats key | metric | type |
//! |---|---|---|
//! | `current-jobs-urgent` | `beanstalkd_current_jobs{state="urgent"}` | gauge |
//! | `current-jobs-ready` | `beanstalkd_current_jobs{state="ready"}` | gauge |
//! | `current-jobs-reserved` | `beanstalkd_current_jobs{state="reserved"}` | gauge |
//! | `current-jobs-delayed` | `beanstalkd_current_jobs{state="delayed"}` | gauge |
//! | `current-jobs-buried` | `beanstalkd_current_jobs{state="buried"}` | gauge |
//! | `cmd-<name>` (all 22) | `beanstalkd_commands_total{cmd="<name>"}` | counter |
//! | `job-timeouts` | `beanstalkd_job_timeouts_total` | counter |
//! | `total-jobs` | `beanstalkd_jobs_total` | counter |
//! | `max-job-size` | `beanstalkd_max_job_size_bytes` | gauge |
//! | `current-tubes` | `beanstalkd_current_tubes` | gauge |
//! | `current-connections` | `beanstalkd_current_connections` | gauge |
//! | `current-producers` | `beanstalkd_current_producers` | gauge |
//! | `current-workers` | `beanstalkd_current_workers` | gauge |
//! | `current-waiting` | `beanstalkd_current_waiting` | gauge |
//! | `total-connections` | `beanstalkd_connections_total` | counter |
//! | `version` | `beanstalkd_build_info{version="<version>"}` (always 1) | gauge |
//! | `rusage-utime` | `beanstalkd_cpu_seconds_total{mode="user"}` | counter |
//! | `rusage-stime` | `beanstalkd_cpu_seconds_total{mode="system"}` | counter |
//! | `uptime` | `beanstalkd_uptime_seconds` | gauge |
//! | `binlog-oldest-index` | `beanstalkd_binlog_oldest_index` | gauge |
//! | `binlog-current-index` | `beanstalkd_binlog_current_index` | gauge |
//! | `binlog-records-migrated` | `beanstalkd_binlog_records_migrated_total` | counter |
//! | `binlog-records-written` | `beanstalkd_binlog_records_written_total` | counter |
//! | `binlog-max-size` | `beanstalkd_binlog_max_size_bytes` | gauge |
//! | `draining` | `beanstalkd_draining` (0 or 1) | gauge |
//!
//! `<name>` in `cmd` is the stats key without its `cmd-` prefix, i.e. the
//! protocol command name: `put`, `peek`, `peek-ready`, `peek-delayed`,
//! `peek-buried`, `reserve`, `reserve-with-timeout`, `delete`, `release`,
//! `use`, `watch`, `ignore`, `bury`, `kick`, `touch`, `stats`, `stats-job`,
//! `stats-tube`, `list-tubes`, `list-tube-used`, `list-tubes-watched`,
//! `pause-tube`. Note that `urgent` jobs are a subset of `ready` jobs (as in
//! `stats`), so summing `beanstalkd_current_jobs` over `state` double-counts.
//!
//! Not exported (identity rather than measurements; see `/admin`): `pid`,
//! `id`, `hostname`, `os`, `platform`.
//!
//! The job, connection and tube gauges are named `beanstalkd_current_*`
//! after their stats keys, which also keeps every gauge name distinct from
//! every counter's base name (`beanstalkd_jobs_total` is a counter whose
//! OpenMetrics family would be `beanstalkd_jobs`).
//!
//! ## Per tube (`stats-tube`), label `tube="<name>"`
//!
//! | stats-tube key | metric | type |
//! |---|---|---|
//! | `current-jobs-urgent` | `beanstalkd_tube_current_jobs{state="urgent"}` | gauge |
//! | `current-jobs-ready` | `beanstalkd_tube_current_jobs{state="ready"}` | gauge |
//! | `current-jobs-reserved` | `beanstalkd_tube_current_jobs{state="reserved"}` | gauge |
//! | `current-jobs-delayed` | `beanstalkd_tube_current_jobs{state="delayed"}` | gauge |
//! | `current-jobs-buried` | `beanstalkd_tube_current_jobs{state="buried"}` | gauge |
//! | `total-jobs` | `beanstalkd_tube_jobs_total` | counter |
//! | `current-using` | `beanstalkd_tube_current_using` | gauge |
//! | `current-watching` | `beanstalkd_tube_current_watching` | gauge |
//! | `current-waiting` | `beanstalkd_tube_current_waiting` | gauge |
//! | `cmd-delete` | `beanstalkd_tube_commands_total{cmd="delete"}` | counter |
//! | `cmd-pause-tube` | `beanstalkd_tube_commands_total{cmd="pause-tube"}` | counter |
//! | `pause` | `beanstalkd_tube_pause_seconds` | gauge |
//! | `pause-time-left` | `beanstalkd_tube_pause_time_left_seconds` | gauge |
//!
//! Tube counters restart from zero when a tube is destroyed (no users,
//! watchers or jobs) and later recreated; Prometheus treats that as a
//! counter reset.
//!
//! ## Cardinality cap
//!
//! Per-tube series are emitted for at most `max_tube_series` tubes: the
//! first ones in `list-tubes` order. Two gauges describe the cap:
//!
//! | metric | meaning |
//! |---|---|
//! | `beanstalkd_tube_series_limit` | the configured `max_tube_series` |
//! | `beanstalkd_tube_series_truncated` | 1 if some tubes were left out, else 0 |
//!
//! `beanstalkd_current_tubes` always reports the full tube count.
//!
//! The snapshot may already hold only some of the tubes (the HTTP listener
//! asks the engine for `max_tube_series + 1` of them, see
//! `Engine::snapshot_limited`): "truncated" means that the snapshot has more
//! tubes than the limit, which that extra tube tells.
//!
//! ## Server-side (beanstalkd-rs only, not in `stats`)
//!
//! | metric | type | meaning |
//! |---|---|---|
//! | `beanstalkd_pending_connections` | gauge | TLS connections in their handshake or awaiting token authentication |
//! | `beanstalkd_pending_rejected_total` | counter | TLS connections closed at accept because `server.max_pending_connections` was reached |
//! | `beanstalkd_auth_timeouts_total` | counter | token connections closed for not authenticating within `auth.timeout` |
//! | `beanstalkd_auth_failures_total` | counter | wrong tokens and commands sent before authentication |
//!
//! # Admin JSON
//!
//! `{"server": {...}, "server_rs": {...}, "tube_limit": N,
//! "tubes_truncated": bool, "tubes": [{...}, ...]}` where `server` holds
//! every `stats` key and each `tubes` entry every `stats-tube` key, with the
//! reference's key names in the reference's order. Numbers are JSON numbers
//! (`rusage-utime` / `rusage-stime` as seconds with six decimals), `draining`
//! is a boolean, and `version`, `id`, `hostname`, `os`, `platform` and the
//! tube `name` are strings. Tubes appear in `list-tubes` order, at most
//! `tube_limit` (`http.max_tube_series`) of them; `tubes_truncated` tells
//! whether some were left out. `server_rs` holds the server-side counters
//! above as `pending-connections`, `pending-rejected`, `auth-timeouts` and
//! `auth-failures` (cumulative ones without the `_total` suffix, like the
//! `stats` keys).

use bstk_engine::Snapshot;
use bstk_proto::{StatsServer, StatsTube};

/// Server-side counters of beanstalkd-rs that `stats` does not have (see
/// `pending::ServerCounters`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ServerRsStats {
    pub pending_connections: u64,
    pub pending_rejected: u64,
    pub auth_timeouts: u64,
    pub auth_failures: u64,
}

/// Server-side metrics: (name, help, kind, value).
type RsScalar = (&'static str, &'static str, Kind, fn(&ServerRsStats) -> u64);

const RS_SCALARS: [RsScalar; 4] = [
    (
        "beanstalkd_pending_connections",
        "TLS connections in their handshake or awaiting token authentication (not in stats).",
        Kind::Gauge,
        |r| r.pending_connections,
    ),
    (
        "beanstalkd_pending_rejected_total",
        "TLS connections closed at accept because server.max_pending_connections was reached.",
        Kind::Counter,
        |r| r.pending_rejected,
    ),
    (
        "beanstalkd_auth_timeouts_total",
        "Token-auth connections closed for not authenticating within auth.timeout.",
        Kind::Counter,
        |r| r.auth_timeouts,
    ),
    (
        "beanstalkd_auth_failures_total",
        "Wrong tokens and commands sent before authentication.",
        Kind::Counter,
        |r| r.auth_failures,
    ),
];

/// The tubes to show under a limit of `limit`, and whether some are left
/// out.
fn shown_tubes(s: &Snapshot, limit: usize) -> (&[StatsTube], bool) {
    let shown = &s.tubes[..s.tubes.len().min(limit)];
    (shown, shown.len() < s.tubes.len())
}

const TEXT_OVERHEAD_PER_TUBE: usize = 1024;
const TEXT_OVERHEAD_SERVER: usize = 8192;

#[derive(Clone, Copy)]
enum Kind {
    Counter,
    Gauge,
}

impl Kind {
    fn as_str(self) -> &'static str {
        match self {
            Kind::Counter => "counter",
            Kind::Gauge => "gauge",
        }
    }
}

/// A server metric sample label value and the field it reads.
type ServerLabelled = (&'static str, fn(&StatsServer) -> u64);
/// A per-tube metric sample label value and the field it reads.
type TubeLabelled = (&'static str, fn(&StatsTube) -> u64);

/// `cmd-*` stats keys, labelled by command name, in STATS_FMT order.
const SERVER_COMMANDS: [ServerLabelled; 22] = [
    ("put", |s| s.cmd_put),
    ("peek", |s| s.cmd_peek),
    ("peek-ready", |s| s.cmd_peek_ready),
    ("peek-delayed", |s| s.cmd_peek_delayed),
    ("peek-buried", |s| s.cmd_peek_buried),
    ("reserve", |s| s.cmd_reserve),
    ("reserve-with-timeout", |s| s.cmd_reserve_with_timeout),
    ("delete", |s| s.cmd_delete),
    ("release", |s| s.cmd_release),
    ("use", |s| s.cmd_use),
    ("watch", |s| s.cmd_watch),
    ("ignore", |s| s.cmd_ignore),
    ("bury", |s| s.cmd_bury),
    ("kick", |s| s.cmd_kick),
    ("touch", |s| s.cmd_touch),
    ("stats", |s| s.cmd_stats),
    ("stats-job", |s| s.cmd_stats_job),
    ("stats-tube", |s| s.cmd_stats_tube),
    ("list-tubes", |s| s.cmd_list_tubes),
    ("list-tube-used", |s| s.cmd_list_tube_used),
    ("list-tubes-watched", |s| s.cmd_list_tubes_watched),
    ("pause-tube", |s| s.cmd_pause_tube),
];

const SERVER_JOB_STATES: [ServerLabelled; 5] = [
    ("urgent", |s| s.current_jobs_urgent),
    ("ready", |s| s.current_jobs_ready),
    ("reserved", |s| s.current_jobs_reserved),
    ("delayed", |s| s.current_jobs_delayed),
    ("buried", |s| s.current_jobs_buried),
];

/// Unlabelled server metrics: (name, help, kind, value).
type ServerScalar = (&'static str, &'static str, Kind, fn(&StatsServer) -> u64);

const SERVER_SCALARS: [ServerScalar; 16] = [
    (
        "beanstalkd_job_timeouts_total",
        "Reserved jobs whose TTR expired (stats job-timeouts).",
        Kind::Counter,
        |s| s.job_timeouts,
    ),
    (
        "beanstalkd_jobs_total",
        "Jobs created since startup (stats total-jobs).",
        Kind::Counter,
        |s| s.total_jobs,
    ),
    (
        "beanstalkd_max_job_size_bytes",
        "Maximum job body size in bytes (stats max-job-size).",
        Kind::Gauge,
        |s| s.max_job_size,
    ),
    (
        "beanstalkd_current_tubes",
        "Tubes currently in existence (stats current-tubes).",
        Kind::Gauge,
        |s| s.current_tubes,
    ),
    (
        "beanstalkd_current_connections",
        "Open connections (stats current-connections).",
        Kind::Gauge,
        |s| s.current_connections,
    ),
    (
        "beanstalkd_current_producers",
        "Open connections that have issued a put (stats current-producers).",
        Kind::Gauge,
        |s| s.current_producers,
    ),
    (
        "beanstalkd_current_workers",
        "Open connections that have issued a reserve (stats current-workers).",
        Kind::Gauge,
        |s| s.current_workers,
    ),
    (
        "beanstalkd_current_waiting",
        "Connections waiting in a reserve (stats current-waiting).",
        Kind::Gauge,
        |s| s.current_waiting,
    ),
    (
        "beanstalkd_connections_total",
        "Connections accepted since startup (stats total-connections).",
        Kind::Counter,
        |s| s.total_connections,
    ),
    (
        "beanstalkd_uptime_seconds",
        "Seconds since the server started (stats uptime).",
        Kind::Gauge,
        |s| s.uptime,
    ),
    (
        "beanstalkd_binlog_oldest_index",
        "Index of the oldest binlog file needed (stats binlog-oldest-index).",
        Kind::Gauge,
        |s| s.binlog_oldest_index,
    ),
    (
        "beanstalkd_binlog_current_index",
        "Index of the binlog file being written (stats binlog-current-index).",
        Kind::Gauge,
        |s| s.binlog_current_index,
    ),
    (
        "beanstalkd_binlog_records_migrated_total",
        "Binlog records rewritten by compaction (stats binlog-records-migrated).",
        Kind::Counter,
        |s| s.binlog_records_migrated,
    ),
    (
        "beanstalkd_binlog_records_written_total",
        "Binlog records written (stats binlog-records-written).",
        Kind::Counter,
        |s| s.binlog_records_written,
    ),
    (
        "beanstalkd_binlog_max_size_bytes",
        "Maximum binlog file size in bytes (stats binlog-max-size).",
        Kind::Gauge,
        |s| s.binlog_max_size,
    ),
    (
        "beanstalkd_draining",
        "1 if the server is in drain mode (stats draining), else 0.",
        Kind::Gauge,
        |s| u64::from(s.draining),
    ),
];

const TUBE_JOB_STATES: [TubeLabelled; 5] = [
    ("urgent", |t| t.current_jobs_urgent),
    ("ready", |t| t.current_jobs_ready),
    ("reserved", |t| t.current_jobs_reserved),
    ("delayed", |t| t.current_jobs_delayed),
    ("buried", |t| t.current_jobs_buried),
];

const TUBE_COMMANDS: [TubeLabelled; 2] = [
    ("delete", |t| t.cmd_delete),
    ("pause-tube", |t| t.cmd_pause_tube),
];

/// Per-tube metrics with only the `tube` label: (name, help, kind, value).
type TubeScalar = (&'static str, &'static str, Kind, fn(&StatsTube) -> u64);

const TUBE_SCALARS: [TubeScalar; 6] = [
    (
        "beanstalkd_tube_jobs_total",
        "Jobs created in the tube (stats-tube total-jobs).",
        Kind::Counter,
        |t| t.total_jobs,
    ),
    (
        "beanstalkd_tube_current_using",
        "Connections using the tube (stats-tube current-using).",
        Kind::Gauge,
        |t| t.current_using,
    ),
    (
        "beanstalkd_tube_current_watching",
        "Connections watching the tube (stats-tube current-watching).",
        Kind::Gauge,
        |t| t.current_watching,
    ),
    (
        "beanstalkd_tube_current_waiting",
        "Connections waiting in a reserve on the tube (stats-tube current-waiting).",
        Kind::Gauge,
        |t| t.current_waiting,
    ),
    (
        "beanstalkd_tube_pause_seconds",
        "Duration of the current pause in seconds (stats-tube pause).",
        Kind::Gauge,
        |t| t.pause,
    ),
    (
        "beanstalkd_tube_pause_time_left_seconds",
        "Seconds until the tube is unpaused (stats-tube pause-time-left).",
        Kind::Gauge,
        |t| t.pause_time_left,
    ),
];

/// Renders `s` in the Prometheus text exposition format 0.0.4 (serve it
/// with `Content-Type: text/plain; version=0.0.4; charset=utf-8`).
/// Per-tube series cover only the first `max_tube_series` tubes in
/// `list-tubes` order; `rs` adds the server-side metrics. See the module
/// docs for the full metric list.
pub fn render_prometheus(s: &Snapshot, max_tube_series: usize, rs: &ServerRsStats) -> String {
    let srv = &s.server;
    let (shown, truncated) = shown_tubes(s, max_tube_series);
    let mut out = String::with_capacity(
        TEXT_OVERHEAD_SERVER + shown.len().saturating_mul(TEXT_OVERHEAD_PER_TUBE),
    );

    family(
        &mut out,
        "beanstalkd_current_jobs",
        "Jobs by state (stats current-jobs-*); urgent jobs are also counted as ready.",
        Kind::Gauge,
    );
    for (state, get) in SERVER_JOB_STATES {
        sample(
            &mut out,
            "beanstalkd_current_jobs",
            &[("state", state)],
            &get(srv).to_string(),
        );
    }

    family(
        &mut out,
        "beanstalkd_commands_total",
        "Commands received, by command (stats cmd-*).",
        Kind::Counter,
    );
    for (cmd, get) in SERVER_COMMANDS {
        sample(
            &mut out,
            "beanstalkd_commands_total",
            &[("cmd", cmd)],
            &get(srv).to_string(),
        );
    }

    for (name, help, kind, get) in SERVER_SCALARS {
        family(&mut out, name, help, kind);
        sample(&mut out, name, &[], &get(srv).to_string());
    }

    family(
        &mut out,
        "beanstalkd_cpu_seconds_total",
        "CPU time consumed by the process in seconds (stats rusage-utime / rusage-stime).",
        Kind::Counter,
    );
    sample(
        &mut out,
        "beanstalkd_cpu_seconds_total",
        &[("mode", "user")],
        &rusage(srv.rusage_utime),
    );
    sample(
        &mut out,
        "beanstalkd_cpu_seconds_total",
        &[("mode", "system")],
        &rusage(srv.rusage_stime),
    );

    family(
        &mut out,
        "beanstalkd_build_info",
        "Always 1; the version label is the stats version.",
        Kind::Gauge,
    );
    sample(
        &mut out,
        "beanstalkd_build_info",
        &[("version", &srv.version)],
        "1",
    );

    for (name, help, kind, get) in RS_SCALARS {
        family(&mut out, name, help, kind);
        sample(&mut out, name, &[], &get(rs).to_string());
    }

    family(
        &mut out,
        "beanstalkd_tube_series_limit",
        "Maximum number of tubes that get per-tube series.",
        Kind::Gauge,
    );
    sample(
        &mut out,
        "beanstalkd_tube_series_limit",
        &[],
        &max_tube_series.to_string(),
    );
    family(
        &mut out,
        "beanstalkd_tube_series_truncated",
        "1 if some tubes have no per-tube series because of the limit, else 0.",
        Kind::Gauge,
    );
    sample(
        &mut out,
        "beanstalkd_tube_series_truncated",
        &[],
        if truncated { "1" } else { "0" },
    );

    // Per-tube families: family outer, tube inner, so each family's
    // samples stay contiguous under its HELP / TYPE lines.
    family(
        &mut out,
        "beanstalkd_tube_current_jobs",
        "Jobs in the tube by state (stats-tube current-jobs-*); urgent jobs are also counted as ready.",
        Kind::Gauge,
    );
    for t in shown {
        for (state, get) in TUBE_JOB_STATES {
            sample(
                &mut out,
                "beanstalkd_tube_current_jobs",
                &[("tube", t.name.as_str()), ("state", state)],
                &get(t).to_string(),
            );
        }
    }

    family(
        &mut out,
        "beanstalkd_tube_commands_total",
        "Commands affecting the tube, by command (stats-tube cmd-delete / cmd-pause-tube).",
        Kind::Counter,
    );
    for t in shown {
        for (cmd, get) in TUBE_COMMANDS {
            sample(
                &mut out,
                "beanstalkd_tube_commands_total",
                &[("tube", t.name.as_str()), ("cmd", cmd)],
                &get(t).to_string(),
            );
        }
    }

    for (name, help, kind, get) in TUBE_SCALARS {
        family(&mut out, name, help, kind);
        for t in shown {
            sample(
                &mut out,
                name,
                &[("tube", t.name.as_str())],
                &get(t).to_string(),
            );
        }
    }

    out
}

/// `# HELP` and `# TYPE` lines of one metric family. `help` must not
/// contain a backslash or newline (all help texts are constants).
fn family(out: &mut String, name: &str, help: &str, kind: Kind) {
    out.push_str("# HELP ");
    out.push_str(name);
    out.push(' ');
    out.push_str(help);
    out.push_str("\n# TYPE ");
    out.push_str(name);
    out.push(' ');
    out.push_str(kind.as_str());
    out.push('\n');
}

fn sample(out: &mut String, name: &str, labels: &[(&str, &str)], value: &str) {
    out.push_str(name);
    if !labels.is_empty() {
        out.push('{');
        for (i, (k, v)) in labels.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            out.push_str(k);
            out.push_str("=\"");
            push_label_value(out, v);
            out.push('"');
        }
        out.push('}');
    }
    out.push(' ');
    out.push_str(value);
    out.push('\n');
}

/// Label value escaping from the text format spec: backslash, double quote
/// and line feed.
fn push_label_value(out: &mut String, v: &str) {
    for c in v.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            c => out.push(c),
        }
    }
}

/// `(seconds, microseconds)` as a decimal number of seconds, printed the
/// way `stats` prints it (`%d.%06d`), so no float rounding is involved.
fn rusage((secs, micros): (u64, u64)) -> String {
    format!("{secs}.{micros:06}")
}

enum Json<'a> {
    Num(u64),
    /// Already a valid JSON number literal.
    Raw(String),
    Str(&'a str),
    Bool(bool),
}

fn server_fields(s: &StatsServer) -> [(&'static str, Json<'_>); 51] {
    use Json::{Bool, Num, Raw, Str};
    [
        ("current-jobs-urgent", Num(s.current_jobs_urgent)),
        ("current-jobs-ready", Num(s.current_jobs_ready)),
        ("current-jobs-reserved", Num(s.current_jobs_reserved)),
        ("current-jobs-delayed", Num(s.current_jobs_delayed)),
        ("current-jobs-buried", Num(s.current_jobs_buried)),
        ("cmd-put", Num(s.cmd_put)),
        ("cmd-peek", Num(s.cmd_peek)),
        ("cmd-peek-ready", Num(s.cmd_peek_ready)),
        ("cmd-peek-delayed", Num(s.cmd_peek_delayed)),
        ("cmd-peek-buried", Num(s.cmd_peek_buried)),
        ("cmd-reserve", Num(s.cmd_reserve)),
        ("cmd-reserve-with-timeout", Num(s.cmd_reserve_with_timeout)),
        ("cmd-delete", Num(s.cmd_delete)),
        ("cmd-release", Num(s.cmd_release)),
        ("cmd-use", Num(s.cmd_use)),
        ("cmd-watch", Num(s.cmd_watch)),
        ("cmd-ignore", Num(s.cmd_ignore)),
        ("cmd-bury", Num(s.cmd_bury)),
        ("cmd-kick", Num(s.cmd_kick)),
        ("cmd-touch", Num(s.cmd_touch)),
        ("cmd-stats", Num(s.cmd_stats)),
        ("cmd-stats-job", Num(s.cmd_stats_job)),
        ("cmd-stats-tube", Num(s.cmd_stats_tube)),
        ("cmd-list-tubes", Num(s.cmd_list_tubes)),
        ("cmd-list-tube-used", Num(s.cmd_list_tube_used)),
        ("cmd-list-tubes-watched", Num(s.cmd_list_tubes_watched)),
        ("cmd-pause-tube", Num(s.cmd_pause_tube)),
        ("job-timeouts", Num(s.job_timeouts)),
        ("total-jobs", Num(s.total_jobs)),
        ("max-job-size", Num(s.max_job_size)),
        ("current-tubes", Num(s.current_tubes)),
        ("current-connections", Num(s.current_connections)),
        ("current-producers", Num(s.current_producers)),
        ("current-workers", Num(s.current_workers)),
        ("current-waiting", Num(s.current_waiting)),
        ("total-connections", Num(s.total_connections)),
        ("pid", Num(s.pid)),
        ("version", Str(&s.version)),
        ("rusage-utime", Raw(rusage(s.rusage_utime))),
        ("rusage-stime", Raw(rusage(s.rusage_stime))),
        ("uptime", Num(s.uptime)),
        ("binlog-oldest-index", Num(s.binlog_oldest_index)),
        ("binlog-current-index", Num(s.binlog_current_index)),
        ("binlog-records-migrated", Num(s.binlog_records_migrated)),
        ("binlog-records-written", Num(s.binlog_records_written)),
        ("binlog-max-size", Num(s.binlog_max_size)),
        ("draining", Bool(s.draining)),
        ("id", Str(&s.id)),
        ("hostname", Str(&s.hostname)),
        ("os", Str(&s.os)),
        ("platform", Str(&s.platform)),
    ]
}

fn tube_fields(t: &StatsTube) -> [(&'static str, Json<'_>); 14] {
    use Json::{Num, Str};
    [
        ("name", Str(t.name.as_str())),
        ("current-jobs-urgent", Num(t.current_jobs_urgent)),
        ("current-jobs-ready", Num(t.current_jobs_ready)),
        ("current-jobs-reserved", Num(t.current_jobs_reserved)),
        ("current-jobs-delayed", Num(t.current_jobs_delayed)),
        ("current-jobs-buried", Num(t.current_jobs_buried)),
        ("total-jobs", Num(t.total_jobs)),
        ("current-using", Num(t.current_using)),
        ("current-watching", Num(t.current_watching)),
        ("current-waiting", Num(t.current_waiting)),
        ("cmd-delete", Num(t.cmd_delete)),
        ("cmd-pause-tube", Num(t.cmd_pause_tube)),
        ("pause", Num(t.pause)),
        ("pause-time-left", Num(t.pause_time_left)),
    ]
}

fn rs_fields(r: &ServerRsStats) -> [(&'static str, Json<'static>); 4] {
    use Json::Num;
    [
        ("pending-connections", Num(r.pending_connections)),
        ("pending-rejected", Num(r.pending_rejected)),
        ("auth-timeouts", Num(r.auth_timeouts)),
        ("auth-failures", Num(r.auth_failures)),
    ]
}

/// Renders `s` as the `/admin` JSON document (compact, keys in the
/// reference's `stats` / `stats-tube` order; see the module docs), with
/// at most `tube_limit` tubes and the server-side counters `rs`.
pub fn render_admin_json(s: &Snapshot, tube_limit: usize, rs: &ServerRsStats) -> String {
    let (shown, truncated) = shown_tubes(s, tube_limit);
    let mut out = String::with_capacity(2048 + shown.len().saturating_mul(512));
    out.push_str("{\"server\":");
    push_object(&mut out, &server_fields(&s.server));
    out.push_str(",\"server_rs\":");
    push_object(&mut out, &rs_fields(rs));
    out.push_str(",\"tube_limit\":");
    out.push_str(&tube_limit.to_string());
    out.push_str(",\"tubes_truncated\":");
    out.push_str(if truncated { "true" } else { "false" });
    out.push_str(",\"tubes\":[");
    for (i, t) in shown.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        push_object(&mut out, &tube_fields(t));
    }
    out.push_str("]}");
    out
}

fn push_object(out: &mut String, fields: &[(&str, Json<'_>)]) {
    out.push('{');
    for (i, (key, value)) in fields.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        push_json_str(out, key);
        out.push(':');
        match value {
            Json::Num(n) => out.push_str(&n.to_string()),
            Json::Raw(r) => out.push_str(r),
            Json::Str(v) => push_json_str(out, v),
            Json::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        }
    }
    out.push('}');
}

fn push_json_str(out: &mut String, v: &str) {
    out.push('"');
    for c in v.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if u32::from(c) < 0x20 => out.push_str(&format!("\\u{:04x}", u32::from(c))),
            c => out.push(c),
        }
    }
    out.push('"');
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet, HashMap};

    use bstk_engine::{Engine, EngineConfig, Snapshot, StaticSysInfo, SysSnapshot};
    use bstk_proto::{Command, StatsServer, StatsTube, TubeName};

    use super::{ServerRsStats, render_admin_json, render_prometheus};

    /// Every field distinct.
    fn rs() -> ServerRsStats {
        ServerRsStats {
            pending_connections: 901,
            pending_rejected: 902,
            auth_timeouts: 903,
            auth_failures: 904,
        }
    }

    // -----------------------------------------------------------------
    // Fixtures
    // -----------------------------------------------------------------

    /// Every numeric field distinct, so a mixed-up mapping cannot pass.
    fn server() -> StatsServer {
        StatsServer {
            current_jobs_urgent: 1,
            current_jobs_ready: 2,
            current_jobs_reserved: 3,
            current_jobs_delayed: 4,
            current_jobs_buried: 5,
            cmd_put: 6,
            cmd_peek: 7,
            cmd_peek_ready: 8,
            cmd_peek_delayed: 9,
            cmd_peek_buried: 10,
            cmd_reserve: 11,
            cmd_reserve_with_timeout: 12,
            cmd_delete: 13,
            cmd_release: 14,
            cmd_use: 15,
            cmd_watch: 16,
            cmd_ignore: 17,
            cmd_bury: 18,
            cmd_kick: 19,
            cmd_touch: 20,
            cmd_stats: 21,
            cmd_stats_job: 22,
            cmd_stats_tube: 23,
            cmd_list_tubes: 24,
            cmd_list_tube_used: 25,
            cmd_list_tubes_watched: 26,
            cmd_pause_tube: 27,
            job_timeouts: 28,
            total_jobs: 29,
            max_job_size: 65535,
            current_tubes: 3,
            current_connections: 31,
            current_producers: 32,
            current_workers: 33,
            current_waiting: 34,
            total_connections: 35,
            pid: 36,
            version: "1.13".into(),
            rusage_utime: (37, 38),
            rusage_stime: (39, 400_000),
            uptime: 41,
            binlog_oldest_index: 42,
            binlog_current_index: 43,
            binlog_records_migrated: 44,
            binlog_records_written: 45,
            binlog_max_size: 10_485_760,
            draining: true,
            id: "0123456789abcdef".into(),
            hostname: "host.example".into(),
            os: "#1 SMP".into(),
            platform: "x86_64".into(),
        }
    }

    fn tube_stats(name: &str, base: u64) -> StatsTube {
        StatsTube {
            name: TubeName::new(name).unwrap(),
            current_jobs_urgent: base + 1,
            current_jobs_ready: base + 2,
            current_jobs_reserved: base + 3,
            current_jobs_delayed: base + 4,
            current_jobs_buried: base + 5,
            total_jobs: base + 6,
            current_using: base + 7,
            current_watching: base + 8,
            current_waiting: base + 9,
            cmd_delete: base + 10,
            cmd_pause_tube: base + 11,
            pause: base + 12,
            pause_time_left: base + 13,
        }
    }

    fn snapshot() -> Snapshot {
        Snapshot {
            server: server(),
            tubes: vec![
                tube_stats("default", 100),
                tube_stats("emails", 200),
                tube_stats("a-b_c+d;e$f(g)/h.i", 300),
            ],
        }
    }

    /// A snapshot taken from a real engine with some activity.
    fn engine_snapshot() -> Snapshot {
        let sys = SysSnapshot {
            pid: 7,
            version: "0.1.0".into(),
            rusage_utime: (0, 123_456),
            rusage_stime: (2, 0),
            id: "feedface".into(),
            hostname: "h".into(),
            os: "o".into(),
            platform: "p".into(),
        };
        let mut e = Engine::new(0, EngineConfig::default(), Box::new(StaticSysInfo(sys)));
        let mut out = Vec::new();
        e.connect(0, 1);
        e.connect(0, 2);
        let tube = TubeName::new("jobs").unwrap();
        e.handle(0, 1, Command::Use(tube.clone()), &mut out);
        for (pri, delay) in [(0, 0), (5000, 0), (1, 60)] {
            let body = bytes::Bytes::from_static(b"x");
            e.handle(
                0,
                1,
                Command::Put {
                    pri,
                    delay,
                    ttr: 5,
                    body,
                },
                &mut out,
            );
        }
        e.handle(0, 2, Command::Watch(tube.clone()), &mut out);
        e.handle(0, 2, Command::Reserve, &mut out);
        e.handle(0, 2, Command::PauseTube { tube, delay: 30 }, &mut out);
        e.snapshot(3 * bstk_engine::NANOS_PER_SEC)
    }

    // -----------------------------------------------------------------
    // Strict parser for the text exposition format (0.0.4)
    // -----------------------------------------------------------------

    #[derive(Debug)]
    struct Family {
        kind: String,
        samples: Vec<(Vec<(String, String)>, String)>,
    }

    fn is_metric_name(s: &str) -> bool {
        let mut chars = s.chars();
        matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_' || c == ':')
            && chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == ':')
    }

    fn is_label_name(s: &str) -> bool {
        is_metric_name(s) && !s.contains(':') && !s.starts_with("__")
    }

    /// Parses `{k="v",...}` starting right after `{`; returns the labels
    /// and the rest of the line after `}`.
    fn parse_labels(mut rest: &str) -> (Vec<(String, String)>, &str) {
        let mut labels = Vec::new();
        loop {
            if let Some(r) = rest.strip_prefix('}') {
                return (labels, r);
            }
            if !labels.is_empty() {
                rest = rest.strip_prefix(',').expect("',' between labels");
            }
            let eq = rest.find('=').expect("label '='");
            let name = &rest[..eq];
            assert!(is_label_name(name), "bad label name {name:?}");
            rest = rest[eq + 1..].strip_prefix('"').expect("opening quote");
            let mut value = String::new();
            let mut chars = rest.char_indices();
            let end = loop {
                let (i, c) = chars.next().expect("unterminated label value");
                match c {
                    '"' => break i,
                    '\\' => match chars.next().expect("dangling escape").1 {
                        '\\' => value.push('\\'),
                        '"' => value.push('"'),
                        'n' => value.push('\n'),
                        other => panic!("invalid escape \\{other}"),
                    },
                    '\n' => panic!("raw newline in label value"),
                    c => value.push(c),
                }
            };
            rest = &rest[end + 1..];
            assert!(
                labels.iter().all(|(n, _)| n != name),
                "duplicate label {name}"
            );
            labels.push((name.to_string(), value));
        }
    }

    /// Parses and validates `text`; returns the families by name.
    fn parse(text: &str) -> BTreeMap<String, Family> {
        assert!(text.ends_with('\n') && !text.ends_with("\n\n"));
        let mut families: BTreeMap<String, Family> = BTreeMap::new();
        let mut current: Option<String> = None;
        let mut pending_help: Option<String> = None;
        let mut seen_series = BTreeSet::new();
        for line in text.lines() {
            assert!(!line.is_empty(), "blank line");
            if let Some(rest) = line.strip_prefix("# HELP ") {
                let (name, help) = rest.split_once(' ').expect("HELP text");
                assert!(is_metric_name(name));
                assert!(!help.is_empty() && !help.contains('\\'));
                assert!(pending_help.is_none(), "HELP without TYPE");
                assert!(!families.contains_key(name), "family {name} repeated");
                pending_help = Some(name.to_string());
            } else if let Some(rest) = line.strip_prefix("# TYPE ") {
                let (name, kind) = rest.split_once(' ').expect("TYPE kind");
                assert_eq!(
                    pending_help.take().as_deref(),
                    Some(name),
                    "TYPE must follow its HELP"
                );
                assert!(matches!(kind, "counter" | "gauge"), "kind {kind}");
                if kind == "counter" {
                    assert!(name.ends_with("_total"), "counter {name} without _total");
                } else {
                    assert!(!name.ends_with("_total"), "gauge {name} ends in _total");
                }
                families.insert(
                    name.to_string(),
                    Family {
                        kind: kind.to_string(),
                        samples: Vec::new(),
                    },
                );
                current = Some(name.to_string());
            } else {
                assert!(!line.starts_with('#'), "unexpected comment {line:?}");
                assert!(pending_help.is_none(), "HELP without TYPE");
                let name_end = line.find(['{', ' ']).expect("sample value");
                let name = &line[..name_end];
                assert_eq!(Some(name), current.as_deref(), "sample outside its family");
                let (labels, rest) = match line[name_end..].strip_prefix('{') {
                    Some(r) => parse_labels(r),
                    None => (Vec::new(), &line[name_end..]),
                };
                let value = rest.strip_prefix(' ').expect("space before value");
                assert!(!value.contains(' '), "timestamps are not emitted");
                let v: f64 = value.parse().expect("numeric value");
                assert!(v.is_finite() && v >= 0.0);
                assert!(
                    seen_series.insert((name.to_string(), labels.clone())),
                    "duplicate series {line}"
                );
                families
                    .get_mut(name)
                    .unwrap()
                    .samples
                    .push((labels, value.to_string()));
            }
        }
        assert!(pending_help.is_none());
        families
    }

    /// `key: value` pairs of a stats YAML document, quotes stripped.
    fn yaml_map(yaml: &[u8]) -> Vec<(String, String)> {
        std::str::from_utf8(yaml)
            .unwrap()
            .lines()
            .skip(1)
            .map(|l| {
                let (k, v) = l.split_once(": ").unwrap();
                (k.to_string(), v.trim_matches('"').to_string())
            })
            .collect()
    }

    fn label<'a>(labels: &'a [(String, String)], name: &str) -> Option<&'a str> {
        labels
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_str())
    }

    /// The stats key a sample was rendered from (independent of the
    /// renderer's own tables), or None for the non-stats series.
    fn server_key(name: &str, labels: &[(String, String)]) -> Option<String> {
        let key = match name {
            "beanstalkd_current_jobs" => format!("current-jobs-{}", label(labels, "state")?),
            "beanstalkd_commands_total" => format!("cmd-{}", label(labels, "cmd")?),
            "beanstalkd_cpu_seconds_total" => match label(labels, "mode")? {
                "user" => "rusage-utime".into(),
                "system" => "rusage-stime".into(),
                other => panic!("mode {other}"),
            },
            "beanstalkd_build_info" => "version".into(),
            "beanstalkd_job_timeouts_total" => "job-timeouts".into(),
            "beanstalkd_jobs_total" => "total-jobs".into(),
            "beanstalkd_max_job_size_bytes" => "max-job-size".into(),
            "beanstalkd_current_tubes" => "current-tubes".into(),
            "beanstalkd_current_connections" => "current-connections".into(),
            "beanstalkd_current_producers" => "current-producers".into(),
            "beanstalkd_current_workers" => "current-workers".into(),
            "beanstalkd_current_waiting" => "current-waiting".into(),
            "beanstalkd_connections_total" => "total-connections".into(),
            "beanstalkd_uptime_seconds" => "uptime".into(),
            "beanstalkd_binlog_oldest_index" => "binlog-oldest-index".into(),
            "beanstalkd_binlog_current_index" => "binlog-current-index".into(),
            "beanstalkd_binlog_records_migrated_total" => "binlog-records-migrated".into(),
            "beanstalkd_binlog_records_written_total" => "binlog-records-written".into(),
            "beanstalkd_binlog_max_size_bytes" => "binlog-max-size".into(),
            "beanstalkd_draining" => "draining".into(),
            _ => return None,
        };
        Some(key)
    }

    fn tube_key(name: &str, labels: &[(String, String)]) -> String {
        match name {
            "beanstalkd_tube_current_jobs" => {
                format!("current-jobs-{}", label(labels, "state").unwrap())
            }
            "beanstalkd_tube_commands_total" => format!("cmd-{}", label(labels, "cmd").unwrap()),
            "beanstalkd_tube_jobs_total" => "total-jobs".into(),
            "beanstalkd_tube_current_using" => "current-using".into(),
            "beanstalkd_tube_current_watching" => "current-watching".into(),
            "beanstalkd_tube_current_waiting" => "current-waiting".into(),
            "beanstalkd_tube_pause_seconds" => "pause".into(),
            "beanstalkd_tube_pause_time_left_seconds" => "pause-time-left".into(),
            other => panic!("unmapped metric {other}"),
        }
    }

    /// Checks every sample against the `stats` / `stats-tube` YAML of the
    /// snapshot, and that every numeric stats key is covered.
    fn check_values(s: &Snapshot, limit: usize) {
        let text = render_prometheus(s, limit, &rs());
        let families = parse(&text);
        let server_yaml: HashMap<String, String> =
            yaml_map(&s.server.to_yaml()).into_iter().collect();
        let tube_yaml: HashMap<String, HashMap<String, String>> = s
            .tubes
            .iter()
            .map(|t| {
                (
                    t.name.as_str().to_string(),
                    yaml_map(&t.to_yaml()).into_iter().collect(),
                )
            })
            .collect();

        let mut server_covered = BTreeSet::new();
        let mut tube_covered: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for (name, fam) in &families {
            for (labels, value) in &fam.samples {
                if let Some(tube) = label(labels, "tube") {
                    assert!(name.starts_with("beanstalkd_tube_"));
                    let key = tube_key(name, labels);
                    assert_eq!(&tube_yaml[tube][&key], value, "{name} {labels:?}");
                    assert!(
                        tube_covered
                            .entry(tube.to_string())
                            .or_default()
                            .insert(key)
                    );
                } else if let Some(key) = server_key(name, labels) {
                    let expected = match key.as_str() {
                        "version" => {
                            assert_eq!(label(labels, "version"), Some(s.server.version.as_str()));
                            "1".to_string()
                        }
                        "draining" => if s.server.draining { "1" } else { "0" }.to_string(),
                        k => server_yaml[k].clone(),
                    };
                    assert_eq!(value, &expected, "{name} {labels:?}");
                    assert!(server_covered.insert(key));
                } else if let Some(v) = rs_value(name) {
                    assert!(labels.is_empty(), "{name} {labels:?}");
                    assert_eq!(value, &v.to_string(), "{name}");
                    assert!(server_covered.insert(name.clone()));
                } else {
                    assert!(
                        name == "beanstalkd_tube_series_limit"
                            || name == "beanstalkd_tube_series_truncated",
                        "unexpected metric {name}"
                    );
                }
            }
        }
        let not_exported = ["pid", "id", "hostname", "os", "platform"];
        let mut expected_server: BTreeSet<String> = server_yaml
            .keys()
            .filter(|k| !not_exported.contains(&k.as_str()))
            .cloned()
            .collect();
        for name in [
            "beanstalkd_pending_connections",
            "beanstalkd_pending_rejected_total",
            "beanstalkd_auth_timeouts_total",
            "beanstalkd_auth_failures_total",
        ] {
            expected_server.insert(name.to_owned());
        }
        assert_eq!(server_covered, expected_server);

        let shown: Vec<&str> = s
            .tubes
            .iter()
            .take(limit)
            .map(|t| t.name.as_str())
            .collect();
        assert_eq!(
            tube_covered
                .keys()
                .map(String::as_str)
                .collect::<BTreeSet<_>>(),
            shown.iter().copied().collect::<BTreeSet<_>>()
        );
        for keys in tube_covered.values() {
            assert_eq!(keys.len(), 13, "every stats-tube key except name");
        }
        let truncated = if shown.len() < s.tubes.len() {
            "1"
        } else {
            "0"
        };
        assert_eq!(
            families["beanstalkd_tube_series_truncated"].samples[0].1,
            truncated
        );
        assert_eq!(
            families["beanstalkd_tube_series_limit"].samples[0].1,
            limit.to_string()
        );
        // Per-tube samples follow list-tubes order within each family.
        let order: Vec<&str> = families["beanstalkd_tube_jobs_total"]
            .samples
            .iter()
            .map(|(l, _)| label(l, "tube").unwrap())
            .collect();
        assert_eq!(order, shown);
    }

    /// The value of a server-side metric in `rs()`.
    fn rs_value(name: &str) -> Option<u64> {
        let r = rs();
        match name {
            "beanstalkd_pending_connections" => Some(r.pending_connections),
            "beanstalkd_pending_rejected_total" => Some(r.pending_rejected),
            "beanstalkd_auth_timeouts_total" => Some(r.auth_timeouts),
            "beanstalkd_auth_failures_total" => Some(r.auth_failures),
            _ => None,
        }
    }

    // -----------------------------------------------------------------
    // Prometheus tests
    // -----------------------------------------------------------------

    #[test]
    fn prometheus_values_equal_snapshot_fields() {
        check_values(&snapshot(), 100);
        check_values(&engine_snapshot(), 100);
        let mut quiet = snapshot();
        quiet.server.draining = false;
        check_values(&quiet, 3);
    }

    #[test]
    fn prometheus_types() {
        let fams = parse(&render_prometheus(&snapshot(), 10, &rs()));
        let counters: Vec<&str> = fams
            .iter()
            .filter(|(_, f)| f.kind == "counter")
            .map(|(n, _)| n.as_str())
            .collect();
        assert_eq!(
            counters,
            [
                "beanstalkd_auth_failures_total",
                "beanstalkd_auth_timeouts_total",
                "beanstalkd_binlog_records_migrated_total",
                "beanstalkd_binlog_records_written_total",
                "beanstalkd_commands_total",
                "beanstalkd_connections_total",
                "beanstalkd_cpu_seconds_total",
                "beanstalkd_job_timeouts_total",
                "beanstalkd_jobs_total",
                "beanstalkd_pending_rejected_total",
                "beanstalkd_tube_commands_total",
                "beanstalkd_tube_jobs_total",
            ]
        );
        // No gauge shares a base name with a counter (OpenMetrics).
        for c in &counters {
            assert!(!fams.contains_key(c.trim_end_matches("_total")));
        }
        assert_eq!(fams["beanstalkd_commands_total"].samples.len(), 22);
    }

    #[test]
    fn prometheus_golden_lines() {
        let text = render_prometheus(&snapshot(), 2, &rs());
        for line in [
            "# HELP beanstalkd_current_jobs Jobs by state (stats current-jobs-*); urgent jobs are also counted as ready.",
            "# TYPE beanstalkd_current_jobs gauge",
            "beanstalkd_current_jobs{state=\"urgent\"} 1",
            "beanstalkd_current_jobs{state=\"buried\"} 5",
            "# TYPE beanstalkd_commands_total counter",
            "beanstalkd_commands_total{cmd=\"put\"} 6",
            "beanstalkd_commands_total{cmd=\"reserve-with-timeout\"} 12",
            "beanstalkd_commands_total{cmd=\"pause-tube\"} 27",
            "beanstalkd_job_timeouts_total 28",
            "beanstalkd_jobs_total 29",
            "beanstalkd_max_job_size_bytes 65535",
            "beanstalkd_current_tubes 3",
            "beanstalkd_connections_total 35",
            "beanstalkd_uptime_seconds 41",
            "beanstalkd_binlog_max_size_bytes 10485760",
            "beanstalkd_draining 1",
            "beanstalkd_cpu_seconds_total{mode=\"user\"} 37.000038",
            "beanstalkd_cpu_seconds_total{mode=\"system\"} 39.400000",
            "beanstalkd_build_info{version=\"1.13\"} 1",
            "# TYPE beanstalkd_pending_connections gauge",
            "beanstalkd_pending_connections 901",
            "# TYPE beanstalkd_pending_rejected_total counter",
            "beanstalkd_pending_rejected_total 902",
            "beanstalkd_auth_timeouts_total 903",
            "beanstalkd_auth_failures_total 904",
            "beanstalkd_tube_series_limit 2",
            "beanstalkd_tube_series_truncated 1",
            "beanstalkd_tube_current_jobs{tube=\"default\",state=\"ready\"} 102",
            "beanstalkd_tube_commands_total{tube=\"emails\",cmd=\"pause-tube\"} 211",
            "beanstalkd_tube_jobs_total{tube=\"emails\"} 206",
            "beanstalkd_tube_pause_time_left_seconds{tube=\"default\"} 113",
        ] {
            assert!(
                text.lines().any(|l| l == line),
                "missing line {line:?} in:\n{text}"
            );
        }
        assert!(!text.contains("a-b_c+d"));
        // 22 server families (incl. the two cap gauges) + 4 server-side
        // ones + 8 tube families, two header lines each; 5 + 22 + 16 + 2 +
        // 1 + 4 + 2 server samples; 13 samples per shown tube.
        assert_eq!(text.lines().count(), 2 * (22 + 4 + 8) + 52 + 2 * 13);
    }

    #[test]
    fn prometheus_truncation() {
        let s = snapshot();
        for limit in [0, 1, 2, 3, 4, usize::MAX] {
            check_values(&s, limit);
        }
        let text = render_prometheus(&s, 0, &rs());
        assert!(!text.contains("tube=\""));
        assert!(text.contains("\nbeanstalkd_tube_series_truncated 1\n"));
        let text = render_prometheus(&s, 3, &rs());
        assert!(text.contains("\nbeanstalkd_tube_series_truncated 0\n"));
        assert!(text.contains("tube=\"a-b_c+d;e$f(g)/h.i\""));

        let empty = Snapshot {
            server: server(),
            tubes: Vec::new(),
        };
        let text = render_prometheus(&empty, 0, &rs());
        parse(&text);
        assert!(text.contains("\nbeanstalkd_tube_series_truncated 0\n"));
    }

    #[test]
    fn prometheus_label_values_are_escaped() {
        let mut s = snapshot();
        s.server.version = "a\\b\"c\nd".into();
        let text = render_prometheus(&s, 10, &rs());
        assert!(
            text.contains("beanstalkd_build_info{version=\"a\\\\b\\\"c\\nd\"} 1\n"),
            "{text}"
        );
        let fams = parse(&text);
        let (labels, _) = &fams["beanstalkd_build_info"].samples[0];
        assert_eq!(label(labels, "version"), Some("a\\b\"c\nd"));
    }

    // -----------------------------------------------------------------
    // Admin JSON tests
    // -----------------------------------------------------------------

    fn json_matches_yaml(obj: &serde_json::Value, yaml: &[u8], string_keys: &[&str]) {
        let obj = obj.as_object().unwrap();
        let yaml = yaml_map(yaml);
        // Same keys, same order (serde_json's default Map is sorted, so
        // check order on the raw text instead).
        assert_eq!(
            obj.keys().collect::<BTreeSet<_>>(),
            yaml.iter().map(|(k, _)| k).collect::<BTreeSet<_>>()
        );
        for (k, v) in &yaml {
            let got = &obj[k];
            if string_keys.contains(&k.as_str()) {
                assert_eq!(got.as_str(), Some(v.as_str()), "{k}");
            } else if k == "draining" {
                assert_eq!(got.as_bool(), Some(v == "true"), "{k}");
            } else if k.starts_with("rusage-") {
                assert_eq!(got.as_f64(), Some(v.parse::<f64>().unwrap()), "{k}");
            } else {
                assert_eq!(got.as_u64(), Some(v.parse::<u64>().unwrap()), "{k}");
            }
        }
    }

    /// Keys of the top-level objects in textual order.
    fn key_order(text: &str) -> Vec<String> {
        text.split(['{', ','])
            .filter_map(|p| p.strip_prefix('"'))
            .filter_map(|p| p.split_once("\":").map(|(k, _)| k.to_string()))
            .collect()
    }

    fn check_json(s: &Snapshot) {
        let text = render_admin_json(s, usize::MAX, &rs());
        let v: serde_json::Value = serde_json::from_str(&text).unwrap();
        let top: BTreeSet<&str> = v.as_object().unwrap().keys().map(String::as_str).collect();
        assert_eq!(
            top,
            BTreeSet::from([
                "server",
                "server_rs",
                "tube_limit",
                "tubes_truncated",
                "tubes"
            ])
        );
        assert_eq!(v["tubes_truncated"], false);
        assert_eq!(v["server_rs"]["pending-connections"], 901);
        assert_eq!(v["server_rs"]["pending-rejected"], 902);
        assert_eq!(v["server_rs"]["auth-timeouts"], 903);
        assert_eq!(v["server_rs"]["auth-failures"], 904);
        json_matches_yaml(
            &v["server"],
            &s.server.to_yaml(),
            &["version", "id", "hostname", "os", "platform"],
        );
        let tubes = v["tubes"].as_array().unwrap();
        assert_eq!(tubes.len(), s.tubes.len());
        for (j, t) in tubes.iter().zip(&s.tubes) {
            json_matches_yaml(j, &t.to_yaml(), &["name"]);
        }
        // Textual key order equals the YAML order.
        let mut expected = vec!["server".to_string()];
        expected.extend(yaml_map(&s.server.to_yaml()).into_iter().map(|(k, _)| k));
        for k in [
            "server_rs",
            "pending-connections",
            "pending-rejected",
            "auth-timeouts",
            "auth-failures",
            "tube_limit",
            "tubes_truncated",
            "tubes",
        ] {
            expected.push(k.into());
        }
        for t in &s.tubes {
            expected.extend(yaml_map(&t.to_yaml()).into_iter().map(|(k, _)| k));
        }
        assert_eq!(key_order(&text), expected);
    }

    #[test]
    fn admin_json_round_trips_values_in_stats_order() {
        check_json(&snapshot());
        check_json(&engine_snapshot());
        check_json(&Snapshot {
            server: StatsServer::default(),
            tubes: Vec::new(),
        });
    }

    #[test]
    fn admin_json_golden() {
        let s = Snapshot {
            server: StatsServer {
                version: "1.13".into(),
                id: "abc".into(),
                rusage_utime: (1, 5),
                rusage_stime: (0, 250_000),
                ..StatsServer::default()
            },
            tubes: vec![tube_stats("default", 0)],
        };
        let expected = concat!(
            r#"{"server":{"current-jobs-urgent":0,"current-jobs-ready":0,"current-jobs-reserved":0,"#,
            r#""current-jobs-delayed":0,"current-jobs-buried":0,"cmd-put":0,"cmd-peek":0,"#,
            r#""cmd-peek-ready":0,"cmd-peek-delayed":0,"cmd-peek-buried":0,"cmd-reserve":0,"#,
            r#""cmd-reserve-with-timeout":0,"cmd-delete":0,"cmd-release":0,"cmd-use":0,"#,
            r#""cmd-watch":0,"cmd-ignore":0,"cmd-bury":0,"cmd-kick":0,"cmd-touch":0,"#,
            r#""cmd-stats":0,"cmd-stats-job":0,"cmd-stats-tube":0,"cmd-list-tubes":0,"#,
            r#""cmd-list-tube-used":0,"cmd-list-tubes-watched":0,"cmd-pause-tube":0,"#,
            r#""job-timeouts":0,"total-jobs":0,"max-job-size":0,"current-tubes":0,"#,
            r#""current-connections":0,"current-producers":0,"current-workers":0,"#,
            r#""current-waiting":0,"total-connections":0,"pid":0,"version":"1.13","#,
            r#""rusage-utime":1.000005,"rusage-stime":0.250000,"uptime":0,"#,
            r#""binlog-oldest-index":0,"binlog-current-index":0,"binlog-records-migrated":0,"#,
            r#""binlog-records-written":0,"binlog-max-size":0,"draining":false,"id":"abc","#,
            r#""hostname":"","os":"","platform":""},"#,
            r#""server_rs":{"pending-connections":0,"pending-rejected":0,"auth-timeouts":0,"#,
            r#""auth-failures":0},"tube_limit":1000,"tubes_truncated":false,"#,
            r#""tubes":[{"name":"default","current-jobs-urgent":1,"current-jobs-ready":2,"#,
            r#""current-jobs-reserved":3,"current-jobs-delayed":4,"current-jobs-buried":5,"#,
            r#""total-jobs":6,"current-using":7,"current-watching":8,"current-waiting":9,"#,
            r#""cmd-delete":10,"cmd-pause-tube":11,"pause":12,"pause-time-left":13}]}"#,
        );
        assert_eq!(
            render_admin_json(&s, 1000, &ServerRsStats::default()),
            expected
        );
    }

    #[test]
    fn admin_json_escapes_strings() {
        let mut s = snapshot();
        s.server.version = "q\"b\\n\nt\tc\u{1}é".into();
        s.server.hostname = "</script>".into();
        let text = render_admin_json(&s, 10, &rs());
        let v: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(v["server"]["version"], s.server.version.as_str());
        assert_eq!(v["server"]["hostname"], "</script>");
        assert!(text.contains(r#""version":"q\"b\\n\nt\tc\u0001é""#));
    }

    #[test]
    fn admin_json_caps_tubes() {
        let s = snapshot();
        for (limit, shown, truncated) in [
            (0, 0, true),
            (1, 1, true),
            (2, 2, true),
            (3, 3, false),
            (4, 3, false),
            (usize::MAX, 3, false),
        ] {
            let text = render_admin_json(&s, limit, &rs());
            let v: serde_json::Value = serde_json::from_str(&text).unwrap();
            let tubes = v["tubes"].as_array().unwrap();
            assert_eq!(tubes.len(), shown, "limit {limit}");
            assert_eq!(v["tubes_truncated"], truncated, "limit {limit}");
            assert_eq!(v["tube_limit"].as_u64(), u64::try_from(limit).ok());
            // The first ones, in list-tubes order.
            for (j, t) in tubes.iter().zip(&s.tubes) {
                assert_eq!(j["name"], t.name.as_str());
            }
            // The server object is unaffected by the cap.
            assert_eq!(v["server"]["current-tubes"], 3);
        }
    }
}
