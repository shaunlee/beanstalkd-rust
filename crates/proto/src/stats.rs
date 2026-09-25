//! YAML payloads for stats / stats-job / stats-tube / list-* (see STATS_FMT,
//! STATS_TUBE_FMT, JOB_STATS_FMT and fmt_* in prot.c). Output must be
//! byte-identical to the reference.

use bytes::Bytes;

use crate::{JobId, TubeName};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatsJob {
    pub id: JobId,
    pub tube: TubeName,
    /// "ready" | "delayed" | "reserved" | "buried"
    pub state: &'static str,
    pub pri: u32,
    pub age: u64,
    pub delay: u64,
    pub ttr: u64,
    pub time_left: u64,
    pub file: u64,
    pub reserves: u64,
    pub timeouts: u64,
    pub releases: u64,
    pub buries: u64,
    pub kicks: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatsTube {
    pub name: TubeName,
    pub current_jobs_urgent: u64,
    pub current_jobs_ready: u64,
    pub current_jobs_reserved: u64,
    pub current_jobs_delayed: u64,
    pub current_jobs_buried: u64,
    pub total_jobs: u64,
    pub current_using: u64,
    pub current_watching: u64,
    pub current_waiting: u64,
    pub cmd_delete: u64,
    pub cmd_pause_tube: u64,
    pub pause: u64,
    pub pause_time_left: u64,
}

/// Server-wide stats. Field set and order follow STATS_FMT exactly; T1 may add
/// fields if the reference has keys missing here (report it).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StatsServer {
    pub current_jobs_urgent: u64,
    pub current_jobs_ready: u64,
    pub current_jobs_reserved: u64,
    pub current_jobs_delayed: u64,
    pub current_jobs_buried: u64,
    pub cmd_put: u64,
    pub cmd_peek: u64,
    pub cmd_peek_ready: u64,
    pub cmd_peek_delayed: u64,
    pub cmd_peek_buried: u64,
    pub cmd_reserve: u64,
    pub cmd_reserve_with_timeout: u64,
    pub cmd_delete: u64,
    pub cmd_release: u64,
    pub cmd_use: u64,
    pub cmd_watch: u64,
    pub cmd_ignore: u64,
    pub cmd_bury: u64,
    pub cmd_kick: u64,
    pub cmd_touch: u64,
    pub cmd_stats: u64,
    pub cmd_stats_job: u64,
    pub cmd_stats_tube: u64,
    pub cmd_list_tubes: u64,
    pub cmd_list_tube_used: u64,
    pub cmd_list_tubes_watched: u64,
    pub cmd_pause_tube: u64,
    pub job_timeouts: u64,
    pub total_jobs: u64,
    pub max_job_size: u64,
    pub current_tubes: u64,
    pub current_connections: u64,
    pub current_producers: u64,
    pub current_workers: u64,
    pub current_waiting: u64,
    pub total_connections: u64,
    pub pid: u64,
    pub version: String,
    /// (seconds, microseconds)
    pub rusage_utime: (u64, u64),
    pub rusage_stime: (u64, u64),
    pub uptime: u64,
    pub binlog_oldest_index: u64,
    pub binlog_current_index: u64,
    pub binlog_records_migrated: u64,
    pub binlog_records_written: u64,
    pub binlog_max_size: u64,
    pub draining: bool,
    pub id: String,
    pub hostname: String,
    pub os: String,
    pub platform: String,
}

impl StatsJob {
    /// `STATS_JOB_FMT`. The returned bytes end with a single `\n` (no
    /// trailing `\r\n`): the caller (`Response::Ok`) appends the generic
    /// `\r\n` framing, matching how `do_stats`/`fmt_job_stats` build the
    /// on-wire payload in prot.c.
    pub fn to_yaml(&self) -> Bytes {
        let s = format!(
            "---\n\
             id: {}\n\
             tube: \"{}\"\n\
             state: {}\n\
             pri: {}\n\
             age: {}\n\
             delay: {}\n\
             ttr: {}\n\
             time-left: {}\n\
             file: {}\n\
             reserves: {}\n\
             timeouts: {}\n\
             releases: {}\n\
             buries: {}\n\
             kicks: {}\n",
            self.id,
            self.tube,
            self.state,
            self.pri,
            self.age,
            self.delay,
            self.ttr,
            self.time_left,
            self.file,
            self.reserves,
            self.timeouts,
            self.releases,
            self.buries,
            self.kicks,
        );
        Bytes::from(s.into_bytes())
    }
}

impl StatsTube {
    /// `STATS_TUBE_FMT` (see the note on `StatsJob::to_yaml` about the
    /// trailing `\n` vs `\r\n`).
    pub fn to_yaml(&self) -> Bytes {
        let s = format!(
            "---\n\
             name: \"{}\"\n\
             current-jobs-urgent: {}\n\
             current-jobs-ready: {}\n\
             current-jobs-reserved: {}\n\
             current-jobs-delayed: {}\n\
             current-jobs-buried: {}\n\
             total-jobs: {}\n\
             current-using: {}\n\
             current-watching: {}\n\
             current-waiting: {}\n\
             cmd-delete: {}\n\
             cmd-pause-tube: {}\n\
             pause: {}\n\
             pause-time-left: {}\n",
            self.name,
            self.current_jobs_urgent,
            self.current_jobs_ready,
            self.current_jobs_reserved,
            self.current_jobs_delayed,
            self.current_jobs_buried,
            self.total_jobs,
            self.current_using,
            self.current_watching,
            self.current_waiting,
            self.cmd_delete,
            self.cmd_pause_tube,
            self.pause,
            self.pause_time_left,
        );
        Bytes::from(s.into_bytes())
    }
}

impl StatsServer {
    /// `STATS_FMT` (see the note on `StatsJob::to_yaml` about the trailing
    /// `\n` vs `\r\n`).
    pub fn to_yaml(&self) -> Bytes {
        let s = format!(
            "---\n\
             current-jobs-urgent: {}\n\
             current-jobs-ready: {}\n\
             current-jobs-reserved: {}\n\
             current-jobs-delayed: {}\n\
             current-jobs-buried: {}\n\
             cmd-put: {}\n\
             cmd-peek: {}\n\
             cmd-peek-ready: {}\n\
             cmd-peek-delayed: {}\n\
             cmd-peek-buried: {}\n\
             cmd-reserve: {}\n\
             cmd-reserve-with-timeout: {}\n\
             cmd-delete: {}\n\
             cmd-release: {}\n\
             cmd-use: {}\n\
             cmd-watch: {}\n\
             cmd-ignore: {}\n\
             cmd-bury: {}\n\
             cmd-kick: {}\n\
             cmd-touch: {}\n\
             cmd-stats: {}\n\
             cmd-stats-job: {}\n\
             cmd-stats-tube: {}\n\
             cmd-list-tubes: {}\n\
             cmd-list-tube-used: {}\n\
             cmd-list-tubes-watched: {}\n\
             cmd-pause-tube: {}\n\
             job-timeouts: {}\n\
             total-jobs: {}\n\
             max-job-size: {}\n\
             current-tubes: {}\n\
             current-connections: {}\n\
             current-producers: {}\n\
             current-workers: {}\n\
             current-waiting: {}\n\
             total-connections: {}\n\
             pid: {}\n\
             version: \"{}\"\n\
             rusage-utime: {}.{:06}\n\
             rusage-stime: {}.{:06}\n\
             uptime: {}\n\
             binlog-oldest-index: {}\n\
             binlog-current-index: {}\n\
             binlog-records-migrated: {}\n\
             binlog-records-written: {}\n\
             binlog-max-size: {}\n\
             draining: {}\n\
             id: {}\n\
             hostname: \"{}\"\n\
             os: \"{}\"\n\
             platform: \"{}\"\n",
            self.current_jobs_urgent,
            self.current_jobs_ready,
            self.current_jobs_reserved,
            self.current_jobs_delayed,
            self.current_jobs_buried,
            self.cmd_put,
            self.cmd_peek,
            self.cmd_peek_ready,
            self.cmd_peek_delayed,
            self.cmd_peek_buried,
            self.cmd_reserve,
            self.cmd_reserve_with_timeout,
            self.cmd_delete,
            self.cmd_release,
            self.cmd_use,
            self.cmd_watch,
            self.cmd_ignore,
            self.cmd_bury,
            self.cmd_kick,
            self.cmd_touch,
            self.cmd_stats,
            self.cmd_stats_job,
            self.cmd_stats_tube,
            self.cmd_list_tubes,
            self.cmd_list_tube_used,
            self.cmd_list_tubes_watched,
            self.cmd_pause_tube,
            self.job_timeouts,
            self.total_jobs,
            self.max_job_size,
            self.current_tubes,
            self.current_connections,
            self.current_producers,
            self.current_workers,
            self.current_waiting,
            self.total_connections,
            self.pid,
            self.version,
            self.rusage_utime.0,
            self.rusage_utime.1,
            self.rusage_stime.0,
            self.rusage_stime.1,
            self.uptime,
            self.binlog_oldest_index,
            self.binlog_current_index,
            self.binlog_records_migrated,
            self.binlog_records_written,
            self.binlog_max_size,
            if self.draining { "true" } else { "false" },
            self.id,
            self.hostname,
            self.os,
            self.platform,
        );
        Bytes::from(s.into_bytes())
    }
}

/// YAML list used by list-tubes / list-tubes-watched (`do_list_tubes`; see
/// the note on `StatsJob::to_yaml` about the trailing `\n` vs `\r\n`).
pub fn yaml_list<'a>(names: impl IntoIterator<Item = &'a TubeName>) -> Bytes {
    let mut s = String::from("---\n");
    for name in names {
        s.push_str("- ");
        s.push_str(name.as_str());
        s.push('\n');
    }
    Bytes::from(s.into_bytes())
}
