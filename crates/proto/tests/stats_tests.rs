//! Golden-string tests for the stats/list YAML payloads, checked against
//! `STATS_FMT` / `STATS_TUBE_FMT` / `STATS_JOB_FMT` / `do_list_tubes` in
//! prot.c.

#![allow(clippy::unwrap_used)]

use bstk_proto::{StatsJob, StatsServer, StatsTube, TubeName, yaml_list};

fn tube(s: &str) -> TubeName {
    TubeName::new(s).expect("valid tube name in test fixture")
}

#[test]
fn stats_job_yaml_golden() {
    let job = StatsJob {
        id: 1,
        tube: tube("default"),
        state: "ready",
        pri: 0,
        age: 143,
        delay: 0,
        ttr: 100,
        time_left: 0,
        file: 0,
        reserves: 0,
        timeouts: 0,
        releases: 0,
        buries: 0,
        kicks: 0,
    };
    let expected = "---\n\
id: 1\n\
tube: \"default\"\n\
state: ready\n\
pri: 0\n\
age: 143\n\
delay: 0\n\
ttr: 100\n\
time-left: 0\n\
file: 0\n\
reserves: 0\n\
timeouts: 0\n\
releases: 0\n\
buries: 0\n\
kicks: 0\n";
    assert_eq!(job.to_yaml(), expected.as_bytes());
}

#[test]
fn stats_tube_yaml_golden() {
    let t = StatsTube {
        name: tube("foo"),
        current_jobs_urgent: 0,
        current_jobs_ready: 1,
        current_jobs_reserved: 0,
        current_jobs_delayed: 2,
        current_jobs_buried: 0,
        total_jobs: 3,
        current_using: 1,
        current_watching: 1,
        current_waiting: 0,
        cmd_delete: 0,
        cmd_pause_tube: 0,
        pause: 0,
        pause_time_left: 0,
    };
    let expected = "---\n\
name: \"foo\"\n\
current-jobs-urgent: 0\n\
current-jobs-ready: 1\n\
current-jobs-reserved: 0\n\
current-jobs-delayed: 2\n\
current-jobs-buried: 0\n\
total-jobs: 3\n\
current-using: 1\n\
current-watching: 1\n\
current-waiting: 0\n\
cmd-delete: 0\n\
cmd-pause-tube: 0\n\
pause: 0\n\
pause-time-left: 0\n";
    assert_eq!(t.to_yaml(), expected.as_bytes());
}

#[test]
fn stats_server_yaml_golden() {
    let s = StatsServer {
        current_jobs_ready: 1,
        cmd_put: 2,
        max_job_size: 65535,
        pid: 123,
        version: "1.13".to_string(),
        rusage_utime: (1, 2),
        rusage_stime: (0, 500000),
        uptime: 10,
        draining: false,
        id: "abcdef0123456789".to_string(),
        hostname: "myhost".to_string(),
        os: "Linux".to_string(),
        platform: "x86_64".to_string(),
        ..Default::default()
    };

    let yaml = s.to_yaml();
    let text = String::from_utf8(yaml.to_vec()).expect("valid utf8");

    // Spot-check ordering and a handful of representative lines rather
    // than the entire (long) blob, to keep the test readable while still
    // pinning down field order and exact formatting.
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines[0], "---");
    assert_eq!(lines[1], "current-jobs-urgent: 0");
    assert_eq!(lines[2], "current-jobs-ready: 1");
    assert!(text.contains("cmd-put: 2\n"));
    assert!(text.contains("max-job-size: 65535\n"));
    assert!(text.contains("pid: 123\n"));
    assert!(text.contains("version: \"1.13\"\n"));
    assert!(text.contains("rusage-utime: 1.000002\n"));
    assert!(text.contains("rusage-stime: 0.500000\n"));
    assert!(text.contains("draining: false\n"));
    assert!(text.contains("hostname: \"myhost\"\n"));
    assert!(text.contains("platform: \"x86_64\"\n"));
    assert!(text.ends_with("platform: \"x86_64\"\n"));
    assert!(!text.ends_with("\r\n")); // the trailing \r\n is added by Response::Ok, not here

    // Order check: cmd-put must come right after current-jobs-buried, and
    // job-timeouts right after cmd-pause-tube, matching STATS_FMT exactly.
    let idx_buried = lines
        .iter()
        .position(|l| l.starts_with("current-jobs-buried"))
        .unwrap();
    let idx_put = lines.iter().position(|l| l.starts_with("cmd-put")).unwrap();
    assert_eq!(idx_put, idx_buried + 1);

    let idx_pause_tube = lines
        .iter()
        .position(|l| l.starts_with("cmd-pause-tube"))
        .unwrap();
    let idx_job_timeouts = lines
        .iter()
        .position(|l| l.starts_with("job-timeouts"))
        .unwrap();
    assert_eq!(idx_job_timeouts, idx_pause_tube + 1);
}

#[test]
fn stats_server_draining_true() {
    let s = StatsServer {
        draining: true,
        ..Default::default()
    };
    let text = String::from_utf8(s.to_yaml().to_vec()).unwrap();
    assert!(text.contains("draining: true\n"));
}

#[test]
fn yaml_list_multiple() {
    let names = [tube("default"), tube("foo")];
    let out = yaml_list(names.iter());
    assert_eq!(out, "---\n- default\n- foo\n".as_bytes());
}

#[test]
fn yaml_list_empty() {
    let names: Vec<TubeName> = vec![];
    let out = yaml_list(names.iter());
    assert_eq!(out, "---\n".as_bytes());
}

#[test]
fn yaml_list_single_matches_reference_example() {
    // Verified against the reference server: `list-tubes-watched` with
    // only the default tube watched returns `OK 14\r\n---\n- default\n\r\n`.
    let names = [tube("default")];
    let out = yaml_list(names.iter());
    assert_eq!(out.len(), 14);
    assert_eq!(out, "---\n- default\n".as_bytes());
}
