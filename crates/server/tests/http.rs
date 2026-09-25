//! The HTTP listener (P2-T4): `/healthz`, `/readyz`, `/metrics`, `/admin`,
//! error statuses, and consistency with `stats` / `stats-tube`.

#![allow(clippy::unwrap_used)]

mod common;

use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::path::Path;
use std::time::{Duration, Instant};

use common::p2::{ConfigServer, Proto, get, http, stat_of};

/// Port 0: plaintext protocol; port 1: HTTP.
const CONFIG: &str = r#"
[[listener]]
addr = "127.0.0.1:{port0}"

[http]
addr = "127.0.0.1:{port1}"
max_tube_series = 2
"#;

fn start() -> ConfigServer {
    ConfigServer::start(CONFIG, 2, &[]).0
}

/// The value of the sample `name` (with its labels, exactly as rendered).
fn sample(metrics: &str, name: &str) -> String {
    metrics
        .lines()
        .find_map(|l| l.strip_prefix(name)?.strip_prefix(' '))
        .unwrap_or_else(|| panic!("{name} missing from\n{metrics}"))
        .to_owned()
}

#[test]
fn health_and_error_statuses() {
    let server = start();
    let addr = server.addr(1);

    let r = get(addr, "/healthz");
    assert_eq!((r.status, r.body.as_str()), (200, "ok"));
    let r = get(addr, "/readyz");
    assert_eq!((r.status, r.body.as_str()), (200, "ready"));

    assert_eq!(get(addr, "/").status, 404);
    assert_eq!(get(addr, "/metrics/x").status, 404);
    assert_eq!(get(addr, "/stats").status, 404);

    for method in ["POST", "PUT", "DELETE", "HEAD"] {
        let r = http(addr, method, "/metrics", "Content-Length: 0\r\n").unwrap();
        assert_eq!(r.status, 405, "{method}");
        assert_eq!(r.header("allow"), Some("GET"), "{method}");
    }

    // Request bodies are refused (and never read).
    let r = http(addr, "GET", "/metrics", "Content-Length: 5\r\n").unwrap();
    assert_eq!(r.status, 400);
    let r = http(addr, "GET", "/admin", "Transfer-Encoding: chunked\r\n").unwrap();
    assert_eq!(r.status, 400);

    // The server still serves after all that.
    assert_eq!(get(addr, "/healthz").status, 200);
}

/// Jobs in every state across three tubes.
fn populate(c: &mut Proto<TcpStream>) {
    assert_eq!(c.cmd("use a"), "USING a");
    assert_eq!(c.put(b"1"), "INSERTED 1");
    assert_eq!(c.cmd("put 5000 0 60 1\r\n2"), "INSERTED 2");
    assert_eq!(c.cmd("put 5000 100 60 1\r\n3"), "INSERTED 3");
    assert_eq!(c.cmd("use b"), "USING b");
    assert_eq!(c.cmd("put 5000 0 60 1\r\n4"), "INSERTED 4");
    assert_eq!(c.cmd("put 5000 0 60 1\r\n5"), "INSERTED 5");
    assert_eq!(c.cmd("watch b"), "WATCHING 2");
    assert_eq!(c.cmd("ignore default"), "WATCHING 1");
    assert_eq!(c.body_reply("reserve").0, "RESERVED 4 1");
    assert_eq!(c.body_reply("reserve").0, "RESERVED 5 1");
    assert_eq!(c.cmd("bury 5 10"), "BURIED");
    assert_eq!(c.cmd("pause-tube a 30"), "PAUSED");
    assert_eq!(c.cmd("use c"), "USING c");
    assert_eq!(c.cmd("delete 4"), "DELETED");
}

#[test]
fn metrics_match_stats() {
    let server = start();
    let mut c = server.plain(0);
    populate(&mut c);

    let tube_a = c.yaml("stats-tube a");
    let stats = c.yaml("stats");
    let r = get(server.addr(1), "/metrics");
    assert_eq!(r.status, 200);
    assert_eq!(
        r.header("content-type"),
        Some("text/plain; version=0.0.4; charset=utf-8")
    );
    let m = r.body;

    for (state, key) in [
        ("urgent", "current-jobs-urgent"),
        ("ready", "current-jobs-ready"),
        ("reserved", "current-jobs-reserved"),
        ("delayed", "current-jobs-delayed"),
        ("buried", "current-jobs-buried"),
    ] {
        let name = format!("beanstalkd_current_jobs{{state=\"{state}\"}}");
        assert_eq!(sample(&m, &name), stat_of(&stats, key), "{key}");
        let name = format!("beanstalkd_tube_current_jobs{{tube=\"a\",state=\"{state}\"}}");
        assert_eq!(sample(&m, &name), stat_of(&tube_a, key), "a {key}");
    }
    for cmd in [
        "put",
        "reserve",
        "delete",
        "bury",
        "use",
        "watch",
        "ignore",
        "pause-tube",
        "stats-tube",
    ] {
        let name = format!("beanstalkd_commands_total{{cmd=\"{cmd}\"}}");
        assert_eq!(
            sample(&m, &name),
            stat_of(&stats, &format!("cmd-{cmd}")),
            "{cmd}"
        );
    }
    // `stats` counts itself in its own reply; the snapshot, taken after,
    // sees that same count (and does not add one of its own).
    assert_eq!(
        sample(&m, "beanstalkd_commands_total{cmd=\"stats\"}"),
        stat_of(&stats, "cmd-stats")
    );
    for (metric, key) in [
        ("beanstalkd_jobs_total", "total-jobs"),
        ("beanstalkd_current_tubes", "current-tubes"),
        ("beanstalkd_current_connections", "current-connections"),
        ("beanstalkd_connections_total", "total-connections"),
        ("beanstalkd_current_producers", "current-producers"),
        ("beanstalkd_current_workers", "current-workers"),
        ("beanstalkd_current_waiting", "current-waiting"),
        ("beanstalkd_max_job_size_bytes", "max-job-size"),
        ("beanstalkd_binlog_max_size_bytes", "binlog-max-size"),
    ] {
        assert_eq!(sample(&m, metric), stat_of(&stats, key), "{key}");
    }
    // Tube `b` (third in list-tubes order) is past the cap of two series.
    for (tube, yaml) in [("a", &tube_a)] {
        for (metric, key) in [
            ("beanstalkd_tube_jobs_total", "total-jobs"),
            ("beanstalkd_tube_current_using", "current-using"),
            ("beanstalkd_tube_current_watching", "current-watching"),
            ("beanstalkd_tube_pause_seconds", "pause"),
        ] {
            let name = format!("{metric}{{tube=\"{tube}\"}}");
            assert_eq!(sample(&m, &name), stat_of(yaml, key), "{tube} {key}");
        }
        let name = format!("beanstalkd_tube_commands_total{{tube=\"{tube}\",cmd=\"delete\"}}");
        assert_eq!(sample(&m, &name), stat_of(yaml, "cmd-delete"), "{tube}");
    }

    // Four tubes (default, a, b, c) and a cap of two series.
    assert_eq!(sample(&m, "beanstalkd_current_tubes"), "4");
    assert_eq!(sample(&m, "beanstalkd_tube_series_limit"), "2");
    assert_eq!(sample(&m, "beanstalkd_tube_series_truncated"), "1");
    assert!(!m.contains("tube=\"b\""), "b is past the cap:\n{m}");

    // Taking snapshots changes no counter.
    for _ in 0..3 {
        get(server.addr(1), "/metrics");
        get(server.addr(1), "/admin");
    }
    let again = c.yaml("stats");
    let before: u64 = stat_of(&stats, "cmd-stats").parse().unwrap();
    assert_eq!(stat_of(&again, "cmd-stats"), (before + 1).to_string());
    for key in [
        "cmd-put",
        "cmd-stats-tube",
        "total-connections",
        "total-jobs",
    ] {
        assert_eq!(stat_of(&again, key), stat_of(&stats, key), "{key}");
    }
}

/// Parses `key: value` stats YAML into (key, value) pairs.
fn yaml_pairs(yaml: &str) -> Vec<(String, String)> {
    yaml.lines()
        .skip(1)
        .filter_map(|l| l.split_once(": "))
        .map(|(k, v)| (k.to_owned(), v.trim().to_owned()))
        .collect()
}

/// A JSON value as the text `stats` would show for it.
fn as_stats_text(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        other => panic!("unexpected JSON value {other}"),
    }
}

#[test]
fn admin_json_matches_stats() {
    let server = start();
    let mut c = server.plain(0);
    populate(&mut c);
    let stats = c.yaml("stats");
    let tubes = c.yaml("list-tubes");
    let per_tube: Vec<(String, String)> = tubes
        .lines()
        .skip(1)
        .map(|l| l.trim_start_matches("- ").to_owned())
        .map(|name| {
            let yaml = c.yaml(&format!("stats-tube {name}"));
            (name, yaml)
        })
        .collect();

    let r = get(server.addr(1), "/admin");
    assert_eq!(r.status, 200);
    assert_eq!(r.header("content-type"), Some("application/json"));
    let doc: serde_json::Value = serde_json::from_str(&r.body).unwrap();

    // Keys in the reference's order, values as `stats` reports them
    // (apart from what changes with time or with our own commands).
    let server_obj = doc["server"].as_object().unwrap();
    // (serde_json sorts object keys; the order itself is covered by the
    // renderer's unit tests.)
    let mut keys: Vec<&String> = server_obj.keys().collect();
    keys.sort();
    let pairs = yaml_pairs(&stats);
    let mut want_keys: Vec<&String> = pairs.iter().map(|(k, _)| k).collect();
    want_keys.sort();
    assert_eq!(keys, want_keys, "server keys");
    for (key, value) in &pairs {
        if matches!(
            key.as_str(),
            "uptime" | "rusage-utime" | "rusage-stime" | "cmd-stats-tube" | "cmd-list-tubes"
        ) {
            continue;
        }
        let got = as_stats_text(&server_obj[key]);
        let want = if key == "draining" {
            value.clone()
        } else {
            value.trim_matches('"').to_owned()
        };
        assert_eq!(got, want, "{key}");
    }

    let json_tubes = doc["tubes"].as_array().unwrap();
    assert_eq!(json_tubes.len(), per_tube.len(), "tubes are never capped");
    for (t, (name, yaml)) in json_tubes.iter().zip(&per_tube) {
        assert_eq!(t["name"], serde_json::Value::String(name.clone()));
        for (key, value) in yaml_pairs(yaml) {
            if key == "pause-time-left" {
                continue;
            }
            assert_eq!(
                as_stats_text(&t[&key]),
                value.trim_matches('"'),
                "{name} {key}"
            );
        }
    }
}

/// Writes `n` jobs of `size` bytes through a pipelined connection.
fn fill(addr: std::net::SocketAddr, n: usize, size: usize) {
    let s = TcpStream::connect(addr).unwrap();
    let mut w = s.try_clone().unwrap();
    let body = vec![b'x'; size];
    let writer = std::thread::spawn(move || {
        let mut chunk = Vec::new();
        for i in 0..n {
            chunk.extend_from_slice(format!("put 0 0 60 {size}\r\n").as_bytes());
            chunk.extend_from_slice(&body);
            chunk.extend_from_slice(b"\r\n");
            if chunk.len() > 1 << 20 || i + 1 == n {
                w.write_all(&chunk).unwrap();
                chunk.clear();
            }
        }
    });
    let mut r = BufReader::new(s);
    let mut line = String::new();
    for _ in 0..n {
        line.clear();
        r.read_line(&mut line).unwrap();
        assert!(line.starts_with("INSERTED"), "{line:?}");
    }
    writer.join().unwrap();
}

fn binlog_config(dir: &Path) -> String {
    format!(
        r#"
[[listener]]
addr = "127.0.0.1:{{port0}}"

[http]
addr = "127.0.0.1:{{port1}}"

[binlog]
dir = "{}"
fsync = "never"
file_size = 104857600
"#,
        dir.display()
    )
}

/// `/readyz` is 503 while the binlog is replayed, `/healthz` 200. Replay
/// time is made long enough to observe by recovering many jobs; the poll
/// starts as soon as the process is spawned. (Best effort by nature: on a
/// very fast machine the 503 window could be shorter than one poll; the
/// job count is chosen so that it takes well over a second in a debug
/// build.)
#[test]
fn readyz_is_503_during_binlog_replay() {
    let binlog = tempfile::tempdir().unwrap();
    const JOBS: usize = 150_000;
    {
        let (mut server, _) = ConfigServer::start(&binlog_config(binlog.path()), 2, &[]);
        fill(server.addr(0), JOBS, 256);
        assert!(server.stop(nix::sys::signal::Signal::SIGTERM).success());
    }

    let dir = tempfile::tempdir().unwrap();
    let template = binlog_config(binlog.path());
    // Start without waiting for the protocol port, then poll HTTP at once.
    let ports = [0, 1].map(|_| common::p2::claim_port());
    let text = template
        .replace("{port0}", &ports[0].to_string())
        .replace("{port1}", &ports[1].to_string());
    let config = dir.path().join("config.toml");
    std::fs::write(&config, text).unwrap();
    let mut child = std::process::Command::new(common::p2::BIN)
        .arg("--config")
        .arg(&config)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let http_addr = ([127, 0, 0, 1], ports[1]).into();
    let started = Instant::now();
    let mut saw_503 = false;
    let mut healthy_while_not_ready = false;
    let mut metrics_503 = false;
    let ready_after = loop {
        assert!(
            started.elapsed() < Duration::from_secs(120),
            "never became ready"
        );
        assert!(child.try_wait().unwrap().is_none(), "server exited");
        match http(http_addr, "GET", "/readyz", "") {
            Ok(r) if r.status == 200 => break started.elapsed(),
            Ok(r) => {
                assert_eq!(r.status, 503);
                assert_eq!(r.body, "not ready");
                saw_503 = true;
                // Recovery may complete between two requests.
                match get(http_addr, "/metrics").status {
                    503 => metrics_503 = true,
                    200 => {}
                    other => panic!("/metrics: {other}"),
                }
                if get(http_addr, "/healthz").status == 200 {
                    healthy_while_not_ready = true;
                }
            }
            Err(_) => std::thread::sleep(Duration::from_millis(2)),
        }
    };
    eprintln!("ready after {ready_after:?}");
    assert!(saw_503, "never saw 503 (ready after {ready_after:?})");
    assert!(healthy_while_not_ready);
    assert!(metrics_503);
    // Ready means recovered: every job is back, and served.
    let mut c = Proto::new(TcpStream::connect(("127.0.0.1", ports[0])).unwrap());
    assert_eq!(c.stat("stats", "current-jobs-ready"), JOBS.to_string());
    let m = get(http_addr, "/metrics").body;
    assert_eq!(
        sample(&m, "beanstalkd_current_jobs{state=\"ready\"}"),
        JOBS.to_string()
    );
    let _ = child.kill();
    let _ = child.wait();
    ports.into_iter().for_each(common::p2::release);
}

#[test]
fn sigterm_stops_http_and_listeners() {
    let mut server = start();
    assert_eq!(get(server.addr(1), "/healthz").status, 200);
    let status = server.stop(nix::sys::signal::Signal::SIGTERM);
    assert!(status.success(), "{status}");
    assert!(TcpStream::connect(server.addr(1)).is_err());
    assert!(TcpStream::connect(server.addr(0)).is_err());
}
